import json
import sys
import threading
import time
from datetime import datetime, timezone

import requests

try:
    import websocket  # pip install websocket-client
except ImportError:
    websocket = None

# ============================================================
# SETTINGS — edit these directly, no command-line flags needed
# ============================================================
N_MARKETS = 70               # how many 5-min windows to watch before stopping
HIGH_THRESHOLD = 0.37         # side that reaches this first becomes "determining"
SUCCESS_THRESHOLD = 0.63      # determining side reaching this = success
LOW_THRESHOLD = 0.14            # determining side falling back to this before success = failure
USE_WEBSOCKET = True          # False = REST polling instead
VERBOSE = False               # False = only print per-window verdicts, not every tick
OUTPUT_FILE = "output.txt"    # every print also gets written here
# ============================================================


class Tee:
    """Duplicates everything written to stdout into a log file as well,
    so all the prints end up in OUTPUT_FILE while still showing live
    in the terminal."""

    def __init__(self, filename, stream):
        self.terminal = stream
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()


GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_REST = "https://clob.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WINDOW_SECONDS = 300  # 5 minutes


def ts_now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def fetch_market(slug: str, retries: int = 5, delay: float = 2.0) -> dict:
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data:
                return data[0]
            r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
            r.raise_for_status()
            events = r.json()
            if events and events[0].get("markets"):
                return events[0]["markets"][0]
        except Exception as e:
            last_err = e
        print(f"[{ts_now()}]   ...market not indexed yet (attempt {attempt+1}/{retries}), retrying")
        time.sleep(delay)
    raise ValueError(f"Could not find market for slug '{slug}' after {retries} retries. Last error: {last_err}")


def parse_outcomes(market: dict) -> dict:
    outcomes = json.loads(market["outcomes"])
    token_ids = json.loads(market["clobTokenIds"])
    raw_prices = json.loads(market.get("outcomePrices", "[]") or "[]")
    out = {}
    for i, name in enumerate(outcomes):
        out[name] = {
            "token_id": token_ids[i],
            "price": float(raw_prices[i]) if i < len(raw_prices) and raw_prices[i] not in (None, "") else None,
        }
    return out


class WindowClassifier:
    """Whichever side hits HIGH_THRESHOLD first becomes the 'determining'
    side. After that, only that side matters:
      - if it reaches SUCCESS_THRESHOLD -> success
      - if it falls back to LOW_THRESHOLD before that -> failure
      - if the window closes without hitting either -> failure
    If no side ever reaches HIGH_THRESHOLD, the window is 'undetermined'."""

    def __init__(self, token_up, token_down, high_thresh, success_thresh, low_thresh):
        self.token_up = token_up
        self.token_down = token_down
        self.high = high_thresh
        self.success_thresh = success_thresh
        self.low = low_thresh
        self.determining_token = None
        self.determining_side = None
        self.status = None  # None | "success" | "failure"
        self.decided_at = None

    def update(self, token_id, price, ts):
        if self.status is not None or token_id not in (self.token_up, self.token_down):
            return

        if self.determining_token is None:
            if price >= self.high:
                self.determining_token = token_id
                self.determining_side = "Up" if token_id == self.token_up else "Down"
            return

        if token_id != self.determining_token:
            return  # only the determining side matters from here on

        if price >= self.success_thresh:
            self.status = "success"
            self.decided_at = ts
        elif price <= self.low:
            self.status = "failure"
            self.decided_at = ts

    def final_status(self):
        """Status once the window has closed."""
        if self.status is not None:
            return self.status  # "success" or "failure"
        if self.determining_token is not None:
            # reached HIGH_THRESHOLD but never hit success or the stop-loss
            return "failure"
        return "undetermined"


def stream_window_ws(outcomes, close_ts, classifier, label):
    token_up = outcomes["Up"]["token_id"]
    token_down = outcomes["Down"]["token_id"]
    token_ids = [token_up, token_down]
    stop_event = threading.Event()
    ws_holder = {}

    def watchdog():
        while not stop_event.is_set():
            if classifier.status is not None or time.time() >= close_ts:
                try:
                    ws_holder["ws"].close()
                except Exception:
                    pass
                return
            time.sleep(0.5)

    def on_open(ws):
        print(f"[{ts_now()}] {label} websocket connected, subscribed to Up/Down tokens")
        ws_holder["ws"] = ws
        ws.send(json.dumps({"assets_ids": token_ids, "type": "market", "custom_feature_enabled": True}))

        def heartbeat():
            while not stop_event.is_set():
                try:
                    ws.send("PING")
                except Exception:
                    break
                time.sleep(10)
        threading.Thread(target=heartbeat, daemon=True).start()
        threading.Thread(target=watchdog, daemon=True).start()

    def on_message(ws, message):
        if message == "PONG":
            return
        try:
            events = json.loads(message)
        except json.JSONDecodeError:
            return
        if isinstance(events, dict):
            events = [events]

        for ev in events:
            etype = ev.get("event_type") or ev.get("type")
            asset_id = ev.get("asset_id")
            price = None
            if etype == "last_trade_price" and asset_id:
                price = float(ev["price"])
            elif etype == "best_bid_ask" and asset_id:
                bid, ask = ev.get("best_bid"), ev.get("best_ask")
                if bid is not None and ask is not None:
                    price = (float(bid) + float(ask)) / 2
            elif etype == "book" and asset_id:
                bids, asks = ev.get("bids") or [], ev.get("asks") or []
                if bids and asks:
                    price = (max(float(b["price"]) for b in bids) + min(float(a["price"]) for a in asks)) / 2

            if price is not None:
                classifier.update(asset_id, price, time.time())
                if VERBOSE:
                    side = "Up" if asset_id == token_up else "Down"
                    print(f"[{ts_now()}] {label} {side}={price:.3f}")

        if classifier.status is not None or time.time() >= close_ts:
            ws.close()

    def on_error(ws, error):
        print(f"[{ts_now()}] {label} [WS error] {error}")

    def on_close(ws, code, msg):
        stop_event.set()

    ws_app = websocket.WebSocketApp(
        CLOB_WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws_app.run_forever()


def rest_price(token_id):
    try:
        r = requests.get(f"{CLOB_REST}/midpoint", params={"token_id": token_id}, timeout=5)
        if r.ok:
            return float(r.json()["mid"])
    except Exception:
        pass
    try:
        r = requests.get(f"{CLOB_REST}/price", params={"token_id": token_id, "side": "buy"}, timeout=5)
        if r.ok:
            return float(r.json()["price"])
    except Exception:
        pass
    return None


def stream_window_poll(outcomes, close_ts, classifier, label, interval=2.0):
    token_up = outcomes["Up"]["token_id"]
    token_down = outcomes["Down"]["token_id"]
    print(f"[{ts_now()}] {label} polling mode (REST), checking every {interval}s")
    while classifier.status is None and time.time() < close_ts:
        for token_id in (token_up, token_down):
            p = rest_price(token_id)
            if p is not None:
                classifier.update(token_id, p, time.time())
                if VERBOSE:
                    side = "Up" if token_id == token_up else "Down"
                    print(f"[{ts_now()}] {label} {side}={p:.3f}")
        time.sleep(interval)


def wait_for_window(target_ts, label):
    """Blocks until target_ts, printing progress so it's never silent."""
    remaining = target_ts - time.time()
    if remaining <= 0:
        return
    print(f"[{ts_now()}] {label} current window already in progress -> "
          f"waiting {int(remaining)}s for the next window to open")
    while True:
        remaining = target_ts - time.time()
        if remaining <= 0:
            return
        chunk = min(15, remaining)
        time.sleep(chunk)
        remaining = target_ts - time.time()
        if remaining > 0:
            print(f"[{ts_now()}] {label} ...still waiting, {int(remaining)}s left")


def analyze():
    print(f"[{ts_now()}] Starting analysis of {N_MARKETS} markets "
          f"(trigger>={HIGH_THRESHOLD}, success>={SUCCESS_THRESHOLD}, stop-loss<={LOW_THRESHOLD})")

    records = []  # list of dicts: slug, label, determining_side, status

    base_ts = int(time.time())
    base_ts -= base_ts % WINDOW_SECONDS

    for i in range(N_MARKETS):
        ts = base_ts + i * WINDOW_SECONDS
        slug = f"btc-updown-5m-{ts}"
        close_ts = ts + WINDOW_SECONDS
        label = f"[{i+1}/{N_MARKETS} {slug}]"

        wait_for_window(ts, label)

        print(f"[{ts_now()}] {label} window is live, closes in {int(close_ts - time.time())}s. Looking it up...")
        try:
            market = fetch_market(slug)
        except ValueError as e:
            print(f"[{ts_now()}] {label} !! {e} -> UNDETERMINED")
            records.append({"slug": slug, "label": label, "determining_side": None, "status": "undetermined"})
            continue

        outcomes = parse_outcomes(market)
        print(f"[{ts_now()}] {label} FOUND market: {market.get('question', '(no title)')}")
        print(f"[{ts_now()}] {label} Up token:   {outcomes['Up']['token_id']}")
        print(f"[{ts_now()}] {label} Down token: {outcomes['Down']['token_id']}")

        classifier = WindowClassifier(
            outcomes["Up"]["token_id"], outcomes["Down"]["token_id"],
            HIGH_THRESHOLD, SUCCESS_THRESHOLD, LOW_THRESHOLD
        )
        if outcomes["Up"]["price"] is not None:
            classifier.update(outcomes["Up"]["token_id"], outcomes["Up"]["price"], time.time())
        if outcomes["Down"]["price"] is not None:
            classifier.update(outcomes["Down"]["token_id"], outcomes["Down"]["price"], time.time())

        if USE_WEBSOCKET and websocket is not None:
            try:
                stream_window_ws(outcomes, close_ts, classifier, label)
            except Exception as e:
                print(f"[{ts_now()}] {label} [WS failed: {e}] falling back to REST polling")
                stream_window_poll(outcomes, close_ts, classifier, label)
        else:
            stream_window_poll(outcomes, close_ts, classifier, label)

        status = classifier.final_status()
        determining = f" (determining side: {classifier.determining_side})" if classifier.determining_side else ""

        if status == "success":
            print(f"[{ts_now()}] {label} >>> RESULT: SUCCESS{determining} (hit {SUCCESS_THRESHOLD}) <<<")
        elif status == "failure":
            if classifier.decided_at is not None:
                print(f"[{ts_now()}] {label} >>> RESULT: FAILURE{determining} (stop-loss hit) <<<")
            else:
                print(f"[{ts_now()}] {label} >>> RESULT: FAILURE{determining} "
                      f"(window closed without reaching {SUCCESS_THRESHOLD}) <<<")
        else:
            print(f"[{ts_now()}] {label} >>> RESULT: UNDETERMINED (never reached {HIGH_THRESHOLD}) <<<")

        records.append({
            "slug": slug,
            "label": label,
            "determining_side": classifier.determining_side,
            "status": status,
        })

    return [(r["status"], r["slug"]) for r in records]


def summarize(results):
    total = len(results)
    successes = sum(1 for s, _ in results if s == "success")
    failures = sum(1 for s, _ in results if s == "failure")
    undetermined = sum(1 for s, _ in results if s == "undetermined")
    decided = successes + failures

    print("\n" + "=" * 55)
    print("SUMMARY")
    print("=" * 55)
    for status, slug in results:
        print(f"  {status.upper():13s} {slug}")
    print("-" * 55)
    print(f"Markets watched:  {total}")
    print(f"  Success:        {successes}")
    print(f"  Failure:        {failures}")
    print(f"  Undetermined:   {undetermined}")
    if decided > 0:
        prob = successes / decided
        print(f"\nSuccess rate (of decided markets): {prob:.4f}  ({prob*100:.2f}%)")
    else:
        print("\nNo decided markets to compute a success rate from.")
    print("=" * 55)


if __name__ == "__main__":
    tee = Tee(OUTPUT_FILE, sys.stdout)
    sys.stdout = tee
    try:
        results = analyze()
        summarize(results)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        sys.stdout = tee.terminal
        tee.close()
