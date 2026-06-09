#!/usr/bin/env python3
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, Response, jsonify

APP_VERSION = "DCL_WATCH_UMBREL_V5_1"

BASE_DIR = Path("/data")
CONFIG_PATH = BASE_DIR / "config.json"
STATE_FILE = BASE_DIR / "state.json"
EVENTS_FILE = BASE_DIR / "events.jsonl"

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8787,
    "poll_seconds": 10,
    "discord_webhook_url": "",
    "discord_username": "DCL Market Watch",
    "marketplace_base_url": "https://marketplace-api.decentraland.org",
    "request_timeout_seconds": 12,
    "nfts_fetch_count": 50,
    "seen_cache_size": 300,
    "bootstrap_without_alert": True
}

BASE_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def load_config() -> Dict[str, Any]:
    user = load_json(CONFIG_PATH, {})
    cfg = dict(DEFAULT_CONFIG)
    if isinstance(user, dict):
        cfg.update(user)
    env_webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    env_interval = os.environ.get("CHECK_INTERVAL", "").strip()
    if env_webhook:
        cfg["discord_webhook_url"] = env_webhook
    if env_interval.isdigit():
        cfg["poll_seconds"] = int(env_interval)
    return cfg


config = load_config()
persisted = load_json(STATE_FILE, {})

state: Dict[str, Any] = {
    "app_version": APP_VERSION,
    "status": persisted.get("status", "starting"),
    "started_at": utc_now_iso(),
    "last_check_at": persisted.get("last_check_at"),
    "last_success_at": persisted.get("last_success_at"),
    "last_error_at": persisted.get("last_error_at"),
    "last_error": persisted.get("last_error"),
    "checks": int(persisted.get("checks", 0)),
    "success": int(persisted.get("success", 0)),
    "errors": int(persisted.get("errors", 0)),
    "alerts": int(persisted.get("alerts", 0)),
    "alert_errors": int(persisted.get("alert_errors", 0)),
    "bootstrapped": bool(persisted.get("bootstrapped", False)),
    "last_seen": persisted.get("last_seen", []),
    "new_seen": persisted.get("new_seen", []),
    "seen_ids": persisted.get("seen_ids", []),
    "logs": persisted.get("logs", []),
    "last_raw_samples": persisted.get("last_raw_samples", {})
}

lock = threading.RLock()


def append_event_log(entry: Dict[str, Any]) -> None:
    try:
        with EVENTS_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def persist_state() -> None:
    with lock:
        snapshot = dict(state)
    save_json(STATE_FILE, snapshot)


def log(level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
    entry = {"ts": utc_now_iso(), "level": level.upper(), "message": message, "extra": extra or {}}
    with lock:
        logs = deque(state.get("logs", []), maxlen=200)
        logs.append(entry)
        state["logs"] = list(logs)
        state["app_version"] = APP_VERSION
    append_event_log(entry)
    persist_state()


def normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
    return []


def http_get_json(url: str, params: Dict[str, Any]) -> Any:
    resp = requests.get(
        url,
        params=params,
        timeout=int(config["request_timeout_seconds"]),
        headers={"Accept": "application/json", "User-Agent": APP_VERSION}
    )
    resp.raise_for_status()
    return resp.json()


def mana_from_wei(value: Any) -> str:
    try:
        if value is None or value == "":
            return "?"
        raw = Decimal(str(value))
        mana = raw / Decimal("1000000000000000000")
        if mana == mana.to_integral():
            return f"{int(mana):,}".replace(",", " ")
        return format(mana.normalize(), "f").rstrip("0").rstrip(".")
    except (InvalidOperation, ValueError, TypeError):
        return str(value or "?")


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def safe_get(d: Any, *path: Any) -> Any:
    cur = d
    for key in path:
        if isinstance(cur, list):
            try:
                idx = int(key)
            except Exception:
                return None
            if idx < 0 or idx >= len(cur):
                return None
            cur = cur[idx]
            continue
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def compact_json(value: Any, limit: int = 1500) -> str:
    try:
        s = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        s = str(value)
    return s[:limit]


def build_listing_key(item: Dict[str, Any]) -> Optional[str]:
    nft_id = first_non_empty(item.get("id"), safe_get(item, "nft", "id"), safe_get(item, "asset", "id"))
    token_id = first_non_empty(item.get("tokenId"), safe_get(item, "nft", "tokenId"), safe_get(item, "asset", "tokenId"))
    contract = first_non_empty(
        item.get("contract"),
        item.get("contractAddress"),
        safe_get(item, "nft", "contractAddress"),
        safe_get(item, "asset", "contractAddress"),
        safe_get(item, "order", "contractAddress"),
        safe_get(item, "activeOrder", "contractAddress")
    )
    price = first_non_empty(item.get("price"), safe_get(item, "order", "price"), safe_get(item, "activeOrder", "price"))
    parts = [str(x).strip() for x in [nft_id, contract, token_id, price] if x is not None and str(x).strip()]
    if not parts:
        return None
    return ":".join(parts)


def extract_listing(item: Dict[str, Any], forced_type: str) -> Optional[Dict[str, Any]]:
    nft = item.get("nft") if isinstance(item.get("nft"), dict) else {}
    asset = item.get("asset") if isinstance(item.get("asset"), dict) else {}
    order = item.get("order") if isinstance(item.get("order"), dict) else {}
    active_order = item.get("activeOrder") if isinstance(item.get("activeOrder"), dict) else {}

    token_id = first_non_empty(item.get("tokenId"), nft.get("tokenId"), asset.get("tokenId"), order.get("tokenId"), active_order.get("tokenId"))
    contract = first_non_empty(item.get("contract"), item.get("contractAddress"), nft.get("contractAddress"), asset.get("contractAddress"), order.get("contractAddress"), active_order.get("contractAddress"))
    price_raw = first_non_empty(item.get("price"), order.get("price"), active_order.get("price"))
    created_at = first_non_empty(order.get("createdAt"), active_order.get("createdAt"), item.get("updatedAt"), item.get("createdAt"), item.get("listedAt"), nft.get("updatedAt"), nft.get("createdAt"))
    name = first_non_empty(nft.get("name"), item.get("name"), item.get("searchText"), asset.get("name"))

    listing_key = build_listing_key(item)
    if not listing_key:
        return None

    token_id_str = str(token_id).strip() if token_id is not None else ""
    contract_str = str(contract).strip().lower() if contract is not None else ""
    name_str = str(name).strip() if name is not None else ""

    if not token_id_str and not contract_str and price_raw is None:
        return None

    return {
        "listing_id": listing_key,
        "type": forced_type,
        "token_id": token_id_str or "?",
        "price_raw": str(price_raw) if price_raw is not None else "?",
        "price_mana": mana_from_wei(price_raw),
        "name": name_str or ("Estate" if forced_type == "ESTATE" else "Parcel"),
        "contract_address": contract_str,
        "created_at": str(created_at).strip() if created_at is not None else "",
        "marketplace_url": f"https://decentraland.org/marketplace/contracts/{contract_str}/tokens/{token_id_str}" if contract_str and token_id_str else "https://decentraland.org/marketplace/"
    }


def fetch_recent_nfts(category: str, forced_type: str) -> List[Dict[str, Any]]:
    base = str(config["marketplace_base_url"]).rstrip("/")
    payload = http_get_json(
        f"{base}/v1/nfts",
        {
            "category": category,
            "isOnSale": "true",
            "sortBy": "recently_listed",
            "first": int(config.get("nfts_fetch_count", 50)),
            "skip": 0
        }
    )

    rows = normalize_rows(payload)
    with lock:
        state["last_raw_samples"][category] = compact_json(rows[0] if rows else {"empty": True})

    out = []
    invalid_count = 0
    for row in rows:
        try:
            listing = extract_listing(row, forced_type)
            if listing is None:
                invalid_count += 1
                continue
            out.append(listing)
        except Exception:
            invalid_count += 1

    if invalid_count:
        log("debug", f"ignored invalid {category} rows", {"count": invalid_count})
    return out


def fetch_recent_listings() -> List[Dict[str, Any]]:
    parcels = fetch_recent_nfts("parcel", "LAND")
    estates = fetch_recent_nfts("estate", "ESTATE")

    dedup: Dict[str, Dict[str, Any]] = {}
    for item in parcels + estates:
        dedup[item["listing_id"]] = item

    items = list(dedup.values())
    items.sort(key=lambda x: (x.get("created_at") or "", x.get("listing_id") or ""), reverse=True)
    return items


def send_discord_alert(listing: Dict[str, Any]) -> None:
    webhook = str(config.get("discord_webhook_url") or "").strip()
    if not webhook:
        raise RuntimeError("discord_webhook_url missing")

    payload = {
        "username": str(config.get("discord_username") or "DCL Market Watch"),
        "content": (
            f"🚨 **{listing['type']} NEW LISTING**\n"
            f"**Name:** {listing['name']}\n"
            f"**ID:** {listing['token_id']}\n"
            f"**Price:** {listing['price_mana']} MANA\n"
            f"{listing['marketplace_url']}"
        )
    }

    resp = requests.post(webhook, json=payload, timeout=int(config["request_timeout_seconds"]))
    resp.raise_for_status()


def send_test_discord_alert() -> None:
    webhook = str(config.get("discord_webhook_url") or "").strip()
    if not webhook:
        raise RuntimeError("discord_webhook_url missing")
    payload = {
        "username": str(config.get("discord_username") or "DCL Market Watch"),
        "content": "✅ DCL Market Watch Umbrel test alert"
    }
    resp = requests.post(webhook, json=payload, timeout=int(config["request_timeout_seconds"]))
    resp.raise_for_status()


def watcher_loop() -> None:
    log("info", "watcher started", {"app_version": APP_VERSION, "mode": "umbrel_v1_nfts"})

    while True:
        started = time.time()
        try:
            with lock:
                state["status"] = "checking"
                state["last_check_at"] = utc_now_iso()
                state["checks"] += 1
                state["app_version"] = APP_VERSION
            persist_state()

            listings = fetch_recent_listings()
            current_ids = [x["listing_id"] for x in listings]
            new_listings = []

            with lock:
                seen_ids = list(state.get("seen_ids", []))
                seen_set = set(seen_ids)

                if not state.get("bootstrapped", False) and bool(config.get("bootstrap_without_alert", True)):
                    state["seen_ids"] = current_ids[: int(config.get("seen_cache_size", 300))]
                    state["bootstrapped"] = True
                    state["last_seen"] = listings[:20]
                    state["new_seen"] = []
                    state["status"] = "running"
                    state["last_success_at"] = utc_now_iso()
                    state["success"] += 1
                    state["last_error"] = None
                    state["app_version"] = APP_VERSION
                else:
                    for listing in listings:
                        if listing["listing_id"] not in seen_set:
                            new_listings.append(listing)

                    merged_seen = current_ids + seen_ids
                    ordered_seen = []
                    added = set()
                    for item_id in merged_seen:
                        if item_id and item_id not in added:
                            ordered_seen.append(item_id)
                            added.add(item_id)

                    state["seen_ids"] = ordered_seen[: int(config.get("seen_cache_size", 300))]
                    state["last_seen"] = listings[:20]
                    if new_listings:
                        state["new_seen"] = (new_listings + list(state.get("new_seen", [])))[:50]

                    state["bootstrapped"] = True
                    state["status"] = "running"
                    state["last_success_at"] = utc_now_iso()
                    state["success"] += 1
                    state["last_error"] = None
                    state["app_version"] = APP_VERSION

            persist_state()

            for listing in new_listings:
                try:
                    send_discord_alert(listing)
                    with lock:
                        state["alerts"] += 1
                    persist_state()
                    log("alert", "new listing detected", listing)
                except Exception as exc:
                    with lock:
                        state["alert_errors"] += 1
                    persist_state()
                    log("error", "discord alert failed", {"error": str(exc), "listing": listing})

            if not new_listings:
                log("debug", "no new land/estate listing")

        except Exception as exc:
            with lock:
                state["status"] = "error"
                state["last_error_at"] = utc_now_iso()
                state["last_error"] = str(exc)
                state["errors"] += 1
                state["app_version"] = APP_VERSION
            persist_state()
            log("error", "watcher cycle failed", {"error": str(exc), "app_version": APP_VERSION})

        elapsed = time.time() - started
        time.sleep(max(1, int(config.get("poll_seconds", 10)) - elapsed))


app = Flask(__name__)


@app.route("/")
@app.route("/dcl-watch/")
def dashboard() -> Response:
    return Response("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DCL Market Watch</title>
<style>
:root{--bg:#f4f1e8;--ink:#101010;--muted:#6d665b;--line:#d8d0c1;--panel:rgba(255,255,255,.58);--panel2:rgba(255,255,255,.78);--green:#1f8f4d;--red:#bf2d2d;--amber:#a86b00}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(circle at 20% 0%,#fff 0,#f4f1e8 36%,#ebe4d6 100%);color:var(--ink);font:14px/1.45 Inter,system-ui,Arial,sans-serif}
body:before{content:"";position:fixed;inset:0;background-image:linear-gradient(rgba(0,0,0,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(0,0,0,.035) 1px,transparent 1px);background-size:28px 28px;pointer-events:none}
.wrap{position:relative;max-width:1440px;margin:0 auto;padding:34px}
.topbar{display:flex;justify-content:space-between;gap:18px;align-items:flex-end;margin-bottom:22px}
.kicker{font:700 12px/1 ui-monospace,Menlo,monospace;letter-spacing:.18em;text-transform:uppercase;color:var(--muted)}
h1{margin:6px 0 4px;font-size:44px;letter-spacing:-.06em;line-height:.95}
.subtitle{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:16px}
.card{grid-column:span 3;background:var(--panel);backdrop-filter:blur(14px);border:1px solid var(--line);border-radius:22px;box-shadow:0 20px 70px rgba(35,28,15,.08);overflow:hidden}
.card.wide{grid-column:span 7}.card.side{grid-column:span 5}.card.full{grid-column:span 12}
.inner{padding:18px}
.label{font:800 11px/1 ui-monospace,Menlo,monospace;letter-spacing:.15em;text-transform:uppercase;color:var(--muted);margin-bottom:12px}
.value{font-size:36px;font-weight:900;letter-spacing:-.06em}
.muted{color:var(--muted);font-size:12px;margin-top:8px}
.badge{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);background:var(--panel2);padding:7px 12px;border-radius:999px;font-weight:800;text-transform:uppercase;font-size:12px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--amber)}.dot.running{background:var(--green)}.dot.error{background:var(--red)}
button{border:1px solid var(--ink);background:var(--ink);color:#fff;border-radius:13px;padding:11px 14px;font-weight:800;cursor:pointer}
button:hover{transform:translateY(-1px)}
.table-wrap{overflow:auto}
table{width:100%;border-collapse:collapse;min-width:780px}
th{font:800 11px/1 ui-monospace,Menlo,monospace;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);text-align:left;padding:12px;border-bottom:1px solid var(--line)}
td{padding:14px 12px;border-bottom:1px solid var(--line);vertical-align:top}
tr:hover td{background:rgba(255,255,255,.38)}
.pill{display:inline-block;border:1px solid var(--ink);border-radius:999px;padding:5px 10px;font-size:11px;font-weight:900}
.name{font-weight:900}.sub,.id{color:var(--muted);font:12px ui-monospace,Menlo,monospace;word-break:break-all;margin-top:4px}
.price{font-weight:900;white-space:nowrap}
a{color:var(--ink);font-weight:900}
.logs,pre{background:rgba(255,255,255,.42);border:1px solid var(--line);border-radius:16px;padding:14px;max-height:360px;overflow:auto;white-space:pre-wrap;font:12px/1.5 ui-monospace,Menlo,monospace}
.empty{color:var(--muted);padding:12px}
@media(max-width:1100px){.card,.card.wide,.card.side,.card.full{grid-column:span 12}.topbar{display:block}h1{font-size:36px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div>
      <div class="kicker" id="version">...</div>
      <h1>DCL Market Watch</h1>
      <div class="subtitle">Umbrel watcher for new Decentraland LAND and ESTATE listings.</div>
    </div>
    <button onclick="testAlert()">Send test Discord alert</button>
  </div>
  <div class="grid">
    <div class="card"><div class="inner"><div class="label">Status</div><div id="statusBox"></div><div class="muted" id="statusMeta"></div></div></div>
    <div class="card"><div class="inner"><div class="label">Checks</div><div class="value" id="checks">0</div><div class="muted" id="checksMeta"></div></div></div>
    <div class="card"><div class="inner"><div class="label">Alerts</div><div class="value" id="alerts">0</div><div class="muted" id="alertsMeta"></div></div></div>
    <div class="card"><div class="inner"><div class="label">Interval</div><div class="value"><span id="pollSeconds">10</span>s</div><div class="muted">Persistent data in /data</div></div></div>
    <div class="card wide"><div class="inner"><div class="label">Last seen listings</div><div class="table-wrap" id="lastSeen"></div></div></div>
    <div class="card side"><div class="inner"><div class="label">Latest new listings</div><div class="table-wrap" id="newSeen"></div></div></div>
    <div class="card wide"><div class="inner"><div class="label">Recent logs</div><div class="logs" id="logs"></div></div></div>
    <div class="card side"><div class="inner"><div class="label">Debug raw API samples</div><pre id="debugPre">No data</pre></div></div>
  </div>
</div>
<script>
function esc(v){return String(v??'').replace(/[&<>"]/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[s]))}
function badge(s){return `<span class="badge"><span class="dot ${esc(s)}"></span>${esc(s||'unknown')}</span>`}
function short(v,n=24){v=String(v??'');return v.length>n?v.slice(0,n)+'…':v}
function table(items,compact=false){
 if(!items||!items.length)return '<div class="empty">No data yet</div>';
 let h='<table><thead><tr><th>Type</th><th>Name</th><th>ID</th><th>Price</th><th>Open</th></tr></thead><tbody>';
 for(const i of items){h+=`<tr><td><span class="pill">${esc(i.type)}</span></td><td><div class="name">${esc(i.name)}</div><div class="sub">${esc(i.contract_address)}</div></td><td><div class="id" title="${esc(i.token_id)}">${esc(short(i.token_id,compact?14:32))}</div></td><td><div class="price">${esc(i.price_mana)} MANA</div></td><td><a href="${esc(i.marketplace_url)}" target="_blank" rel="noreferrer">Marketplace</a></td></tr>`}
 return h+'</tbody></table>';
}
function logs(items){if(!items||!items.length)return'No logs';return items.slice().reverse().map(x=>`[${x.ts}] [${x.level}] ${x.message}${x.extra?' '+JSON.stringify(x.extra):''}`).join('\\n')}
async function loadStatus(){
 const r=await fetch('/api/status',{cache:'no-store'}); const d=await r.json();
 version.textContent=d.app_version||''; statusBox.innerHTML=badge(d.status);
 statusMeta.textContent=`last check: ${d.last_check_at||'-'} | last success: ${d.last_success_at||'-'}${d.last_error?' | last error: '+d.last_error:''}`;
 checks.textContent=d.checks??0; checksMeta.textContent=`success: ${d.success??0} | errors: ${d.errors??0}`;
 alerts.textContent=d.alerts??0; alertsMeta.textContent=`alert errors: ${d.alert_errors??0}`;
 pollSeconds.textContent=d.poll_seconds??10; lastSeen.innerHTML=table(d.last_seen||[]);
 newSeen.innerHTML=table(d.new_seen||[],true); document.getElementById('logs').textContent=logs(d.logs||[]);
 debugPre.textContent=JSON.stringify(d.last_raw_samples||{},null,2);
}
async function testAlert(){
 try{const r=await fetch('/api/test-alert',{method:'POST'});const d=await r.json();alert(d.ok?'Test alert sent':'Error: '+(d.error||'unknown'))}
 catch(e){alert('Error: '+e)}
}
loadStatus(); setInterval(loadStatus,5000);
</script>
</body>
</html>""", mimetype="text/html")


@app.route("/api/status")
@app.route("/dcl-watch/api/status")
def api_status():
    with lock:
        payload = {
            "app_version": state.get("app_version"),
            "status": state.get("status"),
            "started_at": state.get("started_at"),
            "last_check_at": state.get("last_check_at"),
            "last_success_at": state.get("last_success_at"),
            "last_error_at": state.get("last_error_at"),
            "last_error": state.get("last_error"),
            "checks": state.get("checks", 0),
            "success": state.get("success", 0),
            "errors": state.get("errors", 0),
            "alerts": state.get("alerts", 0),
            "alert_errors": state.get("alert_errors", 0),
            "last_seen": state.get("last_seen", [])[:20],
            "new_seen": state.get("new_seen", [])[:20],
            "logs": state.get("logs", [])[-80:],
            "last_raw_samples": state.get("last_raw_samples", {}),
            "poll_seconds": int(config.get("poll_seconds", 10))
        }
    return jsonify(payload)


@app.route("/api/test-alert", methods=["POST"])
@app.route("/dcl-watch/api/test-alert", methods=["POST"])
def api_test_alert():
    try:
        send_test_discord_alert()
        with lock:
            state["alerts"] += 1
        persist_state()
        log("info", "manual discord test alert sent")
        return jsonify({"ok": True})
    except Exception as exc:
        with lock:
            state["alert_errors"] += 1
        persist_state()
        log("error", "manual discord test alert failed", {"error": str(exc)})
        return jsonify({"ok": False, "error": str(exc)}), 500


def main() -> None:
    t = threading.Thread(target=watcher_loop, daemon=True)
    t.start()
    app.run(host=str(config.get("host", "0.0.0.0")), port=int(config.get("port", 8787)), debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
