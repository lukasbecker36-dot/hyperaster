"""
Outbound alerting for the live monitor.

Sends every CRITICAL log record to an HTTP webhook so unhedged-position errors
don't sit silently in monitor.log at 3am. Configured via env vars; if neither
is set, alerting is a no-op (safe default for local dev / paper mode).

Env:
  ALERT_TELEGRAM_BOT_TOKEN + ALERT_TELEGRAM_CHAT_ID  → Telegram
  ALERT_DISCORD_WEBHOOK_URL                          → Discord
  ALERT_GENERIC_WEBHOOK_URL                          → POST {"text": "..."} to any URL
  ALERT_MIN_LEVEL=WARNING                            → optional, raise floor below CRITICAL

Multiple channels can be configured at once; all get notified.
"""

import logging
import os
import queue
import threading
import time
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

_PREFIX = os.getenv("ALERT_PREFIX", "[hyperaster]")
_QUEUE: queue.Queue = queue.Queue(maxsize=200)
_WORKER_STARTED = False
# Rate-limit identical messages: same text -> at most once per 60s
_LAST_SENT: dict[str, float] = {}
_DEDUPE_SECONDS = 60


def _telegram_send(text: str):
    token = os.getenv("ALERT_TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("ALERT_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text[:4000]}).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    urllib.request.urlopen(req, timeout=10).read()


def _discord_send(text: str):
    url = os.getenv("ALERT_DISCORD_WEBHOOK_URL")
    if not url:
        return
    import json
    body = json.dumps({"content": text[:1900]}).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()


def _generic_send(text: str):
    url = os.getenv("ALERT_GENERIC_WEBHOOK_URL")
    if not url:
        return
    import json
    body = json.dumps({"text": text[:2000]}).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()


_CHANNELS = (_telegram_send, _discord_send, _generic_send)


def _worker():
    while True:
        text = _QUEUE.get()
        if text is None:
            return
        for send in _CHANNELS:
            try:
                send(text)
            except Exception as e:
                # Don't log at error level — that could recurse into the handler
                log.debug(f"alert channel {send.__name__} failed: {e}")


def _ensure_worker():
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    t = threading.Thread(target=_worker, name="notify-worker", daemon=True)
    t.start()
    _WORKER_STARTED = True


def send_alert(text: str):
    """Queue an alert. Non-blocking; drops silently if queue is full."""
    full = f"{_PREFIX} {text}"
    now = time.time()
    last = _LAST_SENT.get(full)
    if last is not None and now - last < _DEDUPE_SECONDS:
        return
    # Prune expired dedupe entries so the dict doesn't grow forever.
    if len(_LAST_SENT) > 500:
        for k in [k for k, t in _LAST_SENT.items() if now - t >= _DEDUPE_SECONDS]:
            _LAST_SENT.pop(k, None)
    _LAST_SENT[full] = now
    _ensure_worker()
    try:
        _QUEUE.put_nowait(full)
    except queue.Full:
        pass


class AlertingHandler(logging.Handler):
    """Forwards records at level >= self.level to send_alert()."""

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            send_alert(msg)
        except Exception:
            pass  # never let alerting raise


def install_handler():
    """Attach the alerting handler to the root logger. Idempotent."""
    root = logging.getLogger()
    if any(isinstance(h, AlertingHandler) for h in root.handlers):
        return
    level_name = os.getenv("ALERT_MIN_LEVEL", "CRITICAL").upper()
    level = getattr(logging, level_name, logging.CRITICAL)
    h = AlertingHandler(level=level)
    h.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root.addHandler(h)
    log.info(
        f"Alerting installed (level={level_name}, "
        f"telegram={bool(os.getenv('ALERT_TELEGRAM_BOT_TOKEN'))}, "
        f"discord={bool(os.getenv('ALERT_DISCORD_WEBHOOK_URL'))}, "
        f"generic={bool(os.getenv('ALERT_GENERIC_WEBHOOK_URL'))})"
    )
