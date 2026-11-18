# Deployment — systemd + Telegram control

Three services:
- **`hyperaster`** — the arb monitor (`live_monitor.py`)
- **`hyperaster-control`** — the Telegram control bot (`control_bot.py`)
- **`hyperaster-capture`** — the order-book/oracle capture daemon
  (`scripts/capture_orderbooks.py`), feeding `/opps` and `/basis`

All run from the venv at `/opt/hyperaster/.venv` and load secrets from
`/opt/hyperaster/.env`.

## 1. Install the unit files

```bash
sudo cp /opt/hyperaster/deploy/hyperaster.service /etc/systemd/system/
sudo cp /opt/hyperaster/deploy/hyperaster-control.service /etc/systemd/system/
sudo cp /opt/hyperaster/deploy/hyperaster-capture.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 2. Configure Telegram in .env

```bash
# already present if alerting is set up:
ALERT_TELEGRAM_BOT_TOKEN=123456:ABC...
ALERT_TELEGRAM_CHAT_ID=<your numeric chat id>

# optional overrides for the control bot:
# CONTROL_TELEGRAM_CHAT_IDS=111,222   # allowlist (defaults to ALERT_TELEGRAM_CHAT_ID)
# CONTROL_SERVICE_NAME=hyperaster     # unit the bot controls
# CONTROL_BRANCH=claude/claude-md-setup-12x4q   # branch /restart pulls
```

To find your chat id: message the bot once, then
`curl https://api.telegram.org/bot<TOKEN>/getUpdates` and read `message.chat.id`.

## 3. Enable + start

```bash
sudo systemctl enable --now hyperaster
sudo systemctl enable --now hyperaster-control
sudo systemctl enable --now hyperaster-capture
```

## 4. Verify

```bash
systemctl status hyperaster hyperaster-control hyperaster-capture
journalctl -u hyperaster -f
journalctl -u hyperaster-capture -f    # cycles=… rows=… every 60s
```

Then from Telegram: `/status`, `/help`.

## Paper vs live

Mode is **not** hardcoded in the unit. It's read from `HYPERASTER_MODE` in
`/opt/hyperaster/data/mode.env` (loaded via `EnvironmentFile`). Missing file =
paper, the safe default.

Switch from Telegram — this rewrites the mode file and restarts the trader:

```
/mode        → show current mode
/paper       → switch to paper (warns if live positions are open)
/live YES    → switch to live (requires confirmation — real capital)
```

Or by hand:

```bash
echo 'HYPERASTER_MODE=live' | sudo tee /opt/hyperaster/data/mode.env
sudo systemctl restart hyperaster
```

You can still pin a mode in the unit by adding `--paper` or `--live` to
`ExecStart`; an explicit flag overrides the env var.

## Telegram commands

| command | action |
|---|---|
| `/status` | service state, mode, open position count |
| `/positions` | open positions detail |
| `/pnl` | realised P&L today + all-time, error count |
| `/log [n]` | last n journal lines (default 20) |
| `/mode` | show configured mode |
| `/paper` | switch to paper mode (restarts; warns if live positions open) |
| `/live YES` | switch to live mode (restarts; requires confirm) |
| `/start` | start the trader |
| `/stop YES` | stop the trader (positions left unmanaged!) |
| `/restart` | git pull + restart |
| `/flatten YES` | emergency close ALL positions on both venues |

`/stop` and `/flatten` require the `YES` confirmation (typed inline or as a
bare `YES` reply within 60s). The bot ignores any chat id not in the allowlist.

## Stock carry (`hyperaster-carry`)

`stock_carry.py` runs a market-neutral funding book on the xyz stock perps: daily at 00:05 UTC it shorts
the top-20% 24h funding names and buys the bottom 20%, $1,000 a side by default, 4x isolated, max 10% a
name, each daily cohort held 3 days. Paper by default; `/sc live` (asks YES) switches it to real orders.
It skips the session-flip names and anything the arb bot holds; while live, the arb skips its names.

```bash
sudo cp /opt/hyperaster/deploy/hyperaster-carry.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hyperaster-carry
journalctl -u hyperaster-carry -f
```

`/restart` restarts it with the other services once the unit is installed. Full book margin is about
2 x side / leverage ($500 at $1,000 a side and 4x); keep ~25% more free — on the xyz dex, or as spot USDC if the account is a unified account (HL backs every
dex from spot then, and the xyz dex itself shows $0).
