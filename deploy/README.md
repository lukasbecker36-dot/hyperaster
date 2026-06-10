# Deployment — systemd + Telegram control

Two services:
- **`hyperaster`** — the arb monitor (`live_monitor.py`)
- **`hyperaster-control`** — the Telegram control bot (`control_bot.py`)

Both run from the venv at `/opt/hyperaster/.venv` and load secrets from
`/opt/hyperaster/.env`.

## 1. Install the unit files

```bash
sudo cp /opt/hyperaster/deploy/hyperaster.service /etc/systemd/system/
sudo cp /opt/hyperaster/deploy/hyperaster-control.service /etc/systemd/system/
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
```

## 4. Verify

```bash
systemctl status hyperaster hyperaster-control
journalctl -u hyperaster -f
```

Then from Telegram: `/status`, `/help`.

## Going live

Edit `hyperaster.service`, change `--paper` to `--live`, then:

```bash
sudo systemctl daemon-reload
sudo systemctl restart hyperaster
```

(Or just use `/restart` from Telegram after editing.)

## Telegram commands

| command | action |
|---|---|
| `/status` | service state, mode, open position count |
| `/positions` | open positions detail |
| `/pnl` | realised P&L today + all-time, error count |
| `/log [n]` | last n journal lines (default 20) |
| `/start` | start the trader |
| `/stop YES` | stop the trader (positions left unmanaged!) |
| `/restart` | git pull + restart |
| `/flatten YES` | emergency close ALL positions on both venues |

`/stop` and `/flatten` require the `YES` confirmation (typed inline or as a
bare `YES` reply within 60s). The bot ignores any chat id not in the allowlist.
