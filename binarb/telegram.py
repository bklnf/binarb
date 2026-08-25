from __future__ import annotations
import requests
from .app import _env
def _check(response):
    if response.ok: return
    try: detail = ": " + str(response.json().get("description"))
    except (ValueError, AttributeError): detail = ""
    raise RuntimeError(f"Telegram Bot API returned HTTP {response.status_code}{detail}")
def send_message(text):
    token, chat = _env("TG_BOT_TOKEN"), _env("TG_CHAT_ID")
    if not token or not chat: return
    response = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text}, timeout=10); _check(response)
class TelegramPoller:
    def __init__(self): self.token, self.chat, self.offset = _env("TG_BOT_TOKEN"), _env("TG_CHAT_ID"), 0
    def poll(self, *, timeout, min_date):
        response = requests.get(f"https://api.telegram.org/bot{self.token}/getUpdates",
            params={"timeout": timeout, "offset": self.offset}, timeout=timeout + 5); _check(response)
        result = []
        for update in response.json().get("result", []):
            self.offset = max(self.offset, int(update["update_id"]) + 1); message = update.get("message") or {}
            if self.chat and str((message.get("chat") or {}).get("id")) != str(self.chat): continue
            if float(message.get("date") or 0) < min_date: continue
            parts = str(message.get("text") or "").strip().split()
            if parts and parts[0].startswith("/"):
                parts[0] = parts[0].split("@", 1)[0].lower(); result.append(" ".join(parts))
        return result
