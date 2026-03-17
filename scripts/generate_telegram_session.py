import os

from telethon.sync import TelegramClient
from telethon.sessions import StringSession


def main() -> None:
    api_id_raw = (os.getenv("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.getenv("TELEGRAM_API_HASH") or "").strip()
    if not api_id_raw:
        api_id_raw = input("TELEGRAM_API_ID: ").strip()
    if not api_hash:
        api_hash = input("TELEGRAM_API_HASH: ").strip()
    if not api_id_raw or not api_hash:
        raise SystemExit("Both TELEGRAM_API_ID and TELEGRAM_API_HASH are required.")
    api_id = int(api_id_raw)

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\nSESSION_STRING=" + client.session.save())


if __name__ == "__main__":
    main()
