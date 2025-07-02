import pandas as pd
from telethon.sync import TelegramClient


async def telegram_fetcher(TELEGRAM_API_ID, TELEGRAM_API_HASH, group_name, limit):
    async with TelegramClient(
        "session_name", TELEGRAM_API_ID, TELEGRAM_API_HASH
    ) as client:
        group_username = group_name.split("/")[-1]
        try:
            target_group = await client.get_entity(group_username)
        except Exception as e:
            print(f"Failed to find group: {e}")
            return

        messages_data = []

        async for message in client.iter_messages(target_group, limit=limit):
            if not message.text:
                continue
            sender = await message.get_sender()
            name = (
                sender.first_name
                if sender and hasattr(sender, "first_name")
                else "Unknown"
            )

            messages_data.append(
                {
                    "sender_name": name,
                    "message_text": message.text,
                    "message_date": message.date,
                    "message_id": message.id,
                    "sender_id": sender.id if sender else None,
                }
            )

        df = pd.DataFrame(messages_data)

        df["message_date"] = pd.to_datetime(df["message_date"])

        return df
