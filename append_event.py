import asyncio
import argparse
import json
import uuid
import os
from dotenv import load_dotenv
from src.event_store import EventStore
from src.models.events import BaseEvent
from src.upcasting.registry import registry as upcaster_registry

load_dotenv()

async def main():
    parser = argparse.ArgumentParser(description="Append a manual event to the Ledger.")
    parser.add_argument("--stream", required=True, help="Stream ID (e.g. loan-123)")
    parser.add_argument("--type", required=True, help="Event Type (e.g. NoteAdded)")
    parser.add_argument("--payload", required=True, help="JSON payload string")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()

    try:
        payload = json.loads(args.payload)
        event = BaseEvent(event_type=args.type, payload=payload)
        
        # We use expected_version=None to bypass OCC if the user is just manually appending
        # In production, you'd usually use the current version.
        new_version = await store.append(args.stream, [event], expected_version=None)
        
        print(f"✅ Event successfully appended to stream '{args.stream}'.")
        print(f"New stream version: {new_version}")
    except json.JSONDecodeError:
        print("❌ Error: Payload must be a valid JSON string.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await store.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
