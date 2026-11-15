# Save as send_superstore_events.py
import json
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STR = "<EVENTHUB_CONNECTION_STRING>"
EVENTHUB_NAME = "sales-stream"
FILE_PATH = "superstore_events.jsonl"

producer = EventHubProducerClient.from_connection_string(CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def send_file_as_events(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        batch = producer.create_batch()
        has_items = False
        count = 0
        for line in f:
            line = line.strip()
            if not line:
                continue
            print(f"Preparing event: {line[:100]}...")  # print first 100 chars
            event = EventData(line)
            try:
                batch.add(event)
            except ValueError:
                producer.send_batch(batch)
                print(f"✅ Sent batch with {count} events")
                batch = producer.create_batch()
                batch.add(event)
            has_items = True
            count += 1
        if has_items:
            producer.send_batch(batch)
            print(f"✅ Sent final batch with {count} events")

if __name__ == "__main__":
    with producer:
        send_file_as_events(FILE_PATH)
    print("🎉 Done sending events.")
