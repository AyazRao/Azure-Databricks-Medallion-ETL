import json, time, uuid, random
from azure.eventhub import EventHubProducerClient, EventData

# Replace with your full connection string
CONNECTION_STR = "<EVENTHUB_CONNECTION_STRING>"

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR)

with producer:
    for i in range(100):
        event = {
            "event_id": str(uuid.uuid4()),
            "order_id": f"O-{1000+i}",
            "store_id": random.choice(["S-1","S-2","S-3"]),
            "amount": round(random.uniform(10, 250), 2),
            "currency": "USD",
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        producer.send_batch([EventData(json.dumps(event))])
        time.sleep(0.05)  # 20 events/sec
print("✅ Events sent successfully")