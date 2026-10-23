import json
from kafka import KafkaConsumer

# Connect to Kafka
consumer = KafkaConsumer(
    "tweets",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sentiment-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("=" * 70)
print("🔥 KAFKA CONSUMER STARTED")
print("=" * 70)
print("Waiting for tweets...\n")

# Continuously listen for new tweets
for message in consumer:

    tweet = message.value

    print("📥 NEW TWEET RECEIVED")
    print(f"ID        : {tweet['id']}")
    print(f"Topic     : {tweet['topic']}")
    print(f"Sentiment : {tweet['sentiment']}")
    print(f"Tweet     : {tweet['text']}")
    print("-" * 70)
