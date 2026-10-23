import pandas as pd
import json
import time
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Read dataset
df = pd.read_csv("../data/twitter_training.csv")

print("=" * 70)
print("🚀 TWEET PRODUCER STARTED")
print("=" * 70)
print(f"Total tweets available: {len(df)}")
print("Sending one tweet every 2 seconds...\n")

for index, row in df.iterrows():

    tweet_data = {
        "text": str(row.iloc[3]),
        "true_sentiment": str(row.iloc[2])
    }

    # Send tweet to Kafka
    producer.send("tweets", value=tweet_data)
    producer.flush()

    print(f"📤 Tweet #{index + 1}")
    print(f"Tweet       : {tweet_data['text']}")
    print(f"True Value  : {tweet_data['true_sentiment']}")
    print("-" * 70)

    time.sleep(3)

producer.close()

print("\n🎉 All tweets sent successfully!")
