import pandas as pd
import json
import time
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Read the CSV
df = pd.read_csv("../data/twitter_training.csv")

print("=" * 60)
print("🚀 REAL-TIME TWEET STREAM STARTED")
print("=" * 60)
print(f"Total tweets available: {len(df)}")
print("Sending ONE tweet every 2 seconds...\n")

for index, row in df.iterrows():

    tweet_data = {
        "id": str(row.iloc[0]),
        "topic": str(row.iloc[1]),
        "sentiment": str(row.iloc[2]),
        "text": str(row.iloc[3])
    }

    # Send one tweet to Kafka
    producer.send("tweets", value=tweet_data)
    producer.flush()

    print(f"📤 Tweet #{index + 1}")
    print(f"   Text      : {tweet_data['text']}")
    print(f"   Sentiment : {tweet_data['sentiment']}")
    print("-" * 60)

    # Wait before sending the next tweet
    time.sleep(2)

producer.close()

print("\n🎉 All tweets streamed successfully!")
