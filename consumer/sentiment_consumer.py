import json

from kafka import KafkaConsumer, KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ============================================================
# 1. CREATE VADER SENTIMENT ANALYZER
# ============================================================

analyzer = SentimentIntensityAnalyzer()


# ============================================================
# 2. FUNCTION TO PREDICT SENTIMENT
# ============================================================

def predict_sentiment(text):
    """
    Analyze the tweet text and return:
    Positive, Negative, or Neutral
    """

    scores = analyzer.polarity_scores(text)

    compound_score = scores["compound"]

    if compound_score >= 0.05:
        return "Positive"

    elif compound_score <= -0.05:
        return "Negative"

    else:
        return "Neutral"


# ============================================================
# 3. CREATE KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    "tweets",

    bootstrap_servers="localhost:9092",

    # Convert JSON message into Python dictionary
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),

    # Start from the latest messages
    auto_offset_reset="latest",

    # Consumer group name
    group_id="sentiment-prediction-group",

    # Prevent automatic commit until message processing
    enable_auto_commit=True
)


# ============================================================
# 4. CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",

    # Convert Python dictionary into JSON
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# 5. START CONSUMER
# ============================================================

print("=" * 80)
print("🤖 SENTIMENT CONSUMER STARTED")
print("=" * 80)
print("Listening to Kafka topic: tweets")
print("Publishing results to: sentiment-results")
print("Waiting for tweets...\n")


# ============================================================
# 6. CONTINUOUSLY READ TWEETS
# ============================================================

for message in consumer:

    # Get the Kafka message
    tweet_data = message.value

    # Extract tweet text
    text = tweet_data["text"]

    # Extract the original true sentiment
    true_sentiment = tweet_data["true_sentiment"]


    # ========================================================
    # 7. PREDICT SENTIMENT
    # ========================================================

    predicted_sentiment = predict_sentiment(text)


    # ========================================================
    # 8. COMPARE PREDICTION WITH TRUE VALUE
    # ========================================================

    if predicted_sentiment == true_sentiment:
        result = "MATCH"
    else:
        result = "MISMATCH"


    # ========================================================
    # 9. DISPLAY RESULT
    # ========================================================

    print("=" * 80)
    print("📥 NEW TWEET")
    print("-" * 80)

    print(f"Tweet              : {text}")
    print(f"🤖 Predicted Value  : {predicted_sentiment}")
    print(f"🎯 True Value       : {true_sentiment}")
    print(f"📊 Result            : {result}")


    # ========================================================
    # 10. CREATE RESULT MESSAGE
    # ========================================================

    result_data = {
        "text": text,
        "predicted_sentiment": predicted_sentiment,
        "true_sentiment": true_sentiment,
        "result": result
    }


    # ========================================================
    # 11. SEND RESULT TO KAFKA
    # ========================================================

    producer.send(
        "sentiment-results",
        value=result_data
    )

    producer.flush()

    print("📤 Result published to: sentiment-results")
