import json

import psycopg2
from kafka import KafkaConsumer


# ============================================================
# 1. CONNECT TO POSTGRESQL
# ============================================================

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="sentiment_db",
    user="airflow",
    password="airflow"
)

cursor = connection.cursor()


# ============================================================
# 2. CREATE TABLE
# ============================================================

cursor.execute("""
    CREATE TABLE IF NOT EXISTS sentiment_results (
        id SERIAL PRIMARY KEY,
        tweet_text TEXT,
        predicted_sentiment VARCHAR(20),
        true_sentiment VARCHAR(20),
        result VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

connection.commit()


# ============================================================
# 3. CONNECT TO KAFKA
# ============================================================

consumer = KafkaConsumer(
    "sentiment-results",

    bootstrap_servers="localhost:9092",

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8")),

    auto_offset_reset="earliest",

    group_id="postgres-loader-group",

    enable_auto_commit=True
)


# ============================================================
# 4. START DATABASE CONSUMER
# ============================================================

print("=" * 80)
print("🗄️ DATABASE CONSUMER STARTED")
print("=" * 80)
print("Listening to Kafka topic: sentiment-results")
print("Writing results to PostgreSQL")
print("Waiting for messages...\n")


# ============================================================
# 5. PROCESS KAFKA MESSAGES
# ============================================================

for message in consumer:

    data = message.value

    tweet_text = data["text"]
    predicted_sentiment = data["predicted_sentiment"]
    true_sentiment = data["true_sentiment"]
    result = data["result"]


    # ========================================================
    # 6. INSERT INTO POSTGRESQL
    # ========================================================

    cursor.execute(
        """
        INSERT INTO sentiment_results
        (
            tweet_text,
            predicted_sentiment,
            true_sentiment,
            result
        )
        VALUES (%s, %s, %s, %s)
        """,
        (
            tweet_text,
            predicted_sentiment,
            true_sentiment,
            result
        )
    )

    connection.commit()


    # ========================================================
    # 7. DISPLAY DATABASE INSERT
    # ========================================================

    print("💾 Saved to PostgreSQL")
    print(f"Tweet      : {tweet_text}")
    print(f"Predicted  : {predicted_sentiment}")
    print(f"True       : {true_sentiment}")
    print(f"Result     : {result}")
    print("-" * 80)
