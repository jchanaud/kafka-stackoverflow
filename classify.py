import pandas as pd
from openai import OpenAI
from pydantic import BaseModel
from typing import List
import time
import concurrent.futures



class StructuredResponse(BaseModel):
    row: int
    category: str
    language: str
    difficulty: str
class BatchResponse(BaseModel):
    responses: List[StructuredResponse]

client = OpenAI(api_key="sk-proj-TmljZSB0cnksIGJ1dCBuby4gSGVyZSwgd2VsbCBkZXNlcnZlZDogaHR0cHM6Ly93d3cueW91dHViZS5jb20vd2F0Y2g/dj1kUXc0dzlXZ1hjUQ==")

input_path = "stackoverflow_kafka.csv"
output_path = "stackoverflow_kafka_classified.csv"

# Load existing file if already partially classified
try:
    df = pd.read_csv(output_path, sep=";")
    print(f"Resuming from {output_path}")
except FileNotFoundError:
    df = pd.read_csv(input_path, sep=";")
    df["category"] = None
    df["language"] = None
    df["difficulty"] = None
    print("Starting fresh")

def classify_batch(batch: List[str]) -> BatchResponse:
    """Call OpenAI to classify category and language."""
    prompt = """
    You are an expert at classifying Stack Overflow Kafka-related questions.

    Tasks:
    1. Assign one of these categories using the question and the tags to guide you:

    **Kafka Broker & Ops Fundamentals**
        Everything related to “Core concepts,” “Security & Authentication,” “Deployment & Operations,” and “Performance & Monitoring.” 
        Essentially, any question about Kafka architecture, configuration, cluster setup, security, fault tolerance, or performance goes here.
        Server configuration, broker setup, Zookeeper/KRaft management, topic configuration, partitioning, replication, and monitoring tools.
        Clients configuration and optimization questions should also go here.
        Do not include questions about using the Kafka client libraries (producers/consumers) unless it's about client configuration or connection to the broker issues.
        Examples:
        Kafka is filling up disk space despite of the retention settings
        Running Kafka ACL Commands fail
        Kafka rack awareness feature
        TLS configuration for Kafka
        Server is not working after zookeeper restart. I've created a log folder and mention its path still its not working

    **Kafka Connect & Data Integration**
        Questions about connecting Kafka to other systems (databases, search engines, cloud sources) using Kafka Connect, Debezium, or custom connectors.
        Examples:
        Kafka connect setup to send record from Aurora using AWS MSK
        Read CSV file using kafka-connect and sink into ms sql database?
        Append Strategy on field in Mongodb Document through kafka sink connector

    **Kafka Streams & Real-Time Processing Frameworks**
        Here we expand to other streaming frameworks when applicable. Any question about stream processing logic, state stores, windowing, or integration with Flink, Spark Streaming, ksqlDB, etc.
        Examples:
        How KeyValueStore allows write operations (Kafka Streams)
        Kafka Streams: group by subsequent identical keys and time windows
        Is it possible to create Tumbling Windows in Flink using a timestamp coming in Input Stream? If yes then How?
        TypeError: 'JavaPackage' object is not callable & Spark Streaming's Kafka libraries not found in class path

    **Schema Management & Serialization**
        Questions about data formats, schema evolution, and message serialization. Everything related to Avro, Protobuf, JSON Schema, and tools like Confluent Schema Registry.
        Examples:
        Use kafka-avro-console-producer with a schema already in the schema registry
        Backward Compatibility issue and uncertainty in Schema Registry
        Sending a JSON file with multiple JSON objects through KAFKA


    **Kafka Client and Language-Specific Programming Issues**
        Problems specific to Kafka clients libraries (but not Kafka Client configuration or optimization) while consuming or producing messages in a programming language or framework.
        This includes issues with client libraries, APIs, error handling, and integration with application code.
        Do not include general Kafka broker or operational questions here.
        Problems in a programming language or framework (but not a dedicated streaming framework).
        Examples:
        kafka-clients in Java: Consumer not receiving messages
        OutOfMemory exception when using the Kafka Producer
        Spring Boot Kafka Consumer not consuming, Kafka Listener not triggering
        Sometimes node-rdkafka consumer not reading any messages from topic
        Error in starting gradle project while using Spring Boot to Consume from Kafka
        python: confluent-kafka: No module named 'confluent_kafka.cimpl'

    **Vendor-Specific Issues**
        Questions specific to a cloud provider or vendor-managed Kafka service (AWS MSK, Aiven, Confluent Cloud, Azure Event Hubs, Cloudera, etc.)
        If the question is about Kafka in general and not specific to the vendor, it should go under "Kafka & Operational Fundamentals."
        If the question is about Kafka Connect, Streams, or Schema Registry in the context of a vendor, it should go under the respective category above.
        Examples:
        AWS MSK - IAM authentication for Kafka clients (This question is about IAM auth, which is specific to AWS MSK)
        Confluent Cloud confluent.value.schema.validation not working as expected (This question is about a Confluent Cloud-specific config not available in Apache Kafka)
 
    
    2. Detect the programming language of the question (e.g., Java, Go, C#, ...) when it makes sense.
       Use the tags to help identify the language. For instance, spark and flink are often associated with Scala or Java. pandas is Python. spring is Java.
       All Java, Scala, and Kotlin languages should be classified as "JVM".
       Do not answer "English".
       When the question can't be tied to a programming language (ie. Kafka configuration), answer "N/A".

    3. Classify the difficulty level into one of these categories:

    **Beginner**
        The user is just getting started with Kafka and related tools. Questions usually involve:
        First-time setup or installation issues (Kafka, Zookeeper, KRaft, Connect, Streams).
        Basic producer/consumer usage (how to send/receive a message, simple errors).
        Simple configuration misunderstandings (e.g., retention.ms, auto.offset.reset).
        Straightforward connector or schema usage without advanced customization.
        Rule of thumb: If the user could resolve it by following the Kafka quickstart or basic docs, it’s Beginner.
    **Intermediate**
        The user understands Kafka basics and is now working with more complex setups or integrations. Questions usually involve:
        Managing consumer groups, offsets, partitions, or replication factors.
        Troubleshooting performance or throughput (tuning producer/consumer/broker configs).
        Integrating Kafka with other systems (Connect, Debezium, JDBC, cloud sinks/sources).
        Handling schema evolution or serialization issues.
        Using Streams, Flink, or Spark Streaming for non-trivial use cases (windowing, joins, state stores).
        Rule of thumb: If the user is combining multiple Kafka features or components, it’s Intermediate.

    **Advanced**
        The user is working at a deep operational or architectural level, often requiring strong Kafka internals knowledge. Questions usually involve:
        Debugging broker internals, controller behavior, or partition rebalancing edge cases.
        Advanced security (Kerberos, SSL, IAM, multi-cluster authentication/authorization).
        Designing for multi-cluster, cross-data-center, or hybrid-cloud setups.
        High-throughput, low-latency tuning with tradeoffs across brokers, producers, and consumers.
        Custom development (custom Kafka Connect connectors, custom SerDes, or state store implementations).
        Advanced stream processing (session windows, exactly-once semantics, interactive queries, scaling state stores).
        Rule of thumb: If the question involves deep Kafka internals, large-scale distributed architecture, or custom implementations, it’s Advanced.


    Return your answer strictly as JSON with keys "row", "category", "language", and "difficulty" for each row of the input.
    """
    response = client.responses.parse(
    model="gpt-5-nano",
    input=[
        {
        "role": "system",
        "content": [
            {
            "type": "input_text",
            "text": prompt
            }
        ]
        },
        {
        "role": "user",
        "content": [
            {
            "type": "input_text",
            "text": "\n".join(batch)
            }
        ]
        }
    ],
    text_format=BatchResponse
    )
    return response.output_parsed

    

batches = []
batch = []
# Create batches of 20 rows
start_time = time.time()
for idx, row in df.iterrows():

    if pd.isna(row["category"]) or row["category"] is None:
        batch.append(f"Row: {idx} Tags: {row['tags']}\n Question: {row['question']}")

    if len(batch) >= 20:
        batches.append(batch)
        batch = []

if(len(batch) > 0):
    batches.append(batch)
    batch = []

# Process batches
max_workers = 10
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(classify_batch, batch) for batch in batches]
    for future in concurrent.futures.as_completed(futures):
        try:
            results = future.result()
            if results:
                for res in results.responses:
                    df.at[res.row, "category"] = res.category
                    df.at[res.row, "language"] = res.language
                    df.at[res.row, "difficulty"] = res.difficulty
            df.to_csv(output_path, sep=";", index=False)
            # number of completed futures
            completed = sum(1 for f in futures if f.done())
            elapsed = time.time() - start_time
            print(f"Saved progress to {output_path}. Completed {completed}/{len(futures)} batches. Elapsed time: {elapsed:.2f} seconds")
            # Estimate remaining time
            print(f"Estimated remaining time: {(elapsed / completed) * (len(futures) - completed):.2f} seconds")
        except TimeoutError:
            print("Request timed out")
            continue

        


print("✅ Classification complete")