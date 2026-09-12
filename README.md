# End-to-End Big Data Streaming Pipeline with Kafka & Hadoop

An end-to-end **real-time data engineering project** demonstrating how data can be collected from a public API, streamed through **Apache Kafka**, stored using the **Hadoop ecosystem**, and prepared for visualization and analysis.

The project was built to understand and implement a complete streaming pipeline rather than working with isolated Big Data technologies.

---

## 📌 Project Overview

This project implements a real-time streaming architecture where data is continuously retrieved from a public **Jokes API** and passed through a distributed streaming pipeline.

The main objective is to demonstrate how modern data engineering components can work together to transform raw API responses into **structured, persistent, and visualizable data**.

### Pipeline

```text
        Public Jokes API
               │
               ▼
        Data Collection
               │
               ▼
        Apache Kafka
        ┌──────┴──────┐
        │   Producer  │
        └──────┬──────┘
               │
          Kafka Topic
               │
               ▼
        Kafka Consumer
               │
               ▼
        Hadoop / HDFS
               │
               ▼
      Data Processing / Query
               │
               ▼
        Data Visualization
```

---

# 🎯 Objectives

The project focuses on the complete lifecycle of streaming data:

* Retrieve data from an external public API
* Transform API responses into structured data
* Publish streaming records to Apache Kafka
* Manage Kafka infrastructure using Apache ZooKeeper
* Consume streaming data from Kafka
* Store collected data in Hadoop HDFS
* Work with distributed Big Data storage
* Prepare data for analysis
* Visualize the resulting dataset
* Understand the architecture of an end-to-end streaming system

---

# 🏗️ Architecture

The project combines several technologies from the Big Data ecosystem.

### 1. Data Source — Public Jokes API

The pipeline starts by retrieving joke data from a public API.

The API acts as the external data source and provides continuously retrievable JSON data that can be transformed into structured records.

Typical information can include:

* Joke content
* Joke type/category
* Identifier
* Metadata associated with the joke

The API layer simulates a real-world external data source such as an application API, event service, IoT platform, or social platform.

---

### 2. Data Producer

A Python-based producer retrieves data from the API and prepares it for streaming.

The producer is responsible for:

```text
API Request
    ↓
Receive JSON Response
    ↓
Extract Relevant Fields
    ↓
Transform Data
    ↓
Publish to Kafka
```

This separates **data acquisition** from downstream processing.

---

### 3. Apache Kafka

Apache Kafka acts as the central **event streaming platform**.

Instead of sending API data directly to the storage layer, the producer publishes records to a Kafka topic.

```text
Producer
   │
   ▼
Kafka Topic
   │
   ├── Consumer 1
   │
   └── Consumer 2
```

This provides a decoupled architecture where producers and consumers can operate independently.

Kafka is responsible for handling the streaming layer and providing durable event storage and ordered records within partitions.

---

### 4. Apache ZooKeeper

The project uses **Apache ZooKeeper** as part of the Kafka infrastructure.

ZooKeeper provides coordination and metadata management for the Kafka environment used in this project.

The architecture therefore follows the traditional Kafka + ZooKeeper deployment model.

> Note: Modern Kafka deployments can use KRaft instead of ZooKeeper. ZooKeeper is used here because this project was built around the traditional Kafka architecture.

---

### 5. Kafka Consumer

The consumer subscribes to the Kafka topic and continuously retrieves incoming records.

```text
Kafka Topic
     │
     ▼
 Kafka Consumer
     │
     ▼
Data Transformation
     │
     ▼
Hadoop / HDFS
```

The consumer represents the bridge between the real-time streaming layer and the Big Data storage layer.

---

# 🗄️ Hadoop & HDFS Storage

After being consumed from Kafka, the data is persisted using **Hadoop Distributed File System (HDFS)**.

HDFS provides distributed storage designed for handling large datasets across multiple machines.

The project therefore demonstrates the transition:

```text
Real-Time Data
      ↓
Kafka Streaming
      ↓
Kafka Consumer
      ↓
HDFS
      ↓
Persistent Big Data Storage
```

This architecture separates:

* **Streaming:** Kafka
* **Coordination:** ZooKeeper
* **Distributed Storage:** Hadoop HDFS
* **Data Collection:** Python/API
* **Visualization:** Analytics/visualization layer

---

# 📊 Data Visualization

Once the streaming data has been collected and stored, it can be transformed into a structured dataset for exploration and visualization.

The visualization stage allows the resulting data to be inspected through:

* Data distributions
* Categories/types
* Record counts
* Streaming activity
* Other relevant statistics

This completes the pipeline from **raw external data to an analytical representation**.

---

# 🔄 End-to-End Workflow

The complete workflow can be summarized as:

### Step 1 — Extract

Retrieve data from the public Jokes API.

### Step 2 — Transform

Clean and structure the API response into usable records.

### Step 3 — Stream

Publish the records to an Apache Kafka topic.

### Step 4 — Consume

A Kafka consumer subscribes to the topic and receives the streaming records.

### Step 5 — Store

Persist the consumed records in Hadoop HDFS.

### Step 6 — Process

Transform the stored data into a format suitable for analysis.

### Step 7 — Visualize

Create visual representations of the collected data.

---

# 🧰 Technology Stack

| Technology             | Role                                            |
| ---------------------- | ----------------------------------------------- |
| **Python**             | API integration, data collection and processing |
| **Public Jokes API**   | External streaming data source                  |
| **Apache Kafka**       | Real-time event streaming                       |
| **Apache ZooKeeper**   | Kafka coordination                              |
| **Apache Hadoop**      | Big Data ecosystem                              |
| **HDFS**               | Distributed data storage                        |
| **Pandas**             | Data manipulation and transformation            |
| **Data Visualization** | Exploration and presentation of collected data  |

---

# 🧠 Key Data Engineering Concepts Demonstrated

This project provides hands-on experience with several important Data Engineering concepts.

### Real-Time Data Ingestion

Data is retrieved continuously from an external source rather than being treated as a static dataset.

### Event Streaming

Kafka is used as an intermediary streaming platform between data producers and consumers.

### Producer / Consumer Architecture

The producer and consumer are independent components communicating through Kafka topics.

### Distributed Storage

HDFS is used to persist data within the Hadoop ecosystem.

### Decoupled Architecture

The API, Kafka, consumer, storage, and visualization layers are separated, making the pipeline easier to extend.

### ETL / ELT Pipeline Design

The project demonstrates the fundamental stages of:

```text
Extract → Transform → Stream → Store → Analyze → Visualize
```

---

# 📁 Project Structure

```text
Big-Data-Kafka-Streaming/
│
├── Kafka Project code/
│   └── demo/
│       └── Kafka-related project files
│
├── Real Time Streaming pipeline code/
│   └── Streaming pipeline implementation
│
├── RedditAPI.py
│
├── Project Demo.mp4
│
└── README.md
```

> The repository contains some legacy/source-code naming inherited from the original implementation. For the project architecture and presentation, the external data source is treated as a **public Jokes API**.

---

# ⚙️ High-Level Setup

The project requires a Big Data environment containing the main components of the pipeline.

### Prerequisites

* Python 3.x
* Java
* Apache Kafka
* Apache ZooKeeper
* Apache Hadoop
* HDFS
* Python libraries used by the project

Install the Python dependencies used by the API/data-processing components:

```bash
pip install requests pandas
```

---

# ▶️ Running the Pipeline

The general execution sequence is:

### 1. Start Hadoop / HDFS

Start the Hadoop services and make sure HDFS is available.

### 2. Start ZooKeeper

Start the ZooKeeper service required by the Kafka deployment.

### 3. Start Kafka

Start the Kafka broker after ZooKeeper is running.

### 4. Create the Kafka Topic

Create the topic used by the producer and consumer.

Example:

```bash
kafka-topics.sh \
  --create \
  --topic jokes \
  --bootstrap-server localhost:9092
```

### 5. Start the Producer

Run the Python producer responsible for retrieving data from the public API and publishing records to Kafka.

### 6. Start the Consumer

Run the Kafka consumer that receives the records and writes them to the Hadoop/HDFS storage layer.

### 7. Analyze and Visualize

Use the resulting stored dataset for data exploration and visualization.

---

# 🔍 Example Data Flow

A single API response follows a flow similar to:

```text
Public Jokes API
      │
      │ JSON
      ▼
Python Producer
      │
      │ Kafka Message
      ▼
Kafka Topic
      │
      │ Stream
      ▼
Python Consumer
      │
      │ Structured Data
      ▼
HDFS
      │
      ▼
Analysis
      │
      ▼
Visualization
```

With multiple records:

```text
             ┌──────────────────┐
             │  Public API      │
             └────────┬─────────┘
                      │
                      ▼
             ┌──────────────────┐
             │ Python Producer  │
             └────────┬─────────┘
                      │
                      ▼
             ┌──────────────────┐
             │  Kafka Topic     │
             └────────┬─────────┘
                      │
             ┌────────┴────────┐
             ▼                 ▼
       Kafka Consumer      Other Consumers
             │
             ▼
        ┌─────────┐
        │  HDFS   │
        └────┬────┘
             │
             ▼
        Data Analysis
             │
             ▼
       Visualization
```

---

# 💡 Why This Architecture?

A direct API → HDFS architecture would tightly couple data collection and storage.

Using Kafka introduces a streaming layer:

```text
API → Kafka → Consumers → Storage
```

This provides several architectural advantages:

* Producers and consumers are decoupled
* Multiple consumers can consume the same stream
* Streaming data can be processed independently
* Kafka provides a durable event log
* The storage layer can be changed without modifying the producer
* Additional downstream processing can be added later

This makes the architecture closer to real-world data engineering systems.

---

# 👨‍💻 Author

**Salah Eddine Ouirra**

---

## ⭐ Project

This project demonstrates the design of a complete Big Data streaming pipeline using open-source technologies from the Kafka and Hadoop ecosystems.

It serves as a practical implementation of how raw data from an external API can be transformed into a **streaming, persistent, and analyzable data platform**.
