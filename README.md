# financial-data-pipeline
This is a data pipeline project I built to learn how Kafka, Spark, and Airflow work together. It simulates mutual fund NAV updates and buy/sell transactions, and processes them step by step into a database.

## What it does

A Python script generates fake (simulated) NAV updates and transaction data for a few mutual funds like HDFC Top 100, SBI Bluechip, and Parag Parikh Flexi Cap. This data is sent to Kafka. From there, Spark jobs process the data in stages, going from raw data to cleaned data to final data, and the final data is stored in PostgreSQL. Airflow is used to run all these steps in order automatically.

## Tech used

- Kafka (for streaming the data)
- Spark (for processing the data)
- Airflow (for scheduling/running everything)
- PostgreSQL (for storing the final data)
- Docker (to run everything locally)
- Python

## Project structure

```
financial-data-pipeline/
├── config/
├── dags/
│   └── financial_pipeline_dag.py     -> Airflow DAG that runs the pipeline
├── plugins/
├── producers/
│   └── market_producer.py            -> generates fake NAV/transaction data and sends to Kafka
├── spark_jobs/
│   ├── kafka_to_bronze.py            -> raw data from Kafka
│   ├── bronze_to_silver.py           -> cleans the raw data
│   └── silver_to_gold.py             -> final processed data, saved to PostgreSQL
├── docker-compose.yml
├── .gitignore
└── README.md
```

## How the data flows

1. `market_producer.py` generates fake NAV and transaction data and sends it to two Kafka topics (`nav_raw` and `txn_raw`).
2. `kafka_to_bronze.py` reads this raw data from Kafka and stores it (Bronze layer).
3. `bronze_to_silver.py` cleans up the raw data (Silver layer).
4. `silver_to_gold.py` processes it further and loads it into PostgreSQL (Gold layer).
5. Airflow runs all of these steps in order using the DAG.

## How to run it

1. Start everything with Docker:
```
docker-compose up -d
```

2. Run the producer to start generating data:
```
python producers/market_producer.py
```

3. Open the Airflow UI in your browser and trigger the DAG to run the pipeline.



