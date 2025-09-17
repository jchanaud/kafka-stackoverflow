# Usage

## `kafka.py` 
Queries Stack Overflow API and generate a CSV `stackoverflow_kafka.csv` that looks like this:
````
date;tags;question
2016-12-31 23:29:33;apache-kafka,kafka-producer-api;Kafka producers/brokers not using specified IP
2017-01-01 18:12:04;domain-driven-design,apache-kafka,cqrs,event-sourcing;How to generate identities when source of truth is Apache Kafka?
2017-01-02 04:36:24;apache-kafka,event-sourcing;"How to store &amp; read events specific to an Aggregate from Kafka in an ES/CQRS application?"
````
Their API has a limit per day (15k rows maybe) per IP address. 

## `aggregate.py` 
Takes the first CSV and reorganize the data into a new CSV `tag_frequency_by_month.csv` to easily make charts in Excel.  
Yes I'm old.
