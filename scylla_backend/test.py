from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import threading
import params
import json


def perform_queries():
    # scylla connection
    cluster = Cluster(['scylla'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS scyllakeyspace WITH replication = {'class': 'SimpleStrategy', "
                    "'replication_factor': 1}")
    session.execute("USE scyllakeyspace")

    queries_file = "queries.cql"
    with open(queries_file, "r") as file:
        queries = file.read().split(";")

    # Execute the queries
    for query in queries:
        if query.strip():
            result = session.execute(query)
            if result:
                print(f"Query: {query}")
                for row in result:
                    print(row)
                print()
