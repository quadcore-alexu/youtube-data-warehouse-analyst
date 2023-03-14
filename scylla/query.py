from cassandra.cluster import Cluster

cluster = Cluster(['0.0.0.0'])
session = cluster.connect()

session.execute('USE Test')
result = session.execute("SELECT * FROM User;").one()
print(result.name, result.age)
