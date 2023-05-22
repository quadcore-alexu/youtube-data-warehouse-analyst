CREATE KEYSPACE Test
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

 CREATE TABLE User (
    id int PRIMARY KEY,
    name text,
    age int
 );

USE Test;
INSERT INTO User (id, name, age) VALUES (1, 'mariam', 23);
SELECT * FROM User;
