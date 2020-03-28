create database kafka_tutorial;

USE kafka_tutorial;

DROP TABLE IF EXISTS persons;
CREATE TABLE persons
(
    id   int primary key auto_increment,
    name varchar(255),
    city varchar(255)
);

