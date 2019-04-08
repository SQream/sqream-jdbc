# SQream JDBC Driver

This repository contains the SQream JDBC driver

## Getting the code

Clone the repository with:
```
git clone http://gitlab.sq.l/connectors/jdbc-driver.git
```

## Dependencies

This code is dependent on
- Java 8

## How to build
To build the driver as a JAR, use:

With tests (requires a SQream instance)
```
mvn package
```
Without tests
```
mvn package -DskipTests
```
With dependencies (fat JAR)
```
mvn -f pom-fat-jar.xml package
```

## How to install locally
```
mvn install
```
