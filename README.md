# SQream JDBC Driver

This repository contains the SQream JDBC driver

## Getting the code

Clone the repository with:
```
git clone https://github.com/helipilot50/SQream-JDBC.git
```

## Dependencies

This code is dependent on
- Java 8
- Jackson JSON library
- SQream JAVA-connector (clone and follow readme from http://gitlab.sq.l/connectors/java.git)

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
