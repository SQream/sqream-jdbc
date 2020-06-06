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

## How to upload artifact to maven repository manually
build jar
signed files
```
gpg -ab pom.xml
gpg -ab sqream-jdbc-<release_version>.jar
gpg -ab sqream-jdbc-<release_version>-javadoc.jar
gpg -ab  sqream-jdbc-<release_version>-sources.jar
```
package in single file
```
jar -cvf bundle.jar <files>
```
open nexus repository manager, upload bundle.jar and if test passed promote release.