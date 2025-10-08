# MappingWeaver-java

A data-to-RDF mapping engine written in Java.

MappingWeaver converts data to RDF by processing [RML](https://github.com/kg-construct) or
[ShExML](https://shexml.herminiogarcia.com/spec/) mapping rules.
It depends on [AlgeMapLoom-rs](https://github.com/RMLio/algemaploom-rs) to translate
the mapping rules to an algebraic mapping plan.
It then constructs a pipeline of [Algebraic Mapping Operators](https://github.com/RMLio/Algebraic-Mapping-Operators)
to execute the plan on an embedded [Flink](https://flink.apache.org/) instance.

## Features

### Supported

#### Specs
This project aims to implement following specifications, but is work in progress:

| Spec     | Test cases passing (%) |
|----------|------------------------|
| RML-Core | 87                     |
| RML-IO   | 34                     |
| RML-CC   | 0                      |
| RML-FNML | 2                      |
| RML-STAR | 0                      |
| RML-LV   | 59                     |
| ShExML   | /                      |

#### Data formats
- CSV
- JSON
- XML

#### Data sources
- File
- Relational databases (PostgreSQL is tested, but in theory MySQL, OracleDB and MySQL also work)

#### Output targets
- File
- Kafka
- TCP socket
- MQTT

### Future

- Other data sources, formats
- Instructions on how to deploy on a Flink cluster

## Building

### Prerequisites

- Java JDK >= 21
- Maven >=3

### Command
To build an executable jar, run
```
mvn package
```
or
```
mvn -DskipTests package
```
to skip the tests.

This builds an executable jar `MappingWeaver-0.1.0.jar`.

## Running

To simply execute mappings and write output to standard out, run

```
java -jar MappingWeaver-0.1.0.jar -m <path-to-mapping-file>
```

A full list of options is displayed when running
```
java -jar MappingWeaver-0.1.0.jar --help
```

## Dependencies

|                             Dependency                             | License                     |
|:------------------------------------------------------------------:|-----------------------------|
|               org.apache.flink flink-streaming-java                | Apache License 2.0          |
|                   org.apache.flink flink-clients                   | Apache License 2.0          |
|               org.apache.flink flink-connector-base                | Apache License 2.0          |
|                      org.apache.jena jena-arq                      | Apache License 2.0          |
|          be.ugent.idlab.knows algebraic-mapping-operators          | unreleased                  |
|                  be.ugent.idlab.knows MappingLoom                  | unreleased                  |
|              be.ugent.idlab.knows function-agent-java              |                             |
|             be.ugent.idlab.knows idlab-functions-java              | MIT                         |
|                com.github.FnOio grel-functions-java                | MIT                         |
|                       org.slf4j slf4j-simple                       | MIT                         |
|                        org.slf4j slf4j-api                         | MIT                         |
|                       org.jspecify jspecify                        |                             |
|                   org.eclipse.paho mqttv5.client                   | Eclipse Public License v2.0 |
|                  org.junit.jupiter junit-jupiter                   | Eclipse Public License v2.0 |
|               org.junit.jupiter junit-jupiter-params               | Eclipse Public License v2.0 | 

