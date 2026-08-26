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
| RML-Core | 92                     |
| RML-IO   | 29                     |
| RML-CC   | 0                      |
| RML-FNML | 85                     |
| RML-STAR | 11                     |
| RML-LV   | 63                     |
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

This builds an executable jar `MappingWeaver-0.3.0.jar`.

## Running

To simply execute mappings and write output to standard out, run

```
java -jar MappingWeaver-0.3.0.jar -m <path-to-mapping-file>
```

A full list of options is displayed when running
```bash
java -jar MappingWeaver-0.3.0.jar --help
```

```
Usage: AlgeMapLoom [-hV] [--best-effort] [--custom-functions-only]
                   [--disable-local-parallel] [--json-ld]
                   [--auto-watermark-interval=<time (ms)>]
                   [--checkpoint-interval=<time (ms)>] [-i=<base IRI>] [-j=<job 
                   name>] [-p=<task slots>] [-f=<function descriptions>]...
                   [-m=<RML mapping file> | -l=<AlgeMapLoom mapping plan file>]
                   [-v | -vv | -vvv] [COMMAND]
      --auto-watermark-interval=<time (ms)>
                      If given, Flink's watermarking will be generated
                        periodically with the given interval. If not given, a
                        default value of 50ms will be used.This option is only
                        valid for DataStreams.
      --best-effort   If set, data errors yield no records instead of throwing
                        an exception.
      --checkpoint-interval=<time (ms)>
                      If given, Flink's checkpointing is enabled with the given
                        interval. If not given, checkpointing is enabled when
                        writing to a file (this is required to use the flink
                        StreamingFileSink). Otherwise, checkpointing is
                        disabled.

      --custom-functions-only
                      When set, only the descriptions provided via -f are used;
                        the built-in GREL/IDLab descriptions are excluded.
      --disable-local-parallel
                      By default input records are spread over the available
                        task slots within a task manager to optimise parallel
                        processing, at the cost of losing the order of the
                        records throughout the process. This option disables
                        this behaviour to guarantee that the output order is
                        the same as the input order.
  -f, --function-descriptions=<function descriptions>
                      An optional comma-separated list of paths to function
                        description files (in RDF using FnO). A path can be a
                        file location or a URL.
  -h, --help          Show this help message and exit.
  -i, --base-iri=<base IRI>
                      The base IRI as defined in the R2RML spec.
  -j, --job-name=<job name>
                      The name to assign to the job on the Flink cluster. Put
                        some semantics in here ;)
      --json-ld       Write the output as JSON-LD instead of N-Quads. An object
                        contains all RDF generated from one input record. Note:
                        this is slower than using the default N-Quads format.
  -l, --loom-file=<AlgeMapLoom mapping plan file>
                      The path to an AlgeMapLoom mapping plan file, in JSON
                        format. The path must be accessible on the Flink
                        cluster.
  -m, --mapping-file=<RML mapping file>
                      The path to an RML mapping file. The path must be
                        accessible on the Flink cluster.
  -p, --parallelism=<task slots>
                      Sets the maximum operator parallelism (~nr of task slots
                        used)
  -v                  Set log level to WARN
  -V, --version       Print version information and exit.
  -vv                 Set log level to INFO
  -vvv                Set log level to DEBUG
Commands:
  toFile       Write output to file
  toKafka      Write output to a Kafka topic
  toTCPSocket  Write output to a TCP socket
  toMQTT       Write output to an MQTT topic
  toWebSocket  Write output to a WebSocket endpoint
  noOutput     Do everything, but discard output
```

### Custom FnO function descriptions

MappingWeaver ships four built-in FnO description files (GREL and IDLab functions).
Additional descriptions can be provided with the `-f` flag:

```
java -jar MappingWeaver-0.3.0.jar -m mapping.ttl -f my-functions.ttl -f my-java-mapping.ttl
```

A custom description whose filename matches a built-in's name replaces that built-in.
When `--custom-functions-only` is set, no built-ins are loaded at all.

## Dependencies

|                             Dependency                             | License                     |
|:------------------------------------------------------------------:|-----------------------------|
|               org.apache.flink flink-streaming-java                | Apache License 2.0          |
|                   org.apache.flink flink-clients                   | Apache License 2.0          |
|               org.apache.flink flink-connector-base                | Apache License 2.0          |
|              org.apache.flink flink-connector-kafka                | Apache License 2.0          |
|              org.apache.flink flink-connector-files                | Apache License 2.0          |
|                      org.apache.jena jena-arq                      | Apache License 2.0          |
|          be.ugent.idlab.knows algebraic-mapping-operators          | unreleased                  |
|                  be.ugent.idlab.knows MappingLoom                  | unreleased                  |
|              be.ugent.idlab.knows function-agent-java              |                             |
|             be.ugent.idlab.knows idlab-functions-java              | MIT                         |
|                com.github.fnoio grel-functions-java                | MIT                         |
|                       org.slf4j slf4j-simple                       | MIT                         |
|                        org.slf4j slf4j-api                         | MIT                         |
|                       org.jspecify jspecify                        |                             |
|                    info.picocli picocli                           | Apache License 2.0          |
|                   org.eclipse.paho mqttv5.client                   | Eclipse Public License v2.0 |
|                  org.junit.jupiter junit-jupiter                   | Eclipse Public License v2.0 |
|               org.junit.jupiter junit-jupiter-params               | Eclipse Public License v2.0 | 

