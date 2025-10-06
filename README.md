# AlgeMapLoom-java

The new mapping engine based
on [Algebraic Mapping Operators (AMO)](https://gitlab.ilabt.imec.be/rml/proc/algebraic-mapping-operators), written in
Java.

***

## Features

### Supported

- Local data sources:
    - CSV

### Future

- Local data sources:
    - JSON
    - XML
- Remote data sources

## Usage

## Notice: Alpha version

This library is dependent on its Rust counterpart and AMO, neither of which is released.

### Prerequisites

Users are recommended to install these libraries into their local Maven repository by executing `build_java.sh` in
AlgeMapLoon-rs and by running `mvn clean install -DskipTests` in their local version of AMO.

To execute `build_java.sh`, you'll need to have
[Rust toolchain](https://www.rust-lang.org/tools/install) installed.
For Linux-based users:

* Rust:
  <pre> bash curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs </pre>

For cross-compiling Windows binaries from Linux you need to install the MinGW cross-compiler and have the correct build targets installed:

* x86_64-unknown-linux-gnu
* x86_64-pc-windows-gnu

In case of issues during the execution of `build_java.sh`, make sure to check the log.txt file, located in the correct build target directory.

**IMPORTANT:**  
After these installations, copy the generated `.jar` file (without dependencies) from the `target` folder in `algemaploom-rs/src/java/algemaploom` to the `libs` folder in `algemaploom-java`.

### CLI

Following options are supported:

- `-m --mapping <arg>`: a path a mapping file to perform mapping with.

## Internals

This mapping engine utilizes the [Apache Flink framework](https://flink.apache.org/) for execution of the mapping file.
Internally, mapping files get parsed into a mapping plan using the Rust version of this
library, [AlgeMapLoom-rs](https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs).

This mapping plan consists of one or more operators from AMO, which are subsequently executed on a Flink cluster.

## Dependencies

|                    Dependency                    | License                     |
|:------------------------------------------------:|-----------------------------|
|      org.apache.flink flink-streaming-java       | Apache License 2.0          |
|          org.apache.flink flink-clients          | Apache License 2.0          |
|      org.apache.flink flink-connector-base       | Apache License 2.0          |
|             org.apache.jena jena-arq             | Apache License 2.0          |
| be.ugent.idlab.knows algebraic-mapping-operators | unreleased                  |
|         be.ugent.algemaploom algemaploom         | unreleased                  |
|           be.ugent.idlab.knows dataio            | MIT                         |
|              org.slf4j slf4j-simple              | MIT                         |
|               org.slf4j slf4j-api                | MIT                         |
|              org.jspecify jspecify               |                             |
|         org.junit.jupiter junit-jupiter          | Eclipse Public License v2.0 |
|      org.junit.jupiter junit-jupiter-params      | Eclipse Public License v2.0 | 

