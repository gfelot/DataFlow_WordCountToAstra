#WordCountToCassandra

Based on the word count for Apache Beam I rewrite some part of the code to be able to use gRPC API from the Stargate API and run it against a AstraDB instance.

> Don't forget to change the value for AstraDB in `src/WordCountToCassandra.java`

## Optimization
* Use of a Singleton to create the Stub instead of recreating it at every call
* Use of AsyncIO with GRPC instead of blocking
* Use of tweaked CassandraIO with AstraDB secure bundle to use the benefit of the native driver