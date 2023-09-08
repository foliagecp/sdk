# Closest competitor technologies comparison

## Graph database management system
| Features | ArangoDB | Foliage Graph DBMS (on NATS Key/Value) |
|----------|:-------------:|:------:|
| Graph storage |  Yes | Yes |
| Distributed graph |    >Community only   | Yes |
| Persistance | Yes | Yes |
| Backup & Restore & Disaster recovery | Yes | Yes |
| Additional functionality | JavaScript (V8) - Foxx | Foliage Statefuns |
| Event subscription (triggers) | No | Yes |
| Functional graph | No | Yes |
| Lightweight | No | Yes |
| ARM non-64 | No | Yes |

Query languages comparison is available [here.](./jpgql.md#comparison-with-other-graph-query-languages)

### Backup & Restore & Disaster recovery
NATS Key/Value is based on NATS JetStream. To make a backup of a bucket from KV its stream name must be used. More about backups & restore & disaster recrovey in NATS JetStream:  
https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/disaster_recovery

### Additional functionality
Foliage Graph DBMS is based on NATS key/value so does the storage for statefun's context, therefore statefun's context and graph data are stored in one place in a uniform way.

### Lightweight
ArangoDB's minimum hardware requirements are: RAM: 1 GB; CPU : 2.2 GHz. While NATS can run on very small edge devices like RPI, baseboard management controller, etc. and requires no more than 32MiB of RAM in native installation.

## Functional layer provider
| Features | Apache Flink Statefun | Foliage Statefun (on NATS JetStream) |
|----------|:-------------:|:------:|
| Persistent statefun context | Yes | Yes |
| Clustering and high-availability | Yes | Yes |
| Exactly once | Yes | Yes |
| Independent statefun runtimes | No | Yes |
| Adding statefuns on the fly | No | Yes |
| Access other statefun's context | No | Yes |
| Lightweight | No | Yes |
| No dependencies required | No | Yes |

### Independent statefun runtimes
In Flink when a statefun crashes the whole statefun application crashes causing a restart.

### Lightweight
Flink is a very heavyweight framework for corporate/business solutions each component of which is wirtten on Java. While NATS is a very lightweight messaging system written on GoLang.

### No dependencies required
Flink requires Kafka for ingress and egress. Kafka in its turn requires Zookeeper.

