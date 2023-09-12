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
| Lightweight | No (1GB RAM is required) | Yes |
| ARM non-64 | No | Yes |

Query languages comparison is available [here.](./jpgql.md#comparison-with-other-graph-query-languages)

### Backup, Restore, and Disaster Recovery

NATS Key/Value is based on NATS JetStream. To create a backup of a bucket from Key/Value, you must use its associated stream name. For more information on backups, restore, and disaster recovery in NATS JetStream, please refer to the [NATS JetStream documentation](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/disaster_recovery).

### Additional Functionality

Foliage Graph DBMS is built on NATS key/value, which means that both the storage for statefun's context and graph data are stored in a unified and consistent manner.

### Lightweight

ArangoDB's minimum hardware requirements are as follows: 
- RAM: 1 GB
- CPU: 2.2 GHz

In contrast, NATS can run on very small edge devices, such as Raspberry Pi, baseboard management controller, etc., and it requires no more than 32 MiB of RAM in a native installation.

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

### Independent Statefun Runtimes

In Flink, when a statefun crashes, the whole statefun application crashes, causing a restart.

### Lightweight

Flink is a very heavyweight framework for corporate/business solutions, each component of which is written in Java. In contrast, NATS is a very lightweight messaging system written in GoLang.

### No Dependencies Required

Flink requires Kafka for ingress and egress. Kafka, in turn, requires Zookeeper.


