# Closest alternative solutions comparison

## Graph provider

The table below outlines the features that explain why the platform chose to create its own storage and graph processing system to meet its system requirements.

| Features                          | Foliage Graph Store        | Neo4j                 | Dgraph                       | Amazon Neptune     | ArangoDB               |
|-----------------------------------|:--------------------------:|:---------------------:|:----------------------------:|:------------------:|:----------------------:|
| Graph Storage                     | **Yes**                    | Yes                   | Yes                          | Yes                | Yes                    |
| Distributed Graph                 | **Yes**                    | Yes (Enterprise only) | Yes                          | Yes                | Yes (>Community only)  |
| Persistence                       | **Yes**                    | Yes                   | Yes                          | Yes                | Yes                    |
| Backup & Restore                  | **Yes**                    | Yes                   | Yes                          | Yes                | Yes                    |
| Additional Functionality          | **Foliage Statefuns**      | Cypher                | GraphQL                      | SPARQL and Gremlin | JavaScript (V8) - Foxx |
| Event Subscription (Triggers)     | **Yes**                    | Yes (Enterprise only) | Yes                          | Yes                | No                     |
| Functional Graph                  | **Yes**                    | No                    | Yes                          | No                 | No                     |
| Lightweight                       | **Yes (64MB RAM minimum)** | No (8GB RAM minimum)  | No (8GB RAM minimum)         | No                 | No (1GB RAM minimum)   |
| ARM6, ARM7                        | **Yes**                    | No                    | Yes                          | Yes                | No                     |
| On-premise deploy                 | **Yes**                    | Yes                   | Yes                          | No                 | Yes                    |
| Traditional graph (links & edges) | **Yes**                    | Yes                   | No                           | Yes                | Yes                    |
| Schema-related Constraints        | **No**                     | No                    | Properties and Relationships | No                 | No                     |

Query languages comparison is available [here.](./jpgql.md#comparison-with-other-graph-query-languages)

Currently, the utilization of the Foliage Graph Store is primarily suitable for the platform's internal requirements. However, there are plans to expand its capabilities and develop it into a standalone, fully-featured database product in the future.

### Backup, Restore, and Disaster Recovery

NATS Key/Value is based on NATS JetStream. To create a backup of a bucket from Key/Value, you must use its associated stream name. For more information on backups, restore, and disaster recovery in NATS JetStream, please refer to the [NATS JetStream documentation](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/disaster_recovery).
You can also use `foliage_mgr.sh` that can make backup and restore.

### Additional Functionality

Foliage Graph Store is built on NATS key/value, which means that both the storage for statefun's context and graph data are stored in a unified and consistent manner.

### Lightweight

In contrast to other solutions, NATS server can run on very small edge devices, such as Raspberry Pi, baseboard management controller, etc., and it requires no more than 32 MiB of RAM in a native installation. More about NATS server installation requirements you can read [here](https://docs.nats.io/running-a-nats-service/introduction/installation).

## Functional layer provider

The table below outlines the features that explain why the platform chose to create its own stateful functions processing system to meet its system requirements.

| Features                             | Foliage Statefun | Apache Flink Statefun  | AWS Lambda                 |
|--------------------------------------|:----------------:|:----------------------:|:--------------------------:|
| Stateful functions with context      | **Yes**          | Yes                    | No (ext. storage required) |
| Clustering and high-availability     | **Yes**          | Yes                    | Yes                        |
| Exactly once                         | **Yes**          | Yes                    | Yes                        |
| Independent statefun runtimes        | **Yes**          | No                     | Yes                        |
| Adding statefuns on the fly          | **Yes**          | No                     | Yes                        |
| Access other statefun's context      | **Yes**          | No                     | Yes                        |
| Lightweight                          | **Yes**          | No                     | Yes                        |
| No dependencies required             | **Yes**          | No                     | No (works with cloud)      |
| Easy functions cold start            | **Yes**          | Yes                    | No                         |

### Independent Statefun Runtimes

In Flink, when a statefun crashes, the whole statefun application crashes, causing a restart.

### Lightweight

Flink is a very heavyweight framework for corporate/business solutions, each component of which is written in Java. In contrast, NATS is a very lightweight messaging system written in GoLang.

### No Dependencies Required

Flink requires Kafka for ingress and egress. Kafka, in turn, requires Zookeeper.


