# Closest competitor technologies comparison

## Graph provider

The table below outlines the criteria that explain why the platform chose to create its own storage and graph processing system to meet its system requirements.

| Features                       | Neo4j                 | Dgraph                       | Amazon Neptune     | ArangoDB               | Foliage Graph Store    |
|--------------------------------|:---------------------:|:----------------------------:|:------------------:|:----------------------:|:----------------------:|
| Graph Storage                  | Yes                   | Yes                          | Yes                | Yes                    | Yes                    |
| Distributed Graph              | Yes (Enterprise only) | Yes                          | Yes                | Yes (>Community only)  | Yes                    |
| Persistence                    | Yes                   | Yes                          | Yes                | Yes                    | Yes                    |
| Backup & Restore               | Yes                   | Yes                          | Yes                | Yes                    | Yes                    |
| Additional Functionality       | Cypher                | GraphQL                      | SPARQL and Gremlin | JavaScript (V8) - Foxx | Foliage Statefuns      |
| Event Subscription (Triggers)  | Yes (Enterprise only) | Yes                          | Yes                | No                     | Yes                    |
| Functional Graph               | No                    | Yes                          | No                 | No                     | Yes                    |
| Lightweight                    | No (8GB RAM minimum)  | No (8GB RAM minimum)         | No                 | No (1GB RAM minimum)   | Yes (64MB RAM minimum) |
| ARM Non-64                     | No                    | Yes                          | Yes                | No                     | Yes                    |
| On-premise deploy              | Yes                   | Yes                          | No                 | Yes                    | Yes                    |
| Traditional graph modeling     | Yes                   | No                           | Yes                | Yes                    | Yes                    |
| Schema-related Constraints     | No                    | Properties and Relationships | No                 | No                     | No                     |

Query languages comparison is available [here.](./jpgql.md#comparison-with-other-graph-query-languages)

Currently, the utilization of the Foliage Graph Store is primarily suitable for the platform's internal requirements. However, there are plans to expand its capabilities and develop it into a standalone, fully-featured database product in the future.

### Backup, Restore, and Disaster Recovery

NATS Key/Value is based on NATS JetStream. To create a backup of a bucket from Key/Value, you must use its associated stream name. For more information on backups, restore, and disaster recovery in NATS JetStream, please refer to the [NATS JetStream documentation](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/disaster_recovery).

### Additional Functionality

Foliage Graph Store is built on NATS key/value, which means that both the storage for statefun's context and graph data are stored in a unified and consistent manner.

### Lightweight

ArangoDB's minimum hardware requirements are as follows: 
- RAM: 1 GB
- CPU: 2.2 GHz

In contrast, NATS can run on very small edge devices, such as Raspberry Pi, baseboard management controller, etc., and it requires no more than 32 MiB of RAM in a native installation.

## Functional layer provider

The table below outlines the criteria that explain why the platform chose to create its own stateful functions processing system to meet its system requirements.

| Features                             | Apache Flink Statefun  | Foliage Statefun (on NATS JetStream) | AWS Lambda                 |
|--------------------------------------|:----------------------:|:------------------------------------:|:--------------------------:|
| Stateful functions with context      | Yes                    | Yes                                  | No (ext. storage required) |
| Clustering and high-availability     | Yes                    | Yes                                  | Yes                        |
| Exactly once                         | Yes                    | Yes                                  | Yes                        |
| Independent statefun runtimes        | No                     | Yes                                  | Yes                        |
| Adding statefuns on the fly          | No                     | Yes                                  | Yes                        |
| Access other statefun's context      | No                     | Yes                                  | Yes                        |
| Lightweight                          | No                     | Yes                                  | Yes                        |
| No dependencies required             | No                     | Yes                                  | No (works with cloud)      |
| Easy functions cold start            | Yes                    | Yes                                  | No                         |

### Independent Statefun Runtimes

In Flink, when a statefun crashes, the whole statefun application crashes, causing a restart.

### Lightweight

Flink is a very heavyweight framework for corporate/business solutions, each component of which is written in Java. In contrast, NATS is a very lightweight messaging system written in GoLang.

### No Dependencies Required

Flink requires Kafka for ingress and egress. Kafka, in turn, requires Zookeeper.


