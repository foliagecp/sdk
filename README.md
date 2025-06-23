# Foliage SDK

<p align="center">
  <img src="./docs/pics/logo.png" width="600" alt="Foliage Logo">
</p>

[Foliage](https://www.foliage.dev/) is a collaborative application platform built upon a distributed graph database, providing a unified and extensible environment for effortless automation, cross-domain connectivity, and high-performance, edge-friendly runtimes.

[![License][License-Image]][License-Url] ![Lint][Lint-Status-Image-Url]

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[Lint-Status-Image-Url]: https://github.com/foliagecp/sdk/actions/workflows/golangci-lint.yml/badge.svg

## Table of Contents

- [Introduction](#introduction) <!-- omit in toc -->
- [Core Concepts](#core-concepts)
  - [Abstract](#abstract)
  - [Features](#features)
- [Getting Started](#getting-started)
  - [Minimum Requirements](#minimum-requirements)
  - [Installation](#installation)
  - [Health Status Check](#health-status-check)
  - [Running Tests](#running-tests)
  - [Customization](#customization)
- [Development](#development)
  - [Working with the SDK](#working-with-the-sdk)
- [Technology Stack](#technology-stack)
- [Roadmap](#roadmap)
- [References](#references)
- [License](#license)
- [Contribution](#contribution)

## Introduction

Foliage is an open-source collaborative platform that uses a distributed graph database, offering a unified, extensible environment for automation, cross-domain connectivity, and high-performance, edge-friendly runtimes. It provides a robust foundation for IoT solutions, automation workflows, and edge computing applications.

## Core Concepts

### Abstract

Foliage introduces abstraction, where knowledge about complex systems converges into a unified space, promoting transparent understanding and blurring the boundary between system models and the system itself.

![Abstract](./docs/pics/FoliageUnification.jpg)

### Features

Foliage promotes transparency, consistency, and clarity among system components by consolidating knowledge from diverse domains into a unified space. It reveals hidden dependencies, simplifying system evaluation and relationship management.

![Features](./docs/pics/FoliageSingleSpace.jpg)

Click [here](./docs/features.md) to see all features.

## Getting Started

### Minimum Requirements

**Native Install**

Foliage platform native install requirements correspond to the NATS Jetstream installations requirements listed here:  
https://docs.nats.io/running-a-nats-service/introduction/installation#with-jetstream

Same is for supported OS:  
https://docs.nats.io/running-a-nats-service/introduction/installation#with-jetstream

**Docker Container Install**

Foliage platform install via docker requires as minimal resources as docker engine itself:
https://docs.docker.com/desktop/install/linux-install/

### Installation

To begin using Foliage, clone the repository:

```sh
git clone https://github.com/foliagecp/sdk.git
```

For detailed installation instructions and prerequisites, visit the [official documentation](https://pkg.go.dev/github.com/foliagecp/sdk).

### Running Tests

Foliage provides a set of test samples to help you get familiar with the platform. For example, let's walk through the steps to run the simple test:

#### 1. Navigate to `tests/simple`:

```sh
cd tests/simple
```

#### 2. Build the test runtime:

```sh
docker-compose build
```

#### 3. Modify the `.env` file:

Customize the test environment by editing the `.env` file. For the basic test, find it at `./basic/simple/.env`.

#### 4. Start the tests:

```sh
docker-compose up -d
```

### Health Status Check

1. Check that NATS server and Foliage runtime are running fine:
```sh
% docker ps

CONTAINER ID   IMAGE                                    COMMAND                  CREATED         STATUS         PORTS                                                                    NAMES
03cb34c36948   natsio/prometheus-nats-exporter:latest   "/entrypoint.sh -var…"   3 seconds ago   Up 2 seconds   0.0.0.0:7777->7777/tcp                                                   simple-nats-exporter-1
aeb06fd7081d   natsio/nats-box:latest                   "/bin/sh"                3 seconds ago   Up 1 second                                                                             simple-io-1
93499788c231   foliage-sdk-tests-simple:latest          "/usr/bin/foliage -l…"   3 seconds ago   Up 1 second    0.0.0.0:8080->8080/tcp, 0.0.0.0:9901->9901/tcp                           simple-runtime-1
c6c89111834d   grafana/grafana-enterprise               "/run.sh"                3 seconds ago   Up 2 seconds   0.0.0.0:3000->3000/tcp                                                   simple-grafana-1
75db36f6d460   prom/prometheus                          "/bin/prometheus --c…"   3 seconds ago   Up 2 seconds   0.0.0.0:9090->9090/tcp                                                   simple-prometheus-1
6b7c67da49d2   nats:latest                              "/nats-server -c /et…"   3 seconds ago   Up 2 seconds   0.0.0.0:4222->4222/tcp, 0.0.0.0:6222->6222/tcp, 0.0.0.0:8222->8222/tcp   simple-nats-1
``` 

2. Check that NATS server is running fine:
```sh
% docker logs simple-nats-1

...
[1] 2023/10/16 09:00:43.094325 [INF] Server is ready
```

3. Check that Foliage runtime runs without errors:
```sh
% docker logs simple-runtime-1 | grep "error" -i
```

#### 5. Stop and clean up:

When you're done testing, stop and clean up the environment:

```sh
docker-compose down -v
```

### Customization

Explore available test samples and customize them to gain insights into Foliage's development principles. Refer to [basic test sample documentation](./docs/tests/basic.md).

For statefun logic definition, consider using plugins like [JavaScript](./docs/plugins/js.md).

## Development

### Working with the SDK

Use SDK To develop applications with Foliage:

```sh
go get github.com/foliagecp/sdk
```

- Learn to work with the graph [here](./docs/graph_crud.md)
- Learn to work with the object object [here](./docs/object_crud.md)
- Explore Foliage's JSON Path Graph Query Language (JPGQL) [here](./docs/jpgql.md)
- See how to visually debug your graph [here](./docs/graph_debug.md)
- Find out how to write your own application [here](./docs/how_to_write_an_application.md)
- Measure performance with guidance [here](./docs/performance_measures.md)

## Technology Stack

Foliage relies on a versatile technology stack that includes:

- Backend
  - Jetstream NATS
  - Key/Value Store NATS
  - WebSocket NATS
  - GoLang
  - JavaScript (V8)
- Frontend
  - React
  - TypeScript/JavaScript
  - WebSocket
- Common
  - Docker
  - Docker Compose

[Learn more about our technology choices](./docs/technologies_comparison.md).

## Roadmap

Check out our [Roadmap](./docs/pics/Roadmap.jpg) for more upcoming features and enhancements.

## References

- [Glossary](./docs/glossary.md)
- [Conventions](./docs/conventions.md)
- [External API](./docs/external_api.md) 

## License

Unless otherwise noted, the Foliage source files are distributed under the Apache Version 2.0 license found in the LICENSE file.

## Contribution

Foliage welcomes contributions from the open-source community. Join us in building a collaborative application platform that empowers developers worldwide!
