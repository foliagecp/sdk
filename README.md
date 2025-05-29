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

### Health Status Check

1. Check that NATS server and Foliage runtime are running fine:
```sh
% docker ps

CONTAINER ID   IMAGE                      COMMAND                  CREATED          STATUS          PORTS                                                                    NAMES
...
b5a2deb84082   foliage-sdk-tests:latest   "/usr/bin/tests basic"   11 minutes ago   Up 11 minutes                                                                            tests-runtime-1
fac8d1bfef3a   nats:latest                "/nats-server -js -s…"   11 minutes ago   Up 11 minutes   0.0.0.0:4222->4222/tcp, 0.0.0.0:6222->6222/tcp, 0.0.0.0:8222->8222/tcp   tests-nats-1
``` 

2. Check that NATS server is running fine:
```sh
% docker logs tests-nats-1

...
[1] 2023/10/16 09:00:43.094325 [INF] Server is ready
```

3. Check that Foliage runtime runs without errors:
```sh
% docker logs tests-runtime-1 | grep "error" -i
```


### Running Tests

Foliage provides a set of test samples to help you get familiar with the platform. Follow these steps to run them:

#### 1. Navigate to `tests`:

```sh
cd tests
```

#### 2. Build the tests runtime:

```sh
docker-compose build
```

#### 3. Modify the `.env` file:

Customize the test environment by editing the `.env` file. For the basic test, find it at `./basic/.env`.

#### 4. Start the tests:

```sh
docker-compose up -d
```

To select a different test sample, set the TEST_NAME environment variable before running docker-compose up -d. The basic test sample starts by default.

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

- Learn to work with the graph store [here](./docs/graph_crud.md)
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

# Foliage CMDB Client

Этот клиент предоставляет удобный интерфейс для работы с Foliage Graph Store и CMDB API.

## Возможности

### Высокоуровневый CMDB API
- **Типы (Types)**: Создание, обновление, удаление и чтение типов объектов
- **Связи между типами**: Создание связей между различными типами
- **Объекты (Objects)**: Создание экземпляров типов с произвольными данными
- **Связи между объектами**: Создание связей между экземплярами объектов

### Низкоуровневый Graph API  
- **Вершины (Vertices)**: Прямые операции с вершинами графа
- **Связи (Links)**: Прямые операции со связями между вершинами

## Установка

```bash
go get github.com/foliagecp/sdk/clients/go/db
```

## Быстрый старт

### Создание клиента

```go
import "github.com/foliagecp/sdk/clients/go/db"

// Создание CMDB клиента
client, err := db.NewCMDBClient("nats://localhost:4222", 30, "hub")
if err != nil {
    log.Fatal(err)
}
```

### Создание типов

```go
// Создание типа "Server"
serverTypeBody := easyjson.NewJSONObject()
serverTypeBody.SetByPath("name", easyjson.NewJSON("Server"))
serverTypeBody.SetByPath("description", easyjson.NewJSON("Physical or virtual server"))

err := client.TypeCreate("server", serverTypeBody)
if err != nil {
    log.Fatal(err)
}

// Создание типа "Application"  
appTypeBody := easyjson.NewJSONObject()
appTypeBody.SetByPath("name", easyjson.NewJSON("Application"))
appTypeBody.SetByPath("description", easyjson.NewJSON("Software application"))

err = client.TypeCreate("application", appTypeBody)
if err != nil {
    log.Fatal(err)
}
```

### Создание связи между типами

```go
// Создание связи "Server -> Application" с типом "hosts"
linkBody := easyjson.NewJSONObject()
linkBody.SetByPath("relationship", easyjson.NewJSON("hosts"))
linkBody.SetByPath("description", easyjson.NewJSON("Server hosts application"))

err := client.TypesLinkCreate("server", "application", "hosts", linkBody, []string{"hosting"})
if err != nil {
    log.Fatal(err)
}
```

### Создание объектов

```go
// Создание объекта сервера
serverBody := easyjson.NewJSONObject()
serverBody.SetByPath("hostname", easyjson.NewJSON("web-server-01"))
serverBody.SetByPath("ip_address", easyjson.NewJSON("192.168.1.10"))
serverBody.SetByPath("os", easyjson.NewJSON("Ubuntu 22.04"))

err := client.ObjectCreate("web-server-01", "server", serverBody)
if err != nil {
    log.Fatal(err)
}

// Создание объекта приложения
appBody := easyjson.NewJSONObject()
appBody.SetByPath("name", easyjson.NewJSON("E-commerce Website"))
appBody.SetByPath("version", easyjson.NewJSON("2.1.0"))
appBody.SetByPath("language", easyjson.NewJSON("Python"))

err = client.ObjectCreate("ecommerce-web", "application", appBody)
if err != nil {
    log.Fatal(err)
}
```

### Создание связи между объектами

```go
// Создание связи между сервером и приложением
hostingBody := easyjson.NewJSONObject()
hostingBody.SetByPath("deployment_date", easyjson.NewJSON("2024-01-15"))
hostingBody.SetByPath("environment", easyjson.NewJSON("production"))

err := client.ObjectsLinkCreate("web-server-01", "ecommerce-web", "hosts", hostingBody, []string{"production"})
if err != nil {
    log.Fatal(err)
}
```

### Чтение данных

```go
// Чтение типа
typeData, err := client.TypeRead("server")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Type name: %s\n", typeData.GetByPath("body.name").AsStringDefault(""))

// Чтение объекта
objectData, err := client.ObjectRead("web-server-01")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Server hostname: %s\n", objectData.GetByPath("body.hostname").AsStringDefault(""))

// Чтение связи между объектами
linkData, err := client.ObjectsLinkRead("web-server-01", "ecommerce-web")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Environment: %s\n", linkData.GetByPath("body.environment").AsStringDefault(""))
```

## Полный пример

См. файл `example_usage.go` для полного примера создания типов, связей и объектов.

Пример демонстрирует:

1. **Создание двух типов**: Server и Application
2. **Создание связи между типами**: Server -> Application (hosts)
3. **Создание объектов каждого типа**: 
   - Серверы: web-server-01, db-server-01
   - Приложения: ecommerce-web, postgresql-db
4. **Создание связей между объектами**: следуя модели типов
5. **Проверку созданных данных**: чтение и верификация

## API Методы

### Операции с типами
- `TypeCreate(typeId, body)` - создание типа
- `TypeUpdate(typeId, body, upsert, replace)` - обновление типа
- `TypeDelete(typeId)` - удаление типа
- `TypeRead(typeId)` - чтение типа

### Операции со связями типов
- `TypesLinkCreate(fromType, toType, objectType, body, tags)` - создание связи типов
- `TypesLinkUpdate(fromType, toType, body, tags, upsert, replace)` - обновление связи типов
- `TypesLinkDelete(fromType, toType)` - удаление связи типов
- `TypesLinkRead(fromType, toType)` - чтение связи типов

### Операции с объектами
- `ObjectCreate(objectId, originType, body)` - создание объекта
- `ObjectUpdate(objectId, body, originType, upsert, replace)` - обновление объекта
- `ObjectDelete(objectId)` - удаление объекта
- `ObjectRead(objectId)` - чтение объекта

### Операции со связями объектов
- `ObjectsLinkCreate(fromObject, toObject, name, body, tags)` - создание связи объектов
- `ObjectsLinkUpdate(fromObject, toObject, name, body, tags, upsert, replace)` - обновление связи объектов
- `ObjectsLinkDelete(fromObject, toObject)` - удаление связи объектов
- `ObjectsLinkRead(fromObject, toObject)` - чтение связи объектов

### Низкоуровневые операции (наследуются от GraphSyncClient)
- `VertexCreate(id, body)` - создание вершины
- `VertexUpdate(id, body, replace, upsert)` - обновление вершины
- `VertexDelete(id)` - удаление вершины
- `VertexRead(id, details)` - чтение вершины
- `VerticesLinkCreate(from, to, linkName, linkType, tags, body)` - создание связи
- И другие операции со связями...

## Требования

- Go 1.18+
- NATS сервер
- Foliage runtime

## Запуск примера

```bash
go run example_usage.go
```

Убедитесь, что NATS сервер запущен на `localhost:4222` и Foliage runtime работает.
