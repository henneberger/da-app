# What is this?
A vibe-codable quickly-iterable full-featured end-to-end data pipline stuffed in a yaml file, deployable by a custom k8s operator.

My sub-goals:
- Get the AI model to be able to implement entire features with little-to-no intervention
- Able to deploy an entire data pipeline through k8s crd introspection

## Features
- Full-featured GraphQL server (Queries, Mutations, Subscriptions)
- Multi-database support (Postgres, DuckDB, MySQL, Elasticsearch, Kafka, Mongo, DynamoDB, Oracle)
- A Flink SQL runner
- GraphQL Subscription support via Kafka or Postgres Logical Replication
- Authentication/Authorization
- Python UDFs (Flink and GraphQL)
- Pagination w/ cursor signing
- Full JWT handling for web native deployments
- Postgres CDC support
- Observability (OpenLineage, OpenTelemetry, Prometheus, Zipkin, Jaeger)
- Iceberg Support
- Vector Support
- MCP support w/ Auth

## Example
[A Web Forum](samples/web-forum.yaml)

This is a reddit-styled web forum that:
- Supports Posts, comments, upvotes, flairs
- Event streaming for modlog, scoring, bot & abuse detection
- Streaming Updates
- Semantic Search
- AI summary for links