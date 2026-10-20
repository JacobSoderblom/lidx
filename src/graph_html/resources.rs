//! Resources an architecture diagram shows besides the code: databases,
//! caches, message brokers, secret and config stores, object storage and
//! external systems, found from what the repository says about them:
//!
//! - packages a manifest declares and modules a source file imports
//!   (`Npgsql`, `import redis`, `require('kafkajs')`, `"cloud.google.com/go/pubsub"`);
//! - infrastructure files: Bicep and Terraform resource types, CloudFormation
//!   and SAM resources, Kubernetes objects, compose services and their images,
//!   Helm chart dependencies, Serverless Framework events, a .NET Aspire app
//!   host's `Add*` calls and a Prisma datasource;
//! - edges the index holds: topics published and subscribed, secrets read,
//!   SQL objects referenced, HTTP calls to an absolute URL outside the repo.
//!
//! Every resource and every use carries its evidence (file, line, text). A
//! category with no named technology stays neutral ("message bus"); nothing
//! is named that the repository does not name.

use super::apps::{LinkView, UnitUse, short_key};
use super::build::{RepoFile, RepoRow};
use super::units::{Evidence, Kind, Owners, Unit};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// What a resource is for, as a C4 container diagram groups it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cat {
    Db,
    Cache,
    Search,
    Bus,
    Secrets,
    Config,
    Storage,
    Ext,
}

impl Cat {
    pub fn as_str(self) -> &'static str {
        match self {
            Cat::Db => "db",
            Cat::Cache => "cache",
            Cat::Search => "search",
            Cat::Bus => "bus",
            Cat::Secrets => "secrets",
            Cat::Config => "config",
            Cat::Storage => "storage",
            Cat::Ext => "ext",
        }
    }

    /// The neutral name of a resource whose technology is not known.
    pub fn neutral(self) -> &'static str {
        match self {
            Cat::Db => "database",
            Cat::Cache => "cache",
            Cat::Search => "search index",
            Cat::Bus => "message bus",
            Cat::Secrets => "secret store",
            Cat::Config => "config store",
            Cat::Storage => "object storage",
            Cat::Ext => "external system",
        }
    }

    /// The noun a card shows under the name.
    pub fn noun(self) -> &'static str {
        match self {
            Cat::Db => "database",
            Cat::Cache => "cache",
            Cat::Search => "search",
            Cat::Bus => "messaging",
            Cat::Secrets => "secrets",
            Cat::Config => "config store",
            Cat::Storage => "storage",
            Cat::Ext => "external",
        }
    }

    /// Lane on the map: data and messaging, or external systems.
    pub fn lane(self) -> &'static str {
        if self == Cat::Ext { "ext" } else { "data" }
    }

    /// The family of a link to it, for its colour on the map.
    pub fn family(self) -> &'static str {
        match self {
            Cat::Db | Cat::Cache | Cat::Search => "data",
            Cat::Bus => "msg",
            Cat::Secrets | Cat::Config => "secret",
            Cat::Storage => "store",
            Cat::Ext => "ext",
        }
    }
}

/// `(pattern, category, technology)`: a pattern matches a package name or an
/// imported module, compared without case; a trailing `*` matches a prefix.
/// An empty technology names the category only (an ORM, an umbrella SDK).
const PACKAGES: &[(&str, Cat, &str)] = &[
    // PostgreSQL
    ("pg", Cat::Db, "PostgreSQL"),
    ("pg-promise", Cat::Db, "PostgreSQL"),
    ("postgres", Cat::Db, "PostgreSQL"),
    ("@neondatabase/serverless", Cat::Db, "PostgreSQL"),
    ("@vercel/postgres", Cat::Db, "PostgreSQL"),
    ("psycopg", Cat::Db, "PostgreSQL"),
    ("psycopg2", Cat::Db, "PostgreSQL"),
    ("psycopg2-binary", Cat::Db, "PostgreSQL"),
    ("psycopg_pool", Cat::Db, "PostgreSQL"),
    ("asyncpg", Cat::Db, "PostgreSQL"),
    ("npgsql*", Cat::Db, "PostgreSQL"),
    ("aspire.npgsql*", Cat::Db, "PostgreSQL"),
    ("github.com/jackc/pgx*", Cat::Db, "PostgreSQL"),
    ("github.com/lib/pq", Cat::Db, "PostgreSQL"),
    ("gorm.io/driver/postgres", Cat::Db, "PostgreSQL"),
    ("tokio-postgres", Cat::Db, "PostgreSQL"),
    ("tokio_postgres", Cat::Db, "PostgreSQL"),
    ("cloud.google.com/go/alloydbconn*", Cat::Db, "AlloyDB"),
    // MySQL
    ("mysql", Cat::Db, "MySQL"),
    ("mysql2", Cat::Db, "MySQL"),
    ("pymysql", Cat::Db, "MySQL"),
    ("mysqlclient", Cat::Db, "MySQL"),
    ("aiomysql", Cat::Db, "MySQL"),
    ("mysqlconnector*", Cat::Db, "MySQL"),
    ("mysql.data*", Cat::Db, "MySQL"),
    ("pomelo.entityframeworkcore.mysql*", Cat::Db, "MySQL"),
    ("github.com/go-sql-driver/mysql", Cat::Db, "MySQL"),
    ("gorm.io/driver/mysql", Cat::Db, "MySQL"),
    ("mysql_async", Cat::Db, "MySQL"),
    // SQL Server
    ("mssql", Cat::Db, "SQL Server"),
    ("tedious", Cat::Db, "SQL Server"),
    ("pymssql", Cat::Db, "SQL Server"),
    ("microsoft.data.sqlclient*", Cat::Db, "SQL Server"),
    ("system.data.sqlclient*", Cat::Db, "SQL Server"),
    (
        "microsoft.entityframeworkcore.sqlserver*",
        Cat::Db,
        "SQL Server",
    ),
    ("aspire.microsoft.data.sqlclient*", Cat::Db, "SQL Server"),
    (
        "aspire.microsoft.entityframeworkcore.sqlserver*",
        Cat::Db,
        "SQL Server",
    ),
    ("github.com/microsoft/go-mssqldb", Cat::Db, "SQL Server"),
    ("github.com/denisenko/mssqldb", Cat::Db, "SQL Server"),
    ("tiberius", Cat::Db, "SQL Server"),
    // SQLite
    ("sqlite3", Cat::Db, "SQLite"),
    ("better-sqlite3", Cat::Db, "SQLite"),
    ("aiosqlite", Cat::Db, "SQLite"),
    ("microsoft.data.sqlite*", Cat::Db, "SQLite"),
    ("microsoft.entityframeworkcore.sqlite*", Cat::Db, "SQLite"),
    ("github.com/mattn/go-sqlite3", Cat::Db, "SQLite"),
    ("modernc.org/sqlite", Cat::Db, "SQLite"),
    ("rusqlite", Cat::Db, "SQLite"),
    // Document, wide-column and analytical stores
    ("mongodb", Cat::Db, "MongoDB"),
    ("mongoose", Cat::Db, "MongoDB"),
    ("pymongo", Cat::Db, "MongoDB"),
    ("motor", Cat::Db, "MongoDB"),
    ("mongodb.driver*", Cat::Db, "MongoDB"),
    ("mongodb.bson", Cat::Db, "MongoDB"),
    ("go.mongodb.org/mongo-driver*", Cat::Db, "MongoDB"),
    ("cassandra-driver", Cat::Db, "Cassandra"),
    ("cassandra", Cat::Db, "Cassandra"),
    ("github.com/gocql/gocql", Cat::Db, "Cassandra"),
    ("@aws-sdk/client-dynamodb", Cat::Db, "DynamoDB"),
    ("@aws-sdk/lib-dynamodb", Cat::Db, "DynamoDB"),
    ("awssdk.dynamodbv2", Cat::Db, "DynamoDB"),
    ("amazon.dynamodbv2*", Cat::Db, "DynamoDB"),
    (
        "github.com/aws/aws-sdk-go-v2/service/dynamodb*",
        Cat::Db,
        "DynamoDB",
    ),
    (
        "github.com/aws/aws-sdk-go/service/dynamodb*",
        Cat::Db,
        "DynamoDB",
    ),
    ("@azure/cosmos", Cat::Db, "Cosmos DB"),
    ("azure-cosmos", Cat::Db, "Cosmos DB"),
    ("azure.cosmos*", Cat::Db, "Cosmos DB"),
    ("microsoft.azure.cosmos*", Cat::Db, "Cosmos DB"),
    (
        "microsoft.entityframeworkcore.cosmos*",
        Cat::Db,
        "Cosmos DB",
    ),
    ("@google-cloud/firestore", Cat::Db, "Firestore"),
    ("google-cloud-firestore", Cat::Db, "Firestore"),
    ("google.cloud.firestore*", Cat::Db, "Firestore"),
    ("cloud.google.com/go/firestore*", Cat::Db, "Firestore"),
    ("@google-cloud/spanner", Cat::Db, "Spanner"),
    ("google-cloud-spanner", Cat::Db, "Spanner"),
    ("google.cloud.spanner*", Cat::Db, "Spanner"),
    ("cloud.google.com/go/spanner*", Cat::Db, "Spanner"),
    ("@google-cloud/bigquery", Cat::Db, "BigQuery"),
    ("google-cloud-bigquery", Cat::Db, "BigQuery"),
    ("google.cloud.bigquery*", Cat::Db, "BigQuery"),
    ("cloud.google.com/go/bigquery*", Cat::Db, "BigQuery"),
    ("neo4j", Cat::Db, "Neo4j"),
    ("neo4j-driver", Cat::Db, "Neo4j"),
    ("neo4j.driver*", Cat::Db, "Neo4j"),
    ("@clickhouse/client", Cat::Db, "ClickHouse"),
    ("clickhouse-connect", Cat::Db, "ClickHouse"),
    ("clickhouse_connect", Cat::Db, "ClickHouse"),
    ("@supabase/supabase-js", Cat::Db, "Supabase"),
    ("supabase", Cat::Db, "Supabase"),
    // ORMs and query builders: a database, its kind from elsewhere.
    ("@prisma/client", Cat::Db, ""),
    ("prisma", Cat::Db, ""),
    ("drizzle-orm", Cat::Db, ""),
    ("typeorm", Cat::Db, ""),
    ("sequelize", Cat::Db, ""),
    ("knex", Cat::Db, ""),
    ("kysely", Cat::Db, ""),
    ("sqlalchemy", Cat::Db, ""),
    ("sqlmodel", Cat::Db, ""),
    ("alembic", Cat::Db, ""),
    ("peewee", Cat::Db, ""),
    ("tortoise-orm", Cat::Db, ""),
    ("tortoise", Cat::Db, ""),
    ("microsoft.entityframeworkcore", Cat::Db, ""),
    ("dapper", Cat::Db, ""),
    ("gorm.io/gorm", Cat::Db, ""),
    ("github.com/jmoiron/sqlx", Cat::Db, ""),
    ("database/sql", Cat::Db, ""),
    ("diesel", Cat::Db, ""),
    ("sqlx", Cat::Db, ""),
    ("sea-orm", Cat::Db, ""),
    ("sea_orm", Cat::Db, ""),
    // Caches
    ("redis", Cat::Cache, "Redis"),
    ("ioredis", Cat::Cache, "Redis"),
    ("@redis/client", Cat::Cache, "Redis"),
    ("@upstash/redis", Cat::Cache, "Redis"),
    ("aioredis", Cat::Cache, "Redis"),
    ("redis-py-cluster", Cat::Cache, "Redis"),
    ("stackexchange.redis*", Cat::Cache, "Redis"),
    (
        "microsoft.extensions.caching.stackexchangeredis",
        Cat::Cache,
        "Redis",
    ),
    ("aspire.stackexchange.redis*", Cat::Cache, "Redis"),
    ("github.com/redis/go-redis*", Cat::Cache, "Redis"),
    ("github.com/go-redis/redis*", Cat::Cache, "Redis"),
    ("github.com/gomodule/redigo*", Cat::Cache, "Redis"),
    ("memcached", Cat::Cache, "Memcached"),
    ("pymemcache", Cat::Cache, "Memcached"),
    ("enyimmemcachedcore", Cat::Cache, "Memcached"),
    ("github.com/bradfitz/gomemcache*", Cat::Cache, "Memcached"),
    // Search
    ("@elastic/elasticsearch", Cat::Search, "Elasticsearch"),
    ("elasticsearch", Cat::Search, "Elasticsearch"),
    (
        "elastic.clients.elasticsearch*",
        Cat::Search,
        "Elasticsearch",
    ),
    (
        "github.com/elastic/go-elasticsearch*",
        Cat::Search,
        "Elasticsearch",
    ),
    ("@opensearch-project/opensearch", Cat::Search, "OpenSearch"),
    ("opensearch-py", Cat::Search, "OpenSearch"),
    ("opensearchpy", Cat::Search, "OpenSearch"),
    ("meilisearch", Cat::Search, "Meilisearch"),
    ("algoliasearch", Cat::Search, "Algolia"),
    ("@qdrant/js-client-rest", Cat::Search, "Qdrant"),
    ("qdrant-client", Cat::Search, "Qdrant"),
    ("qdrant_client", Cat::Search, "Qdrant"),
    ("@pinecone-database/pinecone", Cat::Search, "Pinecone"),
    ("pinecone", Cat::Search, "Pinecone"),
    ("azure.search.documents*", Cat::Search, "Azure AI Search"),
    ("@azure/search-documents", Cat::Search, "Azure AI Search"),
    ("azure-search-documents", Cat::Search, "Azure AI Search"),
    // Message brokers
    ("amqplib", Cat::Bus, "RabbitMQ"),
    ("amqp-connection-manager", Cat::Bus, "RabbitMQ"),
    ("pika", Cat::Bus, "RabbitMQ"),
    ("aio-pika", Cat::Bus, "RabbitMQ"),
    ("aio_pika", Cat::Bus, "RabbitMQ"),
    ("rabbitmq.client*", Cat::Bus, "RabbitMQ"),
    ("aspire.rabbitmq.client*", Cat::Bus, "RabbitMQ"),
    ("masstransit.rabbitmq*", Cat::Bus, "RabbitMQ"),
    ("easynetq*", Cat::Bus, "RabbitMQ"),
    ("github.com/rabbitmq/amqp091-go", Cat::Bus, "RabbitMQ"),
    ("github.com/streadway/amqp", Cat::Bus, "RabbitMQ"),
    ("lapin", Cat::Bus, "RabbitMQ"),
    ("kafkajs", Cat::Bus, "Kafka"),
    ("@confluentinc/kafka-javascript", Cat::Bus, "Kafka"),
    ("node-rdkafka", Cat::Bus, "Kafka"),
    ("kafka-python", Cat::Bus, "Kafka"),
    ("kafka", Cat::Bus, "Kafka"),
    ("confluent-kafka", Cat::Bus, "Kafka"),
    ("confluent_kafka", Cat::Bus, "Kafka"),
    ("aiokafka", Cat::Bus, "Kafka"),
    ("confluent.kafka*", Cat::Bus, "Kafka"),
    ("aspire.confluent.kafka*", Cat::Bus, "Kafka"),
    ("masstransit.kafka*", Cat::Bus, "Kafka"),
    ("github.com/segmentio/kafka-go*", Cat::Bus, "Kafka"),
    (
        "github.com/confluentinc/confluent-kafka-go*",
        Cat::Bus,
        "Kafka",
    ),
    ("github.com/ibm/sarama*", Cat::Bus, "Kafka"),
    ("github.com/shopify/sarama*", Cat::Bus, "Kafka"),
    ("github.com/twmb/franz-go*", Cat::Bus, "Kafka"),
    ("rdkafka", Cat::Bus, "Kafka"),
    ("@azure/service-bus", Cat::Bus, "Azure Service Bus"),
    ("azure-servicebus", Cat::Bus, "Azure Service Bus"),
    ("azure.servicebus*", Cat::Bus, "Azure Service Bus"),
    ("azure.messaging.servicebus*", Cat::Bus, "Azure Service Bus"),
    (
        "aspire.azure.messaging.servicebus*",
        Cat::Bus,
        "Azure Service Bus",
    ),
    ("microsoft.azure.servicebus*", Cat::Bus, "Azure Service Bus"),
    (
        "microsoft.azure.webjobs.extensions.servicebus*",
        Cat::Bus,
        "Azure Service Bus",
    ),
    (
        "masstransit.azure.servicebus*",
        Cat::Bus,
        "Azure Service Bus",
    ),
    (
        "github.com/azure/azure-sdk-for-go/sdk/messaging/azservicebus*",
        Cat::Bus,
        "Azure Service Bus",
    ),
    ("@azure/event-hubs", Cat::Bus, "Event Hubs"),
    ("azure-eventhub", Cat::Bus, "Event Hubs"),
    ("azure.eventhub*", Cat::Bus, "Event Hubs"),
    ("azure.messaging.eventhubs*", Cat::Bus, "Event Hubs"),
    ("@azure/eventgrid", Cat::Bus, "Event Grid"),
    ("azure.messaging.eventgrid*", Cat::Bus, "Event Grid"),
    ("@aws-sdk/client-sqs", Cat::Bus, "Amazon SQS"),
    ("awssdk.sqs", Cat::Bus, "Amazon SQS"),
    ("amazon.sqs*", Cat::Bus, "Amazon SQS"),
    (
        "github.com/aws/aws-sdk-go-v2/service/sqs*",
        Cat::Bus,
        "Amazon SQS",
    ),
    (
        "github.com/aws/aws-sdk-go/service/sqs*",
        Cat::Bus,
        "Amazon SQS",
    ),
    ("@aws-sdk/client-sns", Cat::Bus, "Amazon SNS"),
    ("awssdk.simplenotificationservice", Cat::Bus, "Amazon SNS"),
    (
        "github.com/aws/aws-sdk-go-v2/service/sns*",
        Cat::Bus,
        "Amazon SNS",
    ),
    (
        "github.com/aws/aws-sdk-go/service/sns*",
        Cat::Bus,
        "Amazon SNS",
    ),
    ("@aws-sdk/client-kinesis", Cat::Bus, "Kinesis"),
    ("@aws-sdk/client-eventbridge", Cat::Bus, "EventBridge"),
    ("@google-cloud/pubsub", Cat::Bus, "Pub/Sub"),
    ("google-cloud-pubsub", Cat::Bus, "Pub/Sub"),
    ("google.cloud.pubsub*", Cat::Bus, "Pub/Sub"),
    ("google.cloud.pubsub_v1*", Cat::Bus, "Pub/Sub"),
    ("cloud.google.com/go/pubsub*", Cat::Bus, "Pub/Sub"),
    ("nats", Cat::Bus, "NATS"),
    ("nats-py", Cat::Bus, "NATS"),
    ("nats.client*", Cat::Bus, "NATS"),
    ("nats.net*", Cat::Bus, "NATS"),
    ("github.com/nats-io/nats.go*", Cat::Bus, "NATS"),
    ("async-nats", Cat::Bus, "NATS"),
    ("async_nats", Cat::Bus, "NATS"),
    ("mqtt", Cat::Bus, "MQTT"),
    ("paho-mqtt", Cat::Bus, "MQTT"),
    ("paho.mqtt*", Cat::Bus, "MQTT"),
    ("mqttnet*", Cat::Bus, "MQTT"),
    ("bullmq", Cat::Bus, "BullMQ"),
    ("bull", Cat::Bus, "BullMQ"),
    ("celery", Cat::Bus, "Celery"),
    ("pulsar-client", Cat::Bus, "Pulsar"),
    ("pulsar", Cat::Bus, "Pulsar"),
    ("zeromq", Cat::Bus, "ZeroMQ"),
    ("pyzmq", Cat::Bus, "ZeroMQ"),
    ("zmq", Cat::Bus, "ZeroMQ"),
    // Messaging libraries whose transport is configured elsewhere.
    ("masstransit", Cat::Bus, ""),
    ("nservicebus", Cat::Bus, ""),
    ("rebus", Cat::Bus, ""),
    ("dapr.client", Cat::Bus, ""),
    // Secret and config stores
    ("@azure/keyvault-secrets", Cat::Secrets, "Azure Key Vault"),
    ("azure-keyvault-secrets", Cat::Secrets, "Azure Key Vault"),
    ("azure.keyvault.secrets*", Cat::Secrets, "Azure Key Vault"),
    (
        "azure.security.keyvault.secrets*",
        Cat::Secrets,
        "Azure Key Vault",
    ),
    (
        "azure.extensions.aspnetcore.configuration.secrets",
        Cat::Secrets,
        "Azure Key Vault",
    ),
    (
        "aspire.azure.security.keyvault*",
        Cat::Secrets,
        "Azure Key Vault",
    ),
    (
        "github.com/azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets*",
        Cat::Secrets,
        "Azure Key Vault",
    ),
    (
        "@aws-sdk/client-secrets-manager",
        Cat::Secrets,
        "AWS Secrets Manager",
    ),
    ("awssdk.secretsmanager", Cat::Secrets, "AWS Secrets Manager"),
    (
        "github.com/aws/aws-sdk-go-v2/service/secretsmanager*",
        Cat::Secrets,
        "AWS Secrets Manager",
    ),
    (
        "@google-cloud/secret-manager",
        Cat::Secrets,
        "Secret Manager",
    ),
    (
        "google-cloud-secret-manager",
        Cat::Secrets,
        "Secret Manager",
    ),
    (
        "google.cloud.secretmanager*",
        Cat::Secrets,
        "Secret Manager",
    ),
    (
        "cloud.google.com/go/secretmanager*",
        Cat::Secrets,
        "Secret Manager",
    ),
    ("hvac", Cat::Secrets, "Vault"),
    ("node-vault", Cat::Secrets, "Vault"),
    ("vaultsharp*", Cat::Secrets, "Vault"),
    ("github.com/hashicorp/vault/api*", Cat::Secrets, "Vault"),
    (
        "microsoft.extensions.configuration.azureappconfiguration*",
        Cat::Config,
        "Azure App Configuration",
    ),
    (
        "@azure/app-configuration",
        Cat::Config,
        "Azure App Configuration",
    ),
    (
        "azure-appconfiguration",
        Cat::Config,
        "Azure App Configuration",
    ),
    ("@aws-sdk/client-ssm", Cat::Config, "SSM Parameter Store"),
    (
        "awssdk.simplesystemsmanagement",
        Cat::Config,
        "SSM Parameter Store",
    ),
    // Object storage
    ("@azure/storage-blob", Cat::Storage, "Azure Blob Storage"),
    ("azure-storage-blob", Cat::Storage, "Azure Blob Storage"),
    ("azure.storage.blob*", Cat::Storage, "Azure Blob Storage"),
    ("azure.storage.blobs*", Cat::Storage, "Azure Blob Storage"),
    (
        "azure.storage.files.datalake*",
        Cat::Storage,
        "Azure Blob Storage",
    ),
    (
        "aspire.azure.storage.blobs*",
        Cat::Storage,
        "Azure Blob Storage",
    ),
    (
        "github.com/azure/azure-sdk-for-go/sdk/storage/azblob*",
        Cat::Storage,
        "Azure Blob Storage",
    ),
    ("@aws-sdk/client-s3", Cat::Storage, "Amazon S3"),
    ("@aws-sdk/s3-request-presigner", Cat::Storage, "Amazon S3"),
    ("@aws-sdk/lib-storage", Cat::Storage, "Amazon S3"),
    ("awssdk.s3", Cat::Storage, "Amazon S3"),
    ("amazon.s3*", Cat::Storage, "Amazon S3"),
    (
        "github.com/aws/aws-sdk-go-v2/service/s3*",
        Cat::Storage,
        "Amazon S3",
    ),
    (
        "github.com/aws/aws-sdk-go/service/s3*",
        Cat::Storage,
        "Amazon S3",
    ),
    ("aws-sdk-s3", Cat::Storage, "Amazon S3"),
    ("aws_sdk_s3", Cat::Storage, "Amazon S3"),
    ("@google-cloud/storage", Cat::Storage, "Cloud Storage"),
    ("google-cloud-storage", Cat::Storage, "Cloud Storage"),
    ("google.cloud.storage*", Cat::Storage, "Cloud Storage"),
    (
        "cloud.google.com/go/storage*",
        Cat::Storage,
        "Cloud Storage",
    ),
    ("minio", Cat::Storage, "MinIO"),
    ("github.com/minio/minio-go*", Cat::Storage, "MinIO"),
    // Umbrella cloud SDKs: the service is read from the code.
    ("aws-sdk", Cat::Ext, ""),
    ("boto3", Cat::Ext, ""),
    // External services
    ("stripe", Cat::Ext, "Stripe"),
    ("stripe.net", Cat::Ext, "Stripe"),
    ("github.com/stripe/stripe-go*", Cat::Ext, "Stripe"),
    ("openai", Cat::Ext, "OpenAI"),
    ("azure.ai.openai*", Cat::Ext, "OpenAI"),
    ("@azure/openai", Cat::Ext, "OpenAI"),
    ("github.com/sashabaranov/go-openai", Cat::Ext, "OpenAI"),
    ("github.com/openai/openai-go*", Cat::Ext, "OpenAI"),
    ("@anthropic-ai/sdk", Cat::Ext, "Anthropic"),
    ("anthropic", Cat::Ext, "Anthropic"),
    ("@google/generative-ai", Cat::Ext, "Gemini"),
    ("@google/genai", Cat::Ext, "Gemini"),
    ("google-generativeai", Cat::Ext, "Gemini"),
    ("google.generativeai*", Cat::Ext, "Gemini"),
    ("google-genai", Cat::Ext, "Gemini"),
    ("google.genai*", Cat::Ext, "Gemini"),
    ("@google-cloud/vertexai", Cat::Ext, "Vertex AI"),
    ("google-cloud-aiplatform", Cat::Ext, "Vertex AI"),
    ("vertexai", Cat::Ext, "Vertex AI"),
    ("twilio", Cat::Ext, "Twilio"),
    ("@sendgrid/mail", Cat::Ext, "SendGrid"),
    ("sendgrid", Cat::Ext, "SendGrid"),
    ("resend", Cat::Ext, "Resend"),
    ("postmark", Cat::Ext, "Postmark"),
    ("mailgun.js", Cat::Ext, "Mailgun"),
    ("nodemailer", Cat::Ext, "SMTP server"),
    ("mailkit", Cat::Ext, "SMTP server"),
    ("smtplib", Cat::Ext, "SMTP server"),
    ("aiosmtplib", Cat::Ext, "SMTP server"),
    ("emails", Cat::Ext, "SMTP server"),
    ("lettre", Cat::Ext, "SMTP server"),
    ("@slack/web-api", Cat::Ext, "Slack"),
    ("@slack/bolt", Cat::Ext, "Slack"),
    ("slack_sdk", Cat::Ext, "Slack"),
    ("slack-sdk", Cat::Ext, "Slack"),
    ("firebase-admin", Cat::Ext, "Firebase"),
    ("firebase_admin", Cat::Ext, "Firebase"),
    ("@sentry/*", Cat::Ext, "Sentry"),
    ("sentry-sdk", Cat::Ext, "Sentry"),
    ("sentry_sdk", Cat::Ext, "Sentry"),
    ("sentry.aspnetcore", Cat::Ext, "Sentry"),
    ("github.com/getsentry/sentry-go", Cat::Ext, "Sentry"),
    (
        "microsoft.applicationinsights*",
        Cat::Ext,
        "Application Insights",
    ),
    (
        "azure.monitor.opentelemetry*",
        Cat::Ext,
        "Application Insights",
    ),
    ("applicationinsights", Cat::Ext, "Application Insights"),
    ("posthog-node", Cat::Ext, "PostHog"),
    ("posthog-js", Cat::Ext, "PostHog"),
    ("posthog", Cat::Ext, "PostHog"),
];

fn pat_match(pat: &str, name: &str) -> bool {
    match pat.strip_suffix('*') {
        Some(prefix) => {
            name.starts_with(prefix)
                && (name.len() == prefix.len()
                    || prefix.ends_with(['/', '.', '-'])
                    || name[prefix.len()..].starts_with(['/', '.', '-']))
        }
        None => pat == name,
    }
}

/// The resource a package or imported module stands for.
pub fn classify_package(name: &str) -> Option<(Cat, &'static str)> {
    let l = name.trim().to_ascii_lowercase();
    if l.is_empty() {
        return None;
    }
    PACKAGES
        .iter()
        .find(|(p, _, _)| pat_match(p, &l))
        .map(|(_, c, t)| (*c, *t))
}

/// `(type prefix, category, technology)` for resource types of Bicep and ARM
/// (`Microsoft.*`), Terraform (`aws_*`, `azurerm_*`, `google_*`) and
/// CloudFormation (`AWS::*`), compared without case. The longest prefix wins.
const IAC_TYPES: &[(&str, Cat, &str)] = &[
    ("microsoft.sql/", Cat::Db, "Azure SQL"),
    ("microsoft.dbforpostgresql/", Cat::Db, "PostgreSQL"),
    ("microsoft.dbformysql/", Cat::Db, "MySQL"),
    ("microsoft.documentdb/", Cat::Db, "Cosmos DB"),
    ("microsoft.cache/", Cat::Cache, "Redis"),
    ("microsoft.search/", Cat::Search, "Azure AI Search"),
    ("microsoft.servicebus/", Cat::Bus, "Azure Service Bus"),
    ("microsoft.eventhub/", Cat::Bus, "Event Hubs"),
    ("microsoft.eventgrid/", Cat::Bus, "Event Grid"),
    ("microsoft.keyvault/", Cat::Secrets, "Azure Key Vault"),
    (
        "microsoft.appconfiguration/",
        Cat::Config,
        "Azure App Configuration",
    ),
    (
        "microsoft.storage/storageaccounts/queueservices",
        Cat::Bus,
        "Azure Queue Storage",
    ),
    ("microsoft.storage/", Cat::Storage, "Azure Blob Storage"),
    ("azurerm_mssql_", Cat::Db, "Azure SQL"),
    ("azurerm_sql_", Cat::Db, "Azure SQL"),
    ("azurerm_postgresql_", Cat::Db, "PostgreSQL"),
    ("azurerm_mysql_", Cat::Db, "MySQL"),
    ("azurerm_cosmosdb_", Cat::Db, "Cosmos DB"),
    ("azurerm_redis_", Cat::Cache, "Redis"),
    ("azurerm_search_service", Cat::Search, "Azure AI Search"),
    ("azurerm_servicebus_", Cat::Bus, "Azure Service Bus"),
    ("azurerm_eventhub", Cat::Bus, "Event Hubs"),
    ("azurerm_eventgrid_", Cat::Bus, "Event Grid"),
    ("azurerm_key_vault", Cat::Secrets, "Azure Key Vault"),
    (
        "azurerm_app_configuration",
        Cat::Config,
        "Azure App Configuration",
    ),
    ("azurerm_storage_queue", Cat::Bus, "Azure Queue Storage"),
    ("azurerm_storage_", Cat::Storage, "Azure Blob Storage"),
    ("aws_db_", Cat::Db, "Amazon RDS"),
    ("aws_rds_", Cat::Db, "Amazon RDS"),
    ("aws_dynamodb_", Cat::Db, "DynamoDB"),
    ("aws_docdb_", Cat::Db, "DocumentDB"),
    ("aws_elasticache_", Cat::Cache, "ElastiCache"),
    ("aws_opensearch_", Cat::Search, "OpenSearch"),
    ("aws_elasticsearch_", Cat::Search, "OpenSearch"),
    ("aws_sqs_", Cat::Bus, "Amazon SQS"),
    ("aws_sns_", Cat::Bus, "Amazon SNS"),
    ("aws_kinesis_", Cat::Bus, "Kinesis"),
    ("aws_msk_", Cat::Bus, "Kafka"),
    ("aws_mq_", Cat::Bus, "Amazon MQ"),
    ("aws_cloudwatch_event_", Cat::Bus, "EventBridge"),
    ("aws_secretsmanager_", Cat::Secrets, "AWS Secrets Manager"),
    ("aws_ssm_parameter", Cat::Config, "SSM Parameter Store"),
    ("aws_s3_", Cat::Storage, "Amazon S3"),
    ("google_sql_", Cat::Db, "Cloud SQL"),
    ("google_spanner_", Cat::Db, "Spanner"),
    ("google_alloydb_", Cat::Db, "AlloyDB"),
    ("google_firestore_", Cat::Db, "Firestore"),
    ("google_bigquery_", Cat::Db, "BigQuery"),
    ("google_redis_", Cat::Cache, "Redis"),
    ("google_pubsub_", Cat::Bus, "Pub/Sub"),
    ("google_secret_manager_", Cat::Secrets, "Secret Manager"),
    ("google_storage_", Cat::Storage, "Cloud Storage"),
    ("aws::rds::", Cat::Db, "Amazon RDS"),
    ("aws::dynamodb::", Cat::Db, "DynamoDB"),
    ("aws::serverless::simpletable", Cat::Db, "DynamoDB"),
    ("aws::elasticache::", Cat::Cache, "ElastiCache"),
    ("aws::sqs::", Cat::Bus, "Amazon SQS"),
    ("aws::sns::", Cat::Bus, "Amazon SNS"),
    ("aws::kinesis::", Cat::Bus, "Kinesis"),
    ("aws::events::", Cat::Bus, "EventBridge"),
    ("aws::secretsmanager::", Cat::Secrets, "AWS Secrets Manager"),
    ("aws::ssm::parameter", Cat::Config, "SSM Parameter Store"),
    ("aws::s3::", Cat::Storage, "Amazon S3"),
];

/// IaC types that run code: where an app runs, not a resource it uses.
const RUNTIME_TYPES: &[(&str, &str)] = &[
    ("microsoft.app/containerapps", "Azure Container Apps"),
    ("microsoft.app/jobs", "Azure Container Apps"),
    ("microsoft.web/sites", "Azure App Service"),
    ("microsoft.containerservice/managedclusters", "AKS"),
    ("microsoft.containerinstance/", "Azure Container Instances"),
    ("azurerm_container_app", "Azure Container Apps"),
    ("azurerm_linux_web_app", "Azure App Service"),
    ("azurerm_windows_web_app", "Azure App Service"),
    ("azurerm_linux_function_app", "Azure Functions"),
    ("azurerm_kubernetes_cluster", "AKS"),
    ("aws_lambda_function", "AWS Lambda"),
    ("aws_ecs_", "Amazon ECS"),
    ("aws_eks_", "Amazon EKS"),
    ("aws_apprunner_service", "AWS App Runner"),
    ("aws_instance", "Amazon EC2"),
    ("aws::lambda::function", "AWS Lambda"),
    ("aws::serverless::function", "AWS Lambda"),
    ("aws::ecs::", "Amazon ECS"),
    ("google_cloud_run_", "Cloud Run"),
    ("google_cloudfunctions", "Cloud Functions"),
    ("google_container_cluster", "GKE"),
];

/// The resource an IaC type declares (`Microsoft.Sql/servers@2023-05-01`,
/// `aws_sqs_queue`, `AWS::SQS::Queue`).
pub fn classify_iac(kind: &str) -> Option<(Cat, &'static str)> {
    let l = kind.split('@').next().unwrap_or(kind).to_ascii_lowercase();
    IAC_TYPES
        .iter()
        .filter(|(p, _, _)| l.starts_with(p))
        .max_by_key(|(p, _, _)| p.len())
        .map(|(_, c, t)| (*c, *t))
}

/// Where an IaC type runs code, when it does.
pub fn runtime_of(kind: &str) -> Option<&'static str> {
    let l = kind.split('@').next().unwrap_or(kind).to_ascii_lowercase();
    RUNTIME_TYPES
        .iter()
        .find(|(p, _)| l.starts_with(p))
        .map(|(_, t)| *t)
}

/// `(image name segment, category, technology)`: images of well-known
/// servers, matched on the last path segment of the image's repository.
const IMAGES: &[(&str, Cat, &str)] = &[
    ("postgres", Cat::Db, "PostgreSQL"),
    ("postgresql", Cat::Db, "PostgreSQL"),
    ("postgis", Cat::Db, "PostgreSQL"),
    ("pgvector", Cat::Db, "PostgreSQL"),
    ("timescaledb", Cat::Db, "PostgreSQL"),
    ("mysql", Cat::Db, "MySQL"),
    ("mariadb", Cat::Db, "MariaDB"),
    ("mongo", Cat::Db, "MongoDB"),
    ("mongodb", Cat::Db, "MongoDB"),
    ("mssql", Cat::Db, "SQL Server"),
    ("cassandra", Cat::Db, "Cassandra"),
    ("scylla", Cat::Db, "ScyllaDB"),
    ("cockroach", Cat::Db, "CockroachDB"),
    ("clickhouse-server", Cat::Db, "ClickHouse"),
    ("neo4j", Cat::Db, "Neo4j"),
    ("couchdb", Cat::Db, "CouchDB"),
    ("dynamodb-local", Cat::Db, "DynamoDB"),
    ("redis", Cat::Cache, "Redis"),
    ("redis-stack", Cat::Cache, "Redis"),
    ("redis-stack-server", Cat::Cache, "Redis"),
    ("valkey", Cat::Cache, "Valkey"),
    ("keydb", Cat::Cache, "KeyDB"),
    ("memcached", Cat::Cache, "Memcached"),
    ("elasticsearch", Cat::Search, "Elasticsearch"),
    ("opensearch", Cat::Search, "OpenSearch"),
    ("meilisearch", Cat::Search, "Meilisearch"),
    ("qdrant", Cat::Search, "Qdrant"),
    ("typesense", Cat::Search, "Typesense"),
    ("rabbitmq", Cat::Bus, "RabbitMQ"),
    ("kafka", Cat::Bus, "Kafka"),
    ("cp-kafka", Cat::Bus, "Kafka"),
    ("cp-server", Cat::Bus, "Kafka"),
    ("redpanda", Cat::Bus, "Redpanda"),
    ("nats", Cat::Bus, "NATS"),
    ("pulsar", Cat::Bus, "Pulsar"),
    ("activemq", Cat::Bus, "ActiveMQ"),
    ("artemis", Cat::Bus, "ActiveMQ"),
    ("mosquitto", Cat::Bus, "MQTT"),
    ("emqx", Cat::Bus, "MQTT"),
    ("vault", Cat::Secrets, "Vault"),
    ("minio", Cat::Storage, "MinIO"),
    ("azurite", Cat::Storage, "Azure Blob Storage"),
    ("fake-gcs-server", Cat::Storage, "Cloud Storage"),
    ("mailhog", Cat::Ext, "SMTP server"),
    ("mailpit", Cat::Ext, "SMTP server"),
    ("maildev", Cat::Ext, "SMTP server"),
];

/// The resource an image reference runs (`docker.io/library/postgres:16`,
/// `bitnami/redis`, `mcr.microsoft.com/mssql/server:2022-latest`).
pub fn classify_image(reference: &str) -> Option<(Cat, &'static str)> {
    let r = reference
        .trim()
        .trim_matches(['"', '\''])
        .split('@')
        .next()
        .unwrap_or("");
    let path = r.rsplit_once(':').map_or(r, |(p, tag)| {
        // `host:5000/name` keeps its port: a tag never holds a `/`.
        if tag.contains('/') { r } else { p }
    });
    let path = path.to_ascii_lowercase();
    if path.contains('$') || path.contains('{') {
        return None;
    }
    let segs: Vec<&str> = path.split('/').collect();
    let last = *segs.last()?;
    // `mcr.microsoft.com/mssql/server`: the repository names the server.
    if last == "server" && segs.len() >= 2 && segs[segs.len() - 2] == "mssql" {
        return Some((Cat::Db, "SQL Server"));
    }
    IMAGES
        .iter()
        .find(|(n, _, _)| *n == last)
        .map(|(_, c, t)| (*c, *t))
}

/// `(Aspire builder method, category, technology)`.
const ASPIRE: &[(&str, Cat, &str)] = &[
    ("AddPostgres", Cat::Db, "PostgreSQL"),
    ("AddAzurePostgresFlexibleServer", Cat::Db, "PostgreSQL"),
    ("AddSqlServer", Cat::Db, "SQL Server"),
    ("AddAzureSqlServer", Cat::Db, "Azure SQL"),
    ("AddMySql", Cat::Db, "MySQL"),
    ("AddMongoDB", Cat::Db, "MongoDB"),
    ("AddOracle", Cat::Db, "Oracle"),
    ("AddAzureCosmosDB", Cat::Db, "Cosmos DB"),
    ("AddRedis", Cat::Cache, "Redis"),
    ("AddAzureRedis", Cat::Cache, "Redis"),
    ("AddValkey", Cat::Cache, "Valkey"),
    ("AddGarnet", Cat::Cache, "Garnet"),
    ("AddElasticsearch", Cat::Search, "Elasticsearch"),
    ("AddQdrant", Cat::Search, "Qdrant"),
    ("AddMilvus", Cat::Search, "Milvus"),
    ("AddAzureSearch", Cat::Search, "Azure AI Search"),
    ("AddRabbitMQ", Cat::Bus, "RabbitMQ"),
    ("AddKafka", Cat::Bus, "Kafka"),
    ("AddNats", Cat::Bus, "NATS"),
    ("AddAzureServiceBus", Cat::Bus, "Azure Service Bus"),
    ("AddAzureEventHubs", Cat::Bus, "Event Hubs"),
    ("AddAzureKeyVault", Cat::Secrets, "Azure Key Vault"),
    (
        "AddAzureAppConfiguration",
        Cat::Config,
        "Azure App Configuration",
    ),
    ("AddAzureStorage", Cat::Storage, "Azure Blob Storage"),
];

pub fn classify_aspire(method: &str) -> Option<(Cat, &'static str)> {
    ASPIRE
        .iter()
        .find(|(m, _, _)| *m == method)
        .map(|(_, c, t)| (*c, *t))
}

/// The database a Prisma, Drizzle or SQLAlchemy dialect names.
pub fn classify_dialect(provider: &str) -> Option<(Cat, &'static str)> {
    let l = provider.to_ascii_lowercase();
    let t = match l.as_str() {
        "postgresql" | "postgres" | "pg" | "cockroachdb" => "PostgreSQL",
        "mysql" | "mariadb" => "MySQL",
        "sqlserver" | "mssql" => "SQL Server",
        "sqlite" => "SQLite",
        "mongodb" => "MongoDB",
        _ => return None,
    };
    Some((Cat::Db, t))
}

/// AWS services a JavaScript `aws-sdk` (v2) or Python `boto3` file uses,
/// from `new AWS.SQS(`, `AWS.DynamoDB.DocumentClient` or `boto3.client("s3")`.
pub fn aws_services(line: &str) -> Vec<(Cat, &'static str)> {
    let mut out = Vec::new();
    let service = |name: &str| -> Option<(Cat, &'static str)> {
        Some(match name.to_ascii_lowercase().as_str() {
            "s3" => (Cat::Storage, "Amazon S3"),
            "sqs" => (Cat::Bus, "Amazon SQS"),
            "sns" => (Cat::Bus, "Amazon SNS"),
            "kinesis" => (Cat::Bus, "Kinesis"),
            "eventbridge" | "cloudwatchevents" | "events" => (Cat::Bus, "EventBridge"),
            "dynamodb" => (Cat::Db, "DynamoDB"),
            "rds" | "rdsdataservice" | "rds-data" => (Cat::Db, "Amazon RDS"),
            "secretsmanager" => (Cat::Secrets, "AWS Secrets Manager"),
            "ssm" => (Cat::Config, "SSM Parameter Store"),
            "ses" | "sesv2" => (Cat::Ext, "Amazon SES"),
            _ => return None,
        })
    };
    // JavaScript v2: `AWS.<Service>` (`new AWS.SQS()`, `AWS.DynamoDB.DocumentClient`).
    let mut rest = line;
    while let Some(i) = rest.find("AWS.") {
        let tail = &rest[i + 4..];
        let name: String = tail
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if let Some(s) = service(&name)
            && !out.contains(&s)
        {
            out.push(s);
        }
        rest = &tail[name.len().min(tail.len())..];
    }
    // Python: `.client("s3")`, `.resource('dynamodb')`.
    for call in [".client(", ".resource("] {
        let mut rest = line;
        while let Some(i) = rest.find(call) {
            let tail = rest[i + call.len()..].trim_start();
            if let Some(q) = tail.chars().next().filter(|c| *c == '"' || *c == '\'') {
                let name: String = tail[1..].chars().take_while(|c| *c != q).collect();
                if let Some(s) = service(&name)
                    && !out.contains(&s)
                {
                    out.push(s);
                }
            }
            rest = &rest[i + call.len()..];
        }
    }
    out
}

/// Modules a source line imports, by language: `(module, is_import)`.
/// Python `import a.b` / `from a.b import c`, JavaScript and TypeScript
/// `from 'x'` / `require('x')` / `import 'x'`, Go import paths, C#
/// `using X.Y;`, Rust `use x::` and `extern crate x`.
pub fn imports_of(lang: &str, line: &str, in_go_block: bool) -> Vec<String> {
    let t = line.trim();
    let mut out = Vec::new();
    // A commented-out import (`// import x from 'y'`, a JSDoc ` * require('y')`
    // line, a Go `// "path"` in an import block) imports nothing.
    if matches!(lang, "javascript" | "typescript" | "tsx" | "jsx" | "go")
        && (t.starts_with("//") || t.starts_with("/*") || t.starts_with('*'))
    {
        return out;
    }
    match lang {
        "python" => {
            if let Some(rest) = t.strip_prefix("from ") {
                let mut words = rest.split_whitespace();
                if let Some(m) = words.next()
                    && !m.starts_with('.')
                {
                    out.push(m.to_string());
                    // `from google.cloud import pubsub_v1`: the module named.
                    if words.next() == Some("import") {
                        let names: String = words.collect::<Vec<_>>().join(" ");
                        for n in names.trim_matches(['(', ')']).split(',') {
                            if let Some(n) = n.split_whitespace().next()
                                && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                                && !n.is_empty()
                            {
                                out.push(format!("{m}.{n}"));
                            }
                        }
                    }
                }
            } else if let Some(rest) = t.strip_prefix("import ") {
                for part in rest.split(',') {
                    if let Some(m) = part.split_whitespace().next() {
                        out.push(m.to_string());
                    }
                }
            }
        }
        "javascript" | "typescript" | "tsx" | "jsx" => {
            if !t.contains("import") && !t.contains("require(") && !t.starts_with("} from ") {
                return out;
            }
            let quoted = |s: &str| -> Option<String> {
                let s = s.trim_start();
                let q = s.chars().next().filter(|c| matches!(c, '"' | '\'' | '`'))?;
                let body: String = s[1..].chars().take_while(|c| *c != q).collect();
                (!body.is_empty()).then_some(body)
            };
            for key in ["from ", "require(", "import(", "import "] {
                let mut rest = t;
                while let Some(i) = rest.find(key) {
                    let before = rest[..i].chars().last();
                    let tail = &rest[i + key.len()..];
                    if before.is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_' && c != '.')
                        && let Some(m) = quoted(tail)
                        && !m.starts_with('.')
                        && !m.starts_with('/')
                    {
                        let pkg = js_package(&m);
                        if !out.contains(&pkg) {
                            out.push(pkg);
                        }
                    }
                    rest = tail;
                }
            }
        }
        "go" => {
            let body = if let Some(rest) = t.strip_prefix("import ") {
                rest.trim()
            } else if in_go_block {
                t
            } else {
                ""
            };
            if let Some(start) = body.find('"')
                && let Some(len) = body[start + 1..].find('"')
            {
                out.push(body[start + 1..start + 1 + len].to_string());
            }
        }
        "csharp" => {
            let rest = t
                .strip_prefix("global using ")
                .or_else(|| t.strip_prefix("using "));
            if let Some(rest) = rest
                && rest.ends_with(';')
                && !rest.contains('(')
            {
                let rest = rest.trim_end_matches(';').trim();
                let rest = rest.strip_prefix("static ").unwrap_or(rest);
                let ns = rest.rsplit('=').next().unwrap_or(rest).trim();
                if !ns.is_empty() && !ns.contains(' ') {
                    out.push(ns.to_string());
                }
            }
        }
        "rust" => {
            let rest = t
                .strip_prefix("pub use ")
                .or_else(|| t.strip_prefix("use "))
                .or_else(|| t.strip_prefix("extern crate "));
            if let Some(rest) = rest {
                let krate: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if !krate.is_empty()
                    && !matches!(krate.as_str(), "crate" | "self" | "super" | "std")
                {
                    out.push(krate);
                }
            }
        }
        _ => {}
    }
    out
}

/// The package of a JavaScript module specifier: `@scope/pkg/sub` ->
/// `@scope/pkg`, `pkg/sub` -> `pkg`, `node:fs` -> `node:fs`.
fn js_package(spec: &str) -> String {
    let mut parts = spec.split('/');
    let first = parts.next().unwrap_or("");
    if first.starts_with('@') {
        match parts.next() {
            Some(second) => format!("{first}/{second}"),
            None => first.to_string(),
        }
    } else {
        first.to_string()
    }
}

/// The module names an import may match the table by: Python's dotted
/// prefixes (`azure.storage.blob` for `azure.storage.blob.aio`), a Go
/// path's prefixes, else the module itself.
pub fn import_candidates(lang: &str, module: &str) -> Vec<String> {
    match lang {
        "python" | "csharp" => {
            let segs: Vec<&str> = module.split('.').collect();
            (1..=segs.len())
                .rev()
                .map(|n| segs[..n].join("."))
                .collect()
        }
        "go" => {
            let segs: Vec<&str> = module.split('/').collect();
            (1..=segs.len())
                .rev()
                .map(|n| segs[..n].join("/"))
                .collect()
        }
        _ => vec![module.to_string()],
    }
}

/// The resource an import names, most specific module first.
pub fn classify_import(lang: &str, module: &str) -> Option<(Cat, &'static str)> {
    // Go and Rust standard modules that name no resource.
    if lang == "go" && module == "database/sql" {
        return Some((Cat::Db, ""));
    }
    import_candidates(lang, module)
        .iter()
        .find_map(|m| classify_package(m))
        // A Python `import redis` must not read as the Rust crate `redis` of
        // another language: every entry here is language-neutral, so the
        // first match is the answer.
        .filter(|(c, t)| !(lang == "rust" && *c == Cat::Ext && t.is_empty()))
}

/// Hosts an env value or connection string names: `redis-cart:6379`,
/// `postgres://u:p@db:5432/x`, `Host=db;Port=5432`, `amqp://rabbitmq`.
pub fn hosts_in(value: &str) -> Vec<String> {
    let v = value.trim().trim_matches(['"', '\'']);
    let mut out: Vec<String> = Vec::new();
    let mut push = |h: &str| {
        let h = h.trim().trim_matches(['"', '\'', '[', ']']);
        let h = h.split(':').next().unwrap_or(h);
        if !h.is_empty()
            && h.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
            && !out.iter().any(|o| o == h)
        {
            out.push(h.to_ascii_lowercase());
        }
    };
    if let Some((_, rest)) = v.split_once("://") {
        let auth = rest.split(['/', '?', '#']).next().unwrap_or("");
        let host = auth.rsplit('@').next().unwrap_or(auth);
        for h in host.split(',') {
            push(h);
        }
    } else if v.contains('=') && v.contains(';') || v.to_ascii_lowercase().starts_with("host=") {
        for part in v.split(';') {
            if let Some((k, val)) = part.split_once('=') {
                let k = k.trim().to_ascii_lowercase();
                if matches!(
                    k.as_str(),
                    "host" | "server" | "data source" | "address" | "addr"
                ) {
                    push(val.trim().trim_start_matches("tcp:"));
                }
            }
        }
    } else if !v.contains(char::is_whitespace) && !v.contains('/') {
        // `redis-cart:6379` or `db`: one name, maybe with a port.
        if v.contains(':') || v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            push(v);
        }
    }
    out
}

/// The host of an absolute URL: `https://api.github.com/x` -> `api.github.com`.
pub fn url_host(raw: &str) -> Option<String> {
    let (scheme, rest) = raw.trim().split_once("://")?;
    if !matches!(
        scheme.to_ascii_lowercase().as_str(),
        "http" | "https" | "ws" | "wss"
    ) {
        return None;
    }
    let auth = rest.split(['/', '?', '#']).next()?;
    let host = auth
        .rsplit('@')
        .next()?
        .split(':')
        .next()?
        .to_ascii_lowercase();
    if host.is_empty()
        || host.contains(['$', '{', '}', '<', '>', '`', ' '])
        || !host.contains('.')
        || matches!(host.as_str(), "localhost" | "127.0.0.1" | "0.0.0.0")
        || host.ends_with(".local")
        || host.ends_with(".localhost")
        || host.ends_with(".internal")
        || host == "example.com"
        || host.ends_with(".example.com")
        || host.ends_with(".example.org")
        || host.ends_with(".test")
    {
        return None;
    }
    Some(host)
}

// ------------------------------------------------------------------ scanning

/// A resource or workload an infrastructure file declares.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decl {
    /// `bicep`, `terraform`, `cfn`, `k8s`, `compose`, `helm`, `serverless`,
    /// `aspire` or `prisma`.
    pub source: &'static str,
    /// A resource type (`Microsoft.Sql/servers`, `aws_sqs_queue`,
    /// `AWS::SQS::Queue`), `kind:<Kind>` for a Kubernetes object,
    /// `image:<ref>` or `build:<dir>` for a container, `event:<type>` for a
    /// Serverless function's trigger, an Aspire method, `provider:<db>` for
    /// a Prisma datasource or `dep:<chart>` for a Helm dependency.
    pub what: String,
    /// The name it is given (a service, an object, a resource label).
    pub name: String,
    /// A Kubernetes object's `app` label or a Service's selector; an Aspire
    /// database's server.
    pub group: String,
    pub ns: Option<String>,
    /// The lines it spans.
    pub span: (i64, i64),
    pub ev: Evidence,
}

/// A deployment file wiring a service to another: compose `depends_on` and
/// env values, Kubernetes env values, Aspire `WithReference`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Wire {
    pub source: &'static str,
    /// The service (or Aspire variable) that uses.
    pub from: String,
    /// A service name, or a host an env value names.
    pub to: String,
    pub ev: Evidence,
}

fn ev(path: &str, line: i64, text: &str) -> Evidence {
    Evidence {
        file: path.to_string(),
        line,
        text: text.trim().chars().take(160).collect(),
    }
}

fn line_index(text: &str, needle: &str, from: i64) -> i64 {
    text.lines()
        .enumerate()
        .skip(from.max(1) as usize - 1)
        .find(|(_, l)| l.contains(needle))
        .map(|(i, _)| i as i64 + 1)
        .unwrap_or(from.max(1))
}

/// Bicep `resource <sym> '<type>@<version>' [existing] = {`.
pub fn scan_bicep(path: &str, text: &str) -> Vec<Decl> {
    let mut out = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    for (i, raw) in lines.iter().enumerate() {
        let l = raw.trim();
        let Some(rest) = l.strip_prefix("resource ") else {
            continue;
        };
        let mut it = rest.splitn(2, ' ');
        let sym = it.next().unwrap_or("").to_string();
        let tail = it.next().unwrap_or("").trim_start();
        let Some(body) = tail.strip_prefix('\'') else {
            continue;
        };
        let Some(end) = body.find('\'') else { continue };
        let kind = body[..end].split('@').next().unwrap_or("").to_string();
        // A literal `name:` in the next lines, else the symbol.
        let mut name = sym.clone();
        for nl in lines.iter().skip(i + 1).take(8) {
            let nt = nl.trim();
            if let Some(v) = nt.strip_prefix("name:") {
                let v = v.trim();
                if let Some(inner) = v.strip_prefix('\'').and_then(|s| s.strip_suffix('\''))
                    && !inner.contains("${")
                {
                    name = inner.to_string();
                }
                break;
            }
            if nt.starts_with("resource ") {
                break;
            }
        }
        let line = i as i64 + 1;
        out.push(Decl {
            source: "bicep",
            what: kind,
            name,
            group: sym,
            ns: None,
            span: (line, line),
            ev: ev(path, line, l),
        });
    }
    out
}

/// Terraform `resource "<type>" "<name>" {`.
pub fn scan_terraform(path: &str, text: &str) -> Vec<Decl> {
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let l = raw.trim();
        let Some(rest) = l.strip_prefix("resource ") else {
            continue;
        };
        let parts: Vec<&str> = rest.split('"').collect();
        // `resource "type" "name" {` splits to ["", type, " ", name, " {"].
        if parts.len() < 4 {
            continue;
        }
        let line = i as i64 + 1;
        out.push(Decl {
            source: "terraform",
            what: parts[1].to_string(),
            name: parts[3].to_string(),
            group: String::new(),
            ns: None,
            span: (line, line),
            ev: ev(path, line, l),
        });
    }
    out
}

fn ystr(v: &serde_yaml_ng::Value) -> Option<String> {
    match v {
        serde_yaml_ng::Value::String(s) => Some(s.clone()),
        serde_yaml_ng::Value::Number(n) => Some(n.to_string()),
        serde_yaml_ng::Value::Bool(b) => Some(b.to_string()),
        serde_yaml_ng::Value::Tagged(t) => ystr(&t.value),
        _ => None,
    }
}

fn yget<'a>(v: &'a serde_yaml_ng::Value, key: &str) -> Option<&'a serde_yaml_ng::Value> {
    v.as_mapping()?
        .get(serde_yaml_ng::Value::String(key.to_string()))
}

fn ypath<'a>(v: &'a serde_yaml_ng::Value, keys: &[&str]) -> Option<&'a serde_yaml_ng::Value> {
    keys.iter().try_fold(v, |cur, k| yget(cur, k))
}

/// YAML documents of a file with their first and last lines.
fn yaml_docs(text: &str) -> Vec<(i64, i64, String)> {
    let mut out = Vec::new();
    let mut start = 1i64;
    let mut buf = String::new();
    let mut n = 0i64;
    for line in text.lines() {
        n += 1;
        if line.trim_end() == "---" || line.starts_with("--- ") {
            if !buf.trim().is_empty() {
                out.push((start, n - 1, std::mem::take(&mut buf)));
            }
            buf.clear();
            start = n + 1;
            continue;
        }
        buf.push_str(line);
        buf.push('\n');
    }
    if !buf.trim().is_empty() {
        out.push((start, n, buf));
    }
    out
}

const WORKLOADS: &[&str] = &[
    "Deployment",
    "StatefulSet",
    "DaemonSet",
    "Job",
    "CronJob",
    "Pod",
    "ReplicaSet",
];

/// Kubernetes objects, compose services, Helm charts, Serverless functions
/// and CloudFormation resources of one YAML file, and the wiring between
/// its services. `tool` is what [`super::units`] took the file for.
pub fn scan_yaml(path: &str, text: &str, tool: &str) -> (Vec<Decl>, Vec<Wire>) {
    let mut decls = Vec::new();
    let mut wires = Vec::new();
    for (first, last, doc) in yaml_docs(text) {
        let Ok(v) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&doc) else {
            continue;
        };
        let at = |needle: &str| line_index(text, needle, first).min(last.max(first));
        // The line a mapping key `name:` starts, not one that mentions it.
        let at_key = |key: &str| {
            text.lines()
                .enumerate()
                .skip((first.max(1) - 1) as usize)
                .take((last - first + 1).max(1) as usize)
                .find(|(_, l)| {
                    let t = l.trim_start().trim_start_matches("- ");
                    t.strip_prefix(key).is_some_and(|r| r.starts_with(':'))
                        || t.trim_matches(['"', '\'']).starts_with(&format!("{key}:"))
                })
                .map(|(i, _)| i as i64 + 1)
                .unwrap_or_else(|| at(&format!("{key}:")))
        };
        let mk = |source: &'static str, what: String, name: String, line: i64| Decl {
            source,
            what,
            name,
            group: String::new(),
            ns: None,
            span: (first, last),
            ev: ev(
                path,
                line,
                text.lines().nth((line - 1).max(0) as usize).unwrap_or(""),
            ),
        };
        if tool == "compose" {
            let Some(services) = yget(&v, "services").and_then(|s| s.as_mapping()) else {
                continue;
            };
            for (k, s) in services {
                let Some(svc) = ystr(k) else { continue };
                let line = at_key(&svc);
                if let Some(img) = yget(s, "image").and_then(ystr) {
                    decls.push(mk("compose", format!("image:{img}"), svc.clone(), line));
                } else if let Some(b) = yget(s, "build") {
                    let ctx = ystr(b)
                        .or_else(|| yget(b, "context").and_then(ystr))
                        .unwrap_or_else(|| ".".to_string());
                    decls.push(mk("compose", format!("build:{ctx}"), svc.clone(), line));
                }
                let mut deps: Vec<String> = Vec::new();
                match yget(s, "depends_on") {
                    Some(serde_yaml_ng::Value::Sequence(list)) => {
                        deps.extend(list.iter().filter_map(ystr));
                    }
                    Some(serde_yaml_ng::Value::Mapping(m)) => {
                        deps.extend(m.keys().filter_map(ystr));
                    }
                    _ => {}
                }
                if let Some(serde_yaml_ng::Value::Sequence(list)) = yget(s, "links") {
                    deps.extend(
                        list.iter()
                            .filter_map(ystr)
                            .map(|l| l.split(':').next().unwrap_or("").to_string()),
                    );
                }
                for d in deps {
                    let line = line_index(text, &d, line);
                    wires.push(Wire {
                        source: "compose",
                        from: svc.clone(),
                        to: d.to_ascii_lowercase(),
                        ev: ev(path, line, &format!("{svc} depends on {d}")),
                    });
                }
                let mut values: Vec<(String, String)> = Vec::new();
                match yget(s, "environment") {
                    Some(serde_yaml_ng::Value::Sequence(list)) => {
                        for e in list.iter().filter_map(ystr) {
                            if let Some((k, v)) = e.split_once('=') {
                                values.push((k.to_string(), v.to_string()));
                            }
                        }
                    }
                    Some(serde_yaml_ng::Value::Mapping(m)) => {
                        for (k, v) in m {
                            if let (Some(k), Some(v)) = (ystr(k), ystr(v)) {
                                values.push((k, v));
                            }
                        }
                    }
                    _ => {}
                }
                for (k, val) in values {
                    for h in hosts_in(&val) {
                        let line = line_index(text, &k, line);
                        wires.push(Wire {
                            source: "compose",
                            from: svc.clone(),
                            to: h,
                            ev: ev(path, line, &format!("{k}={val}")),
                        });
                    }
                }
            }
            continue;
        }
        if tool == "Serverless" {
            let service = yget(&v, "service").and_then(ystr).unwrap_or_default();
            let Some(fns) = yget(&v, "functions").and_then(|f| f.as_mapping()) else {
                continue;
            };
            // Parameters read from SSM at deploy time.
            if let Some(i) = text.lines().position(|l| l.contains("${ssm:")) {
                decls.push(mk(
                    "serverless",
                    "ssm".to_string(),
                    service.clone(),
                    i as i64 + 1,
                ));
            }
            for (k, f) in fns {
                let Some(fname) = ystr(k) else { continue };
                let line = at_key(&fname);
                // `handler: src/orders.handle` -> the file `src/orders`.
                let handler = yget(f, "handler")
                    .and_then(ystr)
                    .map(|h| h.rsplit_once('.').map_or(h.clone(), |(f, _)| f.to_string()))
                    .unwrap_or_default();
                let mut d = mk(
                    "serverless",
                    "function".to_string(),
                    if service.is_empty() {
                        fname.clone()
                    } else {
                        format!("{service}-{fname}")
                    },
                    line,
                );
                d.group = handler.clone();
                decls.push(d);
                if let Some(serde_yaml_ng::Value::Sequence(events)) = yget(f, "events") {
                    for e in events {
                        let Some(m) = e.as_mapping() else { continue };
                        for (ek, evv) in m {
                            let Some(ek) = ystr(ek) else { continue };
                            let target = ystr(evv)
                                .or_else(|| {
                                    ["arn", "topicName", "queue", "bucket", "stream"]
                                        .iter()
                                        .find_map(|k| yget(evv, k).and_then(ystr))
                                })
                                .unwrap_or_default();
                            let stream_kind = yget(evv, "type").and_then(ystr).unwrap_or_default();
                            let what = match ek.as_str() {
                                "sns" => "event:sns",
                                "sqs" => "event:sqs",
                                "s3" => "event:s3",
                                "stream"
                                    if stream_kind == "kinesis" || target.contains(":kinesis:") =>
                                {
                                    "event:kinesis"
                                }
                                "stream" => "event:dynamodb",
                                "kafka" => "event:kafka",
                                "rabbitmq" => "event:rabbitmq",
                                "eventBridge" => "event:eventbridge",
                                _ => continue,
                            };
                            let line = line_index(text, &format!("{ek}:"), line);
                            let mut d = mk("serverless", what.to_string(), fname.clone(), line);
                            d.group = handler.clone();
                            decls.push(d);
                        }
                    }
                }
            }
            continue;
        }
        if tool == "CloudFormation" {
            let Some(res) = yget(&v, "Resources").and_then(|r| r.as_mapping()) else {
                continue;
            };
            for (k, r) in res {
                let (Some(name), Some(ty)) = (ystr(k), yget(r, "Type").and_then(ystr)) else {
                    continue;
                };
                let line = at_key(&name);
                decls.push(mk("cfn", ty, name, line));
            }
            continue;
        }
        if tool == "Helm" {
            if path.rsplit('/').next() == Some("Chart.yaml") {
                if let Some(name) = yget(&v, "name").and_then(ystr) {
                    decls.push(mk("helm", "chart".to_string(), name, at("name:")));
                }
                if let Some(serde_yaml_ng::Value::Sequence(deps)) = yget(&v, "dependencies") {
                    for d in deps {
                        if let Some(n) = yget(d, "name").and_then(ystr) {
                            let line = line_index(text, &n, first);
                            decls.push(mk("helm", format!("dep:{n}"), n, line));
                        }
                    }
                }
            }
            continue;
        }
        // Kubernetes. A kustomization sets the namespace of what it lists.
        if yget(&v, "kind").and_then(ystr).as_deref() == Some("Kustomization")
            || path
                .rsplit('/')
                .next()
                .is_some_and(|f| f.starts_with("kustomization."))
        {
            if let Some(ns) = yget(&v, "namespace").and_then(ystr) {
                let mut d = mk(
                    "k8s",
                    "kind:Kustomization".to_string(),
                    String::new(),
                    at("namespace:"),
                );
                d.ns = Some(ns);
                decls.push(d);
            }
            continue;
        }
        let (Some(kind), Some(name)) = (
            yget(&v, "kind").and_then(ystr),
            ypath(&v, &["metadata", "name"]).and_then(ystr),
        ) else {
            continue;
        };
        let ns = ypath(&v, &["metadata", "namespace"]).and_then(ystr);
        let line = at("kind:");
        let mut d = mk("k8s", format!("kind:{kind}"), name.clone(), line);
        d.ns = ns.clone();
        if kind == "Service" {
            d.group = ypath(&v, &["spec", "selector", "app"])
                .or_else(|| ypath(&v, &["spec", "selector", "app.kubernetes.io/name"]))
                .and_then(ystr)
                .unwrap_or_default();
        }
        if kind == "SecretProviderClass" {
            let provider = ypath(&v, &["spec", "provider"])
                .and_then(ystr)
                .unwrap_or_default();
            d.what = format!("kind:SecretProviderClass:{provider}");
        }
        if WORKLOADS.contains(&kind.as_str()) {
            let spec = if kind == "CronJob" {
                ypath(&v, &["spec", "jobTemplate", "spec", "template", "spec"])
            } else if kind == "Pod" {
                yget(&v, "spec")
            } else {
                ypath(&v, &["spec", "template", "spec"])
            };
            let labels = if kind == "Pod" {
                ypath(&v, &["metadata", "labels"])
            } else {
                ypath(&v, &["spec", "template", "metadata", "labels"])
            };
            d.group = labels
                .and_then(|l| yget(l, "app").or_else(|| yget(l, "app.kubernetes.io/name")))
                .and_then(ystr)
                .unwrap_or_default();
            let mut containers: Vec<&serde_yaml_ng::Value> = Vec::new();
            for key in ["containers", "initContainers"] {
                if let Some(serde_yaml_ng::Value::Sequence(c)) = spec.and_then(|s| yget(s, key)) {
                    containers.extend(c.iter());
                }
            }
            for c in containers {
                if let Some(img) = yget(c, "image").and_then(ystr) {
                    let l = line_index(text, &img, first);
                    let mut di = mk("k8s", format!("image:{img}"), name.clone(), l);
                    di.ns = ns.clone();
                    di.group = d.group.clone();
                    decls.push(di);
                }
                if let Some(serde_yaml_ng::Value::Sequence(env)) = yget(c, "env") {
                    for e in env {
                        let (Some(k), Some(val)) = (
                            yget(e, "name").and_then(ystr),
                            yget(e, "value").and_then(ystr),
                        ) else {
                            continue;
                        };
                        for h in hosts_in(&val) {
                            let l = line_index(text, &k, first);
                            wires.push(Wire {
                                source: "k8s",
                                from: name.clone(),
                                to: h,
                                ev: ev(path, l, &format!("{k}: {val}")),
                            });
                        }
                    }
                }
            }
        }
        decls.push(d);
    }
    (decls, wires)
}

/// A Prisma schema's `datasource` provider.
pub fn scan_prisma(path: &str, text: &str) -> Vec<Decl> {
    let mut out = Vec::new();
    let mut in_ds = false;
    for (i, raw) in text.lines().enumerate() {
        let l = raw.trim();
        if l.starts_with("datasource ") {
            in_ds = true;
            continue;
        }
        if in_ds && l.starts_with('}') {
            in_ds = false;
        }
        if in_ds
            && let Some(rest) = l.strip_prefix("provider")
            && let Some(v) = rest.trim().strip_prefix('=')
        {
            let p = v.trim().trim_matches('"').to_string();
            let line = i as i64 + 1;
            out.push(Decl {
                source: "prisma",
                what: format!("provider:{p}"),
                name: "datasource".to_string(),
                group: String::new(),
                ns: None,
                span: (line, line),
                ev: ev(path, line, l),
            });
        }
    }
    out
}

/// A project an app host runs: its name, its target and where.
pub type Project = (String, String, Evidence);

/// A .NET Aspire app host: the resources its `builder.Add*` calls declare,
/// the projects it runs (`(name, target)`, the target a project path or
/// `alias:<name>` for `Projects.X`), and which project references which
/// resource. `dir` is the app host's folder.
pub fn scan_aspire(dir: &str, files: &[(String, String)]) -> (Vec<Decl>, Vec<Wire>, Vec<Project>) {
    let mut decls: Vec<Decl> = Vec::new();
    let mut wires: Vec<Wire> = Vec::new();
    let mut projects: Vec<Project> = Vec::new();
    // Variable -> the name of what it holds (resource or project).
    let mut vars: BTreeMap<String, String> = BTreeMap::new();
    let first_str = |s: &str| -> Option<String> {
        let i = s.find('"')?;
        let rest = &s[i + 1..];
        Some(rest[..rest.find('"')?].to_string())
    };
    let args_of = |s: &str| -> Vec<String> {
        // The string literals of a call's arguments.
        let mut out = Vec::new();
        let mut rest = s;
        while let Some(i) = rest.find('"') {
            let tail = &rest[i + 1..];
            let Some(j) = tail.find('"') else { break };
            out.push(tail[..j].to_string());
            rest = &tail[j + 1..];
        }
        out
    };
    for (path, text) in files {
        // Statements: joined lines up to `;`, with their first line.
        let mut stmts: Vec<(i64, String)> = Vec::new();
        let mut cur = String::new();
        let mut start = 0i64;
        for (i, raw) in text.lines().enumerate() {
            let l = raw.split("//").next().unwrap_or("").trim();
            if l.is_empty() {
                continue;
            }
            if cur.is_empty() {
                start = i as i64 + 1;
            }
            cur.push_str(l);
            cur.push(' ');
            if l.ends_with(';') || l.ends_with('{') || l.ends_with('}') {
                stmts.push((start, std::mem::take(&mut cur)));
            }
        }
        for (line, s) in stmts {
            let assigned = s
                .strip_prefix("var ")
                .and_then(|r| r.split_once('='))
                .map(|(v, _)| v.trim().to_string());
            let mut holder: Option<String> = None;
            // `.AddX("name"...)` calls in order: the first names the holder.
            let mut rest = s.as_str();
            while let Some(i) = rest.find(".Add") {
                let tail = &rest[i + 1..];
                let method: String = tail
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                let after = &tail[method.len()..];
                let generic = after.strip_prefix('<').and_then(|g| g.split_once('>'));
                let call = generic.map(|(_, r)| r).unwrap_or(after);
                if !call.starts_with('(') {
                    rest = after;
                    continue;
                }
                let close = call.find(')').unwrap_or(call.len());
                let args = args_of(&call[..close]);
                let e = ev(path, line, &s);
                if let Some((_, _)) = classify_aspire(&method) {
                    if let Some(name) = args.first() {
                        decls.push(Decl {
                            source: "aspire",
                            what: method.clone(),
                            name: name.clone(),
                            group: String::new(),
                            ns: None,
                            span: (line, line),
                            ev: e,
                        });
                        holder.get_or_insert(name.clone());
                    }
                } else if matches!(
                    method.as_str(),
                    "AddDatabase"
                        | "AddBlobs"
                        | "AddQueues"
                        | "AddTables"
                        | "AddBlobContainer"
                        | "AddServiceBusQueue"
                        | "AddServiceBusTopic"
                        | "AddHub"
                        | "AddSecret"
                ) {
                    // A child of the variable the call hangs on.
                    let base = s[..s.len() - rest.len() + i]
                        .rsplit(['=', ' ', '(', ')'])
                        .find(|t| !t.trim().is_empty())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    let parent = vars.get(&base).cloned().or_else(|| holder.clone());
                    if let (Some(name), Some(parent)) = (args.first(), parent) {
                        decls.push(Decl {
                            source: "aspire",
                            what: method.clone(),
                            name: name.clone(),
                            group: parent,
                            ns: None,
                            span: (line, line),
                            ev: e,
                        });
                        holder = Some(name.clone());
                    }
                } else if method.starts_with("Add")
                    && (method.contains("Project")
                        || method.ends_with("App")
                        || method == "AddDockerfile"
                        || method == "AddContainer")
                {
                    let name = args.first().cloned();
                    let target = if let Some((g, _)) = generic {
                        g.trim()
                            .strip_prefix("Projects.")
                            .map(|p| format!("alias:{p}"))
                    } else if method == "AddContainer" {
                        args.get(1).map(|img| format!("image:{img}"))
                    } else {
                        args.get(1).and_then(|p| {
                            super::units::join_rel_pub(
                                dir,
                                &format!("{}/", p.trim_end_matches('/')),
                            )
                            .map(|full| {
                                // A project file names its folder.
                                if full.ends_with("proj/") {
                                    let t = full.trim_end_matches('/');
                                    format!("dir:{}", &t[..t.rfind('/').map_or(0, |i| i + 1)])
                                } else {
                                    format!("dir:{full}")
                                }
                            })
                        })
                    };
                    if let (Some(name), Some(target)) = (name, target) {
                        if method == "AddContainer"
                            && let Some(img) = target.strip_prefix("image:")
                            && classify_image(img).is_some()
                        {
                            decls.push(Decl {
                                source: "aspire",
                                what: target.clone(),
                                name: name.clone(),
                                group: String::new(),
                                ns: None,
                                span: (line, line),
                                ev: e.clone(),
                            });
                        } else {
                            projects.push((name.clone(), target, e));
                        }
                        holder.get_or_insert(name);
                    }
                }
                rest = &call[close.min(call.len())..];
            }
            // `x.WithReference(y)` on an existing variable: `x` is the holder.
            if holder.is_none() {
                let head = s.split('.').next().unwrap_or("").trim();
                if let Some(h) = vars.get(head) {
                    holder = Some(h.clone());
                }
            }
            if let (Some(v), Some(h)) = (&assigned, &holder) {
                vars.insert(v.clone(), h.clone());
            }
            let Some(h) = holder else { continue };
            let mut rest = s.as_str();
            while let Some(i) = rest.find(".WithReference(") {
                let tail = &rest[i + ".WithReference(".len()..];
                let arg: String = tail
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if let Some(target) = vars.get(&arg) {
                    wires.push(Wire {
                        source: "aspire",
                        from: h.clone(),
                        to: target.clone(),
                        ev: ev(path, line, &format!("{h} .WithReference({arg})")),
                    });
                } else if let Some(lit) = first_str(tail).filter(|_| tail.starts_with('"')) {
                    wires.push(Wire {
                        source: "aspire",
                        from: h.clone(),
                        to: lit,
                        ev: ev(path, line, &s),
                    });
                }
                rest = tail;
            }
        }
    }
    (decls, wires, projects)
}

// ------------------------------------------------------------------ building

/// How a unit uses a resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verb {
    Uses,
    ReadsWrites,
    Stores,
    ReadsSecrets,
    Calls,
    Subscribes,
    Publishes,
}

/// Where a use was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum How {
    /// A package the unit's manifest declares.
    Manifest,
    /// A deployment file wires the unit to the resource.
    Deploy,
    /// Through a library whose code uses the resource.
    Via,
    /// A source file imports the resource's client (or a schema names it).
    Import,
    /// An edge of the index: a topic, a secret, a SQL object, an HTTP call.
    Edge,
}

impl How {
    pub fn as_str(self) -> &'static str {
        match self {
            How::Manifest => "manifest",
            How::Deploy => "deploy",
            How::Via => "via",
            How::Import => "import",
            How::Edge => "edge",
        }
    }
}

/// A resource of the system: one per category and technology.
#[derive(Clone, Debug)]
pub struct Res {
    pub cat: Cat,
    /// Empty for a resource of unknown technology.
    pub tech: String,
    /// Named parts: databases, topics, queues, secrets, buckets, services.
    pub items: BTreeMap<String, &'static str>,
    /// Where infrastructure, an app host or a schema declares it.
    pub declared: Vec<Evidence>,
    /// Declared resource types and how many of each.
    pub kinds: BTreeMap<String, usize>,
    /// Units that are this resource (a SQL project).
    pub units: Vec<usize>,
}

impl Res {
    /// The name inside a sentence: "PostgreSQL", "a message bus".
    pub fn in_sentence(&self) -> String {
        if self.tech.is_empty() {
            match self.cat {
                Cat::Storage => self.cat.neutral().to_string(),
                Cat::Ext => format!("an {}", self.cat.neutral()),
                c => format!("a {}", c.neutral()),
            }
        } else {
            self.tech.clone()
        }
    }

    pub fn name(&self) -> String {
        if self.tech.is_empty() {
            let n = self.cat.neutral();
            let mut c = n.chars();
            c.next()
                .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
                .unwrap_or_default()
        } else {
            self.tech.clone()
        }
    }
}

/// One piece of evidence that a unit uses a resource.
#[derive(Clone, Debug)]
pub struct Use {
    pub unit: usize,
    pub res: usize,
    pub verb: Verb,
    pub how: How,
    /// A topic, secret, SQL object, call or service the use names.
    pub item: Option<String>,
    pub qual: Option<String>,
    /// The library the use goes through.
    pub via: Option<usize>,
    pub ev: Evidence,
}

/// Resources found, the uses of them, and what infrastructure declares
/// beyond them.
#[derive(Default, Debug)]
pub struct Found {
    pub res: Vec<Res>,
    pub uses: Vec<Use>,
    /// Per infra unit: declared types that are no resource of the map.
    pub other: BTreeMap<usize, BTreeMap<String, usize>>,
    /// Per infra unit: where it runs code (`Azure Container Apps`).
    pub runtimes: BTreeMap<usize, BTreeSet<&'static str>>,
    /// Per code unit: where a function trigger file runs it.
    pub runs_on: BTreeMap<usize, Vec<(&'static str, Evidence)>>,
    /// Files whose code uses a resource, directly or through the library
    /// files it calls: the resources.
    pub file_res: HashMap<String, BTreeSet<usize>>,
    /// Deployment files wiring one app to another: `(from, to, evidence)`.
    pub app_wires: Vec<(usize, usize, Evidence)>,
}

pub(super) struct Input<'a> {
    pub root: &'a std::path::Path,
    pub units: &'a [Unit],
    pub owners: &'a Owners,
    pub files: &'a [RepoFile],
    pub rows: &'a [RepoRow],
    pub uses: &'a [UnitUse],
    pub links: &'a [LinkView],
    pub in_scope: &'a dyn Fn(&str) -> bool,
    pub deps: &'a [(usize, usize, Evidence)],
}

/// Largest source file read for imports.
const MAX_SOURCE: u64 = 512 * 1024;
/// Declarations kept per resource.
const MAX_DECLARED: usize = 12;

const CODE: &[&str] = &[
    "python",
    "javascript",
    "typescript",
    "tsx",
    "jsx",
    "go",
    "csharp",
    "rust",
];

fn default_verb(cat: Cat) -> Verb {
    match cat {
        Cat::Db => Verb::ReadsWrites,
        Cat::Secrets => Verb::ReadsSecrets,
        Cat::Storage => Verb::Stores,
        Cat::Ext => Verb::Calls,
        _ => Verb::Uses,
    }
}

/// What a declared type's instances are called: `topics` -> `topic`.
fn item_kind(kind: &str) -> Option<&'static str> {
    let l = kind.to_ascii_lowercase();
    let last = l.rsplit(['/', ':', '_']).next().unwrap_or("");
    Some(match last {
        "topics" | "topic" => "topic",
        "queues" | "queue" => "queue",
        "subscriptions" | "subscription" => "subscription",
        "databases" | "database" | "instance" | "cluster" => "database",
        "secrets" | "secret" => "secret",
        "parameter" => "parameter",
        "containers" | "container" | "bucket" => "container",
        "table" => "table",
        "eventhubs" | "stream" => "stream",
        _ => return None,
    })
}

/// `(category, server, client)`: a server infrastructure declares and the
/// technology whose client the code uses to reach it, one resource. Azure
/// SQL is what a SQL Server client reaches, MariaDB what a MySQL client
/// reaches, and Valkey, KeyDB and Garnet are what a Redis client reaches.
const MANAGED: &[(Cat, &str, &str)] = &[
    (Cat::Db, "Azure SQL", "SQL Server"),
    (Cat::Db, "MariaDB", "MySQL"),
    (Cat::Cache, "Valkey", "Redis"),
    (Cat::Cache, "KeyDB", "Redis"),
    (Cat::Cache, "Garnet", "Redis"),
];

/// Databases that hold no SQL objects: a table or stored procedure the code
/// references, or a SQL-only client, never reaches one of these.
const NOT_SQL: &[&str] = &[
    "MongoDB",
    "Cassandra",
    "ScyllaDB",
    "DynamoDB",
    "Cosmos DB",
    "Firestore",
    "Neo4j",
    "CouchDB",
];

/// Clients and ORMs that speak SQL and name no engine (`database/sql`,
/// `sqlx`, SQLAlchemy, Knex, Dapper): the database they reach is a SQL one.
const SQL_CLIENTS: &[&str] = &[
    "database/sql",
    "github.com/jmoiron/sqlx",
    "gorm.io/gorm",
    "sqlx",
    "diesel",
    "sea-orm",
    "sea_orm",
    "sqlalchemy",
    "sqlmodel",
    "alembic",
    "peewee",
    "knex",
    "kysely",
    "drizzle-orm",
    "sequelize",
    "dapper",
];

fn sql_client(lang: &str, module: &str) -> bool {
    import_candidates(lang, &module.to_ascii_lowercase())
        .iter()
        .any(|m| SQL_CLIENTS.contains(&m.as_str()))
}

/// The databases a Django `DATABASES` setting names by its engine:
/// `"ENGINE": "django.db.backends.postgresql"`.
pub fn django_engines(line: &str) -> Vec<(Cat, &'static str)> {
    let mut out = Vec::new();
    for key in ["django.db.backends.", "django.contrib.gis.db.backends."] {
        let mut rest = line;
        while let Some(at) = rest.find(key) {
            let quoted = rest[..at].ends_with(['"', '\'']);
            let tail = &rest[at + key.len()..];
            let engine: String = tail
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            let tech = match engine.as_str() {
                "postgresql" | "postgresql_psycopg2" | "postgis" => Some("PostgreSQL"),
                "mysql" => Some("MySQL"),
                "sqlite3" | "spatialite" => Some("SQLite"),
                "oracle" => Some("Oracle"),
                _ => None,
            };
            if quoted
                && let Some(t) = tech
                && !out.contains(&(Cat::Db, t))
            {
                out.push((Cat::Db, t));
            }
            rest = tail;
        }
    }
    out
}

/// The broker a Celery app's broker URL names: `broker_url =
/// "redis://cache:6379/0"`, `Celery("app", broker="amqp://mq//")`.
pub fn celery_broker_in(line: &str) -> Option<(Cat, &'static str)> {
    let l = line.to_ascii_lowercase();
    if !(l.contains("broker_url") || l.contains("broker=") || l.contains("broker =")) {
        return None;
    }
    url_broker(&l)
}

fn url_broker(l: &str) -> Option<(Cat, &'static str)> {
    if l.contains("redis://") || l.contains("rediss://") {
        Some((Cat::Cache, "Redis"))
    } else if l.contains("amqp://") || l.contains("amqps://") || l.contains("pyamqp://") {
        Some((Cat::Bus, "RabbitMQ"))
    } else if l.contains("sqs://") {
        Some((Cat::Bus, "Amazon SQS"))
    } else {
        None
    }
}

/// The broker a `celery[...]` requirement's extras name: `celery[redis]`.
pub fn celery_broker_extra(line: &str) -> Option<(Cat, &'static str)> {
    let l = line.to_ascii_lowercase();
    let at = l.find("celery[")?;
    let extras = &l[at + "celery[".len()..];
    let extras = &extras[..extras.find(']')?];
    extras.split(',').map(str::trim).find_map(|e| match e {
        "redis" => Some((Cat::Cache, "Redis")),
        "librabbitmq" | "pyamqp" => Some((Cat::Bus, "RabbitMQ")),
        "sqs" => Some((Cat::Bus, "Amazon SQS")),
        _ => None,
    })
}

/// Known hosts of external services, by domain.
const HOSTS: &[(&str, &str)] = &[
    ("stripe.com", "Stripe"),
    ("openai.com", "OpenAI"),
    ("anthropic.com", "Anthropic"),
    ("github.com", "GitHub"),
    ("githubusercontent.com", "GitHub"),
    ("slack.com", "Slack"),
    ("sendgrid.com", "SendGrid"),
    ("twilio.com", "Twilio"),
    ("resend.com", "Resend"),
    ("googleapis.com", "Google APIs"),
];

fn host_name(host: &str) -> String {
    HOSTS
        .iter()
        .find(|(d, _)| host == *d || host.ends_with(&format!(".{d}")))
        .map(|(_, n)| n.to_string())
        .unwrap_or_else(|| host.to_string())
}

/// A use whose resource may still be unnamed.
struct Pending {
    unit: usize,
    cat: Cat,
    tech: String,
    verb: Verb,
    how: How,
    item: Option<String>,
    qual: Option<String>,
    via: Option<usize>,
    ev: Evidence,
    /// For a secret: the infra unit providing it.
    provider: Option<usize>,
    /// A database reached in SQL: a SQL object or a SQL-only client.
    sql: bool,
}

impl Pending {
    fn new(unit: usize, cat: Cat, tech: &str, verb: Verb, how: How, ev: Evidence) -> Self {
        Pending {
            unit,
            cat,
            tech: tech.to_string(),
            verb,
            how,
            item: None,
            qual: None,
            via: None,
            ev,
            provider: None,
            sql: false,
        }
    }
}

/// A service of a deployment file: a resource, or an app.
#[derive(Clone, Copy, Debug)]
enum Svc {
    Res(usize),
    App(usize),
}

struct Reg {
    res: Vec<Res>,
    at: BTreeMap<(Cat, String), usize>,
}

impl Reg {
    fn get(&mut self, cat: Cat, tech: &str) -> usize {
        let k = (cat, tech.to_string());
        if let Some(&i) = self.at.get(&k) {
            return i;
        }
        self.at.insert(k, self.res.len());
        self.res.push(Res {
            cat,
            tech: tech.to_string(),
            items: BTreeMap::new(),
            declared: Vec::new(),
            kinds: BTreeMap::new(),
            units: Vec::new(),
        });
        self.res.len() - 1
    }

    fn declare(&mut self, i: usize, ev: &Evidence, item: Option<(&str, &'static str)>) {
        let r = &mut self.res[i];
        if !r.declared.contains(ev) && r.declared.len() < MAX_DECLARED {
            r.declared.push(ev.clone());
        }
        if let Some((name, k)) = item
            && !name.is_empty()
        {
            r.items.entry(name.to_string()).or_insert(k);
        }
    }
}

/// The code a Serverless function runs: its handler's file, beside the
/// `serverless.yml` that names it.
pub fn serverless_code(file: &str, handler: &str) -> String {
    let dir = dir_of(file);
    if handler.is_empty() {
        dir
    } else {
        super::units::join_rel_pub(&dir, handler).unwrap_or(dir)
    }
}

/// A file only tests run: a test file or folder (`tests/`, `*_test.go`,
/// `*.test.ts`, `test_*.py`), pytest's `conftest.py`, or a compose file
/// for a test or CI stack (`docker-compose.test.yml`,
/// `docker-compose.ci.yml`). What it uses or declares is not a resource of
/// the system.
pub fn test_only(path: &str) -> bool {
    if crate::indexer::test_detection::is_test_file(path) {
        return true;
    }
    let name = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    if name == "conftest.py" {
        return true;
    }
    let stem = name
        .strip_prefix("docker-compose")
        .or_else(|| name.strip_prefix("compose"));
    stem.is_some_and(|rest| {
        rest.split(['.', '-', '_']).any(|w| {
            matches!(
                w,
                "test" | "tests" | "testing" | "ci" | "e2e" | "integration"
            )
        })
    })
}

fn dir_of(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(a, _)| format!("{a}/"))
        .unwrap_or_default()
}

/// Every resource of the repository and every use of one.
pub(super) fn find(inp: &Input) -> Found {
    let units = inp.units;
    let n = units.len();
    let code_unit = |u: usize| {
        u < n
            && !units[u].kind.hidden()
            && !matches!(units[u].kind, Kind::Infra | Kind::Data | Kind::Tooling)
    };
    let is_lib = |u: usize| matches!(units[u].kind, Kind::Lib | Kind::Loose);
    let mut reg = Reg {
        res: Vec::new(),
        at: BTreeMap::new(),
    };
    let mut found = Found::default();
    let mut pend: Vec<Pending> = Vec::new();
    // Owner and scope of every live file, looked up once.
    let file_owner: HashMap<&str, Option<usize>> = inp
        .files
        .iter()
        .map(|(p, _, _)| (p.as_str(), inp.owners.owner(p)))
        .collect();
    let file_scope: HashMap<&str, bool> = inp
        .files
        .iter()
        .map(|(p, _, _)| (p.as_str(), (inp.in_scope)(p)))
        .collect();
    let own = |p: &str| -> Option<usize> {
        file_owner
            .get(p)
            .copied()
            .unwrap_or_else(|| inp.owners.owner(p))
    };
    let scoped = |p: &str| -> bool {
        file_scope
            .get(p)
            .copied()
            .unwrap_or_else(|| (inp.in_scope)(p))
    };

    // ---- declared by infrastructure, app hosts and schemas
    let mut svc: HashMap<(usize, String), Svc> = HashMap::new();
    let mut secret_stores: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
    let app_by_alias = |alias: &str| -> Option<usize> {
        let a = super::units::norm(alias);
        let m: Vec<usize> = (0..n)
            .filter(|&j| code_unit(j) && units[j].aliases.contains(&a))
            .collect();
        (m.len() == 1).then(|| m[0])
    };
    for (u, unit) in units.iter().enumerate() {
        for d in unit
            .decls
            .iter()
            .filter(|d| !test_only(&d.ev.file) && scoped(&d.ev.file))
        {
            let lname = d.name.to_ascii_lowercase();
            // Compose files are stacks of their own: `db` in one is not `db`
            // in another. The name alone stays for overrides that wire to a
            // service another file defines.
            let in_file = (d.source == "compose").then(|| (u, format!("{}#{lname}", d.ev.file)));
            match d.source {
                "bicep" | "terraform" | "cfn" => {
                    if let Some((cat, tech)) = classify_iac(&d.what) {
                        let i = reg.get(cat, tech);
                        *reg.res[i].kinds.entry(d.what.clone()).or_default() += 1;
                        reg.declare(i, &d.ev, item_kind(&d.what).map(|k| (d.name.as_str(), k)));
                        if cat == Cat::Secrets {
                            secret_stores.entry(u).or_default().insert(tech.to_string());
                        }
                    } else if let Some(rt) = runtime_of(&d.what) {
                        found.runtimes.entry(u).or_default().insert(rt);
                    } else {
                        *found
                            .other
                            .entry(u)
                            .or_default()
                            .entry(d.what.clone())
                            .or_default() += 1;
                    }
                }
                "k8s" | "compose" | "aspire" | "helm" => {
                    if let Some(img) = d.what.strip_prefix("image:") {
                        if let Some((cat, tech)) = classify_image(img) {
                            let i = reg.get(cat, tech);
                            reg.declare(i, &d.ev, Some((d.name.as_str(), "service")));
                            svc.insert((u, lname.clone()), Svc::Res(i));
                            if let Some(k) = &in_file {
                                svc.insert(k.clone(), Svc::Res(i));
                            }
                            if !d.group.is_empty() {
                                svc.insert((u, d.group.to_ascii_lowercase()), Svc::Res(i));
                            }
                        } else if let Some(a) =
                            super::units::image_name_pub(img).and_then(|a| app_by_alias(&a))
                        {
                            svc.entry((u, lname.clone())).or_insert(Svc::App(a));
                            if let Some(k) = &in_file {
                                svc.insert(k.clone(), Svc::App(a));
                            }
                            if !d.group.is_empty() {
                                svc.entry((u, d.group.to_ascii_lowercase()))
                                    .or_insert(Svc::App(a));
                            }
                        }
                    } else if let Some(ctx) = d.what.strip_prefix("build:") {
                        let rel = format!("{}/", ctx.trim_end_matches('/'));
                        if let Some(full) = super::units::join_rel_pub(&dir_of(&d.ev.file), &rel)
                            && let Some(a) = own(&full).filter(|&a| code_unit(a))
                        {
                            svc.insert((u, lname.clone()), Svc::App(a));
                            if let Some(k) = &in_file {
                                svc.insert(k.clone(), Svc::App(a));
                            }
                        }
                    } else if let Some(target) = d.what.strip_prefix("project:") {
                        let a = if let Some(alias) = target.strip_prefix("alias:") {
                            app_by_alias(alias)
                        } else if let Some(dir) = target.strip_prefix("dir:") {
                            own(dir).filter(|&a| code_unit(a))
                        } else {
                            None
                        };
                        if let Some(a) = a {
                            svc.insert((u, lname.clone()), Svc::App(a));
                        }
                    } else if let Some(dep) = d.what.strip_prefix("dep:") {
                        if let Some((cat, tech)) = classify_image(dep) {
                            let i = reg.get(cat, tech);
                            reg.declare(i, &d.ev, Some((dep, "chart")));
                            svc.insert((u, dep.to_ascii_lowercase()), Svc::Res(i));
                        }
                    } else if let Some(provider) = d.what.strip_prefix("kind:SecretProviderClass:")
                    {
                        let tech = match provider {
                            "azure" => Some("Azure Key Vault"),
                            "aws" => Some("AWS Secrets Manager"),
                            "gcp" => Some("Secret Manager"),
                            "vault" => Some("Vault"),
                            _ => None,
                        };
                        if let Some(t) = tech {
                            let i = reg.get(Cat::Secrets, t);
                            reg.declare(i, &d.ev, None);
                            secret_stores.entry(u).or_default().insert(t.to_string());
                        }
                    } else if d.what == "kind:Secret" {
                        secret_stores
                            .entry(u)
                            .or_default()
                            .insert("Kubernetes Secrets".to_string());
                    } else if d.source == "aspire"
                        && let Some((cat, tech)) = classify_aspire(&d.what)
                    {
                        let i = reg.get(cat, tech);
                        reg.declare(i, &d.ev, None);
                        svc.insert((u, lname.clone()), Svc::Res(i));
                        if cat == Cat::Secrets {
                            secret_stores.entry(u).or_default().insert(tech.to_string());
                        }
                    } else if d.source == "aspire"
                        && !d.group.is_empty()
                        && let Some(Svc::Res(i)) =
                            svc.get(&(u, d.group.to_ascii_lowercase())).copied()
                    {
                        // A child (database, blob container) of a resource.
                        let k = match d.what.as_str() {
                            "AddDatabase" => "database",
                            "AddBlobs" | "AddBlobContainer" => "container",
                            "AddQueues" | "AddServiceBusQueue" => "queue",
                            "AddServiceBusTopic" => "topic",
                            _ => "resource",
                        };
                        reg.declare(i, &d.ev, Some((d.name.as_str(), k)));
                        svc.insert((u, lname.clone()), Svc::Res(i));
                    }
                }
                "serverless" => {
                    let Some(app) =
                        own(&serverless_code(&d.ev.file, &d.group)).filter(|&a| code_unit(a))
                    else {
                        continue;
                    };
                    if d.what == "ssm" {
                        let mut p = Pending::new(
                            app,
                            Cat::Config,
                            "SSM Parameter Store",
                            Verb::Uses,
                            How::Deploy,
                            d.ev.clone(),
                        );
                        p.qual = Some(d.name.clone());
                        pend.push(p);
                        continue;
                    }
                    if d.what == "function" {
                        let runs = found.runs_on.entry(app).or_default();
                        if runs.len() < MAX_DECLARED {
                            runs.push(("AWS Lambda", d.ev.clone()));
                        }
                        continue;
                    }
                    let (cat, tech) = match d.what.as_str() {
                        "event:sns" => (Cat::Bus, "Amazon SNS"),
                        "event:sqs" => (Cat::Bus, "Amazon SQS"),
                        "event:kinesis" => (Cat::Bus, "Kinesis"),
                        "event:kafka" => (Cat::Bus, "Kafka"),
                        "event:rabbitmq" => (Cat::Bus, "RabbitMQ"),
                        "event:eventbridge" => (Cat::Bus, "EventBridge"),
                        "event:dynamodb" => (Cat::Db, "DynamoDB"),
                        "event:s3" => (Cat::Storage, "Amazon S3"),
                        _ => continue,
                    };
                    let mut p =
                        Pending::new(app, cat, tech, Verb::Subscribes, How::Deploy, d.ev.clone());
                    p.qual = Some(d.name.clone());
                    pend.push(p);
                }
                "prisma" => {
                    let provider = d.what.strip_prefix("provider:").unwrap_or("");
                    let (cat, tech) = classify_dialect(provider).unwrap_or((Cat::Db, ""));
                    let i = reg.get(cat, tech);
                    reg.declare(i, &d.ev, None);
                    if code_unit(u) {
                        pend.push(Pending::new(
                            u,
                            cat,
                            tech,
                            Verb::ReadsWrites,
                            How::Import,
                            d.ev.clone(),
                        ));
                    }
                }
                _ => {}
            }
        }
        // Kubernetes Services by their selector.
        for d in unit
            .decls
            .iter()
            .filter(|d| d.what == "kind:Service" && !d.group.is_empty())
        {
            if let Some(s) = svc.get(&(u, d.group.to_ascii_lowercase())).copied() {
                svc.entry((u, d.name.to_ascii_lowercase())).or_insert(s);
            }
        }
    }
    // SQL projects and folders of SQL: databases of their own.
    for (u, unit) in units.iter().enumerate() {
        if unit.kind != Kind::Data {
            continue;
        }
        let sqlproj = unit
            .manifests
            .iter()
            .any(|m| m.to_ascii_lowercase().ends_with(".sqlproj"));
        let postgres = inp
            .files
            .iter()
            .any(|(p, l, _)| l == "postgres" && own(p) == Some(u));
        let tech = if sqlproj {
            "SQL Server"
        } else if postgres {
            "PostgreSQL"
        } else {
            ""
        };
        let i = reg.get(Cat::Db, tech);
        reg.res[i].units.push(u);
        let ev = Evidence {
            file: unit
                .manifests
                .first()
                .cloned()
                .unwrap_or_else(|| unit.display_root()),
            line: 0,
            text: format!("SQL in {}", unit.display_root()),
        };
        reg.declare(i, &ev, None);
    }

    // ---- packages the manifests declare
    let mut texts: HashMap<String, Option<std::rc::Rc<String>>> = HashMap::new();
    let mut read = |path: &str| -> Option<std::rc::Rc<String>> {
        texts
            .entry(path.to_string())
            .or_insert_with(|| {
                let abs = inp.root.join(path);
                let meta = std::fs::metadata(&abs).ok()?;
                if meta.len() > MAX_SOURCE {
                    return None;
                }
                std::fs::read_to_string(abs).ok().map(std::rc::Rc::new)
            })
            .clone()
    };
    // Go has no dev dependencies: `go.mod` requires what only `_test.go`
    // files import too. A module counts once code outside tests imports it.
    let mut go_imports: BTreeSet<String> = BTreeSet::new();
    for (path, lang, _) in inp.files {
        if lang != "go" || test_only(path) || !scoped(path) {
            continue;
        }
        let Some(text) = read(path) else { continue };
        let mut in_block = false;
        for line in text.lines() {
            let t = line.trim();
            if t.starts_with("import (") {
                in_block = true;
                continue;
            }
            if in_block && t.starts_with(')') {
                in_block = false;
                continue;
            }
            go_imports.extend(imports_of("go", line, in_block));
        }
    }
    for (u, unit) in units.iter().enumerate() {
        if !code_unit(u) {
            continue;
        }
        let go = unit.manifests.iter().any(|m| m.ends_with("go.mod"));
        let mut manifests = unit.manifests.clone();
        for req in ["requirements.txt", "requirements.in"] {
            manifests.push(format!("{}{req}", unit.root));
        }
        for p in &unit.packages {
            let Some((cat, tech)) = classify_package(p) else {
                continue;
            };
            if go
                && !go_imports
                    .iter()
                    .any(|m| m == p || m.starts_with(&format!("{p}/")))
            {
                continue;
            }
            if cat == Cat::Ext && tech.is_empty() {
                continue;
            }
            let ev = manifests
                .iter()
                .find_map(|m| {
                    let text = read(m)?;
                    let (i, l) = text
                        .lines()
                        .enumerate()
                        .find(|(_, l)| l.contains(p.as_str()))?;
                    Some(Evidence {
                        file: m.clone(),
                        line: i as i64 + 1,
                        text: l.trim().chars().take(160).collect(),
                    })
                })
                .unwrap_or_else(|| Evidence {
                    file: unit.manifests.first().cloned().unwrap_or_default(),
                    line: 0,
                    text: p.clone(),
                });
            let mut x = Pending::new(u, cat, tech, Verb::Uses, How::Manifest, ev);
            x.sql = cat == Cat::Db && tech.is_empty() && sql_client("", p);
            pend.push(x);
        }
    }

    // ---- imports in the code
    // Files whose own code uses a resource: (cat, tech, verb).
    // `(category, technology, verb, reached in SQL)`.
    let mut file_res: HashMap<String, BTreeSet<(Cat, String, Verb, bool)>> = HashMap::new();
    for (path, lang, _) in inp.files {
        if !CODE.contains(&lang.as_str()) || !scoped(path) || test_only(path) {
            continue;
        }
        let Some(u) = own(path).filter(|&u| code_unit(u)) else {
            continue;
        };
        let Some(text) = read(path) else { continue };
        let mut in_go_block = false;
        let mut umbrella = false;
        let mut seen: BTreeSet<(Cat, &'static str)> = BTreeSet::new();
        let mut hits: Vec<(Cat, &'static str, i64, String, bool)> = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let t = line.trim();
            if lang == "go" {
                if t.starts_with("import (") {
                    in_go_block = true;
                    continue;
                }
                if in_go_block && t.starts_with(')') {
                    in_go_block = false;
                    continue;
                }
            }
            for m in imports_of(lang, line, in_go_block) {
                let Some((cat, tech)) = classify_import(lang, &m) else {
                    continue;
                };
                if cat == Cat::Ext && tech.is_empty() {
                    umbrella = true;
                    continue;
                }
                if seen.insert((cat, tech)) {
                    let sql = cat == Cat::Db && tech.is_empty() && sql_client(lang, &m);
                    hits.push((cat, tech, i as i64 + 1, t.chars().take(160).collect(), sql));
                }
            }
            if lang == "python" {
                for (cat, tech) in django_engines(line) {
                    if seen.insert((cat, tech)) {
                        hits.push((cat, tech, i as i64 + 1, t.chars().take(160).collect(), true));
                    }
                }
            }
        }
        if umbrella {
            for (i, line) in text.lines().enumerate() {
                for (cat, tech) in aws_services(line) {
                    if seen.insert((cat, tech)) {
                        hits.push((
                            cat,
                            tech,
                            i as i64 + 1,
                            line.trim().chars().take(160).collect(),
                            false,
                        ));
                    }
                }
            }
        }
        for (cat, tech, line, t, sql) in hits {
            let verb = default_verb(cat);
            file_res
                .entry(path.clone())
                .or_default()
                .insert((cat, tech.to_string(), verb, sql));
            let mut x = Pending::new(
                u,
                cat,
                tech,
                verb,
                How::Import,
                Evidence {
                    file: path.clone(),
                    line,
                    text: t,
                },
            );
            x.sql = sql;
            pend.push(x);
        }
    }

    // ---- Celery's broker: a Celery app is a client of the broker its
    // broker URL or its requirement's extra names.
    let celery_units: BTreeSet<usize> = pend
        .iter()
        .filter(|p| p.tech == "Celery")
        .map(|p| p.unit)
        .collect();
    for u in celery_units {
        let mut broker: Option<(Cat, &'static str, Evidence, How)> = None;
        for (path, lang, _) in inp.files {
            if broker.is_some() {
                break;
            }
            if lang != "python" || test_only(path) || !scoped(path) || own(path) != Some(u) {
                continue;
            }
            let Some(text) = read(path) else { continue };
            broker = text.lines().enumerate().find_map(|(i, l)| {
                let (c, t) = celery_broker_in(l)?;
                Some((c, t, ev(path, i as i64 + 1, l), How::Import))
            });
        }
        if broker.is_none() {
            let mut manifests = units[u].manifests.clone();
            for req in ["requirements.txt", "requirements.in"] {
                manifests.push(format!("{}{req}", units[u].root));
            }
            broker = manifests.iter().find_map(|m| {
                let text = read(m)?;
                text.lines().enumerate().find_map(|(i, l)| {
                    let (c, t) = celery_broker_extra(l.split('#').next().unwrap_or(""))?;
                    Some((c, t, ev(m, i as i64 + 1, l), How::Manifest))
                })
            });
        }
        let Some((cat, tech, at, how)) = broker else {
            continue;
        };
        for p in pend
            .iter_mut()
            .filter(|p| p.unit == u && p.tech == "Celery")
        {
            p.cat = cat;
            p.tech = tech.to_string();
            p.ev = at.clone();
            p.how = how;
        }
        for (_, set) in file_res.iter_mut().filter(|(f, _)| own(f) == Some(u)) {
            if set.iter().any(|(_, t, _, _)| t == "Celery") {
                set.retain(|(_, t, _, _)| t != "Celery");
                set.insert((cat, tech.to_string(), Verb::Uses, false));
            }
        }
    }

    // ---- edges: topics, secrets, SQL objects, HTTP calls out of the repo
    for (u, x) in inp.uses.iter().enumerate() {
        if !code_unit(u) {
            continue;
        }
        // A front end's events are in-process (a viewer's event bus) unless
        // its own code names a broker client.
        let in_process = units[u].kind == Kind::Ui
            && !pend
                .iter()
                .any(|p| p.unit == u && p.cat == Cat::Bus && !p.tech.is_empty());
        for (usage, verb, word) in [
            (&x.topics, Verb::Publishes, "publishes"),
            (&x.bus, Verb::Subscribes, "subscribes to"),
        ] {
            if in_process {
                continue;
            }
            for (k, (_, syms)) in &usage.keys {
                let Some((q, f, l)) = syms.iter().find(|(_, f, _)| !test_only(f)) else {
                    continue;
                };
                file_res.entry(f.clone()).or_default().insert((
                    Cat::Bus,
                    String::new(),
                    verb,
                    false,
                ));
                let mut p = Pending::new(
                    u,
                    Cat::Bus,
                    "",
                    verb,
                    How::Edge,
                    Evidence {
                        file: f.clone(),
                        line: *l,
                        text: format!("{word} {}", short_key(k)),
                    },
                );
                p.item = Some(short_key(k).to_string());
                p.qual = Some(q.clone());
                pend.push(p);
            }
        }
        for (k, (_, syms)) in &x.secrets.keys {
            let Some((q, f, l)) = syms.iter().find(|(_, f, _)| !test_only(f)) else {
                continue;
            };
            // Only a secret the repository's infrastructure provides.
            let Some(provider) = inp
                .links
                .iter()
                .find(|lv| {
                    lv.family == "config" && lv.from == u && lv.keys.iter().any(|kk| kk == k)
                })
                .map(|lv| lv.to)
            else {
                continue;
            };
            let mut p = Pending::new(
                u,
                Cat::Secrets,
                "",
                Verb::ReadsSecrets,
                How::Edge,
                Evidence {
                    file: f.clone(),
                    line: *l,
                    text: format!("reads secret {}", short_key(k)),
                },
            );
            p.item = Some(short_key(k).to_string());
            p.qual = Some(q.clone());
            p.provider = Some(provider);
            pend.push(p);
        }
    }
    let internal_hosts: BTreeSet<String> = svc
        .keys()
        .filter(|(_, s)| !s.contains('#'))
        .map(|(_, s)| s.clone())
        .collect();
    for r in inp.rows {
        if !scoped(&r.src_path) {
            continue;
        }
        let Some(u) = own(&r.src_path).filter(|&u| code_unit(u)) else {
            continue;
        };
        let tp = r.tgt_path.as_deref().unwrap_or("");
        let sql_target =
            matches!(r.tgt_lang.as_deref(), Some("sql" | "postgres")) && !tp.is_empty();
        if sql_target && !matches!(r.kind.as_str(), "CONTAINS" | "MODULE_FILE") {
            let owner = own(tp);
            let data = owner.filter(|&t| units[t].kind == Kind::Data);
            let (cat, tech) =
                match data.and_then(|t| reg.res.iter().position(|x| x.units.contains(&t))) {
                    Some(i) => (reg.res[i].cat, reg.res[i].tech.clone()),
                    None => (Cat::Db, String::new()),
                };
            let item = r
                .tgt_qual
                .as_deref()
                .map(|q| q.rsplit(['.', ':']).next().unwrap_or(q).to_string());
            file_res.entry(r.src_path.clone()).or_default().insert((
                cat,
                tech.clone(),
                Verb::ReadsWrites,
                true,
            ));
            let mut p = Pending::new(
                u,
                cat,
                &tech,
                Verb::ReadsWrites,
                How::Edge,
                Evidence {
                    file: r.ev_file.clone(),
                    line: r.ev_line.unwrap_or(r.src_line),
                    text: r
                        .ev
                        .clone()
                        .unwrap_or_default()
                        .trim()
                        .chars()
                        .take(160)
                        .collect(),
                },
            );
            p.item = item;
            p.qual = Some(r.src_qual.clone());
            p.sql = true;
            pend.push(p);
            continue;
        }
        if r.kind == "HTTP_CALL"
            && let Some(d) = &r.detail
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(d)
            && let Some(raw) = v["raw"].as_str()
            && let Some(host) = url_host(raw)
        {
            let first = host.split('.').next().unwrap_or("");
            if internal_hosts.contains(&host) || internal_hosts.contains(first) {
                continue;
            }
            let path = v["path"].as_str().unwrap_or("");
            let mut p = Pending::new(
                u,
                Cat::Ext,
                &host_name(&host),
                Verb::Calls,
                How::Edge,
                Evidence {
                    file: r.ev_file.clone(),
                    line: r.ev_line.unwrap_or(r.src_line),
                    text: raw.chars().take(160).collect(),
                },
            );
            p.item = Some(format!(
                "{} {host}{path}",
                v["method"].as_str().unwrap_or("GET")
            ));
            p.qual = Some(r.src_qual.clone());
            pend.push(p);
        }
    }

    // ---- deployment wiring
    for (u, unit) in units.iter().enumerate() {
        for w in unit.wires.iter().filter(|w| !test_only(&w.ev.file)) {
            let look = |name: &str| {
                let name = name.to_ascii_lowercase();
                svc.get(&(u, format!("{}#{name}", w.ev.file)))
                    .or_else(|| svc.get(&(u, name)))
                    .copied()
            };
            let Some(Svc::App(app)) = look(&w.from) else {
                continue;
            };
            let i = match look(&w.to) {
                Some(Svc::Res(i)) => i,
                Some(Svc::App(to)) => {
                    if to != app
                        && !found
                            .app_wires
                            .iter()
                            .any(|(a, b, _)| *a == app && *b == to)
                    {
                        found.app_wires.push((app, to, w.ev.clone()));
                    }
                    continue;
                }
                None => continue,
            };
            let (cat, tech) = (reg.res[i].cat, reg.res[i].tech.clone());
            let mut p = Pending::new(app, cat, &tech, Verb::Uses, How::Deploy, w.ev.clone());
            p.item = Some(w.to.clone());
            pend.push(p);
        }
    }

    // ---- through libraries: an app calling into a library file that
    // uses a resource uses it too.
    let mut lib_files: HashMap<String, BTreeSet<(Cat, String, Verb, bool)>> = file_res
        .iter()
        .filter(|(f, _)| own(f).is_some_and(is_lib))
        .map(|(f, s)| (f.clone(), s.clone()))
        .collect();
    let code_kind = |k: &str| {
        matches!(
            k,
            "CALLS" | "USES" | "REFERENCES" | "IMPORTS" | "IMPORTS_FILE" | "EXTENDS" | "DISPATCH"
        )
    };
    for _ in 0..3 {
        let mut grew = false;
        for r in inp.rows {
            let Some(tp) = &r.tgt_path else { continue };
            if !code_kind(&r.kind) || tp == &r.src_path {
                continue;
            }
            let Some(set) = lib_files.get(tp).cloned() else {
                continue;
            };
            if own(&r.src_path).is_some_and(is_lib) {
                let e = lib_files.entry(r.src_path.clone()).or_default();
                let before = e.len();
                e.extend(set);
                grew |= e.len() > before;
            }
        }
        if !grew {
            break;
        }
    }
    let mut via_seen: BTreeSet<(usize, usize, Cat, String, Verb, bool)> = BTreeSet::new();
    for r in inp.rows {
        let Some(tp) = &r.tgt_path else { continue };
        if !code_kind(&r.kind) || !scoped(&r.src_path) {
            continue;
        }
        let (Some(a), Some(l)) = (own(&r.src_path), own(tp)) else {
            continue;
        };
        if a == l || !code_unit(a) || is_lib(a) || !is_lib(l) {
            continue;
        }
        let Some(set) = lib_files.get(tp) else {
            continue;
        };
        for (cat, tech, verb, sql) in set {
            if !via_seen.insert((a, l, *cat, tech.clone(), *verb, *sql)) {
                continue;
            }
            let mut p = Pending::new(
                a,
                *cat,
                tech,
                *verb,
                How::Via,
                Evidence {
                    file: r.ev_file.clone(),
                    line: r.ev_line.unwrap_or(r.src_line),
                    text: format!(
                        "{} → {}",
                        r.src_qual,
                        r.tgt_qual.clone().unwrap_or_else(|| tp.clone())
                    ),
                },
            );
            p.qual = Some(r.src_qual.clone());
            p.via = Some(l);
            p.sql = *sql;
            pend.push(p);
        }
    }

    // ---- settle the technology a use does not name
    let mut reach: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    for (u, r) in reach.iter_mut().enumerate() {
        let mut stack = vec![u];
        r.insert(u);
        while let Some(x) = stack.pop() {
            for (a, b, _) in inp.deps {
                if *a == x && r.insert(*b) {
                    stack.push(*b);
                }
            }
        }
    }
    // Technologies named per unit and category, and in the whole repository.
    let mut by_unit: HashMap<(usize, Cat), BTreeSet<String>> = HashMap::new();
    let mut by_cat: HashMap<Cat, BTreeSet<String>> = HashMap::new();
    for p in pend.iter().filter(|p| !p.tech.is_empty()) {
        by_unit
            .entry((p.unit, p.cat))
            .or_default()
            .insert(p.tech.clone());
        by_cat.entry(p.cat).or_default().insert(p.tech.clone());
    }
    let named_by = |cat: Cat, scope: &dyn Fn(usize) -> bool| -> BTreeSet<String> {
        by_unit
            .iter()
            .filter(|((u, c), _)| *c == cat && scope(*u))
            .flat_map(|(_, s)| s.iter().cloned())
            .collect()
    };
    let one = |s: &BTreeSet<String>| -> Option<String> {
        (s.len() == 1).then(|| s.iter().next().cloned()).flatten()
    };
    let all_stores: BTreeSet<String> = secret_stores.values().flatten().cloned().collect();
    let settled: Vec<String> = pend
        .iter()
        .map(|p| {
            if !p.tech.is_empty() {
                return p.tech.clone();
            }
            let tech = match p.cat {
                Cat::Secrets => p
                    .provider
                    .and_then(|pv| secret_stores.get(&pv))
                    .and_then(one)
                    .or_else(|| one(&named_by(Cat::Secrets, &|x| x == p.unit)))
                    .or_else(|| one(&all_stores)),
                cat => {
                    let mine = &reach[p.unit];
                    // SQL reaches a SQL database, not a document store.
                    let fits = |mut s: BTreeSet<String>| {
                        if p.sql {
                            s.retain(|t| !NOT_SQL.contains(&t.as_str()));
                        }
                        s
                    };
                    one(&fits(named_by(cat, &|x| mine.contains(&x)))).or_else(|| {
                        let mut all = by_cat.get(&cat).cloned().unwrap_or_default();
                        all.extend(
                            reg.res
                                .iter()
                                .filter(|r| {
                                    r.cat == cat && !r.tech.is_empty() && !r.declared.is_empty()
                                })
                                .map(|r| r.tech.clone()),
                        );
                        one(&fits(all))
                    })
                }
            };
            tech.unwrap_or_default()
        })
        .collect();
    for (p, tech) in pend.into_iter().zip(settled) {
        let i = reg.get(p.cat, &tech);
        if let Some(item) = &p.item
            && p.how == How::Edge
        {
            let k = match p.cat {
                Cat::Bus => "topic",
                Cat::Secrets => "secret",
                Cat::Ext => "call",
                _ => "object",
            };
            reg.res[i].items.entry(item.clone()).or_insert(k);
        }
        found.uses.push(Use {
            unit: p.unit,
            res: i,
            verb: p.verb,
            how: p.how,
            item: p.item,
            qual: p.qual,
            via: p.via,
            ev: p.ev,
        });
    }
    let mut alias: HashMap<usize, usize> = HashMap::new();
    // A folder of SQL in no named dialect is the repository's one SQL
    // database, when it has exactly one.
    if let Some(&i) = reg.at.get(&(Cat::Db, String::new()))
        && !reg.res[i].units.is_empty()
    {
        let used: BTreeSet<usize> = found.uses.iter().map(|x| x.res).collect();
        let sql: Vec<usize> = (0..reg.res.len())
            .filter(|&j| {
                let r = &reg.res[j];
                r.cat == Cat::Db
                    && !r.tech.is_empty()
                    && !NOT_SQL.contains(&r.tech.as_str())
                    && (!r.declared.is_empty() || used.contains(&j))
            })
            .collect();
        if let [j] = sql[..] {
            let units = std::mem::take(&mut reg.res[i].units);
            let declared = std::mem::take(&mut reg.res[i].declared);
            let items = std::mem::take(&mut reg.res[i].items);
            let r = &mut reg.res[j];
            r.units.extend(units);
            for d in declared {
                if !r.declared.contains(&d) && r.declared.len() < MAX_DECLARED {
                    r.declared.push(d);
                }
            }
            for (k, v) in items {
                r.items.entry(k).or_insert(v);
            }
            if !used.contains(&i) {
                alias.insert(i, j);
            }
        }
    }
    // A managed service the infrastructure declares and the engine the code
    // names are one resource: Azure SQL is what a SQL Server client reaches.
    for (cat, managed, engine) in MANAGED {
        if let (Some(&i), Some(&j)) = (
            reg.at.get(&(*cat, managed.to_string())),
            reg.at.get(&(*cat, engine.to_string())),
        ) && !alias.contains_key(&j)
            && !reg.res[i].declared.is_empty()
            && reg.res[j].declared.iter().all(|e| e.line == 0)
        {
            alias.insert(j, i);
            let moved = std::mem::replace(
                &mut reg.res[j],
                Res {
                    cat: *cat,
                    tech: engine.to_string(),
                    items: BTreeMap::new(),
                    declared: Vec::new(),
                    kinds: BTreeMap::new(),
                    units: Vec::new(),
                },
            );
            let r = &mut reg.res[i];
            r.items.extend(moved.items);
            r.units.extend(moved.units);
            r.declared.extend(moved.declared);
            for x in found.uses.iter_mut().filter(|x| x.res == j) {
                x.res = i;
            }
        }
    }
    // Files whose code uses a resource, for the flows: a technology the
    // file does not name is the one its unit's uses settled on.
    let mut unit_res: HashMap<(usize, Cat), BTreeSet<usize>> = HashMap::new();
    for x in &found.uses {
        if !reg.res[x.res].tech.is_empty() {
            unit_res
                .entry((x.unit, reg.res[x.res].cat))
                .or_default()
                .insert(x.res);
        }
    }
    for (file, set) in file_res.iter().chain(lib_files.iter()) {
        let unit = own(file);
        for (cat, tech, _, _) in set {
            let i = if !tech.is_empty() {
                reg.at.get(&(*cat, tech.clone())).copied()
            } else {
                unit.and_then(|u| unit_res.get(&(u, *cat)))
                    .filter(|s| s.len() == 1)
                    .and_then(|s| s.iter().next().copied())
                    .or_else(|| reg.at.get(&(*cat, String::new())).copied())
            };
            if let Some(i) = i.map(|i| alias.get(&i).copied().unwrap_or(i)) {
                found.file_res.entry(file.clone()).or_default().insert(i);
            }
        }
    }
    found.res = reg.res;
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packages_and_imports_name_their_resource() {
        assert_eq!(
            classify_package("Npgsql.EntityFrameworkCore.PostgreSQL"),
            Some((Cat::Db, "PostgreSQL"))
        );
        assert_eq!(classify_package("npgsqlx"), None);
        assert_eq!(classify_package("@sentry/node"), Some((Cat::Ext, "Sentry")));
        assert_eq!(
            classify_package("StackExchange.Redis"),
            Some((Cat::Cache, "Redis"))
        );
        assert_eq!(classify_package("kafkajs"), Some((Cat::Bus, "Kafka")));
        assert_eq!(classify_package("sqlmodel"), Some((Cat::Db, "")));
        assert_eq!(classify_package("react"), None);
        assert_eq!(
            classify_import("python", "azure.storage.blob.aio"),
            Some((Cat::Storage, "Azure Blob Storage"))
        );
        assert_eq!(
            classify_import("go", "github.com/redis/go-redis/v9"),
            Some((Cat::Cache, "Redis"))
        );
        assert_eq!(classify_import("go", "github.com/redis-other/x"), None);
        assert_eq!(
            classify_import("csharp", "Azure.Messaging.ServiceBus.Administration"),
            Some((Cat::Bus, "Azure Service Bus"))
        );
        assert_eq!(
            imports_of("python", "from google.cloud import pubsub_v1", false),
            ["google.cloud", "google.cloud.pubsub_v1"]
        );
        assert_eq!(
            classify_import("python", "google.cloud.pubsub_v1"),
            Some((Cat::Bus, "Pub/Sub"))
        );
        assert_eq!(
            imports_of("python", "import redis, os", false),
            ["redis", "os"]
        );
        assert_eq!(
            imports_of("typescript", "import { Kafka } from 'kafkajs';", false),
            ["kafkajs"]
        );
        assert_eq!(
            imports_of("javascript", "const AWS = require(\"aws-sdk\");", false),
            ["aws-sdk"]
        );
        assert_eq!(
            imports_of(
                "typescript",
                "import x from '@aws-sdk/client-s3/dist';",
                false
            ),
            ["@aws-sdk/client-s3"]
        );
        assert_eq!(
            imports_of("typescript", "import x from './local';", false),
            Vec::<String>::new()
        );
        assert_eq!(
            imports_of("go", "\t\"github.com/jackc/pgx/v5\"", true),
            ["github.com/jackc/pgx/v5"]
        );
        assert_eq!(
            imports_of("go", "\t\"github.com/jackc/pgx/v5\"", false),
            Vec::<String>::new()
        );
        assert_eq!(
            imports_of("csharp", "using StackExchange.Redis;", false),
            ["StackExchange.Redis"]
        );
        assert_eq!(
            imports_of("csharp", "using var x = new Foo();", false),
            Vec::<String>::new()
        );
        assert_eq!(
            imports_of("rust", "use rusqlite::Connection;", false),
            ["rusqlite"]
        );
        assert_eq!(
            aws_services("const sns = new AWS.SNS(); const d = new AWS.DynamoDB.DocumentClient();"),
            [(Cat::Bus, "Amazon SNS"), (Cat::Db, "DynamoDB")]
        );
        assert_eq!(
            aws_services("s3 = boto3.client('s3')"),
            [(Cat::Storage, "Amazon S3")]
        );
    }

    #[test]
    fn infrastructure_names_its_resource() {
        assert_eq!(
            classify_iac("Microsoft.ServiceBus/namespaces/topics@2022-10-01"),
            Some((Cat::Bus, "Azure Service Bus"))
        );
        assert_eq!(
            classify_iac("Microsoft.Storage/storageAccounts/queueServices"),
            Some((Cat::Bus, "Azure Queue Storage"))
        );
        assert_eq!(
            classify_iac("Microsoft.Storage/storageAccounts"),
            Some((Cat::Storage, "Azure Blob Storage"))
        );
        assert_eq!(
            classify_iac("aws_sqs_queue"),
            Some((Cat::Bus, "Amazon SQS"))
        );
        assert_eq!(
            classify_iac("AWS::DynamoDB::Table"),
            Some((Cat::Db, "DynamoDB"))
        );
        assert_eq!(classify_iac("Microsoft.Network/virtualNetworks"), None);
        assert_eq!(
            runtime_of("Microsoft.App/containerApps@2024-03-01"),
            Some("Azure Container Apps")
        );
        assert_eq!(
            classify_image("docker.io/library/postgres:16-alpine"),
            Some((Cat::Db, "PostgreSQL"))
        );
        assert_eq!(
            classify_image("mcr.microsoft.com/mssql/server:2022-latest"),
            Some((Cat::Db, "SQL Server"))
        );
        assert_eq!(
            classify_image("localhost:5000/redis"),
            Some((Cat::Cache, "Redis"))
        );
        assert_eq!(classify_image("registry/team/orders-api:1.0"), None);
        assert_eq!(classify_image("${IMAGE}"), None);
        assert_eq!(hosts_in("redis-cart:6379"), ["redis-cart"]);
        assert_eq!(hosts_in("postgresql://app:pw@db:5432/app"), ["db"]);
        assert_eq!(hosts_in("Host=pg;Port=5432;Database=x"), ["pg"]);
        assert_eq!(hosts_in("some sentence here"), Vec::<String>::new());
        assert_eq!(
            url_host("https://api.github.com/repos/x"),
            Some("api.github.com".to_string())
        );
        assert_eq!(url_host("http://localhost:3000/x"), None);
        assert_eq!(url_host("/api/x"), None);
        assert_eq!(url_host("https://${HOST}/x"), None);
    }

    #[test]
    fn yaml_compose_and_kubernetes_declare_and_wire() {
        let compose = "services:\n  db:\n    image: postgres:16\n  api:\n    build: ./api\n    depends_on:\n      - db\n    environment:\n      DATABASE_URL: postgres://u:p@db:5432/app\n";
        let (d, w) = scan_yaml("docker-compose.yml", compose, "compose");
        let whats: Vec<(&str, &str)> = d
            .iter()
            .map(|x| (x.what.as_str(), x.name.as_str()))
            .collect();
        assert_eq!(whats, [("image:postgres:16", "db"), ("build:./api", "api")]);
        let wires: Vec<(&str, &str, i64)> = w
            .iter()
            .map(|x| (x.from.as_str(), x.to.as_str(), x.ev.line))
            .collect();
        assert_eq!(wires, [("api", "db", 7), ("api", "db", 9)]);
        let k8s = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: cart\n  namespace: shop\nspec:\n  template:\n    metadata:\n      labels:\n        app: cart\n    spec:\n      containers:\n        - name: c\n          image: reg/cart:1\n          env:\n            - name: REDIS_ADDR\n              value: \"redis-cart:6379\"\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: redis-cart\nspec:\n  selector:\n    app: redis\n";
        let (d, w) = scan_yaml("k8s/cart.yaml", k8s, "Kubernetes");
        let got: Vec<(&str, &str, &str, Option<&str>)> = d
            .iter()
            .map(|x| {
                (
                    x.what.as_str(),
                    x.name.as_str(),
                    x.group.as_str(),
                    x.ns.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                ("image:reg/cart:1", "cart", "cart", Some("shop")),
                ("kind:Deployment", "cart", "cart", Some("shop")),
                ("kind:Service", "redis-cart", "redis", None)
            ]
        );
        assert_eq!(d[0].ev.line, 14);
        assert_eq!(d[1].span, (1, 17));
        assert_eq!(w.len(), 1);
        assert_eq!(
            (w[0].from.as_str(), w[0].to.as_str(), w[0].ev.line),
            ("cart", "redis-cart", 16)
        );
    }

    #[test]
    fn aspire_app_host_declares_resources_projects_and_references() {
        let src = "var builder = DistributedApplication.CreateBuilder(args);\nvar cache = builder.AddRedis(\"cache\");\nvar pg = builder.AddPostgres(\"pg\")\n    .WithLifetime(ContainerLifetime.Persistent);\nvar ordersDb = pg.AddDatabase(\"ordersdb\");\nvar api = builder.AddProject<Projects.Orders_Api>(\"orders-api\")\n    .WithReference(ordersDb)\n    .WithReference(cache);\nbuilder.AddDotnetProject(\"worker\", \"../Worker\")\n    .WithReference(pg);\n";
        let (d, w, p) = scan_aspire(
            "src/AppHost/",
            &[("src/AppHost/Program.cs".to_string(), src.to_string())],
        );
        let got: Vec<(&str, &str, &str)> = d
            .iter()
            .map(|x| (x.what.as_str(), x.name.as_str(), x.group.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                ("AddRedis", "cache", ""),
                ("AddPostgres", "pg", ""),
                ("AddDatabase", "ordersdb", "pg")
            ]
        );
        let projects: Vec<(&str, &str)> = p.iter().map(|x| (x.0.as_str(), x.1.as_str())).collect();
        assert_eq!(
            projects,
            [
                ("orders-api", "alias:Orders_Api"),
                ("worker", "dir:src/Worker/")
            ]
        );
        let wires: Vec<(&str, &str)> = w.iter().map(|x| (x.from.as_str(), x.to.as_str())).collect();
        assert_eq!(
            wires,
            [
                ("orders-api", "ordersdb"),
                ("orders-api", "cache"),
                ("worker", "pg")
            ]
        );
    }

    #[test]
    fn bicep_terraform_prisma_and_serverless_declare() {
        let b = scan_bicep(
            "infra/main.bicep",
            "resource bus 'Microsoft.ServiceBus/namespaces@2022-10-01' = {\n  name: 'orders-bus'\n}\nresource kv 'Microsoft.KeyVault/vaults@2023-07-01' existing = {\n  name: '${prefix}-kv'\n}\n",
        );
        let got: Vec<(&str, &str, i64)> = b
            .iter()
            .map(|x| (x.what.as_str(), x.name.as_str(), x.ev.line))
            .collect();
        assert_eq!(
            got,
            [
                ("Microsoft.ServiceBus/namespaces", "orders-bus", 1),
                ("Microsoft.KeyVault/vaults", "kv", 4)
            ]
        );
        let t = scan_terraform(
            "tf/main.tf",
            "resource \"aws_sqs_queue\" \"jobs\" {\n  name = \"jobs\"\n}\nmodule \"x\" {}\n",
        );
        assert_eq!(t.len(), 1);
        assert_eq!(
            (t[0].what.as_str(), t[0].name.as_str()),
            ("aws_sqs_queue", "jobs")
        );
        let p = scan_prisma(
            "db/schema.prisma",
            "datasource db {\n  provider = \"postgresql\"\n  url = env(\"DATABASE_URL\")\n}\n",
        );
        assert_eq!(
            (p[0].what.as_str(), p[0].ev.line),
            ("provider:postgresql", 2)
        );
        let s = "service: orders\nprovider:\n  name: aws\nfunctions:\n  consume:\n    handler: h.run\n    events:\n      - sqs: arn:aws:sqs:x\n      - http:\n          path: /x\n";
        let (d, _) = scan_yaml("orders/serverless.yml", s, "Serverless");
        let got: Vec<(&str, &str)> = d
            .iter()
            .map(|x| (x.what.as_str(), x.name.as_str()))
            .collect();
        assert_eq!(
            got,
            [("function", "orders-consume"), ("event:sqs", "consume")]
        );
    }
}
