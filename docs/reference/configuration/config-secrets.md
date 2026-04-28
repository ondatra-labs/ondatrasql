---
description: OndatraSQL uses DuckDB's secrets manager for external credentials. Configure access to cloud storage, databases, and APIs for your data pipeline.
draft: false
title: secrets.sql
weight: 2
---
**Secrets: credentials for external data sources**

**Phase:** Pre-catalog | **Order:** 2 | **Required:** No

Credentials for external systems using DuckDB's [secrets manager](https://duckdb.org/docs/stable/configuration/secrets_manager).

## Cloud Storage

```sql
CREATE SECRET aws_chain (
    TYPE s3,
    PROVIDER credential_chain
);
```

```sql
CREATE SECRET s3_explicit (
    TYPE s3,
    KEY_ID '${AWS_ACCESS_KEY_ID}',
    SECRET '${AWS_SECRET_ACCESS_KEY}',
    REGION 'eu-north-1'
);
```

```sql
CREATE SECRET s3_scoped (
    TYPE s3,
    KEY_ID '${AWS_ACCESS_KEY_ID}',
    SECRET '${AWS_SECRET_ACCESS_KEY}',
    SCOPE 's3://prod-data/'
);
```

## Databases

```sql
CREATE SECRET pg_secret (
    TYPE postgres,
    HOST '${PG_HOST}',
    PORT 5432,
    DATABASE 'warehouse',
    USER 'readonly',
    PASSWORD '${PG_PASSWORD}'
);
```

## Supported Providers

S3, GCS, R2, Azure, PostgreSQL, MySQL.
