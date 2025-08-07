# A Deephaven Iceberg deployment

This repository contains a [Deephaven](https://deephaven.io/) deployment for Apache Iceberg.

It deploys an Apache Iceberg [REST catalog](https://www.tabular.io/apache-iceberg-cookbook/getting-started-catalog-background/) with [MinIO](https://www.min.io/) as the [S3](https://aws.amazon.com/s3/)-compatible object store. Deephaven can be used to query the catalog.

This Docker deployment is based on the [Apache Spark quickstart for Iceberg](https://iceberg.apache.org/spark-quickstart/). The deployment is extended by adding Deephaven to the same Docker network as the query engine. In addition, it automates the creation of a sample Iceberg catalog and table on startup.

## Prerequisites

This deployment uses [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/). Additionally, you will need [Git](https://git-scm.com/) to clone the repository.

## Docker

The application launches five Docker containers:

- Deephaven
- MinIO
- The [MinIO client](https://github.com/minio/mc)
- An Apache Iceberg REST catalog
- An Apache Spark server with Iceberg support

The fifth container shuts down once the Iceberg catalog and table are created, leaving only four running.

### About the deployment

This deployment is intended for local development and testing. It is not intended for production use for a number of reasons, the main two being:

- Deephaven uses anonymous authentication (no authentication), which is not secure.
- AWS credentials are insecure and hardcoded into the Docker configurations.

The deployment is defined in [`docker-compose.yml`](./docker-compose.yml). All Docker containers use default images except for the `spark-iceberg` container, which uses a custom image built off the `tabulario/spark-iceberg` image. The custom image adds a Jupyter notebook that gets run on startup to build the catalog with a table. Once the Jupyter notebook completes, the container shuts down.

## Use

This deployment is used by Deephaven's Iceberg user guide. You can follow all of the steps in the guide using this deployment. See the documentation below:

- [Python](https://deephaven.io/core/docs/how-to-guides/data-import-export/iceberg/)
- [Groovy/Java](https://deephaven.io/core/groovy/docs/how-to-guides/data-import-export/iceberg/)

## License

See [License](./LICENSE).

## Questions/comments/concerns?

Reach out to us on [Slack](https://deephaven.io/slack/)!
