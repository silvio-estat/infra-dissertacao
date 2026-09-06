-- Inicialização do PostgreSQL: cria os bancos de dados do projeto.
--
-- O PostgreSQL é INFRAESTRUTURA, não objeto de estudo: serve de backend para o
-- Hive Metastore, para o Airflow e para o OpenMetadata. Não há baseline relacional.

-- Hive Metastore (catálogo Iceberg)
CREATE DATABASE metastore_db;

-- Apache Airflow — orquestração Lakehouse
CREATE DATABASE airflow_db;

-- OpenMetadata — catálogo de governança
CREATE DATABASE openmetadata_db;

-- OpenMetadata Ingestion — Airflow interno do OpenMetadata
CREATE DATABASE airflow_om_db;

-- Permissões ao usuário padrão
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE airflow_om_db TO dlh_admin;
