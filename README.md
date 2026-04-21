export $(grep -v '^#' .env | xargs) export .env into terminal

dbt run --target prod run prod in profiles.yml


mkdir -p metabase-plugins
wget https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.5.1.0/duckdb.metabase-driver.jar -P metabase-plugins/