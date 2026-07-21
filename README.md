# ETL Replicator

A lightweight, near-real-time MySQL-to-MySQL replication and ETL utility written in Python.

`etl_replicator.py` reads row-based events from a MySQL binary log, applies configurable filtering rules, and replicates selected DML and DDL operations to a target MySQL-compatible database.

The project is designed for use cases where native replication is not sufficient because only a subset of the source data must be copied, or where replication decisions depend on row values or related records.

## Features

- Near-real-time replication based on the MySQL binary log
- Supports row events:
  - `INSERT`
  - `UPDATE`
  - `DELETE`
- Supports selected DDL statements:
  - `CREATE`
  - `ALTER`
  - `DROP`
  - `TRUNCATE`
  - `RENAME`
- Replicates a single configured schema
- Supports ignored tables
- Supports row filtering based on allowed column values
- Supports conditional replication through a lookup on a related table
- Automatically discovers primary keys when they are not explicitly configured
- Falls back to full-row matching for deletes when no primary key is available
- Uses `DELETE` followed by `INSERT` for updates on tables without a primary key
- Stores the current binlog file and position to resume after a restart
- Configurable commit batching
- Automatic reconnection to the target database
- Timestamp conversion to a configurable time zone
- Rotating application logs
- Prometheus metrics endpoint
- Optional systemd service for automatic startup and restart after crashes

## Project Files

```text
.
├── etl_replicator.py
├── etl_replicator.yaml
├── LICENSE
├── myetl.service
└── README.md
```

### `etl_replicator.py`

Main ETL and binlog replication program.

### `etl_replicator.yaml`

Sample configuration file.

Before running the program, replace all sample values with settings appropriate for your environment. Do not commit real passwords, credentials, private hostnames, or other sensitive information to a public repository.

### `myetl.service`

Sample systemd unit file used to start the ETL replicator as a Linux service and restart it automatically after a failure.

The file must be updated with the actual installation directory, Python interpreter, operating-system user, and any other environment-specific values.

## Architecture

The application opens two main connections:

1. A binlog stream connection to the source MySQL server.
2. A standard SQL connection to the target MySQL server.

The data flow is:

```text
MySQL source
    |
    | Row-based binary log
    v
etl_replicator.py
    |
    | Filtering and transformation rules
    v
MySQL target
```

After processing events, the application stores the current binlog coordinates in a local checkpoint file:

```text
mysql-bin.000123:456789
```

At the next startup, the program resumes reading from that file and position.

## Requirements

- Linux is recommended when using the included systemd service
- Python 3.9 or later
- A MySQL-compatible source server with binary logging enabled
- A MySQL-compatible target server
- Row-based binary logging on the source
- A dedicated MySQL replication user on the source
- A target user with the permissions required to execute replicated DML and DDL
- Network connectivity from the ETL host to both database servers

Compatible database products may include MySQL, Percona Server for MySQL, and other sufficiently compatible MySQL implementations. Test compatibility before using the program in production.

## Python Dependencies

The application requires the following Python packages:

```text
PyYAML
PyMySQL
mysql-replication
pytz
prometheus-client
```

Install them with:

```bash
python3 -m pip install \
  PyYAML \
  PyMySQL \
  mysql-replication \
  pytz \
  prometheus-client
```

Using a virtual environment is recommended:

```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install \
  PyYAML \
  PyMySQL \
  mysql-replication \
  pytz \
  prometheus-client
```

You may also create a `requirements.txt` file:

```text
PyYAML
PyMySQL
mysql-replication
pytz
prometheus-client
```

Then install the dependencies with:

```bash
python3 -m pip install -r requirements.txt
```

## Source MySQL Configuration

The source server must have binary logging enabled and must use row-based replication.

Example server configuration:

```ini
[mysqld]
server_id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

`binlog_row_image=FULL` is strongly recommended because the program may need the complete before and after images of rows, especially for updates or for tables without primary keys.

Check the current settings with:

```sql
SHOW VARIABLES WHERE Variable_name IN (
    'server_id',
    'log_bin',
    'binlog_format',
    'binlog_row_image'
);
```

The source user requires privileges sufficient to read the binary log and inspect schema metadata.

Example:

```sql
CREATE USER 'etl_reader'@'ETL_HOST'
IDENTIFIED BY 'REPLACE_WITH_A_STRONG_PASSWORD';

GRANT REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'etl_reader'@'ETL_HOST';

GRANT SELECT
ON source_database.* TO 'etl_reader'@'ETL_HOST';
```

Depending on the MySQL version and the configured filtering rules, additional metadata access may be required.

## Target MySQL Permissions

The target account must be able to execute the statements replicated by the program.

For DML-only replication, typical privileges include:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE
ON target_database.* TO 'etl_writer'@'ETL_HOST';
```

If DDL replication is enabled through the current program behavior, the target user may also require privileges such as:

```sql
GRANT CREATE, ALTER, DROP, INDEX, TRIGGER
ON target_database.* TO 'etl_writer'@'ETL_HOST';
```

Grant only the permissions that are actually required in your environment.

## Configuration

The program loads its configuration from:

```text
etl_replicator.yaml
```

The file must exist in the process working directory unless the code is changed to use another path.

A representative configuration structure is shown below. Adapt it to the sample file included with the repository and replace every placeholder with a real value.

```yaml
source_db:
  host: source-mysql.example.com
  port: 3306
  user: etl_reader
  passwd: REPLACE_ME

target_db:
  host: target-mysql.example.com
  port: 3306
  user: etl_writer
  password: REPLACE_ME
  database: application_database
  charset: utf8mb4

replicated_schema: application_database

time_zone: Europe/Rome

binlog_pos_file: /var/lib/myetl/binlog_position.txt

queries_for_commit: 100

logging:
  file: /var/log/myetl/etl_replicator.log
  level: INFO
  max_size_mb: 10
  backup_count: 5

ignored_tables: application_database.audit_log, application_database.temporary_data

filtered_tables:
  application_database.orders:
    field: status
    allowed_values: ["COMPLETED","SHIPPED"]

conditional_replication:
  application_database.order_items:
    join:
      table: orders
      left_field: order_id
      right_field: id
    where:
      field: status
      allowed_values: ["COMPLETED","SHIPPED"]
```

The exact connection keyword accepted by PyMySQL may vary according to how the sample configuration is written. Keep the sample file consistent with the arguments accepted by `pymysql.connect()`.

### Source connection

The `source_db` section is passed to `BinLogStreamReader` and contains the source MySQL connection settings.

Typical fields include:

```yaml
source_db:
  host: source-mysql.example.com
  port: 3306
  user: etl_reader
  passwd: REPLACE_ME
```

### Target connection

The `target_db` section is passed to `pymysql.connect()`.

Example:

```yaml
target_db:
  host: target-mysql.example.com
  port: 3306
  user: etl_writer
  password: REPLACE_ME
  database: application_database
  charset: utf8mb4
```

The program explicitly disables autocommit and manages commits in batches.

### Replicated schema

```yaml
replicated_schema: application_database
```

Only binlog events belonging to this schema are consumed.

The current implementation writes to a schema with the same name on the target.

### Time zone

```yaml
time_zone: Europe/Rome
```

This setting is used when converting values from columns whose target data type is `TIMESTAMP`.

Use a valid IANA time-zone name, for example:

```text
UTC
Europe/Rome
America/New_York
```

### Binlog checkpoint file

```yaml
binlog_pos_file: /var/lib/myetl/binlog_position.txt
```

This file stores the latest binlog file and position processed by the application.

The service user must have read and write access to its parent directory.

Create the directory, for example:

```bash
sudo install -d \
  -o myetl \
  -g myetl \
  -m 0750 \
  /var/lib/myetl
```

### Commit batching

```yaml
queries_for_commit: 100
```

This controls how many binlog row events are processed before the target transaction is committed.

A lower value:

- reduces the amount of uncommitted work;
- reduces recovery exposure;
- increases commit overhead.

A higher value:

- improves throughput in some workloads;
- creates larger target transactions;
- increases the amount of work that may need to be replayed after a failure.

Test an appropriate value for your workload.

### Logging

```yaml
logging:
  file: /var/log/myetl/etl_replicator.log
  level: INFO
  max_size_mb: 10
  backup_count: 5
```

Supported standard Python log levels include:

```text
DEBUG
INFO
WARNING
ERROR
CRITICAL
```

The program uses rotating log files.

Create the log directory and grant access to the service user:

```bash
sudo install -d \
  -o myetl \
  -g myetl \
  -m 0750 \
  /var/log/myetl
```


The program queries `INFORMATION_SCHEMA` to discover teh Primry Key automatically for each table.

For predictable production behavior, explicitly configuring primary keys for replicated tables is recommended.

### Ignored tables

```yaml
ignored_tables: application_database.audit_log, application_database.temporary_data
```

Events from these tables are discarded.

Table names must use the fully qualified form:

```text
schema_name.table_name
```

### Row filtering

The `filtered_tables` section replicates rows only when a configured field contains one of the allowed values.

```yaml
filtered_tables:
  application_database.orders:
    field: status
    allowed_values: ["COMPLETED","SHIPPED"]
```

Rows whose `status` is not in the allowed list are ignored.

For updates, the filter is evaluated against the new row image.

### Conditional replication

Conditional replication performs a lookup on a related table before deciding whether to replicate a row.

```yaml
conditional_replication:
  application_database.order_items:
    join:
      table: orders
      left_field: order_id
      right_field: id
    where:
      field: status
      allowed_values: ["COMPLETED","SHIPPED"]
```

Conceptually, the program executes a lookup similar to:

```sql
SELECT 1
FROM orders
WHERE id = ?
  AND status IN ('COMPLETED', 'SHIPPED')
LIMIT 1;
```

Important: in the current implementation this lookup is executed through the target database connection. The related table must therefore exist and contain the expected data on the target.

## Running Manually

From the project directory:

```bash
source .venv/bin/activate
python etl_replicator.py
```

The program expects `etl_replicator.yaml` in the current working directory.

To run it without a virtual environment:

```bash
python3 etl_replicator.py
```

Check the application log:

```bash
tail -f /var/log/myetl/etl_replicator.log
```

## Running with systemd

The repository includes:

```text
myetl.service
```

This is a sample systemd service file. Before installing it, configure at least:

- `User`
- `Group`
- `WorkingDirectory`
- `ExecStart`
- any environment variables or environment files
- restart behavior, if needed
- file-system permissions

A representative unit file may look like:

```ini
[Unit]
Description=ETL Python Script
After=network.target

[Service]
ExecStart=/usr/bin/python3 /home/myuser/etl_replicator.py
WorkingDirectory=/home/myuser
Restart=always
RestartSec=5
User=myuser
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

The actual `myetl.service` file included in the repository should be reviewed and adapted to the installation environment.

### Install the service

Copy the project to its final directory:

```bash
sudo mkdir -p /opt/myetl
sudo cp etl_replicator.py etl_replicator.yaml /opt/myetl/
sudo chown -R myetl:myetl /opt/myetl
```

Install the service:

```bash
sudo cp myetl.service /etc/systemd/system/myetl.service
sudo systemctl daemon-reload
sudo systemctl enable --now myetl.service
```

Check its status:

```bash
sudo systemctl status myetl.service
```

Follow the systemd journal:

```bash
sudo journalctl -u myetl.service -f
```

Restart the service:

```bash
sudo systemctl restart myetl.service
```

Stop the service:

```bash
sudo systemctl stop myetl.service
```

After changing the unit file:

```bash
sudo systemctl daemon-reload
sudo systemctl restart myetl.service
```

## Prometheus Metrics

The application starts an HTTP metrics server on port:

```text
9104
```

Metrics are available at:

```text
http://HOSTNAME:9104/metrics
```

In the repositiry you can find the file ETL REPLICATOR.json. It is a preconfigured dashbaord for the metrics exposed by the program. You can import on your PMM or Grafana.


The exporter currently exposes the following custom metrics.

### `etl_events_total`

Total number of processed row events, labeled by event type and table.

Example:

```text
etl_events_total{type="insert",table="orders"} 1250
```

### `etl_errors_total`

Total number of errors detected by the application.

### `etl_queries_total`

Total number of SQL statements executed on the target, labeled by query type.

### `etl_lag_seconds`

Estimated difference between the current time and the timestamp of the last processed binlog event.

When no events arrive for more than 60 seconds, the current implementation resets this metric to zero. Therefore, a value of zero may mean either that replication is current or that the source workload is idle.

### `etl_binlog_position`

Current numeric position within the active binlog file.

The metric does not include the binlog filename.

### `etl_up`

Application status as maintained internally by the process:

- `1`: running or connected
- `0`: an error or connection problem was detected

This metric alone should not be treated as a complete end-to-end health check.

### `etl_event_processing_seconds`

Histogram measuring binlog-event processing duration.

### `etl_heartbeat_timestamp_seconds`

Unix timestamp of the latest processed binlog row event.

Example Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: myetl
    static_configs:
      - targets:
          - etl-host.example.com:9104
```

Restrict network access to the metrics endpoint when it must not be publicly reachable.

## Event Processing Behavior

### Inserts

Insert events are applied using:

```sql
INSERT IGNORE
```

Duplicate-key and certain other ignorable insert errors are therefore suppressed by MySQL.

This behavior can prevent the replicator from stopping on duplicates, but it can also hide data inconsistencies. Monitor warnings and validate target data regularly.

### Updates

For tables with a primary key, the program generates an `UPDATE` using the primary-key values from the before image.

For tables without a primary key, the program falls back to:

1. deleting a row using the full before image;
2. inserting the full after image.

Tables without primary keys are therefore less efficient and may produce ambiguous matches when duplicate rows exist.

Adding a stable primary key to every replicated table is strongly recommended.

### Deletes

For tables with a primary key, deletes use the primary-key values from the row image.

If no primary key exists, the program builds a predicate from every available column and applies:

```sql
DELETE ... LIMIT 1
```

When duplicate rows exist, only one matching row is removed.

### DDL

The current program recognizes DDL query events beginning with:

```text
CREATE
ALTER
DROP
TRUNCATE
RENAME
```

The statement is executed directly on the target.

DDL replication should be tested carefully because:

- source and target objects must remain compatible;
- the target user needs additional privileges;
- unsupported or environment-specific DDL may fail;
- schema changes can invalidate cached column and primary-key metadata.

## Checkpoint and Recovery Behavior

The checkpoint file contains:

```text
binlog_filename:binlog_position
```

Example:

```text
mysql-bin.000123:456789
```

On restart, the program passes these coordinates to the binlog reader and resumes from that location.

Operational considerations:

- keep the checkpoint file on persistent storage;
- ensure the service user can update it;
- back it up together with the application configuration;
- retain source binlogs long enough to cover the maximum expected outage;
- do not edit the checkpoint manually unless you understand the consequences;
- if the referenced binlog has been purged, the replicator cannot resume from that position.

The current checkpoint mechanism uses file and position coordinates, not GTID.

## Security Recommendations

- Never publish real credentials in `etl_replicator.yaml`.
- Use placeholders in the repository sample.
- Store production configuration outside the Git repository when possible.
- Restrict configuration-file permissions:

```bash
chmod 600 etl_replicator.yaml
```

- Run the process using a dedicated unprivileged operating-system account.
- Grant the source and target MySQL users only the permissions they need.
- Restrict access to port `9104`.
- Use TLS for database connections when traffic crosses untrusted networks.
- Do not run the service as `root`.
- Review logs before publishing them because they may contain SQL statements and row values.
- Consider using systemd credentials, a secrets manager, or an environment-specific configuration deployment mechanism for production secrets.

Recommended `.gitignore` entries:

```gitignore
# Real configuration and local overrides
etl_replicator.local.yaml
etl_replicator.production.yaml
.env
.env.*

# Checkpoints
binlog_position.txt
*.pos

# Logs
*.log
logs/

# Python
__pycache__/
*.py[cod]
.venv/
venv/

# Editor and OS files
.vscode/
.idea/
.DS_Store
```

If `etl_replicator.yaml` is committed as a public sample, ensure it contains only placeholders and non-sensitive example values.

## Operational Recommendations

Before production use:

1. Create the target schema and verify schema compatibility.
2. Confirm that the source uses row-based binlogging.
3. Confirm that full row images are available.
4. Test each replicated table.
5. Explicitly configure primary keys.
6. Test insert, update, and delete behavior.
7. Test DDL behavior separately.
8. Test a controlled service restart.
9. Verify that the checkpoint resumes from the expected position.
10. Verify source binlog retention.
11. Monitor lag, errors, heartbeat, and process restarts.
12. Validate row counts and checksums between source and target.

Useful source checks:

```sql
SHOW MASTER STATUS;
SHOW BINARY LOGS;
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
```

Useful systemd checks:

```bash
systemctl is-active myetl.service
systemctl show myetl.service -p NRestarts
journalctl -u myetl.service --since "1 hour ago"
```

## Known Limitations

The current implementation has several important limitations:

- It replicates one configured schema.
- The source and target schema names are expected to match.
- It is not a replacement for native MySQL replication.
- It does not use GTID-based checkpoints.
- The checkpoint is stored in a local text file.
- Checkpoint updates are not transactional with target commits.
- A crash at an unfortunate point may cause events to be replayed or skipped, depending on commit and checkpoint timing.
- Errors inside individual replication operations are logged, but the current implementation may continue processing subsequent events.
- `INSERT IGNORE` may hide duplicate-key errors and other warnings.
- Conditional lookups are executed on the target connection.
- Tables without primary keys are handled using less reliable fallback logic.
- SQL values for inserts and update assignments are rendered by application code rather than being fully parameterized.
- The Prometheus port is currently hard-coded to `9104`.
- The binlog reader `server_id` is currently hard-coded to `101`.
- There is no built-in initial snapshot or bulk-load phase.
- There is no automatic schema comparison.
- There is no automatic verification that source and target remain consistent.
- Multi-threaded event application is not implemented.
- Exact transaction boundaries from the source are not preserved as target transaction boundaries.
- The program does not currently provide an exactly-once delivery guarantee.

Use the project only after testing its behavior against the consistency requirements of your workload.

## Initial Data Load

This tool consumes changes from the binary log; it does not perform an initial full copy of existing data.

A typical deployment procedure is:

1. Record a consistent source binlog position.
2. Export the initial source data.
3. Load the data into the target.
4. Configure the recorded binlog position as the starting checkpoint.
5. Start the replicator.
6. Validate source and target consistency.

The exact procedure depends on database size, locking constraints, and consistency requirements.

## Troubleshooting

### The program cannot read the binary log

Check:

- source credentials;
- replication privileges;
- network access;
- binary logging;
- `binlog_format=ROW`;
- whether the checkpoint refers to a binlog that still exists.

### The service starts and immediately exits

Check:

```bash
sudo systemctl status myetl.service
sudo journalctl -u myetl.service -n 100 --no-pager
```

Also verify:

- `WorkingDirectory`;
- `ExecStart`;
- Python virtual-environment path;
- YAML syntax;
- permissions on the configuration, log, and checkpoint paths.

### Target connection errors

Verify:

- hostname and port;
- credentials;
- TLS settings, if configured;
- target grants;
- network/firewall rules;
- maximum connection limits.

### Replication lag increases

Check:

- target query latency;
- target locks;
- slow DDL;
- expensive conditional lookups;
- missing indexes;
- transaction size;
- application errors;
- source event rate.

The conditional lookup table should have indexes covering:

```text
join.right_field
where.field
```

### Rows are missing from the target

Check:

- ignored-table rules;
- filtered-table rules;
- conditional-replication rules;
- `INSERT IGNORE` warnings;
- application errors;
- binlog retention;
- checkpoint correctness;
- whether full row images are available.

### The metrics endpoint is unavailable

Check whether the process is listening:

```bash
ss -lntp | grep 9104
```

Test locally:

```bash
curl http://127.0.0.1:9104/metrics
```

## Development

Run a syntax check:

```bash
python3 -m py_compile etl_replicator.py
```

For local development, use a dedicated test source and target. Do not test against production databases unless the behavior and permissions have been carefully reviewed.

Potential future improvements include:

- transactional checkpointing;
- atomic checkpoint-file updates;
- GTID support;
- fully parameterized SQL;
- configurable Prometheus address and port;
- configurable binlog client server ID;
- source-side conditional lookups;
- dead-letter handling;
- retry policies by error class;
- graceful shutdown and final commit;
- source transaction boundary preservation;
- schema mapping;
- table mapping;
- integration tests;
- package metadata and a command-line interface.

## Disclaimer

This project applies database changes directly to a target system. Incorrect configuration, unsupported schema differences, or unexpected binlog events may cause data loss, duplicate data, or target inconsistencies.

Test thoroughly, maintain backups, monitor the process, and validate replicated data before relying on it in production.

## License

Add the license selected for the project in a `LICENSE` file and update this section accordingly.

For example:

```text
Licensed under the Apache License, Version 2.0.
```

## Contributing

Contributions, bug reports, and improvement proposals are welcome.

When opening an issue, include:

- Python version;
- source and target database product and version;
- relevant configuration with credentials removed;
- relevant log messages;
- table definitions;
- a minimal reproducible example, when possible.


