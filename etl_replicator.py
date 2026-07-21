import os
import yaml
import pymysql
import logging
import threading
import time
import datetime
import json
import pytz
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import QueryEvent
from logging.handlers import RotatingFileHandler

# ------------------ PROMETHEUS ------------------
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ------------------ EXPORTED METRICS ------------------
ETL_EVENTS = Counter(
    "etl_events_total",
    "Total processed events",
    ["type", "table"]
)

ETL_ERRORS = Counter(
    "etl_errors_total",
    "Total errors"
)

ETL_QUERIES = Counter(
    "etl_queries_total",
    "Executed SQL queries",
    ["type"]
)

ETL_LAG = Gauge(
    "etl_lag_seconds",
    "Replication lag in seconds"
)

ETL_BINLOG_POS = Gauge(
    "etl_binlog_position",
    "Current binlog position"
)

ETL_UP = Gauge(
    "etl_up",
    "ETL running status"
)

ETL_PROCESS_TIME = Histogram(
    "etl_event_processing_seconds",
    "Time spent processing events"
)

ETL_HEARTBEAT = Gauge(
    "etl_heartbeat_timestamp_seconds",
    "Last time ETL processed a binlog event"
)


# --------------------------------------------------
# FIX UPDATE LAG METRICS
# --------------------------------------------------

def update_lag_metrics(binlogevent):
    event_ts = datetime.datetime.utcfromtimestamp(binlogevent.timestamp)
    now = datetime.datetime.utcnow()

    lag = (now - event_ts).total_seconds()

    ETL_LAG.set(max(lag, 0))

    # 🔥 nuovo: heartbeat sempre aggiornato
    ETL_HEARTBEAT.set(now.timestamp())



# --------------------------------------------------
# CONFIG & LOGGING
# --------------------------------------------------

def load_config(path="etl_replicator.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def setup_logging(log_file, level=logging.INFO, max_bytes=10*1024*1024, backup_count=10):
    logger = logging.getLogger()
    logger.setLevel(level)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

# --------------------------------------------------
# BINLOG POSITION HELPERS
# --------------------------------------------------

def read_binlog_position(path):
    if not os.path.exists(path):
        return None, None
    with open(path, "r") as f:
        file, pos = f.read().strip().split(":")
        return file, int(pos)

def save_binlog_position(path, file, pos):
    with open(path, "w") as f:
        f.write(f"{file}:{pos}")
    ETL_BINLOG_POS.set(pos)

# --------------------------------------------------
# CONNECTION HELPERS    
# --------------------------------------------------

def connect_to_target(config):
    config["target_db"]["autocommit"] = False
    conn = pymysql.connect(**config["target_db"])

    with conn.cursor() as cursor:
        cursor.execute("SET SESSION wait_timeout = 31536000")
        cursor.execute("SET SESSION interactive_timeout = 31536000") 
        cursor.execute("SET SESSION net_read_timeout = 3600")
        cursor.execute("SET SESSION net_write_timeout = 3600")

    return conn

def ensure_connection(conn, config):
    try:
        conn.ping(reconnect=True)
        return conn
    except pymysql.MySQLError:
        logging.error("Target connection lost.")
        ETL_UP.set(0)
        try:
            conn = connect_to_target(config)
            ETL_UP.set(1)
            return conn
        except pymysql.MySQLError as e:
            logging.error("Re‑connection error: %s", e)
            return None

# --------------------------------------------------
# SCHEMA TYPE CACHE & HELPERS
# --------------------------------------------------

_column_type_cache = {}

def get_column_type(cursor, schema, table, column):
    cache_key = f"{schema}.{table}.{column}"
    
    if cache_key in _column_type_cache:
        return _column_type_cache[cache_key]
    
    try:
        query = """
        SELECT DATA_TYPE 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
          AND TABLE_NAME = %s 
          AND COLUMN_NAME = %s
        """
        cursor.execute(query, (schema, table, column))
        result = cursor.fetchone()
        
        column_type = result[0].upper() if result else 'UNKNOWN'
        _column_type_cache[cache_key] = column_type
        return column_type
    except Exception as e:
        logging.error(f"Error getting column type for {cache_key}: {e}")
        return 'UNKNOWN'

def get_table_column_types(cursor, schema, table):
    cache_key = f"{schema}.{table}"
    
    if cache_key in _column_type_cache:
        return _column_type_cache[cache_key]
    
    try:
        query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
          AND TABLE_NAME = %s
        """
        cursor.execute(query, (schema, table))
        results = cursor.fetchall()
        
        column_types = {col: dtype.upper() for col, dtype in results}
        _column_type_cache[cache_key] = column_types
        return column_types
    except Exception as e:
        logging.error(f"Error getting column types for {cache_key}: {e}")
        return {}

# --------------------------------------------------
# SQL HELPERS
# --------------------------------------------------

_primary_key_cache = {}

def get_primary_key_columns(cursor, schema, table):
    cache_key = f"{schema}.{table}"
    
    if cache_key in _primary_key_cache:
        return _primary_key_cache[cache_key]
    
    try:
        sql = (
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME   = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
            """
        )
        cursor.execute(sql, (schema, table))
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        _primary_key_cache[cache_key] = primary_keys
        return primary_keys
    except Exception as e:
        logging.error(f"Error getting primary keys for {cache_key}: {e}")
        return []

# --------------------------------------------------
# UTILITIES
# --------------------------------------------------

def bytes_keys_to_str(obj):
    if isinstance(obj, dict):
        return {k.decode('utf-8') if isinstance(k, bytes) else k: bytes_keys_to_str(v) 
                for k, v in obj.items()}
    elif isinstance(obj, list):
        return [bytes_keys_to_str(item) for item in obj]
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj

def format_value(value, tz, convert_timestamp=False):
    if value is None:
        return "NULL"
    
    if isinstance(value, bytes):
        return f"X'{value.hex()}'"
    
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
        if isinstance(value, datetime.datetime):
            if convert_timestamp:
                if value.tzinfo is None:
                    value = pytz.utc.localize(value).astimezone(pytz.timezone(tz))
                else:
                    value = value.astimezone(pytz.timezone(tz))
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, datetime.date):
            return f"'{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, datetime.time):
            if value.microsecond > 0:
                return f"'{value.strftime('%H:%M:%S.%f')}'"
            else:
                return f"'{value.strftime('%H:%M:%S')}'"
        elif isinstance(value, datetime.timedelta):
            total_seconds = int(value.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            microseconds = value.microseconds
            if microseconds > 0:
                return f"'{hours:02d}:{minutes:02d}:{seconds:02d}.{microseconds:06d}'"
            else:
                return f"'{hours:02d}:{minutes:02d}:{seconds:02d}'"
    
    if isinstance(value, str):
        import re
        time_pattern = r'^\d{1,2}:\d{2}:\d{2}(\.\d{1,6})?$'
        if re.match(time_pattern, value):
            return f"'{value}'"
        if value.strip().startswith(('{', '[')):
            try:
                json.loads(value)
                return "'" + value.replace("'", "''") + "'"
            except (json.JSONDecodeError, ValueError):
                pass
        return "'" + value.replace("'", "''") + "'"
    
    if isinstance(value, (set, frozenset)):
        set_value = ','.join(str(item) for item in value)
        return f"'{set_value}'"
    
    if isinstance(value, (dict, list)):
        value = bytes_keys_to_str(value)
        json_str = json.dumps(value, ensure_ascii=False).replace("'", "''")
        return f"'{json_str}'"
    
    if isinstance(value, bool):
        return str(value).upper()
    
    return str(value)

def build_where_clause(pk_columns, pk_values):
    if not pk_columns:
        return "", ()

    parts, vals = [], []
    for col in pk_columns:
        val = pk_values[col]
        if val is None:
            parts.append(f"`{col}` IS NULL")
        else:
            parts.append(f"`{col}` = %s")
            vals.append(val)
    return " AND ".join(parts), tuple(vals)

def build_full_row_where(row_data):
    parts, vals = [], []
    for col, val in row_data.items():
        if val is None:
            parts.append(f"`{col}` IS NULL")
        else:
            parts.append(f"`{col}` = %s")
            vals.append(val)
    return " AND ".join(parts) if parts else "1", tuple(vals)

def should_replicate(cursor, config, table_fq, row_data):
    ignored_tables = config.get("ignored_tables", [])
    if ignored_tables and table_fq in ignored_tables:
        return False

    filtered_tables = config.get("filtered_tables", [])
    if filtered_tables and table_fq in filtered_tables:
        filter_cfg = config["filtered_tables"][table_fq]
        if row_data.get(filter_cfg["field"]) not in filter_cfg["allowed_values"]:
            return False
        return True

    conditional_replication = config.get("conditional_replication", [])
    if conditional_replication and table_fq in conditional_replication:
        cond = config["conditional_replication"][table_fq]
        join_table = cond["join"]["table"]
        left_field = cond["join"]["left_field"]
        right_field = cond["join"]["right_field"]
        where_field = cond["where"]["field"]
        allowed_vals = cond["where"]["allowed_values"]

        join_value = row_data.get(left_field)
        if join_value is None:
            return False

        sql = (
            f"""
            SELECT 1 FROM `{join_table}`
             WHERE `{right_field}` = %s
               AND `{where_field}` IN ({','.join(['%s'] * len(allowed_vals))})
             LIMIT 1
            """
        )
        params = [join_value] + allowed_vals
        cursor.execute(sql, params)
        return cursor.fetchone() is not None

    return True

# --------------------------------------------------
# REPLICATE EVENT
# --------------------------------------------------

def replicate_event(cursor, config, table, schema, event_type, row_data, pk_columns, tz):
    sql = None
    try:
        table_fq = f"{schema}.{table}"

        data = row_data if event_type != "update" else row_data["after_values"]
        if not should_replicate(cursor, config, table_fq, data):
            return

        column_types = get_table_column_types(cursor, schema, table)

        # ---------------------- DELETE ------------------------------------
        if event_type == "delete":
            if not pk_columns:
                discovered = get_primary_key_columns(cursor, schema, table)
                pk_columns.extend(discovered)
                if discovered:
                    config.setdefault("primary_keys", {}).setdefault(table_fq, discovered)

            if pk_columns:
                where_clause, params = build_where_clause(pk_columns, row_data)
            else:
                where_clause, params = build_full_row_where(row_data)

            sql = f"DELETE FROM `{schema}`.`{table}` WHERE {where_clause} LIMIT 1"
            cursor.execute(sql, params)
            ETL_QUERIES.labels("delete").inc()
            ETL_EVENTS.labels("delete", table).inc()

        # ---------------------- INSERT ------------------------------------
        if event_type == "insert":
            columns = ", ".join(f"`{k}`" for k in data)
            values = ", ".join(
                format_value(
                    v, 
                    tz, 
                    convert_timestamp=(column_types.get(k) == 'TIMESTAMP')
                ) 
                for k, v in data.items()
            )
            sql = f"INSERT IGNORE INTO `{schema}`.`{table}` ({columns}) VALUES ({values})"
            cursor.execute(sql)
            ETL_QUERIES.labels("insert").inc()
            ETL_EVENTS.labels("insert", table).inc()

        # ---------------------- UPDATE ------------------------------------
        elif event_type == "update":
            if not pk_columns:
                discovered = get_primary_key_columns(cursor, schema, table)
                pk_columns.extend(discovered)
                if discovered:
                    config.setdefault("primary_keys", {}).setdefault(table_fq, discovered)

            if not pk_columns:
                replicate_event(cursor, config, table, schema, "delete", row_data["before_values"], [], tz)
                replicate_event(cursor, config, table, schema, "insert", row_data["after_values"], [], tz)
                return

            set_clause = ", ".join(
                f"`{k}` = {format_value(v, tz, convert_timestamp=(column_types.get(k) == 'TIMESTAMP'))}" 
                for k, v in row_data["after_values"].items()
            )
            where_clause, params = build_where_clause(pk_columns, row_data["before_values"])
            sql = f"UPDATE `{schema}`.`{table}` SET {set_clause} WHERE {where_clause}"
            cursor.execute(sql, params)
            ETL_QUERIES.labels("update").inc()
            ETL_EVENTS.labels("update", table).inc()

        try:
            logging.info(f"{event_type.upper()} replicated on {schema}.{table}: {sql}")
        except Exception:
            pass

    except Exception as e:
        logging.error("Error replicating event on %s.%s: %s %s", schema, table, e, sql)
        ETL_ERRORS.inc()



# --------------------------------------------------
# HEARTBEAT MONITOR
# --------------------------------------------------


def heartbeat_monitor():
    while True:
        try:
            now = time.time()
            last = ETL_HEARTBEAT._value.get()

            if last == 0:
                ETL_LAG.set(0)
            else:
                age = now - last

                # se non arrivano eventi da 60s → lag = 0
                if age > 60:
                    ETL_LAG.set(0)

        except Exception:
            pass

        time.sleep(10)



# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

def main():
    config = load_config()

    log_cfg = config.get("logging", {})
    log_file = log_cfg.get("file", "etl_replicator.log")
    log_level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    max_bytes = log_cfg.get("max_size_mb", 10) * 1024 * 1024
    backup_count = log_cfg.get("backup_count", 5)
    queries_for_commit = config.get("queries_for_commit", 1)
    tz = config["time_zone"]

    setup_logging(log_file, level=log_level, max_bytes=max_bytes, backup_count=backup_count)
    logging.info("Starting binlog replicator…")

    start_http_server(9104)  # Prometheus exporter
    logging.info("Prometheus exporter started on port 9104")
    ETL_UP.set(1)

    binlog_file, binlog_pos = read_binlog_position(config["binlog_pos_file"])

    target_conn = connect_to_target(config)
    target_cursor = target_conn.cursor()

    stream = BinLogStreamReader(
        connection_settings=config["source_db"],
        server_id=101,
        blocking=True,
        resume_stream=True,
        log_file=binlog_file,
        log_pos=binlog_pos,
        only_schemas=[config["replicated_schema"]],
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
    )

    commit_counter = 0

    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    for binlogevent in stream:
        while True:
            target_conn = ensure_connection(target_conn, config)
            if target_conn is not None:
                target_cursor = target_conn.cursor()
                break
            logging.info("Retry target connection every 5 seconds")
            time.sleep(5)

        with ETL_PROCESS_TIME.time():
            try:
                # ------------------- DDL -----------------------------------
                if isinstance(binlogevent, QueryEvent):
                    if (
                        binlogevent.schema.decode("utf-8") == config["replicated_schema"]
                        and binlogevent.query.strip().upper().startswith(
                            ("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME")
                        )
                    ):
                        target_cursor.execute(binlogevent.query)
                        logging.info("DDL executed: %s", binlogevent.query)
                        _column_type_cache.clear()
                        _primary_key_cache.clear()
                        commit_counter = 0

                # ------------------- DML -----------------------------------
                elif isinstance(binlogevent, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    table = binlogevent.table
                    schema = config["replicated_schema"]
                    table_fq = f"{schema}.{table}"
                    pk_cols = config.get("primary_keys", {}).get(table_fq, []).copy()

                    update_lag_metrics(binlogevent)

                    for row in binlogevent.rows:
                        if isinstance(binlogevent, WriteRowsEvent):
                            replicate_event(target_cursor, config, table, schema, "insert", row["values"], pk_cols, tz)
                        elif isinstance(binlogevent, DeleteRowsEvent):
                            replicate_event(target_cursor, config, table, schema, "delete", row["values"], pk_cols, tz)
                        elif isinstance(binlogevent, UpdateRowsEvent):
                            replicate_event(target_cursor, config, table, schema, "update", row, pk_cols, tz)

                    commit_counter += 1

                if commit_counter == queries_for_commit:
                    target_conn.commit()
                    commit_counter = 0

                save_binlog_position(config["binlog_pos_file"], stream.log_file, stream.log_pos)

            except Exception as e:
                logging.error("Error processing binlogevent: %s", e)
                ETL_ERRORS.inc()
                target_conn = None
                ETL_UP.set(0)
                time.sleep(0.1)

if __name__ == "__main__":
    main()
