import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "Zhivitko.A"
DAG_ID = "raw_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# PostgreSQL
PG_HOST = 'pet_project_earthquake--postgres_dwh-1'
PG_PORT = 5432
PG_DATABASE = 'postgres'
PG_USER = 'postgres'
PG_PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def create_postgres_table_if_not_exists():
    """Создает таблицу в PostgreSQL если она не существует"""
    import psycopg2
    
    try:
        # Подключаемся напрямую к PostgreSQL
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        
        # Создаем схему если не существует
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
        
        # Создаем таблицу если не существует
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
                time TIMESTAMP,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                depth DOUBLE PRECISION,
                mag DOUBLE PRECISION,
                mag_type VARCHAR(10),
                nst INTEGER,
                gap DOUBLE PRECISION,
                dmin DOUBLE PRECISION,
                rms DOUBLE PRECISION,
                net VARCHAR(10),
                id VARCHAR(50),
                updated TIMESTAMP,
                place TEXT,
                type VARCHAR(50),
                horizontal_error DOUBLE PRECISION,
                depth_error DOUBLE PRECISION,
                mag_error DOUBLE PRECISION,
                mag_nst INTEGER,
                status VARCHAR(50),
                location_source VARCHAR(50),
                mag_source VARCHAR(50)
            );
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"✅ Table {SCHEMA}.{TARGET_TABLE} created or already exists")
        
    except Exception as e:
        logging.error(f"❌ Failed to create table: {e}")
        raise


def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    
    con = duckdb.connect()
    
    # Настройка S3 и DuckDB
    con.sql(f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """)
    
    # ПРОВЕРКА 1: Есть ли файл и сколько в нем строк?
    try:
        s3_count = con.sql(f"""
            SELECT COUNT(*) FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
        """).fetchone()[0]
        logging.info(f"📊 Rows in S3 file: {s3_count}")
        
        if s3_count == 0:
            logging.warning("⚠️ S3 file is empty!")
            return
            
        # Покажем структуру данных
        sample = con.sql(f"""
            SELECT * FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet' 
            LIMIT 3
        """).fetchdf()
        logging.info(f"📋 Sample data:\n{sample}")
        logging.info(f"📋 Columns in parquet: {list(sample.columns)}")
        
    except Exception as e:
        logging.error(f"❌ Error reading from S3: {e}")
        raise
    
    # Настройка подключения к PostgreSQL через DuckDB
    con.sql(f"""
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST '{PG_HOST}',
            PORT {PG_PORT},
            DATABASE '{PG_DATABASE}',
            USER '{PG_USER}',
            PASSWORD '{PG_PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
    """)
    
    # Удаление старых записей
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
    
    # Удаляем старые данные за этот период
    cursor.execute(f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE} 
        WHERE DATE(time) >= %s AND DATE(time) < %s
    """, (start_date, end_date))
    deleted = cursor.rowcount
    logging.info(f"🧹 Deleted {deleted} existing records for {start_date}")
    conn.commit()
    
    # ПРОВЕРКА 2: Сколько строк до INSERT?
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TARGET_TABLE}")
    before_count = cursor.fetchone()[0]
    logging.info(f"📊 Rows in PostgreSQL BEFORE insert: {before_count}")
    
    # Выполняем INSERT через DuckDB
    try:
        result = con.execute(f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time, latitude, longitude, depth, mag, mag_type,
                nst, gap, dmin, rms, net, id, updated, place,
                type, horizontal_error, depth_error, mag_error,
                mag_nst, status, location_source, mag_source
            )
            SELECT
                time, latitude, longitude, depth, mag,
                magType AS mag_type, nst, gap, dmin, rms,
                net, id, updated, place, type,
                horizontalError AS horizontal_error,
                depthError AS depth_error,
                magError AS mag_error,
                magNst AS mag_nst,
                status,
                locationSource AS location_source,
                magSource AS mag_source
            FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
        """)
        
        # DuckDB не возвращает количество вставленных строк напрямую
        logging.info(f"✅ INSERT executed successfully")
        
    except Exception as e:
        logging.error(f"❌ Error during INSERT: {e}")
        raise
    
    # ПРОВЕРКА 3: Сколько строк после INSERT?
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TARGET_TABLE}")
    after_count = cursor.fetchone()[0]
    logging.info(f"📊 Rows in PostgreSQL AFTER insert: {after_count}")
    logging.info(f"📊 Rows added this run: {after_count - before_count}")
    
    # Проверка конкретно за этот день
    cursor.execute(f"""
        SELECT COUNT(*) FROM {SCHEMA}.{TARGET_TABLE} 
        WHERE DATE(time) >= %s AND DATE(time) < %s
    """, (start_date, end_date))
    day_count = cursor.fetchone()[0]
    logging.info(f"📊 Rows for date {start_date}: {day_count}")
    
    # ЯВНЫЙ КОММИТ
    conn.commit()
    logging.info("✅ Explicit COMMIT executed")
    
    cursor.close()
    conn.close()
    con.close()
    
    logging.info(f"✅ Load completed for date: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end