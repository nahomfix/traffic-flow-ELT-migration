from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from operators.custom import PostgreSqlToMySqlOperator

dag = DAG(
    "migration_dag",
    start_date=datetime.today(),
    schedule_interval="@daily",
    concurrency=100,
)

with dag:
    start = DummyOperator(task_id="start")

    create_table_op = MySqlOperator(
        task_id=f"create_mysql_table",
        mysql_conn_id=f"traffic_flow_mysql",
        sql="""
            create table if not exists traffic_flow (
                id int primary key,
                track_id int,
                type text,
                traveled_d double,
                avg_speed double,
                lat double,
                lon double,
                speed double,
                lon_acc double,
                lat_acc double,
                time double
            )
        """,
    )

    migrate = PostgreSqlToMySqlOperator(
        task_id=f"migrate",
        sql="""
            SELECT * FROM traffic_flow;
        """,
        target_table="traffic_flow",
        identifier="id",
        dag=dag,
    )

    start >> create_table_op >> migrate
