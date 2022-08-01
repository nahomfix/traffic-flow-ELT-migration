from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.custom import PostgreSqlToMySqlOperator


def db_to_df():
    # from sqlalchemy.types import Integer, Numeric, String

    hook = PostgresHook(postgres_conn_id="traffic_flow_dev")
    m_hook = MySqlHook(mysql_conn_id="traffic_flow_mysql")
    conn = m_hook.get_sqlalchemy_engine()
    df = hook.get_pandas_df(sql="SELECT * FROM traffic_flow;")

    df.to_sql(
        "traffic_flow",
        con=conn,
        if_exists="replace",
        index=False,
        # dtype={
        #     "track_id": Integer(),
        #     "traveled_d": Numeric(),
        #     "avg_speed": Numeric(),
        #     "lat": Numeric(),
        #     "lon": Numeric(),
        #     "speed": Numeric(),
        #     "lon_acc": Numeric(),
        #     "lat_acc": Numeric(),
        #     "time": Numeric(),
        # },
    )


def dbt_to_df():
    hook = PostgresHook(postgres_conn_id="traffic_flow_dev")
    m_hook = MySqlHook(mysql_conn_id="traffic_flow_mysql")
    conn = m_hook.get_sqlalchemy_engine()
    df_taxi = hook.get_pandas_df(sql="SELECT * FROM taxis")

    df_taxi.to_sql(
        "taxis",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_bus = hook.get_pandas_df(sql="SELECT * FROM buses")

    df_bus.to_sql(
        "buses",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_cars = hook.get_pandas_df(sql="SELECT * FROM cars")

    df_cars.to_sql(
        "cars",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_dist = hook.get_pandas_df(sql="SELECT * FROM distribution")

    df_dist.to_sql(
        "distribution",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_h_veh = hook.get_pandas_df(sql="SELECT * FROM heavy_vehicles")

    df_h_veh.to_sql(
        "heavy_vehicles",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_least = hook.get_pandas_df(sql="SELECT * FROM least_avg_speed")

    df_least.to_sql(
        "least_avg_speed",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_m_veh = hook.get_pandas_df(sql="SELECT * FROM medium_vehicles")

    df_m_veh.to_sql(
        "medium_vehicles",
        con=conn,
        if_exists="replace",
        index=False,
    )

    df_motor = hook.get_pandas_df(sql="SELECT * FROM motorcycles")

    df_motor.to_sql(
        "motorcycles",
        con=conn,
        if_exists="replace",
        index=False,
    )


dag = DAG(
    "migration_dag",
    start_date=datetime.today(),
    schedule_interval="@daily",
    concurrency=100,
)

with dag:
    start = DummyOperator(task_id="start")

    # create_table_op = MySqlOperator(
    #     task_id=f"create_mysql_table",
    #     mysql_conn_id=f"traffic_flow_mysql",
    #     sql="""
    #         create table if not exists traffic_flow (
    #             id int primary key,
    #             track_id int,
    #             type text,
    #             traveled_d double,
    #             avg_speed double,
    #             lat double,
    #             lon double,
    #             speed double,
    #             lon_acc double,
    #             lat_acc double,
    #             time double
    #         )
    #     """,
    # )

    # migrate = PostgreSqlToMySqlOperator(
    #     task_id=f"migrate",
    #     sql="""
    #         SELECT * FROM traffic_flow;
    #     """,
    #     target_table="traffic_flow",
    #     identifier="id",
    #     dag=dag,
    # )

    migration = PythonOperator(task_id="migration", python_callable=db_to_df)

    migration_dbt = PythonOperator(
        task_id="migration_dbt", python_callable=dbt_to_df
    )

    start >> migration >> migration_dbt
