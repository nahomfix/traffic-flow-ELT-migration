import os

from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

deployment = os.environ.get("DEPLOYMENT", "dev")


class PostgreSqlToMySqlOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        sql=None,
        target_table=None,
        identifier=None,
        mysql_conn_id=f"traffic_flow_mysql",
        postgres_conn_id=f"traffic_flow_{deployment}",
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.sql = sql
        self.target_table = target_table
        self.identifier = identifier
        self.mysql_conn_id = mysql_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):

        start_date = context["data_interval_start"].strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_date = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")

        self.sql = self.sql.format(start_date=start_date, end_date=end_date)
        print("sql", self.sql)

        source = PostgresHook(self.postgres_conn_id)
        target = MySqlHook(self.mysql_conn_id)

        conn = source.get_conn()
        cursor = conn.cursor()

        cursor.execute(self.sql)

        target_fields = [x[0] for x in cursor.description]
        rows = cursor.fetchall()

        target.insert_rows(
            self.target_table,
            rows,
            target_fields=target_fields,
            replace_index=self.identifier,
            replace=True,
        )

        cursor.close()
        conn.close()
