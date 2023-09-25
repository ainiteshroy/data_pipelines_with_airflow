from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        table_list = ['songplays', 'users', 'songs', 'artists', 'time']
        bad_tables = []
        zero_rows_table = []
        for t in table_list:
            records = redshift_hook.get_records(f"SELECT COUNT (*) FROM t")
            if len(records) < 1 or len(records[0]) < 1:
                bad_tables.append(t)
            num_records = records[0][0]
            if num_records < 1:
                zero_rows_table.append(t)
        raise ValueError(f"{len(zero_rows_table)} tables contained 0 rows")
        raise ValueError(f"{len(bad_tables)} table(s) didn't return any output.")