from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_command = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_command = sql_command

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementation')
        redshift = PostgresHook(postgres_conn_id = self.redshfit_conn_id)
        redshift.run("INSERT INTO {}".format(self.table), self.sql_command)