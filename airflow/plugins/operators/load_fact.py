from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 *args, 
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query 

    def execute(self, context):
        self.log.info('LoadFactOperator establishing connection')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('LoadFactOperator connection established!')
        try:
            redshift_hook.run(self.sql_query)
            self.log.info(f'LoadFactOperator, sql_query:{self.sql_query} ran successfully!')
        except:
            self.log.error(f'LoadFactOperator, sql_query:{self.sql_query} did not run sucessfully')
