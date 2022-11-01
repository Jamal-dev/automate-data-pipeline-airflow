from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

def SQL(table_name, s3_path, credentials, format_as):
    return f'''
                COPY                {table_name}
                FROM                '{s3_path}'
                ACCESS_KEY_ID       '{credentials.access_key}'
                SECRET_ACCESS_KEY   '{credentials.secret_key}'
                FORMAT AS json      '{format_as}'
            '''

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket_name="",
                 s3_file_name = "",
                 file_format = "",
                 log_json_file = "",
                 *args, 
                 **kwargs):
        '''
        '''
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name 
        self.s3_bucket_name = s3_bucket_name
        self.s3_file_name = s3_file_name 
        self.file_format = file_format 
        self.log_json_file = log_json_file 
        self.execution_date = kwargs.get('execution_date')  

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        s3_path = f"s3://{self.s3_bucket_name}/{self.s3_file_name}"
        print(s3_path)
        self.log.info(f"Picking staging file for table {self.table_name} from location : {s3_path}")
        if self.log_json_file == '':
            copy_query = SQL(self.table_name,
                        s3_path,
                       credentials,
                       'auto')
        else:
            copy_query = SQL(self.table_name,
                        s3_path,
                       credentials,
                       self.log_json_file)
        
        self.log.info(f"Running copy query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        try:
            redshift_hook.run(query)
            self.log.info(f"Table {self.table_name} staged successfully!!")
        except:
            self.log.error(f"Copy query was not successful : {copy_query}")





