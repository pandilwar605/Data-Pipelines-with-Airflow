from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sqlstmt="""
    copy {} 
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    json '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 aws_credentials_id='',
                 s3_bucket='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
    
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.json_path=json_path        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("truncate table {}".format(self.table))
        self.log.info("truncated table {}".format(self.table))
        copystmt=StageToRedshiftOperator.sqlstmt.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        self.log.info('Copying data from s3 to staging')
        redshift.run(copystmt)

