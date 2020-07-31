from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logged_errors=list()
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if (len(records)<1 or len(records[0])<1):
                logged_errors.append("No records present for {}".format(table))
            numrecords=records[0][0]
            if numrecords==0:
                logged_errors.append("No records present for {}".format(table))           
                
        if len(logged_errors) > 0:
           for error in logged_errors:
               self.log.info(error)                
           raise ValueError(f"Data quality check failed.")                
        else:
           self.log.info(f"Data quality check on tables {self.tables} passed.")