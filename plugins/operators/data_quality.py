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
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if (len(records)<1):
                self.log.error("{} returned no results".format(table))
                raise ValueError(f"Data Quality check failed. {table} has no records")
            numrecords=records[0][0]
            if numrecords==0:
                self.log.error("No records present in table {}".format(table))
                raise ValueError("No records present in table {}".format(table))
            self.log.info("Data quality on table {} check passed with {} records".format(table, numrecords))