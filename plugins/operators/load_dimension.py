from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql="""
    Insert into {table} 
    {sql};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table=table

    def execute(self, context):
        postgres=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insertsqlop=LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )
        postgres.run(insertsqlop)
