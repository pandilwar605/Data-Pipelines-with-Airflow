from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sqlstmt="""
    Insert into {table} 
    {sql};
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,   
                 append_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql   

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table)) 
        insertstmt=LoadFactOperator.sqlstmt.format(
            self.table,
            self.sql
        )
        self.log.info('Inserting data to fact tables')
        redshift.run(insertstmt) 
