from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    copy_sql = """
        {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql = "",
                 table = "",
                 #delimiter=",",
                 append_only = False,
                 ignore_headers=1,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        #self.delimiter = delimiter
        #self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql
        self.append_only = append_only
        self.table = table
        

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Upserting data to Redshift")
        if not self.append_only:
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
        self.log.info("Insert data from staging tables into {} fact table".format(self.table))
        formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(formatted_sql)