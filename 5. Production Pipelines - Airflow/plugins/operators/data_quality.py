from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 tables =[]
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        for table in self.tables:
            redshift_hook = PostgresHook("redshift")
            records = redshift_hook.get_records(f"select count(*) from {table}") 

            if len(records) < 0 or len(records[0]) < 1: 
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]

            if num_records < 1: 
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {num_records} records")