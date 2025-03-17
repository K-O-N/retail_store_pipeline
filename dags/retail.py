from airflow.decorators import dag, task
from datetime import datetime 

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig


@dag(
    start_date = datetime(2025,1,1),
    schedule = None,    # I want to trigger the dag runs manually 
    catchup = False,
    tags = ['retail']    #categorise 
)
    
def retail():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
	      task_id='upload_csv_to_gcs',
          src='include/dataset/online_retail.csv',
	      dst='raw/online_retail.csv',
	      bucket='retail_bucket_pip',
	      gcp_conn_id="gcp",
	      mime_type="text/csv",
)
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
          task_id='create_retail_dataset',
          dataset_id='retail',
          gcp_conn_id='gcp',
)   
    
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://retail_bucket_pip/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
)       
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
)

retail()  