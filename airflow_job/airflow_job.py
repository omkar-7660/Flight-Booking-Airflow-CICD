from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Fetch environment variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow_project_omtech")
    bq_project = Variable.get("bq_project", default_var="halogen-oxide-459605-b6")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")

    # Fetch cluster configuration and other variables
    CLUSTER_CONFIG= Variable.get("CLUSTER_CONFIG", deserialize_json=True)
    CLUSTER_NAME = Variable.get("CLUSTER_NAME",default_var="dataproc-cluster")
    REGION = Variable.get("REGION", default_var="us-central1") 


    # Extract table names from the 'tables' variable
    tables = Variable.get("tables", deserialize_json=True)
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique batch ID using UUID
    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # # Task 1: File Sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow-project-1/source-{env}/flight_booking.csv",  # Full file path in GCS
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )


    # The problem is that the original code was using the DataprocCreateBatchOperator which is nothing
    # but works with the Dataproc Serverless. However, I have tried with DataprocCreateBatchOperator
    # and it was not working. The reson behind this is that when the job tried to run on the dataproc serverless
    # it is giving the error like don't have enough cores it was asking for 12 but max can provide by the
    # dataproc serverless is 8 cores. So I have changed the code reduce the code where i kept 1 executor instance
    # with 4 cores and 8GB memory and 1 driver instance with 4 cores and 8GB memory. Again new issue encountered
    # is that the to run spark job on the dataproc serverless it is asking for at least 2 executers instances
    # okay that is also fine i did that with 2 cores each and 8GB memory each. it was asking cores should be in 
    # multiple of 4. So I have changed the code to 2 executers with 4 cores each and 8GB memory each, but still the 
    # problem was not resolved because driver cores and memory were increased and total cores become 9 and
    # It is out of limit. Now i had two options either increase the cores quto 12 by raising request.
    #or use the dataproc cluster. I have chosen the second option to use the dataproc cluster.

    #     # Uncomment the following lines if you want to use Dataproc Serverless

        
#     # Task 2: Submit PySpark job to Dataproc Serverless
#     batch_details = {
#         "pyspark_batch": {
#             "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark_transformation_job.py",  # Main Python file
#             "python_file_uris": [],  # Python WHL files
#             "jar_file_uris": [],  # JAR files
#             "args": [
#                 f"--env={env}",
#                 f"--bq_project={bq_project}",
#                 f"--bq_dataset={bq_dataset}",
#                 f"--transformed_table={transformed_table}",
#                 f"--route_insights_table={route_insights_table}",
#                 f"--origin_insights_table={origin_insights_table}",
#             ]
#         },
#         "runtime_config": {
#             "version": "2.2",  # Specify Dataproc version (if needed),
             
#             "properties": {
#             "spark.executor.instances": "1",
#             "spark.executor.cores": "4",
#             "spark.executor.memory": "8g",
#             "spark.driver.cores": "4",
#             "spark.driver.memory": "8g"
#  }


#         },
#         "environment_config": {
#             "execution_config": {
#                 "service_account": "202778532141-compute@developer.gserviceaccount.com",
#                 "network_uri": "projects/halogen-oxide-459605-b6/global/networks/default",
#                 "subnetwork_uri": "projects/halogen-oxide-459605-b6/regions/us-central1/subnetworks/default",
#             }
#         },
#     }

#     pyspark_task = DataprocCreateBatchOperator(
#         task_id="run_spark_job_on_dataproc_serverless",
#         batch=batch_details,
#         batch_id=batch_id,
#         project_id="halogen-oxide-459605-b6",
#         region="us-central1",
#         gcp_conn_id="google_cloud_default",
#     )

    # Below code is for the Dataproc cluster creation and job submission.
    #commented below code because the varible defined in variables.json file. That will be imported on
    # airflow UI code for it can check in ci-cd.yml file
    
    # CLUSTER_CONFIG = {
    #     'master_config': {
    #         'num_instances': 1,
    #         'machine_type_uri': 'n1-standard-2',
    #         'disk_config': {
    #             'boot_disk_type': 'pd-standard',
    #             'boot_disk_size_gb': 30
    #         }
    #     },
    #     'worker_config': {
    #         'num_instances': 2,
    #         'machine_type_uri': 'n1-standard-2',
    #         'disk_config': {
    #             'boot_disk_type': 'pd-standard',
    #             'boot_disk_size_gb': 30
    #         }
    #     },
    #     'software_config': {
    #         'image_version': '2.2.26-debian12'
    #     }
    # }


    create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=bq_project,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_CONFIG,
    dag=dag
    )
    # Submit a PySpark job to the Dataproc cluster
    PYSPARK_JOB = {
     "reference": {"project_id": bq_project},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
     "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark_transformation_job.py",
     "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ],

     },
    }
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job_on_dataproc',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=bq_project,

    )
    # Task 3: Delete the Dataproc cluster after job completion

    delete_cluster=DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=bq_project,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule='all_done',  # Ensure this runs even if the create task fails
    )

    # Task Dependencies
    file_sensor >> create_cluster >> submit_pyspark_job >> delete_cluster