from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
import os

script_bucket = os.environ.get('SCRIPT_S3_BUCKET')
glue_arn = os.environ.get('GLUE_ROLE_ARN')

default_args = {
    'owner': 'ginsstaahh',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'iam_role_arn': glue_arn,
    'glue_job_args': {'--job-language': 'scala'},
    'create_job_kwargs': {"GlueVersion": "4.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"}
}

with DAG('glue_job_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag:

    accelerometer_landing_to_trusted = GlueJobOperator(
        task_id='accelerometer_landing_to_trusted',
        job_name='accelerometer_landing_to_trusted_job',    
        script_location=f's3://{script_bucket}/scripts/AccelerometerLandingToTrustedJob.scala',
    )

    customer_landing_to_trusted = GlueJobOperator(
        task_id='customer_landing_to_trusted',
        job_name='customer_landing_to_trusted_job',
        script_location=f's3://{script_bucket}/scripts/Customer_Landing_to_Trusted_Job.scala',
    )

    customer_trusted_to_curated = GlueJobOperator(
        task_id='customer_trusted_to_curated',
        job_name='customer_trusted_to_curated_job',
        script_location=f's3://{script_bucket}/scripts/Customer_Trusted_to_Curated.scala',
    )

    step_trainer_trusted = GlueJobOperator(
        task_id='step_trainer_trusted',
        job_name='step_trainer_trusted_job',
        script_location=f's3://{script_bucket}/scripts/StepTrainerTrustedJob.scala',
    )

    machine_learning_curated = GlueJobOperator(
        task_id='machine_learning_curated',
        job_name='machine_learning_curated_job',
        script_location=f's3://{script_bucket}/scripts/MachineLearningCuratedJob.scala',
    )

customer_landing_to_trusted >> customer_trusted_to_curated >> step_trainer_trusted
[accelerometer_landing_to_trusted, step_trainer_trusted] >> machine_learning_curated