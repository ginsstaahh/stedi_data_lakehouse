from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

script_bucket = os.environ.get('SCRIPT_S3_BUCKET')
project_directory = os.environ.get('STEDI_LAKEHOUSE_PROJECT_DIR')
glue_arn = os.environ.get('GLUE_ROLE_ARN')

default_args = {
    'owner': 'ginsstaahh',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'iam_role_arn': glue_arn,
    'create_job_kwargs': {"GlueVersion": "4.0", "NumberOfWorkers": 3, "WorkerType": "G.1X"}
}

with DAG('glue_job_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag:
    
    accelerometer_transfer = BashOperator(
        task_id='accelerometer_transfer',
        bash_command=f'aws s3 sync {project_directory}/data/accelerometer s3://stedi-data-lakehouse/accelerometer',
    )

    customer_transfer = BashOperator(
        task_id='customer_transfer',
        bash_command=f'aws s3 sync {project_directory}/data/customer s3://stedi-data-lakehouse/customer',
    )

    step_trainer_transfer = BashOperator(
        task_id='step_trainer_transfer',
        bash_command=f'aws s3 sync {project_directory}/data/step_trainer s3://stedi-data-lakehouse/step_trainer',
    )

    accelerometer_landing_to_trusted = GlueJobOperator(
        task_id='accelerometer_landing_to_trusted',
        job_name='accelerometer_landing_to_trusted_job',    
        script_location=f's3://{script_bucket}/scripts/AccelerometerLandingToTrustedJob.scala',
    )

    customer_landing_to_trusted = GlueJobOperator(
        task_id='customer_landing_to_trusted',
        job_name='customer_landing_to_trusted_job',
        script_location=f's3://{script_bucket}/scripts/CustomerLandingToTrustedJob.scala',
    )

    customer_trusted_to_curated = GlueJobOperator(
        task_id='customer_trusted_to_curated',
        job_name='customer_trusted_to_curated_job',
        script_location=f's3://{script_bucket}/scripts/CustomerTrustedToCuratedJob.scala',
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

customer_transfer >> customer_landing_to_trusted
accelerometer_transfer >> accelerometer_landing_to_trusted
step_trainer_transfer >> step_trainer_trusted

customer_landing_to_trusted >> customer_trusted_to_curated >> step_trainer_trusted
[accelerometer_landing_to_trusted, step_trainer_trusted] >> machine_learning_curated