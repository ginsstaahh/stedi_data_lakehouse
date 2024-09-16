from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ginsstaahh',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'iam_role_arn': '',
    'create_job_kwargs': {"GlueVersion": "4.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"}
}

dag = DAG('glue_job_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
)

glue_job_accelerometer_landing_to_trusted = GlueJobOperator(
    task_id='glue_job_accelerometer_landing_to_trusted',
    job_name='accelerometer_landing_to_trusted_job',    
    script_location='s3://aws-glue-assets-294896341946-us-west-2/scripts/Accelerometer_Landing_to_Trusted_Job.scala',
    dag=dag
)

glue_job_customer_landing_to_trusted = GlueJobOperator(
    task_id='glue_job_customer_landing_to_trusted',
    job_name='customer_landing_to_trusted_job',
    script_location='s3://aws-glue-assets-294896341946-us-west-2/scripts/Customer_Landing_to_Trusted_Job.scala',  
    dag=dag
)

glue_job_customer_trusted_to_curated = GlueJobOperator(
    task_id='glue_job_customer_trusted_to_curated',
    job_name='customer_trusted_to_curated_job',
    script_location='s3://aws-glue-assets-294896341946-us-west-2/scripts/Customer_Trusted_to_Curated_Job.scala',
    dag=dag
)

glue_job_step_trainer_trusted = GlueJobOperator(
    task_id='glue_job_step_trainer_trusted',
    job_name='step_trainer_trusted_job',
    script_location='s3://aws-glue-assets-294896341946-us-west-2/scripts/Step_Trainer_Trusted_Job.scala',
    dag=dag
)

glue_job_machine_learning_curated = GlueJobOperator(
    task_id='glue_job_machine_learning_curated',
    job_name='machine_learning_curated_job',
    script_location='s3://aws-glue-assets-294896341946-us-west-2/scripts/Machine_Learning_Curated_Job.scala',
    dag=dag
)

glue_job_customer_landing_to_trusted >> glue_job_customer_trusted_to_curated >> glue_job_step_trainer_trusted
[glue_job_accelerometer_landing_to_trusted, glue_job_step_trainer_trusted] >> glue_job_machine_learning_curated