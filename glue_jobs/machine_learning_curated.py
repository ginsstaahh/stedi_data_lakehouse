import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1708119331302 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkify-bucket-gin/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1708119331302",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1707180956057 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkify-bucket-gin/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1707180956057",
)

# Script generated for node SQL Query
SqlQuery29 = """
select * from accelerometer
join step_trainer
on accelerometer.timestamp = step_trainer.sensorreadingtime;
"""
SQLQuery_node1708120300334 = sparkSqlQuery(
    glueContext,
    query=SqlQuery29,
    mapping={
        "accelerometer": accelerometertrusted_node1708119331302,
        "step_trainer": steptrainertrusted_node1707180956057,
    },
    transformation_ctx="SQLQuery_node1708120300334",
)

# Script generated for node ML trusted
MLtrusted_node1700529751867 = glueContext.getSink(
    path="s3://sparkify-bucket-gin/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MLtrusted_node1700529751867",
)
MLtrusted_node1700529751867.setCatalogInfo(
    catalogDatabase="sparkify", catalogTableName="machine_learning_curated"
)
MLtrusted_node1700529751867.setFormat("json")
MLtrusted_node1700529751867.writeFrame(SQLQuery_node1708120300334)
job.commit()
