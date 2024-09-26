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

# Script generated for node customer curated
customercurated_node1707946204192 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkify-bucket-gin/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1707946204192",
)

# Script generated for node step trainer landing
steptrainerlanding_node1707180956057 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkify-bucket-gin/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1707180956057",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from step_trainer_landing
join customer_trusted
on step_trainer_landing.serialnumber = customer_trusted.serialnumber;
"""
SQLQuery_node1708118314581 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customercurated_node1707946204192,
        "step_trainer_landing": steptrainerlanding_node1707180956057,
    },
    transformation_ctx="SQLQuery_node1708118314581",
)

# Script generated for node Drop Fields
DropFields_node1700530620637 = DropFields.apply(
    frame=SQLQuery_node1708118314581,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1700530620637",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1700529751867 = glueContext.getSink(
    path="s3://sparkify-bucket-gin/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="steptrainertrusted_node1700529751867",
)
steptrainertrusted_node1700529751867.setCatalogInfo(
    catalogDatabase="sparkify", catalogTableName="step_trainer_trusted"
)
steptrainertrusted_node1700529751867.setFormat("json")
steptrainertrusted_node1700529751867.writeFrame(DropFields_node1700530620637)
job.commit()
