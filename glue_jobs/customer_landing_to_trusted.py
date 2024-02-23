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

# Script generated for node Customer Landing
CustomerLanding_node1700090684865 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sparkify-bucket-gin/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1700090684865",
)

# Script generated for node SQL Query
SqlQuery1024 = """
select * from customer_landing
where customer_landing.shareWithResearchAsOfDate != 0;
"""
SQLQuery_node1707166424088 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1024,
    mapping={"customer_landing": CustomerLanding_node1700090684865},
    transformation_ctx="SQLQuery_node1707166424088",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700090688826 = glueContext.getSink(
    path="s3://sparkify-bucket-gin/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1700090688826",
)
CustomerTrusted_node1700090688826.setCatalogInfo(
    catalogDatabase="sparkify", catalogTableName="customer_trusted"
)
CustomerTrusted_node1700090688826.setFormat("json")
CustomerTrusted_node1700090688826.writeFrame(SQLQuery_node1707166424088)
job.commit()
