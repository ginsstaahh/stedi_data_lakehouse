import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node accelerometer landing zone
accelerometerlandingzone_node1700529679236 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="sparkify",
        table_name="accelerometer_landing",
        transformation_ctx="accelerometerlandingzone_node1700529679236",
    )
)

# Script generated for node customer trusted
customertrusted_node1700529677557 = glueContext.create_dynamic_frame.from_catalog(
    database="sparkify",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1700529677557",
)

# Script generated for node SQL Query
SqlQuery309 = """
select customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate
from customer
join accelerometer
on accelerometer.user = customer.email
"""
SQLQuery_node1708714281489 = sparkSqlQuery(
    glueContext,
    query=SqlQuery309,
    mapping={
        "accelerometer": accelerometerlandingzone_node1700529679236,
        "customer": customertrusted_node1700529677557,
    },
    transformation_ctx="SQLQuery_node1708714281489",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708714184661 = DynamicFrame.fromDF(
    SQLQuery_node1708714281489.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708714184661",
)

# Script generated for node customer curated
customercurated_node1700529751867 = glueContext.getSink(
    path="s3://sparkify-bucket-gin/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node1700529751867",
)
customercurated_node1700529751867.setCatalogInfo(
    catalogDatabase="sparkify", catalogTableName="customer_curated"
)
customercurated_node1700529751867.setFormat("json")
customercurated_node1700529751867.writeFrame(DropDuplicates_node1708714184661)
job.commit()
