import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Script generated for node customer landing zone
customerlandingzone_node1700529677557 = glueContext.create_dynamic_frame.from_catalog(
    database="sparkify",
    table_name="customer_trusted",
    transformation_ctx="customerlandingzone_node1700529677557",
)

# Script generated for node Join Customer
JoinCustomer_node1700529696052 = Join.apply(
    frame1=accelerometerlandingzone_node1700529679236,
    frame2=customerlandingzone_node1700529677557,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1700529696052",
)

# Script generated for node Drop Fields
DropFields_node1700530620637 = DropFields.apply(
    frame=JoinCustomer_node1700529696052,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1700530620637",
)

# Script generated for node accelerometer trusted zone
accelerometertrustedzone_node1700529751867 = glueContext.getSink(
    path="s3://sparkify-bucket-gin/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometertrustedzone_node1700529751867",
)
accelerometertrustedzone_node1700529751867.setCatalogInfo(
    catalogDatabase="sparkify", catalogTableName="accelerometer_trusted"
)
accelerometertrustedzone_node1700529751867.setFormat("json")
accelerometertrustedzone_node1700529751867.writeFrame(DropFields_node1700530620637)
job.commit()
