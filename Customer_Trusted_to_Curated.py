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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1693730830466 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1693730830466",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join (AccT_CusT)
JoinAccT_CusT_node1693730888490 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1693730830466,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinAccT_CusT_node1693730888490",
)

# Script generated for node Customers Curated
CustomersCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=JoinAccT_CusT_node1693730888490,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://akwa-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomersCurated_node3",
)

job.commit()
