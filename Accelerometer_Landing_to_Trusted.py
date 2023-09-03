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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1693727510169 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1693727510169",
)

# Script generated for node Join
Join_node1693727557241 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1693727510169,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1693727557241",
)

# Script generated for node Drop Fields
DropFields_node1693727599157 = DropFields.apply(
    frame=Join_node1693727557241,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1693727599157",
)

# Script generated for node Acceleromet Trusted
AccelerometTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693727599157,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://akwa-bucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometTrusted_node3",
)

job.commit()
