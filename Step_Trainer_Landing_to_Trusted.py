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

# Script generated for node Customers Curated
CustomersCurated_node1693733331952 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customers_curated",
    transformation_ctx="CustomersCurated_node1693733331952",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1693742704153 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1693742704153",
)

# Script generated for node Renamed keys for Join2
RenamedkeysforJoin2_node1693743314449 = ApplyMapping.apply(
    frame=CustomersCurated_node1693733331952,
    mappings=[
        ("serialnumber", "string", "cust_serialnumber", "string"),
        ("sharewithpublicasofdate", "long", "cust_sharewithpublicasofdate", "long"),
        ("birthday", "string", "cust_birthday", "string"),
        ("registrationdate", "long", "cust_registrationdate", "long"),
        ("sharewithresearchasofdate", "long", "cust_sharewithresearchasofdate", "long"),
        ("customername", "string", "cust_customername", "string"),
        ("email", "string", "cust_email", "string"),
        ("lastupdatedate", "long", "cust_lastupdatedate", "long"),
        ("phone", "string", "cust_phone", "string"),
        ("sharewithfriendsasofdate", "long", "cust_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin2_node1693743314449",
)

# Script generated for node Join1
Join1_node1693743003261 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=AccelerometerTrusted_node1693742704153,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join1_node1693743003261",
)

# Script generated for node Join2
Join2_node1693743168866 = Join.apply(
    frame1=Join1_node1693743003261,
    frame2=RenamedkeysforJoin2_node1693743314449,
    keys1=["user"],
    keys2=["cust_email"],
    transformation_ctx="Join2_node1693743168866",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join2_node1693743168866,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://akwa-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
