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

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1693733605794 = ApplyMapping.apply(
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
    transformation_ctx="RenamedkeysforJoin_node1693733605794",
)

# Script generated for node Join
Join_node1693733484444 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1693733605794,
    keys1=["serialnumber"],
    keys2=["cust_serialnumber"],
    transformation_ctx="Join_node1693733484444",
)

# Script generated for node Drop Fields
DropFields_node1693733511207 = DropFields.apply(
    frame=Join_node1693733484444,
    paths=[
        "cust_serialnumber",
        "cust_sharewithpublicasofdate",
        "cust_birthday",
        "cust_registrationdate",
        "cust_sharewithresearchasofdate",
        "cust_customername",
        "cust_email",
        "cust_lastupdatedate",
        "cust_phone",
        "cust_sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1693733511207",
)

# Script generated for node Step Trainer Landing to Trusted
StepTrainerLandingtoTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693733511207,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://akwa-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerLandingtoTrusted_node3",
)

job.commit()
