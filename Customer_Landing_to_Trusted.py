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
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where sharewithresearchasofdate IS NOT NULL;

"""
SQLQuery_node1693726958630 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1},
    transformation_ctx="SQLQuery_node1693726958630",
)

# Script generated for node Customer Trusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1693726958630,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://akwa-bucket/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
