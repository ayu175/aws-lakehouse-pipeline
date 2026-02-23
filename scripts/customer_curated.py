import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1765770148621 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1765770148621")

# Script generated for node Customer Trusted
CustomerTrusted_node1765770121242 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1765770121242")

# Script generated for node Join
SqlQuery0 = '''
select distinct c.*
from customerTrusted c
join accelerometerTrusted a
on c.email = a.user
WHERE c.shareWithResearchAsOfDate IS NOT NULL;
'''
Join_node1765770197657 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometerTrusted":AccelerometerTrusted_node1765770148621, "customerTrusted":CustomerTrusted_node1765770121242}, transformation_ctx = "Join_node1765770197657")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=Join_node1765770197657, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765769452046", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1765770347903 = glueContext.getSink(path="s3://YOUR_BUCKET/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1765770347903")
CustomerCurated_node1765770347903.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1765770347903.setFormat("json")
CustomerCurated_node1765770347903.writeFrame(Join_node1765770197657)
job.commit()