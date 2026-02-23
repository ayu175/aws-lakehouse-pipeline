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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1765770148621 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1765770148621")

# Script generated for node Customer Curated
CustomerCurated_node1765770121242 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1765770121242")

# Script generated for node Join
SqlQuery2582 = '''
select *
from steptrainerLanding s
join customerCurated c
on s.serialnumber = c.serialnumber
'''
Join_node1765770197657 = sparkSqlQuery(glueContext, query = SqlQuery2582, mapping = {"steptrainerLanding":StepTrainerLanding_node1765770148621, "customerCurated":CustomerCurated_node1765770121242}, transformation_ctx = "Join_node1765770197657")

# Script generated for node Step Trainer Trusted Zone
EvaluateDataQuality().process_rows(frame=Join_node1765770197657, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765769452046", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedZone_node1765770347903 = glueContext.getSink(path="s3://YOUR_BUCKET/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedZone_node1765770347903")
StepTrainerTrustedZone_node1765770347903.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrustedZone_node1765770347903.setFormat("json")
StepTrainerTrustedZone_node1765770347903.writeFrame(Join_node1765770197657)
job.commit()