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

# Script generated for node Customer Landing
CustomerLanding_node1765766633540 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1765766633540")

# Script generated for node PrivacyFilter
SqlQuery2432 = '''
select * from customerLanding
where shareWithResearchAsOfDate IS NOT NULL
'''
PrivacyFilter_node1765765774544 = sparkSqlQuery(glueContext, query = SqlQuery2432, mapping = {"customerLanding":CustomerLanding_node1765766633540}, transformation_ctx = "PrivacyFilter_node1765765774544")

# Script generated for node Customer Trusted Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1765765774544, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765765603684", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedZone_node1765766018921 = glueContext.getSink(path="s3://YOUR_BUCKET/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedZone_node1765766018921")
CustomerTrustedZone_node1765766018921.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrustedZone_node1765766018921.setFormat("json")
CustomerTrustedZone_node1765766018921.writeFrame(PrivacyFilter_node1765765774544)
job.commit()