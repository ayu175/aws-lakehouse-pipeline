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

# Script generated for node Customer Trusted
CustomerTrusted_node1765767447911 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1765767447911")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1765766633540 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://YOUR_BUCKET/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1765766633540")

# Script generated for node PrivacyFilter - Join on Customer Trusted
SqlQuery0 = '''
select a.user, a.timestamp, a.x, a.y, a.z
from accelerometerLanding a
join customerTrusted c
on a.user = c.email
'''
PrivacyFilterJoinonCustomerTrusted_node1765765774544 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometerLanding":AccelerometerLanding_node1765766633540, "customerTrusted":CustomerTrusted_node1765767447911}, transformation_ctx = "PrivacyFilterJoinonCustomerTrusted_node1765765774544")

# Script generated for node Accelerometer Trusted Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilterJoinonCustomerTrusted_node1765765774544, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1765765603684", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedZone_node1765766018921 = glueContext.getSink(path="s3://YOUR_BUCKET/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedZone_node1765766018921")
AccelerometerTrustedZone_node1765766018921.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AccelerometerTrustedZone_node1765766018921.setFormat("json")
AccelerometerTrustedZone_node1765766018921.writeFrame(PrivacyFilterJoinonCustomerTrusted_node1765765774544)
job.commit()