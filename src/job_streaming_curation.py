import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Source_Session
Source_Session_node1793652085788 = glueContext.create_dynamic_frame.from_catalog(database="db_streaming_catalog", table_name="session", transformation_ctx="Source_Session_node1793652085788")

# Script generated for node Source_Catalog
Source_Catalog_node1793653717395 = glueContext.create_dynamic_frame.from_catalog(database="db_streaming_catalog", table_name="catalog", transformation_ctx="Source_Catalog_node1793653717395")

# Script generated for node Join
Join_node1793652355953 = Join.apply(frame1=Source_Session_node1793652085788, frame2=Source_Catalog_node1793653717395, keys1=["title_id"], keys2=["title_id"], transformation_ctx="Join_node1793652355953")

# Script generated for node Change Schema
ChangeSchema_node1793653833090 = ApplyMapping.apply(frame=Join_node1793652355953, mappings=[("session_id", "string", "session_id", "string"), ("user_id", "string", "user_id", "string"), ("title_id", "string", "title_id", "string"), ("playback_start", "string", "playback_start", "timestamp"), ("playback_end", "string", "playback_end", "timestamp"), ("watched_seconds", "long", "watched_seconds", "bigint"), ("device_type", "string", "device_type", "string"), ("connection_quality", "string", "connection_quality", "string"), ("exit_reason", "string", "exit_reason", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string"), ("title_name", "string", "title_name", "string"), ("content_type", "string", "content_type", "string"), ("primary_genre", "string", "primary_genre", "string"), ("duration_seconds", "int", "duration_seconds", "bigint"), ("release_year", "int", "release_year", "int")], transformation_ctx="ChangeSchema_node1793653833090")

# Script generated for node Filter
Filter_node1793655131270 = Filter.apply(frame=ChangeSchema_node1793653833090, f=lambda row: (row["watched_seconds"] > 5), transformation_ctx="Filter_node1793655131270")

# Script generated for node Amazon S3
if (Filter_node1793655131270.count() >= 1):
   Filter_node1793655131270 = Filter_node1793655131270.coalesce(1)
AmazonS3_node1793654612562 = glueContext.write_dynamic_frame.from_options(frame=Filter_node1793655131270, connection_type="s3", format="glueparquet", connection_options={"path": "s3://streaming-s3-silver", "partitionKeys": ["year", "month", "day"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1793654612562")

job.commit()
