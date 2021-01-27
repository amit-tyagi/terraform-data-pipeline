##########################################################################################################
## Job_Owner : Bhargava MSN                                                                       ########
## Description : This job fetches ESP data from SCADA to our S3 Bucket
## Created Date : 2020/03/16(YYYY/MM/DD)                                                          ########
## Updation Date :Changed By############ DATE###############Description########Line Number        ########

##########################################################################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions  import date_format
from datetime import date
from datetime import timedelta

print ('Job has been started!')

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
glueContext = GlueContext(sc)

# New logic to read the data for multiple partitions
date_range = []
for i in range (0, 7):
    processed_date = date.today() - timedelta(days=i)
    date_range.append(processed_date)

date_range.reverse()

date_range_str = ",".join("{0}".format(d) for d in date_range)
print("Joined Dates: [{0}]".format(date_range_str))

print('Fetching data for the above dates')
path = "s3://mro-scada-dev-bucket01/esp/BAKKEN_ESP_ACTIVE_NEW/processed_date={%s}/*.parquet" % date_range_str
raw_data = spark.read.parquet(path)

final_dataframe = raw_data.withColumn("RunDate", date_format('Timestamp', "yyyy-MM-dd"))

print('Writing output to the S3 bucket')
final_dataframe.write.mode("overwrite").format("parquet").partitionBy("RunDate").save("s3://mro-dev-esp-analytics-bucket/MRO_Analytics/DataSources/ESP/ACTIVE/")
print('Output written to S3')

job.commit()

print ('Job has been completed!')
