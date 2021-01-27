##########################################################################################################
## Job_Owner : Bhargava MSN                                                                       ########
## Description : This job generates Processed ESP Record
## Created Date : 2020/03/16(YYYY/MM/DD)                                                          ########
## Updation Date :Changed By############ DATE###############Description########Line Number        ########

##########################################################################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, col
from awsglue.dynamicframe import DynamicFrame
import pandas as pd
import numpy as np
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date
from datetime import timedelta
pd.set_option('display.max_columns',None)
pd.options.display.float_format = '{:.2f}'.format

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

glueContext = GlueContext(SparkContext.getOrCreate())

date_range = []
for i in range (0, 7):
    processed_date = date.today() - timedelta(days=i)
    date_range.append(processed_date)

date_range.reverse()

date_range_str = ",".join("{0}".format(d) for d in date_range)
print("Joined Dates: [{0}]".format(date_range_str))

print('Fetching data for the above dates')
path = "s3://mro-dev-esp-analytics-bucket/MRO_Analytics/DataSources/ESP/ACTIVE/RunDate={%s}/*.parquet" % date_range_str

spark_esp_df = spark.read.parquet(path)
esp_df = spark_esp_df.toPandas()

esp_df.reset_index(inplace=True)
esp_df = esp_df.drop(['Id','LOWISTagPrefix','Alarms','OOSCode','HighPriorityAlarm','DownHolePump','index'], axis=1)
times = pd.DatetimeIndex(esp_df.Timestamp)

onlyTwoDf = esp_df.filter(['UWID','Timestamp','WellName','YesterdayRuntimeHours','YesterdayStarts'], axis=1)
onlyTwoDf = onlyTwoDf.groupby([onlyTwoDf.WellName,onlyTwoDf.UWID,times.date]).agg(np.max)
onlyTwoDf.reset_index(inplace=True)
onlyTwoDf = onlyTwoDf.drop('Timestamp',axis=1)
onlyTwoDf.rename(columns = {'level_2':'PRODUCTION_DATE'},inplace = True)
onlyTwoDf['RunDate'] = onlyTwoDf['PRODUCTION_DATE']
onlyTwoDf['LOAD_TIMESTAMP'] = pd.to_datetime('now')
print('*************2Col DONE****************')
esp_df = esp_df.drop(['YesterdayRuntimeHours','YesterdayStarts'], axis=1)

# ****Range Calculation****
maxValues = esp_df.groupby([esp_df.WellName,esp_df.UWID,times.date]).agg(np.max)
minValues = esp_df.groupby([esp_df.WellName,esp_df.UWID,times.date]).agg(np.min)

rangeDf = maxValues - minValues
rangeDf = rangeDf.drop(['Timestamp'], axis=1)
rangeDf = rangeDf.add_suffix('_range')
rangeDf.reset_index(inplace=True)
rangeDf.rename(columns = {'level_2':'PRODUCTION_DATE'},inplace = True)
print('*************RANGE DONE****************')

## ****Mean Calculation****
meanDf = esp_df.groupby([esp_df.WellName,esp_df.UWID,times.date]).agg(np.mean)
meanDf = meanDf.add_suffix('_mean')
meanDf.reset_index(inplace=True)
meanDf.rename(columns = {'level_2':'PRODUCTION_DATE'}, inplace = True)
print('*************MEAN DONE****************')

## ****Median Calculation****
medianDf = esp_df.groupby([esp_df.WellName,esp_df.UWID,times.date]).median()

medianDf = medianDf.add_suffix('_median')
medianDf.reset_index(inplace=True)
medianDf.rename(columns = {'level_2':'PRODUCTION_DATE'}, inplace = True)
medianDf.info()
print('*************MEDIAN DONE****************')

## ****IQR Calculation****
def get_iqr (column):
 q1 = np.nanpercentile(column, 25,interpolation = 'midpoint')
 q3 = np.nanpercentile(column, 75,interpolation = 'midpoint')
 return q3 - q1

iqrDf = esp_df.groupby([esp_df.WellName,esp_df.UWID,times.date]).agg([get_iqr])

iqrDf = iqrDf.add_suffix('_IQR')
iqrDf.reset_index(inplace=True)

iqrDf.rename(columns = {'level_2':'PRODUCTION_DATE'}, inplace = True)
iqrDf.columns = ['WellName', 'UWID', 'PRODUCTION_DATE', 'ABVolts_IQR',
      'BCVolts_IQR', 'CAVolts_IQR', 'CasingPressure_IQR',
      'CurrentImbalance_IQR', 'DownholeAmpA_IQR',
      'DownholeAmpB_IQR', 'DownholeAmpC_IQR',
      'IntakePressure_IQR', 'IntakeTemperature_IQR',
      'MotorTemperature_IQR', 'OutputFrequency_IQR',
      'PhaseACurrent_IQR', 'PhaseBCurrent_IQR',
      'PhaseCCurrent_IQR', 'PowerConsumption_IQR',
      'PowerFactor_IQR', 'TubingPressure_IQR',
      'VoltageImbalance_IQR']
print('*************IQR DONE****************')

merged1 = pd.merge(medianDf,meanDf,how='left', on = ['PRODUCTION_DATE','WellName','UWID'])
merged1.info()
merged2 = pd.merge(iqrDf,rangeDf,how='left', on = ['PRODUCTION_DATE','WellName','UWID'])
merged2.info()
merged3 = pd.merge(merged1,merged2,how='left', on = ['PRODUCTION_DATE','WellName','UWID'])
merged3.info()
mergedf = pd.merge(onlyTwoDf,merged3,how='left', on = ['PRODUCTION_DATE','WellName','UWID'])
mergedf.info()
print('*************MERGE DONE****************')

mySchema = StructType([ StructField("WellName", StringType(), False)
,StructField("UWID", StringType(), False)
,StructField("PRODUCTION_DATE", DateType(), False)
,StructField("YesterdayRuntimeHours", DoubleType(), False)
,StructField("YesterdayStarts", DoubleType(), False)
,StructField("RunDate", DateType(), False)
,StructField("LOAD_TIMESTAMP", TimestampType(), False)
,StructField("ABVolts_median", DoubleType(), False)
,StructField("BCVolts_median", DoubleType(), False)
,StructField("CAVolts_median", DoubleType(), False)
,StructField("CasingPressure_median", DoubleType(), False)
,StructField("CurrentImbalance_median", DoubleType(), False)
,StructField("DownholeAmpA_median", DoubleType(), False)
,StructField("DownholeAmpB_median", DoubleType(), False)
,StructField("DownholeAmpC_median", DoubleType(), False)
,StructField("IntakePressure_median", DoubleType(), False)
,StructField("IntakeTemperature_median", DoubleType(), False)
,StructField("MotorTemperature_median", DoubleType(), False)
,StructField("OutputFrequency_median", DoubleType(), False)
,StructField("PhaseACurrent_median", DoubleType(), False)
,StructField("PhaseBCurrent_median", DoubleType(), False)
,StructField("PhaseCCurrent_median", DoubleType(), False)
,StructField("PowerConsumption_median", DoubleType(), False)
,StructField("PowerFactor_median", DoubleType(), False)
,StructField("TubingPressure_median", DoubleType(), False)
,StructField("VoltageImbalance_median", DoubleType(), False)
,StructField("ABVolts_mean", DoubleType(), False)
,StructField("BCVolts_mean", DoubleType(), False)
,StructField("CAVolts_mean", DoubleType(), False)
,StructField("CasingPressure_mean", DoubleType(), False)
,StructField("CurrentImbalance_mean", DoubleType(), False)
,StructField("DownholeAmpA_mean", DoubleType(), False)
,StructField("DownholeAmpB_mean", DoubleType(), False)
,StructField("DownholeAmpC_mean", DoubleType(), False)
,StructField("IntakePressure_mean", DoubleType(), False)
,StructField("IntakeTemperature_mean", DoubleType(), False)
,StructField("MotorTemperature_mean", DoubleType(), False)
,StructField("OutputFrequency_mean", DoubleType(), False)
,StructField("PhaseACurrent_mean", DoubleType(), False)
,StructField("PhaseBCurrent_mean", DoubleType(), False)
,StructField("PhaseCCurrent_mean", DoubleType(), False)
,StructField("PowerConsumption_mean", DoubleType(), False)
,StructField("PowerFactor_mean", DoubleType(), False)
,StructField("TubingPressure_mean", DoubleType(), False)
,StructField("VoltageImbalance_mean", DoubleType(), False)
,StructField("ABVolts_IQR", DoubleType(), False)
,StructField("BCVolts_IQR", DoubleType(), False)
,StructField("CAVolts_IQR", DoubleType(), False)
,StructField("CasingPressure_IQR", DoubleType(), False)
,StructField("CurrentImbalance_IQR", DoubleType(), False)
,StructField("DownholeAmpA_IQR", DoubleType(), False)
,StructField("DownholeAmpB_IQR", DoubleType(), False)
,StructField("DownholeAmpC_IQR", DoubleType(), False)
,StructField("IntakePressure_IQR", DoubleType(), False)
,StructField("IntakeTemperature_IQR", DoubleType(), False)
,StructField("MotorTemperature_IQR", DoubleType(), False)
,StructField("OutputFrequency_IQR", DoubleType(), False)
,StructField("PhaseACurrent_IQR", DoubleType(), False)
,StructField("PhaseBCurrent_IQR", DoubleType(), False)
,StructField("PhaseCCurrent_IQR", DoubleType(), False)
,StructField("PowerConsumption_IQR", DoubleType(), False)
,StructField("PowerFactor_IQR", DoubleType(), False)
,StructField("TubingPressure_IQR", DoubleType(), False)
,StructField("VoltageImbalance_IQR", DoubleType(), False)
,StructField("ABVolts_range", DoubleType(), False)
,StructField("BCVolts_range", DoubleType(), False)
,StructField("CAVolts_range", DoubleType(), False)
,StructField("CasingPressure_range", DoubleType(), False)
,StructField("CurrentImbalance_range", DoubleType(), False)
,StructField("DownholeAmpA_range", DoubleType(), False)
,StructField("DownholeAmpB_range", DoubleType(), False)
,StructField("DownholeAmpC_range", DoubleType(), False)
,StructField("IntakePressure_range", DoubleType(), False)
,StructField("IntakeTemperature_range", DoubleType(), False)
,StructField("MotorTemperature_range", DoubleType(), False)
,StructField("OutputFrequency_range", DoubleType(), False)
,StructField("PhaseACurrent_range", DoubleType(), False)
,StructField("PhaseBCurrent_range", DoubleType(), False)
,StructField("PhaseCCurrent_range" , DoubleType(), False)
,StructField("PowerConsumption_range", DoubleType(), False)
,StructField("PowerFactor_range", DoubleType(), False)
,StructField("TubingPressure_range", DoubleType(), False)
,StructField("VoltageImbalance_range", DoubleType(), False)])

result = spark.createDataFrame(mergedf,schema=mySchema)
result_repartitionframe = result.coalesce(1)

result_repartitionframe.write.mode("overwrite").format("parquet").partitionBy("RunDate").save("s3://mro-dev-esp-analytics-bucket/MRO_Analytics/DataSources/ESP_Processed/Active/")

job.commit()