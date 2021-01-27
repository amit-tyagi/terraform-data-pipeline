#General Python Context
import sys
from datetime import date
from datetime import timedelta

#AWS Context
import boto3

#Glue/Spark Context
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.types as typ
from pyspark.sql.functions import col, max as max_

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def gen_bucket_path(bucket_name, prefix):
    return "s3://{0}/{1}".format(bucket_name, prefix)


#Source buckets
hana_src_bukcet = "mro-hanap-dev-dl-bucket01"
wvw_src_bucket = "mro-wvp-dev-bucket01"
tdm_src_bucket = "mro-tdmp-dev-dl-bucket01"
prcnt_src_bucket = "mro-dl-dev-s3-transformed"
#Destination buckets
esp_dest_bucket = "mro-dev-esp-analytics-bucket"

#Today's date
load_date = date.today().strftime("%Y-%m-%d")
print("Today's Date:", load_date)

#ProCount Well Header
df_prcnt_wh = spark.read.parquet(
    "s3://{0}/proreport/MVW_WELLHEADER/processed_date={1}/*.parquet".format(hana_src_bukcet, load_date)
)
df_prcnt_wh = df_prcnt_wh.repartition(1)
df_prcnt_wh.createOrReplaceTempView('tv_prcnt_wh')

#WellView Well Header
df_wvw_wh = spark.read.parquet(
    "s3://{0}/WELLV10P/WV100CALCUNITS_US/WVWELLHEADER/processed_date={1}/*.parquet".format(wvw_src_bucket, load_date)
)
df_wvw_wh = df_wvw_wh.repartition(1)

#Filtering Wellview Data to be only attributes that we want to bring in.
df_wvw_wh = df_wvw_wh.select('WELLIDD','IDWELL','WELLNAME','WELLCONFIG')
df_wvw_wh.createOrReplaceTempView('tv_wvw_wh')

#adding derived fields
df_prcnt_wh = spark.sql("""
    SELECT
        CASE
            WHEN UPPER(CURRENTPRODUCINGSTATUS) IN ('PRODUCING','RI','ORRI') THEN True
            ELSE False
        END                                as ACTIVE,
        SUBSTRING(MIDASWELL,0,8)           as MIDASWELL,
        SUBSTRING(APIWELLNUMBER,1,10)      as APIWELLNUMBER,
        SUBSTRING(APIWELLNUMBER,11,2)      as APIWELL_SIDETRACK,
        PROCOUNTID,
        UPPER(CURRENTPRODUCINGMETHOD)      as CURRENTPRODUCINGMETHOD,
        UPPER(CURRENTPRODUCINGSTATUS)      as CURRENTPRODUCINGSTATUS,
        CASE
            WHEN UPPER(CURRENTPRODUCINGMETHOD) IN ('PLUNGER LIFT', 'GAS LIFT', 'ROD PUMP','GAS ASSISTED PLUNGER LIFT',
                                            'INTERMITTENT ROD PUMP','SUBMERSIBLE PUMP','INTERMITTENT GAS LIFT',
                                            'ANNULAR VELOCITY ENHANCEMENT','JET PUMP','SUCKER ROD (REMAP MID=5)',
                                            'PROGRESSIVE CAVITY PUMP') THEN True
            ELSE False
        END                                 as HAS_AL,
        CASE
            WHEN OPERATEDSTATUS = 'COOP' AND UPPER(CURRENTPRODUCINGSTATUS) = 'PRODUCING' THEN KPIFIRSTPRODDATE
            WHEN OPERATEDSTATUS = 'COOP' AND UPPER(CURRENTPRODUCINGSTATUS) != 'PRODUCING' AND KPIFIRSTPRODDATE ='1900-01-01 00:00:00' THEN LEAST(FIRSTPRODOIL,FIRSTPRODGAS)
            WHEN OPERATEDSTATUS = 'OBO' THEN LEAST(FIRSTPRODOIL,FIRSTPRODGAS)
            ELSE KPIFIRSTPRODDATE
        END                                as FIRSTPRODDATE,
        VP,
        UPPER(ASSETTEAM)                   as ASSETTEAM,
        UPPER(SUBASSET)                    as SUBASSET,
        UPPER(FIELDGROUPNAME)              as FIELDGROUPNAME,
        UPPER(PAD)                         as PAD,
        UPPER(FORMATION)                   as FORMATION,
        UPPER(RESERVOIR)                   as RESERVOIR,
        UPPER(AREA)                        as AREA,
        UPPER(FACILITY)                    as FACILITY,
        PLANTID,
        UPPER(PLATFORM)                    as PLATFORM,
        UPPER(PROSPECT)                    as PROSPECT,
        UPPER(ROUTENAME)                   as ROUTENAME,
        UPPER(STOPNAME)                    as STOPNAME,
        UPPER(COMPLETIONNAME)              as COMPLETIONNAME,
        MIDASCOMPLETION,
        UPPER(COUNTRY)                     as COUNTRY,
        UPPER(STATE)                       as STATE,
        UPPER(COUNTY)                      as COUNTY,
        SURFACELATITUDE,
        SURFACELONGITUDE,
        ELEVATION,
        MEASUREDDEPTH,
        VERTICALDEPTH,
        UPPER(GATHERINGSYSTEM)             as GATHERINGSYSTEM,
        UPPER(MAJORWELLTYPE)               as MAJORWELLTYPE,
        MAXINJPRESS,
        GASNETREVENUEINTEREST,
        OILNETREVENUEINTEREST,
        UPPER(OPERATORNAME)                as OPERATORNAME,
        UPPER(OPERATEDSTATUS)              as OPERATEDSTATUS,
        SAPWELLCOMPLETION,
        TARGETINJVOL,
        TOWID,
        UICTESTPRESS,
        WORKINGINTEREST,
        UPPER(ACQUISITION)                 as ACQUISITION
    FROM
        (SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY PROCOUNTID ORDER BY MIDASWELL) as row_num
        FROM tv_prcnt_wh)A
    WHERE row_num = 1
""")

#Removing all strings that say UNASSIGNED
df_prcnt_wh = df_prcnt_wh.replace([' UNASSIGNED','UNASSIGNED'], None)

#Creating Temp View
df_prcnt_wh.createOrReplaceTempView('tv_prcnt_wh')

df_prcnt_wh_dedup = spark.sql("""
    SELECT *
    FROM
        (SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY MIDASWELL ORDER BY  FIRSTPRODDATE DESC) as row_num
        FROM
            (SELECT
                    ACTIVE,
                    MIDASWELL,
                    APIWELLNUMBER,
                    APIWELL_SIDETRACK,
                    COLLECT_SET(PROCOUNTID)[0] as PROCOUNTID,
                    COLLECT_SET(CURRENTPRODUCINGMETHOD)[0] as CURRENTPRODUCINGMETHOD,
                    COLLECT_SET(CURRENTPRODUCINGSTATUS) as CURRENTPRODUCINGSTATUS,
                    HAS_AL,
                    FIRSTPRODDATE,
                    VP,
                    ASSETTEAM,
                    SUBASSET,
                    FIELDGROUPNAME,
                    PAD,
                    COLLECT_SET(FORMATION)[0] as FORMATION,
                    COLLECT_SET(RESERVOIR)[0] as RESERVOIR,
                    AREA,
                    FACILITY,
                    COLLECT_SET(COMPLETIONNAME)[0] as COMPLETIONNAME,
                    COLLECT_SET(MIDASCOMPLETION)[0] as MIDASCOMPLETION,
                    COUNTRY,
                    STATE,
                    COUNTY,
                    SURFACELATITUDE,
                    SURFACELONGITUDE,
                    ELEVATION,
                    MEASUREDDEPTH,
                    VERTICALDEPTH,
                    MAJORWELLTYPE,
                    OILNETREVENUEINTEREST,
                    GASNETREVENUEINTEREST,
                    WORKINGINTEREST,
                    OPERATORNAME,
                    OPERATEDSTATUS,
                    COLLECT_SET(PROSPECT)[0] as PROSPECT,
                    COLLECT_SET(ROUTENAME)[0] as ROUTENAME,
                    COLLECT_SET(STOPNAME)[0] as STOPNAME,
                    ACQUISITION
                FROM tv_prcnt_wh
                GROUP BY
                    ACTIVE,MIDASWELL,APIWELLNUMBER,APIWELL_SIDETRACK,HAS_AL,FIRSTPRODDATE,VP,ASSETTEAM,
                    SUBASSET,FIELDGROUPNAME,PAD,AREA,FACILITY,COUNTRY,STATE,COUNTY,SURFACELATITUDE,
                    SURFACELONGITUDE,ELEVATION,MEASUREDDEPTH,VERTICALDEPTH,MAJORWELLTYPE,OILNETREVENUEINTEREST,
                    GASNETREVENUEINTEREST,WORKINGINTEREST,OPERATORNAME,OPERATEDSTATUS,ACQUISITION)A
        )B
    WHERE row_num =1
 """)

df_prcnt_wh_dedup.createOrReplaceTempView('tv_prcnt_wh_dedup')

df_tdm_wh = spark.read.parquet("s3://{0}/TDM_WELL_HEADER_VW/*/processed_date={1}/*.parquet".format(tdm_src_bucket, load_date))
df_tdm_wh.createOrReplaceTempView('tv_tdm_wh')

# Filtering the TDM for Marathon wells
df_tdm_wh = spark.sql("""
    SELECT
        ACTIVE_IND                     as ACTIVE_IND,
        SUBSTR(EIQ_UWI,1,8)            as UWI,
        EIQ_PARENT_UWI                 as PARENT_UWI,
        EIQ_GRANDPARENT_UWI            as GRANDPARENT_UWI,
        EIQ_WELL_NUMBER                as WELL_NUMBER,
        EIQ_COMMON_WELL_NAME           as COMMON_WELL_NAME,
        EIQ_WELL_NAME                  as WELL_NAME,
        EIQ_BASIN_NAME                 as BASIN_NAME,
        EIQ_ASSET                      as ASSET,
        EIQ_SUB_ASSET                  as SUB_ASSET,
        EIQ_FIELD_NAME                 as FIELD_NAME,
        EIQ_COST_CENTER_ID             as COST_CENTER_ID,
        EIQ_COUNTRY                    as COUNTRY,
        EIQ_STATE_NAME                 as STATE_NAME,
        EIQ_COUNTY_NAME                as COUNTY_NAME,
        EIQ_DISTRICT_NAME              as DISTRICT_NAME,
        EIQ_CURRENT_OPERATOR           as CURRENT_OPERATOR,
        TO_DATE(CAST(EIQ_SPUD_DATE as timestamp)) as SPUD_DATE,
        TO_DATE(CAST(EIQ_FINAL_DRILL_DATE as timestamp)) as FINAL_DRILL_DATE,
        TO_DATE(CAST(EIQ_COMPLETION_DATE as timestamp)) as COMPLETION_DATE,
        TO_DATE(CAST(EIQ_RELEASE_DATE_OF_RIG as timestamp)) as RELEASE_DATE_OF_RIG,
        TO_DATE(CAST(EIQ_FIRST_PRODUCTION_DATE as timestamp)) as FIRST_PRODUCTION_DATE,
        TO_DATE(CAST(EIQ_ABANDONMENT_DATE as timestamp)) as ABANDONMENT_DATE,
        EIQ_FINAL_CLASS                as FINAL_CLASS,
        EIQ_FINAL_STATUS               as FINAL_STATUS,
        EIQ_OPERATOR_NAME              as OPERATOR_NAME,
        EIQ_OPERATORSHIP               as OPERATORSHIP,
        EIQ_FORM_AT_TD                 as FORM_AT_TD,
        EIQ_FORM_AT_TD_AGE             as FORM_AT_TD_AGE,
        EIQ_GOVERNMENT_ID              as GOVERNMENT_ID,
        EIQ_HAS_PRODUCTION             as HAS_PRODUCTION,
        EIQ_HAS_WELL_FORMATION         as HAS_WELL_FORMATION,
        EIQ_HAS_WELL_PERFORATION       as HAS_WELL_PERFORATION,
        EIQ_HAS_WELL_PRESSURE          as HAS_WELL_PRESSURE,
        EIQ_HAS_WELL_SURVEYS           as HAS_WELL_SURVEYS,
        EIQ_HAS_WELL_TEST              as HAS_WELL_TEST,
        EIQ_INITIAL_CLASS              as INITIAL_CLASS,
        EIQ_LEASE_NAME                 as LEASE_NAME,
        EIQ_LEASE_NUMBER               as LEASE_NUMBER,
        EIQ_LEVEL_TYPE                 as LEVEL_TYPE,
        EIQ_LOCATION_QUALIFIER         as LOCATION_QUALIFIER,
        EIQ_GLOBAL_COORD_SYS_NAME      as GLOBAL_COORD_SYS_NAME,
        EIQ_RAW_COORD_SYS_NAME         as RAW_COORD_SYS_NAME,
        EIQ_REGIONAL_COORD_SYS_NAME    as REGIONAL_COORD_SYS_NAME,
        EIQ_LAT_BOTTOM_GLOBAL          as LAT_BOTTOM_GLOBAL,
        EIQ_LAT_BOTTOM_RAW             as LAT_BOTTOM_RAW,
        EIQ_LAT_BOTTOM_REGIONAL        as LAT_BOTTOM_REGIONAL,
        EIQ_LAT_SURFACE_GLOBAL         as LAT_SURFACE_GLOBAL,
        EIQ_LAT_SURFACE_RAW            as LAT_SURFACE_RAW,
        EIQ_LAT_SURFACE_REGIONAL       as LAT_SURFACE_REGIONAL,
        EIQ_LONG_BOTTOM_GLOBAL         as LONG_BOTTOM_GLOBAL,
        EIQ_LONG_BOTTOM_RAW            as LONG_BOTTOM_RAW,
        EIQ_LONG_BOTTOM_REGIONAL       as LONG_BOTTOM_REGIONAL,
        EIQ_LONG_SURFACE_GLOBAL        as LONG_SURFACE_GLOBAL,
        EIQ_LONG_SURFACE_RAW           as LONG_SURFACE_RAW,
        EIQ_LONG_SURFACE_REGIONAL      as LONG_SURFACE_REGIONAL,
        EIQ_NRI_GAS                    as NRI_GAS,
        EIQ_NRI_NGL                    as NRI_NGL,
        EIQ_NRI_OIL                    as NRI_OIL,
        EIQ_PROSPECT_AREA              as PROSPECT_AREA,
        EIQ_GR_ELEV                    as GR_ELEV,
        EIQ_GR_ELEV_TYPE               as GR_ELEV_TYPE,
        EIQ_GR_ELEV_UNIT               as GR_ELEV_UNIT,
        EIQ_KB_ELEV                    as KB_ELEV,
        EIQ_KB_ELEV_UNIT               as KB_ELEV_UNIT,
        EIQ_REFERENCE_ELEV             as REFERENCE_ELEV,
        EIQ_REFERENCE_ELEV_UNIT        as REFERENCE_ELEV_UNIT,
        EIQ_RESERVE_ID                 as RESERVE_ID,
        EIQ_DATUM_NAME                 as DATUM_NAME,
        EIQ_TOTAL_DEPTH                as TOTAL_DEPTH,
        EIQ_TOTAL_DEPTH_UNIT           as TOTAL_DEPTH_UNIT,
        EIQ_DEPTH_DATUM                as DEPTH_DATUM,
        EIQ_DERRICK_FLOOR_ELEV         as DERRICK_FLOOR_ELEV,
        EIQ_DERRICK_FLOOR_UNIT         as DERRICK_FLOOR_UNIT,
        EIQ_TVD                        as TVD,
        EIQ_TVD_UNIT                   as TVD_UNIT,
        EIQ_WORKING_INTEREST           as WORKING_INTEREST,
        INDEX_ID                       as INDEX_ID,
        ROUTING_ID                     as ROUTING_ID,
        EIQ_SOURCE_NAME                as SOURCE_NAME,
        EIQ_LASTCHG_BY                 as LASTCHG_BY,
        EIQ_LASTCHG_DATE               as LASTCHG_DATE
    FROM tv_tdm_wh
    WHERE ((UPPER(EIQ_OPERATOR_NAME) LIKE 'MARATH%') OR (EIQ_ASSET IS NOT NULL)) AND
        EIQ_LEVEL_TYPE = 'WELL'
""")
df_tdm_wh = df_tdm_wh.withColumn('COUNTRY', regexp_replace('COUNTRY', 'US', 'UNITED STATES'))
df_tdm_wh.createOrReplaceTempView('tv_tdm_wh_stage')


df_tdm_prcnt_wh_join = spark.sql("""
SELECT
    prcnt.ACTIVE AS ACTIVE_IND,
    tdm.WELL_NAME,
    COALESCE(tdm.UWI, prcnt.MIDASWELL) as UWI,
    COALESCE(tdm.OPERATOR_NAME, prcnt.OPERATORNAME) as OPERATOR_NAME,
    CASE
        WHEN array_contains(prcnt.CURRENTPRODUCINGSTATUS,'PRODUCING') = True THEN 'PRODUCING'
        WHEN array_contains(prcnt.CURRENTPRODUCINGSTATUS,'TEMPORARILY ABANDONED') = True THEN 'TEMPORARILY ABANDONED'
        WHEN array_contains(prcnt.CURRENTPRODUCINGSTATUS,'PERMANENTLY ABANDONED') = True THEN 'PERMANENTLY ABANDONED'
        WHEN array_contains(prcnt.CURRENTPRODUCINGSTATUS,'SOLD') = True THEN 'SOLD'
        ELSE CURRENTPRODUCINGSTATUS[0]
    END AS CURRENTPRODUCINGSTATUS,
    COALESCE(tdm.ASSET, prcnt.ASSETTEAM) as ASSET,
    COALESCE(tdm.SUB_ASSET,prcnt.SUBASSET) as SUB_ASSET,
    COALESCE(tdm.FIELD_NAME, prcnt.FIELDGROUPNAME) as FIELD_NAME,
    prcnt.PAD,
    tdm.BASIN_NAME,
    prcnt.RESERVOIR,
    prcnt.AREA,
    prcnt.FACILITY,
    COALESCE(tdm.LAT_SURFACE_RAW, prcnt.SURFACELATITUDE) as LAT_SURFACE_RAW,
    COALESCE(tdm.LONG_SURFACE_RAW, prcnt.SURFACELONGITUDE) as LONG_SURFACE_RAW,
    COALESCE(tdm.COUNTRY, prcnt.COUNTRY) as COUNTRY,
    COALESCE(tdm.COUNTY_NAME, prcnt.COUNTY) as COUNTY,
    COALESCE(tdm.STATE_NAME, prcnt.STATE) as STATE,
    tdm.FINAL_DRILL_DATE,
    tdm.RELEASE_DATE_OF_RIG,
    tdm.COMPLETION_DATE,
    tdm.SPUD_DATE,
    COALESCE(
        CASE
            WHEN from_unixtime(unix_timestamp(prcnt.FIRSTPRODDATE,'MM/dd/yyyy hh:mm:ss a'),'yyyy-MM-dd') != '1900-01-01' THEN
                from_unixtime(unix_timestamp(prcnt.FIRSTPRODDATE,'MM/dd/yyyy hh:mm:ss a'),'yyyy-MM-dd')
            ELSE Null
        END,
        tdm.FIRST_PRODUCTION_DATE) AS FIRST_PROD_DATE,
    tdm.ABANDONMENT_DATE,
    tdm.DISTRICT_NAME,
    tdm.FORM_AT_TD,
    prcnt.FORMATION,
    COALESCE(tdm.GOVERNMENT_ID,prcnt.APIWELLNUMBER) as APIWELLNUMBER,
    prcnt.APIWELL_SIDETRACK,
    COALESCE(tdm.GR_ELEV, prcnt.ELEVATION) as ELEVATION,
    tdm.LEASE_NAME,
    COALESCE(prcnt.GASNETREVENUEINTEREST,tdm.NRI_GAS) as NRI_GAS,
    tdm.NRI_NGL,
    COALESCE(prcnt.OILNETREVENUEINTEREST, tdm.NRI_OIL) as NRI_OIL,
    COALESCE(tdm.OPERATORSHIP, prcnt.OPERATEDSTATUS) as OPERATED_STATUS,
    COALESCE(tdm.TOTAL_DEPTH, prcnt.MEASUREDDEPTH,'0') as MEASURED_DEPTH_FEET,
    COALESCE(tdm.TVD, prcnt.VERTICALDEPTH) as TRUE_VERT_DEPTH_FEET,
    COALESCE(prcnt.WORKINGINTEREST,tdm.WORKING_INTEREST) as WORKINGINTEREST,
    tdm.RESERVE_ID,
    COALESCE(tdm.PROSPECT_AREA, prcnt.PROSPECT) as PROSPECT_AREA,
    prcnt.ROUTENAME,
    tdm.ROUTING_ID,
    prcnt.PROCOUNTID,
    prcnt.CURRENTPRODUCINGMETHOD,
    prcnt.HAS_AL,
    prcnt.COMPLETIONNAME,
    prcnt.MIDASCOMPLETION,
    prcnt.MAJORWELLTYPE,
    prcnt.ACQUISITION
FROM tv_tdm_wh_stage tdm
FULL OUTER JOIN tv_prcnt_wh_dedup prcnt
    on tdm.uwi = prcnt.MIDASWELL
""")
df_tdm_prcnt_wh_join.createOrReplaceTempView('tv_tdm_prcnt_wh_join')

df_tdm_prcnt_wh_join = df_tdm_prcnt_wh_join.repartition(1)
df_tdm_prcnt_wh_join.write.parquet("s3://{0}/MRO_Analytics/Staging/WAR/tdm_prcnt_wh/".format(esp_dest_bucket), mode ="overwrite")

#ProCount Daily Well Production Data
df_daily_well = spark.read.parquet("s3://{0}/processdata/pct-wellproductionvolumes-sixtyday/".format(prcnt_src_bucket))
df_daily_well = df_daily_well.repartition(1)
df_daily_well.createOrReplaceTempView('tv_daily_well')

#Filter data from wyoming, as it is a divested asset
df_daily_well = df_daily_well.filter(col('ASSETTEAM') !='Wyoming')

#Convert production date to datetime and rename field
df_daily_well = df_daily_well.withColumn('PRODUCTION_DATE',to_date(to_timestamp("productiondate")))

# Converting production values to double datatype for Aggregation
df_daily_well = df_daily_well.withColumn('ALLOCATED_OIL_PRODUCTION',df_daily_well["ALLOCATEDOILPRODUCTION"].cast(typ.DoubleType()))
df_daily_well = df_daily_well.withColumn('ALLOCATED_GAS_PRODUCTION',df_daily_well["ALLOCATEDGASPRODUCTION"].cast(typ.DoubleType()))

#Dropping Fields not needed or that have been renamed
df_daily_well = df_daily_well.drop('ALLOCATEDOILPRODUCTION')
df_daily_well = df_daily_well.drop('ALLOCATEDGASPRODUCTION')
df_daily_well = df_daily_well.drop('productiondate')
df_daily_well.createOrReplaceTempView('tv_daily_well')

#Create temp view
df_daily_well.createOrReplaceTempView('tv_daily_well')

#This dataset aggregates production statisics, for a procount ID, for given time ranges. It also produces the rolling 30 day averages that
#will can used to derive wither a well is active or not.
var_max_prod_dt = spark.sql("""SELECT MAX(PRODUCTION_DATE) as max_prod_dt  FROM tv_daily_well""").first()['max_prod_dt']

df_daily_well_agg = spark.sql("""
    SELECT
        PROCOUNTID,
        '{0}' as LAST_PROD_DT,
        date_sub('{0}', 4) as 5_DAYS_DT,
        SUM(CASE
                WHEN PRODUCTION_DATE >= date_sub('{0}', 29) THEN ALLOCATED_PRODUCTION
                ELSE 0
            END) / 30 as rllng_30day_boe_avg,
        SUM(CASE
                WHEN PRODUCTION_DATE >= date_sub('{0}', 29) THEN DOWNTIMEHOURS
                ELSE 0
            END) / 30 as rllng_30day_dwntme_avg,
        SUM(CASE
                WHEN PRODUCTION_DATE >= date_sub('{0}', 4) THEN ALLOCATED_PRODUCTION
                ELSE 0
            END) as 5day_boe_amt,
        SUM(CASE
                WHEN PRODUCTION_DATE >= date_sub('{0}', 29) THEN ALLOCATED_PRODUCTION
                ELSE 0
            END) as 30day_boe_amt,
        SUM(CASE
                WHEN PRODUCTION_DATE >= date_sub('{0}', 59) THEN ALLOCATED_PRODUCTION
                ELSE 0
            END) as 60day_boe_amt
    FROM
    (SELECT
        PROCOUNTID,
        PRODUCTION_DATE,
        (ALLOCATED_OIL_PRODUCTION + (ALLOCATED_GAS_PRODUCTION / 6) + ALLOCATEDNGLPRODUCTION) as ALLOCATED_PRODUCTION,
        ALLOCATEDWATERPRODUCTION,
        DOWNTIMEHOURS
    FROM tv_daily_well)A
    GROUP BY
        procountid,
        '{0}',
        date_sub('{0}', 4)""".format(var_max_prod_dt))

#Create temp view
df_daily_well_agg.createOrReplaceTempView('tv_df_daily_well_agg')

df_join_daily_pc_wh = spark.sql("""
    SELECT
        UWI,
        MAX(LAST_PROD_DT)                           as LAST_PROD_DT,
        COALESCE(AVG(rllng_30day_boe_avg),'0.0')    as RLLNG_30DAY_BOE_AVG,
        COALESCE(AVG(rllng_30day_dwntme_avg),'0.0') as RLLNG_30DAY_DWNTME_AVG,
        COALESCE(SUM(5day_boe_amt),'0.0')           as PAST_5DAY_BOE_AMT,
        COALESCE(SUM(30day_boe_amt),'0.0')          as PAST_30DAY_BOE_AMT,
        COALESCE(SUM(60day_boe_amt),'0.0')          as PAST_60DAY_BOE_AMT
    FROM (SELECT
            dly.LAST_PROD_DT,
            dly.5_DAYS_DT,
            dly.rllng_30day_boe_avg,
            dly.rllng_30day_dwntme_avg,
            dly.5day_boe_amt,
            dly.30day_boe_amt,
            dly.60day_boe_amt,
            pc.UWI
        FROM tv_tdm_prcnt_wh_join pc
        LEFT OUTER JOIN tv_df_daily_well_agg dly
        ON pc.PROCOUNTID=dly.procountid)A
    GROUP BY UWI
    """)

#df_join_daily_pc_wh= df_join_daily_pc_wh.repartition(1)
df_join_daily_pc_wh.createOrReplaceTempView('tv_join_daily_pc_wh')

#writing the staging file of daily well and well header aggregations
df_join_daily_pc_wh = df_join_daily_pc_wh.repartition(1)
df_join_daily_pc_wh.write.parquet("s3://{0}/Staging/prcnt_daily_well/".format(esp_dest_bucket), mode ="overwrite")

df_tdm_pc_daily_join = spark.sql("""
    SELECT
        COALESCE(
            CASE
                WHEN t1.OPERATED_STATUS='COOP' THEN t1.ACTIVE_IND
                ELSE Null
            END,
            CASE
                WHEN t2.RLLNG_30DAY_BOE_AVG >= 10.0 and t2.RLLNG_30DAY_DWNTME_AVG < 12.0 THEN True
                WHEN t2.RLLNG_30DAY_BOE_AVG <= 10.0 or t2.RLLNG_30DAY_DWNTME_AVG > 12.0 THEN False
                ELSE Null
            END,
            CASE
                WHEN t1.ACTIVE_IND IS NOT NULL THEN t1.ACTIVE_IND
                ELSE Null
            END,
            False) AS ACTIVE_IND,
        t1.WELL_NAME,
        t1.UWI,
        t1.OPERATOR_NAME,
        CASE
            WHEN t1.OPERATED_STATUS='OBO' AND t1.CURRENTPRODUCINGSTATUS = 'PRODUCING' AND
                (t2.RLLNG_30DAY_BOE_AVG >= 10.0 and t2.RLLNG_30DAY_DWNTME_AVG < 12.0) THEN 'PRODUCING'
            WHEN  t1.OPERATED_STATUS='OBO' AND t1.CURRENTPRODUCINGSTATUS = 'PRODUCING' AND
                (t2.RLLNG_30DAY_BOE_AVG <= 10.0 or t2.RLLNG_30DAY_DWNTME_AVG > 12.0) THEN Null
            ELSE t1.CURRENTPRODUCINGSTATUS
        END AS CURRENTPRODUCINGSTATUS,
        'Base' as WELL_LIFE_CYCLE,
        t1.ASSET,
        t1.SUB_ASSET,
        t1.FIELD_NAME,
        t1.PAD,
        t1.BASIN_NAME,
        t1.RESERVOIR,
        t1.AREA,
        t1.FACILITY,
        t1.LAT_SURFACE_RAW,
        t1.LONG_SURFACE_RAW,
        t1.COUNTRY,
        t1.COUNTY,
        t1.STATE,
        t1.FINAL_DRILL_DATE,
        t1.RELEASE_DATE_OF_RIG,
        t1.COMPLETION_DATE,
        t1.SPUD_DATE,
        t1.FIRST_PROD_DATE,
        t2.LAST_PROD_DT,
        t1.ABANDONMENT_DATE,
        t1.DISTRICT_NAME,
        t1.FORM_AT_TD,
        t1.FORMATION,
        t1.APIWELLNUMBER,
        t1.APIWELL_SIDETRACK,
        t1.ELEVATION,
        t1.LEASE_NAME,
        t1.NRI_GAS,
        t1.NRI_NGL,
        t1.NRI_OIL,
        t1.OPERATED_STATUS,
        t1.MEASURED_DEPTH_FEET,
        t1.TRUE_VERT_DEPTH_FEET,
        t1.WORKINGINTEREST,
        t1.RESERVE_ID,
        t1.ROUTENAME,
        t1.ROUTING_ID,
        t1.PROSPECT_AREA,
        t1.PROCOUNTID,
        t1.CURRENTPRODUCINGMETHOD,
        t1.HAS_AL,
        t1.COMPLETIONNAME,
        t1.MIDASCOMPLETION,
        t1.MAJORWELLTYPE,
        t1.ACQUISITION,
        t2.RLLNG_30DAY_BOE_AVG,
        t2.RLLNG_30DAY_DWNTME_AVG,
        t2.PAST_5DAY_BOE_AMT,
        t2.PAST_30DAY_BOE_AMT,
        t2.PAST_60DAY_BOE_AMT
    FROM tv_tdm_prcnt_wh_join t1
    LEFT JOIN tv_join_daily_pc_wh t2
        ON t1.UWI=t2.UWI
    """)

df_tdm_pc_daily_join.createOrReplaceTempView('tv_tdm_pc_daily_join')

df_tdm_pc_daily_wvw_join = spark.sql("""
    SELECT
        t1.*,
        t2.IDWELL,
        t2.WELLNAME                              AS WVW_WELL_NAME,
        TRIM(COALESCE(t2.WELLNAME,t1.WELL_NAME)) AS WVW_TDM_WELL_NAME,
        WELLCONFIG                               AS WVW_WELL_CONFIG
    FROM tv_tdm_pc_daily_join t1
    LEFT JOIN tv_wvw_wh t2
        ON t1.UWI = t2.WELLIDD
""")

df_tdm_pc_daily_wvw_join.createOrReplaceTempView('tv_tdm_pc_daily_wvw_join')

df_tdm_pc_daily_wvw_join = df_tdm_pc_daily_wvw_join.repartition(1)
df_tdm_pc_daily_wvw_join.write.parquet("s3://{0}/MRO_Analytics/Staging/WAR/tdm_wvw_prcnt_dly/".format(esp_dest_bucket), mode ="overwrite")

df_ener_asset = spark.read.parquet("s3://{0}/enersight/*/*ASSETS.parquet".format(hana_src_bukcet))
df_ener_asset = df_ener_asset.repartition(1)
df_ener_asset.createOrReplaceTempView('tv_ener_asset')

df_ener_user = spark.read.parquet("s3://{0}/enersight/*/*ASSETUSERDATA.parquet".format(hana_src_bukcet))
df_ener_user = df_ener_user.repartition(1)
df_ener_user.createOrReplaceTempView('tv_ener_user')

df_ener_act = spark.read.parquet("s3://{0}/enersight/*/*SCHEDULEACTIVITYRESULTS.parquet".format(hana_src_bukcet))
df_ener_act = df_ener_act.repartition(1)
df_ener_act = df_ener_act.select('ID','StartDate','WorkType','ZZVersionName','ZZLastUpdateDateTime')
df_ener_act.createOrReplaceTempView('tv_ener_act')

df_enersight_join = spark.sql("""
    SELECT
        CASE
            WHEN t2.MUWI = '0' THEN Null
            ELSE t2.MUWI
        END AS MUWI_ID,
        t1.FlowsTo                         AS FlowsTo_t1,
        t1.HasInflows                      AS HasInflows_t1,
        t1.HasOutflows                     AS HasOutflows_t1,
        t1.ID                              AS ID_t1,
        t1.LatitudeHeel                    AS LatitudeHeel_t1,
        t1.LatitudeSurface                 AS LatitudeSurface_t1,
        t1.LatitudeToe                     AS LatitudeToe_t1,
        t1.LongitudeHeel                   AS LongitudeHeel_t1,
        t1.LongitudeSurface                AS LongitudeSurface_t1,
        t1.LongitudeToe                    AS LongitudeToe_t1,
        t1.MonteCarloSeed                  AS MonteCarloSeed_t1,
        t1.Name                            AS Name_t1,
        t1.PadName                         AS PadName_t1,
        t1.PlanDataSetName                 AS PlanDataSetName_t1,
        t1.Project                         AS Project_t1,
        t1.ProjectKey                      AS ProjectKey_t1,
        t1.PropID                          AS PropID_t1,
        t1.Scenario                        AS Scenario_t1,
        CASE
            WHEN UPPER(TRIM(t1.SubType)) ='OILWELL' THEN 'OIL WELL'
            WHEN UPPER(TRIM(t1.SubType)) = 'GASWELL' THEN 'GAS WELL'
            ELSE UPPER(TRIM(t1.SubType))
        END                                AS SubType_t1,
        t1.TemplateWellName                AS TemplateWellName_t1,
        t1.Type                            AS Type_t1,
        t1.VersionName                     AS VersionName_t1,
        t1.ZZLastUpdateDateTime            AS ZZLastUpdateDateTime_t1,
        t1.ZZPlanDataSetID                 AS ZZPlanDataSetID_t1,
        t1.ZZPlanDataSetName               AS ZZPlanDataSetName_t1,
        t1.ZZVersionID                     AS ZZVersionID_t1,
        t1.ZZVersionName                   AS ZZVersionName_t1,
        t2.BAK_ID                          AS BAK_ID_t2,
        t2.ENGR_FIELD                      AS ENGR_FIELD_t2,
        t2.FIRST_PROD                      AS FIRST_PROD_t2,
        t2.ID                              AS ID_t2,
        t2.IS_BASE                         AS IS_BASE_t2,
        t2.Name                            AS Name_t2,
        t2.PropID                          AS PropID_t2,
        t2.RESERVE_ID                      AS RESERVE_ID_t2,
        t2.TC_AREA                         AS TC_AREA_t2,
        t2.TC_NUMBER                       AS TC_NUMBER_t2,
        t2.TYPE_CURVE_FAMILY               AS TYPE_CURVE_FAMILY_t2,
        t2.TYPE_CURVE_LL                   AS TYPE_CURVE_LL_t2,
        t2.ASSET_TEAM                      AS ASSET_TEAM_t2,
        t2.TYPE_CURVE_NAME                 AS TYPE_CURVE_NAME_t2,
        t2.WBS                             AS WBS_t2,
        t2.ASSET_ID                        AS ASSET_ID_t2,
        t2.ACRE_SPACING                    AS ACRE_SPACING_t2,
        t2.API_GRAVITY                     AS API_GRAVITY_t2,
        t2.API_NUMBER                      AS API_NUMBER_t2,
        t2.AREA_LOOKUP                     AS AREA_LOOKUP_t2,
        t2.ARTIFICIAL_LIFT_TYPE            AS ARTIFICIAL_LIFT_TYPE_t2,
        t2.BUCKET                          AS BUCKET_t2,
        t2.BUDGET_CAT2                     AS BUDGET_CAT2_t2,
        t2.CAPEX_COST_AREA                 AS CAPEX_COST_AREA_t2,
        t2.CENTRAL_FACILITY                AS CENTRAL_FACILITY_t2,
        t2.CENTRAL_FACILITY_INDEX          AS CENTRAL_FACILITY_INDEX_t2,
        t2.CMP02_COST                      AS CMP02_COST_t2,
        t2.CMP03A_COST                     AS CMP03A_COST_t2,
        t2.CMP03B_COST                     AS CMP03B_COST_t2,
        t2.COMPLETED_LL                    AS COMPLETED_LL_t2,
        t2.COUNTY                          AS COUNTY_t2,
        t2.DC_CAPEX_COST_AREA              AS DC_CAPEX_COST_AREA_t2,
        t2.DEVELOPMENT_TIER                AS DEVELOPMENT_TIER_t2,
        t2.DPL_VS_UNDPL                    AS DPL_VS_UNDPL_t2,
        t2.DRILL_ADD                       AS DRILL_ADD_t2,
        t2.DRILL_AREA                      AS DRILL_AREA_t2,
        t2.DRILL_MULTIPLIER                AS DRILL_MULTIPLIER_t2,
        t2.DRL_COST                        AS DRL_COST_t2,
        t2.DRL_DURATION                    AS DRL_DURATION_t2,
        t2.DSU                             AS DSU_t2,
        t2.DSU_PAD_GROUP                   AS DSU_PAD_GROUP_t2,
        t2.DT_GROUP                        AS DT_GROUP_t2,
        t2.EQP_COST                        AS EQP_COST_t2,
        t2.FACILITY                        AS FACILITY_t2,
        t2.FACILITY_ROUTE                  AS FACILITY_ROUTE_t2,
        t2.FIELD_NAME                      AS FIELD_NAME_t2,
        t2.FIELD_NUMBER                    AS FIELD_NUMBER_t2,
        t2.FORMATION                       AS FORMATION_t2,
        t2.HOLE_DIRECTION                  AS HOLE_DIRECTION_t2,
        t2.LCN_COST                        AS LCN_COST_t2,
        t2.MAJOR                           AS MAJOR_t2,
        t2.MEASURED_DEPTH                  AS MEASURED_DEPTH_t2,
        t2.MetaDataUD1                     AS MetaDataUD1_t2,
        t2.MetaDataUD2                     AS MetaDataUD2_t2,
        t2.NUM_WELLS_PAD                   AS NUM_WELLS_PAD_t2,
        t2.OIL_VS_CND                      AS OIL_VS_CND_t2,
        t2.OPERATIONAL_AREA                AS OPERATIONAL_AREA_t2,
        t2.OPERATOR                        AS OPERATOR_t2,
        t2.OPEX_COST_AREA                  AS OPEX_COST_AREA_t2,
        t2.PAD_DESIGNATION                 AS PAD_DESIGNATION_t2,
        t2.PAD_STAGES                      AS PAD_STAGES_t2,
        t2.PERFORMANCE_AREA                AS PERFORMANCE_AREA_t2,
        t2.PROJECT_TYPE                    AS PROJECT_TYPE_t2,
        t2.PROSPECT                        AS PROSPECT_t2,
        t2.PROSPECT_FACILITY               AS PROSPECT_FACILITY_t2,
        t2.PROTOCOL                        AS PROTOCOL_t2,
        t2.PUD_EXPIRE                      AS PUD_EXPIRE_t2,
        t2.RANGE                           AS RANGE_t2,
        t2.RES_CAT                         AS RES_CAT_t2,
        t2.ROUTE                           AS ROUTE_t2,
        t2.RPTAREA                         AS RPTAREA_t2,
        t2.SECTION                         AS SECTION_t2,
        TO_DATE(CAST(t2.SPUD_DT as timestamp)) AS SPUD_DT_t2,
        t2.SPUD_YEAR                       AS SPUD_YEAR_t2,
        t2.STAGE_SPACING                   AS STAGE_SPACING_t2,
        t2.STAGES                          AS STAGES_t2,
        t2.STATE                           AS STATE_t2,
        t2.SUBSURFACE_AREA                 AS SUBSURFACE_AREA_t2,
        t2.TAX_FLAG                        AS TAX_FLAG_t2,
        t2.TOWNSHIP                        AS TOWNSHIP_t2,
        t2.TRUNK_LINE                      AS TRUNK_LINE_t2,
        t2.TRUNKLINE_GAS                   AS TRUNKLINE_GAS_t2,
        t2.TRUNKLINE_OIL                   AS TRUNKLINE_OIL_t2,
        t2.TRUNKLINE_WATER                 AS TRUNKLINE_WATER_t2,
        t2.TVD                             AS TVD_t2,
        t2.WATER_SOURCE                    AS WATER_SOURCE_t2,
        t3.ID                              AS ID_t3,
        from_unixtime(unix_timestamp(t3.StartDate,'MM/dd/yyyy hh:mm:ss a'),'yyyy-MM-dd') AS StartDate,
        t3.WorkType                        AS WorkType,
        t3.ZZVersionName                   AS ZZVersionName_t3,
        t3.ZZLastUpdateDateTime            AS ZZLastUpdateDateTime_t3
    FROM tv_ener_asset t1
    LEFT JOIN tv_ener_user t2
        on t1.id=t2.id
    LEFT JOIN tv_ener_act t3
        on t1.id=t3.id
    """)

#Filtering for null and for MUWI ID's that have a length equal to eight characters and are not null
df_enersight_join = df_enersight_join.filter(df_enersight_join.MUWI_ID.isNotNull())
df_enersight_join = df_enersight_join.filter(length(df_enersight_join.MUWI_ID)==8)
df_enersight_join = df_enersight_join.withColumn("HAS_AL",lit(False))
df_enersight_join = df_enersight_join.withColumn("ACTIVE_IND",lit(False))
#Create temp view
df_enersight_join.createOrReplaceTempView('tv_enersight_join')

# Dedup by Muwi, Worktype, taking max start date
df_enersight_join = spark.sql("""
    SELECT
        *
    FROM
        (SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY MUWI_ID, WORKTYPE ORDER BY STARTDATE DESC) as row_num
        FROM tv_enersight_join)A
    WHERE A.row_num=1
        """)

#Pivoting our table so that we can get all worktypes into columns, thus making only one record per MUWI.
df_enersight_work = df_enersight_join.select('MUWI_ID','WORKTYPE','STARTDATE').groupBy('MUWI_ID').pivot('WORKTYPE').agg(max_('STARTDATE'))

#making all columns upper
df_enersight_work = df_enersight_work.toDF(*[c.upper() for c in df_enersight_work.columns])

#Take columns we care about  from df_enersight_work
df_enersight_work = df_enersight_work.select('MUWI_ID','COMPLETION','DRILLING','FIRST PRODUCTION')
df_enersight_work.createOrReplaceTempView('tv_enersight_work')

#Dropping duplicates, based on a key of MUWI_ID
df_enersight_join = df_enersight_join.dropDuplicates(subset = ['MUWI_ID']).select(
    'MUWI_ID','API_NUMBER_t2','Name_t1','SubType_t1','ASSET_TEAM_t2','FIELD_NAME_t2',
    'PadName_t1','STATE_t2','COUNTY_t2','LatitudeSurface_t1','LongitudeSurface_t1',
    'ARTIFICIAL_LIFT_TYPE_t2','CENTRAL_FACILITY_t2','FACILITY_t2','FACILITY_ROUTE_t2',
    'FORMATION_t2', 'MEASURED_DEPTH_t2','OPERATOR_t2','PROSPECT_t2',
    'ROUTE_t2','SPUD_DT_t2','TVD_t2','HAS_AL','ACTIVE_IND')

#Renaming column names to match the data model seen in the TDM and Prcnt data
df_enersight_join = df_enersight_join.selectExpr('MUWI_ID as MUWI','API_NUMBER_t2 as API_WELL_NMBR','Name_t1 as WELL_NM',
                            'SubType_t1 as WELL_TYPE','ASSET_TEAM_t2 as ASSET_NM','FIELD_NAME_t2 as FIELD_NM',
                            'PadName_t1 as PAD_NM','STATE_t2 as STATE_NM','COUNTY_t2 as COUNTY_NM',
                            'LatitudeSurface_t1 as GEO_LAT','LongitudeSurface_t1 as GEO_LONG',
                            'ARTIFICIAL_LIFT_TYPE_t2 as ARTIFICIAL_LIFT_TYPE','CENTRAL_FACILITY_t2 as CENTRAL_FACILITY_NM',
                            'FACILITY_t2 as FACILITY_NM','FACILITY_ROUTE_t2 as FACILITY_ROUTE','FORMATION_t2 as FORMATION',
                            'MEASURED_DEPTH_t2 as MEASURED_DEPTH_FEET','OPERATOR_t2 as OPERATOR_NM','PROSPECT_t2 as PROSPECT_NM',
                            'ROUTE_t2 as ROUTE_NM','SPUD_DT_t2 as SPUD_DT','TVD_t2 as TRUE_VERT_DEPTH_FEET','HAS_AL as HAS_AL','ACTIVE_IND')

#Do join  betweetn enersight tables and the enersight work table
df_enersight = df_enersight_work.join(df_enersight_join, df_enersight_work.MUWI_ID == df_enersight_join.MUWI,how='left')

#Rename the production date field
df_enersight = df_enersight.withColumnRenamed("FIRST PRODUCTION", "FIRST_PRODUCTION")

#adding well life cycle, anything coming from  enersight is considered planned.
df_enersight = df_enersight.withColumn('WELL_LIFE_CYCLE',lit('Growth-POD'))
df_enersight.createOrReplaceTempView('tv_enersight')

df_enersight = df_enersight.repartition(1)
df_enersight.write.parquet("s3://{0}/MRO_Analytics/Staging/WAR/enersight/".format(esp_dest_bucket), mode ="overwrite")

df_war_pre = spark.sql("""
    SELECT
        COALESCE(t1.ACTIVE_IND,t2.ACTIVE_IND)                       AS ACTIVE_IND,
        COALESCE(t1.WVW_TDM_WELL_NAME, t2.WELL_NM)                  AS WELL_NM,
        COALESCE(t1.UWI, t2.MUWI_ID)                                AS MUWI_ID,
        COALESCE(t1.APIWELLNUMBER, t2.API_WELL_NMBR)                AS API_WELL_NMBR,
        t1.APIWELL_SIDETRACK                                        AS API_WELL_SDTRCK_NMBR,
        IDWELL,
        t1.PROCOUNTID                                               AS PROCOUNT_ID,
        COALESCE(t1.OPERATOR_NAME, t2.OPERATOR_NM)                  AS OPERATOR_NM,
        t1.OPERATED_STATUS,
        t1.CURRENTPRODUCINGSTATUS                                   AS CURRENT_PRDCNG_STATUS,
        COALESCE(t1.WELL_LIFE_CYCLE, t2.WELL_LIFE_CYCLE)            AS WELL_LIFE_CYCLE,
        COALESCE(t1.ASSET, t2.ASSET_NM)                             AS ASSET_NM,
        t1.SUB_ASSET                                                AS SUB_ASSET_NM,
        COALESCE(t1.field_name,t2.FIELD_NM)                         AS FIELD_NM,
        COALESCE(t1.PAD,t2.PAD_NM)                                  AS PAD_NM,
        t1.BASIN_NAME                                               AS BASIN_NM,
        t1.RESERVOIR                                                AS RESERVOIR_NM,
        t1.AREA                                                     AS AREA_NM,
        COALESCE(t1.FACILITY ,t2.FACILITY_NM)                       AS FACILITY_NM,
        COALESCE(LAT_SURFACE_RAW,GEO_LAT)                           AS GEO_LAT,
        COALESCE(LONG_SURFACE_RAW,GEO_LONG)                         AS GEO_LONG,
        t1.COUNTRY                                                  AS COUNTRY_NM,
        COALESCE(t1.COUNTY,t2.COUNTY_NM)                            AS COUNTY_NM,
        COALESCE(t1.STATE, t2.STATE_NM)                             AS STATE_NM, --had trim
        COALESCE(t1.FINAL_DRILL_DATE,t2.DRILLING)                   AS FINAL_DRILL_DT,
        t1.RELEASE_DATE_OF_RIG                                      AS RELEASE_OF_RIG_DT,
        COALESCE(t1.COMPLETION_DATE,COMPLETION)                     AS COMPLETION_DT,
        COALESCE(t1.SPUD_DATE,t2.SPUD_DT)                           AS SPUD_DT,
        COALESCE( t1.FIRST_PROD_DATE, from_unixtime(unix_timestamp(
        t2.FIRST_PRODUCTION,'dd MMM yyyy'),'yyyy-MM-dd'))           AS FIRST_PROD_DT,
        t1.LAST_PROD_DT                                             AS LAST_PROD_DT,
        t1.ABANDONMENT_DATE                                         AS ABANDONMENT_DT,
        t1.DISTRICT_NAME                                            AS DISTRICT_NM,
        t1.FORM_AT_TD,
        COALESCE(t1.FORMATION,t2.FORMATION)                         AS FORMATION,
        t1.ELEVATION                                                AS ELEVATION_FEET,
        t1.LEASE_NAME                                               AS LEASE_NM,
        t1.NRI_GAS                                                  AS NRI_GAS_RT,
        t1.NRI_NGL                                                  AS NRI_NGL_RT,
        t1.NRI_OIL                                                  AS NRI_OIL_RT,
        COALESCE(t1.MEASURED_DEPTH_FEET,t2.MEASURED_DEPTH_FEET)     AS MEASURED_DEPTH_FEET,
        COALESCE(t1.TRUE_VERT_DEPTH_FEET,t2.TRUE_VERT_DEPTH_FEET)   AS VERT_DEPTH_FEET,
        t1.WORKINGINTEREST                                          AS WORKINGINTEREST,
        t1.RESERVE_ID                                               AS RESERVE_ID,
        COALESCE(t1.ROUTENAME, t2.ROUTE_NM)                         AS ROUTE_NM,
        t1.ROUTING_ID,
        COALESCE(t1.PROSPECT_AREA, t2.PROSPECT_NM)                  AS PROSPECT_AREA_NM,
        COALESCE(t1.CURRENTPRODUCINGMETHOD,ARTIFICIAL_LIFT_TYPE)    AS CURRENT_PRDCNG_MTHD,
        COALESCE(t1.HAS_AL,t2.HAS_AL)                               AS HAS_AL,
        t1.COMPLETIONNAME                                           AS COMPLETION_NM,
        t1.MIDASCOMPLETION                                          AS MUWI_COMPLETION_ID,
        COALESCE(t1.MAJORWELLTYPE,t2.WELL_TYPE)                     AS WELL_TYPE,
        t1.WVW_WELL_CONFIG                                          AS WELL_CONFIG,
        t1.ACQUISITION                                              AS ACQUSITION_COMP_NM,
        t1.RLLNG_30DAY_BOE_AVG                                      AS RLLNG_30DAY_BOE_AVG ,
        t1.RLLNG_30DAY_DWNTME_AVG                                   AS RLLNG_30DAY_DWNTME_AVG,
        t1.PAST_5DAY_BOE_AMT                                        AS PAST_5DAY_BOE_AMT,
        t1.PAST_30DAY_BOE_AMT                                       AS PAST_30DAY_BOE_AMT,
        t1.PAST_60DAY_BOE_AMT                                       AS PAST_60DAY_BOE_AMT,
        CURRENT_TIMESTAMP()                                         AS LOAD_TIMESTAMP
    FROM tv_tdm_pc_daily_wvw_join t1
    FULL OUTER JOIN tv_enersight t2
        on t1.uwi=t2.muwi
""")

df_war_pre.createOrReplaceTempView('tv_war')

#Forcing bad asset names values as null
df_war_pre = df_war_pre.withColumn('ASSET_NM',when(col('ASSET_NM')!='PDM USE ONLY', col('ASSET_NM')))


#State/State abbreviation lookup table
#Reading State/State abbreviation lookup table
df_lkp_state = spark.read.csv(
                    "s3://{0}/MRO_Analytics/DataSources/Lookup_Files/US_State_Abbr.csv".format(esp_dest_bucket),
                    sep = ",",
                    mode = "DROPMALFORMED",
                    header = True
)
df_lkp_state = df_lkp_state.withColumn("State", trim(col("State")))

#joining df_war to the lookup table
df_war_final = df_war_pre.join(df_lkp_state, on=(df_war_pre.STATE_NM==df_lkp_state.State) , how='left' )
df_war_final = df_war_final.withColumn('STATE_NM',coalesce(df_war_final['Abbr'],df_war_final['STATE_NM']))
df_war_final = df_war_final.drop('Country', 'Abbr', 'State')
df_war_final = df_war_final.repartition(1)

#Write to analytics outbound s3 directory - so that data science team can leverage file
df_war_final.write.parquet("s3://{0}/MRO_Analytics/DataOutputs/Analytics_Outbound/WAR/".format(esp_dest_bucket), mode = 'overwrite')

#Write parquet file to snowflake outbound so it can be used
df_war_final.write.parquet(
    "s3://{0}/MRO_Analytics/DataOutputs/Snowflake_Outbound/WAR/processed_date={1}/".format(esp_dest_bucket, load_date),
    mode = 'overwrite'
)
