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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from datetime import date
from datetime import timedelta  
Load_date = date.today() - timedelta(days=1)

load_date_formated=Load_date.strftime("%Y-%m-%d")

wellviewsourcepath = "mro-wvp-dev-bucket01/WELLV10P/WV100CALCUNITS_US"
procountsourcepath = "mro-dl-dev-s3-transformed/processdata"
#pcwellheadersourcepath= "mro-hanap-dev-dl-bucket01"
espsourcepath      = "mro-dev-esp-analytics-bucket/MRO_Analytics/DataSources/ESP_Processed"

TUBING_FILEPATH="WVTUB/processed_date="+load_date_formated
PROBLEM_FILEPATH="WVPROBLEM/processed_date="+load_date_formated
PROBLEMDETAILS_FILEPATH="WVPROBLEMDETAIL/processed_date="+load_date_formated
WVWELLHEADER_FILEPATH="WVWELLHEADER/processed_date="+load_date_formated
#PCWELLHEADER_FILEPATH="proreport/processed_date="+load_date_formated"
PCWELLHEADER_FILEPATH="pct-wellheader-fullload"
PCDAILYWELLDATA_FILE="pct-wellproductionvolumes-fullload"

tubingsourcepath="s3://"+wellviewsourcepath+"/"+TUBING_FILEPATH+"/"
problemsourcepath="s3://"+wellviewsourcepath+"/"+PROBLEM_FILEPATH+"/"
problemdetailsourcepath="s3://"+wellviewsourcepath+"/"+PROBLEMDETAILS_FILEPATH+"/"
wvwellheadersourcepath="s3://"+wellviewsourcepath+"/"+WVWELLHEADER_FILEPATH+"/"

pcwellheaderlsourcepath="s3://"+procountsourcepath+"/"+PCWELLHEADER_FILEPATH+"/"
pcdailywelldatasourcepath="s3://"+procountsourcepath+"/"+PCDAILYWELLDATA_FILE+"/"

df_tubing = spark.read.parquet(tubingsourcepath)
dfp_tubing= df_tubing.repartition(1)
df_tubing.createOrReplaceTempView('WV_TUBING')

df_tubing_1=spark.sql("""
SELECT * FROM 
(SELECT TO_DATE(WT.DTTMRUN) DTTMRUN_ASDATE,
        TO_DATE(WT.DTTMPULL) DTTMPULL_ASDATE,
                                WT.*
                                FROM WV_TUBING WT )
                                --WHERE DTTMPULL_ASDATE IS NOT NULL AND DTTMRUN_ASDATE IS NOT NULL
                                """)

# df_tubing_1[['DTTMRUN_ASDATE','DTTMPULL_ASDATE']].show(5)
df_tubing_1.createOrReplaceTempView('WV_TUBING_1')
df_tubing_data = spark.sql("""select 
                                wvt.IDREC AS TUBING_IDREC,
                                wvt.IDWELL,
                                wvt.COMPTUBDIMCALC,
                                wvt.COMPTUBDIMSZODNOMCALC,
                                wvt.DEPTHBTM,
                                wvt.DEPTHBTMLINKCALC,
                                wvt.DEPTHCUTPULL,
                                wvt.DEPTHTOPLINKCALC,wvt.DTTMPULL,wvt.DTTMRUN,
                                wvt.DTTMPULL_ASDATE as DTTMPULL_ASDATE,
                                wvt.DTTMRUN_ASDATE as DTTMRUN_ASDATE,
                                wvt.GRADECALC,
                                wvt.LENGTHCALC,
                                wvt.PROPOSEDCUTPULL,
                                wvt.PROPOSEDPULL,
                                wvt.PROPOSEDRUN,
                                wvt.PULLREASON,
                                wvt.PULLREASONDETAIL,
                                wvt.STICKUPKBCALC,
                                wvt.STRINGWTDOWN,
                                wvt.STRINGWTROTATING,
                                wvt.STRINGWTUP,
                                wvt.SZDRIFTMINCALC,
                                wvt.SZIDNOMCOMPMINCALC,
                                wvt.SZIDNOMMINCALC,
                                wvt.SZODNOMCOMPMAXCALC,
                                wvt.SZODNOMMAXCALC,
                                wvt.TENSION,
                                wvt.TOTALSTRETCHSUMCALC,
                                wvt.WTPERLENGTHCALC,
                                wvt.CONNTHRDTOPCALC AS CONNTHRDTOP,
                                wvt.DEPTHTVDBTMCALC AS DEPTHTVDBTMCALC ,
                                wvt.DEPTHTOPCALC AS DEPTHTOPCALC
                                from WV_TUBING_1 wvt""")

df_problem = spark.read.parquet(problemsourcepath)
df_problem = df_problem.repartition(1)
df_problem.createOrReplaceTempView('WV_PROBLEM')
df_problem_1 = spark.sql("""
                         SELECT * FROM
                            (SELECT TO_DATE(PBM.DTTMSTART) DTTMSTART_ASDATE,
                                    TO_DATE(PBM.DTTMEND) as DTTMEND_ASDATE,
                                                    pbm.*
                                                    FROM WV_PROBLEM PBM )
                                                    WHERE DTTMSTART_ASDATE IS NOT NULL""")

df_problem_1 = df_problem_1.withColumn("PBM_CAUSE", regexp_replace(df_problem_1['CAUSE'], "\\,\n", ""))
df_problem_1 = df_problem_1.withColumn("PBM_CAUSECOM", regexp_replace(df_problem_1['CAUSECOM'], "\\,\n", ""))
df_problem_1 = df_problem_1.withColumn("PBM_CAUSEDETAIL", regexp_replace(df_problem_1['CAUSEDETAIL'], "\\,\n", ""))
df_problem_1 = df_problem_1.withColumn("PBM_COMMENTS", regexp_replace(df_problem_1['COM'], "\\,\n", ""))
df_problem_1 = df_problem_1.withColumn("PROBLEM_DESCRIPTION", regexp_replace(df_problem_1['DES'], "\\,\n", ""))

df_problem_1.createOrReplaceTempView('WV_PROBLEM_1')
df_problem_data = spark.sql("""
                            SELECT 
                                  PBM.IDWELL AS PBM_IDWELL, 
                                  PBM.IDREC AS PBM_IDREC,
                                  PBM.ACTIONTAKEN, 
                                  PBM_CAUSE, 
                                  PBM_CAUSECOM,
                                  PBM_CAUSEDETAIL,
                                  PBM_COMMENTS,
                                  PROBLEM_DESCRIPTION,
                                  PBM.DTTMACTION,
                                  PBM.DTTMEND, 
                                  PBM.DTTMSTART_ASDATE,
                                  PBM.DURACTIONTOSTARTCALC,
                                  PBM.DURENDTOACTIONCALC, 
                                  PBM.DURENDTOSTARTCALC, 
                                  PBM.DURJOBSTARTTOSTARTCALC,
                                  PBM.ESTCOST, 
                                  PBM.ESTRESERVELOSSCOND, 
                                  PBM.ESTRESERVELOSSGAS,
                                  PBM.ESTRESERVELOSSOIL,
                                  PBM.ESTRESERVELOSSWATER, 
                                  PBM.FAILURESYMPTOM,
                                  PBM.FAILURESYSTEM, 
                                  PBM.PERFORMANCEAFFECT, 
                                  PBM.PRIORITY
                                      FROM 
                                      WV_PROBLEM_1 pbm
                                         """)

df_pbmdetails_1 = spark.read.parquet(problemdetailsourcepath)
df_pbmdetails_1 = df_pbmdetails_1.repartition(1)
df_pbmdetails_1.createOrReplaceTempView('WV_PROBLEM_DTLS_1')
df_pbmdetails= spark.sql("""SELECT 
								 IDREC AS PBMDTLS_IDREC,
                                 TRIM(IDRECPARENT) AS PBMDTLS_IDRECPARENT,
								 TRIM(IDWELL) AS PBMDTLS_IDWELL, 
								 REPLACE(AREAFAILED,',','') AS AREAFAILED, 
								 CATEGORY, 
								 COMPSUBTYPCALC,
								 CONDITIONPULLCALC,
								 CONDITIONRUNCALC, 
								 CONNECTCALC,
								 DEPTH,
								 DEPTHBTMCALC, 
								 DEPTHTVDTOPCALC, 
								 DESCOMPCALC,
								 DTTMPULLCALC,
								 DTTMRUNCALC, 
								 DUREQUIPCALC, 
								 DUREQUIPINSERVICE,
								 DURRUN, 
								 DURSTARTTORUNCALC,
								 HOURSSTARTCALC,
								 IDRECFAILEDITEM,
								 IDRECFAILEDITEMTK,
								 INCLBTMCALC, 
								 INCLMAXCALC, 
								 INCLTOPCALC, 
								 MAKECALC,
								 MATERIALCALC, 
								 SNCALC, 
								 SUBITEM, 
								 SZIDNOMCALC, 
								 SZODMAXCALC,
								 SZODNOMCALC
								 FROM 
								 WV_PROBLEM_DTLS_1""")

df_problem_data.createOrReplaceTempView('WV_PROBLEM_DATA')
df_tubing_data.createOrReplaceTempView('WV_TUBING_DATA')
df_wvwellheader = spark.read.parquet(wvwellheadersourcepath)
df_wvwellheader = df_wvwellheader.repartition(1)
df_wvwellheader = df_wvwellheader[['WELLIDD','IDWELL','WELLNAME']]
df_wvwellheader = df_wvwellheader.filter(df_wvwellheader.WELLIDD.isNotNull() & df_wvwellheader.IDWELL.isNotNull())
df_wvwellheader.createOrReplaceTempView('wellheader_data')

df_wvwellheader=spark.sql("""select DISTINCT WELLIDD,IDWELL,REPLACE(WELLNAME,',','') AS WELLNAME  from wellheader_data""")
df_pcwellheader = spark.read.parquet(pcwellheaderlsourcepath)

df_pcwellheader = df_pcwellheader.repartition(1)
print (pcwellheaderlsourcepath)

df_pcwellheader = df_pcwellheader[['MIDASWELL','PROCOUNTID']]
df_pcwellheader = df_pcwellheader.filter(df_pcwellheader.MIDASWELL.isNotNull() & df_pcwellheader.PROCOUNTID.isNotNull())
df_pcwellheader = df_pcwellheader[['MIDASWELL','PROCOUNTID']]
df_pcwellheader = df_pcwellheader.withColumn('MIDASWELL_FIRST8',split('MIDASWELL','-')[0])
df_pcwellheader = df_pcwellheader[['MIDASWELL_FIRST8','PROCOUNTID']]
df_pcwellheader = df_pcwellheader.dropDuplicates()
df_pcwellheader = df_pcwellheader[['MIDASWELL_FIRST8','PROCOUNTID']]
df_pcwellheader = df_pcwellheader.dropDuplicates()

import pyspark.sql.types as typ

df_pcwellheader = df_pcwellheader.select(df_pcwellheader["MIDASWELL_FIRST8"].cast(typ.StringType())
      ,df_pcwellheader["PROCOUNTID"].cast(typ.IntegerType())
    )

df_pcwellheader[['MIDASWELL_FIRST8','PROCOUNTID']].show(5)
df_pcdailywelldata = spark.read.parquet(pcdailywelldatasourcepath)
df_pcdailywelldata = df_pcdailywelldata.repartition(1)

df_pcdailywelldata.createOrReplaceTempView('PC_DAILYWELLDATA')
df_pcdailywelldata_1 = spark.sql("""SELECT
								 PROCOUNTID,
								 TO_DATE(PRODUCTIONDATE) AS PRODUCTIONDATE_ASDATE,
								 ACTIVECHECKBOX,
								 INDATERANGE,
								 ACTIVE,
								 ACTIVERECORD,
								 ALLOCATEDOILPRODUCTION,
								 ALLOCATEDGASPRODUCTION,
								 ALLOCATEDWATERPRODUCTION,
								 CASINGPRESSURE,
								 INTERCASINGPRESSURE,
								 SURFACECASINGPRESSURE,
								 LINERCASINGPRESSURE,
								 OTHERCASINGPRESSURE,
								 TUBINGPRESSURE,
								 SECONDTUBINGPRESSURE,
								 SHUTINTUBINGPRESSURE,
								 STROKES,
								 CHOKESIZE,
								 ALLOCATEDGASINJECTEDVOL,
								 ALLOCWATERINJECTEDVOL,
								 GASSFREEFLUID,
								 FLUIDLEVEL,
								 EFFECTIVEPRODUCINGSTATUS,
								 EFFECTIVEPRODUCINGMETHOD,
								 GASSALES,
								 OILSALES,
								 GASLIFT,
								 MINUTESON,
								 DOWNTIMEHOURS,
								 DOWNTIMEREASON,
								 DOWNTIMETYPE,
								 DOWNTIMECODE,
								 DTPARENT,
								 DTTYPE,
								 GROUPNAME,
								 GCMENAME,
								 CAUSE,
								 POTENTIALBOE,
								 ALLOCATEDBOE,
								 POTENTIALOIL,
								 POTENTIALGAS,
								 DEFERREDBOE,
								 DEFERREDOIL,
								 DEFERREDGAS,
								 DEFERREDPERCENTBOE,
								 REPLACE(DAILYCOMMENTS,',',':') AS DAILYCOMMENTS,
								 GASENTERED,
								 OILENTERED,
								 WATERENTERED,
								 GASLIFTENTERED,
								 LASTMODIFIEDDATE,
								 INJECTIONPRESSURE,
								 ASSETTEAM,
								 NGLYIELDFACTOR,
								 GASSHRINKFACTOR,
								 NGLESTIMATEVOL,
								 GASRESIDUEVOL,
								 FLAREVOL,
								 FLAREVOL_LP,
								 FLAREVOL_HP,
								 FUELVOL,
								 NRIOIL,
								 NRIGAS,
								 NRINGL,
								 WRKINT,
								 NGLESTIMATEVOL_SALES,
								 GASRESIDUEVOL_SALES,
								 NETOILPROD,
								 NETRESIDUEGAS_SALES,
								 NETNGL_SALES,
								 NETBOE,
								 NGLSKIDSALESMCF,
								 NGLSKIDSALESBBL,
								 ALLOCATEDNGLPRODUCTION,
								 NGLADJUSTMENTFACTOR,
								 GASADJUSTMENTFACTOR,
								 BHPFLOWING,
								 BHTEMPERATURE,
								 USERNUMBER6
									FROM 
									PC_DAILYWELLDATA
                                    WHERE PRODUCTIONDATE>='2016-01-01'

""")

columns_to_drop = ['PRODUCTIONDATE',
 'DATEyyy',
 'DATEdd',
 'DATEmm'
 ]

df_pcdailywelldata_1 = df_pcdailywelldata_1.drop(*columns_to_drop)
df_pcdailywelldata_1.count()
df_pcdailywelldata_1.createOrReplaceTempView('PCDAILYWELL_DATA_1')
df_wvwellheader.createOrReplaceTempView('WVWELLHEADER_DATA')
df_pcwellheader.createOrReplaceTempView('PCWELLHEADER_DATA')

df_Production_welldata = spark.sql("""
SELECT WVWH.IDWELL as WVWH_IDWELL, 
WVWH.WELLIDD as WVWH_WELLIDD,WVWH.WELLNAME,PCWH.MIDASWELL_FIRST8 AS PROCOUNT_MIDASWELL_FIRST8,PCDW.PROCOUNTID AS PCWH_PROCOUNTID,
    PCDW.*
    FROM
        PCDAILYWELL_DATA_1 PCDW 
		LEFT OUTER JOIN 
	    PCWELLHEADER_DATA PCWH ON 
		trim(PCDW.PROCOUNTID)=trim(PCWH.PROCOUNTID)
		LEFT OUTER JOIN 
		WVWELLHEADER_DATA  WVWH  ON  
		TRIM(WVWH.WELLIDD)=TRIM(PCWH.MIDASWELL_FIRST8)""")

df_esp = spark.read.parquet("s3://mro-dev-esp-analytics-bucket/MRO_Analytics/DataSources/ESP_Processed/Inactive/")
df_esp = df_esp.repartition(1)
df_esp = df_esp.withColumnRenamed("WELLNAME", "ESP_WELLNAME")

df_esp.createOrReplaceTempView('ESP_TAGS_VIEW_1')
df_esp_1 = spark.sql("""SELECT
                         ESP_WELLNAME, 
                         UWID, 
                         TO_DATE(PRODUCTION_DATE) AS PRODUCTION_DATE,
                         YesterdayRuntimeHours,
                         YesterdayStarts, 
                         ABVolts_median,
                         BCVolts_median, 
                         CAVolts_median, 
                         CasingPressure_median, 
                         CurrentImbalance_median,
                         DownholeAmpA_median, 
                         DownholeAmpB_median, 
                         DownholeAmpC_median,
                         IntakePressure_median, 
                         IntakeTemperature_median,
                         MotorTemperature_median, 
                         OutputFrequency_median,
                         PhaseACurrent_median, 
                         PhaseBCurrent_median,
                         PhaseCCurrent_median, 
                         PowerConsumption_median, 
                         PowerFactor_median, 
                         TubingPressure_median, 
                         VoltageImbalance_median, 
                         ABVolts_mean, 
                         BCVolts_mean, 
                         CAVolts_mean, 
                         CasingPressure_mean,
                         CurrentImbalance_mean, 
                         DownholeAmpA_mean,
                         DownholeAmpB_mean, 
                         DownholeAmpC_mean, 
                         IntakePressure_mean, 
                         IntakeTemperature_mean, 
                         MotorTemperature_mean, 
                         OutputFrequency_mean, 
                         PhaseACurrent_mean, 
                         PhaseBCurrent_mean, 
                         PhaseCCurrent_mean,
                         PowerConsumption_mean, 
                         PowerFactor_mean,
                         TubingPressure_mean, 
                         VoltageImbalance_mean, 
                         ABVolts_IQR,
                         BCVolts_IQR,
                         CAVolts_IQR, 
                         CasingPressure_IQR,
                         CurrentImbalance_IQR, 
                         DownholeAmpA_IQR, 
                         DownholeAmpB_IQR, 
                         DownholeAmpC_IQR, 
                         IntakePressure_IQR, 
                         IntakeTemperature_IQR,
                         MotorTemperature_IQR,
                         OutputFrequency_IQR, 
                         PhaseACurrent_IQR, 
                         PhaseBCurrent_IQR, 
                         PhaseCCurrent_IQR, 
                         PowerConsumption_IQR, 
                         PowerFactor_IQR, 
                         TubingPressure_IQR, 
                         VoltageImbalance_IQR,
                         ABVolts_range, 
                         BCVolts_range, 
                         CAVolts_range, 
                         CasingPressure_range,
                         CurrentImbalance_range,
                         DownholeAmpA_range, 
                         DownholeAmpB_range, 
                         DownholeAmpC_range, 
                         IntakePressure_range, 
                         IntakeTemperature_range, 
                         MotorTemperature_range, 
                         OutputFrequency_range, 
                         PhaseACurrent_range, 
                         PhaseBCurrent_range, 
                         PhaseCCurrent_range, 
                         PowerConsumption_range, 
                         PowerFactor_range, 
                         --TimeStamp_range, 
                         TubingPressure_range, 
                         VoltageImbalance_range 
                         --production_month
                            FROM ESP_TAGS_VIEW_1""")

df_esp_1.createOrReplaceTempView('ESP_TAGS_VIEW')
df_Production_welldata.createOrReplaceTempView('PRODUCTION_WELLDATA')
df_problem_data.createOrReplaceTempView('WV_PROBLEM_DATA')
df_pbmdetails.createOrReplaceTempView('WV_PROBLEMDTLS_DATA')
df_tubing_data.createOrReplaceTempView('WV_TUBING_DATA')

df_EAR = spark.sql("""
SELECT CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP ,PWD.*,ESP.*,TB.*,PB.*,PBMDTLS.*
    FROM
         PRODUCTION_WELLDATA PWD LEFT OUTER JOIN ESP_TAGS_VIEW ESP ON 
		 UPPER(TRIM(ESP.ESP_WELLNAME))=UPPER(TRIM(PWD.WELLNAME)) AND PWD.PRODUCTIONDATE_ASDATE=ESP.PRODUCTION_DATE
         LEFT OUTER JOIN WV_TUBING_DATA TB ON
         PWD.WVWH_IDWELL=TB.IDWELL AND PWD.PRODUCTIONDATE_ASDATE  between TB.DTTMRUN_ASDATE and nvl(TB.DTTMPULL_ASDATE,CURRENT_DATE())
         LEFT OUTER JOIN WV_PROBLEM_DATA PB
         ON TB.IDWELL=PB.PBM_IDWELL AND  PWD.PRODUCTIONDATE_ASDATE=  PB.DTTMSTART_ASDATE
         LEFT OUTER JOIN WV_PROBLEMDTLS_DATA PBMDTLS
         ON PB.PBM_IDREC=PBMDTLS.PBMDTLS_IDRECPARENT AND PB.PBM_IDWELL=PBMDTLS.PBMDTLS_IDWELL 
         """)

df_EAr = df_EAR.toDF(*[c.upper() for c in df_EAR.columns])

columns_to_drop = ['WVWH_WELLIDD',
 'UWID',
 'PROCOUNTID',
 'PRODUCTION_DATE',
 'ESP_WELLNAME',
 'PBM_IDWELL',
 'IDWELL','DTTMPULL','DTTMRUN','DTTMSTART','PBMDTLS_IDRECPARENT','PBMDTLS_IDWELL'
                ]
df_EAR = df_EAR.drop(*columns_to_drop)
df_EAR = df_EAR.withColumnRenamed('PROCOUNT_MIDASWELL_FIRST8','MUWIID')
df_EAR = df_EAR.withColumnRenamed('PCWH_PROCOUNTID','PROCOUNTID')
df_EAR = df_EAR.withColumnRenamed('PRODUCTIONDATE_ASDATE','PRODUCTIONDATE')
df_EAR = df_EAR.withColumnRenamed('WVWH_IDWELL','IDWELL')

df_EAR = df_EAR.withColumnRenamed('DTTMSTART_ASDATE','DTTMSTART')
df_EAR = df_EAR.withColumnRenamed('DTTMRUN_ASDATE','DTTMRUN')
df_EAR = df_EAR.withColumnRenamed('DTTMPULL_ASDATE','DTTMPULL')
df_EAr = df_EAR.dropDuplicates()

df_EAr = df_EAR.repartition(1)
df_EAR = df_EAR.withColumn('LOAD_DATE', substring('LOAD_TIMESTAMP', 1, 10))  
df_EAR_dynamicFrame = DynamicFrame.fromDF(df_EAR, glueContext, "df_EAR_dynamicFrame")

 # Write the transformed dataset to S3 with Paritioning
datasink5 = glueContext.write_dynamic_frame.from_options(
    frame = df_EAR_dynamicFrame,
    connection_type = "s3",
    connection_options = {"path": "s3://mro-dev-esp-analytics-bucket/MRO_Analytics/DataOutputs/Snowflake_Outbound/EAR_HISTORY_PARQUET/", "partitionKeys": ["LOAD_DATE"]},
    format = "parquet",
    transformation_ctx = "datasink5"
)
