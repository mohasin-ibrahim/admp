from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, regexp_replace
spark = SparkSession.builder.master("yarn-client").enableHiveSupport() \
                    .appName('ADMP-Grp14-Mart-Facts') \
                    .getOrCreate()

deaths_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, stg_deaths.newDeaths from mart.dim_date dt join staging.cov_deaths_nation stg_deaths on dt.dt = stg_deaths.reported_date join mart.DIM_LOCATION dl on dl.code = stg_deaths.areacode ")

deaths_yorkshire = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, stg_deaths.newDeaths from mart.dim_date dt join staging.cov_deaths_yorkshire stg_deaths on dt.dt = stg_deaths.reported_date join mart.DIM_LOCATION dl on dl.code = stg_deaths.areacode ")

total_deaths = deaths_nation.union(deaths_yorkshire)
total_deaths.registerTempTable("total_deaths")
# fact_deaths= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COVID_DEATHS_ID, dim_date_id, dim_location_id, newdeaths from total_deaths")
# fact_deaths.write.insertInto("mart.FACT_COVID_DEATHS", overwrite=True)

doses_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, stg_doses.firstDose, stg_doses.secondDose from mart.dim_date dt join staging.cov_vaccinations stg_doses on dt.dt = stg_doses.reported_date join mart.DIM_LOCATION dl on dl.code = stg_doses.areacode")
doses_nation.registerTempTable("doses_nation")
# fact_vaccines= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COVID_VACCINES_ID, dim_date_id, dim_location_id, firstDose, secondDose from doses_nation")
# fact_vaccines.write.insertInto("mart.FACT_COVID_VACCINES", overwrite=True)

deaths_and_vaccines = spark.sql("select deaths.dim_date_id, deaths.dim_location_id, deaths.newDeaths, doses.firstDose, doses.secondDose from total_deaths deaths join doses_nation doses on deaths.dim_date_id = doses.dim_date_id and deaths.dim_location_id = doses.dim_location_id")
deaths_and_vaccines.registerTempTable("deaths_and_vaccines")
deaths_and_vaccines= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COV_DEATHS_AND_VACCINES_ID, dim_date_id, dim_location_id, newDeaths, firstDose, secondDose from deaths_and_vaccines")
deaths_and_vaccines.write.insertInto("mart.FACT_COV_DEATHS_AND_VACCINES", overwrite=True)

cases_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, da.DIM_AGEGRP_ID, stg_cases.malecases, stg_cases.femalecases from mart.dim_date dt join staging.cov_cases_reg_age_gen stg_cases on dt.dt = stg_cases.reported_date join mart.DIM_LOCATION dl on dl.code = stg_cases.areacode join mart.DIM_AGEGRP da on da.age_group = stg_cases.age ")
cases_nation.registerTempTable("cases_nation")
fact_cases= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id, dim_agegrp_id) as FACT_COV_CASES_ID, dim_date_id, dim_location_id, dim_agegrp_id, malecases, femalecases, (malecases+femalecases) as totalcases from cases_nation")
fact_cases.write.insertInto("mart.FACT_COVID_CASES", overwrite=True)

edu_agg = spark.sql("SELECT codes.RGNCD, codes.RGNNM, ethn.ethnicity, AVG(ethn.VALUE) as AVG_VAL FROM staging.ethnicity ethn JOIN (SELECT DISTINCT UTLACD, UTLANM, RGNCD, RGNNM FROM staging.england_codes) as codes on ethn.GEO_CODE = codes.UTLACD GROUP BY codes.RGNCD, codes.RGNNM, ethn.ethnicity")
edu_agg.registerTempTable("edu_agg")

ethn_and_edu = spark.sql("select dl.DIM_LOCATION_ID, agg.ethnicity, agg.avg_val as ethnic_percent, edu.no_qual, edu.nqf4_above from mart.dim_location dl join staging.education edu on dl.name = edu.region join edu_agg agg on dl.code = agg.rgncd ")
ethn_and_edu.registerTempTable("ethn_and_edu")

fact_regn_misc = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_location_id, ethnicity) as FACT_REGN_MISCELLANEOUS_ID, dim_location_id, ethnicity, ethnic_percent, no_qual, nqf4_above from total_deaths")
fact_regn_misc.write.insertInto("mart.FACT_REGN_MISC", overwrite=True)