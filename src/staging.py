from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, regexp_replace
spark = SparkSession.builder.master("yarn-client").enableHiveSupport() \
                    .appName('ADMP-Grp14-Staging') \
                    .getOrCreate()

ethnicity_schema = StructType([StructField("Measure", StringType(), True)\
                   ,StructField("Time", StringType(), True)\
                   ,StructField("Ethnicity", StringType(), True)\
                   ,StructField("Ethnicity_type", StringType(), True)\
                   ,StructField("Geography_name", StringType(), True)\
                   ,StructField("Geography_code", StringType(), True)\
                   ,StructField("Geography_type", StringType(), True)\
                   ,StructField("Value", FloatType(), True)\
                   ,StructField("Numerator", StringType(), True)\
                   ,StructField("Denominator", StringType(), True) ])

education_schema = StructType([StructField("Region", StringType(), True)\
                   ,StructField("No_Qual", FloatType(), True)\
                   ,StructField("NQF_Level2_Above", FloatType(), True)\
                   ,StructField("NQF_Level3_Above", FloatType(), True)\
                   ,StructField("NQF_Level4_Above", FloatType(), True) ])

flu_schema = StructType([StructField("Year", StringType(), True)\
                   ,StructField("Country", StringType(), True)\
                   ,StructField("Influenza_Deaths", StringType(), True) ])

deaths_nation_schema = StructType([StructField("areaCode", StringType(), True)\
                   ,StructField("areaName", StringType(), True)\
                   ,StructField("areaType", StringType(), True)\
                   ,StructField("date", DateType(), True)\
                   ,StructField("newDeathsByDeathDate", IntegerType(), True) ])

cases_age_region_schema = StructType([StructField("areaCode", StringType(), True)\
                   ,StructField("areaName", StringType(), True)\
                   ,StructField("areaType", StringType(), True)\
                   ,StructField("date", DateType(), True)\
                   ,StructField("age", StringType(), True)\
                   ,StructField("cases", IntegerType(), True)\
                   ,StructField("rollingSum", IntegerType(), True)\
                   ,StructField("rollingRate", StringType(), True) ])

vaccination_schema = StructType([StructField("areaCode", StringType(), True)\
                   ,StructField("areaName", StringType(), True)\
                   ,StructField("areaType", StringType(), True)\
                   ,StructField("date", DateType(), True)\
                   ,StructField("numberOfVaccines", IntegerType(), True) ])

cases_gender_schema = StructType([StructField("areaCode", StringType(), True)\
                   ,StructField("areaName", StringType(), True)\
                   ,StructField("areaType", StringType(), True)\
                   ,StructField("date", DateType(), True)\
                   ,StructField("age", StringType(), True)\
                   ,StructField("rate", StringType(), True)\
                   ,StructField("value", IntegerType(), True) ])

ethnicity_stg = spark.read.csv("/data/input/static/ethnicity", schema=ethnicity_schema)
education_stg = spark.read.csv("/data/input/static/education", schema=education_schema)
flu_stg = spark.read.csv("/data/input/static/flu", schema=flu_schema)
lacode_stg = spark.read.csv("/data/input/static/lacode", sep='~', header=True)
deaths_nation_stg = spark.read.csv("/data/input/dynamic/covid/deaths/nation", schema=deaths_nation_schema)
cases_age_region_stg = spark.read.csv("/data/input/dynamic/covid/infections/england/age", schema=cases_age_region_schema)
deaths_utla_stg = spark.read.csv("/data/input/dynamic/covid/deaths/yorkshire", schema=deaths_nation_schema)
vaccin_first_stg = spark.read.csv("/data/input/dynamic/covid/vaccinations/england/first", schema=vaccination_schema)
vaccin_second_stg = spark.read.csv("/data/input/dynamic/covid/vaccinations/england/second", schema=vaccination_schema)
male_cases_stg = spark.read.csv("/data/input/dynamic/covid/infections/england/male", schema=cases_gender_schema)
female_cases_stg = spark.read.csv("/data/input/dynamic/covid/infections/england/female", schema=cases_gender_schema)

'''Cleaning, filtering and manipulating source datasets'''
major_ethnicities = ["Asian", "Black", "Mixed", "White", "Other"]

eth = ethnicity_stg.filter((col("Measure") == "% of local population in this ethnic group") & (col("Ethnicity_type") == "ONS 2011 5+1") & col("Ethnicity").isin(major_ethnicities)).select("Ethnicity","Geography_name","Geography_code","Value")

edu = education_stg.withColumn("Region", when(col("Region").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Region"))).select("Region", "No_Qual", "NQF_Level2_Above", "NQF_Level3_Above", "NQF_Level4_Above")

la_codes = lacode_stg.withColumn("RGNNM", when(col("RGNNM") == "East of England", "East")
                                 .when(col("RGNNM").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("RGNNM"))).select("UTLACD", "UTLANM", "RGNCD", "RGNNM", "CTRYCD", "CTRYNM")

cases_by_age_region = cases_age_region_stg.withColumn("areaName", when(col("areaName").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("areaName"))).withColumn("age", when(col("age") == "00_04", "0_to_4").when(col("age") == "05_09", "5_to_9")
                                 .otherwise(regexp_replace(col("age"), "_", "_to_"))).filter((col("age") != "unassigned") & (col("age") != "60+") & (col("age") != "00_to_59")).select("areaCode","areaName","areaType","date","age","cases")

vaccin_first = vaccin_first_stg.withColumn("areaName", when(col("areaName").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("areaName"))).select("areaCode","areaName","areaType","date","numberOfVaccines")

vaccin_second = vaccin_second_stg.withColumn("areaName", when(col("areaName").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("areaName"))).select("areaCode","areaName","areaType","date","numberOfVaccines")

male_cases = male_cases_stg.withColumn("areaName", when(col("areaName").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("areaName"))).select("areaCode","areaName","areaType","date","age","value")

female_cases = female_cases_stg.withColumn("areaName", when(col("areaName").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("areaName"))).select("areaCode","areaName","areaType","date","age","value")                               

vaccin_both = vaccin_first.join(vaccin_second, (vaccin_first["areaCode"] == vaccin_second["areaCode"]) & (vaccin_first["date"] == vaccin_second["date"]), "inner").select(vaccin_first["areaCode"], vaccin_first["areaName"], vaccin_first["areaType"], vaccin_first["date"], vaccin_first["numberOfVaccines"].alias("firstDose"), vaccin_second["numberOfVaccines"].alias("secondDose"))

cases_both = male_cases.join(female_cases, (male_cases["areaCode"] == female_cases["areaCode"]) & (male_cases["date"] == female_cases["date"]) & (male_cases["age"] == female_cases["age"]), "inner").select(male_cases["areaCode"], male_cases["areaName"], male_cases["areaType"], male_cases["date"], male_cases["age"], male_cases["value"].alias("maleCases"), female_cases["value"].alias("femaleCases"))
cases_both.registerTempTable("cases_both")
cases_actual_calc = spark.sql("SELECT *, lead(malecases) over (partition by areacode, age order by date desc) as m_l, lead(femalecases) over (partition by areacode, age order by date desc) as f_l from cases_both")
cases_actual_calc.registerTempTable("cases_actual_calc")
cases_both_actual = spark.sql("SELECT areacode, areaname, areatype, date, age, malecases-m_l as actualmalecases, femalecases-f_l as actualfemalecases from cases_actual_calc")

la_codes.registerTempTable("la_codes")
country_codes = spark.sql("SELECT DISTINCT CTRYCD, CTRYNM FROM la_codes")

england_region_codes_all = la_codes.filter(col("CTRYNM") == "England")
england_region_codes_all.registerTempTable("england_codes_all")

yorkshire_region_code = spark.sql("SELECT DISTINCT UTLACD, UTLANM FROM england_codes_all WHERE RGNNM = 'Yorkshire and Humber'")

utla_deaths_yorkshire = deaths_utla_stg.join(yorkshire_region_code, (deaths_utla_stg["areaCode"] == yorkshire_region_code["UTLACD"]), "inner").select(deaths_utla_stg["areaCode"], deaths_utla_stg["areaName"], deaths_utla_stg["areaType"], deaths_utla_stg["date"], deaths_utla_stg["newDeathsByDeathDate"].alias("newDeaths"))

eth.write.insertInto("staging.ethnicity", overwrite=True)
edu.write.insertInto("staging.education", overwrite=True)
flu_stg.write.insertInto("staging.fludeaths", overwrite=True)
deaths_nation_stg.write.insertInto("staging.cov_deaths_nation", overwrite=True)
cases_both_actual.write.insertInto("staging.cov_cases_reg_age_gen", overwrite=True)
vaccin_both.write.insertInto("staging.cov_vaccinations", overwrite=True)
utla_deaths_yorkshire.write.insertInto("staging.cov_deaths_yorkshire", overwrite=True)
country_codes.write.insertInto("staging.country_codes", overwrite=True)
england_region_codes_all.write.insertInto("staging.england_codes", overwrite=True)
yorkshire_region_code.write.insertInto("staging.yorkshire_codes", overwrite=True)