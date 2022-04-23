drop table if exists country_codes;
drop table if exists cov_cases_reg_age_gen;
drop table if exists cov_deaths_nation;
drop table if exists cov_deaths_yorkshire;
drop table if exists cov_vaccinations;
drop table if exists education;
drop table if exists england_codes;
drop table if exists ethnicity;
drop table if exists fludeaths;
drop table if exists yorkshire_codes;
drop table if exists data_quality_audit;
drop table if exists data_validity_audit;
drop database if exists staging;

drop table if exists dim_agegrp;
drop table if exists dim_date;
drop table if exists dim_location;
drop table if exists fact_cov_cases;
drop table if exists fact_cov_deaths_and_vaccines;
drop table if exists fact_regn_miscellaneous;
drop table if exists data_quality_audit;
drop table if exists data_validity_audit;
drop database if exists mart;