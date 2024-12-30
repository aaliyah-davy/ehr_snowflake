use role sysadmin;

create warehouse compute_wh with warehouse_size = 'x-small';

-- create database called ehr and ELT schema 
create database if not exists ehr;
create or replace schema ehr.land;
create or replace schema ehr.raw;
create or replace schema ehr.clean;
create or replace schema ehr.fact;
create or replace schema ehr.dim;
create or replace schema ehr.load;
create or replace schema ehr.procedures;
create or replace schema ehr.analytics;
create or replace schema ehr.forecasts;
create or replace schema ehr.governance;

show schemas in database ehr;

-- set context to land schema
use schema ehr.land;

-- json formatting
create or replace file format ehr.land.json_fmt
    type = json
    null_if = ('\\n','null','')
    strip_outer_array = true
    comment = 'Json file, outer array stripped';

create or replace stage ehr.land.fhir_stage;

list @ehr.land.fhir_stage;

list @fhir_stage/ehr/json_folder

list @fhir_stage/ehr/json_folder/0000e4c0-2057-4c43-a90e-33891c7bc097.json

SELECT t.$1
FROM @ehr.land.fhir_stage/ehr/json_folder/0000e4c0-2057-4c43-a90e-33891c7bc097.json (FILE_FORMAT => ehr.land.json_fmt) t;


-- quick check on landing performance
select
        t.$1:type::text as type,
        t.$1:entry::ARRAY as entry,
        t.$1:resourceType::text as resourceType,
        
        metadata$filename as filename,
        metadata$file_row_number as file_row_number,
        metadata$file_content_key as file_content_key,
        metadata$file_last_modified as file_last_modified
    from @fhir_stage/ehr/json_folder/000a336a-41c8-4e46-8526-1e502346e28f.json (file_format => 'json_fmt') t;

-- run query on entire folder
select
        t.$1:type::text as type,
        t.$1:entry::ARRAY as entry,
        t.$1:resourceType::text as resourceType,
        
        metadata$filename as filename,
        metadata$file_row_number as file_row_number,
        metadata$file_content_key as file_content_key,
        metadata$file_last_modified as file_last_modified
    from @fhir_stage/ehr/json_folder/ (file_format => 'json_fmt') t;


-- snowsql -- push data
-- use database EHR;
-- use schema land;
-- use warehouse compute_wh;
-- use role sysadmin;

-- PUT file: ///fhir/00/000/*.json @fhir_stage/ehr/json_folder/ parallel=50;












