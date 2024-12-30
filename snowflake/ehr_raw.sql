use role sysadmin;
use warehouse compute_wh;
use schema ehr.raw;

-- Creates a transient table inside the raw layer
create or replace transient table ehr.raw.patient_raw_tbl (
    type text not null,
    entry ARRAY not null, 
    resourceType text not null,
    stg_file_name text not null,
    stg_file_row int not null,
    stg_file_hash text not null,
    stg_ts timestamp not null
)
comment = 'This table stores the extracted root elements of raw json data'
;

desc file format ehr.land.json_fmt;

-- DROP TABLE IF EXISTS ehr.raw.patient_raw_tbl;

copy into ehr.raw.patient_raw_tbl from
    (
    SELECT 
        tbl.$1:type::text as type,
        tbl.$1:entry::ARRAY as entry,
        tbl.$1:resourceType::text as resourceType,
        
        metadata$filename as filename,
        metadata$file_row_number as file_row_number,
        metadata$file_content_key as file_content_key,
        metadata$file_last_modified as file_last_modified
    from @ehr.land.fhir_stage/ehr/json_folder/ (file_format => 'ehr.land.json_fmt') tbl)

    on_error = continue; 

-- count number of patients ingested
select count(*) from ehr.raw.patient_raw_tbl

-- head of table (10 rows)
select * from ehr.raw.patient_raw_tbl limit 10
