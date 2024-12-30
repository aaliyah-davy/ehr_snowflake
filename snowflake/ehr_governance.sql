use role sysadmin;
use warehouse compute_wh;
use schema ehr.governance;


-- DATA ACCESS CONTROL ---
create role read_only_role;

-- Grant read-only access to a schema
grant usage on schema ehr.fact to role read_only_role;
grant select on all tables in schema ehr.fact to role read_only_role;

grant role read_only_role to user secretary;

create masking policy patient_masking_policy 
as (val string) 
returns string ->
case 
  when current_role() in ('admin_role') then val
  else 'MASKED'
end;

alter table ehr.dim.patient 
modify column patient_id set masking policy patient_masking_policy;


-- DATA AUDITING ---
select * from information_schema.access_history 
where object_name = 'patient'
and object_schema = 'ehr.dim';

--- SCHEMA EVOLUTION CONTROL ---
alter table ehr.fact.patient 
set enable_schema_evolution = true;


--- DATA RETENTION POLICY ---
alter table ehr.fact.encounter 
set data_retention_time_in_days = 365;


--- DATA LINEAGE ---
comment on table ehr.fact.encounter is 'Stores patient encounter data';
comment on column ehr.fact.encounter.patient_id is 'Foreign key linking to the patient table';


--- DATA QUALITY CHECK ---
-- Add a check constraint for patient age to be positive
alter table ehr.dim.patient 
add constraint age_check check (age > 0);

