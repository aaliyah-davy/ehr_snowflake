use role sysadmin;
use warehouse compute_wh;
use schema ehr.analytics;

---- PATIENT DEMOGRAPHICS ----
select 
    gender,
    age,
    city,
    state,
    count(patient_id) as patient_count
from ehr.dim.patient
group by gender, age, city, state
order by patient_count desc;


---- AVERAGE ENCOUNTER DURATION ----
select 
    status,
    avg(duration) as avg_duration_min
from ehr.fact.encounter
group by status;


---- MOST COMMON CONDITIONS ----
select 
    type as condition_type,
    count(condition_id) as condition_count
from ehr.fact.condition
group by condition_type
order by condition_count desc;


---- PERCENT UNVACCINATED ----
select 
    type as vaccine_type,
    count(*) as total_vaccines,
    sum(case when not_given_vaccine then 1 else 0 end) as not_given_count,
    round((not_given_count / total_vaccines) * 100.0, 2) as not_given_percent
from ehr.fact.immunization
group by vaccine_type;


select 
    p.patient_id,
    p.age,
    p.gender,
    count(distinct e.encounter_id) as total_encounters,
    count(distinct c.condition_id) as total_conditions,
    count(distinct i.immunization_id) as total_immunizations,
    count(distinct o.observation_id) as total_observations,
    count(distinct d.diagnostic_id) as total_diagnostics
from ehr.dim.patient p
left join ehr.fact.encounter e on p.patient_id = e.patient_id
left join ehr.fact.condition c on p.patient_id = c.patient_id
left join ehr.fact.immunization i on p.patient_id = i.patient_id
left join ehr.fact.observation o on p.patient_id = o.patient_id
left join ehr.fact.diagnostic d on p.patient_id = d.patient_id
group by p.patient_id, p.age, p.gender
order by total_encounters desc;

