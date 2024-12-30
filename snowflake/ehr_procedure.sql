use role sysadmin;
use warehouse compute_wh;
use schema ehr.procedures;


--- CHECK FOR NULLS ---
CREATE OR REPLACE PROCEDURE check_patient_nulls()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  IF EXISTS (SELECT 1 FROM ehr.dim.patient WHERE patient_id IS NULL) THEN
      RETURN 'Error: Missing Patient ID in Patient Table';
  ELSE
      RETURN 'Data is clean';
  END IF;
END;
$$;


--- DUPLICATE DATA CHECK ---



--- DATA GOV ---
--- ROLE-BASED ACCESS ---
CREATE OR REPLACE PROCEDURE enforce_rbac_on_patient_data()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  -- Granting specific roles access to sensitive patient data
  GRANT SELECT ON ehr.dim.patient TO ROLE doctor_role;
  GRANT SELECT, INSERT ON ehr.fact.encounter TO ROLE data_scientist_role;
  
  RETURN 'Access granted successfully';
END;
$$;


--- MASKING PROCEDURES ---
CREATE MASKING POLICY ssn_masking 
  AS (val STRING)
  RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('admin_role') THEN val
      ELSE 'XXX-XX-XXXX'
    END;

ALTER TABLE ehr.dim.patient
MODIFY COLUMN ssn SET MASKING POLICY ssn_masking;


--- AGG ---
CREATE OR REPLACE PROCEDURE aggregate_encounters_by_month()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  INSERT INTO ehr.fact.encounter_summary (patient_id, month, encounter_count)
  SELECT patient_id, TO_CHAR(start_date, 'YYYY-MM'), COUNT(*)
  FROM ehr.fact.encounter
  GROUP BY patient_id, TO_CHAR(start_date, 'YYYY-MM');
  
  RETURN 'Aggregation completed successfully';
END;
$$;


--- AUDIT LOGGING ---
CREATE OR REPLACE PROCEDURE log_data_modifications()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  INSERT INTO ehr.audit_log (user_id, action, timestamp)
  VALUES (CURRENT_USER(), 'Data modification action', CURRENT_TIMESTAMP());
  
  RETURN 'Audit log entry created successfully';
END;
$$;


---- VIEW COMMON CONDITIONS, DIAGNOSIS, & PROCEDURES BY PATIENT DEMOGRAPHICS ----

CREATE OR REPLACE PROCEDURE view_by_demographics(
    demographic_column STRING,
    start_date DATE,
    end_date DATE,
    condition_filter STRING
)
RETURNS TABLE (
    demographic STRING,
    common_conditions STRING,
    common_procedures STRING,
    common_diagnoses STRING
)
LANGUAGE SQL
AS
$$
DECLARE
    demographic_expr STRING;
    condition_clause STRING;
    date_clause STRING;
BEGIN
    -- Validate the input to prevent SQL injection
    IF demographic_column NOT IN ('age', 'gender', 'city', 'state', 'race', 'ethnicity') THEN
        RETURN TABLE (demographic STRING, common_conditions STRING, common_procedures STRING, common_diagnoses STRING)
        FROM (SELECT 'Invalid demographic column' AS demographic, NULL AS common_conditions, NULL AS common_procedures, NULL AS common_diagnoses);
    END IF;

    -- Dynamically construct the grouping expression
    demographic_expr = REPLACE(demographic_column, '''', ''); -- Escape single quotes

    -- Construct condition filter clause
    condition_clause = CASE 
        WHEN condition_filter IS NOT NULL THEN ' AND c.type = ''' || condition_filter || ''''
        ELSE ''
    END;

    -- Construct date range filter clause
    date_clause = CASE
        WHEN start_date IS NOT NULL AND end_date IS NOT NULL THEN
            ' AND COALESCE(c.onset_date, pr.date_performed, d.date_effective) BETWEEN ''' || start_date || ''' AND ''' || end_date || ''''
        ELSE ''
    END;

    -- Return the aggregated table
    RETURN TABLE (
        demographic STRING,
        common_conditions STRING,
        common_procedures STRING,
        common_diagnoses STRING
    )
    FROM (
        SELECT
            p.{$demographic_expr} AS demographic,
            ARRAY_AGG(DISTINCT c.type ORDER BY COUNT(c.condition_id) DESC LIMIT 3) AS common_conditions,
            ARRAY_AGG(DISTINCT pr.type ORDER BY COUNT(pr.procedure_id) DESC LIMIT 3) AS common_procedures,
            ARRAY_AGG(DISTINCT d.type ORDER BY COUNT(d.diagnostic_id) DESC LIMIT 3) AS common_diagnoses
        FROM ehr.dim.patient p
        LEFT JOIN ehr.fact.condition c ON p.patient_id = c.patient_id
        LEFT JOIN ehr.fact.procedure pr ON p.patient_id = pr.patient_id
        LEFT JOIN ehr.fact.diagnostic d ON p.patient_id = d.patient_id
        WHERE 1 = 1
            || condition_clause
            || date_clause
        GROUP BY p.{$demographic_expr}
    );
END;
$$;


