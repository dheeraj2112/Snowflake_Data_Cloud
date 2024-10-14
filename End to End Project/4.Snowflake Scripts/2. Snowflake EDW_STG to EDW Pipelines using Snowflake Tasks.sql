--#################################################################################################
--EDW STG to EDW Pipelines using Snowflake based on ORA HR DB objects

--#################################################################################################

--STEP 0  ==> Suspend root task to make any changes/additions/modifications and then resume all child tasks first before the parent root task.


/*
--resume all child before parent once created from step-1

ALTER TASK TSK_EDW_STG_EDW_JOB_HISTORY RESUME;
ALTER TASK TSK_EDW_STG_EDW_EMPLOYEES RESUME;
ALTER TASK TSK_EDW_STG_EDW_JOBS RESUME;
ALTER TASK TSK_EDW_STG_EDW_DEPARTMENTS RESUME;
ALTER TASK TSK_EDW_STG_EDW_LOCATIONS RESUME;	
ALTER TASK TSK_EDW_STG_EDW_COUNTRIES RESUME;
ALTER TASK TSK_EDW_STG_EDW_REGION RESUME;
ALTER TASK ROOT_TSK_EDW_ALL RESUME;


SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp())))
  
  
 ALTER TASK ROOT_TSK_EDW_ALL SUSPEND;
 ALTER TASK TSK_EDW_STG_EDW_REGION SUSPEND;

SHOW TASKS ;

SHOW TASKS LIKE '%EDW%' ;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'ROOT_TSK_EDW_ALL')) ;

*/
--STEP1   ===> ROOT TASK
USE EDW.EDW;

--Use SCHEDULE with MIN option 

CREATE OR REPLACE TASK ROOT_TSK_EDW_ALL
WAREHOUSE=COMPUTE_WH
SCHEDULE='10 MINUTE'  	// EVERY 10 MIN
AS
SELECT CURRENT_TIMESTAMP() as CURRENT_TIME;

--Use SCHEDULE with CRON option

CREATE OR REPLACE TASK ROOT_TSK_EDW_ALL
	WAREHOUSE=COMPUTE_WH
	SCHEDULE='USING CRON 0 10 * * MON-FRI Asia/Calcutta'
	AS SELECT CURRENT_TIMESTAMP() AS CURRENT_TIME;

--REGION 

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_REGION
WAREHOUSE=COMPUTE_WH
AFTER ROOT_TSK_EDW_ALL
AS 

MERGE INTO EDW.EDW.REGIONS AS TGT USING 
(SELECT R.REGION_ID as JOIN_KEY,REGION_ID,REGION_NAME,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.REGIONS R

UNION ALL 

--Additional logic is implemented to handle the inserts for the updated records in the source with Non-matching rows so these can be handled from NOT MATCHED section.

SELECT NULL as JOIN_KEY,STG.REGION_ID,STG.REGION_NAME,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.REGIONS STG 
INNER JOIN  EDW.EDW.REGIONS EDW ON STG.REGION_ID=EDW.REGION_ID

WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.REGION_NAME<>EDW.REGION_NAME

) AS SRC ON TGT.REGION_ID = SRC.JOIN_KEY

--Logic to update end date and active flags for updated records in the source.

WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.REGION_NAME<>SRC.REGION_NAME
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

--Logic to insert the new records in the source.
WHEN NOT MATCHED THEN INSERT
    (
        REGION_ID,
        REGION_NAME,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.REGION_ID,
        SRC.REGION_NAME,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	

 --COUNTRIES
 
CREATE OR REPLACE TASK TSK_EDW_STG_EDW_COUNTRIES
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_REGION
AS 

MERGE INTO EDW.EDW.COUNTRIES AS TGT 
USING 
(SELECT C.COUNTRY_ID as JOIN_KEY,COUNTRY_ID,COUNTRY_NAME,REGION_ID,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.COUNTRIES C

UNION ALL 

SELECT NULL as JOIN_KEY,STG.COUNTRY_ID,STG.COUNTRY_NAME,STG.REGION_ID,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.COUNTRIES STG 
INNER JOIN  EDW.EDW.COUNTRIES EDW ON STG.COUNTRY_ID=EDW.COUNTRY_ID

WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.COUNTRY_NAME<>EDW.COUNTRY_NAME AND STG.REGION_ID<>EDW.REGION_ID

) AS SRC ON TGT.COUNTRY_ID = SRC.JOIN_KEY

WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.COUNTRY_NAME<>SRC.COUNTRY_NAME AND TGT.REGION_ID<>SRC.REGION_ID
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        COUNTRY_ID,
        COUNTRY_NAME,
		REGION_ID,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.COUNTRY_ID,
        SRC.COUNTRY_NAME,
		SRC.REGION_ID,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	
	
--LOCATIONS 

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_LOCATIONS
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_COUNTRIES
AS 

MERGE INTO EDW.EDW.LOCATIONS AS TGT 
USING
 
(SELECT L.LOCATION_ID as JOIN_KEY,LOCATION_ID,STREET_ADDRESS,POSTAL_CODE,CITY,STATE_PROVINCE,COUNTRY_ID,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.LOCATIONS L

UNION ALL 

SELECT NULL AS JOIN_KEY,STG.LOCATION_ID,STG.STREET_ADDRESS,STG.POSTAL_CODE,STG.CITY,STG.STATE_PROVINCE,STG.COUNTRY_ID,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.LOCATIONS STG 
INNER JOIN  EDW.EDW.LOCATIONS EDW ON STG.LOCATION_ID=EDW.LOCATION_ID
WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000'  AND STG.STREET_ADDRESS<>EDW.STREET_ADDRESS
AND STG.POSTAL_CODE<>EDW.POSTAL_CODE AND STG.CITY<>EDW.CITY AND STG.STATE_PROVINCE<>EDW.STATE_PROVINCE AND STG.COUNTRY_ID<>EDW.COUNTRY_ID

) AS SRC ON TGT.LOCATION_ID = SRC.JOIN_KEY

WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.STREET_ADDRESS<>SRC.STREET_ADDRESS
AND TGT.POSTAL_CODE<>SRC.POSTAL_CODE AND TGT.CITY<>SRC.CITY AND TGT.STATE_PROVINCE<>SRC.STATE_PROVINCE AND TGT.COUNTRY_ID<>SRC.COUNTRY_ID
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        LOCATION_ID,
        STREET_ADDRESS,
		POSTAL_CODE,
		CITY,
		STATE_PROVINCE,
		COUNTRY_ID,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.LOCATION_ID,
        SRC.STREET_ADDRESS,
		SRC.POSTAL_CODE,
		SRC.CITY,
		SRC.STATE_PROVINCE,
		SRC.COUNTRY_ID,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	
	
--DEPARTMENTS

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_DEPARTMENTS
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_LOCATIONS
AS 

MERGE INTO EDW.EDW.DEPARTMENTS AS TGT 
USING 
(SELECT D.DEPARTMENT_ID as JOIN_KEY,DEPARTMENT_ID,DEPARTMENT_NAME,MANAGER_ID,LOCATION_ID,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.DEPARTMENTS D

UNION ALL 

SELECT NULL AS JOIN_KEY,STG.DEPARTMENT_ID,STG.DEPARTMENT_NAME,STG.MANAGER_ID,STG.LOCATION_ID,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.DEPARTMENTS STG 
INNER JOIN  EDW.EDW.DEPARTMENTS EDW ON STG.DEPARTMENT_ID=EDW.DEPARTMENT_ID

WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.DEPARTMENT_NAME<>EDW.DEPARTMENT_NAME
AND STG.MANAGER_ID<>EDW.MANAGER_ID AND STG.LOCATION_ID<>EDW.LOCATION_ID 

) AS SRC ON TGT.DEPARTMENT_ID = SRC.JOIN_KEY

WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.DEPARTMENT_NAME<>SRC.DEPARTMENT_NAME
AND TGT.MANAGER_ID<>SRC.MANAGER_ID AND TGT.LOCATION_ID<>SRC.LOCATION_ID 
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        DEPARTMENT_ID,
        DEPARTMENT_NAME,
		MANAGER_ID,
		LOCATION_ID,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.DEPARTMENT_ID,
        SRC.DEPARTMENT_NAME,
		SRC.MANAGER_ID,
		SRC.LOCATION_ID,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	
	
--JOBS 

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_JOBS
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_DEPARTMENTS
AS 

MERGE INTO EDW.EDW.JOBS AS TGT 

USING 
(SELECT J.JOB_ID as JOIN_KEY,JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.JOBS J

UNION ALL 

SELECT NULL AS JOIN_KEY,STG.JOB_ID,STG.JOB_TITLE,STG.MIN_SALARY,STG.MAX_SALARY,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.JOBS STG 
INNER JOIN  EDW.EDW.JOBS EDW ON STG.JOB_ID=EDW.JOB_ID
WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.JOB_TITLE<>EDW.JOB_TITLE
AND STG.MIN_SALARY<>EDW.MIN_SALARY AND STG.MAX_SALARY<>EDW.MAX_SALARY 

) AS SRC ON TGT.JOB_ID = SRC.JOIN_KEY


WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.JOB_TITLE<>SRC.JOB_TITLE
AND TGT.MIN_SALARY<>SRC.MIN_SALARY AND TGT.MAX_SALARY<>SRC.MAX_SALARY 
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        JOB_ID,
        JOB_TITLE,
		MIN_SALARY,
		MAX_SALARY,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.JOB_ID,
        SRC.JOB_TITLE,
		SRC.MIN_SALARY,
		SRC.MAX_SALARY,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	
--EMPLOYEES

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_EMPLOYEES
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_JOBS
AS 

MERGE INTO EDW.EDW.EMPLOYEES AS TGT 

USING 
(SELECT E.EMPLOYEE_ID as JOIN_KEY,EMPLOYEE_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,HIRE_DATE,JOB_ID,SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.EMPLOYEES E

UNION ALL 

SELECT NULL as JOIN_KEY,STG.EMPLOYEE_ID,STG.FIRST_NAME,STG.LAST_NAME,STG.EMAIL,STG.PHONE_NUMBER,STG.HIRE_DATE,STG.JOB_ID,STG.SALARY,STG.COMMISSION_PCT,STG.MANAGER_ID,STG.DEPARTMENT_ID,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.EMPLOYEES STG 
INNER JOIN  EDW.EDW.EMPLOYEES EDW ON STG.EMPLOYEE_ID=EDW.EMPLOYEE_ID

WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.FIRST_NAME<>EDW.FIRST_NAME
AND STG.LAST_NAME<>EDW.LAST_NAME AND STG.EMAIL<>EDW.EMAIL AND STG.PHONE_NUMBER<>EDW.PHONE_NUMBER  AND STG.HIRE_DATE<>EDW.HIRE_DATE  AND STG.JOB_ID<>EDW.JOB_ID 
AND  STG.SALARY<>EDW.SALARY AND STG.COMMISSION_PCT<>EDW.COMMISSION_PCT AND STG.MANAGER_ID<>EDW.MANAGER_ID AND STG.DEPARTMENT_ID<>EDW.DEPARTMENT_ID

) AS SRC ON TGT.EMPLOYEE_ID = SRC.JOIN_KEY


WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.FIRST_NAME<>SRC.FIRST_NAME
AND TGT.LAST_NAME<>SRC.LAST_NAME AND TGT.EMAIL<>SRC.EMAIL AND TGT.PHONE_NUMBER<>SRC.PHONE_NUMBER  AND TGT.HIRE_DATE<>SRC.HIRE_DATE  AND TGT.JOB_ID<>SRC.JOB_ID 
AND  TGT.SALARY<>SRC.SALARY AND TGT.COMMISSION_PCT<>SRC.COMMISSION_PCT AND TGT.MANAGER_ID<>SRC.MANAGER_ID AND TGT.DEPARTMENT_ID<>SRC.DEPARTMENT_ID
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        EMPLOYEE_ID,
        FIRST_NAME,
		LAST_NAME,
		EMAIL,
		PHONE_NUMBER,
		HIRE_DATE,
        JOB_ID,
        SALARY,
        COMMISSION_PCT,
        MANAGER_ID,
        DEPARTMENT_ID,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.EMPLOYEE_ID,
        SRC.FIRST_NAME,
		SRC.LAST_NAME,
		SRC.EMAIL,
		SRC.PHONE_NUMBER,
		SRC.HIRE_DATE,
        SRC.JOB_ID,
        SRC.SALARY,
        SRC.COMMISSION_PCT,
        SRC.MANAGER_ID,
        SRC.DEPARTMENT_ID,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    );
	

--JOB_HISTORY

CREATE OR REPLACE TASK TSK_EDW_STG_EDW_JOB_HISTORY
WAREHOUSE=COMPUTE_WH
AFTER TSK_EDW_STG_EDW_EMPLOYEES
AS 
MERGE INTO EDW.EDW.JOB_HISTORY AS TGT 

USING 
(SELECT JH.EMPLOYEE_ID||JH.START_DATE::STRING as JOIN_KEY,EMPLOYEE_ID,START_DATE,END_DATE,JOB_ID,DEPARTMENT_ID,MD5_HASH,EDW_UPDATED_BY,EDW_CREATE_TIMESTAMP,EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.JOB_HISTORY JH

UNION ALL 

SELECT NULL as JOIN_KEY,STG.EMPLOYEE_ID,STG.START_DATE,STG.END_DATE,STG.JOB_ID,STG.DEPARTMENT_ID,STG.MD5_HASH,STG.EDW_UPDATED_BY,STG.EDW_CREATE_TIMESTAMP,STG.EDW_UPDATE_TIMESTAMP FROM EDW.EDW_STG.JOB_HISTORY STG 
INNER JOIN  EDW.EDW.JOB_HISTORY EDW ON STG.EMPLOYEE_ID=EDW.EMPLOYEE_ID AND STG.START_DATE=EDW.START_DATE

WHERE  EDW.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND STG.START_DATE<>EDW.START_DATE
AND STG.END_DATE<>EDW.END_DATE AND STG.JOB_ID<>EDW.JOB_ID AND STG.DEPARTMENT_ID<>EDW.DEPARTMENT_ID

) AS SRC ON TGT.EMPLOYEE_ID||TGT.START_DATE::STRING = SRC.JOIN_KEY

WHEN MATCHED AND TGT.EDW_EFFECTIVE_END_DATE ='9999-12-31 00:00:00.000' AND TGT.START_DATE<>SRC.START_DATE
AND TGT.END_DATE<>SRC.END_DATE AND TGT.JOB_ID<>SRC.JOB_ID AND TGT.DEPARTMENT_ID<>SRC.DEPARTMENT_ID
THEN UPDATE SET 
 EDW_EFFECTIVE_END_DATE=CURRENT_TIMESTAMP()
,EDW_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP()
,EDW_UPDATED_BY='EDW'
,ACTIVE_FLAG='N'

WHEN NOT MATCHED THEN INSERT
    (
        EMPLOYEE_ID,
        START_DATE,
		END_DATE,
		JOB_ID,
		DEPARTMENT_ID,
        MD5_HASH,
        EDW_UPDATED_BY,
        EDW_CREATE_TIMESTAMP,
        EDW_UPDATE_TIMESTAMP,
        EDW_EFFECTIVE_START_DATE,
        EDW_EFFECTIVE_END_DATE,
        ACTIVE_FLAG
    )
VALUES
    (
        SRC.EMPLOYEE_ID,
        SRC.START_DATE,
		SRC.END_DATE,
		SRC.JOB_ID,
		SRC.DEPARTMENT_ID,
        SRC.MD5_HASH,
        SRC.EDW_UPDATED_BY,
        SRC.EDW_CREATE_TIMESTAMP,
        SRC.EDW_UPDATE_TIMESTAMP,
        CURRENT_TIMESTAMP(),
        '9999-12-31 00:00:00.000',
        'Y'
    )
;



--TASKS LOAD VALIDATIONS for STG TABLE(s) LOAD (only after all the children as well as parent tasks are resumed mentioned in step #0).

--EDW VALIDATION

SELECT 'REGIONS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.REGIONS
UNION ALL 
SELECT 'COUNTRIES' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.COUNTRIES
UNION ALL 
SELECT 'LOCATIONS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.LOCATIONS 
UNION ALL 
SELECT 'DEPARTMENTS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.DEPARTMENTS
UNION ALL 
SELECT 'JOBS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.JOBS
UNION ALL
SELECT 'EMPLOYEES' AS Table_Name, COUNT(1) AS TABLE_COUNTS FROM EDW.EDW.EMPLOYEES
UNION ALL 
SELECT 'JOB_HISTORY' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW.JOB_HISTORY
UNION ALL 
SELECT 'EMP_DETAILS_VIEW' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_EXTR.EMP_DETAILS_VIEW ;


--Truncate STG Tables statements for debugging and issues fixes, if needed.

TRUNCATE TABLE EDW.EDW.REGIONS;
TRUNCATE TABLE EDW.EDW.COUNTRIES;
TRUNCATE TABLE EDW.EDW.LOCATIONS;
TRUNCATE TABLE EDW.EDW.DEPARTMENTS;
TRUNCATE TABLE EDW.EDW.JOBS;
TRUNCATE TABLE EDW.EDW.EMPLOYEES;
TRUNCATE TABLE EDW.EDW.JOB_HISTORY;

--Additional, Dynamic Table Example to get the employee details updated for repoting need.

CREATE OR REPLACE DYNAMIC TABLE EMPLOYEE_DETAILS(
	EMPLOYEE_ID,
	JOB_ID,
	MANAGER_ID,
	DEPARTMENT_ID,
	LOCATION_ID,
	COUNTRY_ID,
	FIRST_NAME,
	LAST_NAME,
	SALARY,
	COMMISSION_PCT,
	DEPARTMENT_NAME,
	JOB_TITLE,
	CITY,
	STATE_PROVINCE,
	COUNTRY_NAME,
	REGION_NAME
) 

LAG = '10 MINUTES' 
REFRESH_MODE = AUTO 
INITIALIZE = ON_CREATE 
WAREHOUSE = COMPUTE_WH
 AS 

 SELECT  
  E.EMPLOYEE_ID,   
  E.JOB_ID,   
  E.MANAGER_ID,   
  E.DEPARTMENT_ID,  
  D.LOCATION_ID,  
  L.COUNTRY_ID,  
  E.FIRST_NAME,  
  E.LAST_NAME,  
  E.SALARY,  
  E.COMMISSION_PCT,  
  D.DEPARTMENT_NAME,  
  J.JOB_TITLE,  
  L.CITY,  
  L.STATE_PROVINCE,  
  C.COUNTRY_NAME,  
  R.REGION_NAME  
FROM  
  EDW.EMPLOYEES E,  
  EDW.DEPARTMENTS D,  
  EDW.JOBS J,  
  EDW.LOCATIONS L,  
  EDW.COUNTRIES C,  
  EDW.REGIONS R  
WHERE E.DEPARTMENT_ID = D.DEPARTMENT_ID  
  AND D.LOCATION_ID = L.LOCATION_ID  
  AND L.COUNTRY_ID = C.COUNTRY_ID  
  AND C.REGION_ID = R.REGION_ID  
  AND J.JOB_ID = E.JOB_ID
  ORDER BY E.EMPLOYEE_ID ;