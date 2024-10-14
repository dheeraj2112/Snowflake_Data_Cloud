--#################################################################################################
-- Snowflake--> SRC to EDW_STG Pipelines using Snowflake Pipe 
--#################################################################################################

--STEP 0  ==> Suspend root task to make any changes/additions/modifications and then resume all child tasks first before the parent root task.

/*Setup Snowpipe to load data from files in a stage into staging tables. Once the PIPE(s) are created from below steps then use the refresh command to load STG tables manually.

Note: The REFRESH functionality is intended for short term use to resolve specific issues when Snowpipe fails to load a subset of files and is not intended for regular use.

The REFRESH functionality in this to trigger Snowpipe explicitly to load existing or scan for new files.  
In production environment, you'll likely enable AUTO_INGEST, connecting it with your cloud storage events (like AWS SNS) and process new files automatically.

ALTER PIPE PP_STG_REGIONS_LD  REFRESH;
ALTER PIPE PP_STG_COUNTRIES_LD  REFRESH;
ALTER PIPE PP_STG_LOCATIONS_LD  REFRESH;
ALTER PIPE PP_STG_DEPARTMENTS_LD  REFRESH;
ALTER PIPE PP_STG_JOBS_LD  REFRESH;
ALTER PIPE PP_STG_EMPLOYEES_LD  REFRESH;
ALTER PIPE PP_STG_JOB_HISTORY_LD  REFRESH;
         
*/

--#################################################################################################
--SRC to EDW_STG TABLES LOAD USING Snowpipe with the required dependencies
--#################################################################################################

USE EDW.EDW_STG;
	
--REGION LOAD

CREATE OR REPLACE PIPE PP_STG_REGIONS_LD
AS 
COPY INTO EDW.EDW_STG.REGIONS 
    FROM
        (SELECT $1::NUMBER,$2::VARCHAR2(25),MD5($2::VARCHAR2(25)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_REGIONS_DATA);
         
 --COUNTRIES LOAD
 
CREATE OR REPLACE PIPE PP_STG_COUNTRIES_LD
AS  
COPY INTO EDW.EDW_STG.COUNTRIES 
    FROM
        (SELECT $1::CHAR(2),$2::VARCHAR2(40),$3::NUMBER,MD5($2||$3::VARCHAR2),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_COUNTRIES_DATA);
		 
		 
 --LOCATIONS LOAD
 
CREATE OR REPLACE PIPE PP_STG_LOCATIONS_LD
AS  
COPY INTO EDW.EDW_STG.LOCATIONS
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(40),$3::VARCHAR2(12),$4::VARCHAR2(30),$5::VARCHAR2(25),$6::CHAR(2),MD5($2||$3||$4||$5||$6),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_LOCATIONS_DATA);
		 

 --DEPARTMENTS LOAD
 
CREATE OR REPLACE PIPE PP_STG_DEPARTMENTS_LD
AS  
COPY INTO EDW.EDW_STG.DEPARTMENTS 
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(30),$3::NUMBER(6),$4::NUMBER(4),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_DEPARTMENTS_DATA);
		 
 --JOBS LOAD
 
CREATE OR REPLACE PIPE PP_STG_JOBS_LD
AS  
COPY INTO EDW.EDW_STG.JOBS
    FROM
        (SELECT $1::VARCHAR2(10),$2::VARCHAR2(35),$3::NUMBER(6),$4::NUMBER(6),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(6)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_JOBS_DATA);
		 
		 
 --EMPLOYEES LOAD
 
CREATE OR REPLACE PIPE PP_STG_EMPLOYEES_LD
AS  
COPY INTO  EDW.EDW_STG.EMPLOYEES
    FROM
        (SELECT $1::NUMBER(6),$2::VARCHAR2(20),$3::VARCHAR2(25),$4::VARCHAR2(25),$5::VARCHAR2(20),$6::DATE,$7::VARCHAR2(10),$8::NUMBER(8,2),$9::NUMBER(2,2),$10::NUMBER(6),$11::NUMBER(4)
		,MD5($2||$3||$4||$5||DATE($6)::VARCHAR2(10)||$7||$8::VARCHAR2(10)||$9::VARCHAR2(4)||$10::VARCHAR2(6)||$11::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_EMPLOYEES_DATA);

 
 --JOB_HISTORY LOAD
 
CREATE OR REPLACE PIPE PP_STG_JOB_HISTORY_LD
AS  
COPY INTO EDW.EDW_STG.JOB_HISTORY
    FROM
        (SELECT $1::NUMBER(6),$2::DATE,$3::DATE,$4::VARCHAR2(10),$5::NUMBER(4),MD5(DATE($2)::VARCHAR2(10)||DATE($3)::VARCHAR2(10)||$4||$5::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_JOB_HISTORY_DATA);
		 


		 
--TASKS LOAD VALIDATIONS for STG TABLE(s) LOAD (only after all the children as well as parent tasks are resumed mentioned in step #1).

USE EDW.EDW_STG;

SELECT 'REGIONS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.REGIONS
UNION ALL 
SELECT 'COUNTRIES' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.COUNTRIES
UNION ALL 
SELECT 'LOCATIONS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.LOCATIONS 
UNION ALL 
SELECT 'DEPARTMENTS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.DEPARTMENTS
UNION ALL 
SELECT 'JOBS' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.JOBS
UNION ALL
SELECT 'EMPLOYEES' AS Table_Name, COUNT(1) AS TABLE_COUNTS FROM EDW.EDW_STG.EMPLOYEES
UNION ALL 
SELECT 'JOB_HISTORY' AS Table_Name, COUNT(1) TABLE_COUNTS FROM EDW.EDW_STG.JOB_HISTORY ;

--Truncate STG Tables statements for debugging and issues fixes, if needed.

USE EDW.EDW_STG;

TRUNCATE TABLE EDW.EDW_STG.REGIONS;
TRUNCATE TABLE EDW.EDW_STG.COUNTRIES;
TRUNCATE TABLE EDW.EDW_STG.LOCATIONS;
TRUNCATE TABLE EDW.EDW_STG.DEPARTMENTS;
TRUNCATE TABLE EDW.EDW_STG.JOBS;
TRUNCATE TABLE EDW.EDW_STG.EMPLOYEES;
TRUNCATE TABLE EDW.EDW_STG.JOB_HISTORY;