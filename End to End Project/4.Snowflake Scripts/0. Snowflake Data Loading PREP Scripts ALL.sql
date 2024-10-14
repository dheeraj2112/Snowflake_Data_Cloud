--#################################################################################################
-- Snowflake--> Data Loading PREP steps 
--#################################################################################################

--Use the context switch to the desired schema, role or Warehouse as required.

USE EDW.EDW_STG;

--Create a file format required by data load process into STG tables from corresponding CSV file.

CREATE OR REPLACE FILE FORMAT EDW_CSV_FRMT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042';

--Create the STAGE(s) required by data load process which will hold the files onto Cloud Storage or the Internal Stage.

CREATE OR REPLACE STAGE HR_REGIONS_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_COUNTRIES_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_LOCATIONS_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_DEPARTMENTS_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_JOBS_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_EMPLOYEES_DATA FILE_FORMAT = EDW_CSV_FRMT;
CREATE OR REPLACE STAGE HR_JOB_HISTORY_DATA FILE_FORMAT = EDW_CSV_FRMT;


---Install and configure the Snowsql and connect to snowflake using named edw_stg connection after modifying the config file as per connections required.
--Default path for SnowSQL config file is --> C:\<Users>\<\.snowsql

--> to get the SnowSQL version

snowsql -v or snowsql --version	     

C:\Users\dheer>snowsql -v
Version: 1.3.1

--> to connect to edw_stg named connection as defined in the config file.

snowsql -c edw_stg                   

C:\Users\dheer>snowsql -c edw_stg
* SnowSQL * v1.3.1
Type SQL statements or !help
dheeraj2112#COMPUTE_WH@EDW.EDW_STG>

--List the STAGE contents 
--Nothing as of now as we haven't loaded anything yet.

LIST @HR_REGIONS_DATA;
LIST @HR_COUNTRIES_DATA; 
LIST @HR_LOCATIONS_DATA; 
LIST @HR_DEPARTMENTS_DATA; 
LIST @HR_JOBS_DATA; 
LIST @HR_EMPLOYEES_DATA; 
LIST @HR_JOB_HISTORY_DATA;   


--SnowSQL PUT Command(s) to push the exported file(s) from Local source directory to the STAGE(s) locations created above. 
--Correct the DIRECTORY path according to your path to point these source files where they are saved as you downloaded earlier.

 
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_REGIONS.csv' @HR_REGIONS_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_COUNTRIES.csv' @HR_COUNTRIES_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_LOCATIONS.csv' @HR_LOCATIONS_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_DEPARTMENTS.csv' @HR_DEPARTMENTS_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_JOBS.csv' @HR_JOBS_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_EMPLOYEES.csv' @HR_EMPLOYEES_DATA;
PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_JOB_HISTORY.csv' @HR_JOB_HISTORY_DATA;


--A successful PUT command will look like as seen below. It will have the details as well time taken for the UPLOAD process to the STAGE from Local Source folder.

dheeraj2112#COMPUTE_WH@EDW.EDW_STG>PUT 'file://D:\\INYDZP\\Snowflake\\End to End Project\\Source Files\\HR_REGIONS.csv' @HR_REGIONS_DATA;

+----------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source         | target            | source_size | target_size | source_compression | target_compression | status   | message |
|----------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| HR_REGIONS.csv | HR_REGIONS.csv.gz |          79 |         112 | NONE               | GZIP               | UPLOADED |         |
+----------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 2.119s


--Reference COPY Command(s) to Load/Validate Data (To be used directly in Tasks or Pipes which are mentioned in next steps for loading process)
--into Staging tables by converting appropriate data types during the data load process itself wherever required as per DDLs.

--REGIONS LOAD
 
COPY INTO EDW.EDW_STG.REGIONS 
    FROM
        (SELECT $1::NUMBER,$2::VARCHAR2(25),MD5($2::VARCHAR2(25)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_REGIONS_DATA);
		 
--COUNTRIES LOAD 
		 
COPY INTO EDW.EDW_STG.COUNTRIES 
    FROM
        (SELECT $1::CHAR(2),$2::VARCHAR2(40),$3::NUMBER,MD5($2||$3::VARCHAR2),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_COUNTRIES_DATA);

--LOCATIONS LOAD 

COPY INTO EDW.EDW_STG.LOCATIONS
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(40),$3::VARCHAR2(12),$4::VARCHAR2(30),$5::VARCHAR2(25),$6::CHAR(2),MD5($2||$3||$4||$5||$6),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_LOCATIONS_DATA);


--DEPARTMENTS LOAD

COPY INTO EDW.EDW_STG.DEPARTMENTS 
    FROM
        (SELECT $1::NUMBER(4),$2::VARCHAR2(30),$3::NUMBER(6),$4::NUMBER(4),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_DEPARTMENTS_DATA);
		 
--JOBS LOAD		

COPY INTO EDW.EDW_STG.JOBS
    FROM
        (SELECT $1::VARCHAR2(10),$2::VARCHAR2(35),$3::NUMBER(6),$4::NUMBER(6),MD5($2||$3::VARCHAR2(6)||$4::VARCHAR2(6)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_JOBS_DATA);
		 
--EMPLOYEES LOAD 

COPY INTO  EDW.EDW_STG.EMPLOYEES
    FROM
        (SELECT $1::NUMBER(6),$2::VARCHAR2(20),$3::VARCHAR2(25),$4::VARCHAR2(25),$5::VARCHAR2(20),$6::DATE,$7::VARCHAR2(10),$8::NUMBER(8,2),$9::NUMBER(2,2),$10::NUMBER(6),$11::NUMBER(4)
		,MD5($2||$3||$4||$5||DATE($6)::VARCHAR2(10)||$7||$8::VARCHAR2(10)||$9::VARCHAR2(4)||$10::VARCHAR2(6)||$11::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_EMPLOYEES_DATA);
		 
--JOB_HISTORY LOAD 

COPY INTO EDW.EDW_STG.JOB_HISTORY
    FROM
        (SELECT $1::NUMBER(6),$2::DATE,$3::DATE,$4::VARCHAR2(10),$5::NUMBER(4),MD5(DATE($2)::VARCHAR2(10)||DATE($3)::VARCHAR2(10)||$4||$5::VARCHAR2(4)),'EDW'::VARCHAR2(32),CURRENT_TIMESTAMP::TIMESTAMP_NTZ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ
         FROM @HR_JOB_HISTORY_DATA);