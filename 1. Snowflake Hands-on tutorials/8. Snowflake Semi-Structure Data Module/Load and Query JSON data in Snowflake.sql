--######################################################################################################
-->   Load and Query JSON data in Snowflake
--######################################################################################################

--Use the appropriate context

USE EDW.EDW_TEST;


--Create a Snowflake Internal named Stage

CREATE OR REPLACE STAGE authors_internal_stage_json;


--SnowSQL 

C:\Users\dheer>snowsql
* SnowSQL * v1.3.1
Type SQL statements or !help
dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>

--Load data from local machine into Internal Stage using SnowSQL
-- ‪File Loaction--> C:\Users\dheer\Downloads\authors.json

dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>put file://C:\Users\dheer\Downloads\authors.json @authors_internal_stage_json;
+--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source       | target          | source_size | target_size | source_compression | target_compression | status   | message |
|--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------|
| authors.json | authors.json.gz |        1332 |         320 | NONE               | GZIP               | UPLOADED |         |
+--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 1.918s
dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>

--Verify the file in Internal Stage using List command

LIST @authors_internal_stage_json;

--Results
name	                                    size	       md5	                                last_modified
authors_internal_stage_json/authors.json.gz	320  	b83f4f1054f763aeb82cf7185f482900	Mon, 21 Oct 2024 15:18:26 GMT

--######################################################################################################
-- Loading JSON data from Snowflake internal Stage into database table
--######################################################################################################


--Create a JSON file format in Snowflake

CREATE OR REPLACE FILE FORMAT json_format
    type = json
    strip_outer_array = true
;

--Results (status)
--File format JSON_FORMAT successfully created.

--Note
--We have used an option called STRIP_OUTER_ARRAY for this load. It helps in removing the outer set of square brackets [ ] when loading the data,
--separating the initial array into multiple lines. Else the entire JSON data gets loaded into single record instead of multiple records.

--Create database table to load JSON data

CREATE OR REPLACE TABLE Authors (
JSON_DATA VARIANT
);

--Load data from Internal Stage into database table using COPY command

COPY INTO Authors
FROM @authors_internal_stage_json/authors.json
FILE_FORMAT = (format_name = json_format);

--Results 

file	                                        status	rows_parsed	 rows_loaded	error_limit 	errors_seen	 first_error	first_error_line	first_error_character	first_error_column_name
authors_internal_stage_json/authors.json.gz 	LOADED	    3	            3	       1	           0	      null              null                null                        null



--Querying JSON data from Snowflake database table

SELECT * FROM Authors;


--The data from the Authors table can be queried directly as shown below. By using the STRIP_OUTER_ARRAY option, we were able remove this initial array [] 
--and treat each object in the array as a row in Snowflake. Hence each author object loaded as a separate row.


--The individual elements in the column JSON_DATA can be queried using standard : notation as shown below.

SELECT 
    JSON_DATA:AuthorName,
    JSON_DATA:Category
FROM Authors;

--or bracket notation as seen below 

SELECT
    JSON_DATA['AuthorName'],
    JSON_DATA['Category']
FROM
    Authors;

--The data in the Category can be further drilled down and required elements information can be fetched as shown below.

SELECT
    JSON_DATA:AuthorName,
    JSON_DATA:Category[0]:CategoryName,
    JSON_DATA:Category[1]:CategoryName
FROM Authors;


--The outer quotes in the column data can be removed by using :: notation which lets you define the end data type of the values being retrieved

SELECT
JSON_DATA:AuthorName::string,
JSON_DATA:Category[0]:CategoryName::string,
JSON_DATA:Category[1]:CategoryName::string
FROM Authors;


--Further more details of author can be drilled down as shown below.

SELECT
    JSON_DATA:AuthorName::string,
    JSON_DATA:Category[0]:CategoryName::string,
    JSON_DATA:Category[0]:Genre[0]:GenreName::string,
    JSON_DATA:Category[0]:Genre[1]:GenreName::string,
    JSON_DATA:Category[1]:CategoryName::string,
    JSON_DATA:Category[1]:Genre[0]:GenreName::string,
    JSON_DATA:Category[1]:Genre[1]:GenreName::string
FROM Authors;


--Unfortunately, this approach is not ideal. As the data increases, you need to add additional levels of category and genre in the query statement specifying the index values. Using the : and [] 
--notation alone is not sufficient to dynamically get every object in an array.