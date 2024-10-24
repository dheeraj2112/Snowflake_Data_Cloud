--######################################################################################################
-->   Load and Query JSON data in Snowflake
--######################################################################################################

-- The sample file is authors.json for this and included in the github repo and can be accessed from below link.

-- https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/1.%20Snowflake%20Hands-on%20tutorials/8.%20Snowflake%20Semi-Structure%20Data%20Module/authors.json

-- authors.json

-- Use the appropriate contexts, if applicable

USE EDW.EDW_TEST;

--Create a Snowflake Internal named Stage

CREATE OR REPLACE STAGE AUTHORS_INTERNAL_STAGE_JSON;


--SnowSQL CLI to connect to Snowflake environment

C:\Users\dheer>snowsql
* SnowSQL * v1.3.1
Type SQL statements or !help
dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>

-- Load data from local machine into Internal Stage using SnowSQL
-- ‪File Loaction--> C:\Users\dheer\Downloads\authors.json

dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>put file://C:\Users\dheer\Downloads\authors.json @authors_internal_stage_json;
+--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source       | target          | source_size | target_size | source_compression | target_compression | status   | message |
|--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------|
| authors.json | authors.json.gz |        1332 |         320 | NONE               | GZIP               | UPLOADED |         |
+--------------+-----------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 1.918s
dheeraj2112#COMPUTE_WH@EDW.EDW_TEST>

-- Verify the file in Internal Stage using List command

LIST @authors_internal_stage_json;

-- Results
name	                                    size	       md5	                                last_modified
authors_internal_stage_json/authors.json.gz	320  	b83f4f1054f763aeb82cf7185f482900	Mon, 21 Oct 2024 15:18:26 GMT

--######################################################################################################
-- Loading JSON data from Snowflake internal Stage into database table
--######################################################################################################


-- Create a JSON file format in Snowflake. This can be attached to Stage directly or can be used in the COPY INTO statement.

CREATE OR REPLACE FILE FORMAT json_format
    type = json
    strip_outer_array = true
;

-- Results (status)
-- File format JSON_FORMAT successfully created.

-- Note
-- We have used an option called STRIP_OUTER_ARRAY for this load. It helps in removing the outer set of square brackets [ ] when loading the data,
-- separating the initial array into multiple lines. Else the entire JSON data gets loaded into single record instead of multiple records.

-- Create database table to load JSON data

CREATE OR REPLACE TABLE Authors (
JSON_DATA VARIANT
);

--Load data from Internal Stage into database table using COPY command

COPY INTO Authors
FROM @authors_internal_stage_json/authors.json
FILE_FORMAT = (format_name = json_format);

-- Results 

file	                                        status	rows_parsed	 rows_loaded	error_limit 	errors_seen	 first_error	first_error_line	first_error_character	first_error_column_name
authors_internal_stage_json/authors.json.gz 	LOADED	    3	            3	       1	           0	      null              null                null                        null



-- Querying JSON data from Snowflake database table

SELECT * FROM Authors;


-- The data from the Authors table can be queried directly as shown below. By using the STRIP_OUTER_ARRAY option, we were able remove this initial array [] 
-- and treat each object in the array as a row in Snowflake. Hence each author object loaded as a separate row.


-- The individual elements in the column JSON_DATA can be queried using standard : notation as shown below.

SELECT 
    JSON_DATA:AuthorName,
    JSON_DATA:Category
FROM Authors;

-- or bracket notation as seen below 

SELECT
    JSON_DATA['AuthorName'],
    JSON_DATA['Category']
FROM
    Authors;

-- The data in the Category can be further drilled down and required elements information can be fetched as shown below.

SELECT
    JSON_DATA:AuthorName,
    JSON_DATA:Category[0]:CategoryName,
    JSON_DATA:Category[1]:CategoryName
FROM Authors;


-- The outer quotes in the column data can be removed by using :: notation which lets you define the end data type of the values being retrieved

SELECT
JSON_DATA:AuthorName::string,
JSON_DATA:Category[0]:CategoryName::string,
JSON_DATA:Category[1]:CategoryName::string
FROM Authors;


-- Further more details of author can be drilled down as shown below.

SELECT
    JSON_DATA:AuthorName::string,
    JSON_DATA:Category[0]:CategoryName::string,
    JSON_DATA:Category[0]:Genre[0]:GenreName::string,
    JSON_DATA:Category[0]:Genre[1]:GenreName::string,
    JSON_DATA:Category[1]:CategoryName::string,
    JSON_DATA:Category[1]:Genre[0]:GenreName::string,
    JSON_DATA:Category[1]:Genre[1]:GenreName::string
FROM Authors;


-- Unfortunately, this approach is not ideal. As the data increases, you need to add additional levels of category and genre in the query statement specifying the index values. Using the : and [] 
-- notation alone is not sufficient to dynamically get every object in an array.

--######################################################################################################
--  Flattening Arrays in JSON data
--######################################################################################################

-- Flattening is a process of unpacking the semi-structured data into a columnar format by converting arrays into different rows of data.

-- Using the LATERAL FLATTEN function we can explode arrays into individual JSON objects. The input for the function is the array in the JSON structure that 
-- we want to flatten (In the example shown below, the array is Category). 
-- The flattened output is stored in a VALUE column. The individual elements from unpacked array can be accessed through the VALUE column as shown below.


SELECT
JSON_DATA:AuthorName::string AS Author,
VALUE:CategoryName::string AS CategoryName
FROM Authors
,LATERAL FLATTEN (input => JSON_DATA:Category);

-- Using an alias for LATERAL FLATTEN and access the VALUE using dot(.) notation

SELECT
JSON_DATA:AuthorName::string AS Author,
LF_CATEGORY.VALUE:CategoryName::string AS CategoryName
FROM Authors
,LATERAL FLATTEN (input => JSON_DATA:Category) AS LF_CATEGORY;

-- When there are multiple arrays which you need to flatten, it is mandatory to pass an alias to every input array. The VALUE column also should be used along with the alias you passed to the input array.

-- In our example, we need to flatten the Category, Genre and Novel arrays to get the desired output. Also note that the Novel array is present inside Genre array which is present inside Category array. 
-- So the flattened array output VALUE becomes input for the array present inside it.

SELECT
JSON_DATA:AuthorName::string AS  Author_Name,
LF_Category.VALUE:CategoryName::string AS  Category_Name,
LF_Genre.VALUE:GenreName::string AS Genre_Name,
LF_Novel.VALUE:Novel::string AS Novel_Name,
LF_Novel.VALUE:Sales:: number AS Sales_in_Millions
FROM Authors
,LATERAL FLATTEN (input => JSON_DATA:Category) AS LF_Category
,LATERAL FLATTEN (input => LF_Category.VALUE:Genre) AS LF_Genre
,LATERAL FLATTEN (input => LF_Genre.VALUE:Novel) AS LF_Novel ;

--######################################################################################################
-- Summary
--######################################################################################################

-- Snowflake supports loading semi structured data files from external and internal stages into  database tables. 
-- Once the data is loaded into the table, it is important to understand the data structure and identify the arrays to flatten which provides the required output.
-- The transformed data can then be loaded into another database tables with proper field names and data types easily. 

-- This is a good example of Snowflake’s ELT features which is extremely helpful as the semi structured data can be easily transformed once loaded without 
-- the help of external ETL tools.

-- <EOD>>