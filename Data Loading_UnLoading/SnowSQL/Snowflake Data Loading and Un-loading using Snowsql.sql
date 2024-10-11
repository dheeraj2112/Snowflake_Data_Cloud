--##########################################################################################################################################
----> Snowflake Data Loading and Un-loading using Snowsql
--##########################################################################################################################################

--Snowsql command to connect to a specific connection defined in Snowsql config in ~Users/user/Snowsql/config

> snowsql -c edw

--##########################################################################################################################################
--PUT COMMAND for DATA Loading
--##########################################################################################################################################

--Creating the STAGE with File Format required

CREATE OR REPLACE STAGE ORDER_DATA   FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '\042') ;

CREATE OR REPLACE STAGE ORDER_DATA   FILE_FORMAT = my_csv_format;

--File FORMAT

CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
  type = csv
  field_delimiter = ','
  skip_header = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
;


--Creating the table for loading 

CREATE OR REPLACE TABLE STG_ORDER
    (O_ORDERKEY         NUMBER,
     O_CUSTKEY          NUMBER,
     O_ORDERSTATUS      STRING,
     O_TOTALPRICE       NUMBER,
     O_ORDERDATE        DATE,
     O_ORDERPRIORITY    STRING,
     O_CLERK            STRING,
     O_SHIPPRIORITY     NUMBER,
     O_COMMENT          STRING,
     FILENAME           STRING NOT NULL,
     FILE_ROW_SEQ       NUMBER NOT NULL,
     LDTS               STRING NOT NULL,
     RSCR               STRING NOT NULL);
	 
	 
	 
--Creating the export for the data loading activty from existing table and saving it as CSV file ( CSV Size: 552 MB, Compressed CSV : 158 MB for 5M records)

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS LIMIT 5000000
		
---connect to snowflake 
		
snowsql -c edw

--List the STAGE contents 
LIST @ORDER_DATA;

--PUT Command to push the Exported file from Local to the STAGE created above
 
PUT file://D:\Downloads\Orders_Data.csv @ORDER_DATA;

--PUT command results with the details and time elapsed

dheeraj2112#COMPUTE_WH@EDW.EDW>PUT file://D:\Downloads\Orders_Data.csv @ORDER_DATA;
+-----------------+--------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source          | target             | source_size | target_size | source_compression | target_compression | status   | message |
|-----------------+--------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| Orders_Data.csv | Orders_Data.csv.gz |   579286608 |   165828432 | NONE               | GZIP               | UPLOADED |         |
+-----------------+--------------------+-------------+-------------+--------------------+--------------------+----------+---------+

1 Row(s) produced. Time Elapsed: 96.110s

--List the STGAE contents after puting the file 

dheeraj2112#COMPUTE_WH@EDW.EDW>LIST @ORDER_DATA;
+-------------------------------+-----------+----------------------------------+------------------------------+
| name                          |      size | md5                              | last_modified                |
|-------------------------------+-----------+----------------------------------+------------------------------|
| order_data/Orders_Data.csv.gz | 165828432 | 8222d98c890c0b8c3224a1708f6a781d | Tue, 1 Oct 2024 12:34:45 GMT |
+-------------------------------+-----------+----------------------------------+------------------------------+

1 Row(s) produced. Time Elapsed: 1.652s



--Some erros due to file data having some values in DQ (along with a comma {,} in it) causing the erros. Double check the FIELD_OPTIONALLY_ENCLOSED_BY parameter accordingly
--Copy Command with erros 

COPY INTO STG_ORDER 
    FROM
        (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
         CURRENT_TIMESTAMP(), 'ORDER SYSTEM'
         FROM @ORDER_DATA);
		 

dheeraj2112#COMPUTE_WH@EDW.EDW>COPY INTO STG_ORDER
                                   FROM
                                       (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9
                                        ,METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
                                        CURRENT_TIMESTAMP(), 'ORDER SYSTEM'
                                        FROM @ORDER_DATA);
100038 (22018): Numeric value '$METADATA$FILENAME' is not recognized
  File 'Orders_Data.csv.gz', line 1, character 1
  Row 1, column $METADATA$FILENAME
  If you would like to continue loading when an error is encountered, use other values such as 'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please run 'info loading_data' in a SQL client.
dheeraj2112#COMPUTE_WH@EDW.EDW>

--Copy Command with SUCCESSFUL load details and elapsed time. 
 
dheeraj2112#COMPUTE_WH@EDW.EDW>COPY INTO STG_ORDER
                                   FROM
                                       (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9
                                        ,METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
                                        CURRENT_TIMESTAMP(), 'ORDER SYSTEM'
                                        FROM @ORDER_DATA);
+-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------+
| file                          | status | rows_parsed | rows_loaded | error_limit | errors_seen | first_error | first_error_line | first_error_character | first_error_column_name |
|-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------|
| order_data/Orders_Data.csv.gz | LOADED |     5000000 |     5000000 |           1 |           0 | NULL        |             NULL |
 NULL | NULL                    |
+-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------+
1 Row(s) produced. Time Elapsed: 27.083s
dheeraj2112#COMPUTE_WH@EDW.EDW>

--Load AGAIN (without TRUNCATE ON TARGET TABLE ) and it will not re-process the file again.

dheeraj2112#COMPUTE_WH@EDW.EDW>COPY INTO STG_ORDER
                                   FROM
                                       (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9
                                        ,METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
                                        CURRENT_TIMESTAMP(), 'ORDER SYSTEM'
                                        FROM @ORDER_DATA);
+---------------------------------------+
| status                                |
|---------------------------------------|
| Copy executed with 0 files processed. |
+---------------------------------------+
1 Row(s) produced. Time Elapsed: 0.514s
dheeraj2112#COMPUTE_WH@EDW.EDW>

--Truncate the table and Load the File again with specific files or pattern (use the FILES or PATTERN parameters accordingly in the copy command)

dheeraj2112#COMPUTE_WH@EDW.EDW>
                               COPY INTO STG_ORDER
                                                                  FROM
                                                                      (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9
                                                                       ,METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
                                                                       CURRENT_TIMESTAMP(), 'ORDER SYSTEM'
                                                                       FROM @ORDER_DATA)
																	   --FILES=('Orders_Data1.txt,'Orders_Data2.txt)
                                                                       PATTERN='.*Orders_Data.*';
+-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------+
| file                          | status | rows_parsed | rows_loaded | error_limit | errors_seen | first_error | first_error_line | first_error_character | first_error_column_name |
|-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------|
| order_data/Orders_Data.csv.gz | LOADED |     5000000 |     5000000 |           1 |           0 | NULL        |             NULL |
 NULL | NULL                    |
+-------------------------------+--------+-------------+-------------+-------------+-------------+-------------+------------------+-----------------------+-------------------------+
1 Row(s) produced. Time Elapsed: 26.517s
dheeraj2112#COMPUTE_WH@EDW.EDW>



--##########################################################################################################################################
--GET COMMAND to Un-load the data 
--##########################################################################################################################################

--List the Stage 

LIST @ORDER_DATA

--Snowsql list STAGE 

dheeraj2112#COMPUTE_WH@EDW.EDW>LIST @ORDER_DATA;
+-------------------------------+-----------+----------------------------------+------------------------------+
| name                          |      size | md5                              | last_modified                |
|-------------------------------+-----------+----------------------------------+------------------------------|
| order_data/Orders_Data.csv.gz | 165828432 | c92ce6d5a0a202be17a53af292d2e6fd | Tue, 1 Oct 2024 13:04:31 GMT |
+-------------------------------+-----------+----------------------------------+------------------------------+
1 Row(s) produced. Time Elapsed: 0.187s
dheeraj2112#COMPUTE_WH@EDW.EDW>

--GET command to Un-load the data from STAGE to Local Directory

dheeraj2112#COMPUTE_WH@EDW.EDW>GET @ORDER_DATA file://D:\Downloads;
+--------------------+-----------+------------+---------+
| file               |      size | status     | message |
|--------------------+-----------+------------+---------|
| Orders_Data.csv.gz | 165828427 | DOWNLOADED |         |
+--------------------+-----------+------------+---------+
1 Row(s) produced. Time Elapsed: 18.686s
dheeraj2112#COMPUTE_WH@EDW.EDW>

--GET command to Un-load the data from STAGE to Local Directory using PARALLEL and PATTERN parameters.

dheeraj2112#COMPUTE_WH@EDW.EDW>GET @ORDER_DATA file://D:\Downloads
                                   PARALLEL = 5
                                   PATTERN = '.*Orders_Data.*';
								   
+--------------------+-----------+------------+---------+
| file               |      size | status     | message |
|--------------------+-----------+------------+---------|
| Orders_Data.csv.gz | 165828427 | DOWNLOADED |         |
+--------------------+-----------+------------+---------+
1 Row(s) produced. Time Elapsed: 18.202s
dheeraj2112#COMPUTE_WH@EDW.EDW>

--Ctrl+D or !quit to exit Snowsql terminal