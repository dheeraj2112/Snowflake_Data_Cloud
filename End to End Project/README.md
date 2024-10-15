**Snowflake End-to-End Project Overview and Summary**

1. Project Overview
The goal of this project is to implement a data warehousing solution using Snowflake, enabling efficient data storage, processing, and analysis. This end-to-end project covers data ingestion, transformation, storage, and reporting, aiming to provide a unified view of business data for better decision-making.

1.1) Pre-requisite:

i. Valid Snowflake Account

ii. Snowsql installed,configured and ready to use with the required connections.

iii. On-Prem Oracle db HR schema objects exported in CSV format which are the source files for this project. The objects export can obtained from step #2 mentioned below, if not available already.

1.2) Reference Architecture Diagram

![image](https://github.com/user-attachments/assets/cfc6e941-e036-4f81-a91c-870f739576bc)


2.  Source and Target
   
Source: Source is Oracle on-prem HR schema objects exports in CSV format. The source files can be used from below link, if not available handy.

https://github.com/dheeraj2112/Snowflake_Data_Cloud/tree/master/End%20to%20End%20Project/2.Source%20Files

Target: Target is Snowflake having both EDW_STG and EDW Layers. The DDLs can be found in the repo. 

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/3.%20Snowflake%20EDW_STG_EDW_Layers_DDLs/IICS_ORA_SNFLK_EDW_DDL.sql

3.  ETL/ELT Data Flows using Snowflake Tasks or Snow PIPEs.

**i.** Source (CSV Files) to EDW_STG (Snowflake) : The code repo can be found here to Create the STAGE(s) and FILE FORMATS then run the PUT command from SnowSQL command line utility to load these CSV files into the respective STAGE(s).

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/0.%20Snowflake%20Data%20Loading%20PREP%20Scripts%20ALL.sql

**The code repo for loading the EDW_STG tables can be found here using Snowflake Tasks.**

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/1.%20Snowflake%20SRC%20to%20EDW_STG%20Pipelines%20using%20Snowflake%20Tasks.sql

Optional: SRC to EDW_STG data pipelines can be implemented using Snowflake feature such Snowflake SnowPipe as well.We can Setup Snowpipe to load data from files in a stage into staging tables. Once the PIPE(s) are created  then use the refresh command to load STG tables manually.

Note: The REFRESH functionality is intended for short term use to resolve specific issues when Snowpipe fails to load a subset of files and is not intended for regular use.
The REFRESH functionality in the Snowpipe is explicitly to load existing or scan for new files. In a production environment, you'll likely enable AUTO_INGEST, connecting it with your cloud storage events (like AWS SNS) and process new files automatically.The code repo can be found here.

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/1.1%20Snowflake%20SRC%20to%20EDW_STG%20Pipelines%20using%20Snowflake%20Pipe.sql

**ii.** EDW_STG (Snowflake) to EDW(Snowflake) : Using SCD Type-2 (Using MERGE SQL statements), the code repo can be found here.

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/2.%20Snowflake%20EDW_STG%20to%20EDW%20Pipelines%20using%20Snowflake%20Tasks.sql

**iii.** Orchestration and scheduling : Using Snowflake Tasks as per needed schedule.
   
4.  Load Validations

The code repo can be found here to validate the SRC to EDW_STG tables load as well as EDW_STG to EDW tables load.

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/3.%20EDW_STG%20and%20EDW%20Pipelines%20Load%20Validations.sql
   
5.  Analysis and Reporting (ETL Code)

Views or Dynamic tables can be leveraged accordingly for the analytics and reporting or visualization needs. This can be done from Snowflake specific features as per need.

https://github.com/dheeraj2112/Snowflake_Data_Cloud/blob/master/End%20to%20End%20Project/4.Snowflake%20Scripts/4.%20EDW_EXTR%20Views%20and%20Dynamic%20Tables%20for%20Reporting.sql
   
6.  Next Steps **-->**
    
i. Incremental/CDC logic handling within Snowflake Data Cloud by leveraging the Streams coupled with TASKs to process them as needed.

ii. Addtional integration with AWS/Azure/GCP in place of Snowflake SnowSQL utility to implement this project. Just SRC to EDW_STG data pipelines logic to be implemented as EDW_STG to EDW data pipelines are already in place within Snowflake as detailed out above.

**ENDOFDOCUMENT**
