**Snowflake End-to-End Project Overview and Summary**

1. Project Overview
The goal of this project is to implement a data warehousing solution using Snowflake, enabling efficient data storage, processing, and analysis. This end-to-end project covers data ingestion, transformation, storage, and reporting, aiming to provide a unified view of business data for better decision-making.

1.1) Pre-requisite:

i. Valid Snowflake Account

ii. Snowsql configured and ready to use with required connections

iii. On-Prem Oracle db (configured as an ODBC connection in IICS) with HR schema objects. The objects with data can be created from #2 mentioned below, if not available already.

1.2) Reference Architecture Diagram


2.  Source and Target
   
Source: Source is Oracle on-prem HR schema. The script can be used to generate that data set, if not available handy. 

Target: Target is Snowflake having both EDW_STG and EDW Layers. The DDLs can be found in the repo. 

3.  ETL/ELT Data Flows using Mappings and Mapping Tasks
   
5.  ETL Code using IICS
   
7.  Analysis and Reporting (ETL Code)
   
9.  Next Steps -->
    
i. Enchance the existing EDW_STG to EDW pipelines in ELT(Extract, Load, Transform) mode within IICS.

ii.Incremental/CDC logic handling ( either having some audit framework or IICS specific CDC handling features with { SETVARIABLE option / in-built $LastRunDate or $LastRunTime variables})

iii. Build the EDW_STG to EDW data pipelines using Snowflake features as mentioned in the optional section of #3.

iv. Addtinal integration with AWS/Azure/GCP in place of Informatica to implemnt this project. Juust SRC to EDW_STG data pipelines logic to be implemneted as EDW_STG to EDW data pipelines are already in place within Snowflake as mentioned in point #ii above.

**ENDOFDOCUMENT**
