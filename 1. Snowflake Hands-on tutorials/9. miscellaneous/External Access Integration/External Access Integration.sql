--Use database and schema

USE EDW.EDW;

--Use ACCOUNTADMIN ROLE 

USE ROLE ACCOUNTADMIN;

--Create the Network Rule

CREATE OR REPLACE NETWORK RULE GOREST_NET_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('gorest.co.in');


--Create the External Access Integration referencing the Network Rule created

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GOREST_EXT_INTEGRATION
ALLOWED_NETWORK_RULES = (GOREST_NET_RULE)
ENABLED = TRUE;

--Create the function to get the users data by using the external_access_integrations

CREATE OR REPLACE FUNCTION get_users_data()
RETURNS TABLE (id varchar, name varchar, email varchar, gender varchar, status varchar)
language python
runtime_version = 3.11
handler = 'getUsersData'
external_access_integrations = (GOREST_EXT_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
AS
$$
import requests
import json

class getUsersData:
    def process(self):
    
        baseurl='https://gorest.co.in/public/v2/users'
        response = requests.get(baseurl)

        if(response.ok):
            jsonData = json.loads(response.content)

            for userData in jsonData:
                yield(userData['id'],userData['name'],userData['email'],userData['gender'],userData['status'])
           
$$;

--get user data items from the function

SELECT * FROM TABLE(get_users_data());