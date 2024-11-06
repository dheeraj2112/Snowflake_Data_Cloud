--######################################################################################################
-->  INTERNAL:: Automating Task Failure Email Notifications in Snowflake
--######################################################################################################


--Steps to Configure Email Alerts in Snowflake

--1. Verify Recipient Email on Snowflake

--2. Create Notification Integration

-- Create a notification integration for email alerts

CREATE OR REPLACE NOTIFICATION INTEGRATION INTERNAL_EMAIL_NOTIFY
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('dheeraj2112@gmail.com', 'dheerajworld2112@gmail.com')
COMMENT='Integration object for sending email notifications';


--For each email address in ALLOWED_RECIPIENTS, make sure that the email address has been verified. If you specify an email address that hasn’t been verified, 
--the CREATE NOTIFICATION INTEGRATION command fails with an error as seen below.
--ERROR
--Email recipients in the given list at indexes [2] are not allowed. Either these email addresses are not yet validated or do not belong to any user in the current account.

CREATE OR REPLACE NOTIFICATION INTEGRATION INTERNAL_EMAIL_NOTIFY
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('dheeraj2112@gmail.com')
COMMENT='Integration object for sending email notifications';

--Status
--Integration INTERNAL_EMAIL_NOTIFY successfully created.

--cannot be set for email that is internal alerts for both Tasks and PIPEs

--> ALTER TASK <name> SET ERROR_INTEGRATION = INTERNAL_EMAIL_NOTIFY;

--3. Use SYSTEM$SEND_EMAIL() to Send Alerts

--Leverage the built-in SYSTEM$SEND_EMAIL() stored procedure to dispatch email notification. This procedure takes parameters such as the integration object name, recipient email address, email subject, message content, and content type.

--i. With our notification integration in place, we can now proceed to create a procedure that handles failed tasks and triggers email notification.


--Create a procedure for sending email notifications for failed tasks

CREATE OR REPLACE PROCEDURE EDW.EDW_TEST.NOTIFY_INTERNAL_FAILURE_ALERT()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var sql_query = "SELECT DATABASE_NAME as DB_NAME,SCHEMA_NAME as SCHEMA_NAME,NAME as name,STATE AS status_message,ERROR_CODE as error_code, ERROR_MESSAGE as error_message,CONVERT_TIMEZONE('Asia/Kolkata', QUERY_START_TIME) as QUERY_START_TIME,CONVERT_TIMEZONE('Asia/Kolkata', COMPLETED_TIME) as COMPLETED_TIME , CURRENT_ACCOUNT() as Account FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY WHERE STATE = 'FAILED' AND SCHEDULED_TIME >= DATEADD(HOUR, -84, CURRENT_TIMESTAMP())";
var sqlstmt = snowflake.createStatement({ sqlText:sql_query });
var rs = sqlstmt.execute();
var msg = `<html><body><table border="1";table-layout="auto";width="100%"><tr><th>Database</th><th>Schema</th><th>Task Name</th><th>Status</th><th nowrap>Error Code</th><th>Error Message</th><th>Query Start Time</th><th>Query Completed Time</th></tr>`;
 
while (rs.next()) {
  var db_name = rs.getColumnValueAsString(1)
  var db_schema = rs.getColumnValueAsString(2);
  var name = rs.getColumnValueAsString(3);
  var status_message = rs.getColumnValueAsString(4);
  var error_code = rs.getColumnValueAsString(5);
  var error_message= rs.getColumnValueAsString(6);
  var starttime = rs.getColumnValueAsString(7);
  var endtime = rs.getColumnValueAsString(8);
  var account = rs.getColumnValueAsString(9);
  status_message = status_message.replace(/'/g, '');
  error_message = error_message.replace(/'/g, '');
  
  msg += '<tr><td>' + db_name + '</td><td>' + db_schema + '</td><td>' + name + '</td><td>' +  status_message + '</td><td>' + error_code + '</td><td nowrap>' + error_message + '</td><td nowrap>' + starttime + '</td><td nowrap>' + endtime + '</td></tr>' ;
}
 
msg += `</table></body></html>`;
 
 
var proc = "CALL SYSTEM$SEND_EMAIL('INTERNAL_EMAIL_NOTIFY', 'dheeraj2112@gmail.com', 'Task Failure Alert: Snowflake Account', '" + msg + "','text/html')";
 
var stmt = snowflake.createStatement({ sqlText:proc });
var rs2 = stmt.execute();
 
return " Email...!!! Sent Successfully....!!!!!!";
$$;


--'CALL EDW.EDW_TEST.NOTIFY_INTERNAL_FAILURE_ALERT();

--'--Customize the code as per your specific use case 

--ii. Create Alert for Task Failures

--Let’s tie everything together by creating an alert that monitors task failures and triggers our email alert procedure.

--Create an alert for task failures and adjust the alerts need based on the requirements

CREATE OR REPLACE ALERT EDW.EDW.TASK_FAILURE_ALERT
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 minute'
IF (EXISTS
(
       SELECT 1
       FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
       WHERE STATE = 'FAILED' AND
             SCHEDULED_TIME >= DATEADD(HOUR, -72, GETDATE())
))
THEN CALL EDW.EDW_TEST.NOTIFY_INTERNAL_FAILURE_ALERT();


--Resume the alerts

ALTER ALERT TASK_FAILURE_ALERT RESUME;

--Show the alerts 

SHOW ALERTS ;

created_on,name,database_name,schema_name,owner,comment,warehouse,schedule,state,condition,action,owner_role_type
2024-10-25 18:08:33.627 +0530,TASK_FAILURE_ALERT,EDW,EDW,ACCOUNTADMIN,,COMPUTE_WH,10 minute,started,"SELECT 1
       FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
       WHERE STATE = 'FAILED' AND
             SCHEDULED_TIME >= DATEADD(HOUR, -72, GETDATE())",CALL EDW.EDW_TEST.NOTIFY_INTERNAL_FAILURE_ALERT(), 

--Alert History using time range

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(SCHEDULED_TIME_RANGE_START =>dateadd('hour',-1,current_timestamp())))
ORDER BY SCHEDULED_TIME DESC;


--Alert History using Alert name 

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(ALERT_NAME=>'TASK_FAILURE_ALERT'))
ORDER BY SCHEDULED_TIME DESC;
