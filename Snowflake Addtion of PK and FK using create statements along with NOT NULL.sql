--###############################################################################################
--1. Snowflake Addtion of PK and FK using create statements along with NOT NULL 
--###############################################################################################

CREATE OR REPLACE TABLE DEPARTMENTS2(
    DEPARTMENT_ID      NUMBER(4, 0)    NOT NULL,
    DEPARTMENT_NAME    VARCHAR(30)     NOT NULL,
    MANAGER_ID         NUMBER(6, 0),
    LOCATION_ID        NUMBER(4, 0),
    CONSTRAINT DEPT_ID_PK PRIMARY KEY (DEPARTMENT_ID)
);


CREATE OR REPLACE TABLE EMPLOYEES2(
    EMPLOYEE_ID       NUMBER(6, 0)    NOT NULL,
    FIRST_NAME        VARCHAR(20),
    LAST_NAME         VARCHAR(25)     NOT NULL,
    EMAIL             VARCHAR(25)     NOT NULL,
    PHONE_NUMBER      VARCHAR(20),
    HIRE_DATE         DATE            NOT NULL,
    JOB_ID            VARCHAR(10)     NOT NULL,
    SALARY            NUMBER(8, 2),
    COMMISSION_PCT    NUMBER(2, 2),
    MANAGER_ID        NUMBER(6, 0),
    DEPARTMENT_ID     NUMBER(4, 0),
    CONSTRAINT EMP_EMP_ID_PK PRIMARY KEY (EMPLOYEE_ID),
    CONSTRAINT DEPT_MGR_FK  FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES2(EMPLOYEE_ID)
);


--###############################################################################################
--2. Addtion of PK and FK using alter  statements along with NOT NULL 
--###############################################################################################
-- TABLE: DEPARTMENTS 
--

CREATE OR REPLACE TABLE DEPARTMENTS(
    DEPARTMENT_ID      NUMBER(4, 0)    NOT NULL,
    DEPARTMENT_NAME    VARCHAR(30)     NOT NULL,
    MANAGER_ID         NUMBER(6, 0),
    LOCATION_ID        NUMBER(4, 0)
)
;

--- NOT NULL addition to MANAGER_ID:: Column 'MANAGER_ID' contains null values. Not null constraint cannot be added.



ALTER TABLE DEPARTMENTS MODIFY LOCATION_ID NUMBER(6, 0);
ALTER TABLE DEPARTMENTS ALTER LOCATION_ID  NOT NULL ;

-- 
-- TABLE: EMPLOYEES 
--

CREATE OR REPLACE TABLE EMPLOYEES(
    EMPLOYEE_ID       NUMBER(6, 0)    NOT NULL,
    FIRST_NAME        VARCHAR(20),
    LAST_NAME         VARCHAR(25)     NOT NULL,
    EMAIL             VARCHAR(25)     NOT NULL,
    PHONE_NUMBER      VARCHAR(20),
    HIRE_DATE         DATE            NOT NULL,
    JOB_ID            VARCHAR(10)     NOT NULL,
    SALARY            NUMBER(8, 2),
    COMMISSION_PCT    NUMBER(2, 2),
    MANAGER_ID        NUMBER(6, 0),
    DEPARTMENT_ID     NUMBER(4, 0)
);

--Addtion of PK and FK using ALTER stamenets 
-----------------PK 
-- 
-- TABLE: DEPARTMENTS 
--

ALTER TABLE DEPARTMENTS ADD 
    CONSTRAINT DEPT_ID_PK PRIMARY KEY (DEPARTMENT_ID)
;


-- 
-- TABLE: EMPLOYEES 
--

ALTER TABLE EMPLOYEES ADD 
    CONSTRAINT EMP_EMP_ID_PK PRIMARY KEY (EMPLOYEE_ID);
	
------------------FK 

-- 
-- TABLE: DEPARTMENTS 

ALTER TABLE DEPARTMENTS ADD CONSTRAINT DEPT_MGR_FK 
    FOREIGN KEY (MANAGER_ID)
    REFERENCES EMPLOYEES(EMPLOYEE_ID) ;