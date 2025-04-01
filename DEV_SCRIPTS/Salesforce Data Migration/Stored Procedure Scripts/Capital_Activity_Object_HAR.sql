CREATE OR REPLACE PROCEDURE HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY_LOAD_HAR()
RETURNS VARCHAR(500)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$

DECLARE email_list STRING;
    email_count NUMBER;


BEGIN
    -- Author:	Bharath
	-- Created: 03/11/2025
	-- Description: Loads data into Capital_Activity__c HARMONIZED table
	-- Modifications:03/11/2025 - Created the Procedure

    
INSERT INTO HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY__C(
Id,
Investor_ID__c,
Fund__c,
Capital_Activity_Date__c,
Capital_Activity_Type__c,
Amount__c,
Share_Class__c,
Transfer__c,
CREATEDDATE,
LASTMODIFIEDDATE
)
SELECT
source.Id__c,
source.Investor_ID_c__c,
source.Fund_c__c,
source.Capital_Activity_Date_c__c,
source.Capital_Activity_Type_c__c,
source.Amount_c__c,
source.Share_Class_c__c,
source.Transfer_C__c,
source.CreatedDate__c,
source.LastModifiedDate__c
FROM RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C source
LEFT JOIN HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY__C target ON source.Id__c = target.Id
WHERE target.Id IS NULL;

INSERT INTO HARMONIZED_DEV.AUDIT_LOGS.AUDIT (
        SOURCE,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        ROW_COUNT,
        LOAD_TIMESTAMP,
        PROCEDURE_NAME,
        STATUS,
    ERROR_MESSAGE
    ) 
    VALUES(
        'SALESFORCE_RAW_DEV',
        'HARMONIZED_DEV',
        'DEV_HAR_SALESFORCE',
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD_HAR',
        'INSERT SUCCESS',
        NULL
    );


UPDATE HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY__C target
SET
    target.Id=source.Id__c,
    target.Investor_ID__c=source.Investor_ID_c__c,
    target.Fund__c=source.Fund_c__c,
    target.Capital_Activity_Date__c=source.Capital_Activity_Date_c__c,
    target.Capital_Activity_Type__c=source.Capital_Activity_Type_c__c,
    target.Amount__c=source.Amount_c__c,
    target.Share_Class__c=source.Share_Class_c__c,
    target.Transfer__c=source.Transfer_C__c,
    target.CreatedDate = source.CreatedDate__c,
    target.LastModifiedDate = source.LastModifiedDate__c

FROM RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C source
WHERE source.Id__c = target.Id AND source.LastModifiedDate__c > target.LastModifiedDate;


INSERT INTO HARMONIZED_DEV.AUDIT_LOGS.AUDIT (
        SOURCE,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        ROW_COUNT,
        LOAD_TIMESTAMP,
        PROCEDURE_NAME,
        STATUS,
    ERROR_MESSAGE
    ) 
    VALUES(
        'SALESFORCE_RAW_DEV',
        'HARMONIZED_DEV',
        'DEV_HAR_SALESFORCE',
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD_HAR',
        'UPDATE SUCCESS',
        NULL
    );



DELETE FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY__C
WHERE ID IN 
(SELECT target.ID
FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.CAPITAL_ACTIVITY__C target
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C source
ON target.ID = source.ID__c
WHERE source.ID__c IS NULL);

INSERT INTO HARMONIZED_DEV.AUDIT_LOGS.AUDIT (
        SOURCE,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        ROW_COUNT,
        LOAD_TIMESTAMP,
        PROCEDURE_NAME,
        STATUS,
    ERROR_MESSAGE
    ) 
    VALUES(
        'SALESFORCE_RAW_DEV',
        'HARMONIZED_DEV',
        'DEV_HAR_SALESFORCE',
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD_HAR',
        'DELETE SUCCESS',
        NULL
    );


EXCEPTION
    WHEN OTHER THEN
        SELECT COUNT(*) INTO :email_count FROM RAW_DEV.DEV_RAW_MERAKI.EMAIL WHERE flag = TRUE;
        IF (:email_count > 0) THEN
    
             SELECT ARRAY_TO_STRING(ARRAY_AGG(email_id),',')
             into email_list
             from RAW_DEV.DEV_RAW_MERAKI.EMAIL 
             where flag =TRUE;
             call system$send_email(
                'email_integration',
                :email_list,
                 'Email Alert: Error while executing CAPITAL_ACTIVITY_LOAD_HAR Procedure',
              :sqlerrm
             );
        END IF;
        
        INSERT INTO HARMONIZED_DEV.AUDIT_LOGS.AUDIT (
        SOURCE,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        ROW_COUNT,
        LOAD_TIMESTAMP,
        PROCEDURE_NAME,
        STATUS,
    ERROR_MESSAGE
    ) 
    VALUES(
        'SALESFORCE_RAW_DEV',
        'HARMONIZED_DEV',
        'DEV_HAR_SALESFORCE',
        'CAPITAL_ACTIVITY__C',
        0,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD_HAR',
        'FAILURE',
        :SQLERRM
    );


END;
$$;