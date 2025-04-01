CREATE OR REPLACE PROCEDURE HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT_LOAD_HAR()
RETURNS VARCHAR(500)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$

DECLARE email_list STRING;
	email_count NUMBER;
    
BEGIN
    -- Author:	Bharath
	-- Created: 03/17/2025
	-- Description: Loads data into ACCOUNT HARMONIZED table
	-- Modifications:03/17/2025 - Created the Procedure

    
INSERT INTO HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT(
    ID,
    Name,
    High_Priority__c,
    Blockchain_Fund_Lead_Owner__c,
    Blockchain_Fund_Status__c,
    Estimated_Investment_Size__c,
    Blockchain_Fund_Class__c,
    CREATEDDATE,
    LASTMODIFIEDDATE
)
SELECT
source.Id__c,
source.Name__c,
source.High_Priority_c__c,
source.Blockchain_Fund_Lead_Owner_c__c,
source.Blockchain_Fund_Status_c__c,
source.Estimated_Investment_Size_c__c,
source.Blockchain_Fund_Class_c__c,
source.CreatedDate__c,
source.LastModifiedDate__c
FROM RAW_DEV.DEV_RAW_SALESFORCE.ACCOUNT source
LEFT JOIN HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT target ON source.Id__c = target.Id
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
        'ACCOUNT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'ACCOUNT_LOAD_HAR',
        'INSERT SUCCESS',
        NULL
    );


UPDATE HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT target
SET
    target.Id=source.Id__c,
    target.Name=source.Name__c,
    target.High_Priority__c=source.High_Priority_c__c,
    target.Blockchain_Fund_Lead_Owner__c=source.Blockchain_Fund_Lead_Owner_c__c,
    target.Blockchain_Fund_Status__c=source.Blockchain_Fund_Status_c__c,
    target.Estimated_Investment_Size__c=source.Estimated_Investment_Size_c__c,
    target.Blockchain_Fund_Class__c=source.Blockchain_Fund_Class_c__c,
    target.CreatedDate = source.CreatedDate__c,
    target.LastModifiedDate = source.LastModifiedDate__c

FROM RAW_DEV.DEV_RAW_SALESFORCE.ACCOUNT source
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
        'ACCOUNT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'ACCOUNT_LOAD_HAR',
        'UPDATE SUCCESS',
        NULL
    );


DELETE FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT
WHERE ID IN 
(SELECT target.ID
FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.ACCOUNT target
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.ACCOUNT source
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
        'ACCOUNT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'ACCOUNT_LOAD_HAR',
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
                 'Email Alert: Error while executing ACCOUNT_LOAD_HAR Procedure',
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
        'ACCOUNT',
        0,
        CURRENT_TIMESTAMP(),
        'ACCOUNT_LOAD_HAR',
        'FAILURE',
        :SQLERRM
    );


END;
$$;
