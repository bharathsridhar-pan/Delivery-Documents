CREATE OR REPLACE PROCEDURE HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER_LOAD_HAR()
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
	-- Description: Loads data into LIMITED_PARTNER__C HARMONIZED table
	-- Modifications:03/17/2025 - Created the Procedure

    
INSERT INTO HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER__C(
Id,
Name,
Limited_Partner_ID__c,
Limited_Partner_Name__c,
Total_Subscriptions__c,
Total_Redemptions__c,
Total_Distributions__c,
Total_AUM__c,
Total_P_L_on_Invested__c,
CREATEDDATE,
LASTMODIFIEDDATE
)
SELECT
source.Id__c,
source.Name__c,
source.Limited_Partner_ID_c__c,
source.Limited_Partner_Name_c__c,
source.Total_Subscriptions_c__c,
source.Total_Redemptions_c__c,
source.Total_Distributions_c__c,
source.Total_AUM_c__c,
source.Total_P_L_on_Invested_c__c,
source.CreatedDate__c,
source.LastModifiedDate__c
FROM RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER__C source
LEFT JOIN HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER__C target ON source.Id__c = target.Id
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
        'LIMITED_PARTNER__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_LOAD_HAR',
        'INSERT SUCCESS',
        NULL
    );


UPDATE HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER__C target
SET
    target.Id=source.Id__c,
    target.Name=source.Name__c,
    target.Limited_Partner_ID__c=source.Limited_Partner_ID_c__c,
    target.Limited_Partner_Name__c=source.Limited_Partner_Name_c__c,
    target.Total_Subscriptions__c=source.Total_Subscriptions_c__c,
    target.Total_Redemptions__c=source.Total_Redemptions_c__c,
    target.Total_Distributions__c=source.Total_Distributions_c__c,
    target.Total_AUM__c=source.Total_AUM_c__c,
    target.Total_P_L_on_Invested__c=source.Total_P_L_on_Invested_c__c,
    target.CreatedDate = source.CreatedDate__c,
    target.LastModifiedDate = source.LastModifiedDate__c

FROM RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER__C source
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
        'LIMITED_PARTNER__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_LOAD_HAR',
        'UPDATE SUCCESS',
        NULL
    );

DELETE FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER__C
WHERE ID IN 
(SELECT target.ID
FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.LIMITED_PARTNER__C target
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER__C source
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
        'LIMITED_PARTNER__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_LOAD_HAR',
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
                 'Email Alert: Error while executing LIMITED_PARTNER_LOAD_HAR Procedure',
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
        'LIMITED_PARTNER__C',
        0,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_LOAD_HAR',
        'FAILURE',
        :SQLERRM
    );


END;
$$;