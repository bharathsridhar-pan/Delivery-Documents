CREATE OR REPLACE PROCEDURE HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT_LOAD_HAR()
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
	-- Description: Loads data into CONTACT HARMONIZED table
	-- Modifications:03/17/2025 - Created the Procedure

    
INSERT INTO HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT(
    Id,
    FirstName, 
    LastName,
    Name,
    Title,
    AccountId,
    Email,
    Investor_Type__c,
    ContactType__c,
    OwnerId,
    Lead_Owner__c,
    New_Channel_picklist__c,
    New_Source_picklist__c,
    Lead_Score__c,
    Contact_Engagement_Status__c,
    Contact_Met_With__c,
    Metro_Area__c,
    Total_Meetings_Calls__c,
    Contact_Meeting_Score__c,
    Investor_Letter_Opens__c,
    Dataroom__c,
    Company_Lead_Score_max__c,
    Company_Engagement_Status__c,
    Company_Met_With__c,
    Blockchain_Fund_Interest__c,
    Primary_Company_Contact__c,
    Early_Redemption_Request__c,
    Estimated_Investment_Size__c,
    Marketing_Stage__c,
    Marketing_Level__c,
    Funds_Invested__c,
    Funds_of_Interest__c,
    CREATEDDATE,
    LASTMODIFIEDDATE
)
SELECT
source.Id__c,
source.FirstName__c,
source.LastName__c,
source.Name__c,
source.Title__c,
source.AccountId__c,
source.Email__c,
source.Investor_Type_c__c,
source.ContactType_c__c,
source.OwnerId__c,
source.Lead_Owner_c__c,
source.New_Channel_picklist_c__c,
source.New_Source_picklist_c__c,
source.Lead_Score_c__c,
source.Contact_Engagement_Status_c__c,
source.Contact_Met_With_c__c,
source.Metro_Area_c__c,
source.Total_Meetings_Calls_c__c,
source.Contact_Meeting_Score_c__c,
source.Investor_Letter_Opens_c__c,
source.Dataroom_c__c,
source.Company_Lead_Score_max_c__c,
source.Company_Engagement_Status_c__c,
source.Company_Met_With_c__c,
source.Blockchain_Fund_Interest_c__c,
source.Primary_Company_Contact_c__c,
source.Early_Redemption_Request_c__c,
source.Estimated_Investment_Size_c__c,
source.Marketing_Stage_c__c,
source.Marketing_Level_c__c,
source.Funds_Invested_c__c,
source.Funds_of_Interest_c__c,
source.CreatedDate__c,
source.LastModifiedDate__c
FROM RAW_DEV.DEV_RAW_SALESFORCE.CONTACT source
LEFT JOIN HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT target ON source.Id__c = target.Id
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
        'CONTACT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CONTACT_LOAD_HAR',
        'INSERT SUCCESS',
        NULL
    );


UPDATE HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT target
SET
    target.Id=source.Id__c,
    target.FirstName =source.FirstName__c,
    target.LastName=source.LastName__c,
    target.Name =source.Name__c,
    target.Title=source.Title__c,
    target.AccountId=source.AccountId__c,
    target.Email=source.Email__c,
    target.Investor_Type__c=source.Investor_Type_c__c,
    target.ContactType__c=source.ContactType_c__c,
    target.OwnerId=source.OwnerId__c,
    target.Lead_Owner__c=source.Lead_Owner_c__c,
    target.New_Channel_picklist__c=source.New_Channel_picklist_c__c,
    target.New_Source_picklist__c=source.New_Source_picklist_c__c,
    target.Lead_Score__c=source.Lead_Score_c__c,
    target.Contact_Engagement_Status__c=source.Contact_Engagement_Status_c__c,
    target.Contact_Met_With__c=source.Contact_Met_With_c__c,
    target.Metro_Area__c=source.Metro_Area_c__c,
    target.Total_Meetings_Calls__c=source.Total_Meetings_Calls_c__c,
    target.Contact_Meeting_Score__c=source.Contact_Meeting_Score_c__c,
    target.Investor_Letter_Opens__c=source.Investor_Letter_Opens_c__c,
    target.Dataroom__c=source.Dataroom_c__c,
    target.Company_Lead_Score_max__c=source.Company_Lead_Score_max_c__c,
    target.Company_Engagement_Status__c=source.Company_Engagement_Status_c__c,
    target.Company_Met_With__c=source.Company_Met_With_c__c,
    target.Blockchain_Fund_Interest__c=source.Blockchain_Fund_Interest_c__c,
    target.Primary_Company_Contact__c=source.Primary_Company_Contact_c__c,
    target.Early_Redemption_Request__c=source.Early_Redemption_Request_c__c,
    target.Estimated_Investment_Size__c=source.Estimated_Investment_Size_c__c,
    target.Marketing_Stage__c=source.Marketing_Stage_c__c,
    target.Marketing_Level__c=source.Marketing_Level_c__c,
    target.Funds_Invested__c=source.Funds_Invested_c__c,
    target.Funds_of_Interest__c=source.Funds_of_Interest_c__c,
    target.CREATEDDATE=source.CreatedDate__c,
    target.LASTMODIFIEDDATE=source.LastModifiedDate__c
FROM RAW_DEV.DEV_RAW_SALESFORCE.CONTACT source
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
        'CONTACT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CONTACT_LOAD_HAR',
        'UPDATE SUCCESS',
        NULL
    );


DELETE FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT
WHERE ID IN 
(SELECT target.ID
FROM HARMONIZED_DEV.DEV_HAR_SALESFORCE.CONTACT target
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.CONTACT source
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
        'CONTACT',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CONTACT_LOAD_HAR',
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
                 'Email Alert: Error while executing CONTACT_LOAD_HAR Procedure',
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
        'CONTACT',
        0,
        CURRENT_TIMESTAMP(),
        'CONTACT_LOAD_HAR',
        'FAILURE',
        :SQLERRM
    );


END;
$$;