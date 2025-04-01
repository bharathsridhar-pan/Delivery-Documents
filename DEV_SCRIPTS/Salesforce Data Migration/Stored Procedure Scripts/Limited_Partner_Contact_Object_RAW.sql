CREATE OR REPLACE PROCEDURE RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT_LOAD()
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
	-- Description: Loads data into LIMITED_PARTNER_CONTACT__C RAW table
	-- Modifications:03/17/2025 - Created the Procedure




INSERT INTO RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT__C(
Average_Subscription_per_Fund_c__c,
cdp_sys_PartitionDate__c,
cdp_sys_SourceVersion__c,
Communication_c__c,
Company_c__c,
Contact_c__c,
Contact_ID_c__c,
CreatedById__c,
CreatedDate__c,
DataSource__c,
DataSourceObject__c,
Email_2_c__c,
Email_c__c,
Hedge_Weighted_AUM_c__c,
Hedge_Weighted_Subscriptions_c__c,
Id__c,
IsDeleted__c,
KQ_Id__c,
LastModifiedById__c,
LastModifiedDate__c,
LastReferencedDate__c,
LastViewedDate__c,
Limited_Partner_ID_c__c,
Limited_Partner_Name_c__c,
LP_Contact_c__c,
LP_Contact_Type_c__c,
Max_Non_Fund_IV_Subscriptions_c__c,
Name__c,
Passive_Weighted_AUM_c__c,
Passive_Weighted_Subscriptions_c__c,
SfdcOrganizationId__c,
SystemModstamp__c,
Total_AUM_c__c,
Total_Fund_IV_Subscriptions_c__c,
Total_Redemptions_c__c,
Total_Subscriptions_c__c,
Total_Weighted_AUM_c__c,
Total_Weighted_Subscriptions_c__c,
Venture_Weighted_AUM_c__c,
Venture_Weighted_Subscriptions_c__c
)
SELECT
source."Average_Subscription_per_Fund_c__c",
source."cdp_sys_PartitionDate__c",
source."cdp_sys_SourceVersion__c",
source."Communication_c__c",
source."Company_c__c",
source."Contact_c__c",
source."Contact_ID_c__c",
source."CreatedById__c",
source."CreatedDate__c",
source."DataSource__c",
source."DataSourceObject__c",
source."Email_2_c__c",
source."Email_c__c",
source."Hedge_Weighted_AUM_c__c",
source."Hedge_Weighted_Subscriptions_c__c",
source."Id__c",
source."IsDeleted__c",
source."KQ_Id__c",
source."LastModifiedById__c",
source."LastModifiedDate__c",
source."LastReferencedDate__c",
source."LastViewedDate__c",
source."Limited_Partner_ID_c__c",
source."Limited_Partner_Name_c__c",
source."LP_Contact_c__c",
source."LP_Contact_Type_c__c",
source."Max_Non_Fund_IV_Subscriptions_c__c",
source."Name__c",
source."Passive_Weighted_AUM_c__c",
source."Passive_Weighted_Subscriptions_c__c",
source."SfdcOrganizationId__c",
source."SystemModstamp__c",
source."Total_AUM_c__c",
source."Total_Fund_IV_Subscriptions_c__c",
source."Total_Redemptions_c__c",
source."Total_Subscriptions_c__c",
source."Total_Weighted_AUM_c__c",
source."Total_Weighted_Subscriptions_c__c",
source."Venture_Weighted_AUM_c__c",
source."Venture_Weighted_Subscriptions_c__c"
FROM DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Limited_Partner_Contact_c_Home__dll" source
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT__C target on source."Id__c"=target.Id__c
WHERE target.Id__c IS NULL;


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
        'SALESFORCE',
        'RAW_DEV',
        'DEV_RAW_SALESFORCE',
        'LIMITED_PARTNER_CONTACT__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_CONTACT_LOAD',
        'INSERT SUCCESS',
        NULL
    );

UPDATE RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT__C target
SET
target.Average_Subscription_per_Fund_c__c=source."Average_Subscription_per_Fund_c__c",
target.cdp_sys_PartitionDate__c=source."cdp_sys_PartitionDate__c",
target.cdp_sys_SourceVersion__c=source."cdp_sys_SourceVersion__c",
target.Communication_c__c=source."Communication_c__c",
target.Company_c__c=source."Company_c__c",
target.Contact_c__c=source."Contact_c__c",
target.Contact_ID_c__c=source."Contact_ID_c__c",
target.CreatedById__c=source."CreatedById__c",
target.CreatedDate__c=source."CreatedDate__c",
target.DataSource__c=source."DataSource__c",
target.DataSourceObject__c=source."DataSourceObject__c",
target.Email_2_c__c=source."Email_2_c__c",
target.Email_c__c=source."Email_c__c",
target.Hedge_Weighted_AUM_c__c=source."Hedge_Weighted_AUM_c__c",
target.Hedge_Weighted_Subscriptions_c__c=source."Hedge_Weighted_Subscriptions_c__c",
target.Id__c=source."Id__c",
target.IsDeleted__c=source."IsDeleted__c",
target.KQ_Id__c=source."KQ_Id__c",
target.LastModifiedById__c=source."LastModifiedById__c",
target.LastModifiedDate__c=source."LastModifiedDate__c",
target.LastReferencedDate__c=source."LastReferencedDate__c",
target.LastViewedDate__c=source."LastViewedDate__c",
target.Limited_Partner_ID_c__c=source."Limited_Partner_ID_c__c",
target.Limited_Partner_Name_c__c=source."Limited_Partner_Name_c__c",
target.LP_Contact_c__c=source."LP_Contact_c__c",
target.LP_Contact_Type_c__c=source."LP_Contact_Type_c__c",
target.Max_Non_Fund_IV_Subscriptions_c__c=source."Max_Non_Fund_IV_Subscriptions_c__c",
target.Name__c=source."Name__c",
target.Passive_Weighted_AUM_c__c=source."Passive_Weighted_AUM_c__c",
target.Passive_Weighted_Subscriptions_c__c=source."Passive_Weighted_Subscriptions_c__c",
target.SfdcOrganizationId__c=source."SfdcOrganizationId__c",
target.SystemModstamp__c=source."SystemModstamp__c",
target.Total_AUM_c__c=source."Total_AUM_c__c",
target.Total_Fund_IV_Subscriptions_c__c=source."Total_Fund_IV_Subscriptions_c__c",
target.Total_Redemptions_c__c=source."Total_Redemptions_c__c",
target.Total_Subscriptions_c__c=source."Total_Subscriptions_c__c",
target.Total_Weighted_AUM_c__c=source."Total_Weighted_AUM_c__c",
target.Total_Weighted_Subscriptions_c__c=source."Total_Weighted_Subscriptions_c__c",
target.Venture_Weighted_AUM_c__c=source."Venture_Weighted_AUM_c__c",
target.Venture_Weighted_Subscriptions_c__c=source."Venture_Weighted_Subscriptions_c__c"

FROM DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Limited_Partner_Contact_c_Home__dll" source
WHERE source."Id__c"=target.Id__c AND source."LastModifiedDate__c" > target.LastModifiedDate__c;

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
        'SALESFORCE',
        'RAW_DEV',
        'DEV_RAW_SALESFORCE',
        'LIMITED_PARTNER_CONTACT__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_CONTACT_LOAD',
        'UPDATE SUCCESS',
        NULL
    );

DELETE FROM RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT__C
WHERE ID__C IN 
(SELECT target.ID__C
FROM RAW_DEV.DEV_RAW_SALESFORCE.LIMITED_PARTNER_CONTACT__C target
LEFT JOIN DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Limited_Partner_Contact_c_Home__dll" source
ON target.ID__C = source."Id__c"
WHERE source."Id__c" IS NULL);

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
        'SALESFORCE',
        'RAW_DEV',
        'DEV_RAW_SALESFORCE',
        'LIMITED_PARTNER_CONTACT__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_CONTACT_LOAD',
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
                 'Email Alert: Error while executing LIMITED_PARTNER_CONTACT_LOAD Procedure',
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
        'SALESFORCE',
        'RAW_DEV',
        'DEV_RAW_SALESFORCE',
        'LIMITED_PARTNER_CONTACT__C',
        0,
        CURRENT_TIMESTAMP(),
        'LIMITED_PARTNER_CONTACT_LOAD',
        'FAILURE',
        :SQLERRM
    );


END;
$$;