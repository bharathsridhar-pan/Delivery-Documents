CREATE OR REPLACE PROCEDURE RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY_LOAD()
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
	-- Description: Loads data into Capital_Activity__c RAW table
	-- Modifications:03/11/2025 - Created the Procedure




INSERT INTO RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C(
Amount_c__c,
Capital_Activity_Date_c__c,
Capital_Activity_Type_c__c,
Capital_Activity_c__c,
Class_S_Co_Investment_c__c,
Class_S_Deal_Series_c__c,
Class_S_Trade_Date_c__c,
CreatedById__c,
CreatedDate__c,
DataSourceObject__c,
DataSource__c,
Full_Redemption_c__c,
Fund_Based_Commitment_c__c,
Fund_c__c,
Id__c,
Investor_ID_c__c,
IsDeleted__c,
KQ_Id__c,
LastModifiedById__c,
LastModifiedDate__c,
LastReferencedDate__c,
LastViewedDate__c,
Limited_Partner_Name_c__c,
Name__c,
Pending_Redemption_c__c,
Realization_Fee_c__c,
Redemption_Percentage_c__c,
Redemption_Series_c__c,
Redemption_Shares_c__c,
Sales_Channel_c__c,
Sales_Credit_1_c__c,
Sales_Credit_2_c__c,
Sales_Credit_3_c__c,
Sales_Credit_4_c__c,
Sales_Credit_5_c__c,
Sales_Credit_6_c__c,
Sales_Percentage_1_c__c,
Sales_Percentage_2_c__c,
Sales_Percentage_3_c__c,
Sales_Percentage_4_c__c,
Sales_Percentage_5_c__c,
Sales_Percentage_6_c__c,
Sales_Source_c__c,
SfdcOrganizationId__c,
Share_Class_c__c,
Side_Letter_c__c,
Strategy_Type_c__c,
SystemModstamp__c,
Transfer_c__c,
Transferor_Transferee_c__c,
cdp_sys_PartitionDate__c,
cdp_sys_SourceVersion__c,
pantera_id_c__c,
redemption_per_admin_c__c
)
SELECT
source."Amount_c__c",
source."Capital_Activity_Date_c__c",
source."Capital_Activity_Type_c__c",
source."Capital_Activity_c__c",
source."Class_S_Co_Investment_c__c",
source."Class_S_Deal_Series_c__c",
source."Class_S_Trade_Date_c__c",
source."CreatedById__c",
source."CreatedDate__c",
source."DataSourceObject__c",
source."DataSource__c",
source."Full_Redemption_c__c",
source."Fund_Based_Commitment_c__c",
source."Fund_c__c",
source."Id__c",
source."Investor_ID_c__c",
source."IsDeleted__c",
source."KQ_Id__c",
source."LastModifiedById__c",
source."LastModifiedDate__c",
source."LastReferencedDate__c",
source."LastViewedDate__c",
source."Limited_Partner_Name_c__c",
source."Name__c",
source."Pending_Redemption_c__c",
source."Realization_Fee_c__c",
source."Redemption_Percentage_c__c",
source."Redemption_Series_c__c",
source."Redemption_Shares_c__c",
source."Sales_Channel_c__c",
source."Sales_Credit_1_c__c",
source."Sales_Credit_2_c__c",
source."Sales_Credit_3_c__c",
source."Sales_Credit_4_c__c",
source."Sales_Credit_5_c__c",
source."Sales_Credit_6_c__c",
source."Sales_Percentage_1_c__c",
source."Sales_Percentage_2_c__c",
source."Sales_Percentage_3_c__c",
source."Sales_Percentage_4_c__c",
source."Sales_Percentage_5_c__c",
source."Sales_Percentage_6_c__c",
source."Sales_Source_c__c",
source."SfdcOrganizationId__c",
source."Share_Class_c__c",
source."Side_Letter_c__c",
source."Strategy_Type_c__c",
source."SystemModstamp__c",
source."Transfer_c__c",
source."Transferor_Transferee_c__c",
source."cdp_sys_PartitionDate__c",
source."cdp_sys_SourceVersion__c",
source."pantera_id_c__c",
source."redemption_per_admin_c__c"
FROM DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Capital_Activity_c_Home__dll" source
LEFT JOIN RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C target on source."Id__c"=target.Id__c
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
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD',
        'INSERT SUCCESS',
        NULL
    );

UPDATE RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C target
SET
    target.Amount_c__c=source."Amount_c__c",
    target.Capital_Activity_Date_c__c=source."Capital_Activity_Date_c__c",
    target.Capital_Activity_Type_c__c=source."Capital_Activity_Type_c__c",
    target.Capital_Activity_c__c=source."Capital_Activity_c__c",
    target.Class_S_Co_Investment_c__c=source."Class_S_Co_Investment_c__c",
    target.Class_S_Deal_Series_c__c=source."Class_S_Deal_Series_c__c",
    target.Class_S_Trade_Date_c__c=source."Class_S_Trade_Date_c__c",
    target.CreatedById__c=source."CreatedById__c",
    target.CreatedDate__c=source."CreatedDate__c",
    target.DataSourceObject__c=source."DataSourceObject__c",
    target.DataSource__c=source."DataSource__c",
    target.Full_Redemption_c__c=source."Full_Redemption_c__c",
    target.Fund_Based_Commitment_c__c=source."Fund_Based_Commitment_c__c",
    target.Fund_c__c=source."Fund_c__c",
    target.Id__c=source."Id__c",
    target.Investor_ID_c__c=source."Investor_ID_c__c",
    target.IsDeleted__c=source."IsDeleted__c",
    target.KQ_Id__c=source."KQ_Id__c",
    target.LastModifiedById__c=source."LastModifiedById__c",
    target.LastModifiedDate__c=source."LastModifiedDate__c",
    target.LastReferencedDate__c=source."LastReferencedDate__c",
    target.LastViewedDate__c=source."LastViewedDate__c",
    target.Limited_Partner_Name_c__c=source."Limited_Partner_Name_c__c",
    target.Name__c=source."Name__c",
    target.Pending_Redemption_c__c=source."Pending_Redemption_c__c",
    target.Realization_Fee_c__c=source."Realization_Fee_c__c",
    target.Redemption_Percentage_c__c=source."Redemption_Percentage_c__c",
    target.Redemption_Series_c__c=source."Redemption_Series_c__c",
    target.Redemption_Shares_c__c=source."Redemption_Shares_c__c",
    target.Sales_Channel_c__c=source."Sales_Channel_c__c",
    target.Sales_Credit_1_c__c=source."Sales_Credit_1_c__c",
    target.Sales_Credit_2_c__c=source."Sales_Credit_2_c__c",
    target.Sales_Credit_3_c__c=source."Sales_Credit_3_c__c",
    target.Sales_Credit_4_c__c=source."Sales_Credit_4_c__c",
    target.Sales_Credit_5_c__c=source."Sales_Credit_5_c__c",
    target.Sales_Credit_6_c__c=source."Sales_Credit_6_c__c",
    target.Sales_Percentage_1_c__c=source."Sales_Percentage_1_c__c",
    target.Sales_Percentage_2_c__c=source."Sales_Percentage_2_c__c",
    target.Sales_Percentage_3_c__c=source."Sales_Percentage_3_c__c",
    target.Sales_Percentage_4_c__c=source."Sales_Percentage_4_c__c",
    target.Sales_Percentage_5_c__c=source."Sales_Percentage_5_c__c",
    target.Sales_Percentage_6_c__c=source."Sales_Percentage_6_c__c",
    target.Sales_Source_c__c=source."Sales_Source_c__c",
    target.SfdcOrganizationId__c=source."SfdcOrganizationId__c",
    target.Share_Class_c__c=source."Share_Class_c__c",
    target.Side_Letter_c__c=source."Side_Letter_c__c",
    target.Strategy_Type_c__c=source."Strategy_Type_c__c",
    target.SystemModstamp__c=source."SystemModstamp__c",
    target.Transfer_c__c=source."Transfer_c__c",
    target.Transferor_Transferee_c__c=source."Transferor_Transferee_c__c",
    target.cdp_sys_PartitionDate__c=source."cdp_sys_PartitionDate__c",
    target.cdp_sys_SourceVersion__c=source."cdp_sys_SourceVersion__c",
    target.pantera_id_c__c=source."pantera_id_c__c",
    target.redemption_per_admin_c__c=source."redemption_per_admin_c__c"

FROM DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Capital_Activity_c_Home__dll" source
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
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD',
        'UPDATE SUCCESS',
        NULL
    );


DELETE FROM RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C
WHERE ID__C IN 
(SELECT target.ID__C
FROM RAW_DEV.DEV_RAW_SALESFORCE.CAPITAL_ACTIVITY__C target
LEFT JOIN DATATOSNOWFLAKEV2."schema_DatatoSnowflakeV2"."Capital_Activity_c_Home__dll" source
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
        'CAPITAL_ACTIVITY__C',
        :SQLROWCOUNT,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD',
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
                 'Email Alert: Error while executing CAPITAL_ACTIVITY_LOAD Procedure',
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
        'CAPITAL_ACTIVITY__C',
        0,
        CURRENT_TIMESTAMP(),
        'CAPITAL_ACTIVITY_LOAD',
        'FAILURE',
        :SQLERRM
    );


END;
$$;