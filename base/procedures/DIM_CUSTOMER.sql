CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK{{ENV}}.BASE.DIM_CUSTOMER()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
    BEGIN
    MERGE INTO UT_DE_FRAMEWORK.BASE.DIM_CUSTOMER target
    USING (
	WITH final AS (
    SELECT 
        C_CUSTKEY,       
        C_NAME,
        C_ADDRESS,
        C_NATIONKEY,
		C_PHONE,
        C_ACCTBAL,
		C_MKTSEGMENT,
		C_COMMENT,
        N_NAME,
        N_REGIONKEY,
        N_COMMENT        
    FROM UT_DE_FRAMEWORK.RAW.CUSTOMER as c
    inner join UT_DE_FRAMEWORK.RAW.NATION as n on (c.C_NATIONKEY = n.N_NATIONKEY)  
)
SELECT * FROM final

) AS source
    ON target.C_CUSTKEY = source.C_CUSTKEY
    WHEN MATCHED THEN
        UPDATE SET 
            target.C_NAME = source.C_NAME,
			target.C_ADDRESS = source.C_ADDRESS,
			target.C_NATIONKEY = source.C_NATIONKEY,
			target.C_PHONE = source.C_PHONE,
			target.C_ACCTBAL = source.C_ACCTBAL,
			target.C_MKTSEGMENT = source.C_MKTSEGMENT,
			target.C_COMMENT = source.C_COMMENT,
            target.N_NAME = source.N_NAME,
			target.N_REGIONKEY = source.N_REGIONKEY,
			target.N_COMMENT = source.N_COMMENT
			WHEN NOT MATCHED THEN
        INSERT (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT,C_COMMENT, N_NAME, N_REGIONKEY, N_COMMENT)
		VALUES (source.C_CUSTKEY, source.C_NAME, source.C_ADDRESS, source.C_NATIONKEY, source.C_PHONE, source.C_ACCTBAL, source.C_MKTSEGMENT, source.C_COMMENT,source.N_NAME, source.N_REGIONKEY, source.N_COMMENT);
		
    RETURN ''Procedure completed successfully'';
    END;
';