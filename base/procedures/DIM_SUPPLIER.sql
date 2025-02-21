CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK{{ENV}}.BASE.DIM_SUPPLIER()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
    BEGIN
    MERGE INTO UT_DE_FRAMEWORK.BASE.DIM_SUPPLIER target
    USING (
	WITH final AS (
    SELECT 
        S_SUPPKEY, 
        S_NAME, 
        S_ADDRESS, 
        S_NATIONKEY, 
        S_PHONE, 
        S_ACCTBAL, 
        S_COMMENT
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER
    WHERE S_NATIONKEY = 5 
)
SELECT 
    S_SUPPKEY, 
    S_NAME, 
    S_ADDRESS, 
    S_NATIONKEY, 
    S_PHONE, 
    S_ACCTBAL, 
    S_COMMENT
FROM final
ORDER BY S_NAME

) AS source
    ON target.S_SUPPKEY = source.S_SUPPKEY
    WHEN MATCHED THEN
        UPDATE SET 
            target.S_NAME = source.S_NAME,
			target.S_ADDRESS = source.S_ADDRESS,
			target.S_NATIONKEY = source.S_NATIONKEY,
			target.S_PHONE = source.S_PHONE,
			target.S_ACCTBAL = source.S_ACCTBAL,
			target.S_COMMENT = source.S_COMMENT
			WHEN NOT MATCHED THEN
        INSERT (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT)
		VALUES (source.S_SUPPKEY, source.S_NAME, source.S_ADDRESS, source.S_NATIONKEY, source.S_PHONE, source.S_ACCTBAL,  source.S_COMMENT);
		
    RETURN ''Procedure completed successfully'';
    END;
';