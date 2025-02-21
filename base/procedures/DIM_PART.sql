CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK{{ENV}}.BASE.DIM_PART()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
    BEGIN
    MERGE INTO UT_DE_FRAMEWORK.BASE.DIM_PART target
    USING (
	WITH final AS (
    SELECT 
        p.P_PARTKEY, 
        p.P_NAME, 
        p.P_MFGR, 
        p.P_BRAND, 
        p.P_TYPE, 
        p.P_SIZE, 
        p.P_CONTAINER, 
        p.P_RETAILPRICE, 
        p.P_COMMENT,
        ps.PS_SUPPKEY, 
        ps.PS_AVAILQTY, 
        ps.PS_SUPPLYCOST, 
        ps.PS_COMMENT
    FROM 
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART p
    JOIN 
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP ps 
		ON p.P_PARTKEY = ps.PS_PARTKEY
    WHERE 
        ps.PS_AVAILQTY > 100  -- Only include parts with available quantity greater than 100
)
-- Main query to select the joined data
SELECT 
    P_PARTKEY, 
    P_NAME, 
    P_MFGR, 
    P_BRAND, 
    P_TYPE, 
    P_SIZE, 
    P_CONTAINER, 
    P_RETAILPRICE, 
    P_COMMENT,
    PS_SUPPKEY, 
    PS_AVAILQTY, 
    PS_SUPPLYCOST, 
    PS_COMMENT
FROM final
ORDER BY P_NAME

) AS source
    ON target.P_PARTKEY = source.P_PARTKEY 
	and target.PS_SUPPKEY = source.PS_SUPPKEY
    WHEN MATCHED THEN
        UPDATE SET 
            target.P_NAME = source.P_NAME,
			target.P_MFGR = source.P_MFGR,
			target.P_BRAND = source.P_BRAND,
			target.P_TYPE = source.P_TYPE,
			target.P_SIZE = source.P_SIZE,
			target.P_CONTAINER = source.P_CONTAINER,
			target.P_RETAILPRICE = source.P_RETAILPRICE,
			target.P_COMMENT = source.P_COMMENT,
			target.PS_AVAILQTY = source.PS_AVAILQTY,
			target.PS_SUPPLYCOST = source.PS_SUPPLYCOST,
			target.PS_COMMENT = source.PS_COMMENT
			WHEN NOT MATCHED THEN
        INSERT (P_NAME, P_MFGR, P_BRAND, P_TYPE,P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT,PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT)
		VALUES (source.P_NAME, source.P_MFGR, source.P_BRAND, source.P_TYPE,source.P_SIZE, source.P_CONTAINER, source.P_RETAILPRICE, source.P_COMMENT,source.PS_AVAILQTY, source.PS_SUPPLYCOST, source.PS_COMMENT);
		
    RETURN ''Procedure completed successfully'';
    END;
';