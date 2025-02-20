CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK{{ENV}}.BASE.DIM_LOCATION()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
    BEGIN
    MERGE INTO UT_DE_FRAMEWORK.BASE.DIM_LOCATION target
    USING (
	WITH final AS (
    SELECT 
        N_NATIONKEY, 
        N_NAME, 
        N_REGIONKEY, 
        N_COMMENT,
        R_NAME,
        R_COMMENT
    FROM UT_DE_FRAMEWORK.RAW.NATION as n
    inner join UT_DE_FRAMEWORK.RAW.REGION as r on (n.N_REGIONKEY = r.R_REGIONKEY)
   
)
SELECT * FROM final

) AS source
    ON target.N_NATIONKEY = source.N_NATIONKEY
    WHEN MATCHED THEN
        UPDATE SET 
        target.N_NAME = source.N_NAME,
		target.N_REGIONKEY = source.N_REGIONKEY,
		target.N_COMMENT = source.N_COMMENT,
        target.R_NAME = source.R_NAME,
		target.R_COMMENT = source.R_COMMENT	

        WHEN NOT MATCHED THEN
        INSERT (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT, R_NAME, R_COMMENT)
		VALUES (source.N_NATIONKEY, source.N_NAME, source.N_REGIONKEY, source.N_COMMENT, source.R_NAME, source.R_COMMENT);
        
    RETURN ''Procedure completed successfully'';
    END;
';