CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK{{ENV}}.BASE.PROC_CUSTOMER_ORDERS_ENRICHED()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- 1. Create the target table if it doesn't exist.
    EXECUTE IMMEDIATE '
        CREATE TABLE IF NOT EXISTS UT_DE_FRAMEWORK.BASE.FACT_CUSTOMER_ORDERS_ENRICHED (
            C_CUSTKEY        NUMBER,
            C_NAME           VARCHAR(100),
            C_ADDRESS        VARCHAR(200),
            C_NATIONKEY      NUMBER,
            C_PHONE          VARCHAR(50),
            C_ACCTBAL        NUMBER(12,2),
            C_MKTSEGMENT     VARCHAR(50),
            C_COMMENT        VARCHAR(500),
            O_ORDERKEY       NUMBER,
            O_ORDERSTATUS    CHAR(1),
            O_TOTALPRICE     NUMBER(12,2),
            O_ORDERDATE      DATE,
            O_ORDERPRIORITY  VARCHAR(50),
            O_CLERK          VARCHAR(50),
            O_SHIPPRIORITY   NUMBER,
            O_COMMENT        VARCHAR(500),
            N_NAME           VARCHAR(50),
            N_REGIONKEY      NUMBER,
            N_COMMENT        VARCHAR(255),
            REGION_NAME      VARCHAR(50),
            REGION_COMMENT   VARCHAR(255)
        )
    ';

    -- 2. Merge enriched data from RAW tables into the target table.
    MERGE INTO UT_DE_FRAMEWORK.BASE.FACT_CUSTOMER_ORDERS_ENRICHED AS target
    USING (
        WITH enriched_data AS (
            -- Step A: Join CUSTOMER with ORDERS
            SELECT 
                c.C_CUSTKEY,
                c.C_NAME,
                c.C_ADDRESS,
                c.C_NATIONKEY,
                c.C_PHONE,
                c.C_ACCTBAL,
                c.C_MKTSEGMENT,
                c.C_COMMENT,
                o.O_ORDERKEY,
                o.O_ORDERSTATUS,
                o.O_TOTALPRICE,
                o.O_ORDERDATE,
                o.O_ORDERPRIORITY,
                o.O_CLERK,
                o.O_SHIPPRIORITY,
                o.O_COMMENT AS ORDER_COMMENT
            FROM UT_DE_FRAMEWORK.RAW.CUSTOMER c
            INNER JOIN UT_DE_FRAMEWORK.RAW.ORDERS o
                ON c.C_CUSTKEY = o.O_CUSTKEY
        ),
        enriched_with_nation AS (
            -- Step B: Join with NATION
            SELECT 
                ed.*,
                n.N_NAME,
                n.N_REGIONKEY,
                n.N_COMMENT
            FROM enriched_data ed
            INNER JOIN UT_DE_FRAMEWORK.RAW.NATION n
                ON ed.C_NATIONKEY = n.N_NATIONKEY
        ),
        final_enriched AS (
            -- Step C: Join with REGION
            SELECT
                enw.*,
                r.R_NAME AS REGION_NAME,
                r.R_COMMENT AS REGION_COMMENT
            FROM enriched_with_nation enw
            INNER JOIN UT_DE_FRAMEWORK.RAW.REGION r
                ON enw.N_REGIONKEY = r.R_REGIONKEY
        )
        SELECT * FROM final_enriched
    ) AS source
    ON target.O_ORDERKEY = source.O_ORDERKEY
    WHEN MATCHED THEN
        UPDATE SET 
            target.C_CUSTKEY       = source.C_CUSTKEY,
            target.C_NAME          = source.C_NAME,
            target.C_ADDRESS       = source.C_ADDRESS,
            target.C_NATIONKEY     = source.C_NATIONKEY,
            target.C_PHONE         = source.C_PHONE,
            target.C_ACCTBAL       = source.C_ACCTBAL,
            target.C_MKTSEGMENT    = source.C_MKTSEGMENT,
            target.C_COMMENT       = source.C_COMMENT,
            target.O_ORDERSTATUS   = source.O_ORDERSTATUS,
            target.O_TOTALPRICE    = source.O_TOTALPRICE,
            target.O_ORDERDATE     = source.O_ORDERDATE,
            target.O_ORDERPRIORITY = source.O_ORDERPRIORITY,
            target.O_CLERK         = source.O_CLERK,
            target.O_SHIPPRIORITY  = source.O_SHIPPRIORITY,
            target.O_COMMENT       = source.ORDER_COMMENT,
            target.N_NAME          = source.N_NAME,
            target.N_REGIONKEY     = source.N_REGIONKEY,
            target.N_COMMENT       = source.N_COMMENT,
            target.REGION_NAME     = source.REGION_NAME,
            target.REGION_COMMENT  = source.REGION_COMMENT
    WHEN NOT MATCHED THEN
        INSERT (
            C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT,
            O_ORDERKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT,
            N_NAME, N_REGIONKEY, N_COMMENT, REGION_NAME, REGION_COMMENT
        )
        VALUES (
            source.C_CUSTKEY, source.C_NAME, source.C_ADDRESS, source.C_NATIONKEY, source.C_PHONE, source.C_ACCTBAL, source.C_MKTSEGMENT, source.C_COMMENT,
            source.O_ORDERKEY, source.O_ORDERSTATUS, source.O_TOTALPRICE, source.O_ORDERDATE, source.O_ORDERPRIORITY, source.O_CLERK, source.O_SHIPPRIORITY, source.ORDER_COMMENT,
            source.N_NAME, source.N_REGIONKEY, source.N_COMMENT, source.REGION_NAME, source.REGION_COMMENT
        );

    RETURN 'Procedure completed successfully - FACT_CUSTOMER_ORDERS_ENRICHED updated';
END;
$$;

