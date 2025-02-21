
CREATE OR REPLACE PROCEDURE UT_DE_FRAMEWORK.BASE.LOAD_FACT_CUSTOMER_TOTAL_ORDERS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate the target table to remove old data
    TRUNCATE TABLE UT_DE_FRAMEWORK.BASE.FACT_CUSTOMER_TOTAL_ORDERS;
    
    -- Insert new aggregated data from the RAW layer.
    INSERT INTO UT_DE_FRAMEWORK.BASE.FACT_CUSTOMER_TOTAL_ORDERS
    (
      C_CUSTKEY, 
      C_NAME, 
      C_ADDRESS, 
      C_NATIONKEY, 
      C_PHONE, 
      C_ACCTBAL, 
      C_MKTSEGMENT, 
      C_COMMENT, 
      CUSTOMER_TOTAL_ORDERS, 
      CUSTOMER_TOTAL_PRICE
    )
    SELECT 
         c.C_CUSTKEY,
         c.C_NAME,
         c.C_ADDRESS,
         c.C_NATIONKEY,
         c.C_PHONE,
         c.C_ACCTBAL,
         c.C_MKTSEGMENT,
         c.C_COMMENT,
         COUNT(o.O_ORDERKEY) AS CUSTOMER_TOTAL_ORDERS,
         SUM(o.O_TOTALPRICE) AS CUSTOMER_TOTAL_PRICE
    FROM UT_DE_FRAMEWORK.RAW.CUSTOMER AS c
    LEFT JOIN UT_DE_FRAMEWORK.RAW.ORDERS AS o
         ON c.C_CUSTKEY = o.O_CUSTKEY
    WHERE o.O_ORDERDATE BETWEEN '1992-01-01' AND '1992-12-31'
    GROUP BY 
         c.C_CUSTKEY,
         c.C_NAME,
         c.C_ADDRESS,
         c.C_NATIONKEY,
         c.C_PHONE,
         c.C_ACCTBAL,
         c.C_MKTSEGMENT,
         c.C_COMMENT;
         
    RETURN 'Truncate and load procedure completed successfully';
END;
$$;