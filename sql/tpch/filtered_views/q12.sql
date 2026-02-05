CREATE TABLE 
    lineitem_f12
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipmode IN ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate
        AND l_shipdate < l_commitdate
        AND l_receiptdate >= CAST('1994-01-01' AS date)
        AND l_receiptdate < CAST('1995-01-01' AS date);
