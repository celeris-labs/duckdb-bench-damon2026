CREATE TABLE 
    orders_f05
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate >= CAST('1994-01-01' AS date)
        AND o_orderdate < CAST('1995-01-01' AS date);

CREATE TABLE 
    region_f05
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/region.parquet')
    WHERE
        r_name = 'ASIA';
