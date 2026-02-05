CREATE TABLE 
    orders_f13
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_comment NOT LIKE '%special%requests%';
