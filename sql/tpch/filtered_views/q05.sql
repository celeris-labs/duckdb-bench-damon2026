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
    customer_f05
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/customer.parquet')
    WHERE
        c_custkey <= 1499999;

CREATE TABLE 
    region_f05
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/region.parquet')
    WHERE
        r_name = 'ASIA';

SELECT * FROM view_rewriter_add_rule('orders', "o_orderdate>='1994-01-01'::DATE AND o_orderdate<'1995-01-01'::DATE", 'orders_f05');
SELECT * FROM view_rewriter_add_rule('customer', "c_custkey<=1499999", 'customer_f05');
SELECT * FROM view_rewriter_add_rule('region', "r_name='ASIA'", 'region_f05');
