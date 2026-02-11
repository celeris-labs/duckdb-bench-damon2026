CREATE TABLE 
    region_f08
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/region.parquet')
    WHERE
        r_name = 'AMERICA';

CREATE TABLE 
    orders_f08
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date);

CREATE TABLE 
    part_f08
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_type = 'ECONOMY ANODIZED STEEL';

SELECT * FROM view_rewriter_add_rule('region', "r_name='AMERICA'", 'region_f08');
SELECT * FROM view_rewriter_add_rule('orders', "o_orderdate>='1995-01-01'::DATE AND o_orderdate<='1996-12-31'::DATE", 'orders_f08');
SELECT * FROM view_rewriter_add_rule('part', "p_type='ECONOMY ANODIZED STEEL'", 'part_f08');
