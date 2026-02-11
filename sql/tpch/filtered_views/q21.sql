CREATE TABLE 
    orders_f21
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderstatus = 'F';

CREATE TABLE 
    nation_f21
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'SAUDI ARABIA';

SELECT * FROM view_rewriter_add_rule('orders', "o_orderstatus='F'", 'orders_f21');
SELECT * FROM view_rewriter_add_rule('nation', "n_name='SAUDI ARABIA'", 'nation_f21');
