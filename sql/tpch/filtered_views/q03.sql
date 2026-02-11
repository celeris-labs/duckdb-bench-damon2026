CREATE TABLE 
    customer_f03 
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/customer.parquet')
    WHERE
        c_mktsegment = 'BUILDING';

CREATE TABLE 
    orders_f03
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate < CAST('1995-03-15' AS date);

CREATE TABLE 
    lineitem_f03
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate > CAST('1995-03-15' AS date);

SELECT * FROM view_rewriter_add_rule('customer', "c_mktsegment='BUILDING'", 'customer_f03');
SELECT * FROM view_rewriter_add_rule('orders', "o_orderdate<'1995-03-15'::DATE", 'orders_f03');
SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate>'1995-03-15'::DATE", 'lineitem_f03');
