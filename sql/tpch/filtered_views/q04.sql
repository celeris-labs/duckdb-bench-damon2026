CREATE TABLE 
    lineitem_f04
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_commitdate < l_receiptdate;

CREATE TABLE 
    orders_f04
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date);

SELECT * FROM view_rewriter_add_rule('orders', "o_orderdate>='1993-07-01'::DATE AND o_orderdate<'1993-10-01'::DATE", 'orders_f04');
