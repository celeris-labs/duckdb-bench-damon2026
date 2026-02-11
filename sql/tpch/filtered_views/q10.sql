CREATE TABLE 
    lineitem_f10
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_returnflag = 'R';

CREATE TABLE 
    orders_f10
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/orders.parquet')
    WHERE
        o_orderdate >= CAST('1993-10-01' AS date)
        AND o_orderdate < CAST('1994-01-01' AS date);

SELECT * FROM view_rewriter_add_rule('lineitem', "l_returnflag='R'", 'lineitem_f10');
SELECT * FROM view_rewriter_add_rule('orders', "o_orderdate>='1993-10-01'::DATE AND o_orderdate<'1994-01-01'::DATE", 'orders_f10');
