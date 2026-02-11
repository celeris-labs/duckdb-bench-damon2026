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

SELECT * FROM view_rewriter_add_rule('lineitem', "l_commitdate<'1995-01-01'::DATE AND l_receiptdate>='1994-01-01'::DATE AND l_receiptdate<'1995-01-01'::DATE AND l_shipdate<'1995-01-01'::DATE AND optional: l_shipmode IN ('MAIL', 'SHIP')", 'lineitem_f12');
