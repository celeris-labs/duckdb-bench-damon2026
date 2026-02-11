CREATE TABLE 
    lineitem_f15
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
        AND l_shipdate < CAST('1996-04-01' AS date);

SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate>='1996-01-01'::DATE AND l_shipdate<'1996-04-01'::DATE", 'lineitem_f14');
