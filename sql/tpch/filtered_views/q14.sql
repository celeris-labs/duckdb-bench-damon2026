CREATE TABLE 
    lineitem_f14
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= date '1995-09-01'
        AND l_shipdate < CAST('1995-10-01' AS date);

SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate>='1995-09-01'::DATE AND l_shipdate<'1995-10-01'::DATE", 'lineitem_f14');
