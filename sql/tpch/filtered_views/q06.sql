CREATE TABLE 
    lineitem_f06
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05
        AND 0.07
        AND l_quantity < 24;

SELECT * FROM view_rewriter_add_rule('lineitem', "l_discount>=0.05 AND l_discount<=0.07 AND l_quantity<24.00 AND l_shipdate>='1994-01-01'::DATE AND l_shipdate<'1995-01-01'::DATE", 'lineitem_f06');
