CREATE TABLE 
    lineitem_f01 
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate <= CAST('1998-09-02' AS date);

SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate<='1998-09-02'::DATE", 'lineitem_f01');
