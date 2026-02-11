CREATE TABLE 
    lineitem_f07
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date);

CREATE TABLE 
    nation_f07
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'FRANCE' 
        OR n_name = 'GERMANY';

SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate>='1995-01-01'::DATE AND l_shipdate<='1996-12-31'::DATE", 'lineitem_f07');
SELECT * FROM view_rewriter_add_rule('nation', "optional: n_name='GERMANY' OR n_name='FRANCE'", 'nation_f07');
SELECT * FROM view_rewriter_add_rule('nation', "optional: n_name='FRANCE' OR n_name='GERMANY'", 'nation_f07');
