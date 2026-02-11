CREATE TABLE 
    part_f20
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_name LIKE 'forest%';

CREATE TABLE 
    lineitem_f20
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date);

CREATE TABLE 
    nation_f20
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'CANADA';

SELECT * FROM view_rewriter_add_rule('part', "p_name>='forest' AND p_name<'foresu'", 'part_f20');
SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipdate>='1994-01-01'::DATE AND l_shipdate<'1995-01-01'::DATE", 'lineitem_f20');
SELECT * FROM view_rewriter_add_rule('nation', "n_name='CANADA'", 'nation_f20');
