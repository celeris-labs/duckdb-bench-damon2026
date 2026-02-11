CREATE TABLE 
    part_f17
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_brand = 'Brand#23'
        AND p_container = 'MED BOX';

SELECT * FROM view_rewriter_add_rule('part', "p_brand='Brand#23' AND p_container='MED BOX'", 'part_f17');
