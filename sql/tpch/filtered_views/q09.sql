CREATE TABLE 
    part_f09
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_name LIKE '%green%';

SELECT * FROM view_rewriter_add_rule('part', "contains(p_name, 'green')", 'part_f09');
