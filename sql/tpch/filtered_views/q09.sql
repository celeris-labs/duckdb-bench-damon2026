CREATE TABLE 
    part_f09
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_name LIKE '%green%';
