CREATE TABLE 
    region_f02 
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/region.parquet')
    WHERE
        r_name = 'EUROPE';

CREATE TABLE 
    part_f02
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_size = 15
        AND p_type LIKE '%BRASS';