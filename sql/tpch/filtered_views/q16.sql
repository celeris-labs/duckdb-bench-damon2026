CREATE TABLE 
    supplier_f16
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/supplier.parquet')
    WHERE
        s_comment LIKE '%Customer%Complaints%';

CREATE TABLE 
    part_f16
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        p_brand <> 'Brand#45'
        AND p_type NOT LIKE 'MEDIUM POLISHED%'
        AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9);
