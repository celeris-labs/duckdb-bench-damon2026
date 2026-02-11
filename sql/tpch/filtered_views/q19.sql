CREATE TABLE 
    part_unused
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/part.parquet')
    WHERE
        (p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND p_size BETWEEN 1 AND 5)
        OR (p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND p_size BETWEEN 1 AND 10)
        OR (p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND p_size BETWEEN 1 AND 15);

CREATE TABLE 
    lineitem_f19
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/lineitem.parquet')
    WHERE
        l_quantity >= 1
        AND l_quantity <= 20 + 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON';

SELECT * FROM view_rewriter_add_rule('lineitem', "l_shipinstruct='DELIVER IN PERSON' AND optional: l_shipmode IN ('AIR', 'AIR REG')", 'lineitem_f19');
