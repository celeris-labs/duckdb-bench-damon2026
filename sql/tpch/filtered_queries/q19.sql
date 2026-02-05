SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem_f19,
    part_f19
WHERE (p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10)
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10)
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10);
