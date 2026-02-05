SELECT
    s_name,
    s_address
FROM
    supplier,
    nation_f20
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part_f20)
            AND ps_availqty > (
                SELECT
                    0.5 * sum(l_quantity)
                FROM
                    lineitem_f20
                WHERE
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey))
            AND s_nationkey = n_nationkey
        ORDER BY
            s_name;
