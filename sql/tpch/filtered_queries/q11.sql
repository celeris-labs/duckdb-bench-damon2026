SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation_f11
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
GROUP BY
    ps_partkey
HAVING
    sum(ps_supplycost * ps_availqty) > (
        SELECT
            sum(ps_supplycost * ps_availqty) * 0.0001000000
        FROM
            partsupp,
            supplier,
            nation_f11
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey)
ORDER BY
    value DESC;
