CREATE TABLE 
    nation_f11
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'GERMANY';

SELECT * FROM view_rewriter_add_rule('nation', "n_name='GERMANY'", 'nation_f11');
