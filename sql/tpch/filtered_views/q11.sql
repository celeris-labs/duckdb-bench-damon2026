CREATE TABLE 
    nation_f11
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/nation.parquet')
    WHERE
        n_name = 'GERMANY';
