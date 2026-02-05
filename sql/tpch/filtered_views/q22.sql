CREATE TABLE 
    customer_f22
AS 
    SELECT 
        * 
    FROM 
        read_parquet('data/tpch/customer.parquet')
    WHERE
        c_acctbal > 0.00
        AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17');
