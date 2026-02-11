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

SELECT * FROM view_rewriter_add_rule('customer', '("substring"(c_phone, 1, 2) IN (''13'', ''31'', ''23'', ''29'', ''30'', ''18'', ''17'')) AND c_acctbal>0.00', 'customer_f22');
SELECT * FROM view_rewriter_add_rule('customer', '("substring"(c_phone, 1, 2) IN (''13'', ''31'', ''23'', ''29'', ''30'', ''18'', ''17''))', 'customer_f22');
