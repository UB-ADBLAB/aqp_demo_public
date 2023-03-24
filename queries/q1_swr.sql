SELECT APPROX_SUM(ol_amount) AS rev,
       APPROX_COUNT(*) AS lcnt
FROM orderline TABLESAMPLE SWR(1000)
WHERE ol_delivery_d < '2023-03-10';
