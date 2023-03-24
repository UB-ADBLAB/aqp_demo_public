SELECT SUM(ol_amount) as rev,
       COUNT(*) as lcnt
FROM orderline
WHERE ol_delivery_d < '2023-03-10';
