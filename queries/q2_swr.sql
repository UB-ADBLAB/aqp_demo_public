SELECT OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_AMOUNT
FROM orderline
WHERE ol_amount > 1.5 * (
       SELECT AVG(ol_amount)
       FROM orderline TABLESAMPLE SWR(500))
LIMIT 10;
