SELECT c_credit AS credit, APPROX_SUM(ol_amount) as rev, APPROX_COUNT(*) AS lcnt
FROM orderline TABLESAMPLE BERNOULLI(0.5),
     orders, customer
WHERE c_id =  o_c_id AND c_w_id  = o_w_id AND c_d_id = o_d_id
     AND ol_o_id =  o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
GROUP BY credit
ORDER BY credit;
