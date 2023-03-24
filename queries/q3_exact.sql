SELECT c_credit AS credit, SUM(ol_amount) as rev, COUNT(*) AS lcnt
FROM orderline,
     orders, customer
WHERE c_id =  o_c_id AND c_w_id  = o_w_id AND c_d_id = o_d_id
  AND ol_o_id =  o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id
GROUP BY credit
ORDER BY credit;
