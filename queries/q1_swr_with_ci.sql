
select total_rev as total_rev_est, total_rev_half_ci / total_rev as total_rev_rel_half_ci,
       lcnt as lcnt_est, lcnt_half_ci / lcnt as lcnt_rel_half_ci
from (
select 
    approx_sum(ol_amount) as total_rev, 
    approx_sum_half_ci(ol_amount, 0.95) as total_rev_half_ci,
    approx_count(*) as lcnt,
    approx_count_star_half_ci(0.95) as lcnt_half_ci
from orderline tablesample swr(500)
where ol_delivery_d < '2023-03-10') T;
