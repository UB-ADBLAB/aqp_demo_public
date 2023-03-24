--change decimal to double/ float8 or double precision
-- signed numric to double
-- all text is text 
-- identifier --> INT8/ BIGINT
-- DSSHUGE --> BIGINT

-- create customer table, need to be change later 
CREATE TABLE customer (
    c_id INT2,
    c_d_id INT2,
    c_w_id INT2, 
    c_name  TEXT,
    c_address TEXT,
    c_nationkey INT2,
    c_phone  TEXT,
    c_balance float8,
    c_mktsegment  TEXT,
    c_credit  char(2),
    c_credit_lim  float8,
    c_discount float8,
    c_ytd_payment float8,
    c_payment_cnt float8,
    c_delivery_cnt INT8,
    c_data TEXT,
    c_comment  TEXT 
);

CREATE TABLE orders(
    o_id INT4,
    o_d_id INT2,
    o_w_id INT2,
    o_c_id INT2,
    o_carrier_id INT2,
    o_entry_d DATE,
    o_ol_cnt INT4,
    o_all_local INT8,
    o_orderpriority TEXT,
    o_shippriority TEXT 
);

CREATE TABLE orderline(
    ol_o_id INT4,
    ol_d_id INT2,
    ol_w_id INT2,
    ol_number INT2,
    ol_i_id INT4,
    ol_supply_w_id INT2,
    ol_delivery_d DATE,
    ol_quantity INT8,
    ol_amount float8,
    ol_dist_info TEXT 
);

CREATE TABLE neworder(
    no_o_id  INT4,
    no_d_id INT2,
    no_w_id INT2 
);

CREATE TABLE item(
    
    i_id  INT4,
    i_name  TEXT,
    i_mfgr  TEXT,
    i_brand  TEXT,
    i_type  TEXT,
    i_size  INT8,
    i_container  TEXT,
    i_price  float8,
    i_data  TEXT 
);

CREATE TABLE partsupp(
    ps_i_id  INT8,
    ps_supp_id  INT8,
    ps_availqty  INT8,
    ps_supplycost  float8,
    ps_data  TEXT 
);

CREATE TABLE supplier(
    su_supp_id  INT4,
    su_name  TEXT,
    su_address  TEXT,
    su_nationkey  INT2,
    su_phone  TEXT,
    su_acctbal  float8,
    su_comment  TEXT 
);

CREATE TABLE nation(
    n_nationkey  INT2,
    n_name  TEXT,
    n_regionkey  INT2,
    n_comment  TEXT 
); 

CREATE TABLE region (
    r_regionkey  INT2,
    r_name  TEXT,
    r_comment  TEXT 
);

CREATE TABLE warehouse(
    w_id  INT2,
    w_name  TEXT,
    w_address  TEXT,
    w_tax  float8,
    w_ytd  float8 
);

CREATE TABLE stock(
    st_i_id INT4, 
    st_w_id INT2,
    st_quantity INT8,
    st_ytd float8,
    st_order_cnt INT8, 
    st_remote_cnt  INT8,
    st_data  TEXT 
);

CREATE TABLE history(
    h_c_id  INT8,
    h_c_d_id  INT2,
    h_c_w_id  INT2,
    h_d_id  INT2,
    h_w_id  INT2,
    h_date  DATE,
    h_amount  float8,
    h_data  TEXT 
);

CREATE TABLE district (
    d_id  INT2,
    d_w_id  INT2,
    d_name  TEXT,
    d_address  TEXT,
    d_nationkey  INT2,
    d_tax  float8,
    d_ytd  float8,
    d_next_o_id  INT4 
);


