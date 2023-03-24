create role dylanzin with login password 'ysHRRDGj' ; 
create database tpcc_w10_dylan with owner = 'dylanzin' template = testdb allow_connections = true; 
\c tpcc_w10_dylan  
alter table customer owner to dylanzin; 
alter table district owner to dylanzin;    
alter table history owner to dylanzin;
alter table item owner to dylanzin;  
alter table nation owner to dylanzin;
alter table neworder owner to dylanzin;  
alter table orders owner to dylanzin;  
alter table orderline owner to dylanzin; 
alter table partsupp owner to dylanzin;  
 alter table region owner to dylanzin; 
 alter table stock owner to dylanzin;
 alter table supplier owner to dylanzin; 
 alter table warehouse owner to dylanzin;


 select count(*), count(distinct o_id) from orders where o_w_id = 1 and o_d_id = 1;
 select sum(ol_amount) as y, stddev_pop(ol_amount) as delta from orderline where ol_delivery_d < '2023-02-28';