create role frontend with login password '+zaZawb4=' ; 
create database tpcc_w100_front with owner = 'frontend' template = tpcc_w100_d10 allow_connections = true; 
\c tpcc_w100_front  
alter table customer owner to frontend; 
alter table district owner to frontend;    
alter table history owner to frontend;
alter table item owner to frontend;  
alter table nation owner to frontend;
alter table neworder owner to frontend;  
alter table orders owner to frontend;  
alter table orderline owner to frontend; 
alter table partsupp owner to frontend;  
 alter table region owner to frontend; 
 alter table stock owner to frontend;
 alter table supplier owner to frontend; 
 alter table warehouse owner to frontend;