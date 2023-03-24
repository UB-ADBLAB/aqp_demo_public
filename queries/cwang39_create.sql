create database tpcc_w100_cwang39 with owner = 'cwang39' template = tpcc_w100_d10 allow_connections = true; 
\c tpcc_w100_cwang39  
alter table customer owner to cwang39; 
alter table district owner to cwang39;    
alter table history owner to cwang39;
alter table item owner to cwang39;  
alter table nation owner to cwang39;
alter table neworder owner to cwang39;  
alter table orders owner to cwang39;  
alter table orderline owner to cwang39; 
alter table partsupp owner to cwang39;  
 alter table region owner to cwang39; 
 alter table stock owner to cwang39;
 alter table supplier owner to cwang39; 
 alter table warehouse owner to cwang39;