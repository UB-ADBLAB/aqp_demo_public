create role zzhao35 with login password '33bac9b35' ; 
create database tpcc_w10_zzhao35 with owner = 'zzhao35' template = testdb allow_connections = true; 
\c tpcc_w10_zzhao35  
alter table customer owner to zzhao35; 
alter table district owner to zzhao35;    
alter table history owner to zzhao35;
alter table item owner to zzhao35;  
alter table nation owner to zzhao35;
alter table neworder owner to zzhao35;  
alter table orders owner to zzhao35;  
alter table orderline owner to zzhao35; 
alter table partsupp owner to zzhao35;  
 alter table region owner to zzhao35; 
 alter table stock owner to zzhao35;
 alter table supplier owner to zzhao35; 
 alter table warehouse owner to zzhao35;