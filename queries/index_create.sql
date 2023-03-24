ALTER TABLE WAREHOUSE ADD PRIMARY KEY (w_id) INCLUDE (w_tax);
ALTER TABLE DISTRICT ADD PRIMARY KEY (D_W_ID, D_ID),
                     ADD FOREIGN KEY (D_W_ID) REFERENCES WAREHOUSE (W_ID);
ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
                        INCLUDE (C_DISCOUNT, C_NAME, C_CREDIT),
                     ADD FOREIGN KEY (C_W_ID, C_D_ID)
                        REFERENCES DISTRICT (D_W_ID, D_ID);
ALTER TABLE HISTORY ADD FOREIGN KEY (H_C_W_ID, H_C_D_ID, H_C_ID)
                            REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID),
                    ADD FOREIGN KEY (H_W_ID, H_D_ID)
                            REFERENCES DISTRICT(D_W_ID, D_ID);
ALTER TABLE ORDERS ADD PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
                   ADD FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID)
                        REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID);
ALTER TABLE NEWORDER ADD PRIMARY KEY (NO_O_ID, NO_D_ID, NO_W_ID),
                     ADD FOREIGN KEY (NO_W_ID, NO_D_ID, NO_O_ID)
                            REFERENCES ORDERS(O_W_ID, O_D_ID, O_ID);
ALTER TABLE ITEM ADD PRIMARY KEY (I_ID)
                    INCLUDE(I_PRICE, I_NAME, I_DATA);
ALTER TABLE STOCK ADD PRIMARY KEY (ST_I_ID, ST_W_ID),
                  ADD FOREIGN KEY (ST_W_ID) REFERENCES WAREHOUSE(W_ID),
                  ADD FOREIGN KEY (ST_I_ID) REFERENCES ITEM(I_ID);
ALTER TABLE ORDERLINE ADD PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
                      ADD FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID)
                            REFERENCES ORDERS (O_W_ID, O_D_ID, O_ID),
                      ADD FOREIGN KEY(OL_SUPPLY_W_ID, OL_I_ID)
                            REFERENCES STOCK(ST_W_ID, ST_I_ID);
create index on orderline using abtree(ol_delivery_d) with 
 (aggregation_type = int8, agg_support = abt_count_support);

