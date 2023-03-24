CREATE OR REPLACE PROCEDURE public.delivery_trans(
    PW_ID int2, PCARRIER_ID INT2, PDELIVERY_D DATE)
LANGUAGE 'plpgsql'
AS $$
DECLARE
    D_ID int2;
    CUR_O_ID int4;
    O_C_ID int2;
    AMT FLOAT8;
BEGIN
    FOR D_ID in 1 .. 10 LOOP
        FOR CUR_O_ID IN
            SELECT NO_O_ID
            FROM NEWORDER
            WHERE NO_W_ID = PW_ID AND NO_D_ID = D_ID
            ORDER BY NO_O_ID ASC
            LIMIT 10
            FOR UPDATE
        LOOP
            DELETE FROM NEWORDER WHERE NO_W_ID = PW_ID AND NO_D_ID = D_ID
                                   AND NO_O_ID = CUR_O_ID;

            UPDATE ORDERS SET O_CARRIER_ID = PCARRIER_ID
            WHERE O_W_ID = PW_ID AND O_D_ID = D_ID
              AND O_ID = CUR_O_ID
            RETURNING orders.O_C_ID INTO O_C_ID;
            
            UPDATE ORDERLINE SET OL_DELIVERY_D = PDELIVERY_D
            WHERE OL_W_ID = PW_ID AND OL_D_ID = D_ID
              AND OL_O_ID = CUR_O_ID;
            
            SELECT SUM(OL_AMOUNT) INTO AMT
            FROM ORDERLINE
            WHERE OL_W_ID = PW_ID AND OL_D_ID = D_ID
              AND OL_O_ID = CUR_O_ID;

            UPDATE CUSTOMER
            SET C_BALANCE = C_BALANCE + AMT,
                C_DELIVERY_CNT = C_DELIVERY_CNT + 1
            WHERE C_W_ID = PW_ID AND C_D_ID = D_ID
              AND C_ID = O_C_ID;
        END LOOP;
    END LOOP;
END;
$$
