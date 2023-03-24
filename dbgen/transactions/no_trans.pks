CREATE FUNCTION NO_TRANS(PW_ID INT2, PD_ID INT2, PC_ID INT2, 
	PO_DATE date, PO_ORDERPRIOR text, PO_SHIPPRIOR text,  
	POL_I_IDS INT4[], POL_SUPPLY_W_IDS INT2[], POL_QUANTITY INT8[])
RETURNS RECORD 
LANGUAGE 'plpgsql'
AS $$
DECLARE 
	wTax real := 0;
	dTax real := 0;
	cDiscount real := 0;
	cName text := '';
	cCredit text := '';
	dNextOId integer := 0;
	sQuantity integer := 0;
	sData text := '';
	iData text := '';
	iPrice real := 0; 
	iName text := '';
	brandGeneric text := '';
	allLocal integer := 1; --passed?
	olAmt real := 0;
	totalAmt real := 0;
	oOlCnt int; 
	rec Record;

BEGIN
	SELECT W_TAX INTO wTax FROM WAREHOUSE WHERE w_id=PW_ID;
	SELECT d_tax, d_next_o_id INTO dTax, dNextOId FROM DISTRICT WHERE d_w_id=PW_ID AND d_id=PD_ID;
	UPDATE district SET d_next_o_id = d_next_o_id + 1 
		WHERE d_w_id=PW_ID AND d_id=PD_ID;

	SELECT c_discount, c_name, c_credit INTO cDiscount, cName, cCredit
	FROM CUSTOMER WHERE c_w_id=PW_ID AND c_d_id=PD_ID AND c_id=PC_ID;

	INSERT INTO neworder VALUES (dNextOId, PD_ID, PW_ID);
	
	oOlCnt := array_length(POL_SUPPLY_W_IDS, 1);
	FOR i IN 1..oOlCnt LOOP
		IF POL_SUPPLY_W_IDS[i] != PW_ID THEN
			PW_ID = 0;
		END IF;
	END LOOP;
	
	INSERT INTO orders VALUES (dNextOId, PD_ID, PW_ID, PC_ID, NULL,
		PO_DATE, oOlCnt, allLocal, PO_ORDERPRIOR, PO_SHIPPRIOR); 

	FOR i IN 1..oOlCnt LOOP
		SELECT i_price, i_name, i_data INTO iPrice, iName, iData FROM ITEM 
			WHERE i_id = POL_I_IDS[i];			

		SELECT st_quantity, st_data INTO sQuantity, sData FROM STOCK WHERE
			st_i_id = POL_I_IDS[i] and st_w_id = POL_SUPPLY_W_IDS[i]; 
		
		IF POL_QUANTITY[i] + 9 < sQuantity THEN
			UPDATE STOCK SET st_quantity = st_quantity - POL_QUANTITY[i]
				WHERE st_i_id = POL_I_IDS[i] and st_w_id = POL_SUPPLY_W_IDS[i]; 
		ELSE
			UPDATE STOCK SET st_quantity = st_quantity - POL_QUANTITY[i] + 91
				WHERE st_i_id = POL_I_IDS[i] and st_w_id = POL_SUPPLY_W_IDS[i]; 
		END IF;

		UPDATE STOCK SET st_ytd = st_ytd + POL_QUANTITY[i], st_remote_cnt = st_remote_cnt + 1
			WHERE st_i_id = POL_I_IDS[i] and st_w_id = POL_SUPPLY_W_IDS[i]; 
 
		IF iData SIMILAR TO '%ORIGINAL%' AND sData SIMILAR TO '%ORIGINAL%' THEN
			brandGeneric = 'B';
		ELSE
			brandGeneric = 'G';
		END IF;
		
		olAmt = POL_QUANTITY[i] * iPrice;
		
		INSERT INTO orderline VALUES(dNextOId, pd_id, pw_id, i, POL_I_IDS[i],
					POL_SUPPLY_W_IDS[i], NULL, POL_QUANTITY[i], olAmt, 'del this');
					
		totalAmt = totalAmt + olAmt*(1-cDiscount)*(1+wTax+dTax);
	END LOOP;
	rec := (totalAmt,olAmt, cDiscount,wTax, dTax,9);
	return rec;
END;
$$;
