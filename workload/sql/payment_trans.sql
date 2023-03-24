CREATE OR REPLACE FUNCTION public.payment_trans(PW_ID int2, PD_ID int2, PC_ID int2, 
	PC_W_ID int2, PC_D_ID int2, PH_AMOUNT float8, PH_DATE date)
RETURNS RECORD 
LANGUAGE 'plpgsql'
AS $$
DECLARE 
	wName text;
	wAddress text;
	wYtd float8;
	cName text;
	cAddress text;
	cPhone text;
	cCredit text;
	cCreditLim float8;
	cDiscount float8;
	cBalance float8;
	cData text;
	dName text; 
	dAddress text;
	dYtd float8; 
	rec Record;
	
BEGIN
	SELECT w_name, w_address, w_ytd INTO wName, wAddress, wYtd FROM warehouse where w_id=PW_ID;
	SELECT d_name, d_address, d_ytd INTO dName, dAddress, dYtd FROM district WHERE d_w_id=PW_ID AND d_id=PD_ID;
	UPDATE warehouse SET w_ytd = w_ytd + PH_AMOUNT WHERE w_id=PW_ID;
	SELECT c_name, c_address, c_phone, c_credit, c_credit_lim, c_discount,
	c_balance INTO cName, cAddress, cPhone, cCredit, cCreditLim, cDiscount, cBalance
	FROM customer WHERE PC_ID=c_id AND PC_W_ID=c_w_id AND PC_D_ID=c_d_id;
	UPDATE customer SET c_balance=c_balance-PH_AMOUNT,  c_ytd_payment=c_ytd_payment+PH_AMOUNT,
	c_payment_cnt = c_payment_cnt+1 WHERE PC_ID=c_id AND PC_W_ID=c_w_id AND PC_D_ID=c_d_id;
	
	IF cCredit='BC' THEN
	SELECT c_data INTO cData FROM customer WHERE PC_ID=c_id AND PC_W_ID=c_w_id AND PC_D_ID=c_d_id;
	END IF;
	INSERT INTO history VALUES(PC_ID, PC_D_ID, PC_W_ID, PD_ID, PW_ID, PH_DATE, PH_AMOUNT, wName||dName);
	rec := (PW_ID,PD_ID);
	return rec;
END;
$$;

