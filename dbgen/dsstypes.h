/*
* $Id: dsstypes.h,v 1.3 2005/10/28 02:57:04 jms Exp $
*
* Revision History
* ===================
* $Log: dsstypes.h,v $
* Revision 1.3  2005/10/28 02:57:04  jms
* allow for larger names in customer table
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.3  2004/04/07 20:17:29  jms
* bug #58 (join fails between order/lineitem)
*
* Revision 1.2  2004/01/22 05:49:29  jms
* AIX porting (AIX 5.1)
*
* Revision 1.1.1.1  2003/08/07 17:58:34  jms
* recreation after CVS crash
*
* Revision 1.2  2003/08/07 17:58:34  jms
* Convery RNG to 64bit space as preparation for new large scale RNG
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
 /* 
 * general definitions and control information for the DSS data types
 * and function prototypes
 */

/*
 * typedefs
 */
typedef struct
{
    DSS_HUGE        custkey;
    DSS_HUGE        district_key;
    DSS_HUGE        warehouse_key;
    char            name[C_NAME_LEN + 3];
    char            address[C_ADDR_MAX + 1];
    int             alen;
    DSS_HUGE        nation_code;		
    char            phone[PHONE_LEN + 1];
    char            mktsegment[MAXAGG_LEN + 1];
    char            credit[C_CRDT_LEN + 1];
    DSS_HUGE        creditlim;
    float        discount;
    DSS_HUGE        balance;
    DSS_HUGE           ytdpayment;
    DSS_HUGE        pymentcnt;
    DSS_HUGE        deliverycnt;
    char            data[C_CMNT_LEN +1];
    char            comment[C_CMNT_MAX + 1];
    int             clen;
}               customer_t;
/* customers.c */
long mk_cust   PROTO((DSS_HUGE n_cust, customer_t * c, 
                      DSS_HUGE ware_id, DSS_HUGE dist_id));
int pr_cust    PROTO((customer_t * c, int mode));
int ld_cust    PROTO((customer_t * c, int mode));


typedef struct
{
	DSS_HUGE no_orderId;
	DSS_HUGE no_districtId;
	DSS_HUGE no_warehouseId;
} neworder_t;

long mk_neworder PROTO((DSS_HUGE index, neworder_t *new_order,
			            DSS_HUGE ware_id, DSS_HUGE dist_id));
int pr_neworder PROTO((neworder_t *neworder, int mode));
typedef struct
{
    DSS_HUGE	    ol_o_id;
	DSS_HUGE		ol_d_id;
	DSS_HUGE		ol_w_id;
    DSS_HUGE            ol_number;
    DSS_HUGE            ol_item_id;
	DSS_HUGE		sup_w_key; 
    char            deliver_date[DATE_LEN+1];
    int             quantity;    
    DSS_HUGE        amount;
    char            ol_dist_info[OL_DIST_LEN+1];
}               order_line_t;
long mk_orderline PROTO((DSS_HUGE index, order_line_t *orderline, 
			             DSS_HUGE order_id, DSS_HUGE ware_id, 
			                DSS_HUGE dist_id, int numberof_days, DSS_HUGE temp_day));
int pr_orderline  PROTO((order_line_t *orderline, int mode));

typedef struct
{
    DSS_HUGE	    okey;
	DSS_HUGE		distkey;
	DSS_HUGE		warekey; 
    DSS_HUGE        custkey;
    DSS_HUGE		carrierId; 
    char            o_en_date[DATE_LEN + 1];
    DSS_HUGE        o_ol_cnt;
    DSS_HUGE             o_all_local;
    char            o_order_priority[MAXAGG_LEN + 1];
    DSS_HUGE            spriority;

}               order_t;

/* order.c */
long	mk_order	PROTO((DSS_HUGE index, order_t * o, 
                            DSS_HUGE dist_id, DSS_HUGE ware_id, 
                            DSS_HUGE cust_id, int numberof_days));
int		pr_order	PROTO((order_t * o, int mode));
int		ld_order	PROTO((order_t * o, int mode));
void	mk_sparse	PROTO((DSS_HUGE index, DSS_HUGE *ok, long seq));

typedef struct
{
    DSS_HUGE            itemkey;
    DSS_HUGE            suppkey;
    DSS_HUGE            qty;
    DSS_HUGE            scost;
    char           comment[PS_CMNT_MAX + 1];
    int            clen;
}               partsupp_t;

typedef struct
{
    DSS_HUGE           itemkey;
    DSS_HUGE            i_im_id;
    char           name[P_NAME_LEN + 1];
    int            nlen;
    char           mfgr[P_MFG_LEN + 1];
    char           brand[P_BRND_LEN + 1];
    char           type[P_TYPE_LEN + 1];
    int            tlen;
    DSS_HUGE           size;
    char           container[P_CNTR_LEN + 1];
    DSS_HUGE           retailprice;
    char           data[C_DATA_MAX +1];
    char           comment[P_CMNT_MAX + 1];
    int            clen;
    partsupp_t     s[SUPP_PER_ITEM];
}               item_t;

/* parts.c */
long mk_part   PROTO((DSS_HUGE index, item_t * p));
int pr_part    PROTO((item_t * part, int mode));
int ld_part    PROTO((item_t * part, int mode));

typedef struct
{
    DSS_HUGE            suppkey;
    char            name[S_NAME_LEN + 1];
    char            address[S_ADDR_MAX + 1];
    int             alen;
    DSS_HUGE            nation_code;
    char            phone[PHONE_LEN + 1];
    DSS_HUGE            acctbal;
    char            comment[S_CMNT_MAX + 1];
    int             clen;
}               supplier_t;
/* supplier.c */
long mk_supp   PROTO((DSS_HUGE index, supplier_t * s));
int pr_supp    PROTO((supplier_t * supp, int mode));
int ld_supp    PROTO((supplier_t * supp, int mode));

typedef struct
{
    DSS_HUGE            timekey;
    char            alpha[DATE_LEN];
    long            year;
    long            month;
    long            week;
    long            day;
} dss_time_t;               

/* time.c */
long mk_time   PROTO((DSS_HUGE h, dss_time_t * t));

/*
 * this assumes that N_CMNT_LEN >= R_CMNT_LEN 
 */
typedef struct
{
    DSS_HUGE            code;
    char            *text;
    long            join;
    char            comment[N_CMNT_MAX + 1];
    int             clen;
} code_t;

typedef struct
{
	DSS_HUGE distkey;
	DSS_HUGE dist_ware_key;
	char name[D_NAME_LEN + 1];
	char address[D_ADDR_MAX + 1];
	DSS_HUGE nation_code;
	int alen;
    float tax;
	DSS_HUGE ytd;
	DSS_HUGE next_oid;
} district_t;
long mk_district PROTO((DSS_HUGE n_dist, district_t * d, DSS_HUGE warehosuekey, DSS_HUGE nextoid));
int pr_district PROTO((DSS_HUGE n_dist, district_t * d, DSS_HUGE warehosuekey));


typedef struct
{
	DSS_HUGE stockkey;
	DSS_HUGE stockWarehousekey;
	DSS_HUGE quantity;
    DSS_HUGE ytd;
    DSS_HUGE stock_order_cnt;
    DSS_HUGE stock_remote_cnt;
    char     s_data[C_DATA_MAX+1];
} stock_t;

long mk_stock	PROTO((DSS_HUGE index, stock_t * s, DSS_HUGE warehouse_key));
int pr_stock	PROTO((stock_t * stock, int mode));
int ld_stock	PROTO((stock_t * stock, int mode));

typedef struct
{
	DSS_HUGE warehousekey;
	char name[W_NAME_LEN + 1];
	char address[W_ADDR_MAX + 1];
	int alen;
	float tax;
	DSS_HUGE YTD;
} ware_t;
long mk_ware PROTO((DSS_HUGE n_ware, ware_t *w));
int  pr_ware PROTO((ware_t *w, int mode));

typedef struct
{
	DSS_HUGE histCustID;
	DSS_HUGE histCustDistID;
	DSS_HUGE histCustWareID;
	DSS_HUGE histDistID;
	DSS_HUGE histWareID;
    char     hist_date[DATE_LEN];
	DSS_HUGE histAmt;
	char	 histData[H_DATA_MAX+1];
} hist_t; 

long mk_hist    PROTO((DSS_HUGE index, hist_t* h, DSS_HUGE cust_id, 
		                DSS_HUGE ware_id, DSS_HUGE dist_id));
int pr_hist     PROTO((hist_t *hist, int mode));

/* code table */
int mk_nation   PROTO((DSS_HUGE i, code_t * c));
int pr_nation    PROTO((code_t * c, int mode));
int ld_nation    PROTO((code_t * c, int mode));
int mk_region   PROTO((DSS_HUGE i, code_t * c));
int pr_region    PROTO((code_t * c, int mode));
int ld_region    PROTO((code_t * c, int mode));

