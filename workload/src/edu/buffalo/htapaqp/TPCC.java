package edu.buffalo.htapaqp;

import java.sql.*;
import java.util.Random;
import java.util.Date;

class TPCC {
    private Connection mDBConn;
    private long mSeed;
    private int mTxnToRun;
    private int mMinWarehouse;
    private int mMaxWarehouse;
    private int mWarehouseTotal;
    private int i = 0;
    private Random mGenerator;
    private java.sql.Date mCurrentDate;
    private int mRemTxnToRunForCurrentDate;

    final int kDaysPerMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    // 2023-03-02
    final int kInitYear = 2023;
    final int kInitMon = 3;
    final int kInitDay = 2;
    final int kTxnPerDateLB = 2500;
    final int kTxnPerDateUB = 3500;

    private PreparedStatement newOrder;
    private PreparedStatement payment;
    private PreparedStatement delivery;

    TPCC(Connection dbConn, long seed, int minWarehouse, int maxWarehouse, int warehouseTotal)
            throws SQLException{
        mDBConn = dbConn;
        mSeed = seed;
        mGenerator = new Random(seed);
        mMinWarehouse = minWarehouse;
        mMaxWarehouse = maxWarehouse;
        mWarehouseTotal = warehouseTotal;
        mCurrentDate = new java.sql.Date(kInitYear - 1900, kInitMon - 1, kInitDay);
        mRemTxnToRunForCurrentDate =
            mGenerator.nextInt(kTxnPerDateUB + 1 - kTxnPerDateLB) + kTxnPerDateLB;


        newOrder = dbConn.prepareStatement("select no_trans(?, ?, ?, ?, ?, ?, ?, ?, ?)");
        payment = dbConn.prepareStatement("select payment_trans(?, ?, ?, ?, ?, ?, ?)");
        delivery = dbConn.prepareStatement("call delivery_trans(?, ?, ?)");
    }

    static boolean is_leap_year(int year) {
        return year % 400 == 0 || (year % 100 != 0 && year % 4 == 0);
    }

    public void run() {
        if(mTxnToRun == 1) {
            this.newOrderTransaction();
        }else if(mTxnToRun == 2) {
            this.payment_transaction();
        }else if (mTxnToRun == 3) {
            // TODO
        }else if (mTxnToRun == 4) {
            delivery_transaction();
        }else if (mTxnToRun == 5) {
            // TODO
        }
        if (--mRemTxnToRunForCurrentDate == 0)
        {
            int day = mCurrentDate.getDate();
            int mon = mCurrentDate.getMonth();
            int year = mCurrentDate.getYear();
            ++day;
            if (day > kDaysPerMonth[mon]) {
                if (mon == 1 && is_leap_year(year)) {
                    if (day > kDaysPerMonth[1] + 1) {
                        ++mon;
                        day = 1;
                    }
                } else {
                    ++mon;
                    day = 1;
                    if (mon > 11)
                    {
                        ++year;
                        mon = 0;
                    }
                }
            }
            mCurrentDate.setDate(day);
            mCurrentDate.setMonth(mon);
            mCurrentDate.setYear(year);
            mRemTxnToRunForCurrentDate =
                mGenerator.nextInt(kTxnPerDateUB + 1 - kTxnPerDateLB) + kTxnPerDateLB;
        }
    }

    public void newOrderTransaction(){
        try {
            //wid rng # between bounds given by driver
            short wID = (short)(this.mGenerator.nextInt(mMaxWarehouse+1 - mMinWarehouse) + mMinWarehouse);
            short dID = (short)(this.mGenerator.nextInt(10)+1);
            short cID = (short)this.NURand(1023, 1, 3000);
            int ol_cnt = this.mGenerator.nextInt(11) + 5;
            int rbk = this.mGenerator.nextInt(100)+1;
            Integer[] ol_i_ids = new Integer[ol_cnt];
            int x;
            Short[] ol_supply_w_ids = new Short[ol_cnt];
            Integer[] ol_quantity = new Integer[ol_cnt];
            for(int i = 0; i < ol_cnt; i++){
                ol_i_ids[i] = this.NURand(8191,1,100000);
                x = this.mGenerator.nextInt(100)+1;
                if(x == 1){
                    ol_supply_w_ids[i] = (short)(this.mGenerator.nextInt(mWarehouseTotal)+1);
                }else{
                    ol_supply_w_ids[i] = wID;
                }
                ol_quantity[i] = this.mGenerator.nextInt(10)+1;
            }
            java.sql.Date o_entry_date = mCurrentDate;
            if(rbk == 1){
                ol_i_ids[ol_cnt-1] = 0;
            }
            newOrder.setShort(1, wID);
            newOrder.setShort(2, dID);
            newOrder.setShort(3,cID);
            newOrder.setDate(4, o_entry_date);
            newOrder.setString(5, "oprior");
            newOrder.setString(6,"shipprior");
            newOrder.setArray(7, mDBConn.createArrayOf("INT4", ol_i_ids));
            newOrder.setArray(8, mDBConn.createArrayOf("INT2", ol_supply_w_ids));
            newOrder.setArray(9,mDBConn.createArrayOf("INT8", ol_quantity));
            newOrder.execute();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    public void payment_transaction(){
        try{
            short wID = (short)(this.mGenerator.nextInt(mMaxWarehouse+1 - mMinWarehouse) + mMinWarehouse);
            short dID = (short)(this.mGenerator.nextInt(10)+1);
            short cID = (short)this.NURand(1023, 1, 3000);
            int x = this.mGenerator.nextInt(100)+1;
            short cDID;
            short cWID = -1;
            if(x <= 85){
                cDID = dID;
                cWID = wID;
            }else{
                cDID = (short)(this.mGenerator.nextInt(10)+1);
                while(wID != cWID){
                    cWID = (short)(this.mGenerator.nextInt(mWarehouseTotal)+1);
                }
            }
            float hAmount = (float) ((this.mGenerator.nextInt(500001 - 100))/100.0);
            java.sql.Date o_entry_date = mCurrentDate;
            payment.setShort(1, wID);
            payment.setShort(2, dID);
            payment.setShort(3, cID);
            payment.setShort(4, cWID);
            payment.setShort(5, cDID);
            payment.setFloat(6, hAmount);
            payment.setDate(7, o_entry_date);
            payment.execute();
        }catch (SQLException e){
            System.out.println(e);
        }
    }

    public void delivery_transaction() {
        try {
            short wID = (short)(this.mGenerator.nextInt(mMaxWarehouse+1 - mMinWarehouse) + mMinWarehouse);
            short carrierID = (short)(mGenerator.nextInt(10) + 1);
            java.sql.Date delivery_d = mCurrentDate;

            delivery.setShort(1, wID);
            delivery.setShort(2, carrierID);
            delivery.setDate(3, delivery_d);
            delivery.execute();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    public int NURand(int A, int x, int y){
        int i = this.mGenerator.nextInt(A + 1);
        int j = this.mGenerator.nextInt(y - x + 1) + x;
        return (((i | j) + 100) % (y - x + 1))+x;
    }


    public void setTxnToRun(int txnToRun) {
        mTxnToRun = txnToRun;
    }
}
