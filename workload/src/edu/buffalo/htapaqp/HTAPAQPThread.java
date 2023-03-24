package edu.buffalo.htapaqp;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HTAPAQPThread extends Thread {
    private int mThreadId; // thread id
    private Connection mDbConn; // connection object specific to this thread
    private String[] mTxns; // transactions passed to this thread
    private String[] mTxnPercents; // probability of transactions
    private long mSeed; // seed for random number
    private Long mSeedValue; // random number generator
    private List<Double> txnIntervals; // list to hold probability intervals
    private volatile boolean bContinue = true;
    private int mWMin;
    private int mWMax;
    private int mWarehouseTotal;
    private boolean mIsSampling;
    private long mSampleSize;

    public HTAPAQPThread(int threadId, Connection dbConn,
                         String[] txns,
                         String[] txnPercents,
                         Long seed, int wMin, int wMax, int wTotal) {
        mIsSampling = false;
        mThreadId = threadId;
        mDbConn = dbConn;
        mTxns = txns;
        mTxnPercents = txnPercents;
        mSeedValue = seed;
        mWMin = wMin;
        mWMax = wMax;
        mWarehouseTotal = wTotal;

        txnIntervals = new ArrayList<Double>();
        Double sum = 0.0;
        for (int i = 0; i < mTxnPercents.length; i++) {
            Double percent = Double.parseDouble(mTxnPercents[i]) * 0.01;
            sum += percent;
            txnIntervals.add(sum);
        }
    }

    public HTAPAQPThread(int threadId, Connection dbConn, long sample_size) {
        mIsSampling = true;
        mThreadId = threadId;
        mDbConn = dbConn;
        mSampleSize = sample_size;
    }

    /**
     * each thread creates an instance of the AQP query and runs it
     */
    public void run() {
        if (mIsSampling) {
            runSampling();
        } else {
            runTPCC();
        }
    }

    private void runSampling() {
        PreparedStatement stmt;
        try {
            stmt = mDbConn.prepareStatement("select count(*) from orderline tablesample aqp.swr(" + mSampleSize + ")");
        } catch (SQLException e) {
            System.out.println(e);
            return ;
        }
        while (bContinue) {
            try {
                stmt.execute();
            } catch (SQLException e) {
                System.out.println(e);
            }
        }
    }

    private void runTPCC() {
        TPCC txn = null;
        try {
            txn = new TPCC(mDbConn, mSeed, mWMin, mWMax, mWarehouseTotal);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int txnToRun;
        double ranNum;
        Random rng = new Random(mSeedValue);

        while (bContinue) {
            ranNum = rng.nextDouble();
            txnToRun = 0;
            for (int i = 0; i < txnIntervals.size(); i++) {
                if (txnIntervals.get(i) > ranNum) {
                    txnToRun = i;
                    break;
                }
            }
            txn.setTxnToRun(txnToRun);
            txn.run();
        }

    }

    public void stopExec() {
        bContinue = false;
    }
}

