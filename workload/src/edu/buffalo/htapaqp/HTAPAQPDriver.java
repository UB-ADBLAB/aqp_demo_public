package edu.buffalo.htapaqp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

public class HTAPAQPDriver {
    private final String mDbConnUrl;
    private Properties mConnProps;
    private int mNumThreads;
    private int mNumSamplingThreads;
    private final String mConnPidQuery = "SELECT pg_backend_pid();";
    private final String mWarehouseQuery = "SELECT count(*) FROM warehouse;";
    private List<Connection> mDBConnList;
    private List<String> mConnPids;
    private String arrOfTxn[];
    private String arrOfTxnPercent[];
    private int mNumWarehouses;
    private long mSampleSize;

    public HTAPAQPDriver(int numThreads, int numSamplingThreads, String hostName, String port, String dbName, String userName, String password, String txns, String txnPercents, long sample_size) {
        mDbConnUrl = "jdbc:postgresql://" + hostName + ":" + port + "/" + dbName;
        mNumThreads = numThreads;
        mNumSamplingThreads = numSamplingThreads;
        mConnProps = new Properties();
        mConnProps.setProperty("user", userName);
        mConnProps.setProperty("password", password);
        mSampleSize = sample_size;

        arrOfTxn = txns.split(",");
        arrOfTxnPercent = txnPercents.split(",");

        makeConnection();
        fetchProcessIds();
        reportProcessIds();
        if (numThreads > 0) {
            try{
                PreparedStatement st = mDBConnList.get(0).prepareStatement(mWarehouseQuery);
                ResultSet wareNum = st.executeQuery();
                wareNum.next();
                mNumWarehouses = wareNum.getInt(1);
                if (mNumWarehouses <= 0 || mNumWarehouses < mNumThreads){
                    System.out.println("Warehouse # " + mNumWarehouses + " must be greater than 0 and numTxns");
                    System.exit(1);
                }
            } catch (SQLException e){
                System.out.println("failed to get warehouse #: " + e.getMessage());
                System.exit(1);
            }
        } else {
            /* no TPCC thread asked -- don't care about W */
            mNumWarehouses = 0;
        }
    }

    private void makeConnection() {
        mDBConnList = new ArrayList<Connection>();
        for (int i = 0; i < mNumThreads + mNumSamplingThreads; i++) {
            try {
                Connection mDbConn = DriverManager.getConnection(mDbConnUrl, mConnProps);
                mDBConnList.add(mDbConn);
            } catch (SQLException e) {
                System.out.println("failed to connect " + e.getMessage());
                System.exit(1);
            }
        }
    }

    private void fetchProcessIds() {
        mConnPids = new ArrayList<String>();
        ResultSet pIdQueryRS;

        for (int i = 0; i < mDBConnList.size(); i++) {
            try {
                PreparedStatement st = mDBConnList.get(i).prepareStatement(mConnPidQuery);
                pIdQueryRS = st.executeQuery();
                while (pIdQueryRS.next()) {
                    mConnPids.add(pIdQueryRS.getString(1));
                }
            } catch (SQLException e) {
                System.out.println("failed to execute query to fetch process id : " + e.getMessage());
            }
        }
    }

    private void reportProcessIds() {
        StringJoiner pIds = new StringJoiner(" ");
        pIds.add("PIDS");
        for (int i = 0; i < mConnPids.size(); i++) {
            pIds.add(mConnPids.get(i));
        }

        System.out.println(pIds.toString());
    }

    private void exec() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        List<HTAPAQPThread> txn_threads = new ArrayList<HTAPAQPThread>();
        Random mRng = new Random(System.nanoTime());
        Long seedValue;
        int numWarehousesPerThread = (mNumThreads == 0) ? 0 : (mNumWarehouses / mNumThreads);
        int remainder = (mNumThreads == 0) ? 0 : (mNumWarehouses % mNumThreads);
        int minW = 1;
        for (int i = 0; i < mNumThreads + mNumSamplingThreads; i++) {
            if (i < mNumThreads) {
                seedValue = mRng.nextLong();
                int maxW = minW + (numWarehousesPerThread + ((i < remainder) ? 1 : 0)) - 1;
                System.out.println(minW + " " + maxW);
                txn_threads.add(new HTAPAQPThread(i, mDBConnList.get(i), arrOfTxn, arrOfTxnPercent, seedValue, minW, maxW, mNumWarehouses));
                minW = maxW + 1;
            } else {
                txn_threads.add(new HTAPAQPThread(i, mDBConnList.get(i), mSampleSize));
            }

        }

        long mStartTime;
        long mEndTime;

        mStartTime = System.currentTimeMillis();
        for (int i = 0; i < mNumThreads + mNumSamplingThreads; i++) {
            txn_threads.get(i).start();
        }

        String stopSignal = "";
        while (!stopSignal.equals("STOP")) {
            try {
                stopSignal = reader.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("stopping threads now");

        for (int i = 0; i < mNumThreads + mNumSamplingThreads; i++) {
            txn_threads.get(i).stopExec();
        }

        mEndTime = System.currentTimeMillis();

        System.out.printf("Runtime with %d threads is %s\n", mNumThreads, String.valueOf((mEndTime - mStartTime) / 1e3));
    }

    public static void main(String[] args) {
        if (args.length < 10) {
            System.out.println("usage: HTAPAQPDriver <numTPCCThreads> <numSamplingThreads> <hostName> <port> <dbName> <userName> <password> <transaction array> <% array> <sample_size>");
            System.exit(1);
        }

        int numThreads = Integer.parseInt(args[0]);
        int numSamplingThreads = Integer.parseInt(args[1]);
        String hostName = args[2];
        String port = args[3];
        String dbName = args[4];
        String userName = args[5];
        String password = args[6];
        String txns = args[7];
        String txnPercents = args[8];
        long sample_size = Long.parseLong(args[9]);

        HTAPAQPDriver driver = new HTAPAQPDriver(numThreads, numSamplingThreads, hostName, port, dbName, userName, password, txns, txnPercents, sample_size);
        driver.exec();
    }
}
