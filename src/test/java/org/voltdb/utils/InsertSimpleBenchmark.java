package org.voltdb.utils;

import org.voltdb.client.Client;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by kunkunur on 1/3/14.
 */
public class InsertSimpleBenchmark {
    private Client client;

    public InsertSimpleBenchmark(Config cArgs) {
    }

    public static void main(String[] args) throws Exception {
        // process args
        Config cArgs = Config.getConfig();
        cArgs.volt_user = "";
        cArgs.volt_password = "";
        cArgs.volt_servers = "localhost";

        System.out.println("****************** Using Client Interface **********************");

        InsertSimpleBenchmark loader = new InsertSimpleBenchmark(cArgs);
        loader.client = VoltDBClientConnectionUtil.connectToVoltDB(cArgs);

        long t1 = System.nanoTime();

        for (int i=0;i<1000;i++) {
            loader.client.callProcedure(null, "TABLE1.insert", new Object[]{"adasdas",i,1,i,i });
        }

        long t2 = System.nanoTime();

        long time1= (t2 - t1) / 1000  ;

        System.out.println(time1 + " micro seconds is the time taken to execute 1000 inserts");
        VoltDBClientConnectionUtil.close(loader.client);


        System.out.println("****************************************************************");
        System.out.println("****************** Using JDBC Connection Interface **********************");
        Connection conn = VoltDBClientConnectionUtil.jdbcConnectToVoltDB(cArgs);

        t1 = System.nanoTime();

        PreparedStatement prepStmnt1 = conn.prepareCall("{call TABLE1.insert(?,?,?,?,?)}");

        for (int i=0;i<1000;i++) {
            prepStmnt1.setString(1, "sdasd");
            prepStmnt1.setLong(2, i);
            prepStmnt1.setLong(3, 1);
            prepStmnt1.setLong(4, i);
            prepStmnt1.setLong(5, i);
            prepStmnt1.executeUpdate();
        }

         t2 = System.nanoTime();

        long time2= (t2 - t1) / 1000  ;
        System.out.println(time2 + " micro seconds is the time taken to execute 1000 inserts.");

        conn.close();
        System.out.println("****************************************************************");
        System.out.println("****************** Using JDBC Connection Pool Interface **********************");
        conn = VoltDBClientConnectionUtil.jdbcConnectPoolToVoltDB(cArgs);

        t1 = System.nanoTime();

             prepStmnt1 = conn.prepareCall("{call TABLE1.insert(?,?,?,?,?)}");

        for (int i=0;i<1000;i++) {
            prepStmnt1.setString(1, "asds");
            prepStmnt1.setLong(2, i);
            prepStmnt1.setLong(3, 1);
            prepStmnt1.setLong(4, i);
            prepStmnt1.setLong(5, i);
            prepStmnt1.executeUpdate();
        }

        t2 = System.nanoTime();

        long time3= (t2 - t1) / 1000  ;

        System.out.println(time3 + " micro seconds is the time taken to execute 1000 inserts.");

        conn.close();
        System.out.println("****************************************************************");

        System.out.println("JDBC / Client : " + (time2/time1));
        System.out.println("JDBC with Connection pool / Client : " + (time3/time1));
    }
}
