package org.voltdb.utils;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created by kunkunur on 1/3/14.
 */
public class SelectSimpleBenchmark {
    private Client client;

    public SelectSimpleBenchmark(Config cArgs) {
    }

    public static void main(String[] args) throws Exception {
        // process args
        Config cArgs = Config.getConfig();
        cArgs.volt_user = "";
        cArgs.volt_password = "";
        cArgs.volt_servers = "localhost";
        int repititions = 100;
        String
                sql = "SELECT * from TABLE1 limit 100000";

        System.out.println("****************** Using Client Interface **********************");

        SelectSimpleBenchmark loader = new SelectSimpleBenchmark(cArgs);
        loader.client = VoltDBClientConnectionUtil.connectToVoltDB(cArgs);

        long t1, t2;
        for (int j = 0; j < repititions; j++) {
            t1 = System.currentTimeMillis();

            VoltTable[] results = loader.client.callProcedure("@AdHoc", sql).getResults();

            System.out.println(results[0].fetchRow(0).getLong(0));

            t2 = System.currentTimeMillis();

            System.out.println("Iteration  " + j + " took " + (t2 - t1) / 1000 + " seconds is the time taken to execute query and get first row");
        }

        VoltDBClientConnectionUtil.close(loader.client);

        System.out.println("****************************************************************");
        System.out.println("****************** Using JDBC Connection Interface **********************");
        Connection conn = VoltDBClientConnectionUtil.jdbcConnectToVoltDB(cArgs);

        ResultSet rs;

        for (int j = 0; j < repititions; j++) {
            t1 = System.currentTimeMillis();

            rs = conn.createStatement().executeQuery(sql);

            rs.next();
            System.out.println(rs.getLong(1));

            t2 = System.currentTimeMillis();

            System.out.println("Iteration  " + j + " took "  + (t2 - t1) / 1000 + " seconds is the time taken to execute query and get first row using jdbc api.");

            rs.close();

        }

        conn.close();
        System.out.println("****************************************************************");
        System.out.println("****************** Using JDBC Connection Pool Interface **********************");
        conn = VoltDBClientConnectionUtil.jdbcConnectPoolToVoltDB(cArgs);
        for (int j = 0; j < repititions; j++) {


            t1 = System.currentTimeMillis();

            rs = conn.createStatement().executeQuery(sql);

            rs.next();
            System.out.println(rs.getLong(1));

            t2 = System.currentTimeMillis();

            System.out.println("Iteration  " + j + " took "  + (t2 - t1) / 1000 + " seconds is the time taken to execute query and get first row using bonecp jdbc api.");

            rs.close();

        }
        conn.close();
        System.out.println("****************************************************************");
    }
}
