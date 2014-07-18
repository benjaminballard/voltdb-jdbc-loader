package org.voltdb.utils;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by kunkunur on 1/3/14.
 */
public class VoltDBClientConnectionUtil {
   private static Logger logger = LoggerFactory.getLogger(SourceReader.class);
   
    public static Client connectToVoltDB(Config config) throws InterruptedException {
        logger.info("Connecting to VoltDB on: " + config.volt_servers);

        ClientConfig cc = new ClientConfig(config.volt_user, config.volt_password);
        cc.setProcedureCallTimeout(config.queryTimeOut * 1000);
        cc.setConnectionResponseTimeout(config.queryTimeOut * 1000);

        final Client client = ClientFactory.createClient(cc);

        String servers = config.volt_servers;
        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectVoltNode(client, server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();

        return client;
    }

    public static Connection jdbcConnectToVoltDB(Config config) throws InterruptedException, ClassNotFoundException, SQLException {
        logger.info("Connecting to VoltDB on: " + config.volt_servers);

        // We need only do this once, to "hot cache" the JDBC driver reference
        // so the JVM may realize it's there.
        Class.forName("org.voltdb.jdbc.Driver");

        // Prepare the JDBC URL for the VoltDB driver
        String url = "jdbc:voltdb://" + config.volt_servers;

        Connection conn = DriverManager.getConnection(url, config.volt_user, config.volt_password);

        return conn;
    }

    public static Connection jdbcConnectPoolToVoltDB(Config cfg) throws InterruptedException, ClassNotFoundException, SQLException {
        logger.info("Connecting to VoltDB on: " + cfg.volt_servers);

        // We need only do this once, to "hot cache" the JDBC driver reference
        // so the JVM may realize it's there.
        Class.forName("org.voltdb.jdbc.Driver");

        // Prepare the JDBC URL for the VoltDB driver
        String url = "jdbc:voltdb://" + cfg.volt_servers;

        BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(url);    // set the JDBC url
        config.setUsername(cfg.volt_user);            // set the username
        config.setPassword(cfg.volt_password);                // set the password
        config.setMinConnectionsPerPartition(1);
        config.setMaxConnectionsPerPartition(2);
        config.setPartitionCount(1);
        config.setIdleMaxAgeInMinutes(1);
        //config.setReleaseHelperThreads(null == dsConfig.getReleaseHelperThreads() ? defualtReleaseHelperThreads : Integer.parseInt(dsConfig.getReleaseHelperThreads()));
        config.setConnectionTestStatement("list tables ");
        config.setIdleConnectionTestPeriodInMinutes(1);
        config.setPoolName("VoltDB");

        BoneCPDataSource ds = new BoneCPDataSource(config);    // create a new configuration object


        return ds.getConnection();
    }

    public static void close(Client client) throws Exception {
        client.drain();
        client.close();
        logger.info("closed connections");
    }

    static void connectVoltNode(Client client, String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            } catch (Exception e) {
                logger.error("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000) sleep += sleep;
            }
        }
        logger.info("Connected to VoltDB node at: %s.\n", server);
    }
}
