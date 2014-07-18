package org.voltdb.utils;
/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.CLIConfig;

/**
 * Uses CLIConfig class to declaratively state command line options
 * with defaults and validation.
 */
public class Config extends CLIConfig {

    public static final String ALL = "ALL";
    public static String DOT_SEPARATOR = "." ;
    public static String COMMA_SEPARATOR = ",";
    public static String MODULE_SUFFIX= "\\.|";
    public static String TABLE_SUFFIX= " *|";

    @Option(desc = "The JDBC connection string for the source database")
    public String jdbcurl = "";

    @Option(desc = "The class name of the JDBC driver. Place the driver JAR files in the lib directory.")
    public String jdbcdriver = "";

    @Option(desc = "Source database username")
    public String jdbcuser = "";

    @Option(desc = "Source database password")
    public String jdbcpassword = "";

    @Option(desc = "Comma separated list in the form of server[:port] for the target VoltDB cluster.")
    public String volt_servers = "localhost";

    @Option(desc = "VoltDB user name")
    public String volt_user = "";

    @Option(desc = "VoltDB password")
    public String volt_password = "";

    @Option(desc = "Comma separate names of modules to be loaded.Special default value ALL represents all modules.")
    public String modules = ALL;

    @Option(desc = "Comma separate names of tables to be loaded.Special default value ALL represents all modules.")
    public String tables = ALL;

    @Option(desc = "Name of procedure for VoltDB insert (optional)")
    public String procname = "";

    @Option(desc = "JDBC fetch size. Give a reasonable number.Default is 1000.")
    public int fetchsize = 1000;

    @Option(desc = "Maximum queue size for the producer thread, the thread that reads data from Netezza, to stop pulling data temporarily allowing consumer thread to catchup. Reasonably higher number.Default is 20.")
    public int maxQueueSize = 20;

    @Option(desc = "Path to the properties files which specifies queries to be executed against the source database against the VoltDB procedure names")
    public String queriesFile = "";

    @Option(desc = "Maximum time consumer can wait without any new data in seconds")
    public long maxWaitTime = 10;

    @Option(desc = "Maximum number of errors per table before loading should be stopped")
    public long maxErrors = 100;

    @Option(desc = "Maxiumum time, in seconds, the client would wait for server to respond to a query")
    public long queryTimeOut = 100;

    public Config() {
    }

    public static Config getConfig() {
        Config config = new Config();
        return config;
    }

    public static Config getConfig(String classname, String[] args) {
        Config config = new Config();
        config.parse(classname, args);
        return config;
    }

    @Override
    public void validate() {
        // add validation rules here
        if (jdbcurl.equals("")) exitWithMessageAndUsage("jdbcurl cannot be blank");
        if (jdbcdriver.equals("")) exitWithMessageAndUsage("jdbcdriver cannot be blank");
        if (volt_servers.equals("")) exitWithMessageAndUsage("volt_servers cannot be blank");
        if (tables.equals("") && queriesFile.equals(""))
            exitWithMessageAndUsage("either tables or queriesFile should be specified");
    }
}
