package jdbcloader;
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
public class LoaderConfig extends CLIConfig {

    @Option(desc = "The JDBC connection string for the source database")
    String jdbcurl = "";
    //String example = "jdbc:mysql://localhost/test?useLegacyDatetimeCode=false&serverTimezone=America/New_York&characterEncoding=UTF-8&useUnicode=true";

    @Option(desc = "The class name of the JDBC driver. Place the driver JAR files in the lib directory.")
    String jdbcdriver = "";

    @Option(desc = "Source database username")
    String jdbcuser = "";

    @Option(desc = "Source database password")
    String jdbcpassword = "";

    @Option(desc = "Comma separated list in the form of server[:port] for the target VoltDB cluster.")
    String volt_servers = "localhost";

    @Option(desc = "VoltDB user name")
    public String volt_user = "";

    @Option(desc = "VoltDB password")
    public String volt_password = "";

    @Option(desc = "Name of table to be loaded")
    public String tablename = "";

    @Option(desc = "Name of procedure for VoltDB insert (optional)")
    public String procname = "";

    @Option(desc = "JDBC fetch size")
    public int fetchsize = 0;

    public LoaderConfig() {
    }

    public static LoaderConfig getConfig(String classname, String[] args) {
        LoaderConfig config = new LoaderConfig();
        config.parse(classname, args);
        return config;
    }
    
    @Override
    public void validate() {
        // add validation rules here
        if (jdbcurl.equals("")) exitWithMessageAndUsage("jdbcurl cannot be blank");
        if (jdbcdriver.equals("")) exitWithMessageAndUsage("jdbcdriver cannot be blank");
        if (volt_servers.equals("")) exitWithMessageAndUsage("volt_servers cannot be blank");
        if (tablename.equals("")) exitWithMessageAndUsage("tablename cannot be blank");
    }
}
