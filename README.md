voltdb-jdbc-loader
==================

A command-line tool to load data into VoltDB from a JDBC source.


Usage:

1. copy JDBC driver .jar file to lib directory
2. edit loader() function in run.sh script to provide the following:
    * JDBC URL
    * JDBC driver class name
    * VoltDB server and port list
    * usernames and passwords
    * table name
3. Execute the following command:
 
    ./run.sh

