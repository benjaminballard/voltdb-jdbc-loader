voltdb-jdbc-loader
==================

A command-line tool to load data into VoltDB from a JDBC source.

Usage:
------

1. copy JDBC driver .jar file to lib directory
2. edit loader() function in run.sh script to provide the following:
    * JDBC URL
    * JDBC driver class name
    * VoltDB server and port list
    * usernames and passwords
    * table name
3. Execute the following command:
 
    ./run.sh

Alternatively update pom.xml and use maven to package and execute the JdbcLoader

Changes done:
-------------

1. Extend JdbcLoader to load more than one table
2. An implementation of producer-consumer pattern to read-from-source and write-to-destination with one set of instances per table.
3. Extend JdbcLoader to read source-destination mappings from a properties file
  a) In the properties file destination is key and source is value.
  b) The properties file allows any sql select query to be specified for the source.
  c) The select query can be parmeterized with some variable names which would be substituted at run time with corresponding values. Currently the variable name to value is mapped within the same properties files
  d) The source can be prefixed with some string to act as a namespace, this is for readability and it also allows loading same table with rows from different source queries.
4. Though JdbcLoader uses the native Client interface for voltdb. A Connection utility has been provided to allow 3 modes
  a) Client
  b) JDBC and
  c) JDBC with connection pool using BoneCP
  d) A simple set of Benchmark tests are added for select and insert queries

Tested usage
-------------

1. Data movementfrom Netezza and PostgreSQL to VoltDB.

Enhancements to be done
-----------------------
1. Enable two way data movement
   a) Leveraging the same consumer/producer pattern.
2. enable invocation of a custom stored procedure per table (or one generic stored procedure for all tables) on the voltdb side instead of defaulting to INSERT stored procedure, to insert records.
3. Generate a much more readable report.
4. Allow restart if jdbcloader fails half-way through. Again 3 helps build a design where we could have a status table to track jdbcloader's progress.


