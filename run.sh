#!/usr/bin/env bash

. ./env.sh

# remove build artifacts
function clean() {
    rm -rf obj log
}

# compile the source code for procedures and the client
function compile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj -Xlint:deprecation \
        src/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

function loader() {
    compile
    java -Xmx512m -Dfile.encoding=UTF-8 -classpath obj:$CLASSPATH \
        -Dlog4j.configuration=file://$LOG4J \
        jdbcloader.JdbcLoader \
        --jdbcurl="jdbc:mysql://localhost/test?useLegacyDatetimeCode=false&serverTimezone=America/New_York&characterEncoding=UTF-8&useUnicode=true" \
        --jdbcdriver="com.mysql.jdbc.Driver" \
        --jdbcuser="" \
        --jdbcpassword="" \
        --volt_servers="localhost" \
        --fetchsize=100 \
        --tablename="mytable"
}

function help() {
    echo "Usage: ./run.sh {help|clean|loader|compile}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else loader; fi
