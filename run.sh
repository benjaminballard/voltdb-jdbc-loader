#!/usr/bin/env bash

. ./env.sh

# remove build artifacts
function clean() {
    mvn clean
    rm -rf log
}

# assemble the source code for procedures and the client
function assemble() {
    mvn assembly:assembly # override db.groupid, db.artifactid and db.version in pom.xml before proceeding
}

function loader() {
    if [ ! -d ./target ]; then assemble; fi

    java -Xmx512m -Dfile.encoding=UTF-8 -classpath target/*:$CLASSPATH \
        -Dlog4j.configuration=file://$LOG4J \
        org.voltdb.utils.JdbcLoader \
        --jdbcurl="jdbc:mysql://localhost/test?useLegacyDatetimeCode=false&serverTimezone=America/New_York&characterEncoding=UTF-8&useUnicode=true" \
        --jdbcdriver="com.mysql.jdbc.Driver" \
        --jdbcuser="" \
        --jdbcpassword="" \
        --volt_servers="localhost" \
        --fetchsize=100 \
        --tablename="mytable"
        --srisvoltdb=false
        --dwisvoltdb=true
}

function help() {
    echo "Usage: ./run.sh {help|clean|loader|assemble}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else loader; fi
