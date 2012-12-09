#!/bin/sh
rm -rf commitlog/ data/
CASSANDRA_INCLUDE=#NODE_FOLDER#/cassandra.in.sh
export CASSANDRA_INCLUDE
cd #CASSANDRA_HOME#/
bin/cassandra -f