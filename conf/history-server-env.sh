#!/bin/bash

#################################################
# History Server app settings
#################################################

# Application host and port to service web pages and API
# It will be available at "HS_HOST:HS_PORT"
export HISTORY_SERVER_HOST="localhost"
export HISTORY_SERVER_PORT="8080"

# Spark event log directory where it stores app-XYZ/local-XYZ files
# Should be specified as full URI, can be HDFS, e.g. hdfs://host:port/sparkApplicaationLogs
# Make sure that directory exists on file system
export SPARK_EVENT_LOG_DIR="file:/tmp/spark-events"

# Mongo connection URL
export MONGO_CONNECTION="mongodb://localhost:27017"

# Path to the log4j configuration file, should be specified in format 'file:/path/to/file'
# If not specified, default file is used in ./conf directory
# export LOG4J_CONF_FILE=""
