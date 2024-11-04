#!/bin/bash
# read config from the config file and then creates or removes the necessary file
# code for maintainance of the database

remove_leanstore_db() {
    if [ "$1" ]; then
        conf=$1
        source $conf
    fi
    echo "Removing: $SSD_FILE $META_FILE"
    sudo rm -rf $SSD_FILE $META_FILE
}

create_leanstore_db() {
    if [ "$1" ]; then
        conf=$1
        source $conf
    fi
    echo "Creating: $SSD_FILE $META_FILE"
    touch $SSD_FILE
    touch $META_FILE
}

remove_trace() {
    if [ "$1" ]; then
        conf=$1
        source $conf
    fi
    echo "Removing: $TRACE_FILE"
    touch $TRACE_FILE
}
