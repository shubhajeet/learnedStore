#/bin/bash

if [ $# -ne 2 ]; then
    # display error message
    echo "Usage: $0 src_cfg dst_dir"
else
    # copy needed file to the dst_dir
    # create director if necessary
    conf=$1
    dst_dir=$2
    mkdir -p $dst_dir
    echo "src_cfg: $conf; dst_dir: $dst_dir; begin to copy"
    source $conf
    # copy config file
    echo "[backup 1] $conf -> $dst_dir/$conf"
    cp $conf $dst_dir/$conf

    echo "[backup 2] $SSD_FILE -> $dst_dir/$SSD_FILE"
    cp $SSD_FILE $dst_dir/$SSD_FILE

    echo "[backup 3] $META_FILE -> $dst_dir/$META_FILE"
    cp $META_FILE $dst_dir/$META_FILE

    echo "[backup 4] $TRACE_FILE -> $dst_dir/$TRACE_FILE"
    cp $TRACE_FILE $dst_dir/$TRACE_FILE

    stats_location=${STATS_LOC#*/}
    stats_location=${stats_location%/*}
    mkdir -p $dst_dir/$stats_location
    echo "[backup 5] $STATS_LOC* -> $dst_dir/$stats_location"
    cp $STATS_LOC* $dst_dir/$stats_location

    echo "[backup 6] $ATTACH_SEG_FILE -> $dst_dir/$ATTACH_SEG_FILE"
    cp $ATTACH_SEG_FILE $dst_dir/$ATTACH_SEG_FILE

    echo "[backup 7] $SPLINE_FILE -> $dst_dir/$SPLINE_FILE"
    cp $SPLINE_FILE $dst_dir/$SPLINE_FILE

    echo "[backup 8] $MAPPING_FILE* -> $dst_dir/"
    cp $MAPPING_FILE* $dst_dir/

    echo "end of copy"
fi