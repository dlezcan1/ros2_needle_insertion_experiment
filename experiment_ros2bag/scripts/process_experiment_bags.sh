#!/bin/bash

ROS_WS=$HOME/dev/needleinsertion_ws

source $ROS_WS/install/setup.bash

IN_DIR=$1
OUT_DIR=$2
shift 2

echo "Processing experimental bag files from $IN_DIR"
for idir in $IN_DIR/Insertion*; do
    insertion="$(basename $idir)"
    odir="$OUT_DIR/$insertion"

    echo "Processing insertion: $insertion"

    failure=0
    ros2 run experiment_ros2bag process_bag \
        $idir \
        --odir $odir \
        $@ || failure=1
    
    if [[ $failure = 1 ]]; then
        break
    fi

    echo "Finished with insertion: $insertion"
    echo
done

