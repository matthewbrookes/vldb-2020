#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

### paths configuration ###
CARGO="cargo run --release --"

### queries configuration ###
WINDOW_1_COUNT="window_1_faster_count"
WINDOW_1_RANK="window_1_faster_rank"
WINDOW_2_COUNT="window_2_faster_count"
WINDOW_2_RANK="window_2_faster_rank"
WINDOW_3_COUNT="window_3_faster_count"
WINDOW_3_RANK="window_3_faster_rank"

### source rate
if [ "$1" == "" ]; then
    echo "Please provide the source rate in records/s"
    exit 1
else
    echo "Source rate is $1"
fi
RATE=$1


### duration
if [ "$2" == "" ]; then
    echo "Please provide the duration in s"
    exit 1
else
    echo "Duration is $2"
fi
DURATION=$2

### workers
if [ "$3" == "" ]; then
    echo "Please provide the number of workers"
    exit 1
else
    echo "Number of workers is $3"
fi
WORKERS=$3


### ### ###  	 ### ### ###

### ### ### MAIN ### ### ###

### ### ###  	 ### ### ###
for QUERY in $WINDOW_1_COUNT $WINDOW_1_RANK $WINDOW_2_COUNT $WINDOW_2_RANK $WINDOW_3_COUNT $WINDOW_3_RANK
do
	for WINDOW_SIZE in 1 5 10 100 1000
	do
		for WINDOW_SLICE_COUNT in 1 5 10 20 50 100
		do
			WINDOW_SLIDE=$((WINDOW_SIZE/WINDOW_SLICE_COUNT))
			if [ $WINDOW_SLIDE -gt 0 ]
			then
				echo "Running experiment with window slide $WINDOW_SLIDE and count $WINDOW_SLICE_COUNT"
				$CARGO --duration $DURATION --rate $RATE --window-slice-count $WINDOW_SLICE_COUNT --window-slide $WINDOW_SLIDE --queries $QUERY -- -w $WORKERS > $QUERY$"-"$WINDOW_SIZE$"-"$WINDOW_SLIDE$".out"
			fi
		done
	done
done
