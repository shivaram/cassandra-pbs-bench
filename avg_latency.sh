#!/bin/bash
# Awk script that calculates average time taken from the output of
# cassandra-stress. The script computes a weighted-average based on number of
# queries in the time-window and avg. query time in the time window.
#
# Example usage: cat pbs-R1W1_insert_0 | ./avg_latency
awk -F',' '{sum+=$3*$4; sumr+=$3} END {print sum/sumr*1000}'
