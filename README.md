cassandra-pbs-bench
===================

Benchmark for measuring PBS overhead in Cassandra

Requirements

  * Python
  * EC2 api tools (http://aws.amazon.com/developertools/351/)


Instructions to run the benchmark

1. Launch a EC2 cluster with PBS AMI.
   python set-up-hosts.py --launch -n 4

2. Run cassandra-stress benchmark using test_perf.sh


Authors: 

  Shivaram Venkataraman (shivaram@cs.berkeley.edu)

  Peter Bailis (pbailis@cs.berekely.edu)
