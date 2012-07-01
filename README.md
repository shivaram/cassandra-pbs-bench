cassandra-pbs-bench
===================

Benchmark for measuring PBS overhead in Cassandra

Requirements

  * Python
  * Install EC2 api tools in $PATH (http://aws.amazon.com/developertools/351/)
  * Export EC2_CERT, EC2_PRIVATE_KEY

Instructions to run the benchmark

1. Launch a EC2 cluster with PBS AMI.
   python setup_hosts.py --launch -n 4

2. Run benchmark using bench_pbs.py. Example:
   python bench_pbs.py --trunk -i 1 -o 10000 -r 1 -w 1


Authors: 

  Shivaram Venkataraman (shivaram@cs.berkeley.edu)

  Peter Bailis (pbailis@cs.berekely.edu)
