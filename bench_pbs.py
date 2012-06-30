# Script to benchmark PBS with different settings

import argparse
from common_funcs import checkout_cassandra_pbs
from common_funcs import checkout_cassandra_trunk
from common_funcs import kill_cassandra
from common_funcs import clean_cassandra
from common_funcs import set_up_cassandra_ring
from common_funcs import launch_cassandra_ring
from common_funcs import set_pbs_jmx
from common_funcs import get_host_ips
from common_funcs import run_process_single

def RunBenchmark(iters, ops, r, w, out_prefix, pbs=True, branch='pbs'):
    # Checkout the right branch
    # TODO: Make this neater by passing a branch name
    if branch == 'pbs':
        checkout_cassandra_pbs()
    elif branch == 'trunk':
        checkout_cassandra_trunk()
    else:
        print "Invalid branch ", branch
        return

    for i in xrange(0, iters):
        print "Restarting cassandra cluster"
        kill_cassandra("all-hosts")
        clean_cassandra("all-hosts")
        set_up_cassandra_ring("all-hosts")
        launch_cassandra_ring("all-hosts")

        # Set the right JMX value
        set_pbs_jmx(pbs)

        # Get the leader and run stress from there
        chosts = get_host_ips("all-hosts")
        leader = chosts[0]
        print "Using Cassandra leader ", leader

        out_insert = "./%s_insert_%d" % (out_prefix, i)
        out_read = "./%s_read_%d" % (out_prefix, i)

        f_out_insert = open(out_insert, "w")
        f_out_read = open(out_read, "w")
        # Run insert test
        run_process_single(leader, "cd cassandra; ./tools/bin/cassandra-stress"\
                           " -d %s -l %d -n %d -o insert" % (leader, w, ops),
                           user="ubuntu", stdout=f_out_insert, stderr=f_out_insert)

        # Run read test
        run_process_single(leader, "cd cassandra; ./tools/bin/cassandra-stress"\
                           " -d %s -l %d -n %d -o read" % (leader, r, ops),
                           user="ubuntu", stdout=f_out_read, stderr=f_out_read)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark PBS Cassandra')
    parser.add_argument('--iterations', '-i', dest='iters', default=5, type=int,
                        help='Number of stress iterations')
    parser.add_argument('--operations', '-o', dest='ops', default=1000000,
                        type=int, 
                        help='Number of operations per stress invocation')
    parser.add_argument('--read-replicas', '-r', dest='r', default=1, type=int,
                        help='Number of read replicas (R)')
    parser.add_argument('--write-replicas', '-w', dest='w', default=1, type=int,
                        help='Number of read replicas (W)')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--pbs-on', action='store_true',
                        help='Run using PBS branch with logging on')
    group.add_argument('--pbs-off', action='store_true',
                        help='Run using PBS branch with logging off')
    group.add_argument('--trunk', action='store_true', 
                        help='Run using cassandra-trunk')

    args = parser.parse_args()
    print "Starting benchmark with iterations:", args.iters,\
          "ops/iteration:", args.ops, "R:", args.r, "W:", args.w

    if args.pbs_on:
        print "Running with PBS ON"
        RunBenchmark(pbs=True, branch='pbs', iters=args.iters, ops=args.ops,
                r=args.r, w=args.w, out_prefix="pbs-R%dW%d" % (args.r, args.w))
    elif args.pbs_off:
        print "Running with PBS OFF"
        RunBenchmark(pbs=False, branch='pbs', iters=args.iters, ops=args.ops,
                r=args.r, w=args.w, 
                out_prefix="no-pbs-R%dW%d" % (args.r, args.w))
    else:
        print "Running with Cassandra trunk"
        RunBenchmark(pbs=False, branch='trunk', iters=args.iters, ops=args.ops,
                r=args.r, w=args.w,
                out_prefix="trunk-R%dW%d" % (args.r, args.w))
