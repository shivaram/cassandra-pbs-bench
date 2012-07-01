# Script to benchmark PBS with different settings

import argparse
from common_funcs import checkout_branch
from common_funcs import kill_cassandra
from common_funcs import clean_cassandra
from common_funcs import set_up_cassandra_ring
from common_funcs import launch_cassandra_ring
from common_funcs import set_pbs_jmx
from common_funcs import get_host_ips
from common_funcs import run_process_single


# Map R,W values to a cassandra consistency level
def GetConsistencyLevel(value):
    if value == 1:
        return "ONE"
    elif value == 2:
        return "TWO"
    elif value == 3:
        return "THREE"
    else:
        return "QUORUM"


def RunBenchmark(iters, ops, r, w, out_prefix, pbs, branch, replicas=3,
        threads=50):
    for i in xrange(0, iters):
        # Checkout the right branch
        checkout_branch(branch)

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
                           " -d %s -l %d -e %s -n %d -t %d -o insert" % (leader,
                               replicas, GetConsistencyLevel(w), ops, threads),
                           user="ubuntu", stdout=f_out_insert,
                           stderr=f_out_insert)

        # Run read test
        run_process_single(leader, "cd cassandra; ./tools/bin/cassandra-stress"\
                           " -d %s -l %d -e %s -n %d -t %d -o read" % (leader,
                               replicas, GetConsistencyLevel(r), ops, threads),
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
    parser.add_argument('--replication-factor', '-f', dest='replicas', 
                        default=3, type=int, 
                        help='Replication factor for the keyspace (default 3)')
    parser.add_argument('--threads', '-t', dest='threads', 
                        default=50, type=int, 
                        help='Number of threads to use in stress')
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
        RunBenchmark(pbs=True, branch='for-cassandra', iters=args.iters,
                ops=args.ops, r=args.r, w=args.w,
                out_prefix="pbs-R%dW%d" % (args.r, args.w),
                replicas=args.replicas, threads=args.threads)
    elif args.pbs_off:
        print "Running with PBS OFF"
        RunBenchmark(pbs=False, branch='for-cassandra', iters=args.iters,
                ops=args.ops, r=args.r, w=args.w,
                out_prefix="no-pbs-R%dW%d" % (args.r, args.w),
                replicas=args.replicas, threads=args.threads)
    else:
        print "Running with Cassandra trunk"
        RunBenchmark(pbs=False, branch='cassandra-trunk', iters=args.iters, 
                ops=args.ops, r=args.r, w=args.w,
                out_prefix="trunk-R%dW%d" % (args.r, args.w),
                replicas=args.replicas, threads=args.threads)
