# Script to setup EC2 cluster for cassandra using PBS AMI in
# AWS east

import argparse
from common_funcs import *
from time import sleep
from os import system

eastAMI = "ami-8ee848e7"
eastInstanceIPs = []


def make_ec2_east(n):
    if n == 0:
        return
    global eastAMI
    f = raw_input("EAST: spinning up %d instances; okay? " % n)

    if f != "Y" and f != "y":
        exit(-1)

    system("ec2-run-instances %s -n %d -g 'cassandra' --t m1.large -k " \
           "'lenovo-pub' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" %
           (eastAMI, n))


def get_instances():
    global eastInstanceIPs

    system("rm instances.txt")
    system("ec2-describe-instances --region us-east-1 >> instances.txt")

    ret = []

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "INSTANCE":
            ip = line[3]
            if ip == "terminated":
                continue
            status = line[5]
            if status.find("shutting") != -1:
                continue
            region = line[10]
            ret.append((ip, region))

    system("rm instances.txt")

    return ret


def make_instancefile(name, hosts):
    f = open("hosts/" + name, 'w')
    for host in hosts:
        f.write("%s\n" % (host))
    f.close


def start_cluster(num_hosts):
    print "Starting EC2 east hosts...",
    make_ec2_east(num_hosts)
    print "Done"

    system("mkdir -p hosts")

    hosts = get_instances()

    make_instancefile("all-hosts.txt", [h[0] for h in hosts])
    # Set first host as leader
    make_instancefile("cassandra-leader.txt", [hosts[0][0]])

    print "Done"

    sleep(30)
    print "Awake!"


def setup_cluster():
    print "Enabling root SSH...",
    run_script("all-hosts", "scripts/enable_root_ssh.sh", "ubuntu")
    print "Done"

    print "Setting up XFS...",
    run_script("all-hosts", "scripts/set_up_xfs.sh")
    print "Done"

    print "Fixing host file bugs...",
    run_script("all-hosts", "scripts/fix-hosts-file.sh")
    print "Done"

    print "Installing NTP (Ignore failures)..."
    run_cmd("all-hosts", "sudo apt-get -q -y install ntp")
    run_cmd("all-hosts", "sudo ntpd -q")
    print "Done"

    print "Installing Jmxterm..."
    run_cmd("all-hosts", "wget http://downloads.sourceforge.net/"\
            "cyclops-group/jmxterm-1.0-alpha-4-uber.jar",
            user="ubuntu")
    print "Done"


def clone_cassandra_pbs(): 
    run_cmd("all-hosts", "rm -rf cassandra", user="ubuntu")
    run_cmd("all-hosts", 
            "git clone https://github.com/pbailis/cassandra-pbs cassandra",
            user="ubuntu")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cassandra on EC2')
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--restart', '-r', action='store_true',
                        help='Restart cassandra cluster')
    parser.add_argument('--machines', '-n', dest='machines', nargs='?', 
                        default=4, 
                        help='Number of machines in cluster, default=4')
    args = parser.parse_args()

    if args.launch:
        print "Launching cassandra cluster"
        start_cluster(args.machines)
        setup_cluster()
        clone_cassandra_pbs()
        checkout_cassandra_pbs()

    if args.restart:
        print "Restarting cassandra cluster"
        kill_cassandra("all-hosts")
        clean_cassandra("all-hosts")
        set_up_cassandra_ring("all-hosts")
        launch_cassandra_ring("all-hosts")

    if not args.launch and not args.restart:
        parser.print_help()
