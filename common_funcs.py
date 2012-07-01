# Common helper functions

import subprocess
from os import system
from time import sleep

cassandra_root_dir = "/home/ubuntu/cassandra/"

def run_cmd(hosts, cmd, user="root"):
    print("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (user, hosts, cmd))
    system("pssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (user, hosts, cmd))

def run_cmd_single(host, cmd, user="root"):
    system("ssh %s@%s \"%s\"" % (user, host, cmd))

def run_process_single(host, cmd, user="root", stdout=None, stderr=None):
    subprocess.call("ssh %s@%s \"%s\"" % (user, host, cmd),
            stdout=stdout, stderr=stderr, shell=True)

def run_script(hosts, script, user="root"):
    system("cp %s /tmp" % (script))
    script = script.split("/")
    script = script[len(script)-1]
    system("pscp -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt /tmp/%s /tmp" % (user, hosts, script))
    run_cmd(hosts, "bash /tmp/%s" % (script), user)

def fetch_file_single(host, remote, local, user="root"):
    system("scp %s@%s:%s %s" % (user, host, remote, local))

def get_host_ips(hosts):
    return open("hosts/%s.txt" % (hosts)).read().split('\n')[:-1]

#### CASSANDRA STUFF

def change_cassandra_seeds(hosts, seed):
    run_cmd(hosts, "sed -i 's/          - seeds: \\\"127.0.0.1\\\"/          - seeds: \\\"%s\\\"/' %s/conf/cassandra.yaml" % (seed, cassandra_root_dir))

def get_node_ips():
    ret = []
    system("ec2-describe-instances > /tmp/instances.txt")
    system("ec2-describe-instances --region us-west-2 >> /tmp/instances.txt")
    for line in open("/tmp/instances.txt"):
        line = line.split()
        if line[0] != "INSTANCE" or line[5] != "running":
            continue
        # addr, externalip, internalip, ami
        ret.append((line[3], line[13], line[14], line[1]))
    return ret

def get_cassandra_hosts(hosts):
    ret = []
    cips = get_host_ips(hosts)
    #argh should use a comprehension/filter; i'm tired
    for h in get_node_ips():
        if h[0] in cips:
            ret.append(h)

    return ret

def get_matching_ip(host, hosts):
    cips = get_host_ips(hosts)
    #argh should use a comprehension/filter; i'm tired
    for h in get_node_ips():
        if h[0] == host:
            return h[1]

def change_cassandra_listen_address(hosts):
    for host_tuple in get_cassandra_hosts(hosts):
        run_cmd_single(host_tuple[0], "sed -i 's/listen_address: localhost/listen_address: %s/' %s/conf/cassandra.yaml" % (host_tuple[2], cassandra_root_dir))
        run_cmd_single(host_tuple[0], "sed -i 's/rpc_address: localhost/rpc_address: %s/' %s/conf/cassandra.yaml" % (host_tuple[0], cassandra_root_dir))
        run_cmd_single(host_tuple[0], "echo -e \\\"\\nbroadcast_address: %s\\n\\\" >> %s/conf/cassandra.yaml" % (host_tuple[1], cassandra_root_dir))

def launch_cassandra_leader(host):
    run_cmd_single(host, "%s/bin/cassandra" % (cassandra_root_dir))

def launch_cassandra_rest(hosts_array):
    for h in hosts_array:
        run_cmd_single(h, "%s/bin/cassandra" % (cassandra_root_dir))
        sleep(30)

def change_cassandra_mem(hosts):
    #run_cmd(hosts, "sed -i 's/rpc_timeout_in_ms: 10000/rpc_timeout_in_ms: 1000000/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/#MAX_HEAP_SIZE/MAX_HEAP_SIZE/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/#HEAP_NEWSIZE/HEAP_NEWSIZE/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/4G/7G/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/800M/500M/' %s/conf/cassandra-env.sh" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/hinted_handoff_enabled: true/hinted_handoff_enabled: false/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/concurrent_reads: 32/concurrent_reads: 512/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/concurrent_writes: 32/concurrent_writes: 512/' %s/conf/cassandra.yaml" % (cassandra_root_dir))

def change_cassandra_logger(hosts):
    run_cmd(hosts, "sed -i 's/log4j.appender.R.File=\/var\/log\/cassandra\/system.log/log4j.appender.R.File=\/mnt\/md0\/cassandra\/system.log/' %s/conf/log4j-server.properties" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/    - \/var\/lib\/cassandra\/data/    - \/mnt\/md0\/cassandra\/data/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    run_cmd(hosts, "sed -i 's/\/var\/lib\/cassandra\/commitlog/\/mnt\/md0\/cassandra\/commitlog/' %s/conf/cassandra.yaml" % (cassandra_root_dir))
    return

def kill_cassandra(hosts):
    print "Killing Cassandra..."
    run_cmd(hosts, "killall java")
    print "Done (okay if RED above)"

def clean_cassandra(hosts):
    print "Cleaning Cassandra..."
    run_cmd(hosts, "rm -rf /mnt/md0/cassandra")
    print "Done"

def check_cassandra_ring(host, desiredcnt):
    run_cmd_single(host, "%s/bin/nodetool -h localhost ring > /tmp/ring.out" % (cassandra_root_dir))
    fetch_file_single(host, "/tmp/ring.out", "/tmp")
    hostcnt = open("/tmp/ring.out").read().count("Up")
    if hostcnt != desiredcnt:
        print "Got %d expected %d!" % (hostcnt, desiredcnt)
        return False
    else:
        print "Saw all %d nodes" % (desiredcnt)
        return True

def set_up_cassandra_ring(hosts):
    run_cmd(hosts, "rm -rf /var/lib/cassandra/*")
    print "Getting host ips..."
    chosts = get_host_ips(hosts)
    print "Done"

    leader = chosts[0]

    leaderPublicIP = get_matching_ip(leader, hosts)

    #change seed
    print "Changing Cassandra seeds..."
    change_cassandra_seeds(hosts, leaderPublicIP)
    print "Done"

    print "Changing listen addresses seeds..."
    change_cassandra_listen_address(hosts)
    print "Done"

    print "Changing logger..."
    change_cassandra_logger(hosts)
    print "Done"

    print "Changing initial tokens..."
    for i in xrange(len(chosts)):
        token = 2 ** 127 / len(chosts) * i
        curnode = chosts[i]
        run_cmd_single(curnode, "sed -i 's/initial_token:/initial_token: %d/' %s/conf/cassandra.yaml" % (token, cassandra_root_dir))
    print "Done"

def launch_cassandra_ring(hosts):

    chosts = get_host_ips(hosts)
    print "Done"

    leader = chosts[0]

    print "Launching Cassandra leader..."
    launch_cassandra_leader(leader)
    sleep(5)
    print "Done"

    print "Launching other Cassandra nodes..."
    launch_cassandra_rest(chosts[1:])
    print "Done"

    if(not check_cassandra_ring(leader, len(chosts))):
        print "EXITING"
        exit(-1)

def checkout_branch(branch):
    run_cmd("all-hosts",
            "cd cassandra && git checkout -- . && git checkout %s && "\
            "git checkout -- . && ant" % (branch), user="ubuntu")

def enable_pbs_jmx():
    run_script("all-hosts", "scripts/enable_pbs_logging.sh",
                user="ubuntu")

def disable_pbs_jmx():
    run_script("all-hosts", "scripts/disable_pbs_logging.sh",
                user="ubuntu")

def set_pbs_jmx(pbs):
    if pbs:
        enable_pbs_jmx()
    else:
        disable_pbs_jmx()
