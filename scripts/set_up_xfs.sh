#!/bin/bash
apt-get -q -y install xfsprogs
umount /mnt
mkfs.xfs -f /dev/xvdb
mkdir -p /mnt/md0 && mount -t xfs -o noatime /dev/xvdb /mnt/md0
