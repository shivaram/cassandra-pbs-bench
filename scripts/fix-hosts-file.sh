#!/bin/bash
cat /etc/hosts | grep -v internal > tmp && mv tmp /etc/hosts
