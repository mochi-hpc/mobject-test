#!/bin/sh

# This must be run as root.
echo "Checking cwd"
echo $PWD
export LD_LIBRARY_PATH=$PWD/install/lib:$PWD/install/lib64
echo "Checking LD_LIBRARY_PATH"
echo $LD_LIBRARY_PATH
echo "Creating /usr/local/tmp to check permission"
mkdir /usr/local/tmp
echo "Running sysctl"
/usr/sbin/sysctl kernel.yama.ptrace_scope=0
echo "Testing using make check"
make check
