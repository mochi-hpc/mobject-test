#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi
source $srcdir/tests/mobject-test-util.sh

TEST_DIR=`$MKTEMP -d /tmp/mobject-connect-test-XXXXXX`
MOBJECT_CLUSTER_FILE=$TEST_DIR/cluster.gid

##############

# start 1 server with 5 second wait, 20s timeout
mobject_test_start_servers 1 5 20 $MOBJECT_CLUSTER_FILE

##############

# export some mobject client env variables
export MOBJECT_CLUSTER_FILE
export MOBJECT_SHUTDOWN_KILL_SERVERS=true

# run a mobject test client
run_to 10 tests/mobject-client-test
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

##############

wait

# cleanup
rm -rf $TEST_DIR

exit 0
