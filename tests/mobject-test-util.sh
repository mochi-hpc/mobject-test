#!/bin/bash
#
# General test script utilities
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

if [ -z "$TIMEOUT" ] ; then
    echo expected TIMEOUT variable defined to its respective command
    exit 1
fi

function run_to()
{
    maxtime=${1}s
    shift
    $TIMEOUT --signal=9 $maxtime "$@"
}

function mobject_test_start_servers()
{
    nservers=${1:-4}
    startwait=${2:-15}
    maxtime=${3:-120}
    storage=${5:-/dev/shm/mobject.dat}

    rm -rf ${storage}
    bake-mkpool -s 50M /dev/shm/mobject.dat

    run_to $maxtime mpirun -np $nservers bedrock na+sm -c $SCRIPT_DIR/config.json -v trace &
    if [ $? -ne 0 ]; then
        # TODO: this doesn't actually work; can't check return code of
        # something executing in background.  We have to rely on the
        # return codes of the actual client side tests to tell if
        # everything started properly
        exit 1
    fi

    # wait for servers to start
    sleep ${startwait}
}
