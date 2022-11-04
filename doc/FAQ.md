# Frequently Asked Questions

* How can HDF5 RADOS VOL plugin work with Mobject?

  See [h5VLrados.c](https://github.com/HDFGroup/vol-rados/blob/master/src/H5VLrados.c) to understand how HDF5 RADOS VOL plugin calls APIs.

* How can I install Mobject on Polaris?

  You can execute the following script.
```
#!/bin/bash                                                                     

# Git clone 3 repos under your home directory.                                  
git clone https://github.com/spack/spack
git clone https://github.com/mochi-hpc/mochi-spack-packages
git clone https://github.com/mochi-hpc-experiments/platform-configurations


# Set up Spack environment on Polaris.                                          
. ~/spack/share/spack/setup-env.sh
spack repo add ~/mochi-spack-packages
spack env list
spack env create mochi-env ~/platform-configurations/ANL/Polaris/spack.yaml
spack env activate mochi-env

# Install mobject on Polaris.                                                   
spack install mobject%gcc@11.2.0+bedrock
```

* How can I test Mobject on Polaris from source code?

Install the required packages in [spack.yaml](../spack.yaml) and load them.

You must use 'dev-replace-sdskv-with-yokan' branch.
```
git clone https://github.com/mochi-hpc/mobject
cd mobject
git checkout dev-replace-sdskv-with-yokan
```

Load system modules.
```
module load cudatoolkit-standalone
module load PrgEnv-gnu
module load libfabric
```

Run the following commands.
```
./prepare.sh
./configure --enable-bedrock
make
make check
```

You should see 3 `PASS` and version `0.7`.

```
PASS: tests/mobject-connect-test.sh
PASS: tests/mobject-client-test.sh
PASS: tests/mobject-aio-test.sh
============================================================================
Testsuite summary for mobject 0.7
============================================================================
# TOTAL: 3
# PASS:  3
# SKIP:  0
# XFAIL: 0
# FAIL:  0
# XPASS: 0
# ERROR: 0
============================================================================
```
