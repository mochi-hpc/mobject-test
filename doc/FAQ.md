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
spack install mobject%gcc@11.2.0
```
