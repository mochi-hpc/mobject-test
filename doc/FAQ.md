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

Check out the Mobject source.
```
git clone https://github.com/mochi-hpc/mobject
cd mobject
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

* How can I run IOR with Mobject using the RADOS backend?

An [IOR backend for RADOS](https://github.com/hpc/ior/blob/main/src/aiori-RADOS.c)
has been developed and available since IOR 3.2. However, this backend currently
has to be modified slightly to work with Mobject.

### Installing with Spack

If using Spack with the `mochi-spack-packages` repo to manage Mobject and its
dependencies, one should simply install IOR with the Mobject variant by doing:

```bash
$ spack install ior+mobject@develop
```

> **_NOTE:_** If using a spack environment, one may need to instead add the IOR
> spec to their environment:
>
>     spack add ior+mobject@develop
>     spack install

### Installing manually

If managing Mobject and its dependencies manually, one should first obtain
the IOR source code:

```bash
$ git clone https://github.com/hpc/ior.git
```

Then, the [RADOS backend](https://github.com/hpc/ior/blob/main/src/aiori-RADOS.c#L24)
should be edited so that it includes the Mobject header instead of the `rados/librados.h`
header. This can be done by editing like:

  `#include <rados/librados.h>` -> `#include <librados-mobject-store.h>`

Finally, the reference to `-lrados` should be removed from [src/Makefile.am](https://github.com/hpc/ior/blob/main/src/Makefile.am#L94).

Once these modifications have been made, IOR should be configured as shown below.
Note that this assumes that the Mobject-related libraries are available in
`LD_LIBRARY_PATH` and it also assumes that the pkg-config files for the Mobject
libraries and dependencies are available in the pkg-config path. IOR can also be
configured by directly passing the appropriate `CFLAGS` and `LDFLAGS` to configure.

```bash
$ ./bootstrap
$ ./configure --with-rados --prefix=/path/to/install/ CFLAGS="$(pkg-config --cflags mobject-store)" LIBS="$(pkg-config --libs mobject-store)"
$ make install
```

### Running IOR

To run IOR with the RADOS backend:

```bash
$ ior -a RADOS [ior_options] --rados.user=foo --rados.pool=bar --rados.conf=baz
```

Note that the user, pool and conf arguments are ignored by Mobject, but are
required by the IOR RADOS backend.
