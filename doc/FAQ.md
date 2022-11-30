# Frequently Asked Questions

## How can HDF5 RADOS VOL plugin work with Mobject?

  See [h5VLrados.c](
  https://github.com/HDFGroup/vol-rados/blob/master/src/H5VLrados.c) 
  to understand how HDF5 RADOS VOL plugin calls APIs.

## How can I install Mobject on Polaris?

  You can execute the following script.
```
#!/bin/bash

# Git clone 3 repos under your home directory.                                  
git clone https://github.com/spack/spack
git clone https://github.com/mochi-hpc/mochi-spack-packages
git clone https://github.com/mochi-hpc-experiments/platform-configurations
```

Once these repos have been checked out, the [Polaris platform configuration](https://github.com/mochi-hpc-experiments/platform-configurations/blob/main/ANL/Polaris/spack.yaml)
file will need to be edited to point to the mochi-spack-packages repository
that was just cloned. The line that needs to be edited looks like:

```
  repos:
  - /path/to/mochi-spack-packages
```

Once finished, the following should be done:

```
#!/bin/bash

# Set up Spack environment on Polaris.                                          
. ~/spack/share/spack/setup-env.sh
spack repo add ~/mochi-spack-packages
spack env list
spack env create mochi-env ~/platform-configurations/ANL/Polaris/spack.yaml
spack env activate mochi-env

# Install mobject on Polaris.                                                   
spack install mobject%gcc@11.2.0+bedrock
```

## How can I test Mobject on Polaris from source code?

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


## How can I test Mobject with Polaris SSD (/local/scratch)?

Submit a qsub job with the following [config.json](../tests/config.json) change.
```
                "pmem_backend": {
                    "targets": [
                        "/local/scratch/mobject.dat"
                    ]
```
You can change `pmem` to `file`.
```
                "file_backend": {
                    "targets": [
                        "/local/scratch/mobject.dat"
                    ]
```

## How can I run IOR with Mobject using the RADOS backend?

An [IOR backend for RADOS](https://github.com/hpc/ior/blob/main/src/aiori-RADOS.c)
has been developed and available since IOR 3.2. However, this backend currently
has to be modified slightly to work with Mobject.

### Installing with Spack

If using Spack with the `mochi-spack-packages` repo to manage Mobject and its
dependencies, one should simply install IOR with the Mobject variant by doing:

```bash
$ spack install ior@main+mobject ^mobject@main
```

> **_NOTE:_** If using a spack environment, one may need to instead add the IOR
> spec to their environment:
>
>     spack add ior@main+mobject ^mobject@main
>     spack install

### Installing manually

If managing Mobject and its dependencies manually (and assuming these are already
installed), one should first obtain the IOR source code:

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

To run IOR with Mobject using the RADOS backend, one should first start
up a Mobject server using bedrock. Typically this would be achieved with
the bedrock command, as in:

```bash
$ bedrock -c name_of_bedrock_config_file fabric_provider:// &
```

Where `name_of_bedrock_config_file` points to the bedrock configuration file
to use ([see here](https://github.com/mochi-hpc/mobject/blob/main/config/example.json)
for an example) and `fabric_provider` would be the string representing the
libfabric provider to use, such as `verbs://`, `tcp://`, etc. An example of
this can be seen [here](https://github.com/mochi-hpc-experiments/mochi-tests/blob/main/perf-regression/polaris/run_ior.qsub#L41-L42).

> **_NOTE:_** It may currently be necessary to add a small wait via
>
>     sleep 5
>
> or similar to allow the bedrock server to come up before using it.

Once the Mobject server is running, IOR can be run with the RADOS backend
by doing:

```bash
$ ior -a RADOS [ior_options] --rados.user=foo --rados.pool=bar --rados.conf=baz
```

Note that the user, pool and conf arguments are ignored by Mobject, but are
required by the IOR RADOS backend.

* How can I run IOR with Mobject using the HDF5 backend?

An [HDF5 VOL connector for RADOS](https://github.com/HDFGroup/vol-rados)
has been developed and can be used to forward HDF5 API calls to the RADOS API
via Mobject. This VOL connector requires a parallel-enabled install of HDF5
1.13.3 or greater. It also requires Mobject by default, but can be built
without Mobject if intending to use the RADOS API via Ceph.

### Installing with Spack

If using Spack with the `mochi-spack-packages` repo to manage HDF5, Mochi, Mobject
and their dependencies, one should simply install IOR and the VOL connector
by doing:

```bash
$ spack install ior@main+hdf5
$ spack install hdf5-rados
```

> **_NOTE:_** If using a spack environment, one may need to instead add IOR and
> the VOL connector specs to their environment:
>
>     spack add ior@main+hdf5
>     spack add hdf5-rados
>     spack install

### Installing manually

If managing HDF5, Mochi, Mobject and their dependencies manually (and assuming
these are already installed), one should first obtain the IOR source code:

```bash
$ git clone https://github.com/hpc/ior.git
```

and then build IOR by doing:

```bash
$ cd ior
$ ./bootstrap
$ ./configure --with-hdf5 --prefix=/path/to/install/ CFLAGS="$(pkg-config --cflags hdf5)" LIBS="$(pkg-config --libs hdf5)"
$ make install
```

Note that this assumes that the HDF5 library is available in `LD_LIBRARY_PATH` and
it also assumes that the pkg-config files for the HDF5 library and dependencies
are available in the pkg-config path. IOR can also be configured by directly passing
the appropriate `CFLAGS` and `LDFLAGS` for your HDF5 installation to configure.

Once IOR has been installed, one should obtain the HDF5 RADOS VOL connector source
code:

```bash
$ git clone https://github.com/HDFGroup/vol-rados.git
```

Once the source code has been obtained, the VOL connector can be built with CMake:

```bash
$ cd vol-rados
$ mkdir build
$ cd build
$ cmake [cmake options] [connector options] ..
$ make install
```

> Refer to https://github.com/HDFGroup/vol-rados/blob/master/README.md for more
> detailed instructions on building the connector

### Running IOR

To run IOR with Mobject using the HDF5 backend, one should first start up
a Mobject server using bedrock similar to the above instructions for running
with the RADOS backend:

```bash
$ bedrock -c name_of_bedrock_config_file fabric_provider:// &
```

Where `name_of_bedrock_config_file` points to the bedrock configuration file
to use and `fabric_provider` would be the string representing the libfabric
provider to use, such as `verbs://`, `tcp://`, etc.

> **_NOTE:_** It may currently be necessary to add a small wait via
>
>     sleep 5
>
> or similar to allow the bedrock server to come up before using it.

Once the Mobject server is running, two environment variables should be set
to instruct HDF5 how to load and use the RADOS VOL connector:

```bash
export HDF5_VOL_CONNECTOR=rados
export HDF5_PLUGIN_PATH=/path/to/installed/connector
```

If the RADOS VOL connector was installed via spack, the following should
correctly set the HDF5 plugin path:

```bash
export HDF5_PLUGIN_PATH="`spack location -i hdf5-rados`/lib"
```

Finally, once these environment variables are set and the bedrock server
for Mobject has come up, IOR can be run with the HDF5 backend by doing:

```bash
$ ior -a HDF5 [ior_options] [hdf5_options]
```

> Refer to https://github.com/hpc/ior/blob/main/README_HDF5 for a list
> of HDF5 options that can be specified

