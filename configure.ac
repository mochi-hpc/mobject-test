AC_PREREQ([2.69])
AC_INIT([mobject], [0.4.2], [ssio-services@lists.mcs.anl.gov])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([include/libmobject-store.h])
AC_CONFIG_HEADER([src/mobject-store-config.h])
LT_PREREQ([2.2])
AM_PROG_AR
LT_INIT([dlopen])

# checks for programs
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_INSTALL

# check for C++11
AX_CXX_COMPILE_STDCXX(11, noext, mandatory)

# checks for libraires
# pkg-config and libraries supporting that
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
	AC_MSG_ERROR([Could not find pkg-config utility!])
fi

# coreutils checks for OSX
AC_ARG_VAR([TIMEOUT], timeout program)
AC_ARG_VAR([MKTEMP], mktemp program)

if test -z "$TIMEOUT" ; then
    AC_CHECK_PROGS(TIMEOUT, [timeout gtimeout])
    if test -z "$TIMEOUT" ; then
        AC_MSG_ERROR([Could not find timeout command (can optionally provide via the TIMEOUT variable)])
    fi
else
    AC_SUBST([TIMEOUT], ["$TIMEOUT"])
fi

if test -z "$MKTEMP" ; then
    AC_CHECK_PROGS(MKTEMP, [mktemp gmktemp])
    if test -z "$MKTEMP" ; then
        AC_MSG_ERROR([Could not find mktemp command (can optionally provide via the MKTEMP variable)])
    fi
else
    AC_SUBST([MKTEMP], ["$MKTEMP"])
fi

# check for MPI -- currently required for SSG on server daemons
AC_MSG_CHECKING([If MPI programs can be compiled])
AC_LINK_IFELSE(
    [AC_LANG_PROGRAM([[#include<mpi.h>]], [[MPI_Init(0,0);]])],
    [AC_MSG_RESULT([yes])],
    [AC_MSG_RESULT([no])]
    [AC_MSG_ERROR([MPI compiler support currently required!])])

dnl seems like this should be a function but my first stab at m4 exploded
dnl mightily...

PKG_CHECK_MODULES([MARGO],[margo],[],
	AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SSG],[ssg],[],
	AC_MSG_ERROR([Could not find working ssg installation!]) )
LIBS="$SSG_LIBS $LIBS"
CPPFLAGS="$SSG_CFLAGS $CPPFLAGS"
CFLAGS="$SSG_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([ARGOBOTS],[argobots],[],
	AC_MSG_ERROR([Could not find working argobots installation!]) )
LIBS="$ARGOBOTS_LIBS $LIBS"
CPPFLAGS="$ARGOBOTS_CFLAGS $CPPFLAGS"
CFLAGS="$ARGOBOTS_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SDSKEYVALSERVER],[sdskv-server],[],
	AC_MSG_ERROR([Could not find working sds-keyval server installation!]) )
SERVER_LIBS="$SDSKEYVALSERVER_LIBS $SERVER_LIBS"
SERVER_CPPFLAGS="$SDSKEYVALSERVER_CFLAGS $SERVER_CPPFLAGS"
SERVER_CFLAGS="$SDSKEYVALSERVER_CFLAGS $SERVER_CFLAGS"

PKG_CHECK_MODULES([SDSKEYVALCLIENT],[sdskv-client],[],
	AC_MSG_ERROR([Could not find working sds-keyval client installation!]) )
SERVER_LIBS="$SDSKEYVALCLIENT_LIBS $SERVER_LIBS"
SERVER_CPPFLAGS="$SDSKEYVALCLIENT_CFLAGS $SERVER_CPPFLAGS"
SERVER_CFLAGS="$SDSKEYVALCLIENT_CFLAGS $SERVER_CFLAGS"

PKG_CHECK_MODULES([PMEM],[libpmemobj],[],
	AC_MSG_ERROR([Could not find working libpmemobj installation!]) )
SERVER_LIBS="$PMEM_LIBS $SERVER_LIBS"
SERVER_CPPFLAGS="$PMEM_CFLAGS $SERVER_CPPFLAGS"
SERVER_CFLAGS="$PMEM_CFLAGS $SERVER_CFLAGS"

PKG_CHECK_MODULES([BAKESERVER],[bake-server],[],
	AC_MSG_ERROR([Could not find working BAKE server installation!]) )
SERVER_LIBS="$BAKESERVER_LIBS $SERVER_LIBS"
SERVER_CPPFLAGS="$BAKESERVER_CFLAGS $SERVER_CPPFLAGS"
SERVER_CFLAGS="$BAKESERVER_CFLAGS $SERVER_CFLAGS"

PKG_CHECK_MODULES([BAKECLIENT],[bake-client],[],
	AC_MSG_ERROR([Could not find working BAKE client installation!]) )
SERVER_LIBS="$BAKECLIENT_LIBS $SERVER_LIBS"
SERVER_CPPFLAGS="$BAKECLIENT_CFLAGS $SERVER_CPPFLAGS"
SERVER_CFLAGS="$BAKECLIENT_CFLAGS $SERVER_CFLAGS"

PKG_CHECK_MODULES([CHPLACEMENT], [ch-placement], [],
	AC_MSG_ERROR([Could not find ch-placement]) )
CLIENT_CFLAGS="$CHPLACEMENT_CFLAGS $CLIENT_CFLAGS"
CLIENT_LIBS="$CHPLACEMENT_LIBS $CLIENT_LIBS"

# check that SSG was compiled with MPI support
AC_CHECK_LIB([ssg], [ssg_group_create_mpi],
    [],
    [AC_MSG_ERROR([SSG must be built with MPI support!])])

# check for RADOS
AC_ARG_WITH([rados],
    AS_HELP_STRING([--with_rados], [Additionally build tests against librados (default is no)]),
    [], [])
if test x"$with_rados" == "xyes" ; then
    # check for RADOS headers and libraries
    AC_CHECK_HEADERS([rados/librados.h rados/rados_types.h],
        [],
        [AC_MSG_ERROR([RADOS support requested but could not find RADOS headers!])],
        [])
    AC_CHECK_LIB([rados], [rados_create],
        [],
        [AC_MSG_ERROR([RADOS support requested but could not link against librados!])])

fi

# both found, set output to additionally build tests against RADOS
AM_CONDITIONAL(HAVE_RADOS, test x"$with_rados" == "xyes")

AC_SUBST(SERVER_LIBS)
AC_SUBST(CLIENT_LIBS)
AC_SUBST(SERVER_CPPFLAGS)
AC_SUBST(CLIENT_CPPFLAGS)
AC_SUBST(SERVER_CFLAGS)
AC_SUBST(CLIENT_CFLAGS)
AC_CONFIG_FILES([Makefile maint/mobject-store.pc maint/mobject-server.pc])
AC_OUTPUT
