AC_PREREQ([2.69])
AC_INIT([mobject], [0.5], [ssio-services@lists.mcs.anl.gov])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([include/libmobject-store.h])
AC_CONFIG_HEADERS([src/mobject-store-config.h])
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

# checks for libraries
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

dnl seems like this should be a function but my first stab at m4 exploded
dnl mightily...

PKG_CHECK_MODULES([MARGO],[margo],[],
	AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

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

PKG_CHECK_MODULES([SSG],[ssg],[],
	AC_MSG_ERROR([Could not find working ssg installation!]) )
CLIENT_LIBS="$SSG_LIBS $CLIENT_LIBS"
CLIENT_CPPFLAGS="$SSG_CFLAGS $CLIENT_CPPFLAGS"
CLIENT_CFLAGS="$SSG_CFLAGS $CLIENT_CFLAGS"

# check for RADOS
AC_ARG_WITH([rados],
    AS_HELP_STRING([--with-rados], [Additionally build tests against librados (default is no)]),
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

AC_ARG_ENABLE(bedrock,
              [AS_HELP_STRING([--enable-bedrock],[Enable Bedrock (Mochi loader) support @<:@default=no@:>@])],
              [case "${enableval}" in
                yes) enable_bedrock="yes" ;;
                no) enable_bedrock="no" ;;
                *) AC_MSG_ERROR(bad value ${enableval} for --enable-bedrock) ;;
               esac],
              [enable_bedrock="no"]
)
AM_CONDITIONAL(ENABLE_BEDROCK, test x$enable_bedrock = xyes)
if test "$enable_bedrock" = "yes" ; then
        AC_DEFINE(USE_BEDROCK, 1, [Bedrock support enabled.])
        ENABLE_BEDROCK=1
        PKG_CHECK_MODULES([BEDROCK],[bedrock-server],[],
              AC_MSG_ERROR([Bedrock requested, but could not find bedrock]))
else
        ENABLE_BEDROCK=0
fi
AM_CONDITIONAL(BUILD_BEDROCK, test $ENABLE_BEDROCK -eq 1)
AC_SUBST(ENABLE_BEDROCK)
AC_SUBST(BEDROCK_LIBS)
AC_SUBST(BEDROCK_CFLAGS)

AC_SUBST(SERVER_LIBS)
AC_SUBST(CLIENT_LIBS)
AC_SUBST(SERVER_CPPFLAGS)
AC_SUBST(CLIENT_CPPFLAGS)
AC_SUBST(SERVER_CFLAGS)
AC_SUBST(CLIENT_CFLAGS)
AC_CONFIG_FILES([Makefile maint/mobject-store.pc maint/mobject-server.pc])
AC_OUTPUT
