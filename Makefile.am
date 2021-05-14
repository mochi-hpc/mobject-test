AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

# setting this to empty so subdir makefiles can use +=
noinst_HEADERS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
bin_PROGRAMS =
check_PROGRAMS =
TESTS =
TESTS_ENVIRONMENT=
EXTRA_DIST = prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS   =

SERVER_LIBS = @SERVER_LIBS@
CLIENT_LIBS = @CLIENT_LIBS@
SERVER_CPPFLAGS = @SERVER_CPPFLAGS@
CLIENT_CPPFLAGS = @CLIENT_CPPFLAGS@
SERVER_CFLAGS = @SERVER_CFLAGS@
CLIENT_CFLAGS = @CLIENT_CFLAGS@

lib_LTLIBRARIES = \
  src/client/libmobject-store.la \
  src/server/libmobject-server.la \
  src/server/libmobject-comparators.la

include_HEADERS = \
  include/libmobject-store.h \
  include/librados-mobject-store.h \
  include/mobject-client.h \
  include/mobject-server.h

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/mobject-server.pc \
		 maint/mobject-store.pc
