ifeq ($(strip $(HOST)),)
	HOST = $(shell hostname)
endif
#
# Local include directory
#
INCLUDEDIR = ./include
#
# Local source directory
#
SOURCEDIR = ./src
#
# Flag for whether it is okay to make on this host
#
OKAY_TO_MAKE = 0
#
TIMESTAMP = $(shell date +%Y%m%d%H%M%S)
#
ifneq ($(or $(findstring casper,$(HOST))),)
# Settings for the DAV nodes
	BUILDEXT = dav
	COMPILER = /glade/u/apps/dav/opt/gnu/9.1.0/bin/g++
	GCCVERSION = $(shell $(COMPILER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 9.1.0
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
	DBINCLUDEDIR = /glade/work/dattore/conda-envs/libpq/include
	LIBDIR = /glade/u/home/rdadata/lib/dav
	DBLIBDIR = /gpfs/fs1/work/dattore/conda-envs/libpq/lib
	DBLIBS = -lpq -lpostgresql
	JASPERINCLUDEDIR = /glade/work/dattore/conda-envs/jasper/include
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	BINDIR = /glade/u/home/rdadata/bin/dav
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /ncar/rda/setuid/bin/rdadatarun
endif
#
ifneq ($(or $(findstring cheyenne,$(HOST)), $(findstring chadmin,$(HOST))),)
# Cheyenne settings
	BUILDEXT = ch
	COMPILER = /glade/u/apps/ch/opt/gnu/9.1.0/bin/g++
	GCCVERSION = $(shell $(COMPILER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 9.1.0
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
	DBINCLUDEDIR = /glade/work/dattore/conda-envs/libpq/include
	LIBDIR = /glade/u/home/rdadata/lib/ch
	DBLIBDIR = /gpfs/fs1/work/dattore/conda-envs/libpq/lib
	DBLIBS = -lpq -lpostgresql
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	BINDIR = /glade/u/home/rdadata/bin/ch
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/ch/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /ncar/rda/setuid/bin/rdadatarun
endif
#
ifneq ($(or $(findstring rda-data,$(HOST)),$(findstring rda-web-test,$(HOST)),$(findstring rda-web-dev,$(HOST))),)
# Settings for the VM machines
ifneq ($(findstring rda-data,$(HOST)),)
	BUILDEXT = vm-data
else ifneq ($(findstring rda-web-test,$(HOST)),)
	BUILDEXT = vm-web-test
else ifneq ($(findstring rda-web-dev,$(HOST)),)
	BUILDEXT = vm-web-dev
endif
	COMPILER = g++49
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
        DBINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/local/decs/lib
        DBLIBDIR = /usr/lib64/mysql
	DBLIBS = mysqlclient
	JASPERLIBDIR = /usr/lib64
	BINDIR = /usr/local/decs/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /usr/local/decs/bin/rdadatarun
endif
#
ifneq ($(findstring rda-web-test01,$(HOST)),)
# Settings for alma-linux machines
ifneq ($(findstring rda-web-test01,$(HOST)),)
	BUILDEXT = alma-web-test
endif
	COMPILER = g++
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
        DBINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/local/decs/lib
        DBLIBDIR = /usr/lib64
	DBLIBS = mysqlclient
	JASPERLIBDIR = /usr/lib64
	BINDIR = /usr/local/decs/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /usr/local/decs/bin/rdadatarun
endif
#
ifneq ($(findstring rda-work,$(HOST)),)
# Settings for rda-work
	BUILDEXT = work
	COMPILER = g++
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
	DBINCLUDEDIR = /usr/pgsql-12/include
        LIBDIR = /usr/local/decs/lib
	DBLIBDIR = /usr/lib64
	DBLIBS = -lpq -lpostgresql
	JASPERLIBDIR = /usr/lib64
	BINDIR = /usr/local/decs/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /usr/local/decs/bin/rdadatarun
endif
ifneq ($(findstring singularity,$(HOST)),)
# Settings for singularity containers
	BUILDEXT = singularity
	COMPILER = g++
	GLOBALINCLUDEDIR = /usr/include/myincludes
        DBINCLUDEDIR = /usr/include/postgresql
        LIBDIR = /usr/lib64/mylibs
        DBLIBDIR = /usr/lib/x86_64-linux-gnu
	DBLIBS = -lpq -lpostgresql
	JASPERINCLUDEDIR = /usr/include/jasper
	JASPERLIBDIR = /usr/lib/x86_64-linux-gnu/lib
	BINDIR = /usr/local/bin
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
endif
#
# If not a "make-able" host, exit with an error message
#
ifeq ($(OKAY_TO_MAKE),0)
%: WRONG-HOST
	$(error unable to make on $(HOST))
WRONG-HOST: ;
endif
#
# Compiler options
#
COMPILE_OPTIONS = -Wall -Wold-style-cast -O3 -std=c++17 -Weffc++ $(DIRECTIVES)
#
# Run-path settings
DBRUNPATH = -Wl,-rpath,$(DBLIBDIR)
JASPERRUNPATH = -Wl,-rpath,$(JASPERLIBDIR)
#
EXECUTABLE =
#
# Set the build directory
#
BUILDDIR = ./build-$(BUILDEXT)
#
PWD = $(shell pwd)
#
# Get the list of libary object files by checking the library source directory
#
GATHERXMLOBJS = $(subst $(SOURCEDIR),$(BUILDDIR),$(patsubst %.cpp,%.o,$(wildcard $(SOURCEDIR)/libgatherxml/*.cpp)))
#
.PHONY: builddir get_version clean install
#
all: _ascii2xml _bufr2xml _dcm _dsgen _fix2xml _gatherxml _grid2xml _gsi _hdf2xml _iinv _nc2xml _obs2xml _prop2xml _rcm _scm _sdp _sml
#
# libgatherxml.so
#
$(BUILDDIR)/libgatherxml/%.o: $(SOURCEDIR)/libgatherxml/%.cpp $(INCLUDEDIR)/gatherxml.hpp
	$(COMPILER) $(COMPILE_OPTIONS) -c -fPIC $< -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -o $@
#
libgatherxml.so: CHECKDIR=$(LIBDIR)
libgatherxml.so: CHECK_TARGET=libgatherxml.so
libgatherxml.so: builddir get_version $(GATHERXMLOBJS) $(INCLUDEDIR)/gatherxml.hpp
ifeq ($(OKAY_TO_MAKE),1)
	$(COMPILER) -shared -o $(BUILDDIR)/$@.$(NEW_VERSION) -Wl,-soname,$@.$(NEW_VERSION) $(GATHERXMLOBJS)
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(LIBDIR) install
ifneq ($(findstring rda-web-test01,$(HOST)),)
	$(RDADATARUN) rsync -a $(LIBDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod01.ucar.edu:$(LIBDIR)/ \
	  && $(RDADATARUN) rsync -a $(LIBDIR)/$@ rdadata@rda-web-prod01.ucar.edu:$(LIBDIR)/
endif
endif
#
_ascii2xml: CHECKDIR=$(BINDIR)
_ascii2xml: CHECK_TARGET=_ascii2xml
_ascii2xml: $(SOURCEDIR)/_ascii2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lobs -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils_pg -lmetahelpers_pg -lgridutils -lsearch_pg -lxml -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_bufr2xml: CHECKDIR=$(BINDIR)
_bufr2xml: CHECK_TARGET=_bufr2xml
_bufr2xml: $(SOURCEDIR)/_bufr2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lbufr -lobs -lbitmap -lgridutils -lsearch_pg -lxml -ls3 -lmyssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
cmd_util_pg: CHECKDIR=$(LOCALBINDIR)
cmd_util_pg: CHECK_TARGET=cmd_util_pg
cmd_util_pg: $(SOURCEDIR)/cmd_util_pg.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lbitmap -lsearch_pg -lxml -lz -lcurl)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(DBLIBS) -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lbitmap -lsearch_pg -lxml -lz -lcurl -o $(BUILDDIR)/$@.$(NEW_VERSION)
ifneq ($(VERSION),TEST)
	$(RDADATARUN) cp $(BUILDDIR)/$@.$(NEW_VERSION) $(LOCALBINDIR)/$@.$(NEW_VERSION) \
          && cd $(LOCALBINDIR) \
	  && $(RDADATARUN) chmod 4710 $@.$(NEW_VERSION) \
	  && ln -s -f $@.$(NEW_VERSION) $@
ifneq ($(findstring rda-web-test01,$(HOST)),)
	$(RDADATARUN) rsync -a $(LOCALBINDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod01.ucar.edu:$(LOCALBINDIR)/
	$(RDADATARUN) rsync -a $(LOCALBINDIR)/$@ rdadata@rda-web-prod01.ucar.edu:$(LOCALBINDIR)/
endif
endif
endif
#
_dcm: CHECKDIR=$(BINDIR)
_dcm: CHECK_TARGET=_dcm
_dcm: $(SOURCEDIR)/_dcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lsearch_pg -lxml -lbitmap -ls3 -lmyssl -lz -lcurl -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_dsgen: CHECKDIR=$(BINDIR)
_dsgen: CHECK_TARGET=_dsgen
_dsgen: $(SOURCEDIR)/_dsgen.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lmetaexport_pg -lmetaexporthelpers_pg -lgridutils -lsearch_pg -lxml -lbitmap -lcitation_pg -lz -lpthread -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
ifneq ($(findstring rda-web-test01,$(HOST)),)
	$(RDADATARUN) rsync -a $(BINDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod01.ucar.edu:$(BINDIR)/
	$(RDADATARUN) rsync -a $(BINDIR)/$@ rdadata@rda-web-prod01.ucar.edu:$(BINDIR)/
endif
endif
#
_fix2xml: CHECKDIR=$(BINDIR)
_fix2xml: CHECK_TARGET=_fix2xml
_fix2xml: $(SOURCEDIR)/_fix2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lgatherxml $(DBLIBS) -lcyclone -lutils -lutilsthread -ldatetime -lio -lmetautils_pg -lmetahelpers_pg -lgridutils -lbitmap -lsearch_pg -lxml -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_gatherxml: CHECKDIR=$(BINDIR)
_gatherxml: CHECK_TARGET=_gatherxml
_gatherxml: $(SOURCEDIR)/_gatherxml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lbitmap -lsearch_pg -lxml -lmyssl -ls3 -lz -lpthread -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_grid2xml: CHECKDIR=$(BINDIR)
_grid2xml: CHECK_TARGET=_grid2xml
_grid2xml: $(SOURCEDIR)/_grid2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lgatherxml $(DBLIBS) -lgrids -ljasper -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils_pg -lmetahelpers_pg -lgridutils -lsearch_pg -lxml -ls3 -lmyssl -lerror -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -I$(JASPERINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -I$(JASPERINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_gsi: CHECKDIR=$(BINDIR)
_gsi: CHECK_TARGET=_gsi
_gsi: $(SOURCEDIR)/_gsi.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lbitmap -lxml -lsearch_pg -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_hdf2xml: CHECKDIR=$(BINDIR)
_hdf2xml: CHECK_TARGET=_hdf2xml
_hdf2xml: $(SOURCEDIR)/_hdf2xml.cpp builddir get_version
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lio -lutils -lutilsthread -ldatetime -lbitmap -lhdf -lmetautils_pg -lmetahelpers_pg -lgridutils -lxml -lsearch_pg -ls3 -lmyssl -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_iinv: CHECKDIR=$(BINDIR)
_iinv: CHECK_TARGET=_iinv
_iinv: $(SOURCEDIR)/_iinv.cpp builddir get_version
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lbitmap -lmetautils_pg -lmetahelpers_pg -lsearch_pg -lxml -lgridutils -ls3 -lmyssl -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_nc2xml: CHECKDIR=$(BINDIR)
_nc2xml: CHECK_TARGET=_nc2xml
_nc2xml: $(SOURCEDIR)/_nc2xml.cpp builddir get_version
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lsearch_pg -lxml -lbufr -lbitmap -ls3 -lmyssl -lerror -lpthread -lz -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_obs2xml: CHECKDIR=$(BINDIR)
_obs2xml: CHECK_TARGET=_obs2xml
_obs2xml: $(SOURCEDIR)/_obs2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lgatherxml $(DBLIBS) -lio -lobs -lutils -lutilsthread -ldatetime -lbitmap -lmetautils_pg -lmetahelpers_pg -lsearch_pg -lxml -lgridutils -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_prop2xml: CHECKDIR=$(BINDIR)
_prop2xml: CHECK_TARGET=_prop2xml
_prop2xml: $(SOURCEDIR)/_prop2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lbitmap -lgridutils -lsearch_pg -lxml -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_rcm: CHECKDIR=$(BINDIR)
_rcm: CHECK_TARGET=_rcm
_rcm: $(SOURCEDIR)/_rcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils_pg -lmetahelpers_pg -lgridutils -lsearch_pg -lxml -lbitmap -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_scm: CHECKDIR=$(BINDIR)
_scm: CHECK_TARGET=_scm
_scm: $(SOURCEDIR)/_scm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lgatherxml -lutils -lutilsthread -ldatetime -lgridutils -lmetautils_pg -lmetahelpers_pg -lsearch_pg -lxml -lbitmap -ls3 -lmyssl -lz -lcurl -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(PWD)/$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_sdp: CHECKDIR=$(BINDIR)
_sdp: CHECK_TARGET=_sdp
_sdp: $(SOURCEDIR)/_sdp.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetautils_pg -lmetahelpers_pg -lsearch_pg -lxml -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_sml: CHECKDIR=$(BINDIR)
_sml: CHECK_TARGET=_sml
_sml: $(SOURCEDIR)/_sml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = $(DBLIBS) -lmetautils_pg -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetahelpers_pg -lsearch_pg -lxml -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(DBRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(DBINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(DBLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
gatherxml_pg-1-os.sif gatherxml_pg-2-pkgs.sif gatherxml_pg-3-python.sif gatherxml_pg-4-libs.sif:
ifneq ($(or $(findstring rda-web-dev,$(HOST)),$(findstring rda-work,$(HOST))),)
	cd /data/ptmp \
	  && export SINGULARITY_TMPDIR=/data/ptmp \
	  && sudo -E /usr/local/bin/singularity build --force $@ /glade/u/home/dattore/gatherxml_pg/singularity/$(basename $@).def \
	  && mv -f $@ /glade/u/home/rdadata/lib/singularity/
else
	$(error make on rda-web-dev.ucar.edu or rda-work.ucar.edu)
endif
#
gatherxml_pg-4-libs-update.sif:
ifneq ($(or $(findstring rda-web-dev,$(HOST)),$(findstring rda-work,$(HOST))),)
	cd /data/ptmp \
	  && export SINGULARITY_TMPDIR=/data/ptmp \
	  && sudo -E /usr/local/bin/singularity build --force $@ /glade/u/home/dattore/gatherxml_pg/singularity/$(basename $@).def \
	  && mv -f $@ /glade/u/home/rdadata/lib/singularity/$(subst -update,,$(basename $@).sif)
else
	$(error make on rda-web-dev.ucar.edu or rda-work.ucar.edu)
endif
#
gatherxml_pg-5-utils.sif:
ifneq ($(or $(findstring rda-web-dev,$(HOST)),$(findstring rda-work,$(HOST))),)
ifeq ($(strip $(UTILITY)),)
	$(eval GO_BACK = $(shell pwd))
	cd /data/ptmp \
	  && export SINGULARITY_TMPDIR=/data/ptmp \
	  && sudo -E /usr/local/bin/singularity build --force $@ /glade/u/home/dattore/gatherxml_pg/singularity/$(basename $@).def \
	  && mv -f $@ /glade/u/home/rdadata/lib/singularity/ \
	  && cd $(GO_BACK) \
	  && $(MAKE) gatherxml_pg-exec.sif
else
	$(error found UTILITY=$(UTILITY) - did you mean to build 'gatherxml-5-utils-update.sif'?)
endif
else
	$(error make on rda-web-dev.ucar.edu or rda-work.ucar.edu)
endif
#
gatherxml_pg-5-utils-update.sif:
ifneq ($(or $(findstring rda-web-dev,$(HOST)),$(findstring rda-work,$(HOST))),)
ifneq ($(strip $(UTILITY)),)
	$(eval GO_BACK = $(shell pwd))
	cd /data/ptmp \
	  && export SINGULARITY_TMPDIR=/data/ptmp \
	  && export UTILITY=$(UTILITY) \
	  && sudo -E /usr/local/bin/singularity build --force $@ /glade/u/home/dattore/gatherxml_pg/singularity/$(basename $@).def \
	  && mv -f $@ /glade/u/home/rdadata/lib/singularity/$(subst -update,,$(basename $@).sif) \
	  && cd $(GO_BACK) \
	  && $(MAKE) gatherxml_pg-exec.sif
else
	$(error missing or undefined utility name (UTILITY=))
endif
else
	$(error make on rda-web-dev.ucar.edu or rda-work.ucar.edu)
endif
#
gatherxml_pg-exec.sif:
ifneq ($(or $(findstring rda-web-dev,$(HOST)),$(findstring rda-work,$(HOST))),)
	cd /data/ptmp \
	  && export SINGULARITY_TMPDIR=/data/ptmp \
	  && sudo -E /usr/local/bin/singularity build --force $@.$(TIMESTAMP) /glade/u/home/dattore/gatherxml_pg/singularity/$(basename $@).def \
	  && mv -f $@.$(TIMESTAMP) /glade/u/home/rdadata/bin/singularity/ \
	  && cd /glade/u/home/rdadata/bin/singularity \
	  && ln -s -f $@.$(TIMESTAMP) $@
else
	$(error make on rda-web-dev.ucar.edu or rda-work.ucar.edu)
endif
#
gatherxml: gatherxml-base.sif gatherxml-libs.sif gatherxml-utils.sif
#
# Create the build directory
#
builddir:
	mkdir -p $(BUILDDIR)/libgatherxml
#
# get the version number to build
#
get_version:
	@ if [ "$(CHECKDIR)" = "" ]; then \
 	  echo "No check directory specified (use CHECKDIR=)"; \
 	  exit 1; \
 	fi; \
 	if [ "$(CHECK_TARGET)" = "" ]; then \
 	  echo "No check target specified (use CHECK_TARGET=)"; \
 	  exit 1; \
 	fi; \
	if [ "$(VERSION)" = "" ]; then \
 	  echo "Missing version number (use VERSION=)"; \
	  exit 1; \
 	fi;
	$(eval NEW_VERSION = $(shell \
	  version="1.0.0"; \
	  if [ -e $(CHECKDIR)/$(CHECK_TARGET) ]; then \
	    version=`ls -l $(CHECKDIR)/$(CHECK_TARGET) |awk '{print $$11}' |sed "s|$(CHECKDIR)/||" |sed "s|$(CHECK_TARGET).||"`; \
	  fi; \
	  major=`echo $$version |awk -F. '{print $$1}'`; \
	  if [ "$(VERSION)" = "MAJOR" ]; then \
	    major=$$(( $$major + 1 )); \
	    echo "$$major.0.0"; \
	  else \
	    minor=`echo $$version |awk -F. '{print $$2}'`; \
	    if [ "$(VERSION)" = "MINOR" ]; then \
	      minor=$$(( $$minor + 1 )); \
	      echo "$$major.$$minor.0"; \
	    else \
	      bug=`echo $$version |awk -F. '{print $$3}'`; \
	      if [ "$(VERSION)" = "BUG" ]; then \
	        bug=$$(( $$bug + 1 )); \
	        echo "$$major.$$minor.$$bug"; \
	      else \
	        echo "$(VERSION)"; \
	      fi; \
	    fi; \
	  fi; \
	))
#
# Install the executable and link to the latest version
#
install:
ifneq ($(strip $(INSTALLDIR)),)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(RDADATARUN) cp $(BUILDDIR)/$(TARGET).$(NEW_VERSION) $(INSTALLDIR)/$(TARGET).$(NEW_VERSION) \
	  && cd $(INSTALLDIR) \
	  && $(RDADATARUN) chmod 740 $(TARGET).$(NEW_VERSION) \
	  && $(RDADATARUN) ln -s -f $(TARGET).$(NEW_VERSION) $(TARGET)
else
ifneq ($(findstring singularity,$(HOST)),)
	mv $(BUILDDIR)/$(TARGET).$(NEW_VERSION) $(INSTALLDIR)/$(TARGET).$(NEW_VERSION) \
	  && cd $(INSTALLDIR) \
	  && ln -s -f $(TARGET).$(NEW_VERSION) $(TARGET)
else
	cd $(BUILDDIR) \
	  && ln -s -f $(TARGET).$(NEW_VERSION) $(TARGET)
endif
endif
else
	$(error missing installation directory (INSTALLDIR=))
endif
#
# Remove the build directory
#
clean:
	rm -rf $(BUILDDIR)
