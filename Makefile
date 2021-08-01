ifeq ($(strip $(HOST)),)
	HOST = $(shell hostname)
endif
#
# Compiler options
#
COMPILE_OPTIONS = -Wall -Wold-style-cast -O3 -std=c++14 -Weffc++
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
ifneq ($(or $(findstring casper,$(HOST))),)
# Settings for the DAV nodes
	BUILDEXT = dav
	COMPILER = /glade/u/apps/dav/opt/ncarcompilers/0.4.1/g++
	GCCVERSION = $(shell $(COMPILER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.3.0
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
	MYSQLINCLUDEDIR = /usr/include/mysql
	LIBDIR = /glade/u/home/rdadata/lib/dav
	MYSQLLIBDIR = /usr/lib64/mysql
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
	COMPILER = /glade/u/apps/ch/opt/gnu/7.1.0/bin/g++
	GCCVERSION = $(shell $(COMPILLER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.1.0
	GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
	MYSQLINCLUDEDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/include
	LIBDIR = /glade/u/home/rdadata/lib/ch
	MYSQLLIBDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/lib
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
        MYSQLINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/local/decs/lib
        MYSQLLIBDIR = /usr/lib64/mysql
	JASPERLIBDIR = /usr/lib64
	BINDIR = /usr/local/decs/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /usr/local/decs/bin/rdadatarun
endif
#
ifneq ($(findstring singularity,$(HOST)),)
# Settings for singularity containers
	BUILDEXT = singularity
	COMPILER = g++
	GLOBALINCLUDEDIR = /usr/include/myincludes
        MYSQLINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/lib64/mylibs
ifneq ($(findstring centos,$(VERSION)),)
        MYSQLLIBDIR = /usr/lib64
	JASPERLIBDIR = /usr/lib64
else
ifneq ($(findstring ubuntu,$(VERSION)),)
        MYSQLLIBDIR = /usr/lib/x86_64-linux-gnu
	JASPERLIBDIR = /usr/lib/x86_64-linux-gnu
endif
endif
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
# Run-path settings
MYSQLRUNPATH = -Wl,-rpath,$(MYSQLLIBDIR)
JASPERRUNPATH = -Wl,-rpath,$(JASPERLIBDIR)
#
EXECUTABLE =
#
# Set the build directory
#
BUILDDIR = ./build-$(BUILDEXT)
#
# Get the list of libary object files by checking the library source directory
#
GATHERXMLOBJS = $(subst $(SOURCEDIR),$(BUILDDIR),$(patsubst %.cpp,%.o,$(wildcard $(SOURCEDIR)/libgatherxml/*.cpp)))
#
.PHONY: builddir clean install
#
all: _ascii2xml _bufr2xml _dcm _dsgen _fix2xml _gatherxml _grid2xml _gsi _hdf2xml _iinv _nc2xml _obs2xml _prop2xml _rcm _scm _sdp _sml
#
# libgatherxml.so
#
$(BUILDDIR)/libgatherxml/%.o: $(SOURCEDIR)/libgatherxml/%.cpp $(INCLUDEDIR)/gatherxml.hpp
	$(COMPILER) $(COMPILE_OPTIONS) -c -fPIC $< -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -o $@
#
libgatherxml.so: CHECKDIR=$(LIBDIR)
libgatherxml.so: CHECK_TARGET=libgatherxml.so
libgatherxml.so: builddir get_version $(GATHERXMLOBJS) $(INCLUDEDIR)/gatherxml.hpp
ifeq ($(OKAY_TO_MAKE),1)
	$(COMPILER) -shared -o $(BUILDDIR)/$@.$(NEW_VERSION) -Wl,-soname,$@.$(NEW_VERSION) $(GATHERXMLOBJS)
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(LIBDIR) install
ifneq ($(findstring rda-web-test,$(HOST)),)
	$(RDADATARUN) rsync -a $(LIBDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod.ucar.edu:$(LIBDIR)/ \
	  && $(RDADATARUN) rsync -a $(LIBDIR)/$@ rdadata@rda-web-prod.ucar.edu:$(LIBDIR)/
endif
endif
#
_ascii2xml: $(SOURCEDIR)/_ascii2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lobs -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_bufr2xml: CHECKDIR=$(BINDIR)
_bufr2xml: CHECK_TARGET=_bufr2xml
_bufr2xml: $(SOURCEDIR)/_bufr2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbufr -lobs -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbufr -lobs -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
cmd_util: $(SOURCEDIR)/cmd_util.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lcurl -o $(BUILDDIR)/$@.$(VERSION)
ifneq ($(VERSION),TEST)
	$(RDADATARUN) cp $(BUILDDIR)/$@.$(VERSION) $(LOCALBINDIR)/$@.$(VERSION) \
          && cd $(LOCALBINDIR) \
	  && $(RDADATARUN) chmod 4710 $@.$(VERSION) \
	  && ln -s -f $@.$(VERSION) $@
endif
endif
endif
#
_dcm: CHECKDIR=$(BINDIR)
_dcm: CHECK_TARGET=_dcm
_dcm: $(SOURCEDIR)/_dcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lmyssl -ls3 -lz -lcurl -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_dsgen: CHECKDIR=$(BINDIR)
_dsgen: CHECK_TARGET=_dsgen
_dsgen: $(SOURCEDIR)/_dsgen.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lmetaexport -lmetaexporthelpers -lgridutils -lsearch -lxml -lbitmap -lcitation -lz -lpthread -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
ifneq ($(findstring rda-web-test,$(HOST)),)
	$(RDADATARUN) rsync -a $(BINDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod.ucar.edu:$(BINDIR)/
	$(RDADATARUN) rsync -a $(BINDIR)/$@ rdadata@rda-web-prod.ucar.edu:$(BINDIR)/
endif
endif
#
_fix2xml: CHECKDIR=$(BINDIR)
_fix2xml: CHECK_TARGET=_fix2xml
_fix2xml: $(SOURCEDIR)/_fix2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -lmysql -lmysqlclient -lgatherxml -lcyclone -lutils -lutilsthread -ldatetime -lio -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -lmysql -lmysqlclient -lgatherxml -lcyclone -lutils -lutilsthread -ldatetime -lio -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_gatherxml: CHECKDIR=$(BINDIR)
_gatherxml: CHECK_TARGET=_gatherxml
_gatherxml: $(SOURCEDIR)/_gatherxml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lmyssl -ls3 -lz -lpthread -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_grid2xml: CHECKDIR=$(BINDIR)
_grid2xml: CHECK_TARGET=_grid2xml
_grid2xml: $(SOURCEDIR)/_grid2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lgatherxml -lmysqlclient -lmysql -lgrids -ljasper -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -ls3 -lmyssl -lerror -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_gsi: CHECKDIR=$(BINDIR)
_gsi: CHECK_TARGET=_gsi
_gsi: $(SOURCEDIR)/_gsi.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_hdf2xml: CHECKDIR=$(BINDIR)
_hdf2xml: CHECK_TARGET=_hdf2xml
_hdf2xml: $(SOURCEDIR)/_hdf2xml.cpp builddir get_version
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lio -lutils -lutilsthread -ldatetime -lbitmap -lhdf -lmetautils -lmetahelpers -lgridutils -lxml -lsearch -lmyssl -ls3 -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_iinv: CHECKDIR=$(BINDIR)
_iinv: CHECK_TARGET=_iinv
_iinv: $(SOURCEDIR)/_iinv.cpp builddir get_version
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -ls3 -lmyssl -lcurl -lpthread -lz)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_nc2xml: CHECKDIR=$(BINDIR)
_nc2xml: CHECK_TARGET=_nc2xml
_nc2xml: $(SOURCEDIR)/_nc2xml.cpp builddir get_version
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbufr -lbitmap -ls3 -lmyssl -lerror -lpthread -lz -lcurl)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION)),$(findstring singularity,$(HOST))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_obs2xml: CHECKDIR=$(BINDIR)
_obs2xml: CHECK_TARGET=_obs2xml
_obs2xml: $(SOURCEDIR)/_obs2xml.cpp builddir get_version
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lio -lobs -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -ls3 -lmyssl -lssl -lcurl -lz -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_prop2xml: $(SOURCEDIR)/_prop2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/_prop2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/_prop2xml.$(VERSION)
endif
endif
#
_rcm: CHECKDIR=$(BINDIR)
_rcm: CHECK_TARGET=_rcm
_rcm: $(SOURCEDIR)/_rcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_scm: CHECKDIR=$(BINDIR)
_scm: CHECK_TARGET=_scm
_scm: $(SOURCEDIR)/_scm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
	$(eval LINK_LIBS = -lmysqlclient -lmysql -lgatherxml -lutils -lutilsthread -ldatetime -lgridutils -lmetautils -lmetahelpers -lsearch -lxml -lbitmap -ls3 -lmyssl -lz -lcurl -lpthread)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) $(LINK_LIBS) -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_sdp: $(SOURCEDIR)/_sdp.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_sml: CHECKDIR=$(BINDIR)
_sml: CHECK_TARGET=_sml
_sml: $(SOURCEDIR)/_sml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
gatherxml-base.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	cd /data/ptmp \
	  && sudo /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/lib/singularity/
else
	$(error missing or undefined OS version (VERSION=))
endif
else
	$(error make on rda-web-dev.ucar.edu)
endif
#
gatherxml-libs.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	cd /data/ptmp \
	  && sudo /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/lib/singularity/
else
	$(error missing or undefined version (VERSION=))
endif
else
	$(error make on rda-web-dev.ucar.edu)
endif
#
gatherxml-libs-update.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	cd /data/ptmp \
	  && sudo /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/lib/singularity/$(subst -update,,$(basename $@)-$(VERSION).sif)
else
	$(error missing or undefined version (VERSION=))
endif
else
	$(error make on rda-web-dev.ucar.edu)
endif
#
gatherxml-utils.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifeq ($(strip $(UTILITY)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	$(eval GO_BACK = $(shell pwd))
	cd /data/ptmp \
	  && sudo /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/lib/singularity/ \
	  && cd $(GO_BACK) \
	  && $(MAKE) gatherxml-exec.sif
else
	$(error missing or undefined version (VERSION=))
endif
else
	$(error found UTILITY=$(UTILITY) - did you mean to build 'gatherxml-utils-update.sif'?)
endif
else
	$(error make on rda-web-dev.ucar.edu)
endif
#
gatherxml-utils-update.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifneq ($(strip $(UTILITY)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	$(eval GO_BACK = $(shell pwd))
	cd /data/ptmp \
	  && export UTILITY=$(UTILITY) \
	  && sudo -E /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/lib/singularity/$(subst -update,,$(basename $@)-$(VERSION).sif) \
	  && cd $(GO_BACK) \
	  && $(MAKE) gatherxml-exec.sif
else
	$(error missing or undefined version (VERSION=))
endif
else
	$(error missing or undefined utility name (UTILITY=))
endif
else
	$(error make on rda-web-dev.ucar.edu)
endif
#
gatherxml-exec.sif:
ifneq ($(findstring rda-web-dev,$(HOST)),)
ifneq ($(findstring ubuntu,$(VERSION)),)
	cd /data/ptmp \
	  && sudo /usr/local/bin/singularity build --force $(basename $@)-$(VERSION).sif /glade/u/home/dattore/gatherxml/singularity/$(basename $@)-$(VERSION).def \
	  && mv -f $(basename $@)-$(VERSION).sif /glade/u/home/rdadata/bin/singularity/$(basename $@)-$(VERSION).sif
else
	$(error missing or undefined version (VERSION=))
endif
else
	$(error make on rda-web-dev.ucar.edu)
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
	fi;
ifeq ($(strip $(VERSION)),BUG)
	$(eval NEW_VERSION = $(shell \
	  version=`ls -l $(CHECKDIR)/$(CHECK_TARGET) |awk '{print $$11}' |sed "s|$(CHECKDIR)/||" |sed "s|$(CHECK_TARGET).||"`; \
	  major=`echo $$version |awk -F. '{print $$1}'`; \
	  minor=`echo $$version |awk -F. '{print $$2}'`; \
	  bug=`echo $$version |awk -F. '{print $$3}'`; \
	  bug=$$(( $$bug + 1 )); \
	  echo "$$major.$$minor.$$bug"))
else
ifeq ($(strip $(VERSION)),MINOR)
	$(eval NEW_VERSION = $(shell \
	  version=`ls -l $(CHECKDIR)/$(CHECK_TARGET) |awk '{print $$11}' |sed "s|$(CHECKDIR)/||" |sed "s|$(CHECK_TARGET).||"`; \
	  major=`echo $$version |awk -F. '{print $$1}'`; \
	  minor=`echo $$version |awk -F. '{print $$2}'`; \
	  minor=$$(( $$minor + 1 )); \
	  echo "$$major.$$minor.0"))
else
ifeq ($(strip $(VERSION)),MAJOR)
	$(eval NEW_VERSION = $(shell \
	  version=`ls -l $(CHECKDIR)/$(CHECK_TARGET) |awk '{print $$11}' |sed "s|$(CHECKDIR)/||" |sed "s|$(CHECK_TARGET).||"`; \
	  major=`echo $$version |awk -F. '{print $$1}'`; \
	  major=$$(( $$major + 1 )); \
	  echo "$$major.0.0"))
else
ifneq ($(strip $(VERSION)),)
	$(eval NEW_VERSION = $(VERSION))
else
	$(error missing version number (VERSION=))
endif
endif
endif
endif
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
