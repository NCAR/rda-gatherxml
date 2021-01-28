HOST = $(shell hostname)
#
# Compiler options
#
COMPILE_OPTIONS = -Wall -Wold-style-cast -O3 -std=c++14 -Weffc++
#
# Global include directory
#
GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
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
DAVMACH = 0
ifneq ($(or $(findstring casper,$(HOST))),)
# Settings for the DAV nodes
	DAVMACH = 1
	BUILDEXT = dav
	COMPILER = /glade/u/apps/dav/opt/ncarcompilers/0.4.1/g++
	GCCVERSION = $(shell $(COMPILER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.3.0
	MYSQLINCLUDEDIR = /usr/include/mysql
	LIBDIR = /glade/u/home/rdadata/lib/dav
	MYSQLLIBDIR = /usr/lib64/mysql
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	ZLIBDIR = /glade/u/home/rdadata/lib/dav/lib
	BINDIR = /glade/u/home/rdadata/bin/dav
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /ncar/rda/setuid/bin/rdadatarun
endif
CHEYENNE = 0
ifneq ($(or $(findstring cheyenne,$(HOST)), $(findstring chadmin,$(HOST))),)
# Cheyenne settings
	CHEYENNE = 1
	BUILDEXT = ch
	COMPILER = /glade/u/apps/ch/opt/gnu/7.1.0/bin/g++
	GCCVERSION = $(shell $(COMPILLER) --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.1.0
	MYSQLINCLUDEDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/include
	LIBDIR = /glade/u/home/rdadata/lib/ch
	MYSQLLIBDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/lib
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	ZLIBDIR = /usr/lib64
	BINDIR = /glade/u/home/rdadata/bin/ch
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/ch/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /ncar/rda/setuid/bin/rdadatarun
endif
VMMACH = 0
ifneq ($(or $(findstring rda-data,$(HOST)),$(findstring rda-web-test,$(HOST)),$(findstring rda-web-dev,$(HOST))),)
# Settings for the VM machines
	VMMACH = 1
ifneq ($(findstring rda-data,$(HOST)),)
	BUILDEXT = vm-data
else ifneq ($(findstring rda-web-test,$(HOST)),)
	BUILDEXT = vm-web-test
else ifneq ($(findstring rda-web-dev,$(HOST)),)
	BUILDEXT = vm-web-dev
endif
	COMPILER = g++49
        MYSQLINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/local/decs/lib
        MYSQLLIBDIR = /usr/lib64/mysql
	JASPERLIBDIR = /usr/lib64
	ZLIBDIR = /usr/lib64
	BINDIR = /usr/local/decs/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAY_TO_MAKE = 1
	RDADATARUN = /usr/local/decs/bin/rdadatarun
endif
#
# If not a "make-able" host, exit with an error message
#
ifeq ($(OKAY_TO_MAKE),0)
%: WRONG-HOST
	$(error Unable to make on $(HOST))
WRONG-HOST: ;
endif
#
# Run-path settings
MYSQLRUNPATH = -Wl,-rpath,$(MYSQLLIBDIR)
JASPERRUNPATH = -Wl,-rpath,$(JASPERLIBDIR)
ZRUNPATH = -Wl,-rpath,$(ZLIBDIR)
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
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lobs -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_bufr2xml: CHECKDIR=$(BINDIR)
_bufr2xml: CHECK_TARGET=_bufr2xml
_bufr2xml: $(SOURCEDIR)/_bufr2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbufr -lobs -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbufr -lobs -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
cmd_util: $(SOURCEDIR)/cmd_util.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -o $(BUILDDIR)/$@.$(VERSION)
	rdadatarun cp $(BUILDDIR)/$@.$(VERSION) $(LOCALBINDIR)/$@.$(VERSION)
	rdadatarun chmod 4710 $(LOCALBINDIR)/$@.$(VERSION)
	ln -s -f $(LOCALBINDIR)/$@.$(VERSION) $(LOCALBINDIR)/$@
endif
endif
#
_dcm: CHECKDIR=$(BINDIR)
_dcm: CHECK_TARGET=_dcm
_dcm: $(SOURCEDIR)/_dcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_dsgen: CHECKDIR=$(BINDIR)
_dsgen: CHECK_TARGET=_dsgen
_dsgen: $(SOURCEDIR)/_dsgen.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lmetaexport -lmetaexporthelpers -lgridutils -lsearch -lxml -lbitmap -lcitation -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lmetaexport -lmetaexporthelpers -lgridutils -lsearch -lxml -lbitmap -lcitation -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
ifneq ($(findstring rda-web-test,$(HOST)),)
	$(RDADATARUN) rsync -a $(BINDIR)/$@.$(NEW_VERSION) rdadata@rda-web-prod.ucar.edu:$(BINDIR)/
	$(RDADATARUN) rsync -a $(BINDIR)/$@ rdadata@rda-web-prod.ucar.edu:$(BINDIR)/
endif
endif
#
_fix2xml: $(SOURCEDIR)/_fix2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	($error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lcyclone -lutils -lutilsthread -ldatetime -lio -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_gatherxml: CHECKDIR=$(BINDIR)
_gatherxml: CHECK_TARGET=_gatherxml
_gatherxml: $(SOURCEDIR)/_gatherxml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_grid2xml: CHECKDIR=$(BINDIR)
_grid2xml: CHECK_TARGET=_grid2xml
_grid2xml: $(SOURCEDIR)/_grid2xml.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lgrids -ljasper -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -D__JASPER -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lgrids -ljasper -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_gsi: CHECKDIR=$(BINDIR)
_gsi: CHECK_TARGET=_gsi
_gsi: $(SOURCEDIR)/_gsi.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_hdf2xml: $(SOURCEDIR)/_hdf2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
ifeq ($(strip $(VERSION)),BUG)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; minor=`echo $$version |awk -F. '{print $$2}'`; bug=`echo $$version |awk -F. '{print $$3}'`; bug=$$(( $$bug + 1 )); echo "$$major.$$minor.$$bug"))
	$(eval OKAYTOBUILD=1)
else
ifeq ($(strip $(VERSION)),MINOR)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; minor=`echo $$version |awk -F. '{print $$2}'`; minor=$$(( $$minor + 1 )); echo "$$major.$$minor.0"))
	$(eval OKAYTOBUILD=1)
else
ifeq ($(strip $(VERSION)),MAJOR)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; major=$$(( $$major + 1 )); echo "$$major.0.0"))
	$(eval OKAYTOBUILD=1)
else
	$(eval THISVERSION = $(VERSION))
	$(eval OKAYTOBUILD=0)
endif
endif
endif
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lbitmap -lhdf -lmetautils -lmetahelpers -lgridutils -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(THISVERSION)
	@ if [ $(OKAYTOBUILD) -eq 1 ]; then \
	  make install VERSION=$(THISVERSION) EXECUTABLE=$@; \
	fi;
endif
endif
#
_iinv: CHECKDIR=$(BINDIR)
_iinv: CHECK_TARGET=_iinv
_iinv: $(SOURCEDIR)/_iinv.cpp builddir get_version
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_nc2xml: CHECKDIR=$(BINDIR)
_nc2xml: CHECK_TARGET=_nc2xml
_nc2xml: $(SOURCEDIR)/_nc2xml.cpp builddir get_version
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbufr -lbitmap -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbufr -lbitmap -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
#
_obs2xml: $(SOURCEDIR)/_obs2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
ifeq ($(strip $(VERSION)),BUG)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; minor=`echo $$version |awk -F. '{print $$2}'`; bug=`echo $$version |awk -F. '{print $$3}'`; bug=$$(( $$bug + 1 )); echo "$$major.$$minor.$$bug"))
	$(eval OKAYTOBUILD=1)
else
ifeq ($(strip $(VERSION)),MINOR)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; minor=`echo $$version |awk -F. '{print $$2}'`; minor=$$(( $$minor + 1 )); echo "$$major.$$minor.0"))
	$(eval OKAYTOBUILD=1)
else
ifeq ($(strip $(VERSION)),MAJOR)
	$(eval THISVERSION = $(shell version=`ls -l $(BINDIR)/$@ |awk '{print $$11}' |sed "s|$(BINDIR)/$@.||"`; major=`echo $$version |awk -F. '{print $$1}'`; major=$$(( $$major + 1 )); echo "$$major.0.0"))
	$(eval OKAYTOBUILD=1)
else
	$(eval THISVERSION = $(VERSION))
	$(eval OKAYTOBUILD=0)
endif
endif
endif
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lobs -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -lz -lpthread -o $(BUILDDIR)/$@.$(THISVERSION)
	if [ $(OKAYTOBUILD) -eq 1 ]; then \
	  make install VERSION=$(THISVERSION) EXECUTABLE=$@; \
	fi;
endif
endif
#
_prop2xml: $(SOURCEDIR)/_prop2xml.cpp builddir
ifeq ($(OKAY_TO_MAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/_prop2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/_prop2xml.$(VERSION)
endif
endif
#
_rcm: CHECKDIR=$(BINDIR)
_rcm: CHECK_TARGET=_rcm
_rcm: $(SOURCEDIR)/_rcm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
endif
	$(MAKE) NEW_VERSION=$(NEW_VERSION) TARGET=$@ INSTALLDIR=$(BINDIR) install
endif
#
_scm: CHECKDIR=$(BINDIR)
_scm: CHECK_TARGET=_scm
_scm: $(SOURCEDIR)/_scm.cpp builddir get_version
ifeq ($(OKAY_TO_MAKE),1)
ifneq ($(or $(findstring BUG,$(VERSION)),$(findstring MINOR,$(VERSION)),$(findstring MAJOR,$(VERSION))),)
	$(COMPILER) $(COMPILE_OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lgridutils -lmetautils -lmetahelpers -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
else
	$(COMPILER) $(COMPILE_OPTIONS) -Wl,-rpath,$(BUILDDIR) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(BUILDDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lgridutils -lmetautils -lmetahelpers -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(NEW_VERSION)
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
	cd $(BUILDDIR) \
	&& ln -s -f $(TARGET).$(NEW_VERSION) $(TARGET)
endif
else
	$(error missing installation directory (INSTALLDIR=))
endif
#
# Remove the build directory
#
clean:
	rm -rf $(BUILDDIR)
