ifeq ($(findstring rda-web-dev,$(HOST)),)
OPTIONS = -Wall -Wold-style-cast -O3 -std=c++11 -Weffc++
C_OPTIONS = -c -fPIC $(OPTIONS)
GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
INCLUDEDIR = ./include
SOURCEDIR = ./src
OKAYTOMAKE = 0
DAVMACH = 0
ifneq ($(or $(findstring geyser,$(HOST)),$(findstring caldera,$(HOST)),$(findstring pronghorn,$(HOST)),$(findstring yslogin,$(HOST)),$(findstring casper,$(HOST))),)
	DAVMACH = 1
	BUILDEXT = dav
	COMPILER = /glade/u/apps/dav/opt/ncarcompilers/0.4.1/g++
	GCCVERSION = $(shell g++ --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.3.0
	MYSQLINCLUDEDIR = /usr/include/mysql
	LIBDIR = /glade/u/home/rdadata/lib/dav
	MYSQLLIBDIR = /usr/lib64/mysql
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	ZLIBDIR = /glade/u/home/rdadata/lib/dav/lib
	BINDIR = /glade/u/home/rdadata/bin/dav
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAYTOMAKE = 1
endif
CHEYENNE = 0
ifneq ($(findstring cheyenne,$(HOST)),)
	CHEYENNE = 1
	BUILDEXT = ch
	COMPILER = /glade/u/apps/ch/opt/gnu/7.1.0/bin/g++
	GCCVERSION = $(shell g++ --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.1.0
	MYSQLINCLUDEDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/include
	LIBDIR = /glade/u/home/rdadata/lib/ch
	MYSQLLIBDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/lib
	JASPERLIBDIR = /glade/u/home/dattore/cpp/lib/jasper/lib
	ZLIBDIR = /usr/lib64
	BINDIR = /glade/u/home/rdadata/bin/ch
	LOCALBINDIR = /ncar/rda/setuid/bin
	RUNPATH = -Wl,-rpath,/glade/u/apps/ch/opt/gnu/$(EXPECTEDGCCVERSION)/lib64 -Wl,-rpath,$(LIBDIR)
	OKAYTOMAKE = 1
endif
VMMACH = 0
ifneq ($(or $(findstring rda-data,$(HOST)),$(findstring rda-web-prod,$(HOST))),)
	VMMACH = 1
ifneq ($(findstring rda-data,$(HOST)),)
	BUILDEXT = vm-data
else ifneq ($(findstring rda-web-prod,$(HOST)),)
	BUILDEXT = vm-web-prod
endif
	COMPILER = g++49
        MYSQLINCLUDEDIR = /usr/include/mysql
        LIBDIR = /usr/local/dss/lib
        MYSQLLIBDIR = /usr/lib64/mysql
	JASPERLIBDIR = /usr/lib64
	ZLIBDIR = /usr/lib64
	BINDIR = /usr/local/dss/bin
	LOCALBINDIR = $(BINDIR)
	RUNPATH = -Wl,-rpath,$(LIBDIR)
	OKAYTOMAKE = 1
endif
MYSQLRUNPATH = -Wl,-rpath,$(MYSQLLIBDIR)
JASPERRUNPATH = -Wl,-rpath,$(JASPERLIBDIR)
ZRUNPATH = -Wl,-rpath,$(ZLIBDIR)
EXECUTABLE =
BUILDDIR = ./build-$(BUILDEXT)
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
	$(COMPILER) $(C_OPTIONS) $< -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -o $@
libgatherxml.so: builddir $(GATHERXMLOBJS) $(INCLUDEDIR)/gatherxml.hpp
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(LIBVERSION)),)
	$(error libgatherxml.so: no version number given)
else
	sudo -u rdadata $(COMPILER) -shared -o $(LIBDIR)/libgatherxml.so.$(LIBVERSION) -Wl,-soname,libgatherxml.so.$(LIBVERSION) $(GATHERXMLOBJS)
	sudo -u rdadata rm -f $(LIBDIR)/libgatherxml.so
	sudo -u rdadata ln -s $(LIBDIR)/libgatherxml.so.$(LIBVERSION) $(LIBDIR)/libgatherxml.so
endif
endif
#
_ascii2xml: $(SOURCEDIR)/_ascii2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lobs -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_bufr2xml: $(SOURCEDIR)/_bufr2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbufr -lobs -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
cmd_util: $(SOURCEDIR)/cmd_util.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -o $(BUILDDIR)/$@.$(VERSION)
	sudo -u rdadata cp $(BUILDDIR)/$@.$(VERSION) $(LOCALBINDIR)/$@.$(VERSION)
	sudo -u rdadata chmod 4710 $(LOCALBINDIR)/$@.$(VERSION)
	rm -f $(LOCALBINDIR)/$@
	ln -s $(LOCALBINDIR)/$@.$(VERSION) $(LOCALBINDIR)/$@
endif
endif
#
_dcm: $(SOURCEDIR)/_dcm.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_dsgen: $(SOURCEDIR)/_dsgen.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lmetaexport -lmetaexporthelpers -lgridutils -lsearch -lxml -lbitmap -lcitation -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_fix2xml: $(SOURCEDIR)/_fix2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	($error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lcyclone -lutils -lutilsthread -ldatetime -lio -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_gatherxml: $(SOURCEDIR)/_gatherxml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_grid2xml: $(SOURCEDIR)/_grid2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lgrids -ljasper -lutils -lutilsthread -ldatetime -lbitmap -lio -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_gsi: $(SOURCEDIR)/_gsi.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_hdf2xml: $(SOURCEDIR)/_hdf2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lbitmap -lhdf -lmetautils -lmetahelpers -lgridutils -lxml -lsearch -lpthread -lz -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_iinv: $(SOURCEDIR)/_iinv.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -lpthread -lz -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_nc2xml: $(SOURCEDIR)/_nc2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbufr -lbitmap -lerror -lpthread -lz -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_obs2xml: $(SOURCEDIR)/_obs2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lobs -lutils -lutilsthread -ldatetime -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lgridutils -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_prop2xml: $(SOURCEDIR)/_prop2xml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/_prop2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/_prop2xml.$(VERSION)
endif
endif
#
_rcm: $(SOURCEDIR)/_rcm.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lmetautils -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_scm: $(SOURCEDIR)/_scm.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -ldatetime -lgridutils -lmetautils -lmetahelpers -lsearch -lxml -lbitmap -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_sdp: $(SOURCEDIR)/_sdp.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetadata -lmetahelpers -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
_sml: $(SOURCEDIR)/_sml.cpp builddir
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(VERSION)),)
	$(error no version number given)
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SOURCEDIR)/$@.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lutils -lutilsthread -ldatetime -lgridutils -lbitmap -lmetautils -lmetahelpers -lsearch -lxml -lz -lpthread -o $(BUILDDIR)/$@.$(VERSION)
endif
endif
#
builddir:
	mkdir -p $(BUILDDIR)/libgatherxml
#
clean:
	rm -rf $(BUILDDIR)
#
install:
ifneq ($(strip $(EXECUTABLE)),)
ifneq ($(strip $(VERSION)),)
	sudo -u rdadata cp ./$(BUILDDIR)/$(EXECUTABLE).$(VERSION) $(BINDIR)/$(EXECUTABLE).$(VERSION)
	sudo -u rdadata chmod 740 $(BINDIR)/$(EXECUTABLE).$(VERSION)
else
	$(error executable requires a version number (VERSION=))
endif
ifeq ($(DAVMACH),1)
ifneq ($(strip $(VERSION)),)
	sudo -u rdadata rm -f $(BINDIR)/$(EXECUTABLE)
	sudo -u rdadata ln -s $(BINDIR)/$(EXECUTABLE).$(VERSION) $(BINDIR)/$(EXECUTABLE)
endif
endif
ifeq ($(CHEYENNE),1)
ifneq ($(strip $(VERSION)),)
	sudo -u rdadata rm -f $(BINDIR)/$(EXECUTABLE)
	sudo -u rdadata ln -s $(BINDIR)/$(EXECUTABLE).$(VERSION) $(BINDIR)/$(EXECUTABLE)
endif
endif
ifeq ($(VMMACH),1)
ifneq ($(strip $(VERSION)),)
	rm -f $(BINDIR)/$(EXECUTABLE)
	ln -s $(BINDIR)/$(EXECUTABLE).$(VERSION) $(BINDIR)/$(EXECUTABLE)
endif
endif
else ifneq ($(strip $(TEMPLATE)),)
	sudo -u rdadata cp ./templates/$(TEMPLATE) /glade/u/home/rdadata/share/templates/
ifeq ($(VMMACH),1)
	sudo -u rdadata mkdir -p /usr/local/dss/share/templates
	sudo -u rdadata cp ./templates/$(TEMPLATE) /usr/local/dss/share/templates/
endif
else
	$(error Nothing was specified to install. Use EXECUTABLE= or TEMPLATE=)
endif
else
%::
	-@ echo "operational software must be built on operational machines"
endif
