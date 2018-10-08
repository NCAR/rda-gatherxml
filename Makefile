ifeq ($(findstring rda-web-dev,$(HOST)),)
OPTIONS = -Wall -Wold-style-cast -O3 -std=c++11 -Weffc++
C_OPTIONS = -c -fPIC $(OPTIONS)
GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
INCLUDEDIR = ./include
SRCDIR = ./src
OKAYTOMAKE = 0
DAVMACH = 0
ifneq ($(findstring geyser,$(HOST)),)
	DAVMACH = 1
	EXT = dav-g
endif
ifneq ($(findstring caldera,$(HOST)),)
	DAVMACH = 1
	EXT = dav-c
endif
ifneq ($(findstring pronghorn,$(HOST)),)
	DAVMACH = 1
	EXT = dav-p
endif
ifneq ($(findstring yslogin,$(HOST)),)
	DAVMACH = 1
	EXT = dav-ys
endif
ifneq ($(findstring casper,$(HOST)),)
	DAVMACH = 1
	EXT = dav-cp
endif
ifeq ($(DAVMACH),1)
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
	EXT = ch
endif
ifeq ($(CHEYENNE),1)
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
ifneq ($(findstring rda-data,$(HOST)),)
	VMMACH = 1
	EXT = vm-data
endif
ifneq ($(findstring rda-web-prod,$(HOST)),)
	VMMACH = 1
	EXT = vm-prod
endif
ifneq ($(findstring rda-web-dev,$(HOST)),)
	VMMACH = 1
	EXT = vm-dev
endif
ifeq ($(VMMACH),1)
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
SYNC =
#
GATHERXMLSUBS = $(SRCDIR)/libgatherxml/detailed_metadata.cpp $(SRCDIR)/libgatherxml/fix_metadata.cpp $(SRCDIR)/libgatherxml/grid_metadata.cpp $(SRCDIR)/libgatherxml/obs_metadata.cpp $(SRCDIR)/libgatherxml/summarize_metadata.cpp $(SRCDIR)/libgatherxml/write_metadata.cpp
GATHERXMLOBJS = detailed_metadata.o fix_metadata.o grid_metadata.o obs_metadata.o summarize_metadata.o write_metadata.o
#
.PHONY: finalize
#
all: _ascii2xml _bufr2xml _dcm _dsgen _fix2xml _gatherxml _grid2xml _gsi _hdf2xml _iinv _nc2xml _obs2xml _prop2xml _rcm _scm _sdp _sml
#
obj_gatherxml: $(GATHERXMLSUBS)
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(C_OPTIONS) $(GATHERXMLSUBS) -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR)
endif
libgatherxml.so: $(GATHERXMLOBJS)
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
_ascii2xml: $(SRCDIR)/_ascii2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_ascii2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lobs -lutils -lutilsthread -lbitmap -lio -liometadata -lmetadata -lmetahelpers -lgridutils -lsearch -lxml -lz -lpthread -o _ascii2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_ascii2xml.$(EXT).$(VERSION) $(BINDIR)/_ascii2xml.$(VERSION)
	rm _ascii2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_bufr2xml: $(SRCDIR)/_bufr2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_bufr2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -lmetadata -lmetahelpers -lbufr -lobs -lbitmap -liometadata -lgridutils -lsearch -lxml -lz -lpthread -o _bufr2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_bufr2xml.$(EXT).$(VERSION) $(BINDIR)/_bufr2xml.$(VERSION)
	rm ./_bufr2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
cmd_util: $(SRCDIR)/cmd_util.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SRCDIR)/cmd_util.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -o cmd_util.$(VERSION)
	sudo -u rdadata cp ./cmd_util.$(VERSION) $(LOCALBINDIR)/
	rm cmd_util.$(VERSION)
	sudo -u rdadata chmod 4710 $(LOCALBINDIR)/cmd_util.$(VERSION)
	rm -f $(LOCALBINDIR)/cmd_util
	ln -s $(LOCALBINDIR)/cmd_util.$(VERSION) $(LOCALBINDIR)/cmd_util
endif
#
_dcm: $(SRCDIR)/_dcm.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_dcm.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o ./_dcm.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_dcm.$(EXT).$(VERSION) $(BINDIR)/_dcm.$(VERSION)
	rm _dcm.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_dsgen: $(SRCDIR)/_dsgen.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_dsgen.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lmetaexport -lmetaexporthelpers -lgridutils -lsearch -lxml -lbitmap -lcitation -lz -lpthread -o ./_dsgen.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_dsgen.$(EXT).$(VERSION) $(BINDIR)/_dsgen.$(VERSION)
	rm ./_dsgen.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_fix2xml: $(SRCDIR)/_fix2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SRCDIR)/_fix2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lcyclone -lutils -lutilsthread -lio -liometadata -lmetadata -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o ./_fix2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_fix2xml.$(EXT).$(VERSION) $(BINDIR)/_fix2xml.$(VERSION)
	rm ./_fix2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_gatherxml: $(SRCDIR)/_gatherxml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_gatherxml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lgridutils -lbitmap -lsearch -lxml -lz -lpthread -o ./_gatherxml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_gatherxml.$(EXT).$(VERSION) $(BINDIR)/_gatherxml.$(VERSION)
	rm ./_gatherxml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_grid2xml: $(SRCDIR)/_grid2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SRCDIR)/_grid2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lgrids -ljasper -lutils -lutilsthread -lbitmap -lio -liometadata -lmetadata -lmetahelpers -lgridutils -lsearch -lxml -lerror -lpthread -lz -o ./_grid2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_grid2xml.$(EXT).$(VERSION) $(BINDIR)/_grid2xml.$(VERSION)
	rm ./_grid2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_gsi: $(SRCDIR)/_gsi.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_gsi.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lgridutils -lbitmap -lxml -lsearch -lpthread -lz -o ./_gsi.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_gsi.$(EXT).$(VERSION) $(BINDIR)/_gsi.$(VERSION)
	rm ./_gsi.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_hdf2xml: $(SRCDIR)/_hdf2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_hdf2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -lbitmap -lhdf -liometadata -lmetadata -lmetahelpers -lgridutils -lxml -lsearch -lpthread -lz -o ./_hdf2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_hdf2xml.$(EXT).$(VERSION) $(BINDIR)/_hdf2xml.$(VERSION)
	rm ./_hdf2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_iinv: $(SRCDIR)/_iinv.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SRCDIR)/_iinv.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lbitmap -lmetadata -lmetahelpers -lsearch -lxml -lgridutils -lpthread -lz -o _iinv.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_iinv.$(EXT).$(VERSION) $(BINDIR)/_iinv.$(VERSION)
	rm _iinv.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_nc2xml: $(SRCDIR)/_nc2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SRCDIR)/_nc2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lutils -lutilsthread -liometadata -lmetadata -lmetahelpers -lgridutils -lsearch -lxml -lbufr -lbitmap -lerror -lpthread -lz -o ./_nc2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_nc2xml.$(EXT).$(VERSION) $(BINDIR)/_nc2xml.$(VERSION)
	rm ./_nc2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_obs2xml: $(SRCDIR)/_obs2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(JASPERRUNPATH) $(ZRUNPATH) $(SRCDIR)/_obs2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(JASPERLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lio -lobs -lutils -lutilsthread -lbitmap -liometadata -lmetadata -lmetahelpers -lsearch -lxml -lgridutils -lz -lpthread -o ./_obs2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_obs2xml.$(EXT).$(VERSION) $(BINDIR)/_obs2xml.$(VERSION)
	rm ./_obs2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_prop2xml: $(SRCDIR)/_prop2xml.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_prop2xml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lbitmap -lgridutils -lsearch -lxml -lz -lpthread -o ./_prop2xml.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_prop2xml.$(EXT).$(VERSION) $(BINDIR)/_prop2xml.$(VERSION)
	rm ./_prop2xml.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_rcm: $(SRCDIR)/_rcm.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_rcm.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lmetadata -lmetahelpers -lgridutils -lsearch -lxml -lbitmap -lz -lpthread -o ./_rcm.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_rcm.$(EXT).$(VERSION) $(BINDIR)/_rcm.$(VERSION)
	rm ./_rcm.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_scm: $(SRCDIR)/_scm.cpp
ifeq ($(strip $(VERSION)),)
	-@ echo "no version number given"
else
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(ZRUNPATH) $(SRCDIR)/_scm.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -L$(ZLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lgridutils -lmetadata -lmetahelpers -lsearch -lxml -lbitmap -lz -lpthread -o _scm.$(EXT).$(VERSION)
	sudo -u rdadata cp ./_scm.$(EXT).$(VERSION) $(BINDIR)/_scm.$(VERSION)
	rm _scm.$(EXT).$(VERSION)
	make SYNC=$@ finalize
endif
endif
#
_sdp: $(SRCDIR)/_sdp.cpp
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SRCDIR)/_sdp.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lgridutils -lbitmap -lmetadata -lmetahelpers -lsearch -lxml -lz -lpthread -o ./_sdp
	sudo -u rdadata cp ./_sdp $(BINDIR)
	rm ./_sdp
endif
#
_sml: $(SRCDIR)/_sml.cpp
ifeq ($(OKAYTOMAKE),1)
	$(COMPILER) $(OPTIONS) $(RUNPATH) $(MYSQLRUNPATH) $(SRCDIR)/_sml.cpp -I$(INCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR) -L$(LIBDIR) -L$(MYSQLLIBDIR) -lmysql -lmysqlclient -lgatherxml -lutils -lutilsthread -lgridutils -lbitmap -lmetadata -lmetahelpers -lsearch -lxml -lz -lpthread -o ./_sml
	sudo -u rdadata cp ./_sml $(BINDIR)/
	rm ./_sml
endif
#
finalize:
ifeq ($(strip $(VERSION)),)
	sudo -u rdadata chmod 740 $(BINDIR)/$(SYNC)
else
	sudo -u rdadata chmod 740 $(BINDIR)/$(SYNC).$(VERSION)
endif
ifeq ($(DAVMACH),1)
ifneq ($(strip $(VERSION)),)
	sudo -u rdadata rm -f $(BINDIR)/$(SYNC)
	sudo -u rdadata ln -s $(BINDIR)/$(SYNC).$(VERSION) $(BINDIR)/$(SYNC)
endif
endif
ifeq ($(CHEYENNE),1)
ifneq ($(strip $(VERSION)),)
	sudo -u rdadata rm -f $(BINDIR)/$(SYNC)
	sudo -u rdadata ln -s $(BINDIR)/$(SYNC).$(VERSION) $(BINDIR)/$(SYNC)
endif
endif
ifeq ($(VMMACH),1)
ifneq ($(strip $(VERSION)),)
	rm -f $(BINDIR)/$(SYNC)
	ln -s $(BINDIR)/$(SYNC).$(VERSION) $(BINDIR)/$(SYNC)
endif
endif
else
%::
	-@ echo "operational software must be built on operational machines"
endif
