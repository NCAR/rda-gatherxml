ifeq ($(findstring rda-web-dev,$(HOST)),)
CC_OPTIONS = -Wall -Wold-style-cast -c -O3 -fPIC -std=c++11 -Weffc++
LOCALINCLUDEDIR = ../include
GLOBALINCLUDEDIR = /glade/u/home/dattore/cpp/lib/include
JASPERINCLUDEDIR = /glade/u/home/rdadata/gnu/include
SRCDIR = .
OKAYTOMAKE = 0
DAVMACH = 0
ifneq ($(findstring geyser,$(HOST)),)
	DAVMACH = 1
endif
ifneq ($(findstring caldera,$(HOST)),)
	DAVMACH = 1
endif
ifneq ($(findstring pronghorn,$(HOST)),)
	DAVMACH = 1
endif
ifneq ($(findstring yslogin,$(HOST)),)
	DAVMACH = 1
endif
ifneq ($(findstring casper,$(HOST)),)
	DAVMACH = 1
endif
ifeq ($(DAVMACH),1)
	CC_COMPILER = /glade/u/apps/dav/opt/ncarcompilers/0.4.1/g++
	GCCVERSION = $(shell g++ --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.3.0
	MYSQLINCLUDEDIR = /usr/include/mysql
	DSSLIBDIR = /glade/u/home/rdadata/lib/dav
	OKAYTOMAKE = 1
endif
CHEYENNE = 0
ifneq ($(findstring cheyenne,$(HOST)),)
	CHEYENNE = 1
endif
ifeq ($(CHEYENNE),1)
	CC_COMPILER = /glade/u/apps/ch/opt/gnu/7.1.0/bin/g++
	GCCVERSION = $(shell g++ --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = 7.1.0
	MYSQLINCLUDEDIR = /glade/u/apps/ch/opt/mysql/5.7.19/gnu/4.8.5/include
	DSSLIBDIR = /glade/u/home/rdadata/lib/ch
	OKAYTOMAKE = 1
endif
RDA_DATA = 0
ifneq ($(findstring rda-data,$(HOST)),)
	RDA_DATA = 1
endif
ifeq ($(RDA_DATA),1)
	CC_COMPILER = g++49
	GCCVERSION = $(shell g++ --version |grep "^g++" |awk '{print $$3}')
	EXPECTEDGCCVERSION = $(GCCVERSION)
	MYSQLINCLUDEDIR = /glade/u/home/rdadata/mysql/include
	DSSLIBDIR = /glade/u/home/rdadata/lib/dsg_mach
	OKAYTOMAKE = 1
endif
LIBVERSION =
#
SUBS = $(SRCDIR)/detailed_metadata.cpp $(SRCDIR)/write_metadata.cpp
OBJS = detailed_metadata.o write_metadata.o
#
all: obj_gatherxml libgatherxml.so
#
obj_gatherxml: $(SUBS)
ifeq ($(OKAYTOMAKE),1)
ifneq ($(GCCVERSION),$(EXPECTEDGCCVERSION))
	$(error obj_gatherxml: can't compile with g++ version $(GCCVERSION))
else
	$(CC_COMPILER) $(CC_OPTIONS) $(SUBS) -I$(LOCALINCLUDEDIR) -I$(GLOBALINCLUDEDIR) -I$(MYSQLINCLUDEDIR)
endif
else
	$(error obj_gatherxml: can't make on $(HOST))
endif
libgatherxml.so: $(OBJS)
ifeq ($(OKAYTOMAKE),1)
ifeq ($(strip $(LIBVERSION)),)
	$(error libgatherxml.so: no version number given)
else
ifneq ($(GCCVERSION),$(EXPECTEDGCCVERSION))
	$(error libgatherxml.so: can't compile with g++ version $(GCCVERSION))
else
	sudo -u rdadata $(CC_COMPILER) -shared -o $(DSSLIBDIR)/libgatherxml.so.$(LIBVERSION) -Wl,-soname,libgatherxml.so.$(LIBVERSION) $(OBJS)
	sudo -u rdadata rm $(DSSLIBDIR)/libgatherxml.so
	sudo -u rdadata ln -s $(DSSLIBDIR)/libxml.so.$(LIBVERSION) $(DSSLIBDIR)/libgatherxml.so
endif
endif
else
	$(error libgatherxml.so: can't make on $(HOST))
endif
else
%::
	$(error operational libraries must be built on operational machines)
endif
