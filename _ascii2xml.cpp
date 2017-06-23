#include <iostream>
#include <fstream>
#include <signal.h>
#include <sstream>
#include <regex>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
std::string user=getenv("USER");
TempFile *tfile=NULL;
TempDir *tdir=NULL;
my::map<metadata::ObML::IDEntry> **idTable;
my::map<metadata::ObML::PlatformEntry> platformTable[metadata::ObML::NUM_OBS_TYPES];
size_t num_not_missing=0;
std::string cmd_type="";
std::string myerror="";

extern "C" void cleanUp()
{
  if (tfile != NULL)
    delete tfile;
  if (tdir != NULL)
    delete tdir;
  if (myerror.length() > 0) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::logError(myerror,"ascii2xml",user,args.argsString);
  }
}

extern "C" void segv_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
  metautils::logError("core dump","ascii2xml",user,args.argsString);
}

extern "C" void int_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
}

void parseArgs()
{
  std::deque<std::string> sp;
  size_t n;
  int idx;

  args.overridePrimaryCheck=false;
  args.updateDB=true;
  args.updateSummary=true;
  args.regenerate=true;
  sp=strutils::split(args.argsString,"!");
  for (n=0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	args.member_name=sp[++n];
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    args.format=strutils::to_lower(args.format);
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.overridePrimaryCheck=true;
    args.updateDB=false;
    args.updateSummary=false;
    args.regenerate=false;
  }
  args.path=sp.back();
  idx=args.path.rfind("/");
  args.filename=args.path.substr(idx+1);
  args.path=args.path.substr(0,idx);
}

struct InvEntry {
  InvEntry() : key(),lat(0.),lon(0.),plat_type(0) {}

  std::string key;
  float lat,lon;
  short plat_type;
};

void addGHCNV3ID(metadata::ObML::IDEntry& ientry,InvEntry& ie,short year,short month)
{
  size_t n,m;
  DateTime dt_start,dt_end;

  dt_start.set(year,month,1,0);
  dt_end.set(year,month,getDaysInMonth(year,month),235959);
  if (!idTable[1]->found(ientry.key,ientry)) {
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->S_lat=ientry.data->N_lat=ie.lat;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (m=0; m < 360; ++m) {
        ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=999.;
    }
    ientry.data->W_lon=ientry.data->E_lon=ie.lon;
    convertLatLonToBox(1,0.,ie.lon,n,m);
    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=ie.lon;
    ientry.data->start=dt_start;
    ientry.data->end=dt_end;
    ientry.data->nsteps=1;
    idTable[1]->insert(ientry);
  }
  else {
    if (ie.lat < ientry.data->S_lat) {
	ientry.data->S_lat=ie.lat;
    }
    if (ie.lat > ientry.data->N_lat) {
	ientry.data->N_lat=ie.lat;
    }
    if (ie.lon < ientry.data->W_lon) {
	ientry.data->W_lon=ie.lon;
    }
    if (ie.lon > ientry.data->E_lon) {
	ientry.data->E_lon=ie.lon;
    }
    convertLatLonToBox(1,0.,ie.lon,n,m);
    if (ientry.data->min_lon_bitmap[m] > 900.) {
	ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=ie.lon;
    }
    else { 
	if (ie.lon < ientry.data->min_lon_bitmap[m]) {
	  ientry.data->min_lon_bitmap[m]=ie.lon;
	}
	if (ie.lon > ientry.data->max_lon_bitmap[m]) {
	  ientry.data->max_lon_bitmap[m]=ie.lon;
	}
    }
    if (dt_start < ientry.data->start) {
	ientry.data->start=dt_start;
    }
    if (dt_end > ientry.data->end) {
	ientry.data->end=dt_end;
    }
    ++(ientry.data->nsteps);
  }
}

void addGHCNV3Platform(metadata::ObML::PlatformEntry& pentry,InvEntry& ie)
{
  size_t n,m;

  if (!platformTable[1].found(pentry.key,pentry)) {
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    if (ie.lat == -90.) {
	pentry.boxflags->spole=1;
    }
    else if (ie.lat == 90.) {
	pentry.boxflags->npole=1;
    }
    else {
        convertLatLonToBox(1,ie.lat,ie.lon,n,m);
        pentry.boxflags->flags[n-1][m]=1;
        pentry.boxflags->flags[n-1][360]=1;
    }
    platformTable[1].insert(pentry);
  }
  else {
    if (ie.lat == -90.) {
        pentry.boxflags->spole=1;
    }
    else if (ie.lat == 90.) {
        pentry.boxflags->npole=1;
    }
    else {
        convertLatLonToBox(1,ie.lat,ie.lon,n,m);
        pentry.boxflags->flags[n-1][m]=1;
        pentry.boxflags->flags[n-1][360]=1;
    }
  }
}

void scanGHCNV3File(std::list<std::string>& filelist)
{
  std::ifstream ifs;
  char line[32768];
  size_t n;
  MySQL::Server server;
  MySQL::Query query;
  MySQL::Row row;
  my::map<InvEntry> inv_table(9999);
  InvEntry ie;
  std::string sdum,sdum2;
  TempDir tdir;
  metadata::ObML::IDEntry ientry;
  metadata::ObML::PlatformEntry pentry;
  metadata::ObML::DataTypeEntry de;
  int off;
  bool add_platform_entry;

  tdir.create("/glade/scratch/rdadata");
  idTable=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
  for (n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
    idTable[n]=new my::map<metadata::ObML::IDEntry>(9999);
  }
// load the station inventory
  metautils::connectToRDAServer(server);
  if (!server) {
    metautils::logError("scanGHCNV3File returned error: '"+server.error()+"' while trying to connect to RDADB","ascii2xml",user,args.argsString);
  }
  query.set("select wfile from dssdb.wfile where dsid = 'ds564.1' and type = 'O' and wfile like '%qcu.inv'");
  if (query.submit(server) < 0) {
    metautils::logError("scanGHCNV3File returned error: '"+query.error()+"' while trying to query for station inventory name","ascii2xml",user,args.argsString);
  }
  if (!query.fetch_row(row)) {
    metautils::logError("scanGHCNV3File unable to get station inventory name","ascii2xml",user,args.argsString);
  }
  sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds564.1/docs/"+row[0],tdir.name());
  ifs.open(sdum.c_str());
  if (!ifs.is_open()) {
    metautils::logError("scanGHCNV3File unable to open station inventory file","ascii2xml",user,args.argsString);
  }
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    ie.key.assign(line,11);
    sdum.assign(&line[12],8);
    ie.lat=std::stof(sdum);
    sdum.assign(&line[21],9);
    ie.lon=std::stof(sdum);
    if (ie.key.substr(0,3) < "800") {
	ie.plat_type=0;
    }
    else {
	ie.plat_type=1;
    }
    inv_table.insert(ie);
    ifs.getline(line,32768);
  }
  ifs.close();
  ifs.clear();
  for (auto& file : filelist) {
    ifs.open(file.c_str());
    if (!ifs.is_open()) {
	myerror="Error opening '"+file+"'";
	exit(1);
    }
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	ie.key.assign(line,11);
	inv_table.found(ie.key,ie);
	if (ie.plat_type == 0) {
	  pentry.key="land_station";
	}
	else {
	  pentry.key="fixed_ship";
	}
	ientry.key=pentry.key+"[!]GHCNV3[!]"+ie.key;
	sdum.assign(&line[11],4);
	off=19;
	add_platform_entry=false;
	for (n=0; n < 12; ++n) {
	  sdum2.assign(&line[off],5);
	  if (sdum2 != "-9999") {
	    addGHCNV3ID(ientry,ie,std::stoi(sdum),n+1);
	    de.key.assign(&line[15],4);
	    if (!ientry.data->data_types_table.found(de.key,de)) {
		de.data.reset(new metadata::ObML::DataTypeEntry::Data);
		de.data->nsteps=1;
		ientry.data->data_types_table.insert(de);
	    }
	    else {
		++(de.data->nsteps);
	    }
	    ++num_not_missing;
	    add_platform_entry=true;
	  }
	  off+=8;
	}
	if (add_platform_entry) {
	  addGHCNV3Platform(pentry,ie);
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
  }
  if (num_not_missing > 0) {
    args.format="ghcnmv3";
    metadata::ObML::writeObML(idTable,platformTable,"ascii2xml",user);
  }
  else {
    metautils::logError("all stations have missing location information - no usable data found; no content metadata will be saved for this file","ascii2xml",user,args.argsString);
  }
  for (n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
    delete idTable[n];
  }
  delete[] idTable;
}

void scanFile()
{
  std::string file_format,error;
  std::list<std::string> filelist;

  tfile=new TempFile;
  tfile->open(directives.tempPath);
  tdir=new TempDir;
  tdir->create(directives.tempPath);
  if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","ascii2xml",user,args.argsString);
  }
  if (filelist.size() == 0) {
    filelist.emplace_back(tfile->name());
  }
  if (args.format == "ghcnmv3") {
    scanGHCNV3File(filelist);
    cmd_type="ObML";
  }
}

int main(int argc,char **argv)
{
  std::string flags;

  if (argc < 4) {
    std::cerr << "usage: ascii2xml -f format -d [ds]nnn.n path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f ghcnmv3    GHCN Monthly V3 format" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>        full HPSS path or URL of the file to read" << std::endl;
    std::cerr << "              - HPSS paths must begin with \"/FS/DSS\" or \"/DSS\"" << std::endl;
    std::cerr << "              - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.argsString=getUnixArgsString(argc,argv,'!');
  metautils::readConfig("ascii2xml",user,args.argsString);
  parseArgs();
  atexit(cleanUp);
  metautils::cmd_register("ascii2xml",user);
  scanFile();
  if (args.updateDB) {
    if (cmd_type.length() == 0) {
	metautils::logError("content metadata type was not specified","ascii2xml",user,args.argsString);
    }
    flags="-f";
    if (std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
	flags="-wf";
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+"."+cmd_type,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
