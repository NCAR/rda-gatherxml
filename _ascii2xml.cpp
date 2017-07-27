#include <iostream>
#include <fstream>
#include <signal.h>
#include <sstream>
#include <regex>
#include <little_r.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
std::string user=getenv("USER");
TempFile *tfile=nullptr;
TempDir *tdir=nullptr;
my::map<metadata::ObML::PlatformEntry> platformTable[metadata::ObML::NUM_OBS_TYPES];
metadata::ObML::PlatformEntry pentry;
my::map<metadata::ObML::IDEntry> **ID_table=nullptr;
metadata::ObML::IDEntry ientry;
size_t total_num_not_missing=0;
enum {GrML_type=1,ObML_type};
int write_type=-1;
std::string myerror="";

extern "C" void cleanUp()
{
  if (tfile != nullptr) {
    delete tfile;
  }
  if (tdir != nullptr) {
    delete tdir;
  }
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

void initialize_for_observations()
{
  if (ID_table == nullptr) {
    ID_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
    for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
	ID_table[n]=new my::map<metadata::ObML::IDEntry>(9999);
    }
  }
}

struct InvEntry {
  InvEntry() : key(),lat(0.),lon(0.),plat_type(0) {}

  std::string key;
  float lat,lon;
  short plat_type;
};

void update_ID_table(size_t obs_type_index,float lat,float lon,DateTime *datetime,DateTime *min_datetime,DateTime *max_datetime)
{
  if (!ID_table[obs_type_index]->found(ientry.key,ientry)) {
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->S_lat=ientry.data->N_lat=lat;
    ientry.data->W_lon=ientry.data->E_lon=lon;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (size_t n=0; n < 360; ++n) {
        ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    size_t n,m;
    convertLatLonToBox(1,0.,lon,n,m);
    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=lon;
    if (min_datetime == nullptr) {
	ientry.data->start=*datetime;
    }
    else {
	ientry.data->start=*min_datetime;
    }
    if (max_datetime == nullptr) {
	ientry.data->end=*datetime;
    }
    else {
	ientry.data->end=*max_datetime;
    }
    ientry.data->nsteps=1;
    ID_table[obs_type_index]->insert(ientry);
  }
  else {
    if (lat != ientry.data->S_lat || lon != ientry.data->W_lon) {
	if (lat < ientry.data->S_lat) {
	  ientry.data->S_lat=lat;
	}
	if (lat > ientry.data->N_lat) {
	  ientry.data->N_lat=lat;
	}
	if (lon < ientry.data->W_lon) {
	  ientry.data->W_lon=lon;
	}
	if (lon > ientry.data->E_lon) {
	  ientry.data->E_lon=lon;
	}
	size_t n,m;
	convertLatLonToBox(1,0.,lon,n,m);
	if (ientry.data->min_lon_bitmap[m] > 900.) {
	  ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=lon;
	}
	else { 
	  if (lon < ientry.data->min_lon_bitmap[m]) {
	    ientry.data->min_lon_bitmap[m]=lon;
	  }
	  if (lon > ientry.data->max_lon_bitmap[m]) {
	    ientry.data->max_lon_bitmap[m]=lon;
	  }
	}
    }
    if (min_datetime == nullptr) {
	if (*datetime < ientry.data->start) {
	  ientry.data->start=*datetime;
	}
    }
    else {
	if (*min_datetime < ientry.data->start) {
	  ientry.data->start=*min_datetime;
	}
    }
    if (max_datetime == nullptr) {
	if (*datetime > ientry.data->end) {
	  ientry.data->end=*datetime;
	}
    }
    else {
	if (*max_datetime > ientry.data->end) {
	  ientry.data->end=*max_datetime;
	}
    }
    ++(ientry.data->nsteps);
  }
}

void update_ID_table(size_t obs_type_index,float lat,float lon,DateTime& datetime)
{
  update_ID_table(obs_type_index,lat,lon,&datetime,nullptr,nullptr);
}

void update_platform_table(size_t obs_type_index,float lat,float lon)
{
  if (!platformTable[obs_type_index].found(pentry.key,pentry)) {
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    platformTable[obs_type_index].insert(pentry);
  }
  if (lat == -90.) {
    pentry.boxflags->spole=1;
  }
  else if (lat == 90.) {
    pentry.boxflags->npole=1;
  }
  else {
    size_t n,m;
    convertLatLonToBox(1,lat,lon,n,m);
    pentry.boxflags->flags[n-1][m]=1;
    pentry.boxflags->flags[n-1][360]=1;
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
  metadata::ObML::DataTypeEntry de;
  int off;
  bool add_platform_entry;

  tdir.create("/glade/scratch/rdadata");
  initialize_for_observations();
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
  for (const auto& file : filelist) {
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
	    DateTime min_datetime,max_datetime;
	    auto year=std::stoi(sdum);
	    auto month=n+1;
	    min_datetime.set(year,month,1,0);
	    max_datetime.set(year,month,getDaysInMonth(year,month),235959);
	    update_ID_table(1,ie.lat,ie.lon,nullptr,&min_datetime,&max_datetime);
	    de.key.assign(&line[15],4);
	    if (!ientry.data->data_types_table.found(de.key,de)) {
		de.data.reset(new metadata::ObML::DataTypeEntry::Data);
		de.data->nsteps=1;
		ientry.data->data_types_table.insert(de);
	    }
	    else {
		++(de.data->nsteps);
	    }
	    ++total_num_not_missing;
	    add_platform_entry=true;
	  }
	  off+=8;
	}
	if (add_platform_entry) {
	  update_platform_table(1,ie.lat,ie.lon);
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
  }
}

void scanLITTLE_RFile(std::list<std::string>& filelist)
{
  const char *DATA_TYPES[10]={"Pressure","Height","Temperature","Dew point","Wind speed","Wind direction","East-west wind","North-south wind","Relative humidity","Thickness"};
  initialize_for_observations();
  std::unique_ptr<unsigned char []> buffer;
  for (const auto& file : filelist) {
    InputLITTLE_RStream istream;
    if (!istream.open(file)) {
	myerror="Error opening '"+file+"'";
	exit(1);
    }
    int rec_len;
    while ( (rec_len=istream.peek()) > 0) {
	buffer.reset(new unsigned char[rec_len]);
	istream.read(buffer.get(),rec_len);
	LITTLE_RObservation obs;
	obs.fill(buffer.get(),false);
	if (obs.num_data_records() > 0) {
	  std::vector<short> data_present(10,0);
	  bool all_missing=true;
	  auto& data_records=obs.data_records();
	  for (const auto& r : data_records) {
	    if (std::get<0>(r) > -888888.) {
		data_present[0]=1;
		all_missing=false;
	    }
	    if (std::get<2>(r) > -888888.) {
		data_present[1]=1;
		all_missing=false;
	    }
	    if (std::get<4>(r) > -888888.) {
		data_present[2]=1;
		all_missing=false;
	    }
	    if (std::get<6>(r) > -888888.) {
		data_present[3]=1;
		all_missing=false;
	    }
	    if (std::get<8>(r) > -888888.) {
		data_present[4]=1;
		all_missing=false;
	    }
	    if (std::get<10>(r) > -888888.) {
		data_present[5]=1;
		all_missing=false;
	    }
	    if (std::get<12>(r) > -888888.) {
		data_present[6]=1;
		all_missing=false;
	    }
	    if (std::get<14>(r) > -888888.) {
		data_present[7]=1;
		all_missing=false;
	    }
	    if (std::get<16>(r) > -888888.) {
		data_present[8]=1;
		all_missing=false;
	    }
	    if (std::get<18>(r) > -888888.) {
		data_present[9]=1;
		all_missing=false;
	    }
	  }
	  if (!all_missing) {
	    size_t obs_type_index;
	    if (obs.is_sounding()) {
		obs_type_index=0;
	    }
	    else {
		obs_type_index=1;
	    }
	    switch (obs.platform_code()) {
		case 12:
		case 14:
		case 15:
		case 16:
		case 32:
		case 34:
		case 35:
		case 38:
		{
		  pentry.key="land_station";
		  break;
		}
		case 13:
		case 33:
		case 36:
		{
		  pentry.key="roving_ship";
		  break;
		}
		case 18:
		case 19:
		{
		  pentry.key="drifting_buoy";
		  break;
		}
		case 37:
		case 42:
		case 96:
		case 97:
		case 101:
		{
		  pentry.key="aircraft";
		  break;
		}
		case 86:
		case 88:
		case 116:
		case 121:
		case 122:
		case 133:
		case 281:
		{
		  pentry.key="satellite";
		  break;
		}
		case 111:
		case 114:
		{
		  pentry.key="gps_receiver";
		  break;
		}
		case 132:
		{
		  pentry.key="wind_profiler";
		  break;
		}
		case 135:
		{
		  pentry.key="bogus";
		  break;
		}
		default:
		{
		  metautils::logError("scanLITTLE_RFile() encountered an undocumented platform code: "+strutils::itos(obs.platform_code()),"ascii2xml",user,args.argsString);
		}
	    }
	    update_platform_table(obs_type_index,obs.getLocation().latitude,obs.getLocation().longitude);
	    size_t j,i;
	    convertLatLonToBox(1,obs.getLocation().latitude,obs.getLocation().longitude,j,i);
	    ientry.key=pentry.key+"[!]latlonbox1[!]"+strutils::ftos((j-1)*360+1+i,5,0,'0');
	    auto date_time=obs.getDateTime();
	    update_ID_table(obs_type_index,obs.getLocation().latitude,obs.getLocation().longitude,date_time);
	    for (size_t n=0; n < data_present.size(); ++n) {
		if (data_present[n] == 1) {
		  metadata::ObML::DataTypeEntry de;
		  de.key=DATA_TYPES[n];
		  if (!ientry.data->data_types_table.found(de.key,de)) {
		    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
		    de.data->nsteps=1;
		    ientry.data->data_types_table.insert(de);
		  }
		  else {
		    ++(de.data->nsteps);
		  }
		}
	    }
	    ++total_num_not_missing;
	  }
	}
    }
    istream.close();
  }
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
    write_type=ObML_type;
  }
  else if (args.format == "little_r") {
    scanLITTLE_RFile(filelist);
    write_type=ObML_type;
  }
}

int main(int argc,char **argv)
{
  if (argc < 4) {
    std::cerr << "usage: ascii2xml -f format -d [ds]nnn.n path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f ghcnmv3    GHCN Monthly V3 format" << std::endl;
    std::cerr << "-f little_r   LITTLE_R for WRFDA format" << std::endl;
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
  std::string flags="-f";
  if (std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(cleanUp);
  metautils::cmd_register("ascii2xml",user);
  scanFile();
  std::string ext;
  if (write_type == GrML_type) {
    ext="GrML";
  }
  else if (write_type == ObML_type) {
    ext="ObML";
    if (total_num_not_missing > 0) {
	metadata::ObML::writeObML(ID_table,platformTable,"ascii2xml",user);
    }
    else {
	std::cerr << "All stations have missing location information - no usable data found; no content metadata will be saved for this file" << std::endl;
	exit(1);
    }
  }
  if (args.updateDB) {
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+"."+ext,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
