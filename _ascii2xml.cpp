#include <iostream>
#include <fstream>
#include <signal.h>
#include <sstream>
#include <regex>
#include <little_r.hpp>
#include <marine.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives meta_directives;
metautils::Args meta_args;
std::string myerror="";
std::string mywarning="";

std::string user=getenv("USER");
TempFile *tfile=nullptr;
TempDir *tdir=nullptr;
my::map<metadata::ObML::PlatformEntry> platform_table[metadata::ObML::NUM_OBS_TYPES];
metadata::ObML::PlatformEntry pentry;
my::map<metadata::ObML::IDEntry> **id_table=nullptr;
metadata::ObML::IDEntry ientry;
size_t total_num_not_missing=0;
enum {grml_type=1,obml_type};
int write_type=-1;

extern "C" void clean_up()
{
  if (tfile != nullptr) {
    delete tfile;
  }
  if (tdir != nullptr) {
    delete tdir;
  }
  if (!myerror.empty()) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::log_error(myerror,"ascii2xml",user);
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","ascii2xml",user);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

void parse_args()
{
  std::deque<std::string> sp;
  size_t n;
  int idx;

  meta_args.override_primary_check=false;
  meta_args.update_db=true;
  meta_args.update_summary=true;
  meta_args.regenerate=true;
  sp=strutils::split(meta_args.args_string,"!");
  for (n=0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
	meta_args.dsnum=sp[++n];
	if (std::regex_search(meta_args.dsnum,std::regex("^ds"))) {
	  meta_args.dsnum=meta_args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-f") {
	meta_args.data_format=sp[++n];
    }
    else if (sp[n] == "-l") {
	meta_args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	meta_args.member_name=sp[++n];
    }
  }
  if (meta_args.data_format.empty()) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    meta_args.data_format=strutils::to_lower(meta_args.data_format);
  }
  if (meta_args.dsnum.empty()) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  if (meta_args.dsnum == "999.9") {
    meta_args.override_primary_check=true;
    meta_args.update_db=false;
    meta_args.update_summary=false;
    meta_args.regenerate=false;
  }
  meta_args.path=sp.back();
  idx=meta_args.path.rfind("/");
  meta_args.filename=meta_args.path.substr(idx+1);
  meta_args.path=meta_args.path.substr(0,idx);
}

void initialize_for_observations()
{
  if (id_table == nullptr) {
    id_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
    for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
	id_table[n]=new my::map<metadata::ObML::IDEntry>(9999);
    }
  }
}

struct InvEntry {
  InvEntry() : key(),lat(0.),lon(0.),plat_type(0) {}

  std::string key;
  float lat,lon;
  short plat_type;
};

void update_id_table(size_t obs_type_index,float lat,float lon,DateTime *datetime,DateTime *min_datetime,DateTime *max_datetime)
{
  if (!id_table[obs_type_index]->found(ientry.key,ientry)) {
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->S_lat=ientry.data->N_lat=lat;
    ientry.data->W_lon=ientry.data->E_lon=lon;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (size_t n=0; n < 360; ++n) {
        ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    size_t n,m;
    geoutils::convert_lat_lon_to_box(1,0.,lon,n,m);
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
    id_table[obs_type_index]->insert(ientry);
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
	geoutils::convert_lat_lon_to_box(1,0.,lon,n,m);
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

void update_id_table(size_t obs_type_index,float lat,float lon,DateTime& datetime)
{
  update_id_table(obs_type_index,lat,lon,&datetime,nullptr,nullptr);
}

void update_platform_table(size_t obs_type_index,float lat,float lon)
{
  if (!platform_table[obs_type_index].found(pentry.key,pentry)) {
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    platform_table[obs_type_index].insert(pentry);
  }
  if (lat == -90.) {
    pentry.boxflags->spole=1;
  }
  else if (lat == 90.) {
    pentry.boxflags->npole=1;
  }
  else {
    size_t n,m;
    geoutils::convert_lat_lon_to_box(1,lat,lon,n,m);
    pentry.boxflags->flags[n-1][m]=1;
    pentry.boxflags->flags[n-1][360]=1;
  }
}

void scan_ghcnv3_file(std::list<std::string>& filelist)
{
  TempDir tdir;
  if (!tdir.create(meta_directives.temp_path)) {
    metautils::log_error("scan_ghcnv3_file(): unable to create temporary directory","ascii2xml",user);
  }
  initialize_for_observations();
// load the station inventory
  MySQL::Server server(meta_directives.database_server,meta_directives.rdadb_username,meta_directives.rdadb_password,"dssdb");
  if (!server) {
    metautils::log_error("scan_ghcnv3_file(): '"+server.error()+"' while trying to connect to RDADB","ascii2xml",user);
  }
  MySQL::Query query("select wfile from dssdb.wfile where dsid = 'ds564.1' and type = 'O' and wfile like '%qcu.inv'");
  if (query.submit(server) < 0) {
    metautils::log_error("scan_ghcnv3_file(): '"+query.error()+"' while trying to query for station inventory name","ascii2xml",user);
  }
  MySQL::Row row;
  if (!query.fetch_row(row)) {
    metautils::log_error("scan_ghcnv3_file(): unable to get station inventory name","ascii2xml",user);
  }
  auto ghcn_inventory=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds564.1/docs/"+row[0],tdir.name());
  std::ifstream ifs(ghcn_inventory.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("scan_ghcnv3_file(): unable to open station inventory file","ascii2xml",user);
  }
  my::map<InvEntry> inv_table(9999);
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    InvEntry ie;
    ie.key.assign(line,11);
    ie.lat=std::stof(std::string(&line[12],8));
    ie.lon=std::stof(std::string(&line[21],9));
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
	InvEntry ie;
	ie.key.assign(line,11);
	inv_table.found(ie.key,ie);
	if (ie.plat_type == 0) {
	  pentry.key="land_station";
	}
	else {
	  pentry.key="fixed_ship";
	}
	ientry.key=pentry.key+"[!]GHCNV3[!]"+ie.key;
	auto year=std::stoi(std::string(&line[11],4));
	auto off=19;
	auto add_platform_entry=false;
	for (size_t n=0; n < 12; ++n) {
	  if (std::string(&line[off],5) != "-9999") {
	    DateTime min_datetime,max_datetime;
	    auto month=n+1;
	    min_datetime.set(year,month,1,0);
	    max_datetime.set(year,month,dateutils::days_in_month(year,month),235959);
	    update_id_table(1,ie.lat,ie.lon,nullptr,&min_datetime,&max_datetime);
	    metadata::ObML::DataTypeEntry de;
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

void scan_little_r_file(std::list<std::string>& filelist)
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
		  metautils::log_error("scan_little_r_file() encountered an undocumented platform code: "+strutils::itos(obs.platform_code()),"ascii2xml",user);
		}
	    }
	    update_platform_table(obs_type_index,obs.location().latitude,obs.location().longitude);
	    size_t j,i;
	    geoutils::convert_lat_lon_to_box(1,obs.location().latitude,obs.location().longitude,j,i);
	    ientry.key=pentry.key+"[!]latlonbox1[!]"+strutils::ftos((j-1)*360+1+i,5,0,'0');
	    auto date_time=obs.date_time();
	    update_id_table(obs_type_index,obs.location().latitude,obs.location().longitude,date_time);
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

void scan_nodc_sea_level_file(std::list<std::string>& filelist)
{
  initialize_for_observations();
  std::unique_ptr<unsigned char[]> buffer(nullptr);
  for (const auto& file : filelist) {
    InputNODCSeaLevelObservationStream istream;
    if (!istream.open(file)) {
	myerror="Error opening '"+file+"'";
	exit(1);
    }
    if (buffer == nullptr) {
	buffer.reset(new unsigned char[InputNODCSeaLevelObservationStream::RECORD_LEN]);
    }
    while (istream.read(buffer.get(),InputNODCSeaLevelObservationStream::RECORD_LEN) > 0) {
	NODCSeaLevelObservation obs;
	obs.fill(buffer.get(),false);
	if (obs.sea_level_height() < 99999) {
	  if (meta_args.data_format == "nodcsl") {
	    meta_args.data_format=obs.data_format();
	  }
	  else if (meta_args.data_format != obs.data_format()) {
	    metautils::log_error("scan_nodc_sea_level_file(): data format changed","ascii2xml",user);
	  }
	  pentry.key="tide_station";
	  update_platform_table(1,obs.location().latitude,obs.location().longitude);
	  ientry.key=pentry.key+"[!]NODC[!]"+obs.location().ID;
	  if (obs.data_format() == "F186") {
	    auto max_date_time=obs.date_time();
	    auto min_date_time=max_date_time;
	    min_date_time.set_day(1);
	    update_id_table(1,obs.location().latitude,obs.location().longitude,nullptr,&min_date_time,&max_date_time);
	  }
	  else {
	    auto date_time=obs.date_time();
	    update_id_table(1,obs.location().latitude,obs.location().longitude,date_time);
	  }
	  metadata::ObML::DataTypeEntry de;
	  de.key="Sea Level Data";
	  if (!ientry.data->data_types_table.found(de.key,de)) {
	    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	    de.data->nsteps=1;
	    ientry.data->data_types_table.insert(de);
	  }
	  else {
	    ++(de.data->nsteps);
	  }
	  ++total_num_not_missing;
	}
    }
    istream.close();
  }
  meta_args.data_format.insert(0,"NODC_");
}

void scan_file()
{
  std::string file_format,error;
  std::list<std::string> filelist;

  tfile=new TempFile;
  tfile->open(meta_directives.temp_path);
  tdir=new TempDir;
  tdir->create(meta_directives.temp_path);
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","ascii2xml",user);
  }
  if (filelist.size() == 0) {
    filelist.emplace_back(tfile->name());
  }
  if (meta_args.data_format == "ghcnmv3") {
    scan_ghcnv3_file(filelist);
    write_type=obml_type;
  }
  else if (meta_args.data_format == "little_r") {
    scan_little_r_file(filelist);
    write_type=obml_type;
  }
  else if (meta_args.data_format == "nodcsl") {
    scan_nodc_sea_level_file(filelist);
    write_type=obml_type;
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
    std::cerr << "-f nodcsl     NODC Sea Level Data Formats" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>        full HPSS path or URL of the file to read" << std::endl;
    std::cerr << "              - HPSS paths must begin with \"/FS/DSS\" or \"/DSS\"" << std::endl;
    std::cerr << "              - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  meta_args.args_string=unixutils::unix_args_string(argc,argv,'!');
  metautils::read_config("ascii2xml",user,meta_args.args_string);
  parse_args();
  std::string flags="-f";
  if (std::regex_search(meta_args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("ascii2xml",user);
  scan_file();
  std::string ext;
  if (write_type == grml_type) {
    ext="GrML";
  }
  else if (write_type == obml_type) {
    ext="ObML";
    if (total_num_not_missing > 0) {
	metadata::ObML::write_obml(id_table,platform_table,"ascii2xml",user);
    }
    else {
	std::cerr << "All stations have missing location information - no usable data found; no content metadata will be saved for this file" << std::endl;
	exit(1);
    }
  }
  if (meta_args.update_db) {
    if (!meta_args.regenerate) {
	flags="-R "+flags;
    }
    if (!meta_args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (unixutils::mysystem2(meta_directives.local_root+"/bin/scm -d "+meta_args.dsnum+" "+flags+" "+meta_args.filename+"."+ext,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
