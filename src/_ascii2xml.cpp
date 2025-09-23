#include <iostream>
#include <fstream>
#include <signal.h>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <pglocks.hpp>
#include <little_r.hpp>
#include <marine.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror = "";
std::string mywarning = "";
std::string myoutput = "";

std::unique_ptr<TempFile> work_file;
std::unique_ptr<TempDir> work_dir;
gatherxml::markup::ObML::IDEntry ientry;
enum {grml_type=1,obml_type};
int write_type=-1;

extern "C" void clean_up()
{
  if (!myerror.empty()) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::log_error(myerror,"ascii2xml",USER);
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","ascii2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

struct InvEntry {
  InvEntry() : key(),lat(0.),lon(0.),plat_type(0) {}

  std::string key;
  float lat,lon;
  short plat_type;
};

void scan_ghcnv3_file(gatherxml::markup::ObML::ObservationData& obs_data,std::list<std::string>& filelist)
{
  static const std::string THIS_FUNC=__func__;
  TempDir stn_tdir;
  if (!stn_tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to create temporary directory","ascii2xml",USER);
  }
// load the station inventory
  Server server(metautils::directives.rdadb_config);
  if (!server) {
    metautils::log_error(THIS_FUNC+"() returned error: '"+server.error()+"' while trying to connect to RDADB","ascii2xml",USER);
  }
  Query query("select wfile from dssdb.wfile_d564001 where type = 'O' and wfile like '%qcu.inv'");
  if (query.submit(server) < 0) {
    metautils::log_error(THIS_FUNC+"() returned error: '"+query.error()+"' while trying to query for station inventory name","ascii2xml",USER);
  }
  Row row;
  if (!query.fetch_row(row)) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to get station inventory name","ascii2xml",USER);
  }
  auto ghcn_inventory=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds564.1/docs/"+row[0],stn_tdir.name());
  std::ifstream ifs(ghcn_inventory.c_str());
  if (!ifs.is_open()) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to open station inventory file","ascii2xml",USER);
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
	std::string platform_type;
	if (ie.plat_type == 0) {
	  platform_type="land_station";
	}
	else {
	  platform_type="fixed_ship";
	}
	ientry.key=platform_type+"[!]GHCNV3[!]"+ie.key;
	auto year=std::stoi(std::string(&line[11],4));
	auto off=19;
	auto add_platform_entry=false;
	for (size_t n=0; n < 12; ++n) {
	  if (std::string(&line[off],5) != "-9999") {
	    DateTime min_datetime,max_datetime;
	    auto month=n+1;
	    min_datetime.set(year,month,1,0);
	    max_datetime.set(year,month,dateutils::days_in_month(year,month),235959);
	    if (!obs_data.added_to_ids("surface",ientry,std::string(&line[15],4),"",ie.lat,ie.lon,-1.,&min_datetime,&max_datetime)) {
	      metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' while adding ID "+ientry.key,"ascii2xml",USER);
	    }
	    add_platform_entry=true;
	  }
	  off+=8;
	}
	if (add_platform_entry) {
	  if (!obs_data.added_to_platforms("surface",platform_type,ie.lat,ie.lon)) {
	    metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' while adding platform "+platform_type,"ascii2xml",USER);
	  }
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
  }
}

void scan_hcn_file(gatherxml::markup::ObML::ObservationData& obs_data,std::list<std::string>& filelist)
{
  static const std::string THIS_FUNC=__func__;
  TempDir stn_tdir;
  if (!stn_tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to create temporary directory","ascii2xml",USER);
  }
// get the station inventory
  auto hcn_inventory=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds511.0/station_libraries/history.txt",stn_tdir.name());
  std::ifstream ifs(hcn_inventory.c_str());
  if (!ifs.is_open()) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to open station history file","ascii2xml",USER);
  }
  std::unordered_map<std::string,std::vector<std::tuple<int,int,float,float>>> stn_history;
  const size_t LINE_LENGTH=32768;
  char line[LINE_LENGTH];
  ifs.getline(line,LINE_LENGTH);
  std::string xref;
  while (!ifs.eof()) {
    if (line[44] == ' ') {
	std::string stn_id;
	if (xref.empty()) {
	  stn_id.assign(&line[0],6);
	}
	else {
	  stn_id.assign(&line[0],2);
	  stn_id+=xref;
	}
	if (stn_history.find(stn_id) == stn_history.end()) {
	  stn_history.emplace(stn_id,std::vector<std::tuple<int,int,float,float>>());
	}
	int yr,mo,dy;
	strutils::strget(&line[7],mo,2);
	strutils::strget(&line[10],dy,2);
	strutils::strget(&line[13],yr,4);
	auto start_date=yr*10000+mo*100+dy;
	strutils::strget(&line[18],mo,2);
	strutils::strget(&line[21],dy,2);
	strutils::strget(&line[24],yr,4);
	auto end_date=yr*10000+mo*100+dy;
	int deg,min;
	strutils::strget(&line[45],deg,3);
	strutils::strget(&line[48],min,3);
	float lat=deg+min/60.;
	strutils::strget(&line[53],deg,3);
	strutils::strget(&line[56],min,3);
	float lon=-(deg+min/60.);
	stn_history[stn_id].emplace_back(start_date,end_date,lat,lon);
    }
    else {
	if (std::string(&line[61],5) == "FROM ") {
	  xref.assign(&line[66],4);
	}
	else {
	  xref="";
	}
    }
    ifs.getline(line,LINE_LENGTH);
  }
  ifs.close();
  ifs.clear();
  for (const auto& file : filelist) {
    ifs.open(file.c_str());
    if (!ifs.is_open()) {
	myerror="Error opening '"+file+"'";
	exit(1);
    }
    ifs.getline(line,LINE_LENGTH);
    while (!ifs.eof()) {
	std::string stn_id(&line[0],6);
	if (stn_id[0] == ' ') {
	  stn_id[0]='0';
	}
	if (stn_history.find(stn_id) == stn_history.end()) {
	  metautils::log_error(THIS_FUNC+"() returned error: no history entry for '"+stn_id+"'","ascii2xml",USER);
	}
	int yr,mo;
	strutils::strget(&line[13],yr,4);
	strutils::strget(&line[17],mo,2);
	auto date=yr*10000+mo*100+1;
	float lat=-999.,lon;
	for (size_t n=0; n < stn_history[stn_id].size(); ++n) {
	  auto entry=stn_history[stn_id][n];
	  if (date >= std::get<0>(entry) && date <= std::get<1>(entry)) {
	    lat=std::get<2>(entry);
	    lon=std::get<3>(entry);
	    break;
	  }
	  else if (date < std::get<0>(entry)) {
	    if (n > 0 && date > std::get<1>(stn_history[stn_id][n-1])) {
		lat=std::get<2>(stn_history[stn_id][n-1]);
		lon=std::get<3>(stn_history[stn_id][n-1]);
		break;
	    }
	    else if (n == 0) {
		lat=std::get<2>(entry);
		lon=std::get<3>(entry);
		break;
	    }
	  }
	}
	if (lat < -900.) {
	  lat=std::get<2>(stn_history[stn_id].back());
	  lon=std::get<3>(stn_history[stn_id].back());
	}
	ientry.key="land_station[!]COOP[!]"+stn_id;
	auto add_platform_entry=false;
	size_t ndays;
	strutils::strget(&line[20],ndays,2);
	auto off=24;
	for (size_t n=0; n < ndays; ++n) {
	  std::string value(&line[off],4);
	  if (value != "-999") {
	    DateTime min_datetime(yr,mo,n+1,0,-2400);
	    DateTime max_datetime=min_datetime.days_added(1);
	    if (!obs_data.added_to_ids("surface",ientry,std::string(&line[7],4),"",lat,lon,-1.,&min_datetime,&max_datetime)) {
	      metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' while adding ID "+ientry.key,"ascii2xml",USER);
	    }
	    add_platform_entry=true;
	  }
	  off+=8;
	}
	if (add_platform_entry) {
	  if (!obs_data.added_to_platforms("surface","land_station",lat,lon)) {
	    metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' while adding platform","ascii2xml",USER);
	  }
	}
	ifs.getline(line,LINE_LENGTH);
    }
    ifs.close();
    ifs.clear();
  }
}

void scan_little_r_file(gatherxml::markup::ObML::ObservationData& obs_data,std::list<std::string>& filelist)
{
  const char *DATA_TYPES[10]={"Pressure","Height","Temperature","Dew point","Wind speed","Wind direction","East-west wind","North-south wind","Relative humidity","Thickness"};
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
	    std::string obs_type;
	    if (obs.is_sounding()) {
		obs_type="upper_air";
	    }
	    else {
		obs_type="surface";
	    }
	    std::string platform_type;
	    switch (obs.platform_code()) {
		case 12:
		case 14:
		case 15:
		case 16:
		case 32:
		case 34:
		case 35:
		case 38: {
		  platform_type="land_station";
		  break;
		}
		case 13:
		case 33:
		case 36: {
		  platform_type="roving_ship";
		  break;
		}
		case 18:
		case 19: {
		  platform_type="drifting_buoy";
		  break;
		}
		case 37:
		case 42:
		case 96:
		case 97:
		case 101: {
		  platform_type="aircraft";
		  break;
		}
		case 86:
		case 88:
		case 116:
		case 121:
		case 122:
		case 133:
		case 281: {
		  platform_type="satellite";
		  break;
		}
		case 111:
		case 114: {
		  platform_type="gps_receiver";
		  break;
		}
		case 132: {
		  platform_type="wind_profiler";
		  break;
		}
		case 135: {
		  platform_type="bogus";
		  break;
		}
		default: {
		  metautils::log_error("scan_little_r_file() encountered an undocumented platform code: "+strutils::itos(obs.platform_code()),"ascii2xml",USER);
		}
	    }
	    if (!obs_data.added_to_platforms(obs_type,platform_type,obs.location().latitude,obs.location().longitude)) {
		metautils::log_error("scan_little_r_file() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"ascii2xml",USER);
	    }
	    size_t j,i;
	    geoutils::convert_lat_lon_to_box(1,obs.location().latitude,obs.location().longitude,j,i);
	    ientry.key=platform_type+"[!]latlonbox1[!]"+strutils::ftos((j-1)*360+1+i,5,0,'0');
	    auto date_time=obs.date_time();
	    for (size_t n=0; n < data_present.size(); ++n) {
		if (data_present[n] == 1) {
		  gatherxml::markup::ObML::DataTypeEntry de;
		  if (!obs_data.added_to_ids(obs_type,ientry,DATA_TYPES[n],"",obs.location().latitude,obs.location().longitude,-1,&date_time)) {
		    metautils::log_error("scan_little_r_file() returned error: '"+myerror+"' while adding ID "+ientry.key,"ascii2xml",USER);
		  }
		}
	    }
	  }
	}
    }
    istream.close();
  }
}

void scan_nodc_sea_level_file(gatherxml::markup::ObML::ObservationData& obs_data,std::list<std::string>& filelist)
{
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
	if (obs.sea_level_height() < 99999 && obs.sea_level_height() > -9999) {
	  if (metautils::args.data_format == "nodcsl") {
	    metautils::args.data_format=obs.data_format();
	  }
	  else if (metautils::args.data_format != obs.data_format()) {
	    metautils::log_error("scan_nodc_sea_level_file(): data format changed","ascii2xml",USER);
	  }
	  if (!obs_data.added_to_platforms("surface","tide_station",obs.location().latitude,obs.location().longitude)) {
	    metautils::log_error("scan_nodc_sea_level_file() returned error: '"+myerror+"' while adding platform","ascii2xml",USER);
	  }
	  ientry.key="tide_station[!]NODC[!]"+obs.location().ID;
	  DateTime start_date_time;
	  std::unique_ptr<DateTime> end_date_time;
	  if (obs.data_format() == "F186") {
	    end_date_time.reset(new DateTime(obs.date_time()));
	    end_date_time->set_time(999999);
	    start_date_time=*end_date_time;
	    start_date_time.set_day(1);
	  }
	  else if (obs.data_format() == "F185") {
	    end_date_time.reset(new DateTime(obs.date_time()));
	    end_date_time->set_time(end_date_time->time()+99);
	    start_date_time=*end_date_time;
	    start_date_time.set_time(99);
	  }
	  else {
	    start_date_time=obs.date_time();
	    start_date_time.set_time(start_date_time.time()+99);
	  }
	  if (!obs_data.added_to_ids("surface",ientry,"Sea Level Data","",obs.location().latitude,obs.location().longitude,-1.,&start_date_time,end_date_time.get())) {
	    metautils::log_error("scan_nodc_sea_level_file() returned error: '"+myerror+"' while adding ID "+ientry.key,"ascii2xml",USER);
	  }
	}
    }
    istream.close();
  }
  metautils::args.data_format.insert(0,"NODC_");
}

void scan_file(gatherxml::markup::ObML::ObservationData& obs_data)
{
  work_file.reset(new TempFile);
  work_file->open(metautils::directives.temp_path);
  work_dir.reset(new TempDir);
  work_dir->create(metautils::directives.temp_path);
  std::string file_format,error;
  std::list<std::string> filelist;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*work_file,*work_dir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","ascii2xml",USER);
  }
  if (filelist.size() == 0) {
    filelist.emplace_back(work_file->name());
  }
  if (metautils::args.data_format == "ghcnmv3") {
    scan_ghcnv3_file(obs_data,filelist);
    write_type=obml_type;
  }
  else if (metautils::args.data_format == "hcn") {
    scan_hcn_file(obs_data,filelist);
    write_type=obml_type;
  }
  else if (metautils::args.data_format == "little_r") {
    scan_little_r_file(obs_data,filelist);
    write_type=obml_type;
  }
  else if (metautils::args.data_format == "nodcsl") {
    scan_nodc_sea_level_file(obs_data,filelist);
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
    std::cerr << "              - HPSS paths must begin with \"/FS/DECS\"" << std::endl;
    std::cerr << "              - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  atexit(clean_up);
  auto arg_delimiter='!';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("ascii2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  std::string flags="-f";
  if (std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  metautils::cmd_register("ascii2xml",USER);
  gatherxml::markup::ObML::ObservationData obs_data;
  scan_file(obs_data);
  std::string ext;
  if (write_type == grml_type) {
    ext="GrML";
  }
  else if (write_type == obml_type) {
    ext="ObML";
    if (!obs_data.is_empty) {
	gatherxml::markup::ObML::write(obs_data,"ascii2xml",USER);
    }
    else {
	std::cerr << "All stations have missing location information - no usable data found; no content metadata will be saved for this file" << std::endl;
	exit(1);
    }
  }
  if (metautils::args.update_db) {
    if (!metautils::args.regenerate) {
	flags="-R "+flags;
    }
    if (!metautils::args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsid+" "+flags+" "+metautils::args.filename+"."+ext,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
