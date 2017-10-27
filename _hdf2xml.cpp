#include <iostream>
#include <sstream>
#include <memory>
#include <deque>
#include <regex>
#include <unordered_set>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <hdf.hpp>
#include <netcdf.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>

metautils::Directives directives;
metautils::Args args;
std::string user=getenv("USER");
TempFile *tfile=NULL;
std::string inv_file;
TempDir *tdir=nullptr,*inv_dir=nullptr;
std::ofstream inv_stream;
struct ScanData {
  ScanData() : map_name(),varlist(),var_changes_table(),found_map(false),convert_ids_to_upper_case(false) {}

  std::string map_name;
  std::list<std::string> varlist;
  my::map<metautils::StringEntry> var_changes_table;
  bool found_map,convert_ids_to_upper_case;
};
struct GridCoordinates {
  struct CoordinateData {
    CoordinateData() : id(),ds(nullptr),data_array() {}

    std::string id;
    std::shared_ptr<InputHDF5Stream::Dataset> ds;
    HDF5::DataArray data_array;
  };
  GridCoordinates() : reference_time(),valid_time(),time_bounds(),forecast_period(),latitude(),longitude() {}

  CoordinateData reference_time,valid_time,time_bounds,forecast_period,latitude,longitude;
};
struct DiscreteGeometriesData {
  DiscreteGeometriesData() : indexes(),z_units(),z_pos() {}

  struct Indexes {
    Indexes() : time_var(),stn_id_var(),lat_var(),lon_var(),sample_dim_var(),instance_dim_var(),z_var() {}

    std::string time_var,stn_id_var,lat_var,lon_var,sample_dim_var,instance_dim_var,z_var;
  };
  Indexes indexes;
  std::string z_units,z_pos;
};
struct ParameterData {
  ParameterData() : table(),map() {}

  my::map<metautils::StringEntry> table;
  ParameterMap map;
};
struct LibEntry {
  struct Data {
    Data() : id(),ispd_id(),lat(0.),lon(0.),plat_type(0),isrc(0),csrc(' '),already_counted(false) {}

    std::string id,ispd_id;
    float lat,lon;
    short plat_type,isrc;
    char csrc;
    bool already_counted;
  };

  LibEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};
my::map<metadata::GrML::GridEntry> *grid_table=nullptr;
metadata::GrML::GridEntry *gentry;
metadata::GrML::LevelEntry *lentry;
metadata::GrML::ParameterEntry *param_entry;
my::map<metadata::ObML::IDEntry> **id_table=nullptr;
my::map<metadata::ObML::PlatformEntry> *platform_table;
std::unordered_set<std::string> unique_observation_table,unique_data_type_observation_set;
metadata::ObML::DataTypeEntry de;
size_t num_not_missing=0;
std::string cmd_type="";
enum {GrML_type=1,ObML_type};
int write_type=-1;
metautils::NcTime::Time nctime;
metautils::NcTime::TimeBounds time_bounds;
metautils::NcTime::TimeData time_data,fcst_ref_time_data;
my::map<InvEntry> inv_U_table,inv_G_table,inv_L_table,inv_P_table,inv_R_table;
std::list<std::string> inv_lines;
std::string myerror="";
std::stringstream wss;

extern "C" void clean_up()
{
  if (tfile != NULL) {
    delete tfile;
  }
  if (tdir != NULL) {
    delete tdir;
  }
  if (wss.str().length() > 0) {
    metautils::log_warning(wss.str(),"hdf2xml",user,args.args_string);
  }
  if (myerror.length() > 0) {
    metautils::log_error(myerror,"hdf2xml",user,args.args_string);
  }
}

void parse_args()
{
  args.override_primary_check=false;
  args.update_DB=true;
  args.update_summary=true;
  args.regenerate=true;
  std::deque<std::string> sp=strutils::split(args.args_string,"%");
  for (size_t n=0; n < sp.size(); ++n) {
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
    else if (sp[n] == "-I") {
	args.inventory_only=true;
	args.update_DB=false;
    }
    else if (sp[n] == "-R") {
	args.regenerate=false;
    }
    else if (sp[n] == "-S") {
	args.update_summary=false;
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
    args.override_primary_check=true;
    args.update_DB=false;
    args.update_summary=false;
    args.regenerate=false;
  }
  args.path=sp[sp.size()-1];
  auto idx=args.path.length()-1;
  while (idx > 0 && args.path[idx] != '/') {
    idx--;
  }
  args.filename=args.path.substr(idx+1);
  args.path=args.path.substr(0,idx);
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","hdf2xml",user,args.args_string);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

void grid_initialize()
{
  if (grid_table == nullptr) {
    grid_table=new my::map<metadata::GrML::GridEntry>;
    gentry=new metadata::GrML::GridEntry;
    lentry=new metadata::GrML::LevelEntry;
    param_entry=new metadata::GrML::ParameterEntry;
  }
}

void grid_finalize()
{
  delete grid_table;
  delete gentry;
  delete lentry;
  delete param_entry;
}

void obs_initialize()
{
  if (id_table == nullptr) {
    id_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
    for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; n++) {
	id_table[n]=new my::map<metadata::ObML::IDEntry>(9999);
    }
    platform_table=new my::map<metadata::ObML::PlatformEntry>[metadata::ObML::NUM_OBS_TYPES];
  }
}

void obs_finalize()
{
  for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; n++) {
    delete id_table[n];
  }
  delete[] id_table;
  delete[] platform_table;
}

void scan_quikscat_hdf4_file(InputHDF4Stream& istream)
{
istream.print_data_descriptors(1965);
}

void scan_hdf4_file(std::list<std::string>& filelist,ScanData& scan_data)
{
  InputHDF4Stream istream;

  for (const auto& file : filelist) {
    if (!istream.open(file.c_str())) {
	myerror+=" - file: '"+file+"'";
	exit(1);
    }
    if (args.format == "quikscathdf4") {
	scan_quikscat_hdf4_file(istream);
    }
    istream.close();
  }
}

std::string ispd_hdf5_platform_type(const LibEntry& le)
{
  if (le.data->plat_type == -1) {
    return "land_station";
  }
  else {
    switch (le.data->plat_type) {
	case 0:
	case 1:
	case 5:
	case 2002:
	{
	  return "roving_ship";
	}
	case 2:
	case 3:
	case 1007:
	{
	  return "ocean_station";
	}
	case 4:
	{
	  return "lightship";
	}
	case 6:
	{
	  return "moored_buoy";
	}
	case 7:
	case 1009:
	case 2007:
	{
	  if (le.data->ispd_id == "008000" || le.data->ispd_id == "008001") {
	    return "unknown";
	  }
	  else {
	    return "drifting_buoy";
	  }
	}
	case 9:
	{
	  return "ice_station";
	}
	case 10:
	case 11:
	case 12:
	case 17:
	case 18:
	case 19:
	case 20:
	case 21:
	{
	  return "oceanographic";
	}
	case 13:
	{
	  return "CMAN_station";
	}
	case 1001:
	case 1002:
	case 2001:
	case 2003:
	case 2004:
	case 2005:
	case 2010:
	case 2011:
	case 2012:
	case 2013:
	case 2020:
	case 2021:
	case 2022:
	case 2023:
	case 2024:
	case 2025:
	case 2031:
	case 2040:
	{
	  return "land_station";
	}
	case 14:
	{
	  return "coastal_station";
	}
	case 15:
	{
	  return "fixed_ocean_platform";
	}
	case 2030:
	{
	  return "bogus";
	}
	case 1003:
	case 1006:
	{
	  if ((le.data->ispd_id == "001000" && ((le.data->csrc >= '2' && le.data->csrc <= '5') || (le.data->csrc >= 'A' && le.data->csrc <= 'H') || le.data->csrc == 'N')) || le.data->ispd_id == "001003" || (le.data->ispd_id == "001005" && le.data->plat_type == 1003) || le.data->ispd_id == "003002" || le.data->ispd_id == "003004" || le.data->ispd_id == "003005" || le.data->ispd_id == "003006" || le.data->ispd_id == "003007" || le.data->ispd_id == "003007" || le.data->ispd_id == "003008" || le.data->ispd_id == "003009" || le.data->ispd_id == "003011" || le.data->ispd_id == "003014" || le.data->ispd_id == "003015" || le.data->ispd_id == "003021" || le.data->ispd_id == "003022" || le.data->ispd_id == "003026" || le.data->ispd_id == "004000" || le.data->ispd_id == "004003") {
	    return "land_station";
	  }
	  else if (le.data->ispd_id == "002000") {
	    if (le.data->id.length() == 5) {
		if (std::stoi(le.data->id) < 99000) {
		  return "land_station";
		}
		else if (std::stoi(le.data->id) < 99100) {
		  return "fixed_ship";
		}
		else {
		  return "roving_ship";
		}
	    }
	    else {
		return "unknown";
	    }
	  }
	  else if (le.data->ispd_id == "002001" || (le.data->ispd_id == "001005" && le.data->plat_type == 1006)) {
		return "unknown";
	  }
	  else if (le.data->ispd_id == "003010") {
	    std::deque<std::string> sp=strutils::split(le.data->id,"-");
	    if (sp.size() == 2 && sp[1].length() == 5 && strutils::is_numeric(sp[1])) {
		return "land_station";
	    }
	  }
	  else if (le.data->ispd_id >= "010000" && le.data->ispd_id <= "019999") {
	    if (le.data->plat_type == 1006 && le.data->id.length() == 5 && strutils::is_numeric(le.data->id)) {
		if (le.data->id < "99000") {
		  return "land_station";
		}
		else if (le.data->id >= "99200" && le.data->id <= "99299") {
		  return "drifting_buoy";
		}
	    }
	    else {
		return "unknown";
	    }
	  }
	  else {
//	    metautils::log_warning("unknown platform type (1) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.args_string);
wss << "unknown platform type (1) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	    return "";
	  }
	}
	default:
	{
//	  metautils::log_warning("unknown platform type (2) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.args_string);
wss << "unknown platform type (2) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	  return "";
	}
    }
  }
}

std::string ispd_hdf5_id_entry(LibEntry& le,std::string pentry_key,DateTime& dt)
{
  std::deque<std::string> sp;
  std::string ientry_key;

  ientry_key="";
  if (le.data->isrc > 0 && le.data->id.length() > 0 && (le.data->id)[1] == ' ') {
    sp=strutils::split(le.data->id);
    ientry_key=pentry_key+"[!]";
    switch (std::stoi(sp[0])) {
	case 2:
	{
	  ientry_key+="generic[!]"+sp[1];
	  break;
	}
	case 3:
	{
	  ientry_key+="WMO[!]"+sp[1];
	  break;
	}
	case 5:
	{
	  ientry_key+="NDBC[!]"+sp[1];
	  break;
	}
	default:
	{
	  ientry_key+="[!]"+le.data->id;
	}
    }
  }
  else if (le.data->ispd_id == "001000") {
    if ((le.data->id)[6] == '-') {
	sp=strutils::split(le.data->id,"-");
	if (sp[0] != "999999") {
	  ientry_key=pentry_key+"[!]WMO+6[!]"+sp[0];
	}
	else {
	  if (sp[1] != "99999") {
	    ientry_key=pentry_key+"[!]WBAN[!]"+sp[1];
	  }
	  else {
//	    metautils::log_warning("unknown id type (1) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.args_string);
wss << "unknown ID type (1) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
	  }
	}
    }
    else {
//	metautils::log_warning("unknown ID type (2) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.args_string);
wss << "unknown ID type (2) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
    }
  }
  else if (le.data->ispd_id == "001002") {
    ientry_key=pentry_key+"[!]WBAN[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "001003") {
    ientry_key=pentry_key+"[!]RUSSIA[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "001005" || le.data->ispd_id == "001006") {
    if (le.data->plat_type >= 1001 && le.data->plat_type <= 1003 && strutils::is_numeric(le.data->id)) {
	if (le.data->id.length() == 5) {
	  ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
	}
	else if (le.data->id.length() == 6) {
	  ientry_key=pentry_key+"[!]WMO+6[!]"+le.data->id;
	}
    }
    else if (le.data->plat_type == 1002 && !strutils::is_numeric(le.data->id)) {
	ientry_key=pentry_key+"[!]NAME[!]"+le.data->id;
    }
    else if (le.data->id == "999999999999") {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->id;
    }
  }
  else if ((le.data->ispd_id == "001007" && le.data->plat_type == 1001) || le.data->ispd_id == "002000" || le.data->ispd_id == "003002" || le.data->ispd_id == "003008" || le.data->ispd_id == "003015" || le.data->ispd_id == "004000" || le.data->ispd_id == "004001" || le.data->ispd_id == "004003") {
    ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
  }
  else if (((le.data->ispd_id == "001011" && le.data->plat_type == 1002) || le.data->ispd_id == "001007" || le.data->ispd_id == "004002" || le.data->ispd_id == "004004") && !strutils::is_numeric(le.data->id)) {
    ientry_key=pentry_key+"[!]NAME[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "001012" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]COOP[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "002001") {
    if (strutils::is_numeric(le.data->id)) {
	if (le.data->id.length() == 5) {
	  if (dt.year() <= 1948) {
	    ientry_key=pentry_key+"[!]WBAN[!]"+le.data->id;
	  }
	  else {
	    ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
	  }
	}
	else {
	  ientry_key=pentry_key+"[!]unknown[!]"+le.data->id;
	}
    }
    else {
	ientry_key=pentry_key+"[!]callSign[!]"+le.data->id;
    }
  }
  else if (le.data->ispd_id == "003002" && strutils::is_numeric(le.data->id)) {
    if (le.data->id.length() == 5) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
    }
    else if (le.data->id.length() == 6) {
	ientry_key=pentry_key+"[!]WMO+6[!]"+le.data->id;
    }
  }
  else if (le.data->ispd_id == "003004") {
    ientry_key=pentry_key+"[!]CANADA[!]"+le.data->id;
  }
  else if ((le.data->ispd_id == "003006" || le.data->ispd_id == "003030") && le.data->plat_type == 1006) {
    ientry_key=pentry_key+"[!]AUSTRALIA[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003009" && le.data->plat_type == 1006) {
    ientry_key=pentry_key+"[!]SPAIN[!]"+le.data->id;
  }
  else if ((le.data->ispd_id == "003010" || le.data->ispd_id == "003011") && le.data->plat_type == 1003) {
    sp=strutils::split(le.data->id,"-");
    if (sp.size() == 2 && sp[1].length() == 5 && strutils::is_numeric(sp[1])) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
    }
  }
  else if (le.data->ispd_id == "003012" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]SWITZERLAND[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003013" && (le.data->plat_type == 1002 || le.data->plat_type == 1003)) {
    ientry_key=pentry_key+"[!]SOUTHAFRICA[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003014" && le.data->plat_type == 1003) {
    ientry_key=pentry_key+"[!]NORWAY[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003016" && le.data->plat_type == 1002) {
    ientry_key=pentry_key+"[!]PORTUGAL[!]"+le.data->id;
  }
  else if ((le.data->ispd_id == "003019" || le.data->ispd_id == "003100") && le.data->plat_type == 1002 && le.data->id.length() > 0) {
    ientry_key=pentry_key+"[!]NEWZEALAND[!]"+le.data->id;
  }
  else if ((le.data->ispd_id == "003007" || le.data->ispd_id == "003021" || le.data->ispd_id == "003022" || le.data->ispd_id == "003023" || le.data->ispd_id == "003025" || le.data->ispd_id == "003101" || le.data->ispd_id == "004005" || le.data->ispd_id == "006000") && le.data->plat_type == 1002 && le.data->id.length() > 0) {
    ientry_key=pentry_key+"[!]NAME[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003026" && le.data->plat_type == 1006 && le.data->id.length() == 5) {
    ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
  }
  else if (le.data->ispd_id == "003030" && le.data->plat_type == 2001) {
    if (strutils::is_numeric(le.data->id)) {
	ientry_key=pentry_key+"[!]AUSTRALIA[!]"+le.data->id;
    }
    else {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->id;
    }
  }
  else if (le.data->ispd_id == "008000" || le.data->ispd_id == "008001") {
    ientry_key=pentry_key+"[!]TropicalCyclone[!]"+le.data->id;
  }
  else if (le.data->ispd_id >= "010000" && le.data->ispd_id <= "019999") {
    if (le.data->id.length() == 5 && strutils::is_numeric(le.data->id)) {
	ientry_key=pentry_key+"[!]WMO[!]"+le.data->id;
    }
    else {
	ientry_key=pentry_key+"[!]unknown[!]"+le.data->id;
    }
  }
  else if (le.data->id == "999999999999" || (le.data->id.length() > 0 && (le.data->ispd_id == "001013" || le.data->ispd_id == "001014" || le.data->ispd_id == "001018" || le.data->ispd_id == "003005" || le.data->ispd_id == "003020" || le.data->ispd_id == "005000"))) {
    ientry_key=pentry_key+"[!]unknown[!]"+le.data->id;
  }
  if (ientry_key.length() == 0) {
//    metautils::log_warning("unknown ID type (3) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'","hdf2xml",user,args.args_string);
wss << "unknown ID type (3) for station '"+le.data->id+"' "+strutils::ftos(le.data->lat,4)+" "+strutils::ftos(le.data->lon,4)+" "+le.data->ispd_id+" "+strutils::itos(le.data->plat_type)+" "+strutils::itos(le.data->isrc)+" '"+std::string(1,le.data->csrc)+"'" << std::endl;
  }
  return ientry_key;
}

void update_id_table(size_t obs_type_index,metadata::ObML::IDEntry& ientry,float lat,float lon,DateTime& dt,double unique_timestamp)
{
  metautils::StringEntry se;
  se.key=strutils::itos(obs_type_index)+";"+ientry.key+"-"+strutils::dtos(unique_timestamp);
  if (!id_table[obs_type_index]->found(ientry.key,ientry)) {
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (size_t n=0; n < 360; ++n) {
	ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    ientry.data->S_lat=ientry.data->N_lat=lat;
    ientry.data->W_lon=ientry.data->E_lon=lon;
    size_t n,m;
    convert_lat_lon_to_box(1,0.,lon,n,m);
    ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=lon;
    ientry.data->start=dt;
    ientry.data->end=dt;
    unique_observation_table.emplace(se.key);
    ientry.data->nsteps=1;
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=1;
    ientry.data->data_types_table.insert(de);
    id_table[obs_type_index]->insert(ientry);
    se.key+=":"+de.key;
    unique_data_type_observation_set.emplace(se.key);
  }
  else {
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
    convert_lat_lon_to_box(1,0.,lon,n,m);
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
    if (dt < ientry.data->start) {
	ientry.data->start=dt;
    }
    if (dt > ientry.data->end) {
	ientry.data->end=dt;
    }
    if (unique_observation_table.find(se.key) == unique_observation_table.end()) {
	++(ientry.data->nsteps);
	unique_observation_table.emplace(se.key);
    }
    if (!ientry.data->data_types_table.found(de.key,de)) {
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	ientry.data->data_types_table.insert(de);
    }
    se.key+=":"+de.key;
    if (unique_data_type_observation_set.find(se.key) == unique_data_type_observation_set.end()) {
	++(de.data->nsteps);
	unique_data_type_observation_set.emplace(se.key);
    }
  }
}

void update_platform_table(size_t obs_type_index,metadata::ObML::PlatformEntry& pentry,float lat,float lon)
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
    convert_lat_lon_to_box(1,lat,lon,n,m);
    pentry.boxflags->flags[n-1][m]=1;
    pentry.boxflags->flags[n-1][360]=1;
  }
}

void scan_ispd_hdf5_file(InputHDF5Stream& istream)
{
  InputHDF5Stream::CompoundDatatype cpd;
  int m,l;
  InputHDF5Stream::DataValue dv;
  my::map<LibEntry> stn_library(9999);
  LibEntry le;
  metadata::ObML::IDEntry ientry;
  metadata::ObML::PlatformEntry pentry;
  std::string timestamp,sdum;
  DateTime dt;
  double v[3]={0.,0.,0.};
  metadata::ObML::DataTypeEntry de;

  obs_initialize();
// load the station library
  auto ds=istream.dataset("/Data/SpatialTemporalLocation/SpatialTemporalLocation");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    myerror="unable to locate spatial/temporal information";
    exit(1);
  }
  HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
  for (const auto& chunk : ds->data.chunks) {
    for (m=0,l=0; m < ds->data.sizes.front(); m++) {
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	le.key=reinterpret_cast<char *>(dv.get());
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	if (le.key.length() > 0 && !stn_library.found(le.key,le)) {
	  le.data.reset(new LibEntry::Data);
	  le.data->plat_type=-1;
	  le.data->isrc=-1;
	  le.data->csrc='9';
	  le.data->already_counted=false;
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
	  le.data->id=reinterpret_cast<char *>(dv.get());
	  strutils::trim(le.data->id);
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[13].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[13].datatype,ds->dataspace,istream.debug_is_on());
	  le.data->lat=*(reinterpret_cast<float *>(dv.get()));
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[14].datatype,ds->dataspace,istream.debug_is_on());
	  le.data->lon=*(reinterpret_cast<float *>(dv.get()));
	  if (le.data->lon > 180.) {
	    le.data->lon-=360.;
	  }
	  stn_library.insert(le);
	}
	l+=ds->data.size_of_element;
    }
  }
// load the ICOADS platform types
  if ( (ds=istream.dataset("/SupplementalData/Tracking/ICOADS/TrackingICOADS")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
		le.data->isrc=*(reinterpret_cast<int *>(dv.get()));
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[4].datatype,ds->dataspace,istream.debug_is_on());
		le.data->plat_type=*(reinterpret_cast<int *>(dv.get()));
	    }
	    else {
		metautils::log_error("no entry for '"+le.key+"' in station library","hdf2xml",user,args.args_string);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load observation types for IDs that don't already have a platform type
  if ( (ds=istream.dataset("/Data/Observations/ObservationTypes")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		if (le.data->plat_type < 0) {
		  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
		  le.data->plat_type=1000+*(reinterpret_cast<int *>(dv.get()));
		}
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[4].datatype,ds->dataspace,istream.debug_is_on());
		le.data->ispd_id=reinterpret_cast<char *>(dv.get());
		strutils::replace_all(le.data->ispd_id," ","0");
	    }
	    else {
		metautils::log_error("no entry for '"+le.key+"' in station library","hdf2xml",user,args.args_string);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  if ( (ds=istream.dataset("/SupplementalData/Tracking/Land/TrackingLand")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
		le.data->csrc=(reinterpret_cast<char *>(dv.get()))[0];
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[3].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[3].datatype,ds->dataspace,istream.debug_is_on());
		sdum=reinterpret_cast<char *>(dv.get());
		if (sdum == "FM-12") {
		  le.data->plat_type=2001;
		}
		else if (sdum == "FM-13") {
		  le.data->plat_type=2002;
		}
		else if (sdum == "FM-14") {
		  le.data->plat_type=2003;
		}
		else if (sdum == "FM-15") {
		  le.data->plat_type=2004;
		}
		else if (sdum == "FM-16") {
		  le.data->plat_type=2005;
		}
		else if (sdum == "FM-18") {
		  le.data->plat_type=2007;
		}
		else if (sdum == "  SAO") {
		  le.data->plat_type=2010;
		}
		else if (sdum == " AOSP") {
		  le.data->plat_type=2011;
		}
		else if (sdum == " AERO") {
		  le.data->plat_type=2012;
		}
		else if (sdum == " AUTO") {
		  le.data->plat_type=2013;
		}
		else if (sdum == "SY-AE") {
		  le.data->plat_type=2020;
		}
		else if (sdum == "SY-SA") {
		  le.data->plat_type=2021;
		}
		else if (sdum == "SY-MT") {
		  le.data->plat_type=2022;
		}
		else if (sdum == "SY-AU") {
		  le.data->plat_type=2023;
		}
		else if (sdum == "SA-AU") {
		  le.data->plat_type=2024;
		}
		else if (sdum == "S-S-A") {
		  le.data->plat_type=2025;
		}
		else if (sdum == "BOGUS") {
		  le.data->plat_type=2030;
		}
		else if (sdum == "SMARS") {
		  le.data->plat_type=2031;
		}
		else if (sdum == "  SOD") {
		  le.data->plat_type=2040;
		}
	    }
	    else {
		metautils::log_error("no entry for '"+le.key+"' in station library","hdf2xml",user,args.args_string);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load tropical storm IDs
  if ( (ds=istream.dataset("/SupplementalData/Misc/TropicalStorms/StormID")) != NULL && ds->datatype.class_ == 6) {
    HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	  le.key=reinterpret_cast<char *>(dv.get());
	  if (le.key.length() > 0) {
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
		le.data->id=reinterpret_cast<char *>(dv.get());
		strutils::trim(le.data->id);
	    }
	    else {
		metautils::log_error("no entry for '"+le.key+"' in station library","hdf2xml",user,args.args_string);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// scan the observations
  if ( (ds=istream.dataset("/Data/Observations/Observations")) == NULL || ds->datatype.class_ != 6) {
    myerror="unable to locate observations";
    exit(1);
  }
  HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
  for (const auto& chunk : ds->data.chunks) {
    for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	timestamp=reinterpret_cast<char *>(dv.get());
	strutils::trim(timestamp);
	if (timestamp.length() > 0) {
	  le.key=timestamp;
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	  le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	  if (stn_library.found(le.key,le)) {
	    if (timestamp.length() > 0) {
		if (std::regex_search(timestamp,std::regex("99$"))) {
		  strutils::chop(timestamp,2);
// patch for some bad timestamps
		  if (std::regex_search(timestamp,std::regex(" $"))) {
		    strutils::chop(timestamp);
		    timestamp.insert(8,"0");
		  }
		  timestamp+="00";
		}
		dt.set(std::stoll(timestamp)*100);
		pentry.key=ispd_hdf5_platform_type(le);
		if (pentry.key.length() > 0) {
		  ientry.key=ispd_hdf5_id_entry(le,pentry.key,dt);
		  if (ientry.key.length() > 0) {
// SLP
		    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
		    if (dv._class_ != 1) {
			metautils::log_error("observed SLP is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.args_string);
		    }
		    if (dv.precision_ == 32) {
			v[0]=*(reinterpret_cast<float *>(dv.get()));
		    }
		    else if (dv.precision_ == 64) {
			v[0]=*(reinterpret_cast<double *>(dv.get()));
		    }
		    else {
			metautils::log_error("bad precision ("+strutils::itos(dv.precision_)+") for SLP","hdf2xml",user,args.args_string);
		    }
// STN P
		    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[5].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[5].datatype,ds->dataspace,istream.debug_is_on());
		    if (dv._class_ != 1) {
			metautils::log_error("observed STN P is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.args_string);
		    }
		    if (dv.precision_ == 32) {
			v[1]=*(reinterpret_cast<float *>(dv.get()));
		    }
		    else if (dv.precision_ == 64) {
			v[1]=*(reinterpret_cast<double *>(dv.get()));
		    }
		    else {
			metautils::log_error("bad precision ("+strutils::itos(dv.precision_)+") for SLP","hdf2xml",user,args.args_string);
		    }
		    if ((v[0] >= 860. && v[0] <= 1090.) || (v[1] >= 400. && v[1] <= 1090.)) {
			update_id_table(1,ientry,le.data->lat,le.data->lon,dt,std::stoll(timestamp));
			++num_not_missing;
			le.data->already_counted=true;
			if (v[0] < 9999.9) {
			  de.key="SLP";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			}
			if (v[1] < 9999.9) {
			  de.key="STNP";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			}
			update_platform_table(1,pentry,le.data->lat,le.data->lon);
		    }
		  }
		}
	    }
	  }
	  else {
	    metautils::log_error("no entry for '"+le.key+"' in station library","hdf2xml",user,args.args_string);
	  }
	}
	l+=ds->data.size_of_element;
    }
  }
// scan for feedback information
  ds=istream.dataset("/Data/AssimilationFeedback/AssimilationFeedback");
  if (ds == nullptr) {
    ds=istream.dataset("/Data/AssimilationFeedback/AssimilationFeedBack");
  }
  if (ds != nullptr && ds->datatype.class_ == 6) {
    HDF5::decode_compound_datatype(ds->datatype,cpd,istream.debug_is_on());
    for (const auto& chunk : ds->data.chunks) {
	for (m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace,istream.debug_is_on());
	  timestamp=reinterpret_cast<char *>(dv.get());
	  strutils::trim(timestamp);
	  if (timestamp.length() > 0) {
	    le.key=timestamp;
	    dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_offsets(),cpd.members[1].datatype,ds->dataspace,istream.debug_is_on());
	    le.key+=std::string(reinterpret_cast<char *>(dv.get()));
	    if (stn_library.found(le.key,le)) {
		if (timestamp.length() > 0) {
		  if (std::regex_search(timestamp,std::regex("99$"))) {
		    strutils::chop(timestamp,2);
		    timestamp+="00";
		  }
		  dt.set(std::stoll(timestamp)*100);
		  pentry.key=ispd_hdf5_platform_type(le);
		  if (pentry.key.length() > 0) {
		    ientry.key=ispd_hdf5_id_entry(le,pentry.key,dt);
		    if (ientry.key.length() > 0) {
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace,istream.debug_is_on());
			if (dv._class_ != 1) {
			  metautils::log_error("modified observed pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.args_string);
			}
			if (dv.precision_ == 32) {
			  v[0]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision_ == 64) {
			  v[0]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(dv.precision_)+") for modified observed pressure","hdf2xml",user,args.args_string);
			}
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[11].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[11].datatype,ds->dataspace,istream.debug_is_on());
			if (dv._class_ != 1) {
			  metautils::log_error("ensemble first guess pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.args_string);
			}
			if (dv.precision_ == 32) {
			  v[1]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision_ == 64) {
			  v[1]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(dv.precision_)+") for ensemble first guess pressure","hdf2xml",user,args.args_string);
			}
			dv.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[14].datatype,ds->dataspace,istream.debug_is_on());
			if (dv._class_ != 1) {
			  metautils::log_error("ensemble analysis pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",user,args.args_string);
			}
			if (dv.precision_ == 32) {
			  v[2]=*(reinterpret_cast<float *>(dv.get()));
			}
			else if (dv.precision_ == 64) {
			  v[2]=*(reinterpret_cast<double *>(dv.get()));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(dv.precision_)+") for ensemble analysis pressure","hdf2xml",user,args.args_string);
			}
			if ((v[0] >= 400. && v[0] <= 1090.) || (v[1] >= 400. && v[1] <= 1090.) || (v[2] >= 400. && v[2] <= 1090.)) {
			  update_id_table(1,ientry,le.data->lat,le.data->lon,dt,std::stoll(timestamp));
			  ++num_not_missing;
			  le.data->already_counted=true;
			  de.key="Feedback";
			  if (!ientry.data->data_types_table.found(de.key,de)) {
			    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
			    de.data->nsteps=1;
			    ientry.data->data_types_table.insert(de);
			  }
			  else {
			    ++(de.data->nsteps);
			  }
			  update_platform_table(1,pentry,le.data->lat,le.data->lon);
			}
		    }
		  }
		}
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  for (const auto& key : stn_library.keys()) {
    stn_library.found(key,le);
    le.data=nullptr;
  }
  write_type=ObML_type;
}

void scan_usarray_transportable_hdf5_file(InputHDF5Stream& istream,ScanData& scan_data)
{
  obs_initialize();
// load the pressure dataset
  auto ds=istream.dataset("/obsdata/presdata");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    myerror="unable to locate the pressure dataset";
    exit(1);
  }
  HDF5::DataArray times,stnids,pres;
  times.fill(istream,*ds,0);
  if (times.type != HDF5::DataArray::long_long_) {
     metautils::log_error("expected the timestamps to be 'long long' but got "+strutils::itos(times.type),"hdf2xml",user,args.args_string);
  }
  stnids.fill(istream,*ds,1);
  if (stnids.type != HDF5::DataArray::short_) {
     metautils::log_error("expected the numeric station IDs to be 'short' but got "+strutils::itos(stnids.type),"hdf2xml",user,args.args_string);
  }
  pres.fill(istream,*ds,2);
  if (pres.type != HDF5::DataArray::float_) {
     metautils::log_error("expected the pressures to be 'float' but got "+strutils::itos(pres.type),"hdf2xml",user,args.args_string);
  }
  int num_values=0;
  float pres_miss_val=3.e48;
  short numeric_id=-1;
  metadata::ObML::IDEntry ientry;
  metadata::ObML::PlatformEntry pentry;
  metadata::ObML::DataTypeEntry de;
  metautils::StringEntry se;
  for (const auto& key : ds->attributes.keys()) {
    InputHDF5Stream::Attribute attr;
    ds->attributes.found(key,attr);
    if (key == "NROWS") {
	num_values=*(reinterpret_cast<int *>(attr.value.value));
    }
    else if (key == "LATITUDE_DDEG") {
	if (id_table[1]->size() == 1) {
	  ientry.data->S_lat=ientry.data->N_lat=*(reinterpret_cast<float *>(attr.value.value));
	  if (ientry.data->S_lat == -90.) {
	    pentry.boxflags->spole=1;
	  }
	  else if (ientry.data->S_lat == 90.) {
	    pentry.boxflags->npole=1;
	  }
	}
	else {
	  metautils::log_error("found latitude but no station ID","hdf2xml",user,args.args_string);
	}
    }
    else if (key == "LONGITUDE_DDEG") {
	if (id_table[1]->size() == 1) {
	  ientry.data->W_lon=ientry.data->E_lon=*(reinterpret_cast<float *>(attr.value.value));
	  size_t n,m;
	  convert_lat_lon_to_box(1,ientry.data->S_lat,ientry.data->W_lon,n,m);
	  ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=ientry.data->W_lon;
	  if (pentry.boxflags->spole == 0 && pentry.boxflags->npole == 0) {
	    pentry.boxflags->flags[n-1][m]=1;
	    pentry.boxflags->flags[n-1][360]=1;
	  }
	}
	else {
	  metautils::log_error("found longitude but no station ID","hdf2xml",user,args.args_string);
	}
    }
    else if (key == "CHAR_STATION_ID") {
	if (id_table[1]->size() == 0 && platform_table[1].empty()) {
	  num_not_missing++;
	  pentry.key="land_station";
	  ientry.key.assign(reinterpret_cast<char *>(attr.value.value));
	  ientry.key.insert(0,pentry.key+"[!]USArray[!]TA.");
	  ientry.data.reset(new metadata::ObML::IDEntry::Data);
	  ientry.data->min_lon_bitmap.reset(new float[360]);
	  ientry.data->max_lon_bitmap.reset(new float[360]);
	  for (auto m=0; m < 360; ++m) {
	    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=999.;
	  }
	  ientry.data->start=DateTime(3000,12,31,235959,0);
	  ientry.data->end=DateTime(1000,1,1,0,0);
	  ientry.data->nsteps=0;
	  id_table[1]->insert(ientry);
	  pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
	  pentry.boxflags->initialize(361,180,0,0);
	  platform_table[1].insert(pentry);
	}
	else {
	  metautils::log_error("multiple station IDs not expected","hdf2xml",user,args.args_string);
	}
    }
    else if (key == "NUMERIC_STATION_ID") {
	numeric_id=*(reinterpret_cast<short *>(attr.value.value));
    }
    else if (key == "FIELD_2_NAME") {
	de.key.assign(reinterpret_cast<char *>(attr.value.value));
    }
    else if (key == "FIELD_2_FILL") {
	pres_miss_val=*(reinterpret_cast<float *>(attr.value.value));
    }
    else if (key == "FIELD_2_DESCRIPTION") {
	se.key.assign(reinterpret_cast<char *>(attr.value.value));
    }
  }
  if (se.key.length() == 0) {
    metautils::log_error("unable to get title for the data value","hdf2xml",user,args.args_string);
  }
  if (de.key.length() == 0) {
    metautils::log_error("unable to get the name of the data value","hdf2xml",user,args.args_string);
  }
  else {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=0;
    ientry.data->data_types_table.insert(de);
  }
  DateTime epoch(1970,1,1,0,0);
  for (auto n=0; n < num_values; ++n) {
    if (stnids.short_value(n) != numeric_id) {
	metautils::log_error("unexpected change in the numeric station ID","hdf2xml",user,args.args_string);
    }
    if (pres.float_value(n) != pres_miss_val) {
	++(ientry.data->nsteps);
	++(de.data->nsteps);
	DateTime dt=epoch.seconds_added(times.long_long_value(n));
	if (dt < ientry.data->start) {
	  ientry.data->start=dt;
	}
	if (dt > ientry.data->end) {
	  ientry.data->end=dt;
	}
    }
  }
  scan_data.map_name=remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds"+args.dsnum+".xml",tdir->name());
  scan_data.found_map=(scan_data.map_name.length() > 0);
  se.key=de.key+"<!>"+se.key+"<!>Hz";
  scan_data.varlist.emplace_back(se.key);
  if (scan_data.found_map) {
    se.key=de.key;
    scan_data.var_changes_table.insert(se);
  }
  write_type=ObML_type;
}

std::string gridded_time_method(const std::shared_ptr<InputHDF5Stream::Dataset> ds,const GridCoordinates& gcoords)
{
  InputHDF5Stream::Attribute attr;
  std::string time_method;

  if (ds->attributes.found("cell_methods",attr) && attr.value._class_ == 3) {
    time_method=metautils::NcTime::time_method_from_cell_methods(reinterpret_cast<char *>(attr.value.value),gcoords.valid_time.id);
    if (time_method[0] == '!') {
	metautils::log_error("cell method '"+time_method.substr(1)+"' is not valid CF","hdf2xml",user,args.args_string);
    }
    else {
	return time_method;
    }
  }
  return "";
}

void add_gridded_time_range(std::string key_start,my::map<metautils::StringEntry>& gentry_table,const metautils::NcTime::TimeRangeEntry& tre,const GridCoordinates& gcoords,InputHDF5Stream& istream)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string gentry_key,time_method,error;
  metautils::StringEntry se;
  InvEntry ie;
  bool found_no_method=false;

  vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2) {
	time_method=gridded_time_method(var.dataset,gcoords);
	if (time_method.length() == 0) {
	  found_no_method=true;
	}
	else {
	  ie.key=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,strutils::capitalize(time_method),error);
	  if (error.length() > 0) {
	    metautils::log_error(error,"hdf2xml",user,args.args_string);
	  }
	  gentry_key=key_start+ie.key;
	  if (!gentry_table.found(gentry_key,se)) {
	    se.key=gentry_key;
	    gentry_table.insert(se);
	  }
	}
    }
  }
  if (found_no_method) {
    ie.key=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,"",error);
    if (error.length() > 0) {
	metautils::log_error(error,"hdf2xml",user,args.args_string);
    }
    gentry_key=key_start+ie.key;
    if (!gentry_table.found(gentry_key,se)) {
	se.key=gentry_key;
	gentry_table.insert(se);
    }
  }
  if (inv_stream.is_open() && !inv_U_table.found(ie.key,ie)) {
    ie.num=inv_U_table.size();
    inv_U_table.insert(ie);
  }
}

void add_gridded_lat_lon_keys(my::map<metautils::StringEntry>& gentry_table,Grid::GridDimensions dim,Grid::GridDefinition def,const metautils::NcTime::TimeRangeEntry& tre,const GridCoordinates& gcoords,InputHDF5Stream& istream)
{
  std::string key_start;

  switch (def.type) {
    case Grid::latitudeLongitudeType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.elatitude,3)+"<!>"+strutils::ftos(def.elongitude,3)+"<!>"+strutils::ftos(def.loincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
	add_gridded_time_range(key_start,gentry_table,tre,gcoords,istream);
	break;
    }
    case Grid::polarStereographicType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>";
	add_gridded_time_range(key_start,gentry_table,tre,gcoords,istream);
	break;
    }
    case Grid::lambertConformalType:
    {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>"+strutils::ftos(def.stdparallel1,3)+"<!>"+strutils::ftos(def.stdparallel2,3)+"<!>";
	add_gridded_time_range(key_start,gentry_table,tre,gcoords,istream);
	break;
    }
  }
  InvEntry ie;
  ie.key=strutils::substitute(key_start,"<!>",",");
  strutils::chop(ie.key);
  if (inv_stream.is_open() && !inv_G_table.found(ie.key,ie)) {
    ie.num=inv_G_table.size();
    inv_G_table.insert(ie);
  }
}

double data_array_value(const HDF5::DataArray& data_array,size_t index,const InputHDF5Stream::Dataset *ds)
{
  double value=0.;
  switch (ds->datatype.class_) {
    case 0:
    {
	switch (ds->data.size_of_element) {
	  case 4:
	  {
	    value=(reinterpret_cast<int *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::log_error("unable to get value for fixed-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",user,args.args_string);
	  }
	}
	break;
    }
    case 1:
    {
	switch (ds->data.size_of_element) {
	  case 4:
	  {
	    value=(reinterpret_cast<float *>(data_array.values))[index];
	    break;
	  }
	  case 8:
	  {
	    value=(reinterpret_cast<double *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::log_error("unable to get value for floating-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",user,args.args_string);
	  }
	}
	break;
    }
    default:
    {
	metautils::log_error("unable to decode time from datatype class "+strutils::itos(ds->datatype.class_),"hdf2xml",user,args.args_string);
    }
  }
  return value;
}

void add_gridded_netcdf_parameter(const InputHDF5Stream::DatasetEntry& var,ScanData& scan_data,const metautils::NcTime::TimeRange& time_range,ParameterData& parameter_data,int num_steps)
{
  std::string description,units,standard_name,sdum;
  InputHDF5Stream::Attribute attr;
  metautils::StringEntry se;

  description="";
  units="";
  if (var.dataset->attributes.found("units",attr) && attr.value._class_ == 3) {
    units=reinterpret_cast<char *>(attr.value.value);
  }
  if (description.length() == 0) {
    if ((var.dataset->attributes.found("description",attr) || var.dataset->attributes.found("Description",attr)) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
    else if ((var.dataset->attributes.found("comment",attr) || var.dataset->attributes.found("Comment",attr)) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
    else if (var.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.value);
    }
  }
  if (var.dataset->attributes.found("standard_name",attr) && attr.value._class_ == 3) {
    standard_name=reinterpret_cast<char *>(attr.value.value);
  }
  sdum=var.key;
  strutils::trim(sdum);
  strutils::trim(description);
  strutils::trim(units);
  strutils::trim(standard_name);
  se.key=sdum+"<!>"+description+"<!>"+units+"<!>"+standard_name;
  if (!parameter_data.table.found(se.key,se)) {
    sdum=parameter_data.map.short_name(var.key);
    if (!scan_data.found_map || sdum.length() == 0) {
	parameter_data.table.insert(se);
	scan_data.varlist.emplace_back(se.key);
    }
    else {
	parameter_data.table.insert(se);
	scan_data.varlist.emplace_back(se.key);
	se.key=var.key;
	scan_data.var_changes_table.insert(se);
    }
  }
  param_entry->start_date_time=time_range.first_valid_datetime;
  param_entry->end_date_time=time_range.last_valid_datetime;
  param_entry->num_time_steps=num_steps;
  lentry->parameter_code_table.insert(*param_entry);
}

bool parameter_matches_dimensions(InputHDF5Stream& istream,InputHDF5Stream::Attribute& attr,const GridCoordinates& gcoords,std::string level_id)
{
  int n,off;
  InputHDF5Stream::ReferenceEntry re[3];
  std::shared_ptr<InputHDF5Stream::Dataset> ds[3]={nullptr,nullptr,nullptr};
  InputHDF5Stream::Attribute at[3];
  std::stringstream ss[3];
  bool parameter_matches=false;

  off=8+attr.value.precision_;
  for (n=1; n < static_cast<int>(attr.value.dim_sizes[0]); n++) {
    if (!istream.reference_table_pointer()->found(HDF5::value(&attr.value.vlen.buffer[off],attr.value.precision_),re[n-1]))
	metautils::log_error("unable to dereference dimension reference","hdf2xml",user,args.args_string);
    off+=attr.value.precision_+4;
  }
  switch (attr.value.dim_sizes[0]) {
    case 3:
    {
	if (level_id == "sfc") {
	  if (re[0].name == gcoords.latitude.id && re[1].name == gcoords.longitude.id) {
	    parameter_matches=true;
	  }
	  else if (re[0].name != gcoords.latitude.id && re[1].name != gcoords.longitude.id) {
	    if ( (ds[0]=istream.dataset("/"+gcoords.latitude.id)) != nullptr && (ds[1]=istream.dataset("/"+gcoords.longitude.id)) != nullptr) {
		ds[0]->attributes.found("DIMENSION_LIST",at[0]);
		at[0].value.print(ss[0],istream.reference_table_pointer());
		ds[1]->attributes.found("DIMENSION_LIST",at[1]);
		at[1].value.print(ss[1],istream.reference_table_pointer());
		std::string sdum="["+re[0].name+", "+re[1].name+"]";
		if (ss[0].str() == sdum && ss[1].str() == sdum) {
		  parameter_matches=true;
		}
	    }
	  }
	}
	break;
    }
    case 4:
    {
	auto can_continue=true;
	if (re[0].name != level_id) {
	  can_continue=false;
	  if ( (ds[0]=istream.dataset("/"+level_id)) != nullptr) {
	    ds[0]->attributes.found("DIMENSION_LIST",at[0]);
	    at[0].value.print(ss[0],istream.reference_table_pointer());
	    if (ss[0].str() == "["+re[0].name+"]") {
		can_continue=true;
		ds[0]->free();
		ds[0]=nullptr;
		ss[0].str("");
	    }
	  }
	  else if (level_id == "sfc" && re[0].name == gcoords.forecast_period.id) {
	    can_continue=true;
	  }
	}
	if (can_continue) {
	  if (re[1].name == gcoords.latitude.id && re[2].name == gcoords.longitude.id) {
	     parameter_matches=true;
	  }
	  else {
	    if ( (ds[0]=istream.dataset("/"+gcoords.latitude.id)) != nullptr && (ds[1]=istream.dataset("/"+gcoords.longitude.id)) != nullptr) {
		ds[0]->attributes.found("DIMENSION_LIST",at[0]);
		at[0].value.print(ss[0],istream.reference_table_pointer());
		ds[1]->attributes.found("DIMENSION_LIST",at[1]);
		at[1].value.print(ss[1],istream.reference_table_pointer());
		std::string sdum="["+re[1].name+", "+re[2].name+"]";
		if (ss[0].str() == sdum && ss[1].str() == sdum) {
		  parameter_matches=true;
		}
	    }
	  }
	}
	break;
    }
  }
  return parameter_matches;
}

void add_gridded_parameters_to_netcdf_level_entry(InputHDF5Stream& istream,std::string& gentry_key,const GridCoordinates& gcoords,std::string level_id,ScanData& scan_data,const metautils::NcTime::TimeRangeEntry& tre,ParameterData& parameter_data)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string time_method,tr_description,error;
  metautils::NcTime::TimeRange time_range;

// find all of the variables
  vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2 && attr.value.vlen.class_ == 7 && parameter_matches_dimensions(istream,attr,gcoords,level_id)) {
	time_method=gridded_time_method(var.dataset,gcoords);
	if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	  time_range.first_valid_datetime=tre.data->instantaneous.first_valid_datetime;
	  time_range.last_valid_datetime=tre.data->instantaneous.last_valid_datetime;
	}
	else {
	  if (time_bounds.changed) {
	    metautils::log_error("time bounds changed","hdf2xml",user,args.args_string);
	  }
	  time_range.first_valid_datetime=tre.data->bounded.first_valid_datetime;
	  time_range.last_valid_datetime=tre.data->bounded.last_valid_datetime;
	}
	time_method=strutils::capitalize(time_method);
	tr_description=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
	if (error.length() > 0) {
	  metautils::log_error(error,"hdf2xml",user,args.args_string);
	}
	tr_description=strutils::capitalize(tr_description);
	if (std::regex_search(gentry_key,std::regex(tr_description+"$"))) {
	  if (attr.value.dim_sizes[0] == 3 || attr.value.dim_sizes[0] == 4) {
	    param_entry->key="ds"+args.dsnum+":"+var.key;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	    InvEntry ie;
	    if (!inv_P_table.found(param_entry->key,ie)) {
		ie.key=param_entry->key;
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
	    }
	  }
	}
    }
  }
}

void update_level_entry(InputHDF5Stream& istream,const metautils::NcTime::TimeRangeEntry tre,const GridCoordinates& gcoords,std::string level_id,ScanData& scan_data,ParameterData& parameter_data,unsigned char& level_write)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string time_method,tr_description,error;
  metautils::NcTime::TimeRange time_range;

  vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2 && attr.value.vlen.class_ == 7 && parameter_matches_dimensions(istream,attr,gcoords,level_id)) {
	param_entry->key="ds"+args.dsnum+":"+var.key;
	time_method=gridded_time_method(var.dataset,gcoords);
	time_method=strutils::capitalize(time_method);
	if (!lentry->parameter_code_table.found(param_entry->key,*param_entry)) {
	  if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	    time_range.first_valid_datetime=tre.data->instantaneous.first_valid_datetime;
	    time_range.last_valid_datetime=tre.data->instantaneous.last_valid_datetime;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	  }
	  else {
	    if (time_bounds.changed)
		metautils::log_error("time bounds changed","hdf2xml",user,args.args_string);
	    time_range.first_valid_datetime=tre.data->bounded.first_valid_datetime;
	    time_range.last_valid_datetime=tre.data->bounded.last_valid_datetime;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	  }
	  gentry->level_table.replace(*lentry);
	}
	else {
	  tr_description=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
	  if (error.length() > 0)
	    metautils::log_error(error,"hdf2xml",user,args.args_string);
	  tr_description=strutils::capitalize(tr_description);
	  if (std::regex_search(gentry->key,std::regex(tr_description+"$"))) {
	    if (time_method.length() == 0 || (myequalf(time_bounds.t1,0,0.0001) && myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
		if (tre.data->instantaneous.first_valid_datetime < param_entry->start_date_time)
		  param_entry->start_date_time=tre.data->instantaneous.first_valid_datetime;
		if (tre.data->instantaneous.last_valid_datetime > param_entry->end_date_time)
		  param_entry->end_date_time=tre.data->instantaneous.last_valid_datetime;
	    }
	    else {
		if (tre.data->bounded.first_valid_datetime < param_entry->start_date_time)
		  param_entry->start_date_time=tre.data->bounded.first_valid_datetime;
		if (tre.data->bounded.last_valid_datetime > param_entry->end_date_time)
		  param_entry->end_date_time=tre.data->bounded.last_valid_datetime;
	    }
	    param_entry->num_time_steps+=tre.data->num_steps;
	    lentry->parameter_code_table.replace(*param_entry);
	    gentry->level_table.replace(*lentry);
	  }
	}
	level_write=1;
	InvEntry ie;
	if (!inv_P_table.found(param_entry->key,ie)) {
	  ie.key=param_entry->key;
	  ie.num=inv_P_table.size();
	  inv_P_table.insert(ie);
	}
    }
  }
}

void fill_time_bounds(const HDF5::DataArray& data_array,InputHDF5Stream::Dataset *ds,metautils::NcTime::TimeRangeEntry& tre)
{
  time_bounds.t1=data_array_value(data_array,0,ds);
  time_bounds.diff=data_array_value(data_array,1,ds)-time_bounds.t1;
  for (size_t n=2; n < data_array.num_values; n+=2) {
    if (!myequalf((data_array_value(data_array,n+1,ds)-data_array_value(data_array,n,ds)),time_bounds.diff)) {
	time_bounds.changed=true;
    }
  }
  time_bounds.t2=data_array_value(data_array,data_array.num_values-1,ds);
  std::string error;
  tre.data->bounded.first_valid_datetime=metautils::NcTime::actual_date_time(time_bounds.t1,time_data,error);
  if (error.length() > 0) {
    metautils::log_error(error,"hdf2xml",user,args.args_string);
  }
  tre.data->bounded.last_valid_datetime=metautils::NcTime::actual_date_time(time_bounds.t2,time_data,error);
  if (error.length() > 0) {
    metautils::log_error(error,"hdf2xml",user,args.args_string);
  }
}

DateTime compute_NcTime(HDF5::DataArray& times,size_t index)
{
  auto val=times.value(index);
  DateTime dt;
  if (time_data.units == "seconds") {
    dt=time_data.reference.seconds_added(val);
  }
  else if (time_data.units == "hours") {
    if (myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.hours_added(val);
    }
    else {
	dt=time_data.reference.seconds_added(lroundf(val*3600.));
    }
  }
  else if (time_data.units == "days") {
    if (myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.days_added(val);
    }
    else {
	dt=time_data.reference.seconds_added(lroundf(val*86400.));
    }
  }
  else {
    metautils::log_error("compute_NcTime() unable to set date/time for units '"+time_data.units+"'","hdf2xml",user,args.args_string);
  }
  return dt;
}

void update_inventory(int unum,int gnum,const GridCoordinates& gcoords)
{
  InvEntry ie;
  if (!inv_L_table.found(lentry->key,ie)) {
    ie.key=lentry->key;
    ie.num=inv_L_table.size();
    inv_L_table.insert(ie);
  }
  std::stringstream inv_line;
  for (size_t n=0; n < gcoords.valid_time.data_array.num_values; ++n) {
    for (const auto& key : lentry->parameter_code_table.keys()) {
	InvEntry pie;
	inv_P_table.found(key,pie);
	inv_line.str("");
	std::string error;
	inv_line << "0|0|" << metautils::NcTime::actual_date_time(data_array_value(gcoords.valid_time.data_array,n,gcoords.valid_time.ds.get()),time_data,error).to_string("%Y%m%d%H%MM") << "|" << unum << "|" << gnum << "|" << ie.num << "|" << pie.num << "|0";
	inv_lines.emplace_back(inv_line.str());
    }
  }
}

void process_units_attribute(const InputHDF5Stream::DatasetEntry& ds_entry,DiscreteGeometriesData& dgd)
{
  InputHDF5Stream::Attribute attr;
  ds_entry.dataset->attributes.found("units",attr);
  std::string attr_val(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
  if (std::regex_search(attr_val,std::regex("since"))) {
    if (!dgd.indexes.time_var.empty()) {
	metautils::log_error("processed_units_attribute() returned error: time was already identified - don't know what to do with variable: "+ds_entry.key,"hdf2xml",user,args.args_string);
    }
    metautils::CF::fill_NC_time_data(attr_val,time_data,user);
    dgd.indexes.time_var=ds_entry.key;
  }
  else if (attr_val == "degrees_north") {
    if (dgd.indexes.lat_var.empty()) {
	dgd.indexes.lat_var=ds_entry.key;
    }
  }
  else if (attr_val == "degrees_east") {
    if (dgd.indexes.lon_var.empty()) {
	dgd.indexes.lon_var=ds_entry.key;
    }
  }
}

void scan_cf_point_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data)
{
  obs_initialize();
  auto ds_entry_list=istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry,dgd);
  }
  ds_entry_list=istream.datasets_with_attribute("coordinates");
// look for a "station ID"
  for (const auto& ds_entry : ds_entry_list) {
    if (ds_entry.dataset->datatype.class_ == 3) {
	InputHDF5Stream::Attribute attr;
	if (ds_entry.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
	  std::string aval(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
	  if (std::regex_search(aval,std::regex("ID")) || std::regex_search(aval,std::regex("ident",std::regex::icase))) {
	    dgd.indexes.stn_id_var=ds_entry.key;
	  }
	}
    }
    if (!dgd.indexes.stn_id_var.empty()) {
	break;
    }
  }
  HDF5::DataArray time_vals;
  if (dgd.indexes.time_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine time variable","hdf2xml",user,args.args_string);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.time_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access time variable","hdf2xml",user,args.args_string);
    }
    InputHDF5Stream::Attribute attr;
    if (ds->attributes.found("calendar",attr)) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    }
    time_vals.fill(istream,*ds);
  }
  HDF5::DataArray lat_vals;
  if (dgd.indexes.lat_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine latitude variable","hdf2xml",user,args.args_string);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.lat_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access latitude variable","hdf2xml",user,args.args_string);
    }
    lat_vals.fill(istream,*ds);
  }
  HDF5::DataArray lon_vals;
  if (dgd.indexes.lon_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine longitude variable","hdf2xml",user,args.args_string);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.lon_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access longitude variable","hdf2xml",user,args.args_string);
    }
    lon_vals.fill(istream,*ds);
  }
  HDF5::DataArray id_vals;
  if (dgd.indexes.stn_id_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine report ID variable","hdf2xml",user,args.args_string);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.stn_id_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access report ID variable","hdf2xml",user,args.args_string);
    }
    id_vals.fill(istream,*ds);
  }
  scan_data.map_name=remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds"+args.dsnum+".xml",tdir->name());
  scan_data.found_map=!scan_data.map_name.empty();
  std::vector<DateTime> date_times;
  date_times.reserve(time_vals.num_values);
  std::vector<std::string> ids;
  ids.reserve(time_vals.num_values);
  std::vector<float> lats,lons;
  lats.reserve(time_vals.num_values);
  lons.reserve(time_vals.num_values);
  metadata::ObML::PlatformEntry pentry;
  pentry.key="unknown";
  metadata::ObML::IDEntry ientry;
  ientry.key.reserve(32768);
  for (const auto& ds_entry : ds_entry_list) {
    if (ds_entry.key != dgd.indexes.time_var && ds_entry.key != dgd.indexes.lat_var && ds_entry.key != dgd.indexes.lon_var && ds_entry.key != dgd.indexes.stn_id_var) {
	unique_data_type_observation_set.clear();
	de.key=ds_entry.key;
	auto ds=istream.dataset("/"+ds_entry.key);
	std::string descr,units;
	for (const auto& key : ds->attributes.keys()) {
	  InputHDF5Stream::Attribute attr;
	  ds->attributes.found(key,attr);
	  auto lkey=strutils::to_lower(key);
	  if (lkey == "long_name" || (descr.empty() && (lkey == "description" || std::regex_search(lkey,std::regex("^comment"))))) {
	    descr.assign(reinterpret_cast<char *>(attr.value.value));
	  }
	  else if (lkey == "units") {
	    units.assign(reinterpret_cast<char *>(attr.value.value));
	  }
	}
	strutils::trim(descr);
	strutils::trim(units);
	metautils::StringEntry se;
	se.key=ds_entry.key+"<!>"+descr+"<!>"+units;
	scan_data.varlist.emplace_back(se.key);
	if (scan_data.found_map) {
	  se.key=ds_entry.key;
	  scan_data.var_changes_table.insert(se);
	}
	HDF5::DataArray var_data;
	var_data.fill(istream,*ds);
	auto var_missing_value=HDF5::decode_data_value(ds->datatype,ds->fillvalue.bytes,1.e33);
	for (size_t n=0; n < time_vals.num_values; ++n) {
	  if (n == date_times.size()) {
	    date_times.emplace_back(compute_NcTime(time_vals,n));
	  }
	  if (n == ids.size()) {
	    auto lat_val=lat_vals.value(n);
	    lats.emplace_back(lat_val);
	    auto lon_val=lon_vals.value(n);
	    if (lon_val > 180.) {
		lon_val-=360.;
	    }
	    lons.emplace_back(lon_val);
	    auto id_val=id_vals.string_value(n);
	    for (auto c=id_val.begin(); c != id_val.end(); ) {
		if (*c < 32 || *c > 126) {
		  id_val.erase(c);
		}
		else {
		  ++c;
		}
	    }
	    if (!id_val.empty()) {
		strutils::trim(id_val);
	    }
	    if (scan_data.convert_ids_to_upper_case) {
		ids.emplace_back(strutils::to_upper(id_val));
	    }
	    else {
		ids.emplace_back(id_val);
	    }
	  }
	  if (!ids[n].empty() && var_data.value(n) != var_missing_value) {
	    update_platform_table(1,pentry,lats[n],lons[n]);
	    ientry.key=pentry.key+"[!]unknown[!]"+ids[n];
	    update_id_table(1,ientry,lats[n],lons[n],date_times[n],time_vals.value(n));
	    ++num_not_missing;
	  }
	}
	ds->free();
    }
  }
  write_type=ObML_type;
}

void scan_gridded_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data)
{
  std::shared_ptr<InputHDF5Stream::Dataset> ds;
  InputHDF5Stream::Attribute attr,attr2;
  GridCoordinates gcoords;
  std::string climo_bounds_id,level_id,sdum,time_method,tr_description,error;
  std::vector<std::string> lat_ids,lon_ids;
  metautils::NcLevel::LevelInfo level_info;
  std::deque<std::string> sp,sp2;
  long long dt;
  my::map<metautils::StringEntry> unique_level_id_table;
  metautils::StringEntry se;
  my::map<metautils::NcTime::TimeRangeEntry> time_range_table;
  metautils::NcTime::TimeRangeEntry tre;
  HDF5::DataArray data_array;
  int m,l,num_levels;
  Grid::GridDimensions dim;
  Grid::GridDefinition def;
  std::list<std::string> map_contents;
  my::map<metautils::StringEntry> gentry_table;
  ParameterData parameter_data;
  double ddum;
  InputHDF5Stream::ReferenceEntry re,re2,re3;

  auto found_time=false;
  grid_initialize();
  metadata::open_inventory(inv_file,&inv_dir,inv_stream,"GrML","hdf2xml",user);
  scan_data.map_name=remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds"+args.dsnum+".xml",tdir->name());
// rename the parameter map so that it is not overwritten by the level map,
//   which has the same name
  if (scan_data.map_name.length() > 0) {
    std::stringstream oss,ess;
    mysystem2("/bin/mv "+scan_data.map_name+" "+scan_data.map_name+".p",oss,ess);
    if (ess.str().length() > 0) {
	metautils::log_error("unable to rename parameter map; error - '"+ess.str()+"'","hdf2xml",user,args.args_string);
    }
    scan_data.map_name+=".p";
    if (parameter_data.map.fill(scan_data.map_name)) {
	scan_data.found_map=true;
    }
  }
  auto vars=istream.datasets_with_attribute("CLASS=DIMENSION_SCALE");
  for (const auto& var : vars) {
    if (var.dataset->attributes.found("units",attr) && (attr.value._class_ == 3 || (attr.value._class_ == 9 && attr.value.vlen.class_ == 3))) {
	std::string units_value;
	if (attr.value._class_ == 3) {
	  units_value=reinterpret_cast<char *>(attr.value.value);
	}
	else {
	  int len=(attr.value.vlen.buffer[0] << 24)+(attr.value.vlen.buffer[1] << 16)+(attr.value.vlen.buffer[2] << 8)+attr.value.vlen.buffer[3];
	  units_value=std::string(reinterpret_cast<char *>(&attr.value.vlen.buffer[4]),len);
	}
	std::string standard_name_value;
	if (var.dataset->attributes.found("standard_name",attr) && (attr.value._class_ == 3 || (attr.value._class_ == 9 && attr.value.vlen.class_ == 3))) {
	  if (attr.value._class_ == 3) {
	    standard_name_value=reinterpret_cast<char *>(attr.value.value);
	  }
	  else {
	    int len=(attr.value.vlen.buffer[0] << 24)+(attr.value.vlen.buffer[1] << 16)+(attr.value.vlen.buffer[2] << 8)+attr.value.vlen.buffer[3];
	    standard_name_value=std::string(reinterpret_cast<char *>(&attr.value.vlen.buffer[4]),len);
	  }
	}
	if (std::regex_search(units_value,std::regex("since"))) {
	  if (found_time) {
	    metautils::log_error("time was already identified - don't know what to do with variable: "+var.key,"hdf2xml",user,args.args_string);
	  }
	  for (const auto& key : var.dataset->attributes.keys()) {
	    var.dataset->attributes.found(key,attr);
	    if (attr.value._class_ == 3) {
		if (key == "bounds") {
		  gcoords.time_bounds.id=reinterpret_cast<char *>(attr.value.value);
		  break;
		}
		else if (key == "climatology") {
		  climo_bounds_id=reinterpret_cast<char *>(attr.value.value);
		  break;
		}
	    }
	  }
	  time_data.units=units_value.substr(0,units_value.find("since"));
	  strutils::trim(time_data.units);
	  gcoords.valid_time.id=var.key;
	  if (standard_name_value == "forecast_reference_time") {
	    gcoords.reference_time.id=var.key;
	  }
	  units_value=units_value.substr(units_value.find("since")+5);
	  strutils::replace_all(units_value,"T"," ");
	  strutils::trim(units_value);
	  if (std::regex_search(units_value,std::regex("Z$"))) {
	    strutils::chop(units_value);
	  }
	  sp=strutils::split(units_value);
	  sp2=strutils::split(sp[0],"-");
	  if (sp2.size() != 3) {
	    metautils::log_error("bad netcdf date in units for time","hdf2xml",user,args.args_string);
	  }
	  dt=std::stoi(sp2[0])*10000000000+std::stoi(sp2[1])*100000000+std::stoi(sp2[2])*1000000;
	  if (sp.size() > 1) {
	    sp2=strutils::split(sp[1],":");
	    dt+=std::stoi(sp2[0])*10000;
	    if (sp2.size() > 1) {
		dt+=std::stoi(sp2[1])*100;
	    }
	    if (sp2.size() > 2) {
		dt+=static_cast<long long>(std::stof(sp2[2]));
	    }
	  }
	  time_data.reference.set(dt);
	  found_time=true;
	}
	else if (units_value == "degrees_north") {
	  lat_ids.emplace_back(var.key);
	}
	else if (units_value == "degrees_east") {
	  lon_ids.emplace_back(var.key);
	}
	else {
	  if (standard_name_value == "forecast_period") {
	    gcoords.forecast_period.id=var.key;
	  }
	  else {
	    if (!unique_level_id_table.found(var.key,se)) {
		level_info.ID.emplace_back(var.key);
		if (var.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
		  level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.value));
		}
		level_info.units.emplace_back(units_value);
		level_info.write.emplace_back(0);
		se.key=var.key;
		unique_level_id_table.insert(se);
	    }
	  }
	}
    }
    else if (var.dataset->attributes.found("positive",attr) && attr.value._class_ == 3 && !unique_level_id_table.found(var.key,se)) {
	level_info.ID.emplace_back(var.key);
	if (var.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
	  level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.value));
	}
	level_info.units.emplace_back("");
	level_info.write.emplace_back(0);
	se.key=var.key;
	unique_level_id_table.insert(se);
    }
  }
  if (gcoords.reference_time.id != gcoords.valid_time.id) {
// check for forecasts
    vars=istream.datasets_with_attribute("standard_name=forecast_reference_time");
    if (vars.size() > 1) {
	metautils::log_error("multiple forecast reference times","hdf2xml",user,args.args_string);
    }
    else if (!vars.empty()) {
	auto var=vars.front();
	if (var.dataset->attributes.found("units",attr) && attr.value._class_ == 3) {
	  sdum=reinterpret_cast<char *>(attr.value.value);
	  if (std::regex_search(sdum,std::regex("since"))) {
	    fcst_ref_time_data.units=sdum.substr(0,sdum.find("since"));
	    strutils::trim(fcst_ref_time_data.units);
	    if (fcst_ref_time_data.units != time_data.units) {
		metautils::log_error("time and forecast reference time have different units","hdf2xml",user,args.args_string);
	    }
	    gcoords.reference_time.id=var.key;
	    sdum=sdum.substr(sdum.find("since")+5);
	    strutils::replace_all(sdum,"T"," ");
	    strutils::trim(sdum);
	    if (std::regex_search(sdum,std::regex("Z$"))) {
		strutils::chop(sdum);
	    }
	    sp=strutils::split(sdum);
	    sp2=strutils::split(sp[0],"-");
	    if (sp2.size() != 3) {
		metautils::log_error("bad netcdf date in units for forecast_reference_time","hdf2xml",user,args.args_string);
	    }
	    dt=std::stoi(sp2[0])*10000000000+std::stoi(sp2[1])*100000000+std::stoi(sp2[2])*1000000;
	    if (sp.size() > 1) {
		sp2=strutils::split(sp[1],":");
		dt+=std::stoi(sp2[0])*10000;
		if (sp2.size() > 1) {
		  dt+=std::stoi(sp2[1])*100;
		}
		if (sp2.size() > 2) {
		  dt+=static_cast<long long>(std::stof(sp2[2]));
		}
	    }
	    fcst_ref_time_data.reference.set(dt);
	  }
	}
    }
  }
  if (lat_ids.empty() && lon_ids.empty()) {
    vars=istream.datasets_with_attribute("units=degrees_north");
    if (vars.empty()) {
	vars=istream.datasets_with_attribute("units=degree_north");
    }
    for (const auto& v : vars) {
	lat_ids.emplace_back(v.key);
    }
    vars=istream.datasets_with_attribute("units=degrees_east");
    if (vars.empty()) {
	vars=istream.datasets_with_attribute("units=degree_east");
    }
    for (const auto& v : vars) {
	lon_ids.emplace_back(v.key);
    }
  }
  if (level_id.empty()) {
    vars=istream.datasets_with_attribute("units=Pa");
    for (const auto& v : vars) {
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value._class_ == 9) {
	  level_info.ID.emplace_back(v.key);
	  level_info.description.emplace_back("Pressure Level");
	  level_info.units.emplace_back("Pa");
	  level_info.write.emplace_back(0);
	}
    }
    vars=istream.datasets_with_attribute("positive");
    for (const auto& v : vars) {
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value._class_ == 9) {
	  level_info.ID.emplace_back(v.key);
	  if (v.dataset->attributes.found("description",attr) && attr.value._class_ == 3) {
	    level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.value));
	  }
	  else {
	    level_info.description.emplace_back("");
	  }
	  if (v.dataset->attributes.found("units",attr) && attr.value._class_ == 3) {
	    level_info.units.emplace_back(reinterpret_cast<char *>(attr.value.value));
	  }
	  else {
	    level_info.units.emplace_back("");
	  }
	  level_info.write.emplace_back(0);
	}
    }
  }
  level_info.ID.emplace_back("sfc");
  level_info.description.emplace_back("Surface");
  level_info.units.emplace_back("");
  level_info.write.emplace_back(0);
  if (found_time && !lat_ids.empty() && !lon_ids.empty()) {
    if (time_range_table.empty()) {
	tre.key=-1;
	tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);
	gcoords.valid_time.ds=istream.dataset("/"+gcoords.valid_time.id);
	if (gcoords.valid_time.ds == nullptr) {
	  metautils::log_error("unable to access the /"+gcoords.valid_time.id+" dataset for the data temporal range","hdf2xml",user,args.args_string);
	}
	gcoords.valid_time.data_array.fill(istream,*gcoords.valid_time.ds);
	nctime.t1=data_array_value(gcoords.valid_time.data_array,0,gcoords.valid_time.ds.get());
	nctime.t2=data_array_value(gcoords.valid_time.data_array,gcoords.valid_time.data_array.num_values-1,gcoords.valid_time.ds.get());
	tre.data->instantaneous.first_valid_datetime=metautils::NcTime::actual_date_time(nctime.t1,time_data,error);
	if (error.length() > 0) {
	  metautils::log_error(error,"hdf2xml",user,args.args_string);
	}
	tre.data->instantaneous.last_valid_datetime=metautils::NcTime::actual_date_time(nctime.t2,time_data,error);
	if (error.length() > 0) {
	  metautils::log_error(error,"hdf2xml",user,args.args_string);
	}
	if (gcoords.reference_time.id == gcoords.valid_time.id) {
	  if (!gcoords.forecast_period.id.empty()) {
	    gcoords.forecast_period.ds=istream.dataset("/"+gcoords.forecast_period.id);
	    if (gcoords.forecast_period.ds != nullptr) {
		gcoords.forecast_period.data_array.fill(istream,*gcoords.forecast_period.ds);
	    }
	  }
	}
	else {
	  if (!gcoords.reference_time.id.empty()) {
	    gcoords.reference_time.ds=istream.dataset("/"+gcoords.reference_time.id);
	    if (gcoords.reference_time.ds  == nullptr) {
		metautils::log_error("unable to access the /"+gcoords.reference_time.id+" dataset for the forecast reference times","hdf2xml",user,args.args_string);
	    }
	    gcoords.reference_time.data_array.fill(istream,*gcoords.reference_time.ds);
	    if (gcoords.reference_time.data_array.num_values != gcoords.valid_time.data_array.num_values) {
		metautils::log_error("number of forecast reference times does not equal number of times","hdf2xml",user,args.args_string);
	    }
	    for (size_t n=0; n < gcoords.valid_time.data_array.num_values; ++n) {
		m=data_array_value(gcoords.valid_time.data_array,n,gcoords.valid_time.ds.get())-data_array_value(gcoords.reference_time.data_array,n,gcoords.reference_time.ds.get());
		if (m > 0) {
		  if (static_cast<int>(tre.key) == -1) {
		    tre.key=-m*100;
		  }
		  if ( (-m*100) != static_cast<int>(tre.key)) {
		    metautils::log_error("forecast period changed","hdf2xml",user,args.args_string);
		  }
		}
		else if (m < 0) {
		  metautils::log_error("found a time value that is less than the forecast reference time value","hdf2xml",user,args.args_string);
		}
	    }
	  }
	}
	tre.data->num_steps=gcoords.valid_time.data_array.num_values;
	if (!gcoords.time_bounds.id.empty()) {
	  if ( (ds=istream.dataset("/"+gcoords.time_bounds.id)) == NULL) {
	    metautils::log_error("unable to access the /"+gcoords.time_bounds.id+" dataset for the time bounds","hdf2xml",user,args.args_string);
	  }
	  data_array.fill(istream,*ds);
	  if (data_array.num_values > 0) {
	    fill_time_bounds(data_array,ds.get(),tre);
	  }
	}
	else if (climo_bounds_id.length() > 0) {
	  if ( (ds=istream.dataset("/"+climo_bounds_id)) == NULL) {
	    metautils::log_error("unable to access the /"+climo_bounds_id+" dataset for the climatology bounds","hdf2xml",user,args.args_string);
	  }
	  data_array.fill(istream,*ds);
	  if (data_array.num_values > 0) {
	    fill_time_bounds(data_array,ds.get(),tre);
	    tre.key=(tre.data->bounded.last_valid_datetime).years_since(tre.data->bounded.first_valid_datetime);
	    tre.data->instantaneous.last_valid_datetime=(tre.data->bounded.last_valid_datetime).years_subtracted(tre.key);
	    if (tre.data->instantaneous.last_valid_datetime == tre.data->bounded.first_valid_datetime) {
		tre.data->unit=3;
	    }
	    else if ((tre.data->instantaneous.last_valid_datetime).months_since(tre.data->bounded.first_valid_datetime) == 3) {
		tre.data->unit=2;
	    }
	    else if ((tre.data->instantaneous.last_valid_datetime).months_since(tre.data->bounded.first_valid_datetime) == 1) {
		tre.data->unit=1;
	    }
	    else {
		metautils::log_error("unable to determine climatology unit","hdf2xml",user,args.args_string);
	    }
// COARDS convention for climatology over all-available years
	    if ((tre.data->instantaneous.first_valid_datetime).year() == 0) {
		tre.key=0x7fffffff;
	    }
	  }
	}
	if (time_data.units == "months") {
	  if ((tre.data->instantaneous.first_valid_datetime).day() == 1 && (tre.data->instantaneous.first_valid_datetime).time() == 0) {
	    tre.data->instantaneous.last_valid_datetime.add_seconds(days_in_month((tre.data->instantaneous.last_valid_datetime).year(),(tre.data->instantaneous.last_valid_datetime).month(),time_data.calendar)*86400-1,time_data.calendar);
	  }
	  if (!gcoords.time_bounds.id.empty()) {
	    if ((tre.data->bounded.first_valid_datetime).day() == 1) {
		tre.data->bounded.last_valid_datetime.add_days(days_in_month((tre.data->bounded.last_valid_datetime).year(),(tre.data->bounded.last_valid_datetime).month(),time_data.calendar)-1,time_data.calendar);
	    }
	  }
	  else if (climo_bounds_id.length() > 0) {
	    if ((tre.data->bounded.first_valid_datetime).day() == (tre.data->bounded.last_valid_datetime).day() && (tre.data->bounded.first_valid_datetime).time() == 0 && (tre.data->bounded.last_valid_datetime).time() == 0) {
		tre.data->bounded.last_valid_datetime.subtract_seconds(1);
	    }
	  }
	}
	if (static_cast<int>(tre.key) == -1 && gcoords.forecast_period.data_array.num_values > 0) {
	  for (size_t n=0; n < gcoords.forecast_period.data_array.num_values; ++n) {
	    metautils::NcTime::TimeRangeEntry f_tre;
	    f_tre.key=data_array_value(gcoords.forecast_period.data_array,n,gcoords.forecast_period.ds.get());
	    f_tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);
	    *f_tre.data=*tre.data;
	    f_tre.data->instantaneous.first_valid_datetime.add(time_data.units,f_tre.key);
	    f_tre.data->instantaneous.last_valid_datetime.add(time_data.units,f_tre.key);
	    if (f_tre.key == 0) {
		f_tre.key=-1;
	    }
	    else {
		f_tre.key=-(f_tre.key*100);
	    }
	    time_range_table.insert(f_tre);
	  }
	}
	else {
	  time_range_table.insert(tre);
	}
    }
    if (lat_ids.size() != lon_ids.size()) {
	metautils::log_error("unequal number of latitude and longitude coordinate variables","hdf2xml",user,args.args_string);
    }
    for (size_t n=0; n < lat_ids.size(); ++n) {
	gcoords.latitude.id=lat_ids[n];
	gcoords.latitude.ds=istream.dataset("/"+lat_ids[n]);
	if (gcoords.latitude.ds == nullptr) {
	  metautils::log_error("unable to access the /"+lat_ids[n]+" dataset for the latitudes","hdf2xml",user,args.args_string);
	}
	gcoords.latitude.data_array.fill(istream,*gcoords.latitude.ds);
	gcoords.longitude.id=lon_ids[n];
	gcoords.longitude.ds=istream.dataset("/"+lon_ids[n]);
	if (gcoords.longitude.ds == nullptr) {
	  metautils::log_error("unable to access the /"+lon_ids[n]+" dataset for the latitudes","hdf2xml",user,args.args_string);
	}
	gcoords.longitude.data_array.fill(istream,*gcoords.longitude.ds);
	def.slatitude=data_array_value(gcoords.latitude.data_array,0,gcoords.latitude.ds.get());
	def.slongitude=data_array_value(gcoords.longitude.data_array,0,gcoords.longitude.ds.get());
	InputHDF5Stream::Attribute lat_attr,lon_attr;
	if (gcoords.latitude.ds->attributes.found("DIMENSION_LIST",lat_attr) && lat_attr.value.dim_sizes.size() == 1 && lat_attr.value.dim_sizes[0] == 2 && lat_attr.value._class_ == 9 && gcoords.longitude.ds->attributes.found("DIMENSION_LIST",lon_attr) && lon_attr.value.dim_sizes.size() == 1 && lon_attr.value.dim_sizes[0] == 2 && lon_attr.value._class_ == 9) {
	  if (lat_attr.value.vlen.class_ == 7 && lon_attr.value.vlen.class_ == 7) {
	    if (istream.reference_table_pointer()->found(HDF5::value(&lat_attr.value.vlen.buffer[4],lat_attr.value.precision_),re) && istream.reference_table_pointer()->found(HDF5::value(&lon_attr.value.vlen.buffer[4],lon_attr.value.precision_),re2) && re.name == re2.name && istream.reference_table_pointer()->found(HDF5::value(&lat_attr.value.vlen.buffer[8+lat_attr.value.precision_],lat_attr.value.precision_),re2) && istream.reference_table_pointer()->found(HDF5::value(&lon_attr.value.vlen.buffer[8+lon_attr.value.precision_],lon_attr.value.precision_),re3) && re2.name == re3.name) {
		if ( (ds=istream.dataset("/"+re.name)) == NULL || !ds->attributes.found("NAME",attr) || attr.value._class_ != 3) {
		  metautils::log_error("(1)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
		}
		sp=strutils::split(std::string(reinterpret_cast<char *>(attr.value.value)));
		if (sp.size() == 11) {
		  dim.y=std::stoi(sp[10]);
		}
		else {
		  metautils::log_error("(2)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
		}
		if ( (ds=istream.dataset("/"+re2.name)) == NULL || !ds->attributes.found("NAME",attr) || attr.value._class_ != 3) {
		  metautils::log_error("(3)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
		}
		sp=strutils::split(std::string(reinterpret_cast<char *>(attr.value.value)));
		if (sp.size() == 11) {
		  dim.x=std::stoi(sp[10]);
		}
		else {
		  metautils::log_error("(4)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
		}
	    }
	    else {
		metautils::log_error("(5)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
	    }
	    auto center_x=dim.x/2;
	    auto center_y=dim.y/2;
	    auto xm=center_x-1;
	    auto ym=center_y-1;
	    if (myequalf(
                  data_array_value(gcoords.latitude.data_array,ym*dim.x+xm,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+xm,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                myequalf(
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+xm,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+center_x,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                myequalf(
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+center_x,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,ym*dim.x+center_x,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                myequalf(
                  fabs(data_array_value(gcoords.longitude.data_array,ym*dim.x+xm,gcoords.longitude.ds.get()))+fabs(data_array_value(gcoords.longitude.data_array,center_y*dim.x+xm,gcoords.longitude.ds.get()))+fabs(data_array_value(gcoords.longitude.data_array,center_y*dim.x+center_x,gcoords.longitude.ds.get()))+fabs(data_array_value(gcoords.longitude.data_array,ym*dim.x+center_x,gcoords.longitude.ds.get())),
                  360.,
                  0.00001
                )
              ) {
		def.type=Grid::polarStereographicType;
		if (data_array_value(gcoords.latitude.data_array,ym*dim.x+xm,gcoords.latitude.ds.get()) >= 0.) {
		  def.projection_flag=0;
		  def.llatitude=60.;
		}
		else {
		  def.projection_flag=1;
		  def.llatitude=-60.;
		}
		def.olongitude=lroundf(data_array_value(gcoords.longitude.data_array,ym*dim.x+xm,gcoords.longitude.ds.get())+45.);
		if (def.olongitude > 180.) {
		  def.olongitude-=360.;
		}
// look for dx and dy at the 60-degree parallel
 		double min_fabs=999.,f;
		int min_m=0;
		for (size_t m=0; m < gcoords.latitude.data_array.num_values; ++m) {
		  if ( (f=fabs(def.llatitude-data_array_value(gcoords.latitude.data_array,m,gcoords.latitude.ds.get()))) < min_fabs) {
			min_fabs=f;
			min_m=m;
		  }
		}
		double rad=3.141592654/180.;
// great circle formula:
// //  theta=2*arcsin[ sqrt( sin^2(delta_phi/2) + cos(phi_1)*cos(phi_2)*sin^2(delta_lambda/2) ) ]
// //  phi_1 and phi_2 are latitudes
// //  lambda_1 and lambda_2 are longitudes
// //  dist = 6372.8 * theta
// //  6372.8 is radius of Earth in km
		def.dx=lroundf(
                         asin(
                           sqrt(
                             sin(fabs(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())-data_array_value(gcoords.latitude.data_array,min_m+1,gcoords.latitude.ds.get()))/2.*rad)*
                             sin(fabs(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())-data_array_value(gcoords.latitude.data_array,min_m+1,gcoords.latitude.ds.get()))/2.*rad)+
                             sin(fabs(data_array_value(gcoords.longitude.data_array,min_m,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,min_m+1,gcoords.longitude.ds.get()))/2.*rad)*
                             sin(fabs(data_array_value(gcoords.longitude.data_array,min_m,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,min_m+1,gcoords.longitude.ds.get()))/2.*rad)*
                             cos(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())*rad)*
                             cos(data_array_value(gcoords.latitude.data_array,min_m+1,gcoords.latitude.ds.get())*rad)
                           )
                         )*12745.6);
		def.dy=lroundf(
                         asin(
                           sqrt(
                             sin(fabs(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())-data_array_value(gcoords.latitude.data_array,min_m+dim.x,gcoords.latitude.ds.get()))/2.*rad)*
                             sin(fabs(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())-data_array_value(gcoords.latitude.data_array,min_m+dim.x,gcoords.latitude.ds.get()))/2.*rad)+
                             sin(fabs(data_array_value(gcoords.longitude.data_array,min_m,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,min_m+dim.x,gcoords.longitude.ds.get()))/2.*rad)*
                             sin(fabs(data_array_value(gcoords.longitude.data_array,min_m,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,min_m+dim.x,gcoords.longitude.ds.get()))/2.*rad)*
                             cos(data_array_value(gcoords.latitude.data_array,min_m,gcoords.latitude.ds.get())*rad)*
                             cos(data_array_value(gcoords.latitude.data_array,min_m+dim.x,gcoords.latitude.ds.get())*rad)
                           )
                         )*12745.6);
	    }
	    else if ((dim.x % 2) == 1 && myequalf(data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),0.00001) && myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()),0.00001)) {
		def.type=Grid::lambertConformalType;
		def.llatitude=def.stdparallel1=def.stdparallel2=lround(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.latitude.ds.get()));
		if (def.llatitude >= 0.) {
		  def.projection_flag=0;
		}
		else {
		  def.projection_flag=1;
		}
		def.olongitude=lround(data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()));
		def.dx=def.dy=lround(111.1*cos(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.latitude.ds.get())*3.141592654/180.)*(data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())));
	    }
	    else if ((dim.x % 2) == 0 && myequalf((data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.longitude.ds.get())+data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())),(data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.longitude.ds.get())+data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())),0.00001) && myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.latitude.ds.get()))) {
		def.type=Grid::lambertConformalType;
		def.llatitude=def.stdparallel1=def.stdparallel2=lround(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.latitude.ds.get()));
		if (def.llatitude >= 0.) {
		  def.projection_flag=0;
		}
		else {
		  def.projection_flag=1;
		}
		def.olongitude=lround((data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.longitude.ds.get())+data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()))/2.);
		def.dx=def.dy=lround(111.1*cos(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x-1,gcoords.latitude.ds.get())*3.141592654/180.)*(data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())-data_array_value(gcoords.longitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x-1,gcoords.longitude.ds.get())));
	    }
	    else {
std::cerr.precision(10);
std::cerr << data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()) << std::endl;
std::cerr << data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()) << std::endl;
std::cerr << myequalf(data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),0.00001) << std::endl;
std::cerr << data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()) << " " << center_y << " " << gcoords.longitude.data_array.dimensions[1] << " " << xm << std::endl;
std::cerr << data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()) << " " << center_y << " " << gcoords.longitude.data_array.dimensions[1] << " " << center_x+1 << std::endl;
std::cerr << myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()),0.00001) << std::endl;
		metautils::log_error("(6)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
	    }
	  }
	  else {
	    metautils::log_error("(7)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",user,args.args_string);
	  }
	}
	else {
	  def.type=Grid::latitudeLongitudeType;
	  dim.y=gcoords.latitude.data_array.num_values;
	  dim.x=gcoords.longitude.data_array.num_values;
	  def.elatitude=data_array_value(gcoords.latitude.data_array,dim.y-1,gcoords.latitude.ds.get());
	  def.laincrement=fabs((def.elatitude-def.slatitude)/(dim.y-1));
	  def.elongitude=data_array_value(gcoords.longitude.data_array,dim.x-1,gcoords.longitude.ds.get());
	  def.loincrement=fabs((def.elongitude-def.slongitude)/(dim.x-1));
	}
	for (size_t m=0; m < level_info.ID.size(); ++m) {
	  gentry_table.clear();
	  level_id=level_info.ID[m];
	  if (m == (level_info.ID.size()-1) && level_id == "sfc") {
	    num_levels=1;
	    ds=nullptr;
	  }
	  else {
	    if ( (ds=istream.dataset("/"+level_id)) == nullptr) {
		metautils::log_error("unable to access the /"+level_id+" dataset for level information","hdf2xml",user,args.args_string);
	    }
	    data_array.fill(istream,*ds);
	    num_levels=data_array.num_values;
	    if (ds->attributes.found("bounds",attr) && attr.value._class_ == 3) {
		sdum=reinterpret_cast<char *>(attr.value.value);
		gcoords.time_bounds.ds=istream.dataset("/"+sdum);
		if (gcoords.time_bounds.ds == nullptr) {
		  metautils::log_error("unable to get bounds for level '"+level_id+"'","hdf2xml",user,args.args_string);
		}
		gcoords.time_bounds.data_array.fill(istream,*gcoords.time_bounds.ds);
	    }
	  }
	  for (const auto& key : time_range_table.keys()) {
	    time_range_table.found(key,tre);
	    add_gridded_lat_lon_keys(gentry_table,dim,def,tre,gcoords,istream);
	    for (const auto& key2 : gentry_table.keys()) {
		gentry->key=key2;
		sp=strutils::split(gentry->key,"<!>");
		InvEntry uie,gie;
		if (inv_stream.is_open()) {
		  inv_U_table.found(sp.back(),uie);
		  gie.key=sp[0];
		  for (size_t nn=1; nn < sp.size()-1; ++nn) {
		    gie.key+=","+sp[nn];
		  }
		  inv_G_table.found(gie.key,gie);
		}
		if (!grid_table->found(gentry->key,*gentry)) {
// new grid
		  gentry->level_table.clear();
		  lentry->parameter_code_table.clear();
		  param_entry->num_time_steps=0;
		  add_gridded_parameters_to_netcdf_level_entry(istream,gentry->key,gcoords,level_id,scan_data,tre,parameter_data);
		  if (!lentry->parameter_code_table.empty()) {
		    for (l=0; l < num_levels; ++l) {
			lentry->key="ds"+args.dsnum+","+level_id+":";
			if (gcoords.time_bounds.ds == nullptr) {
			  if (ds == nullptr) {
			    ddum=0;
			  }
			  else {
			    ddum=data_array_value(data_array,l,ds.get());
			  }
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			}
			else {
			  ddum=data_array_value(gcoords.time_bounds.data_array,l*2,gcoords.time_bounds.ds.get());
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			  ddum=data_array_value(gcoords.time_bounds.data_array,l*2+1,gcoords.time_bounds.ds.get());
			  lentry->key+=":";
			  if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			    lentry->key+=strutils::itos(ddum);
			  }
			  else {
			    lentry->key+=strutils::ftos(ddum,3);
			  }
			}
			gentry->level_table.insert(*lentry);
			level_info.write[m]=1;
			if (inv_stream.is_open()) {
			  update_inventory(uie.num,gie.num,gcoords);
			}
		    }
		  }
		  if (!gentry->level_table.empty()) {
		    grid_table->insert(*gentry);
		  }
 		}
		else {
// existing grid - needs update
		  for (l=0; l < num_levels; ++l) {
		    lentry->key="ds"+args.dsnum+","+level_id+":";
		    if (ds == nullptr) {
			ddum=0;
		    }
		    else {
			ddum=data_array_value(data_array,l,ds.get());
		    }
		    if (myequalf(ddum,static_cast<int>(ddum),0.001)) {
			lentry->key+=strutils::itos(ddum);
		    }
		    else {
			lentry->key+=strutils::ftos(ddum,3);
		    }
		    if (!gentry->level_table.found(lentry->key,*lentry)) {
			lentry->parameter_code_table.clear();
			add_gridded_parameters_to_netcdf_level_entry(istream,gentry->key,gcoords,level_id,scan_data,tre,parameter_data);
			if (!lentry->parameter_code_table.empty()) {
			  gentry->level_table.insert(*lentry);
			  level_info.write[m]=1;
			}
		    }
		    else {
 			update_level_entry(istream,tre,gcoords,level_id,scan_data,parameter_data,level_info.write[m]);
		    }
		    if (level_info.write[m] == 1 && inv_stream.is_open()) {
			update_inventory(uie.num,gie.num,gcoords);
		    }
		  }
		  grid_table->replace(*gentry);
		}
	    }
	  }
	}
	error=metautils::NcLevel::write_level_map(args.dsnum,level_info);
	if (error.length() > 0) {
	  metautils::log_error(error,"hdf2xml",user,args.args_string);
	}
    }
  }
  write_type=GrML_type;
  if (grid_table->size() == 0) {
    if (found_time) {
	metautils::log_error("No grids found - no content metadata will be generated","hdf2xml",user,args.args_string);
    }
    else {
	metautils::log_error("Time coordinate variable not found - no content metadata will be generated","hdf2xml",user,args.args_string);
    }
  }
if (inv_stream.is_open()) {
InvEntry ie;
ie.key="x";
ie.num=0;
inv_R_table.insert(ie);
}
}

void scan_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data)
{
  auto ds=istream.dataset("/");
  if (ds == nullptr) {
    myerror="unable to access global attributes";
    exit(1);
  }
  InputHDF5Stream::Attribute attr;
  if (ds->attributes.found("featureType",attr)) {
    std::string feature_type=reinterpret_cast<char *>(attr.value.value);
// patch for ICOADS netCDF4 IDs, which may be a mix, so ignore case
    if (ds->attributes.found("product_version",attr)) {
	std::string product_version=reinterpret_cast<char *>(attr.value.value);
	if (std::regex_search(product_version,std::regex("ICOADS")) && std::regex_search(product_version,std::regex("netCDF4"))) {
	  scan_data.convert_ids_to_upper_case=true;
	}
    }
    if (feature_type == "point") {
	scan_cf_point_hdf5nc4_file(istream,scan_data);
    }
    else {
	myerror="featureType '"+feature_type+"' not recognized";
	exit(1);
    }
  }
  else {
    scan_gridded_hdf5nc4_file(istream,scan_data);
  }
}

void scan_hdf5_file(std::list<std::string>& filelist,ScanData& scan_data)
{
  InputHDF5Stream istream;
  std::string map_type,warning,error;

  for (const auto& file : filelist) {
    if (!istream.open(file.c_str())) {
	myerror+=" - file: '"+file+"'";
	exit(1);
    }
    if (args.format == "ispdhdf5") {
	scan_ispd_hdf5_file(istream);
    }
    else if (args.format == "hdf5nc4") {
	scan_hdf5nc4_file(istream,scan_data);
    }
    else if (args.format == "usarrthdf5") {
	scan_usarray_transportable_hdf5_file(istream,scan_data);
    }
    else {
	std::cerr << "Error: bad data format specified" << std::endl;
	exit(1);
    }
    istream.close();
  }
  if (write_type == GrML_type) {
    cmd_type="GrML";
    if (!args.inventory_only) {
	metadata::GrML::write_GrML(*grid_table,"hdf2xml",user);
    }
    grid_finalize();
  }
  else if (write_type == ObML_type) {
    if (num_not_missing > 0) {
	args.format="hdf5";
	cmd_type="ObML";
	metadata::ObML::write_ObML(id_table,platform_table,"hdf2xml",user);
    }
    else {
	metautils::log_error("all stations have missing location information - no usable data found; no content metadata will be saved for this file","hdf2xml",user,args.args_string);
    }
    obs_finalize();
  }
  if (write_type == GrML_type) {
    map_type="parameter";
  }
  else if (write_type == ObML_type) {
    map_type="dataType";
  }
  else {
    metautils::log_error("scanHDF5File returned error: unknown map type","hdf2xml",user,args.args_string);
  }
  error=metautils::NcParameter::write_parameter_map(args.dsnum,scan_data.varlist,scan_data.var_changes_table,map_type,scan_data.map_name,scan_data.found_map,warning);
}

void scan_file()
{

  tfile=new TempFile;
  if (!tfile->open(directives.temp_path)) {
    metautils::log_error("scan_file() was not able to create a temporary file in "+directives.temp_path,"hdf2xml",user,args.args_string);
  }
  tdir=new TempDir;
  if (!tdir->create(directives.temp_path)) {
    metautils::log_error("scan_file() was not able to create a temporary directory in "+directives.temp_path,"hdf2xml",user,args.args_string);
  }
  std::string file_format,error;
  std::list<std::string> filelist;
  if (!primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","hdf2xml",user,args.args_string);
  }
  if (filelist.empty()) {
    filelist.emplace_back(tfile->name());
  }
  ScanData scan_data;
  if (std::regex_search(args.format,std::regex("hdf4"))) {
    scan_hdf4_file(filelist,scan_data);
  }
  else if (std::regex_search(args.format,std::regex("hdf5"))) {
    scan_hdf5_file(filelist,scan_data);
  }
  else {
    std::cerr << "Error: bad data format specified" << std::endl;
    exit(1);
  }
}

int main(int argc,char **argv)
{
  std::string flags;

  if (argc < 6) {
    std::cerr << "usage: hdf2xml -f format -d [ds]nnn.n [options...] <path>" << std::endl;
    std::cerr << "\nrequired (choose one):" << std::endl;
    std::cerr << "HDF4 formats:" << std::endl;
    std::cerr << "-f quikscathdf4   NASA QuikSCAT HDF4" << std::endl;
    std::cerr << "HDF5 formats:" << std::endl;
    std::cerr << "-f ispdhdf5       NOAA International Surface Pressure Databank HDF5" << std::endl;
    std::cerr << "-f hdf5nc4        NetCDF4 with HDF5 storage" << std::endl;
    std::cerr << "-f usarrthdf5     EarthScope USArray Transportable Array Pressure Observations" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << "\noptions" << std::endl;
    std::cerr << "-l <name>        name of the HPSS file on local disk (this avoids an HPSS read)" << std::endl;
    std::cerr << "-m <name>        name of member; <path> MUST be the name of a parent file that" << std::endl;
    std::cerr << "                   has support for direct member access" << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>           full HPSS path or URL of the file to read" << std::endl;
    std::cerr << "                 - HPSS paths must begin with \"/FS/DSS\"" << std::endl;
    std::cerr << "                 - URLs must begin with \"http://{rda|dss}.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.args_string=unix_args_string(argc,argv,'%');
  metautils::read_config("hdf2xml",user,args.args_string);
  parse_args();
  atexit(clean_up);
  metautils::cmd_register("hdf2xml",user);
  if (!args.overwrite_only) {
    metautils::check_for_existing_CMD("GrML");
    metautils::check_for_existing_CMD("ObML");
  }
  scan_file();
  flags="-f";
  if (!args.inventory_only && std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  if (args.update_DB) {
    if (!args.update_summary) {
	flags="-S "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (cmd_type.length() == 0) {
	metautils::log_error("content metadata type was not specified","hdf2xml",user,args.args_string);
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.local_root+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+"."+cmd_type,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    InvEntry ie;
    for (const auto& key : inv_U_table.keys()) {
	inv_U_table.found(key,ie);
	inv_stream << "U<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_G_table.keys()) {
	inv_G_table.found(key,ie);
	inv_stream << "G<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_L_table.keys()) {
	inv_L_table.found(key,ie);
	inv_stream << "L<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_P_table.keys()) {
	inv_P_table.found(key,ie);
	inv_stream << "P<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (const auto& key : inv_R_table.keys()) {
	inv_R_table.found(key,ie);
	inv_stream << "R<!>" << ie.num << "<!>" << key << std::endl;
    }
    inv_stream << "-----" << std::endl;
    for (const auto& line : inv_lines) {
	inv_stream << line << std::endl;
    }
    metadata::close_inventory(inv_file,inv_dir,inv_stream,"GrML",true,true,"hdf2xml",user);
  }
}
