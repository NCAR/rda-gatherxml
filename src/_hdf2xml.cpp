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
#include <gatherxml.hpp>
#include <hdf.hpp>
#include <netcdf.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

std::unique_ptr<TempDir> work_dir;
std::unique_ptr<TempFile> work_file;
std::string inv_file;
TempDir *inv_dir=nullptr;
std::ofstream inv_stream;
struct ScanData {
  ScanData() : num_not_missing(0),write_type(-1),cmd_type(),tdir(nullptr),map_name(),platform_type(),varlist(),var_changes_table(),found_map(false),convert_ids_to_upper_case(false) {}

  enum {GrML_type=1,ObML_type};
  size_t num_not_missing;
  int write_type;
  std::string cmd_type;
  std::unique_ptr<TempDir> tdir;
  std::string map_name,platform_type;
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
    Indexes() : time_var(),stn_id_var(),lat_var(),lon_var(),instance_dim_var(),z_var(),sample_dim_vars() {}

    std::string time_var,stn_id_var,lat_var,lon_var,instance_dim_var,z_var;
    std::unordered_map<std::string,std::string> sample_dim_vars;
  };
  Indexes indexes;
  std::string z_units,z_pos;
};
struct NetCDFVariableAttributeData {
  NetCDFVariableAttributeData() : long_name(),units(),cf_keyword(),missing_value() {}

  std::string long_name,units,cf_keyword;
  InputHDF5Stream::DataValue missing_value;
};
struct ParameterData {
  ParameterData() : table(),map() {}

  my::map<metautils::StringEntry> table;
  ParameterMap map;
};
struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};
std::unique_ptr<my::map<gatherxml::markup::GrML::GridEntry>> grid_table;
std::unique_ptr<gatherxml::markup::GrML::GridEntry> gentry;
std::unique_ptr<gatherxml::markup::GrML::LevelEntry> lentry;
std::unique_ptr<gatherxml::markup::GrML::ParameterEntry> param_entry;
std::unordered_set<std::string> unique_observation_table,unique_data_type_observation_set;
gatherxml::markup::ObML::DataTypeEntry de;
my::map<InvEntry> inv_U_table,inv_G_table,inv_L_table,inv_P_table,inv_R_table;
std::list<std::string> inv_lines;
std::stringstream wss;
std::string xml_directory;

extern "C" void clean_up()
{
  if (!wss.str().empty()) {
    metautils::log_warning(wss.str(),"hdf2xml",USER);
  }
  if (!myerror.empty()) {
    metautils::log_error(myerror,"hdf2xml",USER);
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","hdf2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

void grid_initialize()
{
  if (grid_table == nullptr) {
    grid_table.reset(new my::map<gatherxml::markup::GrML::GridEntry>);
    gentry.reset(new gatherxml::markup::GrML::GridEntry);
    lentry.reset(new gatherxml::markup::GrML::LevelEntry);
    param_entry.reset(new gatherxml::markup::GrML::ParameterEntry);
  }
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
    if (metautils::args.data_format == "quikscathdf4") {
	scan_quikscat_hdf4_file(istream);
    }
    istream.close();
  }
}

void extract_from_hdf5_variable_attribute(my::map<InputHDF5Stream::Attribute>& attributes,NetCDFVariableAttributeData& nc_attribute_data)
{
  nc_attribute_data.long_name="";
  nc_attribute_data.units="";
  nc_attribute_data.cf_keyword="";
  nc_attribute_data.missing_value.clear();
  for (const auto& key : attributes.keys()) {
    InputHDF5Stream::Attribute attr;
    attributes.found(key,attr);
    if (attr.key == "long_name") {
	nc_attribute_data.long_name.assign(reinterpret_cast<char *>(attr.value.array));
	strutils::trim(nc_attribute_data.long_name);
    }
    else if (attr.key == "units") {
	nc_attribute_data.units.assign(reinterpret_cast<char *>(attr.value.array));
	strutils::trim(nc_attribute_data.units);
    }
    else if (attr.key == "standard_name") {
	nc_attribute_data.cf_keyword.assign(reinterpret_cast<char *>(attr.value.array));
	strutils::trim(nc_attribute_data.cf_keyword);
    }
    else if (attr.key == "_FillValue" || attr.key == "missing_value") {
	nc_attribute_data.missing_value=attr.value;
    }
  }
}

bool found_missing(const double& time,HDF5::DataArray::Type time_type,const InputHDF5Stream::DataValue *time_missing_value,const double& var_value,HDF5::DataArray::Type var_type,const InputHDF5Stream::DataValue& var_missing_value)
{
  static const std::string THIS_FUNC=__func__;
  bool missing=false;
  if (time_missing_value != nullptr && time_missing_value->_class_ != -1) {
    switch (time_type) {
	case (HDF5::DataArray::Type::BYTE): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<unsigned char *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::SHORT): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<short *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::INT): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<int *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::LONG_LONG): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<long long *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::FLOAT): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<float *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::DOUBLE): {
	  if (floatutils::myequalf(time,*(reinterpret_cast<double *>(time_missing_value->array)))) {
	    missing=true;
	  }
	  break;
	}
	default: {
	  metautils::log_error(THIS_FUNC+"() returned error: can't check times of type "+strutils::itos(static_cast<int>(time_type)),"hdf2xml",USER);
	}
    }
  }
  if (missing) {
    return true;
  }
  if (var_missing_value.size > 0) {
    switch (var_type) {
	case (HDF5::DataArray::Type::BYTE): {
	  if (floatutils::myequalf(var_value,static_cast<int>(*(reinterpret_cast<unsigned char *>(var_missing_value.array))))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::SHORT): {
	  if (floatutils::myequalf(var_value,*(reinterpret_cast<short *>(var_missing_value.array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::INT): {
	  if (floatutils::myequalf(var_value,*(reinterpret_cast<int *>(var_missing_value.array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::LONG_LONG): {
	  if (floatutils::myequalf(var_value,*(reinterpret_cast<long long *>(var_missing_value.array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::FLOAT): {
	  if (floatutils::myequalf(var_value,*(reinterpret_cast<float *>(var_missing_value.array)))) {
	    missing=true;
	  }
	  break;
	}
	case (HDF5::DataArray::Type::DOUBLE): {
	  if (floatutils::myequalf(var_value,*(reinterpret_cast<double *>(var_missing_value.array)))) {
	    missing=true;
	  }
	  break;
	}
	default: {
	  metautils::log_error(THIS_FUNC+"() returned error: can't check variables of type "+strutils::itos(static_cast<int>(var_type)),"hdf2xml",USER);
	}
    }
  }
  return missing;
}

std::string ispd_hdf5_platform_type(const std::tuple<std::string,std::string,float,float,short,short,char,bool>& library_entry)
{
  std::string id,ispd_id;
  float lat,lon;
  short plat_type,isrc;
  char csrc;
  bool already_counted;
  std::tie(id,ispd_id,lat,lon,plat_type,isrc,csrc,already_counted)=library_entry;
  if (plat_type == -1) {
    return "land_station";
  }
  else {
    switch (plat_type) {
	case 0:
	case 1:
	case 5:
	case 2002: {
	  return "roving_ship";
	}
	case 2:
	case 3:
	case 1007: {
	  return "ocean_station";
	}
	case 4: {
	  return "lightship";
	}
	case 6: {
	  return "moored_buoy";
	}
	case 7:
	case 1009:
	case 2007: {
	  if (ispd_id == "008000" || ispd_id == "008001") {
	    return "unknown";
	  }
	  else {
	    return "drifting_buoy";
	  }
	}
	case 9: {
	  return "ice_station";
	}
	case 10:
	case 11:
	case 12:
	case 17:
	case 18:
	case 19:
	case 20:
	case 21: {
	  return "oceanographic";
	}
	case 13: {
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
	case 2040: {
	  return "land_station";
	}
	case 14: {
	  return "coastal_station";
	}
	case 15: {
	  return "fixed_ocean_platform";
	}
	case 2030: {
	  return "bogus";
	}
	case 1003:
	case 1006: {
	  if ((ispd_id == "001000" && ((csrc >= '2' && csrc <= '5') || (csrc >= 'A' && csrc <= 'H') || csrc == 'N')) || ispd_id == "001003" || (ispd_id == "001005" && plat_type == 1003) || ispd_id == "003002" || ispd_id == "003004" || ispd_id == "003005" || ispd_id == "003006" || ispd_id == "003007" || ispd_id == "003007" || ispd_id == "003008" || ispd_id == "003009" || ispd_id == "003011" || ispd_id == "003014" || ispd_id == "003015" || ispd_id == "003021" || ispd_id == "003022" || ispd_id == "003026" || ispd_id == "004000" || ispd_id == "004003") {
	    return "land_station";
	  }
	  else if (ispd_id == "002000") {
	    if (id.length() == 5) {
		if (std::stoi(id) < 99000) {
		  return "land_station";
		}
		else if (std::stoi(id) < 99100) {
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
	  else if (ispd_id == "002001" || (ispd_id == "001005" && plat_type == 1006)) {
		return "unknown";
	  }
	  else if (ispd_id == "003010") {
	    std::deque<std::string> sp=strutils::split(id,"-");
	    if (sp.size() == 2 && sp[1].length() == 5 && strutils::is_numeric(sp[1])) {
		return "land_station";
	    }
	  }
	  else if (ispd_id >= "010000" && ispd_id <= "019999") {
	    if (plat_type == 1006 && id.length() == 5 && strutils::is_numeric(id)) {
		if (id < "99000") {
		  return "land_station";
		}
		else if (id >= "99200" && id <= "99299") {
		  return "drifting_buoy";
		}
	    }
	    else {
		return "unknown";
	    }
	  }
	  else {
	    wss << "unknown platform type (1) for station '"+id+"' "+strutils::ftos(lat,4)+" "+strutils::ftos(lon,4)+" "+ispd_id+" "+strutils::itos(plat_type)+" "+strutils::itos(isrc)+" '"+std::string(1,csrc)+"'" << std::endl;
	    return "";
	  }
	}
	default: {
	  wss << "unknown platform type (2) for station '"+id+"' "+strutils::ftos(lat,4)+" "+strutils::ftos(lon,4)+" "+ispd_id+" "+strutils::itos(plat_type)+" "+strutils::itos(isrc)+" '"+std::string(1,csrc)+"'" << std::endl;
	  return "";
	}
    }
  }
}

std::string ispd_hdf5_id_entry(const std::tuple<std::string,std::string,float,float,short,short,char,bool>& library_entry,std::string platform_type,DateTime& dt)
{
  std::string id,ispd_id;
  float lat,lon;
  short plat_type,isrc;
  char csrc;
  bool already_counted;
  std::tie(id,ispd_id,lat,lon,plat_type,isrc,csrc,already_counted)=library_entry;
  std::string ientry_key;
  if (isrc > 0 && !id.empty() && (id)[1] == ' ') {
    auto parts=strutils::split(id);
    ientry_key=platform_type+"[!]";
    switch (std::stoi(parts[0])) {
	case 2: {
	  ientry_key+="generic[!]"+parts[1];
	  break;
	}
	case 3: {
	  ientry_key+="WMO[!]"+parts[1];
	  break;
	}
	case 5: {
	  ientry_key+="NDBC[!]"+parts[1];
	  break;
	}
	default: {
	  ientry_key+="[!]"+id;
	}
    }
  }
  else if (ispd_id == "001000") {
    if ((id)[6] == '-') {
	auto parts=strutils::split(id,"-");
	if (parts[0] != "999999") {
	  ientry_key=platform_type+"[!]WMO+6[!]"+parts[0];
	}
	else {
	  if (parts[1] != "99999") {
	    ientry_key=platform_type+"[!]WBAN[!]"+parts[1];
	  }
	  else {
	    wss << "unknown ID type (1) for station '"+id+"' "+strutils::ftos(lat,4)+" "+strutils::ftos(lon,4)+" "+ispd_id+" "+strutils::itos(plat_type)+" "+strutils::itos(isrc)+" '"+std::string(1,csrc)+"'" << std::endl;
	  }
	}
    }
    else {
	wss << "unknown ID type (2) for station '"+id+"' "+strutils::ftos(lat,4)+" "+strutils::ftos(lon,4)+" "+ispd_id+" "+strutils::itos(plat_type)+" "+strutils::itos(isrc)+" '"+std::string(1,csrc)+"'" << std::endl;
    }
  }
  else if (ispd_id == "001002") {
    ientry_key=platform_type+"[!]WBAN[!]"+id;
  }
  else if (ispd_id == "001003") {
    ientry_key=platform_type+"[!]RUSSIA[!]"+id;
  }
  else if (ispd_id == "001005" || ispd_id == "001006") {
    if (plat_type >= 1001 && plat_type <= 1003 && strutils::is_numeric(id)) {
	if (id.length() == 5) {
	  ientry_key=platform_type+"[!]WMO[!]"+id;
	}
	else if (id.length() == 6) {
	  ientry_key=platform_type+"[!]WMO+6[!]"+id;
	}
    }
    else if (plat_type == 1002 && !strutils::is_numeric(id)) {
	ientry_key=platform_type+"[!]NAME[!]"+id;
    }
    else if (id == "999999999999") {
	ientry_key=platform_type+"[!]unknown[!]"+id;
    }
  }
  else if ((ispd_id == "001007" && plat_type == 1001) || ispd_id == "002000" || ispd_id == "003002" || ispd_id == "003008" || ispd_id == "003015" || ispd_id == "004000" || ispd_id == "004001" || ispd_id == "004003") {
    ientry_key=platform_type+"[!]WMO[!]"+id;
  }
  else if (((ispd_id == "001011" && plat_type == 1002) || ispd_id == "001007" || ispd_id == "004002" || ispd_id == "004004") && !strutils::is_numeric(id)) {
    ientry_key=platform_type+"[!]NAME[!]"+id;
  }
  else if (ispd_id == "001012" && plat_type == 1002) {
    ientry_key=platform_type+"[!]COOP[!]"+id;
  }
  else if (ispd_id == "002001") {
    if (strutils::is_numeric(id)) {
	if (id.length() == 5) {
	  if (dt.year() <= 1948) {
	    ientry_key=platform_type+"[!]WBAN[!]"+id;
	  }
	  else {
	    ientry_key=platform_type+"[!]WMO[!]"+id;
	  }
	}
	else {
	  ientry_key=platform_type+"[!]unknown[!]"+id;
	}
    }
    else {
	ientry_key=platform_type+"[!]callSign[!]"+id;
    }
  }
  else if (ispd_id == "003002" && strutils::is_numeric(id)) {
    if (id.length() == 5) {
	ientry_key=platform_type+"[!]WMO[!]"+id;
    }
    else if (id.length() == 6) {
	ientry_key=platform_type+"[!]WMO+6[!]"+id;
    }
  }
  else if (ispd_id == "003004") {
    ientry_key=platform_type+"[!]CANADA[!]"+id;
  }
  else if ((ispd_id == "003006" || ispd_id == "003030") && plat_type == 1006) {
    ientry_key=platform_type+"[!]AUSTRALIA[!]"+id;
  }
  else if (ispd_id == "003009" && plat_type == 1006) {
    ientry_key=platform_type+"[!]SPAIN[!]"+id;
  }
  else if ((ispd_id == "003010" || ispd_id == "003011") && plat_type == 1003) {
    auto parts=strutils::split(id,"-");
    if (parts.size() == 2 && parts[1].length() == 5 && strutils::is_numeric(parts[1])) {
	ientry_key=platform_type+"[!]WMO[!]"+id;
    }
  }
  else if (ispd_id == "003012" && plat_type == 1002) {
    ientry_key=platform_type+"[!]SWITZERLAND[!]"+id;
  }
  else if (ispd_id == "003013" && (plat_type == 1002 || plat_type == 1003)) {
    ientry_key=platform_type+"[!]SOUTHAFRICA[!]"+id;
  }
  else if (ispd_id == "003014" && plat_type == 1003) {
    ientry_key=platform_type+"[!]NORWAY[!]"+id;
  }
  else if (ispd_id == "003016" && plat_type == 1002) {
    ientry_key=platform_type+"[!]PORTUGAL[!]"+id;
  }
  else if ((ispd_id == "003019" || ispd_id == "003100") && plat_type == 1002 && !id.empty()) {
    ientry_key=platform_type+"[!]NEWZEALAND[!]"+id;
  }
  else if ((ispd_id == "003007" || ispd_id == "003021" || ispd_id == "003022" || ispd_id == "003023" || ispd_id == "003025" || ispd_id == "003101" || ispd_id == "004005" || ispd_id == "006000") && plat_type == 1002 && !id.empty()) {
    ientry_key=platform_type+"[!]NAME[!]"+id;
  }
  else if (ispd_id == "003026" && plat_type == 1006 && id.length() == 5) {
    ientry_key=platform_type+"[!]WMO[!]"+id;
  }
  else if (ispd_id == "003030" && plat_type == 2001) {
    if (strutils::is_numeric(id)) {
	ientry_key=platform_type+"[!]AUSTRALIA[!]"+id;
    }
    else {
	ientry_key=platform_type+"[!]unknown[!]"+id;
    }
  }
  else if (ispd_id == "008000" || ispd_id == "008001") {
    ientry_key=platform_type+"[!]TropicalCyclone[!]"+id;
  }
  else if (ispd_id >= "010000" && ispd_id <= "019999") {
    if (id.length() == 5 && strutils::is_numeric(id)) {
	ientry_key=platform_type+"[!]WMO[!]"+id;
    }
    else {
	ientry_key=platform_type+"[!]unknown[!]"+id;
    }
  }
  else if (id == "999999999999" || (!id.empty() && (ispd_id == "001013" || ispd_id == "001014" || ispd_id == "001018" || ispd_id == "003005" || ispd_id == "003020" || ispd_id == "005000"))) {
    ientry_key=platform_type+"[!]unknown[!]"+id;
  }
  if (ientry_key.empty()) {
    wss << "unknown ID type (3) for station '"+id+"' "+strutils::ftos(lat,4)+" "+strutils::ftos(lon,4)+" "+ispd_id+" "+strutils::itos(plat_type)+" "+strutils::itos(isrc)+" '"+std::string(1,csrc)+"'" << std::endl;
  }
  return ientry_key;
}

void scan_ispd_hdf5_file(InputHDF5Stream& istream,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data)
{
  static const std::string THIS_FUNC=__func__;
  InputHDF5Stream::DataValue ts_val,uon_val,id_val,lat_val,lon_val;
// load the station library
  auto ds=istream.dataset("/Data/SpatialTemporalLocation/SpatialTemporalLocation");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    myerror="unable to locate spatial/temporal information";
    exit(1);
  }
  std::unordered_map<std::string,std::tuple<std::string,std::string,float,float,short,short,char,bool>> stn_library;
  InputHDF5Stream::CompoundDatatype cpd;
  HDF5::decode_compound_datatype(ds->datatype,cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m=0,l=0; m < ds->data.sizes.front(); m++) {
	ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	std::string key=reinterpret_cast<char *>(ts_val.get());
	uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	if (!key.empty() && (stn_library.find(key) == stn_library.end())) {
	  id_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
	  std::string id=reinterpret_cast<char *>(id_val.get());
	  strutils::trim(id);
	  lat_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[13].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[13].datatype,ds->dataspace);
	  auto lat=*(reinterpret_cast<float *>(lat_val.get()));
	  lon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[14].datatype,ds->dataspace);
	  auto lon=*(reinterpret_cast<float *>(lon_val.get()));
	  if (lon > 180.) {
	    lon-=360.;
	  }
	  stn_library.emplace(key,std::make_tuple(id,"",lat,lon,-1,-1,'9',false));
	}
	l+=ds->data.size_of_element;
    }
  }
// load the ICOADS platform types
  if ( (ds=istream.dataset("/SupplementalData/Tracking/ICOADS/TrackingICOADS")) != NULL && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue plat_val;
    HDF5::decode_compound_datatype(ds->datatype,cpd);
    for (const auto& chunk : ds->data.chunks) {
	for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	  std::string key=reinterpret_cast<char *>(ts_val.get());
	  if (!key.empty()) {
	    uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	    key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	    auto entry=stn_library.find(key);
	    if (entry != stn_library.end()) {
		id_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
		std::get<5>(entry->second)=*(reinterpret_cast<int *>(id_val.get()));
		plat_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[4].datatype,ds->dataspace);
		std::get<4>(entry->second)=*(reinterpret_cast<int *>(plat_val.get()));
	    }
	    else {
		metautils::log_error("no entry for '"+key+"' in station library","hdf2xml",USER);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load observation types for IDs that don't already have a platform type
  if ( (ds=istream.dataset("/Data/Observations/ObservationTypes")) != NULL && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue coll_val;
    HDF5::decode_compound_datatype(ds->datatype,cpd);
    for (const auto& chunk : ds->data.chunks) {
	for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	  std::string key=reinterpret_cast<char *>(ts_val.get());
	  if (!key.empty()) {
	    uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	    key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	    auto entry=stn_library.find(key);
	    if (entry != stn_library.end()) {
		if (std::get<4>(entry->second) < 0) {
		  id_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
		  std::get<4>(entry->second)=1000+*(reinterpret_cast<int *>(id_val.get()));
		}
		coll_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[4].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[4].datatype,ds->dataspace);
		std::string ispd_id=reinterpret_cast<char *>(coll_val.get());
		strutils::replace_all(ispd_id," ","0");
		std::get<1>(entry->second)=ispd_id;
	    }
	    else {
		metautils::log_error("no entry for '"+key+"' in station library","hdf2xml",USER);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  if ( (ds=istream.dataset("/SupplementalData/Tracking/Land/TrackingLand")) != NULL && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue src_flag_val,rpt_type_val;
    HDF5::decode_compound_datatype(ds->datatype,cpd);
    for (const auto& chunk : ds->data.chunks) {
	for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	  std::string key=reinterpret_cast<char *>(ts_val.get());
	  if (!key.empty()) {
	    uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	    key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	    auto entry=stn_library.find(key);
	    if (entry != stn_library.end()) {
		src_flag_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
		std::get<6>(entry->second)=(reinterpret_cast<char *>(src_flag_val.get()))[0];
		rpt_type_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[3].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[3].datatype,ds->dataspace);
		std::string rpt_type=reinterpret_cast<char *>(rpt_type_val.get());
		if (rpt_type == "FM-12") {
		  std::get<4>(entry->second)=2001;
		}
		else if (rpt_type == "FM-13") {
		  std::get<4>(entry->second)=2002;
		}
		else if (rpt_type == "FM-14") {
		  std::get<4>(entry->second)=2003;
		}
		else if (rpt_type == "FM-15") {
		  std::get<4>(entry->second)=2004;
		}
		else if (rpt_type == "FM-16") {
		  std::get<4>(entry->second)=2005;
		}
		else if (rpt_type == "FM-18") {
		  std::get<4>(entry->second)=2007;
		}
		else if (rpt_type == "  SAO") {
		  std::get<4>(entry->second)=2010;
		}
		else if (rpt_type == " AOSP") {
		  std::get<4>(entry->second)=2011;
		}
		else if (rpt_type == " AERO") {
		  std::get<4>(entry->second)=2012;
		}
		else if (rpt_type == " AUTO") {
		  std::get<4>(entry->second)=2013;
		}
		else if (rpt_type == "SY-AE") {
		  std::get<4>(entry->second)=2020;
		}
		else if (rpt_type == "SY-SA") {
		  std::get<4>(entry->second)=2021;
		}
		else if (rpt_type == "SY-MT") {
		  std::get<4>(entry->second)=2022;
		}
		else if (rpt_type == "SY-AU") {
		  std::get<4>(entry->second)=2023;
		}
		else if (rpt_type == "SA-AU") {
		  std::get<4>(entry->second)=2024;
		}
		else if (rpt_type == "S-S-A") {
		  std::get<4>(entry->second)=2025;
		}
		else if (rpt_type == "BOGUS") {
		  std::get<4>(entry->second)=2030;
		}
		else if (rpt_type == "SMARS") {
		  std::get<4>(entry->second)=2031;
		}
		else if (rpt_type == "  SOD") {
		  std::get<4>(entry->second)=2040;
		}
	    }
	    else {
		metautils::log_error("no entry for '"+key+"' in station library","hdf2xml",USER);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
// load tropical storm IDs
  if ( (ds=istream.dataset("/SupplementalData/Misc/TropicalStorms/StormID")) != NULL && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue storm_id_val;
    HDF5::decode_compound_datatype(ds->datatype,cpd);
    for (const auto& chunk : ds->data.chunks) {
	for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	  std::string key=reinterpret_cast<char *>(ts_val.get());
	  if (!key.empty()) {
	    uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	    key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	    auto entry=stn_library.find(key);
	    if (entry != stn_library.end()) {
		storm_id_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
		std::string id=reinterpret_cast<char *>(storm_id_val.get());
		strutils::trim(id);
		std::get<0>(entry->second)=id;
	    }
	    else {
		metautils::log_error("no entry for '"+key+"' in station library","hdf2xml",USER);
	    }
	  }
	  l+=ds->data.size_of_element;
	}
    }
  }
  InputHDF5Stream::DataValue slp_val,stnp_val;
// scan the observations
  if ( (ds=istream.dataset("/Data/Observations/Observations")) == NULL || ds->datatype.class_ != 6) {
    myerror="unable to locate observations";
    exit(1);
  }
  HDF5::decode_compound_datatype(ds->datatype,cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	std::string timestamp=reinterpret_cast<char *>(ts_val.get());
	strutils::trim(timestamp);
	if (!timestamp.empty()) {
	  auto key=timestamp;
	  uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[1].datatype,ds->dataspace);
	  key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	  auto entry=stn_library.find(key);
	  if (entry != stn_library.end()) {
	    if (std::regex_search(timestamp,std::regex("99$"))) {
		strutils::chop(timestamp,2);
// patch for some bad timestamps
		if (std::regex_search(timestamp,std::regex(" $"))) {
		  strutils::chop(timestamp);
		  timestamp.insert(8,"0");
		}
		timestamp+="00";
	    }
	    DateTime dt(std::stoll(timestamp)*100);
	    auto platform_type=ispd_hdf5_platform_type(entry->second);
	    if (!platform_type.empty()) {
		gatherxml::markup::ObML::IDEntry ientry;
		std::vector<double> check_values;
		ientry.key=ispd_hdf5_id_entry(entry->second,platform_type,dt);
		if (!ientry.key.empty()) {
// SLP
		  slp_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
		  if (slp_val._class_ != 1) {
		    metautils::log_error("observed SLP is not a floating point number for '"+ientry.key+"'","hdf2xml",USER);
		  }
		  if (slp_val.precision_ == 32) {
		    check_values.emplace_back(*(reinterpret_cast<float *>(slp_val.get())));
		  }
		  else if (slp_val.precision_ == 64) {
		    check_values.emplace_back(*(reinterpret_cast<double *>(slp_val.get())));
		  }
		  else {
		    metautils::log_error("bad precision ("+strutils::itos(slp_val.precision_)+") for SLP","hdf2xml",USER);
		  }
// STN P
		  stnp_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[5].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[5].datatype,ds->dataspace);
		  if (stnp_val._class_ != 1) {
		    metautils::log_error("observed STN P is not a floating point number for '"+ientry.key+"'","hdf2xml",USER);
		  }
		  if (stnp_val.precision_ == 32) {
		    check_values.emplace_back(*(reinterpret_cast<float *>(stnp_val.get())));
		  }
		  else if (stnp_val.precision_ == 64) {
		    check_values.emplace_back(*(reinterpret_cast<double *>(stnp_val.get())));
		  }
		  else {
		    metautils::log_error("bad precision ("+strutils::itos(stnp_val.precision_)+") for SLP","hdf2xml",USER);
		  }
		  if ((check_values[0] >= 860. && check_values[0] <= 1090.) || (check_values[1] >= 400. && check_values[1] <= 1090.)) {
		    if (check_values[0] < 9999.9) {
			if (!obs_data.added_to_ids("surface",ientry,"SLP","",std::get<2>(entry->second),std::get<3>(entry->second),std::stoll(timestamp),&dt)) {
			  metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+" when adding ID "+ientry.key,"hdf2xml",USER);
			}
			std::get<7>(entry->second)=true;
			++scan_data.num_not_missing;
		    }
		    if (check_values[1] < 9999.9) {
			if (!obs_data.added_to_ids("surface",ientry,"STNP","",std::get<2>(entry->second),std::get<3>(entry->second),std::stoll(timestamp),&dt)) {
			  metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+" when adding ID "+ientry.key,"hdf2xml",USER);
			}
			std::get<7>(entry->second)=true;
			++scan_data.num_not_missing;
		    }
		    if (!obs_data.added_to_platforms("surface",platform_type,std::get<2>(entry->second),std::get<3>(entry->second))) {
			metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+" when adding platform "+platform_type,"hdf2xml",USER);
		    }
		  }
		}
	    }
	  }
	  else {
	    metautils::log_error("no entry for '"+key+"' in station library","hdf2xml",USER);
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
    InputHDF5Stream::DataValue p_val,ens_fg_val,ens_p_val;
    HDF5::decode_compound_datatype(ds->datatype,cpd);
    for (const auto& chunk : ds->data.chunks) {
	for (int m=0,l=0; m < ds->data.sizes.front(); ++m) {
	  ts_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[0].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[0].datatype,ds->dataspace);
	  std::string timestamp=reinterpret_cast<char *>(ts_val.get());
	  strutils::trim(timestamp);
	  if (!timestamp.empty()) {
	    auto key=timestamp;
	    uon_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[1].byte_offset],istream.size_of_offsets(),istream.size_of_offsets(),cpd.members[1].datatype,ds->dataspace);
	    key+=std::string(reinterpret_cast<char *>(uon_val.get()));
	    auto entry=stn_library.find(key);
	    if (entry != stn_library.end()) {
		if (!timestamp.empty()) {
		  if (std::regex_search(timestamp,std::regex("99$"))) {
		    strutils::chop(timestamp,2);
		    timestamp+="00";
		  }
		  DateTime dt(std::stoll(timestamp)*100);
		  auto platform_type=ispd_hdf5_platform_type(entry->second);
		  if (!platform_type.empty()) {
		    gatherxml::markup::ObML::IDEntry ientry;
		    ientry.key=ispd_hdf5_id_entry(entry->second,platform_type,dt);
		    if (!ientry.key.empty()) {
			std::vector<double> check_values;
			p_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[2].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[2].datatype,ds->dataspace);
			if (p_val._class_ != 1) {
			  metautils::log_error("modified observed pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",USER);
			}
			if (p_val.precision_ == 32) {
			  check_values.emplace_back(*(reinterpret_cast<float *>(p_val.get())));
			}
			else if (p_val.precision_ == 64) {
			  check_values.emplace_back(*(reinterpret_cast<double *>(p_val.get())));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(p_val.precision_)+") for modified observed pressure","hdf2xml",USER);
			}
			ens_fg_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[11].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[11].datatype,ds->dataspace);
			if (ens_fg_val._class_ != 1) {
			  metautils::log_error("ensemble first guess pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",USER);
			}
			if (ens_fg_val.precision_ == 32) {
			  check_values.emplace_back(*(reinterpret_cast<float *>(ens_fg_val.get())));
			}
			else if (ens_fg_val.precision_ == 64) {
			  check_values.emplace_back(*(reinterpret_cast<double *>(ens_fg_val.get())));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(ens_fg_val.precision_)+") for ensemble first guess pressure","hdf2xml",USER);
			}
			ens_p_val.set(*istream.file_stream(),&chunk.buffer[l+cpd.members[14].byte_offset],istream.size_of_offsets(),istream.size_of_lengths(),cpd.members[14].datatype,ds->dataspace);
			if (ens_p_val._class_ != 1) {
			  metautils::log_error("ensemble analysis pressure is not a floating point number for '"+ientry.key+"'","hdf2xml",USER);
			}
			if (ens_p_val.precision_ == 32) {
			  check_values.emplace_back(*(reinterpret_cast<float *>(ens_p_val.get())));
			}
			else if (ens_p_val.precision_ == 64) {
			  check_values.emplace_back(*(reinterpret_cast<double *>(ens_p_val.get())));
			}
			else {
			  metautils::log_error("bad precision ("+strutils::itos(ens_p_val.precision_)+") for ensemble analysis pressure","hdf2xml",USER);
			}
			if ((check_values[0] >= 400. && check_values[0] <= 1090.) || (check_values[1] >= 400. && check_values[1] <= 1090.) || (check_values[2] >= 400. && check_values[2] <= 1090.)) {
			  if (!obs_data.added_to_ids("surface",ientry,"Feedback","",std::get<2>(entry->second),std::get<3>(entry->second),std::stoll(timestamp),&dt)) {
			    metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+" when adding ID "+ientry.key,"hdf2xml",USER);
			  }
			  ++scan_data.num_not_missing;
			  std::get<7>(entry->second)=true;
			  if (!obs_data.added_to_platforms("surface",platform_type,std::get<2>(entry->second),std::get<3>(entry->second))) {
			    metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+" when adding platform "+platform_type,"hdf2xml",USER);
			  }
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
  scan_data.write_type=ScanData::ObML_type;
}

void scan_usarray_transportable_hdf5_file(InputHDF5Stream& istream,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data)
{
  obs_data.set_track_unique_observations(false);
// load the pressure dataset
  auto ds=istream.dataset("/obsdata/presdata");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    myerror="unable to locate the pressure dataset";
    exit(1);
  }
  HDF5::DataArray times,stnids,pres;
  times.fill(istream,*ds,0);
  if (times.type != HDF5::DataArray::Type::LONG_LONG) {
     metautils::log_error("expected the timestamps to be 'long long' but got "+strutils::itos(static_cast<int>(times.type)),"hdf2xml",USER);
  }
  stnids.fill(istream,*ds,1);
  if (stnids.type != HDF5::DataArray::Type::SHORT) {
     metautils::log_error("expected the numeric station IDs to be 'short' but got "+strutils::itos(static_cast<int>(stnids.type)),"hdf2xml",USER);
  }
  pres.fill(istream,*ds,2);
  if (pres.type != HDF5::DataArray::Type::FLOAT) {
     metautils::log_error("expected the pressures to be 'float' but got "+strutils::itos(static_cast<int>(pres.type)),"hdf2xml",USER);
  }
  int num_values=0;
  float pres_miss_val=3.e48;
  short numeric_id=-1;
  gatherxml::markup::ObML::IDEntry ientry;
  metautils::StringEntry se;
  std::string platform_type,datatype;
  float lat=-1.e38,lon=-1.e38;
  for (const auto& key : ds->attributes.keys()) {
    InputHDF5Stream::Attribute attr;
    ds->attributes.found(key,attr);
    if (key == "NROWS") {
	num_values=*(reinterpret_cast<int *>(attr.value.array));
    }
    else if (key == "LATITUDE_DDEG") {
	lat=*(reinterpret_cast<float *>(attr.value.array));
    }
    else if (key == "LONGITUDE_DDEG") {
	lon=*(reinterpret_cast<float *>(attr.value.array));
    }
    else if (key == "CHAR_STATION_ID") {
	if (platform_type.empty()) {
	  platform_type="land_station";
	  ientry.key.assign(reinterpret_cast<char *>(attr.value.array));
	  ientry.key.insert(0,platform_type+"[!]USArray[!]TA.");
	}
	else {
	  metautils::log_error("multiple station IDs not expected","hdf2xml",USER);
	}
    }
    else if (key == "NUMERIC_STATION_ID") {
	numeric_id=*(reinterpret_cast<short *>(attr.value.array));
    }
    else if (key == "FIELD_2_NAME") {
	datatype.assign(reinterpret_cast<char *>(attr.value.array));
    }
    else if (key == "FIELD_2_FILL") {
	pres_miss_val=*(reinterpret_cast<float *>(attr.value.array));
    }
    else if (key == "FIELD_2_DESCRIPTION") {
	se.key.assign(reinterpret_cast<char *>(attr.value.array));
    }
  }
  if (platform_type.empty()) {
    metautils::log_error("unable to get the station ID","hdf2xml",USER);
  }
  if (lat == -1.e38) {
    metautils::log_error("unable to get the station latitude","hdf2xml",USER);
  }
  if (lon == -1.e38) {
    metautils::log_error("unable to get the station longitude","hdf2xml",USER);
  }
  if (se.key.empty()) {
    metautils::log_error("unable to get title for the data value","hdf2xml",USER);
  }
  if (datatype.empty()) {
    metautils::log_error("unable to get the name of the data value","hdf2xml",USER);
  }
  if (!obs_data.added_to_platforms("surface",platform_type,lat,lon)) {
    metautils::log_error("scan_usarray_transportable_hdf5_file() returned error: '"+myerror+" when adding platform "+platform_type,"hdf2xml",USER);
  }
  DateTime epoch(1970,1,1,0,0);
  for (auto n=0; n < num_values; ++n) {
    if (stnids.short_value(n) != numeric_id) {
	metautils::log_error("unexpected change in the numeric station ID","hdf2xml",USER);
    }
    if (pres.float_value(n) != pres_miss_val) {
	DateTime dt=epoch.seconds_added(times.long_long_value(n));
	if (!obs_data.added_to_ids("surface",ientry,datatype,"",lat,lon,times.long_long_value(n),&dt)) {
	  metautils::log_error("scan_usarray_transportable_hdf5_file() returned error: '"+myerror+" when adding ID "+ientry.key,"hdf2xml",USER);
	}
	++scan_data.num_not_missing;
    }
  }
  gatherxml::markup::ObML::DataTypeEntry dte;
  ientry.data->data_types_table.found(datatype,dte);
  ientry.data->nsteps=dte.data->nsteps=scan_data.num_not_missing;
  scan_data.map_name=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds"+metautils::args.dsnum+".xml",scan_data.tdir->name());
  scan_data.found_map=(!scan_data.map_name.empty());
  se.key=datatype+"<!>"+se.key+"<!>Hz";
  scan_data.varlist.emplace_back(se.key);
  if (scan_data.found_map && !scan_data.var_changes_table.found(datatype,se)) {
    se.key=datatype;
    scan_data.var_changes_table.insert(se);
  }
  scan_data.write_type=ScanData::ObML_type;
}

std::string gridded_time_method(const std::shared_ptr<InputHDF5Stream::Dataset> ds,const GridCoordinates& gcoords)
{
  InputHDF5Stream::Attribute attr;
  std::string time_method;

  if (ds->attributes.found("cell_methods",attr) && attr.value._class_ == 3) {
    time_method=metautils::NcTime::time_method_from_cell_methods(reinterpret_cast<char *>(attr.value.array),gcoords.valid_time.id);
    if (time_method[0] == '!') {
	metautils::log_error("cell method '"+time_method.substr(1)+"' is not valid CF","hdf2xml",USER);
    }
    else {
	return time_method;
    }
  }
  return "";
}

void add_gridded_time_range(std::string key_start,my::map<metautils::StringEntry>& gentry_table,const metautils::NcTime::TimeRangeEntry& tre,const metautils::NcTime::TimeData& time_data,const GridCoordinates& gcoords,InputHDF5Stream& istream)
{
  InvEntry ie;
  std::string gentry_key;
  bool found_no_method=false;
  auto vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    InputHDF5Stream::Attribute attr;
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && (attr.value.dim_sizes[0] > 2 || (attr.value.dim_sizes[0] == 2 && gcoords.valid_time.data_array.num_values == 1))) {
	auto time_method=gridded_time_method(var.dataset,gcoords);
	if (time_method.empty()) {
	  found_no_method=true;
	}
	else {
	  std::string error;
	  ie.key=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,strutils::capitalize(time_method),error);
	  if (!error.empty()) {
	    metautils::log_error(error,"hdf2xml",USER);
	  }
	  gentry_key=key_start+ie.key;
	  metautils::StringEntry se;
	  if (!gentry_table.found(gentry_key,se)) {
	    se.key=gentry_key;
	    gentry_table.insert(se);
	  }
	}
    }
  }
  if (found_no_method) {
    std::string error;
    ie.key=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,"",error);
    if (!error.empty()) {
	metautils::log_error(error,"hdf2xml",USER);
    }
    gentry_key=key_start+ie.key;
    metautils::StringEntry se;
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

void add_gridded_lat_lon_keys(my::map<metautils::StringEntry>& gentry_table,Grid::GridDimensions dim,Grid::GridDefinition def,const metautils::NcTime::TimeRangeEntry& tre,const metautils::NcTime::TimeData& time_data,const GridCoordinates& gcoords,InputHDF5Stream& istream)
{
  std::string key_start;
  switch (def.type) {
    case Grid::latitudeLongitudeType: {
	key_start=strutils::itos(def.type);
	if (def.is_cell) {
	  key_start.push_back('C');
	}
	key_start+="<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.elatitude,3)+"<!>"+strutils::ftos(def.elongitude,3)+"<!>"+strutils::ftos(def.loincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
	add_gridded_time_range(key_start,gentry_table,tre,time_data,gcoords,istream);
	break;
    }
    case Grid::polarStereographicType: {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>";
	add_gridded_time_range(key_start,gentry_table,tre,time_data,gcoords,istream);
	break;
    }
    case Grid::lambertConformalType: {
	key_start=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  key_start+="N";
	}
	else {
	  key_start+="S";
	}
	key_start+="<!>"+strutils::ftos(def.stdparallel1,3)+"<!>"+strutils::ftos(def.stdparallel2,3)+"<!>";
	add_gridded_time_range(key_start,gentry_table,tre,time_data,gcoords,istream);
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
    case 0: {
	switch (ds->data.size_of_element) {
	  case 4: {
	    value=(reinterpret_cast<int *>(data_array.values))[index];
	    break;
	  }
	  case 8: {
	    value=(reinterpret_cast<long long *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::log_error("unable to get value for fixed-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",USER);
	  }
	}
	break;
    }
    case 1: {
	switch (ds->data.size_of_element) {
	  case 4: {
	    value=(reinterpret_cast<float *>(data_array.values))[index];
	    break;
	  }
	  case 8: {
	    value=(reinterpret_cast<double *>(data_array.values))[index];
	    break;
	  }
	  default:
	  {
	    metautils::log_error("unable to get value for floating-point size "+strutils::itos(ds->data.size_of_element),"hdf2xml",USER);
	  }
	}
	break;
    }
    default:
    {
	metautils::log_error("unable to decode time from datatype class "+strutils::itos(ds->datatype.class_),"hdf2xml",USER);
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
    units=reinterpret_cast<char *>(attr.value.array);
  }
  if (description.empty()) {
    if ((var.dataset->attributes.found("description",attr) || var.dataset->attributes.found("Description",attr)) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.array);
    }
    else if ((var.dataset->attributes.found("comment",attr) || var.dataset->attributes.found("Comment",attr)) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.array);
    }
    else if (var.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
	description=reinterpret_cast<char *>(attr.value.array);
    }
  }
  if (var.dataset->attributes.found("standard_name",attr) && attr.value._class_ == 3) {
    standard_name=reinterpret_cast<char *>(attr.value.array);
  }
  sdum=var.key;
  strutils::trim(sdum);
  strutils::trim(description);
  strutils::trim(units);
  strutils::trim(standard_name);
  se.key=sdum+"<!>"+description+"<!>"+units+"<!>"+standard_name;
  if (!parameter_data.table.found(se.key,se)) {
    sdum=parameter_data.map.short_name(var.key);
    if (!scan_data.found_map || sdum.empty()) {
	parameter_data.table.insert(se);
	scan_data.varlist.emplace_back(se.key);
    }
    else {
	parameter_data.table.insert(se);
	scan_data.varlist.emplace_back(se.key);
	if (!scan_data.var_changes_table.found(var.key,se)) {
	  se.key=var.key;
	  scan_data.var_changes_table.insert(se);
	}
    }
  }
  param_entry->start_date_time=time_range.first_valid_datetime;
  param_entry->end_date_time=time_range.last_valid_datetime;
  param_entry->num_time_steps=num_steps;
  lentry->parameter_code_table.insert(*param_entry);
}

bool parameter_matches_dimensions(InputHDF5Stream& istream,InputHDF5Stream::Attribute& attr,const GridCoordinates& gcoords,std::string level_id)
{
  InputHDF5Stream::ReferenceEntry re[3];
  bool parameter_matches=false;
  auto off=4;
  size_t first=1;
  if (attr.value.dim_sizes[0] == 2 && gcoords.valid_time.data_array.num_values == 1) {
    first=0;
  }
  else {
    off+=attr.value.precision_+4;
  }
  if (gatherxml::verbose_operation) {
    std::cout << "      dimension names to check:" << std::endl;
  }
  for (size_t n=first,rcnt=0; n < attr.value.dim_sizes[0]; ++n,++rcnt) {
    if (!istream.reference_table_pointer()->found(HDF5::value(&attr.value.vlen.buffer[off],attr.value.precision_),re[rcnt])) {
	metautils::log_error("unable to dereference dimension reference","hdf2xml",USER);
    }
    if (gatherxml::verbose_operation) {
	std::cout << "       '" << re[rcnt].name << "'" << std::endl;
    }
    off+=attr.value.precision_+4;
  }
  switch (attr.value.dim_sizes[0]) {
    case 2:
    case 3: {
// data variables dimensioned [time, lat, lon] or [lat, lon] with scalar time
	if (level_id == "sfc" && (attr.value.dim_sizes[0] == 3 || gcoords.valid_time.data_array.num_values == 1)) {
	  if (re[0].name == gcoords.latitude.id && re[1].name == gcoords.longitude.id) {
// latitude and longitude are coordinate variables
	    parameter_matches=true;
	  }
	  else if (re[0].name != gcoords.latitude.id && re[1].name != gcoords.longitude.id) {
// check for auxiliary coordinate variables for latitude and longitude
	    auto lat_ds=istream.dataset("/"+gcoords.latitude.id);
	    auto lon_ds=istream.dataset("/"+gcoords.longitude.id);
	    if (lat_ds != nullptr && lon_ds != nullptr) {
		InputHDF5Stream::Attribute lat_dim_list;
		lat_ds->attributes.found("DIMENSION_LIST",lat_dim_list);
		std::stringstream lat_dims;
		lat_dim_list.value.print(lat_dims,istream.reference_table_pointer());
		InputHDF5Stream::Attribute lon_dim_list;
		lon_ds->attributes.found("DIMENSION_LIST",lon_dim_list);
		std::stringstream lon_dims;
		lon_dim_list.value.print(lon_dims,istream.reference_table_pointer());
		if ((lat_dims.str() == "["+re[0].name+"]" && lon_dims.str() == "["+re[1].name+"]") || (lat_dims.str() == lon_dims.str() && lon_dims.str() == "["+re[0].name+", "+re[1].name+"]")) {
		  parameter_matches=true;
		}
	    }
	  }
	}
	break;
    }
    case 4:
    case 5: {
// data variables dimensioned [time, lev, lat, lon] or [reference_time, forecast_period, lev, lat, lon]
	auto off=attr.value.dim_sizes[0]-4;
	auto can_continue=true;
	if (re[off].name != level_id) {
	  can_continue=false;
	  auto lev_ds=istream.dataset("/"+level_id);
	  if (lev_ds != nullptr) {
	    InputHDF5Stream::Attribute lev_dim_list;
	    lev_ds->attributes.found("DIMENSION_LIST",lev_dim_list);
	    std::stringstream lev_dims;
	    lev_dim_list.value.print(lev_dims,istream.reference_table_pointer());
	    if (lev_dims.str() == "["+re[off].name+"]") {
		can_continue=true;
	    }
	  }
	  else if (level_id == "sfc" && re[off].name == gcoords.forecast_period.id) {
	    can_continue=true;
	  }
	}
	if (can_continue) {
	  if (re[off+1].name == gcoords.latitude.id && re[off+2].name == gcoords.longitude.id) {
// latitude and longitude are coordinate variables
	     parameter_matches=true;
	  }
	  else {
// check for auxiliary coordinate variables for latitude and longitude
	    auto lat_ds=istream.dataset("/"+gcoords.latitude.id);
	    auto lon_ds=istream.dataset("/"+gcoords.longitude.id);
	    if (lat_ds != nullptr && lon_ds != nullptr) {
		InputHDF5Stream::Attribute lat_dim_list;
		lat_ds->attributes.found("DIMENSION_LIST",lat_dim_list);
		std::stringstream lat_dims;
		lat_dim_list.value.print(lat_dims,istream.reference_table_pointer());
		InputHDF5Stream::Attribute lon_dim_list;
		lon_ds->attributes.found("DIMENSION_LIST",lon_dim_list);
		std::stringstream lon_dims;
		lon_dim_list.value.print(lon_dims,istream.reference_table_pointer());
		if ((lat_dims.str() == "["+re[off+1].name+"]" && lon_dims.str() == "["+re[off+2].name+"]") || (lat_dims.str() == lon_dims.str() && lon_dims.str() == "["+re[off+1].name+", "+re[off+2].name+"]")) {
		  parameter_matches=true;
		}
	    }
	  }
	}
	if (off == 1 && parameter_matches) {
	  if (re[0].name != gcoords.forecast_period.id) {
	    parameter_matches=false;
	  }
	}
	break;
    }
  }
  return parameter_matches;
}

void add_gridded_parameters_to_netcdf_level_entry(InputHDF5Stream& istream,std::string& gentry_key,const GridCoordinates& gcoords,std::string level_id,ScanData& scan_data,const metautils::NcTime::TimeRangeEntry& tre,const metautils::NcTime::TimeData& time_data,const metautils::NcTime::TimeBounds& time_bounds,ParameterData& parameter_data)
{
// find all of the variables
  auto vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    InputHDF5Stream::Attribute attr;
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (gatherxml::verbose_operation) {
	std::cout << "    '" << var.key << "' has a DIMENSION_LIST: ";
	attr.value.print(std::cout,istream.reference_table_pointer());
	std::cout << std::endl;
    }
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && (attr.value.dim_sizes[0] > 2 || (attr.value.dim_sizes[0] == 2 && gcoords.valid_time.data_array.num_values == 1)) && attr.value.vlen.class_ == 7 && parameter_matches_dimensions(istream,attr,gcoords,level_id)) {
	if (gatherxml::verbose_operation) {
	  std::cout << "    ...is a netCDF variable" << std::endl;
	}
	auto time_method=gridded_time_method(var.dataset,gcoords);
	metautils::NcTime::TimeRange time_range;
	if (time_method.empty() || (floatutils::myequalf(time_bounds.t1,0,0.0001) && floatutils::myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	  time_range.first_valid_datetime=tre.data->instantaneous.first_valid_datetime;
	  time_range.last_valid_datetime=tre.data->instantaneous.last_valid_datetime;
	}
	else {
	  if (time_bounds.changed) {
	    metautils::log_error("time bounds changed","hdf2xml",USER);
	  }
	  time_range.first_valid_datetime=tre.data->bounded.first_valid_datetime;
	  time_range.last_valid_datetime=tre.data->bounded.last_valid_datetime;
	}
	time_method=strutils::capitalize(time_method);
	std::string tr_description,error;
	tr_description=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
	if (!error.empty()) {
	  metautils::log_error(error,"hdf2xml",USER);
	}
	tr_description=strutils::capitalize(tr_description);
	if (std::regex_search(gentry_key,std::regex(tr_description+"$"))) {
//	  if (attr.value.dim_sizes[0] == 4 || attr.value.dim_sizes[0] == 3 || (attr.value.dim_sizes[0] == 2 && gcoords.valid_time.data_array.num_values == 1)) {
	    param_entry->key="ds"+metautils::args.dsnum+":"+var.key;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	    InvEntry ie;
	    if (!inv_P_table.found(param_entry->key,ie)) {
		ie.key=param_entry->key;
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
	    }
//	  }
	}
    }
  }
}

void update_level_entry(InputHDF5Stream& istream,const metautils::NcTime::TimeRangeEntry tre,const metautils::NcTime::TimeData& time_data,const metautils::NcTime::TimeBounds& time_bounds,const GridCoordinates& gcoords,std::string level_id,ScanData& scan_data,ParameterData& parameter_data,unsigned char& level_write)
{
  std::list<InputHDF5Stream::DatasetEntry> vars;
  InputHDF5Stream::Attribute attr;
  std::string time_method,tr_description,error;
  metautils::NcTime::TimeRange time_range;

  vars=istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    var.dataset->attributes.found("DIMENSION_LIST",attr);
    if (attr.value._class_ == 9 && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] > 2 && attr.value.vlen.class_ == 7 && parameter_matches_dimensions(istream,attr,gcoords,level_id)) {
	param_entry->key="ds"+metautils::args.dsnum+":"+var.key;
	time_method=gridded_time_method(var.dataset,gcoords);
	time_method=strutils::capitalize(time_method);
	if (!lentry->parameter_code_table.found(param_entry->key,*param_entry)) {
	  if (time_method.empty() || (floatutils::myequalf(time_bounds.t1,0,0.0001) && floatutils::myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
	    time_range.first_valid_datetime=tre.data->instantaneous.first_valid_datetime;
	    time_range.last_valid_datetime=tre.data->instantaneous.last_valid_datetime;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	  }
	  else {
	    if (time_bounds.changed) {
		metautils::log_error("time bounds changed","hdf2xml",USER);
	    }
	    time_range.first_valid_datetime=tre.data->bounded.first_valid_datetime;
	    time_range.last_valid_datetime=tre.data->bounded.last_valid_datetime;
	    add_gridded_netcdf_parameter(var,scan_data,time_range,parameter_data,tre.data->num_steps);
	  }
	  gentry->level_table.replace(*lentry);
	}
	else {
	  tr_description=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
	  if (!error.empty())
	    metautils::log_error(error,"hdf2xml",USER);
	  tr_description=strutils::capitalize(tr_description);
	  if (std::regex_search(gentry->key,std::regex(tr_description+"$"))) {
	    if (time_method.empty() || (floatutils::myequalf(time_bounds.t1,0,0.0001) && floatutils::myequalf(time_bounds.t1,time_bounds.t2,0.0001))) {
		if (tre.data->instantaneous.first_valid_datetime < param_entry->start_date_time) {
		  param_entry->start_date_time=tre.data->instantaneous.first_valid_datetime;
		}
		if (tre.data->instantaneous.last_valid_datetime > param_entry->end_date_time) {
		  param_entry->end_date_time=tre.data->instantaneous.last_valid_datetime;
		}
	    }
	    else {
		if (tre.data->bounded.first_valid_datetime < param_entry->start_date_time) {
		  param_entry->start_date_time=tre.data->bounded.first_valid_datetime;
		}
		if (tre.data->bounded.last_valid_datetime > param_entry->end_date_time) {
		  param_entry->end_date_time=tre.data->bounded.last_valid_datetime;
		}
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

void fill_time_bounds(const HDF5::DataArray& data_array,InputHDF5Stream::Dataset *ds,metautils::NcTime::TimeRangeEntry& tre,const metautils::NcTime::TimeData& time_data,metautils::NcTime::TimeBounds& time_bounds)
{
  time_bounds.t1=data_array_value(data_array,0,ds);
  time_bounds.diff=data_array_value(data_array,1,ds)-time_bounds.t1;
  for (size_t n=2; n < data_array.num_values; n+=2) {
    if (!floatutils::myequalf((data_array_value(data_array,n+1,ds)-data_array_value(data_array,n,ds)),time_bounds.diff)) {
	time_bounds.changed=true;
	break;
    }
  }
  time_bounds.t2=data_array_value(data_array,data_array.num_values-1,ds);
  std::string error;
  tre.data->bounded.first_valid_datetime=metautils::NcTime::actual_date_time(time_bounds.t1,time_data,error);
  if (!error.empty()) {
    metautils::log_error(error,"hdf2xml",USER);
  }
  tre.data->bounded.last_valid_datetime=metautils::NcTime::actual_date_time(time_bounds.t2,time_data,error);
  if (!error.empty()) {
    metautils::log_error(error,"hdf2xml",USER);
  }
}

DateTime compute_NcTime(const HDF5::DataArray& times,const metautils::NcTime::TimeData& time_data,size_t index)
{
  auto val=times.value(index);
  DateTime dt;
  if (time_data.units == "seconds") {
    dt=time_data.reference.seconds_added(val);
  }
  else if (time_data.units == "hours") {
    if (floatutils::myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.hours_added(val);
    }
    else {
	dt=time_data.reference.seconds_added(lround(val*3600.));
    }
  }
  else if (time_data.units == "days") {
    if (floatutils::myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.days_added(val);
    }
    else {
	dt=time_data.reference.seconds_added(lround(val*86400.));
    }
  }
  else {
    metautils::log_error("compute_NcTime() unable to set date/time for units '"+time_data.units+"'","hdf2xml",USER);
  }
  return dt;
}

void update_inventory(int unum,int gnum,const GridCoordinates& gcoords,const metautils::NcTime::TimeData& time_data)
{
  InvEntry ie;
  if (!inv_L_table.found(lentry->key,ie)) {
    ie.key=lentry->key;
    ie.num=inv_L_table.size();
    inv_L_table.insert(ie);
  }
  for (size_t n=0; n < gcoords.valid_time.data_array.num_values; ++n) {
    for (const auto& key : lentry->parameter_code_table.keys()) {
	InvEntry pie;
	inv_P_table.found(key,pie);
	std::stringstream inv_line;
	std::string error;
	inv_line << "0|0|" << metautils::NcTime::actual_date_time(data_array_value(gcoords.valid_time.data_array,n,gcoords.valid_time.ds.get()),time_data,error).to_string("%Y%m%d%H%MM") << "|" << unum << "|" << gnum << "|" << ie.num << "|" << pie.num << "|0";
	inv_lines.emplace_back(inv_line.str());
    }
  }
}

void process_units_attribute(const InputHDF5Stream::DatasetEntry& ds_entry,DiscreteGeometriesData& dgd,metautils::NcTime::TimeData& time_data)
{
  InputHDF5Stream::Attribute attr;
  ds_entry.dataset->attributes.found("units",attr);
  std::string attr_val(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
  if (std::regex_search(attr_val,std::regex("since"))) {
    if (!dgd.indexes.time_var.empty()) {
	metautils::log_error("processed_units_attribute() returned error: time was already identified - don't know what to do with variable: "+ds_entry.key,"hdf2xml",USER);
    }
    metautils::CF::fill_nc_time_data(attr_val,time_data,USER);
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

void fill_dgd_index(InputHDF5Stream& istream,std::string attr_name,std::string attr_value,std::string& dgd_index)
{
  auto ds_entry_list=istream.datasets_with_attribute(attr_name);
  if (ds_entry_list.size() > 1) {
    metautils::log_error("fill_dgd_index() returned error: more than one "+attr_name+" variable found","hdf2xml",USER);
  }
  else if (ds_entry_list.size() > 0) {
    InputHDF5Stream::Attribute attr;
    ds_entry_list.front().dataset->attributes.found(attr_name,attr);
    std::string aval(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    if (attr_value.empty() || aval == attr_value) {
	if (!dgd_index.empty()) {
	  metautils::log_error("fill_dgd_index() returned error: "+attr_name+" was already identified - don't know what to do with variable: "+ds_entry_list.front().key,"hdf2xml",USER);
	}
	else {
	  dgd_index=ds_entry_list.front().key;
	}
    }
  }
}

void fill_dgd_index(InputHDF5Stream& istream,std::string attr_name,std::unordered_map<std::string,std::string>& dgd_index)
{
  auto ds_entry_list=istream.datasets_with_attribute(attr_name);
  for (const auto& ds_entry : ds_entry_list) {
    InputHDF5Stream::Attribute attr;
    ds_entry.dataset->attributes.found(attr_name,attr);
    std::string aval(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    if (dgd_index.find(aval) == dgd_index.end()) {
	dgd_index.emplace(aval,ds_entry.key);
    }
    else {
	metautils::log_error("fill_dgd_index() returned error: "+attr_name+" was already identified - don't know what to do with variable: "+ds_entry.key,"hdf2xml",USER);
    }
  }
}

void process_vertical_coordinate_variable(InputHDF5Stream& istream,DiscreteGeometriesData& dgd,std::string& obs_type)
{
  obs_type="";
  auto ds=istream.dataset("/"+dgd.indexes.z_var);
  InputHDF5Stream::Attribute attr;
  if (ds->attributes.found("units",attr)) {
    dgd.z_units.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    strutils::trim(dgd.z_units);
  }
  if (ds->attributes.found("positive",attr)) {
    dgd.z_pos.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    strutils::trim(dgd.z_pos);
    dgd.z_pos=strutils::to_lower(dgd.z_pos);
  }
  if (dgd.z_pos.empty() && !dgd.z_units.empty()) {
    auto z_units_l=strutils::to_lower(dgd.z_units);
    if (std::regex_search(dgd.z_units,std::regex("Pa$")) || std::regex_search(z_units_l,std::regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
	dgd.z_pos="down";
	obs_type="upper_air";
    }
  }
  if (dgd.z_pos.empty()) {
    metautils::log_error("process_vertical_coordinate_variable() returned error: unable to determine vertical coordinate direction","hdf2xml",USER);
  }
  else if (obs_type.empty()) {
    if (dgd.z_pos == "up") {
	obs_type="upper_air";
    }
    else if (dgd.z_pos == "down") {
	auto z_units_l=strutils::to_lower(dgd.z_units);
	if (std::regex_search(dgd.z_units,std::regex("Pa$")) || std::regex_search(z_units_l,std::regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
	  obs_type="upper_air";
	}
	else if (dgd.z_units == "m") {
	  obs_type="subsurface";
	}
    }
  }
}

void scan_cf_point_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data)
{
  auto ds_entry_list=istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  metautils::NcTime::TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry,dgd,time_data);
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
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine time variable","hdf2xml",USER);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.time_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access time variable","hdf2xml",USER);
    }
    InputHDF5Stream::Attribute attr;
    if (ds->attributes.found("calendar",attr)) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    }
    time_vals.fill(istream,*ds);
  }
  HDF5::DataArray lat_vals;
  if (dgd.indexes.lat_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine latitude variable","hdf2xml",USER);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.lat_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access latitude variable","hdf2xml",USER);
    }
    lat_vals.fill(istream,*ds);
  }
  HDF5::DataArray lon_vals;
  if (dgd.indexes.lon_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine longitude variable","hdf2xml",USER);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.lon_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access longitude variable","hdf2xml",USER);
    }
    lon_vals.fill(istream,*ds);
  }
  HDF5::DataArray id_vals;
  if (dgd.indexes.stn_id_var.empty()) {
    metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to determine report ID variable","hdf2xml",USER);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.stn_id_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: unable to access report ID variable","hdf2xml",USER);
    }
    id_vals.fill(istream,*ds);
  }
  scan_data.map_name=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds"+metautils::args.dsnum+".xml",scan_data.tdir->name());
  scan_data.found_map=!scan_data.map_name.empty();
  std::vector<DateTime> date_times;
  date_times.reserve(time_vals.num_values);
  std::vector<std::string> ids;
  ids.reserve(time_vals.num_values);
  std::vector<float> lats,lons;
  lats.reserve(time_vals.num_values);
  lons.reserve(time_vals.num_values);
  std::string platform_type="unknown";
  gatherxml::markup::ObML::IDEntry ientry;
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
	    descr.assign(reinterpret_cast<char *>(attr.value.array));
	  }
	  else if (lkey == "units") {
	    units.assign(reinterpret_cast<char *>(attr.value.array));
	  }
	}
	strutils::trim(descr);
	strutils::trim(units);
	metautils::StringEntry se;
	se.key=ds_entry.key+"<!>"+descr+"<!>"+units;
	scan_data.varlist.emplace_back(se.key);
	if (scan_data.found_map && !scan_data.var_changes_table.found(ds_entry.key,se)) {
	  se.key=ds_entry.key;
	  scan_data.var_changes_table.insert(se);
	}
	HDF5::DataArray var_data;
	var_data.fill(istream,*ds);
	auto var_missing_value=HDF5::decode_data_value(ds->datatype,ds->fillvalue.bytes,1.e33);
	for (size_t n=0; n < time_vals.num_values; ++n) {
	  if (n == date_times.size()) {
	    date_times.emplace_back(compute_NcTime(time_vals,time_data,n));
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
	    if (!obs_data.added_to_platforms("surface",platform_type,lats[n],lons[n])) {
		metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: '"+myerror+" when adding platform "+platform_type,"hdf2xml",USER);
	    }
	    ientry.key=platform_type+"[!]unknown[!]"+ids[n];
	    if (!obs_data.added_to_ids("surface",ientry,ds_entry.key,"",lats[n],lons[n],time_vals.value(n),&date_times[n])) {
		metautils::log_error("scan_cf_point_hdf5nc4_file() returned error: '"+myerror+" when adding ID "+ientry.key,"hdf2xml",USER);
	    }
	    ++scan_data.num_not_missing;
	  }
	}
	ds->free();
    }
  }
  scan_data.write_type=ScanData::ObML_type;
}

void scan_cf_non_orthogonal_profile_hdf5nc4_file(InputHDF5Stream& istream,const DiscreteGeometriesData& dgd,const metautils::NcTime::TimeData& time_data,const HDF5::DataArray& time_vals,const NetCDFVariableAttributeData& nc_ta_data,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data,std::string obs_type)
{
  static const std::string THIS_FUNC=__func__;
  if (gatherxml::verbose_operation) {
    std::cout << "...beginning function "+THIS_FUNC+"()..." << std::endl;
  }
  auto ds=istream.dataset("/"+dgd.indexes.stn_id_var);
  if (ds == nullptr) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to access station ID variable","hdf2xml",USER);
  }
  HDF5::DataArray id_vals(istream,*ds);
  ds=istream.dataset("/"+dgd.indexes.lat_var);
  if (ds == nullptr) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to access latitude variable","hdf2xml",USER);
  }
  HDF5::DataArray lat_vals(istream,*ds);
  ds=istream.dataset("/"+dgd.indexes.lon_var);
  if (ds == nullptr) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to access longitude variable","hdf2xml",USER);
  }
  HDF5::DataArray lon_vals(istream,*ds);
  if (id_vals.num_values != time_vals.num_values || lat_vals.num_values != id_vals.num_values || lon_vals.num_values != lat_vals.num_values) {
    metautils::log_error(THIS_FUNC+"() returned error: profile data does not follow the CF conventions","hdf2xml",USER);
  }
  std::vector<std::string> platform_types,id_types;
  if (!scan_data.platform_type.empty()) {
    for (size_t n=0; n < id_vals.num_values; ++n) {
	platform_types.emplace_back(scan_data.platform_type);
id_types.emplace_back("unknown");
	auto lat=lat_vals.value(n);
	if (lat < -90. || lat > 90.) {
	  lat=-999.;
	}
	auto lon=lon_vals.value(n);
	if (lon < -180. || lon > 360.) {
	  lon=-999.;
	}
	if (lat > -999. && lon > -999. && !obs_data.added_to_platforms(obs_type,platform_types[n],lat,lon)) {
	  metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' when adding platform "+platform_types[n],"nc2xml",USER);
	}
    }
  }
  else {
    metautils::log_error(THIS_FUNC+"() returned error: undefined platform type","nc2xml",USER);
  }
  ds=istream.dataset("/"+dgd.indexes.z_var);
  if (ds == nullptr) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to access vertical level variable","hdf2xml",USER);
  }
  HDF5::DataArray z_vals(istream,*ds);
  InputHDF5Stream::Attribute attr;
  if (!ds->attributes.found("DIMENSION_LIST",attr)) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to get vertical level row size variable","hdf2xml",USER);
  }
  std::stringstream val_ss;
  attr.value.print(val_ss,istream.reference_table_pointer());
  auto var_list=istream.datasets_with_attribute("DIMENSION_LIST");
  auto dims=strutils::split(val_ss.str(),",");
  strutils::replace_all(dims.front(),"[","");
  strutils::replace_all(dims.front(),"]","");
  strutils::trim(dims.front());
  ds=istream.dataset("/"+dgd.indexes.sample_dim_vars.at(dims.front()));
  if (ds == nullptr) {
    metautils::log_error(THIS_FUNC+"() returned error: unable to get vertical level row sizes","hdf2xml",USER);
  }
  HDF5::DataArray z_rowsize_vals(istream,*ds);
  if (dgd.indexes.sample_dim_vars.size() > 0) {
// continuous ragged array H.10
    for (const auto& var : var_list) {
	var.dataset->attributes.found("DIMENSION_LIST",attr);
	val_ss.str("");
	attr.value.print(val_ss,istream.reference_table_pointer());
	auto dims=strutils::split(val_ss.str(),",");
	strutils::replace_all(dims.front(),"[","");
	strutils::replace_all(dims.front(),"]","");
	strutils::trim(dims.front());
	if (dgd.indexes.sample_dim_vars.find(dims.front()) != dgd.indexes.sample_dim_vars.end() && var.key != dgd.indexes.z_var) {
	  ds=istream.dataset("/"+dgd.indexes.sample_dim_vars.at(dims.front()));
	  if (ds == nullptr) {
	    metautils::log_error(THIS_FUNC+"() returned error: unable to get row size data for "+var.key,"hdf2xml",USER);
	  }
	  HDF5::DataArray rowsize_vals(istream,*ds);
	  ds=istream.dataset("/"+var.key);
	  if (ds == nullptr) {
	    metautils::log_error(THIS_FUNC+"() returned error: unable to get variable values for "+var.key,"hdf2xml",USER);
	  }
	  HDF5::DataArray var_vals(istream,*ds);
	  if (var_vals.type != HDF5::DataArray::Type::_NULL) {
	    NetCDFVariableAttributeData nc_va_data;
	    extract_from_hdf5_variable_attribute(var.dataset->attributes,nc_va_data);
	    metautils::StringEntry se;
	    se.key=var.key+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units;
	    scan_data.varlist.emplace_back(se.key);
	    if (scan_data.found_map && !scan_data.var_changes_table.found(var.key,se)) {
		se.key=var.key;
		scan_data.var_changes_table.insert(se);
	    }
	    auto var_off=0;
	    auto z_off=0;
	    for (size_t n=0; n < time_vals.num_values; ++n) {
		auto lat=lat_vals.value(n);
		if (lat < -90. || lat > 90.) {
		  lat=-999.;
		}
		auto lon=lon_vals.value(n);
		if (lon < -180. || lon > 360.) {
		  lon=-999.;
		}
		std::vector<double> level_list;
		for (size_t m=0; m < rowsize_vals.value(n); ++m) {
		  if (!found_missing(time_vals.value(n),time_vals.type,&nc_ta_data.missing_value,var_vals.value(var_off),var_vals.type,nc_va_data.missing_value)) {
		    level_list.emplace_back(z_vals.value(z_off+m));
		  }
		  ++var_off;
		}
		z_off+=z_rowsize_vals.value(n);
		if (level_list.size() > 0 && lat > -999. && lon > -999.) {
		  auto dt=compute_NcTime(time_vals,time_data,n);
		  gatherxml::markup::ObML::IDEntry ientry;
		  ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
		  if (id_vals.type == HDF5::DataArray::Type::INT || id_vals.type == HDF5::DataArray::Type::FLOAT || id_vals.type == HDF5::DataArray::Type::DOUBLE) {
		    ientry.key+=strutils::ftos(id_vals.value(n));
		  }
		  else if (id_vals.type == HDF5::DataArray::Type::STRING) {
		    ientry.key+=id_vals.string_value(n);
		  }
		  if (!obs_data.added_to_ids(obs_type,ientry,var.key,"",lat_vals.value(n),lon_vals.value(n),time_vals.value(n),&dt)) {
		    metautils::log_error(THIS_FUNC+"() returned error: '"+myerror+"' when adding ID "+ientry.key,"nc2xml",USER);
		  }
		  if (level_list.size() > 1) {
		    gatherxml::markup::ObML::DataTypeEntry dte;
		    ientry.data->data_types_table.found(var.key,dte);
		    dte.fill_vertical_resolution_data(level_list,dgd.z_pos,dgd.z_units);
		  }
		  ++scan_data.num_not_missing;
		}
	    }
	  }
	}
    }
  }
  else if (!dgd.indexes.instance_dim_var.empty()) {
// indexed ragged array H.11
metautils::log_error(THIS_FUNC+"() returned error: indexed ragged array not implemented","hdf2xml",USER);
  }
  if (gatherxml::verbose_operation) {
    std::cout << "...function "+THIS_FUNC+"() done." << std::endl;
  }
}

void scan_cf_orthogonal_profile_hdf5nc4_file(InputHDF5Stream& istream,const DiscreteGeometriesData& dgd,const HDF5::DataArray& time_vals,const NetCDFVariableAttributeData& nc_ta_data,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data,std::string obs_type)
{
  if (gatherxml::verbose_operation) {
    std::cout << "...beginning function scan_cf_orthogonal_profile_hdf5nc4_file()..." << std::endl;
  }
  if (gatherxml::verbose_operation) {
    std::cout << "...function scan_cf_orthogonal_profile_hdf5nc4_file() done." << std::endl;
  }
}

void scan_cf_profile_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data)
{
  if (gatherxml::verbose_operation) {
    std::cout << "...beginning function scan_cf_profile_hdf5nc4_file()..." << std::endl;
  }
  auto ds_entry_list=istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  metautils::NcTime::TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry,dgd,time_data);
  }
  fill_dgd_index(istream,"cf_role","profile_id",dgd.indexes.stn_id_var);
  fill_dgd_index(istream,"sample_dimension",dgd.indexes.sample_dim_vars);
  fill_dgd_index(istream,"instance_dimension","",dgd.indexes.instance_dim_var);
  HDF5::DataArray time_vals;
  NetCDFVariableAttributeData nc_ta_data;
  if (dgd.indexes.time_var.empty()) {
    metautils::log_error("scan_cf_profile_hdf5nc4_file() returned error: unable to determine time variable","hdf2xml",USER);
  }
  else {
    auto ds=istream.dataset("/"+dgd.indexes.time_var);
    if (ds == nullptr) {
	metautils::log_error("scan_cf_profile_hdf5nc4_file() returned error: unable to access time variable","hdf2xml",USER);
    }
    InputHDF5Stream::Attribute attr;
    if (ds->attributes.found("calendar",attr)) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
    }
    time_vals.fill(istream,*ds);
    extract_from_hdf5_variable_attribute(ds->attributes,nc_ta_data);
  }
  fill_dgd_index(istream,"axis","Z",dgd.indexes.z_var);
  if (dgd.indexes.z_var.empty()) {
    fill_dgd_index(istream,"positive","",dgd.indexes.z_var);
  }
  std::string obs_type;
  if (dgd.indexes.z_var.empty()) {
    metautils::log_error("scan_cf_profile_hdf5nc4_file() returned error: unable to determine vertical coordinate variable","hdf2xml",USER);
  }
  else {
    process_vertical_coordinate_variable(istream,dgd,obs_type);
  }
  if (obs_type.empty()) {
    metautils::log_error("scan_cf_profile_hdf5nc4_file() returned error: unable to determine observation type","nc2xml",USER);
  }
  scan_data.map_name=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds"+metautils::args.dsnum+".xml",scan_data.tdir->name());
  scan_data.found_map=(!scan_data.map_name.empty());
  if (dgd.indexes.sample_dim_vars.size() > 0 || !dgd.indexes.instance_dim_var.empty()) {
// ex. H.10, H.11
    scan_cf_non_orthogonal_profile_hdf5nc4_file(istream,dgd,time_data,time_vals,nc_ta_data,scan_data,obs_data,obs_type);
  }
  else {
// ex. H.8, H.9
    scan_cf_orthogonal_profile_hdf5nc4_file(istream,dgd,time_vals,nc_ta_data,scan_data,obs_data,obs_type);
  }
  scan_data.write_type=ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    std::cout << "...function scan_cf_profile_hdf5nc4_file() done." << std::endl;
  }
}

void scan_gridded_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data)
{
  static const std::string THIS_FUNC=__func__;
  if (gatherxml::verbose_operation) {
    std::cout << "...beginning function scan_gridded_hdf5nc4_file()..." << std::endl;
  }
  auto found_time=false;
  grid_initialize();
  gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"GrML","hdf2xml",USER);
  scan_data.map_name=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds"+metautils::args.dsnum+".xml",scan_data.tdir->name());
  ParameterData parameter_data;
  if (!scan_data.map_name.empty()) {
// rename the parameter map so that it is not overwritten by the level map,
//   which has the same name
    std::stringstream oss,ess;
    unixutils::mysystem2("/bin/mv "+scan_data.map_name+" "+scan_data.map_name+".p",oss,ess);
    if (!ess.str().empty()) {
	metautils::log_error("unable to rename parameter map; error - '"+ess.str()+"'","hdf2xml",USER);
    }
    scan_data.map_name+=".p";
    if (parameter_data.map.fill(scan_data.map_name)) {
	scan_data.found_map=true;
    }
  }
  auto dim_vars=istream.datasets_with_attribute("CLASS=DIMENSION_SCALE");
  if (gatherxml::verbose_operation) {
    std::cout << "...found " << dim_vars.size() << " 'DIMENSION_SCALE' variables..." << std::endl;
  }
  std::string climo_bounds_id;
  GridCoordinates gcoords;
  metautils::NcTime::TimeData time_data,forecast_period_time_data;
  metautils::NcLevel::LevelInfo level_info;
  std::vector<std::string> lat_ids,lon_ids;
  my::map<metautils::StringEntry> unique_level_id_table;
  for (const auto& var : dim_vars) {
    if (gatherxml::verbose_operation) {
	std::cout << "  '" << var.key << "'" << std::endl;
    }
    InputHDF5Stream::Attribute attr;
    metautils::StringEntry se;
    if (var.dataset->attributes.found("units",attr) && (attr.value._class_ == 3 || (attr.value._class_ == 9 && attr.value.vlen.class_ == 3))) {
	std::string units_value;
	if (attr.value._class_ == 3) {
	  units_value=reinterpret_cast<char *>(attr.value.array);
	}
	else {
	  int len=(attr.value.vlen.buffer[0] << 24)+(attr.value.vlen.buffer[1] << 16)+(attr.value.vlen.buffer[2] << 8)+attr.value.vlen.buffer[3];
	  units_value=std::string(reinterpret_cast<char *>(&attr.value.vlen.buffer[4]),len);
	}
	if (gatherxml::verbose_operation) {
	  std::cout << "    units attribute: '" << units_value << "'" << std::endl;
	}
	std::string standard_name_value;
	if (var.dataset->attributes.found("standard_name",attr) && (attr.value._class_ == 3 || (attr.value._class_ == 9 && attr.value.vlen.class_ == 3))) {
	  if (attr.value._class_ == 3) {
	    standard_name_value=reinterpret_cast<char *>(attr.value.array);
	  }
	  else {
	    int len=(attr.value.vlen.buffer[0] << 24)+(attr.value.vlen.buffer[1] << 16)+(attr.value.vlen.buffer[2] << 8)+attr.value.vlen.buffer[3];
	    standard_name_value=std::string(reinterpret_cast<char *>(&attr.value.vlen.buffer[4]),len);
	  }
	}
	if (std::regex_search(units_value,std::regex("since"))) {
	  if (found_time) {
	    metautils::log_error("time was already identified - don't know what to do with variable: "+var.key,"hdf2xml",USER);
	  }
	  for (const auto& key : var.dataset->attributes.keys()) {
	    var.dataset->attributes.found(key,attr);
	    if (attr.value._class_ == 3) {
		if (key == "bounds") {
		  gcoords.time_bounds.id=reinterpret_cast<char *>(attr.value.array);
		  break;
		}
		else if (key == "climatology") {
		  climo_bounds_id=reinterpret_cast<char *>(attr.value.array);
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
	  time_data.reference=metautils::NcTime::reference_date_time(units_value);
	  if (time_data.reference.year() == 0) {
	    metautils::log_error("bad netcdf date in units for time","hdf2xml",USER);
	  }
	  if (var.dataset->attributes.found("calendar",attr)) {
	    time_data.calendar.assign(reinterpret_cast<char *>(attr.value.get()),attr.value.size);
	  }
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
	    if (!units_value.empty()) {
		forecast_period_time_data.units=units_value;
		if (forecast_period_time_data.units.back() != 's') {
		  forecast_period_time_data.units.append(1,'s');
		}
	    }
	  }
	  else {
	    if (!unique_level_id_table.found(var.key,se)) {
		level_info.ID.emplace_back(var.key);
		if (var.dataset->attributes.found("long_name",attr) && attr.value._class_ == 3) {
		  level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.array));
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
	  level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.array));
	}
	level_info.units.emplace_back("");
	level_info.write.emplace_back(0);
	se.key=var.key;
	unique_level_id_table.insert(se);
    }
  }
  forecast_period_time_data.reference=time_data.reference;
  forecast_period_time_data.calendar=time_data.calendar;
  if (gcoords.reference_time.id != gcoords.valid_time.id) {
    if (gatherxml::verbose_operation) {
	std::cout << "...checking for forecasts..." << std::endl;
    }
// check for forecasts
    auto vars=istream.datasets_with_attribute("standard_name=forecast_reference_time");
    if (vars.size() > 1) {
	metautils::log_error("multiple forecast reference times","hdf2xml",USER);
    }
    else if (!vars.empty()) {
	auto var=vars.front();
	InputHDF5Stream::Attribute attr;
	if (var.dataset->attributes.found("units",attr) && attr.value._class_ == 3) {
	  std::string units_value=reinterpret_cast<char *>(attr.value.array);
	  if (std::regex_search(units_value,std::regex("since"))) {
	    units_value=units_value.substr(0,units_value.find("since"));
	    strutils::trim(units_value);
	    if (units_value != time_data.units) {
		metautils::log_error("time and forecast reference time have different units","hdf2xml",USER);
	    }
	    gcoords.reference_time.id=var.key;
	    auto ref_dt=metautils::NcTime::reference_date_time(units_value);
	    if (ref_dt.year() == 0) {
		metautils::log_error("bad netcdf date in units for forecast_reference_time","hdf2xml",USER);
	    }
	  }
	}
    }
  }
  if (lat_ids.empty() && lon_ids.empty()) {
    if (gatherxml::verbose_operation) {
	std::cout << "...looking for alternate latitude and longitude coordinates..." << std::endl;
    }
    std::vector<std::string> compass{"north","east"};
    std::vector<std::vector<std::string> *> id_list{&lat_ids,&lon_ids};
    for (size_t n=0; n < compass.size(); ++n) {
	auto vars=istream.datasets_with_attribute("units=degrees_"+compass[n]);
	if (vars.empty()) {
	  vars=istream.datasets_with_attribute("units=degree_"+compass[n]);
	}
	for (const auto& v : vars) {
	  auto test_vars=istream.datasets_with_attribute("bounds="+v.key);
	  if (test_vars.empty()) {
	    if (gatherxml::verbose_operation) {
		std::cout << "   ...found '" << v.key << "'" << std::endl;
	    }
	    id_list[n]->emplace_back(v.key);
	    InputHDF5Stream::Attribute attr;
            v.dataset->attributes.found("DIMENSION_LIST",attr);
	    std::stringstream ss;
            attr.value.print(ss,istream.reference_table_pointer());
	    auto dparts=strutils::split(ss.str().substr(1,ss.str().length()-2),",");
	    for (auto& d : dparts) {
		strutils::trim(d);
	    }
	  }
	}
    }
  }
  if (level_info.ID.empty()) {
    if (gatherxml::verbose_operation) {
	std::cout << "...looking for vertical level coordinates..." << std::endl;
    }
    auto vars=istream.datasets_with_attribute("units=Pa");
    if (vars.size() == 0) {
	vars=istream.datasets_with_attribute("units=hPa");
    }
    for (const auto& v : vars) {
	InputHDF5Stream::Attribute attr;
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value._class_ == 9) {
	  level_info.ID.emplace_back(v.key);
	  if (gatherxml::verbose_operation) {
	    std::cout << "   ...found '" << v.key << "'" << std::endl;
	  }
	  level_info.description.emplace_back("Pressure Level");
	  level_info.units.emplace_back("Pa");
	  level_info.write.emplace_back(0);
	}
    }
    vars=istream.datasets_with_attribute("positive");
    for (const auto& v : vars) {
	InputHDF5Stream::Attribute attr;
	if (v.dataset->attributes.found("DIMENSION_LIST",attr) && attr.value.dim_sizes.size() == 1 && attr.value.dim_sizes[0] == 1 && attr.value._class_ == 9) {
	  level_info.ID.emplace_back(v.key);
	  if (gatherxml::verbose_operation) {
	    std::cout << "   ...found '" << v.key << "'" << std::endl;
	  }
	  if (v.dataset->attributes.found("description",attr) && attr.value._class_ == 3) {
	    level_info.description.emplace_back(reinterpret_cast<char *>(attr.value.array));
	  }
	  else {
	    level_info.description.emplace_back("");
	  }
	  if (v.dataset->attributes.found("units",attr) && attr.value._class_ == 3) {
	    level_info.units.emplace_back(reinterpret_cast<char *>(attr.value.array));
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
  if (!found_time) {
    std::cerr << "Terminating - no time coordinate found" << std::endl;
    exit(1);
  }
  if (lat_ids.empty()) {
    std::cerr << "Terminating - no latitude coordinate found" << std::endl;
    exit(1);
  }
  if (lon_ids.empty()) {
    std::cerr << "Terminating - no longitude coordinate found" << std::endl;
    exit(1);
  }
  if (found_time && !lat_ids.empty() && !lon_ids.empty()) {
    if (gatherxml::verbose_operation) {
	std::cout << "...found 'time', 'latitude', and 'longitude' coordinates..." << std::endl;
    }
    my::map<metautils::NcTime::TimeRangeEntry> time_range_table;
    metautils::NcTime::TimeRangeEntry tre;
    tre.key=-1;
    tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);
    gcoords.valid_time.ds=istream.dataset("/"+gcoords.valid_time.id);
    if (gcoords.valid_time.ds == nullptr) {
	metautils::log_error("unable to access the /"+gcoords.valid_time.id+" dataset for the data temporal range","hdf2xml",USER);
    }
    if (!gcoords.valid_time.data_array.fill(istream,*gcoords.valid_time.ds)) {
	metautils::log_error(THIS_FUNC+"() returned error: unable to fill time array","hdf2xml",USER);
    }
    metautils::NcTime::Time nctime;
    nctime.t1=data_array_value(gcoords.valid_time.data_array,0,gcoords.valid_time.ds.get());
    nctime.t2=data_array_value(gcoords.valid_time.data_array,gcoords.valid_time.data_array.num_values-1,gcoords.valid_time.ds.get());
    std::string error;
    tre.data->instantaneous.first_valid_datetime=metautils::NcTime::actual_date_time(nctime.t1,time_data,error);
    if (!error.empty()) {
	metautils::log_error(error,"hdf2xml",USER);
    }
    tre.data->instantaneous.last_valid_datetime=metautils::NcTime::actual_date_time(nctime.t2,time_data,error);
    if (!error.empty()) {
	metautils::log_error(error,"hdf2xml",USER);
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
	    metautils::log_error("unable to access the /"+gcoords.reference_time.id+" dataset for the forecast reference times","hdf2xml",USER);
	  }
	  gcoords.reference_time.data_array.fill(istream,*gcoords.reference_time.ds);
	  if (gcoords.reference_time.data_array.num_values != gcoords.valid_time.data_array.num_values) {
	    metautils::log_error("number of forecast reference times does not equal number of times","hdf2xml",USER);
	  }
	  for (size_t n=0; n < gcoords.valid_time.data_array.num_values; ++n) {
	    int m=data_array_value(gcoords.valid_time.data_array,n,gcoords.valid_time.ds.get())-data_array_value(gcoords.reference_time.data_array,n,gcoords.reference_time.ds.get());
	    if (m > 0) {
		if (static_cast<int>(tre.key) == -1) {
		  tre.key=-m*100;
		}
		if ( (-m*100) != static_cast<int>(tre.key)) {
		  metautils::log_error("forecast period changed","hdf2xml",USER);
		}
	    }
	    else if (m < 0) {
		metautils::log_error("found a time value that is less than the forecast reference time value","hdf2xml",USER);
	    }
	  }
	}
    }
    tre.data->num_steps=gcoords.valid_time.data_array.num_values;
    metautils::NcTime::TimeBounds time_bounds;
    if (!gcoords.time_bounds.id.empty()) {
	auto bounds_ds=istream.dataset("/"+gcoords.time_bounds.id);
	if (bounds_ds == nullptr) {
	  metautils::log_error("unable to access the /"+gcoords.time_bounds.id+" dataset for the time bounds","hdf2xml",USER);
	}
	HDF5::DataArray bounds(istream,*bounds_ds);
	if (bounds.num_values > 0) {
	  fill_time_bounds(bounds,bounds_ds.get(),tre,time_data,time_bounds);
	}
    }
    else if (!climo_bounds_id.empty()) {
	auto bounds_ds=istream.dataset("/"+gcoords.time_bounds.id);
	if (bounds_ds == nullptr) {
	  metautils::log_error("unable to access the /"+climo_bounds_id+" dataset for the climatology bounds","hdf2xml",USER);
	}
	HDF5::DataArray bounds(istream,*bounds_ds);
	if (bounds.num_values > 0) {
	  fill_time_bounds(bounds,bounds_ds.get(),tre,time_data,time_bounds);
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
	    metautils::log_error("unable to determine climatology unit","hdf2xml",USER);
	  }
// COARDS convention for climatology over all-available years
	  if ((tre.data->instantaneous.first_valid_datetime).year() == 0) {
	    tre.key=0x7fffffff;
	  }
	}
    }
    if (time_data.units == "months") {
	if ((tre.data->instantaneous.first_valid_datetime).day() == 1 && (tre.data->instantaneous.first_valid_datetime).time() == 0) {
	  tre.data->instantaneous.last_valid_datetime.add_seconds(dateutils::days_in_month((tre.data->instantaneous.last_valid_datetime).year(),(tre.data->instantaneous.last_valid_datetime).month(),time_data.calendar)*86400-1,time_data.calendar);
	}
	if (!gcoords.time_bounds.id.empty()) {
	  if ((tre.data->bounded.first_valid_datetime).day() == 1) {
	    tre.data->bounded.last_valid_datetime.add_days(dateutils::days_in_month((tre.data->bounded.last_valid_datetime).year(),(tre.data->bounded.last_valid_datetime).month(),time_data.calendar)-1,time_data.calendar);
	  }
	}
	else if (!climo_bounds_id.empty()) {
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
	  f_tre.data->instantaneous.first_valid_datetime.add(forecast_period_time_data.units,f_tre.key);
	  f_tre.data->instantaneous.last_valid_datetime.add(forecast_period_time_data.units,f_tre.key);
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
    if (lat_ids.size() != lon_ids.size()) {
	metautils::log_error("unequal number of latitude and longitude coordinate variables","hdf2xml",USER);
    }
    for (size_t n=0; n < lat_ids.size(); ++n) {
	gcoords.latitude.id=lat_ids[n];
	gcoords.latitude.ds=istream.dataset("/"+lat_ids[n]);
	if (gcoords.latitude.ds == nullptr) {
	  metautils::log_error("unable to access the /"+lat_ids[n]+" dataset for the latitudes","hdf2xml",USER);
	}
	gcoords.latitude.data_array.fill(istream,*gcoords.latitude.ds);
	Grid::GridDefinition def;
	def.slatitude=data_array_value(gcoords.latitude.data_array,0,gcoords.latitude.ds.get());
	gcoords.longitude.id=lon_ids[n];
	gcoords.longitude.ds=istream.dataset("/"+lon_ids[n]);
	if (gcoords.longitude.ds == nullptr) {
	  metautils::log_error("unable to access the /"+lon_ids[n]+" dataset for the latitudes","hdf2xml",USER);
	}
	gcoords.longitude.data_array.fill(istream,*gcoords.longitude.ds);
	def.slongitude=data_array_value(gcoords.longitude.data_array,0,gcoords.longitude.ds.get());
	InputHDF5Stream::Attribute lat_bounds,lon_bounds;
	std::shared_ptr<InputHDF5Stream::Dataset> lat_bounds_ds(nullptr),lon_bounds_ds(nullptr);
	HDF5::DataArray lat_bounds_array,lon_bounds_array;
	if (gcoords.latitude.ds->attributes.found("bounds",lat_bounds) && lat_bounds.value._class_ == 3 && gcoords.longitude.ds->attributes.found("bounds",lon_bounds) && lon_bounds.value._class_ == 3) {
	  if ( (lat_bounds_ds=istream.dataset("/"+std::string(reinterpret_cast<char *>(lat_bounds.value.array)))) != nullptr && (lon_bounds_ds=istream.dataset("/"+std::string(reinterpret_cast<char *>(lon_bounds.value.array)))) != nullptr) {
	    lat_bounds_array.fill(istream,*lat_bounds_ds);
	    lon_bounds_array.fill(istream,*lon_bounds_ds);
	  }
	}
	Grid::GridDimensions dim;
	InputHDF5Stream::Attribute lat_attr,lon_attr;
	if (gcoords.latitude.ds->attributes.found("DIMENSION_LIST",lat_attr) && lat_attr.value.dim_sizes.size() == 1 && lat_attr.value.dim_sizes[0] == 2 && lat_attr.value._class_ == 9 && gcoords.longitude.ds->attributes.found("DIMENSION_LIST",lon_attr) && lon_attr.value.dim_sizes.size() == 1 && lon_attr.value.dim_sizes[0] == 2 && lon_attr.value._class_ == 9) {
	  if (lat_attr.value.vlen.class_ == 7 && lon_attr.value.vlen.class_ == 7) {
	    InputHDF5Stream::ReferenceEntry re,re2,re3;
	    if (istream.reference_table_pointer()->found(HDF5::value(&lat_attr.value.vlen.buffer[4],lat_attr.value.precision_),re) && istream.reference_table_pointer()->found(HDF5::value(&lon_attr.value.vlen.buffer[4],lon_attr.value.precision_),re2) && re.name == re2.name && istream.reference_table_pointer()->found(HDF5::value(&lat_attr.value.vlen.buffer[8+lat_attr.value.precision_],lat_attr.value.precision_),re2) && istream.reference_table_pointer()->found(HDF5::value(&lon_attr.value.vlen.buffer[8+lon_attr.value.precision_],lon_attr.value.precision_),re3) && re2.name == re3.name) {
		auto ds=istream.dataset("/"+re.name);
		InputHDF5Stream::Attribute attr;
		if (ds == nullptr || !ds->attributes.found("NAME",attr) || attr.value._class_ != 3) {
		  metautils::log_error("(1)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
		}
		auto attr_parts=strutils::split(std::string(reinterpret_cast<char *>(attr.value.array)));
		if (attr_parts.size() == 11) {
// netCDF dimension
		  dim.y=std::stoi(attr_parts[10]);
		}
		else {
		  metautils::log_error("(2)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
		}
		ds=istream.dataset("/"+re2.name);
		if (ds == nullptr || !ds->attributes.found("NAME",attr) || attr.value._class_ != 3) {
		  metautils::log_error("(3)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
		}
		attr_parts=strutils::split(std::string(reinterpret_cast<char *>(attr.value.array)));
		if (attr_parts.size() == 11) {
// netCDF dimension
		  dim.x=std::stoi(attr_parts[10]);
		}
		else {
		  metautils::log_error("(4)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
		}
	    }
	    else {
		metautils::log_error("(5)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
	    }
	    auto center_x=dim.x/2;
	    auto center_y=dim.y/2;
	    auto xm=center_x-1;
	    auto ym=center_y-1;
	    if (floatutils::myequalf(
                  data_array_value(gcoords.latitude.data_array,ym*dim.x+xm,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+xm,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                floatutils::myequalf(
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+xm,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+center_x,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                floatutils::myequalf(
                  data_array_value(gcoords.latitude.data_array,center_y*dim.x+center_x,gcoords.latitude.ds.get()),
                  data_array_value(gcoords.latitude.data_array,ym*dim.x+center_x,gcoords.latitude.ds.get()),
                  0.00001
                ) &&
                floatutils::myequalf(
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
	    else if ((dim.x % 2) == 1 && floatutils::myequalf(data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),0.00001) && floatutils::myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()),0.00001)) {
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
	    else if ((dim.x % 2) == 0 && floatutils::myequalf((data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.longitude.ds.get())+data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())),(data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.longitude.ds.get())+data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get())),0.00001) && floatutils::myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.latitude.ds.get()))) {
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
std::cerr << floatutils::myequalf(data_array_value(gcoords.longitude.data_array,ym*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),data_array_value(gcoords.longitude.data_array,(center_y+1)*gcoords.longitude.data_array.dimensions[1]+center_x,gcoords.longitude.ds.get()),0.00001) << std::endl;
std::cerr << data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()) << " " << center_y << " " << gcoords.longitude.data_array.dimensions[1] << " " << xm << std::endl;
std::cerr << data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()) << " " << center_y << " " << gcoords.longitude.data_array.dimensions[1] << " " << center_x+1 << std::endl;
std::cerr << floatutils::myequalf(data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+xm,gcoords.latitude.ds.get()),data_array_value(gcoords.latitude.data_array,center_y*gcoords.longitude.data_array.dimensions[1]+center_x+1,gcoords.latitude.ds.get()),0.00001) << std::endl;
		metautils::log_error("(6)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
	    }
	  }
	  else {
	    metautils::log_error("(7)unable to determine grid definition from '"+lat_ids[n]+"' and '"+lon_ids[n]+"'","hdf2xml",USER);
	  }
	}
	else {
	  def.type=Grid::latitudeLongitudeType;
	  dim.y=gcoords.latitude.data_array.num_values;
	  dim.x=gcoords.longitude.data_array.num_values;
	  def.elatitude=data_array_value(gcoords.latitude.data_array,dim.y-1,gcoords.latitude.ds.get());
	  def.elongitude=data_array_value(gcoords.longitude.data_array,dim.x-1,gcoords.longitude.ds.get());
	  def.laincrement=fabs((def.elatitude-def.slatitude)/(dim.y-1));
	  def.loincrement=fabs((def.elongitude-def.slongitude)/(dim.x-1));
	  if (lat_bounds_array.num_values > 0) {
	    def.slatitude=data_array_value(lat_bounds_array,0,lat_bounds_ds.get());
	    def.slongitude=data_array_value(lon_bounds_array,0,lon_bounds_ds.get());
	    def.elatitude=data_array_value(lat_bounds_array,lat_bounds_array.num_values-1,lat_bounds_ds.get());
	    def.elongitude=data_array_value(lon_bounds_array,lon_bounds_array.num_values-1,lon_bounds_ds.get());
	    def.is_cell=true;
	  }
	}
	if (gatherxml::verbose_operation) {
	  std::cout << "...grid was identified as type " << static_cast<int>(def.type) << "..." << std::endl;
	}
	for (size_t m=0; m < level_info.ID.size(); ++m) {
	  my::map<metautils::StringEntry> gentry_table;
	  auto level_id=level_info.ID[m];
	  std::shared_ptr<InputHDF5Stream::Dataset> levels_ds;
	  HDF5::DataArray levels_array;
	  size_t num_levels;
	  if (m == (level_info.ID.size()-1) && level_id == "sfc") {
	    num_levels=1;
	  }
	  else {
	    levels_ds=istream.dataset("/"+level_id);
	    if (levels_ds == nullptr) {
		metautils::log_error("unable to access the /"+level_id+" dataset for level information","hdf2xml",USER);
	    }
	    levels_array.fill(istream,*levels_ds);
	    num_levels=levels_array.num_values;
	    InputHDF5Stream::Attribute attr;
	    if (levels_ds->attributes.found("bounds",attr) && attr.value._class_ == 3) {
		std::string attr_value=reinterpret_cast<char *>(attr.value.array);
		gcoords.time_bounds.ds=istream.dataset("/"+attr_value);
		if (gcoords.time_bounds.ds == nullptr) {
		  metautils::log_error("unable to get bounds for level '"+level_id+"'","hdf2xml",USER);
		}
		gcoords.time_bounds.data_array.fill(istream,*gcoords.time_bounds.ds);
	    }
	  }
	  for (const auto& key : time_range_table.keys()) {
	    metautils::NcTime::TimeRangeEntry tre;
	    time_range_table.found(key,tre);
	    metautils::NcTime::TimeData &tr_time_data= (gcoords.forecast_period.id.empty()) ? time_data : forecast_period_time_data;
	    add_gridded_lat_lon_keys(gentry_table,dim,def,tre,tr_time_data,gcoords,istream);
	    for (const auto& key2 : gentry_table.keys()) {
		if (gatherxml::verbose_operation) {
		  std::cout << "  processing grid entry: " << key2 << std::endl;
		}
		gentry->key=key2;
		auto key_parts=strutils::split(gentry->key,"<!>");
		InvEntry uie,gie;
		if (inv_stream.is_open()) {
		  inv_U_table.found(key_parts.back(),uie);
		  gie.key=key_parts[0];
		  for (size_t nn=1; nn < key_parts.size()-1; ++nn) {
		    gie.key+=","+key_parts[nn];
		  }
		  inv_G_table.found(gie.key,gie);
		}
		if (!grid_table->found(gentry->key,*gentry)) {
// new grid
		  gentry->level_table.clear();
		  lentry->parameter_code_table.clear();
		  param_entry->num_time_steps=0;
		  add_gridded_parameters_to_netcdf_level_entry(istream,gentry->key,gcoords,level_id,scan_data,tre,tr_time_data,time_bounds,parameter_data);
		  if (!lentry->parameter_code_table.empty()) {
		    for (size_t l=0; l < num_levels; ++l) {
			lentry->key="ds"+metautils::args.dsnum+","+level_id+":";
			if (gcoords.time_bounds.ds == nullptr) {
			  auto level_value= (levels_ds == nullptr) ? 0. : data_array_value(levels_array,l,levels_ds.get());
			  if (floatutils::myequalf(level_value,static_cast<int>(level_value),0.001)) {
			    lentry->key+=strutils::itos(level_value);
			  }
			  else {
			    lentry->key+=strutils::ftos(level_value,3);
			  }
			}
			else {
			  auto level_value=data_array_value(gcoords.time_bounds.data_array,l*2,gcoords.time_bounds.ds.get());
			  if (floatutils::myequalf(level_value,static_cast<int>(level_value),0.001)) {
			    lentry->key+=strutils::itos(level_value);
			  }
			  else {
			    lentry->key+=strutils::ftos(level_value,3);
			  }
			  level_value=data_array_value(gcoords.time_bounds.data_array,l*2+1,gcoords.time_bounds.ds.get());
			  lentry->key+=":";
			  if (floatutils::myequalf(level_value,static_cast<int>(level_value),0.001)) {
			    lentry->key+=strutils::itos(level_value);
			  }
			  else {
			    lentry->key+=strutils::ftos(level_value,3);
			  }
			}
			gentry->level_table.insert(*lentry);
			level_info.write[m]=1;
			if (inv_stream.is_open()) {
			  update_inventory(uie.num,gie.num,gcoords,tr_time_data);
			}
		    }
		  }
		  if (!gentry->level_table.empty()) {
		    grid_table->insert(*gentry);
		  }
 		}
		else {
// existing grid - needs update
		  for (size_t l=0; l < num_levels; ++l) {
		    lentry->key="ds"+metautils::args.dsnum+","+level_id+":";
		    auto level_value= (levels_ds == nullptr) ? 0. : data_array_value(levels_array,l,levels_ds.get());
		    if (floatutils::myequalf(level_value,static_cast<int>(level_value),0.001)) {
			lentry->key+=strutils::itos(level_value);
		    }
		    else {
			lentry->key+=strutils::ftos(level_value,3);
		    }
		    if (!gentry->level_table.found(lentry->key,*lentry)) {
			lentry->parameter_code_table.clear();
			add_gridded_parameters_to_netcdf_level_entry(istream,gentry->key,gcoords,level_id,scan_data,tre,tr_time_data,time_bounds,parameter_data);
			if (!lentry->parameter_code_table.empty()) {
			  gentry->level_table.insert(*lentry);
			  level_info.write[m]=1;
			}
		    }
		    else {
 			update_level_entry(istream,tre,tr_time_data,time_bounds,gcoords,level_id,scan_data,parameter_data,level_info.write[m]);
		    }
		    if (level_info.write[m] == 1 && inv_stream.is_open()) {
			update_inventory(uie.num,gie.num,gcoords,tr_time_data);
		    }
		  }
		  grid_table->replace(*gentry);
		}
	    }
	  }
	}
	error=metautils::NcLevel::write_level_map(level_info);
	if (!error.empty()) {
	  metautils::log_error(error,"hdf2xml",USER);
	}
    }
  }
  scan_data.write_type=ScanData::GrML_type;
  if (grid_table->size() == 0) {
    if (found_time) {
	std::cerr << "Terminating - no grids found and no content metadata will be generated" << std::endl;
    }
    else {
	std::cerr << "Terminating - no time coordinate could be identified and no content metadata will be generated" << std::endl;
    }
    exit(1);
  }
if (inv_stream.is_open()) {
InvEntry ie;
ie.key="x";
ie.num=0;
inv_R_table.insert(ie);
}
  if (gatherxml::verbose_operation) {
    std::cout << "...function scan_gridded_hdf5nc4_file() done." << std::endl;
  }
}

void scan_hdf5nc4_file(InputHDF5Stream& istream,ScanData& scan_data,gatherxml::markup::ObML::ObservationData& obs_data)
{
  auto ds=istream.dataset("/");
  if (ds == nullptr) {
    myerror="unable to access global attributes";
    exit(1);
  }
  scan_data.platform_type="unknown";
  InputHDF5Stream::Attribute attr;
  if (ds->attributes.found("platform",attr)) {
    std::string platform=reinterpret_cast<char *>(attr.value.array);
    if (!platform.empty()) {
	strutils::trim(platform);
	MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
	if (server) {
	  MySQL::LocalQuery query("ObML_platformType","search.GCMD_platforms","path = '"+platform+"'");
	  if (query.submit(server) == 0) {
	    MySQL::Row row;
	    if (query.fetch_row(row)) {
		scan_data.platform_type=row[0];
	    }
	  }
	}
    }
  }
  if (ds->attributes.found("featureType",attr)) {
    std::string feature_type=reinterpret_cast<char *>(attr.value.array);
    auto l_feature_type=strutils::to_lower(feature_type);
// patch for ICOADS netCDF4 IDs, which may be a mix, so ignore case
    if (ds->attributes.found("product_version",attr)) {
	std::string product_version=reinterpret_cast<char *>(attr.value.array);
	if (std::regex_search(product_version,std::regex("ICOADS")) && std::regex_search(product_version,std::regex("netCDF4"))) {
	  scan_data.convert_ids_to_upper_case=true;
	}
    }
    if (l_feature_type == "point") {
	scan_cf_point_hdf5nc4_file(istream,scan_data,obs_data);
    }
    else if (l_feature_type == "profile") {
	scan_cf_profile_hdf5nc4_file(istream,scan_data,obs_data);
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
  if (gatherxml::verbose_operation) {
    std::cout << "Beginning HDF5 file scan..." << std::endl;
  }
  gatherxml::markup::ObML::ObservationData obs_data;
  InputHDF5Stream istream;
  for (const auto& file : filelist) {
    if (gatherxml::verbose_operation) {
	std::cout << "Scanning file: " << file << std::endl;
    }
    if (!istream.open(file.c_str())) {
	myerror+=" - file: '"+file+"'";
	exit(1);
    }
    if (metautils::args.data_format == "ispdhdf5") {
	scan_ispd_hdf5_file(istream,scan_data,obs_data);
    }
    else if (metautils::args.data_format == "hdf5nc4") {
	scan_hdf5nc4_file(istream,scan_data,obs_data);
    }
    else if (metautils::args.data_format == "usarrthdf5") {
	scan_usarray_transportable_hdf5_file(istream,scan_data,obs_data);
    }
    else {
	std::cerr << "Error: bad data format specified" << std::endl;
	exit(1);
    }
    istream.close();
  }
  if (scan_data.write_type == ScanData::GrML_type) {
    scan_data.cmd_type="GrML";
    if (!metautils::args.inventory_only) {
	xml_directory=gatherxml::markup::GrML::write(*grid_table,"hdf2xml",USER);
    }
  }
  else if (scan_data.write_type == ScanData::ObML_type) {
    if (scan_data.num_not_missing > 0) {
	if (metautils::args.data_format != "hdf5nc4") {
	  metautils::args.data_format="hdf5";
	}
	scan_data.cmd_type="ObML";
	gatherxml::markup::ObML::write(obs_data,"hdf2xml",USER);
    }
    else {
	metautils::log_error("all stations have missing location information - no usable data found; no content metadata will be saved for this file","hdf2xml",USER);
    }
  }
  std::string map_type;
  if (scan_data.write_type == ScanData::GrML_type) {
    map_type="parameter";
  }
  else if (scan_data.write_type == ScanData::ObML_type) {
    map_type="dataType";
  }
  else {
    metautils::log_error("scan_hdf5_file() returned error: unknown map type","hdf2xml",USER);
  }
  std::string warning;
  auto error=metautils::NcParameter::write_parameter_map(scan_data.varlist,scan_data.var_changes_table,map_type,scan_data.map_name,scan_data.found_map,warning);
  if (!error.empty()) {
    metautils::log_error("scan_hdf5_file() returned error: "+error,"hdf2xml",USER);
  }
  if (gatherxml::verbose_operation) {
    std::cout << "HDF5 file scan completed." << std::endl;
  }
}

void scan_file(ScanData& scan_data)
{
  if (gatherxml::verbose_operation) {
    std::cout << "Beginning file scan..." << std::endl;
  }
  work_file.reset(new TempFile);
  if (!work_file->open(metautils::directives.temp_path)) {
    metautils::log_error("scan_file() was not able to create a temporary file in "+metautils::directives.temp_path,"hdf2xml",USER);
  }
  work_dir.reset(new TempDir);
  if (!work_dir->create(metautils::directives.temp_path)) {
    metautils::log_error("scan_file() was not able to create a temporary directory in "+metautils::directives.temp_path,"hdf2xml",USER);
  }
  std::string file_format,error;
  std::list<std::string> filelist;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*work_file,*work_dir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","hdf2xml",USER);
  }
  if (filelist.empty()) {
    filelist.emplace_back(work_file->name());
  }
  scan_data.tdir.reset(new TempDir);
  if (!scan_data.tdir->create(metautils::directives.temp_path)) {
    metautils::log_error("scan_file() was not able to create a temporary directory in "+metautils::directives.temp_path,"hdf2xml",USER);
  }
  if (std::regex_search(metautils::args.data_format,std::regex("hdf4"))) {
    scan_hdf4_file(filelist,scan_data);
  }
  else if (std::regex_search(metautils::args.data_format,std::regex("hdf5"))) {
    scan_hdf5_file(filelist,scan_data);
  }
  else {
    std::cerr << "Error: bad data format specified" << std::endl;
    exit(1);
  }
  if (gatherxml::verbose_operation) {
    std::cout << "File scan complete." << std::endl;
  }
}

int main(int argc,char **argv)
{
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
    std::cerr << "-V               verbose operation" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "<path>           full HPSS path or URL of the file to read" << std::endl;
    std::cerr << "                 - HPSS paths must begin with \"/FS/DECS\"" << std::endl;
    std::cerr << "                 - URLs must begin with \"http://{rda|dss}.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  auto arg_delimiter='%';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("hdf2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  atexit(clean_up);
  metautils::cmd_register("hdf2xml",USER);
  if (!metautils::args.overwrite_only) {
    metautils::check_for_existing_cmd("GrML");
    metautils::check_for_existing_cmd("ObML");
  }
  ScanData scan_data;
  scan_file(scan_data);
  if (!metautils::args.inventory_only) {
    if (metautils::args.update_db) {
	std::string flags;
	if (!metautils::args.update_summary) {
	  flags+=" -S ";
	}
	if (!metautils::args.regenerate) {
	  flags+=" -R ";
	}
	if (!xml_directory.empty()) {
	  flags+=" -t "+xml_directory;
	}
	if (std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
	  flags+=" -wf";
	}
	else {
	  flags+=" -f";
	}
	if (scan_data.cmd_type.empty()) {
	  metautils::log_error("content metadata type was not specified","hdf2xml",USER);
	}
	std::stringstream oss,ess;
	if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" "+flags+" "+metautils::args.filename+"."+scan_data.cmd_type,oss,ess) < 0) {
	  std::cerr << ess.str() << std::endl;
	}
    }
    else if (metautils::args.dsnum == "999.9" && !xml_directory.empty()) {
	std::cout << "Output is in:" << std::endl;
	std::cout << "  " << xml_directory << "/" << metautils::args.filename << ".";
	switch (scan_data.write_type) {
	  case ScanData::GrML_type: {
	    std::cout << "Gr";
	    break;
	  }
	  case ScanData::ObML_type: {
	    std::cout << "Ob";
	    break;
	  }
	  default: {
	    std::cout << "??";
	  }
	}
	std::cout << "ML" << std::endl;
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
    gatherxml::fileInventory::close(inv_file,&inv_dir,inv_stream,"GrML",true,true,"hdf2xml",USER);
  }
}
