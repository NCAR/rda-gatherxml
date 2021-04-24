#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <sstream>
#include <regex>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <gatherxml.hpp>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <netcdf.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>
#include <bufr.hpp>

using gatherxml::this_function_label;
using metautils::log_error2;
using metautils::log_warning;
using std::cerr;
using std::cout;
using std::endl;
using std::find;
using std::make_pair;
using std::move;
using std::pair;
using std::regex;
using std::regex_search;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;
using strutils::trim;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

static const size_t MISSING_FLAG = 0xffffffff;

struct ScanData {
  ScanData() : num_not_missing(0), write_type(-1), map_name(), parameter_map(),
      datatype_map(), netcdf_variables(), changed_variables(), map_filled(false)
      { }

  enum {GrML_type = 1, ObML_type};
  size_t num_not_missing;
  int write_type;
  string map_name;
  ParameterMap parameter_map;
  DataTypeMap datatype_map;
  vector<string> netcdf_variables;
  unordered_set<string> changed_variables;
  bool map_filled;
};

struct GridData {
  struct CoordinateData {
    CoordinateData() : dim(MISSING_FLAG), id(), type() { }

    size_t dim;
    string id;
    NetCDF::DataType type;
  };
  GridData(): time(), time_bounds(), lats(), lats_b(), lons(), lons_b(),
      levels(), levdata() { }

  CoordinateData time, time_bounds;
  vector<CoordinateData> lats, lats_b, lons, lons_b, levels;
  metautils::NcLevel::LevelInfo levdata;
};

struct NetCDFVariableAttributeData {
  NetCDFVariableAttributeData() : long_name(),units(),cf_keyword(),missing_value() { }

  string long_name,units,cf_keyword;
  NetCDF::DataValue missing_value;
};

metautils::NcTime::Time time_s;
metautils::NcTime::TimeBounds time_bounds_s;
metautils::NcTime::TimeData time_data;
gatherxml::markup::ObML::IDEntry ientry;
unique_ptr<my::map<gatherxml::markup::GrML::GridEntry>> gridtbl_p;
unique_ptr<gatherxml::markup::GrML::GridEntry> gentry_p;
unique_ptr<gatherxml::markup::GrML::LevelEntry> lentry_p;
unique_ptr<gatherxml::markup::GrML::ParameterEntry> pentry_p;
string inv_file;
TempDir *inv_dir=nullptr;
std::ofstream inv_stream;
unordered_map<string,pair<int,string>> D_map,G_map,I_map,L_map,O_map,P_map,R_map,U_map;

struct InvTimeEntry {
  InvTimeEntry() : key(),dt() { }

  size_t key;
  string dt;
};

vector<string> inv_lines;
TempFile inv_lines2("/tmp");
unordered_set<string> unknown_IDs;
string conventions;
bool is_large_offset=false;

unordered_map<string, string> id_platform_map{
    { "AUSTRALIA", "land_station" },
    { "COOP", "land_station" },
    { "WBAN", "land_station" }
};

extern "C" void clean_up() {
  if (!myerror.empty()) {
    log_error2(myerror, "clean_up()", "nc2xml", USER);
  }
}

extern "C" void segv_handler(int) {
  clean_up();
  metautils::cmd_unregister();
  log_error2("core dump", "segv_handler()", "nc2xml", USER);
}

extern "C" void int_handler(int) {
  clean_up();
  metautils::cmd_unregister();
}

void grid_initialize() {
  if (gridtbl_p == nullptr) {
    gridtbl_p.reset(new my::map<gatherxml::markup::GrML::GridEntry>);
    gentry_p.reset(new gatherxml::markup::GrML::GridEntry);
    lentry_p.reset(new gatherxml::markup::GrML::LevelEntry);
    pentry_p.reset(new gatherxml::markup::GrML::ParameterEntry);
  }
}

void sort_inventory_map(unordered_map<string, pair<int, string>>& inv_table,
     vector<pair<int, string>>& sorted_keys) {
  sorted_keys.clear();
  for (const auto& e : inv_table) {
    sorted_keys.emplace_back(make_pair(e.second.first, e.first));
  }
  std::sort(sorted_keys.begin(), sorted_keys.end(),
  [](const pair<int, string>& left, const pair<int, string>& right) -> bool {
    if (left.first <= right.first) {
      return true;
    }
    return false;
  });
}

void fill_nc_time_data(const InputNetCDFStream::Attribute& attr)
{
  static const string THIS_FUNC=this_function_label(__func__);
  auto u=*(reinterpret_cast<string *>(attr.values));
  if (gatherxml::verbose_operation) {
    cout << "  Time units: '" << u << "'" << endl;
  }
  if (regex_search(u,regex("since"))) {
    time_data.units=to_lower(u.substr(0,u.find("since")));
    trim(time_data.units);
    u=u.substr(u.find("since")+5);
    while (!u.empty() && (u[0] < '0' || u[0] > '9')) {
      u=u.substr(1);
    }
    auto n=u.length()-1;
    while (n > 0 && (u[n] < '0' || u[n] > '9')) {
      --n;
    }
    ++n;
    if (n < u.length()) {
      u=u.substr(0,n);
    }
    trim(u);
    auto uparts=split(u);
    if (uparts.size() < 1 || uparts.size() > 3) {
      log_error2("unable to get reference time from units specified as: '"+*(reinterpret_cast<string *>(attr.values))+"'",THIS_FUNC,"nc2xml",USER);
    }
    auto dparts=split(uparts[0],"-");
    if (dparts.size() != 3) {
      log_error2("unable to get reference time from units specified as: '"+*(reinterpret_cast<string *>(attr.values))+"'",THIS_FUNC,"nc2xml",USER);
    }
    time_data.reference.set_year(stoi(dparts[0]));
    time_data.reference.set_month(stoi(dparts[1]));
    time_data.reference.set_day(stoi(dparts[2]));
    if (uparts.size() > 1) {
      auto tparts=split(uparts[1],":");
      switch (tparts.size()) {
        case 1: { time_data.reference.set_time(stoi(tparts[0])*10000);
          break;
        }
        case 2: { time_data.reference.set_time(stoi(tparts[0])*10000+stoi(tparts[1])*100);
          break;
        }
        case 3: { time_data.reference.set_time(stoi(tparts[0])*10000+stoi(tparts[1])*100+static_cast<int>(std::stof(tparts[2])));
          break;
        }
      }
    }
    if (gatherxml::verbose_operation) {
      cout << "  Reference time set to: " << time_data.reference.to_string("%Y-%m-%d %H:%MM:%SS") << endl;
    }
  } else {
    log_error2("unable to get CF time from time variable units",THIS_FUNC,"nc2xml",USER);
  }
}

DateTime compute_nc_time(NetCDF::VariableData& times,size_t index)
{
  static const string THIS_FUNC=this_function_label(__func__);
  DateTime dt;
  double val=-1;
  static int this_year=dateutils::current_date_time().year();

  val=times[index];
  if (val < 0.) {
    log_error2("Terminating - negative time offset not allowed",THIS_FUNC,"nc2xml",USER);
  }
  if (time_data.units == "seconds") {
    dt=time_data.reference.seconds_added(val);
  } else if (time_data.units == "minutes") {
    if (floatutils::myequalf(val,static_cast<int>(val),0.001)) {
      dt=time_data.reference.minutes_added(val);
    } else {
      dt=time_data.reference.seconds_added(lroundf(val*60.));
    }
  } else if (time_data.units == "hours") {
    if (floatutils::myequalf(val,static_cast<int>(val),0.001)) {
      dt=time_data.reference.hours_added(val);
    } else {
      dt=time_data.reference.seconds_added(lroundf(val*3600.));
    }
  } else if (time_data.units == "days") {
    if (floatutils::myequalf(val,static_cast<int>(val),0.001)) {
      dt=time_data.reference.days_added(val);
    } else {
      dt=time_data.reference.seconds_added(lroundf(val*86400.));
    }
  } else {
    log_error2("compute_nc_time() returned error: unable to set date/time for units '"+time_data.units+"'",THIS_FUNC,"nc2xml",USER);
  }
  if (gatherxml::verbose_operation && dt.year() > this_year) {
    cout << "Warning: " << dt.to_string() << " is in the future; time value: " << val << "; time type: " << static_cast<int>(times.type()) << endl;
  }
  return dt;
}

void extract_from_variable_attribute(const vector<InputNetCDFStream::Attribute>& attribute_list,NetCDF::DataType data_type,NetCDFVariableAttributeData& nc_attribute_data)
{
  nc_attribute_data.long_name="";
  nc_attribute_data.units="";
  nc_attribute_data.cf_keyword="";
  nc_attribute_data.missing_value.clear();
  for (const auto& attr : attribute_list) {
    if (attr.name == "long_name") {
      nc_attribute_data.long_name=*(reinterpret_cast<string *>(attr.values));
      trim(nc_attribute_data.long_name);
    } else if (attr.name == "units") {
      nc_attribute_data.units=*(reinterpret_cast<string *>(attr.values));
      trim(nc_attribute_data.units);
    } else if (attr.name == "standard_name" && conventions.length() > 2 && conventions.substr(0,2) == "CF") {
      nc_attribute_data.cf_keyword=*(reinterpret_cast<string *>(attr.values));
      trim(nc_attribute_data.cf_keyword);
    } else if (attr.name == "_FillValue" || attr.name == "missing_value") {
      nc_attribute_data.missing_value.resize(data_type);
      switch (data_type) {
        case NetCDF::DataType::CHAR: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<char *>(attr.values)));
          break;
        }
        case NetCDF::DataType::SHORT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<short *>(attr.values)));
          break;
        }
        case NetCDF::DataType::INT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<int *>(attr.values)));
          break;
        }
        case NetCDF::DataType::FLOAT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<float *>(attr.values)));
          break;
        }
        case NetCDF::DataType::DOUBLE: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<double *>(attr.values)));
          break;
        }
        default: { }
      }
    } else if ((regex_search(attr.name,regex("^comment")) || regex_search(attr.name,regex("^Comment"))) && nc_attribute_data.long_name.empty()) {
      nc_attribute_data.long_name=*(reinterpret_cast<string *>(attr.values));
    } else if (to_lower(attr.name) == "description" && nc_attribute_data.long_name.empty()) {
      nc_attribute_data.long_name=*(reinterpret_cast<string *>(attr.values));
    }
  }
}

bool found_missing(const double& time,const NetCDF::DataValue *time_missing_value,const double& var_value,const NetCDF::DataValue& var_missing_value)
{
  bool missing;
  if (time_missing_value == nullptr) {
    missing=false;
  } else {
    if (floatutils::myequalf(time,time_missing_value->get())) {
      missing=true;
    } else {
      missing=false;
    }
  }
  if (missing) {
    return true;
  }
  if (var_missing_value.type() == NetCDF::DataType::_NULL) {
    missing=false;
  } else {
    missing=true;
    if (var_value != var_missing_value.get()) {
      missing=false;
    }
  }
  return missing;
}

void add_gridded_netcdf_parameter(const NetCDF::Variable& var, const DateTime&
     first_valid_date_time, const DateTime& last_valid_date_time, int nsteps,
     unordered_set<string>& parameter_table, ScanData& scan_data)
{
  NetCDFVariableAttributeData nc_va_data;
  extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
  auto param_key=var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
  if (parameter_table.find(param_key) == parameter_table.end()) {
    parameter_table.emplace(param_key);
    scan_data.netcdf_variables.emplace_back(param_key);
    auto short_name=scan_data.parameter_map.short_name(var.name);
    if (!short_name.empty()) {
      scan_data.changed_variables.emplace(var.name);
    }
  }
  pentry_p->start_date_time=first_valid_date_time;
  pentry_p->end_date_time=last_valid_date_time;
  pentry_p->num_time_steps=nsteps;
  lentry_p->parameter_code_table.insert(*pentry_p);
  if (inv_stream.is_open()) {
    if (P_map.find(pentry_p->key) == P_map.end()) {
      P_map.emplace(pentry_p->key,make_pair(P_map.size(),""));
    }
  }
}

bool is_zonal_mean_grid_variable(const NetCDF::Variable& var,size_t timedimid,int levdimid,size_t latdimid)
{
  if ((var.dimids.size() == 2 || var.dimids.size() == 3) && var.dimids[0] == timedimid && ((var.dimids.size() == 3 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid) || (var.dimids.size() == 2 && levdimid < 0 && var.dimids[1] == latdimid))) {
    return true;
  } else {
    return false;
  }
}

bool is_regular_lat_lon_grid_variable(const NetCDF::Variable& var,size_t timedimid,int levdimid,size_t latdimid,size_t londimid)
{
  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid && var.dimids[3] == londimid) || (var.dimids.size() == 3 && levdimid < 0 && var.dimids[1] == latdimid && var.dimids[2] == londimid))) {
    return true;
  } else {
    return false;
  }
}

bool is_polar_stereographic_grid_variable(const NetCDF::Variable& var,size_t timedimid,int levdimid,size_t latdimid)
{
  size_t ltdimid=latdimid/10000-1;
  size_t lndimid=(latdimid % 10000)/100-1;

  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == ltdimid && var.dimids[3] == lndimid) || (var.dimids.size() == 3 && levdimid < 0 && var.dimids[1] == ltdimid && var.dimids[2] == lndimid))) {
    return true;
  } else {
    return false;
  }
}

string gridded_time_method(const NetCDF::Variable& var,string timeid)
{
  for (size_t n=0; n < var.attrs.size(); ++n) {
    if (var.attrs[n].name == "cell_methods") {
      auto cell_methods=*(reinterpret_cast<string *>(var.attrs[n].values));
      auto re=regex("  ");
      while (regex_search(cell_methods,re)) {
        replace_all(cell_methods,"  "," ");
      }
      replace_all(cell_methods,"comment: ","");
      replace_all(cell_methods,"comments: ","");
      replace_all(cell_methods,"comment:","");
      replace_all(cell_methods,"comments:","");
      if (!cell_methods.empty() && regex_search(cell_methods,regex(substitute(timeid,".","\\.")+": "))) {
        auto idx=cell_methods.find(timeid+": ");
        if (idx != string::npos) {
          cell_methods=cell_methods.substr(idx);
        }
        replace_all(cell_methods,timeid+": ","");
        trim(cell_methods);
        idx=cell_methods.find(": ");
        if (idx == string::npos) {
          return cell_methods;
        } else {

          // filter out other coordinates for this cell method
          auto idx2=cell_methods.find(" ");
          while (idx2 != string::npos && idx2 > idx) {
            cell_methods=cell_methods.substr(idx2+1);
            idx=cell_methods.find(": ");
            idx2=cell_methods.find(" ");
          }
          idx=cell_methods.find(")");

          // no extra information in parentheses
          if (idx == string::npos) {
            idx=cell_methods.find(" ");
            return cell_methods.substr(0,idx);
          } else {

            // found extra information
            return cell_methods.substr(0,idx+1);
          }
        }
      }
    }
  }
  return "";
}

void add_grid_to_inventory(string gentry_key)
{
  int idx=gentry_key.rfind("<!>");
  auto G_key=substitute(gentry_key.substr(0,idx),"<!>",",");
  if (G_map.find(G_key) == G_map.end()) {
    G_map.emplace(G_key,make_pair(G_map.size(),""));
  }
}

void add_level_to_inventory(string lentry_key,string gentry_key,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,InputNetCDFStream& istream)
{
  static const string THIS_FUNC=this_function_label(__func__);
  if (L_map.find(lentry_key) == L_map.end()) {
    L_map.emplace(lentry_key,make_pair(L_map.size(),""));
  }
  auto idx=gentry_key.rfind("<!>");
  string sdum="|"+itos(U_map[gentry_key.substr(idx+3)].first)+"|"+itos(G_map[substitute(gentry_key.substr(0,idx),"<!>",",")].first)+"|"+itos(L_map[lentry_key].first);
  auto dims=istream.dimensions();
  auto vars=istream.variables();
  for (size_t n=0; n < vars.size(); ++n) {
    auto key="ds"+metautils::args.dsnum+":"+vars[n].name;
    if (vars[n].dimids.size() > 0 && !vars[n].is_coord && vars[n].dimids[0] == timedimid && P_map.find(key) != P_map.end() && ((vars[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(vars[n].dimids[1]) == levdimid && vars[n].dimids[2] == latdimid && vars[n].dimids[3] == londimid) || (vars[n].dimids.size() == 3 && levdimid < 0 && vars[n].dimids[1] == latdimid && vars[n].dimids[2] == londimid))) {
      auto R_key=itos(static_cast<int>(vars[n].data_type));
      if (R_map.find(R_key) == R_map.end()) {
        R_map.emplace(R_key,make_pair(R_map.size(),""));
      }
      auto var_size=vars[n].size;
      if (vars[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(vars[n].dimids[1]) == levdimid) {
        var_size/=dims[levdimid].length;
      }
      long long off=vars[n].offset;
      for (size_t m=0; m < time_s.num_times; ++m) {
        string error;
        inv_lines.emplace_back(lltos(off)+"|"+itos(var_size)+"|"+metautils::NcTime::actual_date_time(time_s.times[m],time_data,error).to_string("%Y%m%d%H%MM")+sdum+"|"+itos(P_map[key].first)+"|"+itos(R_map[R_key].first));
        if (!error.empty()) {
          log_error2(error,THIS_FUNC,"nc2xml",USER);
        }
        if (off > static_cast<long long>(0xffffffff)) {
          is_large_offset=true;
        }
        off+=istream.record_size();
      }
    }
  }
}

void add_gridded_parameters_to_netcdf_level_entry(const vector<NetCDF
    ::Variable>& vars, string gentry_key, string timeid, size_t timedimid, int
    levdimid, size_t latdimid, size_t londimid, const metautils::NcTime
    ::TimeRangeEntry& tre, unordered_set<string>& parameter_table,
    ScanData& scan_data) {
  static const string THIS_FUNC=this_function_label(__func__);

  // find all of the parameters
  for (size_t n=0; n < vars.size(); ++n) {
    if (!vars[n].is_coord) {
      auto time_method=gridded_time_method(vars[n],timeid);
      DateTime first_valid_date_time,last_valid_date_time;
      if (time_method.empty() || (floatutils::myequalf(time_bounds_s.t1,0,0.0001) && floatutils::myequalf(time_bounds_s.t1,time_bounds_s.t2,0.0001))) {
        first_valid_date_time=tre.data->instantaneous.first_valid_datetime;
        last_valid_date_time=tre.data->instantaneous.last_valid_datetime;
      } else {
        if (time_bounds_s.changed) {
          log_error2("time bounds changed",THIS_FUNC,"nc2xml",USER);
        }
        first_valid_date_time=tre.data->bounded.first_valid_datetime;
        last_valid_date_time=tre.data->bounded.last_valid_datetime;
      }
      time_method=strutils::capitalize(time_method);
      string error;
      auto tr_description=metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
      if (!error.empty()) {
        log_error2(error,THIS_FUNC,"nc2xml",USER);
      }
//      tr_description=strutils::capitalize(tr_description);
      if (strutils::has_ending(gentry_key,tr_description)) {

        // check as a zonal mean grid variable
        if (regex_search(gentry_key,regex("^[12]<!>1<!>"))) {
          if (is_zonal_mean_grid_variable(vars[n],timedimid,levdimid,latdimid)) {
            pentry_p->key="ds"+metautils::args.dsnum+":"+vars[n].name;
            add_gridded_netcdf_parameter(vars[n],first_valid_date_time,last_valid_date_time,tre.data->num_steps,parameter_table,scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        } else if (vars[n].dimids.size() == 3 || vars[n].dimids.size() == 4) {
          if (is_regular_lat_lon_grid_variable(vars[n],timedimid,levdimid,latdimid,londimid)) {

            // check as a regular lat/lon grid variable
            pentry_p->key="ds"+metautils::args.dsnum+":"+vars[n].name;
            add_gridded_netcdf_parameter(vars[n],first_valid_date_time,last_valid_date_time,tre.data->num_steps,parameter_table,scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          } else if (is_polar_stereographic_grid_variable(vars[n],timedimid,levdimid,latdimid)) {

            // check as a polar-stereographic grid variable
            pentry_p->key="ds"+metautils::args.dsnum+":"+vars[n].name;
            add_gridded_netcdf_parameter(vars[n],first_valid_date_time,last_valid_date_time,tre.data->num_steps,parameter_table,scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        }
      }
    }
  }
}

void add_gridded_time_range(string key_start,vector<string>& gentry_keys,string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,const metautils::NcTime::TimeRangeEntry& tre,vector<NetCDF::Variable>& vars)
{
  static const string THIS_FUNC=this_function_label(__func__);
  string gentry_key;
  unordered_set<string> unique_gentry_keys;
  auto found_var_with_no_time_method=false;
  for (size_t n=0; n < vars.size(); ++n) {
    if (!vars[n].is_coord && vars[n].dimids.size() >= 3 && (is_zonal_mean_grid_variable(vars[n],timedimid,levdimid,latdimid) || is_regular_lat_lon_grid_variable(vars[n],timedimid,levdimid,latdimid,londimid) || is_polar_stereographic_grid_variable(vars[n],timedimid,levdimid,latdimid))) {
      auto time_method=gridded_time_method(vars[n],timeid);
      if (time_method.empty()) {
        found_var_with_no_time_method=true;
      }
      else {
        string error;
        gentry_key=key_start+metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,time_method,error);
        if (!error.empty()) {
          log_error2(error,THIS_FUNC,"nc2xml",USER);
        }
        if (unique_gentry_keys.find(gentry_key) == unique_gentry_keys.end()) {
          gentry_keys.emplace_back(gentry_key);
          unique_gentry_keys.emplace(gentry_key);
        }
      }
    }
  }
  if (found_var_with_no_time_method) {
    string error;
    gentry_key=key_start+metautils::NcTime::gridded_netcdf_time_range_description(tre,time_data,"",error);
    if (!error.empty()) {
      log_error2(error,THIS_FUNC,"nc2xml",USER);
    }
    gentry_keys.emplace_back(gentry_key);
  }
}

void add_gridded_lat_lon_keys(vector<string>& gentry_keys,Grid::GridDimensions dim,Grid::GridDefinition def,string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,const metautils::NcTime::TimeRangeEntry& tre,vector<NetCDF::Variable>& vars)
{
  string key_start;
  switch (def.type) {
    case Grid::Type::latitudeLongitude:
    case Grid::Type::gaussianLatitudeLongitude: {
      key_start=itos(static_cast<int>(def.type));
      if (def.is_cell) {
        key_start+="C";
      }
      key_start+="<!>"+itos(dim.x)+"<!>"+itos(dim.y)+"<!>"+ftos(def.slatitude,3)+"<!>"+ftos(def.slongitude,3)+"<!>"+ftos(def.elatitude,3)+"<!>"+ftos(def.elongitude,3)+"<!>"+ftos(def.loincrement,3)+"<!>"+ftos(def.laincrement,3)+"<!>";
      add_gridded_time_range(key_start,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
      key_start=itos(static_cast<int>(def.type))+"<!>1<!>"+itos(dim.y)+"<!>"+ftos(def.slatitude,3)+"<!>0<!>"+ftos(def.elatitude,3)+"<!>360<!>"+ftos(def.laincrement,3)+"<!>"+ftos(def.laincrement,3)+"<!>";
      add_gridded_time_range(key_start,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
      break;
    }
    case Grid::Type::polarStereographic: {
      key_start=itos(static_cast<int>(def.type));
      if (def.is_cell) {
        key_start+="C";
      }
      key_start+="<!>"+itos(dim.x)+"<!>"+itos(dim.y)+"<!>"+ftos(def.slatitude,3)+"<!>"+ftos(def.slongitude,3)+"<!>"+ftos(def.llatitude,3)+"<!>"+ftos(def.olongitude,3)+"<!>"+ftos(def.dx,3)+"<!>"+ftos(def.dy,3)+"<!>";
      if (def.projection_flag == 0) {
        key_start+="N";
      }
      else {
        key_start+="S";
      }
      key_start+="<!>";
      add_gridded_time_range(key_start,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
      break;
    }
    case Grid::Type::mercator: {
      key_start=itos(static_cast<int>(def.type));
      if (def.is_cell) {
        key_start+="C";
      }
      key_start+="<!>"+itos(dim.x)+"<!>"+itos(dim.y)+"<!>"+ftos(def.slatitude,3)+"<!>"+ftos(def.slongitude,3)+"<!>"+ftos(def.elatitude,3)+"<!>"+ftos(def.elongitude,3)+"<!>"+ftos(def.loincrement,3)+"<!>"+ftos(def.laincrement,3)+"<!>";
      add_gridded_time_range(key_start,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
      break;
    }
    default: { }
  }
}

void add_gridded_zonal_mean_keys(vector<string>& gentry_keys,Grid::GridDimensions dim,Grid::GridDefinition def,string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,metautils::NcTime::TimeRangeEntry &tre,vector<NetCDF::Variable>& vars)
{
  string key_start;

  key_start="1<!>1<!>"+itos(dim.y)+"<!>"+ftos(def.slatitude,3)+"<!>0<!>"+ftos(def.elatitude,3)+"<!>360<!>"+ftos(def.laincrement,3)+"<!>"+ftos(def.laincrement,3)+"<!>";
  add_gridded_time_range(key_start,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
}

bool found_netcdf_time_from_patch(const NetCDF::Variable& var) {

  // ds260.1
  auto status = 0;
  for (size_t m = 0; m < var.attrs.size(); ++m) {
    if (var.attrs[m].data_type == NetCDF::DataType::CHAR) {
      if (var.attrs[m].name == "units") {
        time_data.units = *(reinterpret_cast<string *>(var.attrs[m].values)) +
            "s";
        ++status;
      } else if (var.attrs[m].name == "comment") {
        auto comment_value = *(reinterpret_cast<string *>(var.attrs[m].values));
        auto sp=split(comment_value);
        auto dt = stoll(sp[sp.size() - 1]) * 10000000000 + 100000000 + 1000000;
        time_data.reference.set(dt);
        ++status;
      }
    }
    if (status == 2) {
      if (time_data.units == "hours") {
        time_data.reference.subtract_hours(1);
      } else if (time_data.units == "days") {
        time_data.reference.subtract_days(1);
      } else if (time_data.units == "months") {
        time_data.reference.subtract_months(1);
      } else {
        return false;
      }
      return true;
    }
  }
  return false;
}

bool ignore_cf_variable(const NetCDF::Variable& var)
{
  string vname=to_lower(var.name);
  size_t n;

  if (vname == "time" || vname == "time_bounds" || vname == "year" || vname == "month" || vname == "day" || vname == "doy" || vname == "hour" || vname == "latitude" || vname == "longitude") {
    return true;
  }
  for (n=0; n < var.attrs.size(); ++n) {
    if (var.attrs[n].name == "cf_role") {
      return true;
    }
  }
  return false;
}

struct DiscreteGeometriesData {
  DiscreteGeometriesData() : indexes(),z_units(),z_pos() { }

  struct Indexes {
    Indexes() : time_var(MISSING_FLAG),time_bounds_var(MISSING_FLAG),stn_id_var(MISSING_FLAG),network_var(MISSING_FLAG),lat_var(MISSING_FLAG),lat_var_bounds(MISSING_FLAG),lon_var(MISSING_FLAG),lon_var_bounds(MISSING_FLAG),sample_dim_var(MISSING_FLAG),instance_dim_var(MISSING_FLAG),z_var(MISSING_FLAG) { }

    size_t time_var,time_bounds_var,stn_id_var,network_var,lat_var,lat_var_bounds,lon_var,lon_var_bounds,sample_dim_var,instance_dim_var,z_var;
  };
  Indexes indexes;
  string z_units,z_pos;
};

void process_units_attribute(const vector<NetCDF::Variable>& vars,size_t var_index,size_t attr_index,DiscreteGeometriesData& dgd)
{
  static const string THIS_FUNC=this_function_label(__func__);
  auto u=*(reinterpret_cast<string *>(vars[var_index].attrs[attr_index].values));
  u=to_lower(u);
  if (regex_search(u,regex("since"))) {
    if (dgd.indexes.time_var != MISSING_FLAG) {
      log_error2("time was already identified - don't know what to do with variable: "+vars[var_index].name,THIS_FUNC,"nc2xml",USER);
    }
    fill_nc_time_data(vars[var_index].attrs[attr_index]);
    dgd.indexes.time_var=var_index;
  } else if (regex_search(u,regex("^degree(s){0,1}(_){0,1}((north)|N)$"))) {
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      dgd.indexes.lat_var=var_index;
    } else {
      for (size_t n=0; n < vars[var_index].attrs.size(); ++n) {
        if (regex_search(vars[var_index].attrs[n].name,regex("bounds",std::regex_constants::icase))) {
          auto v=*(reinterpret_cast<string *>(vars[var_index].attrs[n].values));
          if (v == vars[dgd.indexes.lat_var].name) {
            dgd.indexes.lat_var=var_index;
          }
        }
      }
    }
  } else if (regex_search(u,regex("^degree(s){0,1}(_){0,1}((east)|E)$"))) {
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      dgd.indexes.lon_var=var_index;
    } else {
      for (size_t n=0; n < vars[var_index].attrs.size(); ++n) {
        if (regex_search(vars[var_index].attrs[n].name,regex("bounds",std::regex_constants::icase))) {
          auto v=*(reinterpret_cast<string *>(vars[var_index].attrs[n].values));
          if (v == vars[dgd.indexes.lon_var].name) {
            dgd.indexes.lon_var=var_index;
          }
        }
      }
    }
  }
}

void scan_cf_point_netcdf_file(InputNetCDFStream& istream, string platform_type,
    ScanData& scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function "+THIS_FUNC+"()..." << endl;
  }
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(vars,n,m,dgd);
        }
      }
    }
  }
  NetCDF::VariableData times;
  if (dgd.indexes.time_var == MISSING_FLAG) {
    log_error2("unable to determine time variable",THIS_FUNC,"nc2xml",USER);
  } else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
      if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
        time_data.calendar=*(reinterpret_cast<string *>(vars[dgd.indexes.time_var].attrs[n].values));
      }
    }
    if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
      log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
    }
  }
  NetCDF::VariableData lats;
  if (dgd.indexes.lat_var == MISSING_FLAG) {
    log_error2("unable to determine latitude variable",THIS_FUNC,"nc2xml",USER);
  } else {
    if (istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
      log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
    }
  }
  NetCDF::VariableData lons;
  if (dgd.indexes.lon_var == MISSING_FLAG) {
    log_error2("unable to determine longitude variable",THIS_FUNC,"nc2xml",USER);
  } else {
    if (istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
      log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
    }
  }
  vector<DateTime> date_times;
  vector<string> IDs,datatypes_list;
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.time_var].name && var.name != vars[dgd.indexes.lat_var].name && var.name != vars[dgd.indexes.lon_var].name) {
      NetCDF::VariableData var_data;
      if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get variable data for '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
      datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
      for (size_t n=0; n < times.size(); ++n) {
        if (n == date_times.size()) {
          date_times.emplace_back(compute_nc_time(times,n));
        }
        if (n == IDs.size()) {
          auto lat=ftos(fabs(lats[n]),4);
          if (lats[n] < 0.) {
            lat+="S";
          } else {
            lat+="N";
          }
          auto lon=ftos(fabs(lons[n]),4);
          if (lons[n] < 0.) {
            lon+="W";
          } else {
            lon+="E";
          }
          IDs.emplace_back(lat+lon);
        }
        if (!found_missing(times[n],nullptr,var_data[n],nc_va_data.missing_value)) {
          if (!obs_data.added_to_platforms("surface",platform_type,lats[n],lons[n])) {
            auto error=move(myerror);
            log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
          }
          ientry.key=platform_type+"[!]latlon[!]"+IDs[n];
          if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[n],lons[n],times[n],&date_times[n])) {
            auto error=move(myerror);
            log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
          }
          ++scan_data.num_not_missing;
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << THIS_FUNC << "() done." << endl;
  }
}

void scan_cf_orthogonal_time_series_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, unordered_map<size_t,
    string>& T_map, ScanData& scan_data, gatherxml::markup::ObML
    ::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function "+THIS_FUNC+"()..." << endl;
  }
  gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"ObML","nc2xml",USER);
  if (inv_stream.is_open()) {
    inv_stream << "netCDF:timeSeries|" << istream.record_size() << endl;
    O_map.emplace("surface",make_pair(O_map.size(),""));
  }
  NetCDF::VariableData lats,lons,ids;
  NetCDF::DataType ids_type=NetCDF::DataType::_NULL;
  size_t id_len=0;
  vector<string> platform_types,id_types,id_cache;
  if (dgd.indexes.lat_var == MISSING_FLAG || dgd.indexes.lon_var ==
      MISSING_FLAG || dgd.indexes.stn_id_var == MISSING_FLAG) {

    // lat/lon not found, look for known alternates in global attributes
    auto gattrs=istream.global_attributes();
    size_t known_sources=MISSING_FLAG;
    for (size_t n=0; n < gattrs.size(); ++n) {
      if (to_lower(gattrs[n].name) == "title") {
        if (to_lower(*(reinterpret_cast<string *>(gattrs[n].values))) == "hadisd") {
          known_sources=0x1;
          break;
        }
      }
    }
    if (known_sources == 0x1) {

      // HadISD
      for (size_t n=0; n < gattrs.size(); ++n) {
        if (gattrs[n].name == "latitude") {
          lats.resize(1,NetCDF::DataType::FLOAT);
          lats.set(0,*(reinterpret_cast<float *>(gattrs[n].values)));
        } else if (gattrs[n].name == "longitude") {
          lons.resize(1,NetCDF::DataType::FLOAT);
          lons.set(0,*(reinterpret_cast<float *>(gattrs[n].values)));
        } else if (gattrs[n].name == "station_id") {
          auto id=*(reinterpret_cast<string *>(gattrs[n].values));
          id=id.substr(0,id.find("-"));
          id_cache.emplace_back(id);
          id_len=id.length();
          ids_type=NetCDF::DataType::CHAR;
          ids.resize(id_len,ids_type);
          for (size_t m=0; m < id_len; ++m) {
            ids.set(m,id[m]);
          }
          id_types.emplace_back("WMO+6");
          if (id >= "990000" && id < "991000") {
            platform_types.emplace_back("fixed_ship");
          } else if ((id >= "992000" && id < "993000") || (id >= "995000" && id < "998000")) {
            platform_types.emplace_back("drifting_buoy");
          } else {
            platform_types.emplace_back("land_station");
          }
        }
      }
      if (!obs_data.added_to_platforms("surface",platform_types.back(),lats[0],lons[0])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+platform_types.back(),THIS_FUNC,"nc2xml",USER);
      }
      if (inv_stream.is_open()) {
        auto I_key=id_types.back()+"[!]"+id_cache.back();
        if (I_map.find(I_key) == I_map.end()) {
            I_map.emplace(I_key,make_pair(I_map.size(),ftos(lats.back(),4)+"[!]"+ftos(lons.back(),4)));
        }
      }
    }
  }
  if (dgd.indexes.lat_var == MISSING_FLAG || dgd.indexes.lon_var == MISSING_FLAG || dgd.indexes.stn_id_var == MISSING_FLAG) {
    string error;
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      if (!error.empty()) {
        error+=", ";
      }
      error+="latitude could not be identified";
    }
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      if (!error.empty()) {
        error+=", ";
      }
      error+="longitude could not be identified";
    }
    if (dgd.indexes.stn_id_var == MISSING_FLAG) {
      if (!error.empty()) {
        error+=", ";
      }
      error+="timeseries_id role could not be identified";
    }
    log_error2(error,THIS_FUNC,"nc2xml",USER);
  }
  auto vars=istream.variables();
  NetCDF::VariableData times;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  if (lats.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (lons.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (ids.type() == NetCDF::DataType::_NULL && (ids_type=istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids)) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  auto dims=istream.dimensions();
  if (ids_type == NetCDF::DataType::CHAR && dgd.indexes.stn_id_var != MISSING_FLAG) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  auto stn_dim=-1;
  size_t num_stns=0;
  if (dgd.indexes.stn_id_var != MISSING_FLAG) {
    if (vars[dgd.indexes.stn_id_var].dimids.size() >= 1) {
      stn_dim=vars[dgd.indexes.stn_id_var].dimids.front();
      if (vars[dgd.indexes.stn_id_var].is_rec) {
        num_stns=istream.num_records();
      }
      else {
        num_stns=dims[stn_dim].length;
      }
    } else {
      num_stns=1;
    }
  }
  if (platform_types.size() == 0) {
    string id;
    for (size_t n=0; n < num_stns; ++n) {
      if (ids_type == NetCDF::DataType::SHORT || ids_type == NetCDF::DataType::INT || ids_type == NetCDF::DataType::FLOAT || ids_type == NetCDF::DataType::DOUBLE) {
        id=ftos(ids[n]);
      } else if (ids_type == NetCDF::DataType::CHAR) {
        id.assign(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
      } else {
        log_error2("unable to determine platform type",THIS_FUNC,"nc2xml",USER);
      }
      platform_types.emplace_back(platform_type);
      id_types.emplace_back("unknown");
      id_cache.emplace_back(id);
      if (!obs_data.added_to_platforms("surface",platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
      if (inv_stream.is_open()) {
        auto I_key=id_types[n]+"[!]"+id_cache[n];
        if (I_map.find(I_key) == I_map.end()) {
          I_map.emplace(I_key,make_pair(I_map.size(),ftos(lats[n],4)+"[!]"+ftos(lons[n],4)));
        }
      }
    }
  }
  if (inv_stream.is_open()) {
    for (const auto& plat : platform_types) {
      if (P_map.find(plat) == P_map.end()) {
        P_map.emplace(plat,make_pair(P_map.size(),""));
      }
    }
  }
  vector<DateTime> dts;
  vector<string> datatypes_list;
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() > 0 && ((var.dimids[0] == vars[dgd.indexes.time_var].dimids[0] && (stn_dim == -1 || (var.dimids.size() > 1 && static_cast<int>(var.dimids[1]) == stn_dim))) || (var.dimids.size() > 1 && dgd.indexes.stn_id_var != MISSING_FLAG && var.dimids[0] == vars[dgd.indexes.stn_id_var].dimids[0] && var.dimids[1] == vars[dgd.indexes.time_var].dimids[0]))) {
      if (gatherxml::verbose_operation) {
        cout << "Scanning netCDF variable '" << var.name << "' ..." << endl;
      }
      if (inv_stream.is_open()) {
        if (D_map.find(var.name) == D_map.end()) {
          auto bsize=1;
          for (size_t l=1; l < var.dimids.size(); ++l) {
            bsize*=dims[var.dimids[l]].length;
          }
          switch (var.data_type) {
            case NetCDF::DataType::SHORT: {
              bsize*=2;
              break;
            }
            case NetCDF::DataType::INT:
            case NetCDF::DataType::FLOAT: {
              bsize*=4;
              break;
            }
            case NetCDF::DataType::DOUBLE: {
              bsize*=8;
              break;
            }
            default: { }
          }
          D_map.emplace(var.name,make_pair(D_map.size(),"|"+lltos(var.offset)+"|"+NetCDF::data_type_str[static_cast<int>(var.data_type)]+"|"+itos(bsize)));
        }
      }
      NetCDF::VariableData var_data;
      if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
      datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
      size_t num_times;
      bool time_is_unlimited;
      if (dims[vars[dgd.indexes.time_var].dimids[0]].is_rec) {
        num_times=istream.num_records();
        time_is_unlimited=true;
      }
      else {
        num_times=dims[vars[dgd.indexes.time_var].dimids[0]].length;
        time_is_unlimited=false;
      }
      for (size_t n=0; n < num_stns; ++n) {
        vector<string> miss_lines_list;
        ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]"+id_cache[n];
        for (size_t m=0; m < num_times; ++m) {
          if (dts.size() != num_times) {
            dts.emplace_back(compute_nc_time(times,m));
          }
          auto vidx= (time_is_unlimited) ? n+m*num_stns : n*num_times+m;
          if (!found_missing(times[m],nullptr,var_data[vidx],nc_va_data.missing_value)) {
            if (inv_stream.is_open()) {
              if (T_map.find(m) == T_map.end()) {
                T_map.emplace(m,dts[m].to_string("%Y%m%d%H%MM"));
              }
            }
            if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[n],lons[n],times[m],&dts[m])) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            ++scan_data.num_not_missing;
          } else {
            if (inv_stream.is_open()) {
              string miss_line=itos(m);
              miss_line+="|0|"+itos(P_map[platform_types[n]].first)+"|"+itos(I_map[id_types[n]+"[!]"+id_cache[n]].first)+"|"+itos(D_map[var.name].first);
              miss_lines_list.emplace_back(miss_line);
            }
          }
        }
        if (inv_stream.is_open()) {
          if (miss_lines_list.size() != times.size()) {
            for (const auto& line : miss_lines_list) {
              inv_lines2.writeln(line);
            }
          } else {
            D_map.erase(var.name);
          }
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function "+THIS_FUNC+"() done." << endl;
  }
}

void scan_cf_non_orthogonal_time_series_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, unordered_map<size_t,
    string>& T_map, ScanData& scan_data, gatherxml::markup::ObML
    ::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function "+THIS_FUNC+"()..." << endl;
  }
  size_t id_len=0;
  auto vars=istream.variables();
  NetCDF::VariableData times;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  unique_ptr<NetCDF::VariableData> time_bounds;
  if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
    time_bounds.reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.time_bounds_var].name,*time_bounds) == NetCDF::DataType::_NULL) {
      log_error2("unable to get time bounds data",THIS_FUNC,"nc2xml",USER);
    }
  }
  NetCDF::VariableData lats;
  if (dgd.indexes.lat_var_bounds == MISSING_FLAG) {
    if (istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
      log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
    }
  } else {
    if (istream.variable_data(vars[dgd.indexes.lat_var_bounds].name,lats) == NetCDF::DataType::_NULL) {
      log_error2("unable to get latitude bounds data",THIS_FUNC,"nc2xml",USER);
    }
  }
  NetCDF::VariableData lons;
  if (dgd.indexes.lon_var_bounds == MISSING_FLAG) {
    if (istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
      log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
    }
  } else {
    if (istream.variable_data(vars[dgd.indexes.lon_var_bounds].name,lons) == NetCDF::DataType::_NULL) {
      log_error2("unable to get longitude bounds data",THIS_FUNC,"nc2xml",USER);
    }
  }
  NetCDF::VariableData ids;
  if (istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  NetCDF::VariableData networks;
  if (dgd.indexes.network_var != MISSING_FLAG) {
    istream.variable_data(vars[dgd.indexes.network_var].name,networks);
  }
  auto dims=istream.dimensions();
  auto stn_dim=vars[dgd.indexes.stn_id_var].dimids[0];
  size_t num_stns;
  if (vars[dgd.indexes.stn_id_var].is_rec) {
    num_stns=istream.num_records();
  } else {
    num_stns=dims[stn_dim].length;
  }
  vector<string> platform_types,id_types;
  size_t num_locs=1;
  if (dgd.indexes.lat_var_bounds != MISSING_FLAG && dgd.indexes.lon_var_bounds != MISSING_FLAG) {
    num_locs=2;
  }
  if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n=0; n < num_stns; ++n) {
      platform_types.emplace_back(platform_type);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        id_types.emplace_back(itos(networks[n]));
      } else {
        id_types.emplace_back("unknown");
      }
      for (size_t m=0; m < num_locs; ++m) {
        if (!obs_data.added_to_platforms("surface",platform_types[n],lats[n*num_locs+m],lons[n*num_locs+m])) {
          auto error=move(myerror);
          log_error2(error+"' when adding platform "+platform_types[n],THIS_FUNC,"nc2xml",USER);
        }
      }
    }
  } else if (ids.type() == NetCDF::DataType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    auto network_len=networks.size()/num_stns;
    for (size_t n=0; n < num_stns; ++n) {
      string id(&idbuf[n*id_len],id_len);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        id_types.emplace_back(string(&(reinterpret_cast<char *>(networks.get()))[n*network_len],network_len));
        if (id_types.back() == "WMO") {
          if (id.length() == 5) {
            if (id > "01000" && id < "99000") {
              platform_types.emplace_back("land_station");
            }
            else if (id > "98999") {
              platform_types.emplace_back("roving_ship");
            }
            else {
              platform_types.emplace_back("unknown");
              log_warning("ID '"+id+"' does not appear to be a WMO ID","nc2xml",USER);
            }
          } else {
            platform_types.emplace_back("unknown");
            log_warning("ID '"+id+"' does not appear to be a WMO ID","nc2xml",USER);
          }
        } else if (id_platform_map.find(id_types.back()) != id_platform_map.end()) {
          platform_types.emplace_back(id_platform_map[id_types.back()]);
        } else {
          platform_types.emplace_back("unknown");
        }
      }
      else {
        id_types.emplace_back("unknown");
        platform_types.emplace_back("unknown");
      }
      for (size_t m=0; m < num_locs; ++m) {
        if (!obs_data.added_to_platforms("surface",platform_types[n],lats[n*num_locs+m],lons[n*num_locs+m])) {
          auto error=move(myerror);
          log_error2(error+"' when adding platform "+platform_types[n],THIS_FUNC,"nc2xml",USER);
        }
      }
    }
  } else {
    log_error2("unable to determine platform type",THIS_FUNC,"nc2xml",USER);
  }
  vector<string> datatypes_list;
  auto obs_dim= (vars[dgd.indexes.time_var].dimids[0] == stn_dim) ? vars[dgd.indexes.time_var].dimids[1] : vars[dgd.indexes.time_var].dimids[0];
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {
// continuous ragged array H.6
    if (gatherxml::verbose_operation) {
      cout << "   ...continuous ragged array" << endl;
    }
    NetCDF::VariableData row_sizes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get sample dimension data",THIS_FUNC,"nc2xml",USER);
    }
    for (const auto& var : vars) {
      if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() == 1 && var.dimids[0] == obs_dim) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        long long offset=0;
        for (size_t n=0; n < dims[stn_dim].length; ++n) {
          auto end=offset+row_sizes[n];
          ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
          if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
            ientry.key+=ftos(ids[n]);
          } else if (ids.type() == NetCDF::DataType::CHAR) {
            auto id=string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
            trim(id);
            ientry.key+=id;
          }
          for (size_t m=offset; m < end; ++m) {
            if (!found_missing(times[m],nullptr,var_data[m],nc_va_data.missing_value)) {
              auto dt=compute_nc_time(times,m);
              for (size_t l=0; l < num_locs; ++l) {
                if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[n*num_locs+l],lons[n*num_locs+l],times[m],&dt)) {
                  auto error=move(myerror);
                  log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                }
              }
              ++scan_data.num_not_missing;
            }
          }
          offset=end;
        }
      }
    }
  } else if (dgd.indexes.instance_dim_var != MISSING_FLAG) {
// indexed ragged array H.7
    if (gatherxml::verbose_operation) {
      cout << "   ...indexed ragged array" << endl;
    }
    NetCDF::VariableData station_indexes;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,station_indexes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get instance dimension data",THIS_FUNC,"nc2xml",USER);
    }
    vector<DateTime> min_dts,max_dts;
    if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
      for (size_t n=0; n < station_indexes.size(); ++n) {
        min_dts.emplace_back(compute_nc_time(*time_bounds,n*2));
        max_dts.emplace_back(compute_nc_time(*time_bounds,n*2+1));
      }
    }
    unordered_set<string> ignore_vars{vars[dgd.indexes.time_var].name,vars[dgd.indexes.instance_dim_var].name};
    if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
      ignore_vars.emplace(vars[dgd.indexes.time_bounds_var].name);
    }
    for (const auto& var : vars) {
      if (ignore_vars.find(var.name) == ignore_vars.end() && var.dimids.size() >= 1 && var.dimids[0] == obs_dim) {
        if (gatherxml::verbose_operation) {
          cout << "   ...scanning netCDF variable '" << var.name << "' ..." << endl;
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        for (size_t n=0; n < station_indexes.size(); ++n) {
          size_t idx=station_indexes[n];
          ientry.key=platform_types[idx]+"[!]"+id_types[idx]+"[!]";
          if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
            ientry.key+=ftos(ids[idx]);
          } else if (ids.type() == NetCDF::DataType::CHAR) {
            auto id=string(&(reinterpret_cast<char *>(ids.get()))[idx*id_len],id_len);
            trim(id);
            ientry.key+=id;
          }
          auto check_value=var._FillValue.get();
          for (const auto& value : istream.value_at(var.name,n)) {
            if (value != check_value) {
              check_value=value;
              break;
            }
          }
          if (!found_missing(times[n],nullptr,check_value,nc_va_data.missing_value)) {
            if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
              for (size_t m=0; m < num_locs; ++m) {
                if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[idx*num_locs+m],lons[idx*num_locs+m],times[n],&min_dts[n],&max_dts[n])) {
                  auto error=move(myerror);
                  log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                }
              }
            }
            else {
              auto dt=compute_nc_time(times,n);
              for (size_t m=0; m < num_locs; ++m) {
                if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[idx*num_locs+m],lons[idx*num_locs+m],times[n],&dt)) {
                  auto error=move(myerror);
                  log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                }
              }
            }
            ++scan_data.num_not_missing;
          }
        }
        if (gatherxml::verbose_operation) {
          cout << "   ...scanning netCDF variable '" << var.name << "' done." << endl;
        }
      }
    }
  } else {
// incomplete multidimensional array H.3
    if (gatherxml::verbose_operation) {
      cout << "   ...incomplete multidimensional array" << endl;
    }
    NetCDFVariableAttributeData nc_ta_data;
    extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs,vars[dgd.indexes.time_var].data_type,nc_ta_data);
    size_t num_obs;
    if (vars[dgd.indexes.stn_id_var].is_rec) {
      num_obs=dims[obs_dim].length;
    } else {
      num_obs=istream.num_records();
    }
    for (const auto& var : vars) {
      if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() == 2 && ((var.dimids[0] == stn_dim && var.dimids[1] == obs_dim) || (var.dimids[0] == obs_dim && var.dimids[1] == stn_dim))) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        if (var.dimids.front() == stn_dim) {
          for (size_t n=0; n < num_stns; ++n) {
            ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
            if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
              ientry.key+=ftos(ids[n]);
            }
            else if (ids.type() == NetCDF::DataType::CHAR) {
              auto id=string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
              trim(id);
              ientry.key+=id;
            }
            for (size_t m=0; m < num_obs; ++m) {
              auto idx=n*num_obs+m;
              if (!found_missing(times[idx],&nc_ta_data.missing_value,var_data[idx],nc_va_data.missing_value)) {
                auto dt=compute_nc_time(times,idx);
                for (size_t l=0; l < num_locs; ++l) {
                  if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[n*num_locs+l],lons[n*num_locs+l],times[idx],&dt)) {
                    auto error=move(myerror);
                    log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                  }
                }
                ++scan_data.num_not_missing;
              }
            }
          }
        } else {
          for (size_t n=0; n < num_obs; ++n) {
            for (size_t m=0; m < num_stns; ++m) {
              ientry.key=platform_types[m]+"[!]"+id_types[m]+"[!]";
              if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
                ientry.key+=ftos(ids[m]);
              } else if (ids.type() == NetCDF::DataType::CHAR) {
                auto id=string(&(reinterpret_cast<char *>(ids.get()))[m*id_len],id_len);
                trim(id);
                ientry.key+=id;
              }
              auto idx=n*num_stns+m;
              if (!found_missing(times[idx],&nc_ta_data.missing_value,var_data[idx],nc_va_data.missing_value)) {
                auto dt=compute_nc_time(times,idx);
                for (size_t l=0; l < num_locs; ++l) {
                  if (!obs_data.added_to_ids("surface",ientry,var.name,"",lats[m*num_locs+l],lons[m*num_locs+l],times[idx],&dt)) {
                    auto error=move(myerror);
                    log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                  }
                }
                ++scan_data.num_not_missing;
              }
            }
          }
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function "+THIS_FUNC+"() done." << endl;
  }
}

void scan_cf_time_series_netcdf_file(InputNetCDFStream& istream, string
     platform_type, ScanData& scan_data, gatherxml::markup::ObML
     ::ObservationData& obs_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << THIS_FUNC << "()..." << endl;
  }
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(vars,n,m,dgd);
        } else if (vars[n].attrs[m].name == "cf_role") {
          auto r=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          r=to_lower(r);
          if (r == "timeseries_id") {
            if (dgd.indexes.stn_id_var != MISSING_FLAG) {
              log_error2("station ID was already identified - don't know what to do with variable: "+vars[n].name,THIS_FUNC,"nc2xml",USER);
            }
            dgd.indexes.stn_id_var=n;
          }
        } else if (vars[n].attrs[m].name == "sample_dimension") {
          dgd.indexes.sample_dim_var=n;
        } else if (vars[n].attrs[m].name == "instance_dimension") {
          dgd.indexes.instance_dim_var=n;
        }
      }
    }
  }
  if (dgd.indexes.time_var == MISSING_FLAG) {
    log_error2("unable to determine time variable",THIS_FUNC,"nc2xml",USER);
  } else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
      if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
        time_data.calendar=*(reinterpret_cast<string *>(vars[dgd.indexes.time_var].attrs[n].values));
      }
      else if (vars[dgd.indexes.time_var].attrs[n].name == "bounds") {
        auto bounds_name=*(reinterpret_cast<string *>(vars[dgd.indexes.time_var].attrs[n].values));
        auto dims=istream.dimensions();
        for (size_t m=0; m < vars.size(); ++m) {
          if (vars[m].name == bounds_name && dims[vars[m].dimids.back()].length == 2) {
            dgd.indexes.time_bounds_var=m;
            break;
          }
        }
      }
    }
  }
  unordered_map<size_t,string> T_map;
  if (vars[dgd.indexes.time_var].is_coord) {
// ex. H.2, H.4 (single version of H.2), H.5 (precise locations) stns w/same times
    scan_cf_orthogonal_time_series_netcdf_file(istream, platform_type, dgd,
        T_map, scan_data, obs_data);
  } else {
// ex. H.3 stns w/varying times but same # of obs
// ex. H.6 w/sample_dimension
// ex. H.7 w/instance_dimension
    if (dgd.indexes.stn_id_var == MISSING_FLAG) {
      log_error2("unable to determine timeseries_id variable",THIS_FUNC,"nc2xml",USER);
    } else {
      for (size_t n=0; n < vars[dgd.indexes.stn_id_var].attrs.size(); ++n) {
        if (regex_search(vars[dgd.indexes.stn_id_var].attrs[n].name,regex("network",std::regex_constants::icase))) {
          auto network=*(reinterpret_cast<string *>(vars[dgd.indexes.stn_id_var].attrs[n].values));
          if (gatherxml::verbose_operation) {
            cout << "   ...found network: '" << network << "'" << endl;
          }
          for (size_t m=0; m < vars.size(); ++m) {
            if (vars[m].name == network) {
              dgd.indexes.network_var=m;
              break;
            }
          }
          break;
        }
      }
    }
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      log_error2("unable to determine latitude variable",THIS_FUNC,"nc2xml",USER);
    } else {
      for (size_t n=0; n < vars[dgd.indexes.lat_var].attrs.size(); ++n) {
        if (regex_search(vars[dgd.indexes.lat_var].attrs[n].name,regex("bounds",std::regex_constants::icase))) {
          auto lat_var_bounds=*(reinterpret_cast<string *>(vars[dgd.indexes.lat_var].attrs[n].values));
          if (gatherxml::verbose_operation) {
            cout << "   ...found latitude bounds: '" << lat_var_bounds << "'" << endl;
          }
          for (size_t m=0; m < vars.size(); ++m) {
            if (vars[m].name == lat_var_bounds) {
              dgd.indexes.lat_var_bounds=m;
              break;
            }
          }
          break;
        }
      }
    }
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      log_error2("unable to determine longitude variable",THIS_FUNC,"nc2xml",USER);
    } else {
      for (size_t n=0; n < vars[dgd.indexes.lon_var].attrs.size(); ++n) {
        if (regex_search(vars[dgd.indexes.lon_var].attrs[n].name,regex("bounds",std::regex_constants::icase))) {
          auto lon_var_bounds=*(reinterpret_cast<string *>(vars[dgd.indexes.lon_var].attrs[n].values));
          if (gatherxml::verbose_operation) {
            cout << "   ...found latitude bounds: '" << lon_var_bounds << "'" << endl;
          }
          for (size_t m=0; m < vars.size(); ++m) {
            if (vars[m].name == lon_var_bounds) {
              dgd.indexes.lon_var_bounds=m;
              break;
            }
          }
          break;
        }
      }
    }
    scan_cf_non_orthogonal_time_series_netcdf_file(istream, platform_type, dgd,
        T_map, scan_data, obs_data);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (inv_stream.is_open()) {
    vector<size_t> time_indexes;
    for (const auto& e : T_map) {
      time_indexes.emplace_back(e.first);
    }
    std::sort(time_indexes.begin(),time_indexes.end(),
    [](const size_t& left,const size_t& right) -> bool
    {
      if (left <= right) {
        return true;
      }
      else {
        return false;
      }
    });
    for (const auto& idx : time_indexes) {
      inv_stream << "T<!>" << idx << "<!>" << T_map[idx] << endl;
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function "+THIS_FUNC+"() done." << endl;
  }
}

void scan_cf_orthogonal_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t id_len=1;
  string id_type="unknown";
  NetCDF::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  if (lats.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (lons.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (ids.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == NetCDF::DataType::_NULL) {
    log_error2("unable to get level data",THIS_FUNC,"nc2xml",USER);
  }
  auto dims=istream.dimensions();
  if (ids.type() == NetCDF::DataType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  if (times.size() != lats.size() || lats.size() != lons.size() || lons.size() != ids.size()/id_len) {
    log_error2("profile data does not follow the CF conventions",THIS_FUNC,"nc2xml",USER);
  }
  NetCDFVariableAttributeData nc_ta_data;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs,vars[dgd.indexes.time_var].data_type,nc_ta_data);
  vector<string> datatypes_list;
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.z_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
      NetCDF::VariableData var_data;
      if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
      datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
      for (size_t n=0; n < times.size(); ++n) {
        auto nlvls=0,last_level=-1;
        auto avg_vres=0.;
        auto min=1.e38,max=-1.e38;
        for (size_t m=0; m < levels.size(); ++m) {
          if (!found_missing(times[n],&nc_ta_data.missing_value,var_data[n*levels.size()+m],nc_va_data.missing_value)) {
            if (levels[m] < min) {
              min=levels[m];
            }
            if (levels[m] > max) {
              max=levels[m];
            }
            if (last_level >= 0) {
              avg_vres+=fabsf(levels[m]-levels[last_level]);
            }
            ++nlvls;
            last_level=m;
          }
        }
        if (nlvls > 0) {
          if (!obs_data.added_to_platforms(obs_type,platform_type,lats[n],lons[n])) {
            auto error=move(myerror);
            log_error2(error+"' when adding platform "+obs_type+" "+platform_type,THIS_FUNC,"nc2xml",USER);
          }
          auto dt=compute_nc_time(times,n);
          ientry.key=platform_type+"[!]"+id_type+"[!]";
          if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
            ientry.key+=ftos(ids[n]);
          } else if (ids.type() == NetCDF::DataType::CHAR) {
            ientry.key+=string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
          }
          if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[n],lons[n],times[n],&dt)) {
            auto error=move(myerror);
            log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
          }
          gatherxml::markup::ObML::DataTypeEntry dte;
          ientry.data->data_types_table.found(var.name,dte);
          if (dte.data->vdata == nullptr) {
            dte.data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::VerticalData);
          }
          if (dgd.z_pos == "down") {
            if (dte.data->vdata->min_altitude > 1.e37) {
              dte.data->vdata->min_altitude=-dte.data->vdata->min_altitude;
            }
            if (dte.data->vdata->max_altitude < -1.e37) {
              dte.data->vdata->max_altitude=-dte.data->vdata->max_altitude;
            }
            if (max > dte.data->vdata->min_altitude) {
              dte.data->vdata->min_altitude=max;
            }
            if (min < dte.data->vdata->max_altitude) {
              dte.data->vdata->max_altitude=min;
            }
          } else {
            if (min < dte.data->vdata->min_altitude) {
              dte.data->vdata->min_altitude=min;
            }
            if (max > dte.data->vdata->max_altitude) {
              dte.data->vdata->max_altitude=max;
            }
          }
          dte.data->vdata->units=dgd.z_units;
          dte.data->vdata->avg_nlev+=nlvls;
          dte.data->vdata->avg_res+=(avg_vres/(nlvls-1));
          ++dte.data->vdata->res_cnt;
          ++scan_data.num_not_missing;
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
}

bool compare_z_down(const double& left,const double& right)
{
  if (left >= right) {
    return true;
  }
  return false;
}

bool compare_z_up(const double& left,const double& right)
{
  if (left <= right) {
    return true;
  }
  return false;
}

void fill_vertical_resolution_data(vector<double>& lvls,string z_pos,string z_units,gatherxml::markup::ObML::DataTypeEntry& datatype_entry)
{
  auto min=1.e38,max=-1.e38;
  for (size_t n=0; n < lvls.size(); ++n) {
    if (lvls[n] < min) {
      min=lvls[n];
    }
    if (lvls[n] > max) {
      max=lvls[n];
    }
  }
  if (datatype_entry.data->vdata == nullptr) {
    datatype_entry.data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::VerticalData);
  }
  if (z_pos == "down") {
    std::sort(lvls.begin(),lvls.end(),compare_z_down);
    if (datatype_entry.data->vdata->min_altitude > 1.e37) {
      datatype_entry.data->vdata->min_altitude=-datatype_entry.data->vdata->min_altitude;
    }
    if (datatype_entry.data->vdata->max_altitude < -1.e37) {
      datatype_entry.data->vdata->max_altitude=-datatype_entry.data->vdata->max_altitude;
    }
    if (max > datatype_entry.data->vdata->min_altitude) {
      datatype_entry.data->vdata->min_altitude=max;
    }
    if (min < datatype_entry.data->vdata->max_altitude) {
      datatype_entry.data->vdata->max_altitude=min;
    }
  } else {
    std::sort(lvls.begin(),lvls.end(),compare_z_up);
    if (min < datatype_entry.data->vdata->min_altitude) {
      datatype_entry.data->vdata->min_altitude=min;
    }
    if (max > datatype_entry.data->vdata->max_altitude) {
      datatype_entry.data->vdata->max_altitude=max;
    }
  }
  datatype_entry.data->vdata->units=z_units;
  datatype_entry.data->vdata->avg_nlev+=lvls.size();
  auto avg_vres=0.;
  for (size_t n=1; n < lvls.size(); ++n) {
    avg_vres+=fabs(lvls[n]-lvls[n-1]);
  }
  datatype_entry.data->vdata->avg_res+=(avg_vres/(lvls.size()-1));
  ++datatype_entry.data->vdata->res_cnt;
}

void scan_cf_non_orthogonal_profile_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t id_len=1;
  auto vars=istream.variables();
  NetCDF::VariableData times,lats,lons,ids,levels;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  if (lats.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (lons.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (ids.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  auto dims=istream.dimensions();
  vector<string> platform_types,id_types;
  if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n=0; n < times.size(); ++n) {
//      int id=ids[n];
platform_types.emplace_back(platform_type);
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else if (ids.type() == NetCDF::DataType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < times.size(); ++n) {
      string id(&idbuf[n*id_len],id_len);
platform_types.emplace_back(platform_type);
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else {
    log_error2("unable to determine platform type",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == NetCDF::DataType::_NULL) {
    log_error2("unable to get level data",THIS_FUNC,"nc2xml",USER);
  }
  if (times.size() != lats.size() || lats.size() != lons.size() || lons.size() != ids.size()/id_len) {
    log_error2("profile data does not follow the CF conventions",THIS_FUNC,"nc2xml",USER);
  }
  NetCDFVariableAttributeData nc_ta_data;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs,vars[dgd.indexes.time_var].data_type,nc_ta_data);
  vector<string> datatypes_list;
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {
// continuous ragged array H.10
    NetCDF::VariableData row_sizes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get row size data",THIS_FUNC,"nc2xml",USER);
    }
    for (const auto& var : vars) {
      if (var.name != vars[dgd.indexes.z_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        auto off=0;
        for (size_t n=0; n < times.size(); ++n) {
          vector<double> lvls;
          for (size_t m=0; m < row_sizes[n]; ++m) {
            if (!found_missing(times[n],&nc_ta_data.missing_value,var_data[off],nc_va_data.missing_value)) {
              lvls.emplace_back(levels[off]);
            }
            ++off;
          }
          if (lvls.size() > 0) {
            auto dt=compute_nc_time(times,n);
            ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
            if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
              ientry.key+=ftos(ids[n]);
            }
            else if (ids.type() == NetCDF::DataType::CHAR) {
              ientry.key+=string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
            }
            if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[n],lons[n],times[n],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(var.name,dte);
            fill_vertical_resolution_data(lvls,dgd.z_pos,dgd.z_units,dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  } else if (dgd.indexes.instance_dim_var != MISSING_FLAG) {
// indexed ragged array H.11
    NetCDF::VariableData profile_indexes;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,profile_indexes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get instance dimension data",THIS_FUNC,"nc2xml",USER);
    }
    for (const auto& var : vars) {
      if (var.name != vars[dgd.indexes.z_var].name && var.name != vars[dgd.indexes.instance_dim_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        for (size_t n=0; n < times.size(); ++n) {
          vector<double> lvls;
          for (size_t m=0; m < profile_indexes.size(); ++m) {
            if (profile_indexes[m] == n && !found_missing(times[n],&nc_ta_data.missing_value,var_data[m],nc_va_data.missing_value)) {
              lvls.emplace_back(levels[m]);
            }
          }
          if (lvls.size() > 0) {
            auto dt=compute_nc_time(times,n);
            ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
            if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
              ientry.key+=ftos(ids[n]);
            }
            else if (ids.type() == NetCDF::DataType::CHAR) {
              ientry.key+=string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
            }
            if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[n],lons[n],times[n],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(var.name,dte);
            fill_vertical_resolution_data(lvls,dgd.z_pos,dgd.z_units,dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
}

void process_vertical_coordinate_variable(const vector<NetCDF::Attribute>& attrs,DiscreteGeometriesData& dgd,string& obs_type)
{
  static const string THIS_FUNC=this_function_label(__func__);
  obs_type="";
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "units") {
      dgd.z_units=*(reinterpret_cast<string *>(attrs[n].values));
      trim(dgd.z_units);
    } else if (attrs[n].name == "positive") {
      dgd.z_pos=*(reinterpret_cast<string *>(attrs[n].values));
      trim(dgd.z_pos);
      dgd.z_pos=to_lower(dgd.z_pos);
    }
  }
  if (dgd.z_pos.empty() && !dgd.z_units.empty()) {
    auto z_units_l=to_lower(dgd.z_units);
    if (regex_search(dgd.z_units,regex("Pa$")) || regex_search(z_units_l,regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
      dgd.z_pos="down";
      obs_type="upper_air";
    }
  }
  if (dgd.z_pos.empty()) {
    log_error2("process_vertical_coordinate_variable() returned error: unable to determine vertical coordinate direction",THIS_FUNC,"nc2xml",USER);
  } else if (obs_type.empty()) {
    if (dgd.z_pos == "up") {
      obs_type="upper_air";
    } else if (dgd.z_pos == "down") {
      auto z_units_l=to_lower(dgd.z_units);
      if (dgd.z_pos == "down" && (regex_search(dgd.z_units,regex("Pa$")) || regex_search(z_units_l,regex("^mb(ar){0,1}$")) || z_units_l == "millibars")) {
        obs_type="upper_air";
      }
    }
  }
}

void scan_cf_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, ScanData& scan_data, gatherxml::markup::ObML
     ::ObservationData& obs_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << THIS_FUNC << "()..." << endl;
  }
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(vars,n,m,dgd);
        } else if (vars[n].attrs[m].name == "cf_role") {
          auto r=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          r=to_lower(r);
          if (r == "profile_id") {
            if (dgd.indexes.stn_id_var != MISSING_FLAG) {
              log_error2("station ID was already identified - don't know what to do with variable: "+vars[n].name,THIS_FUNC,"nc2xml",USER);
            }
            dgd.indexes.stn_id_var=n;
          }
        } else if (vars[n].attrs[m].name == "sample_dimension") {
          dgd.indexes.sample_dim_var=n;
        } else if (vars[n].attrs[m].name == "instance_dimension") {
          dgd.indexes.instance_dim_var=n;
        } else if (vars[n].attrs[m].name == "axis") {
          auto axis=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          if (axis == "Z") {
            dgd.indexes.z_var=n;
          }
        }
      }
    }
  }
  if (dgd.indexes.time_var == MISSING_FLAG) {
    log_error2("unable to determine time variable",THIS_FUNC,"nc2xml",USER);
  } else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
      if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
        time_data.calendar=*(reinterpret_cast<string *>(vars[dgd.indexes.time_var].attrs[n].values));
      }
    }
  }
  string obs_type;
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable",THIS_FUNC,"nc2xml",USER);
  } else {
    process_vertical_coordinate_variable(vars[dgd.indexes.z_var].attrs,dgd,obs_type);
  }
  if (obs_type.empty()) {
    log_error2("unable to determine observation type",THIS_FUNC,"nc2xml",USER);
  }
  if (dgd.indexes.sample_dim_var != MISSING_FLAG || dgd.indexes.instance_dim_var != MISSING_FLAG) {
// ex. H.10, H.11
    scan_cf_non_orthogonal_profile_netcdf_file(istream, platform_type, dgd,
        scan_data, obs_data, obs_type);
  } else {
// ex. H.8, H.9
    scan_cf_orthogonal_profile_netcdf_file(istream, platform_type, dgd,
       scan_data, obs_data, obs_type);
  }
  scan_data .write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << THIS_FUNC << "() done." << endl;
  }
}

void scan_cf_orthogonal_time_series_profile_netcdf_file(InputNetCDFStream&
     istream, string platform_type, DiscreteGeometriesData& dgd, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data, string
     obs_type) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t id_len=1;
  string id_type="unknown";
  NetCDF::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  if (lats.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (lons.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (ids.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  auto dims=istream.dimensions();
  vector<string> platform_types,id_types,id_cache;
  if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n=0; n < ids.size(); ++n) {
      id_cache.emplace_back(ftos(ids[n]));
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else if (ids.type() == NetCDF::DataType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < ids.size()/id_len; ++n) {
      id_cache.emplace_back(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else {
    log_error2("unable to determine platform type",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == NetCDF::DataType::_NULL) {
    log_error2("unable to get level data",THIS_FUNC,"nc2xml",USER);
  }
  vector<string> datatypes_list;
  for (const auto& var : vars) {
    if (var.dimids.size() == 3 && var.dimids[0] == vars[dgd.indexes.time_var].dimids[0] && var.dimids[1] == vars[dgd.indexes.z_var].dimids[0] && var.dimids[2] == vars[dgd.indexes.stn_id_var].dimids[0]) {
      NetCDF::VariableData var_data;
      if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
      datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
      for (size_t n=0; n < times.size(); ++n) {
        auto dt=compute_nc_time(times,n);
        size_t num_stns=ids.size()/id_len;
        auto voff=n*(levels.size()*num_stns);
        for (size_t m=0; m < num_stns; ++m) {
          vector<double> lvls;
          auto vidx=voff+m;
          for (size_t l=0; l < levels.size(); ++l) {
            if (!found_missing(times[n],nullptr,var_data[vidx],nc_va_data.missing_value)) {
              lvls.emplace_back(levels[l]);
            }
            vidx+=num_stns;
          }
          if (lvls.size() > 0) {
            ientry.key=platform_types[m]+"[!]"+id_types[m]+"[!]"+id_cache[m];
            if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[m],lons[m],times[n],&dt)) {
              auto error=string(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(var.name,dte);
            fill_vertical_resolution_data(lvls,dgd.z_pos,dgd.z_units,dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
}

void scan_cf_non_orthogonal_time_series_profile_netcdf_file(InputNetCDFStream&
     istream, string platform_type, DiscreteGeometriesData& dgd, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data, string
     obs_type) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t id_len=1;
  string id_type="unknown";
  NetCDF::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == NetCDF::DataType::_NULL) {
    log_error2("unable to get time data",THIS_FUNC,"nc2xml",USER);
  }
  if (lats.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (lons.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data",THIS_FUNC,"nc2xml",USER);
  }
  if (ids.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data",THIS_FUNC,"nc2xml",USER);
  }
  auto dims=istream.dimensions();
  vector<string> platform_types,id_types,id_cache;
  if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n=0; n < ids.size(); ++n) {
      id_cache.emplace_back(ftos(ids[n]));
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else if (ids.type() == NetCDF::DataType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < ids.size()/id_len; ++n) {
      id_cache.emplace_back(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type,platform_types[n],lats[n],lons[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_types[n],THIS_FUNC,"nc2xml",USER);
      }
    }
  } else {
    log_error2("unable to determine platform type",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == NetCDF::DataType::_NULL) {
    log_error2("unable to get level data",THIS_FUNC,"nc2xml",USER);
  }
  vector<string> datatypes_list;
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {
// H.19
    if (dgd.indexes.instance_dim_var == MISSING_FLAG) {
      log_error2("found sample dimension but not instance dimension",THIS_FUNC,"nc2xml",USER);
    }
    NetCDF::VariableData row_sizes,station_indexes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get sample dimension data",THIS_FUNC,"nc2xml",USER);
    }
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,station_indexes) == NetCDF::DataType::_NULL) {
      log_error2("unable to get instance dimension data",THIS_FUNC,"nc2xml",USER);
    }
    if (row_sizes.size() != station_indexes.size()) {
      log_error2("sample dimension and instance dimension have different sizes",THIS_FUNC,"nc2xml",USER);
    }
    for (const auto& var : vars) {
      if (var.dimids.front() == vars[dgd.indexes.z_var].dimids.front() && var.name != vars[dgd.indexes.z_var].name) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        auto off=0;
        for (size_t n=0; n < row_sizes.size(); ++n) {
          auto dt=compute_nc_time(times,n);
          vector<double> lvls;
          for (size_t m=0; m < row_sizes[n]; ++m) {
            if (!found_missing(times[n],nullptr,var_data[off],nc_va_data.missing_value)) {
              lvls.emplace_back(levels[off]);
            }
            ++off;
          }
          if (lvls.size() > 0) {
            ientry.key=platform_types[station_indexes[n]]+"[!]"+id_types[station_indexes[n]]+"[!]"+id_cache[station_indexes[n]];
            if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[station_indexes[n]],lons[station_indexes[n]],times[n],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(var.name,dte);
            fill_vertical_resolution_data(lvls,dgd.z_pos,dgd.z_units,dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  } else {
// H.16, H.18
    auto ntimes=dims[vars[dgd.indexes.time_var].dimids.back()].length;
    auto nlvls=dims[vars[dgd.indexes.z_var].dimids.back()].length;
    auto stn_size=ntimes*nlvls;
    for (const auto& var : vars) {
      if (var.name != vars[dgd.indexes.z_var].name && ((var.dimids.size() == 3 && var.dimids[0] == vars[dgd.indexes.stn_id_var].dimids.front() && var.dimids[1] == vars[dgd.indexes.time_var].dimids.back() && var.dimids[2] == vars[dgd.indexes.z_var].dimids.back()) || (var.dimids.size() == 2 && var.dimids[0] == vars[dgd.indexes.time_var].dimids.back() && var.dimids[1] == vars[dgd.indexes.z_var].dimids.back()))) {
        NetCDF::VariableData var_data;
        if (istream.variable_data(var.name,var_data) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '"+var.name+"'",THIS_FUNC,"nc2xml",USER);
        }
        NetCDFVariableAttributeData nc_va_data;
        extract_from_variable_attribute(var.attrs,var.data_type,nc_va_data);
        datatypes_list.emplace_back(var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword);
        for (size_t n=0; n < var_data.size(); ) {
          auto stn_idx=n/stn_size;
          for (size_t m=0; m < ntimes; ++m) {
            vector<double> lvls;
            for (size_t l=0; l < nlvls; ++l,++n) {
              if (!found_missing(times[n],nullptr,var_data[n],nc_va_data.missing_value)) {
                if (levels.size() == var_data.size()) {
                  lvls.emplace_back(levels[n]);
                } else if (levels.size() == nlvls) {
                  lvls.emplace_back(levels[l]);
                } else {
                  lvls.emplace_back(levels[stn_idx*nlvls+l]);
                }
              }
            }
            if (lvls.size() > 0) {
              ientry.key=platform_types[stn_idx]+"[!]"+id_types[stn_idx]+"[!]"+id_cache[stn_idx];
              if (times.size() == ntimes) {
                auto dt=compute_nc_time(times,m);
                if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[stn_idx],lons[stn_idx],times[m],&dt)) {
                  auto error=move(myerror);
                  log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                }
              } else {
                auto t_idx=stn_idx*ntimes+m;
                auto dt=compute_nc_time(times,t_idx);
                if (!obs_data.added_to_ids(obs_type,ientry,var.name,"",lats[stn_idx],lons[stn_idx],times[t_idx],&dt)) {
                  auto error=move(myerror);
                  log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
                }
              }
              gatherxml::markup::ObML::DataTypeEntry dte;
              ientry.data->data_types_table.found(var.name,dte);
              fill_vertical_resolution_data(lvls,dgd.z_pos,dgd.z_units,dte);
              ++scan_data.num_not_missing;
            }
          }
        }
      }
    }
  }
  for (const auto& type : datatypes_list) {
    auto descr=scan_data.datatype_map.description(type.substr(0,type.find("<!>")));
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
  }
}

void scan_cf_time_series_profile_netcdf_file(InputNetCDFStream& istream,
    string platform_type, ScanData& scan_data, gatherxml::markup::ObML
    ::ObservationData& obs_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << THIS_FUNC << "()..." << endl;
  }
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(vars,n,m,dgd);
        } else if (vars[n].attrs[m].name == "cf_role") {
          auto r=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          r=to_lower(r);
          if (r == "timeseries_id") {
            if (dgd.indexes.stn_id_var != MISSING_FLAG) {
              log_error2("station ID was already identified - don't know what to do with variable: "+vars[n].name,THIS_FUNC,"nc2xml",USER);
            }
            dgd.indexes.stn_id_var=n;
          }
        } else if (vars[n].attrs[m].name == "sample_dimension") {
          dgd.indexes.sample_dim_var=n;
        } else if (vars[n].attrs[m].name == "instance_dimension") {
          dgd.indexes.instance_dim_var=n;
        } else if (vars[n].attrs[m].name == "axis") {
          auto axis=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          if (axis == "Z") {
            dgd.indexes.z_var=n;
          }
        }
      }
    }
  }
  if (dgd.indexes.time_var == MISSING_FLAG) {
    log_error2("unable to determine time variable",THIS_FUNC,"nc2xml",USER);
  } else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
      if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
        time_data.calendar=*(reinterpret_cast<string *>(vars[dgd.indexes.time_var].attrs[n].values));
      }
    }
  }
  string obs_type;
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable",THIS_FUNC,"nc2xml",USER);
  } else {
    process_vertical_coordinate_variable(vars[dgd.indexes.z_var].attrs,dgd,obs_type);
  }
  if (obs_type.empty()) {
    log_error2("unable to determine observation type",THIS_FUNC,"nc2xml",USER);
  }
  if (vars[dgd.indexes.time_var].is_coord && vars[dgd.indexes.z_var].is_coord) {
// ex. H.17
    scan_cf_orthogonal_time_series_profile_netcdf_file(istream, platform_type,
        dgd, scan_data, obs_data, obs_type);
  } else {
// ex. H.16, H.18, H.19
    scan_cf_non_orthogonal_time_series_profile_netcdf_file(istream,
        platform_type, dgd, scan_data, obs_data, obs_type);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function "+THIS_FUNC+"() done." << endl;
  }
}

void find_coordinate_variables(InputNetCDFStream& istream, vector<NetCDF
    ::Variable>& vars, GridData& grid_data, my::map<metautils::NcTime
    ::TimeRangeEntry>& tr_table) {
  static const string THIS_FUNC = this_function_label(__func__);
  unordered_set<string> unique_level_id_table;
  for (size_t n = 0; n < vars.size(); ++n) {
    if (vars[n].is_coord) {
      if (gatherxml::verbose_operation) {
        cout << "'" << vars[n].name << "' is a coordinate variable" << endl;
      }
      for (size_t m = 0; m < vars[n].attrs.size(); ++m) {
        string standard_name;
        if (vars[n].attrs[m].name == "standard_name") {
          standard_name = *(reinterpret_cast<string *>(vars[n].attrs[m]
              .values));
        } else {
          standard_name = "";
        }
        if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR && (vars[n]
            .attrs[m].name == "units" || regex_search(standard_name,
            regex("hybrid_sigma")))) {
          string u;
          if (vars[n].attrs[m].name == "units") {
            u = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
          } else {
            u= "";
          }
          u = to_lower(u);
          if (regex_search(u, regex("since"))) {
            if (!grid_data.time.id.empty()) {
              log_error2("time was already identified - don't know what to do "
                  "with variable: " + vars[n].name, THIS_FUNC, "nc2xml", USER);
            }

            // check for time ranges other than analysis
            string climo_bounds_name;
            for (size_t l = 0; l < vars[n].attrs.size(); ++l) {
              if (vars[n].attrs[l].data_type == NetCDF::DataType::CHAR) {
                if (vars[n].attrs[l].name == "calendar") {
                  time_data.calendar = *(reinterpret_cast<string *>(vars[n]
                      .attrs[l].values));
                } else if (vars[n].attrs[l].name == "bounds") {
                  grid_data.time_bounds.id = *(reinterpret_cast<string *>(vars[
                      n].attrs[l].values));
                } else if (vars[n].attrs[l].name == "climatology") {
                  climo_bounds_name = *(reinterpret_cast<string *>(vars[n]
                      .attrs[l].values));
                }
              }
            }
            time_data.units = u.substr(0, u.find("since"));
            trim(time_data.units);
            grid_data.time.dim = vars[n].dimids[0];
            grid_data.time.id = vars[n].name;
            u = u.substr(u.find("since") + 5);
            trim(u);
            auto sp = split(u);
            auto sp2 = split(sp[0], "-");
            if (sp2.size() != 3) {
              log_error2("bad netcdf date", THIS_FUNC, "nc2xml", USER);
            }
            long long dt = stoi(sp2[0]) * 10000000000 + stoi(sp2[1]) *
                100000000 + stoi(sp2[2]) * 1000000;
            if (sp.size() > 1) {
              sp2 = split(sp[1], ":");
              dt += stoi(sp2[0]) * 10000;
              if (sp2.size() > 1) {
                dt += stoi(sp2[1]) * 100;
              }
              if (sp2.size() > 2) {
                dt += stoi(sp2[2]);
              }
            }
            time_data.reference.set(dt);
            if (!climo_bounds_name.empty()) {
              for (size_t l = 0; l < vars.size(); ++l) {
                if (vars[l].name == climo_bounds_name) {
                  NetCDF::VariableData v;
                  istream.variable_data(vars[l].name, v);
/*
                  unique_ptr<double[]> ta1(new double[v.size()/2]);
                  unique_ptr<double[]> ta2(new double[v.size()/2]);
                  for (size_t x=0; x < static_cast<size_t>(v.size()); x+=2) {
                    ta1[x/2]=v[x];
                    ta2[x/2]=v[x + 1];
                  }
*/
                  time_bounds_s.t1 = v.front();
                  time_bounds_s.t2 = v.back();
                  size_t nsteps = v.size() / 2;
                  for (size_t x = 0; x < nsteps; ++x) {
                    DateTime d1, d2;
                    if (time_data.units == "hours") {
                      d1 = time_data.reference.hours_added(v[x * 2]);
                      d2 = time_data.reference.hours_added(v[x * 2 + 1]);
                    } else if (time_data.units == "days") {
                      d1 = time_data.reference.days_added(v[x * 2]);
                      d2 = time_data.reference.days_added(v[x * 2 + 1]);
                    } else if (time_data.units == "months") {
                      d1 = time_data.reference.months_added(v[x * 2]);
                      d2 = time_data.reference.months_added(v[x * 2 + 1]);
                    } else {
                      log_error2("don't understand climatology_bounds units "
                          "in " + time_data.units, THIS_FUNC, "nc2xml", USER);
                    }
                    metautils::NcTime::TimeRangeEntry tre;
                    tre.key = d2.years_since(d1) + 1;
                    if (!tr_table.found(tre.key, tre)) {
                      tre.data.reset(new metautils::NcTime::TimeRangeEntry
                          ::Data);
                      tre.data->instantaneous.first_valid_datetime.set(
                          static_cast<long long>(30001231235959));
                      tre.data->instantaneous.last_valid_datetime.set(
                          static_cast<long long>(10000101000000));
                      tre.data->bounded.first_valid_datetime.set(
                          static_cast<long long>(30001231235959));
                      tre.data->bounded.last_valid_datetime.set(
                          static_cast<long long>(10000101000000));
                      tr_table.insert(tre);
                    }
                    if (d1 < tre.data->bounded.first_valid_datetime) {
                      tre.data->bounded.first_valid_datetime = d1;
                    }
                    if (d2 > tre.data->bounded.last_valid_datetime) {
                      tre.data->bounded.last_valid_datetime = d2;
                    }
                    if (d1.month() > d2.month()) {
                      d1.set_year(d2.year() - 1);
                    } else {
                      d1.set_year(d2.year());
                    }
                    size_t b = d2.days_since(d1, time_data.calendar);
                    size_t a = dateutils::days_in_month(d1.year(), d1.month());
                    if (b == a || b == (a - 1)) {
                      b = 1;
                    } else {
                      b = d2.months_since(d1);
                      if (b == 3) {
                          b = 2;
                      } else if (b == 12) {
                          b = 3;
                      } else {
                          log_error2("unable to handle climatology of " + itos(
                              b) + "-day means", THIS_FUNC, "nc2xml", USER);
                      }
                    }
                    tre.data->unit = b;
                    ++(tre.data->num_steps);
                  }
                  l = vars.size();
                }
              }
              if (gatherxml::verbose_operation) {
                for (const auto& tr_key : tr_table.keys()) {
                  metautils::NcTime::TimeRangeEntry tre;
                  tr_table.found(tr_key, tre);
                  cout << "   ...setting temporal range for climatology "
                      "key " << tr_key << " to:" << endl;
                  cout << "      " << tre.data->bounded.first_valid_datetime
                      .to_string() << " to " << tre.data->bounded
                      .last_valid_datetime.to_string() << ", units=" << tre
                      .data->unit << endl;
                }
              }
            }
          } else if (u == "degrees_north" || u == "degree_north" ||
              u == "degrees_n" || u == "degree_n" || (u ==
              "degrees" && vars[n].name == "lat")) {
/*
                if (found_lat) {
                  log_warning("latitude was already identified - ignoring '" +
                      vars[n].name + "'", "nc2xml", USER);
                }
else {
*/
            grid_data.lats.emplace_back();
            grid_data.lats.back().dim = vars[n].dimids[0];
            grid_data.lats.back().id = vars[n].name;
            grid_data.lats.back().type = vars[n].data_type;
            string s;
            for (size_t l = 0; l < vars[n].attrs.size(); ++l) {
              if (vars[n].attrs[l].data_type == NetCDF::DataType::CHAR &&
                  vars[n].attrs[l].name == "bounds") {
                s = *(reinterpret_cast<string *>(vars[n].attrs[l].values));
              }
            }
            grid_data.lats_b.emplace_back();
            grid_data.lats_b.back().id = s;
//}
          } else if (u == "degrees_east" || u == "degree_east" || u ==
              "degrees_e" || u == "degree_e" || (u == "degrees" && vars[n]
              .name == "lon")) {
/*
                if (found_lon) {
                  log_warning("longitude was already identified - ignoring '" +
                      vars[n].name + "'", "nc2xml", USER);
                }
else {
*/
            grid_data.lons.emplace_back();
            grid_data.lons.back().dim = vars[n].dimids[0];
            grid_data.lons.back().id = vars[n].name;
            grid_data.lons.back().type = vars[n].data_type;
            string s;
            for (size_t l = 0; l < vars[n].attrs.size(); ++l) {
              if (vars[n].attrs[l].data_type == NetCDF::DataType::CHAR && vars[
                  n].attrs[l].name == "bounds") {
                s = *(reinterpret_cast<string *>(vars[n].attrs[l].values));
              }
            }
            grid_data.lons_b.emplace_back();
            grid_data.lons_b.back().id = s;
//}
          } else {
            if (grid_data.time.id.empty() && vars[n].name == "time") {
              if (found_netcdf_time_from_patch(vars[n])) {
                grid_data.time.dim = vars[n].dimids[0];
                grid_data.time.id = vars[n].name;
              }
            } else {
              metautils::StringEntry se;
              if (unique_level_id_table.find(vars[n].name) ==
                  unique_level_id_table.end()) {
                grid_data.levels.emplace_back();
                grid_data.levels.back().dim = vars[n].dimids[0];
                grid_data.levels.back().id = vars[n].name;
                grid_data.levels.back().type = vars[n].data_type;
                grid_data.levdata.ID.emplace_back(vars[n].name + "@@" + u);
                grid_data.levdata.description.emplace_back();
                grid_data.levdata.units.emplace_back(u);
                grid_data.levdata.write.emplace_back(false);
                for (size_t l = 0; l < vars[n].attrs.size(); ++l) {
                  if (vars[n].attrs[l].data_type == NetCDF::DataType::CHAR &&
                      vars[n].attrs[l].name == "long_name") {
                    grid_data.levdata.description.back() = *(reinterpret_cast<
                        string *>(vars[n].attrs[l].values));
                  }
                }
                unique_level_id_table.emplace(vars[n].name);
              }
            }
          }
        }
      }
    }
  }
}

bool found_alternate_lat_lon_coordinates(vector<NetCDF::Variable>& vars,
    GridData& grid_data) {
  for (size_t n = 0; n < vars.size(); ++n) {
    if (!vars[n].is_coord && vars[n].dimids.size() == 2) {
      for (size_t m = 0; m < vars[n].attrs.size(); ++m) {
        if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].data_type ==
            NetCDF::DataType::CHAR) {
          auto units = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
          if (units == "degrees_north" || units == "degree_north" || units ==
              "degrees_n" || units == "degree_n") {
            size_t d = 0;
            for (size_t l = 0; l < vars[n].dimids.size(); ++l) {
              d = 100 * d + vars[n].dimids[l] + 1;
            }
            d *= 100;
            grid_data.lats.emplace_back();
            grid_data.lats.back().dim = d;
            grid_data.lats.back().id = vars[n].name;
            grid_data.lats.back().type = vars[n].data_type;
          } else if (units == "degrees_east" || units == "degree_east" ||
              units == "degrees_e" || units == "degree_e") {
            size_t d = 0;
            for (size_t l = 0; l < vars[n].dimids.size(); ++l) {
              d = 100 * d + vars[n].dimids[l] + 1;
            }
            d *= 100;
            grid_data.lons.emplace_back();
            grid_data.lons.back().dim = d;
            grid_data.lons.back().id = vars[n].name;
            grid_data.lons.back().type = vars[n].data_type;
          }
        }
      }
    }
  }
  return grid_data.lats.size() > 0 && grid_data.lats.size() == grid_data.lons
      .size();
}

void fill_grid_projection(Grid::GridDimensions& dim,double *lats,double *lons,Grid::GridDefinition& def)
{
  static const string THIS_FUNC=this_function_label(__func__);
  const double PI=3.141592654;
  double min_lat_var=99999.,max_lat_var=0.;
  double min_lon_var=99999.,max_lon_var=0.;
  int n,m,l;
  double diff,min_diff;
  double first_lat,last_lat;

  for (n=1,m=dim.x; n < dim.y; ++n,m+=dim.x) {
    diff=fabs(lats[m]-lats[m-dim.x]);
    if (diff < min_lat_var) {
      min_lat_var=diff;
    }
    if (diff > max_lat_var) {
      max_lat_var=diff;
    }
  }
  for (n=1; n < dim.x; ++n) {
    diff=fabs(lons[n]-lons[n-1]);
    if (diff < min_lon_var) {
      min_lon_var=diff;
    }
    if (diff > max_lon_var) {
      max_lon_var=diff;
    }
  }
  def.type = Grid::Type::not_set;
  dim.size=dim.x*dim.y;
  if (fabs(max_lon_var-min_lon_var) < 0.0001) {
    if (fabs(max_lat_var-min_lat_var) < 0.0001) {
      def.type=Grid::Type::latitudeLongitude;
      def.elatitude=lats[dim.size-1];
      def.elongitude=lons[dim.size-1];
    } else {
      first_lat=lats[0];
      last_lat=lats[dim.size-1];
      if (first_lat >= 0. && last_lat >= 0.) {
      }
      else if (first_lat < 0. && last_lat < 0.) {
      }
      else {
        if (fabs(first_lat) > fabs(last_lat)) {
        } else {
          if (fabs(cos(last_lat*PI/180.)-(min_lat_var/max_lat_var)) < 0.01) {
            def.type=Grid::Type::mercator;
            def.elatitude=lats[dim.size-1];
            def.elongitude=lons[dim.size-1];
            min_diff=99999.;
            l=-1;
            for (n=0,m=0; n < dim.y; ++n,m+=dim.x) {
              diff=fabs(lats[m]-lround(lats[m]));
              if (diff < min_diff) {
                min_diff=diff;
                l=m;
              }
            }
            def.dx=lround(cos(lats[l]*PI/180.)*min_lon_var*111.2);
            def.dy=lround((fabs(lats[l+dim.x]-lats[l])+fabs(lats[l]-lats[l-dim.x]))/2.*111.2);
            def.stdparallel1=lats[l];
          }
        }
      }
    }
  }
  if (def.type == Grid::Type::not_set) {
    log_error2("unable to determine grid projection",THIS_FUNC,"nc2xml",USER);
  }
}

void scan_cf_grid_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << THIS_FUNC << "()..." << endl;
  }
  grid_initialize();

  // open a file inventory unless this is a test run
  if (metautils::args.dsnum < "999.0") {
    gatherxml::fileInventory::open(inv_file, &inv_dir, inv_stream, "GrML",
        "nc2xml", USER);
  }
  auto attrs = istream.global_attributes();
  string source;
  for (size_t n = 0; n < attrs.size(); ++n) {
    if (to_lower(attrs[n].name) == "source") {
      source = *(reinterpret_cast<string *>(attrs[n].values));
    }
  }
  auto vars = istream.variables();
  GridData grid_data;
  my::map<metautils::NcTime::TimeRangeEntry> tr_table;
  find_coordinate_variables(istream, vars, grid_data, tr_table);
  vector<Grid::GridDimensions> grid_dims;
  vector<Grid::GridDefinition> grid_defs;
  if (grid_data.lats.size() == 0 || grid_data.lons.size() == 0) {
    if (grid_data.lats.size() > 0) {

      // could be a zonal mean if latitude was found, but not longitude
      for (size_t n=0; n < grid_data.lats.size(); ++n) {
         grid_data.lons.emplace_back();
         grid_data.lons.back().dim = MISSING_FLAG;
       }
    } else if (grid_data.lons.size() > 0) {
      log_error2("found longitude coordinate variable, but not latitude "
          "coordinate variable", THIS_FUNC, "nc2xml", USER);
    } else {
      if (gatherxml::verbose_operation) {
        cout << "looking for alternate latitude/longitude ..." << endl;
      }
      if (!found_alternate_lat_lon_coordinates(vars, grid_data)) {
        cerr << "Terminating - could not find any latitude/longitude "
            "coordinates" << endl;
        exit(1);
      }
      if (gatherxml::verbose_operation) {
        cout << "... found alternate latitude/longitude" << endl;
      }
      auto dims = istream.dimensions();
      for (size_t n = 0; n < grid_data.lats.size(); ++n) {
        if (grid_data.lats[n].dim != grid_data.lons[n].dim) {
          log_error2("alternate latitude and longitude coordinate variables "
              "(" + itos(n) + ") do not have the same dimensions", THIS_FUNC,
              "nc2xml", USER);
        }
        grid_dims.emplace_back();
        grid_dims.back().y = grid_data.lats[n].dim / 10000 - 1;
        grid_dims.back().x = (grid_data.lats[n].dim % 10000) / 100 - 1;
        grid_defs.emplace_back();
        NetCDF::VariableData v;
        istream.variable_data(grid_data.lats[n].id, v);
        grid_defs.back().slatitude = v.front();
        auto nlats = v.size();
        unique_ptr<double[]> lats(new double[nlats]);
        for (size_t m = 0; m < nlats; ++m) {
          lats[m] = v[m];
        }
        istream.variable_data(grid_data.lons[n].id, v);
        grid_defs.back().slongitude = v.front();
        auto nlons = v.size();
        unique_ptr<double[]> lons(new double[nlons]);
        for (size_t m = 0; m < nlons; ++m) {
          lons[m] = v[m];
        }
        if ( (dims[grid_dims.back().y].length % 2) == 1 && (dims[grid_dims.back()
            .x].length % 2) == 1) {
          grid_dims.back().x = dims[grid_dims.back().x].length;
          grid_dims.back().y = dims[grid_dims.back().y].length;
          fill_grid_projection(grid_dims.back(), lats.get(), lons.get(),
              grid_defs.back());
        } else {
          auto& xd = grid_dims.back().x;
          auto& nx = dims[xd].length;
          auto& yd = grid_dims.back().y;
          auto& ny = dims[yd].length;
          auto ny2 = ny / 2 - 1;
          auto nx2 = nx / 2 - 1;
          // check the four points that surround the center of the grid to see
          //    if the center is the pole:
          //        1) all four latitudes must be the same
          //        2) the sum of the absolute values of opposing longitudes
          //           must equal 180.
          if (floatutils::myequalf(lats[ny2 * nx + nx2], lats[(ny2 + 1) * nx +
              nx2], 0.00001) && floatutils::myequalf(lats[(ny2 + 1) * nx + nx2],
              lats[(ny2 + 1) * nx + nx2 + 1], 0.00001) && floatutils::myequalf(
              lats[(ny2 + 1) * nx + nx2 + 1], lats[ny2 * nx + nx2 + 1],
              0.00001) && floatutils::myequalf(fabs(lons[ny2 * nx + nx2]) +
              fabs(lons[(ny2 + 1) * nx + nx2 + 1]), 180., 0.001) && floatutils
              ::myequalf(fabs(lons[(ny2 + 1) * nx + nx2]) + fabs(lons[ny2 * nx +
              nx2 + 1]), 180., 0.001)) {
            grid_defs.back().type = Grid::Type::polarStereographic;
            if (lats[ny2 * nx + nx2] > 0) {
              grid_defs.back().projection_flag = 0;
              grid_defs.back().llatitude = 60.;
            } else {
              grid_defs.back().projection_flag = 1;
              grid_defs.back().llatitude = -60.;
            }
            grid_defs.back().olongitude = lroundf(lons[ny2 * nx + nx2] + 45.);
            if (grid_defs.back().olongitude > 180.) {
              grid_defs.back().olongitude -= 360.;
            }

            // look for dx and dy at the 60-degree parallel
            // great circle formula:
            //    theta = 2 * arcsin[ sqrt( sin^2( delta_phi / 2 ) +
            //        cos(phi_1) * cos(phi_2) * sin^2( delta_lambda / 2 ) ) ]
            //    phi_1 and phi_2 are latitudes
            //    lambda_1 and lambda_2 are longitudes
            //    dist = 6372.8 * theta
            //    6372.8 is radius of Earth in km
            xd = nx;
            yd = ny;
            double min_fabs = 999., f;
            int min_m = 0;
            for (size_t m = 0; m < nlats; ++m) {
              if ( (f = fabs(grid_defs.back().llatitude - lats[m])) <
                min_fabs) {
                min_fabs = f;
                min_m = m;
              }
            }
            const double RAD = 3.141592654/180.;
            grid_defs.back().dx = lroundf(asin(sqrt(sin(fabs(lats[min_m] - lats[
                min_m + 1]) / 2. * RAD) * sin(fabs(lats[min_m] - lats[min_m +
                1]) / 2. * RAD) + sin(fabs(lons[min_m] - lons[min_m + 1]) / 2. *
                RAD) * sin(fabs(lons[min_m] - lons[min_m + 1]) / 2. * RAD) *
                cos(lats[min_m] * RAD) * cos(lats[min_m + 1] * RAD))) *
                12745.6);
            grid_defs.back().dy = lroundf(asin(sqrt(sin(fabs(lats[min_m] - lats[
                min_m + xd]) / 2. * RAD) * sin(fabs(lats[min_m] - lats[min_m +
                xd]) / 2. * RAD) + sin(fabs(lons[min_m] - lons[min_m + xd]) /
                2. * RAD) * sin(fabs(lons[min_m] - lons[min_m + xd]) / 2. *
                RAD) * cos(lats[min_m] * RAD) * cos(lats[min_m + xd] * RAD))) *
                12745.6);
          } else {
            auto londiff = fabs(lons[1] - lons[0]);
            for (size_t n = 0; n < ny; ++n) {
              for (size_t m = 1; m < nx; ++m) {
                auto x = n * nx + m;
                if (!floatutils::myequalf(fabs(lons[x] - lons[x - 1]), londiff,
                    0.000001)) {
                  londiff = 1.e36;
                  n = ny;
                  m = nx;
                }
              }
            }
            if (!floatutils::myequalf(londiff, 1.e36)) {
              auto latdiff = fabs(lats[1] - lats[0]);
              for (size_t m = 0; m < nx; ++m) {
                for (size_t n = 1; n < ny; ++n) {
                  auto x = m * ny + n;
                  if (!floatutils::myequalf(fabs(lats[x] - lats[x - 1]),
                      latdiff, 0.000001)) {
                    latdiff = 1.e36;
                    m = nx;
                    n = ny;
                  }
                }
              }
              if (!floatutils::myequalf(latdiff, 1.e36)) {
                grid_defs.back().type = Grid::Type::latitudeLongitude;
                xd = nx;
                yd = ny;
                grid_defs.back().elatitude = lats[nlats - 1];
                grid_defs.back().elongitude = lons[nlons - 1];
                grid_defs.back().laincrement = latdiff;
                grid_defs.back().loincrement = londiff;
              } else {
                for (size_t n = 0; n < ny; ++n) {
                  auto x = n * nx;
                  latdiff = fabs(lats[x + 1] - lats[x]);
                  for (size_t m = 2; m < nx; ++m) {
                    auto x = n * nx + m;
                    if (!floatutils::myequalf(fabs(lats[x] - lats[x - 1]),
                        latdiff, 0.000001)) {
                      latdiff = 1.e36;
                      n = ny;
                      m = nx;
                    }
                  }
                }
                if (!floatutils::myequalf(latdiff, 1.e36)) {
                  const double PI = 3.141592654;
                  auto a = log(tan(PI / 4. + lats[0] * PI / 360.));
                  auto b = log(tan(PI / 4. + lats[ny2 * nx] * PI / 360.));
                  auto c = log(tan(PI / 4. + lats[ny2 * 2 * nx] * PI / 360.));
                  if (floatutils::myequalf((b - a)/ny2, (c - a) / (ny2 * 2),
                      0.000001)) {
                    grid_defs.back().type = Grid::Type::mercator;
                    xd = nx;
                    yd = ny;
                    grid_defs.back().elatitude = lats[nlats - 1];
                    grid_defs.back().elongitude = lons[nlons - 1];
                    grid_defs.back().laincrement = (grid_defs.back().
                        elatitude - grid_defs.back().slatitude) / (yd - 1);
                    grid_defs.back().loincrement = londiff;
                  }
                }
              }
            }
          }
          if (grid_defs.back().type == Grid::Type::not_set) {
            log_error2("unable to determine grid projection", THIS_FUNC,
                "nc2xml", USER);
          }
        }
      }
    }
  }
  if (!grid_data.time.id.empty() && grid_data.levdata.ID.size() == 0) {

    // look for a level coordinate that is not a coordinate variable
    if (gatherxml::verbose_operation) {
      cout << "looking for an alternate level coordinate ..." << endl;
    }
    unordered_set<size_t> already_identified_levdimids;
    for (size_t n = 0; n < grid_data.lats.size(); ++n) {
      if (grid_data.lats[n].dim > 100) {
        size_t m = grid_data.lats[n].dim / 10000 - 1;
        size_t l = (grid_data.lats[n].dim % 10000) / 100 - 1;
        if (grid_data.levels.size() > 0 && grid_data.levels.back().dim !=
            MISSING_FLAG) {
          grid_data.levels.emplace_back();
          grid_data.levels.back().dim = MISSING_FLAG;
          grid_data.levdata.ID.emplace_back();
          grid_data.levdata.description.emplace_back();
          grid_data.levdata.units.emplace_back();
          grid_data.levdata.write.emplace_back(false);
        }
        for (size_t k = 0; k < vars.size(); ++k) {
          if (!vars[k].is_coord && vars[k].dimids.size() == 4 && vars[k]
                .dimids[0] == grid_data.time.dim && vars[k].dimids[2] == m &&
                vars[k].dimids[3] == l) {

            // check netCDF variables for what they are using as a level
            //    dimension
            if (grid_data.levels.back().dim == MISSING_FLAG) {
              if (already_identified_levdimids.find(vars[k].dimids[1]) ==
                  already_identified_levdimids.end()) {
                grid_data.levels.back().dim = vars[k].dimids[1];
                already_identified_levdimids.emplace(grid_data.levels.back()
                    .dim);
              }
            } else if (grid_data.levels.back().dim != vars[k].dimids[1]) {
              log_error2("found multiple level dimensions for the gridded "
                  "parameters - failed on parameter '" + vars[k].name + "'",
                  THIS_FUNC, "nc2xml", USER);
            }
          }
        }
      }
    }
    while (grid_data.levels.size() > 0 && grid_data.levels.back().dim ==
        MISSING_FLAG) {
      grid_data.levels.pop_back();
      grid_data.levdata.ID.pop_back();
      grid_data.levdata.description.pop_back();
      grid_data.levdata.units.pop_back();
      grid_data.levdata.write.pop_back();
    }
    if (grid_data.levels.size() > 0) {
      if (gatherxml::verbose_operation) {
        cout << "... found " << grid_data.levels.size() << " level "
            "coordinates" << endl;
      }
      for (size_t n = 0; n < grid_data.levels.size(); ++n) {
        for (size_t k = 0; k < vars.size(); ++k) {
          if (!vars[k].is_coord && vars[k].dimids.size() == 1 && vars[k]
              .dimids[0] == grid_data.levels[n].dim) {
            grid_data.levels[n].id = vars[k].name;
            grid_data.levels[n].type = vars[k].data_type;
            string d, u;
            for (size_t m = 0; m < vars[k].attrs.size(); ++m) {
              if (vars[k].attrs[m].name == "description" && vars[k].attrs[m]
                  .data_type == NetCDF::DataType::CHAR) {
                d = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
              } else if (vars[k].attrs[m].name == "units" && vars[k].attrs[m]
                  .data_type == NetCDF::DataType::CHAR) {
                u = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
              }
            }
            grid_data.levdata.ID[n] = vars[k].name + "@@" + u;
            if (d.empty()) {
              grid_data.levdata.description[n] = vars[k].name;
            } else {
              grid_data.levdata.description[n] = d;
            }
            grid_data.levdata.units[n] = u;
          }
        }
      }
    }
  }
  if (grid_data.levels.size() > 0 && grid_data.levdata.ID.size() == 0) {
    log_error2("unable to determine the level coordinate variable", THIS_FUNC,
        "nc2xml", USER);
  }
  grid_data.levels.emplace_back();
  grid_data.levels.back().dim = MISSING_FLAG;
  grid_data.levels.back().id = "sfc";
  grid_data.levels.back().type = NetCDF::DataType::_NULL;
  grid_data.levdata.ID.emplace_back("sfc");
  grid_data.levdata.description.emplace_back("Surface");
  grid_data.levdata.units.emplace_back();
  grid_data.levdata.write.emplace_back(false);
  unordered_set<string> parameter_table;
  if (!grid_data.time.id.empty() && grid_data.lats.size() > 0 && grid_data.lons
      .size() > 0) {
    if (gatherxml::verbose_operation) {
      cout << "found coordinates, ready to scan netCDF variables ..." << endl;
    }
    if (tr_table.size() == 0) {
      metautils::NcTime::TimeRangeEntry tre;
      tre.key = -1;

      // set key for climate model simulations
      if (source == "CAM") {
        tre.key = -11;
      }
      tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);

      // get t number of time steps and the temporal range
      NetCDF::VariableData v;
      istream.variable_data(grid_data.time.id, v);
      time_s.t1 = v.front();
      time_s.t2 = v.back();
      time_s.num_times = v.size();
      if (inv_stream.is_open()) {
        time_s.times = new double[time_s.num_times];
        for (size_t m = 0; m < static_cast<size_t>(v.size()); ++m) {
          time_s.times[m] = v[m];
        }
      }
      string error;
      tre.data->instantaneous.first_valid_datetime = metautils::NcTime
          ::actual_date_time(time_s.t1, time_data, error);
      if (!error.empty()) {
        log_error2(error, THIS_FUNC, "nc2xml", USER);
      }
      tre.data->instantaneous.last_valid_datetime = metautils::NcTime
          ::actual_date_time(time_s.t2, time_data, error);
      if (!error.empty()) {
        log_error2(error, THIS_FUNC, "nc2xml", USER);
      }
      if (gatherxml::verbose_operation) {
        cout << "   ...setting temporal range to:" << endl;
        cout << "      " << tre.data->instantaneous.first_valid_datetime
            .to_string() << " to " << tre.data->instantaneous
            .last_valid_datetime.to_string() << endl;
      }
      tre.data->num_steps = v.size();
      if (!grid_data.time_bounds.id.empty()) {
        if (gatherxml::verbose_operation) {
          cout << "   ...adjusting times for time bounds" << endl;
        }
        for (size_t m = 0; m < vars.size(); ++m) {
          if (vars[m].name == grid_data.time_bounds.id) {
            grid_data.time_bounds.type = vars[m].data_type;
            m =vars.size();
          }
        }
        if (grid_data.time_bounds.type == NetCDF::DataType::_NULL) {
          log_error2("unable to determine type of time bounds", THIS_FUNC,
              "nc2xml", USER);
        }
        istream.variable_data(grid_data.time_bounds.id, v);
        if (v.size() != time_s.num_times*2) {
          log_error2("unable to handle more than two time bounds values per "
              "time", THIS_FUNC, "nc2xml", USER);
        }
        time_bounds_s.t1 = v.front();
        time_bounds_s.diff = v[1]-time_bounds_s.t1;
        for (size_t l = 2; l < static_cast<size_t>(v.size()); l += 2) {
          double diff = v[l + 1] - v[l];
          if (!floatutils::myequalf(diff, time_bounds_s.diff)) {
            if (time_data.units != "days" || time_bounds_s.diff < 28 ||
                time_bounds_s.diff > 31 || diff < 28 || diff > 31) {
              time_bounds_s.changed = true;
            }
          }
        }
        time_bounds_s.t2 = v.back();
        tre.data->bounded.first_valid_datetime = metautils::NcTime
            ::actual_date_time(time_bounds_s.t1, time_data, error);
        if (!error.empty()) {
          log_error2(error, THIS_FUNC, "nc2xml", USER);
        }
        tre.data->bounded.last_valid_datetime = metautils::NcTime
            ::actual_date_time(time_bounds_s.t2, time_data, error);
        if (!error.empty()) {
          log_error2(error, THIS_FUNC, "nc2xml", USER);
        }
        if (gatherxml::verbose_operation) {
          cout << "      ...now temporal range is:" << endl;
          cout << "         " << tre.data->bounded.first_valid_datetime
              .to_string() << " to " << tre.data->bounded.last_valid_datetime
              .to_string() << endl;
        }
      }
      if (time_data.units == "months") {
        if ((tre.data->instantaneous.first_valid_datetime).day() == 1) {
//          (tre.instantaneous->last_valid_datetime).addDays(dateutils::days_in_month((tre.instantaneous->last_valid_datetime).year(),(tre.instantaneous->last_valid_datetime).month(),time_data.calendar)-1,time_data.calendar);
(tre.data->instantaneous.last_valid_datetime).add_months(1);
        }
/*
        if (!grid_data.time_bounds.id.empty()) {
          if ((tre.data->bounded.first_valid_datetime).day() == 1) {
            (tre.data->bounded.last_valid_datetime).add_months(1);
          }
        }
*/
      }
      tr_table.insert(tre);
    }
    for (size_t n = 0; n < grid_data.lats.size(); ++n) {
      if (grid_data.lats[n].dim < 100) {
        grid_defs.emplace_back(Grid::GridDefinition());
        grid_defs.back().type = Grid::Type::latitudeLongitude;

        // get the latitude range
        NetCDF::VariableData v;
        istream.variable_data(grid_data.lats[n].id, v);
        grid_dims.emplace_back(Grid::GridDimensions());
        grid_dims.back().y = v.size();
        grid_defs.back().slatitude = v.front();
        grid_defs.back().elatitude = v.back();
        grid_defs.back().laincrement = fabs((grid_defs.back().elatitude -
            grid_defs.back().slatitude)/(v.size() - 1));
        if (grid_data.lons[n].dim != MISSING_FLAG) {

          // check for gaussian lat-lon
          if (!floatutils::myequalf(fabs(v[1] - v[0]), grid_defs.back().
              laincrement, 0.001) && floatutils::myequalf(v.size() / 2., v
              .size() / 2, 0.00000000001)) {
            grid_defs.back().type = Grid::Type::gaussianLatitudeLongitude;
            grid_defs.back().laincrement = v.size() / 2;
          }
          if (!grid_data.lats_b[n].id.empty()) {
            if (grid_data.lons_b[n].id.empty()) {
              log_error2("found a lat bounds but no lon bounds", THIS_FUNC,
              "nc2xml", USER);
            }
            istream.variable_data(grid_data.lats_b[n].id, v);
            grid_defs.back().slatitude = v.front();
            grid_defs.back().elatitude = v.back();
            grid_defs.back().is_cell = true;
          }

          // get the longitude range
          istream.variable_data(grid_data.lons[n].id, v);
          grid_dims.back().x = v.size();
          grid_defs.back().slongitude = v.front();
          grid_defs.back().elongitude = v.back();
          grid_defs.back().loincrement = fabs((grid_defs.back().elongitude -
              grid_defs.back().slongitude) / (v.size() - 1));
          if (!grid_data.lons_b[n].id.empty()) {
            if (grid_data.lats_b[n].id.empty()) {
              log_error2("found a lon bounds but no lat bounds", THIS_FUNC,
                  "nc2xml", USER);
            }
            istream.variable_data(grid_data.lons_b[n].id, v);
            grid_defs.back().slongitude = v.front();
            grid_defs.back().elongitude = v.back();
          }
        }
      }
    }
    for (size_t m = 0; m < grid_data.levels.size(); ++m) {
      auto levid = grid_data.levdata.ID[m].substr(0, grid_data.levdata.ID[m]
          .find("@@"));
      NetCDF::VariableData levels;
      size_t num_levels;
      if (grid_data.levels[m].type == NetCDF::DataType::_NULL) {
        num_levels = 1;
      } else {
        istream.variable_data(levid, levels);
        num_levels = levels.size();
      }
      for (const auto& tr_key : tr_table.keys()) {
        metautils::NcTime::TimeRangeEntry tre;
        tr_table.found(tr_key, tre);
        for (size_t k = 0; k < grid_data.lats.size(); ++k) {
          vector<string> gentry_keys;
          add_gridded_lat_lon_keys(gentry_keys, grid_dims[k], grid_defs[k],
              grid_data.time.id, grid_data.time.dim, grid_data.levels[m].dim,
              grid_data.lats[k].dim, grid_data.lons[k].dim, tre, vars);
          for (const auto& g_key : gentry_keys) {
            gentry_p->key = g_key;
            auto idx = gentry_p->key.rfind("<!>");
            auto U_key = gentry_p->key.substr(idx + 3);
            if (U_map.find(U_key) == U_map.end()) {
              U_map.emplace(U_key, make_pair(U_map.size(), ""));
            }
            if (!gridtbl_p->found(gentry_p->key, *gentry_p)) {

              // new grid
              gentry_p->level_table.clear();
              lentry_p->parameter_code_table.clear();
              pentry_p->num_time_steps = 0;
              add_gridded_parameters_to_netcdf_level_entry(vars, gentry_p->key,
                  grid_data.time.id, grid_data.time.dim, grid_data.levels[m].dim,
                  grid_data.lats[k].dim, grid_data.lons[k].dim, tre,
                  parameter_table, scan_data);
              for (size_t n = 0; n < num_levels; ++n) {
                lentry_p->key = "ds" + metautils::args.dsnum + "," + grid_data
                    .levdata.ID[m] + ":";
                switch (grid_data.levels[m].type) {
                  case NetCDF::DataType::INT: {
                    lentry_p->key += itos(levels[n]);
                    break;
                  }
                  case NetCDF::DataType::FLOAT:
                  case NetCDF::DataType::DOUBLE: {
                    lentry_p->key += ftos(levels[n], floatutils::precision(
                        levels[n]) + 2);
                    break;
                  }
                  case NetCDF::DataType::_NULL: {
                    lentry_p->key += "0";
                     break;
                  }
                  default: { }
                }
                if (lentry_p->parameter_code_table.size() > 0) {
                  gentry_p->level_table.insert(*lentry_p);
                  if (inv_stream.is_open()) {
                    add_level_to_inventory(lentry_p->key, gentry_p->key,
                        grid_data.time.dim, grid_data.levels[m].dim, grid_data
                        .lats[k].dim, grid_data.lons[k].dim, istream);
                  }
                  grid_data.levdata.write[m] = true;
                }
              }
              if (gentry_p->level_table.size() > 0) {
                gridtbl_p->insert(*gentry_p);
              }
            } else {

              // existing grid - needs update
              for (size_t n = 0; n < num_levels; ++n) {
                lentry_p->key = "ds" + metautils::args.dsnum + "," + grid_data
                    .levdata.ID[m] + ":";
                switch (grid_data.levels[m].type) {
                  case NetCDF::DataType::INT: {
                    lentry_p->key += itos(levels[n]);
                    break;
                  }
                  case NetCDF::DataType::FLOAT:
                  case NetCDF::DataType::DOUBLE: {
                    lentry_p->key += ftos(levels[n], floatutils::precision(
                        levels[n]) + 2);
                    break;
                  }
                  case NetCDF::DataType::_NULL: {
                    lentry_p->key += "0";
                    break;
                  }
                  default: { }
                }
                if (!gentry_p->level_table.found(lentry_p->key, *lentry_p)) {
                  lentry_p->parameter_code_table.clear();
                  add_gridded_parameters_to_netcdf_level_entry(vars, gentry_p->
                      key, grid_data.time.id, grid_data.time.dim, grid_data
                      .levels[m].dim, grid_data.lats[k].dim, grid_data.lons[k]
                      .dim, tre, parameter_table, scan_data);
                  if (lentry_p->parameter_code_table.size() > 0) {
                    gentry_p->level_table.insert(*lentry_p);
                    if (inv_stream.is_open()) {
                      add_level_to_inventory(lentry_p->key, gentry_p->key,
                          grid_data.time.dim, grid_data.levels[m].dim, grid_data
                          .lats[k].dim, grid_data.lons[k].dim, istream);
                    }
                    grid_data.levdata.write[m] = true;
                  }
                } else {

                  // run through all of the parameters
                  for (size_t l = 0; l < vars.size(); ++l) {
                    if (!vars[l].is_coord && vars[l].dimids[0] == grid_data.time
                        .dim && ((vars[l].dimids.size() == 4 && grid_data.levels[
                        m].dim >= 0 && vars[l].dimids[1] == grid_data.levels[m]
                        .dim && vars[l].dimids[2] == grid_data.lats[k].dim &&
                        vars[l].dimids[3] == grid_data.lons[k].dim) || (vars[l]
                        .dimids.size() == 3 && grid_data.levels[m].dim < 0 &&
                        vars[l].dimids[1] == grid_data.lats[k].dim && vars[l]
                        .dimids[2] == grid_data.lons[k].dim))) {
                      pentry_p->key = "ds" + metautils::args.dsnum + ":" +
                          vars[l].name;
                      auto time_method = gridded_time_method(vars[l], grid_data
                          .time.id);
                      time_method = strutils::capitalize(time_method);
                      if (!lentry_p->parameter_code_table.found(pentry_p->key,
                          *pentry_p)) {
                        if (time_method.empty() || (floatutils::myequalf(
                            time_bounds_s.t1, 0, 0.0001) &&
                            floatutils::myequalf(time_bounds_s.t1,
                            time_bounds_s.t2, 0.0001))) {
                          add_gridded_netcdf_parameter(vars[l], tre.data->
                              instantaneous.first_valid_datetime, tre.data->
                              instantaneous.last_valid_datetime, tre.data->
                              num_steps, parameter_table, scan_data);
                        } else {
                          if (time_bounds_s.changed) {
                            log_error2("time bounds changed", THIS_FUNC,
                            "nc2xml", USER);
                          }
                          add_gridded_netcdf_parameter(vars[l], tre.data->
                              bounded.first_valid_datetime, tre.data->
                              bounded.last_valid_datetime, tre.data->num_steps,
                              parameter_table, scan_data);
                        }
                        gentry_p->level_table.replace(*lentry_p);
                        if (inv_stream.is_open()) {
                          add_level_to_inventory(lentry_p->key, gentry_p->key,
                              grid_data.time.dim, grid_data.levels[m].dim,
                              grid_data.lats[k].dim, grid_data.lons[k].dim,
                              istream);
                        }
                      } else {
                        string error;
                        auto tr_description=metautils::NcTime
                            ::gridded_netcdf_time_range_description(tre,
                            time_data, time_method, error);
                        if (!error.empty()) {
                          log_error2(error, THIS_FUNC, "nc2xml", USER);
                        }
                        tr_description = strutils::capitalize(tr_description);
                        if (strutils::has_ending(gentry_p->key,
                            tr_description)) {
                          if (time_method.empty() || (floatutils::myequalf(
                              time_bounds_s.t1, 0, 0.0001) &&
                              floatutils::myequalf(time_bounds_s.t1,
                              time_bounds_s.t2, 0.0001))) {
                            if (tre.data->instantaneous.first_valid_datetime <
                                pentry_p->start_date_time) {
                              pentry_p->start_date_time = tre.data->
                                  instantaneous.first_valid_datetime;
                            }
                            if (tre.data->instantaneous.last_valid_datetime >
                                pentry_p->end_date_time) {
                              pentry_p->end_date_time = tre.data->
                                  instantaneous.last_valid_datetime;
                            }
                          } else {
                            if (tre.data->bounded.first_valid_datetime <
                                pentry_p->start_date_time) {
                              pentry_p->start_date_time = tre.data->
                                  bounded.first_valid_datetime;
                            }
                            if (tre.data->bounded.last_valid_datetime >
                                pentry_p->end_date_time) {
                              pentry_p->end_date_time=tre.data->
                                  bounded.last_valid_datetime;
                            }
                          }
                          pentry_p->num_time_steps += tre.data->num_steps;
                          lentry_p->parameter_code_table.replace(*pentry_p);
                          gentry_p->level_table.replace(*lentry_p);
                          if (inv_stream.is_open()) {
                            add_level_to_inventory(lentry_p->key, gentry_p->key,
                            grid_data.time.dim, grid_data.levels[m].dim,
                            grid_data.lats[k].dim, grid_data.lons[k].dim,
                            istream);
                          }
                        }
                      }
                      grid_data.levdata.write[m] = true;
                    }
                  }
                }
              }
              gridtbl_p->replace(*gentry_p);
            }
          }
        }
      }
    }
    unique_ptr<TempDir> tdir(new TempDir);
    if (!tdir->create(metautils::directives.temp_path)) {
      log_error2("can't create temporary directory for netCDF levels",
          THIS_FUNC, "nc2xml", USER);
    }
    auto level_map_file = unixutils::remote_web_file("https://rda.ucar.edu/"
        "metadata/LevelTables/netCDF.ds" + metautils::args.dsnum + ".xml",
        tdir->name());
    LevelMap level_map;
    vector<string> map_contents;
    if (level_map.fill(level_map_file)) {
      std::ifstream ifs(level_map_file.c_str());
      char line[32768];
      ifs.getline(line, 32768);
      while (!ifs.eof()) {
        map_contents.emplace_back(line);
        ifs.getline(line, 32768);
      }
      ifs.close();
      map_contents.pop_back();
    } else {
      map_contents.clear();
    }
    stringstream oss, ess;
    if (mysystem2("/bin/mkdir -p " + tdir->name() + "/metadata/LevelTables",
        oss, ess) < 0) {
      log_error2("can't create directory tree for netCDF levels", THIS_FUNC,
          "nc2xml", USER);
    }
    std::ofstream ofs((tdir->name() + "/metadata/LevelTables/netCDF.ds" +
        metautils::args.dsnum + ".xml").c_str());
    if (!ofs.is_open()) {
      log_error2("can't open output for writing netCDF levels", THIS_FUNC,
          "nc2xml", USER);
    }
    if (map_contents.size() > 0) {
      for (const auto& line : map_contents) {
        ofs << line << endl;
      }
    } else {
      ofs << "<?xml version=\"1.0\" ?>" << endl;
      ofs << "<levelMap>" << endl;
    }
    for (size_t m = 0; m < grid_data.levdata.write.size(); ++m) {
      if (grid_data.levdata.write[m] && (map_contents.size() == 0 ||
          (map_contents.size() > 0 && level_map.is_layer(grid_data.levdata.ID[
          m]) < 0))) {
        ofs << "  <level code=\"" << grid_data.levdata.ID[m] << "\">" << endl;
        ofs << "    <description>" << grid_data.levdata.description[m] <<
            "</description>" << endl;
        ofs << "    <units>" << grid_data.levdata.units[m] << "</units>" << endl;
        ofs << "  </level>" << endl;
      }
    }
    ofs << "</levelMap>" << endl;
    ofs.close();
    string error;
    if (unixutils::rdadata_sync(tdir->name(), "metadata/LevelTables/",
        "/data/web", metautils::directives.rdadata_home, error) < 0) {
      log_warning("level map was not synced - error(s): '" + error + "'",
          "nc2xml", USER);
    }
    mysystem2("/bin/cp " + tdir->name() + "/metadata/LevelTables/netCDF.ds" +
        metautils::args.dsnum + ".xml /glade/u/home/rdadata/share/metadata/"
        "LevelTables/", oss, ess);
    if (gatherxml::verbose_operation) {
      cout << "... done scanning netCDF variables" << endl;
    }
  }
  if (gridtbl_p->size() == 0) {
    if (!grid_data.time.id.empty()) {
      log_error2("no grids found - no content metadata will be generated",
          THIS_FUNC, "nc2xml", USER);
    } else {
      log_error2("time coordinate variable not found - no content metadata "
          "will be generated", THIS_FUNC, "nc2xml", USER);
    }
  }
  scan_data.write_type = ScanData::GrML_type;
  delete[] time_s.times;
  if (gatherxml::verbose_operation) {
    cout << "...function " << THIS_FUNC << "() done." << endl;
  }
}

struct LIDEntry {
  size_t key;
};

/*
void scan_wrf_simulation_netcdf_file(InputNetCDFStream& istream,bool& found_map,string& map_name,vector<string>& var_list,my::map<metautils::StringEntry>& changed_var_table)
{
  std::ifstream ifs;
  char line[32768];
  size_t n,m,l,x=0;
  string sdum;
  my::map<metautils::StringEntry> parameter_table;
  ParameterMap parameter_map;
  LevelMap level_map;
  string timeid,latid,lonid;
  size_t timedimid=0x3fffffff;
  size_t latdimid=0;
  size_t londimid=0;
  Grid::GridDimensions dim;
  Grid::GridDefinition def;
  NetCDF::VariableData var_data;
  double *lats=NULL,*lons=NULL;
  size_t nlats,nlons;
  vector<string> gentry_keys,map_contents;
  metautils::NcTime::TimeRangeEntry tre;
  my::map<LIDEntry> unique_levdimids_table;
  vector<int> levdimids;
  LIDEntry lide;
  TempFile *tmpfile=NULL;
  int idx;
  bool found_time,found_lat,found_lon;

  if (gatherxml::verbose_operation) {
    cout << "...beginning function scan_wrf_simulation_netcdf_file()..." << endl;
  }
  sdum=unixutils::remote_web_file("https://rda.ucar.edu/metadata/LevelTables/netCDF.ds"+metautils::args.dsnum+".xml",tdir->name());
  if (level_map.fill(sdum)) {
    ifs.open(sdum.c_str());
    ifs.getline(line,32768);
    while (!ifs.eof()) {
      map_contents.emplace_back(line);
      ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
    map_contents.pop_back();
  }
  map_name=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/netCDF.ds"+metautils::args.dsnum+".xml",tdir->name());
  if (!parameter_map.fill(map_name)) {
    found_map=false;
  } else {
    found_map=true;
  }
  found_time=found_lat=found_lon=false;
  gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"GrML",THIS_FUNC,"nc2xml",USER);
  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (to_lower(attrs[n].name) == "simulation_start_date") {
      break;
    }
  }
  if (n == attrs.size()) {
    metautils::log_error("does not appear to be a WRF Climate Simulation file",THIS_FUNC,"nc2xml",USER);
  }
  tre.key=-11;
  auto dims=istream.dimensions();
  auto vars=istream.variables();
// find the coordinate variables
  for (n=0; n < vars.size(); ++n) {
    for (m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].is_coord) {
        if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR && vars[n].attrs[m].name == "description") {
          sdum=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          sdum=to_lower(sdum);
          if (regex_search(sdum,regex("since"))) {
            if (found_time) {
              metautils::log_error("time was already identified - don't know what to do with variable: "+vars[n].name,THIS_FUNC,"nc2xml",USER);
            }
            fill_nc_time_data(vars[n].attrs[m]);
            timeid=vars[n].name;
            timedimid=vars[n].dimids[0];
            found_time=true;
            istream.variable_data(vars[n].name,var_data);
            time_s.num_times=var_data.size();
            string error;
            tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);
            tre.data->instantaneous.first_valid_datetime=metautils::NcTime::actual_date_time(var_data.front(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,THIS_FUNC,"nc2xml",USER);
            }
            tre.data->instantaneous.last_valid_datetime=metautils::NcTime::actual_date_time(var_data.back(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,THIS_FUNC,"nc2xml",USER);
            }
            if (inv_stream.is_open()) {
                time_s.times=new double[time_s.num_times];
                for (l=0; l < time_s.num_times; ++l) {
                  time_s.times[l]=var_data[l];
                }
            }
            tre.data->num_steps=time_s.num_times;
          }
        }
      }
      else {
        if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
          sdum=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          if (sdum == "degrees_north" || sdum == "degree_north" || sdum == "degrees_n" || sdum == "degree_n") {
            latid=vars[n].name;
            for (l=0; l < vars[n].dimids.size(); ++l) {
              latdimid=100*latdimid+vars[n].dimids[l]+1;
              ++x;
            }
            latdimid*=100;
          } else if (sdum == "degrees_east" || sdum == "degree_east" || sdum == "degrees_e" || sdum == "degree_e") {
            lonid=vars[n].name;
            for (l=0; l < vars[n].dimids.size(); ++l) {
              londimid=100*londimid+vars[n].dimids[l]+1;
            }
            londimid*=100;
          }
        }
      }
    }
  }
  if (!found_time) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() could not find the time coordinate variable",THIS_FUNC,"nc2xml",USER);
  }
  if (latdimid == 0 || londimid == 0) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() could not find the latitude and longitude coordinate variables",THIS_FUNC,"nc2xml",USER);
  } else if (latdimid != londimid) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() found latitude and longitude coordinate variables, but they do not have the same dimensions",THIS_FUNC,"nc2xml",USER);
  } else {
    if (x == 2) {
      londimid=(latdimid % 10000)/100-1;
      latdimid=latdimid/10000-1;
      istream.variable_data(latid,var_data);
      nlats=var_data.size();
      def.slatitude=var_data.front();
      lats=new double[nlats];
      for (m=0; m < nlats; ++m) {
        lats[m]=var_data[m];
      }
      istream.variable_data(lonid,var_data);
      nlons=var_data.size();
      def.slongitude=var_data.front();
      lons=new double[nlons];
      for (m=0; m < nlons; ++m) {
        lons[m]=var_data[m];
      }
      dim.x=dims[londimid].length;
      dim.y=dims[latdimid].length;
      def.type=0;
      fill_grid_projection(dim,lats,lons,def);
      if (def.type == 0) {
        metautils::log_error("scan_wrf_simulation_netcdf_file() was not able to deterimine the grid definition type",THIS_FUNC,"nc2xml",USER);
      }
      delete[] lats;
      delete[] lons;
    }
  }
  for (n=0; n < vars.size(); ++n) {
    if (vars[n].dimids.size() == 3 || vars[n].dimids.size() == 4) {
      if (vars[n].dimids[0] == timedimid && vars[n].dimids[vars[n].dimids.size()-2] == latdimid && vars[n].dimids[vars[n].dimids.size()-1] == londimid) {
        if (vars[n].dimids.size() == 3) {
          lide.key=static_cast<size_t>(-1);
        } else {
          lide.key=vars[n].dimids[1];
        }
        if (!unique_levdimids_table.found(lide.key,lide)) {
          unique_levdimids_table.insert(lide);
          levdimids.emplace_back(lide.key);
        }
      }
    }
  }
  tmpfile=new TempFile("/tmp","");
  std::ofstream ofs((tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml").c_str());
  if (!ofs.is_open()) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() can't open "+tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml for writing netCDF levels",THIS_FUNC,"nc2xml",USER);
  }
  if (map_contents.size() > 0) {
    for (const auto& line : map_contents) {
      ofs << line << endl;
    }
  } else {
    ofs << "<?xml version=\"1.0\" ?>" << endl;
    ofs << "<levelMap>" << endl;
  }
  for (const auto& levdimid : levdimids) {
    add_gridded_lat_lon_keys(gentry_keys,dim,def,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
    if (levdimid == -1) {
      if (map_contents.size() == 0 || (map_contents.size() > 0 && level_map.is_layer("sfc") < 0)) {
        ofs << "  <level code=\"sfc\">" << endl;
        ofs << "    <description>Surface</description>" << endl;
        ofs << "  </level>" << endl;
      }
    } else {
      for (n=0; n < vars.size(); ++n) {
        if (vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid && (map_contents.size() == 0 || (map_contents.size() > 0 && level_map.is_layer(vars[n].name) < 0))) {
          ofs << "  <level code=\"" << vars[n].name << "\">" << endl;
          for (m=0; m < vars[n].attrs.size(); ++m) {
            if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
              if (vars[n].attrs[m].name == "description") {
                ofs << "    <description>" << *(reinterpret_cast<string *>(vars[n].attrs[m].values)) << "</description>" << endl;
              } else if (vars[n].attrs[m].name == "units") {
                ofs << "    <units>" << *(reinterpret_cast<string *>(vars[n].attrs[m].values)) << "</units>" << endl;
              }
            }
          }
          ofs << "  </level>" << endl;
        }
      }
    }
  }
  ofs << "</levelMap>" << endl;
  ofs.close();
  string error;
  if (unixutils::rdadata_sync(tmpfile->name(),".","/data/web/metadata/LevelTables",metautils::directives.rdadata_home,error) < 0) {
    log_warning("scan_wrf_simulation_netcdf_file() - level map was not synced - error(s): '"+error+"'","nc2xml",USER);
  }
  stringstream oss,ess;
  unixutils::mysystem2("/bin/cp "+tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml /glade/u/home/rdadata/share/metadata/LevelTables/",oss,ess);
  delete tmpfile;
  for (const auto& key : gentry_keys) {
    gentry_p->key=key;
    idx=gentry_p->key.rfind("<!>");
    auto U_key=gentry_p->key.substr(idx+3);
    if (U_map.find(U_key) == U_map.end()) {
      U_map.emplace(U_key,make_pair(U_map.size(),""));
    }
    if (!gridtbl_p->found(gentry_p->key,*gentry_p)) {
// new grid
      gentry_p->level_table.clear();
      lentry_p->parameter_code_table.clear();
      pentry_p->num_time_steps=0;
      for (const auto& levdimid : levdimids) {
        add_gridded_parameters_to_netcdf_level_entry(vars,gentry_p->key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
        if (lentry_p->parameter_code_table.size() > 0) {
          if (levdimid < 0) {
            lentry_p->key="ds"+metautils::args.dsnum+",sfc:0";
            gentry_p->level_table.insert(*lentry_p);
          } else {
            for (n=0; n < vars.size(); ++n) {
              if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
                istream.variable_data(vars[n].name,var_data);
                for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
                  lentry_p->key="ds"+metautils::args.dsnum+","+vars[n].name+":";
                  switch (vars[n].data_type) {
                    case NetCDF::DataType::SHORT:
                    case NetCDF::DataType::INT:
                    {
                      lentry_p->key+=itos(var_data[m]);
                      break;
                    }
                    case NetCDF::DataType::FLOAT:
                    case NetCDF::DataType::DOUBLE:
                    {
                      lentry_p->key+=ftos(var_data[m],3);
                      break;
                    }
                    default:
                    {
                      metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),THIS_FUNC,"nc2xml",USER);
                    }
                  }
                  gentry_p->level_table.insert(*lentry_p);
                  if (inv_stream.is_open()) {
                    add_level_to_inventory(lentry_p->key,gentry_p->key,timedimid,levdimid,latdimid,londimid,istream);
                  }
                }
              }
            }
          }
        }
      }
      if (gentry_p->level_table.size() > 0) {
        gridtbl_p->insert(*gentry_p);
      }
    } else {
// existing grid - needs update
      for (const auto& levdimid : levdimids) {
        if (levdimid < 0) {
          lentry_p->key="ds"+metautils::args.dsnum+",sfc:0";
          if (!gentry_p->level_table.found(lentry_p->key,*lentry_p)) {
            lentry_p->parameter_code_table.clear();
            add_gridded_parameters_to_netcdf_level_entry(vars,gentry_p->key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
            if (lentry_p->parameter_code_table.size() > 0) {
              gentry_p->level_table.insert(*lentry_p);
              if (inv_stream.is_open()) {
                add_level_to_inventory(lentry_p->key,gentry_p->key,timedimid,levdimid,latdimid,londimid,istream);
              }
            }
          }
        } else {
          for (n=0; n < vars.size(); ++n) {
            if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
              if (lentry_p->parameter_code_table.size() > 0) {
                istream.variable_data(vars[n].name,var_data);
              }
              for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
                lentry_p->key="ds"+metautils::args.dsnum+","+vars[n].name+":";
                switch (vars[n].data_type) {
                  case NetCDF::DataType::SHORT:
                  case NetCDF::DataType::INT:
                  {
                    lentry_p->key+=itos(var_data[m]);
                    break;
                  }
                  case NetCDF::DataType::FLOAT:
                  case NetCDF::DataType::DOUBLE:
                  {
                    lentry_p->key+=ftos(var_data[m],3);
                    break;
                  }
                  default:
                  {
                    metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),THIS_FUNC,"nc2xml",USER);
                  }
                }
                if (!gentry_p->level_table.found(lentry_p->key,*lentry_p)) {
                  lentry_p->parameter_code_table.clear();
                  add_gridded_parameters_to_netcdf_level_entry(vars,gentry_p->key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
                  if (lentry_p->parameter_code_table.size() > 0) {
                    gentry_p->level_table.insert(*lentry_p);
                    if (inv_stream.is_open()) {
                      add_level_to_inventory(lentry_p->key,gentry_p->key,timedimid,levdimid,latdimid,londimid,istream);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  if (gridtbl_p->size() == 0) {
    metautils::log_error("No grids found - no content metadata will be generated",THIS_FUNC,"nc2xml",USER);
  }
  write_type=GrML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function scan_wrf_simulation_netcdf_file() done." << endl;
  }
}
*/

void scan_cf_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  auto attrs = istream.global_attributes();
  string ft, lft, p;
  for (size_t n = 0; n < attrs.size(); ++n) {
    if (attrs[n].name == "featureType") {
      ft = *(reinterpret_cast<string *>(attrs[n].values));
      lft = to_lower(ft);
      trim(ft);
    } else if (attrs[n].name == "platform") {
      p = *(reinterpret_cast<string *>(attrs[n].values));
      trim(p);
    }
  }

  // rename the parameter map so that it is not overwritten by the level map,
  //    which has the same name
  if (!scan_data.map_name.empty()) {
    stringstream oss, ess;
    mysystem2("/bin/mv " + scan_data.map_name + " " + scan_data.map_name + ".p",
        oss, ess);
    if (!ess.str().empty()) {
      log_error2("unable to rename parameter map; error - '" + ess.str() + "'",
          THIS_FUNC, "nc2xml", USER);
    }
    scan_data.map_name += ".p";
  }
  if (!ft.empty()) {
    if (!scan_data.map_name.empty() && scan_data.datatype_map.fill(scan_data
        .map_name)) {
      scan_data.map_filled = true;
    }
    string pt = "unknown";
    if (!p.empty()) {
      MySQL::Server server(metautils::directives.database_server,
          metautils::directives.metadb_username,
          metautils::directives.metadb_password, "");
      if (server) {
        MySQL::LocalQuery query("ObML_platformType", "search.GCMD_platforms",
            "path = '" + p + "'");
        if (query.submit(server) == 0) {
          MySQL::Row row;
          if (query.fetch_row(row)) {
            pt = row[0];
          }
        }
        server.disconnect();
      }
    }
    if (lft == "point") {
      scan_cf_point_netcdf_file(istream, pt, scan_data, obs_data);
    } else if (lft == "timeseries") {
      scan_cf_time_series_netcdf_file(istream, pt, scan_data, obs_data);
    } else if (lft == "profile") {
      scan_cf_profile_netcdf_file(istream, pt, scan_data, obs_data);
    } else if (lft == "timeseriesprofile") {
      scan_cf_time_series_profile_netcdf_file(istream, pt, scan_data, obs_data);
    } else {
      log_error2("featureType '" + ft + "' not recognized", THIS_FUNC, "nc2xml",
          USER);
    }
  } else {
    if (!scan_data.map_name.empty() && scan_data.parameter_map.fill(scan_data
        .map_name)) {
      scan_data.map_filled = true;
    }
    scan_cf_grid_netcdf_file(istream, scan_data);
  }
}

void scan_raf_aircraft_netcdf_file(InputNetCDFStream& istream, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  string timevarname,timeunits,vunits;
  my::map<metautils::StringEntry> coords_table;
  DateTime reftime;
  metautils::StringEntry se;
  vector<string> datatypes_list;
  bool ignore_as_datatype,ignore_altitude=false;

  auto attrs=istream.global_attributes();
  string obs_type,platform_type;
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "Aircraft") {
      obs_type="upper_air";
      platform_type="aircraft";
      auto attr_val=*(reinterpret_cast<string *>(attrs[n].values));
      auto val_parts=split(attr_val);
      ientry.key=platform_type+"[!]callSign[!]"+metautils::clean_id(val_parts.front());
    } else if (attrs[n].name == "coordinates") {
      auto attr_val=*(reinterpret_cast<string *>(attrs[n].values));
      auto val_parts=split(attr_val);
      for (size_t m=0; m < val_parts.size(); ++m) {
        se.key=val_parts[m];
        coords_table.insert(se);
      }
    }
  }
  if (!obs_type.empty()) {
    if (coords_table.size() == 0) {
      log_error2("unable to determine variable coordinates",THIS_FUNC,"nc2xml",USER);
    }
  } else {
    log_error2("file does not appear to be NCAR-RAF/nimbus compliant netCDF",THIS_FUNC,"nc2xml",USER);
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled=true;
  }
  NetCDF::VariableData time_data,lat_data,lon_data,alt_data;
  NetCDFVariableAttributeData lat_nc_va_data,lon_nc_va_data;
  auto vars=istream.variables();
  for (size_t n=0; n < vars.size(); ++n) {
    ignore_as_datatype=false;
    string long_name,units,standard_name;
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].name == "long_name" || vars[n].attrs[m].name == "standard_name") {
        auto attr_val=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
        if (vars[n].attrs[m].name == "long_name") {
          long_name=attr_val;
        } else if (vars[n].attrs[m].name == "standard_name") {
          standard_name=attr_val;
        }
        attr_val=to_lower(attr_val);
        if (regex_search(attr_val,regex("time")) && timevarname.empty()) {
          timevarname=vars[n].name;
          istream.variable_data(vars[n].name,time_data);
        } else if (regex_search(attr_val,regex("latitude")) || regex_search(attr_val,regex("longitude")) || regex_search(attr_val,regex("altitude"))) {
          ignore_as_datatype=true;
        }
      }
      else if (vars[n].attrs[m].name == "units") {
        units=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
      }
      if (vars[n].name == timevarname && vars[n].attrs[m].name == "units") {
        auto attr_val=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
        auto val_parts=split(attr_val);
        if (val_parts.size() < 4 || val_parts[1] != "since") {
          log_error2("bad units '"+attr_val+"' on time variable",THIS_FUNC,"nc2xml",USER);
        }
        timeunits=val_parts[0];
        auto date_parts=split(val_parts[2],"-");
        if (date_parts.size() != 3) {
          log_error2("bad date in time variable units '"+attr_val+"'",THIS_FUNC,"nc2xml",USER);
        }
        auto yr=stoi(date_parts[0]);
        auto mo=stoi(date_parts[1]);
        auto dy=stoi(date_parts[2]);
        auto time_parts=split(val_parts[3],":");
        if (time_parts.size() != 3) {
          log_error2("bad time in time variable units '"+attr_val+"'",THIS_FUNC,"nc2xml",USER);
        }
        auto time=stoi(time_parts[0])*10000+stoi(time_parts[1])*100+stoi(time_parts[2]);
        reftime.set(yr,mo,dy,time);
      }
    }
    auto var_name_l=to_lower(vars[n].name);
    if (regex_search(var_name_l,regex("lat")) && coords_table.found(vars[n].name,se)) {
      istream.variable_data(vars[n].name,lat_data);
      extract_from_variable_attribute(vars[n].attrs,vars[n].data_type,lat_nc_va_data);
    } else if (regex_search(var_name_l,regex("lon")) && coords_table.found(vars[n].name,se)) {
      istream.variable_data(vars[n].name,lon_data);
      extract_from_variable_attribute(vars[n].attrs,vars[n].data_type,lon_nc_va_data);
    } else if (regex_search(var_name_l,regex("alt")) && coords_table.found(vars[n].name,se)) {
      for (size_t m=0; m < vars[n].attrs.size(); ++m) {
        if (vars[n].attrs[m].name == "units") {
          vunits=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
        }
      }
      istream.variable_data(vars[n].name,alt_data);
    } else if (!ignore_as_datatype && !coords_table.found(vars[n].name,se)) {
      auto datatype=vars[n].name+"<!>"+long_name+"<!>"+units+"<!>";
      if (!units.empty()) {
        datatype+="<nobr>"+units+"</nobr>";
      }
      datatypes_list.emplace_back(datatype);
    }
  }
  size_t seconds_mult=0;
  if (timeunits == "seconds") {
    seconds_mult=1;
  } else if (timeunits == "minutes") {
    seconds_mult=60;
  } else if (timeunits == "hours") {
    seconds_mult=3600;
  }
  if (seconds_mult == 0) {
    log_error2("bad time units '"+timeunits+"' on time variable",THIS_FUNC,"nc2xml",USER);
  }
  double max_altitude=-99999.,min_altitude=999999.;
  for (size_t n=0; n < time_data.size(); ++n) {
//    if (static_cast<double>(lat_data[n]) != lat_nc_va_data.missing_value.get() && static_cast<double>(lon_data[n]) != lon_nc_va_data.missing_value.get()) {
if (lat_data[n] >= -90. && lat_data[n] <= 90. && lon_data[n] >= -180. && lon_data[n] <= 180.) {
      if (!obs_data.added_to_platforms(obs_type,platform_type,lat_data[n],lon_data[n])) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_type,THIS_FUNC,"nc2xml",USER);
      }
      auto dt=reftime.seconds_added(time_data[n]*seconds_mult);
      auto non_missing=0;
      for (const auto& var : datatypes_list) {
        auto datatype=var.substr(0,var.find("<!>"));
        auto check_value=istream.variable(datatype)._FillValue.get();
        for (const auto& value : istream.value_at(datatype,n)) {
          if (value != check_value) {
            check_value=value;
            break;
          }
        }
        if (check_value != istream.variable(datatype)._FillValue.get()) {
          if (!obs_data.added_to_ids(obs_type,ientry,datatype,"",lat_data[n],lon_data[n],time_data[n],&dt)) {
            auto error=move(myerror);
            log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
          }
          non_missing=1;
        }
      }
      scan_data.num_not_missing+=non_missing;
      if (alt_data.size() > 0) {
        if (alt_data[n] > max_altitude) {
          max_altitude=alt_data[n];
        }
        if (alt_data[n] < min_altitude) {
          min_altitude=alt_data[n];
        }
      }
      else {
        ignore_altitude=true;
      }
    }
  }
  for (const auto& type : datatypes_list) {
    gatherxml::markup::ObML::DataTypeEntry dte;
    dte.key=type.substr(0,type.find("<!>"));
/*
    if (!ientry.data->data_types_table.found(dte.key,dte)) {
      dte.data.reset(new gatherxml::markup::ObML::DataTypeEntry::Data);
      if (!ignore_altitude) {
        if (dte.data->vdata == nullptr) {
          dte.data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::VerticalData);
        }
        dte.data->vdata->max_altitude=-99999.;
        dte.data->vdata->min_altitude=999999.;
        dte.data->vdata->avg_nlev=0;
        dte.data->vdata->avg_res=0.;
        dte.data->vdata->res_cnt=1;
      }
      ientry.data->data_types_table.insert(dte);
    }
    dte.data->nsteps+=ientry.data->nsteps;
    if (!ignore_altitude) {
      if (max_altitude > dte.data->vdata->max_altitude) {
        dte.data->vdata->max_altitude=max_altitude;
      }
      if (min_altitude < dte.data->vdata->min_altitude) {
        dte.data->vdata->min_altitude=min_altitude;
      }
      dte.data->vdata->avg_nlev+=ientry.data->nsteps;
      dte.data->vdata->units=vunits;
    }
    auto descr=scan_data.datatype_map.description(dte.key);
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),type) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(type);
      }
    }
*/
    if (!ignore_altitude && ientry.data->data_types_table.found(dte.key,dte)) {
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

void set_time_missing_value(NetCDF::DataValue& time_miss_val,vector<InputNetCDFStream::Attribute>& attr,size_t index,NetCDF::DataType time_type)
{
  static const string THIS_FUNC=this_function_label(__func__);
  time_miss_val.resize(time_type);
  switch (time_type) {
    case NetCDF::DataType::INT: {
      time_miss_val.set(*(reinterpret_cast<int *>(attr[index].values)));
      break;
    }
    case NetCDF::DataType::FLOAT: {
      time_miss_val.set(*(reinterpret_cast<float *>(attr[index].values)));
      break;
    }
    case NetCDF::DataType::DOUBLE: {
      time_miss_val.set(*(reinterpret_cast<double *>(attr[index].values)));
      break;
    }
    default: {
      log_error2("unrecognized time type: "+itos(static_cast<int>(time_type)),THIS_FUNC,"nc2xml",USER);
    }
  }
}

void scan_npn_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function "+THIS_FUNC+"()..." << endl;
  }
  auto dims=istream.dimensions();
  auto vars=istream.variables();
  size_t id_dim=0,id_len=0,low_level_dim=0,high_level_dim=0;
  for (size_t n=0; n < vars.size(); ++n) {
    if (vars[n].name == "id") {
      id_dim=vars[n].dimids.front();
      id_len=dims[vars[n].dimids.back()].length;
    } else if (vars[n].name == "low_level") {
      low_level_dim=vars[n].dimids.front();
    } else if (vars[n].name == "high_level") {
      high_level_dim=vars[n].dimids.front();
    }
  }
  NetCDF::VariableData ids;
  istream.variable_data("id",ids);
  if (ids.size() == 0) {
    log_error2("station ID variable could not be identified",THIS_FUNC,"nc2xml",USER);
  }
  vector<string> station_ids;
  size_t end=ids.size()/id_len;
  for (size_t n=0; n < end; ++n) {
    auto c=reinterpret_cast<char *>(ids.get());
    if (c[n*id_len] == NetCDF::CHAR_NOT_SET) {
      station_ids.emplace_back("");
    } else {
      station_ids.emplace_back(&c[n*id_len],id_len);
      trim(station_ids.back());
    }
  }
  NetCDF::VariableData lats;
  istream.variable_data("lat",lats);
  if (lats.size() == 0) {
    log_error2("latitude variable could not be identified",THIS_FUNC,"nc2xml",USER);
  }
  NetCDF::VariableData lons;
  istream.variable_data("lon",lons);
  if (lons.size() == 0) {
    log_error2("longitude variable could not be identified",THIS_FUNC,"nc2xml",USER);
  }
  NetCDF::VariableData low_levels;
  istream.variable_data("low_level",low_levels);
  if (low_levels.size() == 0) {
    log_error2("'low_level' variable could not be identified",THIS_FUNC,"nc2xml",USER);
  }
  NetCDF::VariableData high_levels;
  istream.variable_data("high_level",high_levels);
  if (high_levels.size() == 0) {
    log_error2("'high_level' variable could not be identified",THIS_FUNC,"nc2xml",USER);
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled=true;
  }
  short yr,mo,dy;
  size_t time;
  NetCDF::VariableData v;
  istream.variable_data("year",v);
  yr=v.front();
  istream.variable_data("month",v);
  mo=v.front();
  istream.variable_data("day",v);
  dy=v.front();
  istream.variable_data("hour",v);
  time=v.front()*10000;
  istream.variable_data("minute",v);
  time+=v.front()*100+99;
  DateTime dt(yr,mo,dy,time,0);
  auto timestamp=dt.minutes_since(DateTime(1950,1,1,0,0));
  regex azim_re("^azim"),elev_re("^elev");
  for (const auto& var : vars) {
    if (var.dimids.size() > 0 && var.dimids.front() == id_dim) {
      string observation_type;
      if (var.dimids.size() == 1 && var.name != "lat" && var.name != "lon" && !regex_search(var.name,azim_re) && !regex_search(var.name,elev_re)) {
        observation_type="surface";
      }
      else if (var.dimids.size() == 2 && (var.dimids.back() == low_level_dim || var.dimids.back() == high_level_dim)) {
        observation_type="upper_air";
      }
      if (!observation_type.empty()) {
        NetCDF::VariableData var_data;
        istream.variable_data(var.name,var_data);
        const NetCDF::VariableData& level_ref= (var.dimids.back() == low_level_dim) ? low_levels : high_levels;
        for (size_t n=0; n < station_ids.size(); ++n) {
          if (!station_ids[n].empty()) {
            vector<double> levels;
            auto not_missing=false;
            if (var_data.size() == station_ids.size()) {
              if (var_data[n] > -1.e30 && var_data[n] < 1.e30) {
                not_missing=true;
              }
            }
            else {
              size_t num_levels=var_data.size()/station_ids.size();
              size_t end=(n+1)*num_levels;
              for (size_t m=n*num_levels,l=0; m < end; ++m) {
                if (var_data[m] > -1.e30 && var_data[m] < 1.e30) {
                  not_missing=true;
                  levels.emplace_back(level_ref[l]);
                }
                ++l;
              }
            }
            if (not_missing) {
              if (!obs_data.added_to_platforms(observation_type,"wind_profiler",lats[n],-lons[n])) {
                auto error=move(myerror);
                log_error2(error+"' when adding platform "+observation_type,THIS_FUNC,"nc2xml",USER);
              }
              ientry.key="wind_profiler[!]NOAA[!]"+station_ids[n];
              if (!obs_data.added_to_ids(observation_type,ientry,var.name,"",lats[n],-lons[n],timestamp,&dt)) {
                auto error=move(myerror);
                log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
              }
              gatherxml::markup::ObML::DataTypeEntry dte;
              if (observation_type == "upper_air") {
                ientry.data->data_types_table.found(var.name,dte);
                fill_vertical_resolution_data(levels,"up","m",dte);
              }
              ++scan_data.num_not_missing;
            }
          }
        }
        auto descr=scan_data.datatype_map.description(var.name);
        if (descr.empty()) {
            NetCDFVariableAttributeData nc_va_data;
            extract_from_variable_attribute(var.attrs,var_data.type(),nc_va_data);
            if (nc_va_data.units.length() == 1 && nc_va_data.units.front() == 0x1) {
              nc_va_data.units="";
            }
            auto v=var.name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
            if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),v) == scan_data.netcdf_variables.end()) {
              scan_data.netcdf_variables.emplace_back(v);
            }
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

struct Header {
  Header() : type(),ID(),valid_time(),lat(0.),lon(0.),elev(0.) { }

  string type,ID,valid_time;
  float lat,lon,elev;
};
void scan_prepbufr_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t n,nhdrs=0;
  string sdum;
  NetCDF::VariableData var_data;
  int idx;
  DateTime base_date_time(30000101235959),date_time;

  auto attrs=istream.global_attributes();
  sdum="";
  for (n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "MET_tool") {
      sdum=*(reinterpret_cast<string *>(attrs[n].values));
    } else if (attrs[n].name == "FileOrigins") {
      sdum=*(reinterpret_cast<string *>(attrs[n].values));
      if (regex_search(sdum,regex("PB2NC tool"))) {
        sdum="pb2nc";
      }
      else {
        sdum="";
      }
    }
  }
  if (sdum != "pb2nc") {
    log_error2("missing global attribute 'MET_tool' or invalid value",THIS_FUNC,"nc2xml",USER);
  }
  unique_ptr<Header[]> array;
  auto dims=istream.dimensions();
  for (n=0; n < dims.size(); ++n) {
    if (dims[n].name == "nhdr") {
      nhdrs=dims[n].length;
      array.reset(new Header[nhdrs]);
    }
  }
  if (!array) {
    log_error2("could not locate 'nhdr' dimension",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("hdr_arr",var_data) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_arr' data",THIS_FUNC,"nc2xml",USER);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].lat=var_data[n*3];
    array[n].lon=var_data[n*3+1];
    array[n].elev=var_data[n*3+2];
  }
  if (istream.variable_data("hdr_typ",var_data) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_typ' data",THIS_FUNC,"nc2xml",USER);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].type.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
  }
  if (istream.variable_data("hdr_sid",var_data) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_sid' data",THIS_FUNC,"nc2xml",USER);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].ID.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
  }
  if (istream.variable_data("hdr_vld",var_data) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_vld' data",THIS_FUNC,"nc2xml",USER);
  }
  for (n=0; n < nhdrs; ++n) {
      array[n].valid_time.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
      if (array[n].valid_time.empty()) {
        log_error2("empty value in 'hdr_vld' at element "+itos(n),THIS_FUNC,"nc2xml",USER);
      }
      sdum=substitute(array[n].valid_time,"_","");
      date_time.set(stoll(sdum));
      if (date_time < base_date_time) {
        base_date_time=date_time;
      }
  }
  if (istream.num_records() == 0) {
    log_error2("no data records found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("obs_arr",var_data) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'obs_arr' data",THIS_FUNC,"nc2xml",USER);
  }
  string obs_type,platform_type;
  for (n=0; n < istream.num_records(); ++n) {
    if (var_data[n*5+4] > -9999.) {
      ++scan_data.num_not_missing;
      idx=var_data[n*5];
      if (array[idx].type == "ADPUPA") {
        obs_type="upper_air";
        platform_type="land_station";
      }
      else if (array[idx].type == "AIRCAR" || array[idx].type == "AIRCFT") {
        obs_type="upper_air";
        platform_type="aircraft";
      }
      else if (array[idx].type == "SATEMP" || array[idx].type == "SATWND") {
        obs_type="upper_air";
        platform_type="satellite";
      }
      else if (array[idx].type == "PROFLR" || array[idx].type == "RASSDA" || array[idx].type == "VADWND") {
        obs_type="upper_air";
        platform_type="wind_profiler";
      }
      else if (array[idx].type == "SPSSMI") {
        platform_type="satellite";
      }
      else if (array[idx].type == "ADPSFC") {
        obs_type="surface";
        platform_type="land_station";
      }
      else if (array[idx].type == "SFCSHP") {
        obs_type="surface";
        if (strutils::is_numeric(array[idx].ID) && array[idx].ID.length() == 5 && array[idx].ID >= "99000") {
          platform_type="fixed_ship";
        } else {
          platform_type="roving_ship";
        }
      }
      else if (array[idx].type == "ASCATW" || array[idx].type == "ERS1DA" || array[idx].type == "QKSWND" || array[idx].type == "SYNDAT" || array[idx].type == "WDSATR") {
        obs_type="surface";
        platform_type="satellite";
      }
      else if (array[idx].type == "SFCBOG") {
        obs_type="surface";
        platform_type="bogus";
      }
      else {
        log_error2("unknown observation type '"+array[idx].type+"'",THIS_FUNC,"nc2xml",USER);
      }
      ientry.key=prepbufr_id_key(metautils::clean_id(array[idx].ID),platform_type,array[idx].type);
      if (ientry.key.empty()) {
        log_error2("unable to get ID key for '"+array[idx].type+"', ID: '"+array[idx].ID+"'",THIS_FUNC,"nc2xml",USER);
      }
      if (array[idx].type == "SPSSMI") {
        if (strutils::has_ending(ientry.key,"A") || strutils::has_ending(ientry.key,"M") || strutils::has_ending(ientry.key,"S") || strutils::has_ending(ientry.key,"U")) {
          obs_type="surface";
        } else {
          obs_type="upper_air";
        }
      }
      if (obs_type.empty()) {
        log_error2("unable to get observation type for '"+array[idx].type+"', ID: '"+array[idx].ID+"'",THIS_FUNC,"nc2xml",USER);
      }
      auto data_type=ftos(var_data[n*5+1]);
      sdum=substitute(array[idx].valid_time,"_","");
      date_time.set(stoll(sdum));
      if (!obs_data.added_to_ids(obs_type,ientry,data_type,"",array[idx].lat,array[idx].lon,date_time.seconds_since(base_date_time),&date_time)) {
          auto error=move(myerror);
          log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
      }
      if (!obs_data.added_to_platforms(obs_type,platform_type,array[idx].lat,array[idx].lon)) {
        auto error=move(myerror);
        log_error2(error+"' when adding platform "+obs_type+" "+platform_type,THIS_FUNC,"nc2xml",USER);
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_idd_metar_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t n,m;
  int index;
  NetCDF::VariableData lats,lons,times,report_ids,parent_index,var_data;
  NetCDF::DataValue time_miss_val,miss_val;
  size_t id_len,num_not_missing;
  int format=-1;
  vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  metautils::StringEntry se;

  string platform_type="land_station";
  if (istream.variable_data("latitude",lats) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("lat",lats) == NetCDF::DataType::_NULL) {
      log_error2("variable 'latitude' not found",THIS_FUNC,"nc2xml",USER);
    }
  }
  if (istream.variable_data("longitude",lons) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("lon",lons) == NetCDF::DataType::_NULL) {
      log_error2("variable 'longitude' not found",THIS_FUNC,"nc2xml",USER);
    }
  }
  if (istream.variable_data("time_observation",times) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("time_obs",times) == NetCDF::DataType::_NULL) {
      log_error2("variable 'time_observation' not found",THIS_FUNC,"nc2xml",USER);
    }
  }
  if (istream.variable_data("report_id",report_ids) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("stn_name",report_ids) == NetCDF::DataType::_NULL) {
      log_error2("variable 'report_id' not found",THIS_FUNC,"nc2xml",USER);
    } else {
      format=0;
    }
  } else {
    if (istream.variable_data("parent_index",parent_index) == NetCDF::DataType::_NULL) {
      log_error2("variable 'parent_index' not found",THIS_FUNC,"nc2xml",USER);
    }
    format=1;
  }
  if (times.size() == 0) {
    return;
  }
  id_len=report_ids.size()/times.size();
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << vars[n].name << "'" << endl;
    }
    if (regex_search(vars[n].name,regex("^time_obs"))) {
      if (gatherxml::verbose_operation) {
        cout << "  - time variable is '" << vars[n].name << "'" << endl;
      }
      for (m=0; m < vars[n].attrs.size(); ++m) {
        if (gatherxml::verbose_operation) {
          cout << "    found attribute: '" << vars[n].attrs[m].name << "'" << endl;
        }
        if (vars[n].attrs[m].name == "units") {
          fill_nc_time_data(vars[n].attrs[m]);
        } else if (vars[n].attrs[m].name == "_FillValue") {
          set_time_missing_value(time_miss_val,vars[n].attrs,m,times.type());
        }
      }
    } else if (vars[n].is_rec && vars[n].name != "parent_index" && vars[n].name != "prevChild" && !regex_search(vars[n].name,regex("^report")) && vars[n].name != "rep_type" && vars[n].name != "stn_name" && vars[n].name != "wmo_id" && vars[n].name != "lat" && vars[n].name != "lon" && vars[n].name != "elev" && !regex_search(vars[n].name,regex("^ob\\_")) && !regex_search(vars[n].name,regex("^time")) && vars[n].name != "xfields" && vars[n].name != "remarks") {
      if (istream.variable_data(vars[n].name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+vars[n].name+"'",THIS_FUNC,"nc2xml",USER);
      }
      attr=vars[n].attrs;
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(attr,var_data.type(),nc_va_data);
      if (gatherxml::verbose_operation) {
        cout << "    - attributes extracted" << endl;
      }
      num_not_missing=0;
      for (m=0; m < static_cast<size_t>(times.size()); ++m) {
        if (!found_missing(times[m],&time_miss_val,var_data[m],nc_va_data.missing_value)) {
          ++num_not_missing;
          ++scan_data.num_not_missing;
          string id(&(reinterpret_cast<char *>(report_ids.get()))[m*id_len],id_len);
          trim(id);
          index= (format == 0) ? m : parent_index[m];
          if (index < static_cast<int>(times.size())) {
            dt=compute_nc_time(times,m);
            ientry.key=platform_type+"[!]callSign[!]"+metautils::clean_id(id);
            auto descr=scan_data.datatype_map.description(vars[n].name);
            if (descr.empty()) {
              auto var=vars[n].name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
              if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),var) == scan_data.netcdf_variables.end()) {
                scan_data.netcdf_variables.emplace_back(var);
              }
            }
            if (lats[index] >= -90. && lats[index] <= 90. && lons[index] >= -180. && lons[index] <= 180.) {
              if (!obs_data.added_to_ids("surface",ientry,vars[n].name,"",lats[index],lons[index],times[m],&dt)) {
                auto error=move(myerror);
                log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
              }
              if (!obs_data.added_to_platforms("surface",platform_type,lats[index],lons[index])) {
                auto error=move(myerror);
                log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
              }
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << num_not_missing << endl;
      }
    }
  }
}

void scan_idd_buoy_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t n,m;
  NetCDF::VariableData lats,lons,times,ship_ids,buoy_ids,stn_types,var_data;
  NetCDF::DataValue time_miss_val,miss_val;
  size_t id_len,num_not_missing;
  vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  metautils::StringEntry se;

  if (istream.variable_data("Lat",lats) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lat' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("Lon",lons) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lon' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("time_obs",times) == NetCDF::DataType::_NULL) {
    log_error2("variable 'time_obs' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("ship",ship_ids) == NetCDF::DataType::_NULL) {
    log_error2("variable 'ship' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (times.size() == 0) {
    return;
  }
  id_len=ship_ids.size()/times.size();
  if (istream.variable_data("buoy",buoy_ids) == NetCDF::DataType::_NULL) {
    log_error2("variable 'buoy' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("stnType",stn_types) == NetCDF::DataType::_NULL) {
    log_error2("variable 'stnType' not found",THIS_FUNC,"nc2xml",USER);
  }
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << vars[n].name << "'" << endl;
    }
    if (vars[n].name == "time_obs") {
      for (m=0; m < vars[n].attrs.size(); ++m) {
        if (vars[n].attrs[m].name == "units") {
          fill_nc_time_data(vars[n].attrs[m]);
        } else if (vars[n].attrs[m].name == "_FillValue") {
          set_time_missing_value(time_miss_val,vars[n].attrs,m,times.type());
        }
      }
    } else if (vars[n].is_rec && vars[n].name != "rep_type" && vars[n].name != "zone" && vars[n].name != "buoy" && vars[n].name != "ship" && !regex_search(vars[n].name,regex("^time")) && vars[n].name != "Lat" && vars[n].name != "Lon" && vars[n].name != "stnType" && vars[n].name != "report") {
      if (istream.variable_data(vars[n].name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+vars[n].name+"'",THIS_FUNC,"nc2xml",USER);
      }
      attr=vars[n].attrs;
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(attr,var_data.type(),nc_va_data);
      num_not_missing=0;
      for (m=0; m < static_cast<size_t>(times.size()); ++m) {
        if (!found_missing(times[m],&time_miss_val,var_data[m],nc_va_data.missing_value)) {
          ++num_not_missing;
          ++scan_data.num_not_missing;
          string id(&(reinterpret_cast<char *>(ship_ids.get()))[m*id_len],id_len);
          trim(id);
          string platform_type;
          if (!id.empty()) {
            if (stn_types[m] == 6.) {
              platform_type="drifting_buoy";
            }
            else {
              platform_type="roving_ship";
            }
            ientry.key=platform_type+"[!]callSign[!]"+metautils::clean_id(id);
          } else {
            platform_type="drifting_buoy";
            ientry.key=platform_type+"[!]other[!]"+itos(buoy_ids[m]);
          }
          dt=compute_nc_time(times,m);
          auto descr=scan_data.datatype_map.description(vars[n].name);
          if (descr.empty()) {
            auto var=vars[n].name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
            if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),var) == scan_data.netcdf_variables.end()) {
              scan_data.netcdf_variables.emplace_back(var);
            }
          }
          if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
            if (!obs_data.added_to_ids("surface",ientry,vars[n].name,"",lats[m],lons[m],times[m],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            if (!obs_data.added_to_platforms("surface",platform_type,lats[m],lons[m])) {
              auto error=move(myerror);
              log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << num_not_missing << endl;
      }
    }
  }
}

void scan_idd_surface_synoptic_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t n,m;
  NetCDF::VariableData lats,lons,times,wmo_ids,var_data;
  NetCDF::DataValue time_miss_val,miss_val;
  size_t num_not_missing;
  vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  metautils::StringEntry se;

  if (istream.variable_data("Lat",lats) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lat' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("Lon",lons) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lon' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("time_obs",times) == NetCDF::DataType::_NULL) {
    log_error2("variable 'time_obs' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("wmoId",wmo_ids) == NetCDF::DataType::_NULL) {
    log_error2("variable 'wmoId' not found",THIS_FUNC,"nc2xml",USER);
  }
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (vars[n].name == "time_obs") {
      for (m=0; m < vars[n].attrs.size(); ++m) {
        if (vars[n].attrs[m].name == "units") {
          fill_nc_time_data(vars[n].attrs[m]);
        } else if (vars[n].attrs[m].name == "_FillValue") {
          set_time_missing_value(time_miss_val,vars[n].attrs,m,times.type());
        }
      }
    } else if (vars[n].is_rec && vars[n].name != "rep_type" && vars[n].name != "wmoId" && vars[n].name != "stnName" && !regex_search(vars[n].name,regex("^time")) && vars[n].name != "Lat" && vars[n].name != "Lon" && vars[n].name != "elev" && vars[n].name != "stnType" && vars[n].name != "report") {
      if (istream.variable_data(vars[n].name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+vars[n].name+"'",THIS_FUNC,"nc2xml",USER);
      }
      attr=vars[n].attrs;
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(attr,var_data.type(),nc_va_data);
      num_not_missing=0;
      for (m=0; m < static_cast<size_t>(times.size()); ++m) {
        if (!found_missing(times[m],&time_miss_val,var_data[m],nc_va_data.missing_value)) {
          ++num_not_missing;
          ++scan_data.num_not_missing;
          string platform_type;
          if (wmo_ids[m] < 99000) {
            platform_type="land_station";
          } else {
            platform_type="fixed_ship";
          }
          ientry.key=platform_type+"[!]WMO[!]"+ftos(wmo_ids[m],5,0,'0');
          dt=compute_nc_time(times,m);
          auto descr=scan_data.datatype_map.description(vars[n].name);
          if (descr.empty()) {
            auto var=vars[n].name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
            if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),var) == scan_data.netcdf_variables.end()) {
              scan_data.netcdf_variables.emplace_back(var);
            }
          }
          if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
            if (!obs_data.added_to_ids("surface",ientry,vars[n].name,"",lats[m],lons[m],times[m],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            if (!obs_data.added_to_platforms("surface",platform_type,lats[m],lons[m])) {
              auto error=move(myerror);
              log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << num_not_missing << endl;
      }
    }
  }
}

void scan_idd_upper_air_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  size_t n,m;
  NetCDF::VariableData lats,lons,times,wmo_ids,stn_ids,var_data;
  NetCDF::DataValue time_miss_val,miss_val,wmo_id_miss_val;
  size_t id_len,num_not_missing;
  vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  metautils::StringEntry se;

  if (istream.variable_data("staLat",lats) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staLat' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("staLon",lons) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staLon' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("synTime",times) == NetCDF::DataType::_NULL) {
    log_error2("variable 'synTime' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("wmoStaNum",wmo_ids) == NetCDF::DataType::_NULL) {
    log_error2("variable 'wmoStaNum' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (istream.variable_data("staName",stn_ids) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staName' not found",THIS_FUNC,"nc2xml",USER);
  }
  if (times.size() == 0) {
    return;
  }
  id_len=stn_ids.size()/times.size();
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << vars[n].name << "'" << endl;
    }
    NetCDFVariableAttributeData nc_wmoid_a_data;
    if (vars[n].name == "synTime") {
      if (gatherxml::verbose_operation) {
        cout << "  - time variable is '" << vars[n].name << "'; " << vars[n].attrs.size() << " attributes; type: " << static_cast<int>(times.type()) << endl;
      }
      for (m=0; m < vars[n].attrs.size(); ++m) {
        if (gatherxml::verbose_operation) {
          cout << "    found attribute: '" << vars[n].attrs[m].name << "'" << endl;
        }
        if (vars[n].attrs[m].name == "units") {
          fill_nc_time_data(vars[n].attrs[m]);
        } else if (vars[n].attrs[m].name == "_FillValue") {
          set_time_missing_value(time_miss_val,vars[n].attrs,m,times.type());
        }
      }
    } else if (vars[n].name == "wmoStaNum") {
      attr=vars[n].attrs;
      extract_from_variable_attribute(attr,wmo_ids.type(),nc_wmoid_a_data);
    } else if (vars[n].is_rec && (vars[n].name == "numMand" || vars[n].name == "numSigT" || vars[n].name == "numSigW" || vars[n].name == "numMwnd" || vars[n].name == "numTrop")) {
      if (istream.variable_data(vars[n].name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+vars[n].name+"'",THIS_FUNC,"nc2xml",USER);
      }
      attr=vars[n].attrs;
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(attr,var_data.type(),nc_va_data);
      if (gatherxml::verbose_operation) {
        cout << "    - attributes extracted" << endl;
      }
      num_not_missing=0;
      for (m=0; m < static_cast<size_t>(times.size()); ++m) {
        if (!found_missing(times[m],&time_miss_val,var_data[m],nc_va_data.missing_value)) {
          ++num_not_missing;
          ++scan_data.num_not_missing;
          string platform_type;
          if (wmo_ids[m] < 99000 || wmo_ids[m] > 99900) {
            platform_type="land_station";
          } else {
            platform_type="fixed_ship";
          }
          if (nc_wmoid_a_data.missing_value.type() != NetCDF::DataType::_NULL) {
            if (wmo_ids[m] == nc_wmoid_a_data.missing_value.get()) {
              string id(&(reinterpret_cast<char *>(stn_ids.get()))[m*id_len],id_len);
              trim(id);
              ientry.key=platform_type+"[!]callSign[!]"+metautils::clean_id(id);
            }
            else {
              ientry.key=platform_type+"[!]WMO[!]"+ftos(wmo_ids[m],5,0,'0');
            }
          } else {
            ientry.key=platform_type+"[!]WMO[!]"+ftos(wmo_ids[m],5,0,'0');
          }
          dt=compute_nc_time(times,m);
          auto descr=scan_data.datatype_map.description(vars[n].name);
          if (descr.empty()) {
            auto var=vars[n].name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
            if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),var) == scan_data.netcdf_variables.end()) {
              scan_data.netcdf_variables.emplace_back(var);
            }
          }
          if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
            if (!obs_data.added_to_ids("upper_air",ientry,vars[n].name,"",lats[m],lons[m],times[m],&dt)) {
              auto error=move(myerror);
              log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
            }
            if (!obs_data.added_to_platforms("upper_air",platform_type,lats[m],lons[m])) {
              auto error=move(myerror);
              log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << num_not_missing << endl;
      }
    }
  }
}

void scan_idd_observation_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  auto attrs=istream.global_attributes();
  string type;
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "title") {
      auto title=*(reinterpret_cast<string *>(attrs[n].values));
      auto title_parts=split(title);
      type=title_parts.front();
    }
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled=true;
  }
  if (type == "METAR") {
    scan_idd_metar_netcdf_file(istream, scan_data, obs_data);
  } else if (type == "BUOY") {
    scan_idd_buoy_netcdf_file(istream, scan_data, obs_data);
  } else if (type == "SYNOPTIC") {
    scan_idd_surface_synoptic_netcdf_file(istream, scan_data, obs_data);
  } else {
    scan_idd_upper_air_netcdf_file(istream, scan_data, obs_data);
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_samos_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string THIS_FUNC=this_function_label(__func__);
  string timevarname,latvarname,lonvarname;
  NetCDF::VariableData times,lats,lons;
  unordered_set<string> unique_var_table;
  bool found_time,found_lat,found_lon;

  found_time=found_lat=found_lon=false;
  string platform_type="roving_ship";
  ientry.key="";
  auto attrs=istream.global_attributes();
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "ID") {
      ientry.key=platform_type+"[!]callSign[!]"+*(reinterpret_cast<string *>(attrs[n].values));
    }
  }
  auto vars=istream.variables();
// find the coordinate variables
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR && vars[n].attrs[m].name == "units") {
        string units_value=to_lower(*(reinterpret_cast<string *>(vars[n].attrs[m].values)));
        if (vars[n].is_coord && regex_search(units_value,regex("since"))) {
          if (found_time) {
            log_error2("time was already identified - don't know what to do with variable: "+vars[n].name,THIS_FUNC,"nc2xml",USER);
          }
          fill_nc_time_data(vars[n].attrs[m]);
          found_time=true;
          timevarname=vars[n].name;
          if (istream.variable_data(vars[n].name,times) == NetCDF::DataType::_NULL) {
            log_error2("unable to get times",THIS_FUNC,"nc2xml",USER);
          }
          if (times.size() == 0) {
            found_time=false;
          }
        } else if (vars[n].dimids.size() == 1 && vars[n].dimids[0] == 0) {
          if (units_value == "degrees_north") {
            if (istream.variable_data(vars[n].name,lats) == NetCDF::DataType::_NULL) {
              log_error2("unable to get latitudes",THIS_FUNC,"nc2xml",USER);
            }
            latvarname=vars[n].name;
            found_lat=true;
          } else if (units_value == "degrees_east") {
            if (istream.variable_data(vars[n].name,lons) == NetCDF::DataType::_NULL) {
              log_error2("unable to get longitudes",THIS_FUNC,"nc2xml",USER);
            }
            lonvarname=vars[n].name;
            found_lon=true;
          }
        }
      }
    }
  }
  if (!found_time) {
    log_error2("could not find the 'time' variable",THIS_FUNC,"nc2xml",USER);
  } else if (!found_lat) {
    log_error2("could not find the 'latitude' variable",THIS_FUNC,"nc2xml",USER);
  } else if (!found_lon) {
    log_error2("could not find the 'longitude' variable",THIS_FUNC,"nc2xml",USER);
  } else if (ientry.key.empty()) {
    log_error2("could not find the vessel ID",THIS_FUNC,"nc2xml",USER);
  }
  gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"ObML","nc2xml",USER);
  if (inv_stream.is_open()) {
    inv_stream << "netCDF:point|" << istream.record_size() << endl;
    if (O_map.find("surface") == O_map.end()) {
      O_map.emplace("surface",make_pair(O_map.size(),""));
    }
    if (P_map.find(platform_type) == P_map.end()) {
      P_map.emplace(platform_type,make_pair(P_map.size(),""));
    }
  }
// find the data variables
  unordered_map<size_t,string> T_map;
  float min_lat=99.,max_lat=-99.;
  for (size_t n=0; n < vars.size(); ++n) {
    if (vars[n].name != timevarname && vars[n].name != latvarname && vars[n].name != lonvarname) {
      NetCDFVariableAttributeData nc_va_data;
      extract_from_variable_attribute(vars[n].attrs,vars[n].data_type,nc_va_data);
      NetCDF::VariableData var_data;
      if (istream.variable_data(vars[n].name,var_data) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '"+vars[n].name+"'",THIS_FUNC,"nc2xml",USER);
      }
      if (scan_data.datatype_map.description(vars[n].name).empty()) {
        auto key=vars[n].name+"<!>"+nc_va_data.long_name+"<!>"+nc_va_data.units+"<!>"+nc_va_data.cf_keyword;
        if (unique_var_table.find(key) == unique_var_table.end()) {
          scan_data.netcdf_variables.emplace_back(key);
          unique_var_table.emplace(key);
        }
      }
      if (D_map.find(vars[n].name) == D_map.end()) {
        auto bsize=1;
        auto dims=istream.dimensions();
        for (size_t l=1; l < vars[n].dimids.size(); ++l) {
          bsize*=dims[vars[n].dimids[l]].length;
        }
        switch (vars[n].data_type) {
          case NetCDF::DataType::SHORT: {
            bsize*=2;
            break;
          }
          case NetCDF::DataType::INT:
          case NetCDF::DataType::FLOAT: {
            bsize*=4;
            break;
          }
          case NetCDF::DataType::DOUBLE: {
            bsize*=8;
            break;
          }
          default: { }
        }
        D_map.emplace(vars[n].name,make_pair(D_map.size(),"|"+lltos(vars[n].offset)+"|"+NetCDF::data_type_str[static_cast<int>(vars[n].data_type)]+"|"+itos(bsize)));
      }
      vector<string> miss_lines_list;
      for (size_t m=0; m < times.size(); ++m) {
        if (!found_missing(times[m],nullptr,var_data[m],nc_va_data.missing_value)) {
          ++scan_data.num_not_missing;
          float lon=lons[m];
          if (lon > 180.) {
            lon-=360.;
          }
          DateTime dt=time_data.reference.added(time_data.units,times[m]);
          if (inv_stream.is_open()) {
            if (T_map.find(m) == T_map.end()) {
              T_map.emplace(m,dt.to_string("%Y%m%d%H%MM")+"[!]"+ftos(lats[m],4)+"[!]"+ftos(lon,4));
            }
          }
          if (!obs_data.added_to_ids("surface",ientry,vars[n].name,"",lats[m],lon,times[m],&dt)) {
            auto error=move(myerror);
            log_error2(error+"' when adding ID "+ientry.key,THIS_FUNC,"nc2xml",USER);
          }
          if (!obs_data.added_to_platforms("surface",platform_type,lats[m],lon)) {
            auto error=move(myerror);
            log_error2(error+"' when adding platform "+platform_type,THIS_FUNC,"nc2xml",USER);
          }
          if (lats[m] < min_lat) {
            min_lat=lats[m];
          }
          if (lats[m] > max_lat) {
            max_lat=lats[m];
          }
        } else {
          if (inv_stream.is_open()) {
            string miss_line=itos(m);
            miss_line+="|0|"+itos(P_map[platform_type].first)+"|"+itos(I_map[ientry.key.substr(ientry.key.find("[!]")+3)].first)+"|"+itos(D_map[vars[n].name].first);
            miss_lines_list.emplace_back(miss_line);
          }
        }
      }
      if (inv_stream.is_open()) {
        if (miss_lines_list.size() != times.size()) {
          for (const auto& line : miss_lines_list) {
            inv_lines2.writeln(line);
          }
        } else {
          D_map.erase(vars[n].name);
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
  if (inv_stream.is_open()) {
    size_t w_index,e_index;
    bitmap::longitudeBitmap::west_east_bounds(ientry.data->min_lon_bitmap.get(),w_index,e_index);
    auto I_key=ientry.key.substr(ientry.key.find("[!]")+3)+"[!]"+ftos(min_lat,4)+"[!]"+ftos(ientry.data->min_lon_bitmap[w_index],4)+"[!]"+ftos(max_lat,4)+"[!]"+ftos(ientry.data->max_lon_bitmap[e_index],4);
    if (I_map.find(I_key) == I_map.end()) {
      I_map.emplace(I_key,make_pair(I_map.size(),""));
    }
    vector<size_t> time_indexes;
    for (const auto& e : T_map) {
      time_indexes.emplace_back(e.first);
    }
    std::sort(time_indexes.begin(),time_indexes.end(),
    [](const size_t& left,const size_t& right) -> bool
    {
      if (left <= right) {
        return true;
      }
      else {
        return false;
      }
    });
    for (const auto& idx : time_indexes) {
      inv_stream << "T<!>" << idx << "<!>" << T_map[idx] << endl;
    }
  }
}

void scan_file(ScanData& scan_data, gatherxml::markup::ObML::ObservationData&
     obs_data) {
  static const string THIS_FUNC = this_function_label(__func__);
  unique_ptr<TempFile> tfile(new TempFile);
  tfile->open(metautils::args.temp_loc);
  unique_ptr<TempDir> tdir(new TempDir);
  tdir->create(metautils::args.temp_loc);
  DataTypeMap datatype_map;
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/netCDF.ds" + metautils::args.dsnum + ".xml",
      tdir->name());
  std::list<string> filelist;
  string file_format, error;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
      *tdir, &filelist, file_format, error)) {
    log_error2(error, THIS_FUNC + ": prepare_file_for_metadata_scanning()",
        "nc2xml", USER);
  }
  if (filelist.size() == 0) {
    filelist.emplace_back(tfile->name());
  }
  if (gatherxml::verbose_operation) {
    cout << "Ready to scan " << filelist.size() << " files." << endl;
  }
  for (const auto& file : filelist) {
    if (gatherxml::verbose_operation) {
      cout << "Beginning scan of " << file << "..." << endl;
    }
    InputNetCDFStream istream;
    istream.open(file.c_str());
    conventions = "";
    auto gattrs=istream.global_attributes();
    for (const auto& gattr : gattrs) {
      if (gattr.name == "Conventions") {
        conventions=*(reinterpret_cast<string *>(gattr.values));
      }
    }
    if (regex_search(metautils::args.data_format,regex("^cfnetcdf"))) {
      scan_cf_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format,regex("^iddnetcdf"))) {
      scan_idd_observation_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format,regex("^npnnetcdf"))) {
      scan_npn_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format,regex("^pbnetcdf"))) {
      scan_prepbufr_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format,regex("^rafnetcdf"))) {
      scan_raf_aircraft_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format,regex("^samosnc"))) {
      scan_samos_netcdf_file(istream, scan_data, obs_data);
/*
    } else if (metautils::args.data_format.beginsWith("wrfsimnetcdf")) {
      scan_wrf_simulation_netcdf_file(istream,found_map,map_name,var_list,changed_var_table);
*/
    } else {
      log_error2(metautils::args.data_format+"-formatted files not recognized",THIS_FUNC,"nc2xml",USER);
    }
    istream.close();
    if (gatherxml::verbose_operation) {
      cout << "  ...scan of " << file << " completed." << endl;
    }
  }
  metautils::args.data_format="netcdf";
  if (!metautils::args.inventory_only && scan_data.netcdf_variables.size() >
      0) {
    if (gatherxml::verbose_operation) {
      cout << "Writing parameter map..." << endl;
    }
    string map_type;
    if (scan_data.write_type == ScanData::GrML_type) {
      map_type="parameter";
    } else if (scan_data.write_type == ScanData::ObML_type) {
      map_type="dataType";
    } else {
      log_error2("unknown map type", THIS_FUNC, "nc2xml", USER);
    }
    vector<string> map_contents;
    if (scan_data.map_filled) {
      std::ifstream ifs(scan_data.map_name.c_str());
      char line[32768];
      ifs.getline(line,32768);
      while (!ifs.eof()) {
        map_contents.emplace_back(line);
        ifs.getline(line,32768);
      }
      ifs.close();
      map_contents.pop_back();
      stringstream oss,ess;
      unixutils::mysystem2("/bin/rm "+scan_data.map_name,oss,ess);
    }
    stringstream oss,ess;
    if (unixutils::mysystem2("/bin/mkdir -p "+tdir->name()+"/metadata/ParameterTables",oss,ess) < 0) {
      log_error2("can't create directory tree for netCDF variables",THIS_FUNC,"nc2xml",USER);
    }
    scan_data.map_name=tdir->name()+"/metadata/ParameterTables/netCDF.ds"+metautils::args.dsnum+".xml";
    std::ofstream ofs(scan_data.map_name.c_str());
    if (!ofs.is_open()) {
      log_error2("can't open parameter map file for output",THIS_FUNC,"nc2xml",USER);
    }
    if (!scan_data.map_filled) {
      ofs << "<?xml version=\"1.0\" ?>" << endl;
      ofs << "<" << map_type << "Map>" << endl;
    } else {
      auto no_write=false;
      for (const auto& line : map_contents) {
        if (regex_search(line,regex(" code=\""))) {
          auto s=split(line,"\"");
          if (scan_data.changed_variables.find(s[1]) != scan_data.changed_variables.end()) {
            no_write=true;
          }
        }
        if (!no_write) {
          ofs << line << endl;
        }
        if (regex_search(line,regex("</"+map_type+">"))) {
          no_write=false;
        }
      }
    }
    for (const auto& nc_var : scan_data.netcdf_variables) {
      auto nc_var_parts=split(nc_var,"<!>");
      if (scan_data.write_type == ScanData::GrML_type) {
        ofs << "  <parameter code=\"" << nc_var_parts[0] << "\">" << endl;
        ofs << "    <shortName>" << nc_var_parts[0] << "</shortName>" << endl;
        if (!nc_var_parts[1].empty()) {
          ofs << "    <description>" << nc_var_parts[1] << "</description>" << endl;
        }
        if (!nc_var_parts[2].empty()) {
          ofs << "    <units>" << substitute(nc_var_parts[2],"-","^-") << "</units>" << endl;
        }
        if (nc_var_parts.size() > 3 && !nc_var_parts[3].empty()) {
          ofs << "    <standardName>" << nc_var_parts[3] << "</standardName>" << endl;
        }
        ofs << "  </parameter>" << endl;
      } else if (scan_data.write_type == ScanData::ObML_type) {
        ofs << "  <dataType code=\"" << nc_var_parts[0] << "\">" << endl;
        ofs << "    <description>" << nc_var_parts[1];
        if (!nc_var_parts[2].empty()) {
          ofs << " (" << nc_var_parts[2] << ")";
        }
        ofs << "</description>" << endl;
        ofs << "  </dataType>" << endl;
      }
    }
    if (scan_data.write_type == ScanData::GrML_type) {
      ofs << "</parameterMap>" << endl;
    } else if (scan_data.write_type == ScanData::ObML_type) {
      ofs << "</dataTypeMap>" << endl;
    }
    ofs.close();
    if (unixutils::rdadata_sync(tdir->name(),"metadata/ParameterTables/","/data/web",metautils::directives.rdadata_home,error) < 0) {
      log_warning("parameter map was not synced - error(s): '"+error+"'","nc2xml",USER);
    }
    unixutils::mysystem2("/bin/cp "+scan_data.map_name+" /glade/u/home/rdadata/share/metadata/ParameterTables/netCDF.ds"+metautils::args.dsnum+".xml",oss,ess);
    if (gatherxml::verbose_operation) {
      cout << "...finished writing parameter map." << endl;
    }
  }
}

int main(int argc,char **argv)
{
  if (argc < 4) {
    cerr << "usage: nc2xml -f format -d [ds]nnn.n [options...] path" << endl;
    cerr << endl;
    cerr << "required (choose one):" << endl;
    cerr << "-f cfnetcdf      Climate and Forecast compliant netCDF3 data" <<
        endl;
    cerr << "-f iddnetcdf     Unidata IDD netCDF3 station data" << endl;
    cerr << "-f npnnetcdf     NOAA Profiler Network vertical profile data" <<
        endl;
    cerr << "-f pbnetcdf      NetCDF3 converted from prepbufr" << endl;
    cerr << "-f rafnetcdf     NCAR-RAF/nimbus compliant netCDF3 aircraft "
        "data" << endl;
    cerr << "-f samosnc       SAMOS netCDF3 data" << endl;
//    cerr << "-f wrfsimnetcdf  Climate Simulations from WRF" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data "
        "file belongs" << endl;
    cerr << endl;
    cerr << "options:" << endl;
    if (USER == "dattore") {
      cerr << "-r/-R            regenerate/don't regenerate the dataset "
          "webpage" << endl;
      cerr << "-s/-S            do/don't update the dataset summary "
          "information (default is -s)" << endl;
      cerr << "-u/-U            do/don't update the database (default is -u)" <<
          endl;
      cerr << "-t <path>        path where temporary files should be created" <<
          endl;
      cerr << "-OO              overwrite only - when content metadata already "
          "exists, the" << endl;
      cerr << "                 default is to first delete existing metadata; "
          "this option saves" << endl;
      cerr << "                 time by overwriting without the delete" << endl;
    }
    cerr << "-V               verbose operation" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "<path>           full MSS path or URL of the file to read" << endl;
    cerr << "                 - MSS paths must begin with \"/FS/DECS\"" << endl;
    cerr << "                 - URLs must begin with "
        "\"https://rda.ucar.edu\"" << endl;
    exit(1);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  auto arg_delimiter = '%';
  metautils::args.args_string=unixutils::unix_args_string(argc, argv,
      arg_delimiter);
  metautils::read_config("nc2xml", USER);
  gatherxml::parse_args(arg_delimiter);
  atexit(clean_up);
  metautils::cmd_register("nc2xml", USER);
  if (!metautils::args.overwrite_only && !metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("GrML");
    metautils::check_for_existing_cmd("ObML");
  }
  ScanData scan_data;
  gatherxml::markup::ObML::ObservationData obs_data;
  scan_file(scan_data, obs_data);
  if (gatherxml::verbose_operation && !metautils::args.inventory_only) {
    cout << "Writing XML..." << endl;
  }
  string ext, tdir;
  if (scan_data.write_type == ScanData::GrML_type) {
    ext = "GrML";
    if (!metautils::args.inventory_only) {
      tdir = gatherxml::markup::GrML::write(*gridtbl_p, "nc2xml", USER);
    }
  } else if (scan_data.write_type == ScanData::ObML_type) {
    ext = "ObML";
    if (!metautils::args.inventory_only) {
      if (scan_data.num_not_missing > 0) {
        gatherxml::markup::ObML::write(obs_data, "nc2xml", USER);
      } else {
        log_error2("Terminating - data variables could not be identified or "
            "they only contain missing values. No content metadata will be "
            "saved for this file", "main()", "nc2xml", USER);
      }
    }
  }
  if (gatherxml::verbose_operation && !metautils::args.inventory_only) {
    cout << "...finished writing XML." << endl;
  }
  if (metautils::args.update_db) {
    string flags;
    if (!metautils::args.update_summary) {
      flags += " -S ";
    }
    if (!metautils::args.regenerate) {
      flags += " -R ";
    }
    if (!tdir.empty()) {
      flags += " -t "+tdir;
    }
    if (!metautils::args.inventory_only && regex_search(metautils::args.path,
        regex("^https://rda.ucar.edu"))) {
      flags += " -wf";
    } else {
      flags += " -f";
    }
    if (gatherxml::verbose_operation) {
      cout << "Calling 'scm' to update the database..." << endl;
    }
    stringstream oss, ess;
    if (unixutils::mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " " + flags + " " + metautils::args.filename +
        "." + ext,oss,ess) < 0) {
      log_error2(ess.str(), "main() running scm", "nc2xml", USER);
    }
    if (gatherxml::verbose_operation) {
      cout << "...'scm' finished." << endl;
    }
  } else if (metautils::args.dsnum == "999.9") {
    cout << "Output is in:" << endl;
    cout << "  " << tdir << "/" << metautils::args.filename << ".";
    switch (scan_data.write_type) {
      case ScanData::GrML_type: {
        cout << "Gr";
        break;
      }
      case ScanData::ObML_type: {
        cout << "Ob";
        break;
      }
      default: {
        cout << "??";
      }
    }
    cout << "ML" << endl;
  }
  if (inv_stream.is_open()) {
    vector<pair<int, string>> sorted_keys;
    if (scan_data.write_type == ScanData::GrML_type) {
      sort_inventory_map(U_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "U<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
      sort_inventory_map(G_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "G<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
      sort_inventory_map(L_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "L<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
      sort_inventory_map(P_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "P<!>" << s_key.first << "<!>" << s_key.second;
        if (is_large_offset) {
          inv_stream << "<!>BIG";
        }
        inv_stream << endl;
      }
      sort_inventory_map(R_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "R<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
    } else if (scan_data.write_type == ScanData::ObML_type) {
      sort_inventory_map(O_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "O<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
      sort_inventory_map(P_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "P<!>" << s_key.first << "<!>" << s_key.second << endl;
      }
      sort_inventory_map(I_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "I<!>" << s_key.first << "<!>" << s_key.second << "[!]" <<
            I_map[s_key.second].second << endl;
      }
      sort_inventory_map(D_map, sorted_keys);
      for (const auto& s_key : sorted_keys) {
        inv_stream << "D<!>" << s_key.first << "<!>" << s_key.second <<
            D_map[s_key.second].second << endl;
      }
    }
    inv_stream << "-----" << endl;
    if (inv_lines.size() > 0) {
      for (const auto& line : inv_lines) {
        inv_stream << line << endl;
      }
    } else {
      inv_lines2.close();
      std::ifstream ifs(inv_lines2.name().c_str());
      if (ifs.is_open()) {
        char line[32768];
        ifs.getline(line, 32768);
        while (!ifs.eof()) {
          inv_stream << line << endl;
          ifs.getline(line, 32768);
        }
        ifs.close();
      }
    }
    gatherxml::fileInventory::close(inv_file, &inv_dir, inv_stream, ext, true,
        metautils::args.update_summary, "nc2xml", USER);
  }
  if (unknown_IDs.size() > 0) {
    stringstream ss;
    for (const auto& id : unknown_IDs) {
      ss << id << endl;
    }
    log_warning("unknown ID(s):\n" + ss.str(), "nc2xml", USER);
  }
}
