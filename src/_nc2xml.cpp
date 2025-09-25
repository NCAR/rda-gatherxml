#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <sstream>
#include <regex>
#include <numeric>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <gatherxml.hpp>
#include <pglocks.hpp>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <netcdf.hpp>
#include <PostgreSQL.hpp>
#include <bufr.hpp>
#include <timer.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using floatutils::myequalf;
using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
using std::accumulate;
using std::cerr;
using std::cout;
using std::endl;
using std::find;
using std::list;
using std::make_pair;
using std::move;
using std::pair;
using std::regex;
using std::regex_search;
using std::sort;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;
using strutils::trim;
using unixutils::mysystem2;
using unixutils::open_output;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
auto env = getenv("USER");
extern const string USER = (env == nullptr) ? "unknown" : env;
string myerror = "";
string mywarning = "";
string myoutput = "";

static const size_t MISSING_FLAG = 0xffffffff;

struct ScanData {
  ScanData() : num_not_missing(0), num_missing_loc(0), write_type(-1),
      map_name(), parameter_map(), datatype_map(), conventions(),
      netcdf_variables(), changed_variables(), grids(nullptr), map_filled(false)
      { }

  enum { GrML_type = 1, ObML_type };
  size_t num_not_missing, num_missing_loc;
  int write_type;
  string map_name;
  ParameterMap parameter_map;
  DataTypeMap datatype_map;
  string conventions;
  vector<string> netcdf_variables;
  unordered_set<string> changed_variables;
  unique_ptr<unordered_map<string, gatherxml::markup::GrML::GridEntry>> grids;
  bool map_filled;
};

struct GridData {
  struct CoordinateData {
    CoordinateData() : dim(MISSING_FLAG), id(), type(NetCDF::NCType::_NULL)
        { }

    size_t dim;
    string id;
    NetCDF::NCType type;
  };
  GridData(): time(), time_bounds(), lats(), lats_b(), lons(), lons_b(),
      levels(), levdata() { }

  CoordinateData time, time_bounds;
  vector<CoordinateData> lats, lats_b, lons, lons_b, levels;
  vector<metautils::NcLevel::LevelInfo> levdata;
};

struct Inventory {
  struct Maps {
    Maps() : D(), G(), I(), L(), O(), P(), R(), U() { }

    unordered_map<string, pair<int, string>> D, G, I, L, O, P, R, U;
  };

  Inventory() : file(), dir(nullptr), stream(), maps(), lines() { }

  string file;
  unique_ptr<TempDir> dir;
  std::ofstream stream;
  Maps maps;
  vector<string> lines;
} g_inv;

struct NetCDFVariableAttributeData {
  NetCDFVariableAttributeData() : long_name(), units(), cf_keyword(),
      missing_value() { }

  string long_name, units, cf_keyword;
  NetCDF::DataValue missing_value;
};

vector<NetCDF::Attribute> gattrs;
vector<NetCDF::Dimension> dims;
vector<NetCDF::Variable> vars;
unordered_set<string> non_data_vars;
metautils::NcTime::Time time_s;
metautils::NcTime::TimeBounds time_bounds_s;
metautils::NcTime::TimeData time_data;
gatherxml::markup::ObML::IDEntry ientry;
string grid_entry_key;
unique_ptr<gatherxml::markup::GrML::GridEntry> grid_entry_ptr;
string level_entry_key;
unique_ptr<gatherxml::markup::GrML::LevelEntry> level_entry_ptr;
string parameter_entry_key;
unique_ptr<gatherxml::markup::GrML::ParameterEntry> parameter_entry_ptr;
TempFile inv_lines2("/tmp");
unordered_set<string> unknown_IDs;
bool is_large_offset = false;

unordered_map<string, string> id_platform_map{ { "AUSTRALIA", "land_station" },
    { "COOP", "land_station" }, { "WBAN", "land_station" } };

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

void grid_initialize(ScanData& scan_data) {
  if (scan_data.grids == nullptr) {
    scan_data.grids.reset(new unordered_map<string, gatherxml::markup::GrML::
        GridEntry>);
    grid_entry_ptr.reset(new gatherxml::markup::GrML::GridEntry);
    level_entry_ptr.reset(new gatherxml::markup::GrML::LevelEntry);
    parameter_entry_ptr.reset(new gatherxml::markup::GrML::ParameterEntry);
  }
}

void sort_inventory_map(unordered_map<string, pair<int, string>>& inv_table,
     vector<pair<int, string>>& sorted_keys) {
  sorted_keys.clear();
  for (const auto& e : inv_table) {
    sorted_keys.emplace_back(make_pair(e.second.first, e.first));
  }
  sort(sorted_keys.begin(), sorted_keys.end(),
  [](const pair<int, string>& left, const pair<int, string>& right) -> bool {
    if (left.first <= right.first) {
      return true;
    }
    return false;
  });
}

void fill_nc_time_data(string time_units) {
  static const string F = this_function_label(__func__);
  if (!regex_search(time_units, regex("since"))) {
    log_error2("not a CF time units value", F, "nc2xml", USER);
  }
  if (gatherxml::verbose_operation) {
    cout << "  Time units: '" << time_units << "'" << endl;
  }
  time_data.units = to_lower(time_units.substr(0, time_units.find("since")));
  trim(time_data.units);
  time_units = time_units.substr(time_units.find("since") + 5);
  while (!time_units.empty() && (time_units[0] < '0' || time_units[0] > '9')) {
    time_units = time_units.substr(1);
  }
  auto n = time_units.length() - 1;
  while (n > 0 && (time_units[n] < '0' || time_units[n] > '9')) {
    --n;
  }
  ++n;
  if (n < time_units.length()) {
    time_units = time_units.substr(0, n);
  }
  trim(time_units);
  auto sp = split(time_units);
  if (sp.size() < 1 || sp.size() > 3) {
    log_error2("unable to get reference time from units specified as: '" +
        time_units + "'", F, "nc2xml", USER);
  }
  auto sp2 = split(sp[0], "-");
  if (sp2.size() != 3) {
    log_error2("unable to get reference time from units specified as: '" +
        time_units + "'", F, "nc2xml", USER);
  }
  time_data.reference.set_year(stoi(sp2[0]));
  time_data.reference.set_month(stoi(sp2[1]));
  time_data.reference.set_day(stoi(sp2[2]));
  if (sp.size() > 1) {
    auto sp3 = split(sp[1], ":");
    switch (sp3.size()) {
      case 1: {
        time_data.reference.set_time(stoi(sp3[0]) * 10000);
        break;
      }
      case 2: {
        time_data.reference.set_time(stoi(sp3[0]) * 10000 + stoi(sp3[1]) * 100);
        break;
      }
      case 3: {
        time_data.reference.set_time(stoi(sp3[0]) * 10000 + stoi(sp3[1]) * 100 +
            static_cast<int>(stof(sp3[2])));
        break;
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "  Reference time set to: " << time_data.reference.to_string(
        "%Y-%m-%d %H:%MM:%SS") << endl;
  }
}

DateTime compute_nc_time(NetCDF::VariableData& times, size_t index) {
  static const string F = this_function_label(__func__);
  static const int Y = dateutils::current_date_time().year();
  double val = times[index];
  if (val < 0.) {
    log_error2("Terminating - negative time offset not allowed", F, "nc2xml",
        USER);
  }
  DateTime dt; // return value
  if (time_data.units == "seconds") {
    dt = time_data.reference.seconds_added(val);
  } else if (time_data.units == "minutes") {
    if (myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.minutes_added(val);
    } else {
      dt = time_data.reference.seconds_added(lroundf(val * 60.));
    }
  } else if (time_data.units == "hours") {
    if (myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.hours_added(val);
    } else {
      dt = time_data.reference.seconds_added(lroundf(val * 3600.));
    }
  } else if (time_data.units == "days") {
    if (myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.days_added(val);
    } else {
      dt = time_data.reference.seconds_added(lroundf(val * 86400.));
    }
  } else {
    log_error2("compute_nc_time() returned error: unable to set date/time for "
        "units '" + time_data.units + "'", F, "nc2xml", USER);
  }
  if (gatherxml::verbose_operation && dt.year() > Y) {
    cout << "Warning: " << dt.to_string() << " is in the future; time value: "
        << val << "; time type: " << static_cast<int>(times.type()) << endl;
  }
  return dt;
}

void extract_from_variable_attribute(const vector<NetCDF::Attribute>&
    attribute_list, NetCDF::NCType nc_type, NetCDFVariableAttributeData&
    nc_attribute_data, string conventions) {
  nc_attribute_data.long_name = "";
  nc_attribute_data.units = "";
  nc_attribute_data.cf_keyword = "";
  nc_attribute_data.missing_value.clear();
  for (const auto& a : attribute_list) {
    if (a.name == "long_name") {
      nc_attribute_data.long_name = *(reinterpret_cast<string *>(a.values));
      trim(nc_attribute_data.long_name);
    } else if (a.name == "units") {
      nc_attribute_data.units = *(reinterpret_cast<string *>(a.values));
      trim(nc_attribute_data.units);
    } else if (a.name == "standard_name" && conventions.length() > 2 &&
        conventions.substr(0, 2) == "CF") {
      nc_attribute_data.cf_keyword = *(reinterpret_cast<string *>(a.values));
      trim(nc_attribute_data.cf_keyword);
    } else if (a.name == "_FillValue" || a.name == "missing_value") {
      nc_attribute_data.missing_value.resize(nc_type);
      switch (nc_type) {
        case NetCDF::NCType::CHAR: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<char *>(a.
              values)));
          break;
        }
        case NetCDF::NCType::SHORT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<short *>(a.
              values)));
          break;
        }
        case NetCDF::NCType::INT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<int *>(a.
              values)));
          break;
        }
        case NetCDF::NCType::FLOAT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<float *>(a.
              values)));
          break;
        }
        case NetCDF::NCType::DOUBLE: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<double *>(a.
              values)));
          break;
        }
        default: { }
      }
    } else if ((regex_search(a.name, regex("^comment")) || regex_search(a.name,
        regex("^Comment"))) && nc_attribute_data.long_name.empty()) {
      nc_attribute_data.long_name = *(reinterpret_cast<string *>(a.values));
    } else if (to_lower(a.name) == "description" && nc_attribute_data.long_name.
        empty()) {
      nc_attribute_data.long_name = *(reinterpret_cast<string *>(a.values));
    }
  }
}

bool found_missing(const double& time, const NetCDF::DataValue
     *time_missing_value, const double& var_value, const NetCDF::DataValue&
     var_missing_value) {
  bool missing = false; // return value
  if (time_missing_value != nullptr) {
    if (myequalf(time, time_missing_value->get())) {
      return true;
    }
  }
  if (var_missing_value.type() != NetCDF::NCType::_NULL) {
    missing = true;
    if (var_value != var_missing_value.get()) {
      missing = false;
    }
  }
  return missing;
}

void add_gridded_netcdf_parameter(const NetCDF::Variable& var, const DateTime&
     first_valid_date_time, const DateTime& last_valid_date_time, int nsteps,
     unordered_set<string>& parameter_table, ScanData& scan_data) {
  NetCDFVariableAttributeData nc_va_data;
  extract_from_variable_attribute(var.attrs, var.nc_type, nc_va_data, scan_data.
      conventions);
  auto key = var.name + "<!>" + nc_va_data.long_name + "<!>" + nc_va_data.units
      + "<!>" + nc_va_data.cf_keyword;
  if (parameter_table.find(key) == parameter_table.end()) {
    parameter_table.emplace(key);
    scan_data.netcdf_variables.emplace_back(key);
    auto s = scan_data.parameter_map.short_name(var.name);
    if (!s.empty()) {
      scan_data.changed_variables.emplace(var.name);
    }
  }
  parameter_entry_ptr->start_date_time = first_valid_date_time;
  parameter_entry_ptr->end_date_time = last_valid_date_time;
  parameter_entry_ptr->num_time_steps = nsteps;
  level_entry_ptr->parameter_code_table.emplace(parameter_entry_key,
      *parameter_entry_ptr);
  if (g_inv.stream.is_open()) {
    if (g_inv.maps.P.find(parameter_entry_key) == g_inv.maps.P.end()) {
      g_inv.maps.P.emplace(parameter_entry_key, make_pair(g_inv.maps.P.size(), ""));
    }
  }
}

bool is_zonal_mean_grid_variable(const NetCDF::Variable& var, size_t timedimid,
    int levdimid, size_t latdimid) {
  if ((var.dimids.size() == 2 || var.dimids.size() == 3) && var.dimids[0] ==
      timedimid && ((var.dimids.size() == 3 && levdimid >= 0 && static_cast<
      int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid) || (var.
      dimids.size() == 2 && levdimid < 0 && var.dimids[1] == latdimid))) {
    return true;
  }
  return false;
}

bool is_regular_lat_lon_grid_variable(const NetCDF::Variable& var, size_t
     timedimid, int levdimid, size_t latdimid, size_t londimid) {
  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 &&
      static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid
      && var.dimids[3] == londimid) || (var.dimids.size() == 3 && levdimid < 0
      && var.dimids[1] == latdimid && var.dimids[2] == londimid))) {
    return true;
  }
  return false;
}

bool is_polar_stereographic_grid_variable(const NetCDF::Variable& var, size_t
     timedimid, int levdimid, size_t latdimid) {
  size_t x = latdimid / 10000 - 1;
  size_t y = (latdimid % 10000) / 100 - 1;

  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 &&
      static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == x && var.
      dimids[3] == y) || (var.dimids.size() == 3 && levdimid < 0 && var.dimids[
      1] == x && var.dimids[2] == y))) {
    return true;
  }
  return false;
}

string gridded_time_method(const NetCDF::Variable& var, string timeid) {
  for (const auto& a : var.attrs) {
    if (a.name == "cell_methods") {
      auto s = *(reinterpret_cast<string *>(a.values));
      auto re = regex("  ");
      while (regex_search(s, re)) {
        replace_all(s, "  ", " ");
      }
      replace_all(s, "comment: ", "");
      replace_all(s, "comments: ", "");
      replace_all(s, "comment:", "");
      replace_all(s, "comments:", "");
      if (!s.empty() && regex_search(s, regex(substitute(timeid, ".", "\\.") +
          ": "))) {
        auto idx = s.find(timeid + ": ");
        if (idx != string::npos) {
          s = s.substr(idx);
        }
        replace_all(s, timeid + ": ", "");
        trim(s);
        idx = s.find(": ");
        if (idx == string::npos) {
          auto parts = split(s);
          if (parts.size() > 1) {
            // remove duplicates
            unordered_set<string> methods;
            for (const auto& part : parts) {
              methods.emplace(part);
            }
            if (methods.size() != parts.size()) {
              s = "";
              for (const auto& e : methods) {
                append(s, e, " ");
              }
            }
          }
          return s;
        } else {

          // filter out other coordinates for this cell method
          auto idx2 = s.find(" ");
          while (idx2 != string::npos && idx2 > idx) {
            s = s.substr(idx2 + 1);
            idx = s.find(": ");
            idx2 = s.find(" ");
          }
          idx = s.find(")");

          // no extra information in parentheses
          if (idx == string::npos) {
            idx = s.find(" ");
            return s.substr(0, idx);
          } else {

            // found extra information
            return s.substr(0, idx + 1);
          }
        }
      }
    }
  }
  return "";
}

void add_grid_to_inventory(string gentry_key) {
  int idx = gentry_key.rfind("<!>");
  auto key = substitute(gentry_key.substr(0, idx), "<!>", ",");
  if (g_inv.maps.G.find(key) == g_inv.maps.G.end()) {
    g_inv.maps.G.emplace(key, make_pair(g_inv.maps.G.size(), ""));
  }
}

void add_level_to_inventory(string lentry_key, string gentry_key, size_t
    timedimid, int levdimid, size_t latdimid, size_t londimid,
    InputNetCDFStream& istream) {
  static const string F = this_function_label(__func__);
  if (g_inv.maps.L.find(lentry_key) == g_inv.maps.L.end()) {
    g_inv.maps.L.emplace(lentry_key, make_pair(g_inv.maps.L.size(), ""));
  }
  auto idx = gentry_key.rfind("<!>");
  string s = "|" + itos(g_inv.maps.U[gentry_key.substr(idx + 3)].first) + "|" +
      itos(g_inv.maps.G[substitute(gentry_key.substr(0, idx), "<!>", ",")].
      first) + "|" + itos(g_inv.maps.L[lentry_key].first);
  auto d = istream.dimensions();
  auto v = istream.variables();
  if (latdimid > 100) {
    latdimid = latdimid / 10000 - 1;
  }
  if (londimid > 100) {
    londimid = (londimid % 10000) / 100 -1;
  }
  for (size_t n = 0; n < v.size(); ++n) {
    auto key = metautils::args.dsid + ":" + v[n].name;
    if (!v[n].dimids.empty() && !v[n].is_coord && v[n].dimids[0] == timedimid &&
        g_inv.maps.P.find(key) != g_inv.maps.P.end() && ((v[n].dimids.size() ==
        4 && levdimid >= 0 && static_cast<int>(v[n].dimids[1]) == levdimid && v[
        n].dimids[2] == latdimid && v[n].dimids[3] == londimid) || (v[n].dimids.
        size() == 3 && levdimid < 0 && v[n].dimids[1] == latdimid && v[n].
        dimids[2] == londimid))) {
      auto rkey = itos(static_cast<int>(v[n].nc_type));
      if (g_inv.maps.R.find(rkey ) == g_inv.maps.R.end()) {
        g_inv.maps.R.emplace(rkey, make_pair(g_inv.maps.R.size(), ""));
      }
      auto vsz = v[n].size;
      if (v[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(v[n].
          dimids[1]) == levdimid) {
        vsz /= d[levdimid].length;
      }
      long long off = v[n].offset;
      for (size_t m = 0; m < time_s.num_times; ++m) {
        string e;
        g_inv.lines.emplace_back(lltos(off) + "|" + itos(vsz) + "|" +
            metautils::NcTime::actual_date_time(time_s.times[m], time_data, e).
            to_string("%Y%m%d%H%MM") + s + "|" + itos(g_inv.maps.P[key].first) +
            "|" + itos(g_inv.maps.R[rkey].first));
        if (!e.empty()) {
          log_error2(e, F, "nc2xml", USER);
        }
        if (off > static_cast<long long>(0xffffffff)) {
          is_large_offset = true;
        }
        off += istream.record_size();
      }
    }
  }
}

void add_gridded_parameters_to_netcdf_level_entry(const vector<NetCDF::
    Variable>& vars, string gentry_key, string timeid, size_t timedimid, int
    levdimid, size_t latdimid, size_t londimid, const metautils::NcTime::
    TimeRangeEntry& tre, unordered_set<string>& parameter_table, ScanData&
    scan_data) {
  static const string F = this_function_label(__func__);
  auto zre = regex("^[12]<!>1<!>");
  // find all of the parameters
  for (const auto& v : vars) {
    if (!v.is_coord) {
      auto tm = gridded_time_method(v, timeid);
      DateTime dt1, dt2;
      if (tm.empty() || (myequalf(time_bounds_s.t1, 0, 0.0001) && myequalf(
          time_bounds_s.t1, time_bounds_s.t2, 0.0001))) {
        dt1 = tre.instantaneous.first_valid_datetime;
        dt2 = tre.instantaneous.last_valid_datetime;
      } else {
        if (time_bounds_s.changed) {
          log_error2("time bounds changed", F, "nc2xml", USER);
        }
        dt1 = tre.bounded.first_valid_datetime;
        dt2 = tre.bounded.last_valid_datetime;
      }
//      tm = strutils::capitalize(tm);
      string e;
      auto tr = metautils::NcTime::gridded_netcdf_time_range_description(tre,
          time_data, tm, e);
      if (!e.empty()) {
        log_error2(e, F, "nc2xml", USER);
      }
//      tr=strutils::capitalize(tr);
      if (strutils::has_ending(gentry_key, tr)) {

        // check as a zonal mean grid variable
        if (regex_search(gentry_key, zre)) {
          if (is_zonal_mean_grid_variable(v, timedimid, levdimid, latdimid)) {
            parameter_entry_key = metautils::args.dsid + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.num_steps,
                parameter_table, scan_data);
            if (g_inv.stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        } else if (v.dimids.size() == 3 || v.dimids.size() == 4) {
          if (is_regular_lat_lon_grid_variable(v, timedimid, levdimid, latdimid,
              londimid)) {

            // check as a regular lat/lon grid variable
            parameter_entry_key = metautils::args.dsid + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.num_steps,
                parameter_table, scan_data);
            if (g_inv.stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          } else if (is_polar_stereographic_grid_variable(v, timedimid,
              levdimid, latdimid)) {

            // check as a polar-stereographic grid variable
            parameter_entry_key = metautils::args.dsid + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.num_steps,
                parameter_table, scan_data);
            if (g_inv.stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        }
      }
    }
  }
}

void update_gridded_parameters_in_netcdf_level_entry(const vector<NetCDF::
    Variable>& vars, GridData& grid_data, size_t y, size_t z, const metautils::
    NcTime::TimeRangeEntry& tre, ScanData& scan_data, unordered_set<string>&
    parameter_table, InputNetCDFStream& istream) {
  static const string F = this_function_label(__func__);
  for (const auto& v : vars) {
    if (!v.is_coord && v.dimids[0] == grid_data.time.dim && ((v.dimids.size() ==
        4 && grid_data.levels[z].dim >= 0 && v.dimids[1] == grid_data.levels[z].
        dim && v.dimids[2] == grid_data.lats[y].dim && v.dimids[3] == grid_data.
        lons[y].dim) || (v.dimids.size() == 3 && grid_data.levels[z].dim < 0 &&
        v.dimids[1] == grid_data.lats[y].dim && v.dimids[2] == grid_data.lons[
        y].dim))) {
      parameter_entry_key = metautils::args.dsid + ":" + v.name;
      auto time_method = gridded_time_method(v, grid_data.time.id);
      time_method = strutils::capitalize(time_method);
      if (level_entry_ptr->parameter_code_table.find(parameter_entry_key) == level_entry_ptr->
          parameter_code_table.end()) {
        if (time_method.empty() || (myequalf(time_bounds_s.t1, 0, 0.0001) &&
            myequalf(time_bounds_s.t1, time_bounds_s.t2, 0.0001))) {
          add_gridded_netcdf_parameter(v, tre.instantaneous.
              first_valid_datetime, tre.instantaneous.last_valid_datetime,
              tre.num_steps, parameter_table, scan_data);
        } else {
          if (time_bounds_s.changed) {
            log_error2("time bounds changed", F, "nc2xml", USER);
          }
          add_gridded_netcdf_parameter(v, tre.bounded.first_valid_datetime, tre.
              bounded.last_valid_datetime, tre.num_steps, parameter_table,
              scan_data);
        }
        grid_entry_ptr->level_table[level_entry_key] = *level_entry_ptr;
        if (g_inv.stream.is_open()) {
          add_level_to_inventory(level_entry_key, grid_entry_key, grid_data.
              time.dim, grid_data.levels[z].dim, grid_data.lats[y].dim,
              grid_data.lons[y].dim, istream);
        }
      } else {
        string error;
        auto tr_description = metautils::NcTime::
            gridded_netcdf_time_range_description(tre, time_data, time_method,
                error);
        if (!error.empty()) {
          log_error2(error, F, "nc2xml", USER);
        }
        tr_description = strutils::capitalize(tr_description);
        if (strutils::has_ending(grid_entry_key, tr_description)) {
          auto pe = level_entry_ptr->parameter_code_table[parameter_entry_key];
          if (time_method.empty() || (myequalf(time_bounds_s.t1, 0, 0.0001) &&
              myequalf(time_bounds_s.t1, time_bounds_s.t2, 0.0001))) {
            if (tre.instantaneous.first_valid_datetime < pe.start_date_time) {
              pe.start_date_time = tre.instantaneous.first_valid_datetime;
            }
            if (tre.instantaneous.last_valid_datetime > pe.end_date_time) {
              pe.end_date_time = tre.instantaneous.last_valid_datetime;
            }
          } else {
            if (tre.bounded.first_valid_datetime < pe.start_date_time) {
              pe.start_date_time = tre.bounded.first_valid_datetime;
            }
            if (tre.bounded.last_valid_datetime > pe.end_date_time) {
              pe.end_date_time=tre.bounded.last_valid_datetime;
            }
          }
          pe.num_time_steps += tre.num_steps;
          grid_entry_ptr->level_table[level_entry_key] = *level_entry_ptr;
          if (g_inv.stream.is_open()) {
            add_level_to_inventory(level_entry_key, grid_entry_key,
            grid_data.time.dim, grid_data.levels[z].dim,
            grid_data.lats[y].dim, grid_data.lons[y].dim,
            istream);
          }
        }
      }
      grid_data.levdata[z].write = true;
    }
  }
}

void add_gridded_time_range(string key_start, vector<string>& gentry_keys,
    string timeid, size_t timedimid, int levdimid, size_t latdimid, size_t
    londimid, const metautils::NcTime::TimeRangeEntry& tre, vector<NetCDF::
    Variable>& vars) {
  static const string F = this_function_label(__func__);
  string gkey;
  unordered_set<string> gset;
  auto no_tm = false;
  for (const auto& v : vars) {
    if (!v.is_coord && v.dimids.size() >= 3 && (
        is_zonal_mean_grid_variable(v, timedimid, levdimid, latdimid) ||
        is_regular_lat_lon_grid_variable(v, timedimid, levdimid, latdimid,
        londimid) || is_polar_stereographic_grid_variable(v, timedimid,
        levdimid, latdimid))) {
      auto tm = gridded_time_method(v, timeid);
      if (tm.empty()) {
        no_tm = true;
      } else {
        string e;
        gkey = key_start + metautils::NcTime::
            gridded_netcdf_time_range_description(tre, time_data, tm, e);
        if (!e.empty()) {
          log_error2(e, F, "nc2xml", USER);
        }
        if (gset.find(gkey) == gset.end()) {
          gentry_keys.emplace_back(gkey);
          gset.emplace(gkey);
        }
      }
    }
  }
  if (no_tm) {
    string e;
    gkey = key_start + metautils::NcTime::gridded_netcdf_time_range_description(
        tre, time_data, "", e);
    if (!e.empty()) {
      log_error2(e, F, "nc2xml", USER);
    }
    gentry_keys.emplace_back(gkey);
  }
}

void add_gridded_lat_lon_keys(vector<string>& gentry_keys, Grid::GridDimensions
    dim, Grid::GridDefinition def, string timeid, size_t timedimid, int
    levdimid, size_t latdimid, size_t londimid, const metautils::NcTime::
    TimeRangeEntry& tre) {
  string k;
  switch (def.type) {
    case Grid::Type::latitudeLongitude:
    case Grid::Type::gaussianLatitudeLongitude: {
      k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          elatitude, 3) + "<!>" + ftos(def.elongitude, 3) + "<!>" + ftos(def.
          loincrement, 3) + "<!>" + ftos(def.laincrement, 3) + "<!>";
      add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid,
          latdimid, londimid, tre, vars);
      k = itos(static_cast<int>(def.type)) + "<!>1<!>" + itos(dim.y) + "<!>" +
          ftos(def.slatitude, 3) + "<!>0<!>" + ftos(def.elatitude, 3) +
          "<!>360<!>" + ftos(def.laincrement, 3) + "<!>" + ftos(def.laincrement,
          3);
      break;
    }
    case Grid::Type::polarStereographic: {
      k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          llatitude, 3) + "<!>" + ftos(def.olongitude, 3) + "<!>" + ftos(def.dx,
          3) +"<!>" + ftos(def.dy, 3) + "<!>";
      if (def.projection_flag == 0) {
        k += "N";
      } else {
        k += "S";
      }
      break;
    }
    case Grid::Type::mercator: {
      k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          elatitude, 3) + "<!>" + ftos(def.elongitude, 3) + "<!>" + ftos(def.
          loincrement, 3) + "<!>" + ftos(def.laincrement, 3);
      break;
    }
    case Grid::Type::lambertConformal: {
      k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          llatitude, 3) + "<!>" + ftos(def.olongitude, 3) + "<!>" + ftos(def.dx,
          3) +"<!>" + ftos(def.dy, 3) + "<!>";
      if (def.projection_flag == 0) {
        k += "N";
      } else {
        k += "S";
      }
      k += "<!>" + ftos(def.stdparallel1, 3) + "<!>" + ftos(def.stdparallel2,
          3);
      break;
    }
    default: {
      return;
    }
  }
  k += "<!>";
  add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid, latdimid,
      londimid, tre, vars);
}

void add_gridded_zonal_mean_keys(vector<string>& gentry_keys, Grid::
    GridDimensions dim, Grid::GridDefinition def, string timeid, size_t
    timedimid, int levdimid, size_t latdimid, size_t londimid, metautils::
    NcTime::TimeRangeEntry &tre) {
  string k = "1<!>1<!>" + itos(dim.y) + "<!>" + ftos(def.slatitude, 3) +
      "<!>0<!>" + ftos(def.elatitude, 3) + "<!>360<!>" + ftos(def.laincrement,
      3) + "<!>" + ftos(def.laincrement , 3) + "<!>";
  add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid, latdimid,
      londimid, tre, vars);
}

bool found_netcdf_time_from_patch(const NetCDF::Variable& var) {

  // ds260.1
  auto stat = 0;
  for (const auto& a : var.attrs) {
    if (a.nc_type == NetCDF::NCType::CHAR) {
      if (a.name == "units") {
        time_data.units = *(reinterpret_cast<string *>(a.values)) +
            "s";
        ++stat;
      } else if (a.name == "comment") {
        auto s = *(reinterpret_cast<string *>(a.values));
        auto sp=split(s);
        auto dt = stoll(sp[sp.size() - 1]) * 10000000000 + 100000000 + 1000000;
        time_data.reference.set(dt);
        ++stat;
      }
    }
    if (stat == 2) {
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

bool ignore_cf_variable(const NetCDF::Variable& var) {
  auto l = to_lower(var.name);
  if (l == "time" || l == "time_bounds" || l == "year" || l == "month" || l ==
      "day" || l == "doy" || l == "hour" || l == "latitude" || l ==
      "longitude") {
    return true;
  }
  for (const auto& a : var.attrs) {
    if (a.name == "cf_role") {
      return true;
    }
  }
  return false;
}

struct DiscreteGeometriesData {
  DiscreteGeometriesData() : indexes(), lengths(), z_units(), z_pos() { }

  struct Indexes {
    Indexes() : time_var(MISSING_FLAG), time_bounds_var(MISSING_FLAG),
        stn_id_var(MISSING_FLAG), network_var(MISSING_FLAG), platform_var(
        MISSING_FLAG), lat_var(MISSING_FLAG), lat_var_bounds(MISSING_FLAG),
        lon_var(MISSING_FLAG), lon_var_bounds(MISSING_FLAG), sample_dim_var(
        MISSING_FLAG), instance_dim_var(MISSING_FLAG), z_var(MISSING_FLAG) { }

    size_t time_var, time_bounds_var, stn_id_var, network_var, platform_var,
        lat_var, lat_var_bounds, lon_var, lon_var_bounds, sample_dim_var,
        instance_dim_var, z_var;
  };

  struct Lengths {
    Lengths() : stn_id_var(MISSING_FLAG), network_var(MISSING_FLAG),
        platform_var(MISSING_FLAG) { }

    size_t stn_id_var, network_var, platform_var;
  };

  Indexes indexes;
  Lengths lengths;
  string z_units, z_pos;
};

void process_units_attribute(size_t var_index, size_t attr_index,
    DiscreteGeometriesData& dgd) {
  static const string F = this_function_label(__func__);
  auto u = *(reinterpret_cast<string *>(vars[var_index].attrs[attr_index].
      values));
  u = to_lower(u);
  if (regex_search(u, regex("since"))) {
    if (dgd.indexes.time_var != MISSING_FLAG) {
      log_error2("time was already identified - don't know what to do with "
          "variable: " + vars[var_index].name, F, "nc2xml", USER);
    }
    fill_nc_time_data(vars[var_index].attrs[attr_index].to_string());
    dgd.indexes.time_var = var_index;
    non_data_vars.emplace(vars[var_index].name);
  } else if (regex_search(u, regex("^degree(s){0,1}(_){0,1}((north)|N)$"))) {
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      dgd.indexes.lat_var = var_index;
      non_data_vars.emplace(vars[var_index].name);
    } else {
      for (const auto& a : vars[var_index].attrs) {
        if (regex_search(a.name, regex("bounds", std::regex_constants::
            icase))) {
          auto v = *(reinterpret_cast<string *>(a.values));
          if (v == vars[dgd.indexes.lat_var].name) {
            dgd.indexes.lat_var = var_index;
            non_data_vars.emplace(vars[var_index].name);
          }
        }
      }
    }
  } else if (regex_search(u, regex("^degree(s){0,1}(_){0,1}((east)|E)$"))) {
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      dgd.indexes.lon_var = var_index;
      non_data_vars.emplace(vars[var_index].name);
    } else {
      for (const auto& a : vars[var_index].attrs) {
        if (regex_search(a.name, regex("bounds", std::regex_constants::
            icase))) {
          auto v = *(reinterpret_cast<string *>(a.values));
          if (v == vars[dgd.indexes.lon_var].name) {
            dgd.indexes.lon_var = var_index;
            non_data_vars.emplace(vars[var_index].name);
          }
        }
      }
    }
  }
}

void process_variable_attributes(DiscreteGeometriesData& dgd) {
  static const string F = this_function_label(__func__);
  for (size_t n = 0; n < vars.size(); ++n) {
    for (size_t m = 0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].nc_type == NetCDF::NCType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(n, m, dgd);
        } else if (vars[n].attrs[m].name == "cf_role") {
          auto r = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
          r = to_lower(r);
          if (r == "timeseries_id" || r == "trajectory_id" || r ==
              "profile_id") {
            if (dgd.indexes.stn_id_var != MISSING_FLAG) {
              log_error2("station ID was already identified - don't know what "
                  "to do with variable: " + vars[n].name, F, "nc2xml", USER);
            }
            dgd.indexes.stn_id_var = n;
            dgd.lengths.stn_id_var = dims[vars[n].dimids.back()].length;
            non_data_vars.emplace(vars[n].name);
          }
        } else if (vars[n].attrs[m].name == "sample_dimension") {
          dgd.indexes.sample_dim_var = n;
          non_data_vars.emplace(vars[n].name);
        } else if (vars[n].attrs[m].name == "instance_dimension") {
          dgd.indexes.instance_dim_var = n;
          non_data_vars.emplace(vars[n].name);
        } else if (vars[n].attrs[m].name == "axis") {
          auto axis = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
          if (axis == "Z") {
            dgd.indexes.z_var = n;
          }
        }
      }
    }
  }
  if (dgd.indexes.time_var == MISSING_FLAG) {
    log_error2("unable to determine time variable", F, "nc2xml", USER);
  }
  for (const auto& a : vars[dgd.indexes.time_var].attrs) {
    if (a.name == "calendar") {
      time_data.calendar = *(reinterpret_cast<string *>(a.values));
    } else if (a.name == "bounds") {
      auto s = *(reinterpret_cast<string *>(a.values));
      for (size_t n = 0; n < vars.size(); ++n) {
        if (vars[n].name == s && dims[vars[n].dimids.back()].length == 2) {
          dgd.indexes.time_bounds_var = n;
          non_data_vars.emplace(vars[n].name);
          break;
        }
      }
    }
  }
  if (dgd.indexes.stn_id_var != MISSING_FLAG) {
    for (const auto& a : vars[dgd.indexes.stn_id_var].attrs) {
      if (a.name == "network") {
        auto s = *(reinterpret_cast<string *>(a.values));
        for (size_t n = 0; n < vars.size(); ++n) {
          if (vars[n].name == s) {
            dgd.indexes.network_var = n;
            dgd.lengths.network_var = dims[vars[n].dimids.back()].length;
            non_data_vars.emplace(vars[n].name);
            break;
          }
        }
      } else if (a.name == "platform") {
        auto s = *(reinterpret_cast<string *>(a.values));
        for (size_t n = 0; n < vars.size(); ++n) {
          if (vars[n].name == s) {
            dgd.indexes.platform_var = n;
            dgd.lengths.platform_var = dims[vars[n].dimids.back()].length;
            non_data_vars.emplace(vars[n].name);
            break;
          }
        }
      }
    }
  }
}

void process_vertical_coordinate_variable(DiscreteGeometriesData& dgd, string&
    obs_type) {
  static const string F = this_function_label(__func__);
  obs_type = "";
  for (const auto& a : vars[dgd.indexes.z_var].attrs) {
    if (a.name == "units") {
      dgd.z_units = *(reinterpret_cast<string *>(a.values));
      trim(dgd.z_units);
    } else if (a.name == "positive") {
      dgd.z_pos = *(reinterpret_cast<string *>(a.values));
      trim(dgd.z_pos);
      dgd.z_pos = to_lower(dgd.z_pos);
    }
  }
  if (dgd.z_pos.empty() && !dgd.z_units.empty()) {
    auto l = to_lower(dgd.z_units);
    if (regex_search(dgd.z_units, regex("Pa$")) || regex_search(l, regex(
        "^mb(ar){0,1}$")) || l == "millibars") {
      dgd.z_pos = "down";
      obs_type = "upper_air";
    }
  }
  if (dgd.z_pos.empty()) {
    log_error2("unable to determine vertical coordinate direction", F, "nc2xml",
        USER);
  }
  if (dgd.z_pos == "up") {
    obs_type = "upper_air";
  } else if (dgd.z_pos == "down") {
    auto l = to_lower(dgd.z_units);
    if (dgd.z_pos == "down" && (regex_search(dgd.z_units, regex("Pa$")) ||
        regex_search(l, regex("^mb(ar){0,1}$")) || l == "millibars")) {
      obs_type = "upper_air";
    }
  }
}

void fill_coordinate_data(InputNetCDFStream& istream, const
    DiscreteGeometriesData& dgd, NetCDF::VariableData *time_data, unique_ptr<
    NetCDF::VariableData> *bounds_data, NetCDF::VariableData *y_data, NetCDF::
    VariableData *x_data, unique_ptr<NetCDF::VariableData> *id_data, unique_ptr<
    NetCDF::VariableData> *network_data, unique_ptr<NetCDF::VariableData>
    *platform_data, string F) {
  if (time_data->type() == NetCDF::NCType::_NULL && istream.variable_data(
      vars[dgd.indexes.time_var].name, *time_data) == NetCDF::NCType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (bounds_data != nullptr && dgd.indexes.time_bounds_var != MISSING_FLAG) {
    bounds_data->reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.time_bounds_var].name,
        **bounds_data) == NetCDF::NCType::_NULL) {
      log_error2("unable to get time bounds data", F, "nc2xml", USER);
    }
  }
  if (dgd.indexes.lat_var_bounds == MISSING_FLAG) {
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      log_error2("unable to determine latitude variable", F, "nc2xml", USER);
    }
    if (y_data->type() == NetCDF::NCType::_NULL && istream.variable_data(vars[
        dgd.indexes.lat_var].name, *y_data) == NetCDF::NCType::_NULL) {
      log_error2("unable to get latitude data", F, "nc2xml", USER);
    }
  } else if (y_data->type() == NetCDF::NCType::_NULL && istream.variable_data(
      vars[dgd.indexes.lat_var_bounds].name, *y_data) == NetCDF::NCType::
     _NULL) {
    log_error2("unable to get latitude bounds data", F, "nc2xml", USER);
  }
  if (dgd.indexes.lon_var_bounds == MISSING_FLAG) {
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      log_error2("unable to determine longitude variable", F, "nc2xml", USER);
    }
    if (x_data->type() == NetCDF::NCType::_NULL && istream.variable_data(vars[
        dgd.indexes.lon_var].name, *x_data) == NetCDF::NCType::_NULL) {
      log_error2("unable to get longitude data", F, "nc2xml", USER);
    }
  } else if (x_data->type() == NetCDF::NCType::_NULL && istream.variable_data(
      vars[dgd.indexes.lon_var_bounds].name, *x_data) == NetCDF::NCType::
     _NULL) {
    log_error2("unable to get longitude bounds data", F, "nc2xml", USER);
  }
  if (id_data != nullptr && dgd.indexes.stn_id_var != MISSING_FLAG) {
    id_data->reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.stn_id_var].name, **id_data) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get station ID data", F, "nc2xml", USER);
    }
  }
  if (network_data != nullptr && dgd.indexes.network_var != MISSING_FLAG) {
    network_data->reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.network_var].name,
        **network_data) == NetCDF::NCType::_NULL) {
      log_error2("unable to get station network data", F, "nc2xml", USER);
    }
  }
  if (platform_data != nullptr && dgd.indexes.platform_var != MISSING_FLAG) {
    platform_data->reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.platform_var].name,
        **platform_data) == NetCDF::NCType::_NULL) {
      log_error2("unable to get station platform data", F, "nc2xml", USER);
    }
  }
}

void add_to_netcdf_variables(string s, ScanData& scan_data) {
  auto d = scan_data.datatype_map.description(s.substr(0, s.find("<!>")));
  if (d.empty()) {
    if (find(scan_data.netcdf_variables.begin(), scan_data.netcdf_variables.
        end(), s) == scan_data.netcdf_variables.end()) {
      scan_data.netcdf_variables.emplace_back(s);
    }
  }
}

bool not_missing_location(double lat, double lon) {
  if (lat < -90. || lat > 90. || lon < -180. || lon > 180.) {
    return false;
  }
  return true;
}

/*
tuple<vector<string>, vector<string>, vector<string>> platforms_and_ids(NetCDF::
    VariableData& id_data, string F) {
  vector<string> p, t, i;
  if (id_data.type() == NetCDF::NCType::INT || id_data.type() == NetCDF::
      DataType::FLOAT || id_data.type() == NetCDF::NCType::DOUBLE) {
    for (size_t n = 0; n < id_data.size(); ++n) {
      i.emplace_back(ftos(id_data[n]));
      p.emplace_back("unknown");
      t.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, p[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + p[n], F,
            "nc2xml", USER);
      }
    }
  } else if (id_data.type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *id = reinterpret_cast<char *>(id_data.get());
    for (size_t n = 0; n < id_data.size() / ilen; ++n) {
      i.emplace_back(&id[n * ilen], ilen);
      p.emplace_back("unknown");
      t.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, p[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + p[n], F,
            "nc2xml", USER);
      }
    }
  } else {
    log_error2("unable to determine platform type", F, "nc2xml", USER);
  }
  return make_tuple(p, t, i);
}
*/

void scan_cf_point_netcdf_file(InputNetCDFStream& istream, string platform_type,
    const DiscreteGeometriesData& dgd, ScanData& scan_data, gatherxml::markup::
    ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  NetCDF::VariableData tvd, yvd, xvd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, nullptr,
      nullptr, nullptr, F);
  vector<DateTime> dtv;
  vector<string> idv;
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.time_var].name && v.name != vars[dgd.indexes.
        lat_var].name && v.name != vars[dgd.indexes.lon_var].name) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get variable data for '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
          conventions);
      add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units +
          "<!>" + ad.cf_keyword, scan_data);
      for (size_t n = 0; n < tvd.size(); ++n) {
        if (n == dtv.size()) {
          dtv.emplace_back(compute_nc_time(tvd, n));
        }
        if (n == idv.size()) {
          auto lat = ftos(fabs(yvd[n]), 4);
          if (yvd[n] < 0.) {
            lat += "S";
          } else {
            lat += "N";
          }
          auto lon = ftos(fabs(xvd[n]), 4);
          if (xvd[n] < 0.) {
            lon += "W";
          } else {
            lon += "E";
          }
          idv.emplace_back(lat + lon);
        }
        if (!found_missing(tvd[n], nullptr, vd[n], ad.missing_value)) {
          if (!obs_data.added_to_platforms("surface", platform_type, yvd[n],
              xvd[n])) {
            auto e = move(myerror);
            log_error2(e + "' when adding platform " + platform_type, F,
                "nc2xml", USER);
          }
          ientry.key = platform_type + "[!]latlon[!]" + idv[n];
          if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[n], xvd[
              n], tvd[n], &dtv[n])) {
            auto e = move(myerror);
            log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml", USER);
          }
          ++scan_data.num_not_missing;
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_orthogonal_time_series_netcdf_file(InputNetCDFStream& istream,
    string platform_type, const DiscreteGeometriesData& dgd, unordered_map<
    size_t, string>& T_map, ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "ObML",
      "nc2xml", USER);
  if (g_inv.stream.is_open()) {
    g_inv.stream << "netCDF:timeSeries|" << istream.record_size() << endl;
    g_inv.maps.O.emplace("surface", make_pair(g_inv.maps.O.size(), ""));
  }
  NetCDF::VariableData yvd, xvd;
  unique_ptr<NetCDF::VariableData> ivd;
  NetCDF::NCType ityp = NetCDF::NCType::_NULL;
  size_t ilen = 0;
  vector<string> pfms, ityps, ids;
  if (dgd.indexes.lat_var == MISSING_FLAG || dgd.indexes.lon_var == MISSING_FLAG
      || dgd.indexes.stn_id_var == MISSING_FLAG) {

    // lat/lon not found, look for known alternates in global attributes
    size_t src = MISSING_FLAG;
    for (size_t n = 0; n < gattrs.size(); ++n) {
      if (to_lower(gattrs[n].name) == "title") {
        if (to_lower(*(reinterpret_cast<string *>(gattrs[n].values))) ==
            "hadisd") {
          src = 0x1;
          break;
        }
      }
    }
    if (src == 0x1) {

      // HadISD
      for (size_t n = 0; n < gattrs.size(); ++n) {
        if (gattrs[n].name == "latitude") {
          yvd.resize(1, NetCDF::NCType::FLOAT);
          yvd.set(0, *(reinterpret_cast<float *>(gattrs[n].values)));
        } else if (gattrs[n].name == "longitude") {
          xvd.resize(1, NetCDF::NCType::FLOAT);
          xvd.set(0, *(reinterpret_cast<float *>(gattrs[n].values)));
        } else if (gattrs[n].name == "station_id") {
          auto id = *(reinterpret_cast<string *>(gattrs[n].values));
          id = id.substr(0, id.find("-"));
          ids.emplace_back(id);
          ilen = id.length();
          ityp = NetCDF::NCType::CHAR;
          ivd.reset(new NetCDF::VariableData);
          ivd->resize(ilen, ityp);
          for (size_t m = 0; m < ilen; ++m) {
            ivd->set(m, id[m]);
          }
          ityps.emplace_back("WMO+6");
          if (id >= "990000" && id < "991000") {
            pfms.emplace_back("fixed_ship");
          } else if ((id >= "992000" && id < "993000") || (id >= "995000" && id
              < "998000")) {
            pfms.emplace_back("drifting_buoy");
          } else {
            pfms.emplace_back("land_station");
          }
        }
      }
      if (!obs_data.added_to_platforms("surface", pfms.back(), yvd[0], xvd[
          0])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + pfms.back(), F, "nc2xml",
            USER);
      }
      if (g_inv.stream.is_open()) {
        auto key = ityps.back() + "[!]" + ids.back();
        if (g_inv.maps.I.find(key) == g_inv.maps.I.end()) {
            g_inv.maps.I.emplace(key, make_pair(g_inv.maps.I.size(), ftos(yvd.back(), 4) +
                "[!]" + ftos(xvd.back(), 4)));
        }
      }
    }
  }
  if (dgd.indexes.lat_var == MISSING_FLAG || dgd.indexes.lon_var == MISSING_FLAG
      || dgd.indexes.stn_id_var == MISSING_FLAG) {
    string e;
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      if (!e.empty()) {
        e += ", ";
      }
      e += "latitude could not be identified";
    }
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      if (!e.empty()) {
        e += ", ";
      }
      e += "longitude could not be identified";
    }
    if (dgd.indexes.stn_id_var == MISSING_FLAG) {
      if (!e.empty()) {
        e += ", ";
      }
      e += "timeseries_id role could not be identified";
    }
    log_error2(e, F, "nc2xml", USER);
  }
  NetCDF::VariableData tvd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, &ivd, nullptr,
      nullptr, F);
  if (ityp == NetCDF::NCType::CHAR && dgd.indexes.stn_id_var !=
      MISSING_FLAG) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  auto sd = -1;
  size_t ns = 0;
  if (dgd.indexes.stn_id_var != MISSING_FLAG) {
    if (vars[dgd.indexes.stn_id_var].dimids.size() >= 1) {
      sd = vars[dgd.indexes.stn_id_var].dimids.front();
      if (vars[dgd.indexes.stn_id_var].is_rec) {
        ns = istream.num_records();
      } else {
        ns = dims[sd].length;
      }
    } else {
      ns = 1;
    }
  }
  if (pfms.empty()) {
    string id;
    for (size_t n = 0; n < ns; ++n) {
      if (ityp == NetCDF::NCType::SHORT || ityp == NetCDF::NCType::INT ||
          ityp == NetCDF::NCType::FLOAT || ityp == NetCDF::NCType::DOUBLE) {
        id = ftos((*ivd)[n]);
      } else if (ityp == NetCDF::NCType::CHAR) {
        id.assign(&(reinterpret_cast<char *>(ivd.get()))[n * ilen], ilen);
      } else {
        log_error2("unable to determine platform type", F, "nc2xml", USER);
      }
      pfms.emplace_back(platform_type);
      ityps.emplace_back("unknown");
      ids.emplace_back(id);
      if (!obs_data.added_to_platforms("surface", pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + pfms[n], F, "nc2xml", USER);
      }
      if (g_inv.stream.is_open()) {
        auto key = ityps[n] + "[!]" + ids[n];
        if (g_inv.maps.I.find(key) == g_inv.maps.I.end()) {
          g_inv.maps.I.emplace(key, make_pair(g_inv.maps.I.size(), ftos(yvd[n],
              4) + "[!]" + ftos(xvd[n], 4)));
        }
      }
    }
  }
  if (g_inv.stream.is_open()) {
    for (const auto& p : pfms) {
      if (g_inv.maps.P.find(p) == g_inv.maps.P.end()) {
        g_inv.maps.P.emplace(p, make_pair(g_inv.maps.P.size(), " "));
      }
    }
  }
  vector<DateTime> dtv;
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.time_var].name && !v.dimids.empty() && ((v.
        dimids[0] == vars[dgd.indexes.time_var].dimids[0] && (sd == -1 || (v.
        dimids.size() > 1 && static_cast<int>(v.dimids[1]) == sd))) || (v.
        dimids.size() > 1 && dgd.indexes.stn_id_var != MISSING_FLAG && v.dimids[
        0] == vars[dgd.indexes.stn_id_var].dimids[0] && v.dimids[1] == vars[dgd.
        indexes.time_var].dimids[0]))) {
      if (gatherxml::verbose_operation) {
        cout << "Scanning netCDF variable '" << v.name << "' ..." << endl;
      }
      if (g_inv.stream.is_open()) {
        if (g_inv.maps.D.find(v.name) == g_inv.maps.D.end()) {
          auto byts = 1;
          for (size_t l = 1; l < v.dimids.size(); ++l) {
            byts *= dims[v.dimids[l]].length;
          }
          switch (v.nc_type) {
            case NetCDF::NCType::SHORT: {
              byts *= 2;
              break;
            }
            case NetCDF::NCType::INT:
            case NetCDF::NCType::FLOAT: {
              byts *= 4;
              break;
            }
            case NetCDF::NCType::DOUBLE: {
              byts *= 8;
              break;
            }
            default: { }
          }
          g_inv.maps.D.emplace(v.name, make_pair(g_inv.maps.D.size(), "|" +
              lltos(v.offset) + "|" + NetCDF::nc_type_str[static_cast<int>(v.
              nc_type)] + "|" + itos(byts)));
        }
      }
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
          conventions);
      add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units +
          "<!>" + ad.cf_keyword, scan_data);
      size_t nt;
      if (dims[vars[dgd.indexes.time_var].dimids[0]].is_rec) {
        nt = istream.num_records();
      } else {
        nt = dims[vars[dgd.indexes.time_var].dimids[0]].length;
      }
      for (size_t n = 0; n < ns; ++n) {
        vector<string> mlst;
        ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]" + ids[n];
        for (size_t m = 0; m < nt; ++m) {
          if (dtv.size() != nt) {
            dtv.emplace_back(compute_nc_time(tvd, m));
          }
          auto x = dims[vars[dgd.indexes.time_var].dimids[0]].is_rec ? n + m *
              ns : n * nt + m;
          if (!found_missing(tvd[m], nullptr, vd[x], ad.missing_value)) {
            if (g_inv.stream.is_open()) {
              if (T_map.find(m) == T_map.end()) {
                T_map.emplace(m, dtv[m].to_string("%Y%m%d%H%MM"));
              }
            }
            if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[n],
                xvd[n], tvd[m], &dtv[m])) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            ++scan_data.num_not_missing;
          } else {
            if (g_inv.stream.is_open()) {
              string s = itos(m);
              s += "|0|" + itos(g_inv.maps.P[pfms[n]].first) + "|" + itos(g_inv.
                  maps.I[ityps[n] + "[!]" + ids[n]].first) + "|" + itos(g_inv.
                  maps.D[v.name].first);
              mlst.emplace_back(s);
            }
          }
        }
        if (g_inv.stream.is_open()) {
          if (mlst.size() != tvd.size()) {
            for (const auto& l : mlst) {
              inv_lines2.writeln(l);
            }
          } else {
            g_inv.maps.D.erase(v.name);
          }
        }
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_non_orthogonal_time_series_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, unordered_map<size_t,
    string>& T_map, ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  NetCDF::VariableData tvd, yvd, xvd;
  unique_ptr<NetCDF::VariableData> bvd, ivd, nvd, pvd;
  fill_coordinate_data(istream, dgd, &tvd, &bvd, &yvd, &xvd, &ivd, &nvd, &pvd,
      F);
  auto sd = vars[dgd.indexes.stn_id_var].dimids[0];
  size_t ns;
  if (vars[dgd.indexes.stn_id_var].is_rec) {
    ns = istream.num_records();
  } else {
    ns = dims[sd].length;
  }
  size_t ilen = 0;
  vector<string> pfms, ityps;
  size_t nl = 1;
  if (dgd.indexes.lat_var_bounds != MISSING_FLAG && dgd.indexes.lon_var_bounds
      != MISSING_FLAG) {
    nl = 2;
  }
  if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::NCType::
      FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
    for (size_t n = 0; n < ns; ++n) {
      pfms.emplace_back(platform_type);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        ityps.emplace_back(itos((*nvd)[n]));
      } else {
        ityps.emplace_back("unknown");
      }
      for (size_t m = 0; m < nl; ++m) {
        if (!obs_data.added_to_platforms("surface", pfms[n], yvd[n * nl + m],
            xvd[n * nl + m])) {
          auto e = move(myerror);
          log_error2(e + "' when adding platform " + pfms[n], F, "nc2xml",
              USER);
        }
      }
    }
  } else if (ivd->type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd->get());
    auto nc = nvd->size() / ns;
    for (size_t n = 0; n < ns; ++n) {
      string id(&i[n * ilen], ilen);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        ityps.emplace_back(string(&(reinterpret_cast<char *>(nvd->get()))[n *
            nc], nc));
        if (ityps.back() == "WMO") {
          if (id.length() == 5) {
            if (id > "01000" && id < "99000") {
              pfms.emplace_back("land_station");
            } else if (id > "98999") {
              pfms.emplace_back("roving_ship");
            } else {
              pfms.emplace_back("unknown");
              log_warning("ID '" + id + "' does not appear to be a WMO ID",
                  "nc2xml", USER);
            }
          } else {
            pfms.emplace_back("unknown");
            log_warning("ID '" + id + "' does not appear to be a WMO ID",
                "nc2xml", USER);
          }
        } else if (ityps.back() == "WMO+6" && id.length() == 6) {
            if (id > "010000" && id < "990000") {
              pfms.emplace_back("land_station");
            } else {
              pfms.emplace_back("unknown");
              log_warning("ID '" + id + "' does not appear to be a WMO+6 ID",
                  "nc2xml", USER);
            }
        } else if (id_platform_map.find(ityps.back()) != id_platform_map.
            end()) {
          pfms.emplace_back(id_platform_map[ityps.back()]);
        } else {
          pfms.emplace_back("unknown");
        }
      } else {
        ityps.emplace_back("unknown");
        pfms.emplace_back("unknown");
      }
      for (size_t m = 0; m < nl; ++m) {
        if (!obs_data.added_to_platforms("surface", pfms[n], yvd[n * nl + m],
            xvd[n * nl + m])) {
          auto e = move(myerror);
          log_error2(e + "' when adding platform " + pfms[n], F, "nc2xml",
              USER);
        }
      }
    }
  } else {
    log_error2("unable to determine platform type", F, "nc2xml", USER);
  }
  auto od = vars[dgd.indexes.time_var].dimids[0] == sd ? vars[dgd.indexes.
      time_var].dimids[1] : vars[dgd.indexes.time_var].dimids[0];
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {

    // continuous ragged array H.6
    if (gatherxml::verbose_operation) {
      cout << "   ...continuous ragged array" << endl;
    }
    NetCDF::VariableData rvd;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name, rvd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get sample dimension data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.time_var].name && v.dimids.size() == 1 &&
          v.dimids[0] == od) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        long long off = 0;
        for (size_t n = 0; n < dims[sd].length; ++n) {
          auto end = off + rvd[n];
          ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
          if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
              NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
            ientry.key += ftos((*ivd)[n]);
          } else if (ivd->type() == NetCDF::NCType::CHAR) {
            auto id = string(&(reinterpret_cast<char *>(ivd->get()))[n * ilen],
                ilen);
            trim(id);
            ientry.key += id;
          }
          for (size_t m = off; m < end; ++m) {
            if (!found_missing(tvd[m], nullptr, vd[m], ad.missing_value)) {
              auto dt = compute_nc_time(tvd, m);
              for (size_t l = 0; l < nl; ++l) {
                if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[n
                    * nl + l], xvd[n * nl + l], tvd[m], &dt)) {
                  auto e = move(myerror);
                  log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                      USER);
                }
              }
              ++scan_data.num_not_missing;
            }
          }
          off = end;
        }
      }
    }
  } else if (dgd.indexes.instance_dim_var != MISSING_FLAG) {

    // indexed ragged array H.7
    if (gatherxml::verbose_operation) {
      cout << "   ...indexed ragged array" << endl;
    }
    NetCDF::VariableData svd;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, svd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get instance dimension data", F, "nc2xml", USER);
    }
    vector<DateTime> dtn, dtx;
    if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
      for (size_t n = 0; n < svd.size(); ++n) {
        dtn.emplace_back(compute_nc_time(*bvd, n * 2));
        dtx.emplace_back(compute_nc_time(*bvd, n * 2 + 1));
      }
    }
    unordered_set<string> noset{ vars[dgd.indexes.time_var].name, vars[dgd.
        indexes.instance_dim_var].name };
    if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
      noset.emplace(vars[dgd.indexes.time_bounds_var].name);
    }
    for (const auto& v : vars) {
      if (noset.find(v.name) == noset.end() && v.dimids.size() >= 1 && v.dimids[
          0] == od) {
        if (gatherxml::verbose_operation) {
          cout << "   ...scanning netCDF variable '" << v.name << "' ..." <<
              endl;
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        for (size_t n = 0; n < svd.size(); ++n) {
          size_t idx = svd[n];
          ientry.key = pfms[idx] + "[!]" + ityps[idx] + "[!]";
          if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
              NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
            ientry.key += ftos((*ivd)[idx]);
          } else if (ivd->type() == NetCDF::NCType::CHAR) {
            auto id=string(&(reinterpret_cast<char *>(ivd->get()))[idx * ilen],
                ilen);
            trim(id);
            ientry.key += id;
          }
          auto fv = v._FillValue.get();
          for (const auto& value : istream.value_at(v.name, n)) {
            if (value != fv) {
              fv = value;
              break;
            }
          }
          if (!found_missing(tvd[n], nullptr, fv, ad.missing_value)) {
            if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
              for (size_t m = 0; m < nl; ++m) {
                auto x = idx * nl + m;
                if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[
                    x], xvd[x], tvd[n], &dtn[n], &dtx[n])) {
                  auto e = move(myerror);
                  log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
                }
              }
            } else {
              auto dt = compute_nc_time(tvd, n);
              for (size_t m = 0; m < nl; ++m) {
                auto x = idx * nl + m;
                if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[
                    x], xvd[x], tvd[n], &dt)) {
                  auto e = move(myerror);
                  log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
                }
              }
            }
            ++scan_data.num_not_missing;
          }
        }
        if (gatherxml::verbose_operation) {
          cout << "   ...scanning netCDF variable '" << v.name << "' done." <<
              endl;
        }
      }
    }
  } else {

    // incomplete multidimensional array H.3
    if (gatherxml::verbose_operation) {
      cout << "   ...incomplete multidimensional array" << endl;
    }
    NetCDFVariableAttributeData td;
    extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs, vars[dgd.
        indexes.time_var].nc_type, td, scan_data.conventions);
    size_t nobs;
    if (vars[dgd.indexes.stn_id_var].is_rec) {
      nobs = dims[od].length;
    } else {
      nobs = istream.num_records();
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.time_var].name && v.dimids.size() == 2 &&
          ((v.dimids[0] == sd && v.dimids[1] == od) || (v.dimids[0] == od && v.
          dimids[1] == sd))) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        if (v.dimids.front() == sd) {
          for (size_t n = 0; n < ns; ++n) {
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
                NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
              ientry.key += ftos((*ivd)[n]);
            } else if (ivd->type() == NetCDF::NCType::CHAR) {
              auto id = string(&(reinterpret_cast<char *>(ivd->get()))[n *
                  ilen], ilen);
              trim(id);
              ientry.key += id;
            }
            for (size_t m = 0; m < nobs; ++m) {
              auto idx = n * nobs + m;
              if (!found_missing(tvd[idx], &td.missing_value, vd[idx], ad.
                  missing_value)) {
                auto dt = compute_nc_time(tvd, idx);
                for (size_t l = 0; l < nl; ++l) {
                  auto x = n * nl + l;
                  if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[
                      x], xvd[x], tvd[idx], &dt)) {
                    auto e = move(myerror);
                    log_error2(e + "' when adding ID " + ientry.key, F,
                        "nc2xml", USER);
                  }
                }
                ++scan_data.num_not_missing;
              }
            }
          }
        } else {
          for (size_t n = 0; n < nobs; ++n) {
            for (size_t m = 0; m < ns; ++m) {
              ientry.key = pfms[m] + "[!]" + ityps[m] + "[!]";
              if (ivd->type() == NetCDF::NCType::INT || ivd->type() ==
                  NetCDF::NCType::FLOAT || ivd->type() == NetCDF::NCType::
                  DOUBLE) {
                ientry.key += ftos((*ivd)[m]);
              } else if (ivd->type() == NetCDF::NCType::CHAR) {
                auto id = string(&(reinterpret_cast<char *>(ivd->get()))[m *
                    ilen], ilen);
                trim(id);
                ientry.key += id;
              }
              auto idx = n * ns + m;
              if (!found_missing(tvd[idx], &td.missing_value, vd[idx], ad.
                  missing_value)) {
                auto dt = compute_nc_time(tvd, idx);
                for (size_t l = 0; l < nl; ++l) {
                  auto x = m * nl + l;
                  if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[
                      x], xvd[x], tvd[idx], &dt)) {
                    auto e = move(myerror);
                    log_error2(e + "' when adding ID " + ientry.key, F,
                        "nc2xml", USER);
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
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_time_series_netcdf_file(InputNetCDFStream& istream, string
     platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  unordered_map<size_t, string> T_map;
  if (vars[dgd.indexes.time_var].is_coord) {

    // ex. H.2, H.4 (single version of H.2), H.5 (precise locations) stns w/same
    // times
    scan_cf_orthogonal_time_series_netcdf_file(istream, platform_type, dgd,
        T_map, scan_data, obs_data);
  } else {

    // ex. H.3 stns w/varying times but same # of obs
    // ex. H.6 w/sample_dimension
    // ex. H.7 w/instance_dimension
    if (dgd.indexes.stn_id_var == MISSING_FLAG) {
      log_error2("unable to determine timeseries_id variable", F, "nc2xml",
          USER);
    }
    for (const auto& a : vars[dgd.indexes.stn_id_var].attrs) {
      if (regex_search(a.name, regex("network", std::regex_constants::icase))) {
        auto s = *(reinterpret_cast<string *>(a.values));
        if (gatherxml::verbose_operation) {
          cout << "   ...found network: '" << s << "'" << endl;
        }
        for (size_t n = 0; n < vars.size(); ++n) {
          if (vars[n].name == s) {
            dgd.indexes.network_var = n;
            break;
          }
        }
        break;
      }
    }
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      log_error2("unable to determine latitude variable", F, "nc2xml", USER);
    }
    for (const auto& a : vars[dgd.indexes.lat_var].attrs) {
      if (regex_search(a.name, regex("bounds", std::regex_constants::icase))) {
        auto s = *(reinterpret_cast<string *>(a.values));
        if (gatherxml::verbose_operation) {
          cout << "   ...found latitude bounds: '" << s << "'" << endl;
        }
        for (size_t n = 0; n < vars.size(); ++n) {
          if (vars[n].name == s) {
            dgd.indexes.lat_var_bounds = n;
            break;
          }
        }
        break;
      }
    }
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      log_error2("unable to determine longitude variable", F, "nc2xml", USER);
    }
    for (const auto& a : vars[dgd.indexes.lon_var].attrs) {
      if (regex_search(a.name, regex("bounds", std::regex_constants::icase))) {
        auto s = *(reinterpret_cast<string *>(a.values));
        if (gatherxml::verbose_operation) {
          cout << "   ...found latitude bounds: '" << s << "'" << endl;
        }
        for (size_t n = 0; n < vars.size(); ++n) {
          if (vars[n].name == s) {
            dgd.indexes.lon_var_bounds = n;
            break;
          }
        }
        break;
      }
    }
    scan_cf_non_orthogonal_time_series_netcdf_file(istream, platform_type, dgd,
        T_map, scan_data, obs_data);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (g_inv.stream.is_open()) {
    vector<size_t> tv;
    for (const auto& e : T_map) {
      tv.emplace_back(e.first);
    }
    sort(tv.begin(), tv.end(), 
    [](const size_t& left, const size_t& right) -> bool {
      if (left <= right) {
        return true;
      }
      return false;
    });
    for (const auto& t : tv) {
      g_inv.stream << "T<!>" << t << "<!>" << T_map[t] << endl;
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_non_orthogonal_trajectory_netcdf_file(InputNetCDFStream& istream,
    DiscreteGeometriesData& dgd, ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  NetCDF::VariableData tvd, yvd, xvd;
  unique_ptr<NetCDF::VariableData> bvd, ivd, nvd, pvd;
  fill_coordinate_data(istream, dgd, &tvd, &bvd, &yvd, &xvd, &ivd, &nvd, &pvd,
      F);
  vector<string> pfms, ityps, ids;
  auto *pc = reinterpret_cast<char *>(pvd->get());
  auto *nc = reinterpret_cast<char *>(nvd->get());
  auto *ic = reinterpret_cast<char *>(ivd->get());
  string pfm = "unknown", ityp = "unknown";
  for (size_t n = 0; n < ivd->size(); ++n) {
    if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
        NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
      ids.emplace_back(ftos((*ivd)[n]));
    } else if (ivd->type() == NetCDF::NCType::CHAR) {
      ids.emplace_back(&ic[n * dgd.lengths.stn_id_var], dgd.lengths.stn_id_var);
    } else {
      log_error2("unable to determine platform type", F, "nc2xml", USER);
    }
    if (nvd->type() == NetCDF::NCType::CHAR) {
      pfm = string(&nc[n * dgd.lengths.network_var], dgd.lengths.network_var);
    }
    if (pvd->type() == NetCDF::NCType::CHAR) {
      ityp = string(&pc[n * dgd.lengths.platform_var], dgd.lengths.
          platform_var);
    }
    pfms.emplace_back(pfm);
    ityps.emplace_back(ityp);
    if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
      auto e = move(myerror);
      log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
          "nc2xml", USER);
    }
  }
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {

    // continuous ragged array H.14
log_error2("continuous ragged array currently not supported", F, "nc2xml", USER);
  }
  else if (dgd.indexes.instance_dim_var != MISSING_FLAG) {

    // indexed ragged array H.15
    NetCDF::VariableData ovd;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, ovd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get instance dimension data", F, "nc2xml", USER);
    }
    auto idim = vars[dgd.indexes.instance_dim_var].dimids.front();
    auto nv = 0;
    for (const auto& v : vars) {
      if (non_data_vars.find(v.name) == non_data_vars.end() && v.dimids.front()
          == idim) {
        ++nv;
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        NetCDF::VariableData vd;
        if (ad.missing_value.type() != NetCDF::NCType::_NULL) {
          if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
            log_error2("unable to get data for variable '" + v.name + "'", F,
                "nc2xml", USER);
          }
        }
        for (size_t n = 0; n < tvd.size(); ++n) {
          if (not_missing_location(yvd[n], xvd[n])) {
            if (ad.missing_value.type() == NetCDF::NCType::_NULL || vd[n] !=
                ad.missing_value.get()) {
              auto idx = ovd[n];
              ientry.key = pfms[idx] + "[!]" + ityps[idx] + "[!]" + ids[idx];
              auto dt = compute_nc_time(tvd, n);
              if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[n],
                  xvd[n], tvd[n], &dt)) {
                auto e = move(myerror);
                log_error2("'" + e + "' when adding ID " + ientry.key, F,
                    "nc2xml", USER);
              }
              ++scan_data.num_not_missing;
            }
          } else {
            ++scan_data.num_missing_loc;
          }
        }
      }
    }
    scan_data.num_missing_loc /= nv;
  }
  else {
    log_error2("unknown trajectory layout", F, "nc2xml", USER);
  }
}

void scan_cf_trajectory_netcdf_file(InputNetCDFStream& istream, string
    platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data, gatherxml::
    markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable", F, "nc2xml",
        USER);
  }
  string otyp;
  process_vertical_coordinate_variable(dgd, otyp);
  if (otyp.empty()) {
    log_error2("unable to determine observation type", F, "nc2xml", USER);
  }
  if (vars[dgd.indexes.time_var].is_coord) {
log_error2("record data not currently supported", F, "nc2xml", USER);
  } else {

    // ex. H.14 w/sample_dimension
    // ex. H.15 w/instance_dimension
    if (dgd.indexes.stn_id_var == MISSING_FLAG) {
      log_error2("unable to determine trajectory_id variable", F, "nc2xml",
          USER);
    }
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      log_error2("unable to determine latitude variable", F, "nc2xml", USER);
    }
    for (const auto& a : vars[dgd.indexes.lat_var].attrs) {
    }
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      log_error2("unable to determine longitude variable", F, "nc2xml", USER);
    }
    for (const auto& a : vars[dgd.indexes.lon_var].attrs) {
    }
    scan_cf_non_orthogonal_trajectory_netcdf_file(istream, dgd, scan_data,
        obs_data, otyp);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_orthogonal_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  string ityp = "unknown";
  NetCDF::VariableData tvd, yvd, xvd, lvd;
  unique_ptr<NetCDF::VariableData> ivd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, &ivd, nullptr,
      nullptr, F);
  if (istream.variable_data(vars[dgd.indexes.z_var].name, lvd) == NetCDF::
      NCType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  size_t ilen = 1;
  if (ivd->type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  if (tvd.size() != yvd.size() || yvd.size() != xvd.size() || xvd.size() !=
      ivd->size() / ilen) {
    log_error2("profile data does not follow the CF conventions", F, "nc2xml",
        USER);
  }
  NetCDFVariableAttributeData ad;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs, vars[dgd.
      indexes.time_var].nc_type, ad, scan_data.conventions);
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.z_var].name && !v.dimids.empty() && v.dimids.
        back() == vars[dgd.indexes.z_var].dimids.front()) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
          conventions);
      add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units +
          "<!>" + ad.cf_keyword, scan_data);
      for (size_t n = 0; n < tvd.size(); ++n) {
        auto nlv = 0, llv = -1;
        auto avg = 0.;
        auto mn = 1.e38, mx = -1.e38;
        for (size_t m = 0; m < lvd.size(); ++m) {
          if (!found_missing(tvd[n], &ad.missing_value, vd[n * lvd.size() + m],
              ad.missing_value)) {
            if (lvd[m] < mn) {
              mn = lvd[m];
            }
            if (lvd[m] > mx) {
              mx = lvd[m];
            }
            if (llv >= 0) {
              avg += fabsf(lvd[m] - lvd[llv]);
            }
            ++nlv;
            llv = m;
          }
        }
        if (nlv > 0) {
          if (!obs_data.added_to_platforms(obs_type, platform_type, yvd[n], xvd[
              n])) {
            auto e = move(myerror);
            log_error2(e + "' when adding platform " + obs_type + " " +
                platform_type, F, "nc2xml", USER);
          }
          auto dt = compute_nc_time(tvd, n);
          ientry.key = platform_type + "[!]" + ityp + "[!]";
          if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
              NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
            ientry.key += ftos((*ivd)[n]);
          } else if (ivd->type() == NetCDF::NCType::CHAR) {
            ientry.key += string(&(reinterpret_cast<char *>(ivd->get()))[n *
                ilen], ilen);
          }
          if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[n], xvd[
              n], tvd[n], &dt)) {
            auto e = move(myerror);
            log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml", USER);
          }
          gatherxml::markup::ObML::DataTypeEntry dte;
          ientry.data->data_types_table.found(v.name, dte);
          if (dte.data->vdata == nullptr) {
            dte.data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::
                Data::VerticalData);
          }
          if (dgd.z_pos == "down") {
            if (dte.data->vdata->min_altitude > 1.e37) {
              dte.data->vdata->min_altitude = -dte.data->vdata->min_altitude;
            }
            if (dte.data->vdata->max_altitude < -1.e37) {
              dte.data->vdata->max_altitude = -dte.data->vdata->max_altitude;
            }
            if (mx > dte.data->vdata->min_altitude) {
              dte.data->vdata->min_altitude = mx;
            }
            if (mn < dte.data->vdata->max_altitude) {
              dte.data->vdata->max_altitude = mn;
            }
          } else {
            if (mn < dte.data->vdata->min_altitude) {
              dte.data->vdata->min_altitude = mn;
            }
            if (mx > dte.data->vdata->max_altitude) {
              dte.data->vdata->max_altitude = mx;
            }
          }
          dte.data->vdata->units = dgd.z_units;
          dte.data->vdata->avg_nlev += nlv;
          if (nlv > 1) {
            dte.data->vdata->avg_res += (avg / (nlv - 1));
            ++dte.data->vdata->res_cnt;
          }
          ++scan_data.num_not_missing;
        }
      }
    }
  }
}

void fill_vertical_resolution_data(vector<double>& lvls, string z_pos, string
    z_units, gatherxml::markup::ObML::DataTypeEntry& datatype_entry) {
  auto mn = 1.e38, mx = -1.e38;
  for (const auto& l : lvls) {
    if (l < mn) {
      mn = l;
    }
    if (l > mx) {
      mx = l;
    }
  }
  if (datatype_entry.data->vdata == nullptr) {
    datatype_entry.data->vdata.reset(new gatherxml::markup::ObML::
        DataTypeEntry::Data::VerticalData);
  }
  if (z_pos == "down") {
    sort(lvls.begin(), lvls.end(),
    [](const double& left, const double& right) -> bool {
      if (left >= right) {
        return true;
      }
      return false;
    });
    if (datatype_entry.data->vdata->min_altitude > 1.e37) {
      datatype_entry.data->vdata->min_altitude = -datatype_entry.data->vdata->
          min_altitude;
    }
    if (datatype_entry.data->vdata->max_altitude < -1.e37) {
      datatype_entry.data->vdata->max_altitude = -datatype_entry.data->vdata->
          max_altitude;
    }
    if (mx > datatype_entry.data->vdata->min_altitude) {
      datatype_entry.data->vdata->min_altitude = mx;
    }
    if (mn < datatype_entry.data->vdata->max_altitude) {
      datatype_entry.data->vdata->max_altitude = mn;
    }
  } else {
    sort(lvls.begin(), lvls.end(),
    [](const double& left, const double& right) -> bool {
      if (left <= right) {
        return true;
      }
      return false;
    });
    if (mn < datatype_entry.data->vdata->min_altitude) {
      datatype_entry.data->vdata->min_altitude = mn;
    }
    if (mx > datatype_entry.data->vdata->max_altitude) {
      datatype_entry.data->vdata->max_altitude = mx;
    }
  }
  datatype_entry.data->vdata->units = z_units;
  datatype_entry.data->vdata->avg_nlev += lvls.size();
  auto avg = 0.;
  for (size_t n = 1; n < lvls.size(); ++n) {
    avg += fabs(lvls[n] - lvls[n - 1]);
  }
  if (lvls.size() > 1) {
    datatype_entry.data->vdata->avg_res += (avg / (lvls.size() - 1));
    ++datatype_entry.data->vdata->res_cnt;
  }
}

void scan_cf_non_orthogonal_profile_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, lvd;
  unique_ptr<NetCDF::VariableData> ivd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, &ivd, nullptr,
      nullptr, F);
  size_t ilen = 1;
  vector<string> pfms, ityps;
  if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::NCType::
      FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
    for (size_t n = 0; n < tvd.size(); ++n) {
//      int id = (*ivd)[n];
pfms.emplace_back(platform_type);
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd->type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd->get());
    for (size_t n = 0; n < tvd.size(); ++n) {
      string id(&i[n * ilen], ilen);
pfms.emplace_back(platform_type);
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else {
    log_error2("unable to determine platform type", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name, lvd) == NetCDF::
      NCType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  if (tvd.size() != yvd.size() || yvd.size() != xvd.size() || xvd.size() !=
      ivd->size() / ilen) {
    log_error2("profile data does not follow the CF conventions", F, "nc2xml",
        USER);
  }
  NetCDFVariableAttributeData td;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs, vars[dgd.
      indexes.time_var].nc_type, td, scan_data.conventions);
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {

    // continuous ragged array H.10
    NetCDF::VariableData rd;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name, rd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get row size data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.z_var].name && !v.dimids.empty() && v.
          dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        auto x = 0;
        for (size_t n = 0; n < tvd.size(); ++n) {
          vector<double> lv;
          for (size_t m = 0; m < rd[n]; ++m) {
            if (!found_missing(tvd[n], &td.missing_value, vd[x], ad.
                missing_value)) {
              lv.emplace_back(lvd[x]);
            }
            ++x;
          }
          if (!lv.empty()) {
            auto dt = compute_nc_time(tvd, n);
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
                NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
              ientry.key += ftos((*ivd)[n]);
            } else if (ivd->type() == NetCDF::NCType::CHAR) {
              ientry.key += string(&(reinterpret_cast<char *>(ivd->get()))[n *
                  ilen], ilen);
            }
            if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[n],
                xvd[n], tvd[n], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(v.name, dte);
            fill_vertical_resolution_data(lv, dgd.z_pos, dgd.z_units, dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  } else if (dgd.indexes.instance_dim_var != MISSING_FLAG) {

    // indexed ragged array H.11
    NetCDF::VariableData ovd;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, ovd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get instance dimension data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.z_var].name && v.name != vars[dgd.indexes.
          instance_dim_var].name && !v.dimids.empty() && v.dimids.back() ==
          vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        for (size_t n = 0; n < tvd.size(); ++n) {
          vector<double> lv;
          for (size_t m = 0; m < ovd.size(); ++m) {
            if (ovd[m] == n && !found_missing(tvd[n], &td.missing_value, vd[m],
                ad.missing_value)) {
              lv.emplace_back(lvd[m]);
            }
          }
          if (!lv.empty()) {
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::
                NCType::FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
              ientry.key += ftos((*ivd)[n]);
            } else if (ivd->type() == NetCDF::NCType::CHAR) {
              ientry.key += string(&(reinterpret_cast<char *>(ivd->get()))[n *
                  ilen], ilen);
            }
            auto dt = compute_nc_time(tvd, n);
            if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[n],
                xvd[n], tvd[n], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(v.name, dte);
            fill_vertical_resolution_data(lv, dgd.z_pos, dgd.z_units, dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  }
}

void scan_cf_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable", F, "nc2xml",
        USER);
  }
  string otyp;
  process_vertical_coordinate_variable(dgd, otyp);
  if (otyp.empty()) {
    log_error2("unable to determine observation type", F, "nc2xml", USER);
  }
  if (dgd.indexes.sample_dim_var != MISSING_FLAG || dgd.indexes.
      instance_dim_var != MISSING_FLAG) {

    // ex. H.10, H.11
    scan_cf_non_orthogonal_profile_netcdf_file(istream, platform_type, dgd,
        scan_data, obs_data, otyp);
  } else {

    // ex. H.8, H.9
    scan_cf_orthogonal_profile_netcdf_file(istream, platform_type, dgd,
       scan_data, obs_data, otyp);
  }
  scan_data .write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_orthogonal_time_series_profile_netcdf_file(InputNetCDFStream&
     istream, string platform_type, DiscreteGeometriesData& dgd, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data, string
     obs_type) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, lvd;
  unique_ptr<NetCDF::VariableData> ivd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, &ivd, nullptr,
      nullptr, F);
  size_t ilen = 1;
  vector<string> pfms, ityps, ids;
  if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::NCType::
      FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
    for (size_t n = 0; n < ivd->size(); ++n) {
      ids.emplace_back(ftos((*ivd)[n]));
pfms.emplace_back("unknown");
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd->type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd->get());
    for (size_t n = 0; n < ivd->size() / ilen; ++n) {
      ids.emplace_back(&i[n * ilen], ilen);
pfms.emplace_back("unknown");
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else {
    log_error2("unable to determine platform type", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name, lvd) == NetCDF::
      NCType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  for (const auto& v : vars) {
    if (v.dimids.size() == 3 && v.dimids[0] == vars[dgd.indexes.time_var].
        dimids[0] && v.dimids[1] == vars[dgd.indexes.z_var].dimids[0] && v.
        dimids[2] == vars[dgd.indexes.stn_id_var].dimids[0]) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
          conventions);
      add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units +
          "<!>" + ad.cf_keyword, scan_data);
      for (size_t n = 0; n < tvd.size(); ++n) {
        auto dt = compute_nc_time(tvd, n);
        size_t ns = ivd->size() / ilen;
        auto off = n * lvd.size() * ns;
        for (size_t m = 0; m < ns; ++m) {
          vector<double> lv;
          auto x = off + m;
          for (size_t l = 0; l < lvd.size(); ++l) {
            if (!found_missing(tvd[n], nullptr, vd[x], ad.missing_value)) {
              lv.emplace_back(lvd[l]);
            }
            x += ns;
          }
          if (!lv.empty()) {
            ientry.key = pfms[m] + "[!]" + ityps[m] + "[!]" + ids[m];
            if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[m],
                xvd[m], tvd[n], &dt)) {
              auto e = string(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(v.name, dte);
            fill_vertical_resolution_data(lv, dgd.z_pos, dgd.z_units, dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  }
}

void scan_cf_non_orthogonal_time_series_profile_netcdf_file(InputNetCDFStream&
     istream, string platform_type, DiscreteGeometriesData& dgd, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data, string
     obs_type) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "   ...beginning function " << F << " ..." << endl;
  }
  NetCDF::VariableData tvd, yvd, xvd, lvd;
  unique_ptr<NetCDF::VariableData> ivd, nvd, pvd;
  fill_coordinate_data(istream, dgd, &tvd, nullptr, &yvd, &xvd, &ivd, &nvd,
      &pvd, F);
  size_t ilen = 1;
  vector<string> pfms, ityps, ids;
  if (ivd->type() == NetCDF::NCType::INT || ivd->type() == NetCDF::NCType::
      FLOAT || ivd->type() == NetCDF::NCType::DOUBLE) {
    for (size_t n = 0; n < ivd->size(); ++n) {
      ids.emplace_back(ftos((*ivd)[n]));
pfms.emplace_back("unknown");
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd->type() == NetCDF::NCType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    size_t num_ids = ivd->size() / ilen;
    size_t nlen = 0, num_nets = 0;
    if (dgd.indexes.network_var != MISSING_FLAG) {
      nlen = dims[vars[dgd.indexes.network_var].dimids.back()].length;
      num_nets = nvd->size() / nlen;
    }
    size_t plen = 0, num_plats = 0;
    if (dgd.indexes.platform_var != MISSING_FLAG) {
      plen = dims[vars[dgd.indexes.platform_var].dimids.back()].length;
      num_plats = pvd->size() / plen;
    }
    char *i = reinterpret_cast<char *>(ivd->get());
    for (size_t n = 0; n < num_ids; ++n) {
      ids.emplace_back(&i[n * ilen], ilen);
      if (num_plats == num_ids) {
        pfms.emplace_back(&(reinterpret_cast<char *>(pvd->get()))[n * plen],
            plen);
      } else {
        pfms.emplace_back("unknown");
      }
      if (num_nets == num_ids) {
        ityps.emplace_back(&(reinterpret_cast<char *>(nvd->get()))[n * nlen],
            nlen);
      } else {
        ityps.emplace_back("unknown");
      }
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else {
    log_error2("unable to determine platform type", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name, lvd) == NetCDF::
      NCType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {

    // H.19
    if (dgd.indexes.instance_dim_var == MISSING_FLAG) {
      log_error2("found sample dimension but not instance dimension", F,
          "nc2xml", USER);
    }
    NetCDF::VariableData rvd, svd;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name, rvd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get sample dimension data", F, "nc2xml", USER);
    }
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, svd) ==
        NetCDF::NCType::_NULL) {
      log_error2("unable to get instance dimension data", F, "nc2xml", USER);
    }
    if (rvd.size() != svd.size()) {
      log_error2("sample dimension and instance dimension have different sizes",
          F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.dimids.front() == vars[dgd.indexes.z_var].dimids.front() && v.name
          != vars[dgd.indexes.z_var].name) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        auto x = 0;
        for (size_t n = 0; n < rvd.size(); ++n) {
          auto dt = compute_nc_time(tvd, n);
          vector<double> lv;
          for (size_t m = 0; m < rvd[n]; ++m) {
            if (!found_missing(tvd[n], nullptr, vd[x], ad.missing_value)) {
              lv.emplace_back(lvd[x]);
            }
            ++x;
          }
          if (!lv.empty()) {
            ientry.key = pfms[svd[n]] + "[!]" + ityps[svd[n]] + "[!]" + ids[svd[
                n]];
            if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[svd[
                n]], xvd[svd[n]], tvd[n], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            gatherxml::markup::ObML::DataTypeEntry dte;
            ientry.data->data_types_table.found(v.name, dte);
            fill_vertical_resolution_data(lv, dgd.z_pos, dgd.z_units, dte);
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  } else {

    // H.16, H.18
    auto nt = dims[vars[dgd.indexes.time_var].dimids.back()].length;
    auto nl = dims[vars[dgd.indexes.z_var].dimids.back()].length;
    auto ssz = nt * nl;
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.z_var].name && ((v.dimids.size() == 3 && v.
          dimids[0] == vars[dgd.indexes.stn_id_var].dimids.front() && v.dimids[
          1] == vars[dgd.indexes.time_var].dimids.back() && v.dimids[2] == vars[
          dgd.indexes.z_var].dimids.back()) || (v.dimids.size() == 2 && v.
          dimids[0] == vars[dgd.indexes.time_var].dimids.back() && v.dimids[1]
          == vars[dgd.indexes.z_var].dimids.back()))) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
            conventions);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        for (size_t n = 0; n < vd.size(); ) {
          auto x = n / ssz;
          for (size_t m = 0; m < nt; ++m) {
            vector<double> lv;
            for (size_t l = 0; l < nl; ++l, ++n) {
              if (!found_missing(tvd[n], nullptr, vd[n], ad.missing_value)) {
                if (lvd.size() == vd.size()) {
                  lv.emplace_back(lvd[n]);
                } else if (lvd.size() == nl) {
                  lv.emplace_back(lvd[l]);
                } else {
                  lv.emplace_back(lvd[x * nl + l]);
                }
              }
            }
            if (!lv.empty()) {
              ientry.key = pfms[x] + "[!]" + ityps[x] + "[!]" + ids[x];
              if (tvd.size() == nt) {
                auto dt = compute_nc_time(tvd, m);
                if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[x],
                    xvd[x], tvd[m], &dt)) {
                  auto e = move(myerror);
                  log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                      USER);
                }
              } else {
                auto i = x * nt + m;
                auto dt = compute_nc_time(tvd, i);
                if (!obs_data.added_to_ids(obs_type, ientry, v.name, "", yvd[x],
                    xvd[x], tvd[i], &dt)) {
                  auto e = move(myerror);
                  log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                      USER);
                }
              }
              gatherxml::markup::ObML::DataTypeEntry dte;
              ientry.data->data_types_table.found(v.name, dte);
              fill_vertical_resolution_data(lv, dgd.z_pos, dgd.z_units, dte);
              ++scan_data.num_not_missing;
            }
          }
        }
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "   ...function " << F << " done." << endl;
  }
}

void scan_cf_time_series_profile_netcdf_file(InputNetCDFStream& istream, string
    platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable", F, "nc2xml",
        USER);
  }
  string otyp;
  process_vertical_coordinate_variable(dgd, otyp);
  if (otyp.empty()) {
    log_error2("unable to determine observation type", F, "nc2xml", USER);
  }
  if (vars[dgd.indexes.time_var].is_coord && vars[dgd.indexes.z_var].is_coord) {

    // ex. H.17
    scan_cf_orthogonal_time_series_profile_netcdf_file(istream, platform_type,
        dgd, scan_data, obs_data, otyp);
  } else {

    // ex. H.16, H.18, H.19
    scan_cf_non_orthogonal_time_series_profile_netcdf_file(istream,
        platform_type, dgd, scan_data, obs_data, otyp);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void find_coordinate_variables(InputNetCDFStream& istream, vector<NetCDF::
    Variable>& vars, GridData& grid_data, my::map<metautils::NcTime::
    TimeRangeEntry>& tr_table) {
  static const string F = this_function_label(__func__);
  unordered_set<string> lset;
  for (const auto& v : vars) {
    if (v.is_coord) {
      if (gatherxml::verbose_operation) {
        cout << "'" << v.name << "' is a coordinate variable" << endl;
      }
      for (const auto& a : v.attrs) {
        string s;
        if (a.name == "standard_name") {
          s = *(reinterpret_cast<string *>(a.values));
        } else {
          s = "";
        }
        if (a.nc_type == NetCDF::NCType::CHAR && (a.name == "units" ||
            regex_search(s, regex("hybrid_sigma")))) {
          string units;
          if (a.name == "units") {
            units = *(reinterpret_cast<string *>(a.values));
          }
          auto ul = to_lower(units);
          if (regex_search(ul, regex("since"))) {
            if (!grid_data.time.id.empty()) {
              log_error2("time was already identified - don't know what to do "
                  "with variable: " + v.name, F, "nc2xml", USER);
            }

            // check for time ranges other than analysis
            string clim;
            for (const auto& a2 : v.attrs) {
              if (a2.nc_type == NetCDF::NCType::CHAR) {
                if (a2.name == "calendar") {
                  time_data.calendar = *(reinterpret_cast<string *>(a2.values));
                } else if (a2.name == "bounds") {
                  grid_data.time_bounds.id = *(reinterpret_cast<string *>(a2.
                      values));
                } else if (a2.name == "climatology") {
                  clim = *(reinterpret_cast<string *>(a2.values));
                }
              }
            }
            time_data.units = ul.substr(0, ul.find("since"));
            trim(time_data.units);
            grid_data.time.dim = v.dimids[0];
            grid_data.time.id = v.name;
            ul = ul.substr(ul.find("since") + 5);
            trim(ul);
            auto sp = split(ul);
            auto sp2 = split(sp[0], "-");
            if (sp2.size() != 3) {
              log_error2("bad netcdf date", F, "nc2xml", USER);
            }
            long long dt = stoi(sp2[0]) * 10000000000 + stoi(sp2[1]) * 100000000
                + stoi(sp2[2]) * 1000000;
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
            if (!clim.empty()) {
              for (const auto& v2 : vars) {
                if (v2.name == clim) {
                  NetCDF::VariableData vd;
                  istream.variable_data(v2.name, vd);
/*
                  unique_ptr<double[]> ta1(new double[vd.size()/2]);
                  unique_ptr<double[]> ta2(new double[vd.size()/2]);
                  for (size_t x=0; x < static_cast<size_t>(vd.size()); x+=2) {
                    ta1[x/2]=vd[x];
                    ta2[x/2]=vd[x + 1];
                  }
*/
                  time_bounds_s.t1 = vd.front();
                  time_bounds_s.t2 = vd.back();
                  size_t nsteps = vd.size() / 2;
                  for (size_t x = 0; x < nsteps; ++x) {
                    DateTime d1, d2;
                    if (time_data.units == "hours") {
                      d1 = time_data.reference.hours_added(vd[x * 2]);
                      d2 = time_data.reference.hours_added(vd[x * 2 + 1]);
                    } else if (time_data.units == "days") {
                      d1 = time_data.reference.days_added(vd[x * 2]);
                      d2 = time_data.reference.days_added(vd[x * 2 + 1]);
                    } else if (time_data.units == "months") {
                      d1 = time_data.reference.months_added(vd[x * 2]);
                      d2 = time_data.reference.months_added(vd[x * 2 + 1]);
                    } else {
                      log_error2("don't understand climatology_bounds units in "
                          + time_data.units, F, "nc2xml", USER);
                    }
                    metautils::NcTime::TimeRangeEntry tre;
                    tre.key = d2.years_since(d1) + 1;
                    if (!tr_table.found(tre.key, tre)) {
                      tre.instantaneous.first_valid_datetime.set(
                          static_cast<long long>(30001231235959));
                      tre.instantaneous.last_valid_datetime.set(
                          static_cast<long long>(10000101000000));
                      tre.bounded.first_valid_datetime.set(
                          static_cast<long long>(30001231235959));
                      tre.bounded.last_valid_datetime.set(
                          static_cast<long long>(10000101000000));
                      tr_table.insert(tre);
                    }
                    if (d1 < tre.bounded.first_valid_datetime) {
                      tre.bounded.first_valid_datetime = d1;
                    }
                    if (d2 > tre.bounded.last_valid_datetime) {
                      tre.bounded.last_valid_datetime = d2;
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
                        log_error2("unable to handle climatology of " + itos(b)
                            + "-day means", F, "nc2xml", USER);
                      }
                    }
                    tre.unit = b;
                    ++(tre.num_steps);
                  }
                  break;
                }
              }
              if (gatherxml::verbose_operation) {
                for (const auto& tr_key : tr_table.keys()) {
                  metautils::NcTime::TimeRangeEntry tre;
                  tr_table.found(tr_key, tre);
                  cout << "   ...setting temporal range for climatology key " <<
                      tr_key << " to:" << endl;
                  cout << "      " << tre.bounded.first_valid_datetime.
                      to_string() << " to " << tre.bounded.last_valid_datetime.
                      to_string() << ", units=" << tre.unit << endl;
                }
              }
            }
          } else if (ul == "degrees_north" || ul == "degree_north" ||
              ul == "degrees_n" || ul == "degree_n" || (ul == "degrees" && v.
                  name == "lat")) {
/*
                if (found_lat) {
                  log_warning("latitude was already identified - ignoring '" +
                      v.name + "'", "nc2xml", USER);
                }
else {
*/
            grid_data.lats.emplace_back();
            grid_data.lats.back().dim = v.dimids[0];
            grid_data.lats.back().id = v.name;
            grid_data.lats.back().type = v.nc_type;
            string s;
            for (const auto& a2 : v.attrs) {
              if (a2.nc_type == NetCDF::NCType::CHAR && a2.name == "bounds") {
                s = *(reinterpret_cast<string *>(a2.values));
              }
            }
            grid_data.lats_b.emplace_back();
            grid_data.lats_b.back().id = s;
//}
          } else if (ul == "degrees_east" || ul == "degree_east" || ul ==
              "degrees_e" || ul == "degree_e" || (ul == "degrees" && v.name ==
              "lon")) {
/*
                if (found_lon) {
                  log_warning("longitude was already identified - ignoring '" +
                      v.name + "'", "nc2xml", USER);
                }
else {
*/
            grid_data.lons.emplace_back();
            grid_data.lons.back().dim = v.dimids[0];
            grid_data.lons.back().id = v.name;
            grid_data.lons.back().type = v.nc_type;
            string s;
            for (const auto& a2 : v.attrs) {
              if (a2.nc_type == NetCDF::NCType::CHAR && a2.name == "bounds") {
                s = *(reinterpret_cast<string *>(a2.values));
              }
            }
            grid_data.lons_b.emplace_back();
            grid_data.lons_b.back().id = s;
//}
          } else {
            if (grid_data.time.id.empty() && v.name == "time") {
              if (found_netcdf_time_from_patch(v)) {
                grid_data.time.dim = v.dimids[0];
                grid_data.time.id = v.name;
              }
            } else {
              metautils::StringEntry se;
              if (lset.find(v.name) == lset.end()) {
                grid_data.levels.emplace_back();
                grid_data.levels.back().dim = v.dimids[0];
                grid_data.levels.back().id = v.name;
                grid_data.levels.back().type = v.nc_type;
                grid_data.levdata.emplace_back();
                grid_data.levdata.back().ID = v.name + "@@" + units;
                grid_data.levdata.back().units = units;
                grid_data.levdata.back().write = false;
                for (const auto& a3 : v.attrs) {
                  if (a3.nc_type == NetCDF::NCType::CHAR && a3.name ==
                      "long_name") {
                    grid_data.levdata.back().description = *(reinterpret_cast<
                        string *>(a3.values));
                  }
                }
                lset.emplace(v.name);
              }
            }
          }
        }
      }
    }
  }
}

bool found_auxiliary_lat_lon_coordinates(GridData& grid_data) {
  for (const auto& v : vars) {
    if (!v.is_coord && v.dimids.size() == 2) {
      for (const auto& a : v.attrs) {
        if (a.name == "units" && a.nc_type == NetCDF::NCType::CHAR) {
          auto units = *(reinterpret_cast<string *>(a.values));
          if (units == "degrees_north" || units == "degree_north" || units ==
              "degrees_n" || units == "degree_n") {
            size_t d = 0;
            for (const auto& x : v.dimids) {
              d = 100 * d + x + 1;
            }
            d *= 100;
            grid_data.lats.emplace_back();
            grid_data.lats.back().dim = d;
            grid_data.lats.back().id = v.name;
            grid_data.lats.back().type = v.nc_type;
          } else if (units == "degrees_east" || units == "degree_east" ||
              units == "degrees_e" || units == "degree_e") {
            size_t d = 0;
            for (const auto& x : v.dimids) {
              d = 100 * d + x + 1;
            }
            d *= 100;
            grid_data.lons.emplace_back();
            grid_data.lons.back().dim = d;
            grid_data.lons.back().id = v.name;
            grid_data.lons.back().type = v.nc_type;
          }
        }
      }
    }
  }
  return !grid_data.lats.empty() && grid_data.lats.size() == grid_data.lons.
      size();
}

bool grid_is_centered_lambert_conformal(const unique_ptr<double[]>& lats,
    const unique_ptr<double[]>& lons, Grid::GridDimensions& dims, Grid::
    GridDefinition& def) {
  if (gatherxml::verbose_operation) {
    cout << "         ... checking for centered Lambert-Conformal projection "
        "..." << endl;
  }
  auto dx2 = dims.x / 2;
  auto dy2 = dims.y / 2;
  switch (dims.x % 2) {
    case 0: {
      auto xm = dx2 - 1;
      auto ym = dy2 - 1;
      auto yp = dy2 + 1;
      if (myequalf((lons[ym * dims.x + xm] + lons[ym * dims.x + dx2]), (lons[yp
          * dims.x + xm] + lons[yp * dims.x + dx2]), 0.00001) && myequalf(lats[
          dy2 * dims.x + xm], lats[dy2 * dims.x + dx2], 0.00001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(lats[dy2 *
            dims.x + dx2]);
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround((lons[dy2 * dims.x + xm] + lons[dy2 * dims.x +
            dx2]) / 2.);
        def.dx = def.dy = lround(111.1 * cos(lats[dy2 * dims.x + xm] *
            3.141592654 / 180.) * (lons[dy2 * dims.x + dx2] - lons[dy2 * dims.x
            + xm]));
        if (gatherxml::verbose_operation) {
          cout << "            ... confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
    case 1: {
      if (myequalf(lons[(dy2 - 1) * dims.x + dx2], lons[(dy2 + 1) * dims.x +
          dx2], 0.00001) && myequalf(lats[dy2 * dims.x + dx2 - 1], lats[dy2 *
          dims.x + dx2 + 1], 0.00001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(lats[dy2 *
            dims.x + dx2]);
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround(lons[dy2 * dims.x + dx2]);
        def.dx = def.dy = lround(111.1 * cos(lats[dy2 * dims.x + dx2] *
            3.141592654 / 180.) * (lons[dy2 * dims.x + dx2 + 1] - lons[dy2 *
            dims.x + dx2]));
        if (gatherxml::verbose_operation) {
          cout << "            ... confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "         ... done." << endl;
  }
  return false;
}

bool grid_is_non_centered_lambert_conformal(const unique_ptr<double[]>& lats,
    const unique_ptr<double[]>& lons, Grid::GridDimensions& dims, Grid::
    GridDefinition& def) {
  if (gatherxml::verbose_operation) {
    cout << "         ... checking for non-centered Lambert-Conformal "
        "projection ..." << endl;
  }
  def.type = Grid::Type::not_set;

  // find the x-offsets in each row where the change in latitude goes from
  //   positive to negative
  vector<double> v;
  v.reserve(dims.y);
  double s = 0.;
  for (auto n = 0; n < dims.y; ++n) {
    auto yoff = n * dims.x;
    for (auto m = 0; m < dims.x - 1; ++m) {
      auto xoff = yoff + m;
      if (lats[xoff + 1] <= lats[xoff]) {
        v.emplace_back(xoff - yoff);
        s += lons[xoff];
        break;
      }
    }
  }

  // find the variance in the x-offsets
  auto xbar = lround(accumulate(v.begin(), v.end(), 0.) / v.size());
  double d = 0.;
  for (const auto& e : v) {
    auto x = e - xbar;
    d += x * x;
  }
  auto var = d / (v.size() - 1);
  if (var >= 1.) return false;

  // if the variance is low, confident that we found the orientation longitude
  def.type = Grid::Type::lambertConformal;
  def.olongitude = lround(s / v.size());

  // find the x-direction distance for each row at the orientation longitude
  const double PI = 3.141592654;
  const double DEGRAD = PI / 180.;
  const double KMDEG = 111.1;
  v.clear();
  for (auto n = xbar; n < dims.x * dims.y; n += dims.x) {
    auto dy = (lons[n + 1] - lons[n]) * KMDEG * cos(lats[n] * DEGRAD);
    auto dx = (lats[n + 1] - lats[n]) * KMDEG;
    v.emplace_back(sqrt(dx * dx + dy * dy));
  }
  def.dx = lround(accumulate(v.begin(), v.end(), 0.) / v.size());
  def.stdparallel1 = def.stdparallel2 = -99.;
  size_t i = 0;
  for (size_t n = 0; n < v.size(); ++n) {
    if (myequalf(v[n], def.dx, 0.001)) {
      auto p = lround(lats[xbar + n  * dims.x]);
      if (def.stdparallel1 < -90.) {
        def.stdparallel1 = p;
        i = xbar + n * dims.x;
      } else if (def.stdparallel2 < -90.) {
        if (p != def.stdparallel1) def.stdparallel2 = p;
      } else if (p != def.stdparallel2) {
        if (gatherxml::verbose_operation) {
          cout << "            ... check for a non-centered projection "
              "failed. Too many tangent latitudes." << endl;
        }
        def.type = Grid::Type::not_set;
        return false;
      }
    }
  }
  if (def.stdparallel1 < -90.) {
    if (gatherxml::verbose_operation) {
      cout << "            ... check for a non-centered projection failed. "
          "No tangent latitude could be identified." << endl;
    }
    def.type = Grid::Type::not_set;
    return false;
  } else if (def.stdparallel2 < -90.) {
    def.stdparallel2 = def.stdparallel1;
  }
  def.llatitude = def.stdparallel1;
  def.projection_flag = def.llatitude >= 0. ? 0 : 1;
  auto dx = (lons[i] - lons[i - dims.x]) * KMDEG * cos(lats[i - dims.x] *
      DEGRAD);
  auto dy = (lats[i] - lats[i - dims.x]) * KMDEG;
  def.dy = lround(sqrt(dx * dx + dy * dy));
  if (gatherxml::verbose_operation) {
    cout << "            ... confirmed a non-centered Lambert-conformal "
        "projection." << endl;
    cout << "         ... done." << endl;
  }
  return true;
}

bool grid_is_lambert_conformal(const unique_ptr<double[]>& lats, const
    unique_ptr<double[]>& lons, Grid::GridDimensions& dims, Grid::
    GridDefinition& def) {
  if (gatherxml::verbose_operation) {
    cout << "      ... checking for Lambert-Conformal projection ..." << endl;
  }
  if (grid_is_centered_lambert_conformal(lats, lons, dims, def)) {
    return true;
  }
  if (gatherxml::verbose_operation) {
    cout << "      ...check for a centered Lambert-Conformal projection "
        "failed, checking for a non-centered projection..." << endl;
  }
  if (grid_is_non_centered_lambert_conformal(lats, lons, dims, def)) {
    return true;
  }
  if (gatherxml::verbose_operation) {
    cout << "      ...check for a Lambert-Conformal projection finished. Not "
        "an LC projection." << endl;
  }
  return false;
}

bool found_grid_projection(bool b) {
  if (gatherxml::verbose_operation) {
    cout << "   ... done." << endl;
  }
  return b;
}

bool filled_grid_projection(const unique_ptr<double[]>& lats, const unique_ptr<
    double[]>& lons, Grid::GridDimensions& d, Grid::GridDefinition& f, size_t
    nlats, size_t nlons) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "   ... trying to fill grid projection ..." << endl;
  }

  // check as one-dimensional coordinates
  double ndy = 99999., xdy = 0.;
  for (int n = 1, m = d.x; n < d.y; ++n, m += d.x) {
    double dy = fabs(lats[m] - lats[m - d.x]);
    if (dy < ndy) {
      ndy = dy;
    }
    if (dy > xdy) {
      xdy = dy;
    }
  }
  double ndx = 99999., xdx = 0.;
  for (int n = 1; n < d.x; ++n) {
    double dx = fabs(lons[n] - lons[n - 1]);
    if (dx < ndx) {
      ndx = dx;
    }
    if (dx > xdx) {
      xdx = dx;
    }
  }
  f.type = Grid::Type::not_set;
  d.size = d.x * d.y;
  if (fabs(xdx - ndx) < 0.0001) {
    if (fabs(xdy - ndy) < 0.0001) {
      f.type = Grid::Type::latitudeLongitude;
      f.elatitude = lats[d.size - 1];
      f.elongitude = lons[d.size - 1];
      f.laincrement = xdy;
      f.loincrement = xdx;
      return found_grid_projection(true);
    }
    auto la1 = lats[0];
    auto la2 = lats[d.size - 1];
    if ((la1 >= 0. && la2 < 0.) || (la1 < 0. && la2 >= 0.)) {
      if (fabs(la1) <= fabs(la2)) {
        const double PI = 3.141592654;
        if (fabs(cos(la2 * PI / 180.) - (ndy / xdy)) < 0.01) {
          f.type = Grid::Type::mercator;
          f.elatitude = lats[d.size - 1];
          f.elongitude = lons[d.size - 1];
          double ma = 99999.;
          int l = -1;
          for (int n = 0, m = 0; n < d.y; ++n, m += d.x) {
            double a = fabs(lats[m] - lround(lats[m]));
            if (a < ma) {
              ma = a;
              l = m;
            }
          }
          f.dx = lround(cos(lats[l] * PI / 180.) * ndx * 111.2);
          f.dy = lround((fabs(lats[l + d.x] - lats[l]) + fabs(lats[l] - lats[l -
              d.x])) / 2. * 111.2);
          f.stdparallel1 = lats[l];
          return found_grid_projection(true);
        }
      }
    }
  }

  // check as two-dimensional coordinates
  static const double LAT_TOL = 0.00001;
  auto dx = fabs(lons[1] - lons[0]);
  for (short n = 0; n < d.y; ++n) {
    for (short m = 1; m < d.x; ++m) {
      auto x = n * d.x + m;
      auto this_dx = fabs(lons[x] - lons[x-1]);
      if (!myequalf(this_dx, dx, LAT_TOL) && !myequalf(this_dx + dx, 360.,
          LAT_TOL)) {
        dx = 1.e36;
        n = d.y;
        m = d.x;
      }
    }
  }
  if (!myequalf(dx, 1.e36)) {
    auto dy = fabs(lats[1] - lats[0]);
    for (short m = 0; m < d.x; ++m) {
      for (short n = 1; n < d.y; ++n) {
        auto x = m * d.y + n;
        if (!myequalf(fabs(lats[x] - lats[x-1]), dy, LAT_TOL)) {
          dy = 1.e36;
          m = d.x;
          n = d.y;
        }
      }
    }
    if (!myequalf(dy, 1.e36)) {
      f.type = Grid::Type::latitudeLongitude;
      f.elatitude = lats[nlats - 1];
      f.elongitude = lons[nlons - 1];
      f.laincrement = dy;
      f.loincrement = dx;
      return found_grid_projection(true);
    }
    for (short n = 0; n < d.y; ++n) {
      auto x = n * d.x;
      dy = fabs(lats[x + 1] - lats[x]);
      for (short m = 2; m < d.x; ++m) {
        auto x = n * d.x + m;
        if (!myequalf(fabs(lats[x] - lats[x - 1]), dy, LAT_TOL)) {
          dy = 1.e36;
          n = d.y;
          m = d.x;
        }
      }
    }
    if (!myequalf(dy, 1.e36)) {
      const double PI = 3.141592654;
      auto ny2 = d.y / 2 - 1;
      auto a = log(tan(PI / 4. + lats[0] * PI / 360.));
      auto b = log(tan(PI / 4. + lats[ny2 * d.x] * PI / 360.));
      auto c = log(tan(PI / 4. + lats[ny2 * 2 * d.x] * PI / 360.));
      if (myequalf((b - a) / ny2, (c - a) / (ny2 * 2), 0.00001)) {
        f.type = Grid::Type::mercator;
        f.elatitude = lats[nlats - 1];
        f.elongitude = lons[nlons - 1];
        f.laincrement = (f.elatitude - f.slatitude) / (d.y - 1);
        f.loincrement = dx;
        return found_grid_projection(true);
      }
    }
  }

  // check for a polar-stereographic grid
  auto ny2 = d.y / 2 - 1;
  auto nx2 = d.x / 2 - 1;

  // check the four points that surround the center of the grid to see if the
  //    center is the pole:
  //        1) all four latitudes must be the same
  //        2) the sum of the absolute values of opposing longitudes must
  //           equal 180.
  if (myequalf(lats[ny2 * d.x + nx2], lats[(ny2 + 1) * d.x + nx2], LAT_TOL) &&
      myequalf(lats[(ny2 + 1) * d.x + nx2], lats[(ny2 + 1) * d.x + nx2 + 1],
      LAT_TOL) && myequalf(lats[(ny2 + 1) * d.x + nx2 + 1], lats[ny2 * d.x +
      nx2 + 1], LAT_TOL) && myequalf(fabs(lons[ny2 * d.x + nx2]) + fabs(lons[(
      ny2 + 1) * d.x + nx2 + 1]), 180., 0.001) && myequalf(fabs(lons[(ny2 + 1)
      * d.x + nx2]) + fabs(lons[ny2 * d.x + nx2 + 1]), 180., 0.001)) {
    f.type = Grid::Type::polarStereographic;
    if (lats[ny2 * d.x + nx2] > 0) {
      f.projection_flag = 0;
      f.llatitude = 60.;
    } else {
      f.projection_flag = 1;
      f.llatitude = -60.;
    }
    f.olongitude = lroundf((lons[nx2] + lons[nx2 + 1]) / 2.);
    if (f.olongitude > 180.) {
      f.olongitude -= 360.;
    }

    // look for dx and dy at the 60-degree parallel
    // great circle formula:
    //    theta = 2 * arcsin[ sqrt( sin^2( delta_phi / 2 ) + cos(phi_1) *
    //        cos(phi_2) * sin^2( delta_lambda / 2 ) ) ]
    //    phi_1 and phi_2 are latitudes
    //    lambda_1 and lambda_2 are longitudes
    //    dist = 6372.8 * theta
    //    6372.8 is radius of Earth in km
    double a, na = 999.;
    int nm = 0;
    for (size_t m = 0; m < nlats; ++m) {
      if ( (a = fabs(f.llatitude - lats[m])) < na) {
        na = a;
        nm = m;
      }
    }
    const double RAD = 3.141592654 / 180.;
    f.dx = lroundf(asin(sqrt(sin(fabs(lats[nm] - lats[nm + 1]) / 2. * RAD) *
        sin(fabs(lats[nm] - lats[nm + 1]) / 2. * RAD) + sin(fabs(lons[nm] -
        lons[nm + 1]) / 2. * RAD) * sin(fabs(lons[nm] - lons[nm + 1]) / 2. *
        RAD) * cos(lats[nm] * RAD) * cos(lats[nm + 1] * RAD))) * 12745.6);
    f.dy = lroundf(asin(sqrt(sin(fabs(lats[nm] - lats[nm + d.x]) / 2. * RAD) *
        sin(fabs(lats[nm] - lats[nm + d.x]) / 2. * RAD) + sin(fabs(lons[nm] -
        lons[nm + d.x]) / 2. * RAD) * sin(fabs(lons[nm] - lons[nm + d.x]) / 2.
        * RAD) * cos(lats[nm] * RAD) * cos(lats[nm + d.x] * RAD))) * 12745.6);
    return found_grid_projection(true);
  }

  // check for a lambert-conformal grid
  return found_grid_projection(grid_is_lambert_conformal(lats, lons, d, f));
}

void process_time_bounds(InputNetCDFStream& istream, string time_bounds_id,
    metautils::NcTime::TimeRangeEntry& tre) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "   ...adjusting times for time bounds" << endl;
  }
  auto tb_type = NetCDF::NCType::_NULL;
  for (const auto& v : vars) {
    if (v.name == time_bounds_id) {
      tb_type = v.nc_type;
      break;
    }
  }
  if (tb_type == NetCDF::NCType::_NULL) {
    log_error2("unable to determine type of time bounds", F, "nc2xml", USER);
  }
  NetCDF::VariableData vd;
  istream.variable_data(time_bounds_id, vd);
  if (vd.size() != time_s.num_times * 2) {
    log_error2("unable to handle more than two time bounds values per time", F,
        "nc2xml", USER);
  }
  time_bounds_s.t1 = vd.front();
  if (static_cast<long long>(tre.key) != -11) {
    time_bounds_s.diff = vd[1] - time_bounds_s.t1;
    for (size_t l = 2; l < vd.size(); l += 2) {
      double diff = vd[l + 1] - vd[l];
      // handle leap years in year-length time bounds
      if (time_data.calendar == "gregorian" && time_data.units == "days" &&
          ((time_bounds_s.diff == 365 && diff == 366) || (time_bounds_s.diff ==
          366 && diff == 365))) {
        time_bounds_s.diff = 365;
        diff = 365;
      }
      if (!myequalf(diff, time_bounds_s.diff)) {
        // allow month-intervals specified in units of days to pass
        if (time_data.units != "days" || time_bounds_s.diff < 28 ||
            time_bounds_s.diff > 31 || diff < 28 || diff > 31) {
          time_bounds_s.changed = true;
        }
      }
    }
  }
  time_bounds_s.t2 = vd.back();
  string e;
  tre.bounded.first_valid_datetime = metautils::NcTime::actual_date_time(
      time_bounds_s.t1, time_data, e);
  if (!e.empty()) {
    log_error2(e, F, "nc2xml", USER);
  }
  tre.bounded.last_valid_datetime = metautils::NcTime::actual_date_time(
      time_bounds_s.t2, time_data, e);
  if (!e.empty()) {
    log_error2(e, F, "nc2xml", USER);
  }
  if (gatherxml::verbose_operation) {
    cout << "      ...now temporal range is:" << endl;
    cout << "         " << tre.bounded.first_valid_datetime.to_string() <<
        " to " << tre.bounded.last_valid_datetime.to_string() << endl;
  }
}

void process_horizontal_grid(InputNetCDFStream& istream, const GridData&
    grid_data, vector<Grid::GridDimensions>& grid_dims, vector<Grid::
    GridDefinition>& grid_defs) {
  static const string F = this_function_label(__func__);
  for (size_t x = 0; x < grid_data.lats.size(); ++x) {
    if (grid_data.lats[x].dim < 100) {
      grid_defs.emplace_back(Grid::GridDefinition());
      grid_defs.back().type = Grid::Type::latitudeLongitude;

      // get the latitude range
      NetCDF::VariableData vd;
      istream.variable_data(grid_data.lats[x].id, vd);
      grid_dims.emplace_back(Grid::GridDimensions());
      grid_dims.back().y = vd.size();
      grid_defs.back().slatitude = vd.front();
      grid_defs.back().elatitude = vd.back();
      grid_defs.back().laincrement = fabs((grid_defs.back().elatitude -
          grid_defs.back().slatitude) / (vd.size() - 1));
      if (grid_data.lons[x].dim != MISSING_FLAG) {

        // check for gaussian lat-lon
        if (!myequalf(fabs(vd[1] - vd[0]), grid_defs.back().laincrement, 0.001)
            && myequalf(vd.size() / 2., vd.size() / 2, 0.00000000001)) {
          grid_defs.back().type = Grid::Type::gaussianLatitudeLongitude;
          grid_defs.back().laincrement = vd.size() / 2;
        }
        if (!grid_data.lats_b[x].id.empty()) {
          if (grid_data.lons_b[x].id.empty()) {
            log_error2("found a lat bounds but no lon bounds", F, "nc2xml",
                USER);
          }
          istream.variable_data(grid_data.lats_b[x].id, vd);
          grid_defs.back().slatitude = vd.front();
          grid_defs.back().elatitude = vd.back();
          grid_defs.back().is_cell = true;
        }

        // get the longitude range
        istream.variable_data(grid_data.lons[x].id, vd);
        grid_dims.back().x = vd.size();
        grid_defs.back().slongitude = vd.front();
        grid_defs.back().elongitude = vd.back();
        grid_defs.back().loincrement = fabs((grid_defs.back().elongitude -
            grid_defs.back().slongitude) / (vd.size() - 1));
        if (!grid_data.lons_b[x].id.empty()) {
          if (grid_data.lats_b[x].id.empty()) {
            log_error2("found a lon bounds but no lat bounds", F, "nc2xml",
                USER);
          }
          istream.variable_data(grid_data.lons_b[x].id, vd);
          grid_defs.back().slongitude = vd.front();
          grid_defs.back().elongitude = vd.back();
        }
      }
    }
  }
}

void scan_cf_grid_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  grid_initialize(scan_data);

  // open a file inventory unless this is a test run
  if (metautils::args.dsid < "d999000") {
    gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
        "nc2xml", USER);
  }
  string source;
  for (const auto& a : gattrs) {
    if (to_lower(a.name) == "source") {
      source = *(reinterpret_cast<string *>(a.values));
    }
  }
  GridData grid_data;
  my::map<metautils::NcTime::TimeRangeEntry> tr_table;
  find_coordinate_variables(istream, vars, grid_data, tr_table);
  if (grid_data.time.id.empty()) {
    log_error2("time coordinate not found", F, "nc2xml", USER);
  }
  vector<Grid::GridDimensions> grid_dims;
  vector<Grid::GridDefinition> grid_defs;
  if (grid_data.lats.empty() || grid_data.lons.empty()) {
    if (!grid_data.lats.empty()) {

      // could be a zonal mean if latitude was found, but not longitude
      for (size_t n=0; n < grid_data.lats.size(); ++n) {
         grid_data.lons.emplace_back();
         grid_data.lons.back().dim = MISSING_FLAG;
      }
    } else if (!grid_data.lons.empty()) {
      log_error2("found longitude coordinate variable, but not latitude "
          "coordinate variable", F, "nc2xml", USER);
    } else {
      if (gatherxml::verbose_operation) {
        cout << "looking for auxiliary latitude/longitude ..." << endl;
      }
      if (!found_auxiliary_lat_lon_coordinates(grid_data)) {
        cerr << "Terminating - could not find any latitude/longitude "
            "coordinates" << endl;
        exit(1);
      }
      if (gatherxml::verbose_operation) {
        cout << "... found auxiliary latitude/longitude" << endl;
      }
      for (size_t n = 0; n < grid_data.lats.size(); ++n) {
        if (grid_data.lats[n].dim != grid_data.lons[n].dim) {
          log_error2("auxiliary latitude and longitude coordinate variables (" +
              itos(n) + ") do not have the same dimensions", F, "nc2xml", USER);
        }
        grid_dims.emplace_back();
        grid_dims.back().y = grid_data.lats[n].dim / 10000 - 1;
        grid_dims.back().x = (grid_data.lats[n].dim % 10000) / 100 - 1;
        grid_defs.emplace_back();
        NetCDF::VariableData vd;
        istream.variable_data(grid_data.lats[n].id, vd);
        grid_defs.back().slatitude = vd.front();
        auto nlats = vd.size();
        unique_ptr<double[]> lats(new double[nlats]);
        for (size_t m = 0; m < nlats; ++m) {
          lats[m] = vd[m];
        }
        istream.variable_data(grid_data.lons[n].id, vd);
        grid_defs.back().slongitude = vd.front();
        auto nlons = vd.size();
        unique_ptr<double[]> lons(new double[nlons]);
        for (size_t m = 0; m < nlons; ++m) {
          lons[m] = vd[m];
        }
        Grid::GridDimensions d;
        d.x = dims[grid_dims.back().x].length;
        d.y = dims[grid_dims.back().y].length;
        if (filled_grid_projection(lats, lons, d, grid_defs.back(), nlats,
            nlons)) {
          grid_dims.back() = d;
        } else {
          log_error2("unable to determine grid projection", F, "nc2xml", USER);
        }
      }
    }
  }
  if (grid_data.levdata.empty()) {

    // look for level coordinates that are not a coordinate variable
    if (gatherxml::verbose_operation) {
      cout << "looking for auxiliary level coordinates ..." << endl;
    }
    unordered_set<size_t> u;
    for (const auto& y : grid_data.lats) {
      if (y.dim > 100) {
        size_t m = y.dim / 10000 - 1;
        size_t l = (y.dim % 10000) / 100 - 1;
        if (grid_data.levels.empty() || (!grid_data.levels.empty() && grid_data.
            levels.back().dim != MISSING_FLAG)) {
          grid_data.levels.emplace_back();
          grid_data.levels.back().dim = MISSING_FLAG;
          grid_data.levdata.emplace_back();
          grid_data.levdata.back().write = false;
        }
        for (const auto& v : vars) {
          if (!v.is_coord && v.dimids.size() == 4 && v.dimids[0] == grid_data.
              time.dim && v.dimids[2] == m && v.dimids[3] == l) {

            // check netCDF variables for what they are using as a level
            //  dimension
            if (grid_data.levels.back().dim == MISSING_FLAG) {
              if (u.find(v.dimids[1]) == u.end()) {
                grid_data.levels.back().dim = v.dimids[1];
                u.emplace(grid_data.levels.back().dim);
              }
            } else if (grid_data.levels.back().dim != v.dimids[1]) {
              if (u.find(v.dimids[1]) == u.end()) {
                grid_data.levels.emplace_back();
                grid_data.levels.back().dim = v.dimids[1];
                u.emplace(grid_data.levels.back().dim);
                grid_data.levdata.emplace_back();
                grid_data.levdata.back().write = false;
              }
            }
          }
        }
      }
    }

    // pop any unused levels
    while (!grid_data.levels.empty() && grid_data.levels.back().dim ==
        MISSING_FLAG) {
      grid_data.levels.pop_back();
      grid_data.levdata.pop_back();
    }
    while (!grid_data.levdata.empty() && grid_data.levdata.back().ID.empty()) {
      grid_data.levdata.pop_back();
    }
    if (!grid_data.levels.empty() && grid_data.levdata.empty()) {
      log_error2("unable to determine the level coordinates", F, "nc2xml",
          USER);
    }
    if (gatherxml::verbose_operation) {
      cout << "... found " << grid_data.levels.size() << " auxiliary level "
          "coordinates" << endl;
    }
    if (!grid_data.levels.empty()) {
      for (size_t n = 0; n < grid_data.levels.size(); ++n) {
        for (const auto& v : vars) {
          if (!v.is_coord && v.dimids.size() == 1 && v.dimids[0] == grid_data.
              levels[n].dim) {
            grid_data.levels[n].id = v.name;
            grid_data.levels[n].type = v.nc_type;
            string d, u;
            for (const auto& a : v.attrs) {
              if (a.name == "description" && a.nc_type == NetCDF::NCType::
                  CHAR) {
                d = *(reinterpret_cast<string *>(a.values));
              } else if (a.name == "units" && a.nc_type == NetCDF::NCType::
                  CHAR) {
                u = *(reinterpret_cast<string *>(a.values));
              }
            }
            grid_data.levdata[n].ID = v.name + "@@" + u;
            if (d.empty()) {
              grid_data.levdata[n].description = v.name;
            } else {
              grid_data.levdata[n].description = d;
            }
            grid_data.levdata[n].units = u;
          }
        }
      }
    }
  }
  grid_data.levels.emplace_back();
  grid_data.levels.back().dim = MISSING_FLAG;
  grid_data.levels.back().id = "sfc";
  grid_data.levels.back().type = NetCDF::NCType::_NULL;
  grid_data.levdata.emplace_back();
  grid_data.levdata.back().ID = "sfc";
  grid_data.levdata.back().description = "Surface";
  grid_data.levdata.back().write = false;
  unordered_set<string> parameter_table;
  if (grid_data.lats.empty() || grid_data.lons.empty()) {
    log_error2("unable to determined horizontal coordinates", F, "nc2xml",
        USER);
  }
  if (gatherxml::verbose_operation) {
    cout << "found coordinates, ready to scan netCDF variables ..." << endl;
  }
  if (tr_table.empty()) {
    metautils::NcTime::TimeRangeEntry tre;
    tre.key = -1;

    // set key for climate model simulations
    if (source == "CAM") {
      tre.key = -11;
    }

    // get number of time steps and the temporal range
    NetCDF::VariableData vd;
    istream.variable_data(grid_data.time.id, vd);
    time_s.t1 = vd.front();
    time_s.t2 = vd.back();
    time_s.num_times = vd.size();
    if (g_inv.stream.is_open()) {
      time_s.times = new double[time_s.num_times];
      for (size_t m = 0; m < vd.size(); ++m) {
        time_s.times[m] = vd[m];
      }
    }
    string e;
    tre.instantaneous.first_valid_datetime = metautils::NcTime::
        actual_date_time(time_s.t1, time_data, e);
    if (!e.empty()) {
      log_error2(e, F, "nc2xml", USER);
    }
    tre.instantaneous.last_valid_datetime = metautils::NcTime::
        actual_date_time(time_s.t2, time_data, e);
    if (!e.empty()) {
      log_error2(e, F, "nc2xml", USER);
    }
    if (gatherxml::verbose_operation) {
      cout << "   ...setting temporal range to:" << endl;
      cout << "      " << tre.instantaneous.first_valid_datetime.
          to_string() << " to " << tre.instantaneous.last_valid_datetime.
          to_string() << endl;
    }
    tre.num_steps = vd.size();
    if (!grid_data.time_bounds.id.empty()) {
      process_time_bounds(istream, grid_data.time_bounds.id, tre);
    }
    if (time_data.units == "months" && tre.instantaneous.first_valid_datetime.
        day() == 1) {
      tre.instantaneous.last_valid_datetime.add_months(1);
    }
    tr_table.insert(tre);
  }
std::cerr << "TR table size=" << tr_table.size() << std::endl;
for (const auto& key : tr_table.keys()) {
std::cerr << static_cast<long long>(key) << std::endl;
}
  process_horizontal_grid(istream, grid_data, grid_dims, grid_defs);
  for (size_t z = 0; z < grid_data.levels.size(); ++z) {
    auto levid = grid_data.levdata[z].ID.substr(0, grid_data.levdata[z].ID.find(
        "@@"));
    if (gatherxml::verbose_operation) {
      cout << "Scanning netCDF variables for level '" << levid << "' ..." <<
          endl;
    }
    NetCDF::VariableData lvd;
    size_t nl;
    if (grid_data.levels[z].type == NetCDF::NCType::_NULL) {
      nl = 1;
    } else {
      istream.variable_data(levid, lvd);
      nl = lvd.size();
    }
    if (gatherxml::verbose_operation) {
      cout << "    Scanning " << tr_table.keys().size() << " time range(s) ..."
          << endl;
    }
    for (const auto& tr_key : tr_table.keys()) {
      metautils::NcTime::TimeRangeEntry tre;
      tr_table.found(tr_key, tre);
      for (size_t y = 0; y < grid_data.lats.size(); ++y) {
        vector<string> v;
        add_gridded_lat_lon_keys(v, grid_dims[y], grid_defs[y], grid_data.time.
            id, grid_data.time.dim, grid_data.levels[z].dim, grid_data.lats[y].
            dim, grid_data.lons[y].dim, tre);
        for (const auto& e : v) {
          grid_entry_key = e;
          auto idx = grid_entry_key.rfind("<!>");
          auto k = grid_entry_key.substr(idx + 3);
          if (g_inv.maps.U.find(k) == g_inv.maps.U.end()) {
            g_inv.maps.U.emplace(k, make_pair(g_inv.maps.U.size(), ""));
          }
          if (scan_data.grids->find(grid_entry_key) == scan_data.grids->end()) {

            // new grid
            grid_entry_ptr->level_table.clear();
            level_entry_ptr->parameter_code_table.clear();
            parameter_entry_ptr->num_time_steps = 0;
            add_gridded_parameters_to_netcdf_level_entry(vars, grid_entry_key,
                grid_data.time.id, grid_data.time.dim, grid_data.levels[z].dim,
                grid_data.lats[y].dim, grid_data.lons[y].dim, tre,
                parameter_table, scan_data);
            for (size_t n = 0; n < nl; ++n) {
              level_entry_key = metautils::args.dsid + "," + grid_data.levdata[
                  z].ID + ":";
              switch (grid_data.levels[z].type) {
                case NetCDF::NCType::INT: {
                  level_entry_key += itos(lvd[n]);
                  break;
                }
                case NetCDF::NCType::FLOAT:
                case NetCDF::NCType::DOUBLE: {
                  level_entry_key += ftos(lvd[n], floatutils::precision(lvd[n])
                      + 2);
                  break;
                }
                case NetCDF::NCType::_NULL: {
                  level_entry_key += "0";
                   break;
                }
                default: { }
              }
              if (!level_entry_ptr->parameter_code_table.empty()) {
                grid_entry_ptr->level_table.emplace(level_entry_key,
                    *level_entry_ptr);
                if (g_inv.stream.is_open()) {
                  add_level_to_inventory(level_entry_key, grid_entry_key,
                      grid_data.time.dim, grid_data.levels[z].dim, grid_data.
                      lats[y].dim, grid_data.lons[y].dim, istream);
                }
                grid_data.levdata[z].write = true;
              }
            }
            if (!grid_entry_ptr->level_table.empty()) {
              scan_data.grids->emplace(grid_entry_key, *grid_entry_ptr);
            }
          } else {

            // existing grid - needs update
            *grid_entry_ptr = (*scan_data.grids)[grid_entry_key];
            for (size_t n = 0; n < nl; ++n) {
              level_entry_key = metautils::args.dsid + "," + grid_data.levdata[
                  z].ID + ":";
              switch (grid_data.levels[z].type) {
                case NetCDF::NCType::INT: {
                  level_entry_key += itos(lvd[n]);
                  break;
                }
                case NetCDF::NCType::FLOAT:
                case NetCDF::NCType::DOUBLE: {
                  level_entry_key += ftos(lvd[n], floatutils::precision(lvd[n])
                      + 2);
                  break;
                }
                case NetCDF::NCType::_NULL: {
                  level_entry_key += "0";
                  break;
                }
                default: { }
              }
              if (grid_entry_ptr->level_table.find(level_entry_key) ==
                  grid_entry_ptr->level_table.end()) {
                level_entry_ptr->parameter_code_table.clear();
                add_gridded_parameters_to_netcdf_level_entry(vars,
                    grid_entry_key, grid_data.time.id, grid_data.time.dim,
                    grid_data.levels[z].dim, grid_data.lats[y].dim, grid_data.
                    lons[y].dim, tre, parameter_table, scan_data);
                if (!level_entry_ptr->parameter_code_table.empty()) {
                  grid_entry_ptr->level_table.emplace(level_entry_key,
                      *level_entry_ptr);
                  if (g_inv.stream.is_open()) {
                    add_level_to_inventory(level_entry_key, grid_entry_key,
                        grid_data.time.dim, grid_data.levels[z].dim, grid_data.
                        lats[y].dim, grid_data.lons[y].dim, istream);
                  }
                  grid_data.levdata[z].write = true;
                }
              } else {

                update_gridded_parameters_in_netcdf_level_entry(vars,
                    grid_data, y, z, tre, scan_data, parameter_table, istream);
              }
            }
            (*scan_data.grids)[grid_entry_key] = *grid_entry_ptr;
          }
        }
      }
    }
    if (gatherxml::verbose_operation) {
      cout << "    ... done scanning time ranges." << endl;
    }
    if (metautils::args.dsid != "test") {
      if (gatherxml::verbose_operation) {
        cout << "Writing level map..." << endl;
      }
      auto e = metautils::NcLevel::write_level_map(grid_data.levdata);
      if (!e.empty()) {
        log_error2(e, "metautils::NcLevel::write_level_map()", "nc2xml", USER);
      }
      if (gatherxml::verbose_operation) {
        cout << "...finished writing level map." << endl;
      }
    }
    if (gatherxml::verbose_operation) {
      cout << "... done scanning netCDF variables" << endl;
    }
  }
  if (scan_data.grids->empty()) {
    log_error2("no grids found - no content metadata will be generated", F,
         "nc2xml", USER);
  }
  scan_data.write_type = ScanData::GrML_type;
  delete[] time_s.times;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
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
  gatherxml::fileInventory::open(inv_file,&inv_dir,g_inv.stream,"GrML",F,"nc2xml",USER);
  for (n=0; n < gattrs.size(); ++n) {
    if (to_lower(gattrs[n].name) == "simulation_start_date") {
      break;
    }
  }
  if (n == attrs.size()) {
    metautils::log_error("does not appear to be a WRF Climate Simulation file",F,"nc2xml",USER);
  }
  tre.key=-11;
  auto dims=istream.dimensions();
  auto vars=istream.variables();
// find the coordinate variables
  for (n=0; n < vars.size(); ++n) {
    for (m=0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].is_coord) {
        if (vars[n].attrs[m].data_type == NetCDF::NCType::CHAR && vars[n].attrs[m].name == "description") {
          sdum=*(reinterpret_cast<string *>(vars[n].attrs[m].values));
          sdum=to_lower(sdum);
          if (regex_search(sdum,regex("since"))) {
            if (found_time) {
              metautils::log_error("time was already identified - don't know what to do with variable: "+vars[n].name,F,"nc2xml",USER);
            }
            fill_nc_time_data(vars[n].attrs[m]);
            timeid=vars[n].name;
            timedimid=vars[n].dimids[0];
            found_time=true;
            istream.variable_data(vars[n].name,var_data);
            time_s.num_times=var_data.size();
            string error;
            tre.data.reset(new metautils::NcTime::TimeRangeEntry::Data);
            tre.instantaneous.first_valid_datetime=metautils::NcTime::actual_date_time(var_data.front(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,F,"nc2xml",USER);
            }
            tre.instantaneous.last_valid_datetime=metautils::NcTime::actual_date_time(var_data.back(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,F,"nc2xml",USER);
            }
            if (g_inv.stream.is_open()) {
                time_s.times=new double[time_s.num_times];
                for (l=0; l < time_s.num_times; ++l) {
                  time_s.times[l]=var_data[l];
                }
            }
            tre.num_steps=time_s.num_times;
          }
        }
      } else {
        if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].data_type == NetCDF::NCType::CHAR) {
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
    metautils::log_error("scan_wrf_simulation_netcdf_file() could not find the time coordinate variable",F,"nc2xml",USER);
  }
  if (latdimid == 0 || londimid == 0) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() could not find the latitude and longitude coordinate variables",F,"nc2xml",USER);
  } else if (latdimid != londimid) {
    metautils::log_error("scan_wrf_simulation_netcdf_file() found latitude and longitude coordinate variables, but they do not have the same dimensions",F,"nc2xml",USER);
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
        metautils::log_error("scan_wrf_simulation_netcdf_file() was not able to deterimine the grid definition type",F,"nc2xml",USER);
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
    metautils::log_error("scan_wrf_simulation_netcdf_file() can't open "+tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml for writing netCDF levels",F,"nc2xml",USER);
  }
  if (!map_contents.empty()) {
    for (const auto& line : map_contents) {
      ofs << line << endl;
    }
  } else {
    ofs << "<?xml version=\"1.0\" ?>" << endl;
    ofs << "<levelMap>" << endl;
  }
  for (const auto& levdimid : levdimids) {
    add_gridded_lat_lon_keys(gentry_keys,dim,def,timeid,timedimid,levdimid,latdimid,londimid,tre);
    if (levdimid == -1) {
      if (map_contents.empty() || (!map_contents.empty() && level_map.is_layer("sfc") < 0)) {
        ofs << "  <level code=\"sfc\">" << endl;
        ofs << "    <description>Surface</description>" << endl;
        ofs << "  </level>" << endl;
      }
    } else {
      for (n=0; n < vars.size(); ++n) {
        if (vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid && (map_contents.empty() || (!map_contents.empty() && level_map.is_layer(vars[n].name) < 0))) {
          ofs << "  <level code=\"" << vars[n].name << "\">" << endl;
          for (m=0; m < vars[n].attrs.size(); ++m) {
            if (vars[n].attrs[m].data_type == NetCDF::NCType::CHAR) {
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
  mysystem2("/bin/cp "+tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml /glade/u/home/rdadata/share/metadata/LevelTables/",oss,ess);
  delete tmpfile;
  for (const auto& key : gentry_keys) {
    grid_entry_key=key;
    idx=grid_entry_key.rfind("<!>");
    auto U_key=grid_entry_key.substr(idx+3);
    if (g_inv.maps.U.find(U_key) == g_inv.maps.U.end()) {
      g_inv.maps.U.emplace(U_key,make_pair(g_inv.maps.U.size(),""));
    }
    if (!scan_data.grids->found(grid_entry_key,*grid_entry_ptr)) {
// new grid
      grid_entry_ptr->level_table.clear();
      level_entry_ptr->parameter_code_table.clear();
      parameter_entry_ptr->num_time_steps=0;
      for (const auto& levdimid : levdimids) {
        add_gridded_parameters_to_netcdf_level_entry(vars,grid_entry_key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
        if (!level_entry_ptr->parameter_code_table.empty()) {
          if (levdimid < 0) {
            level_entry_key="ds"+metautils::args.dsnum+",sfc:0";
            grid_entry_ptr->level_table.insert(*level_entry_ptr);
          } else {
            for (n=0; n < vars.size(); ++n) {
              if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
                istream.variable_data(vars[n].name,var_data);
                for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
                  level_entry_key="ds"+metautils::args.dsnum+","+vars[n].name+":";
                  switch (vars[n].data_type) {
                    case NetCDF::NCType::SHORT:
                    case NetCDF::NCType::INT:
                    {
                      level_entry_key+=itos(var_data[m]);
                      break;
                    }
                    case NetCDF::NCType::FLOAT:
                    case NetCDF::NCType::DOUBLE:
                    {
                      level_entry_key+=ftos(var_data[m],3);
                      break;
                    }
                    default:
                    {
                      metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),F,"nc2xml",USER);
                    }
                  }
                  grid_entry_ptr->level_table.insert(*level_entry_ptr);
                  if (g_inv.stream.is_open()) {
                    add_level_to_inventory(level_entry_key,grid_entry_key,timedimid,levdimid,latdimid,londimid,istream);
                  }
                }
              }
            }
          }
        }
      }
      if (!grid_entry_ptr->level_table.empty()) {
        scan_data.grids->insert(*grid_entry_ptr);
      }
    } else {
// existing grid - needs update
      for (const auto& levdimid : levdimids) {
        if (levdimid < 0) {
          level_entry_key="ds"+metautils::args.dsnum+",sfc:0";
          if (!grid_entry_ptr->level_table.found(level_entry_key,*level_entry_ptr)) {
            level_entry_ptr->parameter_code_table.clear();
            add_gridded_parameters_to_netcdf_level_entry(vars,grid_entry_key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
            if (!level_entry_ptr->parameter_code_table.empty()) {
              grid_entry_ptr->level_table.insert(*level_entry_ptr);
              if (g_inv.stream.is_open()) {
                add_level_to_inventory(level_entry_key,grid_entry_key,timedimid,levdimid,latdimid,londimid,istream);
              }
            }
          }
        } else {
          for (n=0; n < vars.size(); ++n) {
            if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
              if (!level_entry_ptr->parameter_code_table.empty()) {
                istream.variable_data(vars[n].name,var_data);
              }
              for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
                level_entry_key="ds"+metautils::args.dsnum+","+vars[n].name+":";
                switch (vars[n].data_type) {
                  case NetCDF::NCType::SHORT:
                  case NetCDF::NCType::INT:
                  {
                    level_entry_key+=itos(var_data[m]);
                    break;
                  }
                  case NetCDF::NCType::FLOAT:
                  case NetCDF::NCType::DOUBLE:
                  {
                    level_entry_key+=ftos(var_data[m],3);
                    break;
                  }
                  default:
                  {
                    metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),F,"nc2xml",USER);
                  }
                }
                if (!grid_entry_ptr->level_table.found(level_entry_key,*level_entry_ptr)) {
                  level_entry_ptr->parameter_code_table.clear();
                  add_gridded_parameters_to_netcdf_level_entry(vars,grid_entry_key,timeid,timedimid,levdimid,latdimid,londimid,found_map,tre,parameter_table,var_list,changed_var_table,parameter_map);
                  if (!level_entry_ptr->parameter_code_table.empty()) {
                    grid_entry_ptr->level_table.insert(*level_entry_ptr);
                    if (g_inv.stream.is_open()) {
                      add_level_to_inventory(level_entry_key,grid_entry_key,timedimid,levdimid,latdimid,londimid,istream);
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
  if (scan_data.grids->empty()) {
    metautils::log_error("No grids found - no content metadata will be generated",F,"nc2xml",USER);
  }
  write_type=GrML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function scan_wrf_simulation_netcdf_file() done." << endl;
  }
}
*/

void scan_cf_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  string feature_type, lft, platform;
  for (const auto& a : gattrs) {
    if (a.name == "featureType") {
      feature_type = *(reinterpret_cast<string *>(a.values));
      trim(feature_type);
      lft = to_lower(feature_type);
    } else if (a.name == "platform") {
      platform = *(reinterpret_cast<string *>(a.values));
      trim(platform);
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
          F, "nc2xml", USER);
    }
    scan_data.map_name += ".p";
  }
  if (!feature_type.empty()) {
    if (!scan_data.map_name.empty() && scan_data.datatype_map.fill(scan_data.
        map_name)) {
      scan_data.map_filled = true;
    }
    string platform_type = "unknown";
    if (!platform.empty()) {
      Server server(metautils::directives.metadb_config);
      if (server) {
        LocalQuery query("obml_platform_type", "search.gcmd_platforms", "path "
            "= '" + platform + "'");
        if (query.submit(server) == 0) {
          Row row;
          if (query.fetch_row(row)) {
            platform_type = row[0];
          }
        }
        server.disconnect();
      }
    }
    DiscreteGeometriesData dgd;
    process_variable_attributes(dgd);
    if (lft == "point") {
      scan_cf_point_netcdf_file(istream, platform_type, dgd, scan_data,
          obs_data);
    } else if (lft == "timeseries") {
      scan_cf_time_series_netcdf_file(istream, platform_type, dgd, scan_data,
          obs_data);
    } else if (lft == "trajectory") {
      scan_cf_trajectory_netcdf_file(istream, platform_type, dgd, scan_data,
          obs_data);
    } else if (lft == "profile") {
      scan_cf_profile_netcdf_file(istream, platform_type, dgd, scan_data,
          obs_data);
    } else if (lft == "timeseriesprofile") {
      scan_cf_time_series_profile_netcdf_file(istream, platform_type, dgd,
          scan_data, obs_data);
    } else {
      log_error2("featureType '" + feature_type + "' not recognized", F,
          "nc2xml", USER);
    }
  } else {
    if (!scan_data.map_name.empty() && scan_data.parameter_map.fill(scan_data.
        map_name)) {
      scan_data.map_filled = true;
    }
    scan_cf_grid_netcdf_file(istream, scan_data);
  }
}

void scan_raf_aircraft_netcdf_file(InputNetCDFStream& istream, ScanData&
     scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  string otyp, ptyp;
  DateTime ref;
  unordered_set<string> cset;
  vector<string> dtypv;
  for (const auto& a : gattrs) {
    if (a.name == "Aircraft") {
      otyp = "upper_air";
      ptyp = "aircraft";
      auto v = *(reinterpret_cast<string *>(a.values));
      auto sp = split(v);
      ientry.key = ptyp + "[!]callSign[!]" + metautils::clean_id(sp.front());
    } else if (a.name == "coordinates") {
      auto v = *(reinterpret_cast<string *>(a.values));
      auto sp = split(v);
      for (const auto& s : sp) {
        cset.emplace(s);
      }
    }
  }
  if (!otyp.empty()) {
    if (cset.empty()) {
      log_error2("unable to determine variable coordinates", F, "nc2xml", USER);
    }
  } else {
    log_error2("file does not appear to be NCAR-RAF/nimbus compliant netCDF", F,
        "nc2xml", USER);
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled = true;
  }
  NetCDF::VariableData tvd, yvd, xvd, avd;
  NetCDFVariableAttributeData ltad, lnad;
  string tvar, tu, vu;
  for (const auto& v : vars) {
    auto nodata = false;
    string ln, u;
    for (const auto& a : v.attrs) {
      if (a.name == "long_name") {
        auto s = *(reinterpret_cast<string *>(a.values));
        if (a.name == "long_name") {
          ln = s;
        }
        s = to_lower(s);
        if (regex_search(s, regex("time")) && tvar.empty()) {
          tvar = v.name;
          istream.variable_data(v.name, tvd);
        } else if (regex_search(s, regex("latitude")) || regex_search(s, regex(
            "longitude")) || regex_search(s, regex("altitude"))) {
          nodata = true;
        }
      } else if (a.name == "units") {
        u = *(reinterpret_cast<string *>(a.values));
      }
      if (v.name == tvar && a.name == "units") {
        auto s = *(reinterpret_cast<string *>(a.values));
        auto sp = split(s);
        if (sp.size() < 4 || sp[1] != "since") {
          log_error2("bad units '" + s + "' on time variable", F, "nc2xml",
              USER);
        }
        tu = sp[0];
        auto sp2 = split(sp[2], "-");
        if (sp2.size() != 3) {
          log_error2("bad date in time variable units '" + s + "'", F, "nc2xml",
              USER);
        }
        auto y = stoi(sp2[0]);
        auto m = stoi(sp2[1]);
        auto d = stoi(sp2[2]);
        auto sp3 = split(sp[3], ":");
        if (sp3.size() != 3) {
          log_error2("bad time in time variable units '" + s + "'", F, "nc2xml",
              USER);
        }
        auto t = stoi(sp3[0]) * 10000 + stoi(sp3[1]) * 100 + stoi(sp3[2]);
        ref.set(y, m, d, t);
      }
    }
    auto l = to_lower(v.name);
    if (regex_search(l, regex("lat")) && cset.find(v.name) != cset.end()) {
      istream.variable_data(v.name, yvd);
      extract_from_variable_attribute(v.attrs, v.nc_type, ltad, scan_data.
          conventions);
    } else if (regex_search(l, regex("lon")) && cset.find(v.name) != cset.
        end()) {
      istream.variable_data(v.name, xvd);
      extract_from_variable_attribute(v.attrs, v.nc_type, lnad, scan_data.
          conventions);
    } else if (regex_search(l, regex("alt")) && cset.find(v.name) != cset.
        end()) {
      for (const auto& a : v.attrs) {
        if (a.name == "units") {
          vu = *(reinterpret_cast<string *>(a.values));
        }
      }
      istream.variable_data(v.name, avd);
    } else if (!nodata && cset.find(v.name) == cset.end()) {
      auto s = v.name + "<!>" + ln + "<!>" + u + "<!>";
      if (!u.empty()) {
        s += "<nobr>" + u + "</nobr>";
      }
      dtypv.emplace_back(s);
    }
  }
  size_t x = 0;
  if (tu == "seconds") {
    x = 1;
  } else if (tu == "minutes") {
    x = 60;
  } else if (tu == "hours") {
    x = 3600;
  }
  if (x == 0) {
    log_error2("bad time units '" + tu + "' on time variable", F, "nc2xml",
        USER);
  }
  double mx=-99999., mn=999999.;
//  auto ignore_altitude = false;
  for (size_t n = 0; n < tvd.size(); ++n) {
//    if (static_cast<double>(yvd[n]) != ltad.missing_value.get() && static_cast<double>(xvd[n]) != lnad.missing_value.get()) {
if (yvd[n] >= -90. && yvd[n] <= 90. && xvd[n] >= -180. && xvd[n] <= 180.) {
      if (!obs_data.added_to_platforms(otyp, ptyp, yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + otyp + " " + ptyp, F,
            "nc2xml", USER);
      }
      auto dt = ref.seconds_added(tvd[n] * x);
      auto nm = 0;
      for (auto t : dtypv) {
        t = t.substr(0, t.find("<!>"));
        auto fv = istream.variable(t)._FillValue.get();
        for (const auto& v : istream.value_at(t, n)) {
          if (v != fv) {
            fv = v;
            break;
          }
        }
        if (fv != istream.variable(t)._FillValue.get()) {
          if (!obs_data.added_to_ids(otyp, ientry, t, "", yvd[n], xvd[n],
              tvd[n], &dt)) {
            auto e = move(myerror);
            log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml", USER);
          }
          nm = 1;
        }
      }
      scan_data.num_not_missing += nm;
      if (!avd.empty()) {
        if (avd[n] > mx) {
          mx = avd[n];
        }
        if (avd[n] < mn) {
          mn = avd[n];
        }
/*
      } else {
        ignore_altitude=true;
*/
      }
    }
  }
/*
  for (const auto& t : dtypv) {
    gatherxml::markup::ObML::DataTypeEntry dte;
    dte.key=t.substr(0,t.find("<!>"));
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
      if (mx > dte.data->vdata->max_altitude) {
        dte.data->vdata->max_altitude=mx;
      }
      if (mn < dte.data->vdata->min_altitude) {
        dte.data->vdata->min_altitude=mn;
      }
      dte.data->vdata->avg_nlev+=ientry.data->nsteps;
      dte.data->vdata->units=vu;
    }
    auto descr=scan_data.datatype_map.description(dte.key);
    if (descr.empty()) {
      if (find(scan_data.netcdf_variables.begin(),scan_data.netcdf_variables.end(),t) == scan_data.netcdf_variables.end()) {
        scan_data.netcdf_variables.emplace_back(t);
      }
    }
  }
*/
  scan_data.write_type = ScanData::ObML_type;
}

void set_time_missing_value(NetCDF::DataValue& time_miss_val, const vector<
    NetCDF::Attribute>& attr, size_t index, NetCDF::NCType time_type) {
  static const string F = this_function_label(__func__);
  time_miss_val.resize(time_type);
  switch (time_type) {
    case NetCDF::NCType::INT: {
      time_miss_val.set(*(reinterpret_cast<int *>(attr[index].values)));
      break;
    }
    case NetCDF::NCType::FLOAT: {
      time_miss_val.set(*(reinterpret_cast<float *>(attr[index].values)));
      break;
    }
    case NetCDF::NCType::DOUBLE: {
      time_miss_val.set(*(reinterpret_cast<double *>(attr[index].values)));
      break;
    }
    default: {
      log_error2("unrecognized time type: " + itos(static_cast<int>(time_type)),
          F, "nc2xml", USER);
    }
  }
}

void scan_npn_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  size_t idd=0, ilen=0, ld=0, hd=0;
  for (const auto& v : vars) {
    if (v.name == "id") {
      idd = v.dimids.front();
      ilen = dims[v.dimids.back()].length;
    } else if (v.name == "low_level") {
      ld = v.dimids.front();
    } else if (v.name == "high_level") {
      hd = v.dimids.front();
    }
  }
  NetCDF::VariableData ids;
  istream.variable_data("id", ids);
  if (ids.empty()) {
    log_error2("station ID variable could not be identified", F, "nc2xml",
        USER);
  }
  vector<string> sv;
  size_t end = ids.size() / ilen;
  for (size_t n = 0; n < end; ++n) {
    auto c = reinterpret_cast<char *>(ids.get());
    if (c[n * ilen] == NetCDF::CHAR_NOT_SET) {
      sv.emplace_back("");
    } else {
      sv.emplace_back(&c[n * ilen], ilen);
      trim(sv.back());
    }
  }
  NetCDF::VariableData yvd, xvd, lvd, hvd;
  istream.variable_data("lat", yvd);
  if (yvd.empty()) {
    log_error2("latitude variable could not be identified", F, "nc2xml", USER);
  }
  istream.variable_data("lon", xvd);
  if (xvd.empty()) {
    log_error2("longitude variable could not be identified", F, "nc2xml", USER);
  }
  istream.variable_data("low_level", lvd);
  if (lvd.empty()) {
    log_error2("'low_level' variable could not be identified", F, "nc2xml",
        USER);
  }
  istream.variable_data("high_level", hvd);
  if (hvd.empty()) {
    log_error2("'high_level' variable could not be identified", F, "nc2xml",
        USER);
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled = true;
  }
  short y, m, d;
  size_t t;
  NetCDF::VariableData vd;
  istream.variable_data("year", vd);
  y = vd.front();
  istream.variable_data("month", vd);
  m = vd.front();
  istream.variable_data("day", vd);
  d = vd.front();
  istream.variable_data("hour", vd);
  t = vd.front() * 10000;
  istream.variable_data("minute", vd);
  t += vd.front() * 100 + 99;
  DateTime dt(y, m, d, t, 0);
  auto ts = dt.minutes_since(DateTime(1950, 1, 1, 0, 0));
  regex azim_re("^azim"), elev_re("^elev");
  for (const auto& v : vars) {
    if (!v.dimids.empty() && v.dimids.front() == idd) {
      string otyp;
      if (v.dimids.size() == 1 && v.name != "lat" && v.name != "lon" &&
          !regex_search(v.name, azim_re) && !regex_search(v.name, elev_re)) {
        otyp = "surface";
      } else if (v.dimids.size() == 2 && (v.dimids.back() == ld || v.dimids.
          back() == hd)) {
        otyp = "upper_air";
      }
      if (!otyp.empty()) {
        istream.variable_data(v.name, vd);
        const NetCDF::VariableData& lref = v.dimids.back() == ld ? lvd : hvd;
        for (size_t n = 0; n < sv.size(); ++n) {
          if (!sv[n].empty()) {
            vector<double> lv;
            auto nm = false;
            if (vd.size() == sv.size()) {
              if (vd[n] > -1.e30 && vd[n] < 1.e30) {
                nm = true;
              }
            } else {
              size_t nl = vd.size() / sv.size();
              size_t end = (n + 1) * nl;
              for (size_t m = n * nl, l=0; m < end; ++m) {
                if (vd[m] > -1.e30 && vd[m] < 1.e30) {
                  nm = true;
                  lv.emplace_back(lref[l]);
                }
                ++l;
              }
            }
            if (nm) {
              if (!obs_data.added_to_platforms(otyp, "wind_profiler", yvd[n],
                  -xvd[n])) {
                auto e = move(myerror);
                log_error2(e + "' when adding platform " + otyp, F, "nc2xml",
                    USER);
              }
              ientry.key = "wind_profiler[!]NOAA[!]" + sv[n];
              if (!obs_data.added_to_ids(otyp, ientry, v.name, "", yvd[n], -xvd[
                  n], ts, &dt)) {
                auto e = move(myerror);
                log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                    USER);
              }
              gatherxml::markup::ObML::DataTypeEntry dte;
              if (otyp == "upper_air") {
                ientry.data->data_types_table.found(v.name, dte);
                fill_vertical_resolution_data(lv, "up", "m", dte);
              }
              ++scan_data.num_not_missing;
            }
          }
        }
        auto d = scan_data.datatype_map.description(v.name);
        if (d.empty()) {
          NetCDFVariableAttributeData ad;
          extract_from_variable_attribute(v.attrs, vd.type(), ad, scan_data.
              conventions);
          if (ad.units.length() == 1 && ad.units.front() == 0x1) {
            ad.units = "";
          }
          add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.
              units + "<!>" + ad.cf_keyword, scan_data);
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

struct Header {
  Header() : type(), ID(), valid_time(), lat(0.), lon(0.), elev(0.) { }

  string type, ID, valid_time;
  float lat, lon, elev;
};

void scan_prepbufr_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  string s = "";
  for (const auto& a : gattrs) {
    if (a.name == "MET_tool") {
      s = *(reinterpret_cast<string *>(a.values));
    } else if (a.name == "FileOrigins") {
      s = *(reinterpret_cast<string *>(a.values));
      if (regex_search(s, regex("PB2NC tool"))) {
        s = "pb2nc";
      } else {
        s = "";
      }
    }
  }
  if (s != "pb2nc") {
    log_error2("missing global attribute 'MET_tool' or invalid value", F,
        "nc2xml", USER);
  }
  unique_ptr<Header[]> hds;
  size_t nhds = 0;
  for (const auto& d : dims) {
    if (d.name == "nhdr") {
      nhds = d.length;
      hds.reset(new Header[nhds]);
    }
  }
  if (!hds) {
    log_error2("could not locate 'nhdr' dimension", F, "nc2xml", USER);
  }
  NetCDF::VariableData vd;
  if (istream.variable_data("hdr_arr", vd) == NetCDF::NCType::_NULL) {
    log_error2("could not get 'hdr_arr' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].lat=vd[n * 3];
    hds[n].lon=vd[n * 3 + 1];
    hds[n].elev=vd[n * 3 + 2];
  }
  if (istream.variable_data("hdr_typ", vd) == NetCDF::NCType::_NULL) {
    log_error2("could not get 'hdr_typ' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].type.assign(&(reinterpret_cast<char *>(vd.get()))[n * 16], 16);
  }
  if (istream.variable_data("hdr_sid", vd) == NetCDF::NCType::_NULL) {
    log_error2("could not get 'hdr_sid' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].ID.assign(&(reinterpret_cast<char *>(vd.get()))[n * 16], 16);
  }
  if (istream.variable_data("hdr_vld", vd) == NetCDF::NCType::_NULL) {
    log_error2("could not get 'hdr_vld' data", F, "nc2xml", USER);
  }
  DateTime base(30000101235959), dt;
  for (size_t n = 0; n < nhds; ++n) {
      hds[n].valid_time.assign(&(reinterpret_cast<char *>(vd.get()))[n * 16],
          16);
      if (hds[n].valid_time.empty()) {
        log_error2("empty value in 'hdr_vld' at element " + itos(n), F,
        "nc2xml", USER);
      }
      auto s = substitute(hds[n].valid_time, "_", "");
      dt.set(stoll(s));
      if (dt < base) {
        base = dt;
      }
  }
  if (istream.num_records() == 0) {
    log_error2("no data records found", F, "nc2xml", USER);
  }
  if (istream.variable_data("obs_arr", vd) == NetCDF::NCType::_NULL) {
    log_error2("could not get 'obs_arr' data", F, "nc2xml", USER);
  }
  string otyp, ptyp;
  for (size_t n = 0; n < istream.num_records(); ++n) {
    if (vd[n * 5 + 4] > -9999.) {
      ++scan_data.num_not_missing;
      auto idx = vd[n * 5];
      if (hds[idx].type == "ADPUPA") {
        otyp = "upper_air";
        ptyp = "land_station";
      } else if (hds[idx].type == "AIRCAR" || hds[idx].type == "AIRCFT") {
        otyp = "upper_air";
        ptyp = "aircraft";
      } else if (hds[idx].type == "SATEMP" || hds[idx].type == "SATWND") {
        otyp = "upper_air";
        ptyp = "satellite";
      } else if (hds[idx].type == "PROFLR" || hds[idx].type == "RASSDA" || hds[
          idx].type == "VADWND") {
        otyp = "upper_air";
        ptyp = "wind_profiler";
      } else if (hds[idx].type == "SPSSMI") {
        ptyp = "satellite";
      } else if (hds[idx].type == "ADPSFC") {
        otyp = "surface";
        ptyp = "land_station";
      } else if (hds[idx].type == "SFCSHP") {
        otyp = "surface";
        if (strutils::is_numeric(hds[idx].ID) && hds[idx].ID.length() == 5 &&
            hds[idx].ID >= "99000") {
          ptyp = "fixed_ship";
        } else {
          ptyp = "roving_ship";
        }
      } else if (hds[idx].type == "ASCATW" || hds[idx].type == "ERS1DA" || hds[
          idx].type == "QKSWND" || hds[idx].type == "SYNDAT" || hds[idx].type ==
          "WDSATR") {
        otyp = "surface";
        ptyp = "satellite";
      } else if (hds[idx].type == "SFCBOG") {
        otyp = "surface";
        ptyp = "bogus";
      } else {
        log_error2("unknown observation type '" + hds[idx].type + "'", F,
            "nc2xml", USER);
      }
      ientry.key = prepbufr_id_key(metautils::clean_id(hds[idx].ID), ptyp, hds[
          idx].type);
      if (ientry.key.empty()) {
        log_error2("unable to get ID key for '" + hds[idx].type + "', ID: '" +
            hds[idx].ID + "'", F, "nc2xml", USER);
      }
      if (hds[idx].type == "SPSSMI") {
        if (ientry.key.back() == 'A' || ientry.key.back() == 'M' || ientry.key.
           back() == 'S' || ientry.key.back() == 'U') {
          otyp = "surface";
        } else {
          otyp = "upper_air";
        }
      }
      if (otyp.empty()) {
        log_error2("unable to get observation type for '" + hds[idx].type +
            "', ID: '" + hds[idx].ID + "'", F, "nc2xml", USER);
      }
      auto dtyp = ftos(vd[n * 5 + 1]);
      auto s = substitute(hds[idx].valid_time, "_", "");
      dt.set(stoll(s));
      if (!obs_data.added_to_ids(otyp, ientry, dtyp, "", hds[idx].lat, hds[idx].
          lon, dt.seconds_since(base), &dt)) {
        auto e = move(myerror);
        log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml", USER);
      }
      if (!obs_data.added_to_platforms(otyp, ptyp, hds[idx].lat, hds[idx].
          lon)) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + otyp + " " + ptyp, F,
            "nc2xml", USER);
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_idd_metar_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, rvd, pvd;
  string platform_type = "land_station";
  if (istream.variable_data("latitude", yvd) == NetCDF::NCType::_NULL) {
    if (istream.variable_data("lat", yvd) == NetCDF::NCType::_NULL) {
      log_error2("variable 'latitude' not found", F, "nc2xml", USER);
    }
  }
  if (istream.variable_data("longitude", xvd) == NetCDF::NCType::_NULL) {
    if (istream.variable_data("lon", xvd) == NetCDF::NCType::_NULL) {
      log_error2("variable 'longitude' not found", F, "nc2xml", USER);
    }
  }
  if (istream.variable_data("time_observation", tvd) == NetCDF::NCType::
      _NULL) {
    if (istream.variable_data("time_obs", tvd) == NetCDF::NCType::_NULL) {
      log_error2("variable 'time_observation' not found", F, "nc2xml", USER);
    }
  }
  int fmt = -1;
  if (istream.variable_data("report_id", rvd) == NetCDF::NCType::_NULL) {
    if (istream.variable_data("stn_name", rvd) == NetCDF::NCType::_NULL) {
      log_error2("variable 'report_id' not found", F, "nc2xml", USER);
    } else {
      fmt = 0;
    }
  } else {
    if (istream.variable_data("parent_index", pvd) == NetCDF::NCType::_NULL) {
      log_error2("variable 'parent_index' not found", F, "nc2xml", USER);
    }
    fmt = 1;
  }
  if (tvd.empty()) {
    return;
  }
  NetCDF::DataValue tfv;
  auto ilen = rvd.size() / tvd.size();
  for (const auto& v : vars) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << v.name << "'" << endl;
    }
    if (regex_search(v.name, regex("^time_obs"))) {
      if (gatherxml::verbose_operation) {
        cout << "  - time variable is '" << v.name << "'" << endl;
      }
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (gatherxml::verbose_operation) {
          cout << "    found attribute: '" << v.attrs[m].name << "'" << endl;
        }
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m].to_string());
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.is_rec && v.name != "parent_index" && v.name != "prevChild" &&
        !regex_search(v.name, regex("^report")) && v.name != "rep_type" && v.
        name != "stn_name" && v.name != "wmo_id" && v.name != "lat" && v.name !=
        "lon" && v.name != "elev" && !regex_search(v.name, regex("^ob\\_")) &&
        !regex_search(v.name, regex("^time")) && v.name != "xfields" && v.name
        != "remarks") {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad, scan_data.conventions);
      if (gatherxml::verbose_operation) {
        cout << "    - attributes extracted" << endl;
      }
      auto nm = 0;
      for (size_t m = 0; m < tvd.size(); ++m) {
        if (!found_missing(tvd[m], &tfv, vd[m], ad.missing_value)) {
          ++nm;
          ++scan_data.num_not_missing;
          string id(&(reinterpret_cast<char *>(rvd.get()))[m * ilen], ilen);
          trim(id);
          auto i = fmt == 0 ? m : pvd[m];
          if (i < static_cast<int>(tvd.size())) {
            auto dt = compute_nc_time(tvd, m);
            ientry.key = platform_type + "[!]callSign[!]" + metautils::clean_id(
                id);
            auto d = scan_data.datatype_map.description(v.name);
            if (d.empty()) {
              add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" +
                  ad.units + "<!>" + ad.cf_keyword, scan_data);
            }
            if (yvd[i] >= -90. && yvd[i] <= 90. && xvd[i] >= -180. && xvd[i] <=
                180.) {
              if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[i],
                  xvd[i], tvd[m], &dt)) {
                auto e = move(myerror);
                log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                    USER);
              }
              if (!obs_data.added_to_platforms("surface", platform_type, yvd[i],
                  xvd[i])) {
                auto e = move(myerror);
                log_error2(e + "' when adding platform " + platform_type, F,
                    "nc2xml", USER);
              }
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << nm << endl;
      }
    }
  }
}

void scan_idd_buoy_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, shvd, bvd, svd;
  if (istream.variable_data("Lat", yvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'Lat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("Lon", xvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'Lon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("time_obs", tvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'time_obs' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("ship", shvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'ship' not found", F, "nc2xml", USER);
  }
  if (tvd.empty()) {
    return;
  }
  if (istream.variable_data("buoy", bvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'buoy' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("stnType", svd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'stnType' not found", F, "nc2xml", USER);
  }
  auto ilen = shvd.size() / tvd.size();
  NetCDF::DataValue tfv;
  for (const auto& v : vars) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << v.name << "'" << endl;
    }
    if (v.name == "time_obs") {
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m].to_string());
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.is_rec && v.name != "rep_type" && v.name != "zone" && v.name !=
        "buoy" && v.name != "ship" && !regex_search(v.name, regex("^time")) &&
        v.name != "Lat" && v.name != "Lon" && v.name != "stnType" && v.name !=
        "report") {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad, scan_data.conventions);
      auto nm = 0;
      for (size_t m = 0; m < tvd.size(); ++m) {
        if (!found_missing(tvd[m], &tfv, vd[m], ad.missing_value)) {
          ++nm;
          ++scan_data.num_not_missing;
          string id(&(reinterpret_cast<char *>(shvd.get()))[m * ilen], ilen);
          trim(id);
          string platform_type;
          if (!id.empty()) {
            if (svd[m] == 6.) {
              platform_type = "drifting_buoy";
            } else {
              platform_type = "roving_ship";
            }
            ientry.key = platform_type + "[!]callSign[!]" + metautils::clean_id(
                id);
          } else {
            platform_type = "drifting_buoy";
            ientry.key = platform_type + "[!]other[!]" + itos(bvd[m]);
          }
          auto dt = compute_nc_time(tvd, m);
          auto d = scan_data.datatype_map.description(v.name);
          if (d.empty()) {
            add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.
                units + "<!>" + ad.cf_keyword, scan_data);
          }
          if (yvd[m] >= -90. && yvd[m] <= 90. && xvd[m] >= -180. && xvd[m] <=
              180.) {
            if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[m],
                xvd[m], tvd[m], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            if (!obs_data.added_to_platforms("surface", platform_type, yvd[m],
                xvd[m])) {
              auto e = move(myerror);
              log_error2(e + "' when adding platform " + platform_type, F,
                  "nc2xml", USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << nm << endl;
      }
    }
  }
}

void scan_idd_surface_synoptic_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, wvd;
  if (istream.variable_data("Lat", yvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'Lat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("Lon", xvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'Lon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("time_obs",  tvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'time_obs' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("wmoId",  wvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'wmoId' not found", F, "nc2xml", USER);
  }
  NetCDF::DataValue tfv;
  for (const auto& v : vars) {
    if (v.name == "time_obs") {
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m].to_string());
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.is_rec && v.name != "rep_type" && v.name != "wmoId" && v.name
        != "stnName" && !regex_search(v.name, regex("^time")) && v.name != "Lat"
        && v.name != "Lon" && v.name != "elev" && v.name != "stnType" && v.name
        != "report") {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad, scan_data.conventions);
      auto nm = 0;
      for (size_t m = 0; m < tvd.size(); ++m) {
        if (!found_missing(tvd[m], &tfv, vd[m], ad.missing_value)) {
          ++nm;
          ++scan_data.num_not_missing;
          string platform_type;
          if ( wvd[m] < 99000) {
            platform_type = "land_station";
          } else {
            platform_type = "fixed_ship";
          }
          ientry.key = platform_type + "[!]WMO[!]" + ftos( wvd[m], 5, 0,
              '0');
          auto dt = compute_nc_time(tvd, m);
          auto d = scan_data.datatype_map.description(v.name);
          if (d.empty()) {
            add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.
                units + "<!>" + ad.cf_keyword, scan_data);
          }
          if (yvd[m] >= -90. && yvd[m] <= 90. && xvd[m] >= -180. && xvd[m] <=
              180.) {
            if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[m],
                xvd[m], tvd[m], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            if (!obs_data.added_to_platforms("surface", platform_type, yvd[m],                  xvd[m])) {
              auto e = move(myerror);
              log_error2(e + "' when adding platform " + platform_type, F,
                  "nc2xml", USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << nm << endl;
      }
    }
  }
}

void scan_idd_upper_air_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  NetCDF::VariableData tvd, yvd, xvd, wvd, svd;
  if (istream.variable_data("staLat", yvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'staLat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("staLon", xvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'staLon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("synTime", tvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'synTime' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("wmoStaNum", wvd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'wmoStaNum' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("staName", svd) == NetCDF::NCType::_NULL) {
    log_error2("variable 'staName' not found", F, "nc2xml", USER);
  }
  if (tvd.empty()) {
    return;
  }
  auto ilen = svd.size() / tvd.size();
  NetCDF::DataValue tfv;
  for (const auto& v : vars) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << v.name << "'" << endl;
    }
    NetCDFVariableAttributeData nc_wmoid_a_data;
    if (v.name == "synTime") {
      if (gatherxml::verbose_operation) {
        cout << "  - time variable is '" << v.name << "'; " << v.attrs.size() <<
            " attributes; type: " << static_cast<int>(tvd.type()) << endl;
      }
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (gatherxml::verbose_operation) {
          cout << "    found attribute: '" << v.attrs[m].name << "'" << endl;
        }
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m].to_string());
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.name == "wmoStaNum") {
      auto a = v.attrs;
      extract_from_variable_attribute(a, wvd.type(), nc_wmoid_a_data, scan_data.
          conventions);
    } else if (v.is_rec && (v.name == "numMand" || v.name == "numSigT" || v.name
        == "numSigW" || v.name == "numMwnd" || v.name == "numTrop")) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad, scan_data.conventions);
      if (gatherxml::verbose_operation) {
        cout << "    - attributes extracted" << endl;
      }
      auto nm = 0;
      for (size_t m = 0; m < tvd.size(); ++m) {
        if (!found_missing(tvd[m], &tfv, vd[m], ad.missing_value)) {
          ++nm;
          ++scan_data.num_not_missing;
          string platform_type;
          if (wvd[m] < 99000 || wvd[m] > 99900) {
            platform_type = "land_station";
          } else {
            platform_type = "fixed_ship";
          }
          if (nc_wmoid_a_data.missing_value.type() != NetCDF::NCType::_NULL) {
            if (wvd[m] == nc_wmoid_a_data.missing_value.get()) {
              string id(&(reinterpret_cast<char *>(svd.get()))[m * ilen], ilen);
              trim(id);
              ientry.key = platform_type + "[!]callSign[!]" + metautils::
                  clean_id(id);
            } else {
              ientry.key = platform_type + "[!]WMO[!]" + ftos(wvd[m], 5, 0,
                  '0');
            }
          } else {
            ientry.key = platform_type + "[!]WMO[!]" + ftos(wvd[m], 5, 0, '0');
          }
          auto dt = compute_nc_time(tvd, m);
          auto d = scan_data.datatype_map.description(v.name);
          if (d.empty()) {
            add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.
                units + "<!>" + ad.cf_keyword, scan_data);
          }
          if (yvd[m] >= -90. && yvd[m] <= 90. && xvd[m] >= -180. && xvd[m] <=
              180.) {
            if (!obs_data.added_to_ids("upper_air", ientry, v.name, "", yvd[m],
                xvd[m], tvd[m], &dt)) {
              auto e = move(myerror);
              log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml",
                  USER);
            }
            if (!obs_data.added_to_platforms("upper_air", platform_type, yvd[m],
                xvd[m])) {
              auto e = move(myerror);
              log_error2(e + "' when adding platform " + platform_type, F,
                  "nc2xml", USER);
            }
          }
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "    - variable data scanned" << endl;
        cout << "    - # of non-missing values: " << nm << endl;
      }
    }
  }
}

void scan_idd_observation_netcdf_file(InputNetCDFStream& istream, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  string typ;
  for (const auto& a : gattrs) {
    if (a.name == "title") {
      auto s = *(reinterpret_cast<string *>(a.values));
      auto sp = split(s);
      typ = sp.front();
    }
  }
  if (scan_data.datatype_map.fill(scan_data.map_name)) {
    scan_data.map_filled = true;
  }
  if (typ == "METAR") {
    scan_idd_metar_netcdf_file(istream, scan_data, obs_data);
  } else if (typ == "BUOY") {
    scan_idd_buoy_netcdf_file(istream, scan_data, obs_data);
  } else if (typ == "SYNOPTIC") {
    scan_idd_surface_synoptic_netcdf_file(istream, scan_data, obs_data);
  } else {
    scan_idd_upper_air_netcdf_file(istream, scan_data, obs_data);
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_samos_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  string ptyp = "roving_ship";
  ientry.key = "";
  for (const auto& a : gattrs) {
    if (a.name == "ID") {
      ientry.key = ptyp + "[!]callSign[!]" + *(reinterpret_cast<string *>(a.
          values));
    }
  }

  // find the coordinate variables
  string tvn, yvn, xvn;
  NetCDF::VariableData tvd, yvd, xvd;
  auto tm_b = false, lt_b = false, ln_b = false;
  for (const auto& v : vars) {
    for (const auto& a : v.attrs) {
      if (a.nc_type == NetCDF::NCType::CHAR && a.name == "units") {
        auto s = to_lower(*(reinterpret_cast<string *>(a.values)));
        if (v.is_coord && regex_search(s, regex("since"))) {
          if (tm_b) {
            log_error2("time was already identified - don't know what to do "
                "with variable: " + v.name, F, "nc2xml", USER);
          }
          fill_nc_time_data(a.to_string());
          tm_b = true;
          tvn = v.name;
          if (istream.variable_data(v.name, tvd) == NetCDF::NCType::_NULL) {
            log_error2("unable to get tvd", F, "nc2xml", USER);
          }
          if (tvd.empty()) {
            tm_b = false;
          }
        } else if (v.dimids.size() == 1 && v.dimids[0] == 0) {
          if (s == "degrees_north") {
            if (istream.variable_data(v.name, yvd) == NetCDF::NCType::_NULL) {
              log_error2("unable to get latitudes", F, "nc2xml", USER);
            }
            yvn = v.name;
            lt_b = true;
          } else if (s == "degrees_east") {
            if (istream.variable_data(v.name, xvd) == NetCDF::NCType::_NULL) {
              log_error2("unable to get longitudes", F, "nc2xml", USER);
            }
            xvn = v.name;
            ln_b = true;
          }
        }
      }
    }
  }
  if (!tm_b) {
    log_error2("could not find the 'time' variable", F, "nc2xml", USER);
  }
  if (!lt_b) {
    log_error2("could not find the 'latitude' variable", F, "nc2xml", USER);
  }
  if (!ln_b) {
    log_error2("could not find the 'longitude' variable", F, "nc2xml", USER);
  }
  if (ientry.key.empty()) {
    log_error2("could not find the vessel ID", F, "nc2xml", USER);
  }
  gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "ObML",
      "nc2xml", USER);
  if (g_inv.stream.is_open()) {
    g_inv.stream << "netCDF:point|" << istream.record_size() << endl;
    if (g_inv.maps.O.find("surface") == g_inv.maps.O.end()) {
      g_inv.maps.O.emplace("surface", make_pair(g_inv.maps.O.size(), ""));
    }
    if (g_inv.maps.P.find(ptyp) == g_inv.maps.P.end()) {
      g_inv.maps.P.emplace(ptyp, make_pair(g_inv.maps.P.size(), ""));
    }
  }

  // find the data variables
  unordered_map<size_t, string> T_map;
  unordered_set<string> vset;
  float mn = 99., mx = -99.;
  for (const auto& v : vars) {
    if (v.name != tvn && v.name != yvn && v.name != xvn) {
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.nc_type, ad, scan_data.
          conventions);
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::NCType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      if (scan_data.datatype_map.description(v.name).empty()) {
        auto s = v.name + "<!>" + ad.long_name + "<!>" + ad.units + "<!>" + ad.
            cf_keyword;
        if (vset.find(s) == vset.end()) {
          scan_data.netcdf_variables.emplace_back(s);
          vset.emplace(s);
        }
      }
      if (g_inv.maps.D.find(v.name) == g_inv.maps.D.end()) {
        auto byts = 1;
        for (const auto& d : v.dimids) {
          byts *= dims[d].length;
        }
        switch (v.nc_type) {
          case NetCDF::NCType::SHORT: {
            byts *= 2;
            break;
          }
          case NetCDF::NCType::INT:
          case NetCDF::NCType::FLOAT: {
            byts *= 4;
            break;
          }
          case NetCDF::NCType::DOUBLE: {
            byts *= 8;
            break;
          }
          default: { }
        }
        g_inv.maps.D.emplace(v.name, make_pair(g_inv.maps.D.size(), "|" + lltos(v.offset) +
            "|" + NetCDF::nc_type_str[static_cast<int>(v.nc_type)] + "|" +
            itos(byts)));
      }
      vector<string> sv;
      for (size_t m = 0; m < tvd.size(); ++m) {
        if (!found_missing(tvd[m], nullptr, vd[m], ad.missing_value)) {
          ++scan_data.num_not_missing;
          float lon = xvd[m];
          if (lon > 180.) {
            lon -= 360.;
          }
          auto dt = time_data.reference.added(time_data.units, tvd[m]);
          if (g_inv.stream.is_open()) {
            if (T_map.find(m) == T_map.end()) {
              T_map.emplace(m, dt.to_string("%Y%m%d%H%MM") + "[!]" + ftos(yvd[
                  m], 4) + "[!]" + ftos(lon, 4));
            }
          }
          if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[m], lon,
              tvd[m], &dt)) {
            auto e = move(myerror);
            log_error2(e + "' when adding ID " + ientry.key, F, "nc2xml", USER);
          }
          if (!obs_data.added_to_platforms("surface", ptyp, yvd[m], lon)) {
            auto e = move(myerror);
            log_error2(e + "' when adding platform " + ptyp, F, "nc2xml", USER);
          }
          if (yvd[m] < mn) {
            mn = yvd[m];
          }
          if (yvd[m] > mx) {
            mx = yvd[m];
          }
        } else {
          if (g_inv.stream.is_open()) {
            sv.emplace_back(itos(m) + "|0|" + itos(g_inv.maps.P[ptyp].first) + "|" +
                itos(g_inv.maps.I[ientry.key.substr(ientry.key.find("[!]") + 3)].first)
                + "|" + itos(g_inv.maps.D[v.name].first));
          }
        }
      }
      if (g_inv.stream.is_open()) {
        if (sv.size() != tvd.size()) {
          for (const auto& e : sv) {
            inv_lines2.writeln(e);
          }
        } else {
          g_inv.maps.D.erase(v.name);
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
  if (g_inv.stream.is_open()) {
    size_t w, e;
    bitmap::longitudeBitmap::west_east_bounds(ientry.data->min_lon_bitmap.get(),
        w, e);
    auto k = ientry.key.substr(ientry.key.find("[!]") + 3) + "[!]" + ftos(mn, 4)
        + "[!]" + ftos(ientry.data->min_lon_bitmap[w], 4) + "[!]" + ftos(mx, 4)
        + "[!]" + ftos(ientry.data->max_lon_bitmap[ e], 4);
    if (g_inv.maps.I.find(k) == g_inv.maps.I.end()) {
      g_inv.maps.I.emplace(k, make_pair(g_inv.maps.I.size(), ""));
    }
    vector<size_t> v;
    for (const auto& e : T_map) {
      v.emplace_back(e.first);
    }
    sort(v.begin(), v.end(),
    [](const size_t& left, const size_t& right) -> bool {
      if (left <= right) {
        return true;
      }
      return false;
    });
    for (const auto& e : v) {
      g_inv.stream << "T<!>" << e << "<!>" << T_map[e] << endl;
    }
  }
}

void write_parameter_map(string tempdir_name, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "Writing parameter map..." << endl;
  }
  string mtyp;
  if (scan_data.write_type == ScanData::GrML_type) {
    mtyp = "parameter";
  } else if (scan_data.write_type == ScanData::ObML_type) {
    mtyp = "dataType";
  } else {
    log_error2("unknown map type", F, "nc2xml", USER);
  }
  vector<string> v;
  if (scan_data.map_filled) {
    std::ifstream ifs(scan_data.map_name.c_str());
    char l[32768];
    ifs.getline(l, 32768);
    while (!ifs.eof()) {
      v.emplace_back(l);
      ifs.getline(l, 32768);
    }
    ifs.close();
    v.pop_back();
    stringstream oss, ess;
    mysystem2("/bin/rm " + scan_data.map_name, oss, ess);
  }
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -m 0755 -p " + tempdir_name + "/metadata/"
      "ParameterTables", oss, ess) != 0) {
    log_error2("can't create directory tree for netCDF variables", F, "nc2xml",
        USER);
  }
  scan_data.map_name = tempdir_name + "/metadata/ParameterTables/netCDF." +
      metautils::args.dsid + ".xml";
  std::ofstream ofs;
  open_output(ofs, scan_data.map_name);
  if (!ofs.is_open()) {
    log_error2("can't open parameter map file for output", F, "nc2xml", USER);
  }
  if (!scan_data.map_filled) {
    ofs << "<?xml version=\"1.0\" ?>" << endl;
    ofs << "<" << mtyp << "Map>" << endl;
  } else {
    auto no_write = false;
    for (const auto& e : v) {
      if (regex_search(e, regex(" code=\""))) {
        auto sp = split(e, "\"");
        if (scan_data.changed_variables.find(sp[1]) != scan_data.
            changed_variables.end()) {
          no_write = true;
        }
      }
      if (!no_write) {
        ofs << e << endl;
      }
      if (regex_search(e, regex("</" + mtyp + ">"))) {
        no_write = false;
      }
    }
  }
  for (const auto& v : scan_data.netcdf_variables) {
    auto sp = split(v, "<!>");
    if (scan_data.write_type == ScanData::GrML_type) {
      ofs << "  <parameter code=\"" << sp[0] << "\">" << endl;
      ofs << "    <shortName>" << sp[0] << "</shortName>" << endl;
      if (!sp[1].empty()) {
        ofs << "    <description>" << sp[1] << "</description>" << endl;
      }
      if (!sp[2].empty()) {
        ofs << "    <units>" << substitute(sp[2], "-", "^-") << "</units>" <<
            endl;
      }
      if (sp.size() > 3 && !sp[3].empty()) {
        ofs << "    <standardName>" << sp[3] << "</standardName>" << endl;
      }
      ofs << "  </parameter>" << endl;
    } else if (scan_data.write_type == ScanData::ObML_type) {
      ofs << "  <dataType code=\"" << sp[0] << "\">" << endl;
      ofs << "    <description>" << sp[1];
      if (!sp[2].empty()) {
        ofs << " (" << sp[2] << ")";
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
  string e;
  if (unixutils::gdex_upload_dir(tempdir_name, "metadata/ParameterTables/",
      "/data/web", metautils::directives.gdex_upload_key, e) < 0) {
    log_warning("parameter map was not synced - error(s): '" + e + "'",
        "nc2xml", USER);
  }
  mysystem2("/bin/cp " + scan_data.map_name + " " + metautils::directives.
      parameter_map_path + "/netCDF." + metautils::args.dsid + ".xml", oss,
      ess);
  if (gatherxml::verbose_operation) {
    cout << "...finished writing parameter map." << endl;
  }
}

void scan_file(ScanData& scan_data, gatherxml::markup::ObML::ObservationData&
     obs_data) {
  static const string F = this_function_label(__func__);
  unique_ptr<TempFile> tfile(new TempFile);
  tfile->open(metautils::directives.temp_path);
  unique_ptr<TempDir> tdir(new TempDir);
  if (!tdir->create(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary directory in " +
        metautils::directives.temp_path, F +
        ": prepare_file_for_metadata_scanning()", "nc2xml", USER);
  }
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/netCDF." + metautils::args.dsid + ".xml", tdir->
      name());
  list<string> flst;
  string ff, e;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
      *tdir, &flst, ff, e)) {
    log_error2(e, F + ": prepare_file_for_metadata_scanning()", "nc2xml", USER);
  }
  if (flst.empty()) {
    flst.emplace_back(tfile->name());
  }
  if (gatherxml::verbose_operation) {
    cout << "Ready to scan " << flst.size() << " files." << endl;
  }
  for (const auto& f : flst) {
    if (gatherxml::verbose_operation) {
      cout << "Beginning scan of " << f << "..." << endl;
    }
    InputNetCDFStream istream;
    if (!istream.open(f)) {
      auto e = move(myerror);
      log_error2("Terminating - " + e, F, "nc2xml", USER);
    }
    gattrs = istream.global_attributes();
    for (const auto& a : gattrs) {
      if (a.name == "Conventions") {
        scan_data.conventions = *(reinterpret_cast<string *>(a.values));
      }
    }
    dims = istream.dimensions();
    vars = istream.variables();
    if (regex_search(metautils::args.data_format, regex("^cfnetcdf"))) {
      scan_cf_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format, regex("^iddnetcdf"))) {
      scan_idd_observation_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format, regex("^npnnetcdf"))) {
      scan_npn_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format, regex("^pbnetcdf"))) {
      scan_prepbufr_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format, regex("^rafnetcdf"))) {
      scan_raf_aircraft_netcdf_file(istream, scan_data, obs_data);
    } else if (regex_search(metautils::args.data_format, regex("^samosnc"))) {
      scan_samos_netcdf_file(istream, scan_data, obs_data);
/*
    } else if (metautils::args.data_format.beginsWith("wrfsimnetcdf")) {
      scan_wrf_simulation_netcdf_file(istream,found_map,map_name,var_list,changed_var_table);
*/
    } else {
      log_error2(metautils::args.data_format + "-formatted files not "
          "recognized", F, "nc2xml", USER);
    }
    istream.close();
    if (gatherxml::verbose_operation) {
      cout << "  ...scan of " << f << " completed." << endl;
    }
  }
  metautils::args.data_format = "netcdf";
  if (!metautils::args.inventory_only && !scan_data.netcdf_variables.empty() &&
      metautils::args.dsid != "test") {
    write_parameter_map(tdir->name(), scan_data);
  }
}

void show_usage() {
  cerr << "usage: nc2xml -f format -d [ds]nnn.n [options...] path" << endl;
  cerr << endl;
  cerr << "required (choose one):" << endl;
  cerr << "-f cfnetcdf      Climate and Forecast compliant netCDF3 data" <<
      endl;
  cerr << "-f iddnetcdf     Unidata IDD netCDF3 station data" << endl;
  cerr << "-f npnnetcdf     NOAA Profiler Network vertical profile data" <<
      endl;
  cerr << "-f pbnetcdf      NetCDF3 converted from prepbufr" << endl;
  cerr << "-f rafnetcdf     NCAR-RAF/nimbus compliant netCDF3 aircraft data"
      << endl;
  cerr << "-f samosnc       SAMOS netCDF3 data" << endl;
//    cerr << "-f wrfsimnetcdf  Climate Simulations from WRF" << endl;
  cerr << endl;
  cerr << "required:" << endl;
  cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file "
      "belongs" << endl;
  cerr << endl;
  cerr << "options:" << endl;
  if (USER == "dattore") {
    cerr << "-r/-R            regenerate/don't regenerate the dataset webpage"
        << endl;
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
  cerr << "                 - URLs must begin with \"https://rda.ucar.edu\""
      << endl;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    show_usage();
    exit(1);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  atexit(clean_up);
  auto d = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, d);
  metautils::read_config("nc2xml", USER, false);
  gatherxml::parse_args(d);
  if (metautils::args.dsid == "d999009") {
    log_error2("Terminating - Testing has changed. Use:\n  gatherxml -d test "
        "-f " + metautils::args.data_format + " <full path to file '" +
        metautils::args.filename + "'>", "main()", "nc2xml", USER);
  }
  metautils::cmd_register("nc2xml", USER);
  if (metautils::args.dsid != "test" && !metautils::args.overwrite_only &&
      !metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("GrML", "nc2xml", USER);
    metautils::check_for_existing_cmd("ObML", "nc2xml", USER);
  }
  Timer tmr;
  tmr.start();
  ScanData sd;
  gatherxml::markup::ObML::ObservationData od;
  scan_file(sd, od);
  if (gatherxml::verbose_operation && !metautils::args.inventory_only) {
    cout << "Writing XML..." << endl;
  }
  string ext, tdir;
  if (sd.write_type == ScanData::GrML_type) {
    ext = "GrML";
    if (!metautils::args.inventory_only) {
      tdir = gatherxml::markup::GrML::write(*sd.grids, "nc2xml", USER);
    }
  } else if (sd.write_type == ScanData::ObML_type) {
    ext = "ObML";
    if (!metautils::args.inventory_only) {
      if (sd.num_not_missing > 0) {
        gatherxml::markup::ObML::write(od, "nc2xml", USER);
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
    string f;
    if (!metautils::args.update_summary) {
      f += " -S ";
    }
    if (!metautils::args.regenerate) {
      f += " -R ";
    }
    if (!tdir.empty()) {
      f += " -t " + tdir;
    }
    if (!metautils::args.inventory_only && regex_search(metautils::args.path,
        regex("^https://rda.ucar.edu"))) {
      f += " -wf";
    } else {
      f += " -f";
    }
    if (gatherxml::verbose_operation) {
      cout << "Calling 'scm' to update the database..." << endl;
    }
    stringstream oss, ess;
    auto cmd = metautils::directives.local_root + "/bin/scm -d " + metautils::
        args.dsid + " " + f + " " + metautils::args.filename + "." + ext;
    if (mysystem2(cmd, oss, ess) != 0) {
      log_error2(ess.str(), "main(): running scm", "nc2xml", USER);
    }
    if (gatherxml::verbose_operation) {
      cout << "...'scm' finished." << endl;
    }
  } else if (metautils::args.dsid == "test") {
    cout << "Output is in:" << endl;
    cout << "  " << tdir << "/" << metautils::args.filename << ".";
    switch (sd.write_type) {
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
  if (g_inv.stream.is_open()) {
    vector<pair<int, string>> v;
    if (sd.write_type == ScanData::GrML_type) {
      sort_inventory_map(g_inv.maps.U, v);
      for (const auto& e : v) {
        g_inv.stream << "U<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(g_inv.maps.G, v);
      for (const auto& e : v) {
        g_inv.stream << "G<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(g_inv.maps.L, v);
      for (const auto& e : v) {
        g_inv.stream << "L<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(g_inv.maps.P, v);
      for (const auto& e : v) {
        g_inv.stream << "P<!>" << e.first << "<!>" << e.second;
        if (is_large_offset) {
          g_inv.stream << "<!>BIG";
        }
        g_inv.stream << endl;
      }
      sort_inventory_map(g_inv.maps.R, v);
      for (const auto& e : v) {
        g_inv.stream << "R<!>" << e.first << "<!>" << e.second << endl;
      }
    } else if (sd.write_type == ScanData::ObML_type) {
      sort_inventory_map(g_inv.maps.O, v);
      for (const auto& e : v) {
        g_inv.stream << "O<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(g_inv.maps.P, v);
      for (const auto& e : v) {
        g_inv.stream << "P<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(g_inv.maps.I, v);
      for (const auto& e : v) {
        g_inv.stream << "I<!>" << e.first << "<!>" << e.second << "[!]" <<
            g_inv.maps.I[e.second].second << endl;
      }
      sort_inventory_map(g_inv.maps.D, v);
      for (const auto& e : v) {
        g_inv.stream << "D<!>" << e.first << "<!>" << e.second << g_inv.maps.
            D[e.second].second << endl;
      }
    }
    g_inv.stream << "-----" << endl;
    if (!g_inv.lines.empty()) {
      for (const auto& line : g_inv.lines) {
        g_inv.stream << line << endl;
      }
    } else {
      inv_lines2.close();
      std::ifstream ifs(inv_lines2.name().c_str());
      if (ifs.is_open()) {
        char l[32768];
        ifs.getline(l, 32768);
        while (!ifs.eof()) {
          g_inv.stream << l << endl;
          ifs.getline(l, 32768);
        }
        ifs.close();
      }
    }
    gatherxml::fileInventory::close(g_inv.file, g_inv.dir, g_inv.stream, ext,
        true, metautils::args.update_summary, "nc2xml", USER);
  }
  if (!unknown_IDs.empty()) {
    stringstream ss;
    for (const auto& id : unknown_IDs) {
      ss << id << endl;
    }
    log_warning("unknown ID(s):\n" + ss.str(), "nc2xml", USER);
  }
  if (sd.num_missing_loc > 0) {
    cout << sd.num_missing_loc << " observations were ignored because the "
        "location (latitude or longitude) was out of range" << endl;
  }
  tmr.stop();
  log_warning("execution time: " + ftos(tmr.elapsed_time()) + " seconds",
      "gatherxml.time", USER);
}
