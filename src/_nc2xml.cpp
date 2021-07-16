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

using floatutils::myequalf;
using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
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

  enum { GrML_type = 1, ObML_type };
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
  NetCDFVariableAttributeData() : long_name(), units(), cf_keyword(),
      missing_value() { }

  string long_name, units, cf_keyword;
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
TempDir *inv_dir = nullptr;
std::ofstream inv_stream;
unordered_map<string, pair<int, string>> D_map, G_map, I_map, L_map, O_map,
    P_map, R_map, U_map;

struct InvTimeEntry {
  InvTimeEntry() : key(), dt() { }

  size_t key;
  string dt;
};

vector<string> inv_lines;
TempFile inv_lines2("/tmp");
unordered_set<string> unknown_IDs;
string conventions;
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
  sort(sorted_keys.begin(), sorted_keys.end(),
  [](const pair<int, string>& left, const pair<int, string>& right) -> bool {
    if (left.first <= right.first) {
      return true;
    }
    return false;
  });
}

void fill_nc_time_data(const InputNetCDFStream::Attribute& attr) {
  static const string F = this_function_label(__func__);
  auto u = *(reinterpret_cast<string *>(attr.values));
  if (gatherxml::verbose_operation) {
    cout << "  Time units: '" << u << "'" << endl;
  }
  if (regex_search(u, regex("since"))) {
    time_data.units = to_lower(u.substr(0, u.find("since")));
    trim(time_data.units);
    u = u.substr(u.find("since") + 5);
    while (!u.empty() && (u[0] < '0' || u[0] > '9')) {
      u = u.substr(1);
    }
    auto n = u.length() - 1;
    while (n > 0 && (u[n] < '0' || u[n] > '9')) {
      --n;
    }
    ++n;
    if (n < u.length()) {
      u = u.substr(0, n);
    }
    trim(u);
    auto sp = split(u);
    if (sp.size() < 1 || sp.size() > 3) {
      log_error2("unable to get reference time from units specified as: '" +
          *(reinterpret_cast<string *>(attr.values)) + "'", F, "nc2xml", USER);
    }
    auto sp2 = split(sp[0], "-");
    if (sp2.size() != 3) {
      log_error2("unable to get reference time from units specified as: '" +
          *(reinterpret_cast<string *>(attr.values)) + "'", F, "nc2xml", USER);
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
          time_data.reference.set_time(stoi(sp3[0]) * 10000 + stoi(sp3[1]) *
              100);
          break;
        }
        case 3: {
          time_data.reference.set_time(stoi(sp3[0]) * 10000 + stoi(sp3[1]) * 100
              + static_cast<int>(stof(sp3[2])));
          break;
        }
      }
    }
    if (gatherxml::verbose_operation) {
      cout << "  Reference time set to: " << time_data.reference.to_string(
          "%Y-%m-%d %H:%MM:%SS") << endl;
    }
  } else {
    log_error2("unable to get CF time from time variable units", F, "nc2xml",
        USER);
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

void extract_from_variable_attribute(const vector<InputNetCDFStream::Attribute>&
    attribute_list, NetCDF::DataType data_type, NetCDFVariableAttributeData&
    nc_attribute_data) {
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
      nc_attribute_data.missing_value.resize(data_type);
      switch (data_type) {
        case NetCDF::DataType::CHAR: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<char *>(a.
              values)));
          break;
        }
        case NetCDF::DataType::SHORT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<short *>(a.
              values)));
          break;
        }
        case NetCDF::DataType::INT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<int *>(a.
              values)));
          break;
        }
        case NetCDF::DataType::FLOAT: {
          nc_attribute_data.missing_value.set(*(reinterpret_cast<float *>(a.
              values)));
          break;
        }
        case NetCDF::DataType::DOUBLE: {
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
  if (var_missing_value.type() != NetCDF::DataType::_NULL) {
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
  extract_from_variable_attribute(var.attrs, var.data_type, nc_va_data);
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
  pentry_p->start_date_time = first_valid_date_time;
  pentry_p->end_date_time = last_valid_date_time;
  pentry_p->num_time_steps = nsteps;
  lentry_p->parameter_code_table.insert(*pentry_p);
  if (inv_stream.is_open()) {
    if (P_map.find(pentry_p->key) == P_map.end()) {
      P_map.emplace(pentry_p->key, make_pair(P_map.size(), ""));
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
  auto key = substitute(gentry_key.substr(0, idx), "<!>", ", ");
  if (G_map.find(key) == G_map.end()) {
    G_map.emplace(key, make_pair(G_map.size(), ""));
  }
}

void add_level_to_inventory(string lentry_key, string gentry_key, size_t
    timedimid, int levdimid, size_t latdimid, size_t londimid,
    InputNetCDFStream& istream) {
  static const string F = this_function_label(__func__);
  if (L_map.find(lentry_key) == L_map.end()) {
    L_map.emplace(lentry_key, make_pair(L_map.size(), ""));
  }
  auto idx = gentry_key.rfind("<!>");
  string s = "|" + itos(U_map[gentry_key.substr(idx + 3)].first) + "|" + itos(
      G_map[substitute(gentry_key.substr(0, idx), "<!>", ", ")].first) + "|" +
      itos(L_map[lentry_key].first);
  auto d = istream.dimensions();
  auto v = istream.variables();
  for (size_t n = 0; n < v.size(); ++n) {
    auto key = "ds" + metautils::args.dsnum + ":" + v[n].name;
    if (v[n].dimids.size() > 0 && !v[n].is_coord && v[n].dimids[0] == timedimid
        && P_map.find(key) != P_map.end() && ((v[n].dimids.size() == 4 &&
        levdimid >= 0 && static_cast<int>(v[n].dimids[1]) == levdimid && v[n].
        dimids[2] == latdimid && v[n].dimids[3] == londimid) || (v[n].dimids.
        size() == 3 && levdimid < 0 && v[n].dimids[1] == latdimid && v[n].
        dimids[2] == londimid))) {
      auto rkey = itos(static_cast<int>(v[n].data_type));
      if (R_map.find(rkey ) == R_map.end()) {
        R_map.emplace(rkey, make_pair(R_map.size(), ""));
      }
      auto vsz = v[n].size;
      if (v[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(v[n].
          dimids[1]) == levdimid) {
        vsz /= d[levdimid].length;
      }
      long long off = v[n].offset;
      for (size_t m = 0; m < time_s.num_times; ++m) {
        string e;
        inv_lines.emplace_back(lltos(off) + "|" + itos(vsz) + "|" + metautils::
            NcTime::actual_date_time(time_s.times[m], time_data, e).to_string(
            "%Y%m%d%H%MM") + s + "|" + itos(P_map[key].first) + "|" + itos(
            R_map[rkey].first));
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
        dt1 = tre.data->instantaneous.first_valid_datetime;
        dt2 = tre.data->instantaneous.last_valid_datetime;
      } else {
        if (time_bounds_s.changed) {
          log_error2("time bounds changed", F, "nc2xml", USER);
        }
        dt1 = tre.data->bounded.first_valid_datetime;
        dt2 = tre.data->bounded.last_valid_datetime;
      }
      tm = strutils::capitalize(tm);
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
            pentry_p->key = "ds" + metautils::args.dsnum + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.data->num_steps,
                parameter_table, scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        } else if (v.dimids.size() == 3 || v.dimids.size() == 4) {
          if (is_regular_lat_lon_grid_variable(v, timedimid, levdimid, latdimid,
              londimid)) {

            // check as a regular lat/lon grid variable
            pentry_p->key = "ds" + metautils::args.dsnum + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.data->num_steps,
                parameter_table, scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          } else if (is_polar_stereographic_grid_variable(v, timedimid,
              levdimid, latdimid)) {

            // check as a polar-stereographic grid variable
            pentry_p->key = "ds" + metautils::args.dsnum + ":" + v.name;
            add_gridded_netcdf_parameter(v, dt1, dt2, tre.data->num_steps,
                parameter_table, scan_data);
            if (inv_stream.is_open()) {
              add_grid_to_inventory(gentry_key);
            }
          }
        }
      }
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

void add_gridded_lat_lon_keys(vector<string>& gentry_keys, Grid::
    GridDimensions dim, Grid::GridDefinition def, string timeid, size_t
    timedimid, int levdimid, size_t latdimid, size_t londimid, const metautils::
    NcTime::TimeRangeEntry& tre, vector<NetCDF::Variable>& vars) {
  switch (def.type) {
    case Grid::Type::latitudeLongitude:
    case Grid::Type::gaussianLatitudeLongitude: {
      auto k = itos(static_cast<int>(def.type));
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
          3) + "<!>";
      add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid,
          latdimid, londimid, tre, vars);
      break;
    }
    case Grid::Type::polarStereographic: {
      auto k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          llatitude, 3) + "<!>" + ftos(def.olongitude, 3) + "<!>" + ftos(def.dx,
          3) +" <!>" + ftos(def.dy, 3) + "<!>";
      if (def.projection_flag == 0) {
        k += "N";
      } else {
        k += "S";
      }
      k += "<!>";
      add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid,
          latdimid, londimid, tre, vars);
      break;
    }
    case Grid::Type::mercator: {
      auto k = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        k += "C";
      }
      k += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
          slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
          elatitude, 3) + "<!>" + ftos(def.elongitude, 3) + "<!>" + ftos(def.
          loincrement, 3) + "<!>" + ftos(def.laincrement, 3) + "<!>";
      add_gridded_time_range(k, gentry_keys, timeid, timedimid, levdimid,
          latdimid, londimid, tre, vars);
      break;
    }
    default: { }
  }
}

void add_gridded_zonal_mean_keys(vector<string>& gentry_keys, Grid::
    GridDimensions dim, Grid::GridDefinition def, string timeid, size_t
    timedimid, int levdimid, size_t latdimid, size_t londimid, metautils::
    NcTime::TimeRangeEntry &tre, vector<NetCDF::Variable>& vars) {
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
    if (a.data_type == NetCDF::DataType::CHAR) {
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
  DiscreteGeometriesData() : indexes(), z_units(), z_pos() { }

  struct Indexes {
    Indexes() : time_var(MISSING_FLAG), time_bounds_var(MISSING_FLAG),
        stn_id_var(MISSING_FLAG), network_var(MISSING_FLAG),
        lat_var(MISSING_FLAG), lat_var_bounds(MISSING_FLAG),
        lon_var(MISSING_FLAG), lon_var_bounds(MISSING_FLAG),
        sample_dim_var(MISSING_FLAG), instance_dim_var(MISSING_FLAG),
        z_var(MISSING_FLAG) { }

    size_t time_var, time_bounds_var, stn_id_var, network_var, lat_var,
        lat_var_bounds, lon_var, lon_var_bounds, sample_dim_var,
        instance_dim_var, z_var;
  };
  Indexes indexes;
  string z_units, z_pos;
};

void process_units_attribute(const vector<NetCDF::Variable>& vars, size_t
    var_index, size_t attr_index, DiscreteGeometriesData& dgd) {
  static const string F = this_function_label(__func__);
  auto u = *(reinterpret_cast<string *>(vars[var_index].attrs[attr_index].
      values));
  u = to_lower(u);
  if (regex_search(u, regex("since"))) {
    if (dgd.indexes.time_var != MISSING_FLAG) {
      log_error2("time was already identified - don't know what to do with "
          "variable: " + vars[var_index].name, F, "nc2xml", USER);
    }
    fill_nc_time_data(vars[var_index].attrs[attr_index]);
    dgd.indexes.time_var = var_index;
  } else if (regex_search(u, regex("^degree(s){0,1}(_){0,1}((north)|N)$"))) {
    if (dgd.indexes.lat_var == MISSING_FLAG) {
      dgd.indexes.lat_var = var_index;
    } else {
      for (const auto& a : vars[var_index].attrs) {
        if (regex_search(a.name, regex("bounds", std::regex_constants::
            icase))) {
          auto v = *(reinterpret_cast<string *>(a.values));
          if (v == vars[dgd.indexes.lat_var].name) {
            dgd.indexes.lat_var = var_index;
          }
        }
      }
    }
  } else if (regex_search(u, regex("^degree(s){0,1}(_){0,1}((east)|E)$"))) {
    if (dgd.indexes.lon_var == MISSING_FLAG) {
      dgd.indexes.lon_var = var_index;
    } else {
      for (const auto& a : vars[var_index].attrs) {
        if (regex_search(a.name, regex("bounds", std::regex_constants::
            icase))) {
          auto v = *(reinterpret_cast<string *>(a.values));
          if (v == vars[dgd.indexes.lon_var].name) {
            dgd.indexes.lon_var = var_index;
          }
        }
      }
    }
  }
}

void process_variable_attributes(const vector<NetCDF::Variable>& vars,
    const vector<NetCDF::Dimension>& dims, DiscreteGeometriesData& dgd) {
  static const string F = this_function_label(__func__);
  for (size_t n = 0; n < vars.size(); ++n) {
    for (size_t m = 0; m < vars[n].attrs.size(); ++m) {
      if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR) {
        if (vars[n].attrs[m].name == "units") {
          process_units_attribute(vars, n, m, dgd);
        } else if (vars[n].attrs[m].name == "cf_role") {
          auto r = *(reinterpret_cast<string *>(vars[n].attrs[m].values));
          r = to_lower(r);
          if (r == "timeseries_id" || r == "profile_id") {
            if (dgd.indexes.stn_id_var != MISSING_FLAG) {
              log_error2("station ID was already identified - don't know what "
                  "to do with variable: " + vars[n].name, F, "nc2xml", USER);
            }
            dgd.indexes.stn_id_var = n;
          }
        } else if (vars[n].attrs[m].name == "sample_dimension") {
          dgd.indexes.sample_dim_var = n;
        } else if (vars[n].attrs[m].name == "instance_dimension") {
          dgd.indexes.instance_dim_var = n;
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
          break;
        }
      }
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

void scan_cf_point_netcdf_file(InputNetCDFStream& istream, string platform_type,
    ScanData& scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  auto vars = istream.variables();
  auto dims = istream.dimensions();
  DiscreteGeometriesData dgd;
  process_variable_attributes(vars, dims, dgd);
  NetCDF::VariableData tvd, yvd, xvd;
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (dgd.indexes.lat_var == MISSING_FLAG) {
    log_error2("unable to determine latitude variable", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.lat_var].name, yvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (dgd.indexes.lon_var == MISSING_FLAG) {
    log_error2("unable to determine longitude variable", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.lon_var].name, xvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  vector<DateTime> dtv;
  vector<string> idv;
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.time_var].name && v.name != vars[dgd.indexes.
        lat_var].name && v.name != vars[dgd.indexes.lon_var].name) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get variable data for '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
    string platform_type, DiscreteGeometriesData& dgd, unordered_map<size_t,
    string>& T_map, ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  gatherxml::fileInventory::open(inv_file, &inv_dir, inv_stream, "ObML",
      "nc2xml", USER);
  if (inv_stream.is_open()) {
    inv_stream << "netCDF:timeSeries|" << istream.record_size() << endl;
    O_map.emplace("surface", make_pair(O_map.size(), ""));
  }
  NetCDF::VariableData yvd, xvd, ivd;
  NetCDF::DataType ityp = NetCDF::DataType::_NULL;
  size_t ilen = 0;
  vector<string> pfms, ityps, ids;
  if (dgd.indexes.lat_var == MISSING_FLAG || dgd.indexes.lon_var == MISSING_FLAG
      || dgd.indexes.stn_id_var == MISSING_FLAG) {

    // lat/lon not found, look for known alternates in global attributes
    auto ga = istream.global_attributes();
    size_t src = MISSING_FLAG;
    for (size_t n = 0; n < ga.size(); ++n) {
      if (to_lower(ga[n].name) == "title") {
        if (to_lower(*(reinterpret_cast<string *>(ga[n].values))) == "hadisd") {
          src = 0x1;
          break;
        }
      }
    }
    if (src == 0x1) {

      // HadISD
      for (size_t n = 0; n < ga.size(); ++n) {
        if (ga[n].name == "latitude") {
          yvd.resize(1, NetCDF::DataType::FLOAT);
          yvd.set(0, *(reinterpret_cast<float *>(ga[n].values)));
        } else if (ga[n].name == "longitude") {
          xvd.resize(1, NetCDF::DataType::FLOAT);
          xvd.set(0, *(reinterpret_cast<float *>(ga[n].values)));
        } else if (ga[n].name == "station_id") {
          auto id = *(reinterpret_cast<string *>(ga[n].values));
          id = id.substr(0, id.find("-"));
          ids.emplace_back(id);
          ilen = id.length();
          ityp = NetCDF::DataType::CHAR;
          ivd.resize(ilen, ityp);
          for (size_t m = 0; m < ilen; ++m) {
            ivd.set(m, id[m]);
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
      if (inv_stream.is_open()) {
        auto key = ityps.back() + "[!]" + ids.back();
        if (I_map.find(key) == I_map.end()) {
            I_map.emplace(key, make_pair(I_map.size(), ftos(yvd.back(), 4) +
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
  auto vars = istream.variables();
  NetCDF::VariableData tvd;
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (yvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lat_var].name, yvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (xvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lon_var].name, xvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  if (ivd.type() == NetCDF::DataType::_NULL && (ityp=istream.variable_data(vars[
      dgd.indexes.stn_id_var].name, ivd)) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  auto dims = istream.dimensions();
  if (ityp == NetCDF::DataType::CHAR && dgd.indexes.stn_id_var !=
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
  if (pfms.size() == 0) {
    string id;
    for (size_t n = 0; n < ns; ++n) {
      if (ityp == NetCDF::DataType::SHORT || ityp == NetCDF::DataType::INT ||
          ityp == NetCDF::DataType::FLOAT || ityp == NetCDF::DataType::DOUBLE) {
        id = ftos(ivd[n]);
      } else if (ityp == NetCDF::DataType::CHAR) {
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
      if (inv_stream.is_open()) {
        auto key = ityps[n] + "[!]" + ids[n];
        if (I_map.find(key) == I_map.end()) {
          I_map.emplace(key, make_pair(I_map.size(), ftos(yvd[n], 4) + "[!]" +
              ftos(xvd[n], 4)));
        }
      }
    }
  }
  if (inv_stream.is_open()) {
    for (const auto& p : pfms) {
      if (P_map.find(p) == P_map.end()) {
        P_map.emplace(p, make_pair(P_map.size(), " "));
      }
    }
  }
  vector<DateTime> dtv;
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.time_var].name && v.dimids.size() > 0 && ((v.
        dimids[0] == vars[dgd.indexes.time_var].dimids[0] && (sd == -1 || (v.
        dimids.size() > 1 && static_cast<int>(v.dimids[1]) == sd))) || (v.
        dimids.size() > 1 && dgd.indexes.stn_id_var != MISSING_FLAG && v.dimids[
        0] == vars[dgd.indexes.stn_id_var].dimids[0] && v.dimids[1] == vars[dgd.
        indexes.time_var].dimids[0]))) {
      if (gatherxml::verbose_operation) {
        cout << "Scanning netCDF variable '" << v.name << "' ..." << endl;
      }
      if (inv_stream.is_open()) {
        if (D_map.find(v.name) == D_map.end()) {
          auto byts = 1;
          for (size_t l = 1; l < v.dimids.size(); ++l) {
            byts *= dims[v.dimids[l]].length;
          }
          switch (v.data_type) {
            case NetCDF::DataType::SHORT: {
              byts *= 2;
              break;
            }
            case NetCDF::DataType::INT:
            case NetCDF::DataType::FLOAT: {
              byts *= 4;
              break;
            }
            case NetCDF::DataType::DOUBLE: {
              byts *= 8;
              break;
            }
            default: { }
          }
          D_map.emplace(v.name, make_pair(D_map.size(), "|" + lltos(v.offset) +
              "|" + NetCDF::data_type_str[static_cast<int>(v.data_type)] + "|" +
              itos(byts)));
        }
      }
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
            if (inv_stream.is_open()) {
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
            if (inv_stream.is_open()) {
              string s = itos(m);
              s += "|0|" + itos(P_map[pfms[n]].first) + "|" + itos(I_map[ityps[
                  n] + "[!]" + ids[n]].first) + "|" + itos(D_map[v.name].first);
              mlst.emplace_back(s);
            }
          }
        }
        if (inv_stream.is_open()) {
          if (mlst.size() != tvd.size()) {
            for (const auto& l : mlst) {
              inv_lines2.writeln(l);
            }
          } else {
            D_map.erase(v.name);
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
  auto vars = istream.variables();
  NetCDF::VariableData tvd, yvd, xvd, ids, nvd;
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  unique_ptr<NetCDF::VariableData> bvd;
  if (dgd.indexes.time_bounds_var != MISSING_FLAG) {
    bvd.reset(new NetCDF::VariableData);
    if (istream.variable_data(vars[dgd.indexes.time_bounds_var].name, *bvd) ==
        NetCDF::DataType::_NULL) {
      log_error2("unable to get time bounds data", F, "nc2xml", USER);
    }
  }
  if (dgd.indexes.lat_var_bounds == MISSING_FLAG) {
    if (istream.variable_data(vars[dgd.indexes.lat_var].name, yvd) == NetCDF::
        DataType::_NULL) {
      log_error2("unable to get latitude data", F, "nc2xml", USER);
    }
  } else {
    if (istream.variable_data(vars[dgd.indexes.lat_var_bounds].name, yvd) ==
        NetCDF::DataType::_NULL) {
      log_error2("unable to get latitude bounds data", F, "nc2xml", USER);
    }
  }
  if (dgd.indexes.lon_var_bounds == MISSING_FLAG) {
    if (istream.variable_data(vars[dgd.indexes.lon_var].name, xvd) == NetCDF::
        DataType::_NULL) {
      log_error2("unable to get longitude data", F, "nc2xml", USER);
    }
  } else {
    if (istream.variable_data(vars[dgd.indexes.lon_var_bounds].name, xvd) ==
        NetCDF::DataType::_NULL) {
      log_error2("unable to get longitude bounds data", F, "nc2xml", USER);
    }
  }
  if (istream.variable_data(vars[dgd.indexes.stn_id_var].name, ids) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  if (dgd.indexes.network_var != MISSING_FLAG) {
    istream.variable_data(vars[dgd.indexes.network_var].name, nvd);
  }
  auto dims = istream.dimensions();
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
  if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::DataType::
      FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n = 0; n < ns; ++n) {
      pfms.emplace_back(platform_type);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        ityps.emplace_back(itos(nvd[n]));
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
  } else if (ids.type() == NetCDF::DataType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ids.get());
    auto nc = nvd.size() / ns;
    for (size_t n = 0; n < ns; ++n) {
      string id(&i[n * ilen], ilen);
      if (dgd.indexes.network_var != MISSING_FLAG) {
        ityps.emplace_back(string(&(reinterpret_cast<char *>(nvd.get()))[n *
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
        NetCDF::DataType::_NULL) {
      log_error2("unable to get sample dimension data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.time_var].name && v.dimids.size() == 1 &&
          v.dimids[0] == od) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        long long off = 0;
        for (size_t n = 0; n < dims[sd].length; ++n) {
          auto end = off + rvd[n];
          ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
          if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::
              DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
            ientry.key += ftos(ids[n]);
          } else if (ids.type() == NetCDF::DataType::CHAR) {
            auto id = string(&(reinterpret_cast<char *>(ids.get()))[n * ilen],
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
        NetCDF::DataType::_NULL) {
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
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        for (size_t n = 0; n < svd.size(); ++n) {
          size_t idx = svd[n];
          ientry.key = pfms[idx] + "[!]" + ityps[idx] + "[!]";
          if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::
              DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
            ientry.key += ftos(ids[idx]);
          } else if (ids.type() == NetCDF::DataType::CHAR) {
            auto id=string(&(reinterpret_cast<char *>(ids.get()))[idx * ilen],
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
        indexes.time_var].data_type, td);
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
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        if (v.dimids.front() == sd) {
          for (size_t n = 0; n < ns; ++n) {
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::
                DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
              ientry.key += ftos(ids[n]);
            } else if (ids.type() == NetCDF::DataType::CHAR) {
              auto id = string(&(reinterpret_cast<char *>(ids.get()))[n * ilen],
                  ilen);
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
              if (ids.type() == NetCDF::DataType::INT || ids.type() == NetCDF::
                  DataType::FLOAT || ids.type() == NetCDF::DataType::DOUBLE) {
                ientry.key += ftos(ids[m]);
              } else if (ids.type() == NetCDF::DataType::CHAR) {
                auto id = string(&(reinterpret_cast<char *>(ids.get()))[m *
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
     platform_type, ScanData& scan_data, gatherxml::markup::ObML::
     ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto vars = istream.variables();
  auto dims = istream.dimensions();
  DiscreteGeometriesData dgd;
  process_variable_attributes(vars, dims, dgd);
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
  if (inv_stream.is_open()) {
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
      inv_stream << "T<!>" << t << "<!>" << T_map[t] << endl;
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_orthogonal_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  string ityp = "unknown";
  NetCDF::VariableData tvd, yvd, xvd, ivd, lvd;
  auto vars = istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (yvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lat_var].name, yvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (xvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lon_var].name, xvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  if (ivd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.stn_id_var].name, ivd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name, lvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  auto dims = istream.dimensions();
  size_t ilen = 1;
  if (ivd.type() == NetCDF::DataType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  if (tvd.size() != yvd.size() || yvd.size() != xvd.size() || xvd.size() != ivd.
      size() / ilen) {
    log_error2("profile data does not follow the CF conventions", F, "nc2xml",
        USER);
  }
  NetCDFVariableAttributeData ad;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs, vars[dgd.
      indexes.time_var].data_type, ad);
  for (const auto& v : vars) {
    if (v.name != vars[dgd.indexes.z_var].name && v.dimids.size() > 0 && v.
        dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
          if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::
              DataType::FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
            ientry.key += ftos(ivd[n]);
          } else if (ivd.type() == NetCDF::DataType::CHAR) {
            ientry.key += string(&(reinterpret_cast<char *>(ivd.get()))[n *
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
          dte.data->vdata->avg_res += (avg / (nlv - 1));
          ++dte.data->vdata->res_cnt;
          ++scan_data.num_not_missing;
        }
      }
    }
  }
}

bool compare_z_down(const double& left, const double& right) {
  if (left >= right) {
    return true;
  }
  return false;
}

bool compare_z_up(const double& left, const double& right) {
  if (left <= right) {
    return true;
  }
  return false;
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
    sort(lvls.begin(), lvls.end(), compare_z_down);
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
    sort(lvls.begin(), lvls.end(), compare_z_up);
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
  datatype_entry.data->vdata->avg_res += (avg / (lvls.size() - 1));
  ++datatype_entry.data->vdata->res_cnt;
}

void scan_cf_non_orthogonal_profile_netcdf_file(InputNetCDFStream& istream,
    string platform_type, DiscreteGeometriesData& dgd, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  auto vars = istream.variables();
  NetCDF::VariableData tvd, yvd, xvd, ivd, lvd;
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (yvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lat_var].name, yvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (xvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lon_var].name, xvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  if (ivd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.stn_id_var].name, ivd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  auto dims = istream.dimensions();
  size_t ilen = 1;
  vector<string> pfms, ityps;
  if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::DataType::
      FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n = 0; n < tvd.size(); ++n) {
//      int id = ivd[n];
pfms.emplace_back(platform_type);
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd.type() == NetCDF::DataType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd.get());
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
      DataType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  if (tvd.size() != yvd.size() || yvd.size() != xvd.size() || xvd.size() != ivd.
      size() / ilen) {
    log_error2("profile data does not follow the CF conventions", F, "nc2xml",
        USER);
  }
  NetCDFVariableAttributeData td;
  extract_from_variable_attribute(vars[dgd.indexes.time_var].attrs, vars[dgd.
      indexes.time_var].data_type, td);
  if (dgd.indexes.sample_dim_var != MISSING_FLAG) {

    // continuous ragged array H.10
    NetCDF::VariableData rd;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name, rd) ==
        NetCDF::DataType::_NULL) {
      log_error2("unable to get row size data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.z_var].name && v.dimids.size() > 0 && v.
          dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
          if (lv.size() > 0) {
            auto dt = compute_nc_time(tvd, n);
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::
                DataType::FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
              ientry.key += ftos(ivd[n]);
            } else if (ivd.type() == NetCDF::DataType::CHAR) {
              ientry.key += string(&(reinterpret_cast<char *>(ivd.get()))[n *
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
    NetCDF::VariableData pvd;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, pvd) ==
        NetCDF::DataType::_NULL) {
      log_error2("unable to get instance dimension data", F, "nc2xml", USER);
    }
    for (const auto& v : vars) {
      if (v.name != vars[dgd.indexes.z_var].name && v.name != vars[dgd.indexes.
          instance_dim_var].name && v.dimids.size() > 0 && v.dimids.back() ==
          vars[dgd.indexes.z_var].dimids.front()) {
        NetCDF::VariableData vd;
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
        add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units
            + "<!>" + ad.cf_keyword, scan_data);
        for (size_t n = 0; n < tvd.size(); ++n) {
          vector<double> lv;
          for (size_t m = 0; m < pvd.size(); ++m) {
            if (pvd[m] == n && !found_missing(tvd[n], &td.
                missing_value, vd[m], ad.missing_value)) {
              lv.emplace_back(lvd[m]);
            }
          }
          if (lv.size() > 0) {
            auto dt = compute_nc_time(tvd, n);
            ientry.key = pfms[n] + "[!]" + ityps[n] + "[!]";
            if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::
                DataType::FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
              ientry.key += ftos(ivd[n]);
            } else if (ivd.type() == NetCDF::DataType::CHAR) {
              ientry.key += string(&(reinterpret_cast<char *>(ivd.get()))[n *
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
  }
}

void process_vertical_coordinate_variable(const vector<NetCDF::Attribute>&
    attrs, DiscreteGeometriesData& dgd, string& obs_type) {
  static const string F = this_function_label(__func__);
  string otyp = "";
  for (const auto& a : attrs) {
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
      otyp = "upper_air";
    }
  }
  if (dgd.z_pos.empty()) {
    log_error2("process_vertical_coordinate_variable() returned error: unable "
        "to determine vertical coordinate direction", F, "nc2xml", USER);
  }
  if (dgd.z_pos == "up") {
    otyp = "upper_air";
  } else if (dgd.z_pos == "down") {
    auto l = to_lower(dgd.z_units);
    if (dgd.z_pos == "down" && (regex_search(dgd.z_units, regex("Pa$")) ||
        regex_search(l, regex("^mb(ar){0,1}$")) || l == "millibars")) {
      otyp = "upper_air";
    }
  }
}

void scan_cf_profile_netcdf_file(InputNetCDFStream& istream, string
     platform_type, ScanData& scan_data, gatherxml::markup::ObML::
     ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  auto vars = istream.variables();
  auto dims = istream.dimensions();
  DiscreteGeometriesData dgd;
  process_variable_attributes(vars, dims, dgd);
  string otyp;
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable", F, "nc2xml",
        USER);
  }
  process_vertical_coordinate_variable(vars[dgd.indexes.z_var].attrs, dgd,
      otyp);
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
  NetCDF::VariableData tvd, yvd, xvd, ivd, lvd;
  auto vars = istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (yvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lat_var].name, yvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (xvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lon_var].name, xvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  if (ivd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.stn_id_var].name, ivd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  auto dims = istream.dimensions();
  size_t ilen = 1;
  vector<string> pfms, ityps, ids;
  if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::DataType::
      FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n = 0; n < ivd.size(); ++n) {
      ids.emplace_back(ftos(ivd[n]));
pfms.emplace_back("unknown");
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd.type() == NetCDF::DataType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd.get());
    for (size_t n = 0; n < ivd.size() / ilen; ++n) {
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
      DataType::_NULL) {
    log_error2("unable to get level data", F, "nc2xml", USER);
  }
  for (const auto& v : vars) {
    if (v.dimids.size() == 3 && v.dimids[0] == vars[dgd.indexes.time_var].
        dimids[0] && v.dimids[1] == vars[dgd.indexes.z_var].dimids[0] && v.
        dimids[2] == vars[dgd.indexes.stn_id_var].dimids[0]) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.data_type, ad);
      add_to_netcdf_variables(v.name + "<!>" + ad.long_name + "<!>" + ad.units +
          "<!>" + ad.cf_keyword, scan_data);
      for (size_t n = 0; n < tvd.size(); ++n) {
        auto dt = compute_nc_time(tvd, n);
        size_t ns = ivd.size() / ilen;
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
          if (lv.size() > 0) {
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
  NetCDF::VariableData tvd, yvd, xvd, ivd, lvd;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name, tvd) == NetCDF::
      DataType::_NULL) {
    log_error2("unable to get time data", F, "nc2xml", USER);
  }
  if (yvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lat_var].name, yvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get latitude data", F, "nc2xml", USER);
  }
  if (xvd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.lon_var].name, xvd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get longitude data", F, "nc2xml", USER);
  }
  if (ivd.type() == NetCDF::DataType::_NULL && istream.variable_data(vars[dgd.
      indexes.stn_id_var].name, ivd) == NetCDF::DataType::_NULL) {
    log_error2("unable to get station ID data", F, "nc2xml", USER);
  }
  auto dims = istream.dimensions();
  size_t ilen = 1;
  vector<string> pfms, ityps, ids;
  if (ivd.type() == NetCDF::DataType::INT || ivd.type() == NetCDF::DataType::
      FLOAT || ivd.type() == NetCDF::DataType::DOUBLE) {
    for (size_t n = 0; n < ivd.size(); ++n) {
      ids.emplace_back(ftos(ivd[n]));
pfms.emplace_back("unknown");
ityps.emplace_back("unknown");
      if (!obs_data.added_to_platforms(obs_type, pfms[n], yvd[n], xvd[n])) {
        auto e = move(myerror);
        log_error2(e + "' when adding platform " + obs_type + " " + pfms[n], F,
            "nc2xml", USER);
      }
    }
  } else if (ivd.type() == NetCDF::DataType::CHAR) {
    ilen = dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *i = reinterpret_cast<char *>(ivd.get());
    for (size_t n = 0; n < ivd.size() / ilen; ++n) {
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
      DataType::_NULL) {
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
        NetCDF::DataType::_NULL) {
      log_error2("unable to get sample dimension data", F, "nc2xml", USER);
    }
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name, svd) ==
        NetCDF::DataType::_NULL) {
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
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
          if (lv.size() > 0) {
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
        if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
          log_error2("unable to get data for variable '" + v.name + "'", F,
              "nc2xml", USER);
        }
        NetCDFVariableAttributeData ad;
        extract_from_variable_attribute(v.attrs, v.data_type, ad);
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
            if (lv.size() > 0) {
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
}

void scan_cf_time_series_profile_netcdf_file(InputNetCDFStream& istream, string
    platform_type, ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  auto vars = istream.variables();
  auto dims = istream.dimensions();
  DiscreteGeometriesData dgd;
  process_variable_attributes(vars, dims, dgd);
  string otyp;
  if (dgd.indexes.z_var == MISSING_FLAG) {
    log_error2("unable to determine vertical coordinate variable", F, "nc2xml",
        USER);
  }
  process_vertical_coordinate_variable(vars[dgd.indexes.z_var].attrs, dgd,
        otyp);
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
        if (a.data_type == NetCDF::DataType::CHAR && (a.name == "units" ||
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
              if (a2.data_type == NetCDF::DataType::CHAR) {
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
                      tre.data.reset(new metautils::NcTime::TimeRangeEntry::
                          Data);
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
                        log_error2("unable to handle climatology of " + itos(b)
                            + "-day means", F, "nc2xml", USER);
                      }
                    }
                    tre.data->unit = b;
                    ++(tre.data->num_steps);
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
                  cout << "      " << tre.data->bounded.first_valid_datetime.
                      to_string() << " to " << tre.data->bounded.
                      last_valid_datetime.to_string() << ", units=" << tre.
                      data->unit << endl;
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
            grid_data.lats.back().type = v.data_type;
            string s;
            for (const auto& a2 : v.attrs) {
              if (a2.data_type == NetCDF::DataType::CHAR && a2.name ==
                  "bounds") {
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
            grid_data.lons.back().type = v.data_type;
            string s;
            for (const auto& a2 : v.attrs) {
              if (a2.data_type == NetCDF::DataType::CHAR && a2.name ==
                  "bounds") {
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
                grid_data.levels.back().type = v.data_type;
                grid_data.levdata.ID.emplace_back(v.name + "@@" + units);
                grid_data.levdata.description.emplace_back();
                grid_data.levdata.units.emplace_back(units);
                grid_data.levdata.write.emplace_back(false);
                for (const auto& a3 : v.attrs) {
                  if (a3.data_type == NetCDF::DataType::CHAR && a3.name ==
                      "long_name") {
                    grid_data.levdata.description.back() = *(reinterpret_cast<
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

bool found_alternate_lat_lon_coordinates(vector<NetCDF::Variable>& vars,
    GridData& grid_data) {
  for (const auto& v : vars) {
    if (!v.is_coord && v.dimids.size() == 2) {
      for (const auto& a : v.attrs) {
        if (a.name == "units" && a.data_type == NetCDF::DataType::CHAR) {
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
            grid_data.lats.back().type = v.data_type;
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
            grid_data.lons.back().type = v.data_type;
          }
        }
      }
    }
  }
  return grid_data.lats.size() > 0 && grid_data.lats.size() == grid_data.lons.
      size();
}

bool filled_grid_projection(const unique_ptr<double[]>& la, const unique_ptr<
    double[]>& lo, Grid::GridDimensions& d, Grid::GridDefinition& f) {
  static const string F = this_function_label(__func__);
  double nla = 99999., xla = 0.;
  for (int n = 1, m = d.x; n < d.y; ++n, m += d.x) {
    double a = fabs(la[m] - la[m - d.x]);
    if (a < nla) {
      nla = a;
    }
    if (a > xla) {
      xla = a;
    }
  }
  double nlo = 99999., xlo = 0.;
  for (int n = 1; n < d.x; ++n) {
    double a = fabs(lo[n] - lo[n - 1]);
    if (a < nlo) {
      nlo = a;
    }
    if (a > xlo) {
      xlo = a;
    }
  }
  f.type = Grid::Type::not_set;
  d.size = d.x * d.y;
  if (fabs(xlo - nlo) < 0.0001) {
    if (fabs(xla - nla) < 0.0001) {
      f.type = Grid::Type::latitudeLongitude;
      f.elatitude = la[d.size - 1];
      f.elongitude = lo[d.size - 1];
      f.laincrement = xla;
      f.loincrement = xlo;
    } else {
      auto la1 = la[0];
      auto la2 = la[d.size - 1];
      if (la1 >= 0. && la2 >= 0.) {
      } else if (la1 < 0. && la2 < 0.) {
      } else {
        if (fabs(la1) > fabs(la2)) {
        } else {
          const double PI = 3.141592654;
          if (fabs(cos(la2 * PI / 180.) - (nla / xla)) < 0.01) {
            f.type = Grid::Type::mercator;
            f.elatitude = la[d.size - 1];
            f.elongitude = lo[d.size - 1];
            double ma = 99999.;
            int l = -1;
            for (int n = 0, m = 0; n < d.y; ++n, m += d.x) {
              double a = fabs(la[m] - lround(la[m]));
              if (a < ma) {
                ma = a;
                l = m;
              }
            }
            f.dx = lround(cos(la[l] * PI / 180.) * nlo * 111.2);
            f.dy = lround((fabs(la[l + d.x] - la[l]) + fabs(la[l] - la[l - d.
                x])) / 2. * 111.2);
            f.stdparallel1 = la[l];
          }
        }
      }
    }
  }
  return !(f.type == Grid::Type::not_set);
}

void scan_cf_grid_netcdf_file(InputNetCDFStream& istream, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << " ..." << endl;
  }
  grid_initialize();

  // open a file inventory unless this is a test run
  if (metautils::args.dsnum < "999.0") {
    gatherxml::fileInventory::open(inv_file, &inv_dir, inv_stream, "GrML",
        "nc2xml", USER);
  }
  auto attrs = istream.global_attributes();
  string source;
  for (const auto& a : attrs) {
    if (to_lower(a.name) == "source") {
      source = *(reinterpret_cast<string *>(a.values));
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
          "coordinate variable", F, "nc2xml", USER);
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
          log_error2("alternate latitude and longitude coordinate variables (" +
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
        Grid::GridDefinition f;
        if (filled_grid_projection(lats, lons, d, f)) {
          grid_dims.back() = d;
          grid_defs.back() = f;
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
          if (myequalf(lats[ny2 * nx + nx2], lats[(ny2 + 1) * nx + nx2],
              0.00001) && myequalf(lats[(ny2 + 1) * nx + nx2], lats[(ny2 + 1) *
              nx + nx2 + 1], 0.00001) && myequalf(lats[(ny2 + 1) * nx + nx2 +
              1], lats[ny2 * nx + nx2 + 1], 0.00001) && myequalf(fabs(lons[ny2 *
              nx + nx2]) + fabs(lons[(ny2 + 1) * nx + nx2 + 1]), 180., 0.001) &&
              myequalf(fabs(lons[(ny2 + 1) * nx + nx2]) + fabs(lons[ny2 * nx +
              nx2 + 1]), 180., 0.001)) {
            grid_defs.back().type = Grid::Type::polarStereographic;
            if (lats[ny2 * nx + nx2] > 0) {
              grid_defs.back().projection_flag = 0;
              grid_defs.back().llatitude = 60.;
            } else {
              grid_defs.back().projection_flag = 1;
              grid_defs.back().llatitude = -60.;
            }
            grid_defs.back().olongitude = lroundf((lons[nx2] + lons[nx2 + 1]) /
                2.);
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
            const double RAD = 3.141592654 / 180.;
            grid_defs.back().dx = lroundf(asin(sqrt(sin(fabs(lats[min_m] - lats[
                min_m + 1]) / 2. * RAD) * sin(fabs(lats[min_m] - lats[min_m +
                1]) / 2. * RAD) + sin(fabs(lons[min_m] - lons[min_m + 1]) / 2. *
                RAD) * sin(fabs(lons[min_m] - lons[min_m + 1]) / 2. * RAD) *
                cos(lats[min_m] * RAD) * cos(lats[min_m + 1] * RAD))) *
                12745.6);
            grid_defs.back().dy = lroundf(asin(sqrt(sin(fabs(lats[min_m] - lats[
                min_m + xd]) / 2. * RAD) * sin(fabs(lats[min_m] - lats[min_m +
                xd]) / 2. * RAD) + sin(fabs(lons[min_m] - lons[min_m + xd]) / 2.
                * RAD) * sin(fabs(lons[min_m] - lons[min_m + xd]) / 2. * RAD) *
                cos(lats[min_m] * RAD) * cos(lats[min_m + xd] * RAD))) *
                12745.6);
          } else {
            auto londiff = fabs(lons[1] - lons[0]);
            for (size_t n = 0; n < ny; ++n) {
              for (size_t m = 1; m < nx; ++m) {
                auto x = n * nx + m;
                if (!myequalf(fabs(lons[x] - lons[x - 1]), londiff, 0.000001)) {
                  londiff = 1.e36;
                  n = ny;
                  m = nx;
                }
              }
            }
            if (!myequalf(londiff, 1.e36)) {
              auto latdiff = fabs(lats[1] - lats[0]);
              for (size_t m = 0; m < nx; ++m) {
                for (size_t n = 1; n < ny; ++n) {
                  auto x = m * ny + n;
                  if (!myequalf(fabs(lats[x] - lats[x - 1]), latdiff,
                      0.000001)) {
                    latdiff = 1.e36;
                    m = nx;
                    n = ny;
                  }
                }
              }
              if (!myequalf(latdiff, 1.e36)) {
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
                    if (!myequalf(fabs(lats[x] - lats[x - 1]), latdiff,
                        0.000001)) {
                      latdiff = 1.e36;
                      n = ny;
                      m = nx;
                    }
                  }
                }
                if (!myequalf(latdiff, 1.e36)) {
                  const double PI = 3.141592654;
                  auto a = log(tan(PI / 4. + lats[0] * PI / 360.));
                  auto b = log(tan(PI / 4. + lats[ny2 * nx] * PI / 360.));
                  auto c = log(tan(PI / 4. + lats[ny2 * 2 * nx] * PI / 360.));
                  if (myequalf((b - a) / ny2, (c - a) / (ny2 * 2), 0.000001)) {
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
            log_error2("unable to determine grid projection", F, "nc2xml",
                USER);
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
    for (const auto& y : grid_data.lats) {
      if (y.dim > 100) {
        size_t m = y.dim / 10000 - 1;
        size_t l = (y.dim % 10000) / 100 - 1;
        if (grid_data.levels.size() > 0 && grid_data.levels.back().dim !=
            MISSING_FLAG) {
          grid_data.levels.emplace_back();
          grid_data.levels.back().dim = MISSING_FLAG;
          grid_data.levdata.ID.emplace_back();
          grid_data.levdata.description.emplace_back();
          grid_data.levdata.units.emplace_back();
          grid_data.levdata.write.emplace_back(false);
        }
        for (const auto& v : vars) {
          if (!v.is_coord && v.dimids.size() == 4 && v.dimids[0] == grid_data.
              time.dim && v.dimids[2] == m && v.dimids[3] == l) {

            // check netCDF variables for what they are using as a level
            //    dimension
            if (grid_data.levels.back().dim == MISSING_FLAG) {
              if (already_identified_levdimids.find(v.dimids[1]) ==
                  already_identified_levdimids.end()) {
                grid_data.levels.back().dim = v.dimids[1];
                already_identified_levdimids.emplace(grid_data.levels.back().
                    dim);
              }
            } else if (grid_data.levels.back().dim != v.dimids[1]) {
              log_error2("found multiple level dimensions for the gridded "
                  "parameters - failed on parameter '" + v.name + "'", F,
                  "nc2xml", USER);
            }
          }
        }
      }
    }

    // pop any unused levels
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
        cout << "... found " << grid_data.levels.size() << " level coordinates"
            << endl;
      }
      for (size_t n = 0; n < grid_data.levels.size(); ++n) {
        for (const auto& v : vars) {
          if (!v.is_coord && v.dimids.size() == 1 && v.dimids[0] == grid_data.
              levels[n].dim) {
            grid_data.levels[n].id = v.name;
            grid_data.levels[n].type = v.data_type;
            string d, u;
            for (const auto& a : v.attrs) {
              if (a.name == "description" && a.data_type == NetCDF::DataType::
                  CHAR) {
                d = *(reinterpret_cast<string *>(a.values));
              } else if (a.name == "units" && a.data_type == NetCDF::DataType::
                  CHAR) {
                u = *(reinterpret_cast<string *>(a.values));
              }
            }
            grid_data.levdata.ID[n] = v.name + "@@" + u;
            if (d.empty()) {
              grid_data.levdata.description[n] = v.name;
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
    log_error2("unable to determine the level coordinate variable", F,
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
  if (!grid_data.time.id.empty() && grid_data.lats.size() > 0 && grid_data.lons.
      size() > 0) {
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
      NetCDF::VariableData vd;
      istream.variable_data(grid_data.time.id, vd);
      time_s.t1 = vd.front();
      time_s.t2 = vd.back();
      time_s.num_times = vd.size();
      if (inv_stream.is_open()) {
        time_s.times = new double[time_s.num_times];
        for (size_t m = 0; m < vd.size(); ++m) {
          time_s.times[m] = vd[m];
        }
      }
      string e;
      tre.data->instantaneous.first_valid_datetime = metautils::NcTime::
          actual_date_time(time_s.t1, time_data, e);
      if (!e.empty()) {
        log_error2(e, F, "nc2xml", USER);
      }
      tre.data->instantaneous.last_valid_datetime = metautils::NcTime::
          actual_date_time(time_s.t2, time_data, e);
      if (!e.empty()) {
        log_error2(e, F, "nc2xml", USER);
      }
      if (gatherxml::verbose_operation) {
        cout << "   ...setting temporal range to:" << endl;
        cout << "      " << tre.data->instantaneous.first_valid_datetime.
            to_string() << " to " << tre.data->instantaneous.
            last_valid_datetime.to_string() << endl;
      }
      tre.data->num_steps = vd.size();
      if (!grid_data.time_bounds.id.empty()) {
        if (gatherxml::verbose_operation) {
          cout << "   ...adjusting times for time bounds" << endl;
        }
        for (const auto& v : vars) {
          if (v.name == grid_data.time_bounds.id) {
            grid_data.time_bounds.type = v.data_type;
            break;
          }
        }
        if (grid_data.time_bounds.type == NetCDF::DataType::_NULL) {
          log_error2("unable to determine type of time bounds", F, "nc2xml",
              USER);
        }
        istream.variable_data(grid_data.time_bounds.id, vd);
        if (vd.size() != time_s.num_times * 2) {
          log_error2("unable to handle more than two time bounds values per "
              "time", F, "nc2xml", USER);
        }
        time_bounds_s.t1 = vd.front();
        time_bounds_s.diff = vd[1] - time_bounds_s.t1;
        for (size_t l = 2; l < vd.size(); l += 2) {
          double diff = vd[l + 1] - vd[l];
          if (!myequalf(diff, time_bounds_s.diff)) {
            if (time_data.units != "days" || time_bounds_s.diff < 28 ||
                time_bounds_s.diff > 31 || diff < 28 || diff > 31) {
              time_bounds_s.changed = true;
            }
          }
        }
        time_bounds_s.t2 = vd.back();
        string e;
        tre.data->bounded.first_valid_datetime = metautils::NcTime::
            actual_date_time(time_bounds_s.t1, time_data, e);
        if (!e.empty()) {
          log_error2(e, F, "nc2xml", USER);
        }
        tre.data->bounded.last_valid_datetime = metautils::NcTime::
            actual_date_time(time_bounds_s.t2, time_data, e);
        if (!e.empty()) {
          log_error2(e, F, "nc2xml", USER);
        }
        if (gatherxml::verbose_operation) {
          cout << "      ...now temporal range is:" << endl;
          cout << "         " << tre.data->bounded.first_valid_datetime.
              to_string() << " to " << tre.data->bounded.last_valid_datetime.
              to_string() << endl;
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
        NetCDF::VariableData vd;
        istream.variable_data(grid_data.lats[n].id, vd);
        grid_dims.emplace_back(Grid::GridDimensions());
        grid_dims.back().y = vd.size();
        grid_defs.back().slatitude = vd.front();
        grid_defs.back().elatitude = vd.back();
        grid_defs.back().laincrement = fabs((grid_defs.back().elatitude -
            grid_defs.back().slatitude) / (vd.size() - 1));
        if (grid_data.lons[n].dim != MISSING_FLAG) {

          // check for gaussian lat-lon
          if (!myequalf(fabs(vd[1] - vd[0]), grid_defs.back().laincrement,
              0.001) && myequalf(vd.size() / 2., vd.size() / 2,
              0.00000000001)) {
            grid_defs.back().type = Grid::Type::gaussianLatitudeLongitude;
            grid_defs.back().laincrement = vd.size() / 2;
          }
          if (!grid_data.lats_b[n].id.empty()) {
            if (grid_data.lons_b[n].id.empty()) {
              log_error2("found a lat bounds but no lon bounds", F, "nc2xml",
                  USER);
            }
            istream.variable_data(grid_data.lats_b[n].id, vd);
            grid_defs.back().slatitude = vd.front();
            grid_defs.back().elatitude = vd.back();
            grid_defs.back().is_cell = true;
          }

          // get the longitude range
          istream.variable_data(grid_data.lons[n].id, vd);
          grid_dims.back().x = vd.size();
          grid_defs.back().slongitude = vd.front();
          grid_defs.back().elongitude = vd.back();
          grid_defs.back().loincrement = fabs((grid_defs.back().elongitude -
              grid_defs.back().slongitude) / (vd.size() - 1));
          if (!grid_data.lons_b[n].id.empty()) {
            if (grid_data.lats_b[n].id.empty()) {
              log_error2("found a lon bounds but no lat bounds", F, "nc2xml",
                  USER);
            }
            istream.variable_data(grid_data.lons_b[n].id, vd);
            grid_defs.back().slongitude = vd.front();
            grid_defs.back().elongitude = vd.back();
          }
        }
      }
    }
    for (size_t m = 0; m < grid_data.levels.size(); ++m) {
      auto levid = grid_data.levdata.ID[m].substr(0, grid_data.levdata.ID[m].
          find("@@"));
      NetCDF::VariableData lvd;
      size_t nl;
      if (grid_data.levels[m].type == NetCDF::DataType::_NULL) {
        nl = 1;
      } else {
        istream.variable_data(levid, lvd);
        nl = lvd.size();
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
                  grid_data.time.id, grid_data.time.dim, grid_data.levels[m].
                  dim, grid_data.lats[k].dim, grid_data.lons[k].dim, tre,
                  parameter_table, scan_data);
              for (size_t n = 0; n < nl; ++n) {
                lentry_p->key = "ds" + metautils::args.dsnum + "," + grid_data.
                    levdata.ID[m] + ":";
                switch (grid_data.levels[m].type) {
                  case NetCDF::DataType::INT: {
                    lentry_p->key += itos(lvd[n]);
                    break;
                  }
                  case NetCDF::DataType::FLOAT:
                  case NetCDF::DataType::DOUBLE: {
                    lentry_p->key += ftos(lvd[n], floatutils::precision(lvd[n])
                        + 2);
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
                        grid_data.time.dim, grid_data.levels[m].dim, grid_data.
                        lats[k].dim, grid_data.lons[k].dim, istream);
                  }
                  grid_data.levdata.write[m] = true;
                }
              }
              if (gentry_p->level_table.size() > 0) {
                gridtbl_p->insert(*gentry_p);
              }
            } else {

              // existing grid - needs update
              for (size_t n = 0; n < nl; ++n) {
                lentry_p->key = "ds" + metautils::args.dsnum + "," + grid_data.
                    levdata.ID[m] + ":";
                switch (grid_data.levels[m].type) {
                  case NetCDF::DataType::INT: {
                    lentry_p->key += itos(lvd[n]);
                    break;
                  }
                  case NetCDF::DataType::FLOAT:
                  case NetCDF::DataType::DOUBLE: {
                    lentry_p->key += ftos(lvd[n], floatutils::precision(lvd[n])
                        + 2);
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
                      key, grid_data.time.id, grid_data.time.dim, grid_data.
                      levels[m].dim, grid_data.lats[k].dim, grid_data.lons[k].
                      dim, tre, parameter_table, scan_data);
                  if (lentry_p->parameter_code_table.size() > 0) {
                    gentry_p->level_table.insert(*lentry_p);
                    if (inv_stream.is_open()) {
                      add_level_to_inventory(lentry_p->key, gentry_p->key,
                          grid_data.time.dim, grid_data.levels[m].dim,
                          grid_data.lats[k].dim, grid_data.lons[k].dim,
                          istream);
                    }
                    grid_data.levdata.write[m] = true;
                  }
                } else {

                  // run through all of the parameters
                  for (const auto& v : vars) {
                    if (!v.is_coord && v.dimids[0] == grid_data.time.dim && ((v.
                        dimids.size() == 4 && grid_data.levels[m].dim >= 0 && v.
                        dimids[1] == grid_data.levels[m].dim && v.dimids[2] ==
                        grid_data.lats[k].dim && v.dimids[3] == grid_data.lons[
                        k].dim) || (v.dimids.size() == 3 && grid_data.levels[m].
                        dim < 0 && v.dimids[1] == grid_data.lats[k].dim && v.
                        dimids[2] == grid_data.lons[k].dim))) {
                      pentry_p->key = "ds" + metautils::args.dsnum + ":" + v.
                          name;
                      auto time_method = gridded_time_method(v, grid_data.time.
                          id);
                      time_method = strutils::capitalize(time_method);
                      if (!lentry_p->parameter_code_table.found(pentry_p->key,
                          *pentry_p)) {
                        if (time_method.empty() || (myequalf(time_bounds_s.t1,
                            0, 0.0001) && myequalf(time_bounds_s.t1,
                            time_bounds_s.t2, 0.0001))) {
                          add_gridded_netcdf_parameter(v, tre.data->
                              instantaneous.first_valid_datetime, tre.data->
                              instantaneous.last_valid_datetime, tre.data->
                              num_steps, parameter_table, scan_data);
                        } else {
                          if (time_bounds_s.changed) {
                            log_error2("time bounds changed", F, "nc2xml",
                                USER);
                          }
                          add_gridded_netcdf_parameter(v, tre.data->bounded.
                              first_valid_datetime, tre.data->bounded.
                              last_valid_datetime, tre.data->num_steps,
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
                        auto tr_description=metautils::NcTime::
                            gridded_netcdf_time_range_description(tre,
                            time_data, time_method, error);
                        if (!error.empty()) {
                          log_error2(error, F, "nc2xml", USER);
                        }
                        tr_description = strutils::capitalize(tr_description);
                        if (strutils::has_ending(gentry_p->key,
                            tr_description)) {
                          if (time_method.empty() || (myequalf(time_bounds_s.t1,
                              0, 0.0001) && myequalf(time_bounds_s.t1,
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
                              pentry_p->end_date_time=tre.data->bounded.
                                  last_valid_datetime;
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
      log_error2("can't create temporary directory for netCDF levels", F,
          "nc2xml", USER);
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
      log_error2("can't create directory tree for netCDF levels", F, "nc2xml",
          USER);
    }
    std::ofstream ofs((tdir->name() + "/metadata/LevelTables/netCDF.ds" +
        metautils::args.dsnum + ".xml").c_str());
    if (!ofs.is_open()) {
      log_error2("can't open output for writing netCDF levels", F, "nc2xml",
          USER);
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
        ofs << "    <units>" << grid_data.levdata.units[m] << "</units>" <<
            endl;
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
      log_error2("no grids found - no content metadata will be generated", F,
           "nc2xml", USER);
    }
    log_error2("time coordinate variable not found - no content metadata will "
        "be generated", F, "nc2xml", USER);
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
  gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"GrML",F,"nc2xml",USER);
  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (to_lower(attrs[n].name) == "simulation_start_date") {
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
        if (vars[n].attrs[m].data_type == NetCDF::DataType::CHAR && vars[n].attrs[m].name == "description") {
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
            tre.data->instantaneous.first_valid_datetime=metautils::NcTime::actual_date_time(var_data.front(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,F,"nc2xml",USER);
            }
            tre.data->instantaneous.last_valid_datetime=metautils::NcTime::actual_date_time(var_data.back(),time_data,error);
            if (!error.empty()) {
              metautils::log_error(error,F,"nc2xml",USER);
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
      } else {
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
  mysystem2("/bin/cp "+tmpfile->name()+"/netCDF.ds"+metautils::args.dsnum+".xml /glade/u/home/rdadata/share/metadata/LevelTables/",oss,ess);
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
                      metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),F,"nc2xml",USER);
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
                    metautils::log_error("scan_wrf_simulation_netcdf_file() can't get times for data_type "+itos(static_cast<int>(vars[n].data_type)),F,"nc2xml",USER);
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
  auto attrs = istream.global_attributes();
  string ft, lft, p;
  for (const auto& a : attrs) {
    if (a.name == "featureType") {
      ft = *(reinterpret_cast<string *>(a.values));
      lft = to_lower(ft);
      trim(ft);
    } else if (a.name == "platform") {
      p = *(reinterpret_cast<string *>(a.values));
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
          F, "nc2xml", USER);
    }
    scan_data.map_name += ".p";
  }
  if (!ft.empty()) {
    if (!scan_data.map_name.empty() && scan_data.datatype_map.fill(scan_data.
        map_name)) {
      scan_data.map_filled = true;
    }
    string pt = "unknown";
    if (!p.empty()) {
      MySQL::Server server(metautils::directives.database_server, metautils::
          directives.metadb_username, metautils::directives.metadb_password,
          "");
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
      log_error2("featureType '" + ft + "' not recognized", F, "nc2xml", USER);
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
  auto attrs = istream.global_attributes();
  for (const auto& a : attrs) {
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
    if (cset.size() == 0) {
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
  auto vars = istream.variables();
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
      extract_from_variable_attribute(v.attrs, v.data_type, ltad);
    } else if (regex_search(l, regex("lon")) && cset.find(v.name) != cset.
        end()) {
      istream.variable_data(v.name, xvd);
      extract_from_variable_attribute(v.attrs, v.data_type, lnad);
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
      if (avd.size() > 0) {
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
    InputNetCDFStream::Attribute>& attr, size_t index, NetCDF::DataType
    time_type) {
  static const string F = this_function_label(__func__);
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
  auto dims = istream.dimensions();
  auto vars = istream.variables();
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
  if (ids.size() == 0) {
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
  if (yvd.size() == 0) {
    log_error2("latitude variable could not be identified", F, "nc2xml", USER);
  }
  istream.variable_data("lon", xvd);
  if (xvd.size() == 0) {
    log_error2("longitude variable could not be identified", F, "nc2xml", USER);
  }
  istream.variable_data("low_level", lvd);
  if (lvd.size() == 0) {
    log_error2("'low_level' variable could not be identified", F, "nc2xml",
        USER);
  }
  istream.variable_data("high_level", hvd);
  if (hvd.size() == 0) {
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
    if (v.dimids.size() > 0 && v.dimids.front() == idd) {
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
          extract_from_variable_attribute(v.attrs, vd.type(), ad);
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
  auto attrs = istream.global_attributes();
  string s = "";
  for (const auto& a : attrs) {
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
  auto dims = istream.dimensions();
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
  if (istream.variable_data("hdr_arr", vd) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_arr' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].lat=vd[n * 3];
    hds[n].lon=vd[n * 3 + 1];
    hds[n].elev=vd[n * 3 + 2];
  }
  if (istream.variable_data("hdr_typ", vd) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_typ' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].type.assign(&(reinterpret_cast<char *>(vd.get()))[n * 16], 16);
  }
  if (istream.variable_data("hdr_sid", vd) == NetCDF::DataType::_NULL) {
    log_error2("could not get 'hdr_sid' data", F, "nc2xml", USER);
  }
  for (size_t n = 0; n < nhds; ++n) {
    hds[n].ID.assign(&(reinterpret_cast<char *>(vd.get()))[n * 16], 16);
  }
  if (istream.variable_data("hdr_vld", vd) == NetCDF::DataType::_NULL) {
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
  if (istream.variable_data("obs_arr", vd) == NetCDF::DataType::_NULL) {
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
  if (istream.variable_data("latitude", yvd) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("lat", yvd) == NetCDF::DataType::_NULL) {
      log_error2("variable 'latitude' not found", F, "nc2xml", USER);
    }
  }
  if (istream.variable_data("longitude", xvd) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("lon", xvd) == NetCDF::DataType::_NULL) {
      log_error2("variable 'longitude' not found", F, "nc2xml", USER);
    }
  }
  if (istream.variable_data("time_observation", tvd) == NetCDF::DataType::
      _NULL) {
    if (istream.variable_data("time_obs", tvd) == NetCDF::DataType::_NULL) {
      log_error2("variable 'time_observation' not found", F, "nc2xml", USER);
    }
  }
  int fmt = -1;
  if (istream.variable_data("report_id", rvd) == NetCDF::DataType::_NULL) {
    if (istream.variable_data("stn_name", rvd) == NetCDF::DataType::_NULL) {
      log_error2("variable 'report_id' not found", F, "nc2xml", USER);
    } else {
      fmt = 0;
    }
  } else {
    if (istream.variable_data("parent_index", pvd) == NetCDF::DataType::_NULL) {
      log_error2("variable 'parent_index' not found", F, "nc2xml", USER);
    }
    fmt = 1;
  }
  if (tvd.size() == 0) {
    return;
  }
  NetCDF::DataValue tfv;
  auto ilen = rvd.size() / tvd.size();
  auto vars = istream.variables();
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
          fill_nc_time_data(v.attrs[m]);
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
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad);
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
  if (istream.variable_data("Lat", yvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("Lon", xvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("time_obs", tvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'time_obs' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("ship", shvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'ship' not found", F, "nc2xml", USER);
  }
  if (tvd.size() == 0) {
    return;
  }
  auto ilen = shvd.size() / tvd.size();
  if (istream.variable_data("buoy", bvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'buoy' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("stnType", svd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'stnType' not found", F, "nc2xml", USER);
  }
  NetCDF::DataValue tfv;
  auto vars = istream.variables();
  for (const auto& v : vars) {
    if (gatherxml::verbose_operation) {
      cout << "  netCDF variable: '" << v.name << "'" << endl;
    }
    if (v.name == "time_obs") {
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m]);
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.is_rec && v.name != "rep_type" && v.name != "zone" && v.name !=
        "buoy" && v.name != "ship" && !regex_search(v.name, regex("^time")) &&
        v.name != "Lat" && v.name != "Lon" && v.name != "stnType" && v.name !=
        "report") {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad);
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
  if (istream.variable_data("Lat", yvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("Lon", xvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'Lon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("time_obs",  tvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'time_obs' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("wmoId",  wvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'wmoId' not found", F, "nc2xml", USER);
  }
  NetCDF::DataValue tfv;
  auto vars = istream.variables();
  for (const auto& v : vars) {
    if (v.name == "time_obs") {
      for (size_t m = 0; m < v.attrs.size(); ++m) {
        if (v.attrs[m].name == "units") {
          fill_nc_time_data(v.attrs[m]);
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.is_rec && v.name != "rep_type" && v.name != "wmoId" && v.name
        != "stnName" && !regex_search(v.name, regex("^time")) && v.name != "Lat"
        && v.name != "Lon" && v.name != "elev" && v.name != "stnType" && v.name
        != "report") {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad);
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
  if (istream.variable_data("staLat", yvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staLat' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("staLon", xvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staLon' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("synTime", tvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'synTime' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("wmoStaNum", wvd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'wmoStaNum' not found", F, "nc2xml", USER);
  }
  if (istream.variable_data("staName", svd) == NetCDF::DataType::_NULL) {
    log_error2("variable 'staName' not found", F, "nc2xml", USER);
  }
  if (tvd.size() == 0) {
    return;
  }
  auto ilen = svd.size() / tvd.size();
  NetCDF::DataValue tfv;
  auto vars = istream.variables();
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
          fill_nc_time_data(v.attrs[m]);
        } else if (v.attrs[m].name == "_FillValue") {
          set_time_missing_value(tfv, v.attrs, m, tvd.type());
        }
      }
    } else if (v.name == "wmoStaNum") {
      auto a = v.attrs;
      extract_from_variable_attribute(a, wvd.type(), nc_wmoid_a_data);
    } else if (v.is_rec && (v.name == "numMand" || v.name == "numSigT" || v.name
        == "numSigW" || v.name == "numMwnd" || v.name == "numTrop")) {
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
        log_error2("unable to get data for variable '" + v.name + "'", F,
            "nc2xml", USER);
      }
      auto a = v.attrs;
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(a, vd.type(), ad);
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
          if (nc_wmoid_a_data.missing_value.type() != NetCDF::DataType::_NULL) {
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
  auto attrs=istream.global_attributes();
  string typ;
  for (const auto& a : attrs) {
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
  auto attrs = istream.global_attributes();
  for (const auto& a : attrs) {
    if (a.name == "ID") {
      ientry.key = ptyp + "[!]callSign[!]" + *(reinterpret_cast<string *>(a.
          values));
    }
  }

  // find the coordinate variables
  string tvn, yvn, xvn;
  NetCDF::VariableData tvd, yvd, xvd;
  auto tm_b = false, lt_b = false, ln_b = false;
  auto vars = istream.variables();
  for (const auto& v : vars) {
    for (const auto& a : v.attrs) {
      if (a.data_type == NetCDF::DataType::CHAR && a.name == "units") {
        auto s = to_lower(*(reinterpret_cast<string *>(a.values)));
        if (v.is_coord && regex_search(s, regex("since"))) {
          if (tm_b) {
            log_error2("time was already identified - don't know what to do "
                "with variable: " + v.name, F, "nc2xml", USER);
          }
          fill_nc_time_data(a);
          tm_b = true;
          tvn = v.name;
          if (istream.variable_data(v.name, tvd) == NetCDF::DataType::_NULL) {
            log_error2("unable to get tvd", F, "nc2xml", USER);
          }
          if (tvd.size() == 0) {
            tm_b = false;
          }
        } else if (v.dimids.size() == 1 && v.dimids[0] == 0) {
          if (s == "degrees_north") {
            if (istream.variable_data(v.name, yvd) == NetCDF::DataType::_NULL) {
              log_error2("unable to get latitudes", F, "nc2xml", USER);
            }
            yvn = v.name;
            lt_b = true;
          } else if (s == "degrees_east") {
            if (istream.variable_data(v.name, xvd) == NetCDF::DataType::_NULL) {
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
  gatherxml::fileInventory::open(inv_file, &inv_dir, inv_stream, "ObML",
      "nc2xml", USER);
  if (inv_stream.is_open()) {
    inv_stream << "netCDF:point|" << istream.record_size() << endl;
    if (O_map.find("surface") == O_map.end()) {
      O_map.emplace("surface", make_pair(O_map.size(), ""));
    }
    if (P_map.find(ptyp) == P_map.end()) {
      P_map.emplace(ptyp, make_pair(P_map.size(), ""));
    }
  }

  // find the data variables
  unordered_map<size_t, string> T_map;
  unordered_set<string> vset;
  float mn = 99., mx = -99.;
  for (const auto& v : vars) {
    if (v.name != tvn && v.name != yvn && v.name != xvn) {
      NetCDFVariableAttributeData ad;
      extract_from_variable_attribute(v.attrs, v.data_type, ad);
      NetCDF::VariableData vd;
      if (istream.variable_data(v.name, vd) == NetCDF::DataType::_NULL) {
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
      if (D_map.find(v.name) == D_map.end()) {
        auto byts = 1;
        auto dims = istream.dimensions();
        for (const auto& d : v.dimids) {
          byts *= dims[d].length;
        }
        switch (v.data_type) {
          case NetCDF::DataType::SHORT: {
            byts *= 2;
            break;
          }
          case NetCDF::DataType::INT:
          case NetCDF::DataType::FLOAT: {
            byts *= 4;
            break;
          }
          case NetCDF::DataType::DOUBLE: {
            byts *= 8;
            break;
          }
          default: { }
        }
        D_map.emplace(v.name, make_pair(D_map.size(), "|" + lltos(v.offset) +
            "|" + NetCDF::data_type_str[static_cast<int>(v.data_type)] + "|" +
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
          if (inv_stream.is_open()) {
            if (T_map.find(m) == T_map.end()) {
              T_map.emplace(m, dt.to_string("%Y%m%d%H%MM") + "[!]" + ftos(yvd[
                  m], 4) + "[!]" + ftos(lon, 4));
            }
          }
          if (!obs_data.added_to_ids("surface", ientry, v.name, "", yvd[
              m], lon, tvd[m], &dt)) {
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
          if (inv_stream.is_open()) {
            sv.emplace_back(itos(m) + "|0|" + itos(P_map[ptyp].first) + "|" +
                itos(I_map[ientry.key.substr(ientry.key.find("[!]") + 3)].first)
                + "|" + itos(D_map[v.name].first));
          }
        }
      }
      if (inv_stream.is_open()) {
        if (sv.size() != tvd.size()) {
          for (const auto& e : sv) {
            inv_lines2.writeln(e);
          }
        } else {
          D_map.erase(v.name);
        }
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
  if (inv_stream.is_open()) {
    size_t w, e;
    bitmap::longitudeBitmap::west_east_bounds(ientry.data->min_lon_bitmap.get(),
        w, e);
    auto k = ientry.key.substr(ientry.key.find("[!]") + 3) + "[!]" + ftos(mn, 4)
        + "[!]" + ftos(ientry.data->min_lon_bitmap[w], 4) + "[!]" + ftos(mx, 4)
        + "[!]" + ftos(ientry.data->max_lon_bitmap[ e], 4);
    if (I_map.find(k) == I_map.end()) {
      I_map.emplace(k, make_pair(I_map.size(), ""));
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
      inv_stream << "T<!>" << e << "<!>" << T_map[e] << endl;
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
  if (mysystem2("/bin/mkdir -p " + tempdir_name + "/metadata/ParameterTables",
      oss, ess) < 0) {
    log_error2("can't create directory tree for netCDF variables", F, "nc2xml",
        USER);
  }
  scan_data.map_name = tempdir_name + "/metadata/ParameterTables/netCDF.ds" +
      metautils::args.dsnum + ".xml";
  std::ofstream ofs(scan_data.map_name.c_str());
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
  if (unixutils::rdadata_sync(tempdir_name, "metadata/ParameterTables/",
      "/data/web", metautils::directives.rdadata_home, e) < 0) {
    log_warning("parameter map was not synced - error(s): '" + e + "'",
        "nc2xml", USER);
  }
  mysystem2("/bin/cp " + scan_data.map_name + " /glade/u/home/rdadata/share"
      "/metadata/ParameterTables/netCDF.ds" + metautils::args.dsnum + ".xml",
      oss, ess);
  if (gatherxml::verbose_operation) {
    cout << "...finished writing parameter map." << endl;
  }
}

void scan_file(ScanData& scan_data, gatherxml::markup::ObML::ObservationData&
     obs_data) {
  static const string F = this_function_label(__func__);
  unique_ptr<TempFile> tfile(new TempFile);
  tfile->open(metautils::args.temp_loc);
  unique_ptr<TempDir> tdir(new TempDir);
  tdir->create(metautils::args.temp_loc);
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/netCDF.ds" + metautils::args.dsnum + ".xml",
      tdir->name());
  list<string> flst;
  string ff, e;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
      *tdir, &flst, ff, e)) {
    log_error2(e, F + ": prepare_file_for_metadata_scanning()", "nc2xml", USER);
  }
  if (flst.size() == 0) {
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
    istream.open(f.c_str());
    conventions = "";
    auto gattrs = istream.global_attributes();
    for (const auto& a : gattrs) {
      if (a.name == "Conventions") {
        conventions = *(reinterpret_cast<string *>(a.values));
      }
    }
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
  if (!metautils::args.inventory_only && scan_data.netcdf_variables.size() >
      0) {
    write_parameter_map(tdir->name(), scan_data);
  }
}

int main(int argc, char **argv) {
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
    cerr << "-f rafnetcdf     NCAR-RAF/nimbus compliant netCDF3 aircraft data"
        << endl;
    cerr << "-f samosnc       SAMOS netCDF3 data" << endl;
//    cerr << "-f wrfsimnetcdf  Climate Simulations from WRF" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data "
        "file belongs" << endl;
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
    exit(1);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  auto d = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, d);
  metautils::read_config("nc2xml", USER);
  gatherxml::parse_args(d);
  atexit(clean_up);
  metautils::cmd_register("nc2xml", USER);
  if (!metautils::args.overwrite_only && !metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("GrML");
    metautils::check_for_existing_cmd("ObML");
  }
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
      tdir = gatherxml::markup::GrML::write(*gridtbl_p, "nc2xml", USER);
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
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " " + f + " " + metautils::args.filename + "." +
            ext, oss, ess) < 0) {
      log_error2(ess.str(), "main() running scm", "nc2xml", USER);
    }
    if (gatherxml::verbose_operation) {
      cout << "...'scm' finished." << endl;
    }
  } else if (metautils::args.dsnum == "999.9") {
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
  if (inv_stream.is_open()) {
    vector<pair<int, string>> v;
    if (sd.write_type == ScanData::GrML_type) {
      sort_inventory_map(U_map, v);
      for (const auto& e : v) {
        inv_stream << "U<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(G_map, v);
      for (const auto& e : v) {
        inv_stream << "G<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(L_map, v);
      for (const auto& e : v) {
        inv_stream << "L<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(P_map, v);
      for (const auto& e : v) {
        inv_stream << "P<!>" << e.first << "<!>" << e.second;
        if (is_large_offset) {
          inv_stream << "<!>BIG";
        }
        inv_stream << endl;
      }
      sort_inventory_map(R_map, v);
      for (const auto& e : v) {
        inv_stream << "R<!>" << e.first << "<!>" << e.second << endl;
      }
    } else if (sd.write_type == ScanData::ObML_type) {
      sort_inventory_map(O_map, v);
      for (const auto& e : v) {
        inv_stream << "O<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(P_map, v);
      for (const auto& e : v) {
        inv_stream << "P<!>" << e.first << "<!>" << e.second << endl;
      }
      sort_inventory_map(I_map, v);
      for (const auto& e : v) {
        inv_stream << "I<!>" << e.first << "<!>" << e.second << "[!]" << I_map[
            e.second].second << endl;
      }
      sort_inventory_map(D_map, v);
      for (const auto& e : v) {
        inv_stream << "D<!>" << e.first << "<!>" << e.second << D_map[e.second].
            second << endl;
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
        char l[32768];
        ifs.getline(l, 32768);
        while (!ifs.eof()) {
          inv_stream << l << endl;
          ifs.getline(l, 32768);
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
