#include <iostream>
#include <sstream>
#include <memory>
#include <deque>
#include <regex>
#include <unordered_map>
#include <unordered_set>
#include <numeric>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <gatherxml.hpp>
#include <pglocks.hpp>
#include <hdf.hpp>
#include <netcdf.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <timer.hpp>
#include <gridutils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using floatutils::myequalf;
using metautils::NcTime::TimeBounds;
using metautils::NcTime::TimeBounds2;
using metautils::NcTime::TimeData;
using metautils::NcTime::TimeRange;
using metautils::NcTime::TimeRangeEntry;
using metautils::NcTime::TimeRangeEntry2;
using metautils::NcTime::actual_date_time;
using metautils::log_error2;
using miscutils::this_function_label;
using std::accumulate;
using std::ceil;
using std::cerr;
using std::cout;
using std::endl;
using std::floor;
using std::make_pair;
using std::move;
using std::pair;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::capitalize;
using strutils::ftos;
using strutils::itos;
using strutils::split;
using strutils::to_lower;
using strutils::trim;
using strutils::vector_to_string;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

typedef unordered_map<string, InputHDF5Stream::DataValue> AttributeMap;
typedef shared_ptr<InputHDF5Stream::Dataset> DatasetPointer;

/*****************************************************************************/
/* global variables                                                          */
/*****************************************************************************/

enum class ISTREAM_TYPE { _NULL, _HDF4, _HDF5 };
ISTREAM_TYPE g_istream_type;
unique_ptr<idstream> g_istream(nullptr);
stringstream g_warn_ss;
auto& g_dsid = metautils::args.dsnum; // alias
const metautils::UtilityIdentification g_util_ident("hdf2xml", USER);
unique_ptr<TempFile> g_work_file;

/*****************************************************************************/

struct ScanData {
  ScanData() : num_not_missing(0), write_type(-1), cmd_type(), tdir(nullptr),
      map_name(), platform_type(), varlist(), var_changes_table(), found_map(
      false), convert_ids_to_upper_case(false) { }

  enum {GrML_type = 1, ObML_type};
  size_t num_not_missing;
  int write_type;
  string cmd_type;
  unique_ptr<TempDir> tdir;
  string map_name, platform_type;
  std::list<string> varlist;
  unordered_set<string> var_changes_table;
  bool found_map, convert_ids_to_upper_case;
};

struct CoordinateVariables {
  CoordinateVariables() : nc_time(new TimeData), forecast_period(new TimeData),
    lat_ids(), lon_ids(), level_info()  { }

  shared_ptr<TimeData> nc_time;
  shared_ptr<TimeData> forecast_period;
  vector<string> lat_ids, lon_ids;
  vector<metautils::NcLevel::LevelInfo> level_info;
};

struct GridData {
  struct CoordinateData {
    CoordinateData() : id(), ds(nullptr), data_array() {}

    string id;
    DatasetPointer ds;
    HDF5::DataArray data_array;
  };
  GridData() : time_range_entries(), time_data(nullptr), reference_time(),
      valid_time(), time_bounds(), climo_bounds(), forecast_period(), lat(),
      lon(), level(), level_bounds(), coordinate_variables_set() {}

  unordered_map<double, TimeRangeEntry2> time_range_entries;
  shared_ptr<TimeData> time_data;
  CoordinateData reference_time, valid_time, time_bounds, climo_bounds,
      forecast_period, lat, lon, level, level_bounds;
  unordered_set<string> coordinate_variables_set;
};

struct DiscreteGeometriesData {
  DiscreteGeometriesData() : indexes(), z_units(), z_pos() {}

  struct Indexes {
    Indexes() : time_var(), stn_id_var(), lat_var(), lon_var(),
        instance_dim_var(), z_var(), sample_dim_vars() {}

    string time_var, stn_id_var, lat_var, lon_var, instance_dim_var, z_var;
    unordered_map<string, string> sample_dim_vars;
  };
  Indexes indexes;
  string z_units, z_pos;
};

struct Inventory {
  struct Map {
    Map() : map(), lst() { }

    unordered_map<string, pair<size_t, size_t>> map;
    vector<string> lst;
  };

  Inventory() : file(), dir(nullptr), stream(), U(), G(), L(), P(), R(),
      lines() { }

  string file;
  unique_ptr<TempDir> dir;
  std::ofstream stream;
  Map U, G, L, P, R;
  vector<string> lines;
} g_inv;

struct NetCDFVariableAttributeData {
  NetCDFVariableAttributeData() : long_name(), units(), cf_keyword(),
      missing_value() {}

  string long_name, units, cf_keyword;
  InputHDF5Stream::DataValue missing_value;
};

struct ParameterData {
  ParameterData() : set(), map() {}

  unordered_set<string> set;
  ParameterMap map;
};

typedef unordered_map<string, gatherxml::markup::GrML::GridEntry> GridTable;

struct GrMLData {
  struct G {
    G() : key(), entry() { }

    string key;
    gatherxml::markup::GrML::GridEntry entry;
  };

  struct L {
    L() : key(), entry() { }

    string key;
    gatherxml::markup::GrML::LevelEntry entry;
  };

  struct P {
    P() : key(), entry() { }

    string key;
    gatherxml::markup::GrML::ParameterEntry entry;
  };

  GrMLData() : gtb(), g(), l(), p() { }

  GridTable gtb;
  G g;
  L l;
  P p;
};

unique_ptr<GrMLData> g_grml_data;
unordered_set<string> unique_data_type_observation_set;
gatherxml::markup::ObML::DataTypeEntry de;
string xml_directory;

extern "C" void clean_up() {
  if (g_dsid < "999.0") {
    if (!g_warn_ss.str().empty()) {
      metautils::log_warning(g_warn_ss.str(), "hdf2xml", USER);
    }
    if (!myerror.empty()) {
      log_error2(myerror, "clean_up()", g_util_ident);
    }
  } else {
    if (!g_warn_ss.str().empty()) {
      cerr << g_warn_ss.str() << endl;
    }
    if (!myerror.empty()) {
      cerr << myerror << endl;
    }
  }
}

extern "C" void segv_handler(int) {
  clean_up();
  metautils::cmd_unregister();
  if (g_dsid < "999.0") {
    log_error2("core dump", "segv_handler()", g_util_ident);
  } else {
    cerr << "core dump" << endl;
    exit(1);
  }
}

extern "C" void int_handler(int) {
  clean_up();
  metautils::cmd_unregister();
}

void stream_set(ISTREAM_TYPE type) {
  g_istream_type = type;
  switch (type) {
    case ISTREAM_TYPE::_HDF4:
      g_istream.reset(new InputHDF4Stream);
      break;
    case ISTREAM_TYPE::_HDF5:
      g_istream.reset(new InputHDF5Stream);
      break;
    default:
      cerr << "Terminating - undefined input stream type" << endl;
      exit(1);
  }
}

InputHDF4Stream *sget_hdf4() {
  if (g_istream_type == ISTREAM_TYPE::_HDF4) {
    return reinterpret_cast<InputHDF4Stream *>(g_istream.get());
  }
  cerr << "Terminating - wrong input stream type" << endl;
  exit(1);
}

InputHDF5Stream *sget_hdf5() {
  if (g_istream_type == ISTREAM_TYPE::_HDF5) {
    return reinterpret_cast<InputHDF5Stream *>(g_istream.get());
  }
  cerr << "Terminating - wrong input stream type" << endl;
  exit(1);
}

void scan_quikscat_hdf4_file() {
  auto is = sget_hdf4();
is->print_data_descriptors(1965);
}

void scan_hdf4_file(std::list<string>& filelist, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  stream_set(ISTREAM_TYPE::_HDF4);
  auto is = sget_hdf4();

  for (const auto& file : filelist) {
    if (!is->open(file.c_str())) {
      auto error = move(myerror);
      log_error2(error + " - file: '" + file + "'", F, g_util_ident);
    }
    if (metautils::args.data_format == "quikscathdf4") {
      scan_quikscat_hdf4_file();
    }
    is->close();
  }
}

void extract_from_hdf5_variable_attributes(unordered_map<string,
    InputHDF5Stream::DataValue>& attributes,
    NetCDFVariableAttributeData& nc_attribute_data) {
  nc_attribute_data.long_name = "";
  nc_attribute_data.units = "";
  nc_attribute_data.cf_keyword = "";
  nc_attribute_data.missing_value.clear();
  for (const auto& attribute : attributes) {
    if (attribute.first == "long_name") {
      nc_attribute_data.long_name.assign(reinterpret_cast<char *>(attribute.
          second.array));
      trim(nc_attribute_data.long_name);
    } else if (attribute.first == "units") {
      nc_attribute_data.units.assign(reinterpret_cast<char *>(attribute.second.
          array));
      trim(nc_attribute_data.units);
    } else if (attribute.first == "standard_name") {
      nc_attribute_data.cf_keyword.assign(reinterpret_cast<char *>(attribute.
          second.array));
      trim(nc_attribute_data.cf_keyword);
    } else if (attribute.first == "_FillValue" || attribute.first ==
        "missing_value") {
      nc_attribute_data.missing_value = attribute.second;
    }
  }
}

bool found_missing(const double& time, HDF5::DataArray::Type time_type,
    const InputHDF5Stream::DataValue *time_missing_value,
    const HDF5::DataArray &var_vals, size_t var_val_index,
    const InputHDF5Stream::DataValue& var_missing_value) {
  static const string F = this_function_label(__func__);
  bool missing = false;
  if (time_missing_value != nullptr && time_missing_value->_class_ != -1) {
    switch (time_type) {
       case (HDF5::DataArray::Type::BYTE): {
         if (myequalf(time, *(reinterpret_cast<unsigned char *>(
             time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::SHORT): {
         if (myequalf(time, *(reinterpret_cast<short *>(time_missing_value->
             array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::INT): {
         if (myequalf(time, *(reinterpret_cast<int *>(time_missing_value->
             array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::LONG_LONG): {
         if (myequalf(time, *(reinterpret_cast<long long *>(time_missing_value->
             array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::FLOAT): {
         if (myequalf(time, *(reinterpret_cast<float *>(time_missing_value->
             array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::DOUBLE): {
         if (myequalf(time, *(reinterpret_cast<double *>(time_missing_value->
             array)))) {
           missing = true;
         }
         break;
       }
       default: {
         log_error2("can't check times of type " + itos(static_cast<int>(
             time_type)), F, g_util_ident);
       }
    }
  }
  if (missing) return true;
  if (var_missing_value.size > 0) {
    switch (var_vals.type) {
       case (HDF5::DataArray::Type::BYTE): {
         if (var_vals.byte_value(var_val_index) == *(reinterpret_cast<
             unsigned char *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::SHORT): {
         if (var_vals.short_value(var_val_index) == *(reinterpret_cast<short *>(
             var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::INT): {
         if (var_vals.int_value(var_val_index) == *(reinterpret_cast<int *>(
             var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::LONG_LONG): {
         if (var_vals.long_long_value(var_val_index) == *(reinterpret_cast<
             long long *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::FLOAT): {
         if (var_vals.float_value(var_val_index) == *(reinterpret_cast<float *>(
             var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::DOUBLE): {
         if (var_vals.double_value(var_val_index) == *(reinterpret_cast<
             double *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::STRING): {
         if (var_vals.string_value(var_val_index) == string(reinterpret_cast<
             char *>(var_missing_value.array), var_missing_value.size)) {
           missing = true;
         }
         break;
       }
       default: {
         log_error2("can't check variables of type " + itos(static_cast<int>(
             var_vals.type)), F, g_util_ident);
       }
    }
  }
  return missing;
}

string ispd_hdf5_platform_type(const std::tuple<string, string, float, float,
    short, short, char, bool>& library_entry) {
  string id, ispd_id;
  float lat, lon;
  short plat_type, isrc;
  char csrc;
  bool already_counted;
  std::tie(id, ispd_id, lat, lon, plat_type, isrc, csrc, already_counted) =
      library_entry;
  if (plat_type == -1) return "land_station";
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
      if (ispd_id == "008000" || ispd_id == "008001") return "unknown";
      return "drifting_buoy";
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
      if ((ispd_id == "001000" && ((csrc >= '2' && csrc <= '5') || (csrc >=
          'A' && csrc <= 'H') || csrc == 'N')) || ispd_id == "001003" ||
          (ispd_id == "001005" && plat_type == 1003) || ispd_id == "003002" ||
          ispd_id == "003004" || ispd_id == "003005" || ispd_id == "003006" ||
          ispd_id == "003007" || ispd_id == "003007" || ispd_id == "003008" ||
          ispd_id == "003009" || ispd_id == "003011" || ispd_id == "003014" ||
          ispd_id == "003015" || ispd_id == "003021" || ispd_id == "003022" ||
          ispd_id == "003026" || ispd_id == "004000" || ispd_id == "004003") {
        return "land_station";
      } else if (ispd_id == "002000") {
        if (id.length() == 5) {
          if (std::stoi(id) < 99000) {
            return "land_station";
          } else if (std::stoi(id) < 99100) {
            return "fixed_ship";
          } else {
            return "roving_ship";
          }
        } else {
          return "unknown";
        }
      } else if (ispd_id == "002001" || (ispd_id == "001005" && plat_type ==
          1006)) {
        return "unknown";
      } else if (ispd_id == "003010") {
        auto sp = split(id, "-");
        if (sp.size() == 2 && sp[1].length() == 5 &&
            strutils::is_numeric(sp[1])) {
          return "land_station";
        }
      } else if (ispd_id >= "010000" && ispd_id <= "019999") {
        if (plat_type == 1006 && id.length() == 5 &&
            strutils::is_numeric(id)) {
          if (id < "99000") {
            return "land_station";
          } else if (id >= "99200" && id <= "99299") {
            return "drifting_buoy";
          }
        } else {
          return "unknown";
        }
      } else {
        g_warn_ss << "unknown platform type (1) for station '" << id << "' "
            << ftos(lat, 4) << " " << ftos(lon, 4) << " " << ispd_id << " " <<
            itos(plat_type) << " " << itos(isrc) << " '" << string(1, csrc) <<
            "'" << endl;
        return "";
      }
    }
    default: {
      g_warn_ss << "unknown platform type (2) for station '" << id << "' " <<
          ftos(lat, 4) << " " << ftos(lon, 4) << " " << ispd_id << " " <<
          itos(plat_type) << " " << itos(isrc) << " '" << string(1, csrc) <<
          "'" << endl;
      return "";
    }
  }
}

string ispd_hdf5_id_entry(const std::tuple<string, string, float, float, short,
    short, char, bool>& library_entry, string platform_type, DateTime& dt) {
  string id, ispd_id;
  float lat, lon;
  short plat_type, isrc;
  char csrc;
  bool already_counted;
  std::tie(id, ispd_id, lat, lon, plat_type, isrc, csrc, already_counted) =
      library_entry;
  string ientry_key;
  if (isrc > 0 && !id.empty() && (id)[1] == ' ') {
    auto parts = split(id);
    ientry_key = platform_type + "[!]";
    switch (std::stoi(parts[0])) {
      case 2: {
        ientry_key += "generic[!]" + parts[1];
        break;
      }
      case 3: {
        ientry_key += "WMO[!]" + parts[1];
        break;
      }
      case 5: {
        ientry_key += "NDBC[!]" + parts[1];
        break;
      }
      default: {
        ientry_key += "[!]" + id;
      }
    }
  } else if (ispd_id == "001000") {
    if ((id)[6] == '-') {
      auto parts = split(id, "-");
      if (parts[0] != "999999") {
        ientry_key = platform_type + "[!]WMO+6[!]" + parts[0];
      } else {
        if (parts[1] != "99999") {
          ientry_key = platform_type + "[!]WBAN[!]" + parts[1];
        } else {
          g_warn_ss << "unknown ID type (1) for station '" << id << "' " <<
              ftos(lat, 4) << " " << ftos(lon, 4) << " " << ispd_id << " " <<
              itos(plat_type) << " " << itos(isrc) << " '" << string(1, csrc) <<
              "'" << endl;
        }
      }
    } else {
      g_warn_ss << "unknown ID type (2) for station '" << id << "' " <<
          ftos(lat, 4) << " " << ftos(lon, 4) << " " << ispd_id << " " <<
          itos(plat_type) << " " << itos(isrc) << " '" << string(1, csrc) <<
          "'" << endl;
    }
  } else if (ispd_id == "001002") {
    ientry_key = platform_type + "[!]WBAN[!]" + id;
  } else if (ispd_id == "001003") {
    ientry_key = platform_type + "[!]RUSSIA[!]" + id;
  } else if (ispd_id == "001005" || ispd_id == "001006") {
    if (plat_type >= 1001 && plat_type <= 1003 && strutils::is_numeric(id)) {
      if (id.length() == 5) {
        ientry_key = platform_type + "[!]WMO[!]" + id;
      } else if (id.length() == 6) {
        ientry_key = platform_type + "[!]WMO+6[!]" + id;
      }
    } else if (plat_type == 1002 && !strutils::is_numeric(id)) {
      ientry_key = platform_type + "[!]NAME[!]" + id;
    } else if (id == "999999999999") {
      ientry_key = platform_type + "[!]unknown[!]" + id;
    }
  } else if ((ispd_id == "001007" && plat_type == 1001) || ispd_id == "002000"
      || ispd_id == "003002" || ispd_id == "003008" || ispd_id == "003015" ||
      ispd_id == "004000" || ispd_id == "004001" || ispd_id == "004003") {
    ientry_key = platform_type + "[!]WMO[!]" + id;
  } else if (((ispd_id == "001011" && plat_type == 1002) || ispd_id ==
      "001007" || ispd_id == "004002" || ispd_id == "004004") &&
      !strutils::is_numeric(id)) {
    ientry_key = platform_type + "[!]NAME[!]" + id;
  } else if (ispd_id == "001012" && plat_type == 1002) {
    ientry_key = platform_type + "[!]COOP[!]" + id;
  } else if (ispd_id == "002001") {
    if (strutils::is_numeric(id)) {
      if (id.length() == 5) {
        if (dt.year() <= 1948) {
          ientry_key = platform_type + "[!]WBAN[!]" + id;
        } else {
          ientry_key = platform_type + "[!]WMO[!]" + id;
        }
      } else {
        ientry_key = platform_type + "[!]unknown[!]" + id;
      }
    } else {
      ientry_key = platform_type + "[!]callSign[!]" + id;
    }
  } else if (ispd_id == "003002" && strutils::is_numeric(id)) {
    if (id.length() == 5) {
      ientry_key = platform_type + "[!]WMO[!]" + id;
    } else if (id.length() == 6) {
      ientry_key = platform_type + "[!]WMO+6[!]" + id;
    }
  } else if (ispd_id == "003004") {
    ientry_key = platform_type + "[!]CANADA[!]" + id;
  } else if ((ispd_id == "003006" || ispd_id == "003030") && plat_type ==
      1006) {
    ientry_key = platform_type + "[!]AUSTRALIA[!]" + id;
  } else if (ispd_id == "003009" && plat_type == 1006) {
    ientry_key = platform_type + "[!]SPAIN[!]" + id;
  } else if ((ispd_id == "003010" || ispd_id == "003011") && plat_type ==
      1003) {
    auto parts = split(id, "-");
    if (parts.size() == 2 && parts[1].length() == 5 &&
        strutils::is_numeric(parts[1])) {
      ientry_key = platform_type + "[!]WMO[!]" + id;
    }
  } else if (ispd_id == "003012" && plat_type == 1002) {
    ientry_key = platform_type + "[!]SWITZERLAND[!]" + id;
  } else if (ispd_id == "003013" && (plat_type == 1002 || plat_type == 1003)) {
    ientry_key = platform_type + "[!]SOUTHAFRICA[!]" + id;
  } else if (ispd_id == "003014" && plat_type == 1003) {
    ientry_key = platform_type + "[!]NORWAY[!]" + id;
  } else if (ispd_id == "003016" && plat_type == 1002) {
    ientry_key = platform_type + "[!]PORTUGAL[!]" + id;
  } else if ((ispd_id == "003019" || ispd_id == "003100") && plat_type == 1002
      && !id.empty()) {
    ientry_key = platform_type + "[!]NEWZEALAND[!]" + id;
  } else if ((ispd_id == "003007" || ispd_id == "003021" || ispd_id ==
      "003022" || ispd_id == "003023" || ispd_id == "003025" || ispd_id ==
      "003101" || ispd_id == "004005" || ispd_id == "006000") && plat_type ==
      1002 && !id.empty()) {
    ientry_key = platform_type + "[!]NAME[!]" + id;
  } else if (ispd_id == "003026" && plat_type == 1006 && id.length() == 5) {
    ientry_key = platform_type + "[!]WMO[!]" + id;
  } else if (ispd_id == "003030" && plat_type == 2001) {
    if (strutils::is_numeric(id)) {
      ientry_key = platform_type + "[!]AUSTRALIA[!]" + id;
    } else {
      ientry_key = platform_type + "[!]unknown[!]" + id;
    }
  } else if (ispd_id == "008000" || ispd_id == "008001") {
    ientry_key = platform_type + "[!]TropicalCyclone[!]" + id;
  } else if (ispd_id >= "010000" && ispd_id <= "019999") {
    if (id.length() == 5 && strutils::is_numeric(id)) {
      ientry_key = platform_type + "[!]WMO[!]" + id;
    } else {
      ientry_key = platform_type + "[!]unknown[!]" + id;
    }
  } else if (id == "999999999999" || (!id.empty() && (ispd_id == "001013" ||
      ispd_id == "001014" || ispd_id == "001018" || ispd_id == "003005" ||
      ispd_id == "003020" || ispd_id == "005000"))) {
    ientry_key = platform_type + "[!]unknown[!]" + id;
  }
  if (ientry_key.empty()) {
    g_warn_ss << "unknown ID type (3) for station '" << id << "' " << ftos(lat,
        4) << " " << ftos(lon, 4) << " " << ispd_id << " " << itos(plat_type) <<
        " " << itos(isrc) << " '" << string(1, csrc) << "'" << endl;
  }
  return ientry_key;
}

void scan_ispd_hdf5_file(ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto ds = is->dataset("/ISPD_Format_Version");
  if (ds == nullptr || ds->datatype.class_ != 3) {
    log_error2("unable to determine format version", F, g_util_ident);
  }
  HDF5::DataArray da;
  da.fill(*is, *ds);
  auto format_version = da.string_value(0);
  InputHDF5Stream::DataValue ts_val, uon_val, id_val, lat_val, lon_val;

  // load the station library
  ds = is->dataset("/Data/SpatialTemporalLocation/SpatialTemporalLocation");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate spatial/temporal information", F,
        g_util_ident);
  }
  unordered_map<string, std::tuple<string, string, float, float, short,
      short, char, bool>> stn_library;
  InputHDF5Stream::CompoundDatatype cpd;
  HDF5::decode_compound_datatype(ds->datatype, cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m = 0, l = 0; m < ds->data.sizes.front(); m++) {
      ts_val.set(*is->file_stream(), &chunk.buffer[l +
          cpd.members[0].byte_offset], is->size_of_offsets(),
          is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
      string key = reinterpret_cast<char *>(ts_val.get());
      uon_val.set(*is->file_stream(), &chunk.buffer[l +
          cpd.members[1].byte_offset], is->size_of_offsets(),
          is->size_of_lengths(), cpd.members[1].datatype, ds->dataspace);
      key += string(reinterpret_cast<char *>(uon_val.get()));
      if (!key.empty() && (stn_library.find(key) == stn_library.end())) {
        id_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[2].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[2].datatype, ds->dataspace);
        string id = reinterpret_cast<char *>(id_val.get());
        trim(id);
        lat_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[13].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[13].datatype, ds->dataspace);
        auto lat = *(reinterpret_cast<float *>(lat_val.get()));
        lon_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[14].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[14].datatype, ds->dataspace);
        auto lon = *(reinterpret_cast<float *>(lon_val.get()));
        if (lon > 180.) {
          lon -= 360.;
        }
        stn_library.emplace(key, std::make_tuple(id, "", lat, lon, -1, -1, '9',
            false));
      }
      l += ds->data.size_of_element;
    }
  }

  // load the ICOADS platform types
  ds = is->dataset("/SupplementalData/Tracking/ICOADS/TrackingICOADS");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue plat_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*is->file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], is->size_of_offsets(),
              is->size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            id_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[2].datatype, ds->dataspace);
            std::get<5>(entry->second) = *(reinterpret_cast<int *>(id_val.
                get()));
            plat_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[4].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[4].datatype, ds->dataspace);
            std::get<4>(entry->second) = *(reinterpret_cast<int *>(plat_val.
                get()));
          } else {
            log_error2("no entry for '" + key + "' in station library", F,
                g_util_ident);
          }
        }
        l += ds->data.size_of_element;
      }
    }
  }

  // load observation types for IDs that don't already have a platform type
  ds = is->dataset("/Data/Observations/ObservationTypes");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue coll_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*is->file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], is->size_of_offsets(),
              is->size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            if (std::get<4>(entry->second) < 0) {
              id_val.set(*is->file_stream(), &chunk.buffer[l +
                  cpd.members[2].byte_offset], is->size_of_offsets(),
                  is->size_of_lengths(),
                  cpd.members[2].datatype, ds->dataspace);
              std::get<4>(entry->second) = 1000 + *(reinterpret_cast<int *>(
                  id_val.get()));
            }
            coll_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[4].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[4].datatype, ds->dataspace);
            string ispd_id = reinterpret_cast<char *>(coll_val.get());
            strutils::replace_all(ispd_id, " ", "0");
            std::get<1>(entry->second) = ispd_id;
          } else {
            log_error2("no entry for '" + key + "' in station library", F,
                g_util_ident);
          }
        }
        l += ds->data.size_of_element;
      }
    }
  }
  ds = is->dataset("/SupplementalData/Tracking/Land/TrackingLand");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue src_flag_val, rpt_type_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*is->file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], is->size_of_offsets(),
              is->size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            src_flag_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[2].datatype, ds->dataspace);
            std::get<6>(entry->second) = (reinterpret_cast<char *>(src_flag_val.
                get()))[0];
            rpt_type_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[3].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[3].datatype, ds->dataspace);
            string rpt_type = reinterpret_cast<char *>(rpt_type_val.get());
            if (rpt_type == "FM-12") {
              std::get<4>(entry->second)=2001;
            } else if (rpt_type == "FM-13") {
              std::get<4>(entry->second) = 2002;
            } else if (rpt_type == "FM-14") {
              std::get<4>(entry->second) = 2003;
            } else if (rpt_type == "FM-15") {
              std::get<4>(entry->second) = 2004;
            } else if (rpt_type == "FM-16") {
              std::get<4>(entry->second) = 2005;
            } else if (rpt_type == "FM-18") {
              std::get<4>(entry->second) = 2007;
            } else if (rpt_type == "  SAO") {
              std::get<4>(entry->second) = 2010;
            } else if (rpt_type == " AOSP") {
              std::get<4>(entry->second) = 2011;
            } else if (rpt_type == " AERO") {
              std::get<4>(entry->second) = 2012;
            } else if (rpt_type == " AUTO") {
              std::get<4>(entry->second) = 2013;
            } else if (rpt_type == "SY-AE") {
              std::get<4>(entry->second) = 2020;
            } else if (rpt_type == "SY-SA") {
              std::get<4>(entry->second) = 2021;
            } else if (rpt_type == "SY-MT") {
              std::get<4>(entry->second) = 2022;
            } else if (rpt_type == "SY-AU") {
              std::get<4>(entry->second) = 2023;
            } else if (rpt_type == "SA-AU") {
              std::get<4>(entry->second) = 2024;
            } else if (rpt_type == "S-S-A") {
              std::get<4>(entry->second) = 2025;
            } else if (rpt_type == "BOGUS") {
              std::get<4>(entry->second) = 2030;
            } else if (rpt_type == "SMARS") {
              std::get<4>(entry->second) = 2031;
            } else if (rpt_type == "  SOD") {
              std::get<4>(entry->second) = 2040;
            }
          } else {
            log_error2("no entry for '" + key + "' in station library", F,
                g_util_ident);
          }
        }
        l += ds->data.size_of_element;
      }
    }
  }

  // load tropical storm IDs
  ds = is->dataset("/SupplementalData/Misc/TropicalStorms/StormID");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue storm_id_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*is->file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], is->size_of_offsets(),
              is->size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            storm_id_val.set(*is->file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], is->size_of_offsets(),
                is->size_of_lengths(),
                cpd.members[2].datatype, ds->dataspace);
            string id = reinterpret_cast<char *>(storm_id_val.get());
            trim(id);
            std::get<0>(entry->second) = id;
          } else {
            log_error2("no entry for '" + key + "' in station library", F,
                g_util_ident);
          }
        }
        l += ds->data.size_of_element;
      }
    }
  }
  InputHDF5Stream::DataValue slp_val, stnp_val;

  // scan the observations
  ds = is->dataset("/Data/Observations/Observations");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate observations", F, g_util_ident);
  }
  HDF5::decode_compound_datatype(ds->datatype, cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
      ts_val.set(*is->file_stream(), &chunk.buffer[l +
          cpd.members[0].byte_offset], is->size_of_offsets(),
          is->size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
      string timestamp = reinterpret_cast<char *>(ts_val.get());
      trim(timestamp);
      if (!timestamp.empty()) {
        auto key = timestamp;
        uon_val.set(*is->file_stream(), &chunk.buffer[l +
            cpd.members[1].byte_offset], is->size_of_offsets(),
            is->size_of_lengths(), cpd.members[1].datatype, ds->dataspace);
        key += string(reinterpret_cast<char *>(uon_val.get()));
        auto entry = stn_library.find(key);
        if (entry != stn_library.end()) {
          if (regex_search(timestamp, regex("99$"))) {
            strutils::chop(timestamp, 2);

            // patch for some bad timestamps
            if (regex_search(timestamp, regex(" $"))) {
              strutils::chop(timestamp);
              timestamp.insert(8, "0");
            }
            timestamp += "00";
          }
          DateTime dt(std::stoll(timestamp)*100);
          auto platform_type = ispd_hdf5_platform_type(entry->second);
          if (!platform_type.empty()) {
            gatherxml::markup::ObML::IDEntry ientry;
            vector<double> check_values;
            ientry.key = ispd_hdf5_id_entry(entry->second, platform_type, dt);
            if (!ientry.key.empty()) {

              // SLP
              slp_val.set(*is->file_stream(), &chunk.buffer[l +
                  cpd.members[2].byte_offset], is->size_of_offsets(),
                  is->size_of_lengths(),
                  cpd.members[2].datatype, ds->dataspace);
              if (slp_val._class_ != 1) {
                log_error2("observed SLP is not a floating point number for '" +
                    ientry.key + "'", F, g_util_ident);
              }
              if (slp_val.precision_ == 32) {
                check_values.emplace_back(*(reinterpret_cast<float *>(
                    slp_val.get())));
              } else if (slp_val.precision_ == 64) {
                check_values.emplace_back(*(reinterpret_cast<double *>(
                    slp_val.get())));
              } else {
                log_error2("bad precision (" + itos(slp_val.precision_) + ") "
                    "for SLP", F, g_util_ident);
              }

              // STN P
              stnp_val.set(*is->file_stream(), &chunk.buffer[l +
                  cpd.members[5].byte_offset], is->size_of_offsets(),
                  is->size_of_lengths(),
                  cpd.members[5].datatype, ds->dataspace);
              if (stnp_val._class_ != 1) {
                log_error2("observed STN P is not a floating point number for "
                   "'" + ientry.key + "'", F, g_util_ident);
              }
              if (stnp_val.precision_ == 32) {
                check_values.emplace_back(*(reinterpret_cast<float *>(
                    stnp_val.get())));
              } else if (stnp_val.precision_ == 64) {
                check_values.emplace_back(*(reinterpret_cast<double *>(
                    stnp_val.get())));
              } else {
                log_error2("bad precision (" + itos(stnp_val.precision_) + ") "
                    "for SLP", F, g_util_ident);
              }
              if ((check_values[0] >= 860. && check_values[0] <= 1090.) ||
                  (check_values[1] >= 400. && check_values[1] <= 1090.)) {
                if (check_values[0] < 9999.9) {
                  if (!obs_data.added_to_ids("surface", ientry, "SLP", "",
                      std::get<2>(entry->second), std::get<3>(entry->second),
                      std::stoll(timestamp), &dt)) {
                    auto error = move(myerror);
                    log_error2(error + " when adding ID " + ientry.key, F,
                        g_util_ident);
                  }
                  std::get<7>(entry->second) = true;
                  ++scan_data.num_not_missing;
                }
                if (check_values[1] < 9999.9) {
                  if (!obs_data.added_to_ids("surface", ientry, "STNP", "",
                      std::get<2>(entry->second), std::get<3>(entry->second),
                      std::stoll(timestamp), &dt)) {
                    auto error = move(myerror);
                    log_error2(error + " when adding ID " + ientry.key, F,
                        g_util_ident);
                  }
                  std::get<7>(entry->second) = true;
                  ++scan_data.num_not_missing;
                }
                if (!obs_data.added_to_platforms("surface", platform_type,
                    std::get<2>(entry->second), std::get<3>(entry->second))) {
                  auto error = move(myerror);
                  log_error2(error + " when adding platform " + platform_type,
                      F, g_util_ident);
                }
              }
            }
          }
        } else {
          log_error2("no entry for '" + key + "' in station library", F,
              g_util_ident);
        }
      }
      l += ds->data.size_of_element;
    }
  }

  // scan for feedback information
  unordered_map<string, vector<size_t>> feedback_versions{
    {"10.11", {2, 11, 14}},
    {"11.0", {2, 14, 16}}};
  if (feedback_versions.find(format_version) == feedback_versions.end()) {
    log_error2("unknown format version '" + format_version + "'", F,
        g_util_ident);
  }
  ds = is->dataset("/Data/AssimilationFeedback/AssimilationFeedback");
  if (ds == nullptr) {
    ds = is->dataset("/Data/AssimilationFeedback/AssimilationFeedBack");
  }
  if (ds != nullptr && ds->datatype.class_ == 6) {
    auto ts_regex = regex("99$");
//    InputHDF5Stream::DataValue p_val, ens_fg_val, ens_p_val;
    vector<InputHDF5Stream::DataValue> dv(feedback_versions[format_version].
        size());
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*is->file_stream(),
            &chunk.buffer[l + cpd.members[0].byte_offset],
            is->size_of_offsets(), is->size_of_lengths(),
            cpd.members[0].datatype, ds->dataspace);
        string timestamp = reinterpret_cast<char *>(ts_val.get());
        trim(timestamp);
        if (!timestamp.empty()) {
          auto key = timestamp;
          uon_val.set(*is->file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], is->size_of_offsets(),
              is->size_of_offsets(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            if (!timestamp.empty()) {
              if (regex_search(timestamp, ts_regex)) {
                strutils::chop(timestamp, 2);
                timestamp += "00";
              }
              DateTime dt(std::stoll(timestamp)*100);
              auto platform_type = ispd_hdf5_platform_type(entry->second);
              if (!platform_type.empty()) {
                gatherxml::markup::ObML::IDEntry ientry;
                ientry.key = ispd_hdf5_id_entry(entry->second, platform_type,
                    dt);
                if (!ientry.key.empty()) {
                  for (size_t ndv=0; ndv < dv.size(); ++ndv) {
                    auto feedback_idx = feedback_versions[format_version][ndv];
                    dv[ndv].set(*is->file_stream(), &chunk.buffer[l +
                        cpd.members[feedback_idx].byte_offset],
                        is->size_of_offsets(), is->size_of_lengths(),
                        cpd.members[feedback_idx].datatype, ds->dataspace);
                    if (dv[ndv]._class_ != 1) {
                      log_error2("feedback field value " + itos(feedback_idx) +
                          " is not a floating point number for '" + ientry.key +
                          "'", F, g_util_ident);
                    }
                    double check_value = 0.;
                    switch (dv[ndv].precision_) {
                      case 32: {
                        check_value = *(reinterpret_cast<float *>(
                            dv[ndv].get()));
                        break;
                      }
                      case 64: {
                        check_value = *(reinterpret_cast<double *>(
                            dv[ndv].get()));
                        break;
                      }
                      default: {
                        log_error2("bad precision (" + itos(
                            dv[ndv].precision_) + ") for feedback field "
                            "value " + itos(feedback_idx), F, g_util_ident);
                      }
                    }
                    if (check_value >= 400. && check_value <= 1090.) {
                      if (!obs_data.added_to_ids("surface", ientry, "Feedback",
                          "", std::get<2>(entry->second),
                          std::get<3>(entry->second), std::stoll(timestamp),
                          &dt)) {
                        auto error = move(myerror);
                        log_error2(error + " when adding ID " + ientry.key, F,
                            g_util_ident);
                      }
                      ++scan_data.num_not_missing;
                      std::get<7>(entry->second)=true;
                      if (!obs_data.added_to_platforms("surface", platform_type,
                          std::get<2>(entry->second),
                          std::get<3>(entry->second))) {
                        auto error = move(myerror);
                        log_error2(error + " when adding platform " +
                            platform_type, F, g_util_ident);
                      }
                      break;
                    }
                  }
                }
              }
            }
          }
        }
        l += ds->data.size_of_element;
      }
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_usarray_transportable_hdf5_file(
    ScanData& scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  obs_data.set_track_unique_observations(false);

  // load the pressure dataset
  auto ds = is->dataset("/obsdata/presdata");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate the pressure dataset", F, g_util_ident);
  }
  HDF5::DataArray times, stnids, pres;
  times.fill(*is, *ds, 0);
  if (times.type != HDF5::DataArray::Type::LONG_LONG) {
     log_error2("expected the timestamps to be 'long long' but got " +
         itos(static_cast<int>(times.type)), F, g_util_ident);
  }
  stnids.fill(*is, *ds, 1);
  if (stnids.type != HDF5::DataArray::Type::SHORT) {
     log_error2("expected the numeric station IDs to be 'short' but got " +
         itos(static_cast<int>(stnids.type)), F, g_util_ident);
  }
  pres.fill(*is, *ds, 2);
  if (pres.type != HDF5::DataArray::Type::FLOAT) {
     log_error2("expected the pressures to be 'float' but got " + itos(
         static_cast<int>(pres.type)), F, g_util_ident);
  }
  int num_values = 0;
  float pres_miss_val = 3.e48;
  short numeric_id = -1;
  gatherxml::markup::ObML::IDEntry ientry;
  string platform_type, datatype, title;
  float lat = -1.e38, lon = -1.e38;
  for (const auto& a : ds->attributes) {
    if (a.first == "NROWS") {
      num_values = *(reinterpret_cast<int *>(a.second.array));
    } else if (a.first == "LATITUDE_DDEG") {
      lat = *(reinterpret_cast<float *>(a.second.array));
    } else if (a.first == "LONGITUDE_DDEG") {
      lon = *(reinterpret_cast<float *>(a.second.array));
    } else if (a.first == "CHAR_STATION_ID") {
      if (platform_type.empty()) {
        platform_type = "land_station";
        ientry.key.assign(reinterpret_cast<char *>(a.second.array));
        ientry.key.insert(0, platform_type + "[!]USArray[!]TA.");
      } else {
        log_error2("multiple station IDs not expected", F, g_util_ident);
      }
    } else if (a.first == "NUMERIC_STATION_ID") {
      numeric_id = *(reinterpret_cast<short *>(a.second.array));
    } else if (a.first == "FIELD_2_NAME") {
      datatype.assign(reinterpret_cast<char *>(a.second.array));
    } else if (a.first == "FIELD_2_FILL") {
      pres_miss_val = *(reinterpret_cast<float *>(a.second.array));
    } else if (a.first == "FIELD_2_DESCRIPTION") {
      title.assign(reinterpret_cast<char *>(a.second.array));
    }
  }
  if (platform_type.empty()) {
    log_error2("unable to get the station ID", F, g_util_ident);
  }
  if (lat == -1.e38) {
    log_error2("unable to get the station latitude", F, g_util_ident);
  }
  if (lon == -1.e38) {
    log_error2("unable to get the station longitude", F, g_util_ident);
  }
  if (title.empty()) {
    log_error2("unable to get title for the data value", F, g_util_ident);
  }
  if (datatype.empty()) {
    log_error2("unable to get the name of the data value", F, g_util_ident);
  }
  if (!obs_data.added_to_platforms("surface", platform_type, lat, lon)) {
    auto error = move(myerror);
    log_error2(error + " when adding platform " + platform_type, F,
        g_util_ident);
  }
  DateTime epoch(1970, 1, 1, 0, 0);
  for (auto n = 0; n < num_values; ++n) {
    if (stnids.short_value(n) != numeric_id) {
      log_error2("unexpected change in the numeric station ID", F,
          g_util_ident);
    }
    if (pres.float_value(n) != pres_miss_val) {
      DateTime dt = epoch.seconds_added(times.long_long_value(n));
      if (!obs_data.added_to_ids("surface", ientry, datatype, "", lat, lon,
          times.long_long_value(n), &dt)) {
        auto error = move(myerror);
        log_error2(error + " when adding ID " + ientry.key, F, g_util_ident);
      }
      ++scan_data.num_not_missing;
    }
  }
  gatherxml::markup::ObML::DataTypeEntry dte;
  ientry.data->data_types_table.found(datatype, dte);
  ientry.data->nsteps = dte.data->nsteps = scan_data.num_not_missing;
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/HDF5.ds" + g_dsid + ".xml", scan_data.tdir->
      name());
  scan_data.found_map = !scan_data.map_name.empty();
  auto key = datatype + "<!>" + title + "<!>Hz";
  scan_data.varlist.emplace_back(key);
  if (scan_data.found_map && scan_data.var_changes_table.find(datatype) ==
      scan_data.var_changes_table.end()) {
    scan_data.var_changes_table.emplace(datatype);
  }
  scan_data.write_type = ScanData::ObML_type;
}

string gridded_time_method(const DatasetPointer ds, string valid_time_id) {
  static const string F = this_function_label(__func__);
  auto attr_it = ds->attributes.find("cell_methods");
  if (attr_it != ds->attributes.end() && attr_it->second._class_ == 3) {
    auto time_method = metautils::NcTime::time_method_from_cell_methods(
        reinterpret_cast<char *>(attr_it->second.array), valid_time_id);
    if (time_method[0] == '!') {
      log_error2("cell method '" + time_method.substr(1) + "' is not valid CF",
          F, g_util_ident);
    } else {
      return time_method;
    }
  }
  return "";
}

void update_grid_entry_set(string key_start, string time_method, const
    GridData& grid_data, unordered_set<string>& grid_entry_set) {
  static const string F = this_function_label(__func__);
  for (const auto& e : grid_data.time_range_entries) {
    string err;
    auto d = metautils::NcTime::gridded_netcdf_time_range_description2(e.second,
        *grid_data.time_data, capitalize(time_method), err);
    if (!err.empty()) {
      log_error2(err, F, g_util_ident);
    }
    auto k = key_start + d;
    if (grid_entry_set.find(k) == grid_entry_set.end()) {
      grid_entry_set.emplace(k);
    }
    if (g_inv.stream.is_open() && g_inv.U.map.find(d) == g_inv.U.map.end()) {
      g_inv.U.map.emplace(d, make_pair(g_inv.U.map.size(), 0));
      g_inv.U.lst.emplace_back(d);
    }
  }
}

void add_gridded_time_range(string key_start, unordered_set<string>&
    grid_entry_set, const GridData& grid_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto b = false;
  auto vars = is->datasets_with_attribute("DIMENSION_LIST");
  for (const auto& dse : vars) {
    auto attr_it = dse.p_ds->attributes.find("DIMENSION_LIST");
    if (attr_it != dse.p_ds->attributes.end() && grid_data.
        coordinate_variables_set.find(dse.key) == grid_data.
        coordinate_variables_set.end() && attr_it->second.dim_sizes.size() == 1
        && (attr_it->second.dim_sizes[0] > 2 || (attr_it->second.dim_sizes[0] ==
        2 && grid_data.valid_time.data_array.num_values == 1))) {
      auto tm = gridded_time_method(dse.p_ds, grid_data.valid_time.id);
      if (tm.empty()) {
        b = true;
      } else {
        update_grid_entry_set(key_start, tm, grid_data, grid_entry_set);
      }
    }
  }
  if (b) {
    update_grid_entry_set(key_start, "", grid_data, grid_entry_set);
  }
}

void add_gridded_lat_lon_keys(unordered_set<string>& grid_entry_set, Grid::
    GridDimensions dim, Grid::GridDefinition def, const GridData& grid_data) {
  string key_start;
  switch (def.type) {
    case Grid::Type::latitudeLongitude: {
      key_start = itos(static_cast<int>(def.type));
      if (def.is_cell) {
        key_start.push_back('C');
      }
      key_start += "<!>" + itos(dim.x) + "<!>" + itos(dim.y) + "<!>" +
          ftos(def.slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" +
          ftos(def.elatitude, 3) + "<!>" + ftos(def.elongitude, 3) + "<!>" +
          ftos(def.loincrement, 3) + "<!>" + ftos(def.laincrement, 3) + "<!>";
      add_gridded_time_range(key_start, grid_entry_set, grid_data);
      break;
    }
    case Grid::Type::polarStereographic: {
      key_start = itos(static_cast<int>(def.type)) + "<!>" + itos(dim.x) +
          "<!>" + itos(dim.y) + "<!>" + ftos(def.slatitude, 3) + "<!>" + ftos(
          def.slongitude, 3) + "<!>" + ftos(def.llatitude, 3) + "<!>" + ftos(
          def.olongitude, 3) + "<!>" + ftos(def.dx, 3) + "<!>" + ftos(def.dy,
          3) + "<!>";
      if (def.projection_flag == 0) {
        key_start += "N";
      } else {
        key_start += "S";
      }
      key_start += "<!>";
      add_gridded_time_range(key_start, grid_entry_set, grid_data);
      break;
    }
    case Grid::Type::lambertConformal: {
      key_start = itos(static_cast<int>(def.type)) + "<!>" + itos(dim.x) +
          "<!>" + itos(dim.y) + "<!>" + ftos(def.slatitude, 3) + "<!>" + ftos(
          def.slongitude, 3) + "<!>" + ftos(def.llatitude, 3) + "<!>" + ftos(
          def.olongitude, 3) + "<!>" + ftos(def.dx, 3) + "<!>" + ftos(def.dy,
          3) + "<!>";
      if (def.projection_flag == 0) {
        key_start += "N";
      } else {
        key_start += "S";
      }
      key_start += "<!>" + ftos(def.stdparallel1, 3) + "<!>" + ftos(
          def.stdparallel2, 3) + "<!>";
      add_gridded_time_range(key_start, grid_entry_set, grid_data);
      break;
    }
    default: { }
  }
  auto key = strutils::substitute(key_start, "<!>", ",");
  strutils::chop(key);
  if (g_inv.stream.is_open() && g_inv.G.map.find(key) == g_inv.G.map.end()) {
    g_inv.G.map.emplace(key, make_pair(g_inv.G.map.size(), 0));
    g_inv.G.lst.emplace_back(key);
  }
}

double data_array_value(const HDF5::DataArray& data_array, size_t index,
    const InputHDF5Stream::Dataset *ds) {
  static const string F = this_function_label(__func__);
  double value = 0.;
  switch (ds->datatype.class_) {
    case 0: {
      switch (ds->data.size_of_element) {
        case 4: {
          value = (reinterpret_cast<int *>(data_array.values))[index];
          break;
        }
        case 8: {
          value = (reinterpret_cast<long long *>(data_array.values))[index];
          break;
        }
        default: {
          log_error2("unable to get value for fixed-point size " + itos(
              ds->data.size_of_element), F, g_util_ident);
        }
      }
      break;
    }
    case 1: {
      switch (ds->data.size_of_element) {
        case 4: {
          value = (reinterpret_cast<float *>(data_array.values))[index];
          break;
        }
        case 8: {
          value = (reinterpret_cast<double *>(data_array.values))[index];
          break;
        }
        default: {
          log_error2("unable to get value for floating-point size " + itos(
              ds->data.size_of_element), F, g_util_ident);
        }
      }
      break;
    }
    default: {
      log_error2("unable to decode time from datatype class " + itos(
          ds->datatype.class_), F, g_util_ident);
    }
  }
  return value;
}

void add_gridded_netcdf_parameter(const InputHDF5Stream::DatasetEntry& dse,
    ScanData& scan_data, const TimeRange& time_range, ParameterData&
    parameter_data, int num_steps) {
  string description;
  string units;
  auto& attributes = dse.p_ds->attributes;
  auto attr_it = attributes.find("units");
  if (attr_it != attributes.end() && attr_it->second._class_ == 3) {
    units = reinterpret_cast<char *>(attr_it->second.array);
  }
  if (description.empty()) {
    attr_it = attributes.find("description");
    if (attr_it == attributes.end()) {
      attr_it = attributes.find("Description");
    }
    if (attr_it != attributes.end() && attr_it->second._class_ == 3) {
      description = reinterpret_cast<char *>(attr_it->second.array);
    }
    if (description.empty()) {
      attr_it = attributes.find("comment");
      if (attr_it == attributes.end()) {
        attr_it = attributes.find("Comment");
      }
      if (attr_it != attributes.end() && attr_it->second._class_ == 3) {
        description = reinterpret_cast<char *>(attr_it->second.array);
      }
    }
    if (description.empty()) {
      attr_it = attributes.find("long_name");
      if (attr_it != attributes.end() && attr_it->second._class_ == 3) {
        description = reinterpret_cast<char *>(attr_it->second.array);
      }
    }
  }
  string standard_name;
  attr_it = attributes.find("standard_name");
  if (attr_it != attributes.end() && attr_it->second._class_ == 3) {
    standard_name = reinterpret_cast<char *>(attr_it->second.array);
  }
  auto var_name = dse.key;
  trim(var_name);
  trim(description);
  trim(units);
  trim(standard_name);
  auto key = var_name + "<!>" + description + "<!>" + units + "<!>" +
      standard_name;
  if (parameter_data.set.find(key) == parameter_data.set.end()) {
    auto short_name = parameter_data.map.short_name(dse.key);
    if (!scan_data.found_map || short_name.empty()) {
      parameter_data.set.emplace(key);
      scan_data.varlist.emplace_back(key);
    } else {
      parameter_data.set.emplace(key);
      scan_data.varlist.emplace_back(key);
      if (scan_data.var_changes_table.find(dse.key) == scan_data.
          var_changes_table.end()) {
        scan_data.var_changes_table.emplace(dse.key);
      }
    }
  }
  g_grml_data->p.entry.start_date_time = time_range.first_valid_datetime;
  g_grml_data->p.entry.end_date_time = time_range.last_valid_datetime;
  g_grml_data->p.entry.num_time_steps = num_steps;
  g_grml_data->l.entry.parameter_code_table.emplace(g_grml_data->p.key,
      g_grml_data->p.entry);
}

bool parameter_matches_dimensions(const InputHDF5Stream::DataValue&
    dimension_list, const GridData& grid_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  bool parameter_matches = false;
  auto off = 4;
  size_t first = 1;
  if (dimension_list.dim_sizes[0] == 2 && grid_data.valid_time.data_array.
      num_values == 1) {
    first = 0;
  } else {
    off += dimension_list.precision_ + 4;
  }
  if (gatherxml::verbose_operation) {
    cout << "      dimension names to check:" << endl;
  }
  unordered_map<size_t, string>::iterator rtp_it[3];
  for (size_t n = first, rcnt = 0; n < dimension_list.dim_sizes[0]; ++n,
      ++rcnt) {
    rtp_it[rcnt] = is->reference_table_pointer()->find(HDF5::value(
        &dimension_list.vlen.buffer[off], dimension_list.precision_));
    if (rtp_it[rcnt] == is->reference_table_pointer()->end()) {
      log_error2("unable to dereference dimension reference", F, g_util_ident);
    }
    if (gatherxml::verbose_operation) {
      cout << "       '" << rtp_it[rcnt]->second << "'" << endl;
    }
    off += dimension_list.precision_ + 4;
  }
  switch (dimension_list.dim_sizes[0]) {
    case 2:
    case 3: {

      // data variables dimensioned [time, lat, lon] / [time, lon, lat] or
      //     [lat, lon] / [lon, lat] with scalar time
      if (grid_data.level.id == "sfc" && (dimension_list.dim_sizes[0] == 3 ||
          grid_data.valid_time.data_array.num_values == 1)) {
        if ((rtp_it[0]->second == grid_data.lat.id && rtp_it[1]->second ==
            grid_data.lon.id) || (rtp_it[0]->second == grid_data.lon.id &&
            rtp_it[1]->second == grid_data.lat.id)) {

          // latitude and longitude are coordinate variables
          parameter_matches = true;
        } else if (rtp_it[0]->second != grid_data.lat.id && rtp_it[1]->
            second != grid_data.lon.id) {

          // check for auxiliary coordinate variables for latitude and longitude
          auto lat_ds = is->dataset("/" + grid_data.lat.id);
          auto lon_ds = is->dataset("/" + grid_data.lon.id);
          if (lat_ds != nullptr && lon_ds != nullptr) {
            stringstream lat_dims;
            lat_ds->attributes["DIMENSION_LIST"].print(lat_dims, is->
                reference_table_pointer());
            stringstream lon_dims;
            lon_ds->attributes["DIMENSION_LIST"].print(lon_dims, is->
                reference_table_pointer());
            if ((lat_dims.str() == "[" + rtp_it[0]->second + "]" && lon_dims.
                str() == "[" + rtp_it[1]->second + "]") || (lat_dims.str() ==
                lon_dims.str() && lon_dims.str() == "[" + rtp_it[0]->second +
                ", " + rtp_it[1]->second + "]")) {
              parameter_matches = true;
            }
          }
        }
      }
      break;
    }
    case 4:
    case 5: {

      // data variables dimensioned [time, lev, lat, lon] /
      //     [time, lev, lon, lat] or
      //     [reference_time, forecast_period, lev, lat, lon] /
      //     [reference_time, forecast_period, lev, lon, lat]
      auto off = dimension_list.dim_sizes[0] - 4;
      auto can_continue = true;
      if (rtp_it[off]->second != grid_data.level.id) {
        can_continue = false;
        if (grid_data.level.ds != nullptr) {
          stringstream lev_dims;
          grid_data.level.ds->attributes["DIMENSION_LIST"].print(lev_dims,
              is->reference_table_pointer());
          if (lev_dims.str() == "[" + rtp_it[off]->second + "]") {
            can_continue = true;
          }
        } else if (grid_data.level.id == "sfc" && rtp_it[off]->second ==
            grid_data.forecast_period.id) {
          can_continue = true;
        }
      }
      if (can_continue) {
        if ((rtp_it[off+1]->second == grid_data.lat.id && rtp_it[off+2]->second
            == grid_data.lon.id) || (rtp_it[off+1]->second == grid_data.lon.id
            && rtp_it[off+2]->second == grid_data.lat.id)) {

          // latitude and longitude are coordinate variables
          parameter_matches = true;
        } else {

          // check for auxiliary coordinate variables for latitude and longitude
          auto lat_ds = is->dataset("/" + grid_data.lat.id);
          auto lon_ds = is->dataset("/" + grid_data.lon.id);
          if (lat_ds != nullptr && lon_ds != nullptr) {
            stringstream lat_dims;
            lat_ds->attributes["DIMENSION_LIST"].print(lat_dims, is->
                reference_table_pointer());
            stringstream lon_dims;
            lon_ds->attributes["DIMENSION_LIST"].print(lon_dims,
                is->reference_table_pointer());
            if ((lat_dims.str() == "[" + rtp_it[off + 1]->second + "]" &&
                lon_dims.str() == "[" + rtp_it[off + 2]->second + "]") ||
                (lat_dims.str() == lon_dims.str() && lon_dims.str() == "[" +
                rtp_it[off + 1]->second + ", " + rtp_it[off + 2]->second +
                "]")) {
              parameter_matches = true;
            }
          }
        }
      }
      if (off == 1 && parameter_matches) {
        if (rtp_it[0]->second != grid_data.forecast_period.id) {
          parameter_matches = false;
        }
      }
      break;
    }
  }
  return parameter_matches;
}

bool added_gridded_parameters_to_netcdf_level_entry(string& grid_entry_key,
    const GridData& grid_data, ScanData& scan_data, ParameterData&
    parameter_data) {
  static const string F = this_function_label(__func__);
  bool b = false; // return value
  auto is = sget_hdf5();

  // find all of the variables
  auto vars = is->datasets_with_attribute("DIMENSION_LIST");
  for (const auto& dse : vars) {
    if (grid_data.coordinate_variables_set.find(dse.key) == grid_data.
        coordinate_variables_set.end()) {
      auto attr_it = dse.p_ds->attributes.find("DIMENSION_LIST");
      if (gatherxml::verbose_operation) {
        cout << "    '" << dse.key << "' has a DIMENSION_LIST: ";
        attr_it->second.print(cout, is->reference_table_pointer());
        cout << endl;
      }
      if (attr_it->second._class_ == 9 && attr_it->second.dim_sizes.size() == 1
          && (attr_it->second.dim_sizes[0] > 2 || (attr_it->second.dim_sizes[0]
          == 2 && grid_data.valid_time.data_array.num_values == 1)) && attr_it->
          second.vlen.class_ == 7 && parameter_matches_dimensions(attr_it->
          second, grid_data)) {
        if (gatherxml::verbose_operation) {
          cout << "*** is a netCDF variable ***" << endl;
        }
        auto time_method = gridded_time_method(dse.p_ds, grid_data.valid_time.
            id);
        time_method = strutils::capitalize(time_method);
        TimeRange time_range;
        for (const auto& e : grid_data.time_range_entries) {
          if (time_method.empty() || (myequalf(e.second.time_bounds.t1, 0,
              0.0001) && myequalf(e.second.time_bounds.t1, e.second.time_bounds.
              t2, 0.0001))) {
            time_range.first_valid_datetime = e.second.instantaneous.
                first_valid_datetime;
            time_range.last_valid_datetime = e.second.instantaneous.
                last_valid_datetime;
          } else {
            time_range.first_valid_datetime = e.second.bounded.
                first_valid_datetime;
            time_range.last_valid_datetime = e.second.bounded.
                last_valid_datetime;
          }
          string err;
          auto d = metautils::NcTime::gridded_netcdf_time_range_description2(
              e.second, *grid_data.time_data, time_method, err);
          if (!err.empty()) {
            log_error2(err + "; var name '" + dse.key + "'", F, g_util_ident);
          }
          d = capitalize(d);
          if (regex_search(grid_entry_key, regex(d + "$"))) {
            g_grml_data->p.key = "ds" + g_dsid + ":" + dse.key;
            add_gridded_netcdf_parameter(dse, scan_data, time_range,
                parameter_data, e.second.num_steps);
            if (g_inv.P.map.find(g_grml_data->p.key) == g_inv.P.map.end()) {
              g_inv.P.map.emplace(g_grml_data->p.key, make_pair(g_inv.P.map.
                  size(), 0));
              g_inv.P.lst.emplace_back(g_grml_data->p.key);
            }
            b = true;
          }
        }
      }
    }
  }
  return b;
}

void update_level_entry(const GridData& grid_data, ScanData& scan_data,
    ParameterData& parameter_data, bool& level_write) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();

  // find all of the variables
  auto vars = is->datasets_with_attribute("DIMENSION_LIST");
  for (const auto& dse : vars) {
    if (grid_data.coordinate_variables_set.find(dse.key) == grid_data.
        coordinate_variables_set.end()) {
      auto attr_it = dse.p_ds->attributes.find("DIMENSION_LIST");
      if (gatherxml::verbose_operation) {
        cout << "    '" << dse.key << "' has a DIMENSION_LIST: ";
        attr_it->second.print(cout, is->reference_table_pointer());
        cout << endl;
      }
      if (attr_it->second._class_ == 9 && attr_it->second.dim_sizes.size() == 1
          && attr_it->second.dim_sizes[0] > 2 && attr_it->second.vlen.class_ ==
          7 && parameter_matches_dimensions(attr_it->second, grid_data)) {
        if (gatherxml::verbose_operation) {
          cout << "*** is a netCDF variable ***" << endl;
        }
        g_grml_data->p.key = "ds" + g_dsid + ":" + dse.key;
        auto time_method = gridded_time_method(dse.p_ds, grid_data.valid_time.
          id);
        time_method = strutils::capitalize(time_method);
        for (const auto& e : grid_data.time_range_entries) {
          if (g_grml_data->l.entry.parameter_code_table.find(g_grml_data->p.key)
              == g_grml_data->l.entry.parameter_code_table.end()) {
            TimeRange time_range;
            if (time_method.empty() || (myequalf(e.second.time_bounds.t1, 0,
                0.0001) && myequalf(e.second.time_bounds.t1, e.second.
                time_bounds.t2, 0.0001))) {
              time_range.first_valid_datetime = e.second.instantaneous.
                  first_valid_datetime;
              time_range.last_valid_datetime = e.second.instantaneous.
                  last_valid_datetime;
            } else {
              time_range.first_valid_datetime = e.second.bounded.
                  first_valid_datetime;
              time_range.last_valid_datetime = e.second.bounded.
                  last_valid_datetime;
            }
            add_gridded_netcdf_parameter(dse, scan_data, time_range,
                parameter_data, e.second.num_steps);
            g_grml_data->g.entry.level_table[g_grml_data->l.key] = g_grml_data->
                l.entry;
          } else {
            string err;
            auto d = metautils::NcTime::gridded_netcdf_time_range_description2(
                e.second, *grid_data.time_data, time_method, err);
            if (!err.empty()) {
              log_error2(err, F, g_util_ident);
            }
            d = capitalize(d);
            if (regex_search(g_grml_data->g.key, regex(d + "$"))) {
              auto& pe = g_grml_data->l.entry.parameter_code_table[g_grml_data->
                  p.key];
              TimeRange time_range;
              if (time_method.empty() || (myequalf(e.second.time_bounds.t1, 0,
                  0.0001) && myequalf(e.second.time_bounds.t1, e.second.
                  time_bounds.t2, 0.0001))) {
                if (e.second.instantaneous.first_valid_datetime < pe.
                    start_date_time) {
                  pe.start_date_time = e.second.instantaneous.
                      first_valid_datetime;
                }
                if (e.second.instantaneous.last_valid_datetime > pe.
                    end_date_time) {
                  pe.end_date_time = e.second.instantaneous.
                      last_valid_datetime;
                }
              } else {
                if (e.second.bounded.first_valid_datetime < pe.
                    start_date_time) {
                  pe.start_date_time = e.second.bounded.first_valid_datetime;
                }
                if (e.second.bounded.last_valid_datetime > pe.end_date_time) {
                  pe.end_date_time = e.second.bounded.last_valid_datetime;
                }
              }
              pe.num_time_steps += e.second.num_steps;
              g_grml_data->g.entry.level_table[g_grml_data->l.key] =
                  g_grml_data->l.entry;
            }
          }
        }
        level_write = true;
        if (g_inv.P.map.find(g_grml_data->p.key) == g_inv.P.map.end()) {
          g_inv.P.map.emplace(g_grml_data->p.key, make_pair(g_inv.P.map.
              size(), 0));
          g_inv.P.lst.emplace_back(g_grml_data->p.key);
        }
      }
    }
  }
}

void add_new_time_range_entry(const TimeBounds2& time_bounds, const TimeData&
    time_data, GridData& grid_data) {
  static const string F = this_function_label(__func__);
  grid_data.time_range_entries.emplace(time_bounds.diff, TimeRangeEntry2());
  auto &e = grid_data.time_range_entries[time_bounds.diff];
  e.key = -1;
  e.time_bounds.t1 = time_bounds.t1;
  e.time_bounds.diff = time_bounds.diff;
  string err;
  e.instantaneous.first_valid_datetime = actual_date_time(e.time_bounds.t1,
      time_data, err);
  if (!err.empty()) {
    log_error2(err, F, g_util_ident);
  }
  e.bounded.first_valid_datetime = e.instantaneous.first_valid_datetime;
  e.num_steps = 1;
}

void add_time_range_entries(const TimeData& time_data, const HDF5::DataArray&
    data_array, GridData& grid_data) {
  static const string F = this_function_label(__func__);
  TimeBounds2 tb;
  tb.t1 = data_array.value(0);
  tb.diff = data_array.value(1) - tb.t1;
  add_new_time_range_entry(tb, time_data, grid_data);
  auto nsteps = 0;
  for (size_t n = 2; n < data_array.num_values; n += 2) {
    ++nsteps;
    auto curr_diff = data_array.value(n+1) - data_array.value(n);
    if (!myequalf(curr_diff, tb.diff)) {
      auto new_time_range = true;
      if (time_data.units == "days") {
        string err;
        auto a = actual_date_time(tb.t1, time_data, err);
        if (dateutils::days_in_month(a.year(), a.month(), time_data.calendar) ==
            tb.diff) {
          new_time_range = false;
       }
      }
      if (new_time_range) {
        auto& e = grid_data.time_range_entries[tb.diff];
        e.time_bounds.t2 = data_array.value(n-1); 
        string err;
        e.instantaneous.last_valid_datetime = actual_date_time(e.time_bounds.t2,
            time_data, err);
        if (!err.empty()) {
          log_error2(err, F, g_util_ident);
        }
        e.bounded.last_valid_datetime = actual_date_time(e.time_bounds.t2,
            time_data, err);
        if (!err.empty()) {
          log_error2(err, F, g_util_ident);
        }
        e.num_steps += nsteps;
        tb.t1 = data_array.value(n);
        tb.diff = curr_diff;
        if (grid_data.time_range_entries.find(tb.diff) == grid_data.
            time_range_entries.end()) {
          add_new_time_range_entry(tb, time_data, grid_data);
        }
        nsteps = 0;
      }
    }
  }
  auto& e = grid_data.time_range_entries[tb.diff];
  e.time_bounds.t2 = data_array.value(data_array.num_values-1);
  string err;
  e.instantaneous.last_valid_datetime = actual_date_time(e.time_bounds.t2,
      time_data, err);
  if (!err.empty()) {
    log_error2(err, F, g_util_ident);
  }
  e.bounded.last_valid_datetime = actual_date_time(e.time_bounds.t2, time_data,
      err);
  if (!err.empty()) {
    log_error2(err, F, g_util_ident);
  }
  e.num_steps += nsteps;
}

DateTime compute_nc_time(const HDF5::DataArray& times, const TimeData&
    time_data, size_t index) {
  static const string F = this_function_label(__func__);
  auto val = times.value(index);
  DateTime dt;
  if (time_data.units == "seconds") {
    dt = time_data.reference.seconds_added(val);
  } else if (time_data.units == "hours") {
    if (myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.hours_added(val);
    } else {
      dt = time_data.reference.seconds_added(lround(val*3600.));
    }
  } else if (time_data.units == "days") {
    if (myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.days_added(val);
    } else {
      dt = time_data.reference.seconds_added(lround(val*86400.));
    }
  } else {
    log_error2("compute_nc_time() unable to set date/time for units '" +
        time_data.units + "'", F, g_util_ident);
  }
  return dt;
}

void update_inventory(string pkey, string gkey, const GridData& grid_data) {
  if (g_inv.L.map.find(g_grml_data->l.key) == g_inv.L.map.end()) {
    g_inv.L.map.emplace(g_grml_data->l.key, make_pair(g_inv.L.map.size(), 0));
    g_inv.L.lst.emplace_back(g_grml_data->l.key);
  }
  for (size_t n = 0; n < grid_data.valid_time.data_array.num_values; ++n) {
    for (const auto& e : g_grml_data->l.entry.parameter_code_table) {
      stringstream inv_line;
      string error;
      inv_line << "0|1|" << actual_date_time(data_array_value(grid_data.
          valid_time.data_array, n, grid_data.valid_time.ds.get()), *grid_data.
          time_data, error).to_string("%Y%m%d%H%MM") << "|" << g_inv.U.map[
          pkey].first << "|" << g_inv.G.map[gkey].first << "|" << g_inv.L.map[
          g_grml_data->l.key].first << "|" << g_inv.P.map[e.first].first <<
          "|0";
      g_inv.lines.emplace_back(inv_line.str());
      ++g_inv.U.map[pkey].second;
      ++g_inv.G.map[gkey].second;
      ++g_inv.L.map[g_grml_data->l.key].second;
      ++g_inv.P.map[e.first].second;
++g_inv.R.map["x"].second;
    }
  }
}

void process_units_attribute(const InputHDF5Stream::DatasetEntry& ds_entry,
    DiscreteGeometriesData& dgd, TimeData& time_data) {
  static const string F = this_function_label(__func__);
  auto& var_name = ds_entry.key;
  auto attr_val = ds_entry.p_ds->attributes["units"];
  string units_value(reinterpret_cast<char *>(attr_val.get()), attr_val.size);
  if (units_value.find("since") != string::npos) {
    if (!dgd.indexes.time_var.empty()) {
      log_error2("time was already identified - don't know what to do with "
          "variable: " + var_name, F, g_util_ident);
    }
    metautils::CF::fill_nc_time_data(units_value, time_data, USER);
    dgd.indexes.time_var = var_name;
  } else if (units_value == "degrees_north") {
    if (dgd.indexes.lat_var.empty()) {
      dgd.indexes.lat_var = var_name;
    }
  } else if (units_value == "degrees_east") {
    if (dgd.indexes.lon_var.empty()) {
      dgd.indexes.lon_var = var_name;
    }
  }
}

void fill_dgd_index(string attribute_name_to_match, string
    attribute_value_to_match, string& dgd_index) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto ds_entry_list = is->datasets_with_attribute(attribute_name_to_match);
  if (ds_entry_list.size() > 1) {
    log_error2("more than one " + attribute_name_to_match + " variable found",
        F, g_util_ident);
  } else if (ds_entry_list.size() > 0) {
    auto aval = ds_entry_list.front().p_ds->attributes[attribute_name_to_match];
    string attr_val(reinterpret_cast<char *>(aval.get()), aval.size);
    if (attribute_value_to_match.empty() || attr_val ==
        attribute_value_to_match) {
      if (!dgd_index.empty()) {
        log_error2(attribute_name_to_match + " was already identified - don't "
            "know what to do with variable: " + ds_entry_list.front().key, F,
            g_util_ident);
      } else {
        dgd_index = ds_entry_list.front().key;
      }
    }
  }
}

void fill_dgd_index(string attribute_name_to_match, unordered_map<string,
    string>& dgd_index) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto ds_entry_list = is->datasets_with_attribute(attribute_name_to_match);
  for (const auto& ds_entry : ds_entry_list) {
    auto aval = ds_entry.p_ds->attributes[attribute_name_to_match];
    string attr_val(reinterpret_cast<char *>(aval.get()), aval.size);
    if (dgd_index.find(attr_val) == dgd_index.end()) {
      dgd_index.emplace(attr_val, ds_entry.key);
    } else {
      log_error2(attribute_name_to_match + " was already identified - don't "
          "know what to do with variable: " + ds_entry.key, F, g_util_ident);
    }
  }
}

void process_vertical_coordinate_variable(DiscreteGeometriesData& dgd, string&
    obs_type) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  obs_type = "";
  auto ds = is->dataset("/" + dgd.indexes.z_var);
  auto attr_it = ds->attributes.find("units");
  if (attr_it != ds->attributes.end()) {
    dgd.z_units.assign(reinterpret_cast<char *>(attr_it->second.get()),
        attr_it->second.size);
    trim(dgd.z_units);
  }
  attr_it = ds->attributes.find("positive");
  if (attr_it != ds->attributes.end()) {
    dgd.z_pos.assign(reinterpret_cast<char *>(attr_it->second.get()),
        attr_it->second.size);
    trim(dgd.z_pos);
    dgd.z_pos = strutils::to_lower(dgd.z_pos);
  }
  if (dgd.z_pos.empty() && !dgd.z_units.empty()) {
    auto z_units_l = strutils::to_lower(dgd.z_units);
    if (regex_search(dgd.z_units, regex("Pa$")) || regex_search(
        z_units_l, regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
      dgd.z_pos = "down";
      obs_type = "upper_air";
    }
  }
  if (dgd.z_pos.empty()) {
    log_error2("unable to determine vertical coordinate direction", F,
        g_util_ident);
  } else if (obs_type.empty()) {
    if (dgd.z_pos == "up") {
      obs_type = "upper_air";
    } else if (dgd.z_pos == "down") {
      auto z_units_l = strutils::to_lower(dgd.z_units);
      if (regex_search(dgd.z_units, regex("Pa$")) ||
          regex_search(z_units_l, regex("^mb(ar){0,1}$")) ||
          z_units_l == "millibars") {
        obs_type = "upper_air";
      } else if (dgd.z_units == "m") {
        obs_type = "subsurface";
      }
    }
  }
}

void scan_cf_point_hdf5nc4_file(ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "  Beginning " << F << "..." << endl;
  }
  auto is = sget_hdf5();
  auto ds_entry_list = is->datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  ds_entry_list = is->datasets_with_attribute("coordinates");

  // look for a "station ID"
  for (const auto& ds_entry : ds_entry_list) {
    if (ds_entry.p_ds->datatype.class_ == 3) {
      auto attr_it = ds_entry.p_ds->attributes.find("long_name");
      if (attr_it != ds_entry.p_ds->attributes.end() && attr_it->second._class_
          == 3) {
        string attribute_value(reinterpret_cast<char *>(attr_it->
            second.get()), attr_it->second.size);
        if (regex_search(attribute_value, regex("ID")) ||
            regex_search(attribute_value, regex("ident",
            regex::icase))) {
          dgd.indexes.stn_id_var = ds_entry.key;
        }
      }
    }
    if (!dgd.indexes.stn_id_var.empty()) {
      break;
    }
  }
  HDF5::DataArray time_vals;
  if (dgd.indexes.time_var.empty()) {
    log_error2("unable to determine time variable", F, g_util_ident);
  } else {
    if (gatherxml::verbose_operation) {
      cout << "    found time variable '" << dgd.indexes.time_var << "'." <<
          endl;
    }
    auto ds = is->dataset("/" + dgd.indexes.time_var);
    if (ds == nullptr) {
      log_error2("unable to access time variable", F, g_util_ident);
    }
    auto attr_it = ds->attributes.find("calendar");
    if (attr_it != ds->attributes.end()) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr_it->second.get()),
        attr_it->second.size);
    }
    time_vals.fill(*is, *ds);
  }
  HDF5::DataArray lat_vals;
  if (dgd.indexes.lat_var.empty()) {
    log_error2("unable to determine latitude variable", F, g_util_ident);
  } else {
    if (gatherxml::verbose_operation) {
      cout << "    found latitude variable '" << dgd.indexes.lat_var << "'." <<
          endl;
    }
    auto ds = is->dataset("/" + dgd.indexes.lat_var);
    if (ds == nullptr) {
      log_error2("unable to access latitude variable", F, g_util_ident);
    }
    lat_vals.fill(*is, *ds);
  }
  HDF5::DataArray lon_vals;
  if (dgd.indexes.lon_var.empty()) {
    log_error2("unable to determine longitude variable", F, g_util_ident);
  } else {
    if (gatherxml::verbose_operation) {
      cout << "    found longitude variable '" << dgd.indexes.lon_var << "'." <<
          endl;
    }
    auto ds = is->dataset("/" + dgd.indexes.lon_var);
    if (ds == nullptr) {
      log_error2("unable to access longitude variable", F, g_util_ident);
    }
    lon_vals.fill(*is, *ds);
  }
  HDF5::DataArray id_vals;
  if (dgd.indexes.stn_id_var.empty()) {
    log_error2("unable to determine report ID variable", F, g_util_ident);
  } else {
    if (gatherxml::verbose_operation) {
      cout << "    found report ID variable '" << dgd.indexes.stn_id_var << "'."
          << endl;
    }
    auto ds = is->dataset("/" + dgd.indexes.stn_id_var);
    if (ds == nullptr) {
      log_error2("unable to access report ID variable", F, g_util_ident);
    }
    id_vals.fill(*is, *ds);
  }
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/HDF5.ds" + g_dsid + ".xml", scan_data.tdir->
      name());
  scan_data.found_map = !scan_data.map_name.empty();
  vector<DateTime> date_times;
  date_times.reserve(time_vals.num_values);
  vector<string> ids;
  ids.reserve(time_vals.num_values);
  vector<float> lats, lons;
  lats.reserve(time_vals.num_values);
  lons.reserve(time_vals.num_values);
  string platform_type = "unknown";
  gatherxml::markup::ObML::IDEntry ientry;
  ientry.key.reserve(32768);
  if (gatherxml::verbose_operation) {
    cout << "    Ready to scan netCDF variables ..." << endl;
  }
  for (const auto& ds_entry : ds_entry_list) {
    auto& var_name = ds_entry.key;
    if (var_name != dgd.indexes.time_var && var_name != dgd.indexes.lat_var &&
        var_name != dgd.indexes.lon_var && var_name != dgd.indexes.stn_id_var) {
      unique_data_type_observation_set.clear();
      de.key = var_name;
      auto ds = is->dataset("/" + var_name);
      string descr, units;
      for (const auto& attr_entry : ds->attributes) {
      auto lkey = strutils::to_lower(attr_entry.first);
        if (lkey == "long_name" || (descr.empty() && (lkey == "description" ||
            lkey.find("comment") == 0))) {
          descr.assign(reinterpret_cast<char *>(attr_entry.second.array));
        } else if (lkey == "units") {
          units.assign(reinterpret_cast<char *>(attr_entry.second.array));
        }
      }
      trim(descr);
      trim(units);
      auto key = var_name + "<!>" + descr + "<!>" + units;
      scan_data.varlist.emplace_back(key);
      if (scan_data.found_map && scan_data.var_changes_table.find(var_name) ==
          scan_data.var_changes_table.end()) {
        scan_data.var_changes_table.emplace(var_name);
      }
      HDF5::DataArray var_data;
      var_data.fill(*is, *ds);
      auto var_missing_value = HDF5::decode_data_value(ds->datatype,
          ds->fillvalue.bytes, 1.e33);
      if (gatherxml::verbose_operation) {
        cout << "      checking " << time_vals.num_values << " times for '" <<
            key << "' ..." << endl;
      }
      for (size_t n = 0; n < time_vals.num_values; ++n) {
        if (n == date_times.size()) {
          date_times.emplace_back(compute_nc_time(time_vals, time_data, n));
        }
        if (n == ids.size()) {
          auto lat_val = lat_vals.value(n);
          lats.emplace_back(lat_val);
          auto lon_val = lon_vals.value(n);
          if (lon_val > 180.) {
            lon_val -= 360.;
          }
          lons.emplace_back(lon_val);
          auto id_val = id_vals.string_value(n);
          for (auto c = id_val.begin(); c !=  id_val.end(); ) {
            if (*c < 32 || *c > 126) {
              id_val.erase(c);
            } else {
              ++c;
            }
          }
          if (!id_val.empty()) {
            trim(id_val);
          }
          if (scan_data.convert_ids_to_upper_case) {
            ids.emplace_back(strutils::to_upper(id_val));
          } else {
            ids.emplace_back(id_val);
          }
        }
        if (!ids[n].empty() && var_data.value(n) != var_missing_value) {
          if (!obs_data.added_to_platforms("surface", platform_type, lats[n],
              lons[n])) {
            auto error = move(myerror);
            log_error2(error + " when adding platform " + platform_type, F,
                g_util_ident);
          }
          ientry.key = platform_type + "[!]unknown[!]" + ids[n];
          if (!obs_data.added_to_ids("surface", ientry, var_name, "", lats[n],
              lons[n], time_vals.value(n), &date_times[n])) {
            auto error = move(myerror);
            log_error2(error + " when adding ID " + ientry.key, F,
                g_util_ident);
          }
          ++scan_data.num_not_missing;
        }
      }
      ds->free();
    }
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_cf_orthogonal_time_series_hdf5nc4_file(const DiscreteGeometriesData&
    dgd, const TimeData& time_data, ScanData& scan_data, gatherxml::markup::
    ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto is = sget_hdf5();
  if (dgd.indexes.lat_var.empty()) {
    log_error2("latitude could not be identified", F, g_util_ident);
  }
  if (dgd.indexes.lon_var.empty()) {
    log_error2("longitude could not be identified", F, g_util_ident);
  }
  vector<string> platform_types, id_types, id_cache;
  size_t num_stns = 0;
  if (dgd.indexes.stn_id_var.empty()) {
    auto root_ds = is->dataset("/");
    size_t known_sources = 0xffffffff;
    if (root_ds == nullptr) {
      log_error2("unable to get root dataset", F, g_util_ident);
    }
    for (const auto& attr_entry : root_ds->attributes) {
      if (strutils::to_lower(attr_entry.first) == "title") {
        stringstream title_ss;
        attr_entry.second.print(title_ss, nullptr);
        if (strutils::to_lower(title_ss.str()) == "\"hadisd\"") {
          known_sources = 0x1;
        }
      }
    }
    switch (known_sources) {
      case 0x1: {
        auto attr_it = root_ds->attributes.find("station_id");
        if (attr_it != root_ds->attributes.end()) {
          stringstream id_ss;
          attr_it->second.print(id_ss, nullptr);
          auto id = id_ss.str();
          strutils::replace_all(id, "\"", "");
          auto id_parts = split(id, "-");
          if (id_parts.front() != "999999") {
            id_types.emplace_back("WMO+6");
            id_cache.emplace_back(id_parts.front());
            if (id_parts.front() >= "990000" && id_parts.front() < "991000") {
              platform_types.emplace_back("fixed_ship");
            } else if ((id_parts.front() >= "992000" && id_parts.front() <
                "993000") || (id_parts.front() >= "995000" && id_parts.front() <
                "998000")) {
              platform_types.emplace_back("drifting_buoy");
            } else {
              platform_types.emplace_back("land_station");
            }
          } else {
            id_types.emplace_back("WBAN");
            id_cache.emplace_back(id_parts.back());
            platform_types.emplace_back("land_station");
          }
          num_stns = 1;
        }
        break;
      }
    }
    if (id_cache.size() == 0) {
      log_error2("timeseries_id role could not be identified", F, g_util_ident);
    }
  }
  auto times_ds = is->dataset("/" + dgd.indexes.time_var);
  if (times_ds == nullptr) {
    log_error2("unable to get time dataset", F, g_util_ident);
  }
  NetCDFVariableAttributeData nc_ta_data;
  extract_from_hdf5_variable_attributes(times_ds->attributes, nc_ta_data);
  HDF5::DataArray time_vals;
  time_vals.fill(*is, *times_ds);
  auto lats_ds = is->dataset("/" + dgd.indexes.lat_var);
  if (lats_ds == nullptr) {
    log_error2("unable to get latitude dataset", F, g_util_ident);
  }
  HDF5::DataArray lat_vals;
  lat_vals.fill(*is, *lats_ds);
  if (lat_vals.num_values != num_stns) {
    log_error2("number of stations does not match number of latitudes", F,
        g_util_ident);
  }
  auto lons_ds = is->dataset("/" + dgd.indexes.lon_var);
  if (lons_ds == nullptr) {
    log_error2("unable to get longitude dataset", F, g_util_ident);
  }
  HDF5::DataArray lon_vals;
  lon_vals.fill(*is, *lons_ds);
  if (lon_vals.num_values != num_stns) {
    log_error2("number of stations does not match number of longitudes", F,
        g_util_ident);
  }
  if (platform_types.size() == 0) {
log_error2("determining platforms is not implemented", F, g_util_ident);
  }
  for (size_t n = 0; n < num_stns; ++n) {
    if (!obs_data.added_to_platforms("surface", platform_types[n],
        lat_vals.value(n), lon_vals.value(n))) {
      auto error = move(myerror);
      log_error2(error + "' when adding platform " + platform_types[n], F,
          g_util_ident);
    }
  }
  auto ds_entry_list = is->datasets_with_attribute("DIMENSION_LIST");
  auto netcdf_var_re = regex("^\\[" + dgd.indexes.time_var +
      "(,.{1,}){0,1}\\]$");
  vector<DateTime> dts;
  vector<string> datatypes_list;
  for (const auto& ds_entry : ds_entry_list) {
    auto& var_name = ds_entry.key;
    if (var_name != dgd.indexes.time_var && var_name != dgd.indexes.lat_var &&
        var_name != dgd.indexes.lon_var) {
      stringstream dim_list_ss;
      ds_entry.p_ds->attributes["DIMENSION_LIST"].print(dim_list_ss,
          is->reference_table_pointer());
      if (regex_search(dim_list_ss.str(), netcdf_var_re)) {
        if (gatherxml::verbose_operation) {
          cout << "Scanning netCDF variable '" << var_name << "' ..." <<
              endl;
        }
      }
      auto var_ds = is->dataset("/" + var_name);
      if (var_ds == nullptr) {
        log_error2("unable to get data for variable '" + var_name + "'", F,
            g_util_ident);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_hdf5_variable_attributes(var_ds->attributes, nc_va_data);
      HDF5::DataArray var_vals;
      var_vals.fill(*is, *var_ds);
      datatypes_list.emplace_back(var_name + "<!>" + nc_va_data.long_name +
          "<!>" + nc_va_data.units + "<!>" + nc_va_data.cf_keyword);
      for (size_t n = 0; n < num_stns; ++n) {
        gatherxml::markup::ObML::IDEntry ientry;
        ientry.key = platform_types[n] + "[!]" + id_types[n] + "[!]" +
            id_cache[n];
        for (size_t m = 0; m < time_vals.num_values; ++m) {
          if (dts.size() != time_vals.num_values) {
            dts.emplace_back(compute_nc_time(time_vals, time_data, m));
          }
          if (!found_missing(time_vals.value(m), time_vals.type,
              &nc_ta_data.missing_value, var_vals, m,
              nc_va_data.missing_value)) {
            if (!obs_data.added_to_ids("surface", ientry, var_name, "",
                lat_vals.value(n), lon_vals.value(n), time_vals.value(m),
                &dts[m])) {
              auto error = move(myerror);
              log_error2(error + "' when adding ID " + ientry.key, F,
                  g_util_ident);
            }
            ++scan_data.num_not_missing;
          }
        }
      }
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << "() done." << endl;
  }
}

void scan_cf_time_series_hdf5nc4_file(ScanData& scan_data, gatherxml::markup::
    ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto is = sget_hdf5();
  auto ds_entry_list = is->datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  if (dgd.indexes.time_var.empty()) {
    log_error2("unable to determine time variable", F, g_util_ident);
  }
  fill_dgd_index("cf_role", "profile_id", dgd.indexes.stn_id_var);
  fill_dgd_index("sample_dimension", dgd.indexes.sample_dim_vars);
  fill_dgd_index("instance_dimension", "",
      dgd.indexes.instance_dim_var);
  auto ds = is->dataset("/" + dgd.indexes.time_var);
  auto attr_it = ds->attributes.find("_Netcdf4Dimid");
  if (attr_it != ds->attributes.end()) {

    // ex. H.2, H.4 (single version of H.2), H.5 (precise locations) stns w/same
    //     times
    scan_cf_orthogonal_time_series_hdf5nc4_file(dgd, time_data, scan_data,
        obs_data);
  } else {

// ex. H.3 stns w/varying times but same # of obs
// ex. H.6 w/sample_dimension
// ex. H.7 w/instance_dimension
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_cf_non_orthogonal_profile_hdf5nc4_file(const DiscreteGeometriesData&
    dgd, const TimeData& time_data, const HDF5::DataArray& time_vals, const
    NetCDFVariableAttributeData& nc_ta_data, ScanData&
    scan_data, gatherxml::markup::ObML::ObservationData& obs_data, string
    obs_type) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto ds = is->dataset("/" + dgd.indexes.stn_id_var);
  if (ds == nullptr) {
    log_error2("unable to access station ID variable", F, g_util_ident);
  }
  HDF5::DataArray id_vals(*is, *ds);
  ds = is->dataset("/" + dgd.indexes.lat_var);
  if (ds == nullptr) {
    log_error2("unable to access latitude variable", F, g_util_ident);
  }
  HDF5::DataArray lat_vals(*is, *ds);
  ds = is->dataset("/" + dgd.indexes.lon_var);
  if (ds == nullptr) {
    log_error2("unable to access longitude variable", F, g_util_ident);
  }
  HDF5::DataArray lon_vals(*is, *ds);
  if (id_vals.num_values != time_vals.num_values || lat_vals.num_values !=
        id_vals.num_values || lon_vals.num_values != lat_vals.num_values) {
    log_error2("profile data does not follow the CF conventions", F,
        g_util_ident);
  }
  vector<string> platform_types, id_types;
  if (!scan_data.platform_type.empty()) {
    for (size_t n = 0; n < id_vals.num_values; ++n) {
      platform_types.emplace_back(scan_data.platform_type);
id_types.emplace_back("unknown");
      auto lat = lat_vals.value(n);
      if (lat < -90. || lat > 90.) {
        lat = -999.;
      }
      auto lon = lon_vals.value(n);
      if (lon < -180. || lon > 360.) {
        lon = -999.;
      }
      if (lat > -999. && lon > -999. && !obs_data.added_to_platforms(obs_type,
          platform_types[n], lat, lon)) {
        auto error = move(myerror);
        log_error2(error + "' when adding platform " + platform_types[n], F,
            g_util_ident);
      }
    }
  } else {
    log_error2("undefined platform type", F, g_util_ident);
  }
  ds = is->dataset("/" + dgd.indexes.z_var);
  if (ds == nullptr) {
    log_error2("unable to access vertical level variable", F, g_util_ident);
  }
  HDF5::DataArray z_vals(*is, *ds);
  auto attr_it = ds->attributes.find("DIMENSION_LIST");
  if (attr_it == ds->attributes.end()) {
    log_error2("unable to get vertical level row size variable", F,
        g_util_ident);
  }
  stringstream val_ss;
  attr_it->second.print(val_ss, is->reference_table_pointer());
  auto var_list = is->datasets_with_attribute("DIMENSION_LIST");
  auto dims = split(val_ss.str(), ",");
  strutils::replace_all(dims.front(), "[", "");
  strutils::replace_all(dims.front(), "]", "");
  trim(dims.front());
  ds = is->dataset("/" + dgd.indexes.sample_dim_vars.at(dims.front()));
  if (ds == nullptr) {
    log_error2("unable to get vertical level row sizes", F, g_util_ident);
  }
  HDF5::DataArray z_rowsize_vals(*is, *ds);
  if (dgd.indexes.sample_dim_vars.size() > 0) {
// continuous ragged array H.10
    for (const auto& dse : var_list) {
      auto& var_name = dse.key;
      val_ss.str("");
      dse.p_ds->attributes["DIMENSION_LIST"].print(val_ss, is->
          reference_table_pointer());
      auto dims = split(val_ss.str(), ",");
      strutils::replace_all(dims.front(), "[", "");
      strutils::replace_all(dims.front(), "]", "");
      trim(dims.front());
      if (dgd.indexes.sample_dim_vars.find(dims.front()) !=
          dgd.indexes.sample_dim_vars.end() && var_name != dgd.indexes.z_var) {
        ds = is->dataset("/" + dgd.indexes.sample_dim_vars.at(
            dims.front()));
        if (ds == nullptr) {
          log_error2("unable to get row size data for " + var_name, F,
              g_util_ident);
        }
        HDF5::DataArray rowsize_vals(*is, *ds);
        ds = is->dataset("/" + var_name);
        if (ds == nullptr) {
          log_error2("unable to get variable values for " + var_name, F,
              g_util_ident);
        }
        HDF5::DataArray var_vals(*is, *ds);
        if (var_vals.type != HDF5::DataArray::Type::_NULL) {
          NetCDFVariableAttributeData nc_va_data;
          extract_from_hdf5_variable_attributes(dse.p_ds->attributes,
              nc_va_data);
          auto key = var_name + "<!>" + nc_va_data.long_name + "<!>" +
              nc_va_data.units;
          scan_data.varlist.emplace_back(key);
          if (scan_data.found_map && scan_data.var_changes_table.find(var_name)
              == scan_data.var_changes_table.end()) {
            scan_data.var_changes_table.emplace(var_name);
          }
          auto var_off = 0;
          auto z_off = 0;
          for (size_t n = 0; n < time_vals.num_values; ++n) {
            auto lat = lat_vals.value(n);
            if (lat < -90. || lat > 90.) {
              lat = -999.;
            }
            auto lon = lon_vals.value(n);
            if (lon < -180. || lon > 360.) {
              lon = -999.;
            }
            vector<double> level_list;
            for (size_t m = 0; m < rowsize_vals.value(n); ++m) {
              if (!found_missing(time_vals.value(n), time_vals.type,
                  &nc_ta_data.missing_value, var_vals, var_off,
                  nc_va_data.missing_value)) {
                level_list.emplace_back(z_vals.value(z_off + m));
              }
              ++var_off;
            }
            z_off += z_rowsize_vals.value(n);
            if (level_list.size() > 0 && lat > -999. && lon > -999.) {
              auto dt = compute_nc_time(time_vals, time_data, n);
              gatherxml::markup::ObML::IDEntry ientry;
              ientry.key = platform_types[n] + "[!]" + id_types[n] + "[!]";
              if (id_vals.type == HDF5::DataArray::Type::INT || id_vals.type ==
                  HDF5::DataArray::Type::FLOAT || id_vals.type ==
                  HDF5::DataArray::Type::DOUBLE) {
                ientry.key += ftos(id_vals.value(n));
              } else if (id_vals.type == HDF5::DataArray::Type::STRING) {
                ientry.key += id_vals.string_value(n);
              }
              if (!obs_data.added_to_ids(obs_type, ientry, var_name, "",
                  lat_vals.value(n), lon_vals.value(n), time_vals.value(n),
                  &dt)) {
                auto error = move(myerror);
                log_error2(error + "' when adding ID " + ientry.key, F,
                    g_util_ident);
              }
              if (level_list.size() > 1) {
                gatherxml::markup::ObML::DataTypeEntry dte;
                ientry.data->data_types_table.found(var_name, dte);
                dte.fill_vertical_resolution_data(level_list, dgd.z_pos,
                    dgd.z_units);
              }
              ++scan_data.num_not_missing;
            }
          }
        }
      }
    }
  } else if (!dgd.indexes.instance_dim_var.empty()) {

// indexed ragged array H.11
log_error2("indexed ragged array not implemented", F, g_util_ident);
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_orthogonal_profile_hdf5nc4_file(const DiscreteGeometriesData& dgd,
    const HDF5::DataArray& time_vals, const NetCDFVariableAttributeData&
    nc_ta_data, ScanData& scan_data, gatherxml::markup::ObML::ObservationData&
    obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_profile_hdf5nc4_file(ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto is = sget_hdf5();
  auto ds_entry_list = is->datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  fill_dgd_index("cf_role", "profile_id", dgd.indexes.stn_id_var);
  fill_dgd_index("sample_dimension", dgd.indexes.sample_dim_vars);
  fill_dgd_index("instance_dimension", "",
      dgd.indexes.instance_dim_var);
  HDF5::DataArray time_vals;
  NetCDFVariableAttributeData nc_ta_data;
  if (dgd.indexes.time_var.empty()) {
    log_error2("unable to determine time variable", F, g_util_ident);
  } else {
    auto ds = is->dataset("/" + dgd.indexes.time_var);
    if (ds == nullptr) {
      log_error2("unable to access time variable", F, g_util_ident);
    }
    auto attr_it = ds->attributes.find("calendar");
    if (attr_it != ds->attributes.end()) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr_it->second.get()),
          attr_it->second.size);
    }
    time_vals.fill(*is, *ds);
    extract_from_hdf5_variable_attributes(ds->attributes, nc_ta_data);
  }
  fill_dgd_index("axis", "Z", dgd.indexes.z_var);
  if (dgd.indexes.z_var.empty()) {
    fill_dgd_index("positive", "", dgd.indexes.z_var);
  }
  string obs_type;
  if (dgd.indexes.z_var.empty()) {
    log_error2("unable to determine vertical coordinate variable", F,
        g_util_ident);
  } else {
    process_vertical_coordinate_variable(dgd, obs_type);
  }
  if (obs_type.empty()) {
    log_error2("unable to determine observation type", F, g_util_ident);
  }
  scan_data.map_name = unixutils::remote_web_file("https://rda.ucar.edu/"
      "metadata/ParameterTables/netCDF4.ds" + g_dsid + ".xml", scan_data.tdir->
      name());
  scan_data.found_map=(!scan_data.map_name.empty());
  if (dgd.indexes.sample_dim_vars.size() > 0 ||
      !dgd.indexes.instance_dim_var.empty()) {

    // ex. H.10, H.11
    scan_cf_non_orthogonal_profile_hdf5nc4_file(dgd, time_data, time_vals,
        nc_ta_data, scan_data, obs_data, obs_type);
  } else {

    // ex. H.8, H.9
    scan_cf_orthogonal_profile_hdf5nc4_file(dgd, time_vals, nc_ta_data,
        scan_data, obs_data, obs_type);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void find_coordinate_variables(CoordinateVariables& coord_vars, GridData&
    grid_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto dim_vars = is->datasets_with_attribute("CLASS=DIMENSION_SCALE");
  if (gatherxml::verbose_operation) {
    cout << "...found " << dim_vars.size() << " 'DIMENSION_SCALE' "
        "variables:" << endl;
  }
  unordered_set<string> unique_level_id_set;
  auto found_time = false;
  for (const auto& dse : dim_vars) {
    auto& var_name = dse.key;
    if (gatherxml::verbose_operation) {
      cout << "  '" << var_name << "'" << endl;
    }
    auto attr_it = dse.p_ds->attributes.find("units");
    if (attr_it != dse.p_ds->attributes.end() && (attr_it->second._class_ ==
        3 || (attr_it->second._class_ == 9 && attr_it->second.vlen.class_ ==
        3))) {
      string units_value;
      if (attr_it->second._class_ == 3) {
        units_value = reinterpret_cast<char *>(attr_it->second.array);
      } else {
        int len = (attr_it->second.vlen.buffer[0] << 24) + (attr_it->
            second.vlen.buffer[1] << 16) + (attr_it->second.vlen.buffer[2] <<
            8) + attr_it->second.vlen.buffer[3];
        units_value = string(reinterpret_cast<char *>(&attr_it->
            second.vlen.buffer[4]), len);
      }
      if (gatherxml::verbose_operation) {
        cout << "    units attribute: '" << units_value << "'" << endl;
      }
      string standard_name_value;
      attr_it = dse.p_ds->attributes.find("standard_name");
      if (attr_it != dse.p_ds->attributes.end() && (attr_it->second._class_ ==
          3 || (attr_it->second._class_ == 9 && attr_it->second.vlen.class_ ==
          3))) {
        if (attr_it->second._class_ == 3) {
          standard_name_value = reinterpret_cast<char *>(attr_it->second.array);
        } else {
          int len = (attr_it->second.vlen.buffer[0] << 24) + (attr_it->
              second.vlen.buffer[1] << 16) + (attr_it->second.vlen.buffer[2] <<
              8) + attr_it->second.vlen.buffer[3];
          standard_name_value = string(reinterpret_cast<char *>(&attr_it->
              second.vlen.buffer[4]), len);
        }
      }
      if (units_value.find("since") != string::npos) {
        if (found_time) {
          log_error2("time was already identified - don't know what to do with "
              "variable: " + var_name, F, g_util_ident);
        }
        for (const auto& attribute : dse.p_ds->attributes) {
          if (attribute.second._class_ == 3) {
            if (attribute.first == "bounds") {
              grid_data.time_bounds.id = reinterpret_cast<char *>(
                  attribute.second.array);
              grid_data.coordinate_variables_set.emplace(
                  grid_data.time_bounds.id);
              break;
            } else if (attribute.first == "climatology") {
              grid_data.climo_bounds.id = reinterpret_cast<char *>(
                  attribute.second.array);
              break;
            }
          }
        }
        coord_vars.nc_time->units = units_value.substr(0, units_value.find(
            "since"));
        trim(coord_vars.nc_time->units);
        grid_data.valid_time.id = var_name;
        grid_data.coordinate_variables_set.emplace(grid_data.valid_time.id);
        if (standard_name_value == "forecast_reference_time") {
          grid_data.reference_time.id = var_name;
          grid_data.coordinate_variables_set.emplace(grid_data.reference_time.
              id);
        }
        coord_vars.nc_time->reference = metautils::NcTime::reference_date_time(
            units_value);
        if (coord_vars.nc_time->reference.year() == 0) {
          log_error2("bad netcdf date in units for time", F, g_util_ident);
        }
        attr_it = dse.p_ds->attributes.find("calendar");
        if (attr_it != dse.p_ds->attributes.end()) {
          coord_vars.nc_time->calendar.assign(reinterpret_cast<char *>(attr_it->
              second.get()), attr_it->second.size);
          trim(coord_vars.nc_time->calendar);
        }
        found_time = true;
      } else if (units_value == "degrees_north") {
        coord_vars.lat_ids.emplace_back(var_name);
      } else if (units_value == "degrees_east") {
        coord_vars.lon_ids.emplace_back(var_name);
      } else {
        if (standard_name_value == "forecast_period") {
          grid_data.forecast_period.id = var_name;
          grid_data.coordinate_variables_set.emplace(
              grid_data.forecast_period.id);
          if (!units_value.empty()) {
            coord_vars.forecast_period->units = units_value;
            if (coord_vars.forecast_period->units.back() != 's') {
              coord_vars.forecast_period->units.append(1, 's');
            }
          }
        } else {
          if (unique_level_id_set.find(var_name) == unique_level_id_set.end()) {
            coord_vars.level_info.emplace_back();
            coord_vars.level_info.back().ID = var_name;
            attr_it = dse.p_ds->attributes.find("long_name");
            if (attr_it != dse.p_ds->attributes.end() && attr_it->
                second._class_ == 3) {
              coord_vars.level_info.back().description = reinterpret_cast<
                  char *>(attr_it->second.array);
            }
            coord_vars.level_info.back().units = units_value;
            coord_vars.level_info.back().write = false;
            unique_level_id_set.emplace(var_name);
          }
        }
      }
    } else {
      attr_it = dse.p_ds->attributes.find("positive");
      if (attr_it != dse.p_ds->attributes.end() && attr_it->second._class_ ==
          3 && unique_level_id_set.find(var_name) ==
          unique_level_id_set.end()) {
        coord_vars.level_info.emplace_back();
        coord_vars.level_info.back().ID = var_name;
        attr_it = dse.p_ds->attributes.find("long_name");
        if (attr_it != dse.p_ds->attributes.end() && attr_it->second._class_ ==
            3) {
          coord_vars.level_info.back().description = reinterpret_cast<char *>(
              attr_it->second.array);
        }
        coord_vars.level_info.back().units = "";
        coord_vars.level_info.back().write = false;
        unique_level_id_set.emplace(var_name);
      }
    }
  }
  coord_vars.forecast_period->reference = coord_vars.nc_time->reference;
  coord_vars.forecast_period->calendar = coord_vars.nc_time->calendar;
}

void check_for_forecasts(GridData& grid_data, shared_ptr<TimeData>&
    time_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...checking for forecasts..." << endl;
  }
  auto is = sget_hdf5();
  auto vars = is->datasets_with_attribute("standard_name="
      "forecast_reference_time");
  if (vars.size() > 1) {
    log_error2("multiple forecast reference times", F, g_util_ident);
  } else if (!vars.empty()) {
    auto var = vars.front();
    auto attr_it = var.p_ds->attributes.find("units");
    if (attr_it != var.p_ds->attributes.end() && attr_it->second._class_ ==
        3) {
      string units_value = reinterpret_cast<char *>(attr_it->second.array);
      if (units_value.find("since") != string::npos) {
        auto units = units_value.substr(0, units_value.find("since"));
        trim(units);
        if (units != time_data->units) {
          log_error2("time and forecast reference time have different units", F,
              g_util_ident);
        }
        grid_data.reference_time.id = var.key;
        grid_data.coordinate_variables_set.emplace(grid_data.reference_time.id);
        auto ref_dt = metautils::NcTime::reference_date_time(units_value);
        if (ref_dt.year() == 0) {
          log_error2("bad netcdf date in units (" + units_value + ") for "
              "forecast_reference_time", F, g_util_ident);
        }
      }
    }
  }
}

void find_vertical_level_coordinates(CoordinateVariables& coord_vars, GridData&
    grid_data) {
  if (gatherxml::verbose_operation) {
    cout << "...looking for vertical level coordinates..." << endl;
  }
  auto is = sget_hdf5();
  auto vars = is->datasets_with_attribute("units=Pa");
  if (vars.size() == 0) {
    vars = is->datasets_with_attribute("units=hPa");
  }
  for (const auto& dse : vars) {
    auto& var_name = dse.key;
    auto attr_it = dse.p_ds->attributes.find("DIMENSION_LIST");
    if (attr_it != dse.p_ds->attributes.end() && attr_it->
        second.dim_sizes.size() == 1 && attr_it->second.dim_sizes[0] == 1 &&
        attr_it->second._class_ == 9) {
      coord_vars.level_info.emplace_back();
      coord_vars.level_info.back().ID = var_name;
      if (gatherxml::verbose_operation) {
        cout << "   ...found '" << var_name << "'" << endl;
      }
      coord_vars.level_info.back().description = "Pressure Level";
      coord_vars.level_info.back().units = "Pa";
      coord_vars.level_info.back().write = false;
      grid_data.coordinate_variables_set.emplace(var_name);
    }
  }
  vars = is->datasets_with_attribute("positive");
  for (const auto& dse : vars) {
    auto& var_name = dse.key;
    auto attr_it = dse.p_ds->attributes.find("DIMENSION_LIST");
    if (attr_it != dse.p_ds->attributes.end() && attr_it->
        second.dim_sizes.size() == 1 && attr_it->second.dim_sizes[0] == 1 &&
        attr_it->second._class_ == 9) {
      coord_vars.level_info.emplace_back();
      coord_vars.level_info.back().ID = var_name;
      if (gatherxml::verbose_operation) {
        cout << "   ...found '" << var_name << "'" << endl;
      }
      attr_it = dse.p_ds->attributes.find("description");
      if (attr_it != dse.p_ds->attributes.end() && attr_it->second._class_ ==
          3) {
        coord_vars.level_info.back().description = reinterpret_cast<char *>(
            attr_it->second.array);
      } else {
        coord_vars.level_info.back().description = "";
      }
      attr_it = dse.p_ds->attributes.find("units");
      if (attr_it != dse.p_ds->attributes.end() && attr_it->second._class_ ==
          3) {
        coord_vars.level_info.back().units = reinterpret_cast<char *>(
            attr_it->second.array);
      } else {
        coord_vars.level_info.back().units = "";
      }
      coord_vars.level_info.back().write = false;
    }
  }
}

void add_surface_level(CoordinateVariables& coord_vars) {
  coord_vars.level_info.emplace_back();
  coord_vars.level_info.back().ID = "sfc";
  coord_vars.level_info.back().description = "Surface";
  coord_vars.level_info.back().units = "";
  coord_vars.level_info.back().write = false;
}

void get_forecast_data(GridData& gd) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  if (gd.reference_time.id == gd.valid_time.id) {
    if (!gd.forecast_period.id.empty()) {
      gd.forecast_period.ds = is->dataset("/" + gd.forecast_period.id);
      if (gd.forecast_period.ds != nullptr) {
        gd.forecast_period.data_array.fill(*is, *gd.forecast_period.ds);
      }
    }
  } else {
    if (!gd.reference_time.id.empty()) {
      if (gd.time_range_entries.size() > 1) {
        log_error2("forecast data from reference and valid times is not "
            "implemented for multiple time ranges", F, g_util_ident);
      }
      gd.reference_time.ds = is->dataset("/" + gd.  reference_time.id);
      if (gd.reference_time.ds  == nullptr) {
        log_error2("unable to access the /" + gd.reference_time.id + " dataset "
            "for the forecast reference times", F, g_util_ident);
      }
      gd.reference_time.data_array.fill(*is, *gd.reference_time.ds);
      if (gd.reference_time.data_array.num_values != gd.valid_time.data_array.
          num_values) {
        log_error2("number of forecast reference times does not equal number "
            "of times", F, g_util_ident);
      }
      auto it = gd.time_range_entries.find(0.);
      if (it == gd.time_range_entries.end()) {
        log_error2("no time ranges specified", F, g_util_ident);
      }
      for (size_t n = 0; n < gd.valid_time.data_array.num_values; ++n) {
        int m = data_array_value(gd.valid_time.data_array, n, gd.valid_time.ds.
            get()) - data_array_value(gd.reference_time.data_array, n, gd.
            reference_time.ds.get());
        if (m > 0) {
          if (static_cast<int>(it->second.key) == -1) {
            it->second.key = -m * 100;
          }
          if ( (-m * 100) != static_cast<int>(it->second.key)) {
            log_error2("forecast period changed", F, g_util_ident);
          }
        } else if (m < 0) {
          log_error2("found a time value that is less than the forecast "
              "reference time value", F, g_util_ident);
        }
      }
    }
  }
}

void process_time_bounds(const TimeData& nc_time, GridData& gd) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  if (!gd.time_bounds.id.empty()) {
    if (gatherxml::verbose_operation) {
      cout << "...found time bounds '" << gd.time_bounds.id << "'..." << endl;
    }
    gd.time_bounds.ds = is->dataset("/" + gd.time_bounds.id);
    if (gd.time_bounds.ds == nullptr) {
      log_error2("unable to access the /" + gd.time_bounds.id + " dataset for "
          "the time bounds", F, g_util_ident);
    }
    HDF5::DataArray bounds(*is, *gd.time_bounds.ds);
    if (bounds.num_values > 0) {
      add_time_range_entries(nc_time, bounds, gd);
    }
  } else if (!gd.climo_bounds.id.empty()) {
    gd.climo_bounds.ds = is->dataset("/" + gd.climo_bounds.id);
    if (gd.climo_bounds.ds == nullptr) {
      log_error2("unable to access the /" + gd.climo_bounds.id + " dataset for "
          "the climatology bounds", F, g_util_ident);
    }
    HDF5::DataArray bounds(*is, *gd.climo_bounds.ds);
    if (bounds.num_values > 0) {
      add_time_range_entries(nc_time, bounds, gd);
      for (auto& e : gd.time_range_entries) {
        auto& tre2 = e.second;
        tre2.key = tre2.bounded.last_valid_datetime.years_since(tre2.bounded.
            first_valid_datetime);
        tre2.instantaneous.last_valid_datetime = tre2.bounded.
            last_valid_datetime.years_subtracted(tre2.key);
        if (tre2.instantaneous.last_valid_datetime == tre2.bounded.
            first_valid_datetime) {
          tre2.unit = 3;
        } else if (tre2.instantaneous.last_valid_datetime.months_since(tre2.
            bounded.first_valid_datetime) == 3) {
          tre2.unit = 2;
        } else if (tre2.instantaneous.last_valid_datetime.months_since(tre2.
            bounded.first_valid_datetime) == 1) {
          tre2.unit = 1;
        } else {
          log_error2("unable to determine climatology unit", F, g_util_ident);
        }

        // COARDS convention for climatology over all-available years
        if (tre2.instantaneous.first_valid_datetime.year() == 0) {
          tre2.key = 0x7fffffff;
        }
      }
    }
  }
}

void set_month_end_date(GridData& gd, string calendar) {
  for (auto& e : gd.time_range_entries) {
    auto& i = e.second.instantaneous;
    if (i.first_valid_datetime.day() == 1 && i.first_valid_datetime.time() ==
        0) {
      i.last_valid_datetime.add_seconds(dateutils::days_in_month(i.
          last_valid_datetime.year(), i.last_valid_datetime.month(), calendar) *
          86400 - 1, calendar);
    }
    auto& b = e.second.bounded;
    if (!gd.time_bounds.id.empty()) {
      if (b.first_valid_datetime.day() == 1) {
        b.last_valid_datetime.add_days(dateutils::days_in_month(b.
            last_valid_datetime.year(), b.last_valid_datetime.month(), calendar)
            - 1, calendar);
      }
    } else if (!gd.climo_bounds.id.empty()) {
      if (b.first_valid_datetime.day() == b.last_valid_datetime.day() && b.
          first_valid_datetime.time() == 0 && b.last_valid_datetime.time() ==
          0) {
        b.last_valid_datetime.subtract_seconds(1);
      }
    }
  }
}

bool found_alternate_lat_lon_coordinates(GridData& grid_data, vector<string>&
    lat_ids, vector<string>& lon_ids) {
  auto is = sget_hdf5();
  vector<string> compass{"north", "east"};
  unordered_map<string, string> dim_map[compass.size()];
  for (size_t n = 0; n < compass.size(); ++n) {
    auto vars = is->datasets_with_attribute("units=degrees_" + compass[n]);
    if (vars.empty()) {
      vars = is->datasets_with_attribute("units=degree_" + compass[n]);
    }
    for (const auto& dse : vars) {
      auto& var_name = dse.key;
      auto test_vars = is->datasets_with_attribute("bounds=" + var_name);
      if (test_vars.empty()) {
        if (gatherxml::verbose_operation) {
          cout << "   ...found '" << var_name << "'" << endl;
        }
        stringstream dimension_list;
        dse.p_ds->attributes["DIMENSION_LIST"].print(dimension_list, is->
            reference_table_pointer());
        dim_map[n].emplace(dimension_list.str(), var_name);
        grid_data.coordinate_variables_set.emplace(var_name);
      }
    }
  }
  vector<vector<string> *> id_list{&lat_ids, &lon_ids};
  for (const auto& entry : dim_map[0]) {
    id_list[0]->emplace_back(entry.second);
    for (size_t n = 1; n < compass.size(); ++n) {
      auto var_name = dim_map[n].find(entry.first);
      if (var_name != dim_map[n].end()) {
        id_list[n]->emplace_back(var_name->second);
      }
    }
  }
  return !(lat_ids.empty() || lon_ids.empty());
}

void find_alternate_lat_lon_coordinates(GridData& grid_data, vector<string>&
    lat_ids, vector<string>& lon_ids) {
  if (gatherxml::verbose_operation) {
    cout << "...looking for alternate latitude and longitude coordinates..."
        << endl;
  }
  if (!found_alternate_lat_lon_coordinates(grid_data, lat_ids, lon_ids)) {
    cerr << "Terminating - could not find any latitude/longitude coordinates"
        << endl;
    exit(1);
  }
}

bool grid_is_polar_stereographic(const GridData& grid_data,
    Grid::GridDimensions& dim, Grid::GridDefinition& def) {
  auto center_x = dim.x / 2;
  auto center_y = dim.y / 2;
  auto xm = center_x - 1;
  auto ym = center_y - 1;
  if (myequalf(data_array_value(grid_data.lat.data_array, ym * dim.x + xm,
      grid_data.lat.ds.get()), data_array_value(grid_data.lat.data_array,
      center_y * dim.x + xm, grid_data.lat.ds.get()), 0.00001) && myequalf(
      data_array_value(grid_data.lat.data_array, center_y * dim.x + xm,
      grid_data.lat.ds.get()), data_array_value(grid_data.lat.data_array,
      center_y * dim.x + center_x, grid_data.lat.ds.get()), 0.00001) &&
      myequalf(data_array_value(grid_data.lat.data_array, center_y * dim.x +
      center_x, grid_data.lat.ds.get()), data_array_value(grid_data.lat.
      data_array, ym * dim.x + center_x, grid_data.lat.ds.get()), 0.00001) &&
      myequalf(fabs(data_array_value(grid_data.lon.data_array, ym * dim.x + xm,
      grid_data.lon.ds.get())) + fabs(data_array_value(grid_data.lon.data_array,
      center_y * dim.x + xm, grid_data.lon.ds.get())) + fabs(data_array_value(
      grid_data.lon.data_array, center_y * dim.x + center_x, grid_data.lon.ds.
      get())) + fabs(data_array_value(grid_data.lon.data_array, ym * dim.x +
      center_x, grid_data.lon.ds.get())), 360., 0.00001)) {
    def.type = Grid::Type::polarStereographic;
    if (data_array_value(grid_data.lat.data_array, ym * dim.x + xm,
        grid_data.lat.ds.get()) >= 0.) {
      def.projection_flag = 0;
      def.llatitude = 60.;
    } else {
      def.projection_flag = 1;
      def.llatitude = -60.;
    }
    def.olongitude = lroundf(data_array_value(grid_data.lon.data_array,
        ym * dim.x + xm, grid_data.lon.ds.get()) + 45.);
    if (def.olongitude > 180.) {
      def.olongitude -= 360.;
    }

    // look for dx and dy at the 60-degree parallel
    double min_fabs = 999.;
    int min_m = 0;
    for (size_t m = 0; m < grid_data.lat.data_array.num_values; ++m) {
      auto f = fabs(def.llatitude-data_array_value(
          grid_data.lat.data_array, m, grid_data.lat.ds.get()));
      if (f < min_fabs) {
        min_fabs = f;
        min_m = m;
      }
    }
    double rad = 3.141592654/180.;

    // great circle formula:
    //     theta = 2 * arcsin[ sqrt( sin^2(delta_phi / 2) + cos(phi_1) *
    //         cos(phi_2) * sin^2(delta_lambda / 2) ) ]
    //     phi_1 and phi_2 are latitudes
    //     lambda_1 and lambda_2 are longitudes
    //     dist = 6372.8 * theta
    //     6372.8 is radius of Earth in km
    def.dx = lroundf(asin(sqrt(sin(fabs(data_array_value(
        grid_data.lat.data_array, min_m, grid_data.lat.ds.get()) -
        data_array_value(grid_data.lat.data_array, min_m + 1,
        grid_data.lat.ds.get())) / 2. * rad) * sin(fabs(
        data_array_value(grid_data.lat.data_array, min_m,
        grid_data.lat.ds.get()) - data_array_value(
        grid_data.lat.data_array, min_m + 1,
        grid_data.lat.ds.get())) / 2. * rad) + sin(fabs(data_array_value(
        grid_data.lon.data_array, min_m, grid_data.lon.ds.get()) -
        data_array_value(grid_data.lon.data_array, min_m + 1,
        grid_data.lon.ds.get())) / 2. * rad) * sin(fabs(
        data_array_value(grid_data.lon.data_array, min_m,
        grid_data.lon.ds.get()) - data_array_value(
        grid_data.lon.data_array, min_m + 1,
        grid_data.lon.ds.get())) / 2. * rad) * cos(data_array_value(
        grid_data.lat.data_array, min_m, grid_data.lat.ds.get()) *
        rad) * cos(data_array_value(grid_data.lat.data_array, min_m + 1,
        grid_data.lat.ds.get()) * rad))) * 12745.6);
    def.dy = lroundf(asin(sqrt(sin(fabs(data_array_value(
        grid_data.lat.data_array, min_m, grid_data.lat.ds.get()) -
        data_array_value(grid_data.lat.data_array, min_m + dim.x,
        grid_data.lat.ds.get())) / 2. * rad) * sin(fabs(data_array_value(
        grid_data.lat.data_array, min_m, grid_data.lat.ds.get()) -
        data_array_value(grid_data.lat.data_array, min_m + dim.x,
        grid_data.lat.ds.get())) / 2. * rad) + sin(fabs(data_array_value(
        grid_data.lon.data_array, min_m, grid_data.lon.ds.get()) -
        data_array_value(grid_data.lon.data_array, min_m + dim.x,
        grid_data.lon.ds.get())) / 2. * rad) * sin(fabs(data_array_value(
        grid_data.lon.data_array, min_m, grid_data.lon.ds.get()) -
        data_array_value(grid_data.lon.data_array, min_m + dim.x,
        grid_data.lon.ds.get())) / 2. * rad) * cos(data_array_value(
        grid_data.lat.data_array, min_m, grid_data.lat.ds.get()) *
        rad) * cos(data_array_value(grid_data.lat.data_array, min_m +
        dim.x, grid_data.lat.ds.get()) * rad))) * 12745.6);
    return true;
  }
  return false;
}

bool grid_is_centered_lambert_conformal(const GridData& grid_data, Grid::
    GridDimensions& dims, Grid::GridDefinition& def) {
  if (gatherxml::verbose_operation) {
    cout << "    ...checking for a centered Lambert-Conformal projection..." <<
        endl;
  }
  auto dx2 = dims.x / 2;
  auto dy2 = dims.y / 2;
  switch (dims.x % 2) {
    case 0: {
      auto xm = dx2 - 1;
      auto ym = dy2 - 1;
      auto yp = dy2 + 1;
      if (myequalf((data_array_value(grid_data.lon.data_array, ym * grid_data.
          lon.data_array.dimensions[1] + xm, grid_data.lon.ds.get()) +
          data_array_value(grid_data.lon.data_array, ym * grid_data.lon.
          data_array.dimensions[1] + dx2, grid_data.lon.ds.get())),
          (data_array_value(grid_data.lon.data_array, yp * grid_data.lon.
          data_array.dimensions[1] + xm, grid_data.lon.ds.get()) +
          data_array_value(grid_data.lon.data_array, yp * grid_data.lon.
          data_array.dimensions[1] + dx2, grid_data.lon.ds.get())), 0.0001) &&
          myequalf(data_array_value(grid_data.lat.data_array, dy2 * grid_data.
          lon.data_array.dimensions[1] + xm, grid_data.lat.ds.get()),
          data_array_value(grid_data.lat.data_array, dy2 * grid_data.lon.
          data_array.dimensions[1] + dx2, grid_data.lat.ds.get()))) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(
            data_array_value(grid_data.lat.data_array, dy2 * grid_data.
            lon.data_array.dimensions[1] + dx2, grid_data.lat.ds.
            get()));
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround((data_array_value(grid_data.lon.
            data_array, dy2 * grid_data.lon.data_array.dimensions[1] + xm,
            grid_data.lon.ds.get()) + data_array_value(grid_data.
            lon.data_array, dy2 * grid_data.lon.data_array.
            dimensions[1] + dx2, grid_data.lon.ds.get())) / 2.);
        def.dx = def.dy = lround(111.1 * cos(data_array_value(grid_data.
            lat.data_array, dy2 * grid_data.lon.data_array.
            dimensions[1] + dx2 - 1, grid_data.lat.ds.get()) * 3.141592654
            / 180.) * (data_array_value(grid_data.lon.data_array, dy2 *
            grid_data.lon.data_array.dimensions[1] + dx2, grid_data.
            lon.ds.get()) - data_array_value(grid_data.lon.
            data_array, dy2 * grid_data.lon.data_array.dimensions[1] + dx2
            - 1, grid_data.lon.ds.get())));
        if (gatherxml::verbose_operation) {
          cout << "    ...confirmed a centered Lambert-Conformal projection." <<
              endl;
        }
        return true;
      }
      break;
    }
    case 1: {
      auto xp = dx2 + 1;
      if (myequalf(data_array_value(grid_data.lon.data_array, (dy2 - 1) *
          grid_data.lon.data_array.dimensions[1] + dx2, grid_data.lon.ds.get()),
          data_array_value(grid_data.lon.data_array, (dy2 + 1) * grid_data.lon.
          data_array.dimensions[1] + dx2, grid_data.lon.ds.get()), 0.0001) &&
          myequalf(data_array_value(grid_data.lat.data_array, dy2 * grid_data.
          lon.data_array.dimensions[1] + dx2 - 1, grid_data.lat.ds.get()),
          data_array_value(grid_data.lat.data_array, dy2 * grid_data.lon.
          data_array.dimensions[1] + xp, grid_data.lat.ds.get()), 0.0001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(
            data_array_value(grid_data.lat.data_array, dy2 * grid_data.
            lon.data_array.dimensions[1] + dx2, grid_data.lat.ds.
            get()));
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround(data_array_value(grid_data.lon.data_array,
            dy2 * grid_data.lon.data_array.dimensions[1] + dx2, grid_data.
            lon.ds.get()));
        def.dx = def.dy = lround(111.1*cos(data_array_value(grid_data.lat.
            data_array, dy2 * grid_data.lon.data_array.dimensions[1] +
            dx2, grid_data.lat.ds.get()) * 3.141592654 / 180.) *
            (data_array_value(grid_data.lon.data_array, dy2 * grid_data.
            lon.data_array.dimensions[1] + dx2 + 1, grid_data.lon.
            ds.get()) - data_array_value(grid_data.lon.data_array, dy2 *
            grid_data.lon.data_array.dimensions[1] + dx2, grid_data.
            lon.ds.get())));
        if (gatherxml::verbose_operation) {
          cout << "    ...confirmed a centered Lambert-Conformal projection." <<
              endl;
        }
        return true;
      }
      break;
    }
  }
  if (gatherxml::verbose_operation) {
    cout << "    ...projection is not centered Lambert-Conformal." << endl;
  }
  return false;
}

bool grid_is_non_centered_lambert_conformal(const HDF5::DataArray& lats, const
    HDF5::DataArray& lons, Grid::GridDimensions& dims, Grid::GridDefinition&
    def) {
  if (gatherxml::verbose_operation) {
    cout << "    ...checking for non-centered Lambert-Conformal projection..."
        << endl;
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
      if (lats.value(xoff + 1) <= lats.value(xoff)) {
        v.emplace_back(xoff - yoff);
        s += lons.value(xoff);
        break;
      }
    }
  }

  // find the variance in the x-offsets
  auto xbar = lround(accumulate(v.begin(), v.end(), 0.) / v.size());
  double ss = 0.;
  for (const auto& off : v) {
    auto diff = off - xbar;
    ss += diff*diff;
  }
  auto var = ss / (v.size() - 1);
  if (var >= 1.) {
    return false;
  }
  size_t ybar = dims.y / 2;

  // if the variance is low, confident that we found the orientation longitude
  def.type = Grid::Type::lambertConformal;
  def.olongitude = lround(s / v.size());

  // find the x-direction distance for each row at the orientation longitude
  const double PI = 3.141592654;
  const double DEGRAD = PI / 180.;
  const double KMDEG = 111.1;
  v.clear();
  for (auto n = xbar; n < dims.x * dims.y; n += dims.x) {
    auto x1 = lons.value(n);
    auto x2 = lons.value(n + 1);
    auto dx = fabs(x2 - x1) * KMDEG * cos((lats.value(n) + lats.value(n + 1)) /
        2. * DEGRAD);
    v.emplace_back(dx);
  }
  auto dx_avg = accumulate(v.begin(), v.end(), 0.) / v.size();
  def.dx = lround(dx_avg);
  def.dy = lround(fabs(lats.value(xbar + ybar * dims.x) - lats.value(xbar +
      (ybar-1) * dims.x)) * KMDEG);
  def.stdparallel1 = def.stdparallel2 = -99.;
  for (size_t n = 0; n < v.size(); ++n) {
    if (myequalf(v[n], def.dx, 0.001)) {
      auto p = lround(lats.value(xbar + n * dims.x));
      if (def.stdparallel1 < -90.) {
        def.stdparallel1 = p;
      } else if (def.stdparallel2 < -90.) {
        if (p != def.stdparallel1) {
          def.stdparallel2 = p;
        }
      } else if (p != def.stdparallel2) {
        if (gatherxml::verbose_operation) {
          cout << "...check for a non-centered Lambert-Conformal projection "
              "failed. Too many tangent latitudes." << endl;
        }
        def.type = Grid::Type::not_set;
        return false;
      }
    }
  }
  if (def.stdparallel1 < -90.) {
    if (gatherxml::verbose_operation) {
      cout << "    ...tangent latitudes not identified - now testing "
          "gridpoints..." << endl;
    }
    auto min_lat = ceil(lats.value(xbar));
    auto max_lat = floor(lats.value(xbar + (dims.y - 1) * dims.x));
    auto slat = lats.value(0);
    auto slon = lons.value(0);
    if (slon < 0.) {
      slon += 360.;
    }
    auto olon = def.olongitude;
    if (olon < 0.) {
      olon += 360.;
    }
    auto comp_lat = lats.value(xbar + ybar * dims.x);
    auto comp_lon = lons.value(xbar + ybar * dims.x);
    if (comp_lon < 0.) {
      comp_lon += 360.;
    }
    auto min_dist = dx_avg * 0.75;
    for (auto n = min_lat; n <= max_lat; ++n) {
      double lat, elon;
      gridutils::fill_lat_lon_from_lambert_conformal_gridpoint(xbar, ybar, slat,
          slon, dx_avg, olon, n, lat, elon);
      auto dx = (comp_lon - elon) * KMDEG * cos((comp_lat + lat) / 2. * DEGRAD);
      auto dy = (comp_lat - lat) * KMDEG;
      auto dist = sqrt(dx * dx + dy * dy);
      if (dist < min_dist) {
        min_dist = dist;
        def.stdparallel1 = n;
      }
    }
  }
  if (def.stdparallel1 < -90.) {
    if (gatherxml::verbose_operation) {
      cout << "    ...check for a non-centered Lambert-Conformal projection "
          "failed. No tangent latitude could be identified." << endl;
    }
    def.type = Grid::Type::not_set;
    return false;
  } else if (def.stdparallel2 < -90.) {
    def.stdparallel2 = def.stdparallel1;
  }
  def.llatitude = def.stdparallel1;
  def.projection_flag = def.llatitude >= 0. ? 0 : 1;
  if (gatherxml::verbose_operation) {
    cout << "...confirmed a non-centered Lambert-Conformal projection." <<
        endl;
  }
  return true;
}

bool grid_is_lambert_conformal(const GridData& grid_data,
    Grid::GridDimensions& dim, Grid::GridDefinition& def) {
  if (gatherxml::verbose_operation) {
    cout << "...checking grid for a Lambert-Conformal projection..." <<
        endl;
  }
  if (grid_is_centered_lambert_conformal(grid_data, dim, def)) {
    return true;
  }
  if (grid_is_non_centered_lambert_conformal(grid_data.lat.data_array,
      grid_data.lon.data_array, dim, def)) {
    return true;
  }
  if (gatherxml::verbose_operation) {
    cout << "...check for a Lambert-Conformal projection finished. Not a "
        "Lambert-Conformal projection." << endl;
  }
  return false;
}

void add_new_grid(const GridData& gd, CoordinateVariables& cv, size_t nlev,
    size_t lidx, ScanData& sd, ParameterData& pd, string pkey, string gkey) {
  g_grml_data->g.entry.level_table.clear();
  g_grml_data->l.entry.parameter_code_table.clear();
  g_grml_data->p.entry.num_time_steps = 0;
  added_gridded_parameters_to_netcdf_level_entry(g_grml_data->g.key, gd, sd, 
      pd);
  if (!g_grml_data->l.entry.parameter_code_table.empty()) {
    for (size_t l = 0; l < nlev; ++l) {
      g_grml_data->l.key = "ds" + g_dsid + "," + gd.level.id + ":";
      if (gd.level.id == "sfc") {
        g_grml_data->l.key += "0";
      } else if (gd.level_bounds.ds == nullptr) {
        auto v = (gd.level.ds == nullptr) ? 0. : data_array_value(gd.level.
            data_array, l, gd.level.ds.get());
        if (myequalf(v, static_cast<int>(v), 0.001)) {
          g_grml_data->l.key += itos(v);
        } else {
          g_grml_data->l.key += ftos(v, 3);
        }
      } else {
        auto v = data_array_value(
            gd.time_bounds.data_array, l * 2, gd.level_bounds.ds.get());
        if (myequalf(v, static_cast<int>(v), 0.001)) {
          g_grml_data->l.key += itos(v);
        } else {
          g_grml_data->l.key += ftos(v, 3);
        }
        v = data_array_value(gd.time_bounds.data_array, l * 2 + 1, gd.
            level_bounds.ds.get());
        g_grml_data->l.key += ":";
        if (myequalf(v, static_cast<int>(v), 0.001)) {
          g_grml_data->l.key += itos(v);
        } else {
          g_grml_data->l.key += ftos(v, 3);
        }
      }
      g_grml_data->g.entry.level_table.emplace(g_grml_data->l.key, g_grml_data->
          l.entry);
      cv.level_info[lidx].write = 1;
      if (g_inv.stream.is_open()) {
        update_inventory(pkey, gkey, gd);
      }
    }
  }
  if (!g_grml_data->g.entry.level_table.empty()) {
    g_grml_data->gtb.emplace(g_grml_data->g.key, g_grml_data->g.entry);
  }
}

void update_existing_grid(const GridData& gd, CoordinateVariables& cv, size_t
    nlev, size_t lidx, ScanData& sd, ParameterData& pd, string pkey, string
    gkey) {
  bool b = false;
  for (size_t l = 0; l < nlev; ++l) {
    g_grml_data->l.key = "ds" + g_dsid + "," + gd.level.id + ":";
    auto v = gd.level.ds == nullptr ? 0. : data_array_value(gd.level.data_array,
        l, gd.level.ds.get());
    if (gd.level.id == "sfc") {
      g_grml_data->l.key += "0";
    } else if (myequalf(v, static_cast<int>(v), 0.001)) {
      g_grml_data->l.key += itos(v);
    } else {
      g_grml_data->l.key += ftos(v, 3);
    }
    if (g_grml_data->g.entry.level_table.find(g_grml_data->l.key) ==
        g_grml_data->g.entry.level_table.end()) {
      g_grml_data->l.entry.parameter_code_table.clear();
      if (added_gridded_parameters_to_netcdf_level_entry(g_grml_data->g.key, gd,
          sd, pd)) {
        b = true;
      }
      if (!g_grml_data->l.entry.parameter_code_table.empty()) {
        g_grml_data->g.entry.level_table.emplace(g_grml_data->l.key,
            g_grml_data->l.entry);
        cv.level_info[lidx].write = 1;
      }
    } else {
       update_level_entry(gd, sd, pd, cv.level_info[lidx].write);
    }
    if (cv.level_info[lidx].write == 1 && g_inv.stream.is_open()) {
      update_inventory(pkey, gkey, gd);
    }
  }
  if (b) {
    g_grml_data->gtb[g_grml_data->g.key] = g_grml_data->g.entry;
  }
}

void scan_gridded_hdf5nc4_file(ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function scan_gridded_hdf5nc4_file()..." <<
        endl;
  }
  auto is = sget_hdf5();
  if (g_grml_data == nullptr) {
    g_grml_data.reset(new GrMLData);
  }

  // open a file inventory unless this is a test run
  if (g_dsid < "999.0") {
    gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
        "hdf2xml", USER);
  }
  auto f = "https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds" + g_dsid +
      ".xml";
  scan_data.map_name = unixutils::remote_web_file(f, scan_data.tdir->name());
  ParameterData parameter_data;
  if (!scan_data.map_name.empty()) {

    // rename the parameter map so that it is not overwritten by the level map,
    //     which has the same name
    stringstream oss, ess;
    mysystem2("/bin/mv " + scan_data.map_name + " " + scan_data.map_name + ".p",
        oss, ess);
    if (!ess.str().empty()) {
      log_error2("unable to rename parameter map; error - '" + ess.str() + "'",
          F, g_util_ident);
    }
    scan_data.map_name += ".p";
    if (parameter_data.map.fill(scan_data.map_name)) {
      scan_data.found_map = true;
    }
  }
  CoordinateVariables coord_vars;
  GridData grid_data;
  find_coordinate_variables(coord_vars, grid_data);

  // if the reference and valid times are different, look for forecasts
  if (grid_data.reference_time.id != grid_data.valid_time.id) {
    check_for_forecasts(grid_data, coord_vars.nc_time);
  }

  // if no lat/lon coordinate variables found, look for alternates
  if (coord_vars.lat_ids.empty() && coord_vars.lon_ids.empty()) {
    find_alternate_lat_lon_coordinates(grid_data, coord_vars.lat_ids,
      coord_vars.lon_ids);
  }
  if (coord_vars.lat_ids.size() != coord_vars.lon_ids.size()) {
    cerr << "Terminating - unequal number of latitude and longitude coordinate "
        "variables" << endl;
    exit(1);
  }
  if (coord_vars.level_info.empty()) {
    find_vertical_level_coordinates(coord_vars, grid_data);
  }
  add_surface_level(coord_vars);
  if (grid_data.valid_time.id.empty()) {
    cerr << "Terminating - no time coordinate found" << endl;
    exit(1);
  }
  if (gatherxml::verbose_operation) {
    cout << "...found time ('" << grid_data.valid_time.id << "'), latitude ('"
        << vector_to_string(coord_vars.lat_ids) << "'), and longitude ('" <<
        vector_to_string(coord_vars.lon_ids) << "') coordinates..." << endl;
  }
  grid_data.valid_time.ds = is->dataset("/" + grid_data.valid_time.id);
  if (grid_data.valid_time.ds == nullptr) {
    log_error2("unable to access the /" + grid_data.valid_time.id + " dataset "
        "for the data temporal range", F, g_util_ident);
  }
  if (!grid_data.valid_time.data_array.fill(*is, *grid_data.valid_time.
      ds)) {
    auto error = move(myerror);
    log_error2("unable to fill time array - error: '" + error + "'", F,
        g_util_ident);
  }
  process_time_bounds(*coord_vars.nc_time, grid_data);
  if (grid_data.time_range_entries.empty()) {
    grid_data.time_range_entries.emplace(0., TimeRangeEntry2());
    auto &e = grid_data.time_range_entries[0.];
    e.key = -1;
    string err;
    e.instantaneous.first_valid_datetime = actual_date_time(grid_data.
        valid_time.data_array.value(0), *coord_vars.nc_time, err);
    if (!err.empty()) {
      log_error2(err, F, g_util_ident);
    }
    e.num_steps = grid_data.valid_time.data_array.num_values;
    e.instantaneous.last_valid_datetime = actual_date_time(grid_data.valid_time.
        data_array.value(e.num_steps-1), *coord_vars.nc_time, err);
    if (!err.empty()) {
      log_error2(err, F, g_util_ident);
    }
  }
  get_forecast_data(grid_data);
  if (coord_vars.nc_time->units == "months") {
    set_month_end_date(grid_data, coord_vars.nc_time->calendar);
  }
  if (grid_data.forecast_period.data_array.num_values > 0) {
    if (grid_data.time_range_entries.size() == 1 && grid_data.
        time_range_entries.find(0.) != grid_data.time_range_entries.end() &&
        static_cast<int>(grid_data.time_range_entries[0.].key) == -1) {
      auto tre2 = grid_data.time_range_entries[0.];
      grid_data.time_range_entries.clear();
      for (size_t n = 0; n < grid_data.forecast_period.data_array.num_values;
          ++n) {
        int fcst = grid_data.forecast_period.data_array.value(n);
        double key;
        if (fcst == 0) {
          key = -1.;
        } else {
          key = -(fcst * 100);
        }
        grid_data.time_range_entries.emplace(key, TimeRangeEntry2());
        auto& e = grid_data.time_range_entries[key];
        e.key = key;
        e.instantaneous.first_valid_datetime = tre2.instantaneous.
            first_valid_datetime.added(coord_vars.forecast_period->units, fcst);
        e.instantaneous.last_valid_datetime = tre2.instantaneous.
            last_valid_datetime.added(coord_vars.forecast_period->units, fcst);
        e.num_steps = tre2.num_steps;
      }
    } else {
      if (grid_data.time_range_entries.size() > 1) {
        log_error2("forecast data from forecast period values not implemented "
            "for multiple time ranges", F, g_util_ident);
      } else if (grid_data.time_range_entries.find(0.) == grid_data.
          time_range_entries.end()) {
        log_error2("can't find time range entry for forecast period values", F,
            g_util_ident);
      } else if (static_cast<int>(grid_data.time_range_entries[0.].key) != -1) {
        log_error2("bad time range entry key for forecast period values", F,
            g_util_ident);
      }
    }
  }
  for (size_t n = 0; n < coord_vars.lat_ids.size(); ++n) {
    if (gatherxml::verbose_operation) {
      cout << "...checking projection for '" << coord_vars.lat_ids[n] <<
          "' and '" << coord_vars.lon_ids[n] << "' ..." << endl;
    }
    grid_data.lat.id = coord_vars.lat_ids[n];
    grid_data.lat.ds = is->dataset("/" + coord_vars.lat_ids[n]);
    if (grid_data.lat.ds == nullptr) {
      log_error2("unable to access the /" + coord_vars.lat_ids[n] + " dataset "
          "for the latitudes", F, g_util_ident);
    }
    grid_data.lat.data_array.fill(*is, *grid_data.lat.ds);
    Grid::GridDefinition def;
    def.slatitude = data_array_value(grid_data.lat.data_array, 0,
        grid_data.lat.ds.get());
    grid_data.lon.id = coord_vars.lon_ids[n];
    grid_data.lon.ds = is->dataset("/" + coord_vars.lon_ids[n]);
    if (grid_data.lon.ds == nullptr) {
      log_error2("unable to access the /" + coord_vars.lon_ids[n] + " dataset "
          "for the longitudes", F, g_util_ident);
    }
    grid_data.lon.data_array.fill(*is, *grid_data.lon.ds);
    def.slongitude = data_array_value(grid_data.lon.data_array, 0,
        grid_data.lon.ds.get());
    DatasetPointer lat_bounds_ds(nullptr), lon_bounds_ds(nullptr);
    HDF5::DataArray lat_bounds_array, lon_bounds_array;
    auto lat_bounds_it = grid_data.lat.ds->attributes.find("bounds");
    auto lon_bounds_it = grid_data.lon.ds->attributes.find("bounds");
    if (lat_bounds_it != grid_data.lat.ds->attributes.end() &&
        lon_bounds_it != grid_data.lon.ds->attributes.end() &&
        lat_bounds_it->second._class_ == 3 && lon_bounds_it->second._class_ ==
        3) {
      if ( (lat_bounds_ds = is->dataset("/" + string(
          reinterpret_cast<char *>(lat_bounds_it->second.array)))) !=
          nullptr && (lon_bounds_ds = is->dataset("/" + string(
          reinterpret_cast<char *>(lon_bounds_it->second.array)))) !=
          nullptr) {
        lat_bounds_array.fill(*is, *lat_bounds_ds);
        lon_bounds_array.fill(*is, *lon_bounds_ds);
      }
    }
    Grid::GridDimensions dim;
    auto lat_attr_it = grid_data.lat.ds->attributes.find("DIMENSION_LIST");
    auto& lat_dim_list = lat_attr_it->second;
    auto lon_attr_it = grid_data.lon.ds->attributes.find(
        "DIMENSION_LIST");
    auto& lon_dim_list = lon_attr_it->second;
    if (lat_attr_it != grid_data.lat.ds->attributes.end() && lon_attr_it !=
        grid_data.lon.ds->attributes.end() && lat_dim_list.dim_sizes.
        size() == 1 && lat_dim_list.dim_sizes[0] == 2 && lat_dim_list._class_ ==
        9 && lon_dim_list.dim_sizes.size() == 1 && lon_dim_list.dim_sizes[0] ==
        2 && lon_dim_list._class_ == 9) {

      // latitude and longitude variables have one dimension of size 2
      if (lat_dim_list.vlen.class_ == 7 && lon_dim_list.vlen.class_ == 7) {

        // dimensions are variable length values of references to netCDF
        //   dimensions
        unordered_map<size_t, string>::iterator rtp_it[4];
        rtp_it[0] = is->reference_table_pointer()->find(HDF5::value(
            &lat_dim_list.vlen.buffer[4], lat_dim_list.precision_));
        rtp_it[1] = is->reference_table_pointer()->find(HDF5::value(
            &lon_dim_list.vlen.buffer[4], lon_dim_list.precision_));
        rtp_it[2] = is->reference_table_pointer()->find(HDF5::value(
            &lat_dim_list.vlen.buffer[8 + lat_dim_list.precision_],
            lat_dim_list.precision_));
        rtp_it[3] = is->reference_table_pointer()->find(HDF5::value(
            &lon_dim_list.vlen.buffer[8 + lon_dim_list.precision_],
            lon_dim_list.precision_));
        const auto& rtp_end = is->reference_table_pointer()->end();
        if (rtp_it[0] != rtp_end && rtp_it[1] != rtp_end && rtp_it[0]->second ==
            rtp_it[1]->second && rtp_it[2] != rtp_end && rtp_it[3] != rtp_end &&
            rtp_it[2]->second == rtp_it[3]->second) {
          auto ds = is->dataset("/" + rtp_it[0]->second);
          auto attr_it = ds->attributes.find("NAME");
          if (ds == nullptr || attr_it == ds->attributes.end() || attr_it->
              second._class_ != 3) {
            log_error2("(1)unable to determine grid definition from '" +
                coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
                F, g_util_ident);
          }
          auto attr_parts = split(string(reinterpret_cast<char *>(attr_it->
              second.array)));
          if (attr_parts.size() == 11) {

            // netCDF dimension - the convention is:
            // "This is a netCDF dimension but not a netCDF variable.xxxxxxxxxx"
            //     where xxxxxxxxxx is a right-justified integer of width 10
            dim.y = std::stoi(attr_parts[10]);
          } else {
            log_error2("(2)unable to determine grid definition from '" +
                coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
                F, g_util_ident);
          }
          ds = is->dataset("/" + rtp_it[2]->second);
          attr_it = ds->attributes.find("NAME");
          if (ds == nullptr || attr_it == ds->attributes.end() || attr_it->
              second._class_ != 3) {
            log_error2("(3)unable to determine grid definition from '" +
                coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
                F, g_util_ident);
          }
          attr_parts = split(string(reinterpret_cast<char *>(attr_it->
              second.array)));
          if (attr_parts.size() == 11) {

            // netCDF dimension
            // "This is a netCDF dimension but not a netCDF variable.xxxxxxxxxx"
            //     where xxxxxxxxxx is a right-justified integer of width 10
            dim.x = std::stoi(attr_parts[10]);
          } else {
            log_error2("(4)unable to determine grid definition from '" +
                coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
                F, g_util_ident);
          }
        } else {
          log_error2("(5)unable to determine grid definition from '" +
              coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
              F, g_util_ident);
        }
        auto determined_grid_type = grid_is_polar_stereographic(grid_data, dim,
            def);
        if (!determined_grid_type) {
          determined_grid_type = grid_is_lambert_conformal(grid_data, dim, def);
        }
        if (!determined_grid_type ) {
          log_error2("(6)unable to determine grid definition from '" +
              coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'",
              F, g_util_ident);
        }
      } else {
        log_error2("(7)unable to determine grid definition from '" +
            coord_vars.lat_ids[n] + "' and '" + coord_vars.lon_ids[n] + "'", F,
            g_util_ident);
      }
    } else {
      auto it = grid_data.lat.ds->attributes.find("DIMENSION_LIST");
      if (it == grid_data.lat.ds->attributes.end()) {
        dim.y = grid_data.lat.data_array.num_values;
        dim.x = grid_data.lon.data_array.num_values;
      }
      else {
        stringstream ss;
        it->second.print(ss, is->reference_table_pointer());
        auto sp = split(ss.str().substr(1, ss.str().length() - 2));
        switch (sp.size()) {
          case 1: {
            dim.y = grid_data.lat.data_array.num_values;
            dim.x = grid_data.lon.data_array.num_values;
            break;
          }
          case 2: {
            dim.y = grid_data.lat.data_array.dimensions[0];
            dim.x = grid_data.lat.data_array.dimensions[1];
            break;
          }
          default: {
            cerr << "Terminating - latitude and longitude coordinates must be "
                "one- or two-dimensional" << endl;
            exit(1);
          }
        }
      }
      def.type = Grid::Type::latitudeLongitude;
      def.elatitude = data_array_value(grid_data.lat.data_array, grid_data.
          lat.data_array.num_values - 1, grid_data.lat.ds.get());
      def.elongitude = data_array_value(grid_data.lon.data_array,
          grid_data.lon.data_array.num_values - 1, grid_data.lon.ds.
          get());
      def.laincrement = fabs((def.elatitude - def.slatitude)/(dim.y - 1));
      def.loincrement = fabs((def.elongitude - def.slongitude)/(dim.x - 1));
      if (lat_bounds_array.num_values > 0) {
        def.slatitude = data_array_value(lat_bounds_array, 0,
            lat_bounds_ds.get());
        def.slongitude = data_array_value(lon_bounds_array, 0,
            lon_bounds_ds.get());
        def.elatitude = data_array_value(lat_bounds_array,
            lat_bounds_array.num_values - 1, lat_bounds_ds.get());
        def.elongitude = data_array_value(lon_bounds_array,
            lon_bounds_array.num_values - 1, lon_bounds_ds.get());
        def.is_cell = true;
      }
    }
    if (gatherxml::verbose_operation) {
      cout << "...grid was identified as type " << static_cast<int>(def.type) <<
          "..." << endl;
    }
    unordered_set<string> grid_entry_set;
    for (const auto& e : grid_data.time_range_entries) {
      if (gatherxml::verbose_operation) {
        cout << "...processing time range entry: " << e.first << "/" <<
            static_cast<int>(e.second.key) << " ..." << endl;
      }
      grid_data.time_data = grid_data.forecast_period.id.empty() ?  coord_vars.
          nc_time : coord_vars.forecast_period;
      add_gridded_lat_lon_keys(grid_entry_set, dim, def, grid_data);
      for (size_t m = 0; m < coord_vars.level_info.size(); ++m) {
        if (gatherxml::verbose_operation) {
          cout << "...processing vertical level entry: " << coord_vars.
          level_info[m].ID << " ..." << endl;
        }
        grid_data.level.id = coord_vars.level_info[m].ID;
        size_t num_levels;
        if (m == (coord_vars.level_info.size() - 1) && grid_data.level.id ==
            "sfc") {
          num_levels = 1;
        } else {
          grid_data.level.ds = is->dataset("/" + grid_data.level.id);
          if (grid_data.level.ds == nullptr) {
            log_error2("unable to access the /" + grid_data.level.id +
                " dataset for level information", F, g_util_ident);
          }
          grid_data.level.data_array.fill(*is, *grid_data.level.ds);
          num_levels = grid_data.level.data_array.num_values;
          auto attr_it = grid_data.level.ds->attributes.find("bounds");
          if (attr_it != grid_data.level.ds->attributes.end() && attr_it->
              second._class_ == 3) {
            string attr_value = reinterpret_cast<char *>(attr_it->second.array);
            grid_data.level_bounds.ds = is->dataset("/" + attr_value);
            if (grid_data.level_bounds.ds == nullptr) {
              log_error2("unable to get bounds for level '" + grid_data.level.id
                  + "'", F, g_util_ident);
            }
            grid_data.time_bounds.data_array.fill(*is, *grid_data.
                level_bounds.ds);
          }
        }
        for (const auto& key : grid_entry_set) {
          if (gatherxml::verbose_operation) {
            cout << "...processing grid entry: " << key << " ..." << endl;
          }
          g_grml_data->g.key = key;
          auto sp = split(g_grml_data->g.key, "<!>");
          auto& product_key = sp.back();
          string grid_key;
          if (g_inv.stream.is_open()) {
            grid_key = sp[0];
            for (size_t nn = 1; nn < sp.size() - 1; ++nn) {
              grid_key += "," + sp[nn];
            }
          }
          if (g_grml_data->gtb.find(g_grml_data->g.key) == g_grml_data->gtb.
              end()) {
            add_new_grid(grid_data, coord_vars, num_levels, m, scan_data,
                parameter_data, product_key, grid_key);
           } else {
            update_existing_grid(grid_data, coord_vars, num_levels, m,
                scan_data, parameter_data, product_key, grid_key);
          }
          if (gatherxml::verbose_operation) {
            cout << "...grid entry: " << key << " done." << endl;
          }
        }
        if (gatherxml::verbose_operation) {
          cout << "...vertical level entry: " << coord_vars.level_info[m].ID <<
              " done." << endl;
        }
      }
      if (gatherxml::verbose_operation) {
        cout << "...time range entry: " << e.first << "/" << static_cast<int>(e.
            second.key) << " done." << endl;
      }
    }
    auto e = metautils::NcLevel::write_level_map(coord_vars.level_info);
    if (!e.empty()) {
      log_error2(e, F, g_util_ident);
    }
  }
  scan_data.write_type = ScanData::GrML_type;
  if (g_grml_data->gtb.empty()) {
    if (!grid_data.valid_time.id.empty()) {
      cerr << "Terminating - no grids found and no content metadata will be "
          "generated" << endl;
    } else {
      cerr << "Terminating - no time coordinate could be identified and no "
          "content metadata will be generated" << endl;
    }
    exit(1);
  }
if (g_inv.stream.is_open()) {
g_inv.R.map.emplace("x", make_pair(0, 0));
g_inv.R.lst.emplace_back("x");
}
  if (gatherxml::verbose_operation) {
    cout << "...function scan_gridded_hdf5nc4_file() done." << endl;
  }
}

string feature_type(AttributeMap& attributes) {
  string s; // return value
  auto it = attributes.find("featureType");
  if (it != attributes.end()) {
    s = reinterpret_cast<char *>(it->second.array);
  }
  return s;
}

void patch_icoads_netcdf4_ids(AttributeMap& attributes, ScanData& sd) {
  auto it = attributes.find("product_version");
  if (it != attributes.end()) {
    string s = reinterpret_cast<char *>(it->second.array);
    if (s.find("ICOADS") != string::npos && s.find("netCDF4") != string::npos) {
      sd.convert_ids_to_upper_case = true;
    }
  }
}

void scan_hdf5nc4_file(ScanData& scan_data, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto is = sget_hdf5();
  auto ds = is->dataset("/");
  if (ds == nullptr) {
    log_error2("unable to access global attributes", F, g_util_ident);
  }
  scan_data.platform_type="unknown";
  auto attr_it = ds->attributes.find("platform");
  if (attr_it != ds->attributes.end()) {
    string platform = reinterpret_cast<char *>(attr_it->second.array);
    if (!platform.empty()) {
      trim(platform);
      Server server(metautils::directives.database_server, metautils::
          directives.metadb_username, metautils::directives.metadb_password,
          "");
      if (server) {
        LocalQuery query("obml_platform_type", "search.gcmd_platforms", "path "
            "= '" + platform + "'");
        if (query.submit(server) == 0) {
          Row row;
          if (query.fetch_row(row)) {
            scan_data.platform_type = row[0];
          }
        }
      }
    }
  }
  auto ftype = feature_type(ds->attributes);
  if (gatherxml::verbose_operation) {
    cout << "Feature type is '" << ftype << "'." << endl;
  }
  if (ftype.empty()) {
    scan_gridded_hdf5nc4_file(scan_data);
  } else {
    auto l_ftype = to_lower(ftype);

    // patch for ICOADS netCDF4 IDs, which may be a mix, so ignore case
    patch_icoads_netcdf4_ids(ds->attributes, scan_data);
    if (l_ftype == "point") {
      scan_cf_point_hdf5nc4_file(scan_data, obs_data);
    } else if (l_ftype == "timeseries") {
      scan_cf_time_series_hdf5nc4_file(scan_data, obs_data);
    } else if (l_ftype == "profile") {
      scan_cf_profile_hdf5nc4_file(scan_data, obs_data);
    } else {
      log_error2("featureType '" + ftype + "' not recognized", F, g_util_ident);
    }
  }
}

void scan_hdf5_file(std::list<string>& filelist, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "Beginning HDF5 file scan..." << endl;
  }
  gatherxml::markup::ObML::ObservationData obs_data;
  stream_set(ISTREAM_TYPE::_HDF5);
  auto is = sget_hdf5();
  for (const auto& file : filelist) {
    if (gatherxml::verbose_operation) {
      cout << "Scanning file: " << file << endl;
    }
    if (!is->open(file.c_str())) {
      auto error = move(myerror);
      log_error2(error += " - file: '" + file + "'", F, g_util_ident);
    }
    if (metautils::args.data_format == "ispdhdf5") {
      scan_ispd_hdf5_file(scan_data, obs_data);
    } else if (metautils::args.data_format == "hdf5nc4") {
      scan_hdf5nc4_file(scan_data, obs_data);
    } else if (metautils::args.data_format == "usarrthdf5") {
      scan_usarray_transportable_hdf5_file(scan_data, obs_data);
    } else {
      cerr << "Error: bad data format specified" << endl;
      exit(1);
    }
    is->close();
  }
  if (scan_data.write_type == ScanData::GrML_type) {
    scan_data.cmd_type = "GrML";
    if (!metautils::args.inventory_only) {
      xml_directory = gatherxml::markup::GrML::write(g_grml_data->gtb,
          "hdf2xml", USER);
    }
  } else if (scan_data.write_type == ScanData::ObML_type) {
    if (scan_data.num_not_missing > 0) {
      if (metautils::args.data_format != "hdf5nc4") {
        metautils::args.data_format = "hdf5";
      }
      scan_data.cmd_type = "ObML";
      gatherxml::markup::ObML::write(obs_data, "hdf2xml", USER);
    } else {
      log_error2("all stations have missing location information - no usable "
          "data found; no content metadata will be saved for this file", F,
          g_util_ident);
    }
  }
  string map_type;
  if (scan_data.write_type == ScanData::GrML_type) {
    map_type = "parameter";
  } else if (scan_data.write_type == ScanData::ObML_type) {
    map_type = "dataType";
  } else {
    log_error2("unknown map type", F, g_util_ident);
  }
  string warning;
  auto error = metautils::NcParameter::write_parameter_map(scan_data.varlist,
      scan_data.var_changes_table, map_type, scan_data.map_name,
      scan_data.found_map, warning);
  if (!error.empty()) log_error2(error, F, g_util_ident);
  if (gatherxml::verbose_operation) {
    cout << "HDF5 file scan completed." << endl;
  }
}

void scan_file(ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "Beginning file scan..." << endl;
  }
  g_work_file.reset(new TempFile);
  if (!g_work_file->open(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary file in " +
        metautils::directives.temp_path, F, g_util_ident);
  }
  unique_ptr<TempDir> work_dir;
  work_dir.reset(new TempDir);
  if (!work_dir->create(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary directory in " +
        metautils::directives.temp_path, F, g_util_ident);
  }
  string file_format, error;
  std::list<string> filelist;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(
      *g_work_file, *work_dir, &filelist, file_format, error)) {
    log_error2(error + "'", F + ": "
        "prepare_file_for_metadata_scanning()", g_util_ident);
  }
  if (filelist.empty()) filelist.emplace_back(g_work_file->name());
  scan_data.tdir.reset(new TempDir);
  if (!scan_data.tdir->create(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary directory in " +
        metautils::directives.temp_path, F, g_util_ident);
  }
  if (metautils::args.data_format.find("hdf4") != string::npos) {
    scan_hdf4_file(filelist, scan_data);
  } else if (metautils::args.data_format.find("hdf5") != string::npos) {
    scan_hdf5_file(filelist, scan_data);
  } else {
    cerr << "Error: bad data format specified" << endl;
    exit(1);
  }
  if (gatherxml::verbose_operation) cout << "File scan complete." << endl;
}

void show_usage_and_exit() {
  cerr << "usage: hdf2xml -f format -d [ds]nnn.n [options...] <path>" << endl;
  cerr << "\nrequired (choose one):" << endl;
  cerr << "HDF4 formats:" << endl;
  cerr << "-f quikscathdf4   NASA QuikSCAT HDF4" << endl;
  cerr << "HDF5 formats:" << endl;
  cerr << "-f ispdhdf5       NOAA International Surface Pressure Databank HDF5"
      << endl;
  cerr << "-f hdf5nc4        NetCDF4 with HDF5 storage" << endl;
  cerr << "-f usarrthdf5     EarthScope USArray Transportable Array Pressure "
      "Observations" << endl;
  cerr << "\nrequired:" << endl;
  cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file "
      "belongs" << endl;
  cerr << "\noptions" << endl;
  cerr << "-V               verbose operation" << endl;
  cerr << "\nrequired:" << endl;
  cerr << "<path>           full HPSS path or URL of the file to read" << endl;
  cerr << "                 - HPSS paths must begin with \"/FS/DECS\"" << endl;
  cerr << "                 - URLs must begin with \"http://rda.ucar.edu\"" <<
      endl;
  exit(1);
}

string scm_flags() {
  string s; // return value
  if (!metautils::args.update_summary) {
    s += " -S ";
  }
  if (!metautils::args.regenerate) {
    s += " -R ";
  }
  if (!xml_directory.empty()) {
    s += " -t " + xml_directory;
  }
  if (metautils::args.path.find("https://rda.ucar.edu") == 0) {
    s += " -wf";
  } else {
    s += " -f";
  }
  return s;
}

void print_output_location(int write_type) {
  cout << "Output is in: " << xml_directory << "/" << metautils::args.filename
      << ".";
  switch (write_type) {
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

void write_map(string prefix, const Inventory::Map& map) {
  for (const auto& key : map.lst) {
    auto& p = map.map.at(key);
    if (p.second > 0) {
      g_inv.stream << prefix << "<!>" << p.first << "<!>" << key << endl;
    }
  }
}

void write_inventory() {
  if (!g_inv.stream.is_open()) {
    return;
  }
  write_map("U", g_inv.U);
  write_map("G", g_inv.G);
  write_map("L", g_inv.L);
  write_map("P", g_inv.P);
  write_map("R", g_inv.R);
  g_inv.stream << "-----" << endl;
  for (const auto& line : g_inv.lines) {
    g_inv.stream << line << endl;
  }
  gatherxml::fileInventory::close(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
      true, true, "hdf2xml", USER);
}

int main(int argc, char **argv) {
  if (argc < 6) {
    show_usage_and_exit();
  }
  const string F = this_function_label(__func__);
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  auto d = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, d);
  metautils::read_config("hdf2xml", USER);
  gatherxml::parse_args(d);
  atexit(clean_up);
  metautils::cmd_register("hdf2xml", USER);
  if (!metautils::args.overwrite_only) {
    metautils::check_for_existing_cmd("GrML");
    metautils::check_for_existing_cmd("ObML");
  }
  Timer tmr;
  tmr.start();
  ScanData scan_data;
  scan_file(scan_data);
  if (!metautils::args.inventory_only) {
    if (metautils::args.update_db) {
      if (scan_data.cmd_type.empty()) {
        log_error2("content metadata type was not specified", F, g_util_ident);
      }
      string scm = metautils::directives.local_root + "/bin/scm -d " + g_dsid +
          " " + scm_flags() + " " + metautils::args.filename + "." +
          scan_data.cmd_type;
      stringstream oss, ess;
      if (mysystem2(scm, oss, ess) < 0) {
        cerr << ess.str() << endl;
      }
    } else if (g_dsid == "test" && !xml_directory.empty()) {
      print_output_location(scan_data.write_type);
    }
  }
  write_inventory();
  tmr.stop();
  metautils::log_warning("execution time: " + ftos(tmr.elapsed_time()) +
      " seconds", "gatherxml.time", USER);
  return 0;
}
