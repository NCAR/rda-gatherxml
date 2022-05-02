#include <iostream>
#include <sstream>
#include <memory>
#include <deque>
#include <regex>
#include <unordered_set>
#include <sstream>
#include <numeric>
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

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::move;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ftos;
using strutils::itos;
using strutils::split;
using strutils::trim;
using strutils::vector_to_string;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";
stringstream g_warn_ss;

const metautils::UtilityIdentification g_util_ident("hdf2xml", USER);
unique_ptr<TempFile> work_file;

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
  CoordinateVariables() : nc_time(new metautils::NcTime::TimeData),
    forecast_period(new metautils::NcTime::TimeData), lat_ids(), lon_ids(),
    level_info()  {}

  std::shared_ptr<metautils::NcTime::TimeData> nc_time;
  std::shared_ptr<metautils::NcTime::TimeData> forecast_period;
  vector<string> lat_ids, lon_ids;
  vector<metautils::NcLevel::LevelInfo> level_info;
};

struct GridData {
  struct CoordinateData {
    CoordinateData() : id(), ds(nullptr), data_array() {}

    string id;
    std::shared_ptr<InputHDF5Stream::Dataset> ds;
    HDF5::DataArray data_array;
  };
  GridData() : time_range_entry(), time_data(nullptr), reference_time(),
      valid_time(), time_bounds(), climo_bounds(), forecast_period(),
      latitude(), longitude(), level(), level_bounds(),
      coordinate_variables_set() {}

  metautils::NcTime::TimeRangeEntry time_range_entry;
  std::shared_ptr<metautils::NcTime::TimeData> time_data;
  CoordinateData reference_time, valid_time, time_bounds, climo_bounds,
      forecast_period, latitude, longitude, level, level_bounds;
  std::unordered_set<string> coordinate_variables_set;
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
  struct Maps {
    Maps() : U(), G(), L(), P(), R() { }

    unordered_map<string, size_t> U, G, L, P, R;
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
      missing_value() {}

  string long_name, units, cf_keyword;
  InputHDF5Stream::DataValue missing_value;
};

struct ParameterData {
  ParameterData() : set(), map() {}

  std::unordered_set<string> set;
  ParameterMap map;
};

unique_ptr<unordered_map<string, gatherxml::markup::GrML::GridEntry>>
    grid_table_ptr;
string grid_entry_key;
unique_ptr<gatherxml::markup::GrML::GridEntry> grid_entry_ptr;
string level_entry_key;
unique_ptr<gatherxml::markup::GrML::LevelEntry> level_entry_ptr;
string parameter_entry_key;
unique_ptr<gatherxml::markup::GrML::ParameterEntry> parameter_entry_ptr;
std::unordered_set<string> unique_data_type_observation_set;
gatherxml::markup::ObML::DataTypeEntry de;
string xml_directory;

extern "C" void clean_up() {
  if (metautils::args.dsnum < "999.0") {
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
  if (metautils::args.dsnum < "999.0") {
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

void grid_initialize() {
  if (grid_table_ptr == nullptr) {
    grid_table_ptr.reset(new unordered_map<string, gatherxml::markup::GrML::
        GridEntry>);
    grid_entry_ptr.reset(new gatherxml::markup::GrML::GridEntry);
    level_entry_ptr.reset(new gatherxml::markup::GrML::LevelEntry);
    parameter_entry_ptr.reset(new gatherxml::markup::GrML::ParameterEntry);
  }
}

void scan_quikscat_hdf4_file(InputHDF4Stream& istream) {
istream.print_data_descriptors(1965);
}

void scan_hdf4_file(std::list<string>& filelist, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  InputHDF4Stream istream;

  for (const auto& file : filelist) {
    if (!istream.open(file.c_str())) {
      auto error = move(myerror);
       log_error2(error + " - file: '" + file + "'", F, g_util_ident);
    }
    if (metautils::args.data_format == "quikscathdf4") {
       scan_quikscat_hdf4_file(istream);
    }
    istream.close();
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
      nc_attribute_data.long_name.assign(
         reinterpret_cast<char *>(attribute.second.array));
     trim(nc_attribute_data.long_name);
    } else if (attribute.first == "units") {
      nc_attribute_data.units.assign(
         reinterpret_cast<char *>(attribute.second.array));
     trim(nc_attribute_data.units);
    } else if (attribute.first == "standard_name") {
      nc_attribute_data.cf_keyword.assign(
         reinterpret_cast<char *>(attribute.second.array));
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
         if (floatutils::myequalf(time,
             *(reinterpret_cast<unsigned char *>(time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::SHORT): {
         if (floatutils::myequalf(time,
             *(reinterpret_cast<short *>(time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::INT): {
         if (floatutils::myequalf(time,
             *(reinterpret_cast<int *>(time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::LONG_LONG): {
         if (floatutils::myequalf(time,
             *(reinterpret_cast<long long *>(time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::FLOAT): {
         if (floatutils::myequalf(time,
             *(reinterpret_cast<float *>(time_missing_value->array)))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::DOUBLE): {
         if (floatutils::myequalf(time,
             *(reinterpret_cast<double *>(time_missing_value->array)))) {
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
  if (missing) {
    return true;
  }
  if (var_missing_value.size > 0) {
    switch (var_vals.type) {
       case (HDF5::DataArray::Type::BYTE): {
         if (var_vals.byte_value(var_val_index) ==
             *(reinterpret_cast<unsigned char *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::SHORT): {
         if (var_vals.short_value(var_val_index) ==
             *(reinterpret_cast<short *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::INT): {
         if (var_vals.int_value(var_val_index) ==
             *(reinterpret_cast<int *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::LONG_LONG): {
         if (var_vals.long_long_value(var_val_index) ==
             *(reinterpret_cast<long long *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::FLOAT): {
         if (var_vals.float_value(var_val_index) ==
             *(reinterpret_cast<float *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::DOUBLE): {
         if (var_vals.double_value(var_val_index) ==
             *(reinterpret_cast<double *>(var_missing_value.array))) {
           missing = true;
         }
         break;
       }
       case (HDF5::DataArray::Type::STRING): {
         if (var_vals.string_value(var_val_index) == string(
             reinterpret_cast<char *>(var_missing_value.array),
             var_missing_value.size)) {
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
  if (plat_type == -1) {
    return "land_station";
  } else {
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
        } else {
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
          std::deque<string> sp = split(id, "-");
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
  } else if ((ispd_id == "001007" && plat_type == 1001) || ispd_id == "002000" ||
      ispd_id == "003002" || ispd_id == "003008" || ispd_id == "003015" ||
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
  } else if ((ispd_id == "003006" || ispd_id == "003030") && plat_type == 1006) {
    ientry_key = platform_type + "[!]AUSTRALIA[!]" + id;
  } else if (ispd_id == "003009" && plat_type == 1006) {
    ientry_key = platform_type + "[!]SPAIN[!]" + id;
  } else if ((ispd_id == "003010" || ispd_id == "003011") && plat_type == 1003) {
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
  } else if ((ispd_id == "003019" || ispd_id == "003100") && plat_type == 1002 &&
      !id.empty()) {
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

void scan_ispd_hdf5_file(InputHDF5Stream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto ds = istream.dataset("/ISPD_Format_Version");
  if (ds == nullptr || ds->datatype.class_ != 3) {
    log_error2("unable to determine format version", F, g_util_ident);
  }
  HDF5::DataArray da;
  da.fill(istream, *ds);
  auto format_version = da.string_value(0);
  InputHDF5Stream::DataValue ts_val, uon_val, id_val, lat_val, lon_val;

  // load the station library
  ds = istream.dataset("/Data/SpatialTemporalLocation/SpatialTemporalLocation");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate spatial/temporal information", F, g_util_ident);
  }
  unordered_map<string, std::tuple<string, string, float, float, short,
      short, char, bool>> stn_library;
  InputHDF5Stream::CompoundDatatype cpd;
  HDF5::decode_compound_datatype(ds->datatype, cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m = 0, l = 0; m < ds->data.sizes.front(); m++) {
      ts_val.set(*istream.file_stream(), &chunk.buffer[l +
          cpd.members[0].byte_offset], istream.size_of_offsets(),
          istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
      string key = reinterpret_cast<char *>(ts_val.get());
      uon_val.set(*istream.file_stream(), &chunk.buffer[l +
          cpd.members[1].byte_offset], istream.size_of_offsets(),
          istream.size_of_lengths(), cpd.members[1].datatype, ds->dataspace);
      key += string(reinterpret_cast<char *>(uon_val.get()));
      if (!key.empty() && (stn_library.find(key) == stn_library.end())) {
        id_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[2].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[2].datatype, ds->dataspace);
        string id = reinterpret_cast<char *>(id_val.get());
        trim(id);
        lat_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[13].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[13].datatype, ds->dataspace);
        auto lat = *(reinterpret_cast<float *>(lat_val.get()));
        lon_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[14].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[14].datatype, ds->dataspace);
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
  ds = istream.dataset("/SupplementalData/Tracking/ICOADS/TrackingICOADS");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue plat_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*istream.file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], istream.size_of_offsets(),
              istream.size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            id_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
                cpd.members[2].datatype, ds->dataspace);
            std::get<5>(entry->second) = *(reinterpret_cast<int *>(id_val.get()));
            plat_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[4].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
                cpd.members[4].datatype, ds->dataspace);
            std::get<4>(entry->second) = *(reinterpret_cast<int *>(plat_val.get()));
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
  ds = istream.dataset("/Data/Observations/ObservationTypes");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue coll_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*istream.file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], istream.size_of_offsets(),
              istream.size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            if (std::get<4>(entry->second) < 0) {
              id_val.set(*istream.file_stream(), &chunk.buffer[l +
                  cpd.members[2].byte_offset], istream.size_of_offsets(),
                  istream.size_of_lengths(),
                  cpd.members[2].datatype, ds->dataspace);
              std::get<4>(entry->second) = 1000 + *(reinterpret_cast<int *>(id_val.get()));
            }
            coll_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[4].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
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
  ds = istream.dataset("/SupplementalData/Tracking/Land/TrackingLand");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue src_flag_val, rpt_type_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*istream.file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], istream.size_of_offsets(),
              istream.size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            src_flag_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
                cpd.members[2].datatype, ds->dataspace);
            std::get<6>(entry->second) = (reinterpret_cast<char *>(src_flag_val.get()))[0];
            rpt_type_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[3].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
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
  ds = istream.dataset("/SupplementalData/Misc/TropicalStorms/StormID");
  if (ds != nullptr && ds->datatype.class_ == 6) {
    InputHDF5Stream::DataValue storm_id_val;
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[0].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
        string key = reinterpret_cast<char *>(ts_val.get());
        if (!key.empty()) {
          uon_val.set(*istream.file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], istream.size_of_offsets(),
              istream.size_of_lengths(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            storm_id_val.set(*istream.file_stream(), &chunk.buffer[l +
                cpd.members[2].byte_offset], istream.size_of_offsets(),
                istream.size_of_lengths(),
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
  ds = istream.dataset("/Data/Observations/Observations");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate observations", F, g_util_ident);
  }
  HDF5::decode_compound_datatype(ds->datatype, cpd);
  for (const auto& chunk : ds->data.chunks) {
    for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
      ts_val.set(*istream.file_stream(), &chunk.buffer[l +
          cpd.members[0].byte_offset], istream.size_of_offsets(),
          istream.size_of_lengths(), cpd.members[0].datatype, ds->dataspace);
      string timestamp = reinterpret_cast<char *>(ts_val.get());
      trim(timestamp);
      if (!timestamp.empty()) {
        auto key = timestamp;
        uon_val.set(*istream.file_stream(), &chunk.buffer[l +
            cpd.members[1].byte_offset], istream.size_of_offsets(),
            istream.size_of_lengths(), cpd.members[1].datatype, ds->dataspace);
        key += string(reinterpret_cast<char *>(uon_val.get()));
        auto entry = stn_library.find(key);
        if (entry != stn_library.end()) {
          if (std::regex_search(timestamp, std::regex("99$"))) {
            strutils::chop(timestamp, 2);

            // patch for some bad timestamps
            if (std::regex_search(timestamp, std::regex(" $"))) {
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
              slp_val.set(*istream.file_stream(), &chunk.buffer[l +
                  cpd.members[2].byte_offset], istream.size_of_offsets(),
                  istream.size_of_lengths(),
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
              stnp_val.set(*istream.file_stream(), &chunk.buffer[l +
                  cpd.members[5].byte_offset], istream.size_of_offsets(),
                  istream.size_of_lengths(),
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
  ds = istream.dataset("/Data/AssimilationFeedback/AssimilationFeedback");
  if (ds == nullptr) {
    ds = istream.dataset("/Data/AssimilationFeedback/AssimilationFeedBack");
  }
  if (ds != nullptr && ds->datatype.class_ == 6) {
    auto ts_regex = std::regex("99$");
//    InputHDF5Stream::DataValue p_val, ens_fg_val, ens_p_val;
    vector<InputHDF5Stream::DataValue> dv(feedback_versions[format_version].
        size());
    HDF5::decode_compound_datatype(ds->datatype, cpd);
    for (const auto& chunk : ds->data.chunks) {
      for (int m = 0, l = 0; m < ds->data.sizes.front(); ++m) {
        ts_val.set(*istream.file_stream(),
            &chunk.buffer[l + cpd.members[0].byte_offset],
            istream.size_of_offsets(), istream.size_of_lengths(),
            cpd.members[0].datatype, ds->dataspace);
        string timestamp = reinterpret_cast<char *>(ts_val.get());
        trim(timestamp);
        if (!timestamp.empty()) {
          auto key = timestamp;
          uon_val.set(*istream.file_stream(), &chunk.buffer[l +
              cpd.members[1].byte_offset], istream.size_of_offsets(),
              istream.size_of_offsets(),
              cpd.members[1].datatype, ds->dataspace);
          key += string(reinterpret_cast<char *>(uon_val.get()));
          auto entry = stn_library.find(key);
          if (entry != stn_library.end()) {
            if (!timestamp.empty()) {
              if (std::regex_search(timestamp, ts_regex)) {
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
                    dv[ndv].set(*istream.file_stream(), &chunk.buffer[l +
                        cpd.members[feedback_idx].byte_offset],
                        istream.size_of_offsets(), istream.size_of_lengths(),
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

void scan_usarray_transportable_hdf5_file(InputHDF5Stream& istream,
    ScanData& scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  obs_data.set_track_unique_observations(false);

  // load the pressure dataset
  auto ds = istream.dataset("/obsdata/presdata");
  if (ds == nullptr || ds->datatype.class_ != 6) {
    log_error2("unable to locate the pressure dataset", F, g_util_ident);
  }
  HDF5::DataArray times, stnids, pres;
  times.fill(istream, *ds, 0);
  if (times.type != HDF5::DataArray::Type::LONG_LONG) {
     log_error2("expected the timestamps to be 'long long' but got " +
         itos(static_cast<int>(times.type)), F, g_util_ident);
  }
  stnids.fill(istream, *ds, 1);
  if (stnids.type != HDF5::DataArray::Type::SHORT) {
     log_error2("expected the numeric station IDs to be 'short' but got " +
         itos(static_cast<int>(stnids.type)), F, g_util_ident);
  }
  pres.fill(istream, *ds, 2);
  if (pres.type != HDF5::DataArray::Type::FLOAT) {
     log_error2("expected the pressures to be 'float' but got " + itos(
         static_cast<int>(pres.type)), F, g_util_ident);
  }
  int num_values = 0;
  float pres_miss_val = 3.e48;
  short numeric_id = -1;
  gatherxml::markup::ObML::IDEntry ientry;
  metautils::StringEntry se;
  string platform_type, datatype;
  float lat = -1.e38, lon = -1.e38;
  for (const auto& attr_entry : ds->attributes) {
    if (attr_entry.first == "NROWS") {
      num_values = *(reinterpret_cast<int *>(attr_entry.second.array));
    } else if (attr_entry.first == "LATITUDE_DDEG") {
      lat = *(reinterpret_cast<float *>(attr_entry.second.array));
    } else if (attr_entry.first == "LONGITUDE_DDEG") {
      lon = *(reinterpret_cast<float *>(attr_entry.second.array));
    } else if (attr_entry.first == "CHAR_STATION_ID") {
      if (platform_type.empty()) {
        platform_type = "land_station";
        ientry.key.assign(reinterpret_cast<char *>(attr_entry.second.array));
        ientry.key.insert(0, platform_type + "[!]USArray[!]TA.");
      } else {
        log_error2("multiple station IDs not expected", F, g_util_ident);
      }
    } else if (attr_entry.first == "NUMERIC_STATION_ID") {
      numeric_id = *(reinterpret_cast<short *>(attr_entry.second.array));
    } else if (attr_entry.first == "FIELD_2_NAME") {
      datatype.assign(reinterpret_cast<char *>(attr_entry.second.array));
    } else if (attr_entry.first == "FIELD_2_FILL") {
      pres_miss_val = *(reinterpret_cast<float *>(attr_entry.second.array));
    } else if (attr_entry.first == "FIELD_2_DESCRIPTION") {
      se.key.assign(reinterpret_cast<char *>(attr_entry.second.array));
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
  if (se.key.empty()) {
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
  scan_data.map_name = unixutils::remote_web_file(
      "https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds" +
      metautils::args.dsnum + ".xml", scan_data.tdir->name());
  scan_data.found_map = !scan_data.map_name.empty();
  se.key = datatype + "<!>" + se.key + "<!>Hz";
  scan_data.varlist.emplace_back(se.key);
  if (scan_data.found_map && scan_data.var_changes_table.find(datatype) ==
      scan_data.var_changes_table.end()) {
    scan_data.var_changes_table.emplace(datatype);
  }
  scan_data.write_type = ScanData::ObML_type;
}

string gridded_time_method(
    const std::shared_ptr<InputHDF5Stream::Dataset> ds,
    const GridData& grid_data) {
  static const string F = this_function_label(__func__);
  auto attr_it = ds->attributes.find("cell_methods");
  if (attr_it != ds->attributes.end() && attr_it->second._class_ == 3) {
    auto time_method = metautils::NcTime::time_method_from_cell_methods(
        reinterpret_cast<char *>(attr_it->second.array),
        grid_data.valid_time.id);
    if (time_method[0] == '!') {
      log_error2("cell method '" + time_method.substr(1) + "' is not valid CF",
          F, g_util_ident);
    } else {
      return time_method;
    }
  }
  return "";
}

void add_gridded_time_range(string key_start, std::unordered_set<string>&
    grid_entry_set, const GridData& grid_data, InputHDF5Stream& istream) {
  static const string F = this_function_label(__func__);
  string grid_entry_key, inv_key;
  bool found_no_method = false;
  auto vars = istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    auto& dset_ptr = var.second;
    auto attr_it = dset_ptr->attributes.find("DIMENSION_LIST");
    if (attr_it != dset_ptr->attributes.end() &&
        grid_data.coordinate_variables_set.find(var.first) ==
        grid_data.coordinate_variables_set.end() && attr_it->
        second.dim_sizes.size() == 1 && (attr_it->second.dim_sizes[0] > 2 ||
        (attr_it->second.dim_sizes[0] == 2 &&
        grid_data.valid_time.data_array.num_values == 1))) {
      auto time_method = gridded_time_method(dset_ptr, grid_data);
      if (time_method.empty()) {
        found_no_method = true;
      } else {
        string error;
        inv_key = metautils::NcTime::gridded_netcdf_time_range_description(
            grid_data.time_range_entry, *grid_data.time_data,
            strutils::capitalize(time_method), error);
        if (!error.empty()) {
          log_error2(error, F, g_util_ident);
        }
        grid_entry_key = key_start + inv_key;
        metautils::StringEntry se;
        if (grid_entry_set.find(grid_entry_key) == grid_entry_set.end()) {
          grid_entry_set.emplace(grid_entry_key);
        }
      }
    }
  }
  if (found_no_method) {
    string error;
    inv_key = metautils::NcTime::gridded_netcdf_time_range_description(
        grid_data.time_range_entry, *grid_data.time_data, "", error);
    if (!error.empty()) {
      log_error2(error, F, g_util_ident);
    }
    grid_entry_key = key_start + inv_key;
    metautils::StringEntry se;
    if (grid_entry_set.find(grid_entry_key) == grid_entry_set.end()) {
      grid_entry_set.emplace(grid_entry_key);
    }
  }
  if (g_inv.stream.is_open() && g_inv.maps.U.find(inv_key) == g_inv.maps.U.
      end()) {
    g_inv.maps.U.emplace(inv_key, g_inv.maps.U.size());
  }
}

void add_gridded_lat_lon_keys(std::unordered_set<string>& grid_entry_set,
    Grid::GridDimensions dim, Grid::GridDefinition def,
    const GridData& grid_data, InputHDF5Stream& istream) {
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
      add_gridded_time_range(key_start, grid_entry_set, grid_data, istream);
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
      add_gridded_time_range(key_start, grid_entry_set, grid_data, istream);
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
      add_gridded_time_range(key_start, grid_entry_set, grid_data, istream);
      break;
    }
    default: { }
  }
  auto key = strutils::substitute(key_start, "<!>", ",");
  strutils::chop(key);
  if (g_inv.stream.is_open() && g_inv.maps.G.find(key) == g_inv.maps.G.end()) {
    g_inv.maps.G.emplace(key, g_inv.maps.G.size());
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

void add_gridded_netcdf_parameter(const InputHDF5Stream::DatasetEntry& var,
    ScanData& scan_data, const metautils::NcTime::TimeRange& time_range,
    ParameterData& parameter_data, int num_steps) {
  string description;
  string units;
  auto& attributes = var.second->attributes;
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
  auto var_name = var.first;
  trim(var_name);
  trim(description);
  trim(units);
  trim(standard_name);
  metautils::StringEntry se;
  se.key = var_name + "<!>" + description + "<!>" + units + "<!>" +
      standard_name;
  if (parameter_data.set.find(se.key) == parameter_data.set.end()) {
    auto short_name = parameter_data.map.short_name(var.first);
    if (!scan_data.found_map || short_name.empty()) {
      parameter_data.set.emplace(se.key);
      scan_data.varlist.emplace_back(se.key);
    } else {
      parameter_data.set.emplace(se.key);
      scan_data.varlist.emplace_back(se.key);
      if (scan_data.var_changes_table.find(var.first) == scan_data.
          var_changes_table.end()) {
        scan_data.var_changes_table.emplace(var.first);
      }
    }
  }
  parameter_entry_ptr->start_date_time = time_range.first_valid_datetime;
  parameter_entry_ptr->end_date_time = time_range.last_valid_datetime;
  parameter_entry_ptr->num_time_steps = num_steps;
  level_entry_ptr->parameter_code_table.emplace(parameter_entry_key,
      *parameter_entry_ptr);
}

bool parameter_matches_dimensions(InputHDF5Stream& istream, const
    InputHDF5Stream::DataValue& dimension_list, const GridData& grid_data) {
  static const string F = this_function_label(__func__);
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
    rtp_it[rcnt] = istream.reference_table_pointer()->find(HDF5::value(
        &dimension_list.vlen.buffer[off], dimension_list.precision_));
    if (rtp_it[rcnt] == istream.reference_table_pointer()->end()) {
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

      // data variables dimensioned [time, lat, lon] or [lat, lon] with scalar
      //     time
      if (grid_data.level.id == "sfc" && (dimension_list.dim_sizes[0] == 3 ||
          grid_data.valid_time.data_array.num_values == 1)) {
        if (rtp_it[0]->second == grid_data.latitude.id && rtp_it[1]->second ==
            grid_data.longitude.id) {

          // latitude and longitude are coordinate variables
          parameter_matches = true;
        } else if (rtp_it[0]->second != grid_data.latitude.id && rtp_it[1]->
            second != grid_data.longitude.id) {

          // check for auxiliary coordinate variables for latitude and longitude
          auto lat_ds = istream.dataset("/" + grid_data.latitude.id);
          auto lon_ds = istream.dataset("/" + grid_data.longitude.id);
          if (lat_ds != nullptr && lon_ds != nullptr) {
            stringstream lat_dims;
            lat_ds->attributes["DIMENSION_LIST"].print(lat_dims, istream.
                reference_table_pointer());
            stringstream lon_dims;
            lon_ds->attributes["DIMENSION_LIST"].print(lon_dims, istream.
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

      // data variables dimensioned [time, lev, lat, lon] or [reference_time,
      //     forecast_period, lev, lat, lon]
      auto off = dimension_list.dim_sizes[0] - 4;
      auto can_continue = true;
      if (rtp_it[off]->second != grid_data.level.id) {
        can_continue = false;
        if (grid_data.level.ds != nullptr) {
          stringstream lev_dims;
          grid_data.level.ds->attributes["DIMENSION_LIST"].print(lev_dims,
              istream.reference_table_pointer());
          if (lev_dims.str() == "[" + rtp_it[off]->second + "]") {
            can_continue = true;
          }
        } else if (grid_data.level.id == "sfc" && rtp_it[off]->second ==
            grid_data.forecast_period.id) {
          can_continue = true;
        }
      }
      if (can_continue) {
        if (rtp_it[off + 1]->second == grid_data.latitude.id && rtp_it[off +
            2]->second == grid_data.longitude.id) {

          // latitude and longitude are coordinate variables
          parameter_matches = true;
        } else {

          // check for auxiliary coordinate variables for latitude and longitude
          auto lat_ds = istream.dataset("/" + grid_data.latitude.id);
          auto lon_ds = istream.dataset("/" + grid_data.longitude.id);
          if (lat_ds != nullptr && lon_ds != nullptr) {
            stringstream lat_dims;
            lat_ds->attributes["DIMENSION_LIST"].print(lat_dims, istream.
                reference_table_pointer());
            stringstream lon_dims;
            lon_ds->attributes["DIMENSION_LIST"].print(lon_dims,
                istream.reference_table_pointer());
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

void add_gridded_parameters_to_netcdf_level_entry(InputHDF5Stream& istream,
    string& grid_entry_key, const GridData& grid_data, ScanData& scan_data,
    const metautils::NcTime::TimeBounds& time_bounds,
    ParameterData& parameter_data) {
  static const string F = this_function_label(__func__);

  // find all of the variables
  auto vars = istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    if (grid_data.coordinate_variables_set.find(var.first) ==
        grid_data.coordinate_variables_set.end()) {
      auto& dset_ptr = var.second;
      auto attr_it = dset_ptr->attributes.find("DIMENSION_LIST");
      if (gatherxml::verbose_operation) {
        cout << "    '" << var.first << "' has a DIMENSION_LIST: ";
        attr_it->second.print(cout, istream.reference_table_pointer());
        cout << endl;
      }
      if (attr_it->second._class_ == 9 && attr_it->second.dim_sizes.size() ==
          1 && (attr_it->second.dim_sizes[0] > 2 || (attr_it->
          second.dim_sizes[0] == 2 &&
          grid_data.valid_time.data_array.num_values == 1)) && attr_it->
          second.vlen.class_ == 7 && parameter_matches_dimensions(
          istream, attr_it->second, grid_data)) {
        if (gatherxml::verbose_operation) {
          cout << "    ...is a netCDF variable" << endl;
        }
        auto time_method = gridded_time_method(dset_ptr, grid_data);
        metautils::NcTime::TimeRange time_range;
        if (time_method.empty() || (floatutils::myequalf(time_bounds.t1, 0,
            0.0001) && floatutils::myequalf(time_bounds.t1, time_bounds.t2,
            0.0001))) {
          time_range.first_valid_datetime = grid_data.time_range_entry.
              instantaneous.first_valid_datetime;
          time_range.last_valid_datetime = grid_data.time_range_entry.
              instantaneous.last_valid_datetime;
        } else {
          if (time_bounds.changed) {
            log_error2("time bounds changed", F, g_util_ident);
          }
          time_range.first_valid_datetime = grid_data.time_range_entry.bounded.
              first_valid_datetime;
          time_range.last_valid_datetime = grid_data.time_range_entry.bounded.
              last_valid_datetime;
        }
        time_method = strutils::capitalize(time_method);
        string tr_description, error;
        tr_description =
            metautils::NcTime::gridded_netcdf_time_range_description(
            grid_data.time_range_entry, *grid_data.time_data, time_method,
            error);
        if (!error.empty()) {
          log_error2(error + "; var name '" + var.first + "'", F, g_util_ident);
        }
        tr_description = strutils::capitalize(tr_description);
        if (std::regex_search(grid_entry_key, std::regex(tr_description +
            "$"))) {
//          if (attr.value.dim_sizes[0] == 4 || attr.value.dim_sizes[0] == 3 || (attr.value.dim_sizes[0] == 2 && grid_data.valid_time.data_array.num_values == 1)) {
            parameter_entry_key = "ds" + metautils::args.dsnum + ":" +
                var.first;
            add_gridded_netcdf_parameter(var, scan_data, time_range,
                parameter_data, grid_data.time_range_entry.num_steps);
            if (g_inv.maps.P.find(parameter_entry_key) == g_inv.maps.P.end()) {
              g_inv.maps.P.emplace(parameter_entry_key, g_inv.maps.P.size());
            }
//          }
        }
      }
    }
  }
}

void update_level_entry(InputHDF5Stream& istream,
    const metautils::NcTime::TimeBounds& time_bounds, const GridData& grid_data,
    ScanData& scan_data, ParameterData& parameter_data, bool& level_write) {
  static const string F = this_function_label(__func__);
  auto vars = istream.datasets_with_attribute("DIMENSION_LIST");
  for (const auto& var : vars) {
    auto& dset_ptr = var.second;
    auto attr_it = dset_ptr->attributes.find("DIMENSION_LIST");
    if (attr_it->second._class_ == 9 && attr_it->second.dim_sizes.size() == 1 &&
        attr_it->second.dim_sizes[0] > 2 && attr_it->second.vlen.class_ == 7 &&
        parameter_matches_dimensions(istream, attr_it->second, grid_data)) {
      parameter_entry_key = "ds" + metautils::args.dsnum + ":" + var.first;
      auto time_method = gridded_time_method(dset_ptr, grid_data);
      time_method = strutils::capitalize(time_method);
      metautils::NcTime::TimeRange time_range;
      if (level_entry_ptr->parameter_code_table.find(parameter_entry_key) ==
          level_entry_ptr->parameter_code_table.end()) {
        if (time_method.empty() || (floatutils::myequalf(time_bounds.t1, 0,
            0.0001) && floatutils::myequalf(time_bounds.t1, time_bounds.t2,
            0.0001))) {
          time_range.first_valid_datetime = grid_data.time_range_entry.
              instantaneous.first_valid_datetime;
          time_range.last_valid_datetime = grid_data.time_range_entry.
              instantaneous.last_valid_datetime;
          add_gridded_netcdf_parameter(var, scan_data, time_range,
              parameter_data, grid_data.time_range_entry.num_steps);
        } else {
          if (time_bounds.changed) {
            log_error2("time bounds changed", F, g_util_ident);
          }
          time_range.first_valid_datetime = grid_data.time_range_entry.bounded.
              first_valid_datetime;
          time_range.last_valid_datetime = grid_data.time_range_entry.bounded.
              last_valid_datetime;
          add_gridded_netcdf_parameter(var, scan_data, time_range,
              parameter_data, grid_data.time_range_entry.num_steps);
        }
        grid_entry_ptr->level_table[level_entry_key] = *level_entry_ptr;
      } else {
        string error;
        auto tr_description =
            metautils::NcTime::gridded_netcdf_time_range_description(
            grid_data.time_range_entry, *grid_data.time_data, time_method,
            error);
        if (!error.empty()) {
          log_error2(error, F, g_util_ident);
        }
        tr_description = strutils::capitalize(tr_description);
        if (std::regex_search(grid_entry_key, std::regex(tr_description +
            "$"))) {
          auto pe = level_entry_ptr->parameter_code_table[parameter_entry_key];
          if (time_method.empty() || (floatutils::myequalf(time_bounds.t1, 0,
              0.0001) && floatutils::myequalf(time_bounds.t1, time_bounds.t2,
              0.0001))) {
            if (grid_data.time_range_entry.instantaneous.first_valid_datetime <
                pe.start_date_time) {
              pe.start_date_time = grid_data.time_range_entry.instantaneous.
                  first_valid_datetime;
            }
            if (grid_data.time_range_entry.instantaneous.last_valid_datetime >
                pe.end_date_time) {
              pe.end_date_time = grid_data.time_range_entry.instantaneous.
                  last_valid_datetime;
            }
          } else {
            if (grid_data.time_range_entry.bounded.first_valid_datetime <
                pe.start_date_time) {
              pe.start_date_time = grid_data.time_range_entry.bounded.
                  first_valid_datetime;
            }
            if (grid_data.time_range_entry.bounded.last_valid_datetime >
                pe.end_date_time) {
              pe.end_date_time = grid_data.time_range_entry.bounded.
                  last_valid_datetime;
            }
          }
          pe.num_time_steps += grid_data.time_range_entry.num_steps;
          grid_entry_ptr->level_table[level_entry_key] = *level_entry_ptr;
        }
      }
      level_write = true;
      if (g_inv.maps.P.find(parameter_entry_key) == g_inv.maps.P.end()) {
        g_inv.maps.P.emplace(parameter_entry_key, g_inv.maps.P.size());
      }
    }
  }
}

void fill_time_bounds(const HDF5::DataArray& data_array, InputHDF5Stream::
    Dataset *ds, metautils::NcTime::TimeRangeEntry& tre, const metautils::
    NcTime::TimeData& time_data, metautils::NcTime::TimeBounds& time_bounds) {
  static const string F = this_function_label(__func__);
  time_bounds.t1 = data_array_value(data_array, 0, ds);
  time_bounds.diff = data_array_value(data_array, 1, ds)-time_bounds.t1;
  for (size_t n = 2; n < data_array.num_values; n += 2) {
    if (!floatutils::myequalf((data_array_value(data_array, n + 1, ds) -
        data_array_value(data_array, n, ds)), time_bounds.diff)) {
      time_bounds.changed = true;
      break;
    }
  }
  time_bounds.t2 = data_array_value(data_array, data_array.num_values - 1, ds);
  string error;
  tre.bounded.first_valid_datetime = metautils::NcTime::actual_date_time(
      time_bounds.t1, time_data, error);
  if (!error.empty()) {
    log_error2(error, F, g_util_ident);
  }
  tre.bounded.last_valid_datetime = metautils::NcTime::actual_date_time(
      time_bounds.t2, time_data, error);
  if (!error.empty()) {
    log_error2(error, F, g_util_ident);
  }
}

DateTime compute_nc_time(const HDF5::DataArray& times,
    const metautils::NcTime::TimeData& time_data, size_t index) {
  static const string F = this_function_label(__func__);
  auto val = times.value(index);
  DateTime dt;
  if (time_data.units == "seconds") {
    dt = time_data.reference.seconds_added(val);
  } else if (time_data.units == "hours") {
    if (floatutils::myequalf(val, static_cast<int>(val), 0.001)) {
      dt = time_data.reference.hours_added(val);
    } else {
      dt = time_data.reference.seconds_added(lround(val*3600.));
    }
  } else if (time_data.units == "days") {
    if (floatutils::myequalf(val, static_cast<int>(val), 0.001)) {
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

void update_inventory(int unum, int gnum, const GridData& grid_data) {
  if (g_inv.maps.L.find(level_entry_key) == g_inv.maps.L.end()) {
    g_inv.maps.L.emplace(level_entry_key, g_inv.maps.L.size());
  }
  for (size_t n = 0; n < grid_data.valid_time.data_array.num_values; ++n) {
    for (const auto& e : level_entry_ptr->parameter_code_table) {
      stringstream inv_line;
      string error;
      inv_line << "0|0|" << metautils::NcTime::actual_date_time(
          data_array_value(grid_data.valid_time.data_array, n, grid_data.
          valid_time.ds.get()), *grid_data.time_data, error).to_string(
          "%Y%m%d%H%MM") << "|" << unum << "|" << gnum << "|" << g_inv.maps.L[
          level_entry_key] << "|" << g_inv.maps.P[e.first] << "|0";
      g_inv.lines.emplace_back(inv_line.str());
    }
  }
}

void process_units_attribute(const InputHDF5Stream::DatasetEntry& ds_entry,
    DiscreteGeometriesData& dgd, metautils::NcTime::TimeData& time_data) {
  static const string F = this_function_label(__func__);
  auto& var_name = ds_entry.first;
  auto attr_val = ds_entry.second->attributes["units"];
  string units_value(reinterpret_cast<char *>(attr_val.get()),
      attr_val.size);
  if (std::regex_search(units_value, std::regex("since"))) {
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

void fill_dgd_index(InputHDF5Stream& istream, string attribute_name_to_match,
    string attribute_value_to_match, string& dgd_index) {
  static const string F = this_function_label(__func__);
  auto ds_entry_list = istream.datasets_with_attribute(attribute_name_to_match);
  if (ds_entry_list.size() > 1) {
    log_error2("more than one " + attribute_name_to_match + " variable found",
        F, g_util_ident);
  } else if (ds_entry_list.size() > 0) {
    auto aval = ds_entry_list.front().second->
        attributes[attribute_name_to_match];
    string attr_val(reinterpret_cast<char *>(aval.get()), aval.size);
    if (attribute_value_to_match.empty() || attr_val ==
        attribute_value_to_match) {
      if (!dgd_index.empty()) {
        log_error2(attribute_name_to_match + " was already identified - don't "
            "know what to do with variable: " + ds_entry_list.front().first, F,
            g_util_ident);
      } else {
        dgd_index = ds_entry_list.front().first;
      }
    }
  }
}

void fill_dgd_index(InputHDF5Stream& istream, string attribute_name_to_match,
    unordered_map<string, string>& dgd_index) {
  static const string F = this_function_label(__func__);
  auto ds_entry_list = istream.datasets_with_attribute(attribute_name_to_match);
  for (const auto& ds_entry : ds_entry_list) {
    auto aval = ds_entry.second->attributes[attribute_name_to_match];
    string attr_val(reinterpret_cast<char *>(aval.get()), aval.size);
    if (dgd_index.find(attr_val) == dgd_index.end()) {
      dgd_index.emplace(attr_val, ds_entry.first);
    } else {
      log_error2(attribute_name_to_match + " was already identified - don't "
          "know what to do with variable: " + ds_entry.first, F, g_util_ident);
    }
  }
}

void process_vertical_coordinate_variable(InputHDF5Stream& istream,
    DiscreteGeometriesData& dgd, string& obs_type) {
  static const string F = this_function_label(__func__);
  obs_type = "";
  auto ds = istream.dataset("/" + dgd.indexes.z_var);
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
    if (std::regex_search(dgd.z_units, std::regex("Pa$")) || std::regex_search(
        z_units_l, std::regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
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
      if (std::regex_search(dgd.z_units, std::regex("Pa$")) ||
          std::regex_search(z_units_l, std::regex("^mb(ar){0,1}$")) ||
          z_units_l == "millibars") {
        obs_type = "upper_air";
      } else if (dgd.z_units == "m") {
        obs_type = "subsurface";
      }
    }
  }
}

void scan_cf_point_hdf5nc4_file(InputHDF5Stream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto ds_entry_list = istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  metautils::NcTime::TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  ds_entry_list = istream.datasets_with_attribute("coordinates");

  // look for a "station ID"
  for (const auto& ds_entry : ds_entry_list) {
    auto& dset_ptr = ds_entry.second;
    if (dset_ptr->datatype.class_ == 3) {
      auto attr_it = dset_ptr->attributes.find("long_name");
      if (attr_it != dset_ptr->attributes.end() && attr_it->second._class_ ==
          3) {
        string attribute_value(reinterpret_cast<char *>(attr_it->
            second.get()), attr_it->second.size);
        if (std::regex_search(attribute_value, std::regex("ID")) ||
            std::regex_search(attribute_value, std::regex("ident",
            std::regex::icase))) {
          dgd.indexes.stn_id_var = ds_entry.first;
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
    auto ds = istream.dataset("/" + dgd.indexes.time_var);
    if (ds == nullptr) {
      log_error2("unable to access time variable", F, g_util_ident);
    }
    auto attr_it = ds->attributes.find("calendar");
    if (attr_it != ds->attributes.end()) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr_it->second.get()),
        attr_it->second.size);
    }
    time_vals.fill(istream, *ds);
  }
  HDF5::DataArray lat_vals;
  if (dgd.indexes.lat_var.empty()) {
    log_error2("unable to determine latitude variable", F, g_util_ident);
  } else {
    auto ds = istream.dataset("/" + dgd.indexes.lat_var);
    if (ds == nullptr) {
      log_error2("unable to access latitude variable", F, g_util_ident);
    }
    lat_vals.fill(istream, *ds);
  }
  HDF5::DataArray lon_vals;
  if (dgd.indexes.lon_var.empty()) {
    log_error2("unable to determine longitude variable", F, g_util_ident);
  } else {
    auto ds = istream.dataset("/" + dgd.indexes.lon_var);
    if (ds == nullptr) {
      log_error2("unable to access longitude variable", F, g_util_ident);
    }
    lon_vals.fill(istream, *ds);
  }
  HDF5::DataArray id_vals;
  if (dgd.indexes.stn_id_var.empty()) {
    log_error2("unable to determine report ID variable", F, g_util_ident);
  } else {
    auto ds = istream.dataset("/" + dgd.indexes.stn_id_var);
    if (ds == nullptr) {
      log_error2("unable to access report ID variable", F, g_util_ident);
    }
    id_vals.fill(istream, *ds);
  }
  scan_data.map_name = unixutils::remote_web_file(
      "https://rda.ucar.edu/metadata/ParameterTables/HDF5.ds" +
      metautils::args.dsnum + ".xml", scan_data.tdir->name());
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
  for (const auto& ds_entry : ds_entry_list) {
    auto& var_name = ds_entry.first;
    if (var_name != dgd.indexes.time_var && var_name != dgd.indexes.lat_var &&
        var_name != dgd.indexes.lon_var && var_name != dgd.indexes.stn_id_var) {
      unique_data_type_observation_set.clear();
      de.key = var_name;
      auto ds = istream.dataset("/" + var_name);
      string descr, units;
      for (const auto& attr_entry : ds->attributes) {
      auto lkey = strutils::to_lower(attr_entry.first);
        if (lkey == "long_name" || (descr.empty() && (lkey == "description" ||
            std::regex_search(lkey, std::regex("^comment"))))) {
          descr.assign(reinterpret_cast<char *>(attr_entry.second.array));
        } else if (lkey == "units") {
          units.assign(reinterpret_cast<char *>(attr_entry.second.array));
        }
      }
      trim(descr);
      trim(units);
      metautils::StringEntry se;
      se.key = var_name + "<!>" + descr + "<!>" + units;
      scan_data.varlist.emplace_back(se.key);
      if (scan_data.found_map && scan_data.var_changes_table.find(var_name) ==
          scan_data.var_changes_table.end()) {
        scan_data.var_changes_table.emplace(var_name);
      }
      HDF5::DataArray var_data;
      var_data.fill(istream, *ds);
      auto var_missing_value = HDF5::decode_data_value(ds->datatype,
          ds->fillvalue.bytes, 1.e33);
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

void scan_cf_orthogonal_time_series_hdf5nc4_file(InputHDF5Stream& istream,
    const DiscreteGeometriesData& dgd,
    const metautils::NcTime::TimeData& time_data, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  if (dgd.indexes.lat_var.empty()) {
    log_error2("latitude could not be identified", F, g_util_ident);
  }
  if (dgd.indexes.lon_var.empty()) {
    log_error2("longitude could not be identified", F, g_util_ident);
  }
  vector<string> platform_types, id_types, id_cache;
  size_t num_stns = 0;
  if (dgd.indexes.stn_id_var.empty()) {
    auto root_ds = istream.dataset("/");
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
  auto times_ds = istream.dataset("/" + dgd.indexes.time_var);
  if (times_ds == nullptr) {
    log_error2("unable to get time dataset", F, g_util_ident);
  }
  NetCDFVariableAttributeData nc_ta_data;
  extract_from_hdf5_variable_attributes(times_ds->attributes, nc_ta_data);
  HDF5::DataArray time_vals;
  time_vals.fill(istream, *times_ds);
  auto lats_ds = istream.dataset("/" + dgd.indexes.lat_var);
  if (lats_ds == nullptr) {
    log_error2("unable to get latitude dataset", F, g_util_ident);
  }
  HDF5::DataArray lat_vals;
  lat_vals.fill(istream, *lats_ds);
  if (lat_vals.num_values != num_stns) {
    log_error2("number of stations does not match number of latitudes", F,
        g_util_ident);
  }
  auto lons_ds = istream.dataset("/" + dgd.indexes.lon_var);
  if (lons_ds == nullptr) {
    log_error2("unable to get longitude dataset", F, g_util_ident);
  }
  HDF5::DataArray lon_vals;
  lon_vals.fill(istream, *lons_ds);
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
  auto ds_entry_list = istream.datasets_with_attribute("DIMENSION_LIST");
  auto netcdf_var_re = std::regex("^\\[" + dgd.indexes.time_var +
      "(,.{1,}){0,1}\\]$");
  vector<DateTime> dts;
  vector<string> datatypes_list;
  for (const auto& ds_entry : ds_entry_list) {
    auto& var_name = ds_entry.first;
    if (var_name != dgd.indexes.time_var && var_name != dgd.indexes.lat_var &&
        var_name != dgd.indexes.lon_var) {
      stringstream dim_list_ss;
      ds_entry.second->attributes["DIMENSION_LIST"].print(dim_list_ss,
          istream.reference_table_pointer());
      if (std::regex_search(dim_list_ss.str(), netcdf_var_re)) {
        if (gatherxml::verbose_operation) {
          cout << "Scanning netCDF variable '" << var_name << "' ..." <<
              endl;
        }
      }
      auto var_ds = istream.dataset("/" + var_name);
      if (var_ds == nullptr) {
        log_error2("unable to get data for variable '" + var_name + "'", F,
            g_util_ident);
      }
      NetCDFVariableAttributeData nc_va_data;
      extract_from_hdf5_variable_attributes(var_ds->attributes, nc_va_data);
      HDF5::DataArray var_vals;
      var_vals.fill(istream, *var_ds);
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

void scan_cf_time_series_hdf5nc4_file(InputHDF5Stream& istream,
    ScanData& scan_data, gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto ds_entry_list = istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  metautils::NcTime::TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  if (dgd.indexes.time_var.empty()) {
    log_error2("unable to determine time variable", F, g_util_ident);
  }
  fill_dgd_index(istream, "cf_role", "profile_id", dgd.indexes.stn_id_var);
  fill_dgd_index(istream, "sample_dimension", dgd.indexes.sample_dim_vars);
  fill_dgd_index(istream, "instance_dimension", "",
      dgd.indexes.instance_dim_var);
  auto ds = istream.dataset("/" + dgd.indexes.time_var);
  auto attr_it = ds->attributes.find("_Netcdf4Dimid");
  if (attr_it != ds->attributes.end()) {

    // ex. H.2, H.4 (single version of H.2), H.5 (precise locations) stns w/same
    //     times
    scan_cf_orthogonal_time_series_hdf5nc4_file(istream, dgd, time_data,
        scan_data, obs_data);
  } else {

// ex. H.3 stns w/varying times but same # of obs
// ex. H.6 w/sample_dimension
// ex. H.7 w/instance_dimension
  }
  scan_data.write_type = ScanData::ObML_type;
}

void scan_cf_non_orthogonal_profile_hdf5nc4_file(InputHDF5Stream& istream,
    const DiscreteGeometriesData& dgd,
    const metautils::NcTime::TimeData& time_data,
    const HDF5::DataArray& time_vals,
    const NetCDFVariableAttributeData& nc_ta_data, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto ds = istream.dataset("/" + dgd.indexes.stn_id_var);
  if (ds == nullptr) {
    log_error2("unable to access station ID variable", F, g_util_ident);
  }
  HDF5::DataArray id_vals(istream, *ds);
  ds = istream.dataset("/" + dgd.indexes.lat_var);
  if (ds == nullptr) {
    log_error2("unable to access latitude variable", F, g_util_ident);
  }
  HDF5::DataArray lat_vals(istream, *ds);
  ds = istream.dataset("/" + dgd.indexes.lon_var);
  if (ds == nullptr) {
    log_error2("unable to access longitude variable", F, g_util_ident);
  }
  HDF5::DataArray lon_vals(istream, *ds);
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
  ds = istream.dataset("/" + dgd.indexes.z_var);
  if (ds == nullptr) {
    log_error2("unable to access vertical level variable", F, g_util_ident);
  }
  HDF5::DataArray z_vals(istream, *ds);
  auto attr_it = ds->attributes.find("DIMENSION_LIST");
  if (attr_it == ds->attributes.end()) {
    log_error2("unable to get vertical level row size variable", F,
        g_util_ident);
  }
  stringstream val_ss;
  attr_it->second.print(val_ss, istream.reference_table_pointer());
  auto var_list = istream.datasets_with_attribute("DIMENSION_LIST");
  auto dims = split(val_ss.str(), ",");
  strutils::replace_all(dims.front(), "[", "");
  strutils::replace_all(dims.front(), "]", "");
  trim(dims.front());
  ds = istream.dataset("/" + dgd.indexes.sample_dim_vars.at(dims.front()));
  if (ds == nullptr) {
    log_error2("unable to get vertical level row sizes", F, g_util_ident);
  }
  HDF5::DataArray z_rowsize_vals(istream, *ds);
  if (dgd.indexes.sample_dim_vars.size() > 0) {
// continuous ragged array H.10
    for (const auto& var : var_list) {
      auto& var_name = var.first;
      auto& dset_ptr = var.second;
      val_ss.str("");
      dset_ptr->attributes["DIMENSION_LIST"].print(val_ss,
          istream.reference_table_pointer());
      auto dims = split(val_ss.str(), ",");
      strutils::replace_all(dims.front(), "[", "");
      strutils::replace_all(dims.front(), "]", "");
      trim(dims.front());
      if (dgd.indexes.sample_dim_vars.find(dims.front()) !=
          dgd.indexes.sample_dim_vars.end() && var_name != dgd.indexes.z_var) {
        ds = istream.dataset("/" + dgd.indexes.sample_dim_vars.at(
            dims.front()));
        if (ds == nullptr) {
          log_error2("unable to get row size data for " + var_name, F,
              g_util_ident);
        }
        HDF5::DataArray rowsize_vals(istream, *ds);
        ds = istream.dataset("/" + var_name);
        if (ds == nullptr) {
          log_error2("unable to get variable values for " + var_name, F,
              g_util_ident);
        }
        HDF5::DataArray var_vals(istream, *ds);
        if (var_vals.type != HDF5::DataArray::Type::_NULL) {
          NetCDFVariableAttributeData nc_va_data;
          extract_from_hdf5_variable_attributes(dset_ptr->attributes,
              nc_va_data);
          metautils::StringEntry se;
          se.key = var_name + "<!>" + nc_va_data.long_name + "<!>" +
              nc_va_data.units;
          scan_data.varlist.emplace_back(se.key);
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

void scan_cf_orthogonal_profile_hdf5nc4_file(InputHDF5Stream& istream,
    const DiscreteGeometriesData& dgd, const HDF5::DataArray& time_vals,
    const NetCDFVariableAttributeData& nc_ta_data, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data, string obs_type) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void scan_cf_profile_hdf5nc4_file(InputHDF5Stream& istream, ScanData& scan_data,
     gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function " << F << "..." << endl;
  }
  auto ds_entry_list = istream.datasets_with_attribute("units");
  DiscreteGeometriesData dgd;
  metautils::NcTime::TimeData time_data;
  for (const auto& ds_entry : ds_entry_list) {
    process_units_attribute(ds_entry, dgd, time_data);
  }
  fill_dgd_index(istream, "cf_role", "profile_id", dgd.indexes.stn_id_var);
  fill_dgd_index(istream, "sample_dimension", dgd.indexes.sample_dim_vars);
  fill_dgd_index(istream, "instance_dimension", "",
      dgd.indexes.instance_dim_var);
  HDF5::DataArray time_vals;
  NetCDFVariableAttributeData nc_ta_data;
  if (dgd.indexes.time_var.empty()) {
    log_error2("unable to determine time variable", F, g_util_ident);
  } else {
    auto ds = istream.dataset("/" + dgd.indexes.time_var);
    if (ds == nullptr) {
      log_error2("unable to access time variable", F, g_util_ident);
    }
    auto attr_it = ds->attributes.find("calendar");
    if (attr_it != ds->attributes.end()) {
      time_data.calendar.assign(reinterpret_cast<char *>(attr_it->second.get()),
          attr_it->second.size);
    }
    time_vals.fill(istream, *ds);
    extract_from_hdf5_variable_attributes(ds->attributes, nc_ta_data);
  }
  fill_dgd_index(istream, "axis", "Z", dgd.indexes.z_var);
  if (dgd.indexes.z_var.empty()) {
    fill_dgd_index(istream, "positive", "", dgd.indexes.z_var);
  }
  string obs_type;
  if (dgd.indexes.z_var.empty()) {
    log_error2("unable to determine vertical coordinate variable", F,
        g_util_ident);
  } else {
    process_vertical_coordinate_variable(istream, dgd, obs_type);
  }
  if (obs_type.empty()) {
    log_error2("unable to determine observation type", F, g_util_ident);
  }
  scan_data.map_name = unixutils::remote_web_file(
      "https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds" +
      metautils::args.dsnum + ".xml", scan_data.tdir->name());
  scan_data.found_map=(!scan_data.map_name.empty());
  if (dgd.indexes.sample_dim_vars.size() > 0 ||
      !dgd.indexes.instance_dim_var.empty()) {

    // ex. H.10, H.11
    scan_cf_non_orthogonal_profile_hdf5nc4_file(istream, dgd, time_data,
        time_vals, nc_ta_data, scan_data, obs_data, obs_type);
  } else {

    // ex. H.8, H.9
    scan_cf_orthogonal_profile_hdf5nc4_file(istream, dgd, time_vals, nc_ta_data,
        scan_data, obs_data, obs_type);
  }
  scan_data.write_type = ScanData::ObML_type;
  if (gatherxml::verbose_operation) {
    cout << "...function " << F << " done." << endl;
  }
}

void find_coordinate_variables(InputHDF5Stream& istream, CoordinateVariables&
    coord_vars, GridData& grid_data) {
  static const string F = this_function_label(__func__);
  auto dim_vars = istream.datasets_with_attribute("CLASS=DIMENSION_SCALE");
  if (gatherxml::verbose_operation) {
    cout << "...found " << dim_vars.size() << " 'DIMENSION_SCALE' "
        "variables:" << endl;
  }
  std::unordered_set<string> unique_level_id_set;
  auto found_time = false;
  for (const auto& var : dim_vars) {
    auto& var_name = var.first;
    auto& dset_ptr = var.second;
    if (gatherxml::verbose_operation) {
      cout << "  '" << var_name << "'" << endl;
    }
    metautils::StringEntry se;
    auto attr_it = dset_ptr->attributes.find("units");
    if (attr_it != dset_ptr->attributes.end() && (attr_it->second._class_ ==
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
      attr_it = dset_ptr->attributes.find("standard_name");
      if (attr_it != dset_ptr->attributes.end() && (attr_it->second._class_ ==
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
      if (std::regex_search(units_value, std::regex("since"))) {
        if (found_time) {
          log_error2("time was already identified - don't know what to do with "
              "variable: " + var_name, F, g_util_ident);
        }
        for (const auto& attribute : dset_ptr->attributes) {
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
          grid_data.coordinate_variables_set.emplace(
              grid_data.reference_time.id);
        }
        coord_vars.nc_time->reference = metautils::NcTime::reference_date_time(
            units_value);
        if (coord_vars.nc_time->reference.year() == 0) {
          log_error2("bad netcdf date in units for time", F, g_util_ident);
        }
        attr_it = dset_ptr->attributes.find("calendar");
        if (attr_it != dset_ptr->attributes.end()) {
          coord_vars.nc_time->calendar.assign(reinterpret_cast<char *>(attr_it->
              second.get()), attr_it->second.size);
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
            attr_it = dset_ptr->attributes.find("long_name");
            if (attr_it != dset_ptr->attributes.end() && attr_it->
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
      attr_it = dset_ptr->attributes.find("positive");
      if (attr_it != dset_ptr->attributes.end() && attr_it->second._class_ ==
          3 && unique_level_id_set.find(var_name) ==
          unique_level_id_set.end()) {
        coord_vars.level_info.back().ID = var_name;
        attr_it = dset_ptr->attributes.find("long_name");
        if (attr_it != dset_ptr->attributes.end() && attr_it->second._class_ ==
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

void check_for_forecasts(InputHDF5Stream& istream, GridData& grid_data,
      std::shared_ptr<metautils::NcTime::TimeData>& time_data) {
  static const string F = this_function_label(__func__);
  auto vars = istream.datasets_with_attribute(
      "standard_name=forecast_reference_time");
  if (vars.size() > 1) {
    log_error2("multiple forecast reference times", F, g_util_ident);
  } else if (!vars.empty()) {
    auto var = vars.front();
    auto attr_it = var.second->attributes.find("units");
    if (attr_it != var.second->attributes.end() && attr_it->second._class_ ==
        3) {
      string units_value = reinterpret_cast<char *>(attr_it->second.array);
      if (std::regex_search(units_value, std::regex("since"))) {
        units_value = units_value.substr(0, units_value.find("since"));
        trim(units_value);
        if (units_value != time_data->units) {
          log_error2("time and forecast reference time have different units", F,
              g_util_ident);
        }
        grid_data.reference_time.id = var.first;
        grid_data.coordinate_variables_set.emplace(grid_data.reference_time.id);
        auto ref_dt = metautils::NcTime::reference_date_time(units_value);
        if (ref_dt.year() == 0) {
          log_error2("bad netcdf date in units for forecast_reference_time", F,
              g_util_ident);
        }
      }
    }
  }
}

void find_vertical_level_coordinates(InputHDF5Stream& istream,
    CoordinateVariables& coord_vars, GridData& grid_data) {
  if (gatherxml::verbose_operation) {
    cout << "...looking for vertical level coordinates..." << endl;
  }
  auto vars = istream.datasets_with_attribute("units=Pa");
  if (vars.size() == 0) {
    vars = istream.datasets_with_attribute("units=hPa");
  }
  for (const auto& v : vars) {
    auto& var_name = v.first;
    auto& dset_ptr = v.second;
    auto attr_it = dset_ptr->attributes.find("DIMENSION_LIST");
    if (attr_it != dset_ptr->attributes.end() && attr_it->
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
  vars = istream.datasets_with_attribute("positive");
  for (const auto& v : vars) {
    auto& var_name = v.first;
    auto& dset_ptr = v.second;
    auto attr_it = dset_ptr->attributes.find("DIMENSION_LIST");
    if (attr_it != dset_ptr->attributes.end() && attr_it->
        second.dim_sizes.size() == 1 && attr_it->second.dim_sizes[0] == 1 &&
        attr_it->second._class_ == 9) {
      coord_vars.level_info.emplace_back();
      coord_vars.level_info.back().ID = var_name;
      if (gatherxml::verbose_operation) {
        cout << "   ...found '" << var_name << "'" << endl;
      }
      attr_it = dset_ptr->attributes.find("description");
      if (attr_it != dset_ptr->attributes.end() && attr_it->second._class_ ==
          3) {
        coord_vars.level_info.back().description = reinterpret_cast<char *>(
            attr_it->second.array);
      } else {
        coord_vars.level_info.back().description = "";
      }
      attr_it = dset_ptr->attributes.find("units");
      if (attr_it != dset_ptr->attributes.end() && attr_it->second._class_ ==
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

void get_forecast_data(InputHDF5Stream& is, GridData& gd) {
  static const string F = this_function_label(__func__);
  if (gd.reference_time.id == gd.valid_time.id) {
    if (!gd.forecast_period.id.empty()) {
      gd.forecast_period.ds = is.dataset("/" + gd.forecast_period.id);
      if (gd.forecast_period.ds != nullptr) {
        gd.forecast_period.data_array.fill(is, *gd.forecast_period.ds);
      }
    }
  } else {
    if (!gd.reference_time.id.empty()) {
      gd.reference_time.ds = is.dataset("/" + gd.  reference_time.id);
      if (gd.reference_time.ds  == nullptr) {
        log_error2("unable to access the /" + gd.reference_time.id + " dataset "
            "for the forecast reference times", F, g_util_ident);
      }
      gd.reference_time.data_array.fill(is, *gd.reference_time.ds);
      if (gd.reference_time.data_array.num_values != gd.valid_time.data_array.
          num_values) {
        log_error2("number of forecast reference times does not equal number "
            "of times", F, g_util_ident);
      }
      for (size_t n = 0; n < gd.valid_time.data_array.num_values; ++n) {
        int m = data_array_value(gd.valid_time.data_array, n, gd.valid_time.ds.
            get()) - data_array_value(gd.reference_time.data_array, n, gd.
            reference_time.ds.get());
        if (m > 0) {
          if (static_cast<int>(gd.time_range_entry.key) == -1) {
            gd.time_range_entry.key = -m * 100;
          }
          if ( (-m * 100) != static_cast<int>(gd.time_range_entry.key)) {
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

metautils::NcTime::TimeBounds time_bounds(InputHDF5Stream& is, const metautils::
    NcTime::TimeData& nc_time, GridData& gd) {
  static const string F = this_function_label(__func__);
  metautils::NcTime::TimeBounds tbnds; // return value
  if (!gd.time_bounds.id.empty()) {
    gd.time_bounds.ds = is.dataset("/" + gd.time_bounds.id);
    if (gd.time_bounds.ds == nullptr) {
      log_error2("unable to access the /" + gd.time_bounds.id + " dataset for "
          "the time bounds", F, g_util_ident);
    }
    HDF5::DataArray bounds(is, *gd.time_bounds.ds);
    if (bounds.num_values > 0) {
      fill_time_bounds(bounds, gd.time_bounds.ds.get(), gd.time_range_entry,
          nc_time, tbnds);
    }
  } else if (!gd.climo_bounds.id.empty()) {
    gd.climo_bounds.ds = is.dataset("/" + gd.climo_bounds.id);
    if (gd.climo_bounds.ds == nullptr) {
      log_error2("unable to access the /" + gd.climo_bounds.id + " dataset for "
          "the climatology bounds", F, g_util_ident);
    }
    HDF5::DataArray bounds(is, *gd.climo_bounds.ds);
    if (bounds.num_values > 0) {
      fill_time_bounds(bounds, gd.climo_bounds.ds.get(), gd.time_range_entry,
          nc_time, tbnds);
      gd.time_range_entry.key = (gd.time_range_entry.bounded.
          last_valid_datetime).years_since(gd.time_range_entry.bounded.
          first_valid_datetime);
      gd.time_range_entry.instantaneous.last_valid_datetime = gd.
          time_range_entry.bounded.last_valid_datetime.years_subtracted(gd.
          time_range_entry.key);
      if (gd.time_range_entry.instantaneous.last_valid_datetime == gd.
          time_range_entry.bounded.first_valid_datetime) {
        gd.time_range_entry.unit = 3;
      } else if ((gd.time_range_entry.instantaneous.last_valid_datetime).
          months_since(gd.time_range_entry.bounded.first_valid_datetime)
          == 3) {
        gd.time_range_entry.unit = 2;
      } else if ((gd.time_range_entry.instantaneous.last_valid_datetime).
          months_since(gd.time_range_entry.bounded.first_valid_datetime)
          == 1) {
        gd.time_range_entry.unit = 1;
      } else {
        log_error2("unable to determine climatology unit", F, g_util_ident);
      }

      // COARDS convention for climatology over all-available years
      if ((gd.time_range_entry.instantaneous.first_valid_datetime).year()
          == 0) {
        gd.time_range_entry.key = 0x7fffffff;
      }
    }
  }
  return tbnds;
}

void set_month_end_date(GridData& gd, string calendar) {
  auto& i = gd.time_range_entry.instantaneous;
  if (i.first_valid_datetime.day() == 1 && i.first_valid_datetime.time() == 0) {
    i.last_valid_datetime.add_seconds(dateutils::days_in_month(i.
        last_valid_datetime.year(), i.last_valid_datetime.month(), calendar) *
        86400 - 1, calendar);
  }
  auto& b = gd.time_range_entry.bounded;
  if (!gd.time_bounds.id.empty()) {
    if (b.first_valid_datetime.day() == 1) {
      b.last_valid_datetime.add_days(dateutils::days_in_month(b.
          last_valid_datetime.year(), b.last_valid_datetime.month(), calendar) -
          1, calendar);
    }
  } else if (!gd.climo_bounds.id.empty()) {
    if (b.first_valid_datetime.day() == b.last_valid_datetime.day() && b.
        first_valid_datetime.time() == 0 && b.last_valid_datetime.time() == 0) {
      b.last_valid_datetime.subtract_seconds(1);
    }
  }
}

bool found_alternate_lat_lon_coordinates(InputHDF5Stream& istream, GridData&
     grid_data, vector<string>& lat_ids, vector<string>& lon_ids) {
  vector<string> compass{"north", "east"};
  unordered_map<string, string> dim_map[compass.size()];
  for (size_t n = 0; n < compass.size(); ++n) {
    auto vars = istream.datasets_with_attribute("units=degrees_" + compass[n]);
    if (vars.empty()) {
      vars = istream.datasets_with_attribute("units=degree_" + compass[n]);
    }
    for (const auto& v : vars) {
      auto& var_name = v.first;
      auto test_vars = istream.datasets_with_attribute("bounds=" + var_name);
      if (test_vars.empty()) {
        if (gatherxml::verbose_operation) {
          cout << "   ...found '" << var_name << "'" << endl;
        }
        stringstream dimension_list;
        v.second->attributes["DIMENSION_LIST"].print(dimension_list, istream.
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

bool grid_is_polar_stereographic(const GridData& grid_data,
    Grid::GridDimensions& dim, Grid::GridDefinition& def) {
  auto center_x = dim.x / 2;
  auto center_y = dim.y / 2;
  auto xm = center_x - 1;
  auto ym = center_y - 1;
  if (floatutils::myequalf(data_array_value(grid_data.latitude.data_array,
      ym * dim.x + xm, grid_data.latitude.ds.get()), data_array_value(
      grid_data.latitude.data_array, center_y * dim.x + xm,
      grid_data.latitude.ds.get()), 0.00001) && floatutils::myequalf(
      data_array_value(grid_data.latitude.data_array, center_y * dim.x + xm,
      grid_data.latitude.ds.get()), data_array_value(
      grid_data.latitude.data_array, center_y * dim.x + center_x,
      grid_data.latitude.ds.get()), 0.00001) && floatutils::myequalf(
      data_array_value(grid_data.latitude.data_array, center_y * dim.x +
      center_x, grid_data.latitude.ds.get()), data_array_value(
      grid_data.latitude.data_array, ym * dim.x + center_x,
      grid_data.latitude.ds.get()), 0.00001) && floatutils::myequalf(
      fabs(data_array_value(grid_data.longitude.data_array, ym * dim.x + xm,
      grid_data.longitude.ds.get())) + fabs(data_array_value(
      grid_data.longitude.data_array, center_y * dim.x + xm,
      grid_data.longitude.ds.get())) + fabs(data_array_value(
      grid_data.longitude.data_array, center_y * dim.x + center_x,
      grid_data.longitude.ds.get())) + fabs(data_array_value(
      grid_data.longitude.data_array, ym * dim.x + center_x,
      grid_data.longitude.ds.get())), 360., 0.00001)) {
    def.type = Grid::Type::polarStereographic;
    if (data_array_value(grid_data.latitude.data_array, ym * dim.x + xm,
        grid_data.latitude.ds.get()) >= 0.) {
      def.projection_flag = 0;
      def.llatitude = 60.;
    } else {
      def.projection_flag = 1;
      def.llatitude = -60.;
    }
    def.olongitude = lroundf(data_array_value(grid_data.longitude.data_array,
        ym * dim.x + xm, grid_data.longitude.ds.get()) + 45.);
    if (def.olongitude > 180.) {
      def.olongitude -= 360.;
    }

    // look for dx and dy at the 60-degree parallel
    double min_fabs = 999.;
    int min_m = 0;
    for (size_t m = 0; m < grid_data.latitude.data_array.num_values; ++m) {
      auto f = fabs(def.llatitude-data_array_value(
          grid_data.latitude.data_array, m, grid_data.latitude.ds.get()));
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
        grid_data.latitude.data_array, min_m, grid_data.latitude.ds.get()) -
        data_array_value(grid_data.latitude.data_array, min_m + 1,
        grid_data.latitude.ds.get())) / 2. * rad) * sin(fabs(
        data_array_value(grid_data.latitude.data_array, min_m,
        grid_data.latitude.ds.get()) - data_array_value(
        grid_data.latitude.data_array, min_m + 1,
        grid_data.latitude.ds.get())) / 2. * rad) + sin(fabs(data_array_value(
        grid_data.longitude.data_array, min_m, grid_data.longitude.ds.get()) -
        data_array_value(grid_data.longitude.data_array, min_m + 1,
        grid_data.longitude.ds.get())) / 2. * rad) * sin(fabs(
        data_array_value(grid_data.longitude.data_array, min_m,
        grid_data.longitude.ds.get()) - data_array_value(
        grid_data.longitude.data_array, min_m + 1,
        grid_data.longitude.ds.get())) / 2. * rad) * cos(data_array_value(
        grid_data.latitude.data_array, min_m, grid_data.latitude.ds.get()) *
        rad) * cos(data_array_value(grid_data.latitude.data_array, min_m + 1,
        grid_data.latitude.ds.get()) * rad))) * 12745.6);
    def.dy = lroundf(asin(sqrt(sin(fabs(data_array_value(
        grid_data.latitude.data_array, min_m, grid_data.latitude.ds.get()) -
        data_array_value(grid_data.latitude.data_array, min_m + dim.x,
        grid_data.latitude.ds.get())) / 2. * rad) * sin(fabs(data_array_value(
        grid_data.latitude.data_array, min_m, grid_data.latitude.ds.get()) -
        data_array_value(grid_data.latitude.data_array, min_m + dim.x,
        grid_data.latitude.ds.get())) / 2. * rad) + sin(fabs(data_array_value(
        grid_data.longitude.data_array, min_m, grid_data.longitude.ds.get()) -
        data_array_value(grid_data.longitude.data_array, min_m + dim.x,
        grid_data.longitude.ds.get())) / 2. * rad) * sin(fabs(data_array_value(
        grid_data.longitude.data_array, min_m, grid_data.longitude.ds.get()) -
        data_array_value(grid_data.longitude.data_array, min_m + dim.x,
        grid_data.longitude.ds.get())) / 2. * rad) * cos(data_array_value(
        grid_data.latitude.data_array, min_m, grid_data.latitude.ds.get()) *
        rad) * cos(data_array_value(grid_data.latitude.data_array, min_m +
        dim.x, grid_data.latitude.ds.get()) * rad))) * 12745.6);
    return true;
  }
  return false;
}

bool grid_is_centered_lambert_conformal(const GridData& grid_data, Grid::
    GridDimensions& dims, Grid::GridDefinition& def) {
  auto dx2 = dims.x / 2;
  auto dy2 = dims.y / 2;
  switch (dims.x % 2) {
    case 0: {
      auto xm = dx2 - 1;
      auto ym = dy2 - 1;
      auto yp = dy2 + 1;
      if (floatutils::myequalf((data_array_value(grid_data.longitude.data_array,
          ym * grid_data.longitude.data_array.dimensions[1] + xm, grid_data.
          longitude.ds.get()) + data_array_value(grid_data.longitude.data_array,
          ym * grid_data.longitude.data_array.dimensions[1] + dx2, grid_data.
          longitude.ds.get())), (data_array_value(grid_data.longitude.data_array,
          yp * grid_data.longitude.data_array.dimensions[1] + xm, grid_data.
          longitude.ds.get()) + data_array_value(grid_data.longitude.data_array,
          yp * grid_data.longitude.data_array.dimensions[1] + dx2, grid_data.
          longitude.ds.get())), 0.00001) && floatutils::myequalf(
          data_array_value(grid_data.latitude.data_array, dy2 * grid_data.
          longitude.data_array.dimensions[1] + xm, grid_data.latitude.ds.get()),
          data_array_value(grid_data.latitude.data_array, dy2 * grid_data.
          longitude.data_array.dimensions[1] + dx2, grid_data.latitude.ds.
          get()))) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(
            data_array_value(grid_data.latitude.data_array, dy2 * grid_data.
            longitude.data_array.dimensions[1] + dx2, grid_data.latitude.ds.
            get()));
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround((data_array_value(grid_data.longitude.data_array,
            dy2 * grid_data.longitude.data_array.dimensions[1] + xm, grid_data.
            longitude.ds.get()) + data_array_value(grid_data.longitude.
            data_array, dy2 * grid_data.longitude.data_array.dimensions[1] + dx2,
             grid_data.longitude.ds.get())) / 2.);
        def.dx = def.dy = lround(111.1 * cos(data_array_value(grid_data.latitude.
            data_array, dy2 * grid_data.longitude.data_array.dimensions[1] + dx2
            - 1, grid_data.latitude.ds.get()) * 3.141592654/180.) *
            (data_array_value(grid_data.longitude.data_array, dy2 * grid_data.
            longitude.data_array.dimensions[1] + dx2, grid_data.longitude.ds.
            get()) - data_array_value(grid_data.longitude.data_array, dy2 *
            grid_data.longitude.data_array.dimensions[1] + dx2 - 1, grid_data.
            longitude.ds.get())));
        if (gatherxml::verbose_operation) {
          cout << "...confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
    case 1: {
      auto xp = dx2 + 1;
      if (floatutils::myequalf(data_array_value(grid_data.longitude.data_array,
          (dy2 - 1) * grid_data.longitude.data_array.dimensions[1] + dx2,
          grid_data.longitude.ds.get()), data_array_value(grid_data.longitude.
          data_array, (dy2 + 1) * grid_data.longitude.data_array.dimensions[1] +
          dx2, grid_data.longitude.ds.get()), 0.00001) && floatutils::myequalf(
          data_array_value(grid_data.latitude.data_array, dy2 * grid_data.
          longitude.data_array.dimensions[1] + dx2 - 1, grid_data.latitude.ds.
          get()), data_array_value(grid_data.latitude.data_array, dy2 *
          grid_data.longitude.data_array.dimensions[1] + xp, grid_data.latitude.
          ds.get()), 0.00001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(
            data_array_value(grid_data.latitude.data_array, dy2 * grid_data.
            longitude.data_array.dimensions[1] + dx2, grid_data.latitude.ds.
            get()));
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround(data_array_value(grid_data.longitude.data_array,
            dy2 * grid_data.longitude.data_array.dimensions[1] + dx2, grid_data.
            longitude.ds.get()));
        def.dx = def.dy = lround(111.1*cos(data_array_value(grid_data.latitude.
            data_array, dy2 * grid_data.longitude.data_array.dimensions[1] + dx2,
            grid_data.latitude.ds.get()) * 3.141592654 / 180.) *
            (data_array_value(grid_data.longitude.data_array, dy2 * grid_data.
            longitude.data_array.dimensions[1] + dx2 + 1, grid_data.longitude.ds.
            get()) - data_array_value(grid_data.longitude.data_array, dy2 *
            grid_data.longitude.data_array.dimensions[1] + dx2, grid_data.
            longitude.ds.get())));
        if (gatherxml::verbose_operation) {
          cout << "...confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
  }
  return false;
}

bool grid_is_non_centered_lambert_conformal(const GridData& grid_data,
    Grid::GridDimensions& dim, Grid::GridDefinition& def) {

  // try to detect a non-centered LC projection
  // find the x-offsets in each row where the change in latitude goes from
  //     positive to negative
  vector<double> olon_x_offsets;
  olon_x_offsets.reserve(dim.y);
  double avg_olon = 0.;
  for (auto n = 0; n < dim.y; ++n) {
    auto off = n*dim.x;
    for (auto m = 0; m < dim.x - 1; ++m) {
      auto x_offset = off + m;
      if (data_array_value(grid_data.latitude.data_array, x_offset + 1,
          grid_data.latitude.ds.get()) <= data_array_value(
          grid_data.latitude.data_array, x_offset,
          grid_data.latitude.ds.get())) {
        olon_x_offsets.emplace_back(x_offset - off);
        avg_olon += data_array_value(grid_data.longitude.data_array, x_offset,
            grid_data.longitude.ds.get());
        break;
      }
    }
  }

  // find the variance in the x-offsets
  auto olon_x_offsets_mean = lround(std::accumulate(olon_x_offsets.begin(),
      olon_x_offsets.end(), 0.) / olon_x_offsets.size());
  double ss = 0.;
  for (const auto& olon_x_offset : olon_x_offsets) {
    auto diff = olon_x_offset - olon_x_offsets_mean;
    ss += diff*diff;
  }
  auto var = ss / (olon_x_offsets.size() - 1);
// if the variance is low, confident that we found the orientation longitude
  if (var < 1.) {
    def.type = Grid::Type::lambertConformal;
    auto olon_x_offset = lround(std::accumulate(olon_x_offsets.begin(),
        olon_x_offsets.end(), 0.) / olon_x_offsets.size());
    def.olongitude = lround(avg_olon / olon_x_offsets.size());
    // find the x-direction distance for each row at the orientation longitude
    const double PI = 3.141592654;
    const double DEGRAD = PI / 180.;
    vector<double> dist_x_list;
    dist_x_list.reserve(dim.y);
    for (auto n = olon_x_offset; n < dim.x * dim.y; n +=  dim.x) {
      auto dist_lon = (data_array_value(grid_data.longitude.data_array, n + 1,
          grid_data.longitude.ds.get())-data_array_value(
          grid_data.longitude.data_array, n, grid_data.longitude.ds.get())) *
          111.1 * cos(data_array_value(grid_data.latitude.data_array, n,
          grid_data.latitude.ds.get()) * DEGRAD);
      auto dist_lat = (data_array_value(grid_data.latitude.data_array, n + 1,
          grid_data.latitude.ds.get())-data_array_value(
          grid_data.latitude.data_array, n, grid_data.latitude.ds.get())) *
          111.1;
      dist_x_list.emplace_back(sqrt(dist_lon * dist_lon + dist_lat * dist_lat));
    }
    def.dx = lround(std::accumulate(dist_x_list.begin(), dist_x_list.end(),
        0.) / dist_x_list.size());
    def.stdparallel1 = def.stdparallel2 = -99.;
    size_t first_dy_index = 0;
    for (size_t n = 0; n < dist_x_list.size(); ++n) {
      if (floatutils::myequalf(dist_x_list[n], def.dx, 0.001)) {
        auto tanlat = lround(data_array_value(grid_data.latitude.data_array,
            olon_x_offset + n * dim.x, grid_data.latitude.ds.get()));
        if (def.stdparallel1 < -90.) {
          def.stdparallel1 = tanlat;
          first_dy_index = olon_x_offset + n * dim.x;
        } else if (def.stdparallel2 < -90.) {
          if (tanlat != def.stdparallel1) {
            def.stdparallel2 = tanlat;
          }
        } else if (tanlat != def.stdparallel2) {
          if (gatherxml::verbose_operation) {
            cout << "...check for a non-centered projection failed. Too "
                "many tangent latitudes." << endl;
          }
          return false;
        }
      }
    }
    if (def.stdparallel1 < -90.) {
      if (gatherxml::verbose_operation) {
        cout << "...check for a non-centered projection failed. No "
            "tangent latitude could be identified." << endl;
      }
      return false;
    } else if (def.stdparallel2 < -90.) {
      def.stdparallel2 = def.stdparallel1;
    }
    def.llatitude = def.stdparallel1;
    if (def.llatitude >= 0.) {
      def.projection_flag = 0;
    } else {
      def.projection_flag = 1;
    }
    auto dist_lon = (data_array_value(grid_data.longitude.data_array,
        first_dy_index, grid_data.longitude.ds.get()) - data_array_value(
        grid_data.longitude.data_array, first_dy_index - dim.x,
        grid_data.longitude.ds.get())) * 111.1 * cos(data_array_value(
        grid_data.latitude.data_array, first_dy_index - dim.x,
        grid_data.latitude.ds.get()) * DEGRAD);
    auto dist_lat = (data_array_value(grid_data.latitude.data_array,
        first_dy_index, grid_data.latitude.ds.get()) - data_array_value(
        grid_data.latitude.data_array, first_dy_index - dim.x,
        grid_data.latitude.ds.get())) * 111.1;
    def.dy = lround(sqrt(dist_lon*dist_lon + dist_lat*dist_lat));
    if (gatherxml::verbose_operation) {
      cout << "...confirmed a non-centered Lambert-Conformal "
          "projection." << endl;
    }
    return true;
  }
  return false;
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
  if (gatherxml::verbose_operation) {
    cout << "...check for a centered Lambert-Conformal projection failed, "
        "checking for a non-centered projection..." << endl;
  }
  if (grid_is_non_centered_lambert_conformal(grid_data, dim, def)) {
    return true;
  }
  if (gatherxml::verbose_operation) {
    cout << "...check for a Lambert-Conformal projection finished. Not an LC "
        "projection." << endl;
  }
  return false;
}

void scan_gridded_hdf5nc4_file(InputHDF5Stream& istream, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "...beginning function scan_gridded_hdf5nc4_file()..." <<
        endl;
  }
  grid_initialize();

  // open a file inventory unless this is a test run
  if (metautils::args.dsnum < "999.0") {
    gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
        "hdf2xml", USER);
  }
  scan_data.map_name = unixutils::remote_web_file(
      "https://rda.ucar.edu/metadata/ParameterTables/netCDF4.ds" +
      metautils::args.dsnum + ".xml", scan_data.tdir->name());
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
  find_coordinate_variables(istream, coord_vars, grid_data);

  // if the reference and valid times are different, look for forecasts
  if (grid_data.reference_time.id != grid_data.valid_time.id) {
    if (gatherxml::verbose_operation) {
      cout << "...checking for forecasts..." << endl;
    }
    check_for_forecasts(istream, grid_data, coord_vars.nc_time);
  }

  // if no lat/lon coordinate variables found, look for alternates
  if (coord_vars.lat_ids.empty() && coord_vars.lon_ids.empty()) {
    if (gatherxml::verbose_operation) {
      cout << "...looking for alternate latitude and longitude coordinates..."
          << endl;
    }
    if (!found_alternate_lat_lon_coordinates(istream, grid_data, coord_vars.
        lat_ids, coord_vars.lon_ids)) {
      cerr << "Terminating - could not find any latitude/longitude coordinates"
          << endl;
      exit(1);
    }
  }
  if (coord_vars.lat_ids.size() != coord_vars.lon_ids.size()) {
    cerr << "Terminating - unequal number of latitude and longitude coordinate "
        "variables" << endl;
    exit(1);
  }
  if (coord_vars.level_info.empty()) {
    find_vertical_level_coordinates(istream, coord_vars, grid_data);
  }

  // add a surface level
  coord_vars.level_info.emplace_back();
  coord_vars.level_info.back().ID = "sfc";
  coord_vars.level_info.back().description = "Surface";
  coord_vars.level_info.back().units = "";
  coord_vars.level_info.back().write = false;
  if (grid_data.valid_time.id.empty()) {
    cerr << "Terminating - no time coordinate found" << endl;
    exit(1);
  }
  if (gatherxml::verbose_operation) {
    cout << "...found time ('" << grid_data.valid_time.id << "'), latitude ('"
        << vector_to_string(coord_vars.lat_ids) << "'), and longitude ('" <<
        vector_to_string(coord_vars.lon_ids) << "') coordinates..." << endl;
  }
  grid_data.time_range_entry.key = -1;
  grid_data.valid_time.ds = istream.dataset("/" + grid_data.valid_time.id);
  if (grid_data.valid_time.ds == nullptr) {
    log_error2("unable to access the /" + grid_data.valid_time.id + " dataset "
        "for the data temporal range", F, g_util_ident);
  }
  if (!grid_data.valid_time.data_array.fill(istream, *grid_data.valid_time.
      ds)) {
    auto error = move(myerror);
    log_error2("unable to fill time array - error: '" + error + "'", F,
        g_util_ident);
  }
  metautils::NcTime::Time nctime;
  nctime.t1 = data_array_value(grid_data.valid_time.data_array, 0, grid_data.
      valid_time.ds.get());
  nctime.t2 = data_array_value(grid_data.valid_time.data_array, grid_data.
      valid_time.data_array.num_values - 1, grid_data.valid_time.ds.get());
  string e;
  grid_data.time_range_entry.instantaneous.first_valid_datetime = metautils::
      NcTime::actual_date_time(nctime.t1, *coord_vars.nc_time, e);
  if (!e.empty()) log_error2(e, F, g_util_ident);
  grid_data.time_range_entry.instantaneous.last_valid_datetime = metautils::
      NcTime::actual_date_time(nctime.t2, *coord_vars.nc_time, e);
  if (!e.empty()) log_error2(e, F, g_util_ident);
  get_forecast_data(istream, grid_data);
  grid_data.time_range_entry.num_steps = grid_data.valid_time.data_array.
      num_values;
  auto tm_bnds = time_bounds(istream, *coord_vars.nc_time, grid_data);
  if (coord_vars.nc_time->units == "months") {
    set_month_end_date(grid_data, coord_vars.nc_time->calendar);
  }
  vector<metautils::NcTime::TimeRangeEntry> time_ranges;
  if (static_cast<int>(grid_data.time_range_entry.key) == -1 &&
      grid_data.forecast_period.data_array.num_values > 0) {
    for (size_t n = 0; n < grid_data.forecast_period.data_array.num_values;
        ++n) {
      auto tre = grid_data.time_range_entry;
      tre.key = data_array_value(grid_data.forecast_period.data_array, n,
          grid_data.forecast_period.ds.get());
      tre.instantaneous.first_valid_datetime.add(coord_vars.forecast_period->
          units, tre.key);
      tre.instantaneous.last_valid_datetime.add(coord_vars.forecast_period->
          units, tre.key);
      if (tre.key == 0) {
        tre.key = -1;
      } else {
        tre.key = -(tre.key * 100);
      }
      time_ranges.emplace_back(tre);
    }
  } else {
    time_ranges.emplace_back(grid_data.time_range_entry);
  }
  for (size_t n = 0; n < coord_vars.lat_ids.size(); ++n) {
    grid_data.latitude.id = coord_vars.lat_ids[n];
    grid_data.latitude.ds = istream.dataset("/" + coord_vars.lat_ids[n]);
    if (grid_data.latitude.ds == nullptr) {
      log_error2("unable to access the /" + coord_vars.lat_ids[n] + " dataset "
          "for the latitudes", F, g_util_ident);
    }
    grid_data.latitude.data_array.fill(istream, *grid_data.latitude.ds);
    Grid::GridDefinition def;
    def.slatitude = data_array_value(grid_data.latitude.data_array, 0,
        grid_data.latitude.ds.get());
    grid_data.longitude.id = coord_vars.lon_ids[n];
    grid_data.longitude.ds = istream.dataset("/" + coord_vars.lon_ids[n]);
    if (grid_data.longitude.ds == nullptr) {
      log_error2("unable to access the /" + coord_vars.lon_ids[n] + " dataset "
          "for the latitudes", F, g_util_ident);
    }
    grid_data.longitude.data_array.fill(istream, *grid_data.longitude.ds);
    def.slongitude = data_array_value(grid_data.longitude.data_array, 0,
        grid_data.longitude.ds.get());
    std::shared_ptr<InputHDF5Stream::Dataset> lat_bounds_ds(nullptr),
        lon_bounds_ds(nullptr);
    HDF5::DataArray lat_bounds_array, lon_bounds_array;
    auto lat_bounds_it = grid_data.latitude.ds->attributes.find("bounds");
    auto lon_bounds_it = grid_data.longitude.ds->attributes.find("bounds");
    if (lat_bounds_it != grid_data.latitude.ds->attributes.end() &&
        lon_bounds_it != grid_data.longitude.ds->attributes.end() &&
        lat_bounds_it->second._class_ == 3 && lon_bounds_it->second._class_ ==
        3) {
      if ( (lat_bounds_ds = istream.dataset("/" + string(
          reinterpret_cast<char *>(lat_bounds_it->second.array)))) !=
          nullptr && (lon_bounds_ds = istream.dataset("/" + string(
          reinterpret_cast<char *>(lon_bounds_it->second.array)))) !=
          nullptr) {
        lat_bounds_array.fill(istream, *lat_bounds_ds);
        lon_bounds_array.fill(istream, *lon_bounds_ds);
      }
    }
    Grid::GridDimensions dim;
    auto lat_attr_it = grid_data.latitude.ds->attributes.find("DIMENSION_LIST");
    auto& lat_dim_list = lat_attr_it->second;
    auto lon_attr_it = grid_data.longitude.ds->attributes.find(
        "DIMENSION_LIST");
    auto& lon_dim_list = lon_attr_it->second;
    if (lat_attr_it != grid_data.latitude.ds->attributes.end() && lon_attr_it !=
        grid_data.longitude.ds->attributes.end() && lat_dim_list.dim_sizes.
        size() == 1 && lat_dim_list.dim_sizes[0] == 2 && lat_dim_list._class_ ==
        9 && lon_dim_list.dim_sizes.size() == 1 && lon_dim_list.dim_sizes[0] ==
        2 && lon_dim_list._class_ == 9) {

      // latitude and longitude variables have one dimension of size 2
      if (lat_dim_list.vlen.class_ == 7 && lon_dim_list.vlen.class_ == 7) {

        // dimensions are variable length values of references to netCDF
        //   dimensions
        unordered_map<size_t, string>::iterator rtp_it[4];
        rtp_it[0] = istream.reference_table_pointer()->find(HDF5::value(
            &lat_dim_list.vlen.buffer[4], lat_dim_list.precision_));
        rtp_it[1] = istream.reference_table_pointer()->find(HDF5::value(
            &lon_dim_list.vlen.buffer[4], lon_dim_list.precision_));
        rtp_it[2] = istream.reference_table_pointer()->find(HDF5::value(
            &lat_dim_list.vlen.buffer[8 + lat_dim_list.precision_],
            lat_dim_list.precision_));
        rtp_it[3] = istream.reference_table_pointer()->find(HDF5::value(
            &lon_dim_list.vlen.buffer[8 + lon_dim_list.precision_],
            lon_dim_list.precision_));
        const auto& rtp_end = istream.reference_table_pointer()->end();
        if (rtp_it[0] != rtp_end && rtp_it[1] != rtp_end && rtp_it[0]->second ==
            rtp_it[1]->second && rtp_it[2] != rtp_end && rtp_it[3] != rtp_end &&
            rtp_it[2]->second == rtp_it[3]->second) {
          auto ds = istream.dataset("/" + rtp_it[0]->second);
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
          ds = istream.dataset("/" + rtp_it[2]->second);
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
      auto it = grid_data.latitude.ds->attributes.find("DIMENSION_LIST");
      if (it == grid_data.latitude.ds->attributes.end()) {
        dim.y = grid_data.latitude.data_array.num_values;
        dim.x = grid_data.longitude.data_array.num_values;
      }
      else {
        stringstream ss;
        it->second.print(ss, istream.reference_table_pointer());
        auto sp = split(ss.str().substr(1, ss.str().length() - 2));
        switch (sp.size()) {
          case 1: {
            dim.y = grid_data.latitude.data_array.num_values;
            dim.x = grid_data.longitude.data_array.num_values;
            break;
          }
          case 2: {
            dim.y = grid_data.latitude.data_array.dimensions[0];
            dim.x = grid_data.latitude.data_array.dimensions[1];
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
      def.elatitude = data_array_value(grid_data.latitude.data_array, grid_data.
          latitude.data_array.num_values - 1, grid_data.latitude.ds.get());
      def.elongitude = data_array_value(grid_data.longitude.data_array,
          grid_data.longitude.data_array.num_values - 1, grid_data.longitude.ds.
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
    for (size_t m = 0; m < coord_vars.level_info.size(); ++m) {
      grid_data.level.id = coord_vars.level_info[m].ID;
      size_t num_levels;
      if (m == (coord_vars.level_info.size() - 1) && grid_data.level.id ==
          "sfc") {
        num_levels = 1;
      } else {
        grid_data.level.ds = istream.dataset("/" + grid_data.level.id);
        if (grid_data.level.ds == nullptr) {
          log_error2("unable to access the /" + grid_data.level.id + " dataset "
              "for level information", F, g_util_ident);
        }
        grid_data.level.data_array.fill(istream, *grid_data.level.ds);
        num_levels = grid_data.level.data_array.num_values;
        auto attr_it = grid_data.level.ds->attributes.find("bounds");
        if (attr_it != grid_data.level.ds->attributes.end() && attr_it->
            second._class_ == 3) {
          string attr_value = reinterpret_cast<char *>(attr_it->second.array);
          grid_data.level_bounds.ds = istream.dataset("/" + attr_value);
          if (grid_data.level_bounds.ds == nullptr) {
            log_error2("unable to get bounds for level '" + grid_data.level.id +
                "'", F, g_util_ident);
          }
          grid_data.time_bounds.data_array.fill(istream, *grid_data.
              level_bounds.ds);
        }
      }
      std::unordered_set<string> grid_entry_set;
      for (const auto& e : time_ranges) {
        grid_data.time_range_entry = e;
        grid_data.time_data = grid_data.forecast_period.id.empty() ?
            coord_vars.nc_time : coord_vars.forecast_period;
        add_gridded_lat_lon_keys(grid_entry_set, dim, def, grid_data, istream);
        for (const auto& grid_entry : grid_entry_set) {
          if (gatherxml::verbose_operation) {
            cout << "...processing grid entry: " << grid_entry << " ..." <<
                endl;
          }
          grid_entry_key = grid_entry;
          auto key_parts = split(grid_entry_key, "<!>");
          auto& product_key = key_parts.back();
          string grid_key;
          if (g_inv.stream.is_open()) {
            grid_key = key_parts[0];
            for (size_t nn = 1; nn < key_parts.size() - 1; ++nn) {
              grid_key += "," + key_parts[nn];
            }
          }
          if (grid_table_ptr->find(grid_entry_key) == grid_table_ptr->end()) {

            // new grid
            grid_entry_ptr->level_table.clear();
            level_entry_ptr->parameter_code_table.clear();
            parameter_entry_ptr->num_time_steps = 0;
            add_gridded_parameters_to_netcdf_level_entry(istream,
                grid_entry_key, grid_data, scan_data, tm_bnds, parameter_data);
            if (!level_entry_ptr->parameter_code_table.empty()) {
              for (size_t l = 0; l < num_levels; ++l) {
                level_entry_key = "ds" + metautils::args.dsnum + "," +
                    grid_data.level.id + ":";
                if (grid_data.level_bounds.ds == nullptr) {
                  auto level_value = (grid_data.level.ds == nullptr) ? 0. :
                      data_array_value(grid_data.level.data_array, l,
                      grid_data.level.ds.get());
                  if (floatutils::myequalf(level_value, static_cast<int>(
                      level_value), 0.001)) {
                    level_entry_key += itos(level_value);
                  } else {
                    level_entry_key += ftos(level_value, 3);
                  }
                } else {
                  auto level_value = data_array_value(
                      grid_data.time_bounds.data_array, l * 2,
                      grid_data.level_bounds.ds.get());
                  if (floatutils::myequalf(level_value, static_cast<int>(
                      level_value), 0.001)) {
                    level_entry_key += itos(level_value);
                  } else {
                    level_entry_key += ftos(level_value, 3);
                  }
                  level_value = data_array_value(
                      grid_data.time_bounds.data_array, l * 2 + 1,
                      grid_data.level_bounds.ds.get());
                  level_entry_key += ":";
                  if (floatutils::myequalf(level_value, static_cast<int>(
                      level_value), 0.001)) {
                    level_entry_key += itos(level_value);
                  } else {
                    level_entry_key += ftos(level_value, 3);
                  }
                }
                grid_entry_ptr->level_table.emplace(level_entry_key,
                    *level_entry_ptr);
                coord_vars.level_info[m].write = 1;
                if (g_inv.stream.is_open()) {
                  update_inventory(g_inv.maps.U[product_key], g_inv.maps.G[
                      grid_key], grid_data);
                }
              }
            }
            if (!grid_entry_ptr->level_table.empty()) {
              grid_table_ptr->emplace(grid_entry_key, *grid_entry_ptr);
            }
           } else {

            // existing grid - needs update
            for (size_t l = 0; l < num_levels; ++l) {
              level_entry_key = "ds" + metautils::args.dsnum + "," +
                  grid_data.level.id + ":";
              auto level_value = (grid_data.level.ds == nullptr) ? 0. :
                  data_array_value(grid_data.level.data_array, l,
                  grid_data.level.ds.get());
              if (floatutils::myequalf(level_value, static_cast<int>(
                  level_value), 0.001)) {
                level_entry_key += itos(level_value);
              } else {
                level_entry_key += ftos(level_value, 3);
              }
              if (grid_entry_ptr->level_table.find(level_entry_key) ==
                  grid_entry_ptr->level_table.end()) {
                level_entry_ptr->parameter_code_table.clear();
                add_gridded_parameters_to_netcdf_level_entry(istream,
                    grid_entry_key, grid_data, scan_data, tm_bnds,
                    parameter_data);
                if (!level_entry_ptr->parameter_code_table.empty()) {
                  grid_entry_ptr->level_table.emplace(level_entry_key,
                      *level_entry_ptr);
                  coord_vars.level_info[m].write = 1;
                }
              } else {
                 update_level_entry(istream, tm_bnds, grid_data, scan_data,
                    parameter_data, coord_vars.level_info[m].write);
              }
              if (coord_vars.level_info[m].write == 1 && g_inv.stream.
                  is_open()) {
                update_inventory(g_inv.maps.U[product_key], g_inv.maps.G[
                    grid_key], grid_data);
              }
            }
            (*grid_table_ptr)[grid_entry_key] = *grid_entry_ptr;
          }
        }
      }
    }
    auto e = metautils::NcLevel::write_level_map(coord_vars.level_info);
    if (!e.empty()) {
      log_error2(e, F, g_util_ident);
    }
  }
  scan_data.write_type = ScanData::GrML_type;
  if (grid_table_ptr->empty()) {
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
g_inv.maps.R.emplace("x", 0);
}
  if (gatherxml::verbose_operation) {
    cout << "...function scan_gridded_hdf5nc4_file() done." << endl;
  }
}

void scan_hdf5nc4_file(InputHDF5Stream& istream, ScanData& scan_data,
    gatherxml::markup::ObML::ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  auto ds = istream.dataset("/");
  if (ds == nullptr) {
    log_error2("unable to access global attributes", F, g_util_ident);
  }
  scan_data.platform_type="unknown";
  auto attr_it = ds->attributes.find("platform");
  if (attr_it != ds->attributes.end()) {
    string platform = reinterpret_cast<char *>(attr_it->second.array);
    if (!platform.empty()) {
      trim(platform);
      MySQL::Server server(metautils::directives.database_server,
          metautils::directives.metadb_username,
          metautils::directives.metadb_password, "");
      if (server) {
        MySQL::LocalQuery query("ObML_platformType", "search.GCMD_platforms",
            "path = '" + platform + "'");
        if (query.submit(server) == 0) {
          MySQL::Row row;
          if (query.fetch_row(row)) {
            scan_data.platform_type = row[0];
          }
        }
      }
    }
  }
  attr_it = ds->attributes.find("featureType");
  if (attr_it != ds->attributes.end()) {
    string feature_type = reinterpret_cast<char *>(attr_it->second.array);
    auto l_feature_type = strutils::to_lower(feature_type);

    // patch for ICOADS netCDF4 IDs, which may be a mix, so ignore case
    attr_it = ds->attributes.find("product_version");
    if (attr_it != ds->attributes.end()) {
      string product_version = reinterpret_cast<char *>(attr_it->second.array);
      if (std::regex_search(product_version, std::regex("ICOADS")) &&
          std::regex_search(product_version, std::regex("netCDF4"))) {
        scan_data.convert_ids_to_upper_case = true;
      }
    }
    if (l_feature_type == "point") {
      scan_cf_point_hdf5nc4_file(istream, scan_data, obs_data);
    } else if (l_feature_type == "timeseries") {
      scan_cf_time_series_hdf5nc4_file(istream, scan_data, obs_data);
    } else if (l_feature_type == "profile") {
      scan_cf_profile_hdf5nc4_file(istream, scan_data, obs_data);
    } else {
      log_error2("featureType '" + feature_type + "' not recognized", F,
          g_util_ident);
    }
  } else {
    scan_gridded_hdf5nc4_file(istream, scan_data);
  }
}

void scan_hdf5_file(std::list<string>& filelist, ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "Beginning HDF5 file scan..." << endl;
  }
  gatherxml::markup::ObML::ObservationData obs_data;
  InputHDF5Stream istream;
  for (const auto& file : filelist) {
    if (gatherxml::verbose_operation) {
      cout << "Scanning file: " << file << endl;
    }
    if (!istream.open(file.c_str())) {
      auto error = move(myerror);
      log_error2(error += " - file: '" + file + "'", F, g_util_ident);
    }
    if (metautils::args.data_format == "ispdhdf5") {
      scan_ispd_hdf5_file(istream, scan_data, obs_data);
    } else if (metautils::args.data_format == "hdf5nc4") {
      scan_hdf5nc4_file(istream, scan_data, obs_data);
    } else if (metautils::args.data_format == "usarrthdf5") {
      scan_usarray_transportable_hdf5_file(istream, scan_data, obs_data);
    } else {
      cerr << "Error: bad data format specified" << endl;
      exit(1);
    }
    istream.close();
  }
  if (scan_data.write_type == ScanData::GrML_type) {
    scan_data.cmd_type = "GrML";
    if (!metautils::args.inventory_only) {
      xml_directory = gatherxml::markup::GrML::write(*grid_table_ptr, "hdf2xml",
          USER);
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
  if (!error.empty()) {
    log_error2(error, F, g_util_ident);
  }
  if (gatherxml::verbose_operation) {
    cout << "HDF5 file scan completed." << endl;
  }
}

void scan_file(ScanData& scan_data) {
  static const string F = this_function_label(__func__);
  if (gatherxml::verbose_operation) {
    cout << "Beginning file scan..." << endl;
  }
  work_file.reset(new TempFile);
  if (!work_file->open(metautils::directives.temp_path)) {
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
      *work_file, *work_dir, &filelist, file_format, error)) {
    log_error2(error + "'", F + ": "
        "prepare_file_for_metadata_scanning()", g_util_ident);
  }
  if (filelist.empty()) {
    filelist.emplace_back(work_file->name());
  }
  scan_data.tdir.reset(new TempDir);
  if (!scan_data.tdir->create(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary directory in " +
        metautils::directives.temp_path, F, g_util_ident);
  }
  if (std::regex_search(metautils::args.data_format, std::regex("hdf4"))) {
    scan_hdf4_file(filelist, scan_data);
  } else if (std::regex_search(metautils::args.data_format, std::regex(
      "hdf5"))) {
    scan_hdf5_file(filelist, scan_data);
  } else {
    cerr << "Error: bad data format specified" << endl;
    exit(1);
  }
  if (gatherxml::verbose_operation) {
    cout << "File scan complete." << endl;
  }
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

void write_map(string prefix, const unordered_map<string, size_t>& map) {
  for (const auto& e : map) {
    g_inv.stream << prefix << "<!>" << e.second << "<!>" << e.first << endl;
  }
}

void write_inventory() {
  if (!g_inv.stream.is_open()) {
    return;
  }
  write_map("U", g_inv.maps.U);
  write_map("G", g_inv.maps.G);
  write_map("L", g_inv.maps.L);
  write_map("P", g_inv.maps.P);
  write_map("R", g_inv.maps.R);
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
  auto arg_delimiter = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv,
      arg_delimiter);
  metautils::read_config("hdf2xml", USER);
  gatherxml::parse_args(arg_delimiter);
  atexit(clean_up);
  metautils::cmd_register("hdf2xml", USER);
  if (!metautils::args.overwrite_only) {
    metautils::check_for_existing_cmd("GrML");
    metautils::check_for_existing_cmd("ObML");
  }
  ScanData scan_data;
  scan_file(scan_data);
  if (!metautils::args.inventory_only) {
    if (metautils::args.update_db) {
      string flags;
      if (!metautils::args.update_summary) {
        flags += " -S ";
      }
      if (!metautils::args.regenerate) {
        flags += " -R ";
      }
      if (!xml_directory.empty()) {
        flags += " -t " + xml_directory;
      }
      if (std::regex_search(metautils::args.path, std::regex(
          "^https://rda.ucar.edu"))) {
        flags += " -wf";
      } else {
        flags += " -f";
      }
      if (scan_data.cmd_type.empty()) {
        log_error2("content metadata type was not specified", F, g_util_ident);
      }
      stringstream oss, ess;
      if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
          metautils::args.dsnum + " " + flags + " " + metautils::args.filename +
          "." + scan_data.cmd_type, oss, ess) < 0) {
        cerr << ess.str() << endl;
      }
    } else if (metautils::args.dsnum == "test" && !xml_directory.empty()) {
      print_output_location(scan_data.write_type);
    }
  }
  write_inventory();
}
