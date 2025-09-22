#include <fstream>
#include <string>
#include <regex>
#include <unordered_map>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <PostgreSQL.hpp>
#include <bsort.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <search.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using metautils::log_error2;
using miscutils::this_function_label;
using std::endl;
using std::exception;
using std::get;
using std::make_tuple;
using std::move;
using std::regex;
using std::regex_search;
using std::sort;
using std::string;
using std::stringstream;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ds_aliases;
using strutils::ftos;
using strutils::split;
using strutils::strand;
using strutils::substitute;
using strutils::to_sql_tuple_string;

namespace gatherxml {

namespace summarizeMetadata {

void check_point(double latp, double lonp, Server& server, unordered_set<
    string>& location_table) {
  static unordered_map<string, vector<string>> point_cache;
  auto k = ftos(latp) + "," + ftos(lonp);
  if (point_cache.find(k) == point_cache.end()) {
    point_cache.emplace(k, vector<string>());
    auto lat = latp + 0.25;
    double lon;
    size_t cnt = 0;
    string point[9], point2[9];
    for (size_t n = 0; n < 3; ++n) {
      size_t num = 0;
      if (n != 2) {
        num = 2;
        lon = lonp + 0.25;
      } else {
        num = 1;
        lat = latp;
        lon = lonp;
      }
      for (size_t m = 0; m < num; ++m) {
        point[cnt] = "POINT(" + ftos(lat + 90.) + " ";
        point2[cnt] = "";
        if (lon < 0.) {
          point[cnt] += ftos(lon + 360.);
        } else if (lon <= 30.) {
          point2[cnt] = point[cnt];
          point[cnt] += ftos(lon);
          point2[cnt] += ftos(lon + 360.);
        } else {
          point[cnt] += ftos(lon);
        }
        point[cnt] += ")";
        if (!point2[cnt].empty()) {
          point2[cnt] += ")";
        }
        lon -= 0.5;
        ++cnt;
      }
      lat -= 0.5;
    }
    string s;
    for (size_t n = 0; n < cnt; ++n) {
      if (!s.empty()) {
        s += " or ";
      }
      s += "Within(GeomFromText('" + point[n] + "'), bounds) = 1";
      if (!point2[n].empty()) {
        s += " or Within(GeomFromText('" + point2[n] + "'), bounds) = 1";
      }
    }
    Query q("select name, AsText(bounds) from search.political_boundaries "
        "where " + s);
    if (q.submit(server) < 0) {
      myerror = "Error: '" + q.error() + "'";
      exit(1);
    }
//std::cerr << q.show() << endl;
    for (const auto& r : q) {
      if (location_table.find(r[0]) == location_table.end()) {
        for (size_t n = 0; n < cnt; ++n) {
          if (within_polygon(r[1], point[n]) || (point2[n].length() > 0 &&
              within_polygon(r[1], point2[n]))) {
            location_table.emplace(r[0]);
            point_cache[k].emplace_back(r[0]);
            n = cnt;
          }
        }
      }
    }
  } else {
    for (const auto& location : point_cache[k]) {
      if (location_table.find(location) == location_table.end()) {
        location_table.emplace(location);
      }
    }
  }
}

void compress_locations(unordered_set<string>& location_list, my::map<
    ParentLocation>& parent_location_table, vector<string>& sorted_array, string
    caller, string user) {
  Server mysrv(metautils::directives.metadb_config);
  LocalQuery q("select path from search.gcmd_locations where path like "
      "'% > % > %'");
  if (q.submit(mysrv) < 0) {
    myerror = "Error getting GCMD locations";
    exit(1);
  }
  ParentLocation pl;
  for (const auto& r : q) {
    pl.key = r[0];
    while (strutils::occurs(pl.key, " > ") > 1) {
      auto c = pl.key;
      pl.key = pl.key.substr(0, pl.key.rfind(" > "));
      if (!parent_location_table.found(pl.key, pl)) {
        pl.children_set.reset(new unordered_set<string>);
        pl.children_set->emplace(c);
        pl.matched_set = nullptr;
        pl.consolidated_parent_set.reset(new unordered_set<string>);
        parent_location_table.insert(pl);
      } else {
        if (pl.children_set->find(c) == pl.children_set->end()) {
          pl.children_set->emplace(c);
        }
      }
    }
  }
  mysrv.disconnect();

  //special handling for Antarctica since it has no children
  pl.key = "Continent > Antarctica";
  pl.children_set.reset(new unordered_set<string>);
  pl.matched_set = nullptr;
  pl.consolidated_parent_set.reset(new unordered_set<string>);
  parent_location_table.insert(pl);

  // match location keywords to their parents
  for (auto e : location_list) {
    if (strutils::occurs(e, " > ") > 1) {
      pl.key = e.substr(0, e.rfind(" > "));
    } else {
      pl.key = e;
    }
    parent_location_table.found(pl.key, pl);
    if (pl.matched_set == nullptr) {
      pl.matched_set.reset(new unordered_set<string>);
      parent_location_table.replace(pl);
    }
    pl.matched_set->emplace(e);
  }
  sorted_array.clear();
  sorted_array.reserve(parent_location_table.size());
  for (auto& k : parent_location_table.keys()) {
    sorted_array.emplace_back(k);
  }
  binary_sort(sorted_array,
  [](const string& left, const string& right) -> bool {
    if (strutils::occurs(left, " > ") > strutils::occurs(right, " > ")) {
      return true;
    } else if (strutils::occurs(left, " > ") < strutils::occurs(right, " > ")) {
      return false;
    } else {
      if (left <= right) {
        return true;
      }
      return false;
    }
  });

  // continue matching parents to their parents until done
  auto b = true;
  while (b) {
    b = false;
    for (size_t n = 0; n < parent_location_table.size(); ++n) {
      pl.key = sorted_array[n];
      if (strutils::occurs(pl.key, " > ") > 1) {
        parent_location_table.found(pl.key, pl);
        if (pl.matched_set != nullptr) {
          ParentLocation pl2;
          pl2.key = pl.key.substr(0, pl.key.rfind(" > "));
          parent_location_table.found(pl2.key, pl2);
          if (pl2.matched_set == nullptr) {
            pl2.matched_set.reset(new unordered_set<string>);
            parent_location_table.replace(pl2);
          }
          for (const auto& e : *pl.children_set) {
            pl2.children_set->emplace(e);
          }
          for (const auto& e : *pl.matched_set) {
            pl2.matched_set->emplace(e);
          }
          for (const auto& e : *pl.consolidated_parent_set) {
            pl2.consolidated_parent_set->emplace(e);
          }
          pl2.consolidated_parent_set->emplace(pl.key);
          pl.matched_set->clear();
          pl.matched_set.reset();
          pl.matched_set = nullptr;
          parent_location_table.replace(pl);
          b = true;
        }
      }
    }
  }
}

bool summarize_obs_data(string caller, string user) {
  static const string F = this_function_label(__func__);
  auto b = false; // return value
  Server mysrv(metautils::directives.metadb_config);
  Query q("code, format_code", "WObML." + metautils::args.dsid + "_webfiles2");
  if (q.submit(mysrv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
  unordered_map<string, string> fmap;
  for (const auto& r : q) {
    fmap.emplace(r[0], r[1]);
  }
  auto ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
  q.set("format_code, observation_type_code, platform_type_code, box1d_row, "
      "box1d_bitmap", "search.obs_data", "dsid in " + ds_set);
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' while querying search.obs_data", F, caller,
        user);
  }
  unordered_map<string, string> omap;
  for (const auto& r : q) {
    omap.emplace(r[0] + "<!>" + r[1] + "<!>" + r[2] + "<!>" + r[3], r[4]);
  }
  string e;
  q.set("file_code, observation_type_code, platform_type_code, start_date, "
      "end_date, box1d_row, box1d_bitmap", "WObML." + metautils::args.dsid +
      "_locations");
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' while querying WObML." + metautils::args.dsid
        + "_locations", F, caller, user);
  }
  unordered_map<string, tuple<string, string, string>> summary_table;
  for (const auto& r : q) {
    if (fmap.find(r[0]) == fmap.end()) {
      log_error2("found a file_code (" + r[0] + ") in WObML." + metautils::args.
          dsid + "_locations that doesn't exist in WObML." + metautils::args.dsid
          + "_webfiles2", F, caller, user);
    }
    SummaryEntry se;
    se.key=fmap[r[0]] + "<!>" + r[1] + "<!>" + r[2] + "<!>" + r[5];
    if (summary_table.find(se.key) == summary_table.end()) {
      summary_table.emplace(se.key, make_tuple(r[3], r[4], r[6]));
    } else {
      if (r[3] < get<0>(summary_table[se.key])) {
        get<0>(summary_table[se.key]) = r[3];
      }
      if (r[4] > get<1>(summary_table[se.key])) {
        get<1>(summary_table[se.key]) = r[4];
      }
      if (r[6] != get<2>(summary_table[se.key])) {
        get<2>(summary_table[se.key]) = bitmap::add_box1d_bitmaps(get<2>(
            summary_table[se.key]), r[6]);
      }
    }
  }
  for (const auto& e : summary_table) {
    if (omap.find(e.first) != omap.end()) {
      if (get<2>(summary_table[e.first]) != omap[e.first]) {
        b = true;
        break;
      }
    } else {
      b = true;
      break;
    }
  }
  auto uflg = strand(3);
  for (const auto& e : summary_table) {
    auto sp = split(e.first, "<!>");
    if (mysrv.insert(
          "search.obs_data",
          "dsid, format_code, observation_type_code, platform_type_code, "
              "start_date, end_date, box1d_row, box1d_bitmap, uflg",
          "'" + metautils::args.dsid + "', " + sp[0] + ", " + sp[1] + ", " +
              sp[2] + ", " + get<0>(e.second) + ", " + get<1>(e.second) + ", " +
              sp[3] + ", '" + get<2>(e.second) + "', '" + uflg + "'",
          "(dsid, format_code, observation_type_code, platform_type_code, "
              "box1d_row) do update set start_date = excluded.start_date, "
              "end_date = excluded.end_date, box1d_bitmap = excluded."
              "box1d_bitmap, uflg = excluded.uflg"
          ) < 0) {
      log_error2("'" + mysrv.error() + "'", F, caller, user);
    }
  }
  mysrv._delete("search.obs_data", "dsid in " + ds_set + " and uflg != '" + uflg
      + "'");
  summarize_locations("WObML", e);
  if (!e.empty()) {
    log_error2("'" + e + "'", F, caller, user);
  }
  mysrv.disconnect();
  return b;
}

} // end namespace gatherxml::summarizeMetadata

namespace markup {

namespace ObML {

void DataTypeEntry::fill_vertical_resolution_data(vector<double>& level_list,
    string z_positive_direction, string z_units) {
  auto min = 1.e38, max = -1.e38;
  for (size_t n = 0; n < level_list.size(); ++n) {
    if (level_list[n] < min) {
      min = level_list[n];
    }
    if (level_list[n] > max) {
      max = level_list[n];
    }
  }
  if (data->vdata == nullptr) {
    data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::
        VerticalData);
  }
  if (z_positive_direction == "down") {
    sort(level_list.begin(), level_list.end(),
    [](const double& left, const double& right) -> bool {
      if (left > right) {
        return true;
      }
      return false;
    });
    if (data->vdata->min_altitude > 1.e37) {
      data->vdata->min_altitude = -data->vdata->min_altitude;
    }
    if (data->vdata->max_altitude < -1.e37) {
      data->vdata->max_altitude = -data->vdata->max_altitude;
    }
    if (max > data->vdata->min_altitude) {
      data->vdata->min_altitude = max;
    }
    if (min < data->vdata->max_altitude) {
      data->vdata->max_altitude = min;
    }
  } else {
    sort(level_list.begin(), level_list.end(),
    [](const double& left, const double& right) -> bool {
      if (left < right) {
        return true;
      }
      return false;
    });
    if (min < data->vdata->min_altitude) {
      data->vdata->min_altitude = min;
    }
    if (max > data->vdata->max_altitude) {
      data->vdata->max_altitude = max;
    }
  }
  data->vdata->units = z_units;
  data->vdata->avg_nlev += level_list.size();
  auto a = 0.;
  for (size_t n = 1; n < level_list.size(); ++n) {
    a += fabs(level_list[n] - level_list[n - 1]);
  }
  data->vdata->avg_res += (a / (level_list.size() - 1));
  ++data->vdata->res_cnt;
}

ObservationData::ObservationData() : num_types(0), observation_types(),
    observation_indexes(), id_tables(), platform_tables(),
    unique_observation_table(), unknown_id_re("unknown"), unknown_ids(nullptr),
    track_unique_observations(true), is_empty(true) {
  Server mysrv(metautils::directives.metadb_config);
  LocalQuery q("obs_type", "WObML.obs_types");
  if (q.submit(mysrv) == 0) {
    for (const auto& r : q) {
      observation_types.emplace(num_types, r[0]);
      observation_indexes.emplace(r[0], num_types);
      ++num_types;
    }
    for (size_t n = 0; n < num_types; ++n) {
      id_tables.emplace_back(new my::map<IDEntry>(9999));
      platform_tables.emplace_back(new my::map<PlatformEntry>);
    }
  }
  mysrv.disconnect();
}

bool ObservationData::added_to_ids(string observation_type, IDEntry& ientry,
    string data_type, string data_type_map, float lat, float lon, double
    unique_timestamp, DateTime *start_datetime, DateTime *end_datetime) {
  if (lat < -90. || lat > 90. || lon < -180. || lon > 360.) {
    myerror = "latitude or longitude out of range";
    return false;
  }
  auto o = observation_indexes.find(observation_type);
  if (o == observation_indexes.end()) {
    myerror = "no index for observation type '" + observation_type + "'";
    return false;
  }
  const static DateTime base(1000, 1, 1, 0, 0);
  if (unique_timestamp < 0.) {
    unique_timestamp = start_datetime->seconds_since(base);
  }
  auto flon = lon <= 180. ? lon : lon - 360.;
  if (!id_tables[o->second]->found(ientry.key, ientry)) {
    ientry.data.reset(new IDEntry::Data);
    ientry.data->S_lat = ientry.data->N_lat = lat;
    ientry.data->W_lon = ientry.data->E_lon = flon;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (size_t n = 0; n < 360; ++n) {
      ientry.data->min_lon_bitmap[n] = ientry.data->max_lon_bitmap[n] = 999.;
    }
    size_t n, m;
    try {
      geoutils::convert_lat_lon_to_box(1, 0., flon, n, m);
    }
    catch (exception& e) {
      myerror = move(e.what());
      return false;
    }
    ientry.data->min_lon_bitmap[m] = ientry.data->max_lon_bitmap[m] = flon;
    ientry.data->start = *start_datetime;
    if (end_datetime == nullptr) {
      ientry.data->end = ientry.data->start;
    } else {
      ientry.data->end = *end_datetime;
    }
    id_tables[o->second]->insert(ientry);
    DataTypeEntry dte;
    dte.key = data_type;
    dte.data.reset(new DataTypeEntry::Data);
    dte.data->map = data_type_map;
    ientry.data->data_types_table.insert(dte);
    if (track_unique_observations) {
      ientry.data->nsteps = 1;
      dte.data->nsteps = 1;
      stringstream ss;
      ss.setf(std::ios::fixed);
      ss << o->second << ientry.key << unique_timestamp << endl;
      unique_observation_table.emplace(ss.str(), vector<string>{data_type});
    }
  } else {
    if (lat != ientry.data->S_lat || flon != ientry.data->W_lon) {
      if (lat < ientry.data->S_lat) {
        ientry.data->S_lat = lat;
      }
      if (lat > ientry.data->N_lat) {
        ientry.data->N_lat = lat;
      }
      if (flon < ientry.data->W_lon) {
        ientry.data->W_lon = flon;
      }
      if (flon > ientry.data->E_lon) {
        ientry.data->E_lon = flon;
      }
      size_t n, m;
      try {
        geoutils::convert_lat_lon_to_box(1, 0., flon, n, m);
      }
      catch (exception& e) {
        myerror = move(e.what());
        return false;
      }
      if (ientry.data->min_lon_bitmap[m] > 900.) {
        ientry.data->min_lon_bitmap[m] = ientry.data->max_lon_bitmap[m] = flon;
      } else {
        if (flon < ientry.data->min_lon_bitmap[m]) {
          ientry.data->min_lon_bitmap[m] = flon;
        }
        if (flon > ientry.data->max_lon_bitmap[m]) {
          ientry.data->max_lon_bitmap[m] = flon;
        }
      }
    }
    if (*start_datetime < ientry.data->start) {
      ientry.data->start = *start_datetime;
    }
    if (end_datetime == nullptr) {
      if (*start_datetime > ientry.data->end) {
        ientry.data->end = *start_datetime;
      }
    } else {
      if (*end_datetime > ientry.data->end) {
        ientry.data->end = *end_datetime;
      }
    }
    DataTypeEntry dte;
    if (!ientry.data->data_types_table.found(data_type, dte)) {
      dte.key = data_type;
      dte.data.reset(new DataTypeEntry::Data);
      dte.data->map = data_type_map;
      ientry.data->data_types_table.insert(dte);
    }
    if (track_unique_observations) {
      stringstream ss;
      ss.setf(std::ios::fixed);
      ss << o->second << ientry.key << unique_timestamp << endl;
      if (unique_observation_table.find(ss.str()) == unique_observation_table.
          end()) {
        ++ientry.data->nsteps;
        ++dte.data->nsteps;
        unique_observation_table.emplace(ss.str(), vector<string>{data_type});
      } else {
        auto &data_types = unique_observation_table[ss.str()];
        if (std::find(data_types.begin(), data_types.end(), data_type) ==
            data_types.end()) {
          ++(dte.data->nsteps);
          data_types.emplace_back(data_type);
        }
      }
    }
  }
  if (regex_search(ientry.key, unknown_id_re)) {
    if (unknown_ids == nullptr) {
      unknown_ids.reset(new unordered_set<string>);
    }
    unknown_ids->emplace(ientry.key);
  }
  is_empty = false;
  return true;
}

bool ObservationData::added_to_platforms(string observation_type, string
    platform_type, float lat, float lon) {
  if (lat < -90. || lat > 90.) {
    myerror = "latitude '" + ftos(lat) + "' out of range";
    return false;
  }
  if (lon < -180. || lon > 360.) {
    myerror = "longitude '" + ftos(lat) + "' out of range";
    return false;
  }
  auto o = observation_indexes.find(observation_type);
  if (o == observation_indexes.end()) {
    myerror = "no index for observation type '" + observation_type + "'";
    return false;
  }
  PlatformEntry pe;
  if (!platform_tables[o->second]->found(platform_type, pe)) {
    pe.key = platform_type;
    pe.boxflags.reset(new summarizeMetadata::BoxFlags);
    pe.boxflags->initialize(361, 180, 0, 0);
    platform_tables[o->second]->insert(pe);
  }
  if (lat == -90.) {
    pe.boxflags->spole = 1;
  } else if (lat == 90.) {
    pe.boxflags->npole = 1;
  } else {
    size_t n, m;
    auto flon= lon <= 180. ? lon : lon - 360.;
    try {
      geoutils::convert_lat_lon_to_box(1, lat, flon, n, m);
    }
    catch (exception& e) {
      myerror = move(e.what());
      return false;
    }
    pe.boxflags->flags[n - 1][m] = 1;
    pe.boxflags->flags[n - 1][360] = 1;
  }
  return true;
}

} // end namespace gatherxml::markup::ObML

} // end namespace gatherxml::markup

} // end namespace gatherxml
