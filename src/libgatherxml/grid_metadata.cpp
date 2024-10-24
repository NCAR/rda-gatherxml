#include <list>
#include <string>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <regex>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <search_pg.hpp>
#ifdef DUMP_QUERIES
#include <timer.hpp>
#endif

using namespace PostgreSQL;
using bitmap::compress_values;
using bitmap::uncompress_values;
using floatutils::myequalf;
using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
using std::cerr;
using std::endl;
using std::make_pair;
using std::pair;
using std::regex;
using std::regex_search;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::tie;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ds_aliases;
using strutils::itos;
using strutils::lltos;
using strutils::split;
using strutils::sql_ready;
using strutils::strand;
using strutils::substitute;
using strutils::to_sql_tuple_string;

namespace gatherxml {

namespace summarizeMetadata {

void summarize_grid_levels(string database, string caller, string user) {
  const static string F = this_function_label(__func__);
  string t, i;
  if (database == "WGrML") {
    t = "_webfiles2";
  } else {
    log_error2("unknown database '" + database + "'", F, caller, user);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("select distinct p.format_code, g.level_type_codes from " +
      postgres_ready(database) + "." + metautils::args.dsid + "_agrids2 as g "
      "left join " + postgres_ready(database) + "." + metautils::args.dsid + t +
      " as p on p.code = g.file_code where p.format_code is not null");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2(q.error() + "' for '" + q.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_set<string> uset;
  for (const auto& row : q) {
    vector<size_t> v;
    uncompress_values(row[1], v);
    for (const auto& lval : v) {
      auto key = row[0] + ", " + itos(lval);
      if (uset.find(key) == uset.end()) {
        uset.emplace(key);
      }
    }
  }
  auto tbl = database + "." + metautils::args.dsid + "_levels";
  auto uflg = strand(3);
  for (const auto& e : uset) {
    if (srv.insert(
          tbl,
          "format_code, level_type_code, uflg",
          e + ", '" + uflg + "'",
          "(format_code, level_type_code) do update set uflg = excluded.uflg"
          ) < 0) {
      log_error2(srv.error() + " while inserting '" + e + "' into " + tbl, F,
          caller, user);
    }
  }
  srv._delete(tbl, "uflg != '" + uflg + "'");
  srv.disconnect();
}


struct GridSummaryEntry {
  struct Data {
    Data() : start(), end(), level_code_set() { }

    long long start, end;
    unordered_set<size_t> level_code_set;
  };
  GridSummaryEntry() : key(), data(nullptr) { }

  string key;
  std::shared_ptr<Data> data;
};

void summarize_grids(string database, string caller, string user, string
    file_id_code) {
  const static string F = this_function_label(__func__);
  string t, i;
  if (database == "WGrML") {
    t = "_webfiles2";
  } else {
    log_error2("unknown database '" + database + "'", F, caller, user);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("select distinct f.code from " + postgres_ready(database) + "." +
      metautils::args.dsid + t + " as p left join " + postgres_ready(database) +
      ".formats as f on f.code = p.format_code");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2(q.error() + " for query '" + q.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  string fc;
  unordered_map<string, string> ff_map;
  if (q.num_rows() == 1) {
    Row r;
    q.fetch_row(r);
    fc = r[0];
  } else {
    q.set("select p.code, f.code from " + postgres_ready(database) + "." +
        metautils::args.dsid + t + " as p left join " + postgres_ready(database)
        + ".formats as f on f.code = p.format_code where f.code is not null");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(srv) < 0) {
      log_error2(q.error() + " for query '" + q.show() + "'", F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    for (const auto& r : q) {
      ff_map.emplace(r[0], r[1]);
    }
  }
  string s;
  if (!file_id_code.empty()) {
    s = "select file_code, time_range_code, grid_definition_code, parameter, "
        "level_type_codes, start_date, end_date from " + postgres_ready(
        database) + "." + metautils::args.dsid + "_grids2 where file_code = " +
        file_id_code;
  } else {
    s = "select file_code, time_range_codes, grid_definition_codes, "
        "parameter, level_type_codes, start_date, end_date from " +
        postgres_ready(database) + "." + metautils::args.dsid + "_agrids2";
  }
  Query sq(s);
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (sq.submit(srv) < 0) {
    log_error2(sq.error() + " for query '" + sq.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << sq.show()
      << endl;
  }
#endif
  unordered_map<string, vector<size_t>> tr_map, gd_map, l_map;
  my::map<GridSummaryEntry> summ_map(99999);
  for (const auto& row : sq) {
    vector<size_t> trv, gdv;
    if (!file_id_code.empty()) {
      trv.emplace_back(stoi(row[1]));
      gdv.emplace_back(stoi(row[2]));
    } else {
      auto tr = tr_map.find(row[1]);
      if (tr == tr_map.end()) {
        uncompress_values(row[1], trv);
        tr_map.emplace(row[1], trv);
      } else {
        trv = tr->second;
      }
      auto gd = gd_map.find(row[2]);
      if (gd == gd_map.end()) {
        uncompress_values(row[2], gdv);
        gd_map.emplace(row[2], gdv);
      } else {
        gdv = gd->second;
      }
    }
    vector<size_t> lv;
    auto l = l_map.find(row[4]);
    if (l == l_map.end()) {
      uncompress_values(row[4], lv);
      l_map.emplace(row[4], lv);
    } else {
      lv = l->second;
    }
    for (const auto& t : trv) {
      for (const auto& g : gdv) {
        GridSummaryEntry gse;
        if (ff_map.size() == 0) {
          gse.key = fc;
        } else {
          auto ff = ff_map.find(row[0]);
          if (ff != ff_map.end()) {
            gse.key = ff->second;
          }
        }
        if (!gse.key.empty()) {
          gse.key += "," + itos(t) + "," + itos(g) + ",'" + row[3] + "'";
          if (!summ_map.found(gse.key, gse)) {
            gse.data.reset(new GridSummaryEntry::Data);
            gse.data->start = stoll(row[5]);
            gse.data->end = stoll(row[6]);
            for (const auto& l : lv) {
              gse.data->level_code_set.emplace(l);
            }
            summ_map.insert(gse);
          } else {
            auto date = stoll(row[5]);
            if (date < gse.data->start) {
              gse.data->start = date;
            }
            date = stoll(row[6]);
            if (date > gse.data->end) {
              gse.data->end = date;
            }
            for (const auto& l : lv) {
              if (gse.data->level_code_set.find(l) == gse.data->level_code_set.
                  end()) {
                gse.data->level_code_set.emplace(l);
              }
            }
          }
        }
      }
    }
  }
  auto tbl = database + ".summary";
  auto lock_it = pglocks::pglocks.find(tbl);
  if (lock_it == pglocks::pglocks.end()) {
    log_error2("can't find lock ID for '" + tbl + "'", F, caller, user);
  }
  Transaction tx;
  tx.start(srv);
  tx.get_lock(lock_it->second + stoi(metautils::args.dsid.substr(1)), 120);
  auto uflg = strand(3);
  auto ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
  vector<size_t> v;
  for (const auto& key : summ_map.keys()) {
    GridSummaryEntry gse;
    summ_map.found(key, gse);
    auto sp = split(gse.key, ",");
    q.set("level_type_codes, start_date, end_date", tbl, "dsid in " + ds_set +
        " and format_code = " + sp[0] + " and time_range_code = " + sp[1] +
        " and grid_definition_code = " + sp[2] + " and parameter = " + sp[3]);
    if (q.submit(srv) < 0) {
      log_error2(q.error(), F, caller, user);
    }
    auto d1 = lltos(gse.data->start);
    auto d2 = lltos(gse.data->end);
    if (q.num_rows() == 1) {
      Row row;
      q.fetch_row(row);
      vector<size_t> v2;
      uncompress_values(row[0], v2);
      for (const auto& e : v2) {
        if (gse.data->level_code_set.find(e) == gse.data->level_code_set.
            end()) {
          gse.data->level_code_set.insert(e);
        }
      }
      d1 = min(row[1], d1);
      d2 = max(row[2], d2);
    }
    v.clear();
    v.reserve(gse.data->level_code_set.size());
    for (const auto& e : gse.data->level_code_set) {
      v.emplace_back(e);
    }
    std::sort(v.begin(), v.end(), std::less<size_t>());
    string b;
    compress_values(v, b);
    if (srv.insert(
          tbl,
          "dsid, format_code, time_range_code, grid_definition_code, "
              "parameter, level_type_codes, start_date, end_date, uflg",
          "'" + metautils::args.dsid + "', " + gse.key + ", '" + b + "', " + d1
              + ", " + d2 + ", '" + uflg + "'",
          "on constraint summary_pkey do update set level_type_codes = "
              "excluded.level_type_codes, start_date = excluded.start_date, "
              "end_date = excluded.end_date, uflg = excluded.uflg"
          ) < 0) {
      log_error2(srv.error() + " while trying to insert ('" + metautils::args.
          dsid + "', " + gse.key + ", '" + b + "', " + d1 + ", " + d2 + ")", F,
          caller, user);
    }
    gse.data->level_code_set.clear();
    gse.data.reset();
  }
  if (file_id_code.empty()) {
    srv._delete(tbl, "dsid in " + ds_set + " and uflg != '" + uflg + "'");
  }
  tx.commit();
  srv.disconnect();
}

struct GridDefinitionEntry {
  GridDefinitionEntry() : key(), definition(), def_params() { }

  string key;
  string definition, def_params;
};

void summarize_grid_resolutions(string caller, string user, string
    file_id_code) {
  static const string F = string(__func__) + "()";
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("code, definition, def_params", "WGrML.grid_definitions");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2(q.error(), F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_map<string, pair<string, string>> gd_map;
  for (const auto& r : q) {
    gd_map.emplace(r[0], make_pair(r[1], r[2]));
  }
  if (!file_id_code.empty()) {
    q.set("select distinct grid_definition_codes from \"WGrML\"." + metautils::
        args.dsid + "_agrids2 where file_code = " + file_id_code);
  } else {
    srv._delete("search.grid_resolutions", "dsid in " + to_sql_tuple_string(
        ds_aliases(metautils::args.dsid)));
    q.set("select distinct grid_definition_codes from \"WGrML\"." + metautils::
        args.dsid + "_agrids2");
  }
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2(q.error() + " for '" + q.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  for (const auto& r : q) {
    vector<size_t> v;
    uncompress_values(r[0], v);
    for (const auto& e : v) {
      string grid_definition, grid_definition_parameters;
      tie(grid_definition, grid_definition_parameters) = gd_map[itos(e)];
      short rtyp = 0;
      double xres = 0., yres = 0., lat1, lat2;
      auto sp = split(grid_definition_parameters, ":");
      if (grid_definition == "gaussLatLon") {
        xres = stof(sp[6]);
        lat1 = stof(sp[2]);
        lat2 = stof(sp[4]);
        yres = stof(sp[1]);
        yres = fabs(lat2 - lat1) / yres;
        rtyp = 0;
      } else if (grid_definition == "lambertConformal" || grid_definition ==
          "polarStereographic") {
        xres = stof(sp[7]);
        yres = stof(sp[8]);
        rtyp = 1;
      } else if (regex_search(grid_definition, regex("^(latLon|mercator)(Cell)"
          "{0,1}$"))) {
        xres = stof(sp[6]);
        yres = stof(sp[7]);
        if (regex_search(grid_definition, regex("^latLon"))) {
          rtyp = 0;
        } else if (regex_search(grid_definition, regex("^mercator"))) {
          rtyp = 1;
        }
      } else if (grid_definition == "sphericalHarmonics") {
        rtyp = 0;
        auto s = sp[0];
        if (s == "42") {
          xres = yres = 2.8;
        } else if (s == "63") {
          xres = yres = 1.875;
        } else if (s == "85") {
          xres = yres = 1.4;
        } else if (s == "106") {
          xres = yres = 1.125;
        } else if (s == "159") {
          xres = yres = 0.75;
        } else if (s == "799") {
          xres = yres = 0.225;
        } else if (s == "1279") {
          xres = yres = 0.125;
        }
      }
      if (myequalf(xres, 0.) && myequalf(yres, 0.)) {
        log_error2("unknown grid definition '" + grid_definition + "'", F,
            caller, user);
      }
      if (yres > xres) {
        xres = yres;
      }
      auto k = searchutils::horizontal_resolution_keyword(xres, rtyp);
      if (!k.empty()) {
        if (srv.insert(
             "search.grid_resolutions",
             "keyword, vocabulary, dsid, origin",
             "'" + k + "', 'GCMD', '" + metautils::args.dsid+"', 'WGrML'",
             "(keyword, vocabulary, dsid, origin) do nothing"
             ) < 0) {
          log_error2(srv.error(), F, caller, user);
        }
      } else {
        log_warning(F+" issued warning: no grid resolution for " +
            grid_definition + ", " + grid_definition_parameters, caller, user);
      }
    }
  }
  srv.disconnect();
}

void exit_on_bitmap_too_long(string file_id_code, string key, string bitmap,
    vector<size_t> values, string F, string caller, string user) {
  stringstream vss;
  for (const auto& value : values) {
    if (!vss.str().empty()) {
      vss << ", ";
    }
    vss << value;
  }
  log_error2("bitmap for time ranges is too long - file_id_code: " +
      file_id_code + " key: \"" + key + "\" bitmap: '" + bitmap + "'\nvalues: "
      + vss.str(), F, caller, user);
}

void add_bitmap(string bitmap, vector<size_t> values, stringstream& ss) {
  ss << values.front() << ", " << values.back() << ", '";
  if (values.size() <= 2) {
    ss << "!" << values.front();
    if (values.size() == 2) {
      ss << "," << values.back();
    }
  } else {
    ss << sql_ready(bitmap);
  }
  ss << "'";
}

void aggregate_grids(string database, string caller, string user, string
    file_id_code) {
  static const string F = this_function_label(__func__);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q;
  if (database == "WGrML") {
    if (!file_id_code.empty()) {
      q.set("select time_range_code, grid_definition_code, parameter, "
          "level_type_codes, min(start_date), max(end_date) from \"WGrML\"." +
          metautils::args.dsid + "_grids2 where file_code = " + file_id_code +
          " group by time_range_code, grid_definition_code, parameter, "
          "level_type_codes order by parameter, level_type_codes, "
          "time_range_code");
    } else {
      q.set("select time_range_code, grid_definition_code, parameter, "
          "level_type_codes, min(start_date), max(end_date) from \"WGrML\"."
          "summary where dsid in " + to_sql_tuple_string(ds_aliases(metautils::
          args.dsid)) + " group by time_range_code, grid_definition_code, "
          "parameter, level_type_codes order by parameter, level_type_codes, "
          "time_range_code");
    }
  } else {
    q.set("");
  }
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2(q.error(), F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
//std::cerr << q.show() << std::endl;
  unordered_set<size_t> gd_set, ds_set;
  vector<size_t> dsv, trv;
  string d1, d2;
  string lkey;
  auto uflg = strand(3);
  for (const auto& r : q) {
    string key = r[2] + "','" + r[3];
    if (key != lkey) {
      if (!lkey.empty()) {
        if (!file_id_code.empty()) {
          vector<size_t> gdv;
          gdv.reserve(gd_set.size());
          for (const auto& e : gd_set) {
            gdv.emplace_back(e);
            if (ds_set.find(e) == ds_set.end()) {
              dsv.emplace_back(e);
              ds_set.emplace(e);
            }
          }
          std::sort(gdv.begin(), gdv.end(), std::less<size_t>());
          stringstream ss;
          ss << file_id_code << ", ";
          string bmap;
          bitmap::compress_values(trv, bmap);
          if (bmap.length() > 255) {
            exit_on_bitmap_too_long(file_id_code, lkey, bmap, trv, F + "[tr2]",
                caller, user);
          }
          add_bitmap(bmap, trv, ss);
          ss << ", ";
          bitmap::compress_values(gdv, bmap);
          if (bmap.length() > 255) {
            exit_on_bitmap_too_long(file_id_code, lkey, bmap, gdv, F + "[gd2]",
                caller, user);
          }
          add_bitmap(bmap, gdv, ss);
          auto sp = split(lkey, "','");
          ss << ", '" << sp[0] << "', ";
          vector<size_t> lv;
          uncompress_values(sp[1], lv);
          add_bitmap(sp[1], lv, ss);
          ss << ", " << d1 << ", " << d2 << ", '" << uflg << "'";
          if (srv.insert(
                database + "." + metautils::args.dsid + "_agrids2",
                "file_code, time_range_first, time_range_last, "
                    "time_range_codes, grid_definition_first, "
                    "grid_definition_last, grid_definition_codes, parameter, "
                    "level_type_first, level_type_last, level_type_codes, "
                    "start_date, end_date, uflg",
                ss.str(),
                "(file_code, parameter, level_type_codes) do update set uflg = "
                    "excluded.uflg"
                ) < 0) {
            log_error2(srv.error() + " while trying to insert '" + ss.str() +
                "' into " + database + "." + metautils::args.dsid + "_agrids2",
                F, caller, user);
          }
        }
        auto tbl = postgres_ready(database) + "." + metautils::args.dsid +
            "_agrids_cache";
        if (srv.insert(
              tbl,
              "parameter, level_type_codes, min_start_date, max_end_date, uflg",
              "'" + lkey + "', " + d1 + ", " + d2 + ", '" + uflg + "'",
              "(parameter, level_type_codes) do update set min_start_date = "
                  "least(excluded.min_start_date, " + tbl + ".min_start_date), "
                  "max_end_date = greatest(excluded.max_end_date, " + tbl +
                  ".max_end_date), uflg = excluded.uflg"
              ) < 0) {
          log_error2(srv.error() + " while trying to insert ('" + lkey + "', " +
              d1 + ", " + d2 + ")", F, caller, user);
        }
      }
      trv.clear();
      gd_set.clear();
      d1 = "999999999999";
      d2 = "000000000000";
    }
    lkey = key;
    trv.emplace_back(stoi(r[0]));
    auto s = stoi(r[1]);
    if (gd_set.find(s) == gd_set.end()) {
      gd_set.emplace(s);
    }
    if (r[4] < d1) {
      d1 = r[4];
    }
    if (r[5] > d2) {
      d2 = r[5];
    }
  }
  if (!d1.empty()) {
    if (!file_id_code.empty()) {
      vector<size_t> gdv;
      gdv.reserve(gd_set.size());
      for (const auto& e : gd_set) {
        gdv.emplace_back(e);
        if (ds_set.find(e) == ds_set.end()) {
          dsv.emplace_back(e);
          ds_set.emplace(e);
        }
      }
      std::sort(gdv.begin(), gdv.end(), std::less<size_t>());
      stringstream ss;
      ss << file_id_code << ", ";
      string bmap;
      bitmap::compress_values(trv, bmap);
      if (bmap.length() > 255) {
        exit_on_bitmap_too_long(file_id_code, lkey, bmap, trv, F + "[tr1]",
            caller, user);
      }
      add_bitmap(bmap, trv, ss);
      ss << ", ";
      bitmap::compress_values(gdv, bmap);
      if (bmap.length() > 255) {
        exit_on_bitmap_too_long(file_id_code, lkey, bmap, gdv, F + "[gd1]",
            caller, user);
      }
      add_bitmap(bmap, gdv, ss);
      auto sp = split(lkey, "','");
      ss << ", '" << sp[0] << "', ";
      vector<size_t> lv;
      uncompress_values(sp[1], lv);
      add_bitmap(sp[1], lv, ss);
      ss << ", " << d1 << ", " << d2 << ", '" << uflg << "'";
      if (srv.insert(
            database + "." + metautils::args.dsid + "_agrids2",
            "file_code, time_range_first, time_range_last, time_range_codes, "
                "grid_definition_first, grid_definition_last, "
                "grid_definition_codes, parameter, level_type_first, "
                "level_type_last, level_type_codes, start_date, end_date, uflg",
            ss.str(),
            "(file_code, parameter, level_type_codes) do update set uflg = "
                "excluded.uflg"
            ) < 0) {
        log_error2(srv.error() + " while trying to insert '" + ss.str() +
            "' into " + database + "." + metautils::args.dsid + "_agrids2", F,
            caller, user);
      }
      std::sort(dsv.begin(), dsv.end(), std::less<size_t>());
      string dsb;
      bitmap::compress_values(dsv, dsb);
      if (srv.insert(
            database + "." + metautils::args.dsid + "_grid_definitions",
            "file_code, grid_definition_codes, uflg",
            file_id_code + ", '" + sql_ready(dsb) + "', '" + uflg + "'",
            "(file_code) do update set grid_definition_codes = excluded."
                "grid_definition_codes, uflg = excluded.uflg"
            ) < 0) {
        log_error2(srv.error() + " while trying to insert (" + file_id_code +
            ", '" + dsb + "')", F, caller, user);
      }
    }
    auto tbl = postgres_ready(database) + "." + metautils::args.dsid +
        "_agrids_cache";
    if (srv.insert(
          tbl,
          "parameter, level_type_codes, min_start_date, max_end_date, uflg",
          "'" + lkey + "', " + d1 + ", " + d2 + ", '" + uflg + "'",
          "(parameter, level_type_codes) do update set min_start_date = least("
              "excluded.min_start_date, " + tbl + ".min_start_date), "
              "max_end_date = greatest(excluded.max_end_date, " + tbl +
              ".max_end_date), uflg = excluded.uflg"
          ) < 0) {
      log_error2(srv.error() + " while trying to insert ('" + lkey + "', " + d1
          + ", " + d2 + ")", F, caller, user);
    }
  }
  if (!file_id_code.empty()) {
    srv._delete("WGrML." + metautils::args.dsid + "_agrids2", "file_code = " +
        file_id_code + " and uflg != '" + uflg + "'");
    srv._delete("WGrML." + metautils::args.dsid + "_grid_definitions",
        "file_code = " + file_id_code + " and uflg != '" + uflg + "'");
  } else {
    srv._delete("WGrML." + metautils::args.dsid + "_agrids_cache", "uflg != '" +
        uflg + "'");
  }
  srv.disconnect();
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
