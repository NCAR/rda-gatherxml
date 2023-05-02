#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <regex>
#include <unordered_set>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>
#include <xml.hpp>
#include <myerror.hpp>
#ifdef DUMP_QUERIES
#include <timer.hpp>
#endif
#include <mutex.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::endl;
using std::make_pair;
using std::make_tuple;
using std::move;
using std::ofstream;
using std::pair;
using std::regex;
using std::regex_search;
using std::sort;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::tie;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::itos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using unixutils::mysystem2;

namespace gatherxml {

namespace summarizeMetadata {

struct CMDDateRange {
  CMDDateRange() : start(), end(), gindex() { }

  string start, end;
  string gindex;
};

CMDDateRange cmd_date_range(string start, string end, string gindex, size_t&
    precision) {
  CMDDateRange d;
  d.start = start;
  if (d.start.length() > precision) {
    precision = d.start.length();
  }
  while (d.start.length() < 14) {
    d.start += "00";
  }
  d.end = end;
  if (d.end.length() > precision) {
    precision = d.end.length();
  }
  while (d.end.length() < 14) {
    if (d.end.length() < 10) {
      d.end += "23";
    } else {
      d.end += "59";
    }
  }
  d.gindex = gindex;
  return d;
}

void cmd_dates(string database, size_t date_left_padding, vector<CMDDateRange>&
     ranges, size_t& precision) {
  static const string F = this_function_label(__func__);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    myerror = "Error: unable to connect to metadata server";
    exit(1);
  }
  string tbl = database + ".ds" + substitute(metautils::args.dsnum, ".", "") +
      "_webfiles2";
  if (table_exists(mysrv, tbl)) {

// patch until all "webfiles2" tables are migrated from webID -> id
MySQL::LocalQuery q;
if (field_exists(mysrv, tbl, "id")) {
q.set("select min(w.start_date), max(w.end_date), wf.gindex from " + tbl + " as w left join dssdb.wfile as wf on wf.wfile = w.id where wf.dsid = 'ds" + metautils::args.dsnum + "' and wf.type = 'D' and wf.status = 'P' and w.start_date > 0 group by wf.gindex");
} else {
q.set("select min(w.start_date), max(w.end_date), wf.gindex from " + tbl + " as w left join dssdb.wfile as wf on wf.wfile = w.webID where wf.dsid = 'ds" + metautils::args.dsnum + "' and wf.type = 'D' and wf.status = 'P' and w.start_date > 0 group by wf.gindex");
}

#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(mysrv) < 0) {
      myerror = "Error (A): " + q.error();
      exit(1);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    MySQL::Row row;
    auto b = q.fetch_row(row);
    if (q.num_rows() < 2 && (!b || row[2] == "0")) {
      auto i = itos(date_left_padding);
      q.set("select lpad(start_date, " + i + ", '0'), lpad(end_date, " + i +
          ", '0'), 0 from " + tbl + " where start_date != 0 order by "
          "start_date, end_date");
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (q.submit(mysrv) < 0) {
        myerror = "Error (B): " + q.error();
        exit(1);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
          show() << endl;
      }
#endif
      if (q.num_rows() > 0) {
        vector<CMDDateRange> v;
        v.reserve(q.num_rows());
        for (const auto& r : q) {
          v.emplace_back(cmd_date_range(r[0], r[1], r[2], precision));
        }
/*
        sort(v.begin(), v.end(),
        [](const CMDDateRange& left, const CMDDateRange& right) -> bool {
          if (left.start <= right.start) {
            return true;
          }
          return false;
        });
*/
        CMDDateRange d;
        d.gindex = "0";
        DateTime sdt(stoll(v[0].start));
        DateTime edt(stoll(v[0].end));
        auto mxdt = edt;
        auto last = v[0].end;
        d.start = v[0].start;
        for (size_t n = 1; n < q.num_rows(); ++n) {
          sdt.set(stoll(v[n].start));
          if (sdt > mxdt && sdt.days_since(mxdt) > 180) {
            d.end = last;
            ranges.emplace_back(d);
            d.start = v[n].start;
          }
          edt.set(stoll(v[n].end));
          if (edt > mxdt) {
            mxdt = edt;
          }
          if (v[n].end > last) {
            last = v[n].end;
          }
        }
        d.end = last;
        ranges.emplace_back(d);
      }
    } else {
      q.rewind();
      for (const auto& r: q) {
        ranges.emplace_back(cmd_date_range(r[0], r[1], r[2], precision));
      }
    }
  }
  mysrv.disconnect();
}

void summarize_dates(string caller, string user) {
  static const string F = this_function_label(__func__);
  vector<CMDDateRange> v;
  size_t precision = 0;
  cmd_dates("WGrML", 12, v, precision);
  cmd_dates("WObML", 8, v, precision);
  cmd_dates("WFixML", 12, v, precision);
//  cmd_dates("SatML", 14, v, precision);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  if (v.size() > 0) {
    precision = (precision - 2) / 2;
    vector<CMDDateRange> v2;
    v2.reserve(v.size());
    auto n = 0;
    auto b = false;
    for (const auto& e : v) {
      v2.emplace_back(e);
      if (v2[n].gindex != "0") {
        b = true;
      }
      ++n;
    }
    if (!b) {
      auto m = 1;
      mysrv._delete("dsperiod", "dsid = 'ds" + metautils::args.dsnum + "'");
      for (n = v.size() - 1; n >= 0; --n, ++m) {
        DateTime sdt(stoll(v2[n].start));
        DateTime edt(stoll(v2[n].end));
        string s = "'ds" + metautils::args.dsnum + "', 0, " + itos(m) + ", '" +
            sdt.to_string("%Y-%m-%d") + "', '" + sdt.to_string("%T") + "', " +
            itos(precision) + ", '" + edt.to_string("%Y-%m-%d") + "', '" + edt.
            to_string("%T") + "', " + itos(precision) + ", '" + sdt.to_string(
            "%Z") + "'";
        if (mysrv.insert("dsperiod", s) < 0) {
          if (!regex_search(mysrv.error(), regex("^Duplicate entry"))) {
            log_error2("'" + mysrv.error() + "' when trying to insert into "
                "dsperiod(1) (" + s + ")", F, caller, user);
          }
        }
      }
    } else {
      sort(v2.begin(), v2.end(),
      [](const CMDDateRange& left, const CMDDateRange& right) -> bool {
        if (left.end > right.end) {
          return true;
        } else if (left.end < right.end) {
          return false;
        } else {
          if (left.gindex <= right.gindex) {
            return true;
          }
          return false;
        }
      });
      mysrv._delete("dsperiod", "dsid = 'ds" + metautils::args.dsnum + "'");
      for (size_t n = 0; n < v.size(); ++n) {
        DateTime sdt(stoll(v2[n].start));
        DateTime edt(stoll(v2[n].end));
        string s = "'ds" + metautils::args.dsnum + "', " + v2[n].gindex + ", " +
            itos(n) + ", '" + sdt.to_string("%Y-%m-%d") + "', '" + sdt.
            to_string("%T") + "', " + itos(precision) + ", '" + edt.to_string(
            "%Y-%m-%d") + "', '" + edt.to_string("%T") + "', " + itos(precision)
            + ", '" + sdt.to_string("%Z") + "'";
        if (mysrv.insert("dsperiod", s) < 0) {
          auto e = mysrv.error();
          auto n = 0;
          while (n < 3 && regex_search(e, regex("^Deadlock"))) {
            e = "";
            sleep(30);
            if (mysrv.insert("dsperiod", s) < 0) {
              e = mysrv.error();
            }
            ++n;
          }
          if (!e.empty() && !regex_search(e, regex("^Duplicate entry"))) {
            log_error2("'" + e + "' when trying to insert into dsperiod(2) (" +
                s + ")", F, caller, user);
          }
        }
      }
    }
  }
  mysrv.disconnect();
}

bool inserted_time_resolution_keyword(MySQL::Server& server, string database,
    string keyword, string& error) {
  if (server.insert("search.time_resolutions", "'" + keyword + "', 'GCMD', '" +
      metautils::args.dsnum + "', '" + database + "'") < 0) {
    auto e = server.error();
    if (!regex_search(e, regex("Duplicate entry"))) {
      e += "\ntried to insert into search.time_resolutions ('" + keyword +
          "', 'GCMD', '" + metautils::args.dsnum + "', '" + database + "')";
      return false;
    }
  }
  return true;
}

void grids_per(size_t nsteps, DateTime start, DateTime end, double& gridsper,
    string& unit) {
  size_t num_days[] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  const float TOLERANCE = 0.15;
  auto h = end.hours_since(start);
  if (floatutils::myequalf(h, 0.) || nsteps <= 1) {
    unit = "singletimestep";
    return;
  }
  if (dateutils::is_leap_year(end.year())) {
    num_days[2] = 29;
  }
  gridsper = (nsteps - 1) / h;
  double test = gridsper;
  if (test + TOLERANCE > 1.) {
    gridsper = test;
    test /= 60.;
    if (test + TOLERANCE < 1.) {
      unit = "hour";
    } else {
      gridsper = test;
      test /= 60.;
      if (test + TOLERANCE < 1.) {
        unit = "minute";
      } else {
        gridsper = test;
        unit = "second";
      }
    }
  } else {
    gridsper *= 24.;
    if (gridsper + TOLERANCE > 1.) {
      unit = "day";
    } else {
      gridsper *= 7.;
      if (gridsper + TOLERANCE > 1.) {
        unit = "week";
      } else {
        gridsper *= num_days[end.month()] / 7.;
        if (gridsper + TOLERANCE > 1.) {
          unit = "month";
        } else {
          gridsper *= 12;
          if (gridsper + TOLERANCE > 1.) {
            unit = "year";
          } else {
            unit = "";
            gridsper = 0.;
          }
        }
      }
    }
  }
}

pair<string, string> gridded_frequency_data(string time_range) {
  pair<string, string> data; // return value
  if (regex_search(time_range, regex("^Pentad"))) {
    data.first = searchutils::time_resolution_keyword("regular", 5, "day", "");
    data.second = "regular<!>5<!>day";
  } else if (regex_search(time_range, regex("^Weekly"))) {
    data.first = searchutils::time_resolution_keyword("regular", 1, "week", "");
    data.second = "regular<!>1<!>week";
  } else if (regex_search(time_range, regex("^Monthly"))) {
    data.first = searchutils::time_resolution_keyword("regular", 1, "month",
        "");
    data.second = "regular<!>1<!>month";
  } else if (regex_search(time_range, regex("^30-year Climatology"))) {
    data.first = searchutils::time_resolution_keyword("climatology", 1, "30-"
        "year", "");
    data.second = "climatology<!>1<!>30-year";
  } else if (regex_search(time_range, regex("-year Climatology"))) {
    auto num_years = time_range.substr(0, time_range.find("-"));
    if (regex_search(time_range, regex("of Monthly"))) {
      data.first = searchutils::time_resolution_keyword("climatology", 1,
          "month", "");
      data.second = "climatology<!>1<!>" + num_years + "-year";
    } else {
      data.second = "<!>1<!>" + num_years + "-year";
    }
  }
  return data;
}

unordered_set<string> summarize_frequencies_from_wgrml_by_dataset(MySQL::Server&
    server, string caller, string user) {
  static const string F = this_function_label(__func__);
  unordered_set<string> sfreq; // return value
  auto ds = substitute(metautils::args.dsnum, ".", "");
  MySQL::LocalQuery q("select distinct frequency_type, nsteps_per, unit from "
      "WGrML.ds" + ds + "_frequencies where nsteps_per > 0");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(server) < 0) {
    log_error2("'" + q.error() + "' for '" + q.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
  server._delete("WGrML.frequencies", "dsid =  '" + metautils::args.dsnum +
      "'");
  for (const auto& r : q) {
    if (server.insert("WGrML.frequencies", "'" + metautils::args.dsnum + "', " +
        r[1] + ", '" + r[2] + "'") < 0) {
      if (!regex_search(server.error(), regex("Duplicate entry"))) {
        log_error2("'" + server.error() + "' while trying to insert '" +
            metautils::args.dsnum + "', " + r[1] + ", '" + r[2] + "'", F,
            caller, user);
      }
    }
    auto k = searchutils::time_resolution_keyword(r[0], stoi(r[1]), r[2], "");
    if (!k.empty() && sfreq.find(k) == sfreq.end()) {
      sfreq.emplace(k);
    }
  }
  q.set("select min(distinct frequency_type), max(distinct frequency_type), "
      "count(distinct frequency_type) from WGrML.ds" + ds + "_frequencies "
      "where nsteps_per = 0");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(server) < 0) {
    log_error2("'" + q.error() + "' for '" + q.show() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
  for (const auto& r : q) {
    if (r[2] != "0") {
      double n;
      string u;
      grids_per(stoi(r[2]), DateTime(stoll(r[0])), DateTime(stoll(r[1])), n, u);
      if (u != "singletimestep") {
        auto l = lround(n);
        if (server.insert("WGrML.frequencies", "'" + metautils::args.dsnum +
            "', " + itos(l) + ", '" + u + "'") < 0) {
          if (!regex_search(server.error(), regex("Duplicate entry"))) {
            log_error2("'" + server.error() + "' while trying to insert '" +
                metautils::args.dsnum + "', " + itos(l) + ", '" + u + "'", F,
                caller, user);
          }
        } else {
          auto k = searchutils::time_resolution_keyword("irregular", l, u, "");
          if (!k.empty() && sfreq.find(k) == sfreq.end()) {
            sfreq.emplace(k);
          }
        }
      }
    }
  }
  server._delete("search.time_resolutions", "dsid = '" + metautils::args.dsnum +
      "' and origin = 'WGrML'");
  return sfreq;
}

unordered_set<string> summarize_frequencies_from_wgrml_by_data_file(MySQL::
    Server& server, string file_id_code, string caller, string user) {
  static const string F = this_function_label(__func__);
  unordered_set<string> sfreq; // return value
  auto ds = substitute(metautils::args.dsnum, ".", "");

  // get all of the time ranges for the given file id
  MySQL::LocalQuery qt("select time_range_code, min(start_date), max("
      "end_date), sum(nsteps) from WGrML.ds" + ds + "_grids2 where file_code = "
      + file_id_code + " group by time_range_code, parameter");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (qt.submit(server) < 0) {
    return sfreq;
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qt.show()
      << endl;
  }
#endif

  // create a map of time range codes and their descriptions
  MySQL::LocalQuery qc("code, time_range", "WGrML.time_ranges");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (qc.submit(server) < 0) {
    log_error2("'" + qc.error() + "' while querying WGrML.time_ranges", F,
        caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qc.show()
      << endl;
  }
#endif
  unordered_map<string, string> map;
  for (const auto& r : qc) {
    map.emplace(r[0], r[1]);
  }

  unordered_set<string> fset, nset, tset;
  size_t cnt = 0;
  string smin = "30001231235959", smax = "10000101000000";
  for (const auto& r : qt) {
    if (r[3] != "1") {
      auto t = map[r[0]];
      string k, f;
      tie(k, f) = gridded_frequency_data(t);
      if (k.empty()) {
        auto dt1 = r[1];
        while (dt1.length() < 14) {
          dt1 += "00";
        }
        auto dt2 = r[2];
        while (dt2.length() < 14) {
          dt2 += "00";
        }
        DateTime d1(stoll(dt1));
        DateTime d2(stoll(dt2));
        if (regex_search(t, regex("Average$")) || regex_search(t, regex(
            "Product$"))) {
          auto u = t;
          if (regex_search(u, regex("of"))) {
            u = u.substr(u.rfind("of") + 3);
          }
          u = u.substr(0, u.find(" "));
          auto n = stoi(u.substr(0, u.find("-")));
          u = u.substr(u.find("-"));
          if (u == "hour") {
            d2.subtract_hours(n);
          } else if (u == "day") {
            d2.subtract_days(n);
          }
        } else if (regex_search(t, regex(" (Accumulation|Average|Product) "
            "\\(initial\\+"))) {
          auto u = t.substr(0, t.find(" "));
          auto idx = u.find("-");
          auto n = stoi(u.substr(0, idx));
          u = u.substr(idx + 1);
          if (u == "hour") {
            d2.subtract_hours(n);
          }
        }
        size_t n;
        double d;
        string u;
        n = stoi(r[3]);
        grids_per(n, d1, d2, d, u);
        if (d > 0.) {
          if (u != "singletimestep") {
            k = searchutils::time_resolution_keyword("irregular", lround(d), u,
                "");
            f = "irregular<!>" + itos(lround(d)) + "<!>" + u;
          } else if (n > 0) {
            if (nset.find(dt1) == nset.end()) {
              if (dt1 < smin) {
                smin = dt1;
              }
              if (dt1 > smax) {
                smax = dt1;
              }
              cnt += n;
              nset.emplace(dt1);
            }
          }
        }
      }
      if (!k.empty() && sfreq.find(k) == sfreq.end()) {
        sfreq.emplace(k);
      }
      if (!f.empty() && fset.find(f) == fset.end()) {
        fset.emplace(f);
      }
    } else {
      auto k = r[0] + "-" + r[1];
      if (nset.find(k) == nset.end()) {
        auto dt = r[1];
        if (dt.length() < 14) {
          dt.append(14 - dt.length(), '0');
        }
        if (dt < smin) {
          smin = dt;
        }
        if (dt > smax) {
          smax = dt;
        }
        ++cnt;
        nset.emplace(k);
        if (tset.find(r[0]) == tset.end()) {
          tset.emplace(r[0]);
        }
      }
    }
  }
  if (cnt > 0) {
    if (tset.size() > 1) {
      cnt /= tset.size();
    }
    double d;
    string u;
    grids_per(cnt, DateTime(stoll(smin)), DateTime(stoll(smax)), d, u);
    if (d > 0. && !u.empty() && u != "singletimestep") {
      auto k = searchutils::time_resolution_keyword("irregular", lround(d), u,
          "");
      if (!k.empty() && sfreq.find(k) == sfreq.end()) {
        sfreq.emplace(k);
      }
      auto f = "irregular<!>" + itos(lround(d)) + "<!>" + u;
      if (!f.empty() && fset.find(f) == fset.end()) {
        fset.emplace(f);
      }
    }
  }
  if (!table_exists(server, "WGrML.ds" + ds + "_frequencies")) {
    string e;
    if (server.command("create table WGrML.ds" + ds + "_frequencies like WGrML."
        "template_frequencies", e) < 0) {
      log_error2("'" + server.error() + "' while creating table WGrML.ds" + ds +
          "_frequencies", F, caller, user);
    }
  } else {
    server._delete("WGrML.ds" + ds + "_frequencies", "file_code = " +
        file_id_code);
  }
  if (fset.size() > 0) {
    for (const auto& e : fset) {
      auto sp = split(e, "<!>");
      if (server.insert("WGrML.frequencies", "'" + metautils::args.dsnum + "', "
          + sp[1] + ", '" + sp[2] + "'") < 0) {
        if (!regex_search(server.error(), regex("Duplicate entry"))) {
          log_error2("'" + server.error() + "' while trying to insert '" +
              metautils::args.dsnum + "', " + sp[1] + ", '" + sp[2] + "'", F,
              caller, user);
        }
      }
      if (server.insert("WGrML.ds" + ds + "_frequencies", file_id_code + ", '" +
          sp[0] + "', " + sp[1] + ", '" + sp[2] + "'") < 0) {
        log_error2("'" + server.error() + "' while trying to insert '" +
            file_id_code + ", '" + sp[0] + "', " + sp[1] + ", '" + sp[2] + "''",
            F, caller, user);
      }
    }
  } else {
    if (server.insert("WGrML.ds" + ds + "_frequencies", file_id_code + ", '" +
        smin + "', 0, ''") < 0) {
      log_error2("'" + server.error() + "' while trying to insert '" +
          file_id_code + ", '" + smin + "', 0, ''", F, caller, user);
    }
  }
  return sfreq;
}

unordered_set<string> summarize_frequencies_from_wgrml(MySQL::Server& server,
    string file_id_code, string caller, string user) {
  if (file_id_code.empty()) {

    // no file id provided, summarize for full dataset
    return move(summarize_frequencies_from_wgrml_by_dataset(server, caller,
        user));
  } else {

    // adding information from a given file id
    return move(summarize_frequencies_from_wgrml_by_data_file(server,
        file_id_code, caller, user));
  }
}

unordered_set<string> summarize_frequencies_from_wobml(MySQL::Server& server,
    string file_id_code, string caller, string user) {
  static const string F = this_function_label(__func__);
  unordered_set<string> fset;
  MySQL::LocalQuery q("select min(min_obs_per), max(max_obs_per), unit from "
      "ObML.ds" + substitute(metautils::args.dsnum, ".", "") + "_frequencies "
      "group by unit");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(server) < 0) {
    return fset;
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  if (file_id_code.empty()) {
    server._delete("search.time_resolutions", "dsid = '" + metautils::args.dsnum
        + "' and origin = 'ObML'");
  }
  for (const auto& r : q) {
    auto n = stoi(r[0]);
    auto u = r[2];
    string s;
    if (n < 0) {
      s = "climatology";
      if (n == -30) {
        u = "30-year";
      }
    } else {
      s = "irregular";
    }
    auto k = searchutils::time_resolution_keyword(s, n, u, "");
    if (fset.find(k) == fset.end()) {
      fset.emplace(k);
    }
    n = stoi(r[1]);
    u = r[2];
    if (n < 0) {
      s = "climatology";
      if (n == -30) {
        u = "30-year";
      }
    } else {
      s = "irregular";
    }
    k = searchutils::time_resolution_keyword(s, n, u, "");
    if (fset.find(k) == fset.end()) {
      fset.emplace(k);
    }
  }
  return fset;
}

void summarize_frequencies(string caller, string user, string file_id_code) {
  vector<pair<string, unordered_set<string>(*)(MySQL::Server&, string, string,
      string)>> v{
    make_pair("WGrML", summarize_frequencies_from_wgrml),
    make_pair("WObML", summarize_frequencies_from_wobml),
  };
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  for (const auto& e : v) {
    auto t = e.first + ".ds" + substitute(metautils::args.dsnum, ".", "") +
        "_frequencies";
    if (MySQL::table_exists(mysrv, t)) {
      auto e2 = e.second(mysrv, file_id_code, caller, user);

      // update searchable time resolution keywords
      for (const auto& k : e2) {
        string err;
        if (!inserted_time_resolution_keyword(mysrv, e.first, k, err)) {
          log_error2("'" + err + "'", "summarize_frequencies()" , caller, user);
        }
      }
    }
  }
  mysrv.disconnect();
}

void summarize_data_formats(string caller, string user) {
  static const string F = this_function_label(__func__);
  auto dblst = metautils::cmd_databases(caller, user);
  if (dblst.size() == 0) {
    log_error2("empty CMD database list", F, caller, user);
  }
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  for (const auto& db : dblst) {
    string nam, typ;
    tie(nam, typ) = db;
    auto t = nam + ".ds" + substitute(metautils::args.dsnum, ".", "") +
        "_webfiles2";
    if (t[0] != 'V' && table_exists(mysrv, t)) {
      MySQL::LocalQuery q("select distinct f.format from " + t + " as p left "
          "join " + nam + ".formats as f on f.code = p.format_code");
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (q.submit(mysrv) < 0) {
        log_error2("'" + q.error() + "'", F, caller, user);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
          show() << endl;
      }
#endif
      string e;
      if (mysrv.command("lock tables search.formats write", e) < 0) {
        log_error2("'" + mysrv.error() + "'", F, caller, user);
      }
      mysrv._delete("search.formats", "dsid = '" + metautils::args.dsnum +
          "' and (vocabulary = '" + nam + "' or vocabulary = 'dssmm')");
      for (const auto& r : q) {
        if (mysrv.insert("search.formats", "keyword, vocabulary, dsid", "'" + r[0] + "', '" + nam + "', '" + metautils::args.dsnum + "'", "") < 0) {
          metautils::log_warning("summarize_data_formats() issued warning: " +
              mysrv.error(), caller, user);
        }
      }
      if (mysrv.command("unlock tables", e) < 0) {
        log_error2("'" + mysrv.error() + "'", F, caller, user);
      }
    }
  }
  mysrv.disconnect();
}

void create_non_cmd_file_list_cache(string file_type, unordered_set<string>&
    files_with_cmd_table, string caller, string user) {
  static const string F = this_function_label(__func__);

  // random sleep to minimize collisions from concurrent processes
  sleep(getpid() % 15);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  if (!mysrv) {
    log_error2("error connecting to database: '" + mysrv.error() + "'", F,
        caller, user);
  }
  MySQL::LocalQuery q("version", "dataset", "dsid = 'ds" + metautils::args.
     dsnum + "'");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' from query: " + q.show(), F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  MySQL::Row row;
  if (!q.fetch_row(row)) {
    log_error2("unable to get dataset version", F, caller, user);
  }
  vector<string> v;
  if (file_type == "Web") {
    q.set("select w.wfile, w.data_format, w.file_format, w.data_size, w.note, "
        "w.gindex from wfile as w where w.dsid = 'ds" + metautils::args.dsnum +
        "' and w.type = 'D' and w.status = 'P'");
  } else {
    q.set("");
  }
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' from query: " + q.show(), F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  for (const auto& r : q) {
    if (files_with_cmd_table.find(r[0]) == files_with_cmd_table.end()) {
      v.emplace_back(r[0] + "<!>" + r[1] + "<!>" + r[2] + "<!>" + r[3] + "<!>" +
          r[4] + "<!>" + r[5]);
    }
  }
  string f;
  if (file_type == "Web") {
    f = "getWebList_nonCMD.cache";
  }
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  if (v.size() > 0) {

    // create the directory tree in the temp directory
    stringstream oss, ess;
    if (mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata", oss, ess) !=
        0) {
      log_error2("unable to create a temporary directory (1) - '" + ess.str() +
          "'", F, caller, user);
    }
    ofstream ofs((tdir.name() + "/metadata/" + f).c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open output for " + f, F, caller, user);
    }
    ofs << row[0] << endl;
    ofs << v.size() << endl;
    for (const auto& e : v) {
      ofs << e << endl;
    }
    ofs.close();
    string e;
    if (unixutils::rdadata_sync(tdir.name(), "metadata/", "/data/web/datasets/"
        "ds" + metautils::args.dsnum, metautils::directives.rdadata_home, e) <
        0) {
      metautils::log_warning("create_non_cmd_file_list_cache() couldn't sync '"
          + f + "' - rdadata_sync error(s): '" + e + "'", caller, user);
    }
  } else {
    string e;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
        dsnum + "/metadata/" + f,  tdir.name(),  metautils::directives.
        rdadata_home, e) < 0) {
      metautils::log_warning("createNonCMDFileCache couldn't unsync '" + f +
          "' - hostSync error(s): '" + e + "'", caller, user);
    }
  }
  mysrv.disconnect();
}

void fill_gindex_map(MySQL::Server& server, Mutex& g_mutex, unique_ptr<
     unordered_map<string, string>>& gindex_map, string caller, string user) {
  static const string F = this_function_label(__func__);
  if (gindex_map == nullptr && !g_mutex.is_locked()) {
    g_mutex.lock();
    MySQL::Query q("select gindex, grpid from dssdb.dsgroup where dsid = 'ds" +
        metautils::args.dsnum + "'");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(server) < 0) {
      log_error2("'" + q.error() + "' while trying to get groups data", F,
          caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
        << endl;
    }
#endif
    gindex_map.reset(new unordered_map<string, string>);
    for (const auto& r : q) {
      gindex_map->emplace(r[0], r[1]);
    }
    g_mutex.unlock();
  } else if (g_mutex.is_locked()) {
    while (g_mutex.is_locked()) { }
  }
}

void fill_data_formats_table(MySQL::Server& server, string db, unordered_map<
    string, string>& data_formats, string caller, string user) {
  static const string F = this_function_label(__func__);
  MySQL::Query q("select code, format from " + db + ".formats");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(server) < 0) {
    log_error2("'" + q.error() + "' while trying to get formats data", F,
        caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  for (const auto& r : q) {
    data_formats.emplace(r[0], r[1]);
  }
}

struct FileEntry {
  FileEntry() : data_format(), units(), metafile_id(), start(), end(),
      file_format(), data_size(), group_id() { }

  string data_format, units, metafile_id;
  string start, end;
  string file_format, data_size;
  string group_id;
};

void get_file_data(MySQL::Server& server, MySQL::Query& query, string unit,
    string unit_plural, unordered_map<string, string>& gindex_map,
    unordered_map<string, string>& data_formats, unordered_map<string,
    FileEntry>& table) {
  for (const auto& row : query) {
    FileEntry fe;
    fe.data_format = data_formats.find(row[1])->second;
    fe.units = row[2] + " " + unit;
    if (row[2] != "1") {
      fe.units += unit_plural;
    }
    fe.start = dateutils::string_ll_to_date_string(row[3]);
    fe.end = dateutils::string_ll_to_date_string(row[4]);
    auto it = gindex_map.find(row[5]);
    if (it != gindex_map.end()) {
      if (!it->second.empty()) {
        fe.group_id = it->second;
      } else {
        fe.group_id = row[5];
      }
      fe.group_id += "[!]" + row[5];
    } else {
      fe.group_id = "";
    }
    fe.file_format = row[6];
    fe.data_size = row[7];
    table.emplace(row[0], fe);
  }
}

void grml_file_data(string file_type, unordered_map<string, string>&
    gindex_map, unordered_map<string, FileEntry>& grml_file_data_table, string&
    caller, string& user) {
  static const string F = this_function_label(__func__);
  auto d = substitute(metautils::args.dsnum, ".", "");
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    log_error2("'" + mysrv.error() + "' while trying to connect", F, caller,
        user);
  }
  static unordered_map<string, string> m;
  if (m.empty()) {
    fill_data_formats_table(mysrv, "WGrML", m, caller, user);
  }
  MySQL::Query q;

// patch until all webfiles2 tables have migrated webID -> id
if (MySQL::field_exists(mysrv, "WGrML.ds" + d + "_webfiles2", "id")) {
q.set("select id, format_code, num_grids, start_date, end_date, gindex, file_format, data_size from WGrML.ds" + d + "_webfiles2 as w left join dssdb.wfile as d on d.wfile = w.id where d.dsid = 'ds" + metautils::args.dsnum + "' and d.type = 'D' and d.status = 'P' and num_grids > 0");
} else {
q.set("select webID, format_code, num_grids, start_date, end_date, gindex, file_format, data_size from WGrML.ds" + d + "_webfiles2 as w left join dssdb.wfile as d on d.wfile = w.webID where d.dsid = 'ds" + metautils::args.dsnum + "' and d.type = 'D' and d.status = 'P' and num_grids > 0");
}

#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' while trying to get metadata file data", F,
        caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  get_file_data(mysrv, q, "Grid", "s", gindex_map, m, grml_file_data_table);
  mysrv.disconnect();
}

void obml_file_data(string file_type, unordered_map<string, string>&
    gindex_map, unordered_map<string,  FileEntry>& obml_file_data_table, string&
    caller, string& user) {
  static const string F = this_function_label(__func__);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  static unordered_map<string, string> map;
  if (map.empty()) {
    fill_data_formats_table(mysrv, "WObML", map, caller, user);
  }
  MySQL::Query q("select id, format_code, num_observations, start_date, "
      "end_date, gindex, file_format, data_size from WObML.ds" + substitute(
      metautils::args.dsnum, ".", "") + "_webfiles2 as w left join dssdb."
      "wfile as d on d.wfile = w.id where d.dsid = 'ds" + metautils::args.dsnum
      + "' and d.type = 'D' and d.status = 'P' and num_observations > 0");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' while trying to get metadata file data", F,
        caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  get_file_data(mysrv, q, "Observation", "s", gindex_map, map,
      obml_file_data_table);
  mysrv.disconnect();
}

void fixml_file_data(string file_type, unordered_map<string, string>&
     gindex_map, unordered_map<string, FileEntry>& fixml_file_data_table, string
     caller, string user) {
  static const string F = this_function_label(__func__);
  FileEntry fe;

  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    log_error2("'" + mysrv.error() + "' while trying to connect", F, caller,
        user);
  }
  static unordered_map<string, string> data_formats;
  if (data_formats.empty()) {
    fill_data_formats_table(mysrv, "WFixML", data_formats, caller, user);
  }
  MySQL::Query q("select webID, format_code, num_fixes, start_date, end_date, "
      "gindex, file_format, data_size from WFixML.ds" + substitute(metautils::
      args.dsnum, ".", "") + "_webfiles2 as w left join dssdb.wfile as d on "
      "d.wfile = w.webID where d.dsid = 'ds" + metautils::args.dsnum + "' and "
      "d.type = 'D' and d.status = 'P' and num_fixes > 0");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "' while trying to get metadata file data", F,
        caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  get_file_data(mysrv, q, "Cyclone Fix", "es", gindex_map, data_formats,
      fixml_file_data_table);
  mysrv.disconnect();
}

struct ParameterEntry {
  ParameterEntry() : id(), short_name() { }

  string id, short_name;
};

void write_grml_parameters(string file_type, string tindex, ofstream& ofs,
    string caller, string user, string& min, string& max, string&
    init_date_selection) {
  static const string F = this_function_label(__func__);
  if (file_type != "Web" && file_type != "inv") {
    log_error2("unsupported file type '" + file_type + "'", F, caller, user);
  }
  string d2 = substitute(metautils::args.dsnum, ".", "");
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    log_error2("'" + mysrv.error() + "' while trying to connect", F, caller,
        user);
  }
  static unordered_map<string, string> dfmap;
  if (dfmap.empty()) {
    fill_data_formats_table(mysrv, "WGrML", dfmap, caller, user);
  }
  static unordered_set<string> invs;
  static string idsel = "";
  if (file_type == "inv" && invs.empty()) {
    MySQL::LocalQuery q("webID_code, dupe_vdates", "IGrML.ds" + d2 +
        "_inventory_summary");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(mysrv) < 0) {
      log_error2("'" + q.error() + "' while trying to get inventory file codes",
          F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    for (const auto& r : q) {
      invs.emplace(r[0]);
      if (idsel.empty() && r[1] == "Y") {
        idsel = "I";
      }
    }
  }
  init_date_selection = idsel;
  static unordered_map<string, unordered_map<string, string>> ffmap;
  static unordered_map<string, unordered_set<string>> rdafs;
  auto fkey = d2 + ":" + tindex;
  if (ffmap[fkey].empty()) {
    if (!tindex.empty()) {
      MySQL::LocalQuery q("wfile", "dssdb.wfile", "dsid = 'ds" + metautils::
          args.dsnum + "' and tindex = " + tindex + " and " + "type = 'D' and "
          "status = 'P'");
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (q.submit(mysrv) < 0) {
        log_error2("'" + q.error() + "' while trying to get RDA files from "
            "dssdb.wfile", F, caller, user);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
          show() << endl;
      }
#endif
      for (const auto& r : q) {
        rdafs[tindex].emplace(r[0]);
      }
    }

// patch until all webfiles2 tables have migrated webID -> id
MySQL::LocalQuery q;
if (MySQL::field_exists(mysrv, "WGrML.ds" + d2 + "_webfiles2", "id")) {
q.set("code, id, format_code", "WGrML.ds" + d2 + "_webfiles2");
} else {
q.set("code, webID, format_code", "WGrML.ds" + d2 + "_webfiles2");
}

#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(mysrv) < 0) {
      log_error2("'" + q.error() + "' while trying to get metadata files from "
          + "WGrML" + ".ds" + d2 + "_webfiles2", F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    for (const auto& r : q) {
      if ((rdafs[tindex].empty() || rdafs[tindex].find(r[1]) != rdafs[tindex].
          end()) && (invs.empty() || invs.find(r[0]) != invs.end())) {
        ffmap[fkey].emplace(r[0], r[2]);
      }
    }
  }
  static Mutex pd_mutex;
  static vector<tuple<string, string, string, string>> parameter_data;
  if (parameter_data.empty() && !pd_mutex.is_locked()) {
    pd_mutex.lock();
    MySQL::LocalQuery qs("parameter, start_date, end_date, file_code", "WGrML."
        "ds" + d2 + "_agrids2");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (qs.submit(mysrv) < 0) {
      log_error2("'" + qs.error() + "' while trying to get agrids2 data", F,
          caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qs.
        show() << endl;
    }
#endif
    parameter_data.reserve(qs.num_rows());
    for (const auto& r : qs) {
      parameter_data.emplace_back(make_tuple(r[0], r[1], r[2], r[3]));
    }
    pd_mutex.unlock();
  } else if (pd_mutex.is_locked()) {
    while (pd_mutex.is_locked()) { }
  }
  xmlutils::ParameterMapper pmap(metautils::directives.parameter_map_path);
  unordered_set<string> u;
  vector<pair<string, ParameterEntry>> vp;
  unordered_map<string, size_t> idxmap; // map of vp indexes for updates
  auto cnt = 0;
  for (const auto& t : parameter_data) {
    string param, sd, ed, file_code;
    tie(param, sd, ed, file_code) = t;
    auto itm = ffmap[fkey].find(file_code); 
    if (itm != ffmap[fkey].end()) {
      auto uu = itm->second + "!" + param;
      if (u.find(uu) == u.end()) {
        auto f = dfmap[itm->second];
        auto k = pmap.description(f, param);
        auto iti = idxmap.find(k);
        auto pcode = itm->second + "!" + param;
        if (iti == idxmap.end()) {
          vp.emplace_back(make_pair(k, ParameterEntry()));
          vp.back().second.id = pcode;
          auto x = vp.back().first.find("@");
          if (x != string::npos) {
            vp.back().first = vp.back().first.substr(0, x);
          }
          vp.back().second.short_name = pmap.short_name(f, param);
          idxmap.emplace(k, cnt);
          ++cnt;
        } else {
          if (regex_search(pcode, regex("@"))) {
            pcode = pcode.substr(0, pcode.find("@"));
          }
          if (!regex_search(vp[iti->second].second.id, regex(pcode))) {
            vp[iti->second].second.id += "," + pcode;
          }
        }
        u.emplace(uu);
      }
      if (sd < min) {
        min = sd;
      }
      if (ed > max) {
        max = ed;
      }
    }
  }
  mysrv.disconnect();
  vp.resize(cnt);
  sort(vp.begin(), vp.end(),
  [](const pair<string, ParameterEntry>& left, const pair<string,
      ParameterEntry>& right) -> bool {
    if (strutils::to_lower(left.first) <= strutils::to_lower(right.first)) {
      return true;
    }
    return false;
  });
  ofs << cnt << endl;
  for (auto& p : vp) {
    replace_all(p.first, "<br />", " ");
    ofs << p.second.id << "<!>" << p.first << "<!>" << p.second.short_name <<
        endl;
  }
}

void create_file_list_cache(string file_type, string caller, string user, string
    tindex) {
  static const string F = this_function_label(__func__);
  auto d2 = substitute(metautils::args.dsnum, ".", "");
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  static Mutex g_mutex;
  static unique_ptr<unordered_map<string, string>> gmap;
  fill_gindex_map(mysrv, g_mutex, gmap, caller, user);
  unordered_map<string, FileEntry> grmlmap, obmlmap, fixmlmap;
  MySQL::LocalQuery oq, sq;
  auto b = false;
  if (file_type == "Web" || file_type == "inv") {
    if (table_exists(mysrv, "WGrML.ds" + d2 + "_webfiles2")) {
      if (file_type == "inv" && !table_exists(mysrv, "IGrML.ds" + d2 +
          "_inventory_summary")) {
        return;
      }
      if (file_type == "Web" && tindex.empty()) {
        grml_file_data(file_type, *gmap, grmlmap, caller, user);
      }
      b = true;
    }
    if (table_exists(mysrv, "WObML.ds" + d2 + "_webfiles2")) {
      if (file_type == "Web" && tindex.empty()) {
        obml_file_data(file_type, *gmap, obmlmap, caller, user);
      }
      if (file_type == "Web") {
        oq.set("select min(start_date), max(end_date) from WObML.ds" + d2 +
            "_webfiles2 where start_date > 0");
      } else if (file_type == "inv") {
        if (table_exists(mysrv, "IObML.ds" + d2 + "_dataTypes")) {
          oq.set("select min(start_date), max(end_date) from WObML.ds" + d2
              + "_webfiles2 as w left join (select distinct webID_code from "
              "IObML.ds" + d2 + "_dataTypes) as d on d.webID_code = w.code "
              "where !isnull(d.webID_code) and start_date > 0");
        } else if (table_exists(mysrv, "IObML.ds" + d2 +
            "_inventory_summary")) {
          oq.set("select min(start_date), max(end_date) from WObML.ds" + d2
              + "_webfiles2 as w left join IObML.ds" + d2 +
              "_inventory_summary as i on i.webID_code = w.code where !isnull("
              "i.webID_code) and start_date > 0");
        } else {
          return;
        }
      }
    }
    if (table_exists(mysrv, "WFixML.ds" + d2 + "_webfiles2")) {
      if (tindex.empty()) {
        fixml_file_data(file_type, *gmap, fixmlmap, caller, user);
      }
    }
  }
  if (b) {
    string f = "customize.";
    if (file_type == "Web") {
      f += "W";
    } else if (file_type == "inv") {
      f += "I";
    } else {
      f = "";
    }
    if (!f.empty()) {
      f += "GrML";
      if (!tindex.empty()) {
        f += "." + tindex;
      }
      TempDir tdir;
      if (!tdir.create(metautils::directives.temp_path)) {
        log_error2("unable to create temporary directory (1)", F, caller, user);
      }

      // create the directory tree in the temp directory
      stringstream oss, ess;
      if (mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata", oss, ess) !=
          0) {
        log_error2("unable to create a temporary directory tree (1) - '" + ess.
            str() + "'", F, caller, user);
      }
      ofstream ofs((tdir.name() + "/metadata/" + f).c_str());
      if (!ofs.is_open()) {
        log_error2("unable to open temporary file for " + f, F, caller, user);
      }
      if (unixutils::exists_on_server(metautils::directives.web_server, "/data/"
          "web/datasets/ds" + metautils::args.dsnum + "/metadata/inv",
          metautils::directives.rdadata_home)) {
        ofs << "curl_subset=Y" << endl;
      }
      string min = "99999999999999";
      string max = "0";
      string s;
      write_grml_parameters(file_type, tindex, ofs, caller, user, min, max, s);
      ofs << min << " " << max;
      if (!s.empty()) {
        ofs << " " << s;
      }
      ofs << endl;
      ofs.close();
      if (max == "0") {
        string e;
        if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
            args.dsnum + "/metadata/" + f, tdir.name(), metautils::directives.
            rdadata_home, e) < 0) {
          metautils::log_warning(F + "() couldn't unsync '" + f + "' - "
              "rdadata_unsync error(s): '" + e + "'", caller, user);
        }
      } else {
        string e;
        if (unixutils::rdadata_sync(tdir.name(), "metadata/", "/data/web/"
            "datasets/ds" + metautils::args.dsnum, metautils::directives.
            rdadata_home, e) < 0) {
          metautils::log_warning(F + "() couldn't sync '" + f + "' - "
              "rdadata_sync error(s): '" + e + "'", caller, user);
        }
      }
    }
  }
  xmlutils::DataTypeMapper dmap(metautils::directives.parameter_map_path);
  if (oq) {
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (oq.submit(mysrv) < 0) {
      log_error2("'" + oq.error() + "' from query: " + oq.show(), F, caller,
          user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << oq.
        show() << endl;
    }
#endif
    MySQL::Row row;
    oq.fetch_row(row);
    string f;
    if (!row[0].empty() && !row[1].empty()) {
      f = "customize.";
      if (file_type == "Web") {
        f += "W";
      } else if (file_type == "inv") {
        f += "I";
      } else {
        f = "";
      }
    }
    if (!f.empty()) {
      f += "ObML";
      if (!tindex.empty()) {
        f += "." + tindex;
      }
      TempDir tdir;
      if (!tdir.create(metautils::directives.temp_path)) {
        metautils::log_error(F + ": unable to create temporary directory (2)",
            caller, user);
      }

      // create the directory tree in the temp directory
      stringstream oss, ess;
      if (mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata", oss, ess) !=
          0) {
        log_error2("unable to create a temporary directory tree (2) - '" + ess.
            str() + "'", F, caller, user);
      }
      ofstream ofs((tdir.name() + "/metadata/" + f).c_str());
      if (!ofs.is_open()) {
        log_error2("unable to open temporary file for " + f, F, caller, user);
      }

      // date range
      ofs << row[0] << " " << row[1] << endl;

      // platform types
      if (file_type == "Web") {
        oq.set("select distinct l.platformType_code, p.platform_type from "
            "WObML.ds" + d2 + "_dataTypes2 as d left join WObML.ds" + d2 +
            "_dataTypesList as l on l.code = d.dataType_code left join WObML."
            "platform_types as p on p.code = l.platformType_code");
      } else if (file_type == "inv") {
        oq.set("select distinct l.platformType_code, p.platform_type from "
            "WObML.ds" + d2 + "_dataTypes2 as d left join WObML.ds" + d2 +
            "_dataTypesList as l on l.code = d.dataType_code left join WObML."
            "platform_types as p on p.code = l.platformType_code left join "
            "(select distinct webID_code from IObML.ds" + d2 + "_dataTypes) as "
            "dt on dt.webID_code = d.webID_code where !isnull(dt.webID_code)");
      }
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (oq.submit(mysrv) < 0) {
        log_error2("'" + oq.error() + "' from query: " + oq.show(), F, caller,
            user);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << oq.
          show() << endl;
      }
#endif
      ofs << oq.num_rows() << endl;
      for (const auto& r : oq) {
        ofs << r[0] << "<!>" << r[1] << endl;
      }

      // data types and formats
      MySQL::LocalQuery q;
      if (file_type == "Web" || file_type == "inv") {
        q.set("select distinct f.format from WObML.ds" + d2 + "_webfiles2 as w "
            "left join WObML.formats as f on f.code = w.format_code");
        oq.set("select distinct l.dataType from WObML.ds" + d2 + "_dataTypes2 "
            "as d left join WObML.ds" + d2 + "_dataTypesList as l on l.code = "
            "d.dataType_code");
      } else {
        q.set("");
        oq.set("");
      }
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (q.submit(mysrv) < 0) {
        log_error2("'" + q.error() + "' from query: " + q.show(), F, caller,
            user);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
          show() << endl;
      }
#endif
#ifdef DUMP_QUERIES
      {
      Timer tm;
      tm.start();
#endif
      if (oq.submit(mysrv) < 0) {
        log_error2("'" + oq.error() + "' from query: " + oq.show(), F, caller,
            user);
      }
#ifdef DUMP_QUERIES
      tm.stop();
      cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << oq.
          show() << endl;
      }
#endif
      vector<string> vf;
      for (const auto& r : q) {
        vf.emplace_back(r[0]);
      }
      unordered_map<string, string> dtypmap;
      for (const auto& r : oq) {
        auto sp = split(r[0], ":");
        string s, c;
        if (sp.size() == 2) {
          s = sp.front();
          c = sp.back();
        } else {
          s = "ds" + metautils::args.dsnum;
          c = sp.front();
        }
        for (const auto& f : vf) {
          auto k = dmap.description(f, s, c);
          if (!k.empty()) {
            if (dtypmap.find(k) == dtypmap.end()) {
              dtypmap.emplace(k, "");
            }
            if (!dtypmap[k].empty()) {
              dtypmap[k] += ",";
            }
            dtypmap[k] += r[0];
          }
        }
      }
      if (dtypmap.size() > 0) {
        ofs << dtypmap.size() << endl;
        vector<string> v;
        for (const auto& e : dtypmap) {
          v.emplace_back(e.first);
        }
        sort(v.begin(), v.end(),
        [](const string& left, const string& right) -> bool {
          if (left <= right) {
            return true;
          }
          return false;
        });
        for (const auto& k : v) {
          ofs << dtypmap[k] << "<!>" << k << endl;
        }
      }

      // groups
      if (tindex.empty()) {
        if (field_exists(mysrv, "WObML.ds" + d2 + "_webfiles2", "dsid")) {
          oq.set("select distinct g.gindex, g.title, g.grpid from WObML.ds" + d2
              + "_webfiles2 as p left join dssdb.wfile as w on (w.wfile = p.id "
              "and w.type = p.type and w.dsid = p.dsid) left join dssdb."
              "dsgroup as g on (g.dsid = 'ds" + metautils::args.dsnum + "' and "
              "g.gindex = w.gindex) where !isnull(w.wfile) order by g.gindex");
        } else {
          oq.set("select distinct g.gindex, g.title, g.grpid from WObML.ds" + d2
              + "_webfiles2 as p left join dssdb.wfile as w on w.wfile = p.id "
              "left join dssdb.dsgroup as g on (g.dsid = 'ds" + metautils::args.
              dsnum + "' and g.gindex = w.gindex) where !isnull(w.wfile) order "
              "by g.gindex");
        }
#ifdef DUMP_QUERIES
        {
        Timer tm;
        tm.start();
#endif
        if (oq.submit(mysrv) < 0) {
          log_error2("'" + oq.error() + "' from query: " + oq.show(), F, caller,
              user);
        }
#ifdef DUMP_QUERIES
        tm.stop();
        cerr << "Elapsed time: " << tm.elapsed_time() << F << ": " " " << oq.
            show() << endl;
        }
#endif
        if (oq.num_rows() > 1) {
          for (const auto& r : oq) {
            ofs << r[0] << "<!>" << r[1] << "<!>";
            if (r[2].empty()) {
              ofs << r[0];
            } else {
              ofs << r[2];
            }
            ofs << endl;
          }
        }
      }
      ofs.close();
      string e;
      if (unixutils::rdadata_sync(tdir.name(), "metadata/", "/data/web/"
          "datasets/ds" + metautils::args.dsnum, metautils::directives.
          rdadata_home, e) < 0) {
        metautils::log_warning(F + "() couldn't sync '" + f + "' - "
            "rdadata_sync error(s): '" + e + "'", caller, user);
      }
    }
  }
  if (file_type == "inv") {

    // for inventories, the work is done here and we can return
    mysrv.disconnect();
    return;
  }
  unordered_set<string> cmdset;
  auto num = grmlmap.size() + obmlmap.size() + sq.num_rows() + fixmlmap.size();
  if (num > 0) {
    vector<pair<string, FileEntry>> v;
    v.resize(num);
    auto n = 0;
    for (const auto& e : grmlmap) {
      v[n].first = e.first;
      v[n].second = e.second;
      v[n].second.metafile_id = e.first;
      cmdset.emplace(e.first);
      replace_all(v[n].second.metafile_id, "+", "%2B");
      v[n].second.metafile_id += ".GrML";
      replace_all(v[n].second.data_format, "proprietary_", "");
      ++n;
    }
    for (const auto& e : obmlmap) {
      v[n].first = e.first;
      v[n].second = e.second;
      v[n].second.metafile_id = e.first;
      cmdset.emplace(e.first);
      replace_all(v[n].second.metafile_id, "+", "%2B");
      v[n].second.metafile_id += ".ObML";
      replace_all(v[n].second.data_format, "proprietary_", "");
      ++n;
    }
    for (const auto& r : sq) {
      v[n].first = r[0];
      v[n].second.metafile_id = r[0];
      cmdset.emplace(r[0]);
      replace_all(v[n].second.metafile_id, "+", "%2B");
      v[n].second.metafile_id += ".SatML";
      v[n].second.data_format = r[1];
      replace_all(v[n].second.data_format, "proprietary_", "");
      v[n].second.units = r[2];
      if (r[9] == "I") {
        v[n].second.units += " Image";
      } else if (r[9] == "S") {
        v[n].second.units += " Scan Line";
      }
      if (r[2] != "1") {
        v[n].second.units += "s";
      }
      v[n].second.start = dateutils::string_ll_to_date_string(r[3]);
      v[n].second.end = dateutils::string_ll_to_date_string(r[4]);
      v[n].second.file_format = r[5];
      v[n].second.data_size = r[6];
      if (!r[7].empty()) {
        v[n].second.group_id = r[7];
      } else {
        v[n].second.group_id = r[8];
      }
      if (!v[n].second.group_id.empty()) {
        v[n].second.group_id += "[!]" + r[8];
      }
      ++n;
    }
    for (const auto& e : fixmlmap) {
      v[n].first = e.first;
      v[n].second = e.second;
      v[n].second.metafile_id = e.first;
      cmdset.emplace(e.first);
      replace_all(v[n].second.metafile_id, "+", "%2B");
      v[n].second.metafile_id += ".FixML";
      replace_all(v[n].second.data_format, "proprietary_", "");
      ++n;
    }
    sort(v.begin(), v.end(),
    [](const pair<string, FileEntry>& left, const pair<string, FileEntry>&
        right) -> bool {
      if (left.second.start < right.second.start) {
        return true;
      } else if (left.second.start > right.second.start) {
        return false;
      } else {
        if (left.second.end == right.second.end) {
          if (left.first <= right.first) {
            return true;
          }
          return false;
        } else if (left.second.end < right.second.end) {
          return true;
        }
        return false;
      }
    });
    string f;
    if (file_type == "Web") {
      f = "getWebList.cache";
    }
    TempDir tdir;
    if (!tdir.create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary directory (3)", F, caller, user);
    }

    // create the directory tree in the temp directory
    stringstream oss, ess;
    if (mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata", oss, ess) !=
        0) {
      log_error2("unable to create a temporary directory tree (3) - '" + ess.
          str() + "'", F, caller, user);
    }
    ofstream ofs((tdir.name() + "/metadata/" + f).c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open temporary file for " + f, F, caller, user);
    }
    ofs << num << endl;
    for (size_t n = 0; n < num; ++n) {
      string s;
      if (!v[n].second.data_size.empty()) {
        s = strutils::ftos(std::stof(v[n].second.data_size) / 1000000., 8, 2);
      } else {
          metautils::log_warning(F + "() returned warning: empty data size "
              "for '" + v[n].first + "'", caller, user);
        s = "";
      }
      strutils::trim(s);
      ofs << v[n].first << "<!>" << v[n].second.data_format << "<!>" << v[n].
          second.units << "<!>" << v[n].second.start << "<!>" << v[n].second.end
          << "<!>" << v[n].second.file_format << "<!>" << s << "<!>" << v[n].
          second.group_id << "<!>" << v[n].second.metafile_id;
      ofs << endl;
    }
    ofs.close();
    string e;
    if (unixutils::rdadata_sync(tdir.name(), "metadata/", "/data/web/datasets/"
        "ds" + metautils::args.dsnum, metautils::directives.rdadata_home, e) <
        0) {
      metautils::log_warning(F + "() couldn't sync '" + f + "' - rdadata_sync "
          "error(s): '" + e + "'", caller, user);
    }
  } else if (tindex.empty()) {
    string f = "";
    if (file_type == "Web") {
      f = "getWebList.cache";
    }
    TempDir tdir;
    if (!tdir.create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary directory (4)", F, caller, user);
    }
    string e;
    if (!f.empty() && unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" +
        metautils::args.dsnum + "/metadata/" + f, tdir.name(), metautils::
        directives.rdadata_home, e) < 0) {
      metautils::log_warning(F + "() couldn't unsync '" + f + "' - "
          "rdadata_unsync error(s): '" + e + "'", caller, user);
    }
  }
  mysrv.disconnect();
  if (tindex.empty() && cmdset.size() > 0) {
    create_non_cmd_file_list_cache(file_type, cmdset, caller, user);
  }
  mysrv.connect(metautils::directives.database_server, metautils::directives.
      rdadb_username, metautils::directives.rdadb_password, "dssdb");
  mysrv.update("dataset", "meta_link = 'Y', version = version + 1", "dsid = 'ds"
      + metautils::args.dsnum + "'");
  mysrv.disconnect();
}

void summarize_locations(string database, string& error) {
  static const string F = this_function_label(__func__);
  error = "";
  string d2 = substitute(metautils::args.dsnum, ".", "");
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");

  // get all location names that are NOT included
  MySQL::LocalQuery q("distinct gcmd_keyword", database + ".ds" + d2 +
      "_location_names", "include = 'N'");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    error = move(q.error());
    return;
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_set<string> nset;
  for (const auto& row : q) {
    auto s = row[0];
    replace_all(s, "'", "\\'");
    MySQL::LocalQuery qn("name", "search.political_boundaries", "name like '" +
        s + " >%'");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (qn.submit(mysrv) < 0) {
      error = move(qn.error());
      return;
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qn.
        show() << endl;
    }
#endif
    if (qn.num_rows() > 0) {

      // location name is a parent - only add the children
      for (const auto& r : qn) {
        if (nset.find(r[0]) == nset.end()) {
          nset.emplace(r[0]);
        }
      }
    } else {

      // location name is not a parent
      if (nset.find(row[0]) == nset.end()) {
        nset.emplace(row[0]);
      }
    }
  }

   // get the location names that ARE included
  q.set("distinct gcmd_keyword", database + ".ds" + d2 + "_location_names",
      "include = 'Y'");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    error = move(q.error());
    return;
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_set<string> yset;
  vector<string> v;
  for (const auto& row : q) {
    auto s = row[0];
    replace_all(s, "'", "\\'");
    MySQL::LocalQuery qn("name", "search.political_boundaries", "name like '" +
        s + " >%'");
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (qn.submit(mysrv) < 0) {
      error = move(qn.error());
      return;
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qn.
        show() << endl;
    }
#endif
    if (qn.num_rows() > 0) {

      // location name is a parent - only add the children if they are not in
      //  nset
      for (const auto& r : qn) {
        if (nset.find(r[0]) == nset.end() && yset.find(r[0]) == yset.end()) {
          yset.emplace(r[0]);
          v.emplace_back(r[0]);
        }
      }
    } else {

      // location name is not a parent - add the location
      if (yset.find(row[0]) == yset.end()) {
        yset.emplace(row[0]);
        v.emplace_back(row[0]);
      }
    }
  }
  string err;
  mysrv.command("lock tables search.locations write", err);
  mysrv._delete("search.locations", "dsid = '" + metautils::args.dsnum + "'");
  for (auto e : v) {
    replace_all(e, "'", "\\'");
    if (mysrv.insert("search.locations", "'" + e + "', 'GCMD', 'Y', '" +
        metautils::args.dsnum + "'") < 0) {
      error = move(mysrv.error());
      return;
    }
  }
  if (mysrv.command("unlock tables", err) < 0) {
    error = move(mysrv.error());
    return;
  }
  mysrv.disconnect();
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
