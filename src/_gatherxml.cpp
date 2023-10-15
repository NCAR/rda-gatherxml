#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <regex>
#include <sstream>
#include <deque>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>

using namespace MySQL;
using metautils::log_error2;
using std::cerr;
using std::cout;
using std::endl;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::chop;
using strutils::ftos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

bool showinfo = false;

string webhome() {
  if (!metautils::directives.data_root_alias.empty()) {
    return metautils::directives.data_root_alias + "/ds" + metautils::args.
        dsnum;
  }
  return metautils::web_home();
}

void *do_inventory(void *ts) {
  stringstream oss, ess;
  mysystem2(*(reinterpret_cast<string *>(ts)), oss, ess);
  return nullptr;
}

void inventory_all() {
  const string F = miscutils::this_function_label(__func__);
  if (metautils::args.data_format != "grib" && metautils::args.data_format !=
      "grib2" && metautils::args.data_format != "grib0" && metautils::args.
      data_format != "cfnetcdf" && metautils::args.data_format != "hdf5nc4") {
    log_error2("unable to inventory '" + metautils::args.data_format +
        "' files", F, "gatherxml", USER);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  if (!srv) {
    log_error2("unable to connected to RDA metadata database server", F,
        "gatherxml", USER);
  }
  string ds = substitute(metautils::args.dsnum, ".", "");
  LocalQuery q;
  if (table_exists(srv, "IGrML.ds" + ds + "_inventory_summary")) {
    q.set("select w.id, f.format from WGrML.ds" + ds + "_webfiles2 as w "
        "left join IGrML.ds" + ds + "_inventory_summary as i on i.file_code = "
        "w.code left join WGrML.formats as f on f.code = w.format_code where "
        "i.file_code is null or inv is null");
  } else if (table_exists(srv, "WGrML.ds" + ds + "_webfiles2")) {
    q.set("select w.id, f.format from WGrML.ds" + ds + "_webfiles2 as w left "
        "join WGrML.formats as f on f.code = w.format_code");
  } else if (table_exists(srv, "IObML.ds" + ds + "_inventory_summary")) {
    q.set("select w.id, f.format from WObML.ds" + ds + "_webfiles2 as w "
        "left join IObML.ds" + ds + "_inventory_summary as i on i.file_code = "
        "w.code left join WObML.formats as f on f.code = w.format_code where "
        "i.file_code is null or inv is null");
  } else if (table_exists(srv, "WObML.ds" + ds + "_webfiles2")) {
    q.set("select w.id, f.format from WObML.ds" + ds + "_webfiles2 as w left "
        "join WObML.formats as f on f.code = w.format_code");
  }
  if (q.submit(srv) < 0) {
    log_error2("'" + q.error() + "'", F, "gatherxml", USER);
  }
  srv.disconnect();
  const size_t MAX_T = 4;
  vector<int> t_idx(MAX_T, -1);
  vector<pthread_t> tids(MAX_T);
  vector<string> cmds(MAX_T);
  int num_t = 0, ti = 0;
  size_t n = 0;
  for (const auto& row : q) {
    string f = row[1];
    f = to_lower(f);
    ++n;
    if (f == metautils::args.data_format || (f == "netcdf" && metautils::args.
        data_format == "cfnetcdf")) {
      while (num_t == MAX_T) {
        for (int m = 0; m < num_t; ++m) {
          if (pthread_kill(tids[m], 0) != 0) {
            pthread_join(tids[m], nullptr);
            ti = m;
            t_idx[m] = -1;
            --num_t;
            break;
          }
        }
      }
      cmds[ti] = metautils::directives.local_root + "/bin/gatherxml";
      if (n != q.num_rows() && n % 100 != 0) {
        cmds[ti] += " -R -S";
      }
      cmds[ti] += " -d " + metautils::args.dsnum + " -f " + metautils::args.
          data_format + " -I https://rda.ucar.edu" + webhome() + "/" + row[0];
      pthread_create(&tids[ti], nullptr, do_inventory, reinterpret_cast<void *>(
          &cmds[ti]));
      t_idx[ti] = 0;
      ++ti;
      ++num_t;
    }
  }
  for (size_t n = 0; n < MAX_T; ++n) {
    if (t_idx[n] == 0) {
      pthread_join(tids[n], nullptr);
    }
  }
}

int main(int argc, char **argv) {
  if (argc < 6 && argc != 2 && argc != 3) {
    cerr << "For command usage, see the \"Metadata Utilities\" man pages, "
        "which are accessible" << endl;
    cerr << "  from the dashboard under \"Dataset Stewardship Tools and "
        "Documentation\"." << endl;
    exit(1);
  }
  string sep;
  if (argc == 3) {
    sep = argv[1];
    metautils::args.args_string = argv[2];
  } else {
    sep = "%";
    metautils::args.args_string = unixutils::unix_args_string(argc, argv, '%');
  }
  metautils::read_config("gatherxml", USER);
  std::ifstream ifs((metautils::directives.decs_root + "/bin/conf/gatherxml"
      ".conf").c_str());
  if (!ifs.is_open()) {
    log_error2("unable to open " + metautils::directives.decs_root + "/bin"
        "/conf/gatherxml.conf", "main()", "gatherxml", USER);
  }
  unordered_map<string, string> util_map, aka_map, r_aka_map;
  unordered_set<string> util_set;
  char line[256];
  ifs.getline(line, 256);
  while (!ifs.eof()) {
    if (line[0] != '#') {
      auto sp = split(line);
      if (util_set.find(sp[1]) == util_set.end()) {
        util_set.emplace(sp[1]);
      }
      util_map.emplace(sp[0], sp[1]);
      if (sp.size() > 2) {
        auto sp2 = split(sp[2], ",");
        for (auto& p : sp2) {
          aka_map.emplace(p, sp[0]);
        }
        r_aka_map.emplace(sp[0], sp[2]);
      }
    }
    ifs.getline(line, 256);
  }
  ifs.close();
  auto sp = split(metautils::args.args_string, sep);
  size_t np = sp.size();
  if (sp.size() == 1) {
    ++np;
  }
  for (size_t n = 0; n < np - 1; ++n) {
    if (sp[n] == "-f") {
      metautils::args.data_format = sp[++n];
    } else if (sp[n] == "-d") {
      metautils::args.dsnum = sp[++n];
      if (regex_search(metautils::args.dsnum, regex("^ds"))) {
        metautils::args.dsnum = metautils::args.dsnum.substr(2);
      }
    } else if (sp[n] == "-showinfo") {
      showinfo = true;
    }
  }
  if (showinfo) {
    for (const auto& e : util_set) {
      auto p = popen((metautils::directives.decs_bindir + "/" + e + " 2>&1").
          c_str(), "r");
      cerr << "\nutility:" << strutils::substitute(e, "_", " ") << endl;
      cerr << "supported formats (\"-f\" flag):" << endl;
      while (fgets(line, 256, p) != nullptr) {
        string sline = line;
        if (regex_search(sline, regex("^-f"))) {
          chop(sline);
          sp = split(sline);
          cerr << "  '" << sp[1] << "'";
          auto it = r_aka_map.find(sp[1]);
          if (it != r_aka_map.end()) {
            auto sp2 = split(it->second, ",");
            for (auto& p : sp2) {
              cerr << " OR '" << p << "'";
            }
          }
          cerr << " (" << sp[2];
          for (size_t n = 3; n < sp.size(); ++n) {
            cerr << " " << sp[n];
          }
          cerr << ")" << endl;
        }
      }
      pclose(p);
    }
  } else {
    if (metautils::args.data_format.empty()) {
      cerr << "Error: no format specified" << endl;
      exit(1);
    } else {
      metautils::args.data_format = to_lower(metautils::args.data_format);
    }
    if (metautils::args.dsnum.empty()) {
      cerr << "Error: no dataset number specified" << endl;
      exit(1);
    }
    metautils::args.path = sp.back();
if (regex_search(metautils::args.path, regex("^/FS/DECS/"))) {
cerr << "HPSS files are no longer supported" << endl;
exit(1);
}
    if (!regex_search(metautils::args.path, regex("^https://rda.ucar.edu"))) {
      if (metautils::args.path.length() > 128) {
        cerr << "Error: filename exceeds 128 characters in length" << endl;
        exit(1);
      }
      if (metautils::args.path == "invall") {
        inventory_all();
        exit(0);
      } else if (metautils::args.dsnum != "test") {
        string url = metautils::args.path[0] == '/' ? "https://rda.ucar.edu" +
            webhome() + metautils::args.path : "https://rda.ucar.edu" +
            webhome() + "/" + metautils::args.path;
        auto idx = metautils::args.args_string.rfind(sep);
        if (idx == string::npos) {
          log_error2("bad arguments string: '" + metautils::args.args_string,
              "main()", "gatherxml", USER);
        }
        metautils::args.args_string = metautils::args.args_string.substr(0,
            idx + 1) + url;
        metautils::args.path = url;
      }
    }
    auto it = util_map.find(metautils::args.data_format);
    if (it != util_map.end()) {
      auto t1 = std::time(nullptr);
      stringstream oss, ess;
      auto stat = mysystem2(metautils::directives.decs_bindir + "/" + it->second
          + " " + strutils::substitute(metautils::args.args_string, "%", " "),
          oss, ess);
      if (stat != 0) {
        if (stat == 2 || metautils::args.dsnum == "test" || metautils::args.
            dsnum >= "999.0") {
          cerr << ess.str() << endl;
          exit(1);
        } else {
          metautils::log_error2(ess.str(), "main()", "gatherxml", USER);
        }
      } else if (!oss.str().empty()) {
        cout << oss.str() << endl;
      }
      auto t2 = std::time(nullptr);
      metautils::log_warning("execution time: " + ftos(t2 - t1) + " seconds",
          "gatherxml.time", USER);
    } else {
      auto it = aka_map.find(metautils::args.data_format);
      if (it != aka_map.end()) {
        auto it2 = util_map.find(it->second);
        if (it2 != util_map.end()) {
          replace_all(metautils::args.args_string, "-f%" + metautils::args.
              data_format, "-f%" + it2->first);
          auto t1 = std::time(nullptr);
          stringstream oss, ess;
          auto stat = mysystem2(metautils::directives.decs_bindir + "/" +
              it2->second + " "+ substitute(metautils::args.args_string, "%",
              " "), oss, ess);
          if (stat != 0) {
            if (stat == 2 || metautils::args.dsnum == "test" || metautils::args.
                dsnum >= "999.0") {
              cerr << ess.str() << endl;
              exit(1);
            } else {
              log_error2(ess.str(), "main()", "gatherxml", USER);
            }
          }
          auto t2 = std::time(nullptr);
          metautils::log_warning("execution time: " + ftos(t2 - t1) +
             " seconds", "gatherxml.time", USER);
        } else {
          cerr << "format '" << metautils::args.data_format << "' does not map "
              "to a content metadata utility" << endl;
        }
      } else {
        cerr << "format '" << metautils::args.data_format << "' does not map "
            "to a content metadata utility" << endl;
      }
    }
  }
  return 0;
}
