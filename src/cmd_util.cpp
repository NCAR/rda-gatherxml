#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::endl;
using std::cerr;
using std::cout;
using std::move;
using std::unordered_map;
using std::unordered_set;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using strutils::chop;
using strutils::to_lower;
using strutils::split;
using strutils::substitute;
using strutils::trim;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

string whereis_singularity() {
  string s; // return value
  stringstream oss, ess;
  if (mysystem2("/bin/tcsh -c 'which singularity'", oss, ess) != 0) {
    if (mysystem2("/bin/tcsh -c 'module load singularity; which singularity'",
        oss, ess) == 0) {
      s = oss.str();
    }
  } else {
    s = oss.str();
  }
  trim(s);
  return move(s);
}

void show_gatherxml_usage() {
  cout << "usage: (1) gatherxml -f <format> -d [ds]<nnn.n> [-R] [-S] { URL | "
       "WF } " << endl;
  cout << "   or: (2) gatherxml --showinfo" << endl;
  cout << "   or: (3) gatherxml -f <format> -d <[ds]nnn.n> -I { URL | invall }"
      << endl;
  cout << "   or: (4) gatherxml -f <format> -d test PATH" << endl;
  cout << "\nrequired:" << endl;
  cout << "-f <format>     the data format of the file being scanned. Use (2) "
      "to see a list" << endl;
  cout << "                of all supported data formats. (1), (3), (4)" <<
      endl;
  cout << "-d [ds]<nnn.n>  the dataset number, optionally prepended with "
      "\"ds\" (1), (3)" << endl;
  cout << "-d test         perform a test run to see if gatherxml will handle "
      "a particular" << endl;
  cout << "                file (4)" << endl;
  cout << "-I              inventory only. Use this flag when metadata have "
      "already been" << endl;
  cout << "                scanned for the file, but an inventory does "
      "not currently" << endl;
  cout << "                exist. (3)" << endl;
  cout << "--showinfo      show a list of supported data formats (2)" << endl;
  cout << "\noptions:" << endl;
  cout << "-R              save time by not regenerating the dataset web "
      "description (see " << endl;
  cout << "                NOTES below) (1)" << endl;
  cout << "-S              save time by not rebuilding the metadata caches for "
      "the dataset" << endl;
  cout << "                (see NOTES below) (1)" << endl;
  cout << "\nNOTES:" << endl;
  cout << "  - Using the -R and -S options are particularly useful when "
      "backfilling file" << endl;
  cout << "    content metadata for a dataset. This speeds up the individual "
      "gatherxml runs" << endl;
  cout << "    by quite a bit, but you will need to manually run \"scm\" at "
      "the end of a" << endl;
  cout << "    series of gatherxml runs that used -R and -S to rebuild the "
      "dataset caches" << endl;
  cout << "    and regenerate the dataset description. If you don't do this, "
      "your dataset" << endl;
  cout << "    information will be out-of-sync." << endl;
}

void show_gatherxml_info(const unordered_set<string>& util_set, const
    unordered_map<string, string>& r_aka_map) {
  for (const auto& e : util_set) {
    stringstream oss, ess;
    mysystem2(metautils::directives.decs_bindir + "/" + e, oss, ess);
    cout << "\nutility:" << substitute(e, "_", " ") << endl;
    cout << "supported formats (\"-f\" flag):" << endl;
    auto sp = split(ess.str(), "\n");
    sp.pop_back();
    for (const auto& s : sp) {
      if (regex_search(s, regex("^-f"))) {
        auto sp2 = split(s);
        cout << "  '" << sp2[1] << "'";
        auto it = r_aka_map.find(sp2[1]);
        if (it != r_aka_map.end()) {
          auto sp3 = split(it->second, ",");
          for (const auto& s3 : sp3) {
            cout << " OR '" << s3 << "'";
          }
        }
        cout << " (" << sp2[2];
        for (size_t n = 3; n < sp2.size(); ++n) {
          cout << " " << sp2[n];
        }
        cout << ")" << endl;
      }
    }
  }
}

string gatherxml_utility(string user) {
  if (metautils::args.args_string.empty()) {
    show_gatherxml_usage();
    exit(0);
  }
  static const string F = this_function_label(__func__);
  auto sp = split(metautils::args.args_string, "!");
  for (size_t n = 0; n < sp.size(); ++n) {
    if (sp[n] == "-f") {
      metautils::args.data_format = to_lower(sp[++n]);
    } else if (sp[n] == "--help") {
      metautils::args.data_format = "showhelp";
      break;
    } else if (sp[n] == "--showinfo") {
      metautils::args.data_format = "showinfo";
      break;
    }
  }
  if (metautils::args.data_format.empty()) {
    log_error2("unable to determine data format", F, "gatherxml", user);
  }
  std::ifstream ifs((metautils::directives.decs_root + "/bin/conf/gatherxml"
      ".conf").c_str());
  if (!ifs.is_open()) {
    log_error2("unable to open " + metautils::directives.decs_root + "/bin/"
        "conf/gatherxml.conf", F, "gatherxml", user);
  }
  unordered_map<string, string> util_map, aka_map, r_aka_map;
  unordered_set<string> util_set;
  char l[256];
  ifs.getline(l, 256);
  while (!ifs.eof()) {
    if (l[0] != '#') {
      auto sp = split(l);
      if (util_set.find(sp[1]) == util_set.end()) {
        util_set.emplace(sp[1]);
      }
      util_map.emplace(sp[0], sp[1]);
      if (sp.size() > 2) {
        auto sp2 = split(sp[2], ",");
        for (const auto& s : sp) {
          aka_map.emplace(s, sp[0]);
        }
        r_aka_map.emplace(sp[0], sp[2]);
      }
    }
    ifs.getline(l, 256);
  }
  ifs.close();
  if (metautils::args.data_format == "showhelp") {
    show_gatherxml_usage();
    exit(0);
  } else if (metautils::args.data_format == "showinfo") {
    show_gatherxml_info(util_set, r_aka_map);
    exit(0);
  }
  string util; // return value
  auto it = util_map.find(metautils::args.data_format);
  if (it != util_map.end()) {
    util = it->second;
  } else {
    it = aka_map.find(metautils::args.data_format);
    if (it != aka_map.end() && util_map.find(it->second) != util_map.end()) {
      util = util_map[it->second];
    }
  }
  if (util.empty()) {
    log_error2("unable to determine gatherxml utility", F, "gatherxml", user);
  }
  return util.substr(1);
}

int main(int argc, char **argv) {
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '!');
  string util = argv[0];
  auto idx = util.rfind("/");
  if (idx != string::npos) {
    util = util.substr(idx + 1);
  }
  string u = getenv("USER");
  if (!metautils::read_config(util, u)) {
    log_error2("configuration error: '" + myerror + "'", "main()", util, u);
  }
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  if (!srv) {
    log_error2("unable to connect to database; error: " + srv.error(), "main()",
        util, u);
  }
  MySQL::Query q("stat_flag", "dssgrp", "logname = '" + u + "'");
  if (q.submit(srv) < 0) {
    log_error2("authorization server error: " + q.error(), "main()", util, u);
  }
  MySQL::Row row;
  if (!q.fetch_row(row)) {
    cerr << "Error: not authorized" << endl;
    return 1;
  }
  setreuid(15968, 15968);
  string cmd;
  if (regex_search(util, regex("_s$"))) {
    chop(util, 2);
    if (util == "gatherxml") {
      util = gatherxml_utility(u);
    }
    auto s = whereis_singularity();
    if (s.empty()) {
      log_error2("unable to find singularity", "main()", util, u);
    }
    unordered_map<string, string> umap{
        { "dsgen", "gatherxml-utils-ubuntu" },
        { "grid2xml", "gatherxml-utils-ubuntu" },
        { "nc2xml", "gatherxml-utils-ubuntu" },
    };
    auto it = umap.find(util);
    if (it == umap.end()) {
      log_error2("no sif map entry for utility '" + util + "'", "main()", util,
          u);
    }
    unordered_map<string, string> bmap{ { "gatherxml-utils-ubuntu",
        "/glade/u/home/dattore/conf,/glade/scratch/rdadata,/glade/u/home/"
        "rdadata,/glade/collections/rda/data,/gpfs/fs1/collections/rda/work/"
        "logs/md" } };
    auto it2 = bmap.find(it->second);
    if (it2 == bmap.end()) {
      log_error2("no bind map entry for this utility", "main()", util, u);
    }
    auto b = it2->second;
    auto sp = split(metautils::args.args_string, "!");
    if (sp.back()[0] == '/') {

      // test run must specify full path, so bind that path
      auto idx = sp.back().rfind("/");
      b += "," + sp.back().substr(0, idx);
    }
    cmd = s + " -s exec -B " + b + " /glade/u/home/rdadata/bin/singularity/" +
        it->second + ".sif /_" + util;
  } else {
    cmd = metautils::directives.decs_bindir + "/_" + util;
  }
  if (!metautils::args.args_string.empty()) {
    cmd += " " + substitute(metautils::args.args_string, "!", " ");
  }
  stringstream oss, ess;
  mysystem2(cmd, oss, ess);
  if (!oss.str().empty()) {
    cout << oss.str() << endl;
  }
  if (!ess.str().empty()) {
    cerr << ess.str() << endl;
    return 1;
  }
  return 0;
}
