#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <sys/stat.h>
#include <metadata.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using namespace PostgreSQL;
using metautils::log_error2;
using miscutils::this_function_label;
using std::endl;
using std::cerr;
using std::cout;
using std::move;
using std::unordered_map;
using std::unordered_set;
using std::string;
using std::stringstream;
using strutils::append;
using strutils::chop;
using strutils::has_ending;
using strutils::ng_gdex_id;
using strutils::split;
using strutils::substitute;
using strutils::replace_all;
using strutils::to_lower;
using strutils::trim;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";
const char ARG_DELIMITER = '!';

string whereis_lmod() {
  string s; // return value
  stringstream oss, ess;
  if (mysystem2("/bin/tcsh -c 'echo $LMOD_CMD'", oss, ess) == 0) {
    s = oss.str();
    trim(s);
  }
  return s;
}

string whereis_singularity(stringstream& ess) {
  string s; // return value
  stringstream oss;
  if (mysystem2("/bin/tcsh -c 'which singularity'", oss, ess) != 0) {
    auto m = whereis_lmod();
    if (!m.empty()) {
      if (mysystem2("/bin/tcsh -c 'eval `" + m + " tcsh load apptainer`; which "
          "singularity'", oss, ess) == 0) {
        s = oss.str();
      } else if (mysystem2("/bin/tcsh -c 'eval `" + m + " tcsh load "
          "singularity`; which singularity'", oss, ess) == 0) {
        s = oss.str();
      }
    }
  } else {
    s = oss.str();
  }
  trim(s);
  return s;
}

void show_gatherxml_usage() {
  cout << "usage: (1) gatherxml -d [ds]<nnn.n> -f <format> [-R] [-S] { URL | "
       "WF } " << endl;
  cout << "   or: (2) gatherxml --showinfo" << endl;
  cout << "   or: (3) gatherxml -d <[ds]nnn.n> -f <format> -I { URL | invall }"
      << endl;
  cout << "   or: (4) gatherxml -d test -f <format> PATH" << endl;
  cout << "\nrequired:" << endl;
  cout << "  -d [ds]<nnn.n>  (1), (3): the dataset number, optionally "
      "prepended with \"ds\"" << endl;
  cout << "  -d test         (4): perform a test run to see if gatherxml will "
      "handle a" << endl;
  cout << "                    particular file" << endl;
  cout << "  -f <format>     (1), (3), (4): the data format of the file being "
      "scanned. Use" << endl;
  cout << "                    (2) to see a list of all supported data formats."
      << endl;
  cout << "  -I              (3): inventory only. Use this flag when metadata "
      "have already" << endl;
  cout << "                    been scanned for the file, but an inventory "
      "does not" << endl;
  cout << "                    currently exist." << endl;
  cout << "  --showinfo      (2): show a list of supported data formats" <<
      endl;
  cout << endl;
  cout << "  URL             (1), (3): the URL of the data file, beginning with"
      << endl;
  cout << "                    https://rda.ucar.edu/" << endl;
  cout << "  WF              (1): the path of the web file, as for the -WF "
      "option of dsarch" << endl;
  cout << "  invall          (3): with this specified, gatherxml will "
      "determine which files" << endl;
  cout << "                    in the dataset do not have inventories and "
      "generate all of" << endl;
  cout << "                    them in a single call" << endl;
  cout << "  PATH            (4): this is the path of the file to be tested" <<
      endl;
  cout << "\noptions:" << endl;
  cout << "  -R              (1): save time by not regenerating the dataset "
      "web description" << endl;
  cout << "                    (see NOTES below)" << endl;
  cout << "  -S              (1): save time by not rebuilding the metadata "
      "caches for the" << endl;
  cout << "                    dataset (see NOTES below)" << endl;
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
      if (s.find("-f") == 0) {
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

string webhome() {
  if (!metautils::directives.data_root_alias.empty()) {
    return metautils::directives.data_root_alias + "/" + metautils::args.dsid;
  }
  return metautils::web_home();
}

string gatherxml_utility(string user) {
  if (metautils::args.args_string.empty()) {
    show_gatherxml_usage();
    exit(0);
  }
  static const string F = this_function_label(__func__);
  auto sp_a = split(metautils::args.args_string, "!");
  for (size_t n = 0; n < sp_a.size(); ++n) {
    if (sp_a[n] == "-d") {
      metautils::args.dsid = sp_a[++n];
      if (metautils::args.dsid != "test") {
        metautils::args.dsid = ng_gdex_id(metautils::args.dsid);
      }
    } else if (sp_a[n] == "-f") {
      metautils::args.data_format = sp_a[++n];
    } else if (sp_a[n] == "--help") {
      metautils::args.data_format = "showhelp";
      break;
    } else if (sp_a[n] == "--showinfo") {
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
        for (const auto& s : sp2) {
          aka_map.emplace(s, sp[0]);
          r_aka_map.emplace(sp[0], s);
        }
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
  auto k = to_lower(metautils::args.data_format);
  auto it = util_map.find(k);
  if (it != util_map.end()) {
    util = it->second;
  } else {
    it = aka_map.find(k);
    if (it != aka_map.end() && util_map.find(it->second) != util_map.end()) {
      util = util_map[it->second];
      auto f = "-f" + string(1, ARG_DELIMITER);
      replace_all(metautils::args.args_string, f + metautils::args.data_format,
          f + it->second);
    }
  }
  if (util.empty()) {
    cerr << "Error: unable to determine gatherxml utility from format (-f) '" <<
        metautils::args.data_format << "'" << endl;
    cerr << "    Use \"gatherxml --showinfo\" to see a list of supported data "
        "formats" << endl;
    exit(1);
  }
  if (sp_a.back()[0] != '/' && sp_a.back().find("https://rda.ucar.edu/") != 0) {
    if (metautils::args.dsid == "test") {
      char buf[32768];
      auto b = getcwd(buf, 32768);
      if (b == nullptr) {
        cerr << "Error determining current working directory and PATH is not "              "an absolute path." << endl;
        exit(1);
      }
      replace_all(metautils::args.args_string, "!" + sp_a.back(), "!" + string(
          buf) + "/" + sp_a.back());
    } else {
      auto idx = metautils::args.args_string.rfind("!");
      if (idx == string::npos) {
        log_error2("bad arguments string: '" + metautils::args.args_string, F,
            "gatherxml", user);
      }
      metautils::args.args_string = metautils::args.args_string.substr(0, idx +
          1) + "https://rda.ucar.edu" + webhome() + "/" + metautils::args.
          args_string.substr(idx + 1);
    }
  }
  return util.substr(1);
}

int command_execute(string command) {
  stringstream oss, ess;
  mysystem2(command, oss, ess);
  if (!oss.str().empty()) {
    cout << oss.str() << endl;
  }
  if (!ess.str().empty()) {
    cerr << ess.str() << endl;
    return 1;
  }
  return 0;
}

int main(int argc, char **argv) {
  metautils::args.args_string = unixutils::unix_args_string(argc, argv,
      ARG_DELIMITER);
  string util = argv[0];
  auto idx = util.rfind("/");
  if (idx != string::npos) {
    util = util.substr(idx + 1);
  }
  auto env = getenv("USER");
  if (env == nullptr) {
    cerr << "Terminating - unknown user" << endl;
    exit(1);
  }
  auto u = string(env);
  if (!metautils::read_config(util, u)) {
    log_error2("configuration error: '" + myerror + "'", "main()", util, u);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      rdadb_username, metautils::directives.rdadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to database; error: " + srv.error(), "main()",
        util, u);
  }
  LocalQuery q("stat_flag", "dssdb.dssgrp", "logname = '" + u + "'");
  if (q.submit(srv) < 0) {
    log_error2("authorization server error: " + q.error(), "main()", util, u);
  }
  srv.disconnect();
  Row row;
  if (!q.fetch_row(row)) {
    cerr << "Error: not authorized" << endl;
    return 1;
  }
  setreuid(15968, 15968);
  setenv("HOME", "/glade/u/home/rdadata", 1);
  string cmd;
  if (has_ending(util, "_s")) {
    chop(util, 2);
  } else if (has_ending(util, "_pg")) {
    chop(util, 3);
  }
  if (util == "gatherxml") {
    util = gatherxml_utility(u);
  }

  // handle non-containerized utilities
  auto host = unixutils::host_name();
  string c;
  if (host.find("rda-web") == 0 && (util == "doi" || util == "dsgen")) {
    c = "/usr/local/decs/bin/_" + util;
  } else if (host.find("casper") == 0 && util == "doi") {
    c = "/ncar/rda/setuid/bin/_" + util;
  }
  if (!c.empty()) {
    if (!metautils::args.args_string.empty()) {
      c += " " + substitute(metautils::args.args_string, "!", " ");
    }
    return command_execute(c);
  }

  // locate the singularity command
  stringstream ess;
  auto s = whereis_singularity(ess);
  if (s.empty()) {
    log_error2("unable to find singularity: '" + ess.str() + "'", "main()",
        util, u);
  } else {
    string binds;
    struct stat buf;
    for (const auto& bind : metautils::directives.singularity_binds) {
      if (stat(bind.c_str(), &buf) == 0) {
        append(binds, bind, ",");
      }
    }
    auto sp = split(metautils::args.args_string, "!");
    if (sp.size() > 0 && sp.back()[0] == '/') {

      // test run must specify full path, so bind that path
      auto idx = sp.back().rfind("/");
      append(binds, sp.back().substr(0, idx), ",");
    }
    cmd = s + " -s exec --env PYTHONPATH=x -B " + binds + " /glade/u/home/"
        "rdadata/bin/singularity/gatherxml_pg-exec.sif /usr/local/bin/_" + util;
    if (!metautils::args.args_string.empty()) {
      cmd += " " + substitute(metautils::args.args_string, "!", " ");
    }
    return command_execute(cmd);
  }
}
