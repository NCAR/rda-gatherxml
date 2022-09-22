#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <list>
#include <regex>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::endl;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using strutils::chop;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;
using unixutils::exists_on_server;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

string g_old_web_home;
string g_old_name, g_new_name, g_new_dsnum;

bool verified_new_file_is_archived(string& error) {
  const string F = this_function_label(__func__);
  if (metautils::args.dsnum > "998.9" || metautils::args.dsnum == "test") {
    return true;
  }
  MySQL::Server server(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  string qs, col;
  if (regex_search(g_old_name, regex(
      "^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    col = "wfile";
    qs = "select wfile from wfile where dsid = 'ds" + g_new_dsnum + "' and "
        "wfile = '" + metautils::relative_web_filename(g_new_name) + "' and "
        "type = 'D' and status = 'P'";
  }
  MySQL::LocalQuery q(qs);
  if (q.submit(server) < 0) {
    log_error2("error: '" + q.error() + "'", F, "rcm", USER);
  }
  server.disconnect();
  if (q.num_rows() == 0) {
    if (col == "wfile") {
      error = "Error: " + g_new_name + " is not an active web file for ds" +
          g_new_dsnum;
    }
    return false;
  }
  return true;
}

void replace_uri(string& sline, string cmdir, string member_name = "") {
  auto s = sline.substr(0, sline.find("\"") + 1);
  if (cmdir == "wfmd") {
    s += "file://web:" + metautils::relative_web_filename(g_new_name);
  }
  s += sline.substr(sline.find("\" format"));
  sline = s;
}

void rewrite_uri_in_cmd_file(string db) {
  const string F = this_function_label(__func__);
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::ifstream ifs;
  std::ofstream ofs;
  char line[32768],line2[32768];
  std::list<string> file_list;
  std::deque<string> sp,sp2;
  string sline,sline2,cmdir,db_prefix,ref_file,new_ref_file;
  string sdum,error;
  size_t idx;
  TempDir tdir;
  my::map<metautils::StringEntry> unique_stage_table;
  metautils::StringEntry se;
  bool old_is_gzipped=false;

  if (!tdir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, "rcm", USER);
  }
  string oname = g_old_name;
  string nname = g_new_name;
  if (regex_search(g_old_name, regex(
      "^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    cmdir = "wfmd";
    db_prefix = "W";
    if (!g_old_web_home.empty()) {
      replace_all(oname, "http://rda.ucar.edu", "");
      replace_all(oname, "http://dss.ucar.edu", "");
      replace_all(oname, (g_old_web_home + "/"), "");
    } else {
      oname = metautils::relative_web_filename(oname);
    }
    nname = metautils::relative_web_filename(nname);
  }
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata/" + cmdir, oss, ess)
      != 0) {
    log_error2("error: unable to create temporary directory tree (1)", F, "rcm",
        USER);
  }
  if (db == "WGrML" && mysystem2("/bin/mkdir -p " + tdir.name() + "/metadata/"
      "inv", oss, ess) != 0) {
    log_error2("error: unable to create temporary directory tree (2)", F, "rcm",
        USER);
  }
  auto re_uri = regex("uri=");
  replace_all(oname, "/", "%");
  replace_all(nname, "/", "%");
  file_list.emplace_back(oname + "<!>" + nname);
  if (db == (db_prefix + "ObML")) {
    string f = "";
    if (exists_on_server("rda-web-prod.ucar.edu", "/data/web/datasets/ds" +
        metautils::args.dsnum + "/metadata/" + cmdir + "/" + oname + ".ObML.gz",
        metautils::directives.rdadata_home)) {
      f = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
          metautils::args.dsnum + "/metadata/" + cmdir + "/" + oname +
          ".ObML.gz", tdir.name());
      system(("gunzip " + f).c_str());
      chop(f, 3);
      old_is_gzipped = true;
    } else if (exists_on_server("rda-web-prod.ucar.edu", "/data/web/datasets/ds"
        + metautils::args.dsnum + "/metadata/" + cmdir + "/" + oname + ".ObML",
        metautils::directives.rdadata_home)) {
      f = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
          metautils::args.dsnum + "/metadata/" + cmdir + "/" + oname + ".ObML",
          tdir.name());
    }
    if (!f.empty()) {
      ifs.open(f.c_str());
    }
    if (!ifs.is_open()) {
      log_error2("unable to open old file '" + f + "' for input", F, "rcm",
          USER);
    }
    ofs.open((tdir.name() + "/metadata/" + cmdir + "/" + nname + ".ObML").
        c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open new file for output", F, "rcm", USER);
    }
    auto re_ref = regex("ref=");
    auto re_parent = regex("parent=");
    ifs.getline(line, 32768);
    while (!ifs.eof()) {
      sline = line;
      if (regex_search(sline, re_uri)) {
        replace_uri(sline, cmdir);
      } else if (oname != nname && regex_search(sline, re_ref)) {
        ref_file = sline.substr(sline.find("ref=") + 5);
        ref_file = ref_file.substr(0, ref_file.find("\""));
        new_ref_file = substitute(ref_file, oname, nname);
        if (exists_on_server("rda-web-prod.ucar.edu", "/data/web/datasets/ds" +
            metautils::args.dsnum + "/metadata/" + cmdir + "/" + ref_file,
            metautils::directives.rdadata_home)) {
          std::ifstream ifs2(unixutils::remote_web_file(
              "https://rda.ucar.edu/datasets/ds" + metautils::args.dsnum +
              "/metadata/" + cmdir + "/" + ref_file, tdir.name()).c_str());
          if (ifs2.is_open()) {
            std::ofstream ofs2((tdir.name() + "/metadata/" + cmdir + "/" +
                new_ref_file).c_str());
            if (!ofs2.is_open()) {
              log_error2("could not open output file for a reference", F, "rcm",
                  USER);
            }
            ifs2.getline(line2, 32768);
            while (!ifs2.eof()) {
              sline2 = line2;
              if (regex_search(sline2, re_parent)) {
                sline2 = sline2.substr(0, sline2.find("\"") + 1) + nname +
                    ".ObML" + sline2.substr(sline2.find("\" group"));
              }
              ofs2 << sline2 << endl;
              ifs2.getline(line2, 32768);
            }
            ifs2.close();
            ifs2.clear();
            ofs2.close();

            // remove the old ref file
            if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" +
                metautils::args.dsnum + "/metadata/" + cmdir + "/" + ref_file,
                tdir.name(), metautils::directives.rdadata_home, error) < 0) {
              metautils::log_warning("rewrite_uri_in_cmd_file() could not "
                  "remove old reference file '" + ref_file + "'", "rcm", USER);
            }
            replace_all(sline, oname, nname);
          } else {
            log_error2("could not open reference file '" + ref_file + "'", F,
                "rcm", USER);
          }
        }
      }
      ofs << sline << endl;
      ifs.getline(line, 32768);
    }
    ifs.close();
    ofs.close();
    system(("gzip -f " + tdir.name() + "/metadata/" + cmdir + "/" + nname +
        ".ObML").c_str());
    if (oname != nname) {

       // remove the old file
       if (old_is_gzipped) {
         if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
             args.dsnum + "/metadata/" + cmdir + "/" + oname + ".ObML.gz", tdir.
             name(), metautils::directives.rdadata_home, error) < 0) {
          metautils::log_warning("rewrite_uri_in_cmd_file() could not remove " +
              oname + ".ObML - unixutils::rdadata_unsync error(s): '" + error +
              "'", "rcm", USER);
        }
      } else {
         if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
             args.dsnum + "/metadata/" + cmdir + "/" + oname + ".ObML", tdir.
             name(), metautils::directives.rdadata_home, error) < 0) {
          metautils::log_warning("rewrite_uri_in_cmd_file() could not remove " +
              oname + ".ObML - unixutils::rdadata_unsync error(s): '" + error +
              "'", "rcm", USER);
        }
      }
    }
  } else if (db == (db_prefix + "FixML")) {
    auto fixml_filename = unixutils::remote_web_file(
        "https://rda.ucar.edu/datasets/ds" + metautils::args.dsnum +
        "/metadata/" + cmdir + "/" + oname + ".FixML.gz", tdir.name());
    if (fixml_filename.empty()) {
      fixml_filename = unixutils::remote_web_file(
          "https://rda.ucar.edu/datasets/ds" + metautils::args.dsnum +
          "/metadata/" + cmdir + "/" + oname + ".FixML", tdir.name()).c_str();
    } else {
      system(("gunzip " + fixml_filename).c_str());
      chop(fixml_filename, 3);
    }
    ifs.open(fixml_filename.c_str());
    if (!ifs.is_open()) {
      log_error2("unable to open old file for input", F, "rcm", USER);
    }
    ofs.open((tdir.name() + "/metadata/" + cmdir + "/" + nname + ".FixML").
        c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open new file for output", F, "rcm", USER);
    }
    auto re_stage = regex("classification stage=");
    ifs.getline(line, 32768);
    while (!ifs.eof()) {
      sline = line;
      if (regex_search(sline, re_uri)) {
        replace_uri(sline, cmdir);
      } else if (oname != nname && regex_search(sline, re_stage)) {
        se.key = sline.substr(sline.find("classification stage=") + 22);
        if (!se.key.empty() && (idx = se.key.find("\"")) != string::npos) {
          se.key = se.key.substr(0, idx);
          if (!unique_stage_table.found(se.key, se)) {
            unique_stage_table.insert(se);
          }
        }
      }
      ofs << sline << endl;
      ifs.getline(line, 32768);
    }
    ifs.close();
    ofs.close();
    system(("gzip -f " + tdir.name() + "/metadata/" + cmdir + "/" + nname +
        ".FixML").c_str());
    if (oname != nname) {

       // remove the old file
       if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
           args.dsnum + "/metadata/" + cmdir + "/" + oname + ".FixML", tdir.
           name(), metautils::directives.rdadata_home, error) < 0) {
        metautils::log_warning("rewrite_uri_in_cmd_file() could not remove " +
            oname + ".FixML - unixutils::rdadata_unsync error(s): '" + error +
            "'", "rcm", USER);
      }
    }
  } else {
    log_error2("error: unable to rename files in database '" + db + "'", F,
        "rcm", USER);
  }

  // sync all of the new files
  if (unixutils::rdadata_sync(tdir.name(), "metadata/", "/data/web/datasets/ds"
      + g_new_dsnum,metautils::directives.rdadata_home,error) < 0) {
    log_error2("could not sync new file(s) - rdadata_sync error(s): '" + error +
        "'", F, "rcm", USER);
  }
}

bool renamed_cmd() {
  const string F = this_function_label(__func__);
  string dsnum2 = substitute(metautils::args.dsnum,".","");
  string new_dsnum2 = substitute(g_new_dsnum,".","");
  string error;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  string oname,nname,scm_flag,sdum;

  MySQL::Server server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  MySQL::Server server_d(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  string ftbl, col, dcol;
  if (regex_search(g_old_name, regex(
      "^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    ftbl = "_webfiles2";
    col = "webID";
    dcol = "wfile";
    if (!g_old_web_home.empty()) {
      oname = substitute(g_old_name, "http://rda.ucar.edu", "");
      replace_all(oname, "http://dss.ucar.edu", "");
      replace_all(oname, g_old_web_home+"/", "");
    } else {
      oname = metautils::relative_web_filename(g_old_name);
    }
    if (metautils::args.dsnum != g_new_dsnum) {
      auto d = metautils::args.dsnum;
      metautils::args.dsnum = g_new_dsnum;
      nname = metautils::relative_web_filename(g_new_name);
      metautils::args.dsnum = d;
    } else {
      nname = metautils::relative_web_filename(g_new_name);
    }
    scm_flag = "-wf";
  }
  auto databases = server.db_names();
  for (const auto& db : databases) {
    auto table_names = MySQL::table_names(server, db, "ds" + dsnum2 + ftbl,
        error);
    for (const auto& table : table_names) {
      query.set("code", db + "." + table, col + " = '" + oname + "'");
      if (query.submit(server) < 0) {
        log_error2("error: '" + query.error() + "'", F, "rcm", USER);
      }
      if (query.num_rows() > 0) {
        query2.set("code", db + "." + substitute(table, dsnum2, new_dsnum2),
            "binary " +  col  +  " = '" + nname + "'");
        if (query2.submit(server) < 0) {
          log_error2("error: '" + query2.error() + "'", F, "rcm", USER);
        }
        if (query2.num_rows() > 0) {
          if (!g_old_web_home.empty() && oname == nname) {
            rewrite_uri_in_cmd_file(db);
            exit(0);
          } else {
            log_error2("error: '" + g_new_name + "' is already in the content "
                "metadata database", F, "rcm", USER);
          }
        }
        if (db != "WGrML") {
          rewrite_uri_in_cmd_file(db);
        }
        if (g_new_dsnum == metautils::args.dsnum) {
          while (query.fetch_row(row)) {
            if (server.update(db + "." + table, col + " = '" + nname + "'",
                "code = " + row[0]) < 0) {
              log_error2("error: '" + server.error() + "'", F, "rcm", USER);
            }
            server.update(db + "." + table + "2", col + " = '" + nname + "'",
                "code = " + row[0]);
          }
        } else {
          stringstream oss, ess;
          if (mysystem2(metautils::directives.local_root + "/bin/dcm -d " +
              metautils::args.dsnum + " " + g_old_name, oss, ess) != 0) {
            cerr << ess.str() << endl;
          }
          auto scm_file = nname + ".";
          if (db.front() == 'W') {
            scm_file += db.substr(1);
          } else {
            scm_file += db;
          }
          if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
              g_new_dsnum + " " + scm_flag + " " + scm_file, oss, ess) != 0) {
            cerr << ess.str() << endl;
          }
          if (db == "WGrML") {

            // insert the inventory into the new dataset tables
            mysystem2(metautils::directives.local_root + "/bin/iinv -d " +
                g_new_dsnum + " -f " + substitute(nname, "/", "%") +
                ".GrML_inv", oss, ess);
            server.update("WGrML.ds" + substitute(g_new_dsnum, ".", "") +
                "_webfiles2", "inv = 'Y'", "webID = '" + nname + "'");
          }
        }
        auto d = db;
        replace_all(d, "ML", "");
        if (d[0] == 'W') {
          d = d.substr(1);
        }
        if (server_d.update(dcol, "meta_link = '" + d + "'", "dsid = 'ds" +
            metautils::args.dsnum + "' and " + dcol + " = '" + nname + "'") <
            0) {
          metautils::log_warning("renamed_cmd() returned warning: " +
              server_d.error(), "rcm", USER);
        }
        server.disconnect();
        server_d.disconnect();
        return true;
      }
    }
  }
  server.disconnect();
  server_d.disconnect();
  return false;
}

void show_usage() {
  cerr << "usage: rcm [-C] -d [ds]nnn.n [-nd [ds]nnn.n] [-o old_webhome] "
      "old_name new_name" << endl;
  cerr << "\nrequired:" << endl;
  cerr << "-d nnn.n        nnn.n is the dataset number where the original file "
      "resides" << endl;
  cerr << "old_name        old data file name, beginning with "
      "'https://rda.ucar.edu', or as" << endl;
  cerr << "                  for the -WF option of dsarch" << endl;
  cerr << "new_name        new data file name, beginning with "
      "'https://rda.ucar.edu', or as" << endl;
  cerr << "                  for the -WF option of dsarch" << endl;
  cerr << "\noptions:" << endl;
  cerr << "-C              no file list cache created (to save time)" << endl;
  cerr << "-nd nnn.n       new dataset number, if different from old dataset "
      "number" << endl;
  cerr << "-o old_webhome  use this option if the old web home is different "
      "from the new" << endl;
  cerr << "                  one" << endl;
}

int main(int argc, char **argv) {
  if (argc < 5 && argc != 2) {
    show_usage();
    exit(1);
  }
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '%');
  metautils::read_config("rcm", USER);
  auto sp = split(metautils::args.args_string, "%");
  bool no_cache = false;
  for (size_t n = 0; n < sp.size() - 2; ++n) {
    if (sp[n] == "-C") {
      no_cache = true;
    } else if (sp[n] == "-d") {
      metautils::args.dsnum = sp[++n];
    } else if (sp[n] == "-nd") {
      g_new_dsnum = sp[++n];
    } else if (sp[n] == "-o") {
      g_old_web_home = sp[++n];
      if (regex_search(g_old_web_home, regex("/$"))) {
        chop(g_old_web_home);
      }
    }
  }
  if (regex_search(metautils::args.dsnum, regex("^ds"))) {
    metautils::args.dsnum = metautils::args.dsnum.substr(2);
  }
  if (g_new_dsnum.empty()) {
    g_new_dsnum = metautils::args.dsnum;
  } else if (regex_search(g_new_dsnum, regex("^ds"))) {
    g_new_dsnum = g_new_dsnum.substr(2);
  }
  g_old_name = sp[sp.size() - 2];
  g_new_name = sp[sp.size() - 1];
  if (!regex_search(g_old_name, regex("^https://rda.ucar.edu"))) {
    g_old_name = "https://rda.ucar.edu" + metautils::directives.data_root_alias
        + "/ds" + metautils::args.dsnum + "/" + g_old_name;
  }
  if (!exists_on_server("rda-web-prod.ucar.edu",
      "/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/wfmd",
      metautils::directives.rdadata_home)) {
    cerr << "Error: metadata directory not found for ds" << metautils::
        args.dsnum << endl;
    exit(1);
  }
  if (!regex_search(g_new_name, regex("^https://rda.ucar.edu"))) {
    g_new_name = "https://rda.ucar.edu" + metautils::directives.data_root_alias
        + "/ds" + metautils::args.dsnum + "/" + g_new_name;
  }
  if (g_new_name == g_old_name && g_new_dsnum == metautils::args.dsnum) {
    cerr << "Error: new_name must be different from old_name" << endl;
    exit(1);
  }
  metautils::args.path = g_old_name;
  metautils::args.filename = g_new_name;
  while ( (metautils::args.path.length() + metautils::args.filename.length()) >
      254) {
    metautils::args.path.pop_back();
    metautils::args.filename.pop_back();
  }
  metautils::cmd_register("rcm", USER);
  string e;
  if (!verified_new_file_is_archived(e)) {
    log_error2("error: '" + e + "'", "main()", "rcm", USER);
  } else {
    if (!renamed_cmd()) {
      log_error2("error: no content metadata were found for '" + g_old_name +
          "'", "main()", "rcm", USER);
    }
    if (!no_cache) {
      gatherxml::summarizeMetadata::create_file_list_cache("Web", "rcm", USER);
    }
    return 0;
  }
}
