#include <iostream>
#include <fstream>
#include <stdio.h>
#include <sys/stat.h>
#include <regex>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <PostgreSQL.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using metautils::log_error;
using std::cerr;
using std::endl;
using std::regex;
using std::regex_search;
using std::stoi;
using std::string;
using std::stringstream;
using std::to_string;
using strutils::append;
using strutils::ds_aliases;
using strutils::ng_gdex_id;
using strutils::is_numeric;
using strutils::split;
using strutils::strand;
using strutils::to_sql_tuple_string;
using strutils::trim;
using unixutils::gdex_upload_dir;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror="";
string mywarning="";

struct LocalArgs {
  LocalArgs() : ds_set(), gindex(), start_date(), start_time(), end_date(),
      end_time(), tz(), start_date_flag(0), start_time_flag(0),
      end_date_flag(0), end_time_flag(0), db_start_date(), db_start_time(),
      db_end_date(), db_end_time() { }

  string ds_set, gindex;
  string start_date, start_time, end_date, end_time, tz;
  int start_date_flag, start_time_flag, end_date_flag, end_time_flag;
  string db_start_date, db_start_time, db_end_date, db_end_time;
} local_args;
const string G_USER=getenv("USER");

int check_date(string date, string& db_date, string type) {
  db_date = date;
  auto sp = split(date, "-");

  // check the year
  if (sp[0].length() != 4 || sp[0] < "1000" || sp[0] > "3000") {
    cerr << "Error: bad year in " << type << " date" << endl;
    exit(1);
  }
  auto flag = 1;
  if (sp.size() > 1) {

    // check the month
    if (sp[1].length() != 2 || sp[1] < "01" || sp[1] > "12") {
      cerr << "Error: bad month in " << type << " date" << endl;
      exit(1);
    }
    flag = 2;
  } else {
    if (type == "start") {
      db_date += "-01-01";
    } else {
      db_date += "-12-31";
    }
  }
  if (sp.size() > 2) {

    // check the day
    auto ndays = to_string(dateutils::days_in_month(stoi(sp[0]), stoi(sp[1])));
    if (sp[2].length() != 2 || sp[2] < "01" || sp[2] > ndays) {
      cerr << "Error: bad month in " << type << " date" << endl;
      exit(1);
    }
    flag = 3;
  } else {
    if (type == "start") {
      db_date += "-01";
    } else {
      db_date += "-" + to_string(dateutils::days_in_month(stoi(sp[0]), stoi(sp[
          1])));
    }
  }
  return flag;
}

int check_time(string time, string& db_time, string type) {
  db_time=time;
  auto sp = split(time, ":");

  // check the hour
  if (sp[0].length() != 2 || sp[0] < "00" || sp[0] > "23") {
    cerr << "Error: bad hour in " << type << " time" << endl;
    exit(1);
  }
  auto flag = 1;
  if (sp.size() > 1) {

    // check the minutes
    if (sp[1].length() != 2 || sp[1] < "00" || sp[1] > "59") {
      cerr << "Error: bad minutes in " << type << " time" << endl;
      exit(1);
    }
    flag = 2;
  } else {
    if (type == "start") {
      db_time += ":00:00";
    } else {
      db_time += ":59:59";
    }
  }
  if (sp.size() > 2) {

    // check the seconds
    if (sp[2].length() != 2 || sp[2] < "00" || sp[2] > "59") {
      cerr << "Error: bad seconds in " << type << " time" << endl;
      exit(1);
    }
    flag = 3;
  } else {
    if (type == "start") {
      db_time += ":00";
    } else {
      db_time += ":59";
    }
  }
  return flag;
}

void show_usage_and_exit() {
  cerr << "usage: sdp -d <nnn.n> -g <gindex> {-bd YYYY[-MM[-DD -bt "
      "HH[:MM[:SS]]]] -ed YYYY[-MM[-DD -et HH[:MM[:SS]]]]} [-tz <+|-nnnn>]" <<
      endl;
  cerr << endl;
  cerr << "required:" << endl;
  cerr << "-d <nnn.n>          dataset number as nnn.n (will also accept "
      "\"dsnnn.n\")" << endl;
  cerr << "-g <gindex>         group index number for which period is being set"
      << endl;
  cerr << "-bd YYYY[-MM[-DD]]  start date as YYYY-MM-DD, where month and day "
      "are optional" << endl;
  cerr << "-ed YYYY[-MM[-DD]]  end date as YYYY-MM-DD, where month and day are "
      "optional" << endl;
  cerr << "                    **NOTE: one or both of -bd and -ed must be "
      "specified" << endl;
  cerr << endl;
  cerr << "optional:" << endl;
  cerr << "-bt HH[:MM[:SS]]    start time as HH:MM:SS, where minutes and "
      "seconds are" << endl;
  cerr << "                    optional" << endl;
  cerr << "-et HH[:MM[:SS]]    end time as HH:MM:SS, where minutes and seconds "
      "are optional" << endl;
  cerr << "-tz <+|-nnnn>       timezone offset from UTC" << endl;
  exit(1);
}

void parse_args(int argc, char **argv) {
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '%');
  auto sp = split(metautils::args.args_string, "%");
  for (size_t n = 0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
      metautils::args.dsid = ng_gdex_id(sp[++n]);
      local_args.ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
    } else if (sp[n] == "-g") {
      local_args.gindex = sp[++n];
    } else if (sp[n] == "-bd") {
      local_args.start_date = sp[++n];
      local_args.start_date_flag = check_date(local_args.start_date, local_args.
          db_start_date, "start");
    } else if (sp[n] == "-bt") {
      local_args.start_time = sp[++n];
      local_args.start_time_flag = check_time(local_args.start_time, local_args.
          db_start_time, "start");
    } else if (sp[n] == "-ed") {
      local_args.end_date = sp[++n];
      local_args.end_date_flag = check_date(local_args.end_date, local_args.
          db_end_date, "end");
    } else if (sp[n] == "-et") {
      local_args.end_time = sp[++n];
      local_args.end_time_flag = check_time(local_args.end_time, local_args.
          db_end_time, "end");
    } else if (sp[n] == "-tz") {
      local_args.tz = sp[++n];
      if (!regex_search(local_args.tz, regex("^[+-]")) || !is_numeric(
          local_args.tz.substr(1))) {
        cerr << "Error: bad time zone specification" << endl;
        exit(1);
      }
    } else {
      cerr << "Warning: flag " << sp[n] << " is not a valid flag" << endl;
    }
  }
  if (metautils::args.dsid.empty()) {
    cerr << "Error: no or invalid dataset ID specified" << endl;
    exit(1);
  }
  if (local_args.gindex.empty()) {
    cerr << "Error: no group index specified" << endl;
    exit(1);
  }
  if (local_args.start_date.empty() && local_args.end_date.empty()) {
    cerr << "Error: no start or end date specified" << endl;
    exit(1);
  }
  if (!local_args.start_time.empty() && local_args.start_date_flag != 3) {
    cerr << "Error: you must specify a full start date to be able to specify a "
        "start time" << endl;
    exit(1);
  }
  if (!local_args.end_time.empty() && local_args.end_date_flag != 3) {
    cerr << "Error: you must specify a full end date to be able to specify an "
        "end time" << endl;
    exit(1);
  }
}

int main(int argc, char **argv) {
  if (argc < 7) {
    show_usage_and_exit();
  }
  metautils::read_config("sdp", G_USER);
  parse_args(argc, argv);
  Server server_m(metautils::directives.metadb_config);
  if (!server_m) {
    cerr << "Error connecting to metadata database" << endl;
    exit(1);
  }
  Server server_d(metautils::directives.rdadb_config);
  if (!server_d) {
    cerr << "Error connecting to RDADB database" << endl;
    exit(1);
  }
  LocalQuery query("type", "search.datasets", "dsid in " + local_args.ds_set);
  if (query.submit(server_m) < 0) {
    log_error("error '" + query.error() + "' while checking dataset type",
        "sdp", G_USER);
  }
  Row row;
  if (!query.fetch_row(row)) {
    cerr << "Error: " << metautils::args.dsid << " does not exist" << endl;
    exit(1);
  }
  if (row[0] == "I") {
    cerr << "Abort: " << metautils::args.dsid << " is an internal dataset" <<
        endl;
    exit(1);
  }
  auto schema_names = server_m.schema_names();
  auto found_cmd_db = false;
  for (const auto& db : schema_names) {
    if (table_exists(server_m, db + "." + metautils::args.dsid +
        "_primaries")) {
      found_cmd_db = true;
      break;
    }
  }
  if (found_cmd_db) {
    cerr << "Error: the dataset period has been set from content metadata and "
        "can't be modified with " << argv[0] << endl;
    exit(1);
  }
  query.set("gindex", "dssdb.dsperiod", "dsid in " + local_args.ds_set + " and "
      "gindex = " + local_args.gindex);
  if (query.submit(server_d) < 0) {
    cerr << "Error: " << query.error() << endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    cerr << "Error: the specified group index does not have an associated "
        "period - you must first create this association with the Metadata "
        "Manager" << endl;
    exit(1);
  }
  metautils::log_warning("You should consider generating file content metadata "
      "for this dataset", "sdp", G_USER);
  query.set("grpid", "dssdb.dsgroup", "dsid in " + local_args.ds_set + " and "
      "gindex = " + local_args.gindex);
  if (query.submit(server_d) < 0) {
    cerr << "Error: " << query.error() << endl;
    exit(1);
  }
  string group_ID;
  if (query.fetch_row(row)) {
    group_ID = row[0];
  } else {
    group_ID = "Entire Dataset";
  }
  auto *tdir = new TempDir();
  if (!tdir->create(metautils::directives.temp_path)) {
    log_error("unable to create temporary directory (1)", "sdp", G_USER);
  }
  auto old_ds_overview = unixutils::remote_web_file("https://rda.ucar.edu/"
      "datasets/" + metautils::args.dsid + "/metadata/dsOverview.xml", tdir->
      name());
  std::ifstream ifs(old_ds_overview.c_str());
  if (!ifs.is_open()) {
    log_error("unable to open overview XML file for " + metautils::args.dsid,
        "sdp", G_USER);
  }
  auto *sync_dir = new TempDir();
  if (!sync_dir->create(metautils::directives.temp_path)) {
    log_error("unable to create temporary directory (2)", "sdp", G_USER);
  }
  std::ofstream ofs((sync_dir->name() + "/dsOverview.xml").c_str());
  if (!ofs.is_open()) {
    log_error("unable to open output file for updated XML", "sdp", G_USER);
  }
  char line[32768];
  ifs.getline(line, 32768);
  while (!ifs.eof()) {
    string sline = line;
    if (sline.find("  <timeStamp") == 0) {
      ofs << "  <timeStamp value=\"" << dateutils::current_date_time().
          to_string("%Y-%m-%d %HH:%MM:%SS %Z") << "\" />" << endl;
    } else if (sline.find("    <temporal") == 0 && (regex_search(sline, regex("groupID=\"" + group_ID + "\"")) || (group_ID == "Entire Dataset" &&
        !regex_search(sline, regex("groupID"))))) {
      string new_line = "    <temporal";
      if (!local_args.start_date.empty()) {
        new_line += " start=\"" + local_args.start_date;
        if (!local_args.start_time.empty()) {
          new_line += " " + local_args.start_time;
          if (!local_args.tz.empty()) {
            new_line += " " + local_args.tz;
          } else {
            new_line += " +0000";
          }
        }
        new_line += "\"";
      } else {
        auto idx = sline.find("start=");
        auto sp = split(sline.substr(idx));
        new_line += " " + sp.front();
      }
      auto idx = sline.find("end=");
      if (!local_args.end_date.empty()) {
        new_line += " end=\"" + local_args.end_date;
        if (!local_args.end_time.empty()) {
          new_line += " " + local_args.end_time;
          if (!local_args.tz.empty()) {
            new_line += " " + local_args.tz;
          } else {
            new_line += " +0000";
          }
        }
        new_line += "\"";
      } else {
        auto sp = split(sline.substr(idx));
        new_line += " " + sp.front();
      }
      auto group_ID = sline.substr(idx + 5);
      idx=group_ID.find("\"");
      if (idx != string::npos) {
        group_ID = group_ID.substr(idx);
      }
      auto sp = split(group_ID);
      for (size_t n = 1; n < sp.size(); ++n) {
        new_line += " " + sp[n];
      }
      ofs << new_line << endl;
    } else {
      ofs << line << endl;
    }
    ifs.getline(line, 32768);
  }
  ifs.close();
  delete tdir;
  ofs.close();
  auto cvs_key = strand(15);
  stringstream oss, ess;

  // get the name of the production web server
  if (mysystem2("/usr/bin/curl -s --data 'authKey=qGNlKijgo9DJ7MN&cmd="
      "identify' https://rda.ucar.edu/cgi-bin/remoteRDAServerUtils", oss, ess)
      != 0 || oss.str().find("Identify:") != 0) {
    log_error("unable to identify web server - error: '" + ess.str() + "'",
        "sdp", G_USER);
  }
  auto sp = split(oss.str(), "Identify:");
  auto host = sp.back();
  trim(host);
  sp = split(host, ".");

  // copy the new dataset overview to the production web server
  if (mysystem2("/bin/sh -c \"rsync -rptgD -e __INNER_QUOTE__ssh -i " +
      metautils::directives.rdadata_home + "/.ssh/" + sp.front() +
      "-sync_rdadata_rsa -l rdadata__INNER_QUOTE__ " + sync_dir->name() +
      "/dsOverview.xml " + sp.front() + ".ucar.edu:/" + sp.front() + "/web/"
      + metautils::args.dsid + ".xml." + cvs_key + "\"", oss, ess) != 0) {
    log_error("unable to web-sync file for CVS - error: '" + ess.str() + "'",
        "sdp", G_USER);
  }

  // add the new dataset overview to the CVS version control on the
  //   production web server
  mysystem2("/usr/bin/curl -s --data 'authKey=qGNlKijgo9DJ7MN&cmd=cvssdp&"
      "dsnum=" + metautils::args.dsid + "&key=" + cvs_key + "' https://rda."
      "ucar.edu/cgi-bin/remoteRDAServerUtils", oss, ess);
  if (!oss.str().empty()) {
    log_error("cvs error(s): " + oss.str(), "sdp", G_USER);
  }
  string error;

  // sync the new overview to the dataset metadata directory on all sync hosts
  if (gdex_upload_dir(sync_dir->name(), ".", "/data/web/datasets/" + metautils::
      args.dsid + "/metadata", "", error) <
      0) {
    log_error("unable to gdex_upload_dir updated XML file - error(s): '" + error
        + "'", "sdp", G_USER);
  }
  delete sync_dir;

  // update the temporal range in RDADB
  string update_string;
  if (!local_args.db_start_date.empty()) {
    append(update_string, "date_start = '" + local_args.db_start_date + "'",
        ", ");
  }
  if (!local_args.db_start_time.empty()) {
    append(update_string, "time_start = '" + local_args.db_start_time + "'",
        ", ");
  }
  if ( (local_args.start_date_flag + local_args.start_time_flag) > 0) {
    append(update_string, "start_flag = " + to_string(local_args.start_date_flag
        + local_args.start_time_flag), ", ");
  }
  if (!local_args.db_end_date.empty()) {
    append(update_string, "date_end = '" + local_args.db_end_date + "'", ", ");
  }
  if (!local_args.db_end_time.empty()) {
    append(update_string, "time_end = '" + local_args.db_end_time + "'", ", ");
  }
  if ( (local_args.end_date_flag + local_args.end_time_flag) > 0) {
    append(update_string, "end_flag = " + to_string(local_args.end_date_flag +
        local_args.end_time_flag), ", ");
  }
  if (!local_args.tz.empty()) {
    append(update_string, "time_zone = '" + local_args.tz + "'", ", ");
  }
  if (server_d.update("dssdb.dsperiod", update_string, "dsid in " + local_args.
      ds_set + " and gindex = " + local_args.gindex) < 0) {
    log_error("while updating dssdb.dsperiod: '" + server_d.error() + "'",
        "sdp", G_USER);
  }
  mysystem2(metautils::directives.local_root + "/bin/dsgen " + metautils::args.
      dsid, oss, ess);
  server_d.disconnect();
  server_m.disconnect();
}
