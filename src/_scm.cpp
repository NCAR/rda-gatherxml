#include <iostream>
#include <fstream>
#include <thread>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <ftw.h>
#include <signal.h>
#include <pthread.h>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bsort.hpp>
#include <bitmap.hpp>
#include <xml.hpp>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <metahelpers.hpp>
#include <xmlutils.hpp>
#include <search.hpp>
#include <timer.hpp>
#include <myerror.hpp>

using namespace MySQL;
using dateutils::string_date_to_ll_string;
using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::list;
using std::make_pair;
using std::make_shared;
using std::move;
using std::pair;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::sort;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ftos;
using strutils::chop;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::strand;
using strutils::substitute;
using strutils::to_lower;
using strutils::trim;
using strutils::trimmed;
using unixutils::mysystem2;
using unixutils::remote_web_file;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

struct LocalArgs {
  LocalArgs() : dsnum2(), summ_type(), file(), temp_directory(),
      cmd_directory(), data_format(), gindex_list(), summarize_all(false),
      added_variable(false), verbose(false), notify(false), update_graphics(
      true), update_db(true), refresh_web(false), refresh_inv(false),
      is_web_file(false), summarized_hpss_file(false), summarized_web_file(
      false) { }

  string dsnum2;
  string summ_type;
  string file, temp_directory, cmd_directory, data_format;
  vector<string> gindex_list;
  bool summarize_all;
  bool added_variable, verbose, notify;
  bool update_graphics, update_db;
  bool refresh_web, refresh_inv, is_web_file;
  bool summarized_hpss_file, summarized_web_file;
} local_args;

TempDir g_temp_dir;

void parse_args(const char arg_delimiter) {
  auto args = split(metautils::args.args_string, string(1, arg_delimiter));
  for (size_t n = 0; n < args.size(); ++n) {
    if (to_lower(args[n]) == "-wa") {

      // need to_lower() because dsarch often uses -WA
      if (!local_args.file.empty()) {
        cerr << "scm: specify only one of -wa or -wf" << endl;
        exit(1);
      } else {
        local_args.summarize_all = true;
        if (n + 1  < args.size() && args[n + 1][0] != '-') {
          local_args.summ_type = args[++n];
        }
        local_args.cmd_directory = "wfmd";
      }
    } else if (args[n] == "-d") {
      metautils::args.dsnum = args[++n];
      if (metautils::args.dsnum.find("ds") == 0) {
        metautils::args.dsnum = metautils::args.dsnum.substr(2);
      }
      local_args.dsnum2 = strutils::substitute(metautils::args.dsnum, ".", "");
    } else if (args[n] == "-wf") {
      if (local_args.summarize_all) {
        cerr << "scm: specify only one of -wa or -wf" << endl;
        exit(1);
      } else {
        if (n + 1 < args.size()) {
          local_args.file = args[++n];
          local_args.is_web_file = true;
        } else {
          cerr << "scm: the -wf flag requires a file name" << endl;
          exit(1);
        }
      }
    } else if (args[n] == "-G") {
      local_args.update_graphics = false;
    } else if (args[n] == "-N") {
      local_args.notify = true;
    } else if (args[n] == "-rw") {
      local_args.refresh_web = true;
      if (n + 1 < args.size() && args[n + 1][0] != '-') {
        local_args.gindex_list.emplace_back(args[++n]);
      }
    } else if (args[n] == "-ri") {
      local_args.refresh_inv = true;
      if (n + 1 < args.size() && args[n + 1][0] != '-') {
        local_args.gindex_list.emplace_back(args[++n]);
      }
    } else if (args[n] == "-S") {
      local_args.update_db = false;
    } else if (args[n] == "-R") {
      metautils::args.regenerate = false;
    } else if (args[n] == "-t") {
      local_args.temp_directory = args[++n];
    } else if (args[n] == "-V") {
      local_args.verbose = true;
    } else {
      myerror = "Terminating - scm: don't understand argument " + args[n];
      exit(1);
    }
  }
  if (metautils::args.dsnum.empty()) {
    cerr << "scm: no dataset number specified" << endl;
    exit(1);
  }
  if (!local_args.summarize_all && !local_args.is_web_file && !local_args.
      refresh_web && !local_args.refresh_inv) {
    myerror = "Terminating - scm - nothing to do";
    exit(1);
  }
  if (local_args.update_graphics && !local_args.file.empty() && !regex_search(
      local_args.file, regex("(Ob|Fix)ML$"))) {
    local_args.update_graphics = false;
  }
}

void delete_temporary_directory() {
  if (!local_args.temp_directory.empty()) {
    stringstream oss, ess;
    mysystem2("/bin/rm -rf " + local_args.temp_directory, oss, ess);
    if (ess.str().empty()) {
      local_args.temp_directory = "";
    }
  }
}

string table_code(Server& srv, string table_name, string where_conditions, bool
    do_insert = true) {
// where_conditions must have 'and' specified as 'AND' because it is possible
//   that 'and' is in fields in the database tables

  static const string F = this_function_label(__func__);
  replace_all(where_conditions, " &eq; ", " = ");
  LocalQuery q;
  q.set("code", table_name, where_conditions);
  if (q.submit(srv) < 0) {
    log_error2("error: '" + q.error() + " from query: '" + q.show() + "'", F,
        "scm", USER);
  }
  if (q.num_rows() == 0) {
    if (!do_insert) {
      return "";
    }
    string cols, vals;
    auto sp = split(where_conditions, " AND ");
    for (size_t n = 0; n < sp.size(); ++n) {
      auto sp2 = split(sp[n], " = ");
      if (sp2.size() != 2) {
        log_error2("error in where_conditions: " + where_conditions + ", " + sp[
            n], F, "scm", USER);
      }
      auto s = sp2[0];
      trim(s);
      if (!cols.empty()) {
        cols += ", ";
      }
      cols += s;
      s = sp2[1];
      trim(s);
      replace_all(s, " &eq; ", " = ");
      if (!vals.empty()) {
        vals += ", ";
      }
      vals += s;
    }
    string r;
    if (srv.command("lock table " + table_name + " write", r) < 0) {
      log_error2(srv.error(), F, "scm", USER);
    }
    if (srv.insert(table_name, cols, vals, "") < 0) {
      if (srv.error().find("Duplicate entry") != 0) {
        log_error2("server error: '" + srv.error() + "' while inserting (" +
            cols + ") values(" + vals + ") into " + table_name, F, "scm", USER);
      }
    }
    if (srv.command("unlock tables", r) < 0) {
      log_error2(srv.error(), F, "scm", USER);
    }
    q.submit(srv);
    if (q.num_rows() == 0) { // this really should not happen if insert worked
      return "";
    }
  }
  Row r;
  q.fetch_row(r);
  return std::move(r[0]);
}

string grid_definition_parameters(const XMLElement& e) {
  auto v = e.attribute_value("definition");
  if (v == "latLon") {
    return e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" +
        e.attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("endLat") + ":" + e.attribute_value("endLon") +
        ":" + e.attribute_value("xRes") + ":" + e.attribute_value("yRes");
  } else if (v == "gaussLatLon") {
    return e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" +
        e.attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("endLat") + ":" + e.attribute_value("endLon") +
        ":" + e.attribute_value("xRes") + ":" + e.attribute_value("circles");
  } else if (v == "polarStereographic") {
    return e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" +
        e.attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("resLat") + ":" + e.attribute_value("projLon") +
        ":" + e.attribute_value("pole") + ":" + e.attribute_value("xRes") + ":"
        + e.  attribute_value("yRes");
  } else if (v == "lambertConformal") {
    return e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" +
        e.attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("resLat") + ":" + e.attribute_value("projLon") +
        ":" + e.attribute_value("pole") + ":" + e.attribute_value("xRes") + ":"
        + e.attribute_value("yRes") + ":" + e.attribute_value("stdParallel1") +
        ":" + e.attribute_value("stdParallel2");
  }
  return "";
}

void open_markup_file(XMLDocument& xdoc, string filename) {
  auto f = filename;
  if (local_args.temp_directory.empty()) {
    f = remote_web_file("https://rda.ucar.edu" + filename, g_temp_dir.name());
    if (f.empty()) {
      f = remote_web_file("https://rda.ucar.edu" + filename + ".gz", g_temp_dir.
          name());
    }
  }
  if (!xdoc.open(f)) {
    log_error2("unable to open " + filename, "open_markup_file()", "scm", USER);
  }
}

struct MarkupParameters {
  MarkupParameters() : markup_type(), xdoc(), element(), data_type(), server(),
      database(), file_type(), filename(), format(), file_map(), format_map() { }

  string markup_type;
  XMLDocument xdoc;
  string element, data_type;
  Server server;
  string database, file_type, filename, format;
  unordered_map<string, string> file_map, format_map;
};

void initialize_web_file(MarkupParameters *markup_parameters) {
  static const string F = this_function_label(__func__);
  Server mysrv_d(metautils::directives.database_server, metautils::directives.
      rdadb_username, metautils::directives.rdadb_password, "dssdb");
  if (!mysrv_d) {
    log_error2("could not connect to RDADB - error: '" + mysrv_d.error() + "'",
        F, "scm", USER);
  }
  markup_parameters->filename = metautils::relative_web_filename(
      markup_parameters->filename);
  LocalQuery q("tindex", "dssdb.wfile", "wfile = '" + markup_parameters->
      filename + "'");
  Row r;
  if (q.submit(mysrv_d) == 0 && q.fetch_row(r) && r[0] != "0") {
    local_args.gindex_list.emplace_back(r[0]);
  }
  markup_parameters->database = "W" + markup_parameters->markup_type;
  markup_parameters->file_type = "web";
  local_args.summarized_web_file = true;
  if (mysrv_d.update("wfile", "meta_link = '" + substitute(markup_parameters->
      markup_type, "ML", "") + "'", "dsid = 'ds" + metautils::args.dsnum +
      "' and wfile = '" + markup_parameters->filename + "'") < 0) {
    log_error2("error: '" + mysrv_d.error() + "' while trying to update "
        "'dssdb.wfile'", F, "scm", USER);
  }
  mysrv_d.disconnect();
}

void initialize_file(MarkupParameters *markup_parameters) {
  const static string F = this_function_label(__func__);
  markup_parameters->filename = markup_parameters->xdoc.element(
      markup_parameters->markup_type).attribute_value("uri");
  if (regex_search(markup_parameters->filename, regex(
      "^file://MSS:/FS/DECS"))) {
delete_temporary_directory();
myerror = "Terminating - scm no longer works on HPSS files";
exit(1);
  } else if (regex_search(markup_parameters->filename, regex(
      "^http(s){0,1}://rda\\.ucar\\.edu")) || regex_search(markup_parameters->
      filename, regex("^file://web:"))) {
    replace_all(markup_parameters->filename, "file://web:", "");
    initialize_web_file(markup_parameters);
  } else {
    log_error2("invalid uri '" + markup_parameters->filename + "' in xml file",
        F, "scm", USER);
  }
}

void process_data_format(MarkupParameters *markup_parameters) {
  static const string F = this_function_label(__func__);
  markup_parameters->format = markup_parameters->xdoc.element(
      markup_parameters->markup_type).attribute_value("format");
  if (markup_parameters->format.empty()) {
    log_error2("missing " + markup_parameters->database + " format attribute",
        F, "scm", USER);
  }
  if (markup_parameters->server.insert("search.formats", "keyword, vocabulary, "
      "dsid", "'" + markup_parameters->format + "', '" + markup_parameters->
      database + "', '" + metautils::args.dsnum + "'", "update dsid = values("
      "dsid)") < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "inserting into search.formats", F, "scm", USER);
  }
  if (local_args.data_format.empty()) {
    local_args.data_format = markup_parameters->format;
  } else if (markup_parameters->format != local_args.data_format) {
    local_args.data_format = "all";
  }
  if (markup_parameters->format_map.find(markup_parameters->format) ==
      markup_parameters->format_map.end()) {
    auto c = table_code(markup_parameters->server, markup_parameters->database +
        ".formats", "format = '" + markup_parameters->format + "'");
    if (c.empty()) {
      log_error2("unable to get format code", F, "scm", USER);
    }
    markup_parameters->format_map.emplace(markup_parameters->format, c);
  }
}

void create_grml_tables(MarkupParameters *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto tb_base = markup_parameters->database + ".ds" + local_args.dsnum2;
  string r;
  if (markup_parameters->server.command("create table " + tb_base + "_" +
      markup_parameters->file_type + "files2 like " + markup_parameters->
      database + ".template_" + markup_parameters->file_type + "files2", r) <
      0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_" + markup_parameters->file_type +
        "files2", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_levels like " + markup_parameters->database + ".template_levels", r) <
      0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_levels", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base + "_grids2 "
      "like " + markup_parameters->database + ".template_grids2", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_grids2", F, "scm", USER);
  }
}

void create_obml_tables(MarkupParameters *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto tb_base = "WObML.ds" + local_args.dsnum2;
  string r;
  if (markup_parameters->server.command("create table " + tb_base + "_" +
      markup_parameters->file_type + "files2 like " + markup_parameters->
      database + ".template_" + markup_parameters->file_type + "files2", r) <
      0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_" + markup_parameters->file_type +
        "files2", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_locations like " + markup_parameters->database + ".template_locations",
      r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_locations", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_location_names like " + markup_parameters->database +
      ".template_location_names", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_location_names", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_data_types like " + markup_parameters->database +
      ".template_data_types", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_data_types", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_data_types_list like " + markup_parameters->database +
      ".template_data_types_list", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_data_types_list", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_frequencies like " + markup_parameters->database +
      ".template_frequencies", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_frequencies", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base + "_ids "
      "like " + markup_parameters->database + ".template_ids", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_ids", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base + "_id_list "
      "like " + markup_parameters->database + ".template_id_list", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_id_list", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_geobounds like " + markup_parameters->database + ".template_geobounds",
      r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_geobounds", F, "scm", USER);
  }
}

void create_fixml_tables(MarkupParameters *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto tb_base = markup_parameters->database + ".ds" + local_args.dsnum2;
  string r;
  if (markup_parameters->server.command("create table " + tb_base + "_" +
      markup_parameters->file_type + "files2 like " + markup_parameters->
      database + "template_" + markup_parameters->file_type + "files2", r) <
      0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_" + markup_parameters->file_type +
        "files2", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base + "_id_list "
      "like " + markup_parameters->database + ".template_id_list", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_id_list", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_locations like " + markup_parameters->database + ".template_locations",
      r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_locations", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_location_names like " + markup_parameters->database +
      ".template_location_names", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_location_names", F, "scm", USER);
  }
  if (markup_parameters->server.command("create table " + tb_base +
      "_frequencies like " + markup_parameters->database +
      ".template_frequencies", r) < 0) {
    log_error2("error: '" + markup_parameters->server.error() + "' while "
        "creating table " + tb_base + "_frequencies", F, "scm", USER);
  }
}

void create_tables(MarkupParameters *markup_parameters) {
  if (markup_parameters->markup_type == "GrML") {
    create_grml_tables(markup_parameters);
  } else if (markup_parameters->markup_type == "ObML") {
    create_obml_tables(markup_parameters);
  } else if (markup_parameters->markup_type == "FixML") {
    create_fixml_tables(markup_parameters);
  }
}

void insert_filename(MarkupParameters *markup_parameters) {
  const static string F = this_function_label(__func__);
  if (markup_parameters->file_map.find(markup_parameters->filename) ==
      markup_parameters->file_map.end()) {
    auto tb_nam = markup_parameters->database + ".ds" + local_args.dsnum2 +
        "_webfiles2";
    if (!table_exists(markup_parameters->server, tb_nam)) {
      create_tables(markup_parameters);
    }
    auto c = table_code(markup_parameters->server, tb_nam, "id = '" +
        markup_parameters->filename + "'", false);
    if (c.empty()) {
      if (markup_parameters->server.insert(
            tb_nam,
            "id, format_code, num_" + markup_parameters->data_type + ", "
                "start_date, end_date, uflag",
            "'" + markup_parameters->filename + "', " + markup_parameters->
                format_map[markup_parameters->format] + ", 0, 0, 0, '" + strand(
                5) + "'",
            ""
            ) < 0) {
        log_error2("error: '" + markup_parameters->server.error() + " while "
            "inserting into " + tb_nam, F, "scm", USER);
      }
      c = table_code(markup_parameters->server, tb_nam, "id = '" +
          markup_parameters->filename + "'", false);
      if (c.empty()) {
        log_error2("error: unable to retrieve code from '" + tb_nam + "' for "
            "value '" + markup_parameters->filename + "'", F, "scm", USER);
      }
    }
    markup_parameters->file_map.emplace(markup_parameters->filename, c);
    if (markup_parameters->server.update(tb_nam, "format_code = " +
       markup_parameters->format_map[markup_parameters->format], "code = " + c)
       < 0) {
      log_error2("error: '" + markup_parameters->server.error() + "' while "
          "updating " + tb_nam + " with format_code and code", F, "scm", USER);
    }
  }
}

void clear_grml_tables(MarkupParameters *markup_parameters) {
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_processes", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_ensembles", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
}

void clear_obml_tables(MarkupParameters *markup_parameters) {
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_id_list", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_data_types", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_frequencies", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_geobounds", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_location_names", "file_code = " +
      markup_parameters->file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_locations", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
}

void clear_satml_tables(MarkupParameters *markup_parameters) {
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_products", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
}

void clear_fixml_tables(MarkupParameters *markup_parameters) {
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_locations", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_frequencies", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
  markup_parameters->server._delete(markup_parameters->database + ".ds" +
      local_args.dsnum2 + "_id_list", "file_code = " + markup_parameters->
      file_map[markup_parameters->filename]);
}

void clear_tables(MarkupParameters *markup_parameters) {
  if (markup_parameters->markup_type == "GrML") {
    clear_grml_tables(markup_parameters);
  } else if (markup_parameters->markup_type == "ObML") {
    clear_obml_tables(markup_parameters);
  } else if (markup_parameters->markup_type == "SatML") {
    clear_satml_tables(markup_parameters);
  } else if (markup_parameters->markup_type == "FixML") {
    clear_fixml_tables(markup_parameters);
  }
}

struct GrMLParameters : public MarkupParameters {
  GrMLParameters() : prod_set(), summ_lev(false) {
    markup_type = "GrML";
    element = "grid";
    data_type = "grids";
  }

  unordered_set<string> prod_set;
  bool summ_lev;
};

struct ParameterData {
  ParameterData() : min_nsteps(0x7fffffff), max_nsteps(0), level_codes(),
      level_code_list() { }

  size_t min_nsteps, max_nsteps;
  unordered_set<size_t> level_codes;
  vector<size_t> level_code_list;
};

void process_grml_markup(void *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto gp = reinterpret_cast<GrMLParameters *>(markup_parameters);
  static xmlutils::ParameterMapper PMAP(metautils::directives.
      parameter_map_path);
  unordered_map<string, string> tr_map, gd_map, lev_map;
  unordered_map<string, shared_ptr<ParameterData>> pdmap;
  unordered_set<string> p_set;
  string mndt = "3000-12-31 23:59 +0000";
  string mxdt = "0001-01-01 00:00 +0000";
  auto cnt = 0;
  auto found_process = false, found_ensemble = false;
  auto uflg = strand(3);
  for (const auto& g : gp->xdoc.element_list("GrML/" + gp->element)) {
    auto tr = g.attribute_value("timeRange");
    auto prod = to_lower(tr);
    if (prod.find("forecast") != string::npos) {
      prod = prod.substr(0, prod.find("forecast") + 8);
      auto sp = split(prod);
      if (sp.size() > 2) {
        prod = sp[sp.size() - 2] + " " + sp[sp.size() - 1];
      }
    }
    if (gp->prod_set.find(prod) == gp->prod_set.end()) {
      gp->prod_set.emplace(prod);
    }
    auto def = g.attribute_value("definition");
    string defp = "";
    for (const auto& a : g.attribute_list()) {
      auto n = a.name;
      if (n != "timeRange" && n != "definition") {
        if (n == "isCell") {
          def += "Cell";
        } else {
          if (!defp.empty()) {
            defp += ":";
          }
          defp += g.attribute_value(n);
        }
      }
    }
    if (tr_map.find(tr) == tr_map.end()) {
      auto c = table_code(gp->server, gp->database + ".time_ranges",
          "time_range = '" + tr + "'");
      if (c.empty()) {
        log_error2("unable to get time range code", F, "scm", USER);
      }
      tr_map.emplace(tr, c);
    }
    auto gdef = def + ":" + defp;
    if (gd_map.find(gdef) == gd_map.end()) {
      LocalQuery q;
      auto c = table_code(gp->server, gp->database + ".grid_definitions",
          "definition = '" + def + "' AND def_params = '" + defp + "'");
      if (c.empty()) {
        log_error2("unable to get grid definition code", F, "scm", USER);
      }
      gd_map.emplace(gdef, c);
    }
    auto nens = g.element_list("ensemble").size();
    if (nens == 0) {
      nens = 1;
    }
    pdmap.clear();
    for (const auto& ge : g.element_addresses()) {
      auto enam = ge.p->name();
      if (enam == "process") {
        if (!found_process) {
          if (!table_exists(gp->server, gp->database + ".ds" + local_args.dsnum2              + "_processes")) {
            string r;
            if (gp->server.command("create table " + gp->database + ".ds" +
                local_args.dsnum2 + "_processes like " + gp->database +
                ".template_processes", r) < 0) {
              log_error2("error: '" + gp->server.error() + "' while creating "
                  "table " + gp->database + ".ds" + local_args.dsnum2 +
                  "_processes", F, "scm", USER);
            }
          }
          found_process = true;
        }
        if (gp->server.insert(gp->database + ".ds" + local_args.dsnum2 +
            "_processes", gp->file_map[gp->filename] + ", " + tr_map[tr] + ", "
            + gd_map[gdef] + ", '" + ge.p->attribute_value("value") + "'") <
            0) {
          if (gp->server.error().find("Duplicate entry") == string::npos) {
            log_error2("error: '" + gp->server.error() + "' while inserting "
                "into " + gp->database + ".ds" + local_args.dsnum2 +
                "_processes", F, "scm", USER);
          }
        }
      } else if (enam == "ensemble") {
        if (!found_ensemble) {
          if (!table_exists(gp->server, gp->database + ".ds" + local_args.
              dsnum2 + "_ensembles")) {
            string r;
            if (gp->server.command("create table " + gp->database + ".ds" +
                local_args.dsnum2 + "_ensembles like " + gp->database +
                ".template_ensembles", r) < 0) {
              log_error2("error: '" + gp->server.error() + " while creating "
                  "table " + gp->database + ".ds" + local_args.dsnum2 +
                  "_ensembles", F, "scm", USER);
            }
          }
          found_ensemble = true;
        }
        auto v = ge.p->attribute_value("size");
        if (v.empty()) {
          v = "0";
        }
        if (gp->server.insert(gp->database + ".ds" + local_args.dsnum2 +
            "_ensembles", gp->file_map[gp->filename] + ", " + tr_map[tr] + ", "
            + gd_map[gdef] + ", '" + ge.p->attribute_value("type") + "', '" +
            ge.p->attribute_value("ID") + "', " + v) < 0) {
          if (gp->server.error().find("Duplicate entry") == string::npos) {
            log_error2("error: '" + gp->server.error() + "' while inserting "
                "into " + gp->database + ".ds" + local_args.dsnum2 +
                "_ensembles", F, "scm", USER);
          }
        }
      } else if (enam == "level" || enam == "layer") {
        auto lm = ge.p->attribute_value("map");
        auto lt = ge.p->attribute_value("type");
        auto lv = ge.p->attribute_value("value");
        if (lv.empty()) {
          lv = ge.p ->attribute_value("bottom") + "," + ge.p->attribute_value(
              "top");
        }
        auto ltyp = lm + ":" + lt + ":" + lv;
        if (lev_map.find(ltyp) == lev_map.end()) {
          auto c = table_code(gp->server, gp->database + ".levels", "map = '" +
              lm + "' AND type = '" + lt + "' AND value = '" + lv + "'");
          if (c.empty()) {
            log_error2("unable to get level code", F, "scm", USER);
          }
          lev_map.emplace(ltyp, c);
        }
        if (gp->server.insert(gp->database + ".ds" + local_args.dsnum2 +
            "_levels", gp->format_map[gp->format] + ", " + lev_map[ltyp]) < 0) {
          if (gp->server.error().find("Duplicate entry") == string::npos) {
            log_error2("error: '" + gp->server.error() + "' while inserting "
                "into " + gp->database + ".ds" + local_args.dsnum2 + "_levels",
                F, "scm", USER);
          }
        } else {
          gp->summ_lev = true;
        }

        // parameters
        for (const auto& e : ge.p->element_addresses()) {
          auto lm = e.p->attribute_value("map");
          auto lv = e.p->attribute_value("value");
          size_t nsteps = stoi(e.p->attribute_value("nsteps"));
          cnt += nsteps;
          nsteps /= nens;
          auto x = e.p->attribute_value("start");
          if (x < mndt) {
            mndt = x;
          }
          x = e.p->attribute_value("end");
          if (x > mxdt) {
            mxdt = x;
          }
          auto p = lm + ":" + lv + "@" + lt;
          if (p[0] == ':') {
            p = p.substr(1);
          }
          if (p_set.find(p) == p_set.end()) {
            auto d = PMAP.description(gp->format, p);
            replace_all(d, "'", "\\'");
            if (gp->server.insert("search.variables", "'" + d + "', 'CMDMAP', "
                "'" + metautils::args.dsnum + "'") < 0) {
              if (gp->server.error().find("Duplicate entry") == string::npos) {
                log_error2("error: '" + gp->server.error() + "' while "
                "inserting into search.variables", F, "scm", USER);
              }
            } else {
              local_args.added_variable = true;
            }
            p_set.emplace(p);
          }
          auto sv = string_date_to_ll_string(e.p->attribute_value("start").
              substr(0, 16));
          auto ev = string_date_to_ll_string(e.p->attribute_value("end").
              substr(0, 16));
          if (p.find("@") != string::npos) {
            p = p.substr(0, p.find("@"));
          }
          p += "<!>" + sv + "<!>" + ev + "<!>" + itos(nsteps);
          auto i = stoi(lev_map[ltyp]);
          if (pdmap.find(p) == pdmap.end()) {
            pdmap.emplace(p, shared_ptr<ParameterData>(new ParameterData));
          }
          if (pdmap[p]->level_codes.find(i) == pdmap[p]->level_codes.
              end()) {
            pdmap[p]->level_codes.emplace(i);
            pdmap[p]->level_code_list.emplace_back(i);
          }
        }
      }
    }
    for (auto& e : pdmap) {
      sort(e.second->level_code_list.begin(), e.second->level_code_list.end(),
      [](const size_t& left, const size_t& right) -> bool {
        if (left <= right) {
          return true;
        }
        return false;
      });
      string s;
      bitmap::compress_values(e.second->level_code_list, s);
      auto tbl = gp->database + ".ds" + local_args.dsnum2 + "_grids2";
      auto sp = split(e.first, "<!>");
      auto inserts = gp->file_map[gp->filename] + ", " + tr_map[tr] + ", " +
          gd_map[gdef] + ", '" + sp[0] + "', '" + s + "', " + sp[1] + ", " + sp[
          2] + ", " + sp[3] + ", '" + uflg + "'";
      if (gp->server.insert(
            tbl,
            "file_code, time_range_code, grid_definition_code, parameter, "
                "level_type_codes, start_date, end_date, nsteps, uflg",
            inserts,
            "update uflg = values(uflg)"
            ) < 0) {
        log_error2("error: '" + gp->server.error() + " while inserting '" +
            inserts + "' into '" + tbl + "'", F, "scm", USER);
      }
      e.second.reset();
    }
  }
  gp->server._delete(gp->database + ".ds" + local_args.dsnum2 + "_grids2",
      "file_code = " + gp->file_map[gp->filename] + " and uflg != '" + uflg +
      "'");
  mndt = string_date_to_ll_string(mndt);
  replace_all(mndt, "+0000", "");
  mxdt = string_date_to_ll_string(mxdt);
  replace_all(mxdt, "+0000", "");
  auto s = "num_grids = " + itos(cnt) + ", start_date = " + mndt + ", end_date "
      "= " + mxdt;
  auto tb_nam = gp->database + ".ds" + local_args.dsnum2 + "_" + gp->file_type +
      "files2";
  if (gp->server.update(tb_nam, s + ", uflag = '" + strand(5) + "'", "code = " +
       gp->file_map[gp->filename]) < 0) {
    log_error2("error: '" + gp->server.error() + " while updating " + tb_nam, F,
        "scm", USER);
  }
  if (!table_exists(gp->server, gp->database + ".ds" + local_args.dsnum2 +
      "_agrids2")) {
    string r;
    if (gp->server.command("create table " + gp->database + ".ds" + local_args.
        dsnum2 + "_agrids2 like " + gp->database + ".template_agrids2", r) <
        0) {
      log_error2("error: '" + gp->server.error() + " while creating table " +
          gp->database + ".ds" + local_args.dsnum2 + "_agrids2", F, "scm",
          USER);
    }
  }
  if (!table_exists(gp->server, gp->database + ".ds" + local_args.dsnum2 +
      "_agrids_cache")) {
    string r;
    if (gp->server.command("create table " + gp->database + ".ds" + local_args.
        dsnum2 + "_agrids_cache like " + gp->database +
        ".template_agrids_cache", r) < 0) {
      log_error2("error: '" + gp->server.error() + " while creating table " +
          gp->database + ".ds" + local_args.dsnum2 + "_agrids_cache", F, "scm",
          USER);
    }
  }
  if (!table_exists(gp->server, gp->database + ".ds" + local_args.dsnum2 +
      "_grid_definitions")) {
    string r;
    if (gp->server.command("create table " + gp->database + ".ds" + local_args.
        dsnum2 + "_grid_definitions like " + gp->database +
        ".template_grid_definitions", r) < 0) {
      log_error2("error: '" + gp->server.error() + " while creating table " +
          gp->database + ".ds" + local_args.dsnum2 + "_grid_definitions", F,
          "scm", USER);
    }
  }
}

void *thread_summarize_IDs(void *args) {
  static const string F = this_function_label(__func__);
  auto &a = *(reinterpret_cast<vector<string> *>(args));
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");;
  if (!srv) {
    log_error2("could not connect to mysql server - error: '" + srv.error() +
        "'", F, "scm", USER);
  }

  // read in the IDs from the separate XML file
  auto f = remote_web_file("https://rda.ucar.edu" + a[0] + a[1], g_temp_dir.
      name());
  std::ifstream ifs(f.c_str());
  if (!ifs.is_open()) {
    log_error2("unable to open '" + a[0] + a[1] + "'", F, "scm", USER);
  }
  char l[32768];
  double avgd = 0., navgd = 0., avgm = 0., navgm = 0.;
  size_t obs_count = 0;
  unordered_map<string, string> id_map;
  ifs.getline(l, 32768);
  while (!ifs.eof()) {
    if (l[2] == '<' && l[3] == 'I' && l[4] == 'D') {
      auto s = string(l);
      while (l[3] != '/') {
        ifs.getline(l, 32768);
        s += l;
      }
      XMLSnippet snippet = s;
      auto e = snippet.element("ID");
      auto idtyp = e.attribute_value("type");
      if (id_map.find(idtyp) == id_map.end()) {
        auto c = table_code(srv, a[5] + ".id_types", "id_type = '" + idtyp +
            "'");
        if (c.empty()) {
          log_error2("unable to get id type code", F, "scm", USER);
        }
        id_map.emplace(idtyp, c);
      }
      auto ID = e.attribute_value("value");
      replace_all(ID, "\\", "\\\\");
      replace_all(ID, "'", "\\'");
      replace_all(ID, "&quot;", "\"");
      replace_all(ID, " = ", " &eq; ");
      auto v = e.attribute_value("lat");
      string sw_lat, sw_lon, ne_lat, ne_lon;
      if (!v.empty()) {
        sw_lat = ftos(stof(v) * 10000., 0);
        sw_lon = ftos(stof(e.attribute_value("lon")) * 10000., 0);
        ne_lat = sw_lat;
        ne_lon = sw_lon;
      } else {
        v = e.attribute_value("cornerSW");
        auto sp = split(v, ",");
        if (sp.size() != 2) {
          log_error2("error in cornerSW attribute for file code " + a[2] + ", '"
              + a[1] + "', '" + v + "'", F, "scm", USER);
        }
        sw_lat = metatranslations::string_coordinate_to_db(sp[0]);
        sw_lon = metatranslations::string_coordinate_to_db(sp[1]);
        v = e.attribute_value("cornerNE");
        sp = split(v, ",");
        if (sp.size() != 2) {
          log_error2("error in cornerNE attribute for file code " + a[2], F,
              "scm", USER);
        }
        ne_lat = metatranslations::string_coordinate_to_db(sp[0]);
        ne_lon = metatranslations::string_coordinate_to_db(sp[1]);
      }
      if (sw_lon == "-9990000") {
        sw_lon = "-8388608";
      }
      if (ne_lon == "-9990000") {
        ne_lon = "-8388608";
      }
      auto id_code = table_code(srv, a[5] + ".ds" + local_args.dsnum2 + "_ids",
          "id_type_code = " + id_map[idtyp] + " AND id = '" + ID + "' AND "
          "sw_lat = " + sw_lat + " AND sw_lon = " + sw_lon + " AND ne_lat = " +
          ne_lat + " AND ne_lon = " + ne_lon);
      if (id_code.empty()) {
        log_error2("unable to get id code", F, "scm", USER);
      }
      v = e.attribute_value("start");
      replace_all(v, "-", "");
      while (strutils::occurs(v, " ") > 1) {
        v = v.substr(0, v.rfind(" "));
      }
      replace_all(v, " ", "");
      replace_all(v, ":", "");
      while (v.length() < 14) {
        v += "99";
      }
      DateTime dt1(stoll(v));
      v = e.attribute_value("end");
      replace_all(v, "-", "");
      string tz = "0";
      while (strutils::occurs(v, " ") > 1) {
        auto idx = v.rfind(" ");
        tz = v.substr(idx + 1);
        if (tz[0] == '+') {
          tz = tz.substr(1);
        }
        if (tz == "LST") {
          tz = "-2400";
        } else if (tz == "LT") {
          tz = "2400";
        }
        v = v.substr(0, idx);
      }
      replace_all(v,  " ", "");
      replace_all(v, ":", "");
      DateTime dt2;
      if (v.length() == 8) {
        dt2.set(stoll(v + "999999"));
      } else {
        while (v.length() < 14) {
          v += "99";
        }
        dt2.set(stoll(v));
      }
      auto nobs = e.attribute_value("numObs");
      if (srv.insert(a[5] + ".ds" + local_args.dsnum2 + "_id_list", "id_code, "
          "observation_type_code, platform_type_code, file_code, "
          "num_observations, start_date, end_date, time_zone", id_code + ", " +
          a[3] + ", " + a[4] + ", " + a[2] + ", " + nobs + ", " + dt1.to_string(
          "%Y%m%d%H%MM%SS") + ", " + dt2.to_string("%Y%m%d%H%MM%SS") + ", " +
          tz, "") < 0) {
        if (srv.error().find("Duplicate entry") == string::npos) {
          log_error2("'" + srv.error() + "' while inserting '" + id_code + ", "
              + a[3] + ", " + a[4] + ", " + a[2] + ", " + nobs + ", " + dt1.
              to_string("%Y%m%d%H%MM%SS") + ", " + dt2.to_string(
              "%Y%m%d%H%MM%SS") + ", " + tz + "' into " + a[5] + ".ds" +
              local_args.dsnum2 + "_id_list", F, "scm", USER);
        }
      }
      for (const auto& element : e.element_list("dataType")) {
        if (dt2 != dt1 || (dt1.time() == 999999 && dt2.time() == 999999)) {
          auto v = element.attribute_value("numObs");

          // backward compatibility for content metadata that was generated
          //   before the number of observations by data type was captured
          if (v.empty()) {
            v = nobs;
          }
          auto i = stoi(v);
          if (i > 1) {
            if (dt2.time() == 999999) {
              dt2.add_days(1);
            }
            double d;
            d = dt2.days_since(dt1);
            if (d == 0 && dt2.seconds_since(dt1) > 0) {
              d = 1;
            }
            if (d > 0) {
              avgd += i / d;
              ++navgd;
              obs_count += i;
            }
            d = dt2.months_since(dt1);
            if (d > 0) {
              avgm += i / d;
              ++navgm;
            }
          }
        }
      }
    }
    ifs.getline(l, 32768);
  }
  ifs.close();
  if (navgd > 0.) {
    avgd /= navgd;
  }
  if (navgm > 0.) {
    avgm /= navgm;
  }
  string u;
  if (lround(avgd) >= 1) {
    avgd /= 24.;
    if (lround(avgd) >= 1) {
      avgd /= 60.;
      if (lround(avgd) >= 1) {
        avgd /= 60.;
        if (lround(avgd) >= 1) {
          u = "second";
        } else {
          avgd *= 60.;
          u = "minute";
        }
      } else {
        avgd *= 60.;
        u = "hour";
      }
    } else {
      avgd *= 24.;
      u = "day";
    }
  } else {
    avgd *= 7.;
    if (lround(avgd) >= 1) {
      u = "week";
    } else {
      if (lround(avgm) >= 1) {
        u = "month";
      } else {
        avgm *= 12.;
        if (lround(avgm) >= 1) {
          u = "year";
        } else {
          avgm *= 10.;
          if (lround(avgm) >= 1) {
            u = "decade";
          }
        }
      }
    }
  }
  string i, k;
  if (lround(avgd) >= 1) {
    i = itos(lround(avgd));
    k = searchutils::time_resolution_keyword("irregular", lround(avgd), u, "");
  } else {
    i = itos(lround(avgm));
    k = searchutils::time_resolution_keyword("irregular", lround(avgm), u, "");
  }
  string r;
  if (i != "0" && srv.command("insert into " + a[5] + ".ds" + local_args.dsnum2
      + "_frequencies values (" + a[2] + ", " + a[3] + ", " + a[4] + ", " + i +
      ", " + itos(obs_count) + ", '" + u + "', '" + a[7] + "') on duplicate "
      "key update avg_obs_per = values(avg_obs_per), total_obs = values("
      "total_obs), uflag = values(uflag)", r) < 0) {
    if (srv.error().find("Duplicate entry") == string::npos) {
      log_error2("'" + srv.error() + "' while trying to insert into " + a[5] +
          ".ds" + local_args.dsnum2 + "_frequencies (" + a[2] + ", " + a[3] +
          ", " + a[4] + ", " + i + ", " + itos(obs_count) + ", '" + u + "')", F,
          "scm", USER);
    }
  }
  if (a[5] == "WObML" && srv.command("insert into " + a[5] + ".ds" + local_args.
      dsnum2 + "_geobounds (select i2.file_code, min(i.sw_lat), min(i.sw_lon), "
      "max(i.ne_lat), max(i.ne_lon) from " + a[5] + ".ds" + local_args.dsnum2 +
      "_id_list as i2 left join " + a[5] + ".ds" + local_args.dsnum2 + "_ids "
      "as i on i.code = i2.id_code where i2.file_code = " + a[2] + " and i."
      "sw_lat > -990000 and i.sw_lon > -1810000 and i.ne_lat < 990000 and i."
      "ne_lon < 1810000) on duplicate key update min_lat = values(min_lat), "
      "max_lat = values(max_lat),  min_lon = values(min_lon),  max_lon = "
      "values(max_lon)", r) < 0) {
    log_error2("'" + srv.error() + "' while trying to insert into " + a[5] +
        ".ds" + local_args.dsnum2 + "_geobounds for file_code = " + a[2], F,
        "scm", USER);
  }
  srv.disconnect();
  return nullptr;
}

void *thread_summarize_file_ID_locations(void *args) {
  static const string F = this_function_label(__func__);
  static unordered_map<string, vector<string>> lmap;
  auto &a = *reinterpret_cast<vector<string> *>(args);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");;
  if (!srv) {
    log_error2("could not connect to mysql server - error: " + srv.error(), F,
        "scm", USER);
  }
  if (lmap.size() == 0) {
    LocalQuery q("box1d_row, box1d_column, keyword", "search."
        "locations_by_point");
    if (q.submit(srv) < 0)
      log_error2("'" + q.error() + "'", F, "scm", USER);
    for (const auto& r : q) {
      auto k = r["box1d_row"] + "," + r["box1d_column"];
      if (lmap.find(k) == lmap.end()) {
        lmap.emplace(k, vector<string>());
      }
      lmap[k].emplace_back(r["keyword"]);
    }
  }
  auto f = remote_web_file("https://rda.ucar.edu" + a[0] + a[1], g_temp_dir.
      name());
  XMLDocument xdoc;
  if (xdoc.open(f)) {
    unordered_set<string> lset;
    size_t min = 999;
    size_t max = 0;
    for (const auto& e : xdoc.element_list("locations/box1d")) {
      auto b = e.attribute_value("bitmap");
      string lb;
      if (b.length() == 360) {
        lb = "";
        for (size_t n = 0; n < 360; n += 3) {
          auto l = 0;
          for (size_t m = 0; m < 3; ++m) {
            if (b[n + m] == '1') {
              auto k = e.attribute_value("row") + "," + itos(n + m);
              if (lmap.find(k) != lmap.end()) {
                for (const auto& e : lmap[k]) {
                  if (lset.find(e) == lset.end()) {
                    lset.emplace(e);
                  }
                }
              }
              l += pow(2., 2 - m);
            }
          }
          lb += itos(l);
        }
      } else {
        lb = b;
      }
      auto s = a[2] + ", " + a[3];
      if (!a[4].empty()) {
        s += ", " + a[4];
      }
      s += ", " + a[5] + ", " + a[6] + ", " + e.attribute_value("row") + ", '" +
          lb + "'";
      if (srv.insert(a[7] + ".ds" + local_args.dsnum2 + "_locations", s) < 0) {
        if (srv.error().find("Duplicate entry") == string::npos) {
          log_error2("'" + srv.error() + "' while trying to insert into " + a[7]
              + ".ds" + local_args.dsnum2 + "_locations (" + s + ") into " + a[
              7] + ".ds" + local_args.dsnum2 + "_locations", F, "scm", USER);
        }
      }
      size_t i = stoi(e.attribute_value("row"));
      if (min == 999) {
        min = i;
        max = min;
      } else {
        if (i < min) {
          min = i;
        }
        if (i > max) {
          max = i;
        }
      }
    }
    if (lset.size() > 0) {
      my::map<gatherxml::summarizeMetadata::ParentLocation> pmap;
      vector<string> v;
      compress_locations(lset, pmap, v, "scm", USER);
      if (a[7] == "WObML") {
        srv._delete("WObML.ds" + local_args.dsnum2 + "_location_names",
            "file_code = " + a[2] + " and observation_type_code = " + a[3] +
            " and platform_type_code = " + a[4]);
      } else if (a[7] == "WFixML") {
        srv._delete("WFixML.ds" + local_args.dsnum2 + "_location_names",
            "file_code = " + a[2] + " and classification_code = " + a[3]);
      }
      for (const auto& i : v) {
        gatherxml::summarizeMetadata::ParentLocation pl;
        pmap.found(i, pl);
        if (pl.matched_set != nullptr) {
          if (pl.matched_set->size() > pl.children_set->size() / 2) {
            replace_all(pl.key, "'", "\\'");
            string s;
            if (!a[4].empty()) {
              s = a[2] + ", " + a[3] + ", " + a[4] + ", '" + pl.key + "', 'Y'";
            } else {
              s = a[2] + ", " + a[3] + ", '" + pl.key + "', 'Y'";
            }
            if (srv.insert(a[7] + ".ds" + local_args.dsnum2 + "_location_names",
                s) < 0) {
              log_error2("'" + srv.error() + "' while inserting '" + s +
                  "' into " + a[7] + ".ds" + local_args.dsnum2 +
                  "_location_names", F, "scm", USER);
            }
            for (auto key : *pl.children_set) {
              replace_all(key, "'", "\\'");
              if (pl.matched_set->find(key) == pl.matched_set->end() && pl.
                  consolidated_parent_set->find(key) == pl.
                  consolidated_parent_set->end()) {
                if (!a[4].empty()) {
                  s = a[2] + ", " + a[3] + ", " + a[4] + ", '" + key + "', 'N'";
                } else {
                  s = a[2] + ", " + a[3] + ", '" + key + "', 'N'";
                }
                if (srv.insert(a[7] + ".ds" + local_args.dsnum2 +
                    "_location_names", s) < 0) {
                  log_error2("'" + srv.error() + "' while inserting '" + s +
                      "' into " + a[7] + ".ds" + local_args.dsnum2 +
                      "_location_names", F, "scm", USER);
                }
              }
            }
          } else {
            for (auto key : *pl.matched_set) {
              replace_all(key, "'", "\\'");
              string s;
              if (!a[4].empty()) {
                s = a[2] + ", " + a[3] + ", " + a[4] + ", '" + key + "', 'Y'";
              } else {
                s = a[2] + ", " + a[3] + ", '" + key + "', 'Y'";
              }
              if (srv.insert(a[7] + ".ds" + local_args.dsnum2 +
                  "_location_names", s) < 0) {
                log_error2("'" + srv.error() + "' while inserting '" + s +
                    "' into " + a[7] + ".ds" + local_args.dsnum2 +
                    "_location_names", F, "scm", USER);
              }
            }
          }
          pl.matched_set.reset();
        }
        pl.children_set.reset();
        pl.consolidated_parent_set.reset();
      }
    }
    xdoc.close();
  } else {
    log_error2("unable to open referenced XML document '" + f + "'", F, "scm",
        USER);
  }
  srv.disconnect();
  return nullptr;
}

struct ObMLParameters : public MarkupParameters {
  ObMLParameters() : metadata_path() {
    markup_type = "ObML";
    element = "observationType";
    data_type = "observations";
  }

  string metadata_path;
};

void process_obml_markup(void *markup_parameters) {
  auto op = reinterpret_cast<ObMLParameters *>(markup_parameters);
  static const string F = this_function_label(__func__);
  string mndt = "30001231";
  string mxdt = "10000101";
  auto cnt = 0;
  auto uflag = strand(3);
  unordered_map<string, string> obs_map, plat_map, dtyp_map;
  unordered_set<string> kml_table;
  for (const auto& o : op->xdoc.element_list("ObML/" + op->element)) {
    auto obs = o.attribute_value("value");
    if (obs_map.find(obs) == obs_map.end()) {
      auto c = table_code(op->server, "WObML.obs_types", "obs_type = '" + obs +
          "'");
      if (c.empty()) {
        log_error2("unable to get observation type code", F, "scm", USER);
      }
      obs_map.emplace(obs, c);
    }
    for (const auto& platform : o.element_addresses()) {
      auto plat = (platform.p)->attribute_value("type");
      if (plat_map.find(plat) == plat_map.end()) {
        auto c = table_code(op->server, "WObML.platform_types", "platform_type "
            "= '" + plat + "'");
        if (c.empty()) {
          log_error2("unable to get platform code", F, "scm", USER);
        }
        plat_map.emplace(plat, c);
      }
      for (const auto& e : (platform.p)->element_list("dataType")) {
        auto dtyp = e.attribute_value("map");
        if (!dtyp.empty()) {
          dtyp += ":";
        }
        dtyp += e.attribute_value("value");
        auto dtyp_k = obs_map[obs] + "|" + plat_map[plat] + "|" + dtyp;
        if (dtyp_map.find(dtyp_k) == dtyp_map.end()) {
          LocalQuery q("code", "WObML.ds" + local_args.dsnum2 +
              "_data_types_list", "observation_type_code = " + obs_map[obs] +
              " and platform_type_code = " + plat_map[plat] + " and data_type "
              "= '" + dtyp + "'");
          if (q.submit(op->server) < 0) {
            log_error2("'" + q.error() + "' while trying to get data_type code "
                "(1) for '" + obs_map[obs] + ", " + plat_map[plat] + ", '" +
                dtyp + "''", F, "scm", USER);
          }
          string c;
          Row r;
          if (q.fetch_row(r)) {
            c = r[0];
          } else {
            if (op->server.insert("WObML.ds" + local_args.dsnum2 +
                "_data_types_list", obs_map[obs] + ", " + plat_map[plat] + ", '"
                + dtyp + "', NULL") < 0) {
              log_error2("'" + op->server.error() + "' while trying to insert '"
                  + obs_map[obs] + ", " + plat_map[plat] + ", '" + dtyp +
                  "'' into WObML.ds" + local_args.dsnum2 + "_data_types_list",
                  F, "scm", USER);
            }
            auto lid = op->server.last_insert_ID();
            if (lid == 0) {
              log_error2("'" + q.error() + "' while trying to get data_type "
                  "code (2) for '" + obs_map[obs] + ", " + plat_map[plat] +
                  ", '" + dtyp + "''", F, "scm", USER);
            } else {
              c = lltos(lid);
            }
          }
          dtyp_map.emplace(dtyp_k, c);
        }
        string s = op->file_map[op->filename] + ", " + dtyp_map[dtyp_k];
        auto v = e.element("vertical");
        if (v.name() == "vertical") {
          s += ", " + v.attribute_value("min_altitude") + ", " + v.
              attribute_value("max_altitude") + ", '" + v.attribute_value(
              "vunits") + "', " + v.attribute_value("avg_nlev") + ", " + v.
              attribute_value("avg_vres");
        } else {
          s += ", 0, 0, NULL, 0, NULL";
        }
        if (op->server.insert("WObML.ds" + local_args.dsnum2 + "_data_types", s)
            < 0) {
          log_error2("'" + op->server.error() + "' while trying to insert '" + s
              + "' into WObML.ds" + local_args.dsnum2 + "_data_types", F, "scm",
              USER);
        }
      }
      cnt += stoi((platform.p)->attribute_value("numObs"));
      string ts, te;
      vector<pthread_t> tv;
      list<vector<string>> targs;
      for (const auto& e : platform.p->element_addresses()) {
        if (e.p->name() == "IDs") {
          targs.emplace_back(vector<string>());
          targs.back().emplace_back(op->metadata_path);
          targs.back().emplace_back(e.p->attribute_value("ref"));
          targs.back().emplace_back(op->file_map[op->filename]);
          targs.back().emplace_back(obs_map[obs]);
          targs.back().emplace_back(plat_map[plat]);
          targs.back().emplace_back(op->database);
          targs.back().emplace_back(op->file_type);
          targs.back().emplace_back(uflag);
          pthread_t t;
          pthread_create(&t, nullptr, thread_summarize_IDs, &targs.back());
          tv.emplace_back(t);
        } else if (e.p->name() == "temporal") {
          ts = e.p->attribute_value("start");
          replace_all(ts, "-", "");
          if (ts < mndt) {
            mndt = ts;
          }
          te = e.p->attribute_value("end");
          replace_all(te, "-", "");
          if (te > mxdt) {
            mxdt = te;
          }
        } else if (e.p->name() == "locations") {

          // read in the locations from the separate XML file
          targs.emplace_back(vector<string>());
          targs.back().emplace_back(op->metadata_path);
          targs.back().emplace_back(e.p->attribute_value("ref"));
          targs.back().emplace_back(op->file_map[op->filename]);
          targs.back().emplace_back(obs_map[obs]);
          targs.back().emplace_back(plat_map[plat]);
          targs.back().emplace_back(ts);
          targs.back().emplace_back(te);
          targs.back().emplace_back("WObML");
          pthread_t t;
          pthread_create(&t, nullptr, thread_summarize_file_ID_locations,
              &targs.back());
          tv.emplace_back(t);
        }
      }
      for (const auto& t : tv) {
        pthread_join(t, nullptr);
      }
    }
  }
  op->server._delete("WObML.ds" + local_args.dsnum2 + "_frequencies",
      "file_code = " + op->file_map[op->filename] + " and uflag != '" + uflag +
      "'");
  auto s = "num_observations = " + itos(cnt) + ", start_date = " + mndt + ", "
      "end_date = " + mxdt;
  auto tb_nam = "WObML.ds" + local_args.dsnum2 + "_" + op->file_type + "files2";
  if (op->server.update(tb_nam, s + ", uflag = '" + strand(5) + "'", "code = " +
      op->file_map[op->filename]) < 0) {
    log_error2("error: '" + op->server.error() + "' while updating " + tb_nam +
        " with '" + s + "'", F, "scm", USER);
  }
}

struct SatMLParameters : public MarkupParameters {
  SatMLParameters() {
    markup_type = "SatML";
    element = "satellite";
    data_type = "products";
  }
};

void process_satml_markup(void *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto sp = reinterpret_cast<SatMLParameters *>(markup_parameters);
  string mndt = "3000-12-31 23:59:59 +0000";
  string mxdt = "1000-01-01 00:00:00 +0000";
  string ptyp;
  auto cnt = 0;
  auto b = false;
  for (const auto& sat : sp->xdoc.element_list("SatML/" + sp->element)) {
    auto n = 0;
    auto ilst = sat.element_list("images");
    if (ilst.size() > 0) {
      ptyp = "I";
      auto e = sat.element("temporal");
      if (e.attribute_value("start") < mndt) {
        mndt = e.attribute_value("start");
      }
      if (e.attribute_value("end") > mxdt) {
        mxdt = e.attribute_value("end");
      }
      for (const auto& i : ilst) {
        n += stoi(i.attribute_value("num"));
      }
      cnt += n;
    } else {
      auto swlst = sat.element_list("swath/data");
      ptyp = "S";
      for (const auto& s : swlst) {
        auto e = s.element("temporal");
        if (e.attribute_value("start") < mndt) {
          mndt = e.attribute_value("start");
        }
        if (e.attribute_value("end") > mxdt) {
          mxdt = e.attribute_value("end");
        }
        e = s.element("scanLines");
        n = stoi(e.attribute_value("num"));
        cnt += n;
      }
    }
    if (!b) {
      if (!table_exists(sp->server, sp->database + ".ds" + local_args.dsnum2              + "_products")) {
        string r;
        if (sp->server.command("create table " + sp->database + ".ds" +
            local_args.dsnum2 + "_products like " + sp->database +
            ".template_products", r) < 0) {
          log_error2("error: '" + sp->server.error() + "' while creating table "
              + sp->database + ".ds" + local_args.dsnum2 + "_products", F,
             "scm", USER);
        }
      }
      b = true;
    }
    if (sp->server.insert(sp->database + ".ds" + local_args.dsnum2 +
        "_products", sp->file_map[sp->filename] + ", '" + ptyp + "', " + itos(
        n), "update num_products = value(num_products)") < 0) {
      if (sp->server.error().find("Duplicate entry") == string::npos) {
        log_error2("error: '" + sp->server.error() + "' while inserting into " +
            sp->database + ".ds" + local_args.dsnum2 + "_products", F, "scm",
            USER);
      }
    }
  }
  mndt = string_date_to_ll_string(mndt);
  replace_all(mndt, "+0000", "");
  mxdt = string_date_to_ll_string(mxdt);
  replace_all(mxdt, "+0000", "");
  auto s = "num_products = " + itos(cnt) + ", start_date = " + mndt + ", "
      "end_date = " + mxdt;
  auto tb_nam = sp->database + ".ds" + local_args.dsnum2 + "_" + sp->file_type +
      "files2";
  if (sp->server.update(tb_nam, s + ", uflag = '" + strand(5) + "'", "code = " +
      sp->file_map[sp->filename]) < 0) {
    log_error2("error: '" + sp->server.error() + "' while updating " + tb_nam +
        " with '" + s + "'", F, "scm", USER);
  }
}

struct FixMLParameters : public MarkupParameters {
  FixMLParameters() : markup_path(), stg_map(), class_map(), freq_map() {
    markup_type = "FixML";
    element = "feature";
    data_type = "fixes";
  }

  string markup_path;
  unordered_map<string, string> stg_map;
  unordered_map<string, pair<string, string>> class_map;
  unordered_map<string, pair<size_t, size_t>> freq_map;
};

void process_fixml_markup(void *markup_parameters) {
  static const string F = this_function_label(__func__);
  auto fp = reinterpret_cast<FixMLParameters *>(markup_parameters);
  string mndt = "30001231235959";
  string mxdt = "10000101000000";
  auto cnt = 0;
  for (const auto& f : fp->xdoc.element_list("FixML/" + fp->element)) {
    for (const auto& _class : f.element_list("classification")) {
      auto stg = _class.attribute_value("stage");
      if (fp->class_map.find(stg) == fp->class_map.end()) {
        fp->class_map.emplace(stg, make_pair("30001231235959",
            "10000101000000"));
      }
      cnt += stoi(_class.attribute_value("nfixes"));
      if (fp->stg_map.find(stg) == fp->stg_map.end()) {
        auto c = table_code(fp->server, fp->database + ".classifications",
            "classification = '" + stg + "'");
        if (c.empty()) {
          log_error2("unable to get cyclone stage code", F, "scm", USER);
        }
        fp->stg_map.emplace(stg, c);
      }
      if (fp->server.insert(fp->database + ".ds" + local_args.dsnum2 +
          "_id_list", "'" + f.attribute_value("ID") + "', " + fp->stg_map[stg] +
          ", " + fp->file_map[fp->filename] + ", " + _class.attribute_value(
          "nfixes"), "update num_fixes = num_fixes + " + _class.attribute_value(
          "nfixes")) < 0) {
        log_error2("error: '" + fp->server.error() + "' while inserting '" +
            f.attribute_value("ID") + "', " + fp->stg_map[stg] + ", " + fp->
            file_map[fp->filename] + ", " + _class.attribute_value("nfixes") +
            "' into " + fp->database + ".ds" + local_args.dsnum2 + "_id_list",
            F, "scm", USER);
      }
      auto e = _class.element("start");
      auto v = string_date_to_ll_string(e.attribute_value("dateTime"));
      v = v.substr(0, v.length() - 5);
      if (v < mndt) {
        mndt = v;
      }
      if (v < fp->class_map[stg].first) {
        fp->class_map[stg].first = v;
      }
      while (v.length() < 14) {
        v += "00";
      }
      auto dt1 = DateTime(stoll(v));
      e = _class.element("end");
      v = string_date_to_ll_string(e.attribute_value("dateTime"));
      v = v.substr(0, v.length() - 5);
      if (v > mxdt) {
        mxdt = v;
      }
      if (v > fp->class_map[stg].second) {
        fp->class_map[stg].second = v;
      }
      while (v.length() < 14) {
        v += "00";
      }
      auto dt2 = DateTime(stoll(v));
      size_t n = stoi(_class.attribute_value("nfixes"));
      if (n > 1) {
        n = lroundf(24. / (dt2.hours_since(dt1) / static_cast<float>(n - 1)));
/*
if (n != 4)
cerr << n << " " << dt1.to_string() << " " << dt2.to_string() << endl;
*/
        auto k = "day<!>" + fp->stg_map[stg];
        if (fp->freq_map.find(k) == fp->freq_map.end()) {
          fp->freq_map.emplace(k, make_pair(n, n));
        } else {
          if (n < fp->freq_map[k].first) {
            fp->freq_map[k].first = n;
          }
          if (n > fp->freq_map[k].second) {
            fp->freq_map[k].second = n;
          }
        }
      }
    }
  }
  auto s = "num_fixes = " + itos(cnt) + ", start_date = " + mndt + ", end_date "
      "= " + mxdt;
  auto tb_nam = fp->database + ".ds" + local_args.dsnum2 + "_" + fp->file_type +
      "files2";
  if (fp->server.update(tb_nam, s + ", uflag = '" + strand(5) + "'", "code = " +
      fp->file_map[fp->filename]) < 0) {
    log_error2("error: '" + fp->server.error() + "' while updating " + tb_nam +
        " with '" + s + "'", F, "scm", USER);
  }
}

void process_markup(MarkupParameters *markup_parameters) {
  if (markup_parameters->markup_type == "GrML") {
    process_grml_markup(markup_parameters);
  } else if (markup_parameters->markup_type == "ObML") {
    process_obml_markup(markup_parameters);
  } else if (markup_parameters->markup_type == "FixML") {
    process_fixml_markup(markup_parameters);
  }
}

void update_grml_database(GrMLParameters& grml_parameters) {
  gatherxml::summarizeMetadata::aggregate_grids(grml_parameters.database, "scm",
      USER, grml_parameters.file_map[grml_parameters.filename]);
  if (grml_parameters.database == "WGrML") {
    gatherxml::summarizeMetadata::summarize_frequencies("scm", USER,
        grml_parameters.file_map[grml_parameters.filename]);
    gatherxml::summarizeMetadata::summarize_grid_resolutions("scm", USER,
        grml_parameters.file_map[grml_parameters.filename]);
  }
  gatherxml::summarizeMetadata::summarize_grids(grml_parameters.database, "scm",
      USER, grml_parameters.file_map[grml_parameters.filename]);
  if (grml_parameters.summ_lev) {
    gatherxml::summarizeMetadata::summarize_grid_levels(grml_parameters.
        database, "scm", USER);
  }
}

void update_obml_database(ObMLParameters& obml_parameters) {
  if (obml_parameters.database == "WObML") {
    gatherxml::summarizeMetadata::summarize_frequencies("scm", USER,
        obml_parameters.file_map[obml_parameters.filename]);
  }
}

void update_satml_database(SatMLParameters& satml_parameters) {
  gatherxml::summarizeMetadata::summarize_frequencies("scm", USER,
      satml_parameters.file_map[satml_parameters.filename]);
}

void update_fixml_database(FixMLParameters& fixml_parameters) {
  static const string F = this_function_label(__func__);
  for (const auto& e : fixml_parameters.class_map) {
    vector<string> args;
    args.emplace_back(fixml_parameters.markup_path + "." + e.first +
        ".locations.xml");
    args.emplace_back("");
    args.emplace_back(fixml_parameters.file_map[fixml_parameters.filename]);
    args.emplace_back(fixml_parameters.stg_map[e.first]);
    args.emplace_back("");
    auto s = string_date_to_ll_string(e.second.first);
    if (s.length() > 12) {
      s = s.substr(0, 12);
    }
    args.emplace_back(s);
    s = string_date_to_ll_string(e.second.second);
    if (s.length() > 12) {
      s = s.substr(0, 12);
    }
    args.emplace_back(s);
    args.emplace_back(fixml_parameters.database);
    thread_summarize_file_ID_locations(reinterpret_cast<void *>(&args));
  }
  if (fixml_parameters.database == "WFixML") {
    gatherxml::summarizeMetadata::summarize_frequencies("scm", USER,
        fixml_parameters.file_map[fixml_parameters.filename]);
  }
  for (const auto& e : fixml_parameters.freq_map) {
    auto sp = split(e.first, "<!>");
    if (fixml_parameters.server.insert(fixml_parameters.database + ".ds" +
        local_args.dsnum2 + "_frequencies", fixml_parameters.file_map[
        fixml_parameters.filename] + ", " + sp[1] + ", " + itos(e.second.first)
        + ", " + itos(e.second.second) + ", '" + sp[0] + "'") < 0) {
      log_error2("error: '" + fixml_parameters.server.error() + "' while "
          "trying to insert into " + fixml_parameters.database + ".ds" +
          local_args.dsnum2 + "_frequencies '" + fixml_parameters.file_map[
          fixml_parameters.filename] + ", " + sp[1] + ", " + itos(e.second.
          first) + ", " + itos(e.second.second) + ", '" + sp[0] + "''", F,
          "scm", USER);
    }
    if (fixml_parameters.database == "WFixML") {
      auto s = searchutils::time_resolution_keyword("irregular", e.second.
          first, sp[0], "");
      if (fixml_parameters.server.insert("search.time_resolutions", "'" + s +
          "', 'GCMD', '" + metautils::args.dsnum + "', 'WFixML'") < 0) {
        if (fixml_parameters.server.error().find("Duplicate entry") == string::
            npos) {
          log_error2("error: '" + fixml_parameters.server.error() + "' while "
              "trying to insert into search.time_resolutions ''" + s + "', "
              "'GCMD', '" + metautils::args.dsnum + "', 'WFixML''", F, "scm",
              USER);
        }
      }
      s = searchutils::time_resolution_keyword("irregular", e.second.second, sp[
          0], "");
      if (fixml_parameters.server.insert("search.time_resolutions", "'" + s +
          "', 'GCMD', '" + metautils::args.dsnum + "', 'WFixML'") < 0) {
        if (fixml_parameters.server.error().find("Duplicate entry") == string::
            npos)
          log_error2("error: '" + fixml_parameters.server.error() + "' while "
              "trying to insert into search.time_resolutions ''" + s + "', "
              "'GCMD', '" + metautils::args.dsnum + "', 'WFixML''", F, "scm",
              USER);
      }
    }
  }
}

void update_database(MarkupParameters *markup_parameters) {
  if (markup_parameters->markup_type == "GrML") {
    update_grml_database(*reinterpret_cast<GrMLParameters *>(
        markup_parameters));
  } else if (markup_parameters->markup_type == "ObML") {
    update_obml_database(*reinterpret_cast<ObMLParameters *>(
        markup_parameters));
  } else if (markup_parameters->markup_type == "SatML") {
    update_satml_database(*reinterpret_cast<SatMLParameters *>(
        markup_parameters));
  } else if (markup_parameters->markup_type == "FixML") {
    update_fixml_database(*reinterpret_cast<FixMLParameters *>(
        markup_parameters));
  }
}

void summarize_markup(string markup_type, unordered_map<string, vector<
    string>>& file_list) {
  static const string F = this_function_label(__func__);
  vector<string> mtypv;
  if (!markup_type.empty()) {
    mtypv.emplace_back(markup_type);
  } else {
    for (const auto& e : file_list) {
      mtypv.emplace_back(e.first);
    }
  }
  for (const auto& t : mtypv) {
    MarkupParameters *mp = nullptr;
    if (t == "GrML") {
      mp = new GrMLParameters;
    } else if (t == "ObML") {
      mp = new ObMLParameters;
    } else if (t == "FixML") {
      mp = new FixMLParameters;
    } else {
      log_error2("invalid markup type '" + t + "'", F, "scm", USER);
    }
    mp->server.connect(metautils::directives.database_server, metautils::
        directives.metadb_username, metautils::directives.metadb_password, "");
    if (!mp->server) {
      log_error2("could not connect to metadata database - error: '" + mp->
          server.error() + "'", F, "scm", USER);
    }
    Timer tm;
    for (auto& f : file_list[t]) {
      if (local_args.verbose) {
        tm.start();
      }
      open_markup_file(mp->xdoc, f);
      initialize_file(mp);
      process_data_format(mp);
      insert_filename(mp);
      clear_tables(mp);
      if (t == "ObML") {
        reinterpret_cast<ObMLParameters *>(mp)->metadata_path = f.substr(0, f.
            rfind("/") + 1);
      }
      process_markup(mp);
      mp->xdoc.close();
      if (t == "FixML") {
        reinterpret_cast<FixMLParameters *>(mp)->markup_path = f;
      }
      update_database(mp);
      if (local_args.verbose) {
        tm.stop();
        cout << mp->filename << " summarized in " << tm.elapsed_time() <<
            " seconds" << endl;
      }
    }
    mp->server._delete("search.data_types", "dsid = '" + metautils::args.dsnum +
        "' and vocabulary = 'dssmm'");
    string err;
    if (t == "GrML") {
      auto gp = reinterpret_cast<GrMLParameters *>(mp);
      for (const auto& p : gp->prod_set) {
        if (gp->server.insert("search.data_types", "'grid', '" + p + "', '" +
            gp->database + "', '" + metautils::args.dsnum + "'") < 0) {
          if (gp->server.error().find("Duplicate entry") == string::npos) {
            err = gp->server.error();
            break;
          }
        }
      }
    } else if (t == "ObML") {
      if (mp->server.insert("search.data_types", "'platform_observation', '', '"
          + mp->database + "', '" + metautils::args.dsnum + "'") < 0) {
        err = mp->server.error();
      }
    } else if (t == "SatML") {
      if (mp->server.insert("search.data_types", "'satellite', '', '" +
          mp->database + "', '" + metautils::args.dsnum + "'") < 0) {
        err = mp->server.error();
      }
    } else if (t == "FixML") {
      if (mp->server.insert("search.data_types", "'cyclone_fix', '', '" + mp->
          database + "', '" + metautils::args.dsnum + "'") < 0) {
        err = mp->server.error();
      }
    }
    if (!err.empty() && err.find("Duplicate entry") == string::npos) {
      log_error2("'"  + err  + "' while inserting into search.data_types", F,
          "scm", USER);
    }
    mp->server.disconnect();
  }
}

void *thread_generate_detailed_metadata_view(void *args) {
  auto &a = *(reinterpret_cast<vector<string> *>(args));
//PROBLEM!!
  for (const auto& g : local_args.gindex_list) {
    if (g != "0") {
      gatherxml::detailedMetadata::generate_group_detailed_metadata_view(g, a[
          0], "scm", USER);
    }
  }
  if (local_args.summarized_web_file || local_args.refresh_web) {
    gatherxml::detailedMetadata::generate_detailed_metadata_view("scm", USER);
  }
  return nullptr;
}

void *thread_create_file_list_cache(void *args) {
  auto &a = *(reinterpret_cast<vector<string> *>(args));
  gatherxml::summarizeMetadata::create_file_list_cache(a[0], "scm", USER, a[1]);
  return nullptr;
}

void *thread_summarize_obs_data(void *) {
  auto b = gatherxml::summarizeMetadata::summarize_obs_data("scm", USER);
  if (b && local_args.update_graphics) {
    stringstream oss, ess;
    mysystem2(metautils::directives.local_root + "/bin/gsi " + metautils::args.
        dsnum, oss, ess);
  }
  return nullptr;
}

void *thread_summarize_fix_data(void *) {
  gatherxml::summarizeMetadata::summarize_fix_data("scm", USER);
  if (local_args.update_graphics) {
    stringstream oss, ess;
    mysystem2(metautils::directives.local_root + "/bin/gsi " + metautils::args.
        dsnum, oss, ess);
  }
  return nullptr;
}

void *thread_index_variables(void *) {
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  string e;
  if (!searchutils::indexed_variables(srv, metautils::args.dsnum, e)) {
    log_error2(e, "thread_index_variables()", "scm", USER);
  }
  srv.disconnect();
  return nullptr;
}

void *thread_index_locations(void *) {
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  string e;
  if (!searchutils::indexed_locations(srv, metautils::args.dsnum, e)) {
    log_error2(e, "thread_index_locations()", "scm", USER);
  }
  srv.disconnect();
  return nullptr;
}

void *thread_summarize_dates(void *) {
  gatherxml::summarizeMetadata::summarize_dates("scm", USER);
  return nullptr;
}

void *thread_summarize_frequencies(void *) {
  gatherxml::summarizeMetadata::summarize_frequencies("scm", USER);
  return nullptr;
}

void *thread_summarize_grid_resolutions(void *) {
  gatherxml::summarizeMetadata::summarize_grid_resolutions("scm", USER);
  return nullptr;
}

void *thread_summarize_data_formats(void *) {
  gatherxml::summarizeMetadata::summarize_data_formats("scm", USER);
  return nullptr;
}

void *thread_aggregate_grids(void *args) {
  auto &a = *(reinterpret_cast<vector<string> *>(args));

  gatherxml::summarizeMetadata::aggregate_grids(a[0], "scm", USER);
  return nullptr;
}  

extern "C" void clean_up() {
  if (!myerror.empty()) {
    delete_temporary_directory();
    cerr << myerror << endl;
  }
  if (!local_args.temp_directory.empty()) {
    log_warning("clean_up(): non-empty temporary directory: " + local_args.
        temp_directory, "scm", USER);
  }
}

extern "C" void segv_handler(int) {
  log_error2("segmentation fault", "segv_handler()", "scm", USER);
}

void show_usage() {
  cerr << "usage: (1) scm -d [ds]<nnn.n> { -rw | -ri } [ <tindex> | all ]" <<
      endl;
  cerr << "   or: (2) scm -d [ds]<nnn.n> -wa [ <type> ]" << endl;
  cerr << "   or: (3) scm -d [ds]<nnn.n> [-t <tdir> ] -wf FILE" << endl;
  cerr << "summarize content metadata - generally, specialists will use (1) or "
      "(2) and" << endl;
  cerr << "  other gatherxml utilities will use (3)." << endl;
  cerr << "\nrequired:" << endl;
  cerr << "  -d <nnn.n>   (1), (2), (3): the dataset number, optionally "
      "prepended with \"ds\"" << endl;
  cerr << "  -rw | -ri    (1): refresh the web or detailed inventory database. "
      "If a dataset" << endl;
  cerr << "                 has groups, optionally include the top-level index "
      "to summarize" << endl;
  cerr << "                 that specific group, or optionally include the "
      "keyword \"all\" to" << endl;
  cerr << "                 summarize all groups." << endl;
  cerr << "  -wa          (2): summarize every Web metadata file for the "
      "dataset. This can" << endl;
  cerr << "                 be restricted to an optional markup type (e.g. "
      "\"GrML\", \"ObML\"," << endl;
  cerr << "                 etc.)." << endl;
  cerr << "  -wf          (3): summarize the markup for a Web file" << endl;
  cerr << "  FILE         (3): the relative path for the markup file being "
      "summarized" << endl;
  cerr << "\noptions:" << endl;
  cerr << "  -g/-G      generate (default)/don't generate graphics" << endl;
  cerr << "  -k/-K      generate (default)/don't generate KML for observations"
      << endl;
  cerr << "  -N         notify with a message when scm completes" << endl;
  if (USER == "dattore") {
    cerr << "  -F         refresh only - use this flag only when the data file "        "is unchanged" << endl;
    cerr << "               but additional fields/tables have been added to "
        "the database that" << endl;
    cerr << "               need to be populated" << endl;
    cerr << "  -r/-R      regenerate (default)/don't regenerate dataset web "
        "page" << endl;
    cerr << "  -s/-S      summarize (default)/don't summarize date and time "
        "resolutions" << endl;
  }
  cerr << "  -t <tdir>  (3): the name of the temporary directory where FILE is "
      "located." << endl;
  cerr << "               Otherwise, the default location is" << endl;
  cerr << "               /data/web/datasets/dsnnn.n/metadata/wfmd on the "
      "production web" << endl;
  cerr << "               server." << endl;
  cerr << "  -V         verbose mode" << endl;
}

int main(int argc, char **argv) {
  static const string F = this_function_label(__func__);
  if (argc < 3) {
    show_usage();
    exit(1);
  }

  // set exit handlers
  signal(SIGSEGV, segv_handler);
  atexit(clean_up);
  auto ex_stat = 0;

  // read the metadata configuration
  metautils::read_config("scm", USER);

  // parse the arguments to scm
  const char ARG_DELIMITER = '`';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv,
      ARG_DELIMITER);
  parse_args(ARG_DELIMITER);

  // create the global temporary working directory
  if (!g_temp_dir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, "scm", USER);
  }

  // container to hold thread IDs
  vector<pthread_t> tv;

  // container to hold thread arguments, since these arguments need to stay in
  //  scope until the thread is joined - use a list because reallocation doesn't  //  happen (and addresses don't change) when new elements are emplaced
  list<vector<string>> targs;

  // start the execution timer
  Timer tm;
  tm.start();

  Server mysrv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    log_error2("unable to connect to database server at startup", F, "scm",
        USER);
  }
  unordered_map<string, vector<string>> mmap{
      { "GrML", vector<string>() },
      { "ObML", vector<string>() },
      { "SatML", vector<string>() },
      { "FixML", vector<string>() },
  };
  if (local_args.summarize_all) {
    stringstream oss, ess;
    if (mysystem2("/bin/tcsh -c \"wget -q -O - --post-data='authKey="
        "qGNlKijgo9DJ7MN&cmd=listfiles&value=/SERVER_ROOT/web/datasets/ds" +
        metautils::args.dsnum + "/metadata/" + local_args.cmd_directory +
        "' https://rda.ucar.edu/cgi-bin/dss/remoteRDAServerUtils\"", oss, ess)
        != 0) {
      log_error2("unable to get metadata file listing", F, "scm", USER);
    }
    auto sp = split(oss.str(), "\n");
    for (const auto& e : sp) {
      auto s = e.substr(e.find("/datasets"));
      if (regex_search(s, regex("\\.gz$"))) {
        chop(s, 3);
      }
      auto idx = s.rfind(".");
      if (idx != string::npos) {
        auto k = s.substr(idx + 1);
        if (mmap.find(k) != mmap.end()) {
          mmap[k].emplace_back(s);
        }
      }
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "GrML") &&
        mmap["GrML"].size() > 0) {
      if (local_args.cmd_directory == "wfmd") {
        mysrv._delete("WGrML.summary", "dsid = '" + metautils::args.dsnum + "'");
      }
    }
    summarize_markup(local_args.summ_type, mmap);
    if ((local_args.summ_type.empty() || local_args.summ_type == "ObML") &&
        mmap["ObML"].size() > 0) {
      if (local_args.summarized_web_file && local_args.update_db) {
        pthread_t t;
        pthread_create(&t, nullptr, thread_summarize_obs_data, nullptr);
        tv.emplace_back(t);
        pthread_create(&t, nullptr, thread_index_locations, nullptr);
        tv.emplace_back(t);
      }
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "FixML") &&
        mmap["FixML"].size() > 0) {
      if (local_args.update_db) {
        pthread_t t;
        pthread_create(&t, nullptr, thread_summarize_fix_data, nullptr);
        tv.emplace_back(t);
        pthread_create(&t, nullptr, thread_index_locations, nullptr);
        tv.emplace_back(t);
      }
    }
  } else if (!local_args.file.empty()) {
    string s;
    if (!local_args.temp_directory.empty()) {
      s = local_args.temp_directory + "/" + local_args.file;
    } else {
      s = "/datasets/ds" + metautils::args.dsnum + "/metadata/wfmd/" +
          local_args.file;
    }
    auto idx = local_args.file.rfind(".");
    if (idx != string::npos) {
      auto k = local_args.file.substr(idx + 1);
      if (mmap.find(k) != mmap.end()) {
        mmap[k].emplace_back(s);
      }
    }
    if (mmap["GrML"].size() > 0) {
      summarize_markup("GrML", mmap);
    } else if (mmap["ObML"].size() > 0) {
      summarize_markup("ObML", mmap);
      if (local_args.summarized_web_file && local_args.update_db) {
        pthread_t t;
        pthread_create(&t, nullptr, thread_summarize_obs_data, nullptr);
        tv.emplace_back(t);
        pthread_create(&t, nullptr, thread_index_locations, nullptr);
        tv.emplace_back(t);
      }
    } else if (mmap["SatML"].size() > 0) {
      summarize_markup("SatML", mmap);
    } else if (mmap["FixML"].size() > 0) {
      summarize_markup("FixML", mmap);
      if (local_args.summarized_hpss_file && local_args.update_db) {
        pthread_t t;
        pthread_create(&t, nullptr, thread_summarize_fix_data, nullptr);
        tv.emplace_back(t);
        pthread_create(&t, nullptr, thread_index_locations, nullptr);
        tv.emplace_back(t);
      }
    } else {
      log_error2("file extension of '" + local_args.file + "' not recognized",
          F, "scm", USER);
    }
  } else if (local_args.refresh_web) {
    if (local_args.gindex_list.size() > 0 && local_args.gindex_list.front() !=
        "all") {
      LocalQuery q("gidx", "dssdb.dsgroup", "dsid = 'ds" + metautils::args.dsnum
          + "' and gindex = " + local_args.gindex_list.front() + " and pindex "
          "= 0");
      if (q.submit(mysrv) < 0) {
        log_error2("group check failed", F, "scm", USER);
      }
      if (q.num_rows() == 0) {
        log_error2(local_args.gindex_list.front() + " is not a top-level index "
            "for this dataset", F, "scm", USER);
      }
    }
    pthread_t t;
    pthread_create(&t, nullptr, thread_summarize_frequencies, nullptr);
    tv.emplace_back(t);
  }
  if (local_args.summarized_web_file && local_args.added_variable) {
    pthread_t t;
    pthread_create(&t, nullptr, thread_index_variables, nullptr);
    tv.emplace_back(t);
  }

  // make necessary updates to the metadata databases
  if (local_args.update_db) {
    if (local_args.summarized_web_file || local_args.refresh_web) {
      if (local_args.refresh_web) {
        if (table_exists(mysrv, "WGrML.ds" + local_args.dsnum2 +
            "_agrids_cache")) {
          gatherxml::summarizeMetadata::summarize_grids("WGrML", "scm", USER);
          targs.emplace_back(vector<string>());
          targs.back().emplace_back("WGrML");
          pthread_t t;
          pthread_create(&t, nullptr, thread_aggregate_grids, &targs.back());
          tv.emplace_back(t);
          pthread_create(&t, nullptr, thread_summarize_grid_resolutions,
              nullptr);
          tv.emplace_back(t);
        }
        if (table_exists(mysrv, "WObML.ds" + local_args.dsnum2 +
            "_locations")) {
          gatherxml::summarizeMetadata::summarize_obs_data("scm", USER);
        }
        if (local_args.gindex_list.size() == 1) {
          if (local_args.gindex_list.front() == "all") {
            local_args.gindex_list.clear();
            LocalQuery q("select distinct tindex from dssdb.wfile where dsid = "
                "'ds" + metautils::args.dsnum + "' and type = 'D' and status = "
                "'P'");
            if (q.submit(mysrv) < 0) {
              log_error2("error getting group indexes: '" + q.error() + "'", F,
                  "scm", USER);
            }
            for (const auto& r : q) {
              local_args.gindex_list.emplace_back(r[0]);
            }
          }
        }
      }
      if (local_args.update_graphics) {
        stringstream oss, ess;
        for (const auto& g : local_args.gindex_list) {
          if (mysystem2(metautils::directives.local_root + "/bin/gsi -g " + g +
              " " + metautils::args.dsnum, oss, ess) != 0) {
            log_warning("scm: main(): gsi -g failed with error '" + trimmed(ess.
                str()) + "'", "scm", USER);
          }
        }
        if (mysystem2(metautils::directives.local_root + "/bin/gsi " +
            metautils::args.dsnum, oss, ess) != 0) {
          log_warning("scm: main(): gsi failed with error '" + trimmed(ess.
              str()) + "'", "scm", USER);
        }
      }
      for (const auto& g : local_args.gindex_list) {
        gatherxml::summarizeMetadata::create_file_list_cache("Web", "scm", USER,
            g);
        if (!local_args.summarized_web_file) {
          gatherxml::summarizeMetadata::create_file_list_cache("inv", "scm",
              USER, g);
        }
      }
      targs.emplace_back(vector<string>());
      targs.back().emplace_back("Web");
      pthread_t t;
      pthread_create(&t, nullptr, thread_generate_detailed_metadata_view,
          &targs.back());
      tv.emplace_back(t);
      pthread_create(&t, nullptr, thread_summarize_dates, nullptr);
      tv.emplace_back(t);
      pthread_create(&t, nullptr, thread_summarize_data_formats, nullptr);
      tv.emplace_back(t);
      targs.emplace_back(vector<string>());
      targs.back().emplace_back("Web");
      targs.back().emplace_back("");
      pthread_create(&t, nullptr, thread_create_file_list_cache, &targs.back());
      tv.emplace_back(t);
      if (!local_args.summarized_web_file) {
        targs.emplace_back(vector<string>());
        targs.back().emplace_back("inv");
        targs.back().emplace_back("");
        pthread_create(&t, nullptr, thread_create_file_list_cache, &targs.
            back());
        tv.emplace_back(t);
      }
    } else if (local_args.refresh_inv) {
      if (local_args.refresh_inv && local_args.gindex_list.size() == 1) {
        if (local_args.gindex_list.front() == "all") {
          local_args.gindex_list.clear();
          LocalQuery q("select distinct tindex from dssdb.wfile where dsid = "
              "'ds" + metautils::args.dsnum + "' and type = 'D' and status = "
              "'P'");
          if (q.submit(mysrv) < 0) {
            log_error2("error getting group indexes: '" + q.error() + "'", F,
                "scm", USER);
          }
          for (const auto& r : q) {
            local_args.gindex_list.emplace_back(r[0]);
          }
        }
      }
      for (const auto& g : local_args.gindex_list) {
        gatherxml::summarizeMetadata::create_file_list_cache("inv", "scm", USER,
            g);
      }
      gatherxml::summarizeMetadata::create_file_list_cache("inv", "scm", USER);
    }
  }
  mysrv.disconnect();

  // wait for any still-running threads to complete
  for (const auto& t : tv) {
    pthread_join(t, nullptr);
  }

  // if this is not a test run, then clean up the temporary directory
  if (metautils::args.dsnum != "test" && !local_args.temp_directory.empty()) {
    delete_temporary_directory();
  }

  // update the dataset description, if necessary
  if (metautils::args.regenerate) {
    stringstream oss, ess;
    if (unixutils::mysystem2(metautils::directives.local_root + "/bin/dsgen " +
        metautils::args.dsnum, oss, ess) != 0) {
      auto e = "error regenerating the dataset description: '" + trimmed(ess.
          str()) + "'";
      cerr << e << endl;
      metautils::log_info(e, "scm", USER);
      ex_stat = 1;
    }
  }

  if (local_args.notify) {
    cout << "scm has completed successfully" << endl;
  }

  // record the execution time
  tm.stop();
  log_warning("execution time: " + ftos(tm.elapsed_time()) + " seconds",
      "scm.time", USER);
  return ex_stat;
}
