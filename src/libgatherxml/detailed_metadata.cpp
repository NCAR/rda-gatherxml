#include <fstream>
#include <string>
#include <sys/stat.h>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <gatherxml.hpp>
#include <metahelpers.hpp>
#include <datetime.hpp>
#include <PostgreSQL.hpp>
#include <bsort.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <search.hpp>
#include <xml.hpp>
#include <gridutils.hpp>
#include <myerror.hpp>
#ifdef DUMP_QUERIES
#include <timer.hpp>
#endif

using namespace PostgreSQL;
using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::endl;
using std::get;
using std::list;
using std::make_pair;
using std::make_tuple;
using std::map;
using std::move;
using std::ofstream;
using std::pair;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::sort;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::tie;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::ds_aliases;
using strutils::itos;
using strutils::replace_all;
using strutils::split;
using strutils::strand;
using strutils::substitute;
using strutils::to_capital;
using strutils::to_lower;
using strutils::to_sql_tuple_string;
using unixutils::mysystem2;
using unixutils::open_output;

namespace gatherxml {

namespace detailedMetadata {

struct ParameterMetadata {
  ParameterMetadata() : code(), short_name(), description(), units(), comment()
      { }

  string code, short_name, description, units, comment;
};

struct LevelSummary {
  LevelSummary() : map(), type(), value(), variable_table() { }

  string map, type, value;
  unordered_map<string, pair<string, string>> variable_table;
};

struct CombinedLevelSummary {
  CombinedLevelSummary() : map_list(), variable_table() { }

  vector<string> map_list;
  unordered_map<string, pair<string, string>> variable_table;
};

struct GridSummary {
  GridSummary() : description(), start(), end() { }

  string description, start, end;
};

struct ProductSummary {
  ProductSummary() : code(), description(), grid_table() { }

  size_t code;
  string description;
  unordered_map<size_t, GridSummary> grid_table;
};

struct ParameterData {
  ParameterData() : parameter_codes(), level_tables(), level_values() { }

  unordered_set<string> parameter_codes;
  unordered_map<size_t, pair<string, string>> level_tables;
  vector<size_t> level_values;
};

struct TypeEntry {
  TypeEntry() : key(),start(),end(),type_table(nullptr),type_table_keys(nullptr),type_table_keys_2(nullptr) {}

  string key;
  string start,end;
  shared_ptr<my::map<TypeEntry>> type_table;
  shared_ptr<list<string>> type_table_keys,type_table_keys_2;
};

bool compare_parameter_data(const ParameterMetadata& left, const
    ParameterMetadata& right) {
  if (left.code == right.code) {
    return true;
  }
  if (left.code.find(".") == string::npos && right.code.find(".") == string::
      npos) {
    if (strutils::is_numeric(left.code) && strutils::is_numeric(right.code)) {
      auto l = stoi(left.code);
      auto r = stoi(right.code);
      if (l < r) {
        return true;
      }
      return false;
    } else {
      if (left.code < right.code) {
        return true;
      }
      return false;
    }
  } else {
    auto sp1 = split(left.code, ".");
    auto sp2 = split(right.code, ".");
    bool b = true;
    if (sp1.size() != sp2.size()) {
      if (left.code < right.code) {
        b = true;
      } else {
        b = false;
      }
    } else {
      for (size_t n = 0; n < sp1.size(); n++) {
        auto l = stoi(sp1[n]);
        auto r = stoi(sp2[n]);
        if (l < r) {
          b = true;
          n = sp1.size();
        } else if (l > r) {
          b = false;
          n = sp1.size();
        }
      }
    }
    return b;
  }
}

bool compare_levels(const string& left, const string& right) {
  auto sp = split(left, "<!>");
  auto l = sp[0];
  while (l.length() < 3) {
    l.insert(0, "0");
  }
  auto sp2 = split(right, "<!>");
  auto r = sp2[0];
  while (r.length() < 3) {
    r.insert(0, "0");
  }
  if (l <= r) {
    return true;
  }
  return false;
}

bool compare_layers(const string& left, const string& right) {
  auto sp = split(left, "<!>");
  auto sp2 = split(sp[0], "-");
  auto l=sp2[0];
  while (l.length() < 3) {
    l.insert(0, "0");
  }
  auto l2 = sp2[1];
  while (l2.length() < 3) {
    l2.insert(0, "0");
  }
  auto sp3 = split(right, "<!>");
  auto sp4 = split(sp3[0], "-");
  auto r = sp4[0];
  while (r.length() < 3) {
    r.insert(0, "0");
  }
  auto r2 = sp4[1];
  while (r2.length() < 3) {
    r2.insert(0, "0");
  }
  if (l < r) {
    return true;
  } else if (l > r) {
    return false;
  }
  if (l2 <= r2) {
    return true;
  }
  return false;
}

void fill_level_code_map(unordered_map<size_t, tuple<string, string, string>>&
    level_code_map, string caller, string user) {
  static const string F = this_function_label(__func__);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("code, map, type, value", "WGrML.levels");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  for (const auto& r : q) {
    level_code_map.emplace(stoi(r[0]), make_tuple(r[1], r[2], r[3]));
  }
  srv.disconnect();
}

void generate_parameter_cross_reference(string format, string title, string
    html_file, string caller, string user) {
  static const string F = this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }

  // create the directory tree in the temp directory
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -m 0755 -p " + t.name() + "/metadata", oss, ess) !=
      0) {
    log_error2("unable to create temporary directory tree", F, caller, user);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("select distinct parameter from \"WGrML\".summary as s left "
      "join \"WGrML\".formats as f on f.code = s.format_code where s.dsid in " +
      to_sql_tuple_string(ds_aliases(metautils::args.dsid)) + " and f.format = '"
      + format + "'");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  if (q.num_rows() > 0) {
    unordered_map<string, unordered_map<string, ParameterMetadata>> ptmap;
    xmlutils::ParameterMapper pmap(metautils::directives.parameter_map_path);
    for (const auto& r : q) {
      auto c = r[0];
      string m;
      xmlutils::clean_parameter_code(c, m);
      auto key = pmap.title(format, m);
      if (ptmap.find(key) == ptmap.end()) {
        ptmap.emplace(key, unordered_map<string, ParameterMetadata>());
      }
      ParameterMetadata pd;
      if (ptmap[key].find(c) == ptmap[key].end()) {
        pd.code = c;
        pd.short_name = pmap.short_name(format, r[0]);
        pd.description = pmap.description(format, r[0]);
        pd.units = pmap.units(format, r[0]);
        pd.comment = pmap.comment(format, r[0]);
        ptmap[key].emplace(c, pd);
      }
    }
    ofstream ofs;
    open_output(ofs, t.name() + "/metadata/" + html_file);
    if (!ofs.is_open()) {
      log_error2("unable to open html file for output", F, caller, user);
    }
    ofstream ofs2;
    open_output(ofs2, t.name() + "/metadata/" + substitute(html_file, ".html",
        ".xml"));
    if (!ofs2.is_open()) {
      log_error2("unable to open xml file for output", F, caller, user);
    }
    ofs2 << "<?xml version=\"1.0\" ?>" << endl;
    ofs << "<style id=\"detail\">" << endl;
    ofs << "  @import url(/css/transform.css);" << endl;
    ofs << "</style>" << endl;
    ofs2 << "<parameterTables>" << endl;
    for (const auto& e : ptmap) {
      vector<ParameterMetadata> pdv;
      pdv.reserve(e.second.size());
      auto snc = false;
      auto cc = false;
      for (const auto& e2 : e.second) {
        if (!snc && !e2.second.short_name.empty()) {
          snc = true;
        }
        if (!cc && !e2.second.comment.empty()) {
          cc = true;
        }
        pdv.emplace_back(e2.second);
      }
      sort(pdv.begin(), pdv.end(), compare_parameter_data);
      ofs << "<p>The following " << e.second.size() <<
          " parameters from " << e.first << " are included in this dataset:"
          "<center><table class=\"insert\" cellspacing=\"1\" cellpadding=\"5\" "
          "border=\"0\"><tr class=\"bg2\" valign=\"bottom\"><th align="
          "\"center\">";
      ofs2 << "  <parameterTable>" << endl;
      ofs2 << "    <description>" << e.first << "</description>" << endl;
      if (format == "WMO_GRIB2") {
        ofs << "Discipline</th><th align=\"center\">Category</th><th align="
            "\"center\">Code";
      } else {
        ofs << "Parameter Code";
      }
      ofs << "</th>";
      if (snc) {
        ofs << "<th align=\"center\">Short Name</th>";
      }
      ofs << "<th align=\"left\">Description</th><th align=\"center\">Units"
          "</th>";
      if (cc) {
        ofs << "<th align=\"left\">Comments</th>";
      }
      ofs << "</tr>" << endl;
      for (size_t n = 0; n < e.second.size(); n++) {
        ofs << "<tr class=\"bg" << (n % 2) << "\"><td align=\"center\">";
        ofs2 << "    <parameter>" << endl;
        if (format == "WMO_GRIB2") {
          auto sp = split(pdv[n].code, ".");
          ofs << sp[0] << "</td><td align=\"center\">" << sp[1] << "</td><td "
              "align=\"center\">" << sp[2];
          ofs2 << "      <discipline>" << sp[0] << "</discipline>" << endl;
          ofs2 << "      <category>" << sp[1] << "</category>" << endl;
          ofs2 << "      <code>" << sp[2] << "</code>" << endl;
        } else {
          ofs << pdv[n].code;
          ofs2 << "      <code>" << pdv[n].code << "</code>" << endl;
        }
        ofs << "</td>";
        if (snc) {
          ofs << "<td align=\"center\">" << pdv[n].short_name << "</td>";
          ofs2 << "      <shortName>" << pdv[n].short_name << "</shortName>" <<
              endl;
        }
        ofs << "<td align=\"left\">" << pdv[n].description << "</td><td align="
            "\"center\">" << htmlutils::transform_superscripts(pdv[n].units) <<
            "</td>";
        ofs2 << "      <description>" << pdv[n].description << "</description>"
            << endl;
        ofs2 << "      <units>" << pdv[n].units << "</units>" << endl;
        if (cc) {
          ofs << "<td align=\"left\">" << pdv[n].comment << "</td>";
          ofs2 << "      <comments>" << pdv[n].comment << "</comments>" << endl;
        }
        ofs << "</tr>" << endl;
        ofs2 << "    </parameter>" << endl;
      }
      ofs << "</table></center></p>" << endl;
      ofs2 << "  </parameterTable>" << endl;
    }
    ofs2 << "</parameterTables>" << endl;
    ofs.close();
    ofs2.close();
    string e;
    if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/" +
        metautils::args.dsid, metautils::directives.rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() couldn't "
          "sync cross-references - rdadata_sync error(s): '" + e  + "'", caller,
          user);
    }
  } else {

    // remove a parameter table if it exists and there are no parameters for
    //  this format
    string e;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/" + metautils::args.
        dsid + "/metadata/" + html_file, t.name(), metautils::directives.
        rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() tried to "
          "but couldn't delete '" + html_file + "' - error: '" + e + "'",
          caller, user);
    }
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/" + metautils::args.
        dsid + "/metadata/" + substitute(html_file, ".html", ".xml"), t.name(),
        metautils::directives.rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() tried to "
          "but couldn't delete '" + substitute(html_file, ".html", ".xml") +
          "' - error: '" + e + "'", caller, user);
    }
  }
  srv.disconnect();
}

void generate_level_cross_reference(string format, string title, string
     html_file, string caller, string user) {
  static const string F = this_function_label(__func__);
  unordered_map<size_t, tuple<string, string, string>> lcmap;
  fill_level_code_map(lcmap, caller, user);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("select distinct level_type_codes from \"WGrML\".summary as s "
      "left join \"WGrML\".formats as f on f.code = s.format_code where s.dsid "
      "in " + to_sql_tuple_string(ds_aliases(metautils::args.dsid)) + " and f."
      "format = '" + format + "'");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  vector<string> lv, yv;
  if (q.num_rows() > 0) {
    xmlutils::LevelMapper lmap(metautils::directives.level_map_path);
    unordered_set<string> uset;
    for (const auto& r: q) {
      vector<size_t> v;
      bitmap::uncompress_values(r[0], v);
      for (auto& e : v) {
        string m, t, v;
        tie(m, t, v) = lcmap[e];
        if (uset.find(t) == uset.end()) {
          if (t.find("-") != string::npos) {
            auto sp = split(t, "-");
            yv.emplace_back(t + "<!>" + lmap.description(format, t, m) + "<!>" +
                htmlutils::transform_superscripts(lmap.units(format, sp[0], m))
                + "<!>" + htmlutils::transform_superscripts(lmap.units(format,
                sp[1], m)));
          } else {
            lv.emplace_back(t + "<!>" + lmap.description(format, t, m) + "<!>" +
                htmlutils::transform_superscripts(lmap.units(format, t, m)));
          }
          uset.emplace(t);
        }
      }
    }
  }
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  if (lv.size() > 0 || yv.size() > 0) {

    // create the directory tree in the temp directory
    stringstream oss, ess;
    if (mysystem2("/bin/mkdir -m 0755 -p " + t.name() + "/metadata", oss, ess)
        != 0) {
      log_error2("unable to create temporary directory tree", F, caller, user);
    }
    ofstream ofs;
    open_output(ofs, t.name() + "/metadata/" + html_file);
    if (!ofs.is_open()) {
      log_error2("unable to open html file for output", F, caller, user);
    }
    ofs << "<style id=\"detail\">" << endl;
    ofs << "  @import url(/css/transform.css);" << endl;
    ofs << "</style>" << endl;
    auto n = 0;
    if (lv.size() > 0) {
      ofs << "<p>The following " << lv.size() << " " << to_capital(format) <<
          " levels are included in this dataset:<center><table class="
          "\"insert\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\"><tr "
          "class=\"bg2\" valign=\"bottom\"><th align=\"center\">Code</th><th "
          "align=\"left\">Description</th><th align=\"center\">Units</th></tr>"
          << endl;
      sort(lv.begin(), lv.end(), compare_levels);
      for (const auto& e : lv) {
        auto sp = split(e, "<!>");
        ofs << "<tr class=\"bg" << (n % 2) << "\"><td align=\"center\">" << sp[
            0] << "</td><td align=\"left\">" << sp[1] << "</td><td align="
            "\"center\">" << sp[2] << "</td></tr>" << endl;
        ++n;
      }
      ofs << "</table></center></p>" << endl;
    }
    if (yv.size() > 0) {
      ofs << "<p>The following " << yv.size() << " " << to_capital(format) <<
          " layers are included in this dataset:<center><table class="
          "\"insert\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\"><tr "
          "class=\"bg2\" valign=\"bottom\"><th align=\"center\">Code<br />"
          "(Bottom)</th><th align=\"center\">Code<br />(Top)</th><th align="
          "\"left\">Description</th><th align=\"center\">Units<br />(Bottom)"
          "</th><th align=\"center\">Units<br />(Top)</th></tr>" << endl;
      sort(yv.begin(), yv.end(), compare_layers);
      for (const auto& e : yv) {
        auto sp = split(e, "<!>");
        auto sp2 = split(sp[0], "-");
        ofs << "<tr class=\"bg" << (n % 2) << "\"><td align=\"center\">" << sp2[
            0] << "</td><td align=\"center\">" << sp2[1] << "</td><td align="
            "\"left\">" << sp[1] << "</td><td align=\"center\">" << sp[2] <<
            "</td><td align=\"center\">" << sp[3] << "</td></tr>" << endl;
        ++n;
      }
    }
    ofs.close();
    string e;
    if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/" +
        metautils::args.dsid, metautils::directives.rdadata_home, e) < 0) {
      metautils::log_warning(F + " couldn't sync '" + html_file + "' - "
          "rdadata_sync error(s): '" + e + "'", caller, user);
    }
  } else {

    // remove the level table if it exists and there are no levels for this
    //  format
    string e;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/" + metautils::args.
        dsid + "/metadata/" + html_file, t.name(), metautils::directives.
        rdadata_home, e) < 0) {
      metautils::log_warning(F + " tried to but couldn't delete '" + html_file +
          "' - error: '" + e + "'", caller, user);
    }
  }
  srv.disconnect();
}

void replace_token(string& source, string token, string new_s) {
  stringstream ss;
  ss << "\\{" << token;
  while (regex_search(source, regex(ss.str()))) {
    token = source.substr(source.find("{" + token));
    token = token.substr(0, token.find("}"));
    auto sp = split(token, "_");
    if (sp.size() > 1) {
      if (sp[1] == "I3") {
        new_s.insert(0, 3 - new_s.length(), '0');
      }
    }
    token += "}";
    replace_all(source, token, new_s);
  }
}

void add_to_formats(XMLDocument& xdoc, string database, string primaries_table,
    vector<pair<string, string>>& format_list, string& formats, string caller,
    string user) {
  static const string F = this_function_label(__func__);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to metadata server: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q("select distinct f.format, f.code from " + postgres_ready(
      database) + ".formats as f left join " + postgres_ready(database) + "." +
      primaries_table + " as p on p.format_code = f.code where p.format_code "
      "is not null");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(srv) < 0) {
    log_error2("'" + q.error() + "'", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  for (const auto& r : q) {
    format_list.emplace_back(make_pair(r[0], r[1]));
    auto e = xdoc.element("formatReferences/format@name=" + r[0]);
    auto v = e.attribute_value("href");
    formats += "<li>";
    if (!v.empty()) {
      formats += "<a target=\"_format\" href=\"" + v + "\">";
    }
    formats += to_capital(substitute(r[0], "proprietary_",
        "Dataset-specific "));
    if (!v.empty()) {
      formats += "</a>";
    }
    v = e.attribute_value("type");
    if (!v.empty()) {
      formats += " (" + v + ")";
    }
    formats += "</li>";
  }
  srv.disconnect();
}

string parameter_query(Server& mysrv, string format_code, size_t
    format_query_result_size) {
  if (format_query_result_size > 1) {
    return "select a.parameter, a.level_type_codes, min(a.start_date), max(a."
        "end_date) from \"WGrML\"." + metautils::args.dsid + "_agrids2 as a "
        "left join \"WGrML\"." + metautils::args.dsid + "_webfiles2 as p on p."
        "code = a.file_code where p.format_code = " + format_code + " group by "
        "a.parameter, a.level_type_codes";
  }
  return "select parameter, level_type_codes, min(start_date), max(end_date) "
      "from \"WGrML\"." + metautils::args.dsid + "_agrids2 group by parameter, "
      "level_type_codes";
}

string time_range_query(Server& mysrv, string format_code) {
  return "select distinct t.time_range, d.definition, d.def_params from "
      "\"WGrML\".summary as s left join \"WGrML\".time_ranges as t on t.code = "
      "s.time_range_code left join \"WGrML\".grid_definitions as d on d.code = "
      "s.grid_definition_code where s.dsid in " + to_sql_tuple_string(ds_aliases(
      metautils::args.dsid)) + " and format_code = " + format_code;
}

void write_grid_html(ofstream& ofs, size_t num_products) {
  ofs << "<style id=\"detail\">" << endl;
  ofs << "  @import url(/css/transform.css);" << endl;
  ofs << "</style>" << endl;
  ofs << "<style id=\"view_button\">" << endl;
  ofs << "div.view_button_on," << endl;
  ofs << "div.view_button_off {" << endl;
  ofs << "  position: relative;" << endl;
  ofs << "  float: left;" << endl;
  ofs << "  padding: 5px;" << endl;
  ofs << "  border: 2px solid #272264;" << endl;
  ofs << "  font-size: 14px;" << endl;
  ofs << "  margin-left: 15px;" << endl;
  ofs << "}" << endl;
  ofs << "div.view_button_on {" << endl;
  ofs << "  background-color: #27aae0;" << endl;
  ofs << "  color: white;" << endl;
  ofs << "  font-weight: bold;" << endl;
  ofs << "}" << endl;
  ofs << "div.view_button_on:after {" << endl;
  ofs << "  content: '';" << endl;
  ofs << "  display: block;" << endl;
  ofs << "  position: absolute;" << endl;
  ofs << "  width: 0px;" << endl;
  ofs << "  height: 0px;" << endl;
  ofs << "  border-style: solid;" << endl;
  ofs << "  border-width: 10px 10px 0px 10px;" << endl;
  ofs << "  border-color: #272264 transparent transparent transparent;" << endl;
  ofs << "  top: 100%;" << endl;
  ofs << "  left: 50%;" << endl;
  ofs << "  margin-left: -10px;" << endl;
  ofs << "}" << endl;
  ofs << "div.view_button_off {" << endl;
  ofs << "  background-color: #b8edab;" << endl;
  ofs << "}" << endl;
  ofs << "div.view_button_off:hover {" << endl;
  ofs << "  background-color: #27aae0;" << endl;
  ofs << "  color: white;" << endl;
  ofs << "  cursor: pointer;" << endl;
  ofs << "}" << endl;
  ofs << "</style>" << endl;
  ofs << "<script id=\"view_script\">" << endl;
  ofs << "function changeView(e,v) {" << endl;
  ofs << "  if (e.className == 'view_button_off') {" << endl;
  ofs << "    var x = document.getElementsByClassName('view_button_on');" <<
      endl;
  ofs << "    for (n = 0; n < x.length; ++n) {" << endl;
  ofs << "        x[n].className = 'view_button_off';" << endl;
  ofs << "    }" << endl;
  ofs << "    e.className = 'view_button_on';" << endl;
  ofs << "    getAjaxContent('GET',null,'/datasets/" << metautils::args.dsid <<
      "/metadata/'+v,'detail_content');" << endl;
  ofs << "  }" << endl;
  ofs << "}" << endl;
  ofs << "</script>" << endl;
  ofs << "<br />" << endl;
  ofs << "<center><table width=\"95%\" cellspacing=\"0\" cellpadding=\"0\" "
      "border=\"0\">" << endl;
  ofs << "<tr><td>" << endl;
  ofs << "<?php" << endl;
  ofs << "  if (isset($_GET[\"view\"])) {" << endl;
  ofs << "    $view = $_GET[\"view\"];" << endl;
  ofs << "  }" << endl;
  ofs << "  else {" << endl;
  ofs << "    $view = \"parameter\";" << endl;
  ofs << "  }" << endl;
  ofs << "  if (strcmp($view,\"parameter\") == 0) {" << endl;
  ofs << "?>" << endl;
  ofs << "<div class=\"view_button_on\" onClick=\"changeView(this, 'parameter-"
      "detail.html')\">Parameter View</div>" << endl;
  ofs << "<?php" << endl;
  ofs << "  }" << endl;
  ofs << "  else {" << endl;
  ofs << "?>" << endl;
  ofs << "<div class=\"view_button_off\" onClick=\"changeView(this, 'parameter-"
      "detail.html')\">Parameter View</div>" << endl;
  ofs << "<?php" << endl;
  ofs << "  }" << endl;
  ofs << "  if (strcmp($view, \"level\") == 0) {" << endl;
  ofs << "?>" << endl;
  ofs << "<div class=\"view_button_on\" onClick=\"changeView(this, 'level-"
      "detail.html')\">Vertical Level View</div>" << endl;
  ofs << "<?php" << endl;
  ofs << "  }" << endl;
  ofs << "  else {" << endl;
  ofs << "?>" << endl;
  ofs << "<div class=\"view_button_off\" onClick=\"changeView(this, 'level-"
      "detail.html')\">Vertical Level View</div>" << endl;
  ofs << "<?php" << endl;
  ofs << "  }" << endl;
  if (num_products > 0) {
    ofs << "  if (strcmp($view, \"product\") == 0) {" << endl;
    ofs << "?>" << endl;
    ofs << "<div class=\"view_button_on\" onClick=\"changeView(this, 'product-"
        "detail.html')\">Product View</div>" << endl;
    ofs << "<?php" << endl;
    ofs << "  }" << endl;
    ofs << "  else {" << endl;
    ofs << "?>" << endl;
    ofs << "<div class=\"view_button_off\" onClick=\"changeView(this, 'product-"
        "detail.html')\">Product View</div>" << endl;
    ofs << "<?php" << endl;
    ofs << "  }" << endl;
  }
  ofs << "?>" << endl;
  ofs << "<div id=\"detail_content\" style=\"margin-top: 5%\"><center><img "
      "src=\"/images/loader.gif\" /><br ><span style=\"color: #a0a0a0\">"
      "Loading...</span></center></div>" << endl;
  ofs << "</td></tr>" << endl;
  ofs << "</table></center>" << endl;
  ofs << "<img src=\"/images/transpace.gif\" width=\"1\" height=\"0\" onLoad="
      "\"getAjaxContent('GET',null,'/datasets/" << metautils::args.dsid <<
      "/metadata/<?php echo $view; ?>-detail.html','detail_content')\" />" <<
      endl;
}

void generate_gridded_product_detail(Server& mysrv, string file_type, const
    vector<pair<string, string>>& format_list, TempDir& tdir, string caller,
    string user) {
  static const string F = this_function_label(__func__);
  LocalQuery q("time_range, code", "WGrML.time_ranges");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("unable to build the product hash", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_map<size_t, string> trmap;
  for (const auto& r : q) {
    trmap.emplace(stoi(r[1]), r[0]);
  }
  q.set("definition, def_params, code", "WGrML.grid_definitions");
#ifdef DUMP_QUERIES
  {
  Timer tm;
  tm.start();
#endif
  if (q.submit(mysrv) < 0) {
    log_error2("unable to build the grid definition hash", F, caller, user);
  }
#ifdef DUMP_QUERIES
  tm.stop();
  cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.show()
      << endl;
  }
#endif
  unordered_map<size_t, string> gdmap;
  for (const auto& r : q) {
    gdmap.emplace(stoi(r[2]), gridutils::convert_grid_definition(r[0] + "<!>" +
        r[1]));
  }
  ofstream ofs;
  open_output(ofs, tdir.name() + "/metadata/product-detail.html");
  if (!ofs.is_open()) {
    log_error2("unable to open output for product detail", F, caller, user);
  }
  for (const auto& fp : format_list) {
    Query qs;
    if (format_list.size() > 1) {
      qs.set("select a.time_range_codes, a.grid_definition_codes, min(a."
          "start_date), max(a.end_date) from \"WGrML\"." + metautils::args.dsid +
          "_agrids2 as a left join \"WGrML\"." + metautils::args.dsid +
          "_webfiles2 as p on p.code = a.file_code where p.format_code = " + fp.
          second + " group by a.time_range_codes, a.grid_definition_codes");
    } else {
      qs.set("select time_range_codes, grid_definition_codes, min(start_date), "
          "max(end_date) from \"WGrML\"." + metautils::args.dsid +
          "_agrids2 group by time_range_codes, grid_definition_codes");
    }
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (qs.submit(mysrv) < 0) {
      log_error2("unable to build the product summary", F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << qs.
        show() << endl;
    }
#endif
    unordered_map<size_t, ProductSummary> psmap;
    for (const auto& r : qs) {
      vector<size_t> pv;
      bitmap::uncompress_values(r[0], pv);
      vector<size_t> gv;
      bitmap::uncompress_values(r[1], gv);
      for (auto& pc : pv) {
        ProductSummary ps;
        if (psmap.find(pc) == psmap.end()) {
          ps.code = pc;
          ps.description = trmap[pc];
          psmap.emplace(pc, ps);
        }
        for (auto& gc : gv) {
          GridSummary gs;
          if (psmap[pc].grid_table.find(gc) == psmap[pc].grid_table.end()) {
            gs.description = gdmap[gc];
            gs.start = r[2];
            gs.end = r[3];
            psmap[pc].grid_table.emplace(gc, gs);
          } else {
            if (r[2] < psmap[pc].grid_table[gc].start) {
              psmap[pc].grid_table[gc].start = r[2];
            }
            if (r[3] > psmap[pc].grid_table[gc].end) {
              psmap[pc].grid_table[gc].end = r[3];
            }
          }
        }
      }
    }
    ofs << "<div id=\"" << fp.first << "_anchor\"></div>";
    if (format_list.size() > 1) {
      ofs << "<table width=\"100%\" border=\"0\"><tr><td>" << endl;
      for (const auto& e : format_list) {
        if (fp.first != e.first) {
          auto f = e.first;
          replace_all(f, "proprietary", "dataset-specific");
          ofs << "<span class=\"paneltext\">Go to <a href=\"javascript:void(0)"
              "\" onClick=\"javascript:scrollTo('" << e.first <<
              "_anchor')\">" << to_capital(f) << "</a> Summary</span><br>" <<
              endl;
        }
      }
      ofs << "</td></tr></table>" << endl;
    }
    auto f = fp.first;
    replace_all(f, "proprietary", "dataset-specific");
    ofs << "<table class=\"insert\" width=\"100%\" cellspacing=\"1\" "
        "cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan="
        "\"2\" align=\"center\">Summary for Grids in " << to_capital(f) <<
        " Format</th></tr>" << endl;
    q.set("select distinct parameter, level_type_codes from \"WGrML\".summary "
        "where dsid in " + to_sql_tuple_string(ds_aliases(metautils::args.dsid))
        + " and format_code = " + fp.second);
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
    ofs << "<tr class=\"bg0\"><td align=\"left\" colspan=\"2\"><b>Parameter "
        "and Level Information:</b><br />";
    if (q.num_rows() > 1) {
      ofs << "There are multiple parameters and/or vertical levels for the "
          "grids in this format.  Click a product description to see the "
          "available parameters and levels for that product.";
    } else {
      Row row;
      q.fetch_row(row);
      vector<size_t> lv;
      bitmap::uncompress_values(row[1], lv);
      if (lv.size() > 1) {
        ofs << "There are multiple parameters and/or vertical levels for the "
            "grids in this format.  Click a product description to see the "
            "available parameters and levels for that product.";
      } else {
        ofs << "There is one parameter and one vertical level for all of the "
            "grids in this format:<ul class=\"paneltext\"><span class="
            "\"underline\">Parameter:</span> " << row[0] << "<br><span class="
            "\"underline\">Vertical Level:</span> " << row[1] << "</ul>";
      }
    }
    ofs << "</td></tr>";
    ofs << "<tr class=\"bg2\"><th align=\"left\">Product</th><th align=\"left\""
        "><table class=\"paneltext\" width=\"100%\" cellspacing=\"0\" "
        "cellpadding=\"0\" border=\"0\"><tr valign=\"top\" class=\"bg2\"><th "
        "align=\"left\">Geographical Coverage</th><th width=\"41%\" align="
        "\"left\">Temporal Valid Range</th></tr></table></th></tr>" << endl;
    vector<ProductSummary> psv;
    psv.reserve(psmap.size());
    for (auto& e : psmap) {
      psv.emplace_back(e.second);
    }
    sort(psv.begin(), psv.end(),
    [](const ProductSummary& left, const ProductSummary& right) -> bool {
      return metacompares::compare_time_ranges(left.description, right.
          description);
    });
    auto cidx = 0;
    for (auto& ps : psv) {
      ofs << "<tr class=\"bg" << cidx << "\" valign=\"top\"><td>";
      if (q.num_rows() > 1) {
        ofs << "<a title=\"Parameters and Vertical Levels\" href=\"javascript:"
            "void(0)\" onClick=\"popModalWindowWithGetUrl(950, 600, '/cgi-bin/"
            "transform?dsnum=" << metautils::args.dsid << "&view=varlev&"
            "formatCode=" << fp.second << "&ftype=" << strutils::to_lower(
            file_type);
        ofs << "&tcode=" << ps.code << "')\">" << ps.description << "</a>";
      } else {
        ofs << ps.description;
      }
      ofs << "</td><td><table style=\"font-size: 14px\" width=\"100%\" "
          "cellspacing=\"0\" cellpadding=\"0\" border=\"0\">";
      vector<GridSummary> gsv;
      gsv.reserve(ps.grid_table.size());
      for (auto& e : ps.grid_table) {
        gsv.emplace_back(e.second);
      }
      sort(gsv.begin(), gsv.end(),
      [](const GridSummary& left, const GridSummary& right) -> bool {
        return metacompares::compare_grid_definitions(left.description, right.
            description);
      });
      size_t n = 0;
      auto x = ps.grid_table.size() - 1;
      for (const auto& gs : gsv) {
        ofs << "<tr valign=\"top\"><td style=\"border-bottom: " << static_cast<
            int>(n < x) << "px solid #96a4bf\" align=\"left\">&bull;&nbsp;" <<
            gs.description << "</td><td style=\"border-bottom: " << static_cast<
            int>(n < x) << "px solid #96a4bf\">&nbsp;&nbsp;</td><td style="
            "\"border-bottom: " << static_cast<int>(n < x) << "px solid "
            "#96a4bf\" width=\"40%\" align=\"left\"><nobr>" << dateutils::
            string_ll_to_date_string(gs.start) << " to " << dateutils::
            string_ll_to_date_string(gs.end) << "</nobr></td></tr>";
        ++n;
      }
      ofs << "</table></td></tr>";
      cidx = 1 - cidx;
    }
  }
  ofs.close();
}

void generate_detailed_grid_summary(string file_type, ofstream& ofs, const
    vector<pair<string, string>>& format_list, string caller, string user) {
  static const string F = this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }

  // create the directory tree in the temp directory
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -m 0755 -p " + t.name() + "/metadata", oss, ess) !=
      0) {
    log_error2("unable to create temporary directory tree - '" + ess.str() +
        "'", F, caller, user);
  }
  ofstream ofs_p;
  open_output(ofs_p, t.name() + "/metadata/parameter-detail.html");
  if (!ofs_p.is_open()) {
    log_error2("unable to open output for parameter detail", F, caller, user);
  }
  ofstream ofs_l;
  open_output(ofs_l, t.name() + "/metadata/level-detail.html");
  if (!ofs_l.is_open()) {
    log_error2("unable to open output for level detail", F, caller, user);
  }
  xmlutils::ParameterMapper pmap(metautils::directives.parameter_map_path);
  xmlutils::LevelMapper lmap(metautils::directives.level_map_path);
  vector<string> pfv;
  unordered_map<string, ParameterData> pdmap;
  unordered_map<size_t, LevelSummary> lsmap;
  unordered_map<size_t, tuple<string, string, string>> lcmap;
  fill_level_code_map(lcmap, caller, user);
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  LocalQuery q;
  for (const auto& fp : format_list) {
    q.set(parameter_query(srv, fp.second, format_list.size()));
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(srv) < 0) {
      log_error2("'" + q.error() + "'", F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    unordered_map<string, string> m;
    for (const auto& r : q) {
      if (m.find(r[0]) == m.end())  {
        m.emplace(r[0], metatranslations::detailed_parameter(pmap, fp.first,
            r[0]));
      }
      auto key = m[r[0]];
      if (pdmap.find(key) == pdmap.end()) {
        ParameterData pd;
        pd.parameter_codes.emplace(r[0]);
        bitmap::uncompress_values(r[1],pd.level_values);
        for (auto& level_value : pd.level_values) {
          pd.level_tables.emplace(level_value, make_pair(r[2], r[3]));
          if (lsmap.find(level_value) == lsmap.end()) {
            LevelSummary ls;
            ls.map = get<0>(lcmap[level_value]);
            ls.type = get<1>(lcmap[level_value]);
            ls.value = get<2>(lcmap[level_value]);
            lsmap.emplace(level_value, ls);
          }
          if (lsmap[level_value].variable_table.find(key) == lsmap[
              level_value].variable_table.end()) {
            lsmap[level_value].variable_table.emplace(key, pd.level_tables[
                level_value]);
          }
        }
        pdmap.emplace(key, pd);
      } else {
        if (pdmap[key].parameter_codes.find(r[0]) == pdmap[key].parameter_codes.
            end()) {
          pdmap[key].parameter_codes.emplace(r[0]);
        }
        vector<size_t> vlst;
        bitmap::uncompress_values(r[1], vlst);
        for (auto& v : vlst) {
          if (pdmap[key].level_tables.find(v) == pdmap[key].level_tables.
              end()) {
            pdmap[key].level_tables.emplace(v, make_pair(r[2], r[3]));
            pdmap[key].level_values.emplace_back(v);
            if (lsmap.find(v) == lsmap.end()) {
              LevelSummary ls;
              ls.map = get<0>(lcmap[v]);
              ls.type = get<1>(lcmap[v]);
              ls.value = get<2>(lcmap[v]);
              lsmap.emplace(v, ls);
            }
            if (lsmap[v].variable_table.find(key) == lsmap[v].variable_table.
                end()) {
              lsmap[v].variable_table.emplace(key, pdmap[key].level_tables[v]);
            }
          } else {
            if (r[2] < pdmap[key].level_tables[v].first) {
              pdmap[key].level_tables[v].first = r[2];
            }
            if (r[3] > pdmap[key].level_tables[v].second) {
              pdmap[key].level_tables[v].second = r[3];
            }
            if (pdmap[key].level_tables[v].first < lsmap[v].variable_table[key].
                first) {
              lsmap[v].variable_table[key].first = pdmap[key].level_tables[v].
                  first;
            }
            if (pdmap[key].level_tables[v].second > lsmap[v].variable_table[
                key].second) {
              lsmap[v].variable_table[key].second = pdmap[key].level_tables[v].
                  second;
            }
          }
        }
      }
    }
    ofs_p << "<div id=\"" << fp.first << "_anchor\"></div>";
    ofs_l << "<div id=\"" << fp.first << "_anchor\"></div>";
    if (format_list.size() > 1) {
      ofs_p << "<table width=\"100%\" border=\"0\"><tr><td>" << endl;
      ofs_l << "<table width=\"100%\" border=\"0\"><tr><td>" << endl;
      for (auto e : format_list) {
        if (fp.first != e.first) {
          auto f = e.first;
          replace_all(f, "proprietary", "dataset-specific");
          ofs_p << "<span class=\"paneltext\">Go to <a href=\"javascript:void("
              "0)\" onClick=\"javascript:scrollTo('" << e.first <<
              "_anchor')\">" << to_capital(f) << "</a> Summary</span><br>" <<
              endl;
          ofs_l << "<span class=\"paneltext\">Go to <a href=\"javascript:void("
              "0)\" onClick=\"javascript:scrollTo('" << e.first <<
              "_anchor')\">" << to_capital(f) << "</a> Summary</span><br>" <<
              endl;
        }
      }
      ofs_p << "</td></tr></table>" << endl;
      ofs_l << "</td></tr></table>" << endl;
    }
    auto f = fp.first;
    replace_all(f, "proprietary", "dataset-specific");
    auto ncols = 2;
    if (fp.first == "WMO_GRIB1") {
      ncols = 3;
    }
    ofs_p << "<table class=\"insert\" width=\"100%\" cellspacing=\"1\" "
        "cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan=\""
        << ncols << "\" align=\"center\">Summary for Grids in " << strutils::
        to_capital(f) << " Format</th></tr>" << endl;
    ofs_l << "<table class=\"insert\" width=\"100%\" cellspacing=\"1\" "
        "cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan=\""
        << ncols << "\" align=\"center\">Summary for Grids in " << strutils::
        to_capital(f) << " Format</th></tr>" << endl;
    LocalQuery q(time_range_query(srv, fp.second));
#ifdef DUMP_QUERIES
    {
    Timer tm;
    tm.start();
#endif
    if (q.submit(srv) < 0) {
      log_error2("'" + q.error() + "'", F, caller, user);
    }
#ifdef DUMP_QUERIES
    tm.stop();
    cerr << "Elapsed time: " << tm.elapsed_time() << " " << F << ": " << q.
        show() << endl;
    }
#endif
    ofs_p << "<tr class=\"bg0\"><td align=\"left\" colspan=\"" << ncols <<
        "\"><b>Product and Coverage Information:</b><br>";
    ofs_l << "<tr class=\"bg0\"><td align=\"left\" colspan=\"" << ncols <<
        "\"><b>Product and Coverage Information:</b><br>";
    if (q.num_rows() == 1) {
      Row row;
      q.fetch_row(row);
      ofs_p << "There is one product and one geographical coverage for all of "
          "the grids in this format:<ul class=\"paneltext\"><span class="
          "\"underline\">Product:</span> " << row[0] << "<br><span class="
          "\"underline\">Geographical Coverage:</span> " << gridutils::
          convert_grid_definition(row[1] + "<!>" + row[2]) << "</ul>";
      ofs_l << "There is one product and one geographical coverage for all of "
          "the grids in this format:<ul class=\"paneltext\"><span class="
          "\"underline\">Product:</span> " << row[0] << "<br><span class="
          "\"underline\">Geographical Coverage:</span> " << gridutils::
          convert_grid_definition(row[1] + "<!>" + row[2]) << "</ul>";
    } else {
      ofs_p << "There are multiple products and/or geographical coverages for "
          "the grids in this format.  Click a variable name to see the "
          "available products and coverages for that variable.";
      ofs_l << "There are multiple products and/or geographical coverages for "
          "the grids in this format.  Click a vertical level description to "
          "see the available products and coverages for that vertical level.";
      pfv.emplace_back(fp.first + "<!>" + fp.second);
    }
    ofs_p << "</td></tr>" << endl;
    ofs_l << "</td></tr>" << endl;
    ofs_p << "<tr class=\"bg2\"><th align=\"left\">Variable</th>";
    ofs_l << "<tr class=\"bg2\"><th align=\"left\">Vertical Level</th>";
    if (fp.first == "WMO_GRIB1") {
      ofs_p << "<th>Parameter<br>Code</th>";
    }
    ofs_p << "<th align=\"left\"><table class=\"paneltext\" width=\"100%\" "
        "cellspacing=\"0\" cellpadding=\"0\" border=\"0\"><tr valign=\"top\" "
        "class=\"bg2\"><th align=\"left\">Vertical Levels</th><th width="
        "\"41%\" align=\"left\">Temporal Valid Range</th></tr></table></th>"
        "</tr>" << endl;
    ofs_l << "<th width=\"60%\" align=\"left\"><table class=\"paneltext\" "
        "width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\"><tr "
        "valign=\"top\" class=\"bg2\"><th align=\"left\">Variables</th><th "
        "width=\"41%\" align=\"left\">Temporal Valid Range</th></tr></table>"
        "</th></tr>" << endl;
    vector<string> pd_keys;
    for (const auto& e : pdmap) {
      pd_keys.emplace_back(e.first);
    }
    sort(pd_keys.begin(), pd_keys.end(),
    [](const string& left, const string& right) -> bool {
      return metacompares::default_compare(left, right);
    });
    auto cidx = 0;
    unordered_map<size_t, string> ldmap;
    for (const auto& k : pd_keys) {
      auto kk = k;
      replace_all(kk, "&lt;", "<");
      replace_all(kk, "&gt;", ">");
      ofs_p << "<tr class=\"bg" << cidx << "\" valign=\"top\"><td>";
      if (q.num_rows() > 1) {
        ofs_p << "<a title=\"Products and Coverages\" href=\"javascript:void("
            "0)\" onClick=\"popModalWindowWithGetUrl(950, 600, '/cgi-bin/"
            "transform?dsnum=" << metautils::args.dsid << "&view=prodcov&"
            "formatCode=" << fp.second << "&ftype=" << strutils::to_lower(
            file_type);
        for (const auto& e : pdmap[k].parameter_codes) {
          ofs_p << "&pcode=" << e;
        }
        ofs_p << "')\">" << kk << "</a>";
      } else {
        ofs_p << kk;
      }
      unordered_map<string, pair<string, string>> ulmap;
      for (auto& v : pdmap[k].level_values) {
        if (ldmap.find(v) == ldmap.end()) {
          ldmap.emplace(v, metatranslations::detailed_level(lmap, fp.first, get<
              0>(lcmap[v]), get<1>(lcmap[v]), get<2>(lcmap[v]), true));
        }
        auto key = ldmap[v];
        if (ulmap.find(key) == ulmap.end()) {
          ulmap.emplace(key, pdmap[k].level_tables[v]);
        } else {
          if (pdmap[k].level_tables[v].first < ulmap[key].first) {
            ulmap[key].first=pdmap[k].level_tables[v].first;
          }
          if (pdmap[k].level_tables[v].second > ulmap[key].second) {
            ulmap[key].second=pdmap[k].level_tables[v].second;
          }
        }
      }
      pdmap[k].level_tables.clear();
      pdmap[k].level_values.clear();
      if (ulmap.size() > 10) {
        ofs_p << "<br /><span style=\"font-size: 13px; color: #6a6a6a\">(" <<
            ulmap.size() << " levels; scroll to see all)</span>";
      }
      ofs_p << "</td>";
      if (fp.first == "WMO_GRIB1") {
        auto it = pdmap[k].parameter_codes.begin();
        auto idx = it->find(":");
        ofs_p << "<td align=\"center\">" << it->substr(idx + 1) << "</td>";
      }
      ofs_p << "<td>";
      if (ulmap.size() > 10) {
        ofs_p << "<div class=\"detail_scroll1\">";
      } else {
        ofs_p << "<div class=\"detail_scroll2\">";
      }
      ofs_p << "<table style=\"font-size: 14px\" width=\"100%\" cellspacing=\""
          "0\" cellpadding=\"0\" border=\"0\">";
      size_t s = ulmap.size() - 1;
      vector<string> ulv;
      ulv.reserve(ulmap.size());
      for (const auto& e : ulmap) {
        ulv.emplace_back(e.first);
      }
      sort(ulv.begin(), ulv.end(),
      [](const string& left, const string& right) -> bool {
        return metacompares::compare_levels(left, right);
      });
      size_t n = 0;
      for (auto& e : ulv) {
        ofs_p << "<tr valign=\"top\"><td style=\"border-bottom: " <<
            static_cast<int>(n < s) << "px solid #96a4bf\" align=\"left\">"
            "&bull;&nbsp;" << substitute(e,"_"," ") << "</td><td style="
            "\"border-bottom: " << static_cast<int>(n < s) << "px solid "
            "#96a4bf\">&nbsp;&nbsp;</td><td style=\"border-bottom: " <<
            static_cast<int>(n < s) << "px solid #96a4bf\" width=\"40%\" "
            "align=\"left\"><nobr>" << dateutils::string_ll_to_date_string(
            ulmap[e].first) << " to " << dateutils::string_ll_to_date_string(
            ulmap[e].second) << "</nobr></td></tr>";
        ++n;
      }
      ofs_p << "</table></div></td></tr>" << endl;
      cidx = 1 - cidx;
    }
    unordered_map<string, CombinedLevelSummary> clmap;
    for (auto& ls : lsmap) {
      auto key = metatranslations::detailed_level(lmap, fp.first, ls.second.map,
          ls.second.type, ls.second.value, true);
      if (clmap.find(key) == clmap.end()) {
        clmap.emplace(key, CombinedLevelSummary());
      }
      clmap[key].map_list.emplace_back(ls.second.map + "[!]" + ls.second.type +
          "[!]" + ls.second.value);
      if (clmap[key].variable_table.size() == 0) {
        clmap[key].variable_table=ls.second.variable_table;
      } else {
        vector<string> v;
        for (const auto& e : ls.second.variable_table) {
          auto it = clmap[key].variable_table.find(e.first);
          if (it != clmap[key].variable_table.end()) {
            auto it2 = ls.second.variable_table.find(e.first);
            if (it2->second.first < it->second.first) {
              it->second.first=it2->second.first;
            }
            if (it2->second.second > it->second.second) {
              it->second.second=it2->second.second;
            }
            v.emplace_back(it2->first);
          }
        }
        for (auto& k : v) {
          ls.second.variable_table.erase(k);
        }
        v.clear();
        for (const auto& e : ls.second.variable_table) {
          auto it = ls.second.variable_table.find(e.first);
          clmap[key].variable_table.emplace(it->first,
              it->second);
          v.emplace_back(e.first);
        }
        for (auto& k : v) {
          ls.second.variable_table.erase(k);
        }
        v.clear();
      }
    }
    lsmap.clear();
    vector<string> clv;
    for (const auto& e : clmap) {
      clv.emplace_back(e.first);
    }
    sort(clv.begin(), clv.end(),
    [](const string& left, const string& right) -> bool {
      return metacompares::compare_levels(left, right);
    });
    for (auto& key : clv) {
      ofs_l << "<tr class=\"bg" << cidx << "\" valign=\"top\"><td><table><tr "
          "valign=\"bottom\"><td>";
      if (q.num_rows() > 1) {
        ofs_l << "<a title=\"Products and Coverages\" href=\"javascript:void("
            "0)\" onClick=\"popModalWindowWithGetUrl(950, 600, '/cgi-bin/"
            "transform?dsnum=" << metautils::args.dsid << "&view=prodcov&"
            "formatCode=" << fp.second << "&ftype=" << strutils::to_lower(
            file_type);
        for (auto& e : clmap[key].map_list) {
          ofs_l << "&map=" << e;
        }
        ofs_l << "')\">" << key << "</a>";
      } else {
        ofs_l << key;
      }
      if (clmap[key].variable_table.size() > 10) {
        ofs_l << "<br /><span style=\"font-size: 13px; color: #6a6a6a\">(" <<
            clmap[key].variable_table.size() << " variables; scroll to see all)"
            "</span>";
      }
      ofs_l << "</td></tr></table></td><td>";
      if (clmap[key].variable_table.size() > 10) {
        ofs_l << "<div class=\"detail_scroll1\">";
      } else {
        ofs_l << "<div class=\"detail_scroll2\">";
      }
      ofs_l << "<table style=\"font-size: 14px\" width=\"100%\" cellspacing="
          "\"0\" cellpadding=\"0\" border=\"0\">";
      auto s = clmap[key].variable_table.size() - 1;
      size_t n = 0;
      vector<string> vv;
      vv.reserve(clmap[key].variable_table.size());
      for (const auto& e : clmap[key].variable_table) {
        vv.emplace_back(e.first);
      }
      sort(vv.begin(), vv.end(),
      [](const string& left, const string& right) -> bool {
        return  metacompares::default_compare(left, right);
      });
      for (const auto& e : vv) {
        ofs_l << "<tr valign=\"top\"><td style=\"border-bottom: " <<
            static_cast<int>(n < s) << "px solid #96a4bf\" align=\"left\">"
            "&bull;&nbsp;" << e << "</td><td style=\"border-bottom: " <<
            static_cast<int>(n < s) << "px solid #96a4bf\">&nbsp;&nbsp;</td>"
            "<td style=\"border-bottom: " << static_cast<int>(n < s) << "px "
            "solid #96a4bf\" width=\"40%\" align=\"left\"><nobr>" << dateutils::
            string_ll_to_date_string(clmap[key].variable_table[e].first) <<
            " to " << dateutils::string_ll_to_date_string(clmap[key].
            variable_table[e].second) << "</nobr></td></tr>";
        ++n;
      }
      clmap[key].variable_table.clear();
      ofs_l << "</table></div></td></tr>" << endl;
      cidx = 1 - cidx;
    }
    clmap.clear();
    pdmap.clear();
    ofs_p << "</table>" << endl;
    ofs_l << "</table>" << endl;
  }
  ofs_p.close();
  ofs_l.close();
  if (pfv.size() > 0) {
    generate_gridded_product_detail(srv, file_type, format_list, t, caller,
        user);
  }
  srv.disconnect();
  string e;
  if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/" +
      metautils::args.dsid, metautils::directives.rdadata_home, e) < 0) {
    metautils::log_warning(F + " couldn't sync detail files - rdadata_sync "
        "error(s): '" + e + "'", caller, user);
  }
  write_grid_html(ofs, pfv.size());
}

void generate_detailed_observation_summary(string file_type, ofstream& ofs,
    const vector<pair<string, string>>& format_list, string caller, string
    user) {
  static const string F = this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }

  // create the directory tree in the temp directory
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -m 0755 -p " + t.name() + "/metadata", oss, ess) !=
      0) {
    log_error2("unable to create temporary directory tree - '" + ess.str() +
        "'", F, caller, user);
  }
  ofstream ofs_o;
  open_output(ofs_o, t.name() + "/metadata/obs-detail.html");
  if (!ofs_o.is_open()) {
    log_error2("unable to open output for observation detail", F, caller, user);
  }
  Server srv(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv) {
    log_error2("unable to connect to the database: '" + srv.error() + "'", F,
        caller, user);
  }
  xmlutils::DataTypeMapper data_type_mapper(metautils::directives.
      parameter_map_path);
  for (const auto& fp : format_list) {
    map<string, TypeEntry> platform_table;
    auto data_format = fp.first;
    ofs_o << "<br><center><table class=\"insert\" width=\"95%\" cellspacing=\""
        "1\" cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" "
        "colspan=\"4\" align=\"center\">Summary for Platform Observations in "
        << to_capital(substitute(data_format,"proprietary_","")) << " Format"
        "</th></tr>" << endl;
    auto cindex = 0;
    ofs_o << "<tr class=\"bg2\" valign=\"top\"><th align=\"left\">Observing "
        "Platform</th><th align=\"left\">Observation Type<ul class=\""
        "paneltext\"><li>Data Types</li></ul></th><td align=\"left\"><b>"
        "Average Frequency of Data</b><br>(varies by individual platform ID "
        "and data type)</td><td align=\"left\"><b>Temporal/Geographical "
        "Coverage</b><br>(each dot represents a 3&deg; box containing one or "
        "more observations)</td></tr>" << endl;
    Query query("select distinct t.platform_type, o.obs_type, l.data_type, min("
        "start_date),max(end_date) from \"WObML\"." + metautils::args.dsid +
        "_data_types as d left join \"WObML\"." + metautils::args.dsid +
        "_data_types_list as l on l.code = d.data_type_code left join \"WObML\"."
        + metautils::args.dsid + "_webfiles2 as p on p.code = d.file_code left "
        "join \"WObML\".platform_types as t on t.code = l.platform_type_code "
        "left join \"WObML\".obs_types as o on o.code = l.observation_type_code "
        "where p.format_code = " + fp.second + " group by t.platform_type, o."
        "obs_type, l.data_type order by t.platform_type, o.obs_type, l."
        "data_type");
    if (query.submit(srv) < 0) {
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    }
    TypeEntry te, te2;
    for (const auto& res : query) {
      auto pt_key = to_lower(res[0]);
      auto it = platform_table.find(pt_key);
      if (it == platform_table.end()) {

        // te is the entry for an individual platform; te.type_table_keys is the
        //   list of corresponding observation types
        te.key = res[0];
        te.type_table.reset(new my::map<TypeEntry>);
        te.type_table_keys.reset(new list<string>);

        // te2 is the entry for an individual observation type;
        //   te2.type_table_keys is the list of corresponding dataTypes (don't
        //   need te2.type_table for uniqueness because query guarantees each
        //   dataType will be unique
        te2.key = res[1];
        te2.start = res[3];
        te2.end = res[4];
        te2.type_table_keys.reset(new list<string>);
        te2.type_table_keys->emplace_back(res[2]);
        te2.type_table_keys_2 = nullptr;
        te.type_table->insert(te2);
        te.type_table_keys->emplace_back(te2.key);
        platform_table.emplace(pt_key, te);
      } else {
        if (it->second.type_table->found(res[1], te2)) {
          if (res[3] < te2.start) {
            te2.start = res[3];
          }
          if (res[4] > te2.end) {
            te2.end = res[4];
          }
          te2.type_table_keys->emplace_back(res[2]);
          it->second.type_table->replace(te2);
        } else {
          te2.key = res[1];
          te2.start = res[3];
          te2.end = res[4];
          te2.type_table_keys.reset(new list<string>);
          te2.type_table_keys->emplace_back(res[2]);
          te2.type_table_keys_2 = nullptr;
          it->second.type_table->insert(te2);
          it->second.type_table_keys->emplace_back(te2.key);
        }
      }
    }
    if (field_exists(srv, "WObML." + metautils::args.dsid + "_webfiles2",
        "min_obs_per")) {
      query.set("select any_value(l.platform_type), any_value(o.obs_type), min("
          "f.min_obs_per), max(f.max_obs_per), f.unit from \"WObML\"." +
          metautils::args.dsid + "_webfiles2 as p join \"WObML\"." + metautils::
          args.dsid + "_frequencies as f on p.code = f.file_code left join "
          "\"WObML\".obs_types as o on o.code = f.observation_type_code left "
          "join \"WObML\".platform_types as l on l.code = f.platform_type_code "
          "left join \"WObML\".frequency_sort as s on s.keyword = f.unit where "
          "p.format_code = " + fp.second + " group by f.observation_type_code, "
          "f.platform_type_code, f.unit, s.idx order by s.idx");
    } else {
      query.set("select any_value(l.platform_type), any_value(o.obs_type), min("
          "f.avg_obs_per), max(f.avg_obs_per), f.unit from \"WObML\"." +
          metautils::args.dsid + "_webfiles2 as p join \"WObML\"." + metautils::
          args.dsid + "_frequencies as f on p.code = f.file_code left join "
          "\"WObML\".obs_types as o on o.code = f.observation_type_code left "
          "join \"WObML\".platform_types as l on l.code = f.platform_type_code "
          "left join \"WObML\".frequency_sort as s on s.keyword = f.unit where "
          "p.format_code = " + fp.second + " group by f.observation_type_code, "
          "f.platform_type_code, f.unit, s.idx order by s.idx");
    }
    if (query.submit(srv) < 0)
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    for (const auto& res : query) {
      auto pt_key = to_lower(res[0]);
      auto it = platform_table.find(pt_key);
      if (it != platform_table.end()) {
        it->second.type_table->found(res[1], te2);
        if (te2.type_table_keys_2 == NULL) {
          te2.type_table_keys_2.reset(new list<string>);
        }
        if (res[2] == res[3]) {
          te2.type_table_keys_2->emplace_back("<nobr>" + res[2] + " per " + res[
              4] + "</nobr>");
        } else {
          te2.type_table_keys_2->emplace_back("<nobr>" + res[2] + " to " + res[
              3] + "</nobr> per " + res[4]);
        }
        it->second.type_table->replace(te2);
      }
    }
    for (auto& e : platform_table) {
      unordered_set<string> unique_data_types;
      auto n = 0;
      for (const auto& key2 : *(e.second).type_table_keys) {
        if (n == 0) {
          ofs_o << "<tr class=\"bg" << cindex << "\" valign=\"top\"><td "
              "rowspan=\"" << e.second.type_table->size() << "\">" <<
              to_capital(e.second.key) << "</td>";
        } else {
          ofs_o << "<tr class=\"bg" << cindex << "\" valign=\"top\">";
        }
        te2.key = key2;
        e.second.type_table->found(te2.key, te2);
        ofs_o << "<td>" << to_capital(te2.key) << "<ul class=\"paneltext\">";
        for (const auto& key3 : *te2.type_table_keys) {
          auto key = metatranslations::detailed_datatype(data_type_mapper,
              fp.first, key3);
          if (unique_data_types.find(key) == unique_data_types.end()) {
            ofs_o << "<li>" << key << "</li>";
            unique_data_types.insert(key);
          }
        }
        ofs_o << "</ul></td><td><ul class=\"paneltext\">";
        if (te2.type_table_keys_2 != NULL) {
          for (const auto& key3 : *te2.type_table_keys_2) {
            ofs_o << "<li>" << key3 << "</li>";
          }
        }
        ofs_o << "</ul></td><td align=\"center\">" << dateutils::
            string_ll_to_date_string(te2.start) << " to " << dateutils::
            string_ll_to_date_string(te2.end);
        if (unixutils::exists_on_server(metautils::directives.web_server,
            "/data/web/datasets/" + metautils::args.dsid + "/metadata/" +
            te2.key + "." + e.second.key + ".kml", metautils::directives.
            rdadata_home)) {
          ofs_o << "&nbsp;<a href=\"/datasets/" << metautils::args.dsid <<
              "/metadata/" << te2.key << "." << e.second.key << ".kml\"><img "
              "src=\"/images/kml.gif\" width=\"36\" height=\"14\" hspace=\"3\" "
              "border=\"0\" title=\"See stations plotted in Google Earth\">"
              "</a>";
        }
        ofs_o << "<br><img src=\"/datasets/" << metautils::args.dsid <<
            "/metadata/spatial_coverage." << substitute(data_format," ","_") <<
            "_web";
        ofs_o << "_" << key2 << "." << e.second.key << ".gif?" << strand(5) <<
            "\"></td></tr>" << endl;
        te2.type_table_keys->clear();
        te2.type_table_keys.reset();
        ++n;
      }
      e.second.type_table->clear();
      e.second.type_table.reset();
      e.second.type_table_keys->clear();
      e.second.type_table_keys.reset();
      cindex = 1 - cindex;
    }
    ofs_o << "</table></center>" << endl;
  }
  srv.disconnect();
  ofs_o.close();
  string e;
  if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/" +
      metautils::args.dsid, metautils::directives.rdadata_home, e) < 0) {
    metautils::log_warning(F + " couldn't sync detail files - rdadata_sync "
        "error(s): '" + e + "'", caller, user);
  }
}

void generate_detailed_fix_summary(string file_type, ofstream& ofs, const
    vector<pair<string, string>>& format_list, string caller, string user) {
  static const string F = this_function_label(__func__);
  Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"rdadb");
  if (!server) {
    log_error2("unable to connect to the database: '" + server.error() + "'", F,
        caller, user);
  }
  my::map<TypeEntry> platform_table;
  for (const auto& fp : format_list) {
    platform_table.clear();
    auto format=fp.first;
    replace_all(format,"proprietary_","");
    ofs << "<br><center><table class=\"insert\" width=\"95%\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan=\"3\" align=\"center\">Summary for Cyclone Fixes in " << to_capital(format) << " Format</th></tr>" << endl;
    auto cindex=0;
    ofs << "<tr class=\"bg2\" valign=\"top\"><th align=\"left\">Cyclone Classification</th><td align=\"left\"><b>Average Frequency of Data</b><br>(may vary by individual cyclone ID)</td><td align=\"left\"><b>Temporal/Geographical Coverage</b><br>(each dot represents a 3&deg; box containing one or more fixes)</td></tr>" << endl;
    Query query("select distinct classification,min(l.start_date),max(l.end_date) from WFixML."+metautils::args.dsid+"_locations as l left join WFixML."+metautils::args.dsid+"_webfiles2 as p on p.code = l.file_code left join WFixML.classifications as c on c.code = l.classification_code where p.format_code = "+fp.second+" group by classification order by classification");
    if (query.submit(server) < 0) {
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    }
    for (const auto& res : query) {
      TypeEntry te;
      if (!platform_table.found(res[0],te)) {
        te.key=res[0];
        te.start=res[1];
        te.end=res[2];
        te.type_table_keys.reset(new list<string>);
        platform_table.insert(te);
      }
      else {
        if (res[1] < te.start) {
          te.start=res[1];
        }
        if (res[2] > te.end) {
          te.end=res[2];
        }
        platform_table.replace(te);
      }
    }
    query.set("select any_value(c.classification),min(f.min_obs_per),max(f.max_obs_per),f.unit from \"WFixML\"."+metautils::args.dsid+"_webfiles2 as p left join \"WFixML\"."+metautils::args.dsid+"_frequencies as f on p.code = f.file_code left join \"WFixML\".classifications as c on c.code = f.classification_code left join \"WObML\".frequency_sort as s on s.keyword = f.unit where p.format_code = "+fp.second+" group by f.classification_code,f.unit,s.idx order by s.idx");
    if (query.submit(server) < 0) {
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    }
    for (const auto& res : query) {
      TypeEntry te;
      if (platform_table.found(res[0],te)) {
        if (res[1] == res[2]) {
          te.type_table_keys->emplace_back("<nobr>"+res[1]+" per "+res[3]+"</nobr>");
        }
        else {
          te.type_table_keys->emplace_back("<nobr>"+res[1]+" to "+res[2]+"</nobr> per "+res[3]);
        }
      }
    }
    for (const auto& key : platform_table.keys()) {
      TypeEntry te;
      platform_table.found(key,te);
      ofs << "<tr class=\"bg" << cindex << "\" valign=\"top\"><td>" << to_capital(te.key) << "</td><td><ul class=\"paneltext\">";
      for (auto& key2 : *te.type_table_keys) {
        ofs << "<li>" << key2 << "</li>";
      }
      ofs << "</ul></td><td align=\"center\">" << dateutils::string_ll_to_date_string(te.start) << " to " << dateutils::string_ll_to_date_string(te.end) << "<br><img src=\"/datasets/" << metautils::args.dsid << "/metadata/spatial_coverage.web";
      ofs << "_" << te.key << ".gif?" << strand(5) << "\"></td></tr>" << endl;
      te.type_table_keys->clear();
      te.type_table_keys.reset();
      cindex=1-cindex;
    }
    ofs << "</table></center>" << endl;
  }
  server.disconnect();
}

void generate_detailed_metadata_view(string caller, string user) {
  static const string F = this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  string f;
  struct stat buf;
  if (stat((metautils::directives.server_root + "/web/metadata/"
      "FormatReferences.xml").c_str(), &buf) == 0) {
    f = metautils::directives.server_root + "/web/metadata/FormatReferences."
        "xml";
  } else {
    f = unixutils::remote_web_file("https://rda.ucar.edu/metadata/"
        "FormatReferences.xml", t.name());
  }
  XMLDocument xdoc(f);
  if (!xdoc) {
    log_error2("unable to open FormatReferences.xml", F, caller, user);
  }
  Server srv_m(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!srv_m) {
    log_error2("unable to connect to the database: '" + srv_m.error() + "'", F,
        caller, user);
  }
  string fmts, dtyps;
  auto b = false;

  // svec contains the database, the data type description, and a function
  //  that generates an appropriate summary
  vector<tuple<string, string, void(*)(string, ofstream&, const vector<pair<
      string, string>>&, string, string)>> svec{
    make_tuple("WGrML", "Grids", generate_detailed_grid_summary),
    make_tuple("WObML", "Platform Observations",
    generate_detailed_observation_summary),
    make_tuple("WFixML", "Cyclone Fix", generate_detailed_fix_summary),
  };
  vector<vector<pair<string, string>>> vv(svec.size());
  for (size_t n = 0; n < svec.size(); ++n) {
    auto tbl = metautils::args.dsid + "_webfiles2";
    if (table_exists(srv_m, get<0>(svec[n]) + "." + tbl)) {
      b = true;
      dtyps += "<li>" + get<1>(svec[n]) + " </li>";
      add_to_formats(xdoc, get<0>(svec[n]), tbl, vv[n], fmts, caller, user);
    }
  }
  xdoc.close();
  if (stat((metautils::directives.server_root+"/web/datasets/"+metautils::args.dsid+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
    f=metautils::directives.server_root+"/web/datasets/"+metautils::args.dsid+"/metadata/dsOverview.xml";
  } else {
    f=unixutils::remote_web_file("https://rda.ucar.edu/datasets/"+metautils::args.dsid+"/metadata/dsOverview.xml",t.name());
  }
  xdoc.open(f);
  if (!xdoc) {
    if (f.empty()) {
      metautils::log_warning("generate_detailed_metadata_view() returned warning: unable to access dsOverview.xml from the web server",caller,user);
    } else {
      metautils::log_warning("generate_detailed_metadata_view() returned warning: unable to open "+f+" - error: '"+xdoc.parse_error()+"'",caller,user);
    }
  } else {
    auto e=xdoc.element("dsOverview/title");
    Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"rdadb");
    if (!b) {
      auto elist=xdoc.element_list("dsOverview/contentMetadata/dataType");
      for (const auto& element : elist) {
        dtyps+="<li>";
        auto data_type=element.content();
        if (data_type == "grid") {
          dtyps+="Grids";
        } else if (data_type == "platform_observation") {
          dtyps+="Platform Observations";
        } else {
          dtyps+=to_capital(data_type);
        }
        dtyps+="</li>";
      }
      elist=xdoc.element_list("dsOverview/contentMetadata/format");
      for (auto element : elist) {
        auto format=element.content();
        replace_all(format,"proprietary_","");
        fmts+="<li>"+to_capital(format)+"</li>";
      }
    }
// create the metadata directory tree in the temp directory
    stringstream oss,ess;
    if (mysystem2("/bin/mkdir -m 0755 -p "+t.name()+"/metadata",oss,ess) != 0) {
      metautils::log_error("generate_detailed_metadata_view(): unable to create metadata directory tree - '"+ess.str()+"'",caller,user);
    }
    ofstream ofs;
    open_output(ofs, t.name()+"/metadata/detailed.html");
    if (!ofs.is_open()) {
      metautils::log_error("generate_detailed_metadata_view(): unable to open output for detailed.html",caller,user);
    }
    ofs << "<style id=\"detail\">" << endl;
    ofs << "  @import url(/css/transform.css);" << endl;
    ofs << "</style>" << endl;
    ofs << "<?php" << endl;
    ofs << "  include_once(\"MyDBI.inc\");" << endl;
    ofs << "  default_dbinfo(\"\",\"metadata\",\"metadata\");" << endl;
    auto ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
    ofs << "  $title=myget(\"search.datasets\",\"title\",\"dsid in " << ds_set << "\");" << endl;
    ofs << "  $contributors=mymget(\"\",\"\",\"select g.path from search.contributors_new as c left join search.gcmd_providers as g on g.uuid = c.keyword where dsid in " << ds_set << " order by disp_order\");" << endl;
    ofs << "  $projects=mymget(\"\",\"\",\"select g.path from search.projects_new as p left join search.gcmd_projects as g on g.uuid = p.keyword  where dsid in " << ds_set << "\");" << endl;
    ofs << "  $supportedProjects=mymget(\"\",\"\",\"select g.path from search.supported_projects as p left join search.gcmd_projects as g on g.uuid = p.keyword where dsid in " << ds_set << "\");" << endl;
    ofs << "?>" << endl;
    ofs << "<p>The information presented here summarizes the data in the primary (NCAR HPSS) archive of "+metautils::args.dsid+".  Some or all of these data may not be directly accessible from our web server.  If you have questions about data access, please contact the dataset specialist named above.</p>" << endl;
    ofs << "<br>" << endl;
    ofs << "<center><table class=\"insert\" width=\"95%\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\">" << endl;
    ofs << "<tr><th class=\"headerRow\" align=\"center\" colspan=\"2\">Overview</th></tr>" << endl;
    ofs << "<tr><td class=\"bg0\" align=\"left\" colspan=\"2\"><b>Dataset Title:</b>&nbsp;<?php print $title[\"title\"]; ?></td></tr>" << endl;
// citation
    ofs << "<tr><td class=\"bg0\" align=\"left\" colspan=\"2\"><b>Dataset Citation:</b><br /><div id=\"citation\" style=\"margin-bottom: 5px\"><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onLoad=\"getAjaxContent('GET',null,'/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsid << "&style=esip','citation')\" /></div><div style=\"display: inline; background-color: #2a70ae; color: white; padding: 1px 8px 1px 8px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsid << "&style=ris'\" title=\"download citation in RIS format\">RIS</div><div style=\"display: inline; background-color: #2a70ae; color: white; width: 60px; padding: 2px 8px 1px 8px; font-size: 16px; font-weight: bold; font-family: serif; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer; margin-left: 7px\" onClick=\"location='/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsid << "&style=bibtex'\" title=\"download citation in BibTeX format\">BibTeX</div></td></tr>" << endl;
    ofs << "<tr valign=\"top\">" << endl;
    if (!dtyps.empty()) {
      ofs << "<td width=\"50%\" class=\"bg0\" align=\"left\"><b>Types of data:</b><ul class=\"paneltext\">"+dtyps+"</ul></td>" << endl;
    }
    if (!fmts.empty()) {
      ofs << "<td class=\"bg0\" align=\"left\"><b>Data formats:</b><ul class=\"paneltext\">"+fmts+"</ul></td>" << endl;
    }
    ofs << "</tr>" << endl;
    ofs  << "<tr valign=\"top\"><td class=\"bg0\" colspan=\"2\"><b>Data contributors:</b><ul class=\"paneltext\">" << endl;
    ofs << "<?php" << endl;
    ofs << "  for ($n=0; $n < count($contributors[\"path\"]); $n++) {" << endl;
    ofs << "    print \"<li>\" . $contributors[\"path\"][$n] . \"</li>\n\";" << endl;
    ofs << "  }" << endl;
    ofs << "?>" << endl;
    ofs << "</ul></td></tr>" << endl;
    ofs << "<?php" << endl;
    ofs << "  if ($projects) {" << endl;
    ofs << "    print \"<tr valign=\\\"top\\\"><td class=\\\"bg0\\\" colspan=\\\"2\\\"><b>Programs/Experiments that collected the data:</b><ul class=\\\"paneltext\\\">\\n\";" << endl;
    ofs << "    for ($n=0; $n < count($projects[\"path\"]); $n++) {" << endl;
    ofs << "      print \"<li>\" . $projects[\"path\"][$n] . \"</li>\n\";" << endl;
    ofs << "    }" << endl;
    ofs << "    print \"</ul></td></tr>\\n\";" << endl;
    ofs << "  }" << endl;
    ofs << "?>" << endl;
    ofs << "<?php" << endl;
    ofs << "  if ($supportedProjects) {" << endl;
    ofs << "    print \"<tr valign=\\\"top\\\"><td class=\\\"bg0\\\" colspan=\\\"2\\\"><b>Projects that are supported by the data:</b><ul class=\\\"paneltext\\\">\\n\";" << endl;
    ofs << "    for ($n=0; $n < count($supportedProjects[\"path\"]); $n++) {" << endl;
    ofs << "      print \"<li>\" . $supportedProjects[\"path\"][$n] . \"</li>\n\";" << endl;
    ofs << "    }" << endl;
    ofs << "    print \"</ul></td></tr>\\n\";" << endl;
    ofs << "  }" << endl;
    ofs << "?>" << endl;
    LocalQuery query("dweb_size","dssdb.dataset","dsid in "+ds_set);
    if (query.submit(server_d) < 0) {
      myerror = query.error();
      exit(1);
    }
    double volume;
    Row row;
    query.fetch_row(row);
    if (!row[0].empty()) {
      volume=stoll(row[0]);
    } else {
      volume=0.;
    }
    auto n=0;
    while ( (volume/1000.) >= 1.) {
      volume/=1000.;
      ++n;
    }
    const int VUNITS_LEN=6;
    const char *vunits[VUNITS_LEN]={"","K","M","G","T","P"};
    if (n >= VUNITS_LEN) {
      metautils::log_error("generate_detailed_metadata_view() - dataset primary size exceeds volume units specification",caller,user);
    }
    ofs << "<tr valign=\"top\"><td class=\"bg0\" colspan=\"2\"><b>Total volume:</b>&nbsp;&nbsp;" << strutils::ftos(volume,5,3,' ') << " " << vunits[n] << "bytes</td></tr>" << endl;
    if (!b) {
      ofs << "<tr valign=\"top\">" << endl;
      auto elist=xdoc.element_list("dsOverview/contentMetadata/temporal");
      ofs << "<td class=\"bg0\"><b>Temporal Range(s):</b><ul>" << endl;
      for (const auto& element : elist) {
        e=element;
        auto date=e.attribute_value("start");
        ofs << "<li>"+date+" to ";
        date=e.attribute_value("end");
        ofs << date+"</li>" << endl;
      }
      ofs << "</ul></td>" << endl;
      elist=xdoc.element_list("dsOverview/contentMetadata/detailedVariables/detailedVariable");
      if (elist.size() > 0) {
        ofs << "<td class=\"bg0\"><b>Detailed Variable List:</b><ul>" << endl;
      } else {
        ofs << "<td class=\"bg0\">&nbsp;" << endl;
      }
      for (const auto& element : elist) {
        ofs << "<li>"+element.content();
        auto units=element.attribute_value("units");
        if (!units.empty()) {
          ofs << " <small class=\"units\">("+htmlutils::transform_superscripts(units)+")</small>";
        }
        ofs << "</li>" << endl;
      }
      if (elist.size() > 0) {
        ofs << "</ul></td>" << endl;
      } else {
        ofs << "</td>" << endl;
      }
      ofs << "</tr>" << endl;
    }
    ofs << "</table></center>" << endl;
    for (size_t n = 0; n < svec.size(); ++n) {
      if (!vv[n].empty()) {
        auto& generate_summary = get<2>(svec[n]);
        generate_summary("Web", ofs, vv[n], caller, user);
      }
    }
    ofs.close();
    string error;
    if (unixutils::rdadata_sync(t.name(),"metadata/","/data/web/datasets/"+metautils::args.dsid,metautils::directives.rdadata_home,error) < 0) {
      metautils::log_warning("generate_detailed_metadata_view() couldn't sync 'detailed.html' - rdadata_sync error(s): '"+error+"'",caller,user);
    }
    xdoc.close();
    server_d.disconnect();
  }
  srv_m.disconnect();
// generate parameter cross-references
  generate_parameter_cross_reference("WMO_GRIB1","GRIB","grib.html",caller,user);
  generate_parameter_cross_reference("WMO_GRIB2","GRIB2","grib2.html",caller,user);
  generate_level_cross_reference("WMO_GRIB2","GRIB2","grib2_levels.html",caller,user);
  generate_parameter_cross_reference("NCEP_ON84","ON84","on84.html",caller,user);
}

void generate_group_detailed_metadata_view(string group_index,string file_type,string caller,string user)
{
  LocalQuery query,grml_query,obml_query,fixml_query;
  string gtitle,gsummary,output,error;
//  list<string> formatList;
//  bool foundDetail=false;

  Server server_m(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"rdadb");
  Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"rdadb");
  if (!group_index.empty() || group_index == "0") {
    return;
  }
  while (group_index != "0") {
    if (server_d.update("dsgroup","meta_link = 'X'","dsid in "+to_sql_tuple_string(ds_aliases(metautils::args.dsid))+" and gindex = "+group_index) < 0) {
      metautils::log_warning("generate_group_detailed_metadata_view() returned warning: "+server_d.error()+" while trying to update dssdb.dsgroup",caller,user);
    }
    query.set("pindex","dsgroup","gindex = "+group_index);
    if (query.submit(server_d) < 0) {
      metautils::log_error("generate_group_detailed_metadata_view(): "+query.error(),caller,user);
    }
    Row row;
    if (query.fetch_row(row)) {
      group_index=row[0];
    } else {
      group_index="0";
    }
  }
  server_m.disconnect();
  server_d.disconnect();
}

} // end namespace gatherxml::detailedMetadata

} // end namespace gatherxml
