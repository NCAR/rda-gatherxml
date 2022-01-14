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
#include <MySQL.hpp>
#include <bsort.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <search.hpp>
#include <xml.hpp>
#include <gridutils.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::endl;
using std::get;
using std::list;
using std::make_pair;
using std::make_tuple;
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
using strutils::itos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using unixutils::mysystem2;

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

struct DBData {
  DBData() : db(), table(), dtable(), ID_type() { }

  string db, table, dtable, ID_type;
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
  } else {
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
      auto sp = split(left.code, ".");
      auto sp2 = split(right.code, ".");
      bool b = true;
      if (sp.size() != sp2.size()) {
        if (left.code < right.code) {
          b = true;
        } else {
          b = false;
        }
      } else {
        for (size_t n = 0; n < sp.size(); n++) {
          auto l = stoi(sp[n]);
          auto r = stoi(sp2[n]);
          if (l < r) {
            b = true;
            n = sp.size();
          } else if (l > r) {
            b = false;
            n = sp.size();
          }
        }
      }
      return b;
    }
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
  } else {
    if (l2 <= r2) {
      return true;
    }
    return false;
  }
}

void fill_level_code_table(string db, unordered_map<size_t, tuple<string,
    string, string>>& level_code_map) {
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  MySQL::LocalQuery q("code, map, type, value", db + ".levels");
  if (q.submit(mysrv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
  for (const auto& r : q) {
    level_code_map.emplace(stoi(r[0]), make_tuple(r[1], r[2], r[3]));
  }
  mysrv.disconnect();
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
  if (mysystem2("/bin/mkdir -p " + t.name() + "/metadata", oss, ess) != 0) {
    log_error2("unable to create temporary directory tree", F, caller, user);
  }
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  MySQL::LocalQuery q("select distinct parameter from WGrML.summary as s left "
      "join WGrML.formats as f on f.code = s.format_code where s.dsid = '" +
      metautils::args.dsnum + "' and f.format = '" + format + "'");
  if (q.submit(mysrv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
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
    ofstream ofs((t.name() + "/metadata/" + html_file).c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open html file for output", F, caller, user);
    }
    ofstream ofs2((t.name() + "/metadata/" + substitute(html_file, ".html",
        ".xml")).c_str());
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
    if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/ds" +
        metautils::args.dsnum, metautils::directives.rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() couldn't "
          "sync cross-references - rdadata_sync error(s): '" + e  + "'", caller,
          user);
    }
  } else {

    // remove a parameter table if it exists and there are no parameters for
    //  this format
    string e;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
        dsnum + "/metadata/" + html_file, t.name(), metautils::directives.
        rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() tried to "
          "but couldn't delete '" + html_file + "' - error: '" + e + "'",
          caller, user);
    }
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
        dsnum + "/metadata/" + substitute(html_file, ".html", ".xml"), t.name(),
        metautils::directives.rdadata_home, e) < 0) {
      metautils::log_warning("generate_parameter_cross_reference() tried to "
          "but couldn't delete '" + substitute(html_file, ".html", ".xml") +
          "' - error: '" + e + "'", caller, user);
    }
  }
  mysrv.disconnect();
}

void generate_level_cross_reference(string format, string title, string
     html_file, string caller, string user) {
  static const string F = this_function_label(__func__);
  unordered_map<size_t, tuple<string, string, string>> lcmap;
  fill_level_code_table("WGrML", lcmap);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  MySQL::LocalQuery q("select distinct levelType_codes from WGrML.summary as s "
      "left join WGrML.formats as f on f.code = s.format_code where s.dsid = '"
      + metautils::args.dsnum + "' and f.format = '" + format + "'");
  if (q.submit(mysrv) < 0) {
    myerror = move(q.error());
    exit(1);
  }
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
    if (mysystem2("/bin/mkdir -p " + t.name() + "/metadata", oss, ess) != 0) {
      log_error2("unable to create temporary directory tree", F, caller, user);
    }
    ofstream ofs((t.name() + "/metadata/" + html_file).c_str());
    if (!ofs.is_open()) {
      log_error2("unable to open html file for output", F, caller, user);
    }
    ofs << "<style id=\"detail\">" << endl;
    ofs << "  @import url(/css/transform.css);" << endl;
    ofs << "</style>" << endl;
    auto n = 0;
    if (lv.size() > 0) {
      ofs << "<p>The following " << lv.size() << " " << strutils::to_capital(
          format) << " levels are included in this dataset:<center><table "
          "class=\"insert\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\">"
          "<tr class=\"bg2\" valign=\"bottom\"><th align=\"center\">Code</th>"
          "<th align=\"left\">Description</th><th align=\"center\">Units</th>"
          "</tr>" << endl;
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
      ofs << "<p>The following " << yv.size() << " " << strutils::to_capital(
          format) << " layers are included in this dataset:<center><table "
          "class=\"insert\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\">"
          "<tr class=\"bg2\" valign=\"bottom\"><th align=\"center\">Code<br />"
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
    if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/ds" +
        metautils::args.dsnum, metautils::directives.rdadata_home, e) < 0)
      metautils::log_warning("generate_level_cross_reference() couldn't sync '"
          + html_file + "' - rdadata_sync error(s): '" + e + "'", caller, user);
  } else {

    // remove the level table if it exists and there are no levels for this
    //  format
    string e;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
        dsnum + "/metadata/" + html_file, t.name(), metautils::directives.
        rdadata_home, e) < 0) {
      metautils::log_warning("generate_level_cross_reference() tried to but "
          "couldn't delete '" + html_file + "' - error: '" + e + "'", caller,
          user);
    }
  }
  mysrv.disconnect();
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
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!mysrv) {
    log_error2("unable to connect to metadata server", F, caller, user);
  }
  MySQL::LocalQuery q("select distinct f.format, f.code from " + database +
      ".formats as f left join " + primaries_table + " as p on p.format_code = "
      "f.code where !isnull(p.format_code)");
  if (q.submit(mysrv) < 0) {
    log_error2("'" + q.error() + "'", F, caller, user);
  }
  for (const auto& r : q) {
    format_list.emplace_back(make_pair(r[0], r[1]));
    auto e = xdoc.element("formatReferences/format@name=" + r[0]);
    auto v = e.attribute_value("href");
    formats += "<li>";
    if (!v.empty()) {
      formats += "<a target=\"_format\" href=\"" + v + "\">";
    }
    formats += strutils::to_capital(substitute(r[0], "proprietary_",
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
}

string parameter_query(MySQL::Server& mysrv, const DBData& dbdata, string
    format_code, string group_index, size_t format_query_result_size) {
  auto d2 = substitute(metautils::args.dsnum, ".", "");
  if (!group_index.empty()) {
    if (format_query_result_size > 1) {
      if (field_exists(mysrv, dbdata.db + ".ds" + d2 + dbdata.table, "dsid")) {
        return "select a.parameter, a.levelType_codes, min(a.start_date), max("
            "a.end_date) from " + dbdata.db + ".ds" + d2 + dbdata.table + " as "
            "p left join dssdb." + dbdata.dtable + " as x on (x." + dbdata.
            dtable + " = p." + dbdata.ID_type + "ID and x.type = p.type and x."
            "dsid = p.dsid) left join " + dbdata.db + ".ds" + d2 + "_agrids as "
            "a on a." + dbdata.ID_type + "ID_code = p.code where p.format_code "
            "= " + format_code + " and !isnull(x." + dbdata.dtable + ") and x."
            "gindex = " + group_index + " group by a.parameter, a."
            "levelType_codes";
      } else {
        return "select a.parameter, a.levelType_codes, min(a.start_date), max("
            "a.end_date) from " + dbdata.db + ".ds" + d2 + dbdata.table + " as "
            "p left join dssdb." + dbdata.dtable + " as x on x." + dbdata.dtable
            + " = p." + dbdata.ID_type + "ID left join " + dbdata.db + ".ds" +
            d2 + "_agrids as a on a." + dbdata.ID_type + "ID_code = p.code "
            "where p.format_code = " + format_code + " and !isnull(x." + dbdata.
            dtable + ") and x.gindex = " + group_index + " group by a."
            "parameter, a.levelType_codes";
      }
    } else {
      if (field_exists(mysrv, dbdata.db + ".ds" + d2 + dbdata.table, "dsid")) {
        return "select a.parameter, a.levelType_codes, min(a.start_date), max("
            "a.end_date) from " + dbdata.db + ".ds" + d2 + dbdata.table + " as "
            "p left join dssdb." + dbdata.dtable + " as x on (x." + dbdata.
            dtable + " = p." + dbdata.ID_type + "ID and x.type = p.type and x."
            "dsid = p.dsid) left join " + dbdata.db + ".ds" + d2 + "_agrids as "
            "a on a." + dbdata.ID_type + "ID_code = p.code where !isnull(x." +
            dbdata.dtable + ") and x.gindex = " + group_index + " group by a."
            "parameter, a.levelType_codes";
      } else {
        return "select a.parameter, a.levelType_codes, min(a.start_date), max("
            "a.end_date) from " + dbdata.db + ".ds" + d2 + dbdata.table + " as "
            "p left join dssdb." + dbdata.dtable + " as x on x." + dbdata.dtable
            + " = p." + dbdata.ID_type + "ID left join " + dbdata.db + ".ds" +
            d2 + "_agrids as a on a." + dbdata.ID_type + "ID_code = p.code "
            "where !isnull(x." + dbdata.dtable + ") and x.gindex = " +
            group_index + " group by a.parameter, a.levelType_codes";
      }
    }
  } else {
    if (format_query_result_size > 1) {
      return "select a.parameter, a.levelType_codes,min(a.start_date),max(a."
          "end_date) from " + dbdata.db + ".ds" + d2 + "_agrids as a left join "
          + dbdata.db + ".ds" + d2 + dbdata.table + " as p on p.code = a." +
          dbdata.ID_type + "ID_code where p.format_code = " + format_code +
          " group by a.parameter, a.levelType_codes";
    } else {
      return "select parameter, levelType_codes, min(start_date), max("
          "end_date) from " + dbdata.db + ".ds" + d2 + "_agrids group by "
          "parameter, levelType_codes";
    }
  }
}

string time_range_query(MySQL::Server& mysrv, const DBData& dbdata, string
    format_code, string group_index) {
  auto d2 = substitute(metautils::args.dsnum, ".", "");
  if (group_index.empty()) {
    return "select distinct t.timeRange, d.definition, d.defParams from " +
        dbdata.db + ".summary as s left join " + dbdata.db + ".timeRanges as t "
        "on t.code = s.timeRange_code left join " + dbdata.db +
        ".gridDefinitions as d on d.code = s.gridDefinition_code where s.dsid "
        "= '" + metautils::args.dsnum + "' and format_code = " + format_code;
  } else {
    if (field_exists(mysrv, dbdata.db + ".ds" + d2 + dbdata.table, "dsid")) {
      return "select distinct t.timeRange, d.definition, d.defParams from " +
          dbdata.db + ".ds" + d2 + dbdata.table + " as p left join dssdb." +
          dbdata.dtable + " as x on (x." + dbdata.dtable + " = p." + dbdata.
          ID_type + "ID and x.type = p.type and x.dsid = p.dsid) left join " +
          dbdata.db + ".ds" + d2 + "_grids as g on g." + dbdata.ID_type +
          "ID_code = p.code left join " + dbdata.db + ".timeRanges as t on t."
          "code = g.timeRange_code left join " + dbdata.db + ".gridDefinitions "
          "as d on d.code = g.gridDefinition_code where p.format_code = " +
          format_code + " and !isnull(x." + dbdata.dtable + ") and x.gindex = "
          + group_index;
    } else {
      return "select distinct t.timeRange, d.definition, d.defParams from ("
          "select p.code from " + dbdata.db + ".ds" + d2 + dbdata.table + " as "
          "p left join dssdb." + dbdata.dtable + " as x on x." + dbdata.dtable +
          " = p." + dbdata.ID_type + "ID where p.format_code = " + format_code +
          " and !isnull(x." + dbdata.dtable + ") and x.gindex = " + group_index
          + ") as p left join " + dbdata.db + ".ds" + d2 + "_grids as g on g." +
          dbdata.ID_type + "ID_code = p.code left join " + dbdata.db +
          ".timeRanges as t on t.code = g.timeRange_code left join " + dbdata.
          db + ".gridDefinitions as d on d.code = g.gridDefinition_code";
    }
  }
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
  ofs << "<script id = \"view_script\">" << endl;
  ofs << "function changeView(e,v) {" << endl;
  ofs << "  if (e.className == 'view_button_off') {" << endl;
  ofs << "    var x = document.getElementsByClassName('view_button_on');" <<
      endl;
  ofs << "    for (n = 0; n < x.length; ++n) {" << endl;
  ofs << "        x[n].className = 'view_button_off';" << endl;
  ofs << "    }" << endl;
  ofs << "    e.className = 'view_button_on';" << endl;
  ofs << "    getAjaxContent('GET',null,'/datasets/ds" << metautils::args.dsnum
      << "/metadata/'+v,'detail_content');" << endl;
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
      "\"getAjaxContent('GET',null,'/datasets/ds" << metautils::args.dsnum <<
      "/metadata/<?php echo $view; ?>-detail.html','detail_content')\" />" <<
      endl;
}

void generate_gridded_product_detail(MySQL::Server& mysrv, const DBData& dbdata,
    string file_type, string group_index, const vector<pair<string, string>>&
    format_list, TempDir& tdir, string caller, string user) {
  static const string F = this_function_label(__func__);
  MySQL::LocalQuery q;
  q.set("timeRange, code", dbdata.db + ".timeRanges");
  if (q.submit(mysrv) < 0) {
    log_error2("unable to build the product hash", F, caller, user);
  }
  unordered_map<size_t, string> trmap;
  for (const auto& r : q) {
    trmap.emplace(stoi(r[1]), r[0]);
  }
  q.set("definition, defParams, code", dbdata.db + ".gridDefinitions");
  if (q.submit(mysrv) < 0) {
    log_error2("unable to build the grid definition hash", F, caller, user);
  }
  unordered_map<size_t, string> gdmap;
  for (const auto& r : q) {
    gdmap.emplace(stoi(r[2]), gridutils::convert_grid_definition(r[0] + "<!>" +
        r[1]));
  }
  ofstream ofs((tdir.name() + "/metadata/product-detail.html").c_str());
  if (!ofs.is_open()) {
    log_error2("unable to open output for product detail", F, caller, user);
  }
  for (const auto& fp : format_list) {
    if (!group_index.empty()) {
      q.set("");
    } else {
      auto d2 = substitute(metautils::args.dsnum, ".", "");
      if (format_list.size() > 1) {
        q.set("select a.timeRange_codes, a.gridDefinition_codes, min(a."
            "start_date), max(a.end_date) from " + dbdata.db + ".ds" + d2 +
            "_agrids as a left join " + dbdata.db + ".ds" + d2 + dbdata.table +
            " as p on p.code = a." + dbdata.ID_type + "ID_code where p."
            "format_code = " + fp.second + " group by a.timeRange_codes, a."
            "gridDefinition_codes");
      } else {
        q.set("select timeRange_codes, gridDefinition_codes, min(start_date), "
            "max(end_date) from " + dbdata.db + ".ds" + d2 + "_agrids group by "            "timeRange_codes, gridDefinition_codes");
      }
    }
    if (q.submit(mysrv) < 0) {
      log_error2("unable to build the product summary", F, caller, user);
    }
    unordered_map<size_t, ProductSummary> psmap;
    for (const auto& r : q) {
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
              "_anchor')\">" << strutils::to_capital(f) << "</a> Summary</span>"
              "<br>" << endl;
        }
      }
      ofs << "</td></tr></table>" << endl;
    }
    auto f = fp.first;
    replace_all(f, "proprietary", "dataset-specific");
    ofs << "<table class=\"insert\" width=\"100%\" cellspacing=\"1\" "
        "cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan="
        "\"2\" align=\"center\">Summary for Grids in " << strutils::to_capital(
        f) << " Format</th></tr>" << endl;
    if (group_index.empty()) {
      q.set("select distinct parameter, levelType_codes from " + dbdata.db +
          ".summary where dsid = '" + metautils::args.dsnum + "' and "
          "format_code = " + fp.second);
    }
    if (q.submit(mysrv) < 0) {
      log_error2("'" + q.error() + "'", F, caller, user);
    }
    ofs << "<tr class=\"bg0\"><td align=\"left\" colspan=\"2\"><b>Parameter "
        "and Level Information:</b><br />";
    if (q.num_rows() > 1) {
      ofs << "There are multiple parameters and/or vertical levels for the "
          "grids in this format.  Click a product description to see the "
          "available parameters and levels for that product.";
    } else {
      MySQL::Row row;
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
            "void(0)\" onClick=\"popModalWindowWithURL('/cgi-bin/transform?"
            "dsnum=" << metautils::args.dsnum << "&view=varlev&formatCode=" <<
            fp.second << "&ftype=" << strutils::to_lower(file_type);
        if (!group_index.empty()) {
          ofs << "&gindex=" << group_index;
        }
        ofs << "&tcode=" << ps.code << "', 950, 600)\">" << ps.description <<
            "</a>";
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

void generate_detailed_grid_summary(string file_type, string group_index,
    ofstream& ofs, const vector<pair<string, string>>& format_list, string
    caller, string user) {
  static const string F = this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }

  // create the directory tree in the temp directory
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + t.name() + "/metadata", oss, ess) != 0) {
    log_error2("unable to create temporary directory tree - '" + ess.str() +
        "'", F, caller, user);
  }
  ofstream ofs_p((t.name() + "/metadata/parameter-detail.html").c_str());
  if (!ofs_p.is_open()) {
    log_error2("unable to open output for parameter detail", F, caller, user);
  }
  ofstream ofs_l((t.name() + "/metadata/level-detail.html").c_str());
  if (!ofs_l.is_open()) {
    log_error2("unable to open output for level detail", F, caller, user);
  }
  DBData dbdata;
  if (file_type == "Web") {
    dbdata.db = "WGrML";
    dbdata.table = "_webfiles2";
    dbdata.dtable = "wfile";
    dbdata.ID_type = "web";
  }
  auto d2 = substitute(metautils::args.dsnum, ".", "");
  xmlutils::ParameterMapper pmap(metautils::directives.parameter_map_path);
  xmlutils::LevelMapper lmap(metautils::directives.level_map_path);
  vector<string> pfv;
  unordered_map<string, ParameterData> pdmap;
  unordered_map<size_t, LevelSummary> lsmap;
  unordered_map<size_t, tuple<string, string, string>> lcmap;
  fill_level_code_table(dbdata.db, lcmap);
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  MySQL::LocalQuery q;
  for (const auto& fp : format_list) {
    q.set(parameter_query(mysrv, dbdata, fp.second, group_index, format_list.
        size()));
    if (q.submit(mysrv) < 0) {
      log_error2("'" + q.error() + "'", F, caller, user);
    }
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
              "_anchor')\">" << strutils::to_capital(f) << "</a> Summary</span>"
              "<br>" << endl;
          ofs_l << "<span class=\"paneltext\">Go to <a href=\"javascript:void("
              "0)\" onClick=\"javascript:scrollTo('" << e.first <<
              "_anchor')\">" << strutils::to_capital(f) << "</a> Summary</span>"
              "<br>" << endl;
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
    MySQL::LocalQuery q(time_range_query(mysrv, dbdata, fp.second,
        group_index));
    if (q.submit(mysrv) < 0) {
      log_error2("'" + q.error() + "'", F, caller, user);
    }
    ofs_p << "<tr class=\"bg0\"><td align=\"left\" colspan=\"" << ncols <<
        "\"><b>Product and Coverage Information:</b><br>";
    ofs_l << "<tr class=\"bg0\"><td align=\"left\" colspan=\"" << ncols <<
        "\"><b>Product and Coverage Information:</b><br>";
    if (q.num_rows() == 1) {
      MySQL::Row row;
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
            "0)\" onClick=\"popModalWindowWithURL('/cgi-bin/transform?dsnum=" <<
            metautils::args.dsnum << "&view=prodcov&formatCode=" << fp.second <<
            "&ftype=" << strutils::to_lower(file_type);
        if (!group_index.empty()) {
          ofs_p << "&gindex=" << group_index;
        }
        for (const auto& e : pdmap[k].parameter_codes) {
          ofs_p << "&pcode=" << e;
        }
        ofs_p << "', 950, 600)\">" << kk << "</a>";
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
      ofs_p << "<table style=\"font-size: 14px\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\">";
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
            "0)\" onClick=\"popModalWindowWithURL('/cgi-bin/transform?dsnum=" <<
            metautils::args.dsnum << "&view=prodcov&formatCode=" << fp.second <<
            "&ftype=" << strutils::to_lower(file_type);
        if (!group_index.empty()) {
          ofs_l << "&gindex=" << group_index;
        }
        for (auto& e : clmap[key].map_list) {
          ofs_l << "&map=" << e;
        }
        ofs_l << "', 950, 600)\">" << key << "</a>";
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
    generate_gridded_product_detail(mysrv, dbdata, file_type, group_index,
        format_list, t, caller, user);
  }
  string e;
  if (unixutils::rdadata_sync(t.name(), "metadata/", "/data/web/datasets/ds" +
      metautils::args.dsnum, metautils::directives.rdadata_home, e) < 0) {
    metautils::log_warning("generate_detailed_grid_summary() couldn't sync "
        "detail files - rdadata_sync error(s): '" + e + "'", caller, user);
  }
  mysrv.disconnect();
  write_grid_html(ofs, pfv.size());
}

void generate_detailed_observation_summary(string file_type, string group_index,
    ofstream& ofs, const vector<pair<string, string>>& format_list, string
    caller, string user) {
  static const string F = this_function_label(__func__);
  MySQL::Query query;
  TypeEntry te,te2;
  xmlutils::DataTypeMapper data_type_mapper(metautils::directives.parameter_map_path);

  DBData dbdata;
  if (file_type == "Web") {
    dbdata.db = "WObML";
    dbdata.table = "_webfiles2";
    dbdata.dtable = "wfile";
    dbdata.ID_type = "web";
  }
  MySQL::Server mysrv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  string d2 = substitute(metautils::args.dsnum, ".", "");
  my::map<TypeEntry> platform_table;
  for (const auto& fp : format_list) {
    platform_table.clear();
    auto data_format=fp.first;
    ofs << "<br><center><table class=\"insert\" width=\"95%\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan=\"4\" align=\"center\">Summary for Platform Observations in " << strutils::to_capital(substitute(data_format,"proprietary_","")) << " Format</th></tr>" << endl;
    auto cindex=0;
    ofs << "<tr class=\"bg2\" valign=\"top\"><th align=\"left\">Observing Platform</th><th align=\"left\">Observation Type<ul class=\"paneltext\"><li>Data Types</li></ul></th><td align=\"left\"><b>Average Frequency of Data</b><br>(varies by individual platform ID and data type)</td><td align=\"left\"><b>Temporal/Geographical Coverage</b><br>(each dot represents a 3&deg; box containing one or more observations)</td></tr>" << endl;
    if (group_index.empty()) {
      if (MySQL::table_exists(mysrv,dbdata.db+".ds"+d2+"_dataTypes2")) {
        query.set("select distinct platformType,obsType,dataType,min(start_date),max(end_date) from "+dbdata.db+".ds"+d2+"_dataTypes2 as d left join "+dbdata.db+".ds"+d2+"_dataTypesList as l on l.code = d.dataType_code left join "+dbdata.db+".ds"+d2+dbdata.table+" as p on p.code = d."+dbdata.ID_type+"ID_code left join "+dbdata.db+".platformTypes as t on t.code = l.platformType_code left join "+dbdata.db+".obsTypes as o on o.code = l.observationType_code where p.format_code = "+fp.second+" group by platformType,obsType,dataType order by platformType,obsType,dataType");
      }
      else {
        query.set("select distinct platformType,obsType,dataType,min(start_date),max(end_date) from "+dbdata.db+".ds"+d2+"_dataTypes as d left join "+dbdata.db+".ds"+d2+dbdata.table+" as p on p.code = d."+dbdata.ID_type+"ID_code left join "+dbdata.db+".platformTypes as t on t.code = d.platformType_code left join "+dbdata.db+".obsTypes as o on o.code = d.observationType_code where p.format_code = "+fp.second+" group by platformType,obsType,dataType order by platformType,obsType,dataType");
      }
    }
    else {
      if (MySQL::table_exists(mysrv,dbdata.db+".ds"+d2+"_dataTypes2")) {
        query.set("select distinct platformType,obsType,dataType,min(start_date),max(end_date) from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_dataTypes2 as d on d."+dbdata.ID_type+"ID_code = p.code left join "+dbdata.db+".ds"+d2+"_dataTypesList as l on l.code = d.dataType_code left join "+dbdata.db+".platformTypes as t on t.code = l.platformType_code left join "+dbdata.db+".obsTypes as o on o.code = l.observationType_code where x.gindex = "+group_index+" and p.format_code = "+fp.second+" group by platformType,obsType,dataType order by platformType,obsType,dataType");
      }
      else {
        query.set("select distinct platformType,obsType,dataType,min(start_date),max(end_date) from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_dataTypes as d on d."+dbdata.ID_type+"ID_code = p.code left join "+dbdata.db+".platformTypes as t on t.code = d.platformType_code left join "+dbdata.db+".obsTypes as o on o.code = d.observationType_code where x.gindex = "+group_index+" and p.format_code = "+fp.second+" group by platformType,obsType,dataType order by platformType,obsType,dataType");
      }
    }
    if (query.submit(mysrv) < 0) {
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    }
    for (const auto& res : query) {
      if (!platform_table.found(res[0],te)) {
// te is the entry for an individual platform; te.type_table_keys is the list of
//   corresponding observation types
        te.key=res[0];
        te.type_table.reset(new my::map<TypeEntry>);
        te.type_table_keys.reset(new list<string>);
// te2 is the entry for an individual observation type; te2.type_table_keys is
//   the list of corresponding dataTypes (don't need te2.type_table for
//   uniqueness because query guarantees each dataType will be unique
        te2.key=res[1];
        te2.start=res[3];
        te2.end=res[4];
        te2.type_table_keys.reset(new list<string>);
        te2.type_table_keys->emplace_back(res[2]);
        te2.type_table_keys_2=nullptr;
        te.type_table->insert(te2);
        te.type_table_keys->emplace_back(te2.key);
        platform_table.insert(te);
      }
      else {
        if (te.type_table->found(res[1],te2)) {
          if (res[3] < te2.start) {
            te2.start=res[3];
          }
          if (res[4] > te2.end) {
            te2.end=res[4];
          }
          te2.type_table_keys->emplace_back(res[2]);
          te.type_table->replace(te2);
        }
        else {
          te2.key=res[1];
          te2.start=res[3];
          te2.end=res[4];
          te2.type_table_keys.reset(new list<string>);
          te2.type_table_keys->emplace_back(res[2]);
          te2.type_table_keys_2=nullptr;
          te.type_table->insert(te2);
          te.type_table_keys->emplace_back(te2.key);
        }
        platform_table.replace(te);
      }
    }
    if (group_index.empty()) {
      if (field_exists(mysrv,dbdata.db+".ds"+d2+dbdata.table,"min_obs_per")) {
        query.set("select any_value(l.platformType),any_value(o.obsType),min(f.min_obs_per),max(f.max_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".obsTypes as o on o.code = f.observationType_code left join "+dbdata.db+".platformTypes as l on l.code = f.platformType_code left join ObML.frequency_sort as s on s.keyword = f.unit where p.format_code = "+fp.second+" group by f.observationType_code,f.platformType_code,f.unit,s.idx order by s.idx");
      }
      else {
        query.set("select any_value(l.platformType),any_value(o.obsType),min(f.avg_obs_per),max(f.avg_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".obsTypes as o on o.code = f.observationType_code left join "+dbdata.db+".platformTypes as l on l.code = f.platformType_code left join ObML.frequency_sort as s on s.keyword = f.unit where p.format_code = "+fp.second+" group by f.observationType_code,f.platformType_code,f.unit,s.idx order by s.idx");
      }
    }
    else {
      if (field_exists(mysrv,dbdata.db+".ds"+d2+dbdata.table,"min_obs_per")) {
        query.set("select any_value(l.platformType),any_value(o.obsType),min(f.min_obs_per),max(f.max_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".obsTypes as o on o.code = f.observationType_code left join "+dbdata.db+".platformTypes as l on l.code = f.platformType_code left join ObML.frequency_sort as s on s.keyword = f.unit where x.gindex = "+group_index+" and p.format_code = "+fp.second+" group by f.observationType_code,f.platformType_code,f.unit,s.idx order by s.idx");
      }
      else {
        query.set("select any_value(l.platformType),any_value(o.obsType),min(f.avg_obs_per),max(f.avg_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".obsTypes as o on o.code = f.observationType_code left join "+dbdata.db+".platformTypes as l on l.code = f.platformType_code left join ObML.frequency_sort as s on s.keyword = f.unit where x.gindex = "+group_index+" and p.format_code = "+fp.second+" group by f.observationType_code,f.platformType_code,f.unit,s.idx order by s.idx");
      }
    }
    if (query.submit(mysrv) < 0)
      log_error2("'" + query.error() + "' for '" + query.show() + "'", F,
          caller, user);
    for (const auto& res : query) {
      if (platform_table.found(res[0],te)) {
        te.type_table->found(res[1],te2);
        if (te2.type_table_keys_2 == NULL) {
          te2.type_table_keys_2.reset(new list<string>);
        }
        if (res[2] == res[3]) {
          te2.type_table_keys_2->emplace_back("<nobr>"+res[2]+" per "+res[4]+"</nobr>");
        }
        else {
          te2.type_table_keys_2->emplace_back("<nobr>"+res[2]+" to "+res[3]+"</nobr> per "+res[4]);
        }
        te.type_table->replace(te2);
        platform_table.replace(te);
      }
    }
    for (const auto& key : platform_table.keys()) {
      unordered_set<string> unique_data_types;
      platform_table.found(key,te);
      auto n=0;
      for (const auto& key2 : *te.type_table_keys) {
        if (n == 0) {
          ofs << "<tr class=\"bg" << cindex << "\" valign=\"top\"><td rowspan=\"" << te.type_table->size() << "\">" << strutils::to_capital(te.key) << "</td>";
        }
        else {
          ofs << "<tr class=\"bg" << cindex << "\" valign=\"top\">";
        }
        te2.key=key2;
        te.type_table->found(te2.key,te2);
        ofs << "<td>" << strutils::to_capital(te2.key) << "<ul class=\"paneltext\">";
        for (const auto& key3 : *te2.type_table_keys) {
          auto key = metatranslations::detailed_datatype(data_type_mapper,
              fp.first, key3);
          if (unique_data_types.find(key) == unique_data_types.end()) {
            ofs << "<li>" << key << "</li>";
            unique_data_types.insert(key);
          }
        }
        ofs << "</ul></td><td><ul class=\"paneltext\">";
        if (te2.type_table_keys_2 != NULL) {
          for (const auto& key3 : *te2.type_table_keys_2) {
            ofs << "<li>" << key3 << "</li>";
          }
        }
        ofs << "</ul></td><td align=\"center\">" << dateutils::string_ll_to_date_string(te2.start) << " to " << dateutils::string_ll_to_date_string(te2.end);
        if (group_index.empty() && unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+te2.key+"."+te.key+".kml",metautils::directives.rdadata_home)) {
          ofs << "&nbsp;<a href=\"/datasets/ds" << metautils::args.dsnum << "/metadata/" << te2.key << "." << te.key << ".kml\"><img src=\"/images/kml.gif\" width=\"36\" height=\"14\" hspace=\"3\" border=\"0\" title=\"See stations plotted in Google Earth\"></a>";
        }
        ofs << "<br><img src=\"/datasets/ds" << metautils::args.dsnum << "/metadata/spatial_coverage." << substitute(data_format," ","_") << "_" << dbdata.ID_type;
        if (!group_index.empty()) {
          ofs << "_gindex_" << group_index;
        }
        ofs << "_" << key2 << "." << key << ".gif?" << strutils::strand(5) << "\"></td></tr>" << endl;
        te2.type_table_keys->clear();
        te2.type_table_keys.reset();
        ++n;
      }
      te.type_table->clear();
      te.type_table.reset();
      te.type_table_keys->clear();
      te.type_table_keys.reset();
      cindex=1-cindex;
    }
    ofs << "</table></center>" << endl;
  }
  mysrv.disconnect();
}

void generate_detailed_fix_summary(string file_type, string group_index,
    ofstream& ofs, const vector<pair<string, string>>& format_list, string
    caller, string user) {
  static const string F = this_function_label(__func__);
  DBData dbdata;
  if (file_type == "Web") {
    dbdata.db = "WFixML";
    dbdata.table = "_webfiles2";
    dbdata.dtable = "wfile";
    dbdata.ID_type = "web";
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  string d2 = substitute(metautils::args.dsnum, ".", "");
  my::map<TypeEntry> platform_table;
  for (const auto& fp : format_list) {
    platform_table.clear();
    auto format=fp.first;
    replace_all(format,"proprietary_","");
    ofs << "<br><center><table class=\"insert\" width=\"95%\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\"><tr><th class=\"headerRow\" colspan=\"3\" align=\"center\">Summary for Cyclone Fixes in " << strutils::to_capital(format) << " Format</th></tr>" << endl;
    auto cindex=0;
    ofs << "<tr class=\"bg2\" valign=\"top\"><th align=\"left\">Cyclone Classification</th><td align=\"left\"><b>Average Frequency of Data</b><br>(may vary by individual cyclone ID)</td><td align=\"left\"><b>Temporal/Geographical Coverage</b><br>(each dot represents a 3&deg; box containing one or more fixes)</td></tr>" << endl;
    MySQL::Query query;
    if (group_index.empty())
      query.set("select distinct classification,min(l.start_date),max(l.end_date) from "+dbdata.db+".ds"+d2+"_locations as l left join "+dbdata.db+".ds"+d2+dbdata.table+" as p on p.code = l."+dbdata.ID_type+"ID_code left join "+dbdata.db+".classifications as c on c.code = l.classification_code where p.format_code = "+fp.second+" group by classification order by classification");
    else
      query.set("select distinct classification,min(l.start_date),max(l.end_date) from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_locations as l on l."+dbdata.ID_type+"ID_code = p.code left join "+dbdata.db+".classifications as c on c.code = l.classification_code where x.gindex = "+group_index+" and p.format_code = "+fp.second+" group by classification order by classification");
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
    if (group_index.empty()) {
      query.set("select any_value(c.classification),min(f.min_obs_per),max(f.max_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".classifications as c on c.code = f.classification_code left join ObML.frequency_sort as s on s.keyword = f.unit where p.format_code = "+fp.second+" group by f.classification_code,f.unit,s.idx order by s.idx");
    }
    else {
      query.set("select any_value(c.classification),min(f.min_obs_per),max(f.max_obs_per),f.unit from "+dbdata.db+".ds"+d2+dbdata.table+" as p left join dssdb."+dbdata.dtable+" as x on x."+dbdata.dtable+" = p."+dbdata.ID_type+"ID left join "+dbdata.db+".ds"+d2+"_frequencies as f on p.code = f."+dbdata.ID_type+"ID_code left join "+dbdata.db+".classifications as c on c.code = f.classification_code left join ObML.frequency_sort as s on s.keyword = f.unit where x.gindex = "+group_index+" and  p.format_code = "+fp.second+" group by f.classification_code,f.unit,s.idx order by s.idx");
    }
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
      ofs << "<tr class=\"bg" << cindex << "\" valign=\"top\"><td>" << strutils::to_capital(te.key) << "</td><td><ul class=\"paneltext\">";
      for (auto& key2 : *te.type_table_keys) {
        ofs << "<li>" << key2 << "</li>";
      }
      ofs << "</ul></td><td align=\"center\">" << dateutils::string_ll_to_date_string(te.start) << " to " << dateutils::string_ll_to_date_string(te.end) << "<br><img src=\"/datasets/ds" << metautils::args.dsnum << "/metadata/spatial_coverage." << dbdata.ID_type;
      if (!group_index.empty()) {
        ofs << "_gindex_" << group_index;
      }
      ofs << "_" << te.key << ".gif?" << strutils::strand(5) << "\"></td></tr>" << endl;
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
  MySQL::Server mysrv_m(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  string fmts, dtyps;
  auto b = false;
  auto pnam = "webfiles2";

  // svec contains the database, the data type description, and a function
  //  that generates an appropriate summary
  vector<tuple<string, string, void(*)(string, string, ofstream&, const vector<
      pair<string, string>>&, string, string)>> svec{
    make_tuple("WGrML", "Grids", generate_detailed_grid_summary),
    make_tuple("WObML", "Platform Observations",
    generate_detailed_observation_summary),
    make_tuple("WFixML", "Cyclone Fix", generate_detailed_fix_summary),
  };
  vector<vector<pair<string, string>>> vv(svec.size());
  for (size_t n = 0; n < svec.size(); ++n) {
    auto s = get<0>(svec[n]) + ".ds" + substitute(metautils::args.dsnum, ".",
        "") + "_" + pnam;
    if (table_exists(mysrv_m, s)) {
      b = true;
      dtyps += "<li>" + get<1>(svec[n]) + " </li>";
      add_to_formats(xdoc, get<0>(svec[n]), s, vv[n], fmts, caller, user);
    }
  }
  xdoc.close();
  if (stat((metautils::directives.server_root+"/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
    f=metautils::directives.server_root+"/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml";
  } else {
    f=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml",t.name());
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
    MySQL::Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
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
          dtyps+=strutils::to_capital(data_type);
        }
        dtyps+="</li>";
      }
      elist=xdoc.element_list("dsOverview/contentMetadata/format");
      for (auto element : elist) {
        auto format=element.content();
        replace_all(format,"proprietary_","");
        fmts+="<li>"+strutils::to_capital(format)+"</li>";
      }
    }
// create the metadata directory tree in the temp directory
    stringstream oss,ess;
    if (mysystem2("/bin/mkdir -p "+t.name()+"/metadata",oss,ess) != 0) {
      metautils::log_error("generate_detailed_metadata_view(): unable to create metadata directory tree - '"+ess.str()+"'",caller,user);
    }
    ofstream ofs((t.name()+"/metadata/detailed.html").c_str());
    if (!ofs.is_open()) {
      metautils::log_error("generate_detailed_metadata_view(): unable to open output for detailed.html",caller,user);
    }
    ofs << "<style id=\"detail\">" << endl;
    ofs << "  @import url(/css/transform.css);" << endl;
    ofs << "</style>" << endl;
    ofs << "<?php" << endl;
    ofs << "  include_once(\"MyDBI.inc\");" << endl;
    ofs << "  default_dbinfo(\"\",\"metadata\",\"metadata\");" << endl;
    ofs << "  $title=myget(\"search.datasets\",\"title\",\"dsid = '" << metautils::args.dsnum << "'\");" << endl;
    ofs << "  $contributors=mymget(\"\",\"\",\"select g.path from search.contributors_new as c left join search.GCMD_providers as g on g.uuid = c.keyword where dsid = '" << metautils::args.dsnum << "' order by disp_order\");" << endl;
    ofs << "  $projects=mymget(\"\",\"\",\"select g.path from search.projects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword  where dsid = '" << metautils::args.dsnum << "'\");" << endl;
    ofs << "  $supportedProjects=mymget(\"\",\"\",\"select g.path from search.supportedProjects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword where dsid = '" << metautils::args.dsnum << "'\");" << endl;
    ofs << "?>" << endl;
    ofs << "<p>The information presented here summarizes the data in the primary (NCAR HPSS) archive of ds"+metautils::args.dsnum+".  Some or all of these data may not be directly accessible from our web server.  If you have questions about data access, please contact the dataset specialist named above.</p>" << endl;
    ofs << "<br>" << endl;
    ofs << "<center><table class=\"insert\" width=\"95%\" cellspacing=\"1\" cellpadding=\"5\" border=\"0\">" << endl;
    ofs << "<tr><th class=\"headerRow\" align=\"center\" colspan=\"2\">Overview</th></tr>" << endl;
    ofs << "<tr><td class=\"bg0\" align=\"left\" colspan=\"2\"><b>Dataset Title:</b>&nbsp;<?php print $title[\"title\"]; ?></td></tr>" << endl;
// citation
    ofs << "<tr><td class=\"bg0\" align=\"left\" colspan=\"2\"><b>Dataset Citation:</b><br /><div id=\"citation\" style=\"margin-bottom: 5px\"><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onLoad=\"getAjaxContent('GET',null,'/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsnum << "&style=esip','citation')\" /></div><div style=\"display: inline; background-color: #2a70ae; color: white; padding: 1px 8px 1px 8px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsnum << "&style=ris'\" title=\"download citation in RIS format\">RIS</div><div style=\"display: inline; background-color: #2a70ae; color: white; width: 60px; padding: 2px 8px 1px 8px; font-size: 16px; font-weight: bold; font-family: serif; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer; margin-left: 7px\" onClick=\"location='/cgi-bin/datasets/citation?dsnum=" << metautils::args.dsnum << "&style=bibtex'\" title=\"download citation in BibTeX format\">BibTeX</div></td></tr>" << endl;
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
    MySQL::LocalQuery query("primary_size","dataset","dsid = 'ds"+metautils::args.dsnum+"'");
    if (query.submit(server_d) < 0) {
      myerror = query.error();
      exit(1);
    }
    double volume;
    MySQL::Row row;
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
      if (vv[n].size() > 0) {
        auto& generate_summary=get<2>(svec[n]);
        generate_summary("Web", "", ofs, vv[n], caller, user);
      }
    }
    ofs.close();
    string error;
    if (unixutils::rdadata_sync(t.name(),"metadata/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
      metautils::log_warning("generate_detailed_metadata_view() couldn't sync 'detailed.html' - rdadata_sync error(s): '"+error+"'",caller,user);
    }
    xdoc.close();
    server_d.disconnect();
  }
  mysrv_m.disconnect();
// generate parameter cross-references
  generate_parameter_cross_reference("WMO_GRIB1","GRIB","grib.html",caller,user);
  generate_parameter_cross_reference("WMO_GRIB2","GRIB2","grib2.html",caller,user);
  generate_level_cross_reference("WMO_GRIB2","GRIB2","grib2_levels.html",caller,user);
  generate_parameter_cross_reference("NCEP_ON84","ON84","on84.html",caller,user);
}

void generate_group_detailed_metadata_view(string group_index,string file_type,string caller,string user)
{
  ofstream ofs;
  string dsnum2=substitute(metautils::args.dsnum,".","");
  MySQL::LocalQuery query,grml_query,obml_query,fixml_query;
  string gtitle,gsummary,output,error;
//  list<string> formatList;
//  bool foundDetail=false;

  MySQL::Server server_m(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
  if (!group_index.empty() || group_index == "0") {
    return;
  }
while (group_index != "0") {
if (server_d.update("dsgroup","meta_link = 'X'","dsid = 'ds"+metautils::args.dsnum+"' and gindex = "+group_index) < 0) {
metautils::log_warning("generate_group_detailed_metadata_view() returned warning: "+server_d.error()+" while trying to update dssdb.dsgroup",caller,user);
}
query.set("pindex","dsgroup","gindex = "+group_index);
if (query.submit(server_d) < 0) {
metautils::log_error("generate_group_detailed_metadata_view(): "+query.error(),caller,user);
}
MySQL::Row row;
if (query.fetch_row(row)) {
group_index=row[0];
}
else
group_index="0";
}
/*
  if (file_type == "Web") {
    query.set("title,wnote","dsgroup","dsid = 'ds"+dsnum+"' and gindex = "+group_index);
    if (table_exists(server_m,"WGrML.ds"+dsnum2+"_webfiles2")) {
      if (field_exists(server_m,"WGrML.ds"+dsnum2+"_webfiles2","dsid")) {
        grml_query.set("select distinct f.format,f.code from WGrML.formats as f left join WGrML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on (x.wfile = w.webID and x.type = w.type and x.dsid = w.dsid) where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      else {
        grml_query.set("select distinct f.format,f.code from WGrML.formats as f left join WGrML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on x.wfile = w.webID where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      if (grml_query.submit(server_m) < 0) {
        metautils::log_error("generate_group_detailed_metadata_view(): "+grml_query.error(),caller,user,args);
      }
    }
    if (table_exists(server_m,"WObML.ds"+dsnum2+"_webfiles2")) {
      if (field_exists(server_m,"WObML.ds"+dsnum2+"_webfiles2","dsid")) {
        obml_query.set("select distinct f.format,f.code from WObML.formats as f left join WObML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on (x.wfile = w.webID and x.type = w.type and x.dsid = w.dsid) where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      else {
        obml_query.set("select distinct f.format,f.code from WObML.formats as f left join WObML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on x.wfile = w.webID where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      if (obml_query.submit(server_m) < 0) {
        metautils::log_error("generate_group_detailed_metadata_view(): "+obml_query.error(),caller,user,args);
      }
    }
    if (table_exists(server_m,"WFixML.ds"+dsnum2+"_webfiles2")) {
      if (field_exists(server_m,"WObML.ds"+dsnum2+"_webfiles2","dsid")) {
        fixml_query.set("select distinct f.format,f.code from WFixML.formats as f left join WFixML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on (x.wfile = w.webID and x.type = w.type and x.dsid = w.dsid) where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      else {
        fixml_query.set("select distinct f.format,f.code from WFixML.formats as f left join WFixML.ds"+dsnum2+"_webfiles2 as w on f.code = w.format_code left join dssdb.wfile as x on x.wfile = w.webID where !isnull(w.format_code) and x.gindex = "+group_index);
      }
      if (fixml_query.submit(server_m) < 0) {
        metautils::log_error("generate_group_detailed_metadata_view(): "+fixml_query.error(),caller,user,args);
      }
    }
  }
  if (query.submit(server_d) < 0)
    metautils::log_error("generate_group_detailed_metadata_view(): "+query.error(),caller,user,args);
  if (!query.fetch_row(row)) {
    metautils::log_error("generate_group_detailed_metadata_view(): no entry in RDADB for group index "+group_index,caller,user,args);
  }
  gtitle=row[0];
  gsummary=row[1];
  ofs.open(tfile->name().toChar());
  ofs << "<html>" << endl;
  ofs << "<head>" << endl;
  ofs << "<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/transform.css\">" << endl;
  ofs << "</head>" << endl;
  ofs << "<body>" << endl;
  ofs << "<br><center><big>" << gtitle << "</big></center>" << endl;
  if (!gsummary.empty())
    ofs << "<center><table><tr><td align=\"left\">" << gsummary << "</td></tr></table></center>" << endl;
  if (grml_query.num_rows() > 0) {
    generate_detailed_grid_summary(dsnum,grml_query,file_type,group_index,ofs,formatList,caller,user,args);
    foundDetail=true;
  }
  if (obml_query.num_rows() > 0) {
    generate_detailed_observation_summary(dsnum,obml_query,file_type,group_index,ofs,formatList,caller,user,args);
    foundDetail=true;
  }
  if (fixml_query.num_rows() > 0) {
    generate_detailed_fix_summary(dsnum,fixml_query,file_type,group_index,ofs,formatList,caller,user,args);
    foundDetail=true;
  }
  ofs << "</body></html>" << endl;
  ofs.close();
  if (foundDetail) {
    if (hostSync(tfile->name(),"/__HOST__/web/datasets/ds"+dsnum+"/metadata/"+file_type.toLower()+"_detailed_gindex_"+group_index+".html",error) < 0)
      metautils::log_warning("generate_detailed_metadata_view() couldn't sync '"+file_type.toLower()+"_detailed_gindex_"+group_index+".html' - hostSync error(s): '"+error+"'",caller,user,argsString);
  } 
  if (foundDetail) {
    if (file_type == "MSS") {
      if (server_d.update("dsgroup","meta_link = if (meta_link = 'W' or meta_link = 'B','B','M')","dsid = 'ds"+dsnum+"' and gindex = "+group_index) < 0)
        metautils::log_warning("generate_group_detailed_metadata_view() returned warning: "+server_d.error(),caller,user,args);
    }
    else if (file_type == "Web") {
      if (server_d.update("dsgroup","meta_link = if (meta_link = 'M' or meta_link = 'B','B','W')","dsid = 'ds"+dsnum+"' and gindex = "+group_index) < 0)
        metautils::log_warning("generate_group_detailed_metadata_view() returned warning: "+server_d.error(),caller,user,args);
      while (group_index != "0") {
        query.set("pindex","dsgroup","gindex = "+group_index);
        if (query.submit(server_d) < 0)
          metautils::log_error("generate_group_detailed_metadata_view(): "+query.error(),caller,user,args);
        if (query.fetch_row(row)) {
          group_index=row[0];
        }
        else {
          group_index="0";
        }
        if (group_index != "0") {
          if (server_d.update("dsgroup","meta_link = 'X'","dsid = 'ds"+dsnum+"' and gindex = "+group_index) < 0)
            metautils::log_warning("generate_group_detailed_metadata_view() returned warning: "+server_d.error(),caller,user,args);
        }
      }
    }
  }
*/
  server_m.disconnect();
  server_d.disconnect();
}

} // end namespace gatherxml::detailedMetadata

} // end namespace gatherxml
