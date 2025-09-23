#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <regex>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <gatherxml.hpp>
#include <PostgreSQL.hpp>
#include <metadata.hpp>
#include <metahelpers.hpp>
#include <metadata_export.hpp>
#include <citation.hpp>
#include <strutils.hpp>
#include <gridutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <tempfile.hpp>
#include <search.hpp>
#include <tokendoc.hpp>

using namespace PostgreSQL;
using htmlutils::unicode_escape_to_html;
using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::get;
using std::make_tuple;
using std::map;
using std::ofstream;
using std::pair;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::sort;
using std::stof;
using std::stoi;
using std::string;
using std::stringstream;
using std::tie;
using std::to_string;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::capitalize;
using strutils::chop;
using strutils::ds_aliases;
using strutils::ftos;
using strutils::has_beginning;
using strutils::itos;
using strutils::ng_gdex_id;
using strutils::replace_all;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;
using strutils::to_capital;
using strutils::to_sql_tuple_string;
using strutils::to_upper;
using strutils::trim;
using unixutils::exists_on_server;
using unixutils::open_output;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";
auto env = getenv("USER");
extern const string USER = (env == nullptr) ? "unknown" : env;

TempDir temp_dir;
XMLDocument g_xdoc;
Server g_metadata_server, g_wagtail_server;
string g_ds_set, g_dataset_type;
bool no_dset_waf = false;

unordered_map<string, string> wagtail_db{
    { "citations", "dataset_citation_datasetcitationpage" },
    { "num_citations", "dataset_citation_datasetcitationpage" },
    { "abstract", "dataset_description_datasetdescriptionpage" },
    { "acknowledgement", "dataset_description_datasetdescriptionpage" },
    { "access_restrict", "dataset_description_datasetdescriptionpage" },
    { "contributors", "dataset_description_datasetdescriptionpage" },
    { "data_formats", "dataset_description_datasetdescriptionpage" },
    { "data_license", "dataset_description_datasetdescriptionpage" },
    { "data_types", "dataset_description_datasetdescriptionpage" },
    { "levels", "dataset_description_datasetdescriptionpage" },
    { "spatial_coverage", "dataset_description_datasetdescriptionpage" },
    { "dsdoi", "dataset_description_datasetdescriptionpage" },
    { "dslogo", "dataset_description_datasetdescriptionpage" },
    { "dstype", "dataset_description_datasetdescriptionpage" },
    { "dstitle", "dataset_description_datasetdescriptionpage" },
    { "publications", "dataset_description_datasetdescriptionpage" },
    { "related_dslist", "dataset_description_datasetdescriptionpage" },
    { "related_rsrc_list", "dataset_description_datasetdescriptionpage" },
    { "temporal", "dataset_description_datasetdescriptionpage" },
    { "temporal_freq", "dataset_description_datasetdescriptionpage" },
    { "update_freq", "dataset_description_datasetdescriptionpage" },
    { "usage_restrict", "dataset_description_datasetdescriptionpage" },
    { "variables", "dataset_description_datasetdescriptionpage" },
    { "volume", "dataset_description_datasetdescriptionpage" },
};

void update_wagtail(string column, string insert_value, string caller) {
  if (wagtail_db.find(column) == wagtail_db.end()) {
    log_error2("unknown wagtail column '" + column + "'", caller, "dsgen",
        USER);
  }
  if (g_wagtail_server.update("wagtail2." + wagtail_db[column], column + " = '" +
      sql_ready(insert_value) + "'", "dsid in " + g_ds_set) < 0) {
    log_error2("failed to update wagtail2." + wagtail_db[column] + " " + column +
        ": error '" + g_wagtail_server.error() + "'  insert value: '" +
        insert_value + "'", caller, "dsgen", USER);
  }
}

void sync_dataset_files(string tdir_name) {
  string p = "/data/web";
  if (g_dataset_type == "W") {
    p += "/internal";
  }
  p += "/datasets/" + metautils::args.dsid;
  string e;
  if (unixutils::gdex_upload_dir(tdir_name, ".", p, metautils::directives.
      gdex_upload_key, e) < 0) {
    metautils::log_error2("couldn't sync dataset files - rdadata_sync "
        "error(s): '" + e + "'", "main()", "dsgen", USER);
  }
}

void generate_index(string tdir_name) {
  static const string F = this_function_label(__func__);
/*
  ofstream ofs;
  if (g_dataset_type == "W") {
    open_output(ofs, tdir_name + "/test_index.html");
  } else {
    open_output(ofs, tdir_name + "/index.html");
  }
  if (!ofs.is_open()) {
    log_error2("unable to open output for 'index.html'", F, "dsgen", USER);
  }
  auto t = new TokenDocument("/glade/u/home/rdadata/share/templates/"
      "dsgen_index.tdoc");
  if (!*t) {
    delete t;
    t = new TokenDocument("/usr/local/dss/share/templates/dsgen_index.tdoc");
    if (!*t) {
      log_error2("index template not found or unavailable", F, "dsgen", USER);
    }
  }
  auto &tdoc = *t;
  tdoc.add_replacement("__DSNUM__", metautils::args.dsid);
*/
  if (g_dataset_type == "I") {
//    tdoc.add_if("__IS_INTERNAL_DATASET__");
  } else {
    auto parts = split(metautils::directives.web_server, ".");
    std::reverse(parts.begin(), parts.end());
    string identifier = "oai:" + strutils::join(vector<string>(parts.begin(),
        parts.end()), ".") + ":" + metautils::args.dsid;
    string xf;
    xf = unixutils::remote_web_file("https://" + metautils::directives.
        web_server + "/oai/?verb=GetRecord&metadataPrefix=native&identifier=" +
        identifier, temp_dir.name());
    if (xf.empty()) {
      log_error2("dsOverview.xml does not exist for " + metautils::args.dsid, F,
          "dsgen", USER);
    }
    if (!g_xdoc.open(xf)) {
      log_error2("unable to open dsOverview.xml for " + metautils::args.dsid +
          "; parse error: '" + g_xdoc.parse_error() + "'", F, "dsgen", USER);
    }
    stringstream ss;
    if (!metadataExport::export_to_dc_meta_tags(ss, metautils::args.dsid, 0)) {
      log_error2("unable to export DC meta tags: '" + myerror + "'", F, "dsgen",
          USER);
    }
//    tdoc.add_replacement("__DC_META_TAGS__", ss.str());
    auto e = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
        "title");
    auto ti = e.content();
//    tdoc.add_replacement("__TITLE__", ti);
    update_wagtail("dstitle", ti, F);
    TempDir d;
    if (!d.create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary JSON-LD directory", F, "dsgen",
          USER);
    }
    if (!metadataExport::export_to_json_ld(ss, metautils::args.dsid, g_xdoc,
        0)) {
      log_error2("unable to export JSON-LD metadata", F, "dsgen", USER);
    }
    std::ofstream ofs_j;
    open_output(ofs_j, d.name() + "/" + metautils::args.dsid + ".jsonld");
    if (!ofs_j.is_open()) {
      log_error2("unable to open output for JSON-LD", F, "dsgen", USER);
    }
    ofs_j << ss.str();
    ofs_j.close();
    string err;
    if (unixutils::gdex_upload_dir(d.name(), ".", "/data/web/jsonld",
        metautils::directives.gdex_upload_key, err) < 0) {
      metautils::log_error2("couldn't sync JSON-LD file - rdadata_sync "
          "error(s): '" + err + "'", F, "dsgen", USER);
    }
//    tdoc.add_replacement("__JSON_LD__", ss.str());
    e = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/logo");
    if (!e.content().empty()) {
      auto sp = split(e.content(), ".");
      auto sp2 = split(sp[sp.size() - 2], "_");
      auto w = stoi(sp2[sp2.size() - 2]);
      auto h = stoi(sp2[sp2.size() - 1]);
//      tdoc.add_replacement("__LOGO_IMAGE__", e.content());
//      tdoc.add_replacement("__LOGO_WIDTH__", itos(lroundf(w * 70. / h)));
      update_wagtail("dslogo", e.content(), F);
    } else {
//      tdoc.add_replacement("__LOGO_IMAGE__", "default_200_200.png");
//      tdoc.add_replacement("__LOGO_WIDTH__", "70");
    }
    LocalQuery query("upper(doi)", "dssdb.dsvrsn", "dsid in " + g_ds_set +
        " and status = 'A'");
    Row row;
    string ds;
    if (query.submit(g_metadata_server) == 0 && query.fetch_row(row) && !row[0].
        empty()) {
      ds = "&nbsp;|&nbsp;<span class=\"blue\">DOI: " + row[0] + "</span>";
      update_wagtail("dsdoi", row[0], F);
    }
//    tdoc.add_replacement("__DOI_SPAN__", ds);
    if (g_dataset_type == "D") {
//      tdoc.add_if("__IS_DEAD_DATASET__");
    }
  }
//  ofs << tdoc;
//  ofs.close();
//  delete t;
}

bool compare_strings(const string& left, const string& right) {
  if (left <= right) {
    return true;
  }
  return false;
}

bool compare_references(XMLElement& left, XMLElement& right) { 
  auto e = left.element("year");
  auto l = e.content();
  e = right.element("year");
  auto r = e.content();
  if (l > r) {
    return true;
  } else if (l < r) {
    return false;
  } else if (left.attribute_value("type") == "journal" && right.attribute_value(
      "type") == "journal" && left.element("periodical").content() == right.
      element("periodical").content() && left.element("periodical").
      attribute_value("pages") > right.element("periodical").attribute_value(
      "pages")) {
    return true;
  }
  return false;
}

bool compare_levels(const string& left, const string& right) {
  if (((left[0] >= '0' && left[0] <= '9') || (left[0] == '-' && left[1] >= '0'
      && left[1] <= '9')) && ((right[0] >= '0' && right[0] <= '9') || (right[0]
      == '-' && right[1] >= '0' && right[1] <= '9'))) {
    auto li = left.find(" ");
    string lv, lu;
    if (li != string::npos) {
      lv = left.substr(0, li);
      lu = left.substr(li + 1);
    } else {
      lv = left;
      lu = "";
    }
    auto ri = right.find(" ");
    string rv, ru;
    if (ri != string::npos) {
      rv = right.substr(0, ri);
      ru = right.substr(ri + 1);
    } else {
      rv = right;
      ru = "";
    }
    auto flv = stof(lv);
    auto frv = stof(rv);
    if (lu == ru) {
      if (lu == "mbar" || lu == "deg K") {
        if (flv >= frv) {
          return true;
        }
        return false;
      } else {
        if (flv <= frv) {
          return true;
        }
        return false;
      }
    } else {
      if (lu <= ru) {
        return true;
      }
      return false;
    }
  } else {
    if (left[0] >= '0' && left[0] <= '9') {
      return false;
    } else if (right[0] >= '0' && right[0] <= '9') {
      return true;
    } else if (left <= right) {
      return true;
    }
    return false;
  }
}

string stripped_citation(string c) {
  auto idx = c.find("target=\"_orcid\"");
  if (idx != string::npos) {
    replace_all(c, "</a>", "");
    while (idx != string::npos) {
      idx = c.find("target=\"_orcid\"");
      if (idx != string::npos) {
        auto idx2 = c.substr(0, idx).rfind("<a");
        if (idx2 != string::npos) {
          idx = c.find(">");
          if (idx != string::npos) {
            c = c.substr(0, idx2) + c.substr(idx+1);
          }
          idx = c.find("target=\"_orcid\"");
        }
      }
    }
  }
  return c;
}

bool compare_citations(const tuple<string, string>& left, const tuple<string,
    string>& right) {
  if (get<0>(left) > get<0>(right)) {
    return true;
  } else if (get<0>(left) < get<0>(right)) {
    return false;
  }
  return (stripped_citation(get<1>(left)) < stripped_citation(get<1>(right)));
}

string create_table_from_strings(vector<string> list, size_t max_columns, string
    color1, string color2) {
  stringstream ss;
  ss << "<table cellspacing=\"0\">" << endl;
  auto nr = list.size() / max_columns;
  if (list.size() % max_columns != 0) {
    ++nr;
    auto max_mod = list.size() % max_columns;
    for (int n = max_columns - 1; n > 1; --n) {
      if (list.size() / n <= nr) {
        if (list.size() % n == 0) {
          max_columns = n;
          break;
        } else if (list.size() % n > max_mod) {
          max_mod = list.size() % n;
          max_columns = n;
        }
      }
    }
  }
  nr = list.size() / max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++nr;
  }
  size_t mc1 = max_columns - 1;
  size_t n = 0, m = 0;
  for (const auto& i : list) {
    n = n % max_columns;
    if (n == 0) {
      ++m;
      ss << "<tr style=\"vertical-align: top\">" << endl;
    }
    ss << "<td class=\"";
    if (m == 1) {
      ss << "border-top ";
    }
    ss << "border-bottom border-left";
    if (n == mc1) {
      ss << " border-right";
    }
    ss << "\" style=\"padding: 5px 8px 5px 8px; text-align: left; background-"
        "color: ";
    if ( (m % 2) == 0) {
      ss << color1;
    } else {
      ss << color2;
    }
    ss << "; height: 18px;";
    if (m == 1 && n == 0) {
      if (nr > 1) {
        ss << " border-radius: 10px 0px 0px 0px";
      } else {
        if (list.size() == 1) {
          ss << " border-radius: 10px 10px 10px 10px";
        } else {
          ss << " border-radius: 10px 0px 0px 10px";
        }
      }
    } else if (m == 1 && n == mc1) {
      if (m == nr) {
        ss << " border-radius: 0px 10px 10px 0px";
      } else {
        ss << " border-radius: 0px 10px 0px 0px";
      }
    } else if (m == nr && n == 0) {
      ss << " border-radius: 0px 0px 0px 10px";
    } else if (m == nr) {
      if (nr > 1) {
        if (n == mc1) {
          ss << " border-radius: 0px 0px 10px 0px";
        }
      } else {
        if (n == list.size() - 1) {
          ss << " border-radius: 0px 10px 10px 0px";
        }
      }
    }
    ss << "\">" << i << "</td>" << endl;
    if (n == mc1) {
      ss << "</tr>" << endl;
    }
    ++n;
  }
  if (n != max_columns) {
    if (nr > 1) {
      for (size_t l = n; l < max_columns; ++l) {
        ss << "<td class=\"border-bottom border-left";
        if (l == mc1) {
          ss << " border-right";
        }
        ss << "\" style=\"background-color: ";
        if ( (m % 2) == 0) {
          ss << color1;
        } else {
          ss << color2;
        }
        ss << "; height: 18px;";
        if (l == mc1) {
          ss << " border-radius: 0px 0px 10px 0px";
        }
        ss << "\">&nbsp;</td>" << endl;
      }
    }
    ss << "</tr>";
  }
  ss << "</table>" << endl;
  return ss.str();
}

string text_field_from_element(const XMLElement& e) {
  auto s = e.to_string();
  if (!s.empty()) {
    replace_all(s, "<" + e.name() + ">", "");
    replace_all(s, "</" + e.name() + ">", "");
    replace_all(s, "&amp;", "&");
    trim(s);
/*
    auto i = s.find("<p");
    if (i == string::npos) {
      i = s.find("<P");
    }
    if (i == 0) {
      auto i2 = s.find(">", i);
      s.insert(i2, " style=\"margin: 0px; padding: 0px\"");
    }
*/
  }
  return s;
}

void insert_text_field(ofstream& ofs, const XMLElement& e, string
    section_title) {
  auto s = e.to_string();
  if (!s.empty()) {
    replace_all(s, "<" + e.name() + ">", "");
    replace_all(s, "</" + e.name() + ">", "");
    trim(s);
/*
    size_t i = s.find("<p");
    if (i == string::npos) {
      i = s.find("<P");
    }
    if (i == 0) {
      auto i2 = s.find(">", i);
      s.insert(i2, " style=\"margin: 0px; padding: 0px\"");
    }
*/
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">" <<
        section_title << ":</td><td>" << s << "</td></tr>" << endl;
  }
}

void add_abstract(TokenDocument& tdoc) {
  static const string F = this_function_label(__func__);
  auto abstract = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
      "summary").content();
  replace_all(abstract, "&lt;", "<");
  replace_all(abstract, "&gt;", ">");
  replace_all(abstract, "<![CDATA[", "");
  replace_all(abstract, "]]", "");
  tdoc.add_replacement("__ABSTRACT__", abstract);
  update_wagtail("abstract", abstract, F);
}

/*
void exit_if_dead_dataset(TokenDocument& tdoc, string tdir_name, std::ofstream&
    ofs) {
*/
void exit_if_dead_dataset(string tdir_name) {
  if (g_dataset_type == "D") {
/*
    tdoc.add_if("__IS_DEAD_DATASET__");
    ofs << tdoc;
    ofs.close();
*/
    sync_dataset_files(tdir_name);
    exit(0);
  }
}

void initialize(vector<string>& formats, vector<string>& data_types, bool&
    found_content_metadata) {
  static const string F = this_function_label(__func__);
  auto dblist = metautils::cmd_databases("dsgen", "x");
  if (dblist.size() == 0) {
    log_error2("empty CMD database list", F, "dsgen", USER);
  }
  for (const auto& db : dblist) {
    string nm, dt;
    std::tie(nm, dt) = db;
    if (nm[0] != 'V' && table_exists(g_metadata_server, nm + "." + metautils::
        args.dsid + "_webfiles2")) {
      LocalQuery q("select distinct format from \"" + nm + "\".formats as f "
          "left join \"" + nm + "\"." + metautils::args.dsid + "_webfiles2 as "
          "d on d.format_code = f.code where d.format_code is not null");
      if (q.submit(g_metadata_server) < 0) {
        log_error2("query: " + q.show() + " returned error: " + q.error(), F,
            "dsgen", USER);
      }
      for (const auto& r : q) {
        formats.emplace_back(r[0] + "<!>");
      }
      if (formats.size() > 0) {
        found_content_metadata = true; 
        if (!dt.empty()) {
          data_types.emplace_back(dt);
        }
      }
    }
  }
}

//void add_acknowledgement(TokenDocument& tdoc) {
void add_acknowledgement() {
  static const string F = this_function_label(__func__);
  auto ack = text_field_from_element(g_xdoc.element("OAI-PMH/GetRecord/record/"
      "metadata/dsOverview/acknowledgement"));
  if (!ack.empty()) {
/*
    tdoc.add_if("__HAS_ACKNOWLEDGEMENTS__");
    tdoc.add_replacement("__ACKNOWLEDGEMENT__", ack);
*/
  }
  update_wagtail("acknowledgement", ack, F);
}

void decode_missing_journal_pages(string journal_number, stringstream& ss,
    string& json) {
  if (journal_number == "0") {
    string s = "Submitted";
    ss << s;
    json += s;
  } else if (journal_number == "1") {
    string s = "Accepted";
    ss << s;
    json += s;
  } else if (journal_number == "2") {
    string s = "In Press";
    ss << s;
    json += s;
  }
}

void decode_non_missing_journal_pages(string pages, string journal_number,
    stringstream& ss, string& json) {
  string s = "<b>" + journal_number + "</b>, ";
  ss << s;
  json += s;
  if (has_beginning(pages, "AGU:")) {
    s = pages.substr(4);
    ss << s;
    json += s;
  } else {
    auto sp = split(pages, "-"); 
    if (sp.size() == 2 && sp[0] == sp[1]) {
      ss << sp[0];
      json += sp[0];
    } else {
      ss << pages;
      json += pages;
    }
  }
}

void add_journal_to_publication(const XMLElement& e, stringstream& ss, string&
    json) {
  static const string F = this_function_label(__func__);
  auto pd = e.element("periodical");
  auto url = e.element("url").content();
  auto ti = e.element("title").content();
  if (!url.empty()) {
    ss << "<a href=\"" << url << "\">" << ti << "</a>";
    json += "<a href=\\\"" + url + "\\\">" + ti + "</a>";
  } else {
    ss << ti;
    json += ti;
  }
/*
  if (ti.back() == '?') {
    ss << ".";
    json += ".";
  }
  ss << "  <i>" << pd.content() << "</i>, ";
*/
ss << ". <i>" << pd.content() << "</i>, ";
  json += ". <i>" + pd.content() + "</i>, ";
  if (pd.attribute_value("pages") == "0-0") {
    decode_missing_journal_pages(pd.attribute_value("number"), ss, json);
  } else {
    decode_non_missing_journal_pages(pd.attribute_value("pages"), pd.
        attribute_value("number"), ss, json);
  }
  auto doi = e.element("doi").content();
  if (!doi.empty()) {
    ss << " (DOI: " << doi << ")";
    json += " (DOI: " + doi + ")";
  }
  ss << ".";
  json += ".";
}

void add_preprint_to_publication(const XMLElement& e, stringstream& ss, string&
    json) {
  static const string F = this_function_label(__func__);
  auto cnf = e.element("conference");
  auto url = e.element("url").content();
  if (!url.empty()) {
    ss << "<a href=\"" << url << "\">" << e.element("title").content() <<
        "</a>";
    json += "<a href=\\\"" + url + "\\\">" + e.element("title").
        content() + "</a>";
  } else {
    ss << e.element("title").content();
    json += e.element("title").content();
  }
  ss << ".  <i>Proceedings of the " << cnf.content() << "</i>, " << cnf.
      attribute_value("host") << ", " << cnf.attribute_value("location");
  json += ". <i>Proceedings of the " + cnf.content() + "</i>, " + cnf.
      attribute_value("host") + ", " + cnf.attribute_value("location");
  auto pg = cnf.attribute_value("pages");
  if (!pg.empty()) {
    ss << ", " << pg;
    json += ", " + pg;
  }
  auto doi = e.element("doi").content();
  if (!doi.empty()) {
    ss << " (DOI: " << doi << ")";
    json += " (DOI: " + doi + ")";
  }
  ss << ".";
  json += ".";
}

void add_tech_paper_to_publication(const XMLElement& e, stringstream& ss,
    string& json) {
  static const string F = this_function_label(__func__);
  auto org = e.element("organization");
  auto url = e.element("url").content();
  if (!url.empty()) {
    ss << "<i><a href=\"" << url << "\">" << e.element("title").content()
        << "</a>.</i>";
    json += "<i><a href=\\\"" + url + "\\\">" + e.element("title").
        content() + "</a>.</i>";
  } else {
    ss << "<i>" << e.element("title").content() << ".</i>";
    json += "<i>" + e.element("title").content() + ".</i>";
  }
  ss << "  ";
  json += " ";
  auto id = org.attribute_value("reportID");
  if (!id.empty()) {
    ss << id << ", ";
    json += id + ", ";
  }
  ss << org.content();
  json += org.content();
  auto pg = org.attribute_value("pages");
  if (pg != "-99") {
    ss << ", " << pg << " pp.";
    json += ", " + pg + " pp.";
  }
  auto doi = e.element("doi").content();
  if (!doi.empty()) {
    ss << " (DOI: " << doi << ").";
    json += " (DOI: " + doi + ")";
  }
}

void add_book_to_publication(const XMLElement& e, stringstream& ss,
    string& json) {
  static const string F = this_function_label(__func__);
  auto pb = e.element("publisher");
  ss << "<i>" << e.element("title").content() << "</i>. " << pb.content()
      << ", " << pb.attribute_value("place");
  json += "<i>" + e.element("title").content() + "</i>. " + pb.content() +
      ", " + pb.attribute_value("place");
  auto doi = e.element("doi").content();
  if (!doi.empty()) {
    ss << " (DOI: " << doi << ")";
    json += " (DOI: " + doi + ")";
  }
  ss << ".";
  json += ".";
}

void add_book_chapter_to_publication(const XMLElement& e, stringstream& ss,
    string& json) {
  static const string F = this_function_label(__func__);
  auto bk = e.element("book");
  ss << "\"" << e.element("title").content() << "\", in " << bk.content()
      << ". Ed. " << bk.attribute_value("editor") << ", " << bk.
      attribute_value("publisher") << ", ";
  json += "\\\"" + e.element("title").content() + "\\\", in " + bk.
      content() + ". Ed. " + bk.attribute_value("editor") + ", " + bk.
      attribute_value("publisher") + ", ";
  if (bk.attribute_value("pages") == "0-0") {
    ss << "In Press";
    json += "In Press";
  } else {
    ss << bk.attribute_value("pages");
    json += bk.attribute_value("pages");
  }
  auto doi = e.element("doi").content();
  if (!doi.empty()) {
    ss << " (DOI: " << doi << ")";
    json += " (DOI: " + doi + ")";
  }
  ss << ".";
  json += ".";
}

//void add_publications(TokenDocument& tdoc, XMLDocument& g_xdoc) {
void add_publications(XMLDocument& g_xdoc) {
  static const string F = this_function_label(__func__);
  auto rlist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/reference");
  stringstream ss;
  if (!rlist.empty()) {
//    tdoc.add_if("__HAS_PUBLICATIONS__");
    rlist.sort(compare_references);
    string json;
    for (const auto& r : rlist) {
      ss << "<div>" << r.element("authorList").content() << ", " << r.element(
          "year").content() << ": ";
      append(json, "\"" + r.element("authorList").content() + ", " + r.element(
          "year").content() + ": ", ", ");
      auto ptyp = r.attribute_value("type");
      if (ptyp == "journal") {
        add_journal_to_publication(r, ss, json);
      } else if (ptyp == "preprint") {
        add_preprint_to_publication(r, ss, json);
      } else if (ptyp == "technical_report") {
        add_tech_paper_to_publication(r, ss, json);
      } else if (ptyp == "book") {
        add_book_to_publication(r, ss, json);
      } else if (ptyp == "book_chapter") {
        add_book_chapter_to_publication(r, ss, json);
      }
      ss << "</div>" << endl;
      auto ann = r.element("annotation").content();
      if (!ann.empty() > 0) {
        ss << "<div class=\"ms-2 text-muted small\">" << ann << "</div>" <<
            endl;
        json += "<div class=\\\"ms-2 text-muted small\\\">" + ann + "</div>";
      }
      ss << "<br>" << endl;
      json += "\"";
    }
    update_wagtail("publications", "[" + json + "]", F);
  }
  auto ulist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/referenceURL");
  if (!ulist.empty()) {
    if (ss.str().empty()) {
//      tdoc.add_if("__HAS_PUBLICATIONS__");
    }
    for (const auto& u : ulist) {
      ss << "<a href=\"" << u.attribute_value("url") << "\">" << u.content() <<
          "</a><br>" << endl;
    }
  }
  if (!ss.str().empty()) {
//    tdoc.add_replacement("__PUBLICATIONS__", ss.str());
  }
}

//void add_data_formats(TokenDocument& tdoc, vector<string>& formats, bool
void add_data_formats(vector<string>& formats, bool
    found_content_metadata) {
  static const string F = this_function_label(__func__);
  if (!found_content_metadata) {
    auto flist=g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
        "dsOverview/contentMetadata/format");
    for (const auto& f : flist) {
      formats.emplace_back(f.content() + "<!>" + f.attribute_value("href"));
    }
  }
  XMLDocument fdoc;
  auto f = unixutils::remote_web_file("https://" + metautils::directives.
      web_server + "/metadata/FormatReferences.xml", temp_dir.name());
  fdoc.open(f);
  if (!fdoc) {
    log_error2("unable to open FormatReferences.xml", F, "dsgen", USER);
  }
  string df, json;
  size_t n = 0;
  for (const auto& i : formats) {
    string j;
    auto sp = split(i, "<!>");
    auto d = sp[0];
    string u;
    if (has_beginning(d, "proprietary_")) {
      replace_all(d, "proprietary_", "");
      if (!sp[1].empty()) {
        u = sp[1];
      } else {
        d += " (see dataset documentation)";
        u = "";
      }
    } else {
      auto f = fdoc.element(("formatReferences/format@name=" + d));
      u = f.attribute_value("href");
    }
    if (!u.empty()) {
      df += "<a href=\"" + u + "\" target=\"_format\">";
      if (!has_beginning(u, "http://rda.ucar.edu")) {
        df += "<i>";
      }
    }
    replace_all(d, "_", " ");
    df += d;
    append(j, "\"description\": \"" + d + "\"", ", ");
    if (!u.empty()) {
      df += "</a>";
      if (!has_beginning(u, "http://rda.ucar.edu")) {
        df += "</i>";
      }
      append(j, "\"url\": \"" + u + "\"", ", ");
    }
    ++n;
    if (n < formats.size()) {
      df += ", ";
    }
    append(json, "{" + j + "}", ", ");
  }
  fdoc.close();
//  tdoc.add_replacement("__DATA_FORMATS__", df);
  update_wagtail("data_formats", "[" + json + "]", F);
}

void append_book_chapter_to_citation(string& citation, string doi) {
  citation += "\", in ";
  LocalQuery qcw("select pages, isbn from citation.book_chapter_works where "
      "doi = '" + doi + "'");
  Row rcw;
  if (qcw.submit(g_metadata_server) != 0 || !qcw.fetch_row(rcw)) {
    citation = "";
    return;
  }
  LocalQuery qbw("select title, publisher from citation.book_works where isbn "
      "= '" + rcw[1] + "'");
  Row rbw;
  if (qbw.submit(g_metadata_server) != 0 || !qbw.fetch_row(rbw)) {
    citation = "";
    return;
  }
  citation += unicode_escape_to_html(rbw[0]) + ". Ed. ";
  LocalQuery qwa("select first_name, middle_name, last_name from citation."
      "works_authors where id = '" + rcw[1] + "' and id_type = 'ISBN' order by "
      "sequence");
  if (qwa.submit(g_metadata_server) != 0) {
    citation = "";
    return;
  }
  size_t n = 1;
  for (const auto& rwa : qwa) {
    if (n > 1) {
      citation += ", ";
      if (n == qwa.num_rows()) {
        citation += "and ";
      }
    }
    auto fnam = unicode_escape_to_html(rwa[0]);
    string fi;
    if (fnam.find("&") == 0 && fnam.find(";") != string::npos) {
      fi = fnam.substr(0, fnam.find(";")+1);
    } else {
      fi = fnam.substr(0, 1);
    }
    citation += fi + ". ";
    if (!rwa[1].empty()) {
      auto mnam = unicode_escape_to_html(rwa[1]);
      string mi;
      if (mnam.find("&") == 0 && mnam.find(";") != string::npos) {
        mi = mnam.substr(0, mnam.find(";")+1);
      } else {
        mi = mnam.substr(0, 1);
      }
      citation += mi + ". ";
    }
    citation += unicode_escape_to_html(rwa[2]);
    ++n;
  }
  citation += ", " + unicode_escape_to_html(rbw[1]) + ", " + rcw[0] + ".";
}

void append_journal_to_citation(string& citation, string doi) {
  citation += ". ";
  LocalQuery q("select pub_name, volume, pages from citation.journal_works "
      "where doi = '" + doi + "'");
  Row row;
  if (q.submit(g_metadata_server) != 0 || !q.fetch_row(row)) {
    citation = "";
    return;
  }
  citation += "<em>" + unicode_escape_to_html(row[0]) + "</em>";
  if (!row[1].empty()) {
    citation += ", <strong>" + row[1] + "</strong>";
  }
  if (!row[2].empty()) {
    citation += ", " + row[2];
  }
  citation += ", <a href=\"https://doi.org/" + doi + "\" target=\"_doi\">"
      "https://doi.org/" + doi + "</a>";
}

void append_proceedings_to_citation(string& citation, string doi, string
    publisher) {
  citation += ". <em>";
  LocalQuery q("select pub_name, pages from citation.proceedings_works where "
      "doi = '" + doi + "'");
  Row row;
  if (q.submit(g_metadata_server) != 0 || !q.fetch_row(row)) {
    citation = "";
    return;
  }
  auto d = unicode_escape_to_html(row[0]) + "</em>";
  if (!publisher.empty()) {
    d += ", " + publisher;
  }
  if (!row[1].empty()) {
    d += ", " + row[1];
  }
  citation += d + ", <a href=\"https://doi.org/" + doi + "\" target="
      "\"_doi\">https://doi.org/" + doi + "</a>";
}

//void add_data_citations(TokenDocument& tdoc) {
void add_data_citations() {
  static const string F = this_function_label(__func__);
  LocalQuery qc("select distinct d.doi_work from citation.data_citations as d "
      "left join dssdb.dsvrsn as v on v.doi = d.doi_data where v.dsid in " +
       g_ds_set);
  if (qc.submit(g_metadata_server) < 0) {
    return;
  }
  vector<tuple<string, string>> clist;
  for (const auto& rc : qc) {
    auto doi = rc[0];
    LocalQuery qw("select title, pub_year, type, publisher from citation.works "
        "where doi = '" + doi + "'");
    Row rw;
    if (qw.submit(g_metadata_server) == 0 && qw.fetch_row(rw)) {
      auto ti = unicode_escape_to_html(rw[0]);
      auto yr = rw[1];
      auto typ = rw[2];
      LocalQuery qwa("select last_name, first_name, middle_name, orcid_id from "
          "citation.works_authors where id = '" + doi + "' and id_type = 'DOI' "
          "order by sequence");
      if (qwa.submit(g_metadata_server) == 0) {
        string cit;
        size_t n = 1;
        for (const auto& rwa : qwa) {
          auto fnam = unicode_escape_to_html(rwa[1]);
          string fi;
          if (fnam.find("&") == 0 && fnam.find(";") != string::npos) {
            fi = fnam.substr(0, fnam.find(";")+1);
          } else {
            fi = fnam.substr(0, 1);
          }
          auto mnam = unicode_escape_to_html(rwa[2]);
          string mi;
          if (mnam.find("&") == 0 && mnam.find(";") != string::npos) {
            mi = mnam.substr(0, mnam.find(";")+1);
          } else {
            mi = mnam.substr(0, 1);
          }
          if (cit.empty()) {

            // first author
            if (!rwa[3].empty() && rwa[3] != "NULL") {
              cit += "<a href=\"https://orcid.org/" + rwa[3] + "\" target=\""
                  "_orcid\">";
            }
            cit += unicode_escape_to_html(rwa[0]);
            if (!rwa[1].empty()) {
              cit += ", " + fi + ".";
            }
            if (!rwa[2].empty()) {
              cit += " " + mi + ".";
            }
            if (!rwa[3].empty()) {
              cit += "</a>";
            }
          } else {

            // co-authors
            cit += ", ";
            if (n == qwa.num_rows()) {
              cit += "and ";
            }
            if (!rwa[3].empty() && rwa[3] != "NULL") {
              cit += "<a href=\"https://orcid.org/" + rwa[3] + "\" target=\""
                  "_orcid\">";
            }
            if (!rwa[1].empty()) {
              cit += fi + ". ";
            }
            if (!rwa[2].empty()) {
              cit += mi + ". ";
            }
            cit += unicode_escape_to_html(rwa[0]);
            if (!rwa[3].empty()) {
              cit += "</a>";
            }
          }
          ++n;
        }
        if (!cit.empty()) {
          cit += ", " + yr + ": ";
          if (typ == "C") {
            cit += "\"";
          }
          cit += ti;
          switch (typ[0]) {
            case 'C': {
              append_book_chapter_to_citation(cit, doi);
              break;
            }
            case 'J': {
              append_journal_to_citation(cit, doi);
              break;
            }
            case 'P': {
              append_proceedings_to_citation(cit, doi, rw[3]);
              break;
            }
          }
        }
        if (!cit.empty()) {
          clist.emplace_back(make_tuple(yr, cit));
        }
      }
    }
  }
  if (clist.size() > 0) {
//    tdoc.add_if("__HAS_DATA_CITATIONS__");
/*
    if (clist.size() > 1) {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>" + itos(clist.
          size()) + "</strong> times");
    } else {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>" + itos(clist.
          size()) + "</strong> time");
    }
*/
    std::sort(clist.begin(), clist.end(), compare_citations);
    unordered_set<string> yrs;
    string json;
    for (const auto& c : clist) {
      auto pub_year=get<0>(c);
      if (yrs.find(pub_year) == yrs.end()) {
/*
        tdoc.add_repeat("__DATA_CITER__", "CITATION[!]" + get<1>(c) +
            "<!>YEAR[!]" + pub_year);
*/
        append(json, "{\"year\": " + pub_year + ", \"publications\": [\"" +
            substitute(get<1>(c), "\"", "\\\"") + "\"", "]}, ");
        yrs.emplace(pub_year);
      } else {
//        tdoc.add_repeat("__DATA_CITER__", "CITATION[!]" + get<1>(c));
        append(json, "\"" + substitute(get<1>(c), "\"", "\\\"") + "\"", ", ");
      }
    }
    update_wagtail("num_citations", to_string(clist.size()), F);
    update_wagtail("citations", "[" + json + "]}]", F);
  } else {
//    tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>0</strong> times");
  }
}

//bool add_temporal_range(TokenDocument& tdoc, size_t& swp_cnt) {
bool add_temporal_range(size_t& swp_cnt) {
  static const string F = this_function_label(__func__);

  bool grouped_periods = false; // return value
  LocalQuery q("select dsid from dssdb.dsgroup where dsid in " + g_ds_set);
  if (q.submit(g_metadata_server) < 0) {
    log_error2("query: " + q.show() + " returned error: " + q.error(), F,
        "dsgen", USER);
  }
  if (q.num_rows() > 0) {
    q.set("select p.date_start, p.time_start, p.start_flag, p.date_end, "
        "p.time_end, p.end_flag, p.time_zone, g.title, g.grpid from dssdb"
        ".dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p"
        ".gindex = g.gindex) where p.dsid in " + g_ds_set + " and g.pindex = 0 "
        "and date_start > '0001-01-01' and date_start < '5000-01-01' and "
        "date_end > '0001-01-01' and date_end < '5000-01-01' union select p."
        "date_start, p.time_start, p.start_flag, p.date_end, p.time_end, p."
        "end_flag, p.time_zone, g2.title, NULL from dssdb.dsperiod as p left "
        "join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) "
        "left join dssdb.dsgroup as g2 on (p.dsid = g2.dsid and g.pindex = g2."
        "gindex) where p.dsid in " + g_ds_set + " and date_start > "
        "'0001-01-01' and date_start < '5000-01-01' and date_end > "
        "'0001-01-01' and date_end < '5000-01-01' and g2.title is not null "
        "order by title");
    if (q.submit(g_metadata_server) < 0) {
      log_error2("query: " + q.show() + " returned error: " + q.error(), F,
          "dsgen", USER);
    }
  }
  if (q.num_rows() == 0) {
    q.set("select date_start, time_start, start_flag, date_end, time_end, "
        "end_flag, time_zone, NULL, NULL from dssdb.dsperiod where dsid in "
        + g_ds_set + " and (time_zone = 'BCE' or (date_start between "
        "'0001-01-01' and '5000-01-01' and date_end between '0001-01-01' and "
        "'5000-01-01'))");
    if (q.submit(g_metadata_server) < 0) {
      log_error2("query: " + q.show() + " returned error: " + q.error(), F,
          "dsgen", USER);
    }
  }
  string json;
  if (q.num_rows() > 0) {
//    tdoc.add_if("__HAS_TEMPORAL_RANGE__");
    if (q.num_rows() > 1) {
      LocalQuery qgp("distinct gindex", "dssdb.dsperiod", "dsid in " +
          g_ds_set);
      if (qgp.submit(g_metadata_server) < 0) {
        log_error2("query: " + qgp.show() + " returned error: " + qgp.error(),
            F, "dsgen", USER);
      }
      if (qgp.num_rows() > 1) {
        grouped_periods = true;
//        tdoc.add_if("__HAS_TEMPORAL_BY_GROUP1__");
        LocalQuery qdt("select min(concat(date_start, ' ', time_start)), min("
            "start_flag), max(concat(date_end, ' ', time_end)), min(end_flag), "
            "time_zone from dssdb.dsperiod where dsid in " + g_ds_set + " and "
            "date_start > '0001-01-01' and date_start < '5000-01-01' and "
            "date_end > '0001-01-01' and date_end < '5000-01-01' group by "
            "dsid, time_zone");
        if (qdt.submit(g_metadata_server) < 0) {
          log_error2("query: " + qdt.show() + " returned error: " + qdt.error(),
              F, "dsgen", USER);
        }
        Row rdt;
        qdt.fetch_row(rdt);
        auto sdt = metatranslations::date_time(rdt[0], rdt[1], rdt[4]);
        auto edt = metatranslations::date_time(rdt[2], rdt[3], rdt[4]);
        auto temporal = sdt;
        if (!edt.empty() && edt != sdt) {
          temporal += " to " + edt;
        }
//        tdoc.add_replacement("__TEMPORAL_RANGE__", temporal);
        json = "\"full\": \"" + temporal + "\"";
//        tdoc.add_replacement("__N_TEMPORAL__", itos(swp_cnt));
//        tdoc.add_replacement("__N_TEMPORAL1__", itos(swp_cnt + 1));
        swp_cnt += 2;
      }
    }
    map<string, tuple<string, string, string, string>> pdlist;
    for (const auto& r : q) {
      auto sdt = metatranslations::date_time(r[0] + " " + r[1], r[2], r[6]);
      auto edt = metatranslations::date_time(r[3] + " " + r[4], r[5], r[6]);
      string k;
      k = !r[7].empty() ? r[7] : r[8];
      if (pdlist.find(k) == pdlist.end()) {
        pdlist.emplace(k, make_tuple(sdt, r[6], edt, r[6]));
      } else {
        string &start = get<0>(pdlist[k]);
        string &end = get<2>(pdlist[k]);
        if (r[6] == "BCE") {
          if (sdt > start) {
            start = sdt;
          }
          if (edt < end) {
            end = edt;
          }
        } else {
          if (sdt < start) {
            start = sdt;
          }
          if (edt > end) {
            end = edt;
          }
        }
      }   
    }
    string j;
    for (const auto& i : pdlist) {
      string trng, tz1, e, tz2;
      tie(trng, tz1, e, tz2) = i.second;
      if (tz1 == "BCE") {
        trng += " BCE";
      }
      if (!e.empty() && e != trng) {
        trng += " to " + e;
        if (tz2 == "BCE") {
          trng += " BCE";
        }
      }
      if (pdlist.size() > 1) {
        trng += " (" + i.first + ")";
/*
        tdoc.add_repeat("__TEMPORAL_RANGE__", "<div style=\"margin-left: 10px"
            "\">" + trng + "</div>");
*/
        append(j, "\"" + trng + "\"", ", ");
      } else {
//        tdoc.add_repeat("__TEMPORAL_RANGE__", trng);
        json = "\"full\": \"" + trng + "\"";
      }
    }
    if (grouped_periods) {
//      tdoc.add_if("__HAS_TEMPORAL_BY_GROUP2__");
    }
    if (!j.empty()) {
      json += ", \"groups\": [" + j + "]";
    }
  }
  update_wagtail("temporal", "{" + json + "}", F);
  return grouped_periods;
}

//void add_update_frequency(TokenDocument& tdoc) {
void add_update_frequency() {
  static const string F = this_function_label(__func__);
  auto e = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
      "continuingUpdate");
  if (e.attribute_value("value") == "yes") {
/*
    tdoc.add_if("__HAS_UPDATE_FREQUENCY__");
    tdoc.add_replacement("__UPDATE_FREQUENCY__", capitalize(e.attribute_value(
        "frequency")));
*/
    update_wagtail("update_freq", capitalize(e.attribute_value("frequency")),
        F);
  }
}

//void add_access_restrictions(TokenDocument& tdoc) {
void add_access_restrictions() {
  static const string F = this_function_label(__func__);
  auto e = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
      "restrictions/access");
  auto a = e.to_string();
  replace_all(a, "<access>", "");
  replace_all(a, "</access>", "");
/*
  auto idx = a.find("<p");
  if (idx == string::npos) {
    idx = a.find("<P");
  }
  if (idx != string::npos) {
    auto idx2 = a.find(">", idx);
    a.insert(idx2, " style=\"margin: 0px; padding: 0px\"");
  }
*/
  if (!a.empty()) {
/*
    tdoc.add_if("__HAS_ACCESS_RESTRICTIONS__");
    tdoc.add_replacement("__ACCESS_RESTRICTIONS__", a);
*/
  }
  update_wagtail("access_restrict", a, F);
}

//void add_usage_restrictions(TokenDocument& tdoc) {
void add_usage_restrictions() {
  static const string F = this_function_label(__func__);
  auto e = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
      "restrictions/usage");
  auto u = e.to_string();
  replace_all(u, "<usage>", "");
  replace_all(u, "</usage>", "");
/*
  auto idx = u.find("<p");
  if (idx == string::npos) {
    idx = u.find("<P");
  }
  if (idx != string::npos) {
    auto idx2 = u.find(">", idx);
    u.insert(idx2, " style=\"margin: 0px; padding: 0px\"");
  }
*/
  if (!u.empty()) {
/*
    tdoc.add_if("__HAS_USAGE_RESTRICTIONS__");
    tdoc.add_replacement("__USAGE_RESTRICTIONS__", u);
*/
  }
  update_wagtail("usage_restrict", u, F);
}

//void add_variable_table(string data_format, TokenDocument& tdoc, string& json) {
void add_variable_table(string data_format, string& json) {
  if (exists_on_server(metautils::directives.web_server, "/data/web/datasets/"
      + metautils::args.dsid + "/metadata/" + data_format + ".html")) {
    auto token = to_upper(data_format);
    append(json, "{\"format\": \"" + token + "\", \"html\": \"metadata/" +
        data_format + ".html?_do=y\"", ", ");
//    tdoc.add_if("__FOUND_" + token + "_TABLE__");
    auto tb = "<div>" + token + " parameter table:  <a href=\"/datasets/" +
        metautils::args.dsid + "/#metadata/" + data_format + ".html?_do=y\">"
        "HTML</a>";
    if (exists_on_server(metautils::directives.web_server, "/data/web/datasets/"
        + metautils::args.dsid + "/metadata/" + data_format + ".xml")) {
      tb += " | <a href=\"/datasets/" + metautils::args.dsid + "/metadata/" +
          data_format + ".xml\">XML</a></div>";
      append(json, "\"xml\": \"metadata/" + data_format + ".xml?_do=y\"",
          ", ");
    }
    json += "}";
//    tdoc.add_replacement("__" + token + "_TABLE__", tb);
  }
}

//void add_grouped_variables(TokenDocument& tdoc, size_t& swp_cnt) {
void add_grouped_variables(size_t& swp_cnt) {
  LocalQuery q("gindex, title", "dssdb.dsgroup", "dsid in " + g_ds_set + "and "
      "pindex = 0 and dwebcnt > 0");
  if (q.submit(g_metadata_server) != 0) {
    return;
  }
  stringstream ss;
  for (const auto& r : q) {
    if (exists_on_server(metautils::directives.web_server, "/data/web/datasets/"
        + metautils::args.dsid + "/metadata/customize.WGrML." + r[0])) {
      auto f = unixutils::remote_web_file("https://" + metautils::directives.
          web_server + "/datasets/" + metautils::args.dsid + "/metadata/"
          "customize.WGrML." + r[0], temp_dir.name());
      if (!f.empty()) {
        std::ifstream ifs;
        char line[32768];
        ifs.open(f);
        ifs.getline(line, 32768);
        if (has_beginning(line, "curl_subset=")) {
          ifs.getline(line, 32768);
        }
        auto nv = stoi(line);
        vector<string> vlist;
        for (int n = 0; n < nv; ++n) {
          ifs.getline(line, 32768);
          auto sp=split(line, "<!>");
          vlist.emplace_back(sp[1]);
        }
        ifs.close();
        if (vlist.size() > 0) {
          if (ss.tellp() == 0) {
            ss << "<div style=\"position: relative; overflow: hidden\"><span "
                "id=\"D" << swp_cnt << "\"><a href=\"javascript:swapDivs(" <<
                swp_cnt << ", " << swp_cnt + 1 << ")\" title=\"Expand dataset "
                "product variable list\"><img src=\"/images/bluetriangle.gif\" "
                "width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\""
                ">Variables by dataset product</span></a></span><span style=\""
                "visibility: hidden; position: absolute; top: 0\" id=\"D" <<
                swp_cnt + 1 << "\"><a href=\"javascript:swapDivs(" << swp_cnt +
                1 << ", " << swp_cnt << ")\"><img src=\"/images/bluetriangle90."
                "gif\" width=\"15\" height=\"12\" border=\"0\" title=\""
                "Collapse dataset product variable list\"></a><span class=\""
                "fs13px\">Variables by dataset product:";
            swp_cnt += 2;
          }
          ss << "<div style=\"margin-left: 10px\"><span id=\"D" << swp_cnt <<
              "\"><a href=\"javascript:swapDivs(" << swp_cnt << ", " << swp_cnt
              + 1 << ")\" title=\"Expand\"><img src=\"/images/bluetriangle."
              "gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\""
              "fs13px\">Variable list for " << r[1] << "</span></a></span>"
              "<span style=\"visibility: hidden; position: absolute; top: 0\" "
              "id=\"D" << swp_cnt + 1 << "\"><a href=\"javascript:swapDivs(" <<
              swp_cnt + 1 << ", " << swp_cnt << ")\"><img src=\"/images/"
              "bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" "
              "title=\"Collapse\"></a><span class=\"fs13px\">Variable list for "
              << r[1] << ":<div style=\"margin-left: 20px\">";
          swp_cnt += 2;
          for (const auto& var : vlist) {
            ss << var << "<br />";
          }
          ss << "</div></span></span></div>";
        }
      }
    }
  }
  if (ss.tellp() > 0) {
    ss << "</span></span></div>";
/*
    tdoc.add_if("__HAS_VARIABLES_BY_PRODUCT__");
    tdoc.add_replacement("__VARIABLES_BY_PRODUCT__", ss.str());
*/
  }
}

//void add_variables(TokenDocument& tdoc, size_t& swp_cnt) {
void add_variables(size_t& swp_cnt) {
  static const string F = this_function_label(__func__);
  LocalQuery q("select split_part(path, ' > ', -1) as var from search."
      "variables as v left join search.gcmd_sciencekeywords as g on g.uuid = v."
      "keyword where v.vocabulary = 'GCMD' and v.dsid in " + g_ds_set +
      " order by var");
  if (q.submit(g_metadata_server) < 0) {
    log_error2("query: " + q.show() + " returned error: " + q.error(), F,
        "dsgen", USER);
  }
  vector<string> l;
  string json;
  for (const auto& r : q) {
    l.emplace_back(capitalize(r[0]));
    append(json, "\"" + capitalize(r[0]) + "\"", ", ");
  }
  json = "\"gcmd\": [" + json + "]";
/*
  tdoc.add_replacement("__VARIABLES__", create_table_from_strings(l, 4,
      "#e1eaff", "#c8daff"));
*/
  auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/contentMetadata/detailedVariables/detailedVariable");
  if (elist.size() > 0) {
    for (const auto& e : elist) {
      if (has_beginning(e.content(), "http://") || has_beginning(e.content(),
          "https://")) {
//        tdoc.add_if("__HAS_DETAILED_VARIABLES__");
//        tdoc.add_replacement("__DETAILED_VARIABLES_LINK__", e.content());
        break;
      }
    }
  }
  string tj;
/*
  add_variable_table("grib", tdoc, tj);
  add_variable_table("grib2", tdoc, tj);
  add_variable_table("on84", tdoc, tj);
*/
add_variable_table("grib", tj);
add_variable_table("grib2", tj);
add_variable_table("on84", tj);
  if (!tj.empty()) {
    append(json, "\"tables\": [" + tj + "]", ", ");
  }
//  add_grouped_variables(tdoc, swp_cnt);
add_grouped_variables(swp_cnt);
  update_wagtail("variables", "{" + json + "}", F);
}

/*
void add_vertical_levels(TokenDocument& tdoc, const vector<string>& data_types,
    bool found_content_metadata) {
*/
void add_vertical_levels(const vector<string>& data_types, bool
    found_content_metadata) {
  static const string F = this_function_label(__func__);
  string json;
  if (found_content_metadata) {
    for (const auto& dt : data_types) {
      if (dt == "grid") {
//        tdoc.add_if("__HAS_VERTICAL_LEVELS__");
        json = "\"list\": []";
        string v = "See the <a href=\"#metadata/detailed.html?_do=y&view=level"
            "\">detailed metadata</a> for level information";
        if (exists_on_server(metautils::directives.web_server,
            "/data/web/datasets/" + metautils::args.dsid + "/metadata/"
            "grib2_levels.html")) {
          v += "<br /><a href=\"/datasets/" + metautils::args.dsid +
              "/#metadata/grib2_levels.html?_do=y\">GRIB2 level table</a>"; 
        }
//        tdoc.add_replacement("__VERTICAL_LEVELS__", v);
        break;
      }
    }
  } else {
    auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
        "dsOverview/contentMetadata/levels/level");
    auto elist2 = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
        "dsOverview/contentMetadata/levels/layer");
    elist.insert(elist.end(), elist2.begin(), elist2.end());
    if (!elist.empty()) {
//      tdoc.add_if("__HAS_VERTICAL_LEVELS__");
      vector<string> llst;
      for (const auto& ele : elist) {
        if ((ele.attribute_value("value") == "0" || (ele.attribute_value("top")
            == "0" && ele.attribute_value("bottom") == "0")) && ele.
            attribute_value("units").empty()) {
          llst.emplace_back(ele.attribute_value("type"));
        } else {
          if (!ele.attribute_value("value").empty()) {
            auto l = ele.attribute_value("value") + " " + ele.attribute_value(
                "units");
            if (regex_search(l, regex("^\\."))) {
              l = "0" + l;
            }
            if (ele.attribute_value("value") != "0" && regex_search(ele.
                attribute_value("type"), regex("height below"))) {
              l = "-" + l;
            }
            llst.emplace_back(l);
          } else {
            auto layer = ele.attribute_value("top");
            if (regex_search(layer, regex("^\\."))) {
              layer = "0" + layer;
            }
            if (ele.attribute_value("top") != ele.attribute_value("bottom")) {
              auto bottom = ele.attribute_value("bottom");
              if (regex_search(bottom, regex("^\\."))) {
                bottom = "0" + bottom;
              }
              layer += "-" + bottom;
            }
            layer += " " + ele.attribute_value("units");
            llst.emplace_back(layer);
          }
        }
      }
      sort(llst.begin(), llst.end(), compare_levels);
/*
      tdoc.add_replacement("__VERTICAL_LEVELS__", create_table_from_strings(
          llst, 4, "#c8daff", "#e1eaff"));
*/
      for (const auto& e : llst) {
        append(json, "\"" + e + "\"", ", ");
      }
      json = "\"list\": [" + json + "]";
    }
  }
  if (!json.empty()) {
    update_wagtail("levels", "{" + json + "}", F);
  } else {
    update_wagtail("levels", "[]", F);
  }
}

//void add_temporal_frequency(TokenDocument& tdoc) {
void add_temporal_frequency() {
  static const string F = this_function_label(__func__);
  auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/contentMetadata/temporalFrequency");
  auto m = 0;
  if (elist.size() > 0) {
//    tdoc.add_if("__HAS_TEMPORAL_FREQUENCY__");
    stringstream ss;
    for (const auto& ele : elist) {
      auto typ = ele.attribute_value("type");
      if (m > 0) {
        ss << ", ";
      }
      if (typ == "regular") {
        auto n = stoi(ele.attribute_value("number"));
        ss << "Every ";
        if (n > 1) {
          ss << n << " ";
        }
        ss << ele.attribute_value("unit");
        if (n > 1) {
          ss << "s";
        }
        auto s = ele.attribute_value("statistics");
        if (!s.empty()) {
          ss << " (" << capitalize(s) << ")";
        }
        ss << endl;
      } else if (typ == "irregular") {
        ss << "various times per " << ele.attribute_value("unit");
        auto s = ele.attribute_value("statistics");
        if (!s.empty()) {
          ss << " (" << capitalize(s) << ")";
        }
      } else if (typ == "climatology") {
        auto u = ele.attribute_value("unit");
        if (u == "hour") {
          u = "Hourly";
        } else if (u == "day") {
          u = "Daily";
        } else if (u == "week") {
          u = "Weekly";
        } else if (u == "month") {
          u = "Monthly";
        } else if (u == "winter") {
          u = "Winter Season";
        } else if (u == "spring") {
          u = "Spring Season";
        } else if (u == "summer") {
          u = "Summer Season";
        } else if (u == "autumn") {
          u = "Autumn Season";
        } else if (u == "year") {
          u = "Annual";
        } else if (u == "30-year") {
          u = "30-year (climate normal)";
        }
        ss << u << " Climatology";
      }
      ++m;
    }
//    tdoc.add_replacement("__TEMPORAL_FREQUENCY__", ss.str());
    update_wagtail("temporal_freq", ss.str(), F);
  }
}

//void add_data_types(TokenDocument& tdoc, const vector<string>& data_types,
void add_data_types(const vector<string>& data_types,
    bool found_content_metadata) {
  static const string F = this_function_label(__func__);
  string json;
  if (found_content_metadata) {
    if (!data_types.empty()) {
//      tdoc.add_if("__HAS_DATA_TYPES__");
      string s;
      for (const auto& data_type : data_types) {
        append(s, to_capital(data_type), ", ");
        append(json, "\"" + to_capital(data_type) + "\"", ", ");
      }
//      tdoc.add_replacement("__DATA_TYPES__", s);
    }
  } else {
    auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
        "dsOverview/contentMetadata/dataType");
    if (!elist.empty()) {
//      tdoc.add_if("__HAS_DATA_TYPES__");
      string s;
      unordered_map<string, char> u;
      for (const auto& ele : elist) {
        if (u.find(ele.content()) == u.end()) {
          append(s, to_capital(ele.content()), ", ");
          append(json, "\"" + to_capital(ele.content()) + "\"", ", ");
          u[ele.content()] = 'Y';
        }
      }
//      tdoc.add_replacement("__DATA_TYPES__", s);
    }
  }
  update_wagtail("data_types", "[" + json + "]", F);
}

void fill_map(unordered_map<string, string>& map, string query) {
  static const string F = this_function_label(__func__);
  LocalQuery q("select gindex, title from dssdb.dsgroup where dsid in " +
       g_ds_set);
  if (q.submit(g_metadata_server) < 0) {
    log_error2("error: " + q.error() + " while getting groups data", F, "dsgen",
        USER);
  }
  for (const auto& r : q) {
    map.emplace(r[0], r[1]);
  }
}

//void add_spatial_coverage(TokenDocument& tdoc, const vector<string>& data_types,
void add_spatial_coverage(const vector<string>& data_types,
    bool found_content_metadata, bool grouped_periods, size_t& swp_cnt) {
  static const string F = this_function_label(__func__);
  unordered_map<string, string> grps, rfils, mfils;
  if (grouped_periods) {
    string s = "select gindex, title from dssdb.dsgroup where dsid in " +
        g_ds_set;
    fill_map(grps, s);
    s = "select wfile, tindex from dssdb.wfile_" + metautils::args.dsid +
        " where type = 'D' and " "status = 'P'";
    fill_map(rfils, s);
    s = "select code, id from \"WGrML\"." + metautils::args.dsid + "_webfiles2";
    fill_map(mfils, s);
  }
  unordered_map<string, shared_ptr<unordered_set<string>>> udefs;
  if (found_content_metadata) {
    unordered_map<size_t, tuple<string, string>> gdefs;
    for (const auto& dt : data_types) {
      if (dt == "grid") {
        LocalQuery qgc;
        if (grouped_periods) {
          qgc.set("grid_definition_codes, file_code", "WGrML." + metautils::
              args.dsid + "_grid_definitions");
        } else {
          qgc.set("select distinct grid_definition_codes from \"WGrML\"." +
              metautils::args.dsid + "_agrids2");
        }
        if (qgc.submit(g_metadata_server) < 0) {
          log_error2("error: " + qgc.error() + " while getting grid "
              "definitions", F, "dsgen", USER);
        }
        for (const auto& rgc : qgc) {
          vector<size_t> b;
          bitmap::uncompress_values(rgc[0], b);
          for (const auto& v : b) {
            string k;
            if (gdefs.find(v) == gdefs.end()) {
              LocalQuery qgd("definition, def_params", "WGrML.grid_definitions",
                  "code = " + itos(v));
              if (qgd.submit(g_metadata_server) < 0) {
                log_error2("query: " + qgd.show() + " returned error: " + qgd.
                    error(), F, "dsgen", USER);
              }
              Row rgd;
              qgd.fetch_row(rgd);
              gdefs.emplace(v, make_tuple(rgd[0], rgd[1]));
              k = rgd[0] + "<!>" + rgd[1];
            } else {
              auto x = gdefs[v];
              k = get<0>(x) + "<!>" + get<1>(x);
            }
            string gp = "";
            if (!mfils.empty()) {
              unordered_map<string, string>::iterator m, r, g;
              if ( (m =  mfils.find(rgc[1])) != mfils.end() && (r = rfils.find(
                  m->second)) != rfils.end() && (g = grps.find(r->second)) !=
                  grps.end()) {
                gp = g->second;
              }
            }
            auto u = udefs.find(k);
            if (u == udefs.end()) {
              shared_ptr<unordered_set<string>> g;
              if (!gp.empty()) {
                g.reset(new unordered_set<string>);
                g->emplace(gp);
              }
              udefs.emplace(k, g);
            } else if (!gp.empty()) {
              if (u->second == nullptr) {
                u->second.reset(new unordered_set<string>);
              }
              if (u->second->find(gp) == u->second->end()) {
                u->second->emplace(gp);
              }
            }
          }
        }
        break;
      }
    }
  } else {
    auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
        "dsOverview/contentMetadata/geospatialCoverage/grid");
    for (const auto& e : elist) {
      auto d = e.attribute_value("definition");
      if (e.attribute_value("isCell") == "true") {
        d += "Cell";
      }
      auto k = d + "<!>" + e.attribute_value("numX") + ":" + e.attribute_value(
          "numY");
      if (regex_search(k, regex("^(latLon|mercator)"))) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":" + e.attribute_value("endLat") + ":" + e.
            attribute_value("endLon") + ":" + e.attribute_value("xRes") + ":" +
            e.attribute_value("yRes");
      } else if (has_beginning(k, "gaussLatLon")) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":" + e.attribute_value("endLat") + ":" + e.
            attribute_value("endLon") + ":" + e.attribute_value("xRes") + ":" +
            e.attribute_value("numY");
      } else if (has_beginning(k, "polarStereographic")) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":60" + e.attribute_value("pole") + ":" + e.
            attribute_value("projLon") + ":" + e.attribute_value("pole") +
            ":" + e.attribute_value("xRes") + ":" + e.attribute_value("yRes");
      } else if (has_beginning(k, "lambertConformal")) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":" + e.attribute_value("resLat") + ":" + e.
            attribute_value("projLon") + ":" + e.attribute_value("pole") +
            ":" + e.attribute_value("xRes") + ":" + e.attribute_value("yRes") +
            ":" + e.attribute_value("stdParallel1") + ":" + e.attribute_value(
            "stdParallel2");
      }
      udefs.emplace(k, nullptr);
    }
  }
  double mnw = 9999., mxe = -9999., mns = 9999., mxn = -9999.;
  vector<double> slst;
  for (const auto& e : udefs) {
    double wl, el, sl, nl;
    if (gridutils::filled_spatial_domain_from_grid_definition(e.first,
        "primeMeridian", wl, sl, el, nl)) {
      if (el < 0. && el < wl) {

        // data straddle the date line
        slst.emplace_back(el);
      } else {
        if (el > mxe) {
          mxe = el;
        }
      }
      if (wl < mnw) {
        mnw = wl; 
      }
      if (sl < mns) {
        mns = sl;
      }
      if (nl > mxn) {
        mxn = nl;
      }
    }
  }
  if (!slst.empty() && (mnw > -180. || mxe < 180.)) {
    for (const auto& east_lon : slst) {
      if (east_lon > mxe) {
        mxe = east_lon;
      }
    }
  }
  string json;
  if (mns < 9999.) {
    if (mnw < -180.) {
      mnw += 360.;
    }
    if (mxe > 180.) {
      mxe -= 360.;
    }
    auto west=ftos(fabs(mnw), 3);
    if (mnw < 0) {
      west += "W";
    } else {
      west += "E";
    }
    stringstream ss;
    ss << "Longitude Range:  Westernmost=" << west << "  Easternmost=";
    append(json, "\"west\": \"" + west + "\"", ", ");
    auto east=ftos(fabs(mxe), 3);
    if (mxe < 0) {
      east += "W";
    } else {
      east += "E";
    }
    ss << east << "<br />" << endl;
    append(json, "\"east\": \"" + east + "\"", ", ");
    auto south=ftos(fabs(mns), 3);
    if (mns < 0) {
      south += "S";
    } else {
      south += "N";
    }
    ss << "Latitude Range:  Southernmost=" << south << "  Northernmost=";
    append(json, "\"south\": \"" + south + "\"", ", ");
    auto north=ftos(fabs(mxn), 3);
    if (mxn < 0) {
      north += "S";
    } else {
      north += "N";
    }
    ss << north << endl;
    append(json, "\"north\": \"" + north + "\"", ", ");
    ss << "<br /><span id=\"D" << swp_cnt << "\"><a href=\"javascript:swapDivs("
        << swp_cnt << ", " << swp_cnt + 1 << ")\" title=\"Expand coverage "
        "details\"><img src=\"/images/triangle.gif\" width=\"12\" height=\""
        "15\" border=\"0\"><span class=\"fs13px\">Detailed coverage "
        "information</span></a></span><span style=\"visibility: hidden; "
        "position: absolute; top: 0\" id=\"D" << swp_cnt + 1 << "\"><a href=\""
        "javascript:swapDivs(" << swp_cnt + 1 << ", " << swp_cnt << ")\"><img "
        "src=\"/images/triangle90.gif\" width=\"15\" height=\"12\" border=\""
        "0\" title=\"Collapse coverage details\"></a><span class=\"fs13px\">"
        "Detailed coverage information:" << endl;
    swp_cnt += 2;
    map<string, shared_ptr<unordered_set<string>>> gdefs;
    for (const auto& e : udefs) {
      gdefs.emplace(gridutils::convert_grid_definition(e.first), e.second);
    }
    string j;
    for (const auto& e : gdefs) {
      ss << "<div style=\"margin-left: 10px\">" << e.first;
      auto s = "\"" + e.first;
      if (gdefs.size() > 1) {
        if (e.second != nullptr) {
          ss << "<div style=\"margin-left: 15px; color: #6a6a6a\">(";
          auto n = 0;
          string s2;
          for (const auto& group : *e.second) {
            if (n++ > 0) {
              ss << ", ";
            }
            ss << group;
            append(s2, group, ", ");
          }
          ss << ")</div>";
          if (!s2.empty()) {
            s += s2;
          }
        }
      }
      ss << "</div>";
      append(j, s + "\"", ", ");
    }
    if (!j.empty()) {
      json += ", \"details\": [" + j + "]";
    }
    ss << "</span></span>" << endl;
/*
    tdoc.add_if("__HAS_SPATIAL_COVERAGE__");
    tdoc.add_replacement("__SPATIAL_COVERAGE__", ss.str());
*/
    update_wagtail("spatial_coverage", "{" + json + "}", F);
  }
}

//void add_contributors(TokenDocument& tdoc) {
void add_contributors() {
  static const string F = this_function_label(__func__);
  LocalQuery q("select g.path from search.contributors_new as c left join "
      "search.gcmd_providers as g on g.uuid = c.keyword where c.dsid in " +
      g_ds_set + " and c.vocabulary = 'GCMD'");
  if (q.submit(g_metadata_server) < 0) {
    log_error2("query: " + q.show() + " returned error: " + q.error(), F,
        "dsgen", USER);
  }
  stringstream ss;
  string json;
  auto n = 0;
  for (const auto& r : q) {
    if (n > 0) {
      ss << " | ";
    }
    auto snam = r[0];
    string lnam = "";
    auto idx = snam.find(">");
    if (idx != string::npos) {
      lnam = snam.substr(idx + 1);
      snam = snam.substr(0, idx);
    }
    trim(snam);
    trim(lnam);
    ss << "<span class=\"infosrc\" onMouseOver=\"javascript:popInfo(this, 'src"
        << n << "', '#e1eaff', 'left', 'bottom')\" onMouseOut=\"javascript:"
        "hideInfo('src" << n << "')\">" << snam << "</span><div class=\"info\" "
        "id=\"src" << n << "\" class=\"source\"><small>" << lnam << "</small>"
        "</div>";
    append(json, "{\"id\": \"" + snam + "\", \"name\": \"" + lnam + "\"}", ", ");
    ++n;
  }
//  tdoc.add_replacement("__DATA__CONTRIBUTORS__", ss.str());
  if (!json.empty()) {
    update_wagtail("contributors", "[" + json + "]", F);
  }
}

//void add_data_volume(TokenDocument& tdoc, size_t& swp_cnt) {
void add_data_volume(size_t& swp_cnt) {
  static const string F = this_function_label(__func__);
  LocalQuery q("dweb_size", "dssdb.dataset", "dsid in " + g_ds_set);
  if (q.submit(g_metadata_server) < 0) {
    log_error2("query: " + q.show() + " returned error: " + q.error(), F,
        "dsgen", USER);
  }
  LocalQuery q2("select dweb_size, title, grpid from dssdb.dsgroup where dsid "
      "in " + g_ds_set + " and pindex = 0 and dweb_size > 0");
  if (q2.submit(g_metadata_server) < 0) {
    log_error2("query: " + q2.show() + " returned error: " + q2.error(), F,
        "dsgen", USER);
  }
  stringstream ss;
  const int VOLUME_LEN = 4;
  const char *vlist[VOLUME_LEN] = { "MB", "GB", "TB", "PB" };
  string json;
  Row row;
  if (q.fetch_row(row) && !row[0].empty()) {
    auto v = stof(row[0]) / 1000000.;
    auto n = 0;
    while (v > 1000. && n < VOLUME_LEN) {
      v /= 1000.;
      ++n;
    }
    ss << ftos(llround(v * 100.) / 100., 6, 2, ' ') << " " << vlist[n];
    json = "\"full\": \"" + ftos(llround(v * 100.) / 100., 2) + " " + vlist[n] +
        "\"";
  }
  if (q2.num_rows() > 1) {
    ss << " <span class=\"fs13px\">(Entire dataset)</span><br /><span id=\"D" <<
        swp_cnt << "\"><a href=\"javascript:swapDivs(" << swp_cnt << ", " <<
        swp_cnt + 1 << ")\"><img src=\"/images/bluetriangle.gif\" width=\"12\" "
        "height=\"15\" border=\"0\" title=\"Expand dataset product volume "
        "list\"><font size=\"-1\">Volume details by dataset product</font></a>"
        "</span><span style=\"visibility: hidden; position: absolute; top: 0\" "
        "id=\"D" << swp_cnt + 1 << "\"><a href=\"javascript:swapDivs(" <<
        swp_cnt + 1 << ", " << swp_cnt << ")\" title=\"Collapse dataset "
        "product volume list\"><img src=\"/images/bluetriangle90.gif\" width=\""
        "15\" height=\"12\" border=\"0\"></a><span class=\"fs13px\">Volume "
        "details by dataset product:";
    swp_cnt += 2;
    string j;
    for (const auto& r2: q2) {
      auto g = r2[1].empty() ? r2[2] : r2[1];
      replace_all(g, "\"", "\\\"");
      auto v = stof(r2[0]) / 1000000.;
      auto n = 0;
      while (v > 1000. && n < VOLUME_LEN) {
        v /= 1000.;
        ++n;
      }
      ss << "<div style=\"margin-left: 10px\">" << g << ": " << ftos(llround(v *
          100.) / 100., 6, 2, ' ') << " " << vlist[n] << "</div>";
      append(j, "{\"volume\": \"" + ftos(llround(v * 100.) / 100., 2) + " " +
          vlist[n] + "\", \"group\": \"" + g + "\"}", ", ");
    }
    ss << "</span></span>";
    if (!json.empty()) {
      json += ", \"groups\": [" + j + "]";
    }
  }
//  tdoc.add_replacement("__VOLUME__", ss.str());
  if (!json.empty()) {
    update_wagtail("volume", "{" + json + "}", F);
  }
}

//void add_related_websites(TokenDocument& tdoc) {
void add_related_websites() {
  static const string F = this_function_label(__func__);
  auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/relatedResource");
  string json;
  if (!elist.empty()) {
/*
    if (elist.size() > 1) {
      tdoc.add_replacement("__WEB_SITES_VALIGN__", "top");
    } else {
      tdoc.add_replacement("__WEB_SITES_VALIGN__", "bottom");
    }
*/
    stringstream ss;
    for (const auto& e : elist) {
      auto d = e.content();
      trim(d);
      if (d.back() == '.' && d[d.length() - 2] != '.') {
        chop(d);
      }
      auto url = e.attribute_value("url");
      ss << "<a href=\"" << url << "\">";
      auto is_local = false;
      if (has_beginning(url, "http://rda.ucar.edu") || has_beginning(url,
          "https://rda.ucar.edu") || has_beginning(url, "http://dss.ucar.edu")
          || has_beginning(url, "https://dss.ucar.edu")) {
        is_local = true;
      }
      if (!is_local) {
        ss << "<span class=\"italic\">";
      }
      ss << d;
      if (!is_local) {
        ss << "</span>";
      }
      ss << "</a><br />";
      append(json, "{\"description\": \"" + d + "\", \"url\": \"" + url + "\"}",
          ", ");
    }
//    tdoc.add_if("__HAS_RELATED_WEB_SITES__");
//    tdoc.add_replacement("__RELATED_WEB_SITES__", ss.str());
  }
  update_wagtail("related_rsrc_list", "[" + json + "]", F);
}

//void add_related_datasets(TokenDocument& tdoc) {
void add_related_datasets() {
  static const string F = this_function_label(__func__);
  auto elist = g_xdoc.element_list("OAI-PMH/GetRecord/record/metadata/"
      "dsOverview/relatedDataset");
  if (!elist.empty()) {
/*
    if (elist.size() > 1) {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__", "top");
    } else {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__", "bottom");
    }
*/
    elist.sort(
    [](XMLElement& left, XMLElement& right) -> bool {
      if (left.attribute_value("ID") <= right.attribute_value("ID")) {
        return true;
      }
      return false;
    });
    string s, json;
    for (const auto& ele : elist) {
      LocalQuery q("dsid, title", "search.datasets", "dsid = '" + ng_gdex_id(ele.
          attribute_value("ID")) + "' and type in ('P', 'H')");
      if (q.submit(g_metadata_server) < 0) {
        log_error2("query: " + q.show() + " returned error: " + q.error(), F,
            "dsgen", USER);
      }
      Row row;
      if (q.fetch_row(row)) {
        s += "<tr valign=\"top\"><td><a href=\"/datasets/" + row[0] +
            "#description\">" + row[0] + "</a></td><td>-</td><td>" + row[1] +
            "</td></tr>";
      }
      auto title = row[1];
      replace_all(title, "\"", "\\\"");
      append(json, "{\"dsid\": \"" + row[0] + "\", \"title\": \"" + title +
          "\"}", ", ");
    }
//    tdoc.add_if("__HAS_RELATED_DATASETS__");
//    tdoc.add_replacement("__RELATED_DATASETS__", s);
    update_wagtail("related_dslist", "[" + json + "]", F);
  }
}

//void add_more_details(TokenDocument& tdoc) {
void add_more_details() {
  static const string F = this_function_label(__func__);
  if (exists_on_server(metautils::directives.web_server, "/data/web/"
      "datasets/" + metautils::args.dsid + "/metadata/detailed.html")) {
//    tdoc.add_if("__HAS_MORE_DETAILS__");
  }
}

void add_data_license() {
  static const string F = this_function_label(__func__);
  auto id = g_xdoc.element("OAI-PMH/GetRecord/record/metadata/dsOverview/"
      "dataLicense").content();
  if (id.empty()) {
    id = "CC-BY-4.0";
  }
  LocalQuery q("name, url, img_url", "wagtail2.home_datalicense", "id = '" + id +
      "'");
  if (q.submit(g_wagtail_server) < 0) {
    log_error2("unable to retrieve data license - error: '" + g_wagtail_server.
        error() + "'", F, "dsgen", USER);
  }
  if (q.num_rows() == 0) {
    log_error2("no data license for id: '" + id + "'", F, "dsgen", USER);
  }
  Row row;
  if (!q.fetch_row(row)) {
    log_error2("unable to retrieve data license - error: '" + q.error() + "'",
        F, "dsgen", USER);
  }
  update_wagtail("data_license", "{\"name\": \"" + row[0] + "\", \"url\": \"" +
      row[1] + "\", \"img_url\": \"" + row[2] + "\"}", F);
}

void generate_description(string type, string tdir_name) {
  static const string F = this_function_label(__func__);
/*
  ofstream ofs;
  if (g_dataset_type == "W") {
    open_output(ofs, tdir_name + "/test_description.html");
  } else {
    open_output(ofs, tdir_name + "/description.html");
  }
  if (!ofs.is_open()) {
    log_error2("unable to open output for 'description.html'", F, "dsgen",
        USER);
  }
  auto tdoc = TokenDocument("/glade/u/home/rdadata/share/templates/"
      "dsgen_description.tdoc");
  if (!tdoc) {
    tdoc.reset("/usr/local/decs/share/templates/dsgen_description.tdoc");
    if (!tdoc) {
      log_error2("description template not found or unavailable", F, "dsgen",
          USER);
    }
  }
*/
  update_wagtail("dstype", g_dataset_type, F);
//  add_abstract(tdoc); //wagtail
//  exit_if_dead_dataset(tdoc, tdir_name, ofs);
exit_if_dead_dataset(tdir_name);

/*
  if (g_dataset_type == "I") {
    ofs << "<ul>This dataset has been removed from public view.  If you have questions about this dataset, please contact the specialist that is named above.</ul>" << endl;
    ofs.close();
    return;
  }
*/

  // initializations
  size_t swp_cnt = 0; // count for swapped divs
  vector<string> formats, data_types;
  auto found_content_metadata = false;
  initialize(formats, data_types, found_content_metadata);
//  tdoc.add_replacement("__DSNUM__", metautils::args.dsid);

//  add_acknowledgement(tdoc); //wagtail
add_acknowledgement(); //wagtail
//  auto grouped_periods = add_temporal_range(tdoc, swp_cnt); // wagtail
auto grouped_periods = add_temporal_range(swp_cnt); // wagtail
//  add_update_frequency(tdoc); // wagtail
add_update_frequency(); // wagtail
//  add_access_restrictions(tdoc); // wagtail
add_access_restrictions(); // wagtail
//  add_usage_restrictions(tdoc); // wagtail
add_usage_restrictions(); // wagtail
//  add_variables(tdoc, swp_cnt); // wagtail
add_variables(swp_cnt); // wagtail
//  add_vertical_levels(tdoc, data_types, found_content_metadata); // wagtail
add_vertical_levels(data_types, found_content_metadata); // wagtail
  if (!found_content_metadata) {
//    add_temporal_frequency(tdoc); // wagtail
add_temporal_frequency(); // wagtail
  }
//  add_data_types(tdoc, data_types, found_content_metadata); // wagtail
add_data_types(data_types, found_content_metadata); // wagtail
//  add_spatial_coverage(tdoc, data_types, found_content_metadata,
add_spatial_coverage(data_types, found_content_metadata,
     grouped_periods, swp_cnt); // wagtail
//  add_contributors(tdoc); // wagtail
add_contributors(); // wagtail
//  add_related_websites(tdoc); // wagtail
add_related_websites(); // wagtail
//  add_publications(tdoc, g_xdoc); // wagtail
add_publications(g_xdoc); // wagtail
//  add_data_volume(tdoc, swp_cnt); // wagtail
add_data_volume(swp_cnt); // wagtail
//  add_data_formats(tdoc, formats, found_content_metadata); // wagtail
add_data_formats(formats, found_content_metadata); // wagtail
//  add_related_datasets(tdoc); // wagtail
add_related_datasets(); // wagtail
//  add_more_details(tdoc);
//  add_data_citations(tdoc); // wagtail
add_data_citations(); // wagtail
  add_data_license();
/*
  ofs << tdoc;
  ofs.close();
*/
}

void show_usage() {
  cerr << "usage: dsgen nnn.n" << endl;
  cerr << "\noptions:" << endl;
  cerr << "  --no-dset-waf  don't add the dataset to the queue for the DSET WAF"
      << endl;
}

int main(int argc, char **argv) {
std::cerr << unixutils::host_name() << std::endl;
  if (argc != 2 && argc != 3) {
    show_usage();
    exit(1);
  }
  const string F = this_function_label(__func__);
  auto next = 1;
  if (string(argv[next]) == "--no-dset-waf") {
    no_dset_waf = true;
    ++next;
  }
  metautils::args.dsid = argv[next];
  if (metautils::args.dsid == "--help") {
    show_usage();
    exit(0);
  }
  metautils::args.dsid = ng_gdex_id(metautils::args.dsid);
  if (metautils::args.dsid >= "d999000") {
    no_dset_waf = true;
  }
  g_ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
  metautils::args.args_string = unixutils::unix_args_string(argc, argv);
  metautils::read_config("dsgen", USER, false);
std::cerr << "TEMP PATH=" << metautils::directives.temp_path << std::endl;
  if (!temp_dir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, "dsgen", USER);
  }
std::cerr << "METADB Password=" << metautils::directives.metadb_config.password << std::endl;
  g_metadata_server.connect(metautils::directives.metadb_config);
  LocalQuery q("select type from search.datasets where dsid in " + g_ds_set);
  Row row;
  if (q.submit(g_metadata_server) < 0 || !q.fetch_row(row)) {
    log_error2("unable to determine dataset type", F, "dsgen", USER);
  }
  if (row[0] == "W") {

    // dataset is a work in progress, so do nothing
    g_metadata_server.disconnect();
    exit(0);
  }
  g_dataset_type = row[0];
  if (!no_dset_waf) {
    if (g_dataset_type != "P" && g_dataset_type != "H") {
      no_dset_waf = true;
    }
  }
  TempDir d;
  if (!d.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary document directory", F, "dsgen",
        USER);
  }
  g_wagtail_server.connect(metautils::directives.wagtail_config);
  generate_index(d.name());
  if (g_dataset_type != "I") {
    generate_description(g_dataset_type, d.name());
    g_xdoc.close();
  }
  if (!no_dset_waf) {
    if (g_metadata_server.insert(
          "metautil.dset_waf",
          "dsid, uflag",
          "'" + metautils::args.dsid + "', ''",
          "(dsid, uflag) do update set uflag = excluded.uflag"
          ) < 0) {
      metautils::log_warning("not marked for DSET WAF update", "dsgen", USER);
    }
  }
  g_wagtail_server.disconnect();
  g_metadata_server.disconnect();
  sync_dataset_files(d.name());
}
