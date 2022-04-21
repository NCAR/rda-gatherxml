#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <regex>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <MySQL.hpp>
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

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::get;
using std::list;
using std::make_tuple;
using std::map;
using std::ofstream;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::stof;
using std::stoi;
using std::string;
using std::stringstream;
using std::to_string;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::capitalize;
using strutils::chop;
using strutils::ftos;
using strutils::itos;
using strutils::replace_all;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;
using strutils::to_capital;
using strutils::trim;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";
extern const string USER = getenv("USER");

TempDir temp_dir;
XMLDocument xdoc;
MySQL::Server server;
string dataset_type;
bool no_dset_waf = false;

unordered_map<string, string> wagtail_db{
    { "citations", "dataset_citation_datasetcitationpage" },
    { "num_citations", "dataset_citation_datasetcitationpage" },
    { "abstract", "dataset_description_datasetdescriptionpage" },
    { "contributors", "dataset_description_datasetdescriptionpage" },
    { "data_formats", "dataset_description_datasetdescriptionpage" },
    { "dsdoi", "dataset_description_datasetdescriptionpage" },
    { "dstitle", "dataset_description_datasetdescriptionpage" },
    { "related_dslist", "dataset_description_datasetdescriptionpage" },
    { "related_rsrc_list", "dataset_description_datasetdescriptionpage" },
    { "update_freq", "dataset_description_datasetdescriptionpage" },
    { "variables", "dataset_description_datasetdescriptionpage" },
    { "volume", "dataset_description_datasetdescriptionpage" },
};

void update_wagtail(string column, string insert_value, string caller) {
  if (wagtail_db.find(column) == wagtail_db.end()) {
    log_error2("unknown wagtail column '" + column + "'", caller, "dsgen",
        USER);
  }
  if (server.update("wagtail." + wagtail_db[column], column + " = '" +
      sql_ready(insert_value) + "'", "dsid = '" + metautils::args.dsnum + "'") <
      0) {
    log_error2("failed to update wagtail." + wagtail_db[column] + " " + column +
        ": error '" + server.error() + "'", caller, "dsgen", USER);
  }
}

void generate_index(string type, string tdir_name) {
  static const string F = this_function_label(__func__);
  ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name + "/test_index.html").c_str());
  } else {
    ofs.open((tdir_name + "/index.html").c_str());
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
  tdoc.add_replacement("__DSNUM__", metautils::args.dsnum);
  if (dataset_type == "I") {
    tdoc.add_if("__IS_INTERNAL_DATASET__");
  } else {
    struct stat st;
    string xf;
    if (stat(("/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/"
        "dsOverview.xml").c_str(), &st) == 0) {
      xf = "/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/"
          "dsOverview.xml";
    } else {
      xf = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
          metautils::args.dsnum + "/metadata/dsOverview.xml", temp_dir.name());
      if (xf.empty()) {
        log_error2("dsOverview.xml does not exist for " + metautils::args.dsnum,
            F, "dsgen", USER);
      }
    }
    if (!xdoc.open(xf)) {
      log_error2("unable to open dsOverview.xml for " + metautils::args.dsnum +
          "; parse error: '" + xdoc.parse_error() + "'", F, "dsgen", USER);
    }
    stringstream ss;
    if (!metadataExport::export_to_dc_meta_tags(ss, metautils::args.dsnum, xdoc,
        0)) {
      log_error2("unable to export DC meta tags: '" + myerror + "'", F, "dsgen",
          USER);
    }
    tdoc.add_replacement("__DC_META_TAGS__", ss.str());
    auto e = xdoc.element("dsOverview/title");
    auto ti = e.content();
    tdoc.add_replacement("__TITLE__", ti);
    update_wagtail("dstitle", ti, F);
    if (!metadataExport::export_to_json_ld(ss, metautils::args.dsnum, xdoc,
        0)) {
      log_error2("unable to export JSON-LD metadata", F, "dsgen", USER);
    }
    tdoc.add_replacement("__JSON_LD__", ss.str());
    e = xdoc.element("dsOverview/logo");
    if (!e.content().empty()) {
      auto sp = split(e.content(), ".");
      auto sp2 = split(sp[sp.size() - 2], "_");
      auto w = stoi(sp2[sp2.size() - 2]);
      auto h = stoi(sp2[sp2.size() - 1]);
      tdoc.add_replacement("__LOGO_IMAGE__", e.content());
      tdoc.add_replacement("__LOGO_WIDTH__", itos(lroundf(w * 70. / h)));
    } else {
      tdoc.add_replacement("__LOGO_IMAGE__", "default_200_200.png");
      tdoc.add_replacement("__LOGO_WIDTH__", "70");
    }
    MySQL::LocalQuery query("doi", "dssdb.dsvrsn", "dsid = 'ds" + metautils::
        args.dsnum + "' and status = 'A'");
    MySQL::Row row;
    string ds;
    if (query.submit(server) == 0 && query.fetch_row(row) && !row[0].empty()) {
      ds = "&nbsp;|&nbsp;<span class=\"blue\">DOI: " + row[0] + "</span>";
      update_wagtail("dsdoi", row[0], F);
    }
    tdoc.add_replacement("__DOI_SPAN__", ds);
    if (dataset_type == "D") {
      tdoc.add_if("__IS_DEAD_DATASET__");
    }
  }
  ofs << tdoc;
  ofs.close();
  delete t;
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
    auto ilv = stoi(lv);
    auto irv = stoi(rv);
    if (lu == ru) {
      if (lu == "mbar" || lu == "deg K") {
        if (ilv >= irv) {
          return true;
        }
        return false;
      } else {
        if (ilv <= irv) {
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

string create_table_from_strings(list<string> list, size_t max_columns, string
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
    trim(s);
    auto i = s.find("<p");
    if (i == string::npos) {
      i = s.find("<P");
    }
    if (i == 0) {
      auto i2 = s.find(">", i);
      s.insert(i2, " style=\"margin: 0px; padding: 0px\"");
    }
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
    size_t i = s.find("<p");
    if (i == string::npos) {
      i = s.find("<P");
    }
    if (i == 0) {
      auto i2 = s.find(">", i);
      s.insert(i2, " style=\"margin: 0px; padding: 0px\"");
    }
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">" <<
        section_title << ":</td><td>" << s << "</td></tr>" << endl;
  }
}

void add_publications(TokenDocument& tdoc, XMLDocument& xdoc) {
  auto rlist = xdoc.element_list("dsOverview/reference");
  stringstream ss;
  if (rlist.size() > 0) {
    tdoc.add_if("__HAS_PUBLICATIONS__");
    rlist.sort(compare_references);
    for (const auto& r : rlist) {
      ss << "<div>" << r.element("authorList").content() << ", " << r.element(
          "year").content() << ": ";
      auto ptyp = r.attribute_value("type");
      if (ptyp == "journal") {
        auto pd = r.element("periodical");
        auto url = r.element("url").content();
        auto ti = r.element("title").content();
        if (!url.empty()) {
          ss << "<a href=\"" << url << "\">" << ti << "</a>";
        } else {
          ss << ti;
        }
        if (ti.back() == '?') {
          ss << ".";
        }
        ss << "  <i>" << pd.content() << "</i>, ";
        if (pd.attribute_value("pages") == "0-0") {
          if (pd.attribute_value("number") == "0") {
            ss << "Submitted";
          } else if (pd.attribute_value("number") == "1") {
            ss << "Accepted";
          } else if (pd.attribute_value("number") == "2") {
            ss << "In Press";
          }
        } else {
          ss << "<b>" << pd.attribute_value("number") << "</b>, ";
          auto pg = pd.attribute_value("pages");
          if (regex_search(pg, regex("^AGU:"))) {
            ss << pg.substr(4);
          } else {
            auto sp = split(pg, "-"); 
            if (sp.size() == 2 && sp[0] == sp[1]) {
              ss << sp[0];
            } else {
              ss << pd.attribute_value("pages");
            }
          }
        }
        auto doi = r.element("doi").content();
        if (!doi.empty()) {
          ss << " (DOI: " << doi << ")";
        }
        ss << ".";
      } else if (ptyp == "preprint") {
        auto cnf = r.element("conference");
        auto url = r.element("url").content();
        if (!url.empty()) {
          ss << "<a href=\"" << url << "\">" << r.element("title").content() <<
              "</a>";
        } else {
          ss << r.element("title").content();
        }
        ss << ".  <i>Proceedings of the " << cnf.content() << "</i>, " << cnf.
            attribute_value("host") << ", " << cnf.attribute_value("location");
        auto pg = cnf.attribute_value("pages");
        if (!pg.empty()) {
          ss << ", " << pg;
        }
        auto doi = r.element("doi").content();
        if (!doi.empty()) {
          ss << " (DOI: " << doi << ")";
        }
        ss << ".";
      } else if (ptyp == "technical_report") {
        auto org = r.element("organization");
        auto url = r.element("url").content();
        if (!url.empty()) {
          ss << "<i><a href=\"" << url << "\">" << r.element("title").content()
              << "</a>.</i>";
        } else {
          ss << "<i>" << r.element("title").content() << ".</i>";
        }
        ss << "  ";
        auto id = org.attribute_value("reportID");
        if (!id.empty()) {
          ss << id << ", ";
        }
        ss << org.content();
        auto pg = org.attribute_value("pages");
        if (pg != "-99") {
          ss << ", " << pg << " pp.";
        }
        auto doi = r.element("doi").content();
        if (!doi.empty()) {
          ss << " (DOI: " << doi << ").";
        }
      } else if (ptyp == "book") {
        auto pb = r.element("publisher");
        ss << "<i>" << r.element("title").content() << "</i>. " << pb.content()
            << ", " << pb.attribute_value("place");
        auto doi = r.element("doi").content();
        if (!doi.empty()) {
          ss << " (DOI: " << doi << ")";
        }
        ss << ".";
      } else if (ptyp == "book_chapter") {
        auto bk = r.element("book");
        ss << "\"" << r.element("title").content() << "\", in " << bk.content()
            << ". Ed. " << bk.attribute_value("editor") << ", " << bk.
            attribute_value("publisher") << ", ";
        if (bk.attribute_value("pages") == "0-0") {
          ss << "In Press";
        } else {
          ss << bk.attribute_value("pages");
        }
        auto doi = r.element("doi").content();
        if (!doi.empty()) {
          ss << " (DOI: " << doi << ")";
        }
        ss << ".";
      }
      ss << "</div>" << endl;
      auto ann = r.element("annotation").content();
      if (!ann.empty() > 0) {
        ss << "<div style=\"margin-left: 15px; color: #5f5f5f\">" << ann <<
            "</div>" << endl;
      }
      ss << "<br />" << endl;
    }
  }
  auto ulist = xdoc.element_list("dsOverview/referenceURL");
  if (ulist.size() > 0) {
    if (ss.str().empty()) {
      tdoc.add_if("__HAS_PUBLICATIONS__");
    }
    for (const auto& u : ulist) {
      ss << "<a href=\"" << u.attribute_value("url") << "\">" << u.content() <<
          "</a><br>" << endl;
    }
  }
  if (!ss.str().empty()) {
    tdoc.add_replacement("__PUBLICATIONS__", ss.str());
  }
}

void add_data_formats(TokenDocument& tdoc, vector<string>& formats, bool
    found_content_metadata) {
  static const string F = this_function_label(__func__);
  if (!found_content_metadata) {
    auto flist=xdoc.element_list("dsOverview/contentMetadata/format");
    for (const auto& f : flist) {
      formats.emplace_back(f.content() + "<!>" + f.attribute_value("href"));
    }
  }
  struct stat st;
  XMLDocument fdoc;
  if (stat("/usr/local/www/server_root/web/metadata", &st) == 0) {
    fdoc.open("/usr/local/www/server_root/web/metadata/FormatReferences.xml");
  } else {
    auto f = unixutils::remote_web_file("https://rda.ucar.edu/metadata/"
        "FormatReferences.xml", temp_dir.name());
    fdoc.open(f);
  }
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
    if (regex_search(d, regex("^proprietary_"))) {
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
      if (!regex_search(u, regex("^http://rda.ucar.edu"))) {
        df += "<i>";
      }
    }
    replace_all(d, "_", " ");
    df += d;
    append(j, "\"description\": \"" + d + "\"", ", ");
    if (!u.empty()) {
      df += "</a>";
      if (!regex_search(u, regex("^http://rda.ucar.edu"))) {
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
  tdoc.add_replacement("__DATA_FORMATS__", df);
  update_wagtail("data_formats", "[" + json + "]", F);
}

void add_citations(TokenDocument& tdoc) {
  static const string F = this_function_label(__func__);
  MySQL::LocalQuery qc("select distinct d.DOI_work from citation"
      ".data_citations as d left join dssdb.dsvrsn as v on v.doi = d.DOI_data "
      "where v.dsid = 'ds" + metautils::args.dsnum + "'");
  if (qc.submit(server) < 0) {
    return;
  }
  vector<tuple<string, string>> clist;
  for (const auto& rc : qc) {
    auto doi = rc[0];
    MySQL::LocalQuery qw("select title, pub_year, type, publisher from citation"
        ".works where DOI = '" + doi + "'");
    MySQL::Row rw;
    if (qw.submit(server) == 0 && qw.fetch_row(rw)) {
      auto ti = htmlutils::unicode_escape_to_html(rw[0]);
      auto yr = rw[1];
      auto typ = rw[2];
      MySQL::LocalQuery qwa("select last_name, first_name, middle_name from "
          "citation.works_authors where ID = '" + doi + "' and ID_type = 'DOI' "
          "order by sequence");
      if (qwa.submit(server) == 0) {
        string cit;
        size_t n = 1;
        for (const auto& rwa : qwa) {
          if (cit.empty()) {
            cit += htmlutils::unicode_escape_to_html(rwa[0]);
            if (!rwa[1].empty()) {
              cit += ", " + rwa[1].substr(0, 1) + ".";
            }
            if (!rwa[2].empty()) {
              cit += " " + rwa[2].substr(0, 1) + ".";
            }
          } else {
            cit += ", ";
            if (n == qwa.num_rows()) {
              cit += "and ";
            }
            if (!rwa[1].empty()) {
              cit += rwa[1].substr(0, 1) + ". ";
            }
            if (!rwa[2].empty()) {
              cit += rwa[2].substr(0, 1) + ". ";
            }
            cit += htmlutils::unicode_escape_to_html(rwa[0]);
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
              cit += "\", in ";
              MySQL::LocalQuery qcw("select pages, ISBN from citation"
                  ".book_chapter_works where DOI = '" + doi + "'");
              MySQL::Row rcw;
              if (qcw.submit(server) == 0 && qcw.fetch_row(rcw)) {
                MySQL::LocalQuery qbw("select title, publisher from citation"
                    ".book_works where ISBN = '" + rcw[1] + "'");
                MySQL::Row rbw;
                if (qbw.submit(server) == 0 && qbw.fetch_row(rbw)) {
                  cit += htmlutils::unicode_escape_to_html(rbw[0]) + ". Ed. ";
                  qwa.set("select first_name, middle_name, last_name from "
                      "citation.works_authors where ID = '" + rcw[1] + "' and "
                      "ID_type = 'ISBN' order by sequence");
                  if (qwa.submit(server) == 0) {
                    size_t n = 1;
                    for (const auto& rwa : qwa) {
                      if (n > 1) {
                        cit += ", ";
                        if (n == qwa.num_rows()) {
                          cit += "and ";
                        }
                      }
                      cit += rwa[0].substr(0, 1) +" . ";
                      if (!rwa[1].empty()) {
                        cit += rwa[1].substr(0, 1) + ". ";
                      }
                      cit += htmlutils::unicode_escape_to_html(rwa[2]);
                      ++n;
                    }
                    cit += ", " + htmlutils::unicode_escape_to_html(rbw[1]) +
                        ", " + rcw[0] + ".";
                  } else {
                    cit = "";
                  }
                } else {
                  cit = "";
                }
              } else {
                cit = "";
              }
              break;
            }
            case 'J': {
              cit += ". ";
              MySQL::LocalQuery qjw("select pub_name, volume, pages from "
                  "citation.journal_works where DOI = '" + doi + "'");
              MySQL::Row rjw;
              if (qjw.submit(server) == 0 && qjw.fetch_row(rjw)) {
                cit += "<em>" + htmlutils::unicode_escape_to_html(rjw[0]) +
                    "</em>";
                if (!rjw[1].empty()) {
                  cit += ", <strong>" + rjw[1] + "</strong>";
                }
                if (!rjw[2].empty()) {
                  cit += ", " + rjw[2];
                }
                cit += ", <a href=\"https://doi.org/" + doi + "\" target=\"_doi"
                    "\">https://doi.org/" + doi + "</a>";
              } else {
                cit = "";
              }
              break;
            }
            case 'P': {
              cit += ". <em>";
              MySQL::LocalQuery qpw("select pub_name, pages from citation"
                  ".proceedings_works where DOI = '" + doi + "'");
              MySQL::Row rpw;
              if (qpw.submit(server) == 0 && qpw.fetch_row(rpw)) {
                auto d = htmlutils::unicode_escape_to_html(rpw[0]) + "</em>";
                if (!rw[3].empty()) {
                  d += ", " + rw[3];
                }
                if (!rpw[1].empty()) {
                  d += ", " + rpw[1];
                }
                cit += d + ", <a href=\"https://doi.org/" + doi + "\" target="
                    "\"_doi\">https://doi.org/" + doi + "</a>";
              } else {
                cit = "";
              }
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
    tdoc.add_if("__HAS_DATA_CITATIONS__");
    if (clist.size() > 1) {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>" + itos(clist.
          size()) + "</strong> times");
    } else {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>" + itos(clist.
          size()) + "</strong> time");
    }
    std::sort(clist.begin(), clist.end(),
    [](const tuple<string, string>& left, const tuple<string, string>& right)
        -> bool {
      if (get<0>(left) > get<0>(right)) {
        return true;
      } else if (get<0>(left) < get<0>(right)) {
        return false;
      }
      return (get<1>(left) < get<1>(right));
    });
    unordered_set<string> yrs;
    string json;
    for (const auto& c : clist) {
      auto pub_year=get<0>(c);
      if (yrs.find(pub_year) == yrs.end()) {
        tdoc.add_repeat("__DATA_CITER__", "CITATION[!]" + get<1>(c) +
            "<!>YEAR[!]" + pub_year);
        append(json, "{\"year\": " + pub_year + ", \"publications\": [\"" +
            substitute(get<1>(c), "\"", "\\\\\"") + "\"", "]}, ");
        yrs.emplace(pub_year);
      } else {
        tdoc.add_repeat("__DATA_CITER__", "CITATION[!]" + get<1>(c));
        append(json, "\"" + substitute(get<1>(c), "\"", "\\\\\"") + "\"", ", ");
      }
    }
    update_wagtail("num_citations", to_string(clist.size()), F);
//log_error2("[" + json + "]}]", F, "dsgen", USER);
    update_wagtail("citations", "[" + json + "]}]", F);
  } else {
    tdoc.add_replacement("__NUM_DATA_CITATIONS__", "<strong>0</strong> times");
  }
}

void generate_description(string type, string tdir_name) {
  static const string F = this_function_label(__func__);
  string dsnum2 = substitute(metautils::args.dsnum, ".", "");
  ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name + "/test_description.html").c_str());
  } else {
    ofs.open((tdir_name + "/description.html").c_str());
  }
  if (!ofs.is_open()) {
    log_error2("unable to open output for 'description.html'", F, "dsgen",
        USER);
  }
  auto t = new TokenDocument("/glade/u/home/rdadata/share/templates/"
      "dsgen_description.tdoc");
  if (!*t) {
    t = new TokenDocument("/usr/local/dss/share/templates/dsgen_description"
        ".tdoc");
    if (!*t) {
      log_error2("description template not found or unavailable", F, "dsgen",
          USER);
    }
  }
  auto &tdoc = *t;

  // dataset abstract
  tdoc.add_replacement("__ABSTRACT__", text_field_from_element(xdoc.element(
      "dsOverview/summary")));
  update_wagtail("abstract", text_field_from_element(xdoc.element(
      "dsOverview/summary")), F);
  if (dataset_type == "D") {
    tdoc.add_if("__IS_DEAD_DATASET__");
    ofs << tdoc;
    ofs.close();
    return;
  }
/*
  if (dataset_type == "I") {
    ofs << "<ul>This dataset has been removed from public view.  If you have questions about this dataset, please contact the specialist that is named above.</ul>" << endl;
    ofs.close();
    return;
  }
*/
  auto dblist = metautils::cmd_databases("dsgen", "x");
  if (dblist.size() == 0) {
    log_error2("empty CMD database list", F, "dsgen", USER);
  }
  vector<string> formats, data_types;
  auto found_content_metadata = false;
  for (const auto& db : dblist) {
    string nm, dt;
    std::tie(nm, dt) = db;
    if (nm[0] != 'V' && table_exists(server, nm + ".ds" + dsnum2 +
        "_primaries2")) {
      MySQL::LocalQuery q("select distinct format from " + nm + ".formats as f "
          "left join " + nm + ".ds" + dsnum2 + "_primaries2 as d on d"
          ".format_code = f.code where !isnull(d.format_code)");
      if (q.submit(server) < 0) {
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
  tdoc.add_replacement("__DSNUM__", metautils::args.dsnum);

  // custom acknowledgements for the dataset
  auto ack = text_field_from_element(xdoc.element("dsOverview/"
      "acknowledgement"));
  if (!ack.empty()) {
    tdoc.add_if("__HAS_ACKNOWLEDGEMENTS__");
    tdoc.add_replacement("__ACKNOWLEDGEMENT__", ack);
  }

  // temporal range(s)
  MySQL::LocalQuery qt("select dsid from dssdb.dsgroup where dsid = 'ds" +
      metautils::args.dsnum + "'");
  if (qt.submit(server) < 0) {
    log_error2("query: " + qt.show() + " returned error: " + qt.error(), F,
        "dsgen", USER);
  }
  if (qt.num_rows() > 0) {
    qt.set("select p.date_start, p.time_start, p.start_flag, p.date_end, "
        "p.time_end, p.end_flag, p.time_zone, g.title, g.grpid from dssdb"
        ".dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p"
        ".gindex = g.gindex) where p.dsid = 'ds" + metautils::args.dsnum +
        "' and g.pindex = 0 and date_start > '0000-00-00' and date_start < "
        "'3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' "
        "union select p.date_start, p.time_start, p.start_flag, p.date_end, p"
        ".time_end, p.end_flag, p.time_zone, g2.title, g.grpid from dssdb"
        ".dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p"
        ".gindex = g.gindex) left join dssdb.dsgroup as g2 on (p.dsid = g2"
        ".dsid and g.pindex = g2.gindex) where p.dsid = 'ds" + metautils::
        args.dsnum + "' and date_start > '0000-00-00' and date_start < '3000-01"
        "-01' and date_end > '0000-00-00' and date_end < '3000-01-01' and "
        "!isnull(g2.title) order by title");
  } else {
    qt.set("select date_start, time_start, start_flag, date_end, time_end, "
        "end_flag, time_zone, NULL, NULL from dssdb.dsperiod where dsid = 'ds" +
        metautils::args.dsnum + "' and date_start > '0000-00-00' and "
        "date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < "
        "'3000-01-01'");
  }
  if (qt.submit(server) < 0) {
    log_error2("query: " + qt.show() + " returned error: " + qt.error(), F,
        "dsgen", USER);
  }
  if (qt.num_rows() == 0) {
    qt.set("select date_start, time_start, start_flag, date_end, time_end, "
        "end_flag, time_zone, NULL, NULL from dssdb.dsperiod where dsid = "
        "'ds" + metautils::args.dsnum + "' and date_start > '0000-00-00' and "
        "date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < "
        "'3000-01-01'");
    if (qt.submit(server) < 0) {
      log_error2("query: " + qt.show() + " returned error: " + qt.error(), F,
          "dsgen", USER);
    }
  }
  auto dnum = 0;
  bool grouped_periods = false;
  unordered_map<string, string> grps, rfils, mfils;
  if (qt.num_rows() > 0) {
    tdoc.add_if("__HAS_TEMPORAL_RANGE__");
    if (qt.num_rows() > 1) {
      MySQL::LocalQuery qgp("distinct gindex", "dssdb.dsperiod", "dsid = 'ds" +
          metautils::args.dsnum + "'");
      if (qgp.submit(server) < 0) {
        log_error2("query: " + qgp.show() + " returned error: " + qgp.error(),
            F, "dsgen", USER);
      }
      if (qgp.num_rows() > 1) {
        grouped_periods = true;
        tdoc.add_if("__HAS_TEMPORAL_BY_GROUP1__");
        MySQL::LocalQuery qg("select gindex, title from dssdb.dsgroup where "
            "dsid = 'ds" + metautils::args.dsnum + "'");
        if (qg.submit(server) < 0) {
          log_error2("error: " + qg.error() + " while getting groups data", F,
              "dsgen", USER);
        }
        for (const auto& row : qg) {
          grps.emplace(row[0], row[1]);
        }
        MySQL::LocalQuery qwf("select wfile, tindex from dssdb.wfile where "
            "dsid = 'ds" + metautils::args.dsnum + "' and type = 'D' and "
            "status = 'P'");
        if (qwf.submit(server) < 0) {
          log_error2("error: " + qwf.error() + " while getting RDA files data",
              F, "dsgen", USER);
        }
        for (const auto& row : qwf) {
          rfils.emplace(row[0], row[1]);
        }
        MySQL::LocalQuery qmf("select code, webID from WGrML.ds" + dsnum2 +
            "_webfiles2");
        if (qmf.submit(server) == 0) {
          for (const auto& row : qmf) {
             mfils.emplace(row[0], row[1]);
          }
        }
        MySQL::LocalQuery qdt("select min(concat(date_start, ' ', "
            "time_start)), min(start_flag), max(concat(date_end, ' ', "
            "time_end)), min(end_flag), any_value(time_zone) from dssdb"
            ".dsperiod where dsid = 'ds" + metautils::args.dsnum + "' and "
            "date_start > '0000-00-00' and date_start < '3000-01-01' and "
            "date_end > '0000-00-00' and date_end < '3000-01-01' group by "
            "dsid");
        if (qdt.submit(server) < 0) {
          log_error2("query: " + qdt.show() + " returned error: " + qdt.error(),
              F, "dsgen", USER);
        }
        MySQL::Row rdt;
        qdt.fetch_row(rdt);
        auto sdt = metatranslations::date_time(rdt[0], rdt[1], rdt[4]);
        auto edt = metatranslations::date_time(rdt[2], rdt[3], rdt[4]);
        auto temporal = sdt;
        if (!edt.empty() && edt != sdt) {
          temporal += " to " + edt;
        }
        tdoc.add_replacement("__TEMPORAL_RANGE__", temporal);
        tdoc.add_replacement("__N_TEMPORAL__", itos(dnum));
        tdoc.add_replacement("__N_TEMPORAL1__", itos(dnum + 1));
        dnum += 2;
      }
    }
    map<string, tuple<string, string>> pdlist;
    for (const auto& row : qt) {
      auto sdt = metatranslations::date_time(row[0] + " " + row[1], row[2], row[
          6]);
      auto edt = metatranslations::date_time(row[3] + " " + row[4], row[5], row[
          6]);
      string k;
      if (!row[7].empty()) {
        k = row[7];
      } else {
        k = row[8];
      }
      if (pdlist.find(k) == pdlist.end()) {
        pdlist.emplace(k, make_tuple(sdt, edt));
      } else {
        string &start = get<0>(pdlist[k]);
        if (sdt < start) {
          start = sdt;
        }
        string &end = get<1>(pdlist[k]);
        if (edt > end) {
          end = edt;
        }
      }   
    }
    for (const auto& i : pdlist) {
      auto trng = get<0>(i.second);
      auto e = get<1>(i.second);
      if (!e.empty() && e != trng) {
        trng += " to " + e;
      }
      if (pdlist.size() > 1) {
        trng += " (" + i.first + ")";
        tdoc.add_repeat("__TEMPORAL_RANGE__", "<div style=\"margin-left: 10px"
            "\">" + trng + "</div>");
      } else {
        tdoc.add_repeat("__TEMPORAL_RANGE__", trng);
      }
    }
    if (grouped_periods) {
      tdoc.add_if("__HAS_TEMPORAL_BY_GROUP2__");
    }
  }

  // update frequency
  auto e = xdoc.element("dsOverview/continuingUpdate");
  if (e.attribute_value("value") == "yes") {
    tdoc.add_if("__HAS_UPDATE_FREQUENCY__");
    tdoc.add_replacement("__UPDATE_FREQUENCY__", capitalize(e.attribute_value(
        "frequency")));
    update_wagtail("update_freq", capitalize(e.attribute_value("frequency")),
        F);
  }

  // access restrictions
  e = xdoc.element("dsOverview/restrictions/access");
  auto a = e.to_string();
  replace_all(a, "<access>", "");
  replace_all(a, "</access>", "");
  auto idx = a.find("<p");
  if (idx == string::npos) {
    idx = a.find("<P");
  }
  if (idx != string::npos) {
    auto idx2 = a.find(">", idx);
    a.insert(idx2, " style=\"margin: 0px; padding: 0px\"");
  }
  if (!a.empty()) {
    tdoc.add_if("__HAS_ACCESS_RESTRICTIONS__");
    tdoc.add_replacement("__ACCESS_RESTRICTIONS__", a);
  }

  // usage restrictions
  e = xdoc.element("dsOverview/restrictions/usage");
  auto u = e.to_string();
  replace_all(u, "<usage>", "");
  replace_all(u, "</usage>", "");
  idx = u.find("<p");
  if (idx == string::npos) {
    idx = u.find("<P");
  }
  if (idx != string::npos) {
    auto idx2 = u.find(">", idx);
    u.insert(idx2, " style=\"margin: 0px; padding: 0px\"");
  }
  if (!u.empty()) {
    tdoc.add_if("__HAS_USAGE_RESTRICTIONS__");
    tdoc.add_replacement("__USAGE_RESTRICTIONS__", u);
  }

  // variables
  MySQL::LocalQuery qvar("select substring_index(path, ' > ', -1) as var from "
      "search.variables_new as v left join search.GCMD_sciencekeywords as g on "
      "g.uuid = v.keyword where v.vocabulary = 'GCMD' and v.dsid = '" +
      metautils::args.dsnum + "' order by var");
  if (qvar.submit(server) < 0) {
    log_error2("query: " + qvar.show() + " returned error: " + qvar.error(), F,
        "dsgen", USER);
  }
  list<string> l;
  string json;
  for (const auto& row : qvar) {
    l.emplace_back(capitalize(row[0]));
    append(json, "\"" + capitalize(row[0]) + "\"", ", ");
  }
  tdoc.add_replacement("__VARIABLES__", create_table_from_strings(l, 4,
      "#e1eaff", "#c8daff"));
  update_wagtail("variables", "[" + json + "]", F);
  auto elist = xdoc.element_list("dsOverview/contentMetadata/detailedVariables/"
      "detailedVariable");
  if (elist.size() > 0) {
    for (const auto& e : elist) {
      if (regex_search(e.content(), regex("^http://")) || regex_search(e.
          content(), regex("^https://"))) {
        tdoc.add_if("__HAS_DETAILED_VARIABLES__");
        tdoc.add_replacement("__DETAILED_VARIABLES_LINK__", e.content());
        break;
      }
    }
  }
  if (unixutils::exists_on_server(metautils::directives.web_server,
      "/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/grib.html",
      metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_GRIB_TABLE__");
    auto tb = "<div>GRIB parameter table:  <a href=\"/datasets/ds" + metautils::
        args.dsnum + "/#metadata/grib.html?_do=y\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server,
        "/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/grib.xml",
        metautils::directives.rdadata_home)) {
      tb += " | <a href=\"/datasets/ds" + metautils::args.dsnum + "/metadata/"
          "grib.xml\">XML</a></div>";
    }
    tdoc.add_replacement("__GRIB_TABLE__", tb);
  }
  if (unixutils::exists_on_server(metautils::directives.web_server, "/data/web/"
      "datasets/ds" + metautils::args.dsnum + "/metadata/grib2.html",
      metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_GRIB2_TABLE__");
    auto tb = "<div>GRIB2 parameter table:  <a href=\"/datasets/ds" +
        metautils::args.dsnum + "/#metadata/grib2.html?_do=y\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server, "/data/"
        "web/datasets/ds" + metautils::args.dsnum + "/metadata/grib2.xml",
        metautils::directives.rdadata_home)) {
      tb += " | <a href=\"/datasets/ds" + metautils::args.dsnum + "/metadata/"
          "grib2.xml\">XML</a></div>"; 
    }
    tdoc.add_replacement("__GRIB2_TABLE__", tb);
  }
  if (unixutils::exists_on_server(metautils::directives.web_server, "/data/web/"
      "datasets/ds" + metautils::args.dsnum + "/metadata/on84.html",
      metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_ON84_TABLE__");
    auto tb = "<div>ON84 parameter table:  <a href=\"/datasets/ds" +
        metautils::args.dsnum + "/metadata/on84.html\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server, "/data/"
        "web/datasets/ds" + metautils::args.dsnum + "/metadata/on84.html",
        metautils::directives.rdadata_home)) {
      tb += " | <a href=\"/datasets/ds" + metautils::args.dsnum + "/metadata/"
          "on84.xml\">XML</a></div>";
    }
    tdoc.add_replacement("__ON84_TABLE__", tb);
  }
  MySQL::LocalQuery qg("gindex, title", "dssdb.dsgroup", "dsid = 'ds" +
      metautils::args.dsnum + "' and pindex = 0 and dwebcnt > 0");
  if (qg.submit(server) == 0) {
    stringstream ss;
    for (const auto& row : qg) {
      if (unixutils::exists_on_server(metautils::directives.web_server, "/data/"
          "web/datasets/ds" + metautils::args.dsnum + "/metadata/customize"
          ".WGrML." + row[0], metautils::directives.rdadata_home)) {
        auto f = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
            metautils::args.dsnum + "/metadata/customize.WGrML." + row[0],
            temp_dir.name());
        if (!f.empty()) {
          std::ifstream ifs;
          char line[32768];
          ifs.open(f);
          ifs.getline(line, 32768);
          if (regex_search(line, regex("^curl_subset="))) {
            ifs.getline(line, 32768);
          }
          auto nv = stoi(line);
          list<string> vlist;
          for (int n = 0; n < nv; ++n) {
            ifs.getline(line, 32768);
            auto sp=split(line, "<!>");
            vlist.emplace_back(sp[1]);
          }
          ifs.close();
          if (vlist.size() > 0) {
            if (ss.tellp() == 0) {
              ss << "<div style=\"position: relative; overflow: hidden\">"
                  "<span id=\"D" << dnum << "\"><a href=\"javascript:swapDivs("
                  << dnum << ", " << dnum + 1 << ")\" title=\"Expand dataset "
                  "product variable list\"><img src=\"/images/bluetriangle"
                  ".gif\" width=\"12\" height=\"15\" border=\"0\"><span class="
                  "\"fs13px\">Variables by dataset product</span></a></span>"
                  "<span style=\"visibility: hidden; position: absolute; top: "
                  "0\" id=\"D" << dnum + 1 << "\"><a href=\"javascript:"
                  "swapDivs(" << dnum + 1 << ", " << dnum << ")\"><img src=\""
                  "/images/bluetriangle90.gif\" width=\"15\" height=\"12\" "
                  "border=\"0\" title=\"Collapse dataset product variable "
                  "list\"></a><span class=\"fs13px\">Variables by dataset "
                  "product:";
              dnum += 2;
            }
            ss << "<div style=\"margin-left: 10px\"><span id=\"D" << dnum <<
                "\"><a href=\"javascript:swapDivs(" << dnum << ", " << dnum + 1
                << ")\" title=\"Expand\"><img src=\"/images/bluetriangle.gif\" "
                "width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px"
                "\">Variable list for " << row[1] << "</span></a></span><span "
                "style=\"visibility: hidden; position: absolute; top: 0\" id=\""
                "D" << dnum + 1 << "\"><a href=\"javascript:swapDivs(" << dnum +
                1 << ", " << dnum << ")\"><img src=\"/images/bluetriangle90"
                ".gif\" width=\"15\" height=\"12\" border=\"0\" title=\""
                "Collapse\"></a><span class=\"fs13px\">Variable list for " <<
                row[1] << ":<div style=\"margin-left: 20px\">";
            dnum += 2;
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
      tdoc.add_if("__HAS_VARIABLES_BY_PRODUCT__");
      tdoc.add_replacement("__VARIABLES_BY_PRODUCT__", ss.str());
    }
  }

  // vertical levels
  if (found_content_metadata) {
    for (const auto& dt : data_types) {
      if (dt == "grid") {
        tdoc.add_if("__HAS_VERTICAL_LEVELS__");
        string v = "See the <a href=\"#metadata/detailed.html?_do=y&view=level"
            "\">detailed metadata</a> for level information";
        if (unixutils::exists_on_server(metautils::directives.web_server,
            "/data/web/datasets/ds" + metautils::args.dsnum + "/metadata/"
            "grib2_levels.html", metautils::directives.rdadata_home)) {
          v += "<br /><a href=\"/datasets/ds" + metautils::args.dsnum +
              "/#metadata/grib2_levels.html?_do=y\">GRIB2 level table</a>"; 
        }
        tdoc.add_replacement("__VERTICAL_LEVELS__", v);
        break;
      }
    }
  } else {
    elist = xdoc.element_list("dsOverview/contentMetadata/levels/level");
    auto elist2 = xdoc.element_list("dsOverview/contentMetadata/levels/layer");
    elist.insert(elist.end(), elist2.begin(), elist2.end());
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_VERTICAL_LEVELS__");
      list<string> llst;
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
      llst.sort(compare_levels);
      tdoc.add_replacement("__VERTICAL_LEVELS__", create_table_from_strings(
          llst, 4, "#c8daff", "#e1eaff"));
    }
  }

  // temporal frequency
  if (!found_content_metadata) {
    elist = xdoc.element_list("dsOverview/contentMetadata/temporalFrequency");
    auto m = 0;
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_TEMPORAL_FREQUENCY__");
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
      tdoc.add_replacement("__TEMPORAL_FREQUENCY__", ss.str());
    }
  }

  // data types
  if (found_content_metadata) {
    if (data_types.size() > 0) {
      tdoc.add_if("__HAS_DATA_TYPES__");
      string s;
      auto n = 0;
      for (const auto& data_type : data_types) {
        if (n > 0) {
          s += ", ";
        }
        s += to_capital(data_type);
        ++n;
      }
      tdoc.add_replacement("__DATA_TYPES__", s);
    }
  } else {
    elist = xdoc.element_list("dsOverview/contentMetadata/dataType");
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_DATA_TYPES__");
      string s;
      auto n = 0;
      unordered_map<string, char> u;
      for (const auto& ele : elist) {
        if (u.find(ele.content()) == u.end()) {
          if (n++ > 0) {
            s += ", ";
          }
          s += to_capital(ele.content());
          u[ele.content()] = 'Y';
        }
      }
      tdoc.add_replacement("__DATA_TYPES__", s);
    }
  }

  // spatial coverage
  unordered_map<string, shared_ptr<unordered_set<string>>> udefs;
  if (found_content_metadata) {
    unordered_map<size_t, tuple<string, string>> gdefs;
    for (const auto& dt : data_types) {
      if (dt == "grid") {
        MySQL::LocalQuery qgc;
        if (grouped_periods) {
          qgc.set("select gridDefinition_codes, webID_code from WGrML.ds" +
              dsnum2 + "_grid_definitions");
        } else {
          qgc.set("select distinct gridDefinition_codes from WGrML.ds" + dsnum2
              + "_agrids");
        }
        if (qgc.submit(server) < 0) {
          log_error2("error: " + qgc.error() + " while getting grid "
              "definitions", F, "dsgen", USER);
        }
        for (const auto& rgc : qgc) {
          vector<size_t> b;
          bitmap::uncompress_values(rgc[0], b);
          for (const auto& v : b) {
            string k;
            if (gdefs.find(v) == gdefs.end()) {
              MySQL::LocalQuery qgd("definition, defParams", "WGrML"
                  ".gridDefinitions", "code = " + itos(v));
              if (qgd.submit(server) < 0) {
                log_error2("query: " + qgd.show() + " returned error: " + qgd.
                    error(), F, "dsgen", USER);
              }
              MySQL::Row rgd;
              qgd.fetch_row(rgd);
              gdefs.emplace(v, make_tuple(rgd[0], rgd[1]));
              k = rgd[0] + "<!>" + rgd[1];
            } else {
              auto x = gdefs[v];
              k = get<0>(x) + "<!>" + get<1>(x);
            }
            string gp = "";
            if (qg.length() > 1) {
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
    elist = xdoc.element_list("dsOverview/contentMetadata/geospatialCoverage/"
        "grid");
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
      } else if (regex_search(k, regex("^gaussLatLon"))) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":" + e.attribute_value("endLat") + ":" + e.
            attribute_value("endLon") + ":" + e.attribute_value("xRes") + ":" +
            e.attribute_value("numY");
      } else if (regex_search(k, regex("^polarStereographic"))) {
        k += ":" + e.attribute_value("startLat") + ":" + e.attribute_value(
            "startLon") + ":60" + e.attribute_value("pole") + ":" + e.
            attribute_value("projLon") + ":" + e.attribute_value("pole") +
            ":" + e.attribute_value("xRes") + ":" + e.attribute_value("yRes");
      } else if (regex_search(k, regex("^lambertConformal"))) {
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
  list<double> slst;
  for (const auto& e : udefs) {
    double wl, el, sl, nl;
    if (gridutils::fill_spatial_domain_from_grid_definition(e.first,
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
  if (slst.size() > 0 && (mnw > -180. || mxe < 180.)) {
    for (const auto& east_lon : slst) {
      if (east_lon > mxe) {
        mxe = east_lon;
      }
    }
  }
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
    auto east=ftos(fabs(mxe), 3);
    if (mxe < 0) {
      east += "W";
    } else {
      east += "E";
    }
    ss << east << "<br />" << endl;
    auto south=ftos(fabs(mns), 3);
    if (mns < 0) {
      south += "S";
    } else {
      south += "N";
    }
    ss << "Latitude Range:  Southernmost=" << south << "  Northernmost=";
    auto north=ftos(fabs(mxn), 3);
    if (mxn < 0) {
      north += "S";
    } else {
      north += "N";
    }
    ss << north << endl;
    ss << "<br /><span id=\"D" << dnum << "\"><a href=\"javascript:swapDivs(" <<
        dnum << ", " << dnum + 1 << ")\" title=\"Expand coverage details\">"
        "<img src=\"/images/triangle.gif\" width=\"12\" height=\"15\" border="
        "\"0\"><span class=\"fs13px\">Detailed coverage information</span></a>"
        "</span><span style=\"visibility: hidden; position: absolute; top: 0\" "
        "id=\"D" << dnum + 1 << "\"><a href=\"javascript:swapDivs(" << dnum + 1
        << ", " << dnum << ")\"><img src=\"/images/triangle90.gif\" width=\""
        "15\" height=\"12\" border=\"0\" title=\"Collapse coverage details\">"
        "</a><span class=\"fs13px\">Detailed coverage information:" << endl;
    dnum += 2;
    map<string, shared_ptr<unordered_set<string>>> gdefs;
    for (const auto& e : udefs) {
      gdefs.emplace(gridutils::convert_grid_definition(e.first), e.second);
    }
    for (const auto& e : gdefs) {
      ss << "<div style=\"margin-left: 10px\">" << e.first;
      if (gdefs.size() > 1) {
        if (e.second != nullptr) {
          ss << "<div style=\"margin-left: 15px; color: #6a6a6a\">(";
          auto n = 0;
          for (const auto& group : *e.second) {
            if (n++ > 0) {
              ss << ", ";
            }
            ss << group;
          }
          ss << ")</div>";
        }
      }
      ss << "</div>";
    }
    ss << "</span></span>" << endl;
    tdoc.add_if("__HAS_SPATIAL_COVERAGE__");
    tdoc.add_replacement("__SPATIAL_COVERAGE__", ss.str());
  }

  // data contributors
  MySQL::LocalQuery qc("select g.path from search.contributors_new as c left "
      "join search.GCMD_providers as g on g.uuid = c.keyword where c.dsid = '" +
      metautils::args.dsnum + "' and c.vocabulary = 'GCMD'");
  if (qc.submit(server) < 0) {
    log_error2("query: " + qc.show() + " returned error: " + qc.error(), F,
        "dsgen", USER);
  }
  stringstream ss;
  json.clear();
  auto n = 0;
  for (const auto& row : qc) {
    if (n > 0) {
      ss << " | ";
    }
    auto snam = row[0];
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
  tdoc.add_replacement("__DATA__CONTRIBUTORS__", ss.str());
  if (!json.empty()) {
    update_wagtail("contributors", "[" + json + "]", F);
  }

  // related web sites
  elist = xdoc.element_list("dsOverview/relatedResource");
  if (elist.size() > 0) {
    if (elist.size() > 1) {
      tdoc.add_replacement("__WEB_SITES_VALIGN__", "top");
    } else {
      tdoc.add_replacement("__WEB_SITES_VALIGN__", "bottom");
    }
    stringstream ss;
    json.clear();
    for (const auto& e : elist) {
      auto d = e.content();
      trim(d);
      if (d.back() == '.' && d[d.length() - 2] != '.') {
        chop(d);
      }
      auto url = e.attribute_value("url");
      ss << "<a href=\"" << url << "\">";
      auto is_local = false;
      if (regex_search(url, regex("^http://rda.ucar.edu")) || regex_search(url,
          regex("^https://rda.ucar.edu")) || regex_search(url, regex(
          "^http://dss.ucar.edu")) || regex_search(url, regex(
          "^https://dss.ucar.edu"))) {
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
    tdoc.add_if("__HAS_RELATED_WEB_SITES__");
    tdoc.add_replacement("__RELATED_WEB_SITES__", ss.str());
    update_wagtail("related_rsrc_list", "[" + json + "]", F);
  }

  // publications
  add_publications(tdoc, xdoc);

  // dataset volume
  MySQL::LocalQuery qvol("dweb_size", "dssdb.dataset", "dsid = 'ds" +
      metautils::args.dsnum + "'");
  if (qvol.submit(server) < 0) {
    log_error2("query: " + qvol.show() + " returned error: " + qvol.error(), F,
        "dsgen", USER);
  }
  qg.set("select dweb_size, title, grpid from dssdb.dsgroup where dsid = "
      "'ds" + metautils::args.dsnum + "' and pindex = 0 and dweb_size > 0");
  if (qg.submit(server) < 0) {
    log_error2("query: " + qg.show() + " returned error: " + qg.error(), F,
        "dsgen", USER);
  }
  ss.str("");
  const int VOLUME_LEN = 4;
  const char *vlist[VOLUME_LEN] = { "MB", "GB", "TB", "PB" };
  MySQL::Row row;
  if (qvol.fetch_row(row) && !row[0].empty()) {
    auto v = stof(row[0]) / 1000000.;
    n = 0;
    while (v > 1000. && n < VOLUME_LEN) {
      v /= 1000.;
      ++n;
    }
    ss << ftos(llround(v * 100.) / 100., 6, 2, ' ') << " " << vlist[n];
    json = "\"full\": \"" + ftos(llround(v * 100.) / 100., 2) + " " + vlist[n] +
        "\"";
  }
  if (qg.num_rows() > 1) {
    ss << " <span class=\"fs13px\">(Entire dataset)</span><br /><span id=\"D" <<
        dnum << "\"><a href=\"javascript:swapDivs(" << dnum << ", " << dnum + 1
        << ")\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\""
        "15\" border=\"0\" title=\"Expand dataset product volume list\"><font "
        "size=\"-1\">Volume details by dataset product</font></a></span><span "
        "style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" <<
        dnum + 1 << "\"><a href=\"javascript:swapDivs(" << dnum + 1 << ", " <<
        dnum << ")\" title=\"Collapse dataset product volume list\"><img src=\""
        "/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\">"
        "</a><span class=\"fs13px\">Volume details by dataset product:";
    dnum += 2;
    string j;
    for (const auto& row2: qg) {
      auto g = row2[1].empty() ? row2[2] : row2[1];
      auto v=stof(row2[0]) / 1000000.;
      n = 0;
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
  tdoc.add_replacement("__VOLUME__", ss.str());
  if (!json.empty()) {
    update_wagtail("volume", "{" + json + "}", F);
  }

  // data formats
  add_data_formats(tdoc, formats, found_content_metadata);

  // related datasets
  elist = xdoc.element_list("dsOverview/relatedDataset");
  if (elist.size() > 0) {
    if (elist.size() > 1) {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__", "top");
    } else {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__", "bottom");
    }
    elist.sort(
    [](XMLElement& left, XMLElement& right) -> bool {
      if (left.attribute_value("ID") <= right.attribute_value("ID")) {
        return true;
      }
      return false;
    });
    string s, json;
    for (const auto& ele : elist) {
      MySQL::LocalQuery qd("dsid, title", "search.datasets", "dsid = '" +
          ele.attribute_value("ID") + "' and (type = 'P' or type = 'H')");
      if (qd.submit(server) < 0) {
        log_error2("query: " + qd.show() + " returned error: " + qd.error(), F,
            "dsgen", USER);
      }
      MySQL::Row row;
      if (qd.fetch_row(row)) {
        s += "<tr valign=\"top\"><td><a href=\"/datasets/ds" + row[0] +
            "#description\">" + row[0] + "</a></td><td>-</td><td>" + row[1] +
            "</td></tr>";
      }
      append(json, "{\"dsid\": \"" + row[0] + "\", \"title\": \"" + row[1] +
          "\"}", ", ");
    }
    tdoc.add_if("__HAS_RELATED_DATASETS__");
    tdoc.add_replacement("__RELATED_DATASETS__", s);
    update_wagtail("related_dslist", "[" + json + "]", F);
  }

  // more details
  if (unixutils::exists_on_server(metautils::directives.web_server, "/data/web/"
      "datasets/ds" + metautils::args.dsnum + "/metadata/detailed.html",
      metautils::directives.rdadata_home)) {
    tdoc.add_if("__HAS_MORE_DETAILS__");
  }

  // data citations
  add_citations(tdoc);

  ofs << tdoc;
  ofs.close();
  delete t;
}

void show_usage() {
  cerr << "usage: dsgen nnn.n" << endl;
  cerr << "\noptions:" << endl;
  cerr << "  --no-dset-waf  don't add the dataset to the queue for the DSET WAF"
      << endl;
}

int main(int argc, char **argv) {
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
  metautils::args.dsnum = argv[next];
  if (metautils::args.dsnum == "--help") {
    show_usage();
    exit(0);
  }
  if (regex_search(metautils::args.dsnum, regex("^ds"))) {
    metautils::args.dsnum = metautils::args.dsnum.substr(2);
  }
  if (metautils::args.dsnum >= "999.0") {
    no_dset_waf = true;
  }
  metautils::args.args_string = unixutils::unix_args_string(argc, argv);
  metautils::read_config("dsgen", USER);
  if (!temp_dir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, "dsgen", USER);
  }
  server.connect(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  MySQL::LocalQuery q("select type from search.datasets where dsid = '" +
      metautils::args.dsnum + "'");
  MySQL::Row row;
  if (q.submit(server) < 0 || !q.fetch_row(row)) {
    log_error2("unable to determine dataset type", F, "dsgen", USER);
  }
  dataset_type = row[0];
  if (!no_dset_waf) {
    if (dataset_type != "P" && dataset_type != "H") {
      no_dset_waf = true;
    }
  }
  TempDir d;
  if (!d.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary document directory", F, "dsgen",
        USER);
  }
  generate_index(dataset_type, d.name());
  if (dataset_type != "I") {
    generate_description(dataset_type, d.name());
    xdoc.close();
  }
  if (!no_dset_waf) {
    if (server.insert("metautil.dset_waf", "'" + metautils::args.dsnum +
        "', ''", "update dsid = values(dsid)") < 0) {
      metautils::log_warning("not marked for DSET WAF update", "dsgen", USER);
    }
  }
  server.disconnect();
  string p = "/data/web";
  if (dataset_type == "W") {
    p += "/internal";
  }
  p += "/datasets/ds" + metautils::args.dsnum;
  string e;
  if (unixutils::rdadata_sync(d.name(), ".", p, metautils::directives.
      rdadata_home, e) < 0) {
    metautils::log_error2("couldn't sync dataset files - rdadata_sync "
        "error(s): '" + e + "'", "main()", "dsgen", USER);
  }
}
