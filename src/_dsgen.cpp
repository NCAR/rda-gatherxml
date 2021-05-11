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
#include <gatherxml.hpp>
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

using gatherxml::this_function_label;
using metautils::log_error2;
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
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror="";
string mywarning="";
extern const string USER=getenv("USER");

TempDir temp_dir;
XMLDocument xdoc;
MySQL::Server server;
string dataset_type;
bool no_dset_waf=false;

void generate_index(string type,string tdir_name)
{
  static const string F = this_function_label(__func__);
  ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name+"/test_index.html").c_str());
  }
  else {
    ofs.open((tdir_name+"/index.html").c_str());
  }
  if (!ofs.is_open()) {
    log_error2("unable to open output for 'index.html'", F, "dsgen", USER);
  }
  auto t=new TokenDocument("/glade/u/home/rdadata/share/templates/dsgen_index.tdoc");
  if (!*t) {
    delete t;
    t=new TokenDocument("/usr/local/dss/share/templates/dsgen_index.tdoc");
    if (!*t) {
      log_error2("index template not found or unavailable", F, "dsgen", USER);
    }
  }
  auto &tdoc=*t;
  tdoc.add_replacement("__DSNUM__",metautils::args.dsnum);
  if (dataset_type == "I") {
    tdoc.add_if("__IS_INTERNAL_DATASET__");
  }
  else {
    struct stat buf;
    string ds_overview;
    if (stat(("/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
      ds_overview="/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml";
    }
    else {
      ds_overview=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml",temp_dir.name());
      if (ds_overview.empty()) {
        log_error2("dsOverview.xml does not exist for " + metautils::args.dsnum,
            F, "dsgen", USER);
      }
    }
    if (!xdoc.open(ds_overview)) {
      log_error2("unable to open dsOverview.xml for " + metautils::args.dsnum +
          "; parse error: '" + xdoc.parse_error() + "'", F, "dsgen", USER);
    }
    stringstream dc_meta_tags_s;
    if (!metadataExport::export_to_dc_meta_tags(dc_meta_tags_s,metautils::args.dsnum,xdoc,0)) {
      log_error2("unable to export DC meta tags: '" + myerror + "'", F, "dsgen",
          USER);
    }
    tdoc.add_replacement("__DC_META_TAGS__",dc_meta_tags_s.str());
    auto e=xdoc.element("dsOverview/title");
    auto title=e.content();
    tdoc.add_replacement("__TITLE__",title);
    stringstream json_ld_s;
    if (!metadataExport::export_to_json_ld(json_ld_s,metautils::args.dsnum,xdoc,0)) {
      log_error2("unable to export JSON-LD metadata", F, "dsgen", USER);
    }
    tdoc.add_replacement("__JSON_LD__",json_ld_s.str());
    e=xdoc.element("dsOverview/logo");
    if (!e.content().empty()) {
      auto logo_parts=strutils::split(e.content(),".");
      auto geometry_parts=strutils::split(logo_parts[logo_parts.size()-2],"_");
      auto width=stoi(geometry_parts[geometry_parts.size()-2]);
      auto height=stoi(geometry_parts[geometry_parts.size()-1]);
      tdoc.add_replacement("__LOGO_IMAGE__",e.content());
      tdoc.add_replacement("__LOGO_WIDTH__",strutils::itos(lroundf(width*70./height)));
    }
    else {
      tdoc.add_replacement("__LOGO_IMAGE__","default_200_200.png");
      tdoc.add_replacement("__LOGO_WIDTH__","70");
    }
    MySQL::Query query("doi","dssdb.dsvrsn","dsid = 'ds"+metautils::args.dsnum+"' and status = 'A'");
    MySQL::Row row;
    string doi_span;
    if (query.submit(server) == 0 && query.fetch_row(row) && !row[0].empty()) {
      doi_span="&nbsp;|&nbsp;<span class=\"blue\">DOI: "+row[0]+"</span>";
    }
    tdoc.add_replacement("__DOI_SPAN__",doi_span);
    e=xdoc.element("dsOverview/contact");
    auto contact_parts=strutils::split(e.content());
    query.set("select logname,phoneno from dssdb.dssgrp where fstname = '"+contact_parts[0]+"' and lstname = '"+contact_parts[1]+"'");
    if (query.submit(server) < 0) {
      log_error2("mysql error while trying to get specialist information: " +
          query.error(), F, "dsgen", USER);
    }
    if (!query.fetch_row(row)) {
      log_error2("no result returned for specialist '" + e.content() + "'", F,
          "dsgen", USER);
    }
    auto phoneno=row[1];
    strutils::replace_all(phoneno,"(","");
    strutils::replace_all(phoneno,")","");
    tdoc.add_replacement("__CONTACT_LOGIN__",row[0]);
    tdoc.add_replacement("__CONTACT_NAME__",e.content());
    tdoc.add_replacement("__CONTACT_PHONE__",phoneno);
    if (dataset_type == "D") {
      tdoc.add_if("__IS_DEAD_DATASET__");
    }
  }
  ofs << tdoc;
  ofs.close();
  delete t;
}

bool compare_strings(const string& left,const string& right)
{
  if (left <= right) {
    return true;
  }
  else {
    return false;
  }
}

bool compare_references(XMLElement& left,XMLElement& right)
{ 
  auto e=left.element("year");
  auto l=e.content();
  e=right.element("year");
  auto r=e.content();
  if (l > r) {
    return true;
  }
  else if (l < r) {
    return false;
  }
  else {
    if (left.attribute_value("type") == "journal" && right.attribute_value("type") == "journal" && left.element("periodical").content() == right.element("periodical").content()) {
      if (left.element("periodical").attribute_value("pages") > right.element("periodical").attribute_value("pages")) {
        return true;
      }
      else {
        return false;
      }
    }
    else {
      return false;
    }
  }
}

bool compare_levels(const string& left,const string& right)
{
  if (((left[0] >= '0' && left[0] <= '9') || (left[0] == '-' && left[1] >= '0' && left[1] <= '9')) && ((right[0] >= '0' && right[0] <= '9') || (right[0] == '-' && right[1] >= '0' && right[1] <= '9'))) {
    auto lidx=left.find(" ");
    string lval,lunits;
    if (lidx != string::npos) {
      lval=left.substr(0,lidx);
      lunits=left.substr(lidx+1);
    }
    else {
      lval=left;
      lunits="";
    }
    auto ridx=right.find(" ");
    string rval,runits;
    if (ridx != string::npos) {
      rval=right.substr(0,ridx);
      runits=right.substr(ridx+1);
    }
    else {
      rval=right;
      runits="";
    }
    auto ilval=stoi(lval);
    auto irval=stoi(rval);
    if (lunits == runits) {
      if (lunits == "mbar" || lunits == "deg K") {
        if (ilval >= irval) {
          return true;
        }
        else {
          return false;
        }
      }
      else {
        if (ilval <= irval) {
          return true;
        }
        else {
          return false;
        }
      }
    }
    else {
      if (lunits <= runits) {
        return true;
      }
      else {
        return false;
      }
    }
  }
  else {
    if (left[0] >= '0' && left[0] <= '9') {
      return false;
    }
    else if (right[0] >= '0' && right[0] <= '9') {
      return true;
    }
    else if (left <= right) {
      return true;
    }
    else {
      return false;
    }
  }
}

string create_table_from_strings(list<string> list,int max_columns,string color1,string color2)
{
  stringstream ss;
  ss << "<table cellspacing=\"0\">" << endl;
  auto num_rows=list.size()/max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++num_rows;
    auto max_mod=(list.size() % max_columns);
    for (int n=max_columns-1; n > 1; n--) {
      if ( (list.size()/n) <= num_rows) {
        if ( (list.size() % n) == 0) {
          max_columns=n;
          break;
        }
        else if ( (list.size() % n) > max_mod) {
          max_mod=(list.size() % n);
          max_columns=n;
        }
      }
    }
  }
  num_rows=list.size()/max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++num_rows;
  }
  auto cmax=max_columns-1;
  auto n=0;
  size_t m=0;
  for (const auto& item : list) {
    n=n % max_columns;
    if (n == 0) {
      ++m;
      ss << "<tr style=\"vertical-align: top\">" << endl;
    }
    ss << "<td class=\"";
    if (m == 1) {
      ss << "border-top ";
    }
    ss << "border-bottom border-left";
    if (n == cmax) {
      ss << " border-right";
    }
    ss << "\" style=\"padding: 5px 8px 5px 8px; text-align: left; background-color: ";
    if ( (m % 2) == 0) {
      ss << color1;
    }
    else {
      ss << color2;
    }
    ss << "; height: 18px;";
    if (m == 1 && n == 0) {
      if (num_rows > 1) {
        ss << " border-radius: 10px 0px 0px 0px";
      }
      else {
        if (list.size() == 1) {
          ss << " border-radius: 10px 10px 10px 10px";
        }
        else {
          ss << " border-radius: 10px 0px 0px 10px";
        }
      }
    }
    else if (m == 1 && n == cmax) {
      if (m == num_rows) {
        ss << " border-radius: 0px 10px 10px 0px";
      }
      else {
        ss << " border-radius: 0px 10px 0px 0px";
      }
    }
    else if (m == num_rows && n == 0) {
      ss << " border-radius: 0px 0px 0px 10px";
    }
    else if (m == num_rows) {
      if (num_rows > 1) {
        if (n == cmax) {
          ss << " border-radius: 0px 0px 10px 0px";
        }
      }
      else {
        if (n == static_cast<int>(list.size()-1)) {
          ss << " border-radius: 0px 10px 10px 0px";
        }
      }
    }
    ss << "\">"+item+"</td>" << endl;
    if (n == cmax) {
      ss << "</tr>" << endl;
    }
    ++n;
  }
  if (n != max_columns) {
    if (num_rows > 1) {
      for (int l=n; l < max_columns; ++l) {
        ss << "<td class=\"border-bottom border-left";
        if (l == cmax) {
          ss << " border-right";
        }
        ss << "\" style=\"background-color: ";
        if ( (m % 2) == 0) {
          ss << color1;
        }
        else {
          ss << color2;
        }
        ss << "; height: 18px;";
        if (l == cmax) {
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

void insert_table(ofstream& ofs,list<string> list,int max_columns,string color1,string color2)
{
  ofs << "<table cellspacing=\"0\">" << endl;
  auto num_rows=list.size()/max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++num_rows;
    auto max_mod=(list.size() % max_columns);
    for (int n=max_columns-1; n > 1; n--) {
      if ( (list.size()/n) <= num_rows) {
        if ( (list.size() % n) == 0) {
          max_columns=n;
          break;
        }
        else if ( (list.size() % n) > max_mod) {
          max_mod=(list.size() % n);
          max_columns=n;
        }
      }
    }
  }
  num_rows=list.size()/max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++num_rows;
  }
  auto cmax=max_columns-1;
  auto n=0;
  size_t m=0;
  for (const auto& item : list) {
    n=n % max_columns;
    if (n == 0) {
      ++m;
      ofs << "<tr style=\"vertical-align: top\">" << endl;
    }
    ofs << "<td class=\"";
    if (m == 1) {
      ofs << "border-top ";
    }
    ofs << "border-bottom border-left";
    if (n == cmax) {
      ofs << " border-right";
    }
    ofs << "\" style=\"padding: 5px 8px 5px 8px; text-align: left; background-color: ";
    if ( (m % 2) == 0) {
      ofs << color1;
    }
    else {
      ofs << color2;
    }
    ofs << "; height: 18px;";
    if (m == 1 && n == 0) {
      if (num_rows > 1) {
        ofs << " border-radius: 10px 0px 0px 0px";
      }
      else {
        if (list.size() == 1) {
          ofs << " border-radius: 10px 10px 10px 10px";
        }
        else {
          ofs << " border-radius: 10px 0px 0px 10px";
        }
      }
    }
    else if (m == 1 && n == cmax) {
      if (m == num_rows) {
        ofs << " border-radius: 0px 10px 10px 0px";
      }
      else {
        ofs << " border-radius: 0px 10px 0px 0px";
      }
    }
    else if (m == num_rows && n == 0) {
      ofs << " border-radius: 0px 0px 0px 10px";
    }
    else if (m == num_rows) {
      if (num_rows > 1) {
        if (n == cmax) {
          ofs << " border-radius: 0px 0px 10px 0px";
        }
      }
      else {
        if (n == static_cast<int>(list.size()-1)) {
          ofs << " border-radius: 0px 10px 10px 0px";
        }
      }
    }
    ofs << "\">"+item+"</td>" << endl;
    if (n == cmax) {
      ofs << "</tr>" << endl;
    }
    ++n;
  }
  if (n != max_columns) {
    if (num_rows > 1) {
      for (int l=n; l < max_columns; ++l) {
        ofs << "<td class=\"border-bottom border-left";
        if (l == cmax) {
          ofs << " border-right";
        }
        ofs << "\" style=\"background-color: ";
        if ( (m % 2) == 0) {
          ofs << color1;
        }
        else {
          ofs << color2;
        }
        ofs << "; height: 18px;";
        if (l == cmax) {
          ofs << " border-radius: 0px 0px 10px 0px";
        }
        ofs << "\">&nbsp;</td>" << endl;
      }
    }
    ofs << "</tr>";
  }
  ofs << "</table>" << endl;
}

string text_field_from_element(const XMLElement& e)
{
  auto s=e.to_string();
  if (!s.empty()) {
    strutils::replace_all(s,"<"+e.name()+">","");
    strutils::replace_all(s,"</"+e.name()+">","");
    strutils::trim(s);
    size_t idx;
    if ( (idx=s.find("<p")) == string::npos) {
      idx=s.find("<P");
    }
    if (idx == 0) {
      auto idx2=s.find(">",idx);
      s.insert(idx2," style=\"margin: 0px; padding: 0px\"");
    }
  }
  return s;
}

void insert_text_field(ofstream& ofs,const XMLElement& e,string section_title)
{
  auto s=e.to_string();
  if (!s.empty()) {
    strutils::replace_all(s,"<"+e.name()+">","");
    strutils::replace_all(s,"</"+e.name()+">","");
    strutils::trim(s);
    size_t idx;
    if ( (idx=s.find("<p")) == string::npos) {
      idx=s.find("<P");
    }
    if (idx == 0) {
      auto idx2=s.find(">",idx);
      s.insert(idx2," style=\"margin: 0px; padding: 0px\"");
    }
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">" << section_title << ":</td><td>" << s << "</td></tr>" << endl;
  }
}

void add_publications(TokenDocument& tdoc,XMLDocument& xdoc)
{
  auto reference_list=xdoc.element_list("dsOverview/reference");
  stringstream publications_s;
  if (reference_list.size() > 0) {
    tdoc.add_if("__HAS_PUBLICATIONS__");
    reference_list.sort(compare_references);
    for (const auto& reference : reference_list) {
      publications_s << "<div>" << reference.element("authorList").content() << ", " << reference.element("year").content() << ": ";
      auto pub_type=reference.attribute_value("type");
      if (pub_type == "journal") {
        auto periodical=reference.element("periodical");
        auto url=reference.element("url").content();
        auto title=reference.element("title").content();
        if (!url.empty()) {
          publications_s << "<a href=\"" << url << "\">" << title << "</a>";
        }
        else {
          publications_s << title;
        }
        if (!strutils::has_ending(title,"?")) {
          publications_s << ".";
        }
        publications_s << "  <i>" << periodical.content() << "</i>, ";
        if (periodical.attribute_value("pages") == "0-0") {
          if (periodical.attribute_value("number") == "0") {
            publications_s << "Submitted";
          }
          else if (periodical.attribute_value("number") == "1") {
            publications_s << "Accepted";
          }
          else if (periodical.attribute_value("number") == "2") {
            publications_s << "In Press";
          }
        }
        else {
          publications_s << "<b>" << periodical.attribute_value("number") << "</b>, ";
          auto pages=periodical.attribute_value("pages");
          if (regex_search(pages,regex("^AGU:"))) {
            publications_s << pages.substr(4);
          }
          else {
            auto page_parts=strutils::split(pages,"-"); 
            if (page_parts.size() == 2 && page_parts[0] == page_parts[1]) {
              publications_s << page_parts[0];
            }
            else {
              publications_s << periodical.attribute_value("pages");
            }
          }
        }
        auto doi=reference.element("doi").content();
        if (!doi.empty()) {
          publications_s << " (DOI: " << doi << ")";
        }
        publications_s << ".";
      }
      else if (pub_type == "preprint") {
        auto conference=reference.element("conference");
        auto url=reference.element("url").content();
        if (!url.empty()) {
          publications_s << "<a href=\"" << url << "\">" << reference.element("title").content() << "</a>";
        }
        else {
          publications_s << reference.element("title").content();
        }
        publications_s << ".  <i>Proceedings of the " << conference.content() << "</i>, " << conference.attribute_value("host") << ", " << conference.attribute_value("location");
        auto pages=conference.attribute_value("pages");
        if (!pages.empty()) {
          publications_s << ", " << pages;
        }
        auto doi=reference.element("doi").content();
        if (!doi.empty()) {
          publications_s << " (DOI: " << doi << ")";
        }
        publications_s << ".";
      }
      else if (pub_type == "technical_report") {
        auto organization=reference.element("organization");
        auto url=reference.element("url").content();
        if (!url.empty()) {
          publications_s << "<i><a href=\"" << url << "\">" << reference.element("title").content() << "</a>.</i>";
        }
        else {
          publications_s << "<i>" << reference.element("title").content() << ".</i>";
        }
        publications_s << "  ";
        auto report_ID=organization.attribute_value("reportID");
        if (!report_ID.empty()) {
          publications_s << report_ID << ", ";
        }
        publications_s << organization.content();
        auto pages=organization.attribute_value("pages");
        if (pages != "-99") {
          publications_s << ", " << organization.attribute_value("pages") << " pp.";
        }
        auto doi=reference.element("doi").content();
        if (!doi.empty()) {
          publications_s << " (DOI: " << doi << ").";
        }
      }
      else if (pub_type == "book") {
        auto publisher=reference.element("publisher");
        publications_s << "<i>" << reference.element("title").content() << "</i>. " << publisher.content() << ", " << publisher.attribute_value("place");
        auto doi=reference.element("doi").content();
        if (!doi.empty()) {
          publications_s << " (DOI: " << doi << ")";
        }
        publications_s << ".";
      }
      else if (pub_type == "book_chapter") {
        auto book=reference.element("book");
        publications_s << "\"" << reference.element("title").content() << "\", in " << book.content() << ". Ed. " << book.attribute_value("editor") << ", " << book.attribute_value("publisher") << ", ";
        if (book.attribute_value("pages") == "0-0") {
          publications_s << "In Press";
        }
        else {
          publications_s << book.attribute_value("pages");
        }
        auto doi=reference.element("doi").content();
        if (!doi.empty()) {
          publications_s << " (DOI: " << doi << ")";
        }
        publications_s << ".";
      }
      publications_s << "</div>" << endl;
      auto annotation=reference.element("annotation").content();
      if (!annotation.empty() > 0) {
        publications_s << "<div style=\"margin-left: 15px; color: #5f5f5f\">" << annotation << "</div>" << endl;
      }
      publications_s << "<br />" << endl;
    }
  }
  auto reference_url_list=xdoc.element_list("dsOverview/referenceURL");
  if (reference_url_list.size() > 0) {
    if (publications_s.str().empty()) {
      tdoc.add_if("__HAS_PUBLICATIONS__");
    }
    for (const auto& url : reference_url_list) {
      publications_s << "<a href=\"" << url.attribute_value("url") << "\">" << url.content() << "</a><br>" << endl;
    }
  }
  if (!publications_s.str().empty()) {
    tdoc.add_replacement("__PUBLICATIONS__",publications_s.str());
  }
}

void add_data_formats(TokenDocument& tdoc,vector<string>& formats,bool found_content_metadata)
{
  static const string F = this_function_label(__func__);
  if (!found_content_metadata) {
    auto format_list=xdoc.element_list("dsOverview/contentMetadata/format");
    for (const auto& format : format_list) {
      formats.emplace_back(format.content()+"<!>"+format.attribute_value("href"));
    }
  }
  struct stat buf;
  XMLDocument fdoc;
  if (stat("/usr/local/www/server_root/web/metadata",&buf) == 0) {
    fdoc.open("/usr/local/www/server_root/web/metadata/FormatReferences.xml");
  }
  else {
    auto file=unixutils::remote_web_file("https://rda.ucar.edu/metadata/FormatReferences.xml",temp_dir.name());
    fdoc.open(file);
  }
  if (!fdoc) {
    log_error2("unable to open FormatReferences.xml", F, "dsgen", USER);
  }
  string data_formats;
  size_t n=0;
  for (const auto& format_entry : formats) {
    auto format_parts=strutils::split(format_entry,"<!>");
    auto description=format_parts[0];
    string url;
    if (regex_search(description,regex("^proprietary_"))) {
      strutils::replace_all(description,"proprietary_","");
      if (!format_parts[1].empty()) {
        url=format_parts[1];
      }
      else {
        description+=" (see dataset documentation)";
        url="";
      }
    }
    else {
      auto format_reference=fdoc.element(("formatReferences/format@name="+description));
      url=format_reference.attribute_value("href");
    }
    if (!url.empty()) {
      data_formats+="<a href=\""+url+"\" target=\"_format\">";
      if (!regex_search(url,regex("^http://rda.ucar.edu"))) {
        data_formats+="<i>";
      }
    }
    strutils::replace_all(description,"_"," ");
    data_formats+=description;
    if (!url.empty()) {
      data_formats+="</a>";
      if (!regex_search(url,regex("^http://rda.ucar.edu"))) {
        data_formats+="</i>";
      }
    }
    ++n;
    if (n < formats.size()) {
      data_formats+=", ";
    }
  }
  fdoc.close();
  tdoc.add_replacement("__DATA_FORMATS__",data_formats);
}

void add_citations(TokenDocument& tdoc)
{
  MySQL::LocalQuery citations_query("select distinct d.DOI_work from citation.data_citations as d left join dssdb.dsvrsn as v on v.doi = d.DOI_data where v.dsid = 'ds"+metautils::args.dsnum+"'");
  if (citations_query.submit(server) < 0) {
    return;
  }
  vector<tuple<string,string>> citations;
  for (const auto& citations_row : citations_query) {
    auto doi=citations_row[0];
    MySQL::LocalQuery works_query("select title,pub_year,type,publisher from citation.works where DOI = '"+doi+"'");
    MySQL::Row works_row;
    if (works_query.submit(server) == 0 && works_query.fetch_row(works_row)) {
      auto title=htmlutils::unicode_escape_to_html(works_row[0]);
      auto pub_year=works_row[1];
      auto type=works_row[2];
      MySQL::LocalQuery doi_authors_query("select last_name,first_name,middle_name from citation.works_authors where ID = '"+doi+"' and ID_type = 'DOI' order by sequence");
      if (doi_authors_query.submit(server) == 0) {
        string citation;
        size_t n=1;
        for (const auto& doi_authors_row : doi_authors_query) {
          if (citation.empty()) {
            citation+=htmlutils::unicode_escape_to_html(doi_authors_row[0]);
            if (!doi_authors_row[1].empty()) {
              citation+=", "+doi_authors_row[1].substr(0,1)+".";
            }
            if (!doi_authors_row[2].empty()) {
              citation+=" "+doi_authors_row[2].substr(0,1)+".";
            }
          }
          else {
            citation+=", ";
            if (n == doi_authors_query.num_rows()) {
              citation+="and ";
            }
            if (!doi_authors_row[1].empty()) {
              citation+=doi_authors_row[1].substr(0,1)+". ";
            }
            if (!doi_authors_row[2].empty()) {
              citation+=doi_authors_row[2].substr(0,1)+". ";
            }
            citation+=htmlutils::unicode_escape_to_html(doi_authors_row[0]);
          }
          ++n;
        }
        if (!citation.empty()) {
          citation+=", "+pub_year+": ";
          if (type == "C") {
            citation+="\"";
          }
          citation+=title;
          switch (type[0]) {
            case 'C': {
              citation+="\", in ";
              MySQL::LocalQuery chapter_works_query("select pages,ISBN from citation.book_chapter_works where DOI = '"+doi+"'");
              MySQL::Row chapter_works_row;
              if (chapter_works_query.submit(server) == 0 && chapter_works_query.fetch_row(chapter_works_row)) {
                MySQL::LocalQuery book_works_query("select title,publisher from citation.book_works where ISBN = '"+chapter_works_row[1]+"'");
                MySQL::Row book_works_row;
                if (book_works_query.submit(server) == 0 && book_works_query.fetch_row(book_works_row)) {
                  citation+=htmlutils::unicode_escape_to_html(book_works_row[0])+". Ed. ";
                  MySQL::LocalQuery isbn_authors_query("select first_name,middle_name,last_name from citation.works_authors where ID = '"+chapter_works_row[1]+"' and ID_type = 'ISBN' order by sequence");
                  if (isbn_authors_query.submit(server) == 0) {
                    size_t n=1;
                    for (const auto& isbn_authors_row : isbn_authors_query) {
                      if (n > 1) {
                        citation+=", ";
                        if (n == isbn_authors_query.num_rows()) {
                          citation+="and ";
                        }
                      }
                      citation+=isbn_authors_row[0].substr(0,1)+". ";
                      if (!isbn_authors_row[1].empty()) {
                        citation+=isbn_authors_row[1].substr(0,1)+". ";
                      }
                      citation+=htmlutils::unicode_escape_to_html(isbn_authors_row[2]);
                      ++n;
                    }
                    citation+=", "+htmlutils::unicode_escape_to_html(book_works_row[1])+", "+chapter_works_row[0]+".";
                  }
                  else {
                    citation="";
                  }
                }
                else {
                  citation="";
                }
              }
              else {
                citation="";
              }
              break;
            }
            case 'J': {
              citation+=". ";
              MySQL::LocalQuery journal_works_query("select pub_name,volume,pages from citation.journal_works where DOI = '"+doi+"'");
              MySQL::Row journal_works_row;
              if (journal_works_query.submit(server) == 0 && journal_works_query.fetch_row(journal_works_row)) {
                citation+="<em>"+htmlutils::unicode_escape_to_html(journal_works_row[0])+"</em>";
                if (!journal_works_row[1].empty()) {
                  citation+=", <strong>"+journal_works_row[1]+"</strong>";
                }
                if (!journal_works_row[2].empty()) {
                  citation+=", "+journal_works_row[2];
                }
                citation+=", <a href=\"https://doi.org/"+doi+"\" target=\"_doi\">https://doi.org/"+doi+"</a>";
              }
              else {
                citation="";
              }
              break;
            }
            case 'P': {
              citation+=". <em>";
              MySQL::LocalQuery proceedings_works_query("select pub_name,pages from citation.proceedings_works where DOI = '"+doi+"'");
              MySQL::Row proceedings_works_row;
              if (proceedings_works_query.submit(server) == 0 && proceedings_works_query.fetch_row(proceedings_works_row)) {
                auto pub_data=htmlutils::unicode_escape_to_html(proceedings_works_row[0])+"</em>";
                if (!works_row[3].empty()) {
                  pub_data+=", "+works_row[3];
                }
                if (!proceedings_works_row[1].empty()) {
                  pub_data+=", "+proceedings_works_row[1];
                }
                citation+=pub_data+", <a href=\"https://doi.org/"+doi+"\" target=\"_doi\">https://doi.org/"+doi+"</a>";
              }
              else {
                citation="";
              }
              break;
            }
          }
        }
        if (!citation.empty()) {
          citations.emplace_back(make_tuple(pub_year,citation));
        }
      }
    }
  }
  if (citations.size() > 0) {
    tdoc.add_if("__HAS_DATA_CITATIONS__");
    if (citations.size() > 1) {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__","<strong>"+strutils::itos(citations.size())+"</strong> times");
    }
    else {
      tdoc.add_replacement("__NUM_DATA_CITATIONS__","<strong>"+strutils::itos(citations.size())+"</strong> time");
    }
    std::sort(citations.begin(),citations.end(),
    [](const tuple<string,string>& left,const tuple<string,string>& right) -> bool
    {
      if (get<0>(left) > get<0>(right)) {
        return true;
      }
      else if (get<0>(left) < get<0>(right)) {
        return false;
      }
      else {
        return (get<1>(left) < get<1>(right));
      }
    });
    unordered_set<string> pub_years;
    for (const auto& c : citations) {
      auto pub_year=get<0>(c);
      if (pub_years.find(pub_year) == pub_years.end()) {
        tdoc.add_repeat("__DATA_CITER__","CITATION[!]"+get<1>(c)+"<!>YEAR[!]"+pub_year);
        pub_years.emplace(pub_year);
      }
      else {
        tdoc.add_repeat("__DATA_CITER__","CITATION[!]"+get<1>(c));
      }
    }
  }
  else {
    tdoc.add_replacement("__NUM_DATA_CITATIONS__","<strong>0</strong> times");
  }
}

void generate_description(string type,string tdir_name)
{
  static const string F = this_function_label(__func__);
  string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name+"/test_description.html").c_str());
  }
  else {
    ofs.open((tdir_name+"/description.html").c_str());
  }
  if (!ofs.is_open()) {
    log_error2("unable to open output for 'description.html'", F, "dsgen",
        USER);
  }
  auto t=new TokenDocument("/glade/u/home/rdadata/share/templates/dsgen_description.tdoc");
  if (!*t) {
    t=new TokenDocument("/usr/local/dss/share/templates/dsgen_description.tdoc");
    if (!*t) {
      log_error2("description template not found or unavailable", F, "dsgen",
          USER);
    }
  }
  auto &tdoc=*t;
// abstract
  tdoc.add_replacement("__ABSTRACT__",text_field_from_element(xdoc.element("dsOverview/summary")));
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
  auto databases=metautils::cmd_databases("dsgen","x");
  if (databases.size() == 0) {
    log_error2("empty CMD database list", F, "dsgen", USER);
  }
  vector<string> formats,data_types;
  auto found_content_metadata=false;
  for (const auto& db : databases) {
    string db_name,data_type;
    std::tie(db_name,data_type)=db;
    if (db_name[0] != 'V' && table_exists(server,db_name+".ds"+dsnum2+"_primaries2")) {
      MySQL::LocalQuery query("select distinct format from "+db_name+".formats as f left join "+db_name+".ds"+dsnum2+"_primaries2 as d on d.format_code = f.code where !isnull(d.format_code)");
      if (query.submit(server) < 0) {
        log_error2("query: " + query.show() + " returned error: " + query
            .error(), F, "dsgen", USER);
      }
      for (const auto& row : query) {
        formats.emplace_back(row[0]+"<!>");
      }
      if (formats.size() > 0) {
        found_content_metadata=true; 
        if (!data_type.empty()) {
          data_types.emplace_back(data_type);
        }
      }
    }
  }
  tdoc.add_replacement("__DSNUM__",metautils::args.dsnum);
// acknowledgements
  auto acknowledgements=text_field_from_element(xdoc.element("dsOverview/acknowledgement"));
  if (!acknowledgements.empty()) {
    tdoc.add_if("__HAS_ACKNOWLEDGEMENTS__");
    tdoc.add_replacement("__ACKNOWLEDGEMENT__",acknowledgements);
  }
// temporal range(s)
  MySQL::LocalQuery query("select dsid from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    log_error2("query: " + query.show() + " returned error: " + query.error(),
        F, "dsgen", USER);
  }
  if (query.num_rows() > 0) {
    query.set("select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) where p.dsid = 'ds"+metautils::args.dsnum+"' and g.pindex = 0 and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' union select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g2.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) left join dssdb.dsgroup as g2 on (p.dsid = g2.dsid and g.pindex = g2.gindex) where p.dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' and !isnull(g2.title) order by title");
  }
  else {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
  }
  if (query.submit(server) < 0) {
    log_error2("query: " + query.show() + " returned error: " + query.error(),
        F, "dsgen", USER);
  }
  if (query.num_rows() == 0) {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
    if (query.submit(server) < 0) {
      log_error2("query: " + query.show() + " returned error: " + query.error(),
          F, "dsgen", USER);
    }
  }
  auto div_num=0;
  bool grouped_periods=false;
  unordered_map<string,string> groups_table,rda_files_table,metadata_files_table;
  if (query.num_rows() > 0) {
    tdoc.add_if("__HAS_TEMPORAL_RANGE__");
    if (query.num_rows() > 1) {
      MySQL::LocalQuery grouped_periods_query("distinct gindex","dssdb.dsperiod","dsid = 'ds"+metautils::args.dsnum+"'");
      if (grouped_periods_query.submit(server) < 0) {
        log_error2("query: " + grouped_periods_query.show() + " returned "
            "error: " + grouped_periods_query.error(), F, "dsgen", USER);
      }
      if (grouped_periods_query.num_rows() > 1) {
        grouped_periods=true;
        tdoc.add_if("__HAS_TEMPORAL_BY_GROUP1__");
        MySQL::LocalQuery groups_query("select gindex,title from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"'");
        if (groups_query.submit(server) < 0) {
          log_error2("error: " + groups_query.error() + " while getting groups "
              "data", F, "dsgen", USER);
        }
        for (const auto& row : groups_query) {
          groups_table.emplace(row[0],row[1]);
        }
        MySQL::LocalQuery rda_files_query("select wfile,tindex from dssdb.wfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'D' and status = 'P'");
        if (rda_files_query.submit(server) < 0) {
          log_error2("error: " + rda_files_query.error() + " while getting RDA "
              "files data", F, "dsgen", USER);
        }
        for (const auto& row : rda_files_query) {
          rda_files_table.emplace(row[0],row[1]);
        }
        MySQL::LocalQuery metadata_files_query("select code,webID from WGrML.ds"+dsnum2+"_webfiles2");
        if (metadata_files_query.submit(server) == 0) {
          for (const auto& row : metadata_files_query) {
            metadata_files_table.emplace(row[0],row[1]);
          }
        }
        MySQL::LocalQuery date_query("select min(concat(date_start,' ',time_start)),min(start_flag),max(concat(date_end,' ',time_end)),min(end_flag),any_value(time_zone) from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' group by dsid");
        if (date_query.submit(server) < 0) {
          log_error2("query: " + date_query.show() + " returned error: " +
              date_query.error(), F, "dsgen", USER);
        }
        MySQL::Row date_row;
        date_query.fetch_row(date_row);
        auto start_date_time=metatranslations::date_time(date_row[0],date_row[1],date_row[4]);
        auto end_date_time=metatranslations::date_time(date_row[2],date_row[3],date_row[4]);
        auto temporal=start_date_time;
        if (!end_date_time.empty() && end_date_time != start_date_time) {
          temporal+=" to "+end_date_time;
        }
        tdoc.add_replacement("__TEMPORAL_RANGE__",temporal);
        tdoc.add_replacement("__N_TEMPORAL__",strutils::itos(div_num));
        tdoc.add_replacement("__N_TEMPORAL1__",strutils::itos(div_num+1));
        div_num+=2;
      }
    }
    map<string,tuple<string,string>> periods_table;
    for (const auto& row : query) {
      auto start_date_time=metatranslations::date_time(row[0]+" "+row[1],row[2],row[6]);
      auto end_date_time=metatranslations::date_time(row[3]+" "+row[4],row[5],row[6]);
      string key;
      if (!row[7].empty()) {
        key=row[7];
      }
      else {
        key=row[8];
      }
      if (periods_table.find(key) == periods_table.end()) {
        periods_table.emplace(key,make_tuple(start_date_time,end_date_time));
      }
      else {
        string &start=get<0>(periods_table[key]);
        if (start_date_time < start) {
          start=start_date_time;
        }
        string &end=get<1>(periods_table[key]);
        if (end_date_time > end) {
          end=end_date_time;
        }
      }   
    }
    for (const auto& e : periods_table) {
      auto temporal=get<0>(e.second);
      auto end=get<1>(e.second);
      if (!end.empty() && end != temporal) {
        temporal+=" to "+end;
      }
      if (periods_table.size() > 1) {
        temporal+=" ("+e.first+")";
        tdoc.add_repeat("__TEMPORAL_RANGE__","<div style=\"margin-left: 10px\">"+temporal+"</div>");
      }
      else {
        tdoc.add_repeat("__TEMPORAL_RANGE__",temporal);
      }
    }
    if (grouped_periods) {
      tdoc.add_if("__HAS_TEMPORAL_BY_GROUP2__");
    }
  }
// update frequency
  auto e=xdoc.element("dsOverview/continuingUpdate");
  if (e.attribute_value("value") == "yes") {
    tdoc.add_if("__HAS_UPDATE_FREQUENCY__");
    tdoc.add_replacement("__UPDATE_FREQUENCY__",strutils::capitalize(e.attribute_value("frequency")));
  }
// access restrictions
  e=xdoc.element("dsOverview/restrictions/access");
  auto access=e.to_string();
  strutils::replace_all(access,"<access>","");
  strutils::replace_all(access,"</access>","");
  size_t idx;
  if ( (idx=access.find("<p")) == string::npos) {
    idx=access.find("<P");
  }
  if (idx != string::npos) {
    auto idx2=access.find(">",idx);
    access.insert(idx2," style=\"margin: 0px; padding: 0px\"");
  }
  if (!access.empty()) {
    tdoc.add_if("__HAS_ACCESS_RESTRICTIONS__");
    tdoc.add_replacement("__ACCESS_RESTRICTIONS__",access);
  }
// usage restrictions
  e=xdoc.element("dsOverview/restrictions/usage");
  auto usage=e.to_string();
  strutils::replace_all(usage,"<usage>","");
  strutils::replace_all(usage,"</usage>","");
  if ( (idx=usage.find("<p")) == string::npos) {
    idx=usage.find("<P");
  }
  if (idx != string::npos) {
    auto idx2=usage.find(">",idx);
    usage.insert(idx2," style=\"margin: 0px; padding: 0px\"");
  }
  if (!usage.empty()) {
    tdoc.add_if("__HAS_USAGE_RESTRICTIONS__");
    tdoc.add_replacement("__USAGE_RESTRICTIONS__",usage);
  }
// variables
  query.set("select substring_index(path,' > ',-1) as var from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD' and v.dsid = '"+metautils::args.dsnum+"' order by var");
  if (query.submit(server) < 0) {
    log_error2("query: " + query.show() + " returned error: " + query.error(),
        F, "dsgen", USER);
  }
  list<string> strings;
  for (const auto& row : query) {
    strings.emplace_back(strutils::capitalize(row[0]));
  }
  tdoc.add_replacement("__VARIABLES__",create_table_from_strings(strings,4,"#e1eaff","#c8daff"));
  auto elist=xdoc.element_list("dsOverview/contentMetadata/detailedVariables/detailedVariable");
  if (elist.size() > 0) {
    for (const auto& ele : elist) {
      if (regex_search(ele.content(),regex("^http://")) || regex_search(ele.content(),regex("^https://"))) {
        tdoc.add_if("__HAS_DETAILED_VARIABLES__");
        tdoc.add_replacement("__DETAILED_VARIABLES_LINK__",ele.content());
        break;
      }
    }
  }
  if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/grib.html",metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_GRIB_TABLE__");
    auto table="<div>GRIB parameter table:  <a href=\"/datasets/ds"+metautils::args.dsnum+"/#metadata/grib.html?_do=y\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/grib.xml",metautils::directives.rdadata_home)) {
      table+=" | <a href=\"/datasets/ds"+metautils::args.dsnum+"/metadata/grib.xml\">XML</a></div>";
    }
    tdoc.add_replacement("__GRIB_TABLE__",table);
  }
  if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/grib2.html",metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_GRIB2_TABLE__");
    auto table="<div>GRIB2 parameter table:  <a href=\"/datasets/ds"+metautils::args.dsnum+"/#metadata/grib2.html?_do=y\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/grib2.xml",metautils::directives.rdadata_home)) {
      table+=" | <a href=\"/datasets/ds"+metautils::args.dsnum+"/metadata/grib2.xml\">XML</a></div>"; 
    }
    tdoc.add_replacement("__GRIB2_TABLE__",table);
  }
  if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/on84.html",metautils::directives.rdadata_home)) {
    tdoc.add_if("__FOUND_ON84_TABLE__");
    auto table="<div>ON84 parameter table:  <a href=\"/datasets/ds"+metautils::args.dsnum+"/metadata/on84.html\">HTML</a>";
    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/on84.html",metautils::directives.rdadata_home)) {
      table+=" | <a href=\"/datasets/ds"+metautils::args.dsnum+"/metadata/on84.xml\">XML</a></div>";
    }
    tdoc.add_replacement("__ON84_TABLE__",table);
  }
  query.set("gindex,title","dssdb.dsgroup","dsid = 'ds"+metautils::args.dsnum+"' and pindex = 0 and dwebcnt > 0");
  if (query.submit(server) == 0) {
    stringstream vars_by_product_s;
    for (const auto& row : query) {
      if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/customize.WGrML."+row[0],metautils::directives.rdadata_home)) {
        auto c_file=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/customize.WGrML."+row[0],temp_dir.name());
        if (!c_file.empty()) {
          std::ifstream ifs;
          char line[32768];
          ifs.open(c_file);
          ifs.getline(line,32768);
          if (regex_search(line,regex("^curl_subset="))) {
            ifs.getline(line,32768);
          }
          auto nvar=stoi(line);
          list<string> varlist;
          for (int n=0; n < nvar; ++n) {
            ifs.getline(line,32768);
            auto var_parts=strutils::split(line,"<!>");
            varlist.emplace_back(var_parts[1]);
          }
          ifs.close();
          if (varlist.size() > 0) {
            if (vars_by_product_s.tellp() == 0) {
              vars_by_product_s << "<div style=\"position: relative; overflow: hidden\"><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand dataset product variable list\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Variables by dataset product</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse dataset product variable list\"></a><span class=\"fs13px\">Variables by dataset product:";
              div_num+=2;
            }
            vars_by_product_s << "<div style=\"margin-left: 10px\"><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Variable list for " << row[1] << "</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse\"></a><span class=\"fs13px\">Variable list for " << row[1] << ":<div style=\"margin-left: 20px\">";
            div_num+=2;
            for (const auto& var : varlist) {
              vars_by_product_s << var << "<br />";
            }
            vars_by_product_s << "</div></span></span></div>";
          }
        }
      }
    }
    if (vars_by_product_s.tellp() > 0) {
      vars_by_product_s << "</span></span></div>";
      tdoc.add_if("__HAS_VARIABLES_BY_PRODUCT__");
      tdoc.add_replacement("__VARIABLES_BY_PRODUCT__",vars_by_product_s.str());
    }
  }
// vertical levels
  if (found_content_metadata) {
    for (const auto& data_type : data_types) {
      if (data_type == "grid") {
        tdoc.add_if("__HAS_VERTICAL_LEVELS__");
        string vertical_levels="See the <a href=\"#metadata/detailed.html?_do=y&view=level\">detailed metadata</a> for level information";
        if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/grib2_levels.html",metautils::directives.rdadata_home)) {
          vertical_levels+="<br /><a href=\"/datasets/ds"+metautils::args.dsnum+"/#metadata/grib2_levels.html?_do=y\">GRIB2 level table</a>"; 
        }
        tdoc.add_replacement("__VERTICAL_LEVELS__",vertical_levels);
        break;
      }
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/levels/level");
    auto elist2=xdoc.element_list("dsOverview/contentMetadata/levels/layer");
    elist.insert(elist.end(),elist2.begin(),elist2.end());
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_VERTICAL_LEVELS__");
      list<string> levels;
      for (const auto& ele : elist) {
        if ((ele.attribute_value("value") == "0" || (ele.attribute_value("top") == "0" && ele.attribute_value("bottom") == "0")) && ele.attribute_value("units").empty()) {
          levels.emplace_back(ele.attribute_value("type"));
        }
        else {
          if (!ele.attribute_value("value").empty()) {
            auto level=ele.attribute_value("value")+" "+ele.attribute_value("units");
            if (regex_search(level,regex("^\\."))) {
              level="0"+level;
            }
            if (ele.attribute_value("value") != "0" && strutils::contains(ele.attribute_value("type"),"height below")) {
              level="-"+level;
            }
            levels.emplace_back(level);
          }
          else {
            auto layer=ele.attribute_value("top");
            if (regex_search(layer,regex("^\\."))) {
              layer="0"+layer;
            }
            if (ele.attribute_value("top") != ele.attribute_value("bottom")) {
              auto bottom=ele.attribute_value("bottom");
              if (regex_search(bottom,regex("^\\."))) {
                bottom="0"+bottom;
              }
              layer+="-"+bottom;
            }
            layer+=" "+ele.attribute_value("units");
            levels.emplace_back(layer);
          }
        }
      }
      levels.sort(compare_levels);
      tdoc.add_replacement("__VERTICAL_LEVELS__",create_table_from_strings(levels,4,"#c8daff","#e1eaff"));
    }
  }
// temporal frequency
  if (!found_content_metadata) {
    elist=xdoc.element_list("dsOverview/contentMetadata/temporalFrequency");
    auto m=0;
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_TEMPORAL_FREQUENCY__");
      stringstream temporal_frequency_s;
      for (const auto& ele : elist) {
        auto tfreq_type=ele.attribute_value("type");
        if (m > 0) {
          temporal_frequency_s << ", ";
        }
        if (tfreq_type == "regular") {
          auto n=stoi(ele.attribute_value("number"));
          temporal_frequency_s << "Every ";
          if (n > 1) {
            temporal_frequency_s << n << " ";
          }
          temporal_frequency_s << ele.attribute_value("unit");
          if (n > 1) {
            temporal_frequency_s << "s";
          }
          auto stats=ele.attribute_value("statistics");
          if (!stats.empty()) {
            temporal_frequency_s << " (" << strutils::capitalize(stats) << ")";
          }
          temporal_frequency_s << endl;
        }
        else if (tfreq_type == "irregular") {
          temporal_frequency_s << "various times per " << ele.attribute_value("unit");
          auto stats=ele.attribute_value("statistics");
          if (!stats.empty()) {
            temporal_frequency_s << " (" << strutils::capitalize(stats) << ")";
          }
        }
        else if (tfreq_type == "climatology") {
          auto unit=ele.attribute_value("unit");
          if (unit == "hour") {
            unit="Hourly";
          }
          else if (unit == "day") {
            unit="Daily";
          }
          else if (unit == "week") {
            unit="Weekly";
          }
          else if (unit == "month") {
            unit="Monthly";
          }
          else if (unit == "winter") {
            unit="Winter Season";
          }
          else if (unit == "spring") {
            unit="Spring Season";
          }
          else if (unit == "summer") {
            unit="Summer Season";
          }
          else if (unit == "autumn") {
            unit="Autumn Season";
          }
          else if (unit == "year") {
            unit="Annual";
          }
          else if (unit == "30-year") {
            unit="30-year (climate normal)";
          }
          temporal_frequency_s << unit << " Climatology";
        }
        ++m;
      }
      tdoc.add_replacement("__TEMPORAL_FREQUENCY__",temporal_frequency_s.str());
    }
  }
// data types
  if (found_content_metadata) {
    if (data_types.size() > 0) {
      tdoc.add_if("__HAS_DATA_TYPES__");
      string data_types_string;
      auto n=0;
      for (const auto& data_type : data_types) {
        if (n > 0) {
          data_types_string+=", ";
        }
        data_types_string+=strutils::to_capital(data_type);
        ++n;
      }
      tdoc.add_replacement("__DATA_TYPES__",data_types_string);
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/dataType");
    if (elist.size() > 0) {
      tdoc.add_if("__HAS_DATA_TYPES__");
      string data_types_string;
      auto n=0;
      unordered_map<string,char> unique_data_types;
      for (const auto& ele : elist) {
        if (unique_data_types.find(ele.content()) == unique_data_types.end()) {
          if (n++ > 0) {
            data_types_string+=", ";
          }
          data_types_string+=strutils::to_capital(ele.content());
          unique_data_types[ele.content()]='Y';
        }
      }
      tdoc.add_replacement("__DATA_TYPES__",data_types_string);
    }
  }
// spatial coverage
  unordered_map<string,shared_ptr<unordered_set<string>>> unique_grid_definitions_table;
  if (found_content_metadata) {
    unordered_map<size_t,tuple<string,string>> grid_definition_table;
    for (const auto& data_type : data_types) {
      if (data_type == "grid") {
        MySQL::LocalQuery grid_codes_query;
        if (grouped_periods) {
          grid_codes_query.set("select gridDefinition_codes,webID_code from WGrML.ds"+dsnum2+"_grid_definitions");
        }
        else {
          grid_codes_query.set("select distinct gridDefinition_codes from WGrML.ds"+dsnum2+"_agrids");
        }
        if (grid_codes_query.submit(server) < 0) {
          log_error2("error: " + grid_codes_query.error() + " while getting "
              "grid definitions", F, "dsgen", USER);
        }
        for (const auto& grid_codes_row : grid_codes_query) {
          vector<size_t> values;
          bitmap::uncompress_values(grid_codes_row[0],values);
          for (const auto& value : values) {
            string ugd_key;
            if (grid_definition_table.find(value) == grid_definition_table.end()) {
              MySQL::LocalQuery grid_definition_query("definition,defParams","WGrML.gridDefinitions","code = "+strutils::itos(value));
              if (grid_definition_query.submit(server) < 0) {
                log_error2("query: " + grid_definition_query.show() +
                    " returned error: " + grid_definition_query.error(), F,
                    "dsgen", USER);
              }
              MySQL::Row grid_definition_row;
              grid_definition_query.fetch_row(grid_definition_row);
              grid_definition_table.emplace(value,make_tuple(grid_definition_row[0],grid_definition_row[1]));
              ugd_key=grid_definition_row[0]+"<!>"+grid_definition_row[1];
            }
            else {
              auto kval=grid_definition_table[value];
              ugd_key=get<0>(kval)+"<!>"+get<1>(kval);
            }
            string group="";
            if (query.length() > 1) {
              unordered_map<string,string>::iterator m,r,g;
              if ( (m=metadata_files_table.find(grid_codes_row[1])) != metadata_files_table.end() && (r=rda_files_table.find(m->second)) != rda_files_table.end() && (g=groups_table.find(r->second)) != groups_table.end()) {
                group=g->second;
              }
            }
            auto u=unique_grid_definitions_table.find(ugd_key);
            if (u == unique_grid_definitions_table.end()) {
              shared_ptr<unordered_set<string>> g;
              if (!group.empty()) {
                g.reset(new unordered_set<string>);
                g->emplace(group);
              }
              unique_grid_definitions_table.emplace(ugd_key,g);
            }
            else if (!group.empty()) {
              if (u->second == nullptr) {
                u->second.reset(new unordered_set<string>);
              }
              if (u->second->find(group) == u->second->end()) {
                u->second->emplace(group);
              }
            }
          }
        }
        break;
      }
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/geospatialCoverage/grid");
    for (const auto& ele : elist) {
      auto definition=ele.attribute_value("definition");
      if (ele.attribute_value("isCell") == "true") {
        definition+="Cell";
      }
      auto ugd_key=definition+"<!>"+ele.attribute_value("numX")+":"+ele.attribute_value("numY");
      if (regex_search(ugd_key,regex("^(latLon|mercator)"))) {
        ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
      }
      else if (regex_search(ugd_key,regex("^gaussLatLon"))) {
        ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("numY");
      }
      else if (regex_search(ugd_key,regex("^polarStereographic"))) {
        ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":60"+ele.attribute_value("pole")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
      }
      else if (regex_search(ugd_key,regex("^lambertConformal"))) {
        ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("resLat")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes")+":"+ele.attribute_value("stdParallel1")+":"+ele.attribute_value("stdParallel2");
      }
      unique_grid_definitions_table.emplace(ugd_key,nullptr);
    }
  }
  double min_west_lon=9999.,max_east_lon=-9999.,min_south_lat=9999.,max_north_lat=-9999.;
  list<double> straddle_east_lons;
  for (const auto& e : unique_grid_definitions_table) {
    double west_lon,east_lon,south_lat,north_lat;
    if (gridutils::fill_spatial_domain_from_grid_definition(e.first,"primeMeridian",west_lon,south_lat,east_lon,north_lat)) {
      if (east_lon < 0. && east_lon < west_lon) {
// data straddle the date line
        straddle_east_lons.emplace_back(east_lon);
      }
      else {
        if (east_lon > max_east_lon) {
          max_east_lon=east_lon;
        }
      }
      if (west_lon < min_west_lon) {
        min_west_lon=west_lon; 
      }
      if (south_lat < min_south_lat) {
        min_south_lat=south_lat;
      }
      if (north_lat > max_north_lat) {
        max_north_lat=north_lat;
      }
    }
  }
  if (straddle_east_lons.size() > 0 && (min_west_lon > -180. || max_east_lon < 180.)) {
    for (const auto& east_lon : straddle_east_lons) {
      if (east_lon > max_east_lon) {
        max_east_lon=east_lon;
      }
    }
  }
  if (min_south_lat < 9999.) {
    if (min_west_lon < -180.) {
      min_west_lon+=360.;
    }
    if (max_east_lon > 180.) {
      max_east_lon-=360.;
    }
    auto west=strutils::ftos(fabs(min_west_lon),3);
    if (min_west_lon < 0) {
      west+="W";
    }
    else {
      west+="E";
    }
    stringstream spatial_coverage_s;
    spatial_coverage_s << "Longitude Range:  Westernmost=" << west << "  Easternmost=";
    auto east=strutils::ftos(fabs(max_east_lon),3);
    if (max_east_lon < 0) {
      east+="W";
    }
    else {
      east+="E";
    }
    spatial_coverage_s << east << "<br />" << endl;
    auto south=strutils::ftos(fabs(min_south_lat),3);
    if (min_south_lat < 0) {
      south+="S";
    }
    else {
        south+="N";
    }
    spatial_coverage_s << "Latitude Range:  Southernmost=" << south << "  Northernmost=";
    auto north=strutils::ftos(fabs(max_north_lat),3);
    if (max_north_lat < 0) {
      north+="S";
    }
    else {
      north+="N";
    }
    spatial_coverage_s << north << endl;
    spatial_coverage_s << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand coverage details\"><img src=\"/images/triangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Detailed coverage information</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/triangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse coverage details\"></a><span class=\"fs13px\">Detailed coverage information:" << endl;
    div_num+=2;
    map<string,shared_ptr<unordered_set<string>>> grid_definitions;
    for (const auto& e : unique_grid_definitions_table) {
      grid_definitions.emplace(gridutils::convert_grid_definition(e.first),e.second);
    }
    for (const auto& e : grid_definitions) {
      spatial_coverage_s << "<div style=\"margin-left: 10px\">" << e.first;
      if (grid_definitions.size() > 1) {
        if (e.second != nullptr) {
          spatial_coverage_s << "<div style=\"margin-left: 15px; color: #6a6a6a\">(";
          auto n=0;
          for (const auto& group : *e.second) {
            if (n++ > 0) {
              spatial_coverage_s << ", ";
            }
            spatial_coverage_s << group;
          }
          spatial_coverage_s << ")</div>";
        }
      }
      spatial_coverage_s << "</div>";
    }
    spatial_coverage_s << "</span></span>" << endl;
    tdoc.add_if("__HAS_SPATIAL_COVERAGE__");
    tdoc.add_replacement("__SPATIAL_COVERAGE__",spatial_coverage_s.str());
  }
// data contributors
  query.set("select g.path from search.contributors_new as c left join search.GCMD_providers as g on g.uuid = c.keyword where c.dsid = '"+metautils::args.dsnum+"' and c.vocabulary = 'GCMD'");
  if (query.submit(server) < 0) {
    log_error2("query: " + query.show() + " returned error: " + query.error(),
        F, "dsgen", USER);
  }
  stringstream data_contributors_s;
  auto n=0;
  for (const auto& row : query) {
    if (n > 0) {
      data_contributors_s << " | ";
    }
    auto short_name=row[0];
    string long_name="";
    if ( (idx=short_name.find(">")) != string::npos) {
      long_name=short_name.substr(idx+1);
      short_name=short_name.substr(0,idx);
    }
    strutils::trim(short_name);
    strutils::trim(long_name);
    data_contributors_s << "<span class=\"infosrc\" onMouseOver=\"javascript:popInfo(this,'src" << n << "','#e1eaff','left','bottom')\" onMouseOut=\"javascript:hideInfo('src" << n << "')\">" << short_name << "</span><div class=\"info\" id=\"src" << n << "\" class=\"source\"><small>" << long_name << "</small></div>";
    ++n;
  }
  tdoc.add_replacement("__DATA__CONTRIBUTORS__",data_contributors_s.str());
// related web sites
  elist=xdoc.element_list("dsOverview/relatedResource");
  if (elist.size() > 0) {
    if (elist.size() > 1) {
      tdoc.add_replacement("__WEB_SITES_VALIGN__","top");
    }
    else {
      tdoc.add_replacement("__WEB_SITES_VALIGN__","bottom");
    }
    stringstream related_web_sites_s;
    for (const auto& ele : elist) {
      auto description=ele.content();
      strutils::trim(description);
      if (strutils::has_ending(description,".") && description[description.length()-2] != '.') {
        strutils::chop(description);
      }
      related_web_sites_s << "<a href=\"" << ele.attribute_value("url") << "\">";
      auto is_local=false;
      auto url=ele.attribute_value("url");
      if (regex_search(url,regex("^http://rda.ucar.edu")) || regex_search(url,regex("^https://rda.ucar.edu")) || regex_search(url,regex("^http://dss.ucar.edu")) || regex_search(url,regex("^https://dss.ucar.edu"))) {
        is_local=true;
      }
      if (!is_local) {
        related_web_sites_s << "<span class=\"italic\">";
      }
      related_web_sites_s << description;
      if (!is_local) {
        related_web_sites_s << "</span>";
      }
      related_web_sites_s << "</a><br />";
    }
    tdoc.add_if("__HAS_RELATED_WEB_SITES__");
    tdoc.add_replacement("__RELATED_WEB_SITES__",related_web_sites_s.str());
  }

// publications
  add_publications(tdoc,xdoc);

// volume
  query.set("primary_size","dssdb.dataset","dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    log_error2("query: " + query.show() + " returned error: " + query.error(),
        F, "dsgen", USER);
  }
  MySQL::LocalQuery query2("select primary_size,title,grpid from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"' and pindex = 0 and primary_size > 0");
  if (query2.submit(server) < 0) {
    log_error2("query: " + query2.show() + " returned error: " + query2.error(),
        F, "dsgen", USER);
  }
  stringstream volume_s;
  const int VOLUME_LEN=4;
  const char *v[VOLUME_LEN]={"MB","GB","TB","PB"};
  MySQL::Row row;
  if (query.fetch_row(row) && !row[0].empty()) {
    auto volume=stof(row[0])/1000000.;
    n=0;
    while (volume > 1000. && n < VOLUME_LEN) {
      volume/=1000.;
      ++n;
    }
    volume_s << strutils::ftos(llround(volume*100.)/100.,6,2,' ') << " " << v[n];
  }
  if (query2.num_rows() > 1) {
    volume_s << " <span class=\"fs13px\">(Entire dataset)</span><br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\" title=\"Expand dataset product volume list\"><font size=\"-1\">Volume details by dataset product</font></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\" title=\"Collapse dataset product volume list\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\"></a><span class=\"fs13px\">Volume details by dataset product:";
    div_num+=2;
    for (const auto& row2: query2) {
      auto volume=stof(row2[0])/1000000.;
      n=0;
      while (volume > 1000. && n < VOLUME_LEN) {
        volume/=1000.;
        ++n;
      }
      volume_s << "<div style=\"margin-left: 10px\">";
      if (!row2[1].empty()) {
        volume_s << row2[1];
      }
      else {
        volume_s << row2[2];
      }
      volume_s << ": " << strutils::ftos(llround(volume*100.)/100.,6,2,' ') << " " << v[n] << "</div>";
    }
    volume_s << "</span></span>";
  }
  tdoc.add_replacement("__VOLUME__",volume_s.str());

// data formats
  add_data_formats(tdoc,formats,found_content_metadata);

// related datasets
  elist=xdoc.element_list("dsOverview/relatedDataset");
  if (elist.size() > 0) {
    if (elist.size() > 1) {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__","top");
    }
    else {
      tdoc.add_replacement("__RELATED_DATASETS_VALIGN__","bottom");
    }
    elist.sort(
    [](XMLElement& left,XMLElement& right) -> bool
    {
      if (left.attribute_value("ID") <= right.attribute_value("ID")) {
        return true;
      }
      else {
        return false;
      }
    });
    string related_datasets;
    for (const auto& ele : elist) {
      query.set("dsid,title","search.datasets","dsid = '"+ele.attribute_value("ID")+"' and (type = 'P' or type = 'H')");
      if (query.submit(server) < 0) {
        log_error2("query: " + query.show() + " returned error: " + query
            .error(), F, "dsgen", USER);
      }
      MySQL::Row row;
      if (query.fetch_row(row)) {
        related_datasets+="<tr valign=\"top\"><td><a href=\"/datasets/ds"+row[0]+"#description\">"+row[0]+"</a></td><td>-</td><td>"+row[1]+"</td></tr>";
      }
    }
    tdoc.add_if("__HAS_RELATED_DATASETS__");
    tdoc.add_replacement("__RELATED_DATASETS__",related_datasets);
  }

// more details
  if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/detailed.html",metautils::directives.rdadata_home)) {
    tdoc.add_if("__HAS_MORE_DETAILS__");
  }

// data citations
  add_citations(tdoc);

  ofs << tdoc;
  ofs.close();
  delete t;
}

int main(int argc,char **argv)
{
  if (argc != 2 && argc != 3) {
    std::cerr << "usage: dsgen nnn.n" << endl;
    std::cerr << "\noptions:" << endl;
    std::cerr << "--no-dset-waf  don't add the dataset to the queue for the DSET WAF" << endl;
    exit(1);
  }
  const string F = this_function_label(__func__);
  auto next=1;
  if (string(argv[next]) == "--no-dset-waf") {
    no_dset_waf=true;
    ++next;
  }
  metautils::args.dsnum=argv[next];
  if (regex_search(metautils::args.dsnum,regex("^ds"))) {
    metautils::args.dsnum=metautils::args.dsnum.substr(2);
  }
  if (metautils::args.dsnum >= "999.0") {
    no_dset_waf=true;
  }
  metautils::args.args_string=unixutils::unix_args_string(argc,argv);
  metautils::read_config("dsgen",USER);
  if (!temp_dir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, "dsgen", USER);
  }
  server.connect(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::LocalQuery query("select type from search.datasets where dsid = '"+metautils::args.dsnum+"'");
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    log_error2("unable to determine dataset type", F, "dsgen", USER);
  }
  dataset_type=row[0];
  if (!no_dset_waf) {
    if (dataset_type != "P" && dataset_type != "H") {
      no_dset_waf=true;
    }
  }
  TempDir dataset_doc_dir;
  if (!dataset_doc_dir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary document directory", F, "dsgen",
        USER);
  }
  generate_index(dataset_type,dataset_doc_dir.name());
  if (dataset_type != "I") {
    generate_description(dataset_type,dataset_doc_dir.name());
    xdoc.close();
  }
  if (!no_dset_waf) {
    if (server.insert("metautil.dset_waf","'"+metautils::args.dsnum+"',''","update dsid = values(dsid)") < 0) {
      metautils::log_warning("not marked for DSET WAF update","dsgen",USER);
    }
  }
  server.disconnect();
  string remote_path="/data/web";
  if (dataset_type == "W") {
    remote_path+="/internal";
  }
  remote_path+="/datasets/ds"+metautils::args.dsnum;
  string error;
  if (unixutils::rdadata_sync(dataset_doc_dir.name(),".",remote_path,metautils::directives.rdadata_home,error) < 0) {
    metautils::log_warning("couldn't sync dataset files - rdadata_sync error(s): '"+error+"'","dsgen",USER);
  }
}
