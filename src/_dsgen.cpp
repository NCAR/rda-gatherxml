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

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";
extern const std::string USER=getenv("USER");

TempDir temp_dir;
XMLDocument xdoc;
MySQL::Server server;
std::string dataset_type;
bool no_dset_waf=false;

void generate_index(std::string type,std::string tdir_name)
{
  std::ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name+"/test_index.html").c_str());
  }
  else {
    ofs.open((tdir_name+"/index.html").c_str());
  }
  if (!ofs.is_open()) {
    metautils::log_error("unable to open output for 'index.html'","dsgen",USER);
  }
  auto t=new TokenDocument("/glade/u/home/rdadata/share/templates/dsgen_index.tdoc");
  if (!*t) {
    delete t;
    t=new TokenDocument("/usr/local/dss/share/templates/dsgen_index.tdoc");
    if (!*t) {
	metautils::log_error("index template not found or unavailable","dsgen",USER);
    }
  }
  auto &tdoc=*t;
  tdoc.add_replacement("__DSNUM__",metautils::args.dsnum);
  if (dataset_type == "I") {
    tdoc.add_if("__IS_INTERNAL_DATASET__");
  }
  else {
    struct stat buf;
    std::string ds_overview;
    if (stat(("/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
	ds_overview="/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml";
    }
    else {
	ds_overview=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml",temp_dir.name());
	if (ds_overview.empty()) {
	  metautils::log_error("dsOverview.xml does not exist for "+metautils::args.dsnum,"dsgen",USER);
	}
    }
    if (!xdoc.open(ds_overview)) {
	metautils::log_error("unable to open dsOverview.xml for "+metautils::args.dsnum+"; parse error: '"+xdoc.parse_error()+"'","dsgen",USER);
    }
    std::stringstream dc_meta_tags_s;
    if (!metadataExport::export_to_dc_meta_tags(dc_meta_tags_s,metautils::args.dsnum,xdoc,0)) {
	metautils::log_error("unable to export DC meta tags: '"+myerror+"'","dsgen",USER);
    }
    tdoc.add_replacement("__DC_META_TAGS__",dc_meta_tags_s.str());
    auto e=xdoc.element("dsOverview/title");
    auto title=e.content();
    tdoc.add_replacement("__TITLE__",title);
    std::stringstream json_ld_s;
    if (!metadataExport::export_to_json_ld(json_ld_s,metautils::args.dsnum,xdoc,0)) {
	metautils::log_error("unable to export JSON-LD metadata","dsgen",USER);
    }
    tdoc.add_replacement("__JSON_LD__",json_ld_s.str());
    e=xdoc.element("dsOverview/logo");
    if (!e.content().empty()) {
	auto logo_parts=strutils::split(e.content(),".");
	auto geometry_parts=strutils::split(logo_parts[logo_parts.size()-2],"_");
	auto width=std::stoi(geometry_parts[geometry_parts.size()-2]);
	auto height=std::stoi(geometry_parts[geometry_parts.size()-1]);
	tdoc.add_replacement("__LOGO_IMAGE__",e.content());
	tdoc.add_replacement("__LOGO_WIDTH__",strutils::itos(lroundf(width*70./height)));
    }
    else {
	tdoc.add_replacement("__LOGO_IMAGE__","default_200_200.png");
	tdoc.add_replacement("__LOGO_WIDTH__","70");
    }
    MySQL::Query query("doi","dssdb.dsvrsn","dsid = 'ds"+metautils::args.dsnum+"' and status = 'A'");
    MySQL::Row row;
    std::string doi_span;
    if (query.submit(server) == 0 && query.fetch_row(row) && !row[0].empty()) {
	doi_span="&nbsp;|&nbsp;<span class=\"blue\">DOI: "+row[0]+"</span>";
    }
    tdoc.add_replacement("__DOI_SPAN__",doi_span);
    e=xdoc.element("dsOverview/contact");
    auto contact_parts=strutils::split(e.content());
    query.set("select logname,phoneno from dssdb.dssgrp where fstname = '"+contact_parts[0]+"' and lstname = '"+contact_parts[1]+"'");
    if (query.submit(server) < 0) {
	metautils::log_error("mysql error while trying to get specialist information: "+query.error(),"dsgen",USER);
    }
    if (!query.fetch_row(row)) {
	metautils::log_error("no result returned for specialist '"+e.content()+"'","dsgen",USER);
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

bool compare_strings(const std::string& left,const std::string& right)
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

bool compare_levels(const std::string& left,const std::string& right)
{
  if (((left[0] >= '0' && left[0] <= '9') || (left[0] == '-' && left[1] >= '0' && left[1] <= '9')) && ((right[0] >= '0' && right[0] <= '9') || (right[0] == '-' && right[1] >= '0' && right[1] <= '9'))) {
    auto lidx=left.find(" ");
    std::string lval,lunits;
    if (lidx != std::string::npos) {
	lval=left.substr(0,lidx);
	lunits=left.substr(lidx+1);
    }
    else {
	lval=left;
	lunits="";
    }
    auto ridx=right.find(" ");
    std::string rval,runits;
    if (ridx != std::string::npos) {
	rval=right.substr(0,ridx);
	runits=right.substr(ridx+1);
    }
    else {
	rval=right;
	runits="";
    }
    auto ilval=std::stoi(lval);
    auto irval=std::stoi(rval);
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

std::string create_table_from_strings(std::list<std::string> list,int max_columns,std::string color1,std::string color2)
{
  std::stringstream ss;
  ss << "<table cellspacing=\"0\">" << std::endl;
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
	ss << "<tr style=\"vertical-align: top\">" << std::endl;
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
    ss << "\">"+item+"</td>" << std::endl;
    if (n == cmax) {
	ss << "</tr>" << std::endl;
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
	  ss << "\">&nbsp;</td>" << std::endl;
	}
    }
    ss << "</tr>";
  }
  ss << "</table>" << std::endl;
  return ss.str();
}

void insert_table(std::ofstream& ofs,std::list<std::string> list,int max_columns,std::string color1,std::string color2)
{
  ofs << "<table cellspacing=\"0\">" << std::endl;
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
	ofs << "<tr style=\"vertical-align: top\">" << std::endl;
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
    ofs << "\">"+item+"</td>" << std::endl;
    if (n == cmax) {
	ofs << "</tr>" << std::endl;
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
	  ofs << "\">&nbsp;</td>" << std::endl;
	}
    }
    ofs << "</tr>";
  }
  ofs << "</table>" << std::endl;
}

std::string text_field_from_element(const XMLElement& e)
{
  auto s=e.to_string();
  if (!s.empty()) {
    strutils::replace_all(s,"<"+e.name()+">","");
    strutils::replace_all(s,"</"+e.name()+">","");
    strutils::trim(s);
    size_t idx;
    if ( (idx=s.find("<p")) == std::string::npos) {
	idx=s.find("<P");
    }
    if (idx == 0) {
	auto idx2=s.find(">",idx);
	s.insert(idx2," style=\"margin: 0px; padding: 0px\"");
    }
  }
  return s;
}

void insert_text_field(std::ofstream& ofs,const XMLElement& e,std::string section_title)
{
  auto s=e.to_string();
  if (!s.empty()) {
    strutils::replace_all(s,"<"+e.name()+">","");
    strutils::replace_all(s,"</"+e.name()+">","");
    strutils::trim(s);
    size_t idx;
    if ( (idx=s.find("<p")) == std::string::npos) {
	idx=s.find("<P");
    }
    if (idx == 0) {
	auto idx2=s.find(">",idx);
	s.insert(idx2," style=\"margin: 0px; padding: 0px\"");
    }
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">" << section_title << ":</td><td>" << s << "</td></tr>" << std::endl;
  }
}

void generate_description(std::string type,std::string tdir_name)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  std::ofstream ofs;
  if (dataset_type == "W") {
    ofs.open((tdir_name+"/test_description.html").c_str());
  }
  else {
    ofs.open((tdir_name+"/description.html").c_str());
  }
  if (!ofs.is_open()) {
    metautils::log_error("unable to open output for 'description.html'","dsgen",USER);
  }
  auto t=new TokenDocument("/glade/u/home/rdadata/share/templates/dsgen_description.tdoc");
  if (!*t) {
    t=new TokenDocument("/usr/local/dss/share/templates/dsgen_description.tdoc");
    if (!*t) {
	metautils::log_error("description template not found or unavailable","dsgen",USER);
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
    ofs << "<ul>This dataset has been removed from public view.  If you have questions about this dataset, please contact the specialist that is named above.</ul>" << std::endl;
    ofs.close();
    return;
  }
*/
  auto databases=metautils::cmd_databases("dsgen","x");
  if (databases.size() == 0) {
    metautils::log_error("empty CMD database list","dsgen",USER);
  }
  std::vector<std::string> formats,types;
  auto found_content_metadata=false;
  for (const auto& db : databases) {
    std::string db_name,data_type;
    std::tie(db_name,data_type)=db;
    if (db_name[0] != 'V' && table_exists(server,db_name+".ds"+dsnum2+"_primaries")) {
	MySQL::LocalQuery query("select distinct format from "+db_name+".formats as f left join "+db_name+".ds"+dsnum2+"_primaries as d on d.format_code = f.code where !isnull(d.format_code)");
	if (query.submit(server) < 0) {
	  metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
	}
	MySQL::Row row;
	while (query.fetch_row(row)) {
	  formats.emplace_back(row[0]+"<!>");
	}
	if (formats.size() > 0) {
	  found_content_metadata=true; 
	  if (db == "GrML") {
	    types.emplace_back("grid");
	  }
	  else if (db == "ObML") {
	    types.emplace_back("platform_observation");
	  }
	  else if (db == "SatML") {
	    types.emplace_back("satellite");
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
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
  }
  if (query.num_rows() > 0) {
    query.set("select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) where p.dsid = 'ds"+metautils::args.dsnum+"' and g.pindex = 0 and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' union select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g2.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) left join dssdb.dsgroup as g2 on (p.dsid = g2.dsid and g.pindex = g2.gindex) where p.dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' and !isnull(g2.title) order by title");
  }
  else {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
  }
  if (query.num_rows() == 0) {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
    if (query.submit(server) < 0) {
	metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
    }
  }
  auto div_num=0;
  bool grouped_periods=false;
  std::unordered_map<std::string,std::string> groups_table,rda_files_table,metadata_files_table;
  if (query.num_rows() > 0) {
    tdoc.add_if("__HAS_TEMPORAL_RANGE__");
    if (query.num_rows() > 1) {
	MySQL::LocalQuery query2("distinct gindex","dssdb.dsperiod","dsid = 'ds"+metautils::args.dsnum+"'");
	if (query2.submit(server) < 0) {
	  metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",USER);
	}
	if (query2.num_rows() > 1) {
	  grouped_periods=true;
	  tdoc.add_if("__HAS_TEMPORAL_BY_GROUP1__");
	  query2.set("select gindex,title from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"'");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("error: "+query2.error()+" while getting groups data","dsgen",USER);
	  }
	  MySQL::Row row2;
	  while (query2.fetch_row(row2)) {
	    groups_table.emplace(row2[0],row2[1]);
	  }
	  query2.set("select mssfile,tindex from dssdb.mssfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'P' and status = 'P'");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("error: "+query2.error()+" while getting RDA files data","dsgen",USER);
	  }
	  while (query2.fetch_row(row2)) {
	    rda_files_table.emplace(row2[0],row2[1]);
	  }
	  query2.set("select code,mssID from GrML.ds"+dsnum2+"_primaries");
	  if (query2.submit(server) == 0) {
	    while (query2.fetch_row(row2)) {
		metadata_files_table.emplace(row2[0],row2[1]);
	    }
	  }
	  query2.set("select min(concat(date_start,' ',time_start)),min(start_flag),max(concat(date_end,' ',time_end)),min(end_flag),any_value(time_zone) from dssdb.dsperiod where dsid = 'ds"+metautils::args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' group by dsid");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",USER);
	  }
	  query2.fetch_row(row2);
	  auto start_date_time=metatranslations::date_time(row2[0],row2[1],row2[4]);
	  auto end_date_time=metatranslations::date_time(row2[2],row2[3],row2[4]);
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
    std::map<std::string,std::tuple<std::string,std::string>> periods_table;
    MySQL::Row row;
    while (query.fetch_row(row)) {
	auto start_date_time=metatranslations::date_time(row[0]+" "+row[1],row[2],row[6]);
	auto end_date_time=metatranslations::date_time(row[3]+" "+row[4],row[5],row[6]);
	std::string key;
	if (!row[7].empty()) {
	  key=row[7];
	}
	else {
	  key=row[8];
	}
	if (periods_table.find(key) == periods_table.end()) {
	  periods_table.emplace(key,std::make_tuple(start_date_time,end_date_time));
	}
	else {
	  std::string &start=std::get<0>(periods_table[key]);
	  if (start_date_time < start) {
	    start=start_date_time;
	  }
	  std::string &end=std::get<1>(periods_table[key]);
	  if (end_date_time > end) {
	    end=end_date_time;
	  }
	}   
    }
    for (const auto& e : periods_table) {
	auto temporal=std::get<0>(e.second);
	auto end=std::get<1>(e.second);
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
  if ( (idx=access.find("<p")) == std::string::npos) {
    idx=access.find("<P");
  }
  if (idx != std::string::npos) {
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
  if ( (idx=usage.find("<p")) == std::string::npos) {
    idx=usage.find("<P");
  }
  if (idx != std::string::npos) {
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
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
  }
  std::list<std::string> strings;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    strings.emplace_back(strutils::capitalize(row[0]));
  }
  tdoc.add_replacement("__VARIABLES__",create_table_from_strings(strings,4,"#e1eaff","#c8daff"));
  auto elist=xdoc.element_list("dsOverview/contentMetadata/detailedVariables/detailedVariable");
  if (elist.size() > 0) {
    for (const auto& ele : elist) {
	if (std::regex_search(ele.content(),std::regex("^http://")) || std::regex_search(ele.content(),std::regex("^https://"))) {
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
  query.set("gindex,title","dssdb.dsgroup","dsid = 'ds"+metautils::args.dsnum+"' and pindex = 0 and pmsscnt > 0");
  if (query.submit(server) == 0) {
    std::stringstream vars_by_product_s;
    while (query.fetch_row(row)) {
	if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/customize.GrML."+row[0],metautils::directives.rdadata_home)) {
	  auto c_file=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/customize.GrML."+row[0],temp_dir.name());
	  if (!c_file.empty()) {
	    std::ifstream ifs;
	    char line[32768];
	    ifs.open(c_file);
	    ifs.getline(line,32768);
	    if (std::regex_search(line,std::regex("^curl_subset="))) {
		ifs.getline(line,32768);
	    }
	    auto nvar=std::stoi(line);
	    std::list<std::string> varlist;
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
    for (const auto& type : types) {
	if (type == "grid") {
	  tdoc.add_if("__HAS_VERTICAL_LEVELS__");
	  std::string vertical_levels="See the <a href=\"#metadata/detailed.html?_do=y&view=level\">detailed metadata</a> for level information";
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
	std::list<std::string> levels;
	for (const auto& ele : elist) {
	  if ((ele.attribute_value("value") == "0" || (ele.attribute_value("top") == "0" && ele.attribute_value("bottom") == "0")) && ele.attribute_value("units").empty()) {
	    levels.emplace_back(ele.attribute_value("type"));
	  }
	  else {
	    if (!ele.attribute_value("value").empty()) {
		auto level=ele.attribute_value("value")+" "+ele.attribute_value("units");
		if (std::regex_search(level,std::regex("^\\."))) {
		  level="0"+level;
		}
		if (ele.attribute_value("value") != "0" && strutils::contains(ele.attribute_value("type"),"height below")) {
		  level="-"+level;
		}
		levels.emplace_back(level);
	    }
	    else {
		auto layer=ele.attribute_value("top");
		if (std::regex_search(layer,std::regex("^\\."))) {
		  layer="0"+layer;
		}
		if (ele.attribute_value("top") != ele.attribute_value("bottom")) {
		  auto bottom=ele.attribute_value("bottom");
		  if (std::regex_search(bottom,std::regex("^\\."))) {
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
	std::stringstream temporal_frequency_s;
	for (const auto& ele : elist) {
	  auto tfreq_type=ele.attribute_value("type");
	  if (m > 0) {
	    temporal_frequency_s << ", ";
	  }
	  if (tfreq_type == "regular") {
	    auto n=std::stoi(ele.attribute_value("number"));
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
	    temporal_frequency_s << std::endl;
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
    if (types.size() > 0) {
	tdoc.add_if("__HAS_DATA_TYPES__");
	std::string data_types;
	auto n=0;
	for (const auto& type : types) {
	  if (n > 0) {
	    data_types+=", ";
	  }
	  data_types+=strutils::to_capital(type);
	  ++n;
	}
	tdoc.add_replacement("__DATA_TYPES__",data_types);
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/dataType");
    if (elist.size() > 0) {
	tdoc.add_if("__HAS_DATA_TYPES__");
	std::string data_types;
	auto n=0;
	std::unordered_map<std::string,char> unique_data_types;
	for (const auto& ele : elist) {
	  if (unique_data_types.find(ele.content()) == unique_data_types.end()) {
	    if (n++ > 0) {
		data_types+=", ";
	    }
	    data_types+=strutils::to_capital(ele.content());
	    unique_data_types[ele.content()]='Y';
	  }
	}
	tdoc.add_replacement("__DATA_TYPES__",data_types);
    }
  }
// spatial coverage
  std::unordered_map<std::string,std::shared_ptr<std::unordered_set<std::string>>> unique_grid_definitions_table;
  if (found_content_metadata) {
    std::unordered_map<size_t,std::tuple<std::string,std::string>> grid_definition_table;
    for (const auto& type : types) {
	if (type == "grid") {
	  if (grouped_periods) {
	    query.set("select gridDefinition_codes,mssID_code from GrML.ds"+dsnum2+"_grid_definitions");
	  }
	  else {
	    query.set("select distinct gridDefinition_codes from GrML.ds"+dsnum2+"_agrids");
	  }
	  if (query.submit(server) < 0) {
	    metautils::log_error("error: "+query.error()+" while getting grid definitions","dsgen",USER);
	  }
	  while (query.fetch_row(row)) {
	    std::vector<size_t> values;
	    bitmap::uncompress_values(row[0],values);
	    for (const auto& value : values) {
		std::string ugd_key;
		if (grid_definition_table.find(value) == grid_definition_table.end()) {
		  MySQL::LocalQuery query2("definition,defParams","GrML.gridDefinitions","code = "+strutils::itos(value));
		  if (query2.submit(server) < 0) {
		    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",USER);
		  }
		  MySQL::Row row2;
		  query2.fetch_row(row2);
		  grid_definition_table.emplace(value,std::make_tuple(row2[0],row2[1]));
		  ugd_key=row2[0]+"<!>"+row2[1];
		}
		else {
		  auto kval=grid_definition_table[value];
		  ugd_key=std::get<0>(kval)+"<!>"+std::get<1>(kval);
		}
		std::string group="";
		if (query.length() > 1) {
		  std::unordered_map<std::string,std::string>::iterator m,r,g;
		  if ( (m=metadata_files_table.find(row[1])) != metadata_files_table.end() && (r=rda_files_table.find(m->second)) != rda_files_table.end() && (g=groups_table.find(r->second)) != groups_table.end()) {
		    group=g->second;
		  }
		}
		auto u=unique_grid_definitions_table.find(ugd_key);
		if (u == unique_grid_definitions_table.end()) {
		  std::shared_ptr<std::unordered_set<std::string>> g;
		  if (!group.empty()) {
		    g.reset(new std::unordered_set<std::string>);
		    g->emplace(group);
		  }
		  unique_grid_definitions_table.emplace(ugd_key,g);
		}
		else if (!group.empty()) {
		  if (u->second == nullptr) {
		    u->second.reset(new std::unordered_set<std::string>);
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
	if (std::regex_search(ugd_key,std::regex("^(latLon|mercator)"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(ugd_key,std::regex("^gaussLatLon"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("numY");
	}
	else if (std::regex_search(ugd_key,std::regex("^polarStereographic"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":60"+ele.attribute_value("pole")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(ugd_key,std::regex("^lambertConformal"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("resLat")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes")+":"+ele.attribute_value("stdParallel1")+":"+ele.attribute_value("stdParallel2");
	}
	unique_grid_definitions_table.emplace(ugd_key,nullptr);
    }
  }
  double min_west_lon=9999.,max_east_lon=-9999.,min_south_lat=9999.,max_north_lat=-9999.;
  std::list<double> straddle_east_lons;
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
    std::stringstream spatial_coverage_s;
    spatial_coverage_s << "Longitude Range:  Westernmost=" << west << "  Easternmost=";
    auto east=strutils::ftos(fabs(max_east_lon),3);
    if (max_east_lon < 0) {
	east+="W";
    }
    else {
	east+="E";
    }
    spatial_coverage_s << east << "<br />" << std::endl;
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
    spatial_coverage_s << north << std::endl;
    spatial_coverage_s << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand coverage details\"><img src=\"/images/triangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Detailed coverage information</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/triangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse coverage details\"></a><span class=\"fs13px\">Detailed coverage information:" << std::endl;
    div_num+=2;
    std::map<std::string,std::shared_ptr<std::unordered_set<std::string>>> grid_definitions;
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
    spatial_coverage_s << "</span></span>" << std::endl;
    tdoc.add_if("__HAS_SPATIAL_COVERAGE__");
    tdoc.add_replacement("__SPATIAL_COVERAGE__",spatial_coverage_s.str());
  }
// data contributors
  query.set("select g.path from search.contributors_new as c left join search.GCMD_providers as g on g.uuid = c.keyword where c.dsid = '"+metautils::args.dsnum+"' and c.vocabulary = 'GCMD'");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
  }
  std::stringstream data_contributors_s;
  auto n=0;
  while (query.fetch_row(row)) {
    if (n > 0) {
	data_contributors_s << " | ";
    }
    auto short_name=row[0];
    std::string long_name="";
    if ( (idx=short_name.find(">")) != std::string::npos) {
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
    std::stringstream related_web_sites_s;
    for (const auto& ele : elist) {
	auto description=ele.content();
	strutils::trim(description);
	if (strutils::has_ending(description,".") && description[description.length()-2] != '.') {
	  strutils::chop(description);
	}
	related_web_sites_s << "<a href=\"" << ele.attribute_value("url") << "\">";
	auto is_local=false;
	auto url=ele.attribute_value("url");
	if (std::regex_search(url,std::regex("^http://rda.ucar.edu")) || std::regex_search(url,std::regex("^https://rda.ucar.edu")) || std::regex_search(url,std::regex("^http://dss.ucar.edu")) || std::regex_search(url,std::regex("^https://dss.ucar.edu"))) {
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
  elist=xdoc.element_list("dsOverview/reference");
  std::stringstream publications_s;
  if (elist.size() > 0) {
    tdoc.add_if("__HAS_PUBLICATIONS__");
    elist.sort(compare_references);
    for (const auto& ele : elist) {
	publications_s << "<div>" << ele.element("authorList").content() << ", " << ele.element("year").content() << ": ";
	auto pub_type=ele.attribute_value("type");
	if (pub_type == "journal") {
	  e=ele.element("periodical");
	  auto url=ele.element("url").content();
	  auto title=ele.element("title").content();
	  if (!url.empty()) {
	    publications_s << "<a href=\"" << url << "\">" << title << "</a>";
	  }
	  else {
	    publications_s << title;
	  }
	  if (!strutils::has_ending(title,"?")) {
	    publications_s << ".";
	  }
	  publications_s << "  <i>" << e.content() << "</i>, ";
	  if (e.attribute_value("pages") == "0-0") {
	    if (e.attribute_value("number") == "0") {
		publications_s << "Submitted";
	    }
	    else if (e.attribute_value("number") == "1") {
		publications_s << "Accepted";
	    }
	    else if (e.attribute_value("number") == "2") {
		publications_s << "In Press";
	    }
	  }
	  else {
	    publications_s << "<b>" << e.attribute_value("number") << "</b>, ";
	    auto pages=e.attribute_value("pages");
	    if (std::regex_search(pages,std::regex("^AGU:"))) {
		publications_s << pages.substr(4);
	    }
	    else {
		auto page_parts=strutils::split(pages,"-"); 
		if (page_parts.size() == 2 && page_parts[0] == page_parts[1]) {
		  publications_s << page_parts[0];
		}
		else {
		  publications_s << e.attribute_value("pages");
		}
	    }
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    publications_s << " (DOI: " << doi << ")";
	  }
	  publications_s << ".";
	}
	else if (pub_type == "preprint") {
	  e=ele.element("conference");
	  auto url=ele.element("url").content();
	  if (!url.empty()) {
	    publications_s << "<a href=\"" << url << "\">" << ele.element("title").content() << "</a>";
	  }
	  else {
	    publications_s << ele.element("title").content();
	  }
	  publications_s << ".  <i>Proceedings of the " << e.content() << "</i>, " << e.attribute_value("host") << ", " << e.attribute_value("location");
	  auto pages=e.attribute_value("pages");
	  if (!pages.empty()) {
	    publications_s << ", " << pages;
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    publications_s << " (DOI: " << doi << ")";
	  }
	  publications_s << ".";
	}
	else if (pub_type == "technical_report") {
	  e=ele.element("organization");
	  auto url=ele.element("url").content();
	  if (!url.empty()) {
	    publications_s << "<i><a href=\"" << url << "\">" << ele.element("title").content() << "</a>.</i>";
	  }
	  else {
	    publications_s << "<i>" << ele.element("title").content() << ".</i>";
	  }
	  publications_s << "  ";
	  auto report_ID=e.attribute_value("reportID");
	  if (!report_ID.empty()) {
	    publications_s << report_ID << ", ";
	  }
	  publications_s << e.content();
	  auto pages=e.attribute_value("pages");
	  if (pages != "-99") {
	    publications_s << ", " << e.attribute_value("pages") << " pp.";
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    publications_s << " (DOI: " << doi << ").";
	  }
	}
	else if (pub_type == "book") {
	  e=ele.element("publisher");
	  publications_s << "<i>" << ele.element("title").content() << "</i>. " << e.content() << ", " << e.attribute_value("place");
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    publications_s << " (DOI: " << doi << ")";
	  }
	  publications_s << ".";
	}
	else if (pub_type == "book_chapter") {
	  e=ele.element("book");
	  publications_s << "\"" << ele.element("title").content() << "\", in " << e.content() << ". Ed. " << e.attribute_value("editor") << ", " << e.attribute_value("publisher") << ", ";
	  if (e.attribute_value("pages") == "0-0") {
	    publications_s << "In Press";
	  }
	  else {
	    publications_s << e.attribute_value("pages");
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    publications_s << " (DOI: " << doi << ")";
	  }
	  publications_s << ".";
	}
	publications_s << "</div>" << std::endl;
	auto annotation=ele.element("annotation").content();
	if (!annotation.empty() > 0) {
	  publications_s << "<div style=\"margin-left: 15px; color: #5f5f5f\">" << annotation << "</div>" << std::endl;
	}
	publications_s << "<br />" << std::endl;
    }
  }
  elist=xdoc.element_list("dsOverview/referenceURL");
  if (elist.size() > 0) {
    if (publications_s.str().empty()) {
	tdoc.add_if("__HAS_PUBLICATIONS__");
    }
    for (const auto& ele : elist) {
	publications_s << "<a href=\"" << ele.attribute_value("url") << "\">" << ele.content() << "</a><br>" << std::endl;
    }
  }
  if (!publications_s.str().empty()) {
    tdoc.add_replacement("__PUBLICATIONS__",publications_s.str());
  }
// volume
  query.set("primary_size","dssdb.dataset","dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
  }
  MySQL::LocalQuery query2("select primary_size,title,grpid from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"' and pindex = 0 and primary_size > 0");
  if (query2.submit(server) < 0) {
    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",USER);
  }
  std::stringstream volume_s;
  const int VOLUME_LEN=4;
  const char *v[VOLUME_LEN]={"MB","GB","TB","PB"};
  if (query.fetch_row(row) && !row[0].empty()) {
    auto volume=std::stof(row[0])/1000000.;
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
    MySQL::Row row2;
    while (query2.fetch_row(row2)) {
	auto volume=std::stof(row2[0])/1000000.;
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
  if (!found_content_metadata) {
    elist=xdoc.element_list("dsOverview/contentMetadata/format");
    for (const auto& ele : elist) {
	formats.emplace_back(ele.content()+"<!>"+ele.attribute_value("href"));
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
    metautils::log_error("unable to open FormatReferences.xml","dsgen",USER);
  }
  std::string data_formats;
  n=0;
  for (const auto& format : formats) {
    auto format_parts=strutils::split(format,"<!>");
    auto description=format_parts[0];
    std::string url;
    if (std::regex_search(description,std::regex("^proprietary_"))) {
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
	e=fdoc.element(("formatReferences/format@name="+description));
	url=e.attribute_value("href");
    }
    if (!url.empty()) {
	data_formats+="<a href=\""+url+"\" target=\"_format\">";
	if (!std::regex_search(url,std::regex("^http://rda.ucar.edu"))) {
	  data_formats+="<i>";
	}
    }
    strutils::replace_all(description,"_"," ");
    data_formats+=description;
    if (!url.empty()) {
	data_formats+="</a>";
	if (!std::regex_search(url,std::regex("^http://rda.ucar.edu"))) {
	  data_formats+="</i>";
	}
    }
    if (n != static_cast<int>(formats.size()-1)) {
	data_formats+=", ";
    }
    ++n;
  }
  fdoc.close();
  tdoc.add_replacement("__DATA_FORMATS__",data_formats);
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
    std::string related_datasets;
    for (const auto& ele : elist) {
	query.set("dsid,title","search.datasets","dsid = '"+ele.attribute_value("ID")+"' and (type = 'P' or type = 'H')");
	if (query.submit(server) < 0) {
          metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",USER);
	}
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
  query.set("select distinct d.DOI_work from citation.data_citations as d left join dssdb.dsvrsn as v on v.doi = d.DOI_data where v.dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) == 0) {
    std::vector<std::tuple<std::string,std::string>> citations;
    for (const auto& row : query) {
	auto doi=row[0];
	MySQL::LocalQuery wquery("select title,pub_year,type,publisher from citation.works where DOI = '"+doi+"'");
	MySQL::Row wrow;
	if (wquery.submit(server) == 0 && wquery.fetch_row(wrow)) {
	  auto title=htmlutils::unicode_escape_to_html(wrow[0]);
	  auto pub_year=wrow[1];
	  auto type=wrow[2];
	  MySQL::LocalQuery aquery("select last_name,first_name,middle_name from citation.works_authors where ID = '"+doi+"' and ID_type = 'DOI' order by sequence");
	  if (aquery.submit(server) == 0) {
	    std::string citation;
	    size_t n=1;
	    for (const auto& arow : aquery) {
		if (citation.empty()) {
		  citation+=htmlutils::unicode_escape_to_html(arow[0]);
		  if (!arow[1].empty()) {
		    citation+=", "+arow[1].substr(0,1)+".";
		  }
		  if (!arow[2].empty()) {
		    citation+=" "+arow[2].substr(0,1)+".";
		  }
		}
		else {
		  citation+=", ";
		  if (n == aquery.num_rows()) {
		    citation+="and ";
		  }
		  if (!arow[1].empty()) {
		    citation+=arow[1].substr(0,1)+". ";
		  }
		  if (!arow[2].empty()) {
		    citation+=arow[2].substr(0,1)+". ";
		  }
		  citation+=htmlutils::unicode_escape_to_html(arow[0]);
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
		    MySQL::LocalQuery cquery("select pages,ISBN from citation.book_chapter_works where DOI = '"+doi+"'");
		    MySQL::Row crow;
		    if (cquery.submit(server) == 0 && cquery.fetch_row(crow)) {
			MySQL::LocalQuery bquery("select title,publisher from citation.book_works where ISBN = '"+crow[1]+"'");
			MySQL::Row brow;
			if (bquery.submit(server) == 0 && bquery.fetch_row(brow)) {
			  citation+=htmlutils::unicode_escape_to_html(brow[0])+". Ed. ";
			  MySQL::LocalQuery equery("select first_name,middle_name,last_name from citation.works_authors where ID = '"+crow[1]+"' and ID_type = 'ISBN' order by sequence");
			  if (equery.submit(server) == 0) {
			    size_t n=1;
			    for (const auto& erow : equery) {
				if (n > 1) {
				  citation+=", ";
				  if (n == equery.num_rows()) {
				    citation+="and ";
				  }
				}
				citation+=erow[0].substr(0,1)+". ";
				if (!erow[1].empty()) {
				  citation+=erow[1].substr(0,1)+". ";
				}
				citation+=htmlutils::unicode_escape_to_html(erow[2]);
				++n;
			    }
			    citation+=", "+htmlutils::unicode_escape_to_html(brow[1])+", "+crow[0]+".";
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
		    MySQL::LocalQuery jquery("select pub_name,volume,pages from citation.journal_works where DOI = '"+doi+"'");
		    MySQL::Row jrow;
		    if (jquery.submit(server) == 0 && jquery.fetch_row(jrow)) {
			citation+="<em>"+htmlutils::unicode_escape_to_html(jrow[0])+"</em>";
			if (!jrow[1].empty()) {
			  citation+=", <strong>"+jrow[1]+"</strong>";
			}
			if (!jrow[2].empty()) {
			  citation+=", "+jrow[2];
			}
			citation+=", <a href=\"https://doi.org/"+doi+"\" target=\"_doi\">https://doi.org/"+doi+"</a>";
		    }
		    else {
			citation="";
		    }
		    break;
		  }
		  case 'P': {
		    citation+=". <em>Proc. ";
		    MySQL::LocalQuery pquery("select pub_name,pages from citation.proceedings_works where DOI = '"+doi+"'");
		    MySQL::Row prow;
		    if (pquery.submit(server) == 0 && pquery.fetch_row(prow)) {
			citation+=htmlutils::unicode_escape_to_html(prow[0])+"</em>, "+wrow[3]+", "+prow[1]+", <a href=\"https://doi.org/"+doi+"\" target=\"_doi\">https://doi.org/"+doi+"</a>";
		    }
		    else {
			citation="";
		    }
		    break;
		  }
		}
	    }
	    if (!citation.empty()) {
		citations.emplace_back(std::make_tuple(pub_year,citation));
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
        [](const std::tuple<std::string,std::string>& left,const std::tuple<std::string,std::string>& right) -> bool
        {
	  if (std::get<0>(left) > std::get<0>(right)) {
	    return true;
	  }
	  else if (std::get<0>(left) < std::get<0>(right)) {
	    return false;
	  }
	  else {
	    return (std::get<1>(left) < std::get<1>(right));
	  }
        });
	std::unordered_set<std::string> pub_years;
	for (const auto& c : citations) {
	  auto pub_year=std::get<0>(c);
	  if (pub_years.find(pub_year) == pub_years.end()) {
	    tdoc.add_repeat("__DATA_CITER__","CITATION[!]"+std::get<1>(c)+"<!>YEAR[!]"+pub_year);
	    pub_years.emplace(pub_year);
	  }
	  else {
	    tdoc.add_repeat("__DATA_CITER__","CITATION[!]"+std::get<1>(c));
	  }
	}
    }
    else {
	tdoc.add_replacement("__NUM_DATA_CITATIONS__","<strong>0</strong> times");
    }
  }
  ofs << tdoc;
  ofs.close();
  delete t;
}

int main(int argc,char **argv)
{
  if (argc != 2 && argc != 3) {
    std::cerr << "usage: dsgen nnn.n" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "--no-dset-waf  don't add the dataset to the queue for the DSET WAF" << std::endl;
    exit(1);
  }
  auto next=1;
  if (std::string(argv[next]) == "--no-dset-waf") {
    no_dset_waf=true;
    ++next;
  }
  metautils::args.dsnum=argv[next];
  if (std::regex_search(metautils::args.dsnum,std::regex("^ds"))) {
    metautils::args.dsnum=metautils::args.dsnum.substr(2);
  }
  if (metautils::args.dsnum >= "999.0") {
    no_dset_waf=true;
  }
  metautils::args.args_string=unixutils::unix_args_string(argc,argv);
  metautils::read_config("dsgen",USER);
  if (!temp_dir.create(metautils::directives.temp_path)) {
    metautils::log_error("unable to create temporary directory","dsgen",USER);
  }
  server.connect(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::LocalQuery query("select type from search.datasets where dsid = '"+metautils::args.dsnum+"'");
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    metautils::log_error("unable to determine dataset type","dsgen",USER);
  }
  dataset_type=row[0];
  if (!no_dset_waf) {
    if (dataset_type != "P" && dataset_type != "H") {
	no_dset_waf=true;
    }
  }
  TempDir dataset_doc_dir;
  if (!dataset_doc_dir.create(metautils::directives.temp_path)) {
    metautils::log_error("unable to create temporary document directory","dsgen",USER);
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
  std::string remote_path="/data/web";
  if (dataset_type == "W") {
    remote_path+="/internal";
  }
  remote_path+="/datasets/ds"+metautils::args.dsnum;
  std::string error;
  if (unixutils::rdadata_sync(dataset_doc_dir.name(),".",remote_path,metautils::directives.rdadata_home,error) < 0) {
    metautils::log_warning("couldn't sync dataset files - rdadata_sync error(s): '"+error+"'","dsgen",USER);
  }
}
