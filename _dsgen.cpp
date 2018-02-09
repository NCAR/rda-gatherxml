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
#include <citation.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <tempfile.hpp>
#include <search.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
std::string myerror="";
std::string mywarning="";

std::string user=getenv("USER");
TempDir temp_dir;
XMLDocument xdoc;
MySQL::Server server;
std::string dataset_type;

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
    metautils::log_error("unable to open output for 'index.html'","dsgen",user,args.args_string);
  }
  ofs << "<!DOCTYPE html>" << std::endl;
  ofs << "<head>" << std::endl;
  if (dataset_type == "I") {
    ofs << "<meta http-equiv=\"Refresh\" content=\"0; url=/index.html?hash=error&code=404&url=/datasets/ds" << args.dsnum << "\" />" << std::endl;
    ofs << "</head>" << std::endl;
  }
  else {
    struct stat buf;
    std::string ds_overview;
    if (stat(("/data/web/datasets/ds"+args.dsnum+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
	ds_overview="/data/web/datasets/ds"+args.dsnum+"/metadata/dsOverview.xml";
    }
    else {
	ds_overview=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/dsOverview.xml",temp_dir.name());
	if (ds_overview.empty()) {
	  metautils::log_error("dsOverview.xml does not exist for "+args.dsnum,"dsgen",user,args.args_string);
	}
    }
    if (!xdoc.open(ds_overview)) {
	metautils::log_error("unable to open dsOverview.xml for "+args.dsnum+"; parse error: '"+xdoc.parse_error()+"'","dsgen",user,args.args_string);
    }
    ofs << "<meta http-equiv=\"Content-type\" content=\"application/xml+xhtml;charset=UTF-8\" />" << std::endl;
    ofs << "<meta name=\"fragment\" content=\"!\" />" << std::endl;
    if (!metadataExport::export_to_dc_meta_tags(ofs,args.dsnum,xdoc,0)) {
	metautils::log_error("unable to export DC meta tags","dsgen",user,args.args_string);
    }
    auto e=xdoc.element("dsOverview/title");
    auto title=e.content();
    ofs << "<title>CISL RDA: " << title << "</title>" << std::endl;
    ofs << "<?php include (\"main/styles.inc\"); ?>" << std::endl;
    ofs << "<?php include (\"main/scripts.inc\"); ?>" << std::endl;
    ofs << "<?php include (\"main/ds_scripts.inc\"); ?>" << std::endl;
    if (!metadataExport::export_to_json_ld(ofs,args.dsnum,xdoc,0)) {
	metautils::log_error("unable to export JSON-LD metadata","dsgen",user,args.args_string);
    }
    ofs << "</head>" << std::endl;
    ofs << "<body>" << std::endl;
    ofs << "<div id=\"window_container_inner\" style=\"background-color: rgba(255,255,255,0.4); width: 1000px; height: auto; padding: 0px 20px 20px 20px; border-radius: 0px 0px 20px 20px; margin: 0px auto 40px auto\">" << std::endl;
    ofs << "<?php include(\"main/banner.inc\"); ?>" << std::endl;
    ofs << "<div id=\"dstitle\" style=\"width: 980px; height: auto; padding: 10px 10px 0px 10px; background-color: #fffff5\">" << std::endl;
    ofs << "<table width=\"980\" style=\"padding-bottom: 10px\">" << std::endl;
    ofs << "<?php" << std::endl;
    ofs << "  $can_bookmark=false;" << std::endl;
    ofs << "  if (strlen($duser) > 0) {" << std::endl;
    ofs << "    $can_bookmark=true;" << std::endl;
    ofs << "    $is_bookmarked=false;" << std::endl;
    ofs << "    $conn = mysql_connect(\"rda-db.ucar.edu\",\"dssdb\",\"dssdb\");" << std::endl;
    ofs << "    if ($conn) {" << std::endl;
    ofs << "      $db=mysql_select_db(\"dssdb\",$conn);" << std::endl;
    ofs << "      if ($db) {" << std::endl;
    ofs << "        $query=\"select dsid from dsbookmarks where email = '\" . $duser . \"' and dsid = '" << args.dsnum << "'\";" << std::endl;
    ofs << "        $res=mysql_query($query,$conn);" << std::endl;
    ofs << "        if ($res && mysql_num_rows($res) > 0) {" << std::endl;
    ofs << "          $is_bookmarked=true;" << std::endl;
    ofs << "        }" << std::endl;
    ofs << "      }" << std::endl;
    ofs << "    }" << std::endl;
    ofs << "    mysql_close($conn);" << std::endl;
    ofs << "  }" << std::endl;
    ofs << "?>" << std::endl;
    ofs << "<tr valign=\"top\"><td rowspan=\"2\" width=\"1\"><img style=\"float: left; margin-right: 5px\" src=\"/images/ds_logos/";
    e=xdoc.element("dsOverview/logo");
    if (!e.content().empty()) {
	auto logo_parts=strutils::split(e.content(),".");
	auto geometry_parts=strutils::split(logo_parts[logo_parts.size()-2],"_");
	auto width=std::stoi(geometry_parts[geometry_parts.size()-2]);
	auto height=std::stoi(geometry_parts[geometry_parts.size()-1]);
	ofs << e.content() << "\" width=\"" << lroundf(width*70./height) << "\"";
    }
    else {
	ofs << "default_200_200.png\" width=\"70\"";
    }
    ofs << " height=\"70\" /></td><td><span class=\"fs24px bold\">" << title << "</span><br /><span class=\"fs16px bold\"><span class=\"blue\">ds" << args.dsnum << "</span>";
    MySQL::Query query("doi","dssdb.dsvrsn","dsid = 'ds"+args.dsnum+"' and status = 'A'");
    MySQL::Row row;
    if (query.submit(server) == 0 && query.fetch_row(row) && !row[0].empty()) {
	ofs << "&nbsp;|&nbsp;<span class=\"blue\">DOI: " << row[0] << "</span>";
    }
    ofs << "</span><div id=\"ds_bookmark\" style=\"display: inline; margin-left: 2px\"><?php if ($can_bookmark) { $dsid=\"" << args.dsnum << "\"; if ($is_bookmarked) { include(\"dsbookmarks/showunset.inc\"); } else { include(\"dsbookmarks/showset.inc\"); } } ?></div></td></tr>" << std::endl;
    e=xdoc.element("dsOverview/contact");
    auto contact_parts=strutils::split(e.content());
    query.set("select logname,phoneno from dssdb.dssgrp where fstname = '"+contact_parts[0]+"' and lstname = '"+contact_parts[1]+"'");
    if (query.submit(server) < 0) {
	metautils::log_error("mysql error while trying to get specialist information: "+query.error(),"dsgen",user,args.args_string);
    }
    if (!query.fetch_row(row)) {
	metautils::log_error("no result returned for specialist '"+e.content()+"'","dsgen",user,args.args_string);
    }
    auto phoneno=row[1];
    strutils::replace_all(phoneno,"(","");
    strutils::replace_all(phoneno,")","");
    ofs << "<tr valign=\"bottom\"><td align=\"right\"><span class=\"fs16px\">For assistance, contact <a class=\"blue\" href=\"mailto:"+row[0]+"@ucar.edu\">"+e.content()+"</a> <span class=\"mediumGrayText\">("+phoneno+").</span></span></td></tr>" << std::endl;
    ofs << "</table>" << std::endl;
    ofs << "<?php include(\"main/dstabs";
    if (dataset_type == "D") {
	ofs << "-dead";
    }
    ofs << ".inc\"); ?>" << std::endl;
    ofs << "</div>" << std::endl;
    ofs << "<div id=\"content_container\" style=\"width: 980px; height: auto; overflow: visible; padding: 10px; background-color: #fffff5\">" << std::endl;
    ofs << "<?php" << std::endl;
    ofs << "  if ($_POST[\"globus\"] == \"Y\") {" << std::endl;
    ofs << "    print \"<img src=\\\"/images/transpace.gif\\\" onLoad=\\\"getAjaxContent('POST','\" . file_get_contents(\"php://input\") . \"','/php/dsrqst.php','content_container')\\\" />\";" << std::endl;
    ofs << "  }" << std::endl;
    ofs << "  else {" << std::endl;
    ofs << "?>";
    ofs << "<noscript>" << std::endl;
    ofs << "<span class=\"bold red\">WARNING:</span> This website uses Javascript extensively, but it appears that your browser has disabled it.  Your accessibility to information and data will be severely limited unless you enable Javascript." << std::endl;
    ofs << "</noscript>" << std::endl;
    ofs << "<?php" << std::endl;
    ofs << "  }" << std::endl;
    ofs << "?>" << std::endl;
    ofs << "</div>" << std::endl;
    ofs << "<?php include(\"main/footer.inc\"); ?>" << std::endl;
    ofs << "<?php include(\"main/accessories.inc\"); ?>" << std::endl;
    ofs << "</div>" << std::endl;
    ofs << "<?php include(\"main/orgnav.inc\"); ?>" << std::endl;
    ofs << "</body>" << std::endl;
  }
  ofs << "</html>" << std::endl;
  ofs.close();
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
  std::string dsnum2=strutils::substitute(args.dsnum,".","");
  std::ofstream ofs;
  ofs.open((tdir_name+"/description.html").c_str());
  if (!ofs.is_open()) {
    metautils::log_error("unable to open output for 'description.html'","dsgen",user,args.args_string);
  }
  if (dataset_type == "D") {
    ofs << "<table class=\"fs16px\" width=\"100%\" cellspacing=\"10\" cellpadding=\"0\" border=\"0\">" << std::endl;
    insert_text_field(ofs,xdoc.element("dsOverview/summary"),"Abstract");
    ofs << "</table>" << std::endl;
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
  auto databases=metautils::cmd_databases("dsgen","x",args.dsnum);
  if (databases.size() == 0) {
    metautils::log_error("empty CMD database list","dsgen",user,args.args_string);
  }
  std::vector<std::string> formats,types;
  auto found_content_metadata=false;
  for (const auto& db : databases) {
    if (db[0] != 'V' && table_exists(server,db+".ds"+dsnum2+"_primaries")) {
	MySQL::LocalQuery query("select distinct format from "+db+".formats as f left join "+db+".ds"+dsnum2+"_primaries as d on d.format_code = f.code where !isnull(d.format_code)");
	if (query.submit(server) < 0) {
	  metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
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
  ofs << "<?php" << std::endl;
  ofs << "  $rda_blog=false;" << std::endl;
  ofs << "  $cdg_page_path=\"\";" << std::endl;
  ofs << "  $cdg_guidance_path=\"\";" << std::endl;
  ofs << "  $conn=mysql_connect(\"rda-db.ucar.edu\",\"metadata\",\"metadata\");" << std::endl;
  ofs << "  if ($conn) {" << std::endl;
  ofs << "    $res=mysql_query(\"select dsid from metautil.rda_blog where dsid = '" << args.dsnum << "'\");" << std::endl;
  ofs << "    if (mysql_numrows($res) > 0) {" << std::endl;
  ofs << "      $rda_blog=true;" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "    $res=mysql_query(\"select cdg_page_path,cdg_guidance_path from metautil.climate_data_guide where dsid = '" << args.dsnum << "'\");" << std::endl;
  ofs << "    if (mysql_numrows($res) > 0) {" << std::endl;
  ofs << "      @ $row=mysql_fetch_array($res,MYSQL_ASSOC);" << std::endl;
  ofs << "      $cdg_page_path=$row['cdg_page_path'];" << std::endl;
  ofs << "      $cdg_guidance_path=$row['cdg_guidance_path'];" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
  ofs << "<style id=\"border_style\">" << std::endl;
  ofs << ".border-top {" << std::endl;
  ofs << "  border-top: 1px solid #fffff5;" << std::endl;
  ofs << "}" << std::endl;
  ofs << ".border-right {" << std::endl;
  ofs << "  border-right: 1px solid #fffff5;" << std::endl;
  ofs << "}" << std::endl;
  ofs << ".border-bottom {" << std::endl;
  ofs << "  border-bottom: 1px solid #fffff5;" << std::endl;
  ofs << "}" << std::endl;
  ofs << ".border-left {" << std::endl;
  ofs << "  border-left: 1px solid #fffff5;" << std::endl;
  ofs << "}" << std::endl;
  ofs << "</style>" << std::endl;
  ofs << "<table class=\"fs16px\" width=\"100%\" cellspacing=\"10\" cellpadding=\"0\" border=\"0\">" << std::endl;
// YouTube tutorial
  ofs << "<tr style=\"vertical-align: bottom\"><td class=\"bold nowrap\">Help with this page:</td><td><a href=\"http://ncarrda.blogspot.com/search/label/Tutorial+YouTube+Description\" target=\"_tutorial\">RDA dataset description page video tour</a></td></tr>" << std::endl;
// dataset summary (abstract)
  insert_text_field(ofs,xdoc.element("dsOverview/summary"),"Abstract");
// acknowledgments
  insert_text_field(ofs,xdoc.element("dsOverview/acknowledgement"),"Acknowledgments");
// temporal range(s)
  MySQL::LocalQuery query("select dsid from dssdb.dsgroup where dsid = 'ds"+args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
  }
  if (query.num_rows() > 0) {
    query.set("select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) where p.dsid = 'ds"+args.dsnum+"' and g.pindex = 0 and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' union select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g2.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) left join dssdb.dsgroup as g2 on (p.dsid = g2.dsid and g.pindex = g2.gindex) where p.dsid = 'ds"+args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' and !isnull(g2.title) order by title");
  }
  else {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
  }
  if (query.num_rows() == 0) {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
    if (query.submit(server) < 0) {
	metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
    }
  }
  auto div_num=0;
  bool grouped_periods=false;
  std::unordered_map<std::string,std::string> groups_table,rda_files_table,metadata_files_table;
  if (query.num_rows() > 0) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Temporal Range:</td><td>";
    if (query.num_rows() > 1) {
	MySQL::LocalQuery query2("distinct gindex","dssdb.dsperiod","dsid = 'ds"+args.dsnum+"'");
	if (query2.submit(server) < 0) {
	  metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.args_string);
	}
	if (query2.num_rows() > 1) {
	  grouped_periods=true;
	  query2.set("select gindex,title from dssdb.dsgroup where dsid = 'ds"+args.dsnum+"'");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("error: "+query2.error()+" while getting groups data","dsgen",user,args.args_string);
	  }
	  MySQL::Row row2;
	  while (query2.fetch_row(row2)) {
	    groups_table.emplace(row2[0],row2[1]);
	  }
	  query2.set("select mssfile,tindex from dssdb.mssfile where dsid = 'ds"+args.dsnum+"' and type = 'P' and status = 'P'");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("error: "+query2.error()+" while getting RDA files data","dsgen",user,args.args_string);
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
	  query2.set("select min(concat(date_start,' ',time_start)),min(start_flag),max(concat(date_end,' ',time_end)),min(end_flag),any_value(time_zone) from dssdb.dsperiod where dsid = 'ds"+args.dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' group by dsid");
	  if (query2.submit(server) < 0) {
	    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.args_string);
	  }
	  query2.fetch_row(row2);
	  auto start_date_time=summarizeMetadata::set_date_time_string(row2[0],row2[1],row2[4]);
	  auto end_date_time=summarizeMetadata::set_date_time_string(row2[2],row2[3],row2[4]);
	  auto temporal=start_date_time;
	  if (!end_date_time.empty() && end_date_time != start_date_time) {
	    temporal+=" to "+end_date_time;
	  }
	  ofs << temporal << " <span class=\"fs13px\">(Entire dataset)</span>";
	  ofs << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand dataset product period list\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Period details by dataset product</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse dataset product period list\"></a><span class=\"fs13px\">Period details by dataset product:";
	  div_num+=2;
	}
    }
    std::map<std::string,std::tuple<std::string,std::string>> periods_table;
    MySQL::Row row;
    while (query.fetch_row(row)) {
	auto start_date_time=summarizeMetadata::set_date_time_string(row[0]+" "+row[1],row[2],row[6]);
	auto end_date_time=summarizeMetadata::set_date_time_string(row[3]+" "+row[4],row[5],row[6]);
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
	  ofs << "<div style=\"margin-left: 10px\">" << temporal << "</div>";
	}
	else {
	  ofs << temporal;
	}
    }
    if (grouped_periods) {
	ofs << "</span></span>";
    }
    ofs << "</td></tr>" << std::endl;
  }
// update frequency
  auto e=xdoc.element("dsOverview/continuingUpdate");
  if (e.attribute_value("value") == "yes") {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Updates:</td><td>" << strutils::capitalize(e.attribute_value("frequency")) << "</td></tr>" << std::endl;
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
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Access Restrictions:</td><td>" << access << "</td></tr>" << std::endl;
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
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Usage Restrictions:</td><td>" << usage << "</td></tr>" << std::endl;
  }
// variables
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Variables:</td><td>" << std::endl;
  query.set("select substring_index(path,' > ',-1) as var from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD' and v.dsid = '"+args.dsnum+"' order by var");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
  }
  std::list<std::string> strings;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    strings.emplace_back(strutils::capitalize(row[0]));
  }
  insert_table(ofs,strings,4,"#e1eaff","#c8daff");
  auto elist=xdoc.element_list("dsOverview/contentMetadata/detailedVariables/detailedVariable");
  if (elist.size() > 0) {
    for (const auto& ele : elist) {
	if (std::regex_search(ele.content(),std::regex("^http://")) || std::regex_search(ele.content(),std::regex("^https://"))) {
	  ofs << "<a href=\"" << ele.content() << "\">Detailed Variable List</a><br />" << std::endl;
	  break;
	}
    }
  }
  if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/grib.html")) {
    ofs << "<div>GRIB parameter table:  <a href=\"/datasets/ds" << args.dsnum << "/#metadata/grib.html?_do=y\">HTML</a>";
    if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/grib.xml")) {
	ofs << " | <a href=\"/datasets/ds" << args.dsnum << "/metadata/grib.xml\">XML</a></div>";
    }
    ofs << std::endl;
  }
  if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/grib2.html")) {
    ofs << "<div>GRIB2 parameter table:  <a href=\"/datasets/ds" << args.dsnum << "/#metadata/grib2.html?_do=y\">HTML</a>";
    if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/grib2.xml")) {
	ofs << " | <a href=\"/datasets/ds" << args.dsnum << "/metadata/grib2.xml\">XML</a></div>"; 
    }
    ofs << std::endl;
  }
  if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/on84.html")) {
    ofs << "<div>ON84 parameter table:  <a href=\"/datasets/ds" << args.dsnum << "/metadata/on84.html\">HTML</a>";
    if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/on84.html")) {
	ofs << " | <a href=\"/datasets/ds" << args.dsnum << "/metadata/on84.xml\">XML</a></div>";
    }
    ofs << std::endl;
  }
  query.set("gindex,title","dssdb.dsgroup","dsid = 'ds"+args.dsnum+"' and pindex = 0 and pmsscnt > 0");
  if (query.submit(server) == 0) {
    std::stringstream vars_by_product;
    while (query.fetch_row(row)) {
	if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/customize.GrML."+row[0])) {
	  auto c_file=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/customize.GrML."+row[0],temp_dir.name());
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
		if (vars_by_product.tellp() == 0) {
		  ofs << "<div style=\"position: relative; overflow: hidden\"><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand dataset product variable list\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Variables by dataset product</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse dataset product variable list\"></a><span class=\"fs13px\">Variables by dataset product:";
		  div_num+=2;
		}
		vars_by_product << "<div style=\"margin-left: 10px\"><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Variable list for " << row[1] << "</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse\"></a><span class=\"fs13px\">Variable list for " << row[1] << ":<div style=\"margin-left: 20px\">";
		div_num+=2;
		for (const auto& var : varlist) {
		  vars_by_product << var << "<br />";
		}
		vars_by_product << "</div></span></span></div>";
	    }
	  }
	}
    }
    if (vars_by_product.tellp() > 0) {
	ofs << vars_by_product.str() << "</span></span></div>";
    }
  }
  ofs << "</td></tr>" << std::endl;
// vertical levels
  if (found_content_metadata) {
    for (const auto& type : types) {
	if (type == "grid") {
	  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Vertical Levels:</td><td>See the <a href=\"#metadata/detailed.html?_do=y&view=level\">detailed metadata</a> for level information";
	  if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/grib2_levels.html")) {
	    ofs << "<br /><a href=\"/datasets/ds" << args.dsnum << "/#metadata/grib2_levels.html?_do=y\">GRIB2 level table</a>"; 
	  }
	  ofs << "</td></tr>" << std::endl;
	  break;
	}
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/levels/level");
    auto elist2=xdoc.element_list("dsOverview/contentMetadata/levels/layer");
    elist.insert(elist.end(),elist2.begin(),elist2.end());
    if (elist.size() > 0) {
	ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Vertical Levels:</td><td>";
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
	insert_table(ofs,levels,4,"#c8daff","#e1eaff");
	ofs << "</td></tr>" << std::endl;
    }
  }
// temporal frequency
  if (!found_content_metadata) {
    elist=xdoc.element_list("dsOverview/contentMetadata/temporalFrequency");
    auto m=0;
    if (elist.size() > 0) {
	ofs << "<tr style=\"vertical-align: bottom\"><td class=\"bold\">Temporal Frequencies:</td><td>";
	for (const auto& ele : elist) {
	  auto tfreq_type=ele.attribute_value("type");
	  if (m > 0) {
	    ofs << ", ";
	  }
	  if (tfreq_type == "regular") {
	    auto n=std::stoi(ele.attribute_value("number"));
	    ofs << "Every ";
	    if (n > 1) {
		ofs << n << " ";
	    }
	    ofs << ele.attribute_value("unit");
	    if (n > 1) {
		ofs << "s";
	    }
	    auto stats=ele.attribute_value("statistics");
	    if (!stats.empty()) {
		ofs << " (" << strutils::capitalize(stats) << ")";
	    }
	    ofs << std::endl;
	  }
	  else if (tfreq_type == "irregular") {
	    ofs << "various times per " << ele.attribute_value("unit");
	    auto stats=ele.attribute_value("statistics");
	    if (!stats.empty()) {
		ofs << " (" << strutils::capitalize(stats) << ")";
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
	    ofs << unit << " Climatology";
	  }
	  ++m;
	}
	ofs << "</td></tr>" << std::endl;
    }
  }
// data types
  if (found_content_metadata) {
    if (types.size() > 0) {
	ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Types:</td><td>";
	auto n=0;
	for (const auto& type : types) {
	  if (n > 0) {
	    ofs << ", ";
	  }
	  ofs << strutils::to_capital(type);
	  ++n;
	}
	ofs << "</td></tr>" << std::endl;
    }
  }
  else {
    elist=xdoc.element_list("dsOverview/contentMetadata/dataType");
    if (elist.size() > 0) {
	ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Types:</td><td>";
	auto n=0;
	std::unordered_map<std::string,char> unique_data_types;
	for (const auto& ele : elist) {
	  if (unique_data_types.find(ele.content()) == unique_data_types.end()) {
	    if (n++ > 0) {
		ofs << ", ";
	    }
	    ofs << strutils::to_capital(ele.content());
	    unique_data_types[ele.content()]='Y';
	  }
	}
	ofs << "</td></tr>" << std::endl;
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
	    metautils::log_error("error: "+query.error()+" while getting grid definitions","dsgen",user,args.args_string);
	  }
	  while (query.fetch_row(row)) {
	    std::vector<size_t> values;
	    bitmap::uncompress_values(row[0],values);
	    for (const auto& value : values) {
		std::string ugd_key;
		if (grid_definition_table.find(value) == grid_definition_table.end()) {
		  MySQL::LocalQuery query2("definition,defParams","GrML.gridDefinitions","code = "+strutils::itos(value));
		  if (query2.submit(server) < 0) {
		    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.args_string);
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
	auto ugd_key=ele.attribute_value("definition")+"<!>"+ele.attribute_value("numX")+":"+ele.attribute_value("numY");
	if (std::regex_search(ugd_key,std::regex("^latLon"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(ugd_key,std::regex("^gaussLatLon"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("numY");
	}
	else if (std::regex_search(ugd_key,std::regex("^polarStereographic"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":60"+ele.attribute_value("pole")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(ugd_key,std::regex("^mercator"))) {
	  ugd_key+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes")+":"+ele.attribute_value("resLat");
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
    if (fill_spatial_domain_from_grid_definition(e.first,"primeMeridian",west_lon,south_lat,east_lon,north_lat)) {
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
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Spatial Coverage:</td><td>";
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
    ofs << "Longitude Range:  Westernmost=" << west << "  Easternmost=";
    auto east=strutils::ftos(fabs(max_east_lon),3);
    if (max_east_lon < 0) {
	east+="W";
    }
    else {
	east+="E";
    }
    ofs << east << "<br />" << std::endl;
    auto south=strutils::ftos(fabs(min_south_lat),3);
    if (min_south_lat < 0) {
	south+="S";
    }
    else {
        south+="N";
    }
    ofs << "Latitude Range:  Southernmost=" << south << "  Northernmost=";
    auto north=strutils::ftos(fabs(max_north_lat),3);
    if (max_north_lat < 0) {
	north+="S";
    }
    else {
	north+="N";
    }
    ofs << north << std::endl;
    ofs << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand coverage details\"><img src=\"/images/triangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Detailed coverage information</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/triangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse coverage details\"></a><span class=\"fs13px\">Detailed coverage information:" << std::endl;
    div_num+=2;
    std::map<std::string,std::shared_ptr<std::unordered_set<std::string>>> grid_definitions;
    for (const auto& e : unique_grid_definitions_table) {
	grid_definitions.emplace(convert_grid_definition(e.first),e.second);
    }
    for (const auto& e : grid_definitions) {
	ofs << "<div style=\"margin-left: 10px\">" << e.first;
	if (grid_definitions.size() > 1) {
	  if (e.second != nullptr) {
	    ofs << "<div style=\"margin-left: 15px; color: #6a6a6a\">(";
	    auto n=0;
	    for (const auto& group : *e.second) {
		if (n++ > 0) {
		  ofs << ", ";
		}
		ofs << group;
	    }
	    ofs << ")</div>";
	  }
	}
	ofs << "</div>";
    }
    ofs << "</span></span></td></tr>" << std::endl;
  }
// data contributors
  query.set("select g.path from search.contributors_new as c left join search.GCMD_providers as g on g.uuid = c.keyword where c.dsid = '"+args.dsnum+"' and c.vocabulary = 'GCMD'");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
  }
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Contributors:</td><td>";
  auto n=0;
  while (query.fetch_row(row)) {
    if (n > 0) {
	ofs << " | ";
    }
    auto short_name=row[0];
    std::string long_name="";
    if ( (idx=short_name.find(">")) != std::string::npos) {
	long_name=short_name.substr(idx+1);
	short_name=short_name.substr(0,idx);
    }
    strutils::trim(short_name);
    strutils::trim(long_name);
    ofs << "<span class=\"infosrc\" onMouseOver=\"javascript:popInfo(this,'src" << n << "','#e1eaff','left','bottom')\" onMouseOut=\"javascript:hideInfo('src" << n << "')\">" << short_name << "</span><div class=\"info\" id=\"src" << n << "\" class=\"source\"><small>" << long_name << "</small></div>";
    ++n;
  }
  ofs << "</td></tr>" << std::endl;
// NCAR Climate Data Guide
  ofs << "<?php" << std::endl;
  ofs << "  if (strlen($cdg_page_path) > 0) {" << std::endl;
  ofs << "    print \"<tr style=\\\"vertical-align: bottom\\\"><td class=\\\"bold\\\">NCAR Climate Data Guide:</td><td><a target=\\\"_cdg\\\" href=\\\"$cdg_page_path\\\">Dataset Assessment</a> | \";" << std::endl;
  ofs << "    if (strlen($cdg_guidance_path) > 0) {" << std::endl;
  ofs << "      print \"<a target=\\\"_cdg\\\" href=\\\"$cdg_guidance_path\\\">Expert Guidance</a>\";" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "    else {" << std::endl;
  ofs << "      print \"<span style=\\\"color: #6a6a6a\\\">Expert Guidance</span>\";" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "    print \"</td></tr>\n\";" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
// related web sites
  elist=xdoc.element_list("dsOverview/relatedResource");
  if (elist.size() > 0) {
    ofs << "<tr style=\"vertical-align: ";
    if (elist.size() > 1) {
	ofs << "top";
    }
    else {
	ofs << "bottom";
    }
    ofs << "\"><td class=\"bold\">Related Resources:</td><td>";
    for (const auto& ele : elist) {
	auto description=ele.content();
	strutils::trim(description);
	if (strutils::has_ending(description,".") && description[description.length()-2] != '.') {
	  strutils::chop(description);
	}
	ofs << "<a href=\"" << ele.attribute_value("url") << "\">";
	auto is_local=false;
	auto url=ele.attribute_value("url");
	if (std::regex_search(url,std::regex("^http://rda.ucar.edu")) || std::regex_search(url,std::regex("^https://rda.ucar.edu")) || std::regex_search(url,std::regex("^http://dss.ucar.edu")) || std::regex_search(url,std::regex("^https://dss.ucar.edu"))) {
	  is_local=true;
	}
	if (!is_local) {
	  ofs << "<span class=\"italic\">";
	}
	ofs << description;
	if (!is_local) {
	  ofs << "</span>";
	}
	ofs << "</a><br />";
    }
    ofs << "</td></tr>" << std::endl;
  }
// WRF Preprocessing System
  ofs << "<?php" << std::endl;
  ofs << "  if (file_exists(\"/usr/local/www/server_root/web/datasets/ds" << args.dsnum << "/metadata/Vtable.RDA_ds" << args.dsnum << "\")) {" << std::endl;
  ofs << "    print \"<tr style=\\\"vertical-align: bottom\\\"><td class=\\\"bold\\\">WRF Preprocessing System (WPS):</td><td>The GRIB-formatted data in this dataset can be used to initialize the Weather Research and Forecasting (WRF) Model.<br /><!--<a target=\\\"_vtable\\\" href=\\\"/datasets/ds" << args.dsnum << "/metadata/Vtable.RDA_ds" << args.dsnum << "\\\">Vtable</a>&nbsp;|&nbsp;--><a target=\\\"_wrf\\\" href=\\\"http://www2.mmm.ucar.edu/wrf/users/download/free_data.html\\\"><em>WRF Vtables</em></a></td></tr>\";" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
// publications
  elist=xdoc.element_list("dsOverview/reference");
  auto started_pubs=false;
  if (elist.size() > 0) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Publications:</td><td>";
    started_pubs=true;
    elist.sort(compare_references);
    for (const auto& ele : elist) {
	ofs << "<div>" << ele.element("authorList").content() << ", " << ele.element("year").content() << ": ";
	auto pub_type=ele.attribute_value("type");
	if (pub_type == "journal") {
	  e=ele.element("periodical");
	  auto url=ele.element("url").content();
	  auto title=ele.element("title").content();
	  if (!url.empty()) {
	    ofs << "<a href=\"" << url << "\">" << title << "</a>";
	  }
	  else {
	    ofs << title;
	  }
	  if (!strutils::has_ending(title,"?")) {
	    ofs << ".";
	  }
	  ofs << "  <i>" << e.content() << "</i>, ";
	  if (e.attribute_value("pages") == "0-0") {
	    if (e.attribute_value("number") == "0") {
		ofs << "Submitted";
	    }
	    else if (e.attribute_value("number") == "1") {
		ofs << "Accepted";
	    }
	    else if (e.attribute_value("number") == "2") {
		ofs << "In Press";
	    }
	  }
	  else {
	    ofs << "<b>" << e.attribute_value("number") << "</b>, ";
	    auto pages=e.attribute_value("pages");
	    if (std::regex_search(pages,std::regex("^AGU:"))) {
		ofs << pages.substr(4);
	    }
	    else {
		auto page_parts=strutils::split(pages,"-"); 
		if (page_parts.size() == 2 && page_parts[0] == page_parts[1]) {
		  ofs << page_parts[0];
		}
		else {
		  ofs << e.attribute_value("pages");
		}
	    }
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    ofs << " (DOI: " << doi << ")";
	  }
	  ofs << ".";
	}
	else if (pub_type == "preprint") {
	  e=ele.element("conference");
	  auto url=ele.element("url").content();
	  if (!url.empty()) {
	    ofs << "<a href=\"" << url << "\">" << ele.element("title").content() << "</a>";
	  }
	  else {
	    ofs << ele.element("title").content();
	  }
	  ofs << ".  <i>Proceedings of the " << e.content() << "</i>, " << e.attribute_value("host") << ", " << e.attribute_value("location");
	  auto pages=e.attribute_value("pages");
	  if (!pages.empty()) {
	    ofs << ", " << pages;
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    ofs << " (DOI: " << doi << ")";
	  }
	  ofs << ".";
	}
	else if (pub_type == "technical_report") {
	  e=ele.element("organization");
	  auto url=ele.element("url").content();
	  if (!url.empty()) {
	    ofs << "<i><a href=\"" << url << "\">" << ele.element("title").content() << "</a>.</i>";
	  }
	  else {
	    ofs << "<i>" << ele.element("title").content() << ".</i>";
	  }
	  ofs << "  ";
	  auto report_ID=e.attribute_value("reportID");
	  if (!report_ID.empty()) {
	    ofs << report_ID << ", ";
	  }
	  ofs << e.content();
	  auto pages=e.attribute_value("pages");
	  if (pages != "-99") {
	    ofs << ", " << e.attribute_value("pages") << " pp.";
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    ofs << " (DOI: " << doi << ").";
	  }
	}
	else if (pub_type == "book") {
	  e=ele.element("publisher");
	  ofs << "<i>" << ele.element("title").content() << "</i>. " << e.content() << ", " << e.attribute_value("place");
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    ofs << " (DOI: " << doi << ")";
	  }
	  ofs << ".";
	}
	else if (pub_type == "book_chapter") {
	  e=ele.element("book");
	  ofs << "\"" << ele.element("title").content() << "\", in " << e.content() << ". Ed. " << e.attribute_value("editor") << ", " << e.attribute_value("publisher") << ", ";
	  if (e.attribute_value("pages") == "0-0") {
	    ofs << "In Press";
	  }
	  else {
	    ofs << e.attribute_value("pages");
	  }
	  auto doi=ele.element("doi").content();
	  if (!doi.empty()) {
	    ofs << " (DOI: " << doi << ")";
	  }
	  ofs << ".";
	}
	ofs << "</div>" << std::endl;
	auto annotation=ele.element("annotation").content();
	if (!annotation.empty() > 0) {
	  ofs << "<div style=\"margin-left: 15px; color: #5f5f5f\">" << annotation << "</div>" << std::endl;
	}
	ofs << "<br />" << std::endl;
    }
  }
  elist=xdoc.element_list("dsOverview/referenceURL");
  if (elist.size() > 0) {
    if (!started_pubs) {
	ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Publications:</td><td>";
	started_pubs=true;
    }
    for (const auto& ele : elist) {
	ofs << "<a href=\"" << ele.attribute_value("url") << "\">" << ele.content() << "</a><br>" << std::endl;
    }
  }
  if (started_pubs)
    ofs << "</td></tr>" << std::endl;
// citation
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">How to Cite This Dataset:<div style=\"background-color: #2a70ae; color: white; width: 40px; padding: 1px; margin-top: 3px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << args.dsnum << "&style=ris'\" title=\"download citation in RIS format\">RIS</div><div style=\"background-color: #2a70ae; color: white; width: 60px; padding: 2px 8px 2px 8px; font-size: 16px; font-weight: bold; font-family: serif; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer; margin-top: 5px\" onClick=\"location='/cgi-bin/datasets/citation?dsnum=" << args.dsnum << "&style=bibtex'\" title=\"download citation in BibTeX format\">BibTeX</div></td><td><div id=\"citation\" style=\"border: thin solid black; padding: 5px\"><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onLoad=\"getAjaxContent('GET',null,'/cgi-bin/datasets/citation?dsnum=" << args.dsnum << "&style=esip','citation')\" /></div>" << std::endl;
  ofs << "<?php" << std::endl;
  ofs << "  $signedin=false;" << std::endl;
  ofs << "  if (isSet($_SERVER[\"HTTP_COOKIE\"])) {" << std::endl;
  ofs << "    $http_cookie=$_SERVER[\"HTTP_COOKIE\"];" << std::endl;
  ofs << "    $parts=strtok($http_cookie,\";\");" << std::endl;
  ofs << "    while ($parts) {" << std::endl;
  ofs << "      $parts=trim($parts);" << std::endl;
  ofs << "      if (strcmp(substr($parts,0,6),\"duser=\") == 0)" << std::endl;
  ofs << "        $signedin=true;" << std::endl;
  ofs << "      $parts=strtok(\";\");" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "  if ($signedin)" << std::endl;
  ofs << "    print \"<a href=\\\"javascript:void(0)\\\" onClick=\\\"getAjaxContent('GET',null,'/php/ajax/mydatacitation.php?tab=ds_history&dsid=ds" << args.dsnum << "&b=no','content_container')\\\">\";" << std::endl;
  ofs << "  else" << std::endl;
  ofs << "    print \"<span style=\\\"color: #a0a0a0\\\">\";" << std::endl;
  ofs << "  print \"Get a customized data citation\";" << std::endl;
  ofs << "  if ($signedin)" << std::endl;
  ofs << "    print \"</a>\";" << std::endl;
  ofs << "  else" << std::endl;
  ofs << "    print \" <span class=\\\"fs13px\\\">(must be signed in)</span></span>\";" << std::endl;
  ofs << "?>" << std::endl;
  ofs << "</td></tr>" << std::endl;
// volume
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Total Volume:</td><td>";
  query.set("primary_size","dssdb.dataset","dsid = 'ds"+args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
  }
  MySQL::LocalQuery query2("select primary_size,title,grpid from dssdb.dsgroup where dsid = 'ds"+args.dsnum+"' and pindex = 0 and primary_size > 0");
  if (query2.submit(server) < 0) {
    metautils::log_error("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.args_string);
  }
  const int VOLUME_LEN=4;
  const char *v[VOLUME_LEN]={"MB","GB","TB","PB"};
  if (query.fetch_row(row) && !row[0].empty()) {
    auto volume=std::stof(row[0])/1000000.;
    n=0;
    while (volume > 1000. && n < VOLUME_LEN) {
	volume/=1000.;
	++n;
    }
    ofs << strutils::ftos(llround(volume*100.)/100.,6,2,' ') << " " << v[n];
  }
  if (query2.num_rows() > 1) {
    ofs << " <span class=\"fs13px\">(Entire dataset)</span><br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\" title=\"Expand dataset product volume list\"><font size=\"-1\">Volume details by dataset product</font></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\" title=\"Collapse dataset product volume list\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\"></a><span class=\"fs13px\">Volume details by dataset product:";
    div_num+=2;
    MySQL::Row row2;
    while (query2.fetch_row(row2)) {
	auto volume=std::stof(row2[0])/1000000.;
	n=0;
	while (volume > 1000. && n < VOLUME_LEN) {
	  volume/=1000.;
	  ++n;
	}
	ofs << "<div style=\"margin-left: 10px\">";
	if (!row2[1].empty()) {
	  ofs << row2[1];
	}
	else {
	  ofs << row2[2];
	}
	ofs << ": " << strutils::ftos(llround(volume*100.)/100.,6,2,' ') << " " << v[n] << "</div>";
    }
    ofs << "</span></span>";
  }
  ofs << "</td></tr>" << std::endl;
// data formats
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Formats:</td><td>";
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
    auto file=remote_web_file("https://rda.ucar.edu/metadata/FormatReferences.xml",temp_dir.name());
    fdoc.open(file);
  }
  if (!fdoc) {
    metautils::log_error("unable to open FormatReferences.xml","dsgen",user,args.args_string);
  }
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
	ofs << "<a href=\"" << url << "\" target=\"_format\">";
	if (!std::regex_search(url,std::regex("^http://rda.ucar.edu"))) {
	  ofs << "<i>";
	}
    }
    strutils::replace_all(description,"_"," ");
    ofs << description;
    if (!url.empty()) {
	ofs << "</a>";
	if (!std::regex_search(url,std::regex("^http://rda.ucar.edu"))) {
	  ofs << "</i>";
	}
    }
    if (n != static_cast<int>(formats.size()-1)) {
	ofs << ", ";
    }
    ++n;
  }
  fdoc.close();
  ofs << "</td></tr>" << std::endl;
// related datasets
  elist=xdoc.element_list("dsOverview/relatedDataset");
  if (elist.size() > 0) {
    ofs << "<tr style=\"vertical-align: ";
    if (elist.size() > 1)
	ofs << "top";
    else
	ofs << "bottom";
    ofs << "\"><td class=\"bold\">Related RDA Datasets:</td><td><table cellspacing=\"0\" cellpadding=\"2\" border=\"0\">";
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
    for (const auto& ele : elist) {
	query.set("dsid,title","search.datasets","dsid = '"+ele.attribute_value("ID")+"' and (type = 'P' or type = 'H')");
	if (query.submit(server) < 0)
          metautils::log_error("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.args_string);
	if (query.fetch_row(row)) {
	  ofs << "<tr valign=\"top\"><td><a href=\"/datasets/ds" << row[0] << "#description\">" << row[0] << "</a></td><td>-</td><td>" << row[1] << "</td></tr>";
	}
    }
    ofs << "</table></td></tr>" << std::endl;
  }
// more details
  if (exists_on_server(directives.web_server,"/data/web/datasets/ds"+args.dsnum+"/metadata/detailed.html")) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">More Details:</td><td>View <a href=\"#metadata/detailed.html?_do=y\">more details</a> for this dataset, including dataset citation, data contributors, and other detailed metadata</td></tr>" << std::endl;
  }
// RDA blog
  ofs << "<?php" << std::endl;
  ofs << "  if ($rda_blog) {" << std::endl;
  ofs << "    print \"<tr style=\\\"vertical-align: top\\\"><td class=\\\"bold nowrap\\\">RDA Blog:</td><td>Read <a href=\\\"http://ncarrda.blogspot.com/search/label/ds" << args.dsnum << "\\\" target=\\\"_rdablog\\\">posts</a> on the RDA blog that are related to this dataset</td></tr>\n\";" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
// data access
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Access:</td><td><div style=\"display: inline; float: left\">Click the </div><a class=\"clean\" href=\"#access\"><div class=\"dstab dstab-off\" style=\"width: 100px; margin: 0px 5px 0px 5px; text-align: center\">Data Access</div></a><div style=\"display: inline; float: left\"> tab here or in the navigation bar near the top of the page</div></td></tr>" << std::endl;
// metadata record
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Metadata Record:</td><td><div id=\"meta_record\" style=\"display: inline; float: left\"></div><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onload=\"getAjaxContent('GET',null,'/cgi-bin/datasets/showMetadata?dsnum=" << args.dsnum << "','meta_record')\" /></td></tr>" << std::endl;
  ofs << "</table>" << std::endl;
  ofs.close();
}

int main(int argc,char **argv)
{
  if (argc != 2) {
    std::cerr << "usage: dsgen nnn.n" << std::endl;
    exit(1);
  }
  args.dsnum=argv[1];
  if (std::regex_search(args.dsnum,std::regex("^ds"))) {
    args.dsnum=args.dsnum.substr(2);
  }
  args.args_string=unix_args_string(argc,argv);
  metautils::read_config("dsgen",user,args.args_string);
  if (!temp_dir.create(directives.temp_path)) {
    metautils::log_error("unable to create temporary directory","dsgen",user,args.args_string);
  }
  metautils::connect_to_metadata_server(server);
  MySQL::LocalQuery query("select type from search.datasets where dsid = '"+args.dsnum+"'");
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    metautils::log_error("unable to determine dataset type","dsgen",user,args.args_string);
  }
  dataset_type=row[0];
  TempDir dataset_doc_dir;
  if (!dataset_doc_dir.create(directives.temp_path)) {
    metautils::log_error("unable to create temporary document directory","dsgen",user,args.args_string);
  }
  generate_index(dataset_type,dataset_doc_dir.name());
  if (dataset_type != "I") {
    generate_description(dataset_type,dataset_doc_dir.name());
    xdoc.close();
  }
  server.disconnect();
  std::string remote_path="/data/web";
  if (dataset_type == "W") {
    remote_path+="/internal";
  }
  remote_path+="/datasets/ds"+args.dsnum;
  std::string error;
  if (host_sync(dataset_doc_dir.name(),".",remote_path,error) < 0) {
    metautils::log_warning("couldn't sync dataset files - host_sync error(s): '"+error+"'","dsgen",user,args.args_string);
  }
}
