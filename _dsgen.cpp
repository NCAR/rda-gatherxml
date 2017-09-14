#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <deque>
#include <regex>
#include <unordered_map>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <citation.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <tempfile.hpp>
#include <mymap.hpp>
#include <search.hpp>

metautils::Directives directives;
metautils::Args args;

struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};
struct CoverageEntry {
  CoverageEntry() : key(),groups(nullptr) {}

  std::string key;
  std::shared_ptr<my::map<StringEntry>> groups;
};
struct PeriodEntry {
  PeriodEntry() : key(),start(),end() {}

  std::string key;
  std::string start,end;
};
struct GridDefinitionEntry {
  GridDefinitionEntry() : key(),definition(),defParams() {}

  size_t key;
  std::string definition,defParams;
};
struct XEntry {
  XEntry() : key(),a(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> a;
};
std::string user=getenv("USER");
std::string dsnum,server_root;
TempDir temp_dir;
XMLDocument xdoc;
MySQL::Server server;
int div_num=0;

void generateIndex(std::string type)
{
  std::ofstream ofs;
  XMLElement e;
  std::deque<std::string> sp,sp2;
  int width,height;
  MySQL::Query query;
  MySQL::Row row;
  std::string title,sdum;

  ofs.open((temp_dir.name()+"/index.html").c_str());
  if (!ofs.is_open()) {
    metautils::logError("unable to open output for 'index.html'","dsgen",user,args.argsString);
  }
  ofs << "<!DOCTYPE html>" << std::endl;
  ofs << "<head>" << std::endl;
  if (type == "internal") {
    ofs << "<meta http-equiv=\"Refresh\" content=\"0; url=/index.html?hash=error&code=404&url=/datasets/ds" << dsnum << "\" />" << std::endl;
    ofs << "</head>" << std::endl;
  }
  else {
    ofs << "<meta http-equiv=\"Content-type\" content=\"application/xml+xhtml;charset=UTF-8\" />" << std::endl;
    ofs << "<meta name=\"fragment\" content=\"!\">" << std::endl;
    e=xdoc.element("dsOverview/title");
    title=e.content();
    ofs << "<title>CISL RDA: " << title << "</title>" << std::endl;
    ofs << "<?php include (\"main/styles.inc\"); ?>" << std::endl;
    ofs << "<?php include (\"main/scripts.inc\"); ?>" << std::endl;
    ofs << "<?php include (\"main/ds_scripts.inc\"); ?>" << std::endl;
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
    ofs << "        $query=\"select dsid from dsbookmarks where email = '\" . $duser . \"' and dsid = '" << dsnum << "'\";" << std::endl;
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
    if (e.content().length() > 0) {
	sp=strutils::split(e.content(),".");
	sp2=strutils::split(sp[sp.size()-2],"_");
	width=std::stoi(sp2[sp2.size()-2]);
	height=std::stoi(sp2[sp2.size()-1]);
	ofs << e.content() << "\" width=\"" << lroundf(width*70./height) << "\"";
    }
    else {
	ofs << "default_200_200.png\" width=\"70\"";
    }
    ofs << " height=\"70\" /></td><td><span class=\"fs24px bold\">" << title << "</span><br /><span class=\"fs16px bold\"><span class=\"blue\">ds" << dsnum << "</span>";
    query.set("doi","dssdb.dsvrsn","dsid = 'ds"+dsnum+"' and status = 'A'");
    if (query.submit(server) == 0 && query.fetch_row(row) && !row[0].empty()) {
	ofs << "&nbsp;|&nbsp;<span class=\"blue\">DOI: " << row[0] << "</span>";
    }
    ofs << "</span><div id=\"ds_bookmark\" style=\"display: inline; margin-left: 2px\"><?php if ($can_bookmark) { $dsid=\"" << dsnum << "\"; if ($is_bookmarked) { include(\"dsbookmarks/showunset.inc\"); } else { include(\"dsbookmarks/showset.inc\"); } } ?></div></td></tr>" << std::endl;
    e=xdoc.element("dsOverview/contact");
    sp=strutils::split(e.content());
    query.set("select logname,phoneno from dssdb.dssgrp where fstname = '"+sp[0]+"' and lstname = '"+sp[1]+"'");
    if (query.submit(server) < 0) {
	metautils::logError("mysql error while trying to get specialist information: "+query.error(),"dsgen",user,args.argsString);
    }
    if (!query.fetch_row(row)) {
	metautils::logError("no result returned for specialist '"+e.content()+"'","dsgen",user,args.argsString);
    }
    sdum=row[1];
    strutils::replace_all(sdum,"(","");
    strutils::replace_all(sdum,")","");
    ofs << "<tr valign=\"bottom\"><td align=\"right\"><span class=\"fs16px\">For assistance, contact <a class=\"blue\" href=\"mailto:"+row[0]+"@ucar.edu\">"+e.content()+"</a> <span class=\"mediumGrayText\">("+sdum+").</span></span></td></tr>" << std::endl;
    ofs << "</table>" << std::endl;
    ofs << "<?php include(\"main/dstabs";
    if (type == "dead") {
	ofs << "-" << type;
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

bool compareStrings(const std::string& left,const std::string& right)
{
  if (left <= right) {
    return true;
  }
  else {
    return false;
  }
}

bool compareReferences(XMLElement& left,XMLElement& right)
{ 
  std::string l,r;
  XMLElement e;
  
  e=left.element("year");
  l=e.content();
  e=right.element("year");
  r=e.content();
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

bool compareLevels(const std::string& left,const std::string& right)
{
  size_t lidx,ridx;
  std::string lval,lunits,rval,runits;
  int ilval,irval;

  if (((left[0] >= '0' && left[0] <= '9') || (left[0] == '-' && left[1] >= '0' && left[1] <= '9')) && ((right[0] >= '0' && right[0] <= '9') || (right[0] == '-' && right[1] >= '0' && right[1] <= '9'))) {
    lidx=left.find(" ");
    if (lidx != std::string::npos) {
	lval=left.substr(0,lidx);
	lunits=left.substr(lidx+1);
    }
    else {
	lval=left;
	lunits="";
    }
    if ( (ridx=right.find(" ")) != std::string::npos) {
	rval=right.substr(0,ridx);
	runits=right.substr(ridx+1);
    }
    else {
	rval=right;
	runits="";
    }
    ilval=std::stoi(lval);
    irval=std::stoi(rval);
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

void insertTable(std::ofstream& ofs,std::list<std::string> list,int max_columns,std::string color1,std::string color2)
{
  int num_rows;
  int cmax,max_mod;
  int n,m,l;

  ofs << "<table cellspacing=\"0\">" << std::endl;
  num_rows=list.size()/max_columns;
  if ( (list.size() % max_columns) != 0) {
    ++num_rows;
    max_mod=(list.size() % max_columns);
    for (n=max_columns-1; n > 1; n--) {
	if (static_cast<int>(list.size()/n) <= num_rows) {
	  if ( (list.size() % n) == 0) {
	    max_columns=n;
	    break;
	  }
	  else if (static_cast<int>(list.size() % n) > max_mod) {
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
  cmax=max_columns-1;
  n=0;
  m=0;
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
	for (l=n; l < max_columns; ++l) {
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

void insertTextField(std::ofstream& ofs,std::string element_name,std::string section_title)
{
  size_t idx;

  auto sdum=xdoc.element("dsOverview/"+element_name).to_string();
  if (sdum.length() > 0) {
    strutils::replace_all(sdum,"<"+element_name+">","");
    strutils::replace_all(sdum,"</"+element_name+">","");
    strutils::trim(sdum);
    if ( (idx=sdum.find("<p")) == std::string::npos) {
	idx=sdum.find("<P");
    }
    if (idx == 0) {
	auto idx2=sdum.find(">",idx);
	sdum.insert(idx2," style=\"margin: 0px; padding: 0px\"");
    }
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">" << section_title << ":</td><td>" << sdum << "</td></tr>" << std::endl;
  }
}

void generateDescription(std::string type)
{
  std::string sdum,sdum2;
  MySQL::Row row,row2;
  std::string start_date_time,end_date_time;
  my::map<PeriodEntry> periods_table;
  std::list<std::string> databases,formats,types,grids,levels;
  PeriodEntry pe;
  int n,m;
  StringEntry se;
  my::map<CoverageEntry> unique_grid_definitions_table;
  CoverageEntry ce;
  my::map<GridDefinitionEntry> grid_definition_table;
  GridDefinitionEntry gde;
  double west_lon,east_lon,south_lat,north_lat;
  std::list<double> straddle_east_lons;
  double min_west_lon=9999.,max_east_lon=-9999.,min_south_lat=9999.,max_north_lat=-9999.;
  my::map<XEntry> groups_table,rda_files_table(99999),metadata_files_table(99999);
  XEntry xe;
  XMLDocument fdoc;
  std::deque<std::string> sp;
  bool grouped_periods=false;
  bool found_content_metadata=false;
  bool started_pubs=false;
  bool is_local;

  std::string dsnum2=strutils::substitute(dsnum,".","");
  std::ofstream ofs;
  ofs.open((temp_dir.name()+"/description.html").c_str());
  if (!ofs.is_open()) {
    metautils::logError("unable to open output for 'description.html'","dsgen",user,args.argsString);
  }
  if (type == "dead") {
    ofs << "<table class=\"fs16px\" width=\"100%\" cellspacing=\"10\" cellpadding=\"0\" border=\"0\">" << std::endl;
    insertTextField(ofs,"summary","Abstract");
    ofs << "</table>" << std::endl;
    ofs.close();
    return;
  }
/*
  if (type == "internal") {
    ofs << "<ul>This dataset has been removed from public view.  If you have questions about this dataset, please contact the specialist that is named above.</ul>" << std::endl;
    ofs.close();
    return;
  }
*/
  databases=metautils::getCMDDatabases("dsgen","x",dsnum);
  if (databases.size() == 0) {
    metautils::logError("empty CMD database list","dsgen",user,args.argsString);
  }
  for (const auto& db : databases) {
    if (db[0] != 'V' && table_exists(server,db+".ds"+dsnum2+"_primaries")) {
	MySQL::LocalQuery query("select distinct format from "+db+".formats as f left join "+db+".ds"+dsnum2+"_primaries as d on d.format_code = f.code where !isnull(d.format_code)");
	if (query.submit(server) < 0) {
	  metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
	}
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
  ofs << "    $res=mysql_query(\"select dsid from metautil.rda_blog where dsid = '" << dsnum << "'\");" << std::endl;
  ofs << "    if (mysql_numrows($res) > 0) {" << std::endl;
  ofs << "      $rda_blog=true;" << std::endl;
  ofs << "    }" << std::endl;
  ofs << "    $res=mysql_query(\"select cdg_page_path,cdg_guidance_path from metautil.climate_data_guide where dsid = '" << dsnum << "'\");" << std::endl;
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
  insertTextField(ofs,"summary","Abstract");
// acknowledgments
  insertTextField(ofs,"acknowledgement","Acknowledgments");
// temporal range(s)
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Temporal Range:</td><td>";
  MySQL::LocalQuery query("select dsid from dssdb.dsgroup where dsid = 'ds"+dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
  }
  if (query.num_rows() > 0) {
    query.set("select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) where p.dsid = 'ds"+dsnum+"' and g.pindex = 0 and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' union select p.date_start,p.time_start,p.start_flag,p.date_end,p.time_end,p.end_flag,p.time_zone,g2.title,g.grpid from dssdb.dsperiod as p left join dssdb.dsgroup as g on (p.dsid = g.dsid and p.gindex = g.gindex) left join dssdb.dsgroup as g2 on (p.dsid = g2.dsid and g.pindex = g2.gindex) where p.dsid = 'ds"+dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' and !isnull(g2.title) order by title");
  }
  else {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
  }
  if (query.submit(server) < 0) {
    metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
  }
  if (query.num_rows() == 0) {
    query.set("select date_start,time_start,start_flag,date_end,time_end,end_flag,time_zone,NULL,NULL from dssdb.dsperiod where dsid = 'ds"+dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01'");
    if (query.submit(server) < 0) {
	metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
    }
  }
  if (query.num_rows() > 1) {
    MySQL::LocalQuery query2("distinct gindex","dssdb.dsperiod","dsid = 'ds"+dsnum+"'");
    if (query2.submit(server) < 0) {
	metautils::logError("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.argsString);
    }
    if (query2.num_rows() > 1) {
	grouped_periods=true;
	query2.set("select gindex,title from dssdb.dsgroup where dsid = 'ds"+dsnum+"'");
	if (query2.submit(server) < 0) {
	  metautils::logError("error: "+query2.error()+" while getting groups data","dsgen",user,args.argsString);
	}
	while (query2.fetch_row(row2)) {
	  xe.key=row2[0];
	  xe.a.reset(new std::string);
	  *(xe.a)=row2[1];
	  groups_table.insert(xe);
	}
//	query2.set("select mssfile,tindex from dssdb.mssfile where dsid = 'ds"+dsnum+"' and property = 'P' and retention_days > 0");
query2.set("select mssfile,tindex from dssdb.mssfile where dsid = 'ds"+dsnum+"' and type = 'P' and status = 'P'");
	if (query2.submit(server) < 0) {
	  metautils::logError("error: "+query2.error()+" while getting RDA files data","dsgen",user,args.argsString);
	}
	while (query2.fetch_row(row2)) {
	  xe.key=row2[0];
	  xe.a.reset(new std::string);
	  *(xe.a)=row2[1];
	  rda_files_table.insert(xe);
	}
	query2.set("select code,mssID from GrML.ds"+dsnum2+"_primaries");
	if (query2.submit(server) == 0) {
	  while (query2.fetch_row(row2)) {
	    xe.key=row2[0];
	    xe.a.reset(new std::string);
	    *(xe.a)=row2[1];
	    metadata_files_table.insert(xe);
	  }
	}
	query2.set("select min(concat(date_start,' ',time_start)),min(start_flag),max(concat(date_end,' ',time_end)),min(end_flag),any_value(time_zone) from dssdb.dsperiod where dsid = 'ds"+dsnum+"' and date_start > '0000-00-00' and date_start < '3000-01-01' and date_end > '0000-00-00' and date_end < '3000-01-01' group by dsid");
	if (query2.submit(server) < 0) {
	  metautils::logError("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.argsString);
	}
	query2.fetch_row(row2);
	start_date_time=summarizeMetadata::setDateTimeString(row2[0],row2[1],row2[4]);
	end_date_time=summarizeMetadata::setDateTimeString(row2[2],row2[3],row2[4]);
	sdum=start_date_time;
	if (end_date_time.length() > 0 && end_date_time != start_date_time) {
	  sdum+=" to "+end_date_time;
	}
	ofs << sdum << " <span class=\"fs13px\">(Entire dataset)</span>";
	ofs << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand dataset product period list\"><img src=\"/images/bluetriangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Period details by dataset product</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/bluetriangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse dataset product period list\"></a><span class=\"fs13px\">Period details by dataset product:";
	div_num+=2;
    }
  }
  while (query.fetch_row(row)) {
    start_date_time=summarizeMetadata::setDateTimeString(row[0]+" "+row[1],row[2],row[6]);
    end_date_time=summarizeMetadata::setDateTimeString(row[3]+" "+row[4],row[5],row[6]);
    if (row[7].length() > 0) {
	pe.key=row[7];
    }
    else {
	pe.key=row[8];
    }
    if (!periods_table.found(pe.key,pe)) {
	pe.start=start_date_time;
	pe.end=end_date_time;
	periods_table.insert(pe);
    }
    else {
	if (start_date_time < pe.start) {
	  pe.start=start_date_time;
	}
	if (end_date_time > pe.end) {
	  pe.end=end_date_time;
	}
	periods_table.replace(pe);
    }   
  }
  n=0;
  for (const auto& key : periods_table.keys()) {
    periods_table.found(key,pe);
    sdum=pe.start;
    if (pe.end.length() > 0 && pe.end != pe.start) {
	sdum+=" to "+pe.end;
    }
//    if (key.length() > 0) {
if (periods_table.size() > 1) {
	sdum+=" ("+key+")";
    }
/*
    if (grouped_periods || n > 0) {
	ofs << "<br />";
    }
*/
    if (periods_table.size() > 1) {
	ofs << "<div style=\"margin-left: 10px\">" << sdum << "</div>";
    }
    else {
	ofs << sdum;
    }
    ++n;
  }
  if (grouped_periods) {
    ofs << "</span></span>";
  }
  ofs << "</td></tr>" << std::endl;
// update frequency
  auto e=xdoc.element("dsOverview/continuingUpdate");
  if (e.attribute_value("value") == "yes") {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Updates:</td><td>" << strutils::capitalize(e.attribute_value("frequency")) << "</td></tr>" << std::endl;
  }
// access restrictions
  e=xdoc.element("dsOverview/restrictions/access");
  sdum=e.to_string();
  strutils::replace_all(sdum,"<access>","");
  strutils::replace_all(sdum,"</access>","");
  size_t idx;
  if ( (idx=sdum.find("<p")) == std::string::npos) {
    idx=sdum.find("<P");
  }
  if (idx != std::string::npos) {
    auto idx2=sdum.find(">",idx);
    sdum.insert(idx2," style=\"margin: 0px; padding: 0px\"");
  }
  if (sdum.length() > 0) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Access Restrictions:</td><td>" << sdum << "</td></tr>" << std::endl;
  }
// usage restrictions
  e=xdoc.element("dsOverview/restrictions/usage");
  sdum=e.to_string();
  strutils::replace_all(sdum,"<usage>","");
  strutils::replace_all(sdum,"</usage>","");
  if ( (idx=sdum.find("<p")) == std::string::npos) {
    idx=sdum.find("<P");
  }
  if (idx != std::string::npos) {
    auto idx2=sdum.find(">",idx);
    sdum.insert(idx2," style=\"margin: 0px; padding: 0px\"");
  }
  if (sdum.length() > 0) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Usage Restrictions:</td><td>" << sdum << "</td></tr>" << std::endl;
  }
// variables
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Variables:</td><td>" << std::endl;
  query.set("select substring_index(path,' > ',-1) as var from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD' and v.dsid = '"+dsnum+"' order by var");
  if (query.submit(server) < 0) {
    metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
  }
  std::list<std::string> strings;
  while (query.fetch_row(row)) {
    strings.emplace_back(strutils::capitalize(row[0]));
  }
  insertTable(ofs,strings,4,"#e1eaff","#c8daff");
  auto elist=xdoc.element_list("dsOverview/contentMetadata/detailedVariables/detailedVariable");
  if (elist.size() > 0) {
    for (const auto& ele : elist) {
	if (std::regex_search(ele.content(),std::regex("^http://")) || std::regex_search(ele.content(),std::regex("^https://"))) {
	  ofs << "<a href=\"" << ele.content() << "\">Detailed Variable List</a><br />" << std::endl;
	  break;
	}
    }
  }
  struct stat buf;
  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/grib.html",buf)) {
    ofs << "<div>GRIB parameter table:  <a href=\"/datasets/ds" << dsnum << "/#metadata/grib.html?_do=y\">HTML</a>";
    if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/grib.xml",buf))
	ofs << " | <a href=\"/datasets/ds" << dsnum << "/metadata/grib.xml\">XML</a></div>";
    ofs << std::endl;
  }
  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/grib2.html",buf)) {
    ofs << "<div>GRIB2 parameter table:  <a href=\"/datasets/ds" << dsnum << "/#metadata/grib2.html?_do=y\">HTML</a>";
    if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/grib2.xml",buf)) {
	ofs << " | <a href=\"/datasets/ds" << dsnum << "/metadata/grib2.xml\">XML</a></div>"; 
    }
    ofs << std::endl;
  }
  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/on84.html",buf)) {
    ofs << "<div>ON84 parameter table:  <a href=\"/datasets/ds" << dsnum << "/metadata/on84.html\">HTML</a>";
    if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/on84.html",buf)) {
	ofs << " | <a href=\"/datasets/ds" << dsnum << "/metadata/on84.xml\">XML</a></div>";
    }
    ofs << std::endl;
  }
  query.set("gindex,title","dssdb.dsgroup","dsid = 'ds"+dsnum+"' and pindex = 0 and pmsscnt > 0");
  if (query.submit(server) == 0) {
    std::stringstream vars_by_product;
    while (query.fetch_row(row)) {
	if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/customize.GrML."+row[0],buf)) {
	  sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+dsnum+"/metadata/customize.GrML."+row[0],temp_dir.name());
	  if (sdum.length() > 0) {
	    std::ifstream ifs;
	    char line[32768];
	    ifs.open(sdum);
	    ifs.getline(line,32768);
	    if (std::regex_search(line,std::regex("^curl_subset="))) {
		ifs.getline(line,32768);
	    }
	    auto nvar=std::stoi(line);
	    std::list<std::string> varlist;
	    for (n=0; n < nvar; ++n) {
		ifs.getline(line,32768);
		sp=strutils::split(line,"<!>");
		varlist.emplace_back(sp[1]);
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
	  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/grib2_levels.html",buf)) {
	    ofs << "<br /><a href=\"/datasets/ds" << dsnum << "/#metadata/grib2_levels.html?_do=y\">GRIB2 level table</a>"; 
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
	for (const auto& ele : elist) {
	  if ((ele.attribute_value("value") == "0" || (ele.attribute_value("top") == "0" && ele.attribute_value("bottom") == "0")) && ele.attribute_value("units").length() == 0) {
	    levels.emplace_back(ele.attribute_value("type"));
	  }
	  else {
	    if (ele.attribute_value("value").length() > 0) {
		sdum=ele.attribute_value("value")+" "+ele.attribute_value("units");
		if (std::regex_search(sdum,std::regex("^\\."))) {
		  sdum="0"+sdum;
		}
		sdum2=ele.attribute_value("type");
		if (ele.attribute_value("value") != "0" && strutils::contains(sdum2,"height below")) {
		  sdum="-"+sdum;
		}
		levels.emplace_back(sdum);
	    }
	    else {
		sdum=ele.attribute_value("top");
		if (std::regex_search(sdum,std::regex("^\\."))) {
		  sdum="0"+sdum;
		}
		if (ele.attribute_value("top") != ele.attribute_value("bottom")) {
		  sdum2=ele.attribute_value("bottom");
		  if (std::regex_search(sdum2,std::regex("^\\."))) {
		    sdum2="0"+sdum2;
		  }
		  sdum+="-"+sdum2;
		}
		sdum+=" "+ele.attribute_value("units");
		levels.emplace_back(sdum);
	    }
	  }
	}
	levels.sort(compareLevels);
	insertTable(ofs,levels,4,"#c8daff","#e1eaff");
	ofs << "</td></tr>" << std::endl;
    }
  }
// temporal frequency
  if (!found_content_metadata) {
    elist=xdoc.element_list("dsOverview/contentMetadata/temporalFrequency");
    m=0;
    if (elist.size() > 0) {
	ofs << "<tr style=\"vertical-align: bottom\"><td class=\"bold\">Temporal Frequencies:</td><td>";
	for (const auto& ele : elist) {
	  sdum=ele.attribute_value("type");
	  if (m > 0) {
	    ofs << ", ";
	  }
	  if (sdum == "regular") {
	    n=std::stoi(ele.attribute_value("number"));
	    ofs << "Every ";
	    if (n > 1) {
		ofs << n << " ";
	    }
	    ofs << ele.attribute_value("unit");
	    if (n > 1) {
		ofs << "s";
	    }
	    sdum=ele.attribute_value("statistics");
	    if (sdum.length() > 0) {
		ofs << " (" << strutils::capitalize(sdum) << ")";
	    }
	    ofs << std::endl;
	  }
	  else if (sdum == "irregular") {
	    ofs << "various times per " << ele.attribute_value("unit");
	    sdum=ele.attribute_value("statistics");
	    if (sdum.length() > 0) {
		ofs << " (" << strutils::capitalize(sdum) << ")";
	    }
	  }
	  else if (sdum == "climatology") {
	    sdum=ele.attribute_value("unit");
	    if (sdum == "hour") {
		sdum="Hourly";
	    }
	    else if (sdum == "day") {
		sdum="Daily";
	    }
	    else if (sdum == "week") {
		sdum="Weekly";
	    }
	    else if (sdum == "month") {
		sdum="Monthly";
	    }
	    else if (sdum == "winter") {
		sdum="Winter Season";
	    }
	    else if (sdum == "spring") {
		sdum="Spring Season";
	    }
	    else if (sdum == "summer") {
		sdum="Summer Season";
	    }
	    else if (sdum == "autumn") {
		sdum="Autumn Season";
	    }
	    else if (sdum == "year") {
		sdum="Annual";
	    }
	    else if (sdum == "30-year") {
		sdum="30-year (climate normal)";
	    }
	    ofs << sdum << " Climatology";
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
	n=0;
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
	n=0;
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
  if (found_content_metadata) {
    for (const auto& type : types) {
	if (type == "grid") {
	  if (grouped_periods) {
	    query.set("select gridDefinition_codes,mssID_code from GrML.ds"+dsnum2+"_grid_definitions");
	  }
	  else {
	    query.set("select distinct gridDefinition_codes from GrML.ds"+dsnum2+"_agrids");
	  }
	  if (query.submit(server) < 0)
	    metautils::logError("error: "+query.error()+" while getting grid definitions","dsgen",user,args.argsString);
	  while (query.fetch_row(row)) {
	    std::vector<size_t> values;
	    bitmap::uncompressValues(row[0],values);
	    for (const auto& value : values) {
		gde.key=value;
		if (!grid_definition_table.found(gde.key,gde)) {
		  MySQL::LocalQuery query2("definition,defParams","GrML.gridDefinitions","code = "+strutils::itos(gde.key));
		  if (query2.submit(server) < 0) {
		    metautils::logError("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.argsString);
		  }
		  query2.fetch_row(row2);
		  gde.definition=row2[0];
		  gde.defParams=row2[1];
		  grid_definition_table.insert(gde);
		}
		ce.key=gde.definition+"<!>"+gde.defParams;
		se.key="";
		if (query.length() > 1) {
		  if (metadata_files_table.found(row[1],xe) && rda_files_table.found(*(xe.a),xe) && groups_table.found(*(xe.a),xe)) {
		    se.key=*(xe.a);
		  }
		}
		if (!unique_grid_definitions_table.found(ce.key,ce)) {
		  if (se.key.length() > 0) {
		    ce.groups.reset(new my::map<StringEntry>);
		    ce.groups->insert(se);
		  }
		  unique_grid_definitions_table.insert(ce);
		}
		else if (se.key.length() > 0) {
		  if (ce.groups == nullptr) {
		    ce.groups.reset(new my::map<StringEntry>);
		  }
		  if (!ce.groups->found(se.key,se)) {
		    ce.groups->insert(se);
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
	sdum=ele.attribute_value("definition")+"<!>"+ele.attribute_value("numX")+":"+ele.attribute_value("numY");
	if (std::regex_search(sdum,std::regex("^latLon"))) {
	  sdum+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(sdum,std::regex("^gaussLatLon"))) {
	  sdum+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("numY");
	}
	else if (std::regex_search(sdum,std::regex("^polarStereographic"))) {
	  sdum+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":60"+ele.attribute_value("pole")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes");
	}
	else if (std::regex_search(sdum,std::regex("^mercator"))) {
	  sdum+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("endLat")+":"+ele.attribute_value("endLon")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes")+":"+ele.attribute_value("resLat");
	}
	else if (std::regex_search(sdum,std::regex("^lambertConformal"))) {
	  sdum+=":"+ele.attribute_value("startLat")+":"+ele.attribute_value("startLon")+":"+ele.attribute_value("resLat")+":"+ele.attribute_value("projLon")+":"+ele.attribute_value("pole")+":"+ele.attribute_value("xRes")+":"+ele.attribute_value("yRes")+":"+ele.attribute_value("stdParallel1")+":"+ele.attribute_value("stdParallel2");
	}
	ce.key=sdum;
	unique_grid_definitions_table.insert(ce);
    }
  }
  for (const auto& grid : unique_grid_definitions_table.keys()) {
    if (getSpatialDomainFromGridDefinition(grid,"primeMeridian",west_lon,south_lat,east_lon,north_lat)) {
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
    sdum=strutils::ftos(fabs(min_west_lon),3);
    if (min_west_lon < 0) {
	sdum+="W";
    }
    else {
	sdum+="E";
    }
    ofs << "Longitude Range:  Westernmost=" << sdum << "  Easternmost=";
    sdum=strutils::ftos(fabs(max_east_lon),3);
    if (max_east_lon < 0) {
	sdum+="W";
    }
    else {
	sdum+="E";
    }
    ofs << sdum << "<br />" << std::endl;
    sdum=strutils::ftos(fabs(min_south_lat),3);
    if (min_south_lat < 0) {
	sdum+="S";
    }
    else {
        sdum+="N";
    }
    ofs << "Latitude Range:  Southernmost=" << sdum << "  Northernmost=";
    sdum=strutils::ftos(fabs(max_north_lat),3);
    if (max_north_lat < 0) {
	sdum+="S";
    }
    else {
	sdum+="N";
    }
    ofs << sdum << std::endl;
    ofs << "<br /><span id=\"D" << div_num << "\"><a href=\"javascript:swapDivs(" << div_num << "," << div_num+1 << ")\" title=\"Expand coverage details\"><img src=\"/images/triangle.gif\" width=\"12\" height=\"15\" border=\"0\"><span class=\"fs13px\">Detailed coverage information</span></a></span><span style=\"visibility: hidden; position: absolute; top: 0\" id=\"D" << div_num+1 << "\"><a href=\"javascript:swapDivs(" << div_num+1 << "," << div_num << ")\"><img src=\"/images/triangle90.gif\" width=\"15\" height=\"12\" border=\"0\" title=\"Collapse coverage details\"></a><span class=\"fs13px\">Detailed coverage information:" << std::endl;
    div_num+=2;
    my::map<CoverageEntry> grid_definitions;
    for (const auto& key : unique_grid_definitions_table.keys()) {
	unique_grid_definitions_table.found(key,ce);
	ce.key=convertGridDefinition(key);
	grid_definitions.insert(ce);
    }
    grid_definitions.keysort(compareStrings);
    for (const auto& key : grid_definitions.keys()) {
	ofs << "<div style=\"margin-left: 10px\">" << key;
	if (grid_definitions.size() > 1) {
	  grid_definitions.found(key,ce);
	  if (ce.groups != nullptr) {
	    ofs << "<div style=\"margin-left: 15px; color: #6a6a6a\">(";
	    n=0;
	    for (const auto& gkey : ce.groups->keys()) {
		if (n++ > 0) {
		  ofs << ", ";
		}
		ofs << gkey;
	    }
	    ofs << ")</div>";
	  }
	}
	ofs << "</div>";
    }
    ofs << "</span></span></td></tr>" << std::endl;
  }
// data contributors
  query.set("select g.path from search.contributors_new as c left join search.GCMD_providers as g on g.uuid = c.keyword where c.dsid = '"+dsnum+"' and c.vocabulary = 'GCMD'");
  if (query.submit(server) < 0) {
    metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
  }
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Contributors:</td><td>";
  n=0;
  while (query.fetch_row(row)) {
    if (n > 0) {
	ofs << " | ";
    }
    sdum=row[0];
    if ( (idx=sdum.find(">")) != std::string::npos) {
	sdum2=sdum.substr(idx+1);
	sdum=sdum.substr(0,idx);
    }
    strutils::trim(sdum);
    strutils::trim(sdum2);
    ofs << "<span class=\"infosrc\" onMouseOver=\"javascript:popInfo(this,'src" << n << "','#e1eaff','left','bottom')\" onMouseOut=\"javascript:hideInfo('src" << n << "')\">" << sdum << "</span><div class=\"info\" id=\"src" << n << "\" class=\"source\"><small>" << sdum2 << "</small></div>";
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
    if (elist.size() > 1)
	ofs << "top";
    else
	ofs << "bottom";
    ofs << "\"><td class=\"bold\">Related Resources:</td><td>";
    for (const auto& ele : elist) {
	sdum=ele.content();
	strutils::trim(sdum);
	if (strutils::has_ending(sdum,".") && sdum[sdum.length()-2] != '.') {
	  strutils::chop(sdum);
	}
	ofs << "<a href=\"" << ele.attribute_value("url") << "\">";
	is_local=false;
	sdum2=ele.attribute_value("url");
	if (std::regex_search(sdum2,std::regex("^http://rda.ucar.edu")) || std::regex_search(sdum2,std::regex("^https://rda.ucar.edu")) || std::regex_search(sdum2,std::regex("^http://dss.ucar.edu")) || std::regex_search(sdum2,std::regex("^https://dss.ucar.edu"))) {
	  is_local=true;
	}
	if (!is_local) {
	  ofs << "<span class=\"italic\">";
	}
	ofs << sdum;
	if (!is_local) {
	  ofs << "</span>";
	}
	ofs << "</a><br />";
    }
    ofs << "</td></tr>" << std::endl;
  }
// WRF Preprocessing System
  ofs << "<?php" << std::endl;
  ofs << "  if (file_exists(\"/usr/local/www/server_root/web/datasets/ds" << dsnum << "/metadata/Vtable.RDA_ds" << dsnum << "\")) {" << std::endl;
  ofs << "    print \"<tr style=\\\"vertical-align: bottom\\\"><td class=\\\"bold\\\">WRF Preprocessing System (WPS):</td><td>The GRIB-formatted data in this dataset can be used to initialize the Weather Research and Forecasting (WRF) Model.<br /><!--<a target=\\\"_vtable\\\" href=\\\"/datasets/ds" << dsnum << "/metadata/Vtable.RDA_ds" << dsnum << "\\\">Vtable</a>&nbsp;|&nbsp;--><a target=\\\"_wrf\\\" href=\\\"http://www2.mmm.ucar.edu/wrf/users/download/free_data.html\\\"><em>WRF Vtables</em></a></td></tr>\";" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
// publications
  elist=xdoc.element_list("dsOverview/reference");
  if (elist.size() > 0) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">Publications:</td><td>";
    started_pubs=true;
    elist.sort(compareReferences);
    for (const auto& ele : elist) {
	ofs << "<div>" << ele.element("authorList").content() << ", " << ele.element("year").content() << ": ";
	sdum=ele.attribute_value("type");
	if (sdum == "journal") {
	  e=ele.element("periodical");
	  sdum=ele.element("url").content();
	  sdum2=ele.element("title").content();
	  if (sdum.length() > 0) {
	    ofs << "<a href=\"" << sdum << "\">" << sdum2 << "</a>";
	  }
	  else {
	    ofs << sdum2;
	  }
	  if (!strutils::has_ending(sdum2,"?")) {
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
	    sdum=e.attribute_value("pages");
	    if (std::regex_search(sdum,std::regex("^AGU:"))) {
		ofs << sdum.substr(4);
	    }
	    else {
		sp=strutils::split(sdum,"-"); 
		if (sp.size() == 2 && sp[0] == sp[1]) {
		  ofs << sp[0];
		}
		else {
		  ofs << e.attribute_value("pages");
		}
	    }
	  }
	  sdum=ele.element("doi").content();
	  if (sdum.length() > 0) {
	    ofs << " (DOI: " << sdum << ")";
	  }
	  ofs << ".";
	}
	else if (sdum == "preprint") {
	  e=ele.element("conference");
	  sdum=ele.element("url").content();
	  if (sdum.length() > 0)
	    ofs << "<a href=\"" << sdum << "\">" << ele.element("title").content() << "</a>";
	  else
	    ofs << ele.element("title").content();
	  ofs << ".  <i>Proceedings of the " << e.content() << "</i>, " << e.attribute_value("host") << ", " << e.attribute_value("location");
	  sdum=e.attribute_value("pages");
	  if (sdum.length() > 0)
	    ofs << ", " << sdum;
	  sdum=ele.element("doi").content();
	  if (sdum.length() > 0)
	    ofs << " (DOI: " << sdum << ")";
	  ofs << ".";
	}
	else if (sdum == "technical_report") {
	  e=ele.element("organization");
	  sdum=ele.element("url").content();
	  if (sdum.length() > 0)
	    ofs << "<i><a href=\"" << sdum << "\">" << ele.element("title").content() << "</a>.</i>";
	  else
	    ofs << "<i>" << ele.element("title").content() << ".</i>";
	  ofs << "  ";
	  sdum=e.attribute_value("reportID");
	  if (sdum.length() > 0)
	    ofs << sdum << ", ";
	  ofs << e.content();
	  sdum=e.attribute_value("pages");
	  if (sdum != "-99")
	    ofs << ", " << e.attribute_value("pages") << " pp.";
	  sdum=ele.element("doi").content();
	  if (sdum.length() > 0)
	    ofs << " (DOI: " << sdum << ").";
	}
	else if (sdum == "book") {
	  e=ele.element("publisher");
	  ofs << "<i>" << ele.element("title").content() << "</i>. " << e.content() << ", " << e.attribute_value("place");
	  sdum=ele.element("doi").content();
	  if (sdum.length() > 0)
	    ofs << " (DOI: " << sdum << ")";
	  ofs << ".";
	}
	else if (sdum == "book_chapter") {
	  e=ele.element("book");
	  ofs << "\"" << ele.element("title").content() << "\", in " << e.content() << ". Ed. " << e.attribute_value("editor") << ", " << e.attribute_value("publisher") << ", ";
	  if (e.attribute_value("pages") == "0-0")
	    ofs << "In Press";
	  else
	    ofs << e.attribute_value("pages");
	  sdum=ele.element("doi").content();
	  if (sdum.length() > 0)
	    ofs << " (DOI: " << sdum << ")";
	  ofs << ".";
	}
	ofs << "</div>" << std::endl;
	sdum=ele.element("annotation").content();
	if (sdum.length() > 0)
	  ofs << "<div style=\"margin-left: 15px; color: #5f5f5f\">" << sdum << "</div>" << std::endl;
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
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold\">How to Cite This Dataset:<div style=\"background-color: #2a70ae; color: white; width: 40px; padding: 1px; margin-top: 3px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style=ris'\" title=\"download citation in RIS format\">RIS</div><div style=\"background-color: #2a70ae; color: white; width: 60px; padding: 2px 8px 2px 8px; font-size: 16px; font-weight: bold; font-family: serif; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer; margin-top: 5px\" onClick=\"location='/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style=bibtex'\" title=\"download citation in BibTeX format\">BibTeX</div></td><td><div id=\"citation\" style=\"border: thin solid black; padding: 5px\"><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onLoad=\"getAjaxContent('GET',null,'/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style=esip','citation')\" /></div>" << std::endl;
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
  ofs << "    print \"<a href=\\\"javascript:void(0)\\\" onClick=\\\"getAjaxContent('GET',null,'/php/ajax/mydatacitation.php?tab=ds_history&dsid=ds" << dsnum << "&b=no','content_container')\\\">\";" << std::endl;
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
  query.set("primary_size","dssdb.dataset","dsid = 'ds"+dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
  }
  MySQL::LocalQuery query2("select primary_size,title,grpid from dssdb.dsgroup where dsid = 'ds"+dsnum+"' and pindex = 0 and primary_size > 0");
  if (query2.submit(server) < 0) {
    metautils::logError("query: "+query2.show()+" returned error: "+query2.error(),"dsgen",user,args.argsString);
  }
  const int VOLUME_LEN=4;
  const char *v[VOLUME_LEN]={"MB","GB","TB","PB"};
  if (query.fetch_row(row) && row[0].length() > 0) {
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
    while (query2.fetch_row(row2)) {
	auto volume=std::stof(row2[0])/1000000.;
	n=0;
	while (volume > 1000. && n < VOLUME_LEN) {
	  volume/=1000.;
	  ++n;
	}
	ofs << "<div style=\"margin-left: 10px\">";
	if (row2[1].length() > 0) {
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
  if (stat(("/"+server_root+"/web/metadata").c_str(),&buf) == 0) {
    fdoc.open("/"+server_root+"/web/metadata/FormatReferences.xml");
  }
  else {
    sdum=getRemoteWebFile("https://rda.ucar.edu/metadata/FormatReferences.xml",temp_dir.name());
    fdoc.open(sdum);
  }
  if (!fdoc) {
    metautils::logError("unable to open FormatReferences.xml","dsgen",user,args.argsString);
  }
  n=0;
  for (const auto& format : formats) {
    sp=strutils::split(format,"<!>");
    sdum=sp[0];
    if (std::regex_search(sdum,std::regex("^proprietary_"))) {
	strutils::replace_all(sdum,"proprietary_","");
	if (sp[1].length() > 0)
	  sdum2=sp[1];
	else {
	  sdum+=" (see dataset documentation)";
	  sdum2="";
	}
    }
    else {
	e=fdoc.element(("formatReferences/format@name="+sdum));
	sdum2=e.attribute_value("href");
    }
    if (sdum2.length() > 0) {
	ofs << "<a href=\"" << sdum2 << "\" target=\"_format\">";
	if (!std::regex_search(sdum2,std::regex("^http://rda.ucar.edu"))) {
	  ofs << "<i>";
	}
    }
    strutils::replace_all(sdum,"_"," ");
    ofs << sdum;
    if (sdum2.length() > 0) {
	ofs << "</a>";
	if (!std::regex_search(sdum2,std::regex("^http://rda.ucar.edu"))) {
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
          metautils::logError("query: "+query.show()+" returned error: "+query.error(),"dsgen",user,args.argsString);
	if (query.fetch_row(row)) {
	  ofs << "<tr valign=\"top\"><td><a href=\"/datasets/ds" << row[0] << "#description\">" << row[0] << "</a></td><td>-</td><td>" << row[1] << "</td></tr>";
	}
    }
    ofs << "</table></td></tr>" << std::endl;
  }
// more details
  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+dsnum+"/metadata/detailed.html",buf)) {
    ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">More Details:</td><td>View <a href=\"#metadata/detailed.html?_do=y\">more details</a> for this dataset, including dataset citation, data contributors, and other detailed metadata</td></tr>" << std::endl;
  }
// RDA blog
  ofs << "<?php" << std::endl;
  ofs << "  if ($rda_blog) {" << std::endl;
  ofs << "    print \"<tr style=\\\"vertical-align: top\\\"><td class=\\\"bold nowrap\\\">RDA Blog:</td><td>Read <a href=\\\"http://ncarrda.blogspot.com/search/label/ds" << dsnum << "\\\" target=\\\"_rdablog\\\">posts</a> on the RDA blog that are related to this dataset</td></tr>\n\";" << std::endl;
  ofs << "  }" << std::endl;
  ofs << "?>" << std::endl;
// data access
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Data Access:</td><td><div style=\"display: inline; float: left\">Click the </div><a class=\"clean\" href=\"#access\"><div class=\"dstab dstab-off\" style=\"width: 100px; margin: 0px 5px 0px 5px; text-align: center\">Data Access</div></a><div style=\"display: inline; float: left\"> tab here or in the navigation bar near the top of the page</div></td></tr>" << std::endl;
// metadata record
  ofs << "<tr style=\"vertical-align: top\"><td class=\"bold nowrap\">Metadata Record:</td><td><div id=\"meta_record\" style=\"display: inline; float: left\"></div><img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onload=\"getAjaxContent('GET',null,'/cgi-bin/datasets/showMetadata?dsnum=" << dsnum << "','meta_record')\" /></td></tr>" << std::endl;
  ofs << "</table>" << std::endl;
  ofs.close();
}

int main(int argc,char **argv)
{
  std::ofstream ofs;
  std::string sdum,type,output,error;

  if (argc != 2) {
    std::cerr << "usage: dsgen nnn.n" << std::endl;
    exit(1);
  }
  temp_dir.create("/glade2/scratch2/rdadata");
  dsnum=argv[1];
  if (std::regex_search(dsnum,std::regex("^ds"))) {
    dsnum=dsnum.substr(2);
  }
  args.argsString=getUnixArgsString(argc,argv);
  metautils::readConfig("dsgen",user,args.argsString);
  metautils::connectToMetadataServer(server);
  server_root=strutils::token(directives.web_server,".",0);
  struct stat buf;
  if (stat(("/data/web/datasets/ds"+dsnum+"/metadata/dsOverview.xml").c_str(),&buf) == 0) {
    sdum="/data/web/datasets/ds"+dsnum+"/metadata/dsOverview.xml";
  }
  else {
    sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+dsnum+"/metadata/dsOverview.xml",temp_dir.name());
    if (sdum.length() == 0) {
	metautils::logError("dsOverview.xml does not exist for "+dsnum,"dsgen",user,args.argsString);
    }
  }
  if (!xdoc.open(sdum)) {
    metautils::logError("unable to open dsOverview.xml for "+dsnum+"; parse error: '"+xdoc.parse_error()+"'","dsgen",user,args.argsString);
  }
  type=xdoc.element("dsOverview").attribute_value("type");
  generateIndex(type);
  if (type != "internal") {
    generateDescription(type);
  }
  xdoc.close();
  server.disconnect();
  if (type == "work-in-progress") {
    if (hostSync(temp_dir.name()+"/index.html","/__HOST__/web/internal/datasets/ds"+dsnum+"/test_index.html",error) < 0) {
	metautils::logWarning("couldn't sync index.html - hostSync error(s): '"+error+"'","dsgen",user,args.argsString);
    }
    if (hostSync(temp_dir.name()+"/description.html","/__HOST__/web/internal/datasets/ds"+dsnum+"/test_description.html",error) < 0) {
	metautils::logWarning("couldn't sync description.html - hostSync error(s): '"+error+"'","dsgen",user,args.argsString);
    }
  }
  else {
    if (hostSync(temp_dir.name()+"/index.html","/__HOST__/web/datasets/ds"+dsnum+"/index.html",error) < 0) {
	metautils::logWarning("couldn't sync index.html - hostSync error(s): '"+error+"'","dsgen",user,args.argsString);
    }
    if (hostSync(temp_dir.name()+"/description.html","/__HOST__/web/datasets/ds"+dsnum+"/description.html",error) < 0) {
	metautils::logWarning("couldn't sync description.html - hostSync error(s): '"+error+"'","dsgen",user,args.argsString);
    }
  }
}
