#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <list>
#include <regex>
#include <sys/stat.h>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives meta_directives;
metautils::Args meta_args;
std::string myerror="";
std::string mywarning="";

const std::string user=getenv("USER");
std::string old_web_home;
std::string old_name,new_name,new_dsnum;

bool verified_new_file_is_archived(std::string& error)
{
  if (meta_args.dsnum == "999.9") {
    return true;
  }
  MySQL::Server server(meta_directives.database_server,meta_directives.rdadb_username,meta_directives.rdadb_password,"dssdb");
  std::string qstring,column;
  if (std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS"))) {
    column="mssid";
    if (std::regex_search(new_name,std::regex("\\.\\.m\\.\\."))) {
	auto new_name_parts=strutils::split(new_name,"..m..");
	qstring="select hid from htarfile as h left join mssfile as m on m.mssid = h.mssid where m.dsid = 'ds"+new_dsnum+"' and m.mssfile = '"+new_name_parts[0]+"' and m.type = 'P' and m.status = 'P' and h.hfile = '"+new_name_parts[1]+"'";
    }
    else {
	qstring="select mssid from mssfile where dsid = 'ds"+new_dsnum+"' and mssfile = '"+new_name+"' and type = 'P' and status = 'P'";
    }
  }
  else if (std::regex_search(old_name,std::regex("^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    column="wfile";
    auto nname=metautils::relative_web_filename(new_name);
    qstring="select wfile from wfile where dsid = 'ds"+new_dsnum+"' and wfile = '"+nname+"' and type = 'D' and status = 'P'";
  }
  MySQL::LocalQuery query(qstring);
  if (query.submit(server) < 0) {
    metautils::log_error("verified_new_file_is_archived() returned error: "+query.error(),"rcm",user);
  }
  if (query.num_rows() == 0) {
    if (column == "mssid") {
	error="Error: "+new_name+" is not a primary for ds"+new_dsnum;
    }
    else if (column == "wfile") {
	error="Error: "+new_name+" is not an active web file for ds"+new_dsnum;
    }
    server.disconnect();
    return false;
  }
  else {
    server.disconnect();
    return true;
  }
}

void replace_uri(std::string& sline,std::string cmdir,std::string member_name = "")
{
  auto sline2=sline.substr(0,sline.find("\"")+1);
  if (cmdir == "fmd") {
    sline2+="file://MSS:"+new_name;
    if (!member_name.empty()) {
	sline2+="..m.."+member_name;
    }
  }
  else if (cmdir == "wfmd") {
    sline2+="file://web:"+metautils::relative_web_filename(new_name);
  }
  sline2+=sline.substr(sline.find("\" format"));
  sline=sline2;
}

void rewrite_uri_in_cmd_file(std::string db)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::ifstream ifs;
  std::ofstream ofs;
  char line[32768],line2[32768];
  std::string oname=old_name;
  std::string nname=new_name;
  std::list<std::string> file_list;
  std::deque<std::string> sp,sp2;
  std::string sline,sline2,cmdir,db_prefix,ref_file,new_ref_file;
  std::string sdum,error;
  size_t idx;
  TempDir tdir;
  my::map<metautils::StringEntry> unique_stage_table;
  metautils::StringEntry se;
  struct stat buf;
  bool old_is_gzipped=false;

  if (!tdir.create(meta_directives.temp_path)) {
    metautils::log_error("unable to create temporary directory","rcm",user);
  }
  if (std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS"))) {
    cmdir="fmd";
    db_prefix="";
    strutils::replace_all(oname,"/FS/DSS/","");
    strutils::replace_all(oname,"/DSS/","");
    strutils::replace_all(nname,"/FS/DSS/","");
    strutils::replace_all(nname,"/DSS/","");
  }
  else if (std::regex_search(old_name,std::regex("^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    cmdir="wfmd";
    db_prefix="W";
    if (!old_web_home.empty()) {
	strutils::replace_all(oname,"http://rda.ucar.edu","");
	strutils::replace_all(oname,"http://dss.ucar.edu","");
	strutils::replace_all(oname,(old_web_home+"/"),"");
    }
    else {
	oname=metautils::relative_web_filename(oname);
    }
    nname=metautils::relative_web_filename(nname);
  }
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata/"+cmdir,oss,ess) < 0) {
    metautils::log_error("rewrite_uri_in_cmd_file() returned error: unable to create temporary directory tree (1)","rcm",user);
  }
  if (db == "WGrML" && unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata/inv",oss,ess) < 0) {
    metautils::log_error("rewrite_uri_in_cmd_file() returned error: unable to create temporary directory tree (2)","rcm",user);
  }
  strutils::replace_all(oname,"/","%");
  strutils::replace_all(nname,"/","%");
  if (std::regex_search(strutils::to_lower(oname),std::regex("\\.htar$"))) {
    MySQL::Server server(meta_directives.database_server,meta_directives.rdadb_username,meta_directives.rdadb_password,"dssdb");
    query.set("select h.hfile from mssfile as m left join htarfile as h on h.mssid = m.mssid where m.dsid = 'ds"+meta_args.dsnum+"' and mssfile = '"+new_name+"'");
    if (query.submit(server) < 0) {
	metautils::log_error("unable to get HTAR member file names for '"+new_name+"'","rcm",user);
    }
    while (query.fetch_row(row)) {
	file_list.emplace_back(oname+"..m.."+row[0]+"<!>"+nname+"..m.."+row[0]);
    }
  }
  else {
    file_list.emplace_back(oname+"<!>"+nname);
  }
  if (db == (db_prefix+"GrML")) {
    for (const auto& file : file_list) {
	sp=strutils::split(file,"<!>");
	auto grml_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML.gz",tdir.name());
	if (stat(grml_filename.c_str(),&buf) == 0) {
	  system(("gunzip "+grml_filename).c_str());
	  strutils::chop(grml_filename,3);
	  old_is_gzipped=true;
	}
	else {
	  grml_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML",tdir.name());
	}
	ifs.open(grml_filename.c_str());
	if (!ifs.is_open()) {
	  metautils::log_error("unable to open old file for input","rcm",user);
	}
	ofs.open((tdir.name()+"/metadata/"+cmdir+"/"+sp[1]+".GrML").c_str());
	if (!ofs.is_open()) {
	  metautils::log_error("unable to open new file for output","rcm",user);
	}
	ifs.getline(line,32768);
	while (!ifs.eof()) {
	  sline=line;
	  if (strutils::contains(sline,"uri=")) {
	    sp2=strutils::split(sp[0],"..m..");
	    if (sp2.size() == 2) {
		replace_uri(sline,cmdir,sp2[1]);
	    }
	    else {
		replace_uri(sline,cmdir);
	    }
	  }
	  ofs << sline << std::endl;
	  ifs.getline(line,32768);
	}
	ifs.close();
	ofs.close();
	system(("gzip -f "+tdir.name()+"/metadata/"+cmdir+"/"+sp[1]+".GrML").c_str());
	if (sp[0] != sp[1]) {
// remove the old file
	  if (old_is_gzipped) {
 	    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML.gz",meta_directives.rdadata_home,error) < 0) {
		metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+sp[0]+".GrML - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	    }
	  }
	  else {
 	    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML",meta_directives.rdadata_home,error) < 0) {
		metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+sp[0]+".GrML - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	    }
	  }
	}
	if (db == "WGrML") {
// rename the inventory file
	  std::string ext=".gz";
	  auto inv_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/inv/"+sp[0]+".GrML_inv"+ext,tdir.name());
	  if (inv_filename.empty()) {
	    inv_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/inv/"+sp[0]+".GrML_inv",tdir.name());
	    ext="";
	  }
	  if (!inv_filename.empty() && stat(inv_filename.c_str(),&buf) == 0 && unixutils::mysystem2("/bin/mv "+inv_filename+" "+tdir.name()+"/metadata/inv/"+sp[1]+".GrML_inv"+ext,oss,ess) == 0) {
	    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/inv/"+sp[0]+".GrML_inv"+ext,meta_directives.rdadata_home,error) < 0) {
		metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+sp[0]+".GrML_inv"+ext+" - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	    }
	  }
	}
    }
  }
  else if (db == (db_prefix+"ObML")) {
    std::string obml_filename="";
    if (unixutils::exists_on_server("rda-web-prod.ucar.edu","/data/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",meta_directives.rdadata_home)) {
	obml_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",tdir.name());
	system(("gunzip "+obml_filename).c_str());
	strutils::chop(obml_filename,3);
	old_is_gzipped=true;
    }
    else if (unixutils::exists_on_server("rda-web-prod.ucar.edu","/data/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",meta_directives.rdadata_home)) {
	obml_filename=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",tdir.name());
    }
    if (!obml_filename.empty()) {
	ifs.open(obml_filename.c_str());
    }
    if (!ifs.is_open()) {
	metautils::log_error("unable to open old file for input","rcm",user);
    }
    ofs.open((tdir.name()+"/metadata/"+cmdir+"/"+nname+".ObML").c_str());
    if (!ofs.is_open()) {
	metautils::log_error("unable to open new file for output","rcm",user);
    }
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sline=line;
	if (strutils::contains(sline,"uri=")) {
	  replace_uri(sline,cmdir);
	}
	else if (oname != nname && strutils::contains(sline,"ref=")) {
	  ref_file=sline.substr(sline.find("ref=")+5);
	  ref_file=ref_file.substr(0,ref_file.find("\""));
	  new_ref_file=strutils::substitute(ref_file,oname,nname);
	  if (unixutils::exists_on_server("rda-web-prod.ucar.edu","/data/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+ref_file,meta_directives.rdadata_home)) {
	    std::ifstream ifs2(unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+ref_file,tdir.name()).c_str());
	    if (ifs2.is_open()) {
		std::ofstream ofs2((tdir.name()+"/metadata/"+cmdir+"/"+new_ref_file).c_str());
		if (!ofs2.is_open()) {
		  metautils::log_error("rerwite_URI_in_CMD_file() could not open output file for a reference","rcm",user);
		}
		ifs2.getline(line2,32768);
		while (!ifs2.eof()) {
		  sline2=line2;
		  if (strutils::contains(sline2,"parent=")) {
		    sline2=sline2.substr(0,sline2.find("\"")+1)+nname+".ObML"+sline2.substr(sline2.find("\" group"));
		  }
		  ofs2 << sline2 << std::endl;
		  ifs2.getline(line2,32768);
		}
		ifs2.close();
		ifs2.clear();
		ofs2.close();
// remove the old ref file
		if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+ref_file,meta_directives.rdadata_home,error) < 0) {
		  metautils::log_warning("rewrite_uri_in_cmd_file() could not remove old reference file '"+ref_file+"'","rcm",user);
		}
		strutils::replace_all(sline,oname,nname);
	    }
	    else {
		metautils::log_error("rewrite_uri_in_cmd_file() could not open reference file '"+ref_file+"'","rcm",user);
	    }
	  }
	}
	ofs << sline << std::endl;
	ifs.getline(line,32768);
    }
    ifs.close();
    ofs.close();
    system(("gzip -f "+tdir.name()+"/metadata/"+cmdir+"/"+nname+".ObML").c_str());
    if (oname != nname) {
// remove the old file
 	if (old_is_gzipped) {
 	  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",meta_directives.rdadata_home,error) < 0) {
	    metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+oname+".ObML - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	  }
	}
	else {
 	  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",meta_directives.rdadata_home,error) < 0) {
	    metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+oname+".ObML - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	  }
	}
    }
  }
  else if (db == (db_prefix+"FixML")) {
    ifs.open(unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".FixML",tdir.name()).c_str());
    if (!ifs.is_open()) {
	metautils::log_error("unable to open old file for input","rcm",user);
    }
    ofs.open((tdir.name()+"/metadata/"+cmdir+"/"+nname+".FixML").c_str());
    if (!ofs.is_open()) {
	metautils::log_error("unable to open new file for output","rcm",user);
    }
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sline=line;
	if (strutils::contains(sline,"uri=")) {
	  replace_uri(sline,cmdir);
	}
	else if (oname != nname && strutils::contains(sline,"classification stage=")) {
	  se.key=sline.substr(sline.find("classification stage=")+22);
	  if (!se.key.empty() && (idx=se.key.find("\"")) != std::string::npos) {
	    se.key=se.key.substr(0,idx);
	    if (!unique_stage_table.found(se.key,se)) {
		unique_stage_table.insert(se);
	    }
	  }
	}
	ofs << sline << std::endl;
	ifs.getline(line,32768);
    }
    ifs.close();
    ofs.close();
    if (oname != nname) {
// remove the old file
 	if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmdir+"/"+oname+".FixML",meta_directives.rdadata_home,error) < 0) {
	  metautils::log_warning("rewrite_uri_in_cmd_file() could not remove "+oname+".FixML - unixutils::rdadata_unsync error(s): '"+error+"'","rcm",user);
	}
    }
  }
  else {
    metautils::log_error("rewrite_uri_in_cmd_file() returned error: unable to rename files in database "+db,"rcm",user);
  }
// sync all of the new files
  if (unixutils::rdadata_sync(tdir.name(),"metadata/","/data/web/datasets/ds"+new_dsnum,meta_directives.rdadata_home,error) < 0) {
    metautils::log_error("rewrite_uri_in_cmd_file() could not sync new file(s) - rdadata_sync error(s): '"+error+"'","rcm",user);
  }
}

bool renamed_cmd()
{
  std::list<std::string> databases;
  std::string dsnum2=strutils::substitute(meta_args.dsnum,".","");
  std::string new_dsnum2=strutils::substitute(new_dsnum,".","");
  std::string filetable,dfiletable,column,dcolumn,cmd_dir,error;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  std::string oname,nname,scm_flag,sdum;
  std::deque<std::string> sp;

  MySQL::Server server(meta_directives.database_server,meta_directives.metadb_username,meta_directives.metadb_password,"");
  MySQL::Server server_d(meta_directives.database_server,meta_directives.rdadb_username,meta_directives.rdadb_password,"dssdb");
  if (std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS"))) {
    filetable="_primaries";
    column="mssID";
    dcolumn="mssfile";
    oname=old_name;
    nname=new_name;
    cmd_dir="fmd";
    scm_flag="-f";
  }
  else if (std::regex_search(old_name,std::regex("^http(s){0,1}://(rda|dss)\\.ucar\\.edu/"))) {
    filetable="_webfiles";
    column="webID";
    dcolumn="wfile";
    if (!old_web_home.empty()) {
	oname=strutils::substitute(old_name,"http://rda.ucar.edu","");
	strutils::replace_all(oname,"http://dss.ucar.edu","");
	strutils::replace_all(oname,old_web_home+"/","");
    }
    else {
	oname=metautils::relative_web_filename(old_name);
    }
    if (meta_args.dsnum != new_dsnum) {
	sdum=meta_args.dsnum;
	meta_args.dsnum=new_dsnum;
	nname=metautils::relative_web_filename(new_name);
	meta_args.dsnum=sdum;
    }
    else {
	nname=metautils::relative_web_filename(new_name);
    }
    cmd_dir="wfmd";
    scm_flag="-wf";
  }
  databases=server.db_names();
  for (const auto& db : databases) {
    auto table_names=MySQL::table_names(server,db,"ds"+dsnum2+filetable,error);
    for (const auto& table : table_names) {
	if (std::regex_search(strutils::to_lower(oname),std::regex("\\.htar$"))) {
	  query.set("code,"+column,db+"."+table,column+" like '"+oname+"..m..%'");
	}
	else {
	  query.set("code",db+"."+table,column+" = '"+oname+"'");
	}
	if (query.submit(server) < 0) {
	  metautils::log_error("renamed_cmd() returned error: "+query.error(),"rcm",user);
	}
	if (query.num_rows() > 0) {
	  if (std::regex_search(strutils::to_lower(oname),std::regex("\\.htar$"))) {
	    query2.set("code",db+"."+strutils::substitute(table,dsnum2,new_dsnum2),"binary "+column+" like '"+nname+"..m..%'");
	  }
	  else {
	    query2.set("code",db+"."+strutils::substitute(table,dsnum2,new_dsnum2),"binary "+column+" = '"+nname+"'");
	  }
	  if (query2.submit(server) < 0) {
	    metautils::log_error("renamed_cmd() returned error: "+query2.error(),"rcm",user);
	  }
	  if (query2.num_rows() > 0) {
	    if (!old_web_home.empty() && oname == nname) {
		rewrite_uri_in_cmd_file(db);
		exit(0);
	    }
	    else {
		metautils::log_error("renamed_cmd() returned error: "+new_name+" is already in the content metadata database","rcm",user);
	    }
	  }
	  rewrite_uri_in_cmd_file(db);
	  if (new_dsnum == meta_args.dsnum) {
	    while (query.fetch_row(row)) {
		if (query.num_rows() == 1) {
		  if (server.update(db+"."+table,column+" = '"+nname+"'","code = "+row[0]) < 0) {
		    metautils::log_error("renamed_cmd() returned error: "+server.error(),"rcm",user);
		  }
		}
		else {
		  sp=strutils::split(row[1],"..m..");
		  if (server.update(db+"."+table,column+" = '"+nname+"..m.."+sp[1]+"'","code = "+row[0]) < 0) {
		    metautils::log_error("renamed_cmd() returned error: "+server.error(),"rcm",user);
		  }
		}
	    }
	  }
	  else {
	    std::stringstream oss,ess;
	    if (unixutils::mysystem2(meta_directives.local_root+"/bin/dcm -d "+meta_args.dsnum+" "+old_name,oss,ess) < 0) {
		std::cerr << ess.str() << std::endl;
	    }
	    if (query.num_rows() == 1) {
		if (unixutils::mysystem2(meta_directives.local_root+"/bin/scm -d "+new_dsnum+" "+scm_flag+" "+strutils::substitute(strutils::substitute(strutils::substitute(strutils::substitute(nname,"/FS/DSS/",""),"/DSS/",""),"/","%")+"."+db,"W"+db,db),oss,ess) < 0) {
		  std::cerr << ess.str() << std::endl;
		}
		if (!std::regex_search(nname,std::regex("^/FS/DSS")) && db == "WGrML") {
// insert the inventory into the new dataset tables
		  unixutils::mysystem2(meta_directives.local_root+"/bin/iinv -d "+new_dsnum+" -f "+strutils::substitute(nname,"/","%")+".GrML_inv",oss,ess);
		  server.update("WGrML.ds"+strutils::substitute(new_dsnum,".","")+"_webfiles","inv = 'Y'","webID = '"+nname+"'");
		}
	    }
	    else {
		while (query.fetch_row(row)) {
		  sp=strutils::split(row[1],"..m..");
		  if (unixutils::mysystem2(meta_directives.local_root+"/bin/scm -d "+new_dsnum+" "+scm_flag+" "+strutils::substitute(strutils::substitute(strutils::substitute(strutils::substitute(nname,"/FS/DSS/",""),"/DSS/",""),"/","%")+"."+db,"W"+db,db)+"..m.."+sp[1],oss,ess) < 0) {
		    std::cerr << ess.str() << std::endl;
		  }
		}
	    }
	  }
	  sdum=db;
	  strutils::replace_all(sdum,"ML","");
	  if (sdum[0] == 'W') {
	    sdum=sdum.substr(1);
	  }
	  if (server_d.update(dcolumn,"meta_link = '"+sdum+"'","dsid = 'ds"+meta_args.dsnum+"' and "+dcolumn+" = '"+nname+"'") < 0) {
	    metautils::log_warning("renamed_cmd() returned warning: "+server_d.error(),"rcm",user);
	  }
	  server.disconnect();
	  server_d.disconnect();
	  return true;
	}
    }
  }
  server.disconnect();
  server_d.disconnect();
  return false;
}

int main(int argc,char **argv)
{
  std::deque<std::string> sp;
  size_t n;
  std::string error,cmd_dir;
  bool no_cache=false;

  if (argc < 5 && argc != 2) {
    std::cerr << "usage: rcm [-C] -d [ds]nnn.n [-nd [ds]nnn.n] [-o old_webhome] old_name new_name" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d nnn.n        nnn.n is the dataset number where the original file resides" << std::endl;
    std::cerr << "old_name        old data file name, beginning with '/FS/DSS', '/DSS', or" << std::endl;
    std::cerr << "                'https://rda.ucar.edu'" << std::endl;
    std::cerr << "new_name        new data file name, beginning with '/FS/DSS', '/DSS'," << std::endl;
    std::cerr << "                'https://rda.ucar.edu', or as for the -WF option of dsarch" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-C              no file list cache created (to save time)" << std::endl;
    std::cerr << "-nd nnn.n       new dataset number, if different from old dataset number" << std::endl;
    std::cerr << "-o old_webhome  use this option if the old web home is different from the new" << std::endl;
    std::cerr << "                one" << std::endl;
    exit(1);
  }
  meta_args.args_string=unixutils::unix_args_string(argc,argv,'%');
  metautils::read_config("rcm",user,meta_args.args_string);
  sp=strutils::split(meta_args.args_string,"%");
  for (n=0; n < sp.size()-2; n++) {
    if (sp[n] == "-C") {
	no_cache=true;
    }
    else if (sp[n] == "-d") {
	meta_args.dsnum=sp[++n];
    }
    else if (sp[n] == "-nd") {
	new_dsnum=sp[++n];
    }
    else if (sp[n] == "-o") {
	old_web_home=sp[++n];
	strutils::replace_all(old_web_home,"/huron/ftp","");
	if (std::regex_search(old_web_home,std::regex("/$"))) {
	  strutils::chop(old_web_home);
	}
    }
  }
  if (std::regex_search(meta_args.dsnum,std::regex("^ds"))) {
    meta_args.dsnum=meta_args.dsnum.substr(2);
  }
  if (new_dsnum.empty()) {
    new_dsnum=meta_args.dsnum;
  }
  else if (std::regex_search(new_dsnum,std::regex("^ds"))) {
    new_dsnum=new_dsnum.substr(2);
  }
  old_name=sp[sp.size()-2];
  new_name=sp[sp.size()-1];
/*
  if (!std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS")) && !std::regex_search(old_name,std::regex("^http(s){0,1}://rda\\.ucar\\.edu/"))) {
    std::cerr << "Error: old_name must begin with '/FS/DSS', '/DSS', or 'https://rda.ucar.edu'" << std::endl;
    exit(1);
  }
*/
  if (!std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS"))) {
    old_name="https://rda.ucar.edu"+meta_directives.data_root_alias+"/ds"+meta_args.dsnum+"/"+old_name;
  }
  if (std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS"))) {
    cmd_dir="fmd";
  }
  else if (std::regex_search(old_name,std::regex("^http(s){0,1}://rda\\.ucar\\.edu/"))) {
    cmd_dir="wfmd";
  }
  if (!unixutils::exists_on_server("rda-web-prod.ucar.edu","/data/web/datasets/ds"+meta_args.dsnum+"/metadata/"+cmd_dir,meta_directives.rdadata_home)) {
    std::cerr << "Error: metadata directory not found for ds"+meta_args.dsnum << std::endl;
    exit(1);
  }
/*
  if (!std::regex_search(new_name,std::regex("^(/FS){0,1}/DSS")) && !std::regex_search(new_name,std::regex("^https://rda.ucar.edu/"))) {
    std::cerr << "Error: new_name must begin with '/FS/DSS', '/DSS', or 'https://rda.ucar.edu'" << std::endl;
    exit(1);
  }
*/
  if (!std::regex_search(new_name,std::regex("^(/FS){0,1}/DSS"))) {
    new_name="https://rda.ucar.edu"+meta_directives.data_root_alias+"/ds"+meta_args.dsnum+"/"+new_name;
  }
  if (new_name == old_name && new_dsnum == meta_args.dsnum) {
    std::cerr << "Error: new_name must be different from old_name" << std::endl;
    exit(1);
  }
  if ((std::regex_search(old_name,std::regex("^(/FS){0,1}/DSS")) && !std::regex_search(new_name,std::regex("^(/FS){0,1}/DSS"))) || (std::regex_search(old_name,std::regex("^http(s){0,1}://(rda|dss)\\.ucar\\.edu/")) && !std::regex_search(new_name,std::regex("^https://rda\\.ucar\\.edu/")))) {
    std::cerr << "Error: you can only rename content metadata for MSS files OR for web files, but not from one to the other" << std::endl;
    exit(1);
  }
  meta_args.path=old_name;
  meta_args.filename=new_name;
  metautils::cmd_register("rcm",user);
  if (!verified_new_file_is_archived(error))
    metautils::log_error("rcm main returned error: "+error,"rcm",user);
  else {
    if (!renamed_cmd()) {
	metautils::log_error("rcm main retured error: no content metadata were found for "+old_name,"rcm",user);
    }
    if (!no_cache) {
	if (std::regex_search(new_name,std::regex("^(/FS){0,1}/DSS"))) {
	  summarizeMetadata::create_file_list_cache("MSS","rcm",user,"-d:"+meta_args.dsnum+":"+old_name+":"+new_name);
	}
	else {
	  summarizeMetadata::create_file_list_cache("Web","rcm",user,"-d:"+meta_args.dsnum+":"+old_name+":"+new_name);
	}
    }
    return 0;
  }
}
