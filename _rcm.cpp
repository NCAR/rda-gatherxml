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

metautils::Directives directives;
metautils::Args args;
const std::string user=getenv("USER");
std::string old_web_home;
std::string old_name,new_name,new_dsnum;

bool verifiedNewFileIsArchived(std::string& error)
{
  MySQL::Server server;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string qstring,column,nname;
  std::deque<std::string> sp;

  metautils::connectToRDAServer(server);
  if (strutils::has_beginning(old_name,"/FS/DSS/") || strutils::has_beginning(old_name,"/DSS/")) {
    column="mssid";
    if (std::regex_search(new_name,std::regex("\\.\\.m\\.\\."))) {
	sp=strutils::split(new_name,"..m..");
	qstring="select hid from htarfile as h left join mssfile as m on m.mssid = h.mssid where m.dsid = 'ds"+new_dsnum+"' and m.mssfile = '"+sp[0]+"' and m.type = 'P' and m.status = 'P' and h.hfile = '"+sp[1]+"'";
    }
    else {
	qstring="select mssid from mssfile where dsid = 'ds"+new_dsnum+"' and mssfile = '"+new_name+"' and type = 'P' and status = 'P'";
    }
  }
  else if (strutils::has_beginning(old_name,"http://rda.ucar.edu/") || strutils::has_beginning(old_name,"http://dss.ucar.edu/")) {
    column="wfile";
    nname=metautils::getRelativeWebFilename(new_name);
    qstring="select wfile from wfile where dsid = 'ds"+new_dsnum+"' and wfile = '"+nname+"' and type = 'D' and status = 'P'";
  }
  query.set(qstring);
  if (query.submit(server) < 0)
    metautils::logError("verifiedNewFileIsArchived returned error: "+query.error(),"rcm",user,args.argsString);
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

void replaceURI(std::string& sline,std::string cmdir,std::string member_name = "")
{
  auto sline2=sline.substr(0,sline.find("\"")+1);
  if (cmdir == "fmd") {
    sline2+="file://MSS:";
  }
  sline2+=new_name;
  if (member_name.length() > 0) {
    sline2+="..m.."+member_name;
  }
  sline2+=sline.substr(sline.find("\" format"));
  sline=sline2;
}

void rewriteURIinCMDFile(std::string db)
{
  MySQL::Server server;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::ifstream ifs,ifs2;
  std::ofstream ofs,ofs2;
  char line[32768],line2[32768];
  std::string oname=old_name;
  std::string nname=new_name;
  std::list<std::string> file_list;
  std::deque<std::string> sp,sp2;
  std::string sline,sline2,cmdir,db_prefix,ref_file,new_ref_file;
  std::string sdum,error;
  size_t idx;
  TempDir tdir;
  TempFile tfile("/glade2/scratch2/rdadata",""),*tfile2;
  my::map<metautils::StringEntry> unique_stage_table;
  metautils::StringEntry se;
  struct stat buf;
  bool old_is_gzipped=false;

  if (!tdir.create("/glade2/scratch2/rdadata")) {
    metautils::logError("unable to create temporary directory in /glade2/scratch2/rdadata","rcm",user,args.argsString);
  }
  if (strutils::has_beginning(old_name,"/FS/DSS/") || strutils::has_beginning(old_name,"/DSS/")) {
    cmdir="fmd";
    db_prefix="";
    strutils::replace_all(oname,"/FS/DSS/","");
    strutils::replace_all(oname,"/DSS/","");
    strutils::replace_all(nname,"/FS/DSS/","");
    strutils::replace_all(nname,"/DSS/","");
  }
  else if (strutils::has_beginning(old_name,"http://rda.ucar.edu/") || strutils::has_beginning(old_name,"http://dss.ucar.edu/")) {
    cmdir="wfmd";
    db_prefix="W";
    if (old_web_home.length() > 0) {
	strutils::replace_all(oname,"http://rda.ucar.edu","");
	strutils::replace_all(oname,"http://dss.ucar.edu","");
	strutils::replace_all(oname,(old_web_home+"/"),"");
    }
    else {
	oname=metautils::getRelativeWebFilename(oname);
    }
    nname=metautils::getRelativeWebFilename(nname);
  }
  strutils::replace_all(oname,"/","%");
  strutils::replace_all(nname,"/","%");
  if (strutils::has_ending(strutils::to_lower(oname),".htar")) {
    metautils::connectToRDAServer(server);
    query.set("select h.hfile from mssfile as m left join htarfile as h on h.mssid = m.mssid where m.dsid = 'ds"+args.dsnum+"' and mssfile = '"+new_name+"'");
    if (query.submit(server) < 0) {
	metautils::logError("unable to get HTAR member file names for '"+new_name+"'","rcm",user,args.argsString);
    }
    while (query.fetch_row(row)) {
	file_list.push_back(oname+"..m.."+row[0]+"<!>"+nname+"..m.."+row[0]);
    }
  }
  else {
    file_list.push_back(oname+"<!>"+nname);
  }
  if (db == (db_prefix+"GrML")) {
    for (const auto& file : file_list) {
	sp=strutils::split(file,"<!>");
	sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML.gz",tdir.name());
	if (stat(sdum.c_str(),&buf) == 0) {
	  system(("gunzip "+sdum).c_str());
	  strutils::chop(sdum,3);
	  old_is_gzipped=true;
	}
	else {
	  sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML",tdir.name());
	}
	ifs.open(sdum.c_str());
	if (!ifs.is_open()) {
	  metautils::logError("unable to open old file for input","rcm",user,args.argsString);
	}
	ofs.open(tfile.name().c_str());
	if (!ofs.is_open()) {
	  metautils::logError("unable to open new file for output","rcm",user,args.argsString);
	}
	ifs.getline(line,32768);
	while (!ifs.eof()) {
	  sline=line;
	  if (strutils::contains(sline,"uri=")) {
	    sp2=strutils::split(sp[0],"..m..");
	    if (sp2.size() == 2) {
		replaceURI(sline,cmdir,sp2[1]);
	    }
	    else {
		replaceURI(sline,cmdir);
	    }
	  }
	  ofs << sline << std::endl;
	  ifs.getline(line,32768);
	}
	ifs.close();
	ofs.close();
	system(("gzip -f "+tfile.name()).c_str());
	if (sp[0] != sp[1]) {
// remove the old file
	  if (old_is_gzipped) {
 	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML.gz",error) < 0) {
		metautils::logWarning("rewriteURIinCMDFile could not remove "+sp[0]+".GrML - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	    }
	  }
	  else {
 	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+sp[0]+".GrML",error) < 0) {
		metautils::logWarning("rewriteURIinCMDFile could not remove "+sp[0]+".GrML - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	    }
	  }
	}
	if (hostSync(tfile.name()+".gz","/__HOST__/web/datasets/ds"+new_dsnum+"/metadata/"+cmdir+"/"+sp[1]+".GrML.gz",error) < 0) {
	  metautils::logError("rewriteURIinCMDFile could not sync "+sp[1]+".GrML - hostSync error(s): '"+error+"'","rcm",user,args.argsString);
	}
	rename((tfile.name()+".gz").c_str(),tfile.name().c_str());
	if (db == "WGrML") {
// rename the inventory file
	  sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/inv/"+sp[0]+".GrML_inv",tdir.name());
	  if (sdum.length() > 0 && stat(sdum.c_str(),&buf) == 0) {
	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/inv/"+sp[0]+".GrML_inv",error) < 0) {
		metautils::logWarning("rewriteURIinCMDFile could not remove "+sp[0]+".GrML_inv - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	    }
	    if (hostSync(sdum,"/__HOST__/web/datasets/ds"+new_dsnum+"/metadata/inv/"+sp[1]+".GrML_inv",error) < 0) {
		metautils::logError("rewriteURIinCMDFile could not sync "+sp[1]+".GrML_inv - hostSync error(s): '"+error+"'","rcm",user,args.argsString);
	    }
	  }
	}
    }
  }
  else if (db == (db_prefix+"ObML")) {
    sdum="";
    if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",buf)) {
	sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",tdir.name());
	system(("gunzip "+sdum).c_str());
	strutils::chop(sdum,3);
	old_is_gzipped=true;
    }
    else if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",buf)) {
	sdum=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",tdir.name());
    }
    if (sdum.length() > 0) {
	ifs.open(sdum.c_str());
    }
    if (!ifs.is_open()) {
	metautils::logError("unable to open old file for input","rcm",user,args.argsString);
    }
    ofs.open(tfile.name().c_str());
    if (!ofs.is_open()) {
	metautils::logError("unable to open new file for output","rcm",user,args.argsString);
    }
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sline=line;
	if (strutils::contains(sline,"uri=")) {
	  replaceURI(sline,cmdir);
	}
	else if (oname != nname && strutils::contains(sline,"ref=")) {
	  ref_file=sline.substr(sline.find("ref=")+5);
	  ref_file=ref_file.substr(0,ref_file.find("\""));
	  new_ref_file=strutils::substitute(ref_file,oname,nname);
	  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+ref_file,buf)) {
	    ifs2.open(getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+ref_file,tdir.name()).c_str());
	  }
	  if (ifs2.is_open()) {
	    tfile2=new TempFile("/glade2/scratch2/rdadata","");
	    ofs2.open((tfile2->name()).c_str());
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
// remove old ref file from and put new ref file to castle
	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+ref_file,error) < 0) {
		metautils::logWarning("rewriteURIinCMDFile could not remove old reference file '"+ref_file+"'","rcm",user,args.argsString);
	    }
	    if (hostSync(tfile2->name(),"/__HOST__/web/datasets/ds"+new_dsnum+"/metadata/"+cmdir+"/"+new_ref_file,error) < 0) {
		metautils::logError("rewriteURIinCMDFile could not sync new reference file '"+new_ref_file+"' - hostSync error(s): '"+error+"'","rcm",user,args.argsString);
	    }
	    strutils::replace_all(sline,oname,nname);
	    delete tfile2;
	  }
	  else
	    metautils::logError("rewriteURIinCMDFile could not open reference file '"+ref_file+"'","rcm",user,args.argsString);
	}
	ofs << sline << std::endl;
	ifs.getline(line,32768);
    }
    ifs.close();
    ofs.close();
    system(("gzip -f "+tfile.name()).c_str());
    if (oname != nname) {
// remove the old file
 	if (old_is_gzipped) {
 	  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML.gz",error) < 0) {
	    metautils::logWarning("rewriteURIinCMDFile could not remove "+oname+".ObML - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	  }
	}
	else {
 	  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".ObML",error) < 0) {
	    metautils::logWarning("rewriteURIinCMDFile could not remove "+oname+".ObML - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	  }
	}
    }
    if (hostSync(tfile.name()+".gz","/__HOST__/web/datasets/ds"+new_dsnum+"/metadata/"+cmdir+"/"+nname+".ObML.gz",error) < 0) {
	metautils::logError("rewriteURIinCMDFile could not sync "+nname+".ObML - hostSync error(s): '"+error+"'","rcm",user,args.argsString);
    }
    rename((tfile.name()+".gz").c_str(),tfile.name().c_str());
  }
  else if (db == (db_prefix+"FixML")) {
    ifs.open(getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".FixML",tdir.name()).c_str());
    if (!ifs.is_open()) {
	metautils::logError("unable to open old file for input","rcm",user,args.argsString);
    }
    ofs.open(tfile.name().c_str());
    if (!ofs.is_open()) {
	metautils::logError("unable to open new file for output","rcm",user,args.argsString);
    }
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	sline=line;
	if (strutils::contains(sline,"uri=")) {
	  replaceURI(sline,cmdir);
	}
	else if (oname != nname && strutils::contains(sline,"classification stage=")) {
	  se.key=sline.substr(sline.find("classification stage=")+22);
	  if (se.key.length() > 0 && (idx=se.key.find("\"")) != std::string::npos) {
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
 	if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+cmdir+"/"+oname+".FixML",error) < 0) {
	  metautils::logWarning("rewriteURIinCMDFile could not remove "+oname+".FixML - hostUnsync error(s): '"+error+"'","rcm",user,args.argsString);
	}
    }
    if (hostSync(tfile.name(),"/__HOST__/web/datasets/ds"+new_dsnum+"/metadata/"+cmdir+"/"+nname+".FixML",error) < 0) {
	metautils::logError("rewriteURIinCMDFile could not sync "+nname+".FixML - hostSync error(s): '"+error+"'","rcm",user,args.argsString);
    }
  }
  else
    metautils::logError("rewriteURIinCMDFile returned error: unable to rename files in database "+db,"rcm",user,args.argsString);
}

bool renamedCMD()
{
  MySQL::Server server,server_d;
  std::list<std::string> databases,tableNames;
  std::string dsnum2=strutils::substitute(args.dsnum,".","");
  std::string new_dsnum2=strutils::substitute(new_dsnum,".","");
  std::string filetable,dfiletable,column,dcolumn,cmd_dir,error;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  std::string oname,nname,scm_flag,sdum;
  std::deque<std::string> sp;

  metautils::connectToMetadataServer(server);
  metautils::connectToRDAServer(server_d);
  if (strutils::has_beginning(old_name,"/FS/DSS/") || strutils::has_beginning(old_name,"/DSS/")) {
    filetable="_primaries";
    column="mssID";
    dcolumn="mssfile";
    oname=old_name;
    nname=new_name;
    cmd_dir="fmd";
    scm_flag="-f";
  }
  else if (strutils::has_beginning(old_name,"http://rda.ucar.edu/") || strutils::has_beginning(old_name,"http://dss.ucar.edu/")) {
    filetable="_webfiles";
    column="webID";
    dcolumn="wfile";
    if (old_web_home.length() > 0) {
	oname=strutils::substitute(old_name,"http://rda.ucar.edu","");
	strutils::replace_all(oname,"http://dss.ucar.edu","");
	strutils::replace_all(oname,old_web_home+"/","");
    }
    else {
	oname=metautils::getRelativeWebFilename(old_name);
    }
    if (args.dsnum != new_dsnum) {
	sdum=args.dsnum;
	args.dsnum=new_dsnum;
	nname=metautils::getRelativeWebFilename(new_name);
	args.dsnum=sdum;
    }
    else {
	nname=metautils::getRelativeWebFilename(new_name);
    }
    cmd_dir="wfmd";
    scm_flag="-wf";
  }
  databases=server.db_names();
  for (const auto& db : databases) {
    tableNames=MySQL::table_names(server,db,"ds"+dsnum2+filetable,error);
    for (const auto& table : tableNames) {
	if (strutils::has_ending(strutils::to_lower(oname),".htar")) {
	  query.set("code,"+column,db+"."+table,column+" like '"+oname+"..m..%'");
	}
	else {
	  query.set("code",db+"."+table,column+" = '"+oname+"'");
	}
	if (query.submit(server) < 0) {
	  metautils::logError("renamedCMD returned error: "+query.error(),"rcm",user,args.argsString);
	}
	if (query.num_rows() > 0) {
	  if (strutils::has_ending(strutils::to_lower(oname),".htar")) {
	    query2.set("code",db+"."+strutils::substitute(table,dsnum2,new_dsnum2),"binary "+column+" like '"+nname+"..m..%'");
	  }
	  else {
	    query2.set("code",db+"."+strutils::substitute(table,dsnum2,new_dsnum2),"binary "+column+" = '"+nname+"'");
	  }
	  if (query2.submit(server) < 0) {
	    metautils::logError("renamedCMD returned error: "+query2.error(),"rcm",user,args.argsString);
	  }
	  if (query2.num_rows() > 0) {
	    if (old_web_home.length() > 0 && oname == nname) {
		rewriteURIinCMDFile(db);
		exit(0);
	    }
	    else {
		metautils::logError("renamedCMD returned error: "+new_name+" is already in the content metadata database","rcm",user,args.argsString);
	    }
	  }
	  rewriteURIinCMDFile(db);
	  if (new_dsnum == args.dsnum) {
	    while (query.fetch_row(row)) {
		if (query.num_rows() == 1) {
		  if (server.update(db+"."+table,column+" = '"+nname+"'","code = "+row[0]) < 0) {
		    metautils::logError("renamedCMD returned error: "+server.error(),"rcm",user,args.argsString);
		  }
		}
		else {
		  sp=strutils::split(row[1],"..m..");
		  if (server.update(db+"."+table,column+" = '"+nname+"..m.."+sp[1]+"'","code = "+row[0]) < 0) {
		    metautils::logError("renamedCMD returned error: "+server.error(),"rcm",user,args.argsString);
		  }
		}
	    }
	  }
	  else {
	    std::stringstream oss,ess;
	    if (mysystem2(directives.localRoot+"/bin/dcm -d "+args.dsnum+" "+old_name,oss,ess) < 0) {
		std::cerr << ess.str() << std::endl;
	    }
	    if (query.num_rows() == 1) {
		if (mysystem2(directives.localRoot+"/bin/scm -d "+new_dsnum+" "+scm_flag+" "+strutils::substitute(strutils::substitute(strutils::substitute(strutils::substitute(nname,"/FS/DSS/",""),"/DSS/",""),"/","%")+"."+db,"W"+db,db),oss,ess) < 0) {
		  std::cerr << ess.str() << std::endl;
		}
		if (!strutils::has_beginning(nname,"/FS/DSS/") && !strutils::has_beginning(nname,"/DSS/") && db == "WGrML") {
// insert the inventory into the new dataset tables
		  mysystem2(directives.localRoot+"/bin/iinv -d "+new_dsnum+" -f "+strutils::substitute(nname,"/","%")+".GrML_inv",oss,ess);
		  server.update("WGrML.ds"+strutils::substitute(new_dsnum,".","")+"_webfiles","inv = 'Y'","webID = '"+nname+"'");
		}
	    }
	    else {
		while (query.fetch_row(row)) {
		  sp=strutils::split(row[1],"..m..");
		  if (mysystem2(directives.localRoot+"/bin/scm -d "+new_dsnum+" "+scm_flag+" "+strutils::substitute(strutils::substitute(strutils::substitute(strutils::substitute(nname,"/FS/DSS/",""),"/DSS/",""),"/","%")+"."+db,"W"+db,db)+"..m.."+sp[1],oss,ess) < 0) {
		    std::cerr << ess.str() << std::endl;
		  }
		}
	    }
	  }
	  sdum=db;
	  strutils::replace_all(sdum,"ML","");
	  if (strutils::has_beginning(sdum,"W")) {
	    sdum=sdum.substr(1);
	  }
	  if (server_d.update(dcolumn,"meta_link = '"+sdum+"'","dsid = 'ds"+args.dsnum+"' and "+dcolumn+" = '"+nname+"'") < 0) {
	    metautils::logWarning("renamedCMD returned warning: "+server_d.error(),"rcm",user,args.argsString);
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
  struct stat buf;
  bool no_cache=false;

  if (argc < 5 && argc != 2) {
    std::cerr << "usage: rcm [-C] -d [ds]nnn.n [-nd [ds]nnn.n] [-o old_webhome] old_name new_name" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d nnn.n        nnn.n is the dataset number where the original file resides" << std::endl;
    std::cerr << "old_name        old data file name, beginning with '/FS/DSS', '/DSS', or" << std::endl;
    std::cerr << "                'http://rda.ucar.edu'" << std::endl;
    std::cerr << "new_name        new data file name, beginning with '/FS/DSS', '/DSS' or" << std::endl;
    std::cerr << "                'http://rda.ucar.edu'" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-C              no file list cache created (to save time)" << std::endl;
    std::cerr << "-nd nnn.n       new dataset number, if different from old dataset number" << std::endl;
    std::cerr << "-o old_webhome  use this option if the old web home is different from the new" << std::endl;
    std::cerr << "                one" << std::endl;
    exit(1);
  }
  args.argsString=getUnixArgsString(argc,argv,'%');
  metautils::readConfig("rcm",user,args.argsString);
  sp=strutils::split(args.argsString,"%");
  for (n=0; n < sp.size()-2; n++) {
    if (sp[n] == "-C")
	no_cache=true;
    else if (sp[n] == "-d")
	args.dsnum=sp[++n];
    else if (sp[n] == "-nd")
	new_dsnum=sp[++n];
    else if (sp[n] == "-o") {
	old_web_home=sp[++n];
	strutils::replace_all(old_web_home,"/huron/ftp","");
	if (strutils::has_ending(old_web_home,"/"))
	  strutils::chop(old_web_home);
    }
  }
  if (strutils::has_beginning(args.dsnum,"ds"))
    args.dsnum=args.dsnum.substr(2);
  if (new_dsnum.length() == 0)
    new_dsnum=args.dsnum;
  else if (strutils::has_beginning(new_dsnum,"ds"))
    new_dsnum=new_dsnum.substr(2);
  old_name=sp[sp.size()-2];
  new_name=sp[sp.size()-1];
  if (!strutils::has_beginning(old_name,"/FS/DSS/") && !strutils::has_beginning(old_name,"/DSS/") && !strutils::has_beginning(old_name,"http://rda.ucar.edu/") && !strutils::has_beginning(old_name,"http://dss.ucar.edu/")) {
    std::cerr << "Error: old_name must begin with '/FS/DSS', '/DSS', 'http://rda.ucar.edu', or 'http://dss.ucar.edu'" << std::endl;
    exit(1);
  }
  if (strutils::has_beginning(old_name,"/FS/DSS/") || strutils::has_beginning(old_name,"/DSS/"))
    cmd_dir="fmd";
  else if (strutils::has_beginning(old_name,"http://rda.ucar.edu/"))
    cmd_dir="wfmd";
  if (!existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+cmd_dir,buf)) {
    std::cerr << "Error: metadata directory not found for ds"+args.dsnum << std::endl;
    exit(1);
  }
  if (!strutils::has_beginning(new_name,"/FS/DSS/") && !strutils::has_beginning(new_name,"/DSS/") && !strutils::has_beginning(new_name,"http://rda.ucar.edu/")) {
    std::cerr << "Error: new_name must begin with '/FS/DSS', '/DSS', or 'http://rda.ucar.edu'" << std::endl;
    exit(1);
  }
  if (new_name == old_name && new_dsnum == args.dsnum) {
    std::cerr << "Error: new_name must be different from old_name" << std::endl;
    exit(1);
  }
  if (((strutils::has_beginning(old_name,"/FS/DSS/") || strutils::has_beginning(old_name,"/DSS/")) && !strutils::has_beginning(new_name,"/FS/DSS/") && !strutils::has_beginning(new_name,"/DSS/")) || (strutils::has_beginning(old_name,"http://rda.ucar.edu/") && !strutils::has_beginning(new_name,"http://rda.ucar.edu/"))) {
    std::cerr << "Error: you can only rename content metadata for MSS files OR for web files, but" << std::endl;
    std::cerr << " not from one to the other" << std::endl;
    exit(1);
  }
  args.path=old_name;
  args.filename=new_name;
  metautils::cmd_register("rcm",user);
  if (!verifiedNewFileIsArchived(error))
    metautils::logError("rcm main returned error: "+error,"rcm",user,args.argsString);
  else {
    if (!renamedCMD())
	metautils::logError("rcm main retured error: No content metadata were found for "+old_name,"rcm",user,args.argsString);
    if (!no_cache) {
	if (strutils::has_beginning(new_name,"/FS/DSS/") || strutils::has_beginning(new_name,"/DSS/"))
	  summarizeMetadata::createFileListCache("MSS","rcm",user,"-d:"+args.dsnum+":"+old_name+":"+new_name);
	else
	  summarizeMetadata::createFileListCache("Web","rcm",user,"-d:"+args.dsnum+":"+old_name+":"+new_name);
    }
    return 0;
  }
}
