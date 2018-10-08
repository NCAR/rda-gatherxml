#include <iostream>
#include <fstream>
#include <stdio.h>
#include <sys/stat.h>
#include <regex>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

struct LocalArgs {
  LocalArgs() : gindex(),start_date(),start_time(),end_date(),end_time(),tz(),start_date_flag(),start_time_flag(),end_date_flag(),end_time_flag() {}

  std::string gindex;
  std::string start_date,start_time,end_date,end_time,tz;
  int start_date_flag,start_time_flag,end_date_flag,end_time_flag;
} local_args;
const std::string user=getenv("USER");
const std::string server_root="/"+strutils::token(unixutils::host_name(),".",0);

int check_date(std::string date,std::string& db_date,std::string type)
{
  std::string sdum;
  std::deque<std::string> sp;
  int flag=0;

  db_date=date;
  sp=strutils::split(date,"-");
// check the year
  if (sp[0].length() != 4 || sp[0] < "1000" || sp[0] > "3000") {
    std::cerr << "Error: bad year in " << type << " date" << std::endl;
    exit(1);
  }
  flag=1;
  if (sp.size() > 1) {
// check the month
    if (sp[1].length() != 2 || sp[1] < "01" || sp[1] > "12") {
	std::cerr << "Error: bad month in " << type << " date" << std::endl;
	exit(1);
    }
    flag=2;
  }
  else {
    if (type == "start") {
	db_date+="-01-01";
    }
    else {
	db_date+="-12-31";
    }
  }
  if (sp.size() > 2) {
// check the day
    sdum=strutils::itos(dateutils::days_in_month(std::stoi(sp[0]),std::stoi(sp[1])));
    if (sp[2].length() != 2 || sp[2] < "01" || sp[2] > sdum) {
	std::cerr << "Error: bad month in " << type << " date" << std::endl;
	exit(1);
    }
    flag=3;
  }
  else {
    if (type == "start")
	db_date+="-01";
    else
	db_date+="-"+strutils::itos(dateutils::days_in_month(std::stoi(sp[0]),std::stoi(sp[1])));
  }
  return flag;
}

int check_time(std::string time,std::string& db_time,std::string type)
{
  std::string sdum;
  std::deque<std::string> sp;
  int flag=0;

  db_time=time;
  sp=strutils::split(time,":");
// check the hour
  if (sp[0].length() != 2 || sp[0] < "00" || sp[0] > "23") {
    std::cerr << "Error: bad hour in " << type << " time" << std::endl;
    exit(1);
  }
  flag=1;
  if (sp.size() > 1) {
// check the minutes
    if (sp[1].length() != 2 || sp[1] < "00" || sp[1] > "59") {
	std::cerr << "Error: bad minutes in " << type << " time" << std::endl;
	exit(1);
    }
    flag=2;
  }
  else {
    if (type == "start") {
	db_time+=":00:00";
    }
    else {
	db_time+=":59:59";
    }
  }
  if (sp.size() > 2) {
// check the seconds
    if (sp[2].length() != 2 || sp[2] < "00" || sp[2] > "59") {
	std::cerr << "Error: bad seconds in " << type << " time" << std::endl;
	exit(1);
    }
    flag=3;
  }
  else {
    if (type == "start") {
	db_time+=":00";
    }
    else {
	db_time+=":59";
    }
  }
  return flag;
}

int main(int argc,char **argv)
{
//  TempFile t;

  if (argc < 7) {
    std::cerr << "usage: sdp -d <nnn.n> -g <gindex> {-bd YYYY[-MM[-DD -bt HH[:MM[:SS]]]] -ed YYYY[-MM[-DD -et HH[:MM[:SS]]]]} [-tz <+|-nnnn>]" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>          dataset number as nnn.n (will also accept \"dsnnn.n\")" << std::endl;
    std::cerr << "-g <gindex>         group index number for which period is being set" << std::endl;
    std::cerr << "-bd YYYY[-MM[-DD]]  start date as YYYY-MM-DD, where month and day are optional" << std::endl;
    std::cerr << "-ed YYYY[-MM[-DD]]  end date as YYYY-MM-DD, where month and day are optional" << std::endl;
    std::cerr << "                    **NOTE: one or both of -bd and -ed must be specified" << std::endl;
    std::cerr << std::endl;
    std::cerr << "optional:" << std::endl;
    std::cerr << "-bt HH[:MM[:SS]]    start time as HH:MM:SS, where minutes and seconds are" << std::endl;
    std::cerr << "                    optional" << std::endl;
    std::cerr << "-et HH[:MM[:SS]]    end time as HH:MM:SS, where minutes and seconds are optional" << std::endl;
    std::cerr << "-tz <+|-nnnn>       timezone offset from UTC" << std::endl;
    exit(1);
  }
  local_args.start_date_flag=local_args.start_time_flag=local_args.end_date_flag=local_args.end_time_flag=0;
  metautils::read_config("sdp",user);
  std::string dsnum2;
  std::string db_start_date,db_start_time,db_end_date,db_end_time;
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,'%');
  auto unix_args=strutils::split(metautils::args.args_string,"%");
  for (size_t n=0; n < unix_args.size(); ++n) {
    if (unix_args[n] == "-d") {
	metautils::args.dsnum=unix_args[++n];
	if (std::regex_search(metautils::args.dsnum,std::regex("^ds"))) {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
	dsnum2=strutils::substitute(metautils::args.dsnum,".","");
    }
    else if (unix_args[n] == "-g") {
	local_args.gindex=unix_args[++n];
    }
    else if (unix_args[n] == "-bd") {
	local_args.start_date=unix_args[++n];
	local_args.start_date_flag=check_date(local_args.start_date,db_start_date,"start");
    }
    else if (unix_args[n] == "-bt") {
	local_args.start_time=unix_args[++n];
	local_args.start_time_flag=check_time(local_args.start_time,db_start_time,"start");
    }
    else if (unix_args[n] == "-ed") {
	local_args.end_date=unix_args[++n];
	local_args.end_date_flag=check_date(local_args.end_date,db_end_date,"end");
    }
    else if (unix_args[n] == "-et") {
	local_args.end_time=unix_args[++n];
	local_args.end_time_flag=check_time(local_args.end_time,db_end_time,"end");
    }
    else if (unix_args[n] == "-tz") {
	local_args.tz=unix_args[++n];
	if (!std::regex_search(local_args.tz,std::regex("^[+-]")) || !strutils::is_numeric(local_args.tz.substr(1))) {
	  std::cerr << "Error: bad time zone specification" << std::endl;
	  exit(1);
	}
    }
    else {
	std::cerr << "Warning: flag " << unix_args[n] << " is not a valid flag" << std::endl;
    }
  }
  if (metautils::args.dsnum.empty()) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
  if (local_args.gindex.empty()) {
    std::cerr << "Error: no group index specified" << std::endl;
    exit(1);
  }
  if (local_args.start_date.empty() && local_args.end_date.empty()) {
    std::cerr << "Error: no start or end date specified" << std::endl;
    exit(1);
  }
  if (!local_args.start_time.empty() && local_args.start_date_flag != 3) {
    std::cerr << "Error: you must specify a full start date to be able to specify a start time" << std::endl;
    exit(1);
  }
  if (!local_args.end_time.empty() && local_args.end_date_flag != 3) {
    std::cerr << "Error: you must specify a full end date to be able to specify a end time" << std::endl;
    exit(1);
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
  if (!server || !server_d) {
    std::cerr << "Error connecting to MySQL:: database" << std::endl;
    exit(1);
  }
  MySQL::LocalQuery query("type","search.datasets","dsid = '"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("error '"+query.error()+"' while checking dataset type","sdp",user);
  }
  MySQL::Row row;
  if (!query.fetch_row(row)) {
    std::cerr << "Error: ds" << metautils::args.dsnum << " does not exist" << std::endl;
    exit(1);
  }
  if (row[0] == "I") {
    std::cerr << "Abort: ds" << metautils::args.dsnum << " is an internal dataset" << std::endl;
    exit(1);
  }
  auto db_names=server.db_names();
  auto found_cmd_db=false;
  for (const auto& db : db_names) {
    if (table_exists(server,db+".ds"+dsnum2+"_primaries")) {
	found_cmd_db=true;
	break;
    }
  }
  if (found_cmd_db) {
    std::cerr << "Error: the dataset period has been set from content metadata and can't be modified with " << argv[0] << std::endl;
    exit(1);
  }
  query.set("gindex","dssdb.dsperiod","dsid = 'ds"+metautils::args.dsnum+"' and gindex = "+local_args.gindex);
  if (query.submit(server_d) < 0) {
    std::cerr << "Error: " << query.error() << std::endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    std::cerr << "Error: the specified group index does not have an associated period - you must first create this association with the Metadata Manager" << std::endl;
    exit(1);
  }
  metautils::log_warning("You should consider generating file content metadata for this dataset","sdp",user);
  query.set("grpid","dssdb.dsgroup","dsid = 'ds"+metautils::args.dsnum+"' and gindex = "+local_args.gindex);
  if (query.submit(server_d) < 0) {
    std::cerr << "Error: " << query.error() << std::endl;
    exit(1);
  }
  std::string group_ID;
  if (query.fetch_row(row)) {
    group_ID=row[0];
  }
  else {
    group_ID="Entire Dataset";
  }
  auto *tdir=new TempDir();
  if (!tdir->create(metautils::directives.temp_path)) {
    metautils::log_error("unable to create temporary directory (1)","sdp",user);
  }
  auto old_ds_overview=unixutils::remote_web_file("http://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/dsOverview.xml",tdir->name());
  std::ifstream ifs(old_ds_overview.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("unable to open overview XML file for ds"+metautils::args.dsnum,"sdp",user);
  }
  auto *sync_dir=new TempDir();
  if (!sync_dir->create(metautils::directives.temp_path)) {
    metautils::log_error("unable to create temporary directory (2)","sdp",user);
  }
  std::ofstream ofs((sync_dir->name()+"/dsOverview.xml").c_str());
  if (!ofs.is_open()) {
    metautils::log_error("unable to open output file for updated XML","sdp",user);
  }
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (std::regex_search(sline,std::regex("^  <timeStamp"))) {
	ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %HH:%MM:%SS %Z") << "\" />" << std::endl;
    }
    else if (std::regex_search(sline,std::regex("^    <temporal")) && (std::regex_search(sline,std::regex("groupID=\""+group_ID+"\"")) || (group_ID == "Entire Dataset" && !std::regex_search(sline,std::regex("groupID"))))) {
	std::string new_line="    <temporal";
	if (!local_args.start_date.empty()) {
	  new_line+=" start=\""+local_args.start_date;
	  if (!local_args.start_time.empty()) {
	    new_line+=" "+local_args.start_time;
	    if (!local_args.tz.empty()) {
		new_line+=" "+local_args.tz;
	    }
	    else {
		new_line+=" +0000";
	    }
	  }
	  new_line+="\"";
	}
	else {
	  auto idx=sline.find("start=");
	  auto parts=strutils::split(sline.substr(idx));
	  new_line+=" "+parts.front();
	}
	auto idx=sline.find("end=");
	if (!local_args.end_date.empty()) {
	  new_line+=" end=\""+local_args.end_date;
	  if (!local_args.end_time.empty()) {
	    new_line+=" "+local_args.end_time;
	    if (!local_args.tz.empty()) {
		new_line+=" "+local_args.tz;
	    }
	    else {
		new_line+=" +0000";
	    }
	  }
	  new_line+="\"";
	}
	else {
	  auto parts=strutils::split(sline.substr(idx));
	  new_line+=" "+parts.front();
	}
	auto group_ID=sline.substr(idx+5);
	if ( (idx=group_ID.find("\"")) != std::string::npos) {
	  group_ID=group_ID.substr(idx);
	}
	auto group_parts=strutils::split(group_ID);
	for (size_t n=1; n < group_parts.size(); ++n) {
	  new_line+=" "+group_parts[n];
	}
	ofs << new_line << std::endl;
    }
    else {
	ofs << line << std::endl;
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  delete tdir;
  ofs.close();
  auto cvs_key=strutils::strand(15);
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/sh -c \"curl -s --data 'authKey=qGNlKijgo9DJ7MN&cmd=identify' http://rda.ucar.edu/cgi-bin/dss/remoteRDAServerUtils\"",oss,ess) < 0) {
    metautils::log_error("unable to identify web server - error: '"+ess.str()+"'","sdp",user);
  }
  if (unixutils::mysystem2("/bin/sh -c \""+strutils::token(oss.str(),".",0)+"-sync "+sync_dir->name()+"/dsOverview.xml /"+strutils::token(oss.str(),".",0)+"/web/ds"+metautils::args.dsnum+".xml."+cvs_key+"\"",oss,ess) < 0) {
    metautils::log_error("unable to web-sync file for CVS - error: '"+ess.str()+"'","sdp",user);
  }
  unixutils::mysystem2("/usr/bin/wget -q -O - --post-data=\"authKey=qGNlKijgo9DJ7MN&cmd=cvssdp&dsnum="+metautils::args.dsnum+"&key="+cvs_key+"\" http://rda.ucar.edu/cgi-bin/dss/remoteRDAServerUtils",oss,ess);
  if (!oss.str().empty()) {
    metautils::log_error("cvs error(s): "+oss.str(),"sdp",user);
  }
  std::string error;
  if (unixutils::rdadata_sync(sync_dir->name(),".","/data/web/datasets/ds"+metautils::args.dsnum+"/metadata",metautils::directives.rdadata_home,error) < 0) {
    metautils::log_error("unable to rdadata_sync updated XML file - error(s): '"+error+"'","sdp",user);
  }
  delete sync_dir;
  std::string update_string;
  if (!db_start_date.empty()) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="date_start = '"+db_start_date+"'";
  }
  if (!db_start_time.empty()) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="time_start = '"+db_start_time+"'";
  }
  if ( (local_args.start_date_flag+local_args.start_time_flag) > 0) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="start_flag = "+strutils::itos(local_args.start_date_flag+local_args.start_time_flag);
  }
  if (!db_end_date.empty()) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="date_end = '"+db_end_date+"'";
  }
  if (!db_end_time.empty()) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="time_end = '"+db_end_time+"'";
  }
  if ( (local_args.end_date_flag+local_args.end_time_flag) > 0) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="end_flag = "+strutils::itos(local_args.end_date_flag+local_args.end_time_flag);
  }
  if (!local_args.tz.empty()) {
    if (!update_string.empty()) {
	update_string+=", ";
    }
    update_string+="time_zone = '"+local_args.tz+"'";
  }
  if (server_d.update("dssdb.dsperiod",update_string,"dsid = 'ds"+metautils::args.dsnum+"' and gindex = "+local_args.gindex) < 0) {
    metautils::log_error("while updating dssdb.dsperiod: '"+server_d.error()+"'","sdp",user);
  }
  unixutils::mysystem2(metautils::directives.local_root+"/bin/dsgen "+metautils::args.dsnum,oss,ess);
}
