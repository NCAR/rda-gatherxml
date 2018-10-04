#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <string>
#include <regex>
#include <sstream>
#include <deque>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

bool showinfo;
struct Entry {
  Entry() : key(),string() {}

  std::string key;
  std::string string;
};
std::string user=getenv("USER");

std::string webhome()
{
  if (!metautils::directives.data_root_alias.empty()) {
    return metautils::directives.data_root_alias+"/ds"+metautils::args.dsnum;
  }
  else {
    return metautils::web_home();
  }
}

void inventory_all()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string dsnum=strutils::substitute(metautils::args.dsnum,".","");
  std::stringstream output,error;

  if (metautils::args.data_format != "grib" && metautils::args.data_format != "grib2" && metautils::args.data_format != "grib0" && metautils::args.data_format != "cfnetcdf" && metautils::args.data_format != "hdf5nc4") {
    metautils::log_error("unable to inventory '"+metautils::args.data_format+"' files","gatherxml",user);
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("unable to connected to RDA metadata database server","gatherxml",user);
  }
  if (MySQL::table_exists(server,"IGrML.ds"+dsnum+"_inventory_summary")) {
    query.set("select w.webID,f.format from WGrML.ds"+dsnum+"_webfiles as w left join IGrML.ds"+dsnum+"_inventory_summary as i on i.webID_code = w.code left join WGrML.formats as f on f.code = w.format_code where isnull(i.webID_code) or isnull(inv)");
  }
  else if (MySQL::table_exists(server,"WGrML.ds"+dsnum+"_webfiles")) {
    query.set("select w.webID,f.format from WGrML.ds"+dsnum+"_webfiles as w left join WGrML.formats as f on f.code = w.format_code");
  }
  else if (MySQL::table_exists(server,"IObML.ds"+dsnum+"_inventory_summary")) {
    query.set("select w.webID,f.format from WObML.ds"+dsnum+"_webfiles as w left join IObML.ds"+dsnum+"_inventory_summary as i on i.webID_code = w.code left join WObML.formats as f on f.code = w.format_code where isnull(i.webID_code) or isnull(inv)");
  }
  else if (MySQL::table_exists(server,"WObML.ds"+dsnum+"_webfiles")) {
    query.set("select w.webID,f.format from WObML.ds"+dsnum+"_webfiles as w left join WObML.formats as f on f.code = w.format_code");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("inventory_all() returned error: '"+query.error()+"'","gatherxml",user);
  }
  server.disconnect();
  size_t n=0;
  while (query.fetch_row(row)) {
    std::string format=row[1];
    strutils::replace_all(format,"WMO_","");
    format=strutils::to_lower(format);
    if (format == "grib1") {
	format="grib";
    }
    else if (format == "netcdf4") {
	format="hdf5nc4";
    }
    ++n;
    if (format == metautils::args.data_format || (format == "netcdf" && metautils::args.data_format == "cfnetcdf")) {
	std::string command=metautils::directives.local_root+"/bin/gatherxml";
	if (n != query.num_rows() && (n % 100) != 0) {
	  command+=" -R -S";
	}
	command+=" -d "+metautils::args.dsnum+" -f "+metautils::args.data_format+" -I https://rda.ucar.edu"+webhome()+"/"+row[0];
	unixutils::mysystem2(command,output,error);
    }
  }
}

int main(int argc,char **argv)
{
  std::ifstream ifs;
  char line[256];
  std::string sline,separator;
  std::deque<std::string> sp;
  my::map<Entry> utility_lookup_table,alias_table,reverse_alias_table,utility_table;
  Entry e,u;
  FILE *p;
  std::stringstream oss,ess;
  bool ignore_local_file=false;

  if (argc < 6 && argc != 2 && argc != 3) {
    std::cerr << "For command usage, see the \"Metadata Utilities\" man pages, which are accessible" << std::endl;
    std::cerr << "  from the dashboard under \"Dataset Stewardship Tools and Documentation\"." << std::endl;
    exit(1);
  }
  if (argc == 3) {
    separator=argv[1];
    metautils::args.args_string=argv[2];
    ignore_local_file=true;
  }
  else {
    separator="%";
    metautils::args.args_string=unixutils::unix_args_string(argc,argv,'%');
    if (argc == 2) {
	ignore_local_file=true;
    }
  }
/*
if (user == "chifan") {
std::cerr << "Terminating." << std::endl;
exit(1);
}
*/
  metautils::read_config("gatherxml",user);
  ifs.open((metautils::directives.dss_root+"/bin/conf/gatherxml.conf").c_str());
  if (!ifs.is_open()) {
    metautils::log_error("unable to open "+metautils::directives.dss_root+"/bin/conf/gatherxml.conf","gatherxml",user);
  }
  ifs.getline(line,256);
  while (!ifs.eof()) {
    if (line[0] != '#') {
	sline=line;
	sp=strutils::split(sline);
	e.key=sp[0];
	e.string=sp[1];
	if (!utility_table.found(sp[1],u)) {
	  u.key=sp[1];
	  utility_table.insert(u);
	}
	utility_lookup_table.insert(e);
	if (sp.size() > 2) {
	  auto aparts=strutils::split(sp[2],",");
	  for (auto& apart : aparts) {
	    e.key=apart;
	    e.string=sp[0];
	    alias_table.insert(e);
	  }
	  e.key=sp[0];
	  e.string=sp[2];
	  reverse_alias_table.insert(e);
	}
    }
    ifs.getline(line,256);
  }
  ifs.close();
  showinfo=false;
  sp=strutils::split(metautils::args.args_string,separator);
  size_t num_parts=sp.size();
  if (sp.size() == 1) {
    num_parts++;
  }
  for (size_t n=0; n < num_parts-1; ++n) {
    if (sp[n] == "-f") {
	metautils::args.data_format=sp[++n];
    }
    else if (sp[n] == "-d") {
	metautils::args.dsnum=sp[++n];
	if (std::regex_search(metautils::args.dsnum,std::regex("^ds"))) {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-l") {
	if (!ignore_local_file) {
	  metautils::args.local_name=sp[++n];
	}
	else {
	  size_t idx1=metautils::args.args_string.find("-l");
	  size_t idx2=metautils::args.args_string.find("%",idx1+3);
	  metautils::args.args_string=metautils::args.args_string.substr(0,idx1)+metautils::args.args_string.substr(idx2+1);
	}
    }
    else if (sp[n] == "-m") {
	metautils::args.member_name=sp[++n];
    }
    else if (sp[n] == "-showinfo") {
	showinfo=true;
    }
  }
  if (showinfo) {
    for (const auto& key : utility_table.keys()) {
	p=popen((metautils::directives.dss_bindir+"/"+key+" 2>&1").c_str(),"r");
	std::cerr << "\nutility:" << strutils::substitute(key,"_"," ") << std::endl;
	std::cerr << "supported formats (\"-f\" flag):" << std::endl;
	while (fgets(line,256,p) != nullptr) {
	  sline=line;
	  if (std::regex_search(sline,std::regex("^-f"))) {
	    strutils::chop(sline);
	    sp=strutils::split(sline);
	    std::cerr << "  '" << sp[1] << "'";
	    if (reverse_alias_table.found(sp[1],e)) {
		auto aparts=strutils::split(e.string,",");
		for (auto& apart : aparts) {
		  std::cerr << " OR '" << apart << "'";
		}
	    }
	    std::cerr << " (" << sp[2];
	    for (size_t n=3; n < sp.size(); ++n) {
		std::cerr << " " << sp[n];
	    }
	    std::cerr << ")" << std::endl;
	  }
	}
	pclose(p);
    }
  }
  else {
    if (metautils::args.data_format.empty()) {
	std::cerr << "Error: no format specified" << std::endl;
	exit(1);
    }
    else {
	metautils::args.data_format=strutils::to_lower(metautils::args.data_format);
    }
    if (metautils::args.data_format == "grib1") {
	metautils::args.data_format="grib";
    }
    if (metautils::args.dsnum.empty()) {
	std::cerr << "Error: no dataset number specified" << std::endl;
	exit(1);
    }
    metautils::args.path=sp.back();
    if (!std::regex_search(metautils::args.path,std::regex("^(/FS){0,1}/DSS/")) && !std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
	if (metautils::args.path.length() > 128) {
	  std::cerr << "Error: filename exceeds 128 characters in length" << std::endl;
	  exit(1);
	}
	if (metautils::args.path == "invall") {
	  inventory_all();
	  exit(0);
	}
	else {
	  std::string sdum= (metautils::args.path[0] == '/') ? "https://rda.ucar.edu"+webhome()+metautils::args.path : "https://rda.ucar.edu"+webhome()+"/"+metautils::args.path;
	  strutils::replace_all(metautils::args.args_string,metautils::args.path,sdum);
	  metautils::args.path=sdum;
	}
    }
    if (std::regex_search(metautils::args.path,std::regex("^(/FS){0,1}/DSS/")) && std::regex_search(unixutils::host_name(),std::regex("^r([0-9]){1,}i([0-9]){1,}n([0-9]){1,}$"))) {
	std::cerr << "Terminating: cheyenne compute nodes do not have access to the HPSS" << std::endl;
	exit(1);
    }
    sp=strutils::split(metautils::args.path,"..m..");
    if (sp.size() > 1) {
	metautils::args.path=sp[0];
	if (metautils::args.member_name.empty()) {
	  metautils::args.member_name=sp[1];
	}
	else if (metautils::args.member_name != sp[1]) {
	  std::cerr << "Error: two different member name specifications" << std::endl;
	  exit(1);
	}
	sp=strutils::split(metautils::args.args_string,"%");
	metautils::args.args_string=sp[0];
	for (size_t n=1; n < sp.size()-1; ++n) {
	  metautils::args.args_string+="%"+sp[n];
	}
	metautils::args.args_string+="%-m%"+metautils::args.member_name+"%"+metautils::args.path;
    }
    if (!metautils::args.member_name.empty()) {
	if (!std::regex_search(strutils::to_lower(metautils::args.path),std::regex("\\.htar$"))) {
	  std::cerr << "Error: a member name is not valid for the specified data file" << std::endl;
	  exit(1);
	}
    }
    else {
	if (std::regex_search(strutils::to_lower(metautils::args.path),std::regex("\\.htar$"))) {
	  std::cerr << "Error: a member name MUST be specified for an HTAR data file" << std::endl;
	  exit(1);
	}
    }
    if (utility_lookup_table.found(metautils::args.data_format,e)) {
	auto t1=std::time(nullptr);
	if (unixutils::mysystem2(metautils::directives.dss_bindir+"/"+e.string+" "+strutils::substitute(metautils::args.args_string,"%"," "),oss,ess) < 0) {
	  if (std::regex_search(ess.str(),std::regex("^Terminating"))) {
	    std::cerr << ess.str() << std::endl;
	  }
	  else {
	    metautils::log_error("-q"+ess.str(),"gatherxml",user);
	  }
	}
	auto t2=std::time(nullptr);
	metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","gatherxml.time",user);
    }
    else {
	if (alias_table.found(metautils::args.data_format,e)) {
	  if (utility_lookup_table.found(e.string,e)) {
	    strutils::replace_all(metautils::args.args_string,"-f%"+metautils::args.data_format,"-f%"+e.key);
	    auto t1=std::time(nullptr);
	    if (unixutils::mysystem2(metautils::directives.dss_bindir+"/"+e.string+" "+strutils::substitute(metautils::args.args_string,"%"," "),oss,ess) < 0) {
		if (std::regex_search(ess.str(),std::regex("^Terminating"))) {
		  std::cerr << ess.str() << std::endl;
		}
		else {
		  metautils::log_error("-q"+ess.str(),"gatherxml",user);
		}
	    }
	    auto t2=std::time(nullptr);
	    metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","gatherxml.time",user);
	  }
	  else {
	    std::cerr << "format '" << metautils::args.data_format << "' does not map to a content metadata utility" << std::endl;
	  }
	}
	else {
	  std::cerr << "format '" << metautils::args.data_format << "' does not map to a content metadata utility" << std::endl;
	}
    }
  }
  return 0;
}
