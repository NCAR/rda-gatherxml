#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <regex>
#include <sstream>
#include <deque>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

bool showinfo;
struct Entry {
  Entry() : key(),string() {}

  std::string key;
  std::string string;
};

std::string webhome()
{
  if (!metautils::directives.data_root_alias.empty()) {
    return metautils::directives.data_root_alias+"/ds"+metautils::args.dsnum;
  }
  else {
    return metautils::web_home();
  }
}

void *do_inventory(void *ts)
{
  auto *command=reinterpret_cast<std::string *>(ts);
  std::stringstream output,error;
  unixutils::mysystem2(*command,output,error);
  return nullptr;
}

void inventory_all()
{
  const std::string THIS_FUNC=__func__+std::string("()");
  if (metautils::args.data_format != "grib" && metautils::args.data_format != "grib2" && metautils::args.data_format != "grib0" && metautils::args.data_format != "cfnetcdf" && metautils::args.data_format != "hdf5nc4") {
    metautils::log_error2("unable to inventory '"+metautils::args.data_format+"' files",THIS_FUNC,"gatherxml",USER);
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error2("unable to connected to RDA metadata database server",THIS_FUNC,"gatherxml",USER);
  }
  std::string dsnum=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::LocalQuery query;
  if (MySQL::table_exists(server,"IGrML.ds"+dsnum+"_inventory_summary")) {
    query.set("select w.webID,f.format from WGrML.ds"+dsnum+"_webfiles2 as w left join IGrML.ds"+dsnum+"_inventory_summary as i on i.webID_code = w.code left join WGrML.formats as f on f.code = w.format_code where isnull(i.webID_code) or isnull(inv)");
  }
  else if (MySQL::table_exists(server,"WGrML.ds"+dsnum+"_webfiles2")) {
    query.set("select w.webID,f.format from WGrML.ds"+dsnum+"_webfiles2 as w left join WGrML.formats as f on f.code = w.format_code");
  }
  else if (MySQL::table_exists(server,"IObML.ds"+dsnum+"_inventory_summary")) {
    query.set("select w.webID,f.format from WObML.ds"+dsnum+"_webfiles2 as w left join IObML.ds"+dsnum+"_inventory_summary as i on i.webID_code = w.code left join WObML.formats as f on f.code = w.format_code where isnull(i.webID_code) or isnull(inv)");
  }
  else if (MySQL::table_exists(server,"WObML.ds"+dsnum+"_webfiles2")) {
    query.set("select w.webID,f.format from WObML.ds"+dsnum+"_webfiles2 as w left join WObML.formats as f on f.code = w.format_code");
  }
  if (query.submit(server) < 0) {
    metautils::log_error2("'"+query.error()+"'",THIS_FUNC,"gatherxml",USER);
  }
  server.disconnect();
  const size_t MAX_NUM_THREADS=4;
  auto t_idx=new int[MAX_NUM_THREADS];
  for (size_t n=0; n < MAX_NUM_THREADS; ++n) {
    t_idx[n]=-1;
  }
  auto *tids=new pthread_t[MAX_NUM_THREADS];
  auto *commands=new std::string[MAX_NUM_THREADS];
  int num_threads=0,thread_index=0;
  size_t n=0;
  for (const auto& row : query) {
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
	while (num_threads == MAX_NUM_THREADS) {
	  for (int m=0; m < num_threads; ++m) {
	    if (pthread_kill(tids[m],0) != 0) {
		pthread_join(tids[m],nullptr);
		thread_index=m;
		t_idx[m]=-1;
		--num_threads;
		break;
	    }
	  }
	}
	commands[thread_index]=metautils::directives.local_root+"/bin/gatherxml";
	if (n != query.num_rows() && (n % 100) != 0) {
	  commands[thread_index]+=" -R -S";
	}
	commands[thread_index]+=" -d "+metautils::args.dsnum+" -f "+metautils::args.data_format+" -I https://rda.ucar.edu"+webhome()+"/"+row[0];
	pthread_create(&tids[thread_index],nullptr,do_inventory,reinterpret_cast<void *>(&commands[thread_index]));
	t_idx[thread_index]=0;
	++thread_index;
	++num_threads;
    }
  }
  for (size_t n=0; n < MAX_NUM_THREADS; ++n) {
    if (t_idx[n] == 0) {
	pthread_join(tids[n],nullptr);
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

  if (argc < 6 && argc != 2 && argc != 3) {
    std::cerr << "For command usage, see the \"Metadata Utilities\" man pages, which are accessible" << std::endl;
    std::cerr << "  from the dashboard under \"Dataset Stewardship Tools and Documentation\"." << std::endl;
    exit(1);
  }
  if (argc == 3) {
    separator=argv[1];
    metautils::args.args_string=argv[2];
  }
  else {
    separator="%";
    metautils::args.args_string=unixutils::unix_args_string(argc,argv,'%');
  }
  metautils::read_config("gatherxml",USER);
  ifs.open((metautils::directives.decs_root+"/bin/conf/gatherxml.conf").c_str());
  if (!ifs.is_open()) {
    metautils::log_error2("unable to open "+metautils::directives.decs_root+"/bin/conf/gatherxml.conf","main()","gatherxml",USER);
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
    else if (sp[n] == "-showinfo") {
	showinfo=true;
    }
  }
  if (showinfo) {
    for (const auto& key : utility_table.keys()) {
	p=popen((metautils::directives.decs_bindir+"/"+key+" 2>&1").c_str(),"r");
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
if (std::regex_search(metautils::args.path,std::regex("^/FS/DECS/"))) {
std::cerr << "HPSS files are no longer supported" << std::endl;
exit(1);
}
    if (!std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
	if (metautils::args.path.length() > 128) {
	  std::cerr << "Error: filename exceeds 128 characters in length" << std::endl;
	  exit(1);
	}
	if (metautils::args.path == "invall") {
	  inventory_all();
	  exit(0);
	}
	else {
	  std::string url= (metautils::args.path[0] == '/') ? "https://rda.ucar.edu"+webhome()+metautils::args.path : "https://rda.ucar.edu"+webhome()+"/"+metautils::args.path;
	  auto idx=metautils::args.args_string.rfind(separator);
	  if (idx == std::string::npos) {
	    metautils::log_error2("bad arguments string: '"+metautils::args.args_string,"main()","gatherxml",USER);
	  }
	  metautils::args.args_string=metautils::args.args_string.substr(0,idx+1)+url;
	  metautils::args.path=url;
	}
    }
    if (utility_lookup_table.found(metautils::args.data_format,e)) {
	auto t1=std::time(nullptr);
	auto exit_status=unixutils::mysystem2(metautils::directives.decs_bindir+"/"+e.string+" "+strutils::substitute(metautils::args.args_string,"%"," "),oss,ess);
	if (exit_status != 0) {
	  if (exit_status == 2) {
	    std::cerr << ess.str() << std::endl;
	    exit(1);
	  }
	  else {
	    metautils::log_error2(ess.str(),"main()","gatherxml",USER);
	  }
	}
	else if (!oss.str().empty()) {
	  std::cout << oss.str() << std::endl;
	}
	auto t2=std::time(nullptr);
	metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","gatherxml.time",USER);
    }
    else {
	if (alias_table.found(metautils::args.data_format,e)) {
	  if (utility_lookup_table.found(e.string,e)) {
	    strutils::replace_all(metautils::args.args_string,"-f%"+metautils::args.data_format,"-f%"+e.key);
	    auto t1=std::time(nullptr);
	    auto exit_status=unixutils::mysystem2(metautils::directives.decs_bindir+"/"+e.string+" "+strutils::substitute(metautils::args.args_string,"%"," "),oss,ess);
	    if (exit_status != 0) {
		if (exit_status == 2 || metautils::args.dsnum >= "999.0") {
		  std::cerr << ess.str() << std::endl;
		  exit(1);
		}
		else {
		  metautils::log_error2(ess.str(),"main()","gatherxml",USER);
		}
	    }
	    auto t2=std::time(nullptr);
	    metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","gatherxml.time",USER);
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
