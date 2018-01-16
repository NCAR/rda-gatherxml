#include <iostream>
#include <fstream>
#include <thread>
#include <algorithm>
#include <ftw.h>
#include <signal.h>
#include <pthread.h>
#include <sstream>
#include <regex>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bsort.hpp>
#include <bitmap.hpp>
#include <xml.hpp>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <xmlutils.hpp>
#include <search.hpp>
#include <metadata.hpp>
using namespace std;

struct FileEntry {
  FileEntry() : path(),name(),metaname(),start(),end(),is_mss_file(false),is_web_file(false) {}

  std::string path,name;
  std::string metaname;
  DateTime start,end;
  bool is_mss_file,is_web_file;
};
std::list<FileEntry> grml_file_list;
std::list<FileEntry> obml_file_list;
std::list<FileEntry> satml_file_list;
std::list<FileEntry> fixml_file_list;
metautils::Directives directives;
metautils::Args args;
struct LocalArgs {
  LocalArgs() : dsnum2(),summ_type(),file(),cmd_directory(),data_format(),gindex_list(),summarize_all(false),refresh_only(false),added_variable(false),verbose(false),notify(false),create_kml(true),update_graphics(true),update_db(true),refresh_hpss(false),refresh_web(false),refresh_inv(false),is_hpss_file(false),is_web_file(false),summarized_hpss_file(false),summarized_web_file(false) {}

  std::string dsnum2;
  std::string summ_type;
  std::string file,cmd_directory,data_format;
  std::list<std::string> gindex_list;
  bool summarize_all,refresh_only;
  bool added_variable,verbose,notify,create_kml;
  bool update_graphics,update_db;
  bool refresh_hpss,refresh_web,refresh_inv,is_hpss_file,is_web_file;
  bool summarized_hpss_file,summarized_web_file;
} local_args;
MySQL::Server server;
TempDir temp_dir;
auto uflag=strutils::strand(3);
std::string files_table_name,files_template_name,file_ID_type,update_string;
my::map<summarizeMetadata::Entry> file_table,format_table;
std::string format_code,file_ID_code;
struct FEntry {
  struct Data {
    Data() : min(0),max(0) {}

    size_t min,max;
  };
  FEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
} fe;
my::map<FEntry> frequency_table;
struct NEntry {
  size_t key;
} ne;
struct ParameterElement {
  struct Data {
    Data() : min_nsteps(0),max_nsteps(0),level_codes(99999),level_code_list() {}

    size_t min_nsteps,max_nsteps;
    my::map<NEntry> level_codes;
    std::vector<size_t> level_code_list;
  };
  ParameterElement() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
} pe;
std::string user=getenv("USER");
struct ThreadStruct {
  ThreadStruct() : strings(),tid(0) {}

  std::vector<std::string> strings;
  pthread_t tid;
};
struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};

void parse_args()
{
  args.regenerate=true;
  auto sp=strutils::split(args.args_string,"`");
  for (size_t n=0; n < sp.size(); n++) {
    if (sp[n] == "-a") {
	if (!local_args.file.empty()) {
	  std::cerr << "Error: specify only one of -a or -wa or -f or -wf" << std::endl;
	  exit(1);
	}
	else {
	  local_args.summarize_all=true;
	  if ((n+1) < sp.size() && sp[n+1][0] != '-') {
	    local_args.summ_type=sp[++n];
	  }
	  local_args.cmd_directory="fmd";
	}
    }
//    else if (sp[n] == "-wa") {
// patch because dsarch calls with -wa way too often
else if (sp[n] == "-WA") {
	if (!local_args.file.empty()) {
	  std::cerr << "Error: specify only one of -a or -wa or -f or -wf" << std::endl;
	  exit(1);
	}
	else {
	  local_args.summarize_all=true;
	  if ((n+1) < sp.size() && sp[n+1][0] != '-') {
	    local_args.summ_type=sp[++n];
	  }
	  local_args.cmd_directory="wfmd";
	}
    }
    else if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	 }
	local_args.dsnum2=strutils::substitute(args.dsnum,".","");
    }
    else if (sp[n] == "-f") {
	if (local_args.summarize_all) {
	  std::cerr << "Error: specify only one of -a or -wa or -f or -wf" << std::endl;
	  exit(1);
	}
	else {
	  if ( (n+1) < sp.size()) {
	    local_args.file=sp[++n];
	    local_args.is_hpss_file=true;
	  }
	  else {
	    std::cerr << "Error: the -f flag requires a file name" << std::endl;
	    exit(1);
	  }
	}
    }
    else if (sp[n] == "-wf") {
	if (local_args.summarize_all) {
	  std::cerr << "Error: specify only one of -a or -wa or -f or -wf" << std::endl;
	  exit(1);
	}
	else {
	  if ( (n+1) < sp.size()) {
	    local_args.file=sp[++n];
	    local_args.is_web_file=true;
	  }
	  else {
	    std::cerr << "Error: the -wf flag requires a file name" << std::endl;
	    exit(1);
	  }
	}
    }
    else if (sp[n] == "-F") {
	if (user == "dattore") {
	  local_args.refresh_only=true;
	  local_args.update_graphics=false;
	  local_args.create_kml=false;
	  local_args.update_db=false;
	  args.regenerate=false;
	}
    }
    else if (sp[n] == "-G") {
	local_args.update_graphics=false;
    }
    else if (sp[n] == "-K") {
	local_args.create_kml=false;
    }
    else if (sp[n] == "-N") {
	local_args.notify=true;
    }
    else if (sp[n] == "-rm") {
	local_args.refresh_hpss=true;
	if ( (n+1) < sp.size() && sp[n+1][0] != '-') {
	  local_args.gindex_list.emplace_back(sp[++n]);
	}
    }
    else if (sp[n] == "-rw") {
/*
//temporary
if (user != "dattore") {
std::cerr << "Exiting - unnecessary call" << std::endl;
exit(1);
}
*/
	local_args.refresh_web=true;
	args.regenerate=false;
	if ( (n+1) < sp.size() && sp[n+1][0] != '-') {
	  local_args.gindex_list.emplace_back(sp[++n]);
	}
    }
    else if (sp[n] == "-ri") {
	local_args.refresh_inv=true;
	if ( (n+1) < sp.size() && sp[n+1][0] != '-') {
	  local_args.gindex_list.emplace_back(sp[++n]);
	}
    }
    else if (sp[n] == "-S") {
	local_args.update_db=false;
    }
    else if (sp[n] == "-R") {
	args.regenerate=false;
    }
    else if (sp[n] == "-V") {
	local_args.verbose=true;
    }
    else {
	std::cerr << "Error: don't understand argument " << sp[n] << std::endl;
	exit(1);
    }
  }
  if (args.dsnum.empty()) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  if (!local_args.summarize_all && !local_args.is_hpss_file && !local_args.is_web_file && !local_args.refresh_hpss && !local_args.refresh_web && !local_args.refresh_inv) {
    std::cerr << "Exiting - nothing to do" << std::endl;
    exit(1);
  }
  if (local_args.update_graphics && !strutils::has_ending(local_args.file,"ObML") && !strutils::has_ending(local_args.file,"FixML")) {
    local_args.update_graphics=false;
  }
}

void find_in_ancillary_table(MySQL::Server& srv,std::string table_name,std::string where_conditions,MySQL::LocalQuery& query)
{
// where_conditions must have 'and' specified as 'AND' because it is possible
//   that 'and' is in fields in the database tables
  std::string columns,values,error;

  std::string wc=where_conditions;
  strutils::replace_all(wc," &eq; "," = ");
  query.set("code",table_name,wc);
  if (query.submit(srv) < 0) {
    metautils::log_error("find_in_ancillary_table() returned error: "+query.error()+" from query: '"+query.show()+"'","scm",user,args.args_string);
  }
  if (query.num_rows() == 0) {
    auto sp=strutils::split(where_conditions," AND ");
    for (size_t n=0; n < sp.size(); ++n) {
	auto sp2=strutils::split(sp[n]," = ");
	if (sp2.size() != 2) {
	  metautils::log_error("find_in_ancillary_table() error in where_conditions: "+where_conditions+", "+sp[n],"scm",user,args.args_string);
	}
	auto sdum=sp2[0];
	strutils::trim(sdum);
	if (!columns.empty()) {
	  columns+=",";
	}
	columns+=sdum;
	sdum=sp2[1];
	strutils::trim(sdum);
	strutils::replace_all(sdum," &eq; "," = ");
	if (!values.empty()) {
	  values+=",";
	}
	values+=sdum;
    }
    std::string result;
    if (srv.command("lock table "+table_name+" write",result) < 0) {
	metautils::log_error("find_in_ancillary_table() returned "+srv.error(),"scm",user,args.args_string);
    }
    if (srv.insert(table_name,columns,values,"") < 0) {
	error=srv.error();
	if (!std::regex_search(error,std::regex("^Duplicate entry"))) {
	  metautils::log_error("find_in_ancillary_table() srv error: "+error+" while inserting ("+columns+") values("+values+") into "+table_name,"scm",user,args.args_string);
	}
    }
    if (srv.command("unlock tables",result) < 0) {
	metautils::log_error("find_in_ancillary_table() returned "+srv.error(),"scm",user,args.args_string);
    }
    query.submit(srv);
    if (query.num_rows() == 0) {
      metautils::log_error("find_in_ancillary_table() error retrieving code from table "+table_name+" for value(s) ("+columns+") values("+values+")","scm",user,args.args_string);
    }
  }
}

std::string grid_definition_parameters(const XMLElement& e)
{
  std::string def_params;
  auto definition=e.attribute_value("definition");
  if (definition == "latLon") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("endLat")+":"+e.attribute_value("endLon")+":"+e.attribute_value("xRes")+":"+e.attribute_value("yRes");
  }
  else if (definition == "gaussLatLon") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("endLat")+":"+e.attribute_value("endLon")+":"+e.attribute_value("xRes")+":"+e.attribute_value("circles");
  }
  else if (definition == "polarStereographic") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("resLat")+":"+e.attribute_value("projLon")+":"+e.attribute_value("pole")+":"+e.attribute_value("xRes")+":"+e.attribute_value("yRes");
  }
  else if (definition == "lambertConformal") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("resLat")+":"+e.attribute_value("projLon")+":"+e.attribute_value("pole")+":"+e.attribute_value("xRes")+":"+e.attribute_value("yRes")+":"+e.attribute_value("stdParallel1")+":"+e.attribute_value("stdParallel2");
  }
  return def_params;
}

void open_xml_file(XMLDocument& xdoc,std::string filename)
{
  auto file=remote_web_file("https://rda.ucar.edu"+filename+".gz",temp_dir.name());
  struct stat buf;
  if (stat(file.c_str(),&buf) == 0) {
    system(("gunzip "+file).c_str());
    strutils::chop(file,3);
  }
  if (!xdoc.open(file)) {
    file=remote_web_file("https://rda.ucar.edu"+filename,temp_dir.name());
    if (!xdoc.open(file)) {
	metautils::log_error("unable to open "+filename,"scm",user,args.args_string);
    }
  }
}

struct ParameterMapEntry {
  std::string key;
  ParameterMap p;
};
void summarize_grml()
{
  XMLDocument xdoc;
  XMLElement e;
  std::list<XMLElementAddress> grid_elements_list,parameter_list;
  std::string format,grid_definition_code,definition,definition_parameters;;
  std::string map,value;
  std::string time_range_code,time_range;
  std::string level_type_code,level_type,level_type_codes;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  my::map<summarizeMetadata::Entry> time_range_table,grid_definition_table,level_type_table,parameter_table;
  summarizeMetadata::Entry entry;
  std::list<XMLElement> grid_list,ensemble_list;
  std::list<XMLAttribute> attribute_list;
  size_t num_grids;
  std::string pstart,pend,start,end;
  std::string database,error,aname;
  std::deque<std::string> sp;
  std::string sdum;
  size_t nsteps,nensembles;
  time_t tm1=0,tm2;
  my::map<StringEntry> grid_process_table;
  StringEntry se;
  MySQL::Server server_d;
  xmlutils::ParameterMapper parameter_mapper;
  bool inserted_unique_level=false;

  if (grml_file_list.size() == 0) {
    return;
  }
  for (auto& fname : grml_file_list) {
    if (local_args.verbose) {
	tm1=time(NULL);
    }
    open_xml_file(xdoc,fname.path);
    metautils::connect_to_rdadb_server(server_d);
    if (!server_d) {
	metautils::log_error("summarize_grml() could not connect to mysql server - error: "+server_d.error(),"scm",user,args.args_string);
    }
    e=xdoc.element("GrML");
    fname.name=e.attribute_value("uri");
    if (std::regex_search(fname.name,std::regex("^file://MSS:(/FS){0,1}/DSS"))) {
	strutils::replace_all(fname.name,"file://MSS:","");
	query2.set("tindex","dssdb.mssfile","mssfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_mss_file=true;
	database="GrML";
	local_args.summarized_hpss_file=true;
	sp=strutils::split(fname.name,"..m..");
	if (sp.size() > 1) {
	  if (server_d.update("htarfile","meta_link = 'Gr'","dsid = 'ds"+args.dsnum+"' and hfile = '"+sp[1]+"'") < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server_d.error()+" while trying to update 'dssdb.htarfile'","scm",user,args.args_string);
	  }
	}
	else {
	  if (server_d.update("mssfile","meta_link = 'Gr'","dsid = 'ds"+args.dsnum+"' and mssfile = '"+fname.name+"'") < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server_d.error()+" while trying to update 'dssdb.mssfile'","scm",user,args.args_string);
	  }
	}
    }
    else if (std::regex_search(fname.name,std::regex("^http(s){0,1}://rda\\.ucar\\.edu")) || std::regex_search(fname.name,std::regex("^http://dss\\.ucar\\.edu"))) {
	fname.name=metautils::relative_web_filename(fname.name);
	query2.set("tindex","dssdb.wfile","wfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_web_file=true;
	database="WGrML";
	local_args.summarized_web_file=true;
	if (server_d.update("wfile","meta_link = 'Gr'","dsid = 'ds"+args.dsnum+"' and wfile = '"+fname.name+"'") < 0) {
	  metautils::log_error("summarize_grml() returned error: "+server_d.error()+" while trying to update 'dssdb.wfile'","scm",user,args.args_string);
	}
    }
    else {
	metautils::log_error("summarize_grml() returned error: invalid uri '"+fname.name+"' in xml file","scm",user,args.args_string);
    }
    if (query2.fetch_row(row)) {
	if (row[0] != "0") {
	  local_args.gindex_list.emplace_back(row[0]);
	}
    }
    format=e.attribute_value("format");
    if (format.empty()) {
	metautils::log_error("summarize_grml() returned error: missing "+database+" format attribute","scm",user,args.args_string);
    }
    else {
	if (server.insert("search.formats","keyword,vocabulary,dsid","'"+format+"','"+database+"','"+args.dsnum+"'","update dsid = values(dsid)") < 0) {
	  metautils::log_error("summarize_grml() returned error: "+server.error()+" while inserting into search.formats","scm",user,args.args_string);
	}
	if (local_args.data_format.empty()) {
	  local_args.data_format=format;
	}
	else if (format != local_args.data_format) {
	  local_args.data_format="all";
	}
    }
    if (!format_table.found(format,entry)) {
	find_in_ancillary_table(server,database+".formats","format = '"+format+"'",query);
	query.fetch_row(row);
	format_code=row[0];
	entry.key=format;
	entry.code=format_code;
	format_table.insert(entry);
    }
    else {
	format_code=entry.code;
    }
    if (fname.is_mss_file || fname.is_web_file) {
	start="3000-12-31 23:59 +0000";
	end="1000-01-01 00:00 +0000";
	num_grids=0;
	entry.key=fname.name;
	if (!file_table.found(entry.key,entry)) {
	  if (database == "GrML") {
	    files_table_name="GrML.ds"+local_args.dsnum2+"_primaries";
	    files_template_name="GrML.template_primaries";
	    file_ID_type="mss";
	  }
	  else if (database == "WGrML") {
	    files_table_name="WGrML.ds"+local_args.dsnum2+"_webfiles";
	    files_template_name="WGrML.template_webfiles";
	    file_ID_type="web";
	  }
	  query.set("code",files_table_name,file_ID_type+"ID = '"+entry.key+"'");
	  if (query.submit(server) < 0) {
	    error=query.error();
	    if (strutils::contains(error,"doesn't exist")) {
		std::string result;
		if (server.command("create table "+files_table_name+" like "+files_template_name,result) < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+files_table_name,"scm",user,args.args_string);
		}
		if (file_ID_type == "web") {
		  if (server.command("alter table "+files_table_name+" add type char(1) not null default 'D' after webID, add dsid varchar(9) not null default 'ds"+args.dsnum+"' after type, add primary key (webID,type,dsid)",result) < 0) {
		    metautils::log_error("summarize_grml() returned error: "+server.error()+" while modifying "+files_table_name,"scm",user,args.args_string);
		  }
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_levels like "+database+".template_levels",result) < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_levels","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_grids like "+database+".template_grids",result) < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_grids","scm",user,args.args_string);
		}
	    }
	    else {
		metautils::log_error("summarize_grml() returned error: "+error+" while running query "+query.show(),"scm",user,args.args_string);
	    }
	  }
	  if (query.num_rows() == 0) {
	    if (database == "GrML") {
		if (server.insert(files_table_name,"mssID,format_code,num_grids,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while inserting into "+files_table_name,"scm",user,args.args_string);
		}
	    }
	    else if (database == "WGrML") {
		if (server.insert(files_table_name,"webID,format_code,num_grids,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while inserting into "+files_table_name,"scm",user,args.args_string);
		}
	    }
	    query.submit(server);
	    if (query.num_rows() == 0) {
		metautils::log_error("summarize_grml() returned error: unable to retrieve code from "+files_table_name+" for value "+entry.key,"scm",user,args.args_string);
	    }
	  }
	  query.fetch_row(row);
	  entry.code=row[0];
	  file_table.insert(entry);
	}
	file_ID_code=entry.code;
	server._delete(database+".ds"+local_args.dsnum2+"_grids",file_ID_type+"ID_code = "+file_ID_code);
	server._delete(database+".ds"+local_args.dsnum2+"_processes",file_ID_type+"ID_code = "+file_ID_code);
	server._delete(database+".ds"+local_args.dsnum2+"_ensembles",file_ID_type+"ID_code = "+file_ID_code);
	grid_list=xdoc.element_list("GrML/grid");
	my::map<ParameterElement> parameter_element_table(9999);
	for (const auto& grid : grid_list) {
	  time_range=grid.attribute_value("timeRange");
	  se.key=strutils::to_lower(time_range);
	  if (strutils::contains(se.key,"forecast")) {
	    se.key=se.key.substr(0,se.key.find("forecast")+8);
	    sp=strutils::split(se.key);
	    if (sp.size() > 2) {
		se.key=sp[sp.size()-2]+" "+sp[sp.size()-1];
	    }
	  }
	  if (!grid_process_table.found(se.key,se)) {
	    grid_process_table.insert(se);
	  }
	  definition=grid.attribute_value("definition");
	  attribute_list=grid.attribute_list();
	  definition_parameters="";
	  for (const auto& attribute : attribute_list) {
	    aname=attribute.name;
	    if (aname != "timeRange" && aname != "definition") {
		if (!definition_parameters.empty()) {
		  definition_parameters+=":";
		}
		definition_parameters+=grid.attribute_value(aname);
	    }
	  }
	  entry.key=time_range;
	  if (!time_range_table.found(entry.key,entry)) {
	    find_in_ancillary_table(server,database+".timeRanges","timeRange = '"+time_range+"'",query);
	    query.fetch_row(row);
	    time_range_code=row[0];
	    entry.code=time_range_code;
	    time_range_table.insert(entry);
	  }
	  else {
	    time_range_code=entry.code;
	  }
	  entry.key+=":"+definition+":"+definition_parameters;
	  if (!grid_definition_table.found(entry.key,entry)) {
	    find_in_ancillary_table(server,database+".gridDefinitions","definition = '"+definition+"' AND defParams = '"+definition_parameters+"'",query);
	    query.fetch_row(row);
	    grid_definition_code=row[0];
	    entry.code=grid_definition_code;
	    grid_definition_table.insert(entry);
	  }
	  else {
	    grid_definition_code=entry.code;
	  }
	  ensemble_list=grid.element_list("ensemble");
	  nensembles=ensemble_list.size();
	  if (nensembles == 0) {
	    nensembles=1;
	  }
	  grid_elements_list=grid.element_addresses();
	  parameter_element_table.clear();
	  for (const auto& grid_element : grid_elements_list) {
	    sdum=(grid_element.p)->name();
	    if (sdum == "process") {
		if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_processes")) {
		  std::string result;
		  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_processes like "+database+".template_processes",result) < 0) {
		    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_processes","scm",user,args.args_string);
		  }
		}
		if (server.insert(database+".ds"+local_args.dsnum2+"_processes",file_ID_code+","+time_range_code+","+grid_definition_code+",'"+(grid_element.p)->attribute_value("value")+"'") < 0) {
		  error=server.error();
		  if (!strutils::contains(error,"Duplicate entry")) {
		    metautils::log_error("summarize_grml() returned error: "+error+" while inserting into "+database+".ds"+local_args.dsnum2+"_processes","scm",user,args.args_string);
		  }
		}
	    }
	    else if (sdum == "ensemble") {
		if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_ensembles")) {
		  std::string result;
		  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_ensembles like "+database+".template_ensembles",result) < 0) {
		    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_ensembles","scm",user,args.args_string);
		  }
		}
		sdum=(grid_element.p)->attribute_value("size");
		if (sdum.empty()) {
		  sdum="0";
		}
		if (server.insert(database+".ds"+local_args.dsnum2+"_ensembles",file_ID_code+","+time_range_code+","+grid_definition_code+",'"+(grid_element.p)->attribute_value("type")+"','"+(grid_element.p)->attribute_value("ID")+"',"+sdum) < 0) {
		  error=server.error();
		  if (!strutils::contains(error,"Duplicate entry")) {
		    metautils::log_error("summarize_grml() returned error: "+error+" while inserting into "+database+".ds"+local_args.dsnum2+"_ensembles","scm",user,args.args_string);
		  }
		}
	    }
	    else if (sdum == "level" || sdum == "layer") {
		map=(grid_element.p)->attribute_value("map");
		level_type=(grid_element.p)->attribute_value("type");
		value=(grid_element.p)->attribute_value("value");
		if (value.empty()) {
		  value=(grid_element.p)->attribute_value("bottom")+","+(grid_element.p)->attribute_value("top");
		}
		entry.key=map+":"+level_type+":"+value;
		if (!level_type_table.found(entry.key,entry)) {
		  find_in_ancillary_table(server,database+".levels","map = '"+map+"' AND type = '"+level_type+"' AND value = '"+value+"'",query);
		  query.fetch_row(row);
		  level_type_code=row[0];
		  entry.code=level_type_code;
		  level_type_table.insert(entry);
		}
		else {
		  level_type_code=entry.code;
		}
		if (server.insert(database+".ds"+local_args.dsnum2+"_levels",format_code+","+level_type_code) < 0) {
		  error=server.error();
		  if (!strutils::contains(error,"Duplicate entry")) {
		    metautils::log_error("summarize_grml() returned error: "+error+" while inserting into "+database+".ds"+local_args.dsnum2+"_levels","scm",user,args.args_string);
		  }
		}
		else {
		  inserted_unique_level=true;
		}
// parameters
		parameter_list=(grid_element.p)->element_addresses();
		for (const auto& parameter : parameter_list) {
		  map=(parameter.p)->attribute_value("map");
		  value=(parameter.p)->attribute_value("value");
		  nsteps=std::stoi((parameter.p)->attribute_value("nsteps"));
		  num_grids+=nsteps;
		  nsteps/=nensembles;
		  sdum=(parameter.p)->attribute_value("start");
		  if (sdum < start) {
		    start=sdum;
		  }
		  sdum=(parameter.p)->attribute_value("end");
		  if (sdum > end) {
		    end=sdum;
		  }
		  entry.key=map+":"+value+"@"+level_type;
		  if (entry.key[0] == ':') {
		    entry.key=entry.key.substr(1);
		  }
		  if (!parameter_table.found(entry.key,entry)) {
		    sdum=parameter_mapper.description(format,entry.key);
		    strutils::replace_all(sdum,"'","\\'");
		    if (server.insert("search.variables","'"+sdum+"','CMDMAP','"+args.dsnum+"'") < 0) {
			error=server.error();
			if (!strutils::contains(error,"Duplicate entry")) {
			  metautils::log_error("summarize_grml() returned error: "+error+" while inserting into search.variables","scm",user,args.args_string);
			}
		    }
		    else {
			local_args.added_variable=true;
		    }
		    parameter_table.insert(entry);
		  }
		  pstart=(parameter.p)->attribute_value("start").substr(0,16);
		  strutils::replace_all(pstart,"-","");
		  strutils::replace_all(pstart," ","");
		  strutils::replace_all(pstart,":","");
		  pend=(parameter.p)->attribute_value("end").substr(0,16);
		  strutils::replace_all(pend,"-","");
		  strutils::replace_all(pend," ","");
		  strutils::replace_all(pend,":","");
		  if (strutils::contains(entry.key,"@")) {
		    entry.key=entry.key.substr(0,entry.key.find("@"));
		  }
		  entry.key+="<!>"+pstart+"<!>"+pend;
		  ne.key=std::stoi(level_type_code);
		  if (!parameter_element_table.found(entry.key,pe)) {
		    pe.key=entry.key;
		    pe.data.reset(new ParameterElement::Data);
		    pe.data->min_nsteps=pe.data->max_nsteps=nsteps;
		    pe.data->level_codes.insert(ne);
		    pe.data->level_code_list.emplace_back(ne.key);
		    parameter_element_table.insert(pe);
		  }
		  else {
		    if (nsteps < pe.data->min_nsteps) {
			pe.data->min_nsteps=nsteps;
		    }
		    if (nsteps > pe.data->max_nsteps) {
			pe.data->max_nsteps=nsteps;
		    }
		    if (!pe.data->level_codes.found(ne.key,ne)) {
			pe.data->level_codes.insert(ne);
			pe.data->level_code_list.emplace_back(ne.key);
		    }
		  }
		}
	    }
	  }
	  for (const auto& key : parameter_element_table.keys()) {
	    parameter_element_table.found(key,pe);
	    std::sort(pe.data->level_code_list.begin(),pe.data->level_code_list.end(),
	    [](const size_t& left,const size_t& right) -> bool
	    {
		if (left <= right) {
		  return true;
		}
		else {
		  return false;
		}
	    });
	    bitmap::compress_values(pe.data->level_code_list,level_type_codes);
	    sp=strutils::split(pe.key,"<!>");
	    if (server.insert(database+".ds"+local_args.dsnum2+"_grids",file_ID_type+"ID_code,timeRange_code,gridDefinition_code,parameter,levelType_codes,start_date,end_date,min_nsteps,max_nsteps",file_ID_code+","+time_range_code+","+grid_definition_code+",'"+sp[0]+"','"+level_type_codes+"',"+sp[1]+","+sp[2]+","+strutils::itos(pe.data->min_nsteps)+","+strutils::itos(pe.data->max_nsteps),"") < 0) {
		metautils::log_error("summarize_grml() returned error: "+server.error()+" while inserting into "+database+".ds"+local_args.dsnum2+"_grids","scm",user,args.args_string);
	    }
	    pe.data=nullptr;
	  }
	}
	strutils::replace_all(start,"-","");
	strutils::replace_all(start," ","");
	strutils::replace_all(start,":","");
	strutils::replace_all(start,"+0000","");
	strutils::replace_all(end,"-","");
	strutils::replace_all(end," ","");
	strutils::replace_all(end,":","");
	strutils::replace_all(end,"+0000","");
	update_string="num_grids="+strutils::itos(num_grids)+",start_date="+start+",end_date="+end;
	if (server.update(files_table_name,update_string,"code = "+file_ID_code) < 0) {
	  metautils::log_error("summarize_grml() returned error: "+server.error()+" while updating "+files_table_name,"scm",user,args.args_string);
	}
	if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_agrids")) {
	  std::string result;
	  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_agrids like "+database+".template_agrids",result) < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_agrids","scm",user,args.args_string);
	  }
	}
	if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_agrids2")) {
	  std::string result;
	  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_agrids2 like "+database+".template_agrids2",result) < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_agrids2","scm",user,args.args_string);
	  }
	}
	if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_agrids_cache")) {
	  std::string result;
	  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_agrids_cache like "+database+".template_agrids_cache",result) < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_agrids_cache","scm",user,args.args_string);
	  }
	}
	if (!MySQL::table_exists(server,database+".ds"+local_args.dsnum2+"_grid_definitions")) {
	  std::string result;
	  if (server.command("create table "+database+".ds"+local_args.dsnum2+"_grid_definitions like "+database+".template_grid_definitions",result) < 0) {
	    metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_grid_definitions","scm",user,args.args_string);
	  }
	}
	summarizeMetadata::grids::aggregate_grids(args.dsnum,database,"scm",user,args.args_string,file_ID_code);
    }
    xdoc.close();
    if (database == "GrML") {
	summarizeMetadata::summarize_frequencies(args.dsnum,"scm",user,args.args_string,file_ID_code);
	summarizeMetadata::grids::summarize_grid_resolutions(args.dsnum,"scm",user,args.args_string,file_ID_code);
    }
    summarizeMetadata::grids::summarize_grids(args.dsnum,database,"scm",user,args.args_string,file_ID_code);
    if (inserted_unique_level) {
	summarizeMetadata::gridLevels::summarize_grid_levels(args.dsnum,database,"scm",user,args.args_string);
    }
    if (local_args.verbose) {
	tm2=time(NULL);
	std::cout << fname.name << " summarized in " << (tm2-tm1) << " seconds" << std::endl;
    }
    server_d.disconnect();
  }
  server._delete("search.data_types","dsid = '"+args.dsnum+"' and vocabulary = 'dssmm'");
  for (const auto& key : grid_process_table.keys()) {
    if (server.insert("search.data_types","'grid','"+key+"','"+database+"','"+args.dsnum+"'") < 0) {
	error=server.error();
	if (!strutils::contains(error,"Duplicate entry")) {
	  metautils::log_error("summarize_grml() returned error: "+error+" while inserting into search.data_types","scm",user,args.args_string);
	}
    }
  }
}

/*
void fill_obs_per(std::string observationTypeValue,size_t numObs,DateTime start,DateTime end,double& obsper,std::string& unit)
{
  size_t num_days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  --numObs;
  if (numObs < 2) {
    unit="";
    return;
  }
  if (strutils::contains(observationTypeValue,"climatology")) {
    auto idx=observationTypeValue.find("year")-1;
    while (idx != std::string::npos && observationTypeValue[idx] != '_') {
	--idx;
    }
    auto len_s=observationTypeValue.substr(idx+1);
    len_s=len_s.substr(0,len_s.find("-year"));
    auto climo_len=std::stoi(len_s);
    auto nyrs=end.year()-start.year();
    if (nyrs >= climo_len) {
	int nper;
	if (climo_len == 30) {
	  nper=((nyrs % climo_len)/10)+1;
	}
	else {
	  nper=nyrs/climo_len;
	}
	obsper=numObs/static_cast<float>(nper);
	if (static_cast<int>(numObs) == nper) {
	  unit="year";
	}
	else if (numObs/nper == 4) {
	  unit="season";
	}
	else if (numObs/nper == 12) {
	  unit="month";
	}
	else if (nper > 1 && start.month() != end.month()) {
	  nper=(nper-1)*12+12-start.month()+1;
	  if (static_cast<int>(numObs) == nper) {
	    unit="month";
	  }
	  else {
	    unit="";
	  }
	}
	else {
	  unit="";
	}
    }
    else {
	if ((end.month()-start.month()) == static_cast<int>(numObs)) {
	  unit="month";
	}
	else {
	  unit="";
	}
    }
    obsper=-climo_len;
  }
  else {
    if (is_leap_year(end.year())) {
	num_days[2]=29;
    }
    obsper=numObs/static_cast<float>(end.seconds_since(start));
    const double TOLERANCE=0.15;
    if ((obsper+TOLERANCE) > 1.) {
	unit="second";
    }
    else {
	obsper*=60.;
	if ((obsper+TOLERANCE) > 1.) {
	  unit="minute";
	}
	else {
	  obsper*=60;
	  if ((obsper+TOLERANCE) > 1.) {
	    unit="hour";
	  }
	  else {
	    obsper*=24;
	    if ((obsper+TOLERANCE) > 1.) {
		unit="day";
	    }
	    else {
		obsper*=7;
		if ((obsper+TOLERANCE) > 1.) {
		  unit="week";
		}
		else {
		  obsper*=(num_days[end.month()]/7.);
		  if ((obsper+TOLERANCE) > 1.) {
		    unit="month";
		  }
		  else {
		    obsper*=12;
		    if ((obsper+TOLERANCE) > 1.) {
			unit="year";
		    }
		    else {
			unit="";
		    }
		  }
		}
	    }
	  }
	}
    }
    num_days[2]=28;
  }
}
*/

extern "C" void *summarize_IDs(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;
  MySQL::LocalQuery query;
  MySQL::Row row;

  MySQL::Server srv;
  metautils::connect_to_metadata_server(srv);
  if (!srv) {
    metautils::log_error("summarize_IDs() could not connect to mysql server - error: "+srv.error(),"scm",user,args.args_string);
  }
//  frequency_table.clear();
// read in the IDs from the separate XML file
  auto sdum=remote_web_file("https://rda.ucar.edu"+t->strings[0]+t->strings[1],temp_dir.name());
  ifstream ifs;
  ifs.open(sdum.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("summarize_IDs() returned error: unable to open "+t->strings[0]+t->strings[1],"scm",user,args.args_string);
  }
  char line[32768];
  double avg_per_day=0.,avg_day_count=0.,avg_per_month=0.,avg_month_count=0.;
  size_t obs_count=0;
  my::map<summarizeMetadata::Entry> ID_type_table;
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (line[2] == '<' && line[3] == 'I' && line[4] == 'D') {
	sdum=line;
	while (line[3] != '/') {
	  ifs.getline(line,32768);
	  sdum+=line;
	}
	XMLSnippet snippet=sdum;
	auto e=snippet.element("ID");
	summarizeMetadata::Entry entry;
	entry.key=e.attribute_value("type");
	std::string IDType_code;
	if (!ID_type_table.found(entry.key,entry)) {
	  find_in_ancillary_table(srv,t->strings[4]+".IDTypes","IDType = '"+entry.key+"'",query);
	  query.fetch_row(row);
	  IDType_code=row[0];
	  entry.code=IDType_code;
	  ID_type_table.insert(entry);
	}
	else {
	  IDType_code=entry.code;
	}
	auto ID=e.attribute_value("value");
	strutils::replace_all(ID,"\\","\\\\");
	strutils::replace_all(ID,"'","\\'");
	strutils::replace_all(ID,"&quot;","\"");
	strutils::replace_all(ID," = "," &eq; ");
	sdum=e.attribute_value("lat");
	std::string sw_lat,sw_lon,ne_lat,ne_lon;
	if (!sdum.empty()) {
	  sw_lat=strutils::ftos(std::stof(sdum)*10000.,0);
	  sw_lon=strutils::ftos(std::stof(e.attribute_value("lon"))*10000.,0);
	  ne_lat=sw_lat;
	  ne_lon=sw_lon;
	}
	else {
	  sdum=e.attribute_value("cornerSW");
	  auto sp=strutils::split(sdum,",");
	  if (sp.size() != 2) {
	    metautils::log_error("summarize_IDs() returned error in cornerSW attribute for file code "+file_ID_code+", '"+t->strings[1]+"', '"+sdum+"'","scm",user,args.args_string);
	  }
	  sw_lat=metadata::ObML::string_coordinate_to_db(sp[0]);
	  sw_lon=metadata::ObML::string_coordinate_to_db(sp[1]);
	  sdum=e.attribute_value("cornerNE");
	  sp=strutils::split(sdum,",");
	  if (sp.size() != 2) {
	    metautils::log_error("summarize_IDs() returned error in cornerNE attribute for file code "+file_ID_code,"scm",user,args.args_string);
	  }
	  ne_lat=metadata::ObML::string_coordinate_to_db(sp[0]);
	  ne_lon=metadata::ObML::string_coordinate_to_db(sp[1]);
	}
	if (sw_lon == "-9990000") {
	  sw_lon="-8388608";
 	}
	if (ne_lon == "-9990000") {
	  ne_lon="-8388608";
	}
	find_in_ancillary_table(srv,t->strings[4]+".ds"+local_args.dsnum2+"_IDs2","IDType_code = "+IDType_code+" AND ID = '"+ID+"' AND sw_lat = "+sw_lat+" AND sw_lon = "+sw_lon+" AND ne_lat = "+ne_lat+" AND ne_lon = "+ne_lon,query);
	query.fetch_row(row);
	auto ID_code=row[0];
	sdum=e.attribute_value("start");
	strutils::replace_all(sdum,"-","");
	while (strutils::occurs(sdum," ") > 1) {
	  sdum=sdum.substr(0,sdum.rfind(" "));
	}
	strutils::replace_all(sdum," ","");
	strutils::replace_all(sdum,":","");
	while (sdum.length() < 14) {
	  sdum+="99";
	}
	DateTime ostart(std::stoll(sdum));
	sdum=e.attribute_value("end");
	strutils::replace_all(sdum,"-","");
	std::string tz="0";
	while (strutils::occurs(sdum," ") > 1) {
	  auto idx=sdum.rfind(" ");
	  tz=sdum.substr(idx+1);
	  if (tz[0] == '+') {
	    tz=tz.substr(1);
	  }
	  if (tz == "LST") {
	    tz="-2400";
	  }
	  else if (tz == "LT") {
	    tz="2400";
	  }
	  sdum=sdum.substr(0,idx);
	}
	strutils::replace_all(sdum," ","");
	strutils::replace_all(sdum,":","");
	DateTime oend;
	if (sdum.length() == 8) {
	  oend.set(std::stoll(sdum+"999999"));
	}
	else {
	  while (sdum.length() < 14) {
	    sdum+="99";
	  }
	  oend.set(std::stoll(sdum));
	}
	auto num_obs=e.attribute_value("numObs");
	if (srv.insert(t->strings[4]+".ds"+local_args.dsnum2+"_IDList2","ID_code,observationType_code,platformType_code,"+t->strings[5]+"ID_code,num_observations,start_date,end_date,time_zone",ID_code+","+t->strings[2]+","+t->strings[3]+","+file_ID_code+","+num_obs+","+ostart.to_string("%Y%m%d%H%MM%SS")+","+oend.to_string("%Y%m%d%H%MM%SS")+","+tz,"") < 0) {
	  auto error=srv.error();
	  if (!local_args.refresh_only || !strutils::contains(error,"Duplicate entry")) {
	    metautils::log_error("summarize_IDs() returned "+error+" while inserting '"+ID_code+","+t->strings[2]+","+t->strings[3]+","+file_ID_code+","+num_obs+","+ostart.to_string("%Y%m%d%H%MM%SS")+","+oend.to_string("%Y%m%d%H%MM%SS")+","+tz+"' into "+t->strings[4]+".ds"+local_args.dsnum2+"_IDList2","scm",user,args.args_string);
	  }
	}
	auto elist=e.element_list("dataType");
	for (const auto& element : elist) {
	  if (oend != ostart || (ostart.time() == 999999 && oend.time() == 999999)) {
	    auto num_obs_data_type=element.attribute_value("numObs");
// backward compatibility for content metadata that was generated before the
//   number of observations by data type was captured
	    if (num_obs_data_type.empty()) {
		num_obs_data_type=num_obs;
	    }
	    auto nobs=std::stoi(num_obs_data_type);
	    if (nobs > 1) {
		if (oend.time() == 999999) {
		  oend.add_days(1);
		}
		double nsince;
		nsince=oend.days_since(ostart);
		if (nsince == 0 && oend.seconds_since(ostart) > 0) {
		  nsince=1;
		}
		if (nsince > 0) {
		  avg_per_day+=nobs/nsince;
		  ++avg_day_count;
		  obs_count+=nobs;
		}
		nsince=oend.months_since(ostart);
		if (nsince > 0) {
		  avg_per_month+=nobs/nsince;
		  ++avg_month_count;
		}
	    }
	  }
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  if (avg_day_count > 0.) {
    avg_per_day/=avg_day_count;
  }
  if (avg_month_count > 0.) {
    avg_per_month/=avg_month_count;
  }
//  srv._delete(t->strings[4]+".ds"+local_args.dsnum2+"_frequencies",file_ID_type+"ID_code = "+file_ID_code+" and observationType_code = "+t->strings[2]+" and platformType_code = "+t->strings[3]);
  std::string unit;
  if (lround(avg_per_day) >= 1) {
    avg_per_day/=24.;
    if (lround(avg_per_day) >= 1) {
	avg_per_day/=60.;
	if (lround(avg_per_day) >= 1) {
	  avg_per_day/=60.;
	  if (lround(avg_per_day) >= 1) {
	    unit="second";
	  }
	  else {
	    avg_per_day*=60.;
	    unit="minute";
	  }
	}
	else {
	  avg_per_day*=60.;
	  unit="hour";
	}
    }
    else {
	avg_per_day*=24.;
	unit="day";
    }
  }
  else {
    avg_per_day*=7.;
    if (lround(avg_per_day) >= 1) {
	unit="week";
    }
    else {
	if (lround(avg_per_month) >= 1) {
	  unit="month";
	}
	else {
	  avg_per_month*=12.;
	  if (lround(avg_per_month) >= 1) {
	    unit="year";
	  }
	  else {
	    avg_per_month*=10.;
	    if (lround(avg_per_month) >= 1) {
		unit="decade";
	    }
	  }
	}
    }
  }
  std::string obs_per;
  if (lround(avg_per_day) >= 1) {
    obs_per=strutils::itos(lround(avg_per_day));
    sdum=time_resolution_keyword("irregular",lround(avg_per_day),unit,"");
  }
  else {
    obs_per=strutils::itos(lround(avg_per_month));
    sdum=time_resolution_keyword("irregular",lround(avg_per_month),unit,"");
  }
  std::string result;
  if (obs_per != "0" && srv.command("insert into "+t->strings[4]+".ds"+local_args.dsnum2+"_frequencies values ("+file_ID_code+","+t->strings[2]+","+t->strings[3]+","+obs_per+","+strutils::itos(obs_count)+",'"+unit+"','"+uflag+"') on duplicate key update avg_obs_per=values(avg_obs_per),total_obs=values(total_obs),uflag=values(uflag)",result) < 0) {
    auto error=srv.error();
    if (!local_args.refresh_only || !strutils::contains(error,"Duplicate entry")) {
	metautils::log_error("summarize_IDs() returned error: '"+error+"' while trying to insert into "+t->strings[4]+".ds"+local_args.dsnum2+"_frequencies ("+file_ID_code+","+t->strings[2]+","+t->strings[3]+","+obs_per+","+strutils::itos(obs_count)+",'"+unit+"')","scm",user,args.args_string);
    }
  }
  if (t->strings[4] == "ObML" && srv.command("insert into ObML.ds"+local_args.dsnum2+"_geobounds (select i2.mssID_code,min(i.sw_lat),min(i.sw_lon),max(i.ne_lat),max(i.ne_lon) from ObML.ds"+local_args.dsnum2+"_IDList2 as i2 left join ObML.ds"+local_args.dsnum2+"_IDs2 as i on i.code = i2.ID_code where i2.mssID_code = "+file_ID_code+" and i.sw_lat > -990000 and i.sw_lon > -1810000 and i.ne_lat < 990000 and i.ne_lon < 1810000) on duplicate key update min_lat = values(min_lat), max_lat = values(max_lat), min_lon = values(min_lon), max_lon = values(max_lon)",result) < 0) {
    metautils::log_error("summarize_IDs() returned error: '"+srv.error()+"' while trying to insert into "+t->strings[4]+".ds"+local_args.dsnum2+"_geobounds for mssID_code = "+file_ID_code,"scm",user,args.args_string);
  }
  srv.disconnect();
  return NULL;
}

struct LocationEntry {
  LocationEntry() : key(),location_list(nullptr) {}

  std::string key;
  std::shared_ptr<std::list<std::string>> location_list;
};

extern "C" void *summarize_file_ID_locations(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;
  XMLDocument rdoc;
  my::map<summarizeMetadata::Entry> location_table;
  std::list<std::string> location_table_keys;
  size_t min_box1d_row,max_box1d_row;
  std::list<XMLElement> reference_list;
  std::string bitmap,db_bitmap,insert_string,rdocname;
  size_t n,m,l;
  static my::map<LocationEntry> locations_by_point;
  MySQL::Query query;
  MySQL::Row row;
  std::vector<std::string> array;
  std::string sdum,error;
  LocationEntry le;
  summarizeMetadata::Entry e;
  my::map<summarizeMetadata::obsData::ParentLocation> parent_location_table;
  summarizeMetadata::obsData::ParentLocation pl,pl2;
  MySQL::Server srv;

  metautils::connect_to_metadata_server(srv);
  if (!srv)
    metautils::log_error("summarize_file_ID_locations() could not connect to mysql server - error: "+srv.error(),"scm",user,args.args_string);
  if (locations_by_point.size() == 0) {
    query.set("box1d_row,box1d_column,keyword","search.locations_by_point");
    if (query.submit(srv) < 0)
	metautils::log_error("summarize_file_ID_locations() returned error: "+query.error(),"scm",user,args.args_string);
    while (query.fetch_row(row)) {
	le.key=row["box1d_row"]+","+row["box1d_column"];
	if (!locations_by_point.found(le.key,le)) {
	  le.location_list.reset(new std::list<std::string>);
	  locations_by_point.insert(le);
	}
	le.location_list->emplace_back(row["keyword"]);
    }
  }
  rdocname=remote_web_file("https://rda.ucar.edu"+t->strings[0]+t->strings[1],temp_dir.name());
  if (rdoc.open(rdocname)) {
    location_table.clear();
    location_table_keys.clear();
    min_box1d_row=999;
    reference_list=rdoc.element_list("locations/box1d");
    for (const auto& reference : reference_list) {
	bitmap=reference.attribute_value("bitmap");
	if (bitmap.length() == 360) {
	  db_bitmap="";
	  for (n=0; n < 360; n+=3) {
	    l=0;
	    for (m=0; m < 3; m++) {
		if (bitmap[n+m] == '1') {
		  if (locations_by_point.found(reference.attribute_value("row")+","+strutils::itos(n+m),le)) {
		    for (const auto& key : location_table_keys) {
			e.key=key;
			if (!location_table.found(e.key,e)) {
			  location_table.insert(e);
			  location_table_keys.emplace_back(e.key);
			}
		    }
		  }
		  l+=(size_t)pow(2.,(int)(2-m));
		}
	    }
	    db_bitmap+=strutils::itos(l);
	  }
	}
	else {
	  db_bitmap=bitmap;
	}
	insert_string=file_ID_code+","+t->strings[2];
	if (!t->strings[3].empty()) {
	  insert_string+=","+t->strings[3];
	}
	insert_string+=","+t->strings[4]+","+t->strings[5]+","+reference.attribute_value("row")+",'"+db_bitmap+"'";
	if (srv.insert(t->strings[6]+".ds"+local_args.dsnum2+"_locations",insert_string) < 0) {
	  error=srv.error();
	  if (!local_args.refresh_only || !strutils::contains(error,"Duplicate entry")) {
	    metautils::log_error("summarize_file_ID_locations() returned error: '"+error+"' while trying to insert into "+t->strings[6]+".ds"+local_args.dsnum2+"_locations ("+insert_string+") into "+t->strings[6]+".ds"+local_args.dsnum2+"_locations","scm",user,args.args_string);
	  }
	}
	m=std::stoi(reference.attribute_value("row"));
	if (min_box1d_row == 999) {
	  min_box1d_row=m;
	  max_box1d_row=min_box1d_row;
	}
	else {
	  if (m < min_box1d_row) {
	    min_box1d_row=m;
	  }
	  if (m > max_box1d_row) {
	    max_box1d_row=m;
	  }
	}
    }
    if (location_table_keys.size() > 0) {
      compress_locations(location_table_keys,parent_location_table,array,"scm",user,args.args_string);
	if (t->strings[6] == "ObML") {
	  srv._delete("ObML.ds"+local_args.dsnum2+"_location_names","mssID_code = "+file_ID_code+" and observationType_code = "+t->strings[2]+" and platformType_code = "+t->strings[3]);
	}
	else if (t->strings[6] == "WObML") {
	  srv._delete("WObML.ds"+local_args.dsnum2+"_location_names","webID_code = "+file_ID_code+" and observationType_code = "+t->strings[2]+" and platformType_code = "+t->strings[3]);
	}
	else if (t->strings[6] == "FixML") {
	  srv._delete("FixML.ds"+local_args.dsnum2+"_location_names","mssID_code = "+file_ID_code+" and classification_code = "+t->strings[2]);
	}
	else if (t->strings[6] == "WFixML") {
	  srv._delete("WFixML.ds"+local_args.dsnum2+"_location_names","webID_code = "+file_ID_code+" and classification_code = "+t->strings[2]);
	}
	for (n=0; n < array.size(); ++n) {
	  parent_location_table.found(array[n],pl);
	  if (pl.matched_table != NULL) {
	    if (pl.matched_table->size() > (pl.children_table->size()/2)) {
		strutils::replace_all(pl.key,"'","\\'");
		if (!t->strings[3].empty()) {
		  insert_string=file_ID_code+","+t->strings[2]+","+t->strings[3]+",'"+pl.key+"','Y'";
		}
		else {
		  insert_string=file_ID_code+","+t->strings[2]+",'"+pl.key+"','Y'";
		}
		if (srv.insert(t->strings[6]+".ds"+local_args.dsnum2+"_location_names",insert_string) < 0) {
		  metautils::log_error("summarize_file_ID_locations() returned error: "+srv.error()+" while inserting '"+insert_string+"' into "+t->strings[6]+".ds"+local_args.dsnum2+"_location_names","scm",user,args.args_string);
		}
		for (const auto& key : pl.children_table->keys()) {
		  e.key=key;
		  strutils::replace_all(e.key,"'","\\'");
		  if (!pl.matched_table->found(e.key,e) && !pl.consolidated_parent_table->found(e.key,e)) {
		    if (!t->strings[3].empty()) {
			insert_string=file_ID_code+","+t->strings[2]+","+t->strings[3]+",'"+e.key+"','N'";
		    }
		    else {
			insert_string=file_ID_code+","+t->strings[2]+",'"+e.key+"','N'";
		    }
		    if (srv.insert(t->strings[6]+".ds"+local_args.dsnum2+"_location_names",insert_string) < 0) {
			metautils::log_error("summarize_file_ID_locations() returned error: "+srv.error()+" while inserting '"+insert_string+"' into "+t->strings[6]+".ds"+local_args.dsnum2+"_location_names","scm",user,args.args_string);
		    }
		  }
		}
	    }
	    else {
		for (const auto& key : pl.matched_table->keys()) {
		  sdum=key;
		  strutils::replace_all(sdum,"'","\\'");
		  if (!t->strings[3].empty()) {
		    insert_string=file_ID_code+","+t->strings[2]+","+t->strings[3]+",'"+sdum+"','Y'";
		  }
		  else {
		    insert_string=file_ID_code+","+t->strings[2]+",'"+sdum+"','Y'";
		  }
		  if (srv.insert(t->strings[6]+".ds"+local_args.dsnum2+"_location_names",insert_string) < 0) {
		    metautils::log_error("summarize_file_ID_locations() returned error: "+srv.error()+" while inserting '"+insert_string+"' into "+t->strings[6]+".ds"+local_args.dsnum2+"_location_names","scm",user,args.args_string);
		  }
		}
	    }
	    pl.matched_table=nullptr;
	  }
	  pl.children_table=nullptr;
	  pl.consolidated_parent_table=nullptr;
	}
    }
    rdoc.close();
  }
  else {
    metautils::log_error("summarize_file_ID_locations() unable to open referenced XML document '"+rdocname+"'","scm",user,args.args_string);
  }
  srv.disconnect();
  return NULL;
}

extern "C" void *create_kml(void *ts)
{
  ofstream ofs;
//  ThreadStruct *t=(ThreadStruct *)ts;
  MySQL::Server srv;
  MySQL::Query query;
  MySQL::Row row;
  std::string tz,units,date,time;
/*
  double sw_lat,sw_lon,ne_lat,ne_lon,obsper;
  char sw_lat_hemi,sw_lon_hemi,ne_lat_hemi,ne_lon_hemi;
*/
  DateTime start,end;

/*
  metautils::connect_to_metadata_server(srv);
  if (!srv.isConnected())
    metautils::log_error("create_kml could not connect to mysql server - error: "+srv.error(),"scm",user,args.args_string);
  query.set("select t.IDType,i.ID,min(i.sw_lat),min(i.sw_lon),max(i.ne_lat),max(i.ne_lon),min(l.sd),max(l.ed),min(l.tz),sum(l.nobs) from (select ID_code,min(start_date) sd,max(end_date) ed,min(time_zone) tz,sum(num_observations) nobs from ObML.ds"+local_args.dsnum2+"_IDList2 where observationType_code = "+t->strings[2]+" and platformType_code = "+t->strings[3]+" group by ID_code) as l left join ObML.ds"+local_args.dsnum2+"_IDs2 as i on i.code = l.ID_code left join ObML.IDTypes as t on t.code = i.idType_code group by t.IDType,i.ID");
  if (query.submit(srv) < 0)
    metautils::log_error("create_kml returned error: '"+query.error()+"' for query '"+query.show()+"'","scm",user,args.args_string);
  ofs.open((directives.httpRoot+"/md/datasets/ds"+args.dsnum+"/"+t->strings[0]+"."+t->strings[1]+".kml").toChar());
  if (!ofs)
    metautils::log_error("create_kml returned error: unable to open "+directives.httpRoot+"/md/datasets/ds"+args.dsnum+"/"+t->strings[0]+"."+t->strings[1]+".kml","scm",user,args.args_string);
  ofs.setf(ios::fixed);
  ofs.precision(3);
  ofs << "<?xml version=\"1.0\" ?>" << std::endl;
  ofs << "<kml xmlns=\"http://www.opengis.net/kml/2.2\">" << std::endl;
  ofs << "<source>http://rda.ucar.edu/md/datasets/ds" << args.dsnum << "/" << t->strings[0] << "." << t->strings[1] << ".kml</source>" << std::endl;
  ofs << "<Document>" << std::endl;
  ofs << "<Style id=\"fixed_station0\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/purple-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"fixed_station1\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/pink-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"fixed_station2\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/red-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"fixed_station3\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/orange-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"fixed_station4\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/green-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"fixed_station5\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/blue-dot.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station1b\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/pink.png</href></Icon></IconStyle><PolyStyle><color>5f9ef4e1</color><fill>1</fill><outline>1</outline></PolyStyle><LineStyle><color>ff000000</color><width>1</width></LineStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station2b\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/red.png</href></Icon></IconStyle><PolyStyle><color>5f0000ff</color><fill>1</fill><outline>1</outline></PolyStyle><LineStyle><color>ff000000</color><width>1</width></LineStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station3b\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/orange.png</href></Icon></IconStyle><PolyStyle><color>5f007fff</color><fill>1</fill><outline>1</outline></PolyStyle><LineStyle><color>ff000000</color><width>1</width></LineStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station4b\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/green.png</href></Icon></IconStyle><PolyStyle><color>5f00ff00</color><fill>1</fill><outline>1</outline></PolyStyle><LineStyle><color>ff000000</color><width>1</width></LineStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station5b\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/blue.png</href></Icon></IconStyle><PolyStyle><color>5fff0000</color><fill>1</fill><outline>1</outline></PolyStyle><LineStyle><color>ff000000</color><width>1</width></LineStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station1n\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/pink.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station2n\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/red.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station3n\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/orange.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station4n\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/green.png</href></Icon></IconStyle></Style>" << std::endl;
  ofs << "<Style id=\"moved_station5n\"><IconStyle><hotSpot x=\"0.5\" y=\"0.\" xunits=\"fraction\" yunits=\"fraction\" /><Icon><href>http://rda.ucar.edu/images/gmaps/blue.png</href></Icon></IconStyle></Style>" << std::endl;
  while (query.getCurrentResult(row) == 0) {
    sw_lat=atof(row[2].toChar())/10000.;
    sw_lon=atof(row[3].toChar())/10000.;
    ne_lat=atof(row.getColumn(4).toChar())/10000.;
    ne_lon=atof(row.getColumn(5).toChar())/10000.;
    tz=row.getColumn(8);
    if (!tz.beginsWith("-"))
	tz="+"+tz;
    if (tz == "-2400")
	tz="LST";
    else if (tz == "+2400")
	tz="LT";
    else {
	while (tz.length() < 5)
	  tz+="0";
    }
    date=row.getColumn(6).substr(0,8);
    time=row.getColumn(6).substr(8);
    time.replace("99","00");
    start.set((long long)atoll((date+time).toChar()));
    while (row.getColumn(6).length() > 8 && row.getColumn(6).endsWith("99"))
	row.getColumn(6).chop(2);
    date=row.getColumn(7).substr(0,8);
    time=row.getColumn(7).substr(8);
    time.replace("99","00");
    end.set((long long)atoll((date+time).toChar()));
    while (row.getColumn(7).length() > 8 && row.getColumn(7).endsWith("99"))
	row.getColumn(7).chop(2);
    if (start == end)
	obsper=0.;
    else
	fill_obs_per("",atoi(row.getColumn(9).toChar()),start,end,obsper,units);
    if (sw_lat < 0.)
	sw_lat_hemi='S';
    else
	sw_lat_hemi='N';
    if (sw_lon < 0.)
	sw_lon_hemi='W';
    else
	sw_lon_hemi='E';
    if (row[2] != row.getColumn(4) || row[3] != row.getColumn(5)) {
	if (ne_lat < 0.)
	  ne_lat_hemi='S';
	else
	  ne_lat_hemi='N';
	if (ne_lon < 0.)
	  ne_lon_hemi='W';
	else
	  ne_lon_hemi='E';
	if ((ne_lat-sw_lat) < 10. && (ne_lon-sw_lon) < 10.) {
	  ofs << "<Placemark><styleUrl>#moved_station";
	  if (units == "second" || units == "minute")
	    ofs << "1b";
	  else if (units == "hour")
	    ofs << "2b";
	  else if (units == "day")
	    ofs << "3b";
	  else if (units == "week")
	    ofs << "4b";
	  else
	    ofs << "5b";
	  ofs << "</styleUrl><name>" << row[0].capitalized() << ": " << row[1] << "</name><description><![CDATA[Bounding box: " << fabs(sw_lat) << sw_lat_hemi << "," << fabs(sw_lon) << sw_lon_hemi << " to " << fabs(ne_lat) << ne_lat_hemi << "," << fabs(ne_lon) << ne_lon_hemi << "<br>Data period: " << summarizeMetadata::LLToDateString(row.getColumn(6));
	  if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	    ofs << " " << tz;
	  ofs << " to " << summarizeMetadata::LLToDateString(row.getColumn(7));
	  if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	    ofs << " " << tz;
	  ofs << "<br>Total number of observations: " << row.getColumn(9);
	  if (obsper > 0.)
	    ofs << " (~" << lround(obsper) << " per " << units << ")";
	  ofs << "]]></description><MultiGeometry><Point><coordinates>" << (sw_lon+ne_lon)/2. << "," << (sw_lat+ne_lat)/2. << ",0</coordinates></Point><Polygon><outerBoundaryIs><LinearRing><coordinates>" << std::endl;
	  ofs << sw_lon << "," << sw_lat << ",0" << std::endl;
	  ofs << sw_lon << "," << ne_lat << ",0" << std::endl;
	  ofs << ne_lon << "," << ne_lat << ",0" << std::endl;
	  ofs << ne_lon << "," << sw_lat << ",0" << std::endl;
	  ofs << sw_lon << "," << sw_lat << ",0" << std::endl;
	  ofs << "</coordinates></LinearRing></outerBoundaryIs></Polygon></MultiGeometry></Placemark>" << std::endl;
	}
	else {
	  ofs << "<Placemark><styleUrl>#moved_station";
	  if (units == "second" || units == "minute")
	    ofs << "1n";
	  else if (units == "hour")
	    ofs << "2n";
	  else if (units == "day")
	    ofs << "3n";
	  else if (units == "week")
	    ofs << "4n";
	  else
	    ofs << "5n";
	  ofs << "</styleUrl><name>" << row[0].capitalized() << ": " << row[1] << "</name><description><![CDATA[Bounding box: " << fabs(sw_lat) << sw_lat_hemi << "," << fabs(sw_lon) << sw_lon_hemi << " to " << fabs(ne_lat) << ne_lat_hemi << "," << fabs(ne_lon) << ne_lon_hemi << "<br>Data period: " << summarizeMetadata::LLToDateString(row.getColumn(6));
	  if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	    ofs << " " << tz;
	  ofs << " to " << summarizeMetadata::LLToDateString(row.getColumn(7));
	  if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	    ofs << " " << tz;
	  ofs << "<br>Total number of observations: " << row.getColumn(9);
	  if (obsper > 0.)
	    ofs << " (~" << lround(obsper) << " per " << units << ")";
	  ofs << "]]></description><Point><coordinates>" << (sw_lon+ne_lon)/2. << "," << (sw_lat+ne_lat)/2. << ",0</coordinates></Point></Placemark>" << std::endl;
	}
    }
    else {
	ofs << "<Placemark><styleUrl>#fixed_station";
	if (row.getColumn(9) == "1")
	  ofs << "0";
	else if (units == "second" || units == "minute")
	  ofs << "1";
	else if (units == "hour")
	  ofs << "2";
	else if (units == "day")
	  ofs << "3";
	else if (units == "week")
	  ofs << "4";
	else
	  ofs << "5";
	ofs << "</styleUrl><name>" << row[0].capitalized() << ": " << row[1] << "</name><description><![CDATA[Location: " << fabs(sw_lat) << sw_lat_hemi << "," << fabs(sw_lon) << sw_lon_hemi << "<br>Data period: " << summarizeMetadata::LLToDateString(row.getColumn(6));
	if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	  ofs << " " << tz;
	ofs << " to " << summarizeMetadata::LLToDateString(row.getColumn(7));
	if (row.getColumn(6).length() > 8 || row.getColumn(7).length() > 8)
	  ofs << " " << tz;
	ofs << "<br>Total number of observations: " << row.getColumn(9);
	if (obsper > 0.)
	  ofs << " (~" << lround(obsper) << " per " << units << ")";
	ofs << "]]></description><Point><coordinates>" << sw_lon << "," << sw_lat << ",0</coordinates></Point></Placemark>" << std::endl;
    }
  }
  ofs << "</Document>" << std::endl;
  ofs << "</kml>" << std::endl;
  ofs.close();
*/
/*
    putLocalFile(directives.web_server,directives.httpRoot+"/md/datasets/ds"+args.dsnum,ofile->getName(),directives.dssRoot);
    p=popen(("wget -q -O - --post-data=\"authKey=qGNlKijgo9DJ7MN&cmd=rename&value="+directives.httpRoot+"/md/datasets/ds"+args.dsnum+"/"+ofile->getName().substitute(temp_dir.getName(),"")+"&newfile="+directives.httpRoot+"/md/datasets/ds"+args.dsnum+"/"+t->strings[0]+"."+t->strings[1]+".kml\" "+utils::remoteRDAServerUtilsURL+" 2> /dev/null").toChar(),"r");
    pclose(p);
    p=popen(("wget -q -O - --post-data=\"authKey=qGNlKijgo9DJ7MN&cmd=chmod664&value="+directives.httpRoot+"/md/datasets/ds"+args.dsnum+"/"+t->strings[0]+"."+t->strings[1]+".kml\" "+utils::remoteRDAServerUtilsURL+" 2> /dev/null").toChar(),"r");
    pclose(p);
*/
/*
  if (local_args.verbose)
    std::cout << "create_kml for '" << t->strings[0] << "' / '" << t->strings[1] << "' finished at " << current_date_time().to_string() << std::endl;
  srv.disconnect();
*/
  return NULL;
}

struct KMLData {
  KMLData() : observation_type(),platform_type(),observation_type_code(),platform_type_code() {}

  std::string observation_type,platform_type;
  std::string observation_type_code,platform_type_code;
};
struct DataTypeEntry {
  DataTypeEntry() : key(),code(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> code;
};
void summarize_obml(std::list<KMLData>& kml_list)
{
  XMLDocument xdoc;
  XMLElement e,e2;
  std::list<XMLElement> obs_list,data_type_list;
  std::list<XMLElementAddress> platform_list,platform_elements;
  std::string observation_type_code,platform_type_code;
  MySQL::LocalQuery query,query2;
  MySQL::Row row,row2;
  my::map<summarizeMetadata::Entry> obs_type_table,platform_type_table;
  summarizeMetadata::Entry entry;
  std::string where_conditions,path,error,sdum,sdum2,format,database;
  ThreadStruct thread_IDs,thread_locations;
  my::map<StringEntry> kml_table;
  StringEntry se;
  KMLData kmld;
  MySQL::Server server_d;
  my::map<DataTypeEntry> datatypes_table;
  DataTypeEntry dte;

  if (obml_file_list.size() == 0) {
    return;
  }
  for (auto& fname : obml_file_list) {
    path=fname.path.substr(0,fname.path.rfind("/")+1);
    open_xml_file(xdoc,fname.path);
    metautils::connect_to_rdadb_server(server_d);
    if (!server_d) {
      metautils::log_error("summarize_obml() could not connect to mysql server - error: "+server_d.error(),"scm",user,args.args_string);
    }
    e=xdoc.element("ObML");
    fname.name=e.attribute_value("uri");
    if (std::regex_search(fname.name,std::regex("^file://MSS:(/FS){0,1}/DSS"))) {
	strutils::replace_all(fname.name,"file://MSS:","");
	query2.set("gindex","dssdb.mssfile","mssfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_mss_file=true;
	database="ObML";
	local_args.summarized_hpss_file=true;
	auto sp=strutils::split(fname.name,"..m..");
	if (sp.size() > 1) {
	  if (server_d.update("htarfile","meta_link = 'Ob'","dsid = 'ds"+args.dsnum+"' and hfile = '"+sp[1]+"'") < 0) {
	    metautils::log_error("summarize_obml() returned error: '"+server.error()+"' while trying to update htarfile","scm",user,args.args_string);
	  }
	}
	else {
	  if (server_d.update("mssfile","meta_link = 'Ob'","dsid = 'ds"+args.dsnum+"' and mssfile = '"+fname.name+"'") < 0) {
	    metautils::log_error("summarize_obml() returned error: '"+server.error()+"' while trying to update mssfile","scm",user,args.args_string);
	  }
	}
    }
    else if (std::regex_search(fname.name,std::regex("^http(s){0,1}://rda\\.ucar\\.edu"))) {
	fname.name=metautils::relative_web_filename(fname.name);
	query2.set("tindex","dssdb.wfile","wfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_web_file=true;
	database="WObML";
	local_args.summarized_web_file=true;
	if (server_d.update("wfile","meta_link = 'Ob'","dsid = 'ds"+args.dsnum+"' and wfile = '"+fname.name+"'") < 0) {
	  metautils::log_error("summarize_obml() returned error: '"+server.error()+"' while trying to update wfile","scm",user,args.args_string);
	}
    }
    else if (std::regex_search(fname.name,std::regex("file://web:"))) {
	fname.name=fname.name.substr(11);
	query2.set("tindex","dssdb.wfile","wfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_web_file=true;
	database="WObML";
	local_args.summarized_web_file=true;
	if (server_d.update("wfile","meta_link = 'Ob'","dsid = 'ds"+args.dsnum+"' and wfile = '"+fname.name+"'") < 0) {
	  metautils::log_error("summarize_obml() returned error: '"+server.error()+"' while trying to update wfile","scm",user,args.args_string);
	}
    }
    if (query2.fetch_row(row)) {
	if (row[0] != "0") {
	  local_args.gindex_list.emplace_back(row[0]);
	}
    }
    format=e.attribute_value("format");
    if (format.empty()) {
	metautils::log_error("summarize_obml() returned error: missing "+database+" format attribute","scm",user,args.args_string);
    }
    else {
	if (server.insert("search.formats","keyword,vocabulary,dsid","'"+format+"','"+database+"','"+args.dsnum+"'","update dsid = values(dsid)") < 0) {
	  metautils::log_error("summarize_obml() returned error: "+server.error()+" while inserting into search.formats","scm",user,args.args_string);
	}
	if (local_args.data_format.empty()) {
	  local_args.data_format=format;
	}
	else if (format != local_args.data_format) {
	  local_args.data_format="all";
	}
    }
    if (!format_table.found(format,entry)) {
	find_in_ancillary_table(server,database+".formats","format = '"+format+"'",query);
	query.rewind();
	query.fetch_row(row);
	format_code=row[0];
	entry.key=format;
	entry.code=format_code;
	format_table.insert(entry);
    }
    else {
	format_code=entry.code;
    }
    if (fname.is_mss_file || fname.is_web_file) {
	std::string start="30001231";
	std::string end="10000101";
	auto num_obs=0;
	entry.key=fname.name;
	if (!file_table.found(entry.key,entry)) {
	  if (database == "ObML") {
	    files_table_name="ObML.ds"+local_args.dsnum2+"_primaries";
	    files_template_name="ObML.template_primaries";
	    file_ID_type="mss";
	  }
	  else if (database == "WObML") {
	    files_table_name="WObML.ds"+local_args.dsnum2+"_webfiles";
	    files_template_name="WObML.template_webfiles";
	    file_ID_type="web";
	  }
	  query.set("code",files_table_name,file_ID_type+"ID = '"+entry.key+"'");
	  if (query.submit(server) < 0) {
	    error=query.error();
	    if (strutils::contains(error,"doesn't exist")) {
		std::string result;
		if (server.command("create table "+files_table_name+" like "+files_template_name,result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+files_table_name,"scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_locations like "+database+".template_locations",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_locations","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_location_names like "+database+".template_location_names",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_location_names","scm",user,args.args_string);
		}
/*
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_dataTypes like "+database+".template_dataTypes",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_dataTypes","scm",user,args.args_string);
		}
*/
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_dataTypes2 like "+database+".template_dataTypes2",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_dataTypes2","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_dataTypesList like "+database+".template_dataTypesList",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_dataTypesList","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_frequencies like "+database+".template_frequencies",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_frequencies","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_IDs2 like "+database+".template_IDs2",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_IDs2","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_IDList2 like "+database+".template_IDList2",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_IDList2","scm",user,args.args_string);
		}
		if (database == "ObML" && server.command("create table "+database+".ds"+local_args.dsnum2+"_geobounds like "+database+".template_geobounds",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_geobounds","scm",user,args.args_string);
		}
/*
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_ID_dataTypes like "+database+".template_ID_dataTypes",result) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_ID_dataTypes","scm",user,args.args_string);
		}
*/
	    }
	    else
		metautils::log_error("summarize_obml() returned error: "+error+" while running query "+query.show(),"scm",user,args.args_string);
	  }
	  if (query.num_rows() == 0) {
	    if (database == "ObML") {
		if (server.insert(files_table_name,"mssID,format_code,num_observations,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while inserting into "+files_table_name,"scm",user,args.args_string);
		}
	    }
	    else if (database == "WObML") {
		if (server.insert(files_table_name,"webID,format_code,num_observations,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0) {
		  metautils::log_error("summarize_obml() returned error: "+server.error()+" while inserting into "+files_table_name,"scm",user,args.args_string);
		}
	    }
	    query.submit(server);
	    if (query.num_rows() == 0) {
		metautils::log_error("summarize_obml() returned error: error retrieving code from "+files_table_name,"scm",user,args.args_string);
	    }
	  }
	  else {
	    query.rewind();
	    query.fetch_row(row);
	    if (server.update(files_table_name,"format_code = "+format_code,"code = "+row[0]) < 0) {
		metautils::log_error("summarize_obml() returned error: "+server.error()+" while updating "+files_table_name+" with format_code and code","scm",user,args.args_string);
	    }
	  }
	  query.rewind();
	  query.fetch_row(row);
	  entry.code=row[0];
	  file_table.insert(entry);
	}
	file_ID_code=entry.code;
	if (!local_args.refresh_only) {
	  server._delete(database+".ds"+local_args.dsnum2+"_locations",file_ID_type+"ID_code = "+file_ID_code);
//	  server._delete(database+".ds"+local_args.dsnum2+"_dataTypes",file_ID_type+"ID_code = "+file_ID_code);
	  server._delete(database+".ds"+local_args.dsnum2+"_dataTypes2",file_ID_type+"ID_code = "+file_ID_code);
	  server._delete(database+".ds"+local_args.dsnum2+"_dataTypesList",file_ID_type+"ID_code = "+file_ID_code);
	  server._delete(database+".ds"+local_args.dsnum2+"_IDList2",file_ID_type+"ID_code = "+file_ID_code);
//	  server._delete(database+".ds"+local_args.dsnum2+"_ID_dataTypes",file_ID_type+"ID_code = "+file_ID_code);
	}
	obs_list=xdoc.element_list("ObML/observationType");
	for (const auto& obs : obs_list) {
	  entry.key=obs.attribute_value("value");
	  if (!obs_type_table.found(entry.key,entry)) {
	    find_in_ancillary_table(server,database+".obsTypes","obsType = '"+entry.key+"'",query);
	    query.rewind();
	    query.fetch_row(row);
	    observation_type_code=row[0];
	    entry.code=observation_type_code;
	    obs_type_table.insert(entry);
	  }
	  else {
	    observation_type_code=entry.code;
	  }
	  kmld.observation_type=entry.key;
	  kmld.observation_type_code=observation_type_code;
	  platform_list=obs.element_addresses();
	  for (const auto& platform : platform_list) {
	    entry.key=(platform.p)->attribute_value("type");
	    if (!platform_type_table.found(entry.key,entry)) {
		find_in_ancillary_table(server,database+".platformTypes","platformType = '"+entry.key+"'",query);
		query.rewind();
		query.fetch_row(row);
		platform_type_code=row[0];
		entry.code=platform_type_code;
		platform_type_table.insert(entry);
	    }
	    else {
		platform_type_code=entry.code;
	    }
	    if (database == "ObML" && local_args.create_kml && (entry.key == "automated_gauge" || entry.key == "CMAN_station" || entry.key == "coastal_station" || entry.key == "land_station" || entry.key == "moored_buoy" || entry.key == "fixed_ship" || entry.key == "wind_profiler")) {
		kmld.platform_type=entry.key;
		kmld.platform_type_code=platform_type_code;
		se.key=kmld.observation_type_code+"<!>"+kmld.platform_type_code;
		if (!kml_table.found(se.key,se)) {
		  kml_table.insert(se);
		  if (local_args.update_graphics) {
		    kml_list.emplace_back(kmld);
		  }
		}
	    }
	    data_type_list=(platform.p)->element_list("dataType");
	    for (const auto& data_type : data_type_list) {
		auto datatype=data_type.attribute_value("map");
		if (!datatype.empty()) {
		  datatype+=":";
		}
		datatype+=data_type.attribute_value("value");
		dte.key=observation_type_code+"|"+platform_type_code+"|"+datatype;
		if (!datatypes_table.found(dte.key,dte)) {
		  query2.set("code",database+".ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+observation_type_code+" and platformType_code = "+platform_type_code+" and dataType = '"+datatype+"'");
		  if (query2.submit(server) < 0) {
		    metautils::log_error("summarize_obml() returned error: "+query2.error()+" while trying to get dataType code (1) for '"+observation_type_code+","+platform_type_code+",'"+datatype+"''","scm",user,args.args_string);
		  }
		  dte.code.reset(new std::string);
		  if (query2.fetch_row(row2)) {
		    *(dte.code)=row2[0];
		  }
		  else {
		    if (server.insert(database+".ds"+local_args.dsnum2+"_dataTypesList",observation_type_code+","+platform_type_code+",'"+datatype+"',NULL") < 0) {
			metautils::log_error("summarize_obml() returned error: "+server.error()+" while trying to insert '"+observation_type_code+","+platform_type_code+",'"+datatype+"'' into "+database+".ds"+local_args.dsnum2+"_dataTypesList","scm",user,args.args_string);
		    }
		    auto last_id=server.last_insert_ID();
		    if (last_id == 0) {
			metautils::log_error("summarize_obml() returned error: "+query2.error()+" while trying to get dataType code (2) for '"+observation_type_code+","+platform_type_code+",'"+datatype+"''","scm",user,args.args_string);
		    }
		    else {
			*(dte.code)=strutils::lltos(last_id);
		    }
		  }
		  datatypes_table.insert(dte);
		}
		std::string insert_string=file_ID_code+","+*(dte.code);
		e=data_type.element("vertical");
		if (e.name() == "vertical") {
		  insert_string+=","+e.attribute_value("min_altitude")+","+e.attribute_value("max_altitude")+",'"+e.attribute_value("vunits")+"',"+e.attribute_value("avg_nlev")+","+e.attribute_value("avg_vres");
		}
		else {
		  insert_string+=",0,0,NULL,0,NULL";
		}
/*
		if (server.insert(database+".ds"+local_args.dsnum2+"_dataTypes",insert_string) < 0) {
		  error=server.error();
		  if (!local_args.refresh_only || !strutils::contains(error,"Duplicate entry")) {
		    metautils::log_error("summarize_obml() returned error: "+error+" while trying to insert '"+insert_string+"' into "+database+".ds"+local_args.dsnum2+"_dataTypes","scm",user,args.args_string);
		  }
		}
*/
		if (server.insert(database+".ds"+local_args.dsnum2+"_dataTypes2",insert_string) < 0) {
		  metautils::log_error("summarize_obml() returned error: "+error+" while trying to insert '"+insert_string+"' into "+database+".ds"+local_args.dsnum2+"_dataTypes2","scm",user,args.args_string);
		}
	    }
	    num_obs+=std::stoi((platform.p)->attribute_value("numObs"));
	    platform_elements=(platform.p)->element_addresses();
	    std::string tstart,tend;
	    for (const auto& platform_element : platform_elements) {
		if ((platform_element.p)->name() == "IDs") {
		  thread_IDs.strings.clear();
		  thread_IDs.strings.emplace_back(path);
		  thread_IDs.strings.emplace_back((platform_element.p)->attribute_value("ref"));
		  thread_IDs.strings.emplace_back(observation_type_code);
		  thread_IDs.strings.emplace_back(platform_type_code);
		  thread_IDs.strings.emplace_back(database);
		  thread_IDs.strings.emplace_back(file_ID_type);
		  pthread_create(&thread_IDs.tid,NULL,summarize_IDs,&thread_IDs);
		}
		else if ((platform_element.p)->name() == "temporal") {
		  tstart=(platform_element.p)->attribute_value("start");
		  strutils::replace_all(tstart,"-","");
		  if (tstart < start) {
		    start=tstart;
		  }
		  tend=(platform_element.p)->attribute_value("end");
		  strutils::replace_all(tend,"-","");
		  if (tend > end) {
		    end=tend;
		  }
		}
		else if ((platform_element.p)->name() == "locations") {
// read in the locations from the separate XML file
		  thread_locations.strings.clear();
		  thread_locations.strings.emplace_back(path);
		  thread_locations.strings.emplace_back((platform_element.p)->attribute_value("ref"));
		  thread_locations.strings.emplace_back(observation_type_code);
		  thread_locations.strings.emplace_back(platform_type_code);
		  thread_locations.strings.emplace_back(tstart);
		  thread_locations.strings.emplace_back(tend);
		  thread_locations.strings.emplace_back(database);
		  pthread_create(&thread_locations.tid,NULL,summarize_file_ID_locations,&thread_locations);
		}
	    }
	    pthread_join(thread_IDs.tid,NULL);
	    pthread_join(thread_locations.tid,NULL);
	  }
	}
	server._delete(database+".ds"+local_args.dsnum2+"_frequencies",file_ID_type+"ID_code = "+file_ID_code+" and uflag != '"+uflag+"'");
	update_string="num_observations="+strutils::itos(num_obs)+",start_date="+start+",end_date="+end;
	if (server.update(files_table_name,update_string,"code = "+file_ID_code) < 0) {
	  metautils::log_error("summarize_obml() returned error: "+server.error()+" while updating "+files_table_name+" with '"+update_string+"'","scm",user,args.args_string);
	}
    }
    else {
	metautils::log_error("summarize_obml() returned error: not an HPSS file","scm",user,args.args_string);
    }
    xdoc.close();
    if (database == "ObML") {
	summarizeMetadata::summarize_frequencies(args.dsnum,"scm",user,args.args_string,file_ID_code);
    }
    server_d.disconnect();
  }
  server._delete("search.data_types","dsid = '"+args.dsnum+"' and vocabulary = 'dssmm'");
  if (server.insert("search.data_types","'platform_observation','','"+database+"','"+args.dsnum+"'") < 0) {
    error=server.error();
    if (!strutils::contains(error,"Duplicate entry")) {
	metautils::log_error("summarize_obml() returned error: "+error+" while inserting into search.data_types","scm",user,args.args_string);
    }
  }
}

void summarize_satml()
{
  XMLDocument xdoc;
  XMLElement e;
  std::list<XMLElement> satellite_list,image_list,swath_data_list;
  summarizeMetadata::Entry entry;
  std::string start,end,error,sdum;
  size_t num_products=0;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  std::string prod_type;
  MySQL::Server server_d;

  if (satml_file_list.size() == 0)
    return;
  for (auto& fname : satml_file_list) {
    sdum=remote_web_file("https://rda.ucar.edu"+fname.path,temp_dir.name());
    if (!xdoc.open(sdum)) {
	metautils::log_error("unable to open "+fname.path,"scm",user,args.args_string);
    }
    e=xdoc.element("SatML");
    fname.name=e.attribute_value("uri");
    if (std::regex_search(fname.name,std::regex("^file://MSS:(/FS){0,1}/DSS"))) {
	strutils::replace_all(fname.name,"file://MSS:","");
	fname.is_mss_file=true;
	local_args.summarized_hpss_file=true;
	metautils::connect_to_rdadb_server(server_d);
	if (!server_d)
	  metautils::log_error("summarize_satml() could not connect to mysql server - error: "+server_d.error(),"scm",user,args.args_string);
	if (server_d.update("mssfile","meta_link = 'Sat'","dsid = 'ds"+args.dsnum+"' and mssfile = '"+fname.name+"'") < 0)
	  metautils::log_error("summarize_satml() returned error: "+server.error(),"scm",user,args.args_string);
    }
    entry.key=e.attribute_value("format");
    if (entry.key.empty()) {
	std::cerr << "Error: missing SatML format attribute" << std::endl;
	exit(1);
    }
    if (!format_table.found(entry.key,entry)) {
	find_in_ancillary_table(server,"SatML.formats","format = '"+entry.key+"'",query);
	query.rewind();
	query.fetch_row(row);
	format_code=row[0];
	entry.code=format_code;
	format_table.insert(entry);
    }
    else {
	format_code=entry.code;
    }
    if (fname.is_mss_file) {
	start="3000-12-31 23:59:59 +0000";
	end="1000-01-01 00:00:00 +0000";
	entry.key=fname.name;
	if (!file_table.found(entry.key,entry)) {
	  files_table_name="SatML.ds"+local_args.dsnum2+"_primaries";
	  files_template_name="SatML.template_primaries";
	  query.set("code",files_table_name,"mssID = '"+entry.key+"'");
	  if (query.submit(server) < 0) {
	    error=query.error();
	    if (strutils::contains(error,"doesn't exist")) {
		std::string result;
		if (server.command("create table "+files_table_name+" like "+files_template_name,result) < 0) {
		  metautils::log_error("summarize_grml() returned error: "+server.error()+" while creating table "+files_table_name,"scm",user,args.args_string);
		}
	    }
	    else {
		metautils::log_error("suumarizeSatML returned error: "+query.error()+" for query: "+query.show(),"scm",user,args.args_string);
	    }
	  }
	  if (query.num_rows() == 0) {
	    if (server.insert(files_table_name,"mssID,format_code,num_products,start_date,end_date,product","'"+entry.key+"',"+format_code+",0,0,0,''","") < 0)
		metautils::log_error("summarize_satml() returned error: "+server.error()+" when trying to insert '"+entry.key+"',"+format_code+",0,0,0,'' into "+files_table_name,"scm",user,args.args_string);
	    query.submit(server);
	    if (query.num_rows() == 0) {
		std::cerr << "Error retrieving code from " << files_table_name << " for value " << entry.key << std::endl;
		exit(1);
	    }
	  }
	  query.rewind();
	  query.fetch_row(row);
	  entry.code=row[0];
	  file_table.insert(entry);
	}
	file_ID_code=entry.code;
	satellite_list=xdoc.element_list("SatML/satellite");
	for (const auto& satellite : satellite_list) {
	  image_list=satellite.element_list("images");
	  if (image_list.size() > 0) {
	    prod_type="I";
	    e=satellite.element("temporal");
	    if (e.attribute_value("start") < start)
		start=e.attribute_value("start");
	    if (e.attribute_value("end") > end)
		end=e.attribute_value("end");
	    num_products=0;
	    for (const auto& image : image_list) {
		num_products+=std::stoi(image.attribute_value("num"));
	    }
	  }
	  else {
	    swath_data_list=satellite.element_list("swath/data");
	    prod_type="S";
	    for (const auto& swath : swath_data_list) {
		e=swath.element("temporal");
		if (e.attribute_value("start") < start)
		  start=e.attribute_value("start");
		if (e.attribute_value("end") > end)
		  end=e.attribute_value("end");
		e=swath.element("scanLines");
		num_products+=std::stoi(e.attribute_value("num"));
	    }
	  }
	}
	strutils::replace_all(start," ","");
	strutils::replace_all(start,"-","");
	strutils::replace_all(start,":","");
	strutils::replace_all(start,"+0000","");
	strutils::replace_all(end," ","");
	strutils::replace_all(end,"-","");
	strutils::replace_all(end,":","");
	strutils::replace_all(end,"+0000","");
	update_string="num_products="+strutils::itos(num_products)+",start_date="+start+",end_date="+end+",product='"+prod_type+"'";
	if (server.update(files_table_name,update_string,"code = "+file_ID_code) < 0) {
	  std::cerr << server.error() << std::endl;
	  std::cerr << "tried to update " << files_table_name << " with " << update_string << " where code = " << file_ID_code << std::endl;
	  exit(1);
	}
    }
    xdoc.close();
    summarizeMetadata::summarize_frequencies(args.dsnum,"scm",user,args.args_string,file_ID_code);
    server_d.disconnect();
  }
  if (server.insert("search.data_types","'satellite','','SatML','"+args.dsnum+"'") < 0) {
    error=server.error();
    if (!strutils::contains(error,"Duplicate entry")) {
	std::cerr << error << std::endl;
	exit(1);
    }
  }
}

struct ClassificationEntry {
  struct Data {
    Data() : start(),end() {}

    std::string start,end;
  };
  ClassificationEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
void summarize_fixml()
{
  XMLDocument xdoc;
  XMLElement e;
  std::string format,sdum,start,end,error,database;
  summarizeMetadata::Entry entry;
  MySQL::LocalQuery query,query2;
  MySQL::Row row;
  std::list<XMLElement> fix_list,class_list;
  size_t num_fixes=0,n,m;
  ThreadStruct thread_IDs,thread_locations;
  my::map<ClassificationEntry> classification_table;
  std::list<std::string> classification_table_keys;
  ClassificationEntry ce;
  my::map<summarizeMetadata::Entry> class_code_table;
  std::string class_code;
  std::deque<std::string> sp;
  DateTime cstart,cend;
  time_t tm1=0,tm2;
  MySQL::Server server_d;

  if (fixml_file_list.size() == 0) {
    return;
  }
  for (auto& fname : fixml_file_list) {
    if (local_args.verbose) {
      tm1=time(NULL);
    }
    open_xml_file(xdoc,fname.path);
    metautils::connect_to_rdadb_server(server_d);
    if (!server_d) {
	metautils::log_error("summarize_fixml() could not connect to mysql server - error: "+server_d.error(),"scm",user,args.args_string);
    }
    if (!xdoc) {
	metautils::log_error("summarize_fixml() returned error: unable to open "+fname.path,"scm",user,args.args_string);
    }
    classification_table.clear();
    classification_table_keys.clear();
    e=xdoc.element("FixML");
    fname.name=e.attribute_value("uri");
    if (std::regex_search(fname.name,std::regex("^file://MSS:(/FS){0,1}/DSS"))) {
	strutils::replace_all(fname.name,"file://MSS:","");
	query2.set("gindex","dssdb.mssfile","mssfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_mss_file=true;
	database="FixML";
	local_args.summarized_hpss_file=true;
	sp=strutils::split(fname.name,"..m..");
	if (sp.size() > 1) {
	  if (server_d.update("htarfile","meta_link = 'Fix'","dsid = 'ds"+args.dsnum+"' and hfile = '"+sp[1]+"'") < 0) {
	    metautils::log_error("summarize_fixml() returned error: "+server.error(),"scm",user,args.args_string);
	  }
	}
	else {
	  if (server_d.update("mssfile","meta_link = 'Fix'","dsid = 'ds"+args.dsnum+"' and mssfile = '"+fname.name+"'") < 0) {
	    metautils::log_error("summarize_fixml() returned error: "+server.error(),"scm",user,args.args_string);
	  }
	}
    }
    else if (std::regex_search(fname.name,std::regex("^http(s){0,1}://rda\\.ucar\\.edu"))) {
	fname.name=metautils::relative_web_filename(fname.name);
	query2.set("tindex","dssdb.wfile","wfile = '"+fname.name+"'");
	if (query2.submit(server) < 0) {
	  error=query2.error();
	}
	fname.is_web_file=true;
	database="WFixML";
	local_args.summarized_web_file=true;
	if (server_d.update("wfile","meta_link = 'Fix'","dsid = 'ds"+args.dsnum+"' and wfile = '"+fname.name+"'") < 0)
	  metautils::log_error("summarize_fixml() returned error: "+server.error(),"scm",user,args.args_string);
    }
    if (query2.fetch_row(row)) {
	if (row[0] != "0")
	  local_args.gindex_list.emplace_back(row[0]);
    }
    format=e.attribute_value("format");
    if (format.empty())
      metautils::log_error("summarize_fixml() returned error: missing "+database+" format attribute","scm",user,args.args_string);
    else {
	if (server.insert("search.formats","keyword,vocabulary,dsid","'"+format+"','FixML','"+args.dsnum+"'","update dsid = values(dsid)") < 0) {
	  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while inserting into search.formats","scm",user,args.args_string);
	}
	if (local_args.data_format.empty()) {
	  local_args.data_format=format;
	}
	else if (format != local_args.data_format) {
	  local_args.data_format="all";
	}
    }
    if (!format_table.found(format,entry)) {
	find_in_ancillary_table(server,database+".formats","format = '"+format+"'",query);
	query.rewind();
	query.fetch_row(row);
	format_code=row[0];
	entry.key=format;
	entry.code=format_code;
	format_table.insert(entry);
    }
    else
	format_code=entry.code;
    if (fname.is_mss_file || fname.is_web_file) {
	start="30001231235959";
	end="10000101000000";
	entry.key=fname.name;
	if (!file_table.found(entry.key,entry)) {
	  if (database == "FixML") {
	    files_table_name="FixML.ds"+local_args.dsnum2+"_primaries";
	    files_template_name="FixML.template_primaries";
	    file_ID_type="mss";
	  }
	  else if (database == "WFixML") {
	    files_table_name="WFixML.ds"+local_args.dsnum2+"_webfiles";
	    files_template_name="WFixML.template_webfiles";
	    file_ID_type="web";
	  }
	  query.set("code",files_table_name,file_ID_type+"ID = '"+entry.key+"'");
	  if (query.submit(server) < 0) {
	    error=query.error();
	    if (strutils::contains(error,"doesn't exist")) {
		std::string result;
		if (server.command("create table "+files_table_name+" like "+files_template_name,result) < 0) {
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while creating table "+files_table_name,"scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_IDList like "+database+".template_IDList",result) < 0) {
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_IDList","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_locations like "+database+".template_locations",result) < 0) {
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_locations","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_location_names like "+database+".template_location_names",result) < 0) {
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_location_names","scm",user,args.args_string);
		}
		if (server.command("create table "+database+".ds"+local_args.dsnum2+"_frequencies like "+database+".template_frequencies",result) < 0) {
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" while creating table "+database+".ds"+local_args.dsnum2+"_frequencies","scm",user,args.args_string);
		}
	    }
	    else
		metautils::log_error("summarize_fixml() returned error: "+error+" while running query "+query.show(),"scm",user,args.args_string);
	  }
	  if (query.num_rows() == 0) {
	    if (database == "FixML") {
		if (server.insert(files_table_name,"mssID,format_code,num_fixes,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0)
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" when trying to insert '"+entry.key+"',"+format_code+",0,0,0 into "+files_table_name,"scm",user,args.args_string);
	    }
	    else if (database == "WFixML") {
		if (server.insert(files_table_name,"webID,format_code,num_fixes,start_date,end_date","'"+entry.key+"',"+format_code+",0,0,0","") < 0)
		  metautils::log_error("summarize_fixml() returned error: "+server.error()+" when trying to insert '"+entry.key+"',"+format_code+",0,0,0 into "+files_table_name,"scm",user,args.args_string);
	    }
	    query.submit(server);
	    if (query.num_rows() == 0)
		metautils::log_error("summarize_fixml() returned error: unable to retrieve code from "+files_table_name+" for value "+entry.key,"scm",user,args.args_string);
	  }
	  query.rewind();
	  query.fetch_row(row);
	  entry.code=row[0];
	  file_table.insert(entry);
	}
	file_ID_code=entry.code;
	server._delete(database+".ds"+local_args.dsnum2+"_locations",file_ID_type+"ID_code = "+file_ID_code);
	server._delete(database+".ds"+local_args.dsnum2+"_frequencies",file_ID_type+"ID_code = "+file_ID_code);
	server._delete(database+".ds"+local_args.dsnum2+"_IDList",file_ID_type+"ID_code = "+file_ID_code);
	fix_list=xdoc.element_list("FixML/feature");
	for (const auto& fix : fix_list) {
	  class_list=fix.element_list("classification");
	  for (const auto& _class : class_list) {
	    ce.key=_class.attribute_value("stage");
	    if (!classification_table.found(ce.key,ce)) {
		ce.data.reset(new ClassificationEntry::Data);
		ce.data->start="30001231235959";
		ce.data->end="10000101000000";
		classification_table.insert(ce);
		classification_table_keys.emplace_back(ce.key);
	    }
	    num_fixes+=std::stoi(_class.attribute_value("nfixes"));
	    if (!class_code_table.found(ce.key,entry)) {
		find_in_ancillary_table(server,database+".classifications","classification = '"+ce.key+"'",query);
		query.rewind();
		query.fetch_row(row);
		class_code=row[0];
		entry.key=ce.key;
		entry.code=class_code;
		class_code_table.insert(entry);
	    }
	    else
		class_code=entry.code;
	    if (server.insert(database+".ds"+local_args.dsnum2+"_IDList","'"+fix.attribute_value("ID")+"',"+class_code+","+file_ID_code+","+_class.attribute_value("nfixes"),"update num_fixes=num_fixes+"+_class.attribute_value("nfixes")) < 0)
		metautils::log_error(server.error()+" while inserting '"+fix.attribute_value("ID")+"',"+class_code+","+file_ID_code+","+_class.attribute_value("nfixes")+"' into "+database+".ds"+local_args.dsnum2+"_IDList","scm",user,args.args_string);
	    e=_class.element("start");
	    sdum=e.attribute_value("dateTime");
	    strutils::replace_all(sdum," ","");
	    strutils::replace_all(sdum,"-","");
	    strutils::replace_all(sdum,":","");
	    sdum=sdum.substr(0,sdum.length()-5);
	    if (sdum < start) {
		start=sdum;
	    }
	    if (sdum < ce.data->start) {
		ce.data->start=sdum;
	    }
	    while (sdum.length() < 14) {
		sdum+="00";
	    }
	    cstart=std::stoll(sdum);
	    e=_class.element("end");
	    sdum=e.attribute_value("dateTime");
	    strutils::replace_all(sdum," ","");
	    strutils::replace_all(sdum,"-","");
	    strutils::replace_all(sdum,":","");
	    sdum=sdum.substr(0,sdum.length()-5);
	    if (sdum > end) {
		end=sdum;
	    }
	    if (sdum > ce.data->end) {
		ce.data->end=sdum;
	    }
	    while (sdum.length() < 14) {
		sdum+="00";
	    }
	    cend=std::stoll(sdum);
	    n=std::stoi(_class.attribute_value("nfixes"));
	    if (n > 1) {
		m=cend.hours_since(cstart);
		n=lroundf(24./(m/static_cast<float>(n-1)));
/*
if (n != 4)
std::cerr << n << " " << cstart.to_string() << " " << cend.to_string() << std::endl;
*/
		fe.key="day<!>"+class_code;
		if (!frequency_table.found(fe.key,fe)) {
		  fe.data.reset(new FEntry::Data);
		  fe.data->min=fe.data->max=n;
		  frequency_table.insert(fe);
		}
		else {
		  if (n < fe.data->min) {
		    fe.data->min=n;
		  }
		  if (n > fe.data->max) {
		    fe.data->max=n;
		  }
		}
	    }
	  }
	}
	update_string="num_fixes="+strutils::itos(num_fixes)+",start_date="+start+",end_date="+end;
	if (server.update(files_table_name,update_string,"code = "+file_ID_code) < 0)
	  metautils::log_error("got '"+server.error()+"' when trying to update "+files_table_name+" with '"+update_string+"' where code="+file_ID_code,"scm",user,args.args_string);
    }
    xdoc.close();
    for (const auto& key : classification_table_keys) {
	classification_table.found(key,ce);
	thread_locations.strings.clear();
	thread_locations.strings.emplace_back(fname.path+"."+ce.key+".locations.xml");
	thread_locations.strings.emplace_back("");
	class_code_table.found(ce.key,entry);
	thread_locations.strings.emplace_back(entry.code);
	thread_locations.strings.emplace_back("");
	sdum=ce.data->start;
	strutils::replace_all(sdum,"-","");
	strutils::replace_all(sdum,":","");
	strutils::replace_all(sdum," ","");
	if (sdum.length() > 12) {
	  sdum=sdum.substr(0,12);
	}
	thread_locations.strings.emplace_back(sdum);
	sdum=ce.data->end;
	strutils::replace_all(sdum,"-","");
	strutils::replace_all(sdum,":","");
	strutils::replace_all(sdum," ","");
	if (sdum.length() > 12) {
	  sdum=sdum.substr(0,12);
	}
	thread_locations.strings.emplace_back(sdum);
	thread_locations.strings.emplace_back(database);
	summarize_file_ID_locations(reinterpret_cast<void *>(&thread_locations));
    }
    if (database == "FixML")
	summarizeMetadata::summarize_frequencies(args.dsnum,"scm",user,args.args_string,file_ID_code);
    server_d.disconnect();
    if (local_args.verbose) {
	tm2=time(NULL);
	std::cout << fname.name << " summarized in " << (tm2-tm1) << " seconds" << std::endl;
    }
  }
  server._delete("search.data_types","dsid = '"+args.dsnum+"' and vocabulary = 'dssmm'");
  if (server.insert("search.data_types","'cyclone_fix','','FixML','"+args.dsnum+"'") < 0) {
    error=server.error();
    if (!strutils::contains(error,"Duplicate entry")) {
	metautils::log_error("summarize_fixml() returned error: "+error+" while inserting into search.data_types","scm",user,args.args_string);
    }
  }
  for (const auto& key : frequency_table.keys()) {
    frequency_table.found(key,fe);
    sp=strutils::split(fe.key,"<!>");
    if (server.insert(database+".ds"+local_args.dsnum2+"_frequencies",file_ID_code+","+sp[1]+","+strutils::itos(fe.data->min)+","+strutils::itos(fe.data->max)+",'"+sp[0]+"'") < 0) {
	metautils::log_error(server.error()+" while trying to insert into "+database+".ds"+local_args.dsnum2+"_frequencies '"+file_ID_code+","+sp[1]+","+strutils::itos(fe.data->min)+","+strutils::itos(fe.data->max)+",'"+sp[0]+"''","scm",user,args.args_string);
    }
    if (database == "FixML") {
	sdum=time_resolution_keyword("irregular",fe.data->min,sp[0],"");
	if (server.insert("search.time_resolutions","'"+sdum+"','GCMD','"+args.dsnum+"','FixML'") < 0) {
	  error=server.error();
	  if (!strutils::contains(error,"Duplicate entry")) {
	    metautils::log_error(error+" while trying to insert into search.time_resolutions ''"+sdum+"','GCMD','"+args.dsnum+"','FixML''","scm",user,args.args_string);
	  }
	}
	sdum=time_resolution_keyword("irregular",fe.data->max,sp[0],"");
	if (server.insert("search.time_resolutions","'"+sdum+"','GCMD','"+args.dsnum+"','FixML'") < 0) {
	  error=server.error();
	  if (!strutils::contains(error,"Duplicate entry"))
	    metautils::log_error(error+" while trying to insert into search.time_resolutions ''"+sdum+"','GCMD','"+args.dsnum+"','FixML''","scm",user,args.args_string);
	}
    }
    fe.data=nullptr;
  }
}

extern "C" void *thread_generate_detailed_metadata_view(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;
//PROBLEM!!
  for (const auto& gindex : local_args.gindex_list) {
    if (gindex != "0") {
	summarizeMetadata::detailedMetadata::generate_group_detailed_metadata_view(args.dsnum,gindex,t->strings[0],"scm",user,args.args_string);
    }
  }
  if (local_args.summarized_hpss_file || local_args.refresh_hpss) {
    summarizeMetadata::detailedMetadata::generate_detailed_metadata_view(args.dsnum,"scm",user,args.args_string);
  }
  return NULL;
}

extern "C" void *thread_create_file_list_cache(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::create_file_list_cache(t->strings[0],"scm",user,args.args_string,t->strings[1]);
  return NULL;
}

extern "C" void *thread_summarize_obs_data(void *)
{
  std::stringstream output,error;
  bool updated_bitmap;

  updated_bitmap=summarizeMetadata::obsData::summarize_obs_data(args.dsnum,"scm",user,args.args_string);
  if (updated_bitmap && local_args.update_graphics) {
    mysystem2(directives.local_root+"/bin/gsi "+args.dsnum,output,error);
  }
  return NULL;
}

extern "C" void *thread_summarize_fix_data(void *)
{
  std::stringstream output,error;

  summarizeMetadata::fixData::summarize_fix_data(args.dsnum,"scm",user,args.args_string);
  if (local_args.update_graphics) {
    mysystem2(directives.local_root+"/bin/gsi "+args.dsnum,output,error);
  }
  return NULL;
}

extern "C" void *thread_index_variables(void *)
{
  auto error=index_variables(args.dsnum);
  if (!error.empty()) {
    metautils::log_error("thread_index_variables() returned error: "+error,"scm",user,args.args_string);
  }
  return NULL;
}

extern "C" void *thread_index_locations(void *)
{
  auto error=index_locations(args.dsnum);
  if (!error.empty()) {
    metautils::log_error("thread_index_locations() returned error: "+error,"scm",user,args.args_string);
  }
  return NULL;
}

extern "C" void *thread_summarize_dates(void *)
{
  summarizeMetadata::summarize_dates(args.dsnum,"scm",user,args.args_string);
  return NULL;
}

extern "C" void *thread_summarize_frequencies(void *)
{
  summarizeMetadata::summarize_frequencies(args.dsnum,"scm",user,args.args_string);
  return NULL;
}

extern "C" void *thread_summarize_formats(void *)
{
  summarizeMetadata::summarize_formats(args.dsnum,"scm",user,args.args_string);
  return NULL;
}

extern "C" void *thread_aggregate_grids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::grids::aggregate_grids(args.dsnum,t->strings[0],"scm",user,args.args_string);
  return NULL;
}  

extern "C" void segv_handler(int)
{
  metautils::log_error("segmentation fault","scm",user,args.args_string);
}

int main(int argc,char **argv)
{
  ThreadStruct agg,det,flist,flist_i;
  std::list<KMLData> kml_list;
  MySQL::LocalQuery query;
  MySQL::Row row;

  if (argc < 3) {
    std::cerr << "usage: scm -d [ds]nnn.n [options...] {-a|-wa <type> | -f|-wf file | -rm|-rw|-ri <tindex|all>}" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d nnn.n   specifies the dataset number" << std::endl;
    std::cerr << "\nrequired (choose one):" << std::endl;
    std::cerr << "-a <type>  summarize all HPSS metadata (can restrict to optional <type>)" << std::endl;
    std::cerr << "-wa <type> summarize all Web metadata (can restrict to optional <type>)" << std::endl;
    std::cerr << "-f file    summarize metadata only for HPSS file <file>" << std::endl;
    std::cerr << "-wf file   summarize metadata only for Web file <file>" << std::endl;
    std::cerr << "-rm [tindex | all]  refresh the HPSS database (e.g. for dsarch changes)" << std::endl;
    std::cerr << "                    additionally, if you specify a topmost group index or \"all\"," << std::endl;
    std::cerr << "                    the group metadata for the specified index or all indexes" << std::endl;
    std::cerr << "                    will be refreshed" << std::endl;
    std::cerr << "-rw [tindex | all]  refresh the Web database (e.g. for dsarch changes)" << std::endl;
    std::cerr << "                    additionally, if you specify a topmost group index or \"all\"," << std::endl;
    std::cerr << "                    the group metadata for the specified index or all indexes" << std::endl;
    std::cerr << "                    will be refreshed" << std::endl;
    std::cerr << "-ri [tindex | all]  refresh the inv database (e.g. for dsarch changes)" << std::endl;
    std::cerr << "                    additionally, if you specify a topmost group index or \"all\"," << std::endl;
    std::cerr << "                    the group metadata for the specified index or all indexes" << std::endl;
    std::cerr << "                    will be refreshed" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-g/-G       generate (default)/don't generate graphics" << std::endl;
    std::cerr << "-k/-K       generate (default)/don't generate KML for observations" << std::endl;
    std::cerr << "-N          notify with a message when scm completes" << std::endl;
    if (user == "dattore") {
	std::cerr << "-F*         refresh only - use this flag only when the data file is unchanged" << std::endl;
	std::cerr << "              but additional fields/tables have been added to the database that" << std::endl;
	std::cerr << "              need to be populated" << std::endl;
	std::cerr << "-r/-R*      regenerate (default)/don't regenerate dataset web page" << std::endl;
	std::cerr << "-s/-S*      summarize (default)/don't summarize date and time resolutions" << std::endl;
    }
    std::cerr << "-V          verbose mode" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  local_args.summarized_hpss_file=false;
  local_args.summarized_web_file=false;
  args.args_string=unix_args_string(argc,argv,'`');
  metautils::read_config("scm",user,args.args_string);
  parse_args();
  metautils::connect_to_metadata_server(server);
  if (!server) {
    metautils::log_error("unable to connect to MySQL server on startup","scm",user,args.args_string);
  }
  if (!temp_dir.create(directives.temp_path)) {
    metautils::log_error("unable to create temporary directory","scm",user,args.args_string);
  }
  std::vector<pthread_t> tid_list;
  auto t1=std::time(nullptr);
  if (local_args.summarize_all) {
    std::stringstream output,error;
    if (mysystem2("/bin/tcsh -c \"wget -q -O - --post-data='authKey=qGNlKijgo9DJ7MN&cmd=listfiles&value=/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+local_args.cmd_directory+"' https://rda.ucar.edu/cgi-bin/dss/remoteRDAServerUtils\"",output,error) < 0) {
	metautils::log_error("unable to get metadata file listing","scm",user,args.args_string);
    }
    auto lines=strutils::split(output.str(),"\n");
    for (const auto& line : lines) {
	FileEntry f;
	f.path=line.substr(line.find("/datasets"));
	if (std::regex_search(f.path,std::regex("\\.gz$"))) {
	  strutils::chop(f.path,3);
	}
	f.metaname=f.path.substr(f.path.find("/"+local_args.cmd_directory+"/")+local_args.cmd_directory.length()+2);
	f.start.set(3000,12,31,2359);
	f.end.set(1000,1,1,0);
	f.is_mss_file=false;
	f.is_web_file=false;
	if (std::regex_search(f.path,std::regex("\\.GrML$"))) {
	  grml_file_list.emplace_back(f);
	}
	else if (std::regex_search(f.path,std::regex("\\.ObML$"))) {
	  obml_file_list.emplace_back(f);
	}
	else if (std::regex_search(f.path,std::regex("\\.SatML$"))) {
	  satml_file_list.emplace_back(f);
	}
	else if (std::regex_search(f.path,std::regex("\\.FixML$"))) {
	  fixml_file_list.emplace_back(f);
	}
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "GrML") && grml_file_list.size() > 0) {
	if (local_args.cmd_directory == "fmd") {
	  server._delete("GrML.summary","dsid = '"+args.dsnum+"'");
	}
	else if (local_args.cmd_directory == "wfmd") {
	  server._delete("WGrML.summary","dsid = '"+args.dsnum+"'");
	}
	summarize_grml();
	grml_file_list.clear();
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "ObML") && obml_file_list.size() > 0) {
	summarize_obml(kml_list);
	obml_file_list.clear();
	if (local_args.summarized_hpss_file && local_args.update_db) {
	  ThreadStruct ts;
	  pthread_create(&ts.tid,NULL,thread_summarize_obs_data,NULL);
	  tid_list.emplace_back(ts.tid);
	  pthread_create(&ts.tid,NULL,thread_index_locations,NULL);
	  tid_list.emplace_back(ts.tid);
	}
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "SatML") && satml_file_list.size() > 0) {
	summarize_satml();
	satml_file_list.clear();
    }
    if ((local_args.summ_type.empty() || local_args.summ_type == "FixML") && fixml_file_list.size() > 0) {
	summarize_fixml();
	fixml_file_list.clear();
	if (local_args.update_db) {
	  ThreadStruct ts;
	  pthread_create(&ts.tid,NULL,thread_summarize_fix_data,NULL);
	  tid_list.emplace_back(ts.tid);
	  pthread_create(&ts.tid,NULL,thread_index_locations,NULL);
	  tid_list.emplace_back(ts.tid);
	}
    }
  }
  else if (!local_args.file.empty()) {
    FileEntry f;
    if (local_args.is_hpss_file) {
	f.path="/datasets/ds"+args.dsnum+"/metadata/fmd/"+local_args.file;
	f.metaname=f.path.substr(f.path.find("/fmd/")+5);
    }
    else if (local_args.is_web_file) {
	f.path="/datasets/ds"+args.dsnum+"/metadata/wfmd/"+local_args.file;
	f.metaname=f.path.substr(f.path.find("/wfmd/")+6);
    }
    f.start.set(3000,12,31,2359);
    f.end.set(1000,1,1,0);
    f.is_mss_file=false;
    f.is_web_file=false;
    if (std::regex_search(local_args.file,std::regex("GrML$"))) {
	grml_file_list.emplace_back(f);
	summarize_grml();
	grml_file_list.clear();
    }
    else if (std::regex_search(local_args.file,std::regex("ObML$"))) {
	obml_file_list.emplace_back(f);
	summarize_obml(kml_list);
	obml_file_list.clear();
	if (local_args.summarized_hpss_file && local_args.update_db) {
	  ThreadStruct ts;
	  pthread_create(&ts.tid,NULL,thread_summarize_obs_data,NULL);
	  tid_list.emplace_back(ts.tid);
	  pthread_create(&ts.tid,NULL,thread_index_locations,NULL);
	  tid_list.emplace_back(ts.tid);
	}
    }
    else if (std::regex_search(local_args.file,std::regex("SatML$"))) {
	satml_file_list.emplace_back(f);
	summarize_satml();
	satml_file_list.clear();
    }
    else if (std::regex_search(local_args.file,std::regex("FixML$"))) {
	fixml_file_list.emplace_back(f);
	summarize_fixml();
	fixml_file_list.clear();
	if (local_args.summarized_hpss_file && local_args.update_db) {
	  ThreadStruct ts;
	  pthread_create(&ts.tid,NULL,thread_summarize_fix_data,NULL);
	  tid_list.emplace_back(ts.tid);
	  pthread_create(&ts.tid,NULL,thread_index_locations,NULL);
	  tid_list.emplace_back(ts.tid);
	}
    }
    else {
	metautils::log_error("file extension of '"+local_args.file+"' not recognized","scm",user,args.args_string);
    }
  }
  else if (local_args.refresh_hpss) {
    if (local_args.gindex_list.size() > 0 && local_args.gindex_list.front() != "all") {
	query.set("gidx","dssdb.dsgroup","dsid = 'ds"+args.dsnum+"' and gindex = "+local_args.gindex_list.front()+" and pindex = 0");
	if (query.submit(server) < 0) {
	  metautils::log_error("group check failed","scm",user,args.args_string);
	}
	if (query.num_rows() == 0) {
	  std::cerr << "Error: " << local_args.gindex_list.front() << " is not a top-level index for this dataset" << std::endl;
	  exit(1);
	}
    }
    if (MySQL::table_exists(server,"GrML.ds"+local_args.dsnum2+"_agrids_cache")) {
        summarizeMetadata::grids::summarize_grids(args.dsnum,"GrML","scm",user,args.args_string);
	agg.strings.emplace_back("GrML");
	pthread_create(&agg.tid,nullptr,thread_aggregate_grids,&agg);
	tid_list.emplace_back(agg.tid);
    }
    ThreadStruct ts;
    pthread_create(&ts.tid,NULL,thread_summarize_frequencies,NULL);
    tid_list.emplace_back(ts.tid);
    if (local_args.create_kml) {
	query.set("select distinct f.observationType_code,o.obsType,f.platformType_code,p.platformType from ObML.ds"+local_args.dsnum2+"_frequencies as f left join ObML.obsTypes as o on o.code = f.observationType_code left join ObML.platformTypes as p on p.code = f.platformType_code");
	if (query.submit(server) == 0) {
	  while (query.fetch_row(row)) {
	    if (row[3] == "automated_gauge" || row[3] == "CMAN_station" || row[3] == "coastal_station" || row[3] == "land_station" || row[3] == "moored_buoy" || row[3] == "fixed_ship" || row[3] == "wind_profiler") {
		KMLData kmld;
		kmld.observation_type_code=row[0];
		kmld.observation_type=row[1];
		kmld.platform_type_code=row[2];
		kmld.platform_type=row[3];
		kml_list.emplace_back(kmld);
	    }
	  }
	}
    }
  }
  if (local_args.summarized_hpss_file && local_args.added_variable) {
    ThreadStruct ts;
    pthread_create(&ts.tid,NULL,thread_index_variables,NULL);
    tid_list.emplace_back(ts.tid);
  }
  if (local_args.update_db) {
    if (local_args.summarized_hpss_file || local_args.refresh_hpss) {
	if (local_args.refresh_hpss && local_args.gindex_list.size() == 1) {
	  if (local_args.gindex_list.front() == "all") {
	    local_args.gindex_list.clear();
	    query.set("select distinct tindex from dssdb.mssfile where dsid = 'ds"+args.dsnum+"' and type = 'P' and status = 'P'");
	    if (query.submit(server) < 0) {
		metautils::log_error("error getting group indexes: "+query.error(),"scm",user,args.args_string);
	    }
	    while (query.fetch_row(row)) {
		local_args.gindex_list.emplace_back(row[0]);
	    }
	  }
	}
	if (local_args.update_graphics) {
	  for (const auto& gindex : local_args.gindex_list) {
	    std::stringstream output,error;
	    mysystem2(directives.local_root+"/bin/gsi -g "+gindex+" "+args.dsnum,output,error);
	  }
	}
	for (const auto& gindex : local_args.gindex_list) {
	  summarizeMetadata::create_file_list_cache("MSS","scm",user,args.args_string,gindex);
	}
	det.strings.emplace_back("MSS");
	pthread_create(&det.tid,NULL,thread_generate_detailed_metadata_view,&det);
	tid_list.emplace_back(det.tid);
	ThreadStruct ts;
	pthread_create(&ts.tid,NULL,thread_summarize_dates,NULL);
	tid_list.emplace_back(ts.tid);
	pthread_create(&ts.tid,NULL,thread_summarize_formats,NULL);
	tid_list.emplace_back(ts.tid);
	flist.strings.emplace_back("MSS");
	flist.strings.emplace_back("");
	pthread_create(&flist.tid,NULL,thread_create_file_list_cache,&flist);
	tid_list.emplace_back(flist.tid);
    }
    else if (local_args.summarized_web_file || local_args.refresh_web) {
	if (local_args.refresh_web && MySQL::table_exists(server,"WGrML.ds"+local_args.dsnum2+"_agrids_cache")) {
	  summarizeMetadata::grids::summarize_grids(args.dsnum,"WGrML","scm",user,args.args_string);
	  agg.strings.emplace_back("WGrML");
	  pthread_create(&agg.tid,nullptr,thread_aggregate_grids,&agg);
	  tid_list.emplace_back(agg.tid);
	}
	if (local_args.refresh_web && local_args.gindex_list.size() == 1) {
	  if (local_args.gindex_list.front() == "all") {
	    local_args.gindex_list.clear();
	    query.set("select distinct tindex from dssdb.wfile where dsid = 'ds"+args.dsnum+"' and type = 'D' and status = 'P'");
	    if (query.submit(server) < 0) {
		metautils::log_error("error getting group indexes: "+query.error(),"scm",user,args.args_string);
	    }
	    while (query.fetch_row(row)) {
		local_args.gindex_list.emplace_back(row[0]);
	    }
	  }
	}
	if (local_args.update_graphics) {
	  for (const auto& gindex : local_args.gindex_list) {
	    std::stringstream output,error;
	    mysystem2(directives.local_root+"/bin/wgsi -g "+gindex+" "+args.dsnum,output,error);
	  }
	}
	for (const auto& gindex : local_args.gindex_list) {
	  summarizeMetadata::create_file_list_cache("Web","scm",user,args.args_string,gindex);
	  if (!local_args.summarized_web_file) {
	    summarizeMetadata::create_file_list_cache("inv","scm",user,args.args_string,gindex);
	  }
	}
	det.strings.emplace_back("Web");
	pthread_create(&det.tid,NULL,thread_generate_detailed_metadata_view,&det);
	tid_list.emplace_back(det.tid);
	flist.strings.emplace_back("Web");
	flist.strings.emplace_back("");
	pthread_create(&flist.tid,NULL,thread_create_file_list_cache,&flist);
	tid_list.emplace_back(flist.tid);
	if (!local_args.summarized_web_file) {
	  flist_i.strings.emplace_back("inv");
	  flist_i.strings.emplace_back("");
	  pthread_create(&flist_i.tid,NULL,thread_create_file_list_cache,&flist_i);
	  tid_list.emplace_back(flist_i.tid);
	}
    }
    else if (local_args.refresh_inv) {
	if (local_args.refresh_inv && local_args.gindex_list.size() == 1) {
	  if (local_args.gindex_list.front() == "all") {
	    local_args.gindex_list.clear();
//	    query.set("select distinct tindex from dssdb.wfile where dsid = 'ds"+args.dsnum+"' and type = 'D' and property = 'A'");
query.set("select distinct tindex from dssdb.wfile where dsid = 'ds"+args.dsnum+"' and type = 'D' and status = 'P'");
	    if (query.submit(server) < 0) {
		metautils::log_error("error getting group indexes: "+query.error(),"scm",user,args.args_string);
	    }
	    while (query.fetch_row(row)) {
		local_args.gindex_list.emplace_back(row[0]);
	    }
	  }
	}
	for (const auto& gindex : local_args.gindex_list) {
	  summarizeMetadata::create_file_list_cache("inv","scm",user,args.args_string,gindex);
	}
	summarizeMetadata::create_file_list_cache("inv","scm",user,args.args_string);
    }
  }
  ThreadStruct *tkml=nullptr;
  if (kml_list.size() > 0) {
    tkml=new ThreadStruct[kml_list.size()];
    size_t n=0;
    for (const auto& item : kml_list) {
	tkml[n].strings.emplace_back(item.observation_type);
	tkml[n].strings.emplace_back(item.platform_type);
	tkml[n].strings.emplace_back(item.observation_type_code);
	tkml[n].strings.emplace_back(item.platform_type_code);
	if (local_args.verbose) {
	  std::cout << "starting create_kml for '" << tkml[n].strings[0] << "' / '" << tkml[n].strings[1] << "' at " << current_date_time().to_string() << std::endl;
	}
/*
	pthread_create(&(tkml[n].tid),NULL,create_kml,(void *)&(tkml[n]));
	tid_list.emplace_back(tkml[n].tid);
*/
	++n;
    }
  }
  for (const auto& tid : tid_list) {
    pthread_join(tid,NULL);
  }
  if (kml_list.size() > 0) {
    delete[] tkml;
  }
  if (args.regenerate && (local_args.summarized_hpss_file || !local_args.summarized_web_file)) {
    std::stringstream output,error;
    mysystem2(directives.local_root+"/bin/dsgen "+args.dsnum,output,error);
  }
  if (local_args.notify) {
    std::cout << "scm has completed successfully" << std::endl;
  }
  auto t2=std::time(nullptr);
  metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","scm.time",user,args.args_string);
}
