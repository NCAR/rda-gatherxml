#include <iostream>
#include <fstream>
#include <unordered_set>
#include <set>
#include <regex>
#include <signal.h>
#include <gatherxml.hpp>
#include <utils.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

extern "C" void clean_up()
{
  if (!myerror.empty()) {
    metautils::log_error(myerror,"prop2xml",USER);
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","ascii2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

DateTime fill_date(std::string date_s,const std::string& line)
{
  DateTime dt;
  auto dt_parts=strutils::split(date_s);
  if (dt_parts.size() != 1 && dt_parts.size() != 3) {
    std::cerr << "Error: invalid date (" << date_s << ") on line:\n" << line << std::endl;
    exit(1);
  }
  strutils::replace_all(dt_parts[0],"-","");
  if (dt_parts[0].length() != 8) {
    std::cerr << "Error: invalid date (" << date_s << ") on line:\n" << line << std::endl;
    exit(1);
  }
  switch (dt_parts.size()) {
    case 1:
    {
	if (date_s.empty()) {
	  std::cerr << "Error: start date without an end date on line:\n" << line << std::endl;
	  exit(1);
	}
	dt.set(std::stoll(dt_parts[0])*1000000+999999);
	break;
    }
    case 3:
    {
	if (dt_parts[1].length() != 2 && dt_parts[1].length() != 5 && dt_parts[1].length() != 8) {
	  std::cerr << "Error: invalid time (" << dt_parts[1] << ") on line:\n" << line << std::endl;
	  exit(1);
	}
	strutils::replace_all(dt_parts[1],":","");
	while (dt_parts[1].length() < 6) {
	  dt_parts[1]+="99";
	}
	dt.set(std::stoll(dt_parts[0])*1000000+std::stoll(dt_parts[1]));
	dt.set_utc_offset(std::stoi(dt_parts[2]));
	break;
    }
  }
  return dt;
}

void process_variable_list(const std::string& line,std::unordered_map<std::string,std::tuple<std::string,std::set<std::string>>>& variables)
{
  auto fields=strutils::split(line,",");
  for (auto& field : fields) {
    strutils::trim(field);
    strutils::replace_all(field,"__COMMA__",",");
    strutils::replace_all(field,"__SLASH__","\\");
  }
  auto varlist_id=fields[0].substr(fields[0].find("=")+1);
  if (!std::regex_search(fields[1],std::regex("^description="))) {
    std::cerr << "Error: missing variable list description on line:\n" << line << std::endl;
    exit(1);
  }
  auto description=fields[1].substr(fields[1].find("=")+1);
  if (variables.find(varlist_id) == variables.end()) {
    variables.emplace(varlist_id,std::make_tuple(description,std::set<std::string>()));
  }
  else {
    std::cerr << "Error: variable list '" << varlist_id << "' redefined on line\n:" << line << std::endl;
    exit(1);
  }
  for (size_t n=2; n < fields.size(); ++n) {
    std::get<1>(variables[varlist_id]).emplace(fields[n]);
  }
}

void process_observation(const std::string& line,const std::unordered_set<std::string>& platform_types,const std::unordered_set<std::string>& id_types,std::unordered_map<std::string,std::tuple<std::string,std::set<std::string>>>& variables,std::unordered_map<std::string,std::string>& unique_datatypes,gatherxml::markup::ObML::ObservationData& obs_data)
{
  auto fields=strutils::split(line,",");
  if (fields.size() != 9) {
    std::cerr << "Error: wrong number of fields (" << fields.size() << ") on line:\n" << line << std::endl;
    exit(1);
  }
  for (auto& field : fields) {
    strutils::trim(field);
    strutils::replace_all(field,"__COMMA__",",");
    strutils::replace_all(field,"__SLASH__","\\");
  }
  if (obs_data.observation_indexes.find(fields[0]) == obs_data.observation_indexes.end()) {
    std::cerr << "Error: invalid observation type '" << fields[0] << "' on line:\n" << line << std::endl;
    exit(1);
  }
  std::string platform_key;
  if (platform_types.find(fields[1]) == platform_types.end()) {
    std::cerr << "Error: invalid platform type '" << fields[1] << "' on line:\n" << line << std::endl;
    exit(1);
  }
  else {
    platform_key=fields[1];
  }
  gatherxml::markup::ObML::IDEntry ientry;
  if (id_types.find(fields[2]) == id_types.end()) {
    std::cerr << "Error: invalid station ID type '" << fields[2] << "' on line:\n" << line << std::endl;
    exit(1);
  }
  else {
    strutils::replace_all(fields[3],"\"","&quot;");
    ientry.key=platform_key+"[!]"+fields[2]+"[!]"+fields[3];
  }
  auto lat=std::stof(fields[4]);
  if (lat < -90. || lat > 90.) {
    std::cerr << "Error: latitude (" << fields[4] << ") out of range on line:\n" << line << std::endl;
    exit(1);
  }
  auto lon=std::stof(fields[5]);
  if (lon < -180. || lon > 180.) {
    std::cerr << "Error: longitude (" << fields[5] << ") out of range on line:\n" << line << std::endl;
    exit(1);
  }
  auto dtype_parts=strutils::split(fields[8],"|");
  if (dtype_parts.size() == 1) {
    if (variables.find(fields[8]) == variables.end()) {
	std::cerr << "Error: missing variable list for '" << fields[8] << "', found on line:\n" << line << std::endl;
	exit(1);
    }
    else {
	dtype_parts.emplace_back("");
    }
  }
  else if (dtype_parts.size() != 2) {
    std::cerr << "Error: bad data type '" << fields[8] << "' on line:\n" << line << std::endl;
    exit(1);
  }
  if (!obs_data.added_to_platforms(fields[0],platform_key,lat,lon)) {
    std::cerr << "Error: " << myerror << std::endl;
    exit(1);
  }
  auto start_date=fill_date(fields[6],line);
  if (fields[7].empty()) {
    if (!obs_data.added_to_ids(fields[0],ientry,dtype_parts.front(),"",lat,lon,std::stod(start_date.to_string("%Y%m%d%H%MM%SS")),&start_date)) {
	std::cerr << "Error: " << myerror << std::endl;
	exit(1);
    }
  }
  else {
    auto end_date=fill_date(fields[7],line);
    auto ts=(std::stod(start_date.to_string("%Y%m%d%H%MM%SS"))+std::stod(end_date.to_string("%Y%m%d%H%MM%SS")))/2.;
    if (!obs_data.added_to_ids(fields[0],ientry,dtype_parts.front(),"",lat,lon,ts,&start_date,&end_date)) {
	std::cerr << "Error: " << myerror << std::endl;
	exit(1);
    }
  }
  if (unique_datatypes.find(dtype_parts.front()) == unique_datatypes.end()) {
    unique_datatypes.emplace(dtype_parts.front(),dtype_parts.back());
  }
}

void scan_input_file(gatherxml::markup::ObML::ObservationData& obs_data)
{
  std::ifstream ifs(metautils::args.local_name);
  if (!ifs.is_open()) {
    std::cerr << "Error opening '" << metautils::args.local_name << "' for input" << std::endl;
    exit(1);
  }
  long long num_input_lines=0;
  if (gatherxml::verbose_operation) {
    std::stringstream oss,ess;
    unixutils::mysystem2("/bin/tcsh -c \"wc -l "+metautils::args.local_name+" |awk '{print $1}'\"",oss,ess);
    num_input_lines=std::stoll(oss.str());
    std::cout << "Beginning scan of input file '"+metautils::args.local_name+"' containing " << num_input_lines << " lines ..." << std::endl;
  }
  std::unordered_set<std::string> platform_types,id_types;
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("scan_input_file(): unable to connect to the metadata database","prop2xml",USER);
  }
  MySQL::LocalQuery query("platform_type","ObML.platform_types","platform_type != 'unknown'");
  if (query.submit(server) != 0) {
    metautils::log_error("scan_input_file(): platform_types query error '"+query.error()+"'","prop2xml",USER);
  }
  MySQL::Row row;
  while (query.fetch_row(row)) {
    if (!row[0].empty()) {
	platform_types.emplace(row[0]);
    }
  }
  query.set("id_type","ObML.id_types","id_type != 'generic' and id_type != 'unknown'");
  if (query.submit(server) != 0) {
    metautils::log_error("scan_input_file(): id_types query error '"+query.error()+"'","prop2xml",USER);
  }
  while (query.fetch_row(row)) {
    if (!row[0].empty()) {
	id_types.emplace(row[0]);
    }
  }
  std::regex varlist_re("^varlist=");
  std::unordered_map<std::string,std::string> unique_datatypes;
  std::unordered_map<std::string,std::tuple<std::string,std::set<std::string>>> variables;
  const size_t LINE_LENGTH=32768;
  std::unique_ptr<char[]> line(new char[LINE_LENGTH]);
  ifs.getline(line.get(),LINE_LENGTH);
  auto num_lines=0;
  while (!ifs.eof()) {
    std::string sline=line.get();
    strutils::replace_all(sline,"\\\\","__SLASH__");
    strutils::replace_all(sline,"\\,","__COMMA__");
    if (std::regex_search(sline,varlist_re)) {
	process_variable_list(sline,variables);
    }
    else {
	process_observation(sline,platform_types,id_types,variables,unique_datatypes,obs_data);
    }
    ++num_lines;
    if (gatherxml::verbose_operation && (num_lines % 10000) == 0) {
	std::cout << "Processed " << num_lines << " input lines out of a total of " << num_input_lines << std::endl;
    }
    ifs.getline(line.get(),LINE_LENGTH);
  }
  ifs.close();
  if (gatherxml::verbose_operation) {
    std::cout << "... scanning of input file is completed." << std::endl;
  }
  server.disconnect();
  TempDir tdir;
  std::stringstream oss,ess;
  if (!tdir.create(metautils::directives.temp_path) || unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata/ParameterTables",oss,ess) < 0) {
    metautils::log_error("can't create temporary directory for data type map","prop2xml",USER);
  }
  auto existing_datatype_map=unixutils::remote_web_file("https://rda.ucar.edu/metadata/ParameterTables/"+metautils::args.data_format+".ds"+metautils::args.dsnum+".xml",tdir.name());
  std::vector<std::string> existing_map_contents;
  if (!existing_datatype_map.empty()) {
    std::ifstream ifs(existing_datatype_map.c_str());
    char line[32768];
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	existing_map_contents.emplace_back(line);
	ifs.getline(line,32768);
    }
    ifs.close();
    existing_map_contents.pop_back();
  }
  if (gatherxml::verbose_operation) {
    std::cout << "Writing parameter map ..." << std::endl;
  }
  std::string datatype_map=tdir.name()+"/metadata/ParameterTables/"+metautils::args.data_format+".ds"+metautils::args.dsnum+".xml";
  std::ofstream ofs(datatype_map.c_str());
  if (!ofs.is_open()) {
    metautils::log_error("unable to write data type map to temporary directory","prop2xml",USER);
  }
  if (existing_map_contents.size() == 0) {
    ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
    ofs << "<dataTypeMap>" << std::endl;
  }
  else {
    std::regex code_re(" code=\"");
    std::regex dt_end_re("</dataType>");
    auto no_write=false;
    for (const auto& line : existing_map_contents) {
	if (std::regex_search(line,code_re)) {
	  auto parts=strutils::split(line,"\"");
	  if (unique_datatypes.find(parts[1]) != unique_datatypes.end()) {
	    no_write=true;
	  }
	}
	if (!no_write) {
	  ofs << line << std::endl;
	}
	if (std::regex_search(line,dt_end_re)) {
	  no_write=false;
	}
    }
  }
  for (const auto& datatype : unique_datatypes) {
    ofs << "  <dataType code=\"" << datatype.first << "\">" << std::endl;
    ofs << "    <description>";
    if (!datatype.second.empty()) {
	ofs << datatype.second << "</description>" << std::endl;
    }
    else {
	auto e=variables.find(datatype.first);
	if (e != variables.end()) {
	  ofs << std::get<0>(e->second) << "</description>" << std::endl;
	  for (const auto& variable : std::get<1>(e->second)) {
	    ofs << "    <variable>" << variable << "</variable>" << std::endl;
	  }
	}
	else {
	  std::cerr << "Error: missing variable list for '" << datatype.first << "'" << std::endl;
	  exit(1);
	}
    }
    ofs << "  </dataType>" << std::endl;
  }
  ofs << "</dataTypeMap>" << std::endl;
  ofs.close();
  std::string error;
  if (unixutils::rdadata_sync(tdir.name(),"metadata/ParameterTables/","/data/web",metautils::directives.rdadata_home,error) < 0) {
    metautils::log_error("unable to sync data type map - error(s): '"+error+"'","prop2xml",USER);
  }
  if (unixutils::mysystem2("/bin/cp "+datatype_map+" "+metautils::directives.parameter_map_path+"/",oss,ess) < 0) {
    metautils::log_warning("sync of data type map to share directory failed - error(s): '"+error+"'","prop2xml",USER);
  }
  if (gatherxml::verbose_operation) {
    std::cout << "... parameter map written." << std::endl;
  }
}

int main(int argc,char **argv)
{
  if (argc < 7) {
    std::cerr << "usage: prop2xml -f format -d [ds]nnn.n -l <csv-file> path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f ascii    ASCII data file formats" << std::endl;
    std::cerr << "-f binary   Binary data file formats" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-l <csv-file>   input comma-separated-values file" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>    full HPSS path or URL of the file to read" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  auto arg_delimiter='!';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("prop2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  if (metautils::args.data_format != "ascii" && metautils::args.data_format != "binary") {
    std::cerr << "Error: invalid data format" << std::endl;
    exit(1);
  }
  else {
    if (metautils::args.data_format == "ascii") {
	metautils::args.data_format="ASCII";
    }
    else if (metautils::args.data_format == "binary") {
	metautils::args.data_format="Binary";
    }
    metautils::args.data_format.insert(0,"proprietary_");
  }
  atexit(clean_up);
  metautils::cmd_register("prop2xml",USER);
  if (metautils::args.dsnum != "999.9") {
    auto verified_file=false;
    MySQL::Server server(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
    std::string file_type;
    MySQL::LocalQuery query;
    if (std::regex_search(metautils::args.path,std::regex("^/FS/DECS"))) {
	query.set("dsid","mssfile","mssfile = '"+metautils::args.path+"/"+metautils::args.filename+"' and dsid = 'ds"+metautils::args.dsnum+"'");
    }
    else {
	auto path=metautils::args.path;
	strutils::replace_all(path,"https://rda.ucar.edu/data/ds"+metautils::args.dsnum,"");
	if (!path.empty()) {
	  path=path.substr(1)+"/"+metautils::args.filename;
	}
	else {
	  path=metautils::args.filename;
	}
	query.set("dsid","wfile","wfile = '"+path+"' and dsid = 'ds"+metautils::args.dsnum+"'");
    }
    if (query.submit(server) == 0) {
	if (query.num_rows() > 0) {
	  MySQL::Row row;
	  if (query.fetch_row(row)) {
	    if (row[0] == "ds"+metautils::args.dsnum) {
		verified_file=true;
	    }
	  }
	  else {
	    metautils::log_error("database fetch error","prop2xml",USER);
	  }
	}
    }
    else {
	metautils::log_error("database connection error","prop2xml",USER);
    }
    server.disconnect();
    if (!verified_file) {
	std::cerr << "Error: the data file you specified is not in RDADB" << std::endl;
	exit(1);
    }
  }
  gatherxml::markup::ObML::ObservationData obs_data;
  if (obs_data.num_types == 0) {
    metautils::log_error("scan_input_file(): unable to initialize for observations","prop2xml",USER);
  }
  scan_input_file(obs_data);
  gatherxml::markup::ObML::write(obs_data,"prop2xml",USER);
  if (metautils::args.update_db) {
    std::string flags="-f";
    if (!std::regex_search(metautils::args.path,std::regex("^/FS/DECS"))) {
	flags="-wf";
    }
    if (!metautils::args.regenerate) {
	flags="-R "+flags;
    }
    if (!metautils::args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (gatherxml::verbose_operation) {
	std::cout << "Calling 'scm' ..." << std::endl;
    }
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" "+flags+" "+metautils::args.filename+".ObML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (gatherxml::verbose_operation) {
	std::cout << "... 'scm' completed." << std::endl;
    }
  }
}
