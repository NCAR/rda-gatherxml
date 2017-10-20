#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <vector>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <tempfile.hpp>

metautils::Directives directives;
metautils::Args args;
struct LocalArgs {
  LocalArgs() : dsnum2(),create_cache(false),notify(false),verbose(false) {}

  std::string dsnum2;
  bool create_cache,notify,verbose;
} local_args;
struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};
struct InventoryEntry {
  InventoryEntry() : key(),list(nullptr) {}

  std::string key;
  std::shared_ptr<std::list<std::string>> list;
};
struct AncillaryEntry {
  AncillaryEntry() : key(),code(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> code;
};
std::string user=getenv("USER");
std::string server_root=strutils::token(host_name(),".",0);
std::string tindex;
TempDir temp_dir;

void parse_args(int argc,char **argv)
{
  local_args.create_cache=true;
  local_args.notify=false;
  local_args.verbose=false;
  args.args_string=unix_args_string(argc,argv);
  auto sp=strutils::split(args.args_string,":");
  for (size_t n=0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	}
	local_args.dsnum2=strutils::substitute(args.dsnum,".","");
    }
    else if (sp[n] == "-f") {
	args.filename=sp[++n];
    }
    else if (sp[n] == "-C") {
	local_args.create_cache=false;
    }
    else if (sp[n] == "-N") {
	local_args.notify=true;
    }
    else if (sp[n] == "-V") {
	local_args.verbose=true;
    }
  }
}

extern "C" void segv_handler(int)
{
  metautils::log_error("Error: core dump","iinv",user,args.args_string);
}

struct TimeRangeEntry {
  TimeRangeEntry() : code(),hour_diff(0x7fffffff) {}

  std::string code;
  int hour_diff;
};

void insert_grml_inventory()
{
  std::deque<std::string> sp,spx;
  std::vector<TimeRangeEntry> time_range_codes;
  std::list<std::string> lock_table_list;

  MySQL::Server server;
  metautils::connect_to_metadata_server(server);
  auto grml_filename=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/inv/"+args.filename+".gz",temp_dir.name());
  struct stat buf;
  if (stat(grml_filename.c_str(),&buf) == 0) {
    system(("gunzip "+grml_filename).c_str());
    strutils::chop(grml_filename,3);
  }
  else {
    grml_filename=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/inv/"+args.filename,temp_dir.name());
  }
  std::ifstream ifs(grml_filename.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("insert_grml_inventory() was not able to open "+args.filename,"iinv",user,args.args_string);
  }
  grml_filename=strutils::substitute(args.filename,".GrML_inv","");
  strutils::replace_all(grml_filename,"%","/");
  MySQL::LocalQuery query("select code,format_code,tindex from WGrML.ds"+local_args.dsnum2+"_webfiles as w left join dssdb.wfile as x on (x.dsid = 'ds"+args.dsnum+"' and x.type = 'D' and x.wfile = w.webID) where webID = '"+grml_filename+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while looking for code from webfiles","iinv",user,args.args_string);
  }
  if (query.num_rows() == 0) {
    metautils::log_error("insert_grml_inventory() did not find "+grml_filename+" in WGrML.ds"+local_args.dsnum2+"_webfiles","iinv",user,args.args_string);
  }
  MySQL::Row row;
  query.fetch_row(row);
  auto web_id_code=row[0];
  auto format_code=row[1];
  tindex=row[2];
  if (!MySQL::table_exists(server,"IGrML.ds"+local_args.dsnum2+"_inventory_summary")) {
    std::string result;
    if (server.command("create table IGrML.ds"+local_args.dsnum2+"_inventory_summary like IGrML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create inventory_summary table","iinv",user,args.args_string);
    }
  }
  struct InitTimeEntry {
    InitTimeEntry() : key(),time_range_codes(nullptr) {}

    std::string key;
    std::shared_ptr<my::map<StringEntry>> time_range_codes;
  };
  my::map<InitTimeEntry> init_dates_table;
  std::string dupe_vdates="N";
  std::string uflag=strutils::strand(3);
  int nlines=0,num_dupes=0;
  long long total_bytes=0;
  std::vector<std::string> grid_definition_codes,level_codes,parameters,processes,ensembles;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    nlines++;
    std::string sline=line;
    if (std::regex_search(sline,std::regex("<!>"))) {
	sp=strutils::split(sline,"<!>");
	switch (sp[0][0]) {
	  case 'U':
	  {
	    TimeRangeEntry tre;
	    if (sp[2] == "Analysis" || std::regex_search(sp[2],std::regex("^0-hour")) || sp[2] == "Monthly Mean") {
		tre.hour_diff=0;
	    }
	    else if (std::regex_search(sp[2],std::regex("-hour Forecast$"))) {
		tre.hour_diff=std::stoi(sp[2].substr(0,sp[2].find("-")));
	    }
	    else if (std::regex_search(sp[2],std::regex("to initial\\+"))) {
		auto hr=sp[2].substr(sp[2].find("to initial+")+11);
		strutils::chop(hr);
		tre.hour_diff=std::stoi(hr);
	    }
	    else {
		metautils::log_warning("insert_grml_inventory() does not recognize product '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    query.set("code","WGrML.timeRanges","timeRange = '"+sp[2]+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get timeRange code","iinv",user,args.args_string);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no timeRange code for '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    query.fetch_row(row);
	    tre.code=row[0];
	    time_range_codes.emplace_back(tre);
	    break;
	  }
	  case 'G':
	  {
	    spx=strutils::split(sp[2],",");
	    std::string definition,definition_parameters;
	    switch (std::stoi(spx[0])) {
		case Grid::latitudeLongitudeType:
		case Grid::gaussianLatitudeLongitudeType:
		{
		  if (std::stoi(spx[0]) == Grid::latitudeLongitudeType) {
		    definition="latLon";
		  }
		  else if (std::stoi(spx[0]) == Grid::gaussianLatitudeLongitudeType) {
		    definition="gaussLatLon";
		  }
		  definition_parameters=spx[1]+":"+spx[2]+":";
		  if (std::regex_search(spx[3],std::regex("^-"))) {
		    definition_parameters+=spx[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[3]+"N:";
		  }
		  if (std::regex_search(spx[4],std::regex("^-"))) {
		    definition_parameters+=spx[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[4]+"E:";
		  }
		  if (std::regex_search(spx[5],std::regex("^-"))) {
		    definition_parameters+=spx[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[5]+"N:";
		  }
		  if (std::regex_search(spx[6],std::regex("^-"))) {
		    definition_parameters+=spx[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[6]+"E:";
		  }
		  definition_parameters+=spx[7]+":"+spx[8];
		  break;
		}
		case Grid::mercatorType:
		{
		  definition="mercator";
		  definition_parameters=spx[1]+":"+spx[2]+":";
		  if (std::regex_search(spx[3],std::regex("^-"))) {
		    definition_parameters+=spx[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[3]+"N:";
		  }
		  if (std::regex_search(spx[4],std::regex("^-"))) {
		    definition_parameters+=spx[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[4]+"E:";
		  }
		  if (std::regex_search(spx[5],std::regex("^-"))) {
		    definition_parameters+=spx[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[5]+"N:";
		  }
		  if (std::regex_search(spx[6],std::regex("^-"))) {
		    definition_parameters+=spx[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[6]+"E:";
		  }
		  definition_parameters+=spx[7]+":"+spx[8]+":";
		  if (std::regex_search(spx[9],std::regex("^-"))) {
		    definition_parameters+=spx[9].substr(1)+"S";
		  }
		  else {
		    definition_parameters+=spx[9]+"N";
		  }
		  break;
		}
		case Grid::lambertConformalType:
		{
		  definition="lambertConformal";
		  definition_parameters=spx[1]+":"+spx[2]+":";
		  if (std::regex_search(spx[3],std::regex("^-"))) {
		    definition_parameters+=spx[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[3]+"N:";
		  }
		  if (std::regex_search(spx[4],std::regex("^-"))) {
		    definition_parameters+=spx[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[4]+"E:";
		  }
		  if (std::regex_search(spx[5],std::regex("^-"))) {
		    definition_parameters+=spx[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[5]+"N:";
		  }
		  if (std::regex_search(spx[6],std::regex("^-"))) {
		    definition_parameters+=spx[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=spx[6]+"E:";
		  }
		  definition_parameters+=spx[9]+":"+spx[7]+":"+spx[8]+":";
		  if (std::regex_search(spx[10],std::regex("^-"))) {
		    definition_parameters+=spx[10].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=spx[10]+"N:";
		  }
		  if (std::regex_search(spx[11],std::regex("^-"))) {
		    definition_parameters+=spx[11].substr(1)+"S";
		  }
		  else {
		    definition_parameters+=spx[11]+"N";
		  }
		  break;
		}
		case Grid::sphericalHarmonicsType:
		{
		  definition="sphericalHarmonics";
		  definition_parameters=spx[1]+":"+spx[2]+":"+spx[3];
		  break;
		}
		default:
		{
		  metautils::log_error("insert_grml_inventory() does not understand grid type "+spx[0],"iinv",user,args.args_string);
		}
	    }
	    query.set("code","WGrML.gridDefinitions","definition = '"+definition+"' and defParams = '"+definition_parameters+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get gridDefinition code","iinv",user,args.args_string);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no gridDefinition code for '"+definition+","+definition_parameters+"'","iinv",user,args.args_string);
	    }
	    query.fetch_row(row);
	    grid_definition_codes.emplace_back(row[0]);
	    break;
	  }
	  case 'L':
	  {
	    spx=strutils::split(sp[2],":");
	    if (spx.size() < 2 || spx.size() > 3) {
		metautils::log_error("insert_grml_inventory() found bad level code: "+sp[2],"iinv",user,args.args_string);
	    }
	    std::string map,type;
	    if (std::regex_search(spx[0],std::regex(","))) {
		auto idx=spx[0].find(",");
		map=spx[0].substr(0,idx);
		type=spx[0].substr(idx+1);
	    }
	    else {
		map="";
		type=spx[0];
	    }
	    std::string value;
	    switch (spx.size()) {
		case 2:
		{
		  value=spx[1];
		  break;
		}
		case 3:
		{
		  value=spx[2]+","+spx[1];
		  break;
		}
	    }
	    query.set("code","WGrML.levels","map = '"+map+"' and type = '"+type+"' and value = '"+value+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get level code","iinv",user,args.args_string);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no level code for '"+map+","+type+","+value+"'","iinv",user,args.args_string);
	    }
	    query.fetch_row(row);
	    level_codes.emplace_back(row[0]);
	    break;
	  }
	  case 'P':
	  {
	    parameters.emplace_back(sp[2]);
	    auto pcode=format_code+"!"+sp[2];
	    if (!MySQL::table_exists(server,"IGrML.ds"+local_args.dsnum2+"_inventory_"+pcode)) {
		std::string result;
		if (sp.size() > 3 && sp[3] == "BIG") {
		  if (server.command("create table IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"` like IGrML.template_inventory_p_big",result) < 0) {
		    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create parameter inventory table","iinv",user,args.args_string);
		  }
		}
		else {
		  if (server.command("create table IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"` like IGrML.template_inventory_p",result) < 0) {
		    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create parameter inventory table","iinv",user,args.args_string);
		  }
		}
	    }
	    lock_table_list.emplace_back(pcode);
	    break;
	  }
	  case 'R':
	  {
	    processes.emplace_back(sp[2]);
	    break;
	  }
	  case 'E':
	  {
	    ensembles.emplace_back(sp[2]);
	    break;
	  }
	}
    }
    else if (std::regex_search(sline,std::regex("\\|"))) {
/*
	if (first) {
	  sdum="lock tables ";
	  n=0;
	  for (auto& item : lock_table_list) {
	    if (n > 0) {
		sdum+=", ";
	    }
	    sdum+="IGrML.`ds"+local_args.dsnum2+"_inventory_"+item+"` write";
	    ++n;
	  }
	  if (server.issueCommand(sdum,error) < 0) {
	    metautils::log_error("unable to lock parameter tables; error: "+server.error(),"iinv",user,args.args_string);
	  }
	  first=false;
	}
*/
	sp=strutils::split(sline,"|");
	total_bytes+=std::stoll(sp[1]);
	auto tr_index=std::stoi(sp[3]);
	std::string init_date;
	if (time_range_codes[tr_index].hour_diff != 0x7fffffff) {
	  init_date=DateTime(std::stoll(sp[2])*100).hours_subtracted(time_range_codes[tr_index].hour_diff).to_string("%Y%m%d%H%MM");
	  if (dupe_vdates == "N") {
	    InitTimeEntry ite;
	    ite.key=init_date;
	    if (!init_dates_table.found(ite.key,ite)) {
		ite.time_range_codes.reset(new my::map<StringEntry>);
		init_dates_table.insert(ite);
	    }
	    StringEntry se;
	    se.key=time_range_codes[tr_index].code;
	    if (!ite.time_range_codes->found(se.key,se)) {
		ite.time_range_codes->insert(se);
	    }
	    else if (ite.time_range_codes->size() > 1) {
		dupe_vdates="Y";
	    }
	  }
	}
	else {
	  init_date="0";
	}
	auto insert_string=web_id_code+","+sp[0]+","+sp[1]+","+sp[2]+","+init_date+","+time_range_codes[tr_index].code+","+grid_definition_codes[std::stoi(sp[4])]+","+level_codes[std::stoi(sp[5])]+",";
	if (sp.size() > 7 && !sp[7].empty()) {
	  insert_string+="'"+processes[std::stoi(sp[7])]+"',";
	}
	else {
	  insert_string+="'',";
	}
	if (sp.size() > 8 && !sp[8].empty()) {
	  insert_string+="'"+ensembles[std::stoi(sp[8])]+"'";
	}
	else {
	  insert_string+="''";
	}
	insert_string+=",'"+uflag+"'";
	auto pcode=format_code+"!"+parameters[std::stoi(sp[6])];
	if (server.insert("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","webID_code,byte_offset,byte_length,valid_date,init_date,timeRange_code,gridDefinition_code,level_code,process,ensemble,uflag",insert_string,"") < 0) {
	  if (!std::regex_search(server.error(),std::regex("Duplicate entry"))) {
	    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while inserting row '"+insert_string+"'","iinv",user,args.args_string);
	  }
	  else {
	    std::string dupe_where="webID_code = "+web_id_code+" and valid_date = "+sp[2]+" and timeRange_code = "+time_range_codes[tr_index].code+" and gridDefinition_code = "+grid_definition_codes[std::stoi(sp[4])]+" and level_code = "+level_codes[std::stoi(sp[5])];
	    if (sp.size() > 7 && !sp[7].empty()) {
		dupe_where+=" and process = '"+processes[std::stoi(sp[7])]+"'";
	    }
	    else {
		dupe_where+=" and process = ''";
	    }
	    if (sp.size() > 8 && !sp[8].empty()) {
		dupe_where+=" and ensemble = '"+ensembles[std::stoi(sp[8])]+"'";
	    }
	    else {
		dupe_where+=" and ensemble = ''";
	    }
	    query.set("uflag","IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`",dupe_where);
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to get flag for duplicate row: '"+dupe_where+"'","iinv",user,args.args_string);
	    }
	    if (row[0] == uflag) {
		++num_dupes;
		if (local_args.verbose) {
		  std::cout << "**duplicate ignored - line " << nlines << std::endl;
		}
	    }
	    else {
		if (server.update("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","byte_offset = "+sp[0]+", byte_length = "+sp[1]+",init_date = "+init_date+",uflag = '"+uflag+"'",dupe_where) < 0) {
		  metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while updating duplicate row: '"+dupe_where+"'","iinv",user,args.args_string);
		}
	    }
	  }
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (const auto& item : lock_table_list) {
    server._delete("IGrML.`ds"+local_args.dsnum2+"_inventory_"+item+"`","webID_code = "+web_id_code+" and uflag != '"+uflag+"'");
  }
//  server.issueCommand("unlock tables",error);
  if (server.insert("IGrML.ds"+local_args.dsnum2+"_inventory_summary","webID_code,byte_length,dupe_vdates",web_id_code+","+strutils::lltos(total_bytes)+",'"+dupe_vdates+"'","update byte_length = "+strutils::lltos(total_bytes)+", dupe_vdates = '"+dupe_vdates+"'") < 0) {
    if (!std::regex_search(server.error(),std::regex("Duplicate entry"))) {
	metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while inserting row '"+web_id_code+","+strutils::lltos(total_bytes)+",'"+dupe_vdates+"''","iinv",user,args.args_string);
    }
  }
  server.disconnect();
  if (num_dupes > 0) {
    metautils::log_warning(strutils::itos(num_dupes)+" duplicate grids were ignored","iinv_dupes",user,args.args_string);
  }
}

void check_for_times_table(MySQL::Server& server,std::string type,std::string last_decade)
{
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0")) {
    std::string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0 like IObML.template_"+type+"_times_decade",result) < 0) {
	metautils::log_error("check_for_times_table() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0'","iinv",user,args.args_string);
    }
  }
}

void process_IDs(std::string type,MySQL::Server& server,std::string ID_index,std::string ID_data,std::unordered_map<std::string,std::string>& id_table)
{
  auto id_parts=strutils::split(ID_data,"[!]");
  std::string qspec="select i.code from WObML.ds"+local_args.dsnum2+"_IDs2 as i left join WObML.IDTypes as t on t.code = i.IDType_code where i.ID = '"+id_parts[1]+"' and t.IDType = '"+id_parts[0]+"' and i.sw_lat = "+metadata::ObML::string_coordinate_to_db(id_parts[2])+" and i.sw_lon = "+metadata::ObML::string_coordinate_to_db(id_parts[3]);
  if (id_parts.size() > 4) {
    qspec+=" and i.ne_lat = "+metadata::ObML::string_coordinate_to_db(id_parts[4])+" and ne_lon = "+metadata::ObML::string_coordinate_to_db(id_parts[5]);
  }
  MySQL::LocalQuery query(qspec);
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    std::string error="process_IDs() returned error: "+query.error()+" while trying to get ID code for '"+id_parts[0]+","+id_parts[1]+","+id_parts[2]+","+id_parts[3];
    if (id_parts.size() > 4) {
	error+=","+id_parts[4]+","+id_parts[5];
    }
    error+="'";
    metautils::log_error(error,"iinv",user,args.args_string);
  }
  size_t min_lat_i,min_lon_i,max_lat_i,max_lon_i;
  convert_lat_lon_to_box(36,std::stof(id_parts[2]),std::stof(id_parts[3]),min_lat_i,min_lon_i);
  if (id_parts.size() > 4) {
    convert_lat_lon_to_box(36,std::stof(id_parts[4]),std::stof(id_parts[5]),max_lat_i,max_lon_i);
  }
  else {
    max_lat_i=min_lat_i;
    max_lon_i=min_lon_i;
  }
  for (size_t n=min_lat_i; n <= max_lat_i; ++n) {
    for (size_t m=min_lon_i; m <= max_lon_i; ++m) {
	auto s_lat_i=strutils::itos(n);
	auto s_lon_i=strutils::itos(m);
	std::string check_table="IObML.ds"+local_args.dsnum2+"_inventory_"+s_lat_i+"_"+s_lon_i;
	if (type == "irregular") {
	  check_table+="_b";
	}
	if (!MySQL::table_exists(server,check_table)) {
	  std::string command="create table "+check_table+" like IObML.template_inventory_lati_loni";
	  if (type == "irregular") {
	    command+="_b";
	  }
	  std::string result;
	  if (server.command(command,result) < 0) {
	    metautils::log_error("process_IDs() returned error: "+server.error()+" while trying to create '"+check_table+"'","iinv",user,args.args_string);
	  }
	}
	auto s=row[0]+"|"+strutils::itos(min_lat_i)+"|"+strutils::itos(min_lon_i);
	if (id_parts.size() > 4) {
	  s+="|"+strutils::itos(max_lat_i)+"|"+strutils::itos(max_lon_i);
	}
	id_table.emplace(ID_index,s);
    }
  }
}

struct DataVariableEntry {
  struct Data {
    Data() : var_name(),value_type(),offset(0),byte_len(0),missing_table(99999) {}

    std::string var_name,value_type;
    size_t offset,byte_len;
    my::map<StringEntry> missing_table;
  };
  DataVariableEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct MissingEntry {
  MissingEntry() : key(),time_code(0) {}

  std::string key;
  size_t time_code;
};
void insert_obml_netcdf_time_series_inventory(std::ifstream& ifs,MySQL::Server& server,std::string web_ID_code,size_t rec_size)
{
  my::map<AncillaryEntry> obstype_table,platform_table,datatype_table;
  AncillaryEntry ae;
  my::map<DataVariableEntry> datavar_table;
  DataVariableEntry dve;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::list<std::string> times_list;
  my::map<StringEntry> missing_table(999999);
  StringEntry se;

if (rec_size > 0) {
metautils::log_error("insert_obml_netcdf_time_series_inventory() can't insert for observations with a record dimension","iinv",user,args.args_string);
}
  std::string uflag=strutils::strand(3);
//std::cerr << "uflag='" << uflag << "'" << std::endl;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
    std::string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",user,args.args_string);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",user,args.args_string);
    }
  }
  else {
/*
    if (server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to delete from 'ds"+local_args.dsnum2+"_dataTypes' where webID_code = "+web_ID_code,"iinv",user,args.args_string);
    }
*/
    if (server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to delete from 'ds"+local_args.dsnum2+"_inventory_summary' where webID_code = "+web_ID_code,"iinv",user,args.args_string);
    }
  }
  std::stringstream times;
  std::unordered_map<std::string,std::string> id_table;
  std::string last_decade;
  auto inv_line=std::regex("<!>");
  auto missing_line=std::regex("^([0-9]){1,}\\|");
  auto ndbytes=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (std::regex_search(line,inv_line)) {
	auto sp=strutils::split(line,"<!>");
	switch (sp[0][0]) {
	  case 'D':
	  {
	    dve.key=sp[1];
	    dve.data.reset(new DataVariableEntry::Data);
	    auto sp2=strutils::split(sp[2],"|");
	    dve.data->var_name=sp2[0];
	    dve.data->offset=std::stoi(sp2[1]);
	    dve.data->value_type=sp2[2];
	    dve.data->byte_len=std::stoi(sp2[3]);
	    ndbytes+=dve.data->byte_len;
	    datavar_table.insert(dve);
	    break;
	  }
	  case 'I':
	  {
	    process_IDs("regular",server,sp[1],sp[2],id_table);
	    break;
	  }
	  case 'O':
	    query.set("code","WObML.obsTypes","obsType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new std::string);
	    *ae.code=row[0];
	    obstype_table.insert(ae);
	    break;
	  case 'P':
	    query.set("code","WObML.platformTypes","platformType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get platform code for '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new std::string);
	    *ae.code=row[0];
	    platform_table.insert(ae);
	    break;
	  case 'T':
	  {
	    auto this_decade=sp[2].substr(0,3);
	    if (this_decade != last_decade) {
		if (times.tellp() > 0) {
		  check_for_times_table(server,"timeSeries",last_decade);
		  std::string result;
		  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),uflag=values(uflag)",result) < 0) {
		    metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","iinv",user,args.args_string);
		  }
		  server._delete("IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
		  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0",result);
		}
		times.str("");
	    }
	    times << ",(" << sp[2] << "," << sp[1] << "," << web_ID_code << ",'" << uflag << "')";
	    times_list.emplace_back(sp[2]);
	    last_decade=this_decade;
	    break;
	  }
	}
    }
    else if (std::regex_search(line,missing_line)) {
	se.key=line;
	missing_table.insert(se);
	auto idx=se.key.rfind("|");
	dve.key=se.key.substr(idx+1);
	datavar_table.found(dve.key,dve);
	se.key=se.key.substr(0,idx);
	dve.data->missing_table.insert(se);
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  if (times.tellp() > 0) {
    check_for_times_table(server,"timeSeries",last_decade);
    std::string result;
    if (server.command("insert into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","iinv",user,args.args_string);
    }
    server._delete("IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0",result);
  }
  auto nbytes=0;
  std::string inv_insert;
  inv_insert.reserve(800000);
  for (const auto& id_entry : id_table) {
    auto num_inserts=0;
    auto id_parts=strutils::split(id_entry.second,"|");
    std::string inventory_file="IObML.ds"+local_args.dsnum2+"_inventory_"+id_parts[1]+"_"+id_parts[2];
    for (const auto& datavar : datavar_table.keys()) {
	datavar_table.found(datavar,dve);
	std::string missing_ind;
	if (dve.data->missing_table.size() == 0) {
// no times are missing
	  missing_ind="0";
	}
	else {
	  if ( (times_list.size()-dve.data->missing_table.size()) < dve.data->missing_table.size()) {
// times specified are non-missing
	    missing_ind="1";
	  }
	  else {
// times specified are missing
	    missing_ind="2";
	  }
	}
	for (const auto& obstype : obstype_table.keys()) {
	  AncillaryEntry ae_obs;
	  obstype_table.found(obstype,ae_obs);
	  for (const auto& platform : platform_table.keys()) {
	    AncillaryEntry ae_plat;
	    platform_table.found(platform,ae_plat);
	    ae.key=obstype+"|"+platform+"|"+dve.data->var_name;
	    if (!datatype_table.found(ae.key,ae)) {
		query.set("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+*(ae_obs.code)+" and platformType_code = "+*(ae_plat.code)+" and dataType = 'ds"+args.dsnum+":"+dve.data->var_name+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+*(ae_obs.code)+","+*(ae_plat.code)+",'"+dve.data->var_name+"''","iinv",user,args.args_string);
		}
		ae.code.reset(new std::string);
		*(ae.code)=row[0];
		datatype_table.insert(ae);
		if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypes",web_ID_code+","+id_parts[0]+","+*(ae.code)+",'"+dve.data->value_type+"',"+strutils::itos(dve.data->offset)+","+strutils::itos(dve.data->byte_len)+","+missing_ind+",'"+uflag+"'","update value_type='"+dve.data->value_type+"',byte_offset="+strutils::itos(dve.data->offset)+",byte_length="+strutils::itos(dve.data->byte_len)+",missing_ind="+missing_ind+",uflag='"+uflag+"'") < 0) {
		  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert dataType information for '"+*(ae.code)+"'","iinv",user,args.args_string);
		}
	    }
	    std::string key_end="|"+obstype+"|"+platform+"|"+id_entry.first+"|"+datavar;
	    auto n=0;
	    for (const auto& time : times_list) {
		if (missing_ind != "0") {
		  auto found_missing=missing_table.found(strutils::itos(n)+key_end,se);
		  if ((missing_ind == "1" && !found_missing) || (missing_ind == "2" && found_missing)) {
		    if (num_inserts >= 10000) {
			std::string result;
			if (server.command("insert into "+inventory_file+" values "+inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
			  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",user,args.args_string);
			}
			num_inserts=0;
			inv_insert="";
		    }
		    inv_insert+=",("+web_ID_code+","+time+","+id_parts[0]+","+*(ae.code)+",'"+uflag+"')";
		    ++num_inserts;
		  }
		}
		nbytes+=dve.data->byte_len;
		++n;
	    }
	  }
	}
    }
    std::string result;
    if (!inv_insert.empty()  && server.command("insert into "+inventory_file+" values "+inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",user,args.args_string);
    }
    server._delete(inventory_file,"webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table "+inventory_file,result);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  std::string result;
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_dataTypes",result);
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_summary values ("+web_ID_code+","+strutils::itos(nbytes)+","+strutils::itos(ndbytes)+",'"+uflag+"') on duplicate key update byte_length=values(byte_length),dataType_length=values(dataType_length),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert file size data for '"+web_ID_code+"'","iinv",user,args.args_string);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

struct InsertEntry {
  struct Data {
    Data() : inv_insert(),num_inserts(0) {
	inv_insert.reserve(800000);
    }

    std::string inv_insert;
    int num_inserts;
  };
  InsertEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
void insert_obml_netcdf_point_inventory(std::ifstream& ifs,MySQL::Server& server,std::string web_ID_code,size_t rec_size)
{
  my::map<AncillaryEntry> obstype_table,platform_table,datatype_table;
  AncillaryEntry ae;
  my::map<DataVariableEntry> datavar_table;
  DataVariableEntry dve;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::list<std::string> times_list;
  my::map<StringEntry> missing_table(999999);
  StringEntry se;

if (rec_size > 0) {
metautils::log_error("insert_obml_netcdf_point_inventory() can't insert for observations with a record dimension","iinv",user,args.args_string);
}
  std::string uflag=strutils::strand(3);
//std::cerr << "uflag='" << uflag << "'" << std::endl;
  std::string result;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",user,args.args_string);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",user,args.args_string);
    }
  }
  std::stringstream times;
  std::unordered_map<std::string,std::string> id_table;
  std::string last_decade;
  auto inv_line=std::regex("<!>");
  auto missing_line=std::regex("^([0-9]){1,}\\|");
  auto ndbytes=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (std::regex_search(line,inv_line)) {
	auto sp=strutils::split(line,"<!>");
	switch (sp[0][0]) {
	  case 'D':
	  {
	    dve.key=sp[1];
	    dve.data.reset(new DataVariableEntry::Data);
	    auto sp2=strutils::split(sp[2],"|");
	    dve.data->var_name=sp2[0];
	    dve.data->offset=std::stoi(sp2[1]);
	    dve.data->value_type=sp2[2];
	    dve.data->byte_len=std::stoi(sp2[3]);
	    ndbytes+=dve.data->byte_len;
	    datavar_table.insert(dve);
	    break;
	  }
	  case 'I':
	  {
	    process_IDs("regular",server,sp[1],sp[2],id_table);
	    break;
	  }
	  case 'O':
	  {
	    query.set("code","WObML.obsTypes","obsType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new std::string);
	    *ae.code=row[0];
	    obstype_table.insert(ae);
	    break;
	  }
	  case 'P':
	  {
	    query.set("code","WObML.platformTypes","platformType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get platform code for '"+sp[2]+"'","iinv",user,args.args_string);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new std::string);
	    *ae.code=row[0];
	    platform_table.insert(ae);
	    break;
	  }
	  case 'T':
	  {
	    auto this_decade=sp[2].substr(0,3);
	    if (this_decade != last_decade) {
		if (times.tellp() > 0) {
		  check_for_times_table(server,"point",last_decade);
		  std::string result;
		  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),lat=values(lat),lon=values(lon),uflag=values(uflag)",result) < 0) {
		    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","iinv",user,args.args_string);
		  }
		  server._delete("IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
		  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0",result);
		}
		times.str("");
	    }
	    auto tparts=strutils::split(sp[2],"[!]");
	    times << ",(" << tparts[0] << "," << sp[1] << "," << metadata::ObML::string_coordinate_to_db(tparts[1]) << "," << metadata::ObML::string_coordinate_to_db(tparts[2]) << "," << web_ID_code << ",'" << uflag << "')";
	    times_list.emplace_back(sp[2]);
	    last_decade=this_decade;
	    break;
	  }
	}
    }
    else if (std::regex_search(line,missing_line)) {
	se.key=line;
	missing_table.insert(se);
	auto idx=se.key.rfind("|");
	dve.key=se.key.substr(idx+1);
	datavar_table.found(dve.key,dve);
	se.key=se.key.substr(0,idx);
	dve.data->missing_table.insert(se);
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  if (times.tellp() > 0) {
    check_for_times_table(server,"point",last_decade);
    std::string result;
    if (server.command("insert into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),lat=values(lat),lon=values(lon),uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","iinv",user,args.args_string);
    }
    server._delete("IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0",result);
  }
  std::stringstream data_type_ss;
  auto nbytes=0;
  for (const auto& id_entry : id_table) {
    my::map<InsertEntry> insert_table;
    auto id_parts=strutils::split(id_entry.second,"|");
    for (const auto& datavar : datavar_table.keys()) {
	datavar_table.found(datavar,dve);
	std::string missing_ind;
	if (dve.data->missing_table.size() == 0) {
// no times are missing
	  missing_ind="0";
	}
	else {
	  if ( (times_list.size()-dve.data->missing_table.size()) < dve.data->missing_table.size()) {
// times specified are non-missing
	    missing_ind="1";
	  }
	  else {
// times specified are missing
	    missing_ind="2";
	  }
	}
	for (const auto& obstype : obstype_table.keys()) {
	  AncillaryEntry ae_obs;
	  obstype_table.found(obstype,ae_obs);
	  for (const auto& platform : platform_table.keys()) {
	    AncillaryEntry ae_plat;
	    platform_table.found(platform,ae_plat);
	    ae.key=obstype+"|"+platform+"|"+dve.data->var_name;
	    if (!datatype_table.found(ae.key,ae)) {
		query.set("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+*(ae_obs.code)+" and platformType_code = "+*(ae_plat.code)+" and dataType = 'ds"+args.dsnum+":"+dve.data->var_name+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+*(ae_obs.code)+","+*(ae_plat.code)+",'"+dve.data->var_name+"''","iinv",user,args.args_string);
		}
		ae.code.reset(new std::string);
		*(ae.code)=row[0];
		datatype_table.insert(ae);
		data_type_ss << ",(" << web_ID_code << "," << id_parts[0] << "," << *(ae.code) << ",'" << dve.data->value_type << "'," << dve.data->offset << "," << dve.data->byte_len << "," << missing_ind << ",'" << uflag << "')";
	    }
	    std::string key_end="|"+obstype+"|"+platform+"|"+id_entry.first+"|"+datavar;
	    auto n=0;
	    for (const auto& time : times_list) {
		auto tparts=strutils::split(time,"[!]");
		size_t lat_i,lon_i;
		convert_lat_lon_to_box(36,std::stof(tparts[1]),std::stof(tparts[2]),lat_i,lon_i);
		InsertEntry inse;
		inse.key=strutils::itos(lat_i)+"_"+strutils::itos(lon_i);
		if (!insert_table.found(inse.key,inse)) {
		  inse.data.reset(new InsertEntry::Data);
		  insert_table.insert(inse);
		}
		if (missing_ind != "0") {
		  auto found_missing=missing_table.found(strutils::itos(n)+key_end,se);
		  if ((missing_ind == "1" && !found_missing) || (missing_ind == "2" && found_missing)) {
		    if (inse.data->num_inserts >= 10000) {
			std::string result;
			if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_"+inse.key+" values "+inse.data->inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
			  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",user,args.args_string);
			}
			inse.data->num_inserts=0;
			inse.data->inv_insert="";
		    }
		    inse.data->inv_insert+=",("+web_ID_code+","+tparts[0]+","+id_parts[0]+","+*(ae.code)+",'"+uflag+"')";
		    ++(inse.data->num_inserts);
		  }
		}
		nbytes+=dve.data->byte_len;
		++n;
	    }
	  }
	}
    }
    for (const auto& key : insert_table.keys()) {
	InsertEntry inse;
	insert_table.found(key,inse);
	std::string result;
	if (!inse.data->inv_insert.empty()  && server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_"+key+" values "+inse.data->inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
	  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",user,args.args_string);
	}
	server._delete("IObML.ds"+local_args.dsnum2+"_inventory_"+key,"webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
	server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_inventory_"+key,result);
    }
  }
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_dataTypes values "+data_type_ss.str().substr(1)+" on duplicate key update value_type=values(value_type),byte_offset=values(byte_offset),byte_length=values(byte_length),missing_ind=values(missing_ind),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert dataType information for '"+*(ae.code)+"'","iinv",user,args.args_string);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_dataTypes",result);
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_summary values ("+web_ID_code+","+strutils::itos(nbytes)+","+strutils::itos(ndbytes)+",'"+uflag+"') on duplicate key update byte_length=values(byte_length),dataType_length=values(dataType_length),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert file size data for '"+web_ID_code+"'","iinv",user,args.args_string);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

void insert_generic_point_inventory(std::ifstream& ifs,MySQL::Server& server,std::string web_ID_code)
{
  auto uflag=strutils::strand(3);
  int d_min_lat_i=99,d_min_lon_i=99,d_max_lat_i=-1,d_max_lon_i=-1;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_dataTypesList_b")) {
    std::string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypesList_b like IObML.template_dataTypesList_b",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypesList_b'","iinv",user,args.args_string);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",user,args.args_string);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",user,args.args_string);
    }
  }
  std::unordered_map<std::string,std::string> times_table,obs_table,platform_table,id_table;
  std::unordered_map<size_t,std::tuple<size_t,size_t>> data_type_table;
  std::unordered_map<std::string,std::shared_ptr<std::unordered_map<size_t,size_t>>> datatypes_table;
  size_t byte_length=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (std::regex_search(sline,std::regex("<!>"))) {
	auto lparts=strutils::split(sline,"<!>");
	switch (lparts[0][0]) {
	  case 'T':
	  {
	    times_table.emplace(lparts[1],lparts[2]);
	    break;
	  }
	  case 'O':
	  {
	    MySQL::LocalQuery query("code","WObML.obsTypes","obsType = '"+lparts[2]+"'");
	    MySQL::Row row;
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+lparts[2]+"'","iinv",user,args.args_string);
	    }
	    obs_table.emplace(lparts[1],row[0]);
	    break;
	  }
	  case 'P':
	  {
	    MySQL::LocalQuery query("code","WObML.platformTypes","platformType = '"+lparts[2]+"'");
	    MySQL::Row row;
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get platformType code for '"+lparts[2]+"'","iinv",user,args.args_string);
	    }
	    platform_table.emplace(lparts[1],row[0]);
	    break;
	  }
	  case 'I':
	  {
	    process_IDs("irregular",server,lparts[1],lparts[2],id_table);
	    break;
	  }
	  case 'D':
	  {
	    if (lparts.size() > 2) {
		auto dparts=strutils::split(lparts[2],"[!]");
		MySQL::LocalQuery query("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+obs_table[dparts[0]]+" and platformType_code = "+platform_table[dparts[1]]+" and dataType = '"+dparts[2]+"'");
		MySQL::Row row;
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+lparts[2]+"'","iinv",user,args.args_string);
		}
		data_type_table.emplace(std::stoi(lparts[1]),std::make_tuple(std::stoi(row[0]),std::stoi(dparts[3].substr(1))));
		std::string value_type;
		switch (dparts[3][0]) {
		  case 'B':
		  {
		    value_type="byte";
		    break;
		  }
		  case 'F':
		  {
		    value_type="float";
		    break;
		  }
		  case 'I':
		  {
		    value_type="int";
		    break;
		  }
		}
		std::string scale;
		if (dparts.size() > 4) {
		  scale=dparts[4];
		}
		else {
		  scale="1";
		}
		if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypesList_b",row[0]+",'"+value_type+"',"+scale+","+dparts[3].substr(1)+",NULL") < 0) {
		  if (!std::regex_search(server.error(),std::regex("Duplicate entry"))) {
		    metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while inserting row '"+row[0]+",'"+value_type+"',"+scale+","+dparts[3].substr(1)+",NULL'","iinv",user,args.args_string);
		  }
		}
	    }
	    break;
	  }
	}
    }
    else if (std::regex_search(sline,std::regex("\\|"))) {
	auto iparts=strutils::split(sline,"|");
	auto idparts=strutils::split(id_table.at(iparts[2]),"|");
	int min_lat_i=std::stoi(idparts[1]);
	if (min_lat_i < d_min_lat_i) {
	  d_min_lat_i=min_lat_i;
	}
	int min_lon_i=std::stoi(idparts[2]);
	if (min_lon_i < d_min_lon_i) {
	  d_min_lon_i=min_lon_i;
	}
	int max_lat_i=std::stoi(idparts[3]);
	if (max_lat_i > d_max_lat_i) {
	  d_max_lat_i=max_lat_i;
	}
	int max_lon_i=std::stoi(idparts[4]);
	if (max_lon_i > d_max_lon_i) {
	  d_max_lon_i=max_lon_i;
	}
	std::shared_ptr<std::unordered_map<size_t,size_t>> p;
	auto e=datatypes_table.find(idparts[0]);
	if (e == datatypes_table.end()) {
	  p.reset(new std::unordered_map<size_t,size_t>);
	  datatypes_table.emplace(idparts[0],p);
	}
	else {
	  p=e->second;
	}
	std::vector<size_t> ivals,dvals;
	bitmap::uncompress_values(iparts[3],ivals);
	size_t min_dval=0xffffffff,max_dval=0;
	for (auto val : ivals) {
	  auto e=data_type_table.at(val);
	  val=std::get<0>(e);
	  dvals.emplace_back(val);
	  if (val < min_dval) {
	    min_dval=val;
	  }
	  if (val > max_dval) {
	    max_dval=val;
	  }
	  auto field_len=std::get<1>(e);
	  byte_length+=field_len;
	  if (p->find(val) == p->end()) {
	    p->emplace(val,field_len);
	  }
	}
	std::string bitmap;
	bitmap::compress_values(dvals,bitmap);
	for (int n=min_lat_i; n <= max_lat_i; ++n) {
	  for (int m=min_lon_i; m <= max_lon_i; ++m) {
	    if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_"+strutils::itos(n)+"_"+strutils::itos(m)+"_b",web_ID_code+","+times_table.at(iparts[1])+","+idparts[0]+",'"+bitmap+"',"+strutils::itos(min_dval)+","+strutils::itos(max_dval)+",'"+iparts[0]+"','"+uflag+"'","update dataType_codes = '"+bitmap+"', byte_offsets = '"+iparts[0]+"', uflag = '"+uflag+"'") < 0) {
		metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+times_table.at(iparts[1])+","+idparts[0]+",'"+bitmap+"',"+strutils::itos(min_dval)+","+strutils::itos(max_dval)+",'"+iparts[0]+"'' into table IObML.ds"+local_args.dsnum2+"_inventory_"+strutils::itos(n)+"_"+strutils::itos(m)+"_b","iinv",user,args.args_string);
	    }
	  }
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (int n=d_min_lat_i; n <= d_max_lat_i; ++n) {
    for (int m=d_min_lon_i; m <= d_max_lon_i; ++m) {
	server._delete("IObML.ds"+local_args.dsnum2+"_inventory_"+strutils::itos(n)+"_"+strutils::itos(m)+"_b","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    }
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+strutils::itos(byte_length)+",0,'"+uflag+"'","update byte_length = "+strutils::itos(byte_length)+", uflag = '"+uflag+"'") < 0) {
    metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+strutils::itos(byte_length)+"' into table IObML.ds"+local_args.dsnum2+"_inventory_summary","iinv",user,args.args_string);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  for (const auto& e : datatypes_table) {
    for (const auto& e2 : *e.second) {
	if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypes",web_ID_code+","+e.first+","+strutils::itos(e2.first)+",'',0,"+strutils::itos(e2.second)+",0,'"+uflag+"'","update value_type = '', byte_offset = 0, byte_length = "+strutils::itos(e2.second)+", missing_ind = 0, uflag = '"+uflag+"'") < 0) {
	  metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+e.first+","+strutils::itos(e2.first)+",'',0,"+strutils::itos(e2.second)+",0'","iinv",user,args.args_string);
	}
    }
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

void insert_obml_inventory()
{
  std::ifstream ifs;
  MySQL::Server server;
  std::string sdum,sdum2,format_code,error;
  std::vector<std::string> ID_codes;
  my::map<StringEntry> latlon_table,unique_lines(99999);
  StringEntry se;
  std::list<std::string> inventory_tables;
  std::string lockstring;
  my::map<InventoryEntry> inventory_table;
  InventoryEntry ie;
  struct stat buf;

  metautils::connect_to_metadata_server(server);
  sdum=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/inv/"+args.filename+".gz",temp_dir.name());
  if (stat(sdum.c_str(),&buf) == 0) {
    system(("gunzip "+sdum).c_str());
    strutils::chop(sdum,3);
  }
  else {
    sdum=remote_web_file("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/inv/"+args.filename,temp_dir.name());
  }
  ifs.open(sdum.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("insert_obml_inventory() was not able to open "+args.filename,"iinv",user,args.args_string);
  }
  sdum=strutils::substitute(args.filename,".ObML_inv","");
  strutils::replace_all(sdum,"%","/");
  MySQL::LocalQuery query;
  query.set("select code,format_code,tindex from WObML.ds"+local_args.dsnum2+"_webfiles as w left join dssdb.wfile as x on (x.dsid = 'ds"+args.dsnum+"' and x.type = 'D' and x.wfile = w.webID) where webID = '"+sdum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("insert_obml_inventory() returned error: "+query.error()+" while looking for code from webfiles","iinv",user,args.args_string);
  }
  MySQL::Row row;
  query.fetch_row(row);
  auto web_ID_code=row[0];
  format_code=row[1];
  tindex=row[2];
  char line[32768];
  ifs.getline(line,32768);
  if (std::regex_search(line,std::regex("^netCDF:timeSeries"))) {
    std::string sline=line;
    insert_obml_netcdf_time_series_inventory(ifs,server,web_ID_code,std::stoi(sline.substr(sline.find("|")+1)));
  }
  else if (std::regex_search(line,std::regex("^netCDF:point"))) {
    std::string sline=line;
    insert_obml_netcdf_point_inventory(ifs,server,web_ID_code,std::stoi(sline.substr(sline.find("|")+1)));
  }
  else {
    insert_generic_point_inventory(ifs,server,web_ID_code);
  }
/*
  while (!ifs.eof()) {
    std::string sline=line;
std::cerr << sline << std::endl;
    if (std::regex_search(sline,std::regex("<!>"))) {
	auto sp=strutils::split(sline,"<!>");
	switch (sp[0][0]) {
	  case 'O':
	  {
	    break;
	  }
	  case 'P':
	  {
	    break;
	  }
	  case 'I':
	  {
std::cerr << "A" << std::endl;
std::cerr << sp[2] << std::endl;
	    auto spx=strutils::split(sp[2],"[!]");
	    query.set("select i.code from WObML.ds"+local_args.dsnum2+"_IDs2 as i left join WObML.IDTypes as t on t.code = i.IDType_code where t.IDType= '"+spx[0]+"' and i.ID = '"+spx[1]+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+query.error()+" while trying to get ID code","iinv",user,args.args_string);
	    }
	    query.fetch_row(row);
	    ID_codes.emplace_back(row[0]);
	    break;
	  }
	  case 'D':
	  {
	    datatype_codes.emplace_back(sp[2]);
	    break;
	  }
	}
    }
    else if (std::regex_search(sline,std::regex("\\|"))) {
	if (first) {
	  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
	    std::string result;
	    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",user,args.args_string);
	    }
	  }
	  if (server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code) < 0) {
	    metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while deleting web_ID_code "+web_ID_code+" from IObML.ds"+local_args.dsnum2+"_inventory_summary","iinv",user,args.args_string);
	  }
	  inventory_tables=MySQL::table_names(server,"IObML","ds"+local_args.dsnum2+"_inventory_%",error);
	  lockstring="IObML.ds"+local_args.dsnum2+"_inventory_summary write";
	  for (auto& table : inventory_tables) {
	    if (!regex_search(table,std::regex("_summary$"))) {
		if (server._delete("IObML."+table,"webID_code = "+web_ID_code) < 0) {
		  metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to delete web_ID_code "+web_ID_code+" from 'IObML."+table+"'","iinv",user,args.args_string);
		}
		lockstring+=", IObML."+table+" write";
	    }
	  }
	  lockstring+=", IObML.template_inventory_lati_loni write";
	  std::string result;
	  if (server.command("lock tables "+lockstring,result) < 0) {
	    metautils::log_error("unable to lock tables "+lockstring+"; error: "+server.error(),"iinv",user,args.args_string);
	  }
	  first=false;
	}
	auto sp=strutils::split(sline,"|");
	total_bytes+=std::stoll(sp[1]);
	if (convert_lat_lon_to_box(36,std::stof(sp[6]),std::stof(sp[7]),lat_i,lon_i) < 0)
	  metautils::log_error("insert_obml_inventory() was not able to convert lat/lon: "+sp[6]+","+sp[7]+" to a 36-degree box","iinv",user,args.args_string);
	sdum="IObML.ds"+local_args.dsnum2+"_inventory_"+strutils::itos(lat_i)+"_"+strutils::itos(lon_i);
	if (!latlon_table.found(sdum,se)) {
	  if (!MySQL::table_exists(server,sdum)) {
	    std::string result;
	    server.command("unlock tables",result);
	    if (server.command("create table "+sdum+" like IObML.template_inventory_lati_loni",result) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to create '"+sdum+"'","iinv",user,args.args_string);
	    }
	    lockstring+=", "+sdum+" write";
	    if (server.command("lock tables "+lockstring,result) < 0) {
		metautils::log_error("unable to lock tables "+lockstring+"; error: "+server.error(),"iinv",user,args.args_string);
	    }
	  }
	  se.key=sdum;
	  latlon_table.insert(se);
	}
	n=std::stoi(sp[5]);
	m=std::stoi(sp[8]);
	sdum2=web_ID_code+","+sp[0]+","+sp[1]+","+sp[2]+","+ID_codes[n]+","+strutils::itos(lroundf(std::stof(sp[6])*10000.))+","+strutils::itos(lroundf(std::stof(sp[7])*10000.))+",'"+datatype_codes[m]+"'";
	se.key=web_ID_code+","+sp[2]+","+ID_codes[n]+","+datatype_codes[m];
	if (!unique_lines.found(se.key,se)) {
	  unique_lines.insert(se);
	  if (!inventory_table.found(sdum,ie)) {
	    ie.key=sdum;
	    ie.list.reset(new std::list<std::string>);
	    inventory_table.insert(ie);
	  }
	  ie.list->emplace_back(sdum2);
	}
	else {
	  ++num_dupes;
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (auto& key : inventory_table.keys()) {
    inventory_table.found(key,ie);
    if (server.insert(ie.key,*(ie.list)) < 0) {
	metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting multiple rows into "+ie.key,"iinv",user,args.args_string);
    }
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+strutils::lltos(total_bytes)) < 0) {
    metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting row '"+web_ID_code+","+strutils::lltos(total_bytes)+"'","iinv",user,args.args_string);
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+strutils::lltos(total_bytes)) < 0) {
    error=server.error();
    if (!std::regex_search(error,std::regex("Duplicate entry"))) {
	metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting row '"+web_ID_code+","+strutils::lltos(total_bytes)+"'","iinv",user,args.args_string);
    }
  }
  std::string result;
  server.command("unlock tables",result);
  if (num_dupes > 0) {
    metautils::log_warning(strutils::itos(num_dupes)+" duplicate observations were ignored","iinv_dupes",user,args.args_string);
  }
*/
  server.disconnect();
}

void insert_inventory()
{
  int idx;
  std::string ext;

  idx=args.filename.rfind(".");
  ext=args.filename.substr(idx+1);
  if (ext == "GrML_inv") {
    insert_grml_inventory();
  }
  else if (ext == "ObML_inv") {
    insert_obml_inventory();
  }
  else {
    metautils::log_error("insert_inventory() does not recognize inventory extension "+ext,"iinv",user,args.args_string);
  }
}

int main(int argc,char **argv)
{
  if (argc < 4) {
    std::cerr << "usage: iinv -d [ds]nnn.n [options...] -f file" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d nnn.n   specifies the dataset number" << std::endl;
    std::cerr << "-f file    summarize information for inventory file <file>" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-c/-C      create (default)/don't create file list cache" << std::endl;
    std::cerr << "-N         notify with a message when " << argv[0] << " completes" << std::endl;
    std::cerr << "-V         verbose mode" << std::endl;
    exit(1);
  }
  auto t1=time(nullptr);
  signal(SIGSEGV,segv_handler);
  metautils::read_config("iinv",user,args.args_string);
  parse_args(argc,argv);
/*
  if (args.dsnum == "999.9") {
    exit(0);
  }
*/
/*
metautils::log_warning("this command will be run by Bob later - you can ignore this warning","iinv",user,args.args_string);
exit(0);
*/
  metautils::cmd_register("iinv",user);
  temp_dir.create(directives.temp_path);
  insert_inventory();
  if (local_args.create_cache) {
    summarizeMetadata::create_file_list_cache("inv","iinv",user,args.args_string);
    if (!tindex.empty() && tindex != "0") {
	summarizeMetadata::create_file_list_cache("inv","iinv",user,args.args_string,tindex);
    }
  }
  if (local_args.notify) {
    std::cout << argv[0] << " has completed successfully" << std::endl;
  }
  auto t2=time(nullptr);
  metautils::log_warning("execution time: "+strutils::ftos(t2-t1)+" seconds","iinv.time",user,args.args_string);
}
