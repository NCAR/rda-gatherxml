#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <vector>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <gridutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <metahelpers.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::list;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::vector;
using strutils::chop;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::strand;
using strutils::substitute;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER=getenv("USER");
string myerror="";
string mywarning="";

struct LocalArgs {
  LocalArgs() : dsnum2(),temp_directory(),create_cache(true),notify(false),verbose(false),wms_only(false) {}

  string dsnum2;
  string temp_directory;
  bool create_cache,notify,verbose;
  bool wms_only;
} local_args;
struct StringEntry {
  StringEntry() : key() {}

  string key;
};
struct InventoryEntry {
  InventoryEntry() : key(),m_list(nullptr) {}

  string key;
  shared_ptr<list<string>> m_list;
};
struct AncillaryEntry {
  AncillaryEntry() : key(),code(nullptr) {}

  string key;
  shared_ptr<string> code;
};
string server_root=strutils::token(unixutils::host_name(),".",0);
string tindex;
TempDir temp_dir;

void parse_args(int argc,char **argv)
{
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,'`');
  auto sp=split(metautils::args.args_string,"`");
  for (size_t n=0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
	metautils::args.dsnum=sp[++n];
	if (regex_search(metautils::args.dsnum,regex("^ds"))) {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
	local_args.dsnum2=substitute(metautils::args.dsnum,".","");
    }
    else if (sp[n] == "-f") {
	metautils::args.filename=sp[++n];
    }
    else if (sp[n] == "-C") {
	local_args.create_cache=false;
    }
    else if (sp[n] == "-N") {
	local_args.notify=true;
    }
    else if (sp[n] == "-t") {
	local_args.temp_directory=sp[++n];
    }
    else if (sp[n] == "-V") {
	local_args.verbose=true;
    }
    else if (sp[n] == "--wms-only") {
	local_args.wms_only=true;
	local_args.create_cache=false;
    }
  }
}

extern "C" void segv_handler(int)
{
  metautils::log_error("Error: core dump","iinv",USER);
}

struct TimeRangeEntry {
  TimeRangeEntry() : code(),hour_diff(0x7fffffff) {}

  string code;
  int hour_diff;
};

string grid_definition_parameters(const XMLElement& e)
{
  string def_params;
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

void build_wms_capabilities()
{
  string wms_resource="https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/wfmd/"+substitute(metautils::args.filename,"_inv","");
  auto file=unixutils::remote_web_file(wms_resource+".gz",temp_dir.name());
  struct stat buf;
  if (stat(file.c_str(),&buf) == 0) {
    system(("gunzip "+file).c_str());
    chop(file,3);
  }
  XMLDocument xdoc;
  if (!xdoc.open(file)) {
    file=unixutils::remote_web_file(wms_resource,temp_dir.name());
    if (!xdoc.open(file)) {
        metautils::log_error("unable to open "+wms_resource,"iinv",USER);
    }
  }
  auto *tdir=new TempDir;
  if (!tdir->create(metautils::directives.temp_path)) {
    metautils::log_error("build_wms_capabilities() could not create a temporary directory","iinv",USER);
  }
  stringstream oss,ess;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir->name()+"/metadata/wms",oss,ess) < 0) {
    metautils::log_error("build_wms_capabilities() could not create the directory tree","iinv",USER);
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("build_wms_capabilities() could not connect to the metadata database","iinv",USER);
  }
  xmlutils::LevelMapper lmapper("/glade/u/home/rdadata/share/metadata/LevelTables");
  xmlutils::ParameterMapper pmapper("/glade/u/home/rdadata/share/metadata/ParameterTables");
  auto filename=xdoc.element("GrML").attribute_value("uri")+".GrML";
  if (!regex_search(filename,regex("^http(s){0,1}://rda.ucar.edu/"))) {
    metautils::log_warning("build_wms_capabilities() found an invalid uri: "+filename,"iinv",USER);
    return;
  }
  filename=filename.substr(filename.find("rda.ucar.edu")+12);
  auto web_home=metautils::web_home();
  replace_all(web_home,metautils::directives.data_root,metautils::directives.data_root_alias);
  replace_all(filename,web_home+"/","");
  replace_all(filename,"/","%");
  std::ofstream ofs((tdir->name()+"/metadata/wms/"+filename).c_str());
  if (!ofs.is_open()) {
    metautils::log_error("build_wms_capabilities() could not open the output file","iinv",USER);
  }
  ofs.setf(std::ios::fixed);
  ofs.precision(4);
  auto data_format=xdoc.element("GrML").attribute_value("format");
  MySQL::LocalQuery query("code","WGrML.formats","format = '"+data_format+"'");
  string data_format_code;
  MySQL::Row row;
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    data_format_code=row[0];
  }
  else {
    metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
  }
  string error;
  auto tables=table_names(server,"IGrML","%ds%"+local_args.dsnum2+"_inventory_"+data_format_code+"!%",error);
  if (tables.size() == 0) {
    return;
  }
  auto data_file=filename.substr(0,filename.rfind("."));
  replace_all(data_file,"%","/");
  auto grids=xdoc.element_list("GrML/grid");
  auto gcount=0;
  string last_grid_definition_code="-1";
  for (const auto& grid : grids) {
    auto def_params=grid_definition_parameters(grid);
    query.set("code","WGrML.gridDefinitions","definition = '"+grid.attribute_value("definition")+"' and defParams = '"+def_params+"'");
    string grid_definition_code;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	grid_definition_code=row[0];
    }
    else {
	metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
    }
    if (grid_definition_code != last_grid_definition_code) {
	double west_lon,south_lat,east_lon,north_lat;
	if (!gridutils::fill_spatial_domain_from_grid_definition(grid.attribute_value("definition")+"<!>"+def_params,"primeMeridian",west_lon,south_lat,east_lon,north_lat)) {
	  metautils::log_info("build_wms_capabilities() could not get the spatial domain from '"+filename+"'","iinv",USER);
	  return;
	}
	if (last_grid_definition_code != "-1") {
	  ofs << "    </Layer>" << endl;
	}
	ofs << "    <Layer>" << endl;
	ofs << "      <Title>" << grid.attribute_value("definition") << "_" << grid.attribute_value("numX") << "x" << grid.attribute_value("numY") << "</Title>" << endl;
	ofs << "#REPEAT __CRS__" << gcount << "__" << endl;
	ofs << "      <CRS>__CRS__" << gcount << "__.CRS</CRS>" << endl;
	ofs << "#ENDREPEAT __CRS__" << gcount << "__" << endl;
	ofs << "      <EX_GeographicBoundingBox>" << endl;
	ofs << "        <westBoundLongitude>" << west_lon << "</westBoundLongitude>" << endl;
	ofs << "        <eastBoundLongitude>" << east_lon << "</eastBoundLongitude>" << endl;
	ofs << "        <southBoundLatitude>" << south_lat << "</southBoundLatitude>" << endl;
	ofs << "        <northBoundLatitude>" << north_lat << "</northBoundLatitude>" << endl;
	ofs << "      </EX_GeographicBoundingBox>" << endl;;
	ofs << "#REPEAT __CRS__" << gcount << "__" << endl;
	ofs << "      <BoundingBox CRS=\"__CRS__" << gcount << "__.CRS\" minx=\"__CRS__" << gcount << "__." << west_lon << "\" miny=\"__CRS__" << gcount << "__." << south_lat << "\" maxx=\"__CRS__" << gcount << "__." << east_lon << "\" maxy=\"__CRS__" << gcount << "__." << north_lat << "\" />" << endl;
	ofs << "#ENDREPEAT __CRS__" << gcount << "__" << endl;
	last_grid_definition_code=grid_definition_code;
    }
    query.set("code","WGrML.timeRanges","timeRange = '"+grid.attribute_value("timeRange")+"'");
    string time_range_code;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	time_range_code=row[0];
    }
    else {
	metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
    }
    ofs << "      <Layer>" << endl;
    ofs << "        <Title>" << grid.attribute_value("timeRange") << "</Title>" << endl;
    auto vlevels=grid.element_list("level");
    for (const auto& vlevel : vlevels) {
	auto lmap=vlevel.attribute_value("map");
	auto ltype=vlevel.attribute_value("type");
	auto lvalue=vlevel.attribute_value("value");
	query.set("code","WGrML.levels","map = '"+lmap+"' and type = '"+ltype+"' and value = '"+lvalue+"'");
	string level_code;
	if (query.submit(server) == 0 && query.fetch_row(row)) {
	    level_code=row[0];
	}
	else {
	    metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
	}
	auto level_title=lmapper.description(data_format,ltype,lmap);
	if (level_title.empty()) {
	    level_title=vlevel.attribute_value("type")+":"+vlevel.attribute_value("value");
	}
	else if (vlevel.attribute_value("value") != "0") {
	    level_title+=": "+vlevel.attribute_value("value")+lmapper.units(data_format,ltype,lmap);
	}
	ofs << "        <Layer>" << endl;
	ofs << "          <Title>" << level_title << "</Title>" << endl;
	auto params=vlevel.element_list("parameter");
	for (const auto& param : params) {
	    auto pcode=param.attribute_value("map")+":"+param.attribute_value("value");
	    ofs << "          <Layer>" << endl;
	    ofs << "            <Title>" << pmapper.description(data_format,pcode) << "</Title>" << endl;
	    query.set("select distinct valid_date from IGrML.`ds"+local_args.dsnum2+"_inventory_"+data_format_code+"!"+pcode+"` as i left join WGrML.ds"+local_args.dsnum2+"_webfiles2 as w on w.code = i.webID_code where timeRange_code = "+time_range_code+" and gridDefinition_code = "+grid_definition_code+" and level_code = "+level_code+" and webID = '"+data_file+"' order by valid_date");
	    if (query.submit(server) == 0) {
		while (query.fetch_row(row)) {
		  ofs << "            <Layer queryable=\"0\">" << endl;
		  auto wms_layer_name=grid_definition_code+";"+time_range_code+";"+level_code+";"+data_format_code+"!"+pcode+";"+row[0];
		  ofs << "              <Name>" << wms_layer_name << "</Name>" << endl;
		  ofs << "              <Title>" << row[0].substr(0,4) << "-" << row[0].substr(4,2) << "-" << row[0].substr(6,2) << "T" << row[0].substr(8,2) << ":" << row[0].substr(10,2) << "Z</Title>" << endl;
		  ofs << "              <Style>" << endl;
		  ofs << "                <Name>Legend</Name>" << endl;
		  ofs << "                <Title>Legend Graphic</Title>" << endl;
		  ofs << "                <LegendURL>" << endl;
		  ofs << "                  <Format>image/png</Format>" << endl;
		  ofs << "                  <OnlineResource xlink:type=\"simple\" xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << wms_layer_name << "\" />" << endl;
		  ofs << "                </LegendURL>" << endl;
		  ofs << "              </Style>" << endl;
		  ofs << "            </Layer>" << endl;
		}
	    }
	    else {
		metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
	    }
	    ofs << "          </Layer>" << endl;
	}
	ofs << "        </Layer>" << endl;
    }
    auto vlayers=grid.element_list("layer");
    for (const auto& vlayer : vlayers) {
	auto lmap=vlayer.attribute_value("map");
	auto ltype=vlayer.attribute_value("type");
	auto bottom=vlayer.attribute_value("bottom");
	auto top=vlayer.attribute_value("top");
	query.set("code","WGrML.levels","map = '"+lmap+"' and type = '"+ltype+"' and value = '"+bottom+","+top+"'");
	string layer_code;
	if (query.submit(server) == 0 && query.fetch_row(row)) {
	    layer_code=row[0];
	}
	else {
	    metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
	}
	auto layer_title=lmapper.description(data_format,ltype,lmap);
	if (layer_title.empty()) {
	    layer_title=vlayer.attribute_value("type")+":"+vlayer.attribute_value("value");
	}
	else if (vlayer.attribute_value("value") != "0") {
	    auto tparts=split(ltype,"-");
	    if (tparts.size() == 1) {
		tparts.emplace_back(tparts.front());
	    }
	    layer_title+=": "+vlayer.attribute_value("bottom")+lmapper.units(data_format,tparts[0],lmap)+", "+vlayer.attribute_value("top")+lmapper.units(data_format,tparts[1],lmap);;
	}
	ofs << "        <Layer>" << endl;
	ofs << "          <Title>" << layer_title << "</Title>" << endl;
	auto params=vlayer.element_list("parameter");
	for (const auto& param : params) {
	    auto pcode=param.attribute_value("map")+":"+param.attribute_value("value");
	    ofs << "          <Layer>" << endl;
	    ofs << "            <Title>" << pmapper.description(data_format,pcode) << "</Title>" << endl;
	    query.set("select distinct valid_date from IGrML.`ds"+local_args.dsnum2+"_inventory_"+data_format_code+"!"+pcode+"` as i left join WGrML.ds"+local_args.dsnum2+"_webfiles2 as w on w.code = i.webID_code where timeRange_code = "+time_range_code+" and gridDefinition_code = "+grid_definition_code+" and level_code = "+layer_code+" and webID = '"+data_file+"' order by valid_date");
	    if (query.submit(server) == 0) {
		while (query.fetch_row(row)) {
		  ofs << "            <Layer queryable=\"0\">" << endl;
		  auto wms_layer_name=grid_definition_code+";"+time_range_code+";"+layer_code+";"+data_format_code+"!"+pcode+";"+row[0];
		  ofs << "              <Name>" << wms_layer_name << "</Name>" << endl;
		  ofs << "              <Title>" << row[0].substr(0,4) << "-" << row[0].substr(4,2) << "-" << row[0].substr(6,2) << "T" << row[0].substr(8,2) << ":" << row[0].substr(10,2) << "Z</Title>" << endl;
		  ofs << "              <Style>" << endl;
		  ofs << "                <Name>Legend</Name>" << endl;
		  ofs << "                <Title>Legend Graphic</Title>" << endl;
		  ofs << "                <LegendURL>" << endl;
		  ofs << "                  <Format>image/png</Format>" << endl;
		  ofs << "                  <OnlineResource xlink:type=\"simple\" xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << wms_layer_name << "\" />" << endl;
		  ofs << "                </LegendURL>" << endl;
		  ofs << "              </Style>" << endl;
		  ofs << "            </Layer>" << endl;
		}
	    }
	    else {
		metautils::log_error("build_wms_capabilities(): query '"+query.show()+"' failed","iinv",USER);
	    }
	    ofs << "          </Layer>" << endl;
	}
	ofs << "        </Layer>" << endl;
    }
    ofs << "      </Layer>" << endl;
    ++gcount;
  }
  ofs << "    </Layer>" << endl;
  ofs.close();
  unixutils::mysystem2("/bin/sh -c 'gzip "+tdir->name()+"/metadata/wms/"+filename+"'",oss,ess);
  if (unixutils::rdadata_sync(tdir->name(),"metadata/wms/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
    metautils::log_error("build_wms_capabilities() could not sync the capabilities file for '"+filename+"'","iinv",USER);
  }
  delete tdir;
}

void insert_grml_inventory()
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  std::ifstream ifs(local_args.temp_directory+"/"+metautils::args.filename.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("insert_grml_inventory() was not able to open "+metautils::args.filename,"iinv",USER);
  }
  auto web_id=substitute(metautils::args.filename,".GrML_inv","");
  replace_all(web_id,"%","/");
  MySQL::LocalQuery query("select code,format_code,tindex from WGrML.ds"+local_args.dsnum2+"_webfiles2 as w left join dssdb.wfile as x on (x.dsid = 'ds"+metautils::args.dsnum+"' and x.type = 'D' and x.wfile = w.webID) where webID = '"+web_id+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while looking for code from webfiles","iinv",USER);
  }
  if (query.num_rows() == 0) {
    metautils::log_error("insert_grml_inventory() did not find "+web_id+" in WGrML.ds"+local_args.dsnum2+"_webfiles2","iinv",USER);
  }
  MySQL::Row row;
  query.fetch_row(row);
  auto web_id_code=row[0];
  auto format_code=row[1];
  tindex=row[2];
  if (!MySQL::table_exists(server,"IGrML.ds"+local_args.dsnum2+"_inventory_summary")) {
    string result;
    if (server.command("create table IGrML.ds"+local_args.dsnum2+"_inventory_summary like IGrML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create inventory_summary table","iinv",USER);
    }
  }
  struct InitTimeEntry {
    InitTimeEntry() : key(),time_range_codes(nullptr) {}

    string key;
    shared_ptr<my::map<StringEntry>> time_range_codes;
  };
  my::map<InitTimeEntry> init_dates_table;
  string dupe_vdates="N";
  string uflag=strand(3);
  int nlines=0,num_dupes=0;
  long long total_bytes=0;
  vector<TimeRangeEntry> time_range_codes;
  vector<string> grid_definition_codes,level_codes,parameters,processes,ensembles,parameter_codes;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    nlines++;
    string sline=line;
    if (regex_search(sline,regex("<!>"))) {
	auto line_parts=split(sline,"<!>");
	switch (line_parts[0][0]) {
	  case 'U':
	  {
	    TimeRangeEntry tre;
	    if (line_parts[2] == "Analysis" || regex_search(line_parts[2],regex("^0-hour")) || line_parts[2] == "Monthly Mean") {
		tre.hour_diff=0;
	    }
	    else if (regex_search(line_parts[2],regex("-hour Forecast$"))) {
		tre.hour_diff=stoi(line_parts[2].substr(0,line_parts[2].find("-")));
	    }
	    else if (regex_search(line_parts[2],regex("to initial\\+"))) {
		auto hr=line_parts[2].substr(line_parts[2].find("to initial+")+11);
		chop(hr);
		tre.hour_diff=stoi(hr);
	    }
	    else {
		metautils::log_warning("insert_grml_inventory() does not recognize product '"+line_parts[2]+"'","iinv",USER);
	    }
	    query.set("code","WGrML.timeRanges","timeRange = '"+line_parts[2]+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get timeRange code","iinv",USER);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no timeRange code for '"+line_parts[2]+"'","iinv",USER);
	    }
	    query.fetch_row(row);
	    tre.code=row[0];
	    time_range_codes.emplace_back(tre);
	    break;
	  }
	  case 'G':
	  {
	    auto gdef_params=split(line_parts[2],",");
	    string definition,definition_parameters;
	    switch (stoi(gdef_params[0])) {
		case static_cast<int>(Grid::Type::latitudeLongitude):
		case static_cast<int>(Grid::Type::gaussianLatitudeLongitude): {
		  if (stoi(gdef_params[0]) == static_cast<int>(Grid::Type::latitudeLongitude)) {
		    definition="latLon";
		  }
		  else if (stoi(gdef_params[0]) == static_cast<int>(Grid::Type::gaussianLatitudeLongitude)) {
		    definition="gaussLatLon";
		  }
		  if (gdef_params[0].back() == 'C') {
		    definition+="Cell";
		  }
		  definition_parameters=gdef_params[1]+":"+gdef_params[2]+":";
		  if (gdef_params[3][0] == '-') {
		    definition_parameters+=gdef_params[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[3]+"N:";
		  }
		  if (gdef_params[4][0] == '-') {
		    definition_parameters+=gdef_params[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[4]+"E:";
		  }
		  if (gdef_params[5][0] == '-') {
		    definition_parameters+=gdef_params[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[5]+"N:";
		  }
		  if (gdef_params[6][0] == '-') {
		    definition_parameters+=gdef_params[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[6]+"E:";
		  }
		  definition_parameters+=gdef_params[7]+":"+gdef_params[8];
		  break;
		}
		case static_cast<int>(Grid::Type::polarStereographic): {
		  definition="polarStereographic";
		  definition_parameters=gdef_params[1]+":"+gdef_params[2]+":";
		  if (gdef_params[3][0] == '-') {
		    definition_parameters+=gdef_params[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[3]+"N:";
		  }
		  if (gdef_params[4][0] == '-') {
		    definition_parameters+=gdef_params[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[4]+"E:";
		  }
		  if (gdef_params[5][0] == '-') {
		    definition_parameters+=gdef_params[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[5]+"N:";
		  }
		  if (gdef_params[6][0] == '-') {
		    definition_parameters+=gdef_params[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[6]+"E:";
		  }
		  definition_parameters+=gdef_params[9]+":"+gdef_params[7]+":"+gdef_params[8];
		  break;
		}
		case static_cast<int>(Grid::Type::mercator): {
		  definition="mercator";
		  definition_parameters=gdef_params[1]+":"+gdef_params[2]+":";
		  if (gdef_params[3][0] == '-') {
		    definition_parameters+=gdef_params[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[3]+"N:";
		  }
		  if (gdef_params[4][0] == '-') {
		    definition_parameters+=gdef_params[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[4]+"E:";
		  }
		  if (gdef_params[5][0] == '-') {
		    definition_parameters+=gdef_params[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[5]+"N:";
		  }
		  if (gdef_params[6][0] == '-') {
		    definition_parameters+=gdef_params[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[6]+"E:";
		  }
		  definition_parameters+=gdef_params[7]+":"+gdef_params[8]+":";
		  if (gdef_params[9][0] == '-') {
		    definition_parameters+=gdef_params[9].substr(1)+"S";
		  }
		  else {
		    definition_parameters+=gdef_params[9]+"N";
		  }
		  break;
		}
		case static_cast<int>(Grid::Type::lambertConformal): {
		  definition="lambertConformal";
		  definition_parameters=gdef_params[1]+":"+gdef_params[2]+":";
		  if (gdef_params[3][0] == '-') {
		    definition_parameters+=gdef_params[3].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[3]+"N:";
		  }
		  if (gdef_params[4][0] == '-') {
		    definition_parameters+=gdef_params[4].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[4]+"E:";
		  }
		  if (gdef_params[5][0] == '-') {
		    definition_parameters+=gdef_params[5].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[5]+"N:";
		  }
		  if (gdef_params[6][0] == '-') {
		    definition_parameters+=gdef_params[6].substr(1)+"W:";
		  }
		  else {
		    definition_parameters+=gdef_params[6]+"E:";
		  }
		  definition_parameters+=gdef_params[9]+":"+gdef_params[7]+":"+gdef_params[8]+":";
		  if (gdef_params[10][0] == '-') {
		    definition_parameters+=gdef_params[10].substr(1)+"S:";
		  }
		  else {
		    definition_parameters+=gdef_params[10]+"N:";
		  }
		  if (gdef_params[11][0] == '-') {
		    definition_parameters+=gdef_params[11].substr(1)+"S";
		  }
		  else {
		    definition_parameters+=gdef_params[11]+"N";
		  }
		  break;
		}
		case static_cast<int>(Grid::Type::sphericalHarmonics): {
		  definition="sphericalHarmonics";
		  definition_parameters=gdef_params[1]+":"+gdef_params[2]+":"+gdef_params[3];
		  break;
		}
		default: {
		  metautils::log_error("insert_grml_inventory() does not understand grid type "+gdef_params[0],"iinv",USER);
		}
	    }
	    query.set("code","WGrML.gridDefinitions","definition = '"+definition+"' and defParams = '"+definition_parameters+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get gridDefinition code","iinv",USER);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no gridDefinition code for '"+definition+","+definition_parameters+"'","iinv",USER);
	    }
	    query.fetch_row(row);
	    grid_definition_codes.emplace_back(row[0]);
	    break;
	  }
	  case 'L':
	  {
	    auto lev_parts=split(line_parts[2],":");
	    if (lev_parts.size() < 2 || lev_parts.size() > 3) {
		metautils::log_error("insert_grml_inventory() found bad level code: "+line_parts[2],"iinv",USER);
	    }
	    string map,type;
	    if (regex_search(lev_parts[0],regex(","))) {
		auto idx=lev_parts[0].find(",");
		map=lev_parts[0].substr(0,idx);
		type=lev_parts[0].substr(idx+1);
	    }
	    else {
		map="";
		type=lev_parts[0];
	    }
	    string value;
	    switch (lev_parts.size()) {
		case 2:
		{
		  value=lev_parts[1];
		  break;
		}
		case 3:
		{
		  value=lev_parts[2]+","+lev_parts[1];
		  break;
		}
	    }
	    query.set("code","WGrML.levels","map = '"+map+"' and type = '"+type+"' and value = '"+value+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_grml_inventory() returned error: "+query.error()+" while trying to get level code","iinv",USER);
	    }
	    if (query.num_rows() == 0) {
		metautils::log_error("no level code for '"+map+","+type+","+value+"'","iinv",USER);
	    }
	    query.fetch_row(row);
	    level_codes.emplace_back(row[0]);
	    break;
	  }
	  case 'P':
	  {
	    parameters.emplace_back(line_parts[2]);
	    auto pcode=format_code+"!"+line_parts[2];
	    if (!MySQL::table_exists(server,"IGrML.ds"+local_args.dsnum2+"_inventory_"+pcode)) {
		string result;
		if (line_parts.size() > 3 && line_parts[3] == "BIG") {
		  if (server.command("create table IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"` like IGrML.template_inventory_p_big",result) < 0) {
		    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create parameter inventory table","iinv",USER);
		  }
		}
		else {
		  if (server.command("create table IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"` like IGrML.template_inventory_p",result) < 0) {
		    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to create parameter inventory table","iinv",USER);
		  }
		}
	    }
	    parameter_codes.emplace_back(pcode);
	    break;
	  }
	  case 'R':
	  {
	    processes.emplace_back(line_parts[2]);
	    break;
	  }
	  case 'E':
	  {
	    ensembles.emplace_back(line_parts[2]);
	    break;
	  }
	}
    }
    else if (regex_search(sline,regex("\\|"))) {
/*
	if (first) {
	  sdum="lock tables ";
	  n=0;
	  for (auto& item : parameter_codes) {
	    if (n > 0) {
		sdum+=", ";
	    }
	    sdum+="IGrML.`ds"+local_args.dsnum2+"_inventory_"+item+"` write";
	    ++n;
	  }
	  if (server.issueCommand(sdum,error) < 0) {
	    metautils::log_error("unable to lock parameter tables; error: "+server.error(),"iinv",user);
	  }
	  first=false;
	}
*/
	auto inv_parts=split(sline,"|");
	total_bytes+=stoll(inv_parts[1]);
	auto tr_index=stoi(inv_parts[3]);
	string init_date;
	if (time_range_codes[tr_index].hour_diff != 0x7fffffff) {
	  init_date=DateTime(stoll(inv_parts[2])*100).hours_subtracted(time_range_codes[tr_index].hour_diff).to_string("%Y%m%d%H%MM");
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
	auto insert_string=web_id_code+","+inv_parts[0]+","+inv_parts[1]+","+inv_parts[2]+","+init_date+","+time_range_codes[tr_index].code+","+grid_definition_codes[stoi(inv_parts[4])]+","+level_codes[stoi(inv_parts[5])]+",";
	if (inv_parts.size() > 7 && !inv_parts[7].empty()) {
	  insert_string+="'"+processes[stoi(inv_parts[7])]+"',";
	}
	else {
	  insert_string+="'',";
	}
	if (inv_parts.size() > 8 && !inv_parts[8].empty()) {
	  insert_string+="'"+ensembles[stoi(inv_parts[8])]+"'";
	}
	else {
	  insert_string+="''";
	}
	insert_string+=",'"+uflag+"'";
	auto pcode=format_code+"!"+parameters[stoi(inv_parts[6])];
	if (server.insert("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","webID_code,byte_offset,byte_length,valid_date,init_date,timeRange_code,gridDefinition_code,level_code,process,ensemble,uflag",insert_string,"") < 0) {
	  if (!regex_search(server.error(),regex("Duplicate entry"))) {
	    metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while inserting row '"+insert_string+"'","iinv",USER);
	  }
	  else {
	    string dupe_where="webID_code = "+web_id_code+" and valid_date = "+inv_parts[2]+" and timeRange_code = "+time_range_codes[tr_index].code+" and gridDefinition_code = "+grid_definition_codes[stoi(inv_parts[4])]+" and level_code = "+level_codes[stoi(inv_parts[5])];
	    if (inv_parts.size() > 7 && !inv_parts[7].empty()) {
		dupe_where+=" and process = '"+processes[stoi(inv_parts[7])]+"'";
	    }
	    else {
		dupe_where+=" and process = ''";
	    }
	    if (inv_parts.size() > 8 && !inv_parts[8].empty()) {
		dupe_where+=" and ensemble = '"+ensembles[stoi(inv_parts[8])]+"'";
	    }
	    else {
		dupe_where+=" and ensemble = ''";
	    }
	    query.set("uflag","IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`",dupe_where);
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while trying to get flag for duplicate row: '"+dupe_where+"'","iinv",USER);
	    }
	    if (row[0] == uflag) {
		++num_dupes;
		if (local_args.verbose) {
		  cout << "**duplicate ignored - line " << nlines << endl;
		}
	    }
	    else {
		if (server.update("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","byte_offset = "+inv_parts[0]+", byte_length = "+inv_parts[1]+",init_date = "+init_date+",uflag = '"+uflag+"'",dupe_where) < 0) {
		  metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while updating duplicate row: '"+dupe_where+"'","iinv",USER);
		}
	    }
	  }
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (const auto& pcode : parameter_codes) {
    server._delete("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","webID_code = "+web_id_code+" and uflag != '"+uflag+"'");
  }
//  server.issueCommand("unlock tables",error);
  if (server.insert("IGrML.ds"+local_args.dsnum2+"_inventory_summary","webID_code,byte_length,dupe_vdates",web_id_code+","+lltos(total_bytes)+",'"+dupe_vdates+"'","update byte_length = "+lltos(total_bytes)+", dupe_vdates = '"+dupe_vdates+"'") < 0) {
    if (!regex_search(server.error(),regex("Duplicate entry"))) {
	metautils::log_error("insert_grml_inventory() returned error: "+server.error()+" while inserting row '"+web_id_code+","+lltos(total_bytes)+",'"+dupe_vdates+"''","iinv",USER);
    }
  }
  server.disconnect();
  if (num_dupes > 0) {
    metautils::log_warning(itos(num_dupes)+" duplicate grids were ignored","iinv_dupes",USER);
  }
}

void check_for_times_table(MySQL::Server& server,string type,string last_decade)
{
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0")) {
    string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0 like IObML.template_"+type+"_times_decade",result) < 0) {
	metautils::log_error("check_for_times_table() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_"+type+"_times_"+last_decade+"0'","iinv",USER);
    }
  }
}

void process_IDs(string type,MySQL::Server& server,string ID_index,string ID_data,unordered_map<string,string>& id_table)
{
  auto id_parts=split(ID_data,"[!]");
  string qspec="select i.code from WObML.ds"+local_args.dsnum2+"_IDs2 as i left join WObML.IDTypes as t on t.code = i.IDType_code where i.ID = '"+id_parts[1]+"' and t.IDType = '"+id_parts[0]+"' and i.sw_lat = "+metatranslations::string_coordinate_to_db(id_parts[2])+" and i.sw_lon = "+metatranslations::string_coordinate_to_db(id_parts[3]);
  if (id_parts.size() > 4) {
    qspec+=" and i.ne_lat = "+metatranslations::string_coordinate_to_db(id_parts[4])+" and ne_lon = "+metatranslations::string_coordinate_to_db(id_parts[5]);
  }
  MySQL::LocalQuery query(qspec);
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    string error="process_IDs() returned error: "+query.error()+" while trying to get ID code for '"+id_parts[0]+","+id_parts[1]+","+id_parts[2]+","+id_parts[3];
    if (id_parts.size() > 4) {
	error+=","+id_parts[4]+","+id_parts[5];
    }
    error+="'";
    metautils::log_error(error,"iinv",USER);
  }
  size_t min_lat_i,min_lon_i,max_lat_i,max_lon_i;
  geoutils::convert_lat_lon_to_box(36,stof(id_parts[2]),stof(id_parts[3]),min_lat_i,min_lon_i);
  if (id_parts.size() > 4) {
    geoutils::convert_lat_lon_to_box(36,stof(id_parts[4]),stof(id_parts[5]),max_lat_i,max_lon_i);
  }
  else {
    max_lat_i=min_lat_i;
    max_lon_i=min_lon_i;
  }
  for (size_t n=min_lat_i; n <= max_lat_i; ++n) {
    for (size_t m=min_lon_i; m <= max_lon_i; ++m) {
	auto s_lat_i=itos(n);
	auto s_lon_i=itos(m);
	string check_table="IObML.ds"+local_args.dsnum2+"_inventory_"+s_lat_i+"_"+s_lon_i;
	if (type == "irregular") {
	  check_table+="_b";
	}
	if (!MySQL::table_exists(server,check_table)) {
	  string command="create table "+check_table+" like IObML.template_inventory_lati_loni";
	  if (type == "irregular") {
	    command+="_b";
	  }
	  string result;
	  if (server.command(command,result) < 0) {
	    metautils::log_error("process_IDs() returned error: "+server.error()+" while trying to create '"+check_table+"'","iinv",USER);
	  }
	}
	auto s=row[0]+"|"+itos(min_lat_i)+"|"+itos(min_lon_i);
	if (id_parts.size() > 4) {
	  s+="|"+itos(max_lat_i)+"|"+itos(max_lon_i);
	}
	id_table.emplace(ID_index,s);
    }
  }
}

struct DataVariableEntry {
  struct Data {
    Data() : var_name(),value_type(),offset(0),byte_len(0),missing_table(99999) {}

    string var_name,value_type;
    size_t offset,byte_len;
    my::map<StringEntry> missing_table;
  };
  DataVariableEntry() : key(),data(nullptr) {}

  string key;
  shared_ptr<Data> data;
};
struct MissingEntry {
  MissingEntry() : key(),time_code(0) {}

  string key;
  size_t time_code;
};
void insert_obml_netcdf_time_series_inventory(std::ifstream& ifs,MySQL::Server& server,string web_ID_code,size_t rec_size)
{
  my::map<AncillaryEntry> obstype_table,platform_table,datatype_table;
  AncillaryEntry ae;
  my::map<DataVariableEntry> datavar_table;
  DataVariableEntry dve;
  MySQL::LocalQuery query;
  MySQL::Row row;
  list<string> times_list;
  my::map<StringEntry> missing_table(999999);
  StringEntry se;

if (rec_size > 0) {
metautils::log_error("insert_obml_netcdf_time_series_inventory() can't insert for observations with a record dimension","iinv",USER);
}
  string uflag=strand(3);
//cerr << "uflag='" << uflag << "'" << endl;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
    string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",USER);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",USER);
    }
  }
  else {
/*
    if (server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to delete from 'ds"+local_args.dsnum2+"_dataTypes' where webID_code = "+web_ID_code,"iinv",USER);
    }
*/
    if (server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to delete from 'ds"+local_args.dsnum2+"_inventory_summary' where webID_code = "+web_ID_code,"iinv",USER);
    }
  }
  stringstream times;
  unordered_map<string,string> id_table;
  string last_decade;
  auto inv_line=regex("<!>");
  auto missing_line=regex("^([0-9]){1,}\\|");
  auto ndbytes=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (regex_search(line,inv_line)) {
	auto sp=split(line,"<!>");
	switch (sp[0][0]) {
	  case 'D':
	  {
	    dve.key=sp[1];
	    dve.data.reset(new DataVariableEntry::Data);
	    auto sp2=split(sp[2],"|");
	    dve.data->var_name=sp2[0];
	    dve.data->offset=stoi(sp2[1]);
	    dve.data->value_type=sp2[2];
	    dve.data->byte_len=stoi(sp2[3]);
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
		metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+sp[2]+"'","iinv",USER);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new string);
	    *ae.code=row[0];
	    obstype_table.insert(ae);
	    break;
	  case 'P':
	    query.set("code","WObML.platformTypes","platformType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get platform code for '"+sp[2]+"'","iinv",USER);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new string);
	    *ae.code=row[0];
	    platform_table.insert(ae);
	    break;
	  case 'T':
	  {
	    auto this_decade=sp[2].substr(0,3);
	    if (this_decade != last_decade) {
		if (times.tellp() > 0) {
		  check_for_times_table(server,"timeSeries",last_decade);
		  string result;
		  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),uflag=values(uflag)",result) < 0) {
		    metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","iinv",USER);
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
    else if (regex_search(line,missing_line)) {
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
    string result;
    if (server.command("insert into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","iinv",USER);
    }
    server._delete("IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_timeSeries_times_"+last_decade+"0",result);
  }
  auto nbytes=0;
  string inv_insert;
  inv_insert.reserve(800000);
  for (const auto& id_entry : id_table) {
    auto num_inserts=0;
    auto id_parts=split(id_entry.second,"|");
    string inventory_file="IObML.ds"+local_args.dsnum2+"_inventory_"+id_parts[1]+"_"+id_parts[2];
    for (const auto& datavar : datavar_table.keys()) {
	datavar_table.found(datavar,dve);
	string missing_ind;
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
		query.set("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+*(ae_obs.code)+" and platformType_code = "+*(ae_plat.code)+" and dataType = 'ds"+metautils::args.dsnum+":"+dve.data->var_name+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+*(ae_obs.code)+","+*(ae_plat.code)+",'"+dve.data->var_name+"''","iinv",USER);
		}
		ae.code.reset(new string);
		*(ae.code)=row[0];
		datatype_table.insert(ae);
		if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypes",web_ID_code+","+id_parts[0]+","+*(ae.code)+",'"+dve.data->value_type+"',"+itos(dve.data->offset)+","+itos(dve.data->byte_len)+","+missing_ind+",'"+uflag+"'","update value_type='"+dve.data->value_type+"',byte_offset="+itos(dve.data->offset)+",byte_length="+itos(dve.data->byte_len)+",missing_ind="+missing_ind+",uflag='"+uflag+"'") < 0) {
		  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert dataType information for '"+*(ae.code)+"'","iinv",USER);
		}
	    }
	    string key_end="|"+obstype+"|"+platform+"|"+id_entry.first+"|"+datavar;
	    auto n=0;
	    for (const auto& time : times_list) {
		if (missing_ind != "0") {
		  auto found_missing=missing_table.found(itos(n)+key_end,se);
		  if ((missing_ind == "1" && !found_missing) || (missing_ind == "2" && found_missing)) {
		    if (num_inserts >= 10000) {
			string result;
			if (server.command("insert into "+inventory_file+" values "+inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
			  metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",USER);
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
    string result;
    if (!inv_insert.empty()  && server.command("insert into "+inventory_file+" values "+inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",USER);
    }
    server._delete(inventory_file,"webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table "+inventory_file,result);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  string result;
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_dataTypes",result);
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_summary values ("+web_ID_code+","+itos(nbytes)+","+itos(ndbytes)+",'"+uflag+"') on duplicate key update byte_length=values(byte_length),dataType_length=values(dataType_length),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_time_series_inventory() returned error: "+server.error()+" while trying to insert file size data for '"+web_ID_code+"'","iinv",USER);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

struct InsertEntry {
  struct Data {
    Data() : inv_insert(),num_inserts(0) {
	inv_insert.reserve(800000);
    }

    string inv_insert;
    int num_inserts;
  };
  InsertEntry() : key(),data(nullptr) {}

  string key;
  shared_ptr<Data> data;
};
void insert_obml_netcdf_point_inventory(std::ifstream& ifs,MySQL::Server& server,string web_ID_code,size_t rec_size)
{
  my::map<AncillaryEntry> obstype_table,platform_table,datatype_table;
  AncillaryEntry ae;
  my::map<DataVariableEntry> datavar_table;
  DataVariableEntry dve;
  MySQL::LocalQuery query;
  MySQL::Row row;
  list<string> times_list;
  my::map<StringEntry> missing_table(999999);
  StringEntry se;

if (rec_size > 0) {
metautils::log_error("insert_obml_netcdf_point_inventory() can't insert for observations with a record dimension","iinv",USER);
}
  string uflag=strand(3);
//cerr << "uflag='" << uflag << "'" << endl;
  string result;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",USER);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",USER);
    }
  }
  stringstream times;
  unordered_map<string,string> id_table;
  string last_decade;
  auto inv_line=regex("<!>");
  auto missing_line=regex("^([0-9]){1,}\\|");
  auto ndbytes=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    if (regex_search(line,inv_line)) {
	auto sp=split(line,"<!>");
	switch (sp[0][0]) {
	  case 'D':
	  {
	    dve.key=sp[1];
	    dve.data.reset(new DataVariableEntry::Data);
	    auto sp2=split(sp[2],"|");
	    dve.data->var_name=sp2[0];
	    dve.data->offset=stoi(sp2[1]);
	    dve.data->value_type=sp2[2];
	    dve.data->byte_len=stoi(sp2[3]);
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
		metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+sp[2]+"'","iinv",USER);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new string);
	    *ae.code=row[0];
	    obstype_table.insert(ae);
	    break;
	  }
	  case 'P':
	  {
	    query.set("code","WObML.platformTypes","platformType = '"+sp[2]+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get platform code for '"+sp[2]+"'","iinv",USER);
	    }
	    ae.key=sp[1];
	    ae.code.reset(new string);
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
		  string result;
		  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),lat=values(lat),lon=values(lon),uflag=values(uflag)",result) < 0) {
		    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","iinv",USER);
		  }
		  server._delete("IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
		  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0",result);
		}
		times.str("");
	    }
	    auto tparts=split(sp[2],"[!]");
	    times << ",(" << tparts[0] << "," << sp[1] << "," << metatranslations::string_coordinate_to_db(tparts[1]) << "," << metatranslations::string_coordinate_to_db(tparts[2]) << "," << web_ID_code << ",'" << uflag << "')";
	    times_list.emplace_back(sp[2]);
	    last_decade=this_decade;
	    break;
	  }
	}
    }
    else if (regex_search(line,missing_line)) {
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
    string result;
    if (server.command("insert into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0 values "+times.str().substr(1)+" on duplicate key update time_index=values(time_index),lat=values(lat),lon=values(lon),uflag=values(uflag)",result) < 0) {
	metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert list of times into IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","iinv",USER);
    }
    server._delete("IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_point_times_"+last_decade+"0",result);
  }
  stringstream data_type_ss;
  auto nbytes=0;
  for (const auto& id_entry : id_table) {
    my::map<InsertEntry> insert_table;
    auto id_parts=split(id_entry.second,"|");
    for (const auto& datavar : datavar_table.keys()) {
	datavar_table.found(datavar,dve);
	string missing_ind;
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
		query.set("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+*(ae_obs.code)+" and platformType_code = "+*(ae_plat.code)+" and dataType = 'ds"+metautils::args.dsnum+":"+dve.data->var_name+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+*(ae_obs.code)+","+*(ae_plat.code)+",'"+dve.data->var_name+"''","iinv",USER);
		}
		ae.code.reset(new string);
		*(ae.code)=row[0];
		datatype_table.insert(ae);
		data_type_ss << ",(" << web_ID_code << "," << id_parts[0] << "," << *(ae.code) << ",'" << dve.data->value_type << "'," << dve.data->offset << "," << dve.data->byte_len << "," << missing_ind << ",'" << uflag << "')";
	    }
	    string key_end="|"+obstype+"|"+platform+"|"+id_entry.first+"|"+datavar;
	    auto n=0;
	    for (const auto& time : times_list) {
		auto tparts=split(time,"[!]");
		size_t lat_i,lon_i;
		geoutils::convert_lat_lon_to_box(36,stof(tparts[1]),stof(tparts[2]),lat_i,lon_i);
		InsertEntry inse;
		inse.key=itos(lat_i)+"_"+itos(lon_i);
		if (!insert_table.found(inse.key,inse)) {
		  inse.data.reset(new InsertEntry::Data);
		  insert_table.insert(inse);
		}
		if (missing_ind != "0") {
		  auto found_missing=missing_table.found(itos(n)+key_end,se);
		  if ((missing_ind == "1" && !found_missing) || (missing_ind == "2" && found_missing)) {
		    if (inse.data->num_inserts >= 10000) {
			string result;
			if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_"+inse.key+" values "+inse.data->inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
			  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",USER);
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
	string result;
	if (!inse.data->inv_insert.empty()  && server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_"+key+" values "+inse.data->inv_insert.substr(1)+" on duplicate key update uflag=values(uflag)",result) < 0) {
	  metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert inventory data","iinv",USER);
	}
	server._delete("IObML.ds"+local_args.dsnum2+"_inventory_"+key,"webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
	server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_inventory_"+key,result);
    }
  }
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_dataTypes values "+data_type_ss.str().substr(1)+" on duplicate key update value_type=values(value_type),byte_offset=values(byte_offset),byte_length=values(byte_length),missing_ind=values(missing_ind),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert dataType information for '"+*(ae.code)+"'","iinv",USER);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds"+local_args.dsnum2+"_dataTypes",result);
  if (server.command("insert into IObML.ds"+local_args.dsnum2+"_inventory_summary values ("+web_ID_code+","+itos(nbytes)+","+itos(ndbytes)+",'"+uflag+"') on duplicate key update byte_length=values(byte_length),dataType_length=values(dataType_length),uflag=values(uflag)",result) < 0) {
    metautils::log_error("insert_obml_netcdf_point_inventory() returned error: "+server.error()+" while trying to insert file size data for '"+web_ID_code+"'","iinv",USER);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

void insert_generic_point_inventory(std::ifstream& ifs,MySQL::Server& server,string web_ID_code)
{
  auto uflag=strand(3);
  int d_min_lat_i=99,d_min_lon_i=99,d_max_lat_i=-1,d_max_lon_i=-1;
  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_dataTypesList_b")) {
    string result;
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypesList_b like IObML.template_dataTypesList_b",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypesList_b'","iinv",USER);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_dataTypes like IObML.template_dataTypes",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_dataTypes'","iinv",USER);
    }
    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
	metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",USER);
    }
  }
  unordered_map<string,string> times_table,obs_table,platform_table,id_table;
  unordered_map<size_t,std::tuple<size_t,size_t>> data_type_table;
  unordered_map<string,shared_ptr<unordered_map<size_t,size_t>>> datatypes_table;
  size_t byte_length=0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    string sline=line;
    if (regex_search(sline,regex("<!>"))) {
	auto lparts=split(sline,"<!>");
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
		metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get obsType code for '"+lparts[2]+"'","iinv",USER);
	    }
	    obs_table.emplace(lparts[1],row[0]);
	    break;
	  }
	  case 'P':
	  {
	    MySQL::LocalQuery query("code","WObML.platformTypes","platformType = '"+lparts[2]+"'");
	    MySQL::Row row;
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get platformType code for '"+lparts[2]+"'","iinv",USER);
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
		auto dparts=split(lparts[2],"[!]");
		MySQL::LocalQuery query("code","WObML.ds"+local_args.dsnum2+"_dataTypesList","observationType_code = "+obs_table[dparts[0]]+" and platformType_code = "+platform_table[dparts[1]]+" and dataType = '"+dparts[2]+"'");
		MySQL::Row row;
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  metautils::log_error("insert_generic_point_inventory() returned error: "+query.error()+" while trying to get dataType code for '"+lparts[2]+"'","iinv",USER);
		}
		data_type_table.emplace(stoi(lparts[1]),std::make_tuple(stoi(row[0]),stoi(dparts[3].substr(1))));
		string value_type;
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
		string scale;
		if (dparts.size() > 4) {
		  scale=dparts[4];
		}
		else {
		  scale="1";
		}
		if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypesList_b",row[0]+",'"+value_type+"',"+scale+","+dparts[3].substr(1)+",NULL") < 0) {
		  if (!regex_search(server.error(),regex("Duplicate entry"))) {
		    metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" while inserting row '"+row[0]+",'"+value_type+"',"+scale+","+dparts[3].substr(1)+",NULL'","iinv",USER);
		  }
		}
	    }
	    break;
	  }
	}
    }
    else if (regex_search(sline,regex("\\|"))) {
	auto iparts=split(sline,"|");
	auto idparts=split(id_table.at(iparts[2]),"|");
	int min_lat_i=stoi(idparts[1]);
	if (min_lat_i < d_min_lat_i) {
	  d_min_lat_i=min_lat_i;
	}
	int min_lon_i=stoi(idparts[2]);
	if (min_lon_i < d_min_lon_i) {
	  d_min_lon_i=min_lon_i;
	}
	int max_lat_i=stoi(idparts[3]);
	if (max_lat_i > d_max_lat_i) {
	  d_max_lat_i=max_lat_i;
	}
	int max_lon_i=stoi(idparts[4]);
	if (max_lon_i > d_max_lon_i) {
	  d_max_lon_i=max_lon_i;
	}
	shared_ptr<unordered_map<size_t,size_t>> p;
	auto e=datatypes_table.find(idparts[0]);
	if (e == datatypes_table.end()) {
	  p.reset(new unordered_map<size_t,size_t>);
	  datatypes_table.emplace(idparts[0],p);
	}
	else {
	  p=e->second;
	}
	vector<size_t> ivals,dvals;
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
	string bitmap;
	bitmap::compress_values(dvals,bitmap);
	for (int n=min_lat_i; n <= max_lat_i; ++n) {
	  for (int m=min_lon_i; m <= max_lon_i; ++m) {
	    if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_"+itos(n)+"_"+itos(m)+"_b",web_ID_code+","+times_table.at(iparts[1])+","+idparts[0]+",'"+bitmap+"',"+itos(min_dval)+","+itos(max_dval)+",'"+iparts[0]+"','"+uflag+"'","update dataType_codes = '"+bitmap+"', byte_offsets = '"+iparts[0]+"', uflag = '"+uflag+"'") < 0) {
		metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+times_table.at(iparts[1])+","+idparts[0]+",'"+bitmap+"',"+itos(min_dval)+","+itos(max_dval)+",'"+iparts[0]+"'' into table IObML.ds"+local_args.dsnum2+"_inventory_"+itos(n)+"_"+itos(m)+"_b","iinv",USER);
	    }
	  }
	}
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (int n=d_min_lat_i; n <= d_max_lat_i; ++n) {
    for (int m=d_min_lon_i; m <= d_max_lon_i; ++m) {
	server._delete("IObML.ds"+local_args.dsnum2+"_inventory_"+itos(n)+"_"+itos(m)+"_b","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
    }
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+itos(byte_length)+",0,'"+uflag+"'","update byte_length = "+itos(byte_length)+", uflag = '"+uflag+"'") < 0) {
    metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+itos(byte_length)+"' into table IObML.ds"+local_args.dsnum2+"_inventory_summary","iinv",USER);
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
  for (const auto& e : datatypes_table) {
    for (const auto& e2 : *e.second) {
	if (server.insert("IObML.ds"+local_args.dsnum2+"_dataTypes",web_ID_code+","+e.first+","+itos(e2.first)+",'',0,"+itos(e2.second)+",0,'"+uflag+"'","update value_type = '', byte_offset = 0, byte_length = "+itos(e2.second)+", missing_ind = 0, uflag = '"+uflag+"'") < 0) {
	  metautils::log_error("insert_generic_point_inventory() returned error: "+server.error()+" for insert: '"+web_ID_code+","+e.first+","+itos(e2.first)+",'',0,"+itos(e2.second)+",0'","iinv",USER);
	}
    }
  }
  server._delete("IObML.ds"+local_args.dsnum2+"_dataTypes","webID_code = "+web_ID_code+" and uflag != '"+uflag+"'");
}

void insert_obml_inventory()
{
  std::ifstream ifs;
  string sdum,sdum2,format_code,error;
  vector<string> ID_codes;
  my::map<StringEntry> latlon_table,unique_lines(99999);
  StringEntry se;
  list<string> inventory_tables;
  string lockstring;
  my::map<InventoryEntry> inventory_table;
  InventoryEntry ie;
  struct stat buf;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  sdum=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+metautils::args.filename+".gz",temp_dir.name());
  if (stat(sdum.c_str(),&buf) == 0) {
    system(("gunzip "+sdum).c_str());
    chop(sdum,3);
  }
  else {
    sdum=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+metautils::args.filename,temp_dir.name());
  }
  ifs.open(sdum.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("insert_obml_inventory() was not able to open "+metautils::args.filename,"iinv",USER);
  }
  sdum=substitute(metautils::args.filename,".ObML_inv","");
  replace_all(sdum,"%","/");
  MySQL::LocalQuery query;
  query.set("select code,format_code,tindex from WObML.ds"+local_args.dsnum2+"_webfiles2 as w left join dssdb.wfile as x on (x.dsid = 'ds"+metautils::args.dsnum+"' and x.type = 'D' and x.wfile = w.webID) where webID = '"+sdum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("insert_obml_inventory() returned error: "+query.error()+" while looking for code from webfiles","iinv",USER);
  }
  MySQL::Row row;
  query.fetch_row(row);
  auto web_ID_code=row[0];
  format_code=row[1];
  tindex=row[2];
  char line[32768];
  ifs.getline(line,32768);
  if (regex_search(line,regex("^netCDF:timeSeries"))) {
    string sline=line;
    insert_obml_netcdf_time_series_inventory(ifs,server,web_ID_code,stoi(sline.substr(sline.find("|")+1)));
  }
  else if (regex_search(line,regex("^netCDF:point"))) {
    string sline=line;
    insert_obml_netcdf_point_inventory(ifs,server,web_ID_code,stoi(sline.substr(sline.find("|")+1)));
  }
  else {
    insert_generic_point_inventory(ifs,server,web_ID_code);
  }
/*
  while (!ifs.eof()) {
    string sline=line;
    if (regex_search(sline,regex("<!>"))) {
	auto sp=split(sline,"<!>");
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
	    auto spx=split(sp[2],"[!]");
	    query.set("select i.code from WObML.ds"+local_args.dsnum2+"_IDs2 as i left join WObML.IDTypes as t on t.code = i.IDType_code where t.IDType= '"+spx[0]+"' and i.ID = '"+spx[1]+"'");
	    if (query.submit(server) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+query.error()+" while trying to get ID code","iinv",USER);
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
    else if (regex_search(sline,regex("\\|"))) {
	if (first) {
	  if (!MySQL::table_exists(server,"IObML.ds"+local_args.dsnum2+"_inventory_summary")) {
	    string result;
	    if (server.command("create table IObML.ds"+local_args.dsnum2+"_inventory_summary like IObML.template_inventory_summary",result) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to create 'ds"+local_args.dsnum2+"_inventory_summary'","iinv",USER);
	    }
	  }
	  if (server._delete("IObML.ds"+local_args.dsnum2+"_inventory_summary","webID_code = "+web_ID_code) < 0) {
	    metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while deleting web_ID_code "+web_ID_code+" from IObML.ds"+local_args.dsnum2+"_inventory_summary","iinv",USER);
	  }
	  inventory_tables=MySQL::table_names(server,"IObML","ds"+local_args.dsnum2+"_inventory_%",error);
	  lockstring="IObML.ds"+local_args.dsnum2+"_inventory_summary write";
	  for (auto& table : inventory_tables) {
	    if (!regex_search(table,regex("_summary$"))) {
		if (server._delete("IObML."+table,"webID_code = "+web_ID_code) < 0) {
		  metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to delete web_ID_code "+web_ID_code+" from 'IObML."+table+"'","iinv",USER);
		}
		lockstring+=", IObML."+table+" write";
	    }
	  }
	  lockstring+=", IObML.template_inventory_lati_loni write";
	  string result;
	  if (server.command("lock tables "+lockstring,result) < 0) {
	    metautils::log_error("unable to lock tables "+lockstring+"; error: "+server.error(),"iinv",USER);
	  }
	  first=false;
	}
	auto sp=split(sline,"|");
	total_bytes+=stoll(sp[1]);
	if (convert_lat_lon_to_box(36,stof(sp[6]),stof(sp[7]),lat_i,lon_i) < 0)
	  metautils::log_error("insert_obml_inventory() was not able to convert lat/lon: "+sp[6]+","+sp[7]+" to a 36-degree box","iinv",USER);
	sdum="IObML.ds"+local_args.dsnum2+"_inventory_"+itos(lat_i)+"_"+itos(lon_i);
	if (!latlon_table.found(sdum,se)) {
	  if (!MySQL::table_exists(server,sdum)) {
	    string result;
	    server.command("unlock tables",result);
	    if (server.command("create table "+sdum+" like IObML.template_inventory_lati_loni",result) < 0) {
		metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while trying to create '"+sdum+"'","iinv",USER);
	    }
	    lockstring+=", "+sdum+" write";
	    if (server.command("lock tables "+lockstring,result) < 0) {
		metautils::log_error("unable to lock tables "+lockstring+"; error: "+server.error(),"iinv",USER);
	    }
	  }
	  se.key=sdum;
	  latlon_table.insert(se);
	}
	n=stoi(sp[5]);
	m=stoi(sp[8]);
	sdum2=web_ID_code+","+sp[0]+","+sp[1]+","+sp[2]+","+ID_codes[n]+","+itos(lroundf(stof(sp[6])*10000.))+","+itos(lroundf(stof(sp[7])*10000.))+",'"+datatype_codes[m]+"'";
	se.key=web_ID_code+","+sp[2]+","+ID_codes[n]+","+datatype_codes[m];
	if (!unique_lines.found(se.key,se)) {
	  unique_lines.insert(se);
	  if (!inventory_table.found(sdum,ie)) {
	    ie.key=sdum;
	    ie.m_list.reset(new list<string>);
	    inventory_table.insert(ie);
	  }
	  ie.m_list->emplace_back(sdum2);
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
    if (server.insert(ie.key,*(ie.m_list)) < 0) {
	metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting multiple rows into "+ie.key,"iinv",USER);
    }
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+lltos(total_bytes)) < 0) {
    metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting row '"+web_ID_code+","+lltos(total_bytes)+"'","iinv",USER);
  }
  if (server.insert("IObML.ds"+local_args.dsnum2+"_inventory_summary",web_ID_code+","+lltos(total_bytes)) < 0) {
    error=server.error();
    if (!regex_search(error,regex("Duplicate entry"))) {
	metautils::log_error("insert_obml_inventory() returned error: "+server.error()+" while inserting row '"+web_ID_code+","+lltos(total_bytes)+"'","iinv",USER);
    }
  }
  string result;
  server.command("unlock tables",result);
  if (num_dupes > 0) {
    metautils::log_warning(itos(num_dupes)+" duplicate observations were ignored","iinv_dupes",USER);
  }
*/
  server.disconnect();
}

void insert_inventory()
{
  auto idx=metautils::args.filename.rfind(".");
  auto ext=metautils::args.filename.substr(idx+1);
  if (ext == "GrML_inv") {
    if (!local_args.wms_only) {
	insert_grml_inventory();
    }
//    build_wms_capabilities();
  }
  else if (ext == "ObML_inv") {
    insert_obml_inventory();
  }
  else {
    metautils::log_error("insert_inventory() does not recognize inventory extension "+ext,"iinv",USER);
  }
}

int main(int argc,char **argv)
{
  if (argc < 4) {
    cerr << "usage: iinv -d [ds]nnn.n [options...] -f file" << endl;
    cerr << "\nrequired:" << endl;
    cerr << "-d nnn.n   specifies the dataset number" << endl;
    cerr << "-f file    summarize information for inventory file <file>" << endl;
    cerr << "\noptions:" << endl;
    cerr << "-c/-C       create (default)/don't create file list cache" << endl;
    cerr << "-N          notify with a message when " << argv[0] << " completes" << endl;
    cerr << "-V          verbose mode" << endl;
    cerr << "--wms-only  only generate the WMS capabilities document for the file" << endl;
    exit(1);
  }
  auto t1=time(nullptr);
  signal(SIGSEGV,segv_handler);
  metautils::read_config("iinv",USER);
  parse_args(argc,argv);
/*
  if (metautils::args.dsnum == "999.9") {
    exit(0);
  }
*/
/*
metautils::log_warning("this command will be run by Bob later - you can ignore this warning","iinv",USER);
exit(0);
*/
  metautils::cmd_register("iinv",USER);
  temp_dir.create(metautils::directives.temp_path);
  insert_inventory();
  if (local_args.create_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv","iinv",USER);
    if (!tindex.empty() && tindex != "0") {
	gatherxml::summarizeMetadata::create_file_list_cache("inv","iinv",USER,tindex);
    }
  }
  if (local_args.notify) {
    cout << argv[0] << " has completed successfully" << endl;
  }
  auto t2=time(nullptr);
  metautils::log_warning("execution time: "+ftos(t2-t1)+" seconds","iinv.time",USER);
}
