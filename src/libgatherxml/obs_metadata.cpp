#include <fstream>
#include <string>
#include <regex>
#include <unordered_map>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <bsort.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <search.hpp>
#include <myerror.hpp>

namespace gatherxml {

namespace summarizeMetadata {

struct PointEntry {
  PointEntry() : key(),locations() {}

  std::string key;
  std::shared_ptr<std::list<std::string>> locations;
};
void check_point(double latp,double lonp,MySQL::Server& server,my::map<CodeEntry>& location_table)
{
  size_t num;
  std::string point[9],point2[9];

  static my::map<PointEntry> point_cache;
  PointEntry pe;
  pe.key=strutils::ftos(latp)+","+strutils::ftos(lonp);
  if (!point_cache.found(pe.key,pe)) {
    pe.locations=NULL;
    auto lat=latp+0.25;
    double lon;
    size_t cnt=0;
    for (size_t n=0; n < 3; ++n) {
	if (n != 2) {
	  num=2;
	  lon=lonp+0.25;
	}
	else {
	  num=1;
	  lat=latp;
	  lon=lonp;
	}
	for (size_t m=0; m < num; ++m) {
	  point[cnt]="POINT("+strutils::ftos(lat+90.)+" ";
	  point2[cnt]="";
	  if (lon < 0.) {
	    point[cnt]+=strutils::ftos(lon+360.);
	  }
	  else if (lon <= 30.) {
	    point2[cnt]=point[cnt];
	    point[cnt]+=strutils::ftos(lon);
	    point2[cnt]+=strutils::ftos(lon+360.);
	  }
	  else {
	    point[cnt]+=strutils::ftos(lon);
	  }
	  point[cnt]+=")";
	  if (!point2[cnt].empty()) {
	    point2[cnt]+=")";
	  }
	  lon-=0.5;
	  ++cnt;
	}
	lat-=0.5;
    }
    std::string where_conditions;
    for (size_t n=0; n < cnt; ++n) {
	if (!where_conditions.empty()) {
	  where_conditions+=" or ";
	}
	where_conditions+="Within(GeomFromText('"+point[n]+"'),bounds) = 1";
	if (!point2[n].empty()) {
	  where_conditions+=" or Within(GeomFromText('"+point2[n]+"'),bounds) = 1";
	}
    }
    MySQL::Query query("select name,AsText(bounds) from search.political_boundaries where "+where_conditions);
    if (query.submit(server) < 0) {
	myerror = "Error: " + query.error();
	exit(1);
    }
//std::cerr << query.show() << std::endl;
    for (const auto& res : query) {
	CodeEntry ce;
	if (!location_table.found(res[0],ce)) {
	  for (size_t n=0; n < cnt; ++n) {
	    if (within_polygon(res[1],point[n]) || (point2[n].length() > 0 && within_polygon(res[1],point2[n]))) {
		ce.key=res[0];
		location_table.insert(ce);
		if (pe.locations == nullptr) {
		  pe.locations.reset(new std::list<std::string>);
		}
		(pe.locations)->emplace_back(ce.key);
		n=cnt;
	    }
	  }
	}
    }
    point_cache.insert(pe);
  }
  else {
    if (pe.locations != nullptr) {
	for (const auto& location : *pe.locations) {
	  CodeEntry ce;
	  if (!location_table.found(location,ce)) {
	    ce.key=location;
	    location_table.insert(ce);
	  }
	}
    }
  }
}

void compress_locations(std::unordered_set<std::string>& location_list,my::map<ParentLocation>& parent_location_table,std::vector<std::string>& sorted_array,std::string caller,std::string user)
{
  ParentLocation pl,pl2;

  TempDir temp_dir;
  if (!temp_dir.create("/tmp")) {
    myerror = "Error creating temporary directory";
    exit(1);
  }
  std::string gcmd_locations;
  struct stat buf;
  if (stat((metautils::directives.server_root+"/web/metadata/gcmd_locations").c_str(),&buf) == 0) {
    gcmd_locations=metautils::directives.server_root+"/web/metadata/gcmd_locations";
  }
  else {
    gcmd_locations=unixutils::remote_web_file("http://rda.ucar.edu/metadata/gcmd_locations",temp_dir.name());
  }
  std::ifstream ifs(gcmd_locations.c_str());
  if (!ifs.is_open()) {
    myerror = "Error opening gcmd_locations";
    exit(1);
  }
  char line[256];
  ifs.getline(line,256);
  while (!ifs.eof()) {
    std::string sline=line;
    pl.key=sline;
    while (strutils::occurs(pl.key," > ") > 1) {
	auto children_key=pl.key;
	pl.key=pl.key.substr(0,pl.key.rfind(" > "));
	if (!parent_location_table.found(pl.key,pl)) {
	  pl.children_set.reset(new std::unordered_set<std::string>);
	  pl.children_set->emplace(children_key);
	  pl.matched_set=nullptr;
	  pl.consolidated_parent_set.reset(new std::unordered_set<std::string>);
	  parent_location_table.insert(pl);
	}
	else {
	  if (pl.children_set->find(children_key) == pl.children_set->end()) {
	    pl.children_set->emplace(children_key);
	  }
	}
    }
    ifs.getline(line,256);
  }
  ifs.close();
//special handling for Antarctica since it has no children
  pl.key="Continent > Antarctica";
  pl.children_set.reset(new std::unordered_set<std::string>);
  pl.matched_set=nullptr;
  pl.consolidated_parent_set.reset(new std::unordered_set<std::string>);
  parent_location_table.insert(pl);
// match location keywords to their parents
  for (auto item : location_list) {
    if (strutils::occurs(item," > ") > 1) {
	pl.key=item.substr(0,item.rfind(" > "));
    }
    else {
	pl.key=item;
    }
    parent_location_table.found(pl.key,pl);
    if (pl.matched_set == nullptr) {
	pl.matched_set.reset(new std::unordered_set<std::string>);
	parent_location_table.replace(pl);
    }
    pl.matched_set->emplace(item);
  }
  sorted_array.clear();
  sorted_array.reserve(parent_location_table.size());
  for (auto& key : parent_location_table.keys()) {
    sorted_array.emplace_back(key);
  }
  binary_sort(sorted_array,
  [](const std::string& left,const std::string& right) -> bool
  {
    if (strutils::occurs(left," > ") > strutils::occurs(right," > ")) {
	return true;
    }
    else if (strutils::occurs(left," > ") < strutils::occurs(right," > ")) {
	return false;
    }
    else {
	if (left <= right) {
	  return true;
	}
	else {
	  return false;
	}
    }
  });
// continue matching parents to their parents until done
  auto matched_to_parent=true;
  while (matched_to_parent) {
    matched_to_parent=false;
    for (size_t n=0; n < parent_location_table.size(); ++n) {
	pl.key=sorted_array[n];
	if (strutils::occurs(pl.key," > ") > 1) {
	  parent_location_table.found(pl.key,pl);
	  if (pl.matched_set != nullptr) {
	    pl2.key=pl.key.substr(0,pl.key.rfind(" > "));
	    parent_location_table.found(pl2.key,pl2);
	    if (pl2.matched_set == nullptr) {
		pl2.matched_set.reset(new std::unordered_set<std::string>);
		parent_location_table.replace(pl2);
	    }
	    for (const auto& e : *pl.children_set) {
		pl2.children_set->emplace(e);
	    }
	    for (const auto& e : *pl.matched_set) {
		pl2.matched_set->emplace(e);
	    }
	    for (const auto& e : *pl.consolidated_parent_set) {
		pl2.consolidated_parent_set->emplace(e);
	    }
	    pl2.consolidated_parent_set->emplace(pl.key);
	    pl.matched_set->clear();
	    pl.matched_set.reset();
	    pl.matched_set=nullptr;
	    parent_location_table.replace(pl);
	    matched_to_parent=true;
	  }
	}
    }
  }
}

bool summarize_obs_data(std::string caller,std::string user)
{
  const std::string THIS_FUNC=__func__;
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  bool update_bitmap=false;
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Query formats_query("code,format_code","WObML.ds"+dsnum2+"_webfiles2");
  if (formats_query.submit(server) < 0) {
    myerror = formats_query.error();
    exit(1);
  }
  std::unordered_map<std::string,std::string> data_file_formats_map;
  for (const auto& format_row : formats_query) {
    data_file_formats_map.emplace(format_row[0],format_row[1]);
  }
  MySQL::LocalQuery obs_search_query("format_code,observationType_code,platformType_code,box1d_row,box1d_bitmap","search.obs_data","dsid = '"+metautils::args.dsnum+"'");
  if (obs_search_query.submit(server) < 0) {
    metautils::log_error(THIS_FUNC+"() returned '"+obs_search_query.error()+"' while querying search.obs_data",caller,user);
  }
  my::map<SummaryEntry> current_obsdata_table;
  for (const auto& obs_search_row : obs_search_query) {
    SummaryEntry se;
    se.key=obs_search_row[0]+"<!>"+obs_search_row[1]+"<!>"+obs_search_row[2]+"<!>"+obs_search_row[3];
    se.box1d_bitmap=obs_search_row[4];
    current_obsdata_table.insert(se);
  }
  std::string error;
  if (server.command("lock tables WObML.ds"+dsnum2+"_locations write",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  MySQL::LocalQuery locations_query("webID_code,observationType_code,platformType_code,start_date,end_date,box1d_row,box1d_bitmap","WObML.ds"+dsnum2+"_locations");
  if (locations_query.submit(server) < 0) {
    metautils::log_error(THIS_FUNC+"() returned '"+locations_query.error()+"' while querying WObML.ds"+dsnum2+"_locations",caller,user);
  }
  my::map<SummaryEntry> summary_table;
  for (const auto& location_row : locations_query) {
    if (data_file_formats_map.find(location_row[0]) == data_file_formats_map.end()) {
	metautils::log_error(THIS_FUNC+"() found a webID ("+location_row[0]+") in WObML.ds"+dsnum2+"_locations that doesn't exist in WObML.ds"+dsnum2+"_webfiles2",caller,user);
    }
    SummaryEntry se;
    se.key=data_file_formats_map[location_row[0]]+"<!>"+location_row[1]+"<!>"+location_row[2]+"<!>"+location_row[5];
    if (!summary_table.found(se.key,se)) {
	se.start_date=location_row[3];
	se.end_date=location_row[4];
	se.box1d_bitmap=location_row[6];
	summary_table.insert(se);
    }
    else {
	if (location_row[3] < se.start_date) {
	  se.start_date=location_row[3];
	}
	if (location_row[4] > se.end_date) {
	  se.end_date=location_row[4];
	}
	if (location_row[6] != se.box1d_bitmap) {
	  se.box1d_bitmap=bitmap::add_box1d_bitmaps(se.box1d_bitmap,location_row[6]);
	}
	summary_table.replace(se);
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  for (const auto& key : summary_table.keys()) {
    SummaryEntry se,se2;
    if (summary_table.found(key,se) && current_obsdata_table.found(key,se2)) {
	if (se.box1d_bitmap != se2.box1d_bitmap) {
	  update_bitmap=true;
	  break;
	}
    }
    else {
	update_bitmap=true;
	break;
    }
  }
  if (server.command("lock tables search.obs_data write",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  server._delete("search.obs_data","dsid = '"+metautils::args.dsnum+"'");
  for (const auto& key : summary_table.keys()) {
    auto sp=strutils::split(key,"<!>");
    SummaryEntry se;
    summary_table.found(key,se);
    if (server.insert("search.obs_data","'"+metautils::args.dsnum+"',"+sp[0]+","+sp[1]+","+sp[2]+","+se.start_date+","+se.end_date+","+sp[3]+",'"+se.box1d_bitmap+"'") < 0) {
	error=server.error();
	if (!strutils::has_beginning(error,"Duplicate entry")) {
	  metautils::log_error(THIS_FUNC+"(): "+error,caller,user);
	}
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  error=summarize_locations("WObML");
  if (error.length() > 0) {
    metautils::log_error(THIS_FUNC+"(): summarize_locations() returned '"+error+"'",caller,user);
  }
  server.disconnect();
  return update_bitmap;
}

} // end namespace gatherxml::summarizeMetadata

namespace markup {

namespace ObML {

void DataTypeEntry::fill_vertical_resolution_data(std::vector<double>& level_list,std::string z_positive_direction,std::string z_units)
{
  auto min=1.e38,max=-1.e38;
  for (size_t n=0; n < level_list.size(); ++n) {
    if (level_list[n] < min) {
	min=level_list[n];
    }
    if (level_list[n] > max) {
	max=level_list[n];
    }
  }
  if (data->vdata == nullptr) {
    data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::VerticalData);
  }
  if (z_positive_direction == "down") {
    std::sort(level_list.begin(),level_list.end(),
    [](const double& left,const double& right) -> bool
    {
	if (left > right) {
	  return true;
	}
	else {
	  return false;
	}
    });
    if (data->vdata->min_altitude > 1.e37) {
	data->vdata->min_altitude=-data->vdata->min_altitude;
    }
    if (data->vdata->max_altitude < -1.e37) {
	data->vdata->max_altitude=-data->vdata->max_altitude;
    }
    if (max > data->vdata->min_altitude) {
	data->vdata->min_altitude=max;
    }
    if (min < data->vdata->max_altitude) {
	data->vdata->max_altitude=min;
    }
  }
  else {
    std::sort(level_list.begin(),level_list.end(),
    [](const double& left,const double& right) -> bool
    {
	if (left < right) {
	  return true;
	}
	else {
	  return false;
	}
    });
    if (min < data->vdata->min_altitude) {
	data->vdata->min_altitude=min;
    }
    if (max > data->vdata->max_altitude) {
	data->vdata->max_altitude=max;
    }
  }
  data->vdata->units=z_units;
  data->vdata->avg_nlev+=level_list.size();
  auto avg_vres=0.;
  for (size_t n=1; n < level_list.size(); ++n) {
    avg_vres+=fabs(level_list[n]-level_list[n-1]);
  }
  data->vdata->avg_res+=(avg_vres/(level_list.size()-1));
  ++data->vdata->res_cnt;
}

ObservationData::ObservationData() : num_types(0),observation_types(),observation_indexes(),id_tables(),platform_tables(),unique_observation_table(),unknown_id_re("unknown"),unknown_ids(nullptr),track_unique_observations(true),is_empty(true)
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::LocalQuery query("obsType","ObML.obsTypes");
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	observation_types.emplace(num_types,row[0]);
	observation_indexes.emplace(row[0],num_types);
	++num_types;
    }
    for (size_t n=0; n < num_types; ++n) {
	id_tables.emplace_back(new my::map<IDEntry>(9999));
	platform_tables.emplace_back(new my::map<PlatformEntry>);
    }
  }
}

bool ObservationData::added_to_ids(std::string observation_type,IDEntry& ientry,std::string data_type,std::string data_type_map,float lat,float lon,double unique_timestamp,DateTime *start_datetime,DateTime *end_datetime)
{
  if (lat < -90. || lat > 90. || lon < -180. || lon > 360.) {
    myerror="latitude or longitude out of range";
    return false;
  }
  auto o=observation_indexes.find(observation_type);
  if (o == observation_indexes.end()) {
    myerror="no index for observation type '"+observation_type+"'";
    return false;
  }
  const static DateTime base(1000,1,1,0,0);
  if (unique_timestamp < 0.) {
    unique_timestamp=start_datetime->seconds_since(base);
  }
  auto true_lon= (lon <= 180.) ? lon : (lon-360.);
  if (!id_tables[o->second]->found(ientry.key,ientry)) {
    ientry.data.reset(new IDEntry::Data);
    ientry.data->S_lat=ientry.data->N_lat=lat;
    ientry.data->W_lon=ientry.data->E_lon=true_lon;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (size_t n=0; n < 360; ++n) {
	ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    size_t n,m;
    try {
	geoutils::convert_lat_lon_to_box(1,0.,true_lon,n,m);
    }
    catch (std::exception& e) {
	myerror=e.what();
	return false;
    }
    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=true_lon;
    ientry.data->start=*start_datetime;
    if (end_datetime == nullptr) {
	ientry.data->end=ientry.data->start;
    }
    else {
	ientry.data->end=*end_datetime;
    }
    id_tables[o->second]->insert(ientry);
    DataTypeEntry dte;
    dte.key=data_type;
    dte.data.reset(new DataTypeEntry::Data);
    dte.data->map=data_type_map;
    ientry.data->data_types_table.insert(dte);
    if (track_unique_observations) {
	ientry.data->nsteps=1;
	dte.data->nsteps=1;
	std::stringstream key;
	key.setf(std::ios::fixed);
	key << o->second << ientry.key << unique_timestamp << std::endl;
	unique_observation_table.emplace(key.str(),std::vector<std::string>{data_type});
    }
  }
  else {
    if (lat != ientry.data->S_lat || true_lon != ientry.data->W_lon) {
	if (lat < ientry.data->S_lat) {
	  ientry.data->S_lat=lat;
	}
	if (lat > ientry.data->N_lat) {
	  ientry.data->N_lat=lat;
	}
	if (true_lon < ientry.data->W_lon) {
	  ientry.data->W_lon=true_lon;
	}
	if (true_lon > ientry.data->E_lon) {
	  ientry.data->E_lon=true_lon;
	}
	size_t n,m;
	try {
	  geoutils::convert_lat_lon_to_box(1,0.,true_lon,n,m);
	}
	catch (std::exception& e) {
	  myerror=e.what();
	  return false;
	}
	if (ientry.data->min_lon_bitmap[m] > 900.) {
	  ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=true_lon;
	}
	else {
	  if (true_lon < ientry.data->min_lon_bitmap[m]) {
	    ientry.data->min_lon_bitmap[m]=true_lon;
	  }
	  if (true_lon > ientry.data->max_lon_bitmap[m]) {
	    ientry.data->max_lon_bitmap[m]=true_lon;
	  }
	}
    }
    if (*start_datetime < ientry.data->start) {
	ientry.data->start=*start_datetime;
    }
    if (end_datetime == nullptr) {
	if (*start_datetime > ientry.data->end) {
	  ientry.data->end=*start_datetime;
	}
    }
    else {
	if (*end_datetime > ientry.data->end) {
	  ientry.data->end=*end_datetime;
	}
    }
    DataTypeEntry dte;
    if (!ientry.data->data_types_table.found(data_type,dte)) {
	dte.key=data_type;
	dte.data.reset(new DataTypeEntry::Data);
	dte.data->map=data_type_map;
	ientry.data->data_types_table.insert(dte);
    }
    if (track_unique_observations) {
	std::stringstream key;
	key.setf(std::ios::fixed);
	key << o->second << ientry.key << unique_timestamp << std::endl;
	if (unique_observation_table.find(key.str()) == unique_observation_table.end()) {
	  ++(ientry.data->nsteps);
	  ++(dte.data->nsteps);
	  unique_observation_table.emplace(key.str(),std::vector<std::string>{data_type});
	}
	else {
	  auto &data_types=unique_observation_table[key.str()];
	  if (std::find(data_types.begin(),data_types.end(),data_type) == data_types.end()) {
	    ++(dte.data->nsteps);
	    data_types.emplace_back(data_type);
	  }
	}
    }
  }
  if (std::regex_search(ientry.key,unknown_id_re)) {
    if (unknown_ids == nullptr) {
	unknown_ids.reset(new std::unordered_set<std::string>);
    }
    unknown_ids->emplace(ientry.key);
  }
  is_empty=false;
  return true;
}

bool ObservationData::added_to_platforms(std::string observation_type,std::string platform_type,float lat,float lon)
{
  if (lat < -90. || lat > 90.) {
    myerror="latitude '"+strutils::ftos(lat)+"' out of range";
    return false;
  }
  if (lon < -180. || lon > 360.) {
    myerror="longitude '"+strutils::ftos(lat)+"' out of range";
    return false;
  }
  auto o=observation_indexes.find(observation_type);
  if (o == observation_indexes.end()) {
    myerror="no index for observation type '"+observation_type+"'";
    return false;
  }
  PlatformEntry pentry;
  if (!platform_tables[o->second]->found(platform_type,pentry)) {
    pentry.key=platform_type;
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    platform_tables[o->second]->insert(pentry);
  }
  if (lat == -90.) {
    pentry.boxflags->spole=1;
  }
  else if (lat == 90.) {
    pentry.boxflags->npole=1;
  }
  else {
    size_t n,m;
    auto true_lon= (lon <= 180.) ? lon : (lon-360.);
    try {
	geoutils::convert_lat_lon_to_box(1,lat,true_lon,n,m);
    }
    catch (std::exception& e) {
	myerror=e.what();
	return false;
    }
    pentry.boxflags->flags[n-1][m]=1;
    pentry.boxflags->flags[n-1][360]=1;
  }
  return true;
}

} // end namespace gatherxml::markup::ObML

} // end namespace gatherxml::markup

} // end namespace gatherxml
