#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <sstream>
#include <regex>
#include <forward_list>
#include <unordered_set>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <netcdf.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>
#include <bufr.hpp>

metautils::Directives directives;
metautils::Args args;
metautils::NcTime::Time time_s;
metautils::NcTime::TimeBounds time_bounds_s;
metautils::NcTime::TimeData time_data;
my::map<metadata::ObML::PlatformEntry> platform_table[metadata::ObML::NUM_OBS_TYPES];
metadata::ObML::PlatformEntry pentry;
my::map<metadata::ObML::IDEntry> **ID_table=NULL;
metadata::ObML::IDEntry ientry;
my::map<metautils::StringEntry> uniqueObservationTable(999999),uniqueDataTypeObservationTable(999999),dataTypeTable;
metadata::ObML::DataTypeEntry de;
my::map<metadata::GrML::GridEntry> grid_table;
metadata::GrML::ParameterEntry param_entry;
metadata::GrML::LevelEntry lentry;
metadata::GrML::GridEntry gentry;
std::string user=getenv("USER");
enum {GrML_type=1,ObML_type};
int write_type=-1;
TempFile *tfile=NULL;
TempDir *tdir=NULL;
TempFile *inv_file=NULL;
std::ofstream inv;
bool verbose_operation=false;
size_t total_num_not_missing=0;
std::unordered_map<std::string,std::pair<int,std::string>> D_map,G_map,I_map,L_map,O_map,P_map,R_map,U_map;
struct InvTimeEntry {
  InvTimeEntry() : key(),dt() {}

  size_t key;
  std::string dt;
};
std::list<std::string> inv_lines;
TempFile inv_lines2("/tmp");
std::unordered_set<std::string> unknown_IDs;
bool is_large_offset=false;
std::string myerror="";

void sortInventoryMap(std::unordered_map<std::string,std::pair<int,std::string>>& inv_table,std::vector<std::pair<int,std::string>>& sorted_keys)
{
  sorted_keys.clear();
  for (const auto& e : inv_table) {
    sorted_keys.emplace_back(std::make_pair(e.second.first,e.first));
  }
  std::sort(sorted_keys.begin(),sorted_keys.end(),
  [](const std::pair<int,std::string>& left,const std::pair<int,std::string>& right) -> bool
  {
    if (left.first <= right.first) {
	return true;
    }
    else {
	return false;
    }
  });
}

extern "C" void cleanUp()
{
  if (tfile != NULL) {
    delete tfile;
  }
  if (tdir != NULL) {
    delete tdir;
  }
  if (myerror.length() > 0) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::logError(myerror,"nc2xml",user,args.argsString);
  }
}

void parseArgs()
{
  std::deque<std::string> sp;
  size_t n;

  args.updateDB=true;
  args.updateSummary=true;
  args.overridePrimaryCheck=false;
  args.overwriteOnly=false;
  args.regenerate=true;
  args.temp_loc=directives.tempPath;
  sp=strutils::split(args.argsString,"%");
  for (n=0; n < sp.size()-1; ++n) {
    if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	args.member_name=sp[++n];
    }
    else if (sp[n] == "-I") {
	args.inventoryOnly=true;
	args.updateDB=false;
    }
    else if (sp[n] == "-S") {
	args.updateSummary=false;
    }
    else if (sp[n] == "-U") {
	if (user == "dattore") {
	  args.updateDB=false;
	}
    }
    else if (sp[n] == "-NC") {
	if (user == "dattore") {
	  args.overridePrimaryCheck=true;
	}
    }
    else if (sp[n] == "-OO") {
	if (user == "dattore") {
	  args.overwriteOnly=true;
	}
    }
    else if (sp[n] == "-R") {
	if (user == "dattore") {
	  args.regenerate=false;
	}
    }
    else if (sp[n] == "-t") {
	args.temp_loc=sp[++n];
    }
    else if (sp[n] == "-V") {
	verbose_operation=true;
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.overridePrimaryCheck=true;
    args.updateDB=false;
    args.updateSummary=false;
    args.regenerate=false;
  }
  n=sp.back().rfind("/");
  args.path=sp.back().substr(0,n);
  if (!std::regex_search(args.path,std::regex("^/FS/DSS")) && !std::regex_search(args.path,std::regex("^/DSS")) && !std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
    std::cerr << "Error: bad path" << std::endl;
    exit(1);
  }
  args.filename=sp.back().substr(n+1);
}

void initializeForObservations()
{
  if (ID_table == NULL) {
    ID_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
    for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
	ID_table[n]=new my::map<metadata::ObML::IDEntry>(999999);
    }
  }
}

void updateIDTable(size_t obsTypeIndex,float lat,float lon,DateTime& datetime,double unique_timestamp,DateTime *min_datetime,DateTime *max_datetime)
{
  size_t n,m;
  metautils::StringEntry se;

  if (!ID_table[obsTypeIndex]->found(ientry.key,ientry)) {
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->N_lat=ientry.data->S_lat=lat;
    ientry.data->W_lon=ientry.data->E_lon=lon;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (n=0; n < 360; ++n) {
	ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    convertLatLonToBox(1,0.,ientry.data->W_lon,n,m);
    ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=ientry.data->W_lon;
    if (min_datetime == NULL) {
	ientry.data->start=datetime;
    }
    else {
	ientry.data->start=*min_datetime;
    }
    if (max_datetime == NULL) {
	ientry.data->end=ientry.data->start;
    }
    else {
	ientry.data->end=*max_datetime;
    }
    ientry.data->nsteps=1;
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=1;
    ientry.data->data_types_table.insert(de);
    ID_table[obsTypeIndex]->insert(ientry);
    se.key=strutils::itos(obsTypeIndex)+";"+ientry.key+"-"+strutils::dtos(unique_timestamp);
    uniqueObservationTable.insert(se);
    se.key+=":"+de.key;
    uniqueDataTypeObservationTable.insert(se);
  }
  else {
    if (lat != ientry.data->S_lat || lon != ientry.data->W_lon) {
	if (lat < ientry.data->S_lat) {
	  ientry.data->S_lat=lat;
	}
	if (lat > ientry.data->N_lat) {
	  ientry.data->N_lat=lat;
	}
	if (lon < ientry.data->W_lon) {
	  ientry.data->W_lon=lon;
	}
	if (lon > ientry.data->E_lon) {
	  ientry.data->E_lon=lon;
	}
	convertLatLonToBox(1,0.,lon,n,m);
	if (ientry.data->min_lon_bitmap[m] > 900.) {
	  ientry.data->min_lon_bitmap[m]=ientry.data->max_lon_bitmap[m]=lon;
	}
	else {
	  if (lon < ientry.data->min_lon_bitmap[m]) {
	    ientry.data->min_lon_bitmap[m]=lon;
	  }
	  if (lon > ientry.data->max_lon_bitmap[m]) {
	    ientry.data->max_lon_bitmap[m]=lon;
	  }
	}
    }
    if (min_datetime == NULL) {
	if (datetime < ientry.data->start) {
	  ientry.data->start=datetime;
	}
    }
    else {
	if (*min_datetime < ientry.data->start) {
	  ientry.data->start=*min_datetime;
	}
    }
    if (max_datetime == NULL) {
	if (datetime > ientry.data->end) {
	  ientry.data->end=datetime;
	}
    }
    else {
	if (*max_datetime > ientry.data->end) {
	  ientry.data->end=*max_datetime;
	}
    }
    se.key=strutils::itos(obsTypeIndex)+";"+ientry.key+"-"+strutils::dtos(unique_timestamp);
    if (!uniqueObservationTable.found(se.key,se)) {
	++(ientry.data->nsteps);
	uniqueObservationTable.insert(se);
    }
    se.key+=":"+de.key;
    if (!ientry.data->data_types_table.found(de.key,de)) {
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	ientry.data->data_types_table.insert(de);
    }
    if (!uniqueDataTypeObservationTable.found(se.key,se)) {
	++(de.data->nsteps);
	uniqueDataTypeObservationTable.insert(se);
    }
  }
  if (std::regex_search(ientry.key,std::regex("unknown"))) {
    unknown_IDs.emplace(ientry.key);
  }
}

void updateIDTable(size_t obsTypeIndex,float lat,float lon,DateTime& datetime,double unique_timestamp)
{
  updateIDTable(obsTypeIndex,lat,lon,datetime,unique_timestamp,nullptr,nullptr);
}

void updatePlatformTable(size_t obsTypeIndex,float lat,float lon)
{
  if (!platform_table[obsTypeIndex].found(pentry.key,pentry)) {
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    platform_table[obsTypeIndex].insert(pentry);
  }
  if (lat == -90.) {
    pentry.boxflags->spole=1;
  }
  else if (lat == 90.) {
    pentry.boxflags->npole=1;
  }
  else {
    size_t n,m;
    convertLatLonToBox(1,lat,lon,n,m);
    pentry.boxflags->flags[n-1][m]=1;
    pentry.boxflags->flags[n-1][360]=1;
  }
}

void fillNcTimeData(const InputNetCDFStream::Attribute& attr)
{
  auto u=*(reinterpret_cast<std::string *>(attr.values));
  if (verbose_operation) {
    std::cout << "  Time units: '" << u << "'" << std::endl;
  }
  if (std::regex_search(u,std::regex("since"))) {
    time_data.units=strutils::to_lower(u.substr(0,u.find("since")));
    strutils::trim(time_data.units);
    u=u.substr(u.find("since")+5);
    while (!u.empty() && (u[0] < '0' || u[0] > '9')) {
	u=u.substr(1);
    }
    auto n=u.length()-1;
    while (n > 0 && (u[n] < '0' || u[n] > '9')) {
	--n;
    }
    ++n;
    if (n < u.length()) {
	u=u.substr(0,n);
    }
    strutils::trim(u);
    auto uparts=strutils::split(u);
    if (uparts.size() < 1 || uparts.size() > 3) {
	metautils::logError("fillNcTimeData returned error: unable to get reference time from units specified as: '"+*(reinterpret_cast<std::string *>(attr.values))+"'","nc2xml",user,args.argsString);
    }
    auto dparts=strutils::split(uparts[0],"-");
    if (dparts.size() != 3) {
	metautils::logError("fillNcTimeData returned error: unable to get reference time from units specified as: '"+*(reinterpret_cast<std::string *>(attr.values))+"'","nc2xml",user,args.argsString);
    }
    time_data.reference.setYear(std::stoi(dparts[0]));
    time_data.reference.setMonth(std::stoi(dparts[1]));
    time_data.reference.setDay(std::stoi(dparts[2]));
    if (uparts.size() > 1) {
	auto tparts=strutils::split(uparts[1],":");
	switch (tparts.size()) {
	  case 1:
	    time_data.reference.setTime(std::stoi(tparts[0])*10000);
	    break;
	  case 2:
	    time_data.reference.setTime(std::stoi(tparts[0])*10000+std::stoi(tparts[1])*100);
	    break;
	  case 3:
	    time_data.reference.setTime(std::stoi(tparts[0])*10000+std::stoi(tparts[1])*100+static_cast<int>(std::stof(tparts[2])));
	    break;
	}
    }
    if (verbose_operation) {
	std::cout << "  Reference time set to: " << time_data.reference.toString("%Y-%m-%d %H:%MM:%SS") << std::endl;
    }
  }
  else {
    metautils::logError("fillNcTimeData returned error: unable to get CF time from time variable units","nc2xml",user,args.argsString);
  }
}

DateTime computeNcTime(netCDFStream::VariableData& times,size_t index)
{
  DateTime dt;
  double val=-1;
  static int this_year=getCurrentDateTime().getYear();

  val=times[index];
  if (val < 0.) {
    metautils::logError("computeNcTime returned error: negative time offset not allowed","nc2xml",user,args.argsString);
  }
  if (time_data.units == "seconds") {
    dt=time_data.reference.secondsAdded(val);
  }
  else if (time_data.units == "hours") {
    if (myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.hoursAdded(val);
    }
    else {
	dt=time_data.reference.secondsAdded(lroundf(val*3600.));
    }
  }
  else if (time_data.units == "days") {
    if (myequalf(val,static_cast<int>(val),0.001)) {
	dt=time_data.reference.daysAdded(val);
    }
    else {
	dt=time_data.reference.secondsAdded(lroundf(val*86400.));
    }
  }
  else {
    metautils::logError("computeNcTime returned error: unable to set date/time for units '"+time_data.units+"'","nc2xml",user,args.argsString);
  }
  if (verbose_operation && dt.getYear() > this_year) {
    std::cout << "Warning: " << dt.toString() << " is in the future; time value: " << val << "; time type: " << static_cast<int>(times.type()) << std::endl;
  }
  return dt;
}

void extractFromVariableAttribute(const std::vector<InputNetCDFStream::Attribute>& attr,std::string& long_name,std::string& units,netCDFStream::NcType nc_type,netCDFStream::DataValue& miss_val)
{
  long_name="";
  units="";
  miss_val.clear();
  for (size_t n=0; n < attr.size(); ++n) {
    if (attr[n].name == "long_name") {
	long_name=*(reinterpret_cast<std::string *>(attr[n].values));
	strutils::trim(long_name);
    }
    else if (attr[n].name == "units") {
	units=*(reinterpret_cast<std::string *>(attr[n].values));
	strutils::trim(units);
    }
    else if (attr[n].name == "_FillValue" || attr[n].name == "missing_value") {
	miss_val.resize(nc_type);
	switch (nc_type) {
	  case netCDFStream::NcType::CHAR:
	  {
	    miss_val.set(*(reinterpret_cast<char *>(attr[n].values)));
	    break;
	  }
	  case netCDFStream::NcType::INT:
	  {
	    miss_val.set(*(reinterpret_cast<int *>(attr[n].values)));
	    break;
	  }
	  case netCDFStream::NcType::FLOAT:
	  {
	    miss_val.set(*(reinterpret_cast<float *>(attr[n].values)));
	    break;
	  }
	  case netCDFStream::NcType::DOUBLE:
	  {
	    miss_val.set(*(reinterpret_cast<double *>(attr[n].values)));
	    break;
	  }
	  default: {}
	}
    }
  }
}

bool found_missing(const double& time,const netCDFStream::DataValue& time_miss_val,const double& var_value,const netCDFStream::DataValue& miss_val)
{
  bool missing;
  if (time_miss_val.type() == netCDFStream::NcType::_NULL) {
    missing=false;
  }
  else {
//    if (myequalf(times[index/num_vals_at_each_time],time_miss_val.get())) {
if (myequalf(time,time_miss_val.get())) {
	missing=true;
    }
    else {
	missing=false;
    }
  }
  if (missing) {
    return true;
  }
  if (miss_val.type() == netCDFStream::NcType::_NULL) {
    missing=false;
  }
  else {
    missing=true;
//    if (var_data[index] != miss_val.get()) {
if (var_value != miss_val.get()) {
	missing=false;
    }
  }
  return missing;
}

void addGriddedNetCDFParameter(const netCDFStream::Variable& var,const bool& foundMap,const DateTime& firstValidDateTime,const DateTime& lastValidDateTime,int nsteps,my::map<metautils::StringEntry>& parameterTable,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable,ParameterMap& parameterMap)
{
  std::string descr="";
  std::string units="";
  for (size_t n=0; n < var.attrs.size(); ++n) {
    if (var.attrs[n].nc_type == netCDFStream::NcType::CHAR) {
	if (var.attrs[n].name == "long_name") {
	  descr=*(reinterpret_cast<std::string *>(var.attrs[n].values));
	}
	else if (var.attrs[n].name == "units") {
	  units=*(reinterpret_cast<std::string *>(var.attrs[n].values));
	}
	else if ((std::regex_search(var.attrs[n].name,std::regex("^comment")) || std::regex_search(var.attrs[n].name,std::regex("^Comment"))) && descr.length() == 0) {
	  descr=*(reinterpret_cast<std::string *>(var.attrs[n].values));
	}
	else if (strutils::to_lower(var.attrs[n].name) == "description" && descr.length() == 0) {
	  descr=*(reinterpret_cast<std::string *>(var.attrs[n].values));
	}
    }
  }
  metautils::StringEntry se;
  se.key=var.name+"<!>"+descr+"<!>"+units;
  if (!parameterTable.found(se.key,se)) {
    auto short_name=parameterMap.getShortName(var.name);
    if (!foundMap || short_name.empty()) {
	parameterTable.insert(se);
	varList.emplace_back(se.key);
    }
    else {
	parameterTable.insert(se);
	varList.emplace_back(se.key);
	se.key=var.name;
	changedVarTable.insert(se);
    }
  }
  param_entry.startDateTime=firstValidDateTime;
  param_entry.endDateTime=lastValidDateTime;
  param_entry.numTimeSteps=nsteps;
  lentry.parameter_code_table.insert(param_entry);
  if (inv.is_open()) {
    if (P_map.find(param_entry.key) == P_map.end()) {
	P_map.emplace(param_entry.key,std::make_pair(P_map.size(),""));
    }
  }
}

bool isZonalMeanGridVariable(const netCDFStream::Variable& var,size_t timedimid,int levdimid,size_t latdimid)
{
  if ((var.dimids.size() == 2 || var.dimids.size() == 3) && var.dimids[0] == timedimid && ((var.dimids.size() == 3 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid) || (var.dimids.size() == 2 && levdimid < 0 && var.dimids[1] == latdimid))) {
    return true;
  }
  else {
    return false;
  }
}

bool isRegularLatLonGridVariable(const netCDFStream::Variable& var,size_t timedimid,int levdimid,size_t latdimid,size_t londimid)
{
  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == latdimid && var.dimids[3] == londimid) || (var.dimids.size() == 3 && levdimid < 0 && var.dimids[1] == latdimid && var.dimids[2] == londimid))) {
    return true;
  }
  else {
    return false;
  }
}

bool isPolarStereographicGridVariable(const netCDFStream::Variable& var,size_t timedimid,int levdimid,size_t latdimid)
{
  size_t ltdimid=latdimid/10000-1;
  size_t lndimid=(latdimid % 10000)/100-1;

  if (var.dimids[0] == timedimid && ((var.dimids.size() == 4 && levdimid >= 0 && static_cast<int>(var.dimids[1]) == levdimid && var.dimids[2] == ltdimid && var.dimids[3] == lndimid) || (var.dimids.size() == 3 && levdimid < 0 && var.dimids[1] == ltdimid && var.dimids[2] == lndimid))) {
    return true;
  }
  else {
    return false;
  }
}

std::string getGriddedTimeMethod(const netCDFStream::Variable& var,std::string timeid)
{
  std::string cell_methods,time_method;
  size_t m;
  int idx;

  for (m=0; m < var.attrs.size(); ++m) {
    if (var.attrs[m].name == "cell_methods") {
	cell_methods=*(reinterpret_cast<std::string *>(var.attrs[m].values));
	auto re=std::regex("  ");
	while (std::regex_search(cell_methods,re)) {
	  strutils::replace_all(cell_methods,"  "," ");
	}
	strutils::replace_all(cell_methods,"comment: ","");
	strutils::replace_all(cell_methods,"comments: ","");
	strutils::replace_all(cell_methods,"comment:","");
	strutils::replace_all(cell_methods,"comments:","");
	if (cell_methods.length() > 0 && std::regex_search(cell_methods,std::regex(strutils::substitute(timeid,".","\\.")+":"))) {
	  idx=cell_methods.find(timeid+":");
	  if (idx != 0) {
	    cell_methods=cell_methods.substr(idx);
	  }
	  strutils::replace_all(cell_methods,timeid+":","");
	  strutils::trim(cell_methods);
	  idx=cell_methods.find(":");
	  if (idx < 0) {
	    return cell_methods;
	  }
	  else {
	    idx=cell_methods.find(")");
// no extra information in parentheses
	    if (idx < 0) {
		idx=cell_methods.find(" ");
		return cell_methods.substr(0,idx);
	    }
// found extra information
	    else {
		return cell_methods.substr(0,idx+1);
	    }
	  }
	}
    }
  }
  return "";
}

void addGridToInventory(std::string gentry_key)
{
  int idx=gentry_key.rfind("<!>");
  auto G_key=strutils::substitute(gentry_key.substr(0,idx),"<!>",",");
  if (G_map.find(G_key) == G_map.end()) {
    G_map.emplace(G_key,std::make_pair(G_map.size(),""));
  }
}

void addLevelToInventory(std::string lentryKey,std::string gentry_key,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,InputNetCDFStream& istream)
{
  if (L_map.find(lentryKey) == L_map.end()) {
    L_map.emplace(lentryKey,std::make_pair(L_map.size(),""));
  }
  auto idx=gentry_key.rfind("<!>");
  std::string sdum="|"+strutils::itos(U_map[gentry_key.substr(idx+3)].first)+"|"+strutils::itos(G_map[strutils::substitute(gentry_key.substr(0,idx),"<!>",",")].first)+"|"+strutils::itos(L_map[lentryKey].first);
  auto dims=istream.dimensions();
  auto vars=istream.variables();
  for (size_t n=0; n < vars.size(); ++n) {
    auto key="ds"+args.dsnum+":"+vars[n].name;
    if (vars[n].dimids.size() > 0 && !vars[n].is_coord && vars[n].dimids[0] == timedimid && P_map.find(key) != P_map.end() && ((vars[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(vars[n].dimids[1]) == levdimid && vars[n].dimids[2] == latdimid && vars[n].dimids[3] == londimid) || (vars[n].dimids.size() == 3 && levdimid < 0 && vars[n].dimids[1] == latdimid && vars[n].dimids[2] == londimid))) {
	auto R_key=strutils::itos(static_cast<int>(vars[n].nc_type));
	if (R_map.find(R_key) == R_map.end()) {
	  R_map.emplace(R_key,std::make_pair(R_map.size(),""));
	}
	auto var_size=vars[n].size;
	if (vars[n].dimids.size() == 4 && levdimid >= 0 && static_cast<int>(vars[n].dimids[1]) == levdimid) {
	  var_size/=dims[levdimid].length;
	}
	long long off=vars[n].offset;
	for (size_t m=0; m < time_s.num_times; ++m) {
	  std::string error;
	  inv_lines.emplace_back(strutils::lltos(off)+"|"+strutils::itos(var_size)+"|"+metautils::NcTime::getActualDateTime(time_s.times[m],time_data,error).toString("%Y%m%d%H%MM")+sdum+"|"+strutils::itos(P_map[key].first)+"|"+strutils::itos(R_map[R_key].first));
	  if (error.length() > 0) {
	    metautils::logError(error,"nc2xml",user,args.argsString);
	  }
	  if (off > static_cast<long long>(0xffffffff)) {
	    is_large_offset=true;
	  }
	  off+=istream.record_size();
	}
    }
  }
}

void addGriddedParametersToNetCDFLevelEntry(const std::vector<netCDFStream::Variable>& vars,std::string gentry_key,std::string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,const bool& foundMap,const metautils::NcTime::TimeRangeEntry& tre,my::map<metautils::StringEntry>& parameterTable,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable,ParameterMap& parameterMap)
{
  size_t n;
  std::string time_method,tr_description,error;
  DateTime firstValidDateTime,lastValidDateTime;

// find all of the parameters
  for (n=0; n < vars.size(); ++n) {
    if (!vars[n].is_coord) {
	time_method=getGriddedTimeMethod(vars[n],timeid);
	if (time_method.length() == 0 || (myequalf(time_bounds_s.t1,0,0.0001) && myequalf(time_bounds_s.t1,time_bounds_s.t2,0.0001))) {
	  firstValidDateTime=tre.instantaneous->first_valid_datetime;
	  lastValidDateTime=tre.instantaneous->last_valid_datetime;
	}
	else {
	  if (time_bounds_s.changed) {
	    metautils::logError("time bounds changed","nc2xml",user,args.argsString);
	  }
	  firstValidDateTime=tre.bounded->first_valid_datetime;
	  lastValidDateTime=tre.bounded->last_valid_datetime;
	}
	time_method=strutils::capitalize(time_method);
	tr_description=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,time_method,error);
	if (error.length() > 0) {
	  metautils::logError(error,"nc2xml",user,args.argsString);
	}
	tr_description=strutils::capitalize(tr_description);
	if (strutils::has_ending(gentry_key,tr_description)) {
// check as a zonal mean grid variable
	  if (std::regex_search(gentry_key,std::regex("^[12]<!>1<!>"))) {
	    if (isZonalMeanGridVariable(vars[n],timedimid,levdimid,latdimid)) {
		param_entry.key="ds"+args.dsnum+":"+vars[n].name;
		addGriddedNetCDFParameter(vars[n],foundMap,firstValidDateTime,lastValidDateTime,*tre.num_steps,parameterTable,varList,changedVarTable,parameterMap);
		if (inv.is_open()) {
		  addGridToInventory(gentry_key);
		}
	    }
	  }
	  else if (vars[n].dimids.size() == 3 || vars[n].dimids.size() == 4) {
	    if (isRegularLatLonGridVariable(vars[n],timedimid,levdimid,latdimid,londimid)) {
// check as a regular lat/lon grid variable
		param_entry.key="ds"+args.dsnum+":"+vars[n].name;
		addGriddedNetCDFParameter(vars[n],foundMap,firstValidDateTime,lastValidDateTime,*tre.num_steps,parameterTable,varList,changedVarTable,parameterMap);
		if (inv.is_open()) {
		  addGridToInventory(gentry_key);
		}
	    }
	    else if (isPolarStereographicGridVariable(vars[n],timedimid,levdimid,latdimid)) {
// check as a polar-stereographic grid variable
		param_entry.key="ds"+args.dsnum+":"+vars[n].name;
		addGriddedNetCDFParameter(vars[n],foundMap,firstValidDateTime,lastValidDateTime,*tre.num_steps,parameterTable,varList,changedVarTable,parameterMap);
		if (inv.is_open()) {
		  addGridToInventory(gentry_key);
		}
	    }
	  }
	}
    }
  }
}

void addGriddedTimeRange(std::string keyStart,std::list<std::string>& gentry_keys,std::string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,const metautils::NcTime::TimeRangeEntry& tre,std::vector<netCDFStream::Variable>& vars)
{
  std::string gentry_key,time_method,error;
  size_t n;
  bool foundVarWithNoTimeMethod=false;

  for (n=0; n < vars.size(); ++n) {
    if (!vars[n].is_coord && vars[n].dimids.size() >= 3 && (isZonalMeanGridVariable(vars[n],timedimid,levdimid,latdimid) || isRegularLatLonGridVariable(vars[n],timedimid,levdimid,latdimid,londimid) || isPolarStereographicGridVariable(vars[n],timedimid,levdimid,latdimid))) {
	time_method=getGriddedTimeMethod(vars[n],timeid);
	if (time_method.length() == 0) {
	  foundVarWithNoTimeMethod=true;
	}
	else {
	  time_method=strutils::capitalize(time_method);
	  gentry_key=keyStart+metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,time_method,error);
	  if (error.length() > 0) {
	    metautils::logError(error,"nc2xml",user,args.argsString);
	  }
	  gentry_keys.emplace_back(gentry_key);
	}
    }
  }
  if (foundVarWithNoTimeMethod) {
    gentry_key=keyStart+metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,"",error);
    if (error.length() > 0) {
	metautils::logError(error,"nc2xml",user,args.argsString);
    }
    gentry_keys.emplace_back(gentry_key);
  }
}

void addGriddedLatLonKeys(std::list<std::string>& gentry_keys,Grid::GridDimensions dim,Grid::GridDefinition def,std::string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,const metautils::NcTime::TimeRangeEntry& tre,std::vector<netCDFStream::Variable>& vars)
{
  std::string keyStart;
  switch (def.type) {
    case Grid::latitudeLongitudeType:
    case Grid::gaussianLatitudeLongitudeType:
    {
	keyStart=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.elatitude,3)+"<!>"+strutils::ftos(def.elongitude,3)+"<!>"+strutils::ftos(def.loincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
	addGriddedTimeRange(keyStart,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
	keyStart=strutils::itos(def.type)+"<!>1<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>0<!>"+strutils::ftos(def.elatitude,3)+"<!>360<!>"+strutils::ftos(def.laincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
	addGriddedTimeRange(keyStart,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
	break;
    }
    case Grid::polarStereographicType:
    {
	keyStart=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.llatitude,3)+"<!>"+strutils::ftos(def.olongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>";
	if (def.projection_flag == 0) {
	  keyStart+="N";
	}
	else {
	  keyStart+="S";
	}
	keyStart+="<!>";
	addGriddedTimeRange(keyStart,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
	break;
    }
    case Grid::mercatorType:
    {
	keyStart=strutils::itos(def.type)+"<!>"+strutils::itos(dim.x)+"<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>"+strutils::ftos(def.slongitude,3)+"<!>"+strutils::ftos(def.elatitude,3)+"<!>"+strutils::ftos(def.elongitude,3)+"<!>"+strutils::ftos(def.dx,3)+"<!>"+strutils::ftos(def.dy,3)+"<!>"+strutils::ftos(def.stdparallel1,3)+"<!>";
	addGriddedTimeRange(keyStart,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
	break;
    }
  }
}

void addGriddedZonalMeanKeys(std::list<std::string>& gentry_keys,Grid::GridDimensions dim,Grid::GridDefinition def,std::string timeid,size_t timedimid,int levdimid,size_t latdimid,size_t londimid,metautils::NcTime::TimeRangeEntry &tre,std::vector<netCDFStream::Variable>& vars)
{
  std::string keyStart;

  keyStart="1<!>1<!>"+strutils::itos(dim.y)+"<!>"+strutils::ftos(def.slatitude,3)+"<!>0<!>"+strutils::ftos(def.elatitude,3)+"<!>360<!>"+strutils::ftos(def.laincrement,3)+"<!>"+strutils::ftos(def.laincrement,3)+"<!>";
  addGriddedTimeRange(keyStart,gentry_keys,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
}

bool foundNetCDFTimeFromPatch(const netCDFStream::Variable& var)
{
  size_t m;
  long long dt;
  std::string sdum;
  std::deque<std::string> sp;
  int status;

// ds260.1
  status=0;
  for (m=0; m < var.attrs.size(); ++m) {
    if (var.attrs[m].nc_type == netCDFStream::NcType::CHAR) {
	if (var.attrs[m].name == "units") {
	  time_data.units=*(reinterpret_cast<std::string *>(var.attrs[m].values))+"s";
	  ++status;
	}
	else if (var.attrs[m].name == "comment") {
	  sdum=*(reinterpret_cast<std::string *>(var.attrs[m].values));
	  sp=strutils::split(sdum);
	  dt=std::stoll(sp[sp.size()-1])*10000000000+100000000+1000000;
	  time_data.reference.set(dt);
	  ++status;
	}
    }
    if (status == 2) {
	if (time_data.units == "hours") {
	  time_data.reference.subtractHours(1);
	}
	else if (time_data.units == "days") {
	  time_data.reference.subtractDays(1);
	}
	else if (time_data.units == "months") {
	  time_data.reference.subtractMonths(1);
	}
	else {
	  return false;
	}
	return true;
    }
  }
  return false;
}

bool ignoreCFVariable(const netCDFStream::Variable& var)
{
  std::string vname=strutils::to_lower(var.name);
  size_t n;

  if (vname == "time" || vname == "time_bounds" || vname == "year" || vname == "month" || vname == "day" || vname == "doy" || vname == "hour" || vname == "latitude" || vname == "longitude") {
    return true;
  }
  for (n=0; n < var.attrs.size(); ++n) {
    if (var.attrs[n].name == "cf_role") {
	return true;
    }
  }
  return false;
}

struct DiscreteGeometriesData {
  DiscreteGeometriesData() : indexes(),z_units(),z_pos() {}

  struct Indexes {
    Indexes() : time_var(0xffffffff),stn_id_var(0xffffffff),lat_var(0xffffffff),lon_var(0xffffffff),sample_dim_var(0xffffffff),instance_dim_var(0xffffffff),z_var(0xffffffff) {}

    size_t time_var,stn_id_var,lat_var,lon_var,sample_dim_var,instance_dim_var,z_var;
  };
  Indexes indexes;
  std::string z_units,z_pos;
};

void processUnitsAttribute(const netCDFStream::Attribute& attr,const netCDFStream::Variable& var,size_t var_index,DiscreteGeometriesData& dgd)
{
  auto u=*(reinterpret_cast<std::string *>(attr.values));
  u=strutils::to_lower(u);
  if (std::regex_search(u,std::regex("since"))) {
    if (dgd.indexes.time_var != 0xffffffff) {
	metautils::logError("processUnitsAttribute returned error: time was already identified - don't know what to do with variable: "+var.name,"nc2xml",user,args.argsString);
    }
    fillNcTimeData(attr);
    dgd.indexes.time_var=var_index;
  }
  else if (u == "degrees_north") {
    if (dgd.indexes.lat_var == 0xffffffff) {
	dgd.indexes.lat_var=var_index;
    }
  }
  else if (u == "degrees_east") {
    if (dgd.indexes.lon_var == 0xffffffff) {
	dgd.indexes.lon_var=var_index;
    }
  }
}

void scanCFPointNetCDFFile(InputNetCDFStream& istream,bool& foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  if (verbose_operation) {
    std::cout << "...beginning function scanPointNetCDFFile..." << std::endl;
  }
  initializeForObservations();
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR) {
	  if (vars[n].attrs[m].name == "units") {
	    processUnitsAttribute(vars[n].attrs[m],vars[n],n,dgd);
	  }
	}
    }
  }
  netCDFStream::VariableData times;
  if (dgd.indexes.time_var == 0xffffffff) {
    metautils::logError("scanCFPointNetCDFFile returned error: unable to determine time variable","nc2xml",user,args.argsString);
  }
  else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
	if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
	  time_data.calendar=*(reinterpret_cast<std::string *>(vars[dgd.indexes.time_var].attrs[n].values));
	}
    }
    if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFPointNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
    }
  }
  netCDFStream::VariableData lats;
  if (dgd.indexes.lat_var == 0xffffffff) {
    metautils::logError("scanCFPointNetCDFFile returned error: unable to determine latitude variable","nc2xml",user,args.argsString);
  }
  else {
    if (istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFPointNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
    }
  }
  netCDFStream::VariableData lons;
  if (dgd.indexes.lon_var == 0xffffffff) {
    metautils::logError("scanCFPointNetCDFFile returned error: unable to determine longitude variable","nc2xml",user,args.argsString);
  }
  else {
    if (istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFPointNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
    }
  }
  netCDFStream::DataValue time_miss_val;
  std::vector<DateTime> date_times;
  std::vector<std::string> IDs;
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.time_var].name && var.name != vars[dgd.indexes.lat_var].name && var.name != vars[dgd.indexes.lon_var].name) {
	de.key=var.name;
	netCDFStream::VariableData var_data;
	if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanCFPointNetCDFFile returned error: unable to get variable data for '"+var.name+"'","nc2xml",user,args.argsString);
	}
	std::string long_name,units;
	netCDFStream::DataValue miss_val;
	extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	for (size_t n=0; n < times.size(); ++n) {
	  if (n == date_times.size()) {
	    date_times.emplace_back(computeNcTime(times,n));
	  }
	  if (n == IDs.size()) {
	    auto lat=strutils::ftos(fabs(lats[n]),4);
	    if (lats[n] < 0.) {
		lat+="S";
	    }
	    else {
		lat+="N";
	    }
	    auto lon=strutils::ftos(fabs(lons[n]),4);
	    if (lons[n] < 0.) {
		lon+="W";
	    }
	    else {
		lon+="E";
	    }
	    IDs.emplace_back(lat+lon);
	  }
	  if (!found_missing(times[n],time_miss_val,var_data[n],miss_val)) {
	    pentry.key="unknown";
	    updatePlatformTable(1,lats[n],lons[n]);
	    ientry.key=pentry.key+"[!]latlon[!]"+IDs[n];
	    updateIDTable(1,lats[n],lons[n],date_times[n],times[n]);
	    ++total_num_not_missing;
	  }
	}
    }
  }
  write_type=ObML_type;
  if (verbose_operation) {
    std::cout << "...function scanCFPointNetCDFFile done." << std::endl;
  }
}

void scanCFOrthogonalTimeSeriesNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,std::unordered_map<size_t,std::string>& T_map)
{
  metadata::openInventory(inv,&inv_file,"nc2xml",user);
  if (inv.is_open()) {
    inv << "netCDF:timeSeries|" << istream.record_size() << std::endl;
    O_map.emplace("surface",std::make_pair(O_map.size(),""));
  }
  netCDFStream::VariableData lats,lons,ids;
  netCDFStream::NcType ids_type=netCDFStream::NcType::_NULL;
  size_t id_len=0;
  std::vector<std::string> platform_types,id_types,id_cache;
  if (dgd.indexes.lat_var == 0xffffffff || dgd.indexes.lon_var == 0xffffffff || dgd.indexes.stn_id_var == 0xffffffff) {
// lat/lon not found, look for known alternates in global attributes
    auto gattrs=istream.global_attributes();
    size_t known_sources;
    for (size_t n=0; n < gattrs.size(); ++n) {
	if (strutils::to_lower(gattrs[n].name) == "title") {
	  if (strutils::to_lower(*(reinterpret_cast<std::string *>(gattrs[n].values))) == "hadisd") {
	    known_sources=0x1;
	    break;
	  }
	}
    }
    if (known_sources == 0x1) {
// HadISD
	for (size_t n=0; n < gattrs.size(); ++n) {
	  if (gattrs[n].name == "latitude") {
	    lats.resize(1,netCDFStream::NcType::FLOAT);
	    lats.set(0,*(reinterpret_cast<float *>(gattrs[n].values)));
	  }
	  else if (gattrs[n].name == "longitude") {
	    lons.resize(1,netCDFStream::NcType::FLOAT);
	    lons.set(0,*(reinterpret_cast<float *>(gattrs[n].values)));
	  }
	  else if (gattrs[n].name == "station_id") {
	    auto id=*(reinterpret_cast<std::string *>(gattrs[n].values));
	    id=id.substr(0,id.find("-"));
	    id_cache.emplace_back(id);
	    id_len=id.length();
	    ids_type=netCDFStream::NcType::CHAR;
	    ids.resize(id_len,ids_type);
	    for (size_t m=0; m < id_len; ++m) {
		ids.set(m,id[m]);
	    }
	    id_types.emplace_back("WMO+6");
	    if (id >= "990000" && id < "991000") {
		platform_types.emplace_back("fixed_ship");
	    }
	    else if ((id >= "992000" && id < "993000") || (id >= "995000" && id < "998000")) {
		platform_types.emplace_back("drifting_buoy");
	    }
	    else {
		platform_types.emplace_back("land_station");
	    }
	  }
	}
	pentry.key=platform_types.back();
	updatePlatformTable(1,lats[0],lons[0]);
	if (inv.is_open()) {
	  auto I_key=id_types.back()+"[!]"+id_cache.back();
	  if (I_map.find(I_key) == I_map.end()) {
		I_map.emplace(I_key,std::make_pair(I_map.size(),strutils::ftos(lats.back(),4)+"[!]"+strutils::ftos(lons.back(),4)));
	  }
	}
    }
  }
  auto vars=istream.variables();
  netCDFStream::VariableData times;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  if (lats.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  if (lons.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  if (ids.type() == netCDFStream::NcType::_NULL && (ids_type=istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids)) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  if (ids_type == netCDFStream::NcType::CHAR && dgd.indexes.stn_id_var != 0xffffffff) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  auto stn_dim=-1;
  size_t num_stns=1;
  if (dgd.indexes.stn_id_var != 0xffffffff && vars[dgd.indexes.stn_id_var].dimids.size() > 1) {
    stn_dim=vars[dgd.indexes.stn_id_var].dimids[0];
    if (vars[dgd.indexes.stn_id_var].is_rec) {
	num_stns=istream.num_records();
    }
    else {
	num_stns=dims[stn_dim].length;
    }
  }
  if (platform_types.size() == 0) {
    std::string id;
    for (size_t n=0; n < num_stns; ++n) {
	if (ids_type == netCDFStream::NcType::INT || ids_type == netCDFStream::NcType::FLOAT || ids_type == netCDFStream::NcType::DOUBLE) {
	  id=strutils::ftos(ids[n]);
	}
	else if (ids_type == netCDFStream::NcType::CHAR) {
	  id.assign(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
	}
	else {
	  metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to determine platform type","nc2xml",user,args.argsString);
	}
	platform_types.emplace_back("unknown");
	id_types.emplace_back("unknown");
	id_cache.emplace_back(id);
	pentry.key=platform_types[n];
	updatePlatformTable(1,lats[n],lons[n]);
	if (inv.is_open()) {
	  auto I_key=id_types[n]+"[!]"+id_cache[n];
	  if (I_map.find(I_key) == I_map.end()) {
	    I_map.emplace(I_key,std::make_pair(I_map.size(),strutils::ftos(lats[n],4)+"[!]"+strutils::ftos(lons[n],4)));
	  }
	}
    }
  }
  if (inv.is_open()) {
    for (const auto& plat : platform_types) {
	if (P_map.find(plat) == P_map.end()) {
	  P_map.emplace(plat,std::make_pair(P_map.size(),""));
	}
    }
  }
  std::vector<DateTime> dts;
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() > 0 && ((var.dimids[0] == vars[dgd.indexes.time_var].dimids[0] && (stn_dim == -1 || (var.dimids.size() > 1 && static_cast<int>(var.dimids[1]) == stn_dim))) || (var.dimids.size() > 1 && dgd.indexes.stn_id_var != 0xffffffff && var.dimids[0] == vars[dgd.indexes.stn_id_var].dimids[0] && var.dimids[1] == vars[dgd.indexes.time_var].dimids[0]))) {
	de.key=var.name;
	if (inv.is_open()) {
	  if (D_map.find(var.name) == D_map.end()) {
	    auto bsize=1;
	    for (size_t l=1; l < var.dimids.size(); ++l) {
	      bsize*=dims[var.dimids[l]].length;
	    }
	    switch (var.nc_type) {
		case netCDFStream::NcType::SHORT:
		{
		  bsize*=2;
		  break;
		}
		case netCDFStream::NcType::INT:
		case netCDFStream::NcType::FLOAT:
		{
		  bsize*=4;
		  break;
		}
		case netCDFStream::NcType::DOUBLE:
		{
		  bsize*=8;
		  break;
		}
		default: {}
	    }
	    D_map.emplace(var.name,std::make_pair(D_map.size(),"|"+strutils::lltos(var.offset)+"|"+netCDFStream::nc_type[static_cast<int>(var.nc_type)]+"|"+strutils::itos(bsize)));
	  }
	}
	netCDFStream::VariableData var_data;
	if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanCFOrthogonalTimeSeriesNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	}
	std::string long_name,units;
	netCDFStream::DataValue miss_val;
	extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	size_t num_times;
	bool time_is_unlimited;
	if (dims[vars[dgd.indexes.time_var].dimids[0]].is_rec) {
	  num_times=istream.num_records();
	  time_is_unlimited=true;
	}
	else {
	  num_times=dims[vars[dgd.indexes.time_var].dimids[0]].length;
	  time_is_unlimited=false;
	}
	for (size_t n=0; n < num_stns; ++n) {
	  std::vector<std::string> miss_lines_list;
	  ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]"+id_cache[n];
	  for (size_t m=0; m < num_times; ++m) {
	    if (dts.size() != num_times) {
		dts.emplace_back(computeNcTime(times,m));
	    }
	    netCDFStream::DataValue time_miss_val;
	    auto vidx= (time_is_unlimited) ? n+m*num_stns : n*num_times+m;
	    if (!found_missing(times[m],time_miss_val,var_data[vidx],miss_val)) {
		if (inv.is_open()) {
		  if (T_map.find(m) == T_map.end()) {
		    T_map.emplace(m,dts[m].toString("%Y%m%d%H%MM"));
		  }
		}
		updateIDTable(1,lats[n],lons[n],dts[m],times[m]);
		++total_num_not_missing;
	    }
	    else {
		if (inv.is_open()) {
		  std::string miss_line=strutils::itos(m);
		  miss_line+="|0|"+strutils::itos(P_map[platform_types[n]].first)+"|"+strutils::itos(I_map[id_types[n]+"[!]"+id_cache[n]].first)+"|"+strutils::itos(D_map[var.name].first);
		  miss_lines_list.emplace_back(miss_line);
		}
	    }
	  }
	  if (inv.is_open()) {
	    if (miss_lines_list.size() != times.size()) {
		for (const auto& line : miss_lines_list) {
		  inv_lines2.writeln(line);
		}
	    }
	    else {
		D_map.erase(var.name);
	    }
	  }
	}
    }
  }
}

void scanCFNonOrthogonalTimeSeriesNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,std::unordered_map<size_t,std::string>& T_map)
{
  size_t id_len=0;
  auto vars=istream.variables();
  netCDFStream::VariableData times;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  netCDFStream::VariableData lats;
  if (istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  netCDFStream::VariableData lons;
  if (istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  netCDFStream::VariableData ids;
  if (istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  auto stn_dim=vars[dgd.indexes.stn_id_var].dimids[0];
  size_t num_stns;
  if (vars[dgd.indexes.stn_id_var].is_rec) {
    num_stns=istream.num_records();
  }
  else {
    num_stns=dims[stn_dim].length;
  }
  std::vector<std::string> platform_types,id_types;
  if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
    for (size_t n=0; n < num_stns; ++n) {
//	int id=ids[n];
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(1,lats[n],lons[n]);
    }
  }
  else if (ids.type() == netCDFStream::NcType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < num_stns; ++n) {
	std::string id(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(1,lats[n],lons[n]);
    }
  }
  else {
    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to determine platform type","nc2xml",user,args.argsString);
  }
  auto obs_dim= (vars[dgd.indexes.time_var].dimids[0] == stn_dim) ? vars[dgd.indexes.time_var].dimids[1] : vars[dgd.indexes.time_var].dimids[0];
  if (dgd.indexes.sample_dim_var != 0xffffffff) {
// continuous ragged array H.6
    netCDFStream::VariableData row_sizes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get sample dimension data","nc2xml",user,args.argsString);
    }
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() == 1 && var.dimids[0] == obs_dim) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  std::string long_name,units;
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  long long offset=0;
	  for (size_t n=0; n < dims[stn_dim].length; ++n) {
	    auto end=offset+row_sizes[n];
	    ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
	    if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		ientry.key+=strutils::ftos(ids[n]);
	    }
	    else if (ids.type() == netCDFStream::NcType::CHAR) {
		ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
	    }
	    for (size_t m=offset; m < end; ++m) {
		netCDFStream::DataValue time_miss_val;
		if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
		  auto dt=computeNcTime(times,m);
		  updateIDTable(1,lats[n],lons[n],dt,times[m]);
		  ++total_num_not_missing;
		}
	    }
	    offset=end;
	  }
	}
    }
  }
  else if (dgd.indexes.instance_dim_var != 0xffffffff) {
// indexed ragged array H.7
    netCDFStream::VariableData station_indexes;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,station_indexes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get instance dimension data","nc2xml",user,args.argsString);
    }
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.time_var].name && var.name != vars[dgd.indexes.instance_dim_var].name && var.dimids.size() == 1 && var.dimids[0] == obs_dim) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  std::string long_name,units;
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  for (size_t n=0; n < station_indexes.size(); ++n) {
	    size_t idx=station_indexes[n];
	    ientry.key=platform_types[idx]+"[!]"+id_types[idx]+"[!]";
	    if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		ientry.key+=strutils::ftos(ids[idx]);
	    }
	    else if (ids.type() == netCDFStream::NcType::CHAR) {
		ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[idx*id_len],id_len);
	    }
	    netCDFStream::DataValue time_miss_val;
	    if (!found_missing(times[n],time_miss_val,var_data[n],miss_val)) {
		auto dt=computeNcTime(times,n);
		updateIDTable(1,lats[idx],lons[idx],dt,times[n]);
		++total_num_not_missing;
	    }
	  }
	}
    }
  }
  else {
// incomplete multidimensional array H.3
    std::string long_name,units;
    netCDFStream::DataValue time_miss_val;
    extractFromVariableAttribute(vars[dgd.indexes.time_var].attrs,long_name,units,vars[dgd.indexes.time_var].nc_type,time_miss_val);
    size_t num_obs;
    if (vars[dgd.indexes.stn_id_var].is_rec) {
	num_obs=dims[obs_dim].length;
    }
    else {
	num_obs=istream.num_records();
    }
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.time_var].name && var.dimids.size() == 2 && ((var.dimids[0] == stn_dim && var.dimids[1] == obs_dim) || (var.dimids[0] == obs_dim && var.dimids[1] == stn_dim))) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalTimeSeriesNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  if (var.dimids.front() == stn_dim) {
	    for (size_t n=0; n < num_stns; ++n) {
		ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
		if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		  ientry.key+=strutils::ftos(ids[n]);
		}
		else if (ids.type() == netCDFStream::NcType::CHAR) {
		  ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
		}
		for (size_t m=0; m < num_obs; ++m) {
		  auto idx=n*num_obs+m;
		  if (!found_missing(times[idx],time_miss_val,var_data[idx],miss_val)) {
		    auto dt=computeNcTime(times,idx);
		    updateIDTable(1,lats[n],lons[n],dt,times[idx]);
		    ++total_num_not_missing;
		  }
		}
	    }
	  }
	  else {
	    for (size_t n=0; n < num_obs; ++n) {
		for (size_t m=0; m < num_stns; ++m) {
		  ientry.key=platform_types[m]+"[!]"+id_types[m]+"[!]";
		  if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		    ientry.key+=strutils::ftos(ids[m]);
		  }
		  else if (ids.type() == netCDFStream::NcType::CHAR) {
		    ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[m*id_len],id_len);
		  }
		  auto idx=n*num_stns+m;
		  if (!found_missing(times[idx],time_miss_val,var_data[idx],miss_val)) {
		    auto dt=computeNcTime(times,idx);
		    updateIDTable(1,lats[m],lons[m],dt,times[idx]);
		    ++total_num_not_missing;
		  }
		}
	    }
	  }
	}
    }
  }
}

void scanCFTimeSeriesNetCDFFile(InputNetCDFStream& istream,bool& foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  if (verbose_operation) {
    std::cout << "...beginning function scanCFTimeSeriesNetCDFFile..." << std::endl;
  }
  initializeForObservations();
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR) {
	  if (vars[n].attrs[m].name == "units") {
	    processUnitsAttribute(vars[n].attrs[m],vars[n],n,dgd);
	  }
	  else if (vars[n].attrs[m].name == "cf_role") {
	    auto r=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    r=strutils::to_lower(r);
	    if (r == "timeseries_id") {
		if (dgd.indexes.stn_id_var != 0xffffffff) {
		  metautils::logError("scanCFTimeSeriesNetCDFFile returned error: station ID was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
		}
		dgd.indexes.stn_id_var=n;
	    }
	  }
	  else if (vars[n].attrs[m].name == "sample_dimension") {
	    dgd.indexes.sample_dim_var=n;
	  }
	  else if (vars[n].attrs[m].name == "instance_dimension") {
	    dgd.indexes.instance_dim_var=n;
	  }
	}
    }
  }
  if (dgd.indexes.time_var == 0xffffffff) {
    metautils::logError("scanCFTimeSeriesNetCDFFile returned error: unable to determine time variable","nc2xml",user,args.argsString);
  }
  else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
	if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
	  time_data.calendar=*(reinterpret_cast<std::string *>(vars[dgd.indexes.time_var].attrs[n].values));
	}
    }
  }
  std::unordered_map<size_t,std::string> T_map;
  if (vars[dgd.indexes.time_var].is_coord) {
// ex. H.2, H.4 (single version of H.2), H.5 (precise locations) stns w/same times
    scanCFOrthogonalTimeSeriesNetCDFFile(istream,dgd,T_map);
  }
  else {
// ex. H.3 stns w/varying times but same # of obs
// ex. H.6 w/sample_dimension
// ex. H.7 w/instance_dimension
    if (dgd.indexes.stn_id_var == 0xffffffff) {
	metautils::logError("scanCFTimeSeriesNetCDFFile returned error: unable to determine timeseries_id variable","nc2xml",user,args.argsString);
    }
    if (dgd.indexes.lat_var == 0xffffffff) {
	metautils::logError("scanCFTimeSeriesNetCDFFile returned error: unable to determine latitude variable","nc2xml",user,args.argsString);
    }
    if (dgd.indexes.lon_var == 0xffffffff) {
	metautils::logError("scanCFTimeSeriesNetCDFFile returned error: unable to determine longitude variable","nc2xml",user,args.argsString);
    }
    scanCFNonOrthogonalTimeSeriesNetCDFFile(istream,dgd,T_map);
  }
  write_type=ObML_type;
  if (inv.is_open()) {
    std::vector<size_t> time_indexes;
    for (const auto& e : T_map) {
	time_indexes.emplace_back(e.first);
    }
    std::sort(time_indexes.begin(),time_indexes.end(),
    [](const size_t& left,const size_t& right) -> bool
    {
	if (left <= right) {
	  return true;
	}
	else {
	  return false;
	}
    });
    for (const auto& idx : time_indexes) {
	inv << "T<!>" << idx << "<!>" << T_map[idx] << std::endl;
    }
  }
  if (verbose_operation) {
    std::cout << "...function scanCFTimeSeriesNetCDFFile done." << std::endl;
  }
}

void scanCFOrthogonalProfileNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,size_t obs_type_index)
{
  size_t id_len=1;
  std::string platform_type="unknown";
  std::string id_type="unknown";
  netCDFStream::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  if (lats.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  if (lons.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  if (ids.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: unable to get level data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  if (ids.type() == netCDFStream::NcType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
  }
  if (times.size() != lats.size() || lats.size() != lons.size() || lons.size() != ids.size()/id_len) {
    metautils::logError("scanCFOrthogonalProfileNetCDFFile returned error: profile data does not follow the CF conventions","nc2xml",user,args.argsString);
  }
  std::string long_name,units;
  netCDFStream::DataValue time_miss_val;
  extractFromVariableAttribute(vars[dgd.indexes.time_var].attrs,long_name,units,vars[dgd.indexes.time_var].nc_type,time_miss_val);
  for (const auto& var : vars) {
    if (var.name != vars[dgd.indexes.z_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
	de.key=var.name;
	netCDFStream::VariableData var_data;
	if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	}
	netCDFStream::DataValue miss_val;
	extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	for (size_t n=0; n < times.size(); ++n) {
	  auto nlvls=0,last_level=-1;
	  auto avg_vres=0.;
	  auto min=1.e38,max=-1.e38;
	  for (size_t m=0; m < levels.size(); ++m) {
	    if (!found_missing(times[n],time_miss_val,var_data[n*levels.size()+m],miss_val)) {
		if (levels[m] < min) {
		  min=levels[m];
		}
		if (levels[m] > max) {
		  max=levels[m];
		}
		if (last_level >= 0) {
		  avg_vres+=fabsf(levels[m]-levels[last_level]);
		}
		++nlvls;
		last_level=m;
	    }
	  }
	  if (nlvls > 0) {
	    pentry.key=platform_type;
	    updatePlatformTable(obs_type_index,lats[n],lons[n]);
	    auto dt=computeNcTime(times,n);
	    ientry.key=platform_type+"[!]"+id_type+"[!]";
	    if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		ientry.key+=strutils::ftos(ids[n]);
	    }
	    else if (ids.type() == netCDFStream::NcType::CHAR) {
		ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
	    }
	    updateIDTable(obs_type_index,lats[n],lons[n],dt,times[n]);
	    if (de.data->vdata == nullptr) {
		de.data->vdata.reset(new metadata::ObML::DataTypeEntry::Data::VerticalData);
	    }
	    if (dgd.z_pos == "down") {
		if (de.data->vdata->min_altitude > 1.e37) {
		  de.data->vdata->min_altitude=-de.data->vdata->min_altitude;
		}
		if (de.data->vdata->max_altitude < -1.e37) {
		  de.data->vdata->max_altitude=-de.data->vdata->max_altitude;
		}
		if (max > de.data->vdata->min_altitude) {
		  de.data->vdata->min_altitude=max;
		}
		if (min < de.data->vdata->max_altitude) {
		  de.data->vdata->max_altitude=min;
		}
	    }
	    else {
		if (min < de.data->vdata->min_altitude) {
		  de.data->vdata->min_altitude=min;
		}
		if (max > de.data->vdata->max_altitude) {
		  de.data->vdata->max_altitude=max;
		}
	    }
	    de.data->vdata->units=dgd.z_units;
	    de.data->vdata->avg_nlev+=nlvls;
	    de.data->vdata->avg_res+=(avg_vres/(nlvls-1));
	    ++de.data->vdata->res_cnt;
	    ++total_num_not_missing;
	  }
	}
    }
  }
}

bool compareZDown(const double& left,const double& right)
{
  if (left >= right) {
    return true;
  }
  else {
    return false;
  }
}

bool compareZUp(const double& left,const double& right)
{
  if (left <= right) {
    return true;
  }
  else {
    return false;
  }
}

void getVerticalResolutionData(std::vector<double>& lvls,std::string z_pos,std::string z_units)
{
  auto min=1.e38,max=-1.e38;
  for (size_t n=0; n < lvls.size(); ++n) {
    if (lvls[n] < min) {
	min=lvls[n];
    }
    if (lvls[n] > max) {
	max=lvls[n];
    }
  }
  if (de.data->vdata == nullptr) {
    de.data->vdata.reset(new metadata::ObML::DataTypeEntry::Data::VerticalData);
  }
  if (z_pos == "down") {
    std::sort(lvls.begin(),lvls.end(),compareZDown);
    if (de.data->vdata->min_altitude > 1.e37) {
	de.data->vdata->min_altitude=-de.data->vdata->min_altitude;
    }
    if (de.data->vdata->max_altitude < -1.e37) {
	de.data->vdata->max_altitude=-de.data->vdata->max_altitude;
    }
    if (max > de.data->vdata->min_altitude) {
	de.data->vdata->min_altitude=max;
    }
    if (min < de.data->vdata->max_altitude) {
	de.data->vdata->max_altitude=min;
    }
  }
  else {
    std::sort(lvls.begin(),lvls.end(),compareZUp);
    if (min < de.data->vdata->min_altitude) {
	de.data->vdata->min_altitude=min;
    }
    if (max > de.data->vdata->max_altitude) {
	de.data->vdata->max_altitude=max;
    }
  }
  de.data->vdata->units=z_units;
  de.data->vdata->avg_nlev+=lvls.size();
  auto avg_vres=0.;
  for (size_t n=1; n < lvls.size(); ++n) {
    avg_vres+=fabs(lvls[n]-lvls[n-1]);
  }
  de.data->vdata->avg_res+=(avg_vres/(lvls.size()-1));
  ++de.data->vdata->res_cnt;
}

void scanCFNonOrthogonalProfileNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,size_t obs_type_index)
{
  size_t id_len=1;
  auto vars=istream.variables();
  netCDFStream::VariableData times,lats,lons,ids,levels;
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  if (lats.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  if (lons.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  if (ids.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  std::vector<std::string> platform_types,id_types;
  if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
    for (size_t n=0; n < times.size(); ++n) {
//	int id=ids[n];
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else if (ids.type() == netCDFStream::NcType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < times.size(); ++n) {
	std::string id(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to determine platform type","nc2xml",user,args.argsString);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get level data","nc2xml",user,args.argsString);
  }
  if (times.size() != lats.size() || lats.size() != lons.size() || lons.size() != ids.size()/id_len) {
    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: profile data does not follow the CF conventions","nc2xml",user,args.argsString);
  }
  std::string long_name,units;
  netCDFStream::DataValue time_miss_val;
  extractFromVariableAttribute(vars[dgd.indexes.time_var].attrs,long_name,units,vars[dgd.indexes.time_var].nc_type,time_miss_val);
  if (dgd.indexes.sample_dim_var != 0xffffffff) {
// continuous ragged array H.10
    netCDFStream::VariableData row_sizes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get level data","nc2xml",user,args.argsString);
    }
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.z_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  auto off=0;
	  for (size_t n=0; n < times.size(); ++n) {
	    std::vector<double> lvls;
	    for (size_t m=0; m < row_sizes[n]; ++m) {
		if (!found_missing(times[n],time_miss_val,var_data[off],miss_val)) {
		  lvls.emplace_back(levels[off]);
		}
		++off;
	    }
	    if (lvls.size() > 0) {
		auto dt=computeNcTime(times,n);
		ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
		if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		  ientry.key+=strutils::ftos(ids[n]);
		}
		else if (ids.type() == netCDFStream::NcType::CHAR) {
		  ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
		}
		updateIDTable(obs_type_index,lats[n],lons[n],dt,times[n]);
		getVerticalResolutionData(lvls,dgd.z_pos,dgd.z_units);
		++total_num_not_missing;
	    }
	  }
	}
    }
  }
  else if (dgd.indexes.instance_dim_var != 0xffffffff) {
// indexed ragged array H.11
    netCDFStream::VariableData profile_indexes;
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,profile_indexes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get instance dimension data","nc2xml",user,args.argsString);
    }
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.z_var].name && var.name != vars[dgd.indexes.instance_dim_var].name && var.dimids.size() > 0 && var.dimids.back() == vars[dgd.indexes.z_var].dimids.front()) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  for (size_t n=0; n < times.size(); ++n) {
	    std::vector<double> lvls;
	    for (size_t m=0; m < profile_indexes.size(); ++m) {
		if (profile_indexes[m] == n && !found_missing(times[n],time_miss_val,var_data[m],miss_val)) {
		  lvls.emplace_back(levels[m]);
		}
	    }
	    if (lvls.size() > 0) {
		auto dt=computeNcTime(times,n);
		ientry.key=platform_types[n]+"[!]"+id_types[n]+"[!]";
		if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
		  ientry.key+=strutils::ftos(ids[n]);
		}
		else if (ids.type() == netCDFStream::NcType::CHAR) {
		  ientry.key+=std::string(&(reinterpret_cast<char *>(ids.get()))[n*id_len],id_len);
		}
		updateIDTable(obs_type_index,lats[n],lons[n],dt,times[n]);
		getVerticalResolutionData(lvls,dgd.z_pos,dgd.z_units);
		++total_num_not_missing;
	    }
	  }
	}
    }
  }
}

void processVerticalCoordinateVariable(const std::vector<netCDFStream::Attribute>& attrs,DiscreteGeometriesData& dgd,size_t& obs_type_index)
{
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "units") {
	dgd.z_units=*(reinterpret_cast<std::string *>(attrs[n].values));
	strutils::trim(dgd.z_units);
    }
    else if (attrs[n].name == "positive") {
	dgd.z_pos=*(reinterpret_cast<std::string *>(attrs[n].values));
	strutils::trim(dgd.z_pos);
	dgd.z_pos=strutils::to_lower(dgd.z_pos);
    }
  }
  if (dgd.z_pos.empty() && !dgd.z_units.empty()) {
    auto z_units_l=strutils::to_lower(dgd.z_units);
    if (std::regex_search(dgd.z_units,std::regex("Pa$")) || std::regex_search(z_units_l,std::regex("^mb(ar){0,1}$")) || z_units_l == "millibars") {
	dgd.z_pos="down";
	obs_type_index=0;
    }
  }
  if (dgd.z_pos.empty()) {
    metautils::logError("processVerticalCoordinateVariable returned error: unable to determine vertical coordinate direction","nc2xml",user,args.argsString);
  }
  else if (obs_type_index == 0xffffffff) {
    if (dgd.z_pos == "up") {
	obs_type_index=0;
    }
    else if (dgd.z_pos == "down") {
	auto z_units_l=strutils::to_lower(dgd.z_units);
	if (dgd.z_pos == "down" && (std::regex_search(dgd.z_units,std::regex("Pa$")) || std::regex_search(z_units_l,std::regex("^mb(ar){0,1}$")) || z_units_l == "millibars")) {
	  obs_type_index=0;
	}
    }
  }
}

void scanCFProfileNetCDFFile(InputNetCDFStream& istream,bool& foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  if (verbose_operation) {
    std::cout << "...beginning function scanCFProfileNetCDFFile..." << std::endl;
  }
  initializeForObservations();
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR) {
	  if (vars[n].attrs[m].name == "units") {
	    processUnitsAttribute(vars[n].attrs[m],vars[n],n,dgd);
	  }
	  else if (vars[n].attrs[m].name == "cf_role") {
	    auto r=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    r=strutils::to_lower(r);
	    if (r == "profile_id") {
		if (dgd.indexes.stn_id_var != 0xffffffff) {
		  metautils::logError("scanCFProfileNetCDFFile returned error: station ID was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
		}
		dgd.indexes.stn_id_var=n;
	    }
	  }
	  else if (vars[n].attrs[m].name == "sample_dimension") {
	    dgd.indexes.sample_dim_var=n;
	  }
	  else if (vars[n].attrs[m].name == "instance_dimension") {
	    dgd.indexes.instance_dim_var=n;
	  }
	  else if (vars[n].attrs[m].name == "axis") {
	    auto axis=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    if (axis == "Z") {
		dgd.indexes.z_var=n;
	    }
	  }
	}
    }
  }
  if (dgd.indexes.time_var == 0xffffffff) {
    metautils::logError("scanCFProfileNetCDFFile returned error: unable to determine time variable","nc2xml",user,args.argsString);
  }
  else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
	if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
	  time_data.calendar=*(reinterpret_cast<std::string *>(vars[dgd.indexes.time_var].attrs[n].values));
	}
    }
  }
  size_t obs_type_index=0xffffffff;
  if (dgd.indexes.z_var == 0xffffffff) {
    metautils::logError("scanCFProfileNetCDFFile returned error: unable to determine vertical coordinate variable","nc2xml",user,args.argsString);
  }
  else {
    processVerticalCoordinateVariable(vars[dgd.indexes.z_var].attrs,dgd,obs_type_index);
  }
  if (obs_type_index == 0xffffffff) {
    metautils::logError("scanCFProfileNetCDFFile returned error: unable to determine observation type","nc2xml",user,args.argsString);
  }
  if (dgd.indexes.sample_dim_var != 0xffffffff || dgd.indexes.instance_dim_var != 0xffffffff) {
// ex. H.10, H.11
    scanCFNonOrthogonalProfileNetCDFFile(istream,dgd,obs_type_index);
  }
  else {
// ex. H.8, H.9
    scanCFOrthogonalProfileNetCDFFile(istream,dgd,obs_type_index);
  }
  write_type=ObML_type;
  if (verbose_operation) {
    std::cout << "...function scanCFProfileNetCDFFile done." << std::endl;
  }
}

void scanCFOrthogonalTimeSeriesProfileNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,size_t obs_type_index)
{
  size_t id_len=1;
  std::string platform_type="unknown";
  std::string id_type="unknown";
  netCDFStream::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  if (lats.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  if (lons.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  if (ids.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  std::vector<std::string> platform_types,id_types,id_cache;
  if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
    for (size_t n=0; n < ids.size(); ++n) {
	id_cache.emplace_back(strutils::ftos(ids[n]));
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else if (ids.type() == netCDFStream::NcType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < ids.size()/id_len; ++n) {
	id_cache.emplace_back(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to determine platform type","nc2xml",user,args.argsString);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get level data","nc2xml",user,args.argsString);
  }
  netCDFStream::DataValue time_miss_val;
  for (const auto& var : vars) {
    if (var.dimids.size() == 3 && var.dimids[0] == vars[dgd.indexes.time_var].dimids[0] && var.dimids[1] == vars[dgd.indexes.z_var].dimids[0] && var.dimids[2] == vars[dgd.indexes.stn_id_var].dimids[0]) {
	de.key=var.name;
	netCDFStream::VariableData var_data;
	if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	}
	std::string long_name,units;
	netCDFStream::DataValue miss_val;
	extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	for (size_t n=0; n < times.size(); ++n) {
	  auto dt=computeNcTime(times,n);
	  size_t num_stns=ids.size()/id_len;
	  auto voff=n*(levels.size()*num_stns);
	  for (size_t m=0; m < num_stns; ++m) {
	    std::vector<double> lvls;
	    auto vidx=voff+m;
	    for (size_t l=0; l < levels.size(); ++l) {
		if (!found_missing(times[n],time_miss_val,var_data[vidx],miss_val)) {
		  lvls.emplace_back(levels[l]);
		}
		vidx+=num_stns;
	    }
	    if (lvls.size() > 0) {
		ientry.key=platform_types[m]+"[!]"+id_types[m]+"[!]"+id_cache[m];
		updateIDTable(obs_type_index,lats[m],lons[m],dt,times[n]);
		getVerticalResolutionData(lvls,dgd.z_pos,dgd.z_units);
		++total_num_not_missing;
	    }
	  }
	}
    }
  }
}

void scanCFNonOrthogonalTimeSeriesProfileNetCDFFile(InputNetCDFStream& istream,DiscreteGeometriesData& dgd,size_t obs_type_index)
{
  size_t id_len=1;
  std::string platform_type="unknown";
  std::string id_type="unknown";
  netCDFStream::VariableData times,lats,lons,ids,levels;
  auto vars=istream.variables();
  if (istream.variable_data(vars[dgd.indexes.time_var].name,times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get time data","nc2xml",user,args.argsString);
  }
  if (lats.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lat_var].name,lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get latitude data","nc2xml",user,args.argsString);
  }
  if (lons.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.lon_var].name,lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get longitude data","nc2xml",user,args.argsString);
  }
  if (ids.type() == netCDFStream::NcType::_NULL && istream.variable_data(vars[dgd.indexes.stn_id_var].name,ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get station ID data","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  std::vector<std::string> platform_types,id_types,id_cache;
  if (ids.type() == netCDFStream::NcType::INT || ids.type() == netCDFStream::NcType::FLOAT || ids.type() == netCDFStream::NcType::DOUBLE) {
    for (size_t n=0; n < ids.size(); ++n) {
	id_cache.emplace_back(strutils::ftos(ids[n]));
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else if (ids.type() == netCDFStream::NcType::CHAR) {
    id_len=dims[vars[dgd.indexes.stn_id_var].dimids.back()].length;
    char *idbuf=reinterpret_cast<char *>(ids.get());
    for (size_t n=0; n < ids.size()/id_len; ++n) {
	id_cache.emplace_back(&idbuf[n*id_len],id_len);
platform_types.emplace_back("unknown");
id_types.emplace_back("unknown");
	pentry.key=platform_types[n];
	updatePlatformTable(obs_type_index,lats[n],lons[n]);
    }
  }
  else {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to determine platform type","nc2xml",user,args.argsString);
  }
  if (istream.variable_data(vars[dgd.indexes.z_var].name,levels) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get level data","nc2xml",user,args.argsString);
  }
  if (dgd.indexes.sample_dim_var != 0xffffffff) {
// H.19
    if (dgd.indexes.instance_dim_var == 0xffffffff) {
	metautils::logError("scanCFNonOrthgonalTimeSeriesProfileNetCDFFile returned error: found sample dimension but not instance dimension","nc2xml",user,args.argsString);
    }
    netCDFStream::VariableData row_sizes,station_indexes;
    if (istream.variable_data(vars[dgd.indexes.sample_dim_var].name,row_sizes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get sample dimension data","nc2xml",user,args.argsString);
    }
    if (istream.variable_data(vars[dgd.indexes.instance_dim_var].name,station_indexes) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: unable to get instance dimension data","nc2xml",user,args.argsString);
    }
    if (row_sizes.size() != station_indexes.size()) {
	metautils::logError("scanCFNonOrthogonalTimeSeriesProfileNetCDFFile returned error: sample dimension and instance dimension have different sizes","nc2xml",user,args.argsString);
    }
    netCDFStream::DataValue time_miss_val;
    for (const auto& var : vars) {
	if (var.dimids.front() == vars[dgd.indexes.z_var].dimids.front() && var.name != vars[dgd.indexes.z_var].name) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  std::string long_name,units;
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  auto off=0;
	  for (size_t n=0; n < row_sizes.size(); ++n) {
	    auto dt=computeNcTime(times,n);
	    std::vector<double> lvls;
	    for (size_t m=0; m < row_sizes[n]; ++m) {
		if (!found_missing(times[n],time_miss_val,var_data[off],miss_val)) {
		  lvls.emplace_back(levels[off]);
		}
		++off;
	    }
	    if (lvls.size() > 0) {
		ientry.key=platform_types[station_indexes[n]]+"[!]"+id_types[station_indexes[n]]+"[!]"+id_cache[station_indexes[n]];
		updateIDTable(obs_type_index,lats[station_indexes[n]],lons[station_indexes[n]],dt,times[n]);
		getVerticalResolutionData(lvls,dgd.z_pos,dgd.z_units);
		++total_num_not_missing;
	    }
	  }
	}
    }
  }
  else {
// H.16, H.18
    auto ntimes=dims[vars[dgd.indexes.time_var].dimids.back()].length;
    auto nlvls=dims[vars[dgd.indexes.z_var].dimids.back()].length;
    auto stn_size=ntimes*nlvls;
    netCDFStream::DataValue time_miss_val;
    for (const auto& var : vars) {
	if (var.name != vars[dgd.indexes.z_var].name && ((var.dimids.size() == 3 && var.dimids[0] == vars[dgd.indexes.stn_id_var].dimids.front() && var.dimids[1] == vars[dgd.indexes.time_var].dimids.back() && var.dimids[2] == vars[dgd.indexes.z_var].dimids.back()) || (var.dimids.size() == 2 && var.dimids[0] == vars[dgd.indexes.time_var].dimids.back() && var.dimids[1] == vars[dgd.indexes.z_var].dimids.back()))) {
	  de.key=var.name;
	  netCDFStream::VariableData var_data;
	  if (istream.variable_data(var.name,var_data) == netCDFStream::NcType::_NULL) {
	    metautils::logError("scanCFNonOrthogonalProfileNetCDFFile returned error: unable to get data for variable '"+var.name+"'","nc2xml",user,args.argsString);
	  }
	  std::string long_name,units;
	  netCDFStream::DataValue miss_val;
	  extractFromVariableAttribute(var.attrs,long_name,units,var.nc_type,miss_val);
	  for (size_t n=0; n < var_data.size(); ) {
	    auto stn_idx=n/stn_size;
	    for (size_t m=0; m < ntimes; ++m) {
		std::vector<double> lvls;
		for (size_t l=0; l < nlvls; ++l,++n) {
		  if (!found_missing(times[n],time_miss_val,var_data[n],miss_val)) {
		    if (levels.size() == var_data.size()) {
			lvls.emplace_back(levels[n]);
		    }
		    else if (levels.size() == nlvls) {
			lvls.emplace_back(levels[l]);
		    }
		    else {
			lvls.emplace_back(levels[stn_idx*nlvls+l]);
		    }
		  }
		}
		if (lvls.size() > 0) {
		  ientry.key=platform_types[stn_idx]+"[!]"+id_types[stn_idx]+"[!]"+id_cache[stn_idx];
		  if (times.size() == ntimes) {
		    auto dt=computeNcTime(times,m);
		    updateIDTable(obs_type_index,lats[stn_idx],lons[stn_idx],dt,times[m]);
		  }
		  else {
		    auto t_idx=stn_idx*ntimes+m;
		    auto dt=computeNcTime(times,t_idx);
		    updateIDTable(obs_type_index,lats[stn_idx],lons[stn_idx],dt,times[t_idx]);
		  }
		  getVerticalResolutionData(lvls,dgd.z_pos,dgd.z_units);
		  ++total_num_not_missing;
		}
	    }
	  }
	}
    }
  }
}

void scanCFTimeSeriesProfileNetCDFFile(InputNetCDFStream& istream,bool& foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  if (verbose_operation) {
    std::cout << "...beginning function scanCFTimeSeriesProfileNetCDFFile..." << std::endl;
  }
  initializeForObservations();
  auto vars=istream.variables();
  DiscreteGeometriesData dgd;
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR) {
	  if (vars[n].attrs[m].name == "units") {
	    processUnitsAttribute(vars[n].attrs[m],vars[n],n,dgd);
	  }
	  else if (vars[n].attrs[m].name == "cf_role") {
	    auto r=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    r=strutils::to_lower(r);
	    if (r == "timeseries_id") {
		if (dgd.indexes.stn_id_var != 0xffffffff) {
		  metautils::logError("scanCFTimeSeriesProfileNetCDFFile returned error: station ID was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
		}
		dgd.indexes.stn_id_var=n;
	    }
	  }
	  else if (vars[n].attrs[m].name == "sample_dimension") {
	    dgd.indexes.sample_dim_var=n;
	  }
	  else if (vars[n].attrs[m].name == "instance_dimension") {
	    dgd.indexes.instance_dim_var=n;
	  }
	  else if (vars[n].attrs[m].name == "axis") {
	    auto axis=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    if (axis == "Z") {
		dgd.indexes.z_var=n;
	    }
	  }
	}
    }
  }
  if (dgd.indexes.time_var == 0xffffffff) {
    metautils::logError("scanCFTimeSeriesProfileNetCDFFile returned error: unable to determine time variable","nc2xml",user,args.argsString);
  }
  else {
    for (size_t n=0; n < vars[dgd.indexes.time_var].attrs.size(); ++n) {
	if (vars[dgd.indexes.time_var].attrs[n].name == "calendar") {
	  time_data.calendar=*(reinterpret_cast<std::string *>(vars[dgd.indexes.time_var].attrs[n].values));
	}
    }
  }
  size_t obs_type_index=0xffffffff;
  if (dgd.indexes.z_var == 0xffffffff) {
    metautils::logError("scanCFTimeSeriesProfileNetCDFFile returned error: unable to determine vertical coordinate variable","nc2xml",user,args.argsString);
  }
  else {
    processVerticalCoordinateVariable(vars[dgd.indexes.z_var].attrs,dgd,obs_type_index);
  }
  if (obs_type_index == 0xffffffff) {
    metautils::logError("scanCFTimeSeriesProfileNetCDFFile returned error: unable to determine observation type","nc2xml",user,args.argsString);
  }
  if (vars[dgd.indexes.time_var].is_coord && vars[dgd.indexes.z_var].is_coord) {
// ex. H.17
    scanCFOrthogonalTimeSeriesProfileNetCDFFile(istream,dgd,obs_type_index);
  }
  else {
// ex. H.16, H.18, H.19
    scanCFNonOrthogonalTimeSeriesProfileNetCDFFile(istream,dgd,obs_type_index);
  }
  write_type=ObML_type;
  if (verbose_operation) {
    std::cout << "...function scanCFTimeSeriesProfileNetCDFFile done." << std::endl;
  }
}

void getGridProjection(Grid::GridDimensions& dim,double *lats,double *lons,Grid::GridDefinition& def)
{
  double min_lat_var=99999.,max_lat_var=0.;
  double min_lon_var=99999.,max_lon_var=0.;
  int n,m,l;
  double diff,min_diff;
  double first_lat,last_lat;
  const double PI=3.141592654;

  for (n=1,m=dim.x; n < dim.y; ++n,m+=dim.x) {
    diff=fabs(lats[m]-lats[m-dim.x]);
    if (diff < min_lat_var) {
	min_lat_var=diff;
    }
    if (diff > max_lat_var) {
	max_lat_var=diff;
    }
  }
  for (n=1; n < dim.x; ++n) {
    diff=fabs(lons[n]-lons[n-1]);
    if (diff < min_lon_var) {
	min_lon_var=diff;
    }
    if (diff > max_lon_var) {
	max_lon_var=diff;
    }
  }
  def.type=0;
  dim.size=dim.x*dim.y;
  if (fabs(max_lon_var-min_lon_var) < 0.0001) {
    if (fabs(max_lat_var-min_lat_var) < 0.0001) {
	def.type=Grid::latitudeLongitudeType;
	def.elatitude=lats[dim.size-1];
	def.elongitude=lons[dim.size-1];
    }
    else {
	first_lat=lats[0];
	last_lat=lats[dim.size-1];
	if (first_lat >= 0. && last_lat >= 0.) {
	}
	else if (first_lat < 0. && last_lat < 0.) {
	}
	else {
	  if (fabs(first_lat) > fabs(last_lat)) {
	  }
	  else {
	    if (fabs(cos(last_lat*PI/180.)-(min_lat_var/max_lat_var)) < 0.01) {
		def.type=Grid::mercatorType;
		def.elatitude=lats[dim.size-1];
		def.elongitude=lons[dim.size-1];
		min_diff=99999.;
		l=-1;
		for (n=0,m=0; n < dim.y; ++n,m+=dim.x) {
		  diff=fabs(lats[m]-lround(lats[m]));
		  if (diff < min_diff) {
		    min_diff=diff;
		    l=m;
		  }
		}
		def.dx=lround(cos(lats[l]*PI/180.)*min_lon_var*111.2);
		def.dy=lround((fabs(lats[l+dim.x]-lats[l])+fabs(lats[l]-lats[l-dim.x]))/2.*111.2);
		def.stdparallel1=lats[l];
	    }
	  }
	}
    }
  }
  if (def.type == 0) {
    metautils::logError("unable to determine grid projection","nc2xml",user,args.argsString);
  }
}

void scanCFGridNetCDFFile(InputNetCDFStream& istream,bool& foundMap,ParameterMap& parameterMap,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  std::string sdum;
  size_t n,m,l,x=0,y;
  std::string source,timeid,latid,lonid;
  std::vector<std::string> levid;
  std::vector<netCDFStream::NcType> levtype;
  size_t timedimid=0x3fffffff,latdimid=0x3fffffff,londimid=0x3fffffff;
  std::vector<int> levdimid;
  std::vector<std::string> descr,units;
  std::vector<bool> levwrite;
  netCDFStream::VariableData var_data,levels;
  size_t num_levels,nsteps;
  Grid::GridDimensions dim;
  Grid::GridDefinition def;
  std::deque<std::string> sp,sp2;
  long long dt;
  DateTime d1,d2,firstValidDateTime(30001231235959),lastValidDateTime(10000101000000);
  double *ta1=NULL,*ta2=NULL,diff;
  LevelMap levelMap;
  std::ifstream ifs;
  std::ofstream ofs;
  char line[32768];
  my::map<metautils::StringEntry> parameterTable,uniqueLevelIDTable;
  std::list<std::string> gentry_keys,map_contents;
  metautils::StringEntry se;
  std::string timeboundsid,climoBoundsName;
  my::map<metautils::NcTime::TimeRangeEntry> tr_table;
  metautils::NcTime::TimeRangeEntry tre;
  std::string time_method,tr_description;
  TempFile *tmpfile=NULL;
  int idx;
  bool foundTime,foundLat,foundLon;

  if (verbose_operation) {
    std::cout << "...beginning function scanCFGridNetCDFFile..." << std::endl;
  }
  foundTime=foundLat=foundLon=false;
  metadata::openInventory(inv,&inv_file,"nc2xml",user);
  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (strutils::to_lower(attrs[n].name) == "source") {
	source=*(reinterpret_cast<std::string *>(attrs[n].values));
    }
  }
  auto dims=istream.dimensions();
  auto vars=istream.variables();
  auto lontype=netCDFStream::NcType::_NULL;
// find the coordinate variables
  for (n=0; n < vars.size(); ++n) {
    if (vars[n].is_coord) {
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (vars[n].attrs[m].name == "standard_name") {
	    sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	  }
	  else {
	    sdum="";
	  }
	  if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR && (vars[n].attrs[m].name == "units" || std::regex_search(sdum,std::regex("hybrid_sigma")))) {
	    if (vars[n].attrs[m].name == "units") {
		sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    }
	    else {
		sdum="";
	    }
	    sdum=strutils::to_lower(sdum);
	    if (std::regex_search(sdum,std::regex("since"))) {
		if (foundTime) {
		  metautils::logError("time was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
		}
// check for time ranges other than analysis
		for (l=0; l < vars[n].attrs.size(); ++l) {
		  if (vars[n].attrs[l].nc_type == netCDFStream::NcType::CHAR) {
		    if (vars[n].attrs[l].name == "calendar") {
			time_data.calendar=*(reinterpret_cast<std::string *>(vars[n].attrs[l].values));
		    }
		    else if (vars[n].attrs[l].name == "bounds") {
			timeboundsid=*(reinterpret_cast<std::string *>(vars[n].attrs[l].values));
		    }
		    else if (vars[n].attrs[l].name == "climatology") {
			climoBoundsName=*(reinterpret_cast<std::string *>(vars[n].attrs[l].values));
		    }
		  }
		}
		time_data.units=sdum.substr(0,sdum.find("since"));
		strutils::trim(time_data.units);
		timeid=vars[n].name;
		timedimid=vars[n].dimids[0];
		sdum=sdum.substr(sdum.find("since")+5);
		strutils::trim(sdum);
		sp=strutils::split(sdum);
		sp2=strutils::split(sp[0],"-");
		if (sp2.size() != 3) {
		  metautils::logError("bad netcdf date","nc2xml",user,args.argsString);
		}
		dt=std::stoi(sp2[0])*10000000000+std::stoi(sp2[1])*100000000+std::stoi(sp2[2])*1000000;
		if (sp.size() > 1) {
		  sp2=strutils::split(sp[1],":");
		  dt+=std::stoi(sp2[0])*10000;
		  if (sp2.size() > 1) {
		    dt+=std::stoi(sp2[1])*100;
		  }
		  if (sp2.size() > 2) {
		    dt+=std::stoi(sp2[2]);
		  }
		}
		time_data.reference.set(dt);
		if (climoBoundsName.length() > 0) {
		  for (l=0; l < vars.size(); ++l) {
		    if (vars[l].name == climoBoundsName) {
			istream.variable_data(vars[l].name,var_data);
			ta1=new double[var_data.size()/2];
			ta2=new double[var_data.size()/2];
			for (l=0; l < static_cast<size_t>(var_data.size()); l+=2) {
			  ta1[l/2]=var_data[l];
			  ta2[l/2]=var_data[l+1];
			}
			nsteps=var_data.size()/2;
			for (l=0; l < nsteps; ++l) {
			  if (time_data.units == "hours") {
			    d1=time_data.reference.hoursAdded(ta1[l]);
			    d2=time_data.reference.hoursAdded(ta2[l]);
			  }
			  else if (time_data.units == "days") {
			    d1=time_data.reference.daysAdded(ta1[l]);
			    d2=time_data.reference.daysAdded(ta2[l]);
			  }
			  else if (time_data.units == "months") {
			    d1=time_data.reference.monthsAdded(ta1[l]);
			    d2=time_data.reference.monthsAdded(ta2[l]);
			  }
			  else {
			    metautils::logError("don't understand climatology_bounds units in "+time_data.units,"nc2xml",user,args.argsString);
			  }
			  tre.key=d2.getYearsSince(d1)+1;
			  if (!tr_table.found(tre.key,tre)) {
			    tre.unit=new int;
			    tre.num_steps=new int;
			    *tre.num_steps=0;
			    tre.instantaneous=new metautils::NcTime::TimeRange;
			    tre.instantaneous->first_valid_datetime.set(static_cast<long long>(30001231235959));
			    tre.instantaneous->last_valid_datetime.set(static_cast<long long>(10000101000000));
			    tre.bounded=new metautils::NcTime::TimeRange;
			    tre.bounded->first_valid_datetime.set(static_cast<long long>(30001231235959));
			    tre.bounded->last_valid_datetime.set(static_cast<long long>(10000101000000));
			    tr_table.insert(tre);
			  }
			  if (d1 < tre.bounded->first_valid_datetime) {
			    tre.bounded->first_valid_datetime=d1;
			  }
			  if (d2 > tre.bounded->last_valid_datetime) {
			    tre.bounded->last_valid_datetime=d2;
			  }
			  if (d1.getMonth() > d2.getMonth()) {
			    d1.setYear(d2.getYear()-1);
			  }
			  else {
			    d1.setYear(d2.getYear());
			  }
			  y=d2.getDaysSince(d1,time_data.calendar);
			  x=getDaysInMonth(d1.getYear(),d1.getMonth());
			  if (y == x || y == (x-1)) {
			    y=1;
			  }
			  else {
			    y=d2.getMonthsSince(d1);
			    if (y == 3) {
				y=2;
			    }
			    else if (y == 12) {
				y=3;
			    }
			    else {
				metautils::logError("unable to handle climatology of "+strutils::itos(y)+"-day means","nc2xml",user,args.argsString);
			    }
			  }
			  *tre.unit=y;
			  ++(*tre.num_steps);
			}
			delete[] ta1;
			delete[] ta2;
			l=vars.size();
		    }
		  }
		}
		foundTime=true;
	    }
	    else if (sdum == "degrees_north" || sdum == "degree_north" || sdum == "degrees_n" || sdum == "degree_n" || (sdum == "degrees" && vars[n].name == "lat")) {
		if (foundLat) {
		  metautils::logWarning("latitude was already identified - ignoring '"+vars[n].name+"'","nc2xml",user,args.argsString);
		}
else {
		latid=vars[n].name;
		latdimid=vars[n].dimids[0];
		foundLat=true;
}
	    }
	    else if (sdum == "degrees_east" || sdum == "degree_east" || sdum == "degrees_e" || sdum == "degree_e" || (sdum == "degrees" && vars[n].name == "lon")) {
		if (foundLon) {
		  metautils::logWarning("longitude was already identified - ignoring '"+vars[n].name+"'","nc2xml",user,args.argsString);
		}
else {
		lonid=vars[n].name;
		lontype=vars[n].nc_type;
		londimid=vars[n].dimids[0];
		foundLon=true;
}
	    }
	    else {
		if (!foundTime && vars[n].name == "time") {
		  foundTime=foundNetCDFTimeFromPatch(vars[n]);
		  if (foundTime) {
		    timedimid=vars[n].dimids[0];
		    timeid=vars[n].name;
		  }
		}
		else {
		  if (!uniqueLevelIDTable.found(vars[n].name,se)) {
		    levid.emplace_back(vars[n].name+"@@"+sdum);
		    levtype.emplace_back(vars[n].nc_type);
		    levdimid.emplace_back(vars[n].dimids[0]);
		    units.emplace_back(sdum);
		    levwrite.emplace_back(false);
		    for (l=0; l < vars[n].attrs.size(); ++l) {
			if (vars[n].attrs[l].nc_type == netCDFStream::NcType::CHAR && vars[n].attrs[l].name == "long_name") {
			  descr.emplace_back(*(reinterpret_cast<std::string *>(vars[n].attrs[l].values)));
			}
		    }
		    se.key=vars[n].name;
		    uniqueLevelIDTable.insert(se);
		  }
		}
	    }
	  }
	}
    }
  }
  if (!foundLat || !foundLon) {
    if (foundLat) {
	metautils::logError("Found latitude coordinate variable, but not longitude coordinate variable","nc2xml",user,args.argsString);
    }
    else if (foundLon) {
	metautils::logError("Found longitude coordinate variable, but not latitude coordinate variable","nc2xml",user,args.argsString);
    }
    else {
	latdimid=londimid=0;
	for (n=0; n < vars.size(); ++n) {
	  if (!vars[n].is_coord) {
	    for (m=0; m < vars[n].attrs.size(); ++m) {
		if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].nc_type == InputNetCDFStream::NcType::CHAR) {
		  sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
		  if (sdum == "degrees_north" || sdum == "degree_north" || sdum == "degrees_n" || sdum == "degree_n") {
		    latid=vars[n].name;
		    x=0;
		    for (l=0; l < vars[n].dimids.size(); ++l) {
			latdimid=100*latdimid+vars[n].dimids[l]+1;
			++x;
		    }
		    latdimid*=100;
		  }
		  else if (sdum == "degrees_east" || sdum == "degree_east" || sdum == "degrees_e" || sdum == "degree_e") {
		    lonid=vars[n].name;
		    lontype=vars[n].nc_type;
		    for (l=0; l < vars[n].dimids.size(); ++l) {
			londimid=100*londimid+vars[n].dimids[l]+1;
		    }
		    londimid*=100;
		  }
		}
	    }
	  }
	}
	if (latdimid == 0 || londimid == 0) {
	  metautils::logError("Could not find alternate latitude and longitude coordinate variables","nc2xml",user,args.argsString);
	}
	else if (latdimid != londimid) {
	  metautils::logError("Alternate latitude and longitude coordinate variables do not have the same dimensions","nc2xml",user,args.argsString);
	}
	else {
	  foundLat=foundLon=true;
	  if (x == 2) {
	    double *lats=NULL,*lons=NULL;
	    size_t nlats,nlons;
	    dim.y=latdimid/10000-1;
	    dim.x=(latdimid % 10000)/100-1;
	    istream.variable_data(latid,var_data);
	    nlats=var_data.size();
	    def.slatitude=var_data.front();
	    lats=new double[nlats];
	    for (m=0; m < nlats; ++m) {
		lats[m]=var_data[m];
	    }
	    istream.variable_data(lonid,var_data);
	    nlons=var_data.size();
	    def.slongitude=var_data.front();
	    lons=new double[nlons];
	    for (m=0; m < nlons; ++m) {
		lons[m]=var_data[m];
	    }
	    if ( (dims[dim.y].length % 2) == 1 && (dims[dim.x].length % 2) == 1) {
		dim.x=dims[dim.x].length;
		dim.y=dims[dim.y].length;
		getGridProjection(dim,lats,lons,def);
	    }
	    else {
		m=dims[dim.y].length/2-1;
		l=dims[dim.x].length/2-1;
		if (myequalf(lats[m*dims[dim.x].length+l],lats[(m+1)*dims[dim.x].length+l],0.00001) && myequalf(lats[(m+1)*dims[dim.x].length+l],lats[(m+1)*dims[dim.x].length+l+1],0.00001) && myequalf(lats[(m+1)*dims[dim.x].length+l+1],lats[m*dims[dim.x].length+l+1],0.00001) && myequalf(fabs(lons[m*dims[dim.x].length+l])+fabs(lons[(m+1)*dims[dim.x].length+l])+fabs(lons[(m+1)*dims[dim.x].length+l+1])+fabs(lons[m*dims[dim.x].length+l+1]),360.,0.00001)) {
		  def.type=Grid::polarStereographicType;
		  if (lats[m*dims[dim.x].length+l] > 0) {
		    def.projection_flag=0;
		    def.llatitude=60.;
		  }
		  else {
		    def.projection_flag=1;
		    def.llatitude=-60.;
		  }
		  def.olongitude=lroundf(lons[m*dims[dim.x].length+l]+45.);
		  if (def.olongitude > 180.) {
		    def.olongitude-=360.;
		  }
// look for dx and dy at the 60-degree parallel
		  dim.x=dims[dim.x].length;
		  dim.y=dims[dim.y].length;
		  double min_fabs=999.,f;
		  int min_m=0;
		  for (m=0; m < nlats; ++m) {
		    if ( (f=fabs(def.llatitude-lats[m])) < min_fabs) {
			min_fabs=f;
			min_m=m;
		    }
		  }
		  double rad=3.141592654/180.;
// great circle formula:
//  theta=2*arcsin[ sqrt( sin^2(delta_phi/2) + cos(phi_1)*cos(phi_2)*sin^2(delta_lambda/2) ) ]
//  phi_1 and phi_2 are latitudes
//  lambda_1 and lambda_2 are longitudes
//  dist = 6372.8 * theta
//  6372.8 is radius of Earth in km
		  def.dx=lroundf(asin(sqrt(sin(fabs(lats[min_m]-lats[min_m+1])/2.*rad)*sin(fabs(lats[min_m]-lats[min_m+1])/2.*rad)+sin(fabs(lons[min_m]-lons[min_m+1])/2.*rad)*sin(fabs(lons[min_m]-lons[min_m+1])/2.*rad)*cos(lats[min_m]*rad)*cos(lats[min_m+1]*rad)))*12745.6);
		  def.dy=lroundf(asin(sqrt(sin(fabs(lats[min_m]-lats[min_m+dim.x])/2.*rad)*sin(fabs(lats[min_m]-lats[min_m+dim.x])/2.*rad)+sin(fabs(lons[min_m]-lons[min_m+dim.x])/2.*rad)*sin(fabs(lons[min_m]-lons[min_m+dim.x])/2.*rad)*cos(lats[min_m]*rad)*cos(lats[min_m+dim.x]*rad)))*12745.6);
		}
		else {
		  metautils::logError("unable to determine grid type","nc2xml",user,args.argsString);
		}
	    }
	    delete[] lats;
	    delete[] lons;
	  }
	  else {
	    metautils::logError("Can't determine alternate grid from coordinates with "+strutils::itos(x)+" dimension(s)","nc2xml",user,args.argsString);
	  }
	}
    }
  }
  if (foundTime && levid.size() == 0 && latdimid > 100) {
// look for a level coordinate that is not in as a coordinate variable
    m=latdimid/10000-1;
    l=(latdimid % 10000)/100-1;
    levdimid.emplace_back(-1);
    for (n=0; n < vars.size(); ++n) {
	if (!vars[n].is_coord && vars[n].dimids.size() == 4 && vars[n].dimids[0] == timedimid && vars[n].dimids[2] == m && vars[n].dimids[3] == l) {
	  if (levdimid[0] == -1) {
	    levdimid[0]=vars[n].dimids[1];
	  }
	  else if (levdimid[0] != static_cast<int>(vars[n].dimids[1])) {
	    metautils::logError("found multiple level dimensions for the gridded parameters - failed on parameter '"+vars[n].name+"'","nc2xml",user,args.argsString);
	  }
	}
    }
    if (levdimid[0] == -1) {
	levdimid.clear();
    }
    else {
	for (n=0; n < vars.size(); ++n) {
	  if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid[0]) {
	    levtype.emplace_back(vars[n].nc_type);
	    for (m=0; m < vars[n].attrs.size(); ++m) {
		if (vars[n].attrs[m].name == "description" && vars[n].attrs[m].nc_type == InputNetCDFStream::NcType::CHAR) {
		  descr.emplace_back(*(reinterpret_cast<std::string *>(vars[n].attrs[m].values)));
		}
		else if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].nc_type == InputNetCDFStream::NcType::CHAR) {
		  units.emplace_back(*(reinterpret_cast<std::string *>(vars[n].attrs[m].values)));
		}
	    }
	    levid.emplace_back(vars[n].name+"@@"+units[0]);
	    if (descr.size() == 0) {
		descr.emplace_back(vars[n].name);
	    }
	    if (units.size() == 0) {
		units.emplace_back("");
	    }
	  }
	}
    }
  }
  if (levdimid.size() > 0 && levid.size() == 0) {
    metautils::logError("unable to determine the level coordinate variable","nc2xml",user,args.argsString);
  }
  levid.emplace_back("sfc");
  levtype.emplace_back(netCDFStream::NcType::_NULL);
  levdimid.emplace_back(-1);
  descr.emplace_back("Surface");
  units.emplace_back("");
  levwrite.emplace_back(false);
  if (foundTime && foundLat && foundLon) {
    if (tr_table.size() == 0) {
	tre.key=-1;
// set key for climate model simulations
	if (source == "CAM") {
	  tre.key=-11;
	}
	tre.unit=new int;
	*tre.unit=-1;
	tre.num_steps=new int;
	tre.instantaneous=new metautils::NcTime::TimeRange;
	tre.bounded=new metautils::NcTime::TimeRange;
// get t number of time steps and the temporal range
	istream.variable_data(timeid,var_data);
	time_s.t1=var_data.front();
	time_s.t2=var_data.back();
	time_s.num_times=var_data.size();
	if (inv.is_open()) {
	  time_s.times=new double[time_s.num_times];
	  for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
	    time_s.times[m]=var_data[m];
	  }
	}
	std::string error;
	tre.instantaneous->first_valid_datetime=metautils::NcTime::getActualDateTime(time_s.t1,time_data,error);
	if (error.length() > 0) {
	  metautils::logError(error,"nc2xml",user,args.argsString);
	}
	tre.instantaneous->last_valid_datetime=metautils::NcTime::getActualDateTime(time_s.t2,time_data,error);
	if (error.length() > 0) {
	  metautils::logError(error,"nc2xml",user,args.argsString);
	}
	*tre.num_steps=var_data.size();
	if (timeboundsid.length() > 0) {
	  auto timeboundstype=netCDFStream::NcType::_NULL;
	  for (m=0; m < vars.size(); ++m) {
	    if (vars[m].name == timeboundsid) {
		timeboundstype=vars[m].nc_type;
		m=vars.size();
	    }
	  }
	  if (timeboundstype == netCDFStream::NcType::_NULL) {
	    metautils::logError("unable to determine type of time bounds","nc2xml",user,args.argsString);
	  }
	  istream.variable_data(timeboundsid,var_data);
	  if (var_data.size() != time_s.num_times*2) {
	    metautils::logError("unable to handle more than two time bounds values per time","nc2xml",user,args.argsString);
	  }
	  time_bounds_s.t1=var_data.front();
	  time_bounds_s.diff=var_data[1]-time_bounds_s.t1;
	  for (l=2; l < static_cast<size_t>(var_data.size()); l+=2) {
	    diff=var_data[l+1]-var_data[l];
	    if (!myequalf(diff,time_bounds_s.diff)) {
		if (time_data.units != "days" || time_bounds_s.diff < 28 || time_bounds_s.diff > 31 || diff < 28 || diff > 31) {
		  time_bounds_s.changed=true;
		}
	    }
	  }
	  time_bounds_s.t2=var_data.back();
	  tre.bounded->first_valid_datetime=metautils::NcTime::getActualDateTime(time_bounds_s.t1,time_data,error);
	  if (error.length() > 0) {
	    metautils::logError(error,"nc2xml",user,args.argsString);
	  }
	  tre.bounded->last_valid_datetime=metautils::NcTime::getActualDateTime(time_bounds_s.t2,time_data,error);
	  if (error.length() > 0) {
	    metautils::logError(error,"nc2xml",user,args.argsString);
	  }
	}
	if (time_data.units == "months") {
	  if ((tre.instantaneous->first_valid_datetime).getDay() == 1) {
//	    (tre.instantaneous->last_valid_datetime).addDays(getDaysInMonth((tre.instantaneous->last_valid_datetime).getYear(),(tre.instantaneous->last_valid_datetime).getMonth(),time_data.calendar)-1,time_data.calendar);
(tre.instantaneous->last_valid_datetime).addMonths(1);
	  }
	  if (timeboundsid.length() > 0) {
	    if ((tre.bounded->first_valid_datetime).getDay() == 1) {
//		(tre.bounded->last_valid_datetime).addDays(getDaysInMonth((tre.bounded->last_valid_datetime).getYear(),(tre.bounded->last_valid_datetime).getMonth(),time_data.calendar)-1,time_data.calendar);
(tre.bounded->last_valid_datetime).addMonths(1);
	    }
	  }
	}
	tr_table.insert(tre);
    }
    if (latdimid < 100) {
	def.type=Grid::latitudeLongitudeType;
// get the latitude range
	istream.variable_data(latid,var_data);
	dim.y=var_data.size();
	def.slatitude=var_data.front();
	def.elatitude=var_data.back();
	def.laincrement=fabs((def.elatitude-def.slatitude)/(var_data.size()-1));
// check for gaussian lat-lon
	if (!myequalf(fabs(var_data[1]-var_data[0]),def.laincrement,0.001) && myequalf(var_data.size()/2.,var_data.size()/2,0.00000000001)) {
	  def.type=Grid::gaussianLatitudeLongitudeType;
	  def.laincrement=var_data.size()/2;
	}
// get the longitude range
	if (lontype != netCDFStream::NcType::_NULL) {
	  istream.variable_data(lonid,var_data);
	  dim.x=var_data.size();
	  def.slongitude=var_data.front();
	  def.elongitude=var_data.back();
	  def.loincrement=fabs((def.elongitude-def.slongitude)/(var_data.size()-1));
	}
    }
    for (m=0; m < levtype.size(); ++m) {
	gentry_keys.clear();
	sdum=levid[m].substr(0,levid[m].find("@@"));
	if (levtype[m] == netCDFStream::NcType::_NULL) {
	  num_levels=1;
	}
	else {
	  istream.variable_data(sdum,levels);
	  num_levels=levels.size();
	}
	for (const auto& key : tr_table.keys()) {
	  tr_table.found(key,tre);
	  addGriddedLatLonKeys(gentry_keys,dim,def,timeid,timedimid,levdimid[m],latdimid,londimid,tre,vars);
	  for (const auto& key2 : gentry_keys) {
	    gentry.key=key2;
	    idx=gentry.key.rfind("<!>");
	    auto U_key=gentry.key.substr(idx+3);
	    if (U_map.find(U_key) == U_map.end()) {
		U_map.emplace(U_key,std::make_pair(U_map.size(),""));
	    }
	    if (!grid_table.found(gentry.key,gentry)) {
// new grid
		gentry.level_table.clear();
		lentry.parameter_code_table.clear();
		param_entry.numTimeSteps=0;
		addGriddedParametersToNetCDFLevelEntry(vars,gentry.key,timeid,timedimid,levdimid[m],latdimid,londimid,foundMap,tre,parameterTable,varList,changedVarTable,parameterMap);
		for (n=0; n < num_levels; ++n) {
		  lentry.key="ds"+args.dsnum+","+levid[m]+":";
		  switch (levtype[m]) {
		    case netCDFStream::NcType::INT:
		    {
			lentry.key+=strutils::itos(levels[n]);
			break;
		    }
		    case netCDFStream::NcType::FLOAT:
		    case netCDFStream::NcType::DOUBLE:
		    {
			lentry.key+=strutils::ftos(levels[n],3);
			break;
		    }
		    case netCDFStream::NcType::_NULL:
		    {
			lentry.key+="0";
		 	break;
		    }
		    default: {}
		  }
		  if (lentry.parameter_code_table.size() > 0) {
		    gentry.level_table.insert(lentry);
		    if (inv.is_open()) {
			addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid[m],latdimid,londimid,istream);
		    }
		    levwrite[m]=true;
		  }
		}
		if (gentry.level_table.size() > 0) {
		  grid_table.insert(gentry);
		}
	    }
	    else {
// existing grid - needs update
		for (n=0; n < num_levels; ++n) {
		  lentry.key="ds"+args.dsnum+","+levid[m]+":";
		  switch (levtype[m]) {
		    case netCDFStream::NcType::INT:
		    {
			lentry.key+=strutils::itos(levels[n]);
			break;
		    }
		    case netCDFStream::NcType::FLOAT:
		    case netCDFStream::NcType::DOUBLE:
		    {
			lentry.key+=strutils::ftos(levels[n],3);
			break;
		    }
		    case netCDFStream::NcType::_NULL:
		    {
			lentry.key+="0";
			break;
		    }
		    default: {}
		  }
		  if (!gentry.level_table.found(lentry.key,lentry)) {
		    lentry.parameter_code_table.clear();
		    addGriddedParametersToNetCDFLevelEntry(vars,gentry.key,timeid,timedimid,levdimid[m],latdimid,londimid,foundMap,tre,parameterTable,varList,changedVarTable,parameterMap);
		    if (lentry.parameter_code_table.size() > 0) {
			gentry.level_table.insert(lentry);
			if (inv.is_open()) {
			  addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid[m],latdimid,londimid,istream);
			}
			levwrite[m]=true;
		    }
		  }
		  else {
// run through all of the parameters
		    for (l=0; l < vars.size(); ++l) {
			if (!vars[l].is_coord && vars[l].dimids[0] == timedimid && ((vars[l].dimids.size() == 4 && levdimid[m] >= 0 && static_cast<int>(vars[l].dimids[1]) == levdimid[m] && vars[l].dimids[2] == latdimid && vars[l].dimids[3] == londimid) || (vars[l].dimids.size() == 3 && levdimid[m] < 0 && vars[l].dimids[1] == latdimid && vars[l].dimids[2] == londimid))) {
			  param_entry.key="ds"+args.dsnum+":"+vars[l].name;
			  time_method=getGriddedTimeMethod(vars[l],timeid);
			  time_method=strutils::capitalize(time_method);
			  if (!lentry.parameter_code_table.found(param_entry.key,param_entry)) {
			    if (time_method.length() == 0 || (myequalf(time_bounds_s.t1,0,0.0001) && myequalf(time_bounds_s.t1,time_bounds_s.t2,0.0001))) {
				addGriddedNetCDFParameter(vars[l],foundMap,tre.instantaneous->first_valid_datetime,tre.instantaneous->last_valid_datetime,*tre.num_steps,parameterTable,varList,changedVarTable,parameterMap);
			    }
			    else {
				if (time_bounds_s.changed) {
				  metautils::logError("time bounds changed","nc2xml",user,args.argsString);
				}
				addGriddedNetCDFParameter(vars[l],foundMap,tre.bounded->first_valid_datetime,tre.bounded->last_valid_datetime,*tre.num_steps,parameterTable,varList,changedVarTable,parameterMap);
			    }
			    gentry.level_table.replace(lentry);
			    if (inv.is_open()) {
				addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid[m],latdimid,londimid,istream);
			    }
			  }
			  else {
			    std::string error;
			    tr_description=metautils::NcTime::getGriddedNetCDFTimeRangeDescription(tre,time_data,time_method,error);
			    if (error.length() > 0) {
				metautils::logError(error,"nc2xml",user,args.argsString);
			    }
			    tr_description=strutils::capitalize(tr_description);
			    if (strutils::has_ending(gentry.key,tr_description)) {
				if (time_method.length() == 0 || (myequalf(time_bounds_s.t1,0,0.0001) && myequalf(time_bounds_s.t1,time_bounds_s.t2,0.0001))) {
				  if (tre.instantaneous->first_valid_datetime < param_entry.startDateTime) {
				    param_entry.startDateTime=tre.instantaneous->first_valid_datetime;
				  }
				  if (tre.instantaneous->last_valid_datetime > param_entry.endDateTime) {
				    param_entry.endDateTime=tre.instantaneous->last_valid_datetime;
				  }
				}
				else {
				  if (tre.bounded->first_valid_datetime < param_entry.startDateTime) {
				    param_entry.startDateTime=tre.bounded->first_valid_datetime;
				  }
				  if (tre.bounded->last_valid_datetime > param_entry.endDateTime) {
				    param_entry.endDateTime=tre.bounded->last_valid_datetime;
				  }
				}
				param_entry.numTimeSteps+=*tre.num_steps;
				lentry.parameter_code_table.replace(param_entry);
				gentry.level_table.replace(lentry);
				if (inv.is_open()) {
				  addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid[m],latdimid,londimid,istream);
				}
			    }
			  }
			  levwrite[m]=true;
			}
		    }
		  }
		}
		grid_table.replace(gentry);
	    }
	  }
	}
    }
    map_contents.clear();
    sdum=getRemoteWebFile("https://rda.ucar.edu/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
    if (levelMap.fill(sdum)) {
	ifs.open(sdum.c_str());
	ifs.getline(line,32768);
	while (!ifs.eof()) {
	  map_contents.emplace_back(line);
	  ifs.getline(line,32768);
	}
	ifs.close();
	map_contents.pop_back();
    }
    else {
	map_contents.clear();
    }
    tmpfile=new TempFile("/tmp","");
    ofs.open(tmpfile->name().c_str());
    if (!ofs.is_open()) {
	metautils::logError("can't open /tmp/"+tmpfile->name()+" for writing netCDF levels","nc2xml",user,args.argsString);
    }
    if (map_contents.size() > 0) {
	for (const auto& line : map_contents) {
	  ofs << line << std::endl;
	}
    }
    else {
	ofs << "<?xml version=\"1.0\" ?>" << std::endl;
	ofs << "<levelMap>" << std::endl;
    }
    for (m=0; m < levwrite.size(); ++m) {
	if (levwrite[m] && (map_contents.size() == 0 || (map_contents.size() > 0 && levelMap.isLayer(levid[m]) < 0))) {
	  ofs << "  <level code=\"" << levid[m] << "\">" << std::endl;
	  ofs << "    <description>" << descr[m] << "</description>" << std::endl;
	  ofs << "    <units>" << units[m] << "</units>" << std::endl;
	  ofs << "  </level>" << std::endl;
	}
    }
    ofs << "</levelMap>" << std::endl;
    ofs.close();
    std::string error;
    if (hostSync(tmpfile->name(),"/__HOST__/web/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",error) < 0) {
	metautils::logWarning("level map was not synced - error(s): '"+error+"'","nc2xml",user,args.argsString);
    }
    std::stringstream oss,ess;
    mysystem2("/bin/cp "+tmpfile->name()+" /glade/u/home/rdadata/share/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",oss,ess);
    delete tmpfile;
  }
  if (grid_table.size() == 0) {
    if (foundTime) {
	metautils::logError("No grids found - no content metadata will be generated","nc2xml",user,args.argsString);
    }
    else {
	metautils::logError("Time coordinate variable not found - no content metadata will be generated","nc2xml",user,args.argsString);
    }
  }
  write_type=GrML_type;
  delete[] time_s.times;
  if (verbose_operation) {
    std::cout << "...function scanCFGridNetCDFFile done." << std::endl;
  }
}

struct LIDEntry {
  size_t key;
};

void scanWRFSimulationNetCDFFile(InputNetCDFStream& istream,bool& foundMap,std::string& mapName,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  std::ifstream ifs;
  std::ofstream ofs;
  char line[32768];
  size_t n,m,l,x=0;
  std::string sdum;
  my::map<metautils::StringEntry> parameterTable;
  ParameterMap parameterMap;
  LevelMap levelMap;
  std::string timeid,latid,lonid;
  size_t timedimid=0x3fffffff;
  size_t latdimid=0;
  size_t londimid=0;
  Grid::GridDimensions dim;
  Grid::GridDefinition def;
  netCDFStream::VariableData var_data;
  double *lats=NULL,*lons=NULL;
  size_t nlats,nlons;
  std::list<std::string> gentry_keys,map_contents;
  metautils::NcTime::TimeRangeEntry tre;
  my::map<LIDEntry> unique_levdimids_table;
  std::list<int> levdimids_list;
  LIDEntry lide;
  TempFile *tmpfile=NULL;
  int idx;
  bool foundTime,foundLat,foundLon;

  if (verbose_operation) {
    std::cout << "...beginning function scanWRFSimulationNetCDFFile..." << std::endl;
  }
  sdum=getRemoteWebFile("https://rda.ucar.edu/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
  if (levelMap.fill(sdum)) {
    ifs.open(sdum.c_str());
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	map_contents.emplace_back(line);
	ifs.getline(line,32768);
    }
    ifs.close();
    ifs.clear();
    map_contents.pop_back();
  }
  mapName=getRemoteWebFile("https://rda.ucar.edu/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
  if (!parameterMap.fill(mapName)) {
    foundMap=false;
  }
  else {
    foundMap=true;
  }
  foundTime=foundLat=foundLon=false;
  metadata::openInventory(inv,&inv_file,"nc2xml",user);
  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (strutils::to_lower(attrs[n].name) == "simulation_start_date") {
	break;
    }
  }
  if (n == attrs.size()) {
    metautils::logError("does not appear to be a WRF Climate Simulation file","nc2xml",user,args.argsString);
  }
  tre.key=-11;
  auto dims=istream.dimensions();
  auto vars=istream.variables();
// find the coordinate variables
  for (n=0; n < vars.size(); ++n) {
    for (m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].is_coord) {
	  if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR && vars[n].attrs[m].name == "description") {
	    sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    sdum=strutils::to_lower(sdum);
	    if (std::regex_search(sdum,std::regex("since"))) {
		if (foundTime) {
		  metautils::logError("time was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
		}
		fillNcTimeData(vars[n].attrs[m]);
		timeid=vars[n].name;
		timedimid=vars[n].dimids[0];
		foundTime=true;
		tre.instantaneous=new metautils::NcTime::TimeRange;
		tre.num_steps=new int;
		istream.variable_data(vars[n].name,var_data);
		time_s.num_times=var_data.size();
		std::string error;
		tre.instantaneous->first_valid_datetime=metautils::NcTime::getActualDateTime(var_data.front(),time_data,error);
		if (error.length() > 0) {
		  metautils::logError(error,"nc2xml",user,args.argsString);
		}
		tre.instantaneous->last_valid_datetime=metautils::NcTime::getActualDateTime(var_data.back(),time_data,error);
		if (error.length() > 0) {
		  metautils::logError(error,"nc2xml",user,args.argsString);
		}
		if (inv.is_open()) {
		    time_s.times=new double[time_s.num_times];
		    for (l=0; l < time_s.num_times; ++l) {
			time_s.times[l]=var_data[l];
		    }
		}
		*tre.num_steps=time_s.num_times;
	    }
	  }
	}
	else {
	  if (vars[n].attrs[m].name == "units" && vars[n].attrs[m].nc_type == InputNetCDFStream::NcType::CHAR) {
	    sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	    if (sdum == "degrees_north" || sdum == "degree_north" || sdum == "degrees_n" || sdum == "degree_n") {
		latid=vars[n].name;
		for (l=0; l < vars[n].dimids.size(); ++l) {
		  latdimid=100*latdimid+vars[n].dimids[l]+1;
		  ++x;
		}
		latdimid*=100;
	    }
	    else if (sdum == "degrees_east" || sdum == "degree_east" || sdum == "degrees_e" || sdum == "degree_e") {
		lonid=vars[n].name;
		for (l=0; l < vars[n].dimids.size(); ++l) {
		  londimid=100*londimid+vars[n].dimids[l]+1;
		}
		londimid*=100;
	    }
	  }
	}
    }
  }
  if (!foundTime) {
    metautils::logError("scanWRFSimulationNetCDFFile could not find the time coordinate variable","nc2xml",user,args.argsString);
  }
  if (latdimid == 0 || londimid == 0) {
    metautils::logError("scanWRFSimulationNetCDFFile could not find the latitude and longitude coordinate variables","nc2xml",user,args.argsString);
  }
  else if (latdimid != londimid) {
    metautils::logError("scanWRFSimulationNetCDFFile found latitude and longitude coordinate variables, but they do not have the same dimensions","nc2xml",user,args.argsString);
  }
  else {
    if (x == 2) {
	londimid=(latdimid % 10000)/100-1;
	latdimid=latdimid/10000-1;
	istream.variable_data(latid,var_data);
	nlats=var_data.size();
	def.slatitude=var_data.front();
	lats=new double[nlats];
	for (m=0; m < nlats; ++m) {
	  lats[m]=var_data[m];
	}
	istream.variable_data(lonid,var_data);
	nlons=var_data.size();
	def.slongitude=var_data.front();
	lons=new double[nlons];
	for (m=0; m < nlons; ++m) {
	  lons[m]=var_data[m];
	}
	dim.x=dims[londimid].length;
	dim.y=dims[latdimid].length;
	def.type=0;
	getGridProjection(dim,lats,lons,def);
	if (def.type == 0) {
	  metautils::logError("scanWRFSimulationNetCDFFile was not able to deterimine the grid definition type","nc2xml",user,args.argsString);
	}
	delete[] lats;
	delete[] lons;
    }
  }
  for (n=0; n < vars.size(); ++n) {
    if (vars[n].dimids.size() == 3 || vars[n].dimids.size() == 4) {
	if (vars[n].dimids[0] == timedimid && vars[n].dimids[vars[n].dimids.size()-2] == latdimid && vars[n].dimids[vars[n].dimids.size()-1] == londimid) {
	  if (vars[n].dimids.size() == 3) {
	    lide.key=static_cast<size_t>(-1);
	  }
	  else {
	    lide.key=vars[n].dimids[1];
	  }
	  if (!unique_levdimids_table.found(lide.key,lide)) {
	    unique_levdimids_table.insert(lide);
	    levdimids_list.emplace_back(lide.key);
	  }
	}
    }
  }
  tmpfile=new TempFile("/tmp","");
  ofs.open(tmpfile->name().c_str());
  if (!ofs.is_open()) {
    metautils::logError("scanWRFSimulationNetCDFFile can't open /tmp/"+tmpfile->name()+" for writing netCDF levels","nc2xml",user,args.argsString);
  }
  if (map_contents.size() > 0) {
    for (const auto& line : map_contents) {
	ofs << line << std::endl;
    }
  }
  else {
    ofs << "<?xml version=\"1.0\" ?>" << std::endl;
    ofs << "<levelMap>" << std::endl;
  }
  for (const auto& levdimid : levdimids_list) {
    addGriddedLatLonKeys(gentry_keys,dim,def,timeid,timedimid,levdimid,latdimid,londimid,tre,vars);
    if (levdimid == -1) {
	if (map_contents.size() == 0 || (map_contents.size() > 0 && levelMap.isLayer("sfc") < 0)) {
	  ofs << "  <level code=\"sfc\">" << std::endl;
	  ofs << "    <description>Surface</description>" << std::endl;
	  ofs << "  </level>" << std::endl;
	}
    }
    else {
	for (n=0; n < vars.size(); ++n) {
	  if (vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid && (map_contents.size() == 0 || (map_contents.size() > 0 && levelMap.isLayer(vars[n].name) < 0))) {
	    ofs << "  <level code=\"" << vars[n].name << "\">" << std::endl;
	    for (m=0; m < vars[n].attrs.size(); ++m) {
		if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR) {
		  if (vars[n].attrs[m].name == "description") {
		    ofs << "    <description>" << *(reinterpret_cast<std::string *>(vars[n].attrs[m].values)) << "</description>" << std::endl;
		  }
		  else if (vars[n].attrs[m].name == "units") {
		    ofs << "    <units>" << *(reinterpret_cast<std::string *>(vars[n].attrs[m].values)) << "</units>" << std::endl;
		  }
		}
	    }
	    ofs << "  </level>" << std::endl;
	  }
	}
    }
  }
  ofs << "</levelMap>" << std::endl;
  ofs.close();
  std::string error;
  if (hostSync(tmpfile->name(),"/__HOST__/web/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",error) < 0) {
    metautils::logWarning("scanWRFSimulationNetCDFFile - level map was not synced - error(s): '"+error+"'","nc2xml",user,args.argsString);
  }
  std::stringstream oss,ess;
  mysystem2("/bin/cp "+tmpfile->name()+" /glade/u/home/rdadata/share/metadata/LevelTables/netCDF.ds"+args.dsnum+".xml",oss,ess);
  delete tmpfile;
  for (const auto& key : gentry_keys) {
    gentry.key=key;
    idx=gentry.key.rfind("<!>");
    auto U_key=gentry.key.substr(idx+3);
    if (U_map.find(U_key) == U_map.end()) {
	U_map.emplace(U_key,std::make_pair(U_map.size(),""));
    }
    if (!grid_table.found(gentry.key,gentry)) {
// new grid
	gentry.level_table.clear();
	lentry.parameter_code_table.clear();
	param_entry.numTimeSteps=0;
	for (const auto& levdimid : levdimids_list) {
	  addGriddedParametersToNetCDFLevelEntry(vars,gentry.key,timeid,timedimid,levdimid,latdimid,londimid,foundMap,tre,parameterTable,varList,changedVarTable,parameterMap);
	  if (lentry.parameter_code_table.size() > 0) {
	    if (levdimid < 0) {
		lentry.key="ds"+args.dsnum+",sfc:0";
		gentry.level_table.insert(lentry);
	    }
	    else {
		for (n=0; n < vars.size(); ++n) {
		  if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
		    istream.variable_data(vars[n].name,var_data);
		    for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
			lentry.key="ds"+args.dsnum+","+vars[n].name+":";
			switch (vars[n].nc_type) {
			  case netCDFStream::NcType::SHORT:
			  case netCDFStream::NcType::INT:
			  {
			    lentry.key+=strutils::itos(var_data[m]);
			    break;
			  }
			  case netCDFStream::NcType::FLOAT:
			  case netCDFStream::NcType::DOUBLE:
			  {
			    lentry.key+=strutils::ftos(var_data[m],3);
			    break;
			  }
			  default:
			  {
			    metautils::logError("scanWRFSimulationNetCDFFile can't get times for nc_type "+strutils::itos(static_cast<int>(vars[n].nc_type)),"nc2xml",user,args.argsString);
			  }
			}
			gentry.level_table.insert(lentry);
			if (inv.is_open()) {
			  addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid,latdimid,londimid,istream);
			}
		    }
		  }
		}
	    }
	  }
	}
	if (gentry.level_table.size() > 0) {
	  grid_table.insert(gentry);
	}
    }
    else {
// existing grid - needs update
	for (const auto& levdimid : levdimids_list) {
	  if (levdimid < 0) {
	    lentry.key="ds"+args.dsnum+",sfc:0";
	    if (!gentry.level_table.found(lentry.key,lentry)) {
		lentry.parameter_code_table.clear();
		addGriddedParametersToNetCDFLevelEntry(vars,gentry.key,timeid,timedimid,levdimid,latdimid,londimid,foundMap,tre,parameterTable,varList,changedVarTable,parameterMap);
		if (lentry.parameter_code_table.size() > 0) {
		  gentry.level_table.insert(lentry);
		  if (inv.is_open()) {
		    addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid,latdimid,londimid,istream);
		  }
		}
	    }
	  }
	  else {
	    for (n=0; n < vars.size(); ++n) {
		if (!vars[n].is_coord && vars[n].dimids.size() == 1 && static_cast<int>(vars[n].dimids[0]) == levdimid) {
		  if (lentry.parameter_code_table.size() > 0) {
		    istream.variable_data(vars[n].name,var_data);
		  }
		  for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
		    lentry.key="ds"+args.dsnum+","+vars[n].name+":";
		    switch (vars[n].nc_type) {
			case netCDFStream::NcType::SHORT:
			case netCDFStream::NcType::INT:
			{
			  lentry.key+=strutils::itos(var_data[m]);
			  break;
			}
			case netCDFStream::NcType::FLOAT:
			case netCDFStream::NcType::DOUBLE:
			{
			  lentry.key+=strutils::ftos(var_data[m],3);
			  break;
			}
			default:
			{
			  metautils::logError("scanWRFSimulationNetCDFFile can't get times for nc_type "+strutils::itos(static_cast<int>(vars[n].nc_type)),"nc2xml",user,args.argsString);
			}
		    }
		    if (!gentry.level_table.found(lentry.key,lentry)) {
			lentry.parameter_code_table.clear();
			addGriddedParametersToNetCDFLevelEntry(vars,gentry.key,timeid,timedimid,levdimid,latdimid,londimid,foundMap,tre,parameterTable,varList,changedVarTable,parameterMap);
			if (lentry.parameter_code_table.size() > 0) {
			  gentry.level_table.insert(lentry);
			  if (inv.is_open()) {
			    addLevelToInventory(lentry.key,gentry.key,timedimid,levdimid,latdimid,londimid,istream);
			  }
			}
		    }
		  }
		}
	    }
	  }
	}
    }
  }
  if (grid_table.size() == 0) {
    metautils::logError("No grids found - no content metadata will be generated","nc2xml",user,args.argsString);
  }
  write_type=GrML_type;
  if (verbose_operation) {
    std::cout << "...function scanWRFSimulationNetCDFFile done." << std::endl;
  }
}

void scanCFNetCDFFile(InputNetCDFStream& istream,bool& foundMap,std::string& mapName,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  auto attrs=istream.global_attributes();
  std::string feature_type;
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "featureType") {
	feature_type=*(reinterpret_cast<std::string *>(attrs[n].values));
    }
  }
// rename the parameter map so that it is not overwritten by the level map,
//   which has the same name
  if (!mapName.empty()) {
    std::stringstream oss,ess;
    mysystem2("/bin/mv "+mapName+" "+mapName+".p",oss,ess);
    if (ess.str().length() > 0) {
	metautils::logError("unable to rename parameter map; error - '"+ess.str()+"'","nc2xml",user,args.argsString);
    }
    mapName+=".p";
  }
  if (feature_type.length() > 0) {
    DataTypeMap dataTypeMap;
    if (mapName.length() > 0 && dataTypeMap.fill(mapName)) {
	foundMap=true;
    }
    else {
	foundMap=false;
    }
    if (feature_type == "point") {
	scanCFPointNetCDFFile(istream,foundMap,dataTypeMap,varList);
    }
    else if (feature_type == "timeSeries") {
	scanCFTimeSeriesNetCDFFile(istream,foundMap,dataTypeMap,varList);
    }
    else if (feature_type == "profile") {
	scanCFProfileNetCDFFile(istream,foundMap,dataTypeMap,varList);
    }
    else if (feature_type == "timeSeriesProfile") {
	scanCFTimeSeriesProfileNetCDFFile(istream,foundMap,dataTypeMap,varList);
    }
    else {
	metautils::logError("featureType '"+feature_type+"' not recognized","nc2xml",user,args.argsString);
    }
  }
  else {
    ParameterMap parameterMap;
    if (mapName.length() > 0 && parameterMap.fill(mapName)) {
	foundMap=true;
    }
    else {
	foundMap=false;
    }
    scanCFGridNetCDFFile(istream,foundMap,parameterMap,varList,changedVarTable);
  }
}

void scanRAFAircraftNetCDFFile(InputNetCDFStream& istream,bool& foundMap,std::string& mapName,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  int obsTypeIndex=-1;
  size_t n,m,l,k;
  std::string sdum,timevarname,timeunits,long_name,units,vunits;
  std::deque<std::string> sp,sp2;
  my::map<metautils::StringEntry> coordsTable;
  netCDFStream::VariableData var_data;
  int time;
  short yr,mo,dy;
  DateTime reftime,dt_start,dt_end;
  double a=0,b=0,*lats=NULL,*lons=NULL,*alts=NULL,max_altitude=-99999.,min_altitude=999999.;
  metautils::StringEntry se;
  std::list<std::string> dataTypesList;
  bool ignoreAsDataType,ignoreAltitude=false;
  DataTypeMap dataTypeMap;

  initializeForObservations();
  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "Aircraft") {
	obsTypeIndex=0;
	pentry.key="aircraft";
	sdum=*(reinterpret_cast<std::string *>(attrs[n].values));
	sp=strutils::split(sdum);
	ientry.key=pentry.key+"[!]callSign[!]"+metautils::cleanID(sp[0]);
    }
    else if (attrs[n].name == "coordinates") {
	sdum=*(reinterpret_cast<std::string *>(attrs[n].values));
	sp=strutils::split(sdum);
	for (m=0; m < sp.size(); ++m) {
	  se.key=sp[m];
	  coordsTable.insert(se);
	}
    }
  }
  if (obsTypeIndex >= 0) {
    if (coordsTable.size() == 0) {
	metautils::logError("scanRAFAircraftNetCDFFile returned error: unable to determine variable coordinates","nc2xml",user,args.argsString);
    }
    ientry.data.reset(new metadata::ObML::IDEntry::Data);
    ientry.data->N_lat=-99.;
    ientry.data->S_lat=99.;
    ientry.data->W_lon=999.;
    ientry.data->E_lon=-999;
    ientry.data->min_lon_bitmap.reset(new float[360]);
    ientry.data->max_lon_bitmap.reset(new float[360]);
    for (n=0; n < 360; ++n) {
	ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
    }
    ientry.data->start.set(3000,12,31,235959);
    ientry.data->end.set(1000,1,1,0);
    ID_table[obsTypeIndex]->insert(ientry);
    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
    pentry.boxflags->initialize(361,180,0,0);
    platform_table[obsTypeIndex].insert(pentry);
  }
  else {
    metautils::logError("scanRAFAircraftNetCDFFile returned error: file does not appear to be NCAR-RAF/nimbus compliant netCDF","nc2xml",user,args.argsString);
  }
  mapName=getRemoteWebFile("https://rda.ucar.edu/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
  if (dataTypeMap.fill(mapName)) {
    foundMap=true;
  }
  else {
    foundMap=false;
  }
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    ignoreAsDataType=false;
    long_name="";
    units="";
    for (m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].name == "long_name" || vars[n].attrs[m].name == "standard_name") {
	  sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	  if (vars[n].attrs[m].name == "long_name") {
	    long_name=sdum;
	  }
	  sdum=strutils::to_lower(sdum);
	  if (std::regex_search(sdum,std::regex("time"))) {
	    timevarname=vars[n].name;
	    istream.variable_data(vars[n].name,var_data);
	    ientry.data->nsteps=var_data.size();
	    a=var_data.front();
	    b=var_data.back();
	  }
	  else if (std::regex_search(sdum,std::regex("latitude")) || std::regex_search(sdum,std::regex("longitude")) || std::regex_search(sdum,std::regex("altitude"))) {
	    ignoreAsDataType=true;
	  }
	}
	else if (vars[n].attrs[m].name == "units") {
	  units=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	}
	if (vars[n].name == timevarname && vars[n].attrs[m].name == "units") {
	  sdum=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	  sp=strutils::split(sdum);
	  if (sp.size() < 4 || sp[1] != "since") {
	    metautils::logError("scanRAFAircraftNetCDFFile returned error: bad units '"+sdum+"' on time variable","nc2xml",user,args.argsString);
	  }
	  timeunits=sp[0];
	  sp2=strutils::split(sp[2],"-");
	  if (sp2.size() != 3) {
	    metautils::logError("scanRAFAircraftNetCDFFile returned error: bad units on time variable","nc2xml",user,args.argsString);
	  }
	  yr=std::stoi(sp2[0]);
	  mo=std::stoi(sp2[1]);
	  dy=std::stoi(sp2[2]);
	  sp2=strutils::split(sp[3],":");
	  if (sp2.size() != 3) {
	    metautils::logError("scanRAFAircraftNetCDFFile returned error: bad units on time variable","nc2xml",user,args.argsString);
	  }
	  time=std::stoi(sp2[0])*10000+std::stoi(sp2[1])*100+std::stoi(sp2[2]);
	  reftime.set(yr,mo,dy,time);
	}
    }
    sdum=strutils::to_lower(vars[n].name);
    if (std::regex_search(sdum,std::regex("lat")) && coordsTable.found(vars[n].name,se)) {
	istream.variable_data(vars[n].name,var_data);
	lats=new double[var_data.size()];
	for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
	   lats[m]=var_data[m];
	}
    }
    else if (std::regex_search(sdum,std::regex("lon")) && coordsTable.found(vars[n].name,se)) {
	istream.variable_data(vars[n].name,var_data);
	lons=new double[var_data.size()];
	for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
	  lons[m]=var_data[m];
	}
    }
    else if (std::regex_search(sdum,std::regex("alt")) && coordsTable.found(vars[n].name,se)) {
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (vars[n].attrs[m].name == "units") {
	    vunits=*(reinterpret_cast<std::string *>(vars[n].attrs[m].values));
	  }
	}
	istream.variable_data(vars[n].name,var_data);
	alts=new double[var_data.size()];
	for (m=0; m < static_cast<size_t>(var_data.size()); ++m) {
	  alts[m]=var_data[m];
	}
    }
    else if (!ignoreAsDataType && !coordsTable.found(vars[n].name,se)) {
	sdum=vars[n].name+"<!>"+long_name+"<!>";
	if (units.length() > 0) {
	  sdum+="<nobr>"+units+"</nobr>";
	}
	dataTypesList.emplace_back(sdum);
    }
  }
  if (timeunits == "seconds") {
    dt_start=reftime.secondsAdded(a);
    dt_end=reftime.secondsAdded(b);
  }
  else if (timeunits == "minutes") {
    dt_start=reftime.minutesAdded(a);
    dt_end=reftime.minutesAdded(b);
  }
  else if (timeunits == "hours") {
    dt_start=reftime.hoursAdded(a);
    dt_end=reftime.hoursAdded(b);
  }
  else {
    metautils::logError("scanRAFAircraftNetCDFFile returned error: bad time units '"+timeunits+"' on time variable","nc2xml",user,args.argsString);
  }
  if (dt_start < ientry.data->start) {
    ientry.data->start=dt_start;
  }
  if (dt_end > ientry.data->end) {
    ientry.data->end=dt_end;
  }
  for (n=0; n < ientry.data->nsteps; ++n) {
    ++total_num_not_missing;
    if (lats[n] > ientry.data->N_lat) {
	ientry.data->N_lat=lats[n];
    }
    if (lats[n] < ientry.data->S_lat) {
	ientry.data->S_lat=lats[n];
    }
    if (lons[n] > ientry.data->E_lon) {
	ientry.data->E_lon=lons[n];
    }
    if (lons[n] < ientry.data->W_lon) {
	ientry.data->W_lon=lons[n];
    }
    convertLatLonToBox(1,0.,lons[n],l,k);
    if (ientry.data->min_lon_bitmap[k] > 900.) {
	ientry.data->min_lon_bitmap[k]=ientry.data->max_lon_bitmap[k]=lons[n];
    }
    else {
	if (lons[n] < ientry.data->min_lon_bitmap[k]) {
	  ientry.data->min_lon_bitmap[k]=lons[n];
	}
	if (lons[n] > ientry.data->max_lon_bitmap[k]) {
	  ientry.data->max_lon_bitmap[k]=lons[n];
	}
    }
    if (lats[n] == -90.) {
	pentry.boxflags->spole=1;
    }
    else if (lats[n] == 90.) {
	pentry.boxflags->npole=1;
    }
    else {
	convertLatLonToBox(1,lats[n],lons[n],l,k);
	pentry.boxflags->flags[l-1][k]=1;
	pentry.boxflags->flags[l-1][360]=1;
    }
    if (alts != NULL) {
	if (alts[n] > max_altitude) {
	  max_altitude=alts[n];
	}
	if (alts[n] < min_altitude) {
	  min_altitude=alts[n];
	}
	delete[] alts;
    }
    else {
	ignoreAltitude=true;
    }
  }
  delete[] lats;
  delete[] lons;
  for (const auto& type : dataTypesList) {
    de.key=type.substr(0,type.find("<!>"));
    if (!ientry.data->data_types_table.found(de.key,de)) {
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	if (!ignoreAltitude) {
	  if (de.data->vdata == nullptr) {
	    de.data->vdata.reset(new metadata::ObML::DataTypeEntry::Data::VerticalData);
	  }
	  de.data->vdata->max_altitude=-99999.;
	  de.data->vdata->min_altitude=999999.;
	  de.data->vdata->avg_nlev=0;
	  de.data->vdata->avg_res=0.;
	  de.data->vdata->res_cnt=1;
	}
	ientry.data->data_types_table.insert(de);
    }
    de.data->nsteps+=ientry.data->nsteps;
    if (!ignoreAltitude) {
	if (max_altitude > de.data->vdata->max_altitude) {
	  de.data->vdata->max_altitude=max_altitude;
	}
	if (min_altitude < de.data->vdata->min_altitude) {
	  de.data->vdata->min_altitude=min_altitude;
	}
	de.data->vdata->avg_nlev+=ientry.data->nsteps;
	de.data->vdata->units=vunits;
    }
    sdum=dataTypeMap.getDescription(de.key);
    if (!foundMap || sdum.length() == 0) {
	se.key=type;
	if (!dataTypeTable.found(se.key,se)) {
	  dataTypeTable.insert(se);
	  varList.emplace_back(se.key);
	}
    }
  }
  write_type=ObML_type;
}

void setTimeMissingValue(netCDFStream::DataValue& time_miss_val,std::vector<InputNetCDFStream::Attribute>& attr,size_t index,netCDFStream::NcType time_type)
{
  time_miss_val.resize(time_type);
  switch (time_type) {
    case InputNetCDFStream::NcType::INT:
    {
	time_miss_val.set(*(reinterpret_cast<int *>(attr[index].values)));
	break;
    }
    case InputNetCDFStream::NcType::FLOAT:
    {
	time_miss_val.set(*(reinterpret_cast<float *>(attr[index].values)));
	break;
    }
    case InputNetCDFStream::NcType::DOUBLE:
    {
	time_miss_val.set(*(reinterpret_cast<double *>(attr[index].values)));
	break;
    }
    default:
    {
	metautils::logError("setTimeMissingValue returned error: unrecognized time type: "+strutils::itos(static_cast<int>(time_type)),"nc2xml",user,args.argsString);
    }
  }
}

struct Header {
  Header() : type(),ID(),valid_time(),lat(0.),lon(0.),elev(0.) {}

  std::string type,ID,valid_time;
  float lat,lon,elev;
};
void scanPREPBUFRNetCDFFile(InputNetCDFStream& istream,bool& foundMap,std::string& mapName,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  size_t n,nhdrs=0;
  std::string sdum;
  Header *array=nullptr;
  netCDFStream::VariableData var_data;
  int obsTypeIndex=-1,idx;
  DateTime base_date_time(30000101235959),date_time;

  initializeForObservations();
  auto attrs=istream.global_attributes();
  sdum="";
  for (n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "MET_tool") {
	sdum=*(reinterpret_cast<std::string *>(attrs[n].values));
    }
    else if (attrs[n].name == "FileOrigins") {
	sdum=*(reinterpret_cast<std::string *>(attrs[n].values));
	if (std::regex_search(sdum,std::regex("PB2NC tool"))) {
	  sdum="pb2nc";
	}
	else {
	  sdum="";
	}
    }
  }
  if (sdum != "pb2nc") {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: missing global attribute 'MET_tool' or invalid value","nc2xml",user,args.argsString);
  }
  auto dims=istream.dimensions();
  for (n=0; n < dims.size(); ++n) {
    if (dims[n].name == "nhdr") {
	nhdrs=dims[n].length;
	array=new Header[nhdrs];
    }
  }
  if (array == nullptr) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not locate 'nhdr' dimension","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("hdr_arr",var_data) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not get 'hdr_arr' data","nc2xml",user,args.argsString);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].lat=var_data[n*3];
    array[n].lon=var_data[n*3+1];
    array[n].elev=var_data[n*3+2];
  }
  if (istream.variable_data("hdr_typ",var_data) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not get 'hdr_typ' data","nc2xml",user,args.argsString);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].type.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
  }
  if (istream.variable_data("hdr_sid",var_data) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not get 'hdr_sid' data","nc2xml",user,args.argsString);
  }
  for (n=0; n < nhdrs; ++n) {
    array[n].ID.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
  }
  if (istream.variable_data("hdr_vld",var_data) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not get 'hdr_vld' data","nc2xml",user,args.argsString);
  }
  for (n=0; n < nhdrs; ++n) {
	array[n].valid_time.assign(&(reinterpret_cast<char *>(var_data.get()))[n*16],16);
	if (array[n].valid_time.length() == 0) {
	  metautils::logError("scanPREPBUFRNetCDFFile returned error: empty value in 'hdr_vld' at element "+strutils::itos(n),"nc2xml",user,args.argsString);
	}
	sdum=strutils::substitute(array[n].valid_time,"_","");
	date_time.set(std::stoll(sdum));
	if (date_time < base_date_time) {
	  base_date_time=date_time;
	}
  }
  if (istream.num_records() == 0) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: no data records found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("obs_arr",var_data) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanPREPBUFRNetCDFFile returned error: could not get 'obs_arr' data","nc2xml",user,args.argsString);
  }
  for (n=0; n < istream.num_records(); ++n) {
    if (var_data[n*5+4] > -9999.) {
	++total_num_not_missing;
	idx=var_data[n*5];
	if (array[idx].type == "ADPUPA") {
	  obsTypeIndex=0;
	  pentry.key="land_station";
	}
	else if (array[idx].type == "AIRCAR" || array[idx].type == "AIRCFT") {
	  obsTypeIndex=0;
	  pentry.key="aircraft";
	}
	else if (array[idx].type == "SATEMP" || array[idx].type == "SATWND") {
	  obsTypeIndex=0;
	  pentry.key="satellite";
	}
	else if (array[idx].type == "PROFLR" || array[idx].type == "RASSDA" || array[idx].type == "VADWND") {
	  obsTypeIndex=0;
	  pentry.key="wind_profiler";
	}
	else if (array[idx].type == "SPSSMI") {
	  pentry.key="satellite";
	}
	else if (array[idx].type == "ADPSFC") {
	  obsTypeIndex=1;
	  pentry.key="land_station";
	}
	else if (array[idx].type == "SFCSHP") {
	  obsTypeIndex=1;
	  if (strutils::is_numeric(array[idx].ID) && array[idx].ID.length() == 5 && array[idx].ID >= "99000") {
	    pentry.key="fixed_ship";
	  }
	  else {
	    pentry.key="roving_ship";
	  }
	}
	else if (array[idx].type == "ASCATW" || array[idx].type == "ERS1DA" || array[idx].type == "QKSWND" || array[idx].type == "SYNDAT" || array[idx].type == "WDSATR") {
	  obsTypeIndex=1;
	  pentry.key="satellite";
	}
	else if (array[idx].type == "SFCBOG") {
	  obsTypeIndex=1;
	  pentry.key="bogus";
	}
	else {
	  metautils::logError("scanPREPBUFRNetCDFFile returned error: unknown observation type '"+array[idx].type+"'","nc2xml",user,args.argsString);
	}
	ientry.key=getPREPBUFRIDKey(metautils::cleanID(array[idx].ID),pentry.key,array[idx].type);
	if (ientry.key.length() == 0) {
	  metautils::logError("scanPREPBUFRNetCDFFile returned error: unable to get ID key for '"+array[idx].type+"', ID: '"+array[idx].ID+"'","nc2xml",user,args.argsString);
	}
	if (array[idx].type == "SPSSMI") {
	  if (strutils::has_ending(ientry.key,"A") || strutils::has_ending(ientry.key,"M") || strutils::has_ending(ientry.key,"S") || strutils::has_ending(ientry.key,"U")) {
	    obsTypeIndex=1;
	  }
	  else {
	    obsTypeIndex=0;
	  }
	}
	if (obsTypeIndex < 0) {
	  metautils::logError("scanPREPBUFRNetCDFFile returned error: unable to get observation type for '"+array[idx].type+"', ID: '"+array[idx].ID+"'","nc2xml",user,args.argsString);
	}
	de.key=strutils::ftos(var_data[n*5+1]);
	sdum=strutils::substitute(array[idx].valid_time,"_","");
	date_time.set(std::stoll(sdum));
	updateIDTable(obsTypeIndex,array[idx].lat,array[idx].lon,date_time,date_time.getSecondsSince(base_date_time));
	updatePlatformTable(obsTypeIndex,array[idx].lat,array[idx].lon);
    }
  }
  write_type=ObML_type;
}

void scanIDDMETARNetCDFFile(InputNetCDFStream& istream,bool foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  size_t n,m;
  int index;
  netCDFStream::VariableData lats,lons,times,report_ids,parent_index,var_data;
  netCDFStream::DataValue time_miss_val,miss_val;
  size_t id_len,num_not_missing;
  int format=-1;
  std::vector<InputNetCDFStream::Attribute> attr;
  std::string sdum,ID,long_name,units;
  DateTime dt;
  metautils::StringEntry se;

  initializeForObservations();
  pentry.key="land_station";
  if (istream.variable_data("latitude",lats) == netCDFStream::NcType::_NULL) {
    if (istream.variable_data("lat",lats) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanIDDMETARNetCDFFile returned error: variable 'latitude' not found","nc2xml",user,args.argsString);
    }
  }
  if (istream.variable_data("longitude",lons) == netCDFStream::NcType::_NULL) {
    if (istream.variable_data("lon",lons) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanIDDMETARNetCDFFile returned error: variable 'longitude' not found","nc2xml",user,args.argsString);
    }
  }
  if (istream.variable_data("time_observation",times) == netCDFStream::NcType::_NULL) {
    if (istream.variable_data("time_obs",times) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanIDDMETARNetCDFFile returned error: variable 'time_observation' not found","nc2xml",user,args.argsString);
    }
  }
  if (istream.variable_data("report_id",report_ids) == netCDFStream::NcType::_NULL) {
    if (istream.variable_data("stn_name",report_ids) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanIDDMETARNetCDFFile returned error: variable 'report_id' not found","nc2xml",user,args.argsString);
    }
    else {
	format=0;
    }
  }
  else {
    if (istream.variable_data("parent_index",parent_index) == netCDFStream::NcType::_NULL) {
	metautils::logError("scanIDDMETARNetCDFFile returned error: variable 'parent_index' not found","nc2xml",user,args.argsString);
    }
    format=1;
  }
  if (times.size() == 0) {
    return;
  }
  id_len=report_ids.size()/times.size();
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (verbose_operation) {
	std::cout << "  netCDF variable: '" << vars[n].name << "'" << std::endl;
    }
    if (std::regex_search(vars[n].name,std::regex("^time_obs"))) {
	if (verbose_operation) {
	  std::cout << "  - time variable is '" << vars[n].name << "'" << std::endl;
	}
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (verbose_operation) {
	    std::cout << "    found attribute: '" << vars[n].attrs[m].name << "'" << std::endl;
	  }
	  if (vars[n].attrs[m].name == "units") {
	    fillNcTimeData(vars[n].attrs[m]);
	  }
	  else if (vars[n].attrs[m].name == "_FillValue") {
	    setTimeMissingValue(time_miss_val,vars[n].attrs,m,times.type());
	  }
	}
    }
    else if (vars[n].is_rec && vars[n].name != "parent_index" && vars[n].name != "prevChild" && !std::regex_search(vars[n].name,std::regex("^report")) && vars[n].name != "rep_type" && vars[n].name != "stn_name" && vars[n].name != "wmo_id" && vars[n].name != "lat" && vars[n].name != "lon" && vars[n].name != "elev" && !std::regex_search(vars[n].name,std::regex("^ob\\_")) && !std::regex_search(vars[n].name,std::regex("^time")) && vars[n].name != "xfields" && vars[n].name != "remarks") {
	if (istream.variable_data(vars[n].name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanIDDMETARNetCDFFile returned error: unable to get data for variable '"+vars[n].name+"'","nc2xml",user,args.argsString);
	}
	attr=vars[n].attrs;
	extractFromVariableAttribute(attr,long_name,units,var_data.type(),miss_val);
	if (verbose_operation) {
	  std::cout << "    - attributes extracted" << std::endl;
	}
	num_not_missing=0;
	for (m=0; m < static_cast<size_t>(times.size()); ++m) {
	  if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
	    ++num_not_missing;
	    ++total_num_not_missing;
	    ID.assign(&(reinterpret_cast<char *>(report_ids.get()))[m*id_len],id_len);
	    strutils::trim(ID);
	    index= (format == 0) ? m : parent_index[m];
	    if (index < static_cast<int>(times.size())) {
		dt=computeNcTime(times,m);
		ientry.key=pentry.key+"[!]callSign[!]"+metautils::cleanID(ID);
		de.key=vars[n].name;
		sdum=dataTypeMap.getDescription(de.key);
		if (!foundMap || sdum.length() == 0) {
		  se.key=vars[n].name+"<!>"+long_name+"<!>"+units;
		  if (!dataTypeTable.found(se.key,se)) {
		    dataTypeTable.insert(se);
		    varList.emplace_back(se.key);
		  }
		}
		if (lats[index] >= -90. && lats[index] <= 90. && lons[index] >= -180. && lons[index] <= 180.) {
		  updateIDTable(1,lats[index],lons[index],dt,times[m]);
		  updatePlatformTable(1,lats[index],lons[index]);
		}
	    }
	  }
	}
	if (verbose_operation) {
	  std::cout << "    - variable data scanned" << std::endl;
	  std::cout << "    - # of non-missing values: " << num_not_missing << std::endl;
	}
    }
  }
}

void scanIDDBuoyNetCDFFile(InputNetCDFStream& istream,bool foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  size_t n,m;
  netCDFStream::VariableData lats,lons,times,ship_ids,buoy_ids,stn_types,var_data;
  netCDFStream::DataValue time_miss_val,miss_val;
  size_t id_len,num_not_missing;
  std::vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  std::string sdum,ID,long_name,units;
  metautils::StringEntry se;

  initializeForObservations();
  if (istream.variable_data("Lat",lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'Lat' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("Lon",lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'Lon' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("time_obs",times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'time_obs' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("ship",ship_ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'ship' not found","nc2xml",user,args.argsString);
  }
  if (times.size() == 0) {
    return;
  }
  id_len=ship_ids.size()/times.size();
  if (istream.variable_data("buoy",buoy_ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'buoy' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("stnType",stn_types) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'stnType' not found","nc2xml",user,args.argsString);
  }
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (verbose_operation) {
	std::cout << "  netCDF variable: '" << vars[n].name << "'" << std::endl;
    }
    if (vars[n].name == "time_obs") {
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (vars[n].attrs[m].name == "units") {
	    fillNcTimeData(vars[n].attrs[m]);
	  }
	  else if (vars[n].attrs[m].name == "_FillValue") {
	    setTimeMissingValue(time_miss_val,vars[n].attrs,m,times.type());
	  }
	}
    }
    else if (vars[n].is_rec && vars[n].name != "rep_type" && vars[n].name != "zone" && vars[n].name != "buoy" && vars[n].name != "ship" && !std::regex_search(vars[n].name,std::regex("^time")) && vars[n].name != "Lat" && vars[n].name != "Lon" && vars[n].name != "stnType" && vars[n].name != "report") {
	if (istream.variable_data(vars[n].name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanIDDBuoyNetCDFFile returned error: unable to get data for variable '"+vars[n].name+"'","nc2xml",user,args.argsString);
	}
	attr=vars[n].attrs;
	extractFromVariableAttribute(attr,long_name,units,var_data.type(),miss_val);
	num_not_missing=0;
	for (m=0; m < static_cast<size_t>(times.size()); ++m) {
	  if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
	    ++num_not_missing;
	    ++total_num_not_missing;
	    ID.assign(&(reinterpret_cast<char *>(ship_ids.get()))[m*id_len],id_len);
	    strutils::trim(ID);
	    if (ID.length() > 0) {
		if (stn_types[m] == 6.) {
		  pentry.key="drifting_buoy";
		}
		else {
		  pentry.key="roving_ship";
		}
		ientry.key=pentry.key+"[!]callSign[!]"+metautils::cleanID(ID);
	    }
	    else {
		pentry.key="drifting_buoy";
		ientry.key=pentry.key+"[!]other[!]"+strutils::itos(buoy_ids[m]);
	    }
	    dt=computeNcTime(times,m);
	    de.key=vars[n].name;
	    sdum=dataTypeMap.getDescription(de.key);
	    if (!foundMap || sdum.length() == 0) {
		se.key=vars[n].name+"<!>"+long_name+"<!>"+units;
		if (!dataTypeTable.found(se.key,se)) {
		  dataTypeTable.insert(se);
		  varList.emplace_back(se.key);
		}
	    }
	    if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
		updateIDTable(1,lats[m],lons[m],dt,times[m]);
		updatePlatformTable(1,lats[m],lons[m]);
	    }
	  }
	}
	if (verbose_operation) {
	  std::cout << "    - variable data scanned" << std::endl;
	  std::cout << "    - # of non-missing values: " << num_not_missing << std::endl;
	}
    }
  }
}

void scanIDDSurfaceSynopticNetCDFFile(InputNetCDFStream& istream,bool foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  size_t n,m;
  netCDFStream::VariableData lats,lons,times,wmo_ids,var_data;
  netCDFStream::DataValue time_miss_val,miss_val;
  size_t num_not_missing;
  std::vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  std::string sdum,ID,long_name,units;
  metautils::StringEntry se;

  initializeForObservations();
  if (istream.variable_data("Lat",lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDSurfaceSynopticNetCDFFile returned error: variable 'Lat' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("Lon",lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDSurfaceSynopticNetCDFFile returned error: variable 'Lon' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("time_obs",times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDSurfaceSynopticNetCDFFile returned error: variable 'time_obs' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("wmoId",wmo_ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDSurfaceSynopticNetCDFFile returned error: variable 'wmoId' not found","nc2xml",user,args.argsString);
  }
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (vars[n].name == "time_obs") {
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (vars[n].attrs[m].name == "units") {
	    fillNcTimeData(vars[n].attrs[m]);
	  }
	  else if (vars[n].attrs[m].name == "_FillValue") {
	    setTimeMissingValue(time_miss_val,vars[n].attrs,m,times.type());
	  }
	}
    }
    else if (vars[n].is_rec && vars[n].name != "rep_type" && vars[n].name != "wmoId" && vars[n].name != "stnName" && !std::regex_search(vars[n].name,std::regex("^time")) && vars[n].name != "Lat" && vars[n].name != "Lon" && vars[n].name != "elev" && vars[n].name != "stnType" && vars[n].name != "report") {
	if (istream.variable_data(vars[n].name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanIDDSurfaceSynopticNetCDFFile returned error: unable to get data for variable '"+vars[n].name+"'","nc2xml",user,args.argsString);
	}
	attr=vars[n].attrs;
	extractFromVariableAttribute(attr,long_name,units,var_data.type(),miss_val);
	num_not_missing=0;
	for (m=0; m < static_cast<size_t>(times.size()); ++m) {
	  if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
	    ++num_not_missing;
	    ++total_num_not_missing;
	    if (wmo_ids[m] < 99000) {
		pentry.key="land_station";
	    }
	    else {
		pentry.key="fixed_ship";
	    }
	    ientry.key=pentry.key+"[!]WMO[!]"+strutils::ftos(wmo_ids[m],5,0,'0');
	    dt=computeNcTime(times,m);
	    de.key=vars[n].name;
	    sdum=dataTypeMap.getDescription(de.key);
	    if (!foundMap || sdum.length() == 0) {
		se.key=vars[n].name+"<!>"+long_name+"<!>"+units;
		if (!dataTypeTable.found(se.key,se)) {
		  dataTypeTable.insert(se);
		  varList.emplace_back(se.key);
		}
	    }
	    if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
		updateIDTable(1,lats[m],lons[m],dt,times[m]);
		updatePlatformTable(1,lats[m],lons[m]);
	    }
	  }
	}
	if (verbose_operation) {
	  std::cout << "    - variable data scanned" << std::endl;
	  std::cout << "    - # of non-missing values: " << num_not_missing << std::endl;
	}
    }
  }
}

void scanIDDUpperAirNetCDFFile(InputNetCDFStream& istream,bool foundMap,DataTypeMap& dataTypeMap,std::list<std::string>& varList)
{
  size_t n,m;
  netCDFStream::VariableData lats,lons,times,wmo_ids,stn_ids,var_data;
  netCDFStream::DataValue time_miss_val,miss_val,wmo_id_miss_val;
  size_t id_len,num_not_missing;
  std::vector<InputNetCDFStream::Attribute> attr;
  DateTime dt;
  std::string sdum,ID,long_name,units;
  metautils::StringEntry se;

  initializeForObservations();
  if (istream.variable_data("staLat",lats) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDUpperAirNetCDFFile returned error: variable 'staLat' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("staLon",lons) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDUpperAirNetCDFFile returned error: variable 'staLon' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("synTime",times) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDUpperAirNetCDFFile returned error: variable 'synTime' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("wmoStaNum",wmo_ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDUpperAirNetCDFFile returned error: variable 'wmoStaNum' not found","nc2xml",user,args.argsString);
  }
  if (istream.variable_data("staName",stn_ids) == netCDFStream::NcType::_NULL) {
    metautils::logError("scanIDDBuoyNetCDFFile returned error: variable 'staName' not found","nc2xml",user,args.argsString);
  }
  if (times.size() == 0) {
    return;
  }
  id_len=stn_ids.size()/times.size();
  auto vars=istream.variables();
  for (n=0; n < vars.size(); ++n) {
    if (verbose_operation) {
	std::cout << "  netCDF variable: '" << vars[n].name << "'" << std::endl;
    }
    if (vars[n].name == "synTime") {
	if (verbose_operation) {
	  std::cout << "  - time variable is '" << vars[n].name << "'; " << vars[n].attrs.size() << " attributes; type: " << static_cast<int>(times.type()) << std::endl;
	}
	for (m=0; m < vars[n].attrs.size(); ++m) {
	  if (verbose_operation) {
	    std::cout << "    found attribute: '" << vars[n].attrs[m].name << "'" << std::endl;
	  }
	  if (vars[n].attrs[m].name == "units") {
	    fillNcTimeData(vars[n].attrs[m]);
	  }
	  else if (vars[n].attrs[m].name == "_FillValue") {
	    setTimeMissingValue(time_miss_val,vars[n].attrs,m,times.type());
	  }
	}
    }
    else if (vars[n].name == "wmoStaNum") {
	attr=vars[n].attrs;
	extractFromVariableAttribute(attr,long_name,units,wmo_ids.type(),wmo_id_miss_val);
    }
    else if (vars[n].is_rec && (vars[n].name == "numMand" || vars[n].name == "numSigT" || vars[n].name == "numSigW" || vars[n].name == "numMwnd" || vars[n].name == "numTrop")) {
	if (istream.variable_data(vars[n].name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanIDDUpperAirNetCDFFile returned error: unable to get data for variable '"+vars[n].name+"'","nc2xml",user,args.argsString);
	}
	attr=vars[n].attrs;
	extractFromVariableAttribute(attr,long_name,units,var_data.type(),miss_val);
	if (verbose_operation) {
	  std::cout << "    - attributes extracted" << std::endl;
	}
	num_not_missing=0;
	for (m=0; m < static_cast<size_t>(times.size()); ++m) {
	  if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
	    ++num_not_missing;
	    ++total_num_not_missing;
	    if (wmo_ids[m] < 99000 || wmo_ids[m] > 99900) {
		pentry.key="land_station";
	    }
	    else {
		pentry.key="fixed_ship";
	    }
	    if (wmo_id_miss_val.type() != netCDFStream::NcType::_NULL) {
		if (wmo_ids[m] == wmo_id_miss_val.get()) {
		  ID.assign(&(reinterpret_cast<char *>(stn_ids.get()))[m*id_len],id_len);
		  strutils::trim(ID);
		  ientry.key=pentry.key+"[!]callSign[!]"+metautils::cleanID(ID);
		}
		else {
		  ientry.key=pentry.key+"[!]WMO[!]"+strutils::ftos(wmo_ids[m],5,0,'0');
		}
	    }
	    else {
		ientry.key=pentry.key+"[!]WMO[!]"+strutils::ftos(wmo_ids[m],5,0,'0');
	    }
	    dt=computeNcTime(times,m);
	    de.key=vars[n].name;
	    sdum=dataTypeMap.getDescription(de.key);
	    if (!foundMap || sdum.length() == 0) {
		se.key=vars[n].name+"<!>"+long_name+"<!>"+units;
		if (!dataTypeTable.found(se.key,se)) {
		  dataTypeTable.insert(se);
		  varList.emplace_back(se.key);
		}
	    }
	    if (lats[m] >= -90. && lats[m] <= 90. && lons[m] >= -180. && lons[m] <= 180.) {
		updateIDTable(0,lats[m],lons[m],dt,times[m]);
		updatePlatformTable(0,lats[m],lons[m]);
	    }
	  }
	}
	if (verbose_operation) {
	  std::cout << "    - variable data scanned" << std::endl;
	  std::cout << "    - # of non-missing values: " << num_not_missing << std::endl;
	}
    }
  }
}

void scanIDDObservationNetCDFFile(InputNetCDFStream& istream,bool& foundMap,std::string& mapName,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  size_t n;
  std::string sdum,type;
  std::deque<std::string> sp;
  DataTypeMap dataTypeMap;

  auto attrs=istream.global_attributes();
  for (n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "title") {
	sdum=*(reinterpret_cast<std::string *>(attrs[n].values));
	sp=strutils::split(sdum);
	type=sp[0];
    }
  }
  mapName=getRemoteWebFile("https://rda.ucar.edu/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
  if (dataTypeMap.fill(mapName)) {
    foundMap=true;
  }
  else {
    foundMap=false;
  }
  if (type == "METAR") {
    scanIDDMETARNetCDFFile(istream,foundMap,dataTypeMap,varList);
  }
  else if (type == "BUOY") {
    scanIDDBuoyNetCDFFile(istream,foundMap,dataTypeMap,varList);
  }
  else if (type == "SYNOPTIC") {
    scanIDDSurfaceSynopticNetCDFFile(istream,foundMap,dataTypeMap,varList);
  }
  else {
    scanIDDUpperAirNetCDFFile(istream,foundMap,dataTypeMap,varList);
  }
  write_type=ObML_type;
}

void scanSAMOSNetCDFFile(InputNetCDFStream& istream,DataTypeMap& dataTypeMap,bool& foundMap,std::list<std::string>& varList,my::map<metautils::StringEntry>& changedVarTable)
{
  std::string timevarname,latvarname,lonvarname;
  netCDFStream::VariableData times,lats,lons;
  std::forward_list<std::string> vnames;
  my::map<metautils::StringEntry> unique_var_table;
  bool found_time,found_lat,found_lon;

  found_time=found_lat=found_lon=false;
  initializeForObservations();
  pentry.key="roving_ship";
  ientry.key="";
  auto attrs=istream.global_attributes();
  for (size_t n=0; n < attrs.size(); ++n) {
    if (attrs[n].name == "ID") {
	ientry.key=pentry.key+"[!]callSign[!]"+*(reinterpret_cast<std::string *>(attrs[n].values));
    }
  }
  auto vars=istream.variables();
// find the coordinate variables
  for (size_t n=0; n < vars.size(); ++n) {
    for (size_t m=0; m < vars[n].attrs.size(); ++m) {
	if (vars[n].attrs[m].nc_type == netCDFStream::NcType::CHAR && vars[n].attrs[m].name == "units") {
	  std::string sdum=strutils::to_lower(*(reinterpret_cast<std::string *>(vars[n].attrs[m].values)));
	  if (vars[n].is_coord && std::regex_search(sdum,std::regex("since"))) {
	    if (found_time) {
		metautils::logError("scanSAMOSNetCDFFile returned error: time was already identified - don't know what to do with variable: "+vars[n].name,"nc2xml",user,args.argsString);
	    }
	    fillNcTimeData(vars[n].attrs[m]);
	    found_time=true;
	    timevarname=vars[n].name;
	    if (istream.variable_data(vars[n].name,times) == netCDFStream::NcType::_NULL) {
		metautils::logError("scanSAMOSNetCDFFile returned error: unable to get times","nc2xml",user,args.argsString);
	    }
	    if (times.size() == 0) {
		found_time=false;
	    }
	  }
	  else if (vars[n].dimids.size() == 1 && vars[n].dimids[0] == 0) {
	    if (sdum == "degrees_north") {
		if (istream.variable_data(vars[n].name,lats) == netCDFStream::NcType::_NULL) {
		  metautils::logError("scanSAMOSNetCDFFile returned error: unable to get latitudes","nc2xml",user,args.argsString);
		}
		latvarname=vars[n].name;
		found_lat=true;
	    }
	    else if (sdum == "degrees_east") {
		if (istream.variable_data(vars[n].name,lons) == netCDFStream::NcType::_NULL) {
		  metautils::logError("scanSAMOSNetCDFFile returned error: unable to get longitudes","nc2xml",user,args.argsString);
		}
		lonvarname=vars[n].name;
		found_lon=true;
	    }
	  }
	}
    }
  }
  if (!found_time) {
    metautils::logError("scanSAMOSNetCDFFile could not find the 'time' variable","nc2xml",user,args.argsString);
  }
  else if (!found_lat) {
    metautils::logError("scanSAMOSNetCDFFile could not find the 'latitude' variable","nc2xml",user,args.argsString);
  }
  else if (!found_lon) {
    metautils::logError("scanSAMOSNetCDFFile could not find the 'longitude' variable","nc2xml",user,args.argsString);
  }
  else if (ientry.key.length() == 0) {
    metautils::logError("scanSAMOSNetCDFFile could not find the vessel ID","nc2xml",user,args.argsString);
  }
  metadata::openInventory(inv,&inv_file,"nc2xml",user);
  if (inv.is_open()) {
    inv << "netCDF:point|" << istream.record_size() << std::endl;
    if (O_map.find("surface") == O_map.end()) {
	O_map.emplace("surface",std::make_pair(O_map.size(),""));
    }
    if (P_map.find(pentry.key) == P_map.end()) {
	P_map.emplace(pentry.key,std::make_pair(P_map.size(),""));
    }
  }
// find the data variables
  total_num_not_missing=0;
  std::unordered_map<size_t,std::string> T_map;
  float min_lat=99.,max_lat=-99.;
  for (size_t n=0; n < vars.size(); ++n) {
    if (vars[n].name != timevarname && vars[n].name != latvarname && vars[n].name != lonvarname) {
	std::string long_name,units;
	netCDFStream::DataValue time_miss_val,miss_val;
	netCDFStream::VariableData var_data;
	extractFromVariableAttribute(vars[n].attrs,long_name,units,vars[n].nc_type,miss_val);
	if (istream.variable_data(vars[n].name,var_data) == netCDFStream::NcType::_NULL) {
	  metautils::logError("scanSAMOSNetCDFFile returned error: unable to get data for variable '"+vars[n].name+"'","nc2xml",user,args.argsString);
	}
	if (!foundMap || dataTypeMap.getDescription(vars[n].name).length() == 0) {
	  metautils::StringEntry se;
	  se.key=vars[n].name+"<!>"+long_name+"<!>"+units;
	  if (!unique_var_table.found(se.key,se)) {
	    varList.emplace_back(se.key);
	    unique_var_table.insert(se);
	  }
	}
	if (D_map.find(vars[n].name) == D_map.end()) {
	  auto bsize=1;
	  auto dims=istream.dimensions();
	  for (size_t l=1; l < vars[n].dimids.size(); ++l) {
	    bsize*=dims[vars[n].dimids[l]].length;
	  }
	  switch (vars[n].nc_type) {
	    case netCDFStream::NcType::SHORT:
	    {
		bsize*=2;
		break;
	    }
	    case netCDFStream::NcType::INT:
	    case netCDFStream::NcType::FLOAT:
	    {
		bsize*=4;
		break;
	    }
	    case netCDFStream::NcType::DOUBLE:
	    {
		bsize*=8;
		break;
	    }
	    default: {}
	  }
	  D_map.emplace(vars[n].name,std::make_pair(D_map.size(),"|"+strutils::lltos(vars[n].offset)+"|"+netCDFStream::nc_type[static_cast<int>(vars[n].nc_type)]+"|"+strutils::itos(bsize)));
	}
	std::list<std::string> miss_lines_list;
	for (size_t m=0; m < times.size(); ++m) {
	  if (!found_missing(times[m],time_miss_val,var_data[m],miss_val)) {
	    ++total_num_not_missing;
	    float lon=lons[m];
	    if (lon > 180.) {
		lon-=360.;
	    }
	    DateTime dt=time_data.reference.added(time_data.units,times[m]);
	    if (inv.is_open()) {
		if (T_map.find(m) == T_map.end()) {
		  T_map.emplace(m,dt.toString("%Y%m%d%H%MM")+"[!]"+strutils::ftos(lats[m],4)+"[!]"+strutils::ftos(lon,4));
		}
	    }
	    de.key=vars[n].name;
	    updateIDTable(1,lats[m],lon,dt,times[m]);
	    updatePlatformTable(1,lats[m],lon);
	    if (lats[m] < min_lat) {
		min_lat=lats[m];
	    }
	    if (lats[m] > max_lat) {
		max_lat=lats[m];
	    }
	  }
	  else {
	    if (inv.is_open()) {
		std::string sdum=strutils::itos(m);
		sdum+="|0|"+strutils::itos(P_map[pentry.key].first)+"|"+strutils::itos(I_map[ientry.key.substr(ientry.key.find("[!]")+3)].first)+"|"+strutils::itos(D_map[vars[n].name].first);
		miss_lines_list.emplace_back(sdum);
	    }
	  }
	}
	if (inv.is_open()) {
	  if (miss_lines_list.size() != times.size()) {
	    for (const auto& line : miss_lines_list) {
		inv_lines2.writeln(line);
	    }
	  }
	  else {
	    D_map.erase(vars[n].name);
	  }
	}
    }
  }
  write_type=ObML_type;
  if (inv.is_open()) {
    size_t w_index,e_index;
    bitmap::longitudeBitmap::getWestEastBounds(ientry.data->min_lon_bitmap.get(),w_index,e_index);
    auto I_key=ientry.key.substr(ientry.key.find("[!]")+3)+"[!]"+strutils::ftos(min_lat,4)+"[!]"+strutils::ftos(ientry.data->min_lon_bitmap[w_index],4)+"[!]"+strutils::ftos(max_lat,4)+"[!]"+strutils::ftos(ientry.data->max_lon_bitmap[e_index],4);
    if (I_map.find(I_key) == I_map.end()) {
	I_map.emplace(I_key,std::make_pair(I_map.size(),""));
    }
    std::vector<size_t> time_indexes;
    for (const auto& e : T_map) {
	time_indexes.emplace_back(e.first);
    }
    std::sort(time_indexes.begin(),time_indexes.end(),
    [](const size_t& left,const size_t& right) -> bool
    {
	if (left <= right) {
	  return true;
	}
	else {
	  return false;
	}
    });
    for (const auto& idx : time_indexes) {
	inv << "T<!>" << idx << "<!>" << T_map[idx] << std::endl;
    }
  }
}

void scanFile()
{
  InputNetCDFStream istream;
  std::string file_format,output,error,map_type;
  std::deque<std::string> sp;
  std::list<std::string> filelist;
  std::string tempDir;
  metautils::StringEntry se;
  std::list<std::string> varList,map_contents;
  my::map<metautils::StringEntry> changedVarTable;
  std::vector<netCDFStream::Attribute> gattrs;
  bool no_write;

  tfile=new TempFile;
  tfile->open(args.temp_loc);
  tdir=new TempDir;
  tdir->create(args.temp_loc);
  DataTypeMap dataTypeMap;
  bool foundMap;
  std::string mapName=getRemoteWebFile("https://rda.ucar.edu/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",tdir->name());
  if (dataTypeMap.fill(mapName)) {
    foundMap=true;
  }
  else {
    foundMap=false;
  }
  if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","nc2xml",user,args.argsString);
  }
  if (filelist.size() == 0) {
    filelist.emplace_back(tfile->name());
  }
  if (verbose_operation) {
    std::cout << "Ready to scan " << filelist.size() << " files." << std::endl;
  }
  for (const auto& file : filelist) {
    if (verbose_operation) {
	std::cout << "Beginning scan of " << file << "..." << std::endl;
    }
    if (tempDir.length() == 0) {
	tempDir=file;
	if (strutils::occurs(tempDir,"/") > strutils::occurs(args.temp_loc,"/")+1) {
	  tempDir=tempDir.substr(0,tempDir.rfind("/"));
	}
	else {
	  tempDir="";
	}
    }
    istream.open(file.c_str());
    if (std::regex_search(args.format,std::regex("^cfnetcdf"))) {
	scanCFNetCDFFile(istream,foundMap,mapName,varList,changedVarTable);
    }
    else if (std::regex_search(args.format,std::regex("^pbnetcdf"))) {
	scanPREPBUFRNetCDFFile(istream,foundMap,mapName,varList,changedVarTable);
    }
    else if (std::regex_search(args.format,std::regex("^rafnetcdf"))) {
	scanRAFAircraftNetCDFFile(istream,foundMap,mapName,varList,changedVarTable);
    }
    else if (std::regex_search(args.format,std::regex("^iddnetcdf"))) {
	scanIDDObservationNetCDFFile(istream,foundMap,mapName,varList,changedVarTable);
    }
    else if (std::regex_search(args.format,std::regex("^samosnc"))) {
	scanSAMOSNetCDFFile(istream,dataTypeMap,foundMap,varList,changedVarTable);
    }
/*
    else if (args.format.beginsWith("wrfsimnetcdf"))
	scanWRFSimulationNetCDFFile(istream,foundMap,mapName,varList,changedVarTable);
*/
    else {
	metautils::logError(args.format+"-formatted files not recognized","nc2xml",user,args.argsString);
    }
    istream.close();
    if (verbose_operation) {
	std::cout << "  ...scan of " << file << " completed." << std::endl;
    }
  }
  args.format="netcdf";
  if (!args.inventoryOnly && varList.size() > 0) {
    if (verbose_operation) {
	std::cout << "Writing parameter map..." << std::endl;
    }
    if (write_type == GrML_type) {
	map_type="parameter";
    }
    else if (write_type == ObML_type) {
	map_type="dataType";
    }
    else {
	metautils::logError("scanFile returned error: unknown map type","nc2xml",user,args.argsString);
    }
    if (foundMap) {
	std::ifstream ifs;
	char line[32768];
	ifs.open(mapName.c_str());
	ifs.getline(line,32768);
	while (!ifs.eof()) {
	  map_contents.emplace_back(line);
	  ifs.getline(line,32768);
	}
	ifs.close();
	map_contents.pop_back();
    }
    else {
	mapName=tdir->name()+"/netCDF.ds"+args.dsnum+".xml";
    }
    std::ofstream ofs;
    ofs.open(mapName.c_str());
    if (!ofs.is_open()) {
	metautils::logError("scanFile returned error: can't open parameter map file for output","nc2xml",user,args.argsString);
    }
    if (!foundMap) {
	ofs << "<?xml version=\"1.0\" ?>" << std::endl;
	ofs << "<" << map_type << "Map>" << std::endl;
    }
    else {
	no_write=false;
	for (const auto& line : map_contents) {
	  if (std::regex_search(line,std::regex(" code=\""))) {
	    sp=strutils::split(line,"\"");
	    se.key=sp[1];
	    if (changedVarTable.found(se.key,se)) {
		no_write=true;
	    }
	  }
	  if (!no_write) {
	    ofs << line << std::endl;
	  }
	  if (std::regex_search(line,std::regex("</"+map_type+">"))) {
	    no_write=false;
	  }
	}
    }
    for (const auto& var : varList) {
	sp=strutils::split(var,"<!>");
	if (write_type == GrML_type) {
	  ofs << "  <parameter code=\"" << sp[0] << "\">" << std::endl;
	  ofs << "    <shortName>" << sp[0] << "</shortName>" << std::endl;
	  if (sp[1].length() > 0) {
	    ofs << "    <description>" << sp[1] << "</description>" << std::endl;
	  }
	  if (sp[2].length() > 0) {
	    ofs << "    <units>" << strutils::substitute(sp[2],"-","^-") << "</units>" << std::endl;
	  }
	  ofs << "  </parameter>" << std::endl;
	}
	else if (write_type == ObML_type) {
	  ofs << "  <dataType code=\"" << sp[0] << "\">" << std::endl;
	  ofs << "    <description>" << sp[1];
	  if (sp[2].length() > 0) {
	    ofs << " (" << sp[2] << ")";
	  }
	  ofs << "</description>" << std::endl;
	  ofs << "  </dataType>" << std::endl;
	}
    }
    if (write_type == GrML_type) {
	ofs << "</parameterMap>" << std::endl;
    }
    else if (write_type == ObML_type) {
	ofs << "</dataTypeMap>" << std::endl;
    }
    ofs.close();
    if (hostSync(mapName,"/__HOST__/web/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",error) < 0) {
	metautils::logWarning("parameter map was not synced - error(s): '"+error+"'","nc2xml",user,args.argsString);
    }
    std::stringstream oss,ess;
    mysystem2("/bin/cp "+mapName+" /glade/u/home/rdadata/share/metadata/ParameterTables/netCDF.ds"+args.dsnum+".xml",oss,ess);
    if (verbose_operation) {
	std::cout << "...finished writing parameter map." << std::endl;
    }
  }
}

extern "C" void segv_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
  metautils::logError("core dump","nc2xml",user,args.argsString);
}

extern "C" void int_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  std::ifstream ifs;
  char line[32768];
  std::string metadata_file,wfile,flags,ext;

  if (argc < 4) {
    std::cerr << "usage: nc2xml -f format -d [ds]nnn.n [options...] path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f cfnetcdf      Climate and Forecast compliant netCDF3 data" << std::endl;
    std::cerr << "-f iddnetcdf     Unidata IDD netCDF3 station data" << std::endl;
    std::cerr << "-f pbnetcdf      NetCDF3 converted from prepbufr" << std::endl;
    std::cerr << "-f rafnetcdf     NCAR-RAF/nimbus compliant netCDF3 aircraft data" << std::endl;
    std::cerr << "-f samosnc       SAMOS netCDF3 data" << std::endl;
//    std::cerr << "-f wrfsimnetcdf  Climate Simulations from WRF" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    if (user == "dattore")
	std::cerr << "-NC              don't check to see if the MSS file is a primary for the dataset" << std::endl;
    std::cerr << "-l <local_name>  name of the MSS file on local disk (this avoids an MSS read)" << std::endl;
    if (user == "dattore") {
	std::cerr << "-r/-R            regenerate/don't regenerate the dataset webpage" << std::endl;
	std::cerr << "-s/-S            do/don't update the dataset summary information (default is -s)" << std::endl;
	std::cerr << "-u/-U            do/don't update the database (default is -u)" << std::endl;
	std::cerr << "-t <path>        path where temporary files should be created" << std::endl;
      std::cerr << "-OO              overwrite only - when content metadata already exists, the" << std::endl;
      std::cerr << "                 default is to first delete existing metadata; this option saves" << std::endl;
      std::cerr << "                 time by overwriting without the delete" << std::endl;
    }
    std::cerr << "-V               verbose operation" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>           full MSS path or URL of the file to read" << std::endl;
    std::cerr << "                 - MSS paths must begin with \"/FS/DSS\" or \"/DSS\"" << std::endl;
    std::cerr << "                 - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.argsString=getUnixArgsString(argc,argv,'%');
  metautils::readConfig("nc2xml",user,args.argsString);
  parseArgs();
  flags="-f";
  if (!args.inventoryOnly && std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(cleanUp);
  metautils::cmd_register("nc2xml",user);
  if (!args.overwriteOnly && !args.inventoryOnly) {
    metautils::checkForExistingCMD("GrML");
    metautils::checkForExistingCMD("ObML");
  }
  scanFile();
  if (verbose_operation && !args.inventoryOnly) {
    std::cout << "Writing XML..." << std::endl;
  }
  if (write_type == GrML_type) {
    ext="GrML";
    if (!args.inventoryOnly) {
	metadata::GrML::writeGrML(grid_table,"nc2xml",user);
    }
  }
  else if (write_type == ObML_type) {
    ext="ObML";
    if (!args.inventoryOnly) {
	if (total_num_not_missing > 0) {
	  metadata::ObML::writeObML(ID_table,platform_table,"nc2xml",user);
	}
	else {
	  metautils::logError("all variables contain only missing data values - no usable data found; no content metadata will be saved for this file","nc2xml",user,args.argsString);
	}
    }
  }
  if (verbose_operation && !args.inventoryOnly) {
    std::cout << "...finished writing XML." << std::endl;
  }
  if (args.updateDB) {
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (verbose_operation) {
	std::cout << "Calling 'scm' to update the database..." << std::endl;
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+"."+ext,oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (verbose_operation) {
	std::cout << "...'scm' finished." << std::endl;
    }
  }
  if (inv.is_open()) {
    std::vector<std::pair<int,std::string>> sorted_keys;
    if (write_type == GrML_type) {
	sortInventoryMap(U_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "U<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
	sortInventoryMap(G_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "G<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
	sortInventoryMap(L_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "L<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
	sortInventoryMap(P_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "P<!>" << s_key.first << "<!>" << s_key.second;
	  if (is_large_offset) {
	    inv << "<!>BIG";
	  }
	  inv << std::endl;
	}
	sortInventoryMap(R_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "R<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
    }
    else if (write_type == ObML_type) {
	sortInventoryMap(O_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "O<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
	sortInventoryMap(P_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "P<!>" << s_key.first << "<!>" << s_key.second << std::endl;
	}
	sortInventoryMap(I_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "I<!>" << s_key.first << "<!>" << s_key.second << "[!]" << I_map[s_key.second].second << std::endl;
	}
	sortInventoryMap(D_map,sorted_keys);
	for (const auto& s_key : sorted_keys) {
	  inv << "D<!>" << s_key.first << "<!>" << s_key.second << D_map[s_key.second].second << std::endl;
	}
    }
    inv << "-----" << std::endl;
    if (inv_lines.size() > 0) {
	for (const auto& line : inv_lines) {
	  inv << line << std::endl;
	}
    }
    else {
	inv_lines2.close();
	ifs.open(inv_lines2.name().c_str());
	if (ifs.is_open()) {
	  ifs.getline(line,32768);
	  while (!ifs.eof()) {
	    inv << line << std::endl;
	    ifs.getline(line,32768);
	  }
	  ifs.close();
	}
    }
    metadata::closeInventory(inv,inv_file,ext,true,args.updateSummary,"nc2xml",user);
  }
  if (unknown_IDs.size() > 0) {
    std::stringstream ss;
    for (const auto& id : unknown_IDs) {
	ss << id << std::endl;
    }
    metautils::logWarning("unknown ID(s):\n"+ss.str(),"nc2xml",user,args.argsString);
  }
  return 0;
}
