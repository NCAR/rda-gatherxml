#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <ftw.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <regex>
#include <adpstream.hpp>
#include <observation.hpp>
#include <adpobs.hpp>
#include <marine.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <surface.hpp>
#include <td32.hpp>
#include <raob.hpp>
#include <myerror.hpp>

metautils::Directives meta_directives;
metautils::Args meta_args;
std::string myerror="";
std::string mywarning="";

my::map<metadata::ObML::IDEntry> **id_table;
metadata::ObML::IDEntry ientry;
my::map<metadata::ObML::PlatformEntry> platform_table[metadata::ObML::NUM_OBS_TYPES];
metadata::ObML::PlatformEntry pentry;
std::string user=getenv("USER");
TempFile *tfile;
std::string inv_file;
TempDir *tdir,*inv_dir=nullptr;
std::ofstream inv_stream;
size_t num_not_missing=0;
bool verbose_operation=false;

extern "C" void clean_up()
{
  if (tfile != NULL) {
    delete tfile;
    tfile=NULL;
  }
  if (tdir != NULL) {
    delete tdir;
    tdir=NULL;
  }
  if (myerror.length() > 0) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::log_error(myerror,"obs2xml",user);
  }
}

void parse_args()
{
  size_t n;

  meta_args.update_db=true;
  meta_args.update_graphics=true;
  meta_args.update_summary=true;
  meta_args.override_primary_check=false;
  meta_args.temp_loc=meta_directives.temp_path;
  std::deque<std::string> sp=strutils::split(meta_args.args_string,"!");
  for (n=0; n < sp.size()-1; n++) {
    if (sp[n] == "-f") {
	meta_args.data_format=sp[++n];
    }
    else if (sp[n] == "-d") {
	meta_args.dsnum=sp[++n];
	if (std::regex_search(meta_args.dsnum,std::regex("^ds"))) {
	  meta_args.dsnum=meta_args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-G") {
	meta_args.update_graphics=false;
    }
    else if (sp[n] == "-I") {
	meta_args.inventory_only=true;
	meta_args.update_db=false;
    }
    else if (sp[n] == "-l") {
	meta_args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	meta_args.member_name=sp[++n];
    }
    else if (sp[n] == "-R") {
	meta_args.regenerate=false;
    }
    else if (sp[n] == "-S") {
	meta_args.update_summary=false;
    }
    else if (sp[n] == "-U") {
	meta_args.update_db=false;
    }
    else if (sp[n] == "-V") {
	verbose_operation=true;
    }
    else if (sp[n] == "-NC") {
	if (user == "dattore") {
	  meta_args.override_primary_check=true;
	}
    }
    else {
	std::cerr << "Error: bad argument " << sp[n] << std::endl;
	exit(1);
    }
  }
  if (meta_args.data_format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    meta_args.data_format=strutils::to_lower(meta_args.data_format);
  }
  if (meta_args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
  if (meta_args.dsnum == "999.9") {
    meta_args.override_primary_check=true;
    meta_args.update_db=false;
    meta_args.update_summary=false;
    meta_args.regenerate=false;
  }
  n=sp.back().rfind("/");
  meta_args.path=sp.back().substr(0,n);
  meta_args.filename=sp.back().substr(n+1);
}

size_t isd_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  ISDObservation *o=reinterpret_cast<ISDObservation *>(obs);
  std::string rpt_type=o->report_type();

  if (rpt_type == "FM-12" || rpt_type == "FM-15" || rpt_type == "FM-16" || rpt_type == "AUTO " || rpt_type == "SAO  " || rpt_type == "SAOSP" || rpt_type == "SY-AE" || rpt_type == "SY-SA" || rpt_type == "SY-MT" || rpt_type == "S-S-A" || rpt_type == "SMARS" || rpt_type == "AERO " || rpt_type == "NSRDB" || rpt_type == "SURF " || rpt_type == "MEXIC" || rpt_type == "BRAZ " || rpt_type == "AUST ") {
    pentry.key="land_station";
  }
  else if (rpt_type == "FM-13") {
    pentry.key="roving_ship";
  }
  else if (rpt_type == "FM-18") {
    pentry.key="drifting_buoy";
  }
  else if (rpt_type == "99999") {
    pentry.key="unknown";
  }
  else {
    metautils::log_error("no platform and station mapping for report type '"+rpt_type+"'","obs2xml",user);
  }
  start_date=end_date=o->date_time();
  std::deque<std::string> sp=strutils::split(o->location().ID,"-");
  if (sp[0] != "999999") {
    ientry_key=pentry.key+"[!]WMO+6[!]"+sp[0];
  }
  else if (sp[1] != "99999") {
    ientry_key=pentry.key+"[!]WBAN[!]"+sp[1];
  }
  else {
    strutils::trim(sp[2]);
    ientry_key=pentry.key+"[!]callSign[!]"+sp[2];
  }
  obs_type_index=1;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t adp_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  ADPObservation *o=reinterpret_cast<ADPObservation *>(obs);

  switch (o->platform_type()) {
    case 11:
    case 12:
    case 13:
    case 511:
    case 512:
    case 513:
	pentry.key="land_station";
	break;
    case 21:
    case 521:
	pentry.key="ocean_station";
	break;
    case 22:
    case 23:
    case 522:
    case 523:
	pentry.key="roving_ship";
	break;
    case 31:
    case 41:
	pentry.key="aircraft";
	break;
    case 42:
	pentry.key="balloon";
	break;
    case 51:
    case 551:
	pentry.key="bogus";
	break;
    case 61:
    case 62:
    case 63:
	pentry.key="satellite";
	break;
    case 531:
    case 532:
	pentry.key="CMAN_station";
	break;
    case 561:
	pentry.key="moored_buoy";
	break;
    case 562:
	pentry.key="drifting_buoy";
	break;
    case 999:
	pentry.key="999";
	break;
    default:
	std::cerr << "Warning: unknown platform type " << o->platform_type() << " for ID '" << obs->location().ID << "'" << std::endl;
	pentry.key="";
  }
  start_date=end_date=o->synoptic_date_time();
  if (start_date.year() == 0) {
    pentry.key="";
  }
  if (pentry.key.length() > 0) {
    if (obs->location().ID.length() == 5 && strutils::is_numeric(obs->location().ID) && obs->location().ID >= "01001" && obs->location().ID <= "99999") {
	ientry_key=pentry.key+"[!]WMO[!]"+obs->location().ID;
    }
    else {
	ientry_key=obs->location().ID;
	for (size_t n=0; n < ientry_key.length(); ++n) {
	  if (static_cast<int>(ientry_key[n]) < 32 || static_cast<int>(ientry_key[n]) > 127) {
	    if (n > 0) {
		ientry_key=ientry_key.substr(0,n)+"/"+ientry_key.substr(n+1);
	    }
	    else {
		ientry_key="/"+ientry_key.substr(1);
	    }
	  }
	}
	if (std::regex_search(ientry.key,std::regex("&"))) {
	  strutils::replace_all(ientry_key,"&","&amp;");
	}
	if (std::regex_search(ientry.key,std::regex(">"))) {
	  strutils::replace_all(ientry_key,">","&gt;");
	}
	if (std::regex_search(ientry.key,std::regex("<"))) {
	  strutils::replace_all(ientry_key,"<","&lt;");
	}
	ientry_key=strutils::to_upper(ientry_key);
	ientry_key=pentry.key+"[!]callSign[!]"+ientry_key;
    }
    std::list<size_t> ADPcategories=o->categories();
    if (pentry.key == "999") {
	for (auto& item : ADPcategories) {
	  if (item == 6) {
	    pentry.key="aircraft";
	  }
	}
    }
    if (pentry.key == "999") {
//	std::cerr << "Warning: unknown platform type 999 for ID '" << obs->location().ID << "'" << std::endl;
	pentry.key="";
    }
    if (meta_args.data_format == "on29") {
	obs_type_index=0;
    }
    else {
	obs_type_index=1;
    }
  }
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t cpc_summary_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  std::string ID=obs->location().ID;

  pentry.key="land_station";
  start_date=end_date=obs->date_time();
  if (start_date.day() == 0) {
    start_date.set_day(1);
    end_date.set_day(dateutils::days_in_month(end_date.year(),end_date.month()));
  }
  if (ID.length() == 5) {
    ientry_key=pentry.key+"[!]WMO[!]"+ID;
  }
  else {
    ientry_key=pentry.key+"[!]callSign[!]"+ID;
  }
  obs_type_index=1;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

std::string clean_imma_id(std::string id)
{
  if (id.empty()) {
    return id;
  }
  std::string clean_id=id;
  strutils::trim(clean_id);
  strutils::replace_all(clean_id,"\"","'");
  strutils::replace_all(clean_id,"&","&amp;");
  strutils::replace_all(clean_id,">","&gt;");
  strutils::replace_all(clean_id,"<","&lt;");
  auto sp=strutils::split(clean_id);
  return strutils::to_upper(sp[0]);
}

size_t imma_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  IMMAObservation *o=reinterpret_cast<IMMAObservation *>(obs);
  auto id=clean_imma_id(obs->location().ID);
  if (id.empty()) {
    return 0;
  }
  switch (o->platform_type().atti) {
    case 1:
    {
	switch (o->platform_type().code) {
	  case 0:
	  case 1:
	  case 5:
	  {
	    pentry.key="roving_ship";
	    break;
	  }
	  case 2:
	  case 3:
	  {
	    pentry.key="ocean_station";
	    break;
	  }
	  case 4:
	  {
	    pentry.key="lightship";
	    break;
	  }
	  case 6:
	  {
	    pentry.key="moored_buoy";
	    break;
	  }
	  case 7:
	  {
	    pentry.key="drifting_buoy";
	    break;
	  }
	  case 9:
	  {
	    pentry.key="ice_station";
	    break;
	  }
	  case 10:
	  case 11:
	  case 12:
	  case 17:
	  case 18:
	  case 19:
	  case 20:
	  case 21:
	  {
	    pentry.key="oceanographic";
	    break;
	  }
	  case 13:
	  {
	    pentry.key="CMAN_station";
	    break;
	  }
	  case 14:
	  {
	    pentry.key="coastal_station";
	    break;
	  }
	  case 15:
	  {
	    pentry.key="fixed_ocean_platform";
	    break;
	  }
	  case 16:
	  {
	    pentry.key="automated_gauge";
	    break;
	  }
	  default:
	  {
	    switch (o->ID_type()) {
		case 3:
		case 4:
		{
		  pentry.key="drifting_buoy";
		  break;
		}
		case 7:
		{
		  pentry.key="roving_ship";
		  break;
		}
		default:
		{
		  metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized - obs date: "+obs->date_time().to_string()+", ID type: "+strutils::itos(o->ID_type()),"obs2xml",user);
		}
	    }
	  }
	}
	break;
    }
    case 2:
    {
	switch (o->platform_type().code) {
	  case 6:
	  {
	    pentry.key="CMAN_station";
	    break;
	  }
	  default:
	  {
	    metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",user);
	  }
	}
	break;
    }
    default:
    {
	metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",user);
    }
  }
  start_date=end_date=obs->date_time();
  if (start_date.day() < 1 || static_cast<size_t>(start_date.day()) > dateutils::days_in_month(start_date.year(),start_date.month()) || start_date.time() > 235959) {
    return 0;
  }
  ientry_key=pentry.key+"[!]";
  switch (o->ID_type()) {
    case 0:
	ientry_key+="unknown";
	break;
    case 1:
	ientry_key+="callSign";
	break;
    case 2:
	ientry_key+="generic";
	break;
    case 3:
	ientry_key+="WMO";
	break;
    case 4:
	ientry_key+="buoy";
	break;
    case 5:
	ientry_key+="NDBC";
	break;
    case 6:
	if (strutils::is_numeric(id))
	  ientry_key+="number";
	else
	  ientry_key+="name";
	break;
    case 7:
	ientry_key+="NODC";
	break;
    case 8:
	ientry_key+="IATTC";
	break;
    case 9:
	ientry_key+="number";
	break;
    case 10:
      if (id.length() == 8) {
	  if (strutils::is_numeric(id)) {
	    id=id.substr(0,4);
	    ientry_key+="number";
	  }
	  else if (strutils::is_numeric(id.substr(2)) && id[0] >= 'A' && id[0] <= 'Z' && id[1] >= 'A' && id[1] <= 'Z') {
	    id=id.substr(0,2);
	    ientry_key+="generic";
	  }
	  else
	    ientry_key+="name";
      }
      else {
	  if (strutils::is_numeric(id)) {
	    ientry_key+="number";
	  }
	  else {
	    ientry_key+="name";
	  }
	}
	break;
    default:
	metautils::log_error("ID type "+strutils::itos(o->ID_type())+" not recognized - obs date: "+obs->date_time().to_string(),"obs2xml",user);
  }
  ientry_key+="[!]"+id;
  obs_type_index=1;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t nodc_bt_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  pentry.key="roving_ship";
  start_date=end_date=obs->date_time();
  ientry_key=pentry.key+"[!]NODC[!]"+obs->location().ID;
  obs_type_index=2;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t td32_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  TD32Data *o=reinterpret_cast<TD32Data *>(obs);
  static my::map<metadata::ObML::DataTypeEntry> unique_observation_table(999999);
  metadata::ObML::DataTypeEntry de;
  size_t nsteps=0;
  std::string id=obs->location().ID;
  float last_lat=999.,last_lon=9999.;

  pentry.key="land_station";
  TD32Data::Header header=o->header();
  if (meta_args.data_format == "td3210" || meta_args.data_format == "td3280") {
    id=id.substr(3);
    ientry_key=pentry.key+"[!]WBAN[!]"+id;
  }
  else {
    id=id.substr(0,6);
    ientry_key=pentry.key+"[!]COOP[!]"+id;
  }
  obs_type_index=1;
  std::vector<TD32Data::Report> reports=o->reports();
  start_date.set(3000,12,31,0);
  end_date.set(1000,1,1,0);
  for (size_t n=0; n < reports.size(); ++n) {
    if (reports[n].flag1 != 'M') {
	DateTime start,end;
	if (header.type == "DLY") {
	  de.key=id+reports[n].date_time.to_string("%Y-%m-%d");
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+9999);
	  start.set_utc_offset(-2400);
	}
	else if (header.type == "MLY") {
	  if (reports[n].date_time.month() == 13) {
	    de.key=id+reports[n].date_time.to_string("%Y");
	  }
	  else {
	    de.key=id+reports[n].date_time.to_string("%Y-%m");
	  }
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),999999);
	}
	else {
	  de.key=id+reports[n].date_time.to_string("%Y-%m-%d %H:%MM");
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+99);
	  if (start.time() == 240099 || start.time() == 250099) {
	    start.set_time(99);
	    start.add_days(1);
	  }
	}
	if (!unique_observation_table.found(de.key,de)) {
	  ++nsteps;
	  unique_observation_table.insert(de);
	}
	end=start;
	if (start.month() == 13) {
	  start.set_month(1);
	  end.set_month(12);
	}
	if (start.day() == 0) {
	  start.set_day(1);
	  end.set_day(dateutils::days_in_month(end.year(),end.month()));
	}
	if (header.type == "HPD") {
	  if (reports[n].date_time.time()/100 == 2500) {
	    start.subtract_minutes(1439);
	  }
	  else {
	    start.subtract_minutes(59);
	  }
	}
	else if (header.type == "15M") {
	  if (reports[n].date_time.time()/100 == 2500) {
	    start.subtract_minutes(1439);
	  }
	  else {
	    start.subtract_minutes(14);
	  }
	}
	if (start < start_date) {
	  start_date=start;
	}
	if (end > end_date) {
	  end_date=end;
	}
	if (reports[n].loc.latitude != last_lat || reports[n].loc.longitude != last_lon) {
	  lats.emplace_back(reports[n].loc.latitude);
	  lons.emplace_back(reports[n].loc.longitude);
	}
	last_lat=reports[n].loc.latitude;
	last_lon=reports[n].loc.longitude;
    }
  }
  return nsteps;
}

size_t dss_tsr_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  short source;
  std::string id=obs->location().ID;

  if (std::regex_search(id,std::regex("^0"))) {
    id=id.substr(1);
  }
  source=reinterpret_cast<Tsraob *>(obs)->flags().source;
  switch (source) {
    case 15:
	if (id.length() == 5 && id < "03000") {
	  if (id < "00026" || id == "00091" || id == "00092") {
	    pentry.key="ocean_station";
	    ientry_key=pentry.key+"[!]WBAN[!]"+id;
	  }
	  else {
	    pentry.key="roving_ship";
	    ientry_key=pentry.key+"[!]other[!]"+id;
	  }
	}
	else if (id.length() == 6 && id >= "116000") {
	  pentry.key="roving_ship";
	  ientry_key=pentry.key+"[!]other[!]"+id;
	}
	else {
	  pentry.key="land_station";
	  ientry_key=pentry.key+"[!]WBAN[!]"+id;
	}
	break;
    case 36:
	if (id >= "47000" && id <= "47999") {
	  pentry.key="land_station";
	  ientry_key=pentry.key+"[!]WMO[!]"+id;
	}
	else
	  metautils::log_error("no platform and station mapping for source "+strutils::itos(source)+" and ID '"+id+"'","obs2xml",user);
	break;
    case 37:
    case 38:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO[!]"+id;
	break;
    default:
	metautils::log_error("no platform and station mapping for source "+strutils::itos(source),"obs2xml","x","x");
  }
  start_date=end_date=obs->date_time();
  obs_type_index=0;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t uadb_raob_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  std::string id=obs->location().ID;

  switch (reinterpret_cast<UADBRaob *>(obs)->ID_type()) {
    case 1:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO[!]"+id;
	break;
    case 2:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WBAN[!]"+id;
	break;
    case 3:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]callSign[!]"+id;
	break;
    case 4:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]COOP[!]"+id;
	break;
    case 5:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]name[!]"+id;
	break;
    case 6:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]other[!]"+id;
	break;
    case 7:
    case 8:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO+6[!]"+id;
	break;
    case 9:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]CHUAN[!]"+id;
	break;
    default:
	metautils::log_error("no platform and station mapping for observation type "+strutils::itos(reinterpret_cast<UADBRaob *>(obs)->ID_type()),"obs2xml","x","x");
  }
  start_date=end_date=obs->date_time();
  obs_type_index=0;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t wmssc_platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  WMSSCObservation *o=reinterpret_cast<WMSSCObservation *>(obs);
  size_t nsteps=0;
  SurfaceObservation::SurfaceReport report;
  SurfaceObservation::SurfaceAdditionalData addl_data;

  start_date.set(1000,1,1,0);
  for (size_t n=1; n <= 12; ++n) {
    report=o->monthly_report(n);
    addl_data=o->monthly_additional_data(n);
    if (report.data.slp > 0. || report.data.stnp > 0. || report.data.tdry > -99. || addl_data.data.pcp_amt >= 0.) {
	if (start_date.year() == 1000) {
	  start_date.set(obs->date_time().year(),n,1,0);
	}
	end_date.set(obs->date_time().year(),n,dateutils::days_in_month(obs->date_time().year(),n),235959);
	++nsteps;
    }
  }
  if (start_date.year() > 1000) {
    std::string sdum=o->station_name();
    if (std::regex_search(sdum,std::regex("SHIP"))) {
	pentry.key="ocean_station";
    }
    else if (std::regex_search(sdum,std::regex("DRIFTING"))) {
	pentry.key="roving_ship";
    }
    else {
	pentry.key="land_station";
    }
    if (o->format() == 0x3f) {
	ientry_key=pentry.key+"[!]COOP[!]"+obs->location().ID;
    }
    else {
	ientry_key=pentry.key+"[!]WMO+6[!]"+obs->location().ID;
    }
    size_t n=obs->date_time().year();
    if (n < 2200) {
// monthly climatology
	obs_type_index=1;
    }
    else if (n < 3200) {
// 10-year monthly climatology
	obs_type_index=3;
	start_date.set_year(start_date.year()-1009);
	end_date.set_year(end_date.year()-1000);
    }
    else {
// 30-year monthly climatology
	obs_type_index=4;
	start_date.set_year(start_date.year()-2029);
	end_date.set_year(end_date.year()-2000);
    }
  }
  else {
    pentry.key="";
  }
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return nsteps;
}

size_t platform_and_station_information(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  size_t nsteps;

  lats.clear();
  lons.clear();
  if (meta_args.data_format == "isd") {
    nsteps=isd_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "on29" || meta_args.data_format == "on124") {
    nsteps=adp_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "cpcsumm") {
    nsteps=cpc_summary_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "imma") {
    nsteps=imma_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "nodcbt") {
    nsteps=nodc_bt_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (std::regex_search(meta_args.data_format,std::regex("^td32"))) {
    nsteps=td32_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "tsr") {
    nsteps=dss_tsr_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "uadb") {
    nsteps=uadb_raob_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (meta_args.data_format == "wmssc") {
    nsteps=wmssc_platform_and_station_information(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else {
    std::cerr << "Error: unable to get platform and station information from format " << meta_args.data_format << std::endl;
    exit(1);
  }
  return nsteps;
}

void add_isd_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  std::list<std::string> addl_data_codes=reinterpret_cast<ISDObservation *>(obs)->additional_data_codes();
  metadata::ObML::DataTypeEntry de;

  addl_data_codes.push_front("MAN");
  for (auto& item : addl_data_codes) {
    if (!entry.data->data_types_table.found(item,de)) {
	de.key=item;
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	de.data->nsteps=1;
	entry.data->data_types_table.insert(de);
    }
    else {
	++(de.data->nsteps);
    }
  }
}

void add_adp_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;

  std::list<size_t> ADPcategories=reinterpret_cast<ADPObservation *>(obs)->categories();
  for (auto& item : ADPcategories) {
    de.key=strutils::itos(item);
    if (!entry.data->data_types_table.found(de.key,de)) {
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	de.data->nsteps=1;
	entry.data->data_types_table.insert(de);
    }
    else {
	++(de.data->nsteps);
    }
  }
}

void add_cpc_daily_summary_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry) {
  CPCSummaryObservation *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs);
  metadata::ObML::DataTypeEntry de;
  const size_t NUM_DATA_TYPES=16;
  static const std::string data_types[NUM_DATA_TYPES]={"RMAX","RMIN","RPRCP","EPRCP","VP","POTEV","VPD","SLP06","SLP12","SLP18","SLP00","APTMX","CHILLM","RAD","MXRH","MNRH"};
  CPCSummaryObservation::CPCDailySummaryData cpcrpt=cpcobs->CPC_daily_summary_data();
  SurfaceObservation::SurfaceAdditionalData addl_data=cpcobs->additional_data();
  float data_type_values[NUM_DATA_TYPES]={addl_data.data.tmax,addl_data.data.tmin,addl_data.data.pcp_amt,cpcrpt.eprcp,cpcrpt.vp,cpcrpt.potev,cpcrpt.vpd,cpcrpt.slp06,cpcrpt.slp12,cpcrpt.slp18,cpcrpt.slp00,cpcrpt.aptmx,cpcrpt.chillm,cpcrpt.rad,static_cast<float>(cpcrpt.mxrh),static_cast<float>(cpcrpt.mnrh)};
  float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-9.,-9.,-9.,-99.,-99.,-99,-99.,-99.,-99.,-9.,-90.,-90.};
  size_t n;

  for (n=0; n < NUM_DATA_TYPES; n++) {
    de.key=data_types[n];
    if (data_type_values[n] > missing[n]) {
	if (!entry.data->data_types_table.found(de.key,de)) {
	  de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	  de.data->nsteps=1;
	  entry.data->data_types_table.insert(de);
	}
	else {
	  ++(de.data->nsteps);
	}
    }
  }
}

void add_cpc_monthly_summary_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry) {
  CPCSummaryObservation *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs);
  metadata::ObML::DataTypeEntry de;
  const size_t NUM_DATA_TYPES=24;
  static const std::string data_types[NUM_DATA_TYPES]={"TMEAN","TMAX","TMIN","HMAX","RMINL","CTMEAN","APTMAX","WCMIN","HMAXAPT","WCLMINL","RPCP","EPCP","CPCP","EPCPMX","AHS","AVP","APET","AVPD","TRAD","AMAXRH","AMINRH","IHDD","ICDD","IGDD"};
  CPCSummaryObservation::CPCMonthlySummaryData cpcrpt=cpcobs->CPC_monthly_summary_data();
  float data_type_values[NUM_DATA_TYPES]={cpcrpt.tmean,cpcrpt.tmax,cpcrpt.tmin,cpcrpt.hmax,cpcrpt.rminl,cpcrpt.ctmean,cpcrpt.aptmax,cpcrpt.wcmin,cpcrpt.hmaxapt,cpcrpt.wclminl,cpcrpt.rpcp,cpcrpt.epcp,cpcrpt.cpcp,cpcrpt.epcpmax,cpcrpt.ahs,cpcrpt.avp,cpcrpt.apet,cpcrpt.avpd,cpcrpt.trad,static_cast<float>(cpcrpt.amaxrh),static_cast<float>(cpcrpt.aminrh),static_cast<float>(cpcrpt.ihdd),static_cast<float>(cpcrpt.icdd),static_cast<float>(cpcrpt.igdd)};
  float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-9.,-9.,-9.,-9.,-9.,-990.,-990.,-990.,-990.,-990.};

  for (size_t n=0; n < NUM_DATA_TYPES; ++n) {
    de.key=data_types[n];
    if (data_type_values[n] > missing[n]) {
	if (!entry.data->data_types_table.found(de.key,de)) {
	  de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	  de.data->nsteps=1;
	  entry.data->data_types_table.insert(de);
	}
	else {
	  ++(de.data->nsteps);
	}
    }
  }
}

void add_cpc_summary_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  if (obs->date_time().day() > 0) {
    add_cpc_daily_summary_data_type_to_entry(obs,entry);
  }
  else {
    add_cpc_monthly_summary_data_type_to_entry(obs,entry);
  }
}

void add_imma_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;
  std::list<short> attm_ids=reinterpret_cast<IMMAObservation *>(obs)->attachment_ID_list();

  attm_ids.emplace_front(0);
  for (auto& item : attm_ids) {
    de.key=strutils::itos(item);
    if (!entry.data->data_types_table.found(de.key,de)) {
	de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	de.data->nsteps=1;
	entry.data->data_types_table.insert(de);
    }
    else {
	++(de.data->nsteps);
    }
  }
}

void add_nodc_bt_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;
  NODCBTObservation *nodcobs=reinterpret_cast<NODCBTObservation *>(obs);

  de.key=nodcobs->data_type();
  strutils::trim(de.key);
  short nlev=nodcobs->number_of_levels();
  NODCBTObservation::Level lmin=nodcobs->level(0);
  lmin.depth=-lmin.depth;
  NODCBTObservation::Level lmax=nodcobs->level(nlev-1);
  lmax.depth=-lmax.depth;
  if (!entry.data->data_types_table.found(de.key,de)) {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    if (de.data->vdata == nullptr) {
	de.data->vdata.reset(new metadata::ObML::DataTypeEntry::Data::VerticalData);
    }
    de.data->nsteps=1;
    de.data->vdata->max_altitude=lmin.depth;
    de.data->vdata->min_altitude=lmax.depth;
    de.data->vdata->avg_nlev=nlev;
    if (nlev > 1) {
	de.data->vdata->avg_res=fabs(lmax.depth-lmin.depth)/static_cast<float>(nlev-1);
	de.data->vdata->res_cnt=1;
    }
    de.data->vdata->units="m";
    entry.data->data_types_table.insert(de);
  }
  else {
    ++(de.data->nsteps);
    if (lmin.depth > de.data->vdata->max_altitude) {
	de.data->vdata->max_altitude=lmin.depth;
    }
    if (lmax.depth < de.data->vdata->min_altitude) {
	de.data->vdata->min_altitude=lmax.depth;
    }
    de.data->vdata->avg_nlev+=nlev;
    if (nlev > 1) {
	de.data->vdata->avg_res+=fabs(lmax.depth-lmin.depth)/static_cast<float>(nlev-1);
	++(de.data->vdata->res_cnt);
    }
  }
}

void add_td32_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  TD32Data *t=reinterpret_cast<TD32Data *>(obs);
  metadata::ObML::DataTypeEntry de,de2;
  size_t n,m;

  TD32Data::Header header=t->header();
  de.key=header.elem;
  if (!entry.data->data_types_table.found(de.key,de)) {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=0;
    entry.data->data_types_table.insert(de);
  }
  std::vector<TD32Data::Report> reports=t->reports();
  if (header.type == "MLY" && header.elem == "FRZD") {
    for (n=0,m=0; n < reports.size(); n++) {
	if (reports[n].flag1 != 'M') {
	  ++m;
	}
    }
    if (m > 0) {
	++(de.data->nsteps);
    }
    return;
  }
  for (n=0; n < reports.size(); ++n) {
    if (reports[n].flag1 != 'M') {
	if (header.elem != "DYSW" || header.type != "DLY") {
	  if (header.elem == "HPCP" && reports[n].date_time.time() == 2500) {
	    de2.key="DPCP";
	    if (!entry.data->data_types_table.found(de2.key,de2)) {
		de2.data.reset(new metadata::ObML::DataTypeEntry::Data);
		de2.data->nsteps=1;
		entry.data->data_types_table.insert(de2);
	    }
	    else {
		++(de2.data->nsteps);
	    }
	  }
	  else if (header.type == "MLY" && reports[n].date_time.month() == 13) {
	    de2.key=de.key+"_y";
	    if (!entry.data->data_types_table.found(de2.key,de2)) {
		de2.data.reset(new metadata::ObML::DataTypeEntry::Data);
		de2.data->nsteps=1;
		entry.data->data_types_table.insert(de2);
	    }
	    else {
		++(de2.data->nsteps);
	    }
	  }
	  else
	    ++(de.data->nsteps);
	}
	else {
	  de2.key=reports[n].date_time.to_string("%Y-%m-%d");
	  if (!entry.data->unique_table.found(de2.key,de2)) {
	    ++(de.data->nsteps);
	    entry.data->unique_table.insert(de2);
	  }
	}
    }
  }
}

void add_dss_tsr_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;

  size_t fmt_code=reinterpret_cast<Tsraob *>(obs)->flags().format;
  if (fmt_code >= 9 && fmt_code <= 14) {
    fmt_code-=8;
  }
  de.key=strutils::itos(fmt_code);
  if (!entry.data->data_types_table.found(de.key,de)) {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=1;
    entry.data->data_types_table.insert(de);
  }
  else {
    ++(de.data->nsteps);
  }
}

void add_uadb_raob_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;

  de.key=strutils::itos(reinterpret_cast<UADBRaob *>(obs)->observation_type());
  if (!entry.data->data_types_table.found(de.key,de)) {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=1;
    entry.data->data_types_table.insert(de);
  }
  else {
    ++(de.data->nsteps);
  }
}

void add_wmssc_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  WMSSCObservation *o=reinterpret_cast<WMSSCObservation *>(obs);
  metadata::ObML::DataTypeEntry de;
  size_t nsteps=0;

  for (size_t n=1; n <= 12; ++n) {
    SurfaceObservation::SurfaceReport report=o->monthly_report(n);
    SurfaceObservation::SurfaceAdditionalData addl_data=o->monthly_additional_data(n);
    if (report.data.slp > 0. || report.data.stnp > 0. || report.data.tdry > -99. || addl_data.data.pcp_amt >= 0.) {
	++nsteps;
    }
  }
  de.key="standard_parameters";
  if (!entry.data->data_types_table.found(de.key,de)) {
    de.data.reset(new metadata::ObML::DataTypeEntry::Data);
    de.data->nsteps=nsteps;
    entry.data->data_types_table.insert(de);
  }
  else {
    (de.data->nsteps)+=nsteps;
  }
  if (o->has_additional_data()) {
    if (obs->date_time().year() < 1961) {
	de.key="height_data";
	if (!entry.data->data_types_table.found(de.key,de)) {
	  de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	  de.data->nsteps=nsteps;
	  entry.data->data_types_table.insert(de);
	}
	else {
	  (de.data->nsteps)+=nsteps;
	}
    }
    else {
      de.key="additional_parameters";
	if (!entry.data->data_types_table.found(de.key,de)) {
	  de.data.reset(new metadata::ObML::DataTypeEntry::Data);
	  de.data->nsteps=nsteps;
	  entry.data->data_types_table.insert(de);
	}
	else {
	  (de.data->nsteps)+=nsteps;
	}
    }
  }
}

void add_data_type_to_entry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  if (meta_args.data_format == "isd") {
    add_isd_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "on29" || meta_args.data_format == "on124") {
    add_adp_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "cpcsumm") {
    add_cpc_summary_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "imma") {
    add_imma_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "nodcbt") {
    add_nodc_bt_data_type_to_entry(obs,entry);
  }
  else if (std::regex_search(meta_args.data_format,std::regex("^td32"))) {
    add_td32_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "tsr") {
    add_dss_tsr_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "uadb") {
    add_uadb_raob_data_type_to_entry(obs,entry);
  }
  else if (meta_args.data_format == "wmssc") {
    add_wmssc_data_type_to_entry(obs,entry);
  }
  else {
    std::cerr << "Error: unable to add data type for format " << meta_args.data_format << std::endl;
    exit(1);
  }
}

struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};

void scan_file()
{
  idstream *istream;
  const size_t BUF_LEN=80000;
  unsigned char buffer[BUF_LEN];
  int status;
  Observation *obs;
  size_t l,k;
  float lat,lon,true_lon;
  DateTime start_date,end_date;
  std::string file_format,error,sdum;
  int obs_type_index=-1;
  size_t nsteps;
  std::list<float> lats,lons;
  bool add_data_type;
  my::map<metautils::StringEntry> missing_id_table;
  metautils::StringEntry se;
  metadata::ObML::ObservationTypes obs_types;
  my::map<InvEntry> inv_O_table,inv_P_table,inv_I_table;
  InvEntry ie;
  std::list<std::string> inv_lines;

  if (meta_args.data_format == "on29" || meta_args.data_format == "on124") {
    istream=new InputADPStream;
    obs=new ADPObservation;
  }
  else if (meta_args.data_format == "cpcsumm") {
    istream=new InputCPCSummaryObservationStream;
    obs=new CPCSummaryObservation;
  }
  else if (meta_args.data_format == "imma") {
    istream=new InputIMMAObservationStream;
    obs=new IMMAObservation;
  }
  else if (meta_args.data_format == "isd") {
    istream=new InputISDObservationStream;
    obs=new ISDObservation;
  }
  else if (std::regex_search(meta_args.data_format,std::regex("^td32"))) {
    if (meta_args.data_format == "td3200" || meta_args.data_format == "td3210" || meta_args.data_format == "td3220" || meta_args.data_format == "td3240" || meta_args.data_format == "td3260" || meta_args.data_format == "td3280") {
	istream=new InputTD32Stream;
    }
    else {
	std::cerr << "Error: format " << meta_args.data_format << " not recognized" << std::endl;
	exit(1);
    }
    obs=new TD32Data;
  }
  else if (meta_args.data_format == "nodcbt") {
    istream=new InputNODCBTObservationStream;
    obs=new NODCBTObservation;
  }
  else if (meta_args.data_format == "tsr") {
    istream=new InputTsraobStream;
    obs=new Tsraob;
  }
  else if (meta_args.data_format == "uadb") {
    istream=new InputUADBRaobStream;
    obs=new UADBRaob;
  }
  else if (meta_args.data_format == "wmssc") {
    istream=new InputWMSSCObservationStream;
    obs=new WMSSCObservation;
  }
  else {
    std::cerr << "Error: format " << meta_args.data_format << " not recognized" << std::endl;
    exit(1);
  }
  id_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
  for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
    id_table[n]=new my::map<metadata::ObML::IDEntry>(999999);
  }
  tfile=new TempFile;
  tfile->open(meta_args.temp_loc);
  tdir=new TempDir;
  tdir->create(meta_args.temp_loc);
  std::list<std::string> filelist;
  if (verbose_operation) {
    std::cout << "Preparing file for metadata scanning ..." << std::endl;
  }
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning(): '"+error+"'","obs2xml",user);
  }
  if (verbose_operation) {
    std::cout << "... preparation complete." << std::endl;
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
    if (!metautils::primaryMetadata::open_file_for_metadata_scanning(istream,file,error)) {
	metautils::log_error("open_file_for_metadata_scanning(): '"+error+"'","obs2xml",user);
    }
    if (file_format.empty() && meta_args.data_format == "isd") {
	metadata::open_inventory(inv_file,&inv_dir,inv_stream,"ObML","obs2xml",user);
    }
    else if (meta_args.inventory_only) {
	metautils::log_error("scan_file(): unable to inventory "+meta_args.path+"/"+meta_args.filename+" because archive format is '"+file_format+"' and data format is '"+meta_args.data_format+"'","obs2xml",user);
    }
    while ( (status=istream->read(buffer,BUF_LEN)) != bfstream::eof) {
	if (status > 0) {
	  obs->fill(buffer,Observation::full_report);
	  nsteps=platform_and_station_information(obs,ientry.key,start_date,end_date,obs_type_index,lats,lons);
	  std::list<float>::iterator it_lat,it_lon;
	  std::list<float>::iterator end=lats.end();
	  add_data_type=false;
	  for (it_lat=lats.begin(),it_lon=lons.begin(); it_lat != end; ++it_lat,++it_lon) {
	    lat=*it_lat;
	    lon=*it_lon;
// handle longitudes that are expressed as 0-360
	    true_lon= (lon > 180.) ? (lon-360.) : lon;
	    if (pentry.key.length() > 0) {
		if (!id_table[obs_type_index]->found(ientry.key,ientry)) {
		  if (lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
		    num_not_missing++;
		    ientry.data.reset(new metadata::ObML::IDEntry::Data);
		    ientry.data->S_lat=ientry.data->N_lat=lat;
		    ientry.data->min_lon_bitmap.reset(new float[360]);
		    ientry.data->max_lon_bitmap.reset(new float[360]);
		    for (size_t n=0; n < 360; ++n) {
			ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
		    }
		    ientry.data->W_lon=ientry.data->E_lon=true_lon;
		    geoutils::convert_lat_lon_to_box(1,0.,true_lon,l,k);
		    ientry.data->min_lon_bitmap[k]=ientry.data->max_lon_bitmap[k]=true_lon;
		    ientry.data->start=start_date;
		    ientry.data->end=end_date;
		    ientry.data->nsteps=nsteps;
		    add_data_type_to_entry(obs,ientry);
		    add_data_type=true;
		    id_table[obs_type_index]->insert(ientry);
		  }
		}
		else {
		  if (ientry.data->S_lat != lat || ientry.data->W_lon != true_lon) {
		    if (lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
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
			geoutils::convert_lat_lon_to_box(1,0.,true_lon,l,k);
			if (ientry.data->min_lon_bitmap[k] > 900.) {
			  ientry.data->min_lon_bitmap[k]=ientry.data->max_lon_bitmap[k]=true_lon;
			}
			else {
			  if (true_lon < ientry.data->min_lon_bitmap[k]) {
			    ientry.data->min_lon_bitmap[k]=true_lon;
			  }
			  if (true_lon > ientry.data->max_lon_bitmap[k]) {
			    ientry.data->max_lon_bitmap[k]=true_lon;
			  }
			}
		    }
		  }
		  if (lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
		    ++num_not_missing;
		    if (start_date < ientry.data->start) {
			ientry.data->start=start_date;
		    }
		    if (end_date > ientry.data->end) {
			ientry.data->end=end_date;
		    }
		    ientry.data->nsteps+=nsteps;
		    if (!add_data_type) {
			add_data_type_to_entry(obs,ientry);
			add_data_type=true;
		    }
		  }
		}
		if (!platform_table[obs_type_index].found(pentry.key,pentry)) {
		  if (lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
		    pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
		    pentry.boxflags->initialize(361,180,0,0);
		    if (lat == -90.) {
			pentry.boxflags->spole=1;
		    }
		    else if (lat == 90.) {
			pentry.boxflags->npole=1;
		    }
		    else {
			geoutils::convert_lat_lon_to_box(1,lat,true_lon,l,k);
			pentry.boxflags->flags[l-1][k]=1;
			pentry.boxflags->flags[l-1][360]=1;
		    }
		    platform_table[obs_type_index].insert(pentry);
		  }
		  else {
		    if (!missing_id_table.found(ientry.key,se)) {
			se.key=ientry.key;
			missing_id_table.insert(se);
			metautils::log_warning("no location for ID: "+ientry.key+" date: "+obs->date_time().to_string(),"obs2xml",user);
		    }
		  }
		}
		else {
		  if (lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
		    if (lat == -90.) {
			pentry.boxflags->spole=1;
		    }
		    else if (lat == 90.) {
			pentry.boxflags->npole=1;
		    }
		    else {
			geoutils::convert_lat_lon_to_box(1,lat,true_lon,l,k);
			pentry.boxflags->flags[l-1][k]=1;
			pentry.boxflags->flags[l-1][360]=1;
		    }
		  }
		  else {
		    if (!missing_id_table.found(ientry.key,se)) {
			se.key=ientry.key;
			missing_id_table.insert(se);
			metautils::log_warning("no location for ID: "+ientry.key+" date: "+obs->date_time().to_string(),"obs2xml",user);
		    }
		  }
		}
		if (inv_stream.is_open() && lat > -90.01 && lat < 90.01 && true_lon > -180.01 && true_lon < 180.01) {
		  std::stringstream inv_line;
		  inv_line << istream->current_record_offset() << "|" << status << "|" << obs->date_time().to_string("%Y%m%d%H%MM");
		  if (!inv_O_table.found(obs_types.types[obs_type_index],ie)) {
		    ie.key=obs_types.types[obs_type_index];
		    ie.num=inv_O_table.size();
		    inv_O_table.insert(ie);
		  }
		  inv_line << "|" << ie.num;
		  std::deque<std::string> sp=strutils::split(ientry.key,"[!]");
		  if (!inv_P_table.found(sp[0],ie)) {
		    ie.key=sp[0];
		    ie.num=inv_P_table.size();
		    inv_P_table.insert(ie);
		  }
		  inv_line << "|" << ie.num;
		  ie.key=sp[1]+"|"+sp[2];
		  if (!inv_I_table.found(ie.key,ie)) {
		    ie.num=inv_I_table.size();
		    inv_I_table.insert(ie);
		  }
		  inv_line << "|" << ie.num;
		  inv_line << "|" << strutils::ftos(lat,4) << "|" << strutils::ftos(lon,4);
		  inv_lines.emplace_back(inv_line.str());
		}
	    }
	    nsteps=0;
	  }
	} 
 	else {
	  metautils::log_error("unable to read observation "+strutils::itos(istream->number_read()+1)+"; error: '"+myerror+"'","obs2xml",user);
	}
    }
    istream->close();
    if (verbose_operation) {
	std::cout << "  ...scan of " << file << " completed." << std::endl;
    }
  }
  clean_up();
  if (inv_lines.size() > 0) {
    for (auto& key : inv_O_table.keys()) {
	inv_O_table.found(key,ie);
	inv_stream << "O<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (auto& key : inv_P_table.keys()) {
	inv_P_table.found(key,ie);
	inv_stream << "P<!>" << ie.num << "<!>" << key << std::endl;
    }
    for (auto& key : inv_I_table.keys()) {
	inv_I_table.found(key,ie);
	inv_stream << "I<!>" << ie.num << "<!>" << key << std::endl;
    }
    inv_stream << "-----" << std::endl;
    for (auto& line : inv_lines) {
	inv_stream << line << std::endl;
    }
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","obs2xml",user);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  std::string web_home,flags,key;

  if (argc < 4) {
    std::cerr << "usage: obs2xml -f format -d [ds]nnn.n [-l local_name] [options...] path" << std::endl << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f on29      ADP ON29 format" << std::endl;
    std::cerr << "-f on124     ADP ON124 format" << std::endl;
    std::cerr << "-f cpcsumm   CPC Summary of Day/Month format" << std::endl;
    std::cerr << "-f tsr       DSS Tsraob format" << std::endl;
    std::cerr << "-f wmssc     DSS World Monthly Surface Station Climatology format" << std::endl;
    std::cerr << "-f imma      International Maritime Meteorological Archive format" << std::endl;
    std::cerr << "-f isd       NCDC ISD format" << std::endl;
    std::cerr << "-f nodcbt    NODC BT format" << std::endl;
    std::cerr << "-f td3200    NCDC TD3200 format" << std::endl;
    std::cerr << "-f td3210    NCDC TD3210 format" << std::endl;
    std::cerr << "-f td3220    NCDC TD3220 format" << std::endl;
    std::cerr << "-f td3240    NCDC TD3240 format" << std::endl;
    std::cerr << "-f td3260    NCDC TD3260 format" << std::endl;
    std::cerr << "-f td3280    NCDC TD3280 format" << std::endl;
    std::cerr << "-f uadb      Upper Air Database ASCII format" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d <nnn.n> nnn.n is the dataset number to which this data file belongs" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "<path>     full MSS path or URL of file to read" << std::endl;
    std::cerr << "options:" << std::endl;
    if (user == "dattore")
      std::cerr << "-NC              don't check to see if the MSS file is a primary for the dataset" << std::endl;
    std::cerr << "-g/-G            update/don't update graphics (default is -g)" << std::endl;
    std::cerr << "-I               inventory only; no content metadata generated" << std::endl;
    std::cerr << "-l <local_name>  name of the MSS file on local disk (this avoids an MSS read)" << std::endl;
    std::cerr << "-u/-U            update/don't update the database (default is -u)" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  meta_args.args_string=unixutils::unix_args_string(argc,argv,'!');
  metautils::read_config("obs2xml",user,meta_args.args_string);
  parse_args();
  flags="-f";
  if (!meta_args.inventory_only && std::regex_search(meta_args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("obs2xml",user);
  if (!meta_args.inventory_only) {
    metautils::check_for_existing_cmd("ObML");
  }
  scan_file();
  if (!meta_args.inventory_only) {
    if (num_not_missing > 0) {
	metadata::ObML::write_obml(id_table,platform_table,"obs2xml",user);
    }
    else {
	metautils::log_error("all stations have missing location information - no usable data found; no content metadata will be saved for this file","obs2xml",user);
    }
  }
  if (meta_args.update_db) {
    if (!meta_args.update_graphics) {
	flags="-G "+flags;
    }
    if (!meta_args.regenerate) {
	flags="-R "+flags;
    }
    if (!meta_args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (unixutils::mysystem2(meta_directives.local_root+"/bin/scm -d "+meta_args.dsnum+" "+flags+" "+meta_args.filename+".ObML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    metadata::close_inventory(inv_file,inv_dir,inv_stream,"ObML",meta_args.update_summary,true,"obs2xml",user);
  }
  return 0;
}
