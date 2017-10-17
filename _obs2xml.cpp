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

metautils::Directives directives;
metautils::Args args;
my::map<metadata::ObML::IDEntry> **ID_table;
metadata::ObML::IDEntry ientry;
my::map<metadata::ObML::PlatformEntry> platform_table[metadata::ObML::NUM_OBS_TYPES];
metadata::ObML::PlatformEntry pentry;
std::string user=getenv("USER");
TempFile *tfile;
std::string inv_file;
TempDir *tdir,*inv_dir=nullptr;
std::ofstream inv_stream;
size_t num_not_missing=0;
std::string myerror="";

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
    metautils::log_error(myerror,"obs2xml",user,args.args_string);
  }
}

void parse_args()
{
  size_t n;

  args.update_DB=true;
  args.update_graphics=true;
  args.update_summary=true;
  args.override_primary_check=false;
  args.temp_loc=directives.temp_path;
  std::deque<std::string> sp=strutils::split(args.args_string,"!");
  for (n=0; n < sp.size()-1; n++) {
    if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-G") {
	args.update_graphics=false;
    }
    else if (sp[n] == "-I") {
	args.inventory_only=true;
	args.update_DB=false;
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
	args.member_name=sp[++n];
    }
    else if (sp[n] == "-R") {
	args.regenerate=false;
    }
    else if (sp[n] == "-S") {
	args.update_summary=false;
    }
    else if (sp[n] == "-U") {
	args.update_DB=false;
    }
    else if (sp[n] == "-NC") {
	if (user == "dattore") {
	  args.override_primary_check=true;
	}
    }
    else {
	std::cerr << "Error: bad argument " << sp[n] << std::endl;
	exit(1);
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    args.format=strutils::to_lower(args.format);
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.override_primary_check=true;
    args.update_DB=false;
    args.update_summary=false;
    args.regenerate=false;
  }
  n=sp.back().rfind("/");
  args.path=sp.back().substr(0,n);
  args.filename=sp.back().substr(n+1);
}

size_t getISDPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
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
    metautils::log_error("no platform and station mapping for report type '"+rpt_type+"'","obs2xml",user,args.args_string);
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

size_t getADPPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
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
    if (args.format == "on29") {
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

size_t getCPCSummaryPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  std::string ID=obs->location().ID;

  pentry.key="land_station";
  start_date=end_date=obs->date_time();
  if (start_date.day() == 0) {
    start_date.set_day(1);
    end_date.set_day(days_in_month(end_date.year(),end_date.month()));
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

std::string cleanIMMAID(std::string ID)
{
  if (ID.length() == 0) {
    return ID;
  }
  std::string cleanID=ID;
  strutils::trim(cleanID);
  strutils::replace_all(cleanID,"\"","'");
  strutils::replace_all(cleanID,"&","&amp;");
  strutils::replace_all(cleanID,">","&gt;");
  strutils::replace_all(cleanID,"<","&lt;");
  auto sp=strutils::split(cleanID);
  return strutils::to_upper(sp[0]);
}

size_t getIMMAPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  IMMAObservation *o=reinterpret_cast<IMMAObservation *>(obs);
  auto ID=cleanIMMAID(obs->location().ID);
  if (ID.length() == 0) {
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
		  metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized - obs date: "+obs->date_time().to_string()+", ID type: "+strutils::itos(o->ID_type()),"obs2xml",user,args.args_string);
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
	    metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",user,args.args_string);
	  }
	}
	break;
    }
    default:
    {
	metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",user,args.args_string);
    }
  }
  start_date=end_date=obs->date_time();
  if (start_date.day() < 1 || static_cast<size_t>(start_date.day()) > days_in_month(start_date.year(),start_date.month()) || start_date.time() > 235959) {
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
	if (strutils::is_numeric(ID))
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
      if (ID.length() == 8) {
	  if (strutils::is_numeric(ID)) {
	    ID=ID.substr(0,4);
	    ientry_key+="number";
	  }
	  else if (strutils::is_numeric(ID.substr(2)) && ID[0] >= 'A' && ID[0] <= 'Z' && ID[1] >= 'A' && ID[1] <= 'Z') {
	    ID=ID.substr(0,2);
	    ientry_key+="generic";
	  }
	  else
	    ientry_key+="name";
      }
      else {
	  if (strutils::is_numeric(ID)) {
	    ientry_key+="number";
	  }
	  else {
	    ientry_key+="name";
	  }
	}
	break;
    default:
	metautils::log_error("ID type "+strutils::itos(o->ID_type())+" not recognized - obs date: "+obs->date_time().to_string(),"obs2xml",user,args.args_string);
  }
  ientry_key+="[!]"+ID;
  obs_type_index=1;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t getNODCBTPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  pentry.key="roving_ship";
  start_date=end_date=obs->date_time();
  ientry_key=pentry.key+"[!]NODC[!]"+obs->location().ID;
  obs_type_index=2;
  lats.emplace_back(obs->location().latitude);
  lons.emplace_back(obs->location().longitude);
  return 1;
}

size_t getTD32PlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  TD32Data *o=reinterpret_cast<TD32Data *>(obs);
  static my::map<metadata::ObML::DataTypeEntry> uniqueObservationTable(999999);
  metadata::ObML::DataTypeEntry de;
  size_t nsteps=0;
  std::string ID=obs->location().ID;
  float last_lat=999.,last_lon=9999.;

  pentry.key="land_station";
  TD32Data::Header header=o->header();
  if (args.format == "td3210" || args.format == "td3280") {
    ID=ID.substr(3);
    ientry_key=pentry.key+"[!]WBAN[!]"+ID;
  }
  else {
    ID=ID.substr(0,6);
    ientry_key=pentry.key+"[!]COOP[!]"+ID;
  }
  obs_type_index=1;
  std::vector<TD32Data::Report> reports=o->reports();
  start_date.set(3000,12,31,0);
  end_date.set(1000,1,1,0);
  for (size_t n=0; n < reports.size(); ++n) {
    if (reports[n].flag1 != 'M') {
	DateTime start,end;
	if (header.type == "DLY") {
	  de.key=ID+reports[n].date_time.to_string("%Y-%m-%d");
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+9999);
	  start.set_UTC_offset(-2400);
	}
	else if (header.type == "MLY") {
	  if (reports[n].date_time.month() == 13) {
	    de.key=ID+reports[n].date_time.to_string("%Y");
	  }
	  else {
	    de.key=ID+reports[n].date_time.to_string("%Y-%m");
	  }
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),999999);
	}
	else {
	  de.key=ID+reports[n].date_time.to_string("%Y-%m-%d %H:%MM");
	  start.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+99);
	  if (start.time() == 240099 || start.time() == 250099) {
	    start.set_time(99);
	    start.add_days(1);
	  }
	}
	if (!uniqueObservationTable.found(de.key,de)) {
	  ++nsteps;
	  uniqueObservationTable.insert(de);
	}
	end=start;
	if (start.month() == 13) {
	  start.set_month(1);
	  end.set_month(12);
	}
	if (start.day() == 0) {
	  start.set_day(1);
	  end.set_day(days_in_month(end.year(),end.month()));
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

size_t getDSSTsrPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  short source;
  std::string ID=obs->location().ID;

  if (std::regex_search(ID,std::regex("^0"))) {
    ID=ID.substr(1);
  }
  source=reinterpret_cast<Tsraob *>(obs)->flags().source;
  switch (source) {
    case 15:
	if (ID.length() == 5 && ID < "03000") {
	  if (ID < "00026" || ID == "00091" || ID == "00092") {
	    pentry.key="ocean_station";
	    ientry_key=pentry.key+"[!]WBAN[!]"+ID;
	  }
	  else {
	    pentry.key="roving_ship";
	    ientry_key=pentry.key+"[!]other[!]"+ID;
	  }
	}
	else if (ID.length() == 6 && ID >= "116000") {
	  pentry.key="roving_ship";
	  ientry_key=pentry.key+"[!]other[!]"+ID;
	}
	else {
	  pentry.key="land_station";
	  ientry_key=pentry.key+"[!]WBAN[!]"+ID;
	}
	break;
    case 36:
	if (ID >= "47000" && ID <= "47999") {
	  pentry.key="land_station";
	  ientry_key=pentry.key+"[!]WMO[!]"+ID;
	}
	else
	  metautils::log_error("no platform and station mapping for source "+strutils::itos(source)+" and ID '"+ID+"'","obs2xml",user,args.args_string);
	break;
    case 37:
    case 38:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO[!]"+ID;
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

size_t getUADBPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  std::string ID=obs->location().ID;

  switch (reinterpret_cast<UADBRaob *>(obs)->ID_type()) {
    case 1:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO[!]"+ID;
	break;
    case 2:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WBAN[!]"+ID;
	break;
    case 3:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]callSign[!]"+ID;
	break;
    case 4:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]COOP[!]"+ID;
	break;
    case 5:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]name[!]"+ID;
	break;
    case 6:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]other[!]"+ID;
	break;
    case 7:
    case 8:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]WMO+6[!]"+ID;
	break;
    case 9:
	pentry.key="land_station";
	ientry_key=pentry.key+"[!]CHUAN[!]"+ID;
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

size_t getWMSSCPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
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
	end_date.set(obs->date_time().year(),n,days_in_month(obs->date_time().year(),n),235959);
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

size_t getPlatformAndStationInformation(Observation *obs,std::string& ientry_key,DateTime& start_date,DateTime& end_date,int& obs_type_index,std::list<float>& lats,std::list<float>& lons)
{
  size_t nsteps;

  lats.clear();
  lons.clear();
  if (args.format == "isd") {
    nsteps=getISDPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "on29" || args.format == "on124") {
    nsteps=getADPPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "cpcsumm") {
    nsteps=getCPCSummaryPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "imma") {
    nsteps=getIMMAPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "nodcbt") {
    nsteps=getNODCBTPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (std::regex_search(args.format,std::regex("^td32"))) {
    nsteps=getTD32PlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "tsr") {
    nsteps=getDSSTsrPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "uadb") {
    nsteps=getUADBPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else if (args.format == "wmssc") {
    nsteps=getWMSSCPlatformAndStationInformation(obs,ientry_key,start_date,end_date,obs_type_index,lats,lons);
  }
  else {
    std::cerr << "Error: unable to get platform and station information from format " << args.format << std::endl;
    exit(1);
  }
  return nsteps;
}

void addISDDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addADPDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addCPCDailySummaryDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry) {
  CPCSummaryObservation *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs);
  metadata::ObML::DataTypeEntry de;
  const size_t NUM_DATA_TYPES=16;
  static const std::string dataTypes[NUM_DATA_TYPES]={"RMAX","RMIN","RPRCP","EPRCP","VP","POTEV","VPD","SLP06","SLP12","SLP18","SLP00","APTMX","CHILLM","RAD","MXRH","MNRH"};
  CPCSummaryObservation::CPCDailySummaryData cpcrpt=cpcobs->CPC_daily_summary_data();
  SurfaceObservation::SurfaceAdditionalData addl_data=cpcobs->additional_data();
  float dataTypeValues[NUM_DATA_TYPES]={addl_data.data.tmax,addl_data.data.tmin,addl_data.data.pcp_amt,cpcrpt.eprcp,cpcrpt.vp,cpcrpt.potev,cpcrpt.vpd,cpcrpt.slp06,cpcrpt.slp12,cpcrpt.slp18,cpcrpt.slp00,cpcrpt.aptmx,cpcrpt.chillm,cpcrpt.rad,static_cast<float>(cpcrpt.mxrh),static_cast<float>(cpcrpt.mnrh)};
  float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-9.,-9.,-9.,-99.,-99.,-99,-99.,-99.,-99.,-9.,-90.,-90.};
  size_t n;

  for (n=0; n < NUM_DATA_TYPES; n++) {
    de.key=dataTypes[n];
    if (dataTypeValues[n] > missing[n]) {
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

void addCPCMonthlySummaryDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry) {
  CPCSummaryObservation *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs);
  metadata::ObML::DataTypeEntry de;
  const size_t NUM_DATA_TYPES=24;
  static const std::string dataTypes[NUM_DATA_TYPES]={"TMEAN","TMAX","TMIN","HMAX","RMINL","CTMEAN","APTMAX","WCMIN","HMAXAPT","WCLMINL","RPCP","EPCP","CPCP","EPCPMX","AHS","AVP","APET","AVPD","TRAD","AMAXRH","AMINRH","IHDD","ICDD","IGDD"};
  CPCSummaryObservation::CPCMonthlySummaryData cpcrpt=cpcobs->CPC_monthly_summary_data();
  float dataTypeValues[NUM_DATA_TYPES]={cpcrpt.tmean,cpcrpt.tmax,cpcrpt.tmin,cpcrpt.hmax,cpcrpt.rminl,cpcrpt.ctmean,cpcrpt.aptmax,cpcrpt.wcmin,cpcrpt.hmaxapt,cpcrpt.wclminl,cpcrpt.rpcp,cpcrpt.epcp,cpcrpt.cpcp,cpcrpt.epcpmax,cpcrpt.ahs,cpcrpt.avp,cpcrpt.apet,cpcrpt.avpd,cpcrpt.trad,static_cast<float>(cpcrpt.amaxrh),static_cast<float>(cpcrpt.aminrh),static_cast<float>(cpcrpt.ihdd),static_cast<float>(cpcrpt.icdd),static_cast<float>(cpcrpt.igdd)};
  float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-9.,-9.,-9.,-9.,-9.,-990.,-990.,-990.,-990.,-990.};

  for (size_t n=0; n < NUM_DATA_TYPES; ++n) {
    de.key=dataTypes[n];
    if (dataTypeValues[n] > missing[n]) {
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

void addCPCSummaryDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  if (obs->date_time().day() > 0) {
    addCPCDailySummaryDataTypeToEntry(obs,entry);
  }
  else {
    addCPCMonthlySummaryDataTypeToEntry(obs,entry);
  }
}

void addIMMADataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  metadata::ObML::DataTypeEntry de;
  std::list<short> attm_IDs=reinterpret_cast<IMMAObservation *>(obs)->attachment_ID_list();

  attm_IDs.push_front(0);
  for (auto& item : attm_IDs) {
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

void addNODCBTDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addTD32DataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addDSSTsrDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addUADBRaobDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addWMSSCDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
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

void addDataTypeToEntry(Observation *obs,metadata::ObML::IDEntry& entry)
{
  if (args.format == "isd") {
    addISDDataTypeToEntry(obs,entry);
  }
  else if (args.format == "on29" || args.format == "on124") {
    addADPDataTypeToEntry(obs,entry);
  }
  else if (args.format == "cpcsumm") {
    addCPCSummaryDataTypeToEntry(obs,entry);
  }
  else if (args.format == "imma") {
    addIMMADataTypeToEntry(obs,entry);
  }
  else if (args.format == "nodcbt") {
    addNODCBTDataTypeToEntry(obs,entry);
  }
  else if (std::regex_search(args.format,std::regex("^td32"))) {
    addTD32DataTypeToEntry(obs,entry);
  }
  else if (args.format == "tsr") {
    addDSSTsrDataTypeToEntry(obs,entry);
  }
  else if (args.format == "uadb") {
    addUADBRaobDataTypeToEntry(obs,entry);
  }
  else if (args.format == "wmssc") {
    addWMSSCDataTypeToEntry(obs,entry);
  }
  else {
    std::cerr << "Error: unable to add data type for format " << args.format << std::endl;
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
  float lat,lon,trueLon;
  DateTime start_date,end_date;
  std::string file_format,error,sdum;
  int obs_type_index=-1;
  size_t nsteps;
  std::list<float> lats,lons;
  bool addedDataType;
  my::map<metautils::StringEntry> missing_ID_table;
  metautils::StringEntry se;
  metadata::ObML::ObservationTypes obsTypes;
  my::map<InvEntry> inv_O_table,inv_P_table,inv_I_table;
  InvEntry ie;
  std::list<std::string> inv_lines;

  if (args.format == "on29" || args.format == "on124") {
    istream=new InputADPStream;
    obs=new ADPObservation;
  }
  else if (args.format == "cpcsumm") {
    istream=new InputCPCSummaryObservationStream;
    obs=new CPCSummaryObservation;
  }
  else if (args.format == "imma") {
    istream=new InputIMMAObservationStream;
    obs=new IMMAObservation;
  }
  else if (args.format == "isd") {
    istream=new InputISDObservationStream;
    obs=new ISDObservation;
  }
  else if (std::regex_search(args.format,std::regex("^td32"))) {
    if (args.format == "td3200" || args.format == "td3210" || args.format == "td3220" || args.format == "td3240" || args.format == "td3260" || args.format == "td3280") {
	istream=new InputTD32Stream;
    }
    else {
	std::cerr << "Error: format " << args.format << " not recognized" << std::endl;
	exit(1);
    }
    obs=new TD32Data;
  }
  else if (args.format == "nodcbt") {
    istream=new InputNODCBTObservationStream;
    obs=new NODCBTObservation;
  }
  else if (args.format == "tsr") {
    istream=new InputTsraobStream;
    obs=new Tsraob;
  }
  else if (args.format == "uadb") {
    istream=new InputUADBRaobStream;
    obs=new UADBRaob;
  }
  else if (args.format == "wmssc") {
    istream=new InputWMSSCObservationStream;
    obs=new WMSSCObservation;
  }
  else {
    std::cerr << "Error: format " << args.format << " not recognized" << std::endl;
    exit(1);
  }
  ID_table=new my::map<metadata::ObML::IDEntry> *[metadata::ObML::NUM_OBS_TYPES];
  for (size_t n=0; n < metadata::ObML::NUM_OBS_TYPES; ++n) {
    ID_table[n]=new my::map<metadata::ObML::IDEntry>(999999);
  }
  tfile=new TempFile;
  tfile->open(args.temp_loc);
  tdir=new TempDir;
  tdir->create(args.temp_loc);
  if (!primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,NULL,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned error: '"+error+"'","obs2xml",user,args.args_string);
  }
  if (!primaryMetadata::open_file_for_metadata_scanning(istream,tfile->name(),error)) {
    metautils::log_error("open_file_for_metadata_scanning() returned error: '"+error+"'","obs2xml",user,args.args_string);
  }
  if (file_format.length() == 0 && args.format == "isd") {
    metadata::open_inventory(inv_file,&inv_dir,inv_stream,"ObML","obs2xml",user);
  }
  else if (args.inventory_only) {
    metautils::log_error("scan_file returned error: unable to inventory "+args.path+"/"+args.filename+" because archive format is '"+file_format+"' and data format is '"+args.format+"'","obs2xml",user,args.args_string);
  }
  while ( (status=istream->read(buffer,BUF_LEN)) != bfstream::eof) {
    if (status > 0) {
	obs->fill(buffer,Observation::full_report);
	nsteps=getPlatformAndStationInformation(obs,ientry.key,start_date,end_date,obs_type_index,lats,lons);
	std::list<float>::iterator it_lat,it_lon;
	std::list<float>::iterator end=lats.end();
	addedDataType=false;
	for (it_lat=lats.begin(),it_lon=lons.begin(); it_lat != end; ++it_lat,++it_lon) {
	  lat=*it_lat;
	  lon=*it_lon;
// handle longitudes that are expressed as 0-360
	  trueLon= (lon > 180.) ? (lon-360.) : lon;
	  if (pentry.key.length() > 0) {
	    if (!ID_table[obs_type_index]->found(ientry.key,ientry)) {
		if (lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		  num_not_missing++;
		  ientry.data.reset(new metadata::ObML::IDEntry::Data);
		  ientry.data->S_lat=ientry.data->N_lat=lat;
		  ientry.data->min_lon_bitmap.reset(new float[360]);
		  ientry.data->max_lon_bitmap.reset(new float[360]);
		  for (size_t n=0; n < 360; ++n) {
		    ientry.data->min_lon_bitmap[n]=ientry.data->max_lon_bitmap[n]=999.;
		  }
		  ientry.data->W_lon=ientry.data->E_lon=trueLon;
		  convert_lat_lon_to_box(1,0.,trueLon,l,k);
		  ientry.data->min_lon_bitmap[k]=ientry.data->max_lon_bitmap[k]=trueLon;
		  ientry.data->start=start_date;
		  ientry.data->end=end_date;
		  ientry.data->nsteps=nsteps;
		  addDataTypeToEntry(obs,ientry);
		  addedDataType=true;
		  ID_table[obs_type_index]->insert(ientry);
		}
	    }
	    else {
		if (ientry.data->S_lat != lat || ientry.data->W_lon != trueLon) {
		  if (lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		    if (lat < ientry.data->S_lat) {
			ientry.data->S_lat=lat;
		    }
		    if (lat > ientry.data->N_lat) {
			ientry.data->N_lat=lat;
		    }
		    if (trueLon < ientry.data->W_lon) {
			ientry.data->W_lon=trueLon;
		    }
		    if (trueLon > ientry.data->E_lon) {
			ientry.data->E_lon=trueLon;
		    }
		    convert_lat_lon_to_box(1,0.,trueLon,l,k);
		    if (ientry.data->min_lon_bitmap[k] > 900.) {
			ientry.data->min_lon_bitmap[k]=ientry.data->max_lon_bitmap[k]=trueLon;
		    }
		    else {
			if (trueLon < ientry.data->min_lon_bitmap[k]) {
			  ientry.data->min_lon_bitmap[k]=trueLon;
			}
			if (trueLon > ientry.data->max_lon_bitmap[k]) {
			  ientry.data->max_lon_bitmap[k]=trueLon;
			}
		    }
		  }
		}
		if (lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		  ++num_not_missing;
		  if (start_date < ientry.data->start) {
		    ientry.data->start=start_date;
		  }
		  if (end_date > ientry.data->end) {
		    ientry.data->end=end_date;
		  }
		  ientry.data->nsteps+=nsteps;
		  if (!addedDataType) {
		    addDataTypeToEntry(obs,ientry);
		    addedDataType=true;
		  }
		}
	    }
	    if (!platform_table[obs_type_index].found(pentry.key,pentry)) {
		if (lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		  pentry.boxflags.reset(new summarizeMetadata::BoxFlags);
		  pentry.boxflags->initialize(361,180,0,0);
		  if (lat == -90.) {
		    pentry.boxflags->spole=1;
		  }
		  else if (lat == 90.) {
		    pentry.boxflags->npole=1;
		  }
		  else {
		    convert_lat_lon_to_box(1,lat,trueLon,l,k);
		    pentry.boxflags->flags[l-1][k]=1;
		    pentry.boxflags->flags[l-1][360]=1;
		  }
		  platform_table[obs_type_index].insert(pentry);
		}
		else {
		  if (!missing_ID_table.found(ientry.key,se)) {
		    se.key=ientry.key;
		    missing_ID_table.insert(se);
		    metautils::log_warning("no location for ID: "+ientry.key+" date: "+obs->date_time().to_string(),"obs2xml",user,args.args_string);
		  }
		}
	    }
	    else {
		if (lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		  if (lat == -90.) {
		    pentry.boxflags->spole=1;
		  }
		  else if (lat == 90.) {
		    pentry.boxflags->npole=1;
		  }
		  else {
		    convert_lat_lon_to_box(1,lat,trueLon,l,k);
		    pentry.boxflags->flags[l-1][k]=1;
		    pentry.boxflags->flags[l-1][360]=1;
		  }
		}
		else {
		  if (!missing_ID_table.found(ientry.key,se)) {
		    se.key=ientry.key;
		    missing_ID_table.insert(se);
		    metautils::log_warning("no location for ID: "+ientry.key+" date: "+obs->date_time().to_string(),"obs2xml",user,args.args_string);
		  }
		}
	    }
	    if (inv_stream.is_open() && lat > -90.01 && lat < 90.01 && trueLon > -180.01 && trueLon < 180.01) {
		std::stringstream inv_line;
		inv_line << istream->current_record_offset() << "|" << status << "|" << obs->date_time().to_string("%Y%m%d%H%MM");
		if (!inv_O_table.found(obsTypes.types[obs_type_index],ie)) {
		  ie.key=obsTypes.types[obs_type_index];
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
	metautils::log_error("unable to read observation "+strutils::itos(istream->number_read()+1)+"; error: '"+myerror+"'","obs2xml",user,args.args_string);
    }
  }
  istream->close();
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
  metautils::log_error("core dump","obs2xml",user,args.args_string);
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
  args.args_string=unix_args_string(argc,argv,'!');
  metautils::read_config("obs2xml",user,args.args_string);
  parse_args();
  flags="-f";
  if (!args.inventory_only && std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("obs2xml",user);
  if (!args.inventory_only) {
    metautils::check_for_existing_CMD("ObML");
  }
  scan_file();
  if (!args.inventory_only) {
    if (num_not_missing > 0) {
	metadata::ObML::write_ObML(ID_table,platform_table,"obs2xml",user);
    }
    else {
	metautils::log_error("all stations have missing location information - no usable data found; no content metadata will be saved for this file","obs2xml",user,args.args_string);
    }
  }
  if (args.update_DB) {
    if (!args.update_graphics) {
	flags="-G "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (!args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.local_root+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+".ObML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    metadata::close_inventory(inv_file,inv_dir,inv_stream,"ObML",args.update_summary,true,"obs2xml",user);
  }
  return 0;
}
