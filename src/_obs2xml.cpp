#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <ftw.h>
#include <signal.h>
#include <string>
#include <unordered_set>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <pglocks.hpp>
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
#include <timer.hpp>
#include <myerror.hpp>

using std::string;
using std::stringstream;
using std::unique_ptr;
using strutils::ftos;
using strutils::is_numeric;
using strutils::replace_all;
using strutils::split;
using strutils::to_upper;
using strutils::trim;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER=getenv("USER");
string myerror="";
string mywarning="";

gatherxml::markup::ObML::IDEntry ientry;
std::unique_ptr<TempFile> tfile;
string inv_file;
std::unique_ptr<TempDir> tdir;
unique_ptr<TempDir> inv_dir(nullptr);
std::ofstream inv_stream;
bool verbose_operation=false;

extern "C" void clean_up()
{
  if (myerror.length() > 0) {
    std::cerr << "Error: " << myerror << std::endl;
    metautils::log_error(myerror,"obs2xml",USER);
  }
}

string clean_imma_id(string id) {
  if (id.empty()) {
    return id;
  }
  trim(id);
  replace_all(id, "\"", "'");
  replace_all(id, "&", "&amp;");
  replace_all(id, ">", "&gt;");
  replace_all(id, "<", "&lt;");
  auto sp = split(id);
  return to_upper(sp[0]);
}

bool processed_isd_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  ISDObservation *o=reinterpret_cast<ISDObservation *>(obs.get());
  string rpt_type=o->report_type();
  string platform_type;
  if (rpt_type == "FM-12" || rpt_type == "FM-15" || rpt_type == "FM-16" || rpt_type == "AUTO " || rpt_type == "SAO  " || rpt_type == "SAOSP" || rpt_type == "SY-AE" || rpt_type == "SY-SA" || rpt_type == "SY-MT" || rpt_type == "S-S-A" || rpt_type == "SMARS" || rpt_type == "AERO " || rpt_type == "NSRDB" || rpt_type == "SURF " || rpt_type == "MEXIC" || rpt_type == "BRAZ " || rpt_type == "AUST ") {
    platform_type="land_station";
  } else if (rpt_type == "FM-13") {
    platform_type="roving_ship";
  } else if (rpt_type == "FM-18") {
    platform_type="drifting_buoy";
  } else if (rpt_type == "99999") {
    platform_type="unknown";
  } else {
    metautils::log_error("no platform and station mapping for report type '"+rpt_type+"'","obs2xml",USER);
  }
  std::deque<string> sp=split(o->location().ID,"-");
  if (sp[0] != "999999") {
    ientry.key=platform_type+"[!]WMO+6[!]"+sp[0];
  } else if (sp[1] != "99999") {
    ientry.key=platform_type+"[!]WBAN[!]"+sp[1];
  } else {
    trim(sp[2]);
    ientry.key=platform_type+"[!]callSign[!]"+sp[2];
  }
  obs_type="surface";
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_isd_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  auto start_date=o->date_time();
  std::list<string> addl_data_codes=reinterpret_cast<ISDObservation *>(obs.get())->additional_data_codes();
  addl_data_codes.push_front("MAN");
  for (const auto& code : addl_data_codes) {
    if (!obs_data.added_to_ids(obs_type,ientry,code,"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
      metautils::log_error("processed_isd_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+code,"obs2xml",USER);
    }
  }
  return true;
}

bool processed_adp_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  ADPObservation *o=reinterpret_cast<ADPObservation *>(obs.get());
  string platform_type;
  switch (o->platform_type()) {
    case 11:
    case 12:
    case 13:
    case 511:
    case 512:
    case 513:
    {
      platform_type="land_station";
      break;
    }
    case 21:
    case 521:
    {
      platform_type="ocean_station";
      break;
    }
    case 22:
    case 23:
    case 522:
    case 523:
    {
      platform_type="roving_ship";
      break;
    }
    case 31:
    case 41:
    {
      platform_type="aircraft";
      break;
    }
    case 42:
    {
      platform_type="balloon";
      break;
    }
    case 51:
    case 551:
    {
      platform_type="bogus";
      break;
    }
    case 61:
    case 62:
    case 63:
    {
      platform_type="satellite";
      break;
    }
    case 531:
    case 532:
    {
      platform_type="CMAN_station";
      break;
    }
    case 561:
    {
      platform_type="moored_buoy";
      break;
    }
    case 562:
    {
      platform_type="drifting_buoy";
      break;
    }
    case 999:
    {
      platform_type="999";
      break;
    }
    default:
    {
      std::cerr << "Warning: unknown platform type " << o->platform_type() << " for ID '" << obs->location().ID << "'" << std::endl;
      platform_type="";
    }
  }
  auto start_date=o->synoptic_date_time();
  if (start_date.year() == 0) {
    return false;
  }
  if (obs->location().ID.length() == 5 && is_numeric(obs->location().ID) && obs->location().ID >= "01001" && obs->location().ID <= "99999") {
    ientry.key=platform_type+"[!]WMO[!]"+obs->location().ID;
  } else {
    ientry.key=obs->location().ID;
    for (size_t n=0; n < ientry.key.length(); ++n) {
      if (static_cast<int>(ientry.key[n]) < 32 || static_cast<int>(ientry.key[n]) > 127) {
        if (n > 0) {
          ientry.key=ientry.key.substr(0,n)+"/"+ientry.key.substr(n+1);
        } else {
          ientry.key="/"+ientry.key.substr(1);
        }
      }
    }
    if (std::regex_search(ientry.key,std::regex("&"))) {
      replace_all(ientry.key,"&","&amp;");
    }
    if (std::regex_search(ientry.key,std::regex(">"))) {
      replace_all(ientry.key,">","&gt;");
    }
    if (std::regex_search(ientry.key,std::regex("<"))) {
      replace_all(ientry.key,"<","&lt;");
    }
    ientry.key=to_upper(ientry.key);
    ientry.key=platform_type+"[!]callSign[!]"+ientry.key;
  }
  auto ADPcategories=o->categories();
  if (platform_type == "999") {
    for (const auto& item : ADPcategories) {
      if (item == 6) {
        platform_type="aircraft";
      }
    }
  }
  if (platform_type == "999") {
//      std::cerr << "Warning: unknown platform type 999 for ID '" << obs->location().ID << "'" << std::endl;
    platform_type="";
  }
  if (metautils::args.data_format == "on29") {
    obs_type="upper_air";
  } else {
    obs_type="surface";
  }
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_adp_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  for (const auto& category : ADPcategories) {
    if (!obs_data.added_to_ids(obs_type,ientry,strutils::itos(category),"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
      metautils::log_error("processed_adp_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+strutils::itos(category),"obs2xml",USER);
    }
  }
  return true;
}

bool processed_cpc_summary_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  auto ID=obs->location().ID;
  if (ID.length() == 5) {
    ientry.key="land_station[!]WMO[!]"+ID;
  } else {
    ientry.key="land_station[!]callSign[!]"+ID;
  }
  obs_type="surface";
  if (!obs_data.added_to_platforms(obs_type,"land_station",obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_cpc_summary_observation() returned error: '"+myerror+"' while adding platform","obs2xml",USER);
  }
  auto start_date=obs->date_time();
  std::unique_ptr<DateTime> end_date;
  if (start_date.day() == 0) {
    start_date.set_day(1);
    end_date.reset(new DateTime(start_date));
    end_date->set_day(dateutils::days_in_month(end_date->year(),end_date->month()));
  }
  if (!end_date) {
    auto *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs.get());
    const size_t NUM_DATA_TYPES=16;
    static const string data_types[NUM_DATA_TYPES]={"RMAX","RMIN","RPRCP","EPRCP","VP","POTEV","VPD","SLP06","SLP12","SLP18","SLP00","APTMX","CHILLM","RAD","MXRH","MNRH"};
    auto cpcrpt=cpcobs->CPC_daily_summary_data();
    auto addl_data=cpcobs->additional_data();
    float data_type_values[NUM_DATA_TYPES]={addl_data.data.tmax,addl_data.data.tmin,addl_data.data.pcp_amt,cpcrpt.eprcp,cpcrpt.vp,cpcrpt.potev,cpcrpt.vpd,cpcrpt.slp06,cpcrpt.slp12,cpcrpt.slp18,cpcrpt.slp00,cpcrpt.aptmx,cpcrpt.chillm,cpcrpt.rad,static_cast<float>(cpcrpt.mxrh),static_cast<float>(cpcrpt.mnrh)};
    float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-9.,-9.,-9.,-99.,-99.,-99,-99.,-99.,-99.,-9.,-90.,-90.};
    for (size_t n=0; n < NUM_DATA_TYPES; ++n) {
      if (data_type_values[n] > missing[n]) {
        if (!obs_data.added_to_ids(obs_type,ientry,data_types[n],"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
          metautils::log_error("processed_cpc_summary_observation() returned error: '"+myerror+"' while adding daily ID "+ientry.key,"obs2xml",USER);
        }
      }
    }
  } else {
    auto *cpcobs=reinterpret_cast<CPCSummaryObservation *>(obs.get());
    const size_t NUM_DATA_TYPES=24;
    static const string data_types[NUM_DATA_TYPES]={"TMEAN","TMAX","TMIN","HMAX","RMINL","CTMEAN","APTMAX","WCMIN","HMAXAPT","WCLMINL","RPCP","EPCP","CPCP","EPCPMX","AHS","AVP","APET","AVPD","TRAD","AMAXRH","AMINRH","IHDD","ICDD","IGDD"};
    auto cpcrpt=cpcobs->CPC_monthly_summary_data();
    float data_type_values[NUM_DATA_TYPES]={cpcrpt.tmean,cpcrpt.tmax,cpcrpt.tmin,cpcrpt.hmax,cpcrpt.rminl,cpcrpt.ctmean,cpcrpt.aptmax,cpcrpt.wcmin,cpcrpt.hmaxapt,cpcrpt.wclminl,cpcrpt.rpcp,cpcrpt.epcp,cpcrpt.cpcp,cpcrpt.epcpmax,cpcrpt.ahs,cpcrpt.avp,cpcrpt.apet,cpcrpt.avpd,cpcrpt.trad,static_cast<float>(cpcrpt.amaxrh),static_cast<float>(cpcrpt.aminrh),static_cast<float>(cpcrpt.ihdd),static_cast<float>(cpcrpt.icdd),static_cast<float>(cpcrpt.igdd)};
    float missing[NUM_DATA_TYPES]={-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-99.,-9.,-9.,-9.,-9.,-9.,-990.,-990.,-990.,-990.,-990.};
    for (size_t n=0; n < NUM_DATA_TYPES; ++n) {
      if (data_type_values[n] > missing[n]) {
        if (!obs_data.added_to_ids(obs_type,ientry,data_types[n],"",obs->location().latitude,obs->location().longitude,-1.,&start_date,end_date.get())) {
          metautils::log_error("processed_cpc_summary_observation() returned error: '"+myerror+"' while adding monthly ID "+ientry.key,"obs2xml",USER);
        }
      }
    }
  }
  return true;
}

bool processed_imma_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  IMMAObservation *o=reinterpret_cast<IMMAObservation *>(obs.get());
  auto id = clean_imma_id(obs->location().ID);
  if (id.empty()) {
    return false;
  }
  string platform_type;
  switch (o->platform_type().atti) {
    case 1: {
      switch (o->platform_type().code) {
        case 0:
        case 1:
        case 5: {
          platform_type = "roving_ship";
          break;
        }
        case 2:
        case 3: {
          platform_type = "ocean_station";
          break;
        }
        case 4: {
          platform_type = "lightship";
          break;
        }
        case 6: {
          platform_type = "moored_buoy";
          break;
        }
        case 7: {
          platform_type = "drifting_buoy";
          break;
        }
        case 8: {
          platform_type = "ice_buoy";
          break;
        }
        case 9: {
          platform_type = "ice_station";
          break;
        }
        case 10:
        case 11:
        case 12:
        case 17:
        case 18:
        case 19:
        case 20:
        case 21: {
          platform_type = "oceanographic";
          break;
        }
        case 13: {
          platform_type = "CMAN_station";
          break;
        }
        case 14: {
          platform_type = "coastal_station";
          break;
        }
        case 15: {
          platform_type = "fixed_ocean_platform";
          break;
        }
        case 16: {
          platform_type = "automated_gauge";
          break;
        }
        default: {
          switch (o->ID_type()) {
            case 3:
            case 4: {
              platform_type = "drifting_buoy";
              break;
            }
            case 7: {
              platform_type = "roving_ship";
              break;
            }
            default: {
              metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized - obs date: "+obs->date_time().to_string()+", ID type: "+strutils::itos(o->ID_type()),"obs2xml",USER);
            }
          }
        }
      }
      break;
    }
    case 2: {
      switch (o->platform_type().code) {
        case 6: {
          platform_type = "CMAN_station";
          break;
        }
        default: {
          metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",USER);
        }
      }
      break;
    }
    default: {
      metautils::log_error("platform type "+strutils::itos(o->platform_type().atti)+"/"+strutils::itos(o->platform_type().code)+" not recognized","obs2xml",USER);
    }
  }
  auto start_date=obs->date_time();
  if (start_date.day() < 1 || static_cast<size_t>(start_date.day()) > dateutils::days_in_month(start_date.year(),start_date.month()) || start_date.time() > 235959) {
    return false;
  }
  ientry.key = platform_type + "[!]";
  switch (o->ID_type()) {
    case 0: {
      ientry.key += "unknown";
      break;
    }
    case 1: {
      ientry.key += "callSign";
      break;
    }
    case 2: {
      ientry.key += "generic";
      break;
    }
    case 3: {
      ientry.key += "WMO";
      break;
    }
    case 4:
    case 11: {
      ientry.key += "buoy";
      break;
    }
    case 5: {
      ientry.key += "NDBC";
      break;
    }
    case 6: {
      if (is_numeric(id)) {
        ientry.key += "number";
      } else {
        ientry.key += "name";
      }
      break;
    }
    case 7: {
      ientry.key += "NODC";
      break;
    }
    case 8: {
      ientry.key += "IATTC";
      break;
    }
    case 9: {
      ientry.key += "number";
      break;
    }
    case 10: {
      if (id.length() == 8) {
        if (is_numeric(id)) {
          id = id.substr(0, 4);
          ientry.key += "number";
        } else if (is_numeric(id.substr(2)) && id[0] >= 'A' && id[0] <= 'Z' &&
            id[1] >= 'A' && id[1] <= 'Z') {
          id = id.substr(0, 2);
          ientry.key += "generic";
        } else {
          ientry.key += "name";
        }
      } else {
        if (is_numeric(id)) {
          ientry.key += "number";
        } else {
          ientry.key += "name";
        }
      }
      break;
    }
    default: {
      metautils::log_error("ID type "+strutils::itos(o->ID_type())+" not recognized - obs date: "+obs->date_time().to_string(),"obs2xml",USER);
    }
  }
  ientry.key+="[!]"+id;
  obs_type="surface";
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_imma_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  std::list<short> attm_ids=reinterpret_cast<IMMAObservation *>(obs.get())->attachment_ID_list();
  attm_ids.emplace_front(0);
  for (const auto& attm_id : attm_ids) {
    if (!obs_data.added_to_ids(obs_type,ientry,strutils::itos(attm_id),"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
      metautils::log_error("processed_imma_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+strutils::itos(attm_id),"obs2xml",USER);
    }
  }
  return true;
}

bool processed_nodc_bt_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  ientry.key="roving_ship[!]NODC[!]"+obs->location().ID;
  obs_type="underwater";
  if (!obs_data.added_to_platforms(obs_type,"roving_ship",obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_nodc_bt_observation() returned error: '"+myerror+"' while adding platform","obs2xml",USER);
  }
  auto start_date=obs->date_time();
  NODCBTObservation *nodcobs=reinterpret_cast<NODCBTObservation *>(obs.get());
  auto data_type=nodcobs->data_type();
  trim(data_type);
  if (!obs_data.added_to_ids(obs_type,ientry,data_type,"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
    metautils::log_error("processed_nodc_bt_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+"for "+data_type,"obs2xml",USER);
  }
  gatherxml::markup::ObML::DataTypeEntry dte;
  ientry.data->data_types_table.found(data_type,dte);
  if (dte.data->vdata == nullptr) {
    dte.data->vdata.reset(new gatherxml::markup::ObML::DataTypeEntry::Data::VerticalData);
  }
  short nlev=nodcobs->number_of_levels();
  NODCBTObservation::Level lmin=nodcobs->level(0);
  lmin.depth=-lmin.depth;
  NODCBTObservation::Level lmax=nodcobs->level(nlev-1);
  lmax.depth=-lmax.depth;
  if (lmin.depth > dte.data->vdata->max_altitude) {
    dte.data->vdata->max_altitude=lmin.depth;
  }
  if (lmax.depth < dte.data->vdata->min_altitude) {
    dte.data->vdata->min_altitude=lmax.depth;
  }
  dte.data->vdata->avg_nlev+=nlev;
  if (nlev > 1) {
    dte.data->vdata->avg_res+=fabs(lmax.depth-lmin.depth)/static_cast<float>(nlev-1);
    ++(dte.data->vdata->res_cnt);
  }
  return true;
}

bool processed_td32_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  auto *o=reinterpret_cast<TD32Data *>(obs.get());
  auto id=obs->location().ID;
  auto header=o->header();
  if (metautils::args.data_format == "td3210" || metautils::args.data_format == "td3280") {
    id=id.substr(3);
    ientry.key="land_station[!]WBAN[!]"+id;
  } else {
    id=id.substr(0,6);
    ientry.key="land_station[!]COOP[!]"+id;
  }
  obs_type="surface";
  auto reports=o->reports();
  for (size_t n=0; n < reports.size(); ++n) {
    if (reports[n].flag1 != 'M') {
      if (!obs_data.added_to_platforms(obs_type,"land_station",reports[n].loc.latitude,reports[n].loc.longitude)) {
        metautils::log_error("processed_td32_observation() returned error: '"+myerror+"' while adding platform ("+strutils::itos(n)+")","obs2xml",USER);
      }
      DateTime start_date;
      if (header.type == "DLY") {
        start_date.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+9999);
        start_date.set_utc_offset(-2400);
      } else if (header.type == "MLY") {
        start_date.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),999999);
      } else {
        start_date.set(reports[n].date_time.year(),reports[n].date_time.month(),reports[n].date_time.day(),reports[n].date_time.time()/10000+99);
        if (start_date.time() == 240099 || start_date.time() == 250099) {
          start_date.set_time(99);
          start_date.add_days(1);
        }
      }
      std::unique_ptr<DateTime> end_date;
      if (start_date.month() == 13) {
        start_date.set_month(1);
        end_date.reset(new DateTime(start_date));
        end_date->set_month(12);
      }
      if (start_date.day() == 0) {
        start_date.set_day(1);
        if (!end_date) {
          end_date.reset(new DateTime(start_date));
        }
        end_date->set_day(dateutils::days_in_month(end_date->year(),end_date->month()));
      }
      if (header.type == "HPD") {
        if (reports[n].date_time.time()/100 == 2500) {
          start_date.subtract_minutes(1439);
        } else {
          start_date.subtract_minutes(59);
        }
      } else if (header.type == "15M") {
        if (reports[n].date_time.time()/100 == 2500) {
          start_date.subtract_minutes(1439);
        } else {
          start_date.subtract_minutes(14);
        }
      }
      if (!obs_data.added_to_ids(obs_type,ientry,header.elem,"",reports[n].loc.latitude,reports[n].loc.longitude,-1.,&start_date,end_date.get())) {
        metautils::log_error("processed_td32_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+header.elem,"obs2xml",USER);
      }
      if (header.elem == "HPCP" && reports[n].date_time.time() == 2500) {
        if (!obs_data.added_to_ids(obs_type,ientry,"DPCP","",reports[n].loc.latitude,reports[n].loc.longitude,-1.,&start_date)) {
          metautils::log_error("processed_td32_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for DPCP","obs2xml",USER);
        }
      } else if (header.type == "MLY" && reports[n].date_time.month() == 13) {
        if (!obs_data.added_to_ids(obs_type,ientry,header.elem+"_y","",reports[n].loc.latitude,reports[n].loc.longitude,-1.,&start_date,end_date.get())) {
          metautils::log_error("processed_td32_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+header.elem+"_y","obs2xml",USER);
        }
      }
    }
  }
  return true;
}

bool processed_dss_tsr_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  string id=obs->location().ID;
  if (std::regex_search(id,std::regex("^0"))) {
    id=id.substr(1);
  }
  auto source=reinterpret_cast<Tsraob *>(obs.get())->flags().source;
  string platform_type;
  switch (source) {
    case 15:
    {
      if (id.length() == 5 && id < "03000") {
        if (id < "00026" || id == "00091" || id == "00092") {
          platform_type="ocean_station";
          ientry.key=platform_type+"[!]WBAN[!]"+id;
        } else {
          platform_type="roving_ship";
          ientry.key=platform_type+"[!]other[!]"+id;
        }
      } else if (id.length() == 6 && id >= "116000") {
        platform_type="roving_ship";
        ientry.key=platform_type+"[!]other[!]"+id;
      } else {
        platform_type="land_station";
        ientry.key=platform_type+"[!]WBAN[!]"+id;
      }
      break;
    }
    case 36:
    {
      if (id >= "47000" && id <= "47999") {
        platform_type="land_station";
        ientry.key=platform_type+"[!]WMO[!]"+id;
      } else
        metautils::log_error("no platform and station mapping for source "+strutils::itos(source)+" and ID '"+id+"'","obs2xml",USER);
      break;
    }
    case 37:
    case 38:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]WMO[!]"+id;
      break;
    }
    default:
    {
      metautils::log_error("no platform and station mapping for source "+strutils::itos(source),"obs2xml","x","x");
    }
  }
  obs_type="upper_air";
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_dss_tsr_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  auto start_date=obs->date_time();
  auto fmt_code=reinterpret_cast<Tsraob *>(obs.get())->flags().format;
  if (fmt_code >= 9 && fmt_code <= 14) {
    fmt_code-=8;
  }
  if (!obs_data.added_to_ids(obs_type,ientry,strutils::itos(fmt_code),"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
    metautils::log_error("processed_dss_tsr_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+strutils::itos(fmt_code),"obs2xml",USER);
  }
  return true;
}

bool processed_uadb_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  string id=obs->location().ID;
  string platform_type;
  switch (reinterpret_cast<UADBRaob *>(obs.get())->ID_type()) {
    case 1:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]WMO[!]"+id;
      break;
    }
    case 2:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]WBAN[!]"+id;
      break;
    }
    case 3:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]callSign[!]"+id;
      break;
    }
    case 4:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]COOP[!]"+id;
      break;
    }
    case 5:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]name[!]"+id;
      break;
    }
    case 6:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]other[!]"+id;
      break;
    }
    case 7:
    case 8:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]WMO+6[!]"+id;
      break;
    }
    case 9:
    {
      platform_type="land_station";
      ientry.key=platform_type+"[!]CHUAN[!]"+id;
      break;
    }
    default:
    {
      metautils::log_error("no platform and station mapping for observation type "+strutils::itos(reinterpret_cast<UADBRaob *>(obs.get())->ID_type()),"obs2xml","x","x");
    }
  }
  obs_type="upper_air";
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_uadb_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  auto start_date=obs->date_time();
  if (!obs_data.added_to_ids(obs_type,ientry,strutils::itos(reinterpret_cast<UADBRaob *>(obs.get())->observation_type()),"",obs->location().latitude,obs->location().longitude,-1.,&start_date)) {
    metautils::log_error("processed_uadb_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+strutils::itos(reinterpret_cast<UADBRaob *>(obs.get())->observation_type()),"obs2xml",USER);
  }
  return true;
}

bool processed_wmssc_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  auto *o=reinterpret_cast<WMSSCObservation *>(obs.get());
  auto can_continue=false;
  for (size_t n=1; n <= 12; ++n) {
    auto report=o->monthly_report(n);
    auto addl_data=o->monthly_additional_data(n);
    if (report.data.slp > 0. || report.data.stnp > 0. || report.data.tdry > -99. || addl_data.data.pcp_amt >= 0.) {
      can_continue=true;
      break;
    }
  }
  if (!can_continue) {
    return false;
  }
  auto sta_name=o->station_name();
  string platform_type;
  if (std::regex_search(sta_name,std::regex("SHIP"))) {
    platform_type="ocean_station";
  } else if (std::regex_search(sta_name,std::regex("DRIFTING"))) {
    platform_type="roving_ship";
  } else {
    platform_type="land_station";
  }
  if (o->format() == 0x3f) {
    ientry.key=platform_type+"[!]COOP[!]"+obs->location().ID;
  } else {
    ientry.key=platform_type+"[!]WMO+6[!]"+obs->location().ID;
  }
  auto climo_ind=obs->date_time().year();
  if (climo_ind < 2200) {
// monthly climatology
    obs_type="surface";
  } else if (climo_ind < 3200) {
// 10-year monthly climatology
    obs_type="surface_10-year_climatology";
  } else {
// 30-year monthly climatology
    obs_type="surface_30-year_climatology";
  }
  if (!obs_data.added_to_platforms(obs_type,platform_type,obs->location().latitude,obs->location().longitude)) {
    metautils::log_error("processed_wmssc_observation() returned error: '"+myerror+"' while adding platform "+obs_type+"-"+platform_type,"obs2xml",USER);
  }
  for (size_t n=1; n <= 12; ++n) {
    auto report=o->monthly_report(n);
    auto addl_data=o->monthly_additional_data(n);
    if (report.data.slp > 0. || report.data.stnp > 0. || report.data.tdry > -99. || addl_data.data.pcp_amt >= 0.) {
      DateTime start_date,end_date;
      start_date.set(obs->date_time().year(),n,1,0);
      end_date.set(obs->date_time().year(),n,dateutils::days_in_month(obs->date_time().year(),n),235959);
      if (climo_ind >= 3200) {
        start_date.set_year(start_date.year()-2029);
        end_date.set_year(end_date.year()-2000);
      } else if (climo_ind >= 2200) {
        start_date.set_year(start_date.year()-1009);
        end_date.set_year(end_date.year()-1000);
      }
      if (!obs_data.added_to_ids(obs_type,ientry,"standard_parameters","",obs->location().latitude,obs->location().longitude,-1.,&start_date,&end_date)) {
        metautils::log_error("processed_wmssc_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for standard_parameters","obs2xml",USER);
      }
      if (o->has_additional_data()) {
        string data_type;
        if (obs->date_time().year() < 1961) {
          data_type="height_data";
        } else {
          data_type="additional_parameters";
        }
        if (!obs_data.added_to_ids(obs_type,ientry,data_type,"",obs->location().latitude,obs->location().longitude,-1.,&start_date,&end_date)) {
          metautils::log_error("processed_wmssc_observation() returned error: '"+myerror+"' while adding ID "+ientry.key+" for "+data_type,"obs2xml",USER);
        }
      }
    }
  }
  return true;
}

bool processed_observation(std::unique_ptr<Observation>& obs,gatherxml::markup::ObML::ObservationData& obs_data,string& obs_type)
{
  if (metautils::args.data_format == "isd") {
    return processed_isd_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "on29" || metautils::args.data_format == "on124") {
    return processed_adp_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "cpcsumm") {
    return processed_cpc_summary_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "imma") {
    return processed_imma_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "nodcbt") {
    return processed_nodc_bt_observation(obs,obs_data,obs_type);
  } else if (std::regex_search(metautils::args.data_format,std::regex("^td32"))) {
    return processed_td32_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "tsr") {
    return processed_dss_tsr_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "uadb") {
    return processed_uadb_observation(obs,obs_data,obs_type);
  } else if (metautils::args.data_format == "wmssc") {
    return processed_wmssc_observation(obs,obs_data,obs_type);
  }
  std::cerr << "Error: unable to scan format " << metautils::args.data_format << std::endl;
  exit(1);
}

bool open_file(void *istream,string filename)
{
  if (metautils::args.data_format == "cpcsumm") {
    return (reinterpret_cast<InputCPCSummaryObservationStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "imma") {
    return (reinterpret_cast<InputIMMAObservationStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "isd") {
    return (reinterpret_cast<InputISDObservationStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "nodcbt") {
    return (reinterpret_cast<InputNODCBTObservationStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "on29" || metautils::args.data_format == "on124") {
    return (reinterpret_cast<InputADPStream *>(istream))->open(filename);
  } else if (std::regex_search(metautils::args.data_format,std::regex("^td32"))) {
    return (reinterpret_cast<InputTD32Stream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "tsr") {
    return (reinterpret_cast<InputTsraobStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "uadb") {
    return (reinterpret_cast<InputUADBRaobStream *>(istream))->open(filename);
  } else if (metautils::args.data_format == "wmssc") {
    return (reinterpret_cast<InputWMSSCObservationStream *>(istream))->open(filename);
  }
  metautils::log_error("open_file(): "+metautils::args.data_format+"-formatted file not recognized","obs2xml",USER);
  return false;
}

struct InvEntry {
  InvEntry() : key(),num(0) {}

  string key;
  int num;
};

void scan_file(gatherxml::markup::ObML::ObservationData& obs_data)
{
  std::unique_ptr<idstream> istream;
  std::unique_ptr<Observation> obs;
  if (metautils::args.data_format == "cpcsumm") {
    istream.reset(new InputCPCSummaryObservationStream);
    obs.reset(new CPCSummaryObservation);
  } else if (metautils::args.data_format == "imma") {
    istream.reset(new InputIMMAObservationStream);
    obs.reset(new IMMAObservation);
  } else if (metautils::args.data_format == "isd") {
    istream.reset(new InputISDObservationStream);
    obs.reset(new ISDObservation);
  } else if (metautils::args.data_format == "nodcbt") {
    istream.reset(new InputNODCBTObservationStream);
    obs.reset(new NODCBTObservation);
  } else if (metautils::args.data_format == "on29" || metautils::args.data_format == "on124") {
    istream.reset(new InputADPStream);
    obs.reset(new ADPObservation);
  } else if (std::regex_search(metautils::args.data_format,std::regex("^td32"))) {
    if (metautils::args.data_format == "td3200" || metautils::args.data_format == "td3210" || metautils::args.data_format == "td3220" || metautils::args.data_format == "td3240" || metautils::args.data_format == "td3260" || metautils::args.data_format == "td3280") {
      istream.reset(new InputTD32Stream);
    } else {
      std::cerr << "Error: format " << metautils::args.data_format << " not recognized" << std::endl;
      exit(1);
    }
    obs.reset(new TD32Data);
  } else if (metautils::args.data_format == "tsr") {
    istream.reset(new InputTsraobStream);
    obs.reset(new Tsraob);
  } else if (metautils::args.data_format == "uadb") {
    istream.reset(new InputUADBRaobStream);
    obs.reset(new UADBRaob);
  } else if (metautils::args.data_format == "wmssc") {
    istream.reset(new InputWMSSCObservationStream);
    obs.reset(new WMSSCObservation);
  } else {
    std::cerr << "Error: format " << metautils::args.data_format << " not recognized" << std::endl;
    exit(1);
  }
  tfile.reset(new TempFile);
  tfile->open(metautils::args.temp_loc);
  tdir.reset(new TempDir);
  tdir->create(metautils::args.temp_loc);
  std::list<string> filelist;
  if (verbose_operation) {
    std::cout << "Preparing file for metadata scanning ..." << std::endl;
  }
  string file_format,error;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning(): '"+error+"'","obs2xml",USER);
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
  my::map<InvEntry> inv_O_table,inv_P_table,inv_I_table;
  std::list<string> inv_lines;
  for (const auto& file : filelist) {
    if (verbose_operation) {
      std::cout << "Beginning scan of " << file << "..." << std::endl;
    }
    if (!open_file(istream.get(),tfile->name())) {
      metautils::log_error("scan_file(): unable to open file for input","obs2xml",USER);
    }
    if (file_format.empty() && metautils::args.data_format == "isd") {
      gatherxml::fileInventory::open(inv_file,inv_dir,inv_stream,"ObML","obs2xml",USER);
    } else if (metautils::args.inventory_only) {
      metautils::log_error("scan_file(): unable to inventory "+metautils::args.path+"/"+metautils::args.filename+" because archive format is '"+file_format+"' and data format is '"+metautils::args.data_format+"'","obs2xml",USER);
    }
    const size_t BUF_LEN=80000;
    unsigned char buffer[BUF_LEN];
    int num_bytes;
    while ( (num_bytes=istream->read(buffer,BUF_LEN)) != bfstream::eof) {
      if (num_bytes > 0) {
        obs->fill(buffer,Observation::full_report);
        if (obs->location().latitude > -99. && obs->location().longitude > -199.) {
          string obs_type,platform_type;
          if (processed_observation(obs,obs_data,obs_type)) {
            if (inv_stream.is_open()) {
              stringstream inv_line;
              inv_line << istream->current_record_offset() << "|" << num_bytes << "|" << obs->date_time().to_string("%Y%m%d%H%MM");
              InvEntry ie;
              if (!inv_O_table.found(obs_type,ie)) {
                ie.key=obs_type;
                ie.num=inv_O_table.size();
                inv_O_table.insert(ie);
              }
              inv_line << "|" << ie.num;
              auto iparts=split(ientry.key,"[!]");
              if (!inv_P_table.found(iparts[0],ie)) {
                ie.key=iparts[0];
                ie.num=inv_P_table.size();
                inv_P_table.insert(ie);
              }
              inv_line << "|" << ie.num;
              ie.key=iparts[1]+"|"+iparts[2];
              if (!inv_I_table.found(ie.key,ie)) {
                ie.num=inv_I_table.size();
                inv_I_table.insert(ie);
              }
              inv_line << "|" << ie.num;
              inv_line << "|" << ftos(obs->location().latitude,4) << "|" << ftos(obs->location().longitude,4);
              inv_lines.emplace_back(inv_line.str());
            }
          }
        }
      } else {
        metautils::log_error("unable to read observation "+strutils::itos(istream->number_read()+1)+"; error: '"+myerror+"'","obs2xml",USER);
      }
    }
    istream->close();
    if (verbose_operation) {
      std::cout << "  ...scan of " << file << " completed." << std::endl;
    }
  }
  clean_up();
  if (inv_lines.size() > 0) {
    InvEntry ie;
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
  metautils::log_error("core dump","obs2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  string web_home,flags,key;

  if (argc < 4) {
    std::cerr << "usage: obs2xml -f format -d [ds]nnn.n [-l local_name] [options...] path" << std::endl << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f cpcsumm   CPC Summary of Day/Month format" << std::endl;
    std::cerr << "-f imma      International Maritime Meteorological Archive format" << std::endl;
    std::cerr << "-f isd       NCDC ISD format" << std::endl;
    std::cerr << "-f nodcbt    NODC BT format" << std::endl;
    std::cerr << "-f on29      ADP ON29 format" << std::endl;
    std::cerr << "-f on124     ADP ON124 format" << std::endl;
    std::cerr << "-f td3200    NCDC TD3200 format" << std::endl;
    std::cerr << "-f td3210    NCDC TD3210 format" << std::endl;
    std::cerr << "-f td3220    NCDC TD3220 format" << std::endl;
    std::cerr << "-f td3240    NCDC TD3240 format" << std::endl;
    std::cerr << "-f td3260    NCDC TD3260 format" << std::endl;
    std::cerr << "-f td3280    NCDC TD3280 format" << std::endl;
    std::cerr << "-f tsr       DSS Tsraob format" << std::endl;
    std::cerr << "-f uadb      Upper Air Database ASCII format" << std::endl;
    std::cerr << "-f wmssc     DSS World Monthly Surface Station Climatology format" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d <nnn.n> nnn.n is the dataset number to which this data file belongs" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "<path>     full MSS path or URL of file to read" << std::endl;
    std::cerr << "options:" << std::endl;
    if (USER == "dattore") {
      std::cerr << "-NC              don't check to see if the MSS file is a primary for the dataset" << std::endl;
    }
    std::cerr << "-g/-G            update/don't update graphics (default is -g)" << std::endl;
    std::cerr << "-I               inventory only; no content metadata generated" << std::endl;
    std::cerr << "-l <local_name>  name of the MSS file on local disk (this avoids an MSS read)" << std::endl;
    std::cerr << "-u/-U            update/don't update the database (default is -u)" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  atexit(clean_up);
  auto arg_delimiter='!';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("obs2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  flags="-f";
  if (!metautils::args.inventory_only && std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  metautils::cmd_register("obs2xml",USER);
  if (!metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("ObML", "obs2xml", USER);
  }
  Timer tmr;
  tmr.start();
  gatherxml::markup::ObML::ObservationData obs_data;
  scan_file(obs_data);
  if (!metautils::args.inventory_only) {
    if (!obs_data.is_empty) {
      gatherxml::markup::ObML::write(obs_data,"obs2xml",USER);
    } else {
      metautils::log_error("all stations have missing location information - no usable data found; no content metadata will be saved for this file","obs2xml",USER);
    }
  }
  if (metautils::args.update_db) {
    if (!metautils::args.update_graphics) {
      flags="-G "+flags;
    }
    if (!metautils::args.regenerate) {
      flags="-R "+flags;
    }
    if (!metautils::args.update_summary) {
      flags="-S "+flags;
    }
    stringstream oss,ess;
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsid+" "+flags+" "+metautils::args.filename+".ObML",oss,ess) < 0) {
      std::cerr << ess.str() << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    gatherxml::fileInventory::close(inv_file,inv_dir,inv_stream,"ObML",metautils::args.update_summary,true,"obs2xml",USER);
  }
  tmr.stop();
  metautils::log_warning("execution time: " + ftos(tmr.elapsed_time()) +
      " seconds", "gatherxml.time", USER);
  return 0;
}
