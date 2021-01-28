#include <iomanip>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <bufr.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

gatherxml::markup::ObML::IDEntry ientry;
struct UniqueObservationEntry {
  struct Data {
    Data() : nobs(0),dtype_list() {}

    size_t nobs;
    unsigned char dtype_list[64];
  };
  UniqueObservationEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
} uoe;
gatherxml::markup::ObML::DataTypeEntry de;
BUFRReport rpt;
BUFRData **data;
std::unique_ptr<TempFile> tfile;
std::unique_ptr<TempDir> tdir;

extern "C" void clean_up()
{
  if (!myerror.empty()) {
    metautils::log_error2(myerror,"clean_up()","bufr2xml",USER);
  }
}

bool is_valid_date(DateTime& datetime)
{
  if (datetime.year() < 1900 || datetime.year() > dateutils::current_date_time().year()+20) {
    return false;
  }
  if (datetime.month() < 1 || datetime.month() > 12) {
    return false;
  }
  if (datetime.day() < 1 || datetime.day() > static_cast<short>(dateutils::days_in_month(datetime.year(),datetime.month()))) {
    return false;
  }
  return true;
}

void process_ncep_prepbufr_observation(gatherxml::markup::ObML::ObservationData& obs_data,std::string& message_type,size_t subset_number)
{
  const std::string THIS_FUNC=__func__+std::string("()");
  auto **pdata=reinterpret_cast<NCEPPREPBUFRData **>(data);
  auto dhr=pdata[subset_number]->dhr;
  if (dhr == -999) {
    return;
  }
  auto datetime=rpt.date_time();
  if (dhr >= 0) {
    datetime.add_hours(dhr);
  }
  else {
    datetime.subtract_hours(-dhr);
  }
  std::string obs_type,platform_type;
  switch (pdata[subset_number]->type.prepbufr) {
    case 120:
    case 122:
    case 126:
    case 130:
    case 131:
    case 132:
    case 133:
    case 134:
    case 135:
    case 140:
    case 142:
    case 143:
    case 144:
    case 146:
    case 147:
    case 150:
    case 151:
    case 152:
    case 153:
    case 154:
    case 156:
    case 157:
    case 158:
    case 159:
    case 160:
    case 161:
    case 162:
    case 163:
    case 164:
    case 165:
    case 170:
    case 171:
    case 172:
    case 173:
    case 174:
    case 175:
    case 220:
    case 221:
    case 222:
    case 223:
    case 224:
    case 227:
    case 228:
    case 229:
    case 230:
    case 231:
    case 232:
    case 233:
    case 234:
    case 235:
    case 240:
    case 241:
    case 242:
    case 243:
    case 244:
    case 245:
    case 246:
    case 247:
    case 250:
    case 251:
    case 252:
    case 253:
    case 254:
    case 255:
    case 256:
    case 257:
    case 258:
    case 259:
    case 289:
    case 290: {
	obs_type="upper_air";
	break;
    }
    case 180:
    case 181:
    case 182:
    case 183:
    case 187:
    case 188:
    case 190:
    case 191:
    case 280:
    case 281:
    case 282:
    case 283:
    case 284:
    case 285:
    case 286:
    case 287:
    case 288: {
	obs_type="surface";
	break;
    }
    case 210: {
	obs_type="surface";
	platform_type="bogus";
	break;
    }
    default: {
	metautils::log_error2("PREPBUFR type "+strutils::itos(pdata[subset_number]->type.prepbufr)+" not recognized (ON29 code = "+strutils::itos(pdata[subset_number]->type.ON29)+")  date: "+datetime.to_string()+"  id: '"+pdata[subset_number]->stnid+"'",THIS_FUNC,"bufr2xml",USER);
    }
  }
  if (platform_type.empty()) {
    switch (pdata[subset_number]->type.ON29) {
	case 11:
	case 12:
	case 13:
	case 511:
	case 512:
	case 513:
	case 514:
	case 540: {
	  platform_type="land_station";
	  break;
	}
	case 21:
	case 521: {
	  platform_type="fixed_ship";
	  break;
	}
	case 22:
	case 23:
	case 522:
	case 523: {
	  platform_type="roving_ship";
	  break;
	}
	case 31:
	case 41: {
	  platform_type="aircraft";
	  break;
	}
	case 532:
	case 533:
	case 534: {
	  platform_type="automated_gauge";
	  break;
	}
	case 51:
	case 551: {
	  platform_type="bogus";
	  break;
	}
	case 61:
	case 63:
	case 65:
	case 66:
	case 68:
	case 69:
	case 74:
	case 571:
	case 573:
	case 574:
	case 575:
	case 576:
	case 577:
	case 581:
	case 582:
	case 583:
	case 584: {
	  platform_type="satellite";
	  break;
	}
	case 71:
	case 72:
	case 73:
	case 75:
	case 76:
	case 77: {
	  platform_type="wind_profiler";
	  break;
	}
	case 531: {
	  platform_type="CMAN_station";
	  break;
	}
	case 561: {
	  platform_type="moored_buoy";
	  break;
	}
	case 562:
	case 563:
	case 564: {
	  platform_type="drifting_buoy";
	  break;
	}
	default: {
	  metautils::log_error2("unknown platform type "+strutils::itos(pdata[subset_number]->type.ON29),THIS_FUNC,"bufr2xml",USER);
	}
    }
  }
  std::string id;
  if (!pdata[subset_number]->stnid.empty()) {
    id=pdata[subset_number]->stnid;
  }
  else if (!pdata[subset_number]->satid.empty()) {
    id=pdata[subset_number]->satid;
  }
  else if (!pdata[subset_number]->acftid.empty()) {
    id=pdata[subset_number]->acftid;
  }
  else {
    metautils::log_error2("unable to get an ID for '"+message_type+", date "+datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
  }
  ientry.key=prepbufr_id_key(metautils::clean_id(id),platform_type,message_type);
  if (ientry.key.length() == 0) {
    metautils::log_error2("unable to get ID key for '"+message_type+"', ID: '"+id+"' ("+pdata[subset_number]->satid+") ("+pdata[subset_number]->acftid+") ("+pdata[subset_number]->stnid+")",THIS_FUNC,"bufr2xml",USER);
  }
  float true_lon= (pdata[subset_number]->elon > 180.) ? (pdata[subset_number]->elon-360.) : pdata[subset_number]->elon;
  for (const auto& group : pdata[subset_number]->cat_groups) {
    if (group.code > 63) {
	metautils::log_error2("bad category '"+strutils::itos(group.code)+"', "+platform_type+", date "+datetime.to_string()+", id '"+pdata[subset_number]->stnid+"'",THIS_FUNC,"bufr2xml",USER);
    }
    if (is_valid_date(datetime)) {
	if (!obs_data.added_to_platforms(obs_type,platform_type,pdata[subset_number]->lat,true_lon)) {
	  auto error=std::move(myerror);
	  metautils::log_error2("'"+error+"' while adding platform "+obs_type+"-"+platform_type,THIS_FUNC,"bufr2xml",USER);
	}
	std::string data_type_map;
	if (rpt.center() == 99 && rpt.sub_center() == 0) {
// patch for NNRP prepqm files
	  data_type_map="7-3";
	}
	else {
	  data_type_map=strutils::itos(rpt.center())+"-"+strutils::itos(rpt.sub_center());
	}
	if (!obs_data.added_to_ids(obs_type,ientry,strutils::itos(group.code),data_type_map,pdata[subset_number]->lat,true_lon,-1.,&datetime)) {
	  auto error=std::move(myerror);
	  metautils::log_error2("'"+error+"' while adding ID "+ientry.key,THIS_FUNC,"bufr2xml",USER);
	}
    }
  }
}

void process_ncep_adp_bufr_observation(gatherxml::markup::ObML::ObservationData& obs_data,size_t subset_number)
{
  const std::string THIS_FUNC=__func__+std::string("()");
  auto **adata=reinterpret_cast<NCEPADPBUFRData **>(data);
  if (adata[subset_number]->datetime.year() == 31073) {
    adata[subset_number]->datetime=rpt.date_time();
  }
  std::string obs_type,platform_type;
  auto type=rpt.data_type()*1000+rpt.data_subtype();
  switch (type) {
    case 0:
    case 1:
    case 2:
    case 7:
    case 100:
    case 101:
    case 102: {
	obs_type="surface";
	platform_type="land_station";
	break;
    }
    case 1001:
    case 1013: {
	obs_type="surface";
	platform_type="roving_ship";
	break;
    }
    case 1002:
    case 1102: {
	obs_type="surface";
	platform_type="drifting_buoy";
	break;
    }
    case 1003:
    case 1103: {
	obs_type="surface";
	platform_type="moored_buoy";
	break;
    }
    case 1004: {
	obs_type="surface";
	platform_type="CMAN_station";
	break;
    }
    case 1005: {
	obs_type="surface";
	platform_type="automated_gauge";
	break;
    }
    case 1006: {
	obs_type="surface";
	platform_type="bogus";
	break;
    }
    case 1007: {
	obs_type="surface";
	platform_type="coastal_station";
	break;
    }
    case 2001:
    case 2002:
    case 2101:
    case 2102: {
	platform_type="land_station";
	obs_type="upper_air";
	break;
    }
    case 2003:
    case 2103: {
	platform_type="roving_ship";
	obs_type="upper_air";
	break;
    }
    case 2004:
    case 2104: {
	platform_type="aircraft";
	obs_type="upper_air";
	break;
    }
    case 2005:
    case 2009:
    case 2105: {
	platform_type="land_station";
	obs_type="upper_air";
	break;
    }
    case 2007:
    case 2010:
    case 2011:
    case 2013:
    case 2014:
    case 2016:
    case 2018: {
	platform_type="wind_profiler";
	obs_type="upper_air";
	break;
    }
    case 2008:
    case 2017: {
	platform_type="NEXRAD";
	obs_type="upper_air";
	break;
    }
    case 3001:
    case 3002:
    case 3003:
    case 3010:
    case 3101:
    case 3102:
    case 3104: {
	platform_type="satellite";
	obs_type="upper_air";
	break;
    }
    case 4001:
    case 4002:
    case 4003:
    case 4004:
    case 4005:
    case 4006:
    case 4007:
    case 4008:
    case 4009:
    case 4010:
    case 4011:
    case 4012:
    case 4013:
    case 4014:
    case 4015:
    case 4103: {
	platform_type="aircraft";
	obs_type="upper_air";
	break;
    }
    case 5001:
    case 5002:
    case 5003:
    case 5004:
    case 5005:
    case 5006:
    case 5008:
    case 5009:
    case 5010:
    case 5011:
    case 5012:
    case 5013:
    case 5014:
    case 5015:
    case 5016:
    case 5017:
    case 5018:
    case 5019:
    case 5021:
    case 5022:
    case 5023:
    case 5024:
    case 5025:
    case 5026:
    case 5030:
    case 5031:
    case 5032:
    case 5034:
    case 5039:
    case 5041:
    case 5042:
    case 5043:
    case 5044:
    case 5045:
    case 5046:
    case 5050:
    case 5051:
    case 5061:
    case 5062:
    case 5063:
    case 5064:
    case 5065:
    case 5066:
    case 5067:
    case 5068:
    case 5069:
    case 5070:
    case 5071:
    case 5080:
    case 5090:
    case 5091:
    case 8012:
    case 8013:
    case 8015: {
	platform_type="satellite";
	obs_type="upper_air";
	break;
    }
    case 12001:
    case 12002:
    case 12003:
    case 12004:
    case 12005:
    case 12008:
    case 12009:
    case 12010:
    case 12011:
    case 12012:
    case 12013:
    case 12017:
    case 12018:
    case 12022:
    case 12023:
    case 12031:
    case 12034:
    case 12103:
    case 12122:
    case 12123:
    case 12137:
    case 12138:
    case 12139:
    case 12150:
    case 12160:
    case 12222:
    case 12255: {
	platform_type="satellite";
	obs_type="surface";
	break;
    }
    default: {
	metautils::log_error2("rpt sub-type "+strutils::itos(rpt.data_subtype())+" not recognized for rpt type "+strutils::itos(rpt.data_type())+"  date: "+adata[subset_number]->datetime.to_string()+"  id: '"+adata[subset_number]->rpid+"'",THIS_FUNC,"bufr2xml",USER);
    }
  }
  ientry.key="";
  if (platform_type == "satellite") {
    if (!adata[subset_number]->satid.empty()) {
	ientry.key=metautils::clean_id(adata[subset_number]->satid);
	ientry.key=platform_type+"[!]BUFRsatID[!]"+ientry.key;
    }
    else if (!adata[subset_number]->rpid.empty()) {
	ientry.key=metautils::clean_id(adata[subset_number]->rpid);
	switch (type) {
	  case 12003: {
	    ientry.key=platform_type+"[!]SuomiNet[!]"+ientry.key;
	    break;
	  }
	  default: {
	    ientry.key=platform_type+"[!]other[!]"+ientry.key;
	  }
	}
    }
    else if (!adata[subset_number]->stsn.empty()) {
	ientry.key=metautils::clean_id(adata[subset_number]->stsn);
	switch (type) {
	  case 12004: {
	    ientry.key=platform_type+"[!]EUMETNET[!]"+ientry.key;
	    break;
	  }
	  default: {
	    ientry.key=platform_type+"[!]other[!]"+ientry.key;
	  }
	}
    }
  }
  else if (!adata[subset_number]->wmoid.empty()) {
    ientry.key=metautils::clean_id(adata[subset_number]->wmoid);
    ientry.key=platform_type+"[!]WMO[!]"+ientry.key;
  }
  else if (!adata[subset_number]->acrn.empty()) {
    ientry.key=metautils::clean_id(adata[subset_number]->acrn);
    ientry.key=platform_type+"[!]callSign[!]"+ientry.key;
  }
  else if (!adata[subset_number]->acftid.empty()) {
    ientry.key=metautils::clean_id(adata[subset_number]->acftid);
    ientry.key=platform_type+"[!]other[!]"+ientry.key;
  }
  else if (!adata[subset_number]->rpid.empty()) {
    ientry.key=metautils::clean_id(adata[subset_number]->rpid);
    switch (type) {
	case 1002:
	case 1003:
	case 1102:
	case 1103: {
	  ientry.key=platform_type+"[!]WMO[!]"+ientry.key;
	  break;
	}
	case 2:
	case 7:
	case 102:
	case 1001:
	case 1004:
	case 1005:
	case 1007:
	case 1013:
	case 2003:
	case 2004:
	case 2103:
	case 4001:
	case 4002:
	case 4003:
	case 4004:
	case 4005:
	case 4006:
	case 4007:
	case 4008:
	case 4009:
	case 4010:
	case 4011: {
	  ientry.key=platform_type+"[!]callSign[!]"+ientry.key;
	  break;
	}
	case 2002: {
	  if ((adata[subset_number]->rpid == "PSPIU" || adata[subset_number]->rpid == "PSGAL")) {
	    platform_type="wind_profiler";
	    ientry.key=platform_type+"[!]callSign[!]"+ientry.key;
	  }
	  else {
	    ientry.key=platform_type+"[!]other[!]"+ientry.key;
	  }
	  break;
	}
	case 1006: {
	  ientry.key=platform_type+"[!]other[!]"+ientry.key;
	  break;
	}
	case 2008: {
	  if (ientry.key.length() == 7 && strutils::is_numeric(ientry.key.substr(0,4)) && strutils::is_alpha(ientry.key.substr(4))) {
	    ientry.key=platform_type+"[!]NEXRAD[!]"+ientry.key.substr(4);
	  }
	  else {
	    ientry.key=platform_type+"[!]other[!]"+ientry.key;
	  }
	  break;
	}
	default: {
	  metautils::log_error2(std::string("can't get report ID from RPID: "+adata[subset_number]->rpid+"  date: ")+adata[subset_number]->datetime.to_string()+"  data type: "+strutils::itos(rpt.data_type())+"-"+strutils::itos(rpt.data_subtype()),THIS_FUNC,"bufr2xml",USER);
	}
    }
  }
  if (ientry.key.empty()) {
    metautils::log_error2("can't get report ID from anywhere  date: "+adata[subset_number]->datetime.to_string()+"  data type: "+strutils::itos(rpt.data_type())+"-"+strutils::itos(rpt.data_subtype())+"  platform: "+platform_type,THIS_FUNC,"bufr2xml",USER);
  }
  if (is_valid_date(adata[subset_number]->datetime) && adata[subset_number]->lat <= 90. && adata[subset_number]->lat >= -90. && adata[subset_number]->lon <= 180. && adata[subset_number]->lon >= -180.) {
    if (!obs_data.added_to_platforms(obs_type,platform_type,adata[subset_number]->lat,adata[subset_number]->lon)) {
	auto error=std::move(myerror);
	metautils::log_error2("'"+error+"' while adding platform "+obs_type+"-"+platform_type,THIS_FUNC,"bufr2xml",USER);
    }
    if (!obs_data.added_to_ids(obs_type,ientry,strutils::ftos(rpt.data_type(),3,0,'0')+"."+strutils::ftos(rpt.data_subtype(),3,0,'0'),strutils::itos(rpt.center())+"-"+strutils::itos(rpt.sub_center()),adata[subset_number]->lat,adata[subset_number]->lon,-1.,&adata[subset_number]->datetime)) {
	auto error=std::move(myerror);
	metautils::log_error2("'"+error+"' while adding ID "+ientry.key,THIS_FUNC,"bufr2xml",USER);
    }
  }
}

void process_ncep_radiance_bufr_observation(gatherxml::markup::ObML::ObservationData& obs_data,size_t subset_number)
{
  const std::string THIS_FUNC=__func__+std::string("()");
  auto **rdata=reinterpret_cast<NCEPRadianceBUFRData **>(data);
  std::string obs_type,platform_type;
  auto type=rpt.data_type()*1000+rpt.data_subtype();
  switch (type) {
    case 8010:
    case 8011:
    case 8012: {
	platform_type="satellite";
	obs_type="upper_air";
	break;
    }
    case 21020:
    case 21021:
    case 21022:
    case 21023:
    case 21024:
    case 21025:
    case 21027:
    case 21028:
    case 21033:
    case 21034:
    case 21035:
    case 21036:
    case 21041:
    case 21042:
    case 21043:
    case 21051:
    case 21052:
    case 21053:
    case 21054:
    case 21123:
    case 21201:
    case 21202:
    case 21203:
    case 21204:
    case 21205:
    case 21240:
    case 21241:
    case 21248:
    case 21249:
    case 21250:
    case 21254:
    case 21255: {
	platform_type="satellite";
	obs_type="surface";
	break;
    }
    default: {
      metautils::log_error2("rpt sub-type "+strutils::itos(rpt.data_subtype())+" not recognized for rpt type "+strutils::itos(rpt.data_type())+"  date: "+rpt.date_time().to_string()+"  id: '"+rdata[subset_number]->satid+"'",THIS_FUNC,"bufr2xml",USER);
    }
  }
  ientry.key.clear();
  if (!rdata[subset_number]->satid.empty()) {
    ientry.key=metautils::clean_id(rdata[subset_number]->satid);
    ientry.key=platform_type+"[!]BUFRsatID[!]"+ientry.key;
  }
  if (ientry.key.empty()) {
    metautils::log_error2("can't get report ID from anywhere  date: "+rpt.date_time().to_string()+"  data type: "+strutils::itos(rpt.data_type())+"-"+strutils::itos(rpt.data_subtype()),THIS_FUNC,"bufr2xml",USER);
  }
  for (const auto& group : rdata[subset_number]->radiance_groups) {
    DateTime dt=group.datetime;
    if (is_valid_date(dt) && group.lat <= 90. && group.lat >= -90. && group.lon <= 180. && group.lon >= -180.) {
	if (!obs_data.added_to_platforms(obs_type,platform_type,group.lat,group.lon)) {
	  auto error=std::move(myerror);
	  metautils::log_error2("'"+error+"' while adding platform "+obs_type+"-"+platform_type,THIS_FUNC,"bufr2xml",USER);
	}
	if (!obs_data.added_to_ids(obs_type,ientry,strutils::ftos(rpt.data_type(),3,0,'0')+"."+strutils::ftos(rpt.data_subtype(),3,0,'0'),strutils::itos(rpt.center())+"-"+strutils::itos(rpt.sub_center()),group.lat,group.lon,-1.,&dt)) {
	  auto error=std::move(myerror);
	  metautils::log_error2("'"+error+"' while adding ID "+ientry.key,THIS_FUNC,"bufr2xml",USER);
	}
    }
  }
}

void process_ecmwf_bufr_observation(gatherxml::markup::ObML::ObservationData& obs_data,size_t subset_number)
{
  const std::string THIS_FUNC=__func__+std::string("()");
  auto **edata=reinterpret_cast<ECMWFBUFRData **>(data);
  if (edata[subset_number]->datetime.year() > 3000) {
    return;
  }
  std::string obs_type,platform_type;
  auto type=edata[subset_number]->ecmwf_os.rdb_type*1000+edata[subset_number]->ecmwf_os.rdb_subtype;
  switch (type) {
    case 1001:
    case 1002:
    case 1003:
    case 1004:
    case 1007:
    case 1108:
    case 1116:
    case 1117:
    case 1140: {
	if (edata[subset_number]->wmoid.length() == 0) {
	  metautils::log_error2(std::string("WMO ID not found - date: ")+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="land_station";
	auto block_num=(edata[subset_number]->wmoid).substr(0,2);
	if (block_num >= "01" && block_num <= "98") {
	  ientry.key=platform_type+"[!]WMO[!]"+edata[subset_number]->wmoid;
	}
	else {
	  ientry.key=platform_type+"[!]other[!]"+edata[subset_number]->wmoid;
	}
	obs_type="surface";
	break;
    }
    case 1009:
    case 1011:
    case 1012:
    case 1013:
    case 1014:
    case 1019:
    case 1022:
    case 1023: {
	if (edata[subset_number]->shipid.length() == 0) {
	  metautils::log_error2(std::string("SHIP ID not found - date: ")+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="roving_ship";
	ientry.key=platform_type+"[!]other[!]"+metautils::clean_id(edata[subset_number]->shipid);
	obs_type="surface";
	break;
    }
    case 1021:
    case 1027: {
	if (edata[subset_number]->buoyid.length() == 0) {
	  metautils::log_error2("BUOY ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="drifting_buoy";
	ientry.key=platform_type+"[!]other[!]"+edata[subset_number]->buoyid;
	obs_type="surface";
	break;
    }
    case 2054:
    case 2055:
    case 2206:
    case 3082:
    case 3083:
    case 3084:
    case 3085:
    case 3086:
    case 3087: {
	if (edata[subset_number]->satid.length() == 0) {
	  metautils::log_error2("SATELLITE ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="satellite";
	ientry.key=platform_type+"[!]BUFRsatID[!]"+edata[subset_number]->satid;
	obs_type="upper_air";
	break;
    }
    case 3088:
    case 3089:
    case 12127: {
	if (edata[subset_number]->satid.length() == 0) {
	  metautils::log_error2("SATELLITE ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="satellite";
	ientry.key=platform_type+"[!]BUFRsatID[!]"+edata[subset_number]->satid;
	obs_type="surface";
	break;
    }
    case 4091:
    case 5101: {
	if (edata[subset_number]->wmoid.length() == 0) {
	  metautils::log_error2("WMO ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="land_station";
	auto block_num=(edata[subset_number]->wmoid).substr(0,2);
	if (block_num >= "01" && block_num <= "98") {
	  ientry.key=platform_type+"[!]WMO[!]"+edata[subset_number]->wmoid;
	}
	else {
	  ientry.key=platform_type+"[!]other[!]"+edata[subset_number]->wmoid;
	}
	obs_type="upper_air";
	break;
    }
    case 4092:
    case 5102:
    case 5106: {
	if (edata[subset_number]->shipid.length() == 0) {
	  metautils::log_error2("SHIP ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="roving_ship";
	ientry.key=platform_type+"[!]other[!]"+metautils::clean_id(edata[subset_number]->shipid);
	obs_type="upper_air";
	break;
    }
    case 4095:
    case 4096:
    case 4097: {
	if (edata[subset_number]->wmoid.length() == 0) {
	  metautils::log_error2("WMO ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="wind_profiler";
	ientry.key=platform_type+"[!]WMO[!]"+edata[subset_number]->wmoid;
	obs_type="upper_air";
	break;
    }
    case 5103:
    case 7141:
    case 7142:
    case 7143:
    case 7144:
    case 7145: {
	if (edata[subset_number]->acftid.length() == 0) {
	  metautils::log_error2("ACFT ID not found - date: "+edata[subset_number]->datetime.to_string(),THIS_FUNC,"bufr2xml",USER);
	}
	platform_type="aircraft";
	ientry.key=platform_type+"[!]other[!]"+metautils::clean_id(edata[subset_number]->acftid);
	obs_type="upper_air";
	break;
    }
    case 10164: {
	platform_type="bogus";
	ientry.key=platform_type+"[!]other[!]"+edata[subset_number]->ecmwf_os.ID;
	obs_type="upper_air";
	break;
    }
    default: {
      metautils::log_error2("rdb sub-type "+strutils::itos(edata[subset_number]->ecmwf_os.rdb_subtype)+" not recognized for rdb type "+strutils::itos(edata[subset_number]->ecmwf_os.rdb_type)+"  date: "+edata[subset_number]->datetime.to_string()+"  id: '"+edata[subset_number]->ecmwf_os.ID+"'",THIS_FUNC,"bufr2xml",USER);
    }
  }
  if (!obs_data.added_to_platforms(obs_type,platform_type,edata[subset_number]->lat,edata[subset_number]->lon)) {
    auto error=std::move(myerror);
    metautils::log_error2("'"+error+"' while adding platform "+obs_type+"-"+platform_type,THIS_FUNC,"bufr2xml",USER);
  }
  if (!obs_data.added_to_ids(obs_type,ientry,strutils::ftos(edata[subset_number]->ecmwf_os.rdb_type,3,0,'0')+"."+strutils::ftos(edata[subset_number]->ecmwf_os.rdb_subtype,3,0,'0'),strutils::itos(rpt.center())+"-"+strutils::itos(rpt.sub_center()),edata[subset_number]->lat,edata[subset_number]->lon,-1.,&edata[subset_number]->datetime)) {
    auto error=std::move(myerror);
    metautils::log_error2("'"+error+"' while adding ID "+ientry.key,THIS_FUNC,"bufr2xml",USER);
  }
}

void scan_file(gatherxml::markup::ObML::ObservationData& obs_data)
{
  const std::string THIS_FUNC=__func__+std::string("()");
  tfile.reset(new TempFile);
  if (!tfile->open(metautils::args.temp_loc)) {
    metautils::log_error2("could not open temporary file in '"+metautils::args.temp_loc+"'",THIS_FUNC,"bufr2xml",USER);
  }
  tdir.reset(new TempDir);
  if (!tdir->create(metautils::args.temp_loc)) {
    metautils::log_error2("could not create temporary directory in '"+metautils::args.temp_loc+"'",THIS_FUNC,"bufr2xml",USER);
  }
  std::string file_format,error;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,NULL,file_format,error)) {
    metautils::log_error2(error,"prepare_file_for_metadata_scanning()","bufr2xml",USER);
  }
  const size_t BUF_LEN=800000;
  std::unique_ptr<unsigned char[]> buffer(new unsigned char[BUF_LEN]);
  InputBUFRStream istream;
  if (!istream.open(tfile->name())) {
    metautils::log_error2("unable to open file for input",THIS_FUNC,"bufr2xml",USER);
  }
  while (istream.read(buffer.get(),BUF_LEN) > 0) {
    rpt.fill(istream,buffer.get(),BUFRReport::header_only,metautils::directives.rdadata_home+"/share/BUFR");
    if (rpt.data_type() != 11) {
	std::string message_type;
	if (metautils::args.data_format == "prepbufr") {
	  message_type=istream.data_category_description(rpt.data_type());
	}
	data=rpt.data();
	for (int n=0; n < rpt.number_of_subsets(); ++n) {
	  if (metautils::args.data_format == "prepbufr") {
	    process_ncep_prepbufr_observation(obs_data,message_type,n);
	  }
	  else if (metautils::args.data_format == "adpbufr") {
	    process_ncep_adp_bufr_observation(obs_data,n);
	  }
	  else if (metautils::args.data_format == "radbufr") {
	    process_ncep_radiance_bufr_observation(obs_data,n);
	  }
	  else if (metautils::args.data_format == "ecmwfbufr") {
	    process_ecmwf_bufr_observation(obs_data,n);
	  }
	}
    }
  }
  istream.close();
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error2("core dump","segv_handler()","bufr2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  std::stringstream oss,ess;
  std::string flags;

  if (argc < 6) {
    std::cerr << "usage: bufr2xml -f format -d [ds]nnn.n [options...] https://rda.ucar.edu/..." << std::endl;
    std::cerr << "\nrequired (choose one):" << std::endl;
    std::cerr << "-f ecmwfbufr     ECMWF BUFR" << std::endl;
    std::cerr << "-f adpbufr       NCEP ADP BUFR" << std::endl;
    std::cerr << "-f prepbufr      NCEP PREPBUFR" << std::endl;
    std::cerr << "-f radbufr       NCEP Radiance BUFR" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    if (USER == "dattore") {
      std::cerr << "-g/-G            do/don't generate graphics (default is -g)" << std::endl;
      std::cerr << "-u/-U            do/don't update the database (default is -u)" << std::endl;
    }
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  auto arg_delimiter='%';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("bufr2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  if (metautils::args.data_format == "prepbufr") {
    rpt.set_data_handler(handle_ncep_prepbufr);
  }
  else if (metautils::args.data_format == "adpbufr") {
    rpt.set_data_handler(handle_ncep_adpbufr);
  }
  else if (metautils::args.data_format == "radbufr") {
    rpt.set_data_handler(handle_ncep_radiance_bufr);
  }
  else if (metautils::args.data_format == "ecmwfbufr") {
    rpt.set_data_handler(handle_ecmwf_bufr);
  }
  else {
    metautils::log_error2("format "+metautils::args.data_format+" not recognized","main()","bufr2xml",USER);
  }
  flags="-f";
  if (std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("bufr2xml",USER);
  metautils::check_for_existing_cmd("ObML");
  gatherxml::markup::ObML::ObservationData obs_data;
  scan_file(obs_data);
  if (!obs_data.is_empty) {
    gatherxml::markup::ObML::write(obs_data,"bufr2xml",USER);
  }
  else {
    metautils::log_error2("Terminating - no data found - no content metadata will be generated","main()","bufr2xml",USER);
    exit(1);
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
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" "+flags+" "+metautils::args.filename+".ObML",oss,ess) < 0) {
	metautils::log_error2(ess.str(),"main() running scm","bufr2xml",USER);
    }
  }
}
