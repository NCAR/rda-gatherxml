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

using metautils::clean_id;
using metautils::log_error2;
using miscutils::this_function_label;
using std::cout;
using std::endl;
using std::move;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::unique_ptr;
using strutils::ftos;
using strutils::itos;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

extern "C" void clean_up() {
  if (!myerror.empty()) {
    log_error2(myerror, "clean_up()", "bufr2xml", USER);
  }
}

extern "C" void segv_handler(int) {
  clean_up();
  metautils::cmd_unregister();
  log_error2("core dump", "segv_handler()", "bufr2xml", USER);
}

extern "C" void int_handler(int) {
  clean_up();
  metautils::cmd_unregister();
}

gatherxml::markup::ObML::IDEntry g_ientry;
unique_ptr<TempFile> g_tfile;
unique_ptr<TempDir> g_tdir;

bool is_valid_date(DateTime& datetime) {
  if (datetime.year() < 1900 || datetime.year() > dateutils::
      current_date_time().year() + 20) {
    return false;
  }
  if (datetime.month() < 1 || datetime.month() > 12) {
    return false;
  }
  if (datetime.day() < 1 || datetime.day() > static_cast<short>(dateutils::
      days_in_month(datetime.year(), datetime.month()))) {
    return false;
  }
  return true;
}

void process_ncep_prepbufr_observation(BUFRReport& bufr_report, gatherxml::
    markup::ObML::ObservationData& obs_data, string& message_type, size_t
    subset_number) {
  static const string F = this_function_label(__func__);
  auto **data = reinterpret_cast<NCEPPREPBUFRData **>(bufr_report.data());
  auto dhr = data[subset_number]->dhr;
  if (dhr == -999) {
    return;
  }
  auto dt = bufr_report.date_time();
  if (dhr >= 0) {
    dt.add_hours(dhr);
  } else {
    dt.subtract_hours(-dhr);
  }
  string otyp, ptyp;
  switch (data[subset_number]->type.prepbufr) {
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
      otyp = "upper_air";
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
      otyp = "surface";
      break;
    }
    case 210: {
      otyp = "surface";
      ptyp = "bogus";
      break;
    }
    default: {
      log_error2("PREPBUFR type " + itos(data[subset_number]->type.prepbufr) +
          " not recognized (ON29 code = " + itos(data[subset_number]->type.ON29)
          + ")  date: " + dt.to_string() + "  id: '" + data[subset_number]->
          stnid + "'", F, "bufr2xml", USER);
    }
  }
  if (ptyp.empty()) {
    switch (data[subset_number]->type.ON29) {
      case 11:
      case 12:
      case 13:
      case 511:
      case 512:
      case 513:
      case 514:
      case 540: {
        ptyp = "land_station";
        break;
      }
      case 21:
      case 521: {
        ptyp = "fixed_ship";
        break;
      }
      case 22:
      case 23:
      case 522:
      case 523:
      case 524:
      case 525: {
        ptyp = "roving_ship";
        break;
      }
      case 31:
      case 41: {
        ptyp = "aircraft";
        break;
      }
      case 532:
      case 533:
      case 534: {
        ptyp = "automated_gauge";
        break;
      }
      case 51:
      case 551: {
        ptyp = "bogus";
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
        ptyp = "satellite";
        break;
      }
      case 71:
      case 72:
      case 73:
      case 75:
      case 76:
      case 77: {
        ptyp = "wind_profiler";
        break;
      }
      case 530:
      case 531: {
        ptyp = "CMAN_station";
        break;
      }
      case 561: {
        ptyp = "moored_buoy";
        break;
      }
      case 562:
      case 563:
      case 564: {
        ptyp = "drifting_buoy";
        break;
      }
      default: {
        log_error2("unknown platform type " + itos(data[subset_number]->type.
            ON29), F, "bufr2xml", USER);
      }
    }
  }
  string id;
  if (!data[subset_number]->stnid.empty()) {
    id = data[subset_number]->stnid;
  } else if (!data[subset_number]->satid.empty()) {
    id = data[subset_number]->satid;
  } else if (!data[subset_number]->acftid.empty()) {
    id = data[subset_number]->acftid;
  }
  else {
    log_error2("unable to get an ID for '" + message_type + ", date " + dt.
        to_string(), F, "bufr2xml", USER);
  }
  g_ientry.key = prepbufr_id_key(clean_id(id), ptyp, message_type);
  if (g_ientry.key.empty()) {
    log_error2("unable to get ID key for '" + message_type + "', ID: '" + id +
        "' (" + data[subset_number]->satid + ") (" + data[subset_number]->acftid
        + ") (" + data[subset_number]->stnid + ")", F, "bufr2xml", USER);
  }
  auto lon = data[subset_number]->elon;
  if (lon > 180.) {
    lon -= 360.;
  }
  for (const auto& e : data[subset_number]->cat_groups) {
    if (e.code > 63) {
      log_error2("bad category '" + itos(e.code) + "', " + ptyp + ", date " +
          dt.to_string() + ", id '" + data[subset_number]->stnid + "'", F,
          "bufr2xml", USER);
    }
    if (is_valid_date(dt)) {
      if (!obs_data.added_to_platforms(otyp, ptyp, data[subset_number]->lat,
          lon)) {
        auto e = move(myerror);
        log_error2("'" + e + "' while adding platform " + otyp + "-" + ptyp, F,
            "bufr2xml", USER);
      }
      string s;
      if (bufr_report.center() == 99 && bufr_report.sub_center() == 0) {

        // patch for NNRP prepqm files
        s = "7-3";
      } else {
        s = itos(bufr_report.center()) + "-" + itos(bufr_report.sub_center());
      }
      if (!obs_data.added_to_ids(otyp, g_ientry, itos(e.code), s, data[
          subset_number]->lat, lon, -1., &dt)) {
        auto e = move(myerror);
        log_error2("'" + e + "' while adding ID " + g_ientry.key, F, "bufr2xml",
            USER);
      }
    }
  }
}

void process_ncep_adp_bufr_observation(BUFRReport& bufr_report, gatherxml::
    markup::ObML::ObservationData& obs_data, size_t subset_number) {
  static const string F = this_function_label(__func__);
  auto **data = reinterpret_cast<NCEPADPBUFRData **>(bufr_report.data());
  if (data[subset_number]->datetime.year() == 31073) {
    data[subset_number]->datetime = bufr_report.date_time();
  }
  string otyp, ptyp;
  auto x = bufr_report.data_type() * 1000 + bufr_report.data_subtype();
  switch (x) {
    case 0:
    case 1:
    case 2:
    case 7:
    case 100:
    case 101:
    case 102: {
      otyp = "surface";
      ptyp = "land_station";
      break;
    }
    case 1001:
    case 1013:
    case 1101:
    case 1113: {
      otyp = "surface";
      ptyp = "roving_ship";
      break;
    }
    case 1002:
    case 1102: {
      otyp = "surface";
      ptyp = "drifting_buoy";
      break;
    }
    case 1003:
    case 1103: {
      otyp = "surface";
      ptyp = "moored_buoy";
      break;
    }
    case 1004:
    case 1104: {
      otyp = "surface";
      ptyp = "CMAN_station";
      break;
    }
    case 1005: {
      otyp = "surface";
      ptyp = "automated_gauge";
      break;
    }
    case 1006: {
      otyp = "surface";
      ptyp = "bogus";
      break;
    }
    case 1007: {
      otyp = "surface";
      ptyp = "coastal_station";
      break;
    }
    case 2001:
    case 2002:
    case 2101:
    case 2102: {
      ptyp = "land_station";
      otyp = "upper_air";
      break;
    }
    case 2003:
    case 2103: {
      ptyp = "roving_ship";
      otyp = "upper_air";
      break;
    }
    case 2004:
    case 2104: {
      ptyp = "aircraft";
      otyp = "upper_air";
      break;
    }
    case 2005:
    case 2009:
    case 2105: {
      ptyp = "land_station";
      otyp = "upper_air";
      break;
    }
    case 2007:
    case 2010:
    case 2011:
    case 2013:
    case 2014:
    case 2016:
    case 2018: {
      ptyp = "wind_profiler";
      otyp = "upper_air";
      break;
    }
    case 2008:
    case 2017: {
      ptyp = "NEXRAD";
      otyp = "upper_air";
      break;
    }
    case 3001:
    case 3002:
    case 3003:
    case 3010:
    case 3101:
    case 3102:
    case 3104: {
      ptyp = "satellite";
      otyp = "upper_air";
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
      ptyp = "aircraft";
      otyp = "upper_air";
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
    case 5072:
    case 5080:
    case 5090:
    case 5091:
    case 8012:
    case 8013:
    case 8015: {
      ptyp = "satellite";
      otyp = "upper_air";
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
      ptyp = "satellite";
      otyp = "surface";
      break;
    }
    default: {
      log_error2("rpt sub-type " + itos(bufr_report.data_subtype()) + " not "
          "recognized for rpt type " + itos(bufr_report.data_type()) +
          "  date: " + data[subset_number]->datetime.to_string() + "  id: '" +
          data[subset_number]->rpid + "'", F, "bufr2xml", USER);
    }
  }
  g_ientry.key = "";
  if (ptyp == "satellite") {
    if (!data[subset_number]->satid.empty()) {
      g_ientry.key = clean_id(data[subset_number]->satid);
      g_ientry.key = ptyp + "[!]BUFRsatID[!]" + g_ientry.key;
    } else if (!data[subset_number]->rpid.empty()) {
      g_ientry.key = clean_id(data[subset_number]->rpid);
      switch (x) {
        case 12003: {
          g_ientry.key = ptyp + "[!]SuomiNet[!]" + g_ientry.key;
          break;
        }
        default: {
          g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
        }
      }
    } else if (!data[subset_number]->stsn.empty()) {
      g_ientry.key = clean_id(data[subset_number]->stsn);
      switch (x) {
        case 12004: {
          g_ientry.key = ptyp + "[!]EUMETNET[!]" + g_ientry.key;
          break;
        }
        default: {
          g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
        }
      }
    }
  } else if (!data[subset_number]->wmoid.empty()) {
    g_ientry.key = clean_id(data[subset_number]->wmoid);
    g_ientry.key = ptyp + "[!]WMO[!]" + g_ientry.key;
  } else if (!data[subset_number]->acrn.empty()) {
    g_ientry.key = clean_id(data[subset_number]->acrn);
    g_ientry.key = ptyp + "[!]callSign[!]" + g_ientry.key;
  } else if (!data[subset_number]->acftid.empty()) {
    g_ientry.key = clean_id(data[subset_number]->acftid);
    g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
  } else if (!data[subset_number]->rpid.empty()) {
    g_ientry.key = clean_id(data[subset_number]->rpid);
    switch (x) {
      case 1002:
      case 1003:
      case 1102:
      case 1103: {
        g_ientry.key = ptyp + "[!]WMO[!]" + g_ientry.key;
        break;
      }
      case 2:
      case 7:
      case 102:
      case 1001:
      case 1101:
      case 1004:
      case 1005:
      case 1007:
      case 1013:
      case 1113:
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
        g_ientry.key = ptyp + "[!]callSign[!]" + g_ientry.key;
        break;
      }
      case 2002: {
        if ((data[subset_number]->rpid == "PSPIU" || data[subset_number]->rpid
            == "PSGAL")) {
          ptyp = "wind_profiler";
          g_ientry.key = ptyp + "[!]callSign[!]" + g_ientry.key;
        } else {
          g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
        }
        break;
      }
      case 1006: {
        g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
        break;
      }
      case 2008: {
        if (g_ientry.key.length() == 7 && strutils::is_numeric(g_ientry.key.
            substr(0, 4)) && strutils::is_alpha(g_ientry.key.substr(4))) {
          g_ientry.key = ptyp + "[!]NEXRAD[!]" + g_ientry.key.substr(4);
        } else {
          g_ientry.key = ptyp + "[!]other[!]" + g_ientry.key;
        }
        break;
      }
      default: {
        log_error2(string("can't get report ID from RPID: " + data[
            subset_number]->rpid + "  date: ") + data[subset_number]->datetime.
            to_string() + "  data type: " + itos(bufr_report.data_type()) + "-"
            + itos(bufr_report.data_subtype()), F, "bufr2xml", USER);
      }
    }
  }
  if (g_ientry.key.empty()) {
    log_error2("can't get report ID from anywhere  date: " + data[
        subset_number]->datetime.to_string() + "  data type: " + itos(
        bufr_report.data_type()) + "-" + itos(bufr_report.data_subtype()) +
        "  platform: " + ptyp, F, "bufr2xml", USER);
  }
  if (is_valid_date(data[subset_number]->datetime) && data[subset_number]->lat
      <= 90. && data[subset_number]->lat >= -90. && data[subset_number]->lon <=
      180. && data[subset_number]->lon >= -180.) {
    if (!obs_data.added_to_platforms(otyp, ptyp, data[subset_number]->lat, data[
        subset_number]->lon)) {
      auto e = move(myerror);
      log_error2("'" + e + "' while adding platform " + otyp + "-" + ptyp, F,
          "bufr2xml", USER);
    }
    if (!obs_data.added_to_ids(otyp, g_ientry, ftos(bufr_report.data_type(), 3,
        0, '0') + "." + ftos(bufr_report.data_subtype(), 3, 0, '0'), itos(
        bufr_report.center()) + "-" + itos(bufr_report.sub_center()), data[
        subset_number]->lat, data[subset_number]->lon, -1., &data[
        subset_number]->datetime)) {
      auto e = move(myerror);
      log_error2("'" + e + "' while adding ID " + g_ientry.key, F, "bufr2xml",
          USER);
    }
  }
}

void process_ncep_radiance_bufr_observation(BUFRReport& bufr_report, gatherxml::
    markup::ObML::ObservationData& obs_data, size_t subset_number) {
  static const string F = this_function_label(__func__);
  auto **data = reinterpret_cast<NCEPRadianceBUFRData **>(bufr_report.data());
  string otyp, ptyp;
  auto x = bufr_report.data_type() * 1000 + bufr_report.data_subtype();
  switch (x) {
    case 8010:
    case 8011:
    case 8012: {
      ptyp = "satellite";
      otyp = "upper_air";
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
      ptyp = "satellite";
      otyp = "surface";
      break;
    }
    default: {
      log_error2("rpt sub-type " + itos(bufr_report.data_subtype()) + " not "
          "recognized for rpt type " + itos(bufr_report.data_type()) +
          "  date: " + bufr_report.date_time().to_string() + "  id: '" + data[
          subset_number]->satid + "'", F, "bufr2xml", USER);
    }
  }
  g_ientry.key.clear();
  if (!data[subset_number]->satid.empty()) {
    g_ientry.key = clean_id(data[subset_number]->satid);
    g_ientry.key = ptyp + "[!]BUFRsatID[!]" + g_ientry.key;
  }
  if (g_ientry.key.empty()) {
    log_error2("can't get report ID from anywhere  date: " + bufr_report.
        date_time().to_string() + "  data type: " + itos(bufr_report.
        data_type()) + "-" + itos(bufr_report.data_subtype()), F, "bufr2xml",
        USER);
  }
  for (const auto& e : data[subset_number]->radiance_groups) {
    auto dt = e.datetime;
    if (is_valid_date(dt) && e.lat <= 90. && e.lat >= -90. && e.lon <= 180. &&
        e.lon >= -180.) {
      if (!obs_data.added_to_platforms(otyp, ptyp, e.lat, e.lon)) {
        auto e = move(myerror);
        log_error2("'" + e + "' while adding platform " + otyp + "-" + ptyp, F,
            "bufr2xml", USER);
      }
      if (!obs_data.added_to_ids(otyp, g_ientry, ftos(bufr_report.data_type(),
          3, 0, '0') + "." + ftos(bufr_report.data_subtype(), 3, 0, '0'), itos(
          bufr_report.center()) + "-" + itos(bufr_report.sub_center()), e.lat,
          e.lon, -1., &dt)) {
        auto e = move(myerror);
        log_error2("'" + e + "' while adding ID " + g_ientry.key, F, "bufr2xml",
            USER);
      }
    }
  }
}

void process_ecmwf_bufr_observation(BUFRReport& bufr_report, gatherxml::markup::
    ObML::ObservationData& obs_data, size_t subset_number) {
  static const string F = this_function_label(__func__);
  auto **data = reinterpret_cast<ECMWFBUFRData **>(bufr_report.data());
  if (data[subset_number]->datetime.year() > 3000) {
    return;
  }
  string otyp, ptyp;
  auto x = data[subset_number]->ecmwf_os.rdb_type * 1000 + data[subset_number]->
      ecmwf_os.rdb_subtype;
  switch (x) {
    case 1001:
    case 1002:
    case 1003:
    case 1004:
    case 1007:
    case 1108:
    case 1116:
    case 1117:
    case 1140: {
      if (data[subset_number]->wmoid.empty()) {
        log_error2("WMO ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp = "land_station";
      auto s = data[subset_number]->wmoid.substr(0, 2);
      if (s >= "01" && s <= "98") {
        g_ientry.key = ptyp + "[!]WMO[!]" + data[subset_number]->wmoid;
      } else {
        g_ientry.key = ptyp + "[!]other[!]" + data[subset_number]->wmoid;
      }
      otyp = "surface";
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
      if (data[subset_number]->shipid.empty()) {
        log_error2(string("SHIP ID not found - date: ") + data[subset_number]->
            datetime.to_string(), F, "bufr2xml", USER);
      }
      ptyp = "roving_ship";
      g_ientry.key = ptyp + "[!]other[!]" + clean_id(data[subset_number]->shipid);
      otyp = "surface";
      break;
    }
    case 1021:
    case 1027: {
      if (data[subset_number]->buoyid.empty()) {
        log_error2("BUOY ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp = "drifting_buoy";
      g_ientry.key = ptyp + "[!]other[!]" + data[subset_number]->buoyid;
      otyp = "surface";
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
      if (data[subset_number]->satid.empty()) {
        log_error2("SATELLITE ID not found - date: " + data[subset_number]->
            datetime.to_string(), F, "bufr2xml", USER);
      }
      ptyp = "satellite";
      g_ientry.key = ptyp + "[!]BUFRsatID[!]" + data[subset_number]->satid;
      otyp = "upper_air";
      break;
    }
    case 3088:
    case 3089:
    case 12127: {
      if (data[subset_number]->satid.empty()) {
        log_error2("SATELLITE ID not found - date: " + data[subset_number]->
            datetime.to_string(), F, "bufr2xml", USER);
      }
      ptyp = "satellite";
      g_ientry.key = ptyp + "[!]BUFRsatID[!]" + data[subset_number]->satid;
      otyp =  "surface";
      break;
    }
    case 4091:
    case 5101: {
      if (data[subset_number]->wmoid.empty()) {
        log_error2("WMO ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp =  "land_station";
      auto s = data[subset_number]->wmoid.substr(0, 2);
      if (s >= "01" && s <= "98") {
        g_ientry.key = ptyp + "[!]WMO[!]" + data[subset_number]->wmoid;
      } else {
        g_ientry.key = ptyp + "[!]other[!]" + data[subset_number]->wmoid;
      }
      otyp = "upper_air";
      break;
    }
    case 4092:
    case 5102:
    case 5106: {
      if (data[subset_number]->shipid.empty()) {
        log_error2("SHIP ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp = "roving_ship";
      g_ientry.key = ptyp + "[!]other[!]" + clean_id(data[subset_number]->shipid);
      otyp = "upper_air";
      break;
    }
    case 4095:
    case 4096:
    case 4097: {
      if (data[subset_number]->wmoid.empty()) {
        log_error2("WMO ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp = "wind_profiler";
      g_ientry.key = ptyp + "[!]WMO[!]" + data[subset_number]->wmoid;
      otyp = "upper_air";
      break;
    }
    case 5103:
    case 7141:
    case 7142:
    case 7143:
    case 7144:
    case 7145: {
      if (data[subset_number]->acftid.empty()) {
        log_error2("ACFT ID not found - date: " + data[subset_number]->datetime.
            to_string(), F, "bufr2xml", USER);
      }
      ptyp = "aircraft";
      g_ientry.key = ptyp +  "[!]other[!]" +  clean_id(data[subset_number]->
          acftid);
      otyp = "upper_air";
      break;
    }
    case 10164: {
      ptyp = "bogus";
      g_ientry.key = ptyp + "[!]other[!]" + data[subset_number]->ecmwf_os.ID;
      otyp = "upper_air";
      break;
    }
    default: {
      log_error2("rdb sub-type " + itos(data[subset_number]->ecmwf_os.
          rdb_subtype) + " not recognized for rdb type " + itos(data[
          subset_number]->ecmwf_os.rdb_type) + "  date: " + data[
          subset_number]->datetime.to_string() + "  id: '" + data[
          subset_number]->ecmwf_os.ID + "'", F, "bufr2xml", USER);
    }
  }
  if (!obs_data.added_to_platforms(otyp, ptyp, data[subset_number]->lat, data[
      subset_number]->lon)) {
    auto e = move(myerror);
    log_error2("'" + e + "' while adding platform " + otyp + "-" + ptyp, F,
        "bufr2xml", USER);
  }
  if (!obs_data.added_to_ids(otyp, g_ientry, ftos(data[subset_number]->ecmwf_os.
      rdb_type, 3, 0, '0') + "." + ftos(data[subset_number]->ecmwf_os.
      rdb_subtype, 3, 0, '0'), itos(bufr_report.center()) + "-" + itos(
      bufr_report.sub_center()), data[subset_number]->lat, data[subset_number]->
      lon, -1., &data[subset_number]->datetime)) {
    auto e = move(myerror);
    log_error2("'" + e + "' while adding ID " + g_ientry.key, F, "bufr2xml",
        USER);
  }
}

void scan_file(BUFRReport& bufr_report, gatherxml::markup::ObML::
    ObservationData& obs_data) {
  static const string F = this_function_label(__func__);
  g_tfile.reset(new TempFile);
  if (!g_tfile->open(metautils::args.temp_loc)) {
    log_error2("could not open temporary file in '" + metautils::args.temp_loc +
        "'", F, "bufr2xml", USER);
  }
  g_tdir.reset(new TempDir);
  if (!g_tdir->create(metautils::args.temp_loc)) {
    log_error2("could not create temporary directory in '" + metautils::args.
        temp_loc + "'", F, "bufr2xml", USER);
  }
  string f, e;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*g_tfile,
      *g_tdir, NULL, f, e)) {
    log_error2(e, F + ": prepare_file_for_metadata_scanning()", "bufr2xml",
        USER);
  }
  const size_t LEN = 800000;
  unique_ptr<unsigned char[]> p(new unsigned char[LEN]);
  InputBUFRStream istream;
  if (!istream.open(g_tfile->name())) {
    log_error2("unable to open file for input", F, "bufr2xml", USER);
  }
  while (istream.read(p.get(), LEN) > 0) {
    bufr_report.fill(istream, p.get(), BUFRReport::header_only, metautils::
        directives.rdadata_home  + "/share/BUFR");
    if (bufr_report.data_type() != 11) {
      string mtyp;
      if (metautils::args.data_format == "prepbufr") {
        mtyp = istream.data_category_description(bufr_report.data_type());
      }
      for (int n = 0; n < bufr_report.number_of_subsets(); ++n) {
        if (metautils::args.data_format == "prepbufr") {
          process_ncep_prepbufr_observation(bufr_report, obs_data, mtyp, n);
        } else if (metautils::args.data_format == "adpbufr") {
          process_ncep_adp_bufr_observation(bufr_report, obs_data, n);
        } else if (metautils::args.data_format == "radbufr") {
          process_ncep_radiance_bufr_observation(bufr_report, obs_data, n);
        } else if (metautils::args.data_format == "ecmwfbufr") {
          process_ecmwf_bufr_observation(bufr_report, obs_data, n);
        }
      }
    }
  }
  istream.close();
}

void show_usage() {
  cout << "usage: bufr2xml -f format -d [ds]nnn.n [options...] https://"
      "rda.ucar.edu/..." << endl;
  cout << "\nrequired (choose one):" << endl;
  cout << "-f ecmwfbufr     ECMWF BUFR" << endl;
  cout << "-f adpbufr       NCEP ADP BUFR" << endl;
  cout << "-f prepbufr      NCEP PREPBUFR" << endl;
  cout << "-f radbufr       NCEP Radiance BUFR" << endl;
  cout << "\nrequired:" << endl;
  cout << "-d <nnn.n>       nnn.n is the dataset number to which the data file "
      "belongs" << endl;
  cout << "\noptions:" << endl;
  if (USER == "dattore") {
    cout << "-g/-G            do/don't generate graphics (default is -g)" <<
        endl;
    cout << "-u/-U            do/don't update the database (default is -u)" <<
        endl;
  }
}

int main(int argc, char **argv) {
  if (argc < 6) {
    show_usage();
    exit(0);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  auto d = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, d);
  metautils::read_config("bufr2xml", USER);
  gatherxml::parse_args(d);
  BUFRReport b;
  if (metautils::args.data_format == "prepbufr") {
    b.set_data_handler(handle_ncep_prepbufr);
  } else if (metautils::args.data_format == "adpbufr") {
    b.set_data_handler(handle_ncep_adpbufr);
  } else if (metautils::args.data_format == "radbufr") {
    b.set_data_handler(handle_ncep_radiance_bufr);
  } else if (metautils::args.data_format == "ecmwfbufr") {
    b.set_data_handler(handle_ecmwf_bufr);
  } else {
    log_error2("format " + metautils::args.data_format + " not recognized",
        "main()", "bufr2xml", USER);
  }
  string cflg = "-f";
  if (regex_search(metautils::args.path, regex("^https://rda.ucar.edu"))) {
    cflg = "-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("bufr2xml", USER);
  metautils::check_for_existing_cmd("ObML");
  gatherxml::markup::ObML::ObservationData o;
  scan_file(b, o);
  if (!o.is_empty) {
    gatherxml::markup::ObML::write(o, "bufr2xml", USER);
  } else {
    log_error2("Terminating - no data found - no content metadata will be "
        "generated", "main()", "bufr2xml", USER);
  }
  if (metautils::args.update_db) {
    if (!metautils::args.update_graphics) {
      cflg += " -G";
    }
    if (!metautils::args.regenerate) {
      cflg += " -R";
    }
    if (!metautils::args.update_summary) {
      cflg += " -S";
    }
    stringstream oss, ess;
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " " + cflg + " " + metautils::args.filename +
        ".ObML", oss, ess) < 0) {
      log_error2(ess.str(), "main(): running scm", "bufr2xml", USER);
    }
  }
}
