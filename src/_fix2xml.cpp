#include <iostream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <list>
#include <sys/stat.h>
#include <signal.h>
#include <gatherxml.hpp>
#include <pglocks.hpp>
#include <cyclone.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>
#include <metadata.hpp>
#include <datetime.hpp>
#include <myerror.hpp>

namespace FixML = gatherxml::markup::FixML;
using metautils::log_error2;
using std::cerr;
using std::endl;
using std::string;
using std::stringstream;
using std::unique_ptr;
using strutils::is_numeric;
using strutils::itos;
using strutils::replace_all;
using strutils::trim;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

my::map<FixML::FeatureEntry> feature_table;
unique_ptr<TempFile> tfile;
unique_ptr<TempDir> tdir;
my::map<FixML::StageEntry> stage_table;
FixML::StageEntry sentry;

extern "C" void clean_up() {
}

string this_function_label(string function_name) {
  return string(function_name + "()");
}

void process_HURDAT(unique_ptr<Cyclone>& c) {
  HURDATCyclone *hc = reinterpret_cast<HURDATCyclone *>(c.get());
  float elon;
  float min_elon = 99999.;
  float max_elon = -99999.;
  auto fix_data = hc->fix_data();
  FixML::FeatureEntry fe;
  if (hc->latitude_hemisphere() == 'S') {
    if (fix_data[0].datetime.month() >= 7) {
      fe.key = itos(fix_data[0].datetime.year() + 1) + "-" + hc->ID();
    } else {
      fe.key = itos(fix_data[0].datetime.year()) + "-" + hc->ID();
    }
  } else {
    fe.key = itos(fix_data[0].datetime.year()) + "-" + hc->ID();
  }
  fe.data.reset(new FixML::FeatureEntry::Data);
  FixML::ClassificationEntry ce;
  ce.key = "";
  ce.pres_units = "mbar";
  ce.wind_units = "kt";
  for (const auto& fix : fix_data) {
    if (fix.classification != ce.key) {
      if (!ce.key.empty()) {
        fe.data->classification_list.emplace_back(ce);
      }
      ce.key = fix.classification;
      ce.start_datetime = fix.datetime;
      ce.start_lat = fix.latitude;
      ce.start_lon = fix.longitude;
      ce.min_lat = ce.max_lat = fix.latitude;
      ce.min_lon = ce.max_lon = elon = fix.longitude;
      if (elon < 0.) {
        elon += 360.;
      }
      min_elon = max_elon = elon;
      ce.min_pres = ce.max_pres = fix.min_pres;
      ce.min_speed = ce.max_speed = fix.max_wind;
      ce.nfixes = 0;
    }
    ce.end_datetime = fix.datetime;
    ce.end_lat = fix.latitude;
    ce.end_lon = fix.longitude;
    elon = fix.longitude;
    if (elon < 0.) {
      elon += 360.;
    }
    if (fix.latitude < ce.min_lat) {
      ce.min_lat = fix.latitude;
    }
    if (fix.latitude > ce.max_lat) {
      ce.max_lat = fix.latitude;
    }
    if (fix.longitude < ce.min_lon) {
      ce.min_lon = fix.longitude;
    }
    if (fix.longitude > ce.max_lon) {
      ce.max_lon = fix.longitude;
    }
    if (elon < min_elon) {
      min_elon = elon;
    }
    if (elon > max_elon) {
      max_elon = elon;
    }
    if (fix.min_pres < ce.min_pres) {
      ce.min_pres = fix.min_pres;
    }
    if (fix.min_pres > ce.max_pres) {
      ce.max_pres = fix.min_pres;
    }
    if (fix.max_wind < ce.min_speed) {
      ce.min_speed = fix.max_wind;
    }
    if (fix.max_wind > ce.max_speed) {
      ce.max_speed = fix.max_wind;
    }
    ++ce.nfixes;
    if (!stage_table.found(ce.key, sentry)) {
      sentry.key = ce.key;
      sentry.data.reset(new FixML::StageEntry::Data);
      sentry.data->boxflags.initialize(361, 180, 0, 0);
      if (fix.latitude == -90.) {
        sentry.data->boxflags.spole = 1;
      } else if (fix.latitude == 90.) {
        sentry.data->boxflags.npole = 1;
      } else {
        size_t lat_idx, lon_idx;
        geoutils::convert_lat_lon_to_box(1, fix.latitude, fix.longitude, lat_idx,
            lon_idx);
        sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
        sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
      }
      sentry.data->start = fix.datetime;
      sentry.data->end = fix.datetime;
      stage_table.insert(sentry);
    } else {
      if (fix.latitude == -90.) {
        sentry.data->boxflags.spole = 1;
      } else if (fix.latitude == 90.) {
        sentry.data->boxflags.npole = 1;
      } else {
        size_t lat_idx, lon_idx;
        geoutils::convert_lat_lon_to_box(1, fix.latitude, fix.longitude, lat_idx,
            lon_idx);
        sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
        sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
      }
      if (fix.datetime < sentry.data->start) {
        sentry.data->start = fix.datetime;
      }
      if (fix.datetime > sentry.data->end) {
        sentry.data->end = fix.datetime;
      }
    }
  }
  if ( (max_elon - min_elon) > (ce.max_lon - ce.min_lon)) {
    ce.min_lon = min_elon;
    if (ce.min_lon > 180.) {
      ce.min_lon -= 360.;
    }
    ce.max_lon = max_elon;
    if (ce.max_lon > 180.) {
      ce.max_lon -= 360.;
    }
  }
  fe.data->classification_list.emplace_back(ce);
  feature_table.insert(fe);
}

void process_TC_vitals(unique_ptr<Cyclone>& c) {
  TCVitalsCyclone *tcvc = reinterpret_cast<TCVitalsCyclone *>(c.get());
  auto fix_data = tcvc->fix_data();
  stringstream fe_key;
  fe_key << fix_data[0].datetime.year() << "-" << tcvc->storm_number() <<
      tcvc->basin_ID();
  FixML::FeatureEntry fe;
  if (!feature_table.found(fe_key.str(),fe)) {
    fe.key = fe_key.str();
    fe.data.reset(new FixML::FeatureEntry::Data);
    fe.data->alt_id = tcvc->ID();
    feature_table.insert(fe);
  }
  FixML::ClassificationEntry ce;
  ce.key = "tropical";
  if (fe.data->classification_list.size() == 0) {
    ce.start_datetime = ce.end_datetime = fix_data[0].datetime;
    ce.start_lat = ce.end_lat = ce.min_lat = ce.max_lat = fix_data[0].latitude;
    ce.start_lon = ce.end_lon = ce.min_lon = ce.max_lon = fix_data[0].longitude;
    ce.min_pres = ce.max_pres = fix_data[0].min_pres;
    ce.pres_units = "mbar";
    ce.min_speed = ce.max_speed = fix_data[0].max_wind;
    ce.wind_units = "m s-1";
    ce.nfixes = 1;
    fe.data->classification_list.emplace_back(ce);
  } else {
    FixML::ClassificationEntry &b =
        fe.data->classification_list.back();
    if (fix_data[0].datetime < b.start_datetime) {
      b.start_datetime = fix_data[0].datetime;
      b.start_lat = fix_data[0].latitude;
      b.start_lon = fix_data[0].longitude;
    }
    if (fix_data[0].datetime > b.end_datetime) {
      fe.data->alt_id = tcvc->ID();
      b.end_datetime = fix_data[0].datetime;
      b.end_lat = fix_data[0].latitude;
      b.end_lon = fix_data[0].longitude;
    }
    if (fix_data[0].latitude < b.min_lat) {
      b.min_lat = fix_data[0].latitude;
    }
    if (fix_data[0].latitude > b.max_lat) {
      b.max_lat = fix_data[0].latitude;
    }
    if (fix_data[0].longitude < b.min_lon) {
      b.min_lon = fix_data[0].longitude;
    }
    if (fix_data[0].longitude > b.max_lon) {
      b.max_lon = fix_data[0].longitude;
    }
    if (fix_data[0].min_pres < b.min_pres) {
      b.min_pres = fix_data[0].min_pres;
    }
    if (fix_data[0].min_pres > b.max_pres) {
      b.max_pres = fix_data[0].min_pres;
    }
    if (fix_data[0].max_wind < b.min_speed) {
      b.min_speed = fix_data[0].max_wind;
    }
    if (fix_data[0].max_wind > b.max_speed) {
      b.max_speed = fix_data[0].max_wind;
    }
    ++b.nfixes;
  }
  if (!stage_table.found(ce.key,sentry)) {
    sentry.key = ce.key;
    sentry.data.reset(new FixML::StageEntry::Data);
    sentry.data->boxflags.initialize(361,180,0,0);
    if (fix_data[0].latitude == -90.) {
      sentry.data->boxflags.spole = 1;
    } else if (fix_data[0].latitude == 90.) {
      sentry.data->boxflags.npole = 1;
    } else {
      size_t lat_idx, lon_idx;
      geoutils::convert_lat_lon_to_box(1, fix_data[0].latitude,
          fix_data[0].longitude, lat_idx, lon_idx);
      sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
      sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
    }
    sentry.data->start = fix_data[0].datetime;
    sentry.data->end = fix_data[0].datetime;
    stage_table.insert(sentry);
  } else {
    if (fix_data[0].latitude == -90.) {
      sentry.data->boxflags.spole = 1;
    } else if (fix_data[0].latitude == 90.) {
      sentry.data->boxflags.npole = 1;
    } else {
      size_t lat_idx, lon_idx;
      geoutils::convert_lat_lon_to_box(1, fix_data[0].latitude,
          fix_data[0].longitude, lat_idx, lon_idx);
      sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
      sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
    }
    if (fix_data[0].datetime < sentry.data->start) {
      sentry.data->start = fix_data[0].datetime;
    }
    if (fix_data[0].datetime > sentry.data->end) {
      sentry.data->end = fix_data[0].datetime;
    }
  }
}

void process_CXML(XMLDocument& xdoc) {
  auto production_center = xdoc.element(
      "cxml/header/productionCenter").content();
  trim(production_center);
  replace_all(production_center, "\n", "");
  while (strutils::contains(production_center, " ")) {
    replace_all(production_center, " ", "");
  }
  replace_all(production_center, "<subCenter>", "_");
  replace_all(production_center, "</subCenter>", "");
  auto model_name = xdoc.element("cxml/header/generatingApplication/model/name");
  if (!model_name.name().empty()) {
    production_center += "_" + model_name.content();
  }
  auto data_list = xdoc.element_list("cxml/data");
  for (auto& data_element : data_list) {
    auto disturbance_list = data_element.element_list("disturbance");
    for (auto& disturbance : disturbance_list) {
      auto fix_list = disturbance.element_list("fix");
      FixML::ClassificationEntry ce;
      ce.nfixes = fix_list.size();
      if (ce.nfixes > 0) {
        FixML::FeatureEntry fe;
        fe.key = disturbance.attribute_value("ID");
        auto cyclone_name = disturbance.element("cycloneName");
        if (!cyclone_name.content().empty()) {
          ce.key = "tropical";
          fe.key += "_" + cyclone_name.content();
        } else {
          ce.key = "extratropical";
        }
        if (!feature_table.found(fe.key, fe)) {
          fe.data.reset(new FixML::FeatureEntry::Data);
          feature_table.insert(fe);
        }
        auto type = data_element.attribute_value("type");
        ce.src = production_center + "_" + type;
        auto member = data_element.attribute_value("member");
        if (!member.empty()) {
          ce.src += "_member_" + member;
        }
        ce.start_datetime.set_year(1000);
        ce.start_lat = -99.;
        ce.start_lon = -199.;
        ce.min_lat = 99.;
        ce.min_lon = 199.;
        ce.max_lat = -99.;
        ce.max_lon = -199.;
        auto min_elon = 360.;
        auto max_elon = 0.;
        ce.min_pres = 99999.;
        ce.max_pres = -99999.;
        ce.min_speed = 99999.;
        ce.max_speed = -99999.;
        for (const auto& fix : fix_list) {
          auto valid_time = fix.element("validTime");
          if (!valid_time.name().empty()) {
            auto date_time = valid_time.content();
            replace_all(date_time, "-", "");
            replace_all(date_time, "T", "");
            replace_all(date_time, ":", "");
            replace_all(date_time, "Z", "");
            if (ce.start_datetime.year() == 1000) {
              ce.start_datetime.set(std::stoll(date_time));
            }
            ce.end_datetime.set(std::stoll(date_time));
          }
          auto lat = -99.;
          auto lon = -199.;
          auto latitude_element = fix.element("latitude");
          if (!latitude_element.name().empty()) {
            auto latitude = latitude_element.content();
            trim(latitude);
            if (!latitude.empty() && is_numeric(latitude)) {
              lat = std::stof(latitude);
              auto units = latitude_element.attribute_value("units");
              if (units == "deg S") {
                lat = -lat;
              }
            } else {
              cerr << "Terminating - process_CXML() found an empty latitude "
                  "element" << endl;
              exit(1);
            }
          }
          auto longitude_element = fix.element("longitude");
          if (!longitude_element.name().empty()) {
            auto longitude = longitude_element.content();
            trim(longitude);
            if (!longitude.empty() && is_numeric(longitude)) {
              lon = std::stof(longitude);
              auto units = longitude_element.attribute_value("units");
              if (units == "deg W") {
                lon = -lon;
              } else if (units == "deg E") {
                if (lon > 180.) {
                  lon -= 360.;
                }
              }
            } else {
              cerr << "Terminating - process_CXML() found an empty longitude "
                  "element" << endl;
              exit(1);
            }
          }
          if (lat > -99. && lon > -199.) {
            if (ce.start_lat < -90.) {
              ce.start_lat = lat;
              ce.start_lon = lon;
            }
            ce.end_lat = lat;
            ce.end_lon = lon;
            if (lat < ce.min_lat) {
              ce.min_lat = lat;
            }
            if (lon < ce.min_lon) {
              ce.min_lon = lon;
            }
            if (lat > ce.max_lat) {
              ce.max_lat = lat;
            }
            if (lon > ce.max_lon) {
              ce.max_lon = lon;
            }
            auto elon = lon;
            if (elon < 0.) {
              elon += 360.;
            }
            if (elon < min_elon) {
              min_elon = elon;
            }
            if (elon > max_elon) {
              max_elon = elon;
            }
            if (!stage_table.found(ce.key, sentry)) {
              sentry.key = ce.key;
              sentry.data.reset(new FixML::StageEntry::Data);
              sentry.data->boxflags.initialize(361, 180, 0, 0);
              if (lat == -90.) {
                sentry.data->boxflags.spole = 1;
              } else if (lat == 90.) {
                sentry.data->boxflags.npole = 1;
              } else {
                size_t lat_idx, lon_idx;
                geoutils::convert_lat_lon_to_box(1, lat, lon, lat_idx, lon_idx);
                sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
                sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
              }
              sentry.data->start = ce.end_datetime;
              sentry.data->end = ce.end_datetime;
              stage_table.insert(sentry);
            } else {
              if (lat == -90.) {
                sentry.data->boxflags.spole = 1;
              } else if (lat == 90.) {
                sentry.data->boxflags.npole = 1;
              } else {
                size_t lat_idx, lon_idx;
                geoutils::convert_lat_lon_to_box(1, lat, lon, lat_idx, lon_idx);
                sentry.data->boxflags.flags[lat_idx - 1][lon_idx] = 1;
                sentry.data->boxflags.flags[lat_idx - 1][360] = 1;
              }
              if (ce.end_datetime < sentry.data->start) {
                sentry.data->start = ce.end_datetime;
              }
              if (ce.end_datetime > sentry.data->end) {
                sentry.data->end = ce.end_datetime;
              }
            }
          }
          auto pressure_element = fix.element(
              "cycloneData/minimumPressure/pressure");
          if (!pressure_element.name().empty() && is_numeric(
              pressure_element.content())) {
            auto pressure = std::stof(pressure_element.content());
            if (pressure < ce.min_pres) {
              ce.min_pres = pressure;
            }
            if (pressure > ce.max_pres) {
              ce.max_pres = pressure;
            }
            ce.pres_units = pressure_element.attribute_value("units");
          }
          auto max_wind_speed_element = fix.element(
              "cycloneData/maximumWind/speed");
          if (!max_wind_speed_element.name().empty() && is_numeric(
              max_wind_speed_element.content())) {
            auto max_wind_speed = std::stof(max_wind_speed_element.content());
            if (max_wind_speed < ce.min_speed) {
              ce.min_speed = max_wind_speed;
            }
            if (max_wind_speed > ce.max_speed) {
              ce.max_speed = max_wind_speed;
            }
            ce.wind_units = max_wind_speed_element.attribute_value("units");
          }
        }
        if ( (max_elon - min_elon) > (ce.max_lon - ce.min_lon)) {
          ce.min_lon = min_elon;
          if (ce.min_lon > 180.) {
            ce.min_lon -= 360.;
          }
          ce.max_lon = max_elon;
          if (ce.max_lon > 180.) {
            ce.max_lon -= 360.;
          }
        }
        fe.data->classification_list.emplace_back(ce);
      }
    }
  }
}

bool open_file(void *istream, string filename) {
  if (metautils::args.data_format == "hurdat") {
    return (reinterpret_cast<InputHURDATCycloneStream *>(istream))->open(
        filename);
  } else if (metautils::args.data_format == "tcvitals") {
    return (reinterpret_cast<InputTCVitalsCycloneStream *>(istream))->open(
        filename);
  }
  return false;
}

void scan_file() {
  static const string THIS_FUNC = this_function_label(__func__);
  tfile.reset(new TempFile);
  tdir.reset(new TempDir);
  tfile->open(metautils::args.temp_loc);
  tdir->create(metautils::args.temp_loc);
  unique_ptr<idstream> istream;
  unique_ptr<Cyclone> c;
  if (metautils::args.data_format == "cxml") {
    string file_format, error;
    std::list<string> filelist;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
        *tdir, &filelist, file_format, error)) {
      log_error2("prepare_file_for_metadata_scanning() returned '" + error +
          "'", THIS_FUNC, "fix2xml", USER);
    }
    if (filelist.size() == 0) {
      filelist.emplace_back(tfile->name());
    }
    for (const auto& file : filelist) {
      XMLDocument xdoc(file);
      if (!xdoc) {
        log_error2("XML parse error: '"+xdoc.parse_error()+"'", THIS_FUNC,
            "fix2xml", USER);
      }
      process_CXML(xdoc);
      xdoc.close();
    }
  } else if (metautils::args.data_format == "tcvitals") {
    string file_format, error;
    std::list<string> filelist;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,          *tdir, &filelist, file_format, error)) {
      log_error2("prepare_file_for_metadata_scanning() returned '" + error +
          "'", THIS_FUNC, "fix2xml", USER);
    }
    if (filelist.size() == 0) {
      filelist.emplace_back(tfile->name());
    }
    istream.reset(new InputTCVitalsCycloneStream);
    c.reset(new TCVitalsCyclone);
    for (const auto& file : filelist) {
      if (!open_file(istream.get(), file)) {
        log_error2("unable to open file for input", THIS_FUNC, "fix2xml", USER);
      }
      const size_t BUF_LEN = 80000;
      unique_ptr<unsigned char[]> buffer(new unsigned char[BUF_LEN]);
      int status;
      while ( (status = istream->read(buffer.get(), BUF_LEN)) > 0) {
        c->fill(buffer.get(), Cyclone::full_report);
        process_TC_vitals(c);
      }
      istream->close();
      if (status == bfstream::error) {
        log_error2("read error", THIS_FUNC, "fix2xml", USER);
      }
    }
  } else {
    if (metautils::args.data_format == "hurdat") {
      istream.reset(new InputHURDATCycloneStream);
      c.reset(new HURDATCyclone);
    } else {
      log_error2("format " + metautils::args.data_format + " not recognized",
          THIS_FUNC, "fix2xml", USER);
    }
    string file_format, error;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
        *tdir, nullptr, file_format, error)) {
      log_error2("prepare_file_for_metadata_scanning() returned '" + error + "'",
          THIS_FUNC, "fix2xml", USER);
    }
    if (!open_file(istream.get(), tfile->name())) {
      log_error2("unable to open file for input", THIS_FUNC, "fix2xml", USER);
    }
    if (istream != nullptr) {
      const size_t BUF_LEN = 80000;
      unique_ptr<unsigned char[]> buffer(new unsigned char[BUF_LEN]);
      int status;
      while ( (status = istream->read(buffer.get(), BUF_LEN)) > 0) {
        c->fill(buffer.get(), Cyclone::full_report);
        if (metautils::args.data_format == "hurdat") {
          process_HURDAT(c);
        }
      }
      istream->close();
      if (status == bfstream::error) {
        log_error2("read error", THIS_FUNC, "fix2xml", USER);
      }
    }
  }
  if (feature_table.size() == 0) {
    cerr << "Terminating - no fix data found in file" << endl;
    exit(1);
  }
}

extern "C" void segv_handler(int) {
  clean_up();
  metautils::cmd_unregister();
  log_error2("core dump", "segv_handler()", "fix2xml", USER);
}

extern "C" void int_handler(int) {
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc, char **argv) {
  if (argc < 6) {
    cerr << "usage: " << argv[0] << " -f format -d [ds]nnn.n [options...] "
        "path" << endl;
    cerr << endl;
    cerr << "required (choose one):" << endl;
    cerr << "-f cxml     TIGGE Tropical Cyclone XML format" << endl;
    cerr << "-f hurdat   NHC HURDAT format" << endl;
    cerr << "-f tcvitals NCEP Tropical Cyclone Vital Statistics Records "
        "format" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "-d <nnn.n>  nnn.n is the dataset number to which the data file "
        "belongs" << endl;
    cerr << endl;
    cerr << "options:" << endl;
    cerr << "-g/-G            update/don't update graphics (default is -g)" <<
        endl;
    cerr << "-s/-S            do/don't update the dataset summary information "
        "(default is -s)" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "<path>      full MSS path or URL of the file to read" << endl;
    cerr << "            - MSS paths must begin with \"/FS/DECS\"" << endl;
    cerr << "            - URLs must begin with \"https://rda.ucar.edu\"" <<
        endl;
    exit(1);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  atexit(clean_up);
  auto arg_delimiter = '%';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv,
      arg_delimiter);
  metautils::read_config("fix2xml", USER);
  gatherxml::parse_args(arg_delimiter);
  string flags = "-f";
  if (strutils::has_beginning(metautils::args.path, "https://rda.ucar.edu")) {
    flags = "-wf";
  }
  metautils::cmd_register("fix2xml", USER);
  if (!metautils::args.overwrite_only) {
    metautils::check_for_existing_cmd("FixML", "fix2xml", USER);
  }
  scan_file();
  FixML::write(feature_table, stage_table, "fix2xml", USER);
  if (metautils::args.update_db) {
    if (!metautils::args.update_graphics) {
      flags = "-G " + flags;
    }
    if (!metautils::args.update_summary) {
      flags = "-S " + flags;
    }
    stringstream oss, ess;
    if (unixutils::mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsid + " " + flags + " " + metautils::args.filename +
        ".FixML", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
  }
}
