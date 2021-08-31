#include <iomanip>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <regex>
#include <unordered_map>
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
using std::make_tuple;
using std::move;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::tuple;
using std::unique_ptr;
using std::unordered_map;
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

tuple<string, string, string> bufr_types(size_t code, const DateTime&
    report_datetime, string station_id) {
  static const string F = this_function_label(__func__);
  static MySQL::Server mysrv;
  if (!mysrv) {
    mysrv.connect(metautils::directives.database_server, metautils::directives.
        metadb_username, metautils::directives.metadb_password, "");
    if (!mysrv) {
      log_error2("unable to connect to the database", F, "bufr2xml", USER);
    }
  }
  static unordered_map<size_t, tuple<string, string, string>> map;
  if (map.find(code) == map.end()) {
    MySQL::LocalQuery q("observation_type, platform_type, id_type", "metautil."
        "BUFRTypes", "code = " + itos(code));
    if (q.submit(mysrv) < 0 || q.num_rows() > 1) {
      log_error2("'" + q.error() + "' from query '" + q.show() + "'", F,
          "bufr2xml", USER);
    }
    if (q.num_rows() == 0) {
      log_error2("report type code " + itos(code) + " not recognized - date: " +
          report_datetime.to_string() + "  id: '" + station_id + "'", F,
          "bufr2xml", USER);
    }
    MySQL::Row row;
    q.fetch_row(row);
    auto idtyp = row[2];
    map.emplace(code, make_tuple(row[0], row[1], row[2]));
  }
  return map[code];
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
  string otyp, ptyp, idtyp;
  std::tie(otyp, ptyp, idtyp) = bufr_types(1000 + data[subset_number]->type.
      prepbufr, dt, data[subset_number]->stnid);
  string x1, x2;
  std::tie(x1, ptyp, x2) = bufr_types(2000 + data[subset_number]->type.
      prepbufr, dt, data[subset_number]->stnid);
  string id;
  if (!data[subset_number]->stnid.empty()) {
    id = data[subset_number]->stnid;
  } else if (!data[subset_number]->satid.empty()) {
    id = data[subset_number]->satid;
  } else if (!data[subset_number]->acftid.empty()) {
    id = data[subset_number]->acftid;
  } else {
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
  string otyp, ptyp, idtyp;
  std::tie(otyp, ptyp, idtyp) = bufr_types(bufr_report.data_type() * 1000 +
      bufr_report.data_subtype(), data[subset_number]->datetime, data[
      subset_number]->rpid);
  g_ientry.key = "";
  if (ptyp == "satellite") {
    if (!data[subset_number]->satid.empty()) {
      g_ientry.key = ptyp + "[!]BUFRsatID[!]" + clean_id(data[subset_number]->
          satid);
    } else if (!data[subset_number]->rpid.empty()) {
      if (idtyp.empty()) {
        idtyp = "other";
      }
      g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + clean_id(data[
          subset_number]->rpid);
    } else if (!data[subset_number]->stsn.empty()) {
      if (idtyp.empty()) {
        idtyp = "other";
      }
      g_ientry.key = ptyp + "[!]" + idtyp + "[!]"  + clean_id(data[
          subset_number]->stsn);
    }
  } else if (!data[subset_number]->wmoid.empty()) {
    g_ientry.key = ptyp + "[!]WMO[!]" + clean_id(data[subset_number]->wmoid);
  } else if (!data[subset_number]->acrn.empty()) {
    g_ientry.key = ptyp + "[!]callSign[!]" + clean_id(data[subset_number]->
        acrn);
  } else if (!data[subset_number]->acftid.empty()) {
    g_ientry.key = ptyp + "[!]other[!]" + clean_id(data[subset_number]->
        acftid);
  } else if (!data[subset_number]->rpid.empty()) {
    g_ientry.key = clean_id(data[subset_number]->rpid);
    if ((data[subset_number]->rpid == "PSPIU" || data[subset_number]->rpid ==
        "PSGAL")) {
      ptyp = "wind_profiler";
      idtyp = "callSign";
    } else if (ptyp == "NEXRAD" && (g_ientry.key.length() != 7 || !strutils::
        is_numeric(g_ientry.key.substr(0, 4)) || !strutils::is_alpha(g_ientry.
        key.substr(4)))) {
      idtyp = "other";
    } else if (idtyp.empty()) {
      idtyp = "other";
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + g_ientry.key;
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
  string otyp, ptyp, idtyp;
  std::tie(otyp, ptyp, idtyp) = bufr_types(bufr_report.data_type() * 1000 +
      bufr_report.data_subtype(), bufr_report.date_time(), data[subset_number]->
      satid);
  g_ientry.key.clear();
  if (!data[subset_number]->satid.empty()) {
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + clean_id(data[subset_number]->
        satid);
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
  string otyp, ptyp, idtyp;
  std::tie(otyp, ptyp, idtyp) = bufr_types(data[subset_number]->ecmwf_os.
      rdb_type * 1000 + data[subset_number]->ecmwf_os.rdb_subtype, data[
      subset_number]->datetime, data[subset_number]->ecmwf_os.ID);
  if (idtyp == "WMO") {
    auto s = data[subset_number]->wmoid.substr(0, 2);
    if (s < "01" || s > "98") {
      idtyp = "other";
    }
  }
  if (ptyp == "land_station" || ptyp == "wind_profiler") {
    if (data[subset_number]->wmoid.empty()) {
      log_error2("WMO ID not found - date: " + data[subset_number]->datetime.
          to_string(), F, "bufr2xml", USER);
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->wmoid;
  } else if (ptyp == "roving_ship") {
    if (data[subset_number]->shipid.empty()) {
      log_error2(string("SHIP ID not found - date: ") + data[subset_number]->
          datetime.to_string(), F, "bufr2xml", USER);
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->shipid;
  } else if (ptyp == "drifting_buoy") {
    if (data[subset_number]->buoyid.empty()) {
      log_error2("BUOY ID not found - date: " + data[subset_number]->datetime.
          to_string(), F, "bufr2xml", USER);
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->buoyid;
  } else if (ptyp == "satellite") {
    if (data[subset_number]->satid.empty()) {
      log_error2("SATELLITE ID not found - date: " + data[subset_number]->
          datetime.to_string(), F, "bufr2xml", USER);
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->satid;
  } else if (ptyp == "aircraft") {
    if (data[subset_number]->acftid.empty()) {
      log_error2("ACFT ID not found - date: " + data[subset_number]->datetime.
          to_string(), F, "bufr2xml", USER);
    }
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->acftid;
  } else if (ptyp == "bogus") {
    g_ientry.key = ptyp + "[!]" + idtyp + "[!]" + data[subset_number]->
        ecmwf_os.ID;
  }
  if (!obs_data.added_to_platforms(otyp, ptyp, data[subset_number]->lat,
      data[subset_number]->lon)) {
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
