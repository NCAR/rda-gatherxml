#include <fstream>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
using std::deque;
using std::endl;
using std::regex;
using std::regex_search;
using std::sort;
using std::stoi;
using std::stof;
using std::string;
using std::stringstream;
using std::tie;
using std::unique_ptr;
using std::vector;
using strutils::ftos;
using strutils::itos;
using strutils::split;
using strutils::replace_all;
using strutils::substitute;
using strutils::to_upper;
using unixutils::mysystem2;
using unixutils::rdadata_sync;
using unixutils::rdadata_sync_from;

namespace gatherxml {

namespace fileInventory {

void close(string filename, unique_ptr<TempDir>& tdir, std::ofstream& ofs,
    string cmd_type, bool insert_into_db, bool create_cache, string caller,
    string user) {
  if (!ofs.is_open()) {
    return;
  }
  ofs.close();
  ofs.clear();
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  srv.update("W" + cmd_type + ".ds" + substitute(metautils::args.dsnum, ".", "")
      + "_webfiles2", "inv = 'Y'", "webID = '" + substitute(filename, "%", "/")
      + "'");
  if (metautils::args.inventory_only) {
    metautils::args.filename = filename;
  }
  srv.disconnect();
  stringstream oss, ess;
  if (insert_into_db) {
    auto c = metautils::directives.local_root + "/bin/iinv";
    if (!create_cache) {
      c += " -C";
    }
    c += " -d " + metautils::args.dsnum + " -t " + tdir->name() + " -f " +
        filename + "." + cmd_type + "_inv";
    mysystem2(c, oss, ess);
    if (!ess.str().empty()) {
      log_warning("close(): " + ess.str() + "' while running iinv", caller,
          user);
      tdir->set_keep();
    }
  }
}

void open(string& filename, unique_ptr<TempDir>& tdir, std::ofstream& ofs,
    string cmd_type, string caller, string user) {
  static const string F = this_function_label(__func__);
  if (ofs.is_open()) {
    return;
  }
  if (!regex_search(metautils::args.path, regex("^https://rda.ucar.edu"))) {
    return;
  }
  filename = metautils::relative_web_filename(metautils::args.path + "/" +
      metautils::args.filename);
  replace_all(filename, "/", "%");
  if (tdir == nullptr) {
    tdir.reset(new TempDir());
    if (!tdir->create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary directory", F, caller, user);
    }
  }
  ofs.open(tdir->name() + "/" + filename + "." + cmd_type + "_inv");
  if (!ofs.is_open()) {
    log_error2("couldn't open the inventory output file", F, caller, user);
  }
}

} // end namespace gatherxml::fileInventory

void copy_ancillary_files(string file_type, string metadata_file, string
    file_type_name, string caller, string user) {
  static const string F = this_function_label(__func__);
  stringstream oss, ess;
  if (mysystem2("/bin/tcsh -c \"curl -s --data 'authKey=qGNlKijgo9DJ7MN&cmd="
      "listfiles&value=/SERVER_ROOT/web/datasets/ds" + metautils::args.dsnum +
      "/metadata/fmd&pattern=" + metadata_file + "' http://rda.ucar.edu/"
      "cgi-bin/dss/remoteRDAServerUtils\"", oss, ess) != 0) {
    log_warning(F + ": unable to copy " + file_type + " files - error: '" + ess.
        str() + "'", caller, user);
  } else {
    auto sp = split(oss.str(), "\n");
    auto sp2 = split(sp[0], "/");
    string hn = "/" + sp2[1];
    TempFile tf(metautils::directives.temp_path);
    TempDir td;
    if (!td.create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary directory", F, caller, user);
    }

    // create the directory tree in the temp directory
    string p = "metadata/wfmd";
    if (mysystem2("/bin/mkdir -p " + td.name() + "/" + p, oss, ess) != 0) {
      log_error2("unable to create a temporary directory - '" + ess.str() + "'",
          F, caller, user);
    }
    for (size_t n = 0; n < sp.size(); ++n) {
      if (sp[n].substr(sp[n].length() - 4) == ".xml") {
        auto h = sp[n];
        replace_all(h, hn, "/__HOST__");
        rdadata_sync_from(h, tf.name(), metautils::directives.rdadata_home,
            ess);
        std::ifstream ifs(tf.name().c_str());
        if (ifs.is_open()) {
          auto sp2 = split(sp[n], file_type);
          std::ofstream ofs((td.name() + "/" + p + "/" + file_type_name + "." +
              file_type + sp2[1]).c_str());
          char l[32768];
          ifs.getline(l, 32768);
          while (!ifs.eof()) {
            string s = l;
            if (strutils::contains(s, "parent=")) {
              replace_all(s, metadata_file, file_type_name + "." + file_type);
            }
            ofs << s << endl;
            ifs.getline(l, 32768);
          }
          ifs.close();
          ofs.close();
          ofs.clear();
        } else {
          log_warning(F + "(): unable to copy " + file_type + " file '" + sp[n]
              + "'", caller, user);
        }
      }
    }
    string e;
    if (rdadata_sync(td.name(), p + "/", "/data/web/datasets/ds" + metautils::
        args.dsnum, metautils::directives.rdadata_home, e) < 0) {
      log_warning(F + "(): unable to sync - rdadata_sync error(s): '" + e + "'",
          caller, user);
    }
  }
}

namespace markup {

void write_finalize(string filename, string ext, string tdir_name, std::
    ofstream& ofs, string caller, string user) {
  static const string F = this_function_label(__func__);
  ofs.close();
  if (ext != "GrML") {
    string s = "metadata/wfmd";
    if (system(("gzip " + tdir_name + "/" + s + "/" + filename + "." + ext).
        c_str()) != 0) {
      log_error2("unable to gzip metadata file", F, caller, user);
    }
    string e;
    if (rdadata_sync(tdir_name, s + "/", "/data/web/datasets/ds" + metautils::
        args.dsnum, metautils::directives.rdadata_home, e) < 0) {
      log_warning(F + "(): unable to sync '" + filename + "." + ext + "' - "
          "rdadata_sync error(s): '" + e + "'", caller, user);
    }
  }
  metautils::args.filename = filename;
}

void write_initialize(string& filename, string ext, string tdir_name, std::
    ofstream& ofs, string caller, string user) {
  static const string F = this_function_label(__func__);
  if (metautils::args.dsnum == "test") {
    filename = metautils::args.filename;
  } else {
    filename = metautils::args.path + "/" + metautils::args.filename;
    filename = metautils::relative_web_filename(filename);
    replace_all(filename, "/", "%");
  }
  if (ext == "GrML") {

    // gridded content metadata is not stored on the server; it goes into a temp
    //   directory for 'scm' and then gets deleted
    ofs.open(tdir_name + "/" + filename + "." + ext);
  } else {

    // this section is for metadata that gets stored on the server
    string s = "metadata/wfmd";

    // create the directory tree in the temp directory
    stringstream oss, ess;
    if (mysystem2("/bin/mkdir -p " + tdir_name + "/" + s + "/", oss, ess) !=
        0) {
      log_error2("unable to create a temporary directory", F, caller, user);;
    }
    ofs.open(tdir_name + "/" + s + "/" + filename + "." + ext);
  }
  if (!ofs.is_open()) {
    log_error2("unable to open file for output", F, caller, user);
  }
  ofs << "<?xml version=\"1.0\" ?>" << endl;
}

namespace FixML {

void copy(string metadata_file, string URL, string caller, string user) {
  static const string F = this_function_label(__func__);
  auto f = metautils::relative_web_filename(URL);
  replace_all(f, "/", "%");
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }

  // create the directory tree in the temp directory
  string s = "metadata/wfmd";
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + tdir.name() + "/" + s, oss, ess) != 0) {
    log_error2("unable to create a temporary directory - '" + ess.str() + "'",
        F, caller, user);
  }
  TempFile t(metautils::directives.temp_path);
  rdadata_sync_from("/__HOST_/web/datasets/ds" + metautils::args.dsnum + "/metadata/fmd/" + metadata_file, t.name(), metautils::directives.rdadata_home, ess);
  std::ifstream ifs(t.name().c_str());
  if (!ifs.is_open()) {
    log_error2("unable to open input file", F, caller, user);
  }
  std::ofstream ofs((tdir.name() + "/" + s + "/" + f + ".FixML").c_str());
  if (!ofs.is_open()) {
    log_error2("unable to open output file", F, caller, user);
  }
  char l[32768];
  ifs.getline(l, 32768);
  while (!ifs.eof()) {
    string sl = l;
    if (strutils::contains(sl, "uri=")) {
      sl = sl.substr(sl.find(" format"));
      sl = "      uri=\"" + URL + "\"" + s;
    } else if (regex_search(sl, regex("ref="))) {
      replace_all(sl, metadata_file, f + ".FixML");
    }
    ofs << sl << endl;
    ifs.getline(l, 32768);
  }
  ifs.close();
  ofs.close();
  string e;
  if (rdadata_sync(tdir.name(), s + "/", "/data/web/datasets/ds" + metautils::
      args.dsnum, metautils::directives.rdadata_home, e) < 0) {
    log_warning("copy(): unable to sync '" + f + ".FixML' - rdadata_sync error("
        "s): '" + e + "'", caller, user);
  }
  copy_ancillary_files("FixML", metadata_file, f, caller, user);
  metautils::args.filename = f;
}

void write(my::map<FeatureEntry>& feature_table, my::map<StageEntry>&
    stage_table, string caller, string user) {
  static const string F = "FixML::" + this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  string f;
  std::ofstream ofs;
  write_initialize(f, "FixML", t.name(), ofs, caller, user);
  ofs << "<FixML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" <<
      endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << endl;
  ofs << "                           https://rda.ucar.edu/schemas/FixML.xsd\""
      << endl;
  ofs << "       uri=\"" << metautils::args.path << "/" << metautils::args.
      filename << "\" format=\"";
  if (metautils::args.data_format == "hurdat") {
    ofs << "HURDAT";
  } else if (metautils::args.data_format == "cxml") {
    ofs << "CXML";
  } else if (metautils::args.data_format == "tcvitals") {
    ofs << "proprietary_ASCII";
  }
  ofs << "\">" << endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string(
      "%Y-%m-%d %T %Z") << "\" />" << endl;
  ofs.setf(std::ios::fixed);
  ofs.precision(1);
  for (const auto& k : feature_table.keys()) {
    FeatureEntry fe;
    feature_table.found(k, fe);
    ofs << "  <feature ID=\"" << fe.key;
    if (!fe.data->alt_id.empty()) {
      ofs << "-" << fe.data->alt_id;
    }
    ofs << "\">" << endl;
    for (const auto& e : fe.data->classification_list) {
      ClassificationEntry ce;
      ce = e;
      ofs << "    <classification stage=\"" << ce.key << "\" nfixes=\"" << ce.
          nfixes << "\"";
      if (!ce.src.empty()) {
        ofs << " source=\"" << ce.src << "\"";
      }
      ofs << ">" << endl;
      ofs << "      <start dateTime=\"" << ce.start_datetime.to_string(
          "%Y-%m-%d %H:%MM %Z") << "\" latitude=\"" << fabs(ce.start_lat);
      if (ce.start_lat >= 0.) {
        ofs << "N";
      } else {
        ofs << "S";
      }
      ofs << "\" longitude=\"" << fabs(ce.start_lon);
      if (ce.start_lon < 0.) {
        ofs << "W";
      } else {
        ofs << "E";
      }
      ofs << "\" />" << endl;
      ofs << "      <end dateTime=\"" << ce.end_datetime.to_string(
          "%Y-%m-%d %H:%MM %Z") << "\" latitude=\"" << fabs(ce.end_lat);
      if (ce.end_lat >= 0.) {
        ofs << "N";
      } else {
        ofs << "S";
      }
      ofs << "\" longitude=\"" << fabs(ce.end_lon);
      if (ce.end_lon < 0.) {
        ofs << "W";
      } else {
        ofs << "E";
      }
      ofs << "\" />" << endl;
      ofs << "      <centralPressure units=\"" << ce.pres_units << "\" min=\""
          << static_cast<int>(ce.min_pres) << "\" max=\"" << static_cast<int>(
          ce.max_pres) << "\" />" << endl;
      ofs << "      <windSpeed units=\"" << ce.wind_units << "\" min=\"" <<
          static_cast<int>(ce.min_speed) << "\" max=\"" << static_cast<int>(ce.
          max_speed) << "\" />" << endl;
      ofs << "      <boundingBox southWest=\"" << ce.min_lat << "," << ce.
          min_lon << "\" northEast=\"" << ce.max_lat << "," << ce.max_lon <<
          "\" />" << endl;
      ofs << "    </classification>" << endl;
    }
    ofs << "  </feature>" << endl;
  }
  ofs << "</FixML>" << endl;
  for (const auto& k : stage_table.keys()) {
    std::ofstream ofs2((t.name() + "/metadata/wfmd/" + f + ".FixML." + k +
        ".locations.xml").c_str());
    ofs2 << "<?xml version=\"1.0\" ?>" << endl;
    ofs2 << "<locations parent=\"" << f << ".FixML\" stage=\"" << k << "\">" <<
        endl;
    StageEntry se;
    stage_table.found(k, se);
    if (se.data->boxflags.spole == 1) {
      ofs2 << "  <box1d row=\"0\" bitmap=\"0\" />" << endl;
    }
    for (size_t n = 0; n < 180; ++n) {
      if (se.data->boxflags.flags[n][360] == 1) {
        ofs2 << "  <box1d row=\"" << n + 1 << "\" bitmap=\"";
        for (size_t m = 0; m < 360; ++m) {
          ofs2 << static_cast<int>(se.data->boxflags.flags[n][m]);
        }
        ofs2 << "\" />" << endl;
      }
    }
    if (se.data->boxflags.npole == 1) {
      ofs2 << "  <box1d row=\"181\" bitmap=\"0\" />" << endl;
    }
    ofs2 << "</locations>" << endl;
    ofs2.close();
  }
  write_finalize(f, "FixML", t.name(), ofs, caller, user);
}

} // end namespace gatherxml::markup::FixML

namespace GrML {

void write_latitude_longitude_grid(deque<string>& grid_params, bool is_cell,
    std::ofstream& ofs, string caller, string user) {
  auto slat = grid_params[3];
  if (slat.front() == '-') {
    slat = slat.substr(1) + "S";
  } else {
    slat += "N";
  }
  auto slon = grid_params[4];
  if (slon.front() == '-') {
    slon = slon.substr(1) + "W";
  } else {
    slon += "E";
  }
  auto elat = grid_params[5];
  if (elat.front() == '-') {
    elat = elat.substr(1) + "S";
  } else {
    elat += "N";
  }
  auto elon = grid_params[6];
  if (elon.front() == '-') {
    elon = elon.substr(1) + "W";
  } else {
    elon += "E";
  }
  if (stof(grid_params[7]) == 0. || stof(grid_params[8]) == 0.) {
    log_error2("horizontal grid resolutions must be non-zero",
        "write_latitude_longitude_grid()", caller, user);
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\"latLon\" "
      "numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\" "
      "startLat=\"" << slat << "\" startLon=\"" << slon << "\" endLat=\"" <<
      elat << "\" endLon=\"" << elon << "\" xRes=\"" << grid_params[7] << "\" "
      "yRes=\"" << grid_params[8] << "\"";
  if (is_cell) {
    ofs << " isCell=\"true\"";
  }
  ofs << ">" << endl;
}

void write_gaussian_latitude_longitude_grid(deque<string>& grid_params, bool
    is_cell, std::ofstream& ofs) {
  auto re = regex("^-");
  auto a = grid_params[3];
  if (regex_search(grid_params[3], re)) {
    grid_params[3] = grid_params[3].substr(1) + "S";
  } else {
    grid_params[3] += "N";
  }
  if (regex_search(grid_params[4], re)) {
    grid_params[4] = grid_params[4].substr(1) + "W";
  } else {
    grid_params[4] += "E";
  }
  if (regex_search(grid_params[5], re)) {
    grid_params[5] = grid_params[5].substr(1) + "S";
  } else {
    grid_params[5] += "N";
  }
  if (regex_search(grid_params[6], re)) {
    grid_params[6] = grid_params[6].substr(1) + "W";
  } else {
    grid_params[6] += "E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\""
      "gaussLatLon\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[
      2] << "\" startLat=\"" << grid_params[3] << "\" startLon=\"" <<
      grid_params[4] << "\" endLat=\"" << grid_params[5] << "\" endLon=\"" <<
      grid_params[6] << "\" xRes=\"" << grid_params[7] << "\" circles=\"" <<
      grid_params[8] << "\">" << endl;
}

void write_polar_stereographic_grid(deque<string>& grid_params, bool is_cell,
    std::ofstream& ofs) {
  auto re = regex("^-");
  if (regex_search(grid_params[3], re)) {
    grid_params[3] = grid_params[3].substr(1) + "S";
  } else {
    grid_params[3] += "N";
  }
  if (regex_search(grid_params[4], re)) {
    grid_params[4] = grid_params[4].substr(1) + "W";
  } else {
    grid_params[4] += "E";
  }
  if (regex_search(grid_params[5], re)) {
    grid_params[5] = grid_params[5].substr(1) + "S";
  } else {
    grid_params[5] += "N";
  }
  if (regex_search(grid_params[6], re)) {
    grid_params[6] = grid_params[6].substr(1) + "W";
  } else {
    grid_params[6] += "E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[10] << "\" definition=\""
      "polarStereographic\" numX=\"" << grid_params[1] << "\" numY=\"" <<
      grid_params[2] << "\"  startLat=\"" << grid_params[3] << "\" startLon=\""
      << grid_params[4] << "\" resLat=\"" << grid_params[5] << "\" projLon=\""
      << grid_params[6] << "\" pole=\"" << grid_params[9] << "\" xRes=\"" <<
      grid_params[7] << "\" yRes=\"" << grid_params[8] << "\">" << endl;
}

void write_lambert_conformal_grid(deque<string>& grid_params, bool is_cell,
    std::ofstream& ofs) {
  auto re = regex("^-");
  if (regex_search(grid_params[3], re)) {
    grid_params[3] = grid_params[3].substr(1) + "S";
  } else {
    grid_params[3] += "N";
  }
  if (regex_search(grid_params[4], re)) {
    grid_params[4] = grid_params[4].substr(1) + "W";
  } else {
    grid_params[4] += "E";
  }
  if (regex_search(grid_params[5], re)) {
    grid_params[5] = grid_params[5].substr(1) + "S";
  } else {
    grid_params[5] += "N";
  }
  if (regex_search(grid_params[6], re)) {
    grid_params[6] = grid_params[6].substr(1) + "W";
  } else {
    grid_params[6] += "E";
  }
  if (regex_search(grid_params[10], re)) {
    grid_params[10] = grid_params[10].substr(1) + "S";
  } else {
    grid_params[10] += "N";
  }
  if (regex_search(grid_params[11], re)) {
    grid_params[11] = grid_params[11].substr(1) + "S";
  } else {
    grid_params[11] += "N";
  }
  ofs << "  <grid timeRange=\"" << grid_params[12] << "\" definition=\""
      "lambertConformal\" numX=\"" << grid_params[1] << "\" numY=\"" <<
      grid_params[2] << "\"  startLat=\"" << grid_params[3] << "\" startLon=\""
      << grid_params[4] << "\" resLat=\"" << grid_params[5] << "\" projLon=\""
      << grid_params[6] << "\" pole=\"" << grid_params[9] << "\" xRes=\"" <<
      grid_params[7] << "\" yRes=\"" << grid_params[8] << "\" stdParallel1=\""
      << grid_params[10] << "\" stdParallel2=\"" << grid_params[11] << "\">" <<
      endl;
}

void write_mercator_grid(deque<string>& grid_params, bool is_cell, std::
    ofstream& ofs) {
  auto re = regex("^-");
  if (regex_search(grid_params[3], re)) {
    grid_params[3] = grid_params[3].substr(1) + "S";
  } else {
    grid_params[3] += "N";
  }
  if (regex_search(grid_params[4], re)) {
    grid_params[4] = grid_params[4].substr(1) + "W";
  } else {
    grid_params[4] += "E";
  }
  if (regex_search(grid_params[5], re)) {
    grid_params[5] = grid_params[5].substr(1) + "S";
  } else {
    grid_params[5] += "N";
  }
  if (regex_search(grid_params[6], re)) {
    grid_params[6] = grid_params[6].substr(1) + "W";
  } else {
    grid_params[6] += "E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\""
      "mercator\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2]
      << "\" startLat=\"" << grid_params[3] << "\" startLon=\"" << grid_params[
      4] << "\" endLat=\"" << grid_params[5] << "\" endLon=\"" << grid_params[6]
      << "\" xRes=\"" << grid_params[7] << "\" yRes=\"" << grid_params[8] <<
      "\">" << endl;
}

void write_spherical_harmonics_grid(deque<string>& grid_params, bool is_cell,
    std::ofstream& ofs) {
  ofs << "  <grid timeRange=\"" << grid_params[4] << "\" definition=\""
      "sphericalHarmonics\" t1=\"" << grid_params[1] << "\" t2=\"" <<
      grid_params[2] << "\" t3=\"" << grid_params[3] << "\">" << endl;
}

void write_grid(string grid_entry_key, std::ofstream& ofs, string caller, string
    user) {
  auto sp = split(grid_entry_key, "<!>");
  auto b = false;
  if (sp.front().back() == 'C') {
    b = true;
    sp.front().pop_back();
  }
  switch (stoi(sp.front())) {
    case static_cast<int>(Grid::Type::latitudeLongitude): {
      write_latitude_longitude_grid(sp, b, ofs, caller, user);
      break;
    }
    case static_cast<int>(Grid::Type::gaussianLatitudeLongitude): {
      write_gaussian_latitude_longitude_grid(sp, b, ofs);
      break;
    }
    case static_cast<int>(Grid::Type::polarStereographic): {
      write_polar_stereographic_grid(sp, b, ofs);
      break;
    }
    case static_cast<int>(Grid::Type::lambertConformal): {
      write_lambert_conformal_grid(sp, b, ofs);
      break;
    }
    case static_cast<int>(Grid::Type::mercator): {
      write_mercator_grid(sp, b, ofs);
      break;
    }
    case static_cast<int>(Grid::Type::sphericalHarmonics): {
      write_spherical_harmonics_grid(sp, b, ofs);
      break;
    }
    default: {
      log_error2("no grid definition map for " + sp.front(), "write_grid()",
          caller, user);
    }
  }
}

string write(my::map<GridEntry>& grid_table, string caller, string user) {
  static const string F = "GrML::" + this_function_label(__func__);
  TempDir t; // t.name() is the return value
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create a temporary directory", F, caller, user);
  }
  t.set_keep();
  string f;
  std::ofstream ofs;
  write_initialize(f, "GrML", t.name(), ofs, caller, user);
  ofs << "<GrML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << endl;
  ofs << "      xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << endl;
  ofs << "                          https://rda.ucar.edu/schemas/GrML.xsd\"" <<
      endl;
  ofs << "      uri=\"" << metautils::args.path << "/" << metautils::args.
      filename;
  ofs << "\" format=\"";
  if (metautils::args.data_format == "grib") {
    ofs << "WMO_GRIB1";
  } else if (metautils::args.data_format == "grib0") {
    ofs << "WMO_GRIB0";
  } else if (metautils::args.data_format == "grib2") {
    ofs << "WMO_GRIB2";
  } else if (metautils::args.data_format == "jraieeemm") {
    ofs << "proprietary_Binary";
  } else if (metautils::args.data_format == "oct") {
    ofs << "DSS_Octagonal_Grid";
  } else if (metautils::args.data_format == "tropical") {
    ofs << "DSS_Tropical_Grid";
  } else if (metautils::args.data_format == "ll") {
    ofs << "DSS_5-Degree_LatLon_Grid";
  } else if (metautils::args.data_format == "slp") {
    ofs << "DSS_SLP_Grid";
  } else if (metautils::args.data_format == "navy") {
    ofs << "DSS_Navy_Grid";
  } else if (metautils::args.data_format == "ussrslp") {
    ofs << "USSR_SLP_Grid";
  } else if (metautils::args.data_format == "on84") {
    ofs << "NCEP_ON84";
  } else if (metautils::args.data_format == "netcdf") {
    ofs << "netCDF";
  } else if (metautils::args.data_format == "hdf5nc4") {
    ofs << "netCDF4";
  } else if (metautils::args.data_format == "cgcm1") {
    ofs << "proprietary_ASCII";
  } else if (metautils::args.data_format == "cmorph025") {
    ofs << "NCEP_CPC_CMORPH025";
  } else if (metautils::args.data_format == "cmorph8km") {
    ofs << "NCEP_CPC_CMORPH8km";
  } else if (metautils::args.data_format == "gpcp") {
    ofs << "NASA_GSFC_GPCP";
  } else {
    ofs << "??";
  }
  ofs << "\">" << endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string(
      "%Y-%m-%d %T %Z") << "\" />" << endl;
  vector<string> v;
  v.reserve(grid_table.size());
  for (const auto& k : grid_table.keys()) {
    v.emplace_back(k);
  }
  sort(v.begin(), v.end(),
  [](const string& left, const string& right) -> bool {
    if (left[0] < right[0]) {
      return true;
    }
    return false;
  });
  for (size_t m = 0; m < grid_table.size(); ++m) {
    GridEntry ge;
    grid_table.found(v[m], ge);

    // print out <grid> element
    write_grid(ge.key, ofs, caller, user);

    // print out <process> elements
    for (const auto& e : ge.process_table) {
      ofs << "    <process value=\"" << e << "\" />" << endl;
    }

    // print out <ensemble> elements
    for (const auto& e : ge.ensemble_table) {
      auto sp = split(e, "<!>");
      ofs << "    <ensemble type=\"" << sp[0] << "\"";
      if (!sp[1].empty()) {
        ofs << " ID=\"" << sp[1] << "\"";
      }
      if (sp.size() > 2) {
        ofs << " size=\"" << sp[2] << "\"";
      }
      ofs << " />" << endl;
    }

    // print out <level> elements
    vector<string> used;
    for (const auto& k : ge.level_table.keys()) {
      LevelEntry le;
      ge.level_table.found(k, le);
      auto sp = split(k, ":");
      if (metautils::args.data_format == "grib" || metautils::args.data_format
          == "grib0" || metautils::args.data_format == "grib2" || metautils::
          args.data_format == "netcdf" || metautils::args.data_format ==
          "hdf5nc4") {
        if (strutils::occurs(k, ":") == 1) {
          ofs << "    <level map=\"";
          auto idx = sp[0].find(",");
          if (idx != string::npos) {
            ofs << sp[0].substr(0, idx) << "\" type=\"" << sp[0].substr(idx +
                1);
          } else {
            ofs << sp[0];
          }
          ofs << "\" value=\"" << sp[1] << "\">" << endl;
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            auto idx = code.find(":");
            ofs << "      <parameter map=\"" << code.substr(0, idx) << "\" "
                "value=\"" << code.substr(idx + 1) << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </level>" << endl;
          used.emplace_back(k);
        }
      } else if (metautils::args.data_format == "oct" || metautils::args.
          data_format == "tropical" || metautils::args.data_format == "ll" ||
          metautils::args.data_format == "navy" || metautils::args.data_format
          == "slp") {
        if (sp[0] == "0") {
          if (sp[1] == "1013" || sp[1] == "1001" || sp[1] == "980" || sp[1] ==
              "0") {
            ofs << "    <level type=\"" << sp[1] << "\" value=\"0\">" << endl;
          } else {
            ofs << "    <level value=\"" << sp[1] << "\">" << endl;
          }
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            ofs << "      <parameter value=\"" << code << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </level>" << endl;
          used.emplace_back(k);
        }
      } else if (metautils::args.data_format == "jraieeemm") {
        ofs << "    <level map=\"ds" << metautils::args.dsnum << "\" type=\"" <<
            sp[0] << "\" value=\"" << sp[1] << "\">" << endl;
        for (const auto& e : le.parameter_code_table) {
          string code;
          ParameterEntry pe;
          tie(code, pe) = e;
          ofs << "      <parameter map=\"ds" << metautils::args.dsnum << "\" "
              "value=\"" << code << "\" start=\"" << pe.start_date_time.
              to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pe.end_date_time.
              to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pe.
              num_time_steps << "\" />" << endl;
        }
        ofs << "    </level>" << endl;
        used.emplace_back(k);
      } else if (metautils::args.data_format == "on84") {
        auto n = stoi(sp[0]);
        if (n < 3 || n == 6 || n <= 7 || (n == 8 && sp.size() < 3) || (n >= 128
            && n <= 135)) {
          ofs << "    <level type=\"" << n << "\" value=\"" << sp[1] << "\">" <<
              endl;
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            ofs << "      <parameter value=\"" << code << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </level>" << endl;
          used.emplace_back(k);
        }
      } else if (metautils::args.data_format == "cgcm1") {
        ofs << "    <level map=\"ds" << metautils::args.dsnum << "\" type=\"" <<
            sp[0] << "\" value=\"";
        auto a = sp[0];
        if (a == "1") {
          a = "0";
        }
        ofs << a << "\">" << endl;
        for (const auto& e : le.parameter_code_table) {
          string code;
          ParameterEntry pe;
          tie(code, pe) = e;
          ofs << "      <parameter map=\"ds" << metautils::args.dsnum << "\" "
              "value=\"" << code << "\" start=\"" << pe.start_date_time.
              to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pe.end_date_time.
              to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pe.
              num_time_steps << "\" />" << endl;
        }
        ofs << "    </level>" << endl;
        used.emplace_back(k);
      } else if (metautils::args.data_format == "cmorph025" || metautils::args.
          data_format == "cmorph8km" || metautils::args.data_format == "gpcp") {
        ofs << "    <level type=\"" << k << "\" value=\"0\">" << endl;;
        for (const auto& e : le.parameter_code_table) {
          string code;
          ParameterEntry pe;
          tie(code, pe) = e;
          ofs << "      <parameter value=\"" << code << "\" start=\"" << pe.
              start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pe.
              end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pe.
              num_time_steps << "\" />" << endl;
        }
        ofs << "    </level>" << endl;
        used.emplace_back(k);
      } else {
        ofs << k << endl;
      }
    }
    for (const auto& e : used) {
      ge.level_table.remove(e);
    }

    // print out <layer> elements
    used.clear();
    for (const auto& k : ge.level_table.keys()) {
      LevelEntry le;
      ge.level_table.found(k, le);
      auto sp = split(k, ":");
      if (metautils::args.data_format == "grib" || metautils::args.data_format
          == "grib0" || metautils::args.data_format == "grib2" || metautils::
          args.data_format == "netcdf" || metautils::args.data_format ==
          "hdf5nc4") {
        if (strutils::occurs(k, ":") == 2) {
          ofs << "    <layer map=\"";
          auto idx = sp[0].find(",");
          if (idx != string::npos) {
            ofs << sp[0].substr(0, idx) << "\" type=\"" << sp[0].substr(idx +
                1);
          } else {
            ofs << sp[0];
          }
          ofs << "\" bottom=\"" << sp[2] << "\" top=\"" << sp[1] << "\">" <<
              endl;
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            auto idx = code.find(":");
            ofs << "      <parameter map=\"" << code.substr(0, idx) << "\" "
                "value=\"" << code.substr(idx + 1) << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </layer>" << endl;
          used.emplace_back(k);
        }
      } else if (metautils::args.data_format == "oct" || metautils::args.
          data_format == "tropical" || metautils::args.data_format == "ll" ||
          metautils::args.data_format == "navy" || metautils::args.data_format
          == "slp") {
        if (sp[0] == "1") {
          if (sp[1] == "1002" && sp[2] == "950") {
            ofs << "    <layer type=\"" << sp[1] << "\" bottom=\"0\" top=\"0\">"
                << endl;
          } else {
            ofs << "    <layer bottom=\"" << sp[2] << "\" top=\"" << sp[1] <<
                "\">" << endl;
          }
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            ofs << "      <parameter value=\"" << code << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </layer>" << endl;
          used.emplace_back(k);
        }
      } else if (metautils::args.data_format == "on84") {
        auto n = stoi(sp[0]);
        if ((n == 8 && sp.size() == 3) || (n >= 144 && n <= 148)) {
          ofs << "    <layer type=\"" << n << "\" bottom=\"" << sp[2] <<
               "\" top=\"" << sp[1] << "\">" << endl;
          for (const auto& e : le.parameter_code_table) {
            string code;
            ParameterEntry pe;
            tie(code, pe) = e;
            ofs << "      <parameter value=\"" << code << "\" start=\"" << pe.
                start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" <<
                pe.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\""
                << pe.num_time_steps << "\" />" << endl;
          }
          ofs << "    </layer>" << endl;
          used.emplace_back(k);
        }
      } else {
        ofs << k << endl;
      }
    }
    for (const auto& e : used) {
      ge.level_table.remove(e);
    }
    if (ge.level_table.size() > 0) {
      log_error2("level/layer table should be empty but table length is " +
          itos(ge.level_table.size()), F, caller, user);
    }
    ofs << "  </grid>" << endl;
  }
  ofs << "</GrML>" << endl;
  write_finalize(f, "GrML", t.name(), ofs, caller, user);
  return t.name();
}

} // end namespace gatherxml::markup::GrML

namespace ObML {

void copy(string metadata_file, string URL, string caller, string user) {
  static const string F = this_function_label(__func__);

  // create the directory tree in the temp directory
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  string s = "metadata/wfmd";
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + t.name() + "/" + s, oss, ess) != 0) {
    log_error2("unable to create a temporary directory - '" + ess.str() + "'",
        F, caller, user);
  }
  TempFile tf(metautils::directives.temp_path);
  rdadata_sync_from("/__HOST__/web/datasets/ds" + metautils::args.dsnum +
      "/metadata/fmd/" + metadata_file, tf.name(), metautils::directives.
      rdadata_home, ess);
  std::ifstream ifs(tf.name().c_str());
  if (!ifs.is_open()) {
    log_error2("unable to open input file", F, caller, user);
  }
  auto f = metautils::relative_web_filename(URL);
  replace_all(f, "/", "%");
  std::ofstream ofs((t.name() + "/" + s + "/" + f + ".ObML").c_str());
  if (!ofs.is_open()) {
    log_error2("unable to open output file", F, caller, user);
  }
  char l[32768];
  ifs.getline(l, 32768);
  while (!ifs.eof()) {
    string s = l;
    if (regex_search(s, regex("uri="))) {
      s = s.substr(s.find(" format"));
      s = "      uri=\"" + URL + "\"" + s;
    } else if (regex_search(s, regex("ref="))) {
      replace_all(s, metadata_file, f + ".ObML");
    }
    ofs << s << endl;
    ifs.getline(l, 32768);
  }
  ifs.close();
  ofs.close();
  string e;
  if (rdadata_sync(t.name(), s + "/", "/data/web/datasets/ds" + metautils::args.
      dsnum, metautils::directives.rdadata_home, e) < 0) {
    log_warning("copy(): unable to sync '" + f + ".ObML' - rdadata_sync "
        "error(s): '" + e + "'", caller, user);
  }
  copy_ancillary_files("ObML", metadata_file, f, caller, user);
  metautils::args.filename = f;
}

void write(ObservationData& obs_data, string caller, string user) {
  static const string F = "ObML::" + this_function_label(__func__);

  struct PlatformData {
    PlatformData() : nsteps(0), data_types_table(), start(), end() { }

    size_t nsteps;
    my::map<DataTypeEntry> data_types_table;
    DateTime start, end;
  } pd;

  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  string f;
  std::ofstream ofs;
  write_initialize(f, "ObML", t.name(), ofs, caller, user);
  ofs << "<ObML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" <<
      endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << endl;
  ofs << "                           https://rda.ucar.edu/schemas/ObML.xsd\"" <<
      endl;
  ofs << "       uri=\"file://web:" << metautils::relative_web_filename(
      metautils::args.path + "/" + metautils::args.filename);
  ofs << "\" format=\"";
  if (metautils::args.data_format == "on29" || metautils::args.data_format ==
      "on124") {
    ofs << "NCEP_" << to_upper(metautils::args.data_format);
  } else if (regex_search(metautils::args.data_format, regex("bufr"))) {
    ofs << "WMO_BUFR";
  } else if (metautils::args.data_format == "cpcsumm" || metautils::args.
      data_format == "ghcnmv3" || metautils::args.data_format == "hcn" ||
      metautils::args.data_format == "uadb") {
    ofs << "proprietary_ASCII";
  } else if (metautils::args.data_format == "imma") {
    ofs << "NOAA_IMMA";
  } else if (metautils::args.data_format == "isd") {
    ofs << "NCDC_ISD";
  } else if (metautils::args.data_format == "netcdf") {
    ofs << "netCDF";
  } else if (metautils::args.data_format == "nodcbt") {
    ofs << "NODC_BT";
  } else if (regex_search(metautils::args.data_format, regex("^td32"))) {
    ofs << "NCDC_" << to_upper(metautils::args.data_format);
  } else if (metautils::args.data_format == "tsr") {
    ofs << "DSS_TSR";
  } else if (metautils::args.data_format == "wmssc") {
    ofs << "DSS_WMSSC";
  } else if (metautils::args.data_format == "hdf5nc4") {
    ofs << "netCDF4";
  } else if (metautils::args.data_format == "hdf5") {
    ofs << "HDF5";
  } else if (metautils::args.data_format == "little_r") {
    ofs << "LITTLE_R";
  } else {
    ofs << metautils::args.data_format;
  }
  ofs << "\">" << endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %T %Z") << "\" />" << endl;
  string map;
  if (metautils::args.data_format == "cpcsumm" || metautils::args.data_format ==
      "netcdf" || metautils::args.data_format == "hdf5" || metautils::args.
      data_format == "ghcnmv3" || metautils::args.data_format == "hcn" ||
      metautils::args.data_format == "proprietary_ASCII" || metautils::args.
      data_format == "proprietary_Binary") {
    map = "ds" + metautils::args.dsnum;
  }
  string s = "metadata/wfmd";
  for (size_t xx=0; xx < obs_data.num_types; ++xx) {
    obs_data.id_tables[xx]->keysort(
    [](const string& left, const string& right) -> bool {
      if (left <= right) {
        return true;
      }
      return false;
    });
    if (obs_data.platform_tables[xx]->size() > 0) {
      ofs << "  <observationType value=\"" << obs_data.observation_types[xx] <<
          "\">" << endl;
      for (const auto& k : obs_data.platform_tables[xx]->keys()) {
        PlatformEntry pe;
        obs_data.platform_tables[xx]->found(k, pe);
        std::ofstream ofs2((t.name() + "/" + s + "/" + f + ".ObML." +
            obs_data.observation_types[xx] + "." + k + ".IDs.xml").c_str());
        ofs2 << "<?xml version=\"1.0\" ?>" << endl;
        ofs2 << "<IDs parent=\"" << f << ".ObML\" group=\"" << obs_data.
            observation_types[xx] << "." << k << "\">" << endl;
        auto n = 0;
        pd.nsteps = 0;
        pd.start.set(9999, 12, 31, 235959);
        pd.end.set(1, 1, 1, 0);
        pd.data_types_table.clear();
        for (const auto& k2 : obs_data.id_tables[xx]->keys()) {
          if (strutils::has_beginning(k2, k)) {
            auto sp = split(k2, "[!]");
            if (sp.size() < 3) {
              myerror = "Error parsing ID: " + k2;
              exit(1);
            }
            ofs2 << "  <ID type=\"" << sp[1] << "\" value=\"" << sp[2] << "\"";
            IDEntry ie;
            obs_data.id_tables[xx]->found(k2, ie);
            if (floatutils::myequalf(ie.data->S_lat, ie.data->N_lat) &&
                floatutils::myequalf(ie.data->W_lon, ie.data->E_lon)) {
              ofs2 << " lat=\"" << ftos(ie.data->S_lat, 4) << "\" lon=\"" <<
                  ftos(ie.data->W_lon, 4) << "\"";
            } else {
              size_t w, e;
              bitmap::longitudeBitmap::west_east_bounds(ie.data->min_lon_bitmap.
                  get(), w,  e);
              ofs2 << " cornerSW=\"" << ftos(ie.data->S_lat, 4) << "," << ftos(
                  ie.data->min_lon_bitmap[w], 4) << "\" cornerNE=\"" << ftos(ie.
                  data->N_lat, 4) << "," << ftos(ie.data->max_lon_bitmap[e], 4)
                  << "\"";
            }
            ofs2 << " start=\"";
            auto time = ie.data->start.time();
            auto hr = time / 10000;
            auto min = (time - hr * 10000) / 100;
            auto sec = time % 100;
            if (sec != 99) {
              ofs2 << ie.data->start.to_string("%Y-%m-%d %H:%MM:%SS %Z");
            } else if (min != 99) {
              ofs2 << ie.data->start.to_string("%Y-%m-%d %H:%MM %Z");
            } else if (hr != 99) {
              ofs2 << ie.data->start.to_string("%Y-%m-%d %H %Z");
            } else {
              ofs2 << ie.data->start.to_string("%Y-%m-%d");
            }
            ofs2 << "\" end=\"";
            time = ie.data->end.time();
            hr = time / 10000;
            min = (time - hr * 10000) / 100;
            sec = time % 100;
            if (sec != 99) {
              ofs2 << ie.data->end.to_string("%Y-%m-%d %H:%MM:%SS %Z");
            } else if (min != 99) {
              ofs2 << ie.data->end.to_string("%Y-%m-%d %H:%MM %Z");
            } else if (hr != 99) {
              ofs2 << ie.data->end.to_string("%Y-%m-%d %H %Z");
            } else {
              ofs2 << ie.data->end.to_string("%Y-%m-%d");
            }
            ofs2 << "\" numObs=\"" << ie.data->nsteps << "\">" << endl;
            if (ie.data->start < pd.start) {
              pd.start = ie.data->start;
            }
            if (ie.data->end > pd.end) {
              pd.end = ie.data->end;
            }
            pd.nsteps += ie.data->nsteps;
            for (const auto& data_type : ie.data->data_types_table.keys()) {
              DataTypeEntry de;
              ie.data->data_types_table.found(data_type, de);
              if (de.data->nsteps > 0) {
                ofs2 << "    <dataType";
                if (!map.empty()) {
                  ofs2 << " map=\"" << map << "\"";
                } else if (strutils::contains(metautils::args.data_format,
                    "bufr")) {
                  ofs2 << " map=\"" << de.data->map << "\"";
                }
                ofs2 << " value=\"" << de.key << "\" numObs=\"" << de.data->
                    nsteps << "\"";
                if (de.data->vdata == nullptr || de.data->vdata->res_cnt == 0) {
                  ofs2 << " />" << endl;
                } else {
                  ofs2 << ">" << endl;
                  ofs2 << "      <vertical min_altitude=\"" << de.data->vdata->
                      min_altitude << "\" max_altitude=\"" << de.data->vdata->
                      max_altitude << "\" vunits=\"" << de.data->vdata->units <<
                      "\" avg_nlev=\"" << de.data->vdata->avg_nlev / de.data->
                      nsteps << "\" avg_vres=\"" << de.data->vdata->avg_res /
                      de.data->vdata->res_cnt << "\" />" << endl;
                  ofs2 << "    </dataType>" << endl;
                }
                DataTypeEntry de2;
                if (!pd.data_types_table.found(de.key, de2)) {
                  de2.key = de.key;
                  de2.data.reset(new DataTypeEntry::Data);
                  pd.data_types_table.insert(de2);
                }
                if (regex_search(metautils::args.data_format, regex("bufr"))) {
                  de2.data->map = de.data->map;
                }
                de2.data->nsteps += de.data->nsteps;
                if (de2.data->vdata != nullptr && de2.data->vdata->res_cnt >
                    0) {
                  de2.data->vdata->avg_res += de.data->vdata->avg_res;
                  de2.data->vdata->res_cnt += de.data->vdata->res_cnt;
                  if (de.data->vdata->min_altitude < de2.data->vdata->
                      min_altitude) {
                    de2.data->vdata->min_altitude = de.data->vdata->
                        min_altitude;
                  }
                  if (de.data->vdata->max_altitude > de2.data->vdata->
                      max_altitude) {
                    de2.data->vdata->max_altitude = de.data->vdata->
                        max_altitude;
                  }
                  de2.data->vdata->avg_nlev += de.data->vdata->avg_nlev;
                  de2.data->vdata->units = de.data->vdata->units;
                }
              }
            }
            ofs2 << "  </ID>" << endl;
            ++n;
          }
        }
        ofs2 << "</IDs>" << endl;
        ofs2.close();
        ofs << "    <platform type=\"" << k << "\" numObs=\"" << pd.nsteps <<
            "\">" << endl;
        ofs << "      <IDs ref=\"" << f << ".ObML." << obs_data.
            observation_types[xx] << "." << k << ".IDs.xml\" numIDs=\"" << n <<
            "\" />" << endl;
        ofs << "      <temporal start=\"" << pd.start.to_string("%Y-%m-%d") <<
            "\" end=\"" << pd.end.to_string("%Y-%m-%d") << "\" />" << endl;
        ofs << "      <locations ref=\"" << f << ".ObML." << obs_data.
            observation_types[xx] << "." << k << ".locations.xml\" />" << endl;
        ofs2.open((t.name() + "/" + s + "/" + f + ".ObML." + obs_data.
            observation_types[xx] + "." + k + ".locations.xml").c_str());
        ofs2 << "<?xml version=\"1.0\" ?>" << endl;
        ofs2 << "<locations parent=\"" << f << ".ObML\" group=\"" << obs_data.
            observation_types[xx] << "." << k << "\">" << endl;
        if (pe.boxflags->spole == 1) {
          ofs2 << "  <box1d row=\"0\" bitmap=\"0\" />" << endl;
        }
        for (size_t n = 0; n < 180; ++n) {
          if (pe.boxflags->flags[n][360] == 1) {
            ofs2 << "  <box1d row=\"" << n + 1 << "\" bitmap=\"";
            for (size_t m = 0; m < 360; ++m) {
              ofs2 << static_cast<int>(pe.boxflags->flags[n][m]);
            }
            ofs2 << "\" />" << endl;
          }
        }
        if (pe.boxflags->npole == 1) {
          ofs2 << "  <box1d row=\"181\" bitmap=\"0\" />" << endl;
        }
        ofs2 << "</locations>" << endl;
        ofs2.close();
        for (const auto& k2 : pd.data_types_table.keys()) {
          DataTypeEntry de;
          pd.data_types_table.found(k2, de);
          ofs << "      <dataType";
          if (!map.empty()) {
            ofs << " map=\"" << map << "\"";
          } else if (regex_search(metautils::args.data_format, regex("bufr"))) {
            ofs << " map=\"" << de.data->map << "\"";
          }
          ofs << " value=\"" << de.key << "\" numObs=\"" << de.data->nsteps <<
              "\"";
          if (de.data->vdata == nullptr || de.data->vdata->res_cnt == 0) {
            ofs << " />" << endl;
          } else {
            ofs << ">" << endl;
            ofs << "        <vertical min_altitude=\"" << de.data->vdata->
                min_altitude << "\" max_altitude=\"" << de.data->vdata->
                max_altitude << "\" vunits=\"" << de.data->vdata->units << "\""
                "avg_nlev=\"" << de.data->vdata->avg_nlev/de.data->nsteps <<
                "\"avg_vres=\"" << de.data->vdata->avg_res/de.data->vdata->
                res_cnt << "\" />" << endl;
            ofs << "      </dataType>" << endl;
          }
        }
        ofs << "    </platform>" << endl;
      }
      ofs << "  </observationType>" << endl;
    }
  }
  ofs << "</ObML>" << endl;
  write_finalize(f, "ObML", t.name(), ofs, caller, user);
}

} // end namespace gatherxml::markup::ObML

namespace SatML {

void write(my::map<ScanLineEntry>& scan_line_table, std::list<string>&
    scan_line_table_keys, my::map<ImageEntry>& image_table, std::list<string>&
    image_table_keys, string caller, string user) {
  static const string F = "SatML::" + this_function_label(__func__);
  TempDir t;
  if (!t.create(metautils::directives.temp_path)) {
    log_error2("unable to create temporary directory", F, caller, user);
  }
  string f;
  std::ofstream ofs;
  write_initialize(f, "SatML", t.name(), ofs, caller, user);
  ofs << "<SatML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" <<
      endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << endl;
  ofs << "                           https://rda.ucar.edu/schemas/SatML.xsd\""
      << endl;
  ofs << "       uri=\"" << metautils::args.path << "/" << metautils::args.
      filename << "\" format=\"";
  if (metautils::args.data_format == "noaapod") {
    ofs << "NOAA_Polar_Orbiter";
  } else if (metautils::args.data_format == "mcidas") {
    ofs << "McIDAS";
  }
  ofs << "\">" << endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string(
      "%Y-%m-%d %T %Z") << "\" />" << endl;
  string metadata_path="metadata/wfmd";
  if (metautils::args.data_format == "noaapod") {
    for (const auto& k : scan_line_table_keys) {
      ScanLineEntry se;
      scan_line_table.found(k, se);
      ofs << "  <satellite ID=\"" << se.sat_ID << "\">" << endl;
      double a = 0.;
      double d = 0.;
      for (const auto& e : se.scan_line_list) {
        a += e.width;
        d += e.res;
      }
      ofs << "    <swath units=\"km\" avgWidth=\"" << round(a/se.scan_line_list.
          size()) << "\" res=\"" << round(d/se.scan_line_list.size()) << "\">"
          << endl;
      ofs << "      <data type=\"" << k << "\">" << endl;
      ofs << "        <temporal start=\"" << se.start_date_time.to_string(
          "%Y-%m-%d %T %Z") << "\" end=\"" << se.end_date_time.to_string(
          "%Y-%m-%d %T %Z") << "\" />" << endl;
      if (se.sat_ID == "NOAA-11") {
        ofs << "        <spectralBand units=\"um\" value=\"0.58-0.68\" />" <<
            endl;
        ofs << "        <spectralBand units=\"um\" value=\"0.725-1.1\" />" <<
            endl;
        ofs << "        <spectralBand units=\"um\" value=\"3.55-3.93\" />" <<
            endl;
        ofs << "        <spectralBand units=\"um\" value=\"10.3-11.3\" />" <<
            endl;
        ofs << "        <spectralBand units=\"um\" value=\"11.5-12.5\" />" <<
            endl;
      }
      ofs << "        <scanLines ref=\"" << metautils::args.filename << "." << k
          << ".scanLines\" num=\"" << se.scan_line_list.size() << "\" />" <<
          endl;
      std::ofstream ofs2((t.name() + "/" + metadata_path + "/" + metautils::
          args.filename + "." + k + ".scanLines").c_str());
      ofs2 << "<?xml version=\"1.0\" ?>" << endl;
      ofs2 << "<scanLines parent=\"" << metautils::args.filename << ".SatML\">"
          << endl;
      for (const auto& e : se.scan_line_list) {
        ofs2 << "  <scanLine time=\"" << e.date_time.to_string("%Y-%m-%d %T %Z")
            << "\" numPoints=\"409\" width=\"" << round(e.width) << "\" res=\""
            << round(e.res) << "\">" << endl;
        ofs2 << "    <start lat=\"" << round(e.first_coordinate.lat*100.)/100.
            << "\" lon=\"" << round(e.first_coordinate.lon*100.)/100. << "\" />"
            << endl;
        ofs2 << "    <subpoint lat=\"" << round(e.subpoint.lat*100.)/100. <<
            "\" lon=\"" << round(e.subpoint.lon*100.)/100. << "\" />" << endl;
        ofs2 << "    <end lat=\"" << round(e.last_coordinate.lat*100.)/100. <<
            "\" lon=\"" << round(e.last_coordinate.lon*100.)/100. << "\" />" <<
            endl;
        ofs2 << "  </scanLine>" << endl;
      }
      ofs2 << "</scanLines>" << endl;
      ofs2.close();
      ofs << "      </data>" << endl;
      ofs << "    </swath>" << endl;
      ofs << "  </satellite>" << endl;
    }
  } else if (metautils::args.data_format == "mcidas") {
    for (const auto& k : image_table_keys) {
      ImageEntry ie;
      image_table.found(k, ie);
      ofs << "  <satellite ID=\"" << ie.key << "\">" << endl;
      ofs << "    <temporal start=\"" << ie.start_date_time.to_string(
          "%Y-%m-%d %T %Z") << "\" end=\"" << ie.end_date_time.to_string(
          "%Y-%m-%d %T %Z") << "\" />" << endl;
      ofs << "    <images ref=\"" << metautils::args.filename << ".images\" "
          "num=\"" << ie.image_list.size() << "\" />" << endl;
      std::ofstream ofs2((t.name() + "/" + metadata_path + "/" + metautils::
          args.filename + ".images").c_str());
      ofs2 << "<?xml version=\"1.0\" ?>" << endl;
      ofs2 << "<images parent=\"" << metautils::args.filename << ".SatML\">" <<
          endl;
      for (const auto& e : ie.image_list) {
        ofs2 << "  <image>" << endl;
        ofs2 << "    <time value=\"" << e.date_time.to_string("%Y-%m-%d %T %Z")
            << "\" />" << endl;
        ofs2 << "    <resolution units=\"km\" x=\"" << round(e.xres) <<
            "\" y=\"" << round(e.yres) << "\" />" << endl;
        if (ie.key == "GMS Visible") {
          ofs2 << "    <spectralBand units=\"um\" value=\"0.55-1.05\" />" <<
              endl;
        } else if (ie.key == "GMS Infrared") {
          ofs2 << "    <spectralBand units=\"um\" value=\"10.5-12.5\" />" <<
              endl;
        }
        ofs2 << "    <centerPoint lat=\"" << round(e.center.lat*100.)/100. <<
            "\" lon=\"" << round(e.center.lon*100.)/100. << "\" />" << endl;
        if (e.corners.nw_coord.lat < -99. && e.corners.nw_coord.lon < -999. &&
            e.corners.ne_coord.lat < -99. && e.corners.ne_coord.lon < -999. &&
            e.corners.sw_coord.lat < -99. && e.corners.sw_coord.lon < -999. &&
            e.corners.se_coord.lat < -99. && e.corners.se_coord.lon < -999.) {
          ofs2 << "    <fullDiskImage />" << endl;
        } else {
          ofs2 << "    <northwestCorner lat=\"" << round(e.corners.nw_coord.
              lat * 100.) / 100. << "\" lon=\"" << round(e.corners.nw_coord.
              lon * 100.) / 100. << "\" />" << endl;
          ofs2 << "    <southwestCorner lat=\"" << round(e.corners.sw_coord.
              lat * 100.) / 100. << "\" lon=\"" << round(e.corners.sw_coord.
              lon * 100.) / 100. << "\" />" << endl;
          ofs2 << "    <southeastCorner lat=\"" << round(e.corners.se_coord.
              lat * 100.) / 100. << "\" lon=\"" << round(e.corners.se_coord.
              lon * 100.) / 100. << "\" />" << endl;
          ofs2 << "    <northeastCorner lat=\"" << round(e.corners.ne_coord.
              lat * 100.) / 100. << "\" lon=\"" << round(e.corners.ne_coord.
              lon * 100.) / 100. << "\" />" << endl;
        }
        ofs2 << "  </image>" << endl;
      }
      ofs2 << "</images>" << endl;
      ofs2.close();
      ofs << "  </satellite>" << endl;
    }
  }
  ofs << "</SatML>" << endl;
  write_finalize(f, "SatML", t.name(), ofs, caller, user);
}

} // end namespace gatherxml::markup::SatML

} // end namespace gatherxml::markup

} // end namespace gatherxml
