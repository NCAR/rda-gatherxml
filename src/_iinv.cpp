#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <vector>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <gridutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <metahelpers.hpp>
#include <tempfile.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::list;
using std::move;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::chop;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::strand;
using strutils::substitute;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

struct LocalArgs {
  LocalArgs() : dsnum2(), temp_directory(), create_cache(true), notify(false),
      verbose(false), wms_only(false) { }

  string dsnum2;
  string temp_directory;
  bool create_cache, notify, verbose;
  bool wms_only;
} local_args;

struct StringEntry {
  StringEntry() : key() { }

  string key;
};

struct InventoryEntry {
  InventoryEntry() : key(), m_list(nullptr) { }

  string key;
  shared_ptr<list<string>> m_list;
};

struct AncillaryEntry {
  AncillaryEntry() : key(), code(nullptr) { }

  string key;
  shared_ptr<string> code;
};

struct TimeRangeEntry {
  TimeRangeEntry() : code(), hour_diff(0x7fffffff) { }

  string code;
  int hour_diff;
};

struct DataVariableEntry {
  struct Data {
    Data() : var_name(), value_type(), offset(0), byte_len(0), missing_table()
        { }

    string var_name, value_type;
    size_t offset, byte_len;
    unordered_set<string> missing_table;
  };
  DataVariableEntry() : key(), data(nullptr) { }

  string key;
  shared_ptr<Data> data;
};

struct MissingEntry {
  MissingEntry() : key(), time_code(0) { }

  string key;
  size_t time_code;
};

struct InsertEntry {
  struct Data {
    Data() : inv_insert(), num_inserts(0) {
      inv_insert.reserve(800000);
    }

    string inv_insert;
    int num_inserts;
  };
  InsertEntry() : key(), data(nullptr) { }

  string key;
  shared_ptr<Data> data;
};

struct InitTimeEntry {
  InitTimeEntry() : key(), time_range_codes(nullptr) { }

  string key;
  shared_ptr<my::map<StringEntry>> time_range_codes;
};

string tindex;
TempDir temp_dir;

void parse_args(int argc, char **argv) {
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '`');
  auto sp = split(metautils::args.args_string, "`");
  for (size_t n = 0; n < sp.size(); ++n) {
    if (sp[n] == "-d") {
      metautils::args.dsnum = sp[++n];
      if (regex_search(metautils::args.dsnum, regex("^ds"))) {
        metautils::args.dsnum = metautils::args.dsnum.substr(2);
      }
      local_args.dsnum2 = substitute(metautils::args.dsnum, ".", "");
    } else if (sp[n] == "-f") {
      metautils::args.filename = sp[++n];
    } else if (sp[n] == "-C") {
      local_args.create_cache = false;
    } else if (sp[n] == "-N") {
      local_args.notify = true;
    } else if (sp[n] == "-t") {
      local_args.temp_directory = sp[++n];
    } else if (sp[n] == "-V") {
      local_args.verbose = true;
    } else if (sp[n] == "--wms-only") {
      local_args.wms_only = true;
      local_args.create_cache = false;
    }
  }
}

extern "C" void segv_handler(int) {
  log_error2("Error: core dump", this_function_label(__func__), "iinv", USER);
}

string grid_definition_parameters(const XMLElement& e) {
  string s;
  auto d = e.attribute_value("definition");
  if (d == "latLon") {
    s = e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" + e
        .attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("endLat") + ":" + e.attribute_value("endLon") +
        ":" + e.attribute_value("xRes") + ":" + e.attribute_value("yRes");
  } else if (d == "gaussLatLon") {
    s = e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" + e
        .attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("endLat") + ":" + e.attribute_value("endLon") +
        ":" + e.attribute_value("xRes") + ":" + e.attribute_value("circles");
  } else if (d == "polarStereographic") {
    s = e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" + e
        .attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("resLat") + ":" + e.attribute_value("projLon") +
        ":" + e.attribute_value("pole") + ":" + e.attribute_value("xRes") +
        ":" + e.attribute_value("yRes");
  } else if (d == "lambertConformal") {
    s = e.attribute_value("numX") + ":" + e.attribute_value("numY") + ":" + e
        .attribute_value("startLat") + ":" + e.attribute_value("startLon") +
        ":" + e.attribute_value("resLat") + ":" + e.attribute_value("projLon") +
        ":" + e.attribute_value("pole") + ":" + e.attribute_value("xRes") +
        ":" + e.attribute_value("yRes") + ":" + e.attribute_value(
        "stdParallel1") + ":" + e.attribute_value("stdParallel2");
  }
  return move(s);
}

void build_wms_capabilities() {
  static const string F = this_function_label(__func__);
  string res = "https://rda.ucar.edu/datasets/ds" + metautils::args.dsnum +
      "/metadata/wfmd/" + substitute(metautils::args.filename, "_inv", "");
  auto f = unixutils::remote_web_file(res + ".gz", temp_dir.name());
  struct stat buf;
  if (stat(f.c_str(), &buf) == 0) {
    system(("gunzip " + f).c_str());
    chop(f, 3);
  }
  XMLDocument xdoc;
  if (!xdoc.open(f)) {
    f = unixutils::remote_web_file(res, temp_dir.name());
    if (!xdoc.open(f)) {
        log_error2("unable to open " + res, F, "iinv", USER);
    }
  }
  auto *tdir = new TempDir;
  if (!tdir->create(metautils::directives.temp_path)) {
    log_error2("build_wms_capabilities() could not create a temporary "
        "directory", F, "iinv", USER);
  }
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + tdir->name() + "/metadata/wms", oss, ess) <
      0) {
    log_error2("build_wms_capabilities() could not create the directory tree",
        F, "iinv", USER);
  }
  MySQL::Server server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!server) {
    log_error2("build_wms_capabilities() could not connect to the metadata "
        "database", F, "iinv", USER);
  }
  xmlutils::LevelMapper lmapper("/glade/u/home/rdadata/share/metadata/"
      "LevelTables");
  xmlutils::ParameterMapper pmapper("/glade/u/home/rdadata/share/metadata/"
      "ParameterTables");
  auto gfil = xdoc.element("GrML").attribute_value("uri") + ".GrML";
  if (!regex_search(gfil, regex("^http(s){0,1}://rda.ucar.edu/"))) {
    metautils::log_warning("build_wms_capabilities() found an invalid uri: " +
        gfil, "iinv", USER);
    return;
  }
  gfil = gfil.substr(gfil.find("rda.ucar.edu") + 12);
  auto wh = metautils::web_home();
  replace_all(wh, metautils::directives.data_root, metautils::directives
      .data_root_alias);
  replace_all(gfil, wh + "/", "");
  replace_all(gfil, "/", "%");
  std::ofstream ofs((tdir->name() + "/metadata/wms/" + gfil).c_str());
  if (!ofs.is_open()) {
    log_error2("build_wms_capabilities() could not open the output file", F,
        "iinv", USER);
  }
  ofs.setf(std::ios::fixed);
  ofs.precision(4);
  auto dfmt = xdoc.element("GrML").attribute_value("format");
  MySQL::LocalQuery q("code", "WGrML.formats", "format = '" + dfmt + "'");
  string fcod;
  MySQL::Row row;
  if (q.submit(server) == 0 && q.fetch_row(row)) {
    fcod = row[0];
  } else {
    log_error2("build_wms_capabilities(): query '" + q.show() + "' failed", F,
        "iinv", USER);
  }
  string e;
  auto t = table_names(server, "IGrML", "%ds%" + local_args.dsnum2 +
      "_inventory_" + fcod + "!%", e);
  if (t.size() == 0) {
    return;
  }
  auto dfil = gfil.substr(0, gfil.rfind("."));
  replace_all(dfil, "%", "/");
  auto glst = xdoc.element_list("GrML/grid");
  auto gcnt = 0;
  string last = "-1";
  for (const auto& g : glst) {
    auto p = grid_definition_parameters(g);
    q.set("code", "WGrML.gridDefinitions", "definition = '" + g.attribute_value(
        "definition") + "' and defParams = '" + p + "'");
    string gcod;
    if (q.submit(server) == 0 && q.fetch_row(row)) {
      gcod = row[0];
    } else {
      log_error2("build_wms_capabilities(): query '" + q.show() + "' failed", F,
          "iinv", USER);
    }
    if (gcod != last) {
      double w, s, e, n;
      if (!gridutils::fill_spatial_domain_from_grid_definition(g
          .attribute_value("definition") + "<!>" + p, "primeMeridian", w, s, e,
          n)) {
        metautils::log_info("build_wms_capabilities() could not get the "
            "spatial domain from '" + gfil + "'", "iinv", USER);
        return;
      }
      if (last != "-1") {
        ofs << "    </Layer>" << endl;
      }
      ofs << "    <Layer>" << endl;
      ofs << "      <Title>" << g.attribute_value("definition") << "_" << g
          .attribute_value("numX") << "x" << g.attribute_value("numY") <<
          "</Title>" << endl;
      ofs << "#REPEAT __CRS__" << gcnt << "__" << endl;
      ofs << "      <CRS>__CRS__" << gcnt << "__.CRS</CRS>" << endl;
      ofs << "#ENDREPEAT __CRS__" << gcnt << "__" << endl;
      ofs << "      <EX_GeographicBoundingBox>" << endl;
      ofs << "        <westBoundLongitude>" << w << "</westBoundLongitude>" <<
          endl;
      ofs << "        <eastBoundLongitude>" << e << "</eastBoundLongitude>" <<
          endl;
      ofs << "        <southBoundLatitude>" << s << "</southBoundLatitude>" <<
          endl;
      ofs << "        <northBoundLatitude>" << n << "</northBoundLatitude>" <<
          endl;
      ofs << "      </EX_GeographicBoundingBox>" << endl;;
      ofs << "#REPEAT __CRS__" << gcnt << "__" << endl;
      ofs << "      <BoundingBox CRS=\"__CRS__" << gcnt << "__.CRS\" minx="
          "\"__CRS__" << gcnt << "__." << w << "\" miny=\"__CRS__" << gcnt <<
          "__." << s << "\" maxx=\"__CRS__" << gcnt << "__." << e << "\" maxy="
          "\"__CRS__" << gcnt << "__." << n << "\" />" << endl;
      ofs << "#ENDREPEAT __CRS__" << gcnt << "__" << endl;
      last = gcod;
    }
    q.set("code", "WGrML.timeRanges", "timeRange = '" + g.attribute_value(
        "timeRange") + "'");
    string tcod;
    if (q.submit(server) == 0 && q.fetch_row(row)) {
      tcod = row[0];
    } else {
      log_error2("build_wms_capabilities(): query '" + q.show() + "' failed", F,
          "iinv", USER);
    }
    ofs << "      <Layer>" << endl;
    ofs << "        <Title>" << g.attribute_value("timeRange") << "</Title>" <<
        endl;
    auto llst = g.element_list("level");
    for (const auto& l : llst) {
      auto map = l.attribute_value("map");
      auto typ = l.attribute_value("type");
      auto val = l.attribute_value("value");
      q.set("code", "WGrML.levels", "map = '" + map + "' and type = '" + typ +
          "' and value = '" + val + "'");
      string level_code;
      if (q.submit(server) == 0 && q.fetch_row(row)) {
          level_code=row[0];
      } else {
          log_error2("build_wms_capabilities(): query '" + q.show() +
              "' failed", F, "iinv", USER);
      }
      auto ti = lmapper.description(dfmt, typ, map);
      if (ti.empty()) {
          ti = l.attribute_value("type") + ":" + l.attribute_value("value");
      } else if (l.attribute_value("value") != "0") {
          ti += ": " + l.attribute_value("value") + lmapper.units(dfmt,typ,map);
      }
      ofs << "        <Layer>" << endl;
      ofs << "          <Title>" << ti << "</Title>" << endl;
      auto plst = l.element_list("parameter");
      for (const auto& p : plst) {
          auto pcod = p.attribute_value("map") + ":" + p.attribute_value(
              "value");
          ofs << "          <Layer>" << endl;
          ofs << "            <Title>" << pmapper.description(dfmt, pcod) <<
              "</Title>" << endl;
          q.set("select distinct valid_date from IGrML.`ds" + local_args
              .dsnum2 + "_inventory_" + fcod + "!" + pcod + "` as i left join "
              "WGrML.ds" + local_args.dsnum2 + "_webfiles2 as w on w.code = "
              "i.webID_code where timeRange_code = " + tcod + " and "
              "gridDefinition_code = " + gcod + " and level_code = " +
              level_code + " and webID = '" + dfil + "' order by valid_date");
          if (q.submit(server) == 0) {
            for (const auto& r : q) {
              ofs << "            <Layer queryable=\"0\">" << endl;
              auto nam = gcod + ";" + tcod + ";" + level_code + ";" + fcod +
                  "!" + pcod + ";" + r[0];
              ofs << "              <Name>" << nam << "</Name>" << endl;
              ofs << "              <Title>" << r[0].substr(0,4) << "-" << r[0]
                  .substr(4,2) << "-" << r[0].substr(6,2) << "T" << r[0]
                  .substr(8,2) << ":" << r[0].substr(10,2) << "Z</Title>" <<
                  endl;
              ofs << "              <Style>" << endl;
              ofs << "                <Name>Legend</Name>" << endl;
              ofs << "                <Title>Legend Graphic</Title>" << endl;
              ofs << "                <LegendURL>" << endl;
              ofs << "                  <Format>image/png</Format>" << endl;
              ofs << "                  <OnlineResource xlink:type=\"simple\" "
                  "xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << nam <<
                  "\" />" << endl;
              ofs << "                </LegendURL>" << endl;
              ofs << "              </Style>" << endl;
              ofs << "            </Layer>" << endl;
            }
          } else {
            log_error2("build_wms_capabilities(): query '" + q.show() +
                "' failed", F, "iinv", USER);
          }
          ofs << "          </Layer>" << endl;
      }
      ofs << "        </Layer>" << endl;
    }
    auto vlst = g.element_list("layer");
    for (const auto& l : vlst) {
      auto map = l.attribute_value("map");
      auto typ = l.attribute_value("type");
      auto bot = l.attribute_value("bottom");
      auto top = l.attribute_value("top");
      q.set("code", "WGrML.levels", "map = '" + map + "' and type = '" + typ +
          "' and value = '" + bot + ", " + top + "'");
      string lcod;
      if (q.submit(server) == 0 && q.fetch_row(row)) {
        lcod=row[0];
      } else {
        log_error2("build_wms_capabilities(): query '" + q.show() + "' failed",
            F, "iinv", USER);
      }
      auto ti=lmapper.description(dfmt, typ, map);
      if (ti.empty()) {
        ti = l.attribute_value("type") + ":" + l.attribute_value("value");
      } else if (l.attribute_value("value") != "0") {
        auto sp = split(typ,  "-");
        if (sp.size() == 1) {
          sp.emplace_back(sp.front());
        }
        ti += ": " + l.attribute_value("bottom") + lmapper.units(dfmt, sp[0],
            map) + ", " + l.attribute_value("top") + lmapper.units(dfmt, sp[1],
            map);
      }
      ofs << "        <Layer>" << endl;
      ofs << "          <Title>" << ti << "</Title>" << endl;
      auto plst = l.element_list("parameter");
      for (const auto& p : plst) {
          auto pcod = p.attribute_value("map") + ":" + p.attribute_value(
              "value");
          ofs << "          <Layer>" << endl;
          ofs << "            <Title>" << pmapper.description(dfmt, pcod) <<
              "</Title>" << endl;
          q.set("select distinct valid_date from IGrML.`ds" + local_args
              .dsnum2 + "_inventory_" + fcod + "!" + pcod + "` as i left join "
              "WGrML.ds" + local_args.dsnum2 + "_webfiles2 as w on w.code = "
              "i.webID_code where timeRange_code = " + tcod + " and "
              "gridDefinition_code = " + gcod + " and level_code = " + lcod +
              " and webID = '" + dfil + "' order by valid_date");
          if (q.submit(server) == 0) {
            for (const auto& r : q) {
              ofs << "            <Layer queryable=\"0\">" << endl;
              auto nam = gcod + ";" + tcod + ";" + lcod + ";" + fcod + "!" +
                  pcod + ";" + r[0];
              ofs << "              <Name>" << nam << "</Name>" << endl;
              ofs << "              <Title>" << r[0].substr(0,4) << "-" << r[0]
                  .substr(4,2) << "-" << r[0].substr(6,2) << "T" << r[0]
                  .substr(8,2) << ":" << r[0].substr(10,2) << "Z</Title>" <<
                  endl;
              ofs << "              <Style>" << endl;
              ofs << "                <Name>Legend</Name>" << endl;
              ofs << "                <Title>Legend Graphic</Title>" << endl;
              ofs << "                <LegendURL>" << endl;
              ofs << "                  <Format>image/png</Format>" << endl;
              ofs << "                  <OnlineResource xlink:type=\"simple\" "
                  "xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << nam <<
                  "\" />" << endl;
              ofs << "                </LegendURL>" << endl;
              ofs << "              </Style>" << endl;
              ofs << "            </Layer>" << endl;
            }
          } else {
            log_error2("build_wms_capabilities(): query '" + q.show() +
                "' failed", F, "iinv", USER);
          }
          ofs << "          </Layer>" << endl;
      }
      ofs << "        </Layer>" << endl;
    }
    ofs << "      </Layer>" << endl;
    ++gcnt;
  }
  ofs << "    </Layer>" << endl;
  ofs.close();
  mysystem2("/bin/sh -c 'gzip " + tdir->name() + "/metadata/wms/" + gfil + "'",
      oss, ess);
  if (unixutils::rdadata_sync(tdir->name(), "metadata/wms/", "/data/web/"
      "datasets/ds"+metautils::args.dsnum, metautils::directives.rdadata_home,
      e) < 0) {
    log_error2("build_wms_capabilities() could not sync the capabilities file "
        "for '" + gfil + "'", F, "iinv", USER);
  }
  delete tdir;
}

void insert_grml_inventory() {
  static const string F = this_function_label(__func__);
  MySQL::Server server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  std::ifstream ifs(local_args.temp_directory + "/" + metautils::args.filename
      .c_str());
  if (!ifs.is_open()) {
    log_error2("insert_grml_inventory() was not able to open " + metautils::
        args.filename, F, "iinv", USER);
  }
  auto wid = substitute(metautils::args.filename, ".GrML_inv", "");
  replace_all(wid, "%", "/");
  MySQL::LocalQuery q("select code,format_code,tindex from WGrML.ds" +
      local_args.dsnum2 + "_webfiles2 as w left join dssdb.wfile as x on "
      "(x.dsid = 'ds" + metautils::args.dsnum + "' and x.type = 'D' and "
      "x.wfile = w.webID) where webID = '" + wid + "'");
  if (q.submit(server) < 0) {
    log_error2("insert_grml_inventory() returned error: " + q.error() +
        " while looking for code from webfiles", F, "iinv", USER);
  }
  if (q.num_rows() == 0) {
    log_error2("insert_grml_inventory() did not find " + wid + " in WGrML.ds" +
        local_args.dsnum2 + "_webfiles2", F, "iinv", USER);
  }
  MySQL::Row row;
  q.fetch_row(row);
  auto wic = row[0];
  auto fc = row[1];
  tindex=row[2];
  if (!MySQL::table_exists(server,"IGrML.ds"+local_args.dsnum2+"_inventory_summary")) {
    string result;
    if (server.command("create table IGrML.ds"+local_args.dsnum2+"_inventory_summary like IGrML.template_inventory_summary",result) < 0) {
      log_error2("insert_grml_inventory() returned error: " + server.error() +
          " while trying to create inventory_summary table", F, "iinv", USER);
    }
  }
  my::map<InitTimeEntry> idates;
  string dup = "N";
  string uflg = strand(3);
  int nlin = 0, ndup = 0;
  long long tbyts = 0;
  vector<TimeRangeEntry> trmap;
  vector<string> glst, llst, plst, rlst, elst, pclst;
  char line[32768];
  ifs.getline(line, 32768);
  while (!ifs.eof()) {
    ++nlin;
    string s = line;
    if (regex_search(s, regex("<!>"))) {
      auto sp = split(s, "<!>");
      switch (sp[0][0]) {
        case 'U': {
          TimeRangeEntry tre;
          if (sp[2] == "Analysis" || regex_search(sp[2], regex("^0-hour")) ||
              sp[2] == "Monthly Mean") {
            tre.hour_diff = 0;
          } else if (regex_search(sp[2], regex("-hour Forecast$"))) {
            tre.hour_diff = stoi(sp[2].substr(0, sp[2].find("-")));
          } else if (regex_search(sp[2], regex("to initial\\+"))) {
            auto hr = sp[2].substr(sp[2].find("to initial+") + 11);
            chop(hr);
            tre.hour_diff = stoi(hr);
          } else {
            metautils::log_warning("insert_grml_inventory() does not recognize "
                "product '" + sp[2] + "'", "iinv", USER);
          }
          q.set("code", "WGrML.timeRanges", "timeRange = '" + sp[2] + "'");
          if (q.submit(server) < 0) {
            log_error2("insert_grml_inventory() returned error: " + q.error() +
                " while trying to get timeRange code", F, "iinv", USER);
          }
          if (q.num_rows() == 0) {
            log_error2("no timeRange code for '" + sp[2] + "'", F, "iinv",
                USER);
          }
          q.fetch_row(row);
          tre.code = row[0];
          trmap.emplace_back(tre);
          break;
        }
        case 'G': {
          auto sp_g = split(sp[2], ",");
          string d, dp;
          switch (stoi(sp_g[0])) {
            case static_cast<int>(Grid::Type::latitudeLongitude):
            case static_cast<int>(Grid::Type::gaussianLatitudeLongitude): {
              if (stoi(sp_g[0]) == static_cast<int>(Grid::Type::
                  latitudeLongitude)) {
                d = "latLon";
              } else if (stoi(sp_g[0]) == static_cast<int>(Grid::Type::
                  gaussianLatitudeLongitude)) {
                d = "gaussLatLon";
              }
              if (sp_g[0].back() == 'C') {
                d += "Cell";
              }
              dp = sp_g[1] + ":" + sp_g[2] + ":";
              if (sp_g[3][0] == '-') {
                dp += sp_g[3].substr(1) + "S:";
              } else {
                dp += sp_g[3] + "N:";
              }
              if (sp_g[4][0] == '-') {
                dp += sp_g[4].substr(1) + "W:";
              } else {
                dp += sp_g[4] + "E:";
              }
              if (sp_g[5][0] == '-') {
                dp += sp_g[5].substr(1) + "S:";
              } else {
                dp += sp_g[5] + "N:";
              }
              if (sp_g[6][0] == '-') {
                dp += sp_g[6].substr(1) + "W:";
              } else {
                dp += sp_g[6] + "E:";
              }
              dp += sp_g[7] + ":" + sp_g[8];
              break;
            }
            case static_cast<int>(Grid::Type::polarStereographic): {
              d = "polarStereographic";
              dp = sp_g[1] + ":" + sp_g[2] + ":";
              if (sp_g[3][0] == '-') {
                dp += sp_g[3].substr(1) + "S:";
              } else {
                dp += sp_g[3] + "N:";
              }
              if (sp_g[4][0] == '-') {
                dp += sp_g[4].substr(1) + "W:";
              } else {
                dp += sp_g[4] + "E:";
              }
              if (sp_g[5][0] == '-') {
                dp += sp_g[5].substr(1) + "S:";
              } else {
                dp += sp_g[5] + "N:";
              }
              if (sp_g[6][0] == '-') {
                dp += sp_g[6].substr(1) + "W:";
              } else {
                dp += sp_g[6] + "E:";
              }
              dp += sp_g[9] + ":" + sp_g[7] + ":" + sp_g[8];
              break;
            }
            case static_cast<int>(Grid::Type::mercator): {
              d = "mercator";
              dp=sp_g[1]+":"+sp_g[2]+":";
              if (sp_g[3][0] == '-') {
                dp+=sp_g[3].substr(1)+"S:";
              } else {
                dp+=sp_g[3]+"N:";
              }
              if (sp_g[4][0] == '-') {
                dp+=sp_g[4].substr(1)+"W:";
              } else {
                dp+=sp_g[4]+"E:";
              }
              if (sp_g[5][0] == '-') {
                dp+=sp_g[5].substr(1)+"S:";
              } else {
                dp+=sp_g[5]+"N:";
              }
              if (sp_g[6][0] == '-') {
                dp+=sp_g[6].substr(1)+"W:";
              } else {
                dp+=sp_g[6]+"E:";
              }
              dp+=sp_g[7]+":"+sp_g[8]+":";
              if (sp_g[9][0] == '-') {
                dp+=sp_g[9].substr(1)+"S";
              } else {
                dp+=sp_g[9]+"N";
              }
              break;
            }
            case static_cast<int>(Grid::Type::lambertConformal): {
              d="lambertConformal";
              dp = sp_g[1] + ":" + sp_g[2] + ":";
              if (sp_g[3][0] == '-') {
                dp += sp_g[3].substr(1) + "S:";
              } else {
                dp += sp_g[3] + "N:";
              }
              if (sp_g[4][0] == '-') {
                dp += sp_g[4].substr(1) + "W:";
              } else {
                dp += sp_g[4] + "E:";
              }
              if (sp_g[5][0] == '-') {
                dp += sp_g[5].substr(1) + "S:";
              } else {
                dp += sp_g[5] + "N:";
              }
              if (sp_g[6][0] == '-') {
                dp += sp_g[6].substr(1) + "W:";
              } else {
                dp += sp_g[6] + "E:";
              }
              dp += sp_g[9] + ":" + sp_g[7] + ":" + sp_g[8] + ":";
              if (sp_g[10][0] == '-') {
                dp += sp_g[10].substr(1) + "S:";
              } else {
                dp += sp_g[10] + "N:";
              }
              if (sp_g[11][0] == '-') {
                dp += sp_g[11].substr(1) + "S";
              } else {
                dp += sp_g[11] + "N";
              }
              break;
            }
            case static_cast<int>(Grid::Type::sphericalHarmonics): {
              d = "sphericalHarmonics";
              dp = sp_g[1] + ":" + sp_g[2] + ":" + sp_g[3];
              break;
            }
            default: {
              log_error2("insert_grml_inventory() does not understand grid "
                  "type " + sp_g[0], F, "iinv", USER);
            }
          }
          q.set("code", "WGrML.gridDefinitions", "definition = '" + d + "' and "
              "defParams = '" + dp + "'");
          if (q.submit(server) < 0) {
            log_error2("insert_grml_inventory() returned error: " + q.error() +
                " while trying to get gridDefinition code", F, "iinv", USER);
          }
          if (q.num_rows() == 0) {
            log_error2("no gridDefinition code for '" + d + ", " + dp + "'", F,
                "iinv", USER);
          }
          q.fetch_row(row);
          glst.emplace_back(row[0]);
          break;
        }
        case 'L': {
          auto sp_l = split(sp[2], ":");
          if (sp_l.size() < 2 || sp_l.size() > 3) {
            log_error2("insert_grml_inventory() found bad level code: " + sp[2],
                F, "iinv", USER);
          }
          string map, typ;
          if (regex_search(sp_l[0], regex(","))) {
            auto i = sp_l[0].find(",");
            map = sp_l[0].substr(0, i);
            typ = sp_l[0].substr(i + 1);
          } else {
            map = "";
            typ = sp_l[0];
          }
          string val;
          switch (sp_l.size()) {
            case 2: {
              val = sp_l[1];
              break;
            }
            case 3: {
              val = sp_l[2] + "," + sp_l[1];
              break;
            }
          }
          q.set("code", "WGrML.levels", "map = '" + map + "' and type = '" +
              typ + "' and value = '" + val + "'");
          if (q.submit(server) < 0) {
            log_error2("insert_grml_inventory() returned error: " + q.error() +
                " while trying to get level code", F, "iinv", USER);
          }
          if (q.num_rows() == 0) {
            log_error2("no level code for '" + map + ", " + typ + ", " + val +
                "'", F, "iinv", USER);
          }
          q.fetch_row(row);
          llst.emplace_back(row[0]);
          break;
        }
        case 'P': {
          plst.emplace_back(sp[2]);
          auto pcod = fc + "!" + sp[2];
          if (!MySQL::table_exists(server, "IGrML.ds" + local_args.dsnum2 +
              "_inventory_" + pcod)) {
            string res;
            if (sp.size() > 3 && sp[3] == "BIG") {
              if (server.command("create table IGrML.`ds" + local_args.dsnum2 +
                  "_inventory_" + pcod + "` like IGrML"
                  ".template_inventory_p_big", res) < 0) {
                log_error2("insert_grml_inventory() returned error: " + server
                    .error() + " while trying to create parameter inventory "
                    "table", F, "iinv", USER);
              }
            } else {
              if (server.command("create table IGrML.`ds" + local_args.dsnum2 +
                  "_inventory_" + pcod + "` like IGrML.template_inventory_p",
                  res) < 0) {
                log_error2("insert_grml_inventory() returned error: " + server
                    .error() + " while trying to create parameter inventory "
                    "table", F, "iinv", USER);
              }
            }
          }
          pclst.emplace_back(pcod);
          break;
        }
        case 'R': {
          rlst.emplace_back(sp[2]);
          break;
        }
        case 'E': {
          elst.emplace_back(sp[2]);
          break;
        }
      }
    } else if (regex_search(s, regex("\\|"))) {
      auto sp_i = split(s, "|");
      tbyts += stoll(sp_i[1]);
      auto t = stoi(sp_i[3]);
      string idat;
      if (trmap[t].hour_diff != 0x7fffffff) {
        idat = DateTime(stoll(sp_i[2]) * 100).hours_subtracted(trmap[t]
            .hour_diff).to_string("%Y%m%d%H%MM");
        if (dup == "N") {
          InitTimeEntry ite;
          ite.key = idat;
          if (!idates.found(ite.key, ite)) {
            ite.time_range_codes.reset(new my::map<StringEntry>);
            idates.insert(ite);
          }
          StringEntry se;
          se.key = trmap[t].code;
          if (!ite.time_range_codes->found(se.key, se)) {
            ite.time_range_codes->insert(se);
          } else if (ite.time_range_codes->size() > 1) {
            dup = "Y";
          }
        }
      } else {
        idat = "0";
      }
      stringstream iss;
      iss << wic << ", " << sp_i[0] << ", " << sp_i[1] << ", " << sp_i[2] <<
          ", " << idat << ", " << trmap[t].code << ", " << glst[stoi(sp_i[
          4])] << ", " << llst[stoi(sp_i[5])] << ", ";
      if (sp_i.size() > 7 && !sp_i[7].empty()) {
        iss << "'" << rlst[stoi(sp_i[7])] << "',";
      } else {
        iss << "'', ";
      }
      if (sp_i.size() > 8 && !sp_i[8].empty()) {
        iss << "'" << elst[stoi(sp_i[8])] << "'";
      } else {
        iss << "''";
      }
      iss << ", '" << uflg << "'";
      auto pcode=fc+"!"+plst[stoi(sp_i[6])];
      if (server.insert("IGrML.`ds"+local_args.dsnum2+"_inventory_"+pcode+"`","webID_code,byte_offset,byte_length,valid_date,init_date,timeRange_code,gridDefinition_code,level_code,process,ensemble,uflag", iss.str(),"") < 0) {
        if (!regex_search(server.error(),regex("Duplicate entry"))) {
          log_error2("insert_grml_inventory() returned error: " + server
              .error() + " while inserting row '" + iss.str() + "'", F, "iinv",
               USER);
        } else {
          stringstream dss;
          dss << "webID_code = " << wic << " and valid_date = " << sp_i[2] <<
              " and timeRange_code = " << trmap[t].code << " and "
              "gridDefinition_code = " << glst[stoi(sp_i[4])] << " and "
              "level_code = " << llst[stoi(sp_i[5])];
          if (sp_i.size() > 7 && !sp_i[7].empty()) {
            dss << " and process = '" << rlst[stoi(sp_i[7])] << "'";
          } else {
            dss << " and process = ''";
          }
          if (sp_i.size() > 8 && !sp_i[8].empty()) {
            dss << " and ensemble = '" << elst[stoi(sp_i[8])] << "'";
          } else {
            dss << " and ensemble = ''";
          }
          q.set("uflag", "IGrML.`ds" + local_args.dsnum2 + "_inventory_" +
              pcode + "`", dss.str());
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_grml_inventory() returned error: " + server
                .error() + " while trying to get flag for duplicate row: '" +
                dss.str() + "'", F, "iinv", USER);
          }
          if (row[0] == uflg) {
            ++ndup;
            if (local_args.verbose) {
              cout << "**duplicate ignored - line " << nlin << endl;
            }
          } else {
            if (server.update("IGrML.`ds" + local_args.dsnum2 + "_inventory_" +
                pcode + "`","byte_offset = " + sp_i[0] + ", byte_length = " +
                sp_i[1] + ", init_date = " + idat + ", uflag = '" + uflg + "'",
                dss.str()) < 0) {
              log_error2("insert_grml_inventory() returned error: " + server
                  .error() + " while updating duplicate row: '" + dss.str() +
                  "'", F, "iinv", USER);
            }
          }
        }
      }
    }
    ifs.getline(line, 32768);
  }
  ifs.close();
  for (const auto& pc : pclst) {
    server._delete("IGrML.`ds" + local_args.dsnum2 + "_inventory_" + pc + "`",
        "webID_code = " + wic + " and uflag != '" + uflg +"'");
  }
//  server.issueCommand("unlock tables", error);
  if (server.insert("IGrML.ds" + local_args.dsnum2 + "_inventory_summary",
      "webID_code, byte_length, dup", wic + ", " + lltos(tbyts) + ", '" + dup +
      "'", "update byte_length = " + lltos(tbyts) + ", dupe_vdates = '" + dup +       "'") < 0) {
    if (!regex_search(server.error(),regex("Duplicate entry"))) {
      log_error2("insert_grml_inventory() returned error: " + server.error() +
          " while inserting row '" + wic + ", " + lltos(tbyts) +
          ", '" + dup + "''", F, "iinv", USER);
    }
  }
  server.disconnect();
  if (ndup > 0) {
    metautils::log_warning(itos(ndup) + " duplicate grids were ignored",
        "iinv_dupes", USER);
  }
}

void check_for_times_table(MySQL::Server& server, string type, string
    last_decade) {
  static const string F = this_function_label(__func__);
  if (!MySQL::table_exists(server, "IObML.ds" + local_args.dsnum2 + "_" + type +
      "_times_" + last_decade + "0")) {
    string res;
    if (server.command("create table IObML.ds" + local_args.dsnum2 + "_" +
        type + "_times_" + last_decade + "0 like IObML.template_" + type +
        "_times_decade", res) < 0) {
      log_error2("check_for_times_table() returned error: " + server.error() +
          " while trying to create 'ds" + local_args.dsnum2 + "_" + type +
          "_times_" + last_decade + "0'", F, "iinv", USER);
    }
  }
}

void process_IDs(string type, MySQL::Server& server, string ID_index, string
     ID_data, unordered_map<string, string>& id_table) {
  static const string F = this_function_label(__func__);
  auto sp = split(ID_data, "[!]");
  string qs = "select i.code from WObML.ds" + local_args.dsnum2 + "_IDs2 as i "
      "left join WObML.IDTypes as t on t.code = i.IDType_code where i.ID = '" +
      sp[1] + "' and t.IDType = '" + sp[0] + "' and i.sw_lat = " +
      metatranslations::string_coordinate_to_db(sp[2]) + " and i.sw_lon = " +
      metatranslations::string_coordinate_to_db(sp[3]);
  if (sp.size() > 4) {
    qs += " and i.ne_lat = " + metatranslations::string_coordinate_to_db(sp[
        4]) + " and ne_lon = " + metatranslations::string_coordinate_to_db(sp[
        5]);
  }
  MySQL::LocalQuery q(qs);
  MySQL::Row row;
  if (q.submit(server) < 0 || !q.fetch_row(row)) {
    stringstream ess;
    ess << "process_IDs() returned error: " << q.error() << " while trying to "
        "get ID code for '" << sp[0] << ", " << sp[1] << ", " << sp[2] <<
        ", " << sp[3];
    if (sp.size() > 4) {
      ess << ", " << sp[4] << ", " << sp[5];
    }
    ess << "'";
    log_error2(ess.str(), F, "iinv", USER);
  }
  size_t nlt, nln, xlt, xln;
  geoutils::convert_lat_lon_to_box(36, stof(sp[2]), stof(sp[3]), nlt, nln);
  if (sp.size() > 4) {
    geoutils::convert_lat_lon_to_box(36, stof(sp[4]), stof(sp[5]), xlt, xln);
  } else {
    xlt = nlt;
    xln = nln;
  }
  for (size_t n = nlt; n <= xlt; ++n) {
    for (size_t m = nln; m <= xln; ++m) {
      auto slt = itos(n);
      auto sln = itos(m);
      string t = "IObML.ds" + local_args.dsnum2 + "_inventory_" + slt + "_" +
          sln;
      if (type == "irregular") {
        t += "_b";
      }
      if (!MySQL::table_exists(server,t)) {
        string c = "create table " + t + " like IObML"
            ".template_inventory_lati_loni";
        if (type == "irregular") {
          c += "_b";
        }
        string res;
        if (server.command(c, res) < 0) {
          log_error2("process_IDs() returned error: " + server.error() +
              " while trying to create '" + t + "'", F, "iinv", USER);
        }
      }
      auto s = row[0] + "|" + itos(nlt) + "|" + itos(nln);
      if (sp.size() > 4) {
        s += "|" + itos(xlt) + "|" + itos(xln);
      }
      id_table.emplace(ID_index, s);
    }
  }
}

void insert_obml_netcdf_time_series_inventory(std::ifstream& ifs, MySQL::Server&
    server, string web_ID_code, size_t rec_size) {
  static const string F = this_function_label(__func__);
  if (rec_size > 0) {
    log_error2("insert_obml_netcdf_time_series_inventory() can't insert for "           "observations with a record dimension", F, "iinv", USER);
  }
  string uflg = strand(3);
  if (!MySQL::table_exists(server, "IObML.ds" + local_args.dsnum2 +
      "_inventory_summary")) {
    string res;
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_inventory_summary like IObML.template_inventory_summary", res) < 0) {
      log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
          server.error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_inventory_summary'", F, "iinv", USER);
    }
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_dataTypes like IObML.template_dataTypes", res) < 0) {
      log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
          server.error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_dataTypes'", F, "iinv", USER);
    }
  } else {
    if (server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_summary",
        "webID_code = " + web_ID_code) < 0) {
      log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
          server.error() + " while trying to delete from 'ds" +
          local_args.dsnum2 + "_inventory_summary' where webID_code = " +
          web_ID_code, F, "iinv", USER);
    }
  }
  stringstream tss;
  unordered_map<string, string> omap, pmap, dmap, imap;
  vector<string> tlst;
  unordered_set<string> miss_set;
  my::map<DataVariableEntry> datavar_table;
  string l_dec;
  auto i_re = regex("<!>");
  auto m_re = regex("^([0-9]){1,}\\|");
  auto ndbyts = 0;
  char line[32768];
  ifs.getline(line, 32768);
  while (!ifs.eof()) {
    if (regex_search(line, i_re)) {
      auto sp = split(line, "<!>");
      switch (sp[0][0]) {
        case 'D': {
          DataVariableEntry dve;
          dve.key = sp[1];
          dve.data.reset(new DataVariableEntry::Data);
          auto sp2 = split(sp[2], "|");
          dve.data->var_name = sp2[0];
          dve.data->offset = stoi(sp2[1]);
          dve.data->value_type = sp2[2];
          dve.data->byte_len = stoi(sp2[3]);
          ndbyts += dve.data->byte_len;
          datavar_table.insert(dve);
          break;
        }
        case 'I': {
          process_IDs("regular", server, sp[1], sp[2], imap);
          break;
        }
        case 'O': {
          MySQL::LocalQuery q("code", "WObML.obsTypes", "obsType = '" + sp[2] +
              "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_obml_netcdf_time_series_inventory() returned "
                "error: " + q.error() + " while trying to get obsType code for "
                "'" + sp[2] + "'", F, "iinv", USER);
          }
          omap.emplace(sp[1], row[0]);
          break;
        }
        case 'P': {
          MySQL::LocalQuery q("code", "WObML.platformTypes", "platformType = "
              "'" + sp[2] + "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_obml_netcdf_time_series_inventory() returned "
                "error: " + q.error() + " while trying to get platform code "
                "for '" + sp[2] + "'", F, "iinv", USER);
          }
          pmap.emplace(sp[1], row[0]);
          break;
        }
        case 'T': {
          auto dec = sp[2].substr(0, 3);
          if (dec != l_dec) {
            if (tss.tellp() > 0) {
              check_for_times_table(server, "timeSeries", l_dec);
              string res;
              if (server.command("insert into IObML.ds" + local_args.dsnum2 +
                  "_timeSeries_times_" + l_dec + "0 values " + tss.str().substr(
                  1) + " on duplicate key update time_index = values("
                  "time_index), uflag = values(uflag)", res) < 0) {
                log_error2("insert_obml_netcdf_time_series_inventory() "
                    "returned error: " + server.error() + " while trying to "
                    "insert list of times into IObML.ds" + local_args.dsnum2 +
                    "_timeSeries_times_" + l_dec + "0", F, "iinv", USER);
              }
              server._delete("IObML.ds" + local_args.dsnum2 +
                  "_timeSeries_times_" + l_dec + "0", "webID_code = " +
                  web_ID_code + " and uflag != '" + uflg + "'");
              server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" +
                  local_args.dsnum2 + "_timeSeries_times_" + l_dec + "0", res);
            }
            tss.str("");
          }
          tss << ",(" << sp[2] << "," << sp[1] << "," << web_ID_code << ",'" <<
              uflg << "')";
          tlst.emplace_back(sp[2]);
          l_dec = dec;
          break;
        }
      }
    } else if (regex_search(line, m_re)) {
      string s = line;
      miss_set.emplace(s);
      auto i = s.rfind("|");
      DataVariableEntry dve;
      dve.key = s.substr(i + 1);
      datavar_table.found(dve.key, dve);
      s = s.substr(0, i);
      dve.data->missing_table.emplace(s);
    }
    ifs.getline(line, 32768);
  }
  ifs.close();
  if (tss.tellp() > 0) {
    check_for_times_table(server, "timeSeries", l_dec);
    string res;
    if (server.command("insert into IObML.ds" + local_args.dsnum2 +
        "_timeSeries_times_" + l_dec + "0 values " + tss.str().substr(1) +
        " on duplicate key update time_index = values(time_index), uflag = "
        "values(uflag)", res) < 0) {
      log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
          server.error() + " while trying to insert list of times into "
          "IObML.ds" + local_args.dsnum2 + "_timeSeries_times_" + l_dec + "0",
          F, "iinv", USER);
    }
    server._delete("IObML.ds" + local_args.dsnum2 + "_timeSeries_times_" +
        l_dec + "0", "webID_code = " + web_ID_code + " and uflag != '" + uflg +
        "'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" + local_args
        .dsnum2 + "_timeSeries_times_" + l_dec + "0", res);
  }
  stringstream iss;
  auto nbyts = 0;
  for (const auto& i : imap) {
    auto nins = 0;
    auto sp_i = split(i.second, "|");
    string ifil = "IObML.ds" + local_args.dsnum2 + "_inventory_" + sp_i[1] +
        "_" + sp_i[2];
    for (const auto& dkey : datavar_table.keys()) {
      DataVariableEntry dve;
      datavar_table.found(dkey, dve);
      string miss;
      if (dve.data->missing_table.size() == 0) {

        // no times are missing
        miss = "0";
      } else if (tlst.size() - dve.data->missing_table.size() < dve.data->
          missing_table.size()) {

        // times specified are non-missing
        miss = "1";
      } else {

        // times specified are missing
        miss = "2";
      }
      for (const auto& o : omap) {
        for (const auto& p : pmap) {
          auto k = o.first + "|" + p.first + "|" + dve.data->var_name;
          if (dmap.find(k) == dmap.end()) {
            MySQL::LocalQuery q("code", "WObML.ds" + local_args.dsnum2 +
                "_dataTypesList", "observationType_code = " + o.second + " and "
                "platformType_code = " + p.second + " and dataType = 'ds" +
                metautils::args.dsnum + ":" + dve.data->var_name + "'");
            MySQL::Row row;
            if (q.submit(server) < 0 || !q.fetch_row(row)) {
              log_error2("insert_obml_netcdf_time_series_inventory() returned "
                  "error: " + q.error() + " while trying to get dataType code "
                  "for '" + o.second + ", " + p.second  + ", '" + dve.data->
                  var_name + "''", F, "iinv", USER);
            }
            dmap.emplace(k, row[0]);
            if (server.insert("IObML.ds" + local_args.dsnum2 + "_dataTypes",
                web_ID_code + ", " + sp_i[0] + ", " + row[0] + ", '" + dve
                .data->value_type + "', " + itos(dve.data->offset) + ", " +
                itos(dve.data->byte_len) + ", " + miss + ", '" + uflg + "'",
                "update value_type  = '" + dve.data->value_type + "', "
                "byte_offset = " + itos(dve.data->offset) + ", byte_length = " +
                itos(dve.data->byte_len) + ", missing_ind = " + miss + ", "
                "uflag = '" + uflg + "'") < 0) {
              log_error2("insert_obml_netcdf_time_series_inventory() returned "
                  "error: " + server.error() + " while trying to insert "
                  "dataType information for '" + row[0] + "'", F, "iinv", USER);
            }
          }
          string s = "|" + o.first + "|" + p.first + "|" + i.first + "|" + dkey;
          auto n = 0;
          for (const auto& t : tlst) {
            if (miss != "0") {
              auto f = miss_set.find(itos(n) + s) != miss_set.end();
              if ((miss == "1" && !f) || (miss == "2" && f)) {
                if (nins >= 10000) {
                  string res;
                  if (server.command("insert into " + ifil + " values " +iss
                      .str().substr(1) + " on duplicate key update uflag = "
                      "values(uflag)", res) < 0) {
                    log_error2("insert_obml_netcdf_time_series_inventory() "
                        "returned error: " + server.error() + " while trying "
                        "to insert inventory data", F, "iinv", USER);
                  }
                  nins = 0;
                  iss.str("");
                }
                iss << ", (" << web_ID_code << ", " << t << ", " << sp_i[0] <<
                    ", " << dmap[k] << ", '" << uflg << "')";
                ++nins;
              }
            }
            nbyts += dve.data->byte_len;
            ++n;
          }
        }
      }
    }
    string res;
    if (!iss.str().empty()  && server.command("insert into " + ifil +
        " values " + iss.str().substr(1) + " on duplicate key update uflag = "
        "values(uflag)", res) < 0) {
      log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
          server.error() + " while trying to insert inventory data", F, "iinv",
          USER);
    }
    server._delete(ifil, "webID_code = " + web_ID_code + " and uflag != '" +
        uflg + "'");
    server.command("analyze NO_WRITE_TO_BINLOG table " + ifil, res);
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_dataTypes","webID_code = " +
      web_ID_code + " and uflag != '" + uflg + "'");
  string res;
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" + local_args
      .dsnum2 + "_dataTypes", res);
  if (server.command("insert into IObML.ds" + local_args.dsnum2 +
      "_inventory_summary values (" + web_ID_code + ", " + itos(nbyts) + ", " +
      itos(ndbyts) + ", '" + uflg + "') on duplicate key update byte_length = "
      "values(byte_length), dataType_length = values(dataType_length), uflag = "
      "values(uflag)", res) < 0) {
    log_error2("insert_obml_netcdf_time_series_inventory() returned error: " +
        server.error() + " while trying to insert file size data for '" +
        web_ID_code + "'", F, "iinv", USER);
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_summary",
      "webID_code = " + web_ID_code + " and uflag != '" + uflg + "'");
}

void insert_obml_netcdf_point_inventory(std::ifstream& ifs, MySQL::Server&
    server, string web_ID_code, size_t rec_size) {
  static const string F = this_function_label(__func__);
  if (rec_size > 0) {
    log_error2("insert_obml_netcdf_point_inventory() can't insert for "
        "observations with a record dimension", F, "iinv", USER);
  }
  string uflg = strand(3);
  string res;
  if (!MySQL::table_exists(server, "IObML.ds" + local_args.dsnum2 +
      "_inventory_summary")) {
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_inventory_summary like IObML.template_inventory_summary", res) <
        0) {
      log_error2("insert_obml_netcdf_point_inventory() returned error: " +
          server.error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_inventory_summary'", F, "iinv", USER);
    }
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_dataTypes like IObML.template_dataTypes", res) < 0) {
      log_error2("insert_obml_netcdf_point_inventory() returned error: " +
          server.error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_dataTypes'", F, "iinv", USER);
    }
  }
  stringstream tss;
  unordered_map<string, string> imap, omap, pmap, dmap;
  vector<string> tlst;
  unordered_set<string> miss_set;
  my::map<DataVariableEntry> datavar_table;
  string l_dec;
  auto i_re = regex("<!>");
  auto m_re = regex("^([0-9]){1,}\\|");
  auto ndbyts = 0;
  char line[32768];
  ifs.getline(line, 32768);
  while (!ifs.eof()) {
    if (regex_search(line, i_re)) {
      auto sp_l = split(line, "<!>");
      switch (sp_l[0][0]) {
        case 'D': {
          DataVariableEntry dve;
          dve.key = sp_l[1];
          dve.data.reset(new DataVariableEntry::Data);
          auto sp2 = split(sp_l[2], "|");
          dve.data->var_name = sp2[0];
          dve.data->offset = stoi(sp2[1]);
          dve.data->value_type = sp2[2];
          dve.data->byte_len = stoi(sp2[3]);
          ndbyts += dve.data->byte_len;
          datavar_table.insert(dve);
          break;
        }
        case 'I': {
          process_IDs("regular", server, sp_l[1], sp_l[2], imap);
          break;
        }
        case 'O': {
          MySQL::LocalQuery q("code", "WObML.obsTypes", "obsType = '" + sp_l[
              2] + "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_obml_netcdf_point_inventory() returned error: " +
                q.error() + " while trying to get obsType code for '" + sp_l[
                2] + "'", F, "iinv", USER);
          }
          omap.emplace(sp_l[1], row[0]);
          break;
        }
        case 'P': {
          MySQL::LocalQuery q("code", "WObML.platformTypes", "platformType = "
              "'" + sp_l[2] + "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_obml_netcdf_point_inventory() returned error: " +
                q.error() + " while trying to get platform code for '" + sp_l[
                2] + "'", F, "iinv", USER);
          }
          pmap.emplace(sp_l[1], row[0]);
          break;
        }
        case 'T': {
          auto dec = sp_l[2].substr(0,3);
          if (dec != l_dec) {
            if (tss.tellp() > 0) {
              check_for_times_table(server, "point", l_dec);
              string res;
              if (server.command("insert into IObML.ds" + local_args.dsnum2 +
                  "_point_times_" + l_dec + "0 values " + tss.str().substr(1) +
                  " on duplicate key update time_index = values(time_index), "
                  "lat = values(lat), lon = values(lon), uflag = values(uflag)",
                  res) < 0) {
                log_error2("insert_obml_netcdf_point_inventory() returned "
                    "error: " + server.error() + " while trying to insert list "
                    "of times into IObML.ds" + local_args.dsnum2 +
                    "_point_times_" + l_dec + "0", F, "iinv", USER);
              }
              server._delete("IObML.ds" + local_args.dsnum2 + "_point_times_" +
                  l_dec + "0", "webID_code = " + web_ID_code + " and uflag != "
                  "'" + uflg + "'");
              server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" +
                  local_args.dsnum2 + "_point_times_" + l_dec + "0", res);
            }
            tss.str("");
          }
          auto sp = split(sp_l[2], "[!]");
          tss << ", (" << sp[0] << ", " << sp_l[1] << ", " <<
              metatranslations::string_coordinate_to_db(sp[1]) << ", " <<
              metatranslations::string_coordinate_to_db(sp[2]) << ", " <<
              web_ID_code << ", '" << uflg << "')";
          tlst.emplace_back(sp_l[2]);
          l_dec = dec;
          break;
        }
      }
    } else if (regex_search(line, m_re)) {
      string s = line;
      miss_set.emplace(s);
      auto i = s.rfind("|");
      DataVariableEntry dve;
      dve.key = s.substr(i + 1);
      datavar_table.found(dve.key, dve);
      s = s.substr(0, i);
      dve.data->missing_table.emplace(s);
    }
    ifs.getline(line, 32768);
  }
  ifs.close();
  if (tss.tellp() > 0) {
    check_for_times_table(server, "point", l_dec);
    string res;
    if (server.command("insert into IObML.ds" + local_args.dsnum2 +
        "_point_times_" + l_dec + "0 values " + tss.str().substr(1) + " on "
        "duplicate key update time_index = values(time_index), lat = values("
        "lat), lon = values(lon), uflag = values(uflag)", res) < 0) {
      log_error2("insert_obml_netcdf_point_inventory() returned error: " +
          server.error() + " while trying to insert list of times into "
          "IObML.ds" + local_args.dsnum2 + "_point_times_" + l_dec + "0",
          F, "iinv", USER);
    }
    server._delete("IObML.ds" + local_args.dsnum2 + "_point_times_" + l_dec +
        "0", "webID_code = " + web_ID_code + " and uflag != '" + uflg + "'");
    server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" + local_args
        .dsnum2 + "_point_times_" + l_dec + "0", res);
  }
  stringstream dss;
  auto nbyts = 0;
  for (const auto& i : imap) {
    my::map<InsertEntry> insert_table;
    auto sp_i = split(i.second, "|");
    for (const auto& dkey : datavar_table.keys()) {
      DataVariableEntry dve;
      datavar_table.found(dkey, dve);
      string miss;
      if (dve.data->missing_table.size() == 0) {

        // no times are missing
        miss = "0";
      } else if (tlst.size() - dve.data->missing_table.size() < dve.data->
          missing_table.size()) {

        // times specified are non-missing
        miss = "1";
      } else {

        // times specified are missing
        miss = "2";
      }
      for (const auto& o : omap) {
        for (const auto& p : pmap) {
          auto k = o.first + "|" + p.first + "|" + dve.data->var_name;
          if (dmap.find(k) == dmap.end()) {
            MySQL::LocalQuery q("code", "WObML.ds" + local_args.dsnum2 +
                "_dataTypesList", "observationType_code = " + o.second + " and "
                "platformType_code = " + p.second + " and dataType = 'ds" +
                metautils::args.dsnum + ":" + dve.data->var_name + "'");
            MySQL::Row row;
            if (q.submit(server) < 0 || !q.fetch_row(row)) {
              log_error2("insert_obml_netcdf_point_inventory() returned "
                  "error: " + q.error() + " while trying to get dataType code "
                  "for '" + o.second + ", " + p.second + ", '" + dve.data->
                  var_name + "''", F, "iinv", USER);
            }
            dmap.emplace(k, row[0]);
            dss << ", (" << web_ID_code << "," << sp_i[0] << "," << row[0] <<
                ",'" << dve.data->value_type << "'," << dve.data->offset <<
                "," << dve.data->byte_len << "," << miss << ",'" << uflg <<
                "')";
          }
          string s = "|" + o.first + "|" + p.first + "|" + i.first + "|" + dkey;
          auto n = 0;
          for (const auto& t : tlst) {
            auto sp_t = split(t, "[!]");
            size_t j, i;
            geoutils::convert_lat_lon_to_box(36, stof(sp_t[1]), stof(sp_t[2]),
                j, i);
            InsertEntry ie;
            ie.key=itos(j) + "_" + itos(i);
            if (!insert_table.found(ie.key, ie)) {
              ie.data.reset(new InsertEntry::Data);
              insert_table.insert(ie);
            }
            if (miss != "0") {
              auto f = miss_set.find(itos(n) + s) != miss_set.end();
              if ((miss == "1" && !f) || (miss == "2" && f)) {
                if (ie.data->num_inserts >= 10000) {
                  string res;
                  if (server.command("insert into IObML.ds" + local_args
                      .dsnum2 + "_inventory_" + ie.key + " values " + ie.data->
                      inv_insert.substr(1) + " on duplicate key update uflag = "
                      "values(uflag)", res) < 0) {
                    log_error2("insert_obml_netcdf_point_inventory() returned "
                        "error: " + server.error() + " while trying to insert "
                        "inventory data", F, "iinv", USER);
                  }
                  ie.data->num_inserts = 0;
                  ie.data->inv_insert = "";
                }
                ie.data->inv_insert += ", (" + web_ID_code + ", " + sp_t[0] +
                    ", " + sp_i[0] + ", " + dmap[k] + ", '" + uflg + "')";
                ++(ie.data->num_inserts);
              }
            }
            nbyts += dve.data->byte_len;
            ++n;
          }
        }
      }
    }
    for (const auto& key : insert_table.keys()) {
      InsertEntry ie;
      insert_table.found(key, ie);
      string res;
      if (!ie.data->inv_insert.empty() && server.command("insert into "
          "IObML.ds" + local_args.dsnum2 + "_inventory_" + key + " values " + ie
          .data->inv_insert.substr(1) + " on duplicate key update uflag = "
          "values(uflag)", res) < 0) {
        log_error2("insert_obml_netcdf_point_inventory() returned error: " +
            server.error() + " while trying to insert inventory data", F,
            "iinv", USER);
      }
      server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_" + key,
          "webID_code = " + web_ID_code + " and uflag != '" + uflg + "'");
      server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" + local_args
          .dsnum2 + "_inventory_" + key, res);
    }
  }
  if (server.command("insert into IObML.ds" + local_args.dsnum2 + "_dataTypes "
      "values " + dss.str().substr(1) + " on duplicate key update value_type = "
      "values(value_type), byte_offset = values(byte_offset), byte_length = "
      "values(byte_length), missing_ind = values(missing_ind), uflag = values("
      "uflag)", res) < 0) {
    log_error2("insert_obml_netcdf_point_inventory() returned error: " + server
        .error() + " while trying to insert dataType information '" + dss
        .str() + "'", F, "iinv", USER);
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_dataTypes","webID_code = " +
      web_ID_code + " and uflag != '" + uflg + "'");
  server.command("analyze NO_WRITE_TO_BINLOG table IObML.ds" + local_args
      .dsnum2 + "_dataTypes", res);
  if (server.command("insert into IObML.ds" + local_args.dsnum2 +
      "_inventory_summary values (" + web_ID_code + ", " + itos(nbyts) + ", " +
      itos(ndbyts) + ", '" + uflg + "') on duplicate key update byte_length = "
      "values(byte_length), dataType_length = values(dataType_length), uflag = "
      "values(uflag)", res) < 0) {
    log_error2("insert_obml_netcdf_point_inventory() returned error: " + server
        .error() + " while trying to insert file size data for '" +
        web_ID_code + "'", F, "iinv", USER);
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_summary",
      "webID_code = " + web_ID_code + " and uflag != '" + uflg + "'");
}

void insert_generic_point_inventory(std::ifstream& ifs ,MySQL::Server& server,
    string web_ID_code) {
  static const string F = this_function_label(__func__);
  auto uflg = strand(3);
  int mnlat = 99, mnlon = 99, mxlat = -1, mxlon = -1;
  if (!MySQL::table_exists(server, "IObML.ds" + local_args.dsnum2 +
      "_dataTypesList_b")) {
    string res;
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_dataTypesList_b like IObML.template_dataTypesList_b", res) < 0) {
      log_error2("insert_generic_point_inventory() returned error: " + server
          .error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_dataTypesList_b'", F, "iinv", USER);
    }
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_dataTypes like IObML.template_dataTypes", res) < 0) {
      log_error2("insert_generic_point_inventory() returned error: " + server
          .error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_dataTypes'", F, "iinv", USER);
    }
    if (server.command("create table IObML.ds" + local_args.dsnum2 +
        "_inventory_summary like IObML.template_inventory_summary", res) < 0) {
      log_error2("insert_generic_point_inventory() returned error: " + server
          .error() + " while trying to create 'ds" + local_args.dsnum2 +
          "_inventory_summary'", F, "iinv", USER);
    }
  }
  unordered_map<string, string> tmap, omap, pmap, imap;
  unordered_map<size_t, std::tuple<size_t, size_t>> dmap;
  unordered_map<string, shared_ptr<unordered_map<size_t, size_t>>> dtyps;
  size_t nbyts = 0;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    string s = line;
    if (regex_search(s, regex("<!>"))) {
      auto sp=split(s, "<!>");
      switch (sp[0][0]) {
        case 'T': {
          tmap.emplace(sp[1], sp[2]);
          break;
        }
        case 'O': {
          MySQL::LocalQuery q("code", "WObML.obsTypes", "obsType = '" + sp[2] +
              "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_generic_point_inventory() returned error: " + q
                .error() + " while trying to get obsType code for '" + sp[2] +
                "'", F, "iinv", USER);
          }
          omap.emplace(sp[1],row[0]);
          break;
        }
        case 'P': {
          MySQL::LocalQuery q("code", "WObML.platformTypes", "platformType = "
              "'" + sp[2] + "'");
          MySQL::Row row;
          if (q.submit(server) < 0 || !q.fetch_row(row)) {
            log_error2("insert_generic_point_inventory() returned error: " + q
                .error() + " while trying to get platformType code for '" + sp[
                2] + "'", F, "iinv", USER);
          }
          pmap.emplace(sp[1], row[0]);
          break;
        }
        case 'I': {
          process_IDs("irregular", server, sp[1], sp[2], imap);
          break;
        }
        case 'D': {
          if (sp.size() > 2) {
            auto sp2 = split(sp[2], "[!]");
            MySQL::LocalQuery q("code", "WObML.ds" + local_args.dsnum2 +
                "_dataTypesList","observationType_code = " + omap[sp2[0]] +
                " and platformType_code = " + pmap[sp2[1]] + " and dataType = "
                "'" + sp2[2] + "'");
            MySQL::Row row;
            if (q.submit(server) < 0 || !q.fetch_row(row)) {
              log_error2("insert_generic_point_inventory() returned error: " + q
                  .error() + " while trying to get dataType code for '" + sp[
                  2] + "'", F, "iinv", USER);
            }
            dmap.emplace(stoi(sp[1]), std::make_tuple(stoi(row[0]), stoi(sp2[3]
                .substr(1))));
            string typ;
            switch (sp2[3][0]) {
              case 'B': {
                typ = "byte";
                break;
              }
              case 'F': {
                typ = "float";
                break;
              }
              case 'I': {
                typ = "int";
                break;
              }
            }
            string scl;
            if (sp2.size() > 4) {
              scl = sp2[4];
            } else {
              scl = "1";
            }
            if (server.insert("IObML.ds" + local_args.dsnum2 +
                "_dataTypesList_b", row[0] + ", '" + typ + "', " + scl + ", " +
                sp2[3].substr(1) + ", NULL") < 0) {
              if (!regex_search(server.error(), regex("Duplicate entry"))) {
                log_error2("insert_generic_point_inventory() returned error: " +
                    server.error() + " while inserting row '" + row[0] + ", '" +
                    typ + "', " + scl + ", " + sp2[3].substr(1) +
                    ", NULL'", F, "iinv", USER);
              }
            }
          }
          break;
        }
      }
    } else if (regex_search(s, regex("\\|"))) {
      auto sp = split(s, "|");
      auto sp2 = split(imap.at(sp[2]), "|");
      int nlat = stoi(sp2[1]);
      if (nlat < mnlat) {
        mnlat = nlat;
      }
      int nlon = stoi(sp2[2]);
      if (nlon < mnlon) {
        mnlon = nlon;
      }
      int xlat = stoi(sp2[3]);
      if (xlat > mxlat) {
        mxlat = xlat;
      }
      int xlon = stoi(sp2[4]);
      if (xlon > mxlon) {
        mxlon = xlon;
      }
      shared_ptr<unordered_map<size_t, size_t>> p;
      auto t = dtyps.find(sp2[0]);
      if (t == dtyps.end()) {
        p.reset(new unordered_map<size_t, size_t>);
        dtyps.emplace(sp2[0], p);
      } else {
        p = t->second;
      }
      vector<size_t> vlst, vals;
      bitmap::uncompress_values(sp[3], vlst);
      size_t mn = 0xffffffff, mx = 0;
      for (auto val : vlst) {
        auto d = dmap.at(val);
        val=std::get<0>(d);
        vals.emplace_back(val);
        mn = std::min(val, mn);
        mx = std::max(val, mx);
        auto field_len=std::get<1>(d);
        nbyts += field_len;
        if (p->find(val) == p->end()) {
          p->emplace(val, field_len);
        }
      }
      string bmap;
      bitmap::compress_values(vals, bmap);
      for (int n = nlat; n <= xlat; ++n) {
        for (int m = nlon; m <= xlon; ++m) {
          if (server.insert("IObML.ds" + local_args.dsnum2 + "_inventory_" +
              itos(n) + "_" + itos(m) + "_b", web_ID_code + "," + tmap.at(
              sp[1]) + ", " + sp2[0] + ", '" + bmap + "', " + itos(mn) + ", " +
              itos(mx) + ", '" + sp[0] + "', '" + uflg + "'", "update "
              "dataType_codes = '" + bmap + "', byte_offsets = '" + sp[0] +
              "', uflag = '" + uflg + "'") < 0) {
            log_error2("insert_generic_point_inventory() returned error: " +
                server.error() + " for insert: '" + web_ID_code + ", " + tmap
                .at(sp[1]) + ", " + sp2[0] + ", '" + bmap + "', " + itos(mn) +
                ", " + itos(mx) + ", '" + sp[0] + "'' into table IObML.ds" +
                local_args.dsnum2 + "_inventory_" + itos(n) + "_" + itos(m) +
                "_b", F, "iinv", USER);
          }
        }
      }
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  for (int n = mnlat; n <= mxlat; ++n) {
    for (int m = mnlon; m <= mxlon; ++m) {
      server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_" + itos(n) +
          "_" + itos(m) + "_b","webID_code = " + web_ID_code + " and uflag != "
          "'" + uflg + "'");
    }
  }
  if (server.insert("IObML.ds" + local_args.dsnum2 + "_inventory_summary",
      web_ID_code + ", " + itos(nbyts) + ", 0, '" + uflg + "'", "update "
      "byte_length = " + itos(nbyts) + ", uflag = '" + uflg + "'") < 0) {
    log_error2("insert_generic_point_inventory() returned error: " + server
        .error() + " for insert: '" + web_ID_code + ", " + itos(nbyts) +
        "' into table IObML.ds" + local_args.dsnum2 + "_inventory_summary", F,
        "iinv", USER);
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_inventory_summary",
      "webID_code = " + web_ID_code + " and uflag != '" + uflg + "'");
  for (const auto& e : dtyps) {
    for (const auto& e2 : *e.second) {
      if (server.insert("IObML.ds" + local_args.dsnum2 + "_dataTypes",
        web_ID_code + ", " + e.first + ", " + itos(e2.first) + ", '', 0, " +
            itos(e2.second) + ", 0, '" + uflg + "'", "update value_type = '', "
            "byte_offset = 0, byte_length = " + itos(e2.second) + ", "
            "missing_ind = 0, uflag = '" + uflg + "'") < 0) {
        log_error2("insert_generic_point_inventory() returned error: " + server
            .error() + " for insert: '" + web_ID_code + ", " + e.first + ", " +
            itos(e2.first) + ", '', 0, " + itos(e2.second) + ", 0'", F, "iinv",
            USER);
      }
    }
  }
  server._delete("IObML.ds" + local_args.dsnum2 + "_dataTypes","webID_code = " +
      web_ID_code + " and uflag != '" + uflg + "'");
}

void insert_obml_inventory() {
  static const string F = this_function_label(__func__);
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  struct stat buf;
  auto fil = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
      metautils::args.dsnum + "/metadata/inv/" + metautils::args.filename +
      ".gz", temp_dir.name());
  if (stat(fil.c_str(), &buf) == 0) {
    system(("gunzip " + fil).c_str());
    chop(fil, 3);
  } else {
    fil = unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds" +
        metautils::args.dsnum + "/metadata/inv/" + metautils::args.filename,
        temp_dir.name());
  }
  std::ifstream ifs(fil.c_str());
  if (!ifs.is_open()) {
    log_error2("insert_obml_inventory() was not able to open " + metautils::
        args.filename, F, "iinv", USER);
  }
  fil = substitute(metautils::args.filename, ".ObML_inv", "");
  replace_all(fil, "%", "/");
  MySQL::LocalQuery q("select code, tindex from WObML.ds"+local_args.dsnum2+"_webfiles2 as w left join dssdb.wfile as x on (x.dsid = 'ds"+metautils::args.dsnum+"' and x.type = 'D' and x.wfile = w.webID) where webID = '"+fil+"'");
  if (q.submit(srv) < 0) {
    log_error2("insert_obml_inventory() returned error: " + q.error() +
        " while looking for code from webfiles", F, "iinv", USER);
  }
  MySQL::Row row;
  q.fetch_row(row);
  auto wcod = row[0];
  tindex = row[1];
  char line[32768];
  ifs.getline(line, 32768);
  if (regex_search(line, regex("^netCDF:timeSeries"))) {
    string s = line;
    insert_obml_netcdf_time_series_inventory(ifs, srv, wcod, stoi(s.substr(s
        .find("|") + 1)));
  } else if (regex_search(line, regex("^netCDF:point"))) {
    string s = line;
    insert_obml_netcdf_point_inventory(ifs, srv, wcod, stoi(s.substr(s.find(
        "|") + 1)));
  } else {
    insert_generic_point_inventory(ifs, srv, wcod);
  }
  srv.disconnect();
}

void insert_inventory() {
  auto i = metautils::args.filename.rfind(".");
  auto e = metautils::args.filename.substr(i + 1);
  if (e == "GrML_inv") {
    if (!local_args.wms_only) {
      insert_grml_inventory();
    }
//    build_wms_capabilities();
  } else if (e == "ObML_inv") {
    insert_obml_inventory();
  } else {
    log_error2("insert_inventory() does not recognize inventory extension " + e,
        this_function_label(__func__), "iinv", USER);
  }
}

int main(int argc, char **argv) {
  if (argc < 4) {
    cerr << "usage: iinv -d [ds]nnn.n [options...] -f file" << endl;
    cerr << "\nrequired:" << endl;
    cerr << "-d nnn.n   specifies the dataset number" << endl;
    cerr << "-f file    summarize information for inventory file <file>" <<
        endl;
    cerr << "\noptions:" << endl;
    cerr << "-c/-C       create (default)/don't create file list cache" << endl;
    cerr << "-N          notify with a message when " << argv[0] <<
        " completes" << endl;
    cerr << "-V          verbose mode" << endl;
    cerr << "--wms-only  only generate the WMS capabilities document for the "
        "file" << endl;
    exit(1);
  }
  auto t1 = time(nullptr);
  signal(SIGSEGV, segv_handler);
  metautils::read_config("iinv", USER);
  parse_args(argc, argv);
  metautils::cmd_register("iinv", USER);
  temp_dir.create(metautils::directives.temp_path);
  insert_inventory();
  if (local_args.create_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv", "iinv", USER);
    if (!tindex.empty() && tindex != "0") {
      gatherxml::summarizeMetadata::create_file_list_cache("inv", "iinv", USER,
          tindex);
    }
  }
  if (local_args.notify) {
    cout << argv[0] << " has completed successfully" << endl;
  }
  auto t2 = time(nullptr);
  metautils::log_warning("execution time: " + ftos(t2 - t1) + " seconds",
      "iinv.time", USER);
}
