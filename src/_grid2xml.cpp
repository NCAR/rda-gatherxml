#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <list>
#include <deque>
#include <vector>
#include <regex>
#include <unordered_map>
#include <gatherxml.hpp>
#include <pglocks.hpp>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <xmlutils.hpp>
#include <netcdf.hpp>
#include <timer.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::unordered_map;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_map;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::substitute;
using strutils::trim;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");

unordered_map<string, gatherxml::markup::GrML::GridEntry> grid_table;
unique_ptr<TempFile> tfile;
string tfile_name, inv_file;
unique_ptr<TempDir> tdir;
TempDir *inv_dir = nullptr;
string myerror = "";
string mywarning = "";
string myoutput = "";
const char *grib1_time_unit[] = { "minute", "hour", "day", "month", "year",
    "year", "year", "year", "", "", "hour", "hour", "hour" };
const int grib1_per_day[] = { 1440, 24, 1, 0, 0, 0, 0, 0, 0, 0, 24, 24, 24 };
const int grib1_unit_mult[] = { 1, 1, 1, 1, 1, 10, 30, 100, 1, 1, 3, 6, 12 };
const char *grib2_time_unit[] = { "minute", "hour", "day", "month", "year",
    "year", "year", "year", "", "", "hour", "hour", "hour", "second" };
const int grib2_per_day[] = { 1440, 24, 1, 0, 0, 0, 0, 0, 0, 0, 24, 24, 24,
    86400 };
const int grib2_unit_mult[] = { 1, 1, 1, 1, 1, 10, 30, 100, 1, 1, 3, 6, 12, 1 };

struct Inventory {
  Inventory() : file(), dir(nullptr), stream() { }

  string file;
  unique_ptr<TempDir> dir;
  std::ofstream stream;
} g_inv;

extern "C" void clean_up() {

  if (metautils::args.data_format == "cmorph025" || metautils::args.data_format
      == "cmorph8km") {

    // remove temporary file that was sym-linked because the data file name
    // contains metadata
    stringstream oss, ess;
    mysystem2("/bin/rm " + tfile_name, oss, ess);
  }
  if (!myerror.empty()) {
    log_error2(myerror, "clean_up()", "grid2xml", USER);
  }
}

bool open_file(void *istream, string filename) {
  if (metautils::args.data_format == "cgcm1") {
    return (reinterpret_cast<InputCGCM1GridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "cmorph025") {
    return (reinterpret_cast<InputCMORPH025GridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "cmorph8km") {
    return (reinterpret_cast<InputCMORPH8kmGridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "gpcp") {
    return (reinterpret_cast<InputGPCPGridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "grib" || metautils::args.data_format ==
      "grib2") {
    return (reinterpret_cast<InputGRIBStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "jraieeemm") {
    return (reinterpret_cast<InputJRAIEEEMMGridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "ll") {
    return (reinterpret_cast<InputLatLonGridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "navy") {
    return (reinterpret_cast<InputNavyGridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "noaaoi2") {
    return (reinterpret_cast<InputNOAAOI2SSTGridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "oct") {
    return (reinterpret_cast<InputOctagonalGridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "on84") {
    return (reinterpret_cast<InputON84GridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "slp") {
    return (reinterpret_cast<InputSLPGridStream *>(istream))->open(filename);
  }
  if (metautils::args.data_format == "tropical") {
    return (reinterpret_cast<InputTropicalGridStream *>(istream))->open(
        filename);
  }
  if (metautils::args.data_format == "ussrslp") {
    return (reinterpret_cast<InputUSSRSLPGridStream *>(istream))->open(
        filename);
  }
  log_error2(metautils::args.data_format + "-formatted file not recognized",
      "open_file()", "grid2xml", USER);
  return false;
}

void scan_file() {
  static const string F = this_function_label(__func__);
  unique_ptr<idstream> istream;
  Grid *grid = nullptr;
  unique_ptr<GRIBMessage> message;
  if (metautils::args.data_format == "cgcm1") {
    istream.reset(new InputCGCM1GridStream);
    grid = new CGCM1Grid;
  } else if (metautils::args.data_format == "cmorph025") {
    istream.reset(new InputCMORPH025GridStream);
    grid = new CMORPH025Grid;
  } else if (metautils::args.data_format == "cmorph8km") {
    istream.reset(new InputCMORPH8kmGridStream);
    grid = new CMORPH8kmGrid;
  } else if (metautils::args.data_format == "gpcp") {
    istream.reset(new InputGPCPGridStream);
    grid = new GPCPGrid;
  } else if (metautils::args.data_format == "grib" || metautils::args.
      data_format == "grib0") {
    istream.reset(new InputGRIBStream);
    message.reset(new GRIBMessage);
    grid = new GRIBGrid;
  } else if (metautils::args.data_format == "grib2") {
    istream.reset(new InputGRIBStream);
    message.reset(new GRIB2Message);
    grid = new GRIB2Grid;
  } else if (metautils::args.data_format == "jraieeemm") {
    istream.reset(new InputJRAIEEEMMGridStream);
    grid = new JRAIEEEMMGrid;
  } else if (metautils::args.data_format == "ll") {
    istream.reset(new InputLatLonGridStream);
    grid = new LatLonGrid;
  } else if (metautils::args.data_format == "navy") {
    istream.reset(new InputNavyGridStream);
    grid = new NavyGrid;
  } else if (metautils::args.data_format == "noaaoi2") {
    istream.reset(new InputNOAAOI2SSTGridStream);
    grid = new NOAAOI2SSTGrid;
  } else if (metautils::args.data_format == "oct") {
    istream.reset(new InputOctagonalGridStream);
    grid = new OctagonalGrid;
  } else if (metautils::args.data_format == "on84") {
    istream.reset(new InputON84GridStream);
    grid = new ON84Grid;
  } else if (metautils::args.data_format == "slp") {
    istream.reset(new InputSLPGridStream);
    grid = new SLPGrid;
  } else if (metautils::args.data_format == "tropical") {
    istream.reset(new InputTropicalGridStream);
    grid = new TropicalGrid;
  } else if (metautils::args.data_format == "ussrslp") {
    istream.reset(new InputUSSRSLPGridStream);
    grid = new USSRSLPGrid;
  } else {
    log_error2(metautils::args.data_format + "-formatted files not recognized",
        F, "grid2xml", USER);
  }
  tfile.reset(new TempFile);
  tdir.reset(new TempDir);
  if (metautils::args.data_format == "jraieeemm") {
    tfile->open(metautils::args.temp_loc, "." + metautils::args.filename);
    tfile_name = tfile->name();
  } else {
    tfile->open(metautils::args.temp_loc);
    if (metautils::args.data_format == "cmorph025" || metautils::args.
        data_format == "cmorph8km") {

      // symlink the temporary file when the data file name contains metadata
      tfile_name = metautils::args.filename;
      auto idx = tfile_name.rfind("/");
      if (idx != string::npos) {
        tfile_name = tfile_name.substr(idx + 1);
      }
      idx = (tfile->name()).rfind("/");
      tfile_name = (tfile->name()).substr(0, idx + 1) + tfile_name;
      stringstream oss, ess;
      if (mysystem2("/bin/ln -s " + tfile->name() + " " + tfile_name, oss, ess)
          != 0) {
        log_error2("unable to sym-link to temporary file - " + ess.str(), F,
            "grid2xml", USER);
      }
    } else {
      tfile_name = tfile->name();
    }
  }
  tdir->create(metautils::args.temp_loc);
  string f, e;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,
      *tdir, NULL, f, e)) {
    log_error2(e, F + ": prepare_file_for_metadata_scanning()", "grid2xml",
        USER);
  }
  if (!open_file(istream.get(), tfile_name)) {
    log_error2("unable to open file for input", F, "grid2xml", USER);
  }
  if ((f.empty() || f == "TAR") && (metautils::args.data_format == "grib" ||
      metautils::args.data_format == "grib0" || metautils::args.data_format ==
      "grib2")) {
    gatherxml::fileInventory::open(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
        "grid2xml", USER);
  } else if (metautils::args.inventory_only) {
    log_error2("unable to inventory " + metautils::args.path + "/" + metautils::
        args.filename + " because archive format is '" + f + "'", F, "grid2xml",
        USER);
  }
  xmlutils::LevelMapper lmap("/glade/u/home/rdadata/share/metadata"
      "/LevelTables");
  unordered_map<string, int> U_map, G_map, L_map, P_map, R_map, E_map;
  std::list<string> U_lst, G_lst, L_lst, P_lst, R_lst, E_lst;
  std::list<string> ilst;
  unique_ptr<unsigned char[]> b;
  int blen = 0;
  size_t ngds = 0;
  while (1) {
    if ((metautils::args.data_format != "grib" && metautils::args.data_format !=
        "grib2") || ngds == message->number_of_grids()) {
      auto byts = istream->peek();
      if (byts < 0) {
        if (byts == bfstream::error) {
          log_error2("an error occurred while reading the data file - no "
              "content metadata was generated", F, "grid2xml", USER);
          exit(1);
        } else {
          break;
        }
      }
      if (byts > blen) {
        blen = byts;
        b.reset(new unsigned char[blen]);
      }
      istream->read(b.get(), blen);
      ngds = 0;
    }
    if (metautils::args.data_format == "grib" || metautils::args.data_format ==
        "grib2") {
      if (ngds == 0) {
        message->fill(b.get(), true);
        while (message->number_of_grids() == 0) {
          auto byts = istream->peek();
          if (byts < 0) {
            if (byts == bfstream::error) {
              log_error2("an error occurred while reading the data file - no "
                  "content metadata was generated", F, "grid2xml", USER);
              exit(1);
            } else {
              break;
            }
          }
          if (byts > blen) {
            blen = byts;
            b.reset(new unsigned char[blen]);
          }
          istream->read(b.get(), blen);
          message->fill(b.get(), true);
        }
        if (message->number_of_grids() == 0) {
          break;
        }
      }
      grid = message->grid(ngds);
      ++ngds;
    } else {
      if (metautils::args.data_format == "cmorph025") {
        reinterpret_cast<CMORPH025Grid *>(grid)->set_date_time(reinterpret_cast<
            InputCMORPH025GridStream *>(istream.get())->date_time());
        reinterpret_cast<CMORPH025Grid *>(grid)->set_latitudes(reinterpret_cast<
            InputCMORPH025GridStream *>(istream.get())->start_latitude(),
            reinterpret_cast<InputCMORPH025GridStream *>(istream.get())->
            end_latitude());
      } else if (metautils::args.data_format == "gpcp") {
        grid->fill(b.get(), Grid::FULL_GRID);
      } else {
        grid->fill(b.get(), Grid::HEADER_ONLY);
      }
    }
    auto def =grid->definition();
      if (metautils::args.data_format == "gpcp" && reinterpret_cast<GPCPGrid *>(
          grid)->is_empty_grid()) {
      def.type = Grid::Type::not_set;
    }
    if (def.type != Grid::Type::not_set) {
      auto dt1 = grid->valid_date_time();
      auto gkey = itos(static_cast<int>(def.type)) + "<!>";
      if (def.type != Grid::Type::sphericalHarmonics) {
        auto dim = grid->dimensions();
        gkey += itos(dim.x) + "<!>" + itos(dim.y) + "<!>" + ftos(def.
            slatitude, 3) + "<!>" + ftos(def.slongitude, 3) + "<!>" + ftos(def.
            elatitude, 3) + "<!>" + ftos(def.elongitude, 3) + "<!>" + ftos(def.
            loincrement, 3) + "<!>";
        switch (def.type) {
          case Grid::Type::gaussianLatitudeLongitude: {
            gkey += itos(def.num_circles);
            break;
          }
          default: {
            gkey += ftos(def.laincrement, 3);
          }
        }
        if (def.type == Grid::Type::polarStereographic || def.type == Grid::
            Type::lambertConformal) {
          auto pole = def.projection_flag ==  0 ? "N" : "S";
          gkey += string("<!>") + pole;
          if (def.type == Grid::Type::lambertConformal) {
            gkey += "<!>" + ftos(def.stdparallel1, 3) + "<!>" + ftos(def.
                stdparallel2);
          }
        }
      } else {
        gkey += itos(def.trunc1) + "<!>" + itos(def.trunc2) + "<!>" + itos(
            def.trunc3);
      }
      string pe_key = "";
      string pkey = "";
      string le_key = "";
      string ekey = "";
      auto fhr = grid->forecast_time() / 10000;
      DateTime dt2;
      if (metautils::args.data_format == "grib" || metautils::args.data_format
          == "grib0") {
        if (message->edition() == 0) {
          metautils::args.data_format = "grib0";
        }
        dt2 = dt1;
        dt1 = grid->forecast_date_time();
        le_key = ftos(grid->source()) + "-" + ftos((reinterpret_cast<
            GRIBGrid *>(grid))->sub_center_id());
        pe_key = le_key;
        if (metautils::args.data_format == "grib") {
          pe_key += "." + ftos((reinterpret_cast<GRIBGrid *>(grid))->
              parameter_table_code());
        }
        pe_key += ":" + ftos(grid->parameter());
        gkey += "<!>" + (reinterpret_cast<GRIBGrid *>(grid))->
            product_description();
        pkey = le_key + ":" + itos((reinterpret_cast<GRIBGrid *>(grid))->
            process());
        auto e = grid->ensemble_data();
        if (!e.fcst_type.empty()) {
          ekey = e.fcst_type + "<!>" + pkey;
          if (!e.id.empty()) {
            ekey += "%" + e.id;
          }
          if (e.total_size > 0) {
            ekey += "<!>" + itos(e.total_size);
          }
        }
        auto l = itos((reinterpret_cast<GRIBGrid *>(grid))->first_level_type());
        string f;
        if (metautils::args.data_format == "grib" || metautils::args.data_format
            == "grib1") {
          f = "WMO_GRIB1";
        } else {
          f = "WMO_GRIB0";
        }
        if (lmap.level_is_a_layer(f, l, le_key) < 0) {
          log_error2("no entry for " + l + ", '" + le_key + "' in level map on "
              + grid->valid_date_time().to_string(), F, "grid2xml", USER);
        }
        le_key += ",";
        if (lmap.level_is_a_layer(f, l, le_key)) {
          le_key += l + ":" + ftos(grid->first_level_value(), 5) + ":" + ftos(
              grid->second_level_value(), 5);
        } else {
          le_key += l + ":" + ftos(grid->first_level_value(), 5);
        }
        auto idx = gkey.rfind("<!>");
        if (g_inv.stream.is_open()) {
          auto i = lltos((reinterpret_cast<InputGRIBStream *>(istream.get()))->
              current_record_offset()) + "|" + itos(message->length()) + "|" +
              grid->valid_date_time().to_string("%Y%m%d%H%MM");
          auto key = gkey.substr(idx + 3);
          if (U_map.find(key) == U_map.end()) {
            U_map.emplace(key, U_map.size());
            U_lst.emplace_back(key);
          }
          i += "|" + itos(U_map[key]);
          key = substitute(gkey.substr(0, idx), "<!>", ",");
          if (G_map.find(key) == G_map.end()) {
            G_map.emplace(key, G_map.size());
            G_lst.emplace_back(key);
          }
          i += "|" + itos(G_map[key]);
          key = le_key;
          if (L_map.find(key) == L_map.end()) {
            L_map.emplace(key, L_map.size());
            L_lst.emplace_back(key);
          }
          i += "|" + itos(L_map[key]);
          key = pe_key;
          if (P_map.find(key) == P_map.end()) {
            P_map.emplace(key, P_map.size());
            P_lst.emplace_back(key);
          }
          i += "|" + itos(P_map[key]);
          key=pkey;
          i += "|";
          if (!key.empty()) {
            if (R_map.find(key) == R_map.end()) {
              R_map.emplace(key, R_map.size());
              R_lst.emplace_back(key);
            }
            i += itos(R_map[key]);
          }
          key = substitute(ekey, "<!>", ",");
          i += "|";
          if (!key.empty()) {
            if (E_map.find(key) == E_map.end()) {
              E_map.emplace(key, E_map.size());
              E_lst.emplace_back(key);
            }
            i += itos(E_map[key]);
          }
          ilst.emplace_back(i);
        }
      } else if (metautils::args.data_format == "grib2") {
//        dt2=dt1;
//dt1=grid->reference_date_time().hours_added(grid->forecast_time() / 10000);
        le_key = ftos(grid->source()) + "-" + ftos((reinterpret_cast<
            GRIB2Grid *>(grid))->sub_center_id());
        pe_key = le_key + "." + ftos((reinterpret_cast<GRIB2Grid *>(grid))->
            parameter_table_code()) + "-" + ftos((reinterpret_cast<GRIB2Grid *>(
            grid))->local_table_code()) + ":" + ftos((reinterpret_cast<
            GRIB2Grid *>(grid))->discipline()) + "." + ftos((reinterpret_cast<
            GRIB2Grid *>(grid))->parameter_category()) + "." + ftos(grid->
            parameter());
        pkey = le_key + ":" + itos((reinterpret_cast<GRIBGrid *>(grid))->
            process()) + "." + itos((reinterpret_cast<GRIB2Grid *>(grid))->
            background_process()) + "." + itos((reinterpret_cast<GRIB2Grid *>(
            grid))->forecast_process());
        auto level_type = itos((reinterpret_cast<GRIB2Grid *>(grid))->
            first_level_type());
        auto n = (reinterpret_cast<GRIB2Grid *>(grid))->second_level_type();
        le_key += ",";
        if (n != 255) {
          le_key += level_type + "-" + itos(n) + ":";
          if (!floatutils::myequalf(grid->first_level_value(),
              Grid::MISSING_VALUE)) {
            le_key += ftos(grid->first_level_value(), 5);
          } else {
            le_key += "0";
          }
          le_key += ":";
          if (!floatutils::myequalf(grid->second_level_value(),
              Grid::MISSING_VALUE)) {
            le_key += ftos(grid->second_level_value(), 5);
          } else {
            le_key += "0";
          }
        } else {
          le_key += level_type + ":";
          if (!floatutils::myequalf(grid->first_level_value(),
              Grid::MISSING_VALUE)) {
            le_key += ftos(grid->first_level_value(), 5);
          } else {
            le_key += "0";
          }
        }
        short p =( reinterpret_cast<GRIB2Grid *>(grid))->product_type();
        gkey += "<!>" + (reinterpret_cast<GRIB2Grid *>(grid))->
            product_description();
        dt1 = grid->forecast_date_time();
        dt2 = grid->valid_date_time();
        switch (p) {
          case 0:
          case 1:
          case 2: {
            if (p == 1) {
              auto ensdata = grid->ensemble_data();
              if (!ensdata.fcst_type.empty()) {
                ekey = ensdata.fcst_type + "<!>" + pkey;
                if (!ensdata.id.empty()) {
                  ekey += "%" + ensdata.id;
                }
                if (ensdata.total_size > 0) {
                  ekey += "<!>" + itos(ensdata.total_size);
                }
              }
            }
            break;
          }
          case 8:
          case 11:
          case 12: {
            if (p == 11 || p == 12) {
              auto ensdata = grid->ensemble_data();
              if (!ensdata.fcst_type.empty()) {
                ekey = ensdata.fcst_type + "<!>" + pkey;
                if (!ensdata.id.empty()) {
                  ekey += "%" + ensdata.id;
                }
                if (ensdata.total_size > 0) {
                  ekey += "<!>" + itos(ensdata.total_size);
                }
              }
            }
            break;
          }
        }
        auto idx = gkey.rfind("<!>");
        if (g_inv.stream.is_open()) {
          auto inv_line = lltos((reinterpret_cast<InputGRIBStream *>(istream.
              get()))->current_record_offset()) + "|" + lltos(message->length())
              + "|" + dt2.to_string("%Y%m%d%H%MM");
          auto key = gkey.substr(idx + 3);
          if (U_map.find(key) == U_map.end()) {
            U_map.emplace(key, U_map.size());
            U_lst.emplace_back(key);
          }
          inv_line += "|" + itos(U_map[key]);
          key = substitute(gkey.substr(0, idx), "<!>", ",");
          if (G_map.find(key) == G_map.end()) {
            G_map.emplace(key, G_map.size());
            G_lst.emplace_back(key);
          }
          inv_line += "|" + itos(G_map[key]);
          key = le_key;
          if (L_map.find(key) == L_map.end()) {
            L_map.emplace(key, L_map.size());
            L_lst.emplace_back(key);
          }
          inv_line += "|" + itos(L_map[key]);
          key = pe_key;
          if (P_map.find(key) == P_map.end()) {
            P_map.emplace(key, P_map.size());
            P_lst.emplace_back(key);
          }
          inv_line += "|" + itos(P_map[key]);
          key = pkey;
          inv_line += "|";
          if (!key.empty()) {
            if (R_map.find(key) == R_map.end()) {
              R_map.emplace(key, R_map.size());
              R_lst.emplace_back(key);
            }
            inv_line += itos(R_map[key]);
          }
          key = substitute(ekey, "<!>", ",");
          inv_line += "|";
          if (!key.empty()) {
            if (E_map.find(key) == E_map.end()) {
              E_map.emplace(key, E_map.size());
              E_lst.emplace_back(key);
            }
            inv_line += itos(E_map[key]);
          }
          ilst.emplace_back(inv_line);
        }
      } else if (metautils::args.data_format == "oct" || metautils::args.
          data_format == "tropical" || metautils::args.data_format == "ll" ||
          metautils::args.data_format == "slp" || metautils::args.data_format ==
          "navy") {
        dt2 = dt1;
        pe_key = itos(grid->parameter());
        if (fhr == 0) {
          if (dt1.day() == 0) {
            dt1.add_days(1);
            dt2.add_months(1);
            gkey += "<!>Monthly Mean of Analyses";
          } else {
            gkey += "<!>Analysis";
          }
        } else {
          if (dt1.day() == 0) {
            dt1.add_days(1);
            dt2.add_months(1);
            gkey += "<!>Monthly Mean of " + itos(fhr) + "-hour Forecasts";
          } else {
            gkey += "<!>" + itos(fhr) + "-hour Forecast";
          }
          dt1.add_hours(fhr);
        }
        if (floatutils::myequalf(grid->second_level_value(), 0.)) {
          le_key = "0:" + itos(grid->first_level_value());
        } else {
          le_key = "1:" + itos(grid->first_level_value()) + ":" + itos(grid->
              second_level_value());
        }
      } else if (metautils::args.data_format == "jraieeemm") {
        dt2 = dt1;
        dt2.set_day(dateutils::days_in_month(dt1.year(), dt1.month()));
        dt2.add_hours(18);
        pe_key = itos(grid->parameter());
        gkey += "<!>Monthly Mean (4 per day) of ";
        if (grid->forecast_time() == 0) {
          gkey += "Analyses";
        } else {
          gkey += "Forecasts";
          auto f = (reinterpret_cast<JRAIEEEMMGrid *>(grid))->time_range();
          if (f == "MN  ") {
            gkey += " of 6-hour Average";
          } else if (f == "AC  ") {
            gkey += " of 6-hour Accumulation";
          } else if (f == "MAX ") {
            gkey += " of 6-hour Maximum";
          } else if (f == "MIN ") {
            gkey += " of 6-hour Minimum";
          }
        }
        le_key = itos(grid->first_level_type()) + ":" + ftos(grid->
            first_level_value(), 5);
      } else if (metautils::args.data_format == "ussrslp") {
        pe_key = itos(grid->parameter());
        if (fhr == 0) {
          gkey += "<!>Analysis";
        } else {
          gkey += "<!>" + itos(fhr) + "-hour Forecast";
        }
      } else if (metautils::args.data_format == "on84") {
        dt2 = dt1;
        pe_key = itos(grid->parameter());
        switch ((reinterpret_cast<ON84Grid *>(grid))->time_marker()) {
          case 0: {
            gkey += "<!>Analysis";
            break;
          }
          case 3: {
            fhr -= (reinterpret_cast<ON84Grid *>(grid))->F2();
            gkey += "<!>";
            if (fhr > 0) {
              gkey += itos(fhr + (reinterpret_cast<ON84Grid *>(grid))->F2()) +
                  "-hour Forecast of ";
            }
            gkey += itos((reinterpret_cast<ON84Grid *>(grid))->F2()) +
                "-hour Accumulation";
            break;
          }
          case 4: {
            dt1.set_day(1);
            short n = dateutils::days_in_month(dt2.year(), dt2.month());
            if (grid->number_averaged() == n) {
              gkey += "<!>Monthly Mean of Analyses";
            } else {
              gkey += "<!>Mean of " + itos(grid->number_averaged()) +
                  " Analyses";
            }
            dt2.set_day(grid->number_averaged());
            break;
          }
          default: {
            log_error2("ON84 time marker " + itos((reinterpret_cast<ON84Grid *>(
                grid))->time_marker()) + " not recognized", F, "grid2xml",
                USER);
          }
        }
        pkey = itos((reinterpret_cast<ON84Grid *>(grid))->run_marker()) + "." +
            itos((reinterpret_cast<ON84Grid *>(grid))->generating_program());
        if (floatutils::myequalf(grid->second_level_value(), 0.)) {
          switch ((reinterpret_cast<ON84Grid *>(grid))->first_level_type()) {
            case 144:
            case 145:
            case 146:
            case 147:
            case 148: {
              le_key = itos((reinterpret_cast<ON84Grid *>(grid))->
                  first_level_type()) + ":1:0";
              break;
            }
            default: {
              le_key = itos((reinterpret_cast<ON84Grid *>(grid))->
                  first_level_type()) + ":" + ftos(grid->first_level_value(),
                  5);
            }
          }
        } else {
          le_key = itos((reinterpret_cast<ON84Grid *>(grid))->
              first_level_type()) + ":" + ftos(grid->first_level_value(), 5) +
              ":" + ftos(grid->second_level_value(), 5);
        }
      } else if (metautils::args.data_format == "cgcm1") {
        pe_key = (reinterpret_cast<CGCM1Grid *>(grid))->parameter_name();
        trim(pe_key);
        replace_all(pe_key, "\"", "&quot;");
        dt2 = dt1;
        if (fhr == 0) {
          if (dt1.day() > 0) {
            gkey += "<!>Analysis";
          } else {
            gkey += "<!>Monthly Mean of Analyses";
            dt1.set_day(1);
            dt2.set_day(dateutils::days_in_month(dt2.year(), dt2.month()));
          }
        } else {
          gkey += "<!>" + itos(fhr) + "-hour Forecast";
        }
        le_key = itos(grid->first_level_value());
      } else if (metautils::args.data_format == "cmorph025") {
        pe_key = (reinterpret_cast<InputCMORPH025GridStream *>(istream.get()))->
            parameter_code();
        trim(pe_key);
        dt2 = dt1;
        gkey += "<!>Analysis";
        le_key = "surface";
      } else if (metautils::args.data_format == "cmorph8km") {
        pe_key = "CMORPH";
        dt2 = dt1;
        dt1 = grid->reference_date_time();
        gkey += "<!>30-minute Average (initial+0 to initial+30)";
        le_key = "surface";
      } else if (metautils::args.data_format == "gpcp") {
        pe_key = reinterpret_cast<GPCPGrid *>(grid)->parameter_name();
        dt2 = dt1;
        dt1 = grid->reference_date_time();
        size_t h = dt2.hours_since(dt1);
        if (h == 24) {
          gkey += "<!>Daily Mean";
        } else if (h == dateutils::days_in_month(dt1.year(), dt1.month()) *
            24) {
          gkey += "<!>Monthly Mean";
        } else {
          log_error2("can't figure out gridded product type", F, "grid2xml",
              USER);
        }
        le_key = "surface";
      }
      if (grid_table.find(gkey) == grid_table.end()) {
        gatherxml::markup::GrML::ParameterEntry pe;
        pe.start_date_time = dt1;
        pe.end_date_time = dt2;
        pe.num_time_steps = 1;
        gatherxml::markup::GrML::LevelEntry le;
        le.parameter_code_table.emplace(pe_key, pe);
        gatherxml::markup::GrML::GridEntry ge;
        ge.level_table.emplace(le_key, le);
        if (!pkey.empty()) {
          ge.process_table.emplace(pkey);
        }
        if (!ekey.empty()) {
          ge.ensemble_table.emplace(ekey);
        }
        grid_table.emplace(gkey, ge);
      } else {
        auto& ge = grid_table[gkey];
        if (ge.level_table.find(le_key) == ge.level_table.end()) {
          gatherxml::markup::GrML::ParameterEntry pe;
          pe.start_date_time = dt1;
          pe.end_date_time = dt2;
          pe.num_time_steps = 1;
          gatherxml::markup::GrML::LevelEntry le;
          le.parameter_code_table.emplace(pe_key, pe);
          ge.level_table.emplace(le_key, le);
        } else {
          auto& le = ge.level_table[le_key];
          if (le.parameter_code_table.find(pe_key) == le.parameter_code_table.
              end()) {
            gatherxml::markup::GrML::ParameterEntry pe;
            pe.start_date_time = dt1;
            pe.end_date_time = dt2;
            pe.num_time_steps = 1;
            le.parameter_code_table.emplace(pe_key, pe);
          } else {
            auto& pe = le.parameter_code_table[pe_key];
            if (dt1 < pe.start_date_time) {
              pe.start_date_time = dt1;
            }
            if (dt2 > pe.end_date_time) {
              pe.end_date_time = dt2;
            }
            ++pe.num_time_steps;
          }
        }
        if (!pkey.empty() && ge.process_table.find(pkey) == ge.
            process_table.end()) {
          ge.process_table.emplace(pkey);
        }
        if (!ekey.empty() && ge.ensemble_table.find(ekey) == ge.
            ensemble_table.end()) {
          ge.ensemble_table.emplace(ekey);
        }
      }
    }
  }
  istream->close();
  if (grid_table.size() == 0) {
    log_error2("Terminating - no grids found so no content metadata will be "
        "generated", F, "grid2xml", USER);
  }
  if (ilst.size() > 0) {
    for (const auto& key : U_lst) {
      g_inv.stream << "U<!>" << U_map[key] << "<!>" << key << endl;
    }
    for (const auto& key : G_lst) {
      g_inv.stream << "G<!>" << G_map[key] << "<!>" << key << endl;
    }
    for (const auto& key : L_lst) {
      g_inv.stream << "L<!>" << L_map[key] << "<!>" << key << endl;
    }
    for (const auto& key : P_lst) {
      g_inv.stream << "P<!>" << P_map[key] << "<!>" << key << endl;
    }
    for (const auto& key : R_lst) {
      g_inv.stream << "R<!>" << R_map[key] << "<!>" << key << endl;
    }
    for (const auto& key : E_lst) {
      g_inv.stream << "E<!>" << E_map[key] << "<!>" << key << endl;
    }
    g_inv.stream << "-----" << endl;
    for (const auto& l : ilst)
      g_inv.stream << l << endl;
  }
}

extern "C" void segv_handler(int) {
  clean_up();
  metautils::cmd_unregister();
  log_error2("core dump", "segv_handler()", "grid2xml", USER);
}

extern "C" void int_handler(int) {
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc, char **argv) {
  if (argc < 4) {
    cerr << "usage: grid2xml -f format -d [ds]nnn.n [options...] path" << endl;
    cerr << endl;
    cerr << "required (choose one):" << endl;
    cerr << "-f cgcm1         Canadian CGCM1 Model ASCII format" << endl;
    cerr << "-f cmorph025     CPC Morphing Technique 0.25-degree precipitation "
        "format" << endl;
    cerr << "-f cmorph8km     CPC Morphing Technique 8-km precipitation format"
        << endl;
    cerr << "-f gpcp          GPCP grid format" << endl;
    cerr << "-f grib          GRIB0 and GRIB1 grid formats" << endl;
    cerr << "-f grib2         GRIB2 grid format" << endl;
    cerr << "-f jraieeemm     JRA IEEE Monthly Mean grid format" << endl;
    cerr << "-f ll            DSS 5-degree Lat/Lon grid format" << endl;
    cerr << "-f navy          DSS Navy grid format" << endl;
    cerr << "-f noaaoi2       NOAA OI2 SST format" << endl;
    cerr << "-f oct           DSS Octagonal grid format" << endl;
    cerr << "-f on84          NCEP Office Note 84 grid format" << endl;
    cerr << "-f slp           DSS Sea-Level Pressure grid format" << endl;
    cerr << "-f tropical      DSS Tropical grid format" << endl;
    cerr << "-f ussrslp       USSR Sea-Level Pressure grid format" << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data "
        "file belongs" << endl;
    cerr << endl;
    cerr << "options:" << endl;
    cerr << "-r/-R            regenerate/don't regenerate the dataset webpage "
        "(default is -r)" << endl;
    cerr << "-s/-S            do/don't update the dataset summary information "
        "(default is -s)" << endl;
    if (USER == "dattore") {
      cerr << "-u/-U            do/don't update the database (default is -u)" <<
          endl;
      cerr << "-t <path>        path where temporary files should be created" <<
          endl;
      cerr << "-I               inventory only; no content metadata generated"
          << endl;
      cerr << "-OO              overwrite only - when content metadata already "
          "exists, the" << endl;
      cerr << "                 default is to first delete existing metadata; "
          "this option saves" << endl;
      cerr << "                 time by overwriting without the delete" << endl;
    }
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "<path>           full URL of the file to read" << endl;
    cerr << "                   - URLs must begin with \"https://rda.ucar.edu\""
        << endl;
    exit(1);
  }
  signal(SIGSEGV, segv_handler);
  signal(SIGINT, int_handler);
  atexit(clean_up);
  auto d = '!';
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, d);
  metautils::read_config("grid2xml", USER);
  gatherxml::parse_args(d);
  metautils::cmd_register("grid2xml", USER);
  if (!metautils::args.overwrite_only && !metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("GrML");
  }
  Timer tmr;
  tmr.start();
  scan_file();
  if (!metautils::args.inventory_only) {
    auto tdir = gatherxml::markup::GrML::write(grid_table, "grid2xml", USER);
    if (metautils::args.update_db) {
      string flags;
      if (!metautils::args.update_summary) {
        flags += " -S";
      }
      if (!metautils::args.regenerate) {
        flags += " -R";
      }
      if (!tdir.empty()) {
        flags += " -t " + tdir;
      }
      if (regex_search(metautils::args.path, regex("^https://rda.ucar.edu"))) {
        flags += " -wf";
      } else {
        log_error2("Terminating - invalid path '" + metautils::args.path + "'",
            "main()", "grid2xml", USER);
      }
      stringstream oss, ess;
      if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
          metautils::args.dsid + " " + flags + " " + metautils::args.filename +
          ".GrML", oss, ess) != 0) {
        string e = ess.str();
        trim(e);
        log_error2(e, "main(): running scm", "grid2xml", USER);
      }
    } else if (metautils::args.dsid == "test") {
      cout << "Output is in:" << endl;
      cout << "  " << tdir << "/" << metautils::args.filename << ".GrML" <<
          endl;
    }
  }
  if (g_inv.stream.is_open()) {
    gatherxml::fileInventory::close(g_inv.file, g_inv.dir, g_inv.stream, "GrML",
        true, metautils::args.update_summary, "grid2xml", USER);
  }
  tmr.stop();
  metautils::log_warning("execution time: " + ftos(tmr.elapsed_time()) +
      " seconds", "gatherxml.time", USER);
  return 0;
}
