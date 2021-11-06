#include <iostream>
#include <stdlib.h>
#include <string>
#include <deque>
#include <regex>
#include <sstream>
#include <pthread.h>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using metautils::log_warning;
using miscutils::this_function_label;
using std::cout;
using std::endl;
using std::stoi;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::unique_ptr;
using strutils::ftos;
using strutils::itos;
using strutils::split;
using strutils::substitute;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;

struct ThreadStruct {
  ThreadStruct() : query(), imagetag(), tid(-1) { }

  MySQL::LocalQuery query;
  string imagetag;
  pthread_t tid;
};

MySQL::Server server;
string dsnum2;
char g_bitmap[60][121];
const string USER = getenv("USER");
string myerror = "";
string mywarning = "";
const string PLT_EXT = ".py";
const string IMG_EXT = ".png";

void write_plot_head(unique_ptr<TempFile>& plot_file, unique_ptr<TempFile>&
    image_file) {
  plot_file->writeln("import cartopy.crs as ccrs");
  plot_file->writeln("import cartopy.feature as cfeature");
  plot_file->writeln("import matplotlib.pyplot as plt");
  plot_file->writeln("import matplotlib.ticker as mticker");
  plot_file->writeln("ax = plt.axes(projection=ccrs.PlateCarree())");
  plot_file->writeln("ax.set_extent([-180, 180, -90, 90], ccrs.PlateCarree())");
  plot_file->writeln("ax.add_feature(cfeature.LAND, color='#54755a')");
  plot_file->writeln("ax.add_feature(cfeature.OCEAN, color='#27aae4')");
  plot_file->writeln("gl = ax.gridlines(crs=ccrs.PlateCarree(), color="
      "'#54755a')");
  plot_file->writeln("gl.ylocator = mticker.FixedLocator([-90, -60, -30, 0, "
      "30, 60, 90])");
  plot_file->writeln("gl.xlocator = mticker.FixedLocator([-180, -150, -120, "
      "-90, -60, -30, 0, 30, 60, 90, 120, 150, 180])");
}

void *thread_plot(void *tnc) {
  const static string F = this_function_label(__func__);
  ThreadStruct *t = reinterpret_cast<ThreadStruct *>(tnc);
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (t->query.submit(srv) < 0) {
    log_error2(t->query.error(), F, "gsi", USER);
  }

  // set up a 3-degree by 3-degree bitmap
  char wrotemap[60][121];
  for (size_t n = 0; n < 60; ++n) {
    for (size_t m = 0; m < 121; ++m) {
      wrotemap[n][m] = 0;
    }
  }
  auto plt_p = unique_ptr<TempFile>(new TempFile(metautils::directives.
      temp_path, PLT_EXT));
  auto img_p = unique_ptr<TempFile>(new TempFile(metautils::directives.
      temp_path, IMG_EXT));
  write_plot_head(plt_p, img_p);
  stringstream xss, yss;
  xss.setf(std::ios::fixed);
  xss.precision(1);
  yss.setf(std::ios::fixed);
  yss.precision(1);
  for (const auto& row : t->query) {
    auto y = stoi(row[0]);
    y = (y - 1) / 3;
    if (wrotemap[y][120] < 120) {
      for (size_t x = 0; x < row[1].length(); ++x) {
        if (row[1][x] != '0' && wrotemap[y][x] == 0) {
          if (!xss.str().empty()) {
            xss << ", ";
            yss << ", ";
          }
          xss << -178.5 + x * 3;
          yss << -88.5 + y * 3;
          if (wrotemap[y][x] == 0) {
            ++wrotemap[y][120];
          }
          wrotemap[y][x] = 1;
          g_bitmap[y][x] = 1;
          g_bitmap[y][120] = 1;
        }
      }
    }
  }
  srv.disconnect();
  plt_p->writeln("lons = [" + xss.str() + "]");
  plt_p->writeln("lats = [" + yss.str() + "]");
  plt_p->writeln("plt.plot(lons, lats, marker='.', color='#ffffdc', linewidth="
      "0)");
  plt_p->writeln("plt.savefig('" + img_p->name() + "')");
  plt_p->close();
  auto tdir = unique_ptr<TempDir>(new TempDir);
  if (!tdir->create(metautils::directives.temp_path)) {
    log_error2("can't create temporary directory", F, "gsi", USER);
  }
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + tdir->name() + "/datasets/ds" + metautils::
      args.dsnum + "/metadata", oss, ess) < 0) {
    log_error2("can't create directory tree", F, "gsi", USER);
  }
  mysystem2("/bin/tcsh -c \"conda run -p /glade/u/home/rdadata/conda-envs/"
      "rdaviz python " + plt_p->name() + "; convert -trim +repage -resize "
      "360x180 " + img_p->name() + " " + tdir->name() + "/datasets/ds" +
      metautils::args.dsnum + "/metadata/spatial_coverage." + t->imagetag +
      ".gif\"", oss, ess);
  string e;
  if (unixutils::rdadata_sync(tdir->name(), "datasets/ds" + metautils::args.
      dsnum + "/metadata/", "/data/web", metautils::directives.rdadata_home, e)
      < 0) {
    log_warning("rdadata_sync errors: '" + e + "'", "gsi", USER);
  }
  return nullptr;
}

void generate_graphics(MySQL::LocalQuery& query, string type, string table,
    string gindex) {
  const static string F = this_function_label(__func__);
  if (query.submit(server) < 0) {
    log_error2(query.error(), F, "gsi", USER);
  }
  for (size_t n = 0; n < 60; ++n) {
    for (size_t m = 0; m < 121; ++m) {
      g_bitmap[n][m] = 0;
    }
  }
  auto tnc = unique_ptr<ThreadStruct[]>(new ThreadStruct[query.num_rows()]);
  size_t nn = 0;
  for (const auto& row : query) {
    if (gindex.empty()) {
      if (type == "obs") {
        tnc[nn].query.set("select distinct box1d_row, box1d_bitmap from " +
            table + " where dsid = '" + metautils::args.dsnum + "' and "
            "observationType_code = " + row[0] + " and platformType_code = " +
            row[1] + " and format_code = " + row[4] + " order by box1d_row");
        tnc[nn].imagetag = substitute(row[5], " ", "_") + "_web_" + row[2] + "."
            + row[3];
      } else if (type == "fix") {
        tnc[nn].query.set("select box1d_row, box1d_bitmap from " + table +
            " where dsid = '" + metautils::args.dsnum + "' and "
            "classification_code = " + row[0] + " and format_code = " + row[2] +
            " order by box1d_row");
        tnc[nn].imagetag = substitute(row[3], " ", "_") + "_web_" + row[1];
      }
    } else {
      if (type == "obs") {
        tnc[nn].query.set("select distinct box1d_row, box1d_bitmap from WObML."
            "ds" + dsnum2 + "_webfiles2 as p left join dssdb.wfile as x on (x."
            "dsid = 'ds" + metautils::args.dsnum + "' and x.wfile = p.webID) "
            "left join " + table + " as t on t.webID_code = p.code where x."
            "gindex = " + gindex + " and observationType_code = " + row[0] +
            " and platformType_code = " + row[1] + " and p.format_code = " +
            row[4] + " order by box1d_row");
        tnc[nn].imagetag = substitute(row[5], " ", "_") + "_web_gindex_" +
            gindex + "_" + row[2] + "." + row[3];
      } else if (type == "fix") {
        tnc[nn].query.set("select box1d_row, box1d_bitmap from WFixML.ds" +
            dsnum2 + "_webfiles2 as p left join dssdb.wfile as x on x.wfile = "
            "p.webID left join " + table + " as t on t.webID_code = p.code "
            "where x.gindex = " + gindex + " and classification_code = " + row[
            0] + " and p.format_code = " + row[2] + " order by box1d_row");
        tnc[nn].imagetag = substitute(row[3], " ", "_") + "_web_gindex_" +
            gindex + "_" + row[1];
      }
    }
    pthread_create(&tnc[nn].tid, nullptr, thread_plot, reinterpret_cast<void *>(
        &tnc[nn]));
    ++nn;
  }
  for (size_t m = 0; m < nn; ++m) {
    pthread_join(tnc[m].tid, nullptr);
  }
  auto plt_p = unique_ptr<TempFile>(new TempFile(metautils::directives.
      temp_path, PLT_EXT));
  auto img_p = unique_ptr<TempFile>(new TempFile(metautils::directives.
      temp_path, IMG_EXT));
  write_plot_head(plt_p, img_p);
  stringstream xss, yss;
  xss.setf(std::ios::fixed);
  xss.precision(1);
  yss.setf(std::ios::fixed);
  yss.precision(1);
  for (size_t n = 0; n < 60; ++n) {
    if (g_bitmap[n][120] == 1) {
      for (size_t m = 0; m < 120; ++m) {
        if (g_bitmap[n][m] == 1) {
          if (!xss.str().empty()) {
            xss << ", ";
            yss << ", ";
          }
          xss << -178.5 + m * 3;
          yss << -88.5 + n * 3;
        }
      }
    }
  }
  plt_p->writeln("lons = [" + xss.str() + "]");
  plt_p->writeln("lats = [" + yss.str() + "]");
  plt_p->writeln("plt.plot(lons, lats, marker='.', color='#ffffdc', linewidth="
      "0)");
  plt_p->writeln("plt.savefig('" + img_p->name() + "')");
  plt_p->close();
  auto tdir = unique_ptr<TempDir>(new TempDir);
  if (!tdir->create(metautils::directives.temp_path)) {
    log_error2("can't create temporary directory", F, "gsi", USER);
  }
  stringstream oss, ess;
  if (mysystem2("/bin/mkdir -p " + tdir->name() + "/datasets/ds" + metautils::
      args.dsnum + "/metadata", oss, ess) < 0) {
    log_error2("can't create directory tree", F, "gsi", USER);
  }
  mysystem2("/bin/tcsh -c \"conda run -p /glade/u/home/rdadata/conda-envs/"
      "rdaviz python " + plt_p->name() + "; convert -trim +repage -resize "
      "360x180 " + img_p->name() + " " + tdir->name() + "/datasets/ds" +
      metautils::args.dsnum + "/metadata/spatial_coverage.gif\"", oss, ess);
  string e;
  if (unixutils::rdadata_sync(tdir->name(),"datasets/ds" + metautils::args.
      dsnum + "/metadata/", "/data/web", metautils::directives.rdadata_home, e)
      < 0) {
    log_warning("rdadata_sync errors: '" + e + "'", "gsi", USER);
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    cout << "usage: gsi [options...] nnn.n" << endl;
    cout << "\noptions:" << endl;
    cout << "-g <gindex>   create graphics only for group index <gindex>" <<
        endl;
    cout << "-N            notify with a message when gsi completes" << endl;
    exit(0);
  }
  static const string F = this_function_label(__func__);
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '!');
  metautils::read_config("gsi", USER);
  string gindex;
  bool notify = false;
  auto sp = split(metautils::args.args_string, "!");
  for (size_t n = 0; n < sp.size() - 1; ++n) {
    if (sp[n] == "-N") {
      notify = true;
    } else if (sp[n] == "-g") {
      gindex = sp[++n];
    }
  }
  metautils::args.dsnum = sp.back();
  dsnum2 = substitute(metautils::args.dsnum, ".", "");
  server.connect(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  if (!server) {
    log_error2("unable to connect to MySQL server on startup", F, "gsi", USER);
  }
  MySQL::LocalQuery query;
  string table;
  if (table_exists(server, "WObML.ds" + dsnum2 + "_dataTypes2")) {
    if (gindex.empty()) {
      query.set("select distinct l.observationType_code, l.platformType_code, "
          "o.obsType, pf.platformType, p.format_code, f.format from WObML.ds" +
          dsnum2 + "_webfiles2 as p left join WObML.ds" + dsnum2 +
          "_dataTypes2 as d on d.webID_code = p.code left join WObML.ds" +
          dsnum2 + "_dataTypesList as l on l.code = d.dataType_code left join "
          "WObML.obsTypes as o on l.observationType_code = o.code left join "
          "WObML.platformTypes as pf on l.platformType_code = pf.code left "
          "join WObML.formats as f on f.code = p.format_code");
      table = "search.obs_data";
    } else {
      query.set("select distinct l.observationType_code, l.platformType_code, "
          "o.obsType, pf.platformType, p.format_code, f.format from WObML.ds" +
          dsnum2 + "_webfiles2 as p left join dssdb.wfile as x on x.wfile "
          "= p.webID left join WObML.ds" + dsnum2 + "_dataTypes2 as d on d."
          "webID_code = p.code left join WObML.ds" + dsnum2 + "_dataTypesList "
          "as l on l.code = d.dataType_code left join WObML.obsTypes as o on l."
          "observationType_code = o.code left join WObML.platformTypes as pf "
          "on l.platformType_code = pf.code left join WObML.formats as f on f."
          "code = p.format_code where x.gindex = " + gindex);
      table = "WObML.ds" + dsnum2 + "_locations";
    }
    generate_graphics(query, "obs", table, gindex);
  }
  if (table_exists(server, "WFixML.ds" + dsnum2 + "_locations")) {
    if (gindex.empty()) {
      query.set("select distinct d.classification_code, c.classification, p."
          "format_code, f.format from WFixML.ds" + dsnum2 + "_webfiles2 as p "
          "left join WFixML.ds" + dsnum2 + "_locations as d on d.webID_code = "
          "p.code left join WFixML.classifications as c on d."
          "classification_code = c.code left join WFixML.formats as f on f."
          "format = p.format_code");
      table = "search.fix_data";
    } else {
      query.set("select distinct d.classification_code, c.classification, p."
          "format_code, f.format from WFixML.ds" + dsnum2 + "_webfiles2 as p "
          "left join dssdb.wfile as x on x.wfile = p.webID left join WFixML."
          "ds" + dsnum2 + "_locations as d on d.webID_code = p.code left join "
          "WFixML.classifications as c on d.classification_code = c.code left "
          "join WFixML.formats as f on f.format = p.format_code where x.gindex "
          "= " + gindex);
      table = "WFixML.ds" + dsnum2 + "_locations";
    }
    generate_graphics(query, "fix", table, gindex);
  }
  if (notify) {
    cout << "gsi has completed successfully" << endl;
  }
}
