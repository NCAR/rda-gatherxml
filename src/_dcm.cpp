#include <iostream>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <gatherxml.hpp>
#include <PostgreSQL.hpp>
#include <pglocks.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::regex;
using std::regex_search;
using std::stoi;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strutils::chop;
using strutils::ds_aliases;
using strutils::ng_gdex_id;
using strutils::replace_all;
using strutils::split;
using strutils::strand;
using strutils::substitute;
using strutils::to_sql_tuple_string;
using unixutils::exists_on_server;
using unixutils::gdex_upload_dir;
using unixutils::gdex_unlink;
using unixutils::mysystem2;
using unixutils::remote_web_file;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
auto env = getenv("USER");
extern const string USER = (env == nullptr) ? "unknown" : env;
string myerror = "";
string mywarning = "";
string g_ds_set;

struct Entry {
  Entry() : key() { }

  string key;
};

struct LocalArgs {
  LocalArgs() : notify(false), keep_xml(false) { }

  bool notify, keep_xml;
} local_args;

struct ThreadStruct {
  ThreadStruct() : strings(), removed_file_index(-1), still_running(true),
      file_removed(false), tid(0) { }

  vector<string> strings;
  int removed_file_index;
  bool still_running, file_removed;
  pthread_t tid;
};

vector<string> g_files;
unordered_set<string> g_web_tindex_set, g_inv_tindex_set;
unordered_set<string> g_web_gindex_set;
bool g_removed_from_wgrml, g_removed_from_wobml, g_removed_from_satml,
    g_removed_from_wfixml;
bool g_create_web_filelist_cache = false, create_inv_filelist_cache = false;

void parse_args(Server& server) {
  auto arg_list = split(metautils::args.args_string, "%");
  for (auto arg = arg_list.begin(); arg != arg_list.end(); ++arg) {
    if (*arg == "-d") {
      ++arg;
      metautils::args.dsid = ng_gdex_id(*arg);
      g_ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
    } else if (*arg == "-G") {
      metautils::args.update_graphics = false;
    } else if (*arg == "-k") {
      local_args.keep_xml = true;
    } else if (*arg == "-N") {
      local_args.notify = true;
    } else if (*arg == "-V") {
      gatherxml::verbose_operation = true;
    } else {
      g_files.emplace_back(*arg);
    }
  }
  if (metautils::args.dsid.empty()) {
    cerr << "Error: no or invalid dataset ID specified" << endl;
    exit(1);
  }
}

string tempdir_name() {
  static TempDir *tdir = nullptr;
  if (tdir == nullptr) {
    tdir = new TempDir;
    if (!tdir->create(metautils::directives.temp_path)) {
      log_error2("unable to create temporary directory", "tempdir_name()",
          "dcm", USER);
    }
  }
  return tdir->name();
}

void copy_version_controlled_data(Server& server, string db, string
    file_id_code) {
  string error;
  auto tnames = table_names(server, db, metautils::args.dsid + "%", error);
  for (auto t : tnames) {
    auto tbl = "\"" + db + "\"." + t;
    if (field_exists(server, tbl, "file_code")) {
      auto vtable = db;
      if (vtable.front() == 'W') {
        vtable = vtable.substr(1);
      }
      vtable = "\"V" + db + "\"." + t;
      if (!table_exists(server, vtable)) {
        if (server.duplicate_table(tbl, vtable) != 0) {
          log_error2("error: '" + server.error() + "' while trying to create " +
              vtable, "copy_version_controlled_data()", "dcm", USER);
        }
      }
      server.command("insert into " + vtable + " select * from " + tbl + " where "
          "file_code = " + file_id_code);
    }
  }
}

void clear_tables_by_file_id(string db, string file_id_code, bool
    is_version_controlled) {
  static const string F = this_function_label(__func__);
  Server local_server(metautils::directives.metadb_config);
  if (!local_server) {
    log_error2("unable to connect to database server while clearing file code "
        + file_id_code + " from " + db, F, "dcm", USER);
  }
  if (is_version_controlled) {
    copy_version_controlled_data(local_server, db, file_id_code);
  }
  string error;
  auto tnames = table_names(local_server, db, metautils::args.dsid + "%",
      error);
  for (auto t : tnames) {
    t = db + "." + t;
    if (field_exists(local_server, t, "file_code")) {
      if (local_server._delete(t, "file_code = " + file_id_code) < 0) {
        log_error2("error: '" + local_server.error() + "' while clearing " + t,
            F, "dcm", USER);
      }
    }
  }
  local_server.disconnect();
}

void clear_grid_cache(Server& server, string db) {
  static const string F = this_function_label(__func__);
  LocalQuery query("select parameter, level_type_codes, min(start_date), max("
      "end_date) from \"" + db + "\"." + metautils::args.dsid + "_agrids2 "
      "group by parameter, level_type_codes");
  if (query.submit(server) < 0) {
    log_error2("error: '" + query.error() + "' while trying to rebuild grid "
        "cache", F, "dcm", USER);
  }
  auto tbl_ref = metautils::args.dsid + "_agrids_cache";
  auto tbl = db + "." + tbl_ref;
  auto lock_it = gatherxml::pglocks::pglocks.find(substitute(tbl, metautils::
      args.dsid, "dnnnnnn"));
  if (lock_it == gatherxml::pglocks::pglocks.end()) {
    log_error2("can't find lock ID for '" + tbl + "'", F, "dcm", USER);
  }
  Transaction tx;
  tx.start(server);
  tx.get_lock(lock_it->second + stoi(metautils::args.dsid.substr(1)), 120);
  auto uflg = strand(3);
  for (const auto& row : query) {
    if (server.insert(
          tbl,
          "parameter, level_type_codes, min_start_date, max_end_date, uflg",
          "'" + row[0] + "', '" + row[1] + "', " + row[2] + ", " + row[3] +
              ", '" + uflg + "'",
          "(parameter, level_type_codes) do update set min_start_date = least("
              "excluded.min_start_date, " + tbl_ref + ".min_start_date), "
              "max_end_date = greatest(excluded.max_end_date, " + tbl_ref +
              ".max_end_date), uflg = excluded.uflg"
          ) < 0) {
      log_error2("error: '" + server.error() + "' while inserting into " + db +
          "." + metautils::args.dsid + "_agrids_cache", F, "dcm", USER);
    }
  }
  server._delete(tbl, "uflg != '" + uflg + "'");
  tx.commit();
}

bool remove_from(string database, string table_ext, string file_field_name,
    string md_directory, string& file, string file_ext, string& file_id_code,
    bool& is_version_controlled) {
  static const string F = this_function_label(__func__);
  is_version_controlled = false;
  string error;
  auto file_table = "\"" + database + "\"." + metautils::args.dsid + table_ext;
  Server local_server(metautils::directives.metadb_config);
  if (!local_server) {
    log_error2("unable to connect to database server while removing  " + file,
        F, "dcm", USER);
  }
  LocalQuery query("code", file_table, file_field_name + " = '" + file + "'");
  if (query.submit(local_server) == 0) {
    if (query.num_rows() == 1) {
      Row row;
      query.fetch_row(row);
      file_id_code = row[0];

      // check for a file moved to the saved area
      query.set("sid", "dssdb.sfile", "dsid in " + g_ds_set + " and sfile = '" +
          file + "'");
      if (query.submit(local_server) < 0) {
        log_error2("error: '" + local_server.error() + "' while trying to "
            "check sfile for '" + file + "'", F, "dcm", USER);
      }
      if (query.num_rows() == 1) {
        is_version_controlled = true;
      }
      if (is_version_controlled) {
        auto vtable = database;
        if (vtable.front() == 'W') {
          vtable = vtable.substr(1);
        }
        vtable = "\"V" + vtable + "\"." + metautils::args.dsid + table_ext;
        if (!table_exists(local_server, vtable)) {
          if (local_server.duplicate_table(file_table, vtable) != 0) {
            log_error2("error: '" + local_server.error() + "' while trying to "
                "create " + vtable, F, "dcm", USER);
          }
        }
        local_server.command("insert into " + vtable + " select * from " +
            file_table + " where code = " + file_id_code);
      }
      if (local_server._delete(file_table, "code = " + file_id_code) < 0) {
        log_error2("error: '" + local_server.error() + "'", F, "dcm", USER);
      }
      if (database == "WGrML" || database == "WObML") {
        auto tables = table_names(local_server, substitute(database, "W", "I"),
            metautils::args.dsid + "_inventory_%", error);
        for (const auto& table : tables) {
          local_server._delete(substitute(database, "W", "I") + ".`" + table +
              "`", "file_code = " + file_id_code);
        }
        if (local_server._delete(substitute(database, "W", "I") + "." +
            metautils::args.dsid + "_inventory_summary", "file_code = " +
            file_id_code) == 0) {
          query.set("select tindex from dssdb.wfile_" + metautils::args.dsid +
              " where wfile = '" + file + "'");
          if (query.submit(local_server) == 0 && query.num_rows() == 1) {
            query.fetch_row(row);
            if (!row[0].empty() && row[0] != "0") {
              g_inv_tindex_set.emplace(row[0]);
            }
          }
          create_inv_filelist_cache = true;
        }
      }
      if (database == "WObML") {
        local_server._delete("I" + database.substr(1) + "." + metautils::args.
            dsid + "_data_types", "file_code = " + file_id_code);
        auto tables = table_names(local_server, "I" + database.substr(1),
            metautils::args.dsid + "_time_series_times_%", error);
        for (const auto& table : tables) {
          local_server._delete("I" + database.substr(1) + "." + table,
              "file_code = " + file_id_code);
        }
      }
      auto md_file = file;
      replace_all(md_file, "/", "%");
      md_file += file_ext;
      if (!local_args.keep_xml) {
        unique_ptr<TempDir> tdir;
        tdir.reset(new TempDir);
        if (!tdir->create(metautils::directives.temp_path)) {
          log_error2("could not create a temporary directory", F, "dcm", USER);
        }
        if (is_version_controlled) {
          stringstream oss, ess;
          if (mysystem2("/bin/mkdir -m 0755 -p " + tdir->name() + "/metadata/" +
               md_directory + "/v", oss, ess) < 0) {
            log_error2("could not create the temporary directory tree", F,
                "dcm", USER);
          }
        }
        short flag = 0;
        if (exists_on_server(metautils::directives.web_server, "/data/web/"
            "datasets/" + metautils::args.dsid + "/metadata/" + md_directory +
            "/" + md_file + ".gz")) {
          flag = 1;
        } else if (exists_on_server(metautils::directives.web_server, "/data/"
            "web/datasets/" + metautils::args.dsid + "/metadata/" + md_directory
            + "/" + md_file)) {
          flag = 2;
        }
        if (file_ext == ".ObML" && flag > 0) {
          string xml_parent;
          if (flag == 1) {
            xml_parent = remote_web_file("https://rda.ucar.edu/datasets/" +
                metautils::args.dsid + "/metadata/" + md_directory + "/" +
                md_file + ".gz", tempdir_name());
            struct stat buf;
            if (stat(xml_parent.c_str(), &buf) == 0) {
              if (system(("gunzip " + xml_parent).c_str()) != 0) {
                log_error2("could not unzip '" + xml_parent + "'", F, "dcm",
                    USER);
              }
              chop(xml_parent, 3);
            }
          } else if (flag == 2) {
            xml_parent = remote_web_file("https://rda.ucar.edu/datasets/" +
                metautils::args.dsid + "/metadata/" + md_directory + "/" +
                md_file, tempdir_name());
          }
          XMLDocument xdoc;
          if (xdoc.open(xml_parent)) {
            auto elist = xdoc.element_list("ObML/observationType/platform/IDs");
            for (const auto& e : elist) {
              if (is_version_controlled) {
                auto f = remote_web_file("https://rda.ucar.edu/datasets/" +
                    metautils::args.dsid + "/metadata/" + md_directory + "/" +
                    e.attribute_value("ref"), tdir->name() + "/metadata/" +
                    md_directory + "/v");
                if (f.empty()) {
                  log_error2("could not get remote file https://rda.ucar.edu/"
                      "datasets/" + metautils::args.dsid + "/metadata/" +
                      md_directory + "/" + e.attribute_value("ref"), F, "dcm",
                      USER);
                }
              }
              if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
                  "/metadata/" + md_directory + "/" + e.attribute_value(
                  "ref"), "", error) < 0) {
                metautils::log_warning("unable to unsync " + e.attribute_value(
                    "ref"), "dcm", USER);
              }
            }
            elist = xdoc.element_list("ObML/observationType/platform/"
                "locations");
            for (const auto& e : elist) {
              if (is_version_controlled) {
                auto f = remote_web_file("https://rda.ucar.edu/datasets/" +
                    metautils::args.dsid + "/metadata/" + md_directory + "/" +
                    e.attribute_value("ref"), tdir->name() + "/metadata/" +
                    md_directory + "/v");
                if (f.empty()) {
                  log_error2("could not get remote file https://rda.ucar.edu/"
                      "datasets/" + metautils::args.dsid + "/metadata/" +
                      md_directory + "/" + e.attribute_value("ref"), F, "dcm",
                      USER);
                }
              }
              if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
                  "/metadata/" + md_directory + "/" + e.attribute_value(
                  "ref"), metautils::directives.gdex_unlink_key, error) < 0) {
                metautils::log_warning("unable to unsync " + e.attribute_value(
                    "ref"), "dcm", USER);
              }
            }
            xdoc.close();
          }
        }
        if (md_directory == "wfmd") {
          if (exists_on_server(metautils::directives.web_server, "/data/web/"
              "datasets/" + metautils::args.dsid + "/metadata/inv/" + md_file +
              "_inv.gz")) {
            if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
                "/metadata/inv/" + md_file + "_inv.gz", metautils::directives.
                gdex_unlink_key, error) < 0) {
              metautils::log_warning("unable to unsync " + md_file + "_inv",
                  "dcm", USER);
            }
          } else if (exists_on_server(metautils::directives.web_server, "/data/"
              "web/datasets/" + metautils::args.dsid + "/metadata/inv/" +
              md_file + "_inv")) {
            if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
                "/metadata/inv/" + md_file + "_inv", metautils::directives.
                gdex_unlink_key, error) < 0) {
              metautils::log_warning("unable to unsync " + md_file + "_inv",
                  "dcm", USER);
            }
          }
          if (exists_on_server(metautils::directives.web_server, "/data/web/"
              "datasets/" + metautils::args.dsid + "/metadata/wms/" + md_file +
              ".gz")) {
            if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
                "/metadata/wms/" + md_file + ".gz", metautils::directives.
                gdex_unlink_key, error) < 0) {
              metautils::log_warning("unable to unsync wms file " + md_file,
                  "dcm", USER);
            }
          }
        }
        if (flag == 1) {
          md_file += ".gz";
        }
        if (is_version_controlled) {
          auto f = remote_web_file("https://rda.ucar.edu/datasets/" +
              metautils::args.dsid + "/metadata/" + md_directory + "/" +
              md_file, tdir->name() + "/metadata/" + md_directory + "/v");
          if (!f.empty()) {
            string error;
            if (gdex_upload_dir(tdir->name(), "metadata/" + md_directory +
                "/v/", "/data/web/datasets/" + metautils::args.dsid, metautils::
                directives.gdex_upload_key, error) < 0) {
              log_error2("unable to move version-controlled metadata file " +
                  file + "; error: " + error, F, "dcm", USER);
            }
          }
        }
        if (gdex_unlink("/data/web/datasets/" + metautils::args.dsid +
            "/metadata/" + md_directory + "/" + md_file, metautils::directives.
            gdex_unlink_key, error) < 0) {
          metautils::log_warning("unable to unsync " + md_file, "dcm", USER);
        }
      }
      return true;
    }
  }
  local_server.disconnect();
  return false;
}

extern "C" void *t_removed(void *ts) {
  ThreadStruct *t = (ThreadStruct *)ts;
  auto file = t->strings[0];
  auto file_removed = false;
  string file_id_code;
  bool is_version_controlled;
  auto was_removed = remove_from("WGrML", "_webfiles2", "id", "wfmd", file,
      ".GrML", file_id_code, is_version_controlled);
  if (was_removed) {
    clear_tables_by_file_id("WGrML", file_id_code, is_version_controlled);
    file_removed = true;
    g_removed_from_wgrml = true;
  }
  was_removed = remove_from("WObML", "_webfiles2", "id", "wfmd", file,
      ".ObML", file_id_code, is_version_controlled);
  if (was_removed) {
    clear_tables_by_file_id("WObML", file_id_code, is_version_controlled);
    file_removed = true;
    g_removed_from_wobml = true;
  }
  was_removed = remove_from("WFixML", "_webfiles2", "webID", "wfmd", file,
      ".FixML", file_id_code, is_version_controlled);
  if (was_removed) {
    clear_tables_by_file_id("WFixML", file_id_code, is_version_controlled);
    file_removed = true;
    g_removed_from_wfixml = true;
  }
  Server server_d(metautils::directives.rdadb_config);
  LocalQuery query("gindex", "dssdb.wfile_" + metautils::args.dsid, "wfile = '"
      + file + "'");
  Row row;
  if (query.submit(server_d) == 0 && query.fetch_row(row)) {
    g_web_gindex_set.emplace(row[0]);
  }
  if (file_removed) {
    query.set("tindex", "dssdb.wfile_" + metautils::args.dsid, "wfile = '" +
        file + "'");
    if (query.submit(server_d) == 0 && query.num_rows() == 1) {
      query.fetch_row(row);
      if (!row[0].empty() && row[0] != "0") {
        g_web_tindex_set.emplace(row[0]);
      }
    }
    g_create_web_filelist_cache = true;
  }
  server_d.update("dssdb.wfile_" + metautils::args.dsid, "meta_link = NULL",
      "wfile = '" + file + "'");
  server_d.disconnect();
  t->file_removed = file_removed;
  if (gatherxml::verbose_operation) {
    if (t->file_removed) {
      cout << "... " << t->strings[0] << " was successfully removed." << endl;
    } else {
      cout << "... " << t->strings[0] << " was NOT removed." << endl;
    }
  }
  t->still_running = false;
  return nullptr;
}

void generate_graphics() {
  stringstream oss, ess;
  mysystem2(metautils::directives.local_root + "/bin/gsi " + metautils::args.
      dsid, oss, ess);
}

void generate_dataset_home_page() {
  stringstream oss, ess;
  mysystem2("/bin/bash -c 'curl -s -k https://" + metautils::directives.
      web_server + "/redeploy/dsgen" + metautils::args.dsid + "'", oss, ess);
}

extern "C" void *t_index_variables(void *) {
  Server srv(metautils::directives.metadb_config);
  string e;
  searchutils::indexed_variables(srv, metautils::args.dsid, e);
  srv.disconnect();
  return nullptr;
}

extern "C" void *t_summarize_frequencies(void *ts) {
  ThreadStruct *t = (ThreadStruct *)ts;
  gatherxml::summarizeMetadata::summarize_frequencies(t->strings[0], "dcm",
      USER);
  return nullptr;
}

extern "C" void *t_summarize_grids(void *ts) {
  ThreadStruct *t = (ThreadStruct *)ts;
  gatherxml::summarizeMetadata::summarize_grids(t->strings[0], "dcm", USER);
  return nullptr;
}

extern "C" void *t_aggregate_grids(void *ts) {
  ThreadStruct *t = (ThreadStruct *)ts;
  gatherxml::summarizeMetadata::aggregate_grids(t->strings[0], "dcm", USER);
  return nullptr;
}

extern "C" void *t_summarize_grid_levels(void *ts) {
  ThreadStruct *t = (ThreadStruct *)ts;
  gatherxml::summarizeMetadata::summarize_grid_levels(t->strings[0], "dcm",
      USER);
  return nullptr;
}

extern "C" void *t_summarize_grid_resolutions(void *) {
  gatherxml::summarizeMetadata::summarize_grid_resolutions("dcm", USER, "");
  return nullptr;
}

extern "C" void *t_generate_detailed_metadata_view(void *) {
  char flag;
  gatherxml::detailedMetadata::generate_detailed_metadata_view(flag, "dcm",
      USER);
  return nullptr;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    cerr << "usage: dcm -d <nnn.n> [options...] files..." << endl;
    cerr << endl;
    cerr << "purpose:" << endl;
    cerr << "dcm deletes content metadata from the RDADB for the specified "
        "files." << endl;
    cerr << endl;
    cerr << "required:" << endl;
    cerr << "-d <nnn.n>  nnn.n is the dataset number to which the data file(s) "
        "belong" << endl;
    cerr << endl;
    cerr << "options:" << endl;
    cerr << "-g/-G       generate (default)/don't generate graphics for ObML "
        "files" << endl;
    cerr << "-N          notify with a message when dcm completes" << endl;
    cerr << endl;
    cerr << "NOTES:" << endl;
    cerr << "  1) each file in <files...> must be specified as for the -WF "
        "option of \"dsarch\"" << endl;
    exit(1);
  }
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '%');
  metautils::read_config("dcm", USER);
  Server server(metautils::directives.metadb_config);
  if (!server) {
    log_error2("unable to connect to database server on startup", "main()",
        "dcm", USER);
  }
  parse_args(server);
  g_removed_from_wgrml = false;
  g_removed_from_wobml = false;
  g_removed_from_satml = false;
  g_removed_from_wfixml = false;
  const size_t MAX_NUM_THREADS = 6;
  vector<ThreadStruct> threads(MAX_NUM_THREADS);
  vector<pthread_t> tid_list;
  for (size_t n = 0; n < g_files.size(); ++n) {
    if (tid_list.size() < MAX_NUM_THREADS) {
      threads[tid_list.size()].strings.clear();
      threads[tid_list.size()].strings.emplace_back(g_files[n]);
      threads[tid_list.size()].removed_file_index = n;
      threads[tid_list.size()].still_running = true;
      threads[tid_list.size()].file_removed = false;
      pthread_create(&threads[tid_list.size()].tid, nullptr, t_removed,
          &threads[tid_list.size()]);
      pthread_detach(threads[tid_list.size()].tid);
      tid_list.emplace_back(threads[tid_list.size()].tid);
      if (gatherxml::verbose_operation) {
        cout << "thread created for removal of " << g_files[n] << " ..." <<
            endl;
      }
    } else {
      if (gatherxml::verbose_operation) {
        cout << "... at maximum thread capacity, waiting for an available slot "
            "..." << endl;
      }
      size_t free_tid_idx = 0xffffffffffffffff;
      while (1) {
        for (size_t m = 0; m < MAX_NUM_THREADS; ++m) {
          if (!threads[m].still_running) {
            if (threads[m].file_removed) {
              if (gatherxml::verbose_operation) {
                cout << "File " << g_files[threads[m].removed_file_index] <<
                    " cleared." << endl;
              }
              g_files[threads[m].removed_file_index].clear();
            }
            free_tid_idx = m;
            break;
          }
        }
        if (free_tid_idx < 0xffffffffffffffff) {
          break;
        }
      }
      threads[free_tid_idx].strings.clear();
      threads[free_tid_idx].strings.emplace_back(g_files[n]);
      threads[free_tid_idx].removed_file_index = n;
      threads[free_tid_idx].still_running = true;
      threads[free_tid_idx].file_removed = false;
      pthread_create(&threads[free_tid_idx].tid, nullptr, t_removed,
          &threads[free_tid_idx]);
      pthread_detach(threads[free_tid_idx].tid);
      tid_list[free_tid_idx] = threads[free_tid_idx].tid;
      if (gatherxml::verbose_operation) {
        cout << "available thread slot at " << free_tid_idx << " found, thread "
            "created for removal of " << g_files[n] << " ..." << endl;
      }
    }
  }
  auto found_thread = true;
  while (found_thread) {
    found_thread = false;
    for (size_t n = 0; n < tid_list.size(); ++n) {
      if (tid_list[n] < 0xffffffffffffffff) {
        if (!threads[n].still_running) {
          if (threads[n].file_removed) {
            g_files[threads[n].removed_file_index].clear();
            if (gatherxml::verbose_operation) {
              cout << "File " << g_files[threads[n].removed_file_index] << "/"
                  << g_files.size() << " cleared." << endl;
            }
          }
          tid_list[n] = 0xffffffffffffffff;
        } else {
          found_thread = true;
        }
      }
    }
  }
  for (const auto& gindex : g_web_gindex_set) {
    gatherxml::detailedMetadata::generate_group_detailed_metadata_view(gindex,
        "Web", "dcm", USER);
  }
  stringstream oss, ess;
  if (g_removed_from_wgrml) {
    clear_grid_cache(server, "WGrML");
    threads.clear();
    threads.resize(7);
    pthread_create(&threads[0].tid, nullptr, t_index_variables, nullptr);
    threads[1].strings.emplace_back("WGrML");
    pthread_create(&threads[1].tid, nullptr, t_summarize_frequencies,
        &threads[1]);
    threads[2].strings.emplace_back("WGrML");
    pthread_create(&threads[2].tid, nullptr, t_summarize_grids, &threads[2]);
    threads[3].strings.emplace_back("WGrML");
    pthread_create(&threads[3].tid, nullptr, t_aggregate_grids, &threads[3]);
    threads[4].strings.emplace_back("WGrML");
    pthread_create(&threads[4].tid, nullptr, t_summarize_grid_levels,
        &threads[4]);
    pthread_create(&threads[5].tid, nullptr, t_summarize_grid_resolutions,
        nullptr);
    pthread_create(&threads[6].tid, nullptr, t_generate_detailed_metadata_view,
        nullptr);
    for (const auto& t : threads) {
      pthread_join(t.tid, nullptr);
    }
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsid + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
         metautils::args.dsid + " -ri", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    generate_dataset_home_page();
  }
  if (g_removed_from_wobml) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm", USER);
    gatherxml::summarizeMetadata::summarize_obs_data("dcm",  USER);
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsid + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (metautils::args.update_graphics) {
      generate_graphics();
    }
    generate_dataset_home_page();
  }
  if (g_removed_from_satml) {
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsid + " -rm", oss, ess) < 0)
      cerr << ess.str() << endl;
  }
  if (g_removed_from_wfixml) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm", USER);
    gatherxml::summarizeMetadata::summarize_fix_data("dcm", USER);
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsid + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (metautils::args.update_graphics) {
      generate_graphics();
    }
    generate_dataset_home_page();
  }
  char progress_flag;
  if (g_create_web_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web", progress_flag,
        "dcm", USER);
  }
  for (const auto& tindex : g_web_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web", progress_flag,
        "dcm", USER, tindex);
  }
  if (create_inv_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv", progress_flag,
        "dcm", USER);
  }
  for (const auto& tindex : g_inv_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv", progress_flag,
        "dcm", USER, tindex);
  }
  for (auto it = g_files.begin(); it != g_files.end(); ++it) {
    if (it->empty()) {
      g_files.erase(it--);
    }
  }
  if (!g_files.empty()) {
    cerr << "Warning: content metadata for the following files was not "
        "removed (maybe it never existed?):";
    for (const auto& file : g_files) {
      cerr << " " << file;
    }
    cerr << endl;
  }
  server.disconnect();
  server.connect(metautils::directives.rdadb_config);
  if (server) {
    server.update("dssdb.dataset", "version = version + 1", "dsid in " +
        g_ds_set);
    server.disconnect();
  }
  if (g_files.empty() && local_args.notify) {
    cout << "dcm has completed successfully" << endl;
  }
}
