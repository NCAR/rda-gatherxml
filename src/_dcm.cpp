#include <iostream>
#include <sys/stat.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <gatherxml.hpp>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>
#include <myerror.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cerr;
using std::cout;
using std::endl;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strutils::chop;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using unixutils::exists_on_server;
using unixutils::mysystem2;
using unixutils::rdadata_sync;
using unixutils::rdadata_unsync;
using unixutils::remote_web_file;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const string USER = getenv("USER");
string myerror = "";
string mywarning = "";

struct Entry {
  Entry() : key() { }

  string key;
};

struct LocalArgs {
  LocalArgs() : notify(false), keep_xml(false) { }

  bool notify, keep_xml;
} local_args;

struct ThreadStruct {
  ThreadStruct() : strings(), removed_file_index(-1), file_removed(false),
      tid(0) { }

  vector<string> strings;
  int removed_file_index;
  bool file_removed;
  pthread_t tid;
};

string g_dsnum2;
vector<string> g_files;
unordered_set<string> g_web_tindex_set, g_inv_tindex_set;
unordered_set<string> g_web_gindex_set;
bool g_removed_from_wgrml, g_removed_from_wobml, g_removed_from_satml,
    g_removed_from_wfixml;
bool g_create_web_filelist_cache = false, create_inv_filelist_cache = false;

void parse_args(MySQL::Server& server) {
  auto arg_list = split(metautils::args.args_string, "%");
  for (auto arg = arg_list.begin(); arg != arg_list.end(); ++arg) {
    if (*arg == "-d") {
      ++arg;
      metautils::args.dsnum = *arg;
      if (metautils::args.dsnum.find("ds") == 0) {
        metautils::args.dsnum = metautils::args.dsnum.substr(2);
      }
      g_dsnum2 = substitute(metautils::args.dsnum, ".", "");
    } else if (*arg == "-G") {
      metautils::args.update_graphics = false;
    } else if (*arg == "-k") {
      local_args.keep_xml = true;
    } else if (*arg == "-N") {
      local_args.notify = true;
    } else {
      g_files.emplace_back(*arg);
    }
  }
  if (metautils::args.dsnum.empty()) {
    cerr << "Error: no dataset specified" << endl;
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

void copy_version_controlled_data(MySQL::Server& server, string db, string
    file_id_code) {
  string error;
  auto tnames = table_names(server, db, "ds" + g_dsnum2 + "%", error);
  for (auto t : tnames) {
    t = db + "." + t;
    if (field_exists(server, t, "file_code")) {
      if (!table_exists(server, "V" + t)) {
        if (server.command("create table V" + t + " like " + t, error) < 0) {
          log_error2("error: '" + server.error() + "' while trying to create V"
              + t, "copy_version_controlled_data()", "dcm", USER);
        }
      }
      string res;
      server.command("insert into V" + t + " select * from " + t + " where "
          "file_code = " + file_id_code, res);
    }
  }
}

void clear_tables_by_file_id(string db, string file_id_code, bool
    is_version_controlled) {
  static const string F = this_function_label(__func__);
  MySQL::Server local_server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!local_server) {
    log_error2("unable to connect to MySQL server while clearing file code " +
        file_id_code + " from " + db, F, "dcm", USER);
  }
  if (is_version_controlled) {
    copy_version_controlled_data(local_server, db, file_id_code);
  }
  string error;
  auto tnames = table_names(local_server, db, "ds" + g_dsnum2 + "%", error);
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

void clear_grid_cache(MySQL::Server& server, string db) {
  static const string F = this_function_label(__func__);
  MySQL::LocalQuery query("select parameter, level_type_codes, min("
      "start_date), max(end_date) from " + db + ".ds" + g_dsnum2 + "_agrids2 "
      "group by parameter, level_type_codes");
  if (query.submit(server) < 0) {
    log_error2("error: '" + query.error() + "' while trying to rebuild grid "
        "cache", F, "dcm", USER);
  }
  if (server._delete(db + ".ds" + g_dsnum2 + "_agrids_cache") < 0) {
    log_error2("error: '" + server.error() + "' while clearing " + db + ".ds" +
        g_dsnum2 + "_agrids_cache", F, "dcm", USER);
  }
  for (const auto& row : query) {
    if (server.insert(db + ".ds" + g_dsnum2 + "_agrids_cache", "'" + row[0] +
        "', '" + row[1] + "', " + row[2] + ", " + row[3]) < 0) {
      log_error2("error: '" + server.error() + "' while inserting into " + db +
          ".ds" + g_dsnum2 + "_agrids_cache", F, "dcm", USER);
    }
  }
}

bool remove_from(string database, string table_ext, string file_field_name,
    string md_directory, string& file, string file_ext, string& file_id_code,
    bool& is_version_controlled) {
  static const string F = this_function_label(__func__);
  is_version_controlled = false;
  string error;
  auto file_table = database + ".ds" + g_dsnum2 + table_ext;
  MySQL::Server local_server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!local_server) {
    log_error2("unable to connect to MySQL server while removing  " + file, F,
        "dcm", USER);
  }
  MySQL::LocalQuery query("code", file_table, file_field_name + " = '" + file +
      "'");
  if (query.submit(local_server) == 0) {
    if (query.num_rows() == 1) {
      MySQL::Row row;
      query.fetch_row(row);
      file_id_code = row[0];

      // check for a file moved to the saved area
      query.set("sid", "dssdb.file", "dsid = 'ds" + metautils::args.dsnum +
          "' and sfile = '" + file + "'");
      if (query.submit(local_server) < 0) {
        log_error2("error: '" + local_server.error() + "' while trying to "
            "check sfile for '" + file + "'", F, "dcm", USER);
      }
      if (query.num_rows() == 1) {
        is_version_controlled = true;
      }
      if (is_version_controlled) {
        string res;
        if (!table_exists(local_server, "V" + file_table)) {
          if (local_server.command("create table V" + file_table + " like " +
              file_table, error) < 0) {
            log_error2("error: '" + local_server.error() + "' while trying to "
                "create V" + file_table, F, "dcm", USER);
          }
        }
        local_server.command("insert into V" + file_table + " select * from " +
            file_table + " where code = " + file_id_code, res);
      }
      if (local_server._delete(file_table, "code = " + file_id_code) < 0) {
        log_error2("error: '" + local_server.error() + "'", F, "dcm", USER);
      }
      if (database == "WGrML" || database == "WObML") {
        auto tables = MySQL::table_names(local_server, substitute(database, "W",
            "I"), "ds" + g_dsnum2 + "_inventory_%", error);
        for (const auto& table : tables) {
          local_server._delete(substitute(database, "W", "I") + ".`" + table +
              "`", "file_code = " + file_id_code);
        }
        if (local_server._delete(substitute(database, "W", "I") + ".ds" +
            g_dsnum2 + "_inventory_summary", "webID_code = " + file_id_code) ==
            0) {
          query.set("select tindex from dssdb.wfile where wfile = '" + file +
              "'");
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
        local_server._delete("I" + database.substr(1) + ".ds" + g_dsnum2 +
            "_dataTypes", "file_code = " + file_id_code);
        auto tables = MySQL::table_names(local_server, "I" + database.substr(1),
            "ds" + g_dsnum2 + "_timeSeries_times_%", error);
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
          if (mysystem2("/bin/mkdir -p " + tdir->name() + "/metadata/" +
               md_directory + "/v", oss, ess) < 0) {
            log_error2("could not create the temporary directory tree", F,
                "dcm", USER);
          }
        }
        short flag = 0;
        if (exists_on_server(metautils::directives.web_server, "/data/web/"
            "datasets/ds" + metautils::args.dsnum + "/metadata/" + md_directory
            + "/" + md_file + ".gz", metautils::directives.rdadata_home)) {
          flag = 1;
        } else if (exists_on_server(metautils::directives.web_server, "/data/"
            "web/datasets/ds" + metautils::args.dsnum + "/metadata/" +
            md_directory + "/" + md_file, metautils::directives.rdadata_home)) {
          flag = 2;
        }
        if (file_ext == ".ObML" && flag > 0) {
          string xml_parent;
          if (flag == 1) {
            xml_parent = remote_web_file("https://rda.ucar.edu/datasets/ds" +
                metautils::args.dsnum + "/metadata/" + md_directory + "/" +
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
            xml_parent = remote_web_file("https://rda.ucar.edu/datasets/ds" +
                metautils::args.dsnum + "/metadata/" + md_directory + "/" +
                md_file, tempdir_name());
          }
          XMLDocument xdoc;
          if (xdoc.open(xml_parent)) {
            auto elist = xdoc.element_list("ObML/observationType/platform/IDs");
            for (const auto& e : elist) {
              if (is_version_controlled) {
                auto f = remote_web_file("https://rda.ucar.edu/datasets/ds" +
                    metautils::args.dsnum + "/metadata/" + md_directory + "/" +
                    e.attribute_value("ref"), tdir->name() + "/metadata/" +
                    md_directory + "/v");
                if (f.empty()) {
                  log_error2("could not get remote file https://rda.ucar.edu/"
                      "datasets/ds" + metautils::args.dsnum + "/metadata/" +
                      md_directory + "/" + e.attribute_value("ref"), F, "dcm",
                      USER);
                }
              }
              if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
                  args.dsnum + "/metadata/" + md_directory + "/" + e.
                  attribute_value("ref"), tdir->name(), metautils::directives.
                  rdadata_home, error) < 0) {
                metautils::log_warning("unable to unsync " + e.attribute_value(
                    "ref"), "dcm", USER);
              }
            }
            elist = xdoc.element_list("ObML/observationType/platform/"
                "locations");
            for (const auto& e : elist) {
              if (is_version_controlled) {
                auto f = remote_web_file("https://rda.ucar.edu/datasets/ds" +
                    metautils::args.dsnum + "/metadata/" + md_directory + "/" +
                    e.attribute_value("ref"), tdir->name() + "/metadata/" +
                    md_directory + "/v");
                if (f.empty()) {
                  log_error2("could not get remote file https://rda.ucar.edu/"
                      "datasets/ds" + metautils::args.dsnum + "/metadata/" +
                      md_directory + "/" + e.attribute_value("ref"), F, "dcm",
                      USER);
                }
              }
              if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::
                  args.dsnum + "/metadata/" + md_directory + "/" + e.
                  attribute_value("ref"), tdir->name(), metautils::directives.
                  rdadata_home, error) < 0) {
                metautils::log_warning("unable to unsync " + e.attribute_value(
                    "ref"), "dcm", USER);
              }
            }
            xdoc.close();
          }
        }
        if (md_directory == "wfmd") {
          if (exists_on_server(metautils::directives.web_server, "/data/web/"
              "datasets/ds" + metautils::args.dsnum + "/metadata/inv/" +
              md_file + "_inv.gz", metautils::directives.rdadata_home)) {
            if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
                dsnum + "/metadata/inv/" + md_file + "_inv.gz", tdir->name(),
                metautils::directives.rdadata_home, error) < 0) {
              metautils::log_warning("unable to unsync " + md_file + "_inv",
                  "dcm", USER);
            }
          } else if (exists_on_server(metautils::directives.web_server, "/data/"
              "web/datasets/ds" + metautils::args.dsnum + "/metadata/inv/" +
              md_file + "_inv", metautils::directives.rdadata_home)) {
            if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
                dsnum + "/metadata/inv/" + md_file + "_inv", tdir->name(),
                metautils::directives.rdadata_home, error) < 0) {
              metautils::log_warning("unable to unsync " + md_file + "_inv",
                  "dcm", USER);
            }
          }
          if (exists_on_server(metautils::directives.web_server, "/data/web/"
              "datasets/ds" + metautils::args.dsnum + "/metadata/wms/" + md_file
              + ".gz", metautils::directives.rdadata_home)) {
            if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.
                dsnum + "/metadata/wms/" + md_file + ".gz", tdir->name(),
                metautils::directives.rdadata_home, error) < 0) {
              metautils::log_warning("unable to unsync wms file " + md_file,
                  "dcm", USER);
            }
          }
        }
        if (flag == 1) {
          md_file += ".gz";
        }
        if (is_version_controlled) {
          auto f = remote_web_file("https://rda.ucar.edu/datasets/ds" +
              metautils::args.dsnum + "/metadata/" + md_directory + "/" +
              md_file, tdir->name() + "/metadata/" + md_directory + "/v");
          if (!f.empty()) {
            string error;
            if (rdadata_sync(tdir->name(), "metadata/" + md_directory + "/v/",
                "/data/web/datasets/ds" + metautils::args.dsnum, metautils::
                directives.rdadata_home, error) < 0) {
              log_error2("unable to move version-controlled metadata file " +
                  file + "; error: " + error, F, "dcm", USER);
            }
          }
        }
        if (rdadata_unsync("/__HOST__/web/datasets/ds" + metautils::args.dsnum +
            "/metadata/" + md_directory + "/" + md_file, tdir->name(),
            metautils::directives.rdadata_home, error) < 0) {
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
  was_removed = remove_from("WObML", "_webfiles2", "webID", "wfmd", file,
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
  MySQL::Server server_d(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "");
  MySQL::LocalQuery query("gindex", "wfile", "wfile = '" + file + "'");
  MySQL::Row row;
  if (query.submit(server_d) == 0 && query.fetch_row(row)) {
    g_web_gindex_set.emplace(row[0]);
  }
  if (file_removed) {
    query.set("tindex", "wfile", "wfile = '" + file + "'");
    if (query.submit(server_d) == 0 && query.num_rows() == 1) {
      query.fetch_row(row);
      if (!row[0].empty() && row[0] != "0") {
        g_web_tindex_set.emplace(row[0]);
      }
    }
    g_create_web_filelist_cache = true;
  }
  server_d.update("dssdb.wfile", "meta_link = NULL", "dsid = 'ds" + metautils::
      args.dsnum + "' and wfile = '" + file + "'");
  server_d.disconnect();
  t->file_removed = file_removed;
  if (gatherxml::verbose_operation) {
    if (t->file_removed) {
      cout << "... " << t->strings[0] << " was successfully removed." << endl;
    } else {
      cout << "... " << t->strings[0] << " was NOT removed." << endl;
    }
  }
  return nullptr;
}

void generate_graphics() {
  stringstream oss, ess;
  mysystem2(metautils::directives.local_root + "/bin/gsi " + metautils::args.
      dsnum, oss, ess);
}

void generate_dataset_home_page() {
  stringstream oss, ess;
  mysystem2(metautils::directives.local_root + "/bin/dsgen " + metautils::args.
      dsnum, oss, ess);
}

extern "C" void *t_index_variables(void *) {
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  string e;
  searchutils::indexed_variables(srv, metautils::args.dsnum, e);
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
  gatherxml::detailedMetadata::generate_detailed_metadata_view("dcm", USER);
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
  MySQL::Server server(metautils::directives.database_server, metautils::
      directives.metadb_username, metautils::directives.metadb_password, "");
  if (!server) {
    log_error2("unable to connect to MySQL server on startup", "main()", "dcm",
        USER);
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
          if (pthread_kill(tid_list[m], 0) != 0) {
            if (threads[m].file_removed) {
              g_files[threads[m].removed_file_index].clear();
              if (gatherxml::verbose_operation) {
                cout << "File " << g_files[threads[m].removed_file_index] << "/"
                    << g_files.size() << " cleared." << endl;
              }
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
        if (pthread_kill(tid_list[n], 0) != 0) {
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
        metautils::args.dsnum + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
         metautils::args.dsnum + " -ri", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    generate_dataset_home_page();
  }
  if (g_removed_from_wobml) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm", USER);
    gatherxml::summarizeMetadata::summarize_obs_data("dcm",  USER);
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (metautils::args.update_graphics) {
      generate_graphics();
    }
    generate_dataset_home_page();
  }
  if (g_removed_from_satml) {
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " -rm", oss, ess) < 0)
      cerr << ess.str() << endl;
  }
  if (g_removed_from_wfixml) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm", USER);
    gatherxml::summarizeMetadata::summarize_fix_data("dcm", USER);
    if (mysystem2(metautils::directives.local_root + "/bin/scm -d " +
        metautils::args.dsnum + " -rw", oss, ess) < 0) {
      cerr << ess.str() << endl;
    }
    if (metautils::args.update_graphics) {
      generate_graphics();
    }
    generate_dataset_home_page();
  }
  if (g_create_web_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web", "dcm", USER);
  }
  for (const auto& tindex : g_web_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web", "dcm", USER,
        tindex);
  }
  if (create_inv_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv", "dcm", USER);
  }
  for (const auto& tindex : g_inv_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv", "dcm", USER,
        tindex);
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
  server.connect(metautils::directives.database_server, metautils::directives.
      rdadb_username, metautils::directives.rdadb_password, "");
  if (server) {
    server.update("dataset", "version = version + 1", "dsid = 'ds" +
        metautils::args.dsnum + "'");
    server.disconnect();
  }
  if (g_files.empty() && local_args.notify) {
    cout << "dcm has completed successfully" << endl;
  }
}
