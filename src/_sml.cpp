#include <iostream>
#include <stdlib.h>
#include <string>
#include <deque>
#include <list>
#include <PostgreSQL.hpp>
#include <metadata.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using strutils::ds_aliases;
using strutils::ng_gdex_id;
using strutils::to_sql_tuple_string;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

Server server_m,server_d;
auto env = getenv("USER");
std::string USER = (env == nullptr) ? "unknown" : env;
std::string ds_meta_link="N";
struct Entry {
  Entry() : key(),meta_link() {}

  std::string key;
  std::string meta_link;
};
my::map<Entry> files_with_metadata;
std::list<std::string> file_list;
std::string g_ds_set;

void fill_files_with_metadata(std::string filename = "")
{
  const size_t NUM_DATABASES=7;
  static const std::vector<std::string> dbs{ "WGrML", "WObML", "SatML", "WFixML" };
  LocalQuery query;
  Row row;
  std::string meta_link;
  Entry e;

  for (size_t n=0; n < NUM_DATABASES; ++n) {
    if (dbs[n][0] == 'W') {
	meta_link.assign(&dbs[n][1],2);
    }
    if (table_exists(server_m,std::string(dbs[n])+"."+metautils::args.dsid+"_webfiles2")) {
	if (!filename.empty()) {
	  query.set("select id from "+dbs[n]+"."+metautils::args.dsid+"_webfiles2"+" where id = '"+filename+"'");
	}
	else {
	  query.set("select id from "+dbs[n]+"."+metautils::args.dsid+"_webfiles2");
	}
	if (query.submit(server_m) < 0) {
	  metautils::log_error("error '"+query.error()+"' while trying to get "+dbs[n]+" filelist","sml",user);
	}
	while (query.fetch_row(row)) {
	  e.key=row[0];
	  e.meta_link=meta_link;
	  files_with_metadata.insert(e);
	}
    }
  }
}

void nullify_meta_link(std::string table,std::string file,std::string old_meta_link)
{
  if (server_d.update("dssdb."+table,"meta_link = NULL","dsid in "+g_ds_set+" and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to nullify meta_link for '"+file+"'","sml",user);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to NULL for file: '" << file << "'" << std::endl;
}

void update_meta_link(std::string table,std::string file,std::string new_meta_link,std::string old_meta_link)
{
  if (server_d.update("dssdb."+table,"meta_link = '"+new_meta_link+"'","dsid in "+g_ds_set+" and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to set meta_link for '"+file+"' to '"+new_meta_link+"'","sml",user);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to '" << new_meta_link << "' for file: '" << file << "'" << std::endl;
}

void set_file_meta_links()
{
  LocalQuery query;
  Row row;
  Entry e;

  query.set("select mssfile,meta_link,'mssfile' from mssfile where dsid in "+g_ds_set+" and type = 'P' and status = 'P' union select wfile,meta_link,'wfile' from dssdb.wfile_"+metautils::args.dsid+" where type = 'D' and status = 'P'");
  if (query.submit(server_d) < 0)
    metautils::log_error("error '"+query.error()+"' while trying to get dssdb filelist","sml",user);
  while (query.fetch_row(row)) {
    if (!files_with_metadata.found(row[0],e)) {
	if (!row[1].empty() && row[1] != "N")
	  nullify_meta_link(row[2],row[0],row[1]);
    }
    else {
	if (row[1] != e.meta_link)
	  update_meta_link(row[2],row[0],e.meta_link,row[1]);
    }
  }
}

void set_file_meta_link(std::string filename)
{
  LocalQuery query;
  if (strutils::has_beginning(filename,"/FS/DECS/")) {
    query.set("select meta_link,'mssfile' from mssfile where dsid in "+g_ds_set+" and mssfile = '"+filename+"'");
  }
  else {
    query.set("select meta_link,'wfile' from dssdb.wfile_"+metautils::args.dsid+" where wfile = '"+filename+"'");
  }
  if (query.submit(server_d) < 0) {
    metautils::log_error("error '"+query.error()+"' while trying to get entry for '"+filename+"' from dssdb","sml",user);
  }
  if (query.num_rows() == 0) {
    metautils::log_error("'"+filename+"' is not in dssdb","sml",user);
  }
  else {
    Row row;
    query.fetch_row(row);
    Entry e;
    if (!files_with_metadata.found(filename,e)) {
	if (!row[0].empty() && row[0] != "N") {
	  nullify_meta_link(row[1],filename,row[0]);
	}
    }
    else {
	if (row[0] != e.meta_link) {
	  update_meta_link(row[1],filename,e.meta_link,row[0]);
	}
    }
  }
}

int main(int argc,char **argv)
{
  if (argc < 3) {
    std::cerr << "usage: sml -d nnn.n [files...]" << std::endl;
    exit(1);
  }
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,'%');
  auto unix_args=strutils::split(metautils::args.args_string,"%");
  for (size_t n=0; n < unix_args.size(); ++n) {
    if (unix_args[n] == "-d") {
	metautils::args.dsid=ng_gdex_id(unix_args[++n]);
      g_ds_set = to_sql_tuple_string(ds_aliases(metautils::args.dsid));
    }
    else if (n > 1) {
	file_list.push_back(unix_args[n]);
    }
  }
  if (metautils::args.dsid.empty()) {
    std::cerr << "Error: no or invalid dataset ID specified" << std::endl;
    exit(1);
  }
  metautils::read_config("sml",user,false);
  server_m.connect(metautils::directives.metadb_config);
  server_d.connect(metautils::directives.rdadb_config);
  if (file_list.size() == 0) {
    fill_files_with_metadata();
    set_file_meta_links();
  }
  else {
    for (const auto& file : file_list) {
	fill_files_with_metadata(file);
	set_file_meta_link(file);
    }
  }
  if (files_with_metadata.size() > 0) {
    ds_meta_link="Y";
  }
  if (server_d.update("dssdb.dataset","meta_link = '"+ds_meta_link+"', version = version + 1","dsid in "+g_ds_set) < 0) {
    metautils::log_error("error '"+server_d.error()+"' while updating meta_link field for dataset","sml",user);
  }
  server_d.disconnect();
  server_m.disconnect();
}
