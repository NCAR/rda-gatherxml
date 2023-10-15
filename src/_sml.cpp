#include <iostream>
#include <stdlib.h>
#include <string>
#include <deque>
#include <list>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using namespace MySQL;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

Server server_m,server_d;
std::string user=getenv("USER");
std::string dsnum2;
std::string ds_meta_link="N";
struct Entry {
  Entry() : key(),meta_link() {}

  std::string key;
  std::string meta_link;
};
my::map<Entry> files_with_metadata;
std::list<std::string> file_list;

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
    if (table_exists(server_m,std::string(dbs[n])+".ds"+dsnum2+"_webfiles2")) {
	if (!filename.empty()) {
	  query.set("select id from "+dbs[n]+".ds"+dsnum2+"_webfiles2"+" where id = '"+filename+"'");
	}
	else {
	  query.set("select id from "+dbs[n]+".ds"+dsnum2+"_webfiles2");
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
  if (server_d.update("dssdb."+table,"meta_link = NULL","dsid = 'ds"+metautils::args.dsnum+"' and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to nullify meta_link for '"+file+"'","sml",user);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to NULL for file: '" << file << "'" << std::endl;
}

void update_meta_link(std::string table,std::string file,std::string new_meta_link,std::string old_meta_link)
{
  if (server_d.update("dssdb."+table,"meta_link = '"+new_meta_link+"'","dsid = 'ds"+metautils::args.dsnum+"' and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to set meta_link for '"+file+"' to '"+new_meta_link+"'","sml",user);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to '" << new_meta_link << "' for file: '" << file << "'" << std::endl;
}

void set_file_meta_links()
{
  LocalQuery query;
  Row row;
  Entry e;

//  query.set("select mssfile,meta_link,'mssfile' from mssfile where dsid = 'ds"+metautils::args.dsnum+"' and property = 'P' and retention_days > 0 union select wfile,meta_link,'wfile' from wfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'D' and property = 'A'");
query.set("select mssfile,meta_link,'mssfile' from mssfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'P' and status = 'P' union select wfile,meta_link,'wfile' from wfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'D' and status = 'P'");
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
    query.set("select meta_link,'mssfile' from mssfile where dsid = 'ds"+metautils::args.dsnum+"' and mssfile = '"+filename+"'");
  }
  else {
    query.set("select meta_link,'wfile' from wfile where dsid = 'ds"+metautils::args.dsnum+"' and wfile = '"+filename+"'");
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
	metautils::args.dsnum=unix_args[++n];
	if (strutils::has_beginning(metautils::args.dsnum,"ds")) {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
    }
    else if (n > 1) {
	file_list.push_back(unix_args[n]);
    }
  }
  if (metautils::args.dsnum.empty()) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  metautils::read_config("sml",user,false);
  server_m.connect(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  server_d.connect(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
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
  if (server_d.update("dssdb.dataset","meta_link = '"+ds_meta_link+"', version = version + 1","dsid = 'ds"+metautils::args.dsnum+"'") < 0) {
    metautils::log_error("error '"+server_d.error()+"' while updating meta_link field for dataset","sml",user);
  }
  server_d.disconnect();
  server_m.disconnect();
}
