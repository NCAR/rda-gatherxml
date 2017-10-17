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

metautils::Directives directives;
metautils::Args args;
MySQL::Server server_m,server_d;
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
  const char *dbs[NUM_DATABASES]={"GrML","WGrML","ObML","WObML","SatML","FixML","WFixML"};
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string IDType,listType,meta_link;
  Entry e;

  for (size_t n=0; n < NUM_DATABASES; ++n) {
    if (dbs[n][0] == 'W') {
	IDType="web";
	listType="_webfiles";
	meta_link.assign(&dbs[n][1],2);
    }
    else {
	IDType="mss";
	listType="_primaries";
	meta_link.assign(&dbs[n][0],2);
    }
    if (table_exists(server_m,std::string(dbs[n])+".ds"+dsnum2+listType)) {
	if (filename.length() > 0) {
	  query.set("select "+IDType+"ID from "+dbs[n]+".ds"+dsnum2+listType+" where "+IDType+"ID = '"+filename+"'");
	}
	else {
	  query.set("select "+IDType+"ID from "+dbs[n]+".ds"+dsnum2+listType);
	}
	if (query.submit(server_m) < 0) {
	  metautils::log_error("error '"+query.error()+"' while trying to get "+dbs[n]+" filelist","sml",user,args.args_string);
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
  if (server_d.update("dssdb."+table,"meta_link = NULL","dsid = 'ds"+args.dsnum+"' and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to nullify meta_link for '"+file+"'","sml",user,args.args_string);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to NULL for file: '" << file << "'" << std::endl;
}

void update_meta_link(std::string table,std::string file,std::string new_meta_link,std::string old_meta_link)
{
  if (server_d.update("dssdb."+table,"meta_link = '"+new_meta_link+"'","dsid = 'ds"+args.dsnum+"' and "+table+" = '"+file+"'") < 0)
    metautils::log_error("error '"+server_d.error()+"' while trying to set meta_link for '"+file+"' to '"+new_meta_link+"'","sml",user,args.args_string);
  std::cout << "Info: changed 'meta_link' from '" << old_meta_link << "' to '" << new_meta_link << "' for file: '" << file << "'" << std::endl;
}

void set_file_meta_links()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  Entry e;

//  query.set("select mssfile,meta_link,'mssfile' from mssfile where dsid = 'ds"+args.dsnum+"' and property = 'P' and retention_days > 0 union select wfile,meta_link,'wfile' from wfile where dsid = 'ds"+args.dsnum+"' and type = 'D' and property = 'A'");
query.set("select mssfile,meta_link,'mssfile' from mssfile where dsid = 'ds"+args.dsnum+"' and type = 'P' and status = 'P' union select wfile,meta_link,'wfile' from wfile where dsid = 'ds"+args.dsnum+"' and type = 'D' and status = 'P'");
  if (query.submit(server_d) < 0)
    metautils::log_error("error '"+query.error()+"' while trying to get dssdb filelist","sml",user,args.args_string);
  while (query.fetch_row(row)) {
    if (!files_with_metadata.found(row[0],e)) {
	if (row[1].length() > 0 && row[1] != "N")
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
  MySQL::LocalQuery query;
  if (strutils::has_beginning(filename,"/FS/DSS/") || strutils::has_beginning(filename,"/DSS/")) {
    query.set("select meta_link,'mssfile' from mssfile where dsid = 'ds"+args.dsnum+"' and mssfile = '"+filename+"'");
  }
  else {
    query.set("select meta_link,'wfile' from wfile where dsid = 'ds"+args.dsnum+"' and wfile = '"+filename+"'");
  }
  if (query.submit(server_d) < 0) {
    metautils::log_error("error '"+query.error()+"' while trying to get entry for '"+filename+"' from dssdb","sml",user,args.args_string);
  }
  if (query.num_rows() == 0) {
    metautils::log_error("'"+filename+"' is not in dssdb","sml",user,args.args_string);
  }
  else {
    MySQL::Row row;
    query.fetch_row(row);
    Entry e;
    if (!files_with_metadata.found(filename,e)) {
	if (row[0].length() > 0 && row[0] != "N") {
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
  args.args_string=unix_args_string(argc,argv,'%');
  auto unix_args=strutils::split(args.args_string,"%");
  for (size_t n=0; n < unix_args.size(); ++n) {
    if (unix_args[n] == "-d") {
	args.dsnum=unix_args[++n];
	if (strutils::has_beginning(args.dsnum,"ds")) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (n > 1) {
	file_list.push_back(unix_args[n]);
    }
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  dsnum2=strutils::substitute(args.dsnum,".","");
  metautils::read_config("sml",user,args.args_string,false);
  metautils::connect_to_metadata_server(server_m);
  metautils::connect_to_RDADB_server(server_d);
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
  if (server_d.update("dssdb.dataset","meta_link = '"+ds_meta_link+"', version = version + 1","dsid = 'ds"+args.dsnum+"'") < 0) {
    metautils::log_error("error '"+server_d.error()+"' while updating meta_link field for dataset","sml",user,args.args_string);
  }
  server_d.disconnect();
  server_m.disconnect();
}
