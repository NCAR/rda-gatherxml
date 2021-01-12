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

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

struct Entry {
  Entry() : key() {}

  std::string key;
};
struct LocalArgs {
  LocalArgs() : notify(false),keep_xml(false) {}

  bool notify,keep_xml;
} local_args;
struct ThreadStruct {
  ThreadStruct() : strings(),removed_file_index(-1),file_removed(false),tid(0) {}

  std::vector<std::string> strings;
  int removed_file_index;
  bool file_removed;
  pthread_t tid;
};
std::string dsnum2;
std::unordered_set<std::string> cmd_set;
std::vector<std::string> files;
std::unordered_set<std::string> mss_tindex_set,web_tindex_set,inv_tindex_set;
std::unordered_set<std::string> mss_gindex_set,web_gindex_set;
bool removed_from_GrML,removed_from_WGrML,removed_from_ObML,removed_from_WObML,removed_from_SatML,removed_from_FixML,removed_from_WFixML;
bool create_MSS_filelist_cache=false,create_Web_filelist_cache=false,create_inv_filelist_cache=false;

void parse_args(MySQL::Server& server)
{
  auto arg_list=strutils::split(metautils::args.args_string,"%");
  for (auto arg=arg_list.begin(); arg != arg_list.end(); ++arg) {
    if (*arg == "-d") {
	++arg;
	metautils::args.dsnum=*arg;
	if (std::regex_search(metautils::args.dsnum,std::regex("^ds"))) {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
	dsnum2=strutils::substitute(metautils::args.dsnum,".","");
    }
    else if (*arg == "-G") {
	metautils::args.update_graphics=false;
    }
    else if (*arg == "-k") {
	local_args.keep_xml=true;
    }
    else if (*arg == "-N") {
	local_args.notify=true;
    }
    else {
	std::string cmd;
	if (std::regex_search(*arg,std::regex("^/FS/DECS/"))) {
	  cmd="fmd";
	}
	else {
	  cmd="wfmd";
	}
	cmd_set.emplace(cmd);
	if (std::regex_search(strutils::to_lower(*arg),std::regex("\\.htar$"))) {
	  if (cmd == "fmd") {
	    MySQL::LocalQuery query("mssID","GrML.ds"+dsnum2+"_primaries2","mssID like '"+*arg+"..m..%'");
	    if (query.submit(server) == 0) {
		MySQL::Row row;
		while (query.fetch_row(row)) {
		  files.emplace_back(row[0]);
		}
	    }
	  }
	}
	else {
	  files.emplace_back(*arg);
	}
    }
  }
  if (metautils::args.dsnum.empty()) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
}

std::string tempdir_name()
{
  static TempDir *tdir=nullptr;
  if (tdir == nullptr) {
    tdir=new TempDir;
    if (!tdir->create(metautils::directives.temp_path)) {
	metautils::log_error("unable to create temporary directory","dcm",USER);
    }
  }
  return tdir->name();
}

void copy_version_controlled_data(MySQL::Server& server,std::string db,std::string file_ID_code)
{
  std::string error;
  auto tnames=table_names(server,db,"ds"+dsnum2+"%",error);
  for (auto t : tnames) {
    t=db+"."+t;
    if (field_exists(server,t,"mssID_code")) {
	if (!table_exists(server,"V"+t)) {
	  if (server.command("create table V"+t+" like "+t,error) < 0) {
	    metautils::log_error("copy_version_controlled_table() returned error: "+server.error()+" while trying to create V"+t,"dcm",USER);
	  }
	}
	std::string result;
	server.command("insert into V"+t+" select * from "+t+" where mssID_code = "+file_ID_code,result);
    }
  }
}

void clear_tables_by_file_ID(std::string db,std::string file_ID_code,bool is_version_controlled)
{
  MySQL::Server local_server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!local_server) {
    metautils::log_error("clear_tables_by_file_ID() unable to connect to MySQL server while clearing fileID code "+file_ID_code+" from "+db,"dcm",USER);
  }
  if (is_version_controlled) {
    copy_version_controlled_data(local_server,db,file_ID_code);
  }
  std::string code_name;
  if (db[0] == 'W') {
    code_name="webID_code";
  }
  else {
    code_name="mssID_code";
  }
  std::string error;
  auto tnames=table_names(local_server,db,"ds"+dsnum2+"%",error);
  for (auto t : tnames) {
    t=db+"."+t;
    if (field_exists(local_server,t,code_name)) {
	if (local_server._delete(t,code_name+" = "+file_ID_code) < 0) {
	  metautils::log_error("clear_tables_by_file_ID() returned error: "+local_server.error()+" while clearing "+t,"dcm",USER);
	}
    }
  }
  local_server.disconnect();
}

void clear_grid_cache(MySQL::Server& server,std::string db)
{
  MySQL::LocalQuery query("select parameter,levelType_codes,min(start_date),max(end_date) from "+db+".ds"+dsnum2+"_agrids group by parameter,levelType_codes");
  if (query.submit(server) < 0) {
    metautils::log_error("clear_grid_cache() returned error: "+query.error()+" while trying to rebuild grid cache","dcm",USER);
  }
  std::string result;
  server.command("lock tables "+db+".ds"+dsnum2+"_agrids_cache write",result);
  if (server._delete(db+".ds"+dsnum2+"_agrids_cache") < 0) {
    metautils::log_error("clear_grid_cache() returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_agrids_cache","dcm",USER);
  }
  MySQL::Row row;
  while (query.fetch_row(row)) {
    if (server.insert(db+".ds"+dsnum2+"_agrids_cache","'"+row[0]+"','"+row[1]+"',"+row[2]+","+row[3]) < 0) {
	metautils::log_error("clear_grid_cache() returned error: "+server.error()+" while inserting into "+db+".ds"+dsnum2+"_agrids_cache","dcm",USER);
    }
  }
  server.command("unlock tables",result);
}

bool remove_from(std::string database,std::string table_ext,std::string file_field_name,std::string md_directory,std::string& file,std::string file_ext,std::string& file_ID_code,bool& is_version_controlled)
{
  is_version_controlled=false;
  std::string error;
  if (md_directory == "wfmd" && std::regex_search(file,std::regex("^http(s){0,1}://(rda|dss)\\.ucar\\.edu"))) {
    file=metautils::relative_web_filename(file);
  }
  auto file_table=database+".ds"+dsnum2+table_ext;
  MySQL::Server local_server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!local_server) {
    metautils::log_error("remove_from() unable to connect to MySQL server while removing  "+file,"dcm",USER);
  }
  MySQL::LocalQuery query("code",file_table,file_field_name+" = '"+file+"'");
  if (query.submit(local_server) == 0) {
    if (query.num_rows() == 1) {
	MySQL::Row row;
	query.fetch_row(row);
	file_ID_code=row[0];
	if (md_directory == "fmd") {
// check for a primary moved to version-controlled
	  query.set("mssid","dssdb.mssfile","dsid = 'ds"+metautils::args.dsnum+"' and mssfile = '"+file+"' and property = 'V'");
	  if (query.submit(local_server) < 0) {
	    metautils::log_error("remove_from() returned error: "+local_server.error()+" while trying to check mssfile property for '"+file+"'","dcm",USER);
	  }
	  if (query.num_rows() == 1) {
	    is_version_controlled=true;
	  }
	}
	if (is_version_controlled) {
	  std::string result;
	  if (!table_exists(local_server,"V"+file_table)) {
	    if (local_server.command("create table V"+file_table+" like "+file_table,error) < 0) {
		metautils::log_error("remove_from() returned error: "+local_server.error()+" while trying to create V"+file_table,"dcm",USER);
	    }
	  }
	  local_server.command("insert into V"+file_table+" select * from "+file_table+" where code = "+file_ID_code,result);
	}
	if (local_server._delete(file_table,"code = "+file_ID_code) < 0) {
	  metautils::log_error("remove_from() returned error: "+local_server.error(),"dcm",USER);
	}
	if (database == "WGrML" || database == "WObML") {
	  auto tables=MySQL::table_names(local_server,strutils::substitute(database,"W","I"),"ds"+dsnum2+"_inventory_%",error);
	  for (const auto& table : tables) {
	    local_server._delete(strutils::substitute(database,"W","I")+".`"+table+"`","webID_code = "+file_ID_code);
	  }
	  if (local_server._delete(strutils::substitute(database,"W","I")+".ds"+dsnum2+"_inventory_summary","webID_code = "+file_ID_code) == 0) {
	    query.set("select tindex from dssdb.wfile where wfile = '"+file+"'");
	    if (query.submit(local_server) == 0 && query.num_rows() == 1) {
		query.fetch_row(row);
		if (!row[0].empty() && row[0] != "0") {
		  inv_tindex_set.emplace(row[0]);
		}
	    }
	    create_inv_filelist_cache=true;
	  }
	}
	if (database == "WObML") {
	  local_server._delete("I"+database.substr(1)+".ds"+dsnum2+"_dataTypes","webID_code = "+file_ID_code);
	  auto tables=MySQL::table_names(local_server,"I"+database.substr(1),"ds"+dsnum2+"_timeSeries_times_%",error);
	  for (const auto& table : tables) {
	    local_server._delete("I"+database.substr(1)+"."+table,"webID_code = "+file_ID_code);
	  }
	}
	auto md_file=file;
	if (md_directory == "fmd") {
	  strutils::replace_all(md_file,"/FS/DECS/","");
	}
	strutils::replace_all(md_file,"/","%");
	md_file+=file_ext;
	if (!local_args.keep_xml) {
	  std::unique_ptr<TempDir> tdir;
	  if (is_version_controlled) {
	    tdir.reset(new TempDir);
	    if (!tdir->create(metautils::directives.temp_path)) {
		metautils::log_error("remove_from() could not create a temporary directory","dcm",USER);
	    }
	    std::stringstream oss,ess;
	    if (unixutils::mysystem2("/bin/mkdir -p "+tdir->name()+"/metadata/"+md_directory+"/v",oss,ess) < 0) {
		metautils::log_error("remove_from() could not create the temporary directory tree","dcm",USER);
	    }
	  }
	  short flag=0;
	  if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file+".gz",metautils::directives.rdadata_home)) {
	    flag=1;
	  }
	  else if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file,metautils::directives.rdadata_home)) {
	    flag=2;
	  }
	  if (file_ext == ".ObML" && flag > 0) {
	    std::string xml_parent;
	    if (flag == 1) {
		xml_parent=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file+".gz",tempdir_name());
		struct stat buf;
		if (stat(xml_parent.c_str(),&buf) == 0) {
		  system(("gunzip "+xml_parent).c_str());
		  strutils::chop(xml_parent,3);
		}
	    }
	    else if (flag == 2) {
		xml_parent=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file,tempdir_name());
	    }
	    XMLDocument xdoc;
	    if (xdoc.open(xml_parent)) {
		auto elist=xdoc.element_list("ObML/observationType/platform/IDs");
		for (const auto& e : elist) {
		  if (is_version_controlled) {
		    auto f=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),tdir->name()+"/metadata/"+md_directory+"/v");
		    if (f.empty()) {
			metautils::log_error("remove_from() could not get remote file https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),"dcm",USER);
		    }
		  }
		  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),metautils::directives.rdadata_home,error) < 0) {
		    metautils::log_warning("unable to unsync "+e.attribute_value("ref"),"dcm",USER);
		  }
		}
		elist=xdoc.element_list("ObML/observationType/platform/locations");
		for (const auto& e : elist) {
		  if (is_version_controlled) {
		    auto f=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),tdir->name()+"/metadata/"+md_directory+"/v");
		    if (f.empty()) {
			metautils::log_error("remove_from() could not get remote file https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),"dcm",USER);
		    }
		  }
		  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),metautils::directives.rdadata_home,error) < 0) {
		    metautils::log_warning("unable to unsync "+e.attribute_value("ref"),"dcm",USER);
		  }
		}
		xdoc.close();
	    }
	  }
	  if (md_directory == "wfmd") {
	    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+md_file+"_inv.gz",metautils::directives.rdadata_home)) {
		if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+md_file+"_inv.gz",metautils::directives.rdadata_home,error) < 0) {
		  metautils::log_warning("unable to unsync "+md_file+"_inv","dcm",USER);
		}
	    }
	    else if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+md_file+"_inv",metautils::directives.rdadata_home)) {
		if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/inv/"+md_file+"_inv",metautils::directives.rdadata_home,error) < 0) {
		  metautils::log_warning("unable to unsync "+md_file+"_inv","dcm",USER);
		}
	    }
	    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/wms/"+md_file+".gz",metautils::directives.rdadata_home)) {
		if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/wms/"+md_file+".gz",metautils::directives.rdadata_home,error) < 0) {
		  metautils::log_warning("unable to unsync wms file "+md_file,"dcm",USER);
		}
	    }

	  }
	  if (flag == 1) {
	    md_file+=".gz";
	  }
	  if (is_version_controlled) {
	    auto f=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file,tdir->name()+"/metadata/"+md_directory+"/v");
	    if (!f.empty()) {
		std::string error;
		if (unixutils::rdadata_sync(tdir->name(),"metadata/"+md_directory+"/v/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
		  metautils::log_error("unable to move version-controlled metadata file "+file+"; error: "+error,"dcm",USER);
		}
	    }
	  }
	  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+md_directory+"/"+md_file,metautils::directives.rdadata_home,error) < 0) {
	    metautils::log_warning("unable to unsync "+md_file,"dcm",USER);
	  }
	}
	return true;
    }
  }
  local_server.disconnect();
  return false;
}

extern "C" void *t_removed(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;
  auto file=t->strings[0];
  bool file_removed=false;
  std::string file_ID_code;
  bool is_version_controlled;
  if (std::regex_search(file,std::regex("^/FS/DECS/"))) {
    auto was_removed=remove_from("GrML","_primaries2","mssID","fmd",file,".GrML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("GrML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_GrML=true;
    }
    was_removed=remove_from("ObML","_primaries2","mssID","fmd",file,".ObML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("ObML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_ObML=true;
    }
    was_removed=remove_from("SatML","_primaries2","mssID","fmd",file,".SatML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("SatML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_SatML=true;
    }
    was_removed=remove_from("FixML","_primaries2","mssID","fmd",file,".FixML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("FixML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_FixML=true;
    }
    MySQL::Server server_m(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
    MySQL::LocalQuery query("gindex","mssfile","mssfile = '"+file+"'");
    MySQL::Row row;
    if (query.submit(server_m) == 0 && query.fetch_row(row)) {
	mss_gindex_set.emplace(row[0]);
    }
    server_m.disconnect();
    MySQL::Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"");
    if (file_removed) {
	query.set("tindex","mssfile","mssfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (!row[0].empty() && row[0] != "0") {
	    mss_tindex_set.emplace(row[0]);
	  }
	}
	create_MSS_filelist_cache=true;
    }
    server_d.update("mssfile","meta_link = NULL","dsid = 'ds"+metautils::args.dsnum+"' and mssfile = '"+file+"'");
    server_d.disconnect();
  }
  else {
    auto was_removed=remove_from("WGrML","_webfiles2","webID","wfmd",file,".GrML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("WGrML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_WGrML=true;
    }
    was_removed=remove_from("WObML","_webfiles2","webID","wfmd",file,".ObML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("WObML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_WObML=true;
    }
    was_removed=remove_from("WFixML","_webfiles2","webID","wfmd",file,".FixML",file_ID_code,is_version_controlled);
    if (was_removed) {
	clear_tables_by_file_ID("WFixML",file_ID_code,is_version_controlled);
	file_removed=true;
	removed_from_WFixML=true;
    }
    MySQL::Server server_m(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
    MySQL::LocalQuery query("gindex","wfile","wfile = '"+file+"'");
    MySQL::Row row;
    if (query.submit(server_m) == 0 && query.fetch_row(row)) {
	web_gindex_set.emplace(row[0]);
    }
    server_m.disconnect();
    MySQL::Server server_d(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"");
    if (file_removed) {
	query.set("tindex","wfile","wfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (!row[0].empty() && row[0] != "0") {
	    web_tindex_set.emplace(row[0]);
	  }
	}
	create_Web_filelist_cache=true;
    }
    server_d.update("dssdb.wfile","meta_link = NULL","dsid = 'ds"+metautils::args.dsnum+"' and wfile = '"+file+"'");
    server_d.disconnect();
  }
  t->file_removed=file_removed;
  if (gatherxml::verbose_operation) {
    if (t->file_removed) {
	std::cout << "... " << t->strings[0] << " was successfully removed." << std::endl;
    }
    else {
	std::cout << "... " << t->strings[0] << " was NOT removed." << std::endl;
    }
  }
  return nullptr;
}

void generate_graphics(std::string type)
{
  if (type == "mss") {
    type="gsi";
  }
  else if (type == "web") {
    type="wgsi";
  }
  std::stringstream oss,ess;
  unixutils::mysystem2(metautils::directives.local_root+"/bin/"+type+" "+metautils::args.dsnum,oss,ess);
}

void generate_dataset_home_page()
{
  std::stringstream oss,ess;
  unixutils::mysystem2(metautils::directives.local_root+"/bin/dsgen "+metautils::args.dsnum,oss,ess);
}

extern "C" void *t_index_variables(void *)
{
  MySQL::Server srv(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  searchutils::index_variables(srv,metautils::args.dsnum);
  srv.disconnect();
  return nullptr;
}

extern "C" void *t_summarize_frequencies(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  gatherxml::summarizeMetadata::summarize_frequencies(t->strings[0],"dcm",USER);
  return nullptr;
}

extern "C" void *t_summarize_grids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  gatherxml::summarizeMetadata::summarize_grids(t->strings[0],"dcm",USER);
  return nullptr;
}

extern "C" void *t_aggregate_grids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  gatherxml::summarizeMetadata::aggregate_grids(t->strings[0],"dcm",USER);
  return nullptr;
}

extern "C" void *t_summarize_grid_levels(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  gatherxml::summarizeMetadata::summarize_grid_levels(t->strings[0],"dcm",USER);
  return nullptr;
}

extern "C" void *t_summarize_grid_resolutions(void *)
{
  gatherxml::summarizeMetadata::summarize_grid_resolutions("dcm",USER,"");
  return nullptr;
}

extern "C" void *t_create_file_list_cache(void *)
{
  gatherxml::summarizeMetadata::create_file_list_cache("MSS","dcm",USER);
  return nullptr;
}

extern "C" void *t_generate_detailed_metadata_view(void *)
{
  gatherxml::detailedMetadata::generate_detailed_metadata_view("dcm",USER);
  return nullptr;
}

int main(int argc,char **argv)
{
  if (argc < 4) {
    std::cerr << "usage: dcm -d <nnn.n> [options...] files..." << std::endl;
    std::cerr << std::endl;
    std::cerr << "purpose:" << std::endl;
    std::cerr << "dcm deletes content metadata from the RDADB for the specified files." << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>  nnn.n is the dataset number to which the data file(s) belong" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    std::cerr << "-g/-G       generate (default)/don't generate graphics for ObML files" << std::endl;
    std::cerr << "-N          notify with a message when dcm completes" << std::endl;
    std::cerr << std::endl;
    std::cerr << "NOTES:" << std::endl;
    std::cerr << "  1) each file in <files...> must begin with \"/FS/DECS/\" or" << std::endl;
    std::cerr << "       \"https://rda.ucar.edu\", or be specified as for the -WF option of" << std::endl;
    std::cerr << "       dsarch" << std::endl;
    std::cerr << "  2) for files that support individual members, you can either specify the" << std::endl;
    std::cerr << "       name of the parent file and metadata for all members will be deleted," << std::endl;
    std::cerr << "       or you can specify the name of an individual member as" << std::endl;
    std::cerr << "       \"parent..m..member\" and the metadata for just that member will be" << std::endl;
    std::cerr << "       deleted" << std::endl;
    exit(1);
  }
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,'%');
  metautils::read_config("dcm",USER);
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("unable to connect to MySQL server on startup","dcm",USER);
  }
  parse_args(server);
/*
  if (metautils::args.dsnum == "999.9") {
    exit(0);
  }
*/
  for (auto cmd=cmd_set.begin(); cmd != cmd_set.end(); ) {
    if (!unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+*cmd,metautils::directives.rdadata_home)) {
	cmd_set.erase(cmd);
    }
    else {
	++cmd;
    }
  }
  if (cmd_set.size() == 0) {
    exit(0);
  }
  removed_from_GrML=false;
  removed_from_WGrML=false;
  removed_from_ObML=false;
  removed_from_WObML=false;
  removed_from_SatML=false;
  removed_from_FixML=false;
  removed_from_WFixML=false;
  const size_t MAX_NUM_THREADS=6;
  std::vector<ThreadStruct> threads(MAX_NUM_THREADS);
  std::vector<pthread_t> tid_list;
  for (size_t n=0; n < files.size(); ++n) {
    if (tid_list.size() < MAX_NUM_THREADS) {
	threads[tid_list.size()].strings.clear();
	threads[tid_list.size()].strings.emplace_back(files[n]);
	threads[tid_list.size()].removed_file_index=n;
	threads[tid_list.size()].file_removed=false;
	pthread_create(&threads[tid_list.size()].tid,nullptr,t_removed,&threads[tid_list.size()]);
	pthread_detach(threads[tid_list.size()].tid);
	tid_list.emplace_back(threads[tid_list.size()].tid);
	if (gatherxml::verbose_operation) {
	  std::cout << "thread created for removal of " << files[n] << " ..." << std::endl;
	}
    }
    else {
	if (gatherxml::verbose_operation) {
	  std::cout << "... at maximum thread capacity, waiting for an available slot ..." << std::endl;
	}
	size_t free_tid_idx=0xffffffffffffffff;
	while (1) {
	  for (size_t m=0; m < MAX_NUM_THREADS; ++m) {
	    if (pthread_kill(tid_list[m],0) != 0) {
		if (threads[m].file_removed) {
		  files[threads[m].removed_file_index].clear();
		  if (gatherxml::verbose_operation) {
		    std::cout << "File " << files[threads[m].removed_file_index] << "/" << files.size() << " cleared." << std::endl;
		  }
		}
		free_tid_idx=m;
		break;
	    }
	  }
	  if (free_tid_idx < 0xffffffffffffffff) {
	    break;
	  }
	}
	threads[free_tid_idx].strings.clear();
	threads[free_tid_idx].strings.emplace_back(files[n]);
	threads[free_tid_idx].removed_file_index=n;
	threads[free_tid_idx].file_removed=false;
	pthread_create(&threads[free_tid_idx].tid,nullptr,t_removed,&threads[free_tid_idx]);
	pthread_detach(threads[free_tid_idx].tid);
	tid_list[free_tid_idx]=threads[free_tid_idx].tid;
	if (gatherxml::verbose_operation) {
	  std::cout << "available thread slot at " << free_tid_idx << " found, thread created for removal of " << files[n] << " ..." << std::endl;
	}
    }
  }
  auto found_thread=true;
  while (found_thread) {
    found_thread=false;
    for (size_t n=0; n < tid_list.size(); ++n) {
	if (tid_list[n] < 0xffffffffffffffff) {
	  if (pthread_kill(tid_list[n],0) != 0) {
	    if (threads[n].file_removed) {
		files[threads[n].removed_file_index].clear();
		if (gatherxml::verbose_operation) {
		  std::cout << "File " << files[threads[n].removed_file_index] << "/" << files.size() << " cleared." << std::endl;
		}
	    }
	    tid_list[n]=0xffffffffffffffff;
	  }
	  else {
	    found_thread=true;
	  }
	}
    }
  }
  for (const auto& gindex : mss_gindex_set) {
    gatherxml::detailedMetadata::generate_group_detailed_metadata_view(gindex,"MSS","dcm",USER);
  }
  for (const auto& gindex : web_gindex_set) {
    gatherxml::detailedMetadata::generate_group_detailed_metadata_view(gindex,"Web","dcm",USER);
  }
  std::stringstream oss,ess;
  if (removed_from_GrML) {
    clear_grid_cache(server,"GrML");
    threads.clear();
    threads.resize(8);
    pthread_create(&threads[0].tid,NULL,t_index_variables,NULL);
    threads[1].strings.emplace_back("GrML");
    pthread_create(&threads[1].tid,NULL,t_summarize_frequencies,&threads[1]);
    threads[2].strings.emplace_back("GrML");
    pthread_create(&threads[2].tid,NULL,t_summarize_grids,&threads[2]);
    threads[3].strings.emplace_back("GrML");
    pthread_create(&threads[3].tid,NULL,t_aggregate_grids,&threads[3]);
    threads[4].strings.emplace_back("GrML");
    pthread_create(&threads[4].tid,NULL,t_summarize_grid_levels,&threads[4]);
    pthread_create(&threads[5].tid,NULL,t_summarize_grid_resolutions,NULL);
    pthread_create(&threads[6].tid,NULL,t_generate_detailed_metadata_view,NULL);
    if (create_MSS_filelist_cache) {
	pthread_create(&threads[7].tid,NULL,t_create_file_list_cache,NULL);
    }
    for (const auto& t : threads) {
	pthread_join(t.tid,NULL);
    }
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    generate_dataset_home_page();
  }
  if (removed_from_WGrML) {
    clear_grid_cache(server,"WGrML");
    gatherxml::summarizeMetadata::summarize_grids("WGrML","dcm",USER);
    gatherxml::summarizeMetadata::aggregate_grids("WGrML","dcm",USER);
    gatherxml::summarizeMetadata::summarize_grid_levels("WGrML","dcm",USER);
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -ri",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (removed_from_ObML) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm",USER);
    gatherxml::summarizeMetadata::summarize_obs_data("dcm",USER);
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
    if (metautils::args.update_graphics) {
	generate_graphics("mss");
    }
    generate_dataset_home_page();
  }
  if (removed_from_WObML) {
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (metautils::args.update_graphics) {
	generate_graphics("web");
    }
  }
  if (removed_from_SatML) {
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
  }
  if (removed_from_FixML) {
    gatherxml::summarizeMetadata::summarize_frequencies("dcm",USER);
    gatherxml::summarizeMetadata::summarize_fix_data("dcm",USER);
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (metautils::args.update_graphics) {
	generate_graphics("mss");
    }
    generate_dataset_home_page();
  }
  if (removed_from_WFixML) {
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (metautils::args.update_graphics) {
	generate_graphics("web");
    }
  }
  for (const auto& tindex : mss_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("MSS","dcm",USER,tindex);
  }
  if (create_Web_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web","dcm",USER);
  }
  for (const auto& tindex : web_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("Web","dcm",USER,tindex);
  }
  if (create_inv_filelist_cache) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv","dcm",USER);
  }
  for (const auto& tindex : inv_tindex_set) {
    gatherxml::summarizeMetadata::create_file_list_cache("inv","dcm",USER,tindex);
  }
  for (auto it=files.begin(); it != files.end(); ++it) {
    if (it->empty()) {
	files.erase(it--);
    }
  }
  if (files.size() > 0) {
    std::cerr << "Warning: content metadata for the following files was not removed (maybe it never existed?):";
    for (const auto& file : files) {
	std::cerr << " " << file;
    }
    std::cerr << std::endl;
  }
  server.disconnect();
  server.connect(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"");
  if (server) {
    server.update("dataset","version = version+1","dsid = 'ds"+metautils::args.dsnum+"'");
    server.disconnect();
  }
  if (files.size() == 0 && local_args.notify) {
    std::cout << "dcm has completed successfully" << std::endl;
  }
}
