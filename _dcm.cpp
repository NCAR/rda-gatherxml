#include <iostream>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <search.hpp>

metautils::Directives directives;
metautils::Args args;
struct Entry {
  Entry() : key() {}

  std::string key;
};
struct LocalArgs {
  LocalArgs() : notify(false),keep_xml(false) {}

  bool notify,keep_xml;
} local_args;
struct ThreadStruct {
  ThreadStruct() : strings(),tid(0) {}

  std::vector<std::string> strings;
  pthread_t tid;
};
MySQL::Server server;
std::string user=getenv("USER");
std::string dsnum2;
std::unordered_set<std::string> cmd_set;
std::vector<std::string> files;
std::unordered_set<std::string> mss_tindex_set,web_tindex_set,inv_tindex_set;
std::unordered_set<std::string> mss_gindex_set,web_gindex_set;
bool removed_from_GrML,removed_from_WGrML,removed_from_ObML,removed_from_WObML,removed_from_SatML,removed_from_FixML,removed_from_WFixML;
bool create_MSS_filelist_cache=false,create_Web_filelist_cache=false,create_inv_filelist_cache=false;

void parse_args()
{
  args.updateGraphics=true;
  auto arg_list=strutils::split(args.argsString,"%");
  for (auto arg=arg_list.begin(); arg != arg_list.end(); ++arg) {
    if (*arg == "-d") {
	++arg;
	args.dsnum=*arg;
	if (std::regex_search(args.dsnum,std::regex("^ds"))) {
	  args.dsnum=args.dsnum.substr(2);
	}
	dsnum2=strutils::substitute(args.dsnum,".","");
    }
    else if (*arg == "-G") {
	args.updateGraphics=false;
    }
    else if (*arg == "-k") {
	local_args.keep_xml=true;
    }
    else if (*arg == "-N") {
	local_args.notify=true;
    }
    else {
	std::string cmd;
	if (std::regex_search(*arg,std::regex("^/FS/DSS/")) || std::regex_search(*arg,std::regex("^/DSS/"))) {
	  cmd="fmd";
	}
	else {
	  cmd="wfmd";
	}
	cmd_set.emplace(cmd);
	if (std::regex_search(strutils::to_lower(*arg),std::regex("\\.htar$"))) {
	  if (cmd == "fmd") {
	    MySQL::LocalQuery query("mssID","GrML.ds"+dsnum2+"_primaries","mssID like '"+*arg+"..m..%'");
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
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
}

std::string tempdir_name()
{
  static TempDir *tdir=nullptr;
  if (tdir == nullptr) {
    tdir=new TempDir;
    if (!tdir->create("/glade/scratch/rdadata")) {
	metautils::logError("unable to create temporary directory in /glade/scratch/rdadata","dcm",user,args.argsString);
    }
  }
  return tdir->name();
}

void copy_version_controlled_data(std::string db,std::string fileID_code)
{
  std::string error;
  auto tnames=table_names(server,db,"ds"+dsnum2+"%",error);
  for (auto t : tnames) {
    t=db+"."+t;
    if (field_exists(server,t,"mssID_code")) {
	if (!table_exists(server,"V"+t)) {
	  if (server.command("create table V"+t+" like "+t,error) < 0) {
	    metautils::logError("copy_version_controlled_table() returned error: "+server.error()+" while trying to create V"+t,"dcm",user,args.argsString);
	  }
	}
	std::string result;
	server.command("insert into V"+t+" select * from "+t+" where mssID_code = "+fileID_code,result);
    }
  }
}

void clear_tables_by_file_ID(std::string db,std::string fileID_code,bool is_version_controlled)
{
  if (is_version_controlled) {
    copy_version_controlled_data(db,fileID_code);
  }
  std::string code_name;
  if (db[0] == 'W') {
    code_name="webID_code";
  }
  else {
    code_name="mssID_code";
  }
  std::string error;
  auto tnames=table_names(server,db,"ds"+dsnum2+"%",error);
  for (auto t : tnames) {
    t=db+"."+t;
    if (field_exists(server,t,code_name)) {
	if (server._delete(t,code_name+" = "+fileID_code) < 0) {
	  metautils::logError("clear_tables_by_file_ID() returned error: "+server.error()+" while clearing "+t,"dcm",user,args.argsString);
	}
    }
  }
}

void clear_grid_cache(std::string db)
{
  MySQL::LocalQuery query("select parameter,levelType_codes,min(start_date),max(end_date) from "+db+".ds"+dsnum2+"_agrids group by parameter,levelType_codes");
  if (query.submit(server) < 0) {
    metautils::logError("clear_grid_cache() returned error: "+query.error()+" while trying to rebuild grid cache","dcm",user,args.argsString);
  }
  std::string result;
  server.command("lock tables "+db+".ds"+dsnum2+"_agrids_cache write",result);
  if (server._delete(db+".ds"+dsnum2+"_agrids_cache") < 0) {
    metautils::logError("clear_grid_cache() returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_agrids_cache","dcm",user,args.argsString);
  }
  MySQL::Row row;
  while (query.fetch_row(row)) {
    if (server.insert(db+".ds"+dsnum2+"_agrids_cache","'"+row[0]+"','"+row[1]+"',"+row[2]+","+row[3]) < 0) {
	metautils::logError("clear_grid_cache() returned error: "+server.error()+" while inserting into "+db+".ds"+dsnum2+"_agrids_cache","dcm",user,args.argsString);
    }
  }
  server.command("unlock tables",result);
}

void move_version_controlled_file(std::string md_directory,std::string file)
{
  auto f=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+file,tempdir_name());
  if (!f.empty()) {
    std::stringstream ess;
    makeRDAWebDirectory("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/v",ess);
  }
  else {
    metautils::logError("unable to get remote file https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+file,"dcm",user,args.argsString);
  }
  std::string error;
  if (hostSync(f,"/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/v/"+file,error) < 0) {
    metautils::logError("unable to move version-controlled metadata file "+file+"; error: "+error,"dcm",user,args.argsString);
  }
}

bool remove_from(std::string database,std::string table_ext,std::string file_field_name,std::string md_directory,std::string& file,std::string file_ext,std::string& fileID_code,bool& is_version_controlled)
{
  is_version_controlled=false;
  std::string error;
  if (md_directory == "wfmd") {
    file=metautils::getRelativeWebFilename(file);
  }
  auto file_table=database+".ds"+dsnum2+table_ext;
  MySQL::LocalQuery query("code",file_table,file_field_name+" = '"+file+"'");
  if (query.submit(server) == 0) {
    if (query.num_rows() == 1) {
	MySQL::Row row;
	query.fetch_row(row);
	fileID_code=row[0];
	if (md_directory == "fmd") {
// check for a primary moved to version-controlled
	  query.set("mssid","dssdb.mssfile","dsid = 'ds"+args.dsnum+"' and mssfile = '"+file+"' and property = 'V'");
	  if (query.submit(server) < 0) {
	    metautils::logError("remove_from() returned error: "+server.error()+" while trying to check mssfile property for '"+file+"'","dcm",user,args.argsString);
	  }
	  if (query.num_rows() == 1) {
	    is_version_controlled=true;
	  }
	}
	if (is_version_controlled) {
	  std::string result;
	  if (!table_exists(server,"V"+file_table)) {
	    if (server.command("create table V"+file_table+" like "+file_table,error) < 0) {
		metautils::logError("remove_from() returned error: "+server.error()+" while trying to create V"+file_table,"dcm",user,args.argsString);
	    }
	  }
	  server.command("insert into V"+file_table+" select * from "+file_table+" where code = "+fileID_code,result);
	}
	if (server._delete(file_table,"code = "+fileID_code) < 0) {
	  metautils::logError("remove_from() returned error: "+server.error(),"dcm",user,args.argsString);
	}
	if (database == "WGrML" || database == "WObML") {
	  auto tables=MySQL::table_names(server,strutils::substitute(database,"W","I"),"ds"+dsnum2+"_inventory_%",error);
	  for (const auto& table : tables) {
	    server._delete(strutils::substitute(database,"W","I")+".`"+table+"`","webID_code = "+fileID_code);
	  }
	  if (server._delete(strutils::substitute(database,"W","I")+".ds"+dsnum2+"_inventory_summary","webID_code = "+fileID_code) == 0) {
	    query.set("select tindex from dssdb.wfile where wfile = '"+file+"'");
	    if (query.submit(server) == 0 && query.num_rows() == 1) {
		query.fetch_row(row);
		if (row[0].length() > 0 && row[0] != "0") {
		  inv_tindex_set.emplace(row[0]);
		}
	    }
	    create_inv_filelist_cache=true;
	  }
	}
	if (database == "WObML") {
	  server._delete("I"+database.substr(1)+".ds"+dsnum2+"_dataTypes","webID_code = "+fileID_code);
	  auto tables=MySQL::table_names(server,"I"+database.substr(1),"ds"+dsnum2+"_timeSeries_times_%",error);
	  for (const auto& table : tables) {
	    server._delete("I"+database.substr(1)+"."+table,"webID_code = "+fileID_code);
	  }
	}
	auto md_file=file;
	if (md_directory == "fmd") {
	  strutils::replace_all(md_file,"/FS/DSS/","");
	  strutils::replace_all(md_file,"/DSS/","");
	}
	strutils::replace_all(md_file,"/","%");
	md_file+=file_ext;
	if (!local_args.keep_xml) {
	  struct stat buf;
	  short flag=0;
	  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+md_file+".gz",buf)) {
	    flag=1;
	  }
	  else if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+md_file,buf)) {
	    flag=2;
	  }
	  if (file_ext == ".ObML" && flag > 0) {
	    std::string xml_parent;
	    if (flag == 1) {
		xml_parent=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+md_file+".gz",tempdir_name());
		if (stat(xml_parent.c_str(),&buf) == 0) {
		  system(("gunzip "+xml_parent).c_str());
		  strutils::chop(xml_parent,3);
		}
	    }
	    else if (flag == 2) {
		xml_parent=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+md_file,tempdir_name());
	    }
	    XMLDocument xdoc;
	    if (xdoc.open(xml_parent)) {
		auto elist=xdoc.element_list("ObML/observationType/platform/IDs");
		for (const auto& e : elist) {
		  if (is_version_controlled) {
		    move_version_controlled_file(md_directory,e.attribute_value("ref"));
		  }
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),error) < 0) {
		    metautils::logWarning("unable to unsync "+e.attribute_value("ref"),"dcm",user,args.argsString);
		  }
		}
		elist=xdoc.element_list("ObML/observationType/platform/locations");
		for (const auto& e : elist) {
		  if (is_version_controlled) {
		    move_version_controlled_file(md_directory,e.attribute_value("ref"));
		  }
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),error) < 0) {
		    metautils::logWarning("unable to unsync "+e.attribute_value("ref"),"dcm",user,args.argsString);
		  }
		}
		xdoc.close();
	    }
	    if (md_directory == "wfmd") {
		if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/inv/"+md_file+"_inv.gz",buf)) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/inv/"+md_file+"_inv.gz",error) < 0) {
		    metautils::logWarning("unable to unsync "+md_file+"_inv","dcm",user,args.argsString);
		  }
		}
		else if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/inv/"+md_file+"_inv",buf)) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/inv/"+md_file+"_inv",error) < 0) {
		    metautils::logWarning("unable to unsync "+md_file+"_inv","dcm",user,args.argsString);
		  }
		}
	    }
	  }
	  if (flag == 1) {
	    md_file+=".gz";
	  }
	  if (is_version_controlled) {
	    move_version_controlled_file(md_directory,md_file);
	  }
	  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+md_file,error) < 0) {
	    metautils::logWarning("unable to unsync "+md_file,"dcm",user,args.argsString);
	  }
	}
	return true;
    }
  }
  return false;
}

bool removed(std::string file)
{
  bool file_removed=false;
  std::string fileID_code;
  bool is_version_controlled;
  if (std::regex_search(file,std::regex("^/FS/DSS/")) || std::regex_search(file,std::regex("^/DSS/"))) {
    if ( (removed_from_GrML=remove_from("GrML","_primaries","mssID","fmd",file,".GrML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("GrML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    if ( (removed_from_ObML=remove_from("ObML","_primaries","mssID","fmd",file,".ObML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("ObML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    if ( (removed_from_SatML=remove_from("SatML","_primaries","mssID","fmd",file,".SatML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("SatML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    if ( (removed_from_FixML=remove_from("FixML","_primaries","mssID","fmd",file,".FixML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("FixML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    MySQL::LocalQuery query("gindex","mssfile","mssfile = '"+file+"'");
    MySQL::Row row;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	mss_gindex_set.emplace(row[0]);
    }
    MySQL::Server server_d;
    metautils::connectToRDAServer(server_d);
    if (file_removed) {
	query.set("tindex","mssfile","mssfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (row[0].length() > 0 && row[0] != "0") {
	    mss_tindex_set.emplace(row[0]);
	  }
	}
	create_MSS_filelist_cache=true;
    }
    server_d.update("mssfile","meta_link = NULL","dsid = 'ds"+args.dsnum+"' and mssfile = '"+file+"'");
    server_d.disconnect();
  }
  else {
    if ( (removed_from_WGrML=remove_from("WGrML","_webfiles","webID","wfmd",file,".GrML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("WGrML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    if ( (removed_from_WObML=remove_from("WObML","_webfiles","webID","wfmd",file,".ObML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("WObML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    if ( (removed_from_WFixML=remove_from("WFixML","_webfiles","webID","wfmd",file,".FixML",fileID_code,is_version_controlled))) {
	clear_tables_by_file_ID("WFixML",fileID_code,is_version_controlled);
	file_removed=true;
    }
    MySQL::LocalQuery query("gindex","wfile","wfile = '"+file+"'");
    MySQL::Row row;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	web_gindex_set.emplace(row[0]);
    }
    MySQL::Server server_d;
    metautils::connectToRDAServer(server_d);
    if (file_removed) {
	query.set("tindex","wfile","wfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (row[0].length() > 0 && row[0] != "0") {
	    web_tindex_set.emplace(row[0]);
	  }
	}
	create_Web_filelist_cache=true;
    }
    server_d.update("dssdb.wfile","meta_link = NULL","dsid = 'ds"+args.dsnum+"' and wfile = '"+file+"'");
    server_d.disconnect();
  }
  return file_removed;
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
  mysystem2(directives.localRoot+"/bin/"+type+" "+args.dsnum,oss,ess);
}

void generate_dataset_home_page()
{
  std::stringstream oss,ess;
  mysystem2(directives.localRoot+"/bin/dsgen "+args.dsnum,oss,ess);
}

extern "C" void *t_index_variables(void *)
{
  indexVariables(args.dsnum);
  return NULL;
}

extern "C" void *t_summarize_frequencies(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::summarizeFrequencies(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *t_summarize_grids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::grids::summarizeGrids(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *t_aggregate_grids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::grids::aggregateGrids(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *t_summarize_grid_levels(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::gridLevels::summarizeGridLevels(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *t_summarize_grid_resolutions(void *)
{
  summarizeMetadata::grids::summarizeGridResolutions(args.dsnum,"dcm",user,args.argsString,"");
  return NULL;
}

extern "C" void *t_create_file_list_cache(void *)
{
  summarizeMetadata::createFileListCache("MSS","dcm",user,args.argsString);
  return NULL;
}

extern "C" void *t_generate_detailed_metadata_view(void *)
{
  summarizeMetadata::detailedMetadata::generateDetailedMetadataView(args.dsnum,"dcm",user,args.argsString);
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
    std::cerr << "  1) each file in <files...> must begin with \"/FS/DSS/\", \"/DSS/\", or" << std::endl;
    std::cerr << "       \"https://rda.ucar.edu\"" << std::endl;
    std::cerr << "  2) for files that support individual members, you can either specify the" << std::endl;
    std::cerr << "       name of the parent file and metadata for all members will be deleted," << std::endl;
    std::cerr << "       or you can specify the name of an individual member as" << std::endl;
    std::cerr << "       \"parent..m..member\" and the metadata for just that member will be" << std::endl;
    std::cerr << "       deleted" << std::endl;
    exit(1);
  }
  args.argsString=getUnixArgsString(argc,argv,'%');
  metautils::readConfig("dcm",user,args.argsString);
  metautils::connectToMetadataServer(server);
  if (!server) {
    metautils::logError("unable to connect to MySQL:: server on startup","dcm",user,args.argsString);
  }
  parse_args();
/*
  if (args.dsnum == "999.9") {
    exit(0);
  }
*/
  for (auto cmd=cmd_set.begin(); cmd != cmd_set.end(); ) {
    struct stat buf;
    if (!existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+*cmd,buf)) {
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
  for (auto it=files.begin(); it != files.end(); ++it) {
    if (removed(*it)) {
	files.erase(it--);
    }
  }
  for (const auto& gindex : mss_gindex_set) {
    summarizeMetadata::detailedMetadata::generateGroupDetailedMetadataView(args.dsnum,gindex,"MSS","dcm",user,args.argsString);
  }
  for (const auto& gindex : web_gindex_set) {
    summarizeMetadata::detailedMetadata::generateGroupDetailedMetadataView(args.dsnum,gindex,"Web","dcm",user,args.argsString);
  }
  std::stringstream oss,ess;
  if (removed_from_GrML) {
    clear_grid_cache("GrML");
    std::vector<ThreadStruct> threads(8);
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
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    generate_dataset_home_page();
  }
  if (removed_from_WGrML) {
    clear_grid_cache("WGrML");
    summarizeMetadata::grids::summarizeGrids(args.dsnum,"WGrML","dcm",user,args.argsString);
    summarizeMetadata::grids::aggregateGrids(args.dsnum,"WGrML","dcm",user,args.argsString);
    summarizeMetadata::gridLevels::summarizeGridLevels(args.dsnum,"WGrML","dcm",user,args.argsString);
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -ri",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (removed_from_ObML) {
    summarizeMetadata::summarizeFrequencies(args.dsnum,"dcm",user,args.argsString);
    summarizeMetadata::obsData::summarizeObsData(args.dsnum,"dcm",user,args.argsString);
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
    if (args.updateGraphics) {
	generate_graphics("mss");
    }
    generate_dataset_home_page();
  }
  if (removed_from_WObML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generate_graphics("web");
    }
  }
  if (removed_from_SatML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
  }
  if (removed_from_FixML) {
    summarizeMetadata::summarizeFrequencies(args.dsnum,"dcm",user,args.argsString);
    summarizeMetadata::fixData::summarizeFixData(args.dsnum,"dcm",user,args.argsString);
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generate_graphics("mss");
    }
    generate_dataset_home_page();
  }
  if (removed_from_WFixML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generate_graphics("web");
    }
  }
  for (const auto& tindex : mss_tindex_set) {
    summarizeMetadata::createFileListCache("MSS","dcm",user,args.argsString,tindex);
  }
  if (create_Web_filelist_cache) {
    summarizeMetadata::createFileListCache("Web","dcm",user,args.argsString);
  }
  for (const auto& tindex : web_tindex_set) {
    summarizeMetadata::createFileListCache("Web","dcm",user,args.argsString,tindex);
  }
  if (create_inv_filelist_cache) {
    summarizeMetadata::createFileListCache("inv","dcm",user,args.argsString);
  }
  for (const auto& tindex : inv_tindex_set) {
    summarizeMetadata::createFileListCache("inv","dcm",user,args.argsString,tindex);
  }
  if (files.size() > 0) {
    std::cerr << "Warning: content metadata for the following files was not removed (maybe it never existed?):";
    for (const auto& file : files) {
	std::cerr << " " << file;
    }
    std::cerr << std::endl;
  }
  server.disconnect();
  metautils::connectToRDAServer(server);
  if (server) {
    server.update("dataset","version = version+1","dsid = 'ds"+args.dsnum+"'");
    server.disconnect();
  }
  if (files.size() == 0 && local_args.notify) {
    std::cout << "dcm has completed successfully" << std::endl;
  }
}
