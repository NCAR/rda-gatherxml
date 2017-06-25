#include <iostream>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <list>
#include <deque>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <mymap.hpp>
#include <metadata.hpp>
#include <search.hpp>

metautils::Directives directives;
metautils::Args args;
struct Entry {
  Entry() : key() {}

  std::string key;
};
my::map<Entry> mss_gindexTable,web_gindexTable;
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
std::list<std::string> files,cmd_list;
my::map<Entry> MSS_tindex_table,Web_tindex_table,inv_tindex_table;
bool removedFromGrML,removedFromWGrML,removedFromObML,removedFromWObML,removedFromSatML,removedFromFixML,removedFromWFixML;
bool create_MSS_filelist_cache=false,create_Web_filelist_cache=false,create_inv_filelist_cache=false;

void parseArgs()
{
  my::map<Entry> cmd_table;
  Entry e;
  MySQL::LocalQuery query;
  MySQL::Row row;

  auto sp=strutils::split(args.argsString,"%");
  args.updateGraphics=true;
  for (size_t n=0; n < sp.size(); n++) {
    if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (strutils::has_beginning(args.dsnum,"ds")) {
	  args.dsnum=args.dsnum.substr(2);
	}
	dsnum2=strutils::substitute(args.dsnum,".","");
    }
    else if (sp[n] == "-G") {
	args.updateGraphics=false;
    }
    else if (sp[n] == "-k") {
	local_args.keep_xml=true;
    }
    else if (sp[n] == "-N") {
	local_args.notify=true;
    }
    else {
	if (strutils::has_beginning(sp[n],"/FS/DSS/") || strutils::has_beginning(sp[n],"/DSS/")) {
	  if (!cmd_table.found("fmd",e)) {
	    e.key="fmd";
	    cmd_table.insert(e);
	    cmd_list.emplace_back(e.key);
	  }
	}
	else if (strutils::has_beginning(sp[n],"http://rda.ucar.edu")) {
	  if (!cmd_table.found("wfmd",e)) {
	    e.key="wfmd";
	    cmd_table.insert(e);
	    cmd_list.emplace_back(e.key);
	  }
	}
	else {
	  std::cerr << "Error: file names must begin with \"/FS/DSS/\", \"/DSS/\", or \"http://rda.ucar.edu\"" << std::endl;
	  exit(1);
	}
	if (strutils::has_ending(strutils::to_lower(sp[n]),".htar")) {
	  if (e.key == "fmd") {
	    query.set("mssID","GrML.ds"+dsnum2+"_primaries","mssID like '"+sp[n]+"..m..%'");
	    if (query.submit(server) == 0) {
		while (query.fetch_row(row)) {
		  files.emplace_back(row[0]);
		}
	    }
	  }
	}
	else {
	  files.emplace_back(sp[n]);
	}
    }
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset specified" << std::endl;
    exit(1);
  }
}

void clearGridTablesByFileID(std::string db,std::string fileID_code)
{
  std::string fileID_type;

  if (db == "GrML")
    fileID_type="mss";
  else if (db == "WGrML")
    fileID_type="web";
  if (server._delete(db+".ds"+dsnum2+"_grids",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("clearGridTablesByFileID returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_grids","dcm",user,args.argsString);
  if (server._delete(db+".ds"+dsnum2+"_agrids",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("clearGridTablesByFileID returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_agrids","dcm",user,args.argsString);
  if (MySQL::table_exists(server,db+".ds"+dsnum2+"_grid_definitions")) {
    if (server._delete(db+".ds"+dsnum2+"_grid_definitions",fileID_type+"ID_code = "+fileID_code) < 0)
	metautils::logError("clearGridTablesByFileID returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_grid_definitions","dcm",user,args.argsString);
  }
  if (MySQL::table_exists(server,db+".ds"+dsnum2+"_processes")) {
    if (server._delete(db+".ds"+dsnum2+"_processes",fileID_type+"ID_code = "+fileID_code) < 0)
	metautils::logError("clearGridTablesByFileID returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_processes","dcm",user,args.argsString);
  }
  if (MySQL::table_exists(server,db+".ds"+dsnum2+"_ensembles")) {
    if (server._delete(db+".ds"+dsnum2+"_ensembles",fileID_type+"ID_code = "+fileID_code) < 0)
	metautils::logError("clearGridTablesByFileID returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_ensembles","dcm",user,args.argsString);
  }
}

void clearGridCache(std::string db)
{
  MySQL::LocalQuery query;
  MySQL::Row row;

  query.set("select parameter,levelType_codes,min(start_date),max(end_date) from "+db+".ds"+dsnum2+"_agrids group by parameter,levelType_codes");
  if (query.submit(server) < 0) {
    metautils::logError("clearGridCache returned error: "+query.error()+" while trying to rebuild grid cache","dcm",user,args.argsString);
  }
  std::string result;
  server.command("lock tables "+db+".ds"+dsnum2+"_agrids_cache write",result);
  if (server._delete(db+".ds"+dsnum2+"_agrids_cache") < 0) {
    metautils::logError("clearGridCache returned error: "+server.error()+" while clearing "+db+".ds"+dsnum2+"_agrids_cache","dcm",user,args.argsString);
  }
  while (query.fetch_row(row)) {
    if (server.insert(db+".ds"+dsnum2+"_agrids_cache","'"+row[0]+"','"+row[1]+"',"+row[2]+","+row[3]) < 0) {
	metautils::logError("clearGridCache returned error: "+server.error()+" while inserting into "+db+".ds"+dsnum2+"_agrids_cache","dcm",user,args.argsString);
    }
  }
  server.command("unlock tables",result);
}

void clearObsTablesByFileID(std::string db,std::string fileID_code)
{
  std::string fileID_type;

  if (db == "ObML") {
    fileID_type="mss";
  }
  else if (db == "WObML") {
    fileID_type="web";
  }
  if (server._delete(db+".ds"+dsnum2+"_IDList2",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_IDList2","dcm",user,args.argsString);
  }
  if (MySQL::table_exists(server,db+".ds"+dsnum2+"_dataTypes") && server._delete(db+".ds"+dsnum2+"_dataTypes",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_dataTypes","dcm",user,args.argsString);
  }
  if (MySQL::table_exists(server,db+".ds"+dsnum2+"_dataTypes2") && server._delete(db+".ds"+dsnum2+"_dataTypes2",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_dataTypes2","dcm",user,args.argsString);
  }
/*
  if (server._delete(db+".ds"+dsnum2+"_ID_dataTypes",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_ID_dataTypes","dcm",user,args.argsString);
  }
*/
  if (server._delete(db+".ds"+dsnum2+"_frequencies",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_frequencies","dcm",user,args.argsString);
  }
  if (server._delete(db+".ds"+dsnum2+"_locations",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_locations","dcm",user,args.argsString);
  }
  if (server._delete(db+".ds"+dsnum2+"_location_names",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table "+db+".ds"+dsnum2+"_location_names","dcm",user,args.argsString);
  }
  if (db == "ObML" && server._delete("ObML.ds"+dsnum2+"_geobounds",fileID_type+"ID_code = "+fileID_code) < 0) {
    metautils::logError("removed returned error: "+server.error()+" for table ObML.ds"+dsnum2+"_geobounds","dcm",user,args.argsString);
  }
}

void clearFixTablesByFileID(std::string db,std::string fileID_code)
{
  std::string fileID_type;

  if (db == "FixML")
    fileID_type="mss";
  else if (db == "WFixML")
    fileID_type="web";
  if (server._delete(db+".ds"+dsnum2+"_IDList",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("removed returned error: "+server.error(),"dcm",user,args.argsString);
  if (server._delete(db+".ds"+dsnum2+"_frequencies",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("removed returned error: "+server.error(),"dcm",user,args.argsString);
  if (server._delete(db+".ds"+dsnum2+"_locations",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("removed returned error: "+server.error(),"dcm",user,args.argsString);
  if (server._delete(db+".ds"+dsnum2+"_location_names",fileID_type+"ID_code = "+fileID_code) < 0)
    metautils::logError("removed returned error: "+server.error(),"dcm",user,args.argsString);
}

bool removeFrom(std::string database,std::string table_ext,std::string file_field_name,std::string md_directory,std::string& file,std::string file_ext,std::string& fileID_code,bool& file_removed)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string filebase,sdum,error;
  std::list<std::string> tables;
  std::deque<std::string> sp,sp2;
  Entry e;

  if (md_directory == "wfmd") {
    file=metautils::getRelativeWebFilename(file);
  }
  query.set("code",database+".ds"+dsnum2+table_ext,file_field_name+" = '"+file+"'");
  if (query.submit(server) == 0) {
    if (query.num_rows() == 1) {
	query.fetch_row(row);
	fileID_code=row[0];
	if (server._delete(database+".ds"+dsnum2+table_ext,"code = "+fileID_code) < 0) {
	  metautils::logError("removeFrom returned error: "+server.error(),"dcm",user,args.argsString);
	}
	if (database == "WGrML" || database == "WObML") {
	  tables=MySQL::table_names(server,strutils::substitute(database,"W","I"),"ds"+dsnum2+"_inventory_%",error);
	  for (const auto& table : tables) {
	    server._delete(strutils::substitute(database,"W","I")+".`"+table+"`","webID_code = "+fileID_code);
	  }
	  if (server._delete(strutils::substitute(database,"W","I")+".ds"+dsnum2+"_inventory_summary","webID_code = "+fileID_code) == 0) {
	    query.set("select tindex from dssdb.wfile where wfile = '"+file+"'");
	    if (query.submit(server) == 0 && query.num_rows() == 1) {
		query.fetch_row(row);
		if (row[0].length() > 0 && row[0] != "0" && !inv_tindex_table.found(row[0],e)) {
		  e.key=row[0];
		  inv_tindex_table.insert(e);
		}
	    }
	    create_inv_filelist_cache=true;
	  }
	}
	if (database == "WObML") {
	  server._delete("I"+database.substr(1)+".ds"+dsnum2+"_dataTypes","webID_code = "+fileID_code);
	  tables=MySQL::table_names(server,"I"+database.substr(1),"ds"+dsnum2+"_timeSeries_times_%",error);
	  for (const auto& table : tables) {
	    server._delete("I"+database.substr(1)+"."+table,"webID_code = "+fileID_code);
	  }
	}
	sdum=file;
	if (md_directory == "fmd") {
	  strutils::replace_all(sdum,"/FS/DSS/","");
	  strutils::replace_all(sdum,"/DSS/","");
	}
	strutils::replace_all(sdum,"/","%");
	if (!local_args.keep_xml) {
	  struct stat buf;
	  short flag=0;
	  if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext+".gz",buf)) {
	    flag=1;
	  }
	  else if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext,buf)) {
	    flag=2;
	  }
	  if (file_ext == ".ObML" && flag > 0) {
	    TempDir tdir;
	    if (!tdir.create("/glade/scratch/rdadata")) {
		metautils::logError("unable to create temporary directory in /glade/scratch/rdadata","dcm",user,args.argsString);
	    }
	    std::string xml_parent;
	    if (flag == 1) {
		xml_parent=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext+".gz",tdir.name());
		if (stat(xml_parent.c_str(),&buf) == 0) {
		  system(("gunzip "+xml_parent).c_str());
		  strutils::chop(xml_parent,3);
		}
	    }
	    else if (flag == 2) {
		xml_parent=getRemoteWebFile("https://rda.ucar.edu/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext,tdir.name());
	    }
	    XMLDocument xdoc;
	    if (xdoc.open(xml_parent)) {
		auto elist=xdoc.element_list("ObML/observationType/platform/IDs");
		for (const auto& e : elist) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),error) < 0) {
		    metautils::logWarning("unable to unsync "+e.attribute_value("ref"),"dcm",user,args.argsString);
		  }
		}
		elist=xdoc.element_list("ObML/observationType/platform/locations");
		for (const auto& e : elist) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+e.attribute_value("ref"),error) < 0) {
		    metautils::logWarning("unable to unsync "+e.attribute_value("ref"),"dcm",user,args.argsString);
		  }
		}
		xdoc.close();
	    }
	    if (md_directory == "wfmd") {
		if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/inv/"+sdum+file_ext+"_inv.gz",buf)) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/inv/"+sdum+file_ext+"_inv.gz",error) < 0) {
		    metautils::logWarning("unable to unsync "+sdum+file_ext+"_inv","dcm",user,args.argsString);
		  }
		}
		else if (existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/inv/"+sdum+file_ext+"_inv",buf)) {
		  if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/inv/"+sdum+file_ext+"_inv",error) < 0) {
		    metautils::logWarning("unable to unsync "+sdum+file_ext+"_inv","dcm",user,args.argsString);
		  }
		}
	    }
	  }
	  if (flag == 1) {
	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext+".gz",error) < 0) {
		metautils::logWarning("unable to unsync "+sdum+file_ext,"dcm",user,args.argsString);
	    }
	  }
	  else if (flag == 2) {
	    if (hostUnsync("/__HOST__/web/datasets/ds"+args.dsnum+"/metadata/"+md_directory+"/"+sdum+file_ext,error) < 0) {
		metautils::logWarning("unable to unsync "+sdum+file_ext,"dcm",user,args.argsString);
	    }
	  }
	}
	file_removed=true;
	return true;
    }
  }
  return false;
}

bool removed(std::string file)
{
  std::string fileID_code;
  bool file_removed=false;
  MySQL::LocalQuery query;
  MySQL::Row row;
  Entry e;
  MySQL::Server server_d;

  if (strutils::has_beginning(file,"/FS/DSS/") || strutils::has_beginning(file,"/DSS/")) {
    removedFromGrML=removeFrom("GrML","_primaries","mssID","fmd",file,".GrML",fileID_code,file_removed);
    if (removedFromGrML) {
	clearGridTablesByFileID("GrML",fileID_code);
    }
    removedFromObML=removeFrom("ObML","_primaries","mssID","fmd",file,".ObML",fileID_code,file_removed);
    if (removedFromObML) {
	clearObsTablesByFileID("ObML",fileID_code);
    }
    removedFromSatML=removeFrom("SatML","_primaries","mssID","fmd",file,".SatML",fileID_code,file_removed);
    removedFromFixML=removeFrom("FixML","_primaries","mssID","fmd",file,".FixML",fileID_code,file_removed);
    if (removedFromFixML) {
	clearFixTablesByFileID("FixML",fileID_code);
    }
    query.set("gindex","mssfile","mssfile = '"+file+"'");
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	if (!mss_gindexTable.found(row[0],e)) {
	  e.key=row[0];
	  mss_gindexTable.insert(e);
	}
    }
    metautils::connectToRDAServer(server_d);
    if (file_removed) {
	query.set("tindex","mssfile","mssfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (row[0].length() > 0 && row[0] != "0" && !MSS_tindex_table.found(row[0],e)) {
	    e.key=row[0];
	    MSS_tindex_table.insert(e);
	  }
	}
	create_MSS_filelist_cache=true;
    }
    server_d.update("mssfile","meta_link = NULL","dsid = 'ds"+args.dsnum+"' and mssfile = '"+file+"'");
    server_d.disconnect();
  }
  else {
    removedFromWGrML=removeFrom("WGrML","_webfiles","webID","wfmd",file,".GrML",fileID_code,file_removed);
    if (removedFromWGrML) {
	clearGridTablesByFileID("WGrML",fileID_code);
    }
    removedFromWObML=removeFrom("WObML","_webfiles","webID","wfmd",file,".ObML",fileID_code,file_removed);
    if (removedFromWObML) {
	clearObsTablesByFileID("WObML",fileID_code);
    }
    removedFromWFixML=removeFrom("WFixML","_webfiles","webID","wfmd",file,".FixML",fileID_code,file_removed);
    if (removedFromWFixML) {
	clearFixTablesByFileID("WFixML",fileID_code);
    }
    query.set("gindex","wfile","wfile = '"+file+"'");
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	if (!web_gindexTable.found(row[0],e)) {
	  e.key=row[0];
	  web_gindexTable.insert(e);
	}
    }
    metautils::connectToRDAServer(server_d);
    if (file_removed) {
	query.set("tindex","wfile","wfile = '"+file+"'");
	if (query.submit(server_d) == 0 && query.num_rows() == 1) {
	  query.fetch_row(row);
	  if (row[0].length() > 0 && row[0] != "0" && !Web_tindex_table.found(row[0],e)) {
	    e.key=row[0];
	    Web_tindex_table.insert(e);
	  }
	}
	create_Web_filelist_cache=true;
    }
    server_d.update("dssdb.wfile","meta_link = NULL","dsid = 'ds"+args.dsnum+"' and wfile = '"+file+"'");
    server_d.disconnect();
  }
  return file_removed;
}

void generateGraphics(std::string type)
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

void generateDatasetHomePage()
{
  std::stringstream oss,ess;
  mysystem2(directives.localRoot+"/bin/dsgen "+args.dsnum,oss,ess);
}

extern "C" void *thread_indexVariables(void *)
{
  indexVariables(args.dsnum);
  return NULL;
}

extern "C" void *thread_summarizeFrequencies(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::summarizeFrequencies(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *thread_summarizeGrids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::grids::summarizeGrids(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *thread_aggregateGrids(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::grids::aggregateGrids(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *thread_summarizeGridLevels(void *ts)
{
  ThreadStruct *t=(ThreadStruct *)ts;

  summarizeMetadata::gridLevels::summarizeGridLevels(args.dsnum,t->strings[0],"dcm",user,args.argsString);
  return NULL;
}

extern "C" void *thread_summarizeGridResolutions(void *)
{
  summarizeMetadata::grids::summarizeGridResolutions(args.dsnum,"dcm",user,args.argsString,"");
  return NULL;
}

extern "C" void *thread_createFileListCache(void *)
{
  summarizeMetadata::createFileListCache("MSS","dcm",user,args.argsString);
  return NULL;
}

extern "C" void *thread_generateDetailedMetadataView(void *)
{
  summarizeMetadata::detailedMetadata::generateDetailedMetadataView(args.dsnum,"dcm",user,args.argsString);
  return nullptr;
}

int main(int argc,char **argv)
{
  struct stat buf;
  std::stringstream oss,ess;
  std::list<std::string>::iterator it,end;

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
    std::cerr << "       \"http://rda.ucar.edu\"" << std::endl;
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
  parseArgs();
  if (args.dsnum == "999.9") {
    exit(0);
  }
  for (it=cmd_list.begin(),end=cmd_list.end(); it != end; ++it) {
    if (!existsOnRDAWebServer("rda.ucar.edu","/SERVER_ROOT/web/datasets/ds"+args.dsnum+"/metadata/"+*it,buf)) {
	cmd_list.erase(it--);
    }
  }
  if (cmd_list.size() == 0) {
    exit(0);
  }
  removedFromGrML=false;
  removedFromWGrML=false;
  removedFromObML=false;
  removedFromWObML=false;
  removedFromSatML=false;
  removedFromFixML=false;
  removedFromWFixML=false;
  for (it=files.begin(),end=files.end(); it != end; ++it) {
    if (removed(*it)) {
	files.erase(it--);
    }
  }
  for (const auto& key : mss_gindexTable.keys()) {
    summarizeMetadata::detailedMetadata::generateGroupDetailedMetadataView(args.dsnum,key,"MSS","dcm",user,args.argsString);
  }
  for (const auto& key : web_gindexTable.keys()) {
    summarizeMetadata::detailedMetadata::generateGroupDetailedMetadataView(args.dsnum,key,"Web","dcm",user,args.argsString);
  }
  if (removedFromGrML) {
    clearGridCache("GrML");
    std::vector<ThreadStruct> threads(8);
    pthread_create(&threads[0].tid,NULL,thread_indexVariables,NULL);
    threads[1].strings.emplace_back("GrML");
    pthread_create(&threads[1].tid,NULL,thread_summarizeFrequencies,&threads[1]);
    threads[2].strings.emplace_back("GrML");
    pthread_create(&threads[2].tid,NULL,thread_summarizeGrids,&threads[2]);
    threads[3].strings.emplace_back("GrML");
    pthread_create(&threads[3].tid,NULL,thread_aggregateGrids,&threads[3]);
    threads[4].strings.emplace_back("GrML");
    pthread_create(&threads[4].tid,NULL,thread_summarizeGridLevels,&threads[4]);
    pthread_create(&threads[5].tid,NULL,thread_summarizeGridResolutions,NULL);
    pthread_create(&threads[6].tid,NULL,thread_generateDetailedMetadataView,NULL);
    if (create_MSS_filelist_cache) {
	pthread_create(&threads[7].tid,NULL,thread_createFileListCache,NULL);
    }
    for (const auto& t : threads) {
	pthread_join(t.tid,NULL);
    }
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    generateDatasetHomePage();
  }
  if (removedFromWGrML) {
    clearGridCache("WGrML");
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
  if (removedFromObML) {
    summarizeMetadata::summarizeFrequencies(args.dsnum,"dcm",user,args.argsString);
    summarizeMetadata::obsData::summarizeObsData(args.dsnum,"dcm",user,args.argsString);
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
    if (args.updateGraphics) {
	generateGraphics("mss");
    }
    generateDatasetHomePage();
  }
  if (removedFromWObML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generateGraphics("web");
    }
  }
  if (removedFromSatML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0)
	std::cerr << ess.str() << std::endl;
  }
  if (removedFromFixML) {
    summarizeMetadata::summarizeFrequencies(args.dsnum,"dcm",user,args.argsString);
    summarizeMetadata::fixData::summarizeFixData(args.dsnum,"dcm",user,args.argsString);
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rm",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generateGraphics("mss");
    }
    generateDatasetHomePage();
  }
  if (removedFromWFixML) {
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" -rw",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
    if (args.updateGraphics) {
	generateGraphics("web");
    }
  }
  for (const auto& key : MSS_tindex_table.keys()) {
    summarizeMetadata::createFileListCache("MSS","dcm",user,args.argsString,key);
  }
  if (create_Web_filelist_cache) {
    summarizeMetadata::createFileListCache("Web","dcm",user,args.argsString);
  }
  for (const auto& key : Web_tindex_table.keys()) {
    summarizeMetadata::createFileListCache("Web","dcm",user,args.argsString,key);
  }
  if (create_inv_filelist_cache) {
    summarizeMetadata::createFileListCache("inv","dcm",user,args.argsString);
  }
  for (const auto& key : inv_tindex_table.keys()) {
    summarizeMetadata::createFileListCache("inv","dcm",user,args.argsString,key);
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
