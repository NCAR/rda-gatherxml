#include <string>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <bitmap.hpp>
#include <strutils.hpp>
#include <search.hpp>

namespace gatherxml {

namespace summarizeMetadata {

void summarize_fix_data(std::string caller,std::string user)
{
  const std::string THIS_FUNC=__func__;
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Query formats_query("code,format_code","WFixML.ds"+dsnum2+"_webfiles2");
  if (formats_query.submit(server) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+formats_query.error(),caller,user);
  }
  std::unordered_map<std::string,std::string> data_file_formats_map;
  for (const auto& format_row : formats_query) {
    data_file_formats_map.emplace(format_row[0],format_row[1]);
  }
  std::string error;
  if (server.command("lock tables WFixML.ds"+dsnum2+"_locations write",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  MySQL::LocalQuery locations_query("webID_code,classification_code,start_date,end_date,box1d_row,box1d_bitmap","WFixML.ds"+dsnum2+"_locations");
  if (locations_query.submit(server) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+locations_query.error(),caller,user);
  }
  my::map<SummaryEntry> summary_table;
  for (const auto& location_row : locations_query) {
    if (data_file_formats_map.find(location_row[0]) == data_file_formats_map.end()) {
        metautils::log_error(THIS_FUNC+"() found a webID ("+location_row[0]+") in WFixML.ds"+dsnum2+"_locations that doesn't exist in WFixML.ds"+dsnum2+"_webfiles2",caller,user);
    }
    SummaryEntry se;
    se.key=data_file_formats_map[location_row[0]]+"<!>"+location_row[1]+"<!>"+location_row[4];
    if (!summary_table.found(se.key,se)) {
	se.start_date=location_row[2];
	se.end_date=location_row[3];
	se.box1d_bitmap=location_row[5];
	summary_table.insert(se);
    }
    else {
	if (location_row[2] < se.start_date) {
	  se.start_date=location_row[2];
	}
	if (location_row[3] > se.end_date) {
	  se.end_date=location_row[3];
	}
	if (location_row[5] != se.box1d_bitmap) {
	  se.box1d_bitmap=bitmap::add_box1d_bitmaps(se.box1d_bitmap,location_row[5]);
	}
	summary_table.replace(se);
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  if (server.command("lock tables search.fix_data write",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  server._delete("search.fix_data","dsid = '"+metautils::args.dsnum+"'");
  for (const auto& key : summary_table.keys()) {
    auto parts=strutils::split(key,"<!>");
    SummaryEntry se;
    summary_table.found(key,se);
    if (server.insert("search.fix_data","'"+metautils::args.dsnum+"',"+parts[0]+","+parts[1]+",'"+se.start_date+"','"+se.end_date+"',"+parts[2]+",'"+se.box1d_bitmap+"'") < 0) {
	error=server.error();
	if (!strutils::has_beginning(error,"Duplicate entry")) {
	  metautils::log_error(THIS_FUNC+"(): "+error,caller,user);
	}
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error(THIS_FUNC+"(): "+server.error(),caller,user);
  }
  error=summarize_locations("WFixML");
  if (!error.empty()) {
    metautils::log_error(THIS_FUNC+"(): summarize_locations() returned '"+error+"'",caller,user);
  }
  server.disconnect();
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
