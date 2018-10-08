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
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Query query("code,format_code","FixML.ds"+dsnum2+"_primaries");
  if (query.submit(server) < 0) {
    metautils::log_error("summarize_fix_data(): "+query.error(),caller,user);
  }
  my::map<CodeEntry> mss_table;
  for (const auto& res : query) {
    CodeEntry ce;
    ce.key=res[0];
    ce.code=res[1];
    mss_table.insert(ce);
  }
  std::string error;
  if (server.command("lock tables FixML.ds"+dsnum2+"_locations write",error) < 0)
    metautils::log_error("summarize_fix_data(): "+server.error(),caller,user);
  query.set("mssID_code,classification_code,start_date,end_date,box1d_row,box1d_bitmap","FixML.ds"+dsnum2+"_locations");
  if (query.submit(server) < 0) {
    metautils::log_error("summarize_fix_data(): "+query.error(),caller,user);
  }
  my::map<SummaryEntry> summary_table;
  for (const auto& res : query) {
    CodeEntry ce;
    mss_table.found(res[0],ce);
    SummaryEntry se;
    se.key=ce.code+"<!>"+res[1]+"<!>"+res[4];
    if (!summary_table.found(se.key,se)) {
	se.start_date=res[2];
	se.end_date=res[3];
	se.box1d_bitmap=res[5];
	summary_table.insert(se);
    }
    else {
	if (res[2] < se.start_date) {
	  se.start_date=res[2];
	}
	if (res[3] > se.end_date) {
	  se.end_date=res[3];
	}
	if (res[5] != se.box1d_bitmap) {
	  se.box1d_bitmap=bitmap::add_box1d_bitmaps(se.box1d_bitmap,res[5]);
	}
	summary_table.replace(se);
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error("summarize_fix_data(): "+server.error(),caller,user);
  }
  if (server.command("lock tables search.fix_data write",error) < 0) {
    metautils::log_error("summarize_fix_data(): "+server.error(),caller,user);
  }
  server._delete("search.fix_data","dsid = '"+metautils::args.dsnum+"'");
  for (const auto& key : summary_table.keys()) {
    auto parts=strutils::split(key,"<!>");
    SummaryEntry se;
    summary_table.found(key,se);
    if (server.insert("search.fix_data","'"+metautils::args.dsnum+"',"+parts[0]+","+parts[1]+",'"+se.start_date+"','"+se.end_date+"',"+parts[2]+",'"+se.box1d_bitmap+"'") < 0) {
	error=server.error();
	if (!strutils::has_beginning(error,"Duplicate entry")) {
	  metautils::log_error("summarize_fix_data(): "+error,caller,user);
	}
    }
  }
  if (server.command("unlock tables",error) < 0) {
    metautils::log_error("summarize_fix_data(): "+server.error(),caller,user);
  }
  error=summarize_locations("FixML");
  if (!error.empty()) {
    metautils::log_error("summarize_locations() returned '"+error+"'",caller,user);
  }
  server.disconnect();
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
