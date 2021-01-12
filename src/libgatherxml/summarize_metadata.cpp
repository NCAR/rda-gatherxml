#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <regex>
#include <unordered_set>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <bsort.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>
#include <xml.hpp>

namespace gatherxml {

namespace summarizeMetadata {

struct CMDDateRange {
  CMDDateRange() : start(),end(),gindex() {}

  std::string start,end;
  std::string gindex;
};
CMDDateRange cmd_date_range(std::string start,std::string end,std::string gindex,size_t& precision)
{
  CMDDateRange d;
  d.start=start;
  if (d.start.length() > precision) {
    precision=d.start.length();
  }
  while (d.start.length() < 14) {
    d.start+="00";
  }
  d.end=end;
  if (d.end.length() > precision) {
    precision=d.end.length();
  }
  while (d.end.length() < 14) {
    if (d.end.length() < 10) {
	d.end+="23";
    }
    else {
	d.end+="59";
    }
  }
  d.gindex=gindex;
  return d;
}

void cmd_dates(std::string database,size_t date_left_padding,std::list<CMDDateRange>& range_list,size_t& precision)
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    std::cerr << "Error: unable to connect to metadata server" << std::endl;
    exit(1);
  }
  std::string table=database+".ds"+strutils::substitute(metautils::args.dsnum,".","")+"_primaries2";
  if (table_exists(server,table)) {
    MySQL::LocalQuery query("select min(p.start_date) as md,max(p.end_date),m.gindex from "+table+" as p left join dssdb.mssfile as m on m.mssfile = p.mssID where m.dsid = 'ds"+metautils::args.dsnum+"' and m.type = 'P' and m.status = 'P' and p.start_date > 0 group by m.gindex union select min(p.start_date) as md,max(p.end_date),h.gindex from "+table+" as p left join (select concat(m.mssfile,\"..m..\",h.hfile) as member,m.gindex as gindex from dssdb.htarfile as h left join dssdb.mssfile as m on m.mssid = h.mssid where h.dsid = 'ds"+metautils::args.dsnum+"' and m.type = 'P' and m.status = 'P') as h on h.member = p.mssID where !isnull(h.member) and p.start_date > 0 group by h.gindex");
    if (query.submit(server) < 0) {
	std::cerr << "Error (A): " << query.error() << std::endl;
	exit(1);
    }
    MySQL::Row row;
    auto fetched=query.fetch_row(row);
    if (query.num_rows() < 2 && (!fetched || row[2] == "0")) {
	auto lpad=strutils::itos(date_left_padding);
	query.set("select lpad(start_date,"+lpad+",'0'),lpad(end_date,"+lpad+",'0'),0 from "+table+" where start_date != 0 order by start_date,end_date");
	if (query.submit(server) < 0) {
	  std::cerr << "Error (B): " << query.error() << std::endl;
	  exit(1);
	}
	if (query.num_rows() > 0) {
	  std::vector<CMDDateRange> darray;
	  darray.reserve(query.num_rows());
	  while (query.fetch_row(row)) {
	    darray.emplace_back(cmd_date_range(row[0],row[1],row[2],precision));
	  }
	  binary_sort(darray,
	  [](CMDDateRange& left,CMDDateRange& right) -> bool
	  {
	    if (left.start <= right.start) {
		return true;
	    }
	    else {
		return false;
	    }
	  });
	  CMDDateRange d;
	  d.gindex="0";
	  DateTime sdt(std::stoll(darray[0].start));
	  DateTime edt(std::stoll(darray[0].end));
	  auto max_edt=edt;
	  auto last_edt=darray[0].end;
	  d.start=darray[0].start;
	  for (size_t n=1; n < query.num_rows(); n++) {
	    sdt.set(std::stoll(darray[n].start));
	    if (sdt > max_edt && sdt.days_since(max_edt) > 180) {
		d.end=last_edt;
		range_list.emplace_back(d);
		d.start=darray[n].start;
	    }
	    edt.set(std::stoll(darray[n].end));
	    if (edt > max_edt) {
		max_edt=edt;
	    }
	    if (darray[n].end > last_edt) {
		last_edt=darray[n].end;
	    }
	  }
	  d.end=last_edt;
	  range_list.emplace_back(d);
	}
    }
    else {
	query.rewind();
	while (query.fetch_row(row)) {
	  range_list.emplace_back(cmd_date_range(row[0],row[1],row[2],precision));
	}
    }
  }
  server.disconnect();
}

void summarize_dates(std::string caller,std::string user)
{
  std::string error;
  bool foundGroups=false;

  std::list<CMDDateRange> range_list;
  size_t precision=0;
  cmd_dates("GrML",12,range_list,precision);
  cmd_dates("ObML",8,range_list,precision);
  cmd_dates("FixML",12,range_list,precision);
  cmd_dates("SatML",14,range_list,precision);
  MySQL::Server dssdb_server(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");;
  if (range_list.size() > 0) {
    precision=(precision-2)/2;
    std::vector<CMDDateRange> darray;
    darray.reserve(range_list.size());
    auto n=0;
    for (const auto& item : range_list) {
	darray.emplace_back(item);
	if (darray[n].gindex != "0") {
	  foundGroups=true;
	}
	++n;
    }
    if (!foundGroups) {
	auto m=1;
	dssdb_server._delete("dsperiod","dsid = 'ds"+metautils::args.dsnum+"'");
	for (n=range_list.size()-1; n >= 0; n--,m++) {
	  DateTime sdt(std::stoll(darray[n].start));
	  DateTime edt(std::stoll(darray[n].end));
	  std::string insert_string="'ds"+metautils::args.dsnum+"',0,"+strutils::itos(m)+",'"+sdt.to_string("%Y-%m-%d")+"','"+sdt.to_string("%T")+"',"+strutils::itos(precision)+",'"+edt.to_string("%Y-%m-%d")+"','"+edt.to_string("%T")+"',"+strutils::itos(precision)+",'"+sdt.to_string("%Z")+"'";
	  if (dssdb_server.insert("dsperiod",insert_string) < 0) {
	    error=dssdb_server.error();
	    if (!std::regex_search(error,std::regex("^Duplicate entry"))) {
		metautils::log_error("summarize_dates(): "+error+" when trying to insert into dsperiod(1) ("+insert_string+")",caller,user);
	    }
	  }
	}
    }
    else {
	binary_sort(darray,
	[](CMDDateRange& left,CMDDateRange& right) -> bool
	{
	  if (left.end > right.end) {
	    return true;
	  }
	  else if (left.end < right.end) {
	    return false;
	  }
	  else {
	    if (left.gindex <= right.gindex) {
	        return true;
	    }
	    else {
	        return false;
	    }
	  }
	});
	dssdb_server._delete("dsperiod","dsid = 'ds"+metautils::args.dsnum+"'");
	for (size_t n=0; n < range_list.size(); ++n) {
	  DateTime sdt(std::stoll(darray[n].start));
	  DateTime edt(std::stoll(darray[n].end));
	  std::string insert_string="'ds"+metautils::args.dsnum+"',"+darray[n].gindex+","+strutils::itos(n)+",'"+sdt.to_string("%Y-%m-%d")+"','"+sdt.to_string("%T")+"',"+strutils::itos(precision)+",'"+edt.to_string("%Y-%m-%d")+"','"+edt.to_string("%T")+"',"+strutils::itos(precision)+",'"+sdt.to_string("%Z")+"'";
	  if (dssdb_server.insert("dsperiod",insert_string) < 0) {
	    error=dssdb_server.error();
	    auto num_retries=0;
	    while (num_retries < 3 && strutils::has_beginning(error,"Deadlock")) {
		error="";
		sleep(30);
		if (dssdb_server.insert("dsperiod",insert_string) < 0) {
		  error=dssdb_server.error();
		}
		++num_retries;
	    }
	    if (!error.empty() && !strutils::has_beginning(error,"Duplicate entry")) {
		metautils::log_error("summarize_dates(): "+error+" when trying to insert into dsperiod(2) ("+insert_string+")",caller,user);
	    }
	  }
	}
    }
  }
  dssdb_server.disconnect();
}

bool inserted_time_resolution_keyword(MySQL::Server& server,std::string database,std::string keyword,std::string& error)
{
  if (server.insert("search.time_resolutions","'"+keyword+"','GCMD','"+metautils::args.dsnum+"','"+database+"'") < 0) {
    error=server.error();
    if (!strutils::contains(error,"Duplicate entry")) {
	error+="\ntried to insert into search.time_resolutions ('"+keyword+"','GCMD','"+metautils::args.dsnum+"','"+database+"')";
	return false;
    }
  }
  return true;
}

void grids_per(size_t nsteps,DateTime start,DateTime end,double& gridsper,std::string& unit)
{
  size_t num_days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
  double nhrs,test;
  const float TOLERANCE=0.15;

  nhrs=end.hours_since(start);
  if (floatutils::myequalf(nhrs,0.) || nsteps <= 1) {
    unit="singletimestep";
    return;
  }
  if (dateutils::is_leap_year(end.year())) {
    num_days[2]=29;
  }
  gridsper=(nsteps-1)/nhrs;
  test=gridsper;
  if ((test+TOLERANCE) > 1.) {
    gridsper=test;
    test/=60.;
    if ((test+TOLERANCE) < 1.) {
	unit="hour";
    }
    else {
	gridsper=test;
	test/=60.;
	if ((test+TOLERANCE) < 1.) {
	  unit="minute";
	}
	else {
	  gridsper=test;
	  unit="second";
	}
    }
  }
  else {
    gridsper*=24.;
    if ((gridsper+TOLERANCE) > 1.) {
	unit="day";
    }
    else {
	gridsper*=7.;
	if ((gridsper+TOLERANCE) > 1.) {
	  unit="week";
	}
	else {
	  gridsper*=(num_days[end.month()]/7.);
	  if ((gridsper+TOLERANCE) > 1.) {
	    unit="month";
	  }
	  else {
	    gridsper*=12;
	    if ((gridsper+TOLERANCE) > 1.) {
		unit="year";
	    }
	    else {
		unit="";
		gridsper=0.;
	    }
	  }
	}
    }
  }
}

void summarize_frequencies(std::string caller,std::string user,std::string mssID_code)
{
  MySQL::LocalQuery query;
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Row row;
  std::string keyword,timeRange,sdum;
  std::string start,end,unit,error;
  double gridsper;
  std::string min_start="30001231235959",max_end="10000101000000";
  size_t nsteps,single_nsteps=0;
  int num;
  DateTime d1,d2;
  my::map<CodeEntry> tr_table,sn_table(999999);
  std::unordered_set<std::string> tr_keyword_set,frequencies_set;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
// summarize from GrML
  if (!mssID_code.empty()) {
    query.set("code,timeRange","GrML.timeRanges");
    if (query.submit(server) < 0) {
	metautils::log_error("summarize_frequencies(): "+query.error()+" while querying GrML.timeRanges",caller,user);
    }
    while (query.fetch_row(row)) {
	CodeEntry e;
	e.key=row[0];
	e.code=row[1];
	tr_table.insert(e);
    }
    query.set("select timeRange_code,min(start_date),max(end_date),sum(min_nsteps),sum(max_nsteps) from GrML.ds"+dsnum2+"_grids where mssID_code = "+mssID_code+" group by timeRange_code,parameter");
    if (query.submit(server) == 0) {
	while (query.fetch_row(row)) {
	  if (row[3] != "1" || row[4] != "1") {
	    CodeEntry e;
	    tr_table.found(row[0],e);
	    timeRange=e.code;
	    keyword="";
	    if (strutils::has_beginning(timeRange,"Pentad")) {
		keyword=searchutils::time_resolution_keyword("regular",5,"day","");
		e.key="regular<!>5<!>day";
	    }
	    else if (strutils::has_beginning(timeRange,"Weekly")) {
		keyword=searchutils::time_resolution_keyword("regular",1,"week","");
		e.key="regular<!>1<!>week";
	    }
	    else if (strutils::has_beginning(timeRange,"Monthly")) {
		keyword=searchutils::time_resolution_keyword("regular",1,"month","");
		e.key="regular<!>1<!>month";
	    }
	    else if (strutils::has_beginning(timeRange,"30-year Climatology")) {
		keyword=searchutils::time_resolution_keyword("climatology",1,"30-year","");
		e.key="climatology<!>1<!>30-year";
	    }
	    else if (strutils::contains(timeRange,"-year Climatology")) {
		num=std::stoi(timeRange.substr(0,timeRange.find("-")));
		sdum=strutils::itos(num);
		if (strutils::contains(timeRange,"of Monthly")) {
		  keyword=searchutils::time_resolution_keyword("climatology",1,"month","");
		  e.key="climatology<!>1<!>"+sdum+"-year";
		}
		else {
		  e.key="<!>1<!>"+sdum+"-year";
		}
	    }
	    if (!keyword.empty()) {
		if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
		  tr_keyword_set.emplace(keyword);
		}
		if (frequencies_set.find(e.key) == frequencies_set.end()) {
		  frequencies_set.emplace(e.key);
		}
	    }
	    else {
		start=row[1];
		while (start.length() < 14) {
		  start+="00";
		}
		end=row[2];
		while (end.length() < 14) {
		  end+="00";
		}
		d1.set(std::stoll(start));
		d2.set(std::stoll(end));
		if (strutils::has_ending(timeRange,"Average") || strutils::has_ending(timeRange,"Product")) {
		  unit=timeRange;
		  if (strutils::contains(unit,"of")) {
		    unit=unit.substr(unit.rfind("of")+3);
		  }
		  unit=unit.substr(0,unit.find(" "));
		  num=std::stoi(unit.substr(0,unit.find("-")));
		  unit=unit.substr(unit.find("-"));
		  if (unit == "hour") {
		    d2.subtract_hours(num);
		  }
		  else if (unit == "day") {
		    d2.subtract_days(num);
		  }
		}
		else if (std::regex_search(timeRange,std::regex(" (Accumulation|Average|Product) \\(initial\\+"))) {
		  unit=timeRange.substr(0,timeRange.find(" "));
		  auto idx=unit.find("-");
		  num=std::stoi(unit.substr(0,idx));
		  unit=unit.substr(idx+1);
		  if (unit == "hour") {
		    d2.subtract_hours(num);
		  }
		}
		if (row[3] != row[4]) {
		  nsteps=std::stoi(row[3]);
		  grids_per(nsteps,d1,d2,gridsper,unit);
		  if (gridsper > 0.) {
		    if (unit != "singletimestep") {
			auto freq_s="irregular<!>"+strutils::itos(lround(gridsper))+"<!>"+unit;
			if (frequencies_set.find(freq_s) == frequencies_set.end()) {
			  frequencies_set.emplace(freq_s);
			}
			keyword=searchutils::time_resolution_keyword("irregular",lround(gridsper),unit,"");
			if (!keyword.empty()) {
			  if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
			    tr_keyword_set.emplace(keyword);
			  }
			}
		    }
		  }
		}
		nsteps=std::stoi(row[4]);
		grids_per(nsteps,d1,d2,gridsper,unit);
		if (gridsper > 0.) {
		  if (unit != "singletimestep") {
		    auto freq_s="irregular<!>"+strutils::itos(lround(gridsper))+"<!>"+unit;
		    if (frequencies_set.find(freq_s) == frequencies_set.end()) {
			frequencies_set.emplace(freq_s);
		    }
		    keyword=searchutils::time_resolution_keyword("irregular",lround(gridsper),unit,"");
		    if (!keyword.empty()) {
			if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
			  tr_keyword_set.emplace(keyword);
			}
		    }
		  }
		  else if (nsteps > 0) {
		    e.key=start;
		    if (!sn_table.found(e.key,e)) {
			if (start < min_start) {
			  min_start=start;
			}
			if (start > max_end) {
			  max_end=start;
			}
			single_nsteps+=nsteps;
			sn_table.insert(e);
		    }
		  }
		}
	    }
	  }
	  else {
	    CodeEntry e;
	    e.key=row[1];
	    if (!sn_table.found(e.key,e)) {
		sdum=row[1];
		while (sdum.length() < 14) {
		  sdum+="00";
		}
		if (sdum < min_start) {
		  min_start=sdum;
		}
		if (sdum > max_end) {
		  max_end=sdum;
		}
		single_nsteps++;
		sn_table.insert(e);
	    }
	  }
	}
	if (single_nsteps > 0) {
	  grids_per(single_nsteps,DateTime(std::stoll(min_start)),DateTime(std::stoll(max_end)),gridsper,unit);
	  if (gridsper > 0.) {
	    if (!unit.empty() && unit != "singletimestep") {
		auto freq_s="irregular<!>"+strutils::itos(lround(gridsper))+"<!>"+unit;
		if (frequencies_set.find(freq_s) == frequencies_set.end()) {
		  frequencies_set.emplace(freq_s);
		}
		keyword=searchutils::time_resolution_keyword("irregular",lround(gridsper),unit,"");
		if (!keyword.empty()) {
		  if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
		    tr_keyword_set.emplace(keyword);
		  }
		}
	    }
	  }
	}
	if (!table_exists(server,"GrML.ds"+dsnum2+"_frequencies")) {
	  std::string error;
	  if (server.command("create table GrML.ds"+dsnum2+"_frequencies like GrML.template_frequencies",error) < 0) {
	    metautils::log_error("summarize_frequencies(): "+server.error()+" while creating table GrML.ds"+dsnum2+"_frequencies",caller,user);
	  }
	}
	else {
	  server._delete("GrML.ds"+dsnum2+"_frequencies","mssID_code = "+mssID_code);
	}
	for (const auto& freq_s : frequencies_set) {
	  auto sp=strutils::split(freq_s,"<!>");
	  if (server.insert("GrML.frequencies","'"+metautils::args.dsnum+"',"+sp[1]+",'"+sp[2]+"'") < 0) {
	    error=server.error();
	    if (!strutils::contains(error,"Duplicate entry")) {
		metautils::log_error("summarize_frequencies(): "+error+" while trying to insert '"+metautils::args.dsnum+"',"+sp[1]+",'"+sp[2]+"'",caller,user);
	    }
	  }
	  if (server.insert("GrML.ds"+dsnum2+"_frequencies",mssID_code+",'"+sp[0]+"',"+sp[1]+",'"+sp[2]+"'") < 0) {
	    metautils::log_error("summarize_frequencies(): "+server.error()+" while trying to insert '"+mssID_code+",'"+sp[0]+"',"+sp[1]+",'"+sp[2]+"''",caller,user);
	  }
	}
    }
  }
  else {
    query.set("select distinct frequency_type,nsteps_per,unit from GrML.ds"+dsnum2+"_frequencies");
    if (query.submit(server) == 0) {
	server._delete("GrML.frequencies","dsid =  '"+metautils::args.dsnum+"'");
	while (query.fetch_row(row)) {
	  if (server.insert("GrML.frequencies","'"+metautils::args.dsnum+"',"+row[1]+",'"+row[2]+"'") < 0) {
	    error=server.error();
	    if (!strutils::contains(error,"Duplicate entry")) {
		metautils::log_error("summarize_frequencies(): "+error+" while trying to insert '"+metautils::args.dsnum+"',"+row[1]+",'"+row[2]+"'",caller,user);
	    }
	  }
	  keyword=searchutils::time_resolution_keyword(row[0],std::stoi(row[1]),row[2],"");
	  if (!keyword.empty()) {
	    if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
		tr_keyword_set.emplace(keyword);
	    }
	  }
	}
    }
    server._delete("search.time_resolutions","dsid = '"+metautils::args.dsnum+"' and origin = 'GrML'");
  }
  for (const auto& keyword : tr_keyword_set) {
    std::string error;
    if (!inserted_time_resolution_keyword(server,"GrML",keyword,error)) {
	metautils::log_error("summarize_frequencies(): "+error,caller,user);
    }
  }
// summarize from ObML
  tr_keyword_set.clear();
  query.set("select min(min_obs_per),max(max_obs_per),unit from ObML.ds"+dsnum2+"_frequencies group by unit");
  if (query.submit(server) == 0) {
    if (mssID_code.empty())
	server._delete("search.time_resolutions","dsid = '"+metautils::args.dsnum+"' and origin = 'ObML'");
    while (query.fetch_row(row)) {
	num=std::stoi(row[0]);
	unit=row[2];
	if (num < 0) {
	  timeRange="climatology";
	  if (num == -30) {
	    unit="30-year";
	  }
	}
	else {
	  timeRange="irregular";
	}
	auto keyword=searchutils::time_resolution_keyword(timeRange,num,unit,"");
	if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
	  tr_keyword_set.emplace(keyword);
	}
	num=std::stoi(row[1]);
	unit=row[2];
	if (num < 0) {
	  timeRange="climatology";
	  if (num == -30) {
	    unit="30-year";
	  }
	}
	else {
	  timeRange="irregular";
	}
	keyword=searchutils::time_resolution_keyword(timeRange,num,unit,"");
	if (tr_keyword_set.find(keyword) == tr_keyword_set.end()) {
	  tr_keyword_set.emplace(keyword);
	}
    }
    for (const auto& keyword : tr_keyword_set) {
	std::string error;
	if (!inserted_time_resolution_keyword(server,"GrML",keyword,error)) {
	  metautils::log_error("summarize_frequencies(): "+error,caller,user);
      }
    }
  }
// summarize from SatML

// summarize from FixML
  server.disconnect();
}

void summarize_data_formats(std::string caller,std::string user)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  auto cmd_databases=metautils::cmd_databases(caller,user);
  if (cmd_databases.size() == 0) {
    metautils::log_error("summarize_data_formats():  empty CMD database list",caller,user);
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  for (const auto& db : cmd_databases) {
    auto db_name=std::get<0>(db);
    auto table_name=db_name+".ds"+dsnum2+"_primaries2";
    if (table_name[0] != 'V' && table_exists(server,table_name)) {
	MySQL::LocalQuery query("select distinct f.format from "+table_name+" as p left join "+db_name+".formats as f on f.code = p.format_code");
	if (query.submit(server) < 0) {
	  metautils::log_error("summarize_data_formats():  "+query.error(),caller,user);
	}
	std::string error;
	if (server.command("lock tables search.formats write",error) < 0) {
	  metautils::log_error("summarize_data_formats(): "+server.error(),caller,user);
	}
	server._delete("search.formats","dsid = '"+metautils::args.dsnum+"' and (vocabulary = '"+db_name+"' or vocabulary = 'dssmm')");
	MySQL::Row row;
	while (query.fetch_row(row)) {
	  if (server.insert("search.formats","keyword,vocabulary,dsid","'"+row[0]+"','"+db_name+"','"+metautils::args.dsnum+"'","") < 0) {
	    metautils::log_warning("summarize_data_formats() issued warning: "+server.error(),caller,user);
	  }
	}
	if (server.command("unlock tables",error) < 0) {
	  metautils::log_error("summarize_data_formats(): "+server.error(),caller,user);
	}
    }
  }
  server.disconnect();
}

void create_non_cmd_file_list_cache(std::string file_type,my::map<CodeEntry>& files_with_cmd_table,std::string caller,std::string user)
{
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error("create_non_cmd_file_list_cache(): unable to create temporary directory",caller,user);
  }
// random sleep to minimize collisions from concurrent processes
  sleep( (getpid() % 15) );
  std::string cache_name,CMD_cache,nonCMD_cache,cnt_field,dsarch_flag;
  if (file_type == "MSS") {
    cache_name="getMssList_nonCMD.cache";
    CMD_cache=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/getMssList.cache",tdir.name());
    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/getMssList_nonCMD.cache",metautils::directives.rdadata_home)) {
	nonCMD_cache=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/getMssList_nonCMD.cache",tdir.name());
    }
    cnt_field="pmsscnt";
    dsarch_flag="-sm";
  }
  else if (file_type == "Web") {
    cache_name="getWebList_nonCMD.cache";
    CMD_cache=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/getWebList.cache",tdir.name());
    if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/getWebList_nonCMD.cache",metautils::directives.rdadata_home)) {
	nonCMD_cache=unixutils::remote_web_file("https://rda.ucar.edu/datasets/ds"+metautils::args.dsnum+"/metadata/getWebList_nonCMD.cache",tdir.name());
    }
    cnt_field="dwebcnt";
    dsarch_flag="-sw";
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
  MySQL::LocalQuery query("version,"+cnt_field,"dataset","dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("create_non_cmd_file_list_cache(): "+query.error()+" from query: "+query.show(),caller,user);
  }
  MySQL::Row row;
  if (!query.fetch_row(row)) {
    metautils::log_error("create_non_cmd_file_list_cache(): unable to get dataset version number",caller,user);
  }
  auto version=row[0];
  size_t filecnt=std::stoi(row[1]);
  std::ifstream ifs(CMD_cache.c_str());
  if (!ifs.is_open()) {
    metautils::log_error("create_non_cmd_file_list_cache(): could not open the CMD cache",caller,user);
  }
  char line[32768];
  ifs.getline(line,32768);
  auto num_lines=std::stoi(line);
  ifs.getline(line,32768);
  std::list<std::string> cache_list;
  size_t cmdcnt=0;
  auto n=0;
  while (!ifs.eof()) {
    ++n;
    if (n > num_lines) {
	break;
    }
    auto line_parts=strutils::split(line,"<!>");
    if (line_parts.size() != 9) {
	metautils::log_error("create_non_cmd_file_list_cache(): bad line in cache (1)",caller,user);
    }
    CodeEntry e;
    e.key=line_parts[0];
    if (!files_with_cmd_table.found(e.key,e)) {
	if (file_type == "MSS") {
	  auto file_parts=strutils::split(e.key,"..m..");
	  if (file_parts.size() > 1) {
	    query.set("select h.data_size,h.note from htarfile as h left join mssfile as m on m.mssid = h.mssid where m.mssfile = '"+file_parts[0]+"' and h.hfile = '"+file_parts[1]+"'");
	  }
	  else {
	    query.set("select data_size,note from mssfile where mssfile = '"+e.key+"'");
	  }
	}
	else if (file_type == "Web") {
	  query.set("select data_size,note from wfile where wfile = '"+e.key+"'");
	}
	if (query.submit(server) < 0) {
	  metautils::log_error("create_non_cmd_file_list_cache(): "+query.error()+" from query: "+query.show(),caller,user);
	}
	if (!query.fetch_row(row)) {
	  metautils::log_error("create_non_cmd_file_list_cache(): "+query.error()+" from query: "+query.show(),caller,user);
	}
	std::string cache_line=line_parts[0]+"<!>"+line_parts[1]+"<!>"+line_parts[5]+"<!>"+row[0]+"<!>"+row[1]+"<!>";
	if (!line_parts[7].empty()) {
	  auto group_parts=strutils::split(line_parts[7],"[!]");
	  cache_line+=group_parts[0];
	}
	cache_list.emplace_back(cache_line);
    }
    else {
	++cmdcnt;
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  ifs.clear();
  size_t noncmdcnt=0;
  ifs.open(nonCMD_cache.c_str());
  if (ifs.is_open()) {
    ifs.getline(line,32768);
    if (strutils::is_numeric(line)) {
	ifs.getline(line,32768);
	ifs.getline(line,32768);
	auto hpss_root_re=std::regex("^"+metautils::directives.hpss_root);
	while (!ifs.eof()) {
	  ++noncmdcnt;
	  auto line_parts=strutils::split(line,"<!>");
	  if (!std::regex_search(line_parts[0],hpss_root_re)) {
	    cache_list.clear();
	    break;
	  }
	  CodeEntry e;
	  e.key=line_parts[0];
	  if (!files_with_cmd_table.found(e.key,e)) {
	    cache_list.emplace_back(line);
	  }
	  ifs.getline(line,32768);
	}
    }
    ifs.close();
  }
  if ( (cmdcnt+cache_list.size()) != filecnt) {
    std::stringstream output,error;
    unixutils::mysystem2("/bin/tcsh -c \"dsarch -ds ds"+metautils::args.dsnum+" "+dsarch_flag+" -wn -md\"",output,error);
    query.set(cnt_field,"dataset","dsid = 'ds"+metautils::args.dsnum+"'");
    if (query.submit(server) < 0) {
	metautils::log_error("create_non_cmd_file_list_cache(): "+query.error()+" from query: "+query.show(),caller,user);
    }
    if (!query.fetch_row(row)) {
	metautils::log_error("create_non_cmd_file_list_cache(): unable to get dataset file count",caller,user);
    }
    filecnt=std::stoi(row[0]);
    if ( (cmdcnt+cache_list.size()) != filecnt) {
	cache_list.clear();
	noncmdcnt=0;
	if (file_type == "MSS") {
	  query.set("select m.mssfile,m.data_format,m.file_format,m.data_size,m.note,m.gindex,h.hfile,h.data_format,h.file_format,h.data_size,h.note from dssdb.mssfile as m left join dssdb.htarfile as h on h.mssid = m.mssid where m.dsid = 'ds"+metautils::args.dsnum+"' and m.type = 'P' and m.status = 'P'");
	}
	else if (file_type == "Web") {
	  query.set("select w.wfile,w.data_format,w.file_format,w.data_size,w.note,w.gindex from wfile as w where w.dsid = 'ds"+metautils::args.dsnum+"' and w.type = 'D' and w.status = 'P'");
	}
	if (query.submit(server) < 0)
	  metautils::log_error("create_non_cmd_file_list_cache(): "+query.error()+" from query: "+query.show(),caller,user);
	while (query.fetch_row(row)) {
	  CodeEntry e;
	  e.key=row[0];
	  if (file_type == "MSS" && !row[6].empty()) {
	    e.key+="..m.."+row[6];
	  }
	  if (!files_with_cmd_table.found(e.key,e)) {
	    if (file_type == "MSS" && !row[6].empty()) {
		cache_list.emplace_back(e.key+"<!>"+row[7]+"<!>"+row[8]+"<!>"+row[9]+"<!>"+row[10]+"<!>"+row[5]);
	    }
	    else {
		cache_list.emplace_back(e.key+"<!>"+row[1]+"<!>"+row[2]+"<!>"+row[3]+"<!>"+row[4]+"<!>"+row[5]);
	    }
	  }
	}
    }
  }
  if (noncmdcnt != cache_list.size()) {
    TempDir tdir;
    if (!tdir.create(metautils::directives.temp_path)) {
	metautils::log_error("create_non_cmd_file_list_cache(): unable to create temporary directory",caller,user);
    }
// create the directory tree in the temp directory
    std::stringstream output,error;
    if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata",output,error) < 0) {
	metautils::log_error("write_initialize(): unable to create a temporary directory (1) - '"+error.str()+"'",caller,user);
    }
    std::ofstream ofs((tdir.name()+"/metadata/"+cache_name).c_str());
    if (!ofs.is_open()) {
	metautils::log_error("create_non_cmd_file_list_cache(): unable to open output for "+cache_name,caller,user);
    }
    ofs << version << std::endl;
    ofs << cache_list.size() << std::endl;
    for (const auto& item : cache_list) {
	ofs << item << std::endl;
    }
    ofs.close();
    std::string herror;
    if (unixutils::rdadata_sync(tdir.name(),"metadata/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,herror) < 0) {
	metautils::log_warning("create_non_cmd_file_list_cache() couldn't sync '"+cache_name+"' - rdadata_sync error(s): '"+herror+"'",caller,user);
    }
  }
  else if (noncmdcnt == 0) {
    std::string herror;
    if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+cache_name,metautils::directives.rdadata_home,herror) < 0) {
	metautils::log_warning("createNonCMDFileCache couldn't unsync '"+cache_name+"' - hostSync error(s): '"+herror+"'",caller,user);
    }
  }
  server.disconnect();
}

struct XEntry {
  XEntry() : key(),strings() {}

  std::string key;
  std::shared_ptr<std::vector<std::string>> strings;
};

void fill_gindex_table(MySQL::Server& server,my::map<XEntry>& gindex_table,std::string caller,std::string user)
{
  MySQL::Query query("select gindex,grpid from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0) {
    metautils::log_error("fill_gindex_table(): "+query.error()+" while trying to get groups data",caller,user);
  }
  MySQL::Row row;
  while (query.fetch_row(row)) {
    XEntry xe;
    xe.key=row[0];
    xe.strings.reset(new std::vector<std::string>);
    xe.strings->emplace_back(row[1]);
    gindex_table.insert(xe);
  }
}

void fill_rdadb_files_table(std::string file_type,MySQL::Server& server,my::map<XEntry>& rda_files_table,std::string caller,std::string user)
{
  MySQL::Query query;
  MySQL::Row row;
  XEntry xe;

  if (file_type == "MSS") {
    query.set("select m.mssfile,m.gindex,m.file_format,m.data_size,h.hfile,h.file_format,h.data_size from dssdb.mssfile as m left join dssdb.htarfile as h on h.mssid = m.mssid where m.dsid = 'ds"+metautils::args.dsnum+"' and m.type = 'P' and m.status = 'P'");
  }
  else {
    query.set("select wfile,gindex,file_format,data_size from dssdb.wfile where dsid = 'ds"+metautils::args.dsnum+"' and type = 'D' and status = 'P'");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("fill_rdadb_files_table(): "+query.error()+" while trying to get RDA file data",caller,user);
  }
  while (query.fetch_row(row)) {
    xe.key=row[0];
    xe.strings.reset(new std::vector<std::string>);
    xe.strings->emplace_back(row[1]);
    if (file_type == "MSS" && !row[4].empty()) {
	xe.key+="..m.."+row[4];
	xe.strings->emplace_back(row[5]);
	xe.strings->emplace_back(row[6]);
    }
    else {
	xe.strings->emplace_back(row[2]);
	xe.strings->emplace_back(row[3]);
    }
    rda_files_table.insert(xe);
  }
}

void fill_data_formats_table(std::string file_type,std::string db,MySQL::Server& server,my::map<XEntry>& data_formats,std::string caller,std::string user)
{
  MySQL::Query query;
  if (file_type == "MSS") {
    query.set("select code,format from "+db+".formats");
  }
  else {
    query.set("select code,format from W"+db+".formats");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("fill_data_formats_table(): "+query.error()+" while trying to get formats data",caller,user);
  }
  MySQL::Row row;
  while (query.fetch_row(row)) {
    XEntry xe;
    xe.key=row[0];
    xe.strings.reset(new std::vector<std::string>);
    xe.strings->emplace_back(row[1]);
    data_formats.insert(xe);
  }
}

struct FileEntry {
  FileEntry() : key(),data_format(),units(),metafile_id(),start(),end(),file_format(),data_size(),group_id() {}

  std::string key;
  std::string data_format,units,metafile_id;
  std::string start,end;
  std::string file_format,data_size;
  std::string group_id;
};
void get_file_data(MySQL::Server& server,MySQL::Query& query,std::string unit,std::string unit_plural,my::map<XEntry>& gindex_table,my::map<XEntry>& rda_files_table,my::map<XEntry>& data_formats,my::map<FileEntry>& table)
{
  for (const auto& row : query) {
    XEntry xe;
    if (rda_files_table.found(row[0],xe)) {
	FileEntry fe;
	fe.key=row[0];
	XEntry xe2;
	data_formats.found(row[1],xe2);
	fe.data_format=xe2.strings->at(0);
	fe.units=row[2]+" "+unit;
	if (row[2] != "1") {
	  fe.units+=unit_plural;
	}
	fe.start=dateutils::string_ll_to_date_string(row[3]);
	fe.end=dateutils::string_ll_to_date_string(row[4]);
	if (gindex_table.found(xe.strings->at(0),xe2)) {
	  if (!xe2.strings->at(0).empty()) {
	    fe.group_id=xe2.strings->at(0);
	  }
	  else {
	    fe.group_id=xe.strings->at(0);
	  }
	  fe.group_id+="[!]"+xe.strings->at(0);
	}
	else {
	  fe.group_id="";
	}
	fe.file_format=xe.strings->at(1);
	fe.data_size=xe.strings->at(2);
	table.insert(fe);
    }
  }
}

void grml_file_data(std::string file_type,my::map<XEntry>& gindex_table,my::map<XEntry>& rda_files_table,my::map<FileEntry>& grml_file_data_table,std::string& caller,std::string& user)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Query query;
  my::map<XEntry> data_formats;
  XEntry xe;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("grml_file_data(): "+server.error()+" while trying to connect",caller,user);
  }
  fill_data_formats_table(file_type,"GrML",server,data_formats,caller,user);
  if (file_type == "MSS") {
    query.set("select mssID,format_code,num_grids,start_date,end_date from GrML.ds"+dsnum2+"_primaries2 where num_grids > 0");
  }
  else {
    query.set("select webID,format_code,num_grids,start_date,end_date from WGrML.ds"+dsnum2+"_webfiles2 where num_grids > 0");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("grml_file_data(): "+query.error()+" while trying to get metadata file data",caller,user);
  }
  get_file_data(server,query,"Grid","s",gindex_table,rda_files_table,data_formats,grml_file_data_table);
  server.disconnect();
}

void obml_file_data(std::string file_type,my::map<XEntry>& gindex_table,my::map<XEntry>& rda_files_table,my::map<FileEntry>& obml_file_data_table,std::string& caller,std::string& user)
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  my::map<XEntry> data_formats;
  fill_data_formats_table(file_type,"ObML",server,data_formats,caller,user);
  MySQL::Query query;
  auto dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  if (file_type == "MSS") {
    query.set("select mssID,format_code,num_observations,start_date,end_date from ObML.ds"+dsnum2+"_primaries2 where num_observations > 0");
  }
  else {
    query.set("select webID,format_code,num_observations,start_date,end_date from WObML.ds"+dsnum2+"_webfiles2 where num_observations > 0");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("obml_file_data(): "+query.error()+" while trying to get metadata file data",caller,user);
  }
  get_file_data(server,query,"Observation","s",gindex_table,rda_files_table,data_formats,obml_file_data_table);
  server.disconnect();
}

void fixml_file_data(std::string file_type,my::map<XEntry>& gindex_table,my::map<XEntry>& rda_files_table,my::map<FileEntry>& fixml_file_data_table,std::string& caller,std::string& user)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Query query;
  my::map<XEntry> data_formats;
  XEntry xe,xe2;
  FileEntry fe;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("fixml_file_data(): "+server.error()+" while trying to connect",caller,user);
  }
  fill_data_formats_table(file_type,"FixML",server,data_formats,caller,user);
  if (file_type == "MSS") {
    query.set("select mssID,format_code,num_fixes,start_date,end_date from FixML.ds"+dsnum2+"_primaries2 where num_fixes > 0");
  }
  else {
    query.set("select webID,format_code,num_fixes,start_date,end_date from WFixML.ds"+dsnum2+"_webfiles2 where num_fixes > 0");
  }
  if (query.submit(server) < 0) {
    metautils::log_error("fixml_file_data(): "+query.error()+" while trying to get metadata file data",caller,user);
  }
  get_file_data(server,query,"Cyclone Fix","es",gindex_table,rda_files_table,data_formats,fixml_file_data_table);
  server.disconnect();
}

struct ParameterEntry {
  ParameterEntry() : key(),description(),short_name(),count(0),selected(false) {}

  std::string key;
  std::string description,short_name;
  size_t count;
  bool selected;
};
void write_grml_parameters(std::string& file_type,std::string& tindex,std::ofstream& ofs,std::string& caller,std::string& user,std::string& min,std::string& max,std::string& init_date_selection)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server)
    metautils::log_error("write_grml_parameters(): "+server.error()+" while trying to connect",caller,user);
  std::string db_prefix,dssdb_filetable,metadata_filetable,where_conditions,id_type;
  if (file_type == "MSS") {
    db_prefix="";
    dssdb_filetable="mssfile";
    metadata_filetable="_primaries2";
    where_conditions="type = 'P' and status = 'P'";
    id_type="mss";
  }
  else if (file_type == "Web" || file_type == "inv") {
    db_prefix="W";
    dssdb_filetable="wfile";
    metadata_filetable="_webfiles2";
    where_conditions="type = 'D' and status = 'P'";
    id_type="web";
  }
  MySQL::LocalQuery query("code,format",db_prefix+"GrML.formats");
  if (query.submit(server) < 0) {
    metautils::log_error("write_grml_parameters(): "+query.error()+" while trying to get formats",caller,user);
  }
  std::unordered_map<std::string,std::string> data_formats;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    data_formats.emplace(row[0],row[1]);
  }
  std::unordered_set<std::string> rda_files;
  if (!tindex.empty()) {
    query.set(dssdb_filetable,"dssdb."+dssdb_filetable,"dsid = 'ds"+metautils::args.dsnum+"' and tindex = "+tindex+" and "+where_conditions);
    if (query.submit(server) < 0)
	metautils::log_error("write_grml_parameters(): "+query.error()+" while trying to get RDA files from dssdb."+dssdb_filetable,caller,user);
    while (query.fetch_row(row)) {
	rda_files.emplace(row[0]);
    }
  }
  std::unordered_set<std::string> inv_codes;
  if (file_type == "inv") {
    query.set("webID_code,dupe_vdates","IGrML.ds"+dsnum2+"_inventory_summary");
    if (query.submit(server) < 0) {
	metautils::log_error("write_grml_parameters(): "+query.error()+" while trying to get inventory file codes",caller,user);
    }
    init_date_selection="";
    while (query.fetch_row(row)) {
	inv_codes.emplace(row[0]);
	if (init_date_selection.empty() && row[1] == "Y") {
	  init_date_selection="I";
	}
    }
  }
  MySQL::Query s_query("code,"+id_type+"ID,format_code",db_prefix+"GrML.ds"+dsnum2+metadata_filetable);
  if (s_query.submit(server) < 0) {
    metautils::log_error("write_grml_parameters(): "+s_query.error()+" while trying to get metadata files from "+db_prefix+"GrML.ds"+dsnum2+metadata_filetable,caller,user);
  }
  std::unordered_map<std::string,std::string> metadata_files;
  while (s_query.fetch_row(row)) {
    if ((rda_files.size() == 0 || rda_files.find(row[1]) != rda_files.end()) && (inv_codes.size() == 0 || inv_codes.find(row[0]) != inv_codes.end())) {
	metadata_files.emplace(row[0],row[2]);
    }
  }
  s_query.set("parameter,start_date,end_date,"+id_type+"ID_code",db_prefix+"GrML.ds"+dsnum2+"_agrids");
  if (s_query.submit(server) < 0) {
    metautils::log_error("write_grml_parameters(): "+s_query.error()+" while trying to get agrids data",caller,user);
  }
  std::unordered_set<std::string> unique_parameters;
  my::map<ParameterEntry> parameter_description_table;
  xmlutils::ParameterMapper parameter_mapper(metautils::directives.parameter_map_path);
  std::vector<ParameterEntry> parray;
  auto len=0;
  while (s_query.fetch_row(row)) {
    auto mf_elem=metadata_files.find(row[3]); 
    if (mf_elem != metadata_files.end()) {
	auto u_param=mf_elem->second+"!"+row[0];
	if (unique_parameters.find(u_param) == unique_parameters.end()) {
	  auto data_format=data_formats[mf_elem->second];
	  ParameterEntry pe;
	  pe.key=parameter_mapper.description(data_format,row[0]);
	  if (!parameter_description_table.found(pe.key,pe)) {
	    pe.count=len;
	    parray.emplace_back();
	    parray.back().key=mf_elem->second+"!"+row[0];
	    if (strutils::contains(parray.back().key,"@")) {
		parray.back().key=parray.back().key.substr(0,parray.back().key.find("@"));
	    }
	    parray.back().description=pe.key;
	    strutils::replace_all(parray.back().description,"<br />"," ");
	    parray.back().short_name=parameter_mapper.short_name(data_format,row[0]);
	    parameter_description_table.insert(pe);
	    ++len;
	  }
	  else {
	    auto pcode=mf_elem->second+"!"+row[0];
	    if (std::regex_search(pcode,std::regex("@"))) {
		pcode=pcode.substr(0,pcode.find("@"));
	    }
	    if (!strutils::contains(parray[pe.count].key,pcode)) {
		parray[pe.count].key+=","+pcode;
	    }
	  }
	  unique_parameters.emplace(u_param);
	}
	if (row[1] < min) {
	  min=row[1];
	}
	if (row[2] > max) {
	  max=row[2];
	}
    }
  }
  server.disconnect();
  parray.resize(len);
  binary_sort(parray,
  [](ParameterEntry& left,ParameterEntry& right) -> bool
  {
    if (strutils::to_lower(left.description) <= strutils::to_lower(right.description)) {
	return true;
    }
    else {
	return false;
    }
  });
  ofs << len << std::endl;
  for (const auto& p : parray) {
    ofs << p.key << "<!>" << p.description << "<!>" << p.short_name << std::endl;
  }
}

void write_groups(std::string& file_type,std::string db,std::ofstream& ofs,std::string& caller,std::string& user)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  my::map<XEntry> groups_table,files_table(99999),matched_groups_table;
  XEntry xe,xe2;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    metautils::log_error("write_groups(): "+server.error()+" while trying to connect",caller,user);
  }
  MySQL::LocalQuery query("select gindex,title,grpid from dssdb.dsgroup where dsid = 'ds"+metautils::args.dsnum+"'");
  if (query.submit(server) < 0)
    metautils::log_error("write_groups(): "+query.error()+" while trying to get list of groups",caller,user);
  if (query.num_rows() > 1) {
    std::string metadata_db_prefix,ID_type,metadata_filetable,dssdb_filetable,dssdb_typedescr,dssdb_filetype;
    if (file_type == "MSS") {
	metadata_db_prefix="";
	ID_type="mss";
	metadata_filetable="_primaries2";
	dssdb_filetable="mssfile";
	dssdb_typedescr="property";
	dssdb_filetype="P";
    }
    else if (file_type == "Web" || file_type == "inv") {
	metadata_db_prefix="W";
	ID_type="web";
	metadata_filetable="_webfiles2";
	dssdb_filetable="wfile";
	dssdb_typedescr="type";
	dssdb_filetype="D";
    }
    MySQL::Row row;
    while (query.fetch_row(row)) {
	xe.key=row[0];
	xe.strings.reset(new std::vector<std::string>);
	xe.strings.get()->emplace_back(row[1]);
	if (row[2].empty()) {
	  xe.strings.get()->emplace_back(row[0]);
	}
	else {
	  xe.strings.get()->emplace_back(row[2]);
	}
	groups_table.insert(xe);
    }
    query.set("select "+ID_type+"ID from "+metadata_db_prefix+db+".ds"+dsnum2+metadata_filetable);
    if (query.submit(server) < 0) {
	metautils::log_error("write_groups(): "+query.error()+" while trying to get webfiles",caller,user);
    }
    while (query.fetch_row(row)) {
	xe.key=row[0];
	files_table.insert(xe);
    }
    query.set("select "+dssdb_filetable+",gindex from dssdb."+dssdb_filetable+" where dsid = 'ds"+metautils::args.dsnum+"' and "+dssdb_typedescr+" = '"+dssdb_filetype+"'");
    if (query.submit(server) < 0) {
	metautils::log_error("write_groups(): "+query.error()+" while trying to get wfile,gindex list",caller,user);
    }
    std::vector<XEntry> array;
    array.resize(query.num_rows());
    auto n=0;
    while (query.fetch_row(row)) {
	if (files_table.found(row[0],xe)) {
	  if (!matched_groups_table.found(row[1],xe)) {
	    if (groups_table.found(row[1],xe2)) {
		array[n].key=row[1];
		array[n].strings=xe2.strings;
		++n;
		xe.key=row[1];
		matched_groups_table.insert(xe);
	    }
	  }
	}
    }
    array.resize(n);
    binary_sort(array,
    [](XEntry& left,XEntry& right) -> bool
    {
	if (left.key.length() < right.key.length()) {
	  return true;
	}
	else if (left.key.length() > right.key.length()) {
	  return false;
	}
	else {
	  if (left.key <= right.key) {
	    return true;
	  }
	  else {
	    return false;
	  }
	}
    });
    for (size_t n=0; n < array.size(); ++n) {
	ofs << array[n].key << "<!>" << (*array[n].strings.get())[0] << "<!>" << (*array[n].strings.get())[1] << std::endl;
    }
  }
  server.disconnect();
}

void create_file_list_cache(std::string file_type,std::string caller,std::string user,std::string tindex)
{
  static const std::string THIS_FUNC=__func__;
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::LocalQuery satellite_query;
  MySQL::LocalQuery ocustom,fquery;
  MySQL::Row row;
  std::string min,max,ext;
  std::vector<FileEntry> array;
  std::ofstream ofs;
  XMLElement e;
  std::string filename,output,error;
  CodeEntry ee;
  my::map<FileEntry> grml_file_data_table(99999),obml_file_data_table(99999),fixml_file_data_table(99999);
  FileEntry fe;
  XEntry xe;
  bool can_customize_GrML=false;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  my::map<XEntry> gindex_table;
  fill_gindex_table(server,gindex_table,caller,user);
  my::map<XEntry> rda_files_table(99999);
  fill_rdadb_files_table(file_type,server,rda_files_table,caller,user);
  if (file_type == "MSS") {
    if (table_exists(server,"GrML.ds"+dsnum2+"_primaries2")) {
	grml_file_data(file_type,gindex_table,rda_files_table,grml_file_data_table,caller,user);
	can_customize_GrML=true;
    }
    if (table_exists(server,"ObML.ds"+dsnum2+"_primaries2")) {
	obml_file_data(file_type,gindex_table,rda_files_table,obml_file_data_table,caller,user);
	if (tindex.empty()) {
	  ocustom.set("select min(start_date),max(end_date) from ObML.ds"+dsnum2+"_primaries2 where start_date > 0");
	}
	else {
	  ocustom.set("select min(start_date),max(end_date) from ObML.ds"+dsnum2+"_primaries2 as p left join dssdb.mssfile as m on m.mssfile = p.mssID where m.tindex = "+tindex+" and m.dsid = 'ds"+metautils::args.dsnum+"' and start_date > 0");
	}
    }
    if (table_exists(server,"SatML.ds"+dsnum2+"_primaries2")) {
	satellite_query.set("select p.mssID,f.format,p.num_products,p.start_date,p.end_date,m.file_format,m.data_size,g.grpid,g.gindex,p.product from SatML.ds"+dsnum2+"_primaries2 as p left join SatML.formats as f on f.code = p.format_code left join dssdb.mssfile as m on m.mssfile = p.mssID left join dssdb.dsgroup as g on m.gindex = g.gindex where g.dsid = 'ds"+metautils::args.dsnum+"' or m.gindex = 0");
	if (satellite_query.submit(server) < 0)
	  metautils::log_error(THIS_FUNC+"(): "+satellite_query.error()+" from query: "+satellite_query.show(),caller,user);
    }
    if (table_exists(server,"FixML.ds"+dsnum2+"_primaries2")) {
	fixml_file_data(file_type,gindex_table,rda_files_table,fixml_file_data_table,caller,user);
    }
  }
  else if (file_type == "Web" || file_type == "inv") {
    if (table_exists(server,"WGrML.ds"+dsnum2+"_webfiles2")) {
	if (file_type == "inv" && !table_exists(server,"IGrML.ds"+dsnum2+"_inventory_summary")) {
	  return;
	}
	if (file_type == "Web" && tindex.empty()) {
	  grml_file_data(file_type,gindex_table,rda_files_table,grml_file_data_table,caller,user);
	}
	can_customize_GrML=true;
    }
    if (table_exists(server,"WObML.ds"+dsnum2+"_webfiles2")) {
	if (file_type == "Web" && tindex.empty()) {
	  obml_file_data(file_type,gindex_table,rda_files_table,obml_file_data_table,caller,user);
	}
	if (file_type == "Web") {
	  ocustom.set("select min(start_date),max(end_date) from WObML.ds"+dsnum2+"_webfiles2 where start_date > 0");
	}
	else if (file_type == "inv") {
	  if (table_exists(server,"IObML.ds"+dsnum2+"_dataTypes")) {
	    ocustom.set("select min(start_date),max(end_date) from WObML.ds"+dsnum2+"_webfiles2 as w left join (select distinct webID_code from IObML.ds"+dsnum2+"_dataTypes) as d on d.webID_code = w.code where !isnull(d.webID_code) and start_date > 0");
	  }
	  else if (table_exists(server,"IObML.ds"+dsnum2+"_inventory_summary")) {
	    ocustom.set("select min(start_date),max(end_date) from WObML.ds"+dsnum2+"_webfiles2 as w left join IObML.ds"+dsnum2+"_inventory_summary as i on i.webID_code = w.code where !isnull(i.webID_code) and start_date > 0");
	  }
	  else {
	    return;
	  }
	}
    }
    if (table_exists(server,"WFixML.ds"+dsnum2+"_webfiles2")) {
	if (tindex.empty()) {
	  fixml_file_data(file_type,gindex_table,rda_files_table,fixml_file_data_table,caller,user);
	}
    }
  }
  if (can_customize_GrML) {
    ext="";
    if (file_type == "MSS") {
	ext="GrML";
    }
    else if (file_type == "Web") {
	ext="WGrML";
    }
    else if (file_type == "inv") {
	ext="IGrML";
    }
    if (!ext.empty()) {
	filename="customize."+ext;
	if (!tindex.empty()) {
	  filename+="."+tindex;
	}
	TempDir tdir;
	if (!tdir.create(metautils::directives.temp_path)) {
	  metautils::log_error(THIS_FUNC+"(): unable to create temporary directory (1)",caller,user);
	}
// create the directory tree in the temp directory
	std::stringstream oss,ess;
	if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata",oss,ess) < 0) {
	  metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory tree (1) - '"+ess.str()+"'",caller,user);
	}
	ofs.open((tdir.name()+"/metadata/"+filename).c_str());
	if (!ofs.is_open()) {
	  metautils::log_error(THIS_FUNC+"(): unable to open temporary file for "+filename,caller,user);
	}
	if (unixutils::exists_on_server(metautils::directives.web_server,"/data/web/datasets/ds"+metautils::args.dsnum+"/metadata/inv",metautils::directives.rdadata_home)) {
	  ofs << "curl_subset=Y" << std::endl;
	}
	min="99999999999999";
	max="0";
	std::string init_date_selection;
	write_grml_parameters(file_type,tindex,ofs,caller,user,min,max,init_date_selection);
	ofs << min << " " << max;
	if (!init_date_selection.empty()) {
	  ofs << " " << init_date_selection;
	}
	ofs << std::endl;
	if (tindex.empty()) {
	  write_groups(file_type,"GrML",ofs,caller,user);
	}
	ofs.close();
	if (max == "0") {
	  if (unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+filename,metautils::directives.rdadata_home,error) < 0) {
	    metautils::log_warning(THIS_FUNC+"() couldn't unsync '"+filename+"' - rdadata_unsync error(s): '"+error+"'",caller,user);
	  }
	}
	else {
	  if (unixutils::rdadata_sync(tdir.name(),"metadata/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
	    metautils::log_warning(THIS_FUNC+"() couldn't sync '"+filename+"' - rdadata_sync error(s): '"+error+"'",caller,user);
	  }
	}
    }
  }
  xmlutils::DataTypeMapper data_type_mapper(metautils::directives.parameter_map_path);
  if (ocustom) {
    if (ocustom.submit(server) < 0) {
	metautils::log_error(THIS_FUNC+"(): "+ocustom.error()+" from query: "+ocustom.show(),caller,user);
    }
    ext="";
    ocustom.fetch_row(row);
    if (!row[0].empty() && !row[1].empty()) {
	if (file_type == "MSS") {
	  ext="ObML";
	}
	else if (file_type == "Web") {
	  ext="WObML";
	}
	else if (file_type == "inv") {
	  ext="IObML";
	}
    }
    if (!ext.empty()) {
	filename="customize."+ext;
	if (!tindex.empty()) {
	  filename+="."+tindex;
	}
	TempDir tdir;
	if (!tdir.create(metautils::directives.temp_path)) {
	  metautils::log_error(THIS_FUNC+"(): unable to create temporary directory (2)",caller,user);
	}
// create the directory tree in the temp directory
	std::stringstream oss,ess;
	if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata",oss,ess) < 0) {
	  metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory tree (2) - '"+ess.str()+"'",caller,user);
	}
	ofs.open((tdir.name()+"/metadata/"+filename).c_str());
	if (!ofs.is_open()) {
	  metautils::log_error(THIS_FUNC+"(): unable to open temporary file for "+filename,caller,user);
	}
// date range
	ofs << row[0] << " " << row[1] << std::endl;
// platform types
	if (file_type == "MSS") {
	  if (MySQL::table_exists(server,"ObML.ds"+dsnum2+"_dataTypes2")) {
	    ocustom.set("select distinct l.platformType_code,p.platformType from ObML.ds"+dsnum2+"_dataTypes2 as d left join ObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code left join ObML.platformTypes as p on p.code = l.platformType_code");
	  }
	  else {
	    ocustom.set("select distinct d.platformType_code,p.platformType from ObML.ds"+dsnum2+"_dataTypes as d left join ObML.platformTypes as p on p.code = d.platformType_code");
	  }
	}
	else if (file_type == "Web") {
	  ocustom.set("select distinct l.platformType_code,p.platformType from WObML.ds"+dsnum2+"_dataTypes2 as d left join WObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code left join WObML.platformTypes as p on p.code = l.platformType_code");
	}
	else if (file_type == "inv") {
	  ocustom.set("select distinct l.platformType_code,p.platformType from WObML.ds"+dsnum2+"_dataTypes2 as d left join WObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code left join WObML.platformTypes as p on p.code = l.platformType_code left join (select distinct webID_code from IObML.ds"+dsnum2+"_dataTypes) as dt on dt.webID_code = d.webID_code where !isnull(dt.webID_code)");
	}
	if (ocustom.submit(server) < 0) {
	  metautils::log_error(THIS_FUNC+"(): "+ocustom.error()+" from query: "+ocustom.show(),caller,user);
	}
	ofs << ocustom.num_rows() << std::endl;
	while (ocustom.fetch_row(row)) {
	  ofs << row[0] << "<!>" << row[1] << std::endl;
	}
// data types and formats
	if (file_type == "MSS") {
	  fquery.set("select distinct f.format from ObML.ds"+dsnum2+"_primaries2 as p left join ObML.formats as f on f.code = p.format_code");
	  if (MySQL::table_exists(server,"ObML.ds"+dsnum2+"_dataTypes2")) {
	    ocustom.set("select distinct l.dataType from ObML.ds"+dsnum2+"_dataTypes2 as d left join ObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code");
	  }
	  else {
	    ocustom.set("select distinct dataType from ObML.ds"+dsnum2+"_dataTypes");
	  }
	}
	else if (file_type == "Web" || file_type == "inv") {
	  fquery.set("select distinct f.format from WObML.ds"+dsnum2+"_webfiles2 as w left join WObML.formats as f on f.code = w.format_code");
	  ocustom.set("select distinct l.dataType from WObML.ds"+dsnum2+"_dataTypes2 as d left join WObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code");
	}
	if (fquery.submit(server) < 0) {
	  metautils::log_error(THIS_FUNC+"(): "+fquery.error()+" from query: "+fquery.show(),caller,user);
	}
	if (ocustom.submit(server) < 0) {
	  metautils::log_error(THIS_FUNC+"(): "+ocustom.error()+" from query: "+ocustom.show(),caller,user);
	}
	std::vector<std::string> data_formats;
	while (fquery.fetch_row(row)) {
	  data_formats.emplace_back(row[0]);
	}
	my::map<XEntry> unique_data_types_table;
	while (ocustom.fetch_row(row)) {
	  auto dtype_parts=strutils::split(row[0],":");
	  std::string format_specialization,data_type_code;
	  if (dtype_parts.size() == 2) {
	    format_specialization=dtype_parts.front();
	    data_type_code=dtype_parts.back();
	  }
	  else {
	    format_specialization="ds"+metautils::args.dsnum;
	    data_type_code=dtype_parts.front();
	  }
	  for (const auto& data_format : data_formats) {
	    xe.key=data_type_mapper.description(data_format,format_specialization,data_type_code);
	    if (!xe.key.empty()) {
		if (!unique_data_types_table.found(xe.key,xe)) {
		  xe.strings.reset(new std::vector<std::string>(1));
		  unique_data_types_table.insert(xe);
		}
		if (xe.strings->size() > 0 && !xe.strings->at(0).empty()) {
		  xe.strings->at(0)+=",";
		}
		xe.strings->at(0)+=row[0];
	    }
	  }
	}
	if (unique_data_types_table.size() > 0) {
	  ofs << unique_data_types_table.size() << std::endl;
	  unique_data_types_table.keysort(
	  [](const std::string& left,const std::string& right) -> bool
	  {
	    if (left <= right) {
		return true;
	    }
	    else {
		return false;
	    }
	  });
	  for (const auto& key : unique_data_types_table.keys()) {
	    unique_data_types_table.found(key,xe);
	    ofs << xe.strings->at(0) << "<!>" << xe.key << std::endl;
	  }
	}
// groups
	if (tindex.empty()) {
	  if (file_type == "MSS") {
	    ocustom.set("select distinct g.gindex,g.title,g.grpid from (select m.gindex from ObML.ds"+dsnum2+"_primaries2 as p left join dssdb.mssfile as m on (m.dsid = 'ds"+metautils::args.dsnum+"' and m.mssfile = p.mssID)) as m left join dssdb.dsgroup as g on (g.dsid = 'ds"+metautils::args.dsnum+"' and g.gindex = m.gindex) where !isnull(g.gindex) order by g.gindex");
	  }
	  else {
	    if (field_exists(server,"WObML.ds"+dsnum2+"_webfiles2","dsid")) {
		ocustom.set("select distinct g.gindex,g.title,g.grpid from WObML.ds"+dsnum2+"_webfiles2 as p left join dssdb.wfile as w on (w.wfile = p.webID and w.type = p.type and w.dsid = p.dsid) left join dssdb.dsgroup as g on (g.dsid = 'ds"+metautils::args.dsnum+"' and g.gindex = w.gindex) where !isnull(w.wfile) order by g.gindex");
	    }
	    else {
		ocustom.set("select distinct g.gindex,g.title,g.grpid from WObML.ds"+dsnum2+"_webfiles2 as p left join dssdb.wfile as w on w.wfile = p.webID left join dssdb.dsgroup as g on (g.dsid = 'ds"+metautils::args.dsnum+"' and g.gindex = w.gindex) where !isnull(w.wfile) order by g.gindex");
	    }
	  }
	  if (ocustom.submit(server) < 0) {
	    metautils::log_error(THIS_FUNC+"(): "+ocustom.error()+" from query: "+ocustom.show(),caller,user);
	  }
	  if (ocustom.num_rows() > 1) {
	    while (ocustom.fetch_row(row)) {
		auto grpid=row[2];
		if (grpid.empty()) {
		  grpid=row[0];
		}
		ofs << row[0] << "<!>" << row[1] << "<!>" << grpid << std::endl;
	    }
	  }
	}
	ofs.close();
	if (unixutils::rdadata_sync(tdir.name(),"metadata/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
	  metautils::log_warning(THIS_FUNC+"() couldn't sync '"+filename+"' - rdadata_sync error(s): '"+error+"'",caller,user);
	}
    }
  }
  if (file_type == "inv") {
// for inventories, the work is done here and we can return
    server.disconnect();
    return;
  }
  my::map<CodeEntry> files_with_CMD_table(99999);
  auto num=grml_file_data_table.size()+obml_file_data_table.size()+satellite_query.num_rows()+fixml_file_data_table.size();
  if (num > 0) {
    array.resize(num);
    auto n=0;
    for (const auto& key : grml_file_data_table.keys()) {
	grml_file_data_table.found(key,fe);
	array[n]=fe;
	array[n].metafile_id=fe.key;
/*
	if (file_type == "MSS") {
	  strutils::replace_all(array[n].metafile_id,"/FS/DSS/","");
	  strutils::replace_all(array[n].metafile_id,"/DSS/","");
	}
*/
	ee.key=fe.key;
	files_with_CMD_table.insert(ee);
//	strutils::replace_all(array[n].metafile_id,"/","%25");
	strutils::replace_all(array[n].metafile_id,"+","%2B");
	array[n].metafile_id+=".GrML";
	strutils::replace_all(array[n].data_format,"proprietary_","");
	++n;
    }
    for (const auto& key : obml_file_data_table.keys()) {
	obml_file_data_table.found(key,fe);
	array[n]=fe;
	array[n].metafile_id=fe.key;
/*
	if (file_type == "MSS") {
	  strutils::replace_all(array[n].metafile_id,"/FS/DSS/","");
	  strutils::replace_all(array[n].metafile_id,"/DSS/","");
	}
*/
	ee.key=fe.key;
	files_with_CMD_table.insert(ee);
//	strutils::replace_all(array[n].metafile_id,"/","%25");
	strutils::replace_all(array[n].metafile_id,"+","%2B");
	array[n].metafile_id+=".ObML";
	strutils::replace_all(array[n].data_format,"proprietary_","");
	++n;
    }
    while (satellite_query.fetch_row(row)) {
	array[n].key=row[0];
	array[n].metafile_id=row[0];
/*
	if (file_type == "MSS") {
	  strutils::replace_all(array[n].metafile_id,"/FS/DSS/","");
	  strutils::replace_all(array[n].metafile_id,"/DSS/","");
	}
*/
	ee.key=row[0];
	files_with_CMD_table.insert(ee);
//	strutils::replace_all(array[n].metafile_id,"/","%25");
	strutils::replace_all(array[n].metafile_id,"+","%2B");
	array[n].metafile_id+=".SatML";
	array[n].data_format=row[1];
	strutils::replace_all(array[n].data_format,"proprietary_","");
	array[n].units=row[2];
	if (row[9] == "I") {
	  array[n].units+=" Image";
	}
	else if (row[9] == "S") {
	  array[n].units+=" Scan Line";
	}
	if (row[2] != "1") {
	  array[n].units+="s";
	}
	array[n].start=dateutils::string_ll_to_date_string(row[3]);
	array[n].end=dateutils::string_ll_to_date_string(row[4]);
	array[n].file_format=row[5];
	array[n].data_size=row[6];
	if (!row[7].empty()) {
	  array[n].group_id=row[7];
	}
	else {
	  array[n].group_id=row[8];
	}
	if (!array[n].group_id.empty()) {
	  array[n].group_id+="[!]"+row[8];
	}
	++n;
    }
    for (const auto& key : fixml_file_data_table.keys()) {
	fixml_file_data_table.found(key,fe);
	array[n]=fe;
	array[n].metafile_id=fe.key;
/*
	if (file_type == "MSS") {
	  strutils::replace_all(array[n].metafile_id,"/FS/DSS/","");
	  strutils::replace_all(array[n].metafile_id,"/DSS/","");
	}
*/
	ee.key=fe.key;
	files_with_CMD_table.insert(ee);
//	strutils::replace_all(array[n].metafile_id,"/","%25");
	strutils::replace_all(array[n].metafile_id,"+","%2B");
	array[n].metafile_id+=".FixML";
	strutils::replace_all(array[n].data_format,"proprietary_","");
	++n;
    }
    binary_sort(array,
    [](const FileEntry& left,const FileEntry& right) -> bool
    {
	if (left.start < right.start) {
	  return true;
	}
	else if (left.start > right.start) {
	  return false;
	}
	else {
	  if (left.end == right.end) {
	    if (left.key <= right.key) {
		return true;
	    }
	    else {
		return false;
	    }
	  }
	  else if (left.end < right.end) {
	    return true;
	  }
	  else {
	    return false;
	  }
	}
    });
    if (file_type == "MSS") {
	filename="getMssList.cache";
    }
    else if (file_type == "Web") {
	filename="getWebList.cache";
    }
    TempDir tdir;
    if (!tdir.create(metautils::directives.temp_path)) {
	metautils::log_error("create file_list_cache(): unable to create temporary directory (3)",caller,user);
    }
// create the directory tree in the temp directory
    std::stringstream oss,ess;
    if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/metadata",oss,ess) < 0) {
	metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory tree (3) - '"+ess.str()+"'",caller,user);
    }
    ofs.open((tdir.name()+"/metadata/"+filename).c_str());
    if (!ofs.is_open()) {
	metautils::log_error(THIS_FUNC+"(): unable to open temporary file for "+filename,caller,user);
    }
    ofs << num << std::endl;
    size_t num_missing_data_size=0;
    for (size_t n=0; n < num; n++) {
	std::string data_size;
	if (!array[n].data_size.empty()) {
	  data_size=strutils::ftos(std::stof(array[n].data_size)/1000000.,8,2);
	}
	else {
metautils::log_warning(THIS_FUNC+"() returned warning: empty data size for '"+array[n].key+"'",caller,user);
//	  ++num_missing_data_size;
	  data_size="";
	}
	strutils::trim(data_size);
	ofs << array[n].key << "<!>" << array[n].data_format << "<!>" << array[n].units << "<!>" << array[n].start << "<!>" << array[n].end << "<!>" << array[n].file_format << "<!>" << data_size << "<!>" << array[n].group_id << "<!>" << array[n].metafile_id;
	ofs << std::endl;
    }
    if (num_missing_data_size > 0) {
	metautils::log_warning(THIS_FUNC+"() returned warning: empty data sizes for "+strutils::itos(num_missing_data_size)+" files",caller,user);
    }
    ofs.close();
    if (unixutils::rdadata_sync(tdir.name(),"metadata/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
	metautils::log_warning(THIS_FUNC+"() couldn't sync '"+filename+"' - rdadata_sync error(s): '"+error+"'",caller,user);
    }
  }
  else if (tindex.empty()) {
    filename="";
    if (file_type == "MSS") {
	filename="getMssList.cache";
    }
    else if (file_type == "Web") {
	filename="getWebList.cache";
    }
    if (!filename.empty() && unixutils::rdadata_unsync("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/"+filename,metautils::directives.rdadata_home,error) < 0) {
	metautils::log_warning(THIS_FUNC+"() couldn't unsync '"+filename+"' - rdadata_unsync error(s): '"+error+"'",caller,user);
    }
  }
  server.disconnect();
  if (tindex.empty() && files_with_CMD_table.size() > 0) {
    create_non_cmd_file_list_cache(file_type,files_with_CMD_table,caller,user);
  }
  server.connect(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");
  server.update("dataset","meta_link = 'Y',version = version+1","dsid = 'ds"+metautils::args.dsnum+"'");
  server.disconnect();
}

std::string summarize_locations(std::string database)
{
  std::string dsnum2=strutils::substitute(metautils::args.dsnum,".","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
// get all location names that are NOT included
  MySQL::LocalQuery query("distinct gcmd_keyword",database+".ds"+dsnum2+"_location_names","include = 'N'");
  if (query.submit(server) < 0) {
    return query.error();
  }
  std::unordered_set<std::string> include_set,noinclude_set;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    auto location_name=row[0];
    strutils::replace_all(location_name,"'","\\'");
    MySQL::LocalQuery q("name","search.political_boundaries","name like '"+location_name+" >%'");
    if (q.submit(server) < 0) {
	return q.error();
    }
    if (q.num_rows() > 0) {
// location name is a parent - only add the children
	MySQL::Row r;
	while (q.fetch_row(r)) {
	  if (noinclude_set.find(r[0]) == noinclude_set.end()) {
	    noinclude_set.emplace(r[0]);
	  }
	}
    }
    else {
// location name is not a parent
	if (noinclude_set.find(row[0]) == noinclude_set.end()) {
	  noinclude_set.emplace(row[0]);
	}
    }
  }
// get the location names that ARE included
  query.set("distinct gcmd_keyword",database+".ds"+dsnum2+"_location_names","include = 'Y'");
  if (query.submit(server) < 0) {
    return query.error();
  }
  std::list<std::string> location_list;
  while (query.fetch_row(row)) {
    auto location_name=row[0];
    strutils::replace_all(location_name,"'","\\'");
    MySQL::LocalQuery q("name","search.political_boundaries","name like '"+location_name+" >%'");
    if (q.submit(server) < 0) {
	return q.error();
    }
    if (q.num_rows() > 0) {
// location name is a parent - only add the children if they are not in
//   noinclude_set
	MySQL::Row r;
	while (q.fetch_row(r)) {
	  if (noinclude_set.find(r[0]) == noinclude_set.end() && include_set.find(r[0]) == include_set.end()) {
	    include_set.emplace(r[0]);
	    location_list.emplace_back(r[0]);
	  }
	}
    }
    else {
// location name is not a parent - add the location
	if (include_set.find(row[0]) == include_set.end()) {
	  include_set.emplace(row[0]);
	  location_list.emplace_back(row[0]);
	}
    }
  }
  std::string error;
  server.command("lock tables search.locations write",error);
  server._delete("search.locations","dsid = '"+metautils::args.dsnum+"'");
  for (auto item : location_list) {
    strutils::replace_all(item,"'","\\'");
    if (server.insert("search.locations","'"+item+"','GCMD','Y','"+metautils::args.dsnum+"'") < 0) {
	return server.error();
    }
  }
  if (server.command("unlock tables",error) < 0) {
    return server.error();
  }
  server.disconnect();
  return "";
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
