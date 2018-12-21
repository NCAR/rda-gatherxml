#include <fstream>
#include <string>
#include <unistd.h>
#include <sys/stat.h>
#include <sstream>
#include <regex>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bfstream.hpp>
#include <grid.hpp>
#include <bufr.hpp>
#include <adpstream.hpp>
#include <satellite.hpp>
#include <marine.hpp>
#include <raob.hpp>
#include <surface.hpp>
#include <td32.hpp>
#include <cyclone.hpp>

namespace metautils {

namespace primaryMetadata {

bool prepare_file_for_metadata_scanning(TempFile& tfile,TempDir& tdir,std::list<std::string> *filelist,std::string& file_format,std::string& error)
{
  std::string filename,local_name;
  std::ifstream ifs;
  char line[256];
  std::string sline,sdum;
  icstream chkstream;
  struct stat64 statbuf;
  FILE *p,*ifs64,*ofs;
  MySQL::Server server;
  MySQL::LocalQuery query;
  MySQL::Row row;
  bool status;
  std::deque<std::string> sp,spf;
  int n,m;
  std::list<std::string> flist,*f;
  char buffer[32768];
  long long size=-1;
  std::string coscombine,fname,f1,web_home;
  std::stringstream oss,ess;
  TempDir temp_dir;
  bool compare_hpss_to_local=false;

  error="";
  if (!temp_dir.create(directives.temp_path)) {
    error="Error creating temporary directory";
    return false;
  }
  if (filelist != NULL) {
    filelist->clear();
  }
  server.connect(directives.database_server,directives.rdadb_username,directives.rdadb_password,"dssdb");
  if (!server) {
    error="Error connecting to MySQL server";
    return false;
  }
  if (std::regex_search(args.path,std::regex("^/FS/DSS")) || std::regex_search(args.path,std::regex("^/DSS"))) {
// HPSS file
    if (!args.override_primary_check) {
	query.set("dsid,property,file_format","mssfile","mssfile = '"+args.path+"/"+args.filename+"'");
	if (query.submit(server) < 0) {
	  error=query.error();
	  return false;
	}
	status=query.fetch_row(row);
	if (query.num_rows() == 0 || row[0] != "ds"+args.dsnum || row[1] != "P") {
	  error=args.path+"/"+args.filename+" is not a primary for this dataset";
	  return false;
	}
	if (status) {
	  file_format=row[2];
	}
    }
    if (args.local_name.empty()) {
	if (strutils::has_ending(strutils::to_lower(args.filename),".htar")) {
// HTAR file
 	  if (args.member_name.empty()) {
	    error="**a member name must be provided for an HTAR file";
	    return false;
 	  }
 	  else {
	    query.set("file_format","htarfile","dsid = 'ds"+args.dsnum+"' and hfile = '"+args.member_name+"'");
	    if (query.submit(server) < 0) {
		error=query.error();
		return false;
	    }
	    if (query.fetch_row(row)) {
		file_format=row[0];
	    }
	    else {
		error="**database error while trying to get file format of member file";
		return false;
	    }
	    if (stat64(args.member_name.c_str(),&statbuf) != 0) {
// not on disk, so extract from HTAR file
		if (getuid() == 60001) {
		  setreuid(8342,static_cast<uid_t>(-1));
		}
		auto hsi_command=unixutils::hsi_command();
		if (hsi_command.empty()) {
		  error="**htar not found";
		  return false;
		}
		strutils::replace_all(hsi_command,"hsi","htar");
		system((hsi_command+" -xqOf "+args.path+"/"+args.filename+" "+args.member_name+" 1> "+tfile.name()+" 2> /dev/null").c_str());
	    }
	    else {
// file is on disk already
		local_name=args.member_name;
		compare_hpss_to_local=true;
	    }
 	  }
	}
	else {
// regular HPSS file
	  if (strutils::has_ending(file_format,"HTAR")) {
	    error="**HTAR files must end with the '.htar' extension";
	    return false;
	  }
 	  if (!args.member_name.empty()) {
	    error="**a member file is not valid for a regular HPSS file";
	    return false;
 	  }
	  else {
	    if (stat64(args.filename.c_str(),&statbuf) != 0) {
// not on disk, so read from HPSS
		if (getuid() == 60001) {
		  setreuid(8342,static_cast<uid_t>(-1));
		}
		auto hsi_command=unixutils::hsi_command();
		if (hsi_command.empty()) {
		  error="**hsi not found";
		  return false;
		}
		system((hsi_command+" get "+tfile.name()+" : "+args.path+"/"+args.filename+" 1> /dev/null 2>&1").c_str());
		if (stat64(tfile.name().c_str(),&statbuf) != 0 || statbuf.st_size == 0) {
		  error="**HPSS file '"+args.path+"/"+args.filename+" does not exist or cannot be copied";
		  return false;
		}
	    }
	    else {
// file is on disk already
		local_name=args.filename;
		compare_hpss_to_local=true;
	    }
	  }
	}
    }
    else {
	local_name=args.local_name;
	if ( (stat64(local_name.c_str(),&statbuf)) != 0) {
	  auto web_path=strutils::substitute(local_name,directives.data_root,directives.data_root_alias);
	  if (web_path[0] != '/') {
	    web_path="/"+local_name;
	  }
	  auto remote_file=unixutils::remote_web_file("https://rda.ucar.edu"+web_path,temp_dir.name());
	  if (remote_file.empty()) {
	    error="local file '"+args.local_name+"' not found or unable to transfer";
	    return false;
	  }
	  else {
	    local_name=remote_file;
	  }
	}
	compare_hpss_to_local=true;
    }
    if (compare_hpss_to_local) {
// compare the file to the HPSS copy before continuing
	if (!args.override_primary_check) {
	  auto hsi_command=unixutils::hsi_command();
	  if (hsi_command.empty()) {
	    error="**hsi not found";
	    return false;
	  }
	  if (!args.member_name.empty()) {
	    strutils::replace_all(hsi_command,"hsi","htar");
	  }
	  auto num_retries=0;
	  while (num_retries < 5) {
	    if (!args.member_name.empty()) {
		p=popen((hsi_command+" -tf "+args.path+"/"+args.filename+" "+args.member_name+" 2>&1").c_str(),"r");
	    }
	    else {
		p=popen((hsi_command+" ls -l "+args.path+"/"+args.filename+" 2>&1").c_str(),"r");
	    }
	    while (fgets(line,256,p) != NULL) {
		sline=line;
		if ((strutils::contains(sline,"dss") && strutils::contains(sline,args.filename)) || (strutils::contains(sline,"HTAR:") && strutils::contains(sline,args.member_name))) {
		  sp=strutils::split(sline);
		  if (sp.size() < 5) {
		    error="unable to obtain 'hsi ls -l' info for "+args.path+"/"+args.filename;
		    return false;
		  }
		  if (!args.member_name.empty()) {
		    n=3;
		  }
		  else {
		    n=4;
		  }
		  size=std::stoll(sp[n]);
		  if (statbuf.st_size != size) {
		    error="local file size "+strutils::lltos(statbuf.st_size)+" does not match HPSS file size "+sp[n];
		    return false;
		  }
		}
	    }
	    pclose(p);
	    if (size < 0) {
		if (strutils::contains(sline,"HTAR SUCCESSFUL")) {
		  error="'"+args.member_name+"' is not a member of "+args.path+"/"+args.filename;
		  return false;
		}
		else {
		  sleep(15*pow(2.,num_retries));
		  num_retries++;
		}
	    }
	    else {
		num_retries=5;
	    }
	  }
	  if (size < 0) {
	    error="bad file size "+strutils::lltos(size)+" from 'hsi ls -l' info for "+args.path+"/"+args.filename;
	    return false;
	  }
	}
	if (unixutils::mysystem2("/bin/cp "+local_name+" "+tfile.name(),oss,ess) < 0) {
	  error=ess.str();
	  return false;
	}
    }
  }
  else if (std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
// Web file
    f1=metautils::relative_web_filename(args.path+"/"+args.filename);
    query.set("property,file_format","wfile","dsid = 'ds"+args.dsnum+"' and wfile = '"+f1+"'");
    if (query.submit(server) < 0) {
	error=query.error();
	return false;
    }
    status=query.fetch_row(row);
    if (!args.override_primary_check) {
	if (query.num_rows() == 0 || row[0] != "A") {
	  error=args.path+"/"+args.filename+" is not active for this dataset";
	  return false;
	}
    }
    if (status) {
	file_format=row[1];
    }
    local_name=args.path;
    strutils::replace_all(local_name,"https://rda.ucar.edu","");
    if (std::regex_search(local_name,std::regex("^"+directives.data_root_alias))) {
	local_name=directives.data_root+local_name.substr(directives.data_root_alias.length());
    }
    local_name=local_name+"/"+args.filename;
    if ( (stat64(local_name.c_str(),&statbuf)) != 0) {
	local_name=unixutils::remote_web_file(args.path+"/"+args.filename,temp_dir.name());
	if (local_name.empty()) {
	  error="Web file '"+args.path+"/"+args.filename+"' not found or unable to transfer";
	  return false;
	}
    }
    system(("cp "+local_name+" "+tfile.name()).c_str());
  }
  else {
    error="path of file '"+args.path+"' not recognized";
    return false;
  }
  filename=tfile.name();
  if (strutils::has_ending(file_format,"HTAR")) {
    strutils::chop(file_format,4);
    if (file_format.back() == '.') {
	strutils::chop(file_format);
    }
  }
  if (file_format.empty()) {
    if (args.data_format == "grib" ||  args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
// check to see if file is COS-blocked
	if (!chkstream.open(filename.c_str())) {
	  error="unable to open "+filename;
	  return false;
	}
	if (chkstream.ignore() > 0) {
	  system((directives.dss_bindir+"/cosconvert -b "+filename+" 1> /dev/null").c_str());
	}
	chkstream.close();
	while (expand_file(tdir.name(),filename,&file_format));
    }
    else {
	while (expand_file(tdir.name(),filename,&file_format));
    }
    if (filelist != NULL) {
	filelist->emplace_back(filename);
    }
  }
  else {
    spf=strutils::split(file_format,".");
    if (spf.size() > 3) {
	error="archive file format is too complicated";
	return false;
    }
    for (n=spf.size()-1; n >= 0; n--) {
	if (spf[n] == "BI") {
	  if (n == 0 || spf[n-1] != "LVBS") {
	    if (args.data_format == "grib" || args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
		unixutils::mysystem2(directives.dss_bindir+"/cosconvert -b "+filename+" "+filename+".bi",oss,ess);
		if (!ess.str().empty()) {
		  error=ess.str();
		  return false;
		}
		else {
		  system(("mv "+filename+".bi "+filename).c_str());
		}
	    }
	  }
	}
	else if (spf[n] == "CH" || spf[n] == "C1") {
	  if (args.data_format == "grib" || args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
	    system((directives.dss_bindir+"/cosconvert -c "+filename+" 1> /dev/null").c_str());
	  }
	}
	else if (spf[n] == "GZ" || spf[n] == "BZ2") {
	  std::string command,ext;
	  if (spf[n] == "GZ") {
	    command="gunzip";
	    ext="gz";
	  }
	  else if (spf[n] == "BZ2") {
	    command="bunzip2";
	    ext="bz2";
	  }
	  if (spf[n] == spf.back()) {
	    system(("mv "+filename+" "+filename+"."+ext+"; "+command+" -f "+filename+"."+ext).c_str());
	  }
	  else if (spf[n] == spf.front()) {
	    if ((args.data_format == "cxml" || args.data_format == "tcvitals" || strutils::contains(args.data_format,"netcdf") || strutils::contains(args.data_format,"hdf") || strutils::contains(args.data_format,"bufr")) && filelist != NULL) {
		for (const auto& file : *filelist) {
		  flist.emplace_back(file);
		}
		filelist->clear();
	    }
	    ofs=fopen64(filename.c_str(),"w");
	    for (const auto& file : flist) {
		system((command+" -f "+file).c_str());
		sdum=file;
		strutils::replace_all(sdum,"."+ext,"");
		if (args.data_format != "cxml" && args.data_format != "tcvitals" && !strutils::contains(args.data_format,"netcdf") && !strutils::contains(args.data_format,"hdf")) {
		  if ( (ifs64=fopen64(sdum.c_str(),"r")) == NULL) {
		    error="error while combining ."+ext+" files - could not open "+sdum;
		    return false;
		  }
		  while ( (m=fread(buffer,1,32768,ifs64)) > 0) {
		    fwrite(buffer,1,m,ofs);
		  }
		  fclose(ifs64);
		}
		else {
		  if (filelist != nullptr) {
		    filelist->emplace_back(sdum);
		  }
		  else {
		    error="non-null filelist must be provided for format "+args.data_format;
		    return false;
		  }
		}
	    }
	    fclose(ofs);
	  }
	  else {
	    error="archive file format is too complicated";
	    return false;
	  }
	}
	else if (spf[n] == "TAR" || spf[n] == "TGZ") {
	  if (n == 0 && args.data_format != "cxml" && args.data_format != "tcvitals" && !strutils::contains(args.data_format,"netcdf") && !std::regex_search(args.data_format,std::regex("nc$")) && !strutils::contains(args.data_format,"hdf") && args.data_format != "mcidas" && args.data_format != "uadb") {
	    if (std::regex_search(args.data_format,std::regex("^grib"))) {
		expand_file(tdir.name(),filename,NULL);
	    }
	  }
	  else if (args.data_format == "cxml" || args.data_format == "tcvitals" || strutils::contains(args.data_format,"netcdf") || std::regex_search(args.data_format,std::regex("nc$")) || strutils::contains(args.data_format,"hdf") || args.data_format == "mcidas" || args.data_format == "uadb" || strutils::contains(args.data_format,"bufr") || n == static_cast<int>(spf.size()-1)) {
	    if (args.data_format == "cxml" || args.data_format == "tcvitals" || strutils::contains(args.data_format,"netcdf") || std::regex_search(args.data_format,std::regex("nc$")) || strutils::contains(args.data_format,"hdf") || args.data_format == "mcidas" || args.data_format == "uadb") {
		if (filelist != nullptr) {
		  f=filelist;
		}
		else {
		  error="non-null filelist must be provided for format "+args.data_format;
		  return false;
		}
	    }
	    else {
		f=&flist;
	    }
	    p=popen(("tar tvf "+filename+" 2>&1").c_str(),"r");
	    while (fgets(line,256,p) != NULL) {
		sline=line;
		if (strutils::contains(sline,"checksum error")) {
		  error="tar extraction failed - is it really a tar file?";
		  return false;
		}
		else if (sline[0] == '-') {
		  strutils::chop(sline);
		  sp=strutils::split(sline,"");
		  f->emplace_back(tdir.name()+"/"+sp[sp.size()-1]);
		}
	    }
	    pclose(p);
	    sdum=filename.substr(filename.rfind("/")+1);
	    system(("mv "+filename+" "+tdir.name()+"/; cd "+tdir.name()+"; tar xvf "+sdum+" 1> /dev/null 2>&1").c_str());
	  }
	  else {
	    error="archive file format is too complicated";
	    return false;
	  }
	}
	else if (spf[n] == "VBS") {
	  if (static_cast<int>(spf.size()) >= (n+2) && spf[n+1] == "BI") {
	    system((directives.dss_bindir+"/cosconvert -v "+filename+" "+filename+".vbs 1> /dev/null;mv "+filename+".vbs "+filename).c_str());
	  }
	}
	else if (spf[n] == "LVBS") {
	  if (static_cast<int>(spf.size()) >= (n+2) && spf[n+1] == "BI") {
	    system((directives.dss_root+"/bin/cossplit -p "+filename+" "+filename).c_str());
	    m=2;
	    coscombine=directives.dss_root+"/bin/coscombine noeof";
	    fname=filename+".f"+strutils::ftos(m,3,0,'0');
	    while (stat64(fname.c_str(),&statbuf) == 0) {
		coscombine+=" "+fname;
		m+=3;
		fname=filename+".f"+strutils::ftos(m,3,0,'0');
	    }
	    coscombine+=" "+filename+" 1> /dev/null";
	    system(coscombine.c_str());
	    system(("rm -f "+filename+".f*").c_str());
	    system((directives.dss_bindir+"/cosconvert -v "+filename+" "+filename+".vbs 1> /dev/null;mv "+filename+".vbs "+filename).c_str());
	  }
	}
	else if (spf[n] == "Z") {
	  if (n == static_cast<int>(spf.size()-1))
	    system(("mv "+filename+" "+filename+".Z; uncompress "+filename).c_str());
	  else if (n == 0) {
	    ofs=fopen64(filename.c_str(),"w");
	    for (auto& file : flist) {
		system(("uncompress "+file).c_str());
		sdum=file;
		strutils::replace_all(sdum,".Z","");
		if (!strutils::contains(args.data_format,"netcdf") && !strutils::contains(args.data_format,"hdf")) {
		  ifs.open(sdum.c_str());
		  if (!ifs) {
		    error="error while combining .Z files";
		    return false;
		  }
		  while (!ifs.eof()) {
		    ifs.read(buffer,32768);
		    m=ifs.gcount();
		    fwrite(buffer,1,m,ofs);
		  }
		  ifs.close();
		}
		else {
		  if (filelist != NULL) {
		    filelist->emplace_back(sdum);
		  }
		  else {
		    error="non-null filelist must be provided for format "+args.data_format;
		    return false;
		  }
		}
	    }
	    fclose(ofs);
	  }
	  else {
	    error="archive file format is too complicated";
	    return false;
	  }
	}
	else if (spf[n] == "RPTOUT") {
// no action needed for these archive formats
	}
	else {
	  error="don't recognize '"+spf[n]+"' in archive format field for this HPSS file";
	  return false;
	}
    }
  }
  server.disconnect();
  return true;
}

bool open_file_for_metadata_scanning(void *istream,std::string filename,std::string& error)
{
  if (args.data_format == "on29" || args.data_format == "on124") {
    return (reinterpret_cast<InputADPStream *>(istream))->open(filename);
  }
  else if (strutils::contains(args.data_format,"bufr")) {
    return (reinterpret_cast<InputBUFRStream *>(istream))->open(filename);
  }
  else if (args.data_format == "grib" || args.data_format == "grib2") {
    return (reinterpret_cast<InputGRIBStream *>(istream))->open(filename);
  }
  else if (args.data_format == "jraieeemm") {
    return (reinterpret_cast<InputJRAIEEEMMGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "ll") {
    return (reinterpret_cast<InputLatLonGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "mcidas") {
    return (reinterpret_cast<InputMcIDASStream *>(istream))->open(filename.c_str());
  }
  else if (args.data_format == "navy") {
    return (reinterpret_cast<InputNavyGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "noaapod") {
    return (reinterpret_cast<InputNOAAPolarOrbiterDataStream *>(istream))->open(filename.c_str());
  }
  else if (args.data_format == "oct") {
    return (reinterpret_cast<InputOctagonalGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "tropical") {
    return (reinterpret_cast<InputTropicalGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "on84") {
    return (reinterpret_cast<InputON84GridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "slp") {
    return (reinterpret_cast<InputSLPGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "tsr") {
    return (reinterpret_cast<InputTsraobStream *>(istream))->open(filename);
  }
  else if (args.data_format == "ussrslp") {
    return (reinterpret_cast<InputUSSRSLPGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "cpcsumm") {
    return (reinterpret_cast<InputCPCSummaryObservationStream *>(istream))->open(filename);
  }
  else if (args.data_format == "imma") {
    return (reinterpret_cast<InputIMMAObservationStream *>(istream))->open(filename);
  }
  else if (args.data_format == "isd") {
    return (reinterpret_cast<InputISDObservationStream *>(istream))->open(filename);
  }
  else if (args.data_format == "nodcbt") {
    return (reinterpret_cast<InputNODCBTObservationStream *>(istream))->open(filename);
  }
  else if (std::regex_search(args.data_format,std::regex("^td32"))) {
    return (reinterpret_cast<InputTD32Stream *>(istream))->open(filename);
  }
  else if (args.data_format == "wmssc") {
    return (reinterpret_cast<InputWMSSCObservationStream *>(istream))->open(filename);
  }
  else if (args.data_format == "hurdat") {
    return (reinterpret_cast<InputHURDATCycloneStream *>(istream))->open(filename);
  }
  else if (args.data_format == "tcvitals") {
    return (reinterpret_cast<InputTCVitalsCycloneStream *>(istream))->open(filename);
  }
  else if (args.data_format == "cgcm1") {
    return (reinterpret_cast<InputCGCM1GridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "cmorph025") {
    return (reinterpret_cast<InputCMORPH025GridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "cmorph8km") {
    return (reinterpret_cast<InputCMORPH8kmGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "gpcp") {
    return (reinterpret_cast<InputGPCPGridStream *>(istream))->open(filename);
  }
  else if (args.data_format == "uadb") {
    return (reinterpret_cast<InputUADBRaobStream *>(istream))->open(filename);
  }
  else {
    error=args.data_format+"-formatted files not recognized";
    return false;
  }
}

} // end namespace primaryMetadata

} // end namespace metautils
