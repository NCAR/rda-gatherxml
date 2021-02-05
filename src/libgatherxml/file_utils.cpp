#include <fstream>
#include <string>
#include <unistd.h>
#include <sys/stat.h>
#include <sstream>
#include <regex>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>

namespace metautils {

namespace primaryMetadata {

bool prepare_file_for_metadata_scanning(TempFile& tfile,TempDir& tdir,std::list<std::string> *filelist,std::string& file_format,std::string& error)
{
  error="";
  TempDir temp_dir;
  if (!temp_dir.create(directives.temp_path)) {
    error="Error creating temporary directory";
    return false;
  }
  if (filelist != NULL) {
    filelist->clear();
  }
  MySQL::Server server(directives.database_server,directives.rdadb_username,directives.rdadb_password,"dssdb");
  if (!server) {
    error="Error connecting to MySQL server";
    return false;
  }
  if (std::regex_search(args.path,std::regex("^/FS/DECS"))) {
// HPSS file
error="Terminating - HPSS files are no longer supported";
return false;
  }
  else if (std::regex_search(args.path,std::regex("^https://rda.ucar.edu"))) {
// Web file
    auto wfile=metautils::relative_web_filename(args.path+"/"+args.filename);
    MySQL::LocalQuery query("property,file_format","wfile","dsid = 'ds"+args.dsnum+"' and wfile = '"+wfile+"'");
    if (query.submit(server) < 0) {
	error=query.error();
	return false;
    }
    MySQL::Row row;
    auto fetched_row=query.fetch_row(row);
    if (!args.override_primary_check) {
	if (query.num_rows() == 0 || row[0] != "A") {
	  error="Terminating - "+args.path+"/"+args.filename+" is not active for this dataset";
	  return false;
	}
    }
    if (fetched_row) {
	file_format=row[1];
    }
    std::string local_name;
    if (!args.override_primary_check) {
	local_name=args.path;
	strutils::replace_all(local_name,"https://rda.ucar.edu","");
	if (std::regex_search(local_name,std::regex("^"+directives.data_root_alias))) {
	  local_name=directives.data_root+local_name.substr(directives.data_root_alias.length());
	}
	local_name=local_name+"/"+args.filename;
    }
    else {
	local_name=args.filename;
    }
    struct stat64 statbuf;
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
  auto filename=tfile.name();
  if (strutils::has_ending(file_format,"HTAR")) {
    strutils::chop(file_format,4);
    if (file_format.back() == '.') {
	strutils::chop(file_format);
    }
  }
  if (file_format.empty()) {
    if (args.data_format == "grib" ||  args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
// check to see if file is COS-blocked
	std::stringstream oss,ess;
	unixutils::mysystem2(directives.decs_bindir+"/cosfile "+filename,oss,ess);
	if (ess.str().empty()) {
	  system((directives.decs_bindir+"/cosconvert -b "+filename+" 1> /dev/null").c_str());
	}
	else if (!std::regex_search(ess.str(),std::regex("error on record 1"))) {
	  error="unable to open '"+filename+"'; error: "+ess.str();
	  return false;
	}
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
    auto ffparts=strutils::split(file_format,".");
    if (ffparts.size() > 3) {
	error="archive file format is too complicated";
	return false;
    }
    std::list<std::string> flist;
    for (int n=ffparts.size()-1; n >= 0; n--) {
	if (ffparts[n] == "BI") {
	  if (n == 0 || ffparts[n-1] != "LVBS") {
	    if (args.data_format == "grib" || args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
		std::stringstream oss,ess;
		unixutils::mysystem2(directives.decs_bindir+"/cosconvert -b "+filename+" "+filename+".bi",oss,ess);
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
	else if (ffparts[n] == "CH" || ffparts[n] == "C1") {
	  if (args.data_format == "grib" || args.data_format == "grib2" || strutils::contains(args.data_format,"bufr") || args.data_format == "mcidas") {
	    system((directives.decs_bindir+"/cosconvert -c "+filename+" 1> /dev/null").c_str());
	  }
	}
	else if (ffparts[n] == "GZ" || ffparts[n] == "BZ2") {
	  std::string command,ext;
	  if (ffparts[n] == "GZ") {
	    command="gunzip";
	    ext="gz";
	  }
	  else if (ffparts[n] == "BZ2") {
	    command="bunzip2";
	    ext="bz2";
	  }
	  if (ffparts[n] == ffparts.back()) {
	    system(("mv "+filename+" "+filename+"."+ext+"; "+command+" -f "+filename+"."+ext).c_str());
	  }
	  else if (ffparts[n] == ffparts.front()) {
	    if ((args.data_format == "cxml" || args.data_format == "tcvitals" || strutils::contains(args.data_format,"netcdf") || strutils::contains(args.data_format,"hdf") || strutils::contains(args.data_format,"bufr")) && filelist != NULL) {
		for (const auto& file : *filelist) {
		  flist.emplace_back(file);
		}
		filelist->clear();
	    }
	    auto ofs=fopen64(filename.c_str(),"w");
	    for (auto file : flist) {
		system((command+" -f "+file).c_str());
		strutils::replace_all(file,"."+ext,"");
		if (args.data_format != "cxml" && args.data_format != "tcvitals" && !strutils::contains(args.data_format,"netcdf") && !strutils::contains(args.data_format,"hdf")) {
		  auto ifs64=fopen64(file.c_str(),"r");
		  if (ifs64 == nullptr) {
		    error="error while combining ."+ext+" files - could not open "+file;
		    return false;
		  }
		  char buffer[32768];
		  int num_bytes;
		  while ( (num_bytes=fread(buffer,1,32768,ifs64)) > 0) {
		    fwrite(buffer,1,num_bytes,ofs);
		  }
		  fclose(ifs64);
		}
		else {
		  if (filelist != nullptr) {
		    filelist->emplace_back(file);
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
	else if (ffparts[n] == "TAR" || ffparts[n] == "TGZ") {
	  if (n == 0 && args.data_format != "cxml" && args.data_format != "tcvitals" && !strutils::contains(args.data_format,"netcdf") && !std::regex_search(args.data_format,std::regex("nc$")) && !strutils::contains(args.data_format,"hdf") && args.data_format != "mcidas" && args.data_format != "uadb") {
	    if (std::regex_search(args.data_format,std::regex("^grib"))) {
		expand_file(tdir.name(),filename,NULL);
	    }
	  }
	  else if (args.data_format == "cxml" || args.data_format == "tcvitals" || strutils::contains(args.data_format,"netcdf") || std::regex_search(args.data_format,std::regex("nc$")) || strutils::contains(args.data_format,"hdf") || args.data_format == "mcidas" || args.data_format == "uadb" || strutils::contains(args.data_format,"bufr") || n == static_cast<int>(ffparts.size()-1)) {
	    std::list<std::string> *f;
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
	    auto p=popen(("tar tvf "+filename+" 2>&1").c_str(),"r");
	    char line[256];
	    while (fgets(line,256,p) != NULL) {
		if (strutils::contains(line,"checksum error")) {
		  error="tar extraction failed - is it really a tar file?";
		  return false;
		}
		else if (line[0] == '-') {
		  std::string sline=line;
		  strutils::chop(sline);
		  auto line_parts=strutils::split(sline,"");
		  f->emplace_back(tdir.name()+"/"+line_parts.back());
		}
	    }
	    pclose(p);
	    auto tar_file=filename.substr(filename.rfind("/")+1);
	    system(("mv "+filename+" "+tdir.name()+"/; cd "+tdir.name()+"; tar xvf "+tar_file+" 1> /dev/null 2>&1").c_str());
	  }
	  else {
	    error="archive file format is too complicated";
	    return false;
	  }
	}
	else if (ffparts[n] == "VBS") {
	  if (static_cast<int>(ffparts.size()) >= (n+2) && ffparts[n+1] == "BI") {
	    system((directives.decs_bindir+"/cosconvert -v "+filename+" "+filename+".vbs 1> /dev/null;mv "+filename+".vbs "+filename).c_str());
	  }
	}
	else if (ffparts[n] == "LVBS") {
	  if (static_cast<int>(ffparts.size()) >= (n+2) && ffparts[n+1] == "BI") {
	    system((directives.decs_root+"/bin/cossplit -p "+filename+" "+filename).c_str());
	    auto file_num=2;
	    std::string coscombine=directives.decs_root+"/bin/coscombine noeof";
	    auto fname=filename+".f"+strutils::ftos(file_num,3,0,'0');
	    struct stat64 statbuf;
	    while (stat64(fname.c_str(),&statbuf) == 0) {
		coscombine+=" "+fname;
		file_num+=3;
		fname=filename+".f"+strutils::ftos(file_num,3,0,'0');
	    }
	    coscombine+=" "+filename+" 1> /dev/null";
	    system(coscombine.c_str());
	    system(("rm -f "+filename+".f*").c_str());
	    system((directives.decs_bindir+"/cosconvert -v "+filename+" "+filename+".vbs 1> /dev/null;mv "+filename+".vbs "+filename).c_str());
	  }
	}
	else if (ffparts[n] == "Z") {
	  if (n == static_cast<int>(ffparts.size()-1))
	    system(("mv "+filename+" "+filename+".Z; uncompress "+filename).c_str());
	  else if (n == 0) {
	    auto ofs=fopen64(filename.c_str(),"w");
	    for (auto file : flist) {
		system(("uncompress "+file).c_str());
		strutils::replace_all(file,".Z","");
		if (!strutils::contains(args.data_format,"netcdf") && !strutils::contains(args.data_format,"hdf")) {
		  std::ifstream ifs(file.c_str());
		  if (!ifs) {
		    error="error while combining .Z files";
		    return false;
		  }
		  while (!ifs.eof()) {
		    char buffer[32768];
		    ifs.read(buffer,32768);
		    auto num_bytes=ifs.gcount();
		    fwrite(buffer,1,num_bytes,ofs);
		  }
		  ifs.close();
		}
		else {
		  if (filelist != NULL) {
		    filelist->emplace_back(file);
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
	else if (ffparts[n] == "RPTOUT") {
// no action needed for these archive formats
	}
	else {
	  error="don't recognize '"+ffparts[n]+"' in archive format field for this HPSS file";
	  return false;
	}
    }
  }
  server.disconnect();
  return true;
}

} // end namespace primaryMetadata

} // end namespace metautils
