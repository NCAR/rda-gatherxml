#include <fstream>
#include <sstream>
#include <regex>
#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bitmap.hpp>
#include <bsort.hpp>
#include <MySQL.hpp>
#include <tempfile.hpp>

namespace gatherxml {

namespace fileInventory {

void close(std::string filename,TempDir **tdir,std::ofstream& ofs,std::string cmd_type,bool insert_into_db,bool create_cache,std::string caller,std::string user)
{
  if (!ofs.is_open()) {
    return;
  }
  ofs.close();
  ofs.clear();
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  server.update("W"+cmd_type+".ds"+strutils::substitute(metautils::args.dsnum,".","")+"_webfiles2","inv = 'Y'","webID = '"+strutils::substitute(filename,"%","/")+"'");
  if (metautils::args.inventory_only) {
    metautils::args.filename=filename;
  }
  server.disconnect();
  std::stringstream output,error;
  if (insert_into_db) {
    auto iinv_command=metautils::directives.local_root+"/bin/iinv";
    if (!create_cache) {
	iinv_command+=" -C";
    }
    iinv_command+=" -d "+metautils::args.dsnum+" -t "+(*tdir)->name()+" -f "+filename+"."+cmd_type+"_inv";
    unixutils::mysystem2(iinv_command,output,error);
    if (!error.str().empty()) {
	metautils::log_warning("close(): '"+error.str()+"' while running iinv",caller,user);
	(*tdir)->set_keep();
    }
  }
  if (*tdir != nullptr) {
    delete *tdir;
  }
}

void open(std::string& filename,TempDir **tdir,std::ofstream& ofs,std::string cmd_type,std::string caller,std::string user)
{
  if (ofs.is_open()) {
    return;
  }
  if (!std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
    return;
  }
  filename=metautils::relative_web_filename(metautils::args.path+"/"+metautils::args.filename);
  strutils::replace_all(filename,"/","%");
  if (*tdir == nullptr) {
    *tdir=new TempDir();
    if (!(*tdir)->create(metautils::directives.temp_path)) {
	metautils::log_error("open(): unable to create temporary directory",caller,user);
    }
  }
  ofs.open((*tdir)->name()+"/"+filename+"."+cmd_type+"_inv");
  if (!ofs.is_open()) {
    metautils::log_error("open(): couldn't open the inventory output file",caller,user);
  }
}

} // end namespace gatherxml::fileInventory

void copy_ancillary_files(std::string file_type,std::string metadata_file,std::string file_type_name,std::string caller,std::string user)
{
  const std::string THIS_FUNC=__func__;
  std::stringstream output,error;
  if (unixutils::mysystem2("/bin/tcsh -c \"curl -s --data 'authKey=qGNlKijgo9DJ7MN&cmd=listfiles&value=/SERVER_ROOT/web/datasets/ds"+metautils::args.dsnum+"/metadata/fmd&pattern="+metadata_file+"' http://rda.ucar.edu/cgi-bin/dss/remoteRDAServerUtils\"",output,error) < 0) {
    metautils::log_warning(THIS_FUNC+"(): unable to copy "+file_type+" files - error: '"+error.str()+"'",caller,user);
  }
  else {
    auto oparts=strutils::split(output.str(),"\n");
    auto path_parts=strutils::split(oparts[0],"/");
    std::string __host__="/"+path_parts[1];
    TempFile itemp(metautils::directives.temp_path),otemp(metautils::directives.temp_path);
    TempDir tdir;
    if (!tdir.create(metautils::directives.temp_path)) {
	metautils::log_error(THIS_FUNC+"(): unable to create temporary directory",caller,user);
    }

// create the directory tree in the temp directory
    std::string metadata_path="metadata/wfmd";
    std::stringstream output,error;
    if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/"+metadata_path,output,error) < 0) {
	metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory - '"+error.str()+"'",caller,user);
    }
    for (size_t n=0; n < oparts.size(); ++n) {
	if (strutils::has_ending(oparts[n],".xml")) {
	  auto hs=oparts[n];
	  strutils::replace_all(hs,__host__,"/__HOST__");
	  unixutils::rdadata_sync_from(hs,itemp.name(),metautils::directives.rdadata_home,error);
	  std::ifstream ifs(itemp.name().c_str());
	  if (ifs.is_open()) {
	    auto fparts=strutils::split(oparts[n],file_type);
	    std::ofstream ofs((tdir.name()+"/"+metadata_path+"/"+file_type_name+"."+file_type+fparts[1]).c_str());
	    char line[32768];
	    ifs.getline(line,32768);
	    while (!ifs.eof()) {
		std::string line_s=line;
		if (strutils::contains(line_s,"parent=")) {
		  strutils::replace_all(line_s,metadata_file,file_type_name+"."+file_type);
		}
		ofs << line_s << std::endl;
		ifs.getline(line,32768);
	    }
	    ifs.close();
	    ofs.close();
	    ofs.clear();
	  }
	  else {
	    metautils::log_warning(THIS_FUNC+"(): unable to copy "+file_type+" file '"+oparts[n]+"'",caller,user);
	  }
	}
    }
    std::string herror;
    if (unixutils::rdadata_sync(tdir.name(),metadata_path+"/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,herror) < 0) {
	metautils::log_warning(THIS_FUNC+"(): unable to sync - rdadata_sync error(s): '"+herror+"'",caller,user);
    }
  }
}

namespace markup {

void write_finalize(std::string filename,std::string ext,std::string tdir_name,std::ofstream& ofs,std::string caller,std::string user)
{
  const std::string THIS_FUNC=__func__;
  ofs.close();
  if (ext != "GrML") {
    std::string metadata_path="metadata/wfmd";
    system(("gzip "+tdir_name+"/"+metadata_path+"/"+filename+"."+ext).c_str());
    std::string error;
    if (unixutils::rdadata_sync(tdir_name,metadata_path+"/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,error) < 0) {
	metautils::log_warning(THIS_FUNC+"(): unable to sync '"+filename+"."+ext+"' - rdadata_sync error(s): '"+error+"'",caller,user);
    }
  }
  metautils::args.filename=filename;
}

void write_initialize(std::string& filename,std::string ext,std::string tdir_name,std::ofstream& ofs,std::string caller,std::string user)
{
  const std::string THIS_FUNC=__func__;
  filename=metautils::args.path+"/"+metautils::args.filename;
  filename=metautils::relative_web_filename(filename);
  strutils::replace_all(filename,"/","%");

  if (ext == "GrML") {
// gridded content metadata is not stored on the server; it goes into a temp
//   directory for 'scm' and then gets deleted

    ofs.open(tdir_name+"/"+filename+"."+ext);
  }
  else {
// this section is for metadata that gets stored on the server

    std::string metadata_path="metadata/wfmd";

// create the directory tree in the temp directory
    std::stringstream oss,ess;
    if (unixutils::mysystem2("/bin/mkdir -p "+tdir_name+"/"+metadata_path+"/",oss,ess) < 0) {
	metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory",caller,user);;
    }

    ofs.open(tdir_name+"/"+metadata_path+"/"+filename+"."+ext);
  }
  if (!ofs.is_open()) {
    metautils::log_error(THIS_FUNC+"(): unable to open file for output",caller,user);
  }
  ofs << "<?xml version=\"1.0\" ?>" << std::endl;
}

namespace FixML {

void copy(std::string metadata_file,std::string URL,std::string caller,std::string user)
{
  auto FixML_name=metautils::relative_web_filename(URL);
  strutils::replace_all(FixML_name,"/","%");
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error("copy(): unable to create temporary directory",caller,user);
  }

// create the directory tree in the temp directory
  std::string metadata_path="metadata/wfmd";
  std::stringstream output,error;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/"+metadata_path,output,error) < 0) {
    metautils::log_error("copy(): unable to create a temporary directory - '"+error.str()+"'",caller,user);
  }
  TempFile itemp(metautils::directives.temp_path);
  unixutils::rdadata_sync_from("/__HOST_/web/datasets/ds"+metautils::args.dsnum+"/metadata/fmd/"+metadata_file,itemp.name(),metautils::directives.rdadata_home,error);
  std::ifstream ifs(itemp.name().c_str());
  if (!ifs.is_open()) {
    metautils::log_error("copy(): unable to open input file",caller,user);
  }
  std::ofstream ofs((tdir.name()+"/"+metadata_path+"/"+FixML_name+".FixML").c_str());
  if (!ofs.is_open()) {
    metautils::log_error("copy(): unable to open output file",caller,user);
  }
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string line_s=line;
    if (strutils::contains(line_s,"uri=")) {
	line_s=line_s.substr(line_s.find(" format"));
	line_s="      uri=\""+URL+"\""+line_s;
    }
    else if (strutils::contains(line_s,"ref=")) {
	strutils::replace_all(line_s,metadata_file,FixML_name+".FixML");
    }
    ofs << line_s << std::endl;
    ifs.getline(line,32768);
  }
  ifs.close();
  ofs.close();
  std::string herror;
  if (unixutils::rdadata_sync(tdir.name(),metadata_path+"/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,herror) < 0) {
    metautils::log_warning("copy(): unable to sync '"+FixML_name+".FixML' - rdadata_sync error(s): '"+herror+"'",caller,user);
  }
  copy_ancillary_files("FixML",metadata_file,FixML_name,caller,user);
  metautils::args.filename=FixML_name;
}

void write(my::map<FeatureEntry>& feature_table,my::map<StageEntry>& stage_table,std::string caller,std::string user)
{
  const std::string THIS_FUNC=std::string("FixML::")+__func__;
  std::ofstream ofs,ofs2;
  TempFile tfile2(metautils::directives.temp_path);
  std::string filename,sdum,output,error;
  FeatureEntry fe;
  StageEntry se;
  ClassificationEntry ce;
  size_t n,m;
  MySQL::Server server;

  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"(): unable to create temporary directory",caller,user);
  }
  write_initialize(filename,"FixML",tdir.name(),ofs,caller,user);
  ofs << "<FixML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << std::endl;
  ofs << "                           https://rda.ucar.edu/schemas/FixML.xsd\"" << std::endl;
  ofs << "       uri=\"" << metautils::args.path << "/" << metautils::args.filename << "\" format=\"";
  if (metautils::args.data_format == "hurdat") {
    ofs << "HURDAT";
  }
  else if (metautils::args.data_format == "cxml") {
    ofs << "CXML";
  }
  else if (metautils::args.data_format == "tcvitals") {
    ofs << "proprietary_ASCII";
  }
  ofs << "\">" << std::endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
  ofs.setf(std::ios::fixed);
  ofs.precision(1);
  for (const auto& key : feature_table.keys()) {
    feature_table.found(key,fe);
    ofs << "  <feature ID=\"" << fe.key;
    if (!fe.data->alt_id.empty()) {
	ofs << "-" << fe.data->alt_id;
    }
    ofs << "\">" << std::endl;
    for (const auto& class_ : fe.data->classification_list) {
	ce=class_;
	ofs << "    <classification stage=\"" << ce.key << "\" nfixes=\"" << ce.nfixes << "\"";
	if (!ce.src.empty()) {
	  ofs << " source=\"" << ce.src << "\"";
	}
	ofs << ">" << std::endl;
	ofs << "      <start dateTime=\"" << ce.start_datetime.to_string("%Y-%m-%d %H:%MM %Z") << "\" latitude=\"" << fabs(ce.start_lat);
	if (ce.start_lat >= 0.) {
	  ofs << "N";
	}
	else {
	  ofs << "S";
	}
	ofs << "\" longitude=\"" << fabs(ce.start_lon);
	if (ce.start_lon < 0.) {
	  ofs << "W";
	}
	else {
	  ofs << "E";
	}
	ofs << "\" />" << std::endl;
	ofs << "      <end dateTime=\"" << ce.end_datetime.to_string("%Y-%m-%d %H:%MM %Z") << "\" latitude=\"" << fabs(ce.end_lat);
	if (ce.end_lat >= 0.) {
	  ofs << "N";
	}
	else {
	  ofs << "S";
	}
	ofs << "\" longitude=\"" << fabs(ce.end_lon);
	if (ce.end_lon < 0.) {
	  ofs << "W";
	}
	else {
	  ofs << "E";
	}
	ofs << "\" />" << std::endl;
	ofs << "      <centralPressure units=\"" << ce.pres_units << "\" min=\"" << static_cast<int>(ce.min_pres) << "\" max=\"" << static_cast<int>(ce.max_pres) << "\" />" << std::endl;
	ofs << "      <windSpeed units=\"" << ce.wind_units << "\" min=\"" << static_cast<int>(ce.min_speed) << "\" max=\"" << static_cast<int>(ce.max_speed) << "\" />" << std::endl;
	ofs << "      <boundingBox southWest=\"" << ce.min_lat << "," << ce.min_lon << "\" northEast=\"" << ce.max_lat << "," << ce.max_lon << "\" />" << std::endl;
	ofs << "    </classification>" << std::endl;
    }
    ofs << "  </feature>" << std::endl;
  }
  ofs << "</FixML>" << std::endl;
  for (const auto& key : stage_table.keys()) {
    ofs2.open((tdir.name()+"/metadata/wfmd/"+filename+".FixML."+key+".locations.xml").c_str());
    ofs2 << "<?xml version=\"1.0\" ?>" << std::endl;
    ofs2 << "<locations parent=\"" << filename << ".FixML\" stage=\"" << key << "\">" << std::endl;
    stage_table.found(key,se);
    if (se.data->boxflags.spole == 1) {
	ofs2 << "  <box1d row=\"0\" bitmap=\"0\" />" << std::endl;
    }
    for (n=0; n < 180; ++n) {
	if (se.data->boxflags.flags[n][360] == 1) {
	  ofs2 << "  <box1d row=\"" << n+1 << "\" bitmap=\"";
	  for (m=0; m < 360; ++m) {
	    ofs2 << static_cast<int>(se.data->boxflags.flags[n][m]);
	  }
	  ofs2 << "\" />" << std::endl;
	}
    }
    if (se.data->boxflags.npole == 1) {
	ofs2 << "  <box1d row=\"181\" bitmap=\"0\" />" << std::endl;
    }
    ofs2 << "</locations>" << std::endl;
    ofs2.close();
  }
  write_finalize(filename,"FixML",tdir.name(),ofs,caller,user);
}

} // end namespace gatherxml::markup::FixML

namespace GrML {

void write_latitude_longitude_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  auto slat=grid_params[3];
  if (std::regex_search(slat,std::regex("^-"))) {
    slat=slat.substr(1)+"S";
  }
  else {
    slat+="N";
  }
  auto slon=grid_params[4];
  if (std::regex_search(slon,std::regex("^-"))) {
    slon=slon.substr(1)+"W";
  }
  else {
    slon+="E";
  }
  auto elat=grid_params[5];
  if (std::regex_search(elat,std::regex("^-"))) {
    elat=elat.substr(1)+"S";
  }
  else {
    elat+="N";
  }
  auto elon=grid_params[6];
  if (std::regex_search(elon,std::regex("^-"))) {
    elon=elon.substr(1)+"W";
  }
  else {
    elon+="E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\"latLon\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\" startLat=\"" << slat << "\" startLon=\"" << slon << "\" endLat=\"" << elat << "\" endLon=\"" << elon << "\" xRes=\"" << grid_params[7] << "\" yRes=\"" << grid_params[8] << "\"";
  if (is_cell) {
    ofs << " isCell=\"true\"";
  }
  ofs << ">" << std::endl;
}

void write_gaussian_latitude_longitude_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  auto slat=grid_params[3];
  if (strutils::has_beginning(slat,"-")) {
    slat=slat.substr(1)+"S";
  }
  else {
    slat+="N";
  }
  auto slon=grid_params[4];
  if (strutils::has_beginning(slon,"-")) {
    slon=slon.substr(1)+"W";
  }
  else {
    slon+="E";
  }
  auto elat=grid_params[5];
  if (strutils::has_beginning(elat,"-")) {
    elat=elat.substr(1)+"S";
  }
  else {
    elat+="N";
  }
  auto elon=grid_params[6];
  if (strutils::has_beginning(elon,"-")) {
    elon=elon.substr(1)+"W";
  }
  else {
    elon+="E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\"gaussLatLon\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\" startLat=\"" << slat << "\" startLon=\"" << slon << "\" endLat=\"" << elat << "\" endLon=\"" << elon << "\" xRes=\"" << grid_params[7] << "\" circles=\"" << grid_params[8] << "\">" << std::endl;
}

void write_polar_stereographic_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  auto slat=grid_params[3];
  if (strutils::has_beginning(slat,"-")) {
    slat=slat.substr(1)+"S";
  }
  else {
    slat+="N";
  }
  auto slon=grid_params[4];
  if (strutils::has_beginning(slon,"-")) {
    slon=slon.substr(1)+"W";
  }
  else {
    slon+="E";
  }
  auto elat=grid_params[5];
  if (strutils::has_beginning(elat,"-")) {
    elat=elat.substr(1)+"S";
  }
  else {
    elat+="N";
  }
  auto elon=grid_params[6];
  if (strutils::has_beginning(elon,"-")) {
    elon=elon.substr(1)+"W";
  }
  else {
    elon+="E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[10] << "\" definition=\"polarStereographic\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\"  startLat=\"" << slat << "\" startLon=\"" << slon << "\" resLat=\"" << elat << "\" projLon=\"" << elon << "\" pole=\"" << grid_params[9] << "\" xRes=\"" << grid_params[7] << "\" yRes=\"" << grid_params[8] << "\">" << std::endl;
}

void write_lambert_conformal_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  auto slat=grid_params[3];
  if (strutils::has_beginning(slat,"-")) {
    slat=slat.substr(1)+"S";
  }
  else {
    slat+="N";
  }
  auto slon=grid_params[4];
  if (strutils::has_beginning(slon,"-")) {
    slon=slon.substr(1)+"W";
  }
  else {
    slon+="E";
  }
  auto elat=grid_params[5];
  if (strutils::has_beginning(elat,"-")) {
    elat=elat.substr(1)+"S";
  }
  else {
    elat+="N";
  }
  auto elon=grid_params[6];
  if (strutils::has_beginning(elon,"-")) {
    elon=elon.substr(1)+"W";
  }
  else {
    elon+="E";
  }
  std::string stdpar1=grid_params[10];
  if (strutils::has_beginning(stdpar1,"-")) {
    stdpar1=stdpar1.substr(1)+"S";
  }
  else {
    stdpar1+="N";
  }
  std::string stdpar2=grid_params[11];
  if (strutils::has_beginning(stdpar2,"-")) {
    stdpar2=stdpar2.substr(1)+"S";
  }
  else {
    stdpar2+="N";
  }
  ofs << "  <grid timeRange=\"" << grid_params[12] << "\" definition=\"lambertConformal\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\"  startLat=\"" << slat << "\" startLon=\"" << slon << "\" resLat=\"" << elat << "\" projLon=\"" << elon << "\" pole=\"" << grid_params[9] << "\" xRes=\"" << grid_params[7] << "\" yRes=\"" << grid_params[8] << "\" stdParallel1=\"" << stdpar1 << "\" stdParallel2=\"" << stdpar2 << "\">" << std::endl;
}

void write_mercator_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  auto slat=grid_params[3];
  if (strutils::has_beginning(slat,"-")) {
    slat=slat.substr(1)+"S";
  }
  else {
    slat+="N";
  }
  auto slon=grid_params[4];
  if (strutils::has_beginning(slon,"-")) {
    slon=slon.substr(1)+"W";
  }
  else {
    slon+="E";
  }
  auto elat=grid_params[5];
  if (strutils::has_beginning(elat,"-")) {
    elat=elat.substr(1)+"S";
  }
  else {
    elat+="N";
  }
  auto elon=grid_params[6];
  if (strutils::has_beginning(elon,"-")) {
    elon=elon.substr(1)+"W";
  }
  else {
    elon+="E";
  }
  ofs << "  <grid timeRange=\"" << grid_params[9] << "\" definition=\"mercator\" numX=\"" << grid_params[1] << "\" numY=\"" << grid_params[2] << "\" startLat=\"" << slat << "\" startLon=\"" << slon << "\" endLat=\"" << elat << "\" endLon=\"" << elon << "\" xRes=\"" << grid_params[7] << "\" yRes=\"" << grid_params[8] << "\">" << std::endl;
}

void write_spherical_harmonics_grid(std::deque<std::string>& grid_params,bool is_cell,std::ofstream& ofs)
{
  ofs << "  <grid timeRange=\"" << grid_params[4] << "\" definition=\"sphericalHarmonics\" t1=\"" << grid_params[1] << "\" t2=\"" << grid_params[2] << "\" t3=\"" << grid_params[3] << "\">" << std::endl;
}

void write_grid(std::string grid_entry_key,std::ofstream& ofs,std::string caller,std::string user)
{
  auto grid_params=strutils::split(grid_entry_key,"<!>");
  auto is_cell=false;
  if (grid_params.front().back() == 'C') {
    is_cell=true;
    grid_params.front().pop_back();
  }
  switch (std::stoi(grid_params.front())) {
    case Grid::latitudeLongitudeType: {
	write_latitude_longitude_grid(grid_params,is_cell,ofs);
	break;
    }
    case Grid::gaussianLatitudeLongitudeType: {
	write_gaussian_latitude_longitude_grid(grid_params,is_cell,ofs);
	break;
    }
    case Grid::polarStereographicType: {
	write_polar_stereographic_grid(grid_params,is_cell,ofs);
	break;
    }
    case Grid::lambertConformalType: {
	write_lambert_conformal_grid(grid_params,is_cell,ofs);
	break;
    }
    case Grid::mercatorType: {
	write_mercator_grid(grid_params,is_cell,ofs);
	break;
    }
    case Grid::sphericalHarmonicsType: {
	write_spherical_harmonics_grid(grid_params,is_cell,ofs);
	break;
    }
    default: {
      metautils::log_error("write_grid(): no grid definition map for "+grid_params.front(),caller,user);
    }
  }
}

std::string write(my::map<GridEntry>& grid_table,std::string caller,std::string user)
{
  const std::string THIS_FUNC=std::string("GrML::")+__func__;
  std::string filename;
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"(): unable to create a temporary directory",caller,user);
  }
  tdir.set_keep();
  std::ofstream ofs;
  write_initialize(filename,"GrML",tdir.name(),ofs,caller,user);
  ofs << "<GrML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  ofs << "      xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << std::endl;
  ofs << "                          https://rda.ucar.edu/schemas/GrML.xsd\"" << std::endl;
  ofs << "      uri=\"" << metautils::args.path << "/" << metautils::args.filename;
  ofs << "\" format=\"";
  if (metautils::args.data_format == "grib") {
    ofs << "WMO_GRIB1";
  }
  else if (metautils::args.data_format == "grib0") {
    ofs << "WMO_GRIB0";
  }
  else if (metautils::args.data_format == "grib2") {
    ofs << "WMO_GRIB2";
  }
  else if (metautils::args.data_format == "jraieeemm") {
    ofs << "proprietary_Binary";
  }
  else if (metautils::args.data_format == "oct") {
    ofs << "DSS_Octagonal_Grid";
  }
  else if (metautils::args.data_format == "tropical") {
    ofs << "DSS_Tropical_Grid";
  }
  else if (metautils::args.data_format == "ll") {
    ofs << "DSS_5-Degree_LatLon_Grid";
  }
  else if (metautils::args.data_format == "slp") {
    ofs << "DSS_SLP_Grid";
  }
  else if (metautils::args.data_format == "navy") {
    ofs << "DSS_Navy_Grid";
  }
  else if (metautils::args.data_format == "ussrslp") {
    ofs << "USSR_SLP_Grid";
  }
  else if (metautils::args.data_format == "on84") {
    ofs << "NCEP_ON84";
  }
  else if (metautils::args.data_format == "netcdf") {
    ofs << "netCDF";
  }
  else if (metautils::args.data_format == "hdf5nc4") {
    ofs << "netCDF4";
  }
  else if (metautils::args.data_format == "cgcm1") {
    ofs << "proprietary_ASCII";
  }
  else if (metautils::args.data_format == "cmorph025") {
    ofs << "NCEP_CPC_CMORPH025";
  }
  else if (metautils::args.data_format == "cmorph8km") {
    ofs << "NCEP_CPC_CMORPH8km";
  }
  else if (metautils::args.data_format == "gpcp") {
    ofs << "NASA_GSFC_GPCP";
  }
  else {
    ofs << "??";
  }
  ofs << "\">" << std::endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
  std::vector<std::string> array;
  array.reserve(grid_table.size());
  for (const auto& key : grid_table.keys()) {
    array.emplace_back(key);
  }
  binary_sort(array,
  [](std::string& left,std::string& right) -> bool
  {
    if (left[0] < right[0]) {
	return true;
    }
    else {
	return false;
    }
  });
  for (size_t m=0; m < grid_table.size(); ++m) {
    GridEntry gentry;
    grid_table.found(array[m],gentry);
// print out <grid> element
    write_grid(gentry.key,ofs,caller,user);
// print out <process> elements
    for (const auto& key : gentry.process_table.keys()) {
	ofs << "    <process value=\"" << key << "\" />" << std::endl;
    }
// print out <ensemble> elements
    for (const auto& e_key : gentry.ensemble_table.keys()) {
	auto e_parts=strutils::split(e_key,"<!>");
	ofs << "    <ensemble type=\"" << e_parts[0] << "\"";
	if (!e_parts[1].empty()) {
	  ofs << " ID=\"" << e_parts[1] << "\"";
	}
	if (e_parts.size() > 2) {
	  ofs << " size=\"" << e_parts[2] << "\"";
	}
	ofs << " />" << std::endl;
    }
// print out <level> elements
    std::list<std::string> keys_to_remove;
    for (const auto& l_key : gentry.level_table.keys()) {
	LevelEntry lentry;
	gentry.level_table.found(l_key,lentry);
	auto l_parts=strutils::split(l_key,":");
	if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib0" || metautils::args.data_format == "grib2" || metautils::args.data_format == "netcdf" || metautils::args.data_format == "hdf5nc4") {
	  if (strutils::occurs(l_key,":") == 1) {
	    ofs << "    <level map=\"";
	    if (strutils::contains(l_parts[0],",")) {
		ofs << l_parts[0].substr(0,l_parts[0].find(",")) << "\" type=\"" << l_parts[0].substr(l_parts[0].find(",")+1);
	    }
	    else {
		ofs << l_parts[0];
	    }
	    ofs << "\" value=\"" << l_parts[1] << "\">" << std::endl;
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter map=\"" << param.substr(0,param.find(":")) << "\" value=\"" << param.substr(param.find(":")+1) << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </level>" << std::endl;
	    keys_to_remove.emplace_back(l_key);
	  }
	}
	else if (metautils::args.data_format == "oct" || metautils::args.data_format == "tropical" || metautils::args.data_format == "ll" || metautils::args.data_format == "navy" || metautils::args.data_format == "slp") {
	  if (l_parts[0] == "0") {
	    if (l_parts[1] == "1013" || l_parts[1] == "1001" || l_parts[1] == "980" || l_parts[1] == "0") {
		ofs << "    <level type=\"" << l_parts[1] << "\" value=\"0\">" << std::endl;
	    }
	    else {
		ofs << "    <level value=\"" << l_parts[1] << "\">" << std::endl;
	    }
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </level>" << std::endl;
	    keys_to_remove.emplace_back(l_key);
	  }
	}
	else if (metautils::args.data_format == "jraieeemm") {
	  ofs << "    <level map=\"ds" << metautils::args.dsnum << "\" type=\"" << l_parts[0] << "\" value=\"" << l_parts[1] << "\">" << std::endl;
	  for (const auto& param : lentry.parameter_code_table.keys()) {
	    ParameterEntry pentry;
	    if (lentry.parameter_code_table.found(param,pentry)) {
		ofs << "      <parameter map=\"ds" << metautils::args.dsnum << "\" value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
	    }
	  }
	  ofs << "    </level>" << std::endl;
	  keys_to_remove.emplace_back(l_key);
	}
	else if (metautils::args.data_format == "on84") {
	  auto n=std::stoi(l_parts[0]);
	  if (n < 3 || n == 6 || n <= 7 || (n == 8 && l_parts.size() < 3) || (n >= 128 && n <= 135)) {
	    ofs << "    <level type=\"" << n << "\" value=\"" << l_parts[1] << "\">" << std::endl;
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </level>" << std::endl;
	    keys_to_remove.emplace_back(l_key);
	  }
	}
	else if (metautils::args.data_format == "cgcm1") {
	  ofs << "    <level map=\"ds" << metautils::args.dsnum << "\" type=\"" << l_parts[0] << "\" value=\"";
	  auto sdum=l_parts[0];
	  if (sdum == "1") {
	    sdum="0";
	  }
	  ofs << sdum << "\">" << std::endl;
	  for (const auto& param : lentry.parameter_code_table.keys()) {
	    ParameterEntry pentry;
	    if (lentry.parameter_code_table.found(param,pentry))
		ofs << "      <parameter map=\"ds" << metautils::args.dsnum << "\" value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
	  }
	  ofs << "    </level>" << std::endl;
	  keys_to_remove.emplace_back(l_key);
	}
	else if (metautils::args.data_format == "cmorph025" || metautils::args.data_format == "cmorph8km" || metautils::args.data_format == "gpcp") {
	  ofs << "    <level type=\"" << l_key << "\" value=\"0\">" << std::endl;;
	  for (const auto& param : lentry.parameter_code_table.keys()) {
	    ParameterEntry pentry;
	    if (lentry.parameter_code_table.found(param,pentry))
		ofs << "      <parameter value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
	  }
	  ofs << "    </level>" << std::endl;
	  keys_to_remove.emplace_back(l_key);
	}
	else {
	  ofs << l_key << std::endl;
	}
    }
    for (const auto& r_key : keys_to_remove) {
	gentry.level_table.remove(r_key);
    }
// print out <layer> elements
    keys_to_remove.clear();
    for (const auto& key : gentry.level_table.keys()) {
	LevelEntry lentry;
	gentry.level_table.found(key,lentry);
	auto y_parts=strutils::split(key,":");
	if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib0" || metautils::args.data_format == "grib2" || metautils::args.data_format == "netcdf" || metautils::args.data_format == "hdf5nc4") {
	  if (strutils::occurs(key,":") == 2) {
	    ofs << "    <layer map=\"";
	    if (strutils::contains(y_parts[0],",")) {
		ofs << y_parts[0].substr(0,y_parts[0].find(",")) << "\" type=\"" << y_parts[0].substr(y_parts[0].find(",")+1);
	    }
	    else {
		ofs << y_parts[0];
	    }
	    ofs << "\" bottom=\"" << y_parts[2] << "\" top=\"" << y_parts[1] << "\">" << std::endl;
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter map=\"" << param.substr(0,param.find(":")) << "\" value=\"" << param.substr(param.find(":")+1) << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </layer>" << std::endl;
	    keys_to_remove.emplace_back(key);
	  }
	}
	else if (metautils::args.data_format == "oct" || metautils::args.data_format == "tropical" || metautils::args.data_format == "ll" || metautils::args.data_format == "navy" || metautils::args.data_format == "slp") {
	  if (y_parts[0] == "1") {
	    if (y_parts[1] == "1002" && y_parts[2] == "950") {
		ofs << "    <layer type=\"" << y_parts[1] << "\" bottom=\"0\" top=\"0\">" << std::endl;
	    }
	    else {
		ofs << "    <layer bottom=\"" << y_parts[2] << "\" top=\"" << y_parts[1] << "\">" << std::endl;
	    }
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </layer>" << std::endl;
	    keys_to_remove.emplace_back(key);
	  }
	}
	else if (metautils::args.data_format == "on84") {
	  auto n=std::stoi(y_parts[0]);
	  if ((n == 8 && y_parts.size() == 3) || (n >= 144 && n <= 148)) {
	    ofs << "    <layer type=\"" << n << "\" bottom=\"" << y_parts[2] << "\" top=\"" << y_parts[1] << "\">" << std::endl;
	    for (const auto& param : lentry.parameter_code_table.keys()) {
		ParameterEntry pentry;
		if (lentry.parameter_code_table.found(param,pentry)) {
		  ofs << "      <parameter value=\"" << param << "\" start=\"" << pentry.start_date_time.to_string("%Y-%m-%d %R %Z") << "\" end=\"" << pentry.end_date_time.to_string("%Y-%m-%d %R %Z") << "\" nsteps=\"" << pentry.num_time_steps << "\" />" << std::endl;
		}
	    }
	    ofs << "    </layer>" << std::endl;
	    keys_to_remove.emplace_back(key);
	  }
	}
	else {
	  ofs << key << std::endl;
	}
    }
    for (const auto& r_key : keys_to_remove) {
	gentry.level_table.remove(r_key);
    }
    if (gentry.level_table.size() > 0) {
	metautils::log_error(THIS_FUNC+"(): level/layer table should be empty but table length is "+strutils::itos(gentry.level_table.size()),caller,user);
    }
    ofs << "  </grid>" << std::endl;
  }
  ofs << "</GrML>" << std::endl;
  write_finalize(filename,"GrML",tdir.name(),ofs,caller,user);
  return tdir.name();
}

} // end namespace gatherxml::markup::GrML

namespace ObML {

void copy(std::string metadata_file,std::string URL,std::string caller,std::string user)
{
  TempFile itemp(metautils::directives.temp_path);
  auto ObML_name=metautils::relative_web_filename(URL);
  strutils::replace_all(ObML_name,"/","%");
  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error("copy(): unable to create temporary directory",caller,user);
  }

// create the directory tree in the temp directory
  std::string metadata_path="metadata/wfmd";
  std::stringstream output,error;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir.name()+"/"+metadata_path,output,error) < 0) {
    metautils::log_error("copy(): unable to create a temporary directory - '"+error.str()+"'",caller,user);
  }
  unixutils::rdadata_sync_from("/__HOST__/web/datasets/ds"+metautils::args.dsnum+"/metadata/fmd/"+metadata_file,itemp.name(),metautils::directives.rdadata_home,error);
  std::ifstream ifs(itemp.name().c_str());
  if (!ifs.is_open()) {
    metautils::log_error("copy(): unable to open input file",caller,user);
  }
  std::ofstream ofs((tdir.name()+"/"+metadata_path+"/"+ObML_name+".ObML").c_str());
  if (!ofs.is_open()) {
    metautils::log_error("copy(): unable to open output file",caller,user);
  }
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (strutils::contains(sline,"uri=")) {
	sline=sline.substr(sline.find(" format"));
	sline="      uri=\""+URL+"\""+sline;
    }
    else if (strutils::contains(sline,"ref=")) {
	strutils::replace_all(sline,metadata_file,ObML_name+".ObML");
    }
    ofs << sline << std::endl;
    ifs.getline(line,32768);
  }
  ifs.close();
  ofs.close();
  std::string herror;
  if (unixutils::rdadata_sync(tdir.name(),metadata_path+"/","/data/web/datasets/ds"+metautils::args.dsnum,metautils::directives.rdadata_home,herror) < 0) {
    metautils::log_warning("copy(): unable to sync '"+ObML_name+".ObML' - rdadata_sync error(s): '"+herror+"'",caller,user);
  }
  copy_ancillary_files("ObML",metadata_file,ObML_name,caller,user);
  metautils::args.filename=ObML_name;
}

void write(ObservationData& obs_data,std::string caller,std::string user)
{
  const std::string THIS_FUNC=std::string("ObML::")+__func__;
  std::ofstream ofs,ofs2;
  TempFile tfile2(metautils::directives.temp_path);
  std::string path,filename;
  std::deque<std::string> sp;
  size_t numIDs,time,hr,min,sec;
  bitmap::longitudeBitmap::bitmap_gap biggest,current;
  struct PlatformData {
    PlatformData() : nsteps(0),data_types_table(),start(),end() {}

    size_t nsteps;
    my::map<DataTypeEntry> data_types_table;
    DateTime start,end;
  } platform;
  DataTypeEntry de,de2;
  MySQL::Server server;
  std::string sdum,output,error;
  IDEntry ientry;
  PlatformEntry pentry;

  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"(): unable to create temporary directory",caller,user);
  }
  write_initialize(filename,"ObML",tdir.name(),ofs,caller,user);
  ofs << "<ObML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << std::endl;
  ofs << "                           https://rda.ucar.edu/schemas/ObML.xsd\"" << std::endl;
  ofs << "       uri=\"file://web:" << metautils::relative_web_filename(metautils::args.path+"/"+metautils::args.filename);
  ofs << "\" format=\"";
  if (metautils::args.data_format == "on29" || metautils::args.data_format == "on124") {
    ofs << "NCEP_" << strutils::to_upper(metautils::args.data_format);
  }
  else if (strutils::contains(metautils::args.data_format,"bufr")) {
    ofs << "WMO_BUFR";
  }
  else if (metautils::args.data_format == "cpcsumm" || metautils::args.data_format == "ghcnmv3" || metautils::args.data_format == "hcn" || metautils::args.data_format == "uadb") {
    ofs << "proprietary_ASCII";
  }
  else if (metautils::args.data_format == "imma") {
    ofs << "NOAA_IMMA";
  }
  else if (metautils::args.data_format == "isd") {
    ofs << "NCDC_ISD";
  }
  else if (metautils::args.data_format == "netcdf") {
    ofs << "netCDF";
  }
  else if (metautils::args.data_format == "nodcbt") {
    ofs << "NODC_BT";
  }
  else if (std::regex_search(metautils::args.data_format,std::regex("^td32"))) {
    ofs << "NCDC_" << strutils::to_upper(metautils::args.data_format);
  }
  else if (metautils::args.data_format == "tsr") {
    ofs << "DSS_TSR";
  }
  else if (metautils::args.data_format == "wmssc") {
    ofs << "DSS_WMSSC";
  }
  else if (metautils::args.data_format == "hdf5nc4") {
    ofs << "netCDF4";
  }
  else if (metautils::args.data_format == "hdf5") {
    ofs << "HDF5";
  }
  else if (metautils::args.data_format == "little_r") {
    ofs << "LITTLE_R";
  }
  else {
    ofs << metautils::args.data_format;
  }
  ofs << "\">" << std::endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
  std::string datatype_map;
  if (metautils::args.data_format == "cpcsumm" || metautils::args.data_format == "netcdf" || metautils::args.data_format == "hdf5" || metautils::args.data_format == "ghcnmv3" || metautils::args.data_format == "hcn" || metautils::args.data_format == "proprietary_ASCII" || metautils::args.data_format == "proprietary_Binary") {
    datatype_map="ds"+metautils::args.dsnum;
  }
  std::string metadata_path="metadata/wfmd";
  for (size_t xx=0; xx < obs_data.num_types; ++xx) {
    obs_data.id_tables[xx]->keysort(
    [](const std::string& left,const std::string& right) -> bool
    {
	if (left <= right) {
	  return true;
	}
	else {
	  return false;
	}
    });
    if (obs_data.platform_tables[xx]->size() > 0) {
	ofs << "  <observationType value=\"" << obs_data.observation_types[xx] << "\">" << std::endl;
	for (const auto& key : obs_data.platform_tables[xx]->keys()) {
	  obs_data.platform_tables[xx]->found(key,pentry);
	  ofs2.open((tdir.name()+"/"+metadata_path+"/"+filename+".ObML."+obs_data.observation_types[xx]+"."+key+".IDs.xml").c_str());
	  ofs2 << "<?xml version=\"1.0\" ?>" << std::endl;
	  ofs2 << "<IDs parent=\"" << filename << ".ObML\" group=\"" << obs_data.observation_types[xx] << "." << key << "\">" << std::endl;
	  numIDs=0;
	  platform.nsteps=0;
	  platform.start.set(9999,12,31,235959);
	  platform.end.set(1,1,1,0);
	  platform.data_types_table.clear();
	  for (const auto& ikey : obs_data.id_tables[xx]->keys()) {
	    if (strutils::has_beginning(ikey,key)) {
		sp=strutils::split(ikey,"[!]");
		if (sp.size() < 3) {
		  std::cerr << "Error parsing ID: " << ikey << std::endl;
		  exit(1);
		}
		ofs2 << "  <ID type=\"" << sp[1] << "\" value=\"" << sp[2] << "\"";
		obs_data.id_tables[xx]->found(ikey,ientry);
		if (floatutils::myequalf(ientry.data->S_lat,ientry.data->N_lat) && floatutils::myequalf(ientry.data->W_lon,ientry.data->E_lon)) {
		  ofs2 << " lat=\"" << strutils::ftos(ientry.data->S_lat,4) << "\" lon=\"" << strutils::ftos(ientry.data->W_lon,4) << "\"";
		}
		else {
		  size_t w_index,e_index;
		  bitmap::longitudeBitmap::west_east_bounds(ientry.data->min_lon_bitmap.get(),w_index,e_index);
		  ofs2 << " cornerSW=\"" << strutils::ftos(ientry.data->S_lat,4) << "," << strutils::ftos(ientry.data->min_lon_bitmap[w_index],4) << "\" cornerNE=\"" << strutils::ftos(ientry.data->N_lat,4) << "," << strutils::ftos(ientry.data->max_lon_bitmap[e_index],4) << "\"";
		}
		ofs2 << " start=\"";
		time=ientry.data->start.time();
		hr=time/10000;
		min=(time-hr*10000)/100;
		sec=time % 100;
		if (sec != 99) {
		  ofs2 << ientry.data->start.to_string("%Y-%m-%d %H:%MM:%SS %Z");
		}
		else if (min != 99) {
		  ofs2 << ientry.data->start.to_string("%Y-%m-%d %H:%MM %Z");
		}
		else if (hr != 99) {
		  ofs2 << ientry.data->start.to_string("%Y-%m-%d %H %Z");
		}
		else {
		  ofs2 << ientry.data->start.to_string("%Y-%m-%d");
		}
		ofs2 << "\" end=\"";
		time=ientry.data->end.time();
		hr=time/10000;
		min=(time-hr*10000)/100;
		sec=time % 100;
		if (sec != 99) {
		  ofs2 << ientry.data->end.to_string("%Y-%m-%d %H:%MM:%SS %Z");
		}
		else if (min != 99) {
		  ofs2 << ientry.data->end.to_string("%Y-%m-%d %H:%MM %Z");
		}
		else if (hr != 99) {
		  ofs2 << ientry.data->end.to_string("%Y-%m-%d %H %Z");
		}
		else {
		  ofs2 << ientry.data->end.to_string("%Y-%m-%d");
		}
		ofs2 << "\" numObs=\"" << ientry.data->nsteps << "\">" << std::endl;
		if (ientry.data->start < platform.start) {
		  platform.start=ientry.data->start;
		}
		if (ientry.data->end > platform.end) {
		  platform.end=ientry.data->end;
		}
		platform.nsteps+=ientry.data->nsteps;
		for (const auto& data_type : ientry.data->data_types_table.keys()) {
		  ientry.data->data_types_table.found(data_type,de);
		  if (de.data->nsteps > 0) {
		    ofs2 << "    <dataType";
		    if (!datatype_map.empty()) {
			ofs2 << " map=\"" << datatype_map << "\"";
		    }
		    else if (strutils::contains(metautils::args.data_format,"bufr")) {
			ofs2 << " map=\"" << de.data->map << "\"";
		    }
		    ofs2 << " value=\"" << de.key << "\" numObs=\"" << de.data->nsteps << "\"";
		    if (de.data->vdata == nullptr || de.data->vdata->res_cnt == 0) {
			ofs2 << " />" << std::endl;
		    }
		    else {
			ofs2 << ">" << std::endl;
			ofs2 << "      <vertical min_altitude=\"" << de.data->vdata->min_altitude << "\" max_altitude=\"" << de.data->vdata->max_altitude << "\" vunits=\"" << de.data->vdata->units << "\" avg_nlev=\"" << de.data->vdata->avg_nlev/de.data->nsteps << "\" avg_vres=\"" << de.data->vdata->avg_res/de.data->vdata->res_cnt << "\" />" << std::endl;
			ofs2 << "    </dataType>" << std::endl;
		    }
		    if (!platform.data_types_table.found(de.key,de2)) {
			de2.key=de.key;
			de2.data.reset(new DataTypeEntry::Data);
			platform.data_types_table.insert(de2);
		    }
		    if (strutils::contains(metautils::args.data_format,"bufr")) {
			de2.data->map=de.data->map;
		    }
		    de2.data->nsteps+=de.data->nsteps;
		    if (de2.data->vdata != nullptr && de2.data->vdata->res_cnt > 0) {
			de2.data->vdata->avg_res+=de.data->vdata->avg_res;
			de2.data->vdata->res_cnt+=de.data->vdata->res_cnt;
			if (de.data->vdata->min_altitude < de2.data->vdata->min_altitude) {
			  de2.data->vdata->min_altitude=de.data->vdata->min_altitude;
			}
			if (de.data->vdata->max_altitude > de2.data->vdata->max_altitude) {
			  de2.data->vdata->max_altitude=de.data->vdata->max_altitude;
			}
			de2.data->vdata->avg_nlev+=de.data->vdata->avg_nlev;
			de2.data->vdata->units=de.data->vdata->units;
		    }
		  }
		}
		ofs2 << "  </ID>" << std::endl;
		numIDs++;
	    }
	  }
	  ofs2 << "</IDs>" << std::endl;
	  ofs2.close();
	  ofs << "    <platform type=\"" << key << "\" numObs=\"" << platform.nsteps << "\">" << std::endl;
	  ofs << "      <IDs ref=\"" << filename << ".ObML." << obs_data.observation_types[xx] << "." << key << ".IDs.xml\" numIDs=\"" << numIDs << "\" />" << std::endl;
	  ofs << "      <temporal start=\"" << platform.start.to_string("%Y-%m-%d") << "\" end=\"" << platform.end.to_string("%Y-%m-%d") << "\" />" << std::endl;
	  ofs << "      <locations ref=\"" << filename << ".ObML." << obs_data.observation_types[xx] << "." << key << ".locations.xml\" />" << std::endl;
	  ofs2.open((tdir.name()+"/"+metadata_path+"/"+filename+".ObML."+obs_data.observation_types[xx]+"."+key+".locations.xml").c_str());
	  ofs2 << "<?xml version=\"1.0\" ?>" << std::endl;
	  ofs2 << "<locations parent=\"" << filename << ".ObML\" group=\"" << obs_data.observation_types[xx] << "." << key << "\">" << std::endl;
	  if (pentry.boxflags->spole == 1) {
	    ofs2 << "  <box1d row=\"0\" bitmap=\"0\" />" << std::endl;
	  }
	  for (size_t n=0; n < 180; ++n) {
	    if (pentry.boxflags->flags[n][360] == 1) {
		ofs2 << "  <box1d row=\"" << n+1 << "\" bitmap=\"";
		for (size_t m=0; m < 360; ++m) {
		  ofs2 << static_cast<int>(pentry.boxflags->flags[n][m]);
		}
		ofs2 << "\" />" << std::endl;
	    }
	  }
	  if (pentry.boxflags->npole == 1) {
	    ofs2 << "  <box1d row=\"181\" bitmap=\"0\" />" << std::endl;
	  }
	  ofs2 << "</locations>" << std::endl;
	  ofs2.close();
	  for (const auto& key2 : platform.data_types_table.keys()) {
	    platform.data_types_table.found(key2,de);
	    ofs << "      <dataType";
	    if (!datatype_map.empty()) {
		ofs << " map=\"" << datatype_map << "\"";
	    }
	    else if (strutils::contains(metautils::args.data_format,"bufr")) {
		ofs << " map=\"" << de.data->map << "\"";
	    }
	    ofs << " value=\"" << de.key << "\" numObs=\"" << de.data->nsteps << "\"";
	    if (de.data->vdata == nullptr || de.data->vdata->res_cnt == 0) {
		ofs << " />" << std::endl;
	    }
	    else {
		ofs << ">" << std::endl;
		ofs << "        <vertical min_altitude=\"" << de.data->vdata->min_altitude << "\" max_altitude=\"" << de.data->vdata->max_altitude << "\" vunits=\"" << de.data->vdata->units << "\" avg_nlev=\"" << de.data->vdata->avg_nlev/de.data->nsteps << "\" avg_vres=\"" << de.data->vdata->avg_res/de.data->vdata->res_cnt << "\" />" << std::endl;
		ofs << "      </dataType>" << std::endl;
	    }
	  }
	  ofs << "    </platform>" << std::endl;
	}
	ofs << "  </observationType>" << std::endl;
    }
  }
  ofs << "</ObML>" << std::endl;
  write_finalize(filename,"ObML",tdir.name(),ofs,caller,user);
}

} // end namespace gatherxml::markup::ObML

namespace SatML {

void write(my::map<ScanLineEntry>& scan_line_table,std::list<std::string>& scan_line_table_keys,my::map<ImageEntry>& image_table,std::list<std::string>& image_table_keys,std::string caller,std::string user)
{
  const std::string THIS_FUNC=std::string("SatML::")+__func__;
  std::ofstream ofs,ofs2;
  TempFile tfile2(metautils::directives.temp_path);
  std::string filename,cmd_type;
  std::stringstream output,error;
  std::string herror;
  ScanLineEntry se;
  ImageEntry ie;

  TempDir tdir;
  if (!tdir.create(metautils::directives.temp_path)) {
    metautils::log_error(THIS_FUNC+"(): unable to create temporary directory",caller,user);
  }
  write_initialize(filename,"SatML",tdir.name(),ofs,caller,user);
  ofs << "<SatML xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  ofs << "       xsi:schemaLocation=\"https://rda.ucar.edu/schemas" << std::endl;
  ofs << "                           https://rda.ucar.edu/schemas/SatML.xsd\"" << std::endl;
  ofs << "       uri=\"" << metautils::args.path << "/" << metautils::args.filename << "\" format=\"";
  if (metautils::args.data_format == "noaapod") {
    ofs << "NOAA_Polar_Orbiter";
  }
  else if (metautils::args.data_format == "mcidas") {
    ofs << "McIDAS";
  }
  ofs << "\">" << std::endl;
  ofs << "  <timeStamp value=\"" << dateutils::current_date_time().to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
  std::string metadata_path="metadata/wfmd";
  if (metautils::args.data_format == "noaapod") {
    for (const auto& key : scan_line_table_keys) {
	scan_line_table.found(key,se);
	ofs << "  <satellite ID=\"" << se.sat_ID << "\">" << std::endl;
	double avg_width=0.;
	double res=0.;
	for (const auto& scanline : se.scan_line_list) {
	  avg_width+=scanline.width;
	  res+=scanline.res;
	}
	ofs << "    <swath units=\"km\" avgWidth=\"" << round(avg_width/se.scan_line_list.size()) << "\" res=\"" << round(res/se.scan_line_list.size()) << "\">" << std::endl;
	ofs << "      <data type=\"" << key << "\">" << std::endl;
	ofs << "        <temporal start=\"" << se.start_date_time.to_string("%Y-%m-%d %T %Z") << "\" end=\"" << se.end_date_time.to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
	if (se.sat_ID == "NOAA-11") {
	  ofs << "        <spectralBand units=\"um\" value=\"0.58-0.68\" />" << std::endl;
	  ofs << "        <spectralBand units=\"um\" value=\"0.725-1.1\" />" << std::endl;
	  ofs << "        <spectralBand units=\"um\" value=\"3.55-3.93\" />" << std::endl;
	  ofs << "        <spectralBand units=\"um\" value=\"10.3-11.3\" />" << std::endl;
	  ofs << "        <spectralBand units=\"um\" value=\"11.5-12.5\" />" << std::endl;
	}
	ofs << "        <scanLines ref=\"" << metautils::args.filename << "." << key << ".scanLines\" num=\"" << se.scan_line_list.size() << "\" />" << std::endl;
	ofs2.open((tdir.name()+"/"+metadata_path+"/"+metautils::args.filename+"."+key+".scanLines").c_str());
	ofs2 << "<?xml version=\"1.0\" ?>" << std::endl;
	ofs2 << "<scanLines parent=\"" << metautils::args.filename << ".SatML\">" << std::endl;
	for (const auto& scanline : se.scan_line_list) {
	  ofs2 << "  <scanLine time=\"" << scanline.date_time.to_string("%Y-%m-%d %T %Z") << "\" numPoints=\"409\" width=\"" << round(scanline.width) << "\" res=\"" << round(scanline.res) << "\">" << std::endl;
	  ofs2 << "    <start lat=\"" << round(scanline.first_coordinate.lat*100.)/100. << "\" lon=\"" << round(scanline.first_coordinate.lon*100.)/100. << "\" />" << std::endl;
	  ofs2 << "    <subpoint lat=\"" << round(scanline.subpoint.lat*100.)/100. << "\" lon=\"" << round(scanline.subpoint.lon*100.)/100. << "\" />" << std::endl;
	  ofs2 << "    <end lat=\"" << round(scanline.last_coordinate.lat*100.)/100. << "\" lon=\"" << round(scanline.last_coordinate.lon*100.)/100. << "\" />" << std::endl;
	  ofs2 << "  </scanLine>" << std::endl;
	}
	ofs2 << "</scanLines>" << std::endl;
	ofs2.close();
	ofs << "      </data>" << std::endl;
	ofs << "    </swath>" << std::endl;
	ofs << "  </satellite>" << std::endl;
    }
  }
  else if (metautils::args.data_format == "mcidas") {
    for (const auto& key : image_table_keys) {
	image_table.found(key,ie);
	ofs << "  <satellite ID=\"" << ie.key << "\">" << std::endl;
	ofs << "    <temporal start=\"" << ie.start_date_time.to_string("%Y-%m-%d %T %Z") << "\" end=\"" << ie.end_date_time.to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
	ofs << "    <images ref=\"" << metautils::args.filename << ".images\" num=\"" << ie.image_list.size() << "\" />" << std::endl;
	ofs2.open((tdir.name()+"/"+metadata_path+"/"+metautils::args.filename+".images").c_str());
	ofs2 << "<?xml version=\"1.0\" ?>" << std::endl;
	ofs2 << "<images parent=\"" << metautils::args.filename << ".SatML\">" << std::endl;
	for (const auto& image : ie.image_list) {
	  ofs2 << "  <image>" << std::endl;
	  ofs2 << "    <time value=\"" << image.date_time.to_string("%Y-%m-%d %T %Z") << "\" />" << std::endl;
	  ofs2 << "    <resolution units=\"km\" x=\"" << round(image.xres) << "\" y=\"" << round(image.yres) << "\" />" << std::endl;
	  if (ie.key == "GMS Visible") {
	    ofs2 << "    <spectralBand units=\"um\" value=\"0.55-1.05\" />" << std::endl;
	  }
	  else if (ie.key == "GMS Infrared") {
	    ofs2 << "    <spectralBand units=\"um\" value=\"10.5-12.5\" />" << std::endl;
	  }
	  ofs2 << "    <centerPoint lat=\"" << round(image.center.lat*100.)/100. << "\" lon=\"" << round(image.center.lon*100.)/100. << "\" />" << std::endl;
	  if (image.corners.nw_coord.lat < -99. && image.corners.nw_coord.lon < -999. && image.corners.ne_coord.lat < -99. && image.corners.ne_coord.lon < -999. && image.corners.sw_coord.lat < -99. && image.corners.sw_coord.lon < -999. && image.corners.se_coord.lat < -99. && image.corners.se_coord.lon < -999.) {
	    ofs2 << "    <fullDiskImage />" << std::endl;
	  }
	  else {
	    ofs2 << "    <northwestCorner lat=\"" << round(image.corners.nw_coord.lat*100.)/100. << "\" lon=\"" << round(image.corners.nw_coord.lon*100.)/100. << "\" />" << std::endl;
	    ofs2 << "    <southwestCorner lat=\"" << round(image.corners.sw_coord.lat*100.)/100. << "\" lon=\"" << round(image.corners.sw_coord.lon*100.)/100. << "\" />" << std::endl;
	    ofs2 << "    <southeastCorner lat=\"" << round(image.corners.se_coord.lat*100.)/100. << "\" lon=\"" << round(image.corners.se_coord.lon*100.)/100. << "\" />" << std::endl;
	    ofs2 << "    <northeastCorner lat=\"" << round(image.corners.ne_coord.lat*100.)/100. << "\" lon=\"" << round(image.corners.ne_coord.lon*100.)/100. << "\" />" << std::endl;
	  }
	  ofs2 << "  </image>" << std::endl;
	}
	ofs2 << "</images>" << std::endl;
	ofs2.close();
	ofs << "  </satellite>" << std::endl;
    }
  }
  ofs << "</SatML>" << std::endl;
  write_finalize(filename,"SatML",tdir.name(),ofs,caller,user);
}

} // end namespace gatherxml::markup::SatML

} // end namespace gatherxml::markup

} // end namespace gatherxml
