#include <iostream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <list>
#include <sys/stat.h>
#include <signal.h>
#include <gatherxml.hpp>
#include <cyclone.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <datetime.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");
std::string myerror="";
std::string mywarning="";

my::map<gatherxml::markup::FixML::FeatureEntry> feature_table;
std::unique_ptr<TempFile> tfile;
std::unique_ptr<TempDir> tdir;
my::map<gatherxml::markup::FixML::StageEntry> stage_table;
gatherxml::markup::FixML::StageEntry sentry;

extern "C" void clean_up()
{
}

void process_HURDAT(std::unique_ptr<Cyclone>& c)
{
  HURDATCyclone *hc=reinterpret_cast<HURDATCyclone *>(c.get());
  gatherxml::markup::FixML::FeatureEntry fe;
  gatherxml::markup::FixML::ClassificationEntry ce;
  size_t l,k;
  float elon,min_elon=99999.,max_elon=-99999.;

  auto fd=hc->fix_data();
  if (hc->latitude_hemisphere() == 'S') {
    if (fd[0].datetime.month() >= 7) {
	fe.key=strutils::itos(fd[0].datetime.year()+1)+"-"+hc->ID();
    }
    else {
	fe.key=strutils::itos(fd[0].datetime.year())+"-"+hc->ID();
    }
  }
  else {
    fe.key=strutils::itos(fd[0].datetime.year())+"-"+hc->ID();
  }
  fe.data.reset(new gatherxml::markup::FixML::FeatureEntry::Data);
  ce.key="";
  ce.pres_units="mbar";
  ce.wind_units="kt";
  for (const auto& fix : fd) {
    if (fix.classification != ce.key) {
	if (!ce.key.empty()) {
	  fe.data->classification_list.emplace_back(ce);
	}
	ce.key=fix.classification;
	ce.start_datetime=fix.datetime;
	ce.start_lat=fix.latitude;
	ce.start_lon=fix.longitude;
	ce.min_lat=ce.max_lat=fix.latitude;
	ce.min_lon=ce.max_lon=elon=fix.longitude;
	if (elon < 0.) {
	  elon+=360.;
	}
	min_elon=max_elon=elon;
	ce.min_pres=ce.max_pres=fix.min_pres;
	ce.min_speed=ce.max_speed=fix.max_wind;
	ce.nfixes=0;
    }
    ce.end_datetime=fix.datetime;
    ce.end_lat=fix.latitude;
    ce.end_lon=fix.longitude;
    elon=fix.longitude;
    if (elon < 0.)
	elon+=360.;
    if (fix.latitude < ce.min_lat)
	ce.min_lat=fix.latitude;
    if (fix.latitude > ce.max_lat)
	ce.max_lat=fix.latitude;
    if (fix.longitude < ce.min_lon)
	ce.min_lon=fix.longitude;
    if (fix.longitude > ce.max_lon)
	ce.max_lon=fix.longitude;
    if (elon < min_elon)
	min_elon=elon;
    if (elon > max_elon)
	max_elon=elon;
    if (fix.min_pres < ce.min_pres)
	ce.min_pres=fix.min_pres;
    if (fix.min_pres > ce.max_pres)
	ce.max_pres=fix.min_pres;
    if (fix.max_wind < ce.min_speed)
	ce.min_speed=fix.max_wind;
    if (fix.max_wind > ce.max_speed)
	ce.max_speed=fix.max_wind;
    ce.nfixes++;
    if (!stage_table.found(ce.key,sentry)) {
	sentry.key=ce.key;
	sentry.data.reset(new gatherxml::markup::FixML::StageEntry::Data);
	sentry.data->boxflags.initialize(361,180,0,0);
	if (fix.latitude == -90.) {
	  sentry.data->boxflags.spole=1;
	}
	else if (fix.latitude == 90.) {
	  sentry.data->boxflags.npole=1;
	}
	else {
	  geoutils::convert_lat_lon_to_box(1,fix.latitude,fix.longitude,l,k);
	  sentry.data->boxflags.flags[l-1][k]=1;
	  sentry.data->boxflags.flags[l-1][360]=1;
	}
	sentry.data->start=fix.datetime;
	sentry.data->end=fix.datetime;
	stage_table.insert(sentry);
    }
    else {
	if (fix.latitude == -90.) {
	  sentry.data->boxflags.spole=1;
	}
	else if (fix.latitude == 90.) {
	  sentry.data->boxflags.npole=1;
	}
	else {
	  geoutils::convert_lat_lon_to_box(1,fix.latitude,fix.longitude,l,k);
	  sentry.data->boxflags.flags[l-1][k]=1;
	  sentry.data->boxflags.flags[l-1][360]=1;
	}
	if (fix.datetime < sentry.data->start) {
	  sentry.data->start=fix.datetime;
	}
	if (fix.datetime > sentry.data->end) {
	  sentry.data->end=fix.datetime;
	}
    }
  }
  if ( (max_elon-min_elon) > (ce.max_lon-ce.min_lon)) {
    ce.min_lon=min_elon;
    if (ce.min_lon > 180.) {
	ce.min_lon-=360.;
    }
    ce.max_lon=max_elon;
    if (ce.max_lon > 180.) {
	ce.max_lon-=360.;
    }
  }
  fe.data->classification_list.emplace_back(ce);
  feature_table.insert(fe);
}

void process_TC_vitals(std::unique_ptr<Cyclone>& c)
{
  TCVitalsCyclone *tcvc=reinterpret_cast<TCVitalsCyclone *>(c.get());
  gatherxml::markup::FixML::FeatureEntry fe;
  gatherxml::markup::FixML::ClassificationEntry ce;

  auto fd=tcvc->fix_data();
  std::stringstream fe_key;
  fe_key << fd[0].datetime.year() << "-" << tcvc->storm_number() << tcvc->basin_ID();
  if (!feature_table.found(fe_key.str(),fe)) {
    fe.key=fe_key.str();
    fe.data.reset(new gatherxml::markup::FixML::FeatureEntry::Data);
    fe.data->alt_id=tcvc->ID();
    feature_table.insert(fe);
  }
  ce.key="tropical";
  if (fe.data->classification_list.size() == 0) {
    ce.start_datetime=ce.end_datetime=fd[0].datetime;
    ce.start_lat=ce.end_lat=ce.min_lat=ce.max_lat=fd[0].latitude;
    ce.start_lon=ce.end_lon=ce.min_lon=ce.max_lon=fd[0].longitude;
    ce.min_pres=ce.max_pres=fd[0].min_pres;
    ce.pres_units="mbar";
    ce.min_speed=ce.max_speed=fd[0].max_wind;
    ce.wind_units="m s-1";
    ce.nfixes=1;
    fe.data->classification_list.emplace_back(ce);
  }
  else {
    gatherxml::markup::FixML::ClassificationEntry &b=fe.data->classification_list.back();
    if (fd[0].datetime < b.start_datetime) {
	b.start_datetime=fd[0].datetime;
	b.start_lat=fd[0].latitude;
	b.start_lon=fd[0].longitude;
    }
    if (fd[0].datetime > b.end_datetime) {
	fe.data->alt_id=tcvc->ID();
	b.end_datetime=fd[0].datetime;
	b.end_lat=fd[0].latitude;
	b.end_lon=fd[0].longitude;
    }
    if (fd[0].latitude < b.min_lat) {
	b.min_lat=fd[0].latitude;
    }
    if (fd[0].latitude > b.max_lat) {
	b.max_lat=fd[0].latitude;
    }
    if (fd[0].longitude < b.min_lon) {
	b.min_lon=fd[0].longitude;
    }
    if (fd[0].longitude > b.max_lon) {
	b.max_lon=fd[0].longitude;
    }
    if (fd[0].min_pres < b.min_pres) {
	b.min_pres=fd[0].min_pres;
    }
    if (fd[0].min_pres > b.max_pres) {
	b.max_pres=fd[0].min_pres;
    }
    if (fd[0].max_wind < b.min_speed) {
	b.min_speed=fd[0].max_wind;
    }
    if (fd[0].max_wind > b.max_speed) {
	b.max_speed=fd[0].max_wind;
    }
    ++b.nfixes;
  }
  if (!stage_table.found(ce.key,sentry)) {
    sentry.key=ce.key;
    sentry.data.reset(new gatherxml::markup::FixML::StageEntry::Data);
    sentry.data->boxflags.initialize(361,180,0,0);
    if (fd[0].latitude == -90.) {
	sentry.data->boxflags.spole=1;
    }
    else if (fd[0].latitude == 90.) {
	sentry.data->boxflags.npole=1;
    }
    else {
	size_t l,k;
	geoutils::convert_lat_lon_to_box(1,fd[0].latitude,fd[0].longitude,l,k);
	sentry.data->boxflags.flags[l-1][k]=1;
	sentry.data->boxflags.flags[l-1][360]=1;
    }
    sentry.data->start=fd[0].datetime;
    sentry.data->end=fd[0].datetime;
    stage_table.insert(sentry);
  }
  else {
    if (fd[0].latitude == -90.) {
	sentry.data->boxflags.spole=1;
    }
    else if (fd[0].latitude == 90.) {
	sentry.data->boxflags.npole=1;
    }
    else {
	size_t l,k;
	geoutils::convert_lat_lon_to_box(1,fd[0].latitude,fd[0].longitude,l,k);
	sentry.data->boxflags.flags[l-1][k]=1;
	sentry.data->boxflags.flags[l-1][360]=1;
    }
    if (fd[0].datetime < sentry.data->start) {
	sentry.data->start=fd[0].datetime;
    }
    if (fd[0].datetime > sentry.data->end) {
	sentry.data->end=fd[0].datetime;
    }
  }
}

void process_CXML(XMLDocument& xdoc)
{
  XMLElement e;
  std::list<XMLElement> elist;
  gatherxml::markup::FixML::FeatureEntry fe;
  gatherxml::markup::FixML::ClassificationEntry ce;
  std::string src,sdum;
  float fdum,lat,lon;
  size_t k,l;
  float elon,min_elon,max_elon;

  e=xdoc.element("cxml/header/productionCenter");
  src=e.content();
  strutils::trim(src);
  strutils::replace_all(src,"\n","");
  while (strutils::contains(src," ")) {
    strutils::replace_all(src," ","");
  }
  strutils::replace_all(src,"<subCenter>","_");
  strutils::replace_all(src,"</subCenter>","");
  e=xdoc.element("cxml/header/generatingApplication/model/name");
  if (!e.name().empty()) {
    src+="_"+e.content();
  }
  elist=xdoc.element_list("cxml/data");
  for (auto& ele : elist) {
    auto disturbance_list=ele.element_list("disturbance");
    for (auto& dist : disturbance_list) {
	auto fix_list=dist.element_list("fix");
	ce.nfixes=fix_list.size();
	if (ce.nfixes > 0) {
	  fe.key=dist.attribute_value("ID");
	  e=dist.element("cycloneName");
	  if (!e.content().empty()) {
	    ce.key="tropical";
	    fe.key+="_"+e.content();
	  }
	  else {
	    ce.key="extratropical";
	  }
	  if (!feature_table.found(fe.key,fe)) {
	    fe.data.reset(new gatherxml::markup::FixML::FeatureEntry::Data);
	    feature_table.insert(fe);
	  }
	  sdum=ele.attribute_value("type");
	  ce.src=src+"_"+sdum;
	  sdum=ele.attribute_value("member");
	  if (!sdum.empty()) {
	    ce.src+="_member_"+sdum;
	  }
	  ce.start_datetime.set_year(1000);
	  ce.start_lat=-99.;
	  ce.start_lon=-199.;
	  ce.min_lat=99.;
	  ce.min_lon=199.;
	  ce.max_lat=-99.;
	  ce.max_lon=-199.;
	  min_elon=360.;
	  max_elon=0.;
	  ce.min_pres=99999.;
	  ce.max_pres=-99999.;
	  ce.min_speed=99999.;
	  ce.max_speed=-99999.;
	  for (const auto& fix : fix_list) {
	    e=fix.element("validTime");
	    if (!e.name().empty()) {
		sdum=e.content();
		strutils::replace_all(sdum,"-","");
		strutils::replace_all(sdum,"T","");
		strutils::replace_all(sdum,":","");
		strutils::replace_all(sdum,"Z","");
		if (ce.start_datetime.year() == 1000) {
		  ce.start_datetime.set(std::stoll(sdum));
		}
		ce.end_datetime.set(std::stoll(sdum));
	    }
	    lat=-99.;
	    lon=-199.;
	    e=fix.element("latitude");
	    if (!e.name().empty()) {
		sdum=e.content();
		strutils::trim(sdum);
		if (!sdum.empty() && strutils::is_numeric(sdum)) {
		  lat=std::stof(sdum);
		  sdum=e.attribute_value("units");
		  if (sdum == "deg S") {
		    lat=-lat;
		  }
		}
		else {
		  std::cerr << "Terminating - process_CXML() found an empty latitude element" << std::endl;
		  exit(1);
		}
	    }
	    e=fix.element("longitude");
	    if (!e.name().empty()) {
		sdum=e.content();
		strutils::trim(sdum);
		if (!sdum.empty() && strutils::is_numeric(sdum)) {
		  lon=std::stof(sdum);
		  sdum=e.attribute_value("units");
		  if (sdum == "deg W") {
		    lon=-lon;
		  }
		  else if (sdum == "deg E") {
		    if (lon > 180.) {
			lon-=360.;
		    }
		  }
		}
		else {
		  std::cerr << "Terminating - process_CXML() found an empty longitude element" << std::endl;
		  exit(1);
		}
	    }
	    if (lat > -99. && lon > -199.) {
		if (ce.start_lat < -90.) {
	 	  ce.start_lat=lat;
		  ce.start_lon=lon;
		}
		ce.end_lat=lat;
		ce.end_lon=lon;
		if (lat < ce.min_lat) {
		  ce.min_lat=lat;
		}
		if (lon < ce.min_lon) {
		  ce.min_lon=lon;
		}
		if (lat > ce.max_lat) {
		  ce.max_lat=lat;
		}
		if (lon > ce.max_lon) {
		  ce.max_lon=lon;
		}
		elon=lon;
		if (elon < 0.) {
		  elon+=360.;
		}
		if (elon < min_elon) {
		  min_elon=elon;
		}
		if (elon > max_elon) {
		  max_elon=elon;
		}
		if (!stage_table.found(ce.key,sentry)) {
		  sentry.key=ce.key;
		  sentry.data.reset(new gatherxml::markup::FixML::StageEntry::Data);
		  sentry.data->boxflags.initialize(361,180,0,0);
		  if (lat == -90.) {
		    sentry.data->boxflags.spole=1;
		  }
		  else if (lat == 90.) {
		    sentry.data->boxflags.npole=1;
		  }
		  else {
		    geoutils::convert_lat_lon_to_box(1,lat,lon,l,k);
		    sentry.data->boxflags.flags[l-1][k]=1;
		    sentry.data->boxflags.flags[l-1][360]=1;
		  }
		  sentry.data->start=ce.end_datetime;
		  sentry.data->end=ce.end_datetime;
		  stage_table.insert(sentry);
		}
		else {
		  if (lat == -90.) {
		    sentry.data->boxflags.spole=1;
		  }
		  else if (lat == 90.) {
		    sentry.data->boxflags.npole=1;
		  }
		  else {
		    geoutils::convert_lat_lon_to_box(1,lat,lon,l,k);
		    sentry.data->boxflags.flags[l-1][k]=1;
		    sentry.data->boxflags.flags[l-1][360]=1;
		  }
		  if (ce.end_datetime < sentry.data->start) {
		    sentry.data->start=ce.end_datetime;
		  }
		  if (ce.end_datetime > sentry.data->end) {
		    sentry.data->end=ce.end_datetime;
		  }
		}
	    }
	    e=fix.element("cycloneData/minimumPressure/pressure");
	    if (!e.name().empty() && strutils::is_numeric(e.content())) {
		fdum=std::stof(e.content());
		if (fdum < ce.min_pres) {
		  ce.min_pres=fdum;
		}
		if (fdum > ce.max_pres) {
		  ce.max_pres=fdum;
		}
		ce.pres_units=e.attribute_value("units");
	    }
	    e=fix.element("cycloneData/maximumWind/speed");
	    if (!e.name().empty() && strutils::is_numeric(e.content())) {
		fdum=std::stof(e.content());
		if (fdum < ce.min_speed) {
		  ce.min_speed=fdum;
		}
		if (fdum > ce.max_speed) {
		  ce.max_speed=fdum;
		}
		ce.wind_units=e.attribute_value("units");
	    }
	  }
	  if ( (max_elon-min_elon) > (ce.max_lon-ce.min_lon)) {
	    ce.min_lon=min_elon;
	    if (ce.min_lon > 180.) {
		ce.min_lon-=360.;
	    }
	    ce.max_lon=max_elon;
	    if (ce.max_lon > 180.) {
		ce.max_lon-=360.;
	    }
	  }
	  fe.data->classification_list.emplace_back(ce);
	}
    }
  }
}

bool open_file(void *istream,std::string filename)
{
  if (metautils::args.data_format == "hurdat") {
    return (reinterpret_cast<InputHURDATCycloneStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "tcvitals") {
    return (reinterpret_cast<InputTCVitalsCycloneStream *>(istream))->open(filename);
  }
  return false;
}

void scan_file()
{
  tfile.reset(new TempFile);
  tdir.reset(new TempDir);
  tfile->open(metautils::args.temp_loc);
  tdir->create(metautils::args.temp_loc);
  std::unique_ptr<idstream> istream;
  std::unique_ptr<Cyclone> c;
  if (metautils::args.data_format == "cxml") {
    std::string file_format,error;
    std::list<std::string> filelist;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
	metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","fix2xml",USER);
    }
    if (filelist.size() == 0) {
	filelist.emplace_back(tfile->name());
    }
    for (const auto& file : filelist) {
	XMLDocument xdoc(file);
	if (!xdoc) {
	  if (metautils::args.dsnum != "330.3")
	    std::cerr << "Error: scan_file() was unable to parse " << file << std::endl;
	  exit(1);
	}
	process_CXML(xdoc);
	xdoc.close();
    }
  }
  else if (metautils::args.data_format == "tcvitals") {
    std::string file_format,error;
    std::list<std::string> filelist;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,&filelist,file_format,error)) {
	metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","fix2xml",USER);
    }
    if (filelist.size() == 0) {
	filelist.emplace_back(tfile->name());
    }
    istream.reset(new InputTCVitalsCycloneStream);
    c.reset(new TCVitalsCyclone);
    for (const auto& file : filelist) {
	if (!open_file(istream.get(),file)) {
	  metautils::log_error("scan_file(): unable to open file for input","fix2xml",USER);
	}
	const size_t BUF_LEN=80000;
	std::unique_ptr<unsigned char[]> buffer(new unsigned char[BUF_LEN]);
	int status;
	while ( (status=istream->read(buffer.get(),BUF_LEN)) > 0) {
	  c->fill(buffer.get(),Cyclone::full_report);
	  process_TC_vitals(c);
	}
	istream->close();
	if (status == bfstream::error) {
	  metautils::log_error("read error","fix2xml",USER);
	}
    }
  }
  else {
    if (metautils::args.data_format == "hurdat") {
	istream.reset(new InputHURDATCycloneStream);
	c.reset(new HURDATCyclone);
    }
    else {
	metautils::log_error("format "+metautils::args.data_format+" not recognized","fix2xml",USER);
    }
    std::string file_format,error;
    if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,nullptr,file_format,error)) {
	metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","fix2xml",USER);
    }
    if (!open_file(istream.get(),tfile->name())) {
	metautils::log_error("scan_file(): unable to open file for input","fix2xml",USER);
    }
    if (istream != nullptr) {
	const size_t BUF_LEN=80000;
	std::unique_ptr<unsigned char[]> buffer(new unsigned char[BUF_LEN]);
	int status;
	while ( (status=istream->read(buffer.get(),BUF_LEN)) > 0) {
	  c->fill(buffer.get(),Cyclone::full_report);
	  if (metautils::args.data_format == "hurdat") {
	    process_HURDAT(c);
	  }
	}
	istream->close();
	if (status == bfstream::error) {
	  metautils::log_error("read error","fix2xml",USER);
	}
    }
  }
  if (feature_table.size() == 0) {
    if (metautils::args.dsnum != "330.3") {
	std::cerr << "Terminating - no fix data found in file" << std::endl;
    }
    exit(1);
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","fix2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  if (argc < 6) {
    std::cerr << "usage: " << argv[0] << " -f format -d [ds]nnn.n [options...] path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f cxml     TIGGE Tropical Cyclone XML format" << std::endl;
    std::cerr << "-f hurdat   NHC HURDAT format" << std::endl;
    std::cerr << "-f tcvitals NCEP Tropical Cyclone Vital Statistics Records format" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>  nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    std::cerr << "-g/-G            update/don't update graphics (default is -g)" << std::endl;
    std::cerr << "-l <local_name>  name of the MSS file on local disk (avoids an MSS read)" << std::endl;
    std::cerr << "-s/-S            do/don't update the dataset summary information (default is -s)" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>      full MSS path or URL of the file to read" << std::endl;
    std::cerr << "            - MSS paths must begin with \"/FS/DECS\"" << std::endl;
    std::cerr << "            - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  auto arg_delimiter='%';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("fix2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  std::string flags="-f";
  if (strutils::has_beginning(metautils::args.path,"https://rda.ucar.edu")) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("fix2xml",USER);
  if (!metautils::args.overwrite_only) {
    metautils::check_for_existing_cmd("FixML");
  }
  scan_file();
  gatherxml::markup::FixML::write(feature_table,stage_table,"fix2xml",USER);
  if (metautils::args.update_db) {
    if (!metautils::args.update_graphics) {
	flags="-G "+flags;
    }
    if (!metautils::args.update_summary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" "+flags+" "+metautils::args.filename+".FixML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
