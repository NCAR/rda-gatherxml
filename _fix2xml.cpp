#include <iostream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <list>
#include <sys/stat.h>
#include <signal.h>
#include <cyclone.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tempfile.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <datetime.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
my::map<metadata::FixML::FeatureEntry> featureTable;
std::string user=getenv("USER");
TempFile *tfile=NULL;
TempDir *tdir=NULL;
my::map<metadata::FixML::StageEntry> stageTable;
metadata::FixML::StageEntry sentry;
std::string myerror="";

extern "C" void cleanUp()
{
  if (tfile != NULL)
    delete tfile;
  if (tdir != NULL)
    delete tdir;
}

void parseArgs()
{
  std::deque<std::string> sp;
  size_t n;

  args.updateDB=true;
  args.updateSummary=true;
  args.updateGraphics=true;
  args.overridePrimaryCheck=false;
  args.overwriteOnly=false;
  args.temp_loc=directives.tempPath;
  sp=strutils::split(args.argsString,"%");
  for (n=0; n < sp.size()-1; n++) {
    if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
        args.member_name=sp[++n];
    }
    else if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (strutils::has_beginning(args.dsnum,"ds")) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-m") {
	args.member_name=sp[++n];
    }
    else if (sp[n] == "-G") {
	args.updateGraphics=false;
    }
    else if (sp[n] == "-S") {
	args.updateSummary=false;
    }
    else if (sp[n] == "-OO") {
        args.overwriteOnly=true;
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "Error: no format specified" << std::endl;
    exit(1);
  }
  else {
    strutils::to_lower(args.format);
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "Error: no dataset number specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.overridePrimaryCheck=true;
    args.updateDB=false;
    args.updateSummary=false;
    args.regenerate=false;
  }
  args.path=sp[sp.size()-1];
  n=args.path.length()-1;
  while (n > 0 && args.path[n] != '/') {
    --n;
  }
  args.filename=args.path.substr(n+1);
  args.path=args.path.substr(0,n);
}

void processHURDAT(Cyclone *c)
{
  HURDATCyclone *hc=reinterpret_cast<HURDATCyclone *>(c);
  Array<Cyclone::FixData> fd;
  metadata::FixML::FeatureEntry fe;
  metadata::FixML::ClassificationEntry ce;
  size_t n,l,k;
  float elon,min_elon=99999.,max_elon=-99999.;

  fd=hc->getFixData();
  if (hc->getLatitudeHemisphere() == 'S') {
    if (fd[0].datetime.getMonth() >= 7) {
	fe.key=strutils::itos(fd[0].datetime.getYear()+1)+"-"+hc->getID();
    }
    else {
	fe.key=strutils::itos(fd[0].datetime.getYear())+"-"+hc->getID();
    }
  }
  else {
    fe.key=strutils::itos(fd[0].datetime.getYear())+"-"+hc->getID();
  }
  fe.data.reset(new metadata::FixML::FeatureEntry::Data);
  ce.key="";
  ce.pres_units="mbar";
  ce.wind_units="kt";
  for (n=0; n < fd.length(); ++n) {
    if (fd(n).classification != ce.key) {
	if (ce.key.length() > 0) {
	  fe.data->classificationList.emplace_back(ce);
	}
	ce.key=fd(n).classification;
	ce.start_datetime=fd(n).datetime;
	ce.start_lat=fd(n).latitude;
	ce.start_lon=fd(n).longitude;
	ce.min_lat=ce.max_lat=fd(n).latitude;
	ce.min_lon=ce.max_lon=elon=fd(n).longitude;
	if (elon < 0.) {
	  elon+=360.;
	}
	min_elon=max_elon=elon;
	ce.min_pres=ce.max_pres=fd(n).min_pres;
	ce.min_speed=ce.max_speed=fd(n).max_wind;
	ce.nfixes=0;
    }
    ce.end_datetime=fd(n).datetime;
    ce.end_lat=fd(n).latitude;
    ce.end_lon=fd(n).longitude;
    elon=fd(n).longitude;
    if (elon < 0.)
	elon+=360.;
    if (fd(n).latitude < ce.min_lat)
	ce.min_lat=fd(n).latitude;
    if (fd(n).latitude > ce.max_lat)
	ce.max_lat=fd(n).latitude;
    if (fd(n).longitude < ce.min_lon)
	ce.min_lon=fd(n).longitude;
    if (fd(n).longitude > ce.max_lon)
	ce.max_lon=fd(n).longitude;
    if (elon < min_elon)
	min_elon=elon;
    if (elon > max_elon)
	max_elon=elon;
    if (fd(n).min_pres < ce.min_pres)
	ce.min_pres=fd(n).min_pres;
    if (fd(n).min_pres > ce.max_pres)
	ce.max_pres=fd(n).min_pres;
    if (fd(n).max_wind < ce.min_speed)
	ce.min_speed=fd(n).max_wind;
    if (fd(n).max_wind > ce.max_speed)
	ce.max_speed=fd(n).max_wind;
    ce.nfixes++;
    if (!stageTable.found(ce.key,sentry)) {
	sentry.key=ce.key;
	sentry.data.reset(new metadata::FixML::StageEntry::Data);
	sentry.data->boxflags.initialize(361,180,0,0);
	if (fd(n).latitude == -90.) {
	  sentry.data->boxflags.spole=1;
	}
	else if (fd(n).latitude == 90.) {
	  sentry.data->boxflags.npole=1;
	}
	else {
	  convertLatLonToBox(1,fd(n).latitude,fd(n).longitude,l,k);
	  sentry.data->boxflags.flags[l-1][k]=1;
	  sentry.data->boxflags.flags[l-1][360]=1;
	}
	sentry.data->start=fd(n).datetime;
	sentry.data->end=fd(n).datetime;
	stageTable.insert(sentry);
    }
    else {
	if (fd(n).latitude == -90.) {
	  sentry.data->boxflags.spole=1;
	}
	else if (fd(n).latitude == 90.) {
	  sentry.data->boxflags.npole=1;
	}
	else {
	  convertLatLonToBox(1,fd(n).latitude,fd(n).longitude,l,k);
	  sentry.data->boxflags.flags[l-1][k]=1;
	  sentry.data->boxflags.flags[l-1][360]=1;
	}
	if (fd(n).datetime < sentry.data->start) {
	  sentry.data->start=fd(n).datetime;
	}
	if (fd(n).datetime > sentry.data->end) {
	  sentry.data->end=fd(n).datetime;
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
  fe.data->classificationList.emplace_back(ce);
  featureTable.insert(fe);
}

void processTCVitals(Cyclone *c)
{
  TCVitalsCyclone *tcvc=reinterpret_cast<TCVitalsCyclone *>(c);
  metadata::FixML::FeatureEntry fe;
  metadata::FixML::ClassificationEntry ce;
  Array<Cyclone::FixData> fd;

  fd=tcvc->getFixData();
  std::stringstream fe_key;
  fe_key << fd(0).datetime.getYear() << "-" << tcvc->getStormNumber() << tcvc->getBasinID();
  if (!featureTable.found(fe_key.str(),fe)) {
    fe.key=fe_key.str();
    fe.data.reset(new metadata::FixML::FeatureEntry::Data);
    fe.data->altID=tcvc->getID();
    featureTable.insert(fe);
  }
  ce.key="tropical";
  if (fe.data->classificationList.size() == 0) {
    ce.start_datetime=ce.end_datetime=fd(0).datetime;
    ce.start_lat=ce.end_lat=ce.min_lat=ce.max_lat=fd(0).latitude;
    ce.start_lon=ce.end_lon=ce.min_lon=ce.max_lon=fd(0).longitude;
    ce.min_pres=ce.max_pres=fd(0).min_pres;
    ce.pres_units="mbar";
    ce.min_speed=ce.max_speed=fd(0).max_wind;
    ce.wind_units="m s-1";
    ce.nfixes=1;
    fe.data->classificationList.emplace_back(ce);
  }
  else {
    metadata::FixML::ClassificationEntry &b=fe.data->classificationList.back();
    if (fd(0).datetime < b.start_datetime) {
	b.start_datetime=fd(0).datetime;
	b.start_lat=fd(0).latitude;
	b.start_lon=fd(0).longitude;
    }
    if (fd(0).datetime > b.end_datetime) {
	fe.data->altID=tcvc->getID();
	b.end_datetime=fd(0).datetime;
	b.end_lat=fd(0).latitude;
	b.end_lon=fd(0).longitude;
    }
    if (fd(0).latitude < b.min_lat) {
	b.min_lat=fd(0).latitude;
    }
    if (fd(0).latitude > b.max_lat) {
	b.max_lat=fd(0).latitude;
    }
    if (fd(0).longitude < b.min_lon) {
	b.min_lon=fd(0).longitude;
    }
    if (fd(0).longitude > b.max_lon) {
	b.max_lon=fd(0).longitude;
    }
    if (fd(0).min_pres < b.min_pres) {
	b.min_pres=fd(0).min_pres;
    }
    if (fd(0).min_pres > b.max_pres) {
	b.max_pres=fd(0).min_pres;
    }
    if (fd(0).max_wind < b.min_speed) {
	b.min_speed=fd(0).max_wind;
    }
    if (fd(0).max_wind > b.max_speed) {
	b.max_speed=fd(0).max_wind;
    }
    ++b.nfixes;
  }
  if (!stageTable.found(ce.key,sentry)) {
    sentry.key=ce.key;
    sentry.data.reset(new metadata::FixML::StageEntry::Data);
    sentry.data->boxflags.initialize(361,180,0,0);
    if (fd(0).latitude == -90.) {
	sentry.data->boxflags.spole=1;
    }
    else if (fd(0).latitude == 90.) {
	sentry.data->boxflags.npole=1;
    }
    else {
	size_t l,k;
	convertLatLonToBox(1,fd(0).latitude,fd(0).longitude,l,k);
	sentry.data->boxflags.flags[l-1][k]=1;
	sentry.data->boxflags.flags[l-1][360]=1;
    }
    sentry.data->start=fd(0).datetime;
    sentry.data->end=fd(0).datetime;
    stageTable.insert(sentry);
  }
  else {
    if (fd(0).latitude == -90.) {
	sentry.data->boxflags.spole=1;
    }
    else if (fd(0).latitude == 90.) {
	sentry.data->boxflags.npole=1;
    }
    else {
	size_t l,k;
	convertLatLonToBox(1,fd(0).latitude,fd(0).longitude,l,k);
	sentry.data->boxflags.flags[l-1][k]=1;
	sentry.data->boxflags.flags[l-1][360]=1;
    }
    if (fd(0).datetime < sentry.data->start) {
	sentry.data->start=fd(0).datetime;
    }
    if (fd(0).datetime > sentry.data->end) {
	sentry.data->end=fd(0).datetime;
    }
  }
}

void processCXML(XMLDocument& xdoc)
{
  XMLElement e;
  std::list<XMLElement> elist,disturbanceList,fixList;
  metadata::FixML::FeatureEntry fe;
  metadata::FixML::ClassificationEntry ce;
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
  if (e.name().length() > 0) {
    src+="_"+e.content();
  }
  elist=xdoc.element_list("cxml/data");
  for (auto& ele : elist) {
    disturbanceList=ele.element_list("disturbance");
    for (auto& dist : disturbanceList) {
	fixList=dist.element_list("fix");
	ce.nfixes=fixList.size();
	if (ce.nfixes > 0) {
	  fe.key=dist.attribute_value("ID");
	  e=dist.element("cycloneName");
	  if (e.content().length() > 0) {
	    ce.key="tropical";
	    fe.key+="_"+e.content();
	  }
	  else {
	    ce.key="extratropical";
	  }
	  if (!featureTable.found(fe.key,fe)) {
	    fe.data.reset(new metadata::FixML::FeatureEntry::Data);
	    featureTable.insert(fe);
	  }
	  sdum=ele.attribute_value("type");
	  ce.src=src+"_"+sdum;
	  sdum=ele.attribute_value("member");
	  if (sdum.length() > 0) {
	    ce.src+="_member_"+sdum;
	  }
	  ce.start_datetime.setYear(1000);
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
	  for (auto& fix : fixList) {
	    e=fix.element("validTime");
	    if (e.name().length() > 0) {
		sdum=e.content();
		strutils::replace_all(sdum,"-","");
		strutils::replace_all(sdum,"T","");
		strutils::replace_all(sdum,":","");
		strutils::replace_all(sdum,"Z","");
		if (ce.start_datetime.getYear() == 1000) {
		  ce.start_datetime.set(std::stoll(sdum));
		}
		ce.end_datetime.set(std::stoll(sdum));
	    }
	    lat=-99.;
	    lon=-199.;
	    e=fix.element("latitude");
	    if (e.name().length() > 0) {
		sdum=e.content();
		strutils::trim(sdum);
		if (sdum.length() > 0 && strutils::is_numeric(sdum)) {
		  lat=std::stof(sdum);
		  sdum=e.attribute_value("units");
		  if (sdum == "deg S") {
		    lat=-lat;
		  }
		}
		else {
		  std::cerr << "Terminating - processCXML found an empty latitude element" << std::endl;
		  exit(1);
		}
	    }
	    e=fix.element("longitude");
	    if (e.name().length() > 0) {
		sdum=e.content();
		strutils::trim(sdum);
		if (sdum.length() > 0 && strutils::is_numeric(sdum)) {
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
		  std::cerr << "Terminating - processCXML found an empty longitude element" << std::endl;
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
		if (!stageTable.found(ce.key,sentry)) {
		  sentry.key=ce.key;
		  sentry.data.reset(new metadata::FixML::StageEntry::Data);
		  sentry.data->boxflags.initialize(361,180,0,0);
		  if (lat == -90.) {
		    sentry.data->boxflags.spole=1;
		  }
		  else if (lat == 90.) {
		    sentry.data->boxflags.npole=1;
		  }
		  else {
		    convertLatLonToBox(1,lat,lon,l,k);
		    sentry.data->boxflags.flags[l-1][k]=1;
		    sentry.data->boxflags.flags[l-1][360]=1;
		  }
		  sentry.data->start=ce.end_datetime;
		  sentry.data->end=ce.end_datetime;
		  stageTable.insert(sentry);
		}
		else {
		  if (lat == -90.) {
		    sentry.data->boxflags.spole=1;
		  }
		  else if (lat == 90.) {
		    sentry.data->boxflags.npole=1;
		  }
		  else {
		    convertLatLonToBox(1,lat,lon,l,k);
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
	    if (e.name().length() > 0 && strutils::is_numeric(e.content())) {
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
	    if (e.name().length() > 0 && strutils::is_numeric(e.content())) {
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
	  fe.data->classificationList.emplace_back(ce);
	}
    }
  }
}

void scanFile()
{
  idstream *istream=NULL;
  Cyclone *c=NULL;
  XMLDocument xdoc;
  std::string file_format,error;
  const size_t BUF_LEN=80000;
  unsigned char buffer[BUF_LEN];
  int status;
  std::list<std::string> filelist;

  tfile=new TempFile;
  tdir=new TempDir;
  tfile->open(args.temp_loc);
  tdir->create(args.temp_loc);
  if (args.format == "cxml") {
    if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,&filelist,file_format,error)) {
	metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","fix2xml",user,args.argsString);
    }
    if (filelist.size() == 0) {
	filelist.emplace_back(tfile->name());
    }
    for (const auto& file : filelist) {
	if (!xdoc.open(file)) {
	  if (args.dsnum != "330.3")
	    std::cerr << "Error: scanFile was unable to parse " << file << std::endl;
	  exit(1);
	}
	processCXML(xdoc);
	xdoc.close();
    }
  }
  else if (args.format == "tcvitals") {
    if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,&filelist,file_format,error)) {
	metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","fix2xml",user,args.argsString);
    }
    if (filelist.size() == 0) {
	filelist.emplace_back(tfile->name());
    }
    istream=new InputTCVitalsCycloneStream;
    c=new TCVitalsCyclone;
    for (const auto& file : filelist) {
	if (!primaryMetadata::openFileForMetadataScanning(istream,file,error)) {
	  metautils::logError("openFileForMetadataScanning() returned '"+error+"'","fix2xml",user,args.argsString);
	}
	while ( (status=istream->read(buffer,BUF_LEN)) > 0) {
	  c->fill(buffer,Cyclone::full_report);
	  processTCVitals(c);
	}
	istream->close();
	if (status == bfstream::error) {
	  metautils::logError("read error","fix2xml",user,args.argsString);
	}
    }
  }
  else {
    if (args.format == "hurdat") {
	istream=new InputHURDATCycloneStream;
	c=new HURDATCyclone;
    }
    else {
	metautils::logError("format "+args.format+" not recognized","fix2xml",user,args.argsString);
    }
    if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,nullptr,file_format,error)) {
	metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","fix2xml",user,args.argsString);
    }
    if (!primaryMetadata::openFileForMetadataScanning(istream,tfile->name(),error)) {
	metautils::logError("openFileForMetadataScanning() returned '"+error+"'","fix2xml",user,args.argsString);
    }
    if (istream != NULL) {
	while ( (status=istream->read(buffer,BUF_LEN)) > 0) {
	  c->fill(buffer,Cyclone::full_report);
	  if (args.format == "hurdat") {
	    processHURDAT(c);
	  }
	}
	istream->close();
	if (status == bfstream::error) {
	  metautils::logError("read error","fix2xml",user,args.argsString);
	}
    }
  }
  if (featureTable.size() == 0) {
    if (args.dsnum != "330.3") {
	std::cerr << "Error: scanFile returned: no fix data found in file" << std::endl;
    }
    exit(1);
  }
}

extern "C" void segv_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
  metautils::logError("core dump","fix2xml",user,args.argsString);
}

extern "C" void int_handler(int)
{
  cleanUp();
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
    std::cerr << "            - MSS paths must begin with /FS/DSS or /DSS" << std::endl;
    std::cerr << "            - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.argsString=getUnixArgsString(argc,argv,'%');
  metautils::readConfig("fix2xml",user,args.argsString);
  parseArgs();
  std::string flags="-f";
  if (strutils::has_beginning(args.path,"https://rda.ucar.edu")) {
    flags="-wf";
  }
  atexit(cleanUp);
  metautils::cmd_register("fix2xml",user);
  if (!args.overwriteOnly) {
    metautils::checkForExistingCMD("FixML");
  }
  scanFile();
  metadata::FixML::writeFixML(featureTable,stageTable,"fix2xml",user);
  if (args.updateDB) {
    if (!args.updateGraphics) {
	flags="-G "+flags;
    }
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    std::stringstream oss,ess;
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+".FixML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
}
