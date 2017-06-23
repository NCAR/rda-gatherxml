#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <signal.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <list>
#include <deque>
#include <vector>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <netcdf.hpp>
#include <MySQL.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
my::map<metadata::GrML::GridEntry> grid_table;
const std::string user=getenv("USER");
TempFile *tfile=nullptr;
std::string tfile_name;
TempDir *tdir=nullptr;
TempFile *inv_file=nullptr;
std::ofstream inv;
std::string myerror="";
std::string mywarning="";
const char *grib1_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour"};
const int grib1_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24};
const int grib1_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12};
const char *grib2_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour","second"};
const int grib2_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24,86400};
const int grib2_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12,1};

void parseArgs()
{
  std::deque<std::string> sp;
  size_t n;

  args.updateDB=true;
  args.updateSummary=true;
  args.overridePrimaryCheck=false;
  args.overwriteOnly=false;
  args.regenerate=true;
  args.temp_loc=directives.tempPath;
  sp=strutils::split(args.argsString,"!");
  for (n=0; n < sp.size()-1; n++) {
    if (sp[n] == "-f") {
	args.format=sp[++n];
    }
    else if (sp[n] == "-d") {
	args.dsnum=sp[++n];
	if (strutils::has_beginning(args.dsnum,"ds")) {
	  args.dsnum=args.dsnum.substr(2);
	}
    }
    else if (sp[n] == "-l") {
	args.local_name=sp[++n];
    }
    else if (sp[n] == "-m") {
        args.member_name=sp[++n];
    }
    else if (sp[n] == "-I") {
	args.inventoryOnly=true;
	args.updateDB=false;
    }
    else if (sp[n] == "-S") {
	args.updateSummary=false;
    }
    else if (sp[n] == "-U") {
	if (user == "dattore") {
	  args.updateDB=false;
	}
    }
    else if (sp[n] == "-NC") {
	if (user == "dattore") {
	  args.overridePrimaryCheck=true;
	}
    }
    else if (sp[n] == "-OO") {
	if (user == "dattore") {
	  args.overwriteOnly=true;
	}
    }
    else if (sp[n] == "-R") {
	args.regenerate=false;
    }
    else if (sp[n] == "-t") {
	args.temp_loc=sp[++n];
    }
  }
  if (args.format.length() == 0) {
    std::cerr << "no format specified" << std::endl;
    exit(1);
  }
  else {
    args.format=strutils::to_lower(args.format);
  }
  if (args.format == "grib1") {
    args.format="grib";
  }
  if (args.dsnum.length() == 0) {
    std::cerr << "no dataset number specified" << std::endl;
    exit(1);
  }
  if (args.dsnum == "999.9") {
    args.overridePrimaryCheck=true;
    args.updateDB=false;
    args.updateSummary=false;
    args.regenerate=false;
  }
  n=sp[sp.size()-1].rfind("/");
  args.path=sp[sp.size()-1].substr(0,n);
  args.filename=sp[sp.size()-1].substr(n+1);
}

extern "C" void cleanUp()
{
  std::stringstream oss,ess;

  if (tfile != nullptr) {
    delete tfile;
  }
// remove temporary file that was sym-linked because the data file name contains
//  metadata
  if (args.format == "cmorph025" || args.format == "cmorph8km") {
    mysystem2("/bin/rm "+tfile_name,oss,ess);
  }
  if (tdir != nullptr) {
    delete tdir;
  }
  if (myerror.length() > 0) {
    metautils::logError(myerror,"grid2xml",user,args.argsString);
  }
}

struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};

void scanFile()
{
  idstream *istream=nullptr;
  Grid *grid=nullptr;
  GRIBMessage *message=nullptr;
  Grid::GridDimensions griddim;
  Grid::GridDefinition griddef;
  int BUF_LEN=0;
  unsigned char *buffer=nullptr;
  int n;
  std::string file_format,units,levelType;
  const char *pole[]={"N","S"};
  DateTime firstValidDateTime,lastValidDateTime;
  size_t fcst_hr;
  metautils::StringEntry pe,ee;
  Grid::EnsembleData ensdata;
  size_t ngrids=0;
  std::vector<GRIB2Grid::StatisticalProcessRange> spranges;
  DateTime sp_end;
  std::stringstream oss,ess;
  std::string error,sdum,sdum2;
  int bytes_read,idx;
  metadata::GrML::GridEntry gentry;
  metadata::GrML::LevelEntry lentry;
  metadata::GrML::ParameterEntry pentry;
  my::map<InvEntry> inv_U_table,inv_G_table,inv_L_table,inv_P_table,inv_R_table,inv_E_table;
  std::list<std::string> inv_U_list,inv_G_list,inv_L_list,inv_P_list,inv_R_list,inv_E_list;
  InvEntry ie;
  std::list<std::string> inv_lines;
  xmlutils::LevelMapper level_mapper;

  if (args.format == "grib" || args.format == "grib0") {
    istream=new InputGRIBStream;
    message=new GRIBMessage;
    grid=new GRIBGrid;
  }
  else if (args.format == "grib2") {
    istream=new InputGRIBStream;
    message=new GRIB2Message;
    grid=new GRIB2Grid;
  }
  else if (args.format == "jraieeemm") {
    istream=new InputJRAIEEEMMGridStream;
    grid=new JRAIEEEMMGrid;
  }
  else if (args.format == "oct") {
    istream=new InputOctagonalGridStream;
    grid=new OctagonalGrid;
  }
  else if (args.format == "tropical") {
    istream=new InputTropicalGridStream;
    grid=new TropicalGrid;
  }
  else if (args.format == "ll") {
    istream=new InputLatLonGridStream;
    grid=new LatLonGrid;
  }
  else if (args.format == "slp") {
    istream=new InputSLPGridStream;
    grid=new SLPGrid;
  }
  else if (args.format == "navy") {
    istream=new InputNavyGridStream;
    grid=new NavyGrid;
  }
  else if (args.format == "ussrslp") {
    istream=new InputUSSRSLPGridStream;
    grid=new USSRSLPGrid;
  }
  else if (args.format == "on84") {
    istream=new InputON84GridStream;
    grid=new ON84Grid;
  }
  else if (args.format == "noaaoi2") {
    istream=new InputNOAAOI2SSTGridStream;
    grid=new NOAAOI2SSTGrid;
  }
  else if (args.format == "cgcm1") {
    istream=new InputCGCM1GridStream;
    grid=new CGCM1Grid;
  }
  else if (args.format == "cmorph025") {
    istream=new InputCMORPH025GridStream;
    grid=new CMORPH025Grid;
  }
  else if (args.format == "cmorph8km") {
    istream=new InputCMORPH8kmGridStream;
    grid=new CMORPH8kmGrid;
  }
  else if (args.format == "gpcp") {
    istream=new InputGPCPGridStream;
    grid=new GPCPGrid;
  }
  else {
    metautils::logError(args.format+"-formatted files not recognized","grid2xml",user,args.argsString);
  }
  tfile=new TempFile;
  tdir=new TempDir;
  if (args.format == "jraieeemm") {
    tfile->open(args.temp_loc,"."+args.filename);
    tfile_name=tfile->name();
  }
  else {
    tfile->open(args.temp_loc);
// symlink the temporary file when the data file name contains metadata
    if (args.format == "cmorph025" || args.format == "cmorph8km") {
	tfile_name= (args.local_name.length() > 0) ? args.local_name : args.filename;
	size_t idx;
	if ( (idx=tfile_name.rfind("/")) != std::string::npos) {
	  tfile_name=tfile_name.substr(idx+1);
	}
	idx=(tfile->name()).rfind("/");
	tfile_name=(tfile->name()).substr(0,idx+1)+tfile_name;
	if (mysystem2("/bin/ln -s "+tfile->name()+" "+tfile_name,oss,ess) < 0) {
	  metautils::logError("unable to sym-link to temporary file - "+ess.str(),"grid2xml",user,args.argsString);
	}
    }
    else {
	tfile_name=tfile->name();
    }
  }
  tdir->create(args.temp_loc);
  if (!primaryMetadata::prepareFileForMetadataScanning(*tfile,*tdir,NULL,file_format,error)) {
    metautils::logError("prepareFileForMetadataScanning() returned '"+error+"'","grid2xml",user,args.argsString);
  }
  if (!primaryMetadata::openFileForMetadataScanning(istream,tfile_name,error)) {
    if (error.length() > 0) {
	metautils::logError("openFileForMetadataScanning() returned '"+error+"'","grid2xml",user,args.argsString);
    }
    else {
	metautils::logError("openFileForMetadataScanning(): unable to open file","grid2xml",user,args.argsString);
    }
  }
  if ((file_format.length() == 0 || file_format == "TAR") && (args.format == "grib" || args.format == "grib0" || args.format == "grib2")) {
    metadata::openInventory(inv,&inv_file,"grid2xml",user);
  }
  else if (args.inventoryOnly) {
    metautils::logError("scanFile returned error: unable to inventory "+args.path+"/"+args.filename+" because archive format is '"+file_format+"'","grid2xml",user,args.argsString);
  }
  while (1) {
    if ((args.format != "grib" && args.format != "grib2") || ngrids == message->getNumberOfGrids()) {
	if ( (bytes_read=istream->peek()) < 0) {
	  if (bytes_read == bfstream::error) {
	    metautils::logError("An error occurred while reading the data file - no content metadata was generated","grid2xml",user,args.argsString);
	    exit(1);
	  }
	  else {
	    break;
	  }
	}
	if (bytes_read > BUF_LEN) {
	  if (buffer != nullptr) {
	    delete[] buffer;
	  }
	  BUF_LEN=bytes_read;
	  buffer=new unsigned char[BUF_LEN];
	}
	istream->read(buffer,BUF_LEN);
	ngrids=0;
    }
    if (args.format == "grib" || args.format == "grib2") {
	if (ngrids == 0) {
	  message->fill(buffer,true);
	  while (message->getNumberOfGrids() == 0) {
	    if ( (bytes_read=istream->peek()) < 0) {
		if (bytes_read == bfstream::error) {
		  metautils::logError("An error occurred while reading the data file - no content metadata was generated","grid2xml",user,args.argsString);
		  exit(1);
		}
		else
		  break;
	    }
	    if (bytes_read > BUF_LEN) {
		if (buffer != nullptr)
		  delete[] buffer;
		BUF_LEN=bytes_read;
	    }
	    istream->read(buffer,BUF_LEN);
	    message->fill(buffer,true);
	  }
	  if (message->getNumberOfGrids() == 0) {
	    break;
	  }
	}
	grid=message->getGrid(ngrids);
	++ngrids;
    }
    else {
	if (args.format == "cmorph025") {
	  reinterpret_cast<CMORPH025Grid *>(grid)->setDateTime(reinterpret_cast<InputCMORPH025GridStream *>(istream)->getDateTime());
	  reinterpret_cast<CMORPH025Grid *>(grid)->setLatitudes(reinterpret_cast<InputCMORPH025GridStream *>(istream)->getStartLatitude(),reinterpret_cast<InputCMORPH025GridStream *>(istream)->getEndLatitude());
	}
	else if (args.format == "gpcp") {
	  grid->fill(buffer,Grid::fullGrid);
	}
	else {
	  grid->fill(buffer,Grid::headerOnly);
	}
    }
    griddef=grid->getDefinition();
    if (args.format == "gpcp" && reinterpret_cast<GPCPGrid *>(grid)->isEmptyGrid()) {
	griddef.type=0;
    }
    if (griddef.type > 0) {
	firstValidDateTime=grid->getValidDateTime();
	gentry.key=strutils::itos(griddef.type)+"<!>";
	if (griddef.type != Grid::sphericalHarmonicsType) {
	  griddim=grid->getDimensions();
	  gentry.key+=strutils::itos(griddim.x)+"<!>"+strutils::itos(griddim.y)+"<!>"+strutils::ftos(griddef.slatitude,3)+"<!>"+strutils::ftos(griddef.slongitude,3)+"<!>"+strutils::ftos(griddef.elatitude,3)+"<!>"+strutils::ftos(griddef.elongitude,3)+"<!>"+strutils::ftos(griddef.loincrement,3)+"<!>";
	  switch (griddef.type) {
	    case Grid::gaussianLatitudeLongitudeType:
		gentry.key+=strutils::itos(griddef.num_circles);
		break;
	    default:
		gentry.key+=strutils::ftos(griddef.laincrement,3);
	  }
	  if (griddef.type == Grid::polarStereographicType || griddef.type == Grid::lambertConformalType) {
	    n= (griddef.projection_flag ==  0) ? 0 : 1;
	    gentry.key+=std::string("<!>")+pole[n];
	    if (griddef.type == Grid::lambertConformalType) {
		gentry.key+="<!>"+strutils::ftos(griddef.stdparallel1,3)+"<!>"+strutils::ftos(griddef.stdparallel2);
	    }
	  }
	  else if (griddef.type == Grid::mercatorType) {
	    gentry.key+="<!>"+strutils::ftos(griddef.stdparallel1,3);
	  }
	}
	else {
	  gentry.key+=strutils::itos(griddef.trunc1)+"<!>"+strutils::itos(griddef.trunc2)+"<!>"+strutils::itos(griddef.trunc3);
	}
	pe.key="";
	ee.key="";
	units="";
	fcst_hr=grid->getForecastTime()/10000;
	if (args.format == "grib" || args.format == "grib0") {
	  if (message->getEdition() == 0) {
	    args.format="grib0";
	  }
	  lastValidDateTime=firstValidDateTime;
	  lentry.key=strutils::ftos(grid->getSource())+"-"+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->getSubCenterID());
	  pentry.key=lentry.key;
	  if (args.format == "grib") {
	    pentry.key+="."+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->getParameterTableCode());
	  }
	  pentry.key+=":"+strutils::ftos(grid->getParameter());
	  gentry.key+="<!>"+(reinterpret_cast<GRIBGrid *>(grid))->getProductDescription();
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->getProcess());
	  ensdata=grid->getEnsembleData();
	  if (ensdata.fcst_type.length() > 0) {
	    ee.key=ensdata.fcst_type+"<!>"+pe.key;
	    if (ensdata.ID.length() > 0)
		ee.key+="%"+ensdata.ID;
	    if (ensdata.total_size > 0)
		ee.key+="<!>"+strutils::itos(ensdata.total_size);
	  }
	  levelType=strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->getFirstLevelType());
	  if (args.format == "grib" || args.format == "grib1") {
	    sdum="WMO_GRIB1";
	  }
	  else {
	    sdum="WMO_GRIB0";
	  }
	  if (level_mapper.levelIsLayer(sdum,levelType,lentry.key) < 0) {
	    metautils::logError("no entry for "+levelType+", '"+lentry.key+"' in level map on "+grid->getValidDateTime().toString(),"grid2xml",user,args.argsString);
	  }
	  lentry.key+=",";
	  if (level_mapper.levelIsLayer(sdum,levelType,lentry.key)) {
	    lentry.key+=levelType+":"+strutils::ftos(grid->getFirstLevelValue(),5)+":"+strutils::ftos(grid->getSecondLevelValue(),5);
	  }
	  else {
	    lentry.key+=levelType+":"+strutils::ftos(grid->getFirstLevelValue(),5);
	  }
	  idx=gentry.key.rfind("<!>");
	  if (inv.is_open()) {
	    sdum=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream))->getCurrentRecordOffset())+"|"+strutils::itos(message->length())+"|"+grid->getValidDateTime().toString("%Y%m%d%H%MM");
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.push_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.push_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    inv_lines.push_back(sdum);
	  }
	}
	else if (args.format == "grib2") {
/*
	  lastValidDateTime=firstValidDateTime;
firstValidDateTime=grid->getReferenceDateTime().hoursAdded(grid->getForecastTime()/10000);
*/
	  lentry.key=strutils::ftos(grid->getSource())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->getSubCenterID());
	  pentry.key=lentry.key+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->getParameterTableCode())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->getLocalTableCode())+":"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->getDiscipline())+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->getParameterCategory())+"."+strutils::ftos(grid->getParameter());
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->getProcess())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->getBackgroundProcess())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->getForecastProcess());
	  levelType=strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->getFirstLevelType());
	  n=(reinterpret_cast<GRIB2Grid *>(grid))->getSecondLevelType();
	  lentry.key+=",";
	  if (n != 255) {
	    lentry.key+=levelType+"-"+strutils::itos(n)+":";
	    if (!myequalf(grid->getFirstLevelValue(),Grid::missingValue)) {
		lentry.key+=strutils::ftos(grid->getFirstLevelValue(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	    lentry.key+=":";
	    if (!myequalf(grid->getSecondLevelValue(),Grid::missingValue)) {
		lentry.key+=strutils::ftos(grid->getSecondLevelValue(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	  }
	  else {
	    lentry.key+=levelType+":";
	    if (!myequalf(grid->getFirstLevelValue(),Grid::missingValue)) {
		lentry.key+=strutils::ftos(grid->getFirstLevelValue(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	  }
	  short ptype=(reinterpret_cast<GRIB2Grid *>(grid))->getProductType();
	  gentry.key+="<!>"+(reinterpret_cast<GRIB2Grid *>(grid))->getProductDescription();
	  firstValidDateTime=grid->getForecastDateTime();
	  lastValidDateTime=grid->getValidDateTime();
	  switch (ptype) {
	    case 0:
	    case 1:
	    case 2:
		if (ptype == 1) {
		  ensdata=grid->getEnsembleData();
		  if (ensdata.fcst_type.length() > 0) {
		    ee.key=ensdata.fcst_type+"<!>"+pe.key;
		    if (ensdata.ID.length() > 0) {
			ee.key+="%"+ensdata.ID;
		    }
		    if (ensdata.total_size > 0) {
			ee.key+="<!>"+strutils::itos(ensdata.total_size);
		    }
		  }
		}
		break;
	    case 8:
	    case 11:
	    case 12:
		if (ptype == 11 || ptype == 12) {
		  ensdata=grid->getEnsembleData();
		  if (ensdata.fcst_type.length() > 0) {
		    ee.key=ensdata.fcst_type+"<!>"+pe.key;
		    if (ensdata.ID.length() > 0)
			ee.key+="%"+ensdata.ID;
		    if (ensdata.total_size > 0)
			ee.key+="<!>"+strutils::itos(ensdata.total_size);
		  }
		}
		break;
	  }
	  idx=gentry.key.rfind("<!>");
	  if (inv.is_open()) {
//	    sdum=strutils::lltos(((InputGRIBStream *)istream)->getCurrentRecordOffset())+"|"+strutils::lltos(message->length())+"|"+grid->getValidDateTime().toString("%Y%m%d%H%MM");
sdum=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream))->getCurrentRecordOffset())+"|"+strutils::lltos(message->length())+"|"+lastValidDateTime.toString("%Y%m%d%H%MM");
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.push_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.push_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.push_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    inv_lines.push_back(sdum);
	  }
	}
	else if (args.format == "oct" || args.format == "tropical" || args.format == "ll" || args.format == "slp" || args.format == "navy") {
	  lastValidDateTime=firstValidDateTime;
	  pentry.key=strutils::itos(grid->getParameter());
	  if (fcst_hr == 0) {
	    if (firstValidDateTime.getDay() == 0) {
		firstValidDateTime.addDays(1);
		lastValidDateTime.addMonths(1);
		gentry.key+="<!>Monthly Mean of Analyses";
	    }
	    else
		gentry.key+="<!>Analysis";
	  }
	  else {
	    if (firstValidDateTime.getDay() == 0) {
		firstValidDateTime.addDays(1);
	   	lastValidDateTime.addMonths(1);
		gentry.key+="<!>Monthly Mean of "+strutils::itos(fcst_hr)+"-hour Forecasts";
	    }
	    else {
		gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	    }
	    firstValidDateTime.addHours(fcst_hr);
	  }
	  if (myequalf(grid->getSecondLevelValue(),0.)) {
	    lentry.key="0:"+strutils::itos(grid->getFirstLevelValue());
	  }
	  else {
	    lentry.key="1:"+strutils::itos(grid->getFirstLevelValue())+":"+strutils::itos(grid->getSecondLevelValue());
	  }
	}
	else if (args.format == "jraieeemm") {
	  lastValidDateTime=firstValidDateTime;
	  lastValidDateTime.setDay(getDaysInMonth(firstValidDateTime.getYear(),firstValidDateTime.getMonth()));
	  lastValidDateTime.addHours(18);
	  pentry.key=strutils::itos(grid->getParameter());
	  gentry.key+="<!>Monthly Mean (4 per day) of ";
	  if (grid->getForecastTime() == 0) {
	    gentry.key+="Analyses";
	  }
	  else {
	    gentry.key+="Forecasts";
	    sdum=(reinterpret_cast<JRAIEEEMMGrid *>(grid))->getTimeRange();
	    if (sdum == "MN  ") {
		gentry.key+=" of 6-hour Average";
	    }
	    else if (sdum == "AC  ") {
		gentry.key+=" of 6-hour Accumulation";
	    }
	    else if (sdum == "MAX ") {
		gentry.key+=" of 6-hour Maximum";
	    }
	    else if (sdum == "MIN ") {
		gentry.key+=" of 6-hour Minimum";
	    }
	  }
	  lentry.key=strutils::itos(grid->getFirstLevelType())+":"+strutils::ftos(grid->getFirstLevelValue(),5);
	}
	else if (args.format == "ussrslp") {
	  pentry.key=strutils::itos(grid->getParameter());
	  if (fcst_hr == 0)
	    gentry.key+="<!>Analysis";
	  else
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	}
	else if (args.format == "on84") {
	  lastValidDateTime=firstValidDateTime;
	  pentry.key=strutils::itos(grid->getParameter());
	  switch ((reinterpret_cast<ON84Grid *>(grid))->getTimeMarker()) {
	    case 0:
		gentry.key+="<!>Analysis";
		break;
	    case 3:
		fcst_hr-=(reinterpret_cast<ON84Grid *>(grid))->getF2();
		gentry.key+="<!>";
		if (fcst_hr > 0)
		  gentry.key+=strutils::itos(fcst_hr+(reinterpret_cast<ON84Grid *>(grid))->getF2())+"-hour Forecast of ";
		gentry.key+=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getF2())+"-hour Accumulation";
		break;
	    default:
		metautils::logError("ON84 time marker "+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getTimeMarker())+" not recognized","grid2xml",user,args.argsString);
	  }
	  pe.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getRunMarker())+"."+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getGeneratingProgram());
	  if (myequalf(grid->getSecondLevelValue(),0.)) {
	    switch ((reinterpret_cast<ON84Grid *>(grid))->getFirstLevelType()) {
		case 144:
		case 145:
		case 146:
		case 147:
		case 148:
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getFirstLevelType())+":1:0";
		  break;
		default:
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getFirstLevelType())+":"+strutils::ftos(grid->getFirstLevelValue(),5);
	    }
	  }
	  else {
	    lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->getFirstLevelType())+":"+strutils::ftos(grid->getFirstLevelValue(),5)+":"+strutils::ftos(grid->getSecondLevelValue(),5);
	  }
	}
	else if (args.format == "cgcm1") {
	  pentry.key=(reinterpret_cast<CGCM1Grid *>(grid))->getParameterName();
	  strutils::trim(pentry.key);
	  strutils::replace_all(pentry.key,"\"","&quot;");
	  lastValidDateTime=firstValidDateTime;
	  if (fcst_hr == 0) {
	    if (firstValidDateTime.getDay() > 0)
		gentry.key+="<!>Analysis";
	    else {
		gentry.key+="<!>Monthly Mean of Analyses";
		firstValidDateTime.setDay(1);
		lastValidDateTime.setDay(getDaysInMonth(lastValidDateTime.getYear(),lastValidDateTime.getMonth()));
	    }
	  }
	  else {
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	  }
	  lentry.key=strutils::itos(grid->getFirstLevelValue());
	}
	else if (args.format == "cmorph025") {
	  pentry.key=(reinterpret_cast<InputCMORPH025GridStream *>(istream))->getParameterCode();
	  strutils::trim(pentry.key);
	  lastValidDateTime=firstValidDateTime;
	  gentry.key+="<!>Analysis";
	  lentry.key="surface";
	}
	else if (args.format == "cmorph8km") {
	  pentry.key="CMORPH";
	  lastValidDateTime=firstValidDateTime;
	  firstValidDateTime=grid->getReferenceDateTime();
	  gentry.key+="<!>30-minute Average (initial+0 to initial+30)";
	  lentry.key="surface";
	}
	else if (args.format == "gpcp") {
	  pentry.key=reinterpret_cast<GPCPGrid *>(grid)->parameter_name();
	  lastValidDateTime=firstValidDateTime;
	  firstValidDateTime=grid->getReferenceDateTime();
	  size_t hrs_since=lastValidDateTime.getHoursSince(firstValidDateTime);
	  if (hrs_since == 24) {
	    gentry.key+="<!>Daily Mean";
	  }
	  else if (hrs_since == getDaysInMonth(firstValidDateTime.getYear(),firstValidDateTime.getMonth())*24) {
	    gentry.key+="<!>Monthly Mean";
	  }
	  else {
	    metautils::logError("can't figure out gridded product type","grid2xml",user,args.argsString);
	  }
	  lentry.key="surface";
	}
	if (!grid_table.found(gentry.key,gentry)) {
	  gentry.level_table.clear();
	  lentry.units=units;
	  lentry.parameter_code_table.clear();
	  pentry.startDateTime=firstValidDateTime;
	  pentry.endDateTime=lastValidDateTime;
	  pentry.numTimeSteps=1;
	  lentry.parameter_code_table.insert(pentry);
	  gentry.level_table.insert(lentry);
	  gentry.process_table.clear();
	  if (pe.key.length() > 0) {
	    gentry.process_table.insert(pe);
	  }
	  gentry.ensemble_table.clear();
	  if (ee.key.length() > 0) {
	    gentry.ensemble_table.insert(ee);
	  }
	  grid_table.insert(gentry);
	}
	else {
	  if (!gentry.level_table.found(lentry.key,lentry)) {
	    lentry.units=units;
	    lentry.parameter_code_table.clear();
	    pentry.startDateTime=firstValidDateTime;
	    pentry.endDateTime=lastValidDateTime;
	    pentry.numTimeSteps=1;
	    lentry.parameter_code_table.insert(pentry);
	    gentry.level_table.insert(lentry);
	  }
	  else {
	    if (!lentry.parameter_code_table.found(pentry.key,pentry)) {
		pentry.startDateTime=firstValidDateTime;
		pentry.endDateTime=lastValidDateTime;
		pentry.numTimeSteps=1;
		lentry.parameter_code_table.insert(pentry);
	    }
	    else {
		if (firstValidDateTime < pentry.startDateTime) {
		  pentry.startDateTime=firstValidDateTime;
		}
		if (lastValidDateTime > pentry.endDateTime) {
		  pentry.endDateTime=lastValidDateTime;
		}
		pentry.numTimeSteps++;
		lentry.parameter_code_table.replace(pentry);
	    }
	    gentry.level_table.replace(lentry);
	  }
	  if (pe.key.length() > 0 && !gentry.process_table.found(pe.key,pe)) {
	    gentry.process_table.insert(pe);
	  }
	  if (ee.key.length() > 0 && !gentry.ensemble_table.found(ee.key,ee)) {
	    gentry.ensemble_table.insert(ee);
	  }
	  grid_table.replace(gentry);
	}
    }
  }
  istream->close();
  if (grid_table.size() == 0) {
    std::cerr << "Error: No grids found - no content metadata will be generated" << std::endl;
    exit(1);
  }
  if (inv_lines.size() > 0) {
    for (auto item : inv_U_list) {
	inv_U_table.found(item,ie);
	inv << "U<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_G_list) {
	inv_G_table.found(item,ie);
	inv << "G<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_L_list) {
	inv_L_table.found(item,ie);
	inv << "L<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_P_list) {
	inv_P_table.found(item,ie);
	inv << "P<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_R_list) {
	inv_R_table.found(item,ie);
	inv << "R<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_E_list) {
	inv_E_table.found(item,ie);
	inv << "E<!>" << ie.num << "<!>" << item << std::endl;
    }
    inv << "-----" << std::endl;
    for (auto line : inv_lines)
	inv << line << std::endl;
  }
}

extern "C" void segv_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
  metautils::logError("core dump","grid2xml",user,args.argsString);
}

extern "C" void int_handler(int)
{
  cleanUp();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  std::string web_home,flags;
  std::stringstream oss,ess;

  if (argc < 4) {
    std::cerr << "usage: grid2xml -f format -d [ds]nnn.n [options...] path" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required (choose one):" << std::endl;
    std::cerr << "-f cgcm1         Canadian CGCM1 Model ASCII format" << std::endl;
    std::cerr << "-f cmorph025     CPC Morphing Technique 0.25-degree precipitation format" << std::endl;
    std::cerr << "-f cmorph8km     CPC Morphing Technique 8-km precipitation format" << std::endl;
    std::cerr << "-f gpcp          GPCP grid format" << std::endl;
    std::cerr << "-f grib          GRIB0 and GRIB1 grid formats" << std::endl;
    std::cerr << "-f grib2         GRIB2 grid format" << std::endl;
    std::cerr << "-f jraieeemm     JRA IEEE Monthly Mean grid format" << std::endl;
    std::cerr << "-f ll            DSS 5-degree Lat/Lon grid format" << std::endl;
    std::cerr << "-f oct           DSS Octagonal grid format" << std::endl;
    std::cerr << "-f tropical      DSS Tropical grid format" << std::endl;
    std::cerr << "-f on84          NCEP Office Note 84 grid format" << std::endl;
    std::cerr << "-f navy          DSS Navy grid format" << std::endl;
//    std::cerr << "-f netcdf        netCDF format" << std::endl;
    std::cerr << "-f noaaoi2       NOAA OI2 SST format" << std::endl;
    std::cerr << "-f slp           DSS Sea-Level Pressure grid format" << std::endl;
    std::cerr << "-f ussrslp       USSR Sea-Level Pressure grid format" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    if (user == "dattore") {
	std::cerr << "-NC              don't check to see if the MSS file is a primary for the dataset" << std::endl;
    }
    std::cerr << "-l <local_name>  name of the MSS file on local disk (this avoids an MSS read)" << std::endl;
    std::cerr << "-r/-R            regenerate/don't regenerate the dataset webpage" << std::endl;
    std::cerr << "-s/-S            do/don't update the dataset summary information (default is -s)" << std::endl;
    if (user == "dattore") {
	std::cerr << "-u/-U            do/don't update the database (default is -u)" << std::endl;
	std::cerr << "-t <path>        path where temporary files should be created" << std::endl;
      std::cerr << "-I               inventory only; no content metadata generated" << std::endl;
      std::cerr << "-OO              overwrite only - when content metadata already exists, the" << std::endl;
      std::cerr << "                 default is to first delete existing metadata; this option saves" << std::endl;
      std::cerr << "                 time by overwriting without the delete" << std::endl;
    }
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>           full MSS path or URL of the file to read" << std::endl;
    std::cerr << "                 - MSS paths must begin with \"/FS/DSS\" or \"/DSS\"" << std::endl;
    std::cerr << "                 - URLs must begin with \"https://{rda|dss}.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  args.argsString=getUnixArgsString(argc,argv,'!');
  metautils::readConfig("grid2xml",user,args.argsString);
  parseArgs();
  flags="-f";
  if (!args.inventoryOnly && strutils::has_beginning(args.path,"https://rda.ucar.edu")) {
    flags="-wf";
  }
  atexit(cleanUp);
  metautils::cmd_register("grid2xml",user);
  if (!args.overwriteOnly && !args.inventoryOnly) {
    metautils::checkForExistingCMD("GrML");
  }
  scanFile();
  if (!args.inventoryOnly) {
    metadata::GrML::writeGrML(grid_table,"grid2xml",user);
  }
  if (args.updateDB) {
    if (!args.updateSummary) {
	flags="-S "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (mysystem2(directives.localRoot+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+".GrML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv.is_open()) {
    metadata::closeInventory(inv,inv_file,"GrML",true,args.updateSummary,"grid2xml",user);
  }
  return 0;
}
