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
std::string tfile_name,inv_file;
TempDir *tdir=nullptr,*inv_dir=nullptr;
std::ofstream inv_stream;
std::string myerror="";
std::string mywarning="";
const char *grib1_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour"};
const int grib1_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24};
const int grib1_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12};
const char *grib2_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour","second"};
const int grib2_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24,86400};
const int grib2_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12,1};

void parse_args()
{
  std::deque<std::string> sp;
  size_t n;

  args.update_DB=true;
  args.update_summary=true;
  args.override_primary_check=false;
  args.overwrite_only=false;
  args.regenerate=true;
  args.temp_loc=directives.temp_path;
  sp=strutils::split(args.args_string,"!");
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
	args.inventory_only=true;
	args.update_DB=false;
    }
    else if (sp[n] == "-S") {
	args.update_summary=false;
    }
    else if (sp[n] == "-U") {
	if (user == "dattore") {
	  args.update_DB=false;
	}
    }
    else if (sp[n] == "-NC") {
	if (user == "dattore") {
	  args.override_primary_check=true;
	}
    }
    else if (sp[n] == "-OO") {
	if (user == "dattore") {
	  args.overwrite_only=true;
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
    args.override_primary_check=true;
    args.update_DB=false;
    args.update_summary=false;
    args.regenerate=false;
  }
  n=sp[sp.size()-1].rfind("/");
  args.path=sp[sp.size()-1].substr(0,n);
  args.filename=sp[sp.size()-1].substr(n+1);
}

extern "C" void clean_up()
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
    metautils::log_error(myerror,"grid2xml",user,args.args_string);
  }
}

struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};

void scan_file()
{
  idstream *istream=nullptr;
  Grid *grid=nullptr;
  GRIBMessage *message=nullptr;
  Grid::GridDimensions griddim;
  Grid::GridDefinition griddef;
  int BUF_LEN=0;
  unsigned char *buffer=nullptr;
  int n;
  std::string file_format,units,level_type;
  const char *pole[]={"N","S"};
  DateTime first_valid_date_time,last_valid_date_time;
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
    metautils::log_error(args.format+"-formatted files not recognized","grid2xml",user,args.args_string);
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
	  metautils::log_error("unable to sym-link to temporary file - "+ess.str(),"grid2xml",user,args.args_string);
	}
    }
    else {
	tfile_name=tfile->name();
    }
  }
  tdir->create(args.temp_loc);
  if (!primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,NULL,file_format,error)) {
    metautils::log_error("prepare_file_for_metadata_scanning() returned '"+error+"'","grid2xml",user,args.args_string);
  }
  if (!primaryMetadata::open_file_for_metadata_scanning(istream,tfile_name,error)) {
    if (error.length() > 0) {
	metautils::log_error("open_file_for_metadata_scanning() returned '"+error+"'","grid2xml",user,args.args_string);
    }
    else {
	metautils::log_error("open_file_for_metadata_scanning(): unable to open file","grid2xml",user,args.args_string);
    }
  }
  if ((file_format.length() == 0 || file_format == "TAR") && (args.format == "grib" || args.format == "grib0" || args.format == "grib2")) {
    metadata::open_inventory(inv_file,&inv_dir,inv_stream,"GrML","grid2xml",user);
  }
  else if (args.inventory_only) {
    metautils::log_error("scan_file() returned error: unable to inventory "+args.path+"/"+args.filename+" because archive format is '"+file_format+"'","grid2xml",user,args.args_string);
  }
  while (1) {
    if ((args.format != "grib" && args.format != "grib2") || ngrids == message->number_of_grids()) {
	if ( (bytes_read=istream->peek()) < 0) {
	  if (bytes_read == bfstream::error) {
	    metautils::log_error("An error occurred while reading the data file - no content metadata was generated","grid2xml",user,args.args_string);
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
	  while (message->number_of_grids() == 0) {
	    if ( (bytes_read=istream->peek()) < 0) {
		if (bytes_read == bfstream::error) {
		  metautils::log_error("An error occurred while reading the data file - no content metadata was generated","grid2xml",user,args.args_string);
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
	  if (message->number_of_grids() == 0) {
	    break;
	  }
	}
	grid=message->grid(ngrids);
	++ngrids;
    }
    else {
	if (args.format == "cmorph025") {
	  reinterpret_cast<CMORPH025Grid *>(grid)->set_date_time(reinterpret_cast<InputCMORPH025GridStream *>(istream)->date_time());
	  reinterpret_cast<CMORPH025Grid *>(grid)->set_latitudes(reinterpret_cast<InputCMORPH025GridStream *>(istream)->start_latitude(),reinterpret_cast<InputCMORPH025GridStream *>(istream)->end_latitude());
	}
	else if (args.format == "gpcp") {
	  grid->fill(buffer,Grid::full_grid);
	}
	else {
	  grid->fill(buffer,Grid::header_only);
	}
    }
    griddef=grid->definition();
    if (args.format == "gpcp" && reinterpret_cast<GPCPGrid *>(grid)->is_empty_grid()) {
	griddef.type=0;
    }
    if (griddef.type > 0) {
	first_valid_date_time=grid->valid_date_time();
	gentry.key=strutils::itos(griddef.type)+"<!>";
	if (griddef.type != Grid::sphericalHarmonicsType) {
	  griddim=grid->dimensions();
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
	fcst_hr=grid->forecast_time()/10000;
	if (args.format == "grib" || args.format == "grib0") {
	  if (message->edition() == 0) {
	    args.format="grib0";
	  }
	  last_valid_date_time=first_valid_date_time;
	  lentry.key=strutils::ftos(grid->source())+"-"+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->sub_center_ID());
	  pentry.key=lentry.key;
	  if (args.format == "grib") {
	    pentry.key+="."+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->parameter_table_code());
	  }
	  pentry.key+=":"+strutils::ftos(grid->parameter());
	  gentry.key+="<!>"+(reinterpret_cast<GRIBGrid *>(grid))->product_description();
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->process());
	  ensdata=grid->ensemble_data();
	  if (ensdata.fcst_type.length() > 0) {
	    ee.key=ensdata.fcst_type+"<!>"+pe.key;
	    if (ensdata.ID.length() > 0)
		ee.key+="%"+ensdata.ID;
	    if (ensdata.total_size > 0)
		ee.key+="<!>"+strutils::itos(ensdata.total_size);
	  }
	  level_type=strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->first_level_type());
	  if (args.format == "grib" || args.format == "grib1") {
	    sdum="WMO_GRIB1";
	  }
	  else {
	    sdum="WMO_GRIB0";
	  }
	  if (level_mapper.level_is_a_layer(sdum,level_type,lentry.key) < 0) {
	    metautils::log_error("no entry for "+level_type+", '"+lentry.key+"' in level map on "+grid->valid_date_time().to_string(),"grid2xml",user,args.args_string);
	  }
	  lentry.key+=",";
	  if (level_mapper.level_is_a_layer(sdum,level_type,lentry.key)) {
	    lentry.key+=level_type+":"+strutils::ftos(grid->first_level_value(),5)+":"+strutils::ftos(grid->second_level_value(),5);
	  }
	  else {
	    lentry.key+=level_type+":"+strutils::ftos(grid->first_level_value(),5);
	  }
	  idx=gentry.key.rfind("<!>");
	  if (inv_stream.is_open()) {
	    sdum=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream))->current_record_offset())+"|"+strutils::itos(message->length())+"|"+grid->valid_date_time().to_string("%Y%m%d%H%MM");
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.emplace_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.emplace_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    inv_lines.emplace_back(sdum);
	  }
	}
	else if (args.format == "grib2") {
/*
	  last_valid_date_time=first_valid_date_time;
first_valid_date_time=grid->reference_date_time().hours_added(grid->forecast_time()/10000);
*/
	  lentry.key=strutils::ftos(grid->source())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->sub_center_ID());
	  pentry.key=lentry.key+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->parameter_table_code())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->local_table_code())+":"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->discipline())+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->parameter_category())+"."+strutils::ftos(grid->parameter());
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->process())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->background_process())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->forecast_process());
	  level_type=strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->first_level_type());
	  n=(reinterpret_cast<GRIB2Grid *>(grid))->second_level_type();
	  lentry.key+=",";
	  if (n != 255) {
	    lentry.key+=level_type+"-"+strutils::itos(n)+":";
	    if (!myequalf(grid->first_level_value(),Grid::missing_value)) {
		lentry.key+=strutils::ftos(grid->first_level_value(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	    lentry.key+=":";
	    if (!myequalf(grid->second_level_value(),Grid::missing_value)) {
		lentry.key+=strutils::ftos(grid->second_level_value(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	  }
	  else {
	    lentry.key+=level_type+":";
	    if (!myequalf(grid->first_level_value(),Grid::missing_value)) {
		lentry.key+=strutils::ftos(grid->first_level_value(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	  }
	  short ptype=(reinterpret_cast<GRIB2Grid *>(grid))->product_type();
	  gentry.key+="<!>"+(reinterpret_cast<GRIB2Grid *>(grid))->product_description();
	  first_valid_date_time=grid->forecast_date_time();
	  last_valid_date_time=grid->valid_date_time();
	  switch (ptype) {
	    case 0:
	    case 1:
	    case 2:
		if (ptype == 1) {
		  ensdata=grid->ensemble_data();
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
		  ensdata=grid->ensemble_data();
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
	  if (inv_stream.is_open()) {
//	    sdum=strutils::lltos(((InputGRIBStream *)istream)->getCurrentRecordOffset())+"|"+strutils::lltos(message->length())+"|"+grid->valid_date_time().to_string("%Y%m%d%H%MM");
sdum=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream))->current_record_offset())+"|"+strutils::lltos(message->length())+"|"+last_valid_date_time.to_string("%Y%m%d%H%MM");
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.emplace_back(ie.key);
	    }
	    sdum+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.emplace_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    sdum+="|";
	    if (ie.key.length() > 0) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.emplace_back(ie.key);
		}
		sdum+=strutils::itos(ie.num);
	    }
	    inv_lines.emplace_back(sdum);
	  }
	}
	else if (args.format == "oct" || args.format == "tropical" || args.format == "ll" || args.format == "slp" || args.format == "navy") {
	  last_valid_date_time=first_valid_date_time;
	  pentry.key=strutils::itos(grid->parameter());
	  if (fcst_hr == 0) {
	    if (first_valid_date_time.day() == 0) {
		first_valid_date_time.add_days(1);
		last_valid_date_time.add_months(1);
		gentry.key+="<!>Monthly Mean of Analyses";
	    }
	    else
		gentry.key+="<!>Analysis";
	  }
	  else {
	    if (first_valid_date_time.day() == 0) {
		first_valid_date_time.add_days(1);
	   	last_valid_date_time.add_months(1);
		gentry.key+="<!>Monthly Mean of "+strutils::itos(fcst_hr)+"-hour Forecasts";
	    }
	    else {
		gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	    }
	    first_valid_date_time.add_hours(fcst_hr);
	  }
	  if (myequalf(grid->second_level_value(),0.)) {
	    lentry.key="0:"+strutils::itos(grid->first_level_value());
	  }
	  else {
	    lentry.key="1:"+strutils::itos(grid->first_level_value())+":"+strutils::itos(grid->second_level_value());
	  }
	}
	else if (args.format == "jraieeemm") {
	  last_valid_date_time=first_valid_date_time;
	  last_valid_date_time.set_day(days_in_month(first_valid_date_time.year(),first_valid_date_time.month()));
	  last_valid_date_time.add_hours(18);
	  pentry.key=strutils::itos(grid->parameter());
	  gentry.key+="<!>Monthly Mean (4 per day) of ";
	  if (grid->forecast_time() == 0) {
	    gentry.key+="Analyses";
	  }
	  else {
	    gentry.key+="Forecasts";
	    sdum=(reinterpret_cast<JRAIEEEMMGrid *>(grid))->time_range();
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
	  lentry.key=strutils::itos(grid->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5);
	}
	else if (args.format == "ussrslp") {
	  pentry.key=strutils::itos(grid->parameter());
	  if (fcst_hr == 0)
	    gentry.key+="<!>Analysis";
	  else
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	}
	else if (args.format == "on84") {
	  last_valid_date_time=first_valid_date_time;
	  pentry.key=strutils::itos(grid->parameter());
	  switch ((reinterpret_cast<ON84Grid *>(grid))->time_marker()) {
	    case 0:
		gentry.key+="<!>Analysis";
		break;
	    case 3:
		fcst_hr-=(reinterpret_cast<ON84Grid *>(grid))->F2();
		gentry.key+="<!>";
		if (fcst_hr > 0)
		  gentry.key+=strutils::itos(fcst_hr+(reinterpret_cast<ON84Grid *>(grid))->F2())+"-hour Forecast of ";
		gentry.key+=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->F2())+"-hour Accumulation";
		break;
	    default:
		metautils::log_error("ON84 time marker "+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->time_marker())+" not recognized","grid2xml",user,args.args_string);
	  }
	  pe.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->run_marker())+"."+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->generating_program());
	  if (myequalf(grid->second_level_value(),0.)) {
	    switch ((reinterpret_cast<ON84Grid *>(grid))->first_level_type()) {
		case 144:
		case 145:
		case 146:
		case 147:
		case 148:
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":1:0";
		  break;
		default:
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5);
	    }
	  }
	  else {
	    lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5)+":"+strutils::ftos(grid->second_level_value(),5);
	  }
	}
	else if (args.format == "cgcm1") {
	  pentry.key=(reinterpret_cast<CGCM1Grid *>(grid))->parameter_name();
	  strutils::trim(pentry.key);
	  strutils::replace_all(pentry.key,"\"","&quot;");
	  last_valid_date_time=first_valid_date_time;
	  if (fcst_hr == 0) {
	    if (first_valid_date_time.day() > 0)
		gentry.key+="<!>Analysis";
	    else {
		gentry.key+="<!>Monthly Mean of Analyses";
		first_valid_date_time.set_day(1);
		last_valid_date_time.set_day(days_in_month(last_valid_date_time.year(),last_valid_date_time.month()));
	    }
	  }
	  else {
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	  }
	  lentry.key=strutils::itos(grid->first_level_value());
	}
	else if (args.format == "cmorph025") {
	  pentry.key=(reinterpret_cast<InputCMORPH025GridStream *>(istream))->parameter_code();
	  strutils::trim(pentry.key);
	  last_valid_date_time=first_valid_date_time;
	  gentry.key+="<!>Analysis";
	  lentry.key="surface";
	}
	else if (args.format == "cmorph8km") {
	  pentry.key="CMORPH";
	  last_valid_date_time=first_valid_date_time;
	  first_valid_date_time=grid->reference_date_time();
	  gentry.key+="<!>30-minute Average (initial+0 to initial+30)";
	  lentry.key="surface";
	}
	else if (args.format == "gpcp") {
	  pentry.key=reinterpret_cast<GPCPGrid *>(grid)->parameter_name();
	  last_valid_date_time=first_valid_date_time;
	  first_valid_date_time=grid->reference_date_time();
	  size_t hrs_since=last_valid_date_time.hours_since(first_valid_date_time);
	  if (hrs_since == 24) {
	    gentry.key+="<!>Daily Mean";
	  }
	  else if (hrs_since == days_in_month(first_valid_date_time.year(),first_valid_date_time.month())*24) {
	    gentry.key+="<!>Monthly Mean";
	  }
	  else {
	    metautils::log_error("can't figure out gridded product type","grid2xml",user,args.args_string);
	  }
	  lentry.key="surface";
	}
	if (!grid_table.found(gentry.key,gentry)) {
	  gentry.level_table.clear();
	  lentry.units=units;
	  lentry.parameter_code_table.clear();
	  pentry.start_date_time=first_valid_date_time;
	  pentry.end_date_time=last_valid_date_time;
	  pentry.num_time_steps=1;
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
	    pentry.start_date_time=first_valid_date_time;
	    pentry.end_date_time=last_valid_date_time;
	    pentry.num_time_steps=1;
	    lentry.parameter_code_table.insert(pentry);
	    gentry.level_table.insert(lentry);
	  }
	  else {
	    if (!lentry.parameter_code_table.found(pentry.key,pentry)) {
		pentry.start_date_time=first_valid_date_time;
		pentry.end_date_time=last_valid_date_time;
		pentry.num_time_steps=1;
		lentry.parameter_code_table.insert(pentry);
	    }
	    else {
		if (first_valid_date_time < pentry.start_date_time) {
		  pentry.start_date_time=first_valid_date_time;
		}
		if (last_valid_date_time > pentry.end_date_time) {
		  pentry.end_date_time=last_valid_date_time;
		}
		pentry.num_time_steps++;
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
    std::cerr << "Terminating - no grids found so no content metadata will be generated" << std::endl;
    exit(1);
  }
  if (inv_lines.size() > 0) {
    for (auto item : inv_U_list) {
	inv_U_table.found(item,ie);
	inv_stream << "U<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_G_list) {
	inv_G_table.found(item,ie);
	inv_stream << "G<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_L_list) {
	inv_L_table.found(item,ie);
	inv_stream << "L<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_P_list) {
	inv_P_table.found(item,ie);
	inv_stream << "P<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_R_list) {
	inv_R_table.found(item,ie);
	inv_stream << "R<!>" << ie.num << "<!>" << item << std::endl;
    }
    for (auto item : inv_E_list) {
	inv_E_table.found(item,ie);
	inv_stream << "E<!>" << ie.num << "<!>" << item << std::endl;
    }
    inv_stream << "-----" << std::endl;
    for (auto line : inv_lines)
	inv_stream << line << std::endl;
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error("core dump","grid2xml",user,args.args_string);
}

extern "C" void int_handler(int)
{
  clean_up();
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
  args.args_string=unix_args_string(argc,argv,'!');
  metautils::read_config("grid2xml",user,args.args_string);
  parse_args();
  flags="-f";
  if (!args.inventory_only && strutils::has_beginning(args.path,"https://rda.ucar.edu")) {
    flags="-wf";
  }
  atexit(clean_up);
  metautils::cmd_register("grid2xml",user);
  if (!args.overwrite_only && !args.inventory_only) {
    metautils::check_for_existing_CMD("GrML");
  }
  scan_file();
  if (!args.inventory_only) {
    metadata::GrML::write_GrML(grid_table,"grid2xml",user);
  }
  if (args.update_DB) {
    if (!args.update_summary) {
	flags="-S "+flags;
    }
    if (!args.regenerate) {
	flags="-R "+flags;
    }
    if (mysystem2(directives.local_root+"/bin/scm -d "+args.dsnum+" "+flags+" "+args.filename+".GrML",oss,ess) < 0) {
	std::cerr << ess.str() << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    metadata::close_inventory(inv_file,inv_dir,inv_stream,"GrML",true,args.update_summary,"grid2xml",user);
  }
  return 0;
}
