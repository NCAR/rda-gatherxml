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
#include <regex>
#include <gatherxml.hpp>
#include <grid.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <xmlutils.hpp>
#include <netcdf.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
bool gatherxml::verbose_operation;
extern const std::string USER=getenv("USER");

my::map<gatherxml::markup::GrML::GridEntry> grid_table;
std::unique_ptr<TempFile> tfile;
std::string tfile_name,inv_file;
std::unique_ptr<TempDir> tdir;
TempDir *inv_dir=nullptr;
std::ofstream inv_stream;
std::string myerror="";
std::string mywarning="";
const char *grib1_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour"};
const int grib1_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24};
const int grib1_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12};
const char *grib2_time_unit[]={"minute","hour","day","month","year","year","year","year","","","hour","hour","hour","second"};
const int grib2_per_day[]={1440,24,1,0,0,0,0,0,0,0,24,24,24,86400};
const int grib2_unit_mult[]={1,1,1,1,1,10,30,100,1,1,3,6,12,1};

extern "C" void clean_up()
{
// remove temporary file that was sym-linked because the data file name contains
//  metadata
  if (metautils::args.data_format == "cmorph025" || metautils::args.data_format == "cmorph8km") {
    std::stringstream oss,ess;
    unixutils::mysystem2("/bin/rm "+tfile_name,oss,ess);
  }
  if (!myerror.empty()) {
    metautils::log_error2(myerror,"clean_up()","grid2xml",USER);
  }
}

std::string this_function_label(std::string function_name)
{
  return std::string(function_name+"()");
}

struct InvEntry {
  InvEntry() : key(),num(0) {}

  std::string key;
  int num;
};

bool open_file(void *istream,std::string filename)
{
  if (metautils::args.data_format == "cgcm1") {
    return (reinterpret_cast<InputCGCM1GridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "cmorph025") {
    return (reinterpret_cast<InputCMORPH025GridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "cmorph8km") {
    return (reinterpret_cast<InputCMORPH8kmGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "gpcp") {
    return (reinterpret_cast<InputGPCPGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib2") {
    return (reinterpret_cast<InputGRIBStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "jraieeemm") {
    return (reinterpret_cast<InputJRAIEEEMMGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "ll") {
    return (reinterpret_cast<InputLatLonGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "navy") {
    return (reinterpret_cast<InputNavyGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "noaaoi2") {
    return (reinterpret_cast<InputNOAAOI2SSTGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "oct") {
    return (reinterpret_cast<InputOctagonalGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "on84") {
    return (reinterpret_cast<InputON84GridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "slp") {
    return (reinterpret_cast<InputSLPGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "tropical") {
    return (reinterpret_cast<InputTropicalGridStream *>(istream))->open(filename);
  }
  else if (metautils::args.data_format == "ussrslp") {
    return (reinterpret_cast<InputUSSRSLPGridStream *>(istream))->open(filename);
  }
  else {
    metautils::log_error2(metautils::args.data_format+"-formatted file not recognized","open_file()","grid2xml",USER);
  }
  return false;
}

void scan_file()
{
  static const std::string THIS_FUNC=this_function_label(__func__);
  Grid::GridDimensions griddim;
  Grid::GridDefinition griddef;
  std::string units;
  DateTime first_valid_date_time,last_valid_date_time;
  metautils::StringEntry pe,ee;
  gatherxml::markup::GrML::GridEntry gentry;
  gatherxml::markup::GrML::LevelEntry lentry;
  gatherxml::markup::GrML::ParameterEntry pentry;
  my::map<InvEntry> inv_U_table,inv_G_table,inv_L_table,inv_P_table,inv_R_table,inv_E_table;
  std::list<std::string> inv_U_list,inv_G_list,inv_L_list,inv_P_list,inv_R_list,inv_E_list;

  std::unique_ptr<idstream> istream;
  Grid *grid=nullptr;
  std::unique_ptr<GRIBMessage> message;
  if (metautils::args.data_format == "cgcm1") {
    istream.reset(new InputCGCM1GridStream);
    grid=new CGCM1Grid;
  }
  else if (metautils::args.data_format == "cmorph025") {
    istream.reset(new InputCMORPH025GridStream);
    grid=new CMORPH025Grid;
  }
  else if (metautils::args.data_format == "cmorph8km") {
    istream.reset(new InputCMORPH8kmGridStream);
    grid=new CMORPH8kmGrid;
  }
  else if (metautils::args.data_format == "gpcp") {
    istream.reset(new InputGPCPGridStream);
    grid=new GPCPGrid;
  }
  else if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib0") {
    istream.reset(new InputGRIBStream);
    message.reset(new GRIBMessage);
    grid=new GRIBGrid;
  }
  else if (metautils::args.data_format == "grib2") {
    istream.reset(new InputGRIBStream);
    message.reset(new GRIB2Message);
    grid=new GRIB2Grid;
  }
  else if (metautils::args.data_format == "jraieeemm") {
    istream.reset(new InputJRAIEEEMMGridStream);
    grid=new JRAIEEEMMGrid;
  }
  else if (metautils::args.data_format == "ll") {
    istream.reset(new InputLatLonGridStream);
    grid=new LatLonGrid;
  }
  else if (metautils::args.data_format == "navy") {
    istream.reset(new InputNavyGridStream);
    grid=new NavyGrid;
  }
  else if (metautils::args.data_format == "noaaoi2") {
    istream.reset(new InputNOAAOI2SSTGridStream);
    grid=new NOAAOI2SSTGrid;
  }
  else if (metautils::args.data_format == "oct") {
    istream.reset(new InputOctagonalGridStream);
    grid=new OctagonalGrid;
  }
  else if (metautils::args.data_format == "on84") {
    istream.reset(new InputON84GridStream);
    grid= new ON84Grid;
  }
  else if (metautils::args.data_format == "slp") {
    istream.reset(new InputSLPGridStream);
    grid= new SLPGrid;
  }
  else if (metautils::args.data_format == "tropical") {
    istream.reset(new InputTropicalGridStream);
    grid= new TropicalGrid;
  }
  else if (metautils::args.data_format == "ussrslp") {
    istream.reset(new InputUSSRSLPGridStream);
    grid=new USSRSLPGrid;
  }
  else {
    metautils::log_error2(metautils::args.data_format+"-formatted files not recognized",THIS_FUNC,"grid2xml",USER);
  }
  tfile.reset(new TempFile);
  tdir.reset(new TempDir);
  if (metautils::args.data_format == "jraieeemm") {
    tfile->open(metautils::args.temp_loc,"."+metautils::args.filename);
    tfile_name=tfile->name();
  }
  else {
    tfile->open(metautils::args.temp_loc);
// symlink the temporary file when the data file name contains metadata
    if (metautils::args.data_format == "cmorph025" || metautils::args.data_format == "cmorph8km") {
	tfile_name=metautils::args.filename;
	auto idx=tfile_name.rfind("/");
	if (idx != std::string::npos) {
	  tfile_name=tfile_name.substr(idx+1);
	}
	idx=(tfile->name()).rfind("/");
	tfile_name=(tfile->name()).substr(0,idx+1)+tfile_name;
	std::stringstream oss,ess;
	if (unixutils::mysystem2("/bin/ln -s "+tfile->name()+" "+tfile_name,oss,ess) < 0) {
	  metautils::log_error2("unable to sym-link to temporary file - "+ess.str(),THIS_FUNC,"grid2xml",USER);
	}
    }
    else {
	tfile_name=tfile->name();
    }
  }
  tdir->create(metautils::args.temp_loc);
  std::string file_format,error;
  if (!metautils::primaryMetadata::prepare_file_for_metadata_scanning(*tfile,*tdir,NULL,file_format,error)) {
    metautils::log_error2(error,THIS_FUNC+": prepare_file_for_metadata_scanning()","grid2xml",USER);
  }
  if (!open_file(istream.get(),tfile_name)) {
    metautils::log_error2("unable to open file for input",THIS_FUNC,"grid2xml",USER);
  }
  if ((file_format.empty() || file_format == "TAR") && (metautils::args.data_format == "grib" || metautils::args.data_format == "grib0" || metautils::args.data_format == "grib2")) {
    gatherxml::fileInventory::open(inv_file,&inv_dir,inv_stream,"GrML","grid2xml",USER);
  }
  else if (metautils::args.inventory_only) {
    metautils::log_error2("unable to inventory "+metautils::args.path+"/"+metautils::args.filename+" because archive format is '"+file_format+"'",THIS_FUNC,"grid2xml",USER);
  }
  xmlutils::LevelMapper level_mapper("/glade/u/home/rdadata/share/metadata/LevelTables");
  std::list<std::string> inv_lines;
  std::unique_ptr<unsigned char[]> buffer;
  size_t ngrids=0;
  int buffer_length=0;
  while (1) {
    if ((metautils::args.data_format != "grib" && metautils::args.data_format != "grib2") || ngrids == message->number_of_grids()) {
	auto bytes_read=istream->peek();
	if (bytes_read < 0) {
	  if (bytes_read == bfstream::error) {
	    metautils::log_error2("an error occurred while reading the data file - no content metadata was generated",THIS_FUNC,"grid2xml",USER);
	    exit(1);
	  }
	  else {
	    break;
	  }
	}
	if (bytes_read > buffer_length) {
	  buffer_length=bytes_read;
	  buffer.reset(new unsigned char[buffer_length]);
	}
	istream->read(buffer.get(),buffer_length);
	ngrids=0;
    }
    if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib2") {
	if (ngrids == 0) {
	  message->fill(buffer.get(),true);
	  while (message->number_of_grids() == 0) {
	    auto bytes_read=istream->peek();
	    if (bytes_read < 0) {
		if (bytes_read == bfstream::error) {
		  metautils::log_error2("an error occurred while reading the data file - no content metadata was generated",THIS_FUNC,"grid2xml",USER);
		  exit(1);
		}
		else {
		  break;
		}
	    }
	    if (bytes_read > buffer_length) {
		buffer_length=bytes_read;
		buffer.reset(new unsigned char[buffer_length]);
	    }
	    istream->read(buffer.get(),buffer_length);
	    message->fill(buffer.get(),true);
	  }
	  if (message->number_of_grids() == 0) {
	    break;
	  }
	}
	grid=message->grid(ngrids);
	++ngrids;
    }
    else {
	if (metautils::args.data_format == "cmorph025") {
	  reinterpret_cast<CMORPH025Grid *>(grid)->set_date_time(reinterpret_cast<InputCMORPH025GridStream *>(istream.get())->date_time());
	  reinterpret_cast<CMORPH025Grid *>(grid)->set_latitudes(reinterpret_cast<InputCMORPH025GridStream *>(istream.get())->start_latitude(),reinterpret_cast<InputCMORPH025GridStream *>(istream.get())->end_latitude());
	}
	else if (metautils::args.data_format == "gpcp") {
	  grid->fill(buffer.get(),Grid::FULL_GRID);
	}
	else {
	  grid->fill(buffer.get(),Grid::HEADER_ONLY);
	}
    }
    griddef=grid->definition();
	if (metautils::args.data_format == "gpcp" && reinterpret_cast<GPCPGrid *>(grid)->is_empty_grid()) {
	griddef.type = Grid::Type::not_set;
    }
    if (griddef.type != Grid::Type::not_set) {
	first_valid_date_time=grid->valid_date_time();
	gentry.key=strutils::itos(static_cast<int>(griddef.type))+"<!>";
	if (griddef.type != Grid::Type::sphericalHarmonics) {
	  griddim=grid->dimensions();
	  gentry.key+=strutils::itos(griddim.x)+"<!>"+strutils::itos(griddim.y)+"<!>"+strutils::ftos(griddef.slatitude,3)+"<!>"+strutils::ftos(griddef.slongitude,3)+"<!>"+strutils::ftos(griddef.elatitude,3)+"<!>"+strutils::ftos(griddef.elongitude,3)+"<!>"+strutils::ftos(griddef.loincrement,3)+"<!>";
	  switch (griddef.type) {
	    case Grid::Type::gaussianLatitudeLongitude: {
		gentry.key+=strutils::itos(griddef.num_circles);
		break;
	    }
	    default: {
		gentry.key+=strutils::ftos(griddef.laincrement,3);
	    }
	  }
	  if (griddef.type == Grid::Type::polarStereographic || griddef.type == Grid::Type::lambertConformal) {
	    auto pole= (griddef.projection_flag ==  0) ? "N" : "S";
	    gentry.key+=std::string("<!>")+pole;
	    if (griddef.type == Grid::Type::lambertConformal) {
		gentry.key+="<!>"+strutils::ftos(griddef.stdparallel1,3)+"<!>"+strutils::ftos(griddef.stdparallel2);
	    }
	  }
	}
	else {
	  gentry.key+=strutils::itos(griddef.trunc1)+"<!>"+strutils::itos(griddef.trunc2)+"<!>"+strutils::itos(griddef.trunc3);
	}
	pe.key="";
	ee.key="";
	units="";
	auto fcst_hr=grid->forecast_time()/10000;
	if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib0") {
	  if (message->edition() == 0) {
	    metautils::args.data_format="grib0";
	  }
	  last_valid_date_time=first_valid_date_time;
	  first_valid_date_time=grid->forecast_date_time();
	  lentry.key=strutils::ftos(grid->source())+"-"+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->sub_center_id());
	  pentry.key=lentry.key;
	  if (metautils::args.data_format == "grib") {
	    pentry.key+="."+strutils::ftos((reinterpret_cast<GRIBGrid *>(grid))->parameter_table_code());
	  }
	  pentry.key+=":"+strutils::ftos(grid->parameter());
	  gentry.key+="<!>"+(reinterpret_cast<GRIBGrid *>(grid))->product_description();
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->process());
	  auto ensdata=grid->ensemble_data();
	  if (!ensdata.fcst_type.empty()) {
	    ee.key=ensdata.fcst_type+"<!>"+pe.key;
	    if (!ensdata.id.empty()) {
		ee.key+="%"+ensdata.id;
	    }
	    if (ensdata.total_size > 0) {
		ee.key+="<!>"+strutils::itos(ensdata.total_size);
	    }
	  }
	  auto level_type=strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->first_level_type());
	  std::string data_format;
	  if (metautils::args.data_format == "grib" || metautils::args.data_format == "grib1") {
	    data_format="WMO_GRIB1";
	  }
	  else {
	    data_format="WMO_GRIB0";
	  }
	  if (level_mapper.level_is_a_layer(data_format,level_type,lentry.key) < 0) {
	    metautils::log_error2("no entry for "+level_type+", '"+lentry.key+"' in level map on "+grid->valid_date_time().to_string(),THIS_FUNC,"grid2xml",USER);
	  }
	  lentry.key+=",";
	  if (level_mapper.level_is_a_layer(data_format,level_type,lentry.key)) {
	    lentry.key+=level_type+":"+strutils::ftos(grid->first_level_value(),5)+":"+strutils::ftos(grid->second_level_value(),5);
	  }
	  else {
	    lentry.key+=level_type+":"+strutils::ftos(grid->first_level_value(),5);
	  }
	  auto idx=gentry.key.rfind("<!>");
	  if (inv_stream.is_open()) {
	    auto inv_line=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream.get()))->current_record_offset())+"|"+strutils::itos(message->length())+"|"+grid->valid_date_time().to_string("%Y%m%d%H%MM");
	    InvEntry ie;
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    inv_line+="|";
	    if (!ie.key.empty()) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.emplace_back(ie.key);
		}
		inv_line+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    inv_line+="|";
	    if (!ie.key.empty()) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.emplace_back(ie.key);
		}
		inv_line+=strutils::itos(ie.num);
	    }
	    inv_lines.emplace_back(inv_line);
	  }
	}
	else if (metautils::args.data_format == "grib2") {
//	  last_valid_date_time=first_valid_date_time;
//first_valid_date_time=grid->reference_date_time().hours_added(grid->forecast_time()/10000);
	  lentry.key=strutils::ftos(grid->source())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->sub_center_id());
	  pentry.key=lentry.key+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->parameter_table_code())+"-"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->local_table_code())+":"+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->discipline())+"."+strutils::ftos((reinterpret_cast<GRIB2Grid *>(grid))->parameter_category())+"."+strutils::ftos(grid->parameter());
	  pe.key=lentry.key+":"+strutils::itos((reinterpret_cast<GRIBGrid *>(grid))->process())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->background_process())+"."+strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->forecast_process());
	  auto level_type=strutils::itos((reinterpret_cast<GRIB2Grid *>(grid))->first_level_type());
	  auto n=(reinterpret_cast<GRIB2Grid *>(grid))->second_level_type();
	  lentry.key+=",";
	  if (n != 255) {
	    lentry.key+=level_type+"-"+strutils::itos(n)+":";
	    if (!floatutils::myequalf(grid->first_level_value(),Grid::MISSING_VALUE)) {
		lentry.key+=strutils::ftos(grid->first_level_value(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	    lentry.key+=":";
	    if (!floatutils::myequalf(grid->second_level_value(),Grid::MISSING_VALUE)) {
		lentry.key+=strutils::ftos(grid->second_level_value(),5);
	    }
	    else {
		lentry.key+="0";
	    }
	  }
	  else {
	    lentry.key+=level_type+":";
	    if (!floatutils::myequalf(grid->first_level_value(),Grid::MISSING_VALUE)) {
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
	    {
		if (ptype == 1) {
		  auto ensdata=grid->ensemble_data();
		  if (!ensdata.fcst_type.empty()) {
		    ee.key=ensdata.fcst_type+"<!>"+pe.key;
		    if (!ensdata.id.empty()) {
			ee.key+="%"+ensdata.id;
		    }
		    if (ensdata.total_size > 0) {
			ee.key+="<!>"+strutils::itos(ensdata.total_size);
		    }
		  }
		}
		break;
	    }
	    case 8:
	    case 11:
	    case 12:
	    {
		if (ptype == 11 || ptype == 12) {
		  auto ensdata=grid->ensemble_data();
		  if (!ensdata.fcst_type.empty()) {
		    ee.key=ensdata.fcst_type+"<!>"+pe.key;
		    if (!ensdata.id.empty()) {
			ee.key+="%"+ensdata.id;
		    }
		    if (ensdata.total_size > 0) {
			ee.key+="<!>"+strutils::itos(ensdata.total_size);
		    }
		  }
		}
		break;
	    }
	  }
	  auto idx=gentry.key.rfind("<!>");
	  if (inv_stream.is_open()) {
	    auto inv_line=strutils::lltos((reinterpret_cast<InputGRIBStream *>(istream.get()))->current_record_offset())+"|"+strutils::lltos(message->length())+"|"+last_valid_date_time.to_string("%Y%m%d%H%MM");
	    InvEntry ie;
	    ie.key=gentry.key.substr(idx+3);
	    if (!inv_U_table.found(ie.key,ie)) {
		ie.num=inv_U_table.size();
		inv_U_table.insert(ie);
		inv_U_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=strutils::substitute(gentry.key.substr(0,idx),"<!>",",");
	    if (!inv_G_table.found(ie.key,ie)) {
		ie.num=inv_G_table.size();
		inv_G_table.insert(ie);
		inv_G_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=lentry.key;
	    if (!inv_L_table.found(ie.key,ie)) {
		ie.num=inv_L_table.size();
		inv_L_table.insert(ie);
		inv_L_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=pentry.key;
	    if (!inv_P_table.found(ie.key,ie)) {
		ie.num=inv_P_table.size();
		inv_P_table.insert(ie);
		inv_P_list.emplace_back(ie.key);
	    }
	    inv_line+="|"+strutils::itos(ie.num);
	    ie.key=pe.key;
	    inv_line+="|";
	    if (!ie.key.empty()) {
		if (!inv_R_table.found(ie.key,ie)) {
		  ie.num=inv_R_table.size();
		  inv_R_table.insert(ie);
		  inv_R_list.emplace_back(ie.key);
		}
		inv_line+=strutils::itos(ie.num);
	    }
	    ie.key=strutils::substitute(ee.key,"<!>",",");
	    inv_line+="|";
	    if (!ie.key.empty()) {
		if (!inv_E_table.found(ie.key,ie)) {
		  ie.num=inv_E_table.size();
		  inv_E_table.insert(ie);
		  inv_E_list.emplace_back(ie.key);
		}
		inv_line+=strutils::itos(ie.num);
	    }
	    inv_lines.emplace_back(inv_line);
	  }
	}
	else if (metautils::args.data_format == "oct" || metautils::args.data_format == "tropical" || metautils::args.data_format == "ll" || metautils::args.data_format == "slp" || metautils::args.data_format == "navy") {
	  last_valid_date_time=first_valid_date_time;
	  pentry.key=strutils::itos(grid->parameter());
	  if (fcst_hr == 0) {
	    if (first_valid_date_time.day() == 0) {
		first_valid_date_time.add_days(1);
		last_valid_date_time.add_months(1);
		gentry.key+="<!>Monthly Mean of Analyses";
	    }
	    else {
		gentry.key+="<!>Analysis";
	    }
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
	  if (floatutils::myequalf(grid->second_level_value(),0.)) {
	    lentry.key="0:"+strutils::itos(grid->first_level_value());
	  }
	  else {
	    lentry.key="1:"+strutils::itos(grid->first_level_value())+":"+strutils::itos(grid->second_level_value());
	  }
	}
	else if (metautils::args.data_format == "jraieeemm") {
	  last_valid_date_time=first_valid_date_time;
	  last_valid_date_time.set_day(dateutils::days_in_month(first_valid_date_time.year(),first_valid_date_time.month()));
	  last_valid_date_time.add_hours(18);
	  pentry.key=strutils::itos(grid->parameter());
	  gentry.key+="<!>Monthly Mean (4 per day) of ";
	  if (grid->forecast_time() == 0) {
	    gentry.key+="Analyses";
	  }
	  else {
	    gentry.key+="Forecasts";
	    auto fcst_type=(reinterpret_cast<JRAIEEEMMGrid *>(grid))->time_range();
	    if (fcst_type == "MN  ") {
		gentry.key+=" of 6-hour Average";
	    }
	    else if (fcst_type == "AC  ") {
		gentry.key+=" of 6-hour Accumulation";
	    }
	    else if (fcst_type == "MAX ") {
		gentry.key+=" of 6-hour Maximum";
	    }
	    else if (fcst_type == "MIN ") {
		gentry.key+=" of 6-hour Minimum";
	    }
	  }
	  lentry.key=strutils::itos(grid->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5);
	}
	else if (metautils::args.data_format == "ussrslp") {
	  pentry.key=strutils::itos(grid->parameter());
	  if (fcst_hr == 0) {
	    gentry.key+="<!>Analysis";
	  }
	  else {
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	  }
	}
	else if (metautils::args.data_format == "on84") {
	  last_valid_date_time=first_valid_date_time;
	  pentry.key=strutils::itos(grid->parameter());
	  switch ((reinterpret_cast<ON84Grid *>(grid))->time_marker()) {
	    case 0: {
		gentry.key+="<!>Analysis";
		break;
	    }
	    case 3: {
		fcst_hr-=(reinterpret_cast<ON84Grid *>(grid))->F2();
		gentry.key+="<!>";
		if (fcst_hr > 0) {
		  gentry.key+=strutils::itos(fcst_hr+(reinterpret_cast<ON84Grid *>(grid))->F2())+"-hour Forecast of ";
		}
		gentry.key+=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->F2())+"-hour Accumulation";
		break;
	    }
	    case 4: {
		first_valid_date_time.set_day(1);
		short ndays=dateutils::days_in_month(last_valid_date_time.year(),last_valid_date_time.month());
		if (grid->number_averaged() == ndays) {
		  gentry.key+="<!>Monthly Mean of Analyses";
		}
		else {
		  gentry.key+="<!>Mean of "+strutils::itos(grid->number_averaged())+" Analyses";
		}
		last_valid_date_time.set_day(grid->number_averaged());
		break;
	    }
	    default: {
		metautils::log_error2("ON84 time marker "+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->time_marker())+" not recognized",THIS_FUNC,"grid2xml",USER);
	    }
	  }
	  pe.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->run_marker())+"."+strutils::itos((reinterpret_cast<ON84Grid *>(grid))->generating_program());
	  if (floatutils::myequalf(grid->second_level_value(),0.)) {
	    switch ((reinterpret_cast<ON84Grid *>(grid))->first_level_type()) {
		case 144:
		case 145:
		case 146:
		case 147:
		case 148: {
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":1:0";
		  break;
		}
		default: {
		  lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5);
		}
	    }
	  }
	  else {
	    lentry.key=strutils::itos((reinterpret_cast<ON84Grid *>(grid))->first_level_type())+":"+strutils::ftos(grid->first_level_value(),5)+":"+strutils::ftos(grid->second_level_value(),5);
	  }
	}
	else if (metautils::args.data_format == "cgcm1") {
	  pentry.key=(reinterpret_cast<CGCM1Grid *>(grid))->parameter_name();
	  strutils::trim(pentry.key);
	  strutils::replace_all(pentry.key,"\"","&quot;");
	  last_valid_date_time=first_valid_date_time;
	  if (fcst_hr == 0) {
	    if (first_valid_date_time.day() > 0) {
		gentry.key+="<!>Analysis";
	    }
	    else {
		gentry.key+="<!>Monthly Mean of Analyses";
		first_valid_date_time.set_day(1);
		last_valid_date_time.set_day(dateutils::days_in_month(last_valid_date_time.year(),last_valid_date_time.month()));
	    }
	  }
	  else {
	    gentry.key+="<!>"+strutils::itos(fcst_hr)+"-hour Forecast";
	  }
	  lentry.key=strutils::itos(grid->first_level_value());
	}
	else if (metautils::args.data_format == "cmorph025") {
	  pentry.key=(reinterpret_cast<InputCMORPH025GridStream *>(istream.get()))->parameter_code();
	  strutils::trim(pentry.key);
	  last_valid_date_time=first_valid_date_time;
	  gentry.key+="<!>Analysis";
	  lentry.key="surface";
	}
	else if (metautils::args.data_format == "cmorph8km") {
	  pentry.key="CMORPH";
	  last_valid_date_time=first_valid_date_time;
	  first_valid_date_time=grid->reference_date_time();
	  gentry.key+="<!>30-minute Average (initial+0 to initial+30)";
	  lentry.key="surface";
	}
	else if (metautils::args.data_format == "gpcp") {
	  pentry.key=reinterpret_cast<GPCPGrid *>(grid)->parameter_name();
	  last_valid_date_time=first_valid_date_time;
	  first_valid_date_time=grid->reference_date_time();
	  size_t hrs_since=last_valid_date_time.hours_since(first_valid_date_time);
	  if (hrs_since == 24) {
	    gentry.key+="<!>Daily Mean";
	  }
	  else if (hrs_since == dateutils::days_in_month(first_valid_date_time.year(),first_valid_date_time.month())*24) {
	    gentry.key+="<!>Monthly Mean";
	  }
	  else {
	    metautils::log_error2("can't figure out gridded product type",THIS_FUNC,"grid2xml",USER);
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
	  if (!pe.key.empty()) {
	    gentry.process_table.insert(pe);
	  }
	  gentry.ensemble_table.clear();
	  if (!ee.key.empty()) {
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
	  if (!pe.key.empty() && !gentry.process_table.found(pe.key,pe)) {
	    gentry.process_table.insert(pe);
	  }
	  if (!ee.key.empty() && !gentry.ensemble_table.found(ee.key,ee)) {
	    gentry.ensemble_table.insert(ee);
	  }
	  grid_table.replace(gentry);
	}
    }
  }
  istream->close();
  if (grid_table.size() == 0) {
    metautils::log_error2("Terminating - no grids found so no content metadata will be generated",THIS_FUNC,"grid2xml",USER);
  }
  if (inv_lines.size() > 0) {
    InvEntry ie;
    for (const auto& U : inv_U_list) {
	inv_U_table.found(U,ie);
	inv_stream << "U<!>" << ie.num << "<!>" << U << std::endl;
    }
    for (const auto& G : inv_G_list) {
	inv_G_table.found(G,ie);
	inv_stream << "G<!>" << ie.num << "<!>" << G << std::endl;
    }
    for (const auto& L : inv_L_list) {
	inv_L_table.found(L,ie);
	inv_stream << "L<!>" << ie.num << "<!>" << L << std::endl;
    }
    for (const auto& P : inv_P_list) {
	inv_P_table.found(P,ie);
	inv_stream << "P<!>" << ie.num << "<!>" << P << std::endl;
    }
    for (const auto& R : inv_R_list) {
	inv_R_table.found(R,ie);
	inv_stream << "R<!>" << ie.num << "<!>" << R << std::endl;
    }
    for (const auto& E : inv_E_list) {
	inv_E_table.found(E,ie);
	inv_stream << "E<!>" << ie.num << "<!>" << E << std::endl;
    }
    inv_stream << "-----" << std::endl;
    for (const auto& line : inv_lines)
	inv_stream << line << std::endl;
  }
}

extern "C" void segv_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
  metautils::log_error2("core dump","segv_handler()","grid2xml",USER);
}

extern "C" void int_handler(int)
{
  clean_up();
  metautils::cmd_unregister();
}

int main(int argc,char **argv)
{
  const std::string THIS_FUNC=this_function_label(__func__);
  std::string web_home;
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
    std::cerr << "-f navy          DSS Navy grid format" << std::endl;
    std::cerr << "-f noaaoi2       NOAA OI2 SST format" << std::endl;
    std::cerr << "-f oct           DSS Octagonal grid format" << std::endl;
    std::cerr << "-f on84          NCEP Office Note 84 grid format" << std::endl;
    std::cerr << "-f slp           DSS Sea-Level Pressure grid format" << std::endl;
    std::cerr << "-f tropical      DSS Tropical grid format" << std::endl;
    std::cerr << "-f ussrslp       USSR Sea-Level Pressure grid format" << std::endl;
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "-d <nnn.n>       nnn.n is the dataset number to which the data file belongs" << std::endl;
    std::cerr << std::endl;
    std::cerr << "options:" << std::endl;
    std::cerr << "-r/-R            regenerate/don't regenerate the dataset webpage (default is -r)" << std::endl;
    std::cerr << "-s/-S            do/don't update the dataset summary information (default is -s)" << std::endl;
    if (USER == "dattore") {
	std::cerr << "-u/-U            do/don't update the database (default is -u)" << std::endl;
	std::cerr << "-t <path>        path where temporary files should be created" << std::endl;
      std::cerr << "-I               inventory only; no content metadata generated" << std::endl;
      std::cerr << "-OO              overwrite only - when content metadata already exists, the" << std::endl;
      std::cerr << "                 default is to first delete existing metadata; this option saves" << std::endl;
      std::cerr << "                 time by overwriting without the delete" << std::endl;
    }
    std::cerr << std::endl;
    std::cerr << "required:" << std::endl;
    std::cerr << "<path>           full URL of the file to read" << std::endl;
    std::cerr << "                   - URLs must begin with \"https://rda.ucar.edu\"" << std::endl;
    exit(1);
  }
  signal(SIGSEGV,segv_handler);
  signal(SIGINT,int_handler);
  auto arg_delimiter='!';
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,arg_delimiter);
  metautils::read_config("grid2xml",USER);
  gatherxml::parse_args(arg_delimiter);
  atexit(clean_up);
  metautils::cmd_register("grid2xml",USER);
  if (!metautils::args.overwrite_only && !metautils::args.inventory_only) {
    metautils::check_for_existing_cmd("GrML");
  }
  scan_file();
  if (!metautils::args.inventory_only) {
    auto tdir=gatherxml::markup::GrML::write(grid_table,"grid2xml",USER);
    if (metautils::args.update_db) {
	std::string flags;
	if (!metautils::args.update_summary) {
	  flags+=" -S";
	}
	if (!metautils::args.regenerate) {
	  flags+=" -R";
	}
	if (!tdir.empty()) {
	  flags+=" -t "+tdir;
	}
	if (std::regex_search(metautils::args.path,std::regex("^https://rda.ucar.edu"))) {
	  flags+=" -wf";
	}
	else {
	  metautils::log_error2("Terminating - invalid path '"+metautils::args.path+"'",THIS_FUNC,"grid2xml",USER);
	}
	if (unixutils::mysystem2(metautils::directives.local_root+"/bin/scm -d "+metautils::args.dsnum+" "+flags+" "+metautils::args.filename+".GrML",oss,ess) < 0) {
	  metautils::log_error2(ess.str(),"main() running scm","grid2xml",USER);
	}
    }
    else if (metautils::args.dsnum == "999.9") {
	std::cout << "Output is in:" << std::endl;
	std::cout << "  " << tdir << "/" << metautils::args.filename << ".GrML" << std::endl;
    }
  }
  if (inv_stream.is_open()) {
    gatherxml::fileInventory::close(inv_file,&inv_dir,inv_stream,"GrML",true,metautils::args.update_summary,"grid2xml",USER);
  }
  return 0;
}
