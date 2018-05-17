#include <iostream>
#include <stdlib.h>
#include <string>
#include <deque>
#include <regex>
#include <sstream>
#include <pthread.h>
#include <MySQL.hpp>
#include <tempfile.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <myerror.hpp>

metautils::Directives meta_directives;
metautils::Args meta_args;
struct ThreadStruct {
  ThreadStruct() : query(),imagetag(),tid(-1) {}

  MySQL::LocalQuery query;
  std::string imagetag;
  pthread_t tid;
};
MySQL::Server server;
std::string dsnum2;
char summary_bitmap[60][121];
std::string user=getenv("USER");
std::string myerror="";
std::string mywarning="";

void write_ncl_head(TempFile *ncl_file,TempFile *ncgm_file)
{
  ncl_file->writeln("load \"$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl\"");
  ncl_file->writeln("load \"$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl\"");
  ncl_file->writeln("begin");
  ncl_file->writeln("  wks = gsn_open_wks(\"ncgm\",\""+ncgm_file->base_name()+"\")");
//  ncl_file->writeln("  cmap=(/ (/1.,1.,1./), (/0.,0.,0./), (/.55294,.513725,.443137/), (/.3529,.3882,.58039/), (/.4,.4,.4/), (/.55,.55,.55/), (/.87,.87,.87/) /)");
ncl_file->writeln("  cmap=(/ (/1.,1.,1./), (/0.,0.,0./), (/0.3294,0.4588,0.3529/), (/0.1529,0.6667,0.8960/), (/0.55,0.55,0.55/), (/.55,.55,.55/), (/1.0,1.0,0.8608/) /)");
  ncl_file->writeln("  gsn_define_colormap(wks,cmap)");
  ncl_file->writeln("  minlat   =  -90.");
  ncl_file->writeln("  maxlat   =  90.");
  ncl_file->writeln("  mpres = True");
  ncl_file->writeln("  minlon   = -180.");
  ncl_file->writeln("  mpres@mpCenterLonF = 0.");
  ncl_file->writeln("  maxlon   =  180.");
  ncl_file->writeln("  mpres@gsnFrame = False");
  ncl_file->writeln("  mpres@gsnMaximize = True");
  ncl_file->writeln("  mpres@mpProjection = \"CylindricalEquidistant\"");
  ncl_file->writeln("  mpres@mpLimitMode = \"LatLon\"");
  ncl_file->writeln("  mpres@mpOutlineOn = True");
  ncl_file->writeln("  mpres@mpOutlineBoundarySets = \"National\"");
  ncl_file->writeln("  mpres@mpNationalLineColor = 4");
  ncl_file->writeln("  mpres@mpGeophysicalLineColor = 2");
  ncl_file->writeln("  mpres@mpGeophysicalLineThicknessF = 0.");
  ncl_file->writeln("  mpres@mpMinLatF   = minlat");
  ncl_file->writeln("  mpres@mpMaxLatF   = maxlat");
  ncl_file->writeln("  mpres@mpMinLonF   = minlon");
  ncl_file->writeln("  mpres@mpMaxLonF   = maxlon");
  ncl_file->writeln("  mpres@mpFillColors=(/0,3,2,3/)");
  ncl_file->writeln("  mpres@mpPerimOn = False");
  ncl_file->writeln("  mpres@mpLabelsOn = False");
  ncl_file->writeln("  mpres@mpGridAndLimbOn = True");
  ncl_file->writeln("  mpres@mpGridMaskMode = \"MaskNotOcean\"");
  ncl_file->writeln("  mpres@mpGridLineColor = 2");
  ncl_file->writeln("  mpres@mpGridSpacingF = 30.");
  ncl_file->writeln("  mpres@tmBorderLineColor = 0");
  ncl_file->writeln("  mpres@tmXBOn = False");
  ncl_file->writeln("  mpres@tmXTOn = False");
  ncl_file->writeln("  mpres@tmYLOn = False");
  ncl_file->writeln("  mpres@tmYROn = False");
  ncl_file->writeln("  map = gsn_csm_map(wks,mpres)");
  ncl_file->writeln("  mres = True");
  ncl_file->writeln("  mres@gsMarkerIndex = 6");
  ncl_file->writeln("  mres@gsMarkerSizeF = 0.002634");
  ncl_file->writeln("  mres@gsMarkerColor = 6");
}

extern "C" void *thread_ncl(void *tnc)
{
  ThreadStruct *t=(ThreadStruct *)tnc;
  MySQL::Server srv(meta_directives.database_server,meta_directives.metadb_username,meta_directives.metadb_password,"");
  if (t->query.submit(srv) < 0) {
    metautils::log_error("thread_ncl() returned error: "+t->query.error(),"gsi",user);
  }
  char wrotemap[60][121];
  for (size_t n=0; n < 60; ++n) {
    for (size_t m=0; m < 121; ++m) {
	wrotemap[n][m]=0;
    }
  }
  auto ncl_file=new TempFile(meta_directives.temp_path,".ncl");
  auto ncgm_file=new TempFile(meta_directives.temp_path,".ncgm");
  auto tiff_file=new TempFile(meta_directives.temp_path,".tiff");
  write_ncl_head(ncl_file,ncgm_file);
  ncl_file->writeln("  p=new((/500000,2/),\"float\",-999)");
  auto cnt=0;
  MySQL::Row row;
  while (t->query.fetch_row(row)) {
    auto y=std::stoi(row[0]);
    y=(y-1)/3;
    if (wrotemap[y][120] < 120) {
	auto bitmap=row[1];
	for (size_t x=0; x < bitmap.length(); ++x) {
	  if (bitmap[x] != '0') {
	    if (wrotemap[y][x] == 0) {
		ncl_file->writeln("  p("+strutils::itos(cnt)+",0)="+strutils::ftos(-178.5+x*3,1));
		ncl_file->writeln("  p("+strutils::itos(cnt)+",1)="+strutils::ftos(-88.5+y*3,1));
		if (wrotemap[y][x] == 0) {
		  wrotemap[y][120]++;
		}
		wrotemap[y][x]=1;
		summary_bitmap[y][x]=1;
		summary_bitmap[y][120]=1;
		++cnt;
	    }
	  }
	}
    }
  }
  srv.disconnect();
  ncl_file->writeln("  do i=0,"+strutils::itos(cnt-1));
  ncl_file->writeln("    gsn_polymarker(wks,map,p(i,0),p(i,1),mres)");
  ncl_file->writeln("  end do");
  ncl_file->writeln("end");
  ncl_file->close();
  auto tdir=new TempDir;
  if (!tdir->create(meta_directives.temp_path)) {
    metautils::log_error("thread_ncl() can't create temporary directory","gsi",user);
  }
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata",oss,ess) < 0) {
    metautils::log_error("thread_ncl() can't create directory tree","gsi",user);
  }
  if (std::regex_search(unixutils::host_name(),std::regex("^(cheyenne|geyser)"))) {
    unixutils::mysystem2("/bin/tcsh -c \"module delete intel; module load gnu ncl; ncl "+ncl_file->name()+"; ctrans -d sun -res 360x360 "+ncgm_file->name()+" |convert sun:- "+tiff_file->name()+"; mogrify -crop 360x180+0+90! "+tiff_file->name()+"; convert "+tiff_file->name()+" "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata/spatial_coverage."+t->imagetag+".gif\"",oss,ess);
  }
  else {
    unixutils::mysystem2("/bin/tcsh -c \"setenv NCARG_NCARG /usr/share/ncarg; /usr/bin/ncl "+ncl_file->name()+"; /usr/bin/ctrans -d sun -res 360x360 "+ncgm_file->name()+" |convert sun:- "+tiff_file->name()+"; mogrify -crop 360x180+0+90! "+tiff_file->name()+"; convert "+tiff_file->name()+" "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata/spatial_coverage."+t->imagetag+".gif\"",oss,ess);
  }
  delete ncl_file;
  delete ncgm_file;
  delete tiff_file;
  std::string error;
  if (unixutils::rdadata_sync(tdir->name(),"datasets/ds"+meta_args.dsnum+"/metadata/","/data/web",meta_directives.rdadata_home,error) < 0) {
    metautils::log_warning("rdadata_sync errors: '"+error+"'","gsi",user);
  }
  return NULL;
}

void generate_graphics(MySQL::LocalQuery& query,std::string type,std::string table,std::string gindex)
{
  ThreadStruct *tnc;
  if (query.submit(server) < 0) {
    metautils::log_error("generate_graphics() returned error: "+query.error(),"gsi",user);
  }
  for (size_t n=0; n < 60; ++n) {
    for (size_t m=0; m < 121; ++m) {
	summary_bitmap[n][m]=0;
    }
  }
  tnc=new ThreadStruct[query.num_rows()];
  size_t nn=0;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    if (gindex.empty()) {
	if (type == "obs") {
	  tnc[nn].query.set("select distinct box1d_row,box1d_bitmap from "+table+" where dsid = '"+meta_args.dsnum+"' and observationType_code = "+row[0]+" and platformType_code = "+row[1]+" and format_code = "+row[4]+" order by box1d_row");
	  tnc[nn].imagetag=strutils::substitute(row[5]," ","_")+"_mss_"+row[2]+"."+row[3];
	}
	else if (type == "fix") {
	  tnc[nn].query.set("select box1d_row,box1d_bitmap from "+table+" where dsid = '"+meta_args.dsnum+"' and classification_code = "+row[0]+" and format_code = "+row[2]+" order by box1d_row");
	  tnc[nn].imagetag=strutils::substitute(row[3]," ","_")+"_mss_"+row[1];
	}
    }
    else {
	if (type == "obs") {
	  tnc[nn].query.set("select distinct box1d_row,box1d_bitmap from ObML.ds"+dsnum2+"_primaries as p left join dssdb.mssfile as x on (x.dsid = 'ds"+meta_args.dsnum+"' and x.mssfile = p.mssID) left join "+table+" as t on t.mssID_code = p.code where x.gindex = "+gindex+" and observationType_code = "+row[0]+" and platformType_code = "+row[1]+" and p.format_code = "+row[4]+" order by box1d_row");
	  tnc[nn].imagetag=strutils::substitute(row[5]," ","_")+"_mss_gindex_"+gindex+"_"+row[2]+"."+row[3];
	}
	else if (type == "fix") {
	  tnc[nn].query.set("select box1d_row,box1d_bitmap from FixML.ds"+dsnum2+"_primaries as p left join dssdb.mssfile as x on x.mssfile = p.mssID left join "+table+" as t on t.mssID_code = p.code where x.gindex = "+gindex+" and classification_code = "+row[0]+" and p.format_code = "+row[2]+" order by box1d_row");
	  tnc[nn].imagetag=strutils::substitute(row[3]," ","_")+"_mss_gindex_"+gindex+"_"+row[1];
	}
    }
    pthread_create(&tnc[nn].tid,NULL,thread_ncl,reinterpret_cast<void *>(&tnc[nn]));
    ++nn;
  }
  for (size_t m=0; m < nn; ++m) {
    pthread_join(tnc[m].tid,NULL);
  }
  delete[] tnc;
  auto ncl_file=new TempFile(meta_directives.temp_path,".ncl");
  auto ncgm_file=new TempFile(meta_directives.temp_path,".ncgm");
  auto tiff_file=new TempFile(meta_directives.temp_path,".tiff");
  write_ncl_head(ncl_file,ncgm_file);
  ncl_file->writeln("  p=new((/500000,2/),\"float\",-999)");
  auto cnt=0;
  for (size_t n=0; n < 60; ++n) {
    if (summary_bitmap[n][120] == 1) {
	for (size_t m=0; m < 120; ++m) {
	  if (summary_bitmap[n][m] == 1) {
	    ncl_file->writeln("  p("+strutils::itos(cnt)+",0)="+strutils::ftos(-178.5+m*3,1));
	    ncl_file->writeln("  p("+strutils::itos(cnt)+",1)="+strutils::ftos(-88.5+n*3,1));
	    ++cnt;
	  }
	}
    }
  }
  ncl_file->writeln("  do i=0,"+strutils::itos(cnt-1));
  ncl_file->writeln("    gsn_polymarker(wks,map,p(i,0),p(i,1),mres)");
  ncl_file->writeln("  end do");
  ncl_file->writeln("end");
  ncl_file->close();
  auto tdir=new TempDir;
  if (!tdir->create(meta_directives.temp_path)) {
    metautils::log_error("generate_graphics() can't create temporary directory","gsi",user);
  }
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/mkdir -p "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata",oss,ess) < 0) {
    metautils::log_error("generate_graphics() can't create directory tree","gsi",user);
  }
  if (std::regex_search(unixutils::host_name(),std::regex("^(cheyenne|geyser)"))) {
    unixutils::mysystem2("/bin/tcsh -c \"module delete intel; module load gnu ncl; ncl "+ncl_file->name()+"; ctrans -d sun -res 360x360 "+ncgm_file->name()+" |convert sun:- "+tiff_file->name()+"; mogrify -crop 360x180+0+90! "+tiff_file->name()+"; convert "+tiff_file->name()+" "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata/spatial_coverage.gif\"",oss,ess);
  }
  else {
    unixutils::mysystem2("/bin/tcsh -c \"setenv NCARG_NCARG /usr/share/ncarg; /usr/bin/ncl "+ncl_file->name()+"; /usr/bin/ctrans -d sun -res 360x360 "+ncgm_file->name()+" |convert sun:- "+tiff_file->name()+"; mogrify -crop 360x180+0+90! "+tiff_file->name()+"; convert "+tiff_file->name()+" "+tdir->name()+"/datasets/ds"+meta_args.dsnum+"/metadata/spatial_coverage.gif\"",oss,ess);
  }
  delete ncl_file;
  delete ncgm_file;
  delete tiff_file;
  std::string error;
  if (unixutils::rdadata_sync(tdir->name(),"datasets/ds"+meta_args.dsnum+"/metadata/","/data/web",meta_directives.rdadata_home,error) < 0) {
    metautils::log_warning("rdadata_sync errors: '"+error+"'","gsi",user);
  }
}

int main(int argc,char **argv)
{
  MySQL::LocalQuery query,check_query;
  std::deque<std::string> sp;
  std::string table,gindex;
  size_t n;
  bool notify=false;

  if (argc < 2) {
    std::cerr << "usage: gsi [options...] nnn.n" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-g <gindex>   create graphics only for group index <gindex>" << std::endl;
    std::cerr << "-N            notify with a message when gsi completes" << std::endl;
    exit(1);
  }
  meta_args.args_string=unixutils::unix_args_string(argc,argv,'!');
  metautils::read_config("gsi",user);
  sp=strutils::split(meta_args.args_string,"!");
  for (n=0; n < sp.size()-1; n++) {
    if (sp[n] == "-N") {
	notify=true;
    }
    else if (sp[n] == "-g") {
	gindex=sp[++n];
    }
  }
  meta_args.dsnum=sp[n];
  dsnum2=strutils::substitute(meta_args.dsnum,".","");
  server.connect(meta_directives.database_server,meta_directives.metadb_username,meta_directives.metadb_password,"");
  if (!server) {
    metautils::log_error("unable to connect to MySQL server on startup","gsi",user);
  }
  if (table_exists(server,"ObML.ds"+dsnum2+"_dataTypes2")) {
    if (gindex.empty()) {
	query.set("select distinct l.observationType_code,l.platformType_code,o.obsType,pf.platformType,p.format_code,f.format from ObML.ds"+dsnum2+"_primaries as p left join ObML.ds"+dsnum2+"_dataTypes2 as d on d.mssID_code = p.code left join ObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code left join ObML.obsTypes as o on l.observationType_code = o.code left join ObML.platformTypes as pf on l.platformType_code = pf.code left join ObML.formats as f on f.code = p.format_code");
	table="search.obs_data";
    }
    else {
	query.set("select distinct l.observationType_code,l.platformType_code,o.obsType,pf.platformType,p.format_code,f.format from ObML.ds"+dsnum2+"_primaries as p left join dssdb.mssfile as x on x.mssfile = p.mssID left join ObML.ds"+dsnum2+"_dataTypes2 as d on d.mssID_code = p.code left join ObML.ds"+dsnum2+"_dataTypesList as l on l.code = d.dataType_code left join ObML.obsTypes as o on l.observationType_code = o.code left join ObML.platformTypes as pf on l.platformType_code = pf.code left join ObML.formats as f on f.code = p.format_code where x.gindex = "+gindex);
	table="ObML.ds"+dsnum2+"_locations";
    }
    generate_graphics(query,"obs",table,gindex);
  }
  if (table_exists(server,"FixML.ds"+dsnum2+"_locations")) {
    if (gindex.empty()) {
	query.set("select distinct d.classification_code,c.classification,p.format_code,f.format from FixML.ds"+dsnum2+"_primaries as p left join FixML.ds"+dsnum2+"_locations as d on d.mssID_code = p.code left join FixML.classifications as c on d.classification_code = c.code left join FixML.formats as f on f.format = p.format_code where dsid = '"+meta_args.dsnum+"'");
	table="search.fix_data";
    }
    else {
	query.set("select distinct d.classification_code,c.classification,p.format_code,f.format from FixML.ds"+dsnum2+"_primaries as p left join dssdb.mssfile as x on x.mssfile = p.mssID left join FixML.ds"+dsnum2+"_locations as d on d.mssID_code = p.code left join FixML.classifications as c on d.classification_code = c.code left join FixML.formats as f on f.format = p.format_code where x.gindex = "+gindex);
	table="FixML.ds"+dsnum2+"_locations";
    }
    generate_graphics(query,"fix",table,gindex);
  }
  if (notify) {
    std::cout << "gsi has completed successfully" << std::endl;
  }
}
