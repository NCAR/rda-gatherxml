#include <list>
#include <tempfile.hpp>
#include <datetime.hpp>
#include <mymap.hpp>
#include <metadata.hpp>

namespace gatherxml {

extern bool verbose_operation;

extern void parse_args(char arg_delimiter);

namespace detailedMetadata {

extern void generate_detailed_metadata_view(std::string caller,std::string user);
extern void generate_group_detailed_metadata_view(std::string group_index,std::string file_type,std::string caller,std::string user);

} // end namespace gatherxml::detailedMetadata

namespace summarizeMetadata {

struct BoxFlags {
  BoxFlags() : num_y(0),num_x(0),flags(nullptr),npole(),spole() {}
  BoxFlags(const BoxFlags& source) : BoxFlags() { *this=source; }
  ~BoxFlags() {
    clear();
  }
  BoxFlags& operator=(const BoxFlags& source) {
    if (this == &source) {
	return *this;
    }
    clear();
    num_y=source.num_y;
    num_x=source.num_x;
    flags=new unsigned char *[num_y];
    for (size_t n=0; n < num_y; ++n) {
	flags[n]=new unsigned char[num_x];
	for (size_t m=0; m < num_x; ++m) {
	  flags[n][m]=source.flags[n][m];
	}
    }
    npole=source.npole;
    spole=source.spole;
  }
  void clear() {
    if (flags != nullptr) {
	for (size_t n=0; n < num_y; ++n) {
	  delete[] flags[n];
	}
	delete[] flags;
	flags=nullptr;
    }
  }
  void initialize(size_t dim_x,size_t dim_y,unsigned char n_pole,unsigned char s_pole) {
    clear();
    flags=new unsigned char *[dim_y];
    for (size_t n=0; n < dim_y; ++n) {
	flags[n]=new unsigned char[dim_x];
	for (size_t m=0; m < dim_x; flags[n][m++]=0);
    }
    npole=n_pole;
    spole=s_pole;
  }

  size_t num_y,num_x;
  unsigned char **flags;
  unsigned char npole,spole;
};
struct CodeEntry {
  CodeEntry() : key(),code() {}

  std::string key;
  std::string code;
};
struct ParentLocation {
  ParentLocation() : key(),children_set(nullptr),matched_set(nullptr),consolidated_parent_set(nullptr) {}

  std::string key;
  std::shared_ptr<std::unordered_set<std::string>> children_set,matched_set,consolidated_parent_set;
};
struct SummaryEntry {
  SummaryEntry() : key(),start_date(),end_date(),box1d_bitmap() {}

  std::string key;
  std::string start_date,end_date;
  std::string box1d_bitmap;
};

extern void aggregate_grids(std::string database,std::string caller,std::string user,std::string file_ID_code = "");
extern void check_point(double latp,double lonp,MySQL::Server& server,my::map<CodeEntry>& location_table);
extern void compress_locations(std::list<std::string>& location_list,my::map<ParentLocation>& parent_location_table,std::vector<std::string>& sorted_array,std::string caller,std::string user);
extern void create_file_list_cache(std::string file_type,std::string caller,std::string user,std::string gindex = "");
extern void create_non_cmd_file_list_cache(std::string file_type,my::map<CodeEntry>& files_with_cmd_table,std::string caller,std::string user);
extern void grids_per(size_t nsteps,DateTime start,DateTime end,double& gridsper,std::string& unit);
extern void summarize_data_formats(std::string caller,std::string user);
extern void summarize_dates(std::string caller,std::string user);
extern void summarize_fix_data(std::string caller,std::string user);
extern void summarize_frequencies(std::string caller,std::string user,std::string mss_ID_code = "");
extern void summarize_grid_levels(std::string database,std::string caller,std::string user);
extern void summarize_grid_resolutions(std::string caller,std::string user,std::string mss_ID_code = "");
extern void summarize_grids(std::string database,std::string caller,std::string user,std::string file_ID_code = "");
extern std::string summarize_locations(std::string database);
extern bool summarize_obs_data(std::string caller,std::string user);

} // end namespace gatherxml::summarizeMetadata

namespace fileInventory {

extern void close(std::string filename,TempDir **tdir,std::ofstream& ofs,std::string cmd_type,bool insert_into_db,bool create_cache,std::string caller,std::string user);
extern void open(std::string& filename,TempDir **tdir,std::ofstream& ofs,std::string cmd_type,std::string caller,std::string user);

} // end namespace gatherxml::fileInventory

namespace markup {

namespace FixML {

struct ClassificationEntry {
  ClassificationEntry() : key(),src(),start_datetime(),end_datetime(),start_lat(0.),start_lon(0.),end_lat(0.),end_lon(0.),min_lat(0.),min_lon(0.),max_lat(0.),max_lon(0.),min_pres(0.),max_pres(0.),min_speed(0.),max_speed(0.),nfixes(0),pres_units(),wind_units() {}

  std::string key;
  std::string src;
  DateTime start_datetime,end_datetime;
  float start_lat,start_lon,end_lat,end_lon;
  float min_lat,min_lon,max_lat,max_lon;
  float min_pres,max_pres,min_speed,max_speed;
  size_t nfixes;
  std::string pres_units,wind_units;
};
struct FeatureEntry {
  struct Data {
    Data() : alt_id(),classification_list() {}

    std::string alt_id;
    std::list<ClassificationEntry> classification_list;
  };
  FeatureEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct StageEntry {
  struct Data {
    Data() : boxflags(),start(),end() {}

    summarizeMetadata::BoxFlags boxflags;
    DateTime start,end;
  };
  StageEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};

extern void copy(std::string metadata_file,std::string URL,std::string caller,std::string user);
extern void write(my::map<FeatureEntry>& feature_table,my::map<StageEntry>& stage_table,std::string caller,std::string user);

} // end namespace gatherxml::markup::FixML

namespace GrML {

struct ParameterEntry {
  ParameterEntry() : key(),start_date_time(),end_date_time(),num_time_steps(0) {}

  std::string key;
  DateTime start_date_time,end_date_time;
  size_t num_time_steps;
};
struct LevelEntry {
  LevelEntry() : key(),units(),parameter_code_table() {}

  std::string key;
  std::string units;
  my::map<ParameterEntry> parameter_code_table;
};
struct GridEntry {
  GridEntry() : key(),level_table(),process_table(),ensemble_table() {}

  std::string key;
  my::map<LevelEntry> level_table;
  my::map<metautils::StringEntry> process_table,ensemble_table;
};

extern std::string write(my::map<GridEntry>& grid_table,std::string caller,std::string user);

} // end namespace gatherxml::markup::GrML

namespace ObML {

struct DataTypeEntry {
  struct Data {
    Data() : map(),nsteps(0),vdata(nullptr) {}

    struct VerticalData {
	VerticalData() : min_altitude(1.e38),max_altitude(-1.e38),avg_res(0.),avg_nlev(0),res_cnt(0),units() {}

	float min_altitude,max_altitude,avg_res;
	size_t avg_nlev,res_cnt;
	std::string units;
    };

    std::string map;
    size_t nsteps;
    std::shared_ptr<VerticalData> vdata;
  };

  DataTypeEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct IDEntry {
  struct Data {
    Data() : S_lat(),N_lat(),W_lon(),E_lon(),min_lon_bitmap(nullptr),max_lon_bitmap(nullptr),start(),end(),nsteps(),data_types_table() {}

    float S_lat,N_lat,W_lon,E_lon;
    std::unique_ptr<float []> min_lon_bitmap,max_lon_bitmap;
    DateTime start,end;
    size_t nsteps;
    my::map<DataTypeEntry> data_types_table;
  };

  IDEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct PlatformEntry {
  PlatformEntry() : key(),boxflags(nullptr) {}

  std::string key;
  std::shared_ptr<summarizeMetadata::BoxFlags> boxflags;
};
struct ObservationData {
  ObservationData();
  bool added_to_ids(std::string observation_type,IDEntry& ientry,std::string data_type,std::string data_type_map,float lat,float lon,double unique_timestamp,DateTime *start_datetime,DateTime *end_datetime = nullptr);
  bool added_to_platforms(std::string observation_type,std::string platform_type,float lat,float lon);
  void set_track_unique_observations(bool track) { track_unique_observations=track; }

  size_t num_types;
  std::unordered_map<size_t,std::string> observation_types;
  std::unordered_map<std::string,size_t> observation_indexes;
  std::vector<my::map<IDEntry> *> id_tables;
  std::vector<my::map<PlatformEntry> *> platform_tables;
  std::unordered_map<std::string,std::vector<std::string>> unique_observation_table;
  std::regex unknown_id_re;
  std::unique_ptr<std::unordered_set<std::string>> unknown_ids;
  bool track_unique_observations,is_empty;
};

extern void copy(std::string metadata_file,std::string URL,std::string caller,std::string user);
extern void write(ObservationData& obs_data,std::string caller,std::string user);

} // end namespace gatherxml::markup::ObML

namespace SatML {

struct Image {
  Image() : date_time(),corners(),center(),xres(0.),yres(0.) {}

  DateTime date_time;
  SatelliteImage::ImageCorners corners;
  SatelliteImage::EarthCoordinate center;
  float xres,yres;
};
struct ImageEntry {
  ImageEntry() : key(),start_date_time(),end_date_time(),image_list() {}

  std::string key;
  DateTime start_date_time,end_date_time;
  std::list<Image> image_list;
};
struct ScanLine {
  ScanLine() : date_time(),first_coordinate(),last_coordinate(),subpoint(),res(0.),width(0.),subpoint_resolution(0.) {}

  DateTime date_time;
  SatelliteS::EarthCoordinate first_coordinate,last_coordinate,subpoint;
  double res;
  float width,subpoint_resolution;
};
struct ScanLineEntry {
  ScanLineEntry() : key(),sat_ID(),start_date_time(),end_date_time(),scan_line_list() {}

  std::string key;
  std::string sat_ID;
  DateTime start_date_time,end_date_time;
  std::list<ScanLine> scan_line_list;
};

extern void write(my::map<ScanLineEntry>& scan_line_table,std::list<std::string>& scan_line_table_keys,my::map<ImageEntry>& image_table,std::list<std::string>& image_table_keys,std::string caller,std::string user);

} // end namespace gatherxml::markup::SatML

extern void write_finalize(bool is_mss_file,std::string filename,std::string ext,std::string tdir_name,std::ofstream& ofs,std::string caller,std::string user);
extern void write_initialize(bool& is_mss_file,std::string& filename,std::string ext,std::string tdir_name,std::ofstream& ofs,std::string caller,std::string user);

} // end namespace gatherxml::markup

} // end namespace gatherxml
