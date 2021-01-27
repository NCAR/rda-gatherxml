#include <gatherxml.hpp>
#include <strutils.hpp>

extern const std::string USER;

namespace gatherxml {

extern bool verbose_operation;

void parse_args(char arg_delimiter)
{
  verbose_operation=false;
  metautils::args.temp_loc=metautils::directives.temp_path;
  auto args=strutils::split(metautils::args.args_string,std::string(1,arg_delimiter));
  for (size_t n=0; n < args.size()-1; ++n) {
    if (args[n] == "-d") {
	metautils::args.dsnum=args[++n];
	if (metautils::args.dsnum.substr(0,2) == "ds") {
	  metautils::args.dsnum=metautils::args.dsnum.substr(2);
	}
    }
    else if (args[n] == "-f") {
	metautils::args.data_format=args[++n];
    }
    else if (args[n] == "-G") {
	metautils::args.update_graphics=false;
    }
    else if (args[n] == "-I") {
	metautils::args.inventory_only=true;
	metautils::args.update_db=false;
    }
    else if (args[n] == "-m") {
	metautils::args.member_name=args[++n];
    }
    else if (args[n] == "-R") {
	metautils::args.regenerate=false;
    }
    else if (args[n] == "-S") {
	metautils::args.update_summary=false;
    }
    else if (args[n] == "-t") {
	metautils::args.temp_loc=args[++n];
    }
    else if (args[n] == "-U") {
	if (USER == "dattore") {
	  metautils::args.update_db=false;
	}
    }
    else if (args[n] == "-V") {
	verbose_operation=true;
    }
    else if (args[n] == "-NC") {
	if (USER == "dattore") {
	  metautils::args.override_primary_check=true;
	}
    }
    else if (args[n] == "-OO") {
	if (USER == "dattore") {
	  metautils::args.overwrite_only=true;
	}
    }
  }
  if (metautils::args.data_format.empty()) {
    std::cerr << "no format specified" << std::endl;
    exit(1);
  }
  else {
    metautils::args.data_format=strutils::to_lower(metautils::args.data_format);
  }
  if (metautils::args.data_format == "grib1") {
    metautils::args.data_format="grib";
  }
  if (metautils::args.dsnum.empty()) {
    std::cerr << "no dataset number specified" << std::endl;
    exit(1);
  }
  if (metautils::args.dsnum == "999.9") {
    metautils::args.override_primary_check=true;
    metautils::args.update_db=false;
    metautils::args.update_summary=false;
    metautils::args.regenerate=false;
  }
  auto idx=args.back().rfind("/");
  metautils::args.path=args.back().substr(0,idx);
  metautils::args.filename=args.back().substr(idx+1);
}

} // end namespace gatherxml
