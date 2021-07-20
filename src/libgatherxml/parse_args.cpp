#include <gatherxml.hpp>
#include <strutils.hpp>

using std::cerr;
using std::endl;
using std::string;
using strutils::to_lower;

extern const string USER;

namespace gatherxml {

extern bool verbose_operation;

void parse_args(char arg_delimiter) {
  verbose_operation = false;
  metautils::args.temp_loc = metautils::directives.temp_path;
  auto sp = strutils::split(metautils::args.args_string, string(1,
      arg_delimiter));
  for (size_t n = 0; n < sp.size()-1; ++n) {
    if (sp[n] == "-d") {
      metautils::args.dsnum = sp[++n];
      if (metautils::args.dsnum.substr(0, 2) == "ds") {
        metautils::args.dsnum = metautils::args.dsnum.substr(2);
      }
    } else if (sp[n] == "-f") {
      metautils::args.data_format = sp[++n];
    } else if (sp[n] == "-G") {
      metautils::args.update_graphics = false;
    } else if (sp[n] == "-I") {
      metautils::args.inventory_only = true;
      metautils::args.update_db = false;
    } else if (sp[n] == "-m") {
      metautils::args.member_name = sp[++n];
    } else if (sp[n] == "-R") {
      metautils::args.regenerate = false;
    } else if (sp[n] == "-S") {
      metautils::args.update_summary = false;
    } else if (sp[n] == "-t") {
      metautils::args.temp_loc = sp[++n];
    } else if (sp[n] == "-U") {
      if (USER == "dattore") {
        metautils::args.update_db = false;
      }
    } else if (sp[n] == "-V") {
      verbose_operation=true;
    } else if (sp[n] == "-NC") {
      if (USER == "dattore") {
        metautils::args.override_primary_check = true;
      }
    } else if (sp[n] == "-OO") {
      if (USER == "dattore") {
        metautils::args.overwrite_only = true;
      }
    } else  {
      cerr << "invalid flag '" << sp[n] << "'" << endl;
      exit(1);
    }
  }
  if (metautils::args.data_format.empty()) {
    cerr << "no format specified" << endl;
    exit(1);
  } else {
    metautils::args.data_format = to_lower(metautils::args.data_format);
  }
  if (metautils::args.data_format == "grib1") {
    metautils::args.data_format = "grib";
  }
  if (metautils::args.dsnum.empty()) {
    cerr << "no dataset number specified" << endl;
    exit(1);
  }
  if (metautils::args.dsnum == "test") {
    metautils::args.override_primary_check = true;
    metautils::args.update_db = false;
    metautils::args.update_summary = false;
    metautils::args.regenerate = false;
  }
  auto idx = sp.back().rfind("/");
  if (idx != string::npos) {
    metautils::args.path = sp.back().substr(0, idx);
    metautils::args.filename = sp.back().substr(idx + 1);
  } else {
    metautils::args.path = ".";
    metautils::args.filename = sp.back();
  }
}

} // end namespace gatherxml
