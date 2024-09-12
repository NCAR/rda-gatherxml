#include <gatherxml.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using miscutils::this_function_label;
using std::cerr;
using std::endl;
using std::string;
using strutils::ng_gdex_id;
using strutils::to_lower;

extern const string USER;

namespace gatherxml {

void parse_args(char arg_delimiter) {
  static const string F = this_function_label(__func__);
  verbose_operation = false;
  metautils::args.temp_loc = metautils::directives.temp_path;
  auto sp = strutils::split(metautils::args.args_string, string(1,
      arg_delimiter));
  for (size_t n = 0; n < sp.size()-1; ++n) {
    if (sp[n] == "-d") {
      metautils::args.dsid = sp[++n];
      if (metautils::args.dsid != "test") {
        metautils::args.dsid = ng_gdex_id(metautils::args.dsid);
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
      verbose_operation = true;
    } else if (sp[n] == "-NC") {
      if (USER == "dattore") {
        metautils::args.override_primary_check = true;
      }
    } else if (sp[n] == "-OO") {
      if (USER == "dattore") {
        metautils::args.overwrite_only = true;
      }
    } else  {
      myerror = "Terminating - fatal error: " + F + ": invalid flag '" + sp[n] +
          "'";
      exit(1);
    }
  }
  if (metautils::args.data_format.empty()) {
    myerror = "Terminating - fatal error: " + F + ": no format specified";
    exit(1);
  } else {
    metautils::args.data_format = to_lower(metautils::args.data_format);
  }
  if (metautils::args.data_format == "grib1") {
    metautils::args.data_format = "grib";
  }
  if (metautils::args.dsid.empty()) {
    myerror = "Terminating - fatal error: " + F + ": no or invalid dataset ID "
        "specified";
    exit(1);
  }
  auto idx = sp.back().rfind("/");
  metautils::args.path = sp.back().substr(0, idx);
  metautils::args.filename = sp.back().substr(idx + 1);
  if (metautils::args.dsid == "test") {
    if (metautils::args.path[0] != '/') {
      myerror = "Terminating - fatal error: " + F + ": full path of test files "
          "must be specified";
      exit(1);
    }
    metautils::args.override_primary_check = true;
    metautils::args.update_db = false;
    metautils::args.update_summary = false;
    metautils::args.regenerate = false;
  }
}

} // end namespace gatherxml
