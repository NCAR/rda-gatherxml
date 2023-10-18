#include <string>
#include <sys/stat.h>
#include <gatherxml.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <bitmap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>

using namespace MySQL;
using metautils::log_error2;
using miscutils::this_function_label;
using std::string;
using std::unordered_map;
using strutils::split;
using strutils::strand;
using strutils::substitute;

namespace gatherxml {

namespace summarizeMetadata {

void summarize_fix_data(string caller, string user) {
  static const string F = this_function_label(__func__);
  string dsnum2 = substitute(metautils::args.dsnum, ".", "");
  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  Query formats_query("code, format_code", "WFixML.ds" + dsnum2 + "_webfiles2");
  if (formats_query.submit(server) < 0) {
    log_error2(formats_query.error(), F, caller, user);
  }
  unordered_map<string, string> data_file_formats_map;
  for (const auto& format_row : formats_query) {
    data_file_formats_map.emplace(format_row[0], format_row[1]);
  }
  string error;
  LocalQuery locations_query("file_code, classification_code, start_date, "
      "end_date, box1d_row, box1d_bitmap", "WFixML.ds" + dsnum2 + "_locations");
  if (locations_query.submit(server) < 0) {
    log_error2(locations_query.error(), F, caller, user);
  }
  my::map<SummaryEntry> summary_table;
  for (const auto& location_row : locations_query) {
    if (data_file_formats_map.find(location_row[0]) == data_file_formats_map.
        end()) {
      log_error2("found a webID (" + location_row[0] + ") in WFixML.ds" + dsnum2
          + "_locations that doesn't exist in WFixML.ds" + dsnum2 +
          "_webfiles2", F, caller, user);
    }
    SummaryEntry se;
    se.key = data_file_formats_map[location_row[0]] + "<!>" + location_row[1] +
        "<!>" + location_row[4];
    if (!summary_table.found(se.key, se)) {
      se.start_date = location_row[2];
      se.end_date = location_row[3];
      se.box1d_bitmap = location_row[5];
      summary_table.insert(se);
    } else {
      if (location_row[2] < se.start_date) {
        se.start_date = location_row[2];
      }
      if (location_row[3] > se.end_date) {
        se.end_date = location_row[3];
      }
      if (location_row[5] != se.box1d_bitmap) {
        se.box1d_bitmap = bitmap::add_box1d_bitmaps(se.box1d_bitmap,
            location_row[5]);
      }
      summary_table.replace(se);
    }
  }
  auto uflg = strand(3);
  for (const auto& key : summary_table.keys()) {
    auto parts = split(key, "<!>");
    SummaryEntry se;
    summary_table.found(key, se);
    if (server.insert(
          "search.fix_data",
          "dsid, format_code, classification_code, start_date, end_date, "
              "box1d_row, box1d_bitmap, uflg",
          "'" + metautils::args.dsnum + "', " + parts[0] + ", " + parts[1] +
              ", '" + se.start_date + "', '" + se.  end_date + "', " + parts[2]
              + ", '" + se.box1d_bitmap + "', '" + uflg + "'",
          "update uflg = values(uflg)"
          ) < 0) {
      if (server.error().find("Duplicate entry") != 0) {
        log_error2(server.error(), F, caller, user);
      }
    }
  }
  server._delete("search.fix_data", "dsid = '" + metautils::args.dsnum + "' "
      "and uflg != '" + uflg + "'");
  summarize_locations("WFixML", error);
  if (!error.empty()) {
    log_error2("summarize_locations() returned '" + error + "'", F, caller,
        user);
  }
  server.disconnect();
}

} // end namespace gatherxml::summarizeMetadata

} // end namespace gatherxml
