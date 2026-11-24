#include <gatherxml.hpp>
#include <metadata.hpp>
#include <utils.hpp>

using metautils::log_error2;
using miscutils::this_function_label;
using std::cout;
using std::endl;
using std::string;

namespace gatherxml {

namespace utils {

DateTime compute_nc_time(double time_val, const metautils::NcTime::TimeData&
    time_data, const string& user) {
  static const string F = this_function_label(__func__);
  static const int Y = dateutils::current_date_time().year();
  if (time_val < 0.) {
    log_error2("Terminating - negative time offset not allowed", F, "nc2xml",
        user);
  }
  string e;
  auto dt = metautils::NcTime::actual_date_time(time_val, time_data, e);
  if (!e.empty()) {
    log_error2("error: '" + e + "' for units '" + time_data.units + "'", F,
        "nc2xml", user);
  }
  if (gatherxml::verbose_operation && dt.year() > Y) {
    cout << "Warning: " << dt.to_string() << " is in the future; time value: "
        << time_val << endl;
  }
  return dt;
}

} // end namespace utils::gatherxml

} // end namespace gatherxml
