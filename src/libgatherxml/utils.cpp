#include <gatherxml.hpp>

using std::string;

namespace gatherxml {

string this_function_label(string function_name) {
  return string(function_name + "()");
}

} // end namespace gatherxml
