#ifndef PGLOCKS_H
#define   PGLOCKS_H

#include <unordered_map>
#include <string>

namespace gatherxml {

namespace pglocks {

std::unordered_map<std::string, long long> pglocks{
    { "WGrML.summary", 0x4000000000000000 },
    { "WGrML.dsnnnn_agrids_cache", 0x2000000000000000 }
};

} // end namespace pglocks

} // end namespace gatherxml

#endif
