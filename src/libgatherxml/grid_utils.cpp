#include <numeric>
#include <gatherxml.hpp>
#include <utils.hpp>

using floatutils::myequalf;
using std::accumulate;
using std::cout;
using std::endl;
using std::make_pair;
using std::vector;

namespace gatherxml {

namespace gridutils {

bool grid_is_centered_lambert_conformal(const vector<double>& lats, const
    vector<double>& lons, Grid::GridDimensions& dims, Grid::GridDefinition&
    def) {
  if (verbose_operation) {
    cout << "         ... checking for centered Lambert-Conformal projection "
        "..." << endl;
  }
  auto dx2 = dims.x / 2;
  auto dy2 = dims.y / 2;
  switch (dims.x % 2) {
    case 0: {
      auto xm = dx2 - 1;
      auto ym = dy2 - 1;
      auto yp = dy2 + 1;
      if (myequalf((lons[ym * dims.x + xm] + lons[ym * dims.x + dx2]), (lons[yp
          * dims.x + xm] + lons[yp * dims.x + dx2]), 0.00001) && myequalf(lats[
          dy2 * dims.x + xm], lats[dy2 * dims.x + dx2], 0.00001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(lats[dy2 *
            dims.x + dx2]);
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround((lons[dy2 * dims.x + xm] + lons[dy2 * dims.x +
            dx2]) / 2.);
        def.dx = def.dy = lround(111.1 * cos(lats[dy2 * dims.x + xm] *
            3.141592654 / 180.) * (lons[dy2 * dims.x + dx2] - lons[dy2 * dims.x
            + xm]));
        if (verbose_operation) {
          cout << "            ... confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
    case 1: {
      if (myequalf(lons[(dy2 - 1) * dims.x + dx2], lons[(dy2 + 1) * dims.x +
          dx2], 0.00001) && myequalf(lats[dy2 * dims.x + dx2 - 1], lats[dy2 *
          dims.x + dx2 + 1], 0.00001)) {
        def.type = Grid::Type::lambertConformal;
        def.llatitude = def.stdparallel1 = def.stdparallel2 = lround(lats[dy2 *
            dims.x + dx2]);
        if (def.llatitude >= 0.) {
          def.projection_flag = 0;
        } else {
          def.projection_flag = 1;
        }
        def.olongitude = lround(lons[dy2 * dims.x + dx2]);
        def.dx = def.dy = lround(111.1 * cos(lats[dy2 * dims.x + dx2] *
            3.141592654 / 180.) * (lons[dy2 * dims.x + dx2 + 1] - lons[dy2 *
            dims.x + dx2]));
        if (verbose_operation) {
          cout << "            ... confirmed a centered Lambert-Conformal "
              "projection." << endl;
        }
        return true;
      }
      break;
    }
  }
  if (verbose_operation) {
    cout << "         ... done." << endl;
  }
  return false;
}

size_t find_non_centered_lambert_conformal_stdparallel(const vector<double>& v,
    Grid::GridDefinition& def, const Grid::GridDimensions& dims, size_t xbar,
    const vector<double>& lats) {
  size_t i = 0; // return value
  def.stdparallel1 = def.stdparallel2 = -99.;
  for (size_t n = 0; n < v.size(); ++n) {
    if (myequalf(v[n], def.dx, 0.001)) {
      auto p = lround(lats[n  * dims.x + xbar]);
      if (def.stdparallel1 < -90.) {
        def.stdparallel1 = p;
        i = xbar + n * dims.x;
      } else if (def.stdparallel2 < -90.) {
        if (p != def.stdparallel1) {
          def.stdparallel2 = p;
        }
      } else if (p != def.stdparallel2) {
        def.stdparallel1 = def.stdparallel2 = -99.;
        return 0;
      }
    }
  }
  return i;
}

bool grid_is_non_centered_lambert_conformal(const vector<double>& lats, const
    vector<double>& lons, Grid::GridDimensions& dims, Grid::GridDefinition&
    def) {
  if (verbose_operation) {
    cout << "         ... checking for a non-centered Lambert-Conformal "
        "projection ..." << endl;
  }
  def.type = Grid::Type::not_set;

  // find the x-offsets in each row where the change in latitude goes from
  //   positive to negative
  vector<double> v;
  v.reserve(dims.y);
  double s = 0.;
  for (auto n = 0; n < dims.y; ++n) {
    auto yoff = n * dims.x;
    for (auto m = 0; m < dims.x - 1; ++m) {
      auto xoff = yoff + m;
      if (lats[xoff + 1] <= lats[xoff]) {
        v.emplace_back(xoff - yoff);
        s += lons[xoff];
        break;
      }
    }
  }

  // find the variance in the x-offsets
  auto xbar = lround(accumulate(v.begin(), v.end(), 0.) / v.size());
  double d = 0.;
  for (const auto& e : v) {
    auto x = e - xbar;
    d += x * x;
  }
  auto var = d / (v.size() - 1);
  if (var >= 1.) {
    return false;
  }

  // if the variance is low, confident that we found the orientation longitude
  def.type = Grid::Type::lambertConformal;
  def.olongitude = lround(s / v.size());

  // find the x-direction distance for each row at the orientation longitude
  const double PI = 3.141592654;
  const double DEGRAD = PI / 180.;
  const double KMDEG = 111.1;
  v.clear();
  auto min_floor = make_pair(99., 0);
  auto min_ceil = make_pair(99., 0);
  for (auto n = xbar; n < dims.x * dims.y; n += dims.x) {
    auto dx = (lons[n+1] - lons[n]) * KMDEG * cos((lats[n] + lats[n+1]) / 2. *
        DEGRAD);
    auto dy = (lats[n+1] - lats[n]) * KMDEG;
    auto dist = sqrt(dx * dx + dy * dy);
    auto diff = dist - floor(dist);
    min_floor.first = std::min(diff, min_floor.first);
    if (min_floor.first == diff) {
      min_floor.second = v.size();
    }
    diff = ceil(dist) - dist;
    min_ceil.first = std::min(diff, min_ceil.first);
    if (min_ceil.first == diff) {
      min_ceil.second = v.size();
    }
    v.emplace_back(sqrt(dx * dx + dy * dy));
  }
  def.dx = lround(accumulate(v.begin(), v.end(), 0.) / v.size());
  auto i = find_non_centered_lambert_conformal_stdparallel(v, def, dims, xbar,
      lats);
  if (def.stdparallel1 < -90.) {
    def.dx = (min_floor.first <= min_ceil.first) ? lround(v[min_floor.second]) :
        lround(v[min_ceil.second]);
    i = find_non_centered_lambert_conformal_stdparallel(v, def, dims, xbar,
        lats);
    if (def.stdparallel1 < -90.) {
      if (verbose_operation) {
        cout << "            ... check for a non-centered projection failed. "
            "No tangent latitude could be identified." << endl;
      }
      def.type = Grid::Type::not_set;
      return false;
    }
  } else if (def.stdparallel2 < -90.) {
    def.stdparallel2 = def.stdparallel1;
  }
  def.llatitude = def.stdparallel1;
  def.projection_flag = def.llatitude >= 0. ? 0 : 1;
  auto dx = (lons[i] - lons[i-dims.x]) * KMDEG * cos(lats[i-dims.x] * DEGRAD);
  auto dy = (lats[i] - lats[i-dims.x]) * KMDEG;
  def.dy = lround(sqrt(dx * dx + dy * dy));
  if (verbose_operation) {
    cout << "            ... confirmed a non-centered Lambert-conformal "
        "projection." << endl;
    cout << "         ... done." << endl;
  }
  return true;
}

bool grid_is_lambert_conformal(const vector<double>& lats, const vector<
    double>& lons, Grid::GridDimensions& dims, Grid::GridDefinition& def) {
  if (verbose_operation) {
    cout << "      ... checking for Lambert-Conformal projection ..." << endl;
  }
  if (grid_is_centered_lambert_conformal(lats, lons, dims,
      def)) {
    return true;
  }
  if (verbose_operation) {
    cout << "      ...check for a centered Lambert-Conformal projection "
        "failed, checking for a non-centered projection..." << endl;
  }
  if (grid_is_non_centered_lambert_conformal(lats, lons,
      dims, def)) {
    return true;
  }
  if (verbose_operation) {
    cout << "      ...check for a Lambert-Conformal projection finished. Not "
        "an LC projection." << endl;
  }
  return false;
}

} // end namespace gridutils

} // end namespace gatherxml
