#define BOOST_TEST_MODULE MetautilsTest
#include <boost/test/included/unit_test.hpp>
#include <iostream>
#include <gatherxml.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

BOOST_AUTO_TEST_SUITE(Metautils_Tests)

BOOST_AUTO_TEST_CASE(Directives_Test)
{
  metautils::read_config("metautils_test",getenv("USER"));
  BOOST_REQUIRE_MESSAGE(!metautils::directives.dss_bindir.empty(),"gatherxml bin directory could not be determined from the host name");
}

BOOST_AUTO_TEST_SUITE_END()
