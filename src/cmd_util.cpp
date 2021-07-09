#include <iostream>
#include <unistd.h>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using metautils::log_error2;
using std::endl;
using std::cerr;
using std::cout;
using std::string;
using std::stringstream;
using strutils::substitute;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

int main(int argc, char **argv) {
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '!');
  string exec = argv[0];
  auto idx = exec.rfind("/");
  if (idx != string::npos) {
    exec = exec.substr(idx + 1);
  }
  string u = getenv("USER");
  if (!metautils::read_config(exec, u)) {
    log_error2("configuration error: '" + myerror + "'", "main()", exec, u);
  }
  MySQL::Server srv(metautils::directives.database_server, metautils::
      directives.rdadb_username, metautils::directives.rdadb_password, "dssdb");
  if (!srv) {
    log_error2("unable to connect to database; error: " + srv.error(), "main()",
        exec, u);
  }
  MySQL::Query q("stat_flag", "dssgrp", "logname = '" + u + "'");
  if (q.submit(srv) < 0) {
    log_error2("authorization server error: " + q.error(), "main()", exec, u);
  }
  MySQL::Row row;
  if (!q.fetch_row(row)) {
    cerr << "Error: not authorized" << endl;
    return 1;
  }
  setreuid(15968, 15968);
  stringstream oss, ess;
  if (!metautils::args.args_string.empty()) {
    mysystem2(metautils::directives.decs_bindir + "/_" + exec + " " +
        substitute(metautils::args.args_string, "!", " "),
        oss, ess);
  } else {
    mysystem2(metautils::directives.decs_bindir + "/_" + exec, oss, ess);
  }
  if (!oss.str().empty()) {
    cout << oss.str() << endl;
  }
  if (!ess.str().empty()) {
    cerr << ess.str() << endl;
    return 1;
  }
  return 0;
}
