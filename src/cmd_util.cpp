#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <metadata.hpp>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

int main(int argc,char **argv)
{
  metautils::args.args_string=unixutils::unix_args_string(argc,argv,'!');
  std::string cmd_util=argv[0];
  auto idx=cmd_util.rfind("/");
  if (idx != std::string::npos) {
    cmd_util=cmd_util.substr(idx+1);
  }
  std::string user=getenv("USER");
  metautils::read_config(cmd_util,user);
  MySQL::Server server(metautils::directives.database_server,metautils::directives.rdadb_username,metautils::directives.rdadb_password,"dssdb");;
  if (!server) {
    metautils::log_error("unable to connect to database; error: "+server.error(),cmd_util,user);
  }
  MySQL::Query query("stat_flag","dssgrp","logname = '"+user+"'");
  if (query.submit(server) < 0) {
     metautils::log_error("authorization server error: "+query.error(),cmd_util,user);
  }
  MySQL::Row row;
  if (!query.fetch_row(row)) {
    std::cerr << "Error: not authorized" << std::endl;
    exit(1);
  }
  setreuid(15968,15968);
  std::stringstream output,error;
  if (!metautils::args.args_string.empty()) {
    unixutils::mysystem2(metautils::directives.dss_bindir+"/_"+cmd_util+" "+strutils::substitute(metautils::args.args_string,"!"," "),output,error);
  }
  else {
    unixutils::mysystem2(metautils::directives.dss_bindir+"/_"+cmd_util,output,error);
  }
  if (!output.str().empty()) {
    std::cout << output.str() << std::endl;
  }
  if (!error.str().empty()) {
    std::cerr << error.str() << std::endl;
  }
}
