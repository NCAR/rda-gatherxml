#include <fstream>
#include <string>
#include <unistd.h>
#include <sys/stat.h>
#include <sstream>
#include <regex>
#include <PostgreSQL.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <s3.hpp>

using namespace PostgreSQL;
using std::regex;
using std::regex_search;
using std::list;
using std::string;
using std::stringstream;
using strutils::chop;
using strutils::ds_aliases;
using strutils::ftos;
using strutils::replace_all;
using strutils::split;
using strutils::to_sql_tuple_string;
using unixutils::mysystem2;

namespace metautils {

namespace primaryMetadata {

bool prepare_file_for_metadata_scanning(TempFile& tfile, TempDir& tdir, list<
    string> *filelist, string& file_format, string& error) {
  error = "";
  if (filelist != nullptr) {
    filelist->clear();
  }
  if (args.dsid == "test" && args.file_format.empty()) {
    if (filelist != nullptr) {
      filelist->emplace_back(args.path + "/" + args.filename);
      return true;
    }
  }
  Server mys(directives.rdadb_config);
  if (!mys) {
    error = "Error connecting to database server";
    return false;
  }
  if (regex_search(args.path, regex("^https://rda.ucar.edu")) || args.dsid ==
      "test") {

    // Web file
    auto loc = 'G';
    if (args.dsid != "test") {
      auto w = relative_web_filename(args.path + "/" + args.filename);
      LocalQuery q("status, file_format, locflag", "dssdb.wfile_" + args.dsid,
          "wfile = '" + w + "'");
      if (q.submit(mys) < 0) {
        error = q.error();
        return false;
      }
      Row row;
      q.fetch_row(row);
      if (!args.override_primary_check) {
        if (q.num_rows() == 0 || row[0] != "P") {
          error = "Terminating - " + args.path + "/" + args.filename + " is "
              "not active for this dataset";
          return false;
        }
      }
      if (q.num_rows() != 0) {
        file_format = row[1];
        args.file_format = row[1];
        loc = row[2][0];
      }
    }
    string lnm = args.path;
    if (!args.override_primary_check) {
      replace_all(lnm, "https://rda.ucar.edu", "");
      if (lnm.find(directives.data_root_alias) == 0) {
        if (loc == 'B' || loc == 'G') {
          lnm = directives.data_root + lnm.substr(directives.data_root_alias
              .length());
        } else if (loc == 'O') {
          lnm = lnm.substr(directives.data_root_alias.length() + 1);
        }
      }
    }
    lnm += "/" + args.filename;
    struct stat64 s64;
    if (stat64(lnm.c_str(), &s64) != 0 && args.dsid != "test") {

      // file is not in local directory, so get it from it's archive location
      switch (loc) {
        case 'B':
        case 'G': {

          // file is on glade
          auto gnm = args.path + "/" + args.filename;
          replace_all(gnm, "https://rda.ucar.edu/data", directives.data_root);
          if ( (stat64(gnm.c_str(), &s64)) != 0 || s64.st_size == 0) {
            error = "glade file not found or zero length";
            return false;
          }
          if (args.file_format.empty()) {
            if (filelist != nullptr) {
              filelist->emplace_back(gnm);
              return true;
            }
          }
          TempDir t;
          if (!t.create(directives.temp_path)) {
            error = "Error creating temporary directory";
            return false;
          }
          if (system(("cp " + gnm + " " + tfile.name()).c_str()) != 0) {
            error = "error copying glade file " + gnm;
            return false;
          }
          if ( (stat64(tfile.name().c_str(), &s64)) != 0 || s64.st_size == 0) {
            error = "Web file '" + args.path + "/" + args.filename + "' not "
                "found or unable to transfer";
            return false;
          }
          break;
        }
        case 'O': {

          // file is on object storage
          s3::Session s3("stratus.ucar.edu", "AK06XKLYCIANHSDVOSL6",
              "bN3i7jcp3avElZ1/cSI1zTloy50Lbqcwz04ajD5Q", "us-east-1",
              "aws4_request");
          if (!s3.download_file("rda-data", lnm, tfile.name())) {
            error = "Object store file '" + lnm + "' not found or unable to "
                "transfer";
            return false;
          }
          break;
        }
      }
    } else {
      // if no file format, open in place
      if (args.file_format.empty()) {
        if (filelist != nullptr) {
          filelist->emplace_back(lnm);
          return true;
        }
      }

      // make a copy of the local file because we might need to remove blocking,
      //    uncompress, etc.
      if (system(("cp " + lnm + " " + tfile.name()).c_str()) != 0) {
        error = "error copying local file " + lnm + " to temporary";
        return false;
      }
    }
  } else {
    error = "path of file '" + args.path + "' not recognized";
    return false;
  }
  auto tf = tfile.name();
  auto bufr_re = regex("bufr");
  if (args.file_format.empty()) {
    if (args.data_format == "grib" ||  args.data_format == "grib2" ||
        regex_search(args.data_format, bufr_re) || args.data_format ==
        "mcidas") {

      // check to see if file is COS-blocked
      stringstream oss, ess;

      if (mysystem2(directives.decs_bindir + "/cosfile " + tf, oss,
          ess)) { } // suppress compiler warning
      if (ess.str().empty()) {
        if (system((directives.decs_bindir + "/cosconvert -b " + tf + " 1> /dev"
            "/null").c_str()) != 0) {
          error = "error cosconverting " + tf + " to a binary file";
          return false;
        }
      } else if (!regex_search(ess.str(), regex("error on record 1"))) {
        error = "unable to open '" + tf + "'; error: " + ess.str();
        return false;
      }
      while (expand_file(tdir.name(), tf, &args.file_format)) { }
    } else {
      while (expand_file(tdir.name(), tf, &args.file_format)) { }
    }
    if (filelist != nullptr) {
      filelist->emplace_back(tf);
    }
  } else {
    auto ff = split(args.file_format, ".");
    if (ff.size() > 3) {
      error = "archive file format is too complicated";
      return false;
    }
    list<string> flist;
    auto hdf_re = regex("hdf");
    auto netcdf_re = regex("netcdf");
    for (int n = ff.size() - 1; n >= 0; --n) {
      if (ff[n] == "BI") {
        if (n == 0 || ff[n - 1] != "LVBS") {
          if (args.data_format == "grib" || args.data_format == "grib2" ||
              regex_search(args.data_format, bufr_re) || args.data_format ==
              "mcidas") {
            stringstream oss, ess;
            mysystem2(directives.decs_bindir + "/cosconvert -b " + tf + " " +
                tf + ".bi", oss, ess);
            if (!ess.str().empty()) {
              error = ess.str();
              return false;
            } else {
              if (system(("mv " + tf + ".bi " + tf).c_str()) != 0) {
                error = "error overwriting " + tf + " with binary version";
                return false;
              }
            }
          }
        }
      } else if (ff[n] == "CH" || ff[n] == "C1") {
        if (args.data_format == "grib" || args.data_format == "grib2" ||
            regex_search(args.data_format, bufr_re) || args.data_format ==
            "mcidas") {
          if (system((directives.decs_bindir + "/cosconvert -c " + tf + " 1> "
              "/dev/null").c_str()) != 0) {
            error = "error cosconverting " + tf;
            return false;
          }
        }
      } else if (ff[n] == "GZ" || ff[n] == "BZ2") {
        string cmd, ext = strutils::to_lower(ff[n]);
        if (ff[n] == "GZ") {
          cmd = "gunzip";
        } else if (ff[n] == "BZ2") {
          cmd = "bunzip2";
        }
        if (ff[n] == ff.back()) {
          if (system(("mv " + tf + " " + tf + "." + ext + "; " + cmd + " -f " +
              tf + "." + ext).c_str()) != 0) {
            error = "error uncompressing " + tf;
            return false;
          }
        } else if (ff[n] == ff.front()) {
          if ((args.data_format == "cxml" || args.data_format == "tcvitals" ||
              regex_search(args.data_format, netcdf_re) || regex_search(args
              .data_format, hdf_re) || regex_search(args.data_format,
              bufr_re)) && filelist != nullptr) {
            for (const auto& f : *filelist) {
              flist.emplace_back(f);
            }
            filelist->clear();
          }
          auto ofs = fopen64(tf.c_str(), "w");
          for (auto f : flist) {
            if (system((cmd + " -f " + f).c_str()) != 0) {
              error = "error uncompressing " + f;
              return false;
            }
            replace_all(f, "." + ext, "");
            if (args.data_format != "cxml" && args.data_format != "tcvitals" &&
                !regex_search(args.data_format, netcdf_re) && !regex_search(
                args.data_format, hdf_re)) {
              auto ifs64 = fopen64(f.c_str(), "r");
              if (ifs64 == nullptr) {
                error = "error while combining ." + ext + " files - could not "
                    "open " + f;
                return false;
              }
              char buf[32768];
              int nb;
              while ( (nb = fread(buf, 1, 32768, ifs64)) > 0) {
                fwrite(buf, 1, nb, ofs);
              }
              fclose(ifs64);
            } else {
              if (filelist != nullptr) {
                filelist->emplace_back(f);
              } else {
                error = "non-null filelist must be provided for format " + args
                    .data_format;
                return false;
              }
            }
          }
          fclose(ofs);
        } else {
          error = "archive file format is too complicated";
          return false;
        }
      } else if (ff[n] == "TAR" || ff[n] == "TGZ") {
        if (n == 0 && args.data_format != "cxml" && args.data_format !=
            "tcvitals" && !regex_search(args.data_format, netcdf_re) &&
            !regex_search(args.data_format,regex("nc$")) && !regex_search(args
            .data_format, hdf_re) && args.data_format != "mcidas" && args
            .data_format != "uadb") {
          if (regex_search(args.data_format, regex("^grib"))) {
            expand_file(tdir.name(), tf, nullptr);
          }
        } else if (args.data_format == "cxml" || args.data_format ==
            "tcvitals" || regex_search(args.data_format, netcdf_re) ||
            regex_search(args.data_format,regex("nc$")) || regex_search(args
           .data_format, hdf_re) || args.data_format == "mcidas" || args
           .data_format == "uadb" || regex_search(args.data_format, bufr_re) ||
           n == static_cast<int>(ff.size() - 1)) {
          list<string> *f;
          if (args.data_format == "cxml" || args.data_format == "tcvitals" ||
              regex_search(args.data_format, netcdf_re) || regex_search(args
             .data_format,regex("nc$")) || regex_search(args.data_format,
             hdf_re) || args.data_format == "mcidas" || args.data_format ==
             "uadb") {
            if (filelist != nullptr) {
              f = filelist;
            } else {
              error = "non-null filelist must be provided for format " + args
                  .data_format;
              return false;
            }
          } else {
            f = &flist;
          }
          auto p = popen(("tar tvf " + tf + " 2>&1").c_str(), "r");
          char line[256];
          while (fgets(line, 256, p) != nullptr) {
            if (regex_search(line, regex("checksum error"))) {
              error = "tar extraction failed - is it really a tar file?";
              return false;
            } else if (line[0] == '-') {
              string s = line;
              chop(s);
              auto x = split(s, "");
              f->emplace_back(tdir.name() + "/" + x.back());
            }
          }
          pclose(p);
          auto tar_file = tf.substr(tf.rfind("/") + 1);
          if (system(("mv " + tf + " " + tdir.name() + "/; cd " + tdir.name() +
              "; tar xvf " + tar_file + " 1> /dev/null 2>&1").c_str()) != 0) {
            error = "error untarring " + tar_file;
            return false;
          }
        } else {
          error = "archive file format is too complicated";
          return false;
        }
      } else if (ff[n] == "VBS") {
        if (static_cast<int>(ff.size()) >= (n + 2) && ff[n + 1] == "BI") {
          if (system((directives.decs_bindir + "/cosconvert -v " + tf + " " + tf
              + ".vbs 1> /dev/null;mv " + tf + ".vbs " + tf).c_str()) != 0) {
            error = "error cosconverting " + tf + " to a .vbs file";
            return false;
          }
        }
      } else if (ff[n] == "LVBS") {
        if (static_cast<int>(ff.size()) >= (n + 2) && ff[n + 1] == "BI") {
          if (system((directives.decs_root + "/bin/cossplit -p " + tf + " " +
              tf) .c_str()) != 0) {
            error = "error cosspliting " + tf;
            return false;
          }
          auto fnum = 2;
          string cos = directives.decs_root + "/bin/coscombine noeof";
          auto f = tf + ".f" + ftos(fnum, 3, 0, '0');
          struct stat64 s64;
          while (stat64(f.c_str(), &s64) == 0) {
            cos += " " + f;
            fnum += 3;
            f = tf + ".f" + ftos(fnum, 3, 0, '0');
          }
          cos += " " + tf + " 1> /dev/null";
          if (system(cos.c_str()) != 0) {
            error = "coscombine error for '" + cos + "'";
            return false;
          }
          if (system(("rm -f " + tf + ".f*").c_str()))
              { } // suppress compiler warning
          if (system((directives.decs_bindir + "/cosconvert -v " + tf + " " + tf
              + ".vbs 1> /dev/null;mv " + tf + ".vbs " + tf).c_str()) != 0) {
            error = "error cosconverting " + tf + " to a .vbs file";
            return false;
          }
        }
      } else if (ff[n] == "Z") {
        if (n == static_cast<int>(ff.size() - 1)) {
          if (system(("mv " + tf + " " + tf + ".Z; uncompress " + tf).c_str())
              != 0) {
            error = "error Z-uncompressing " + tf + ".Z";
            return false;
          }
        } else if (n == 0) {
          auto ofs = fopen64(tf.c_str(), "w");
          for (auto f : flist) {
            if (system(("uncompress " + f).c_str()) != 0) {
              error = "error Z-uncompressing " + f;
              return false;
            }
            replace_all(f, ".Z", "");
            if (!regex_search(args.data_format, netcdf_re) && !regex_search(args
                .data_format, hdf_re)) {
              std::ifstream ifs(f.c_str());
              if (!ifs) {
                error = "error while combining .Z files";
                return false;
              }
              while (!ifs.eof()) {
                char buf[32768];
                ifs.read(buf, 32768);
                auto cnt = ifs.gcount();
                fwrite(buf, 1, cnt, ofs);
              }
              ifs.close();
            } else {
              if (filelist != nullptr) {
                filelist->emplace_back(f);
              } else {
                error = "non-null filelist must be provided for format " +
                    args.data_format;
                return false;
              }
            }
          }
          fclose(ofs);
        } else {
          error = "archive file format is too complicated";
          return false;
        }
      } else if (ff[n] == "RPTOUT") {

        // no action needed for these archive formats
      } else {
        error = "don't recognize '" + ff[n] + "' in archive format field for "
            "this HPSS file";
        return false;
      }
    }
  }
  mys.disconnect();
  return true;
}

} // end namespace primaryMetadata

} // end namespace metautils
