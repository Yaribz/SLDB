#!/usr/bin/env perl
#
# Copyright (C) 2022  Yann Riou <yaribzh@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# Version 0.1 (2022/01/31)

use warnings;
use strict;

use List::Util 'any';

use FindBin;
use lib $FindBin::Bin;

use SimpleConf;

sub usage {
  print <<EOU;
Usage:
  $0 <confFile> <setting1>:<value1> [<setting2>:<value2>]
  $0 <confFile> --from-file <sourceFile>
      (values can contain \${NAME} placeholders which will be replaced the corresponding environment variable values)
EOU
  exit;
}

usage() unless($#ARGV > 0 && -f $ARGV[0]);

my $r_updates={};
if(any {$ARGV[1] eq $_} (qw'--from-file -f')) {
  usage() unless($#ARGV == 2);
  SimpleConf::readConf($ARGV[2],$r_updates);
}else{
  foreach my $param (@ARGV[1..$#ARGV]) {
    if($param =~ /([^:]+):(.*)$/) {
      $r_updates->{$1}=$2;
    }else{
      usage();
    }
  }
}

usage() unless(%{$r_updates});

foreach my $v (values %{$r_updates}) {
  $v =~ s/\$\{([^\}]+)\}/$ENV{$1}/g;
}

SimpleConf::updateConf($ARGV[0],$r_updates);
