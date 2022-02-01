
#!/usr/bin/perl -w
#
# This file implements the init process for SLDB DB, it is part of SLDB.
#
# The setup process for SLDB consists in two main tasks:
# - database initialization
#
# Copyright (C) 2013-2020  Amadeus Folego <amadeusfolego@gmail.com>
#
# SLDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# SLDB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with SLDB.  If not, see <http://www.gnu.org/licenses/>.
#

use strict;

use File::Basename qw/fileparse/;
use File::Spec::Functions qw/catdir catfile file_name_is_absolute rel2abs/;
use File::Path;
use FileHandle;
use Term::ANSIColor;
use Term::UI;
use Term::ReadLine;

my ($scriptBaseName,$scriptDir)=fileparse(rel2abs($0),'.pl');
unshift(@INC,$scriptDir);
require SimpleConf;
require SimpleLog;
require Sldb;

my $confFile=catfile($scriptDir,'etc',"$scriptBaseName.conf");
my %conf=(dbName => undef,
          dbLogin => undef,
          dbPwd => undef);
my $sldb;
my $dbOk=0;
my $tbOk=0;
my $compConfOk=0;

my $t = Term::ReadLine->new('SLDB DB Init');
my $tOut = $t->OUT() || \*STDOUT;
my $tIn = $t->IN() || \*STDIN;
sub p { print $tOut "$_[0]\n" }

sub checkDb {
  my $dbDs=$conf{dbName};
  $dbDs="DBI:mysql:database=$dbDs;host=localhost" unless($dbDs =~ /^DBI:/i);
  my $sLogSldb=SimpleLog->new(logFiles => [''],
                              logLevels => [2],
                              useANSICodes => [1],
                              useTimestamps => [0],
                              prefix => "[SLDB] ");
  $sldb->disconnect() if($dbOk);
  $sldb=Sldb->new({dbDs => $dbDs,
                   dbLogin => $conf{dbLogin},
                   dbPwd => $conf{dbPwd},
                   sLog => $sLogSldb,
                   sqlErrorHandler => sub { $sLogSldb->log($_[0],3) } });
  $dbOk=$sldb->connect({PrintError => 1});
  if ($dbOk) {
    p '> Connection to database successful'
  } else {
    print <<EOU;
---------------------------------------------------------
An error has occurred connecting to the database instance.
Make sure the instance is running, accessible and that the configuration at
${confFile} is correct

If all of the above are the case, follow the instructions below to create the
database:

> mysql --user=root --host=<hostname> --password=<rootpassword>

Run the following statements:

> create database if not exists $conf{dbName};
> grant all on $conf{dbName}.* to $conf{dbLogin} identified by '$conf{dbPwd}';
EOU
    exit;
  }
}

sub checkTb {
  p '> Checking tables existence';
  my $sth=$sldb->prepExec('select 0 from prefUsers where 0');
  if(defined $sth && $sth) {
    $tbOk=1;
    $sth->finish();
    $sth=undef;
  }else{
    $tbOk=0;
  }
}

p '';
p colored((('=' x 29).' START OF SLDB SETUP '.('=' x 29)),'bold cyan');

p 'This program will help you initialize SLDB database';

SimpleConf::readConf($confFile,\%conf);
checkDb();
checkTb();

if ($tbOk) {
  p '> Tables are populated';
} else {
  p '> Tables arent populated';
  initCreateTb();
}

sub initCreateTb {
  p "Creating SLDB tables...";
  $sldb->createTablesIfNeeded();
  p "Checking SLDB tables creation...";
  checkTb();
  if($tbOk) {
    p '['.colored('OK','green').']';
    p "Database initialized...";
    p "Insert records manually to the gamesNames table for the final step"
  }else{
    p '['.colored('FAILED','red').']';
  }
}

$sldb->disconnect() if($dbOk);
p '';
p colored((('=' x 30).' END OF SLDB DB INIT '.('=' x 30)),'bold cyan');
p '';
