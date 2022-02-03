#!/usr/bin/perl -w
#
# This file implements the setup process for SLDB, it is part of SLDB.
#
# The setup process for SLDB consists in two main tasks:
# - database initialization
# - components configuration
#
# Copyright (C) 2013-2022  Yann Riou <yaribzh@gmail.com>
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

use File::Basename qw/dirname fileparse/;
use File::Spec::Functions qw/catdir catfile file_name_is_absolute rel2abs/;
use File::Path;
use FileHandle;
use Term::ANSIColor;
use Term::UI;
use Term::ReadLine;
eval 'use Win32::Console::ANSI' if($^O eq 'MSWin32');

my ($scriptBaseName,$scriptDir)=fileparse(rel2abs($0),'.pl');
unshift(@INC,$scriptDir);
require SimpleConf;
require SimpleLog;
require Sldb;

if(@ARGV && (@ARGV != 1 || $ARGV[0] ne '--init-db')) {
  print <<EOU;
Usage:
  $0
      Launch full SLDB setup script
  $0 --init-db
      Initialize SLDB database only
EOU
  exit; 
}
my $initDbOnly=@ARGV;

my $confFile=catfile($scriptDir,'etc',"$scriptBaseName.conf");
# zkMonitor is deprecated
#my @sldbComponents=qw/slMonitor zkMonitor ratingEngine xmlRpc/;
my @sldbComponents=qw/slMonitor ratingEngine xmlRpc/;
my %specificParams=(slMonitor => { lobbyPassword => 'Spring lobby password for SpringLobbyMonitor account',
                                   lobbyAdminIds => 'list of Spring lobby account IDs having admin access on slMonitor, comma separated' },
                    xmlRpc => { listenAddr => 'listening address for XmlRpc interface, example: "12.34.56.78"',
                                listenPort => 'listening port for XmlRpc interface, example: "8300"' });
my %compPrereqs=(zkMonitor => [qw/HTML::TreeBuilder WWW::Mechanize/],
                 ratingEngine => ['Inline::Python'],
                 xmlRpc => [qw/RPC::XML RPC::XML::Server Net::Server Net::Server::PreFork/]);
my %conf=(dbName => undef,
          dbLogin => undef,
          dbPwd => undef);
my $sldb;
my $dbOk=0;
my $tbOk=0;
my $compConfOk=0;

my $t = Term::ReadLine->new('SLDB Setup');
my $tOut = $t->OUT() || \*STDOUT;
my $tIn = $t->IN() || \*STDIN;
sub p { print $tOut "$_[0]\n" }

sub checkConf {
  return 1 if(defined $conf{dbName} && defined $conf{dbLogin} && defined $conf{dbPwd});
  return 0;
}

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
  $dbOk=$sldb->connect({PrintError => 0});
  $sldb=undef unless($dbOk);
}

sub checkTb {
  my $sth=$sldb->prepExec('select 0 from prefUsers where 0');
  if(defined $sth && $sth) {
    $tbOk=1;
    $sth->finish();
    $sth=undef;
  }else{
    $tbOk=0;
  }
}

sub readComponentConfigFile {
  my $comp=shift;
  my $compConfFile=catfile($scriptDir,'etc',"$comp.conf");
  return {} unless(-f $compConfFile);
  my %compConf;
  SimpleConf::readConf($compConfFile,\%compConf);
  return \%compConf;
}

sub checkComponentConfigFile {
  my $comp=shift;
  my $p_compConf=readComponentConfigFile($comp);
  return 0 unless(exists $p_compConf->{dbName} && exists $p_compConf->{dbLogin} && exists $p_compConf->{dbPwd});
  if(exists $specificParams{$comp}) {
    foreach my $specificParam (keys %{$specificParams{$comp}}) {
      return 0 unless(exists $p_compConf->{$specificParam});
    }
  }
  return 0 if($comp eq 'xmlRpc' && ! -f catfile($scriptDir,'etc',"$comp.users.conf"));
  return 1;
}

sub getUnconfiguredComponent {
  for my $sldbComponent (@sldbComponents) {
    return $sldbComponent unless(checkComponentConfigFile($sldbComponent));
  }
  return undef;
}

p '';
p colored((('=' x 29).' START OF SLDB SETUP '.('=' x 29)),'bold cyan');

if(! -f $confFile) {
  p '';
  p 'This program will help you initialize SLDB database'.($initDbOnly?'':' and configure SLDB components');
}else{
  SimpleConf::readConf($confFile,\%conf);
  checkDb() if(checkConf());
  checkTb() if($dbOk);
  if(! $initDbOnly) {
    my $unconfiguredComponent=getUnconfiguredComponent();
    $compConfOk=1 unless(defined $unconfiguredComponent);
  }
}

my ($initTask,$configTask,$quitTask)=('Initialize database','Configure components','Quit');
my @tasks=($initTask,$configTask,$quitTask);
my ($initConfDbStep,$initCreateDbStep,$initCreateTbStep,$quitStep)=('Configure database access','Create database and user','Create tables','End task');
my %steps=($initTask => [$initConfDbStep,$initCreateDbStep,$initCreateTbStep,$quitStep],
           $configTask => []);
for my $sldbComponent (@sldbComponents) {
  push(@{$steps{$configTask}},"Configure $sldbComponent");
}
push(@{$steps{$configTask}},$quitStep);

my %stepFunctions = ( $initConfDbStep => \&initConfDb,
                      $initCreateDbStep => \&initCreateDb,
                      $initCreateTbStep => \&initCreateTb );

sub initConfDb {
  my ($dbName,$dbLogin,$dbPwd);
  my $default='';
  $default=$conf{dbName} if(defined $conf{dbName});
  $dbName=$t->get_reply( print_me => 'Please enter the name of the database which will be used by SLDB:',
                         prompt => 'dbName?',
                         allow => qr/^\w+$/,
                         default => $default );
  $default='';
  $default=$conf{dbLogin} if(defined $conf{dbLogin});
  $dbLogin=$t->get_reply( print_me => 'Please enter the user name which will be used by SLDB to connect to the database:',
                          prompt => 'dbLogin?',
                          allow => qr/^\w+$/,
                          default => $default );
  $default='';
  $default=$conf{dbPwd} if(defined $conf{dbPwd});
  $dbPwd=$t->get_reply( print_me => 'Please enter the password which will be used by SLDB to connect to the database:',
                        prompt => 'dbPwd?',
                        allow => qr/^.+$/,
                        default => $default );
  p '';
  p "Following database configuration ready to be saved: dbName=$dbName, dbLogin=$dbLogin, dbPwd=$dbPwd";
  my $confirm=$t->ask_yn( prompt => 'Is this information correct?',
                          default => 'y' );
  if(! $confirm) {
    p '';
    p '--> '.colored('Database configuration aborted!','yellow');
    return;
  }
  %conf=(dbName => $dbName, dbLogin => $dbLogin, dbPwd => $dbPwd);
  mkpath(dirname($confFile));
  SimpleConf::writeConf($confFile,\%conf);
  p '';
  p '--> '.colored("Database configuration saved into $confFile",'green');
  checkDb();
  checkTb() if($dbOk);
}

sub initCreateDb {
  my ($dbName,$dbIsLocal,$dbHost);
  if($conf{dbName} =~ /^\w+$/) {
    ($dbName,$dbIsLocal)=($conf{dbName},1);
  }elsif($conf{dbName} =~ /^dbi:mysql:(.+)$/i) {
    my @dbDefs=split(/;/,$1);
    foreach my $dbDef (@dbDefs) {
      if($dbDef =~ /^(\w+)=(.+)$/) {
        my ($attr,$val)=(lc($1),$2);
        if($attr eq 'database') {
          $dbName=$val;
        }elsif($attr eq 'host') {
          $dbHost=lc($val);
        }
      }else{
        $dbName=$dbDef if(@dbDefs == 1);
      }
    }
    if(defined $dbName &&
       (! defined $dbHost || $dbHost eq 'localhost' || $dbHost eq '127.0.0.1')) {
      $dbIsLocal=1;
    }
  }
  if(! defined $dbName) {
    p colored('Unrecognized dbName setting format','yellow');
    $dbName='<database_name>';
  }
  my @sqlCommands=("create database if not exists $dbName;",
                   "grant all on $dbName.* to $conf{dbLogin} identified by '$conf{dbPwd}';");
  if($dbIsLocal) {
    my $sqlFile=catfile($scriptDir,"$scriptBaseName.sql");
    my $fh=new FileHandle($sqlFile,'w');
    if(! defined $fh) {
      p colored("Unable to open $sqlFile for writing, exiting!",'red');
      exit 1;
    }
    foreach my $sqlCmd (@sqlCommands) {
      print $fh $sqlCmd."\n";
    }
    $fh->close();
    p "SLDB database creation commands have been written into following file: $sqlFile";
    p "Please run following command in another console to execute the script as MySQL admin user (you will be asked to enter the MySQL admin password):";
    p "    mysql --user=root -p < $sqlFile";
    p '';
    p 'Once the script has been executed, press enter to continue...';
    <$tIn>;
    unlink($sqlFile);
  }else{
    p 'Please run following SLDB database creation commands on your database server '.(defined $dbHost ? "($dbHost) " : ''). 'as MySQL admin user:';
    foreach my $sqlCmd (@sqlCommands) {
      p "    $sqlCmd";
    }
    p '';
    p 'Once the commands have been executed, press enter to continue...';
    <$tIn>;
  }
  print $tOut "Testing connection to database $conf{dbName} as user $conf{dbLogin}...";
  checkDb();
  if($dbOk) {
    p '['.colored('OK','green').']';
  }else{
    p '['.colored('FAILED','red').']';
  }
}

sub initCreateTb {
  p "Creating SLDB tables...";
  $sldb->createTablesIfNeeded();
  print $tOut "Checking SLDB tables creation...";
  checkTb();
  if($tbOk) {
    p '['.colored('OK','green').']';
  }else{
    p '['.colored('FAILED','red').']';
  }
}

sub executeConfigureStep {
  my $step=shift;
  $step =~ /Configure (.*)$/;
  my $comp=$1;

  if(exists $compPrereqs{$comp}) {
    p "Checking Perl module dependencies for $comp:";
    my $missingDeps=0;
    foreach my $dep (@{$compPrereqs{$comp}}) {
      print $tOut $dep.('.' x (40 - length($dep)));
      eval "require $dep";
      if($@) {
        $missingDeps++;
        p '['.colored('NOT FOUND','red').']';
      }else{
        p '['.colored('FOUND','green').']';
      }
    }
    if($missingDeps) {
      p '--> '.colored("$missingDeps Perl module".($missingDeps == 1 ? ' is' : 's are')." missing, $comp will not work properly until missing dependenc".($missingDeps == 1 ? 'y is' : 'ies are').' resolved!','yellow');
    }
    p '';
  }

  my $p_compConf=readComponentConfigFile($comp);
  my %compConf;
  for my $param (keys %conf) {
    my $prompt=$param;
    $prompt.=" (previous value: $p_compConf->{$param}) " if(exists $p_compConf->{$param} && $p_compConf->{$param} ne $conf{$param});
    my $allowedRegex = $param =~ /Pwd$/ ? qr/^.+$/ : qr/^\w+$/;
    my $value=$t->get_reply( print_me => "Please enter the $param setting value for $comp:",
                             prompt => "$prompt?",
                             allow => $allowedRegex,
                             default => $conf{$param} );
    $compConf{$param}=$value;
  }
  if(exists $specificParams{$comp}) {
    foreach my $param (keys %{$specificParams{$comp}}) {
      my $default='';
      $default=$p_compConf->{$param} if(exists $p_compConf->{$param});
      my $value=$t->get_reply( print_me => "Please enter the $param setting value for $comp: ($specificParams{$comp}->{$param})",
                               prompt => "$param?",
                               allow => qr/^.+$/,
                               default => $default );
      $compConf{$param}=$value;
    }
  }
  my $compSettings=join(', ',map {"$_=$compConf{$_}"} (keys %compConf));
  p '';
  p "Following $comp configuration ready to be saved: $compSettings";
  my $confirm=$t->ask_yn( prompt => 'Is this information correct?',
                          default => 'y' );
  if(! $confirm) {
    p '';
    p '--> '.colored("$comp configuration aborted!",'yellow');
    return;
  }
  my $compConfFile=catfile($scriptDir,'etc',"$comp.conf");
  mkpath(dirname($compConfFile));
  SimpleConf::writeConf($compConfFile,\%compConf);
  p '';
  p '--> '.colored("$comp configuration saved into $compConfFile",'green');
  if($comp eq 'xmlRpc' && ! -f catfile($scriptDir,'etc',"$comp.users.conf")) {
    p '';
    my $xmlRpcUser=$t->get_reply( print_me => 'Please enter a name for the allowed XmlRpc user:',
                                  prompt => 'XmlRpc user name?',
                                  allow => qr/^\w+$/,
                                  default => 'replaySite' );
    my $xmlRpcPwd=$t->get_reply( print_me => "Please enter a passwrd for $xmlRpcUser XmlRpc user:",
                                 prompt => "XmlRpc password for $xmlRpcUser?",
                                 allow => qr/^.+$/,
                                 default => '' );
    p '';
    p "Following $comp users configuration ready to be saved: xmlRpcUser=$xmlRpcUser, xmlRpcPwd=$xmlRpcPwd";
    my $confirm=$t->ask_yn( prompt => 'Is this information correct?',
                            default => 'y' );
    if(! $confirm) {
      p '';
      p '--> '.colored("$comp users configuration aborted!",'yellow');
      return;
    }
    my %compUsersConf=($xmlRpcUser => $xmlRpcPwd);
    my $compUsersConfFile=catfile($scriptDir,'etc',"$comp.users.conf");
    mkpath(dirname($compUsersConfFile));
    SimpleConf::writeConf($compUsersConfFile,\%compUsersConf);
    p '';
    p '--> '.colored("$comp users configuration saved into $compConfFile",'green');
  }
}

sub executeStep {
  my $step=shift;
  if(exists $stepFunctions{$step}) {
    &{$stepFunctions{$step}}();
  }else{
    executeConfigureStep($step);
  }
}

while(1) {
  my ($task,$step,$defaultTask,$defaultStep,@allowedTasks,@allowedSteps);
  @allowedTasks=@tasks;
  $defaultTask=$quitTask;

  if($tbOk) {
    $defaultTask=$configTask unless($compConfOk);
  }else{
    @allowedTasks=($initTask,$quitTask);
    $defaultTask=$initTask;
  }
  if($initDbOnly) {
    $task=$initTask;
  }else{
    p '';
    $task=$t->get_reply( print_me => colored('Setup tasks:','bold blue'),
                         choices => \@allowedTasks,
                         default => $defaultTask,
                         prompt => 'Which task do you want to perform?' );
    last if($task eq $quitTask);
  }

  while(1) {
    @allowedSteps=@{$steps{$task}};
    my $previousDefaultStep=$defaultStep;
    $defaultStep=$quitStep;
    if($task eq $initTask) {
      if(checkConf()) {
        if($dbOk) {
          $defaultStep=$initCreateTbStep unless($tbOk);
        }else{
          @allowedSteps=($initConfDbStep,$initCreateDbStep,$quitStep);
          $defaultStep=$initCreateDbStep;
        }
      }else{
        @allowedSteps=($initConfDbStep,$quitStep);
        $defaultStep=$initConfDbStep;
      }
    }elsif($task eq $configTask) {
      my $unconfiguredComponent=getUnconfiguredComponent();
      if(defined $unconfiguredComponent) {
        $compConfOk=0;
        $defaultStep="Configure $unconfiguredComponent";
      }else{
        $compConfOk=1;
      }
    }
    if($initDbOnly) {
      last if(defined $previousDefaultStep && $previousDefaultStep eq $defaultStep);
      $step=$defaultStep;
    }else{
      p '';
      $step=$t->get_reply( print_me => colored("$task steps:",'bold blue'),
                           choices => \@allowedSteps,
                           default => $defaultStep,
                           prompt => 'Which step do you want to perform?' );
    }
    last if($step eq $quitStep);
    p '';
    executeStep($step);
  }
  last if($initDbOnly);
}

$sldb->disconnect() if($dbOk);
p '';
p colored((('=' x 30).' END OF SLDB SETUP '.('=' x 30)),'bold cyan');
p '';
