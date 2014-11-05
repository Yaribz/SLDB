#!/usr/bin/perl -w
#
# This file implements the XML-RPC interface for SLDB, it is part of SLDB.
#
# The XML-RPC interface offers various services to handle SLDB user preferences
# and access players ranking data and statistics. It is used by the Spring
# replay site to display SLDB data to players in realtime.
#
# Copyright (C) 2013  Yann Riou <yaribzh@gmail.com>
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

# Version 0.1 (2013/11/27)

use strict;

use File::Basename qw/dirname fileparse/;
use File::Spec::Functions qw/catfile file_name_is_absolute rel2abs/;
use File::Path;
use RPC::XML;
use RPC::XML::Server;
use Time::HiRes;

my ($scriptBaseName,$scriptDir)=fileparse(rel2abs($0),'.pl');
unshift(@INC,$scriptDir);
require SimpleConf;
require SimpleLog;
require Sldb;

my $confFile=catfile($scriptDir,'etc',"$scriptBaseName.conf");
$confFile=$ARGV[0] if($#ARGV == 0);
if($#ARGV > 0 || ! -f $confFile) {
  print "usage: $0 [<confFile>]\n";
  exit 1;
}

my %conf=(logLevel => 4,
          sldbLogLevel => 4,
          logFile => catfile('var','log',"$scriptBaseName.log"),
          usersFile => catfile('etc',"$scriptBaseName.users.conf"),
          dbName => 'sldb',
          dbLogin => $scriptBaseName,
          dbPwd => undef,
          listenAddr => undef,
          listenPort => undef,
          preFork => { min_servers => 2,
                       max_servers => 10,
                       min_spare_servers => 1,
                       max_spare_servers => 8 }
    );
SimpleConf::readConf($confFile,\%conf);

my $usersFile=$conf{usersFile};
$usersFile=catfile($scriptDir,$usersFile) unless(file_name_is_absolute($usersFile));
my %users;
SimpleConf::readConf($usersFile,\%users);

my $logFile=$conf{logFile};
$logFile=catfile($scriptDir,$logFile) unless(file_name_is_absolute($logFile));
mkpath(dirname($logFile));

my $dbDs=$conf{dbName};
$dbDs="DBI:mysql:database=$dbDs;host=localhost" unless($dbDs =~ /^DBI:/i);

my $sLog;
my $sldb;
my $usePrefork;

sub createSlog {
  my $pid='';
  $pid="($$)" if($usePrefork);
  return SimpleLog->new(logFiles => [$logFile,''],
                        logLevels => [$conf{logLevel},5],
                        useANSICodes => [0,1],
                        useTimestamps => [1,1],
                        prefix => "[xmlRpc$pid] ");
}

sub createSldbSlog {
  my $pid='';
  $pid="($$)" if($usePrefork);
  return SimpleLog->new(logFiles => [$logFile,''],
                        logLevels => [$conf{sldbLogLevel},4],
                        useANSICodes => [0,1],
                        useTimestamps => [1,1],
                        prefix => "[SLDB$pid] ");
}

sub slog {
  $sLog->log(@_);
}

sub error {
  my $m=shift;
  slog($m,0);
  exit 1;
}

sub formatFloat {
  my ($n,$p)=@_;
  $n=sprintf("%.${p}f",$n) if($n=~/^-?\d+\.\d+$/);
  return $n;
}

eval "require Net::Server::PreFork";
if($@) {
  $usePrefork=0;
  $sLog=createSlog();
  slog("Net::Server::PreFork module not found, falling back to degraded mode (no parallel processing!)",2);
  $sldb=Sldb->new({dbDs => $dbDs,
                   dbLogin => $conf{dbLogin},
                   dbPwd => $conf{dbPwd},
                   sLog => createSldbSlog(),
                   sqlErrorHandler => \&error});
  slog("Connecting to database $dbDs as user $conf{dbLogin}",3);
  $sldb->connect();
}else{
  $usePrefork=1;
  $sLog=createSlog();
  my $preforkSettings=join(', ',map {"$_=$conf{preFork}->{$_}"} (keys %{$conf{preFork}}));
  slog("Using PreFork server ($preforkSettings)",3);
  $sLog=undef;
  {
    no warnings 'once';
    *RPC::XML::Server::child_init_hook = sub {
      $sLog=createSlog();
      slog("New server process created.",5);
      $sldb=Sldb->new({dbDs => $dbDs,
                       dbLogin => $conf{dbLogin},
                       dbPwd => $conf{dbPwd},
                       sLog => createSldbSlog(),
                       sqlErrorHandler => \&error});
      slog("Connecting to database $dbDs as user $conf{dbLogin}",5);
      $sldb->connect();
    };
    *RPC::XML::Server::child_finish_hook = sub {
      slog("Server process stopped.",5);
    }
  }
}

my $s=RPC::XML::Server->new(host => $conf{listenAddr}, port => $conf{listenPort}, no_http => $usePrefork);
$s->add_method( {name => 'getPref',
                 signature => ['struct string string int string'],
                 code => \&getPref} );
$s->add_method( {name => 'setPref',
                 signature => ['struct string string int string','struct string string int string string'],
                 code => \&setPref} );
$s->add_method( {name => 'getSkills',
                 signature => ['struct string string string array'],
                 code => \&getSkills} );
$s->add_method( {name => 'getMatchSkills',
                 signature => ['struct string string array'],
                 code => \&getMatchSkills} );
$s->add_method( {name => 'getLeaderboards',
                 signature => ['struct string string string array'],
                 code => \&getLeaderboards} );
$s->add_method( {name => 'getPlayerStats',
                 signature => ['struct string string string int'],
                 code => \&getPlayerStats} );

my $perfTimer;

sub perfBegin {
  $perfTimer=Time::HiRes::time;
}

sub perfEnd {
  $perfTimer=int((Time::HiRes::time-$perfTimer)*1000);
  my $logLevel=5;
  if($perfTimer > 4000) {
    $logLevel=2;
  }elsif($perfTimer > 2000) {
    $logLevel=3;
  }elsif($perfTimer > 1000) {
    $logLevel=4;
  }
  slog(( caller(1) )[3]." took ${perfTimer}ms to complete",$logLevel);
}

sub getPref {
  perfBegin();
  my ($p_server,$login,$password,$accountId,$pref)=@_;
  slog("getPref called with login=\"$login\", password=\"$password\", accountId=\"$accountId\", pref=\"$pref\" [$p_server->{peerhost}]",5);

  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("getPref called with invalid login/password \"$login/$password\" (accountId=\"$accountId\", pref=\"$pref\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  my $prefVal;
  if(! $sldb->getUserPref($accountId,$pref,\$prefVal)) {
    slog("getPref called with invalid preference \"$pref\" (login=\"$login\", accountId=\"$accountId\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }
  perfEnd();
  return { status => 0, result => RPC::XML::string->new($prefVal)};
}

sub setPref {
  perfBegin();
  my ($p_server,$login,$password,$accountId,$pref,$val)=@_;
  my $valString='_UNDEF_';
  $valString=$val if(defined $val);
  slog("setPref called with login=\"$login\", password=\"$password\", accountId=\"$accountId\", pref=\"$pref\", val=\"$valString\" [$p_server->{peerhost}]",5);

  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("setPref called with invalid login/password \"$login/$password\" (accountId=\"$accountId\", pref=\"$pref\", val=\"$valString\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  my $res=$sldb->setUserPref($accountId,$pref,$val);
  if($res == 0) {
    slog("setPref called with invalid preference \"$pref\" (login=\"$login\", accountId=\"$accountId\", val=\"$valString\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }elsif($res == -1) {
    slog("setPref called with invalid value \"$valString\" for preference \"$pref\" (login=\"$login\", accountId=\"$accountId\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }elsif($res == -2) {
    slog("setPref called with unknown accountId \"$accountId\" (login=\"$login\", pref=\"$pref\", val=\"$valString\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  perfEnd();
  return { status => 0 };
}

sub getSkills {
  perfBegin();
  my ($p_server,$login,$password,$modShortName,$p_accountIds)=@_;
  my $accountIds=join(',',@{$p_accountIds});
  slog("getSkills called with login=\"$login\", password=\"$password\", modShortName=\"$modShortName\", accoundIds=\"$accountIds\" [$p_server->{peerhost}]",5);

  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("getSkills called with invalid login/password \"$login/$password\" (modShortName=\"$modShortName\", accoundIds=\"$accountIds\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  my $fixedModShorName=$sldb->fixModShortName($modShortName);
  if(! defined $fixedModShorName) {
    slog("getSkills called with invalid modShortName \"$modShortName\" (login=\"$login\", accoundIds=\"$accountIds\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }
  $modShortName=$fixedModShorName;
  my $quotedModShortName=$sldb->quote($modShortName);

  if(! @{$p_accountIds}) {
    slog("getSkills called with empty accountId list (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  if($#{$p_accountIds} > 31) {
    slog("getSkills called with accountId list too big (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  my $currentRatingPeriod=$sldb->getCurrentRatingPeriod();
  my @results;
  foreach my $accountId (@{$p_accountIds}) {
    if($accountId !~ /^\d+$/) {
      slog("getSkills called with invalid accountId \"$accountId\" (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",2);
      push(@results,{ accountId => $accountId, status => 1 });
      next;
    }

    my $p_skills=$sldb->getSkills($currentRatingPeriod,$accountId,undef,$quotedModShortName);
    if(! %{$p_skills}) {
      slog("getSkills called with unrated accountId \"$accountId\" (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",2);
      push(@results,{ accountId => $accountId, status => 2 });
      next;
    }

    foreach my $k (keys %{$p_skills}) {
      $p_skills->{$k}->{mu}=formatFloat($p_skills->{$k}->{mu},2);
      $p_skills->{$k}->{sigma}=formatFloat($p_skills->{$k}->{sigma},2);
    }
    my @skills=("$p_skills->{Duel}->{mu}|$p_skills->{Duel}->{sigma}","$p_skills->{FFA}->{mu}|$p_skills->{FFA}->{sigma}","$p_skills->{Team}->{mu}|$p_skills->{Team}->{sigma}","$p_skills->{TeamFFA}->{mu}|$p_skills->{TeamFFA}->{sigma}","$p_skills->{Global}->{mu}|$p_skills->{Global}->{sigma}");

    my $userPrivacyMode;
    $sldb->getUserPref($accountId,'privacyMode',\$userPrivacyMode);

    push(@results,{ accountId => $accountId,  status => 0, privacyMode => $userPrivacyMode, skills => \@skills });
  }

  perfEnd();
  return { status => 0, results => \@results };
}

sub getMatchSkills {
  perfBegin();
  my ($p_server,$login,$password,$p_gameIds)=@_;
  my $gameIds=join(',',@{$p_gameIds});
  slog("getMatchSkills called with login=\"$login\", password=\"$password\", gameIds=\"$gameIds\" [$p_server->{peerhost}]",5);
  
  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("getMatchSkills called with invalid login/password \"$login/$password\" (gameIds=\"$gameIds\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  if(! @{$p_gameIds}) {
    slog("getMatchSkills called with empty gameId list (login=\"$login\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  if($#{$p_gameIds} > 31) {
    slog("getMatchSkills called with gameId list too big (login=\"$login\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  my %privacyModeCache;

  my @results;
  foreach my $gameId (@{$p_gameIds}) {

    if($gameId !~ /^[a-z0-9\-]*$/) {
      slog("getMatchSkills called with invalid gameId \"$gameId\" (login=\"$login\") [$p_server->{peerhost}]",2);
      push(@results,{ gameId => $gameId, status => 1 });
      next;
    }

    my $p_bSkills=$sldb->getBattleSkills($gameId);
    if(! %{$p_bSkills}) {
      slog("getMatchSkills called with unrated gameId \"$gameId\" (login=\"$login\") [$p_server->{peerhost}]",2);
      push(@results,{ gameId => $gameId, status => 2 });
      next;
    }

    my @players;
    foreach my $accountId (keys %{$p_bSkills->{globalSkills}}) {
      my $userPrivacyMode;
      if(exists $privacyModeCache{$accountId}) {
        $userPrivacyMode=$privacyModeCache{$accountId};
      }else{
        $sldb->getUserPref($accountId,'privacyMode',\$userPrivacyMode);
        $privacyModeCache{$accountId}=$userPrivacyMode;
      }
      my $p_gSkills=$p_bSkills->{globalSkills}->{$accountId};
      my $p_sSkills=$p_bSkills->{specificSkills}->{$accountId};
      foreach my $k (keys %{$p_gSkills}) {
        $p_gSkills->{$k}=formatFloat($p_gSkills->{$k},2);
        $p_sSkills->{$k}=formatFloat($p_sSkills->{$k},2);
      }

      push(@players,{accountId => $accountId, privacyMode => $userPrivacyMode, skills => ["$p_sSkills->{muBefore}|$p_sSkills->{sigmaBefore}","$p_sSkills->{muAfter}|$p_sSkills->{sigmaAfter}","$p_gSkills->{muBefore}|$p_gSkills->{sigmaBefore}","$p_gSkills->{muAfter}|$p_gSkills->{sigmaAfter}"]});
    }

    push(@results,{ gameId => $gameId, status => 0, gameType => $p_bSkills->{gameType}, players => \@players });
  }

  perfEnd();
  return { status => 0, results => \@results };
}

sub getLeaderboards {
  perfBegin();
  my ($p_server,$login,$password,$modShortName,$p_gameTypes)=@_;
  my $gameTypes=join(',',@{$p_gameTypes});
  slog("getLeaderboards called with login=\"$login\", password=\"$password\", modShortName=\"$modShortName\", gameTypes=\"$gameTypes\" [$p_server->{peerhost}]",5);

  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("getLeaderboards called with invalid login/password \"$login/$password\" (modShortName=\"$modShortName\", gameTypes=\"$gameTypes\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  my $fixedModShorName=$sldb->fixModShortName($modShortName);
  if(! defined $fixedModShorName) {
    slog("getLeaderboards called with invalid modShortName \"$modShortName\" (login=\"$login\", gameTypes=\"$gameTypes\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }
  $modShortName=$fixedModShorName;

  if(! @{$p_gameTypes}) {
    slog("getLeaderboards called with empty gameType list (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  if($#{$p_gameTypes} > 31) {
    slog("getLeaderboards called with gameType list too big (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 2 }
  }

  my $currentRatingPeriod=$sldb->getCurrentRatingPeriod();

  my @results;
  for my $gType (@{$p_gameTypes}) {
    my $gameType=$sldb->fixGameType($gType);
    if(! defined $gameType) {
      slog("getLeaderboards called with invalid gameType \"$gType\" (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",3);
      push(@results, { gameType => $gType, status => 1 });
      next;
    }
    my $p_lbData=$sldb->getLeaderboard($currentRatingPeriod,$modShortName,$gameType);
    if(! defined $p_lbData) {
      slog("Unexpected SLDB::getLeaderboards result with following params: ratingPeriod=\"$currentRatingPeriod\", modShortName=\"$modShortName\", gameType=\"$gameType\" [$p_server->{peerhost}]",2);
      push(@results, { gameType => $gameType, status => 2 });
      next;
    }
    foreach my $p_player (@{$p_lbData}) {
      foreach my $floatKey (qw/trustedSkill estimatedSkill uncertainty/) {
        $p_player->{$floatKey}=RPC::XML::string->new(formatFloat($p_player->{$floatKey},2));
      }
      $p_player->{accountId}=delete($p_player->{userId});
    }
    push(@results, { gameType => $gameType, status => 0, players => $p_lbData });
  }

  perfEnd();
  return { status => 0, results => \@results };
}

sub getPlayerStats {
  perfBegin();
  my ($p_server,$login,$password,$modShortName,$accountId)=@_;
  slog("getPlayerStats called with login=\"$login\", password=\"$password\", modShortName=\"$modShortName\", accoundId=\"$accountId\" [$p_server->{peerhost}]",5);

  if(! exists $users{$login} || $password ne $users{$login}) {
    slog("getPlayerStats called with invalid login/password \"$login/$password\" (modShortName=\"$modShortName\", accoundId=\"$accountId\") [$p_server->{peerhost}]",3);
    perfEnd();
    return { status => 1 }
  }

  my $fixedModShorName=$sldb->fixModShortName($modShortName);
  if(! defined $fixedModShorName) {
    slog("getPlayerStats called with invalid modShortName \"$modShortName\" (login=\"$login\", accoundId=\"$accountId\") [$p_server->{peerhost}]",2);
    perfEnd();
    return { status => 2 }
  }
  $modShortName=$fixedModShorName;

  if($accountId !~ /^\d+$/) {
    slog("getPlayerStats called with invalid accountId \"$accountId\" (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",2);
    perfEnd();
    return { status => 2 }
  }

  my $p_stats=$sldb->getPlayerStats($accountId,$modShortName);
  if(! %{$p_stats}) {
    slog("getPlayerStats called with an unknown accountId \"$accountId\" (login=\"$login\", modShortName=\"$modShortName\") [$p_server->{peerhost}]",2);
    perfEnd();
    return { status => 2 }
  }

  my %results;
  foreach my $gameType (keys %{$p_stats}) {
    $results{$gameType}=[$p_stats->{$gameType}->{lost},$p_stats->{$gameType}->{won},$p_stats->{$gameType}->{draw}];
  }
  perfEnd();
  return { status => 0, results => \%results }
}

my %serverParams=(server_type => 'PreFork', no_client_stdout => 1, log_level => 1 );
foreach my $k (keys %{$conf{preFork}}) {
  $serverParams{$k}=$conf{preFork}->{$k};
}
$s->server_loop(%serverParams);
