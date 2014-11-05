#!/usr/bin/perl -w
#
# This file implements the rating engine for SLDB, it is part of SLDB.
#
# The rating engine is in charge of computing all games results to produce
# players ranking data. It is based on TrueSkill(tm) ranking algorithm.
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
use Time::HiRes;

my ($scriptBaseName,$scriptDir)=fileparse(rel2abs($0),'.pl');
unshift(@INC,$scriptDir);
require SimpleConf;
require SimpleLog;
require Sldb;

use Inline Python => <<'END_PYTHON';
from trueskill import Rating, TrueSkill, rate, quality, rate_1vs1, quality_1vs1, setup 
END_PYTHON

my $confFile=catfile($scriptDir,'etc',"$scriptBaseName.conf");
$confFile=$ARGV[0] if($#ARGV == 0);
if($#ARGV > 0 || ! -f $confFile) {
  print "usage: $0 [<confFile>]\n";
  exit 1;
}

my %conf=(logLevel => 4,
          sldbLogLevel => 4,
          logFile => catfile('var','log',"$scriptBaseName.log"),
          dbName => 'sldb',
          dbLogin => $scriptBaseName,
          dbPwd => undef,
          trueskillMu => 25,
          trueskillSigma => 25/3,
          trueskillBeta => 25/6,
          trueskillTau => 25/300,
          trueskillDrawProb => 1129/61928,
          inactivityPenalty => {threshold => 8,
                                minMu => 30,
                                maxSigma => 25/6,
                                maxPenalties => 100,
                                muPenalty => 0.06,
                                sigmaPenalty => 0.02},
          rerateDelay => 1800,
          maxRunTime => 86400
    );
SimpleConf::readConf($confFile,\%conf);

my $logFile=$conf{logFile};
$logFile=catfile($scriptDir,$logFile) unless(file_name_is_absolute($logFile));
mkpath(dirname($logFile));

my $dbDs=$conf{dbName};
$dbDs="DBI:mysql:database=$dbDs;host=localhost" unless($dbDs =~ /^DBI:/i);

my %rankInitSkills=(0 => 20,
                    1 => 22,
                    2 => 23,
                    3 => 24,
                    4 => 25,
                    5 => 26,
                    6 => 28,
                    7 => 30);

my $sLog=SimpleLog->new(logFiles => [$logFile,''],
                        logLevels => [$conf{logLevel},3],
                        useANSICodes => [0,1],
                        useTimestamps => [1,1],
                        prefix => "[ratingEngine] ");

my $sLogSldb=SimpleLog->new(logFiles => [$logFile,''],
                            logLevels => [$conf{sldbLogLevel},3],
                            useANSICodes => [0,1],
                            useTimestamps => [1,1],
                            prefix => "[SLDB] ");

my $running=time + $conf{maxRunTime};
my $currentRatingYear;
my $currentRatingMonth;

my %gameRatingMapping=('' => 'global',
                       'Duel' => 'duel',
                       'Ffa' => 'ffa',
                       'Team' => 'team',
                       'TeamFfa' => 'teamFfa');

$SIG{TERM} = \&sigTermHandler;
$SIG{USR1} = \&sigUsr1Handler;

sub sigTermHandler {
  print "Received SIGTERM signal, exiting cleanly as soon as possible!\n";
  $running=0;
}

sub sigUsr1Handler {
  print "Received SIGUSR1 signal, restarting cleanly as soon as possible!\n";
  $running=1;
}

sub slog {
  $sLog->log(@_);
}

sub error {
  my $m=shift;
  slog($m,0);
  exit 1;
}

my $sldb=Sldb->new({dbDs => $dbDs,
                    dbLogin => $conf{dbLogin},
                    dbPwd => $conf{dbPwd},
                    sLog => $sLogSldb,
                    sqlErrorHandler => \&error});

slog("Connecting to database $dbDs as user $conf{dbLogin}",3);
$sldb->connect();

slog("Initializing trueskill rating environment",3);
setup($conf{trueskillMu},$conf{trueskillSigma},$conf{trueskillBeta},$conf{trueskillTau},$conf{trueskillDrawProb});

sub secToTime {
  my $sec=shift;
  my @units=qw/year day hour minute second/;
  my @amounts=(gmtime $sec)[5,7,2,1,0];
  $amounts[0]-=70;
  my @strings;
  for my $i (0..$#units) {
    if($amounts[$i] == 1) {
      push(@strings,"1 $units[$i]");
    }elsif($amounts[$i] > 1) {
      push(@strings,"$amounts[$i] $units[$i]s");
    }
  }
  @strings=("0 second") unless(@strings);
  return $strings[0] if($#strings == 0);
  my $endString=pop(@strings);
  my $startString=join(", ",@strings);
  return "$startString and $endString";
}

sub currentMonth {
  return (localtime())[4]+1;
}

sub currentYear {
  return (localtime())[5]+1900;
}

sub previousYearMonth {
  my ($year,$month)=@_;
  $month--;
  if($month==0) {
    $month=12;
    $year--;
  }
  $month=sprintf('%02d',$month);
  return ($year,$month);
}

sub nextYearMonth {
  my ($year,$month)=@_;
  $month++;
  if($month==13) {
    $month=1;
    $year++;
  }
  $month=sprintf('%02d',$month);
  return ($year,$month);
}

sub newRatings {
  my $rank=shift;
  if(defined $rank) {
    return {global => new Rating($rankInitSkills{$rank}),
            duel => new Rating($rankInitSkills{$rank}),
            ffa => new Rating($rankInitSkills{$rank}),
            team => new Rating($rankInitSkills{$rank}),
            teamFfa => new Rating($rankInitSkills{$rank})};
  }else{
    return {global => new Rating,
            duel => new Rating,
            ffa => new Rating,
            team => new Rating,
            teamFfa => new Rating};
  }
}

sub copyRatings {
  my $p_ratings=shift;
  my %clonedRatings;
  foreach my $ratingType (keys %{$p_ratings}) {
    $clonedRatings{$ratingType}=new Rating($p_ratings->{$ratingType}->{mu}+0,$p_ratings->{$ratingType}->{sigma}+0);
  }
  return \%clonedRatings;
}

sub rateNewGame {
  my $gameId=shift;

  my $preRatingTime=Time::HiRes::time;

  my $quotedGameId=$sldb->quote($gameId);
  $sldb->do("update tsRatingQueue set status=1 where gameId=$quotedGameId","update tsRatingQueue table for rating of game \"$gameId\"");

  my $sth=$sldb->prepExec("select count(*) from tsGames where gameId=$quotedGameId","check for duplicate rating of game \"$gameId\"");
  my @countResult=$sth->fetchrow_array();
  error("Unable to query table tsGames for duplicate rating of game \"$gameId\"") unless(@countResult);
  if($countResult[0] > 0) {
    slog("Ignoring rating request of game \"$gameId\" (duplicate)",2);
    $sldb->do("update tsRatingQueue set status=2 where gameId=$quotedGameId","update tsRatingQueue table for duplicate rating of game \"$gameId\"");
    return;
  }

  $sth=$sldb->prepExec("select gd.gdrTimestamp,gd.type,gn.shortName,gd.undecided from games g,gamesDetails gd, gamesNames gn where g.gameId=$quotedGameId and gd.gameId=$quotedGameId and g.modName regexp gn.regex","retrieve information for rating of game \"$gameId\"");
  my @gameData=$sth->fetchrow_array();
  if(! @gameData) {
    slog("Ignoring rating request of game \"$gameId\" (unknown game)",2);
    $sldb->do("update tsRatingQueue set status=3 where gameId=$quotedGameId","update tsRatingQueue table for rating error of game \"$gameId\"");
    return;
  }
  my ($gdrTimestamp,$gameType,$modShortName,$undecided)=@gameData;
  if($undecided) {
    slog("Ignoring rating request of game \"$gameId\" (undecided game)",2);
    $sldb->do("update tsRatingQueue set status=6 where gameId=$quotedGameId","update tsRatingQueue table for rating error of game \"$gameId\"");
    return;
  }

  my ($gdrYear,$gdrMonth);
  if($gdrTimestamp =~ /^(\d{4})-(\d{2})-/) {
    ($gdrYear,$gdrMonth)=($1,$2);
  }else{
    slog("Unable to parse gdrTimestamp of game \"$gameId\" for rating",1);
    $sldb->do("update tsRatingQueue set status=4 where gameId=$quotedGameId","update tsRatingQueue table for rating error of game \"$gameId\"");
    return;
  }
  if($gdrYear != $currentRatingYear || $gdrMonth != $currentRatingMonth) {
    my ($nextRatingYear,$nextRatingMonth)=nextYearMonth($currentRatingYear,$currentRatingMonth);
    if($gdrYear != $nextRatingYear || $gdrMonth != $nextRatingMonth) {
      slog("Ignoring rating request of game \"$gameId\" (inconsistent GDR timestamp $gdrYear-$gdrMonth)",2);
      $sldb->do("update tsRatingQueue set status=5 where gameId=$quotedGameId","update tsRatingQueue table for rating error of game \"$gameId\"");
      return;
    }
    my $p_allMods=$sldb->getModsShortNames();
    foreach my $modShortName (@{$p_allMods}) {
      applyMonthPenalties($currentRatingYear,$currentRatingMonth,$modShortName);
    }
    slog("Initializing rating tables for new month ($gdrYear-$gdrMonth)",3);
    foreach my $gType (keys %gameRatingMapping) {
      $sldb->do("insert into ts${gType}Players (select $gdrYear$gdrMonth,userId,modShortName,skill,mu,sigma,nbPenalties from ts${gType}Players where period=$currentRatingYear$currentRatingMonth)","initialize players rating of $gdrYear-$gdrMonth from $currentRatingYear-$currentRatingMonth");
    }
    $currentRatingYear=$gdrYear;
    $currentRatingMonth=$gdrMonth;
    $sldb->do("update tsRatingState set value=$currentRatingYear where param='currentRatingYear'","update currentRatingYear parameter in tsRatingState table");
    $sldb->do("update tsRatingState set value=$currentRatingMonth where param='currentRatingMonth'","update currentRatingMonth parameter in tsRatingState table");
  }

  my %ratings;
  my %previouslyRatedUsers;
  my $quotedModShortName=$sldb->quote($modShortName);
  my $currentRatingPeriod=$currentRatingYear.$currentRatingMonth;

  foreach my $gType (keys %gameRatingMapping) {
    my $ratingType=$gameRatingMapping{$gType};
    my $sth=$sldb->prepExec("select tsp.userId,tsp.mu,tsp.sigma from playersDetails pd,userAccounts ua,ts${gType}Players tsp where tsp.period=$currentRatingPeriod and pd.gameId=$quotedGameId and pd.team is not null and pd.accountId=ua.accountId and ua.userId=tsp.userId and tsp.modShortName=$quotedModShortName","select ratings of players in game \"$gameId\" for rating type $ratingType and period $currentRatingPeriod from table ts${gType}Players");
    my @ratingData;
    while(@ratingData=$sth->fetchrow_array()) {
      $ratingData[1]+=0;
      $ratingData[2]+=0;
      $previouslyRatedUsers{$ratingData[0]}=1;
      if(exists $ratings{$ratingData[0]}) {
        $ratings{$ratingData[0]}->{$ratingType}=new Rating($ratingData[1],$ratingData[2]);
      }else{
        $ratings{$ratingData[0]}={$ratingType => new Rating($ratingData[1],$ratingData[2])};
      }
    }
  }

  $preRatingTime=int((Time::HiRes::time-$preRatingTime)*1000);

  my $calculationTime=Time::HiRes::time;
  rateGameBatch(\%ratings,$gameId,$gdrTimestamp,$gameType,$modShortName);
  $calculationTime=int((Time::HiRes::time-$calculationTime)*1000);

  my $postRatingTime=Time::HiRes::time;

  foreach my $gType (keys %gameRatingMapping) {
    my $ratingType=$gameRatingMapping{$gType};
    foreach my $userId (keys %ratings) {
      my ($newMu,$newSigma)=($ratings{$userId}->{$ratingType}->{mu},$ratings{$userId}->{$ratingType}->{sigma});
      my $newSkill=$newMu-3*$newSigma;
      if(exists $previouslyRatedUsers{$userId}) {
        $sldb->do("update ts${gType}Players set skill=$newSkill, mu=$newMu, sigma=$newSigma where period=$currentRatingPeriod and userId=$userId and modShortName=$quotedModShortName","update rating for user $userId, period $currentRatingPeriod and type $ratingType in ts${gType}Players table");
      }else{
        $sldb->do("insert into ts${gType}Players values ($currentRatingPeriod,$userId,$quotedModShortName,$newSkill,$newMu,$newSigma,0)","insert rating for new user $userId, period $currentRatingPeriod and type $ratingType in ts${gType}Players table");
      }
    }
  }

  $sldb->do("delete from tsRatingQueue where gameId=$quotedGameId","remove pending rating of game \"$gameId\" from tsRatingQueue table");

  $postRatingTime=int((Time::HiRes::time-$postRatingTime)*1000);
  slog("Real-time rating performances for game $gameId ($gameType,$modShortName): ${preRatingTime}ms/${calculationTime}ms/${postRatingTime}ms",4);
}

sub rateGameBatch {
  my ($p_ratings,$gameId,$gdrTimestamp,$gameType,$modShortName)=@_;
  my ($quotedGameId,$quotedModShortName)=$sldb->quote($gameId,$modShortName);
  
  my %userAccounts;
  my %preGameRatings;
  my %postGameRatings;
  my @playersData;
  my $sth;

  if($gameType eq 'Duel') {

    $sth=$sldb->prepExec("select pd.accountId,userId,win from playersDetails pd,userAccounts ua where pd.gameId=$quotedGameId and pd.accountId=ua.accountId and allyTeam is not null","extract player information from tables playersDetails and userAccounts for Duel game $gameId");
    my ($winningUser,$losingUser);
    my $tie=0;
    while(@playersData=$sth->fetchrow_array()) {
      my $userId=$playersData[1];
      if(exists $userAccounts{$userId}) {
        slog("Unable to rate duel game $gameId, same user $userId appears multiple times!",1);
        return 0;
      }
      $userAccounts{$userId}=$playersData[0];
      if(exists $p_ratings->{$userId}) {
        $preGameRatings{$userId}=$p_ratings->{$userId};
      }else{
        my $sth2=$sldb->prepExec("select rank from accounts where id=$userId");
        my @rankData=$sth2->fetchrow_array();
        if(@rankData) {
          $preGameRatings{$userId}=newRatings($rankData[0]);
        }else{
          slog("Rating a game with an unknown user \"$userId\" ($gameId)",2);
          $preGameRatings{$userId}=newRatings();
        }
      }
      $postGameRatings{$userId}=copyRatings($preGameRatings{$userId});
      if($playersData[2]==1) {
        $winningUser=$userId;
      }else{
        if(defined $losingUser) {
          $winningUser=$userId;
          $tie=1;
        }else{
          $losingUser=$userId;
        }
      }
    }
    if(! defined $losingUser) {
      slog("Unable to rate duel game $gameId, no loser identified and no tie indicator!",1);
      return 0;
    }

    foreach my $ratingType ('global','duel') {
      my ($winnerRating,$loserRating)=rate_1vs1($preGameRatings{$winningUser}->{$ratingType},$preGameRatings{$losingUser}->{$ratingType},$tie);
      error("Error during duel rating process! ($ratingType rating)") unless(defined $winnerRating && defined $loserRating);
      $postGameRatings{$winningUser}->{$ratingType}=$winnerRating;
      $postGameRatings{$losingUser}->{$ratingType}=$loserRating;
    }

    foreach my $userId (keys %postGameRatings) {
      $sldb->do("insert into tsDuelGames values ($quotedGameId,$userAccounts{$userId},$userId,$quotedModShortName,'$gdrTimestamp',$preGameRatings{$userId}->{duel}->{mu},$preGameRatings{$userId}->{duel}->{sigma},$postGameRatings{$userId}->{duel}->{mu},$postGameRatings{$userId}->{duel}->{sigma})","insert new rating for user $userId in tsDuelGames table");
    }

  }elsif($gameType eq 'FFA') {

    $sth=$sldb->prepExec("select pd.accountId,userId,win from playersDetails pd,userAccounts ua where pd.gameId=$quotedGameId and pd.accountId=ua.accountId and allyTeam is not null","extract player information from tables playersDetails and userAccounts for FFA game $gameId");
    my $winningUser;
    my @losingUsers;
    my %losingRatings=(global => [],
                       ffa => []);
    while(@playersData=$sth->fetchrow_array()) {
      my $userId=$playersData[1];
      if(exists $userAccounts{$userId}) {
        slog("Unable to rate FFA game $gameId, same user $userId appears multiple times!",1);
        return 0;
      }
      $userAccounts{$userId}=$playersData[0];
      if(exists $p_ratings->{$userId}) {
        $preGameRatings{$userId}=$p_ratings->{$userId};
      }else{
        my $sth2=$sldb->prepExec("select rank from accounts where id=$userId");
        my @rankData=$sth2->fetchrow_array();
        if(@rankData) {
          $preGameRatings{$userId}=newRatings($rankData[0]);
        }else{
          slog("Rating a game with an unknown user \"$userId\" ($gameId)",2);
          $preGameRatings{$userId}=newRatings();
        }
      }
      $postGameRatings{$userId}=copyRatings($preGameRatings{$userId});
      if($playersData[2]==1) {
        if(defined $winningUser) {
          slog("Unable to rate FFA game $gameId, multiple winning users!",2);
          return 0;
        }
        $winningUser=$userId;
      }else{
        push(@losingUsers,$userId);
        push(@{$losingRatings{global}},[$preGameRatings{$userId}->{global}]);
        push(@{$losingRatings{ffa}},[$preGameRatings{$userId}->{ffa}]);
      }
    }
    if($#losingUsers < 1) {
      slog("Unable to rate FFA game $gameId, it does not seem to be a FFA!",1);
      return 0;
    }
    if(! defined $winningUser) {
      slog("Skipping rating of FFA game $gameId, no winner identified and tie FFA games can't be rated",4);
      return 0;
    }

    foreach my $ratingType ('global','ffa') {
      my $p_ffaRateResult=rate([[$preGameRatings{$winningUser}->{$ratingType}],@{$losingRatings{$ratingType}}],[1,(2) x ($#losingUsers+1)]);
      error("Error during FFA rating process! ($ratingType rating)") unless(@{$p_ffaRateResult});
      error("Inconsistent FFA rating result! ($ratingType rating, bad number of teams)") if($#{$p_ffaRateResult} != $#losingUsers+1);
      $postGameRatings{$winningUser}->{$ratingType}=$p_ffaRateResult->[0]->[0];

      my @fakeDuelRatings;
      my $fakeDuelWinnerMuIncrease=0;
      for my $losingIndex (0..$#losingUsers) {
        my $losingUser=$losingUsers[$losingIndex];
        my ($winnerRating,$loserRating)=rate_1vs1($preGameRatings{$winningUser}->{$ratingType},$preGameRatings{$losingUser}->{$ratingType});
        error("Error during FFA rating process! (fake duel rating for $ratingType rating)") unless(defined $winnerRating && defined $loserRating);
        $fakeDuelWinnerMuIncrease+=$winnerRating->{mu}-$preGameRatings{$winningUser}->{$ratingType}->{mu};
        push(@fakeDuelRatings,$loserRating);
      }
      
      my $realWinnerMuIncrease=$postGameRatings{$winningUser}->{$ratingType}->{mu}-$preGameRatings{$winningUser}->{$ratingType}->{mu};
      my $realFfaRatingRatio=$realWinnerMuIncrease/$fakeDuelWinnerMuIncrease;
      for my $losingIndex (0..$#losingUsers) {
        my $losingUser=$losingUsers[$losingIndex];
        my $fakeDuelMuDiff=$fakeDuelRatings[$losingIndex]->{mu}-$preGameRatings{$losingUser}->{$ratingType}->{mu};
        my $fakeDuelSigmaDiff=$fakeDuelRatings[$losingIndex]->{sigma}-$preGameRatings{$losingUser}->{$ratingType}->{sigma};
        $postGameRatings{$losingUsers[$losingIndex]}->{$ratingType}=new Rating($preGameRatings{$losingUser}->{$ratingType}->{mu}+($fakeDuelMuDiff*$realFfaRatingRatio),$preGameRatings{$losingUser}->{$ratingType}->{sigma}+($fakeDuelSigmaDiff*$realFfaRatingRatio));
      }
    }

    foreach my $userId (keys %postGameRatings) {
      $sldb->do("insert into tsFfaGames values ($quotedGameId,$userAccounts{$userId},$userId,$quotedModShortName,'$gdrTimestamp',$preGameRatings{$userId}->{ffa}->{mu},$preGameRatings{$userId}->{ffa}->{sigma},$postGameRatings{$userId}->{ffa}->{mu},$postGameRatings{$userId}->{ffa}->{sigma})","insert new rating for user $userId in tsFfaGames table");
    }

  }elsif($gameType eq 'Team') {

    $sth=$sldb->prepExec("select pd.accountId,userId,allyTeam,win from playersDetails pd,userAccounts ua where pd.gameId=$quotedGameId and pd.accountId=ua.accountId and allyTeam is not null","extract player information from tables playersDetails and userAccounts for Team game $gameId");
    my ($winningTeam,$losingTeam,%tieTeams);
    my %teamsUsers;
    my %teamsRatings=(global => {},
                      team => {});
    while(@playersData=$sth->fetchrow_array()) {
      my $userId=$playersData[1];
      if(exists $userAccounts{$userId}) {
        slog("Unable to rate team game $gameId, same user $userId appears multiple times!",1);
        return 0;
      }
      $userAccounts{$userId}=$playersData[0];
      if(exists $p_ratings->{$userId}) {
        $preGameRatings{$userId}=$p_ratings->{$userId};
      }else{
        my $sth2=$sldb->prepExec("select rank from accounts where id=$userId");
        my @rankData=$sth2->fetchrow_array();
        if(@rankData) {
          $preGameRatings{$userId}=newRatings($rankData[0]);
        }else{
          slog("Rating a game with an unknown user \"$userId\" ($gameId)",2);
          $preGameRatings{$userId}=newRatings();
        }
      }
      $postGameRatings{$userId}=copyRatings($preGameRatings{$userId});
      my $teamNb=$playersData[2];
      if(exists $teamsUsers{$teamNb}) {
          push(@{$teamsUsers{$teamNb}},$userId);
          push(@{$teamsRatings{global}->{$teamNb}},$preGameRatings{$userId}->{global});
          push(@{$teamsRatings{team}->{$teamNb}},$preGameRatings{$userId}->{team});
      }else{
          $teamsUsers{$teamNb}=[$userId];
          $teamsRatings{global}->{$teamNb}=[$preGameRatings{$userId}->{global}];
          $teamsRatings{team}->{$teamNb}=[$preGameRatings{$userId}->{team}];
      }
      if($playersData[3]==0) {
        if(defined $losingTeam && $losingTeam != $teamNb) {
          slog("Unable to rate team game $gameId, multiple losing teams found!",1);
          return 0;
        }
        if(%tieTeams) {
          slog("Unable to rate team game $gameId, tie mixed with lost!",1);
          return 0;
        }
        $losingTeam=$teamNb;
      }elsif($playersData[3]==1) {
        if(defined $winningTeam && $winningTeam != $teamNb) {
          slog("Unable to rate team game $gameId, multiple winning teams found!",1);
          return 0;
        }
        if(%tieTeams) {
          slog("Unable to rate team game $gameId, tie mixed with win!",1);
          return 0;
        }
        $winningTeam=$teamNb;
      }else{
        if(defined $losingTeam || defined $winningTeam) {
          slog("Unable to rate team game $gameId, tie mixed with win/lost!",1);
          return 0;
        }
        $tieTeams{$teamNb}=1;
      }
    }
    if(keys %teamsUsers != 2) {
      slog("Unable to rate team game $gameId, it does not seem to be a team game!",1);
      return 0;
    }
    my $tie=0;
    if(%tieTeams) {
      $tie=1;
      ($winningTeam,$losingTeam)=(keys %tieTeams);
    }

    my $maxTeamSize=$#{$teamsRatings{global}->{$winningTeam}}+1;
    $maxTeamSize=$#{$teamsRatings{global}->{$losingTeam}}+1 if($#{$teamsRatings{global}->{$losingTeam}}+1 > $maxTeamSize);
    if(abs($#{$teamsRatings{global}->{$winningTeam}} - $#{$teamsRatings{global}->{$losingTeam}}) / $maxTeamSize > 1/3) {
      slog("Skipping rating of team game $gameId, teams are too uneven (".($#{$teamsRatings{global}->{$winningTeam}}+1).'v'.($#{$teamsRatings{global}->{$losingTeam}}+1).')',4);
      return 0;
    }

    foreach my $ratingType ('global','team') {
      my $p_teamRateResult=rate([$teamsRatings{$ratingType}->{$winningTeam},$teamsRatings{$ratingType}->{$losingTeam}],[$tie,1]);
      error("Error during team rating process! ($ratingType rating)") unless(@{$p_teamRateResult});
      error("Inconsistent team rating result! ($ratingType rating, bad number of teams)") if($#{$p_teamRateResult} != 1);
      error("Inconsistent team rating result! ($ratingType rating, bad team size)") if($#{$p_teamRateResult->[0]} != $#{$teamsUsers{$winningTeam}} || $#{$p_teamRateResult->[1]} != $#{$teamsUsers{$losingTeam}});
      for my $winnerIndex (0..$#{$p_teamRateResult->[0]}) {
        my $userId=$teamsUsers{$winningTeam}->[$winnerIndex];
        $postGameRatings{$userId}->{$ratingType}=$p_teamRateResult->[0]->[$winnerIndex];
        if($postGameRatings{$userId}->{$ratingType}->{sigma} > $preGameRatings{$userId}->{$ratingType}->{sigma}) {
          $postGameRatings{$userId}->{$ratingType}=new Rating($postGameRatings{$userId}->{$ratingType}->{mu}+0,$preGameRatings{$userId}->{$ratingType}->{sigma}+0);
        }
      }
      for my $loserIndex (0..$#{$p_teamRateResult->[1]}) {
        my $userId=$teamsUsers{$losingTeam}->[$loserIndex];
        $postGameRatings{$userId}->{$ratingType}=$p_teamRateResult->[1]->[$loserIndex];
        if($postGameRatings{$userId}->{$ratingType}->{sigma} > $preGameRatings{$userId}->{$ratingType}->{sigma}) {
          $postGameRatings{$userId}->{$ratingType}=new Rating($postGameRatings{$userId}->{$ratingType}->{mu}+0,$preGameRatings{$userId}->{$ratingType}->{sigma}+0);
        }
      }
    }

    foreach my $userId (keys %postGameRatings) {
      $sldb->do("insert into tsTeamGames values ($quotedGameId,$userAccounts{$userId},$userId,$quotedModShortName,'$gdrTimestamp',$preGameRatings{$userId}->{team}->{mu},$preGameRatings{$userId}->{team}->{sigma},$postGameRatings{$userId}->{team}->{mu},$postGameRatings{$userId}->{team}->{sigma})","insert new rating for user $userId in tsTeamGames table");
    }
  }elsif($gameType eq 'TeamFFA') {

    $sth=$sldb->prepExec("select pd.accountId,userId,allyTeam,win from playersDetails pd,userAccounts ua where pd.gameId=$quotedGameId and pd.accountId=ua.accountId and allyTeam is not null","extract player information from tables playersDetails and userAccounts for TeamFFA game $gameId");
    my ($winningTeam,%losingTeams);
    my %teamsUsers;
    my %teamsRatings=(global => {},
                      teamFfa => {});
    while(@playersData=$sth->fetchrow_array()) {
      my $userId=$playersData[1];
      if(exists $userAccounts{$userId}) {
        slog("Unable to rate teamFFA game $gameId, same user $userId appears multiple times!",1);
        return 0;
      }
      $userAccounts{$userId}=$playersData[0];
      if(exists $p_ratings->{$userId}) {
        $preGameRatings{$userId}=$p_ratings->{$userId};
      }else{
        my $sth2=$sldb->prepExec("select rank from accounts where id=$userId");
        my @rankData=$sth2->fetchrow_array();
        if(@rankData) {
          $preGameRatings{$userId}=newRatings($rankData[0]);
        }else{
          slog("Rating a game with an unknown user \"$userId\" ($gameId)",2);
          $preGameRatings{$userId}=newRatings();
        }
      }
      $postGameRatings{$userId}=copyRatings($preGameRatings{$userId});
      my $teamNb=$playersData[2];
      if(exists $teamsUsers{$teamNb}) {
          push(@{$teamsUsers{$teamNb}},$userId);
          push(@{$teamsRatings{global}->{$teamNb}},$preGameRatings{$userId}->{global});
          push(@{$teamsRatings{teamFfa}->{$teamNb}},$preGameRatings{$userId}->{teamFfa});
      }else{
          $teamsUsers{$teamNb}=[$userId];
          $teamsRatings{global}->{$teamNb}=[$preGameRatings{$userId}->{global}];
          $teamsRatings{teamFfa}->{$teamNb}=[$preGameRatings{$userId}->{teamFfa}];
      }
      if($playersData[3]==1) {
        if(defined $winningTeam && $winningTeam != $teamNb) {
          slog("Unable to rate TeamFFA game $gameId, multiple winning teams!",2);
          return 0;
        }
        $winningTeam=$teamNb;
      }else{
        $losingTeams{$teamNb}=1;
      }
    }
    if(keys %losingTeams < 2) {
      slog("Unable to rate TeamFFA game $gameId, it does not seem to be a TeamFFA!",1);
      return 0;
    }
    if(! defined $winningTeam) {
      slog("Skipping rating of TeamFFA game $gameId, no winner team identified and tie TeamFFA games can't be rated",4);
      return 0;
    }
    my @losingTeamNumbers=sort {$a <=> $b} (keys %losingTeams);

    foreach my $ratingType ('global','teamFfa') {
      my @orderedLoosingTeamsRatings;
      foreach my $teamNumber (@losingTeamNumbers) {
        push(@orderedLoosingTeamsRatings,$teamsRatings{$ratingType}->{$teamNumber});
      }
      my $p_teamFfaRateResult=rate([$teamsRatings{$ratingType}->{$winningTeam},@orderedLoosingTeamsRatings],[1,(2) x ($#losingTeamNumbers+1)]);
      error("Error during TeamFFA rating process! ($ratingType rating)") unless(@{$p_teamFfaRateResult});
      error("Inconsistent TeamFFA rating result! ($ratingType rating, bad number of teams)") if($#{$p_teamFfaRateResult} != $#losingTeamNumbers+1);
      my $realWinningTeamMuIncrease=0;
      for my $winnerIndex (0..$#{$teamsUsers{$winningTeam}}) {
        my $winningUser=$teamsUsers{$winningTeam}->[$winnerIndex];
        $postGameRatings{$winningUser}->{$ratingType}=$p_teamFfaRateResult->[0]->[$winnerIndex];
        if($postGameRatings{$winningUser}->{$ratingType}->{sigma} > $preGameRatings{$winningUser}->{$ratingType}->{sigma}) {
          $postGameRatings{$winningUser}->{$ratingType}=new Rating($postGameRatings{$winningUser}->{$ratingType}->{mu}+0,$preGameRatings{$winningUser}->{$ratingType}->{sigma}+0);
        }
        $realWinningTeamMuIncrease+=$postGameRatings{$winningUser}->{$ratingType}->{mu}-$preGameRatings{$winningUser}->{$ratingType}->{mu};
      }

      my %fakeTeamLosingTeamsRatings;
      my $fakeTeamWinningTeamMuIncrease=0;
      for my $losingIndex (0..$#losingTeamNumbers) {
        my $losingTeam=$losingTeamNumbers[$losingIndex];
        my $p_fakeTeamRateResult=rate([$teamsRatings{$ratingType}->{$winningTeam},$teamsRatings{$ratingType}->{$losingTeam}],[1,2]);
        error("Error during TeamFFA rating process! (fake team rating for $ratingType rating)") unless(@{$p_fakeTeamRateResult});
        error("Inconsistent TeamFFA rating result! (fake team rating for $ratingType rating, bad number of teams)") if($#{$p_fakeTeamRateResult} != 1);
        error("Inconsistent TeamFFA rating result! (fake team rating for $ratingType rating, bad number of teams)") if($#{$p_fakeTeamRateResult->[0]} != $#{$teamsUsers{$winningTeam}});
        error("Inconsistent TeamFFA rating result! (fake team rating for $ratingType rating, bad number of teams)") if($#{$p_fakeTeamRateResult->[1]} != $#{$teamsUsers{$losingTeam}});
        for my $winnerIndex (0..$#{$teamsUsers{$winningTeam}}) {
          my $winningUser=$teamsUsers{$winningTeam}->[$winnerIndex];
          $fakeTeamWinningTeamMuIncrease+=$p_fakeTeamRateResult->[0]->[$winnerIndex]->{mu}-$preGameRatings{$winningUser}->{$ratingType}->{mu};
        }
        $fakeTeamLosingTeamsRatings{$losingTeam}=[];
        for my $loserIndex (0..$#{$teamsUsers{$losingTeam}}) {
          push(@{$fakeTeamLosingTeamsRatings{$losingTeam}},$p_fakeTeamRateResult->[1]->[$loserIndex]);
        }
      }
      
      my $realTeamFfaRatingRatio=$realWinningTeamMuIncrease/$fakeTeamWinningTeamMuIncrease;
      for my $losingIndex (0..$#losingTeamNumbers) {
        my $losingTeam=$losingTeamNumbers[$losingIndex];
        for my $loserIndex (0..$#{$fakeTeamLosingTeamsRatings{$losingTeam}}) {
          my $losingUser=$teamsUsers{$losingTeam}->[$loserIndex];
          my $fakeTeamMuDiff=$fakeTeamLosingTeamsRatings{$losingTeam}->[$loserIndex]->{mu}-$preGameRatings{$losingUser}->{$ratingType}->{mu};
          my $fakeTeamSigmaDiff=$fakeTeamLosingTeamsRatings{$losingTeam}->[$loserIndex]->{sigma}-$preGameRatings{$losingUser}->{$ratingType}->{sigma};
          $fakeTeamSigmaDiff=0 if($fakeTeamSigmaDiff > 0);
          $postGameRatings{$losingUser}->{$ratingType}=new Rating($preGameRatings{$losingUser}->{$ratingType}->{mu}+($fakeTeamMuDiff*$realTeamFfaRatingRatio),$preGameRatings{$losingUser}->{$ratingType}->{sigma}+($fakeTeamSigmaDiff*$realTeamFfaRatingRatio));
        }
      }
    }

    foreach my $userId (keys %postGameRatings) {
      $sldb->do("insert into tsTeamFfaGames values ($quotedGameId,$userAccounts{$userId},$userId,$quotedModShortName,'$gdrTimestamp',$preGameRatings{$userId}->{teamFfa}->{mu},$preGameRatings{$userId}->{teamFfa}->{sigma},$postGameRatings{$userId}->{teamFfa}->{mu},$postGameRatings{$userId}->{teamFfa}->{sigma})","insert new rating for user $userId in tsTeamFfaGames table");
    }
    
  }else{
    slog("Unable to rate game $gameId, invalid game type \"$gameType\" !",1);
    return 0;
  }

  foreach my $userId (keys %postGameRatings) {
    $p_ratings->{$userId}=$postGameRatings{$userId};
    $sldb->do("insert into tsGames values ($quotedGameId,$userAccounts{$userId},$userId,$quotedModShortName,'$gdrTimestamp',$preGameRatings{$userId}->{global}->{mu},$preGameRatings{$userId}->{global}->{sigma},$postGameRatings{$userId}->{global}->{mu},$postGameRatings{$userId}->{global}->{sigma})","insert new global rating for user $userId in tsGames table");
  }
}

sub applyMonthPenalties {
  my ($rateYear,$rateMonth,$modShortName)=@_;
  $rateMonth=sprintf('%02d',$rateMonth);
  my $ratePeriod=$rateYear.$rateMonth;

  slog("Applying month penalties for month $rateYear-$rateMonth ($modShortName)",4);

  my $penaltyTime=time;

  my ($nextYear,$nextMonth)=nextYearMonth($rateYear,$rateMonth);
  my $quotedModShortName=$sldb->quote($modShortName);
  my $sth=$sldb->prepExec("select tsp.userId,coalesce(pGames.nbGames,0),tsp.nbPenalties from tsPlayers tsp
  left join (select userId,count(*) nbGames from tsGames
               where modShortName=$quotedModShortName
                     and gdrTimestamp >= '$rateYear-$rateMonth-01'
                     and gdrTimestamp < '$nextYear-$nextMonth-01'
                     group by userId) pGames on tsp.userId=pGames.userId
  where tsp.period=$ratePeriod
        and tsp.modShortName=$quotedModShortName
        and tsp.mu > $conf{inactivityPenalty}->{minMu}
        and tsp.sigma < $conf{inactivityPenalty}->{maxSigma}
        and tsp.nbPenalties < $conf{inactivityPenalty}->{maxPenalties}
        and coalesce(pGames.nbGames,0) <  $conf{inactivityPenalty}->{threshold}","retrieve inactive players for $modShortName during month $rateYear-$rateMonth");
  my ($nbPenalizedUsers,$totalPenalties)=(0,0);
  my @inactiveData;
  while(@inactiveData=$sth->fetchrow_array()) {
    my ($userId,$nbGames,$nbPenalties)=@inactiveData;
    foreach my $gameType (keys %gameRatingMapping) {
      my $sth2=$sldb->prepExec("select mu,sigma from ts${gameType}Players where period=$ratePeriod and userId=$userId and modShortName=$quotedModShortName","select $modShortName $gameRatingMapping{$gameType} ratings for inactive user $userId");
      my @ratingData=$sth2->fetchrow_array();
      error("Unable to retrieve $modShortName $gameRatingMapping{$gameType} ratings for inactive user $userId") unless(@ratingData);
      my ($mu,$sigma)=@ratingData;
      my $nbPenaltiesToApply=0;
      while($mu > $conf{inactivityPenalty}->{minMu}
          && $sigma < $conf{inactivityPenalty}->{maxSigma}
          && $nbPenaltiesToApply < ($conf{inactivityPenalty}->{threshold} - $nbGames)
          && $nbPenalties + $nbPenaltiesToApply < $conf{inactivityPenalty}->{maxPenalties}) {
        $nbPenaltiesToApply++;
        $mu-=$conf{inactivityPenalty}->{muPenalty};
        $sigma+=$conf{inactivityPenalty}->{sigmaPenalty};
      }
      applyPenalties($userId,$ratePeriod,$quotedModShortName,$gameType,$nbPenaltiesToApply);
      if($gameType eq '') {
        $nbPenalizedUsers++;
        $totalPenalties+=$nbPenaltiesToApply;
      }
    }
  }
  slog("$nbPenalizedUsers players penalized for inactivity ($totalPenalties penalties applied)",4) if($totalPenalties > 0);

  $penaltyTime=time-$penaltyTime;
  my $recoveryTime=time;

  $sth=$sldb->prepExec("select tsp.userId,tsp.nbPenalties,count(*) nbGames from tsPlayers tsp,tsGames tsg
  where tsp.period=$ratePeriod
        and tsp.userId=tsg.userId
        and tsp.modShortName=$quotedModShortName
        and tsp.nbPenalties > 0
        and tsg.modShortName=$quotedModShortName
        and tsg.gdrTimestamp >= '$rateYear-$rateMonth-01'
        and tsg.gdrTimestamp < '$nextYear-$nextMonth-01'
  group by userId
  having nbGames > $conf{inactivityPenalty}->{threshold}","retrieve players back from inactivity for $modShortName during month $rateYear-$rateMonth");
  ($nbPenalizedUsers,$totalPenalties)=(0,0);
  while(@inactiveData=$sth->fetchrow_array()) {
    my ($userId,$nbPenalties,$nbGames)=@inactiveData;
    my $nbPenaltiesToDrop=$nbGames - $conf{inactivityPenalty}->{threshold};
    $nbPenaltiesToDrop=$nbPenalties if($nbPenaltiesToDrop > $nbPenalties);
    foreach my $gameType (keys %gameRatingMapping) {
      $sldb->do("update ts${gameType}Players set nbPenalties = 0 where period=$ratePeriod and userId=$userId and modShortName=$quotedModShortName and nbPenalties > 0 and nbPenalties < $nbPenaltiesToDrop","purge penalties for user $userId, mod $modShortName and gameType $gameType");
      $sldb->do("update ts${gameType}Players set nbPenalties = nbPenalties-$nbPenaltiesToDrop where period=$ratePeriod and userId=$userId and modShortName=$quotedModShortName and nbPenalties >= $nbPenaltiesToDrop","drop $nbPenaltiesToDrop penalties for user $userId, mod $modShortName anbd gameType $gameType");
    }
    $nbPenalizedUsers++;
    $totalPenalties+=$nbPenaltiesToDrop;
  }
  slog("$nbPenalizedUsers players recovered from inactivity period ($totalPenalties penalties dropped)",4) if($totalPenalties > 0);

  $recoveryTime=time-$recoveryTime;

  slog("Batch penalty process performances for $rateYear-$rateMonth ($modShortName): ${penaltyTime}s/${recoveryTime}s",4);  
}

sub applyPenalties {
  my ($userId,$ratePeriod,$quotedModShortName,$gameType,$nbPenaltiesToApply)=@_;
  my ($muPenalty,$sigmaPenalty)=($nbPenaltiesToApply*$conf{inactivityPenalty}->{muPenalty},$nbPenaltiesToApply*$conf{inactivityPenalty}->{sigmaPenalty});
  my $skillPenalty=$muPenalty+3*$sigmaPenalty;
  $sldb->do("update ts${gameType}Players set mu=mu-$muPenalty,sigma=sigma+$sigmaPenalty,skill=skill-$skillPenalty,nbPenalties=nbPenalties+$nbPenaltiesToApply where period=$ratePeriod and userId=$userId and modShortname=$quotedModShortName","apply penalties for user $userId in table ts${gameType}Players for period $ratePeriod");
}

sub rateMonth {
  my ($rateYear,$rateMonth,$modShortName)=@_;
  $rateMonth=sprintf('%02d',$rateMonth);
  
  slog("Rating month $rateYear-$rateMonth ($modShortName)...",4);

  my $preRatingTime=time;

  $sldb->{dbh}->{AutoCommit}=0;

  my ($previousYear,$previousMonth)=previousYearMonth($rateYear,$rateMonth);
  my ($nextYear,$nextMonth)=nextYearMonth($rateYear,$rateMonth);

  my $ratePeriod=$rateYear.$rateMonth;
  my $previousRatePeriod=$previousYear.$previousMonth;

  my %ratings;
  my $quotedModShortName=$sldb->quote($modShortName);

  foreach my $gameType (keys %gameRatingMapping) {
    my $ratingType=$gameRatingMapping{$gameType};
    my $sth=$sldb->prepExec("select userId,mu,sigma from ts${gameType}Players where period=$previousRatePeriod and modShortName=$quotedModShortName","select previous ratings for type $ratingType and period $rateYear-$rateMonth (=> previous period = $previousYear-$previousMonth) from table ts${gameType}Players");
    my @ratingData;
    while(@ratingData=$sth->fetchrow_array()) {
      $ratingData[1]+=0;
      $ratingData[2]+=0;
      if(exists $ratings{$ratingData[0]}) {
        $ratings{$ratingData[0]}->{$ratingType}=new Rating($ratingData[1],$ratingData[2]);
      }else{
        $ratings{$ratingData[0]}={$ratingType => new Rating($ratingData[1],$ratingData[2])};
      }
    }
    $sldb->do("delete from ts${gameType}Games where modShortName=$quotedModShortName and gdrTimestamp >= '$rateYear-$rateMonth-01' and gdrTimestamp < '$nextYear-$nextMonth-01'","flush ts${gameType}Games table for $modShortName($rateYear-$rateMonth)");
  }
  my $sth=$sldb->prepExec("select g.gameId,gd.gdrTimestamp,type from games g,gamesDetails gd,gamesNames gn where g.gameId=gd.gameId and gd.gdrTimestamp >= '$rateYear-$rateMonth-01' and gd.gdrTimestamp < '$nextYear-$nextMonth-01' and type != 'Solo' and bots = 0 and undecided = 0 and gn.shortName=$quotedModShortName and g.modName regexp gn.regex order by gd.gdrTimestamp,gd.gameId","extract games for month rating of $modShortName($rateYear-$rateMonth)");

  $preRatingTime=time-$preRatingTime;
  
  my $calculationTime=time;

  my @gameData;
  while(@gameData=$sth->fetchrow_array()) {
    rateGameBatch(\%ratings,$gameData[0],$gameData[1],$gameData[2],$modShortName);
  }

  $calculationTime=time-$calculationTime;

  my $postRatingTime=time;

  foreach my $gameType (keys %gameRatingMapping) {
    $sldb->do("delete from ts${gameType}Players where period=$ratePeriod and modShortName=$quotedModShortName","flush ts${gameType}Players table for $modShortName and period $ratePeriod");
    my $ratingType=$gameRatingMapping{$gameType};
    foreach my $userId (keys %ratings) {
      my ($newMu,$newSigma)=($ratings{$userId}->{$ratingType}->{mu},$ratings{$userId}->{$ratingType}->{sigma});
      my $newSkill=$newMu-3*$newSigma;
      $sldb->do("insert into ts${gameType}Players values ($ratePeriod,$userId,$quotedModShortName,$newSkill,$newMu,$newSigma,0)","insert new rating for user $userId, period $ratePeriod and type $ratingType in ts${gameType}Players table");
    }
    $sldb->do("update ts${gameType}Players tsp,ts${gameType}Players prevTsp set tsp.nbPenalties=prevTsp.nbPenalties where tsp.period=$ratePeriod and prevTsp.period=$previousRatePeriod and tsp.userId=prevTsp.userId and tsp.modShortName=$quotedModShortName and prevTsp.modShortName=$quotedModShortName",'copy inactivity penalties from previous month');
  }


  $sldb->{dbh}->commit();

  $sldb->{dbh}->{AutoCommit}=1;

  $postRatingTime=time-$postRatingTime;

  my $perfLogLevel=4;
  $perfLogLevel=3 if(${preRatingTime}+${calculationTime}+${postRatingTime} > 60);
  $perfLogLevel=2 if(${preRatingTime}+${calculationTime}+${postRatingTime} > 90);
  slog("Batch rating performances for $rateYear-$rateMonth ($modShortName): ${preRatingTime}s/${calculationTime}s/${postRatingTime}s",$perfLogLevel);
}

sub rateModFromMonth {
  my ($startYear,$startMonth,$modShortName)=@_;
  slog("Rating game $modShortName from month $startYear-$startMonth...",3);
  my $currentYear=currentYear();
  my $currentMonth=currentMonth();
  for my $rateYear ($startYear..$currentYear) {
    my $firstMonth=1;
    $firstMonth=$startMonth if($rateYear == $startYear);
    my $lastMonth=12;
    $lastMonth=$currentMonth if($rateYear == $currentYear);
    for my $rateMonth ($firstMonth..$lastMonth) {
      rateMonth($rateYear,$rateMonth,$modShortName);
      applyMonthPenalties($rateYear,$rateMonth,$modShortName) unless($rateYear == $currentYear && $rateMonth == $currentMonth);
    }
  }
}

slog("Checking rating database state",3);
my $p_ratingState=$sldb->getRatingState();
if(! exists $p_ratingState->{currentRatingMonth}) {  
  slog("Start of batch rating (ratings initialization)",3);
  $sldb->do("insert into tsRatingState values('batchRatingStatus',1) on duplicate key update value=1",'update batchRatingStatus parameter to 1 in tsRatingState table');
  my $p_allMods=$sldb->getModsShortNames();
  foreach my $modShortName (@{$p_allMods}) {
    my $quotedModShortName=$sldb->quote($modShortName);
    my $sth=$sldb->prepExec("select YEAR(gd.gdrTimestamp),MONTH(gd.gdrTimestamp) from games g,gamesDetails gd,gamesNames gn where g.gameId=gd.gameId and gd.type != 'Solo' and gd.bots = 0 and gd.undecided = 0 and gn.shortName=$quotedModShortName and g.modName regexp gn.regex order by gd.gdrTimestamp limit 1","select first ratable $modShortName game from games and gamesDetails");
    my @dataFound=$sth->fetchrow_array();
    rateModFromMonth($dataFound[0],$dataFound[1],$modShortName) if(@dataFound);
  }
  $currentRatingYear=currentYear();
  $currentRatingMonth=currentMonth();
  $sldb->do("insert into tsRatingState values('currentRatingYear',$currentRatingYear)","initialize currentRatingYear parameter to $currentRatingYear in tsRatingState table");
  $sldb->do("insert into tsRatingState values('currentRatingMonth',$currentRatingMonth)","initialize currentRatingMonth parameter to $currentRatingMonth in tsRatingState table");
  $sldb->do("update tsRatingState set value=0 where param='batchRatingStatus'",'update batchRatingStatus parameter to 0 in tsRatingState table');
  slog("End of batch rating (ratings initialization)",3);
}else{
  $currentRatingYear=$p_ratingState->{currentRatingYear};
  $currentRatingMonth=$p_ratingState->{currentRatingMonth};
}
$currentRatingMonth=sprintf('%02d',$currentRatingMonth);

my $rerateTs=0;
slog("Starting rating queues polling...",3);
while($running && time < $running) {
  my $sth=$sldb->prepExec("select accountId,UNIX_TIMESTAMP(accountTimestamp) from tsRerateAccounts where accountTimestamp > FROM_UNIXTIME($rerateTs)",'check for new accounts to rerate');
  my $p_accountsToRerate=$sth->fetchall_arrayref();
  if(@{$p_accountsToRerate}) {
    slog("Found potential accounts to rerate, checking for unneeded rerates...",4);
    my $newRerateTs=0;
    foreach my $p_accountToRerate (@{$p_accountsToRerate}) {
      my ($accountId,$accountTs)=($p_accountToRerate->[0],$p_accountToRerate->[1]);
      $sth=$sldb->prepExec("select count(*) from tsGames where accountId=$accountId","check if account $accountId has already been rated in tsGames table");
      my @countRes=$sth->fetchrow_array();
      error("Unable to query table tsGames to check for unneeded global rerate for account $accountId") unless(@countRes);
      if($countRes[0] == 0) {
        slog("Global rerate is not needed for account $accountId (no game rated yet for this account)",4);
        $sldb->do("delete from tsRerateAccounts where accountId=$accountId","remove unneeded global rerate for account $accountId from tsRerateAccounts table");
      }else{
        $newRerateTs=$accountTs if($newRerateTs < $accountTs);
      }
    }
    if($newRerateTs) {
      if($rerateTs < $newRerateTs) {
        $rerateTs=$newRerateTs;
        my $rerateScheduleTime=$rerateTs+$conf{rerateDelay};
        if(time >= $rerateScheduleTime) {
          $rerateScheduleTime='now!';
        }else{
          $rerateScheduleTime='in '.secToTime($rerateScheduleTime - time);
        }
        slog("Found new accounts to rerate, global rerate scheduled $rerateScheduleTime",3);
      }else{
        slog('Inconsistent state: found new rerate accounts which should have been treated before? cancelling global rerate to start over check algorithm!',2);
        $rerateTs=0;
      }
    }
  }
  if($rerateTs && time >= $rerateTs+$conf{rerateDelay}) {
    slog("Starting global rerate process, checking mods to rerate...",3);
    $rerateTs=0;
    $sth=$sldb->prepExec("select YEAR(min(tsg.gdrTimestamp)),MONTH(min(tsg.gdrTimestamp)),tsg.modShortName from tsGames tsg,tsRerateAccounts tsra where tsg.accountId=tsra.accountId group by tsg.modShortName",'retrieve rerate start date from tsGames and tsRerateAccounts tables');
    my %rerateYearMonthByMod;
    my @rerateTimeData;
    while(@rerateTimeData=$sth->fetchrow_array()) {
      $rerateYearMonthByMod{$rerateTimeData[2]}={year => $rerateTimeData[0], month => $rerateTimeData[1]};
    }
    if(! %rerateYearMonthByMod) {
      slog('Unable to find any rerate start date, cancelling global rerate!',1);
    }else{
      $sldb->do("truncate table tsRerateAccounts");
      my @modsStartDates;
      foreach my $modShortName (keys %rerateYearMonthByMod) {
        push(@modsStartDates,"$modShortName (since $rerateYearMonthByMod{$modShortName}->{year}-$rerateYearMonthByMod{$modShortName}->{month})");
      }
      slog('Start of batch rating, rerate queue: '.join(', ',@modsStartDates),3);
      $sldb->do("insert into tsRatingState values('batchRatingStatus',1) on duplicate key update value=1",'update batchRatingStatus parameter to 1 in tsRatingState table');
      foreach my $modShortName (keys %rerateYearMonthByMod) {
        my ($rerateYear,$rerateMonth)=($rerateYearMonthByMod{$modShortName}->{year},$rerateYearMonthByMod{$modShortName}->{month});
        if($rerateYear > $currentRatingYear || ($rerateYear == $currentRatingYear && $rerateMonth > $currentRatingMonth)) {
          slog("Rerate start date \"$rerateYear-$rerateMonth\" is a future date, cancelling global rerate for $modShortName!",1);
        }else{
          rateModFromMonth($rerateYear,$rerateMonth,$modShortName);
        }
      }
      $sldb->do("update tsRatingState set value=0 where param='batchRatingStatus'",'update batchRatingStatus parameter to 0 in tsRatingState table');
      slog('End of batch rating, processed: '.join(', ',@modsStartDates),3);
    }
  }
  $sth=$sldb->prepExec("select gameId from tsRatingQueue where status=0 order by gdrTimestamp,gameId","retrieve games queued for rating in tsRatingQueue table");
  my $p_newGamesToRate=$sth->fetchall_arrayref();
  if(! @{$p_newGamesToRate}) {
    sleep(1);
    next;
  }
  foreach my $p_newGameId (@{$p_newGamesToRate}) {
    my $gameId=$p_newGameId->[0];
    slog("Rating game \"$gameId\"",4);
    rateNewGame($gameId);
  }
}

if($running) {
  if($running == 1) {
    slog('Restarting',3);
  }else{
    slog('Process running since '.secToTime($conf{maxRunTime}).', restarting',3);
  }
  exec($0);
}else{
  slog("Exiting.",3);
}
