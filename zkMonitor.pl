#!/usr/bin/perl -w
#
# This file implements the Zero-K website monitoring functionality for SLDB
# (zkMonitor), it is part of SLDB.
#
# zkMonitor is an HTTP bot, it monitors the Zero-K website to detect newly
# finished battles and performs following operations on each of them:
# - extract the battle details data from Zero-K battle HTML page
# - check the battle details are consistent with SLDB data (stored by slMonitor)
# - store the battle details into SLDB if data are consistent
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

use strict;

use File::Basename qw/dirname fileparse/;
use File::Spec::Functions qw/catdir catfile file_name_is_absolute rel2abs/;
use File::Path;
use HTML::TreeBuilder;
use Storable qw/dclone/;
use Time::Piece;
use WWW::Mechanize;

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

my %conf=(baseUrl => 'http://zero-k.info',
          linkBattles => '/Battles',
          linkBattleDetail => '/Battles/Detail/',
          linkUserDetail => '/Home/GetTooltip?key=%24user%24',
          firstGameIndex => 90492,
          logLevel => 4,
          sldbLogLevel => 4,
          logFile => catfile('var','log',"$scriptBaseName.log"),
          cacheDir => catdir('var','cache'),
          dbName => 'sldb',
          dbLogin => $scriptBaseName,
          dbPwd => undef,
          httpRetryDelay => 60,
          minGameLength => 180,
          timeshift => 0,
          tsTolerance => 60,
          refreshDelay => 300,
          maxRunTime => 86400
    );
SimpleConf::readConf($confFile,\%conf);

my $logFile=$conf{logFile};
$logFile=catfile($scriptDir,$logFile) unless(file_name_is_absolute($logFile));
mkpath(dirname($logFile));

my $cacheDir=$conf{cacheDir};
$cacheDir=catdir($scriptDir,$cacheDir) unless(file_name_is_absolute($cacheDir));
mkpath($cacheDir);

my $dbDs=$conf{dbName};
$dbDs="DBI:mysql:database=$dbDs;host=localhost" unless($dbDs =~ /^DBI:/i);

my $sLog=SimpleLog->new(logFiles => [$conf{logFile},''],
                        logLevels => [$conf{logLevel},3],
                        useANSICodes => [0,1],
                        useTimestamps => [1,1],
                        prefix => "[zkMonitor] ");
my $sLogSldb=SimpleLog->new(logFiles => [$conf{logFile},''],
                            logLevels => [$conf{sldbLogLevel},3],
                            useANSICodes => [0,1],
                            useTimestamps => [1,1],
                            prefix => "[SLDB] ");

my $sldb=Sldb->new({dbDs => $dbDs,
                    dbLogin => $conf{dbLogin},
                    dbPwd => $conf{dbPwd},
                    sLog => $sLogSldb,
                    sqlErrorHandler => \&error});
$sldb->connect();
my $sth;

my $running=time + $conf{maxRunTime};

$SIG{TERM} = \&sigTermHandler;
$SIG{USR1} = \&sigUsr1Handler;

my %invalidBattles=(115733 => 1,
                    139092 => 1,
                    140865 => 1,
                    165625 => 1,
                    165650 => 1,
                    166191 => 1,
                    166196 => 1,
                    166533 => 1,
                    166563 => 1,
                    166565 => 1,
                    166667 => 1,
                    166783 => 1,
                    166784 => 1,
                    166786 => 1,
                    166792 => 1,
                    213035 => 1,
                    222008 => 1,
                    230357 => 1,
                    245821 => 1,
                    245825 => 1,
                    245917 => 1,
                    245930 => 1,
                    246088 => 1,
                    246091 => 1,
                    246093 => 1,
                    293614 => 1,
                    295599 => 1,
                    295639 => 1,
                    296626 => 1);
my %manualStartTsBattles=(92081 => '2012-07-20 00:50:31',
                          114235 => '2012-10-14 21:13:33');

my $mech = WWW::Mechanize->new(agent => 'Mozilla/5.0 (compatible; Ask Jeeves/Teoma; +http://about.ask.com/en/docs/about/webmasters.shtml)',
                               timeout => 6,
                               autocheck => 0);

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

sub buildTimestamp {
  my @time = localtime();
  $time[4]++;
  @time = map(sprintf("%02d",$_),@time);
  return ($time[5]+1900).$time[4].$time[3].$time[2].$time[1].$time[0]
}

sub errorHtml {
  my $m=shift;
  slog($m,0);
  my $ts=buildTimestamp();
  $mech->save_content("error.$ts.html");
  exit 1;
}

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

sub getLatestBattleId {
  while(1) {
    $mech->get($conf{baseUrl}.$conf{linkBattles});
    if($mech->success()) {
      if($mech->content() =~ /\'$conf{linkBattleDetail}(\d+)\'/) {
        return $1;
      }else{
        errorHtml("Unable to find any battle detail link (\'$conf{linkBattleDetail}\\d+\') from page \"$conf{baseUrl}.$conf{linkBattles}\"");
      }
    }else{
      my $status=$mech->status();
      if($running) {
        slog("Unable to query page \"$conf{baseUrl}$conf{linkBattles}\" (HTTP status: $status), retrying in $conf{httpRetryDelay} seconds",2);
      }else{
        slog("Unable to query page \"$conf{baseUrl}$conf{linkBattles}\" (HTTP status: $status)",2);
        return 0;
      }
    }
    sleep($conf{httpRetryDelay});
  }
}

sub getBattleDetail {
  my $bId=shift;

  my $cacheIndex=int($bId/1000);

  if(! -f "$cacheDir/$cacheIndex/$bId.html") {
    while(1) {
      $mech->get($conf{baseUrl}.$conf{linkBattleDetail}.$bId);
      if($mech->success()) {
        error("Unable to create $cacheDir/$cacheIndex directory") unless(-d "$cacheDir/$cacheIndex" || mkdir("$cacheDir/$cacheIndex"));
        $mech->save_content("$cacheDir/$cacheIndex/$bId.html");
        last;
      }else{
        my $status=$mech->status();
        if($running) {
          slog("Unable to query page \"$conf{baseUrl}$conf{linkBattleDetail}$bId\" for battle detail (HTTP status: $status), retrying in $conf{httpRetryDelay} seconds",2);
        }else{
          slog("Unable to query page \"$conf{baseUrl}$conf{linkBattleDetail}$bId\" for battle detail (HTTP status: $status)",2);
          return {};
        }
      }
      sleep($conf{httpRetryDelay});
    }
  }

  my %bDetails=(battleId => $bId);

  my $tree=HTML::TreeBuilder->new;
  $tree->parse_file("$cacheDir/$cacheIndex/$bId.html");
  my $mainDiv=$tree->look_down(_tag => 'div',
                               id => 'renderbody');
  error("Unable to find main \"div\" in battle detail page of battle \#$bId") unless($mainDiv);

  my $mapLink=$mainDiv->look_down(_tag => 'a',
                                  sub { my @linkContents = $_[0]->content_list();
                                        @linkContents && (! ref($linkContents[0])) && ($linkContents[0] =~ /Map:/);
                                  });
  error("Unable to find map name in battle detail page of battle \#$bId") unless($mapLink);
  if($mapLink->as_text =~ /Map:\s*(.+)$/) {
    $bDetails{mapName}=$1;
  }else{
    error("Unable to parse map name from \"".($mapLink->as_text)."\" in battle detail page of battle \#$bId");
  }

  my $detailTable=$mainDiv->look_down(_tag => 'table');
  error("Unable to find detail table in battle detail page of battle \#$bId") unless($detailTable);
  my @detailsRows=$detailTable->content_list();
  my %detailTableContent;
  my $hostName='';
  foreach my $detailRow (@detailsRows) {
    my @detailCells=$detailRow->content_list();
    if($#detailCells != 1) {
      my $detailRowText=$detailRow->as_text;
      slog("Invalid number of entries in detail row \"$detailRowText\", ignoring...",2);
      next;
    }
    if($detailCells[0]->as_trimmed_text eq 'Host:') {
      my $userDetailLink=$detailCells[1]->look_down(_tag => 'a',
                                                    sub {defined $_[0]->attr('href') && $_[0]->attr('href') =~ /\/Users\/Detail\/\d+$/});
      if(! defined $userDetailLink) {
        my $detailCellText=$detailCells[1]->as_text;
        error("Unable to find user detail link in detail entry \"$detailCellText\" in battle detail page of battle \#$bId");
      }
      if($userDetailLink->attr('href') =~ /\/Users\/Detail\/(\d+)$/) {
        $bDetails{hostAccountId}=$1;
        $hostName=$userDetailLink->as_trimmed_text;
      }else{
        error("Unable to parse host account ID from \"".($userDetailLink->attr('href'))."\" in battle detail page of battle \#$bId");
      }
    }else{
      $detailTableContent{$detailCells[0]->as_trimmed_text}=$detailCells[1]->as_trimmed_text;
    }
  }
  error("Unable to find host account id in battle detail page of battle \#$bId") unless(exists $bDetails{hostAccountId});
  my %detailTableMapping=(engine => 'Engine version:',
                          duration => 'Duration:',
                          bots => 'Bots:',
                          nbPlayers => 'Players:');
  for my $detailName (keys %detailTableMapping) {
    error("Missing mandatory detail \"$detailTableMapping{$detailName}\" in battle detail page of battle \#$bId") unless(exists $detailTableContent{$detailTableMapping{$detailName}});
    $bDetails{$detailName}=$detailTableContent{$detailTableMapping{$detailName}};
  }
  if($bDetails{duration} =~ /^(\d+) seconds$/) {
    $bDetails{duration}=$1;
  }elsif($bDetails{duration} =~ /^(\d+) minutes$/) {
    $bDetails{duration}=$1*60;
  }elsif($bDetails{duration} =~ /^(\d+) hours$/) {
    $bDetails{duration}=$1*3600;
  }else{
    error("Unable to recognize duration format \"$bDetails{duration}\" in battle detail page of battle \#$bId");
  }
  
  if(exists $manualStartTsBattles{$bId}) {
    $bDetails{startDate}=$manualStartTsBattles{$bId};
  }else{
    my $replayLink=$mainDiv->look_down(_tag => 'a',
                                       sub {defined $_[0]->attr('href') && $_[0]->attr('href') =~ /^$conf{baseUrl}\/replays\/\d+_\d+_.*\.sdf$/});
    if(! $replayLink) {
      $tree->delete;
      return { skip => 1 };
      error("Unable to find replay link in battle detail page of battle \#$bId");
    }
    if($replayLink->attr('href')=~ /^$conf{baseUrl}\/replays\/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2}+)_.*\.sdf$/) {
      my $replayStartTime=Time::Piece->strptime("$1-$2-$3 $4:$5:$6",'%Y-%m-%d %T');
      $replayStartTime+=$conf{timeshift};
      $bDetails{startDate}=$replayStartTime->strftime('%F %T');
    }else{
      error("Unable to parse replay link \"".($replayLink->attr('href'))."\" in battle detail page of battle \#$bId");
    }
  }

  my %teamCounts;
  my %playerTeams;
  my %playerNames;
  my %teamResults;
  my %groupResults=(loser => 0, winner => 1, spec => 0);
  my ($winnerFound,$loserFound)=(0,0);
  for my $group (keys %groupResults) {
    my @groupDivs=$mainDiv->look_down(_tag => 'div',
                                      sub { defined $_[0]->attr('class') && ($_[0]->attr('class') =~ /battle_$group/ || $_[0]->attr('class') =~ /team $group/)});
    foreach my $div (@groupDivs) {
      my $team='';
      if($group ne 'spec') {
        my $span=$div->look_down(_tag => 'span');
        error("Unable to find team number for a $group group in battle detail page of battle \#$bId") unless($span);
        if($span->as_trimmed_text =~ /Team (\d+) /) {
          $team=$1-1;
        }else{
          error("Unable to find team number for a $group group from \"".($span->as_trimmed_text)."\" in battle detail page of battle \#$bId");
        }
        $winnerFound=1 if($group eq 'winner');
        $loserFound=1 if($group eq 'loser');
      }
      $teamResults{$team}=$groupResults{$group};
      my @userDetailLinks=$div->look_down(_tag => 'a',
                                          sub {defined $_[0]->attr('href') && $_[0]->attr('href') =~ /\/Users\/Detail\/\d+$/});
      error("Empty team $team found while parsing battle detail page of battle \#$bId") unless($group eq 'spec' || @userDetailLinks);
      foreach my $userDetailLink (@userDetailLinks) {
        if($userDetailLink->attr('href') =~ /\/Users\/Detail\/(\d+)$/) {
          $playerTeams{$1}=$team;
          $playerNames{$1}=$userDetailLink->as_trimmed_text;
          if($team ne '') {
            $teamCounts{$team}=0 unless(exists $teamCounts{$team});
            $teamCounts{$team}++;
          }
        }else{
          error("Unable to find user detail link from \"".($userDetailLink->attr('href'))."\" in battle detail page of battle \#$bId");
        }
      }
    }
  }
  error("Inconsistent number of players while parsing battle detail page of battle \#$bId") unless($bDetails{nbPlayers} == grep {$playerTeams{$_} ne ''} (keys %playerTeams));
  my $undecided=0;
  if($bDetails{bots} eq 'False') {
    if(! ($winnerFound && $loserFound)) {
      $undecided=1;
      foreach my $team (keys %teamResults) {
        $teamResults{$team}=2 unless($team eq '');
      }
    }
    $bDetails{bots}={};
  }else{
    my $botTeam=0;
    while(exists $teamResults{$botTeam}) {
      $botTeam++;
    }
    $bDetails{bots}={team => $botTeam,
                     win => 0};
    $bDetails{bots}->{win}=1 unless(grep {/^1$/} (values %teamResults));
    $teamCounts{$botTeam}=1;
  }
  $bDetails{players}={};
  foreach my $playerId (keys %playerTeams) {
    $bDetails{players}->{$playerId}={team => $playerTeams{$playerId},
                                     name => $playerNames{$playerId},
                                     win => $teamResults{$playerTeams{$playerId}}};
  }
  if(! exists $bDetails{players}->{$bDetails{hostAccountId}}) {
    $bDetails{players}->{$bDetails{hostAccountId}}={team => '', 
                                                    name => $hostName,
                                                    win => 0};
  }
  if($undecided) {
    $bDetails{result}='undecided';
  }else{
    $bDetails{result}='gameOver';
  }

  my $maxTeamSize=0;
  my $nbTeams=0;
  my @teamSizes;
  foreach my $teamNb (sort keys %teamCounts) {
    $nbTeams++;
    $maxTeamSize=$teamCounts{$teamNb} if($teamCounts{$teamNb} > $maxTeamSize);
    push(@teamSizes,$teamCounts{$teamNb});
  }
  my $gameStructure=join('v',@teamSizes);
  my $gameType='Solo';
  if($nbTeams == 2) {
    if($maxTeamSize == 1) {
      $gameType='Duel';
    }else{
      $gameType='Team';
    }
  }elsif($nbTeams > 2) {
    if($maxTeamSize == 1) {
      $gameType='FFA';
      $gameStructure=$nbTeams.'-way';
    }else{
      $gameType='TeamFFA';
    }
  }
  $bDetails{structure}=$gameStructure;
  $bDetails{type}=$gameType;

  $tree->delete;

  return \%bDetails;
}

sub getUserAliases {
  my $id=shift;
  if(! -f "$cacheDir/user/$id.html") {
    while(1) {
      $mech->get($conf{baseUrl}.$conf{linkUserDetail}.$id);
      if($mech->success()) {
        error("Unable to create $cacheDir/user directory") unless(-d "$cacheDir/user" || mkdir("$cacheDir/user"));
        $mech->save_content("$cacheDir/user/$id.html");
        last;
      }else{
        my $status=$mech->status();
        if($running) {
          slog("Unable to query page \"$conf{baseUrl}$conf{linkUserDetail}$id\" for user detail (HTTP status: $status), retrying in $conf{httpRetryDelay} seconds",2);
        }else{
          slog("Unable to query page \"$conf{baseUrl}$conf{linkUserDetail}$id\" for user detail (HTTP status: $status)",2);
          return [];
        }
      }
      sleep($conf{httpRetryDelay});
    }
  }
  my $tree=HTML::TreeBuilder->new;
  $tree->parse_file("$cacheDir/user/$id.html");
  my $bAliases=$tree->look_down(_tag => 'b',
                                sub { $_[0]->as_trimmed_text eq 'Aliases:' });
  if(! $bAliases) {
    slog("Unable to find aliases \"b\" tag in user detail page of user \#$id",2);
    return [];
  }
  my $aliasesTextNode=$bAliases->right();
  error("Unable to find aliases text node in user detail page of user \#$id") unless(defined $aliasesTextNode);
  my @aliases=split(/,/,$aliasesTextNode);

  $tree->delete;

  return \@aliases;
}

sub gdrReconciliation {
  my $p_gdr=shift;
  my $gdrTime=time;
  my $userAccountId=$p_gdr->{hostAccountId};
  my ($quotedGameId,$quotedMap,$quotedStartDate)=$sldb->quote("zk-$p_gdr->{battleId}",$p_gdr->{mapName},$p_gdr->{startDate});

  slog("Received a game data report for \#$userAccountId (gameId:zk-$p_gdr->{battleId},startDate:$p_gdr->{startDate},duration:$p_gdr->{duration},type:$p_gdr->{type},structure:$p_gdr->{structure})",4);

  if($p_gdr->{duration} < $conf{minGameLength}) {
    slog("Game too short ($p_gdr->{duration}), discarding GDR!",4);
    return 1;
  }

  $sth=$sldb->prepExec("select count(*) from games where gameId=$quotedGameId","check for duplicate gameId in database!");
  my @gameIdCount=$sth->fetchrow_array();
  error("Unable to check for duplicate GDR") unless(@gameIdCount);
  if($gameIdCount[0] > 0) {
    slog("Duplicate gameId (zk-$p_gdr->{battleId}), discarding GDR!",2);
    return 2;
  }

  my $quotedHostName=$sldb->quote($p_gdr->{players}->{$userAccountId}->{name});
  $sth=$sldb->prepExec("select startTimestamp,nbPlayer,hostAccountId from games where (hostAccountId=$userAccountId or hostName=$quotedHostName) and modName like 'Zero-K %' and mapName=$quotedMap and gameId is NULL and ABS(TIMESTAMPDIFF(SECOND,startTimestamp,$quotedStartDate)) < $conf{tsTolerance} order by (ABS(TIMESTAMPDIFF(SECOND,startTimestamp,$quotedStartDate)))","perform reconciliation for zk-$p_gdr->{battleId} in database");
  my $foundTs=0;
  my $foundTsMatchingEntries=0;
  my @possibleTs;
  while(@possibleTs=$sth->fetchrow_array()) {
    $foundTsMatchingEntries=1;
    my $realUserId=$possibleTs[2];

    slog("Trying reconciliation of \"zk-$p_gdr->{battleId}\" with [$realUserId,$possibleTs[0]]",5);
    my $sth2=$sldb->prepExec("select accountId,name from players where hostAccountId=$realUserId and DATE_FORMAT(startTimestamp,'%Y-%m-%d %H:%i:%S')=\"$possibleTs[0]\"","list players for game [$realUserId,$possibleTs[0]]!");
    my %realIdToRealName;
    my @player;
    while(@player=$sth2->fetchrow_array()) {
      $realIdToRealName{$player[0]}=$player[1];
    }

    my $idsInGame=join(',',keys %realIdToRealName);
    $sth2=$sldb->prepExec("select n.accountId,n.name from names n inner join (select accountId id,max(lastConnection) lastConn from names group by id) connTimes on n.accountId=connTimes.id and n.lastConnection=connTimes.lastConn where n.accountId in ($idsInGame)","retrieve new names of players in battle [$realUserId,$possibleTs[0]]");
    my (%zkNameToRealId,%realIdToZkName);
    while(@player=$sth2->fetchrow_array()) {
      $realIdToZkName{$player[0]}=$player[1];
      $zkNameToRealId{$player[1]}=$player[0];
    }

    my $compatible=1;

    my %newPlayers;
    foreach my $zkId (keys %{$p_gdr->{players}}) {
      my $zkPlayerName=$p_gdr->{players}->{$zkId}->{name};
      my $realId;
      if(! exists $zkNameToRealId{$zkPlayerName}) {
        foreach my $name (keys %zkNameToRealId) {
          if(lc($zkPlayerName) eq lc($name)) {
            slog("Restored case of player name \"$zkPlayerName\" to \"$name\" during reconciliation (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]])",5);
            $zkPlayerName=$name;
            last;
          }
        }
      }
      if(exists $zkNameToRealId{$zkPlayerName}) {
        $realId=$zkNameToRealId{$zkPlayerName};
      }else{
        slog("Unable to identify player \"$zkPlayerName\" (\#$zkId) during reconciliation (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]]), now trying with ZK aliases",2);
        my $p_zkAliases=getUserAliases($zkId);
        foreach my $zkAlias (@{$p_zkAliases}) {
          if(! exists $zkNameToRealId{$zkAlias}) {
            foreach my $name (keys %zkNameToRealId) {
              if(lc($zkAlias) eq lc($name)) {
                slog("Restored case of player \"$zkPlayerName\" alias \"$zkAlias\" to \"$name\" during reconciliation (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]])",5);
                $zkAlias=$name;
                last;
              }
            }
          }
          if(exists $zkNameToRealId{$zkAlias}) {
            $realId=$zkNameToRealId{$zkAlias};
            last;
          }
        }
        if(! defined $realId) {
          slog("Unable to identify player \"$zkPlayerName\" (\#$zkId) with ZK aliases, now trying with direct account ID mapping (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]])",2);
          if(! exists $realIdToRealName{$zkId}) {
            slog("Player \"$zkPlayerName\" (\#$zkId) was not marked as being in the battle, reconciliation failed! (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]])",2);
            $compatible=0;
            last;
          }
          $realId=$zkId;
        }
      }
      my $realName=$realIdToRealName{$realId};
      my $zkName=$realIdToZkName{$realId};
      $newPlayers{$realId}=dclone($p_gdr->{players}->{$zkId});
      $newPlayers{$realId}->{name}=$realName;
      delete $realIdToRealName{$realId};
      delete $realIdToZkName{$realId};
      delete $zkNameToRealId{$zkName};
      if($realId != $zkId || $realName ne $zkPlayerName) {
        slog("Resolved \"$zkPlayerName\" (\#$zkId) into \"$realName\" (\#$realId) during reconciliation (zk-$p_gdr->{battleId} [$realUserId,$possibleTs[0]])",5);
      }
    }
    if($compatible) {
      $p_gdr->{players}=\%newPlayers;
      $foundTs=$possibleTs[0];
      slog("Inconsistent number of players in GDR \#$p_gdr->{battleId} (in database: $possibleTs[1], in GDR: $p_gdr->{nbPlayers} [$userAccountId,$possibleTs[0]]",2) unless($p_gdr->{nbPlayers} == $possibleTs[1]);
      if($userAccountId != $realUserId) {
        slog("Reconciliation succeeded with game [$realUserId,$possibleTs[0]], with host account ID remapping for \"$p_gdr->{players}->{$realUserId}->{name}\" ($userAccountId -> $realUserId)",5);
        $userAccountId=$realUserId;
      }else{
        slog("Reconciliation succeeded with game [$realUserId,$possibleTs[0]]",5);
      }
      last;
    }
  }
  if(! $foundTs) {
    if($foundTsMatchingEntries) {
      slog("Unable to find compatible battle despite matching timestamps found, discarding GDR of \#$userAccountId [zk-$p_gdr->{battleId},$p_gdr->{startDate}]",2);
      return 3;
    }else{
      slog("Unable to find compatible battle, discarding GDR of \#$userAccountId [zk-$p_gdr->{battleId},$p_gdr->{startDate}]",2);
      return 4;
    }
  }

  $sldb->do("update games set gameId=$quotedGameId where hostAccountId=$userAccountId and DATE_FORMAT(startTimestamp,'%Y-%m-%d %H:%i:%S')=\"$foundTs\"","update gameId in table games");

  my $hasBot=0;
  $hasBot=1 if(%{$p_gdr->{bots}});
  my $undecided=0;
  $undecided=1 if($p_gdr->{result} eq 'undecided');
  my ($quotedEngine,$quotedType,$quotedStructure)=$sldb->quote($p_gdr->{engine},$p_gdr->{type},$p_gdr->{structure});
  $sldb->do("insert into gamesDetails values ($quotedGameId,FROM_UNIXTIME($gdrTime),$quotedStartDate,FROM_UNIXTIME(UNIX_TIMESTAMP($quotedStartDate)+$p_gdr->{duration}),$p_gdr->{duration},$quotedEngine,$quotedType,$quotedStructure,$hasBot,$undecided)","insert data in table gamesDetails");
  my $teamIndex=0;
  foreach my $playerId (keys %{$p_gdr->{players}}) {
    my $allyTeam=$p_gdr->{players}->{$playerId}->{team};
    my $team;
    if($allyTeam eq '') {
      $allyTeam='NULL';
      $team='NULL';
    }else{
      $team=$teamIndex++;
    }
    $sth=$sldb->prepExec("select name from players where hostAccountId=$userAccountId and DATE_FORMAT(startTimestamp,'%Y-%m-%d %H:%i:%S')=\"$foundTs\" and accountId=$playerId","retrieve original player name from players table");
    my @playerName=$sth->fetchrow_array();
    error("Unable to find original player name for account \#$playerId in battle hosted by \#$userAccountId on $foundTs") unless(@playerName);
    my $quotedPlayerName=$sldb->quote($playerName[0]);
    $sldb->do("insert into playersDetails values ($quotedGameId,$playerId,$quotedPlayerName,0,$team,$allyTeam,$p_gdr->{players}->{$playerId}->{win})","insert data in table playersDetails");
  }
  if($hasBot) {
    $sldb->do("insert into botsDetails values ($quotedGameId,'UNKNOWN',NULL,NULL,$teamIndex,$p_gdr->{bots}->{team},$p_gdr->{bots}->{win})","insert data in table botsDetails");
  }else{
    $sldb->do("insert into tsRatingQueue values ($quotedGameId,FROM_UNIXTIME($gdrTime),0)","add game zk-$p_gdr->{battleId} in rating queue table") unless($p_gdr->{type} eq 'Solo' || $undecided);
  }
  return 0;
}

my $nextBattleId=$conf{firstGameIndex};
$sth=$sldb->prepExec("select gameId from games where gameId like 'zk-%' order by endTimestamp desc limit 10");
my $latestProcessedGameId;
my @gameIdFound;
while(@gameIdFound=$sth->fetchrow_array()) {
  if($gameIdFound[0] =~ /^zk-(\d+)$/) {
    $latestProcessedGameId=$1 unless(defined $latestProcessedGameId && $latestProcessedGameId > $1);
  }
}
$nextBattleId=$latestProcessedGameId+1 if(defined $latestProcessedGameId);

my $latestBattleId=getLatestBattleId();
error("Unable to retrieve latest battle ID") unless($latestBattleId);

slog("Zero-K Monitor initialized with next battle ID: \#$nextBattleId, latest battle ID available: \#$latestBattleId",3);

while($running && time < $running) {
  while($nextBattleId > $latestBattleId && $running && time < $running) {
    sleep($conf{refreshDelay});
    $latestBattleId=getLatestBattleId();
    error("Unable to retrieve latest battle ID") unless($latestBattleId);
  }
  if($nextBattleId == $latestBattleId) {
    slog('Found 1 new battle to process',4);
  }elsif($nextBattleId < $latestBattleId) {
    slog('Found '.($latestBattleId-$nextBattleId+1).' new battles to process',4);
  }
  my $startProcessingTs=time;
  my $nbOfBattlesProcessed=0;
  while($nextBattleId <= $latestBattleId && $running) {
    if(exists $invalidBattles{$nextBattleId}) {
      slog("Skipping invalid battle $nextBattleId",2);
      $nextBattleId++;
      next;
    }
    my $p_bDetails=getBattleDetail($nextBattleId++);
    error("Unable to get battle detail of ".($nextBattleId-1)) unless(%{$p_bDetails});
    if(exists $p_bDetails->{skip}) {
      slog('Skipping battle '.($nextBattleId-1).' (no replay link found)',2);
      next;
    }
    my $recRes=gdrReconciliation($p_bDetails);
    error("Error during GDR reconciliation, status:$recRes") if($recRes == 2 || $recRes == 3);
    if(! (++$nbOfBattlesProcessed % 100) && $nextBattleId <= $latestBattleId) {
      my $nbRemainingBattles=$latestBattleId-$nextBattleId+1;
      my $timeRemaining=secToTime(int($nbRemainingBattles*(time-$startProcessingTs)/$nbOfBattlesProcessed));
      slog("Number of battles processed so far: $nbOfBattlesProcessed (ETA: $timeRemaining)",3);
    }
  }
  slog("Next battle ID: \#$nextBattleId, latest battle ID available: \#$latestBattleId",4);
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
