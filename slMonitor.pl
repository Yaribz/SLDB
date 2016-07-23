#!/usr/bin/perl -w
#
# This file implements the Spring lobby monitoring functionality for SLDB
# (slMonitor), it is part of SLDB.
#
# slMonitor is a Spring lobby bot, it serves 2 main purposes:
# - monitor and store all lobby data (users, battles...) into SLDB in realtime
# - receive, check, and store game data reports (GDR) sent by SPADS into SLDB
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

# Version 0.1 (2016/07/22)

use strict;

use File::Basename qw/dirname fileparse/;
use File::Spec::Functions qw/catdir catfile file_name_is_absolute rel2abs/;
use File::Path;
use IO::Select;
use MIME::Base64;
use Storable qw(thaw);

my ($scriptBaseName,$scriptDir)=fileparse(rel2abs($0),'.pl');
unshift(@INC,$scriptDir);
require SimpleConf;
require SimpleLog;
require Sldb;
require SpringLobbyInterface;

my $confFile=catfile($scriptDir,'etc',"$scriptBaseName.conf");
$confFile=$ARGV[0] if($#ARGV == 0);
if($#ARGV > 0 || ! -f $confFile) {
  print "usage: $0 [<confFile>]\n";
  exit 1;
}

my %conf=(lobbyHost => 'lobby.springrts.com',
          lobbyPort => 8200,
          lobbyLogin => 'SpringLobbyMonitor',
          lobbyPassword => undef,
          lobbyAdminIds => undef,
          logLevel => 4,
          sldbLogLevel => 4,
          lobbyLogLevel => 4,
          logFile => catfile('var','log',"$scriptBaseName.log"),
          logPvDir => catdir('var','log','slMonPv'),
          dbName => 'sldb',
          dbLogin => $scriptBaseName,
          dbPwd => undef,
          minGameLength => 180,
          maxChatMessageLength => 512,
          tsTolerance => 30,
          dynIpThreshold => 30,
          dynIpRange => 512);
SimpleConf::readConf($confFile,\%conf);

my $logFile=$conf{logFile};
$logFile=catfile($scriptDir,$logFile) unless(file_name_is_absolute($logFile));
mkpath(dirname($logFile));

my $logPvDir=$conf{logPvDir};
$logPvDir=catdir($scriptDir,$logPvDir) unless(file_name_is_absolute($logPvDir));
mkpath($logPvDir);

my $dbDs=$conf{dbName};
$dbDs="DBI:mysql:database=$dbDs;host=localhost" unless($dbDs =~ /^DBI:/i);

my @lobbyAdminIds=split(/,/,$conf{lobbyAdminIds});

my $sLog=SimpleLog->new(logFiles => [$logFile,''],
                        logLevels => [$conf{logLevel},3],
                        useANSICodes => [0,1],
                        useTimestamps => [1,1],
                        prefix => "[slMonitor] ");

my $sLogSldb=SimpleLog->new(logFiles => [$logFile,''],
                            logLevels => [$conf{sldbLogLevel},3],
                            useANSICodes => [0,1],
                            useTimestamps => [1,1],
                            prefix => "[SLDB] ");

my $sLogLobby=SimpleLog->new(logFiles => [$logFile],
                             logLevels => [$conf{lobbyLogLevel}],
                             useANSICodes => [0],
                             useTimestamps => [1],
                             prefix => "[SpringLobbyInterface] ");

my $lobby = SpringLobbyInterface->new(serverHost => $conf{lobbyHost},
                                      serverPort => $conf{lobbyPort},
                                      simpleLog => $sLogLobby,
                                      warnForUnhandledMessages => 0);

my %commands=( quit => \&hQuit,
               restart => \&hRestart,
               setloglevel => \&hSetLogLevel,
               status => \&hStatus);

sub slog {
  $sLog->log(@_);
}

sub error {
  my $m=shift;
  slog($m,0);
  exit 1;
}

sub nonFatalError {
  my $m=shift;
  slog($m,1);
}

sub logMsg {
  my ($file,$msg)=@_;
  if(! open(CHAT,">>$logPvDir/$file.log")) {
    slog("Unable to log chat message into file \"$logPvDir/$file.log\"",1);
    return;
  }
  my $dateTime=localtime();
  print CHAT "[$dateTime] $msg\n";
  close(CHAT);
}

sub forkedError {
  my ($msg,$level)=@_;
  slog($msg,$level);
  exit 1;
}

my $stopping=0; # (0:running, 1:quitting, 2:restarting)
my $lobbyState=0; # (0:not_connected, 1:connecting, 2: connected, 3:logged_in, 4:start_data_received)
my %timestamps=(connectAttempt => 0,
                ping => 0);

my %hosts;
my %battles;
my %unmonitoredGames;
my %monitoredBattles;
my %monitoredGames;
my %GDRs;
my $lSock;

my $sldb=Sldb->new({dbDs => $dbDs,
                    dbLogin => $conf{dbLogin},
                    dbPwd => $conf{dbPwd},
                    sLog => $sLogSldb,
                    sqlErrorHandler => \&error});
$sldb->connect();
$sldb->do("set session time_zone = '+0:00'","set UTC timezone to avoid DST problems");

sub cbLobbyConnect {
  $lobbyState=2;
  if($_[4]) {
    slog("Lobby server is running in LAN mode, disconnecting...",2);
    $lobbyState=0;
    $lobby->disconnect();
    return;
  }

  $lobby->addPreCallbacks({REMOVEUSER => \&cbPreRemoveUser,
                           CLIENTSTATUS => \&cbPreClientStatus});

  $lobby->addCallbacks({LOGININFOEND => \&cbLoginInfoEnd,
                        ADDUSER => \&cbAddUser,
                        BATTLEOPENED => \&cbBattleOpened,
                        BATTLECLOSED => \&cbBattleClosed,
                        REMOVEUSER => \&cbRemoveUser,
                        UPDATEBATTLEINFO => \&cbUpdateBattleInfo,
                        CLIENTSTATUS => \&cbClientStatus,
                        JOINEDBATTLE => \&cbJoinedBattle,
                        LEFTBATTLE => \&cbLeftBattle,
                        SAIDPRIVATE => \&cbSaidPrivate,
                        SERVERMSG => \&cbServerMsg,
                        BROADCAST => \&cbBroadcast});

  $lobby->sendCommand(['LOGIN',$conf{lobbyLogin},$lobby->marshallPasswd($conf{lobbyPassword}),0,'*','SpringLobbyMonitor v0.1',0,'a b sp cl'],
                      {ACCEPTED => \&cbLoginAccepted,
                       DENIED => \&cbLoginDenied,
                       AGREEMENTEND => \&cbAgreementEnd},
                      \&cbLoginTimeout);
}

sub cbLobbyDisconnect {
  slog("Disconnected from lobby server (connection reset by peer)",2);
  $lobbyState=0;
  $lobby->disconnect();
  endMonitoring();
}

sub cbConnectTimeout {
  slog("Timeout while connecting to lobby server ($conf{lobbyHost}:$conf{lobbyPort})",2);
  $lobbyState=0;
}

sub cbLoginAccepted {
  $lobbyState=3;
  slog("Logged on lobby server",4);
}

sub cbLoginDenied {
  my (undef,$reason)=@_;
  slog("Login denied on lobby server ($reason)",1);
  $lobbyState=0;
  $lobby->disconnect();
}

sub cbAgreementEnd {
  slog("Spring Lobby agreement has not been accepted for this account yet, please login with a Spring lobby client and accept the agreement",1);
  $stopping=1;
  $lobbyState=0;
  $lobby->disconnect();
}

sub cbLoginTimeout {
  slog("Unable to log on lobby server (timeout)",2);
  $lobbyState=0;
  $lobby->disconnect();
}

sub cbLoginInfoEnd {
  $lobbyState=4;
  if(exists $lobby->{users}->{$conf{lobbyLogin}} && ! $lobby->{users}->{$conf{lobbyLogin}}->{status}->{bot}) {
    slog('The lobby account currently used by slMonitor is not tagged as bot. It is recommended to ask a lobby administrator for bot flag on accounts used by slMonitor',2);
  }
  $lobby->sendCommand(["JOIN",'SLDB']);
}

sub cbAddUser {
  my (undef,$user,$country,$cpu,$id)=@_;
  return if($user eq 'ChanServ');
  if(! defined $country) {
    slog("Received an invalid ADDUSER command from server (country field not provided for user $user)",2);
    $country='??';
  }
  if(! defined $cpu || $cpu !~ /^\d+$/) {
    slog("Received an invalid ADDUSER command from server (cpu field not provided or invalid for user $user)",2);
    $cpu=0;
  }
  if(! defined $id || ! $id || $id !~ /^\d+$/) {
    slog("Received an invalid ADDUSER command from server (accountId field not provided or invalid for user $user)",1);
    return;
  }
  seenUser($user,$id);
  $lobby->sendCommand(['GETUSERID',$user]);
  $lobby->sendCommand(['GETIP',$user]);
  ($user,$country)=$sldb->quote($user,$country);
  $sldb->do("insert into names values ($id,$user,now()) on duplicate key update lastConnection=now()","insert or update names table for account \"$id\" and name \"$user\"");
  $sldb->do("insert into countries values ($id,$country,now()) on duplicate key update lastConnection=now()","insert or update countries table for account \"$id\" and country \"$country\"");
  $sldb->do("insert into cpus values ($id,$cpu,now()) on duplicate key update lastConnection=now()","insert or update cpus table for account \"$id\" and cpu \"$cpu\"");
  $sldb->do("insert into rtPlayers values ($id,$user,0,0,$country,$cpu,0,0,0,0,0)","insert data in rtPlayers on addUser ($user)",\&nonFatalError);
}

sub cbPreRemoveUser {
  my (undef,$user)=@_;
  if(exists $lobby->{users}->{$user}) {
    my $accountId=$lobby->{users}->{$user}->{accountId};
    $sldb->do("delete from rtPlayers where accountId=$accountId","update rtPlayers on removeUser ($user)",\&nonFatalError);
    $sldb->do("delete from rtBattlePlayers where accountId=$accountId","update rtBattlePlayers on removeUser ($user)",\&nonFatalError);
  }else{
    slog("Ignoring invalid REMOVEUSER command (unknown user \"$user\")",2);
  }
}

sub cbRemoveUser {
  my (undef,$user)=@_;
  if(exists $hosts{$user}) {
    my $bId=$hosts{$user};
    slog("REMOVEUSER: Removing data in memory for host \"$user\" <-> battle \"$bId\"",5);
    if(exists $monitoredGames{$bId}) {
      slog("REMOVEUSER: Terminating monitoring of game prematurely [$monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs}]",4);
      $sldb->do("update games set endTimestamp=now(),endCause=1 where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","update games table on REMOVEUSER for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
      delete $monitoredGames{$bId};
    }
    delete $unmonitoredGames{$bId};
    delete $monitoredBattles{$bId};
    delete $battles{$bId};
    delete $hosts{$user};
    $sldb->do("delete from rtBattles where battleId=$bId","update rtBattles on removeUser host ($user)",\&nonFatalError);
    $sldb->do("delete from rtBattlePlayers where battleId=$bId","update rtBattlePlayers on removeUser host ($user)",\&nonFatalError);
  }
}

sub cbBattleOpened {
  my ($bId,$user)=($_[1],$_[4]);
  slog("BATTLEOPENED: Adding data in memory for host \"$user\" <-> battle \"$bId\"",5);
  $hosts{$user}=$bId;
  $battles{$bId}=$user;
  if($lobbyState > 3 && $lobby->{battles}->{$bId}->{type} == 0) {
    if($lobby->{users}->{$user}->{status}->{inGame}) {
      slog("BATTLEOPENED: New battle opened directly in-game, added to unmonitored battles (\"$user\" <-> battle \"$bId\")",3);
      $unmonitoredGames{$bId}=1;
    }else{
      slog("BATTLEOPENED: New battle added to monitored battles",5);
      $monitoredBattles{$bId}=1;
    }
  }
  if(! exists $lobby->{users}->{$user}) {
    slog("Ignoring invalid BATTLEOPENED command (unknown user \"$user\")",2);
    return;
  }
  if(! exists $lobby->{battles}->{$bId}) {
    slog("Ignoring invalid BATTLEOPENED command (unknown battle \"$bId\")",2);
    return;
  }
  my $hostAccountId=$lobby->{users}->{$user}->{accountId};
  my $p_b=$lobby->{battles}->{$bId};
  my ($quotedFounder,$quotedMod,$quotedMap,$quotedDescription,$quotedEngineName,$quotedEngineVersion)=$sldb->quote($user,$p_b->{mod},$p_b->{map},$p_b->{title},$p_b->{engineName},$p_b->{engineVersion});
  $sldb->do("insert into rtBattles values ($bId,$hostAccountId,$quotedFounder,INET_ATON('$p_b->{ip}'),$p_b->{port},$p_b->{type},$p_b->{natType},$p_b->{locked},$p_b->{passworded},$p_b->{rank},$quotedMod,$quotedMap,$p_b->{mapHash},$quotedDescription,$p_b->{maxPlayers},$p_b->{nbSpec},$quotedEngineName,$quotedEngineVersion)","insert new battle in rtBattles on BattleOpened ($bId,$user)",\&nonFatalError);
  $sldb->do("insert into rtBattlePlayers values ($hostAccountId,$bId)","insert host in rtBattlePlayers on BattleOpened ($bId,$user)",\&nonFatalError);
}

sub cbBattleClosed {
  my (undef,$bId)=@_;
  if(! exists $battles{$bId}) {
    slog("Ignoring invalid BATTLECLOSED command (unknown battle \"$bId\")",2);
    return;
  }
  my $user=$battles{$bId};
  slog("BATTLECLOSED: Removing data in memory for host \"$user\" <-> battle \"$bId\"",5);
  if(exists $monitoredGames{$bId}) {
    slog("BATTLECLOSED: Terminating monitoring of game prematurely [$monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs}]",4);
    $sldb->do("update games set endTimestamp=now(),endCause=1 where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","update games table on BATTLECLOSED for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
    delete $monitoredGames{$bId};
  }
  delete $unmonitoredGames{$bId};
  delete $monitoredBattles{$bId};
  delete $battles{$bId};
  delete $hosts{$user};
  $sldb->do("delete from rtBattles where battleId=$bId","delete battle from rtBattles on BattleClosed",\&nonFatalError);
  $sldb->do("delete from rtBattlePlayers where battleId=$bId","delete battle from rtBattlePlayers on BattleClosed",\&nonFatalError);
}

sub cbUpdateBattleInfo {
  my (undef,$bId)=@_;
  if(exists $lobby->{battles}->{$bId}) {
    my $p_b=$lobby->{battles}->{$bId};
    my $quotedMap=$sldb->quote($p_b->{map});
    $sldb->do("update rtBattles set nbSpec=$p_b->{nbSpec},locked=$p_b->{locked},mapName=$quotedMap,mapHash=$p_b->{mapHash} where battleId=$bId","updating rtBattles on UpdateBattleInfo ($bId)",\&nonFatalError);
  }else{
    slog("Ignoring invalid UPDATEBATTLEINFO command (unknown battle \"$bId\")",2);
  }
}

sub cbPreClientStatus {
  my (undef,$user,$newStatus)=@_;
  my $p_newStatus=$lobby->unmarshallClientStatus($newStatus);
  if(! exists $lobby->{users}->{$user}) {
    slog("Ignoring invalid CLIENTSTATUS command (unknown user \"$user\")",2);
    return;
  }
  my $p_currentStatus=$lobby->{users}->{$user}->{status};
  my @sqlUpdates;
  foreach my $statusKey (keys %{$p_newStatus}) {
    push(@sqlUpdates,"$statusKey=$p_newStatus->{$statusKey}") if($p_newStatus->{$statusKey} != $p_currentStatus->{$statusKey});
  }
  if(@sqlUpdates) {
    push(@sqlUpdates,"gameTimestamp=NOW()") if($p_newStatus->{inGame} != $p_currentStatus->{inGame});
    push(@sqlUpdates,"awayTimestamp=NOW()") if($p_newStatus->{away} != $p_currentStatus->{away});
    my $sqlUpdatesString=join(',',@sqlUpdates);
    my $accountId=$lobby->{users}->{$user}->{accountId};
    $sldb->do("update rtPlayers set $sqlUpdatesString where accountId=$accountId","update rtPlayers table on ClientStatus ($user)",\&nonFatalError);
  }
}

sub cbClientStatus {
  my (undef,$user)=@_;
  if(! exists $lobby->{users}->{$user}) {
    slog("Ignoring invalid CLIENTSTATUS command (unknown user \"$user\")",2);
    return;
  }
  return if($user eq 'ChanServ');
  my $p_user=$lobby->{users}->{$user};
  $sldb->do("insert into accounts values ($p_user->{accountId},$p_user->{status}->{rank},$p_user->{status}->{access},$p_user->{status}->{bot},now()) on duplicate key update rank=$p_user->{status}->{rank},admin=$p_user->{status}->{access},bot=$p_user->{status}->{bot},lastUpdate=now()","insert or update accounts table for account \"$p_user->{accountId}\" name \"$user\"");
  if(exists $hosts{$user}) {
    my $bId=$hosts{$user};
    if($lobby->{battles}->{$bId}->{type} == 0) {
      if($lobbyState < 4) {
        if($lobby->{users}->{$user}->{status}->{inGame}) {
          slog("CLIENTSTATUS: Lobby connection init, host in game => adding game to unmonitored games (\"$user\" <-> battle \"$bId\")",5);
          $unmonitoredGames{$bId}=1;
        }else{
          slog("CLIENTSTATUS: Lobby connection init, host not in game => adding battle to monitored battles (\"$user\" <-> battle \"$bId\")",5);
          $monitoredBattles{$bId}=1;
        }
      }else{
        if($lobby->{users}->{$user}->{status}->{inGame}) {
          if(exists $monitoredBattles{$bId}) {
            delete $monitoredBattles{$bId};
            my @userList=@{$lobby->{battles}->{$bId}->{userList}};
            my $nbUsers=$#userList+1;
            my $nbSpecs=$lobby->{battles}->{$bId}->{nbSpec};
            if($nbSpecs > $nbUsers) {
              slog("Got a spec count superior to total number of players for battle hosted by \"$user\"",2);
              $nbSpecs=$nbUsers;
            }
            my $nbPlayers=$nbUsers-$nbSpecs;
            if($nbPlayers > 0) {
              my $timestamp=time;
              $monitoredGames{$bId}={accountId => $lobby->{users}->{$user}->{accountId},
                                     startTs => $timestamp};
              slog("CLIENTSTATUS: A monitored battle went in game, storing game data in database (\"$user\" <-> battle \"$bId\") [$lobby->{users}->{$user}->{accountId},$timestamp]",5);
              my $battleTitle=$lobby->{battles}->{$bId}->{title};
              $battleTitle=$1 if($battleTitle =~ /^Incompatible \(spring [^\)]*\) *(.*)$/);
              my ($quotedUser,$quotedMod,$quotedMap);
              ($battleTitle,$quotedUser,$quotedMod,$quotedMap)=$sldb->quote($battleTitle,$user,$lobby->{battles}->{$bId}->{mod},$lobby->{battles}->{$bId}->{map});
              $sldb->do("insert into games values ($monitoredGames{$bId}->{accountId},FROM_UNIXTIME($timestamp),0,0,$quotedUser,$quotedMod,$quotedMap,$nbSpecs,$nbPlayers,$battleTitle,$lobby->{battles}->{$bId}->{passworded},NULL)","insert into games table for game ($monitoredGames{$bId}->{accountId},$timestamp)");
              foreach my $player (@userList) {
                my $quotedPlayer=$sldb->quote($player);
                $sldb->do("insert into players values ($monitoredGames{$bId}->{accountId},FROM_UNIXTIME($timestamp),$lobby->{users}->{$player}->{accountId},$quotedPlayer)","insert into players table for player ($monitoredGames{$bId}->{accountId},FROM_UNIXTIME($timestamp),$lobby->{users}->{$player}->{accountId})");
              }
            }else{
              slog("CLIENTSTATUS: A monitored battle went in game with no player, adding game to unmonitored games (\"$user\" <-> battle \"$bId\")",5);
              $unmonitoredGames{$bId}=1;
            }
          }
        }else{
          if(exists $unmonitoredGames{$bId}) {
            slog("CLIENTSTATUS: An unmonitored game finished, adding battle to monitored battles (\"$user\" <-> battle \"$bId\")",5);
            delete $unmonitoredGames{$bId};
            $monitoredBattles{$bId}=1;
          }elsif(exists $monitoredGames{$bId}) {
            if(time - $monitoredGames{$bId}->{startTs} < $conf{minGameLength}) {
              slog("CLIENTSTATUS: Discarding game [$monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs}] (game too short), adding battle to monitored battles (\"$user\" <-> battle \"$bId\")",5);
              $sldb->do("delete from games where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","delete from games table on CLIENTSTATUS for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
              $sldb->do("delete from players where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","delete from players table on CLIENTSTATUS for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
            }else{
              slog("CLIENTSTATUS: A monitored game finished [$monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs}], adding battle to monitored battles (\"$user\" <-> battle \"$bId\")",5);
              $sldb->do("update games set endTimestamp=now() where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","update games table on CLIENTSTATUS for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
            }
            delete $monitoredGames{$bId};
            $monitoredBattles{$bId}=1;
          }
        }
      }
    }
  }
}

sub cbJoinedBattle {
  my (undef,$bId,$user)=@_;
  if(! exists $lobby->{users}->{$user}) {
    slog("Ignoring invalid JOINEDBATTLE command (unknown user \"$user\")",2);
    return;
  }
  if(! exists $lobby->{battles}->{$bId}) {
    slog("Ignoring invalid JOINEDBATTLE command (unknown battle \"$bId\")",2);
    return;
  }
  my $accountId=$lobby->{users}->{$user}->{accountId};
  $sldb->do("insert into rtBattlePlayers values ($accountId,$bId)","insert data into rtBattlePlayers on JoinedBattle ($bId,$user)",\&nonFatalError);
}

sub cbLeftBattle {
  my (undef,undef,$user)=@_;
  if(! exists $lobby->{users}->{$user}) {
    slog("Ignoring invalid LEFTBATTLE command (unknown user \"$user\")",2);
    return;
  }
  my $accountId=$lobby->{users}->{$user}->{accountId};
  $sldb->do("delete from rtBattlePlayers where accountId=$accountId","delete data from rtBattlePlayers on LeftBattle ($user)",\&nonFatalError);
}

sub cbSaidPrivate {
  my (undef,$user,$msg)=@_;
  logMsg("pv_$user","<$user> $msg");
  if($msg =~ /^!([\w\#]+)\s*(.*)$/) {
    my ($command,$params)=($1,$2);
    if(exists $GDRs{$user}) {
      if($command ne '#endGDR') {
        $GDRs{$user}->{data}.=$msg;
      }else{
        handleCommand($user,$command,$params);
      }
    }else{
      handleCommand($user,$command,$params);
    }
  }elsif(exists $GDRs{$user}) {
    $GDRs{$user}->{data}.=$msg;
  }
}

sub cbServerMsg {
  my (undef,$msg)=@_;
  slog("SERVER MESSAGE: $msg",5);
  if($msg =~ /^The ID for <([^>]+)> is (\-?\d+)/) {
    my ($user,$hardwareId)=($1,$2);
    if(exists $lobby->{users}->{$user}) {
      seenHardwareId($lobby->{users}->{$user}->{accountId},$hardwareId);
    }else{
      slog("Ignoring hardwareId \"$hardwareId\" received for offline user \"$user\"",4);
    }
  }elsif($msg =~ /^<([^>]+)> is currently bound to (\d+\.\d+\.\d+\.\d+)/) {
    seenLobbyIp($1,$2);
  }
}

sub cbBroadcast {
  my (undef,$msg)=@_;
  print "Lobby broadcast message: $msg\n";
  slog("Lobby broadcast message: $msg",3);
}

sub splitMsg {
  my ($longMsg,$maxSize)=@_;
  my @messages=($longMsg =~ /.{1,$maxSize}/gs);
  return \@messages;
}

sub sayPrivate {
  my ($user,$msg)=@_;
  my $p_messages=splitMsg($msg,$conf{maxChatMessageLength}-1);
  foreach my $mes (@{$p_messages}) {
    $lobby->sendCommand(['SAYPRIVATE',$user,$mes]);
    logMsg("pv_$user","<$conf{lobbyLogin}> $mes");
  }
}

sub getFirstRangeAddr {
  my $ip=shift;
  $ip-=$ip%256;
  return $ip;
}

sub getLastRangeAddr {
  my $ip=shift;
  $ip=$ip-($ip%256)+255;
  return $ip;
}

sub seenUser {
  my ($user,$id)=@_;
  my $sth=$sldb->prepExec("select count(*) from userAccounts where accountId=$id","check if id \"$id\" is already known in userAccounts!");
  my @uaCount=$sth->fetchrow_array();
  return if($uaCount[0] > 0);

  my ($name,$clan)=('','');
  if($user=~/^\[([^\]]+)\](.+)$/) {
    ($clan,$name)=($1,$2);
  }else{
    $name=$user;
  }
  $clan=$sldb->quote($clan);
  my $quotedName=$sldb->quote($sldb->findAvailableUserName($name));

  $sldb->do("insert into userAccounts values ($id,$id,0,0)","insert data in table userAccounts");
  $sldb->do("insert into userDetails values ($id,$quotedName,$clan,NULL,NULL,0)","insert data in table userDetails");
}

sub initializeUserTablesIfNeeded {
  my $sth=$sldb->prepExec("select count(*) from userAccounts","check userAccounts state in database!");
  my @uaCount=$sth->fetchrow_array();
  return if($uaCount[0] > 0);
  slog("Initializing \"userAccounts\" and \"userDetails\" tables from \"names\" table",3);
  $sth=$sldb->prepExec("select accountId,name from names group by accountId","read \"names\" table for user tables initialization!");
  my ($id,$name);
  while(($id,$name)=$sth->fetchrow_array()) {
    seenUser($name,$id);
  }
  slog("User tables initialization done.",3);
}

sub seenHardwareId {
  my ($accountId,$hardwareId)=@_;
  $sldb->do("insert into hardwareIds values ($accountId,$hardwareId,now()) on duplicate key update lastConnection=now()","insert or update hardwareIds table for account \"$accountId\" and hardwareId \"$hardwareId\"");
  return unless($hardwareId);
  return unless($lobby->{users}->{$lobby->{accounts}->{$accountId}}->{status}->{rank} == 0);
  my $smurfId=$sldb->getUserId($accountId);
  return unless($smurfId == $accountId);
  my $sth=$sldb->prepExec("select distinct ua.userId from userAccounts ua,hardwareIds hw where ua.userId != $smurfId and ua.accountId=hw.accountId and hw.hardwareId=$hardwareId limit 2","retrieve users with accounts matching hardwareId \"$hardwareId\" from userAccounts and hardwareIds");
  my $p_results=$sth->fetchall_arrayref();
  return unless(@{$p_results});
  if($#{$p_results} > 0) {
    slog("HardwareId of newbie account \"$lobby->{accounts}->{$accountId}\" matches multiple users, cancelling auto-merge",4);
    return;
  }
  my $userId=$p_results->[0]->[0];
  my $mergeStatus=checkUserMerge($userId,$smurfId,1);
  return unless($mergeStatus);
  slog("Detected smurf for user \#$userId: \#$smurfId ($lobby->{accounts}->{$accountId}), merging users...",3);
  $sldb->adminEvent('JOIN_ACC',$mergeStatus,0,0,{mainUserId => $userId, childUserId => $smurfId});
  $sth=$sldb->prepExec("select accountId from userAccounts where userId=$smurfId","retrieve smurfs list of user $smurfId that will be joined with user $userId");
  my $p_oldUserSmurfs=$sth->fetchall_arrayref();
  foreach my $p_accountId (@{$p_oldUserSmurfs}) {
    $sldb->queueGlobalRerate($p_accountId->[0]);
  }
  $sldb->do("update userAccounts set userId=$userId where userId=$smurfId","update data in table userAccounts for new smurf found \"$smurfId\" of \"$userId\"");
  $sldb->computeAllUserIps($userId,$conf{dynIpThreshold},$conf{dynIpRange});
}

sub seenLobbyIp {
  my ($user,$ip)=@_;
  return unless(exists $lobby->{users}->{$user});
  my $id=$lobby->{users}->{$user}->{accountId};

  my $ipNb;
  if($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/) {
    $ipNb=$sldb->ipToInt($ip);
    if(! $ipNb) {
      slog("Unable to convert ip to number in seenLobbyIp ($ip)!",2);
      return 0;
    }
  }elsif($ip =~ /^\d+$/) {
    $ipNb=$ip;
  }else{
    slog("Invalid ip in seenLobbyIp ($ip)!",2);
    return 0;
  }

  my $sth=$sldb->prepExec("select bot,noSmurf from accounts,userAccounts where id=$id and accountId=$id","check bot and noSmurf flags for account \"$id\" in tables accounts and userAccounts!");
  my @foundData=$sth->fetchrow_array();
  if($foundData[0] || $foundData[1]) {
    slog("Skipping smurf detection from lobby IP for account \"$id\" (bot=$foundData[0], noSmurf=$foundData[1])",5);
    return 1;
  }
  $sth=$sldb->prepExec("select userId from userAccounts where accountId=$id","query userAccounts for user id of account \"$id\"!");
  @foundData=$sth->fetchrow_array();
  error("Unable to find user id of account \"$id\" in userAccounts table!") unless(@foundData);
  my $userId=$foundData[0];
  
  $sth=$sldb->prepExec("select rank from accounts where id=$userId","retrieve rank of account \"$userId\"");
  @foundData=$sth->fetchrow_array();
  error("Unable to find rank of user \"$userId\" in accounts table!") unless(@foundData);
  my $userRank=$foundData[0];

  # Smurf detection (search of static IPs matching one IP)
  $sth=$sldb->prepExec("select userId,max(rank) maxRank from accounts,userAccounts,ips where id=userAccounts.accountId and id=ips.accountId and bot=0 and noSmurf=0 and userId != $userId and ip=$ipNb group by userId order by maxRank desc","search smurfs for ip $ipNb of user \"$userId\" in ips table");
  my $p_newSmurfData=$sth->fetchall_arrayref();
  foreach my $p_possibleSmurfUser (@{$p_newSmurfData}) {
    my ($smurfId,$smurfRank)=($p_possibleSmurfUser->[0],$p_possibleSmurfUser->[1]);
    my $mergeStatus=checkUserMerge($userId,$smurfId,1);
    next unless($mergeStatus);
    my ($oldUserId,$newUserId);
    if($smurfRank < $userRank || ($smurfRank == $userRank && $userId < $smurfId)) {
      ($oldUserId,$newUserId)=($smurfId,$userId);
    }else{
      ($oldUserId,$newUserId)=($userId,$smurfId);
      ($userId,$userRank)=($smurfId,$smurfRank);
    }
    slog("Merging $oldUserId into $newUserId based on lobby IP!",3);
    $sldb->adminEvent('JOIN_ACC',$mergeStatus,0,0,{mainUserId => $newUserId, childUserId => $oldUserId});
    $sth=$sldb->prepExec("select accountId from userAccounts where userId=$oldUserId","retrieve smurfs list of user $oldUserId that will be joined with user $newUserId");
    my $p_oldUserSmurfs=$sth->fetchall_arrayref();
    foreach my $p_accountId (@{$p_oldUserSmurfs}) {
      $sldb->queueGlobalRerate($p_accountId->[0]);
    }
    $sldb->do("update userAccounts set userId=$newUserId where userId=$oldUserId","update data in table userAccounts for new smurf found \"$oldUserId\" of \"$newUserId\"");
    $sldb->computeAllUserIps($newUserId,$conf{dynIpThreshold},$conf{dynIpRange});
  }
}

sub seenIp {
  my ($id,$ip,$date)=@_;

  if(defined $date) {
    $date="FROM_UNIXTIME($date)";
  }else{
    $date='now()';
  }

  my $ipNb;
  if($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/) {
    $ipNb=$sldb->ipToInt($ip);
    if(! $ipNb) {
      slog("Unable to convert ip to number ($ip)!",2);
      return 0;
    }
  }elsif($ip =~ /^\d+$/) {
    $ipNb=$ip;
  }

  return 0 if($sldb->isReservedIpNb($ipNb));

  my $sth=$sldb->prepExec("select ua.userId,ua.nbIps,ud.nbIps from userAccounts ua,userDetails ud where ua.accountId=$id and ua.userId=ud.userId","query id \"$id\" in userAccounts and userDetails");
  my @foundData=$sth->fetchrow_array();
  if(! @foundData) {
    slog("Unable to retrieve data for id \"$id\" in userAccounts and userDetails, ignoring ip \"$ip\"!",2);
    return 0;
  }
  my ($userId,$nbIps,$nbUserIps)=@foundData;

  my @ipCount;
  if($date eq 'now()') {
    $sth=$sldb->prepExec("select count(*) from userIps where userId=$userId and ip=$ipNb","check if ip \"$ip\" for userId \"$userId\" is already known in userIps table");
    @ipCount=$sth->fetchrow_array();
    if($ipCount[0] > 0) {
      $sldb->do("update userIps set lastSeen=$date where userId=$userId and ip=$ipNb","update ip last seen date in table userIps");
    }else{
      if($nbUserIps < $conf{dynIpThreshold}) {
        $sldb->do("insert into userIps values ($userId,$ipNb,$date)","insert new ip \"$ip\" for userId \"$userId\" in table userIps");
        $sldb->do("update userDetails set nbIps=nbIps+1 where userId=$userId","increment nbIps for userId \"$userId\" in table userDetails");
      }
      if($nbUserIps == $conf{dynIpThreshold}-1) {
        slog("User \#$userId has been detected as using dynamic IP, starting dynamic IP ranges detection...",3);
        $sldb->computeAllUserIps($userId,$conf{dynIpThreshold},$conf{dynIpRange});
      }elsif($nbUserIps > $conf{dynIpThreshold}-1) {
        $sth=$sldb->prepExec("select count(*) from userIpRanges where userId=$userId and ip1 <= $ipNb and $ipNb <= ip2","check if ip \"$ip\" for userId \"$userId\" is already known in userIpRanges table");
        @ipCount=$sth->fetchrow_array();
        if($ipCount[0] > 0) {
          $sldb->do("update userIpRanges set lastSeen=$date where userId=$userId and ip1 <= $ipNb and $ipNb <= ip2","update ip last seen date in table userIpRanges");
        }else{
          $sth=$sldb->prepExec("select ip from userIps where userId=$userId and ABS(cast(ip as signed) - $ipNb) <= $conf{dynIpRange}","query userIps table for neighbour ips of \"$ip\" for userId \"$userId\"");
          my @neighbourIps;
          while(@foundData=$sth->fetchrow_array()) {
            push(@neighbourIps,$foundData[0]);
          }
          my @sortedIps=sort {$a <=> $b} (@neighbourIps,$ipNb);
          my ($rangeStart,$rangeEnd)=($sortedIps[0],$sortedIps[$#sortedIps]);
          if($rangeStart<$rangeEnd) {
            $rangeStart=getFirstRangeAddr($rangeStart);
            $rangeEnd=getLastRangeAddr($rangeEnd);
          }
          $sth=$sldb->prepExec("select ip1,ip2 from userIpRanges where userId=$userId and (($rangeStart >= ip1 and $rangeStart <= ip2+$conf{dynIpRange}) or ($rangeEnd >= ip1-$conf{dynIpRange} and $rangeEnd <= ip2) or ($rangeStart < ip1 and $rangeEnd > ip2))","query userIpRanges table for neighbour ranges of userId \"$userId\"");
          my @neighbourRanges;
          while(@foundData=$sth->fetchrow_array()) {
            push(@neighbourRanges,$foundData[0]);
            $rangeStart=$foundData[0] if($foundData[0]<$rangeStart);
            $rangeEnd=$foundData[1] if($foundData[1]>$rangeEnd);
          }
          if(@neighbourIps) {
            my $neighbourIpsString=join(',',@neighbourIps);
            $sldb->do("delete from userIps where userId=$userId and ip in ($neighbourIpsString)","remove aggregated ip addresses for userId \"$userId\" in table userIps during dynamic ip ranges detection");
          }
          if(@neighbourRanges) {
            my $neighbourRangesString=join(',',@neighbourRanges);
            $sldb->do("delete from userIpRanges where userId=$userId and ip1 in ($neighbourRangesString)","remove aggregated ip ranges for userId \"$userId\" in table userIpRanges during dynamic ip ranges detection");
          }
          if($rangeStart<$rangeEnd) {
            $rangeStart=getFirstRangeAddr($rangeStart);
            $rangeEnd=getLastRangeAddr($rangeEnd);
            $sldb->do("insert into userIpRanges values ($userId,$rangeStart,$rangeEnd,$date)","insert aggregated range for userId \"$userId\" in table userIpRanges during dynamic ip ranges detection");
          }else{
            $sldb->do("insert into userIps values ($userId,$ipNb,$date)","insert new ip \"$ip\" for userId \"$userId\" in table userIps");
          }
        }
      }
    }
  }

  $sth=$sldb->prepExec("select count(*) from ips where accountId=$id and ip=$ipNb","check if ip \"$ip\" for id \"$id\" is already known in ips table!");
  @ipCount=$sth->fetchrow_array();
  if($ipCount[0] > 0) {
    $sldb->do("update ips set lastSeen=$date where accountId=$id and ip=$ipNb","update ip last seen date in table ips");
    return 1;
  }

  if($nbIps < $conf{dynIpThreshold}-1) {
    $sldb->do("insert into ips values ($id,$ipNb,$date)","insert new ip \"$ip\" for id \"$id\" in table ips");
    $sldb->do("update userAccounts set nbIps=nbIps+1 where accountId=$id","increment nbIps for account \"$id\" in table userAccounts");
    checkSmurf($id,$ipNb);
  }else{

    if($nbIps == $conf{dynIpThreshold}-1) {
      slog("Account \#$id has been detected as using dynamic IP, starting dynamic IP ranges detection...",3);

      $sth=$sldb->prepExec("select ip,UNIX_TIMESTAMP(lastSeen) from ips where accountId=$id","retrieve known ips for id \"$id\" in ips table for dynamic ip ranges detection!");
      my (@knownIp,%knownIps);
      while(@knownIp=$sth->fetchrow_array()) {
        $knownIps{$knownIp[0]}=$knownIp[1];
      }
      my (@isolatedIps,@ranges);
      my ($rangeStart,$rangeEnd,$rangeTs)=(0,0,0);
      foreach my $testedIp (sort {$a <=> $b} keys %knownIps) {
        if($rangeEnd) {
          if(getFirstRangeAddr($testedIp)-$rangeEnd <= $conf{dynIpRange}) {
            $rangeEnd=getLastRangeAddr($testedIp);
            $rangeTs=$knownIps{$testedIp} if($knownIps{$testedIp} > $rangeTs);
          }else{
            push(@ranges,[$rangeStart,$rangeEnd,$rangeTs]);
            ($rangeStart,$rangeEnd,$rangeTs)=($testedIp,0,$knownIps{$testedIp});
          }
        }elsif($rangeStart) {
          if(getFirstRangeAddr($testedIp)-$rangeStart <= $conf{dynIpRange}) {
            $rangeStart=getFirstRangeAddr($rangeStart);
            $rangeEnd=getLastRangeAddr($testedIp);
            $rangeTs=$knownIps{$testedIp} if($knownIps{$testedIp} > $rangeTs);
          }else{
            push(@isolatedIps,$rangeStart);
            ($rangeStart,$rangeTs)=($testedIp,$knownIps{$testedIp});
          }
        }else{
          ($rangeStart,$rangeTs)=($testedIp,$knownIps{$testedIp});
        }
      }
      if($rangeEnd) {
        push(@ranges,[$rangeStart,$rangeEnd,$rangeTs]);
      }elsif($rangeStart) {
        push(@isolatedIps,$rangeStart);
      }else{
        error("Inconsistent state while processing dynamic ip ranges detection");
      }
      
      $sldb->do("delete from ips where accountId=$id","flush ip addresses for account \"$id\" in table ips for dynamic ip ranges detection");
      foreach my $isolatedIp (@isolatedIps) {
        $sldb->do("insert into ips values ($id,$isolatedIp,FROM_UNIXTIME($knownIps{$isolatedIp}))","reinsert ip addresses for account \"$id\" in table ips for dynamic ip ranges detection");
      }
      foreach my $p_range (@ranges) {
        $sldb->do("insert into ipRanges values ($id,$p_range->[0],$p_range->[1],FROM_UNIXTIME($p_range->[2]))","insert ip ranges for account \"$id\" in table ipRanges for dynamic ip ranges detection");
      }
      
      my $nbRangesFound=$#ranges+1;
      if($nbRangesFound) {
        slog($nbRangesFound." dynamic IP range(s) found.",4);
      }else{
        slog("No dynamic IP range found.",4);
      }

      $sldb->do("update userAccounts set nbIps=$conf{dynIpThreshold} where accountId=$id","set nbIps to $conf{dynIpThreshold} for account \"$id\" in table userAccounts");
    }

    $sth=$sldb->prepExec("select count(*) from ipRanges where accountId=$id and $ipNb >= ip1 and $ipNb <= ip2","check if ip \"$ip\" for id \"$id\" is already known in ipRanges table!");
    @ipCount=$sth->fetchrow_array();
    if($ipCount[0] > 0) {
      $sldb->do("update ipRanges set lastSeen=$date where accountId=$id and $ipNb >= ip1 and $ipNb <= ip2","update ip last seen date in table ips");
      return 1;
    }

    $sth=$sldb->prepExec("select ip from ips where accountId=$id and ABS(cast(ip as signed) - $ipNb) <= $conf{dynIpRange}","query ips table for neighbour ips of \"$ip\" for id \"$id\"!");
    my @neighbourIps;
    while(@foundData=$sth->fetchrow_array()) {
      push(@neighbourIps,$foundData[0]);
    }
    my @sortedIps=sort {$a <=> $b} (@neighbourIps,$ipNb);
    my ($rangeStart,$rangeEnd)=($sortedIps[0],$sortedIps[$#sortedIps]);
    if($rangeStart<$rangeEnd) {
      $rangeStart=getFirstRangeAddr($rangeStart);
      $rangeEnd=getLastRangeAddr($rangeEnd);
    }
    
    $sth=$sldb->prepExec("select ip1,ip2 from ipRanges where accountId=$id and (($rangeStart >= ip1 and $rangeStart <= ip2+$conf{dynIpRange}) or ($rangeEnd >= ip1-$conf{dynIpRange} and $rangeEnd <= ip2) or ($rangeStart < ip1 and $rangeEnd > ip2))","query ipRanges table for neighbour ranges of id \"$id\"!");
    my @neighbourRanges;
    while(@foundData=$sth->fetchrow_array()) {
      push(@neighbourRanges,$foundData[0]);
      $rangeStart=$foundData[0] if($foundData[0]<$rangeStart);
      $rangeEnd=$foundData[1] if($foundData[1]>$rangeEnd);
    }
    if(@neighbourIps) {
      my $neighbourIpsString=join(',',@neighbourIps);
      $sldb->do("delete from ips where accountId=$id and ip in ($neighbourIpsString)","remove aggregated ip addresses for account \"$id\" in table ips during dynamic ip ranges detection");
    }
    if(@neighbourRanges) {
      my $neighbourRangesString=join(',',@neighbourRanges);
      $sldb->do("delete from ipRanges where accountId=$id and ip1 in ($neighbourRangesString)","remove aggregated ip ranges for account \"$id\" in table ipRanges during dynamic ip ranges detection");
    }
    if($rangeStart<$rangeEnd) {
      $rangeStart=getFirstRangeAddr($rangeStart);
      $rangeEnd=getLastRangeAddr($rangeEnd);
      $sldb->do("insert into ipRanges values ($id,$rangeStart,$rangeEnd,$date)","insert aggregated range for account \"$id\" in table ipRanges during dynamic ip ranges detection");
      checkSmurf($id,$rangeStart,$rangeEnd);
    }else{
      $sldb->do("insert into ips values ($id,$ipNb,$date)","insert new ip \"$ip\" for id \"$id\" in table ips");
      checkSmurf($id,$ipNb);
    }
  }
  
}

sub checkSmurf {
  my ($id,$ipNbParam,$ipNbParam2)=@_;
  my $paramsString=join(',',@_);
  my $functionStartTime=time;
  my $sth=$sldb->prepExec("select bot,noSmurf from accounts,userAccounts where id=$id and accountId=$id","check bot and noSmurf flags for account \"$id\" in tables accounts and userAccounts!");
  my @foundData=$sth->fetchrow_array();
  if($foundData[0] || $foundData[1]) {
    slog("Skipping smurf detection for account \"$id\" (bot=$foundData[0], noSmurf=$foundData[1])",4);
    return 1;
  }
  $sth=$sldb->prepExec("select userId from userAccounts where accountId=$id","query userAccounts for user id of account \"$id\"!");
  @foundData=$sth->fetchrow_array();
  error("Unable to find user id of account \"$id\" in userAccounts table!") unless(@foundData);
  my $userId=$foundData[0];
  
  $sth=$sldb->prepExec("select rank from accounts where id=$userId","retrieve rank of account \"$userId\"");
  @foundData=$sth->fetchrow_array();
  error("Unable to find rank of user \"$userId\" in accounts table!") unless(@foundData);
  my $userRank=$foundData[0];

  my @ipDataToCheck;
  if(defined $ipNbParam) {
    if(defined $ipNbParam2) {
      @ipDataToCheck=([$ipNbParam,$ipNbParam2]);
    }else{
      @ipDataToCheck=([$ipNbParam]);
    }
  }else{
    $sth=$sldb->prepExec("select distinct(ip) from ips where accountId in (select accountId from userAccounts where userId=$userId)","retrieve all known ips for user $userId");
    while(@foundData=$sth->fetchrow_array()) {
      push(@ipDataToCheck,[$foundData[0]]);
    }
    $sth=$sldb->prepExec("select ip1,ip2 from ipRanges where accountId in (select accountId from userAccounts where userId=$userId) group by concat(ip1,ip2)","retrieve all known ip ranges for user $userId");
    while(@foundData=$sth->fetchrow_array()) {
      push(@ipDataToCheck,[$foundData[0],$foundData[1]]);
    }
  }

  foreach my $p_ipData (@ipDataToCheck) {
    my ($ipNb,$ipNb2)=@{$p_ipData};

    if(! defined $ipNb2) {

      # Smurf detection (search of static IPs matching one IP)
      $sth=$sldb->prepExec("select userId,max(rank) maxRank from accounts,userAccounts,ips where id=userAccounts.accountId and id=ips.accountId and bot=0 and noSmurf=0 and userId != $userId and ip=$ipNb group by userId order by maxRank desc","search smurfs for ip $ipNb of user \"$userId\" in ips table");
      my $p_newSmurfData=$sth->fetchall_arrayref();
      foreach my $p_possibleSmurfUser (@{$p_newSmurfData}) {
        my ($smurfId,$smurfRank)=($p_possibleSmurfUser->[0],$p_possibleSmurfUser->[1]);
        my $mergeStatus=checkUserMerge($userId,$smurfId,1);
        next unless($mergeStatus);
        my ($oldUserId,$newUserId);
        if($smurfRank < $userRank || ($smurfRank == $userRank && $userId < $smurfId)) {
          ($oldUserId,$newUserId)=($smurfId,$userId);
        }else{
          ($oldUserId,$newUserId)=($userId,$smurfId);
          ($userId,$userRank)=($smurfId,$smurfRank);
        }
        $sldb->adminEvent('JOIN_ACC',$mergeStatus,0,0,{mainUserId => $newUserId, childUserId => $oldUserId});
        $sth=$sldb->prepExec("select accountId from userAccounts where userId=$oldUserId","retrieve smurfs list of user $oldUserId that will be joined with user $newUserId");
        my $p_oldUserSmurfs=$sth->fetchall_arrayref();
        foreach my $p_accountId (@{$p_oldUserSmurfs}) {
          $sldb->queueGlobalRerate($p_accountId->[0]);
        }
        $sldb->do("update userAccounts set userId=$newUserId where userId=$oldUserId","update data in table userAccounts for new smurf found \"$oldUserId\" of \"$newUserId\"");
        $sldb->computeAllUserIps($newUserId,$conf{dynIpThreshold},$conf{dynIpRange});
      }
      
      # Probable smurf detection (search of dynamic IP ranges matching one IP)
      $sth=$sldb->prepExec("select id,userId from accounts,userAccounts,ipRanges where id=userAccounts.accountId and id=ipRanges.accountId and bot=0 and noSmurf=0 and userId != $userId and ip1 <= $ipNb and ip2 >= $ipNb group by concat(id,userId)","search probable smurfs for ip $ipNb of account $id in ipRanges table");
      my $p_newProbableSmurfsData=$sth->fetchall_arrayref();
      my %newProbableSmurfs;
      foreach my $p_probableSmurfUser (@{$p_newProbableSmurfsData}) {
        my ($probSmurfAccId,$probSmurfUserId)=($p_probableSmurfUser->[0],$p_probableSmurfUser->[1]);
        next unless(checkUserMerge($userId,$probSmurfUserId) == 1);
        if(exists $newProbableSmurfs{$probSmurfUserId}) {
          $newProbableSmurfs{$probSmurfUserId}->{$probSmurfAccId}=1;
        }else{
          $newProbableSmurfs{$probSmurfUserId}={$probSmurfAccId => 1};
        }
      }
      foreach my $probSmurfUserId (keys %newProbableSmurfs) {
        my $bestId=$probSmurfUserId;
        if(! exists $newProbableSmurfs{$probSmurfUserId}->{$probSmurfUserId}) {
          my @probSmurfUserIds=sort {$a <=> $b} (keys %{$newProbableSmurfs{$probSmurfUserId}});
          $bestId=$probSmurfUserIds[0];
        }
        my ($id1,$id2)= $bestId < $id ? ($bestId,$id) : ($id,$bestId);
        $sldb->adminEvent('ADD_PROB_SMURF',0,0,0,{accountId1 => $id1, accountId2 => $id2});
        $sldb->do("insert into smurfs values ($id1,$id2,2,0)","add probable smurf \"$id1\" <-> \"$id2\" into smurfs table");
      }

    }else{

      # Probable smurf detection (search of static IPs matching a dynamic IP range)
      $sth=$sldb->prepExec("select id,userId from accounts,userAccounts,ips where id=userAccounts.accountId and id=ips.accountId and bot=0 and noSmurf=0 and userId != $userId and ip >= $ipNb and ip <= $ipNb2 group by concat(id,userId)","search probable smurfs for ip range ($ipNb,$ipNb2) of account $id in ips table");
      my $p_newProbableSmurfsData=$sth->fetchall_arrayref();
      my %newProbableSmurfs;
      foreach my $p_probableSmurfUser (@{$p_newProbableSmurfsData}) {
        my ($probSmurfAccId,$probSmurfUserId)=($p_probableSmurfUser->[0],$p_probableSmurfUser->[1]);
        next unless(checkUserMerge($userId,$probSmurfUserId) == 1);
        if(exists $newProbableSmurfs{$probSmurfUserId}) {
          $newProbableSmurfs{$probSmurfUserId}->{$probSmurfAccId}=1;
        }else{
          $newProbableSmurfs{$probSmurfUserId}={$probSmurfAccId => 1};
        }
      }

      # Probable smurf detection (search of dynamic IP ranges matching a dynamic IP range)
      $sth=$sldb->prepExec("select id,userId from accounts,userAccounts,ipRanges where id=userAccounts.accountId and id=ipRanges.accountId and bot=0 and noSmurf=0 and userId != $userId and (($ipNb >= ip1 and $ipNb <= ip2+$conf{dynIpRange}) or ($ipNb2 >= ip1-$conf{dynIpRange} and $ipNb2 <= ip2) or ($ipNb < ip1 and $ipNb2 > ip2)) group by concat(id,userId)","search probable smurfs for ip range ($ipNb,$ipNb2) of account $id in ipRanges table");
      $p_newProbableSmurfsData=$sth->fetchall_arrayref();
      foreach my $p_probableSmurfUser (@{$p_newProbableSmurfsData}) {
        my ($probSmurfAccId,$probSmurfUserId)=($p_probableSmurfUser->[0],$p_probableSmurfUser->[1]);
        next if(exists $newProbableSmurfs{$probSmurfUserId} && exists $newProbableSmurfs{$probSmurfUserId}->{$probSmurfAccId});
        next unless(checkUserMerge($userId,$probSmurfUserId) == 1);
        if(exists $newProbableSmurfs{$probSmurfUserId}) {
          $newProbableSmurfs{$probSmurfUserId}->{$probSmurfAccId}=1;
        }else{
          $newProbableSmurfs{$probSmurfUserId}={$probSmurfAccId => 1};
        }
      }

      foreach my $probSmurfUserId (keys %newProbableSmurfs) {
        my $bestId=$probSmurfUserId;
        if(! exists $newProbableSmurfs{$probSmurfUserId}->{$probSmurfUserId}) {
          my @probSmurfUserIds=sort {$a <=> $b} (keys %{$newProbableSmurfs{$probSmurfUserId}});
          $bestId=$probSmurfUserIds[0];
        }
        my ($id1,$id2)= $bestId < $id ? ($bestId,$id) : ($id,$bestId);
        $sldb->adminEvent('ADD_PROB_SMURF',0,0,0,{accountId1 => $id1, accountId2 => $id2});
        $sldb->do("insert into smurfs values ($id1,$id2,2,0)","add probable smurf \"$id1\" <-> \"$id2\" into smurfs table");
      }
    }

  }
  my $elapsedTime=time-$functionStartTime;
  slog("checkSmurf($paramsString) call lasted $elapsedTime seconds",2) if($elapsedTime > 1);
}

# Check smurfs data don't prevent the merge, then remove all matching probable smurfs data to prepare merge if 3rd parameter is true
sub checkUserMerge {
  my ($u1,$u2,$removeProbableSmurfs)=@_;
  $removeProbableSmurfs=0 unless(defined $removeProbableSmurfs);
  my @accountsUser1;
  my $sth=$sldb->prepExec("select accountId from userAccounts where userId=$u1","retrieve accounts of user \"$u1\"");
  my $p_foundData=$sth->fetchall_arrayref();
  error("checkUserMerge($u1,$u2): unable to retrieve accounts of user \"$u1\"") unless(@{$p_foundData});
  foreach my $p_acc (@{$p_foundData}) {
    push(@accountsUser1,$p_acc->[0]);
  }
  my @accountsUser2;
  $sth=$sldb->prepExec("select accountId from userAccounts where userId=$u2","retrieve accounts of user \"$u2\"");
  $p_foundData=$sth->fetchall_arrayref();
  error("checkUserMerge($u1,$u2): unable to retrieve accounts of user \"$u2\"") unless(@{$p_foundData});
  foreach my $p_acc (@{$p_foundData}) {
    push(@accountsUser2,$p_acc->[0]);
  }
  my $user1AccountsString=join(',',@accountsUser1);
  my $user2AccountsString=join(',',@accountsUser2);
  $sth=$sldb->prepExec("select distinct(status) from smurfs where ((id1 in ($user1AccountsString) and id2 in ($user2AccountsString)) or (id1 in ($user2AccountsString) and id2 in ($user1AccountsString)))","check smurfs state of users \"$u1\" ($user1AccountsString) and \"$u2\" ($user2AccountsString) in smurfs table");
  my %statusFound;
  my @foundData;
  while(@foundData=$sth->fetchrow_array()) {
    $statusFound{$foundData[0]}=1;
  }
  error("smurfs table in inconsistent state when preparing user merge for \"$u1\" ($user1AccountsString) and \"$u2\" ($user2AccountsString): found entries with status 1") if(exists $statusFound{1});
  return 0 if(exists $statusFound{0});
  $sth=$sldb->prepExec("select pd1.accountId,pd2.accountId from playersDetails pd1,playersDetails pd2 where pd1.gameId=pd2.gameId and pd1.accountId in ($user1AccountsString) and pd2.accountId in ($user2AccountsString) and pd1.team is not null and pd1.ip is not null and pd1.ip != 0 and pd2.team is not null and pd2.ip is not null and pd2.ip != 0","check if users $u1 and $u2 have already played in same game in playersDetails table");
  my %simultaneousPlays;
  while(@foundData=$sth->fetchrow_array()) {
    if(exists $simultaneousPlays{$foundData[0]}) {
      $simultaneousPlays{$foundData[0]}->{$foundData[1]}=1;
    }else{
      $simultaneousPlays{$foundData[0]}={$foundData[1] => 1};
    }
    if(exists $simultaneousPlays{$foundData[1]}) {
      $simultaneousPlays{$foundData[1]}->{$foundData[0]}=1;
    }else{
      $simultaneousPlays{$foundData[1]}={$foundData[0] => 1};
    }
  }
  if(exists $statusFound{2} && ($removeProbableSmurfs || %simultaneousPlays)) {
    my $eventSubType=0;
    $eventSubType=2 if(%simultaneousPlays);
    $sth=$sldb->prepExec("select id1,id2 from smurfs where (id1 in ($user1AccountsString) and id2 in ($user2AccountsString)) or (id1 in ($user2AccountsString) and id2 in ($user1AccountsString))","query ids from smurfs table during user merge check ($u1 <-> $u2)");
    while(@foundData=$sth->fetchrow_array()) {
      $sldb->adminEvent('DEL_PROB_SMURF',$eventSubType,0,0,{accountId1 => $foundData[0], accountId2 => $foundData[1]});
    }
    $sldb->do("delete from smurfs where (id1 in ($user1AccountsString) and id2 in ($user2AccountsString)) or (id1 in ($user2AccountsString) and id2 in ($user1AccountsString))","remove probable smurf entries between users ($u1 <-> $u2)");
  }
  if(%simultaneousPlays) {
    my ($firstUser,$secondUser) = $u1 < $u2 ? ($u1,$u2) : ($u2,$u1);
    my ($notSmurfId1,$notSmurfId2);
    if(exists $simultaneousPlays{$u1} && exists $simultaneousPlays{$u1}->{$u2}) {
      ($notSmurfId1,$notSmurfId2)=($firstUser,$secondUser);
    }elsif(exists $simultaneousPlays{$firstUser}) {
      my @secondUserAccounts = sort {$a <=> $b} (keys %{$simultaneousPlays{$firstUser}});
      ($notSmurfId1,$notSmurfId2) = $firstUser < $secondUserAccounts[0] ? ($firstUser,$secondUserAccounts[0]) : ($secondUserAccounts[0],$firstUser);
    }elsif(exists $simultaneousPlays{$secondUser}) {
      my @firstUserAccounts = sort {$a <=> $b} (keys %{$simultaneousPlays{$secondUser}});
      ($notSmurfId1,$notSmurfId2) = $secondUser < $firstUserAccounts[0] ? ($secondUser,$firstUserAccounts[0]) : ($firstUserAccounts[0],$secondUser);
    }else{
      my @firstAccounts=sort {$a <=> $b} (keys %simultaneousPlays);
      $notSmurfId1=$firstAccounts[0];
      my @secondAccounts=sort {$a <=> $b} (keys %{$simultaneousPlays{$notSmurfId1}});
      $notSmurfId2=$secondAccounts[0];
    }
    $sldb->adminEvent('ADD_NOT_SMURF',2,0,0,{accountId1 => $notSmurfId1, accountId2 => $notSmurfId2});
    $sldb->do("insert into smurfs values ($notSmurfId1,$notSmurfId2,0,0)","add not-smurf entry \"$notSmurfId1\" <-> \"$notSmurfId2\" into smurfs table");
    return 0;
  }
  return 2 if(exists $statusFound{2});
  return 1;
  
}

sub checkNonSmurfs {
  my $p_nonSmurfs=shift;
  my $functionStartTime=time;
  my @orderedNonSmurfs=sort {$a <=> $b} @{$p_nonSmurfs};
  my $nonSmurfsString=join(',',@orderedNonSmurfs);

  my $sth=$sldb->prepExec("select id1,id2 from smurfs where id1 in ($nonSmurfsString) and id2 in ($nonSmurfsString) and status=2","check smurfs table for probable smurfs in ids \"$nonSmurfsString\"");
  my $p_probSmurfsData=$sth->fetchall_arrayref();
  if(@{$p_probSmurfsData}) {
    foreach my $p_probSmurfData (@{$p_probSmurfsData}) {
      my ($id1,$id2)=($p_probSmurfData->[0],$p_probSmurfData->[1]);
      $sldb->adminEvent('DEL_PROB_SMURF',1,0,0,{accountId1 => $id1, accountId2 => $id2});
      $sldb->adminEvent('ADD_NOT_SMURF',1,0,0,{accountId1 => $id1, accountId2 => $id2});
    }
    $sldb->do("update smurfs set status=0 where id1 in ($nonSmurfsString) and id2 in ($nonSmurfsString) and status=2","transform probable smurfs into not-smurf in smurfs table for \"$nonSmurfsString\"");
  }

  $sth=$sldb->prepExec("select accountId,userId from userAccounts where accountId in ($nonSmurfsString)","retrieve userId of accounts \"$nonSmurfsString\" from userAccounts table!");
  my $p_accountsData=$sth->fetchall_hashref('accountId');
  error("Unable to find userId values of accounts \"$nonSmurfsString\" in userAccounts table!") unless(%{$p_accountsData});

  my %userConflictingAccounts;
  my %conflictingUsers;
  foreach my $a (keys %{$p_accountsData}) {
    my $u=$p_accountsData->{$a}->{userId};
    if(exists $userConflictingAccounts{$u}) {
      push(@{$userConflictingAccounts{$u}},$a);
      $conflictingUsers{$u}=1;
    }else{
      $userConflictingAccounts{$u}=[$a];
    }
  }

  my @detachedUsers;
  foreach my $u (keys %conflictingUsers) {
    my ($p_conflictingSmurfGroups,$p_smurfGroups)=$sldb->getUserOrderedSmurfGroups($u,$userConflictingAccounts{$u},$conf{dynIpRange});
    for my $groupNb (0..$#{$p_conflictingSmurfGroups}) {
      push(@detachedUsers,detachSmurfGroup($u,$p_smurfGroups->[$groupNb])) if($groupNb!=0);
      setNonSmurfGroup($p_conflictingSmurfGroups->[$groupNb],$userConflictingAccounts{$u});
    }
  }

  foreach my $detachedUser (@detachedUsers) {
    checkSmurf($detachedUser);
  }

  my $elapsedTime=time-$functionStartTime;
  slog("checkNonSmurfs($nonSmurfsString) call lasted $elapsedTime seconds",2) if($elapsedTime > 1);

}

sub detachSmurfGroup {
  my ($oldUserId,$p_smurfGroup)=@_;
  my $smurfGroupString=join(',',@{$p_smurfGroup});
  my $sth=$sldb->prepExec("select id,rank from accounts where id in ($smurfGroupString) and bot=0","read non-bot ranks for id(s) \"$smurfGroupString\" in accounts table");
  my $p_smurfsData=$sth->fetchall_hashref('id');
  if(! %{$p_smurfsData}) {
    slog("Detaching a bot smurf group \"$smurfGroupString\"",2);
    $sth=$sldb->prepExec("select id,rank from accounts where id in ($smurfGroupString)","read ranks for id(s) \"$smurfGroupString\" in accounts table");
    $p_smurfsData=$sth->fetchall_hashref('id');
    error("Unable to find ranks for id(s) \"$smurfGroupString\" in accounts table") unless(%{$p_smurfsData});
  }
  my $maxRank=-1;
  my @mainAccountCandidates;
  foreach my $id (keys %{$p_smurfsData}) {
    if($p_smurfsData->{$id}->{rank} == $maxRank) {
      push(@mainAccountCandidates,$id);
    }elsif($p_smurfsData->{$id}->{rank} > $maxRank) {
      $maxRank=$p_smurfsData->{$id}->{rank};
      @mainAccountCandidates=($id);
    }
  }
  error("Unable to choose new main account for smurf group \"$smurfGroupString\"") unless(@mainAccountCandidates);

  @mainAccountCandidates=sort {$a <=> $b} @mainAccountCandidates;
  my $newUserId=$mainAccountCandidates[0];
  foreach my $id (@{$p_smurfGroup}) {
    my $subType=0;
    $subType=1 if($newUserId!=$id);
    $sldb->adminEvent('SPLIT_ACC',$subType,0,0,{oldUserId => $oldUserId, newUserId => $newUserId, accountId => $id});
    $sldb->queueGlobalRerate($id);
  }
  $sldb->do("update userAccounts set userId=$newUserId where accountId in ($smurfGroupString)","update userAccounts for new main account \"$newUserId\" for smurf group \"$smurfGroupString\"");

  $sldb->computeAllUserIps($newUserId,$conf{dynIpThreshold},$conf{dynIpRange});
  $sldb->computeAllUserIps($oldUserId,$conf{dynIpThreshold},$conf{dynIpRange});

  return $newUserId;
}

# This function must always be called for all conflicting groups of the conflicting accounts!
sub setNonSmurfGroup {
  my ($p_conflictingGroup,$p_conflictingAccounts)=@_;
  my %conflictingAccounts;
  @conflictingAccounts{@{$p_conflictingAccounts}}=@{$p_conflictingAccounts};
  delete @conflictingAccounts{@{$p_conflictingGroup}};
  my @accountsToTest=keys %conflictingAccounts;
  foreach my $idInGroup (@{$p_conflictingGroup}) {
    foreach my $conflictingId (@accountsToTest) {
      next unless($idInGroup < $conflictingId);
      $sldb->adminEvent('ADD_NOT_SMURF',0,0,0,{accountId1 => $idInGroup, accountId2 => $conflictingId});
      $sldb->do("insert into smurfs values ($idInGroup,$conflictingId,0,0)","add non-smurf \"$idInGroup\" <-> \"$conflictingId\" into smurfs table");
    }
  }
}

sub initializeIpTablesIfNeeded {
  my $sth=$sldb->prepExec("select count(*) from ips","check ips state in database!");
  my @ipCount=$sth->fetchrow_array();
  return if($ipCount[0] > 0);
  slog("Initializing IP and smurf tables from \"playersDetails\" and \"gamesDetails\" tables",3);

  $sth=$sldb->prepExec("select gd.gameId,group_concat(accountId),UNIX_TIMESTAMP(endTimestamp) from playersDetails pd,gamesDetails gd where pd.gameId=gd.gameId and team is not null and ip is not null and ip != 0 group by gd.gameId order by endTimestamp","read \"playersDetails\" and \"gamesDetails\" tables for ip tables initialization");
  my $sth2=$sldb->{dbh}->prepare('select accountId,ip from playersDetails where ip is not null and gameId = ?');
  my ($gameId,$accountIds,$endTimestamp);
  while(($gameId,$accountIds,$endTimestamp)=$sth->fetchrow_array()) {
    error("Unable to execute prepared statement with parameter $gameId during IP and smurf tables initialization") unless($sth2->execute($gameId));
    my ($playerId,$playerIp);
    while(($playerId,$playerIp)=$sth2->fetchrow_array()) {
      seenIp($playerId,$playerIp,$endTimestamp) unless($playerIp == 0);
    }
    my @nonSmurfs=split(/,/,$accountIds);
    checkNonSmurfs(\@nonSmurfs) if($#nonSmurfs > 0);
  }

  slog("IP and smurf tables initialization done.",3);
}

sub initializeUserIpTablesIfNeeded {
  my $sth=$sldb->prepExec("select count(*) from userIps","check userIps state in database!");
  my @ipCount=$sth->fetchrow_array();
  return if($ipCount[0] > 0);
  slog("Initializing user IP tables from \"playersDetails\" and \"gamesDetails\" tables",3);
  $sth=$sldb->prepExec("select userId from userAccounts where userId=accountId","get all userIds from userAccounts table for user IP tables initialization");
  my $p_results=$sth->fetchall_arrayref();
  foreach my $p_userId (@{$p_results}) {
    $sldb->computeAllUserIps($p_userId->[0],$conf{dynIpThreshold},$conf{dynIpRange});
  }
  slog("User IP tables initialization done.",3);
}

sub isValidGDR {
  my $p_gdr=shift;
  foreach my $field (qw/startTs endTs gameId duration engine type structure players bots/) {
    if(! exists $p_gdr->{$field} || ! defined $p_gdr->{$field}) {
      slog("Missing field \"$field\" in GDR!",2);
      return 0;
    }
  }
  foreach my $field (qw/startTs endTs duration/) {
    if($p_gdr->{$field} !~ /^\d+$/) {
      slog("Field \"$field\" contains non-numeric value ($p_gdr->{$field}) in GDR!",2);
      return 0;
    }
  }
  foreach my $field (qw/gameId type structure/) {
    if($p_gdr->{$field} !~ /^[\w\-]+$/) {
      slog("Invalid \"$field\" value in GDR ($p_gdr->{$field})!",2);
      return 0;
    }
  }
  foreach my $field (qw/players bots/) {
    if(! (ref($p_gdr->{$field}) eq 'ARRAY')) {
      slog("Invalid \"$field\" field in GDR (not an array reference)!",2);
      return 0;
    }
  }
  my $hasUndecidedWinValues=0;
  foreach my $p_player (@{$p_gdr->{players}}) {
    foreach my $field (qw/accountId name ip team allyTeam win/) {
      if(! exists $p_player->{$field} || ! defined $p_player->{$field}) {
        slog("Missing field \"$field\" in player entry in GDR!",2);
        return 0;
      }
    }
    if($p_player->{accountId} !~ /^\d+$/) {
      slog("Invalid accountId value in player entry in GDR ($p_player->{accountId})!",2);
      return 0;
    }
    if($p_player->{name} !~ /^[\w\[\]]{1,20}$/) {
      slog("Invalid name value in player entry in GDR ($p_player->{name})!",2);
      return 0;
    }
    foreach my $field (qw/team allyTeam/) {
      if($p_player->{$field} !~ /^\d*$/) {
        slog("Invalid \"$field\" value in player entry in GDR ($p_player->{$field})!",2);
        return 0;
      }
    }
    if($p_player->{win} !~ /^\d$/) {
      slog("Invalid win value in player entry in GDR ($p_player->{win})!",2);
      return 0;
    }
    if($p_player->{win} != 0 && $p_player->{win} != 1) {
      $hasUndecidedWinValues=1;
    }
  }
  foreach my $p_bot (@{$p_gdr->{bots}}) {
    foreach my $field (qw/accountId name ai team allyTeam win/) {
      if(! exists $p_bot->{$field} || ! defined $p_bot->{$field}) {
        slog("Missing field \"$field\" in bot entry in GDR!",2);
        return 0;
      }
    }
    if($p_bot->{accountId} !~ /^\d+$/) {
      slog("Invalid accountId value in bot entry in GDR ($p_bot->{accountId})!",2);
      return 0;
    }
    foreach my $field (qw/team allyTeam/) {
      if($p_bot->{$field} !~ /^\d*$/) {
        slog("Invalid \"$field\" value in bot entry in GDR ($p_bot->{$field})!",2);
        return 0;
      }
    }
    if($p_bot->{win} !~ /^\d$/) {
      slog("Invalid win value in bot entry in GDR ($p_bot->{win})!",2);
      return 0;
    }
    if($p_bot->{win} != 0 && $p_bot->{win} != 1) {
      $hasUndecidedWinValues=1;
    }
  }
  if(exists $p_gdr->{result}) {
    if(! defined $p_gdr->{result}) {
      slog("Field \"result\" existing but not defined in GDR!",2);
      return 0;
    }
    if($p_gdr->{result} ne 'gameOver' && $p_gdr->{result} ne 'undecided') {
      slog("Invalid \"result\" value in GDR ($p_gdr->{result})!",2);
      return 0;
    }
  }else{
    if($hasUndecidedWinValues) {
      $p_gdr->{result}='undecided';
    }else{
      $p_gdr->{result}='gameOver';
    }
  }
  if(exists $p_gdr->{cheating}) {
    if(! defined $p_gdr->{cheating}) {
      slog("Field \"cheating\" existing but not defined in GDR!",2);
      return 0;
    }
    if($p_gdr->{cheating} ne '0' && $p_gdr->{cheating} ne '1') {
      slog("Invalid \"cheating\" value in GDR ($p_gdr->{cheating})!",2);
      return 0;
    }
  }else{
    $p_gdr->{cheating}=0;
  }
  return 1;
}

sub handleCommand {
  my ($user,$command,$params)=@_;

  my $userAccountId=$lobby->{users}->{$user}->{accountId};
  if($command eq '#startGDR' && $params =~ /^\d+$/) {
    my $timestamp=time;
    slog("New GDR received from \"$user\" whereas previous GDR wasn't finished!",2) if(exists $GDRs{$user});
    $GDRs{$user}={timestamp => $timestamp,
                  timeshift => $timestamp-$params,
                  data => ''};
  }elsif($command eq '#endGDR') {
    if(exists $GDRs{$user}) {
      $GDRs{$user}->{data}=decode_base64($GDRs{$user}->{data});
      my $p_gdr=eval { thaw($GDRs{$user}->{data}) };
      if($@) {
        slog("Unable to read GDR received from \"$user\"!",2);
      }elsif(! defined $p_gdr || ! %{$p_gdr}) {
        slog("Received an empty game data report from \"$user\"!",2);
      }elsif(! isValidGDR($p_gdr)) {
        slog("Received an invalid game data report from \"$user\"!",2);
      }else{
        slog("Received a game data report (timeshift:$GDRs{$user}->{timeshift}) from $user (gameId:$p_gdr->{gameId},startTs:$p_gdr->{startTs},endTs:$p_gdr->{endTs},duration:$p_gdr->{duration},type:$p_gdr->{type},structure:$p_gdr->{structure})",4);
        if($p_gdr->{duration} < $conf{minGameLength}) {
          slog("Discarding GDR of \"$user\" [$p_gdr->{gameId},$p_gdr->{startTs}] (game too short)",2);
        }else{
          my $quotedGameId=$sldb->quote($p_gdr->{gameId});
          my $sth=$sldb->prepExec("select count(*) from games where gameId=$quotedGameId","check for duplicate gameId in database!");
          my @gameIdCount=$sth->fetchrow_array();
          if($gameIdCount[0] > 0) {
            slog("Duplicate gameId ($p_gdr->{gameId}), discarding GDR!",2);
          }else{
            $sth=$sldb->prepExec("select startTimestamp from games where hostAccountId=$userAccountId and gameId is NULL and ABS(TIMESTAMPDIFF(SECOND,startTimestamp,FROM_UNIXTIME($p_gdr->{startTs}+$GDRs{$user}->{timeshift}))) < $conf{tsTolerance} order by (ABS(TIMESTAMPDIFF(SECOND,startTimestamp,FROM_UNIXTIME($p_gdr->{startTs}+$GDRs{$user}->{timeshift}))))","perform reconciliation in database!");
            my $foundTs=0;
            my @possibleTs;
            while(@possibleTs=$sth->fetchrow_array()) {
              slog("Trying reconciliation with [$userAccountId,$possibleTs[0]]",5);
              my $sth2=$sldb->prepExec("select accountId from players where hostAccountId=$userAccountId and startTimestamp=\"$possibleTs[0]\"","list players for game [$userAccountId,$possibleTs[0]]!");
              my %playersInGame;
              my @player;
              while(@player=$sth2->fetchrow_array()) {
                $playersInGame{$player[0]}=1;
              }
              my $compatible=1;
              foreach my $p_player (@{$p_gdr->{players}}) {
                next if($p_player->{name} eq $user);
                if(! exists $playersInGame{$p_player->{accountId}}) {
                  slog("Player \"$p_player->{name}\" wasn't marked as being in the battle, reconciliation failed! [$userAccountId,$possibleTs[0]]",5);
                  $compatible=0;
                  last;
                }
              }
              if($compatible) {
                slog("Reconciliation succeeded with game [$userAccountId,$possibleTs[0]]",5);
                $foundTs=$possibleTs[0];
                last;
              }
            }
            if(! $foundTs) {
              slog("Unable to find compatible battle, discarding GDR of \"$user\" [$p_gdr->{gameId},$p_gdr->{startTs}]",2);
            }else{
              my $hasBot=0;
              $hasBot=1 if(@{$p_gdr->{bots}});
              my $undecided=0;
              $undecided=1 if($p_gdr->{result} eq 'undecided');
              $sldb->do("update games set gameId=$quotedGameId where hostAccountId=$userAccountId and startTimestamp=\"$foundTs\"","update gameId in table games");
              my ($quotedEngine,$quotedType,$quotedStructure)=$sldb->quote($p_gdr->{engine},$p_gdr->{type},$p_gdr->{structure});
              $sldb->do("insert into gamesDetails values ($quotedGameId,FROM_UNIXTIME($GDRs{$user}->{timestamp}),FROM_UNIXTIME($p_gdr->{startTs}+$GDRs{$user}->{timeshift}),FROM_UNIXTIME($p_gdr->{endTs}+$GDRs{$user}->{timeshift}),$p_gdr->{duration},$quotedEngine,$quotedType,$quotedStructure,$hasBot,$undecided,$p_gdr->{cheating})","insert data in table gamesDetails");
              my @nonSmurfAccounts;
              my %seenIps;
              foreach my $p_player (@{$p_gdr->{players}}) {
                my $ipAddr=$p_player->{ip};
                if($ipAddr =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/ && $1<256 && $2<256 && $3<256 && $4<256) {
                  $seenIps{$p_player->{accountId}}=$ipAddr;
                  $ipAddr="INET_ATON(\"$ipAddr\")";
                }else{
                  slog("Got an invalid IP address for user \"$p_player->{name}\" ($ipAddr)",2) unless($p_player->{name} eq $user || $ipAddr eq '');
                  $ipAddr='NULL';
                }
                my $team=$p_player->{team};
                if($team eq '') {
                  $team='NULL';
                }else{
                  push(@nonSmurfAccounts,$p_player->{accountId}) if($ipAddr ne 'NULL');
                }
                my $allyTeam=$p_player->{allyTeam};
                $allyTeam='NULL' if($allyTeam eq '');
                my $quotedName=$sldb->quote($p_player->{name});
                $sldb->do("insert into playersDetails values ($quotedGameId,$p_player->{accountId},$quotedName,$ipAddr,$team,$allyTeam,$p_player->{win})","insert data in table playersDetails");
              }
              foreach my $accountId (keys %seenIps) {
                seenIp($accountId,$seenIps{$accountId});
              }
              checkNonSmurfs(\@nonSmurfAccounts) if($#nonSmurfAccounts > 0);
              foreach my $p_bot (@{$p_gdr->{bots}}) {
                my ($quotedName,$quotedAi)=$sldb->quote($p_bot->{name},$p_bot->{ai});
                $sldb->do("insert into botsDetails values ($quotedGameId,$quotedName,$p_bot->{accountId},$quotedAi,$p_bot->{team},$p_bot->{allyTeam},$p_bot->{win})","insert data in table botsDetails");
              }
              $sldb->do("insert into tsRatingQueue values ($quotedGameId,FROM_UNIXTIME($GDRs{$user}->{timestamp}),0)","add game $p_gdr->{gameId} in rating queue table") if(! $hasBot && ! $undecided && ! $p_gdr->{cheating} && $p_gdr->{type} ne 'Solo');
            }
          }
        }
      }
      delete $GDRs{$user};
    }else{
      slog("Orphan #endGDR received from \"$user\"!",2);
    }
  }

  return unless(grep {$userAccountId eq $_} @lobbyAdminIds);
  my $lcCommand=lc($command);
  if(! exists $commands{$lcCommand}) {
    sayPrivate($user,'Unknown command.');
  }else{
    &{$commands{$lcCommand}}($user,$params);
  }
}

sub hQuit {
  my $user=shift;
  sayPrivate($user,'Quitting!');
  $stopping=1;
}

sub hRestart {
  my $user=shift;
  sayPrivate($user,'Restarting!');
  $stopping=2;
}

sub hSetLogLevel {
  my ($user,$params)=@_;
  if($params =~ /^(main|sldb|lobby) ([1-5])$/) {
    my ($log,$level)=($1,$2);
    my %logs=(main => $sLog,
              sldb => $sLogSldb,
              lobby => $sLogLobby);
    $logs{$log}->setLevels([$level]);
  }else{
    sayPrivate($user,'Invalid syntax (!setLogLevel main|sldb|lobby <logLevel>)');
  }
}

sub hStatus {
  my ($user,$params)=@_;
  my @hostKeys=keys %hosts;
  my $hostsString=join(',',@hostKeys);
  sayPrivate($user,"Current hosts: $hostsString");
  my $nbBattles=keys %battles;
  sayPrivate($user,"\%hosts (".($#hostKeys+1).") and \%battles ($nbBattles) don't match!") if($nbBattles != $#hostKeys+1);
  my @unmonGames;
  foreach my $bId (keys %unmonitoredGames) {
    push(@unmonGames,$battles{$bId});
  }
  sayPrivate($user,($#unmonGames+1).' unmonitored game(s) ('.join(',',@unmonGames).')');
  my @monGames;
  foreach my $bId (keys %monitoredGames) {
    push(@monGames,$battles{$bId});
  }
  sayPrivate($user,($#monGames+1).' monitored game(s) ('.join(',',@monGames).')');
  my @monBattles;
  foreach my $bId (keys %monitoredBattles) {
    push(@monBattles,$battles{$bId});
  }
  sayPrivate($user,($#monBattles+1).' monitored battle(s) ('.join(',',@monBattles).')');
}

sub endMonitoring {
  foreach my $bId (keys %monitoredGames) {
    slog("Lobby disconnection: terminating monitoring of game prematurely [$monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs}]",4);
    $sldb->do("update games set endTimestamp=now(),endCause=2 where hostAccountId=$monitoredGames{$bId}->{accountId} and startTimestamp=FROM_UNIXTIME($monitoredGames{$bId}->{startTs})","update games table on lobby disconnect for game ($monitoredGames{$bId}->{accountId},$monitoredGames{$bId}->{startTs})");
  }
  
  %hosts=();
  %battles=();
  %unmonitoredGames=();
  %monitoredBattles=();
  %monitoredGames=();
}

initializeUserTablesIfNeeded();
initializeIpTablesIfNeeded();
initializeUserIpTablesIfNeeded();

while(! $stopping) {
  if(! $lobbyState) {
    if(time-$timestamps{connectAttempt} > 30) {
      $timestamps{connectAttempt}=time;
      $lobbyState=1;
      $sldb->do("truncate rtBattles",'empty real time table rtBattles on connect',\&nonFatalError);
      $sldb->do("truncate rtPlayers",'empty real time table rtPlayers on connect',\&nonFatalError);
      $sldb->do("truncate rtBattlePlayers",'empty real time table rtBattlePlayers on connect',\&nonFatalError);
      $lSock=$lobby->connect(\&cbLobbyDisconnect,{TASSERVER => \&cbLobbyConnect},\&cbConnectTimeout);
      if(! $lSock) {
        $lobbyState=0;
        slog("Connection to lobby server failed",1);
      }
    }else{
      sleep($timestamps{connectAttempt}+31-time);
    }
  }

  if($lobbyState > 0) {
    my @pendingSockets=IO::Select->new(($lSock))->can_read(1);
    
    $lobby->receiveCommand() if(@pendingSockets);

    if(time - $timestamps{connectAttempt} > 30 && time - $lobby->{lastRcvTs} > 60) {
      slog("Disconnected from lobby server (timeout)",2);
      $lobbyState=0;
      $lobby->disconnect();
      endMonitoring();
    }
  }

  if($lobbyState > 1 && ( ( time - $timestamps{ping} > 5 && time - $lobby->{lastSndTs} > 28)
                          || ( time - $timestamps{ping} > 28 && time - $lobby->{lastRcvTs} > 28) ) ) {
    $lobby->sendCommand(['PING']);
    $timestamps{ping}=time;
  }
}

if($lobbyState) {
  $lobbyState=0;
  $lobby->disconnect();
}
endMonitoring();

if($stopping == 2) {
  exec($0) || forkedError('Unable to restart slMonitor',0);
}
