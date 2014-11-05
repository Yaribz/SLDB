#!/usr/bin/perl -w
#
# This file implements the Spring lobby interface for SLDB (sldbLi), it is part
# of SLDB.
#
# sldbLi is a Spring lobby bot, it serves 3 main purposes:
# - allow (auto)hosts to access ranking data for auto-balancing/matchmaking
# - offer basic ranking data to players and advanced ranking data to SLDB admins
# - allow SLDB admins to manage SLDB user data manually
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

use POSIX (':sys_wait_h','ceil');

use IO::Select;
use Text::ParseWords;

use SimpleLog;
use Sldb;
use SldbLiConf;

use SpringLobbyInterface;

my $sldbLiVer='0.1';

$SIG{TERM} = \&sigTermHandler;
$SIG{USR1} = \&sigUsr1Handler;
$SIG{USR2} = \&sigUsr2Handler;

my %ADMIN_EVT_TYPE=('UPD_USERDETAILS' => 0,
                    'JOIN_ACC' => 1,
                    'SPLIT_ACC' => 2,
                    'ADD_PROB_SMURF' => 3,
                    'DEL_PROB_SMURF' => 4,
                    'ADD_NOT_SMURF' => 5,
                    'DEL_NOT_SMURF' => 6);
my %gameTypeMapping=('Duel' => 'Duel',
                     'FFA' => 'Ffa',
                     'Team' => 'Team',
                     'TeamFFA' => 'TeamFfa',
                     'Global' => '');

my %ircColors;
my %noColor;
for my $i (0..15) {
  $ircColors{$i}=''.sprintf('%02u',$i);
  $noColor{$i}='';
}
my @ircStyle=(\%ircColors,'');
my @noIrcStyle=(\%noColor,'');

my %lobbyHandlers = ( adminevents => \&hAdminEvents,
                      checkips => \&hCheckIps,
                      checkprobsmurfs => \&hCheckProbSmurfs,
                      '#getskill' => \&hGetSkill,
                      help => \&hHelp,
                      helpall => \&hHelpAll,
                      joinacc => \&hJoinAcc,
                      leaderboard => \&hLeaderboard,
                      notsmurf => \&hNotSmurf,
                      quit => \&hQuit,
                      ranking => \&hRanking,
                      reloadconf => \&hReloadConf,
                      restart => \&hRestart,
                      searchuser => \&hSearchUser,
                      sendlobby => \&hSendLobby,
                      set => \&hSet,
                      setname => \&hSetName,
                      splitacc => \&hSplitAcc,
                      topskill => \&hTopSkill,
                      version => \&hVersion,
                      whois => \&hWhois );

# Basic checks ################################################################

if($#ARGV != 0 || ! (-f $ARGV[0])) {
  print "usage: $0 <configurationFile>\n";
  exit 1;
}

my $confFile=$ARGV[0];
my $sLog=SimpleLog->new(prefix => "[SldbLi] ");
my $botConf=SldbLiConf->new($confFile,$sLog);

sub slog {
  $sLog->log(@_);
}

if(! $botConf) {
  slog("Unable to load SldbLi configuration at startup",0);
  exit 1;
}

my $masterChannel=$botConf->{conf}->{masterChannel};
$masterChannel=$1 if($masterChannel =~ /^([^\s]+)\s/);

# State variables #############################################################

my %conf=%{$botConf->{conf}};
$sLog=$botConf->{log};
my $lSock;
my @sockets=();
my $running=1;
my $quitScheduled=0;
my %timestamps=(connectAttempt => 0,
                ping => 0,
                ratingStateCheck => 0);
my $lobbyState=0; # (0:not_connected, 1:connecting, 2: connected, 3:logged_in, 4:start_data_received)
my %pendingRedirect;
my $p_answerFunction;
my $lobbyBrokenConnection=0;
my %lastSentMessages;
my @messageQueue=();
my @lowPriorityMessageQueue=();
my %lastCmds;
my %ignoredUsers;
my $triedGhostWorkaround=0;
my $lanMode=0;
my %hostBattles;
my %hostSkills;
my %battleHosts;
my %newGamesFinished;
my %hostsVersions;

my $sldbSimpleLog=SimpleLog->new(logFiles => [$conf{logDir}."/sldbLi.log",''],
                                 logLevels => [$conf{sldbLogLevel},3],
                                 useANSICodes => [0,1],
                                 useTimestamps => [1,1],
                                 prefix => "[SLDB] ");

my $lobbySimpleLog=SimpleLog->new(logFiles => [$conf{logDir}."/sldbLi.log",''],
                                  logLevels => [$conf{lobbyInterfaceLogLevel},2],
                                  useANSICodes => [0,1],
                                  useTimestamps => [1,1],
                                  prefix => "[SpringLobbyInterface] ");

my $lobby = SpringLobbyInterface->new(serverHost => $conf{lobbyHost},
                                      serverPort => $conf{lobbyPort},
                                      simpleLog => $lobbySimpleLog,
                                      warnForUnhandledMessages => 0);
my $sldb;
if($conf{sldb} =~ /^([^\/]+)\/([^\@]+)\@((?i:dbi)\:\w+\:\w.*)$/) {
  my ($sldbLogin,$sldbPasswd,$sldbDs)=($1,$2,$3);
  $sldb=Sldb->new({dbDs => $sldbDs,
                   dbLogin => $sldbLogin,
                   dbPwd => $sldbPasswd,
                   sLog => $sldbSimpleLog,
                   sqlErrorHandler => \&sqlErrorHandler });
}else{
  slog("Unable to parse sldb configuration parameter",1);
  exit;
}
if(! $sldb->connect()) {
  slog("Unable to connect to SLDB",1);
  exit;
}

# Subfunctions ################################################################

sub sigTermHandler {
  scheduleQuit('SIGTERM signal received');
}

sub sigUsr1Handler {
  scheduleQuit('SIGUSR1 signal received',2);
}

sub sigUsr2Handler {
  my $newSldbLi=SldbLiConf->new($confFile,$sLog);
  if(! $newSldbLi) {
    slog('Unable to reload SldbLi configuration',2);
  }else{
    $botConf=$newSldbLi;
    %conf=%{$botConf->{conf}};
    slog('SldbLi configuration reloaded',3);
  }
}

sub sqlErrorHandler {
  my $m=shift;
  $sldb->{sLog}->log($m,1);
  broadcastMsg('SQL error!');
}

sub forkedError {
  my ($msg,$level)=@_;
  slog($msg,$level);
  exit 1;
}

sub convertDuration {
  my $duration=shift;
  $duration = $1 * 525600 if($duration =~ /^(\d+)y$/);
  $duration = $1 * 43200 if($duration =~ /^(\d+)m$/);
  $duration = $1 * 10080 if($duration =~ /^(\d+)w$/);
  $duration = $1 * 1440 if($duration =~ /^(\d+)d$/);
  $duration = $1 * 60 if($duration =~ /^(\d+)h$/);
  return $duration;
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

sub intToIp {
  my $ipNb=shift;
  return join('.',unpack('C4',pack('N', $ipNb)));
}

sub previousRatingPeriod {
  my ($ratingYear,$ratingMonth)=$sldb->getCurrentRatingYearMonth();
  $ratingMonth--;
  if($ratingMonth == 0) {
    $ratingMonth=12;
    $ratingYear--;
  }
  $ratingMonth=sprintf('%02d',$ratingMonth);
  return $ratingYear.$ratingMonth;
}

sub secToDayAge {
  my $sec=shift;
  return 'Now' if($sec < 60);
  if($sec < 3600) {
    my $nbMin=int($sec/60);
    return "$nbMin min. ago";
  }
  if($sec < 86400) {
    my $nbHours=int($sec/3600);
    return "$nbHours hour".($nbHours > 1 ? 's' : '').' ago';
  }
  my $nbDays=int($sec/86400);
  return "Yesterday" if($nbDays < 2);
  return "$nbDays days ago";
}

sub realLength {
  my $s=shift;
  $s=~s/\d{1,2}(?:,\d{1,2})?//g;
  $s=~s/[]//g;
  return length($s);
}

sub formatList {
  my ($p_list,$maxLength)=@_;
  return '' unless(@{$p_list});
  return '...' if(realLength($p_list->[0]) > $maxLength || ($#{$p_list} > 0 && realLength("$p_list->[0]...") > $maxLength));
  my $result=$p_list->[0];
  for my $i (1..$#{$p_list}) {
    if($i == $#{$p_list}) {
      return "$result..." if(realLength("$result,$p_list->[$i]") > $maxLength);
    }else{
      return "$result..." if(realLength("$result,$p_list->[$i]...") > $maxLength);
    }
    $result.=",$p_list->[$i]";
  }
  return $result;
}

sub rightPadString {
  my ($string,$size)=@_;
  my $length=realLength($string);
  if($length < $size) {
    $string.=' 'x($size-$length);
  }elsif($length > $size) {
    $string=substr($string,0,$size-3);
    $string.='...';
  }
  return $string;
}

sub formatArray {
  my ($p_fields,$p_entries,$title,$maxLength,$maxSize)=@_;
  $title='' unless(defined $title);
  $maxLength=100 unless(defined $maxLength);
  my @fields=@{$p_fields};
  my @entries=@{$p_entries};
  my @rows;
  my $overSized=0;
  if(defined $maxSize && $#entries >= $maxSize) {
    $overSized=1;
    $#entries=$maxSize-1;
    $#rows=$#entries+4;
  }else{
    $#rows=$#entries+3;
  }
  my $rowLength=0;
  for my $i (0..$#rows) {
    $rows[$i]="";
  }
  for my $i (0..$#fields) {
    my $field=$fields[$i];
    my $length=getMaxLength($field,$p_entries);
    $length=$maxLength if($length > $maxLength);
    $rowLength+=$length;
    for my $j (0..$#rows) {
      if($j==0) {
        $rows[0].=rightPadString($field,$length);
      }elsif($j==1) {
        $rows[1].=('-' x $length);
      }elsif($overSized && $j==$#rows-1) {
        if($length > 2) {
          $rows[$j].=rightPadString('...',$length);
        }else{
          $rows[$j].=('.' x $length);
        }
      }elsif($j==$#rows) {
        $rows[$j].=('=' x $length);
      }elsif(exists $entries[$j-2]->{$field} && defined $entries[$j-2]->{$field}) {
        $rows[$j].=rightPadString($entries[$j-2]->{$field},$length);
      }else{
        $rows[$j].=(' ' x $length);
      }
      if($i != $#fields) {
        if($j == $#rows) {
          $rows[$j].="==";
        }else{
          $rows[$j].="  ";
        }
      }
    }
  }
  if($title) {
    $rowLength+=$#fields * 2 if($#fields > 0);
    if(realLength($title) < $rowLength-3) {
      $title="[ $title ]";
      $title=(' ' x int(($rowLength-realLength($title))/2)).$title.(' ' x ceil(($rowLength-realLength($title))/2));
    }elsif(realLength($title) < $rowLength-1) {
      $title="[$title]";
      $title=(' ' x int(($rowLength-realLength($title))/2)).$title.(' ' x ceil(($rowLength-realLength($title))/2));
    }
    unshift(@rows,$title);
  }
  return \@rows;
}

sub getMaxLength {
  my ($field,$p_entries)=@_;
  my $length=realLength($field);
  foreach my $entry (@{$p_entries}) {
    if(exists $entry->{$field} && defined $entry->{$field} && realLength($entry->{$field}) > $length) {
      $length=realLength($entry->{$field});
    }
  }
  return $length;
}

sub formatFloat {
  my ($n,$p)=@_;
  $n=sprintf("%.${p}f",$n) if($n=~/^-?\d+\.\d+$/);
  return $n;
}

sub formatInteger {
  my $n=shift;
  if($n >= 100000000) {
    $n=int($n / 1000000);
    $n.='M.';
  }elsif($n >= 100000) {
    $n=int($n / 1000);
    $n.='K.';
  }
  return $n;
}

sub getCpuSpeed {
  if(-f "/proc/cpuinfo" && -r "/proc/cpuinfo") {
    my @cpuInfo=`cat /proc/cpuinfo 2>/dev/null`;
    my %cpu;
    foreach my $line (@cpuInfo) {
      if($line =~ /^([\w\s]*\w)\s*:\s*(.*)$/) {
        $cpu{$1}=$2;
      }
    }
    if(defined $cpu{"model name"} && $cpu{"model name"} =~ /(\d+)\+/) {
      return $1;
    }
    if(defined $cpu{"cpu MHz"} && $cpu{"cpu MHz"} =~ /^(\d+)(?:\.\d*)?$/) {
      return $1;
    }
    if(defined $cpu{bogomips} && $cpu{bogomips} =~ /^(\d+)(?:\.\d*)?$/) {
      return $1;
    }
    slog("Unable to parse CPU info from /proc/cpuinfo",2);
    return 0;
  }else{
    slog("Unable to retrieve CPU info from /proc/cpuinfo",2);
    return 0;
  }
}

sub getLocalLanIp {
  my @ips;

  $ENV{LANG}="C";
  my $ifconfigBin;
  if(-x '/sbin/ifconfig') {
    $ifconfigBin='/sbin/ifconfig';
  }elsif(-x '/bin/ifconfig') {
    $ifconfigBin='/bin/ifconfig';
  }else{
    $ifconfigBin='ifconfig';
  }
  my @ifConfOut=`$ifconfigBin`;
  foreach my $line (@ifConfOut) {
    next unless($line =~ /inet addr:\s*(\d+\.\d+\.\d+\.\d+)\s/);
    push(@ips,$1);
  }
  foreach my $ip (@ips) {
    if($ip =~ /^10\./ || $ip =~ /192\.168\./) {
      slog("Following local LAN IP address detected: $ip",4);
      return $ip;
    }
    if($ip =~ /^172\.(\d+)\./) {
      if($1 > 15 && $1 < 32) {
        slog("Following local LAN IP address detected: $ip",4);
        return $ip;
      }
    }
  }
  slog("No local LAN IP address found",4);
  return "*";
}

sub scheduleQuit {
  my ($reason,$type)=@_;
  $type=1 unless(defined $type);
  $quitScheduled=$type;
  my %quitTypes=(1 => 'shutdown',
                 2 => 'restart');
  my $msg="Bot $quitTypes{$type} scheduled (reason: $reason)";
  broadcastMsg($msg);
  slog($msg,3);
}

sub computeMessageSize {
  my $p_msg=shift;
  my $size=0;
  {
    use bytes;
    foreach my $word (@{$p_msg}) {
      $size+=length($word)+1;
    }
  }
  return $size;
}

sub checkLastSentMessages {
  my $sent=0;
  foreach my $timestamp (keys %lastSentMessages) {
    if(time - $timestamp > $conf{sendRecordPeriod}) {
      delete $lastSentMessages{$timestamp};
    }else{
      foreach my $msgSize (@{$lastSentMessages{$timestamp}}) {
        $sent+=$msgSize;
      }
    }
  }
  return $sent;
}

sub queueLobbyCommand {
  my @params=@_;
  if($params[0]->[0] =~ /SAYPRIVATE/) {
    push(@lowPriorityMessageQueue,\@params);
  }elsif(@messageQueue) {
    push(@messageQueue,\@params);
  }else{
    my $alreadySent=checkLastSentMessages();
    my $toBeSent=computeMessageSize($params[0]);
    if($alreadySent+$toBeSent+5 >= $conf{maxBytesSent}) {
      slog("Output flood protection: queueing message(s)",2);
      push(@messageQueue,\@params);
    }else{
      sendLobbyCommand(\@params,$toBeSent);
    }
  }
}

sub sendLobbyCommand {
  my ($p_params,$size)=@_;
  $size=computeMessageSize($p_params->[0]) unless(defined $size);
  my $timestamp=time;
  $lastSentMessages{$timestamp}=[] unless(exists $lastSentMessages{$timestamp});
  push(@{$lastSentMessages{$timestamp}},$size);
  if(! $lobby->sendCommand(@{$p_params})) {
    $lobbyBrokenConnection=1 if($lobbyState > 0);
  }
}

sub checkQueuedLobbyCommands {
  return unless($lobbyState > 1 && (@messageQueue || @lowPriorityMessageQueue));
  my $alreadySent=checkLastSentMessages();
  while(@messageQueue) {
    my $toBeSent=computeMessageSize($messageQueue[0]->[0]);
    last if($alreadySent+$toBeSent+5 >= $conf{maxBytesSent});
    my $p_command=shift(@messageQueue);
    sendLobbyCommand($p_command,$toBeSent);
    $alreadySent+=$toBeSent;
  }
  my $nbMsgSentInLoop=0;
  while(@lowPriorityMessageQueue && $nbMsgSentInLoop < 100) {
    my $toBeSent=computeMessageSize($lowPriorityMessageQueue[0]->[0]);
    last if($alreadySent+$toBeSent+5 >= $conf{maxLowPrioBytesSent});
    my $p_command=shift(@lowPriorityMessageQueue);
    sendLobbyCommand($p_command,$toBeSent);
    $alreadySent+=$toBeSent;
    $nbMsgSentInLoop++;
  }
}

sub answer {
  my $msg=shift;
  &{$p_answerFunction}($msg);
}

sub broadcastMsg {
  my $msg=shift;
  my @broadcastChans=split(/;/,$conf{broadcastChannels});
  foreach my $chan (@broadcastChans) {
    $chan=$1 if($chan =~ /^([^\s]+)\s/);
    sayChan($chan,$msg);
  }
}

sub splitMsg {
  my ($longMsg,$maxSize)=@_;
  my @messages=($longMsg =~ /.{1,$maxSize}/gs);
  return \@messages;
}

sub sayPrivate {
  my ($user,$msg)=@_;
  my $p_messages=splitMsg($msg,$conf{maxChatMessageLength}-12-length($user));
  foreach my $mes (@{$p_messages}) {
    queueLobbyCommand(["SAYPRIVATE",$user,$mes]);
    logMsg("pv_$user","<$conf{lobbyLogin}> $mes") if($conf{logPvChat});
  }
}

sub sayChan {
  my ($chan,$msg)=@_;
  return unless($lobbyState >= 4 && (exists $lobby->{channels}->{$chan}));
  my $p_messages=splitMsg($msg,$conf{maxChatMessageLength}-9-length($chan));
  foreach my $mes (@{$p_messages}) {
    queueLobbyCommand(["SAYEX",$chan,"* $mes"]);
  }
}

sub getCommandLevels {
  my ($source,$user,$cmd)=@_;
  return $botConf->getCommandLevels($cmd,$source,'outside','stopped');
}

sub getUserAccessLevel {
  my $user=shift;
  my $p_userData;
  if(! exists $lobby->{users}->{$user}) {
    return 0;
  }else{
    $p_userData=$lobby->{users}->{$user};
  }
  return $botConf->getUserAccessLevel($user,$p_userData,! $lanMode);
}

sub handleRequest {
  my ($source,$user,$command,$floodCheck)=@_;
  $floodCheck=1 unless(defined $floodCheck);
  
  return if($floodCheck && checkCmdFlood($user));
  
  my %answerFunctions = ( pv => sub { sayPrivate($user,$_[0]) },
                          chan => sub { sayChan($masterChannel,$_[0]) } );
  $p_answerFunction=$answerFunctions{$source};
  
  my @cmd=grep {$_ ne ""} (split(/ /,$command));
  my $lcCmd=lc($cmd[0]);

  my %aliases=( ae => 'adminEvents',
                cps => 'checkProbSmurfs',
                ja => 'joinAcc',
                lb => 'leaderboard',
                ns => 'notSmurf',
                r => 'ranking',
                sa => 'splitAcc',
                sn => 'setName',
                su => 'searchUser',
                ts => 'topSkill',
                w => 'whois' );
  if(exists $aliases{$lcCmd}) {
    $lcCmd=lc($aliases{$lcCmd});
    $cmd[0]=$lcCmd;
  }

  if(exists $botConf->{commands}->{$lcCmd}) {
    slog("Start of \"$lcCmd\" command processing",5);
    
    my $p_levels=getCommandLevels($source,$user,$lcCmd);
    
    my $level=getUserAccessLevel($user);

    if(defined $p_levels->{directLevel} && $p_levels->{directLevel} ne "" && $level >= $p_levels->{directLevel}) {
      executeCommand($source,$user,\@cmd);
    }else{
      answer("$user, you are not allowed to call command \"$cmd[0]\" in current context.");
    }

    slog("End of \"$lcCmd\" command processing",5);
  }else{
    answer("Invalid command \"$cmd[0]\"") unless($source eq "chan");
  }
}

sub executeCommand {
  my ($source,$user,$p_cmd)=@_;

  my %answerFunctions = ( pv => sub { sayPrivate($user,$_[0]) },
                          chan => sub { sayChan($masterChannel,$_[0]) } );
  $p_answerFunction=$answerFunctions{$source};

  my @cmd=@{$p_cmd};
  my $command=lc(shift(@cmd));

  if(exists $lobbyHandlers{$command}) {
    if($sldb->getAccountPref($lobby->{users}->{$user}->{accountId},'ircColors') == 2 && $command !~ /^#/ && $command ne 'set') {
      sayPrivate($user,'*' x 80);
      sayPrivate($user,"WARNING: your ircColors preference is not set. You can either type:");
      sayPrivate($user,"- \"!set ircColors 1\" to keep using colors in bot output and disable this warning.");
      sayPrivate($user,"- \"!set ircColors 0\" to disable colors in bot output if your lobby client can't show them correctly.");
      sayPrivate($user,'*' x 80);
    }
    return &{$lobbyHandlers{$command}}($source,$user,\@cmd);
  }else{
    answer("Invalid command \"$command\"");
    return 0;
  }

}

sub invalidSyntax {
  my ($user,$cmd,$reason)=@_;
  $reason="" unless(defined $reason);
  $reason=" (".$reason.")" if($reason);
  answer("Invalid $cmd command usage$reason. $user, please refer to help sent in private message.");
  executeCommand("pv",$user,["help",$cmd]);
}
  

sub checkTimedEvents {
  if(time - $timestamps{ratingStateCheck} > 5 && $lobbyState > 3 && exists $lobby->{users}->{$conf{lobbyLogin}} && exists $lobby->{channels}->{$masterChannel}) {
    $timestamps{ratingStateCheck}=time;

    my %clientStatus = %{$lobby->{users}->{$conf{lobbyLogin}}->{status}};
    my $lobbyState=$clientStatus{inGame};

    my $realState=0;
    my $p_ratingState=$sldb->getRatingState();
    $realState=1 if(exists $p_ratingState->{batchRatingStatus} && $p_ratingState->{batchRatingStatus});

    if($lobbyState != $realState) {
      my $broadcastMsg='Rating batch status: ';
      if($realState) {
        $broadcastMsg.='running';
      }else{
        $broadcastMsg.='stopped';
      }
      broadcastMsg($broadcastMsg);
      $clientStatus{inGame}=$realState;
      queueLobbyCommand(["MYSTATUS",$lobby->marshallClientStatus(\%clientStatus)]);
    }
  }
  foreach my $host (keys %newGamesFinished) {
    if(time - $newGamesFinished{$host} > 3) {
      delete $newGamesFinished{$host};
      my @newGetSkillParams;
      foreach my $player (keys %{$hostSkills{$host}}) {
        if(! exists $lobby->{users}->{$player}) {
          slog("A disconnected user ($player) hasn't been removed from the skill monitored users of host $host !",2);
          delete $hostSkills{$host}->{$player};
        }else{
          my $latestRatedGame=$sldb->getLatestRatedGameId($lobby->{users}->{$player}->{accountId},$hostSkills{$host}->{$player}->{mod});
          if($latestRatedGame ne $hostSkills{$host}->{$player}->{game}) {
            $hostSkills{$host}->{$player}->{game}=$latestRatedGame;
            push(@newGetSkillParams,"$lobby->{users}->{$player}->{accountId}|$hostSkills{$host}->{$player}->{ip}");
          }
        }
        if($#newGetSkillParams > 6) {
          unshift(@newGetSkillParams,3) if(exists $hostsVersions{$host} && $hostsVersions{$host} == 3);
          my $paramsString=join(' ',@newGetSkillParams);
          handleRequest('pv',$host,"#getSkill $paramsString");
          @newGetSkillParams=();
        }
      }
      if(@newGetSkillParams) {
        unshift(@newGetSkillParams,3) if(exists $hostsVersions{$host} && $hostsVersions{$host} == 3);
        my $paramsString=join(' ',@newGetSkillParams);
        handleRequest('pv',$host,"#getSkill $paramsString");
      }
    }
  }
}

sub checkCmdFlood {
  my $user=shift;

  my $timestamp=time;
  $lastCmds{$user}={} unless(exists $lastCmds{$user});
  $lastCmds{$user}->{$timestamp}=0 unless(exists $lastCmds{$user}->{$timestamp});
  $lastCmds{$user}->{$timestamp}++;
  
  return 0 if(getUserAccessLevel($user) >= $conf{floodImmuneLevel});

  if(exists $ignoredUsers{$user}) {
    if(time > $ignoredUsers{$user}) {
      delete $ignoredUsers{$user};
    }else{
      return 1;
    }
  }

  my @autoIgnoreData=split(/;/,$conf{cmdFloodAutoIgnore});

  my $received=0;
  foreach my $timestamp (keys %{$lastCmds{$user}}) {
    if(time - $timestamp > $autoIgnoreData[1]) {
      delete $lastCmds{$user}->{$timestamp};
    }else{
      $received+=$lastCmds{$user}->{$timestamp};
    }
  }

  if($autoIgnoreData[0] && $received >= $autoIgnoreData[0]) {
    broadcastMsg("Ignoring $user for $autoIgnoreData[2] minute(s) (command flood protection)");
    $ignoredUsers{$user}=time+($autoIgnoreData[2] * 60);
    return 1;
  }
  
  return 0;
}

sub logMsg {
  my ($file,$msg)=@_;
  if(! -d $conf{logDir}."/chat") {
    if(! mkdir($conf{logDir}."/chat")) {
      slog("Unable to create directory \"$conf{logDir}/chat\"",1);
      return;
    }
  }
  if(! open(CHAT,">>$conf{logDir}/chat/$file.log")) {
    slog("Unable to log chat message into file \"$conf{logDir}/chat/$file.log\"",1);
    return;
  }
  my $dateTime=localtime();
  print CHAT "[$dateTime] $msg\n";
  close(CHAT);
}

sub initUserIrcColors {
  my $user=shift;
  my $useIrcColors;
  $sldb->getAccountPref($lobby->{users}->{$user}->{accountId},'ircColors',\$useIrcColors);
  if($useIrcColors) {
    return @ircStyle;
  }else{
    return @noIrcStyle;
  }
}

# SldbLi commands handlers #####################################################

sub hAdminEvents {
  my (undef,$user,$p_params)=@_;
  my ($maxAge,$typeFilter,$subTypeFilter,$origFilter,$origIdFilter)=@{$p_params};
  $maxAge='1w' unless(defined $maxAge);
  $maxAge=convertDuration($maxAge);
  if($maxAge !~ /^\d+$/) {
    invalidSyntax($user,'adminevents','invalid maxAge value');
    return 0;
  }
  $maxAge=time-(60*$maxAge);

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my $sql="select eventId,date,orig,message from adminEvents where date >= FROM_UNIXTIME($maxAge)";
  if(defined $typeFilter && $typeFilter ne '*') {
    $typeFilter=$ADMIN_EVT_TYPE{$typeFilter} if(exists $ADMIN_EVT_TYPE{$typeFilter});
    if($typeFilter !~ /^\d+$/) {
      invalidSyntax($user,'adminevents','invalid typeFilter value');
      return 0;
    }
    $sql.=" and type=$typeFilter";
  }
  if(defined $subTypeFilter && $subTypeFilter ne '*') {
    if($subTypeFilter !~ /^\d+$/) {
      invalidSyntax($user,'adminevents','invalid subTypeFilter value');
      return 0;
    }
    $sql.=" and subType=$subTypeFilter";
  }
  if(defined $origFilter && $origFilter ne '*') {
    if($origFilter !~ /^\d+$/) {
      invalidSyntax($user,'adminevents','invalid origFilter value');
      return 0;
    }
    $sql.=" and orig=$origFilter";
  }
  if(defined $origIdFilter) {
    if($origIdFilter !~ /^\d+$/) {
      invalidSyntax($user,'adminevents','invalid origIdFilter value');
      return 0;
    }
    $sql.=" and origId=$origIdFilter";
  }
  $sql.=' order by date desc limit 100';

  my %origMapping=( 0 => "$C{14}auto",
                    1 => "$C{4}admin",
                    2 => "$C{10}user" );

  my $sth=$sldb->prepExec($sql,"retrieve matching admin events from adminEvents table [!adminEvents]");
  my @results;
  my @resultData;
  while(@results=$sth->fetchrow_array()) {
    push(@resultData,{"$C{5}id" => "$C{12}$results[0]", date => "$C{3}$results[1]", orig => $origMapping{$results[2]}, message => "$C{1}$results[3]"});
  }
  if(@resultData) {
    my $p_resultLines=formatArray(["$C{5}id",'date','orig','message'],\@resultData,"$C{2}Admin events$C{1}");
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
    sayPrivate($user,"$C{4}Result truncated to first 100 entries.") if($#resultData == 99);
  }else{
    answer("No matching admin event found.");
  }
}

sub hCheckProbSmurfs {
  my ($source,$user,$p_params)=@_;

  if($#{$p_params} > 0) {
    invalidSyntax($user,'checkprobsmurfs');
    return 0;
  }

  my ($maxAge)=@{$p_params};
  $maxAge='6m' unless(defined $maxAge);
  $maxAge=convertDuration($maxAge);
  if($maxAge !~ /^\d+$/) {
    invalidSyntax($user,'checkprobsmurfs','invalid maxAge value');
    return 0;
  }
  $maxAge=time-(60*$maxAge);

  my $p_probableSmurfs=$sldb->getProbableSmurfs();

  if(! @{$p_probableSmurfs}) {
    answer("No probable smurf entries in database!");
    return 1;
  }

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my @searchResults;
  my %alreadyProcessed;
  my $firstLine=1;
  foreach my $p_s (@{$p_probableSmurfs}) {
    my $entryAge=time;
    my @entryData;
    my @ids=($p_s->{id1},$p_s->{id2});
    foreach my $id (@ids) {
      my $sth=$sldb->prepExec("select ud.userId,ud.name,c.country,ca.nb,cpus.cpu,cpusa.nb,UNIX_TIMESTAMP(a.lastUpdate),coalesce(rtp.inGame,-1),a.rank,group_concat(n.name order by n.lastConnection desc)
  from userDetails ud,
       accounts a,
       names n,
       userAccounts ua left join rtPlayers rtp on ua.accountId=rtp.accountId,
       countries c join (select c1.accountId accountId,max(lastConnection) maxLastConn, count(*) nb from countries c1 where c1.accountId=$id group by c1.accountId) ca
         on c.accountId=ca.accountId and c.lastConnection=ca.maxLastConn,
       cpus join (select cpus1.accountId accountId,max(lastConnection) maxLastConn, count(*) nb from cpus cpus1 where cpus1.accountId=$id group by cpus1.accountId) cpusa
         on cpus.accountId=cpusa.accountId and cpus.lastConnection=cpusa.maxLastConn
    where ud.userId=ua.userId
          and ua.accountId=$id
          and a.id=$id
          and n.accountId=$id
    group by n.accountId","");
      my ($userId,$userName,$country,$nbCountry,$cpu,$nbCpu,$lastUpdate,$inGame,$rank,$names)=$sth->fetchrow_array();
      my $accountName;
      if($names =~ /^([^,]+),(.*)$/) {
        ($accountName,$names)=($1,$2);
      }else{
        $accountName=$names;
        $names='';
      }
      my $rawUserName=$userName;
      $rawUserName=$1 if($userName =~ /^([^\(]+)\(/);
      my $rawAccountName=$accountName;
      $rawAccountName=$1 if($accountName =~ /^\[[^\]]+\](.+)$/);
      $accountName="$accountName $C{1}\[$userName]" unless($rawUserName eq $rawAccountName);
      $entryAge=$lastUpdate if($lastUpdate < $entryAge);
      my $accountId=$id;
      if($accountId == $userId) {
        $accountId="$C{1}$accountId";
      }else{
        $accountId="$C{15}$accountId";
      }
      my $countryString=$country;
      if($nbCountry > 1) {
        $nbCountry-=1;
        $countryString.=" (+$nbCountry)";
      }
      my $cpuString=$cpu;
      if($nbCpu > 1) {
        $nbCpu-=1;
        $cpuString.=" (+$nbCpu)";
      }
      my $accountActivity;
      if($inGame == -1) {
        my $accountAge=time-$lastUpdate;
        $accountActivity=secToDayAge($accountAge);
        if($accountAge > 7776000) {
          $accountActivity="$C{15}$accountActivity";
        }elsif($accountAge > 2592000) {
          $accountActivity="$C{14}$accountActivity";
        }else{
          $accountActivity="$C{1}$accountActivity";
        }
      }elsif($inGame == 0) {
        $accountActivity="$C{3}Online";
      }else{
        $accountActivity="$C{4}Ingame";
      }
      my %rankColors=(0 => $C{15}, 1 => $C{14}, 2 => $C{1}, 3 => $C{1}, 4 => $C{1}, 5 => $C{6}, 6 => $C{7}, 7 => $C{13});
      push(@entryData,{"$C{5}AccountName [UserName]" => "$C{2}$accountName",
                       AccountId => $accountId,
                       Country => "$C{1}$countryString",
                       Cpu => $cpuString,
                       LastActivity => $accountActivity,
                       Rank => $rankColors{$rank}.$rank,
                       PreviousNames => "$C{1}$names"});
    }
    next if($entryAge < $maxAge);
    if($firstLine) {
      $firstLine=0;
    }else{
      push(@searchResults,{"$C{5}AccountName [UserName]" => '.'});
    }
    push(@searchResults,@entryData);
#    last if($#searchResults >= 100);
  }
  
  if(@searchResults) {
    my $p_resultLines=formatArray(["$C{5}AccountName [UserName]",'AccountId','Country','Cpu','LastActivity','Rank','PreviousNames'],\@searchResults,"$C{2}List of probable smurfs$C{1}",80);
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
#    sayPrivate($user,"$C{4}Result truncated to first 100 entries.") if($#searchResults >= 100);
  }else{
    answer("No result!");
  }
}

sub hGetSkill {
  my (undef,$user,$p_params)=@_;
  if(! @{$p_params}) {
    slog("Ignoring invalid getSkill call from $user (no param)!",2);
    return;
  }
  my $currentRatingPeriod=$sldb->getCurrentRatingPeriod();

  my $interfaceVersion=2;
  shift(@{$p_params}) if($p_params->[0] eq '2');

  if($p_params->[0] eq '3') {
    $interfaceVersion=3;
    shift(@{$p_params});
  }

  $hostsVersions{$user}=$interfaceVersion;

  my $notAHost=0;
  if(! exists $hostBattles{$user}) {
    slog("Denying getSkill call from $user (not a host)!",2);
    $notAHost=1;
  }elsif(! exists $lobby->{battles}->{$hostBattles{$user}}) {
    slog("sldbLi / SpringLobbyInterface inconsistency for host $user, ignoring getSkill request!",1);
    return;
  }

  my %topPlayers;

  my $quotedModShortName;
  if(! $notAHost) {
    my $modShortName=$sldb->getModShortName($lobby->{battles}->{$hostBattles{$user}}->{mod});
    if(defined $modShortName) {
      $quotedModShortName=$sldb->quote($modShortName);
      if($interfaceVersion == 3) {
        foreach my $gameType (keys %gameTypeMapping) {
          $topPlayers{$gameType}={};
          my $p_topPlayers=$sldb->getTopPlayers($currentRatingPeriod,$modShortName,$gameType,20);
          if(defined $p_topPlayers) {
            for my $i (0..$#{$p_topPlayers}) {
              my $topUserId=$p_topPlayers->[$i];
              if($i == 0) {
                $topPlayers{$gameType}->{$topUserId}=1;
              }elsif($i < 5) {
                $topPlayers{$gameType}->{$topUserId}=2;
              }elsif($i < 10) {
                $topPlayers{$gameType}->{$topUserId}=3;
              }else{
                $topPlayers{$gameType}->{$topUserId}=4;
              }
            }
          }
        }
      }
    }
  }

  my @returnParams;
  foreach my $data (@{$p_params}) {

    my ($accountId,$ip);
    if($data =~ /^(\d+)\|(\d{1,3}(?:\.\d{1,3}){3})$/) {
      ($accountId,$ip)=($1,$2);
    }elsif($data =~ /^(\d+)\|?$/) {
      ($accountId,$ip)=($1,'');
    }else{
      slog("Ignoring an invalid getSkill parameter from $user ($data)",2);
      next;
    }

    if($notAHost) {
      push(@returnParams,"$accountId|1");
      next;
    }
    if(! exists $lobby->{accounts}->{$accountId}) {
      slog("Unable to find ID \"$accountId\" in lobby (getSkill call from $user)",2);
      push(@returnParams,"$accountId|1");
      next;
    }
    my $playerName=$lobby->{accounts}->{$accountId};
    my $quotedPlayerName=quotemeta($playerName);
    if(! grep {/^$quotedPlayerName$/} @{$lobby->{battles}->{$hostBattles{$user}}->{userList}}) {
      slog("Unable to find player \"$playerName\" (ID $accountId) in battle (getSkill call from $user)",2);
      push(@returnParams,"$accountId|1");
      next;
    }

    if(! defined $quotedModShortName) {
      slog("Unable to find ratable mod matching host mod, getSkill call from $user",2);
      push(@returnParams,"$accountId|2");
      next;
    }

    my $p_skills=$sldb->getSkills($currentRatingPeriod,$accountId,$ip,$quotedModShortName);

    if(! %{$p_skills}) {
      slog("Unable to find skill of ID \"$accountId\" / IP \"$ip\", getSkill call from $user",2);
      push(@returnParams,"$accountId|2");
      $hostSkills{$user}->{$playerName}={ip => $ip,
                                         mod => $quotedModShortName,
                                         game => ''};
      next;
    }
    foreach my $k (keys %{$p_skills}) {
      $p_skills->{$k}->{mu}=formatFloat($p_skills->{$k}->{mu},2);
      $p_skills->{$k}->{sigma}=formatFloat($p_skills->{$k}->{sigma},2);
    }
    my $userPrivacyMode;
    $sldb->getUserPref($accountId,'privacyMode',\$userPrivacyMode);
    if($interfaceVersion == 3) {
      my $userId=$sldb->getUserId($accountId);
      my %skillClasses;
      foreach my $gameType (keys %gameTypeMapping) {
        if(exists $topPlayers{$gameType}->{$userId}) {
          $skillClasses{$gameType}=$topPlayers{$gameType}->{$userId};
        }else{
          $skillClasses{$gameType}=5;
        }
      }
      push(@returnParams,"$accountId|0|$userPrivacyMode|$p_skills->{Duel}->{mu},$p_skills->{Duel}->{sigma},$skillClasses{Duel}|$p_skills->{FFA}->{mu},$p_skills->{FFA}->{sigma},$skillClasses{FFA}|$p_skills->{Team}->{mu},$p_skills->{Team}->{sigma},$skillClasses{Team}|$p_skills->{TeamFFA}->{mu},$p_skills->{TeamFFA}->{sigma},$skillClasses{TeamFFA}");
    }else{
      push(@returnParams,"$accountId|0|$userPrivacyMode|$p_skills->{Duel}->{mu},$p_skills->{Duel}->{sigma}|$p_skills->{FFA}->{mu},$p_skills->{FFA}->{sigma}|$p_skills->{Team}->{mu},$p_skills->{Team}->{sigma}|$p_skills->{TeamFFA}->{mu},$p_skills->{TeamFFA}->{sigma}");
    }

    my $latestRatedGame=$sldb->getLatestRatedGameId($accountId,$quotedModShortName);
    $hostSkills{$user}->{$playerName}={ip => $ip,
                                       mod => $quotedModShortName,
                                       game => $latestRatedGame};
  }

  if(@returnParams) {
    my $returnParamsString=join(' ',@returnParams);
    sayPrivate($user,"!\#skill $returnParamsString");
  }
}

sub hHelp {
  my ($source,$user,$p_params)=@_;
  my ($cmd)=@{$p_params};

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  if(defined $cmd) {
    my $helpCommand=lc($cmd);
    $helpCommand=$1 if($helpCommand =~ /^!(.+)$/);
    if($helpCommand !~ /^\w+$/) {
      invalidSyntax($user,"help");
      return 0;
    }
    if(exists $botConf->{help}->{$helpCommand}) {

      my $p_help=$botConf->{help}->{$helpCommand};

      sayPrivate($user,"$B********** Help for command $C{12}$cmd$C{1} **********");
      sayPrivate($user,"$B$C{10}Syntax:");
      my $helpLine=$p_help->[0];
      $helpLine="$C{12}$1$C{5}$2$C{1}$3" if($helpLine =~ /^(!\w+)(.*)( - .*)$/);
      sayPrivate($user,'  '.$helpLine);
      sayPrivate($user,"$B$C{10}Example(s):") if($#{$p_help} > 0);
      for my $i (1..$#{$p_help}) {
        $helpLine=$p_help->[$i];
        $helpLine="\"$C{3}$1$C{1}\"$2" if($helpLine =~ /^\"([^\"]+)\"(.+)$/);
        sayPrivate($user,'  '.$helpLine);
      }

    }else{
      sayPrivate($user,"\"$C{12}$cmd$C{1}\" is not a valid command or setting.");
    }
  }else{

    my $level=getUserAccessLevel($user);
    my $p_helpForUser=$botConf->getHelpForLevel($level);

    sayPrivate($user,"$B********** Available commands for your access level **********");
    foreach my $i (0..$#{$p_helpForUser->{direct}}) {
      $p_helpForUser->{direct}->[$i]="$C{3}$1$C{5}$2$C{1}$3" if($p_helpForUser->{direct}->[$i] =~ /^(!\w+)(.*)( - .*)$/);
      sayPrivate($user,$p_helpForUser->{direct}->[$i]);
    }
  }

}

sub hHelpAll {
  my (undef,$user,undef)=@_;

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my $p_help=$botConf->{help};

  sayPrivate($user,"$B********** SldbLi commands **********");
  for my $command (sort (keys %{$p_help})) {
    next unless($command);
    my $helpLine=$p_help->{$command}->[0];
    $helpLine="$C{3}$1$C{5}$2$C{1}$3" if($helpLine =~ /^(!\w+)(.*)( - .*)$/);
    sayPrivate($user,$helpLine);
  }
}

sub hJoinAcc {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} < 1 || $#{$p_params} > 3) {
    invalidSyntax($user,'joinacc');
    return 0;
  }
  my ($sticky,$force,$test)=(0,0,0);
  my ($id1,$id2);
  foreach my $param (@{$p_params}) {
    if($param eq '-f') {
      $force=1;
    }elsif($param eq '-s') {
      $sticky=1;
    }elsif($param eq '-t') {
      $test=1;
    }else{
      if($param =~ /^\#(\d+)$/) {
        $param=$1;
      }else{
        my $userId=$sldb->getUserIdByName($param);
        if(defined $userId) {
          $param=$userId;
        }else{
          answer("Unable to join accounts, user \"$param\" is unknown!");
          return 0;
        }
      }
      if(! defined $id1) {
        $id1=$param;
      }elsif(! defined $id2) {
        $id2=$param;
      }else{
        invalidSyntax($user,'joinacc');
        return 0;
      }
    }
  }
  if(! defined $id2) {
    invalidSyntax($user,'joinacc');
    return 0;
  }

  foreach my $id ($id1,$id2) {
    my $idType=$sldb->getIdType($id);
    if($idType eq 'unknown') {
      answer("Unable to join accounts, ID $id is unknown!");
      return 0;
    }elsif($idType eq 'account') {
      answer("This command must be used with user IDs, $id is an account ID!");
      return 0;
    }
  }
  if($id1 == $id2) {
    answer("Can't join user ID $id1 with itself!");
    return 0;
  }
  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my $p_smurfStates=$sldb->getUsersSmurfStates($id1,$id2);
  if(exists $p_smurfStates->{1}) {
    answer("Aborting user account join, the users were already marked as smurfs, database is in inconsistent state and must be checked manually!");
    return 0;
  }
  if(! $force && exists $p_smurfStates->{0}) {
    my $conflicts='';
    foreach my $p_accs (@{$p_smurfStates->{0}}) {
      $conflicts.=" $p_accs->[0]|$p_accs->[1]";
    }
    answer("Aborting user account join, following not-smurf entries are conflicting:$conflicts (use -f to bypass)");
    return 0 unless($test);
  }

  my $p_simultaneousGames=$sldb->getSimultaneousUserGames($id1,$id2);
  if(! $force && @{$p_simultaneousGames}) {
    my @conflictData;
    foreach my $p_data (@{$p_simultaneousGames}) {
      push(@conflictData,{"$C{5}GameId" => "$C{12}$p_data->{gameId}$C{1}", AccountId1 => $p_data->{id1}, AccountId2 => $p_data->{id2}})
    }
    answer("Aborting user account join, following simultaneous plays are conflicting (use -f to bypass):");
    my $p_resultLines=formatArray(["$C{5}GameId",'AccountId1','AccountId2'],\@conflictData);
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
    sayPrivate($user,"$C{4}Result truncated to first 10 entries.") if($#conflictData == 9);
    return 0 unless($test);
  }

  if(%{$p_smurfStates} && ! $test) {
    if(exists $p_smurfStates->{0}) {
      foreach my $p_accs (@{$p_smurfStates->{0}}) {
        $sldb->adminEvent('DEL_NOT_SMURF',$p_accs->[2] > 1 ? 1 : 0,1,$lobby->{users}->{$user}->{accountId},{accountId1 => $p_accs->[0], accountId2 => $p_accs->[1]});
      }
      answer('Removed '.($#{$p_smurfStates->{0}}+1).' not-smurf entries!');
    }
    if(exists $p_smurfStates->{2}) {
      foreach my $p_accs (@{$p_smurfStates->{2}}) {
        $sldb->adminEvent('DEL_PROB_SMURF',0,1,$lobby->{users}->{$user}->{accountId},{accountId1 => $p_accs->[0], accountId2 => $p_accs->[1]});
      }
      answer('Removed '.($#{$p_smurfStates->{2}}+1).' probable smurf entries.');
    }
    $sldb->deleteUsersSmurfStates($id1,$id2);
  }

  my $mainUserId=$sldb->chooseMainUserId($id1,$id2);
  my $childUserId = $mainUserId == $id1 ? $id2 : $id1;

  if($test) {
    answer("--- END OF TEST ---");
    answer("TEST: join account $childUserId into account $mainUserId");
    return 1;
  }
  
  my $mergeStatus=1;
  $mergeStatus=2 if(exists $p_smurfStates->{2});
  $mergeStatus=0 if(exists $p_smurfStates->{0});
  $sldb->adminEvent('JOIN_ACC',$mergeStatus,1,$lobby->{users}->{$user}->{accountId},{mainUserId => $mainUserId, childUserId => $childUserId});
  my $p_oldUserSmurfs=$sldb->getUserAccounts($childUserId);
  foreach my $accountId (@{$p_oldUserSmurfs}) {
    $sldb->queueGlobalRerate($accountId);
  }
  $sldb->do("update userAccounts set userId=$mainUserId where userId=$childUserId","update data in table userAccounts for manual join of \"$childUserId\" to \"$mainUserId\"");
  if($sticky) {
    my ($firstId,$lastId)=$id1 < $id2 ? ($id1,$id2) : ($id2,$id1);
    $sldb->do("insert into smurfs values ($firstId,$lastId,1,$lobby->{users}->{$user}->{accountId})","add sticky smurf entry \"$firstId\" <-> \"$lastId\" into smurfs table");
  }

  my $message="Joined user account $childUserId into account $mainUserId";
  $message.=' (sticky)' if($sticky);
  answer($message);
}

sub genericLeaderboard {
  my ($source,$user,$lcCommand,$p_params)=@_;

  if($#{$p_params} < 0 || $#{$p_params} > 2) {
    invalidSyntax($user,$lcCommand);
    return 0;
  }
  my ($modShortName,$gameType,$nbPlayers)=@{$p_params};
  $gameType='Global' unless(defined $gameType);
  if((! defined $nbPlayers) && $gameType =~ /^([\-\d]\d*)$/) {
    $gameType='Global';
    $nbPlayers=$1;
  }

  if(! defined $nbPlayers) {
    $nbPlayers=20;
  }elsif($nbPlayers eq '-') {
    $nbPlayers='-20';
  }

  if($nbPlayers ne '20') {
    my $level=getUserAccessLevel($user);
    if($level < 120) {
      answer("You are not authorized to change $lcCommand size !");
      return 0;
    }
  }

  my $fixedGameType=$sldb->fixGameType($gameType);
  if(! defined $fixedGameType) {
    invalidSyntax($user,$lcCommand,'allowed game types: '.join(',',(keys %gameTypeMapping)));
    return 0;
  }
  $gameType=$fixedGameType;

  my $mode='top';
  if($nbPlayers =~ /^-(.*)$/) {
    $nbPlayers=$1;
    $mode='bottom';
  }
  if($nbPlayers !~ /^\d+$/) {
    invalidSyntax($user,$lcCommand);
    return 0;
  }
  $nbPlayers=100 if($nbPlayers > 100);

  my $fixedModShorName=$sldb->fixModShortName($modShortName);
  if(! defined $fixedModShorName) {
    my $p_allowedMods=$sldb->getModsShortNames();
    my $allowedModsString=join(',',@{$p_allowedMods});
    invalidSyntax($user,$lcCommand,"allowed games: $allowedModsString");
    return 0;
  }
  $modShortName=$fixedModShorName;

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my $currentRatingPeriod=$sldb->getCurrentRatingPeriod();
  
  my $skillMode='trusted';
  $skillMode='estimated' if($lcCommand eq 'topskill');
  my $p_lbData=$sldb->getLeaderboard($currentRatingPeriod,$modShortName,$gameType,$skillMode,$mode,$nbPlayers);

  my @resultData;
  for my $i (0..$#{$p_lbData}) {
    my $p_player=$p_lbData->[$i];
    my $coloredPenalties="$C{15}0";
    $coloredPenalties="$C{13}$p_player->{inactivity}" if($p_player->{inactivity} > 0);
    my ($trustedSkill,$estimatedSkill,$uncertainty)=map { formatFloat($_,2) } ($p_player->{trustedSkill},$p_player->{estimatedSkill},$p_player->{uncertainty});
    my $coloredUncertainty;
    if($uncertainty < 1) {
      $coloredUncertainty=$C{15};
    }elsif($uncertainty < 1.5) {
      $coloredUncertainty=$C{14};
    }elsif($uncertainty < 2) {
      $coloredUncertainty=$C{6};
    }elsif($uncertainty < 2.5) {
      $coloredUncertainty=$C{7};
    }elsif($uncertainty < 3) {
      $coloredUncertainty=$C{4};
    }else{
      $coloredUncertainty=$C{13};
    }
    $coloredUncertainty.=$uncertainty;
    push(@resultData,{"$C{5}Rank" => "$C{12}".($i+1),
                      UserId => "$C{14}$p_player->{userId}",
                      Name => "$C{2}$p_player->{name}",
                      Inactivity => $coloredPenalties,
                      TrustedSkill => "$C{3}$trustedSkill",
                      EstimatedSkill => "$C{10}$estimatedSkill",
                      Uncertainty => $coloredUncertainty});
  }
  if(@resultData) {
    my $title='Top';
    $title='Worst' if($mode eq 'bottom');
    $title.=" $nbPlayers $modShortName";
    $title.=" $gameType" if($gameType ne 'Global');
    $title.=' player';
    $title.='s' if($nbPlayers > 1);
    $title="$C{2}$title sorted by ";
    if($skillMode eq 'trusted') {
      $title.="TrustedSkill$C{1}";
    }else{
      $title.="EstimatedSkill$C{1}";
    }
    my $p_resultLines=formatArray(["$C{5}Rank",'UserId','Name','Inactivity','TrustedSkill','EstimatedSkill','Uncertainty'],\@resultData,$title);
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
  }else{
    answer("No result!");
  }
}

sub hLeaderboard {
  my ($source,$user,$p_params)=@_;
  return genericLeaderboard($source,$user,'leaderboard',$p_params);
}


sub hNotSmurf {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} != 1) {
    invalidSyntax($user,'notsmurf');
    return 0;
  }
  my ($id1,$id2,$userId1,$userId2);
  foreach my $param (@{$p_params}) {
    my $id;
    if($param =~ /^\#(\d+)$/) {
      $id=$1;
    }else{
      $id=$sldb->identifyUniqueAccountByString($param);
      if(! defined $id) {
        answer("Unable to identify any account matching \"$param\" !");
      }elsif($id == -1) {
        answer("Multiple account names match \"$param\", use more specific string or use account ID instead.");
      }elsif($id == -2) {
        answer("Multiple user names contain \"$param\", use more specific string or use account ID instead.");
      }elsif($id == -3) {
        answer("Multiple account names contain \"$param\", use more specific string or use account ID instead.");
      }
      return 0 unless(defined $id && $id > 0);
    }
    my $userId=$sldb->getUserId($id);
    if(! defined $userId) {
      answer("Unable to mark as not-smurf, ID $id is unknown!");
      return 0;
    }
    if(defined $id1) {
      $id2=$id;
      $userId2=$userId;
    }else{
      $id1=$id;
      $userId1=$userId;
    }
  }
  if($userId1 == $userId2) {
    answer("Accounts $id1 and $id2 belong to same user ($userId1), use !splitAcc command instead if you want to split an account from a user!");
    return 0;
  }

  my $p_smurfState=$sldb->getAccountsSmurfState($id1,$id2);
  if(@{$p_smurfState}) {
    if($p_smurfState->[0] == 0) {
      answer("Accounts $id1 and $id2 are already marked as not-smurf!");
      return 0;
    }elsif($p_smurfState->[0] == 1) {
      answer("Accounts $id1 and $id2 are marked as smurf manually, database is in inconsistent state and must be checked manually!");
      return 0;
    }
  }else{
    answer("Accounts $id1 and $id2 aren't marked as probable smurfs, aborting operation.");
    return 0;
  }

  my ($firstId,$lastId)=$id1 < $id2 ? ($id1,$id2) : ($id2,$id1);
  $sldb->adminEvent('DEL_PROB_SMURF',3,1,$lobby->{users}->{$user}->{accountId},{accountId1 => $firstId, accountId2 => $lastId});
  $sldb->deleteAccountsSmurfState($firstId,$lastId);
  $sldb->adminEvent('ADD_NOT_SMURF',3,1,$lobby->{users}->{$user}->{accountId},{accountId1 => $firstId, accountId2 => $lastId});
  $sldb->do("insert into smurfs values ($firstId,$lastId,0,$lobby->{users}->{$user}->{accountId})","add not-smurf entry \"$firstId\" <-> \"$lastId\" into smurfs table");
  answer("Accounts $id1 and $id2 are now marked as not-smurf.");
}

sub hQuit {
  my ($source,$user)=@_;
  my %sourceNames = ( pv => 'private',
                      chan => "channel #$masterChannel" );
  scheduleQuit("requested by $user in $sourceNames{$source}");
}

sub hRanking {
  my ($source,$user,$p_params)=@_;

  if($#{$p_params} > 0) {
    invalidSyntax($user,'ranking');
    return 0;
  }

  my $accountString;
  if(@{$p_params}) {
    my $level=getUserAccessLevel($user);
    if($level < 120) {
      answer("You are not authorized to query other players' ratings !");
      return 0;
    }
    $accountString=$p_params->[0];
  }else{
    $accountString='#'.$lobby->{users}->{$user}->{accountId};
  }

  my $sth;
  my @results;

  my $accountId;
  if($accountString =~ /^\#(\d+)$/) {
    $accountId=$1;
  }else{
    $accountId=$sldb->identifyUniqueAccountByStringUserFirst($accountString);
    if(! defined $accountId) {
      answer("No account found for search string \"$accountString\" !");
    }elsif($accountId == -1) {
      answer("Multiple account names match your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }elsif($accountId == -2) {
      answer("Multiple user names contain your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }elsif($accountId == -3) {
      answer("Multiple account names contain your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }
    return 0 unless(defined $accountId && $accountId > 0);
  }

  $sth=$sldb->prepExec("select ua.userId,ud.name from userAccounts ua,userDetails ud where ua.accountId=$accountId and ua.userId=ud.userId");
  @results=$sth->fetchrow_array();
  if(! @results) {
    answer("Unknown account ID $accountId !");
    return 0;
  }
  my ($userId,$userName)=@results;

  my $currentRatingPeriod=$sldb->getCurrentRatingPeriod();
  my $previousRatingPeriod=previousRatingPeriod();
  my %ratings;
  my %prevRatings;

  my @ratedMods;
  $sth=$sldb->prepExec("select modShortName from tsPlayers where period=$currentRatingPeriod and userId=$userId");
  while(@results=$sth->fetchrow_array()) {
    push(@ratedMods,$results[0]);
  }

  foreach my $mod (@ratedMods) {
    my $quotedMod=$sldb->quote($mod);
    $ratings{$mod}={};
    $prevRatings{$mod}={};
    for my $gameType (keys %gameTypeMapping) {
      my $gType=$gameTypeMapping{$gameType};
      $sldb->do("set \@rank=0");
      $sth=$sldb->prepExec("select ranks.rank,tsp.skill,tsp.mu,tsp.sigma,tsp.nbPenalties from ts${gType}Players tsp,(select userId,\@rank:=\@rank+1 rank from ts${gType}Players where period=$currentRatingPeriod and modShortName=$quotedMod order by skill desc) ranks where tsp.period=$currentRatingPeriod and tsp.userId=$userId and tsp.modShortName=$quotedMod and ranks.userId=$userId");
      while(@results=$sth->fetchrow_array()) {
        my ($rank,$skill,$mu,$sigma,$nbPen)=map { formatFloat($_,2) } @results;
        $ratings{$mod}->{$gameType}={rank => $rank, skill => $skill, mu => $mu, sigma => $sigma, nbPen => $nbPen};
      }
      $sldb->do("set \@rank=0");
      $sth=$sldb->prepExec("select ranks.rank,tsp.skill,tsp.mu,tsp.sigma,tsp.nbPenalties from ts${gType}Players tsp,(select userId,\@rank:=\@rank+1 rank from ts${gType}Players where period=$previousRatingPeriod and modShortName=$quotedMod order by skill desc) ranks where tsp.period=$previousRatingPeriod and tsp.userId=$userId and tsp.modShortName=$quotedMod and ranks.userId=$userId");
      while(@results=$sth->fetchrow_array()) {
        my ($rank,$skill,$mu,$sigma,$nbPen)=map { formatFloat($_,2) } @results;
        $prevRatings{$mod}->{$gameType}={rank => $rank, skill => $skill, mu => $mu, sigma => $sigma, nbPen => $nbPen};
      }
    }
  }

  if(! %ratings) {
    answer("No ranking data available for user $userName");
    return 0;
  }

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  foreach my $mod (sort keys %ratings) {
    my @rankingData;
    foreach my $gt (qw/Duel Team FFA TeamFFA Global/) {
      next if($ratings{$mod}->{$gt}->{sigma} > 7);
      my %fRatings=map { $_ => formatFloat($ratings{$mod}->{$gt}->{$_},2) } (keys %{$ratings{$mod}->{$gt}});
      my $coloredUncertainty;
      if($ratings{$mod}->{$gt}->{sigma} < 1) {
        $coloredUncertainty=$C{15};
      }elsif($ratings{$mod}->{$gt}->{sigma} < 1.5) {
        $coloredUncertainty=$C{14};
      }elsif($ratings{$mod}->{$gt}->{sigma} < 2) {
        $coloredUncertainty=$C{6};
      }elsif($ratings{$mod}->{$gt}->{sigma} < 2.5) {
        $coloredUncertainty=$C{7};
      }elsif($ratings{$mod}->{$gt}->{sigma} < 3) {
        $coloredUncertainty=$C{4};
      }else{
        $coloredUncertainty=$C{13};
      }
      if(exists $prevRatings{$mod}->{$gt}) {
        foreach my $k (keys %fRatings) {
          my $diffRating=$ratings{$mod}->{$gt}->{$k}-$prevRatings{$mod}->{$gt}->{$k};
          my ($parentColor,$diffColor)=($C{1},$C{14});
          ($parentColor,$diffColor)=($C{15},$C{15}) if($k eq 'sigma' && $coloredUncertainty eq $C{15});
          if($diffRating > 0) {
            if($k eq 'rank' || $k eq 'nbPen') {
              $diffColor=$C{4};
            }elsif($k eq 'skill' || $k eq 'mu') {
              $diffColor=$C{3};
            }
            $fRatings{$k}.=" $parentColor($diffColor+".formatFloat($diffRating,2)."$parentColor)";
          }elsif($diffRating < 0) {
            if($k eq 'rank' || $k eq 'nbPen') {
              $diffColor=$C{3};
            }elsif($k eq 'skill' || $k eq 'mu') {
              $diffColor=$C{4};
            }
            $fRatings{$k}.=" $parentColor($diffColor".formatFloat($diffRating,2)."$parentColor)";
          }
        }
      }
      $coloredUncertainty.=$fRatings{sigma};
      push(@rankingData,{ "$C{5}GameType" => $gt,
                          Rank => "$C{12}$fRatings{rank}",
                          Inactivity => $fRatings{nbPen} == 0 ? "$C{15}$fRatings{nbPen}" : "$C{13}$fRatings{nbPen}",
                          TrustedSkill => "$C{10}$fRatings{skill}",
                          EstimatedSkill => "$C{1}$fRatings{mu}",
                          Uncertainty => $coloredUncertainty });
    }
    next unless(@rankingData);
    my $p_resultLines=formatArray(["$C{5}GameType",'Rank','Inactivity','TrustedSkill','EstimatedSkill','Uncertainty'],\@rankingData,"$C{2}$mod$C{1} ranking for $C{12}$userName$C{1} ($C{14}$userId$C{1})");
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
  }
}

sub hReloadConf {
  my ($source,$user)=@_;

  my $newSldbLi=SldbLiConf->new($confFile,$sLog);
  if(! $newSldbLi) {
    answer("Unable to reload SldbLi configuration");
    return 0;
  }

  $botConf=$newSldbLi;
  %conf=%{$botConf->{conf}};

  answer('SldbLi configuration reloaded');
}

sub hRestart {
  my ($source,$user)=@_;
   my %sourceNames = ( pv => "private",
                       chan => "channel #$conf{masterChannel}");
  scheduleQuit("requested by $user in $sourceNames{$source}",2);
}

sub hSearchUser {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} < 0) {
    invalidSyntax($user,'searchuser');
    return 0;
  }

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  my $sth;
  my @results;
  my @userIds;
  my %accountIds;

  my ($search,$quotedSearch);
  if($#{$p_params}==0 && $p_params->[0] =~ /^\@(\d{1,3}(?:\.\d{1,3}){3})$/) {
    $search=$1;
    $quotedSearch=$sldb->quote($search);
    $sth=$sldb->prepExec("select ua.userId,ua.accountId,UNIX_TIMESTAMP(ips.lastSeen) from userAccounts ua,ips where ua.accountId=ips.accountId and ips.ip=INET_ATON($quotedSearch)","retrieve users matching IP address $search frim ips table");
    my %userIdsTs;
    while(@results=$sth->fetchrow_array()) {
      $userIdsTs{$results[0]}=$results[2] unless(exists $userIdsTs{$results[0]} && $userIdsTs{$results[0]} >= $results[2]);
      $accountIds{$results[1]}=1;
    }
    $sth=$sldb->prepExec("select ua.userId,ua.accountId,UNIX_TIMESTAMP(ipr.lastSeen) from userAccounts ua,ipRanges ipr where ua.accountId=ipr.accountId and ipr.ip1 <= INET_ATON($quotedSearch) and ipr.ip2 >= INET_ATON($quotedSearch)","retrieve dynamic IP users matching IP address $search from ipRanges table");
    while(@results=$sth->fetchrow_array()) {
      $userIdsTs{$results[0]}=$results[2] unless(exists $userIdsTs{$results[0]} && $userIdsTs{$results[0]} >= $results[2]);
      $accountIds{$results[1]}=1;
    }
    @userIds=sort {$userIdsTs{$b} <=> $userIdsTs{$b}} (keys %userIdsTs);
  }elsif($#{$p_params}==0 && $p_params->[0] =~ /^\&(\-?\d+)$/) {
    $search=$1;
    $sth=$sldb->prepExec("select ua.userId,ua.accountId,UNIX_TIMESTAMP(hw.lastConnection) from userAccounts ua,hardwareIds hw where ua.accountId=hw.accountId and hw.hardwareId=$search order by hw.lastConnection desc limit 1000","retrieve users with accounts matching hardwareId \"$search\" from userAccounts and hardwareIds [!searchUser]");
    my %userIdsTs;
    while(@results=$sth->fetchrow_array()) {
      $userIdsTs{$results[0]}=$results[2] unless(exists $userIdsTs{$results[0]} && $userIdsTs{$results[0]} >= $results[2]);
      $accountIds{$results[1]}=1;
    }
    @userIds=sort {$userIdsTs{$b} <=> $userIdsTs{$b}} (keys %userIdsTs);
  }else{
    if($#{$p_params}==0) {
      $search=$p_params->[0];
      $quotedSearch=$sldb->quote($search);
      my $exactMatch=$sldb->getUserIdByName($search);
      my $alreadyFoundString='';
      if(defined $exactMatch) {
        push(@userIds,$exactMatch);
        $alreadyFoundString=" and ua.userId != $exactMatch";
      }
      $sth=$sldb->prepExec("select ua.userId from userAccounts ua,names n where ua.accountId=n.accountId and n.name=$quotedSearch$alreadyFoundString group by ua.userId order by max(n.lastConnection) desc limit 200","retrieve users with accounts matching exactly $quotedSearch from userAccounts and names [!searchUser]");
      while(@results=$sth->fetchrow_array()) {
        push(@userIds,$results[0]);
      }
    }else{
      $search=join('%',@{$p_params});
    }
    $search="\%$search\%";
    $quotedSearch=$sldb->quote($search);
    my $alreadyFoundString='';
    $alreadyFoundString=' and ud.userId not in ('.join(',',@userIds).')' if(@userIds);
    $sth=$sldb->prepExec("select ud.userId from userDetails ud,userAccounts ua,accounts a where ud.name like $quotedSearch$alreadyFoundString and ud.userId=ua.userId and ua.userId=a.id group by ud.userId order by a.lastUpdate desc limit 200","retrieve users whose name matches $quotedSearch from userDetails [!searchUser]");
    while(@results=$sth->fetchrow_array()) {
      push(@userIds,$results[0]);
    }
    $alreadyFoundString=' and ua.userId not in ('.join(',',@userIds).')' if(@userIds);
    $sth=$sldb->prepExec("select ua.userId from userAccounts ua,names n where ua.accountId=n.accountId and n.name like $quotedSearch$alreadyFoundString group by ua.userId order by max(n.lastConnection) desc limit 200","retrieve users with accounts matching $quotedSearch from userAccounts and names [!searchUser]");
    while(@results=$sth->fetchrow_array()) {
      push(@userIds,$results[0]);
    }
  }

  my @searchResults;
  foreach my $userId (@userIds) {
    $sth=$sldb->prepExec("select ud.name,ua.accountId,UNIX_TIMESTAMP(a.lastUpdate),coalesce(rtp.inGame,-1),a.rank,c.country
  from userDetails ud,
       accounts a,
       countries c join (select c1.accountId,max(lastConnection) maxLastConn from countries c1,userAccounts ua1 where userId=$userId and c1.accountId=ua1.accountId group by c1.accountId) lastCountries
         on c.lastConnection=lastCountries.maxLastConn and c.accountId=lastCountries.accountId,
       userAccounts ua left join rtPlayers rtp on ua.accountId=rtp.accountId
       where ua.userId=$userId
             and ud.userId=$userId
             and ua.accountId=a.id
             and ua.accountId=c.accountId
       group by ua.accountId
       order by a.lastUpdate desc limit 200","query accounts data for user $userId [!searchUser]");
    my $firstResult=1;
    while(@results=$sth->fetchrow_array()) {
      my $accountId=$results[1];
      my @names;
      my $sth2=$sldb->prepExec("select name from names where accountId=$accountId order by lastConnection desc");
      my @nameResult;
      while(@nameResult=$sth2->fetchrow_array()) {
        push(@names,$nameResult[0]);
      }
      if($accountId == $userId) {
        if(exists $accountIds{$accountId}) {
          $accountId="* $accountId";
        }else{
          $accountId="  $accountId";
        }
        $accountId="$C{1}$accountId";
      }else{
        if(exists $accountIds{$accountId}) {
          $accountId="* $accountId";
        }else{
          $accountId="  $accountId";
        }
        $accountId="$C{15}$accountId";
      }
      my $country=(split(',',$results[5]))[0];
      my $accountActivity;
      if($results[3] == -1) {
        my $accountAge=time-$results[2];
        $accountActivity=secToDayAge($accountAge);
        if($accountAge > 7776000) {
          $accountActivity="$C{15}$accountActivity";
        }elsif($accountAge > 2592000) {
          $accountActivity="$C{14}$accountActivity";
        }else{
          $accountActivity="$C{1}$accountActivity";
        }
      }elsif($results[3] == 0) {
        $accountActivity="$C{3}Online";
      }else{
        $accountActivity="$C{4}Ingame";
      }
      my %rankColors=(0 => $C{15}, 1 => $C{14}, 2 => $C{1}, 3 => $C{1}, 4 => $C{1}, 5 => $C{6}, 6 => $C{7}, 7 => $C{13});
      my %accountData=(AccountId => $accountId,
                       Country => $country,
                       LastActivity => $accountActivity,
                       Rank => $rankColors{$results[4]}.$results[4],
                       AccountNames => "$C{1}".join(',',@names) );
      if($firstResult) {
        $firstResult=0;
        $accountData{"$C{5}UserName"}="$C{2}$results[0]";
      }
      push(@searchResults,\%accountData);
      last if($#searchResults == 200);
    }
    last if($#searchResults == 200);
  }
  
  if(@searchResults) {
    my $p_resultLines=formatArray(["$C{5}UserName",'AccountId','Country','LastActivity','Rank','AccountNames'],\@searchResults,"$C{2}User search result$C{1}");
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
    sayPrivate($user,"$C{4}Result truncated to first 200 entries.") if($#searchResults == 200);
  }else{
    answer("No result!");
  }
}

sub hSendLobby {
  my ($source,$user,$p_params,)=@_;

  if($#{$p_params} < 0) {
    invalidSyntax($user,"sendlobby");
    return 0;
  }

  if($lobbyState < 4) {
    answer("Unable to send data to lobby server, not connected");
    return 0;
  }

  sendLobbyCommand([$p_params]);

  return 1;
}

sub hSet {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} < 0 || $#{$p_params} >  1) {
    invalidSyntax($user,'set');
    return 0;
  }
  my ($pref,$val)=@{$p_params};
  my $res=$sldb->setAccountPref($lobby->{users}->{$user}->{accountId},$pref,$val);
  $res=$sldb->setUserPref($lobby->{users}->{$user}->{accountId},$pref,$val) if($res == 0);
  if($res == 0) {
    answer("Invalid setting \"$pref\"");
    return 0;
  }elsif($res == -1) {
    answer("Invalid value \"$val\" for $pref setting");
    return 0;
  }elsif($res == 1) {
    answer("Done. ($pref=$val)"); 
  }else{
    answer("Done. ($pref reset to default value)");
  }
}

sub hSetName {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} != 1) {
    invalidSyntax($user,'setname');
    return 0;
  }
  my ($oldUser,$newUser)=@{$p_params};
  my $oldUserId;

  my $sth;
  my @results;
  if($oldUser =~ /^\#(\d+)$/) {
    $oldUserId=$1;
    my $idType=$sldb->getIdType($oldUserId);
    if($idType eq 'unknown') {
      answer("Unknown user ID $oldUserId !");
      return 0;
    }elsif($idType eq 'account') {
      answer("You must use the user ID to rename a user, $oldUserId is an account ID");
      return 0;
    }
  }else{
    my $userByNameRes=$sldb->getUserIdByName($oldUser);
    if(! defined $userByNameRes) {
      answer("Unknown user \"$oldUser\" !");
      return 0;
    }
    $oldUserId=$userByNameRes;
  }

  my $quotedNewUser=$sldb->quote($newUser);
  $sth=$sldb->prepExec("select ud.userId,ua.userId from userDetails ud,userAccounts ua where ud.userId=ua.accountId and ud.name=$quotedNewUser");
  @results=$sth->fetchrow_array();
  if(@results) {
    my ($nameUserId,$realUserId)=@results;
    if($nameUserId == $realUserId) {
      answer("Name $quotedNewUser is already taken by user ID $results[0] !");
      return 0;
    }else{
      my $availableNewUser=$sldb->quote($sldb->findAvailableUserName($newUser));
      $sldb->do("update userDetails set name=$availableNewUser where userId=$nameUserId");
      slog("Renamed unused user \#$nameUserId from $quotedNewUser to $availableNewUser to use his name for manual rename of user \#$oldUserId",2);
    }
  }

  my $oldUserName;
  $sth=$sldb->prepExec("select name from userDetails where userId=$oldUserId");
  @results=$sth->fetchrow_array();
  if(@results) {
    $oldUserName=$results[0];
  }else{
    answer("Logic error while processing rename!");
    return 0;
  }

  my $admEvt=$sldb->adminEvent('UPD_USERDETAILS',0,1,$lobby->{users}->{$user}->{accountId},{updatedUserId => $oldUserId, updatedParam => 'name', oldValue => $oldUserName, newValue => $newUser});
  if($admEvt > 0) {
    $sldb->do("update userDetails set name=$quotedNewUser where userId=$oldUserId");
    answer("Renamed user \"$oldUserName\" ($oldUserId) to \"$newUser\" (admin event: $admEvt)");
  }else{
    answer("Unable to log action in admin event table, rename cancelled!");
    return 0;
  }
}

sub hSplitAcc {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} < 1 || $#{$p_params} > 3) {
    invalidSyntax($user,'splitacc');
    return 0;
  }
  my ($sticky,$force,$test)=(0,0,0);
  my ($userId,$accountId);
  foreach my $param (@{$p_params}) {
    if($param eq '-f') {
      $force=1;
    }elsif($param eq '-s') {
      $sticky=1;
    }elsif($param eq '-t') {
      $test=1;
    }else{
      if(! defined $userId) {
        if($param =~ /^\#(\d+)$/) {
          $userId=$1;
          my $idType=$sldb->getIdType($userId);
          if($idType eq 'unknown') {
            answer("Unable to split accounts, ID $userId is unknown!");
            return 0;
          }elsif($idType eq 'account') {
            answer("Unable to split accounts, ID $userId is an account ID, splitAcc command expects a userId as first ID!");
            return 0;
          }
        }else{
          $userId=$sldb->getUserIdByName($param);
          if(! defined $userId) {
            answer("Unable to split accounts, user \"$param\" is unknown!");
            return 0;
          }
        }
      }elsif(! defined $accountId) {
        if($param =~ /^\#(\d+)$/) {
          $accountId=$1;
          my $idType=$sldb->getIdType($accountId);
          if($idType eq 'unknown') {
            answer("Unable to split accounts, ID $accountId is unknown!");
            return 0;
          }elsif($idType eq 'user') {
            answer("Unable to split accounts, ID $accountId is a user ID whereas splitAcc command expects an accountId as second ID!");
            return 0;
          }
          my $p_userAccounts=$sldb->getUserAccounts($userId);
          if(! grep {/^$accountId$/} @{$p_userAccounts}) {
            answer("Unable to split accounts, account ID $accountId is not linked to user ID $userId !");
            return 0;
          }
        }else{
          my $p_smurfIds=$sldb->getUserSmurfIdsByName($userId,$param);
          if(! @{$p_smurfIds}) {
            answer("Unable to split accounts, \"$param\" does not match any alternate account of user ID $userId !");
            return 0;
          }
          if($#{$p_smurfIds} > 0) {
            answer("Unable to split accounts, \"$param\" is ambiguous, use account ID to identify the exact account instead !");
            return 0;
          }
          $accountId=$p_smurfIds->[0];
        }
      }else{
        invalidSyntax($user,'splitacc');
        return 0;
      }
    }
  }
  if(! defined $accountId) {
    invalidSyntax($user,'splitacc');
    return 0;
  }

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};

  my $p_smurfState=$sldb->getAccountsSmurfState($userId,$accountId);
  if(@{$p_smurfState}) {
    if($p_smurfState->[0] == 0) {
      answer("Aborting user account split, the accounts were already marked as not-smurf, database is in inconsistent state and must be checked manually!");
      return 0;
    }elsif($p_smurfState->[0] == 2) {
      answer("Aborting user account split, the accounts were marked as probable smurf whereas they were joined, database is in inconsistent state and must be checked manually!");
      return 0;
    }else{
      if(! $force) {
        answer("Aborting user account split, the accounts were marked as smurf manually (use -f to bypass)");
        return 0 unless($test);
      }
      $sldb->deleteAccountsSmurfState($userId,$accountId) unless($test);
    }
  }

  if($test) {
    answer("--- END OF TEST ---");
    answer("TEST: detach account ID $accountId from user ID $userId");
    return 1;
  }
  my (undef,$p_smurfGroups)=$sldb->getUserOrderedSmurfGroups($userId,[$userId,$accountId],512);
  my $newUserId=$sldb->chooseMainAccountId($p_smurfGroups->[1]);
  my $smurfGroupString=join(',',@{$p_smurfGroups->[1]});
  foreach my $id (@{$p_smurfGroups->[1]}) {
    my $subType=0;
    $subType=1 if($newUserId!=$id);
    $sldb->adminEvent('SPLIT_ACC',$subType,1,$lobby->{users}->{$user}->{accountId},{oldUserId => $userId, newUserId => $newUserId, accountId => $id});
    $sldb->queueGlobalRerate($id);
  }
  $sldb->do("update userAccounts set userId=$newUserId where accountId in ($smurfGroupString)","update userAccounts for new main account \"$newUserId\" for smurf group \"$smurfGroupString\"");
  if($sticky) {
    my ($firstId,$lastId)=$userId < $accountId ? ($userId,$accountId) : ($accountId,$userId);
    $sldb->adminEvent('ADD_NOT_SMURF',0,1,$lobby->{users}->{$user}->{accountId},{accountId1 => $firstId, accountId2 => $lastId});
    $sldb->do("insert into smurfs values ($firstId,$lastId,0,$lobby->{users}->{$user}->{accountId})","add not-smurf entry \"$firstId\" <-> \"$lastId\" into smurfs table");
  }  

   my $message="Detached user ID $newUserId (account ID(s): $smurfGroupString) from user ID $userId";
   $message.=' (sticky)' if($sticky);
   answer($message);
}

sub hTopSkill {
  my ($source,$user,$p_params)=@_;
  return genericLeaderboard($source,$user,'topskill',$p_params);
}

sub hVersion {
  my (undef,$user,undef)=@_;

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};
  
  sayPrivate($user,"$C{12}$conf{lobbyLogin}$C{1} is running ${B}$C{5}SldbLi $C{10}v$sldbLiVer$B$C{1}, with following components:");
  sayPrivate($user,"- $C{5}Perl$C{10} $^V");
  my %components = (SpringLobbyInterface => $lobby,
                    SldbLiConf => $botConf,
                    SimpleLog => $sLog,
                    SLDB => $sldb);
  foreach my $module (keys %components) {
    my $ver=$components{$module}->getVersion();
    sayPrivate($user,"- $C{5}$module$C{10} v$ver");
  }

}
sub hCheckIps {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} != 0) {
    invalidSyntax($user,'whois');
    return 0;
  }

  my $accountId;
  my $accountString=$p_params->[0];
  if($accountString =~ /^\#(\d+)$/) {
    $accountId=$1;
  }else{
    $accountId=$sldb->identifyUniqueAccountByString($accountString);
    if(! defined $accountId) {
      answer("No account found for search string \"$accountString\" !");
    }elsif($accountId == -1) {
      answer("Multiple account names match \"$accountString\"");
    }elsif($accountId == -2) {
      answer("Multiple user names contain \"$accountString\"");
    }elsif($accountId == -3) {
      answer("Multiple account names contain \"$accountString\"");
    }
    return 0 unless(defined $accountId && $accountId > 0);
  }

  if($sldb->getIdType($accountId) eq 'unknown') {
    answer("Unknown account ID $accountId!");
    return 0;
  }

  my $p_ips=$sldb->getAccountIps($accountId);
  if(! %{$p_ips}) {
    answer("No IP found for account ID $accountId.");
    return 0;
  }

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};

  my @ipData;
  foreach my $ip (sort {$a <=> $b} (keys %{$p_ips})) {
    my $ipVal=intToIp($ip);
    if(exists $p_ips->{$ip}->{ip2}) {
      my $ip2Val=intToIp($p_ips->{$ip}->{ip2});
      $ipVal.=" - $ip2Val";
    }
    push(@ipData,{"$C{5}IP" => $ipVal, LastSeen => $p_ips->{$ip}->{lastSeen}});
  }

  my $p_resultLines=formatArray(["$C{5}IP",'LastSeen'],\@ipData,"$C{2}In-game IPs for account \#$accountId");
  sayPrivate($user,'.');
  foreach my $resultLine (@{$p_resultLines}) {
    sayPrivate($user,$resultLine);
  }

}

sub hWhois {
  my ($source,$user,$p_params)=@_;
  if($#{$p_params} != 0 && $#{$p_params} != 1) {
    invalidSyntax($user,'whois');
    return 0;
  }
  my $maxSize=20;
  if($#{$p_params} == 1) {
    my $option=shift(@{$p_params});
    if($option ne '-f') {
      invalidSyntax($user,'whois');
      return 0;
    }else{
      $maxSize=200;
    }
  }

  my $sth;
  my @results;

  my $accountId;
  my $accountString=$p_params->[0];
  if($accountString =~ /^\#(\d+)$/) {
    $accountId=$1;
  }else{
    $accountId=$sldb->identifyUniqueAccountByString($accountString);
    if(! defined $accountId) {
      answer("No account found for search string \"$accountString\" !");
    }elsif($accountId == -1) {
      answer("Multiple account names match your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }elsif($accountId == -2) {
      answer("Multiple user names contain your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }elsif($accountId == -3) {
      answer("Multiple account names contain your search string \"$accountString\", use more specific search string or use !searchUser command instead");
    }
    return 0 unless(defined $accountId && $accountId > 0);
  }

  $sth=$sldb->prepExec("select ua.userId,ua.nbIps,ud.name,a.rank,UNIX_TIMESTAMP(a.lastUpdate),coalesce(rtp.inGame,-1) from userAccounts ua left join rtPlayers rtp on ua.accountId=rtp.accountId,userDetails ud,accounts a where ua.accountId=$accountId and a.id=$accountId and ua.userId=ud.userId");
  @results=$sth->fetchrow_array();
  if(! @results) {
    answer("Unknown account ID $accountId !");
    return 0;
  }
  my ($userId,$nbIp,$userName,$rank,$lastActivity,$inGame)=@results;

  my @names;
  $sth=$sldb->prepExec("select name from names where accountId=$accountId order by lastConnection desc");
  while(@results=$sth->fetchrow_array()) {
    push(@names,$results[0]);
  }
  my @countries;
  $sth=$sldb->prepExec("select country from countries where accountId=$accountId order by lastConnection desc");
  while(@results=$sth->fetchrow_array()) {
    push(@countries,$results[0]);
  }
  my @cpus;
  $sth=$sldb->prepExec("select cpu from cpus where accountId=$accountId order by lastConnection desc");
  while(@results=$sth->fetchrow_array()) {
    push(@cpus,$results[0]);
  }
  my @hardwareIds;
  $sth=$sldb->prepExec("select hardwareId from hardwareIds where accountId=$accountId order by lastConnection desc");
  while(@results=$sth->fetchrow_array()) {
    push(@hardwareIds,$results[0]);
  }

  my ($name,$country,$cpu,$hardwareId)=(shift @names,shift @countries,shift @cpus,shift @hardwareIds);
  $hardwareId='?' unless(defined $hardwareId);

  my ($p_C,$B)=initUserIrcColors($user);
  my %C=%{$p_C};

  my %statusMapping=( -1 => "$C{1}Offline",
                      0 => "$C{3}Online",
                      1 => "$C{4}Ingame" );
  my $accountActivity;
  my $accountAge=time-$lastActivity;
  $accountActivity=secToDayAge($accountAge);
  if($accountAge > 7776000) {
    $accountActivity="$C{15}$accountActivity";
  }elsif($accountAge > 2592000) {
    $accountActivity="$C{14}$accountActivity";
  }else{
    $accountActivity="$C{1}$accountActivity";
  }
  my %rankColors=(0 => $C{15}, 1 => $C{14}, 2 => $C{1}, 3 => $C{1}, 4 => $C{1}, 5 => $C{6}, 6 => $C{7}, 7 => $C{13});
  my %whoisData = ( "$C{14}UserName" => "$C{14}$userName",
                    UserId => "$C{14}$userId",
                    "$C{5}AccountName" => "$C{2}$name",
                    AccountId => "$C{14}$accountId",
                    Status => $statusMapping{$inGame},
                    Country => "$C{1}$country",
                    CPU => $cpu,
                    HardwareId => $hardwareId,
                    Rank => $rankColors{$rank}.$rank,
                    LastActivity => $accountActivity,
                    IpMode => $nbIp == 30 ? "$C{4}Dynamic" : "$C{1}Static($nbIp)" );

  my $p_resultLines=formatArray(["$C{14}UserName",'UserId',"$C{5}AccountName",'AccountId','Status','Country','CPU','HardwareId','Rank','LastActivity','IpMode'],[\%whoisData],"$C{2}Account Information$C{1}");
  sayPrivate($user,'.');
  foreach my $resultLine (@{$p_resultLines}) {
    sayPrivate($user,$resultLine);
  }

  my @historicData;
  push(@historicData,{ "$C{5}Info" => "$C{10}Name$C{1}", PreviousValues => join(',',@names) }) if(@names);
  push(@historicData,{ "$C{5}Info" => "$C{10}Country$C{1}", PreviousValues => join(',',@countries) }) if(@countries);
  push(@historicData,{ "$C{5}Info" => "$C{10}CPU$C{1}", PreviousValues => join(',',@cpus) }) if(@cpus);
  push(@historicData,{ "$C{5}Info" => "$C{10}HardwareId$C{1}", PreviousValues => join(',',@hardwareIds) }) if(@hardwareIds);
  if(@historicData) {
    $p_resultLines=formatArray(["$C{5}Info",'PreviousValues'],\@historicData,"$C{2}Account history$C{1}");
    sayPrivate($user,'.');
    foreach my $resultLine (@{$p_resultLines}) {
      sayPrivate($user,$resultLine);
    }
  }

  my @alternateAccounts;
  $sth=$sldb->prepExec("select ua.accountId,UNIX_TIMESTAMP(a.lastUpdate) from userAccounts ua,accounts a where ua.userId=$userId and ua.accountId!=$accountId and ua.accountId=a.id group by ua.accountId order by a.lastUpdate desc");
  while(@results=$sth->fetchrow_array()) {
    my @names2;
    my $sth2=$sldb->prepExec("select name from names where accountId=$results[0] order by lastConnection desc limit 1");
    my @nameResult;
    while(@nameResult=$sth2->fetchrow_array()) {
      push(@names2,$nameResult[0]);
    }
    $accountAge=time-$results[1];
    $accountActivity=secToDayAge($accountAge);
    if($accountAge > 7776000) {
      $accountActivity="$C{15}$accountActivity";
    }elsif($accountAge > 2592000) {
      $accountActivity="$C{14}$accountActivity";
    }else{
      $accountActivity="$C{1}$accountActivity";
    }
    push(@alternateAccounts,{ "$C{5}ID" => "$C{14}$results[0]",
                              Name => "$C{2}$names2[0]",
                              LastActivity => $accountActivity });
  }

  my %tsProbSmurfData;
  my %tsNotSmurfData;
  $sth=$sldb->prepExec("select s.id1,s.status,UNIX_TIMESTAMP(a.lastUpdate) from smurfs s,accounts a where s.id2=$accountId and s.status != 1 and s.id1=a.id group by s.id1 order by a.lastUpdate desc");
  while(@results=$sth->fetchrow_array()) {
    my ($smurfId,$smurfStatus,$timestamp)=@results;
    my @names2;
    my $sth2=$sldb->prepExec("select name from names where accountId=$smurfId order by lastConnection desc limit 1");
    my @nameResult;
    while(@nameResult=$sth2->fetchrow_array()) {
      push(@names2,$nameResult[0]);
    }
    my $p_smurfHash;
    if($smurfStatus == 0) {
      $p_smurfHash=\%tsNotSmurfData;
    }elsif($smurfStatus == 2) {
      $p_smurfHash=\%tsProbSmurfData;
    }
    $accountAge=time-$timestamp;
    $accountActivity=secToDayAge($accountAge);
    if($accountAge > 7776000) {
      $accountActivity="$C{15}$accountActivity";
    }elsif($accountAge > 2592000) {
      $accountActivity="$C{14}$accountActivity";
    }else{
      $accountActivity="$C{1}$accountActivity";
    }
    if(exists $p_smurfHash->{$timestamp}) {
      push(@{$p_smurfHash->{$timestamp}},{ "$C{5}ID" => "$C{14}$smurfId",
                                           Name => "$C{2}$names2[0]",
                                           LastActivity => $accountActivity });
    }else{
      $p_smurfHash->{$timestamp}=[ { "$C{5}ID" => "$C{14}$smurfId",
                                     Name => "$C{2}$names2[0]",
                                     LastActivity => $accountActivity } ];
    }
  }
  $sth=$sldb->prepExec("select s.id2,s.status,UNIX_TIMESTAMP(a.lastUpdate),group_concat(n.name) from smurfs s,accounts a,names n where s.id1=$accountId and s.status != 1 and s.id2=a.id and s.id2=n.accountId group by s.id2 order by a.lastUpdate desc, n.lastConnection desc");
  while(@results=$sth->fetchrow_array()) {
    my ($smurfId,$smurfStatus,$timestamp)=@results;
    my @names2;
    my $sth2=$sldb->prepExec("select name from names where accountId=$smurfId order by lastConnection desc limit 1");
    my @nameResult;
    while(@nameResult=$sth2->fetchrow_array()) {
      push(@names2,$nameResult[0]);
    }
    my $p_smurfHash;
    if($smurfStatus == 0) {
      $p_smurfHash=\%tsNotSmurfData;
    }elsif($smurfStatus == 2) {
      $p_smurfHash=\%tsProbSmurfData;
    }
    $accountAge=time-$timestamp;
    $accountActivity=secToDayAge($accountAge);
    if($accountAge > 7776000) {
      $accountActivity="$C{15}$accountActivity";
    }elsif($accountAge > 2592000) {
      $accountActivity="$C{14}$accountActivity";
    }else{
      $accountActivity="$C{1}$accountActivity";
    }
    if(exists $p_smurfHash->{$timestamp}) {
      push(@{$p_smurfHash->{$timestamp}},{ "$C{5}ID" => "$C{14}$smurfId",
                                           Name => "$C{2}$names2[0]",
                                           LastActivity => $accountActivity });
    }else{
      $p_smurfHash->{$timestamp}=[ { "$C{5}ID" => "$C{14}$smurfId",
                                     Name => "$C{2}$names2[0]",
                                     LastActivity => $accountActivity } ];
    }
  }
  my @probSmurfData;
  my @notSmurfData;
  foreach my $ts (sort {$b <=> $a} keys %tsProbSmurfData) {
    foreach my $p_smurfData (@{$tsProbSmurfData{$ts}}) {
      push(@probSmurfData,$p_smurfData);
    }
  }
  foreach my $ts (sort {$b <=> $a} keys %tsNotSmurfData) {
    foreach my $p_smurfData (@{$tsNotSmurfData{$ts}}) {
      push(@notSmurfData,$p_smurfData);
    }
  }
  
  sayPrivate($user,'.') if(@alternateAccounts || @probSmurfData || @notSmurfData);

  my $padding=0;
  my $spacing=4;
  $p_resultLines=[];
  if(@alternateAccounts) {
    $p_resultLines=formatArray(["$C{5}ID",'Name','LastActivity'],\@alternateAccounts,"$C{2}Alternate accounts$C{1}",50,$maxSize);
    $padding=realLength($p_resultLines->[1])+$spacing;
  }
  if(@probSmurfData) {
    my $p_resultLines2=formatArray(["$C{5}ID",'Name','LastActivity'],\@probSmurfData,"$C{2}Probable smurfs$C{1}",50,$maxSize);
    foreach my $i (0..$#{$p_resultLines2}) {
      if(! defined $p_resultLines->[$i]) {
        $p_resultLines->[$i]=' ' x $padding;
      }else{
        $p_resultLines->[$i].=' ' x ($padding-realLength($p_resultLines->[$i]));
      }
      $p_resultLines->[$i].=$p_resultLines2->[$i];
    }
    $padding+=realLength($p_resultLines2->[1])+$spacing;
  }
  if(@notSmurfData) {
    my $p_resultLines2=formatArray(["$C{5}ID",'Name','LastActivity'],\@notSmurfData,"$C{2}NOT smurfs$C{1}",50,$maxSize);
    foreach my $i (0..$#{$p_resultLines2}) {
      if(! defined $p_resultLines->[$i]) {
        $p_resultLines->[$i]=' ' x $padding;
      }else{
        $p_resultLines->[$i].=' ' x ($padding-realLength($p_resultLines->[$i]));
      }
      $p_resultLines->[$i].=$p_resultLines2->[$i];
    }
  }
  foreach my $resultLine (@{$p_resultLines}) {
    sayPrivate($user,$resultLine);
  }
}

# Lobby interface callbacks ###################################################

sub cbLobbyConnect {
  $lobbyState=2;
  $lobbyBrokenConnection=0;
  $lanMode=$_[4];

  $lobby->addCallbacks({CHANNELTOPIC => \&cbChannelTopic,
                        LOGININFOEND => \&cbLoginInfoEnd,
                        JOIN => \&cbJoin,
                        JOINFAILED => \&cbJoinFailed,
                        SAID => \&cbSaid,
                        CHANNELMESSAGE => \&cbChannelMessage,
                        SAIDEX => \&cbSaidEx,
                        SAIDPRIVATE => \&cbSaidPrivate,
                        BROADCAST => \&cbBroadcast,
                        JOINED => \&cbJoined,
                        LEFT => \&cbLeft,
                        LEFTBATTLE => \&cbLeftBattle,
                        BATTLEOPENED => \&cbBattleOpened,
                        BATTLECLOSED => \&cbBattleClosed,
                        SERVERMSG => \&cbServerMsg});

  $lobby->addPreCallbacks({CLIENTSTATUS => \&cbPreClientStatus});

  my $localLanIp=$conf{localLanIp};
  $localLanIp=getLocalLanIp() unless($localLanIp);
  queueLobbyCommand(["LOGIN",$conf{lobbyLogin},$lobby->marshallPasswd($conf{lobbyPassword}),getCpuSpeed(),$localLanIp,"SldbLi v$sldbLiVer",0,'a b sp et'],
                    {ACCEPTED => \&cbLoginAccepted,
                     DENIED => \&cbLoginDenied,
                     AGREEMENTEND => \&cbAgreementEnd},
                    \&cbLoginTimeout);
  %hostBattles=();
  %hostSkills=();
  %battleHosts=();
  %newGamesFinished=();
  %hostsVersions=();
}

sub cbBroadcast {
  my (undef,$msg)=@_;
  print "Lobby broadcast message: $msg\n";
  slog("Lobby broadcast message: $msg",3);
}

sub cbServerMsg {
  my (undef,$msg)=@_;
  slog("Lobby server message: $msg",3);
}

sub cbRedirect {
  my (undef,$ip,$port)=@_;
  $ip='' unless(defined $ip);
  if($conf{lobbyFollowRedirect}) {
    if($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/ && $1<256 && $2<256 && $3<256 && $4<256) {
      $port=$conf{lobbyPort} unless(defined $port);
      if($port !~ /^\d+$/) {
        slog("Invalid port \"$port\" received in REDIRECT command, ignoring redirection",1);
        return;
      }
    }else{
      slog("Invalid IP address \"$ip\" received in REDIRECT command, ignoring redirection",1);
      return;
    }
    %pendingRedirect=(ip => $ip, port => $port);
  }else{
    slog("Ignoring redirection request to address $ip",2);
  }
}

sub cbLobbyDisconnect {
  slog("Disconnected from lobby server (connection reset by peer)",2);
  $lobbyState=0;
  foreach my $joinedChan (keys %{$lobby->{channels}}) {
    logMsg("channel_$joinedChan","=== $conf{lobbyLogin} left ===") if($conf{logChanJoinLeave});
  }
  $lobby->disconnect();
}

sub cbConnectTimeout {
  $lobbyState=0;
  slog("Timeout while connecting to lobby server ($conf{lobbyHost}:$conf{lobbyPort})",2);
}

sub cbLoginAccepted {
  $lobbyState=3;
  slog("Logged on lobby server",4);
  $triedGhostWorkaround=0;
}

sub cbLoginInfoEnd {
  $lobbyState=4;
  queueLobbyCommand(["JOIN",$conf{masterChannel}]) if($conf{masterChannel} ne '');
  my %chansToJoin;
  if($conf{broadcastChannels}) {
    my @broadcastChans=split(/;/,$conf{broadcastChannels});
    foreach my $chan (@broadcastChans) {
      $chansToJoin{$chan}=1;
    }
  }
  foreach my $chan (keys %chansToJoin) {
    next if($chan eq $conf{masterChannel});
    queueLobbyCommand(["JOIN",$chan]);
  }
  if(exists $lobby->{users}->{$conf{lobbyLogin}} && ! $lobby->{users}->{$conf{lobbyLogin}}->{status}->{bot}) {
    slog('The lobby account currently used by SldbLi is not tagged as bot. It is recommended to ask a lobby administrator for bot flag on accounts used by SldbLi',2);
  }
}

sub cbLoginDenied {
  my (undef,$reason)=@_;
  slog("Login denied on lobby server ($reason)",1);
  if(($reason !~ /^Already logged in/ && $reason !~ /^This account has already logged in/) || $triedGhostWorkaround > 2) {
    scheduleQuit("loggin denied on lobby server");
  }
  if($reason =~ /^Already logged in/) {
    $triedGhostWorkaround++;
  }else{
    $triedGhostWorkaround=0;
  }
  $lobbyState=0;
  $lobby->disconnect();
}

sub cbAgreementEnd {
  slog('Spring Lobby agreement has not been accepted for this account yet, please login with a Spring lobby client and accept the agreement',1);
  scheduleQuit('spring Lobby agreement not accepted yet for this account');
  $lobbyState=0;
  $lobby->disconnect();
}

sub cbLoginTimeout {
  slog("Unable to log on lobby server (timeout)",2);
  $lobbyState=0;
  foreach my $joinedChan (keys %{$lobby->{channels}}) {
    logMsg("channel_$joinedChan","=== $conf{lobbyLogin} left ===") if($conf{logChanJoinLeave});
  }
  $lobby->disconnect();
}

sub cbJoin {
  my (undef,$channel)=@_;
  slog("Channel $channel joined",4);
  logMsg("channel_$channel","=== $conf{lobbyLogin} joined ===") if($conf{logChanJoinLeave});
}

sub cbJoinFailed {
  my (undef,$channel,$reason)=@_;
  slog("Unable to join channel $channel ($reason)",2);
}

sub cbJoined {
  my (undef,$chan,$user)=@_;
  logMsg("channel_$chan","=== $user joined ===") if($conf{logChanJoinLeave});
}

sub cbLeft {
  my (undef,$chan,$user,$reason)=@_;
  my $reasonString ="";
  $reasonString=" ($reason)" if(defined $reason && $reason ne "");
  logMsg("channel_$chan","=== $user left$reasonString ===") if($conf{logChanJoinLeave});
}

sub cbLeftBattle {
  my (undef,$battleId,$user)=@_;
  if(! (exists $battleHosts{$battleId} && exists $hostSkills{$battleHosts{$battleId}})) {
    slog("Ignoring invalid LEFTBATTLE command from lobby server, battle $battleId is unknown!",2);
  }else{
    delete $hostSkills{$battleHosts{$battleId}}->{$user};
  }
}

sub cbBattleOpened {
  my ($bId,$founder)=($_[1],$_[4]);
  $hostBattles{$founder}=$bId;
  $hostSkills{$founder}={};
  $battleHosts{$bId}=$founder;
}

sub cbBattleClosed {
  my $bId=$_[1];
  if(exists $battleHosts{$bId}) {
    delete $hostBattles{$battleHosts{$bId}};
    delete $hostSkills{$battleHosts{$bId}};
    delete $battleHosts{$bId};
  }else{
    slog("Ignoring invalid BATTLECLOSED command from lobby server, battle $bId is unknown!",2);
  }
}

sub cbPreClientStatus {
  my (undef,$user,$status)=@_;
  return unless(exists $hostBattles{$user});
  my $p_newClientStatus=$lobby->unmarshallClientStatus($status);
  $newGamesFinished{$user}=time if($lobby->{users}->{$user}->{status}->{inGame} && ! $p_newClientStatus->{inGame});
}

sub cbSaid {
  my (undef,$chan,$user,$msg)=@_;
  logMsg("channel_$chan","<$user> $msg") if($conf{logChanChat});
  if($chan eq $masterChannel && $msg =~ /^!(\w.*)$/) {
    handleRequest("chan",$user,$1);
  }
}

sub cbChannelMessage {
  my (undef,$chan,$msg)=@_;
  logMsg("channel_$chan","* Channel message: $msg") if($conf{logChanChat});
}

sub cbSaidEx {
  my (undef,$chan,$user,$msg)=@_;
  logMsg("channel_$chan","* $user $msg") if($conf{logChanChat});
}

sub cbSaidPrivate {
  my (undef,$user,$msg)=@_;
  logMsg("pv_$user","<$user> $msg") if($conf{logPvChat});
  if($msg =~ /^!([\w\#].*)$/) {
    handleRequest("pv",$user,$1);
  }
}

sub cbChannelTopic {
  my (undef,$chan,$user,$time,$topic)=@_;
  logMsg("channel_$chan","* Topic is '$topic' (set by $user)") if($conf{logChanChat});
}

# Main ########################################################################

slog("Initializing SldbLi",3);

while($running) {

  if(! $lobbyState && ! $quitScheduled) {
    if($timestamps{connectAttempt} != 0 && $conf{lobbyReconnectDelay} == 0) {
      scheduleQuit('disconnected from lobby server, no reconnection delay configured');
    }else{
      if(time-$timestamps{connectAttempt} > $conf{lobbyReconnectDelay}) {
        $timestamps{connectAttempt}=time;
        $lobbyState=1;
        if(defined $lSock) {
          my @newSockets=();
          foreach my $sock (@sockets) {
            push(@newSockets,$sock) unless($sock == $lSock);
          }
          @sockets=@newSockets;
        }
        $lobby->addCallbacks({REDIRECT => \&cbRedirect});
        $lSock = $lobby->connect(\&cbLobbyDisconnect,{TASSERVER => \&cbLobbyConnect},\&cbConnectTimeout);
        if($lSock) {
          push(@sockets,$lSock);
        }else{
          $lobby->removeCallbacks(['REDIRECT']);
          $lobbyState=0;
          slog("Connection to lobby server failed",1);
        }
      }
    }
  }

  checkQueuedLobbyCommands();

  checkTimedEvents();

  my @pendingSockets=IO::Select->new(@sockets)->can_read(1);

  foreach my $pendingSock (@pendingSockets) {
    if($pendingSock == $lSock) {
      $lobby->receiveCommand();
    }else{
      scheduleQuit("received data on unknown socket");
    }
  }

  if( $lobbyState > 0 && ( (time - $timestamps{connectAttempt} > 30 && time - $lobby->{lastRcvTs} > 60) || $lobbyBrokenConnection ) ) {
    if($lobbyBrokenConnection) {
      $lobbyBrokenConnection=0;
      slog("Disconnecting from lobby server (broken connection detected)",2);
    }else{
      slog("Disconnected from lobby server (timeout)",2);
    }
    $lobbyState=0;
    foreach my $joinedChan (keys %{$lobby->{channels}}) {
      logMsg("channel_$joinedChan","=== $conf{lobbyLogin} left ===") if($conf{logChanJoinLeave});
    }
    $lobby->disconnect();
  }

  if($lobbyState > 1 && ( ( time - $timestamps{ping} > 5 && time - $lobby->{lastSndTs} > 28)
                          || ( time - $timestamps{ping} > 28 && time - $lobby->{lastRcvTs} > 28) ) ) {
    sendLobbyCommand([['PING']],5);
    $timestamps{ping}=time;
  }

  if(%pendingRedirect) {
    my ($ip,$port)=($pendingRedirect{ip},$pendingRedirect{port});
    %pendingRedirect=();
    slog("Following redirection to $ip:$port",3);
    $lobbyState=0;
    foreach my $joinedChan (keys %{$lobby->{channels}}) {
      logMsg("channel_$joinedChan","=== $conf{lobbyLogin} left ===") if($conf{logChanJoinLeave});
    }
    $lobby->disconnect();
    $conf{lobbyHost}=$ip;
    $conf{lobbyPort}=$port;
    $lobby = SpringLobbyInterface->new(serverHost => $conf{lobbyHost},
                                       serverPort => $conf{lobbyPort},
                                       simpleLog => $lobbySimpleLog,
                                       warnForUnhandledMessages => 0);
    $timestamps{connectAttempt}=0;
  }

  if($quitScheduled) {
    slog("No pending process, exiting",3);
    $running=0;
  }
}

if($lobbyState) {
  foreach my $joinedChan (keys %{$lobby->{channels}}) {
    logMsg("channel_$joinedChan","=== $conf{lobbyLogin} left ===") if($conf{logChanJoinLeave});
  }
  $lobbyState=0;
  if($quitScheduled == 2) {
    sendLobbyCommand([['EXIT','SldbLi restarting']]);
  }else{
    sendLobbyCommand([['EXIT','SldbLi shutting down']]);
  }
  $lobby->disconnect();
}
if($quitScheduled == 2) {
  exec($0,$confFile) || forkedError("Unable to restart SldbLi",0);
}

exit 0;
