# Perl module implementing the SLDB data model.
# This file is part of SLDB.
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

package Sldb;

use strict;

use DBI;
use Time::HiRes;

use SimpleLog;

my $moduleVersion='0.1';

my %ADMIN_EVT_TYPE=('UPD_USERDETAILS' => 0,
                    'JOIN_ACC' => 1,
                    'SPLIT_ACC' => 2,
                    'ADD_PROB_SMURF' => 3,
                    'DEL_PROB_SMURF' => 4,
                    'ADD_NOT_SMURF' => 5,
                    'DEL_NOT_SMURF' => 6);
my %ADMIN_EVT_PARAMS=(0 => [qw/updatedUserId updatedParam oldValue newValue/],
                      1 => [qw/mainUserId childUserId/],
                      2 => [qw/oldUserId newUserId accountId/],
                      3 => [qw/accountId1 accountId2/],
                      4 => [qw/accountId1 accountId2/],
                      5 => [qw/accountId1 accountId2/],
                      6 => [qw/accountId1 accountId2/]);
my %ADMIN_EVT_MSG=(0 => 'Update of setting "%updatedParam%" for user "%updatedUserId%": "%oldValue%" --> "%newValue%"',
                   1 => 'Join of account "%childUserId%" with user "%mainUserId%"',
                   2 => 'Split of account "%accountId%" and user "%oldUserId%" (new user: "%newUserId%")',
                   3 => 'New probable smurfs: "%accountId1%" <-> "%accountId2%"',
                   4 => 'Removal of probable smurfs: "%accountId1%" <-> "%accountId2%"',
                   5 => 'New not-smurfs record: "%accountId1%" <-> "%accountId2%"',
                   6 => 'Removal of not-smurfs record: "%accountId1%" <-> "%accountId2%"');

my %gameTypeMapping=('Duel' => 'Duel',
                     'FFA' => 'Ffa',
                     'Team' => 'Team',
                     'TeamFFA' => 'TeamFfa',
                     'Global' => '');

my %ACCOUNTS_PREF=( ircColors => ['[01]',1] );
my %USERS_PREF=( privacyMode => ['[012]',1] );

my $chartClickerUnavailable;

sub new {
  my ($objectOrClass,$p_params)=@_;
  my $class = ref($objectOrClass) || $objectOrClass;
  my $self={ dbDs => undef,
             dbLogin => undef,
             dbPwd => undef,
             sLog => undef,
             sqlErrorHandler => undef };
  foreach my $param (keys %{$p_params}) {
    if(grep(/^$param$/,(keys %{$self}))) {
      $self->{$param}=$p_params->{$param};
    }else{
      $self->{sLog}=SimpleLog->new(prefix => "[Sldb] ") unless(defined $self->{sLog});
      $self->{sLog}->log("Ignoring invalid constructor parameter ($param)",2);
    }
  }
  foreach my $param (keys %{$self}) {
    if(! defined $self->{$param}) {
      $self->{sLog}=SimpleLog->new(prefix => "[Sldb] ") unless(defined $self->{sLog});
      $self->{sLog}->log("Missing parameter \"$param\" in constructor call",1);
      exit;
    }
  }
  $self->{dbh}=undef;
  bless ($self, $class);
  return $self;
}

sub getVersion {
  return $moduleVersion;
}

sub log {
  my ($self,$m,$l)=@_;
  $self->{sLog}->log($m,$l);
}

###################
# Basic functions #
###################

sub connect {
  my ($self,$p_options)=@_;
  my $p_connectOptions={AutoCommit => 1, mysql_auto_reconnect => 1};
  if(defined $p_options && %{$p_options}) {
    foreach my $option (keys %{$p_options}) {
      $p_connectOptions->{$option}=$p_options->{$option}
    }
  }
  if(defined $self->{dbh}) {
    $self->log("connect(): Already connected to a database, use \"disconnect()\" first",2);
    return 0;
  }
  $self->{dbh}=DBI->connect($self->{dbDs},$self->{dbLogin},$self->{dbPwd},$p_connectOptions);
  if(! $self->{dbh}) {
    &{$self->{sqlErrorHandler}}("Unable to connect to Spring Lobby Database ($DBI::errstr)");
    return 0;
  }
  return 1;
}

sub disconnect {
  my $self=shift;
  if(! defined $self->{dbh}) {
    $self->log("disconnect(): module is not connected to any database, use \"connect()\" first",2);
    return 0;
  }
  if(! $self->{dbh}->disconnect()) {
    $self->log("disconnect(): unable to disconnect from database ($DBI::errstr)",1);
    return 0;
  }
  return 1;
}

sub quote {
  my ($self,@params)=@_;
  if(! defined $self->{dbh}) {
    my $paramsString=join(',',@params);
    &{$self->{sqlErrorHandler}}("Unable to quote value(s) \"$paramsString\" (module is not connected!)");
    return @params;
  }
  my @res=map {$self->{dbh}->quote($_)} @params;
  if($#res == 0) {
    return $res[0];
  }else{
    return @res;
  }
}

sub do {
  my ($self,$sqlCommand,$desc,$p_errorHandler)=@_;
  $desc="execute \"$sqlCommand\"" unless(defined $desc);
  $self->log("SLDB: $desc",5);
  $desc="Unable to $desc";
  $p_errorHandler=$self->{sqlErrorHandler} unless(defined $p_errorHandler);
  if(! defined $self->{dbh}) {
    &{$p_errorHandler}("$desc (module is not connected!)");
    return 0;
  }
  $self->log("SQL DO: $sqlCommand",5);
  if(! $self->{dbh}->do($sqlCommand)) {
    &{$p_errorHandler}("$desc ($DBI::errstr)");
    return 0;
  }
  return 1;
}

sub prepExec {
  my ($self,$sqlCommand,$desc)=@_;
  $desc="select \"$sqlCommand\"" unless(defined $desc);
  $self->log("SLDB: $desc",5);
  $desc="Unable to $desc";
  if(! defined $self->{dbh}) {
    &{$self->{sqlErrorHandler}}("$desc (module is not connected!)");
    return 0;
  }
  $self->log("SQL PREPARE AND EXEC: $sqlCommand",5);
  my $sth=$self->{dbh}->prepare($sqlCommand);
  if(! $sth->execute()) {
    &{$self->{sqlErrorHandler}}("$desc (".$sth->errstr.')');
    return 0;
  }
  return $sth;
}

#################
# Init function #
#################

# Called by sldbSetup.pl
sub createTablesIfNeeded {
  my $self=shift;

  $self->do('
create table if not exists accounts (
  id int unsigned primary key,
  rank tinyint(1) unsigned,
  admin tinyint(1) unsigned,
  bot tinyint(1) unsigned,
  lastUpdate timestamp,
  index (lastUpdate)
) engine=MyISAM','create table "accounts"');

  $self->do('
create table if not exists names (
  accountId int unsigned,
  name char(20),
  lastConnection timestamp,
  primary key (accountId,name),
  index (name),
  index (lastConnection)
) engine=MyISAM','create table "names"');

  $self->do('
create table if not exists countries (
  accountId int unsigned,
  country char(2),
  lastConnection timestamp,
  primary key (accountId,country),
  index (country),
  index (lastConnection)
) engine=MyISAM','create table "countries"');

  $self->do('
create table if not exists cpus (
  accountId int unsigned,
  cpu int unsigned,
  lastConnection timestamp,
  primary key (accountId,cpu),
  index (lastConnection)
) engine=MyISAM','create table "cpus"');

  $self->do('
create table if not exists hardwareIds (
  accountId int unsigned,
  hardwareId int,
  lastConnection timestamp,
  primary key (accountId,hardwareId),
  index (lastConnection)
) engine=MyISAM','create table "hardwareIds"');

  $self->do('
create table if not exists games (
  hostAccountId int unsigned,
  startTimestamp timestamp default 0,
  endTimestamp timestamp default 0,
  endCause tinyint(1) unsigned,
  hostName char(20),
  modName varchar(255),
  mapName varchar(255),
  nbSpec tinyint unsigned,
  nbPlayer tinyint unsigned,
  description varchar(255),
  passworded tinyint(1) unsigned,
  gameId char(32),
  primary key (hostAccountId,startTimestamp),
  index (startTimestamp),
  index (endTimestamp),
  index (modName),
  index (mapName),
  unique index (gameId)
) engine=MyISAM','create table "games"');

  $self->do('
create table if not exists players (
  hostAccountId int unsigned,
  startTimestamp timestamp default 0,
  accountId int unsigned,
  name char(20),
  primary key (hostAccountId,startTimestamp,accountId),
  index (startTimestamp),
  index (accountId)
) engine=MyISAM','create table "players"');

  $self->do('
create table if not exists gamesDetails (
  gameId char(32) primary key,
  gdrTimestamp timestamp default 0,
  startTimestamp timestamp default 0,
  endTimestamp timestamp default 0,
  duration int unsigned,
  engine varchar(64),
  type char(16),
  structure varchar(64),
  bots tinyint unsigned,
  undecided tinyint unsigned,
  cheating tinyint unsigned,
  index (gdrTimestamp),
  index (startTimestamp),
  index (endTimestamp)
) engine=MyISAM','create table "gamesDetails"');

  $self->do('
create table if not exists playersDetails (
  gameId char(32),
  accountId int unsigned,
  name char(20),
  ip int unsigned,
  team tinyint unsigned,
  allyTeam tinyint unsigned,
  win tinyint(1) unsigned,
  primary key (gameId,accountId),
  index (accountId),
  index (name),
  index (ip)
) engine=MyISAM','create table "playersDetails"');

  $self->do('
create table if not exists botsDetails (
  gameId char(32),
  name char(20),
  ownerAccountId int unsigned,
  ai varchar(64),
  team tinyint unsigned,
  allyTeam tinyint unsigned,
  win tinyint(1) unsigned,
  primary key (gameId,name)
) engine=MyISAM','create table "botsDetails"');

  $self->do('
create table if not exists userAccounts (
  accountId int unsigned primary key,
  userId int unsigned,
  nbIps int unsigned,
  noSmurf tinyint(1) unsigned,
  index (userId)
) engine=MyISAM','create table "userAccounts"');

  $self->do('
create table if not exists userDetails (
  userId int unsigned primary key,
  name char(24),
  clanTag char(18),
  email varchar(64),
  forumId int unsigned,
  nbIps int unsigned,
  unique index (name)
) engine=MyISAM','create table "userDetails"');

  $self->do('
create table if not exists ips (
  accountId int unsigned,
  ip int unsigned,
  lastSeen timestamp,
  primary key (accountId,ip),
  index (ip),
  index (lastSeen)
) engine=MyISAM','create table "ips"');

  $self->do('
create table if not exists ipRanges (
  accountId int unsigned,
  ip1 int unsigned,
  ip2 int unsigned,
  lastSeen timestamp,
  primary key (accountId,ip1),
  index (ip1),
  index (ip2),
  index (lastSeen)
) engine=MyISAM','create table "ipRanges"');

  $self->do('
create table if not exists userIps (
  userId int unsigned,
  ip int unsigned,
  lastSeen timestamp,
  primary key (userId,ip),
  index (ip),
  index (lastSeen)
) engine=MyISAM','create table "userIps"');

  $self->do('
create table if not exists userIpRanges (
  userId int unsigned,
  ip1 int unsigned,
  ip2 int unsigned,
  lastSeen timestamp,
  primary key (userId,ip1),
  index (ip1),
  index (ip2),
  index (lastSeen)
) engine=MyISAM','create table "userIpRanges"');

  $self->do('
create table if not exists smurfs (
  id1 int unsigned,
  id2 int unsigned,
  status tinyint unsigned,
  orig int unsigned,
  primary key (id1,id2),
  index (id2)
) engine=MyISAM','create table "notSmurf"');

  $self->do('
create table if not exists adminEvents (
  eventId int unsigned auto_increment primary key,
  date timestamp,
  type smallint unsigned,
  subType smallint unsigned,
  orig tinyint unsigned,
  origId int unsigned,
  message varchar(255),
  index (date)
) engine=MyISAM','create table "adminEvents"');

  $self->do('
create table if not exists adminEventsParams (
  eventId int unsigned,
  paramName char(16),
  paramValue varchar(64),
  primary key (eventId,paramName)
) engine=MyISAM','create table "adminEventsParams"');

  $self->do('
create table if not exists rtBattles (
  battleId int unsigned primary key,
  founderId int unsigned,
  founder varchar(30),
  ip int unsigned,
  port smallint unsigned,
  type tinyint(1),
  natType tinyint(1),
  locked tinyint(1),
  passworded tinyint(1),
  rankLimit tinyint(1) unsigned,
  modName varchar(255),
  mapName varchar(255),
  mapHash int,
  description varchar(255),
  maxPlayers int,
  nbSpec tinyint unsigned,
  engineName varchar(30),
  engineVersion varchar(100),
  index(founderId),
  index(founder),
  index(ip),
  index(port),
  index(modName),
  index(mapName),
  index(mapHash),
  index(description),
  index(maxPlayers),
  index(nbSpec)
) engine=MyISAM','create table "rtBattles"');

  $self->do('
create table if not exists rtPlayers (
  accountId int unsigned primary key,
  name varchar(30),
  access tinyint(1),
  bot tinyint(1),
  country char(2),
  cpu mediumint unsigned,
  rank tinyint(1),
  inGame tinyint(1),
  gameTimestamp timestamp default 0,
  away tinyint(1),
  awayTimestamp  timestamp default 0,
  index(name),
  index(country),
  index(cpu)
) engine=MyISAM','create table "rtPlayers"');

  $self->do('
create table if not exists rtBattlePlayers (
  accountId int unsigned primary key,
  battleId int unsigned,
  index(battleId)
) engine=MyISAM','create table "rtBattlePlayers"');

  $self->do('
create table if not exists gamesNames (
  name varchar(64) primary key,
  shortName char(8),
  regex varchar(64),
  chickenRegex varchar(64),
  index(shortName)
) engine=MyISAM','create table "gamesNames"');

  my $partitionDefs="partition by list(period) (";
  for my $year (2012..2017) {
    for my $i (1..12) {
      next if($year == 2012 && $i < 7);
      $partitionDefs.=',' unless($year == 2012 && $i == 7);
      my $month=sprintf("%02d",$i);
      my $period=$year.$month;
      $partitionDefs.="\n  partition p$period values in ($period)";
    }
  }
  $partitionDefs.="\n)";

  foreach my $gameType (values %gameTypeMapping) {
    $self->do("
create table if not exists ts${gameType}Games (
  gameId char(32),
  accountId int unsigned,
  userId int unsigned,
  modShortName char(8),
  gdrTimestamp timestamp,
  muBefore decimal(7,4),
  sigmaBefore decimal(7,4),
  muAfter decimal(7,4),
  sigmaAfter decimal(7,4),
  primary key (gameId,accountId),
  index(accountId),
  index(userId),
  index(gdrTimestamp)
) engine=MyISAM","create table \"ts${gameType}Games\"");

    $self->do("
create table if not exists ts${gameType}Players (
  period int unsigned,
  userId int unsigned,
  modShortName char(8),
  skill decimal(7,4),
  mu decimal(7,4),
  sigma decimal(7,4),
  nbPenalties smallint unsigned,
  primary key (period,userId,modShortName),
  index(skill)
) engine=MyISAM
$partitionDefs","create partitioned table \"ts${gameType}Players\"");
  }

  $self->do('
create table if not exists tsRatingQueue (
  gameId char(32) primary key,
  gdrTimestamp timestamp,
  status tinyint(1),
  index(gdrTimestamp)
) engine=MyISAM','create table "tsRatingQueue"');

  $self->do('
create table if not exists tsRatingState (
  param varchar(32) primary key,
  value varchar(32)
) engine=MyISAM','create table "tsRatingState"');

  $self->do('
create table if not exists tsRerateAccounts (
  accountId int unsigned primary key,
  accountTimestamp Timestamp
) engine=MyISAM','create table "tsRerateAccounts"');

  $self->do('
create table if not exists prefAccounts (
  accountId int unsigned,
  prefName char(16),
  prefValue varchar(64),
  primary key (accountId,prefName)
) engine=MyISAM','create table "prefAccounts"');

  $self->do('
create table if not exists prefUsers (
  userId int unsigned,
  prefName char(16),
  prefValue varchar(64),
  primary key (userId,prefName)
) engine=MyISAM','create table "prefUsers"');

}

##################################
# User/account lookup  functions #
##################################

# Called by sldbLi.pl, getIdType(), getUserPref(), setUserPref(), getSkills(), getPlayerStats()
sub getUserId {
  my ($self,$id)=@_;
  my $sth=$self->prepExec("select userId from userAccounts where accountId=$id","retrieve userId for accountId \"$id\" from userAccounts table");
  my @results=$sth->fetchrow_array();
  return $results[0] if(@results);
  return undef;
}

# Called by getUserSmurfs()
sub getUserIds {
  my ($self,$p_ids)=@_;
  return [] unless(@{$p_ids});
  my $idsString=join(',',@{$p_ids});
  my $sth=$self->prepExec("select distinct(userId) from userAccounts where accountId in ($idsString)","retrieve userIds for accountIds \"$idsString\" from userAccounts table");
  my @userIds;
  my @result;
  while(@result=$sth->fetchrow_array()) {
    push(@userIds,$result[0]);
  }
  return \@userIds;
}

# Called by sldbLi.pl, identifyUniqueAccountByString(), identifyUniqueAccountByStringUserFirst()
sub getUserIdByName {
  my ($self,$name)=@_;
  my $quotedName=$self->quote($name);
  my $sth=$self->prepExec("select ud.userId from userDetails ud,userAccounts ua where ud.name=$quotedName and ua.userId=ud.userId and ua.userId=ua.accountId");
  my @results=$sth->fetchrow_array();
  if(@results) {
    return $results[0];
  }else{
    return undef;
  }
}

# Called by sldbLi.pl
sub getIdType {
  my ($self,$id)=@_;
  return 'invalid' unless($id =~ /^\d+$/);
  my $userId=$self->getUserId($id);
  return 'unknown' unless(defined $userId);
  return 'user' if($userId == $id);
  return 'account';
}

# Called by sldbLi.pl, getUsersSmurfStates(), deleteUsersSmurfStates(), getUserOrderedSmurfGroups()
sub getUserAccounts {
  my ($self,$userId)=@_;
  my @accounts;
  my $sth=$self->prepExec("select accountId from userAccounts where userId=$userId","retrieve accounts of user \"$userId\"");
  my @account;
  while(@account=$sth->fetchrow_array()) {
    push(@accounts,$account[0]);
  }
  return \@accounts;
}

# Called by sldbLi.pl
sub identifyUniqueAccountByString {
  my ($self,$search)=@_;
  my $quotedSearch=$self->quote($search);
  my $sth=$self->prepExec("select accountId from names where name=$quotedSearch limit 2","search $quotedSearch in names table");
  my $p_results=$sth->fetchall_arrayref();
  return -1 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  my $getByNameRes=$self->getUserIdByName($search);
  return $getByNameRes if(defined $getByNameRes);
  $quotedSearch=$self->quote('%'.$search.'%');
  $sth=$self->prepExec("select distinct(accountId) from names where name like $quotedSearch limit 2","search $quotedSearch matches in names table");
  $p_results=$sth->fetchall_arrayref();
  return -3 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  $sth=$self->prepExec("select ud.userId from userDetails ud,userAccounts ua where ud.name like $quotedSearch and ud.userId=ua.userId and ua.accountId=ua.userId limit 2","search $quotedSearch matches in userDetails and userAccounts tables");
  $p_results=$sth->fetchall_arrayref();
  return -2 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  return undef;
}

# Called by sldbLi.pl
sub identifyUniqueAccountByStringUserFirst {
  my ($self,$search)=@_;
  my $getByNameRes=$self->getUserIdByName($search);
  return $getByNameRes if(defined $getByNameRes);
  my $quotedSearch=$self->quote($search);
  my $sth=$self->prepExec("select accountId from names where name=$quotedSearch limit 2","search $quotedSearch in names table");
  my $p_results=$sth->fetchall_arrayref();
  return -1 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  $quotedSearch=$self->quote('%'.$search.'%');
  $sth=$self->prepExec("select ud.userId from userDetails ud,userAccounts ua where ud.name like $quotedSearch and ud.userId=ua.userId and ua.accountId=ua.userId limit 2","search $quotedSearch matches in userDetails and userAccounts tables");
  $p_results=$sth->fetchall_arrayref();
  return -2 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  $sth=$self->prepExec("select distinct(accountId) from names where name like $quotedSearch limit 2","search $quotedSearch matches in names table");
  $p_results=$sth->fetchall_arrayref();
  return -3 if($#{$p_results} > 0);
  return $p_results->[0]->[0] if($#{$p_results} == 0);
  return undef;
}

####################################
# Preferences management functions #
####################################

# Called by sldbLi.pl
sub getAccountPref {
  my ($self,$accountId,$pref,$p_val)=@_;
  foreach my $realPref (keys %ACCOUNTS_PREF) {
    if(lc($pref) eq lc($realPref)) {
      $pref=$realPref;
      last;
    }
  }
  return 0 if(! exists $ACCOUNTS_PREF{$pref});
  my $quotedPref=$self->quote($pref);
  my $sth=$self->prepExec("select prefValue from prefAccounts where accountId=$accountId and prefName=$quotedPref");
  my @result=$sth->fetchrow_array();
  if(@result) {
    ${$p_val}=$result[0] if(defined $p_val);
    return 1;
  }else{
    ${$p_val}=$ACCOUNTS_PREF{$pref}->[1] if(defined $p_val);
    return 2;
  }
}

# Called by sldbLi.pl
sub setAccountPref {
  my ($self,$accountId,$pref,$val)=@_;
  foreach my $realPref (keys %ACCOUNTS_PREF) {
    if(lc($pref) eq lc($realPref)) {
      $pref=$realPref;
      last;
    }
  }
  return 0 unless(exists $ACCOUNTS_PREF{$pref});
  my $quotedPref=$self->quote($pref);
  if(defined $val) {
    return -1 if($val !~ /^$ACCOUNTS_PREF{$pref}->[0]$/);
    my $quotedVal=$self->quote($val);
    $self->do("insert into prefAccounts values ($accountId,$quotedPref,$quotedVal) on duplicate key update prefValue=$quotedVal");
    return 1;
  }else{
    $self->do("delete from prefAccounts where accountId=$accountId and prefName=$quotedPref");
    return 2;
  }
}

# Called by sldbLi.pl, xmlRpc.pl
sub getUserPref {
  my ($self,$id,$pref,$p_val)=@_;
  foreach my $realPref (keys %USERS_PREF) {
    if(lc($pref) eq lc($realPref)) {
      $pref=$realPref;
      last;
    }
  }
  return 0 if(! exists $USERS_PREF{$pref});
  my $userId=$self->getUserId($id);
  if(defined $userId) {
    my $quotedPref=$self->quote($pref);
    my $sth=$self->prepExec("select prefValue from prefUsers where userId=$userId and prefName=$quotedPref");
    my @result=$sth->fetchrow_array();
    if(@result) {
      ${$p_val}=$result[0] if(defined $p_val);
      return 1;
    }
  }
  ${$p_val}=$USERS_PREF{$pref}->[1] if(defined $p_val);
  return 2;
}

# Called by sldbLi.pl, xmlRpc.pl
sub setUserPref {
  my ($self,$id,$pref,$val)=@_;
  foreach my $realPref (keys %USERS_PREF) {
    if(lc($pref) eq lc($realPref)) {
      $pref=$realPref;
      last;
    }
  }
  return 0 unless(exists $USERS_PREF{$pref});
  my $userId=$self->getUserId($id);
  return -2 unless(defined $userId);
  my $quotedPref=$self->quote($pref);
  if(defined $val) {
    return -1 if($val !~ /^$USERS_PREF{$pref}->[0]$/);
    my $quotedVal=$self->quote($val);
    $self->do("insert into prefUsers values ($userId,$quotedPref,$quotedVal) on duplicate key update prefValue=$quotedVal");
    return 1;
  }else{
    $self->do("delete from prefUsers where userId=$userId and prefName=$quotedPref");
    return 2;
  }
}

#####################################
# Parameterization access functions #
#####################################

# Called by sldbLi.pl
sub getModShortName {
  my ($self,$mod)=@_;
  my $quotedMod=$self->quote($mod);
  my $sth=$self->prepExec("select shortName from gamesNames where $quotedMod regexp regex");
  my @found=$sth->fetchrow_array();
  return $found[0] if(@found);
  return undef;
}

# Called by sldbLi.pl, ratingEngine.pl, fixModShortName()
sub getModsShortNames {
  my $self=shift;
  my $sth=$self->prepExec('select shortName from gamesNames','retrieve mods short names from gamesNames');
  my @shortNames;
  my @shortName;
  while(@shortName=$sth->fetchrow_array()) {
    push(@shortNames,$shortName[0]);
  }
  return \@shortNames;
}

# Called by sldbLi.pl, xmlRpc.pl
sub fixModShortName {
  my ($self,$modShortName)=@_;
  my $p_allowedMods=$self->getModsShortNames();
  for my $msn (@{$p_allowedMods}) {
    return $msn if(lc($modShortName) eq lc($msn));
  }
  return;
}

# Called by sldbLi.pl, xmlRpc.pl
sub fixGameType {
  my ($self,$gameType)=@_;
  foreach my $gt (keys %gameTypeMapping) {
    return $gt if(lc($gameType) eq lc($gt));
  }
  return;
}

#######################
# Statistics function #
#######################

# Called by xmlRpc.pl
sub getPlayerStats {
  my ($self,$accountId,$modShortName,$mode)=@_;
  
  my $userId=$self->getUserId($accountId);
  if(! defined $userId) {
    $self->log("getPlayerStats called for an unknown ID \"$accountId\"",2);
    return {};
  }

  if(! defined $mode) {
    my $userPrivacyMode;
    $self->getUserPref($accountId,'privacyMode',\$userPrivacyMode);
    if($userPrivacyMode) {
      $mode='account';
    }else{
      $mode='user';
    }
  }

  my $sqlWherePart;
  if($mode eq 'user') {
    $sqlWherePart=", userAccounts ua where ua.userId=$userId and ua.accountId=pd.accountId";
  }else{
    $sqlWherePart=" where pd.accountId=$accountId";
  }
  my $quotedModShortName=$self->quote($modShortName);
  
  my %results;
  foreach my $gameType (keys %gameTypeMapping) {
    next if($gameType eq 'Global');
    $results{$gameType}={won => 0, lost => 0, draw => 0};
  }
  my @resultMapping=('lost','won','draw');
  my $sth=$self->prepExec("select gd.type,pd.win,count(*) from games g,gamesNames gn, gamesDetails gd, playersDetails pd$sqlWherePart and pd.gameId=gd.gameId and gd.type!='Solo' and gd.bots=0 and gd.undecided=0 and gd.cheating=0 and gd.gameId=g.gameId and g.modName regexp gn.regex and gn.shortName=$quotedModShortName and pd.team is not null group by gd.type,pd.win","extract players stats data from games,gamesNames,gamesDetails,playersDetails,userAccounts tables");
  my @sqlResults;
  while(@sqlResults=$sth->fetchrow_array()) {
    my ($gameType,$result,$count)=@sqlResults;
    $result=$resultMapping[$result];
    $results{$gameType}->{$result}=$count;
  }

  return \%results;
}

# Called by xmlRpc.pl
sub getPlayerSkillGraphs {
  my ($self,$accountId,$modShortName)=@_;
  
  my $sth=$self->prepExec("select ua.userId,ud.name from userAccounts ua,userDetails ud where ua.accountId=$accountId and ua.userId=ud.userId");
  my @results=$sth->fetchrow_array();
  if(! @results) {
    $self->log("getPlayerSkillGraphs called for an unknown ID \"$accountId\"",2);
    return undef;
  }
  my ($userId,$userName)=@results;
  return $self->generateSkillGraphs($userId,$userName,$modShortName);
}

# Called by sldbLi.pl, getPlayerSkillGraphs()
sub generateSkillGraphs {
  my ($self,$userId,$userName,$modShortName,$tmpDir)=@_;

  if(! defined $chartClickerUnavailable) {
    eval <<'END_OF_EVAL_LIST';
    use Chart::Clicker;
    use Chart::Clicker::Context;
    use Chart::Clicker::Data::DataSet;
    use Chart::Clicker::Data::Series;
    use Chart::Clicker::Drawing::ColorAllocator;
    use Chart::Clicker::Renderer::Line;
    use Chart::Clicker::Renderer::StackedArea;
    use Graphics::Color::RGB;
    use Graphics::Primitive::Font;
END_OF_EVAL_LIST
    $chartClickerUnavailable=$@;
    $self->log("Chart::Clicker module could not be loaded, skill graph functionality disabled: $chartClickerUnavailable",1) if($chartClickerUnavailable);
  }

  return undef if($chartClickerUnavailable);

  my $quotedModShortName=$self->quote($modShortName);
  my $sth;
  my @skillGraphsFiles;
  my %skillGraphsData;
  foreach my $gameType (keys %gameTypeMapping) {
    my $gType=$gameTypeMapping{$gameType};
    $sth=$self->prepExec("select muBefore,sigmaBefore from ts${gType}Games where userId=$userId and modShortName=$quotedModShortName order by gdrTimestamp limit 1","retrieve initial skill data for mod $modShortName, user $userId and game type $gameType from table ts${gType}Games");
    my @result=$sth->fetchrow_array();
    next unless(@result);
    my ($initMu,$initSigma)=@result;
  
    $sth=$self->prepExec("select period,skill,mu,sigma from ts${gType}Players where modShortName=$quotedModShortName and userId=$userId order by period","retrieve historical skill data for mod $modShortName, user $userId and game type $gameType from ts${gType}Players table");
    my @periods;
    my %estimatedSkills;
    my %trustedSkills;
    my %skillRegions;
    my $index=0;
    $estimatedSkills{0}=$initMu;
    $trustedSkills{0}=$initMu-3*$initSigma;
    $skillRegions{0}=6*$initSigma;
    while(@result=$sth->fetchrow_array()) {
      $index++;
      if($result[0]=~/^(\d{4})(\d\d)$/) {
        push(@periods,"$1-$2");
      }else{
        $self->log("Invalid period string \"$result[0]\" encountered in generateSkillGraphs for mod $modShortName, user $userId and game type $gameType",1);
        return undef;
      }
      $estimatedSkills{$index}=$result[2];
      $trustedSkills{$index}=$result[2]-3*$result[3];
      $skillRegions{$index}=6*$result[3];
    }
    next if($index < 2);

    my $ca = Chart::Clicker::Drawing::ColorAllocator->new( {
      colors => [ Graphics::Color::RGB->new(red => 0.9, green => 0.9, blue => 0.9, alpha => 0),
                  Graphics::Color::RGB->new(red => 0.2, green => 0.2, blue => 1.0, alpha => 0.5),
                  Graphics::Color::RGB->new(red => 0, green => 0, blue => 1, alpha => 0.5) ] } );

    my $cc = Chart::Clicker->new(width => 1024, height => 512, format => 'png', color_allocator => $ca);

    my $defctx = $cc->get_context('default');
    $defctx->range_axis->range->min(0);
    $defctx->range_axis->range->max(50);
    $defctx->range_axis->ticks(10);
    $defctx->range_axis->format(sub { return int(shift); });
    $defctx->range_axis->label('TrueSkill');
    $defctx->range_axis->label_color(Graphics::Color::RGB->new(red => 0, green => 0, blue => 1, alpha => 1));
    $defctx->range_axis->label_font->weight('bold');
    $defctx->domain_axis->tick_values([1..$index-1]);
    $defctx->domain_axis->tick_labels(\@periods);
    $defctx->domain_axis->tick_label_angle(0.785);
    $defctx->domain_axis->label('Time');
    $defctx->domain_axis->label_font->weight('bold');

    my $stackedCtx=Chart::Clicker::Context->new( name => 'stacked' );
    my $stackedRenderer=Chart::Clicker::Renderer::StackedArea->new(opacity => 0.5);
    $stackedRenderer->brush->width(0);
    $stackedCtx->renderer($stackedRenderer);
    $stackedCtx->share_axes_with($defctx);
    $cc->add_to_contexts($stackedCtx);

    my $stackedSeries1=Chart::Clicker::Data::Series->new(\%trustedSkills);
    my $stackedSeries2=Chart::Clicker::Data::Series->new(\%skillRegions);
    my $stackedDs=Chart::Clicker::Data::DataSet->new(series => [$stackedSeries1,$stackedSeries2]);
    $stackedDs->context('stacked');
    $cc->add_to_datasets($stackedDs);

    my $lineRenderer = Chart::Clicker::Renderer::Line->new();
    $lineRenderer->brush->width(3);
    $defctx->renderer($lineRenderer);

    my $lineSeries = Chart::Clicker::Data::Series->new(\%estimatedSkills);
    my $lineDs=Chart::Clicker::Data::DataSet->new(series => [$lineSeries]);
    $cc->add_to_datasets($lineDs);

    $cc->title->text("$modShortName $gameType TrueSkill graph for $userName (\#$userId)");
    $cc->title->font->weight('bold');
    $cc->title->font->size(20);
    $cc->title->padding->top(5);
    $cc->title->padding->bottom(5);
    $cc->legend->visible(0);
    if(defined $tmpDir) {
      my $graphName="SkillGraph_${modShortName}_${gameType}_$userId.png";
      $cc->write_output("$tmpDir/$graphName");
      push(@skillGraphsFiles,$graphName);
    }else{
      $cc->draw;
      $skillGraphsData{$gameType}=$cc->rendered_data;
    }
  }

  if(defined $tmpDir) {
    return \@skillGraphsFiles;
  }else{
    return \%skillGraphsData;
  }
}

################################
# Rating data access functions #
################################

# Called by sldbLi.pl, ratingEngine.pl, getCurrentRatingYearMonth()
sub getRatingState {
  my $self=shift;
  my $sth=$self->prepExec('select param,value from tsRatingState','retrieve rating state from tsRatingState');
  my %ratingState;
  my @data;
  while(@data=$sth->fetchrow_array()) {
    $ratingState{$data[0]}=$data[1];
  }
  return \%ratingState;
}

# Called by sldbLi.pl, getCurrentRatingPeriod()
sub getCurrentRatingYearMonth {
  my $self=shift;
  my $p_ratingState=$self->getRatingState();
  my ($currentRatingYear,$currentRatingMonth);
  if(! exists $p_ratingState->{currentRatingYear}) {
    $self->log("Unable to retrieve current rating year from rating state table, using current year instead!",2);
    $currentRatingYear=(localtime())[5]+1900;
  }else{
    $currentRatingYear=$p_ratingState->{currentRatingYear};
  }
  if(! exists $p_ratingState->{currentRatingMonth}) {
    $self->log("Unable to retrieve current rating month from rating state table, using current month instead!",2);
    $currentRatingMonth=(localtime())[4]+1;
  }else{
    $currentRatingMonth=$p_ratingState->{currentRatingMonth};
  }
  return ($currentRatingYear,$currentRatingMonth);
}

# Called by sldbLi.pl, xmlRpc.pl
sub getCurrentRatingPeriod {
  my $self=shift;
  my ($currentRatingYear,$currentRatingMonth)=$self->getCurrentRatingYearMonth();
  $currentRatingMonth=sprintf('%02d',$currentRatingMonth);
  return $currentRatingYear.$currentRatingMonth;
}

# Called by sldbLi.pl
sub getLatestRatedGameId {
  my ($self,$accountId,$quotedModShortName)=@_;
  my $sth=$self->prepExec("select gameId from tsGames where accountId=$accountId and modShortName=$quotedModShortName order by gdrTimestamp desc limit 1");
  my @foundData=$sth->fetchrow_array();
  return $foundData[0] if(@foundData);
  return '';
}

# Called by sldbLi.pl, slMonitor.pl
sub queueGlobalRerate {
  my ($self,$accountId)=@_;
  $self->log("Queuing a global rerate for account $accountId",3);
  $self->do("insert into tsRerateAccounts values ($accountId,now()) on duplicate key update accountTimestamp=now()","add account \"$accountId\" in tsRerateAccounts table");
}

# Called by sldbLi.pl, xmlRpc.pl
sub getSkills {
  my ($self,$period,$id,$ip,$quotedModShortName)=@_;

  my $userId=$self->getUserId($id);
  if(! defined $userId) {
    $self->log("getSkills called for an unknown ID \"$id\"",2);
    return {};
  }

  my $userSkill;
  my $sth=$self->prepExec("select skill,sigma from tsPlayers where period=$period and modShortName=$quotedModShortName and userId=$userId");
  my @foundData=$sth->fetchrow_array();
  $userSkill=$foundData[0] if(@foundData);

  if(! defined $userSkill || $foundData[1] > 25/9) {
    my $p_smurfs=$self->getUserSmurfs($userId,2);
    my @smurfs=@{$p_smurfs};

    if(defined $ip && $ip ne '') {
      my $quotedIp=$self->quote($ip);
      my $p_notSmurfs=$self->getUserSmurfs($userId,0);
      my @notSmurfs=@{$p_notSmurfs};
      my $smurfFilter=join(',',@smurfs,@notSmurfs);
      $smurfFilter=" and ua.userId not in ($smurfFilter)" if($smurfFilter);
      $sth=$self->prepExec("select ua.userId from accounts a,userAccounts ua,ips,tsPlayers tsp where tsp.period=$period and ua.accountId=a.id and ua.accountId=ips.accountId and ua.userId=tsp.userId and bot=0 and noSmurf=0 and modShortName=$quotedModShortName and ua.userId != $userId$smurfFilter and ip=INET_ATON($quotedIp) order by skill desc limit 1");
      @foundData=$sth->fetchrow_array();
      if(@foundData) {
        push(@smurfs,$foundData[0]);
        $smurfFilter=join(',',@smurfs,@notSmurfs);
        $smurfFilter=" and ua.userId not in ($smurfFilter)";
      }
      $sth=$self->prepExec("select ua.userId from accounts a,userAccounts ua,userIpRanges ipr,tsPlayers tsp where tsp.period=$period and ua.accountId=a.id and ua.accountId=ipr.userId and ua.userId=tsp.userId and bot=0 and noSmurf=0 and modShortName=$quotedModShortName and ua.userId != $userId$smurfFilter and ip1 <= INET_ATON($quotedIp) and ip2 >= INET_ATON($quotedIp) order by skill desc limit 1");
      @foundData=$sth->fetchrow_array();
      push(@smurfs,$foundData[0]) if(@foundData);
    }

    if(@smurfs) {
      my $smurfsString=join(',',@smurfs);
      $sth=$self->prepExec("select userId,skill from tsPlayers where period=$period and userId in ($smurfsString) and modShortName=$quotedModShortName order by skill desc limit 1");
      @foundData=$sth->fetchrow_array();
      ($userId,$userSkill)=@foundData unless(! @foundData || (defined $userSkill && $userSkill >= $foundData[1]));
    }
  }

  if(! defined $userSkill) {
    $self->log("getSkills called for non-rated user ID \"$userId\"",3);
    my %rankInitSkills=(0 => 20,
                        1 => 22,
                        2 => 23,
                        3 => 24,
                        4 => 25,
                        5 => 26,
                        6 => 28,
                        7 => 30);
    $sth=$self->prepExec("select rank from accounts where id=$userId");
    @foundData=$sth->fetchrow_array();
    if(@foundData) {
      my $skillInit=$rankInitSkills{$foundData[0]};
      my %skills;
      foreach my $gameType (keys %gameTypeMapping) {
        $skills{$gameType}={mu => $skillInit, sigma => 25/3};
      }
      return \%skills;
    }else{
      $self->log("Unable to find rank of unrated player (id:$id, userId:$userId)",1);
      return {};
    }
  }

  my $getSkillErrors=0;
  my %skills;
  foreach my $gameType (keys %gameTypeMapping) {
    my $gType=$gameTypeMapping{$gameType};
    $sth=$self->prepExec("select mu,sigma from ts${gType}Players where period=$period and modShortName=$quotedModShortName and userId=$userId");
    @foundData=$sth->fetchrow_array();
    if(! @foundData && $getSkillErrors < 10) {
      $getSkillErrors++;
      Time::HiRes::usleep(50000);
      redo;
    }
    $skills{$gameType}={mu => $foundData[0], sigma => $foundData[1]};
    if($gameType eq 'TeamFFA' && $foundData[1] > 25/6) {
      my $teamFfaCorrectionFactor=($foundData[1]-25/6)/(25/3);
      if($teamFfaCorrectionFactor > 1) {
        $self->log("TeamFFA correction factor > 1 for user ID \"$userId\" !",2);
        $teamFfaCorrectionFactor=1;
      }
      $sth=$self->prepExec("select mu from tsPlayers where period=$period and modShortName=$quotedModShortName and userId=$userId");
      @foundData=$sth->fetchrow_array();
      $skills{TeamFFA}->{mu}=$teamFfaCorrectionFactor*$foundData[0]+(1-$teamFfaCorrectionFactor)*$skills{TeamFFA}->{mu};
    }
  }

  $self->log("Encountered $getSkillErrors lookup failure".($getSkillErrors>1?'s':'')." while fetching skills for $userId (concurrent rerate in progress?)",2) if($getSkillErrors);

  return \%skills;
}

# Called by xmlRpc.pl
sub getBattleSkills {
  my ($self,$gameId)=@_;
  my $quotedGameId=$self->quote($gameId);
  my $sth=$self->prepExec("select gd.type,gn.shortName from games g,gamesDetails gd, gamesNames gn where g.gameId=$quotedGameId and gd.gameId=$quotedGameId and g.modName regexp gn.regex","retrieve type and mod short name for game ID \"$gameId\"");
  my @gameData=$sth->fetchrow_array();
  if(! @gameData) {
    $self->log("getBattleSkills called for an unknown game ID \"$gameId\" or unratable mod",2);
    return {};
  }
  my ($gameType,$gameMod)=@gameData;
  if(! exists $gameTypeMapping{$gameType}) {
    $self->log("getBattleSkills called for an unratable game type (gameId=\"$gameId\", gameType=\"$gameType\")",2);
    return {};
  }
  my $gType=$gameTypeMapping{$gameType};

  $sth=$self->prepExec("select accountId,muBefore,sigmaBefore,muAfter,sigmaAfter from tsGames where gameId=$quotedGameId","retrieve global skills for game ID \"$gameId\"");
  my %globalSkills;
  my @data;
  while(@data=$sth->fetchrow_array()) {
    $globalSkills{$data[0]}={muBefore => $data[1], sigmaBefore => $data[2], muAfter => $data[3], sigmaAfter => $data[4]};
  }
  if(! %globalSkills) {
    $self->log("getBattleSkills called for an unrated game ID \"$gameId\"",2);
    return {};
  }

  $sth=$self->prepExec("select accountId,muBefore,sigmaBefore,muAfter,sigmaAfter from ts${gType}Games where gameId=$quotedGameId","retrieve $gameType skills for game ID \"$gameId\"");
  my %specificSkills;
  while(@data=$sth->fetchrow_array()) {
    $specificSkills{$data[0]}={muBefore => $data[1], sigmaBefore => $data[2], muAfter => $data[3], sigmaAfter => $data[4]};
  }
  if(! %specificSkills) {
    $self->log("getBattleSkills called for an unrated $gameType game ID \"$gameId\"",2);
    return {};
  }

  return {gameMod => $gameMod, gameType => $gameType, globalSkills => \%globalSkills, specificSkills => \%specificSkills};
}

# Called by sldbLi.pl, xmlRpc.pl
sub getLeaderboard {
  my ($self,$period,$modShortName,$gameType,$skillType,$mode,$size)=@_;
  
  $gameType='Global' unless(defined $gameType);
  if(! exists $gameTypeMapping{$gameType}) {
    $self->log("getLeaderboard called with an invalid game type \"$gameType\"",2);
    return;
  }
  $skillType='trusted' unless(defined $skillType);
  if($skillType eq 'trusted') {
    $skillType='skill';
  }elsif($skillType eq 'estimated') {
    $skillType='mu';
  }else{
    $self->log("getLeaderboard called with an invalid skillType \"$skillType\"",2);
    return;
  }
  $mode='top' unless(defined $mode);
  if($mode eq 'top') {
    $mode='desc';
  }elsif($mode eq 'bottom') {
    $mode='asc';
  }else{
    $self->log("getLeaderboard called with an invalid mode \"$mode\"",2);
    return;
  }
  $size=20 unless(defined $size);
  if($size !~ /^\d+$/) {
    $self->log("getLeaderboard called with an invalid size \"$size\"",2);
    return;
  }

  my $quotedModShortName=$self->quote($modShortName);
  my $gType=$gameTypeMapping{$gameType};
  my $sth=$self->prepExec("select tsp.userId,ud.name,tsp.nbPenalties,tsp.skill,tsp.mu,tsp.sigma from ts${gType}Players tsp,userDetails ud where tsp.period=$period and tsp.userId=ud.userId and tsp.modShortName=$quotedModShortName order by tsp.$skillType $mode limit $size","extract leaderboard data from ts${gType}Players and userDetails tables");

  my @results;
  my @resultData;
  while(@results=$sth->fetchrow_array()) {
    push(@resultData,{ userId => $results[0],
                       name => $results[1],
                       inactivity => $results[2],
                       trustedSkill => $results[3],
                       estimatedSkill => $results[4],
                       uncertainty => $results[5] });
  }
  return \@resultData;
}

# Called by sldbLi.pl
sub getTopPlayers {
  my ($self,$period,$modShortName,$gameType,$size)=@_;

  $gameType='Global' unless(defined $gameType);
  if(! exists $gameTypeMapping{$gameType}) {
    $self->log("getTopPlayers called with an invalid game type \"$gameType\"",2);
    return;
  }
  $size=20 unless(defined $size);
  if($size !~ /^\d+$/) {
    $self->log("getTopPlayers called with an invalid size \"$size\"",2);
    return;
  }
  my $quotedModShortName=$self->quote($modShortName);
  my $gType=$gameTypeMapping{$gameType};
  my $sth=$self->prepExec("select userId from ts${gType}Players where period=$period and modShortName=$quotedModShortName order by skill desc limit $size","extract top players data from ts${gType}Players table");
  my @topPlayers;
  my @results;
  while(@results=$sth->fetchrow_array()) {
    push(@topPlayers,$results[0]);
  }
  return \@topPlayers;
}

##############################
# Smurf management functions #
##############################

sub ipToInt {
  my ($self,$ip)=@_;
  my $int=0;
  $int=$1*(256**3)+$2*(256**2)+$3*256+$4 if ($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  return $int;
}

# Called by computeAllUserIps()
sub isReservedIpNb {
  my ($self,$ipNb)=@_;
  my @reservedIps=(['0.0.0.0','0.255.255.255'],
                   ['10.0.0.0','10.255.255.255'],
                   ['100.64.0.0','100.127.255.255'],
                   ['127.0.0.0','127.255.255.255'],
                   ['169.254.0.0','169.254.255.255'],
                   ['172.16.0.0','172.31.255.255'],
                   ['192.0.0.0','192.0.0.255'],
                   ['192.0.2.0','192.0.2.255'],
                   ['192.168.0.0','192.168.255.255'],
                   ['198.18.0.0','198.19.255.255'],
                   ['198.51.100.0','198.51.100.255'],
                   ['203.0.113.0','203.0.113.255'],
                   ['224.0.0.0','239.255.255.255'],
                   ['255.255.255.255','255.255.255.255']);
  foreach my $p_reservedRange (@reservedIps) {
    return 1 if($self->ipToInt($p_reservedRange->[0]) <= $ipNb && $ipNb <= $self->ipToInt($p_reservedRange->[1]));
  }
  return 0;
}

# Called by computeAllUserIps()
sub getFirstRangeAddr {
  my $ip=shift;
  $ip-=$ip%256;
  return $ip;
}

# Called by computeAllUserIps()
sub getLastRangeAddr {
  my $ip=shift;
  $ip=$ip-($ip%256)+255;
  return $ip;
}

# Called by sldbLi.pl, slMonitor.pl
sub computeAllUserIps {
  my ($self,$userId,$dynIpThreshold,$dynIpRange)=@_;
  $self->do("delete from userIps where userId=$userId","flush ip addresses for userId \"$userId\" in table userIps");
  $self->do("delete from userIpRanges where userId=$userId","flush ip addresses for userId \"$userId\" in table userIpRanges");
  $self->do("update userDetails set nbIps=0 where userId=$userId","reset IP counter in userDetails table for userId \"$userId\"");
  my $sth=$self->prepExec("select pd.ip,UNIX_TIMESTAMP(max(gd.endTimestamp)) from playersDetails pd,gamesDetails gd,userAccounts ua where ua.userId=$userId and ua.accountId=pd.accountId and pd.ip is not null and pd.ip != 0 and pd.gameId=gd.gameId group by pd.ip order by pd.ip","list all user IPs from tables playersDetails,gamesDetails,userAccounts for userId $userId");
  my $p_results=$sth->fetchall_arrayref();
  my @seenIps;
  foreach my $p_result (@{$p_results}) {
    next if($self->isReservedIpNb($p_result->[0]));
    push(@seenIps,$p_result);
  }
  return unless(@seenIps);
  my $nbIps=$#seenIps+1;
  if($nbIps < $dynIpThreshold) {
    foreach my $p_seenIp (@seenIps) {
      $self->do("insert into userIps values ($userId,$p_seenIp->[0],FROM_UNIXTIME($p_seenIp->[1]))","insert new ip \"$p_seenIp->[0]\" for userId \"$userId\" in table userIps");
    }
    $self->do("update userDetails set nbIps=$nbIps where userId=$userId","set IP counter to $nbIps for userId \"$userId\" in userDetails table");
  }else{
    my (@isolatedIps,@ranges);
    my ($rangeStart,$rangeEnd,$rangeTs)=(0,0,0);
    foreach my $p_seenIp (@seenIps) {
      if($rangeEnd) {
        if(getFirstRangeAddr($p_seenIp->[0])-$rangeEnd <= $dynIpRange) {
          $rangeEnd=getLastRangeAddr($p_seenIp->[0]);
          $rangeTs=$p_seenIp->[1] if($p_seenIp->[1] > $rangeTs);
        }else{
          push(@ranges,[$rangeStart,$rangeEnd,$rangeTs]);
          ($rangeStart,$rangeEnd,$rangeTs)=($p_seenIp->[0],0,$p_seenIp->[1]);
        }
      }elsif($rangeStart) {
        if(getFirstRangeAddr($p_seenIp->[0])-$rangeStart <= $dynIpRange) {
          $rangeStart=getFirstRangeAddr($rangeStart);
          $rangeEnd=getLastRangeAddr($p_seenIp->[0]);
          $rangeTs=$p_seenIp->[1] if($p_seenIp->[1] > $rangeTs);
        }else{
          push(@isolatedIps,[$rangeStart,$rangeTs]);
          ($rangeStart,$rangeTs)=($p_seenIp->[0],$p_seenIp->[1]);
        }
      }else{
        ($rangeStart,$rangeTs)=($p_seenIp->[0],$p_seenIp->[1]);
      }
    }
    if($rangeEnd) {
      push(@ranges,[$rangeStart,$rangeEnd,$rangeTs]);
    }elsif($rangeStart) {
      push(@isolatedIps,[$rangeStart,$rangeTs]);
    }else{
      error("Inconsistent state while processing dynamic user ip ranges detection");
    }
    foreach my $p_isolatedIp (@isolatedIps) {
      $self->do("insert into userIps values ($userId,$p_isolatedIp->[0],FROM_UNIXTIME($p_isolatedIp->[1]))","insert ip addresses for user \"$userId\" in table userIps for dynamic ip ranges detection");
    }
    foreach my $p_range (@ranges) {
      $self->do("insert into userIpRanges values ($userId,$p_range->[0],$p_range->[1],FROM_UNIXTIME($p_range->[2]))","insert ip ranges for user \"$userId\" in table userIpRanges for dynamic ip ranges detection");
    }
    $self->do("update userDetails set nbIps=$dynIpThreshold where userId=$userId","set nbIps to $dynIpThreshold for userId \"$userId\" in userDetails table");
  }
}

# Called by sldbLi.pl
sub getAccountIps {
  my ($self,$accId)=@_;

  my $sth=$self->prepExec("select ip,lastSeen from ips where accountId=$accId","query ips for ips of account \"$accId\"");
  my %ips;
  my @ipData;
  while(@ipData=$sth->fetchrow_array()) {
    $ips{$ipData[0]}={lastSeen => $ipData[1]};
  }

  $sth=$self->prepExec("select ip1,ip2,lastSeen from ipRanges where accountId=$accId","query ipRanges for ip ranges of account \"$accId\"");
  while(@ipData=$sth->fetchrow_array()) {
    $ips{$ipData[0]}={ip2 => $ipData[1], lastSeen => $ipData[2]};
  }

  return \%ips;
}

# Called by sldbLi.pl
sub getUserIps {
  my ($self,$userId)=@_;

  my $sth=$self->prepExec("select ip,lastSeen from userIps where userId=$userId","query userIps for ips of user \"$userId\"");
  my %ips;
  my @ipData;
  while(@ipData=$sth->fetchrow_array()) {
    $ips{$ipData[0]}={lastSeen => $ipData[1]};
  }

  $sth=$self->prepExec("select ip1,ip2,lastSeen from userIpRanges where userId=$userId","query userIpRanges for ip ranges of user \"$userId\"");
  while(@ipData=$sth->fetchrow_array()) {
    $ips{$ipData[0]}={ip2 => $ipData[1], lastSeen => $ipData[2]};
  }

  return \%ips;
}

# Called by getSkills()
sub getUserSmurfs {
  my ($self,$userId,$status)=@_;
  my %smurfsHash;
  my $sth=$self->prepExec("select s.id2 from smurfs s,userAccounts ua where ua.userId=$userId and ua.accountId=s.id1 and status=$status");
  my @results;
  while(@results=$sth->fetchrow_array()) {
    $smurfsHash{$results[0]}=1;
  }
  $sth=$self->prepExec("select s.id1 from smurfs s,userAccounts ua where ua.userId=$userId and ua.accountId=s.id2 and status=$status");
  while(@results=$sth->fetchrow_array()) {
    $smurfsHash{$results[0]}=1;
  }
  my @smurfs=keys %smurfsHash;
  my $p_userSmurfs=$self->getUserIds(\@smurfs);
  return $p_userSmurfs;
}

# Called by sldbLi.pl
sub getUsersSmurfStates {
  my ($self,$userId1,$userId2)=@_;
  my $p_accounts1=$self->getUserAccounts($userId1);
  my $p_accounts2=$self->getUserAccounts($userId2);
  my $user1AccountsString=join(',',@{$p_accounts1});
  my $user2AccountsString=join(',',@{$p_accounts2});
  my $sth=$self->prepExec("select status,id1,id2,orig from smurfs where ((id1 in ($user1AccountsString) and id2 in ($user2AccountsString)) or (id1 in ($user2AccountsString) and id2 in ($user1AccountsString)))","check smurfs state of users \"$userId1\" ($user1AccountsString) and \"$userId2\" ($user2AccountsString) in smurfs table");
  my %statusFound;
  my @foundData;
  while(@foundData=$sth->fetchrow_array()) {
    $statusFound{$foundData[0]}=[] unless(exists $statusFound{$foundData[0]});
    push(@{$statusFound{$foundData[0]}},[$foundData[1],$foundData[2],$foundData[3]]);
  }
  return \%statusFound;
}

# Called by sldbLi.pl
sub deleteUsersSmurfStates {
  my ($self,$userId1,$userId2)=@_;
  my $p_accounts1=$self->getUserAccounts($userId1);
  my $p_accounts2=$self->getUserAccounts($userId2);
  my $user1AccountsString=join(',',@{$p_accounts1});
  my $user2AccountsString=join(',',@{$p_accounts2});
  $self->do("delete from smurfs where (id1 in ($user1AccountsString) and id2 in ($user2AccountsString)) or (id1 in ($user2AccountsString) and id2 in ($user1AccountsString))","remove smurf state entries between users ($userId1 <-> $userId2)");
}

# Called by sldbLi.pl
sub getAccountsSmurfState {
  my ($self,$accountId1,$accountId2)=@_;
  my ($id1,$id2)= $accountId1 < $accountId2 ? ($accountId1,$accountId2) : ($accountId2,$accountId1);
  my $sth=$self->prepExec("select status,orig from smurfs where id1=$id1 and id2=$id2","check smurfs state of accounts \"$id1\" and \"$id2\" in smurfs table");
  my @foundData=$sth->fetchrow_array();
  return \@foundData;
}

# Called by sldbLi.pl
sub deleteAccountsSmurfState {
  my ($self,$accountId1,$accountId2)=@_;
  my ($id1,$id2)= $accountId1 < $accountId2 ? ($accountId1,$accountId2) : ($accountId2,$accountId1);
  $self->do("delete from smurfs where id1=$id1 and id2=$id2");
}

# Called by sldbLi.pl
sub getSimultaneousUserGames {
  my ($self,$userId1,$userId2)=@_;
  my @simultaneousGames;
  my $sth=$self->prepExec("select pd1.gameId,pd1.accountId,pd2.accountId from playersDetails pd1,playersDetails pd2,userAccounts ua1, userAccounts ua2, games g where ua1.userId=$userId1 and ua2.userId=$userId2 and ua1.accountId=pd1.accountId and ua2.accountId=pd2.accountId and pd1.gameId=pd2.gameId and pd1.gameId=g.gameId and pd1.team is not null and pd1.ip is not null and pd1.ip != 0 and pd2.team is not null and pd2.ip is not null and pd2.ip != 0 order by g.endTimestamp desc limit 10","check if users $userId1 and $userId2 have already played in same game in playersDetails table");
  my @foundData;
  while(@foundData=$sth->fetchrow_array()) {
    push(@simultaneousGames,{gameId => $foundData[0], id1 => $foundData[1], id2 => $foundData[2]});
  }
  return \@simultaneousGames;
}

# Called by sldbLi.pl
sub chooseMainUserId {
  my ($self,$userId1,$userId2)=@_;
  my $sth=$self->prepExec("select ua.userId from accounts a,userAccounts ua where a.id=ua.accountId and ua.userId in ($userId1,$userId2) group by userId order by bot,max(rank) desc,ua.userId limit 1","choose best user ID between $userId1 and $userId2");
  my @result=$sth->fetchrow_array();
  return $result[0];
}

# Called by sldbLi.pl
sub chooseMainAccountId {
  my ($self,$p_accountIds)=@_;
  my $accountsString=join(',',@{$p_accountIds});
  my $sth=$self->prepExec("select id from accounts where id in ($accountsString) order by bot,rank desc,id limit 1","choose best account ID among $accountsString");
  my @result=$sth->fetchrow_array();
  return $result[0];
}

# Called by sldbLi.pl, slMonitor.pl
sub findAvailableUserName {
  my ($self,$name)=@_;
  my $quotedName=$self->quote($name);
  my $sth=$self->prepExec("select count(*) from userDetails where name=$quotedName","check if name \"$name\" is already known in userDetails");
  my @udCount=$sth->fetchrow_array();
  if($udCount[0] > 0) {
    my ($i,$newName)=(0,'');
    while($udCount[0] > 0) {
      $i++;
      $newName="$name($i)";
      $quotedName=$self->quote($newName);
      $sth=$self->prepExec("select count(*) from userDetails where name=$quotedName","check if name \"$newName\" is already known in userDetails");
      @udCount=$sth->fetchrow_array();
    }
    $name=$newName;
  }
  return $name;
}

# Called by sldbLi.pl
sub getUserSmurfIdsByName {
  my ($self,$userId,$name)=@_;
  my $quotedName=$self->quote($name);
  my $sth=$self->prepExec("select ua.accountId from userAccounts ua,names n where ua.userId=$userId and ua.accountId != $userId and ua.accountId=n.accountId and n.name=$quotedName");
  my @accountIds;
  my @results;
  while(@results=$sth->fetchrow_array()) {
    push(@accountIds,$results[0]);
  }
  return \@accountIds;
}

# Called by sldbLi.pl
sub getProbableSmurfs {
  my $self=shift;
  my @probableSmurfs;
  my $sth=$self->prepExec("select id1,id2,orig from smurfs where status=2 order by id1,id2");
  my @results;
  while(@results=$sth->fetchrow_array()) {
    push(@probableSmurfs,{id1 => $results[0],
                          id2 => $results[1],
                          orig => $results[2]});
  }
  return \@probableSmurfs;
}

# Called by getUserOrderedSmurfGroups()
sub getLatestAccountCpu {
  my ($self,$accId)=@_;
  my $sth=$self->prepExec("select cpu from cpus where accountId=$accId and lastConnection in (select max(lastConnection) from cpus where accountId=$accId)","read cpus table to get latest cpu for account \"$accId\""); 
  my @foundData=$sth->fetchrow_array();
  if(! @foundData) {
    $self->log("Unable to find latest cpu for account \"$accId\"",2);
    return 0;
  }else{
    return $foundData[0];
  }
}

# Called by getUserOrderedSmurfGroups()
sub getTrueSmurfsByIP {
  my ($self,$p_trueSmurfs,$p_accountsToTest,$currentLevel)=@_;

  $currentLevel=1 unless(defined $currentLevel);
  
  return [] unless(%{$p_accountsToTest});

  my $trueSmurfsString=join(',',(keys %{$p_trueSmurfs}));
  my $accountsToTestString=join(',',(keys %{$p_accountsToTest}));
  my $sth=$self->prepExec("select distinct(ips1.accountId) from ips ips1,ips ips2 where ips1.ip=ips2.ip and ips1.accountId in ($accountsToTestString) and ips2.accountId in ($trueSmurfsString)","check true smurfs for \"$trueSmurfsString\" among \"$accountsToTestString\" in ips table");

  my @newTrueSmurfsFound;
  my @dataFound;
  while(@dataFound=$sth->fetchrow_array()) {
    push(@newTrueSmurfsFound,$dataFound[0]);
    $p_trueSmurfs->{$dataFound[0]}=$currentLevel;
    delete $p_accountsToTest->{$dataFound[0]};
  }
  
  return [] unless(@newTrueSmurfsFound);
  my $p_nextTrueSmurfsFound=$self->getTrueSmurfsByIP($p_trueSmurfs,$p_accountsToTest,$currentLevel+1);
  return [@newTrueSmurfsFound,@{$p_nextTrueSmurfsFound}];
}

# Called by getUserOrderedSmurfGroups()
sub getProbableSmurfsByIP {
  my ($self,$p_probableSmurfs,$p_accountsToTest,$dynIpRange)=@_;
  
  return [] unless(%{$p_accountsToTest});

  my $probableSmurfsString=join(',',(keys %{$p_probableSmurfs}));
  my $accountsToTestString=join(',',(keys %{$p_accountsToTest}));
  my $sth=$self->prepExec("select distinct(ips1.accountId) from ipRanges ips1,ipRanges ips2 where ((ips1.ip1 >= ips2.ip1 and ips1.ip1 <= ips2.ip2+$dynIpRange) or (ips1.ip2 >= ips2.ip1-$dynIpRange and ips1.ip2 <= ips2.ip2) or (ips1.ip1 < ips2.ip1 and ips1.ip2 > ips2.ip2)) and ips1.accountId in ($accountsToTestString) and ips2.accountId in ($probableSmurfsString)","check probable smurfs for \"$probableSmurfsString\" among \"$accountsToTestString\" in ipRanges table");

  my @newProbableSmurfsFound;
  my @dataFound;
  while(@dataFound=$sth->fetchrow_array()) {
    push(@newProbableSmurfsFound,$dataFound[0]);
    $p_probableSmurfs->{$dataFound[0]}=1;
    delete $p_accountsToTest->{$dataFound[0]};
  }

  if(%{$p_accountsToTest}) {

    $probableSmurfsString=join(',',(keys %{$p_probableSmurfs}));
    $accountsToTestString=join(',',(keys %{$p_accountsToTest}));
    $sth=$self->prepExec("select distinct(ips.accountId) from ips,ipRanges where ips.ip >= ipRanges.ip1 and ips.ip <= ipRanges.ip2 and ips.accountId in ($accountsToTestString) and ipRanges.accountId in ($probableSmurfsString)","check probable smurfs for \"$probableSmurfsString\" among \"$accountsToTestString\" in ips and ipRanges tables");
    
    while(@dataFound=$sth->fetchrow_array()) {
      push(@newProbableSmurfsFound,$dataFound[0]);
      $p_probableSmurfs->{$dataFound[0]}=1;
      delete $p_accountsToTest->{$dataFound[0]};
    }
  
    if(%{$p_accountsToTest}) {
      $probableSmurfsString=join(',',(keys %{$p_probableSmurfs}));
      $accountsToTestString=join(',',(keys %{$p_accountsToTest}));
      $sth=$self->prepExec("select distinct(ipRanges.accountId) from ips,ipRanges where ips.ip >= ipRanges.ip1 and ips.ip <= ipRanges.ip2 and ips.accountId in ($probableSmurfsString) and ipRanges.accountId in ($accountsToTestString)","check probable smurfs for \"$probableSmurfsString\" among \"$accountsToTestString\" in ipRanges and ips tables");
      
      while(@dataFound=$sth->fetchrow_array()) {
        push(@newProbableSmurfsFound,$dataFound[0]);
        $p_probableSmurfs->{$dataFound[0]}=1;
        delete $p_accountsToTest->{$dataFound[0]};
      }
    }

  }
  
  return [] unless(@newProbableSmurfsFound);

  my $p_nextProbableSmurfsFound=$self->getProbableSmurfsByIP($p_probableSmurfs,$p_accountsToTest,$dynIpRange);
  return [@newProbableSmurfsFound,@{$p_nextProbableSmurfsFound}];

}

# Called by sldbLi.pl, slMonitor.pl
sub getUserOrderedSmurfGroups {
  my ($self,$userId,$p_conflictingAccounts,$dynIpRange)=@_;

  my $p_accounts=$self->getUserAccounts($userId);
  my @userAccounts=@{$p_accounts};
  my $accIdsString=join(',',@userAccounts);

  my $sth=$self->prepExec("select id1,id2,status from smurfs where id1 in ($accIdsString) and id2 in ($accIdsString)","read smurfs table to build smurf graph of \"$accIdsString\"");
  my $p_userSmurfData=$sth->fetchall_hashref(['id1','id2']);
  my %smurfGroups;
  my $nextSmurfGroup=0;
  foreach my $id1 (keys %{$p_userSmurfData}) {
    $smurfGroups{$id1}=$nextSmurfGroup++ if(! exists $smurfGroups{$id1});
    foreach my $id2 (keys %{$p_userSmurfData->{$id1}}) {
      if($p_userSmurfData->{$id1}->{$id2}->{status} == 1) {
        if(exists $smurfGroups{$id2}) {
          if($smurfGroups{$id1} != $smurfGroups{$id2}) {
            my ($replacedSmurfGroup,$newSmurfGroup)=$smurfGroups{$id1} > $smurfGroups{$id2} ? ($smurfGroups{$id1},$smurfGroups{$id2}) : ($smurfGroups{$id2},$smurfGroups{$id1});
            foreach my $id (keys %smurfGroups) {
              $smurfGroups{$id}=$newSmurfGroup if($smurfGroups{$id} == $replacedSmurfGroup);
            }
          }
        }else{
          $smurfGroups{$id2}=$smurfGroups{$id1};
        }
      }else{
        $self->log("Inconsistent smurf state! (\"$id1\" and \"$id2\" are smurfed through userId \"$userId\" in userAccounts, but their state is \"$p_userSmurfData->{$id1}->{$id2}->{status}\" in smurfs table)",2);
      }
    }
  }

  my %groupsByNb;
  foreach my $id (@userAccounts) {
    $smurfGroups{$id}=$nextSmurfGroup++ if(! exists $smurfGroups{$id});
    my $groupNb=$smurfGroups{$id};
    if(exists $groupsByNb{$groupNb}) {
      push(@{$groupsByNb{$groupNb}},$id);
    }else{
      $groupsByNb{$groupNb}=[$id];
    }
  }

  my @accountsInvolvedInConflict;
  my %conflictingGroupsByNb;
  foreach my $conflictingAccount (@{$p_conflictingAccounts}) {
    my $groupNb=$smurfGroups{$conflictingAccount};
    if(exists $conflictingGroupsByNb{$groupNb}) {
      push(@{$conflictingGroupsByNb{$groupNb}},$conflictingAccount);
    }else{
      $conflictingGroupsByNb{$groupNb}=[$conflictingAccount];
      push(@accountsInvolvedInConflict,@{$groupsByNb{$groupNb}});
    }
  }

  my %possibleTrueSmurfs;
  @possibleTrueSmurfs{@userAccounts}=((1) x ($#userAccounts+1));
  delete $possibleTrueSmurfs{$userId};

  my %userSmurfLevels=($userId => 0);

  $self->getTrueSmurfsByIP(\%userSmurfLevels,\%possibleTrueSmurfs);

  my $firstGroup;
  if(exists $conflictingGroupsByNb{$smurfGroups{$userId}}) {
    $firstGroup=$smurfGroups{$userId};
  }else{
    my @firstGroupPool=keys %conflictingGroupsByNb;

    my $minGroupDistance;
    my %groupNbByDistance;
    foreach my $groupNb (@firstGroupPool) {
      my $groupDistance;
      foreach my $id (@{$groupsByNb{$groupNb}}) {
        $groupDistance=$userSmurfLevels{$id} if(exists $userSmurfLevels{$id} && (! defined $groupDistance || $userSmurfLevels{$id} < $groupDistance));
      }
      if(defined $groupDistance) {
        $minGroupDistance=$groupDistance if(! defined $minGroupDistance || $groupDistance < $minGroupDistance);
        if(exists $groupNbByDistance{$groupDistance}) {
          push(@{$groupNbByDistance{$groupDistance}},$groupNb);
        }else{
          $groupNbByDistance{$groupDistance}=[$groupNb];
        }
      }
    }

    if(defined $minGroupDistance) {
      @firstGroupPool=@{$groupNbByDistance{$minGroupDistance}};
      $firstGroup=$firstGroupPool[0] if($#firstGroupPool == 0);
    }

    if(! defined $firstGroup) {
      my $maxGroupSize=0;
      my %groupNbsBySize;
      foreach my $groupNb (@firstGroupPool) {
        my $groupSize=$#{$groupsByNb{$groupNb}}+1;
        $maxGroupSize=$groupSize if($groupSize > $maxGroupSize);
        if(exists $groupNbsBySize{$groupSize}) {
          push(@{$groupNbsBySize{$groupSize}},$groupNb);
        }else{
          $groupNbsBySize{$groupSize}=[$groupNb];
        }
      }
      my @biggestGroups=@{$groupNbsBySize{$maxGroupSize}};
      if($#biggestGroups == 0) {
        $firstGroup=$biggestGroups[0];
      }else{
        my $userCpu=$self->getLatestAccountCpu($userId);
        my ($bestGroup,$bestGroupAvgDeviation);
        for my $i (0..$#biggestGroups) {
          my $groupDeviation=0;
          foreach my $id (@{$groupsByNb{$biggestGroups[$i]}}) {
            $groupDeviation+=abs($self->getLatestAccountCpu($id)-$userCpu);
          }
          my $groupAvgDeviation=$groupDeviation/$maxGroupSize;
          ($bestGroup,$bestGroupAvgDeviation)=($biggestGroups[$i],$groupAvgDeviation) unless(defined $bestGroupAvgDeviation && $bestGroupAvgDeviation < $groupAvgDeviation);
        }
        $firstGroup=$bestGroup;
      }
    }
  }

  my @orderedSmurfGroups=($groupsByNb{$firstGroup});
  my @orderedConflictingSmurfGroups=($conflictingGroupsByNb{$firstGroup});

  my %keptAccounts;
  @keptAccounts{@{$groupsByNb{$firstGroup}}}=((0) x ($#{$groupsByNb{$firstGroup}}+1));
  $keptAccounts{$userId}=0;

  my %orphanAccounts;
  @orphanAccounts{@userAccounts}=((1) x ($#userAccounts+1));
  delete @orphanAccounts{@accountsInvolvedInConflict};
  delete $orphanAccounts{$userId};
  
  $self->getTrueSmurfsByIP(\%keptAccounts,\%orphanAccounts);

  foreach my $groupNb (keys %conflictingGroupsByNb) {
    next if($groupNb == $firstGroup);
    my %groupAccounts;
    @groupAccounts{@{$groupsByNb{$groupNb}}}=((0) x ($#{$groupsByNb{$groupNb}}+1));
    my $p_addedAccounts=$self->getTrueSmurfsByIP(\%groupAccounts,\%orphanAccounts);
    if(@{$p_addedAccounts}) {
      my $detachedAccountsString=join(',',@{$p_addedAccounts});
      $self->log("Detaching orphan account(s) \"$detachedAccountsString\" as dependency during split of user $userId",3);
      push(@{$groupsByNb{$groupNb}},@{$p_addedAccounts});
    }
  }

  $self->getProbableSmurfsByIP(\%keptAccounts,\%orphanAccounts,$dynIpRange);

  foreach my $groupNb (keys %conflictingGroupsByNb) {
    next if($groupNb == $firstGroup);
    my %groupAccounts;
    @groupAccounts{@{$groupsByNb{$groupNb}}}=((0) x ($#{$groupsByNb{$groupNb}}+1));
    my $p_addedAccounts=$self->getProbableSmurfsByIP(\%groupAccounts,\%orphanAccounts,$dynIpRange);
    if(@{$p_addedAccounts}) {
      my $detachedAccountsString=join(',',@{$p_addedAccounts});
      $self->log("Detaching orphan account(s) \"$detachedAccountsString\" as probable dependency during split of user $userId",3);
      push(@{$groupsByNb{$groupNb}},@{$p_addedAccounts});
    }
    push(@orderedSmurfGroups,$groupsByNb{$groupNb});
    push(@orderedConflictingSmurfGroups,$conflictingGroupsByNb{$groupNb});
  }

  return (\@orderedConflictingSmurfGroups,\@orderedSmurfGroups);
}

#########################
# Admin event functions #
#########################

# Called by sldbLi.pl, slMonitor.pl
sub adminEvent {
  my ($self,$typeName,$subType,$orig,$origId,$p_params,$message)=@_;
  if(! exists $ADMIN_EVT_TYPE{$typeName}) {
    &{$self->{sqlErrorHandler}}("adminEvent called with an invalid event type $typeName");
    return -1;
  }
  my $type=$ADMIN_EVT_TYPE{$typeName};
  foreach my $requiredParam (@{$ADMIN_EVT_PARAMS{$type}}) {
    if(! exists $p_params->{$requiredParam}) {
      &{$self->{sqlErrorHandler}}("Missing parameter \"$requiredParam\" in adminEvent $typeName");
      return -2;
    }
  }
  if(! defined $message) {
    $message=$ADMIN_EVT_MSG{$type};
    foreach my $param (@{$ADMIN_EVT_PARAMS{$type}}) {
      $message=~s/\%$param\%/$p_params->{$param}/g;
    }
  }
  my $quotedMessage=$self->quote($message);
  $self->do("insert into adminEvents (date,type,subType,orig,origId,message) values (now(),$type,$subType,$orig,$origId,$quotedMessage)","insert new $typeName event into adminEvents table");
  my $sth=$self->prepExec("select LAST_INSERT_ID()");
  my @eventId=$sth->fetchrow_array();
  if(! @eventId) {
    &{$self->{sqlErrorHandler}}("Unable to retrieve eventId from adminEvents table");
    return -3;
  }
  foreach my $param (keys %{$p_params}) {
    my ($quotedParam,$quotedValue)=$self->quote($param,$p_params->{$param});
    $self->do("insert into adminEventsParams values ($eventId[0],$quotedParam,$quotedValue)","insert new $typeName event parameter \"$param\" ($p_params->{$param}) into adminEventsParams table (event id: $eventId[0])");
  }
  $self->log("New admin event ($eventId[0]): $message",4);
  return $eventId[0];
}

1;
