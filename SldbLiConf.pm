# sldbLi configuration module
# This file is part of SLDB.
#
# Copyright (C) 2013-2020  Yann Riou <yaribzh@gmail.com>
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

package SldbLiConf;

use strict;

use Storable 'dclone';

use SimpleLog;

# Internal data ###############################################################

my $moduleVersion='0.3';

my %globalParameters = (lobbyLogin => ["login"],
                        lobbyPassword => ["password"],
                        lobbyHost => ["hostname"],
                        lobbyPort => ["port"],
                        lobbyReconnectDelay => ["integer"],
                        localLanIp => ["ipAddr","star","null"],
                        lobbyFollowRedirect => ["bool"],
                        sldb => ['db'],
                        adminListenAddr => ['ipAddr','null'],
                        adminListenPort => ['port','null'],
                        adminAnsiColors => ['bool'],
                        etcDir => ["readableDir"],
                        varDir => ["writableDir"],
                        logDir => ["writableDir"],
                        sendRecordPeriod => ["integer"],
                        maxBytesSent => ["integer"],
                        maxLowPrioBytesSent => ["integer"],
                        maxChatMessageLength => ["integer"],
                        cmdFloodAutoIgnore => ["integerTriplet"],
                        floodImmuneLevel => ["integer"],
                        masterChannel => ["channel","null"],
                        broadcastChannels => ["channelList","null"],
                        logLevel => ["integer"],
                        sldbLogLevel => ['integer'],
                        lobbyInterfaceLogLevel => ["integer"],
                        logChanChat => ["bool"],
                        logChanJoinLeave => ["bool"],
                        logPvChat => ["bool"]);

my %paramTypes = (login => '[\w\[\]]{2,20}',
                  password => '[^\s]+',
                  hostname => '\w[\w\-\.]*',
                  port => sub { return ($_[0] =~ /^\d+$/ && $_[0] < 65536) },
                  integer => '\d+',
                  nonNullInteger => '[1-9]\d*',
                  ipAddr => '\d+\.\d+\.\d+\.\d+',
                  star => '\*',
                  null => "",
                  executableFile => sub { return (-f $_[0] && -x $_[0]) },
                  readableDir => sub { return (-d $_[0] && -x $_[0] && -r $_[0]) },
                  writableDir => sub { return (-d $_[0] && -x $_[0] && -r $_[0] && -w $_[0]) },
                  integerCouple => '\d+;\d+',
                  integerTriplet => '\d+;\d+;\d+',
                  bool => '[01]',
                  bool2 => '[012]',
                  channel => '[\w\[\]\ ]+',
                  channelList => '([\w\[\]\ ]+(;[\w\[\]\ ]+)*)?',
                  notNull => '.+',
                  readableFile => sub { return (-f $_[0] && -r $_[0]) },
                  integerRange => '\d+\-\d+',
                  nonNullIntegerRange => '[1-9]\d*\-\d+',
                  float => '\d+(\.\d*)?',
                  db => '[^\/]+\/[^\@]+\@(?i:dbi)\:\w+\:\w.*');

my @usersFields=(["accountId","name","country","rank","access","bot","auth"],["level"]);
my @levelsFields=(["level"],["description"]);
my @commandsFields=(["source","status","gameState"],["directLevel","voteLevel"]);

# Constructor #################################################################

sub new {
  my ($objectOrClass,$confFile,$sLog) = @_;
  my $class = ref($objectOrClass) || $objectOrClass;

  my $p_conf = loadSettingsFile($sLog,$confFile,\%globalParameters);
  if(! checkSldbLiConfig($sLog,$p_conf)) {
    $sLog->log("Unable to load main configuration parameters",1);
    return 0;
  }

  $sLog=SimpleLog->new(logFiles => [$p_conf->{""}->{logDir}."/sldbLi.log",''],
                       logLevels => [$p_conf->{""}->{logLevel},3],
                       useANSICodes => [0,1],
                       useTimestamps => [1,1],
                       prefix => "[SldbLi] ");

  my $p_users=loadTableFile($sLog,$p_conf->{""}->{etcDir}."/users.conf",\@usersFields);
  my $p_levels=loadTableFile($sLog,$p_conf->{""}->{etcDir}."/levels.conf",\@levelsFields);
  my $p_commands=loadTableFile($sLog,$p_conf->{""}->{etcDir}."/commands.conf",\@commandsFields,1);
  my $p_help=loadSimpleTableFile($sLog,$p_conf->{""}->{varDir}."/help.dat",1);
  
  if(! checkNonEmptyHash($p_users,$p_levels,$p_commands,$p_help)) {
    $sLog->log("Unable to load commands, help and permission system",1);
    return 0;
  }

  my $self = {
    commands => $p_commands,
    levels => $p_levels,
    users => $p_users->{""},
    help => $p_help,
    log => $sLog,
    conf => $p_conf->{""}
  };

  bless ($self, $class);

  return $self;
}


# Accessor ####################################################################

sub getVersion {
  return $moduleVersion;
}

# Internal functions ##########################################################

sub checkNonEmptyHash {
  foreach my $p_hash (@_) {
    return 0 unless(%{$p_hash});
  }
  return 1;
}

sub ipToInt {
  my $ip=shift;
  my $int=0;
  $int=$1*(256**3)+$2*(256**2)+$3*256+$4 if ($ip =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  return $int;
}

sub findMatchingData {
  my ($p_data,$p_filters)=@_;
  my %data=%{$p_data};
  my @filters=@{$p_filters};
  my @matchingData;
  for my $i (0..$#filters) {
    my @filterData=@{$filters[$i]};
    my %filter=%{$filterData[0]};
    my $matched=1;
    foreach my $field (keys %data) {
      next if($data{$field} eq "");
      next unless(exists $filter{$field} && defined $filter{$field} && $filter{$field} ne "");
      my @filterFieldValues=split(",",$filter{$field});
      my $matchedField=0;
      my $fieldData=$data{$field};
      $fieldData=$1 if($field eq "accountId" && $fieldData =~ /^([^\(]+)\(/);
      foreach my $filterFieldValue (@filterFieldValues) {
        if($field eq "accountId" && $filterFieldValue =~ /^([^\(]+)(\(.*)$/) {
          my ($filterAccountId,$filterUserName)=($1,$2);
          if($fieldData =~ /^\(/) {
            $filterFieldValue=$filterUserName;
          }else{
            $filterFieldValue=$filterAccountId;
          }
        }
        if($fieldData =~ /^\d+$/ && $filterFieldValue =~ /^(\d+)\-(\d+)$/) {
          if($1 <= $fieldData && $fieldData <= $2) {
            $matchedField=1;
            last;
          }
        }elsif($fieldData =~ /^\d+\.\d+\.\d+\.\d+$/ && $filterFieldValue =~ /^(\d+\.\d+\.\d+\.\d+)\-(\d+\.\d+\.\d+\.\d+)$/) {
          my ($startIp,$endIp)=(ipToInt($1),ipToInt($2));
          my $ip=ipToInt($fieldData);
          if($startIp <= $ip && $ip <= $endIp) {
            $matchedField=1;
            last;
          }
        }elsif($filterFieldValue =~ /^~(.*)$/ && $fieldData =~ /^$1$/) {
          $matchedField=1;
          last;
        }elsif($fieldData eq $filterFieldValue) {
          $matchedField=1;
          last;
        }
      }
      $matched=$matchedField;
      last unless($matched);
    }
    push(@matchingData,$filters[$i]->[1]) if($matched);
  }
  return \@matchingData;
}

# Internal functions - Configuration ##########################################

sub loadSettingsFile {
  my ($sLog,$cFile,$p_globalParams,$p_sectionParams)=@_;

  my %newConf=("" => {});
  if(! open(CONF,"<$cFile")) {
    $sLog->log("Unable to read configuration file ($cFile)",1);
    return {};
  }

  my @invalidGlobalParams;
  while(<CONF>) {
    next if(/^\s*(\#.*)?$/);
    if(/^([^:]+):(.*)$/) {
      my ($param,$value)=($1,$2);
      if(! exists $p_globalParams->{$param}) {
        $sLog->log("Ignoring invalid global parameter ($param)",2);
        next;
      }
      push(@invalidGlobalParams,$param) unless(checkValue($value,$p_globalParams->{$param}));
      if(exists $newConf{""}->{$param}) {
        $sLog->log("Duplicate parameter definitions in configuration file \"$cFile\" (parameter \"$param\")",2);
      }
      $newConf{""}->{$param}=$value;
      next;
    }else{
      chomp($_);
      $sLog->log("Ignoring invalid configuration line in file \"$cFile\" ($_)",2);
      next;
    }
  }

  close(CONF);

  if(@invalidGlobalParams) {
    $sLog->log("Configuration file \"$cFile\" contains inconsistent values for following global parameter(s): ".join(",",@invalidGlobalParams),1);
    return {};
  }

  return \%newConf;
}

sub checkValue {
  my ($value,$p_types)=@_;
  return 1 unless(@{$p_types});
  foreach my $type (@{$p_types}) {
    my $checkFunction=$paramTypes{$type};
    if(ref($checkFunction)) {
      return 1 if(&{$checkFunction}($value));
    }else{
      return 1 if($value =~ /^$checkFunction$/);
    }
  }
  return 0;
}

sub loadTableFile {
  my ($sLog,$cFile,$p_fieldsArrays,$caseInsensitive)=@_;
  $caseInsensitive=0 unless(defined $caseInsensitive);

  if(! open(CONF,"<$cFile")) {
    $sLog->log("Unable to read file ($cFile)",1);
    return {};
  }

  my @pattern;
  my $section="";
  my %newConf=("" => []);

  while(<CONF>) {
    my $line=$_;
    chomp($line);
    if(/^\s*\#\?\s*([^\s]+)\s*$/) {
      my $patternString=$1;
      my @subPatternStrings=split(/\|/,$patternString);
      @pattern=();
      for my $i (0..$#subPatternStrings) {
        my @splitSubPattern=split(/\:/,$subPatternStrings[$i]);
        $pattern[$i]=\@splitSubPattern;
      }
      if($#pattern != $#{$p_fieldsArrays}) {
        $sLog->log("Invalid pattern \"$line\" in configuration file \"$cFile\" (number of fields invalid)",1);
        close(CONF);
        return {};
      }
      for my $index (0..$#pattern) {
        my @fields=@{$pattern[$index]};
        foreach my $field (@fields) {
          if(! grep(/^$field$/,@{$p_fieldsArrays->[$index]})) {
            $sLog->log("Invalid pattern \"$line\" in configuration file \"$cFile\" (invalid field: \"$field\")",1);
            close(CONF);
            return {};
          }
        }
      }
      next;
    }
    next if(/^\s*(\#.*)?$/);
    if(/^\s*\[([^\]]+)\]\s*$/) {
      $section=$1;
      $section=lc($section) if($caseInsensitive);
      if(exists $newConf{$section}) {
        $sLog->log("Duplicate section definitions in configuration file \"$cFile\" ($section)",2);
      }else{
        $newConf{$section}=[];
      }
      next;
    }
    my $p_data=parseTableLine($sLog,\@pattern,$line);
    if(@{$p_data}) {
      push(@{$newConf{$section}},$p_data);
    }else{
      $sLog->log("Invalid configuration line in file \"$cFile\" ($line)",1);
      close(CONF);
      return {};
    }
  }
  close(CONF);

  return \%newConf;

}

sub parseTableLine {
  my ($sLog,$p_pattern,$line,$iter)=@_;
  $iter=0 unless(defined $iter);
  my $p_subPattern=$p_pattern->[$iter];
  my $subPatSize=$#{$p_subPattern};
  my %hashData;
  for my $index (0..($subPatSize-1)) {
    if($line =~ /^([^:]*):(.*)$/) {
      $hashData{$p_subPattern->[$index]}=$1;
      $line=$2;
    }else{
      $sLog->log("Unable to parse fields in following configuration data \"$line\"",1);
      return [];
    }
  }
  if($line =~ /^([^\|]*)\|(.*)$/) {
    $hashData{$p_subPattern->[$subPatSize]}=$1;
    $line=$2;
  }else{
    $hashData{$p_subPattern->[$subPatSize]}=$line;
    $line="";
  }
  my @data=(\%hashData);
  if($iter < $#{$p_pattern}) {
    my $p_data=parseTableLine($sLog,$p_pattern,$line,++$iter);
    return [] unless(@{$p_data});
    push(@data,@{$p_data});
  }
  return \@data;
}

sub loadSimpleTableFile {
  my ($sLog,$cFile,$caseInsensitive)=@_;
  $caseInsensitive=0 unless(defined $caseInsensitive);

  if(! open(CONF,"<$cFile")) {
    $sLog->log("Unable to read file ($cFile)",1);
    return {};
  }

  my $section="";
  my %newConf=("" => []);

  while(<CONF>) {
    my $line=$_;
    next if(/^\s*(\#.*)?$/);
    if(/^\s*\[([^\]]+)\]\s*$/) {
      $section=$1;
      $section=lc($section) if($caseInsensitive);
      $newConf{$section}=[] unless(exists $newConf{$section});
      next;
    }
    chomp($line);
    if($section) {
      push(@{$newConf{$section}},$line);
    }else{
      $sLog->log("Invalid configuration file \"$cFile\" (missing section declaration)",1);
      close(CONF);
      return {};
    }
  }
  close(CONF);

  return \%newConf;
}

sub checkSldbLiConfig {
  my ($sLog,$p_conf)=@_;

  return 0 unless(%{$p_conf});

  my @missingParams;
  foreach my $requiredGlobalParam (keys %globalParameters) {
    if(! exists $p_conf->{""}->{$requiredGlobalParam}) {
      push(@missingParams,$requiredGlobalParam);
    }
  }
  if(@missingParams) {
    my $mParams=join(",",@missingParams);
    $sLog->log("Incomplete SldbLi configuration (missing global parameters: $mParams)",1);
    return 0;
  }

  return 1;
}

# Business functions ##########################################################

sub getFullCommandsHelp {
  my $self=shift;
  my $p_fullHelp=loadSimpleTableFile($self->{log},$self->{conf}->{varDir}."/help.dat");
  return $p_fullHelp;
}

sub getUserAccessLevel {
  my ($self,$name,$p_user,$authenticated)=@_;
  my $p_userData={name => $name,
                  accountId => $p_user->{accountId},
                  country => $p_user->{country},
                  rank => $p_user->{status}->{rank},
                  access => $p_user->{status}->{access},
                  bot => $p_user->{status}->{bot},
                  auth => $authenticated};
  my $p_levels=findMatchingData($p_userData,$self->{users});
  if(@{$p_levels}) {
    return $p_levels->[0]->{level};
  }else{
    return 0;
  }
}

sub getLevelDescription {
  my ($self,$level)=@_;
  my $p_descriptions=findMatchingData({level => $level},$self->{levels}->{""});
  if(@{$p_descriptions}) {
    return $p_descriptions->[0]->{description};
  }else{
    return "Unknown level";
  }
}

sub getCommandLevels {
  my ($self,$command,$source,$status,$gameState)=@_;
  if(exists $self->{commands}->{$command}) {
    my $p_rights=findMatchingData({source => $source, status => $status, gameState => $gameState},$self->{commands}->{$command});
    return dclone($p_rights->[0]) if(@{$p_rights});
  }
  return {};
}

sub getHelpForLevel {
  my ($self,$level)=@_;
  my @direct=();
  my @vote=();
  foreach my $command (sort keys %{$self->{commands}}) {
    if(! exists $self->{help}->{$command}) {
      $self->{log}->log("Missing help for command \"$command\"",2) unless($command =~ /^#/);
      next;
    }
    my $p_filters=$self->{commands}->{$command};
    my $foundDirect=0;
    my $foundVote=0;
    foreach my $p_filter (@{$p_filters}) {
      if(exists $p_filter->[1]->{directLevel}
         && defined $p_filter->[1]->{directLevel}
         && $p_filter->[1]->{directLevel} ne ""
         && $level >= $p_filter->[1]->{directLevel}) {
        $foundDirect=1;
      }
      if(exists $p_filter->[1]->{voteLevel}
         && defined $p_filter->[1]->{voteLevel}
         && $p_filter->[1]->{voteLevel} ne ""
         && $level >= $p_filter->[1]->{voteLevel}) {
        $foundVote=1;
      }
      last if($foundDirect);
    }
    if($foundDirect) {
      push(@direct,$self->{help}->{$command}->[0]);
    }elsif($foundVote) {
      push(@vote,$self->{help}->{$command}->[0]);
    }
  }
  return {direct => \@direct, vote => \@vote};
}

1;
