#!/usr/bin/env perl
#
# This file implements a basic client which connects to the sldbLi admin
# interface, it is part of SLDB.
#
# Copyright (C) 2020  Yann Riou <yaribzh@gmail.com>
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

# Version 0.1 (2020/04/30)

use strict;
use warnings;

use IO::Select;
use IO::Socket::INET;
use List::Util qw'all any none';
use POSIX ();
use Term::ReadLine;

sub invalidUsage {
  print "Invalid usage.\nUsage:\n  $0 [<ipAddr>:]<port>\n";
  exit;
}

my ($ipAddr,$port)=('127.0.0.1');
invalidUsage() unless($#ARGV == 0);
if($ARGV[0] =~ /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):(\d+)$/) {
  invalidUsage() unless(all {$_ < 256} ($1,$2,$3,$4));
  $ipAddr=join('.',map {$_+0} ($1,$2,$3,$4));
  $port=$5;
}elsif($ARGV[0] =~ /^\d+$/ && $ARGV[0] < 65536) {
  $port=$ARGV[0]+0;
}else{
  invalidUsage();
}

my $term = Term::ReadLine->new('SLDB Admin interface',\*STDIN,\*STDOUT);
$term->ornaments(0);
my $r_termAttribs=$term->Attribs();
$r_termAttribs->{completion_function} = sub {
  my ($text, $line, $start) = @_;
  if (substr($line, 0, $start) =~ /^\s*$/) {
    my @possibleMatches = grep {index(lc($_),lc($text)) == 0} qw'!adminEvents !banList !checkIps !checkProbSmurfs !checkUserIps !help !helpAll !ipWhois !joinAcc !leaderboard !notSmurf !quit !ranking !reloadConf !restart !searchUser !sendLobby !set !setBanList !setName !skillGraph !splitAcc !topSkill !uWhois !version !whois';
    return @possibleMatches;
  }else{
    return ();
  }
};

if($term->ReadLine() eq 'Term::ReadLine::Stub') {
  print "No suitable Term::ReadLine module found!\n";
  print "You can either:\n";
  print "- install a Perl ReadLine module such as Term::ReadLine::Gnu\n";
  print "- use netcat or telnet instead to connect directly to the SLDB admin interface\n";
  exit 1;
}

print "Connecting to $ipAddr:$port...\n";
my $sldbSock = IO::Socket::INET->new(PeerHost => $ipAddr,
                                     PeerPort => $port,
                                     Proto => 'tcp',
                                     Blocking => 1)
    or die "Unable to connect to SLDB admin interface ($@)\n";
print "Connected to SLDB admin interface (use Ctrl-d to quit, !help for list of commands)\n";
print "-------------------------------------------------------------------------------\n";
my ($sentTs,$receivedTs)=(time,time);

my $running=1;
$term->event_loop(
  sub {
    my @pendingSockets;
    while($running && (none {$_ == $term->IN()} @pendingSockets)) {
      @pendingSockets=IO::Select->new($term->IN(),$sldbSock)->can_read(1);
      if(any {$_ == $sldbSock} @pendingSockets) {
        my $readData;
        my $readLength=$sldbSock->sysread($readData,POSIX::BUFSIZ);
        if(! defined $readLength) {
          print "-------------------------------------------------------------------------------\n";
          print "ERROR   - Unable to read data from SLDB admin interface: $!\n";
          print "Press enter to exit...";
          $running=0;
        }elsif(! $readLength) {
          print "-------------------------------------------------------------------------------\n";
          print "Lost connection to SLDB admin interface.\n";
          print "Press enter to exit...";
          $running=0;
        }else{
          $receivedTs=time;
          print $readData unless($readData =~ /^!#pong$/);
        }
      }elsif(time - $receivedTs > 40) {
        print "-------------------------------------------------------------------------------\n";
        print "Timeout on SLDB admin interface.\n";
        print "Press enter to exit...";
        $running=0;
      }
      if($running && (time - $sentTs > 15)) {
        print $sldbSock "!#ping\n";
        $sentTs=time;
      }
    }
    if(! $running) {
      shutdown($sldbSock,2);
      $sldbSock->close();
    }
  } );

my $cmd;
while (defined ($cmd = $term->readline(''))) {
  last unless($running);
  next if($cmd =~ /^\s*$/);
  if($cmd =~ /^\/(.*)$/) {
    if(any {lc($1) eq $_} qw'quit exit') {
      last;
    }else{
      print "> Invalid command\n";
    }
  }else{
    print $sldbSock $cmd."\n";
    $sentTs=time;
  }
}
if($running) {
  shutdown($sldbSock,2);
  $sldbSock->close();
  print "-------------------------------------------------------------------------------\n";
  print "Exiting.\n";
}
