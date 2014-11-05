#!/usr/bin/perl -w
#
# This file implements automatic hot backup for SLDB database, it is part of
# SLDB.
#
# sldbBackup performs a hot backup of the SLDB database, stores the archive
# locally and sends it to a remote server using FTP. sldbBackup is supposed to
# be executed automatically through crontab for example, but it requires an
# account with sufficient privileges to run mysqlhotcopy.
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

# Version 0.2 (2014/11/05)

use strict;

use File::Path;
use Time::Piece;


################################################################################
#                              Configuration                                   #
################################################################################

# absolute directory where backup archives will be stored locally
my $backupDir='/home/sldb/var';

# path of mysqlhotcopy utility
my $mysqlhotcopyBin='/usr/bin/mysqlhotcopy';

# LFTP bookmark to use when sending backup archives through FTP
my $lftpBookmark='sldbbackup';

# remote directory where backup archives will be stored
my $ftpDir='backup';

# bandwidth usage limit when uploading files through FTP (in bytes)
my $ftpSpeedLimit=512000;

################################################################################


my $verbose=0;
$verbose=1 if(@ARGV);

print "Creating temporary backup directory.\n" if($verbose);
my $tmpDir="$backupDir/backup_".(localtime->strftime('%Y%m%d_%H%M%S'));
mkpath($tmpDir);

print "Performing backup.\n" if($verbose);
system("$mysqlhotcopyBin -q --noindices sldb $tmpDir");

print "Archiving backup.\n" if($verbose);
system("tar c -C $tmpDir -f $tmpDir.tar sldb");

print "Removing temporary files.\n" if($verbose);
rmtree("$tmpDir");

print "Compressing backup.\n" if($verbose);
system("gzip $tmpDir.tar");

print "Sending backup by FTP.\n" if($verbose);
my $redir='';
$redir=' >/dev/null 2>&1' unless($verbose);
system("lftp $lftpBookmark -e \"set net:limit-total-rate $ftpSpeedLimit;cd $ftpDir; put $tmpDir.tar.gz;quit\"$redir");

print "Done.\n" if($verbose);
