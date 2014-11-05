SLDB
====
SLDB (Spring Lobby DataBase) is a data warehouse and real time ranking
application for all games based on [SpringRTS](http://springrts.com/) engine. It
is designed to offer all the required functionalities to manage the player base
in the context of FOSS (Free Open Source Software) games, where players can
create as many accounts as they want and games can be hosted by anyone. For
this purpose, following functionalities have notably been implemented:
* advanced automatic multi-accounts ("smurfs") detection
* automatic and manual (by admins) accounts join/split
* by-game and by-game-type TrueSkill ranking system to optimize players balance
  on all types of games
* retro-active account management regarding ranking (automatic re-rating of all
  required matches when new accounts are joined/split)
* protection against fake data sent by thrid parties
* configurable privacy levels to limit account information disclosure

SLDB is also designed to allow fast access to historical data concerning all
games and players using main Spring lobby since July 2012 (matches results,
ranking data, statistics...).

Components
----------
* [ratingEngine.pl](ratingEngine.pl): Rating engine
* [Sldb.pm](Sldb.pm): Data model layer module
* [sldbBackup.pl](sldbBackup.pl): Backup script
* [sldbLi.pl](sldbLi.pl): Lobby interface bot
* [SldbLiConf.pm](SldbLiConf.pm): Configuration management module for the lobby
  interface bot
* [sldbSetup.pl](sldbSetup.pl): Setup script
* [slMonitor.pl](slMonitor.pl): Lobby monitoring application
* [xmlRpc.pl](xmlRpc.pl): XmlRpc interface
* [zkMonitor.pl](zkMonitor.pl): Zero-K monitoring application
* [Lobby interface configuration templates](etc): Templates for the
  configuration of the lobby interface bot (other SLDB configuration files are
  generated in the same directory by the setup script)
* [var/help.dat](var/help.dat): Data file for the commands help of the lobby
  interface bot.

Please see the file called [COMPONENTS](COMPONENTS) for a more detailed
description of each component.

The SLDB lobby inteface bot is based on the templates provided by following project:
* [SpringLobbyBot](https://github.com/Yaribz/SpringLobbyBot)

Dependencies
------------
The SLDB application is based on a partitionned database which requires MySQL
5.5 or later.

The SLDB application depends on following projects:
* [SimpleConf](https://github.com/Yaribz/SimpleConf)
* [SimpleLog](https://github.com/Yaribz/SimpleLog)
* [SpringLobbyInterface](https://github.com/Yaribz/SpringLobbyInterface)
* [TrueSkill python module](https://github.com/sublee/trueskill)

The SLDB backup script requires following dependencies, which are only needed if
you plan to use this script for your backups:
* GNU [tar](http://www.gnu.org/software/tar/) and
  [gzip](http://www.gnu.org/software/gzip/)
* [LFTP](http://lftp.yar.ru/)

Additionally, some SLDB components require some standard but non-core Perl
modules to be available on the system (easily installable through CPAN):
* ratingEngine requires the "Inline::Python" Perl module
* xmlRpc requires the "RPC::XML::Server" Perl module ("Net::Server::PreFork" is
  also highly recommended)
* zkMonitor requires the "HTML::TreeBuilder" and "WWW::Mechanize" Perl modules

SLDB also depends on following project (hosted remotely) for additional
functionalities:
* [spring replay site](https://github.com/dansan/spring-replay-site) for HTTP
  interface to some SLDB data.

Installation
------------
* Ensure MySQL 5.5 or later is installed on the system
* Copy following dependencies into SLDB directory:
  [SimpleConf.pm](https://raw.github.com/Yaribz/SimpleConf/master/SimpleConf.pm),
  [SimpleLog.pm](https://raw.github.com/Yaribz/SimpleLog/master/SimpleLog.pm) and
  [SpringLobbyInterface.pm](https://raw.github.com/Yaribz/SpringLobbyInterface/master/SpringLobbyInterface.pm)
* Install the [TrueSkill python module](https://github.com/sublee/trueskill) as
  "trueskill" subdirectory of SLDB (version "0.2.1" is known to be compatible
  with SLDB)
* Use your favorite Perl package manager to install following standard Perl
  modules (available on CPAN) and their dependencies: Inline::Python,
  RPC::XML::Server, Net::Server::PreFork, HTML::TreeBuilder and WWW::Mechanize
* run the sldbSetup.pl script from SLDB directory and execute all steps, as
  selectionned by default by the script
* Edit the etc/sldbLi.conf file to set following parameters:
  * lobbyPassword (password of the lobby account used by sldbLi)
  * sldb (replace  &lt;dbLogin&gt;, &lt;dbPwd&gt; and &lt;dbName&gt; by the
    corresponding values for SLDB)
  * etcDir (directory containing sldbLi config files, should be the
    "etc" subdirectory of SLDB)
  * varDir (directory containing sldbLi dynamic data, should be the "var"
    subdirectory of SLDB)
  * logDir (directory containing sldbLi log files, should be the "var/log"
    subdirectory of SLDB)
* Edit the etc/users.conf file and update the <> placeholders with the desired
  privileged lobby user names and account IDs for SLDB
* You are now ready to launch all SLDB components, using recommended order:

        ./slMonitor.pl
        ./zkMonitor.pl
        ./ratingEngine.pl
        ./sldbLi.pl
        ./xmlRpc.pl

Backups
-------
SLDB includes a basic backup script: [sldbBackup.pl](sldbBackup.pl)

The "Configuration" section of this script must be edited before first use, to
match host environment (in particular, a LFTP bookmark must be created first to
store the remote FTP server connection information).

This script requires an account with sufficient privileges to run mysqlhotcopy.
It can be launched manually once with verbose option to check that all works as
expected:

        ./sldbBackup.pl --verbose

Then it is recommended to schedule this script to be executed weekly in off peak
periods, using crontab for example.

Documentation
-------------
Please see the file called [COMPONENTS](COMPONENTS) for a description of each
SLDB component.

Please see the file called [TABLES](TABLES) for a basic description of SLDB
data model.

Please see the file called [XMLRPC](XMLRPC) for a description of the XML-RPC
interface.

Licensing
---------
Please see the file called [LICENSE](LICENSE).

Author
------
Yann Riou <yaribzh@gmail.com>
