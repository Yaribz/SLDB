#!/bin/bash

DELIMITER_CONF_FILES='#'
DELIMITER_CONF_SETTINGS='%'

logfile_var_name="$(echo $COMPONENT_NAME | tr '[:lower:]' '[:upper:]')_LOGFILE"
logfile_var=${!logfile_var_name}

conf_var_name="$(echo $COMPONENT_NAME | tr '[:lower:]' '[:upper:]')_CONF"
conf_var=${!conf_var_name}

IFS=$DELIMITER_CONF_FILES; conf_lines=($conf_var); unset IFS;

for conf_line in "${conf_lines[@]}"
do
  IFS=$DELIMITER_CONF_SETTINGS; conf_entries=($conf_line); unset IFS;
  conf_file=${conf_entries[0]}
  conf_settings=${conf_entries[1]}

  [[ -f /etc/sldb/$conf_file.conf ]] && cp /etc/sldb/$conf_file.conf etc/$conf_file.conf || touch etc/$conf_file.conf
  [[ -n $conf_settings ]] && perl updateConfFile.pl etc/$conf_file.conf $conf_settings
  [[ -f /etc/sldb/$conf_file.docker.conf ]] && perl updateConfFile.pl etc/$conf_file.conf --from-file /etc/sldb/$conf_file.docker.conf
done

[[ -f /etc/sldb/sldb.conf ]] && cp /etc/sldb/sldb.conf etc/sldb.conf || touch etc/sldb.conf
[[ -n $SLDB_CONF ]] && perl updateConfFile.pl etc/sldb.conf $SLDB_CONF

[[ ! -f /etc/sldb/levels.conf ]] && cp /etc/sldb/levels.conf etc
[[ ! -f /etc/sldb/users.conf ]] && cp /etc/sldb/users.conf etc
[[ ! -f /etc/sldb/commands.conf ]] && cp /etc/sldb/commands.conf etc

echo "Running $@ at $(printf '%s %s\n' "$(date)")" >> $logfile_var

exec $@ 2>&1
