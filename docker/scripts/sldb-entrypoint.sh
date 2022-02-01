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

  touch /etc/sldb/$conf_file.conf
  mv /etc/sldb/$conf_file.conf etc/$conf_file.conf
  perl updateConfFile.pl etc/$conf_file.conf $conf_settings
done

echo "Running $@ at $(printf '%s %s\n' "$(date)")" >> $logfile_var

exec $@ 2>&1 | tee -a /sldb/log/$LOGFILE.log
