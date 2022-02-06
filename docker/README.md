Docker
======

One can use docker services to either provision or develop SLDB.

[Install `docker-compose`](https://docs.docker.com/compose/install/) and
`docker` for your platform and follow the instructions to ensure both the
docker daemon and docker-compose are operational.

Provisioning
------------

To provision docker services for deployment:

- Copy the `.env` and `docker-compose.yml` files to the directory you want
  SLDB to live on your server
- Edit the `.env` file to your requirements, see [Configuration](#configuration)
- Run `mkdir etc log`
- _(optional)_ Copy the users, levels and users configuration files and edit
  them to your requirements:
  `cd etc && curl -SLO "https://raw.githubusercontent.com/Yaribz/SLDB/master/etc/{commands,levels,users}.conf"`
  If this step is not performed, the default files from SLDB will be used
- _(optional)_ If your database server is not intended to be hosted with docker
  , remove any `depends_on` references to `sldb-db` on `docker-compose.yml` and
  edit the `SLDB_DB_DATABASE` environment variable accordingly.
- Run `docker-compose pull`

### _(optional)_ Initialize the database

If you are provisioning a new installation of SLDB:

- Create a file named `etc/initSldb.conf` with the following contents:
  ```
  dbLogin:<username>
  dbName:DBI:mysql:database=<dbname>;host=<dbhost>
  dbPwd:<password>
  ```
  If provisioning database with docker, replace the variables with the ones
  configured in `.env` as `MYSQL_*`
- Run `docker-compose run xmlRpc perl sldbSetup.pl --init-db` and follow any
  instructions required

### _(optional)_ Migrate the database

If you have a previous installation of SLDB you are migrating from:

- Create a dump with `mysqldump -u <username> -p <password> -h <hostname> --databases <dbname> | gzip -c > sldbdump_$(date '+%Y-%m-%d_%H-%M-%S').gz`
- Copy the generated dump to your new SLDB base directory
- Run: `docker-compose up -d sldb-db `
- Run: `gunzip -c <yourdumpfile> | docker-compose exec -T sldb-db /usr/bin/mysql -u root --password=<$MYSQL_ROOT_PASSWORD> <$MYSQL_DATABASE>`
- Find and execute any migration steps required between your old and new SLDB versions

### Starting the services

Read the [documentation](https://github.com/Yaribz/SLDB#documentation) to
have a basic understanding of how SLDB operates.

The services are orchestrated so that running only `docker-compose up -d sldbLi`
should provide you with an operational set of SLDB component services.

The service `xmlRpc` is optional and only required if you have an external
service that needs to communicate with SLDB.

### Configuration

Most of the variables are self explanatory, otherwise:

- `MYSQL_*`: the variables that will be used to initialize the database instance, when provisioned from docker.
- `UID|GID`: run `id` on your host machine to fetch and configure the values, this ensures permissions are available for the shared volumes: `log` and `etc`
- `<COMPONENT>_CONF`: set of key-values to dynamically configure the component upon initialization, these override defaults when configured
- `docker-compose.yml`: service configuration, avoid touching whenever possible
- `etc/{users,levels,commands}.conf`: general SLDB interface configuration
- `etc/xmlRpc.users.conf`: configuration for users that have access to xmlRpc interface

### Operation

Logs and configuration files are available at `etc` and `log` on the host
machine.

Check all services are running as expected with `docker-compose ps`. Output
should look like this:

```
       Name                      Command               State          Ports
-------------------------------------------------------------------------------------
sldb-db               /scripts/run.sh                  Up      3306/tcp
sldb_ratingEngine_1   /opt/sldb-entrypoint.sh pe ...   Up
sldb_slMonitor_1      /opt/sldb-entrypoint.sh pe ...   Up
sldb_sldbLi_1         /opt/sldb-entrypoint.sh pe ...   Up
sldb_xmlRpc_1         /opt/sldb-entrypoint.sh pe ...   Up      0.0.0.0:8300->8300/tcp
```

_(db provisioned with docker)_ Never stop the database service (sldb-db) unless
you don't need sldb running. All components depend on the database service and
should start it automatically in case it's not running when they are started.

- To start a component: `docker-compose up -d <component>`
- To stop a component: `docker-compose stop <component>`

Wait for the command to finish to ensure integrity of the database.

In case hot-reloading is necessary, it's possible to edit the config files at
`etc` in the host machine and sending `SIGUSR2` to the sldbLi process.
Alternatively, send `!reloadConf` to the SLDB bot user if your user has
been configured to have access to it. This only applies to sldbLi, all other
components require restart.

This is only encouraged for testing, quick fixes or uptime and won't be
persisted if the service is restarted. Use the `.env` or `docker-compose.yml`
file to persist configuration changes (requires restart).
