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
- Edit the `.env` file to your requirements
- _(optional)_ If your database server is not intended to be hosted with docker
  , remove any `depends_on` references to `sldb-db` on `docker-compose.yml` and
  edit the `SLDB_DB_DATABASE` environment variable accordingly.
- Run `docker-compose pull`

### _(optional)_ Migrate the database

If you have a previous installation of SLDB you want to migrate from:

- Create a dump with `mysqldump -u <username> -p <password> -h <hostname> --databases <dbname> | gzip -c > sldbdump_$(date '+%Y-%m-%d_%H-%M-%S').gz`
- Copy the generated dump to your new SLDB base directory
- Run: `docker-compose up -d sldb-db `
- Run: `gunzip -c <yourdumpfile> | docker-compose exec -T sldb-db /usr/bin/mysql -u root --password=<$MYSQL_ROOT_PASSWORD> <$MYSQL_DATABASE>`
- Find and execute any other migration steps required between your old and new SLDB versions

### Starting the services

Read the [documentation](https://github.com/Yaribz/SLDB#documentation) to
have a basic understanding of how SLDB operates.

The services are orchestrated so that running only `docker-compose up -d sldb`
should provide you with an operational set of SLDB component services.

The service `xmlrpc` is optional and only required if you have an external
service that needs to communicate with SLDB.
