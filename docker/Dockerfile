FROM alpine

## alpine curl and wget aren't fully compatible, so we install them
## here. gnupg is needed for Module::Signature.
ENV PYTHONUNBUFFERED=1
RUN apk update \
 && apk upgrade \
 && apk add --no-cache bash cairo-dev curl tar make gcc build-base wget gnupg vim \
                       python3 python3-dev \
                       musl-obstack-dev expat-dev readline-dev \
                       mariadb-connector-c-dev mariadb-client zlib-dev perl-devel-checklib \
 && ln -sf python3 /usr/bin/python \
 && python3 -m ensurepip \
 && pip3 install --no-cache --upgrade pip setuptools six \
 && mkdir -p /usr/src/perl

WORKDIR /usr/src/perl

## some flags from http://git.alpinelinux.org/cgit/aports/tree/main/perl/APKBUILD?id=19b23f225d6e4f25330e13144c7bf6c01e624656
RUN curl -SLO https://www.cpan.org/src/5.0/perl-5.32.0.tar.gz \
    && echo 'ddecb3117c016418b19ed3a8827e4b521b47d6bb *perl-5.32.0.tar.gz' | sha1sum -c - \
    && tar --strip-components=1 -xzf perl-5.32.0.tar.gz -C /usr/src/perl \
    && rm perl-5.32.0.tar.gz \
    && ./Configure -des \
        -Duse64bitall \
        -Dcccdlflags='-fPIC' \
        -Dcccdlflags='-fPIC' \
        -Dccdlflags='-rdynamic' \
        -Dlocincpth=' ' \
        -Duselargefiles \
        -Dusethreads \
        -Duseshrplib \
        -Dd_semctl_semun \
        -Dusenm \
    && make libperl.so \
    && make -j$(nproc) \
    && make install \
    && curl -LO https://raw.githubusercontent.com/miyagawa/cpanminus/master/cpanm \
    && chmod +x cpanm \
    && ./cpanm --notest App::cpanminus \
    && rm -fr ./cpanm /root/.cpanm /usr/src/perl \
    && mkdir -p /sldb

## from tianon/perl
ENV PERL_CPANM_OPT --verbose --mirror https://cpan.metacpan.org --mirror-only
RUN cpanm --notest Digest::SHA Module::Signature && rm -rf ~/.cpanm
ENV PERL_CPANM_OPT $PERL_CPANM_OPT --verify

WORKDIR /sldb

RUN cpanm --quiet --notest Inline::Python \
 && cpanm --quiet --notest RPC::XML::Server \
 && cpanm --quiet --notest Net::Server::PreFork \
 && cpanm --quiet --notest DBI \
 && cpanm --quiet --notest DBD::mysql \
 && cpanm --quiet --notest Term::UI \
 && cpanm --quiet --notest Term::ReadLine::Gnu \
 && cpanm --quiet --notest Chart::Clicker

ENV TRUESKILL_VERSION=0.4.5
RUN curl -LO https://github.com/sublee/trueskill/archive/refs/tags/v$TRUESKILL_VERSION.tar.gz \
 && tar -xvf v$TRUESKILL_VERSION.tar.gz trueskill-$TRUESKILL_VERSION/trueskill \
 && mv trueskill-$TRUESKILL_VERSION/trueskill ./ \
 && rm -rf trueskill-$TRUESKILL_VERSION v$TRUESKILL_VERSION.tar.gz

ADD https://raw.githubusercontent.com/Yaribz/SimpleConf/master/SimpleConf.pm ./
ADD https://raw.githubusercontent.com/Yaribz/SimpleLog/master/SimpleLog.pm ./
ADD https://raw.githubusercontent.com/Yaribz/SpringLobbyInterface/master/SpringLobbyInterface.pm ./

COPY *.pm *.pl ./
COPY ./var ./var/
COPY ./docker/scripts/*.sh /opt/
COPY etc /etc/sldb/
COPY docker/etc /etc/sldb/

RUN chmod -R o+rw /sldb

ENTRYPOINT [ "/opt/sldb-entrypoint.sh" ]
