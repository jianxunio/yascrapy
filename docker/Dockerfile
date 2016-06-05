FROM base/archlinux
MAINTAINER cubarco "me@cubarco.org"

ADD ./bin/rabbitmq-start /usr/local/bin/
ADD ./bin/startall /usr/local/bin/
ADD ./bloomd.conf /etc/

RUN \
    echo \
    'Server = http://mirrors.ustc.edu.cn/archlinux/$repo/os/$arch' > /etc/pacman.d/mirrorlist && \
    pacman -Sy && \
    pacman --noconfirm -S \
        archlinux-keyring && \
    pacman --noconfirm -S \
        autoconf git make scons libunistring gcc pcre \
        rabbitmq \
        redis \
        mongodb

# dirty hack for conflicts
RUN \
    ln -s /usr/lib/libncursesw.so.5 /usr/lib/libncursesw.so.6

# install bloomd
RUN \
    cd /tmp && \
    git clone --depth 1 https://armon@github.com/armon/bloomd.git && \
    cd bloomd && \
    scons && \
    cp bloomd /usr/bin

# redis config
RUN \
    sed \
        -e 's/daemonize no/daemonize yes/' \
        -e 's/^bind.*/bind 0.0.0.0/' \
        -i /etc/redis.conf

# mongodb config
RUN \
    sed \
        -e 's/bind_ip = 127.0.0.1/bind_ip = 0.0.0.0/' \
        -i /etc/mongodb.conf

# install ssdb
RUN \
    cd /tmp && \
    git clone --depth 1 https://github.com/ideawu/ssdb.git && \
    cd ssdb && \
    make && \
    make install

# ssdb config (from https://github.com/ideawu/ssdb/blob/master/Dockerfile)
RUN \
    mkdir -p /var/lib/ssdb && \
    sed \
        -e 's@home.*@home /var/lib@' \
        -e 's/loglevel.*/loglevel info/' \
        -e 's@work_dir = .*@work_dir = /var/lib/ssdb@' \
        -e 's@pidfile = .*@pidfile = /run/ssdb.pid@' \
        -e 's@level:.*@level: info@' \
        -e 's@ip:.*@ip: 0.0.0.0@' \
        -i /usr/local/ssdb/ssdb.conf

# clean up
RUN \
    pacman -Rcs --noconfirm autoconf git make scons gcc
 
ENV TZ Asia/Shanghai
WORKDIR /tmp
# rabbitmq
EXPOSE 5672
EXPOSE 15672
# redis
EXPOSE 6379
# bloomd
EXPOSE 8673
# ssdb
EXPOSE 8888
# mongodb
EXPOSE 27017

ENTRYPOINT /usr/local/bin/startall
