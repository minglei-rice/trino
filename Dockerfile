## 基于bilib docker hub中已有的基础镜像构建，它是由docker/Dockerfile.kerberos
## 构建得到的镜像，这里引用了默认的镜像版本oplap-kerberos:java11
FROM hub.bilibili.co/compile/olap-kerberos:java11

#自定义环境变量应用名
ARG app_name=trino-adhoc

ENV TRINO_BASE_DIR=/data/app/${app_name} TRINO_LOG_DIR=/data/log/${app_name} TRINO_DATA_DIR=/data/data/${app_name} \
    TRINO_VERSION=trino-server-367-bili-0.2-SNAPSHOT APP_CONF_DIR=/data/conf

# Make `/data/log && /data/data` directories readable and writable, because Caster needs them.
# Create new directory `/data/script`, because Caster should use it to bind POD to SLB.
# Caster needs `/data/log/stdout` to store log files when using entrypoint.
RUN useradd -m -s /bin/bash -G root trino \
    && chmod u+w /etc/sudoers \
    && echo 'trino	ALL=(ALL:ALL) ALL' >> /etc/sudoers \
    && echo 'trino	ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && chmod u-w /etc/sudoers \
    && mkdir -m 777 -p /data/script \
    && mkdir -m 777 -p /data/log/stdout \
    && mkdir -m 777 -p $TRINO_BASE_DIR \
    && mkdir -m 777 -p $TRINO_LOG_DIR \
    && chmod -R 777 /data/log \
    && mkdir -m 777 -p $TRINO_DATA_DIR \
    && chmod -R 777 /data/data \
    && mkdir -m 777 -p $APP_CONF_DIR \
    && apt-get update \
    && apt-get install -y python3 net-tools less \
    && ln -s /usr/bin/python3 /usr/bin/python

ADD ./release $TRINO_BASE_DIR/

USER trino
WORKDIR $TRINO_BASE_DIR

RUN cd $TRINO_BASE_DIR \
  && ls -la \
  && tar -zxf docker.tar.gz \
  && rm docker.tar.gz \
  && cd docker \
  && ./install.sh \
  && whoami && ls -la
