# Usage
We can build images manually through `docker build` command, pushing a new `tag` or a new branch with `docker` postfix to
trigger CI workflow.

If a image is built by CI, finally the new image `$RIDER_PROJECT_NAME:tag_name` will be pushed into Caster image hub,
for example `datacenter.olap.trino-adhoc:docker-v1`, of which `datacenter.olap.trino-adhoc` is corresponding the
`RIDER_PROJECT_NAME` defined in `.gitlab-ci.yaml` and `docker-vq` corresponding to the source tag.

Notice `../Dockerfile` is used to build Trino server/worker image, before start building, you can change the version
number through env `TRINO_VERSION` defined in Dockerfile, which will figure out the location of Trino source.

__The process of buiding docker image is relying on rider project, here is the help manual: [Trino on k8s](https://info.bilibili.co/display/INF/Trino+on+Kubernetes).__

# Dependencies
`Dockerfile.kerberos` will produce a Kerberos image with the latest openjdk-11, which is necessary for Trino, so please
make sure the Kerberos image has been existed before building Trino image.

# Enviroments
Before installation, there are some configurable enviroments defined in `./script/envs`, which can be set
through `export XXX=xxx` or `XXX=xxx ./install.sh`.

# Installation

Run `install.sh` as `root` user to install all the dependencies and the trino program.

After that, exec `launcher.sh` with options 'start | stop | kill | status | restart', then you will see
the process named of 'TrinoServer', listed by ```jps -lvm | grep -i trino```.

If no 'TrinoServer' listed, please check it's log file: `${TIRNO_DATA_DIR}/var/log/server.log`.

Besides install all the components through `install.sh`, user can launch the bash files in `scripts/` independently,
to reinstall dependecies or trino program.

# Installation through Jump Server
See: [Trino集群生产部署](https://info.bilibili.co/pages/viewpage.action?pageId=274285552)
