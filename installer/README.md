# trino-installer

# Package
```
tar -cvf installer.tar installer
```

# Installation
Step 1. Upload Trino archive and installer archive to base directory `TRINO_HOME_BASE`. Extract installer archive.
```
cd $TRINO_HOME_BASE
rm -rf installer
tar -xvf installer.tar
```

Step 2. Prepare for cluster configuration in `cluster-env/<cluster_name>-env.sh`. All configurable variables can be found in `cluster-env/default-env.sh`.

Step 3. Install trino using pre-defined configuration
```
export TRINO_VERSION=367-bili-0.3.7
installer/install.sh -c <cluster_name>
```
If we only need to update configuration of cluster, use `-u` option
```
installer/install.sh -c <cluster_name> -u
```

# Launch
The following commands should be run as `trino` user.
```
cd $TRINO_HOME_BASE
./switch_version.sh
./launcher.sh <command>
```
Command options are `start | stop | kill | status | restart`.
