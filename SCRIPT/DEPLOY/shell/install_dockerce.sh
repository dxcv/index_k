#!/bin/sh

# Uninstall old versions
yum remove docker docker-common docker-selinux docker-engine &&

# SET UP THE REPOSITORY
# 1) Install required packages. `yum-utils` provaides the `yum-config-manager` utility,
# and `device-mapper-persistent-data` and `lvm2` are required by the `devicemapper` storage driver
yum install -y yum-utils device-mapper-persistent-data lvm2 &&

# 2) Use the following command to set up the stable repository. You always need the stable repository,
# even if you want to install builds from the edge or test repositories as well.
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo &&

# 3) Optional: Enable the edge and test repositories. These repositories are included in the `docker.repo` file above
# but are disabled by default. You can enable them alongside the stable repository.
# yum-config-manager --enable docker-ce-edge
# yum-config-manager --enable docker-ce-test

# You can disable the edge or test repository by running the yum-config-manager command with the --disable flag.
# To re-enable it, use the --enable flag. The following command disables the edge repository.
# yum-config-manager --disable docker-ce-edge

# INSTALL DOCKER CE
# 1) Install the latest version of Docker CE, or go to the next step to install a specific version.
yum install -y docker-ce &&

# 2) On production systems, you should install a specific version of Dokcer CE instead of always using the latest.
# List the available versions. This example uses the sort -r command to sort the results by version number, highest to lowest.
# yum list docker-ce --showduplicates | sort -r
# yum install <FULLY-QUALIFIED-PACKAGE-NAME>
#
# 3) Start Docker
systemctl start docker &&

# 4) Verify that `docker` is installed correctly by running the `hello-world` image
docker run hello-world