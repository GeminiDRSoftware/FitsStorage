## Jenkins Notes

### Installation

Although I have a Jenkins docker definition, I have setup a dedicated server on
the CentOS 7 host `hbffits-lv3.hi.gemini.edu`.  This may be temporary as a 
sandbox I have more of a free hand with, to eventually guide deployment on 
Bruno's DRAGONS Jenkins server (for instance).  Or we may go to Gitlab when
that is ready.  For now, I want something I can make big changes on and setup
docker/etc.

### Instructions

Instructions for installing Jenkins on CentOS 7 came from:

```
https://linuxize.com/post/how-to-install-jenkins-on-centos-7/
```

#### Additional Requirements

##### git

```
sudo yum install -y git
```

##### docker

Install Container-SELinux
```
sudo yum install -y http://mirror.centos.org/centos/7/extras/x86_64/Packages/container-selinux-2.107-1.el7_6.noarch.rpm
```

Install Docker Repo and Packages
```
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install docker-ce docker-ce-cli containerd.io
sudo systemctl start docker
sudo usermod -a -G docker jenkins
sudo usermod -a -G root jenkins
```

Then restart Jenkins
```
sudo systemctl stop jenkins
sudo systemctl start jenkins
```

#### pytest

Install python3
```
sudo yum install python3
```

#### Ansible

First, install python3 as described in the pytest section.  Then:
```
pip3 install ansible
```

### FitsStorage Deploys

I have the Jenkins server setup to do deploys of the FitsServer to `dev` and `qap-dev`.
These builds involve creating a set of docker images to replicate the service spinning 
up the containers, creating the database and running a set of unit and integration tests
against it.  Only if everything passes does Jenkins then use ansible to deploy the latest
checkin onto the target host.

#### Jenkins Docker Disk Space

Docker has a nasty habit of eating up disk space until you run out.  If you see errors in
the build about a `devicemapper` folder being full, do this on the server as root.  *Note that
although the path is `/var/lib/docker`, I have made that a symlink into the larger `/data/` 
folder.  So it's the free space in `/data/` that matters here.

```
sudo service docker stop
sudo rm -rf /var/lib/docker/image/devicemapper /var/lib/docker/devicemapper
sudo service docker start
```