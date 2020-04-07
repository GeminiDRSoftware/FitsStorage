# Jenkins Notes

## Installation

Although I have a Jenkins docker definition, I have setup a dedicated server on
the CentOS 7 host `hbffits-lv3.hi.gemini.edu`.  This may be temporary as a 
sandbox I have more of a free hand with, to eventually guide deployment on 
Bruno's DRAGONS Jenkins server (for instance).  Or we may go to Gitlab when
that is ready.  For now, I want something I can make big changes on and setup
docker/etc.

## Instructions

Instructions for installing Jenkins on CentOS 7 came from:

```
https://linuxize.com/post/how-to-install-jenkins-on-centos-7/
```

### Additional Requirements

#### git

```
sudo yum install -y git
```

#### docker

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

### pytest

Install python3
```
sudo yum install python3
```

### Ansible

First, install python3 as described in the pytest section.  Then:
```
pip3 install ansible
```
