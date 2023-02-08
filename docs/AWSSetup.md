# AWS Setup Documentation

## SSH Tunnel
deprecated - we use git currently
#### ssh connection that tunnels svn with it
#### From inside gemini:
ssh -R 8080:scisoft:80 arcdev

## Setup from scratch:
1. Launch Instance
2. AWS Marketplace -> search for centos
3. CentOS 7 (x86_64) with updates HVM # don't select a specific version eg 6.4 - just get generic latest
4. m4.2xlarge
5. configure details
6. subnet for 2a
7. protect against termination
8. add storage
 - root to 8 GB General purpose SSD
 - 250GB IO1 with 500 IOPS
9. tag instance
10. config security group
11. select existing secuirty group and select it
12. review and launch
13. launch
14. label it as archive in instance list

## User/SSH Setup
1. ssh -i arcdev.pem centos@ip-address
 - Check root partition is full size
2. yum update   This will take a while
3. shutdown -r now  # to pick up new kernel etc from update
4. ssh back in
5. useradd -u 5151 phirst
6. useradd -u 5179 fitsdata
7. passwd phirst
8. passwd fitsdata
9. sudo vi /etc/ssh/sshd_config
 - RSAAuthentication yes
 - PubkeyAuthentication yes
 - PasswordAuthentication yes
 - TCPKeepAlive yes
10. systemctl restart sshd
11. vi /etc/group 
 - add phirst user to wheel group at least
12. vi /etc/selinux/config
 - SELINUX=permissive

## Install basic required packages
1. yum install -y lvm2 mdadm ntp tcsh wget sysstat psmisc dump subversion
2. vi /etc/ntp.conf
3. server x.amazon.pool.ntp.org iburst   (for all servers)
4. systemctl start ntpd
5. systemctl enable ntpd
6. ntpstat  # to check status
7. vi /etc/sysconfig/network
 - HOSTNAME=arcdev.gemini.edu
8. vi /etc/hostname
 - arcdev.gemini.edu
9. vi /etc/cloud/cloud.cfg append to end of file:
 - preserve_hostname: true

## LVM layer
1. pvcreate /dev/xvdb
2. vgcreate data /dev/xvdb
3. lvcreate --size=240GB -n data data
4. mke2fs -t ext4 -L data /dev/data/data
5. vi /etc/fstab
 - mount /data

## SELINUX permissive
1. vi /etc/selinux/config 
 - SELINUX=permissive

## Firewall
1. yum install system-config-firewall
 - might need to reboot
2. system-config-firewall 
 - disable it

## postfix for email
1. yum install postfix
2. vi /etc/postfix/main.cf
 - set hostname to arcdev.gemini.edu
3. AWS webform to request reverse DNS entry for elastic IP and EIP whitelisting
