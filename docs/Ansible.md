### Ansible Installs

The install is managed by running an Ansible play.  This play is wrapped in a convenient shell script called
`archive_install.sh` in the `ansible` folder.  The play relies on you having a proper secrets setup to handle ssh
logins to the remote host, `hbffits-lv1.hi.gemini.edu`.

In the `ansible/playbooks` folder, you need to create a `secret` file.  Do this like so:

```
ansible-vault create secret
```

This will allow you to save a protected file that holds your ssh login password.  The file will look like this:

```
ansible_sudo_pass: mysudopassword
using_s3: True
s3_bucket_name: gemini-archive
s3_backup_bucket_name: gemini-archive-backup
s3_staging_area: /data/s3_staging
aws_access_key: <access_key>
aws_secret_key: <secret_key>
```

But now that file is also, in turn, password protected.  You can work around this with a "vault".  Create a file called
`vault.txt` in the `ansible/` folder and make the contents your password for the `secret` file created above.

Now you should have added two files.  Note that we *do not* add these to the repo.  This is a convoluted setup, but it
is what works.

```
ansible/playooks/secret
ansible/vault.txt
```

Now that this is done, assuming you have sudo permission to root on `hbffits-lv1`, you can run the ansible play.
To install, simply:

```
cd ansible
bash ./archive_install.sh -i dev
```

The `dev` file is setup to install to `hbffits-lv4.hi.gemini.edu`.  There are other files such 
as `dev-centos7` to deploy to a CentOS 7 host on `hbffits-lv1.hi.gemini.edu`.  There is also 
a `dev-aws` file for deploying to `arcdev.gemini.edu`.

Once the install finishes, you should be able to browse the deployed site at:

https://hbffits-lv4.hi.gemini.edu/searchform/
