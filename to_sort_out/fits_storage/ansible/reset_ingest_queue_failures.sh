#!/bin/bash
# See: https://stackoverflow.com/questions/21870083/specify-sudo-password-for-ansible
ansible-playbook $* --vault-password-file vault.txt playbooks/reset_failed_ingests.yml