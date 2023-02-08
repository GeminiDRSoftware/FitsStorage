#!/bin/bash
# See: https://stackoverflow.com/questions/21870083/specify-sudo-password-for-ansible
# Usage: add_to_ingest_queue.sh -i dev -e "newfiles=20"
ansible-playbook $* playbooks/ingest_files.yml --ask-become-pass
