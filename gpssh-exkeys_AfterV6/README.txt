To enabling passwordless SSH in GP cluster, only use in Greenplum 6 version lately

Step 1. Generate key with ssh-keygen.
Step 2. Add to known_hosts.
Step 3. Use the ssh-copy-id command to add the user's public key to the other hosts, This enables 1-n passwordless SSH.
Step 4. Use gpssh-exkeys utility with your hostfile_exkeys file to enable n-n passwordless SSH.


Preparation:
1. If there is no sshpass cmd in your env, then copy sshpass to /usr/local/bin/

cp sshpass /usr/local/bin
chmod +x /usr/local/bin/sshpass

2. Edit hostfile

3. Run script
sh gpssh-exkeys_afterV6.sh -f allhosts

