#! /bin/bash

for i in {0..7}
do
	vm=$((i+2))
	folder="invoker"$((i))
	mkdir -p $folder
	path="beitong2@sp21-cs525-g20-0"$vm".cs.illinois.edu:/var/tmp/wsklogs"
	echo 'password' | sshfs -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $path $folder -o workaround=rename -o password_stdin
done

vm=$((1))
folder="controller0"
mkdir -p $folder
path="beitong2@sp21-cs525-g20-0"$vm".cs.illinois.edu:/var/tmp/wsklogs"
echo 'password' | sshfs -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $path $folder -o workaround=rename -o password_stdin
