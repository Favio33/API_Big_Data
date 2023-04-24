#!/bin/sh

wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
mv cloud_sql_proxy.linux.amd64 cloud_sql_proxy
chmod +x cloud_sql_proxy
./cloud_sql_proxy -credential_file=./service_account/code-challenge-384515-55073656010b.json -dir=/cloudsql --instances=code-challenge-384515:us-central1:hr-db=tcp:5001
