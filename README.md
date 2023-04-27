# Code Challenge

Hi! This repo was created to solve the challenge, I have fun develop this features and learn a lot of things.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Contributing](#contributing)
- [License](#license)

## Introduction

It is develop using Python as a runtime and Flask for the APIs. The database is allocated in Cloud SQL - GCP, files are storaged in GCS and, as mostly every Big Data Project, Hadoop and Spark was used for some features using Dataproc.
On the other hand, Cloud Functions helped me to deploy some features to diversify GCP tools

- https://console.cloud.google.com/welcome?authuser=1&project=code-challenge-384515

## Features

- List rows from an table (http://127.0.0.1:5000/api/db/<table_name>/<num_rows>)
- Insert new rows in a certain table (http://127.0.0.1:5000/api/db/bulkInsert/<table_name>)
- Script to historyc insert - batch_historyc.py (It must be run in Dataproc using init_proxy.sh to activate Cloud SQL Proxy)
- Backup Table - Cloud Function (https://us-central1-code-challenge-384515.cloudfunctions.net/backup-db)
- Recovery Table - Cloud Function (https://us-central1-code-challenge-384515.cloudfunctions.net/recovery-table)

## Installation

Historyc Script
- Open Dataproc Jupyter Lab
- Open a terminal and run this shell (./service_account/init_proxy.sh)
- Open BatchHistoric.ipynb in GCS directory
- Run it!

API (List and Insert Rows)
- First run pip install -r requirements.txt
- Set up your profile configuration in Google Cloud SDK to connect to the database; and set up .env credentials for database connection
  [Guide Proxy](https://docs.google.com/document/d/1tVSUHsMd0pDiPr9gKnLgQpFi1rG75kSo/edit?usp=share_link&ouid=117615636739294319730&rtpof=true&sd=true)
- Run main.py locally and use postman or a web to request the API
  - List Rows: http://127.0.0.1:5000/api/db/<table_name>/<num_rows>
  - Insert Rows: http://127.0.0.1:5000/api/db/bulkInsert/<table_name> - Body must be considerar all the columns for each register and focus on FK!

Backup Table
  - Try it in Cloud Functions! No body requests
  - Find your avro files in Cloud Storage gs://backup-cd

Recovery Table
  - Try it in Cloud Functions! Body request: name and date("%Y%m%d")
  - Check your database!

