#!/bin/bash

i=1
while [ $i -le 5 ]; do
    gsutil -m cp -r  gs://seven-central-dwh-dev-plu-master-pre/plu_master_csv_files_21000_30000_20210515000000 gs://seven-central-dev-plu-master/
    current_date_time="`date +%Y%m%d%H%M%S`"
    echo $current_date_time
    i=$(($i+1))
    sleep 1800
done