#!/bin/bash

i=1
FILE="/Users/liu/Downloads/temp/"$i".json"
echo $FILE
cat $FILE | jq -r ". | {id,sendtimes,agg:.aggregate_unit[]} | [.id, .sendtimes, .agg.store_cd, .agg.created_datetime,.agg.instore_label_cd, .agg.item_cd, .agg.tyunou_yyyymmdd, .agg.shipping_no, .agg.delivery_amnt, .agg.sales_amnt, .agg.disposal_amnt,.returns_amnt,
.agg.move_amnt, .agg.inside_store_amnt, .agg.delivery_plans_datetime, .agg.out_of_freshness_datetime, .agg.delivery_processing_datetime,
.agg.final_shipment_datetime, .agg.final_shipment_processing_type] | @csv" > ./result/instore.csv

i=$(($i+1))
while [ $i -le 99 ]; do

    FILE="/Users/liu/Downloads/temp/"$i".json"
    echo $FILE
    cat $FILE | jq -r ". | {id,sendtimes,agg:.aggregate_unit[]} | [.id, .sendtimes, .agg.store_cd, .agg.created_datetime,.agg.instore_label_cd, .agg.item_cd, .agg.tyunou_yyyymmdd, .agg.shipping_no, .agg.delivery_amnt, .agg.sales_amnt, .agg.disposal_amnt,.returns_amnt,
.agg.move_amnt,.inside_store_amnt, .agg.delivery_plans_datetime, .agg.out_of_freshness_datetime, .agg.delivery_processing_datetime,
.agg.final_shipment_datetime, .agg.final_shipment_processing_type] | @csv" >> ./result/instore.csv
    i=$(($i+1))

done