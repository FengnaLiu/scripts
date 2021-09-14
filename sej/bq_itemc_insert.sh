#!/bin/bash
# 商品コントロールマスタ対応リリース前対応に実施する作業用のシェルスクリプト
# 差分コピージョブを実施する
# 実行するコマンドは、bash bq_itemc_insert.sh PROJECT_ID START_DATE END_DATE
# 例えばdev環境の商品コントロールマスタに対して、2021年06月7日から2021年06月11日までの範囲で実施したい場合は
# bash bq_insert.sh seven-central-dwh-dev 20210607 20210611

SCRIPT_NAME="$(basename ${BASH_SOURCE})"

## bqインストールされているかをチェックする
which bq >/dev/null
if [ $? -ne 0 ]; then
	echo "this script require bq." >/dev/stderr
	exit 1
fi

## gdateインストールされているかをチェックする
which gdate >/dev/null
if [ $? -ne 0 ]; then
	echo "this script require gdate." >/dev/stderr
	exit 1
fi

## 引数個数チェック
if [ $# -ne 3 ]; then
    echo "Usage: ${SCRIPT_NAME} PROJECT_ID START_DATE(yyyymmdd) END_DATE(yyyymmdd)" >&2
    exit 1
fi

## START_DATE,END_DATEフォーマットチェック
if [ $# -eq 3 ]; then
   PROJECT_ID=$1
   grep -E -x 'seven-central-dwh-(dev|dev2|test|prod)' <<<${PROJECT_ID}
   if [ $? -ne 0 ]; then
        echo "ERROR: Invalid PROJECT_ID: ${PROJECT_ID}" >&2
        exit 1
   fi

   START_DATE=$2
   grep -E -q -e '^[[:digit:]]{8}$' <<<${START_DATE}
   if [ $? -ne 0 ]; then
        echo "ERROR: Invalid START_DATE: ${START_DATE}" >&2
        exit 1
   fi

   END_DATE=$3
   grep -E -q -e '^[[:digit:]]{8}$' <<<${END_DATE}
   if [ $? -ne 0 ]; then
        echo "ERROR: Invalid END_DATE: ${END_DATE}" >&2
        exit 1
   fi

fi


echo "Start insert job from ${START_DATE} to ${END_DATE} in bigquery table of ${PROJECT_ID}"

# START_DATEからEND_DATEの日付範囲を対象としたそれぞれのBQクエリを順番に実行する。
# GNU dateを使用しているため、動作させるにはGNU dateの導入が必要
for (( DATE="${START_DATE}" ; ${DATE} <= "${END_DATE}" ; DATE=`gdate -d "${DATE} 1 day" '+%Y%m%d'`)) ; do
  # BQの日付フォーマットへ変更
  TARGET_DATE=`gdate -d "${DATE}" '+%Y-%m-%d'`

  # 補完ジョブを実施
  echo "Start Insert Job in ${TARGET_DATE} Partition of ${PROJECT_ID}"

  bq query --nouse_legacy_sql --project_id=${PROJECT_ID} \
  "INSERT \`${PROJECT_ID}.dwh.ItemControlMaster_tmp\` \
  SELECT PARSE_DATE('%Y-%m-%d','${TARGET_DATE}') as processing_date, * except(processing_date, processing_datetime), TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', current_timestamp)) as processing_datetime \
  FROM \`${PROJECT_ID}.dwh.ItemControlMaster_tmp\` \
  WHERE processing_date = DATE_SUB('${TARGET_DATE}', INTERVAL 1 DAY) AND processing_date > end_date AND processing_date < DATE_ADD(end_date, INTERVAL 183 DAY) AND (item_cd,pattern_no,start_date) not in \
  (select (item_cd,pattern_no,start_date) FROM \`${PROJECT_ID}.dwh.ItemControlMaster_tmp\` \
     WHERE processing_date = '${TARGET_DATE}');" \

done