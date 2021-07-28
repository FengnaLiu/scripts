#!/bin/bash
# 商品パターン区分マスタのSpanner側データ更新ジョブのシェルスクリプト
# 商品パターン区分マスタのSpanner側データ更新ジョブのリカバリーを実施する
# 実行するコマンドは、bash recovery_itempatternclass_spanner_update.sh PROJECT_ID START_DATE END_DATE
# 例えばdev環境の商品コントロールマスタに対して、2021年06月7日から2021年06月11日までの範囲で実施したい場合は
# bash recovery_itempatternclass_spanner_update.sh seven-central-dwh-dev 20210607 20210611

SCRIPT_NAME="$(basename ${BASH_SOURCE})"

## gcloudインストールされているかをチェックする
which gcloud >/dev/null
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
  echo "this script require gcloud." >/dev/stderr
  exit 1
fi

## gdateインストールされているかをチェックする
which gdate >/dev/null
# shellcheck disable=SC2181
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
  echo "project: ${PROJECT_ID}"
  grep -E -q -x 'seven-central-dwh-(dev|dev2|test|prod)' <<<${PROJECT_ID}
  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    echo "ERROR: Invalid PROJECT_ID: ${PROJECT_ID}" >&2
    exit 1
  fi

  START_DATE=$2
  echo "start_date: ${START_DATE}"
  grep -E -q -e '^[[:digit:]]{8}$' <<<${START_DATE}
  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    echo "ERROR: Invalid START_DATE: ${START_DATE}" >&2
    exit 1
  fi

  END_DATE=$3
  echo "end_date: ${END_DATE}"
  grep -E -q -e '^[[:digit:]]{8}$' <<<${END_DATE}
  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    echo "ERROR: Invalid END_DATE: ${END_DATE}" >&2
    exit 1
  fi

fi

echo "Start insert job from ${START_DATE} to ${END_DATE} in Spanner table of ${PROJECT_ID}"

## 初期化ログディレクトリ
rm -rf ./log
mkdir log

# リカバリー作業を実施
SUFFIX=$(date '+%Y-%m-%d-%H-%M-%S')
LOG_FILE="recovery_his_${SUFFIX}.txt"
echo "Start recovery job for ${TARGET_DATE} in ${PROJECT_ID}" 1>./log/${LOG_FILE}

# START_DATEからEND_DATEの日付範囲を対象としたそれぞれのクエリを順番に実行する。
# GNU dateを使用しているため、動作させるにはGNU dateの導入が必要
for ((DATE = "${START_DATE}"; ${DATE} <= "${END_DATE}"; DATE = $(gdate -d "${DATE} 1 day" '+%Y%m%d'))); do
  # 日付フォーマットへ変更
  TARGET_DATE=$(gdate -d "${DATE}" '+%Y-%m-%d')
  NEXT_DATE=$(gdate -d "${DATE} 1 day" '+%Y-%m-%d')

  echo "${TARGET_DATE} recovery job started." 1>>./log/"${LOG_FILE}"
  # loopで小分けして差分を削除する
  while true; do
      gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project="${PROJECT_ID}" \
        --sql="DELETE FROM ItemPatternClassMaster \
       WHERE ItemPatternClassMaster.item_cd IN \
       (SELECT ItemPatternClassMaster.item_cd FROM ItemPatternClassMaster INNER JOIN ItemControlMaster \
          ON ItemPatternClassMaster.item_cd = ItemControlMaster.item_cd AND \
          ItemPatternClassMaster.pattern_class <> ItemControlMaster.pattern_class \
          AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z' \
          AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z' limit 1800000)" 1>>./log/"${LOG_FILE}" 2>&1

      # Mutation制限した削除ジョブ失敗したら、無限LOOPに落ちないように、スクリプトを終了させる
      # shellcheck disable=SC2181
      if [ $? -ne 0 ]; then
        echo "Failed: diff delete: ${TARGET_DATE}" 1>>./log/"${LOG_FILE}"
        exit 1
      fi

      # Mutation制限なしで削除ジョブを実施する（すべて削除できたかを確認する）
      gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project="${PROJECT_ID}" \
        --sql="DELETE FROM ItemPatternClassMaster \
       WHERE ItemPatternClassMaster.item_cd IN \
       (SELECT ItemPatternClassMaster.item_cd FROM ItemPatternClassMaster INNER JOIN ItemControlMaster \
            ON ItemPatternClassMaster.item_cd = ItemControlMaster.item_cd AND \
            ItemPatternClassMaster.pattern_class <> ItemControlMaster.pattern_class \
            AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z' \
            AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z')" 2>/dev/null 1>>./log/"${LOG_FILE}"

      # 上記のMutation制限無しでも実行成功するとしたら、削除ジョブ無事完了できたことを判断できるため、LOOPから抜け出す
      # shellcheck disable=SC2181
      if [ $? -eq 0 ]; then
        echo "Completed: diff delete: ${TARGET_DATE}" 1>>./log/"${LOG_FILE}"
        break
      fi

 done

# loopで小分けして差分を挿入する
  while true; do
      if [ "${PROJECT_ID}" == "seven-central-dwh-prod" ]; then
          # prodの環境で同じ日に同じ商品コードで、複数パターンクラスになる場合あるため、失敗される仕様にするため、ANY_VALUE付けない
          gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project=${PROJECT_ID} \
            --sql="INSERT INTO ItemPatternClassMaster (commit_timestamp,item_cd, pattern_class) \
              SELECT DISTINCT CURRENT_TIMESTAMP(), item_cd, pattern_class　FROM ItemControlMaster \
                 WHERE item_cd NOT in (SELECT item_cd FROM ItemPatternClassMaster) \
                 AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z'  \
                 AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z' limit 6000 " 1>>./log/"${LOG_FILE}" 2>&1

          # Mutation制限した挿入ジョブ失敗したら、無限LOOPに落ちないように、スクリプトを終了させる
          # shellcheck disable=SC2181
          if [ $? -ne 0 ]; then
            echo "Failed: diff insert: ${TARGET_DATE}" 1>>./log/recovery_his.txt
            exit 1
          fi

          # limitなしでコマンド実施する（すべて挿入できたかを確認する）
          gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project=${PROJECT_ID} \
            --sql="INSERT INTO ItemPatternClassMaster (commit_timestamp,item_cd, pattern_class) \
             SELECT DISTINCT CURRENT_TIMESTAMP(), item_cd, pattern_class　FROM ItemControlMaster \
               WHERE item_cd NOT in (SELECT item_cd FROM ItemPatternClassMaster) \
               AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z'  \
              AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z'" 2>/dev/null 1>>./log/"${LOG_FILE}"

          #　limitなしでもコマンド成功したら、すべての挿入成功しているのを判断できるため、LOOPから抜き出す
          # shellcheck disable=SC2181
          if [ $? -eq 0 ]; then
            echo "Completed: diff insert: ${TARGET_DATE}" 1>>./log/recovery_his.txt
            break
          fi

      else
          # test,dev,dev2の環境で同じ日に同じ商品コードで、複数パターンクラスになる場合あるため、ANY_VALUE(pattern_class)にしている
          gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project=${PROJECT_ID} \
            --sql="INSERT INTO ItemPatternClassMaster (commit_timestamp,item_cd, pattern_class) \
              SELECT DISTINCT CURRENT_TIMESTAMP(), item_cd, ANY_VALUE(pattern_class)　FROM ItemControlMaster \
                 WHERE item_cd NOT in (SELECT item_cd FROM ItemPatternClassMaster) \
                 AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z'  \
                 AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z' GROUP BY item_cd limit 6000 " 1>>./log/"${LOG_FILE}" 2>&1

          # Mutation制限した挿入ジョブ失敗したら、無限LOOPに落ちないように、スクリプトを終了させる
          # shellcheck disable=SC2181
          if [ $? -ne 0 ]; then
            echo "Failed: diff insert: ${TARGET_DATE}" 1>>./log/"${LOG_FILE}"
            exit 1
          fi

          # limitなしでコマンド実施する（すべて挿入できたかを確認する）
          gcloud spanner databases execute-sql store-items --instance=seven-central-dwh --project=${PROJECT_ID} \
            --sql="INSERT INTO ItemPatternClassMaster (commit_timestamp,item_cd, pattern_class) \
             SELECT DISTINCT CURRENT_TIMESTAMP(), item_cd, ANY_VALUE(pattern_class)　FROM ItemControlMaster \
               WHERE item_cd NOT in (SELECT item_cd FROM ItemPatternClassMaster) \
               AND ItemControlMaster.commit_timestamp>='${TARGET_DATE}T15:00:00.000Z'  \
              AND ItemControlMaster.commit_timestamp<'${NEXT_DATE}T15:00:00.000Z' GROUP BY item_cd " 2>/dev/null 1>>./log/"${LOG_FILE}"

          #　limitなしでもコマンド成功したら、すべての挿入成功しているのを判断できるため、LOOPから抜き出す
          # shellcheck disable=SC2181
          if [ $? -eq 0 ]; then
            echo "Completed: diff insert: ${TARGET_DATE}" 1>>./log/"${LOG_FILE}"
            break
          fi

      fi

  done

done