# %%
import base64
import collections
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from time import sleep
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(".env.yaml")


# 楽天の認証情報の設定
# todo: 5/7にライセンスキーの認証が切れる
# https://cat-marketing.jp/2022/12/16/1471/
b64 = os.environ["SERVICE_SECRETS"] + ":" + os.environ["LISCENSE_KEY"]
b64_en = base64.b64encode(b64.encode())

headers = {
    "Authorization": b"ESA " + b64_en,
    "Content-Type": "application/json; charset=utf-8",
}
hits_limit = "100"


def get_item_list() -> pd.DataFrame:
    """商品の一覧を取得する関数。１回のAPIの取得上限があるため、繰り返しAPIを呼び出し

    Returns:
        pd.DataFrame: すべての商品のレスポンスデータを統合したDataFrame
    """
    serch_endpoint = (
        "https://api.rms.rakuten.co.jp/es/2.0/items/search?isHiddenItem=false&hits="
        + hits_limit
    )

    first_serch_endpoint = serch_endpoint + "&cursorMark=*"
    response = requests.get(url=first_serch_endpoint, headers=headers)
    df_items = pd.json_normalize(response.json()["results"])

    pre_cursor_mark = "*"
    post_cursor_mark = response.json()["nextCursorMark"]

    while pre_cursor_mark != post_cursor_mark:
        response_while = requests.get(
            url=(serch_endpoint + "&cursorMark=" + post_cursor_mark), headers=headers
        )
        df_items = pd.concat(
            [df_items, pd.json_normalize(response_while.json()["results"])]
        )
        pre_cursor_mark = post_cursor_mark
        post_cursor_mark = response.json()["nextCursorMark"]
    df_items = df_items.reset_index(drop=True)
    return df_items


#########################
# df_items = get_item_list()
# df = df_items
##########################


def prefix_df(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameの前処理。名前変更。
    メモリ節約のため、非破壊操作。

    Args:
        df (pd.DataFrame): 楽天ItemAPIで取得した商品一覧のDataFrame
    """
    # 価格情報と商品名、商品管理番号のみ抽出
    df_1 = df[["item.manageNumber", "item.title"]]
    df_2 = df.filter(like="standardPrice", axis="columns")
    df_necessary = pd.concat([df_1, df_2], axis="columns").fillna("")
    df_necessary["combined"] = df_necessary.iloc[:, 2:].apply(
        lambda x: ",".join(x.astype(str)), axis=1
    )
    # 価格情報の取得
    df_necessary.insert(0, "price", 0)
    for index, row in df_necessary.iterrows():
        s = row["combined"]
        # ステップ1: カンマで分割してリストに変換
        numbers_str_list = s.split(",")
        # ステップ2: 空の要素を除去
        numbers_str_list = [num for num in numbers_str_list if num]
        # SKUのバリエーションによって、商品名が異なるため
        # 重複しない要素の個数を判定して、格納
        c = collections.Counter(numbers_str_list)
        df_necessary.loc[index, "sku_number"] = len(c)
        # ステップ3: 各要素を整数型に変換
        numbers = [int(num) for num in numbers_str_list]
        # ステップ4: min関数を使用して最小値を見つける
        df_necessary.loc[index, "price"] = min(numbers)

    df_necessary = df_necessary.loc[
        :, ["item.manageNumber", "item.title", "price", "sku_number"]
    ]
    ############################
    # df_necessary = df_necessary.loc[:0, :]
    ############################
    # 全クーポン情報を取得して、全品に適用できるクーポンを抽出
    response_all_coupon = requests.get(
        url=("https://api.rms.rakuten.co.jp/es/1.0/coupon/search"),
        headers=headers,
    )
    root_all_item = ET.fromstring(response_all_coupon.content.decode("utf-8"))
    # クーポンコード
    coupon_code_list = []
    for value in root_all_item.iter("couponCode"):
        coupon_code_list.append(value.text)
    coupon_code_list.pop(0)
    # 商品タイプ。4だと、すべての商品に反映可能なクーポン
    itemtype_list = []
    for value in root_all_item.iter("itemType"):
        itemtype_list.append(value.text)
    # クーポンの開始日時
    start_date_list = []
    for value in root_all_item.iter("couponStartDate"):
        start_date_list.append(value.text)
    start_date_list.pop(0)
    # クーポンの終了日時
    end_date_list = []
    for value in root_all_item.iter("couponEndDate"):
        end_date_list.append(value.text)
    end_date_list.pop(0)
    # クーポンタイプ（割引なのか、値引きなのか）も取得
    coupont_type_list = []
    for value in root_all_item.iter("discountType"):
        coupont_type_list.append(value.text)
    # クーポンの割引額
    discount_list = []
    for value in root_all_item.iter("discountFactor"):
        discount_list.append(value.text)
    data_all_item = {
        "coupon_code": coupon_code_list,
        "item_type": itemtype_list,
        "start_date": start_date_list,
        "end_date": end_date_list,
        "discount": discount_list,
        "coupon_type": coupont_type_list,
    }
    coupon_df_all_item = pd.DataFrame(data_all_item)
    coupon_df_all_item = coupon_df_all_item[
        coupon_df_all_item["item_type"] == "4"
    ].reset_index(drop=True)
    coupon_df_all_item = coupon_df_all_item[
        coupon_df_all_item.columns[coupon_df_all_item.columns != "item_type"]
    ]
    # 各クーポンの適用条件を取得
    temp_coupon_df = pd.DataFrame()
    for index, row in coupon_df_all_item.iterrows():
        response_each_coupon = requests.get(
            url=(
                "https://api.rms.rakuten.co.jp/es/1.0/coupon/get?couponCode="
                + row["coupon_code"]
            ),
            headers=headers,
        )
        root_each_item = ET.fromstring(response_each_coupon.content.decode("utf-8"))
        # クーポンの適用タイプ
        condition_type = []
        for value in root_each_item.iter("conditionTypeCode"):
            condition_type.append(value.text)
        # クーポンの適用条件
        condition_value = []
        for value in root_each_item.iter("startValue"):
            condition_value.append(value.text)
        data_each_item = {
            "condition_type": condition_type,
            "condition_value": condition_value,
        }
        coupon_df_each_item = pd.DataFrame(data_each_item)
        coupon_df_each_item = coupon_df_each_item[
            coupon_df_each_item["condition_type"] == "RS003"
        ]
        if len(coupon_df_each_item) > 0:
            coupon_df_each_item.loc[:, "coupon_code"] = row["coupon_code"]
            temp_coupon_df = pd.concat([temp_coupon_df, coupon_df_each_item])
        else:
            temp_data = {
                "condition_type": "",
                "condition_value": 0,
                "coupon_code": row["coupon_code"],
            }
            temp_coupon_df = pd.concat(
                [temp_coupon_df, pd.Series(temp_data).to_frame().T]
            )
        sleep(1)
    coupon_df_all_item = (
        coupon_df_all_item.set_index(keys="coupon_code")
        .join(temp_coupon_df.set_index(keys="coupon_code"))
        .reset_index()
    )
    # 商品管理番号を指定してクーポン情報の取得
    coupon_endpoint = "https://api.rms.rakuten.co.jp/es/1.0/coupon/search"
    # 今日の日付
    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(tz=JST)
    for index, row in df_necessary.iterrows():
        ### クーポン情報の取得＋整理 ###
        response = requests.get(
            url=(coupon_endpoint + "?itemUrl=" + row["item.manageNumber"]),
            headers=headers,
        )
        root = ET.fromstring(response.content.decode("utf-8"))
        # クーポンコード
        coupon_code_list = []
        for value in root.iter("couponCode"):
            coupon_code_list.append(value.text)
        coupon_code_list.pop(0)
        # クーポンの開始日時
        start_date_list = []
        for value in root.iter("couponStartDate"):
            start_date_list.append(value.text)
        start_date_list.pop(0)
        # クーポンの終了日時
        end_date_list = []
        for value in root.iter("couponEndDate"):
            end_date_list.append(value.text)
        end_date_list.pop(0)
        # クーポンタイプ（割引なのか、値引きなのか）も取得
        coupont_type_list = []
        for value in root.iter("discountType"):
            coupont_type_list.append(value.text)
        # クーポンの割引額
        discount_list = []
        for value in root.iter("discountFactor"):
            discount_list.append(value.text)
        # クーポンがない時のエラー対応のため、適当な値を格納
        if len(discount_list) == 0:
            coupon_code_list.append("")
            start_date_list.append("2024-01-01T00:00:00+09:00")
            end_date_list.append("2024-01-01T00:00:00+09:00")
            discount_list.append(0)
            coupont_type_list.append(1)
        # lists to dict to dataframe
        data = {
            "coupon_code": coupon_code_list,
            "start_date": start_date_list,
            "end_date": end_date_list,
            "discount": discount_list,
            "coupon_type": coupont_type_list,
        }
        coupon_df = pd.DataFrame(data)
        # 全品に適用できるクーポン情報も結合
        coupon_df = pd.concat([coupon_df, coupon_df_all_item], axis="index")
        coupon_df.loc[:, "condition_value"] = (
            coupon_df.loc[:, "condition_value"].fillna(0).astype("int")
        )
        ### クーポン情報をDataFrameに格納完了 ###

        ### 条件から、適切なクーポンを抽出 ###
        # 今日の日付を満たすクーポンがある？
        coupon_df["start_date"] = pd.to_datetime(coupon_df["start_date"])
        coupon_df["end_date"] = pd.to_datetime(coupon_df["end_date"])
        available_coupon_df = coupon_df[
            (coupon_df["start_date"] < pd.to_datetime(today))
            & (coupon_df["end_date"] > pd.to_datetime(today))
        ]
        # 割引後の値段が最も大きいクーポンを選んでいく
        available_coupon_df.loc[:, "discount"] = available_coupon_df["discount"].astype(
            "int32"
        )
        available_coupon_df = available_coupon_df.reset_index(drop=True)

        # 該当するクーポンがないとき
        if len(available_coupon_df) < 1:
            df_necessary.loc[index, "discount"] = 0
            df_necessary.loc[index, "discount_type"] = 0
            df_necessary.loc[index, "discount_price"] = 0
        else:
            # それぞれのクーポンを適用すると、いくらになるのか
            for tmp_index, tmp_row in available_coupon_df.iterrows():
                if tmp_row["coupon_type"] == "1":
                    available_coupon_df.loc[tmp_index, "discounted_price"] = (
                        df_necessary.loc[index, "price"] - tmp_row["discount"]
                    )
                elif tmp_row["coupon_type"] == "2":
                    available_coupon_df.loc[tmp_index, "discounted_price"] = (
                        df_necessary.loc[index, "price"]
                        * (100 - tmp_row["discount"])
                        / 100
                    )
                else:
                    available_coupon_df.loc[tmp_index, "discounted_price"] = (
                        df_necessary.loc[index, "price"]
                    )
            # 割引後の価格を小さい順に並べ替え
            ordereded_available_coupon_df = available_coupon_df.sort_values(
                by=["discounted_price"]
            )
            # クーポンの適用条件を満たしているチェック
            is_available = False
            for i, r in ordereded_available_coupon_df.iterrows():
                if r["condition_value"] <= df_necessary.loc[index, "price"]:
                    df_necessary.loc[index, "discount"] = available_coupon_df.loc[
                        i, "discount"
                    ]
                    df_necessary.loc[index, "discount_type"] = available_coupon_df.loc[
                        i, "coupon_type"
                    ]
                    df_necessary.loc[index, "discount_price"] = available_coupon_df.loc[
                        i, "discounted_price"
                    ]
                    is_available = True
                    break
                else:
                    print("クーポンの適用条件を満たさない！次のクーポンをチェック！")
            # 最終的に条件を満たしているクーポンがあったかどうかの判定
            if is_available == False:
                df_necessary.loc[index, "discount"] = 0
                df_necessary.loc[index, "discount_type"] = 0
                df_necessary.loc[index, "discount_price"] = 0

        ### 適切なクーポン情報を取得完了 ###

        ### 商品名の変更を開始 ###
        # 新しい商品名をカラムに追加していく
        old_title = re.sub(r"^【[^】]*】", "", row["item.title"])
        # 型変換
        discount_price = int(df_necessary.loc[index, "discount_price"])
        discount = df_necessary.loc[index, "discount"]
        ## 定額値引き
        if df_necessary.loc[index, "discount_type"] == "1":
            ## SKUの数によって場合分け
            if row["sku_number"] > 1:
                df_necessary.loc[index, "new_name"] = (
                    f"【{today.strftime('%-m/%-d')}！クーポンで{discount_price:,}円～】{old_title}"
                )
            elif row["sku_number"] == 1:
                df_necessary.loc[index, "new_name"] = (
                    f"【{today.strftime('%-m/%-d')}！クーポンで{discount_price:,}円】{old_title}"
                )
        ## 定率値引き
        # この時、割引率で場合わけ必要
        elif df_necessary.loc[index, "discount_type"] == "2":
            if discount > 51:
                ## SKUの数によって場合分け
                if row["sku_number"] > 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！{str(discount)}％OFF！{discount_price:,}円～】{old_title}"
                    )
                elif row["sku_number"] == 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！{str(discount)}％OFF！{discount_price:,}円】{old_title}"
                    )
            elif discount == 50:
                ## SKUの数によって場合分け
                if row["sku_number"] > 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！半額クーポンで{discount_price:,}円～】{old_title}"
                    )
                elif row["sku_number"] == 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！半額クーポンで{discount_price:,}円】{old_title}"
                    )
            else:
                if row["sku_number"] > 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！クーポン利用で{discount_price:,}円～】{old_title}"
                    )
                elif row["sku_number"] == 1:
                    df_necessary.loc[index, "new_name"] = (
                        f"【{today.strftime('%-m/%-d')}！クーポン利用で{discount_price:,}円】{old_title}"
                    )
        # その他
        else:
            df_necessary.loc[index, "new_name"] = "【{}！】{}".format(
                today.strftime("%-m/%-d"),
                old_title,
            )
        sleep(1)
        # print("{}商品目完了".format(index + 1))
        # print(df_necessary.loc[index, "new_name"])

    print("====新しい商品名への変更完了====")

    return df_necessary


def upsert_items(df: pd.DataFrame):
    upsert_endpoint = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers/"
    for index, row in df.iterrows():
        response = requests.patch(
            url=upsert_endpoint + str(row["item.manageNumber"]),
            headers=headers,
            json={"title": row["new_name"]},
        )
        sleep(1)
        if response.status_code == 204:
            print(f"{index + 1}商品目変更完了")
        else:
            print(response.json())
            print(f"{index + 1}商品目変更エラー")


def main(argas):
    try:
        df_items = get_item_list()
        df_items_necessary = prefix_df(df_items)
        upsert_items(df_items_necessary)
    except:
        sleep(5)
        print("1st Retry")
        try:
            df_items = get_item_list()
            df_items_necessary = prefix_df(df_items)
            upsert_items(df_items_necessary)
        except:
            sleep(10)
            print("2nd Retry")
            df_items = get_item_list()
            df_items_necessary = prefix_df(df_items)
            upsert_items(df_items_necessary)
    return "200"


# %%
# df_items = get_item_list()
# df_items_necessary = prefix_df(df_items)
# upsert_items(df_items_necessary)
