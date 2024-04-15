# %%
import base64
import collections
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime
from time import sleep
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import zeep
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
        "https://api.rms.rakuten.co.jp/es/2.0/items/search?hits=" + hits_limit
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
    # SKUが複数ある商品においては、「○○円～！」。一つの時は「○○円！」
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
    df_necessary.insert(0, "discount", "")

    # クーポン情報の取得
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
            start_date_list.append("2024-01-01T00:00:00+09:00")
            end_date_list.append("2024-01-01T00:00:00+09:00")
            discount_list.append(0)
            coupont_type_list.append(1)
        # lists to dict to dataframe
        data = {
            "start_date": start_date_list,
            "end_date": end_date_list,
            "discount": discount_list,
            "coupon_type": coupont_type_list,
        }
        coupon_df = pd.DataFrame(data)
        ### クーポン情報をDataFrameに格納完了 ###

        ### 条件から、適切なクーポンを抽出 ###
        # 今日の日付を満たすクーポンがある？
        coupon_df["start_date"] = pd.to_datetime(coupon_df["start_date"])
        coupon_df["end_date"] = pd.to_datetime(coupon_df["end_date"])
        available_coupon_df = coupon_df[
            (coupon_df["start_date"] < pd.to_datetime(today))
            & (coupon_df["end_date"] > pd.to_datetime(today))
        ]
        # 複数ある場合は、割引額が最も大きいクーポンを選択
        available_coupon_df.loc[:, "discount"] = available_coupon_df["discount"].astype(
            "int32"
        )
        if len(available_coupon_df) > 1:
            max_index = available_coupon_df["discount"].idxmax()
            available_coupon_df = available_coupon_df.loc[max_index]
            df_necessary.loc[index, "discount"] = available_coupon_df["discount"]
            df_necessary.loc[index, "discount_type"] = available_coupon_df[
                "coupon_type"
            ]
        else:
            df_necessary.loc[index, "discount"] = 0
            df_necessary.loc[index, "discount_type"] = 0
        ### 適切なクーポン情報を取得完了 ###

        ### 商品名の変更を開始 ###
        # 新しい商品名をカラムに追加していく
        if len(row["item.title"].split("】")) > 1:
            old_title = row["item.title"].split("】")[1]
        else:
            old_title = row["item.title"]
        ## 定額値引き
        if df_necessary.loc[index, "discount_type"] == "1":
            discount_price = row["price"] - df_necessary.loc[index, "discount"]
            ## SKUの数によって場合分け
            if row["sku_number"] > 1:
                df_necessary.loc[index, "new_name"] = (
                    "【{}！最大{}円OFF！{:.0f}円～（値引き後）クーポン利用で】{}".format(
                        today.strftime("%-m/%-d"),
                        str(df_necessary.loc[index, "discount"]),
                        discount_price,
                        old_title,
                    )
                )
            elif row["sku_number"] == 1:
                df_necessary.loc[index, "new_name"] = (
                    "【{}！最大{}円OFF！{:.0f}円（値引き後）クーポン利用で】{}".format(
                        today.strftime("%-m/%-d"),
                        str(df_necessary.loc[index, "discount"]),
                        discount_price,
                        old_title,
                    )
                )
            else:
                df_necessary.loc[index, "new_name"] = "【{}！】{}".format(
                    today.strftime("%-m/%-d"),
                    old_title,
                )
        ## 定率値引き
        elif df_necessary.loc[index, "discount_type"] == "2":
            discount_price = (
                row["price"] * (100 - df_necessary.loc[index, "discount"]) / 100
            )
            ## SKUの数によって場合分け
            if row["sku_number"] > 1:
                df_necessary.loc[index, "new_name"] = (
                    "【{}！最大{}％OFF！{:.0f}円～（値引き後）クーポン利用で】{}".format(
                        today.strftime("%-m/%-d"),
                        str(df_necessary.loc[index, "discount"]),
                        discount_price,
                        old_title,
                    )
                )
            elif row["sku_number"] == 1:
                df_necessary.loc[index, "new_name"] = (
                    "【{}！最大{}％OFF！{:.0f}円（値引き後）クーポン利用で】{}".format(
                        today.strftime("%-m/%-d"),
                        str(df_necessary.loc[index, "discount"]),
                        discount_price,
                        old_title,
                    )
                )
            else:
                df_necessary.loc[index, "new_name"] = "【{}！】{}".format(
                    today.strftime("%-m/%-d"),
                    old_title,
                )
        # その他
        else:
            df_necessary.loc[index, "new_name"] = "【{}！】{}".format(
                today.strftime("%-m/%-d"),
                old_title,
            )
        sleep(1)
        print("{}商品目完了".format(index + 1))
        print(df_necessary.loc[index, "new_name"])

    return df_necessary


def upsert_items(df: pd.DataFrame):
    upsert_endpoint = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers/"
    for index, row in df.iterrows():
        response = requests.patch(
            url=upsert_endpoint + str(row["item.manageNumber"]),
            headers=headers,
            json={"title": row["new_name"]},
        )
        if response.status_code == 204:
            print(f"{index + 1}商品目変更完了")
        else:
            print(response.json())
            print(f"{index + 1}商品目変更エラー")
            print(row)


# %%
df_items = get_item_list()
df_items_necessary = prefix_df(df_items)
