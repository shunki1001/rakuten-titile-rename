# %%
import base64
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from time import sleep
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import zeep
from dotenv import load_dotenv

load_dotenv(".env.yaml")


# 楽天の認証情報の設定
# 商品APIはREST APIだからheaderに。クーポン情報はSOAP APIだからbodyに。
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


def prefix_df(df: pd.DataFrame):
    """DataFrameの前処理。名前変更。
    メモリ節約のため、非破壊操作。

    Args:
        df (pd.DataFrame): 楽天ItemAPIで取得した商品一覧のDataFrame
    """
    df = df[["item.manageNumber", "item.title"]]
    df.insert(0, "discount", "")

    # クーポン情報の取得
    coupon_endpoint = "https://api.rms.rakuten.co.jp/es/1.0/coupon/search"
    # 今日の日付
    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(tz=JST)
    for index, row in df_items.iterrows():
        coupon_endpoint = "https://api.rms.rakuten.co.jp/es/1.0/coupon/search"
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
        # クーポンの割引額
        discount_list = []
        for value in root.iter("discountFactor"):
            discount_list.append(value.text)
        # lists to dict to dataframe
        data = {
            "start_date": start_date_list,
            "end_date": end_date_list,
            "discount": discount_list,
        }
        coupon_df = pd.DataFrame(data)
        # 今日の日付を満たすクーポンがある？
        coupon_df["start_date"] = pd.to_datetime(coupon_df["start_date"])
        coupon_df["end_date"] = pd.to_datetime(coupon_df["end_date"])
        # available_coupon_df = coupon_df[
        #     (coupon_df["start_date"] < pd.to_datetime(today))
        #     & (coupon_df["end_date"] > pd.to_datetime(today))
        # ]
        # 複数ある場合は、割引額が最も大きいクーポンを選択
        # available_coupon_df["discount"] = available_coupon_df["discount"].astype(
        #     "int32"
        # )
        df.loc[index, "discount"] = coupon_df["discount"].max()
        sleep(3)
    return df


# def upsert_items(df: pd.DataFrame):

#     upsert_endpoint = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers/"
#     for row in df.iterrows:
#         response = ?requests?.patch(
#             url=upsert_endpoint + row[1]['item.manageNumber'],
#             headers=headers,
#             data={
#                 "title": row[1]['item.title']
#             }
#         )
# %%
df_items = get_item_list()
df = prefix_df(df_items)
