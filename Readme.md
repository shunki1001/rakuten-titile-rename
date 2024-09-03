## 楽天市場の商品名を変更するツール

1. 商品一覧を取得して、
2. クーポン一覧を取得して、
3. 次項に記載の条件により、新しい商品名を決定して、
4. 商品名変更の patchAPI を呼び出す

## 商品名変更のロジック

## デプロイコマンド

gcloud functions deploy rakuten-scheduled-rename --gen2 --runtime=python311 --region=asia-northeast1 --source=. --entry-point=main --trigger-http --env-vars-file=.env.yaml
