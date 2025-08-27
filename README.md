# gBizINFO Bulk Collector

gBizINFO API を用いて法人情報を大量に収集し、詳細情報を取得するツールです。

## 特徴
- 株式会社の法人番号・社名を都道府県別または全国一括で収集（dump）
- 収集した法人番号から詳細情報を個別取得（hydrate）
- 進捗表示機能（処理速度、残り時間の表示）
- レジューム機能（中断後の処理再開）
- HTTPコネクション再利用による高速化
- 法人活動情報（exist_flg）での絞り込み対応

## インストール

```bash
git clone https://github.com/あなたのアカウント/gbizinfo-search-cli.git
cd gbizinfo-search-cli
pip install -r requirements.txt
```

## 設定

プロジェクト直下に `.env` ファイルを作成し、gBizINFO API トークンを記載してください。

```
GBIZ_API_TOKEN=your_api_token_here
```

トークンは [gBizINFO 開発者ページ](https://info.gbiz.go.jp/) から取得できます。

## 使い方

### 1. dump - 株式会社の一覧を収集

都道府県ごとまたは全国の株式会社（法人種別301）の法人番号と社名を収集します。

```bash
# 東京都の株式会社を収集
python gbiz_bulk_collector.py dump --pref 13

# 全国の株式会社を収集
python gbiz_bulk_collector.py dump --pref all

# 法人活動情報がある企業のみ収集
python gbiz_bulk_collector.py dump --pref all --exist-flg true
```

#### dumpの主なオプション

- `--pref`: 都道府県コード（01-47）または "all"（全国）
- `--out`: 出力CSVファイル（デフォルト: gbiz_list.csv）
- `--exist-flg`: 法人活動情報での絞り込み（true/false/any）
- `--resume`: 既存CSVの法人番号をスキップ（追記モード）
- `--limit`: 1ページあたりの取得件数（最大5000）

### 2. hydrate - 詳細情報を取得

dumpで収集した法人番号リストをもとに、各企業の詳細情報を取得します。

```bash
# 基本的な使い方
python gbiz_bulk_collector.py hydrate

# 進捗を10件ごとに表示
python gbiz_bulk_collector.py hydrate --progress-every 10

# 5秒ごとに進捗を表示
python gbiz_bulk_collector.py hydrate --progress-interval 5.0
```

#### hydrateの主なオプション

- `--in`: 入力CSVファイル（デフォルト: gbiz_list.csv）
- `--out`: 出力CSVファイル（デフォルト: gbiz_enriched.csv）
- `--resume`: 処理済みの法人番号をスキップ
- `--progress-every`: N件ごとに進捗表示
- `--progress-interval`: N秒ごとに進捗表示
- `--sleep`: リクエスト間のスリープ秒数

### 3. pipeline - dump→hydrateを連続実行

dumpとhydrateを連続で実行する便利なコマンドです。大量のデータを一括処理する際に推奨します。

```bash
# 全国の株式会社を収集して詳細情報を取得
python gbiz_bulk_collector.py pipeline --pref all

# 東京都の企業を収集・詳細取得（進捗を100件ごとに表示）
python gbiz_bulk_collector.py pipeline --pref 13 --progress-every 100

# 大阪府の法人活動情報がある企業のみ処理（レジューム機能付き）
python gbiz_bulk_collector.py pipeline --pref 27 --exist-flg true --resume

# カスタムファイル名で出力
python gbiz_bulk_collector.py pipeline \
  --pref 40 \
  --list-out fukuoka_list.csv \
  --enrich-out fukuoka_enriched.csv
```

#### pipelineの主なオプション

- `--pref`: 都道府県コード（01-47）または "all"（全国）
- `--list-out`: dump結果の出力ファイル（デフォルト: gbiz_list.csv）
- `--enrich-out`: hydrate結果の出力ファイル（デフォルト: gbiz_enriched.csv）
- `--exist-flg`: 法人活動情報での絞り込み（true/false/any）
- `--resume`: 既存ファイルから再開（dump/hydrate両方で有効）
- `--progress-every`: hydrate時のN件ごとの進捗表示
- `--progress-interval`: hydrate時のN秒ごとの進捗表示
- `--sleep`: 各リクエスト間のスリープ秒数

## 出力形式

### dump出力（gbiz_list.csv）

```csv
corporate_number,name
1234567890123,サンプル株式会社
2345678901234,テスト工業株式会社
```

### hydrate出力（gbiz_enriched.csv）

```csv
corporate_number,name,date_of_establishment,employee_number,capital_stock,prefecture_code,city_code,postal_code,location,company_url,business_summary
1234567890123,サンプル株式会社,2017-04-01,800,200000000,13,101,1050001,東京都港区虎ノ門一丁目23番1号,https://example.com,ソフトウェア開発
```

## 進捗表示の例

```text
[hydrate] 1250/5000 (25.0%) added=1200 err=3 rate=12.5/s ETA=0:05:00 elapsed=0:01:40
```

## 注意事項

- API制限に注意してください（デフォルトで0.2秒のスリープ）
- 大量データの処理には時間がかかります（10万件で約3時間）
- レジューム機能を使えば中断後も続きから処理できます

## 今後のアップデート予定

### 開発中の機能

#### 高優先度
- [ ] **複数法人種別の同時指定**
  - カンマ区切りで複数指定：`--corporate-type 301,305,401`
  - 全法人種別を対象：`--corporate-type all`

- [ ] **ページング設定の拡張**
  - 最大ページ数の上限解除（現在は10ページ固定）
  - ページサイズのカスタマイズ

#### 中優先度
- [ ] **統計情報・サマリー機能**
  - 都道府県別の企業数集計
  - 従業員数・資本金の分布分析
  - データ品質レポート（欠損値の割合など）

- [ ] **高度なフィルタリング**
  - 従業員数・資本金での絞り込み
  - 複数都道府県の同時指定
  - 設立年での範囲指定

- [ ] **エラー処理とログ機能**
  - 失敗した法人番号のリスト出力
  - 詳細なエラー情報のJSON保存
  - リトライ用の失敗リスト読み込み

- [ ] **出力形式の拡張**
  - JSON形式での出力対応
  - Excel形式での出力対応

- [ ] **並列処理による高速化**
  - 複数スレッドでの同時実行
  - バッチ処理の最適化

#### 低優先度
- [ ] **設定ファイル対応**
  - YAML/JSONでの設定管理
  - プロファイル機能

- [ ] **ドライラン・見積もり機能**
  - 処理時間の事前見積もり
  - APIを叩かずに動作確認

- [ ] **差分更新機能**
  - 前回取得から更新された企業のみ取得
  - 増分バックアップ

### 実装予定のコマンド例

```bash
# 複数の法人種別を指定
python gbiz_bulk_collector.py dump --corporate-type 301,305 --pref 13

# 条件付きhydrate
python gbiz_bulk_collector.py hydrate --min-employees 100 --min-capital 10000000

# 統計情報の表示
python gbiz_bulk_collector.py stats --in gbiz_enriched.csv

# JSON形式で出力
python gbiz_bulk_collector.py hydrate --format json --out companies.json

# 並列処理で高速化
python gbiz_bulk_collector.py hydrate --workers 4 --batch-size 100

# 処理時間の見積もり
python gbiz_bulk_collector.py estimate --pref all --corporate-type 301
```
