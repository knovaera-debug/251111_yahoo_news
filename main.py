import os
import re
import time
import json
import gspread
import requests
import traceback
import google.generativeai as genai
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError
from gspread.exceptions import APIError as GSpreadAPIError

# --- グローバル変数 ---
# Googleスプレッドシートのスコープと認証情報
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# 環境変数からスプレッドシートキーを取得
SPREADSHEET_KEY = os.environ.get("SPREADSHEET_KEY")
if not SPREADSHEET_KEY:
    print("❌ 環境変数 'SPREADSHEET_KEY' が設定されていません。")
    exit()

# Geminiモデルのグローバルインスタンス
gemini_model = None

# 検索キーワード
SEARCH_KEYWORDS = [
    "トヨタ", "日産", "ホンダ", "三菱自動車",
    "マツダ", "スバル", "ダイハツ", "スズキ"
]

# プロンプトのファイルパス
PROMPT_FILES = {
    "role": "prompt_gemini_role.txt",
    "sentiment": "prompt_posinega.txt",
    "category": "prompt_category.txt",
    "company_info": "prompt_target_company.txt",
    "nissan_mention": "prompt_nissan_mention.txt",
    "nissan_sentiment": "prompt_nissan_sentiment.txt",
}

# 読み込んだプロンプトを格納する辞書
PROMPTS = {}


def setup_gspread():
    """
    Google スプレッドシート API への認証を行う。
    環境変数 GCP_SERVICE_ACCOUNT_KEY から認証情報を読み込む。
    """
    try:
        # 環境変数からサービスアカウントキーのJSON文字列を取得
        creds_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        if not creds_json_str:
            print("❌ 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
            return None

        # JSON文字列を辞書に変換
        creds_dict = json.loads(creds_json_str)

        # 辞書から認証情報オブジェクトを作成
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        
        # gspread クライアントを認証
        gc = gspread.authorize(credentials)
        
        # スプレッドシートが開けるかテスト
        gc.open_by_key(SPREADSHEET_KEY)
        
        print("✅ Googleスプレッドシートへの認証に成功しました。")
        return gc

    except json.JSONDecodeError:
        print("❌ 'GCP_SERVICE_ACCOUNT_KEY' のJSON形式が正しくありません。")
        return None
    except Exception as e:
        print(f"❌ Googleスプレッドシートへの認証に失敗しました: {e}")
        return None


def get_worksheet(gc, sheet_name):
    """
    gspread クライアントとシート名を受け取り、ワークシートオブジェクトを返す。
    """
    if not gc:
        print(f"  ❌ ワークシート '{sheet_name}' を取得できません (gspreadクライアント未初期化)。")
        return None
    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_KEY)
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except GSpreadAPIError as e:
        print(f"  ❌ ワークシート '{sheet_name}' が見つからないか、アクセス権限がありません: {e}")
        return None
    except Exception as e:
        print(f"  ❌ ワークシート '{sheet_name}' の取得中に予期せぬエラー: {e}")
        return None


def load_existing_urls(ws):
    """
    SOURCE ワークシートから B 列（URL）のデータを読み込み、
    重複チェック用のセットとして返す。
    """
    try:
        # B列の全ての値を取得
        urls = ws.col_values(2) # B列は 2
        # 1行目（ヘッダー）を除く
        return set(urls[1:])
    except Exception as e:
        print(f"  ❌ 既存URLの読み込みに失敗しました: {e}")
        # 空のセットを返して処理を続行
        return set()


def get_yahoo_news_search_results(keyword):
    """
    指定されたキーワードで Yahoo!ニュースを検索し、
    記事のタイトル、URL、発行元、投稿時間のリストを返す。
    """
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(search_url, headers=headers)
        response.raise_for_status() # HTTPエラーをチェック
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 検索結果のコンテナ (新しいYahoo!ニュースの構造)
        # 'newsFeed' クラスを持つ ul 要素を探す
        search_results_container = soup.find("ul", class_="newsFeed_list")

        if not search_results_container:
            # 代替: 'div.NewsFeed' (古い構造 or 別のパターン)
            search_results_container = soup.find("div", class_="NewsFeed")

        if not search_results_container:
            print("  - 検索結果のコンテナが見つかりません (ul.newsFeed_list or div.NewsFeed)。")
            return []

        # 記事要素 (li または div)
        # 'newsFeed_item' クラスを持つ li 要素を探す
        articles = search_results_container.find_all("li", class_="newsFeed_item")

        if not articles:
            # 代替: 'div.newsFeed_item' (別のパターン)
            articles = search_results_container.find_all("div", class_="newsFeed_item")

        if not articles:
            print("  - 記事要素 (li.newsFeed_item or div.newsFeed_item) が見つかりません。")
            return []

        results = []
        for article in articles:
            try:
                # タイトルとURL
                title_tag = article.find("a", class_="newsFeed_item_link")
                if not title_tag:
                    # 代替セレクタ (例: サムネイルリンク)
                    title_tag = article.find("a", class_=re.compile(r"thumbnail_thumbnail"))
                    
                if not title_tag or "href" not in title_tag.attrs:
                    continue # タイトルタグやURLがない場合はスキップ

                url = title_tag["href"]
                
                # タイトル取得 (
                title_text_tag = article.find("div", class_="newsFeed_item_title")
                if not title_text_tag:
                     # 代替セレクタ (aタグ自身のテキスト)
                    title = title_tag.text.strip()
                else:
                    title = title_text_tag.text.strip()
                
                if not title:
                     title = "（タイトル取得失敗）"


                # URLが記事ページかチェック (https://news.yahoo.co.jp/articles/...)
                if not url.startswith("https://news.yahoo.co.jp/articles/"):
                    continue

                # 発行元 (例: 'newsFeed_item_media')
                # (注: Yahoo!のHTML構造変更により、セレクタは頻繁に変わる)
                source_tag = article.find("span", class_="newsFeed_item_media")
                if not source_tag:
                    source_tag = article.find("div", class_=re.compile(r"newsFeed_item_subMedia"))

                source = source_tag.text.strip() if source_tag else "発行元不明"

                # 投稿時間 (例: 'newsFeed_item_date')
                time_tag = article.find("time", class_="newsFeed_item_date")
                if not time_tag:
                     time_tag = article.find("div", class_=re.compile(r"newsFeed_item_date"))
                     
                post_time_str = time_tag.text.strip() if time_tag else "時間不明"

                results.append({
                    "title": title,
                    "url": url,
                    "source": source,
                    "post_time_str": post_time_str,
                    "keyword": keyword
                })

            except Exception as e:
                print(f"  - 記事パースエラー: {e}")
                continue
                
        print(f"  Yahoo!ニュース件数: {len(results)} 件取得")
        return results

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Yahoo!ニュース検索リクエスト失敗: {e}")
        return []
    except Exception as e:
        print(f"  ❌ Yahoo!ニュース検索処理エラー: {e}")
        traceback.print_exc()
        return []


def parse_relative_time(time_str):
    """
    Yahoo!ニュースの相対時間（例: '1時間前', '11/11(月) 10:00'）を
    datetime オブジェクトに変換する。
    """
    now = datetime.now()
    
    # 1. '11/11(月) 10:00' 形式 (今年)
    match = re.search(r"(\d{1,2})/(\d{1,2})\(.\) (\d{1,2}):(\d{1,2})", time_str)
    if match:
        month, day, hour, minute = map(int, match.groups())
        try:
            return now.replace(month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            # 閏年などで存在しない日付の場合、去年の日付として扱う
             return now.replace(year=now.year - 1, month=month, day=day, hour=hour, minute=minute, second=0, microsecond=0)

    # 2. '○分前' 形式
    match = re.search(r"(\d+)分前", time_str)
    if match:
        minutes = int(match.group(1))
        return now - timedelta(minutes=minutes)

    # 3. '○時間前' 形式
    match = re.search(r"(\d+)時間前", time_str)
    if match:
        hours = int(match.group(1))
        return now - timedelta(hours=hours)

    # 4. '昨日' 形式
    if "昨日" in time_str:
        match = re.search(r"(\d{1,2}):(\d{1,2})", time_str)
        day_delta = 1
        if match:
            hour, minute = map(int, match.groups())
            return (now - timedelta(days=day_delta)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            return now - timedelta(days=day_delta) # 時間不明なら0時0分

    # 5. '○日前' 形式 (7日以上前は '11/11' 形式になるはずだが念のため)
    match = re.search(r"(\d+)日前", time_str)
    if match:
        days = int(match.group(1))
        return now - timedelta(days=days)

    # 不明な形式
    return None


def get_article_details(article_url):
    """
    記事URLから本文（最大10ページ）、コメント数、正確な投稿日時を取得する。
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    article_body_parts = []
    comment_count = "0" # デフォルト
    full_post_time = None # デフォルト

    try:
        # --- 1ページ目の取得 (コメント数と日時もここから取る) ---
        response = requests.get(article_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # コメント数 (セレクタは変更の可能性大)
        comment_count_tag = soup.find("a", href=re.compile(r"/comments/"))
        if comment_count_tag:
            match = re.search(r"(\d+)", comment_count_tag.text)
            if match:
                comment_count = match.group(1)

        # 正確な投稿日時 (セレクタは変更の可能性大)
        time_tag = soup.find("time")
        if time_tag and time_tag.has_attr("datetime"):
            try:
                # ISO 8601 形式 (例: 2023-11-10T10:00:00.000Z)
                full_post_time = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
            except ValueError:
                print(f"  - 日時パース失敗: {time_tag['datetime']}")
                full_post_time = None

        # 記事本文 (1ページ目)
        # (セレクタは変更の可能性大)
        # 'article_body' or 'articleBody'
        body_container = soup.find("div", class_=re.compile(r"articleBody"))
        
        if body_container:
            # 本文テキスト
            body_text = body_container.get_text(separator="\n", strip=True)
            article_body_parts.append(body_text)
        else:
            print(f"  - 記事本文(P1)が見つかりません (URL: {article_url})")
            article_body_parts.append("（本文取得失敗）")


        # --- 2ページ目以降の取得 (最大10ページ) ---
        for page_num in range(2, 11): # 2〜10ページ
            next_page_url = f"{article_url}?page={page_num}"
            try:
                response_page = requests.get(next_page_url, headers=headers)
                
                # ページが存在しない場合 (404など)
                if response_page.status_code != 200:
                    print(f"  - 記事本文 ページ {page_num} は存在しませんでした。本文取得を完了します。")
                    break # ループ中断
                
                soup_page = BeautifulSoup(response_page.text, "html.parser")
                body_container_page = soup_page.find("div", class_=re.compile(r"articleBody"))
                
                if body_container_page:
                    body_text_page = body_container_page.get_text(separator="\n", strip=True)
                    # 1ページ目と同じ内容かチェック (ページネーションの終端判定)
                    if body_text_page == article_body_parts[0]:
                         print(f"  - 記事本文 ページ {page_num} は1ページ目と同じ内容のため終了します。")
                         break
                    
                    print(f"  - 記事本文 ページ {page_num} を取得しました。")
                    article_body_parts.append(body_text_page)
                else:
                    print(f"  - 記事本文 ページ {page_num} が見つかりませんでした。")
                    break # コンテナが見つからなければ終了
                
                time.sleep(1) # サーバー負荷軽減

            except requests.exceptions.RequestException as re_e:
                # ページネーション中のエラー (404 Client Error など)
                if "404" in str(re_e):
                    print(f"  ❌ ページなし (404 Client Error): {next_page_url}")
                    print(f"  - 記事本文 ページ {page_num} は存在しませんでした。本文取得を完了します。")
                else:
                    print(f"  ❌ ページ {page_num} 取得エラー: {re_e}")
                break # エラーが発生したら中断
            except Exception as e_page:
                print(f"  ❌ ページ {page_num} 処理エラー: {e_page}")
                break

    except requests.exceptions.RequestException as re_e:
        print(f"  ❌ 記事詳細ページ取得エラー (URL: {article_url}): {re_e}")
        return ["（本文取得失敗）"] * 10, "0", None
    except Exception as e:
        print(f"  ❌ 記事詳細処理エラー (URL: {article_url}): {e}")
        traceback.print_exc()
        return ["（本文取得失敗）"] * 10, "0", None

    # 10件に満たない場合は「-」で埋める
    if len(article_body_parts) < 10:
        article_body_parts.extend(["-"] * (10 - len(article_body_parts)))
    
    return article_body_parts[:10], comment_count, full_post_time


def load_prompts():
    """
    グローバル変数 PROMPT_FILES に基づいて、
    プロンプトファイルを読み込み、グローバル変数 PROMPTS に格納する。
    """
    global PROMPTS
    print("  プロンプトファイルを読み込んでいます...")
    try:
        for key, file_path in PROMPT_FILES.items():
            if not os.path.exists(file_path):
                print(f"  ❌ 警告: プロンプトファイル '{file_path}' が見つかりません。")
                continue
                
            with open(file_path, "r", encoding="utf-8") as f:
                PROMPTS[key] = f.read()
        
        if not PROMPTS:
             print("  ❌ エラー: 読み込めたプロンプトが1つもありません。")
             return False
             
        print("  ✅ プロンプトの読み込みが完了しました。")
        return True

    except Exception as e:
        print(f"  ❌ プロンプトファイルの読み込み中にエラー: {e}")
        return False


def initialize_gemini():
    """
    Gemini API を初期化する。
    """
    global gemini_model
    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("  ❌ 警告: 環境変数 'GOOGLE_API_KEY' が設定されていません。")
            return

        # --- (修正箇所) ---
        # 'module 'google.genai' has no attribute 'configure'' エラー対策
        # 古いバージョンのライブラリを考慮し、configure が存在するかチェック
        if hasattr(genai, "configure"):
             genai.configure(api_key=api_key)
        else:
             print("  ⚠️ 警告: genai.configure が見つかりません。APIキーの手動設定を試みます。")
             # (注: 古いバージョンではこれでは不十分かもしれないが、
             #  main.yml での --no-cache-dir --upgrade が本命の対策)
             pass # APIキーは Model() に渡す

        # モデルを初期化
        # (注: 'gemini-1.5-pro-latest' は APIキーのみでの認証 (API Key) に対応していない可能性がある)
        # (注: APIキー認証の場合は 'gemini-pro' (1.0) を使うのが確実)
        
        # 安定板の 'gemini-pro' を使用
        model = genai.GenerativeModel('gemini-pro')
        
        # (オプション: 1.5-flash を試す場合)
        # model = genai.GenerativeModel('gemini-1.5-flash-latest')

        # APIキーを渡して初期化 (genai.configureが使えない場合のフォールバック)
        if not hasattr(genai, "configure"):
            model = genai.GenerativeModel('gemini-pro', api_key=api_key)


        # (注: 'gemini-1.5-pro-latest' は 2024/11 時点で APIキー認証 (API Key) に非対応)
        # model = genai.GenerativeModel('gemini-1.5-pro-latest')

        # 疎通確認 (ダミーリクエスト)
        # model.generate_content("test", generation_config={"max_output_tokens": 1})

        gemini_model = model
        print("✅ Geminiクライアントの初期化に成功しました。 (model: gemini-pro)")

    except Exception as e:
        print(f"  ❌ 警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
        traceback.print_exc()
        gemini_model = None


def analyze_article_with_gemini(article_body):
    """
    記事本文を受け取り、Gemini API を使って
    sentiment, category, company_info, nissan_mention, nissan_sentiment を
    JSON 形式で返す。
    """
    if not gemini_model:
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }

    # 本文が長すぎる場合、先頭の10000文字程度に丸める (Geminiのコンテキスト上限対策)
    max_length = 10000
    if len(article_body) > max_length:
        article_body = article_body[:max_length]

    # JSONモードを有効にするための GenerationConfig
    # (注: gemini-pro は正式なJSONモードに非対応。1.5以降が必要)
    # (ここでは 1.0 pro を使う前提で、プロンプトでJSON出力させる)
    # generation_config = {
    #     "response_mime_type": "application/json",
    # }

    # プロンプトを組み立てる
    # 1. 役割
    # 2. 記事本文
    # 3. 各タスク (sentiment, category, company_info)
    # 4. JSON出力指示
    
    full_prompt = f"""
{PROMPTS.get("role", "あなたは業界アナリストです。")}

【記事本文】
{article_body}
【記事本文ここまで】

---
【タスク】
記事本文を分析し、以下のタスクを実行してください。
結果は必ず指定されたJSONフォーマットで、キー「sentiment」「category」「company_info」「nissan_mention」「nissan_sentiment」を持つ単一のJSONオブジェクトとして出力してください。

1. **sentimentの判定**:
{PROMPTS.get("sentiment", "（sentimentルール）")}

2. **categoryの判定**:
{PROMPTS.get("category", "（categoryルール）")}

3. **company_infoの判定**:
{PROMPTS.get("company_info", "（company_infoルール）")}

4. **nissan_mentionの判定**:
(注: company_infoが「日産」*以外*の場合のみ、本文中の「日産」への言及を確認せよ)
{PROMPTS.get("nissan_mention", "（nissan_mentionルール）")}

5. **nissan_sentimentの判定**:
(注: nissan_mentionが「-」*以外*の場合のみ、その言及が日産にとってポジティブ/ネガティブ/ニュートラルか判定せよ)
{PROMPTS.get("nissan_sentiment", "（nissan_sentimentルール）")}

---
【出力フォーマット (JSON)】
{{
  "sentiment": "（1の判定結果）",
  "category": "（2の判定結果）",
  "company_info": "（3の判定結果）",
  "nissan_mention": "（4の判定結果）",
  "nissan_sentiment": "（5の判定結果）"
}}
"""

    try:
        # print(f"  [Debug] Gemini Prompt: {full_prompt[:200]}...") # デバッグ用
        
        response = gemini_model.generate_content(full_prompt)
        
        # print(f"  [Debug] Gemini Response: {response.text}") # デバッグ用

        # Gemini 1.0 Pro は JSON "モード" に対応していないため、
        # 出力テキストから JSON 部分を抽出する
        json_match = re.search(r"\{.*\}", response.text, re.DOTALL)
        
        if not json_match:
            print("  ❌ Gemini応答からJSONを抽出できませんでした。")
            print(f"  応答: {response.text}")
            return {
                "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
                "nissan_mention": "N/A", "nissan_sentiment": "N/A"
            }

        json_str = json_match.group(0)
        
        # JSON文字列をパース
        result = json.loads(json_str)
        
        # 必要なキーが揃っているか確認
        required_keys = ["sentiment", "category", "company_info", "nissan_mention", "nissan_sentiment"]
        if not all(key in result for key in required_keys):
             print(f"  ❌ Gemini応答JSONに必要なキーが不足しています。 {result.keys()}")
             # 不足しているキーを 'N/A' で補完
             for key in required_keys:
                 if key not in result:
                     result[key] = "N/A (キー欠損)"

        return result

    except json.JSONDecodeError as e:
        print(f"  ❌ Gemini応答のJSONパースに失敗しました: {e}")
        print(f"  応答テキスト (JSON抽出後): {json_str}")
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }
    except GoogleAPIError as e:
        print(f"  ❌ Gemini API エラー: {e}")
        # (例: クォータ超過、認証エラーなど)
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }
    except Exception as e:
        print(f"  ❌ Gemini分析中に予期せぬエラー: {e}")
        traceback.print_exc()
        return {
            "sentiment": "N/A", "category": "N/A", "company_info": "N/A",
            "nissan_mention": "N/A", "nissan_sentiment": "N/A"
        }


# --- (修正箇所) ---
# 問題(3)対策：コメントURLの形式変更に伴い、
# get_yahoo_news_comments 関数の引数に article_url を追加
def get_yahoo_news_comments(article_id, article_url):
    """
    記事IDと記事URLを受け取り、コメントページの1〜3ページ目までをスクレイピングする。
    """
    print(f"    - コメント本文 (S列～AC列) を取得中...")
    comments_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # --- (修正箇所) ---
        # 問題(3)対策：コメントURLの形式を古いものから新しいものへ変更
        # 旧: f"https://news.yahoo.co.jp/comments/{article_id}?page={page_num}"
        # 新: f"https://news.yahoo.co.jp/articles/{article_id}/comments?page={page_num}"
        # 記事URL全体 (article_url) を使うように変更します。
        
        base_comments_url = f"{article_url}/comments"
        
        for page_num in range(1, 4): # 1ページから3ページまで
            if page_num == 1:
                comments_url = base_comments_url
            else:
                comments_url = f"{base_comments_url}?page={page_num}"

            # print(f"      - コメント ページ {page_num} ( {comments_url} ) を取得中...") # デバッグ用
            response = requests.get(comments_url, headers=headers)
            
            # ページが存在しない場合 (404エラーなど)
            if response.status_code != 200:
                print(f"    ❌ コメント ページ {page_num} ( {comments_url} ) が存在しないか取得失敗。ステータス: {response.status_code}")
                break # 存在しない場合は以降のページのチェックを中断

            # --- (修正ここまで) ---

            soup = BeautifulSoup(response.text, "html.parser")

            # コメントのコンテナを探す (Yahoo!のHTML構造に依存)
            # (注: このセレクタ 'div.comment-list-item' も変更されている可能性があります)
            # (Yahoo!のクラス名は難読化されていることが多い)
            
            # 難読化されたクラス名に対応するため、部分一致 (class*='...') を使用
            comments = soup.select("div[class*='CommentItem__Container']") # 新しい可能性のあるセレクタ
            
            if not comments:
                 # 以前のセレクタも試す (保険)
                comments = soup.select("div.comment-list-item") # 元のセレクタ
            
            if not comments:
                # print(f"    - コメント ページ {page_num} にコメントが見つかりませんでした。") # デバッグ用
                break # ページはあるがコメントが無い場合も中断

            for comment in comments:
                # ユーザー名
                user_name_tag = comment.select_one("h3[class*='CommentItem__UserName']")
                user_name = user_name_tag.text.strip() if user_name_tag else "ユーザー名不明"

                # コメント本文
                comment_text_tag = comment.select_one("p[class*='CommentItem__Text']")
                comment_text = comment_text_tag.text.strip() if comment_text_tag else "コメント本文なし"

                comments_data.append(f"【{user_name}】{comment_text}")

                if len(comments_data) >= 10: # 10件取得したら終了
                    break
            
            if len(comments_data) >= 10:
                break
            
            time.sleep(1) # 1秒待機

        # 10件に満たない場合は「-」で埋める
        if not comments_data:
            print(f"    - コメントが1件も見つかりませんでした（またはコメント欄閉鎖）。")
            return ["取得不可"] * 10

        if len(comments_data) < 10:
            comments_data.extend(["-"] * (10 - len(comments_data)))

        print(f"    ✅ コメント {len(comments_data)} 件を取得しました。")
        return comments_data[:10]

    except Exception as e:
        print(f"    ❌ コメント取得エラー: {e}")
        traceback.print_exc()
        return ["取得不可"] * 10


def update_source_sheet(ws, new_articles, existing_urls):
    """
    SOURCE ワークシートを更新する。
    1. 新しい記事をフィルタリング
    2. 新しい記事をシートに追加 (A-E列)
    3. analysis_flag が "TRUE" かつ 本文が空の記事 (F-AC列) を更新
    """
    
    # --- 1. 新しい記事をフィルタリング ---
    articles_to_add = []
    for article in new_articles:
        if article["url"] not in existing_urls:
            
            # 投稿日時をパース
            post_time = parse_relative_time(article["post_time_str"])
            if post_time:
                # Google Sheets が認識できる形式にフォーマット
                post_time_formatted = post_time.strftime("%Y/%m/%d %H:%M:%S")
            else:
                post_time_formatted = article["post_time_str"] # パース失敗時はそのまま

            # A列: 検索キーワード, B列: URL, C列: 投稿日時, D列: 発行元, E列: タイトル
            row_data = [
                article["keyword"],
                article["url"],
                post_time_formatted,
                article["source"],
                article["title"],
                "TRUE" # F列: analysis_flag (新規追加時はデフォルトTRUE)
            ]
            articles_to_add.append(row_data)
            
            # メモリ上の existing_urls にも追加 (重複追加防止)
            existing_urls.add(article["url"])

    # --- 2. 新しい記事をシートに追加 ---
    if articles_to_add:
        try:
            # 既存データの最終行の次に追加
            ws.append_rows(articles_to_add, value_input_option="USER_ENTERED")
            print(f"  ✅ {len(articles_to_add)} 件の新しい記事を SOURCEシート に追加しました。")
        except Exception as e:
            print(f"  ❌ 新規記事のスプレッドシートへの書き込みに失敗しました: {e}")
            # ここで失敗しても、次のステップ（本文取得）は試みる
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")


    # --- 3. 本文・コメント等が未取得の記事を更新 ---
    try:
        # A列からAC列 (1~29列) までの全データを取得
        print("  ... 本文・コメント未取得のデータをスプレッドシートから読み込み中 ...")
        all_data = ws.get_all_values()
        if len(all_data) <= 1:
            print("  - データがありません。")
            return # ヘッダーのみ

        headers = all_data[0]
        data_rows = all_data[1:]
        
        # 列インデックスの特定 (0始まり)
        try:
            url_col = headers.index("URL") # B列
            flag_col = headers.index("analysis_flag") # F列
            body_p1_col = headers.index("body_p1") # G列
            comment_count_col = headers.index("comment_count") # Q列
            full_time_col = headers.index("full_post_time") # R列
            comment_1_col = headers.index("comment_1") # S列
        except ValueError as e:
            print(f"  ❌ 必要な列が見つかりません: {e}。本文取得をスキップします。")
            return

        # 更新データを溜め込むリスト (gspread バッチ更新用)
        batch_update_data = []

        # 2行目から (インデックス 0 = 2行目)
        for i, row in enumerate(data_rows):
            row_index = i + 2 # 実際のシート上の行番号
            
            # 行が短すぎる場合 (途中の空行など) はスキップ
            if len(row) <= max(flag_col, body_p1_col, url_col):
                continue
            
            analysis_flag = row[flag_col]
            body_p1 = row[body_p1_col]
            
            # (analysis_flagがTRUE かつ body_p1が空 または '（本文取得失敗）') の場合に実行
            if (analysis_flag.upper() == "TRUE" or analysis_flag == "1") and \
               (not body_p1 or body_p1 == "（本文取得失敗）"):
                
                print(f"  - 行 {row_index} (記事: {row[4][:30]}...): 本文(P1-P10)/コメント数/日時補完/コメント本文 を取得中... (完全取得)")
                
                article_url = row[url_col]
                # 記事IDをURLから抽出
                article_id_match = re.search(r"/articles/([a-f0-9]+)", article_url)
                if not article_id_match:
                    print(f"    - URLから記事IDが抽出できませんでした: {article_url}")
                    continue
                
                article_id = article_id_match.group(1)

                # 詳細を取得
                article_body_parts, comment_count, full_post_time = get_article_details(article_url)
                
                # --- (修正箇所) ---
                # 問題(3)対策：get_yahoo_news_comments に article_url を渡す
                comments_data = get_yahoo_news_comments(article_id, article_url)
                # --- (修正ここまで) ---
                
                # 更新用データリストを作成
                update_row_data = []
                update_row_data.extend(article_body_parts) # G-P列 (10列)
                update_row_data.append(comment_count) # Q列
                
                # R列 (full_post_time)
                if full_post_time:
                    # 'YYYY/MM/DD HH:MM:SS' 形式に
                    jst = full_post_time.astimezone(timedelta(hours=9))
                    update_row_data.append(jst.strftime("%Y/%m/%d %H:%M:%S"))
                else:
                    update_row_data.append("-") # 取得失敗時はハイフン

                update_row_data.extend(comments_data) # S-AC列 (10列)
                
                # 更新範囲 (G列 から AC列 まで)
                start_col_letter = gspread.utils.rowcol_to_a1(row_index, body_p1_col + 1)[0]
                end_col_letter = gspread.utils.rowcol_to_a1(row_index, comment_1_col + 9)
                end_col_letter = ''.join([c for c in end_col_letter if not c.isdigit()]) # 'AC' など

                range_to_update = f"{start_col_letter}{row_index}:{end_col_letter}{row_index}"
                
                batch_update_data.append({
                    'range': range_to_update,
                    'values': [update_row_data]
                })

                # サーバー負荷軽減のため 3秒待機
                time.sleep(3)
        
        # --- 4. 溜め込んだ更新を一括実行 ---
        if batch_update_data:
            print(f"  ... {len(batch_update_data)} 件の本文/コメントデータをスプレッドシートに一括書き込み中 ...")
            ws.batch_update(batch_update_data, value_input_option="USER_ENTERED")
            print("  ✅ 本文/コメントデータの一括書き込みが完了しました。")

    except Exception as e:
        print(f"  ❌ 本文・コメント取得・書き込み処理中にエラー: {e}")
        traceback.print_exc()


def sort_and_format_sheet(gc):
    """
    SOURCE ワークシートの C列 (投稿日時) の書式を整え、
    シート全体を C列 の降順 (新しい順) でソートする。
    """
    print("\n===== 📑 ステップ③ 記事データのソートと整形 =====")
    ws = get_worksheet(gc, "SOURCE")
    if not ws:
        return

    try:
        # C列 (投稿日時) の書式を 'yyyy/mm/dd hh:mm:ss' に設定
        # (注: gspread v6 では set_basic_filter がない場合がある)
        
        # A1表記で行数・列数を取得
        end_cell = gspread.utils.rowcol_to_a1(ws.row_count, ws.col_count)
        
        # C列全体の書式設定リクエスト (C2からC列最後まで)
        format_request = {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1,  # 2行目から (0-indexed)
                    "endRowIndex": ws.row_count,
                    "startColumnIndex": 2, # C列 (0-indexed)
                    "endColumnIndex": 3
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE_TIME",
                            "pattern": "yyyy/mm/dd hh:mm:ss"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        }

        # ソートリクエスト (C列=列インデックス2 で降順ソート)
        sort_request = {
            "sortRange": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 1, # 2行目から (ヘッダー除く)
                    "endRowIndex": ws.row_count,
                    "startColumnIndex": 0, # A列から
                    "endColumnIndex": ws.col_count
                },
                "sortSpecs": [
                    {
                        "dimensionIndex": 2, # C列 (0-indexed)
                        "sortOrder": "DESCENDING"
                    }
                ]
            }
        }
        
        # (gspread v5.x 以前の方法: C列の曜日 (月) などを除去)
        # (これは API v4 では不要。書式設定で対応)
        print(" スプレッドシート上でC列の書式設定とソートを実行します。")
        
        # バッチアップデートで書式設定とソートを同時に実行
        ws.spreadsheet.batch_update({
            "requests": [format_request, sort_request]
        })
        
        print(f" ✅ C列(2行目〜{ws.row_count}行) の表示形式を 'yyyy/mm/dd hh:mm:ss' に設定しました。")
        print(" ✅ SOURCEシートを投稿日時の新しい順にスプレッドシート上で並び替えました。")

    except Exception as e:
        print(f"  ❌ ソート・書式設定中にエラー: {e}")
        print("  (注: Google Sheets API v4 が必要です)")


# --- (修正箇所) ---
# 問題(1)対策：gspread API の 429 エラー (Quota Exceeded) を回避するため、
# 1件ずつ ws.update するのではなく、ws.batch_update で一括書き込みする
def analyze_with_gemini_and_update_sheet(gc):
    """
    スプレッドシートの「分析フラグ」が立っている記事（最大30件）をGeminiで分析し、
    結果をP-R列 (sentiment, category, company_info) と
    AD-AE列 (nissan_mention, nissan_sentiment) に一括で書き込む。
    """
    try:
        if not gemini_model:
            print("\n===== 🧠 ステップ④ (スキップ) =====")
            print("  Geminiモデルが初期化されていないため、分析をスキップします。")
            return

        print("\n===== 🧠 ステップ④ Gemini分析の実行・即時反映 (P-R, AD-AE列) [最大30件] =====")
        ws = get_worksheet(gc, "SOURCE")
        if not ws:
            return

        # ヘッダー行を取得して、列インデックスを動的に見つける
        headers = ws.row_values(1)
        try:
            # 必要な列のインデックス（0始まり）を取得
            title_col_idx = headers.index("title") + 1 # E列
            analysis_flag_col_idx = headers.index("analysis_flag") + 1 # F列
            body_col_idx = headers.index("body_p1") + 1 # G列
            sentiment_col_idx = headers.index("sentiment") + 1 # P列
            category_col_idx = headers.index("category") + 1 # Q列
            company_info_col_idx = headers.index("company_info") + 1 # R列
            nissan_mention_col_idx = headers.index("nissan_mention") + 1 # AD列
            nissan_sentiment_col_idx = headers.index("nissan_sentiment") + 1 # AE列

        except ValueError as e:
            print(f"  ❌ 必要な列が見つかりません: {e}。分析を中断します。")
            return

        # A列からAE列までの全データを一度に取得
        print("  ... 分析対象データをスプレッドシートから読み込み中 ...")
        all_data = ws.get_all_values()
        if len(all_data) <= 1:
            print("  分析対象データがありません。")
            return

        # ヘッダー行を除いたデータ本体
        data_rows = all_data[1:]
        
        # --- (修正箇所) ---
        # 問題(1)対策：書き込みデータを一時的に溜め込むリストを初期化
        batch_updates = []
        # --- (修正ここまで) ---

        count = 0
        max_analyze = 30 # 最大分析件数

        # 2行目から (インデックス0 = 2行目)
        for i, row in enumerate(data_rows):
            row_index = i + 2 # 実際のシート上の行番号
            
            # 行データが短い場合はスキップ
            if len(row) <= max(analysis_flag_col_idx-1, sentiment_col_idx-1, body_col_idx-1):
                continue

            try:
                # 分析フラグ (analysis_flag_col_idx-1) と 分析結果 (sentiment_col_idx-1) をチェック
                analysis_flag = row[analysis_flag_col_idx - 1]
                sentiment = row[sentiment_col_idx - 1]
                
                # 分析フラグが 'TRUE' (または '1') で、かつ sentiment が空 (または 'N/A') の場合のみ実行
                if (analysis_flag.upper() == "TRUE" or analysis_flag == "1") and (not sentiment or sentiment == "N/A"):
                    
                    if count >= max_analyze:
                        print(f"  分析件数が{max_analyze}件に達したため、残りは次回に回します。")
                        break
                    
                    count += 1
                    title = row[title_col_idx - 1][:30] # タイトル列
                    print(f"  - 行 {row_index} (記事: {title}...): Gemini分析を実行中... ({count}/{max_analyze}件目)")

                    # 本文 (G列からP列の直前まで)
                    body_p1_to_p10 = row[body_col_idx - 1 : body_col_idx + 9]
                    article_body = " ".join([text for text in body_p1_to_p10 if text and text != "-"])
                    
                    if len(article_body.strip()) < 50: # 本文が短すぎる場合はスキップ
                        print(f"    ...本文が短すぎるためスキップ (本文: {article_body[:50]}...)")
                        # スキップした場合でも、フラグを 'FALSE' にして次回以降の無駄なチェックを防ぐ
                        analysis_result = {
                            "sentiment": "N/A (本文短)", "category": "N/A", "company_info": "N/A",
                            "nissan_mention": "-", "nissan_sentiment": "-"
                        }
                    else:
                        # Gemini APIに分析をリクエスト
                        analysis_result = analyze_article_with_gemini(article_body)
                    
                    # 分析結果を各変数に格納
                    sentiment = analysis_result.get("sentiment", "N/A")
                    category = analysis_result.get("category", "N/A")
                    company_info = analysis_result.get("company_info", "N/A")
                    nissan_mention = analysis_result.get("nissan_mention", "N/A")
                    nissan_sentiment = analysis_result.get("nissan_sentiment", "N/A")

                    # --- (修正箇所) ---
                    # 問題(1)対策：ws.update() を呼び出す代わりに、batch_updatesリストにデータを追加する
                    
                    # メインの分析結果 (P列〜R列)
                    batch_updates.append({
                        'range': f"{gspread.utils.rowcol_to_a1(row_index, sentiment_col_idx)}:{gspread.utils.rowcol_to_a1(row_index, company_info_col_idx)}",
                        'values': [[sentiment, category, company_info]]
                    })
                    
                    # 日産関連の分析結果 (AD列〜AE列)
                    batch_updates.append({
                        'range': f"{gspread.utils.rowcol_to_a1(row_index, nissan_mention_col_idx)}:{gspread.utils.rowcol_to_a1(row_index, nissan_sentiment_col_idx)}",
                        'values': [[nissan_mention, nissan_sentiment]]
                    })
                    
                    # (注: 分析フラグ F列 を 'FALSE' にする処理はここには無い)
                    # (もし 'FALSE' にしたいなら、ここにもう1つ append を追加する必要がある)
                    
                    # --- (修正ここまで) ---

                    time.sleep(1) # APIリクエストの間に短い待機 (Gemini APIのレート制限対策)

            except Exception as e:
                print(f"  ❌ 行 {row_index} の処理中にエラー: {e}")
                traceback.print_exc()

        # --- (修正箇所) ---
        # 問題(1)対策：ループが全て終わった後、溜め込んだデータを一括で書き込む
        if batch_updates:
            print(f"  ... {len(batch_updates) // 2} 件の分析結果をスプレッドシートに一括書き込み中 ...")
            try:
                ws.batch_update(batch_updates, value_input_option="USER_ENTERED")
                print("  ✅ 分析結果の一括書き込みが完了しました。")
            except Exception as e:
                print(f"  ❌ スプレッドシートへの一括書き込みに失敗しました: {e}")
                # (ここで 429 Quota Exceeded が出る場合は、batch_updates の量が多すぎる可能性)
                traceback.print_exc()
        elif count == 0:
            print("  分析対象（分析フラグがTRUEで未分析）の記事はありませんでした。")
        # --- (修正ここまで) ---

    except Exception as e:
        print(f"  ❌ Gemini分析ステップ全体でエラー: {e}")
        traceback.print_exc()


def main():
    """
    メイン処理
    """
    print("--- 統合スクリプト開始 ---")
    start_time = time.time()
    
    # --- セットアップ ---
    gc = setup_gspread()
    if not gc:
        print("スプレッドシート認証に失敗。処理を終了します。")
        return

    ws = get_worksheet(gc, "SOURCE")
    if not ws:
        print("SOURCE ワークシートの取得に失敗。処理を終了します。")
        return
        
    if not load_prompts():
        print("プロンプト読み込みに失敗。Gemini分析は実行されません。")
        # (処理は続行)

    initialize_gemini() # Gemini APIの初期化

    # --- ステップ① ニュースリスト取得 & ステップ② 本文・コメント取得 ---
    existing_urls = load_existing_urls(ws)
    print(f"  (現在 {len(existing_urls)} 件の記事URLをロード済み)")
    
    all_new_articles = []
    for keyword in SEARCH_KEYWORDS:
        print(f"\n===== 🔑 ステップ① ニュースリスト取得: {keyword} =====")
        new_articles = get_yahoo_news_search_results(keyword)
        
        # ステップ② (更新処理)
        # (注: 本来はステップ①を全キーワード分やってから②をやるべきだが、
        #  元のコード ではキーワードごとに②を実行していたため、それを踏襲)
        
        # (元のコード のロジックに従い、
        #  キーワードごとに新規追加と、全データの本文取得チェックを実行)
        print(f"\n===== 📝 ステップ② 本文/コメント更新 (キーワード: {keyword} 追加後) =====")
        update_source_sheet(ws, new_articles, existing_urls)
        
        # (注: この設計だと、キーワード「トヨタ」の実行時に、
        #  「日産」の古い未取得データも取得しにいく。
        #  キーワード「日産」の実行時にも、再度「トヨタ」の未取得データもチェックしにいく。
        #  非効率だが、元の設計 を維持する)


    # --- ステップ③ ソート & 書式設定 ---
    # (全キーワードの処理が終わった後に1回だけ実行)
    sort_and_format_sheet(gc)

    # --- ステップ④ Gemini 分析 ---
    # (全キーワードの処理が終わった後に1回だけ実行)
    analyze_with_gemini_and_update_sheet(gc)

    end_time = time.time()
    print(f"\n--- 統合スクリプト終了 (所要時間: {end_time - start_time:.2f}秒) ---")


if __name__ == "__main__":
    main()
