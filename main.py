# -*- coding: utf-8 -*-
"""
統合スクリプト（国内8社対応版） - 最終設定バージョン：
1. keywords.txtから全メーカーを読み込み、順次Yahooシートに記事リストを追記 (A-D列)。
2. 投稿日時から曜日を確実に削除し、クリーンな形式で格納。
3. 本文とコメント数を取得し、行ごとにスプレッドシートに即時反映。
    -> 【改修】記事本文は1ページ1セルでE-N列に格納。
    -> 【改修】コメント数はO列に格納。
    -> 【改修】コメント本文は1ページ1セルでS-AC列に格納。
4. 全記事を投稿日の新しい順に並び替え (C列基準)。
5. ソートされた記事に対し、新しいものからGemini分析 (P-R列, AD-AE列) を実行。
    -> 【改修】API消費量対策のため、1回の実行で分析する件数を制限。
"""

import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any
import sys
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode # 追加

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Gemini API 関連のインポート ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted
# ------------------------------------

# ====== 設定 ======
# ▼▼▼ ユーザー指示により修正 ▼▼▼
SHARED_SPREADSHEET_ID = "1FlQmR1Xe25wCLi-zt-lnigDoh8Qz8RUeJbWZifuDW84"
# ▲▲▲ ユーザー指示により修正 ▲▲▲
KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
# 曜日削除の対象とする最大行数を10000に設定
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 10 # 記事本文取得の最大巡回ページ数 (※ロジック改修により現在は1ページのみ取得)

# ▼▼▼【変更】 E列を「本文P1」～「本文P10」の10列に変更し、以降の列をすべてシフト (全31列) ▼▼▼
YAHOO_SHEET_HEADERS = [
    # 基本情報 (A-D)
    "URL", "タイトル", "投稿日時", "ソース", 
    # 記事本文 (E-N)
    "本文P1", "本文P2", "本文P3", "本文P4", "本文P5", 
    "本文P6", "本文P7", "本文P8", "本文P9", "本文P10",
    # コメント数 (O)
    "コメント数",
    # 基本AI分析 (P-R)
    "対象企業", "カテゴリ分類", "ポジネガ分類",
    # コメント本文 (S-AC)
    "コメントP1", "コメントP2", "コメントP3", "コメントP4", "コメントP5", 
    "コメントP6", "コメントP7", "コメントP8", "コメントP9", "コメントP10", 
    "コメントP11(以降)",
    # 日産関連AI分析 (AD-AE)
    "日産関連言及", "日産視点ポジネガ"
]
# ▲▲▲【変更】 E列を「本文P1」～「本文P10」の10列に変更し、以降の列をすべてシフト (全31列) ▲▲▲

REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# ▼▼▼【修正】 日産関連のプロンプトを追加 ▼▼▼
PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt",
    "prompt_nissan_mention.txt",    # <-- 追加
    "prompt_nissan_sentiment.txt"   # <-- 追加
]
# ▲▲▲【修正】 日産関連のプロンプトを追加 ▲▲▲

try:
    # ▼▼▼ ユーザー指示により修正 (APIキー設定を追加) ▼▼▼
    # APIキーをここで設定
    genai.configure(api_key="AIzaSyCwNV4NgFl1-9yxEXqr-QJs-F7X4QjmyNQ")
    
    GEMINI_CLIENT = genai.Client()
    # ▲▲▲ ユーザー指示により修正 ▲▲▲
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

GEMINI_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

# 【修正点】gspread.utils.col_to_letter の代替関数を定義
def gspread_util_col_to_letter(col_index: int) -> str:
    """ gspreadの古いバージョンで col_to_letter がない場合の代替関数 (1-indexed) """
    if col_index < 1:
        raise ValueError("Column index must be 1 or greater")
    
    # gspread.utils.rowcol_to_a1(1, col_index) を利用してA1表記を取得し、行番号を削除して列文字のみを抽出
    a1_notation = gspread.utils.rowcol_to_a1(1, col_index)
    return re.sub(r'\d+', '', a1_notation)

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    # 【修正点①】日時の表示形式を yyyy/mm/dd hh:mm:ss に変更
    return dt_obj.strftime("%Y/%m/%d %H:%M:%S") # 2025/10/08 10:00:28 の形式

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        
        # 曜日のパターンを削除する正規表現を確実に実行
        s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
        
        # 配信という文字が残っている場合は削除
        s = s.replace('配信', '').strip()
        
        # 修正後のフォーマットを含めてパースを試みる
        for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    # 年がない形式の場合、今年を適用
                    dt = dt.replace(year=today_jst.year)
                
                # 年が未来（現在月の翌月以降）であれば、前年に修正する (月日のみの形式を考慮)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31):
                    dt = dt.replace(year=dt.year - 1)
                    
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def build_gspread_client() -> gspread.Client:
    try:
        # 環境変数 GCP_SERVICE_ACCOUNT_KEY から認証情報を読み込む
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            # GCP_SERVICE_ACCOUNT_KEY が設定されていない場合は、ローカルファイル認証を試みる (フォールバック)
            try:
                return gspread.service_account(filename='credentials.json')
            except FileNotFoundError:
                raise RuntimeError("Google認証情報 (GCP_SERVICE_ACCOUNT_KEY)が環境変数、または 'credentials.json' ファイルに見つかりません。")

    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        if not keywords:
            raise ValueError("キーワードファイルに有効なキーワードが含まれていません。")
        return keywords
    except FileNotFoundError:
        print(f"致命的エラー: キーワードファイル '{filename}' が見つかりません。")
        return []
    except Exception as e:
        print(f"キーワードファイルの読み込みエラー: {e}")
        return []

def load_gemini_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
        
    combined_instructions = []
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_instruction = ""

        role_file = PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        
        for filename in PROMPT_FILES[1:]:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content:
                combined_instructions.append(content)
                        
        if not role_instruction or not combined_instructions:
            print("致命的エラー: プロンプトファイルの内容が不完全または空です。")
            return ""

        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"

        GEMINI_PROMPT_TEMPLATE = base_prompt
        print(f" Geminiプロンプトテンプレートを {PROMPT_FILES} から読み込み、結合しました。")
        return base_prompt
        
    except FileNotFoundError as e:
        print(f"致命的エラー: プロンプトファイルの一部が見つかりません。ファイル名: {e.filename}")
        return ""
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラーが発生しました: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    """ 記事本文取得用のリトライ付きリクエストヘルパー """
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            
            # 💡 改修点②: 404 Client Error の場合、リトライせず None を返して即座にスキップ
            if res.status_code == 404:
                print(f"  ❌ ページなし (404 Client Error): {url}")
                return None
                
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ 接続エラー、リトライ中... ({attempt + 1}/{max_retries})。待機: {wait_time:.2f}秒")
                time.sleep(wait_time)
            else:
                print(f"  ❌ 最終リトライ失敗: {e}")
                return None
    return None

# ====== Gemini 分析関数 (【修正】日産関連の分析キーを追加) ======
def analyze_with_gemini(text_to_analyze: str) -> Tuple[str, str, str, str, str, bool]:
    """
    【修正】
    - 戻り値に nissan_mention, nissan_sentiment を追加。
    - response_schema に新しいキーを追加。
    """
    if not GEMINI_CLIENT:
        return "N/A", "N/A", "N/A", "N/A", "N/A", False
        
    if not text_to_analyze.strip():
        return "N/A", "N/A", "N/A", "N/A", "N/A", False

    prompt_template = load_gemini_prompt()
    if not prompt_template:
        return "ERROR(Prompt Missing)", "ERROR", "ERROR", "ERROR", "ERROR", False

    MAX_RETRIES = 3
    MAX_CHARACTERS = 15000
    
    for attempt in range(MAX_RETRIES):
        try:
            text_for_prompt = text_to_analyze[:MAX_CHARACTERS]
            prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text_for_prompt)
            
            response = GEMINI_CLIENT.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    # ▼▼▼【修正】 スキーマに5つのキーを定義 ▼▼▼
                    response_schema={"type": "object", "properties": {
                        "company_info": {"type": "string", "description": "記事の主題企業名と（）内に共同開発企業名を記載した結果"},
                        "category": {"type": "string", "description": "企業、モデル、技術などの分類結果"},
                        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブのいずれか"},
                        "nissan_mention": {"type": "string", "description": "日産関連の言及（あれば）"},
                        "nissan_sentiment": {"type": "string", "description": "日産視点でのポジネガ判定（あれば）"}
                    }}
                    # ▲▲▲【修正】 スキーマに5つのキーを定義 ▲▲▲
                ),
            )

            analysis = json.loads(response.text.strip())
            
            # ▼▼▼【修正】 5つのキーを取得 ▼▼▼
            company_info = analysis.get("company_info", "N/A")
            category = analysis.get("category", "N/A")
            sentiment = analysis.get("sentiment", "N/A")
            nissan_mention = analysis.get("nissan_mention", "N/A")
            nissan_sentiment = analysis.get("nissan_sentiment", "N/A")

            return company_info, category, sentiment, nissan_mention, nissan_sentiment, False
            # ▲▲▲【修正】 5つのキーを取得 ▲▲▲

        # クォータ制限エラーを最優先で捕捉し、強制終了
        except ResourceExhausted as e:
            print(f"  🚨 Gemini API クォータ制限エラー (429): {e}")
            print("\n===== 🛑 クォータ制限を検出したため、システムを直ちに中断します。 =====")
            sys.stdout.flush()
            sys.exit(1) # プロセス全体を終了

        # クォータ以外の一般的なエラーのみリトライ対象とする
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ Gemini API 一時的な接続または処理エラー。{wait_time:.2f} 秒待機してリトライします (試行 {attempt + 1}/{MAX_RETRIES})。")
                time.sleep(wait_time)
                continue
            else:
                print(f"Gemini分析エラー: {e}")
                return "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", False
        
    return "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", False

# ====== データ取得関数 (ソース抽出ロジック修正) ======

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f" WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
    
    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(3)
    except Exception as e:
        print(f"  ⚠️ ページロードまたは要素検索でタイムアウト。エラー: {e}")
        time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    # 記事リストの親要素を特定 (セレクタは適宜調整が必要になる場合がある)
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    
    articles_data = []
    today_jst = jst_now()
    
    for article in articles:
        try:
            # A. タイトル
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            # B. URL
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            # C. 投稿日時 (C列) 抽出
            date_str = ""
            time_tag = article.find("time")
            if time_tag:
                date_str = time_tag.text.strip()
            
            # D. ソース (D列) 抽出ロジックの改善
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            
            if source_container:
                # タイムスタンプやコメント数の後に続く最初のテキストを探す
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                
                if time_and_comments:
                    # div内の全てのテキストノードを取得し、日付やコメントの要素のテキストを除去
                    source_candidates = [
                        span.text.strip() for span in time_and_comments.find_all("span")
                        if not span.find("svg") # コメントアイコンではない
                        and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', span.text.strip()) # 日付ではない
                    ]
                    # 最も長い（ソースである可能性が高い）テキストを採用
                    if source_candidates:
                        source_text = max(source_candidates, key=len)
                        
                    # 上記で取得できない場合、直下のテキストノードを探す
                    if not source_text:
                        for content in time_and_comments.contents:
                            if content.name is None and content.strip() and not re.match(r'\d{1,2}/\d{1,2}\([月火水木金土日]\)\d{1,2}:\d{2}', content.strip()):
                                source_text = content.strip()
                                break
                    
            if title and url:
                formatted_date = ""
                if date_str:
                    try:
                        # 取得した生の日付文字列から日付オブジェクトを作成
                        dt_obj = parse_post_date(date_str, today_jst)
                        
                        if dt_obj:
                            # 修正した format_datetime を使用し、yyyy/mm/dd hh:mm:ss 形式で格納
                            formatted_date = format_datetime(dt_obj)
                        else:
                            # パース失敗時は曜日だけ削除した生文字列をそのまま保持
                            formatted_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
                    except:
                        formatted_date = date_str

                articles_data.append({
                    "URL": url,
                    "タイトル": title,
                    "投稿日時": formatted_date if formatted_date else "取得不可",
                    "ソース": source_text if source_text else "取得不可"
                })
        except Exception as e:
            continue
            
    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

# ====== 詳細取得関数 (【修正】「1ページ1セル」のリストを返す) ======
def fetch_article_body_and_comments(base_url: str) -> Tuple[List[str], int, Optional[str]]:
    """
    【修正】
    複数ページ巡回ロジックを使い、本文を「1ページ1セル」のリスト (10要素) として返す。
    Returns:
        Tuple[List[str], int, Optional[str]]: 
            ( [本文P1, 本文P2, ...], コメント数, 日付文字列 )
    """
    comment_count = -1 # コメント数は1ページ目でのみ取得
    extracted_date_str = None # 日時も1ページ目でのみ取得
    
    # 戻り値用のリスト (10列分)
    MAX_BODY_PAGES = 10
    body_pages_list = []
    
    # URLから記事IDを取得
    article_id_match = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not article_id_match:
        print(f"  ❌ URLから記事IDが抽出できませんでした: {base_url}")
        return ["本文取得不可"] * MAX_BODY_PAGES, -1, None
    
    # ベースURLから ? 以降を削除
    clean_base_url = base_url.split('?')[0]
    
    # 最大10ページのループ (1から10まで)
    for page_num in range(1, MAX_BODY_PAGES + 1):
        
        # 1ページ目のみURLが異なる
        if page_num == 1:
            current_url = clean_base_url
        else:
            current_url = f"{clean_base_url}?page={page_num}"
            
        # 2. HTML取得
        response = request_with_retry(current_url) # 既存のリトライ関数 を使用
        
        # ページが存在しない (404など) or 取得失敗
        if not response:
            if page_num == 1:
                # 1ページ目から失敗したら即終了
                print(f"  ❌ 記事本文(1ページ目)の取得に失敗。: {current_url}")
                body_pages_list.append("本文取得不可")
            else:
                # 2ページ目以降の失敗は、それが最終ページだったということ
                print(f"  - 記事本文 ページ {page_num} は存在しませんでした。本文取得を完了します。")
            break # ループを抜ける
        
        print(f"  - 記事本文 ページ {page_num} を取得しました。")
        soup = BeautifulSoup(response.text, 'html.parser')

        # 3. 記事本文の抽出
        article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))
        
        current_page_body_parts = []
        if article_content:
            # 最新のHTML構造に対応したセレクタ
            paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget'))
            if not paragraphs: # 上記セレクタで取得できなければ汎用<p>を試す
                paragraphs = article_content.find_all('p')
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    current_page_body_parts.append(text)
        
        # 2ページ目以降で本文が取れなかった場合、そこで終了
        if not current_page_body_parts and page_num > 1:
             print(f"  - 記事本文 ページ {page_num} から本文を抽出できませんでした。取得を完了します。")
             break
        
        # 取得した本文を結合して、1セル（1ページ）分のデータにする
        current_page_body_text = "\n".join(current_page_body_parts)
        
        if not current_page_body_text and page_num == 1:
            body_pages_list.append("本文取得不可")
        elif not current_page_body_text:
            pass # 2ページ目以降で空の場合はリストに追加しない
        else:
            body_pages_list.append(current_page_body_text)

        
        # --- コメント数と日時は 1ページ目からのみ取得 ---
        if page_num == 1:
            # コメント数
            comment_button = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")}) or \
                                 soup.find("a", attrs={"data-cl-params": re.compile(r"cmtmod")})
            if comment_button:
                text = comment_button.get_text(strip=True).replace(",", "")
                match = re.search(r'(\d+)', text)
                if match:
                    comment_count = int(match.group(1))

            # 日時 (1ページ目の本文の冒頭から)
            body_text_partial_for_date = "\n".join(current_page_body_parts[:3]) # 1ページ目の本文の冒頭3行
            if body_text_partial_for_date:
                match_date = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', body_text_partial_for_date)
                if match_date:
                    month_day = match_date.group(1)
                    time_str = match_date.group(3)
                    extracted_date_str = f"{month_day} {time_str}"
                    
        # ページが切り替わる間に少し待機 (負荷軽減)
        if page_num < MAX_BODY_PAGES:
            time.sleep(0.5 + random.random() * 0.5) 

    # 4. 10列に満たない場合、残りを「-」で埋める
    if len(body_pages_list) < MAX_BODY_PAGES:
        body_pages_list.extend(["-"] * (MAX_BODY_PAGES - len(body_pages_list)))
    
    # 10列を超えた場合はスライスする (ほぼ発生しないが念のため)
    final_body_pages_list = body_pages_list[:MAX_BODY_PAGES]
    
    return final_body_pages_list, comment_count, extracted_date_str


# ====== 【新規追加】コメント本文取得関数 (最大10ページ + 存在確認) ======
def fetch_comments_by_page(base_url: str, total_comment_count: int) -> List[str]:
    """
    記事のコメントページを巡回し、「1ページ1セル」の形式でコメント本文を取得する。
    
    Args:
        base_url (str): /articles/ から始まる記事URL
        total_comment_count (int): F列に表示されるコメント総数 (100件を超えるか判定に使用)

    Returns:
        List[str]: 
            11個の要素を持つリスト。
            [ "Page 1 comments...", "Page 2 comments...", ..., "100件以上あり" ]
    """
    
    # 取得する最大ページ数 (J列～S列の10列分)
    MAX_PAGES_TO_SCRAPE = 10
    
    # 記事URL (.../articles/...) をコメントURL (.../comments/...) に変換
    comment_base_url = base_url.split('?')[0].replace("/articles/", "/comments/")
    if "/comments/" not in comment_base_url:
        print(f"    - ⚠️ コメントURLの生成に失敗: {base_url}")
        # 11列分のエラーメッセージを返す
        return ["コメントURL生成失敗"] * (MAX_PAGES_TO_SCRAPE + 1)

    page_comments_list = [] # 戻り値となるリスト (最大11要素)

    for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
        
        # コメント総数が 10件 (page_num=2) や 20件 (page_num=3) の場合、
        # それ以上ページは存在しないため、スクレイピングを停止する
        if total_comment_count <= (page_num - 1) * 10 and page_num > 1:
             print(f"    - コメント総数 ({total_comment_count}件) に基づき、ページ {page_num} 以降の取得をスキップ。")
             break # forループを抜ける

        current_url = f"{comment_base_url}?page={page_num}"
        
        # request_with_retry は既存の関数を流用
        response = request_with_retry(current_url, max_retries=2) 
        
        if not response:
            print(f"    - コメント ページ {page_num} ( {current_url} ) が存在しないか取得失敗。")
            page_comments_list.append("コメント取得不可")
            break # 失敗したら、それ以降のページ（例：page 3, 4...）の試行はしない

        print(f"    - コメント ページ {page_num} を取得中...")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Yahoo!ニュースのコメント本文は <p data-testid="comment-text"> に格納されている
        comment_tags = soup.find_all('p', attrs={"data-testid": "comment-text"})
        
        comments_on_this_page = []
        for tag in comment_tags:
            text = tag.get_text(strip=True)
            if text:
                comments_on_this_page.append(text)
        
        if not comments_on_this_page:
            # ページはあるがコメントが（何らかの理由で）ない場合
            print(f"    - コメント ページ {page_num} からコメントを抽出できませんでした。")
            page_comments_list.append("コメントなし")
            break # 念のため、これ以上は進まない
        
        # 取得したコメント (最大10件) を改行で結合し、1つのセル用データにする
        page_comments_list.append("\n".join(comments_on_this_page))
        time.sleep(0.5 + random.random() * 0.5) # サーバー負荷軽減

    # --- 10ページ分の処理が完了 ---

    # 途中でループが終了した場合（例：3ページ目で終わった）、残りの列（4～10）を「-」で埋める
    if len(page_comments_list) < MAX_PAGES_TO_SCRAPE:
        page_comments_list.extend(["-"] * (MAX_PAGES_TO_SCRAPE - len(page_comments_list)))
    
    # T列（11列目）の処理: 11ページ目 (101件目) が存在するか？
    # F列から取得したコメント総数が 100件 (10ページ * 10件) より多いか
    if total_comment_count > (MAX_PAGES_TO_SCRAPE * 10):
        page_comments_list.append(f"{MAX_PAGES_TO_SCRAPE * 10}件以上あり")
    else:
        page_comments_list.append("-")
        
    return page_comments_list


# ====== スプレッドシート操作関数 (ソート/置換ロジックを修正) ======

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        requests = []
        requests.append({
           "updateDimensionProperties": {
                 "range": {
                     "sheetId": ws.id,
                     "dimension": "ROWS",
                     "startIndex": 1,
                     "endIndex": ws.row_count
                 },
                 "properties": {
                     "pixelSize": row_height_pixels
                 },
                 "fields": "pixelSize"
            }
        })
        ws.spreadsheet.batch_update({"requests": requests})
        print(f" 2行目以降の**行の高さ**を {row_height_pixels} ピクセルに設定しました。")
    except Exception as e:
        print(f" ⚠️ 行高設定エラー: {e}")


def ensure_source_sheet_headers(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows=str(MAX_SHEET_ROWS_FOR_REPLACE), cols=str(len(YAHOO_SHEET_HEADERS)))
        
    current_headers = ws.row_values(1)
    if current_headers != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    worksheet = ensure_source_sheet_headers(sh)
            
    existing_data = worksheet.get_all_values(value_render_option='UNFORMATTED_VALUE')
    # 既存のA列（URL）をセットに格納
    existing_urls = set(str(row[0]) for row in existing_data[1:] if len(row) > 0 and str(row[0]).startswith("http"))
    
    # URLが重複しない新しいデータのみを抽出
    new_data = [[a['URL'], a['タイトル'], a['投稿日時'], a['ソース']] for a in articles if a['URL'] not in existing_urls]
    
    if new_data:
        # A～D列に追記
        worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")
    else:
        print("  SOURCEシートに追記すべき新しいデータはありません。")

def sort_yahoo_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        worksheet = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("ソートスキップ: Yahooシートが見つかりません。")
        return

    # 最終行を取得（データがある範囲を特定するため）
    last_row = len(worksheet.col_values(1))
    
    if last_row <= 1:
        print("ソート対象データがありません。ソートをスキップします。")
        return

    # --- 🚨 曜日削除のための batch_update (既存) ---
    try:
        requests = []
        
        # 曜日リスト
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        
        # 1. 各曜日に対応する個別の置換リクエストを生成 (7つのリクエスト)
        for day in days_of_week:
            requests.append({
                "findReplace": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 1, # 2行目から
                        "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE, # 10000行目まで
                        "startColumnIndex": 2, # C列
                        "endColumnIndex": 3 # C列
                    },
                    "find": rf"\({day}\)", # f-stringとraw stringで \(月\) の正規表現を生成
                    "replacement": "",
                    "searchByRegex": True,
                }
            })
            
        # 2. 曜日の直後に残る可能性のあるスペースや連続するスペースを削除し、半角スペース1つに統一 (1つのリクエスト)
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"\s{2,}",
                "replacement": " ",
                "searchByRegex": True,
            }
        })
        
        # 3. 最後に残る可能性のある前後の不要な空白を削除 (Trim機能の代替 - 1つのリクエスト)
        requests.append({
            "findReplace": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": MAX_SHEET_ROWS_FOR_REPLACE,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "find": r"^\s+|\s+$",
                "replacement": "",
                "searchByRegex": True,
            }
        })
        
        # batch_update でまとめて実行
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(" スプレッドシート上でC列の**曜日記載を個別に削除し、体裁を整えました**。")
        
    except Exception as e:
        print(f" ⚠️ スプレッドシート上の置換エラー: {e}")
    # ----------------------------------------------------

    # --- 【修正ポイント②】日時の表示形式変更 (repeatCellを使用) ---
    # --- 【修正ポイント】書式設定後にsleepを追加 ---
    try:
        format_requests = []
        format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 2,
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
        })
        worksheet.spreadsheet.batch_update({"requests": format_requests})
        print(f" ✅ C列(2行目〜{last_row}行) の表示形式を 'yyyy/mm/dd hh:mm:ss' に設定しました。")
        time.sleep(2)
    except Exception as e:
        print(f" ⚠️ C列の表示形式設定エラー: {e}") 

    # --- Google Sheets APIのsortRangeリクエスト ---
    try:
        sort_request = {
            "sortRange": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(YAHOO_SHEET_HEADERS)
                },
                "sortSpecs": [
                    {
                        "dimensionIndex": 2,
                        "sortOrder": "DESCENDING"
                    }
                ]
            }
        }
        worksheet.spreadsheet.batch_update({"requests": [sort_request]})
        print(" ✅ SOURCEシートを投稿日時の**新しい順**にGoogle Sheets APIで並び替えました。")
    except Exception as e:
        print(f" ⚠️ スプレッドシート上のソートエラー: {e}")
    # --- 【修正ポイント①】スプレッドシート上でのソート (APIソート) ---
    try:
        last_col_index = len(YAHOO_SHEET_HEADERS) # 31 (AE列)
        # gspread.utils.col_to_letter の代替関数を使用
        last_col_a1 = gspread_util_col_to_letter(last_col_index)
        sort_range = f'A2:{last_col_a1}{last_row}'

        # C列（3列目）を降順（新しい順）でソート
        # gspreadのsortメソッドを使用
        worksheet.sort((3, 'desc'), range=sort_range)
        print(" ✅ SOURCEシートを投稿日時の**新しい順**にスプレッドシート上で並び替えました。")
    except Exception as e:
        print(f" ⚠️ スプレッドシート上のソートエラー: {e}")

# ====== 本文・コメント数の取得と即時更新 (【修正】列シフト対応) ======

def fetch_details_and_update_sheet(gc: gspread.Client):
    """ 
    【修正】
    列のシフト (E-N列に本文、O列にコメント数) に対応。
    """
    
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("詳細取得スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、詳細取得をスキップします。")
        return
        
    data_rows = all_values[1:]
    update_count = 0
    
    print("\n===== 📄 ステップ② 記事本文とコメント数の取得・即時反映 =====")

    now_jst = jst_now()
    three_days_ago = (now_jst - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)

    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        post_date_raw = str(data_row[2]) # C列
        source = str(data_row[3])        # D列
        
        # ▼▼▼【修正】 E列～N列 (本文) と O列 (コメント数) を読み込む ▼▼▼
        body_p1 = str(data_row[4])       # E列 (本文P1)
        comment_count_str = str(data_row[14]) # O列 (コメント数)
        
        if not url.strip() or not url.startswith('http'):
            print(f"  - 行 {row_num}: URLが無効なためスキップ。")
            continue

        is_content_fetched = (body_p1.strip() and body_p1 != "本文取得不可") # 本文P1が取得済みか
        needs_body_fetch = not is_content_fetched # 本文取得が初回必要かどうか
        
        post_date_dt = parse_post_date(post_date_raw, now_jst)
        is_within_three_days = (post_date_dt and post_date_dt >= three_days_ago)
        
        # --- 判定ロジック (変更なし) ---
        if is_content_fetched and not is_within_three_days:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): 本文取得済みかつ3日より古い記事のため、**完全スキップ**。")
            continue
        is_comment_only_update = is_content_fetched and is_within_three_days
        needs_full_fetch = needs_body_fetch
        needs_detail_fetch = is_comment_only_update or needs_full_fetch

        if not needs_detail_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): 詳細更新の必要がないためスキップ。")
            continue

        # --- 詳細取得を実行 ---
        if needs_full_fetch:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **本文(P1-P10)/コメント数/日時補完/コメント本文 を取得中... (完全取得)**")
        elif is_comment_only_update:
            print(f"  - 行 {row_num} (記事: {title[:20]}...): **コメント数を更新中... (軽量更新)**")
            
        # ▼▼▼【修正】 fetch_article_body_and_comments は 10要素のList[str]を返す ▼▼▼
        fetched_body_pages, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url)

        fetched_comments_list = None
        if needs_full_fetch:
            try:
                comment_count_for_check = int(fetched_comment_count)
            except ValueError:
                comment_count_for_check = -1 
            fetched_comments_list = fetch_comments_by_page(url, total_comment_count=comment_count_for_check) 
        # ▲▲▲【修正】 fetch_article_body_and_comments は 10要素のList[str]を返す ▲▲▲

        new_comment_count = comment_count_str
        new_post_date = post_date_raw
        
        needs_cd_update = False # C, D列の更新フラグ
        needs_en_update = False # E-N列 (本文) の更新フラグ
        needs_o_update = False  # O列 (コメント数) の更新フラグ

        # 1. E-N列(本文)の更新 (本文未取得の場合のみ)
        if needs_full_fetch:
            # fetched_body_pages[0] (P1) と data_row[4] (E列) を比較
            if fetched_body_pages[0] != "本文取得不可" and fetched_body_pages[0] != str(data_row[4]):
                needs_en_update = True
            elif fetched_body_pages[0] == "本文取得不可" and str(data_row[4]) != "本文取得不可":
                 needs_en_update = True # 本文取得不可になった場合
        elif is_comment_only_update and fetched_body_pages[0] == "本文取得不可" and str(data_row[4]) != "本文取得不可":
             # コメント更新目的で叩いたが404になっていた場合
             needs_en_update = True
            
        # 2. C列(日時)の更新
        if needs_full_fetch and ("取得不可" in post_date_raw or not post_date_raw.strip()) and extracted_date:
            dt_obj = parse_post_date(extracted_date, now_jst)
            if dt_obj:
                formatted_dt = format_datetime(dt_obj)
                if formatted_dt != post_date_raw:
                    new_post_date = formatted_dt
                    needs_cd_update = True
            else:
                raw_date = re.sub(r"\([月火水木金土日]\)$", "", extracted_date).strip()
                if raw_date != post_date_raw:
                    new_post_date = raw_date
                    needs_cd_update = True
            
        # 3. O列(コメント数)の更新
        if fetched_comment_count != -1:
            if needs_full_fetch or is_comment_only_update:
                if str(fetched_comment_count) != comment_count_str:
                    new_comment_count = str(fetched_comment_count)
                    needs_o_update = True
        else:
            if needs_detail_fetch: 
                print(f"    - ⚠️ コメント数の取得に失敗しました。既存の値 ({comment_count_str}) を維持します。")

        # ▼▼▼【修正】 更新処理を列範囲ごとに分割 ▼▼▼
        if needs_cd_update:
            # C, D列を更新 (D列はソース。変更なしだが範囲に含める)
            ws.update(
                range_name=f'C{row_num}:D{row_num}',
                values=[[new_post_date, source]],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1)

        if needs_en_update:
            # E-N列 (本文P1-P10) を更新
            ws.update(
                range_name=f'E{row_num}:N{row_num}',
                values=[fetched_body_pages], # 1x10 のリスト
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1)
            
        if needs_o_update:
            # O列 (コメント数) を更新
            ws.update(
                range_name=f'O{row_num}',
                values=[[new_comment_count]],
                value_input_option='USER_ENTERED'
            )
            update_count += 1
            time.sleep(1)

        if fetched_comments_list:
            # S列～AC列 (コメントP1～P11以降) の11列に書き込む
            ws.update(
                range_name=f'S{row_num}:AC{row_num}',
                values=[fetched_comments_list], # 1x11 のリスト
                value_input_option='USER_ENTERED'
            )
            print(f"    - ✅ コメント本文 (S列～AC列) を更新しました。")
            time.sleep(0.5)
        # ▲▲▲【修正】 更新処理を列範囲ごとに分割 ▲▲▲

# ====== Gemini分析の実行と強制中断 (【修正】列シフト・消費量対策) ======

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    """ 
    【修正】
    列のシフトに対応。
    - E-N列 (P1-P10) から本文を読み取り、結合してGeminiに渡す。
    - P-R列 (基本分析) に書き込む。
    - AD-AE列 (日産分析) に書き込む。
    
    【消費量対策】
    - 1回の実行で分析する最大件数を MAX_ANALYSIS_PER_RUN で制限する。
    """
    
    # ▼▼▼【消費量削減対策】▼▼▼
    # 1回の実行でGeminiが分析する記事の最大数を設定 (例: 30件)
    # シートは新しい順にソート済みの前提
    MAX_ANALYSIS_PER_RUN = 30
    # ▲▲▲【消費量削減対策】▲▲▲

    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print("Gemini分析スキップ: Yahooシートが見つかりません。")
        return
        
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1:
        print(" Yahooシートにデータがないため、Gemini分析をスキップします。")
        return
        
    data_rows = all_values[1:]
    update_count = 0
    
    # ▼▼▼【消費量削減対策】▼▼▼
    print(f"\n===== 🧠 ステップ④ Gemini分析の実行・即時反映 (P-R, AD-AE列) [最大{MAX_ANALYSIS_PER_RUN}件] =====")
    # ▲▲▲【消費量削減対策】▲▲▲

    for idx, data_row in enumerate(data_rows):

        # ▼▼▼【消費量削減対策】▼▼▼
        # update_count (実際に分析・更新した件数) が上限に達したらループを抜ける
        if update_count >= MAX_ANALYSIS_PER_RUN:
            print(f"  - 分析件数が上限 ({MAX_ANALYSIS_PER_RUN}件) に達したため、Gemini分析を終了します。")
            break # forループを抜ける
        # ▲▲▲【消費量削減対策】▲▲▲

        if len(data_row) < len(YAHOO_SHEET_HEADERS):
            data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
            
        row_num = idx + 2
        
        url = str(data_row[0])
        title = str(data_row[1])
        
        # ▼▼▼【修正】 E-N列 (4～13) から本文を読み込み、結合する ▼▼▼
        body_pages = data_row[4:14] # E列(index 4) から N列(index 13) までの10要素
        body = "\n".join(str(p) for p in body_pages if str(p) and str(p) != "-") # 「-」を除外して結合
        
        # P-R列 (AI分析)
        company_info = str(data_row[15]) # P列 (16番目)
        category = str(data_row[16])     # Q列
        sentiment = str(data_row[17])    # R列

        # AD-AE列 (日産分析)
        nissan_mention = str(data_row[29]) # AD列 (30番目)
        nissan_sentiment = str(data_row[30])# AE列 (31番目)

        needs_analysis = not company_info.strip() or not category.strip() or not sentiment.strip() or \
                         not nissan_mention.strip() or not nissan_sentiment.strip()
        # ▲▲▲【修正】 E-N列 (4～13) から本文を読み込み、結合する ▲▲▲

        if not needs_analysis:
            continue
            
        if not body.strip() or body == "本文取得不可":
            print(f"  - 行 {row_num}: 本文がないため分析をスキップし、N/Aを設定。")
            
            # ▼▼▼【修正】 P-R列 と AD-AE列 を N/A で埋める ▼▼▼
            ws.update(
                range_name=f'P{row_num}:R{row_num}',
                values=[['N/A(No Body)', 'N/A', 'N/A']],
                value_input_option='USER_ENTERED'
            )
            ws.update(
                range_name=f'AD{row_num}:AE{row_num}',
                values=[['N/A(No Body)', 'N/A']],
                value_input_option='USER_ENTERED'
            )
            # ▲▲▲【修正】 P-R列 と AD-AE列 を N/A で埋める ▲▲▲
            
            update_count += 1 # N/A設定も「1件処理」としてカウントする
            time.sleep(1)
            continue
            
        if not url.strip():
            print(f"  - 行 {row_num}: URLがないためスキップ。")
            continue

        # ▼▼▼【消費量削減対策】▼▼▼
        print(f"  - 行 {row_num} (記事: {title[:20]}...): Gemini分析を実行中... ({update_count + 1}/{MAX_ANALYSIS_PER_RUN}件目)")
        # ▲▲▲【消費量削減対策】▲▲▲

        # --- Gemini分析を実行 (5つの戻り値) ---
        final_company_info, final_category, final_sentiment, \
        final_nissan_mention, final_nissan_sentiment, _ = analyze_with_gemini(body)
        
        # ▼▼▼【修正】 P-R列 と AD-AE列 に書き込む ▼▼▼
        ws.update(
            range_name=f'P{row_num}:R{row_num}',
            values=[[final_company_info, final_category, final_sentiment]],
            value_input_option='USER_ENTERED'
        )
        ws.update(
            range_name=f'AD{row_num}:AE{row_num}',
            values=[[final_nissan_mention, final_nissan_sentiment]],
            value_input_option='USER_ENTERED'
        )
        # ▲▲▲【修正】 P-R列 と AD-AE列 に書き込む ▼▲▲
        
        update_count += 1 # API呼び出しを「1件処理」としてカウント
        time.sleep(1 + random.random() * 0.5)

    print(f" ✅ Gemini分析を {update_count} 行について実行し、即時反映しました。")


# ====== メイン処理 (変更なし) ======

def main():
    print("--- 統合スクリプト開始 ---")
    
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        sys.exit(0)

    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        sys.exit(1)
    
    # ① ステップ① ニュース取得: A～D列の取得・追記を全キーワードで実行
    for current_keyword in keywords:
        print(f"\n===== 🔑 ステップ① ニュースリスト取得: {current_keyword} =====")
        yahoo_news_articles = get_yahoo_news_with_selenium(current_keyword)
        write_news_list_to_source(gc, yahoo_news_articles)
        time.sleep(2) # シートへの連続アクセス回避

    # ② ステップ② 本文・コメント数の取得と即時更新 (E-N, O, S-AC列)
    fetch_details_and_update_sheet(gc)

    # ③ ステップ③ ソートとC列の整形・書式設定
    print("\n===== 📑 ステップ③ 記事データのソートと整形 =====")
    sort_yahoo_sheet(gc)
    
    # ④ ステップ④ Gemini分析の実行と即時反映 (P-R, AD-AE列)
    analyze_with_gemini_and_update_sheet(gc)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    # スクリプトディレクトリをパスに追加して、プロンプトファイルを読み込めるようにする
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
    main()
