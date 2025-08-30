from dotenv import load_dotenv, find_dotenv
load_dotenv()  # .env 읽기
from pathlib import Path
import pandas as pd
from notion_client import Client
import tempfile
import sys
import urllib.parse
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from ju_make_final_df import make_final_df
from ju_make_finance_df import make_finance_df
from ju_make_excel import build_finance_excel
from googleapiclient.http import MediaIoBaseUpload
import os
import streamlit as st, tempfile, os, json


NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
if not NOTION_TOKEN:
    st.error("NOTION_TOKEN이 설정되지 않았습니다.")
    st.stop()

DRIVE_SA_JSON_PATH = None  # 빌드 환경에서 동적으로 해석합니다


if "GOOGLE_SERVICE_ACCOUNT_JSON" in st.secrets:
    _sa_path = tempfile.NamedTemporaryFile(delete=False, suffix=".json").name
    with open(_sa_path, "w", encoding="utf-8") as f:
        f.write(st.secrets["GOOGLE_SERVICE_ACCOUNT_JSON"])
    os.environ["DRIVE_SA_JSON_PATH"] = _sa_path
if "NOTION_TOKEN" in st.secrets:
    NOTION_TOKEN = st.secrets["NOTION_TOKEN"]


st.set_page_config(page_title="소셜라운지 정산 자동화", layout="wide")
st.title("소셜라운지 정산 자동화")
# Notion 클라이언트 초기화
notion = Client(auth=NOTION_TOKEN)

# Cache: Drive service (리소스)
def _resolve_service_account_path() -> str:
    # 우선순위: 환경변수 → 실행파일/스크립트 폴더의 service_account.json → 기존 상수 경로(호환)
    env_path = os.environ.get("DRIVE_SA_JSON_PATH")
    if env_path and os.path.exists(env_path):
        return env_path
    base_dir = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    local_candidate = os.path.join(base_dir, "service_account.json")
    if os.path.exists(local_candidate):
        return local_candidate
    if DRIVE_SA_JSON_PATH and os.path.exists(DRIVE_SA_JSON_PATH):
        return DRIVE_SA_JSON_PATH
    raise FileNotFoundError("서비스 계정 JSON을 찾을 수 없습니다. 환경변수 DRIVE_SA_JSON_PATH 설정 또는 service_account.json 파일을 실행파일 폴더에 두세요.")

@st.cache_resource(show_spinner=False)
def get_drive_service():
    sa_path = _resolve_service_account_path()
    with open(sa_path, "r", encoding="utf-8") as f:
        info = json.load(f)
    # 업로드/생성 권한이 필요하므로 전체 Drive 쓰기 스코프 사용
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds)

def _drive_download_content(_drive, file_id: str, mime_type: str | None) -> bytes:
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = _drive.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = _drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.getvalue()

def _concat_drive_excels(_drive, files: list[dict]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for f in files:
        name = f.get("name") or ""
        mime = f.get("mimeType")
        try:
            if name.lower().endswith((".xlsx", ".xls")) or mime in (
                "application/vnd.ms-excel",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.google-apps.spreadsheet",
            ):
                content = _drive_download_content(_drive, f.get("id"), mime)
                bio = io.BytesIO(content)
                df = pd.read_excel(bio)
                df["__source_file__"] = name
                frames.append(df)
        except Exception as e:
            # 개별 파일 오류는 건너뛰고 계속 진행
            continue
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

def _extract_notion_table(df: pd.DataFrame) -> pd.DataFrame:
    def _dedupe_headers_with_nan_to_prev_underscore(cols_in):
        result = []
        used = set()
        last_non_empty = None
        for raw in list(cols_in):
            s = str(raw).strip()
            if s == "" or s.lower() == "nan":
                if last_non_empty is None:
                    s = "_"
                else:
                    s = last_non_empty + "_"
            else:
                last_non_empty = s
            # ensure uniqueness
            base = s
            while s in used:
                s = base + "_"
                base = s
            used.add(s)
            result.append(s)
        return result
    # 헤더 행 추정: 'NO'가 포함된 행을 찾아 그 행을 헤더로 사용
    header_idx = None
    needed = {"NO", "카테고리", "상품명"}
    max_scan = min(30, len(df))
    for i in range(max_scan):
        row_vals = set(str(v).strip() for v in list(df.iloc[i].values))
        if "NO" in row_vals and ("카테고리" in row_vals or "분류" in row_vals):
            header_idx = i
            break
        # 느슨한 조건: 필요한 키 일부 만족
        if len(needed.intersection(row_vals)) >= 2:
            header_idx = i
            break
    if header_idx is None:
        # 실패 시 현재 컬럼으로 시도
        work = df.copy()
        work.columns = _dedupe_headers_with_nan_to_prev_underscore(work.columns)
    else:
        work = df.iloc[header_idx + 1 :].copy()
        cols = list(df.iloc[header_idx].values)
        work.columns = _dedupe_headers_with_nan_to_prev_underscore(cols)

    # 'NO' 열 정규화
    if "NO" not in work.columns:
        # 가장 왼쪽 컬럼이 NO일 가능성 처리
        first_col = work.columns[0]
        if str(first_col).strip().upper() == "NO":
            work.rename(columns={first_col: "NO"}, inplace=True)
        else:
            return pd.DataFrame()

    work["NO_num"] = pd.to_numeric(work["NO"], errors="coerce")
    # 연속 구간: 첫 유효값부터 다음 NaN 전까지
    mask = work["NO_num"].notna()
    if not mask.any():
        return pd.DataFrame()
    start_idx = mask.idxmax()
    after = work.loc[start_idx:]
    # 첫 NaN 위치 찾기
    stop_rel = after["NO_num"].isna()
    if stop_rel.any():
        stop_idx = stop_rel.idxmax()
        sliced = work.loc[start_idx: stop_idx - 1]
    else:
        sliced = after
    # 불필요 컬럼 제거 및 정리
    if "NO_num" in sliced.columns:
        sliced = sliced.drop(columns=["NO_num"])  # 표시용 제거
    # 공백/Unnamed 컬럼 정리
    sliced = sliced.loc[:, ~sliced.columns.astype(str).str.contains("^Unnamed")]
    sliced = sliced.drop(columns=["상품명_","구성_"],axis=1)
    return sliced.reset_index(drop=True)

def _extract_page_title(page):
    """검색 결과의 페이지 객체에서 사람이 읽을 수 있는 제목을 추출합니다."""
    # 1) 데이터베이스 항목(행)인 경우: properties 안의 type==title 속성에서 추출
    properties = page.get("properties", {}) or {}
    for _, prop in properties.items():
        if isinstance(prop, dict) and prop.get("type") == "title":
            title_fragments = prop.get("title", [])
            if title_fragments:
                return "".join([frag.get("plain_text", "") for frag in title_fragments]) or None
    # 2) 일반 페이지인 경우: URL slug에서 유추
    url = page.get("url", "")
    if url:
        try:
            last = url.split("/")[-1].split("?")[0]
            parts = last.split("-")
            if len(parts) > 1:
                slug = "-".join(parts[:-1])
            else:
                slug = last
            return urllib.parse.unquote(slug).replace("-", " ")
        except Exception:
            return None
    return None

def _normalize_text(text):
    return "".join((text or "").lower().split())

def _decode_filename(text: str) -> str:
    try:
        return urllib.parse.unquote(text or "", encoding="utf-8", errors="replace")
    except Exception:
        try:
            return urllib.parse.unquote(text or "")
        except Exception:
            return text or ""

def search_pages_by_title(title):
    """제목으로 페이지를 검색하고 후보 목록을 반환합니다.

    반환값: [{ id, title, url }]
    """
    try:
        resp = notion.search(
            query=title,
            filter={"property": "object", "value": "page"},
            sort={"direction": "descending", "timestamp": "last_edited_time"},
        )
        results = resp.get("results", [])
        candidates = []
        for page in results:
            if page.get("object") != "page":
                continue
            human_title = _extract_page_title(page) or "제목 없음"
            candidates.append({
                "id": page.get("id"),
                "title": human_title,
                "url": page.get("url"),
            })

        # 우선 정확 일치, 다음 부분 일치 정렬
        norm_query = _normalize_text(title)
        exact = [c for c in candidates if _normalize_text(c["title"]) == norm_query]
        if exact:
            return exact
        partial = [c for c in candidates if norm_query in _normalize_text(c["title"])]
        return partial or candidates
    except Exception as e:
        st.error(f"페이지 검색 중 오류 발생: {str(e)}")
        return []

def _list_all_blocks(block_id):
    blocks = []
    start_cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=block_id, start_cursor=start_cursor)
        blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return blocks


def get_xlsx_files_from_page(page_id):
    """페이지(및 모든 하위 블록/하위 페이지/속성)에서 .xlsx/.xls 파일을 수집합니다."""
    try:
        xlsx_files = []

        def _is_excel_by_name_or_url(name: str, url: str) -> bool:
            lname = (name or "").lower()
            lurl = (url or "").lower()
            return lname.endswith((".xlsx", ".xls")) or lurl.split("?")[0].endswith((".xlsx", ".xls"))

        # 1) 페이지 속성에 첨부된 파일(데이터베이스 행 등) 수집
        try:
            page_obj = notion.pages.retrieve(page_id=page_id)
            for prop in (page_obj.get("properties") or {}).values():
                if isinstance(prop, dict) and prop.get("type") == "files":
                    for item in prop.get("files", []):
                        itype = item.get("type")  # file | external
                        url = (item.get(itype) or {}).get("url")
                        raw_name = item.get("name") or (url or "").rsplit("/", 1)[-1]
                        name = _decode_filename(raw_name)
                        if url and _is_excel_by_name_or_url(name, url):
                            xlsx_files.append({"name": name, "url": url})
        except Exception:
            # 페이지가 권한 또는 형식 문제로 조회되지 않는 경우 무시하고 블록 탐색으로 계속
            pass

        # 2) 블록을 재귀적으로 순회하며 file 블록과 child_page, 그리고 has_children 블록을 탐색
        def collect_from_blocks(blocks):
            for block in blocks:
                btype = block.get("type")
                if btype == "file":
                    file_info = block.get("file", {})
                    ftype = file_info.get("type")  # file | external
                    url = (file_info.get(ftype) or {}).get("url")
                    # 블록에는 name이 없을 수 있어 URL에서 유추 (Windows/URL 모두 안전하게 처리)
                    try:
                        name_guess = os.path.basename(urllib.parse.urlparse(url or "").path) or (url or "").rsplit("/", 1)[-1]
                    except Exception:
                        name_guess = (url or "").rsplit("/", 1)[-1]
                    decoded_name = _decode_filename(name_guess)
                    if url and _is_excel_by_name_or_url(decoded_name, url):
                        xlsx_files.append({"name": decoded_name or "download.xlsx", "url": url})

                # 모든 블록에서 자식이 있으면 탐색
                if block.get("has_children"):
                    child_id = block.get("id")
                    child_blocks = _list_all_blocks(child_id)
                    collect_from_blocks(child_blocks)

                # 별도로 child_page는 위 로직에 포함되지만 명시적으로 한 번 더 안전하게 처리
                if btype == "child_page":
                    child_id = block.get("id")
                    child_blocks = _list_all_blocks(child_id)
                    collect_from_blocks(child_blocks)

        root_blocks = _list_all_blocks(page_id)
        collect_from_blocks(root_blocks)

        # 중복 제거(같은 url 기준)
        uniq = {}
        for f in xlsx_files:
            uniq[f["url"]] = f
        return list(uniq.values())
    except Exception as e:
        st.error(f"Notion API 오류: {str(e)}")
        return []

@st.cache_data(show_spinner=False)
def list_purchase_orders(_drive, folder_id):
    files = []
    page_token = None
    while True:
        resp = _drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false and name contains '발주서'",
            fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return [f for f in files if (f.get("name") or "").startswith("발주서")]


def main():
    st.divider()
    st.info("1. 구글 드라이브 folders/ 뒷부분의 문자를 입력하고, 가져오기 버튼을 눌러주세요")
    folder_id = st.text_input("구글 폴더 ID",value="1t86O2qdONoW-8H5xN2unWg8YzW--Z4IM")

    col1, col2 = st.columns([1,1])
    with col1:
        run = st.button("가져오기")
    with col2:
        reset = st.button("초기화")

    if reset:
        for k in [
            "drive_files", "product_name", "notion_page_id", "notion_xlsx_files",
            "selected_xlsx_index", "last_folder_id", "initialized", "df_invoice_raw",
            "df_notion", "raw_unique_keys", "notion_unique_keys", "matching_map",
            "grid_current_df", "df_matching"
        ]:
            if k in st.session_state:
                del st.session_state[k]

    if run:
        try:
            drive = get_drive_service()
        except Exception as e:
            st.error(f"드라이브 인증 실패: {e}")
            return
        try:
            drive_files = list_purchase_orders(drive, folder_id)
        except Exception as e:
            st.error(f"드라이브 목록 조회 실패: {e}")
            return
        if not drive_files:
            st.warning("'발주서'로 시작하는 파일이 없습니다.")
            return
        st.session_state["drive_files"] = drive_files
        st.session_state["last_folder_id"] = folder_id

        # 구글 xlsx 모두 concat → df_invoice_raw 저장
        df_invoice_raw = _concat_drive_excels(drive, drive_files)
        st.session_state["df_invoice_raw"] = df_invoice_raw
        st.session_state["initialized"] = True

        first_name = drive_files[0].get("name") or ""
        base = first_name.rsplit(".", 1)[0]
        parts = base.split("_")
        if len(parts) < 4:
            st.error("파일명 규칙(발주서_날짜_셀러_품목.xlsx)에 맞지 않습니다.")
            return
        product_name = parts[3]
        st.session_state["product_name"] = product_name

        candidates = search_pages_by_title(product_name)
        if not candidates:
            st.error(f"노션에서 '{product_name}' 페이지를 찾지 못했습니다.")
            return
        page = candidates[0]
        page_id = page.get("id")
        st.session_state["notion_page_id"] = page_id

        st.session_state["notion_xlsx_files"] = get_xlsx_files_from_page(page_id)

    # Render saved results regardless of run state (빠른 렌더)
    if "drive_files" in st.session_state:
        with st.expander("드라이브 발주서 파일 목록"):
            st.dataframe(pd.DataFrame(st.session_state["drive_files"]), use_container_width=True)
    if "df_invoice_raw" in st.session_state and not st.session_state["df_invoice_raw"].empty:
        with st.expander("발주서 취합본(구글 xlsx 병합)"):
            st.dataframe(st.session_state["df_invoice_raw"], use_container_width=True)
    if "product_name" in st.session_state and "notion_page_id" in st.session_state:
        st.success(f"추출된 품목: {st.session_state['product_name']}/노션 페이지 ID: {st.session_state['notion_page_id']}")

    if "notion_xlsx_files" in st.session_state and st.session_state["notion_xlsx_files"]:
        files = st.session_state["notion_xlsx_files"]
        names = [f["name"] for f in files]
        default_index = st.session_state.get("selected_xlsx_index", 0)
        st.divider()
        st.info("2. 노션 xlsx 파일을 선택해주세요")
        selected_name = st.selectbox("노션 xlsx 파일선택", options=names, index=min(default_index, len(names)-1), key="xlsx_selector")
        selected_index = names.index(selected_name)
        st.session_state["selected_xlsx_index"] = selected_index
        selected_file = files[selected_index]

        try:
            import requests
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
                response = requests.get(selected_file["url"], timeout=30)
                response.raise_for_status()
                tmp_file.write(response.content)
            df_x = pd.read_excel(tmp_file.name)
            # 노션 표 추출 → df_notion
            df_notion = _extract_notion_table(df_x)
            st.session_state["df_notion"] = df_notion.copy()
            if not df_notion.empty:
                st.dataframe(df_notion, use_container_width=True)
            else:
                st.info("테이블 헤더/구간을 찾지 못했습니다. 원본을 표시합니다.")
                st.dataframe(df_x, use_container_width=True)
            with open(tmp_file.name, "rb") as f:
                st.download_button(
                    label="다운로드 (.xlsx)",
                    data=f,
                    file_name=selected_file["name"] if selected_file["name"].lower().endswith(".xlsx") else f"{selected_file['name']}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            os.unlink(tmp_file.name)
        except Exception as e:
            st.error(f"노션 파일 처리 중 오류: {e}")

    # 매핑 UI: df_invoice_raw 와 df_notion 이 있어야 진행
    if ("df_invoice_raw" in st.session_state and isinstance(st.session_state["df_invoice_raw"], pd.DataFrame)
        and not st.session_state["df_invoice_raw"].empty
        and "df_notion" in st.session_state and isinstance(st.session_state["df_notion"], pd.DataFrame)
        and not st.session_state["df_notion"].empty):
        st.divider()
        df_raw = st.session_state["df_invoice_raw"]
        df_notion = st.session_state["df_notion"]

        raw_columns = list(map(str, df_raw.columns))
        st.info("3. 정산과 매핑에 필요한 정보를 입력합니다.")
        with st.form("raw_cols_form", clear_on_submit=False):
            sel_product = st.selectbox("1-1. 주문데이터의 상품명 컬럼을 선택해주세요", options=raw_columns, key="raw_product_col")
            sel_option = st.selectbox("1-2. 주문데이터의 옵션명 컬럼을 선택해주세요", options=["없음"] + raw_columns, key="raw_option_col")
            sel_qty = st.selectbox("1-3. 수량 컬럼을 선택해주세요", options=raw_columns, key="raw_qty_col")
            sel_orderno = st.selectbox("1-4. 주문번호 컬럼을 선택해주세요", options=raw_columns, key="raw_orderno_col")
            input_seller_ratio = st.number_input("1-5. 셀러부담 배송비 비율을 입력해주세요", min_value=0, max_value=100, step=1, value=100, key="seller_shipping_ratio")
            input_shipping_fee = st.number_input("1-6. 배송비를 입력해주세요", min_value=0, step=1, value=3000, key="shipping_fee")
            input_shipping_cond = st.number_input("1-7. 배송비 조건 금액을 입력해주세요(예: 50000원 이하 → 50000)", min_value=0, step=1000, value=40000, key="shipping_condition_amount")
            sel_island_col = st.selectbox("1-8. 도서산간배송비 컬럼을 선택해주세요", options=["(없음)"] + raw_columns, key="raw_island_col")
            sel_island_mode = st.selectbox(
                "1-9. 도서산간배송비 산출방법을 선택해주세요",
                options=["실제 배송비가 raw 데이터에 존재", "도서산간 구분만 존재"],
                key="raw_island_mode",
            )
            island_flag_text = ""
            island_fee_value = 0
            if sel_island_mode == "도서산간 구분만 존재":
                island_flag_text = st.text_input("1-10. RAW 데이터 도서산간 구분 텍스트를 입력해주세요", value="", key="island_flag_text")
                island_fee_value = st.number_input("1-11. 도서산간배송비를 입력해주세요", min_value=0, step=1000, value=0, key="island_fee_value")
            submitted = st.form_submit_button("제출")

        if submitted:
            try:
                prod_series = df_raw[sel_product].astype(str)
                if sel_option == "없음":
                    raw_keys = prod_series.str.strip().fillna("")
                else:
                    opt_series = df_raw[sel_option].astype(str)
                    raw_keys = (prod_series.str.strip() + "(" + opt_series.str.strip() + ")").fillna("")
                raw_unique = sorted(raw_keys.unique())

                notion_prod = df_notion.get("상품명").astype(str) if "상품명" in df_notion.columns else pd.Series(dtype=str)
                notion_cfg = df_notion.get("구성").astype(str) if "구성" in df_notion.columns else pd.Series(dtype=str)
                if len(notion_prod) and len(notion_cfg):
                    notion_keys = sorted((notion_prod.str.strip() + "(" + notion_cfg.str.strip() + ")").unique())
                else:
                    notion_keys = []

                st.session_state["raw_unique_keys"] = raw_unique
                st.session_state["notion_unique_keys"] = notion_keys
                if "matching_map" not in st.session_state:
                    st.session_state["matching_map"] = {}
                # 추가 입력값 저장
                st.session_state["selected_orderno_col"] = sel_orderno
                st.session_state["selected_qty_col"] = sel_qty
                # 위젯 key와 다른 세션 키에 저장하여 충돌 방지
                st.session_state["shipping_fee_value"] = int(input_shipping_fee or 0)
                st.session_state["seller_shipping_ratio_value"] = int(input_seller_ratio or 0)
                st.session_state["shipping_condition_amount_value"] = int(input_shipping_cond or 0)
                st.session_state["island_col"] = None if sel_island_col == "(없음)" else sel_island_col
                st.session_state["island_mode"] = ("raw" if sel_island_mode == "실제 배송비가 raw 데이터에 존재" else "flag")
                st.session_state["island_flag_text_value"] = island_flag_text
                st.session_state["island_fee_value_int"] = int(island_fee_value or 0)
            except Exception as e:
                st.error(f"매핑 준비 중 오류: {e}")

        if "raw_unique_keys" in st.session_state and st.session_state.get("notion_unique_keys") is not None:
            raw_unique = st.session_state.get("raw_unique_keys", [])
            notion_keys = st.session_state.get("notion_unique_keys", [])

            st.divider()
            st.info("4. 발주서와 노션파일을 매핑합니다. 노션상품 컬럼을 모두 채워주세요")
            mapping = st.session_state.get("matching_map", {})
            options = ["(선택 안함)"] + sorted({str(x).strip() for x in notion_keys if str(x).strip()})

            with st.form("matching_form_table", clear_on_submit=False):
                edit_df = pd.DataFrame({
                    "주문상품": raw_unique,
                    "노션상품": [mapping.get(k) for k in raw_unique],
                })
                edited = st.data_editor(
                    edit_df,
                    column_config={
                        "주문상품": st.column_config.TextColumn("주문상품", disabled=True),
                        "노션상품": st.column_config.SelectboxColumn("노션상품", options=options, required=False),
                    },
                    num_rows="fixed",
                    use_container_width=True,
                    key="matching_editor",
                )

                submitted_match = st.form_submit_button("매칭 저장")

            if submitted_match:
                # 유효성(중복 금지, 선택 안함/None 제외)
                chosen_vals = [v for v in edited["노션상품"].tolist() if v not in (None, "(선택 안함)")]
                if pd.Series(chosen_vals).duplicated(keep=False).any():
                    st.error("동일한 노션상품이 여러 행에 선택되었습니다. 중복을 제거해주세요.")
                else:
                    mapping = {
                        row["주문상품"]: (None if (pd.isna(row["노션상품"]) or row["노션상품"] == "(선택 안함)") else row["노션상품"]) 
                        for _, row in edited.iterrows()
                    }
                    st.session_state["matching_map"] = mapping
                    df_matching = pd.DataFrame([
                        {"주문상품": k, "노션상품": v}
                        for k, v in mapping.items() if v is not None
                    ])
                    st.session_state["df_matching"] = df_matching
                    st.success("매칭이 저장되었습니다.")

            if "df_matching" in st.session_state and not st.session_state["df_matching"].empty:
                st.divider()
                st.info("5. RAW데이터와 정산 데이터 파일을 생성했습니다.")
                df_matching = st.session_state["df_matching"]
                df_final = make_final_df(
                    df_raw,
                    df_notion,
                    df_matching,
                    sel_product,
                    sel_option,
                    sel_qty,
                    st.session_state.get("selected_orderno_col"),
                    st.session_state.get("shipping_fee_value"),
                    st.session_state.get("shipping_condition_amount_value"),
                    st.session_state.get("seller_shipping_ratio_value"),
                    st.session_state.get("island_col"),
                    st.session_state.get("island_mode"),
                    st.session_state.get("island_flag_text_value"),
                    st.session_state.get("island_fee_value_int"),
                )
                with st.expander("RAW데이터 보기"):
                    st.dataframe(df_final, use_container_width=True)

                # 5. 정산 정리 df_finance 생성
                try:
                    df_finance = make_finance_df(
                        df_final,
                        st.session_state.get("drive_files", []),
                        st.session_state.get("selected_qty_col"),
                        st.session_state.get("shipping_fee_value"),
                        st.session_state.get("seller_shipping_ratio_value"),
                        st.session_state.get("island_fee_value_int"),
                    )
                    with st.expander("정산 집계 데이터보기"):
                        st.dataframe(df_finance, use_container_width=True)
                    st.session_state["df_finance"] = df_finance
                    # 다운로드 버튼
                    xls_bytes, final_filename = build_finance_excel(
                        df_finance,
                        df_final,
                        st.session_state.get("drive_files", []),
                        title="정산 리포트",
                    )
                    # 다운로드 버튼 (업로드는 별도 버튼으로 실행)
                    st.download_button(
                        label="파일 다운로드 (정산 리포트 .xlsx)",
                        data=xls_bytes,
                        file_name=final_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_finance_excel",
                    )

                    # 업로드 버튼 (눌렀을 때만 업로드)
                    if st.button("구글 드라이브로 업로드", key="upload_finance_to_drive"):
                        try:
                            drive = get_drive_service()
                            folder_id = st.session_state.get("last_folder_id")
                            if folder_id and xls_bytes and final_filename:
                                media = MediaIoBaseUpload(io.BytesIO(xls_bytes), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                                file_metadata = {"name": final_filename, "parents": [folder_id]}
                                drive.files().create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True).execute()
                                st.success(f"구글 드라이브 업로드 완료, https://drive.google.com/drive/u/1/folders/{folder_id} 에서 확인해주세요")
                            else:
                                st.info("업로드할 폴더 또는 파일 데이터가 없습니다.")
                        except Exception as ue:
                            st.error(f"드라이브 업로드 실패: {ue}")
                    # 자동 업로드 제거됨: 아래 업로드 버튼으로만 업로드 수행
                except Exception as e:
                    st.error(f"정산 DF 생성 중 오류: {e}")

if __name__ == "__main__":
    main()