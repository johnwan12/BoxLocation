# BoxLocation.py — Full Streamlit App
# ============================================================
# Box Location + LN Inventory (LN3 multi-tank) + Freezer Inventory + Use_log + Final Report
#
# ✅ NEW (this version):
# - "Combo input" behavior for BOTH BoxLabel_group and Prefix in Freezer Add block:
#   - User can either pick from dropdown OR type a new value.
#   - We FIRST check if the typed value exists in dropdown options.
#   - If not, we allow it and add it to dropdown list (session cache) after Save.
#
# How it works:
# - UI shows: a dropdown (optional) + a text input.
# - If user types something, that typed value wins.
# - If user leaves text blank, dropdown selection is used.
# - After save, the new values are added to session-state sets and st.rerun() refreshes UI.
#
# ============================================================

import re
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Tuple

import pandas as pd
import pytz
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- Page --------------------
st.set_page_config(page_title="Box Location + LN/Freezer", layout="wide")
st.title("📦 Box Location + 🧊 LN Tank + 🧊 Freezer Inventory")

# -------------------- Session State --------------------
if "last_qr_link" not in st.session_state:
    st.session_state.last_qr_link = ""
if "last_qr_uid" not in st.session_state:
    st.session_state.last_qr_uid = ""
if "usage_final_rows" not in st.session_state:
    st.session_state.usage_final_rows = []  # TubeAmount hidden in report

# ✅ dynamic dropdown caches
if "custom_boxlabel_groups" not in st.session_state:
    st.session_state.custom_boxlabel_groups = set()
if "custom_prefixes" not in st.session_state:
    st.session_state.custom_prefixes = set()

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN_TAB = "LN3"
FREEZER_TAB = "Freezer_Inventory"
USE_LOG_TAB = "Use_log"

# Shared columns
BOX_LABEL_COL = "BoxLabel_group"
BOXID_COL = "BoxID"
AMT_COL = "TubeAmount"
MEMO_COL = "Memo"

# LN columns
TANK_COL = "TankID"
RACK_COL = "RackNumber"
TUBE_COL = "TubeNumber"
BOXUID_COL = "BoxUID"
QR_COL = "QRCodeLink"

# Freezer columns (your schema)
FREEZER_COL = "FreezerID"
PREFIX_COL = "Prefix"
SUFFIX_COL = "Tube suffix"
DATE_COLLECTED_COL = "Date Collected"
SAMPLES_RECEIVED_COL = "Samples Received"
MISSING_COL = "Missing"
URINE_RESULTS_COL = "Urine Results"
COLLECTED_BY_COL = "Collected By"

HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL", "NON-DRUG": "NON-DRUG"}

QR_PX = 118
SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]
NY_TZ = pytz.timezone("America/New_York")

# -------------------- Google Sheets service --------------------
@st.cache_resource(show_spinner=False)
def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(dict(st.secrets["google_service_account"]), scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# -------------------- Helpers --------------------
def safe_strip(x) -> str:
    return "" if x is None else str(x).strip()

def to_int_amount(x, default=0) -> int:
    try:
        s = safe_strip(x)
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def col_to_a1(col_idx_0based: int) -> str:
    n = col_idx_0based + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    return f"{time_str} {date_str}"

def today_str_ny() -> str:
    d = datetime.now(NY_TZ).date()
    return d.strftime("%m/%d/%Y")

def split_tube_number(t: str) -> Tuple[str, str]:
    t = safe_strip(t)
    if not t:
        return "", ""
    parts = t.split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def read_tab(tab_name: str) -> pd.DataFrame:
    svc = sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab_name}'!A1:ZZ",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = [safe_strip(h) for h in values[0]]
    rows = values[1:]
    n = len(header)

    fixed = []
    for r in rows:
        r = list(r)
        if len(r) < n:
            r += [""] * (n - len(r))
        elif len(r) > n:
            r = r[:n]
        fixed.append(r)

    return pd.DataFrame(fixed, columns=header)

def build_box_map() -> dict:
    df = read_tab(BOX_TAB)
    if df.empty:
        return {}

    study_candidates = ["StudyID", "Study ID", "Study Id", "ID"]
    box_candidates = ["BoxNumber", "Box Number", "Box", "Box#", "Box #"]

    study_col = next((c for c in study_candidates if c in df.columns), None)
    box_col = next((c for c in box_candidates if c in df.columns), None)
    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, r in df.iterrows():
        sid = safe_strip(r.get(study_col, "")).upper()
        bx = safe_strip(r.get(box_col, ""))
        if sid:
            m[sid] = bx
    return m

def get_sheet_id(service, sheet_title: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_title:
            return int(props.get("sheetId"))
    raise ValueError(f"Could not find sheetId for tab: {sheet_title}")

# ✅ Do NOT drop blanks from header
def get_header(service, tab: str) -> list:
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:ZZ1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    return [safe_strip(x) for x in row1]

def set_header_if_blank(service, tab: str, header: list):
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:ZZ1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    row1 = [safe_strip(x) for x in row1]
    if (not row1) or all(x == "" for x in row1):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_row_by_header(service, tab: str, data: dict):
    header = get_header(service, tab)
    if not header or all(h == "" for h in header):
        raise ValueError(f"{tab} header row is empty.")

    last = max(i for i, h in enumerate(header) if h != "")
    header = header[: last + 1]

    aligned = [data.get(col, "") for col in header]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:ZZ",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [aligned]},
    ).execute()

def cleanup_zero_amount_rows(service, tab_name: str, df: pd.DataFrame, amount_col: str = AMT_COL) -> bool:
    if df is None or df.empty or amount_col not in df.columns:
        return False

    amounts = pd.to_numeric(df[amount_col], errors="coerce").fillna(0).astype(int)
    zero_idxs = [int(i) for i in df.index[amounts == 0].tolist()]
    if not zero_idxs:
        return False

    sheet_id = get_sheet_id(service, tab_name)
    zero_idxs.sort(reverse=True)

    requests = [{
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": idx0 + 1,  # +1 for header
                "endIndex": idx0 + 2,
            }
        }
    } for idx0 in zero_idxs]

    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
    return True

def update_amount_by_index(service, tab_name: str, idx0: int, amount_col: str, new_amount: int):
    header = get_header(service, tab_name)
    if amount_col not in header:
        raise ValueError(f"{tab_name} missing '{amount_col}' column in header.")
    col_idx = header.index(amount_col)
    a1_col = col_to_a1(col_idx)
    sheet_row = idx0 + 2
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab_name}'!{a1_col}{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[int(new_amount)]]},
    ).execute()

def delete_row_by_index(service, tab_name: str, idx0: int):
    sheet_id = get_sheet_id(service, tab_name)
    start = idx0 + 1
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": start,
                    "endIndex": start + 1,
                }
            }
        }]},
    ).execute()

def get_current_max_boxid(df_view: pd.DataFrame) -> int:
    if df_view is None or df_view.empty or BOXID_COL not in df_view.columns:
        return 0
    s = pd.to_numeric(df_view[BOXID_COL], errors="coerce").dropna()
    return int(s.max()) if not s.empty else 0

def compute_next_boxuid(ln_view_df: pd.DataFrame, tank_id: str, rack: int, hp_hn: str, drug_code: str) -> str:
    tank_id = safe_strip(tank_id).upper()
    prefix = f"{tank_id}-R{int(rack):02d}-{hp_hn}-{drug_code}-"
    max_n = 0
    if ln_view_df is not None and (not ln_view_df.empty) and (BOXUID_COL in ln_view_df.columns):
        for v in ln_view_df[BOXUID_COL].dropna().astype(str):
            s = v.strip()
            if s.startswith(prefix) and re.search(r"-(\d{2})$", s):
                try:
                    n = int(s.split("-")[-1])
                    max_n = max(max_n, n)
                except ValueError:
                    pass
    nxt = max_n + 1
    if nxt > 99:
        raise ValueError(f"BoxUID sequence exceeded 99 for {prefix}**")
    return f"{prefix}{nxt:02d}"

def ensure_ln_header(service):
    recommended = [
        "TankID",
        "RackNumber",
        "BoxLabel_group",
        "BoxUID",
        "TubeNumber",
        "TubeAmount",
        "Memo",
        "BoxID",
        "QRCodeLink",
    ]
    set_header_if_blank(service, LN_TAB, recommended)

def ensure_freezer_header(service):
    recommended = [
        "FreezerID",
        "BoxID",
        "Prefix",
        "Tube suffix",
        "TubeAmount",
        "Date Collected",
        "BoxLabel_group",
        "Samples Received",
        "Missing",
        "Urine Results",
        "Collected By",
        "Memo",
    ]
    set_header_if_blank(service, FREEZER_TAB, recommended)

def ensure_use_log_header(service):
    recommended = [
        "StorageType",
        "TankID",
        "FreezerID",
        "BoxLabel_group",
        "BoxID",
        "TubeNumber",
        "Prefix",
        "Tube suffix",
        "Use",
        "User",
        "Time_stamp",
        "ShippingTo",
        "Memo",
    ]
    set_header_if_blank(service, USE_LOG_TAB, recommended)

def build_use_log_row(
    storage_type: str,
    tank_id: str,
    freezer_id: str,
    box_label_group: str,
    boxid: str,
    prefix: str,
    suffix: str,
    use_amt: int,
    user_initials: str,
    shipping_to: str,
    memo_in: str,
) -> dict:
    tube_number_combined = f"{safe_strip(prefix).upper()} {safe_strip(suffix)}".strip()
    return {
        "StorageType": safe_strip(storage_type),
        "TankID": safe_strip(tank_id).upper(),
        "FreezerID": safe_strip(freezer_id).upper(),
        "BoxLabel_group": safe_strip(box_label_group),
        "BoxID": safe_strip(boxid),
        "TubeNumber": tube_number_combined,
        "Prefix": safe_strip(prefix).upper(),
        "Tube suffix": safe_strip(suffix),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": now_timestamp_str(),
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(memo_in),
    }

def build_final_report_row(
    storage_type: str,
    storage_id: str,
    box_label_group: str,
    boxid: str,
    prefix: str,
    suffix: str,
    use_amt: int,
    user_initials: str,
    time_stamp: str,
    shipping_to: str,
    memo: str,
) -> dict:
    return {
        "StorageType": safe_strip(storage_type),
        "StorageID": safe_strip(storage_id).upper(),
        "BoxLabel_group": safe_strip(box_label_group),
        "BoxID": safe_strip(boxid),
        "Prefix": safe_strip(prefix).upper(),
        "Tube suffix": safe_strip(suffix),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": safe_strip(time_stamp),
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(memo),
    }

def find_ln_row_index(ln_all_df: pd.DataFrame, tank_id: str, box_label_group: str, boxid: str, tube_number: str):
    if ln_all_df is None or ln_all_df.empty:
        return None, None
    needed = {TANK_COL, BOX_LABEL_COL, BOXID_COL, TUBE_COL, AMT_COL}
    if not needed.issubset(set(ln_all_df.columns)):
        return None, None

    df = ln_all_df.copy()
    df[TANK_COL] = df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
    df[BOX_LABEL_COL] = df[BOX_LABEL_COL].astype(str).map(safe_strip)
    df[BOXID_COL] = df[BOXID_COL].astype(str).map(safe_strip)
    df[TUBE_COL] = df[TUBE_COL].astype(str).map(safe_strip)

    mask = (
        (df[TANK_COL] == safe_strip(tank_id).upper()) &
        (df[BOX_LABEL_COL] == safe_strip(box_label_group)) &
        (df[BOXID_COL] == safe_strip(boxid)) &
        (df[TUBE_COL] == safe_strip(tube_number))
    )
    hits = df[mask]
    if hits.empty:
        return None, None
    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get(AMT_COL, 0), default=0)
    return idx0, cur_amount

def find_freezer_row_index(fr_all_df: pd.DataFrame, freezer_id: str, box_label_group: str, boxid: str, prefix: str, suffix: str):
    if fr_all_df is None or fr_all_df.empty:
        return None, None
    needed = {FREEZER_COL, BOX_LABEL_COL, BOXID_COL, PREFIX_COL, SUFFIX_COL, AMT_COL}
    if not needed.issubset(set(fr_all_df.columns)):
        return None, None

    df = fr_all_df.copy()
    df[FREEZER_COL] = df[FREEZER_COL].astype(str).map(lambda x: safe_strip(x).upper())
    df[BOX_LABEL_COL] = df[BOX_LABEL_COL].astype(str).map(safe_strip)
    df[BOXID_COL] = df[BOXID_COL].astype(str).map(safe_strip)
    df[PREFIX_COL] = df[PREFIX_COL].astype(str).map(lambda x: safe_strip(x).upper())
    df[SUFFIX_COL] = df[SUFFIX_COL].astype(str).map(safe_strip)

    mask = (
        (df[FREEZER_COL] == safe_strip(freezer_id).upper()) &
        (df[BOX_LABEL_COL] == safe_strip(box_label_group)) &
        (df[BOXID_COL] == safe_strip(boxid)) &
        (df[PREFIX_COL] == safe_strip(prefix).upper()) &
        (df[SUFFIX_COL] == safe_strip(suffix))
    )
    hits = df[mask]
    if hits.empty:
        return None, None
    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get(AMT_COL, 0), default=0)
    return idx0, cur_amount

# --------- Combo-input helper (dropdown + optional typed override) ----------
def combo_pick_or_type(
    label: str,
    options: list,
    key_prefix: str,
    normalize_upper: bool = False,
    placeholder: str = "Type here to add new (optional)"
) -> str:
    options = [safe_strip(x) for x in options if safe_strip(x)]
    options = sorted(set(options))
    dd = st.selectbox(f"{label} (dropdown)", ["(select)"] + options, key=f"{key_prefix}_dd")
    typed = st.text_input(f"{label} (type to override / add new)", placeholder=placeholder, key=f"{key_prefix}_typed").strip()
    val = typed if typed else ("" if dd == "(select)" else dd)
    return val.upper() if normalize_upper else val

# ============================================================
# Sidebar (Global Controls)
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select Study", DISPLAY_TABS, index=0)

    STORAGE_TYPE = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)

    if STORAGE_TYPE == "LN Tank":
        selected_tank = st.selectbox("Select LN Tank", ["LN1", "LN2", "LN3"], index=2)
        selected_freezer = None
    else:
        selected_freezer = st.selectbox("Select Freezer", ["Sammy", "Tom", "Jerry"], index=0)
        selected_tank = None

    STORAGE_ID = selected_tank if STORAGE_TYPE == "LN Tank" else selected_freezer
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION
# ============================================================
st.header("📦 Box Location")
st.caption(f"Current context → Study: {selected_display_tab} | Storage: {STORAGE_TYPE} / {STORAGE_ID}")

try:
    df = read_tab(TAB_MAP[selected_display_tab])
    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.subheader(f"📋 All data in: {selected_display_tab}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")
        if "StudyID" not in df.columns:
            st.info("This tab does not have a 'StudyID' column.")
        else:
            studyids = df["StudyID"].dropna().astype(str).map(safe_strip)
            options = sorted([s for s in studyids.unique().tolist() if s])
            selected_studyid = st.selectbox("Select StudyID", ["(select)"] + options, key="studyid_select")
            if selected_studyid != "(select)":
                box_map = build_box_map()
                box = box_map.get(safe_strip(selected_studyid).upper(), "")
                st.markdown("**BoxNumber:**")
                st.success(box) if safe_strip(box) else st.error("Not Found")

except Exception as e:
    st.error("Box Location error")
    st.code(str(e), language="text")

# ============================================================
# 2) Services + headers
# ============================================================
service = sheets_service()
ensure_use_log_header(service)
ensure_ln_header(service)
ensure_freezer_header(service)

# ============================================================
# 3) Use_log viewer (always visible)
# ============================================================
st.divider()
st.subheader("🧾 Use_log (viewer)")
try:
    use_log_df = read_tab(USE_LOG_TAB)
    st.dataframe(use_log_df, use_container_width=True, hide_index=True) if not use_log_df.empty else st.info("Use_log is empty.")
except Exception as e:
    st.warning(f"Unable to read Use_log: {e}")

# ============================================================
# 4) LN MODULE (unchanged behavior)
# ============================================================
st.divider()
st.header("🧊 LN Tank Inventory")

if STORAGE_TYPE != "LN Tank":
    st.info("You selected **Freezer**. LN module hidden.")
else:
    try:
        ln_all_df = read_tab(LN_TAB)
    except Exception:
        ln_all_df = pd.DataFrame()

    try:
        if cleanup_zero_amount_rows(service, LN_TAB, ln_all_df, AMT_COL):
            st.info("🧹 Auto-clean: removed LN3 row(s) where TubeAmount was 0.")
            ln_all_df = read_tab(LN_TAB)
    except Exception as e:
        st.warning(f"LN3 auto-clean failed: {e}")

    ln_view_df = ln_all_df.copy()
    if not ln_view_df.empty and TANK_COL in ln_view_df.columns:
        ln_view_df[TANK_COL] = ln_view_df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
        ln_view_df = ln_view_df[ln_view_df[TANK_COL] == safe_strip(selected_tank).upper()].copy()

    st.subheader(f"📋 LN Inventory Table ({selected_tank})")
    st.dataframe(ln_view_df, use_container_width=True, hide_index=True) if not ln_view_df.empty else st.info("No records.")

# ============================================================
# 5) FREEZER MODULE (COMBO BoxLabel_group + Prefix)
# ============================================================
st.divider()
st.header("🧊 Freezer Inventory")

if STORAGE_TYPE != "Freezer":
    st.info("You selected **LN Tank**. Freezer module hidden.")
else:
    try:
        fr_all_df = read_tab(FREEZER_TAB)
    except Exception:
        fr_all_df = pd.DataFrame()

    try:
        if cleanup_zero_amount_rows(service, FREEZER_TAB, fr_all_df, AMT_COL):
            st.info("🧹 Auto-clean: removed Freezer_Inventory row(s) where TubeAmount was 0.")
            fr_all_df = read_tab(FREEZER_TAB)
    except Exception as e:
        st.warning(f"Freezer auto-clean failed: {e}")

    fr_view_df = fr_all_df.copy()
    if not fr_view_df.empty and FREEZER_COL in fr_view_df.columns:
        fr_view_df[FREEZER_COL] = fr_view_df[FREEZER_COL].astype(str).map(lambda x: safe_strip(x).upper())
        fr_view_df = fr_view_df[fr_view_df[FREEZER_COL] == safe_strip(selected_freezer).upper()].copy()

    st.subheader(f"📋 Freezer Inventory Table ({selected_freezer})")
    st.dataframe(fr_view_df, use_container_width=True, hide_index=True) if not fr_view_df.empty else st.info("No records.")

    # ---------- AddFreezer Inventory Record ----------
    st.subheader("➕ AddFreezer Inventory Record")

    current_max_boxid = get_current_max_boxid(fr_view_df)
    st.caption(f"Current max BoxID in {selected_freezer}: {current_max_boxid if current_max_boxid else '(none)'}")

    # build options from sheet + session cache
    existing_groups = []
    if not fr_view_df.empty and BOX_LABEL_COL in fr_view_df.columns:
        existing_groups = [safe_strip(x) for x in fr_view_df[BOX_LABEL_COL].dropna().unique().tolist() if safe_strip(x)]
    group_opts = sorted(set(existing_groups).union(st.session_state.custom_boxlabel_groups))

    existing_prefixes = []
    if not fr_view_df.empty and PREFIX_COL in fr_view_df.columns:
        existing_prefixes = [safe_strip(x).upper() for x in fr_view_df[PREFIX_COL].dropna().unique().tolist() if safe_strip(x)]
    prefix_opts = sorted(set(existing_prefixes).union(st.session_state.custom_prefixes))
    if not prefix_opts:
        prefix_opts = ["GICU", "HCCU"]

    with st.form("freezer_add", clear_on_submit=True):
        freezer_id = safe_strip(selected_freezer).upper()
        st.text_input("FreezerID (locked)", value=freezer_id, disabled=True)

        box_choice = st.radio("BoxID option", ["Using previous box", "Open a new box"], horizontal=True, key="fr_box_choice")
        if box_choice == "Using previous box":
            boxid_val = max(current_max_boxid, 1)
        else:
            boxid_val = (current_max_boxid + 1) if current_max_boxid >= 0 else 1
        st.text_input("BoxID (locked)", value=str(int(boxid_val)), disabled=True)
        boxid_input = str(int(boxid_val))

        # ✅ Combo behavior: dropdown + type override/add-new
        box_label_group = combo_pick_or_type(
            label="BoxLabel_group",
            options=group_opts,
            key_prefix="fr_boxlabel",
            normalize_upper=False,
            placeholder="e.g., HP-COC (leave blank to use dropdown)"
        )
        if box_label_group and box_label_group not in group_opts:
            st.info("New BoxLabel_group will be added to dropdown after Save.")

        prefix = combo_pick_or_type(
            label="Prefix",
            options=prefix_opts,
            key_prefix="fr_prefix",
            normalize_upper=True,
            placeholder="e.g., GICU / HCCU / ABCU (leave blank to use dropdown)"
        )
        if prefix and prefix not in [p.upper() for p in prefix_opts]:
            st.info("New Prefix will be added to dropdown after Save.")

        tube_suffix = st.text_input("Tube suffix", placeholder="e.g., 02 036").strip()
        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)

        date_collected = today_str_ny()
        st.text_input("Date Collected (auto today)", value=date_collected, disabled=True)

        c3, c4 = st.columns(2)
        with c3:
            samples_received = st.text_input("Samples Received", placeholder="optional").strip()
            missing = st.text_input("Missing", placeholder="optional").strip()
        with c4:
            urine_results = st.text_input("Urine Results", placeholder="optional").strip()
            collected_by = st.text_input("Collected By", placeholder="optional").strip()

        memo = st.text_area("Memo", placeholder="optional").strip()

        submitted_fr_add = st.form_submit_button("Save to Freezer_Inventory", type="primary")
        if submitted_fr_add:
            if not box_label_group:
                st.error("BoxLabel_group is required.")
                st.stop()
            if not prefix:
                st.error("Prefix is required.")
                st.stop()
            if not tube_suffix:
                st.error("Tube suffix is required.")
                st.stop()

            try:
                # ✅ add to session caches if new (so dropdown updates)
                st.session_state.custom_boxlabel_groups.add(box_label_group)
                st.session_state.custom_prefixes.add(prefix.upper())

                data = {
                    FREEZER_COL: freezer_id,
                    BOXID_COL: boxid_input,
                    PREFIX_COL: prefix.upper(),
                    SUFFIX_COL: tube_suffix,
                    AMT_COL: int(tube_amount),
                    DATE_COLLECTED_COL: date_collected,
                    BOX_LABEL_COL: box_label_group,
                    SAMPLES_RECEIVED_COL: samples_received,
                    MISSING_COL: missing,
                    URINE_RESULTS_COL: urine_results,
                    COLLECTED_BY_COL: collected_by,
                    MEMO_COL: memo,
                }
                append_row_by_header(service, FREEZER_TAB, data)
                st.success("Saved ✅ Freezer_Inventory record")
                st.rerun()
            except Exception as e:
                st.error("Failed to save Freezer_Inventory record")
                st.code(str(e), language="text")
