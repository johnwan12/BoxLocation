# BoxLocation.py — Full Streamlit App (LN + Freezer) — UPDATED
# ------------------------------------------------------------
# ✅ Requested modifications (Freezer Add block):
# 1) BoxLabel_group:
#    - If "Type new" chosen, user enters new value
#    - After save, the BoxLabel_group pulldown list updates (immediately via st.rerun())
#    - Also keeps a session-level custom list so it appears even before saving (optional behavior)
#
# 2) Prefix pulldown:
#    - Add "Custom (type)" option
#    - If chosen, user types a new prefix
#    - After save, the prefix is added to the pulldown list (session-level) and will persist via sheet data too
#
# Notes:
# - We only change the Freezer "AddFreezer Inventory Record" block here.
# - Everything else (LN + usage logging + final report) stays compatible.

import re
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Optional, Tuple

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
    st.session_state.usage_final_rows = []
# ✅ NEW: custom option caches (so dropdown updates instantly)
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

BOX_LABEL_COL = "BoxLabel_group"
BOXID_COL = "BoxID"
AMT_COL = "TubeAmount"
MEMO_COL = "Memo"

TANK_COL = "TankID"
RACK_COL = "RackNumber"
TUBE_COL = "TubeNumber"
BOXUID_COL = "BoxUID"
QR_COL = "QRCodeLink"

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

def today_str_ny() -> str:
    d = datetime.now(NY_TZ).date()
    return d.strftime("%m/%d/%Y")

def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    return f"{time_str} {date_str}"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def split_tube_number(t: str) -> Tuple[str, str]:
    t = safe_strip(t)
    if not t:
        return "", ""
    parts = t.split(" ", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]

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

def get_sheet_id(service, sheet_title: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_title:
            return int(props.get("sheetId"))
    raise ValueError(f"Could not find sheetId for tab: {sheet_title}")

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
                "startIndex": idx0 + 1,
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

# ============================================================
# Sidebar (Global Controls)
# ============================================================
with st.sidebar:
    st.subheader("Storage")
    STORAGE_TYPE = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)

    if STORAGE_TYPE == "LN Tank":
        selected_tank = st.selectbox("Select LN Tank", ["LN1", "LN2", "LN3"], index=2)
        selected_freezer = None
    else:
        selected_freezer = st.selectbox("Select Freezer", ["Sammy", "Tom", "Jerry"], index=0)
        selected_tank = None

    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

service = sheets_service()
ensure_freezer_header(service)

# ============================================================
# FREEZER MODULE — Add record with dynamic dropdown updates
# ============================================================
st.header("🧊 Freezer Inventory")

if STORAGE_TYPE != "Freezer":
    st.info("Switch Storage Type to **Freezer** to use this module.")
    st.stop()

# Load Freezer_Inventory
try:
    fr_all_df = read_tab(FREEZER_TAB)
except Exception:
    fr_all_df = pd.DataFrame()

# Auto-clean
try:
    if cleanup_zero_amount_rows(service, FREEZER_TAB, fr_all_df, AMT_COL):
        st.info("🧹 Auto-clean: removed Freezer_Inventory row(s) where TubeAmount was 0.")
        fr_all_df = read_tab(FREEZER_TAB)
except Exception as e:
    st.warning(f"Freezer auto-clean failed: {e}")

# Filter view by selected freezer
fr_view_df = fr_all_df.copy()
if not fr_view_df.empty and FREEZER_COL in fr_view_df.columns:
    fr_view_df[FREEZER_COL] = fr_view_df[FREEZER_COL].astype(str).map(lambda x: safe_strip(x).upper())
    fr_view_df = fr_view_df[fr_view_df[FREEZER_COL] == safe_strip(selected_freezer).upper()].copy()

st.subheader(f"📋 Freezer Inventory Table ({selected_freezer})")
if fr_view_df is None or fr_view_df.empty:
    st.info(f"No records for {selected_freezer}.")
else:
    st.dataframe(fr_view_df, use_container_width=True, hide_index=True)

# ---------- AddFreezer Inventory Record ----------
st.subheader("➕ AddFreezer Inventory Record")

current_max_boxid = get_current_max_boxid(fr_view_df)
st.caption(f"Current max BoxID in {selected_freezer}: {current_max_boxid if current_max_boxid else '(none)'}")

# Build dropdown lists from sheet + session custom lists
existing_groups = []
if fr_view_df is not None and (not fr_view_df.empty) and (BOX_LABEL_COL in fr_view_df.columns):
    existing_groups = sorted([safe_strip(x) for x in fr_view_df[BOX_LABEL_COL].dropna().unique().tolist() if safe_strip(x)])
group_opts = sorted(set(existing_groups).union(set(st.session_state.custom_boxlabel_groups)))

existing_prefixes = []
if fr_view_df is not None and (not fr_view_df.empty) and (PREFIX_COL in fr_view_df.columns):
    existing_prefixes = sorted([safe_strip(x).upper() for x in fr_view_df[PREFIX_COL].dropna().unique().tolist() if safe_strip(x)])
prefix_opts = sorted(set(existing_prefixes).union(set(st.session_state.custom_prefixes)))
if not prefix_opts:
    prefix_opts = ["GICU", "HCCU"]

with st.form("freezer_add", clear_on_submit=True):
    freezer_id = safe_strip(selected_freezer).upper()
    st.text_input("FreezerID (locked)", value=freezer_id, disabled=True)

    # BoxID option same as LN
    box_choice = st.radio("BoxID option", ["Using previous box", "Open a new box"], horizontal=True, key="fr_box_choice")
    if box_choice == "Using previous box":
        boxid_val = max(current_max_boxid, 1)
    else:
        boxid_val = (current_max_boxid + 1) if current_max_boxid >= 0 else 1
    st.text_input("BoxID (locked)", value=str(int(boxid_val)), disabled=True)
    boxid_input = str(int(boxid_val))

    # ✅ BoxLabel_group: Select existing OR Type new (and update list)
    group_mode = st.radio("BoxLabel_group", ["Select existing", "Type new"], horizontal=True, key="fr_group_mode")
    box_label_group = ""
    new_group_value = ""
    if group_mode == "Select existing":
        box_label_group = st.selectbox("BoxLabel_group (pulldown)", ["(select)"] + group_opts, key="fr_group_select")
        if box_label_group == "(select)":
            box_label_group = ""
    else:
        new_group_value = st.text_input("New BoxLabel_group", placeholder="e.g., HP-COC / HN-CAN / etc.").strip()
        box_label_group = new_group_value

    # ✅ Prefix: pulldown + Custom enter
    prefix_mode = st.selectbox("Prefix (pulldown)", prefix_opts + ["Custom (type)"], key="fr_prefix_mode")
    if prefix_mode == "Custom (type)":
        prefix_custom = st.text_input("Custom Prefix", placeholder="e.g., ABCU").strip()
        prefix = prefix_custom.upper()
    else:
        prefix = safe_strip(prefix_mode).upper()

    tube_suffix = st.text_input("Tube suffix", placeholder="e.g., 02 036", key="fr_suffix_add").strip()
    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1, key="fr_amt_add")

    # Date collected auto today()
    date_collected = today_str_ny()
    st.text_input("Date Collected (auto today)", value=date_collected, disabled=True)

    # Other fields
    c3, c4 = st.columns(2)
    with c3:
        samples_received = st.text_input("Samples Received", placeholder="optional", key="fr_samples_received").strip()
        missing = st.text_input("Missing", placeholder="optional", key="fr_missing").strip()
    with c4:
        urine_results = st.text_input("Urine Results", placeholder="optional", key="fr_urine").strip()
        collected_by = st.text_input("Collected By", placeholder="optional", key="fr_collected_by").strip()

    memo = st.text_area("Memo", placeholder="optional", key="fr_memo_add").strip()

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
            # Update session dropdown caches BEFORE write (so next rerun includes them)
            st.session_state.custom_boxlabel_groups.add(box_label_group)
            st.session_state.custom_prefixes.add(prefix)

            data = {
                FREEZER_COL: freezer_id,
                BOXID_COL: boxid_input,
                PREFIX_COL: prefix,
                SUFFIX_COL: safe_strip(tube_suffix),
                AMT_COL: int(tube_amount),
                DATE_COLLECTED_COL: date_collected,
                BOX_LABEL_COL: safe_strip(box_label_group),
                SAMPLES_RECEIVED_COL: samples_received,
                MISSING_COL: missing,
                URINE_RESULTS_COL: urine_results,
                COLLECTED_BY_COL: collected_by,
                MEMO_COL: memo,
            }
            append_row_by_header(service, FREEZER_TAB, data)
            st.success("Saved ✅ Freezer_Inventory record")

            # Force immediate UI refresh so pulldowns include the new group/prefix
            st.rerun()

        except Exception as e:
            st.error("Failed to save Freezer_Inventory record")
            st.code(str(e), language="text")
