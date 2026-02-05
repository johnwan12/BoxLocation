# BoxLocation.py — Full Streamlit App (LN + Freezer)
# -------------------------------------------------
# ✅ LN inventory tab: LN3 (multi-tank via TankID)
#    - LN3 column: BoxLabel_group (NOT BoxNumber)
# ✅ Freezer inventory tab: Freezer_Inventory (multi-freezer via FreezerID)
# ✅ Use_log tab: logs both LN + Freezer usage (no RackNumber/BoxUID/BoxNumber)
#
# Features
# - Box Location viewer + StudyID -> BoxNumber lookup (from 'boxNumber' tab)
# - LN module:
#   - Add LN record (auto BoxUID + QR, append to LN3)
#   - Auto-clean on load: delete LN3 rows where TubeAmount == 0
#   - Log Usage (LN): subtract TubeAmount; if 0 delete row; append to Use_log; session Final Report
# - Freezer module:
#   - Auto-clean on load: delete Freezer_Inventory rows where TubeAmount == 0
#   - Load Use_log viewer
#   - Log Usage (Freezer): subtract TubeAmount; if 0 delete row; append to Use_log; session Final Report
#
# Notes
# - If tabs already have headers, code will NOT overwrite them.
# - Required Inventory columns:
#   LN3: TankID, BoxLabel_group, BoxID, TubeNumber, TubeAmount
#   Freezer_Inventory: FreezerID, BoxLabel_group, BoxID, TubeNumber, TubeAmount
# - Use_log recommended (but not forced if you already have a header):
#   StorageType, TankID, FreezerID, BoxLabel_group, BoxID, TubeNumber, Use, User, Time_stamp, ShippingTo, Memo

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
    st.session_state.usage_final_rows = []  # session final report (TubeAmount hidden)

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}

# Box lookup tab (separate from inventory tabs)
BOX_TAB = "boxNumber"

# Inventory + log tabs
LN_TAB = "LN3"
FREEZER_TAB = "Freezer_Inventory"
USE_LOG_TAB = "Use_log"

# Column names (shared)
BOX_COL = "BoxLabel_group"
TUBE_COL = "TubeNumber"
AMT_COL = "TubeAmount"
BOXID_COL = "BoxID"
MEMO_COL = "Memo"

# LN specific
TANK_COL = "TankID"
RACK_COL = "RackNumber"
BOXUID_COL = "BoxUID"
QR_COL = "QRCodeLink"

# Freezer specific
FREEZER_COL = "FreezerID"

# Use_log recommended columns (RackNumber/BoxNumber/BoxUID removed)
USE_LOG_RECOMMENDED = [
    "StorageType",     # "LN" or "Freezer"
    "TankID",          # blank for Freezer records
    "FreezerID",       # blank for LN records
    "BoxLabel_group",
    "BoxID",
    "TubeNumber",
    "Use",
    "User",
    "Time_stamp",
    "ShippingTo",
    "Memo",
]

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

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def split_tube(t: str) -> Tuple[str, str]:
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

def build_box_map() -> dict:
    """
    Uses BOX_TAB='boxNumber' (separate lookup sheet).
    """
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

# ✅ Important: do NOT drop blanks from header (prevents column misalignment)
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

    row1 = get_header(service, LN_TAB)
    required = ["TankID", "BoxLabel_group", "BoxID", "TubeNumber", "TubeAmount"]
    missing = [c for c in required if c not in row1]
    if missing:
        st.warning(f"{LN_TAB} header missing required columns: {', '.join(missing)}")

def ensure_freezer_header(service):
    recommended = [
        "FreezerID",
        "BoxLabel_group",
        "BoxID",
        "TubeNumber",
        "TubeAmount",
        "Memo",
    ]
    set_header_if_blank(service, FREEZER_TAB, recommended)

    row1 = get_header(service, FREEZER_TAB)
    required = ["FreezerID", "BoxLabel_group", "BoxID", "TubeNumber", "TubeAmount"]
    missing = [c for c in required if c not in row1]
    if missing:
        st.warning(f"{FREEZER_TAB} header missing required columns: {', '.join(missing)}")

def ensure_use_log_header(service):
    # Recommended; if your Use_log already exists with a different header, we won't overwrite.
    set_header_if_blank(service, USE_LOG_TAB, USE_LOG_RECOMMENDED)

    row1 = get_header(service, USE_LOG_TAB)
    missing = [c for c in USE_LOG_RECOMMENDED if c not in row1]
    if missing:
        st.warning(f"{USE_LOG_TAB} header missing columns: {', '.join(missing)}")

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
                "startIndex": idx0 + 1,  # +1 because row0 is header
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

def find_row_index_by_keys(
    df: pd.DataFrame,
    id_col: str,
    id_val: str,
    box_label_group: str,
    boxid: str,
    tube_number: str,
) -> Tuple[Optional[int], Optional[int]]:
    """
    Generic row finder for LN or Freezer inventories.
    Returns (idx0, current_amount).
    """
    if df is None or df.empty:
        return None, None

    needed = {id_col, BOX_COL, BOXID_COL, TUBE_COL, AMT_COL}
    if not needed.issubset(set(df.columns)):
        return None, None

    d = df.copy()
    d[id_col] = d[id_col].astype(str).map(lambda x: safe_strip(x).upper())
    d[BOX_COL] = d[BOX_COL].astype(str).map(safe_strip)
    d[BOXID_COL] = d[BOXID_COL].astype(str).map(safe_strip)
    d[TUBE_COL] = d[TUBE_COL].astype(str).map(safe_strip)

    mask = (
        (d[id_col] == safe_strip(id_val).upper()) &
        (d[BOX_COL] == safe_strip(box_label_group)) &
        (d[BOXID_COL] == safe_strip(boxid)) &
        (d[TUBE_COL] == safe_strip(tube_number))
    )

    hits = d[mask]
    if hits.empty:
        return None, None

    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get(AMT_COL, 0), default=0)
    return idx0, cur_amount

def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    return f"{time_str} {date_str}"

def build_use_log_row(
    storage_type: str,   # "LN" or "Freezer"
    tank_id: str,
    freezer_id: str,
    box_label_group: str,
    boxid: str,
    tube_number: str,
    use_amt: int,
    user_initials: str,
    shipping_to: str,
    memo_in: str,
) -> dict:
    return {
        "StorageType": safe_strip(storage_type),
        "TankID": safe_strip(tank_id).upper(),
        "FreezerID": safe_strip(freezer_id).upper(),
        "BoxLabel_group": safe_strip(box_label_group),
        "BoxID": safe_strip(boxid),
        "TubeNumber": safe_strip(tube_number),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": now_timestamp_str(),
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(memo_in),
    }

def build_final_report_row_from_ui(
    storage_type: str,
    storage_id: str,     # TankID or FreezerID
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
    # Store both types in one session report with a unified "StorageType" + "StorageID"
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

# ============================================================
# Sidebar (Global Controls)
# ============================================================
with st.sidebar:
    st.subheader("Box Location")

    selected_display_tab = st.selectbox("Select Study", DISPLAY_TABS, index=0)

    STORAGE_TYPE = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)

    if STORAGE_TYPE == "LN Tank":
        TANK_OPTIONS = ["LN1", "LN2", "LN3"]
        selected_tank = st.selectbox("Select LN Tank", TANK_OPTIONS, index=2)
        selected_freezer = None
    else:
        # You can change these labels anytime (they are just UI labels)
        FREEZER_OPTIONS = ["Sammy", "Tom", "Jerry"]
        selected_freezer = st.selectbox("Select Freezer", FREEZER_OPTIONS, index=0)
        selected_tank = None

    STORAGE_ID = selected_tank if STORAGE_TYPE == "LN Tank" else selected_freezer
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION (study display)
# ============================================================
st.header("📦 Box Location")
st.caption(f"Current context → Study: {selected_display_tab} | Storage: {STORAGE_TYPE} / {STORAGE_ID}")

tab_name = TAB_MAP[selected_display_tab]
try:
    df = read_tab(tab_name)
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

            selected_studyid = st.selectbox("Select StudyID", ["(select)"] + options)
            if selected_studyid != "(select)":
                box_map = build_box_map()
                box = box_map.get(safe_strip(selected_studyid).upper(), "")
                st.markdown("**BoxNumber:**")
                if safe_strip(box) == "":
                    st.error("Not Found")
                else:
                    st.success(box)

except HttpError as e:
    st.error("Google Sheets API error (Box Location)")
    st.code(str(e), language="text")
except Exception as e:
    st.error("Unexpected error (Box Location)")
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
    if use_log_df.empty:
        st.info("Use_log is empty.")
    else:
        st.dataframe(use_log_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Unable to read Use_log: {e}")

# ============================================================
# 4) LN MODULE (only if LN Tank selected)
# ============================================================
st.divider()
st.header("🧊 LN Tank Inventory")

if STORAGE_TYPE != "LN Tank":
    st.info("You selected **Freezer**. LN module hidden.")
else:
    # Load LN3
    try:
        ln_all_df = read_tab(LN_TAB)
    except Exception:
        ln_all_df = pd.DataFrame()

    # ✅ Auto-clean LN3
    try:
        if cleanup_zero_amount_rows(service, LN_TAB, ln_all_df, AMT_COL):
            st.info("🧹 Auto-clean: removed LN3 row(s) where TubeAmount was 0.")
            ln_all_df = read_tab(LN_TAB)
    except Exception as e:
        st.warning(f"LN3 auto-clean failed: {e}")

    # Filter view by selected tank (inventory view + add record)
    ln_view_df = ln_all_df.copy()
    if not ln_view_df.empty and TANK_COL in ln_view_df.columns:
        ln_view_df[TANK_COL] = ln_view_df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
        ln_view_df = ln_view_df[ln_view_df[TANK_COL] == safe_strip(selected_tank).upper()].copy()

    # -------- Add New LN Record --------
    st.subheader("➕ Add LN Record")

    with st.form("ln_add", clear_on_submit=True):
        rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

        c1, c2 = st.columns(2)
        with c1:
            hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
        with c2:
            drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly", "NON-DRUG"], index=0)

        hp_hn = HIV_CODE[hiv_status]
        drug_code = DRUG_CODE.get(drug_group)
        if not drug_code:
            st.error(f"Unknown Drug Group: {drug_group}. Please update DRUG_CODE.")
            st.stop()

        box_label_group = f"{hp_hn}-{drug_code}"

        current_max_boxid = get_current_max_boxid(ln_view_df)
        st.caption(f"Current max BoxID in {selected_tank}: {current_max_boxid if current_max_boxid else '(none)'}")

        box_choice = st.radio("BoxID option", ["Using previous box", "Open a new box"], horizontal=True)
        opened_new_box = (box_choice == "Open a new box")

        if box_choice == "Using previous box":
            boxid_val = max(current_max_boxid, 1)
            st.text_input("BoxID (locked: current max BoxID)", value=str(boxid_val), disabled=True)
        else:
            boxid_val = (current_max_boxid + 1) if current_max_boxid >= 0 else 1
            st.text_input("BoxID (locked: max + 1)", value=str(boxid_val), disabled=True)

        boxid_input = str(int(boxid_val))

        c3, c4 = st.columns(2)
        with c3:
            tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
        with c4:
            tube_input = st.text_input("Tube Input", placeholder="e.g., 02 036").strip()

        tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""
        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
        memo = st.text_area("Memo (optional)")

        preview_uid, preview_qr, preview_err = "", "", ""
        try:
            preview_uid = compute_next_boxuid(ln_view_df, selected_tank, rack, hp_hn, drug_code)
            preview_qr = qr_link_for_boxuid(preview_uid)
        except Exception as e:
            preview_err = str(e)

        st.markdown("**BoxUID (auto):**")
        if preview_err:
            st.error(preview_err)
        else:
            st.info(preview_uid)

        st.markdown("**QR Preview (~1cm x 1cm):**")
        if preview_qr:
            st.image(preview_qr, width=QR_PX)

        submitted = st.form_submit_button("Save to LN", type="primary")

        if submitted:
            if not tube_input:
                st.error("Tube Input is required.")
                st.stop()

            try:
                box_uid = compute_next_boxuid(ln_view_df, selected_tank, rack, hp_hn, drug_code)
                qr_link = qr_link_for_boxuid(box_uid)

                data = {
                    TANK_COL: safe_strip(selected_tank).upper(),
                    RACK_COL: int(rack),
                    BOX_COL: box_label_group,
                    BOXUID_COL: box_uid,
                    TUBE_COL: tube_number,
                    AMT_COL: int(tube_amount),
                    MEMO_COL: memo,
                    BOXID_COL: boxid_input,
                    QR_COL: qr_link,
                }
                append_row_by_header(service, LN_TAB, data)
                st.success(f"Saved ✅ {box_uid}")

                if opened_new_box:
                    st.markdown(
                        f"""
                        <div style="padding:12px;border-radius:8px;background-color:#e8f5e9;border:1px solid #2e7d32;font-size:16px;">
                          ⚠️ <b>Please mark the box using the updated BoxID.</b><br><br>
                          <span style="color:#2e7d32;font-weight:700;font-size:20px;">
                            Hint: BoxID = {boxid_input}
                          </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                st.session_state.last_qr_link = qr_link
                st.session_state.last_qr_uid = box_uid

                ln_all_df = read_tab(LN_TAB)
                ln_view_df = ln_all_df.copy()
                if TANK_COL in ln_view_df.columns:
                    ln_view_df[TANK_COL] = ln_view_df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
                    ln_view_df = ln_view_df[ln_view_df[TANK_COL] == safe_strip(selected_tank).upper()].copy()

            except HttpError as e:
                st.error("Google Sheets API error while writing to LN.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to save LN record")
                st.code(str(e), language="text")

    # Download QR
    if st.session_state.last_qr_link:
        try:
            png_bytes = fetch_bytes(st.session_state.last_qr_link)
            st.download_button(
                label="⬇️ Download last saved QR PNG",
                data=png_bytes,
                file_name=f"{st.session_state.last_qr_uid or 'LN'}.png",
                mime="image/png",
                key="download_last_qr_png",
            )
        except Exception as e:
            st.warning(f"Saved, but QR download failed: {e}")

    # Show LN inventory
    st.subheader(f"📋 LN Inventory Table ({selected_tank})")
    if ln_view_df is None or ln_view_df.empty:
        st.info(f"No records for {selected_tank}.")
    else:
        st.dataframe(ln_view_df, use_container_width=True, hide_index=True)

    # -------- Log Usage (LN) --------
    st.subheader("📉 Log Usage (LN) — subtract TubeAmount + append Final Report")

    if ln_all_df is None or ln_all_df.empty:
        st.info("LN3 is empty — nothing to log.")
    elif not {TANK_COL, BOX_COL, BOXID_COL, TUBE_COL, AMT_COL}.issubset(set(ln_all_df.columns)):
        st.error(f"LN3 must include columns: {TANK_COL}, {BOX_COL}, {BOXID_COL}, {TUBE_COL}, {AMT_COL}.")
    else:
        dfv = ln_all_df.copy()
        dfv[TANK_COL] = dfv[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
        dfv[BOX_COL] = dfv[BOX_COL].astype(str).map(safe_strip)
        dfv[BOXID_COL] = dfv[BOXID_COL].astype(str).map(safe_strip)
        dfv[TUBE_COL] = dfv[TUBE_COL].astype(str).map(safe_strip)
        dfv[AMT_COL] = pd.to_numeric(dfv[AMT_COL], errors="coerce").fillna(0).astype(int)

        dfv["_prefix"] = dfv[TUBE_COL].map(lambda x: split_tube(x)[0].upper())
        dfv["_suffix"] = dfv[TUBE_COL].map(lambda x: split_tube(x)[1])

        # Dropdown chain
        tank_opts = sorted([t for t in dfv[TANK_COL].dropna().unique().tolist() if safe_strip(t)])
        chosen_tank = st.selectbox("TankID (pulldown)", ["(select)"] + tank_opts, key="ln_use_tank")
        scoped = dfv[dfv[TANK_COL] == safe_strip(chosen_tank).upper()].copy() if chosen_tank != "(select)" else dfv.iloc[0:0].copy()

        box_opts = sorted([b for b in scoped[BOX_COL].dropna().unique().tolist() if safe_strip(b)])
        chosen_box = st.selectbox("BoxLabel_group (pulldown)", ["(select)"] + box_opts, key="ln_use_box")
        scoped2 = scoped[scoped[BOX_COL] == safe_strip(chosen_box)].copy() if chosen_box != "(select)" else scoped.iloc[0:0].copy()

        boxid_opts = sorted([x for x in scoped2[BOXID_COL].dropna().unique().tolist() if safe_strip(x)])
        chosen_boxid = st.selectbox("BoxID (pulldown)", ["(select)"] + boxid_opts, key="ln_use_boxid")
        scoped3 = scoped2[scoped2[BOXID_COL] == safe_strip(chosen_boxid)].copy() if chosen_boxid != "(select)" else scoped2.iloc[0:0].copy()

        prefix_opts = sorted([p for p in scoped3["_prefix"].dropna().unique().tolist() if safe_strip(p)])
        chosen_prefix = st.selectbox("Prefix (pulldown)", ["(select)"] + prefix_opts, key="ln_use_prefix")
        scoped4 = scoped3[scoped3["_prefix"] == safe_strip(chosen_prefix).upper()].copy() if chosen_prefix != "(select)" else scoped3.iloc[0:0].copy()

        suffix_opts = sorted([s for s in scoped4["_suffix"].dropna().unique().tolist() if safe_strip(s)])
        chosen_suffix = st.selectbox("Tube suffix (pulldown)", ["(select)"] + suffix_opts, key="ln_use_suffix")

        match_df = scoped4[scoped4["_suffix"] == safe_strip(chosen_suffix)].copy() if chosen_suffix != "(select)" else scoped4.iloc[0:0].copy()
        st.markdown("**Current matching record(s): (SHOW TubeAmount)**")
        if match_df.empty:
            st.info("No matching record yet. Select TankID → BoxLabel_group → BoxID → Prefix → Tube suffix.")
        else:
            show_cols = [c for c in [TANK_COL, RACK_COL, BOX_COL, BOXID_COL, BOXUID_COL, TUBE_COL, AMT_COL, MEMO_COL] if c in match_df.columns]
            st.dataframe(match_df[show_cols], use_container_width=True, hide_index=True)

        with st.form("ln_usage_submit"):
            use_amt = st.number_input("Use", min_value=1, step=1, value=1)
            user_initials = st.text_input("User (initials)", placeholder="e.g., JW").strip()
            shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF / Building 3").strip()
            memo_in = st.text_area("Memo (optional)", placeholder="Usage memo...").strip()

            submitted_use = st.form_submit_button("Submit Usage (LN)", type="primary")

            if submitted_use:
                if "(select)" in [chosen_tank, chosen_box, chosen_boxid, chosen_prefix, chosen_suffix]:
                    st.error("Please select TankID, BoxLabel_group, BoxID, Prefix, and Tube suffix.")
                    st.stop()
                if not user_initials:
                    st.error("Please enter User initials.")
                    st.stop()
                if not shipping_to:
                    st.error("Please enter ShippingTo.")
                    st.stop()

                tube_number = f"{safe_strip(chosen_prefix).upper()} {safe_strip(chosen_suffix)}".strip()

                try:
                    idx0, cur_amount = find_row_index_by_keys(
                        df=ln_all_df,
                        id_col=TANK_COL,
                        id_val=chosen_tank,
                        box_label_group=chosen_box,
                        boxid=chosen_boxid,
                        tube_number=tube_number,
                    )
                    if idx0 is None:
                        st.error("No matching LN3 row found for the selected keys.")
                        st.stop()

                    new_amount = int(cur_amount) - int(use_amt)
                    if new_amount < 0:
                        st.error(f"Not enough stock. Current TubeAmount={cur_amount}, Use={int(use_amt)}")
                        st.stop()

                    # Append to Use_log (persist)
                    append_row_by_header(
                        service,
                        USE_LOG_TAB,
                        build_use_log_row(
                            storage_type="LN",
                            tank_id=chosen_tank,
                            freezer_id="",
                            box_label_group=chosen_box,
                            boxid=chosen_boxid,
                            tube_number=tube_number,
                            use_amt=int(use_amt),
                            user_initials=user_initials,
                            shipping_to=shipping_to,
                            memo_in=memo_in,
                        ),
                    )

                    # Update/delete LN3 row
                    if new_amount == 0:
                        delete_row_by_index(service, LN_TAB, idx0)
                        st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — LN3 row deleted.")
                    else:
                        update_amount_by_index(service, LN_TAB, idx0, AMT_COL, new_amount)
                        st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                    # Append to session Final Report (HIDE TubeAmount; show Use)
                    ts = now_timestamp_str()
                    st.session_state.usage_final_rows.append(
                        build_final_report_row_from_ui(
                            storage_type="LN",
                            storage_id=chosen_tank,
                            box_label_group=chosen_box,
                            boxid=chosen_boxid,
                            prefix=chosen_prefix,
                            suffix=chosen_suffix,
                            use_amt=int(use_amt),
                            user_initials=user_initials,
                            time_stamp=ts,
                            shipping_to=shipping_to,
                            memo=memo_in,
                        )
                    )

                    # Refresh
                    ln_all_df = read_tab(LN_TAB)
                    if new_amount == 0:
                        st.rerun()

                except HttpError as e:
                    st.error("Google Sheets API error while logging LN usage.")
                    st.code(str(e), language="text")
                except Exception as e:
                    st.error("Failed to log LN usage.")
                    st.code(str(e), language="text")

# ============================================================
# 5) FREEZER MODULE (only if Freezer selected)
# ============================================================
st.divider()
st.header("🧊 Freezer Inventory")

if STORAGE_TYPE != "Freezer":
    st.info("You selected **LN Tank**. Freezer module hidden.")
else:
    # Load Freezer inventory
    try:
        fr_all_df = read_tab(FREEZER_TAB)
    except Exception:
        fr_all_df = pd.DataFrame()

    # ✅ Auto-clean Freezer_Inventory
    try:
        if cleanup_zero_amount_rows(service, FREEZER_TAB, fr_all_df, AMT_COL):
            st.info("🧹 Auto-clean: removed Freezer_Inventory row(s) where TubeAmount was 0.")
            fr_all_df = read_tab(FREEZER_TAB)
    except Exception as e:
        st.warning(f"Freezer auto-clean failed: {e}")

    # Show freezer inventory filtered by selected freezer (if FreezerID exists)
    fr_view_df = fr_all_df.copy()
    if not fr_view_df.empty and FREEZER_COL in fr_view_df.columns:
        fr_view_df[FREEZER_COL] = fr_view_df[FREEZER_COL].astype(str).map(lambda x: safe_strip(x).upper())
        fr_view_df = fr_view_df[fr_view_df[FREEZER_COL] == safe_strip(selected_freezer).upper()].copy()

    st.subheader(f"📋 Freezer Inventory Table ({selected_freezer})")
    if fr_view_df is None or fr_view_df.empty:
        st.info(f"No records for {selected_freezer}.")
    else:
        st.dataframe(fr_view_df, use_container_width=True, hide_index=True)

    # -------- Log Usage (Freezer) --------
    st.subheader("📉 Log Usage (Freezer) — subtract TubeAmount + append Final Report")

    if fr_all_df is None or fr_all_df.empty:
        st.info("Freezer_Inventory is empty — nothing to log.")
    elif not {FREEZER_COL, BOX_COL, BOXID_COL, TUBE_COL, AMT_COL}.issubset(set(fr_all_df.columns)):
        st.error(f"{FREEZER_TAB} must include columns: {FREEZER_COL}, {BOX_COL}, {BOXID_COL}, {TUBE_COL}, {AMT_COL}.")
    else:
        dfv = fr_all_df.copy()
        dfv[FREEZER_COL] = dfv[FREEZER_COL].astype(str).map(lambda x: safe_strip(x).upper())
        dfv[BOX_COL] = dfv[BOX_COL].astype(str).map(safe_strip)
        dfv[BOXID_COL] = dfv[BOXID_COL].astype(str).map(safe_strip)
        dfv[TUBE_COL] = dfv[TUBE_COL].astype(str).map(safe_strip)
        dfv[AMT_COL] = pd.to_numeric(dfv[AMT_COL], errors="coerce").fillna(0).astype(int)

        dfv["_prefix"] = dfv[TUBE_COL].map(lambda x: split_tube(x)[0].upper())
        dfv["_suffix"] = dfv[TUBE_COL].map(lambda x: split_tube(x)[1])

        freezer_opts = sorted([f for f in dfv[FREEZER_COL].dropna().unique().tolist() if safe_strip(f)])
        chosen_freezer = st.selectbox("FreezerID (pulldown)", ["(select)"] + freezer_opts, key="fr_use_freezer")
        scoped = dfv[dfv[FREEZER_COL] == safe_strip(chosen_freezer).upper()].copy() if chosen_freezer != "(select)" else dfv.iloc[0:0].copy()

        box_opts = sorted([b for b in scoped[BOX_COL].dropna().unique().tolist() if safe_strip(b)])
        chosen_box = st.selectbox("BoxLabel_group (pulldown)", ["(select)"] + box_opts, key="fr_use_box")
        scoped2 = scoped[scoped[BOX_COL] == safe_strip(chosen_box)].copy() if chosen_box != "(select)" else scoped.iloc[0:0].copy()

        boxid_opts = sorted([x for x in scoped2[BOXID_COL].dropna().unique().tolist() if safe_strip(x)])
        chosen_boxid = st.selectbox("BoxID (pulldown)", ["(select)"] + boxid_opts, key="fr_use_boxid")
        scoped3 = scoped2[scoped2[BOXID_COL] == safe_strip(chosen_boxid)].copy() if chosen_boxid != "(select)" else scoped2.iloc[0:0].copy()

        prefix_opts = sorted([p for p in scoped3["_prefix"].dropna().unique().tolist() if safe_strip(p)])
        chosen_prefix = st.selectbox("Prefix (pulldown)", ["(select)"] + prefix_opts, key="fr_use_prefix")
        scoped4 = scoped3[scoped3["_prefix"] == safe_strip(chosen_prefix).upper()].copy() if chosen_prefix != "(select)" else scoped3.iloc[0:0].copy()

        suffix_opts = sorted([s for s in scoped4["_suffix"].dropna().unique().tolist() if safe_strip(s)])
        chosen_suffix = st.selectbox("Tube suffix (pulldown)", ["(select)"] + suffix_opts, key="fr_use_suffix")

        match_df = scoped4[scoped4["_suffix"] == safe_strip(chosen_suffix)].copy() if chosen_suffix != "(select)" else scoped4.iloc[0:0].copy()
        st.markdown("**Current matching record(s): (SHOW TubeAmount)**")
        if match_df.empty:
            st.info("No matching record yet. Select FreezerID → BoxLabel_group → BoxID → Prefix → Tube suffix.")
        else:
            show_cols = [c for c in [FREEZER_COL, BOX_COL, BOXID_COL, TUBE_COL, AMT_COL, MEMO_COL] if c in match_df.columns]
            st.dataframe(match_df[show_cols], use_container_width=True, hide_index=True)

        with st.form("fr_usage_submit"):
            use_amt = st.number_input("Use", min_value=1, step=1, value=1, key="fr_use_amt")
            user_initials = st.text_input("User (initials)", placeholder="e.g., JW", key="fr_user").strip()
            shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF / Building 3", key="fr_ship").strip()
            memo_in = st.text_area("Memo (optional)", placeholder="Usage memo...", key="fr_memo").strip()

            submitted_use = st.form_submit_button("Submit Usage (Freezer)", type="primary")

            if submitted_use:
                if "(select)" in [chosen_freezer, chosen_box, chosen_boxid, chosen_prefix, chosen_suffix]:
                    st.error("Please select FreezerID, BoxLabel_group, BoxID, Prefix, and Tube suffix.")
                    st.stop()
                if not user_initials:
                    st.error("Please enter User initials.")
                    st.stop()
                if not shipping_to:
                    st.error("Please enter ShippingTo.")
                    st.stop()

                tube_number = f"{safe_strip(chosen_prefix).upper()} {safe_strip(chosen_suffix)}".strip()

                try:
                    idx0, cur_amount = find_row_index_by_keys(
                        df=fr_all_df,
                        id_col=FREEZER_COL,
                        id_val=chosen_freezer,
                        box_label_group=chosen_box,
                        boxid=chosen_boxid,
                        tube_number=tube_number,
                    )
                    if idx0 is None:
                        st.error("No matching Freezer_Inventory row found for the selected keys.")
                        st.stop()

                    new_amount = int(cur_amount) - int(use_amt)
                    if new_amount < 0:
                        st.error(f"Not enough stock. Current TubeAmount={cur_amount}, Use={int(use_amt)}")
                        st.stop()

                    # Append to Use_log (persist)
                    append_row_by_header(
                        service,
                        USE_LOG_TAB,
                        build_use_log_row(
                            storage_type="Freezer",
                            tank_id="",
                            freezer_id=chosen_freezer,
                            box_label_group=chosen_box,
                            boxid=chosen_boxid,
                            tube_number=tube_number,
                            use_amt=int(use_amt),
                            user_initials=user_initials,
                            shipping_to=shipping_to,
                            memo_in=memo_in,
                        ),
                    )

                    # Update/delete freezer row
                    if new_amount == 0:
                        delete_row_by_index(service, FREEZER_TAB, idx0)
                        st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — Freezer_Inventory row deleted.")
                    else:
                        update_amount_by_index(service, FREEZER_TAB, idx0, AMT_COL, new_amount)
                        st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                    # Append to session Final Report (HIDE TubeAmount; show Use)
                    ts = now_timestamp_str()
                    st.session_state.usage_final_rows.append(
                        build_final_report_row_from_ui(
                            storage_type="Freezer",
                            storage_id=chosen_freezer,
                            box_label_group=chosen_box,
                            boxid=chosen_boxid,
                            prefix=chosen_prefix,
                            suffix=chosen_suffix,
                            use_amt=int(use_amt),
                            user_initials=user_initials,
                            time_stamp=ts,
                            shipping_to=shipping_to,
                            memo=memo_in,
                        )
                    )

                    fr_all_df = read_tab(FREEZER_TAB)
                    if new_amount == 0:
                        st.rerun()

                except HttpError as e:
                    st.error("Google Sheets API error while logging Freezer usage.")
                    st.code(str(e), language="text")
                except Exception as e:
                    st.error("Failed to log Freezer usage.")
                    st.code(str(e), language="text")

# ============================================================
# 6) Final Report (combined: LN + Freezer; TubeAmount hidden)
# ============================================================
st.divider()
st.subheader("✅ Final Report (session view; HIDE TubeAmount, show Use)")

final_cols = [
    "StorageType",
    "StorageID",
    "BoxLabel_group",
    "BoxID",
    "Prefix",
    "Tube suffix",
    "Use",
    "User",
    "Time_stamp",
    "ShippingTo",
    "Memo",
]

if st.session_state.usage_final_rows:
    final_df = pd.DataFrame(st.session_state.usage_final_rows).reindex(columns=final_cols, fill_value="")
    st.dataframe(final_df, use_container_width=True, hide_index=True)

    csv_bytes = final_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download session final report CSV",
        data=csv_bytes,
        file_name="final_report_session.csv",
        mime="text/csv",
        key="download_final_report",
    )

    if st.button("🧹 Clear session final report", key="clear_final_report"):
        st.session_state.usage_final_rows = []
        st.success("Session final report cleared (Use_log remains saved).")
else:
    st.info("No usage records in this session yet.")
