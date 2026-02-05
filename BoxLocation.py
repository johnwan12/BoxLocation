# BoxLocation.py — Full Streamlit App (UPDATED)
# Box Location + LN Inventory (multi-tank) + Use_log
#
# ✅ Updates per your latest headers:
# - LN3: "BoxNumber" renamed to "BoxLabel_group"
# - Use_log: REMOVE columns RackNumber, BoxNumber, BoxUID
#   Use_log expected columns:
#     TankID | BoxLabel_group | BoxID | TubeNumber | Use | User | Time_stamp | ShippingTo | Memo
#
# ✅ Features:
# - Box Location viewer + StudyID -> BoxNumber lookup (from separate 'boxNumber' tab)
# - LN3 inventory (multi-tank via TankID column)
# - Add LN record (auto BoxUID + QR, append to LN3)
# - ✅ Auto-clean on load: delete LN3 rows where TubeAmount == 0
# - Use_log viewer
# - ✅ Log Usage (LN) block:
#     TankID(pulldown) | BoxLabel_group(pulldown) | BoxID(pulldown) | Prefix(pulldown) | Tube suffix(pulldown)
#     User enters Use, User, ShippingTo, Memo
#   - Current matching record(s): SHOW TubeAmount
#   - Final report: HIDE TubeAmount, show Use
#   - Subtract Use from TubeAmount; if becomes 0 -> DELETE LN3 row
#
# IMPORTANT:
# - If LN3 / Use_log already have non-blank header rows, code won't overwrite them.
# - Make sure LN3 header row includes at least:
#   TankID, RackNumber, BoxLabel_group, BoxUID, TubeNumber, TubeAmount, Memo, BoxID, QRCodeLink

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
st.set_page_config(page_title="Box Location + LN Tank", layout="wide")
st.title("📦 Box Location + 🧊 Liquid Nitrogen Tank")

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

# Box lookup tab (separate from LN3)
BOX_TAB = "boxNumber"

# LN inventory + Use_log tabs
LN_TAB = "LN3"
USE_LOG_TAB = "Use_log"

# ✅ LN3 column name: BoxLabel_group (replaces BoxNumber)
TANK_COL = "TankID"
RACK_COL = "RackNumber"
BOX_COL = "BoxLabel_group"
BOXUID_COL = "BoxUID"
TUBE_COL = "TubeNumber"
AMT_COL = "TubeAmount"
MEMO_COL = "Memo"
BOXID_COL = "BoxID"
QR_COL = "QRCodeLink"

# ✅ Use_log columns (RackNumber/BoxNumber/BoxUID removed)
USE_LOG_EXPECTED = [
    "TankID",
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
    This is NOT LN3.
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

    # Only use columns up to last non-blank header cell
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
        "BoxLabel_group",  # ✅
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
    missing_required = [c for c in required if c not in row1]
    if missing_required:
        st.warning(f"{LN_TAB} header missing required columns: {', '.join(missing_required)}")

def ensure_use_log_header(service):
    set_header_if_blank(service, USE_LOG_TAB, USE_LOG_EXPECTED)

    row1 = get_header(service, USE_LOG_TAB)
    missing = [c for c in USE_LOG_EXPECTED if c not in row1]
    if missing:
        st.warning(f"{USE_LOG_TAB} header missing columns: {', '.join(missing)}")

def cleanup_zero_tubeamount_rows(service, ln_all_df: pd.DataFrame) -> bool:
    if ln_all_df is None or ln_all_df.empty or AMT_COL not in ln_all_df.columns:
        return False

    amounts = pd.to_numeric(ln_all_df[AMT_COL], errors="coerce").fillna(0).astype(int)
    zero_idxs = [int(i) for i in ln_all_df.index[amounts == 0].tolist()]
    if not zero_idxs:
        return False

    sheet_id = get_sheet_id(service, LN_TAB)
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

def get_current_max_boxid(ln_view_df: pd.DataFrame) -> int:
    if ln_view_df is None or ln_view_df.empty or BOXID_COL not in ln_view_df.columns:
        return 0
    s = pd.to_numeric(ln_view_df[BOXID_COL], errors="coerce").dropna()
    if s.empty:
        return 0
    return int(s.max())

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

def update_ln_tubeamount_by_index(service, idx0: int, new_amount: int):
    header = get_header(service, LN_TAB)
    if AMT_COL not in header:
        raise ValueError(f"{LN_TAB} missing '{AMT_COL}' column in header.")

    col_idx = header.index(AMT_COL)
    a1_col = col_to_a1(col_idx)
    sheet_row = idx0 + 2  # header row +1, plus 1-indexed rows

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN_TAB}'!{a1_col}{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[int(new_amount)]]},
    ).execute()

def delete_ln_row_by_index(service, idx0: int):
    sheet_id = get_sheet_id(service, LN_TAB)
    start = idx0 + 1  # header offset
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

def find_ln_row_index_by_keys(
    ln_all_df: pd.DataFrame,
    tank_id: str,
    box_label_group: str,
    boxid: str,
    tube_number: str,
) -> Tuple[Optional[int], Optional[int]]:
    if ln_all_df is None or ln_all_df.empty:
        return None, None

    needed = {TANK_COL, BOX_COL, BOXID_COL, TUBE_COL, AMT_COL}
    if not needed.issubset(set(ln_all_df.columns)):
        return None, None

    df = ln_all_df.copy()
    df[TANK_COL] = df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
    df[BOX_COL] = df[BOX_COL].astype(str).map(safe_strip)
    df[BOXID_COL] = df[BOXID_COL].astype(str).map(safe_strip)
    df[TUBE_COL] = df[TUBE_COL].astype(str).map(safe_strip)

    mask = (
        (df[TANK_COL] == safe_strip(tank_id).upper()) &
        (df[BOX_COL] == safe_strip(box_label_group)) &
        (df[BOXID_COL] == safe_strip(boxid)) &
        (df[TUBE_COL] == safe_strip(tube_number))
    )

    hits = df[mask]
    if hits.empty:
        return None, None

    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get(AMT_COL, 0), default=0)
    return idx0, cur_amount

def build_use_log_row_from_ln_row(
    row: pd.Series,
    use_amt: int,
    user_initials: str,
    shipping_to: str,
    memo_in: str,
    tank_id: str,
) -> dict:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    ts = f"{time_str} {date_str}"

    # ✅ Use_log no longer stores RackNumber/BoxUID
    return {
        "TankID": safe_strip(tank_id).upper(),
        "BoxLabel_group": safe_strip(row.get(BOX_COL, "")),
        "BoxID": safe_strip(row.get(BOXID_COL, "")),
        "TubeNumber": safe_strip(row.get(TUBE_COL, "")),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": ts,
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(memo_in),
    }

def build_final_report_row_from_ui(
    tank_id: str,
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
        "TankID": safe_strip(tank_id).upper(),
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
# 2) LN Tank Module (only when LN Tank selected)
# ============================================================
st.divider()
st.header("🧊 Liquid Nitrogen Tank Inventory")

if STORAGE_TYPE != "LN Tank":
    st.info("You selected **Freezer**. LN Tank module is hidden.")
    st.stop()

service = sheets_service()
ensure_ln_header(service)
ensure_use_log_header(service)

# Load FULL LN sheet
try:
    ln_all_df = read_tab(LN_TAB)
except Exception:
    ln_all_df = pd.DataFrame()

# ✅ Auto-clean on load: remove TubeAmount == 0
try:
    if cleanup_zero_tubeamount_rows(service, ln_all_df):
        st.info("🧹 Auto-clean: removed LN3 row(s) where TubeAmount was 0.")
        ln_all_df = read_tab(LN_TAB)
except Exception as e:
    st.warning(f"Auto-clean failed: {e}")

# Filter view for sidebar-selected tank (inventory view + add record)
ln_view_df = ln_all_df.copy()
if ln_view_df is not None and (not ln_view_df.empty) and (TANK_COL in ln_view_df.columns):
    ln_view_df[TANK_COL] = ln_view_df[TANK_COL].astype(str).map(lambda x: safe_strip(x).upper())
    ln_view_df = ln_view_df[ln_view_df[TANK_COL] == safe_strip(selected_tank).upper()].copy()

# ---------- Add New LN Record ----------
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

    # this is the LN3 grouping label (HP-COC, etc.)
    box_label_group = f"{hp_hn}-{drug_code}"

    # ----- BoxID (NOT editable) -----
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
                BOX_COL: box_label_group,  # ✅ BoxLabel_group
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

# ---------- Download QR outside the form ----------
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

# ---------- Show LN table (filtered view) ----------
st.subheader(f"📋 LN Inventory Table ({selected_tank})")
if ln_view_df is None or ln_view_df.empty:
    st.info(f"No records for {selected_tank}.")
else:
    st.dataframe(ln_view_df, use_container_width=True, hide_index=True)

# ============================================================
# 3) Use_log viewer (load Use_log tab)
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
# 4) Log Usage (LN) block (dropdowns + subtract TubeAmount + final report)
# ============================================================
st.divider()
st.subheader("📉 Log Usage (LN) — subtract from TubeAmount + append Final Report")

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

    tank_opts = sorted([t for t in dfv[TANK_COL].dropna().unique().tolist() if safe_strip(t)])
    chosen_tank = st.selectbox("TankID (pulldown)", ["(select)"] + tank_opts, key="use_ln_tank")

    scoped = dfv.copy()
    if chosen_tank != "(select)":
        scoped = scoped[scoped[TANK_COL] == safe_strip(chosen_tank).upper()].copy()
    else:
        scoped = scoped.iloc[0:0].copy()

    box_opts = sorted([b for b in scoped[BOX_COL].dropna().unique().tolist() if safe_strip(b)])
    chosen_box = st.selectbox("BoxLabel_group (pulldown)", ["(select)"] + box_opts, key="use_ln_box")

    scoped2 = scoped.copy()
    if chosen_box != "(select)":
        scoped2 = scoped2[scoped2[BOX_COL] == safe_strip(chosen_box)].copy()
    else:
        scoped2 = scoped2.iloc[0:0].copy()

    boxid_opts = sorted([x for x in scoped2[BOXID_COL].dropna().unique().tolist() if safe_strip(x)])
    chosen_boxid = st.selectbox("BoxID (pulldown)", ["(select)"] + boxid_opts, key="use_ln_boxid")

    scoped3 = scoped2.copy()
    if chosen_boxid != "(select)":
        scoped3 = scoped3[scoped3[BOXID_COL] == safe_strip(chosen_boxid)].copy()
    else:
        scoped3 = scoped3.iloc[0:0].copy()

    prefix_opts = sorted([p for p in scoped3["_prefix"].dropna().unique().tolist() if safe_strip(p)])
    chosen_prefix = st.selectbox("Prefix (pulldown)", ["(select)"] + prefix_opts, key="use_ln_prefix")

    scoped4 = scoped3.copy()
    if chosen_prefix != "(select)":
        scoped4 = scoped4[scoped4["_prefix"] == safe_strip(chosen_prefix).upper()].copy()
    else:
        scoped4 = scoped4.iloc[0:0].copy()

    suffix_opts = sorted([s for s in scoped4["_suffix"].dropna().unique().tolist() if safe_strip(s)])
    chosen_suffix = st.selectbox("Tube suffix (pulldown)", ["(select)"] + suffix_opts, key="use_ln_suffix")

    # Current matching record(s): SHOW TubeAmount
    match_df = scoped4.copy()
    if chosen_suffix != "(select)":
        match_df = match_df[match_df["_suffix"] == safe_strip(chosen_suffix)].copy()
    else:
        match_df = match_df.iloc[0:0].copy()

    st.markdown("**Current matching record(s): (SHOW TubeAmount)**")
    if match_df.empty:
        st.info("No matching record yet. Select TankID → BoxLabel_group → BoxID → Prefix → Tube suffix.")
    else:
        show_cols = [c for c in [TANK_COL, RACK_COL, BOX_COL, BOXID_COL, BOXUID_COL, TUBE_COL, AMT_COL, MEMO_COL] if c in match_df.columns]
        st.dataframe(match_df[show_cols], use_container_width=True, hide_index=True)

    # Submit usage
    with st.form("submit_ln_usage"):
        st.markdown("**User inputs (LN): Use, User, ShippingTo, Memo**")
        use_amt = st.number_input("Use", min_value=1, step=1, value=1)
        user_initials = st.text_input("User (initials)", placeholder="e.g., JW").strip()
        shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF / Building 3").strip()
        memo_in = st.text_area("Memo (optional)", placeholder="Usage memo...").strip()

        submitted_use = st.form_submit_button("Submit Usage", type="primary")

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
                idx0, cur_amount = find_ln_row_index_by_keys(
                    ln_all_df=ln_all_df,
                    tank_id=chosen_tank,
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

                row_before = ln_all_df.iloc[idx0].copy()

                # Append to Use_log (persist) — header has no RackNumber/BoxUID
                append_row_by_header(
                    service,
                    USE_LOG_TAB,
                    build_use_log_row_from_ln_row(row_before, int(use_amt), user_initials, shipping_to, memo_in, chosen_tank),
                )

                # Update or delete LN3 row
                if new_amount == 0:
                    delete_ln_row_by_index(service, idx0)
                    st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — LN3 row deleted.")
                else:
                    update_ln_tubeamount_by_index(service, idx0, new_amount)
                    st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                # Append to session final report (HIDE TubeAmount; show Use)
                now = datetime.now(NY_TZ)
                ts = f"{(now.strftime('%I:%M:%S').lstrip('0') or now.strftime('%I:%M:%S'))} {now.strftime('%m/%d/%Y')}"
                st.session_state.usage_final_rows.append(
                    build_final_report_row_from_ui(
                        tank_id=chosen_tank,
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
                st.error("Google Sheets API error while logging usage.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to log usage.")
                st.code(str(e), language="text")

    # Final report (session view; hide TubeAmount, show Use)
    st.markdown("### ✅ Final Report (session view; HIDE TubeAmount, show Use)")
    final_cols = [
        "TankID",
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
            file_name="LN_final_report_session.csv",
            mime="text/csv",
            key="download_ln_final_report",
        )

        if st.button("🧹 Clear session final report", key="clear_ln_final_report"):
            st.session_state.usage_final_rows = []
            st.success("Session final report cleared (Use_log remains saved).")
    else:
        st.info("No usage records in this session yet.")
