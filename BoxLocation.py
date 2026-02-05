# BoxLocation.py — Full Streamlit App
# ✅ Box Location (study tabs) + ✅ LN (multi-tank) + ✅ Freezer Inventory + ✅ Use_log
#
# UPDATE (per your request):
# ✅ Global max BoxID = MAX(BoxID) from:
#   - tab 'boxNumber' (explicit numeric column 'BoxID')
#   - tab 'Freezer_Inventory' (explicit numeric column 'BoxID')
# (No parsing from BoxNumber text.)
#
# New tab:
# ✅ Freezer_Inventory
#   Columns include your list PLUS TubeAmount, BoxID, TubePrefix
#
# Use_log:
# ✅ Permanent usage storage
# ✅ Session Final Usage Report (TubeAmount hidden)
#
# Recommended headers:
#   boxNumber (row 1) should include BoxID + BoxNumber at minimum
#   Freezer_Inventory (row 1):
#     FreezerID | Date Collected | Box Number | StudyCode | Samples Received | Missing Samples | Group |
#     Urine Results | All Collected By | TubePrefix | TubeAmount | BoxID | Memo
#   Use_log (row 1):
#     StorageType | StorageID | TankID | RackNumber | BoxNumber | BoxUID | BoxID | TubeNumber | TubePrefix |
#     Use | User | Time_stamp | ShippingTo | Memo
#   LN3 (LN_TAB) recommended (row 1):
#     TankID | RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo | BoxID | QRCodeLink

import re
import urllib.parse
import urllib.request
from datetime import datetime

import pandas as pd
import pytz
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- Page --------------------
st.set_page_config(page_title="Box Location + LN + Freezer", layout="wide")
st.title("📦 Box Location + 🧊 LN Tank + 🧊 Freezer Inventory")

# -------------------- Session State --------------------
if "last_qr_link" not in st.session_state:
    st.session_state.last_qr_link = ""
if "last_qr_uid" not in st.session_state:
    st.session_state.last_qr_uid = ""
if "usage_final_rows" not in st.session_state:
    st.session_state.usage_final_rows = []  # session report (TubeAmount hidden)

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}

BOX_TAB = "boxNumber"  # should contain BoxID + BoxNumber (source of truth for max BoxID)
LN_TAB = "LN3"         # one inventory tab for all LN tanks (recommended: includes TankID column)
FREEZER_TAB = "Freezer_Inventory"
USE_LOG_TAB = "Use_log"

HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL", "NON-DRUG": "NON-DRUG"}

FREEZER_OPTIONS = ["Sammy", "Tom", "Jerry"]
TANK_OPTIONS = ["LN1", "LN2", "LN3"]

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
def safe_strip(x):
    return "" if x is None else str(x).strip()

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
        range=f"'{tab}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    return [safe_strip(x) for x in row1 if safe_strip(x) != ""]

def set_header_if_blank(service, tab: str, header: list):
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:Z1",
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
    if not header:
        raise ValueError(f"{tab} header row is empty.")
    aligned = [data.get(col, "") for col in header]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [aligned]},
    ).execute()

def col_to_a1(col_idx_0based: int) -> str:
    n = col_idx_0based + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def to_int_amount(x, default=0) -> int:
    try:
        s = safe_strip(x)
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

# -------------------- Global max BoxID (explicit BoxID columns only) --------------------
def max_boxid_from_boxid_col(df: pd.DataFrame, col: str = "BoxID") -> int:
    if df is None or df.empty or col not in df.columns:
        return 0
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return 0
    return int(s.max())

def compute_global_max_boxid_from_boxnumber_and_freezer() -> int:
    """
    ✅ Global max BoxID = max(BoxID) from:
      - boxNumber.BoxID
      - Freezer_Inventory.BoxID
    """
    mx = 0

    # boxNumber
    try:
        d = read_tab(BOX_TAB)
        mx = max(mx, max_boxid_from_boxid_col(d, "BoxID"))
    except Exception:
        pass

    # Freezer_Inventory
    try:
        fz = read_tab(FREEZER_TAB)
        mx = max(mx, max_boxid_from_boxid_col(fz, "BoxID"))
    except Exception:
        pass

    return mx

# -------------------- boxNumber map (StudyID -> BoxNumber) --------------------
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

# -------------------- Ensure headers --------------------
def ensure_boxnumber_has_boxid(service):
    hdr = get_header(service, BOX_TAB)
    if not hdr:
        st.warning("boxNumber header is empty. Please add header row (must include BoxID, BoxNumber).")
        return
    if "BoxID" not in hdr:
        st.warning("boxNumber tab is missing column 'BoxID'. Global max BoxID will be wrong until you add BoxID.")
    if "BoxNumber" not in hdr and "Box Number" not in hdr:
        st.warning("boxNumber tab is missing column 'BoxNumber' (or 'Box Number').")

def ensure_ln_header(service):
    required = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "QRCodeLink"]
    recommended = ["TankID", "RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "BoxID", "QRCodeLink"]
    set_header_if_blank(service, LN_TAB, recommended)

    row1 = get_header(service, LN_TAB)
    missing_required = [c for c in required if c not in row1]
    if missing_required:
        st.warning(f"{LN_TAB} header missing REQUIRED columns: {', '.join(missing_required)}")
    if "TankID" not in row1:
        st.warning(f"{LN_TAB} header has no 'TankID' column. Tank filtering will not work (recommended to add TankID).")

def ensure_freezer_header(service):
    expected = [
        "FreezerID",
        "Date Collected",
        "Box Number",
        "StudyCode",
        "Samples Received",
        "Missing Samples",
        "Group",
        "Urine Results",
        "All Collected By",
        "TubePrefix",
        "TubeAmount",
        "BoxID",
        "Memo",
    ]
    set_header_if_blank(service, FREEZER_TAB, expected)

    row1 = get_header(service, FREEZER_TAB)
    missing = [c for c in expected if c not in row1]
    if missing:
        st.warning(f"{FREEZER_TAB} header missing columns: {', '.join(missing)}")

def ensure_use_log_header(service):
    expected = [
        "StorageType",   # "LN" or "Freezer"
        "StorageID",     # LN1/LN2/LN3 or Sammy/Tom/Jerry
        "TankID",        # LN1/LN2/LN3 (optional)
        "RackNumber",
        "BoxNumber",
        "BoxUID",
        "BoxID",
        "TubeNumber",
        "TubePrefix",
        "Use",
        "User",
        "Time_stamp",
        "ShippingTo",
        "Memo",
    ]
    set_header_if_blank(service, USE_LOG_TAB, expected)

    row1 = get_header(service, USE_LOG_TAB)
    missing = [c for c in expected if c not in row1]
    if missing:
        st.warning(
            f"{USE_LOG_TAB} header missing columns: {', '.join(missing)}. "
            "Please add them to row 1 for clean logging."
        )

# -------------------- LN helpers --------------------
def compute_next_boxuid(ln_view_df: pd.DataFrame, tank_id: str, rack: int, hp_hn: str, drug_code: str) -> str:
    tank_id = safe_strip(tank_id).upper()
    prefix = f"{tank_id}-R{int(rack):02d}-{hp_hn}-{drug_code}-"
    max_n = 0

    if ln_view_df is not None and (not ln_view_df.empty) and ("BoxUID" in ln_view_df.columns):
        for v in ln_view_df["BoxUID"].dropna().astype(str):
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

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def cleanup_zero_rows(service, tab: str, df: pd.DataFrame, amount_col: str) -> bool:
    if df is None or df.empty or amount_col not in df.columns:
        return False

    amounts = pd.to_numeric(df[amount_col], errors="coerce").fillna(0).astype(int)
    zero_idxs = [int(i) for i in df.index[amounts == 0].tolist()]
    if not zero_idxs:
        return False

    sheet_id = get_sheet_id(service, tab)
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

def update_cell_by_index(service, tab: str, idx0: int, col_name: str, new_value):
    header = get_header(service, tab)
    if col_name not in header:
        raise ValueError(f"{tab} missing '{col_name}' in header.")
    col_idx = header.index(col_name)
    a1_col = col_to_a1(col_idx)
    sheet_row = idx0 + 2

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!{a1_col}{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[new_value]]},
    ).execute()

def delete_row_by_index(service, tab: str, idx0: int):
    sheet_id = get_sheet_id(service, tab)
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

# -------------------- Use_log builders --------------------
def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    return f"{time_str} {date_str}"

def build_use_log_row_for_ln(row: pd.Series, use_amt: int, user_initials: str, shipping_to: str, tank_id: str) -> dict:
    return {
        "StorageType": "LN",
        "StorageID": safe_strip(tank_id).upper(),
        "TankID": safe_strip(tank_id).upper(),
        "RackNumber": safe_strip(row.get("RackNumber", "")),
        "BoxNumber": safe_strip(row.get("BoxNumber", "")),
        "BoxUID": safe_strip(row.get("BoxUID", "")),
        "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
        "TubeNumber": safe_strip(row.get("TubeNumber", "")),
        "TubePrefix": "",
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": now_timestamp_str(),
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(row.get("Memo", "")),
    }

def build_use_log_row_for_freezer(row: pd.Series, use_amt: int, user_initials: str, shipping_to: str, freezer_id: str) -> dict:
    return {
        "StorageType": "Freezer",
        "StorageID": safe_strip(freezer_id),
        "TankID": "",
        "RackNumber": "",
        "BoxNumber": safe_strip(row.get("Box Number", "")),
        "BoxUID": "",
        "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
        "TubeNumber": "",
        "TubePrefix": safe_strip(row.get("TubePrefix", "")),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": now_timestamp_str(),
        "ShippingTo": safe_strip(shipping_to),
        "Memo": safe_strip(row.get("Memo", "")),
    }

def build_final_report_row(kind: str, row: pd.Series, use_amt: int, storage_id: str) -> dict:
    if kind == "LN":
        return {
            "StorageType": "LN",
            "StorageID": storage_id,
            "RackNumber": safe_strip(row.get("RackNumber", "")),
            "BoxNumber": safe_strip(row.get("BoxNumber", "")),
            "BoxUID": safe_strip(row.get("BoxUID", "")),
            "TubeNumber": safe_strip(row.get("TubeNumber", "")),
            "TubePrefix": "",
            "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
            "Use": int(use_amt),
            "Memo": safe_strip(row.get("Memo", "")),
        }
    else:
        return {
            "StorageType": "Freezer",
            "StorageID": storage_id,
            "RackNumber": "",
            "BoxNumber": safe_strip(row.get("Box Number", "")),
            "BoxUID": "",
            "TubeNumber": "",
            "TubePrefix": safe_strip(row.get("TubePrefix", "")),
            "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
            "Use": int(use_amt),
            "Memo": safe_strip(row.get("Memo", "")),
        }

# ============================================================
# Sidebar (Global Controls)
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select Study", DISPLAY_TABS, index=0)

    STORAGE_TYPE = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)

    if STORAGE_TYPE == "LN Tank":
        selected_tank = st.selectbox("Select LN Tank", TANK_OPTIONS, index=2)
        selected_freezer = None
    else:
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
                service = sheets_service()
                ensure_boxnumber_has_boxid(service)  # warn if BoxID missing in boxNumber

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
# 2) STORAGE MODULE
# ============================================================
st.divider()
st.header("🧊 Storage Inventory")

service = sheets_service()
ensure_use_log_header(service)
ensure_boxnumber_has_boxid(service)

# ---------- Session Final Usage Report ----------
st.subheader("✅ Final Usage Report (session view; permanently saved in Use_log)")
final_cols = ["StorageType", "StorageID", "RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubePrefix", "BoxID", "Use", "Memo"]

if st.session_state.usage_final_rows:
    final_df = pd.DataFrame(st.session_state.usage_final_rows).reindex(columns=final_cols, fill_value="")
    st.dataframe(final_df, use_container_width=True, hide_index=True)
    csv_bytes = final_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download session report CSV",
        data=csv_bytes,
        file_name="final_usage_report_session.csv",
        mime="text/csv",
        key="download_final_usage_report",
    )
    if st.button("🧹 Clear session report", key="clear_final_report"):
        st.session_state.usage_final_rows = []
        st.success("Session report cleared (Use_log remains saved).")
else:
    st.info("No usage records in this session yet.")

st.divider()

# ============================================================
# 2A) LN TANK MODULE
# ============================================================
if STORAGE_TYPE == "LN Tank":
    ensure_ln_header(service)

    # Load full LN sheet, cleanup TubeAmount==0
    try:
        ln_all_df = read_tab(LN_TAB)
    except Exception:
        ln_all_df = pd.DataFrame()

    try:
        if cleanup_zero_rows(service, LN_TAB, ln_all_df, amount_col="TubeAmount"):
            st.info("🧹 Removed LN row(s) where TubeAmount was 0.")
            ln_all_df = read_tab(LN_TAB)
    except Exception as e:
        st.warning(f"LN zero-row cleanup failed: {e}")

    # Filter view for selected tank if TankID exists
    ln_view_df = ln_all_df.copy()
    if ln_view_df is not None and (not ln_view_df.empty) and ("TankID" in ln_view_df.columns):
        ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
        ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()

    st.subheader(f"🧊 LN Inventory ({selected_tank})")

    # -------- Add LN Record --------
    st.markdown("### ➕ Add LN Record")
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

        box_number = f"{hp_hn}-{drug_code}"

        # ✅ Global max BoxID (explicit BoxID) from boxNumber + Freezer_Inventory only
        global_max_boxid = compute_global_max_boxid_from_boxnumber_and_freezer()
        st.caption(f"Global max BoxID (boxNumber.BoxID + Freezer_Inventory.BoxID): {global_max_boxid if global_max_boxid else '(none)'}")

        box_choice = st.radio("BoxID option", ["Use the previous box", "Open a new box"], horizontal=True)
        opened_new_box = (box_choice == "Open a new box")

        if box_choice == "Use the previous box":
            boxid_val = max(global_max_boxid, 1)
            st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)
        else:
            boxid_val = (global_max_boxid + 1) if global_max_boxid >= 0 else 1
            st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)

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
                    "TankID": safe_strip(selected_tank).upper(),
                    "RackNumber": int(rack),
                    "BoxNumber": box_number,
                    "BoxUID": box_uid,
                    "TubeNumber": tube_number,
                    "TubeAmount": int(tube_amount),
                    "Memo": memo,
                    "BoxID": boxid_input,
                    "QRCodeLink": qr_link,
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

            except HttpError as e:
                st.error("Google Sheets API error while writing to LN.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to save LN record")
                st.code(str(e), language="text")

    # Download last QR (outside form)
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

    # Reload + show table
    try:
        ln_all_df = read_tab(LN_TAB)
        ln_view_df = ln_all_df.copy()
        if "TankID" in ln_view_df.columns:
            ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
            ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()
    except Exception:
        ln_view_df = pd.DataFrame()

    st.markdown("### 📋 LN Inventory Table")
    if ln_view_df is None or ln_view_df.empty:
        st.info(f"No records for {selected_tank}.")
    else:
        st.dataframe(ln_view_df, use_container_width=True, hide_index=True)

    st.markdown("### 🔎 Search LN by BoxNumber")
    if ln_view_df is not None and (not ln_view_df.empty) and ("BoxNumber" in ln_view_df.columns):
        opts = sorted([safe_strip(x) for x in ln_view_df["BoxNumber"].dropna().unique().tolist() if safe_strip(x)])
        chosen = st.selectbox("BoxNumber", ["(select)"] + opts, key="ln_search_boxnumber")
        if chosen != "(select)":
            res = ln_view_df[ln_view_df["BoxNumber"].astype(str).map(safe_strip) == chosen].copy()
            st.dataframe(res, use_container_width=True, hide_index=True)

    # Usage log for LN
    st.markdown("### 📉 Log LN Usage (subtract TubeAmount, delete if 0, save to Use_log)")
    if ln_view_df is None or ln_view_df.empty:
        st.info("No LN records to use.")
    else:
        box_opts = sorted([safe_strip(x) for x in ln_view_df["BoxNumber"].dropna().astype(str).tolist() if safe_strip(x)])
        chosen_box = st.selectbox("Select BoxNumber", ["(select)"] + sorted(set(box_opts)), key="ln_use_box")
        chosen_tube = "(select)"
        chosen_uid = ""

        if chosen_box != "(select)":
            sub = ln_view_df.copy()
            sub["BoxNumber"] = sub["BoxNumber"].astype(str).map(safe_strip)
            sub["TubeNumber"] = sub["TubeNumber"].astype(str).map(safe_strip)
            sub = sub[sub["BoxNumber"] == safe_strip(chosen_box)].copy()

            tube_opts = sorted([safe_strip(x) for x in sub["TubeNumber"].dropna().astype(str).tolist() if safe_strip(x)])
            chosen_tube = st.selectbox("Select TubeNumber", ["(select)"] + sorted(set(tube_opts)), key="ln_use_tube")

            if chosen_tube != "(select)" and "BoxUID" in sub.columns:
                sub2 = sub[sub["TubeNumber"] == safe_strip(chosen_tube)].copy()
                if len(sub2) > 1:
                    sub2["BoxUID"] = sub2["BoxUID"].astype(str).map(safe_strip)
                    uid_opts = sorted([x for x in sub2["BoxUID"].dropna().tolist() if safe_strip(x)])
                    chosen_uid = st.selectbox("Multiple matches found. Select BoxUID", ["(select)"] + uid_opts, key="ln_use_uid")
                    if chosen_uid == "(select)":
                        chosen_uid = ""

            if chosen_tube != "(select)":
                show = sub[sub["TubeNumber"] == safe_strip(chosen_tube)].copy()
                if chosen_uid and "BoxUID" in show.columns:
                    show["BoxUID"] = show["BoxUID"].astype(str).map(safe_strip)
                    show = show[show["BoxUID"] == safe_strip(chosen_uid)].copy()
                st.markdown("**Current matching record(s):**")
                st.dataframe(show, use_container_width=True, hide_index=True)

        with st.form("ln_use_form"):
            user_initials = st.text_input("Your initials (User)", placeholder="e.g., JW").strip()
            shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF").strip()
            use_amt = st.number_input("Use", min_value=0, step=1, value=1)
            submitted_use = st.form_submit_button("Submit Usage", type="primary")

            if submitted_use:
                if chosen_box == "(select)" or chosen_tube == "(select)":
                    st.error("Please select BoxNumber and TubeNumber.")
                    st.stop()
                if use_amt <= 0:
                    st.error("Use must be > 0.")
                    st.stop()
                if not user_initials:
                    st.error("Please enter your initials (User).")
                    st.stop()
                if not shipping_to:
                    st.error("Please enter ShippingTo.")
                    st.stop()

                try:
                    ln_all_df = read_tab(LN_TAB)
                except Exception:
                    ln_all_df = pd.DataFrame()

                df0 = ln_all_df.copy()
                if df0.empty or "BoxNumber" not in df0.columns or "TubeNumber" not in df0.columns or "TubeAmount" not in df0.columns:
                    st.error(f"{LN_TAB} must include BoxNumber, TubeNumber, TubeAmount.")
                    st.stop()

                df0["BoxNumber"] = df0["BoxNumber"].astype(str).map(safe_strip)
                df0["TubeNumber"] = df0["TubeNumber"].astype(str).map(safe_strip)

                if "TankID" in df0.columns:
                    df0["TankID"] = df0["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
                    mask = (df0["TankID"] == safe_strip(selected_tank).upper())
                else:
                    mask = pd.Series([True] * len(df0))

                mask = mask & (df0["BoxNumber"] == safe_strip(chosen_box)) & (df0["TubeNumber"] == safe_strip(chosen_tube))

                if chosen_uid and "BoxUID" in df0.columns:
                    df0["BoxUID"] = df0["BoxUID"].astype(str).map(safe_strip)
                    mask = mask & (df0["BoxUID"] == safe_strip(chosen_uid))

                hits = df0[mask]
                if hits.empty:
                    st.error("No matching LN row found.")
                    st.stop()

                idx0 = int(hits.index[0])
                row_before = ln_all_df.iloc[idx0].copy()
                cur_amount = to_int_amount(row_before.get("TubeAmount", 0), default=0)

                new_amount = cur_amount - int(use_amt)
                if new_amount < 0:
                    st.error(f"Not enough stock. Current TubeAmount={cur_amount}, Use={int(use_amt)}")
                    st.stop()

                append_row_by_header(
                    service,
                    USE_LOG_TAB,
                    build_use_log_row_for_ln(row_before, int(use_amt), user_initials, shipping_to, selected_tank),
                )

                if new_amount == 0:
                    delete_row_by_index(service, LN_TAB, idx0)
                    st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — LN row deleted.")
                else:
                    update_cell_by_index(service, LN_TAB, idx0, "TubeAmount", int(new_amount))
                    st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                st.session_state.usage_final_rows.append(build_final_report_row("LN", row_before, int(use_amt), storage_id=selected_tank))
                st.rerun()

# ============================================================
# 2B) FREEZER MODULE
# ============================================================
else:
    ensure_freezer_header(service)

    st.subheader(f"🧊 Freezer Inventory ({selected_freezer})")

    try:
        fz_all_df = read_tab(FREEZER_TAB)
    except Exception:
        fz_all_df = pd.DataFrame()

    try:
        if cleanup_zero_rows(service, FREEZER_TAB, fz_all_df, amount_col="TubeAmount"):
            st.info("🧹 Removed Freezer row(s) where TubeAmount was 0.")
            fz_all_df = read_tab(FREEZER_TAB)
    except Exception as e:
        st.warning(f"Freezer zero-row cleanup failed: {e}")

    fz_view_df = fz_all_df.copy()
    if fz_view_df is not None and (not fz_view_df.empty) and ("FreezerID" in fz_view_df.columns):
        fz_view_df["FreezerID"] = fz_view_df["FreezerID"].astype(str).map(safe_strip)
        fz_view_df = fz_view_df[fz_view_df["FreezerID"] == safe_strip(selected_freezer)].copy()

    st.markdown("### ➕ Add Freezer Record")
    with st.form("freezer_add", clear_on_submit=True):
        freezer_id = selected_freezer

        date_collected = st.date_input("Date Collected")
        box_number_str = st.text_input("Box Number (string for search)", placeholder="e.g., AD-BOX-001").strip()
        study_code = st.text_input("StudyCode", placeholder="e.g., AD").strip()

        c1, c2 = st.columns(2)
        with c1:
            samples_received = st.number_input("Samples Received", min_value=0, step=1, value=0)
        with c2:
            missing_samples = st.number_input("Missing Samples", min_value=0, step=1, value=0)

        group = st.text_input("Group", placeholder="e.g., Control / AD / HIV+").strip()
        urine_results = st.text_input("Urine Results", placeholder="optional").strip()
        all_collected_by = st.text_input("All Collected By", placeholder="initials / name").strip()

        tube_prefix = st.text_input("TubePrefix", placeholder="e.g., Serum / DNA / ADTU").strip()
        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)

        memo = st.text_area("Memo (optional)")

        # ✅ Global max BoxID (explicit BoxID) from boxNumber + Freezer_Inventory only
        global_max_boxid = compute_global_max_boxid_from_boxnumber_and_freezer()
        st.caption(f"Global max BoxID (boxNumber.BoxID + Freezer_Inventory.BoxID): {global_max_boxid if global_max_boxid else '(none)'}")

        box_choice = st.radio("BoxID option", ["Use the previous box", "Open a new box"], horizontal=True)
        opened_new_box = (box_choice == "Open a new box")
        if box_choice == "Use the previous box":
            boxid_val = max(global_max_boxid, 1)
            st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)
        else:
            boxid_val = (global_max_boxid + 1) if global_max_boxid >= 0 else 1
            st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)

        boxid_input = str(int(boxid_val))

        submitted_fz = st.form_submit_button("Save to Freezer", type="primary")

        if submitted_fz:
            if not box_number_str:
                st.error("Box Number is required.")
                st.stop()
            if not study_code:
                st.error("StudyCode is required.")
                st.stop()
            if not tube_prefix:
                st.error("TubePrefix is required.")
                st.stop()

            try:
                data = {
                    "FreezerID": freezer_id,
                    "Date Collected": date_collected.strftime("%m/%d/%Y"),
                    "Box Number": box_number_str,
                    "StudyCode": study_code,
                    "Samples Received": int(samples_received),
                    "Missing Samples": int(missing_samples),
                    "Group": group,
                    "Urine Results": urine_results,
                    "All Collected By": all_collected_by,
                    "TubePrefix": tube_prefix,
                    "TubeAmount": int(tube_amount),
                    "BoxID": boxid_input,
                    "Memo": memo,
                }
                append_row_by_header(service, FREEZER_TAB, data)
                st.success("Saved ✅ Freezer record added.")

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

            except HttpError as e:
                st.error("Google Sheets API error while writing to Freezer_Inventory.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to save freezer record")
                st.code(str(e), language="text")

    try:
        fz_all_df = read_tab(FREEZER_TAB)
        fz_view_df = fz_all_df.copy()
        if "FreezerID" in fz_view_df.columns:
            fz_view_df["FreezerID"] = fz_view_df["FreezerID"].astype(str).map(safe_strip)
            fz_view_df = fz_view_df[fz_view_df["FreezerID"] == safe_strip(selected_freezer)].copy()
    except Exception:
        fz_view_df = pd.DataFrame()

    st.markdown("### 📋 Freezer Inventory Table")
    if fz_view_df is None or fz_view_df.empty:
        st.info("No freezer records yet.")
    else:
        st.dataframe(fz_view_df, use_container_width=True, hide_index=True)

    st.markdown("### 🔎 Search Freezer by Box Number")
    if fz_view_df is not None and (not fz_view_df.empty) and ("Box Number" in fz_view_df.columns):
        opts = sorted([safe_strip(x) for x in fz_view_df["Box Number"].dropna().unique().tolist() if safe_strip(x)])
        chosen_box = st.selectbox("Box Number", ["(select)"] + opts, key="fz_search_boxnumber")
        if chosen_box != "(select)":
            res = fz_view_df[fz_view_df["Box Number"].astype(str).map(safe_strip) == safe_strip(chosen_box)].copy()
            st.dataframe(res, use_container_width=True, hide_index=True)

    st.markdown("### 📉 Log Freezer Usage (subtract TubeAmount, delete if 0, save to Use_log)")
    if fz_view_df is None or fz_view_df.empty:
        st.info("No freezer records to use.")
    else:
        opts = sorted([safe_strip(x) for x in fz_view_df["Box Number"].dropna().unique().tolist() if safe_strip(x)])
        use_box = st.selectbox("Select Box Number", ["(select)"] + opts, key="fz_use_box")

        if use_box != "(select)":
            cur_rows = fz_view_df[fz_view_df["Box Number"].astype(str).map(safe_strip) == safe_strip(use_box)].copy()
            st.markdown("**Current matching record(s):**")
            st.dataframe(cur_rows, use_container_width=True, hide_index=True)

        with st.form("fz_use_form"):
            user_initials = st.text_input("Your initials (User)", placeholder="e.g., JW").strip()
            shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF").strip()
            use_amt = st.number_input("Use", min_value=0, step=1, value=1)
            submitted_use = st.form_submit_button("Submit Usage", type="primary")

            if submitted_use:
                if use_box == "(select)":
                    st.error("Please select a Box Number.")
                    st.stop()
                if use_amt <= 0:
                    st.error("Use must be > 0.")
                    st.stop()
                if not user_initials:
                    st.error("Please enter your initials (User).")
                    st.stop()
                if not shipping_to:
                    st.error("Please enter ShippingTo.")
                    st.stop()

                try:
                    fz_all_df = read_tab(FREEZER_TAB)
                except Exception:
                    fz_all_df = pd.DataFrame()

                df0 = fz_all_df.copy()
                if df0.empty or "FreezerID" not in df0.columns or "Box Number" not in df0.columns or "TubeAmount" not in df0.columns:
                    st.error(f"{FREEZER_TAB} must include FreezerID, Box Number, TubeAmount.")
                    st.stop()

                df0["FreezerID"] = df0["FreezerID"].astype(str).map(safe_strip)
                df0["Box Number"] = df0["Box Number"].astype(str).map(safe_strip)

                mask = (df0["FreezerID"] == safe_strip(selected_freezer)) & (df0["Box Number"] == safe_strip(use_box))
                hits = df0[mask]
                if hits.empty:
                    st.error("No matching Freezer row found.")
                    st.stop()

                idx0 = int(hits.index[0])
                row_before = fz_all_df.iloc[idx0].copy()
                cur_amount = to_int_amount(row_before.get("TubeAmount", 0), default=0)

                new_amount = cur_amount - int(use_amt)
                if new_amount < 0:
                    st.error(f"Not enough stock. Current TubeAmount={cur_amount}, Use={int(use_amt)}")
                    st.stop()

                append_row_by_header(
                    service,
                    USE_LOG_TAB,
                    build_use_log_row_for_freezer(row_before, int(use_amt), user_initials, shipping_to, selected_freezer),
                )

                if new_amount == 0:
                    delete_row_by_index(service, FREEZER_TAB, idx0)
                    st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — Freezer row deleted.")
                else:
                    update_cell_by_index(service, FREEZER_TAB, idx0, "TubeAmount", int(new_amount))
                    st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                st.session_state.usage_final_rows.append(build_final_report_row("Freezer", row_before, int(use_amt), storage_id=selected_freezer))
                st.rerun()

# ============================================================
# 3) Use_log viewer
# ============================================================
st.divider()
st.subheader("🧾 Use_log (permanent saved usage records)")
try:
    use_log_df = read_tab(USE_LOG_TAB)
    if use_log_df.empty:
        st.info("Use_log is empty.")
    else:
        st.dataframe(use_log_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Unable to read Use_log: {e}")
