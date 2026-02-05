# BoxLocation.py — Complete Streamlit App
# 📦 Box Location (study tabs) + 🧊 LN Tank (LN1/LN2/LN3) + 🧊 Freezer Inventory + 🧾 Use_log

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

# -------------------- Page Config --------------------
st.set_page_config(page_title="Box Location + LN + Freezer", layout="wide")
st.title("📦 Box Location + 🧊 LN Tank + 🧊 Freezer Inventory")

# -------------------- Session State --------------------
if "last_qr_link" not in st.session_state:
    st.session_state.last_qr_link = ""
if "last_qr_uid" not in st.session_state:
    st.session_state.last_qr_uid = ""
if "usage_final_rows" not in st.session_state:
    st.session_state.usage_final_rows = []
if "user_name" not in st.session_state:
    st.session_state.user_name = ""

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN_TAB = "LN3"  # one sheet for LN1/LN2/LN3 (filtered by TankID)
FREEZER_TAB = "Freezer_Inventory"
USE_LOG_TAB = "Use_log"

HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL", "NON-DRUG": "NON-DRUG"}
FREEZER_OPTIONS = ["Sammy", "Tom", "Jerry"]
TANK_OPTIONS = ["LN1", "LN2", "LN3"]
QR_PX = 118

SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]
NY_TZ = pytz.timezone("America/New_York")

# -------------------- Google Sheets Service --------------------
@st.cache_resource(show_spinner=False)
def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]), scopes=scopes
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# -------------------- Helper Functions --------------------
def safe_strip(x):
    return "" if x is None else str(x).strip()

def read_tab(tab_name: str) -> pd.DataFrame:
    svc = sheets_service()
    try:
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
            r = list(r) + [""] * (n - len(r)) if len(r) < n else r[:n]
            fixed.append(r)
        return pd.DataFrame(fixed, columns=header)
    except Exception as e:
        st.error(f"Failed to read tab '{tab_name}': {str(e)}")
        return pd.DataFrame()

def get_header(service, tab: str) -> list:
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    return [safe_strip(x) for x in (resp.get("values", [[]]) or [[]])[0]]

def set_header_if_blank(service, tab: str, header: list):
    row1 = get_header(service, tab)
    if not row1 or all(safe_strip(x) == "" for x in row1):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_row_by_header(service, tab: str, data: dict):
    header = [safe_strip(x) for x in get_header(service, tab)]
    if not header or all(h == "" for h in header):
        raise ValueError(f"Header row is empty in tab: {tab}")
    aligned = [data.get(col, "") for col in header]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [aligned]},
    ).execute()

def col_to_a1(col_idx: int) -> str:
    n = col_idx + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def to_int_amount(x, default=0) -> int:
    try:
        s = safe_strip(x)
        return int(float(s)) if s else default
    except:
        return default

# -------------------- BoxID Logic (Source of Truth: boxNumber + Freezer_Inventory) --------------------
def max_boxid_from_boxid_col(df: pd.DataFrame, col: str = "BoxID") -> int:
    if df.empty or col not in df.columns:
        return 0
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return int(s.max()) if not s.empty else 0

def compute_current_max_boxid() -> int:
    mx = 0
    for tab in [BOX_TAB, FREEZER_TAB]:
        try:
            df = read_tab(tab)
            mx = max(mx, max_boxid_from_boxid_col(df))
        except:
            pass
    return mx

def locked_boxid_from_choice(choice: str) -> tuple[int, bool, int]:
    current_max = compute_current_max_boxid()
    opened_new = (choice == "Open a new box")
    boxid = current_max if not opened_new else current_max + 1
    if boxid == 0:
        boxid = 1
    return boxid, opened_new, current_max

def green_boxid_reminder(boxid: int):
    st.markdown(
        f"""
        <div style="padding:16px; border-radius:8px; background:#e8f5e9; border:1px solid #2e7d32; margin:12px 0;">
            <strong style="color:#2e7d32; font-size:1.3em;">New Box Created – Please Label:</strong><br><br>
            BoxID = <span style="font-size:1.6em; font-weight:bold; color:#2e7d32;">{boxid}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -------------------- Box Map (StudyID → BoxNumber) --------------------
def build_box_map() -> dict:
    df = read_tab(BOX_TAB)
    if df.empty:
        return {}
    study_col = next((c for c in ["StudyID", "Study ID", "ID"] if c in df.columns), None)
    box_col = next((c for c in ["BoxNumber", "Box Number", "Box", "Box#"] if c in df.columns), None)
    if not study_col or not box_col:
        return {}
    return {
        safe_strip(r.get(study_col, "")).upper(): safe_strip(r.get(box_col, ""))
        for _, r in df.iterrows() if safe_strip(r.get(study_col, ""))
    }

# -------------------- Header Setup --------------------
def ensure_ln_header(service):
    header = ["TankID", "RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "BoxID", "QRCodeLink"]
    set_header_if_blank(service, LN_TAB, header)

def ensure_freezer_header(service):
    header = [
        "FreezerID", "Date Collected", "Box Number", "StudyCode", "Samples Received",
        "Missing Samples", "Group", "Urine Results", "All Collected By",
        "TubePrefix", "TubeAmount", "BoxID", "Memo"
    ]
    set_header_if_blank(service, FREEZER_TAB, header)

def ensure_use_log_header(service):
    header = [
        "StorageType", "StorageID", "TankID", "RackNumber", "BoxNumber", "BoxUID", "BoxID",
        "TubeNumber", "TubePrefix", "Use", "User", "Time_stamp", "ShippingTo", "Memo"
    ]
    set_header_if_blank(service, USE_LOG_TAB, header)

# -------------------- LN Helpers --------------------
def compute_next_boxuid(ln_df: pd.DataFrame, tank: str, rack: int, hp_hn: str, drug_code: str) -> str:
    prefix = f"{tank.upper()}-R{rack:02d}-{hp_hn}-{drug_code}-"
    max_seq = 0
    if not ln_df.empty and "BoxUID" in ln_df.columns:
        for uid in ln_df["BoxUID"].dropna().astype(str):
            if uid.startswith(prefix) and re.search(r"-(\d{2})$", uid):
                try:
                    seq = int(uid.split("-")[-1])
                    max_seq = max(max_seq, seq)
                except:
                    pass
    next_seq = max_seq + 1
    if next_seq > 99:
        raise ValueError(f"BoxUID sequence exceeded 99 for prefix: {prefix}")
    return f"{prefix}{next_seq:02d}"

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def cleanup_zero_rows(service, tab: str, df: pd.DataFrame, amount_col: str) -> bool:
    if df.empty or amount_col not in df.columns:
        return False
    zero_rows = df.index[pd.to_numeric(df[amount_col], errors='coerce').fillna(0).astype(int) == 0].tolist()
    if not zero_rows:
        return False
    sheet_id = get_sheet_id(service, tab)
    requests = [
        {
            "deleteDimension": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": i + 1, "endIndex": i + 2}
            }
        }
        for i in sorted(zero_rows, reverse=True)
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests}
    ).execute()
    return True

# -------------------- Timestamp --------------------
def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    return now.strftime("%-I:%M:%S %p  %m/%d/%Y")

# -------------------- Use Log Row --------------------
def build_use_log_row(**kwargs) -> dict:
    return {
        "StorageType": kwargs.get("StorageType", ""),
        "StorageID": kwargs.get("StorageID", ""),
        "TankID": kwargs.get("TankID", ""),
        "RackNumber": kwargs.get("RackNumber", ""),
        "BoxNumber": kwargs.get("BoxNumber", ""),
        "BoxUID": kwargs.get("BoxUID", ""),
        "BoxID": kwargs.get("BoxID", ""),
        "TubeNumber": kwargs.get("TubeNumber", ""),
        "TubePrefix": kwargs.get("TubePrefix", ""),
        "Use": kwargs.get("Use", ""),
        "User": kwargs.get("User", ""),
        "Time_stamp": kwargs.get("Time_stamp", now_timestamp_str()),
        "ShippingTo": kwargs.get("ShippingTo", ""),
        "Memo": kwargs.get("Memo", ""),
    }

# -------------------- Sidebar --------------------
with st.sidebar:
    st.subheader("Global Controls")
    user_name = st.text_input("Your Name / Initials", value=st.session_state.user_name)
    if user_name:
        st.session_state.user_name = user_name.strip()

    st.divider()
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select Study", DISPLAY_TABS, index=0)
    storage_type = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)
    if storage_type == "LN Tank":
        selected_tank = st.selectbox("LN Tank", TANK_OPTIONS, index=2)
        selected_freezer = None
    else:
        selected_freezer = st.selectbox("Freezer", FREEZER_OPTIONS, index=0)
        selected_tank = None

# -------------------- Main Content --------------------
service = sheets_service()
ensure_use_log_header(service)

# ────────────────────────────────────────────────
# 1. BOX LOCATION SECTION
# ────────────────────────────────────────────────
st.header("📦 Box Location")
tab_name = TAB_MAP[selected_display_tab]
try:
    df = read_tab(tab_name)
    if df.empty:
        st.info(f"No data in tab: {selected_display_tab}")
    else:
        st.subheader(f"Current data: {selected_display_tab}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("StudyID → Box Number Lookup")
        box_map = build_box_map()
        studyids = sorted(set(
            safe_strip(s).upper() for s in df.get("StudyID", pd.Series()).dropna()
            if safe_strip(s)
        ))
        selected_study = st.selectbox("Select StudyID", ["(none)"] + studyids)
        if selected_study != "(none)":
            box_num = box_map.get(selected_study, "")
            if box_num:
                st.success(f"**Box Number:** {box_num}")
            else:
                st.error("Not found in boxNumber tab")
except Exception as e:
    st.error(f"Error loading Box Location: {str(e)}")

# ────────────────────────────────────────────────
# 2. STORAGE INVENTORY + FINAL USAGE REPORT
# ────────────────────────────────────────────────
st.divider()
st.header("🧊 Storage & Usage")

st.subheader("✅ Final Usage Report (this session)")
final_cols = [
    "StorageType", "StorageID", "TankID", "RackNumber", "BoxNumber", "BoxUID", "BoxID",
    "TubeNumber", "TubePrefix", "Use", "User", "Time_stamp", "ShippingTo", "Memo"
]

if st.session_state.usage_final_rows:
    final_df = pd.DataFrame(st.session_state.usage_final_rows)
    st.dataframe(final_df.reindex(columns=final_cols, fill_value=""), use_container_width=True)
    st.download_button(
        "Download Session Report (CSV)",
        final_df.to_csv(index=False).encode("utf-8"),
        "session_usage_report.csv",
        "text/csv",
    )
    if st.button("Clear Session Report", type="secondary"):
        st.session_state.usage_final_rows = []
        st.rerun()
else:
    st.info("No usage records added in this session yet.")

st.divider()

# ────────────────────────────────────────────────
# 2A. LN TANK SECTION
# ────────────────────────────────────────────────
if storage_type == "LN Tank":
    ensure_ln_header(service)

    ln_all = read_tab(LN_TAB)
    if cleanup_zero_rows(service, LN_TAB, ln_all, "TubeAmount"):
        ln_all = read_tab(LN_TAB)
        st.success("Removed rows with TubeAmount = 0")

    ln_view = ln_all[ln_all["TankID"].astype(str).str.upper() == selected_tank.upper()] \
        if "TankID" in ln_all.columns else ln_all

    st.subheader(f"LN Tank Inventory — {selected_tank}")

    # Add new record
    with st.form("add_ln_record", clear_on_submit=True):
        st.markdown("### Add New LN Box / Tubes")
        rack = st.selectbox("Rack", range(1, 7), index=0)
        col1, col2 = st.columns(2)
        with col1:
            hiv = st.selectbox("HIV Status", ["HIV+", "HIV-"])
        with col2:
            drug = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly", "NON-DRUG"])

        hp_hn = HIV_CODE[hiv]
        drug_code = DRUG_CODE[drug]
        box_number = f"{hp_hn}-{drug_code}"

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid, is_new_box, current_max = locked_boxid_from_choice(box_choice)

        st.caption(f"Current highest BoxID (source: boxNumber + Freezer_Inventory): **{current_max or '—'}**")
        st.text_input("BoxID (locked)", str(boxid), disabled=True)

        col3, col4 = st.columns(2)
        with col3:
            prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"])
        with col4:
            tube_input = st.text_input("Tube Number", placeholder="e.g. 02 036").strip()

        tube_number = f"{prefix} {tube_input}" if tube_input else ""
        tube_amount = st.number_input("Number of Tubes", min_value=0, value=1, step=1)
        memo = st.text_area("Memo / Notes", height=80)

        preview_uid = ""
        try:
            preview_uid = compute_next_boxuid(ln_view, selected_tank, rack, hp_hn, drug_code)
            st.info(f"**Next BoxUID:** {preview_uid}")
            qr_url = qr_link_for_boxuid(preview_uid)
            st.image(qr_url, width=QR_PX)
        except Exception as e:
            st.error(f"Cannot preview BoxUID: {e}")

        submitted = st.form_submit_button("Save to LN Tank", type="primary", use_container_width=True)

        if submitted:
            if not tube_input:
                st.error("Tube number input is required.")
            else:
                try:
                    box_uid = compute_next_boxuid(ln_view, selected_tank, rack, hp_hn, drug_code)
                    qr_url = qr_link_for_boxuid(box_uid)

                    row_data = {
                        "TankID": selected_tank.upper(),
                        "RackNumber": rack,
                        "BoxNumber": box_number,
                        "BoxUID": box_uid,
                        "TubeNumber": tube_number,
                        "TubeAmount": tube_amount,
                        "Memo": memo,
                        "BoxID": str(boxid),
                        "QRCodeLink": qr_url,
                    }
                    append_row_by_header(service, LN_TAB, row_data)
                    st.success(f"Added: {box_uid} (BoxID {boxid})")

                    st.session_state.last_qr_link = qr_url
                    st.session_state.last_qr_uid = box_uid

                    if is_new_box:
                        green_boxid_reminder(boxid)

                except Exception as e:
                    st.error(f"Failed to save LN record: {str(e)}")

    # Show last QR download
    if st.session_state.last_qr_link:
        try:
            png = fetch_bytes(st.session_state.last_qr_link)
            st.download_button(
                "Download Last QR Code",
                png,
                f"{st.session_state.last_qr_uid}.png",
                "image/png"
            )
        except:
            st.warning("Could not prepare QR download")

    # Show current view
    st.subheader(f"Current {selected_tank} Inventory")
    if ln_view.empty:
        st.info("No records yet.")
    else:
        st.dataframe(ln_view, use_container_width=True, hide_index=True)

# ────────────────────────────────────────────────
# 2B. FREEZER SECTION
# ────────────────────────────────────────────────
else:
    ensure_freezer_header(service)

    fz_all = read_tab(FREEZER_TAB)
    if cleanup_zero_rows(service, FREEZER_TAB, fz_all, "TubeAmount"):
        fz_all = read_tab(FREEZER_TAB)
        st.success("Removed rows with TubeAmount = 0")

    fz_view = fz_all[fz_all["FreezerID"].astype(str).str.strip() == selected_freezer.strip()] \
        if "FreezerID" in fz_all.columns else fz_all

    st.subheader(f"Freezer Inventory — {selected_freezer}")

    with st.form("add_freezer_record", clear_on_submit=True):
        st.markdown("### Add New Freezer Box / Tubes")

        date_collected = st.date_input("Date Collected", datetime.now(NY_TZ).date())
        box_number = st.text_input("Box Number", placeholder="e.g. AD-BOX-001").strip()
        study_code = st.text_input("Study Code", placeholder="e.g. AD").strip()

        col1, col2 = st.columns(2)
        with col1:
            received = st.number_input("Samples Received", min_value=0, value=0)
        with col2:
            missing = st.number_input("Missing Samples", min_value=0, value=0)

        group = st.text_input("Group").strip()
        urine = st.text_input("Urine Results").strip()
        collected_by = st.text_input("Collected By").strip()
        tube_prefix = st.text_input("Tube Prefix", placeholder="Serum / DNA / etc").strip()
        tube_amount = st.number_input("Number of Tubes", min_value=0, value=1, step=1)
        memo = st.text_area("Memo / Notes", height=80)

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid, is_new_box, current_max = locked_boxid_from_choice(box_choice)

        st.caption(f"Current highest BoxID: **{current_max or '—'}**")
        st.text_input("BoxID (locked)", str(boxid), disabled=True)

        submitted = st.form_submit_button("Save to Freezer", type="primary", use_container_width=True)

        if submitted:
            if not box_number or not study_code or not tube_prefix:
                st.error("Box Number, Study Code, and Tube Prefix are required.")
            else:
                try:
                    row_data = {
                        "FreezerID": selected_freezer,
                        "Date Collected": date_collected.strftime("%m/%d/%Y"),
                        "Box Number": box_number,
                        "StudyCode": study_code,
                        "Samples Received": received,
                        "Missing Samples": missing,
                        "Group": group,
                        "Urine Results": urine,
                        "All Collected By": collected_by,
                        "TubePrefix": tube_prefix,
                        "TubeAmount": tube_amount,
                        "BoxID": str(boxid),
                        "Memo": memo,
                    }
                    append_row_by_header(service, FREEZER_TAB, row_data)
                    st.success(f"Added freezer record (BoxID {boxid})")

                    if is_new_box:
                        green_boxid_reminder(boxid)

                except Exception as e:
                    st.error(f"Failed to save freezer record: {str(e)}")

    st.subheader(f"Current {selected_freezer} Inventory")
    if fz_view.empty:
        st.info("No records yet.")
    else:
        st.dataframe(fz_view, use_container_width=True, hide_index=True)

# ────────────────────────────────────────────────
# 3. USE_LOG VIEWER
# ────────────────────────────────────────────────
st.divider()
st.subheader("🧾 Permanent Use Log (all historical usage)")
try:
    use_log = read_tab(USE_LOG_TAB)
    if use_log.empty:
        st.info("Use_log is empty.")
    else:
        st.dataframe(use_log, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Could not load Use_log: {str(e)}")
