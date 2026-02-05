# BoxLocation.py — Complete Streamlit App
# 📦 Box Location (study tabs) + 🧊 LN Tank (LN1/LN2/LN3) + 🧊 Freezer Inventory + 🧾 Use_log
#
# ✅ BoxID behavior (LOCKED) — source of truth:
#   current max BoxID = MAX(BoxID) from:
#     - tab 'boxNumber'
#     - tab 'Freezer_Inventory'
#
#   Use previous box → BoxID = current max (locked)
#   Open a new box   → BoxID = max + 1 (locked) + show green reminder (after save)
#
# ✅ Date Collected defaults to today()
# ✅ Use_log permanently stores final usage report
# ✅ Session Final Usage Report view (TubeAmount hidden)

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
    st.session_state.usage_final_rows = []

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}

BOX_TAB = "boxNumber"
LN_TAB = "LN3"  # one sheet that stores LN1/LN2/LN3 (requires TankID column to filter)
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
    return [safe_strip(x) for x in row1]

def set_header_if_blank(service, tab: str, header: list):
    row1 = get_header(service, tab)
    if (not row1) or all(safe_strip(x) == "" for x in row1):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_row_by_header(service, tab: str, data: dict):
    header = [safe_strip(x) for x in get_header(service, tab)]
    if not header or all(h == "" for h in header):
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

# -------------------- BoxID logic (SOURCE OF TRUTH) --------------------
def max_boxid_from_boxid_col(df: pd.DataFrame, col: str = "BoxID") -> int:
    if df is None or df.empty or col not in df.columns:
        return 0
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return 0
    return int(s.max())

def compute_current_max_boxid() -> int:
    """
    ✅ current max BoxID = MAX(BoxID) from:
      - boxNumber
      - Freezer_Inventory
    """
    mx = 0
    try:
        d = read_tab(BOX_TAB)
        mx = max(mx, max_boxid_from_boxid_col(d, "BoxID"))
    except Exception:
        pass
    try:
        fz = read_tab(FREEZER_TAB)
        mx = max(mx, max_boxid_from_boxid_col(fz, "BoxID"))
    except Exception:
        pass
    return int(mx)

def locked_boxid_from_choice(choice: str) -> tuple[int, bool, int]:
    """
    Returns (boxid_val, opened_new_box, current_max)
      - Use previous box → BoxID = current max (locked)
      - Open a new box   → BoxID = max + 1 (locked)
    """
    current_max = compute_current_max_boxid()
    opened_new_box = (choice == "Open a new box")

    if choice == "Use the previous box":
        boxid_val = current_max if current_max > 0 else 1
    else:
        boxid_val = (current_max if current_max > 0 else 0) + 1

    return int(boxid_val), opened_new_box, int(current_max)

def green_boxid_reminder(boxid: int):
    st.markdown(
        f"""
        <div style="padding:12px;border-radius:8px;background-color:#e8f5e9;border:1px solid #2e7d32;font-size:16px;">
          ⚠️ <b>Please mark the box using the updated BoxID.</b><br><br>
          <span style="color:#2e7d32;font-weight:700;font-size:20px;">
            Hint: BoxID = {int(boxid)}
          </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

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
def ensure_ln_header(service):
    header = ["TankID", "RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "BoxID", "QRCodeLink"]
    set_header_if_blank(service, LN_TAB, header)

def ensure_freezer_header(service):
    header = [
        "FreezerID", "Date Collected", "Box Number", "StudyCode", "Samples Received", "Missing Samples",
        "Group", "Urine Results", "All Collected By", "TubePrefix", "TubeAmount", "BoxID", "Memo"
    ]
    set_header_if_blank(service, FREEZER_TAB, header)

def ensure_use_log_header(service):
    header = [
        "StorageType", "StorageID", "TankID", "RackNumber", "BoxNumber", "BoxUID", "BoxID",
        "TubeNumber", "TubePrefix", "Use", "User", "Time_stamp", "ShippingTo", "Memo"
    ]
    set_header_if_blank(service, USE_LOG_TAB, header)

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
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": idx0 + 1, "endIndex": idx0 + 2}
        }
    } for idx0 in zero_idxs]
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
    return True

def update_cell_by_index(service, tab: str, idx0: int, col_name: str, new_value):
    header = [safe_strip(x) for x in get_header(service, tab)]
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
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start, "endIndex": start + 1}
            }
        }]},
    ).execute()

# -------------------- Use_log + Final report --------------------
def now_timestamp_str() -> str:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    return f"{time_str} {date_str}"

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
        "Time_stamp": kwargs.get("Time_stamp", ""),
        "ShippingTo": kwargs.get("ShippingTo", ""),
        "Memo": kwargs.get("Memo", ""),
    }

def add_to_session_final_report(row: dict):
    st.session_state.usage_final_rows.append(row)

# ============================================================
# Sidebar (Global Controls)
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select Study", DISPLAY_TABS, index=0)

    storage_type = st.radio("Storage Type", ["LN Tank", "Freezer"], horizontal=True)

    if storage_type == "LN Tank":
        selected_tank = st.selectbox("Select LN Tank", TANK_OPTIONS, index=2)
        selected_freezer = None
    else:
        selected_freezer = st.selectbox("Select Freezer", FREEZER_OPTIONS, index=0)
        selected_tank = None

    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION
# ============================================================
st.header("📦 Box Location")
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
# 2) STORAGE
# ============================================================
st.divider()
st.header("🧊 Storage Inventory")

service = sheets_service()
ensure_use_log_header(service)

# ---------- Session Final Usage Report ----------
st.subheader("✅ Final Usage Report (session view; permanently saved in Use_log)")
final_cols = [
    "StorageType", "StorageID", "TankID", "RackNumber", "BoxNumber", "BoxUID", "BoxID",
    "TubeNumber", "TubePrefix", "Use", "User", "Time_stamp", "ShippingTo", "Memo"
]

if st.session_state.usage_final_rows:
    final_df = pd.DataFrame(st.session_state.usage_final_rows).reindex(columns=final_cols, fill_value="")
    st.dataframe(final_df, use_container_width=True, hide_index=True)
    st.download_button(
        "⬇️ Download session report CSV",
        data=final_df.to_csv(index=False).encode("utf-8"),
        file_name="final_usage_report_session.csv",
        mime="text/csv",
        key="download_session_report",
    )
    if st.button("🧹 Clear session report"):
        st.session_state.usage_final_rows = []
        st.success("Session report cleared (Use_log remains saved).")
else:
    st.info("No usage records in this session yet.")

st.divider()

# ============================================================
# 2A) LN TANK
# ============================================================
if storage_type == "LN Tank":
    ensure_ln_header(service)

    # Load LN + cleanup TubeAmount==0
    try:
        ln_all_df = read_tab(LN_TAB)
    except Exception:
        ln_all_df = pd.DataFrame()

    try:
        if cleanup_zero_rows(service, LN_TAB, ln_all_df, "TubeAmount"):
            ln_all_df = read_tab(LN_TAB)
            st.info("🧹 Removed LN row(s) where TubeAmount was 0.")
    except Exception as e:
        st.warning(f"LN zero-row cleanup failed: {e}")

    # Filter view by TankID
    ln_view_df = ln_all_df.copy()
    if not ln_view_df.empty and "TankID" in ln_view_df.columns:
        ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
        ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()
    elif not ln_view_df.empty:
        st.warning(f"{LN_TAB} has no TankID column; cannot filter by LN1/LN2/LN3.")

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
        drug_code = DRUG_CODE[drug_group]
        box_number = f"{hp_hn}-{drug_code}"

        # ✅ BoxID locked logic (source of truth: boxNumber + Freezer_Inventory)
        box_choice = st.radio("BoxID option", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid_val, opened_new_box, current_max = locked_boxid_from_choice(box_choice)
        st.caption(f"Current max BoxID (boxNumber + Freezer_Inventory): {current_max if current_max else '(none)'}")
        st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)

        c3, c4 = st.columns(2)
        with c3:
            tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
        with c4:
            tube_input = st.text_input("Tube Input", placeholder="e.g., 02 036").strip()

        tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""
        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
        memo = st.text_area("Memo (optional)")

        # Preview BoxUID + QR
        preview_uid, preview_qr, preview_err = "", "", ""
        try:
            preview_uid = compute_next_boxuid(ln_view_df, selected_tank, rack, hp_hn, drug_code)
            preview_qr = qr_link_for_boxuid(preview_uid)
        except Exception as e:
            preview_err = str(e)

        st.markdown("**BoxUID (auto):**")
        if preview_err:
            st.error(preview_err)
        elif preview_uid:
            st.info(preview_uid)

        st.markdown("**QR Preview:**")
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
                    "BoxID": str(int(boxid_val)),
                    "QRCodeLink": qr_link,
                }
                append_row_by_header(service, LN_TAB, data)
                st.success(f"Saved ✅ {box_uid}")

                st.session_state.last_qr_link = qr_link
                st.session_state.last_qr_uid = box_uid

                # ✅ Green reminder (only when open new box)
                if opened_new_box:
                    green_boxid_reminder(boxid_val)

            except HttpError as e:
                st.error("Google Sheets API error while writing to LN.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to save LN record")
                st.code(str(e), language="text")

    # Download QR outside form
    if st.session_state.last_qr_link:
        try:
            png_bytes = fetch_bytes(st.session_state.last_qr_link)
            st.download_button(
                label="⬇️ Download last saved QR PNG",
                data=png_bytes,
                file_name=f"{st.session_state.last_qr_uid or 'LN'}.png",
                mime="image/png",
                key="download_last_qr",
            )
        except Exception as e:
            st.warning(f"Saved, but QR download failed: {e}")

    # Reload view + table
    try:
        ln_all_df = read_tab(LN_TAB)
        ln_view_df = ln_all_df.copy()
        if not ln_view_df.empty and "TankID" in ln_view_df.columns:
            ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
            ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()
    except Exception:
        ln_view_df = pd.DataFrame()

    st.markdown("### 📋 LN Inventory Table")
    if ln_view_df.empty:
        st.info("No records for this tank yet.")
    else:
        st.dataframe(ln_view_df, use_container_width=True, hide_index=True)

# ============================================================
# 2B) FREEZER
# ============================================================
else:
    ensure_freezer_header(service)

    # Load Freezer + cleanup TubeAmount==0
    try:
        fz_all_df = read_tab(FREEZER_TAB)
    except Exception:
        fz_all_df = pd.DataFrame()

    try:
        if cleanup_zero_rows(service, FREEZER_TAB, fz_all_df, "TubeAmount"):
            fz_all_df = read_tab(FREEZER_TAB)
            st.info("🧹 Removed Freezer row(s) where TubeAmount was 0.")
    except Exception as e:
        st.warning(f"Freezer zero-row cleanup failed: {e}")

    # Filter view by freezer
    fz_view_df = fz_all_df.copy()
    if not fz_view_df.empty and "FreezerID" in fz_view_df.columns:
        fz_view_df["FreezerID"] = fz_view_df["FreezerID"].astype(str).map(safe_strip)
        fz_view_df = fz_view_df[fz_view_df["FreezerID"] == safe_strip(selected_freezer)].copy()

    st.subheader(f"🧊 Freezer Inventory ({selected_freezer})")

    # -------- Add Freezer Record --------
    st.markdown("### ➕ Add Freezer Record")
    with st.form("freezer_add", clear_on_submit=True):
        # ✅ Date Collected defaults to today()
        date_collected = st.date_input("Date Collected", value=datetime.now(NY_TZ).date())

        box_number_str = st.text_input("Box Number (string for search)", placeholder="e.g., AD-BOX-001").strip()
        study_code = st.text_input("StudyCode", placeholder="e.g., AD").strip()

        c1, c2 = st.columns(2)
        with c1:
            samples_received = st.number_input("Samples Received", min_value=0, step=1, value=0)
        with c2:
            missing_samples = st.number_input("Missing Samples", min_value=0, step=1, value=0)

        group = st.text_input("Group").strip()
        urine_results = st.text_input("Urine Results").strip()
        all_collected_by = st.text_input("All Collected By").strip()

        tube_prefix = st.text_input("TubePrefix", placeholder="e.g., Serum/DNA").strip()
        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
        memo = st.text_area("Memo (optional)")

        # ✅ BoxID locked logic (source of truth: boxNumber + Freezer_Inventory)
        box_choice = st.radio("BoxID option", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid_val, opened_new_box, current_max = locked_boxid_from_choice(box_choice)
        st.caption(f"Current max BoxID (boxNumber + Freezer_Inventory): {current_max if current_max else '(none)'}")
        st.text_input("BoxID (locked)", value=str(boxid_val), disabled=True)

        submitted = st.form_submit_button("Save to Freezer", type="primary")

        if submitted:
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
                    "FreezerID": safe_strip(selected_freezer),
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
                    "BoxID": str(int(boxid_val)),
                    "Memo": memo,
                }
                append_row_by_header(service, FREEZER_TAB, data)
                st.success("Saved ✅ Freezer record added.")

                # ✅ Green reminder (only when open new box)
                if opened_new_box:
                    green_boxid_reminder(boxid_val)

            except HttpError as e:
                st.error("Google Sheets API error while writing to Freezer_Inventory.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Failed to save freezer record")
                st.code(str(e), language="text")

# ============================================================
# 3) Use_log viewer (optional)
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
