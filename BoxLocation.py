# BoxLocation.py — Full Streamlit App
# Box Location + LN Inventory (multi-tank) + Use_log
#
# NEW:
# ✅ Sidebar now supports:
#   - Study selection
#   - Storage Type: LN Tank vs Freezer
#   - If LN Tank: TankID selector (LN1/LN2/LN3, default LN3)
#   - If Freezer: Freezer selector (Sammy/Tom/Jerry)
#
# ✅ LN inventory now supports TankID:
#   - BoxUID prefix uses selected TankID (LN1/LN2/LN3) instead of hard-coded LN3
#   - If LN sheet has column "TankID", rows are written with TankID and filtered by TankID in UI
#   - If LN sheet does NOT have "TankID", app still works, but tank filtering cannot apply (warns)
#
# Existing:
# ✅ Use_log tab with required columns
# ✅ TubeAmount==0 rows are deleted on load (global, across all tanks)
# ✅ st.download_button() kept OUTSIDE st.form()

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
st.set_page_config(page_title="Box Location + LN Tank", layout="wide")
st.title("📦 Box Location + 🧊 Liquid Nitrogen Tank")

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
BOX_TAB = "boxNumber"

# You can keep LN3_TAB name as your inventory tab name (one tab for all LN tanks)
LN_TAB = "LN3"          # ✅ your existing LN tab name
USE_LOG_TAB = "Use_log" # ✅ required

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

def ensure_ln_header(service):
    """
    Recommended LN header includes TankID (so one tab can store LN1/LN2/LN3).
    This WILL NOT overwrite existing non-empty header.
    """
    required = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "QRCodeLink"]
    recommended = ["TankID", "RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "BoxID", "QRCodeLink"]

    set_header_if_blank(service, LN_TAB, recommended)

    row1 = get_header(service, LN_TAB)
    missing_required = [c for c in required if c not in row1]
    if missing_required:
        st.warning(f"{LN_TAB} header missing REQUIRED columns: {', '.join(missing_required)}")

    if "TankID" not in row1:
        st.warning(
            f"{LN_TAB} header has no 'TankID' column. "
            "Tank selection will NOT filter data. (Recommended: add TankID to header row 1.)"
        )

def ensure_use_log_header(service):
    expected = ["RackNumber", "BoxNumber", "BoxUID", "BoxID", "TubeNumber", "Use", "User", "Time_stamp", "ShippingTo", "Memo"]
    set_header_if_blank(service, USE_LOG_TAB, expected)

    row1 = get_header(service, USE_LOG_TAB)
    missing = [c for c in expected if c not in row1]
    if missing:
        st.warning(f"{USE_LOG_TAB} header missing columns: {', '.join(missing)}")

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

def get_current_max_boxid(ln_view_df: pd.DataFrame) -> int:
    """
    Max BoxID within the CURRENT VIEW (i.e., selected tank if TankID exists).
    """
    if ln_view_df is None or ln_view_df.empty or "BoxID" not in ln_view_df.columns:
        return 0
    s = pd.to_numeric(ln_view_df["BoxID"], errors="coerce").dropna()
    if s.empty:
        return 0
    return int(s.max())

def compute_next_boxuid(ln_view_df: pd.DataFrame, tank_id: str, rack: int, hp_hn: str, drug_code: str) -> str:
    """
    BoxUID format:
      {TankID}-R{rack:02d}-{HP/HN}-{COC/CAN/POL/NON-DRUG}-{NN}
    Example:
      LN2-R06-HN-COC-02
    """
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

def cleanup_zero_tubeamount_rows(service, ln_all_df: pd.DataFrame) -> bool:
    """
    After loading LN tab, delete any rows where TubeAmount == 0.
    Runs on the FULL LN sheet (across all tanks).
    """
    if ln_all_df is None or ln_all_df.empty or "TubeAmount" not in ln_all_df.columns:
        return False

    amounts = pd.to_numeric(ln_all_df["TubeAmount"], errors="coerce").fillna(0).astype(int)
    zero_idxs = [int(i) for i in ln_all_df.index[amounts == 0].tolist()]
    if not zero_idxs:
        return False

    sheet_id = get_sheet_id(service, LN_TAB)
    zero_idxs.sort(reverse=True)  # delete bottom -> top

    requests = [{
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": idx0 + 1,  # +1 to skip header
                "endIndex": idx0 + 2,
            }
        }
    } for idx0 in zero_idxs]

    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
    return True

def find_ln_row_index(
    ln_all_df: pd.DataFrame,
    tank_id: str | None,
    box_number: str,
    tube_number: str,
    box_uid: str = "",
):
    """
    Find row index in FULL LN DF (not filtered), but if TankID column exists, enforce tank_id match.
    Returns (idx0, current_amount).
    """
    if ln_all_df is None or ln_all_df.empty:
        return None, None

    needed = {"BoxNumber", "TubeNumber", "TubeAmount"}
    if not needed.issubset(set(ln_all_df.columns)):
        return None, None

    df = ln_all_df.copy()

    # Normalize strings
    df["BoxNumber"] = df["BoxNumber"].astype(str).map(safe_strip)
    df["TubeNumber"] = df["TubeNumber"].astype(str).map(safe_strip)

    mask = (df["BoxNumber"] == safe_strip(box_number)) & (df["TubeNumber"] == safe_strip(tube_number))

    # Optional BoxUID restriction
    if box_uid and "BoxUID" in df.columns:
        df["BoxUID"] = df["BoxUID"].astype(str).map(safe_strip)
        mask = mask & (df["BoxUID"] == safe_strip(box_uid))

    # Enforce TankID if present
    if tank_id and "TankID" in df.columns:
        df["TankID"] = df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
        mask = mask & (df["TankID"] == safe_strip(tank_id).upper())

    hits = df[mask]
    if hits.empty:
        return None, None

    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get("TubeAmount", 0), default=0)
    return idx0, cur_amount

def update_ln_tubeamount_by_index(service, idx0: int, new_amount: int):
    header = get_header(service, LN_TAB)
    if "TubeAmount" not in header:
        raise ValueError(f"{LN_TAB} missing 'TubeAmount' column in header.")

    col_idx = header.index("TubeAmount")
    a1_col = col_to_a1(col_idx)
    sheet_row = idx0 + 2  # header row is 1

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN_TAB}'!{a1_col}{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[int(new_amount)]]},
    ).execute()

def delete_ln_row_by_index(service, idx0: int):
    sheet_id = get_sheet_id(service, LN_TAB)
    start = idx0 + 1
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start,
                        "endIndex": start + 1,
                    }
                }
            }]
        },
    ).execute()

def build_final_report_row(row: pd.Series, use_amt: int) -> dict:
    # TubeAmount hidden
    return {
        "RackNumber": safe_strip(row.get("RackNumber", "")),
        "BoxNumber": safe_strip(row.get("BoxNumber", "")),
        "BoxUID": safe_strip(row.get("BoxUID", "")),
        "TubeNumber": safe_strip(row.get("TubeNumber", "")),
        "Memo": safe_strip(row.get("Memo", "")),
        "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
        "Use": int(use_amt),
    }

def build_use_log_row(row: pd.Series, use_amt: int, user_initials: str, shipping_to: str) -> dict:
    now = datetime.now(NY_TZ)
    time_str = now.strftime("%I:%M:%S").lstrip("0") or now.strftime("%I:%M:%S")
    date_str = now.strftime("%m/%d/%Y")
    ts = f"{time_str} {date_str}"

    return {
        "RackNumber": safe_strip(row.get("RackNumber", "")),
        "BoxNumber": safe_strip(row.get("BoxNumber", "")),
        "BoxUID": safe_strip(row.get("BoxUID", "")),
        "BoxID": safe_strip(row.get("BoxID", "")) if "BoxID" in row.index else "",
        "TubeNumber": safe_strip(row.get("TubeNumber", "")),
        "Use": int(use_amt),
        "User": safe_strip(user_initials).upper(),
        "Time_stamp": ts,
        "ShippingTo": safe_strip(shipping_to),
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
        TANK_OPTIONS = ["LN1", "LN2", "LN3"]
        selected_tank = st.selectbox("Select LN Tank", TANK_OPTIONS, index=2)  # default LN3
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
    st.info("You selected **Freezer**. LN Tank module is hidden. (You can add a Freezer module later.)")
    st.stop()

service = sheets_service()
ensure_ln_header(service)
ensure_use_log_header(service)

# Load FULL LN sheet then create a VIEW filtered by TankID if column exists
try:
    ln_all_df = read_tab(LN_TAB)
except Exception:
    ln_all_df = pd.DataFrame()

# Cleanup TubeAmount == 0 (FULL sheet)
try:
    if cleanup_zero_tubeamount_rows(service, ln_all_df):
        st.info("🧹 Removed LN row(s) where TubeAmount was 0.")
        ln_all_df = read_tab(LN_TAB)
except Exception as e:
    st.warning(f"Zero-row cleanup failed: {e}")

# Create a filtered view for selected tank (if TankID column exists)
ln_view_df = ln_all_df.copy()
if ln_view_df is not None and (not ln_view_df.empty) and ("TankID" in ln_view_df.columns):
    ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
    ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()

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

    box_number = f"{hp_hn}-{drug_code}"

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
                # If LN header has TankID, it will be populated; if not, it's ignored by append_row_by_header
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
                      <span style="color:#2e7d32FS; font-weight:700; font-size:20px; color:#2e7d32;">
                        Hint: BoxID = {boxid_input}
                      </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.session_state.last_qr_link = qr_link
            st.session_state.last_qr_uid = box_uid

            # Reload LN
            ln_all_df = read_tab(LN_TAB)
            ln_view_df = ln_all_df.copy()
            if "TankID" in ln_view_df.columns:
                ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
                ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()

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

# ---------- Search LN by BoxNumber (filtered view) ----------
st.subheader("🔎 Search LN by BoxNumber (shows matching rows)")
if ln_view_df is not None and (not ln_view_df.empty) and ("BoxNumber" in ln_view_df.columns):
    opts = sorted([safe_strip(x) for x in ln_view_df["BoxNumber"].dropna().unique().tolist() if safe_strip(x)])
    chosen = st.selectbox("BoxNumber", ["(select)"] + opts, key="search_boxnumber")
    if chosen != "(select)":
        res = ln_view_df[ln_view_df["BoxNumber"].astype(str).map(safe_strip) == chosen].copy()
        st.dataframe(res, use_container_width=True, hide_index=True)
else:
    st.info("No BoxNumber data available yet.")

# ============================================================
# 3) LOG USAGE + SAVE TO Use_log
# ============================================================
st.subheader("📉 Log Usage (subtract from TubeAmount, delete row if 0, save to Use_log)")

if ln_view_df is None or ln_view_df.empty:
    st.info(f"{selected_tank} is empty — nothing to log.")
else:
    needed = {"BoxNumber", "TubeNumber", "TubeAmount"}
    if not needed.issubset(set(ln_view_df.columns)):
        st.warning("LN sheet must include: BoxNumber, TubeNumber, TubeAmount.")
    else:
        box_opts = sorted([safe_strip(x) for x in ln_view_df["BoxNumber"].dropna().astype(str).tolist() if safe_strip(x)])
        chosen_box = st.selectbox("Select BoxNumber", ["(select)"] + sorted(set(box_opts)), key="use_box")

        chosen_tube = "(select)"
        chosen_uid = ""

        if chosen_box != "(select)":
            sub = ln_view_df.copy()
            sub["BoxNumber"] = sub["BoxNumber"].astype(str).map(safe_strip)
            sub["TubeNumber"] = sub["TubeNumber"].astype(str).map(safe_strip)
            sub = sub[sub["BoxNumber"] == safe_strip(chosen_box)].copy()

            tube_opts = sorted([safe_strip(x) for x in sub["TubeNumber"].dropna().astype(str).tolist() if safe_strip(x)])
            chosen_tube = st.selectbox("Select TubeNumber", ["(select)"] + sorted(set(tube_opts)), key="use_tube")

            if chosen_tube != "(select)" and "BoxUID" in sub.columns:
                sub2 = sub[sub["TubeNumber"] == safe_strip(chosen_tube)].copy()
                if len(sub2) > 1:
                    sub2["BoxUID"] = sub2["BoxUID"].astype(str).map(safe_strip)
                    uid_opts = sorted([x for x in sub2["BoxUID"].dropna().tolist() if safe_strip(x)])
                    chosen_uid = st.selectbox(
                        "Multiple matches found. Select BoxUID",
                        ["(select)"] + uid_opts,
                        key="use_uid",
                    )
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
            st.markdown("**Required for Use_log**")
            user_initials = st.text_input("Your initials (User)", placeholder="e.g., JW").strip()
            shipping_to = st.text_input("ShippingTo", placeholder="e.g., Dr. Smith / UCSF / Building 3").strip()

            st.divider()
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
                    idx0, cur_amount = find_ln_row_index(
                        ln_all_df,
                        tank_id=selected_tank,          # enforced if TankID column exists
                        box_number=chosen_box,
                        tube_number=chosen_tube,
                        box_uid=chosen_uid,
                    )
                    if idx0 is None:
                        st.error("No matching LN row found to update.")
                        st.stop()

                    new_amount = cur_amount - int(use_amt)
                    if new_amount < 0:
                        st.error(f"Not enough stock. Current TubeAmount={cur_amount}, Use={int(use_amt)}")
                        st.stop()

                    row_before = ln_all_df.iloc[idx0].copy()

                    # Save to Use_log (audit trail)
                    append_row_by_header(
                        service,
                        USE_LOG_TAB,
                        build_use_log_row(row_before, int(use_amt), user_initials, shipping_to),
                    )

                    # Update LN / delete if 0
                    if new_amount == 0:
                        delete_ln_row_by_index(service, idx0)
                        st.success("Usage logged ✅ Saved to Use_log. TubeAmount reached 0 — LN row deleted.")
                    else:
                        update_ln_tubeamount_by_index(service, idx0, new_amount)
                        st.success(f"Usage logged ✅ Saved to Use_log. Used {int(use_amt)} (remaining: {new_amount})")

                    # Session report
                    st.session_state.usage_final_rows.append(build_final_report_row(row_before, int(use_amt)))

                    # Reload LN after update/delete
                    ln_all_df = read_tab(LN_TAB)
                    ln_view_df = ln_all_df.copy()
                    if "TankID" in ln_view_df.columns:
                        ln_view_df["TankID"] = ln_view_df["TankID"].astype(str).map(lambda x: safe_strip(x).upper())
                        ln_view_df = ln_view_df[ln_view_df["TankID"] == safe_strip(selected_tank).upper()].copy()

                    if new_amount == 0:
                        st.rerun()

                except HttpError as e:
                    st.error("Google Sheets API error while logging usage.")
                    st.code(str(e), language="text")
                except Exception as e:
                    st.error("Failed to log usage.")
                    st.code(str(e), language="text")

        # Session report + download (outside form)
        st.markdown("### ✅ Final Usage Report (session view; permanently saved in Use_log)")
        final_cols = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "Memo", "BoxID", "Use"]

        if st.session_state.usage_final_rows:
            final_df = pd.DataFrame(st.session_state.usage_final_rows).reindex(columns=final_cols, fill_value="")
            st.dataframe(final_df, use_container_width=True, hide_index=True)

            csv_bytes = final_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Download session report CSV",
                data=csv_bytes,
                file_name=f"{safe_strip(selected_tank)}_final_usage_report_session.csv",
                mime="text/csv",
                key="download_final_usage_report",
            )

            if st.button("🧹 Clear session report", key="clear_final_report"):
                st.session_state.usage_final_rows = []
                st.success("Session report cleared (Use_log remains saved).")
        else:
            st.info("No usage records in this session yet.")

# ============================================================
# 4) Use_log viewer
# ============================================================
st.subheader("🧾 Use_log (saved Final Usage Report)")
try:
    use_log_df = read_tab(USE_LOG_TAB)
    if use_log_df.empty:
        st.info("Use_log is empty.")
    else:
        st.dataframe(use_log_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.warning(f"Unable to read Use_log: {e}")
