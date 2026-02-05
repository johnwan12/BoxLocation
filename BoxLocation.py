# BoxLocation.py
# Streamlit app: Box Location + LN Tank + Freezer Inventory + Use Log preview
# Updates included:
# 1) Max Box logic is based on MAX(BoxLabel_group numeric part) across 'boxNumber' + 'Freezer_Inventory'
# 2) BoxID rule:
#       Use the previous box = current_max_boxnumber
#       Open a new box       = current_max_boxnumber + 1
# 3) 'Group' column renamed to 'BoxLabel_group'
# 4) Freezer entry order:
#       Date collected, StudyCode, BoxLabel_group, Prefix, Tube suffix, BoxID, All Collected By, Memo
# 5) Show "Max Box Logic" BEFORE BoxID selector

import urllib.parse
import urllib.request
from datetime import datetime
import pandas as pd
import pytz
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ────────────────────────────────────────────────
# Page & Session State
# ────────────────────────────────────────────────
st.set_page_config(page_title="Box Location • LN • Freezer", layout="wide")
st.title("📦 Box Location + 🧊 LN Tank + 🧊 Freezer Inventory")

if "last_qr_link" not in st.session_state:
    st.session_state.last_qr_link = ""
if "last_qr_uid" not in st.session_state:
    st.session_state.last_qr_uid = ""
if "usage_final_rows" not in st.session_state:
    st.session_state.usage_final_rows = []
if "user_name" not in st.session_state:
    st.session_state.user_name = ""

# ────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}

BOX_TAB         = "boxNumber"
FREEZER_TAB     = "Freezer_Inventory"
LN_TAB          = "LN3"           # single tab for LN1/LN2/LN3 (filtered by TankID)
USE_LOG_TAB     = "Use_log"

HIV_CODE  = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL", "NON-DRUG": "NON-DRUG"}

FREEZER_OPTIONS = ["Sammy", "Tom", "Jerry"]
TANK_OPTIONS    = ["LN1", "LN2", "LN3"]

QR_PX = 120
NY_TZ = pytz.timezone("America/New_York")

SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]

# ────────────────────────────────────────────────
# Google Sheets Service (cached)
# ────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]), scopes=scopes
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# ────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────
def safe_strip(x) -> str:
    return "" if x is None else str(x).strip()

def read_tab(tab_name: str) -> pd.DataFrame:
    service = get_sheets_service()
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab_name}'!A1:ZZ",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()

        values = resp.get("values", [])
        if not values:
            return pd.DataFrame()

        header = [safe_strip(h) for h in values[0]]
        rows = []
        for r in values[1:]:
            if len(r) < len(header):
                r = r[:len(header)] + [""] * (len(header) - len(r))
            else:
                r = r[:len(header)]
            rows.append(r)

        return pd.DataFrame(rows, columns=header)
    except Exception as e:
        st.error(f"Cannot read tab '{tab_name}': {e}")
        return pd.DataFrame()

def get_header(tab: str) -> list:
    service = get_sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:ZZ1",
    ).execute()
    return [safe_strip(x) for x in (resp.get("values", [[]]) or [[]])[0]]

def set_header_if_blank(tab: str, header: list):
    service = get_sheets_service()
    existing = get_header(tab)
    if (not existing) or all(not safe_strip(x) for x in existing):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_row(tab: str, data: dict):
    service = get_sheets_service()
    header = get_header(tab)
    if not header:
        raise ValueError(f"No header found in {tab}")

    row = [data.get(col, "") for col in header]
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:ZZ",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

def qr_url(box_uid: str, size: int = QR_PX) -> str:
    text = urllib.parse.quote(box_uid)
    return f"https://quickchart.io/qr?text={text}&size={size}&ecLevel=Q&margin=1"

def fetch_image_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as r:
        return r.read()

# ────────────────────────────────────────────────
# Max Box Logic (based on BoxLabel_group / BoxNumber variants)
# ────────────────────────────────────────────────
def find_boxlabel_col(df: pd.DataFrame) -> str | None:
    """
    Detect the column storing the box label/group across schema changes.
    Accepts BoxLabel_group / BoxNumber / Group / Box Number.
    """
    if df.empty:
        return None

    normalized = {
        safe_strip(c).lower().replace(" ", "").replace("_", ""): c
        for c in df.columns
    }

    # Prefer new name first
    candidates = [
        "boxlabelgroup",   # BoxLabel_group
        "boxnumber",       # BoxNumber
        "group",           # legacy Group
        "boxlabel",        # fallback
        "boxnumbergroup",  # rare variants
    ]

    for cand in candidates:
        if cand in normalized:
            return normalized[cand]

    return None

def extract_max_number(series: pd.Series) -> int:
    """
    Extract first integer from strings like:
      AD-BOX-012 -> 12
      LN3-R01-HP-CAN-03 -> 3
    Return max; if none found -> 0.
    """
    if series is None or series.empty:
        return 0
    s = series.astype(str)
    nums = pd.to_numeric(s.str.extract(r"(\d+)", expand=False), errors="coerce").dropna()
    return int(nums.max()) if not nums.empty else 0

def get_max_boxnumber_in_tab(tab_name: str) -> int:
    df = read_tab(tab_name)
    col = find_boxlabel_col(df)
    if not col:
        return 0
    return extract_max_number(df[col])

@st.cache_data(ttl=5)
def current_max_boxnumber() -> int:
    return max(
        get_max_boxnumber_in_tab(BOX_TAB),
        get_max_boxnumber_in_tab(FREEZER_TAB),
    )

def resolve_boxid(choice: str) -> tuple[int, bool]:
    """
    BoxID rule:
      Use previous box -> current_max_boxnumber
      Open a new box   -> current_max_boxnumber + 1
    """
    mx = current_max_boxnumber()
    if mx == 0:
        return 1, True  # first-ever box
    if choice == "Open a new box":
        return mx + 1, True
    return mx, False

def show_new_box_reminder(boxid: int):
    st.markdown(
        f"""<div style="padding:16px; background:#e8f5e9; border:1px solid #2e7d32; border-radius:8px; margin:16px 0;">
        <strong style="color:#1b5e20; font-size:1.3em;">New Box Created – Please Label:</strong><br><br>
        BoxID = <span style="font-size:1.8em; font-weight:bold;">{boxid}</span>
        </div>""",
        unsafe_allow_html=True,
    )

# ────────────────────────────────────────────────
# Sidebar
# ────────────────────────────────────────────────
with st.sidebar:
    st.subheader("User")
    st.session_state.user_name = st.text_input(
        "Your name / initials", st.session_state.user_name.strip()
    ).strip()

    st.divider()
    st.subheader("View")
    study_tab = st.selectbox("Study", DISPLAY_TABS)
    storage_type = st.radio("Storage", ["LN Tank", "Freezer"], horizontal=True)

    if storage_type == "LN Tank":
        tank = st.selectbox("Tank", TANK_OPTIONS, index=2)
    else:
        freezer = st.selectbox("Freezer", FREEZER_OPTIONS)

# ────────────────────────────────────────────────
# 1. Box Location (study tabs)
# ────────────────────────────────────────────────
st.header("📦 Box Location")
try:
    df = read_tab(TAB_MAP[study_tab])
    if df.empty:
        st.info("No data yet.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("StudyID → Box Label")
    box_map = {}
    box_df = read_tab(BOX_TAB)

    # Detect which column holds the box label/group in boxNumber tab
    label_col = find_boxlabel_col(box_df) if not box_df.empty else None

    if (not box_df.empty) and label_col:
        for _, r in box_df.iterrows():
            sid = safe_strip(r.get("StudyID", "")).upper()
            bx = safe_strip(r.get(label_col, ""))
            if sid and bx:
                box_map[sid] = bx

    study_ids = sorted({safe_strip(s).upper() for s in df.get("StudyID", []) if safe_strip(s)})
    sel = st.selectbox("StudyID", ["—"] + study_ids)
    if sel != "—":
        bx = box_map.get(sel, "")
        st.metric("Box Label", bx or "Not found", delta_color="off" if bx else "normal")
except Exception as e:
    st.error(f"Box Location error: {e}")

# ────────────────────────────────────────────────
# 2. Storage
# ────────────────────────────────────────────────
st.divider()
st.header("🧊 Storage")

# Session usage preview
st.subheader("Session Final Usage Report")
if st.session_state.usage_final_rows:
    df_usage = pd.DataFrame(st.session_state.usage_final_rows)
    st.dataframe(df_usage, use_container_width=True, hide_index=True)
    st.download_button("Download session CSV", df_usage.to_csv(index=False), "session_usage.csv")
    if st.button("Clear session report"):
        st.session_state.usage_final_rows = []
        st.rerun()
else:
    st.info("No usage logged this session.")

# ── LN Tank ──────────────────────────────────────
if storage_type == "LN Tank":
    # LN schema uses BoxLabel_group (renamed from Group/BoxNumber)
    set_header_if_blank(
        LN_TAB,
        ["TankID","RackNumber","BoxLabel_group","BoxUID","TubeNumber","TubeAmount","Memo","BoxID","QRCodeLink"],
    )

    ln_df = read_tab(LN_TAB)
    view = (
        ln_df[ln_df["TankID"].astype(str).str.upper() == tank.upper()]
        if ("TankID" in ln_df.columns) else ln_df
    )

    st.subheader(f"LN Tank – {tank}")

    with st.form("ln_add", clear_on_submit=True):
        rack = st.selectbox("Rack", range(1, 7))
        c1, c2 = st.columns(2)
        hiv  = c1.selectbox("HIV",  ["HIV+","HIV-"])
        drug = c2.selectbox("Drug", ["Cocaine","Cannabis","Poly","NON-DRUG"])

        # Max Box Logic BEFORE BoxID
        mx_box = current_max_boxnumber()
        st.markdown(f"**Max Box (boxNumber + Freezer_Inventory):** `{mx_box or 0}`")

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid, is_new = resolve_boxid(box_choice)
        st.text_input("BoxID", str(boxid), disabled=True, key="ln_boxid")

        c3, c4 = st.columns(2)
        prefix = c3.selectbox("Prefix", ["GICU","HCCU"])
        suffix = c4.text_input("Tube suffix", placeholder="02 036").strip()

        amount = st.number_input("Tube count", 0, step=1, value=1)
        memo   = st.text_area("Memo", height=90)

        # --- BoxUID preview (robust)
        prefix_str = f"{tank}-R{rack:02d}-{HIV_CODE[hiv]}-{DRUG_CODE[drug]}-"
        seq = 1
        try:
            if (not view.empty) and ("BoxUID" in view.columns):
                nums = []
                for uid in view["BoxUID"].astype(str):
                    if uid.startswith(prefix_str):
                        tail = uid.split("-")[-1]
                        try:
                            nums.append(int(tail))
                        except Exception:
                            pass
                seq = (max(nums) + 1) if nums else 1

            box_uid = f"{prefix_str}{seq:02d}"
            st.info(f"→ BoxUID: **{box_uid}**")
            st.image(qr_url(box_uid), width=QR_PX)
        except Exception as e:
            box_uid = f"{prefix_str}{seq:02d}"
            st.warning(f"Could not preview BoxUID (will still save as {box_uid}). Error: {e}")

        if st.form_submit_button("Save LN record", type="primary"):
            if not suffix:
                st.error("Tube suffix required.")
            else:
                try:
                    # Recompute seq on save from latest data (race-safe)
                    ln_df_latest = read_tab(LN_TAB)
                    view_latest = (
                        ln_df_latest[ln_df_latest["TankID"].astype(str).str.upper() == tank.upper()]
                        if ("TankID" in ln_df_latest.columns) else ln_df_latest
                    )

                    seq2 = 1
                    if (not view_latest.empty) and ("BoxUID" in view_latest.columns):
                        nums2 = []
                        for uid in view_latest["BoxUID"].astype(str):
                            if uid.startswith(prefix_str):
                                tail = uid.split("-")[-1]
                                try:
                                    nums2.append(int(tail))
                                except Exception:
                                    pass
                        seq2 = (max(nums2) + 1) if nums2 else 1

                    box_uid2 = f"{prefix_str}{seq2:02d}"
                    qr_link = qr_url(box_uid2)

                    row = {
                        "TankID": tank,
                        "RackNumber": rack,
                        "BoxLabel_group": f"{HIV_CODE[hiv]}-{DRUG_CODE[drug]}",
                        "BoxUID": box_uid2,
                        "TubeNumber": f"{prefix} {suffix}",
                        "TubeAmount": amount,
                        "Memo": memo,
                        "BoxID": str(boxid),
                        "QRCodeLink": qr_link,
                    }
                    append_row(LN_TAB, row)

                    st.success(f"Saved → {box_uid2} (BoxID {boxid})")
                    st.session_state.last_qr_link = qr_link
                    st.session_state.last_qr_uid = box_uid2
                    if is_new:
                        show_new_box_reminder(boxid)

                except Exception as e:
                    st.error(f"Save failed: {e}")

    if st.session_state.last_qr_link:
        try:
            png = fetch_image_bytes(st.session_state.last_qr_link)
            st.download_button("↓ Last QR", png, f"{st.session_state.last_qr_uid}.png", "image/png")
        except Exception:
            pass

    st.subheader(f"{tank} content")
    st.dataframe(view, use_container_width=True, hide_index=True)

# ── Freezer ──────────────────────────────────────
else:
    # Freezer schema + header order exactly as requested
    FREEZER_HEADER = [
        "Date collected",
        "StudyCode",
        "BoxLabel_group",
        "Prefix",
        "Tube suffix",
        "BoxID",
        "All Collected By",
        "Memo",
    ]
    set_header_if_blank(FREEZER_TAB, FREEZER_HEADER)

    fz_df = read_tab(FREEZER_TAB)
    st.subheader(f"Freezer – {freezer}")
    st.dataframe(fz_df, use_container_width=True, hide_index=True)

    with st.form("fz_add", clear_on_submit=True):
        # Entry order exactly as requested
        date = st.date_input("Date collected", datetime.now(NY_TZ).date())
        study = st.text_input("StudyCode").strip()
        boxlabel = st.text_input("BoxLabel_group", placeholder="e.g. HP-COC").strip()

        c1, c2 = st.columns(2)
        prefix = c1.text_input("Prefix", placeholder="e.g. Serum / DNA").strip()
        tube_suffix = c2.text_input("Tube suffix", placeholder="e.g. 02 036").strip()

        # Max Box Logic BEFORE BoxID
        mx_box = current_max_boxnumber()
        st.markdown(f"**Max Box (boxNumber + Freezer_Inventory):** `{mx_box or 0}`")

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True, key="fz_choice")
        boxid, is_new = resolve_boxid(box_choice)
        st.text_input("BoxID", str(boxid), disabled=True, key="fz_boxid")

        collected_by = st.text_input("All Collected By").strip()
        memo = st.text_area("Memo", height=90)

        if st.form_submit_button("Save Freezer record", type="primary"):
            if not all([study, boxlabel, prefix, tube_suffix]):
                st.error("StudyCode, BoxLabel_group, Prefix, and Tube suffix are required.")
            else:
                try:
                    row = {
                        "Date collected": date.strftime("%m/%d/%Y"),
                        "StudyCode": study,
                        "BoxLabel_group": boxlabel,
                        "Prefix": prefix,
                        "Tube suffix": tube_suffix,
                        "BoxID": str(boxid),
                        "All Collected By": collected_by,
                        "Memo": memo,
                    }
                    append_row(FREEZER_TAB, row)
                    st.success(f"Saved (BoxID {boxid})")
                    if is_new:
                        show_new_box_reminder(boxid)
                except Exception as e:
                    st.error(f"Save failed: {e}")

# ────────────────────────────────────────────────
# Use Log (view only for now)
# ────────────────────────────────────────────────
st.divider()
st.subheader("Use_log (permanent record – view only)")
try:
    ul = read_tab(USE_LOG_TAB)
    st.dataframe(ul, use_container_width=True, hide_index=True)
except Exception:
    st.info("Use_log tab not readable or empty.")
