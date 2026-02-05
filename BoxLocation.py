# BoxLocation.py
# Streamlit app: Box Location + LN Tank + Freezer Inventory + Use Log preview

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
def safe_strip(x): return "" if x is None else str(x).strip()

def read_tab(tab_name: str) -> pd.DataFrame:
    service = get_sheets_service()
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab_name}'!A1:ZZ",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        values = resp.get("values", [])
        if not values: return pd.DataFrame()
        header = [safe_strip(h) for h in values[0]]
        rows = [r[:len(header)] + [""] * (len(header) - len(r)) if len(r) < len(header) else r[:len(header)] for r in values[1:]]
        return pd.DataFrame(rows, columns=header)
    except Exception as e:
        st.error(f"Cannot read tab '{tab_name}': {e}")
        return pd.DataFrame()

def get_header(tab: str) -> list:
    service = get_sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1:Z1",
    ).execute()
    return [safe_strip(x) for x in (resp.get("values", [[]]) or [[]])[0]]

def set_header_if_blank(tab: str, header: list):
    service = get_sheets_service()
    if not get_header(tab) or all(not safe_strip(x) for x in get_header(tab)):
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
        range=f"'{tab}'!A:Z",
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
# BoxID Logic – only boxNumber + Freezer_Inventory
# ────────────────────────────────────────────────
def get_max_boxid(df: pd.DataFrame) -> int:
    if df.empty or "BoxID" not in df.columns: return 0
    nums = pd.to_numeric(df["BoxID"], errors="coerce").dropna()
    return int(nums.max()) if not nums.empty else 0

@st.cache_data(ttl=20)
def current_max_boxid() -> int:
    mx = 0
    for tab in [BOX_TAB, FREEZER_TAB]:
        try:
            df = read_tab(tab)
            mx = max(mx, get_max_boxid(df))
        except:
            pass
    return mx

def resolve_boxid(choice: str) -> tuple[int, bool]:
    mx = current_max_boxid()
    if choice == "Open a new box":
        return mx + 1, True
    else:  # "Use the previous box"
        return max(mx, 1), False

def show_new_box_reminder(boxid: int):
    st.markdown(
        f"""<div style="padding:16px; background:#e8f5e9; border:1px solid #2e7d32; border-radius:8px; margin:16px 0;">
        <strong style="color:#1b5e20; font-size:1.3em;">New Box Created – Please Label:</strong><br><br>
        BoxID = <span style="font-size:1.8em; font-weight:bold;">{boxid}</span>
        </div>""",
        unsafe_allow_html=True
    )

# ────────────────────────────────────────────────
# Sidebar
# ────────────────────────────────────────────────
with st.sidebar:
    st.subheader("User")
    st.session_state.user_name = st.text_input("Your name / initials", st.session_state.user_name.strip()).strip()

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

    st.subheader("StudyID → Box Number")
    box_map = {}
    box_df = read_tab(BOX_TAB)
    if not box_df.empty:
        for _, r in box_df.iterrows():
            sid = safe_strip(r.get("StudyID", "")).upper()
            bx = safe_strip(r.get("BoxNumber", ""))
            if sid and bx:
                box_map[sid] = bx

    study_ids = sorted({safe_strip(s).upper() for s in df.get("StudyID", []) if safe_strip(s)})
    sel = st.selectbox("StudyID", ["—"] + study_ids)
    if sel != "—":
        bx = box_map.get(sel, "")
        st.metric("Box Number", bx or "Not found", delta_color="off" if bx else "normal")
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
    set_header_if_blank(LN_TAB, ["TankID","RackNumber","BoxNumber","BoxUID","TubeNumber","TubeAmount","Memo","BoxID","QRCodeLink"])

    ln_df = read_tab(LN_TAB)
    view = ln_df[ln_df["TankID"].str.upper() == tank.upper()] if "TankID" in ln_df else ln_df

    st.subheader(f"LN Tank – {tank}")

    with st.form("ln_add", clear_on_submit=True):
        rack = st.selectbox("Rack", range(1,7))
        c1, c2 = st.columns(2)
        hiv  = c1.selectbox("HIV",  ["HIV+","HIV-"])
        drug = c2.selectbox("Drug", ["Cocaine","Cannabis","Poly","NON-DRUG"])

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid, is_new = resolve_boxid(box_choice)
        st.caption(f"Current max BoxID: **{current_max_boxid() or '—'}**")
        st.text_input("BoxID", str(boxid), disabled=True, key="ln_boxid")

        c3, c4 = st.columns(2)
        prefix = c3.selectbox("Prefix", ["GICU","HCCU"])
        suffix = c4.text_input("Tube suffix", placeholder="02 036").strip()

        amount = st.number_input("Tube count", 0, step=1, value=1)
        memo   = st.text_area("Memo", height=90)

        try:
            prefix_str = f"{tank}-R{rack:02d}-{HIV_CODE[hiv]}-{DRUG_CODE[drug]}-"
            seq = max((int(uid.split("-")[-1]) for uid in view["BoxUID"].astype(str) if uid.startswith(prefix_str)), default=0) + 1
            box_uid = f"{prefix_str}{seq:02d}"
            st.info(f"→ BoxUID: **{box_uid}**")
            st.image(qr_url(box_uid), width=QR_PX)
        except:
            st.warning("Could not preview BoxUID")

        if st.form_submit_button("Save LN record", type="primary"):
            if not suffix:
                st.error("Tube suffix required.")
            else:
                try:
                    box_uid = f"{prefix_str}{seq:02d}"   # re-compute to be safe
                    qr_link = qr_url(box_uid)
                    row = {
                        "TankID": tank,
                        "RackNumber": rack,
                        "BoxNumber": f"{HIV_CODE[hiv]}-{DRUG_CODE[drug]}",
                        "BoxUID": box_uid,
                        "TubeNumber": f"{prefix} {suffix}",
                        "TubeAmount": amount,
                        "Memo": memo,
                        "BoxID": str(boxid),
                        "QRCodeLink": qr_link,
                    }
                    append_row(LN_TAB, row)
                    st.success(f"Saved → {box_uid} (BoxID {boxid})")
                    st.session_state.last_qr_link = qr_link
                    st.session_state.last_qr_uid = box_uid
                    if is_new:
                        show_new_box_reminder(boxid)
                except Exception as e:
                    st.error(f"Save failed: {e}")

    if st.session_state.last_qr_link:
        try:
            png = fetch_image_bytes(st.session_state.last_qr_link)
            st.download_button("↓ Last QR", png, f"{st.session_state.last_qr_uid}.png", "image/png")
        except:
            pass

    st.subheader(f"{tank} content")
    st.dataframe(view, use_container_width=True, hide_index=True)

# ── Freezer ──────────────────────────────────────
else:
    set_header_if_blank(FREEZER_TAB, [
        "FreezerID","Date Collected","Box Number","StudyCode","Samples Received","Missing Samples",
        "Group","Urine Results","All Collected By","TubePrefix","TubeAmount","BoxID","Memo"
    ])

    fz_df = read_tab(FREEZER_TAB)
    view = fz_df[fz_df["FreezerID"] == freezer] if "FreezerID" in fz_df else fz_df

    st.subheader(f"Freezer – {freezer}")

    with st.form("fz_add", clear_on_submit=True):
        date = st.date_input("Date collected", datetime.now(NY_TZ).date())
        box_nr = st.text_input("Box Number", placeholder="e.g. AD-BOX-001").strip()
        study  = st.text_input("StudyCode", placeholder="AD").strip()

        c1, c2 = st.columns(2)
        received = c1.number_input("Samples received", 0)
        missing  = c2.number_input("Missing", 0)

        group = st.text_input("Group").strip()
        urine = st.text_input("Urine results").strip()
        by    = st.text_input("Collected by").strip()
        prefix = st.text_input("Tube prefix", placeholder="Serum / DNA").strip()
        amount = st.number_input("Tube count", 0, step=1, value=1)
        memo   = st.text_area("Memo", height=90)

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True, key="fz_choice")
        boxid, is_new = resolve_boxid(box_choice)
        st.caption(f"Current max BoxID: **{current_max_boxid() or '—'}**")
        st.text_input("BoxID", str(boxid), disabled=True, key="fz_boxid")

        if st.form_submit_button("Save Freezer record", type="primary"):
            if not all([box_nr, study, prefix]):
                st.error("Box Number, StudyCode and Tube prefix are required.")
            else:
                try:
                    row = {
                        "FreezerID": freezer,
                        "Date Collected": date.strftime("%m/%d/%Y"),
                        "Box Number": box_nr,
                        "StudyCode": study,
                        "Samples Received": received,
                        "Missing Samples": missing,
                        "Group": group,
                        "Urine Results": urine,
                        "All Collected By": by,
                        "TubePrefix": prefix,
                        "TubeAmount": amount,
                        "BoxID": str(boxid),
                        "Memo": memo,
                    }
                    append_row(FREEZER_TAB, row)
                    st.success(f"Saved (BoxID {boxid})")
                    if is_new:
                        show_new_box_reminder(boxid)
                except Exception as e:
                    st.error(f"Save failed: {e}")

    st.subheader(f"{freezer} content")
    st.dataframe(view, use_container_width=True, hide_index=True)

# ────────────────────────────────────────────────
# Use Log (view only for now)
# ────────────────────────────────────────────────
st.divider()
st.subheader("Use_log (permanent record – view only)")
try:
    ul = read_tab(USE_LOG_TAB)
    st.dataframe(ul, use_container_width=True, hide_index=True)
except:
    st.info("Use_log tab not readable or empty.")
