# BoxLocation.py
# Streamlit app: Box Location + LN Tank + Freezer Inventory + Use Log (editable)
# + Usage logging with dropdowns for:
#   - StudyCode, BoxLabel_group, Prefix, Tube suffix (LN + Freezer)
# Notes:
#   - Dropdown choices are built from CURRENT inventory in the selected storage (LN tank or FreezerID).
#   - When you pick StudyCode -> it filters available BoxLabel_group -> Prefix -> Tube suffix.

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
LN_TAB          = "LN3"
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

def now_ts_str() -> str:
    return datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M:%S")

def to_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

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

def update_tab_from_df(tab: str, df: pd.DataFrame):
    service = get_sheets_service()
    df2 = df.copy()
    df2.columns = [safe_strip(c) for c in df2.columns]
    df2 = df2.fillna("").astype(str)
    values = [df2.columns.tolist()] + df2.values.tolist()

    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:ZZ",
        body={},
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="RAW",
        body={"values": values},
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
    if df.empty:
        return None
    normalized = {
        safe_strip(c).lower().replace(" ", "").replace("_", ""): c
        for c in df.columns
    }
    for cand in ["boxlabelgroup", "boxnumber", "group", "boxlabel"]:
        if cand in normalized:
            return normalized[cand]
    return None

def extract_max_number(series: pd.Series) -> int:
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
    mx = current_max_boxnumber()
    if mx == 0:
        return 1, True
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
# Auto-clean on load
# ────────────────────────────────────────────────
def clean_zero_rows(tab: str, amount_col: str = "TubeAmount") -> None:
    df = read_tab(tab)
    if df.empty or amount_col not in df.columns:
        return
    amt = df[amount_col].apply(to_int)
    keep = amt != 0
    if keep.all():
        return
    update_tab_from_df(tab, df.loc[keep].copy())

# ────────────────────────────────────────────────
# Ensure schemas
# ────────────────────────────────────────────────
LN_HEADER = ["TankID","RackNumber","BoxLabel_group","BoxUID","TubeNumber","TubeAmount","Memo","BoxID","QRCodeLink"]
FREEZER_HEADER = [
    "FreezerID",
    "Date Collected",
    "StudyCode",
    "BoxLabel_group",
    "Prefix",
    "Tube suffix",
    "TubeAmount",
    "BoxID",
    "All Collected By",
    "Memo",
]
USE_LOG_HEADER = [
    "StudyCode","TankID","FreezerID","RackNumber","BoxLabel_group","BoxID",
    "Prefix","Tube suffix","Use","User","Time_stamp","ShippingTo","Memo","StorageType"
]

set_header_if_blank(LN_TAB, LN_HEADER)
set_header_if_blank(FREEZER_TAB, FREEZER_HEADER)
set_header_if_blank(USE_LOG_TAB, USE_LOG_HEADER)

try:
    clean_zero_rows(LN_TAB, "TubeAmount")
    clean_zero_rows(FREEZER_TAB, "TubeAmount")
except Exception as e:
    st.warning(f"Auto-clean skipped due to error: {e}")

# ────────────────────────────────────────────────
# Helpers for dropdowns (usage forms)
# ────────────────────────────────────────────────
def parse_ln_prefix_suffix(df_ln: pd.DataFrame) -> pd.DataFrame:
    """Add Prefix_only and Suffix_only extracted from TubeNumber."""
    df = df_ln.copy()
    df["Prefix_only"] = df.get("TubeNumber", "").astype(str).str.split().str[0].fillna("")
    df["Suffix_only"] = df.get("TubeNumber", "").astype(str).str.replace(r"^\S+\s*", "", regex=True).fillna("")
    df["TubeAmount_int"] = df.get("TubeAmount", "").apply(to_int)
    df["RackNumber_int"] = df.get("RackNumber", "").apply(to_int)
    return df

def unique_sorted(series: pd.Series) -> list:
    vals = [safe_strip(x) for x in series.dropna().tolist()]
    vals = [v for v in vals if v]
    return sorted(list(dict.fromkeys(vals)), key=lambda x: x.lower())

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
    storage_type = st.radio("Storage", ["LN Tank", "Freezer"], horizontal=True)

    if storage_type == "LN Tank":
        tank = st.selectbox("Tank", TANK_OPTIONS, index=2)
    else:
        freezer = st.selectbox("Freezer", FREEZER_OPTIONS)

# ────────────────────────────────────────────────
# Storage + Final Report
# ────────────────────────────────────────────────
st.header("🧊 Storage")

st.subheader("Session Final Report (append records)")
if st.session_state.usage_final_rows:
    df_report = pd.DataFrame(st.session_state.usage_final_rows)
    if "TubeAmount" in df_report.columns:
        df_report = df_report.drop(columns=["TubeAmount"])
    st.dataframe(df_report, use_container_width=True, hide_index=True)
    st.download_button("Download session CSV", df_report.to_csv(index=False), "session_final_report.csv")
    if st.button("Clear session final report"):
        st.session_state.usage_final_rows = []
        st.rerun()
else:
    st.info("No usage logged this session.")

# ────────────────────────────────────────────────
# LN Tank section + Log Usage (LN) with dropdowns
# ────────────────────────────────────────────────
if storage_type == "LN Tank":
    ln_df_all = read_tab(LN_TAB)
    ln_df = ln_df_all.copy()
    if "TankID" in ln_df.columns:
        ln_df = ln_df[ln_df["TankID"].astype(str).str.upper() == tank.upper()].copy()

    st.subheader(f"LN Tank – {tank}")
    st.dataframe(ln_df, use_container_width=True, hide_index=True)

    st.subheader("Log Usage (LN) — dropdowns")

    ln_live = parse_ln_prefix_suffix(ln_df)

    # Dropdown sources
    study_choices = ["—"] + unique_sorted(ln_live.get("StudyCode", pd.Series(dtype=str)))
    boxlabel_choices_all = unique_sorted(ln_live.get("BoxLabel_group", pd.Series(dtype=str)))

    with st.form("ln_usage", clear_on_submit=False):
        # StudyCode dropdown (if column missing, allow manual)
        if "StudyCode" in ln_live.columns and len(study_choices) > 1:
            study_sel = st.selectbox("StudyCode", study_choices, key="ln_use_study_sel")
            studycode_u = "" if study_sel == "—" else study_sel
        else:
            studycode_u = st.text_input("StudyCode", key="ln_use_study_text").strip()

        rack_u = st.selectbox("RackNumber", sorted(unique_sorted(ln_live.get("RackNumber", pd.Series(dtype=str))) or ["1","2","3","4","5","6"]),
                              key="ln_use_rack")

        # Filtered BoxLabel_group dropdown
        df_step = ln_live.copy()
        if studycode_u and "StudyCode" in df_step.columns:
            df_step = df_step[df_step["StudyCode"].astype(str).str.strip().str.upper() == studycode_u.strip().upper()]

        df_step = df_step[df_step["RackNumber_int"] == to_int(rack_u)]

        box_choices = ["—"] + unique_sorted(df_step.get("BoxLabel_group", pd.Series(dtype=str)))
        box_sel = st.selectbox("BoxLabel_group", box_choices, key="ln_use_box_sel")
        boxlabel_u = "" if box_sel == "—" else box_sel

        # Prefix dropdown
        df_step2 = df_step.copy()
        if boxlabel_u:
            df_step2 = df_step2[df_step2["BoxLabel_group"].astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()]

        prefix_choices = ["—"] + unique_sorted(df_step2.get("Prefix_only", pd.Series(dtype=str)))
        prefix_sel = st.selectbox("Prefix", prefix_choices, key="ln_use_prefix_sel")
        prefix_u = "" if prefix_sel == "—" else prefix_sel

        # Tube suffix dropdown
        df_step3 = df_step2.copy()
        if prefix_u:
            df_step3 = df_step3[df_step3["Prefix_only"].astype(str).str.strip().str.upper() == prefix_u.strip().upper()]

        suffix_choices = ["—"] + unique_sorted(df_step3.get("Suffix_only", pd.Series(dtype=str)))
        suffix_sel = st.selectbox("Tube suffix", suffix_choices, key="ln_use_suffix_sel")
        suffix_u = "" if suffix_sel == "—" else suffix_sel

        # Show matching record(s)
        matches = df_step3.copy()
        if suffix_u:
            matches = matches[matches["Suffix_only"].astype(str).str.strip() == suffix_u.strip()]

        if not matches.empty:
            st.markdown("**Current matching record(s) — TubeAmount shown:**")
            st.dataframe(
                matches[["TankID","RackNumber","BoxLabel_group","BoxID","TubeNumber","TubeAmount_int","Memo"]]
                .rename(columns={"TubeAmount_int": "TubeAmount"}),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No matching LN record found yet (select dropdowns).")

        use_n = st.number_input("Use (subtract from TubeAmount)", min_value=1, step=1, value=1, key="ln_use_n")
        user_u = st.text_input("User", value=st.session_state.user_name, key="ln_use_user").strip()
        ship_u = st.text_input("ShippingTo", key="ln_use_ship").strip()
        memo_u = st.text_area("Memo (usage)", height=80, key="ln_use_memo")

        if st.form_submit_button("Submit Usage (LN)", type="primary"):
            if matches.empty:
                st.error("No matching LN row to update.")
            elif not user_u:
                st.error("User is required.")
            else:
                # choose first match
                current_amt = to_int(matches.iloc[0]["TubeAmount_int"], 0)
                if use_n > current_amt:
                    st.error(f"Use ({use_n}) exceeds TubeAmount ({current_amt}).")
                else:
                    full_ln = read_tab(LN_TAB).copy()
                    full_ln2 = parse_ln_prefix_suffix(full_ln)

                    mask = (
                        (full_ln2.get("TankID","").astype(str).str.upper() == tank.upper()) &
                        (full_ln2["RackNumber_int"] == to_int(rack_u)) &
                        (full_ln2.get("BoxLabel_group","").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                        (full_ln2["Prefix_only"].str.upper() == prefix_u.strip().upper()) &
                        (full_ln2["Suffix_only"].astype(str).str.strip() == suffix_u.strip())
                    )
                    if mask.sum() == 0:
                        st.error("Could not re-locate the LN row in the full sheet (data changed). Try again.")
                    else:
                        first_i = full_ln2.index[mask][0]
                        new_amt = to_int(full_ln2.loc[first_i, "TubeAmount_int"], 0) - int(use_n)
                        full_ln.loc[first_i, "TubeAmount"] = str(max(new_amt, 0))

                        # delete if 0
                        tmp = full_ln.copy()
                        tmp["TubeAmount_int2"] = tmp.get("TubeAmount", "").apply(to_int)
                        tmp = tmp[tmp["TubeAmount_int2"] != 0].drop(columns=["TubeAmount_int2"], errors="ignore")
                        update_tab_from_df(LN_TAB, tmp)

                        rec = {
                            "StudyCode": studycode_u,
                            "TankID": tank,
                            "RackNumber": to_int(rack_u),
                            "BoxLabel_group": boxlabel_u,
                            "BoxID": safe_strip(full_ln.loc[first_i, "BoxID"]),
                            "Prefix": prefix_u,
                            "Tube suffix": suffix_u,
                            "Use": int(use_n),
                            "User": user_u,
                            "Time_stamp": now_ts_str(),
                            "ShippingTo": ship_u,
                            "Memo": memo_u,
                        }
                        st.session_state.usage_final_rows.append(rec)

                        append_row(USE_LOG_TAB, {
                            "StudyCode": studycode_u,
                            "TankID": tank,
                            "FreezerID": "",
                            "RackNumber": str(to_int(rack_u)),
                            "BoxLabel_group": boxlabel_u,
                            "BoxID": safe_strip(full_ln.loc[first_i, "BoxID"]),
                            "Prefix": prefix_u,
                            "Tube suffix": suffix_u,
                            "Use": str(int(use_n)),
                            "User": user_u,
                            "Time_stamp": now_ts_str(),
                            "ShippingTo": ship_u,
                            "Memo": memo_u,
                            "StorageType": "LN",
                        })

                        st.success("Usage logged. TubeAmount updated (row deleted if reached 0).")
                        st.rerun()

# ────────────────────────────────────────────────
# Freezer section + Log Usage (Freezer) with dropdowns
# ────────────────────────────────────────────────
else:
    fz_all = read_tab(FREEZER_TAB)
    fz = fz_all.copy()
    if "FreezerID" in fz.columns:
        fz = fz[fz["FreezerID"].astype(str).str.upper() == freezer.upper()].copy()

    st.subheader(f"Freezer – {freezer}")
    st.dataframe(fz, use_container_width=True, hide_index=True)

    st.subheader("Log Usage (Freezer) — dropdowns")

    fz["TubeAmount_int"] = fz.get("TubeAmount", "").apply(to_int)

    study_choices = ["—"] + unique_sorted(fz.get("StudyCode", pd.Series(dtype=str)))
    with st.form("fz_usage", clear_on_submit=False):
        # StudyCode dropdown
        if "StudyCode" in fz.columns and len(study_choices) > 1:
            study_sel = st.selectbox("StudyCode", study_choices, key="fz_use_study_sel")
            studycode_u = "" if study_sel == "—" else study_sel
        else:
            studycode_u = st.text_input("StudyCode", key="fz_use_study_text").strip()

        df_step = fz.copy()
        if studycode_u and "StudyCode" in df_step.columns:
            df_step = df_step[df_step["StudyCode"].astype(str).str.strip().str.upper() == studycode_u.strip().upper()]

        # BoxLabel_group dropdown
        box_choices = ["—"] + unique_sorted(df_step.get("BoxLabel_group", pd.Series(dtype=str)))
        box_sel = st.selectbox("BoxLabel_group", box_choices, key="fz_use_box_sel")
        boxlabel_u = "" if box_sel == "—" else box_sel

        df_step2 = df_step.copy()
        if boxlabel_u:
            df_step2 = df_step2[df_step2["BoxLabel_group"].astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()]

        # Prefix dropdown
        prefix_choices = ["—"] + unique_sorted(df_step2.get("Prefix", pd.Series(dtype=str)))
        prefix_sel = st.selectbox("Prefix", prefix_choices, key="fz_use_prefix_sel")
        prefix_u = "" if prefix_sel == "—" else prefix_sel

        df_step3 = df_step2.copy()
        if prefix_u:
            df_step3 = df_step3[df_step3["Prefix"].astype(str).str.strip().str.upper() == prefix_u.strip().upper()]

        # Tube suffix dropdown
        suffix_choices = ["—"] + unique_sorted(df_step3.get("Tube suffix", pd.Series(dtype=str)))
        suffix_sel = st.selectbox("Tube suffix", suffix_choices, key="fz_use_suffix_sel")
        suffix_u = "" if suffix_sel == "—" else suffix_sel

        matches = df_step3.copy()
        if suffix_u:
            matches = matches[matches["Tube suffix"].astype(str).str.strip() == suffix_u.strip()]

        if not matches.empty:
            st.markdown("**Current matching record(s) — TubeAmount shown:**")
            st.dataframe(
                matches[["FreezerID","StudyCode","BoxLabel_group","BoxID","Prefix","Tube suffix","TubeAmount_int","Memo"]]
                .rename(columns={"TubeAmount_int": "TubeAmount"}),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No matching Freezer record found yet (select dropdowns).")

        use_n = st.number_input("Use (subtract from TubeAmount)", min_value=1, step=1, value=1, key="fz_use_n")
        user_u = st.text_input("User", value=st.session_state.user_name, key="fz_use_user").strip()
        ship_u = st.text_input("ShippingTo", key="fz_use_ship").strip()
        memo_u = st.text_area("Memo (usage)", height=80, key="fz_use_memo")

        if st.form_submit_button("Submit Usage (Freezer)", type="primary"):
            if matches.empty:
                st.error("No matching Freezer row to update.")
            elif not user_u:
                st.error("User is required.")
            else:
                current_amt = to_int(matches.iloc[0]["TubeAmount_int"], 0)
                if use_n > current_amt:
                    st.error(f"Use ({use_n}) exceeds TubeAmount ({current_amt}).")
                else:
                    full_fz = read_tab(FREEZER_TAB).copy()
                    mask = (
                        (full_fz.get("FreezerID","").astype(str).str.upper() == freezer.upper()) &
                        (full_fz.get("StudyCode","").astype(str).str.strip().str.upper() == studycode_u.strip().upper()) &
                        (full_fz.get("BoxLabel_group","").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                        (full_fz.get("Prefix","").astype(str).str.strip().str.upper() == prefix_u.strip().upper()) &
                        (full_fz.get("Tube suffix","").astype(str).str.strip() == suffix_u.strip())
                    )
                    if mask.sum() == 0:
                        st.error("Could not re-locate the Freezer row in the full sheet (data changed). Try again.")
                    else:
                        first_i = full_fz.index[mask][0]
                        new_amt = to_int(full_fz.loc[first_i, "TubeAmount"], 0) - int(use_n)
                        full_fz.loc[first_i, "TubeAmount"] = str(max(new_amt, 0))

                        # delete if 0
                        full_fz["TubeAmount_int2"] = full_fz.get("TubeAmount", "").apply(to_int)
                        full_fz2 = full_fz[full_fz["TubeAmount_int2"] != 0].drop(columns=["TubeAmount_int2"], errors="ignore")
                        update_tab_from_df(FREEZER_TAB, full_fz2)

                        rec = {
                            "StudyCode": studycode_u,
                            "FreezerID": freezer,
                            "BoxLabel_group": boxlabel_u,
                            "BoxID": safe_strip(full_fz.loc[first_i, "BoxID"]),
                            "Prefix": prefix_u,
                            "Tube suffix": suffix_u,
                            "Use": int(use_n),
                            "User": user_u,
                            "Time_stamp": now_ts_str(),
                            "ShippingTo": ship_u,
                            "Memo": memo_u,
                        }
                        st.session_state.usage_final_rows.append(rec)

                        append_row(USE_LOG_TAB, {
                            "StudyCode": studycode_u,
                            "TankID": "",
                            "FreezerID": freezer,
                            "RackNumber": "",
                            "BoxLabel_group": boxlabel_u,
                            "BoxID": safe_strip(full_fz.loc[first_i, "BoxID"]),
                            "Prefix": prefix_u,
                            "Tube suffix": suffix_u,
                            "Use": str(int(use_n)),
                            "User": user_u,
                            "Time_stamp": now_ts_str(),
                            "ShippingTo": ship_u,
                            "Memo": memo_u,
                            "StorageType": "Freezer",
                        })

                        st.success("Usage logged. TubeAmount updated (row deleted if reached 0).")
                        st.rerun()

# ────────────────────────────────────────────────
# Use Log (permanent record – EDIT)
# ────────────────────────────────────────────────
st.divider()
st.subheader("Use_log (permanent record – EDIT)")

try:
    ul = read_tab(USE_LOG_TAB)

    if ul.empty:
        st.info("Use_log is empty.")
    else:
        edited = st.data_editor(
            ul,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="use_log_editor",
        )

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("💾 Save changes to Use_log", type="primary"):
                try:
                    update_tab_from_df(USE_LOG_TAB, edited)
                    st.success("Use_log saved.")
                except Exception as e:
                    st.error(f"Save failed: {e}")

        with c2:
            st.download_button(
                "Download Use_log CSV",
                edited.to_csv(index=False),
                "Use_log.csv",
            )

except Exception as e:
    st.error(f"Use_log error: {e}")
