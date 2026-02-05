# BoxLocation.py
# Streamlit app: Box Location + LN Tank + Freezer Inventory + Use Log (editable)
# + Usage logging:
#   - Subtract from TubeAmount
#   - Append usage records to Final Report (session) + Use_log tab (permanent)
#   - Show matching record(s) TubeAmount before usage
#   - Final Report hides TubeAmount, shows Use
#   - If TubeAmount becomes 0: delete that row (by rewriting sheet without it)
#   - Auto-clean on load: remove rows with TubeAmount == 0 for LN3 + Freezer_Inventory

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
    st.session_state.usage_final_rows = []  # session final report (list[dict])
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
    """Overwrite the entire sheet tab with df (header row + values)."""
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
    """Detect box label/group column across schema changes."""
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
    """Use previous = max; Open new = max+1; if none -> 1."""
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
# TubeAmount utilities + Auto-clean (delete rows where TubeAmount == 0)
# ────────────────────────────────────────────────
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

def clean_zero_rows(tab: str, amount_col: str = "TubeAmount") -> None:
    """
    Auto-clean on load:
      - load tab
      - if TubeAmount == 0 => delete row (by rewriting sheet without it)
    """
    df = read_tab(tab)
    if df.empty or amount_col not in df.columns:
        return
    amt = df[amount_col].apply(to_int)
    keep = amt != 0
    if keep.all():
        return
    df2 = df.loc[keep].copy()
    update_tab_from_df(tab, df2)

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
# Permanent Use_log schema (append-only)
USE_LOG_HEADER = [
    "StudyCode","TankID","FreezerID","RackNumber","BoxLabel_group","BoxID",
    "Prefix","Tube suffix","Use","User","Time_stamp","ShippingTo","Memo","StorageType"
]

set_header_if_blank(LN_TAB, LN_HEADER)
set_header_if_blank(FREEZER_TAB, FREEZER_HEADER)
set_header_if_blank(USE_LOG_TAB, USE_LOG_HEADER)

# ✅ Auto-clean on load (LN3 + Freezer_Inventory)
#    (This will remove any rows with TubeAmount == 0)
try:
    clean_zero_rows(LN_TAB, "TubeAmount")
    clean_zero_rows(FREEZER_TAB, "TubeAmount")
except Exception as e:
    st.warning(f"Auto-clean skipped due to error: {e}")

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
# 1) Box Location (study tabs)
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
# 2) Storage + Usage Logging + Final Report
# ────────────────────────────────────────────────
st.divider()
st.header("🧊 Storage")

# ── Final Report (session) — hides TubeAmount, shows Use ─────────
st.subheader("Session Final Report (append records)")
if st.session_state.usage_final_rows:
    df_report = pd.DataFrame(st.session_state.usage_final_rows)
    # Ensure TubeAmount is not shown even if someone accidentally adds it
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
# LN Tank UI
# ────────────────────────────────────────────────
if storage_type == "LN Tank":
    ln_df = read_tab(LN_TAB)
    view = (
        ln_df[ln_df["TankID"].astype(str).str.upper() == tank.upper()]
        if ("TankID" in ln_df.columns) else ln_df
    )

    st.subheader(f"LN Tank – {tank}")

    # ------- Add LN record -------
    with st.form("ln_add", clear_on_submit=True):
        rack = st.selectbox("Rack", range(1, 7))
        c1, c2 = st.columns(2)
        hiv  = c1.selectbox("HIV",  ["HIV+","HIV-"])
        drug = c2.selectbox("Drug", ["Cocaine","Cannabis","Poly","NON-DRUG"])

        mx_box = current_max_boxnumber()
        st.markdown(f"**Max Box (boxNumber + Freezer_Inventory):** `{mx_box or 0}`")

        box_choice = st.radio("BoxID", ["Use the previous box", "Open a new box"], horizontal=True)
        boxid, is_new = resolve_boxid(box_choice)
        st.text_input("BoxID", str(boxid), disabled=True, key="ln_boxid")

        c3, c4 = st.columns(2)
        prefix = c3.selectbox("Prefix", ["GICU","HCCU"])
        suffix = c4.text_input("Tube suffix", placeholder="02 036").strip()

        amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
        memo   = st.text_area("Memo", height=90)

        # BoxUID preview
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
                    # race-safe BoxUID recompute on save
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
                        "TubeAmount": str(amount),
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

    # ------- Log Usage (LN) -------
    st.subheader("Log Usage (LN) — subtract TubeAmount + append to Final Report + Use_log")

    # Inputs user can enter: Use, User, ShippingTo, Memo
    with st.form("ln_usage", clear_on_submit=False):
        # Identify record
        studycode_u = st.text_input("StudyCode (LN usage)", placeholder="e.g. AD / HIV / ...").strip()
        rack_u = st.selectbox("RackNumber (LN usage)", range(1, 7), key="ln_use_rack")
        boxlabel_u = st.text_input("BoxLabel_group (LN usage)", placeholder="e.g. HP-COC").strip()
        c1, c2 = st.columns(2)
        prefix_u = c1.selectbox("Prefix (LN usage)", ["GICU","HCCU"], key="ln_use_prefix")
        suffix_u = c2.text_input("Tube suffix (LN usage)", placeholder="02 036", key="ln_use_suffix").strip()

        # Show matching record(s) TubeAmount
        ln_df_live = read_tab(LN_TAB)
        if "TankID" in ln_df_live.columns:
            ln_df_live = ln_df_live[ln_df_live["TankID"].astype(str).str.upper() == tank.upper()].copy()

        if not ln_df_live.empty:
            ln_df_live["RackNumber"] = ln_df_live.get("RackNumber", "").apply(to_int)
            ln_df_live["TubeAmount_int"] = ln_df_live.get("TubeAmount", "").apply(to_int)
            ln_df_live["Prefix_only"] = ln_df_live.get("TubeNumber", "").astype(str).str.split().str[0].fillna("")
            ln_df_live["Suffix_only"] = ln_df_live.get("TubeNumber", "").astype(str).str.replace(r"^\S+\s*", "", regex=True)

            filt = (
                (ln_df_live["RackNumber"] == int(rack_u)) &
                (ln_df_live.get("BoxLabel_group", "").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                (ln_df_live["Prefix_only"].str.upper() == prefix_u.strip().upper()) &
                (ln_df_live["Suffix_only"].astype(str).str.strip() == suffix_u.strip())
            )
            matches = ln_df_live.loc[filt].copy()
        else:
            matches = pd.DataFrame()

        if not matches.empty:
            st.markdown("**Current matching record(s) — TubeAmount shown:**")
            st.dataframe(
                matches[["TankID","RackNumber","BoxLabel_group","BoxID","TubeNumber","TubeAmount_int","Memo"]]
                .rename(columns={"TubeAmount_int": "TubeAmount"}),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No matching LN record found (check Rack / BoxLabel_group / Prefix / Tube suffix).")

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
                # Use the FIRST matching row (common lab behavior). If you want multi-row, expand later.
                idx = matches.index[0]
                current_amt = to_int(matches.loc[idx, "TubeAmount_int"], 0)
                if use_n > current_amt:
                    st.error(f"Use ({use_n}) exceeds TubeAmount ({current_amt}).")
                else:
                    # Update sheet: decrement + delete if 0 by rewriting the whole tab
                    full_ln = read_tab(LN_TAB).copy()
                    # locate the same row by keys
                    full_ln["RackNumber_int"] = full_ln.get("RackNumber", "").apply(to_int)
                    full_ln["TubeAmount_int"] = full_ln.get("TubeAmount", "").apply(to_int)
                    full_ln["Prefix_only"] = full_ln.get("TubeNumber", "").astype(str).str.split().str[0].fillna("")
                    full_ln["Suffix_only"] = full_ln.get("TubeNumber", "").astype(str).str.replace(r"^\S+\s*", "", regex=True)

                    mask = (
                        (full_ln.get("TankID","").astype(str).str.upper() == tank.upper()) &
                        (full_ln["RackNumber_int"] == int(rack_u)) &
                        (full_ln.get("BoxLabel_group","").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                        (full_ln["Prefix_only"].str.upper() == prefix_u.strip().upper()) &
                        (full_ln["Suffix_only"].astype(str).str.strip() == suffix_u.strip())
                    )

                    if mask.sum() == 0:
                        st.error("Could not re-locate the LN row in the full sheet (data changed). Try again.")
                    else:
                        # Update only the first matched row
                        first_i = full_ln.index[mask][0]
                        new_amt = to_int(full_ln.loc[first_i, "TubeAmount_int"], 0) - int(use_n)
                        full_ln.loc[first_i, "TubeAmount"] = str(max(new_amt, 0))

                        # Drop if TubeAmount becomes 0
                        full_ln["TubeAmount_int2"] = full_ln.get("TubeAmount", "").apply(to_int)
                        full_ln2 = full_ln.loc[full_ln["TubeAmount_int2"] != 0].drop(
                            columns=[c for c in ["RackNumber_int","TubeAmount_int","Prefix_only","Suffix_only","TubeAmount_int2"] if c in full_ln.columns],
                            errors="ignore"
                        )

                        update_tab_from_df(LN_TAB, full_ln2)

                        # Append to session final report (TubeAmount hidden; include Use)
                        rec = {
                            "StudyCode": studycode_u,
                            "TankID": tank,
                            "RackNumber": int(rack_u),
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

                        # Append to permanent Use_log
                        append_row(USE_LOG_TAB, {
                            "StudyCode": studycode_u,
                            "TankID": tank,
                            "FreezerID": "",
                            "RackNumber": str(int(rack_u)),
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
# Freezer UI + Log Usage (Freezer)
# ────────────────────────────────────────────────
else:
    fz_df = read_tab(FREEZER_TAB)
    st.subheader(f"Freezer – {freezer}")
    st.dataframe(fz_df, use_container_width=True, hide_index=True)

    # ------- Add Freezer record -------
    with st.form("fz_add", clear_on_submit=True):
        st.text_input("FreezerID", freezer, disabled=True)

        date = st.date_input("Date Collected", datetime.now(NY_TZ).date())
        study = st.text_input("StudyCode").strip()
        boxlabel = st.text_input("BoxLabel_group", placeholder="e.g. HP-COC").strip()

        c1, c2 = st.columns(2)
        prefix = c1.text_input("Prefix", placeholder="e.g. Serum / DNA").strip()
        tube_suffix = c2.text_input("Tube suffix", placeholder="e.g. 02 036").strip()

        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)

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
                row = {
                    "FreezerID": freezer,
                    "Date Collected": date.strftime("%m/%d/%Y"),
                    "StudyCode": study,
                    "BoxLabel_group": boxlabel,
                    "Prefix": prefix,
                    "Tube suffix": tube_suffix,
                    "TubeAmount": str(tube_amount),
                    "BoxID": str(boxid),
                    "All Collected By": collected_by,
                    "Memo": memo,
                }
                append_row(FREEZER_TAB, row)
                st.success(f"Saved (BoxID {boxid})")
                if is_new:
                    show_new_box_reminder(boxid)
                st.rerun()

    # ------- Log Usage (Freezer) -------
    st.subheader("Log Usage (Freezer) — subtract TubeAmount + append to Final Report + Use_log")

    with st.form("fz_usage", clear_on_submit=False):
        # Identify record
        studycode_u = st.text_input("StudyCode (Freezer usage)", placeholder="e.g. AD / HIV / ...", key="fz_use_study").strip()
        boxlabel_u = st.text_input("BoxLabel_group (Freezer usage)", placeholder="e.g. HP-COC", key="fz_use_boxlabel").strip()
        c1, c2 = st.columns(2)
        prefix_u = c1.text_input("Prefix (Freezer usage)", placeholder="Serum / DNA", key="fz_use_prefix").strip()
        suffix_u = c2.text_input("Tube suffix (Freezer usage)", placeholder="02 036", key="fz_use_suffix").strip()

        # Show matching record(s) TubeAmount
        fz_live = read_tab(FREEZER_TAB)
        if "FreezerID" in fz_live.columns:
            fz_live = fz_live[fz_live["FreezerID"].astype(str).str.upper() == freezer.upper()].copy()

        if not fz_live.empty:
            fz_live["TubeAmount_int"] = fz_live.get("TubeAmount", "").apply(to_int)
            filt = (
                (fz_live.get("BoxLabel_group","").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                (fz_live.get("Prefix","").astype(str).str.strip().str.upper() == prefix_u.strip().upper()) &
                (fz_live.get("Tube suffix","").astype(str).str.strip() == suffix_u.strip())
            )
            matches = fz_live.loc[filt].copy()
        else:
            matches = pd.DataFrame()

        if not matches.empty:
            st.markdown("**Current matching record(s) — TubeAmount shown:**")
            st.dataframe(
                matches[["FreezerID","BoxLabel_group","BoxID","Prefix","Tube suffix","TubeAmount_int","Memo"]]
                .rename(columns={"TubeAmount_int": "TubeAmount"}),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No matching Freezer record found (check BoxLabel_group / Prefix / Tube suffix).")

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
                idx = matches.index[0]
                current_amt = to_int(matches.loc[idx, "TubeAmount_int"], 0)
                if use_n > current_amt:
                    st.error(f"Use ({use_n}) exceeds TubeAmount ({current_amt}).")
                else:
                    full_fz = read_tab(FREEZER_TAB).copy()
                    full_fz["TubeAmount_int"] = full_fz.get("TubeAmount", "").apply(to_int)

                    mask = (
                        (full_fz.get("FreezerID","").astype(str).str.upper() == freezer.upper()) &
                        (full_fz.get("BoxLabel_group","").astype(str).str.strip().str.upper() == boxlabel_u.strip().upper()) &
                        (full_fz.get("Prefix","").astype(str).str.strip().str.upper() == prefix_u.strip().upper()) &
                        (full_fz.get("Tube suffix","").astype(str).str.strip() == suffix_u.strip())
                    )

                    if mask.sum() == 0:
                        st.error("Could not re-locate the Freezer row in the full sheet (data changed). Try again.")
                    else:
                        first_i = full_fz.index[mask][0]
                        new_amt = to_int(full_fz.loc[first_i, "TubeAmount_int"], 0) - int(use_n)
                        full_fz.loc[first_i, "TubeAmount"] = str(max(new_amt, 0))

                        # Drop if TubeAmount becomes 0
                        full_fz["TubeAmount_int2"] = full_fz.get("TubeAmount", "").apply(to_int)
                        full_fz2 = full_fz.loc[full_fz["TubeAmount_int2"] != 0].drop(
                            columns=[c for c in ["TubeAmount_int","TubeAmount_int2"] if c in full_fz.columns],
                            errors="ignore"
                        )

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
