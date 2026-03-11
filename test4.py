import streamlit as st
import pandas as pd
import datetime
import io
import warnings
warnings.filterwarnings('ignore')

# ── 서비스 매핑 (컬럼명 기준) ─────────────────────────────────
SERVICE_MAP = {
    "DM/Set up" : {"service": "DM/Set up",   "lfpf": "PF"},
    "STAT"      : {"service": "STAT",         "lfpf": "PF"},
    "EDC"       : {"service": "cubeCDMS",     "lfpf": "LF"},
    "IWRS"      : {"service": "cubeIWRS",     "lfpf": "LF"},
    "PRO"       : {"service": "cubePRO",      "lfpf": "LF"},
    "독립적평가자":{"service": "IE",           "lfpf": "LF"},
    "FTP"       : {"service": "FTP",          "lfpf": "LF"},
    "PSDV"      : {"service": "PSDV",         "lfpf": "LF"},
    "SAFETY"    : {"service": "cubeSAFETY",   "lfpf": "LF"},
    "TMF"       : {"service": "cubeTMF",      "lfpf": "LF"},
    "CTMS"      : {"service": "cubeCTMS",     "lfpf": "LF"},
    "CONSENT"   : {"service": "cubeCONCENT",  "lfpf": "LF"},
    "DDC"       : {"service": "cubeDDC",      "lfpf": "LF"},
    "RBQM"      : {"service": "cubeRBQM",     "lfpf": "LF"},
    "LMS"       : {"service": "cubeLMS",      "lfpf": "LF"},
}
PF_TYPE_MAP = {
    "CDMS"   : "cubeCDMS",
    "Safety" : "cubeSAFETY",
    "CONSENT": "cubeCONCENT",
    "CTMS"   : "cubeCTMS",
    "LMS"    : "cubeLMS",
    "RBQM"   : "cubeRBQM",
    "TMF"    : "cubeTMF",
    "OTHER"  : "OTHER",
}
MANDATORY_COLS   = ["EDC", "IWRS", "PRO", "독립적평가자"]
EXCLUDE_KEYWORDS = ["TOTAL","TOTAL AMT","TOTAL 금액","TOTAL 금액(C)","합계","소계","SUBTOTAL","SUM금액(S)"]
EXCLUDE_UPPER    = [k.upper() for k in EXCLUDE_KEYWORDS]
RESULT_COLS      = [
    "SUBJID","BD","CRScube","Customer","Country",
    "LF/PF","Service","Currency","Billing Amt","FX rate",
    "Adj. Billing Amt","청구예정일(C)","청구일(C)","입금일(C)","Source_Sheet"
]

# ── 페이지 설정 ───────────────────────────────────────────────
st.set_page_config(page_title="CRScube 비용 데이터 정제 Ver 2.0",
                   layout="wide", initial_sidebar_state="expanded")
st.title("📊 CRScube 비용 데이터 정제 Ver 2.0")
st.markdown("""
### 🎯 주요 기능
- **CA/CB 서비스 컬럼**: 컬럼명 기준 자동 인식 (DM/Set up ~ PF)
- **PF구분 + PF**: PF 서비스 자동 매핑
- **TBD**: 값 있으면 기타(LF)로 출력
- **CA 필수항목**: EDC·IWRS·PRO·독립적평가자는 0원도 포함
- **결과**: SUBJID 앞 2글자 기준 시트 분리 출력
""")

# ── 사이드바: 파일 업로드 ─────────────────────────────────────
st.sidebar.header("📂 파일 업로드")
st.sidebar.markdown("---")
source_file = st.sidebar.file_uploader(
    "1️⃣ 원본 파일 (.xlsb / .xlsx / .xls)",
    type=["xlsb", "xlsx", "xls"],
    help="DG, CA, CB, CX 시트가 포함된 파일"
)
rate_file = st.sidebar.file_uploader(
    "2️⃣ 환율 파일 (.xlsx / .xls)",
    type=["xlsx", "xls"],
    help="Exchange Rate 또는 StdExRate 시트 포함"
)

# ── 파일 읽기 헬퍼 ───────────────────────────────────────────
def read_sheet(file, sheet_name, engine=None):
    """확장자에 맞는 엔진으로 시트 읽기"""
    ext = file.name.rsplit(".", 1)[-1].lower()
    if engine is None:
        engine = "pyxlsb" if ext == "xlsb" else "openpyxl"
    try:
        df = pd.read_excel(file, sheet_name=sheet_name, engine=engine).fillna("")
        file.seek(0)  # 다음 읽기를 위해 포인터 초기화
        return df
    except Exception as e:
        st.warning(f"'{sheet_name}' 시트 로드 실패: {e}")
        file.seek(0)
        return pd.DataFrame()

# ── 환율 로드 ─────────────────────────────────────────────────
def load_rate_data(file):
    for sheet_name, hdr in [("Exchange Rate", 3), ("StdExRate", 0)]:
        try:
            df = pd.read_excel(file, sheet_name=sheet_name,
                               header=hdr, engine="openpyxl").fillna("")
            file.seek(0)
            df.columns = [str(c).strip() for c in df.columns]
            # StdExRate: 헤더 행 자동 탐색
            if sheet_name == "StdExRate":
                found = False
                for h in range(20):
                    temp = pd.read_excel(file, sheet_name=sheet_name,
                                         header=h, engine="openpyxl").fillna("")
                    file.seek(0)
                    temp.columns = [str(c).strip() for c in temp.columns]
                    if (any(x in temp.columns for x in ["Date","날짜","일자"]) and
                        any(x in temp.columns for x in ["Currency","통화","통화명"]) and
                        any(x in temp.columns for x in ["Rate","환율","매매기준율"])):
                        df = temp
                        found = True
                        break
                if not found:
                    continue
            rate_cols = [c for c in df.columns if any(k in c for k in ["환율","매매기준율","Rate"])]
            rm = {rate_cols[0]: "Rate"} if rate_cols else {}
            rm.update({"날짜":"Date","일자":"Date","통화":"Currency","통화명":"Currency"})
            df = df.rename(columns={k:v for k,v in rm.items() if k in df.columns})
            if {"Date","Currency","Rate"}.issubset(df.columns):
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"]).sort_values("Date")
                st.sidebar.success(f"✅ {sheet_name} 시트 로드 성공")
                return df
        except Exception:
            file.seek(0)
            continue
    st.sidebar.error("❌ 환율 데이터 로드 실패")
    return pd.DataFrame()

# ── 환율 찾기 ─────────────────────────────────────────────────
def find_rate(df_rate, currency, target_date):
    tc = str(currency).strip().upper()
    if tc in ("KRW","NAN","","NONE"):
        return 1.0
    if pd.isnull(target_date) or df_rate is None or df_rate.empty:
        return None
    mask = (df_rate["Currency"].str.contains(tc, na=False, case=False) &
            (df_rate["Date"] <= target_date))
    pos = df_rate[mask]
    return float(str(pos.iloc[-1]["Rate"]).replace(",","")) if not pos.empty else None

# ── 핵심 처리 함수 ────────────────────────────────────────────
def process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate):
    progress = st.progress(0)
    status   = st.empty()

    # DG / CX 마스터 맵 (컬럼명 기준으로 접근 → set_index 후 iloc 밀림 방지)
    dg_subjid_col = df_dg.columns[0]   # SUBJID
    dg_bd_col     = df_dg.columns[1]   # BD Type
    dg_entity_col = df_dg.columns[2]   # Entity(Contract)
    dg_country_col= df_dg.columns[8]   # 국가
    dg_customer_col=df_dg.columns[12]  # 계약대상 히든
    dg_map      = df_dg.set_index(dg_subjid_col)
    cx_map      = df_cx.set_index(df_cx.columns[0])
    cx_curr_col = df_cx.columns[4]

    all_rows     = []
    rate_cache   = {}
    sheets       = [("CA", df_ca), ("CB", df_cb)]
    total_sheets = sum(1 for _, d in sheets if d is not None and not d.empty)
    done         = 0

    for sheet_name, df in sheets:
        if df is None or df.empty:
            continue
        status.text(f"⏳ {sheet_name} 시트 처리 중... ({len(df):,}행)")
        progress.progress(int(done / total_sheets * 70))

        headers = list(df.columns)

        # 컬럼명으로 위치 자동 확인
        svc_cols    = [c for c in headers if c in SERVICE_MAP]
        tbd_col     = "TBD"    if "TBD"    in headers else None
        pf_type_col = "PF구분" if "PF구분" in headers else None
        pf_amt_col  = "PF"     if "PF"     in headers else None
        date_exp_col  = "청구예정일(C)"
        date_bill_col = "청구일(C)"
        date_pay_col  = "입금일(C)"

        for _, row in df.iterrows():
            subjid = str(row.iloc[0]).strip()
            if not subjid: continue
            if any(kw in subjid.upper() for kw in EXCLUDE_UPPER): continue

            # DG 정보 (컬럼명으로 접근 → iloc 밀림 없음)
            if subjid in dg_map.index:
                dg       = dg_map.loc[subjid]
                bd       = dg[dg_bd_col]
                entity   = dg[dg_entity_col]
                customer = dg[dg_customer_col]
                country  = dg[dg_country_col]
            else:
                bd = entity = customer = country = ""

            # 통화
            currency = str(cx_map.loc[subjid, cx_curr_col]).strip() \
                       if subjid in cx_map.index else ""
            if sheet_name == "CB":
                col_c = str(row.iloc[2]).upper()
                if "(USD)" in col_c:                               currency = "USD"
                elif "(EUR)" in col_c or "(EURO)" in col_c:        currency = "EUR"

            # 날짜
            d_exp  = row.get(date_exp_col,  "")
            d_bill = row.get(date_bill_col, "")
            d_pay  = row.get(date_pay_col,  "")
            target_date = pd.to_datetime(d_bill or d_exp, errors="coerce")

            # 환율 (캐시)
            ck = (currency, str(target_date))
            if ck not in rate_cache:
                rate_cache[ck] = find_rate(df_rate, currency, target_date)
            fx = rate_cache[ck]

            def add_row(lfpf, service, billing_amt):
                adj = round(billing_amt * fx, 0) \
                      if fx is not None and pd.notna(fx) else None
                all_rows.append({
                    "SUBJID": subjid, "BD": bd, "CRScube": entity,
                    "Customer": customer, "Country": country,
                    "LF/PF": lfpf, "Service": service, "Currency": currency,
                    "Billing Amt": billing_amt, "FX rate": fx,
                    "Adj. Billing Amt": adj,
                    "청구예정일(C)": d_exp, "청구일(C)": d_bill,
                    "입금일(C)": d_pay, "Source_Sheet": sheet_name
                })

            # ① 일반 서비스 컬럼
            for col in svc_cols:
                svc = SERVICE_MAP[col]
                val = pd.to_numeric(
                    str(row.get(col, "")).replace(",", ""), errors="coerce")
                if pd.isna(val) or val == 0:
                    if sheet_name == "CA" and col in MANDATORY_COLS:
                        add_row(svc["lfpf"], svc["service"], 0)
                    continue
                add_row(svc["lfpf"], svc["service"], val)

            # ② TBD (CA 전용)
            if tbd_col:
                tv = pd.to_numeric(
                    str(row.get(tbd_col, "")).replace(",", ""), errors="coerce")
                if not pd.isna(tv) and tv != 0:
                    add_row("LF", "기타", tv)

            # ③ PF구분 + PF
            if pf_type_col and pf_amt_col:
                pt = str(row.get(pf_type_col, "")).strip()
                pa = pd.to_numeric(
                    str(row.get(pf_amt_col, "")).replace(",", ""), errors="coerce")
                if pt and not pd.isna(pa) and pa != 0:
                    add_row("PF", PF_TYPE_MAP.get(pt, pt), pa)

        done += 1

    if not all_rows:
        progress.progress(100)
        status.text("⚠️ 처리할 데이터가 없습니다")
        return pd.DataFrame()

    progress.progress(90)
    status.text("📊 최종 데이터 생성 중...")
    result = pd.DataFrame(all_rows, columns=RESULT_COLS)
    progress.progress(100)
    status.text("✅ 완료!")
    return result

# ── 엑셀 생성 (SUBJID 앞 2글자 기준 시트 분리) ───────────────
@st.cache_data(show_spinner=False)
def build_excel_bytes(result_json):
    df = pd.read_json(io.StringIO(result_json), orient="split")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df["_prefix"] = df["SUBJID"].str[:2].str.upper()
        for prefix, grp in df.groupby("_prefix"):
            grp   = grp.drop(columns=["_prefix"])
            sname = f"{prefix}_Cleaned"
            grp.to_excel(writer, index=False, sheet_name=sname)
            ws     = writer.sheets[sname]
            k_idx  = grp.columns.get_loc("Adj. Billing Amt")
            k_ltr  = chr(65 + k_idx)
            for cell in ws[k_ltr][1:]:
                cell.number_format = "#,##0"
            for ci, cn in enumerate(grp.columns):
                ml = max(grp[cn].astype(str).map(len).max(), len(str(cn)))
                ws.column_dimensions[chr(65 + ci)].width = min(ml + 2, 40)
    output.seek(0)
    return output.getvalue()

# ── 메인 실행 ─────────────────────────────────────────────────
if source_file and rate_file:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 데이터 정제 시작", use_container_width=True, type="primary"):
            try:
                ext = source_file.name.rsplit(".", 1)[-1].lower()
                engine = "pyxlsb" if ext == "xlsb" else "openpyxl"

                with st.spinner("📖 파일 읽는 중..."):
                    df_dg = read_sheet(source_file, "DG", engine)
                    df_ca = read_sheet(source_file, "CA", engine)
                    df_cx = read_sheet(source_file, "CX", engine)
                    try:
                        df_cb = read_sheet(source_file, "CB", engine)
                        if df_cb.empty: raise ValueError
                    except Exception:
                        df_cb = pd.DataFrame()
                        st.info("ℹ️ CB 시트 없음 – CA만 처리합니다")
                    df_rate = load_rate_data(rate_file)

                missing = [n for n, d in [("DG",df_dg),("CA",df_ca),("CX",df_cx)]
                           if d.empty]
                if missing:
                    st.error(f"❌ 필수 시트 로드 실패: {', '.join(missing)}")
                    st.stop()

                result_df = process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate)

                if not result_df.empty:
                    st.session_state["result_df"]   = result_df
                    st.session_state["result_json"] = result_df.to_json(
                        orient="split", date_format="iso")
                else:
                    st.warning("⚠️ 처리할 데이터가 없습니다.")

            except Exception as e:
                st.error(f"❌ 오류 발생: {str(e)}")
                with st.expander("🔍 상세 오류"):
                    st.code(str(e))

    # ── 결과 표시 (session_state → 다운로드 후에도 유지) ─────
    if "result_df" in st.session_state:
        result_df = st.session_state["result_df"]
        st.success(f"✨ 정제 완료! 총 **{len(result_df):,}**개 행 생성")

        with st.expander("📋 데이터 미리보기 (상위 50행)", expanded=True):
            st.dataframe(result_df.head(50), use_container_width=True, height=400)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("총 행 수",    f"{len(result_df):,}")
        c2.metric("고유 SUBJID", f"{result_df['SUBJID'].nunique():,}")
        c3.metric("LF 서비스",   f"{(result_df['LF/PF']=='LF').sum():,}")
        c4.metric("PF 서비스",   f"{(result_df['LF/PF']=='PF').sum():,}")

        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.spinner("📦 다운로드 파일 준비 중..."):
                excel_bytes = build_excel_bytes(st.session_state["result_json"])
            st.download_button(
                label="📥 정제된 데이터 다운로드 (Excel)",
                data=excel_bytes,
                file_name=f"CRScube_Cleaned_v2.0_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
else:
    st.info("👈 왼쪽 사이드바에서 파일을 업로드해주세요.")
    with st.expander("📖 사용 방법"):
        st.markdown("""
        1. **원본 파일** (.xlsb / .xlsx / .xls) 업로드 — DG, CA, CB, CX 시트 포함
        2. **환율 파일** (.xlsx / .xls) 업로드 — Exchange Rate 또는 StdExRate 시트 포함
        3. **데이터 정제 시작** 버튼 클릭
        4. 처리 완료 후 Excel 다운로드 (SUBJID 앞 2글자 기준 시트 분리)
        """)
