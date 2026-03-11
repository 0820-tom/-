import streamlit as st
import pandas as pd
import datetime
import io
import warnings
warnings.filterwarnings('ignore')

# --- 페이지 설정 ---
st.set_page_config(
    page_title="CRScube 비용 데이터 정제 Ver 1.0", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 CRScube 비용 데이터 정제 Ver 1.0")
st.markdown("""
### 🎯 주요 기능
- **CA 시트**: 필수 항목(EDC, IWRS, PRO, 독립적평가자) 포함, 0원 허용
- **CB 시트**: 0원 제외, Total/합계 컬럼 자동 제외
- **환율 자동 매칭**: Exchange Rate 또는 StdExRate 시트 자동 인식
- **진행 상황**: 실시간 처리 진행률 표시
""")

# --- 사이드바: 파일 업로드 ---
st.sidebar.header("📂 파일 업로드")
st.sidebar.markdown("---")
source_file = st.sidebar.file_uploader(
    "1️⃣ 원본 파일 (.xlsb)", 
    type=["xlsb"],
    help="DG, CA, CB, CX 시트가 포함된 파일"
)
rate_file = st.sidebar.file_uploader(
    "2️⃣ 환율 파일 (.xlsx/.xls)", 
    type=["xlsx", "xls"],
    help="Exchange Rate 또는 StdExRate 시트 포함"
)

# --- 환율 로드 함수 (개선된 버전) ---
def load_rate_data(file):
    """환율 데이터를 로드하는 함수 (다양한 형식 지원)"""
    df_rate = None
    
    # 1단계: Exchange Rate 시트 시도 (최신 형식)
    try:
        df_rate = pd.read_excel(file, sheet_name='Exchange Rate', header=3).fillna('')
        df_rate.columns = [str(c).strip() for c in df_rate.columns]
        
        rate_cols = [c for c in df_rate.columns if any(k in c for k in ['환율', '매매기준율', 'Rate'])]
        if rate_cols:
            rename_map = {
                rate_cols[0]: 'Rate',
                '날짜': 'Date',
                '일자': 'Date', 
                '통화': 'Currency',
                '통화명': 'Currency'
            }
            df_rate = df_rate.rename(columns={k: v for k, v in rename_map.items() if k in df_rate.columns})
            
            if 'Date' in df_rate.columns and 'Currency' in df_rate.columns and 'Rate' in df_rate.columns:
                df_rate['Date'] = pd.to_datetime(df_rate['Date'], errors='coerce')
                df_rate = df_rate.dropna(subset=['Date']).sort_values('Date')
                st.sidebar.success("✅ Exchange Rate 시트 로드 성공")
                return df_rate
    except Exception:
        st.sidebar.info(f"Exchange Rate 시트 확인 중... 다른 형식 시도")
    
    # 2단계: StdExRate 시트 시도 (레거시 형식)
    try:
        for header_row in range(20):
            try:
                temp = pd.read_excel(file, sheet_name='StdExRate', header=header_row).fillna('')
                temp.columns = [str(c).strip() for c in temp.columns]
                
                has_date = any(x in temp.columns for x in ['Date', '날짜', '일자'])
                has_currency = any(x in temp.columns for x in ['Currency', '통화', '통화명'])
                has_rate = any(x in temp.columns for x in ['Rate', '환율', '매매기준율'])
                
                if has_date and has_currency and has_rate:
                    rename_map = {
                        '날짜': 'Date', '일자': 'Date',
                        '통화': 'Currency', '통화명': 'Currency',
                        '매매기준율': 'Rate', '환율': 'Rate'
                    }
                    df_rate = temp.rename(columns={k: v for k, v in rename_map.items() if k in temp.columns})
                    df_rate['Date'] = pd.to_datetime(df_rate['Date'], errors='coerce')
                    df_rate = df_rate.dropna(subset=['Date']).sort_values('Date')
                    st.sidebar.success(f"✅ StdExRate 시트 로드 성공 (헤더: {header_row}행)")
                    return df_rate
            except:
                continue
    except:
        pass
    
    # 3단계: 첫 번째 시트 시도 (최후 수단)
    try:
        df_rate = pd.read_excel(file, sheet_name=0, header=0).fillna('')
        st.sidebar.warning("⚠️ 기본 시트로 환율 로드 시도")
    except:
        st.sidebar.error("❌ 환율 파일을 읽을 수 없습니다")
    
    return df_rate

# --- 핵심 처리 함수 (벡터화로 속도 개선) ---
def process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate):
    """데이터 정제 핵심 로직 (벡터화 처리로 속도 향상)"""
    
    idx_subjid, idx_ac, idx_ad, idx_ae = 0, 28, 29, 30
    idx_dg_subjid, idx_dg_bd, idx_dg_entity, idx_dg_country, idx_dg_hidden = 0, 1, 2, 8, 12
    idx_cx_subjid, idx_cx_currency = 0, 4

    mandatory_cols = ["EDC", "IWRS", "PRO", "독립적평가자"]
    rename_dict = {
        "EDC": "cubeCDMS", 
        "IWRS": "cubeIWRS", 
        "PRO": "cubePRO", 
        "독립적평가자": "IE"
    }
    pf_original_names = ["DM", "DM/Set up", "STAT"]
    
    exclude_keywords = [
        "TOTAL", "TOTAL AMT", "TOTAL 금액", "TOTAL 금액(C)",
        "합계", "소계", "SUBTOTAL", "SUM" "금액(S)"
    ]
    exclude_keywords_upper = [k.upper() for k in exclude_keywords]
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    sheets_to_process = [("CA", df_ca), ("CB", df_cb)]
    all_melted = []
    
    total_sheets = sum(1 for _, df in sheets_to_process if df is not None and not df.empty)
    sheet_done = 0

    for sheet_name, df in sheets_to_process:
        if df is None or df.empty:
            continue

        status_text.text(f"⏳ {sheet_name} 시트 전처리 중...")
        progress_bar.progress(int(sheet_done / total_sheets * 50))

        service_cols = list(df.columns[4:22])
        
        # ── SUBJID 필터링 (Total 행 제외) ──────────────────────────────
        subjid_col = df.columns[idx_subjid]
        mask = ~df[subjid_col].astype(str).str.strip().str.upper().apply(
            lambda v: any(kw in v for kw in exclude_keywords_upper)
        )
        df = df[mask].copy()

        # ── 서비스 컬럼 필터 (Total 컬럼 제외) ────────────────────────
        valid_service_cols = [
            c for c in service_cols
            if str(c).strip().upper() not in exclude_keywords_upper
        ]

        # ── 값 변환 (벡터화) ───────────────────────────────────────────
        for col in valid_service_cols:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',', '', regex=False),
                errors='coerce'
            )

        # ── melt로 long format 변환 ────────────────────────────────────
        id_cols = list(df.columns[:4]) + list(df.columns[22:])
        melted = df.melt(
            id_vars=id_cols,
            value_vars=valid_service_cols,
            var_name='_col_name',
            value_name='Specific_Billing_Amt'
        )
        melted['Source_Sheet'] = sheet_name

        # ── 행 생성 조건 필터링 ────────────────────────────────────────
        if sheet_name == "CA":
            keep = (
                melted['_col_name'].isin(mandatory_cols) |
                (melted['Specific_Billing_Amt'].notna() & (melted['Specific_Billing_Amt'] != 0))
            )
        else:  # CB
            keep = melted['Specific_Billing_Amt'].notna() & (melted['Specific_Billing_Amt'] != 0)
        melted = melted[keep].copy()

        # ── LF/PF, Service 이름 ────────────────────────────────────────
        melted['LF_PF_Final'] = melted['_col_name'].apply(
            lambda c: 'PF' if c in pf_original_names else 'LF'
        )
        melted['Service_Name_Final'] = melted['_col_name'].map(rename_dict).fillna(melted['_col_name'])
        melted['Specific_Billing_Amt'] = melted['Specific_Billing_Amt'].fillna(0)

        # ── CB 통화 예외사항 ───────────────────────────────────────────
        if sheet_name == "CB":
            col_c = df.columns[2]
            col_c_upper = melted[col_c].astype(str).str.upper()
            melted['Override_Currency'] = None
            melted.loc[col_c_upper.str.contains(r'\(USD\)', regex=True), 'Override_Currency'] = 'USD'
            melted.loc[
                col_c_upper.str.contains(r'\(EUR\)', regex=True) |
                col_c_upper.str.contains(r'\(EURO\)', regex=True),
                'Override_Currency'
            ] = 'EUR'
        else:
            melted['Override_Currency'] = None

        all_melted.append(melted)
        sheet_done += 1

    if not all_melted:
        progress_bar.progress(100)
        status_text.text("⚠️ 처리할 데이터가 없습니다")
        return pd.DataFrame()

    df_expanded = pd.concat(all_melted, ignore_index=True)

    # ── DG, CX 병합 ────────────────────────────────────────────────────
    progress_bar.progress(60)
    status_text.text("🔗 마스터 데이터 병합 중...")

    subjid_col = df_ca.columns[idx_subjid]

    merged = pd.merge(
        df_expanded,
        df_dg.iloc[:, [idx_dg_subjid, idx_dg_bd, idx_dg_entity, idx_dg_hidden, idx_dg_country]],
        left_on=subjid_col,
        right_on=df_dg.columns[idx_dg_subjid],
        how='left'
    )
    merged = pd.merge(
        merged,
        df_cx.iloc[:, [idx_cx_subjid, idx_cx_currency]],
        left_on=subjid_col,
        right_on=df_cx.columns[idx_cx_subjid],
        how='left'
    )

    # ── 날짜 처리 ──────────────────────────────────────────────────────
    ad_col = df_ca.columns[idx_ad]
    ac_col = df_ca.columns[idx_ac]
    ae_col = df_ca.columns[idx_ae]

    merged['Target_Date'] = pd.to_datetime(
        merged[ad_col].fillna(merged[ac_col]),
        errors='coerce'
    )

    # ── 환율 매칭 (캐시로 속도 향상) ──────────────────────────────────
    progress_bar.progress(80)
    status_text.text("💱 환율 매칭 중...")

    curr_col = df_cx.columns[idx_cx_currency]

    def find_rate_cached(target_curr, target_date):
        """캐시 기반 환율 찾기"""
        if pd.isnull(target_date):
            return None
        tc = str(target_curr).strip().upper()
        if tc in ('KRW', 'NAN', '', 'NONE'):
            return 1.0
        if df_rate is None or df_rate.empty:
            return None
        currency_match = df_rate['Currency'].str.contains(tc, na=False, case=False)
        date_match = df_rate['Date'] <= target_date
        possible = df_rate[currency_match & date_match]
        if not possible.empty:
            return float(str(possible.iloc[-1]['Rate']).replace(',', ''))
        return None

    # (통화, 날짜) 조합별로 한 번만 계산
    rate_cache = {}
    merged['_eff_currency'] = merged.apply(
        lambda r: r['Override_Currency'] if pd.notnull(r.get('Override_Currency')) else r[curr_col],
        axis=1
    )
    unique_pairs = merged[['_eff_currency', 'Target_Date']].drop_duplicates()
    for _, pair in unique_pairs.iterrows():
        key = (pair['_eff_currency'], pair['Target_Date'])
        if key not in rate_cache:
            rate_cache[key] = find_rate_cached(pair['_eff_currency'], pair['Target_Date'])

    merged['FX rate'] = merged.apply(
        lambda r: rate_cache.get((r['_eff_currency'], r['Target_Date'])), axis=1
    )

    # ── 최종 DataFrame 구성 ────────────────────────────────────────────
    progress_bar.progress(95)
    status_text.text("📊 최종 데이터 생성 중...")

    final = pd.DataFrame()
    final['SUBJID']        = merged[subjid_col]
    final['BD']            = merged[df_dg.columns[idx_dg_bd]]
    final['CRScube']       = merged[df_dg.columns[idx_dg_entity]]
    final['Customer']      = merged[df_dg.columns[idx_dg_hidden]]
    final['Country']       = merged[df_dg.columns[idx_dg_country]]
    final['LF/PF']         = merged['LF_PF_Final']
    final['Service']       = merged['Service_Name_Final']
    final['Currency']      = merged['_eff_currency']
    final['Billing Amt']   = merged['Specific_Billing_Amt']
    final['FX rate']       = merged['FX rate']
    final['Adj. Billing Amt'] = final.apply(
        lambda r: round(r['Billing Amt'] * r['FX rate'], 0) if pd.notnull(r['FX rate']) else None,
        axis=1
    )
    final['청구예정일(C)'] = merged[ac_col]
    final['청구일(C)']     = merged[ad_col]
    final['입금일(C)']     = merged[ae_col]

    progress_bar.progress(100)
    status_text.text("✅ 모든 정제 작업 완료!")

    return final

# ── 엑셀 파일 생성 (압축 최적화, 용량 최소화) ──────────────────────────
@st.cache_data(show_spinner=False)
def build_excel_bytes(result_df_json: str) -> bytes:
    """결과 DataFrame을 압축된 xlsx 바이트로 변환 (캐시 적용)"""
    result_df = pd.read_json(io.StringIO(result_df_json), orient='split')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, index=False, sheet_name='Cleaned_Data')

        workbook  = writer.book
        worksheet = writer.sheets['Cleaned_Data']

        # K열 숫자 포맷 (#,##0)
        num_fmt = workbook.add_format({'num_format': '#,##0'})
        k_col_idx = result_df.columns.get_loc('Adj. Billing Amt')
        worksheet.set_column(k_col_idx, k_col_idx, 16, num_fmt)

        # 열 너비 자동 조정 (최대 40자로 제한)
        for col_idx, col_name in enumerate(result_df.columns):
            max_len = max(
                result_df[col_name].astype(str).map(len).max(),
                len(str(col_name))
            )
            if col_idx != k_col_idx:
                worksheet.set_column(col_idx, col_idx, min(max_len + 2, 40))

    output.seek(0)
    return output.getvalue()


# ── 메인 실행 섹션 ─────────────────────────────────────────────────────
if source_file and rate_file:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 데이터 정제 시작", use_container_width=True, type="primary"):
            try:
                with st.spinner("📖 엑셀 파일 읽는 중..."):
                    df_dg = pd.read_excel(source_file, sheet_name='DG', engine='pyxlsb')
                    df_ca = pd.read_excel(source_file, sheet_name='CA', engine='pyxlsb')
                    try:
                        df_cb = pd.read_excel(source_file, sheet_name='CB', engine='pyxlsb')
                    except:
                        df_cb = None
                        st.info("ℹ️ CB 시트가 없습니다. CA 시트만 처리합니다.")
                    df_cx = pd.read_excel(source_file, sheet_name='CX', engine='pyxlsb')
                    df_rate = load_rate_data(rate_file)

                result_df = process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate)

                if not result_df.empty:
                    # ✅ session_state에 저장 → 다운로드 후에도 화면 유지
                    st.session_state['result_df'] = result_df
                    st.session_state['result_json'] = result_df.to_json(orient='split', date_format='iso')
                else:
                    st.warning("⚠️ 처리할 데이터가 없습니다. 입력 파일을 확인해주세요.")

            except Exception as e:
                st.error(f"❌ 오류 발생: {str(e)}")
                with st.expander("🔍 상세 오류 정보"):
                    st.code(str(e))
                st.info("💡 파일 형식과 시트명이 올바른지 확인해주세요.")

    # ── 결과 표시 (session_state 기반 → 다운로드 후에도 유지) ─────────
    if 'result_df' in st.session_state:
        result_df = st.session_state['result_df']

        st.success(f"✨ 정제 완료! 총 **{len(result_df):,}**개 행이 생성되었습니다.")

        with st.expander("📋 데이터 미리보기 (상위 50행)", expanded=True):
            st.dataframe(result_df.head(50), use_container_width=True, height=400)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("총 행 수", f"{len(result_df):,}")
        with col2:
            st.metric("고유 SUBJID", f"{result_df['SUBJID'].nunique():,}")
        with col3:
            st.metric("LF 서비스", f"{(result_df['LF/PF']=='LF').sum():,}")
        with col4:
            st.metric("PF 서비스", f"{(result_df['LF/PF']=='PF').sum():,}")

        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.spinner("📦 다운로드 파일 준비 중..."):
                excel_bytes = build_excel_bytes(st.session_state['result_json'])

            st.download_button(
                label="📥 정제된 데이터 다운로드 (Excel)",
                data=excel_bytes,
                file_name=f"CRScube_Cleaned_v1.0_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
else:
    st.info("👈 왼쪽 사이드바에서 파일을 업로드해주세요.")
    with st.expander("📖 사용 방법"):
        st.markdown("""
        1. **원본 파일 (.xlsb)**: DG, CA, CB, CX 시트가 포함된 파일
        2. **환율 파일 (.xlsx/.xls)**: Exchange Rate 또는 StdExRate 시트 포함
        3. **데이터 정제 시작** 버튼 클릭
        4. 처리 완료 후 결과 다운로드
        """)
