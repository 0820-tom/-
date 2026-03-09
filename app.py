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
        
        # 환율 컬럼 찾기
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
    except Exception as e:
        st.sidebar.info(f"Exchange Rate 시트 확인 중... 다른 형식 시도")
    
    # 2단계: StdExRate 시트 시도 (레거시 형식)
    try:
        for header_row in range(20):
            try:
                temp = pd.read_excel(file, sheet_name='StdExRate', header=header_row).fillna('')
                temp.columns = [str(c).strip() for c in temp.columns]
                
                # 필수 컬럼 확인
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
        # 위와 동일한 로직...
        st.sidebar.warning("⚠️ 기본 시트로 환율 로드 시도")
    except:
        st.sidebar.error("❌ 환율 파일을 읽을 수 없습니다")
    
    return df_rate

# --- 핵심 처리 함수 (개선된 버전) ---
def process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate):
    """데이터 정제 핵심 로직"""
    
    # 인덱스 설정
    idx_subjid, idx_ac, idx_ad, idx_ae = 0, 28, 29, 30
    idx_dg_subjid, idx_dg_bd, idx_dg_entity, idx_dg_country, idx_dg_hidden = 0, 1, 2, 8, 12
    idx_cx_subjid, idx_cx_currency = 0, 4

    # 설정값들
    mandatory_cols = ["EDC", "IWRS", "PRO", "독립적평가자"]
    rename_dict = {
        "EDC": "cubeCDMS", 
        "IWRS": "cubeIWRS", 
        "PRO": "cubePRO", 
        "독립적평가자": "IE"
    }
    pf_original_names = ["DM", "DM/Set up", "STAT"]
    
    # 제외할 키워드 (대소문자 통일)
    exclude_keywords = [
        "TOTAL", "TOTAL AMT", "TOTAL 금액", "TOTAL 금액(C)",
        "합계", "소계", "SUBTOTAL", "SUM" "금액(S)"
    ]
    exclude_keywords_upper = [k.upper() for k in exclude_keywords]
    
    expanded_rows = []
    sheets_to_process = [("CA", df_ca), ("CB", df_cb)]
    
    # 진행 상황 표시
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 전체 작업량 계산
    total_work = sum(len(df) for _, df in sheets_to_process if df is not None)
    current_work = 0

    # 데이터 처리
    for sheet_name, df in sheets_to_process:
        if df is None or df.empty:
            continue
            
        service_cols = df.columns[4:22]
        sheet_rows = len(df)
        
        for i, (idx, row) in enumerate(df.iterrows()):
            current_work += 1
            
            # 진행률 업데이트 (최적화: 50줄마다)
            if current_work % 50 == 0 or current_work == total_work:
                progress_pct = int((current_work / total_work) * 100)
                progress_bar.progress(progress_pct)
                status_text.text(
                    f"⏳ {sheet_name} 시트 처리 중... "
                    f"({i+1}/{sheet_rows} 행) - 전체 {progress_pct}% 완료"
                )
            
            # SUBJID 필터링 (Total 행 제외)
            subjid_val = str(row[df.columns[idx_subjid]]).strip()
            if any(kw in subjid_val.upper() for kw in exclude_keywords_upper):
                continue

            # 서비스 컬럼 처리
            for col in service_cols:
                col_clean = str(col).strip()
                
                # Total 컬럼 제외 (개선된 체크)
                if col_clean.upper() in exclude_keywords_upper:
                    continue
                
                # 값 파싱
                val = pd.to_numeric(str(row[col]).replace(',', ''), errors='coerce')
                
                # 행 생성 조건 확인
                should_create_row = False
                
                if sheet_name == "CA":
                    # CA: 필수 항목 또는 0보다 큰 값
                    if col_clean in mandatory_cols or (pd.notnull(val) and val != 0):
                        should_create_row = True
                        
                elif sheet_name == "CB":
                    # CB: 0보다 큰 값만
                    if pd.notnull(val) and val != 0:
                        should_create_row = True
                
                if should_create_row:
                    new_row = row.copy()
                    new_row['Source_Sheet'] = sheet_name
                    new_row['LF_PF_Final'] = 'PF' if col_clean in pf_original_names else 'LF'
                    new_row['Service_Name_Final'] = rename_dict.get(col_clean, col_clean)
                    new_row['Specific_Billing_Amt'] = val if pd.notnull(val) else 0
                    
                    # CB 시트 예외사항: C열(인덱스 2)에 특정 통화 표기 시 환율/통화 지정 오버라이드
                    override_currency = None
                    if sheet_name == "CB":
                        col_c_val = str(row.iloc[2]).upper()
                        if '(USD)' in col_c_val:
                            override_currency = 'USD'
                        elif '(EUR)' in col_c_val or '(EURO)' in col_c_val:
                            override_currency = 'EUR'
                    new_row['Override_Currency'] = override_currency
                    
                    expanded_rows.append(new_row)

    # 데이터가 없는 경우 처리
    if not expanded_rows:
        progress_bar.progress(100)
        status_text.text("⚠️ 처리할 데이터가 없습니다")
        return pd.DataFrame()

    # DataFrame 생성 및 병합
    df_expanded = pd.DataFrame(expanded_rows)
    progress_bar.progress(70)
    status_text.text("🔗 마스터 데이터 병합 중...")
    
    # DG, CX 데이터 병합
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

    # 날짜 처리
    ad_col = df_ca.columns[idx_ad]
    ac_col = df_ca.columns[idx_ac]
    ae_col = df_ca.columns[idx_ae]
    
    merged['Target_Date'] = pd.to_datetime(
        merged[ad_col].fillna(merged[ac_col]), 
        errors='coerce'
    )
    
    progress_bar.progress(85)
    status_text.text("💱 환율 매칭 중...")
    
    # 환율 찾기 함수 (개선된 버전)
    def find_rate(row):
        """정확한 통화 매칭으로 환율 찾기"""
        if pd.notnull(row.get('Override_Currency')):
            target_curr = str(row['Override_Currency']).strip().upper()
        else:
            curr_col = df_cx.columns[idx_cx_currency]
            target_curr = str(row[curr_col]).strip().upper()
        
        # KRW는 환율 1
        if target_curr in ['KRW', 'NAN', '', 'NONE']:
            return 1.0
            
        # 환율 데이터가 없는 경우
        if df_rate is None or df_rate.empty:
            return None
        
        # 유연한 통화 매칭 방식 (contains 사용)
        currency_match = df_rate['Currency'].str.contains(target_curr, na=False, case=False)
        date_match = df_rate['Date'] <= row['Target_Date']
        
        possible_rates = df_rate[currency_match & date_match]
        
        if not possible_rates.empty:
            # 가장 최근 환율 사용
            latest_rate = possible_rates.iloc[-1]['Rate']
            return float(str(latest_rate).replace(',', ''))
        
        return None

    # 최종 데이터프레임 생성
    progress_bar.progress(95)
    status_text.text("📊 최종 데이터 생성 중...")
    
    final = pd.DataFrame()
    final['SUBJID'] = merged[subjid_col]
    final['BD'] = merged[df_dg.columns[idx_dg_bd]]
    final['CRScube'] = merged[df_dg.columns[idx_dg_entity]]
    final['Customer'] = merged[df_dg.columns[idx_dg_hidden]]
    final['Country'] = merged[df_dg.columns[idx_dg_country]]
    final['LF/PF'] = merged['LF_PF_Final']
    final['Service'] = merged['Service_Name_Final']
    
    # 통화는 예외사항(Override_Currency)를 우선 적용
    final['Currency'] = merged.apply(
        lambda r: r['Override_Currency'] if pd.notnull(r.get('Override_Currency')) else r[df_cx.columns[idx_cx_currency]], 
        axis=1
    )
    final['Billing Amt'] = merged['Specific_Billing_Amt']
    
    # 환율 적용
    final['FX rate'] = merged.apply(find_rate, axis=1)
    # 데이터 단계에서도 반올림 수행 (엑셀 표시는 하단의 number_format 로직으로 유지)
    final['Adj. Billing Amt'] = final.apply(
        lambda r: round(r['Billing Amt'] * r['FX rate'], 0) if pd.notnull(r['FX rate']) else None, 
        axis=1
    )
    
    # 날짜 정보
    final['청구예정일(C)'] = merged[ac_col]
    final['청구일(C)'] = merged[ad_col]
    final['입금일(C)'] = merged[ae_col]
    
    progress_bar.progress(100)
    status_text.text("✅ 모든 정제 작업 완료!")
    
    return final

# --- 메인 실행 섹션 ---
if source_file and rate_file:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 데이터 정제 시작", width='stretch', type="primary"):
            try:
                # 데이터 로드
                with st.spinner("📖 엑셀 파일 읽는 중..."):
                    df_dg = pd.read_excel(source_file, sheet_name='DG', engine='pyxlsb')
                    df_ca = pd.read_excel(source_file, sheet_name='CA', engine='pyxlsb')
                    
                    # CB 시트는 선택적
                    try:
                        df_cb = pd.read_excel(source_file, sheet_name='CB', engine='pyxlsb')
                    except:
                        df_cb = None
                        st.info("ℹ️ CB 시트가 없습니다. CA 시트만 처리합니다.")
                    
                    df_cx = pd.read_excel(source_file, sheet_name='CX', engine='pyxlsb')
                    df_rate = load_rate_data(rate_file)
                
                # 데이터 처리
                result_df = process_combined_logic(df_dg, df_ca, df_cb, df_cx, df_rate)
                
                # 결과 표시
                if not result_df.empty:
                    st.success(f"✨ 정제 완료! 총 **{len(result_df):,}**개 행이 생성되었습니다.")
                    
                    # 데이터 미리보기
                    with st.expander("📋 데이터 미리보기 (상위 50행)", expanded=True):
                        st.dataframe(
                            result_df.head(50),
                            width='stretch',
                            height=400
                        )
                    
                    # 통계 정보
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("총 행 수", f"{len(result_df):,}")
                    with col2:
                        st.metric("고유 SUBJID", f"{result_df['SUBJID'].nunique():,}")
                    with col3:
                        st.metric("LF 서비스", f"{(result_df['LF/PF']=='LF').sum():,}")
                    with col4:
                        st.metric("PF 서비스", f"{(result_df['LF/PF']=='PF').sum():,}")
                    
                    # 다운로드 섹션
                    st.markdown("---")
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        # 엑셀 파일 생성
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            result_df.to_excel(writer, index=False, sheet_name='Cleaned_Data')
                            
                            # 자동 열 너비 조정
                            worksheet = writer.sheets['Cleaned_Data']
                            for column in result_df:
                                column_length = max(result_df[column].astype(str).map(len).max(), len(column))
                                col_idx = result_df.columns.get_loc(column)
                                worksheet.column_dimensions[chr(65 + col_idx)].width = min(column_length + 2, 50)
                                
                            # K열(Adj. Billing Amt) 엑셀 셀 표시 형식 변경 (소수점 제거 및 반올림 표시)
                            for cell in worksheet['K'][1:]: # 헤더 제외
                                cell.number_format = '#,##0'

                        output.seek(0)
                        
                        # 다운로드 버튼
                        st.download_button(
                            label="📥 정제된 데이터 다운로드 (Excel)",
                            data=output.getvalue(),
                            file_name=f"CRScube_Cleaned_v1.0_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            width='stretch'
                        )
                else:
                    st.warning("⚠️ 처리할 데이터가 없습니다. 입력 파일을 확인해주세요.")
                    
            except Exception as e:
                st.error(f"❌ 오류 발생: {str(e)}")
                with st.expander("🔍 상세 오류 정보"):
                    st.code(str(e))
                st.info("💡 파일 형식과 시트명이 올바른지 확인해주세요.")
else:
    # 파일 업로드 안내
    st.info("👈 왼쪽 사이드바에서 파일을 업로드해주세요.")
    
    with st.expander("📖 사용 방법"):
        st.markdown("""
        1. **원본 파일 (.xlsb)**: DG, CA, CB, CX 시트가 포함된 파일
        2. **환율 파일 (.xlsx/.xls)**: Exchange Rate 또는 StdExRate 시트 포함
        3. **데이터 정제 시작** 버튼 클릭
        4. 처리 완료 후 결과 다운로드
        """)
