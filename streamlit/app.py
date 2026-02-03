import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import uuid
from datetime import datetime
import os
from typing import Final
from dotenv import load_dotenv

# Config & Auth
APP_ENV: Final = os.getenv("APP_ENV", "development")

def get_sheet_id() -> str:
    if APP_ENV == "production":
        return os.getenv("SHEET_ID_PROD")
    else: 
        return os.getenv("SHEET_ID_DEV")

if os.path.exists("../.env"):
    load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SHEET_ID = get_sheet_id()

subject_list = ["Math", "Portuguese", "English", "Japanese"]
stages_dict = {
    "Math": ["6A", "5A", "4A", "3A", "2A", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"],
    "Portuguese": ["7A", "6A", "5A", "4A", "3A", "2A", "AI", "AII", "BI", "BII", "CI", "CII", "DI", "DII", "EI", "EII", "FI", "FII", "GI", "GII", "HI", "HII", "II", "III", "J", "K", "L"],
    "English": ["7A", "6A", "5A", "4A", "3A", "2A", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"],
    "Japanese": ["4A", "3A", "2A", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"],
}
grades_list = ["F5", "F4", "F3", "F2", "F1", "1EF", "2EF", "3EF", "4EF", "5EF", "6EF", "7EF", "8EF", "9EF", "1EM", "2EM", "3EM", "AD", "EE"]
fct_status_list = ["current", "new", "new_multi", "new_former", "absent", "absent_graduate", "absent_transfer"]
# Service Functions

@st.cache_resource
def get_sheets_service():
    if not json_path:
        st.error("Variable GOOGLE_APPLICATION_CREDENTIALS not found.")
        st.stop()
    creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
    creds = creds.with_quota_project("kumon-analytics-automation")
    return build('sheets', 'v4', credentials=creds)

def load_data_official(range_name):
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=range_name
        ).execute()
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()
        headers = [h.strip() for h in values[0]]
        return pd.DataFrame(values[1:], columns=headers)
    except Exception as e:
        st.error(f"Could not load data at ({range_name}): {e}")
        return pd.DataFrame()

def save_new_row(range_name, data_dict):
    """For individual records - low frequency writes"""
    service = get_sheets_service()
    values = [list(data_dict.values())]
    body = {'values': values}
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def batch_append_rows(range_name, rows_list):
    """Writes multiple lines at once - batch writes"""
    if not rows_list:
        return
    service = get_sheets_service()
    body = {'values': rows_list}
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def batch_update_students(df_all_students):
    service = get_sheets_service()
    df_to_save = df_all_students.fillna("")
    values = [df_to_save.columns.tolist()] + df_to_save.values.tolist()
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="dim_students",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

# Interface
st.set_page_config(page_title="Kumon Analytics - Gestão", layout="wide")

if 'df_students' not in st.session_state or 'df_fact' not in st.session_state or st.sidebar.button("🔄 Atualizar Dados"):
    with st.spinner("Carregando base de dados do Google Sheets..."):
        st.session_state.df_students = load_data_official("dim_students!A:Z")
        st.session_state.df_fact = load_data_official("fct_status_report!A:Z")

st.sidebar.title("Navegação - Legal")
page = st.sidebar.radio("",["Cadastro de Alunos", "Relatório de Performance"])

# Page: Student Enroll
if page == "Cadastro de Alunos":
    st.header("👤 Gestão de Alunos")
    
    with st.expander("➕ Cadastrar Novo Aluno"):
        with st.form("form_cadastro"):
            kumon_id = st.text_input("ID Kumon")
            name = st.text_input("Nome Completo")
            col1, col2 = st.columns(2)
            gender = col1.selectbox("Gênero", ["male", "female"])
            birth_date = col2.date_input("Data de Nascimento", min_value=datetime(1900,1,1), max_value=datetime.today())
            current_grade = col1.selectbox("Série Escolar", grades_list)
            enroll_date_sub = col2.date_input("Data de Matrícula", min_value=datetime(2022,2,1))
            subject = col1.selectbox("Disciplina", subject_list)
            current_stage = col2.selectbox("Estágio Inicial", stages_dict[subject])
            type = col1.selectbox("Tipo", ["connect", "paper"])
            status = col2.selectbox("Status", fct_status_list[1:4])

            submit = st.form_submit_button("Salvar Aluno", type="primary")
            if submit:
                if kumon_id and name:
                    new_student = {
                        "student_id": str(uuid.uuid4()),
                        "kumon_id": int(kumon_id),
                        "name": name.upper(),
                        "gender": gender,
                        "birth_date": datetime.strftime(birth_date, "%Y-%m-%d"),
                        "current_grade": current_grade,
                        "subject": subject.upper(),
                        "current_stage": current_stage,
                        "enroll_date_sub": datetime.strftime(enroll_date_sub, "%Y-%m-%d"),
                        "type": type,
                        "status": status,
                        "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    save_new_row("dim_students", new_student)
                    st.success("Aluno cadastrado!")
                    st.session_state.df_students = load_data_official("dim_students")
                    st.rerun()

    st.subheader("Lista de Alunos (Cadastral)")
    st.dataframe(st.session_state.df_students[[
                "name",
                "subject",
                "current_grade",
                "current_stage",
                "type",]].loc[st.session_state.df_students["status"].isin(fct_status_list[0:4])].sort_values(by="name"),
                hide_index=True)

# Page: Performance Report
elif page == "Relatório de Performance":
    st.header("📊 Lançamento de Performance Mensal")
    
    df_students_master = st.session_state.df_students.copy()
    df_fact_master = st.session_state.df_fact.copy()
    
    if df_students_master.empty:
        st.warning("Nenhum dado encontrado na planilha dim_students.")
        st.stop()

    if not df_fact_master.empty:
        df_fact_master['report_date'] = pd.to_datetime(df_fact_master['report_date'])
        latest_perf = df_fact_master.sort_values('report_date', ascending=False).drop_duplicates('student_id', keep='first')
        latest_perf = latest_perf[['student_id', 'current_lesson', 'stage', 'advanced']].rename(columns={
            'current_lesson': 'last_lesson',
            'stage': 'last_stage_fact',
            'advanced':'advanced'
        })
        df_work = df_students_master.merge(latest_perf, on='student_id', how='left')
    else:
        df_work = df_students_master.copy()
        df_work['last_lesson'] = "1"
        df_work['last_stage_fact'] = None

    df_work['last_stage'] = df_work['last_stage_fact'].fillna(df_work['current_stage'])
    df_work['last_lesson'] = df_work['last_lesson'].fillna("10")
    df_active = df_work[df_work["status"].isin(fct_status_list[0:4])].copy()
    
    tabs = st.tabs(subject_list)
    all_updates = []

    for i, sub in enumerate(subject_list):
        with tabs[i]:
            df_sub = df_active[df_active["subject"] == sub.upper()].copy()
            if df_sub.empty:
                st.write(f"Nenhum aluno ativo em {sub}.")
                continue

            df_sub["new_stage"] = df_sub["last_stage"]
            df_sub["new_lesson"] = df_sub["last_lesson"]
            df_sub["total_sheets"] = 0
            
            cols_to_show = ["student_id", "name", "subject", "type", "current_grade", "last_stage", "last_lesson", "new_stage", "new_lesson", "total_sheets", "advanced", "status"]
            
            edited_df = st.data_editor(
                df_sub[cols_to_show].sort_values("name"),
                column_config={
                    "student_id": None,
                    "name": st.column_config.TextColumn("Nome", disabled=True),
                    "subject": st.column_config.TextColumn("Subject", disabled=True),
                    "type": st.column_config.TextColumn("Type", disabled=True),
                    "current_grade": st.column_config.TextColumn("Série", disabled=True),
                    "last_stage": st.column_config.TextColumn("Estágio Ant.", disabled=True),
                    "last_lesson": st.column_config.TextColumn("Lição Ant.", disabled=True),
                    "new_stage": st.column_config.SelectboxColumn("Novo Estágio", options=stages_dict[sub], required=True),
                    "new_lesson": st.column_config.SelectboxColumn("Nova Lição", options=range(10,210,10), required=True),
                    "total_sheets": st.column_config.NumberColumn("Folhas", min_value=0, step=1, required=True),
                    "advanced": None,
                    "status": st.column_config.SelectboxColumn("Status", options=fct_status_list, required=True)
                },
                key=f"editor_{sub}",
                use_container_width=True,
                hide_index=True
            )
            all_updates.append(edited_df)

    st.markdown("---")
    confirm = st.checkbox("Confirmo que revisei os dados acima e desejo salvar o relatório.")

    if st.button("💾 Salvar Lançamentos Mensais", type="primary", disabled=not confirm):
        with st.spinner("Sincronizando dados em lote..."):
            final_df = pd.concat(all_updates).fillna("")
            current_month_ref = datetime.now().strftime("%Y-%m-01")
            
            facts_to_save = []

            for _, row in final_df.iterrows():
                # 1. Prepare row for fact table (list forma for batch update)
                fact_entry = [
                    str(uuid.uuid4()),
                    row["student_id"],
                    current_month_ref,
                    row["subject"],
                    row["type"],
                    row["current_grade"],
                    row["new_stage"],
                    row["new_lesson"],
                    row["total_sheets"],
                    row["advanced"],
                    row["status"],
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
                print(fact_entry)
                facts_to_save.append(fact_entry)
                
                # 2. Update current state in dim_students
                idx = st.session_state.df_students.index[st.session_state.df_students['student_id'] == row['student_id']]
                st.session_state.df_students.loc[idx, "current_stage"] = row["new_stage"]
                st.session_state.df_students.loc[idx, "status"] = row["status"] if row["status"] not in ("new", "new_multi", "new_transfer") else "current"

            # Batch Operations
            
            # 1. Batch Append - fct_status_report
            batch_append_rows("fct_status_report", facts_to_save)
            
            # 2. Batch Update - dim_students (overwrite)
            batch_update_students(st.session_state.df_students)
            
            # Reload_facts_for_session
            st.session_state.df_fact = load_data_official("fct_status_report")
            
            st.success(f"Report processed successfully: {len(facts_to_save)} records saved.")
            st.balloons()