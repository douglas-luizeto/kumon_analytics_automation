import pandas as pd
import uuid
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SOURCE_ID = os.getenv("SOURCE_ID")
DESTINATION_ID = os.getenv("DESTINATION_ID")


def get_gspread_client(json_key_path: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(json_key_path, scopes=SCOPES)
    return gspread.authorize(creds)


def migrate_dim_students(sh: gspread.Spreadsheet, df_raw: pd.DataFrame) -> pd.DataFrame:
    df_students = (
        df_raw[["kumon_id", "name", "birth_date", "enroll_date", "gender"]]
        .drop_duplicates(subset="kumon_id")
        .copy()
    )
    # Create student_id surrogate key
    df_students["student_id"] = [str(uuid.uuid4()) for _ in range(len(df_students))]

    # Create status column that states whether a student is active considering the last report_date
    last_date = df_raw["report_date"].max()
    df_students["status"] = [
        (
            "active"
            if (id == df_raw["kumon_id"].loc[df_raw["report_date"] == last_date]).any()
            else "inactive"
        )
        for id in df_students["kumon_id"]
    ]

    ingested_at = pd.Timestamp.now()
    df_students["ingested_at"] = [ingested_at for _ in range(len(df_students))]
    cols = [
        "student_id",
        "kumon_id",
        "name",
        "birth_date",
        "enroll_date",
        "gender",
        "status",
        "ingested_at",
    ]
    df_students = df_students[cols]

    try:
        worksheet = sh.worksheet("dim_students")
        print("dim_students worksheet opened.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="dim_students", rows="100", cols="20")

    worksheet.clear()

    print(f"Loading {len(df_students)} records in 'dim_students'...")

    set_with_dataframe(
        worksheet=worksheet,
        dataframe=df_students,
        row=1,
        col=1,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    print("Data loaded sucessfully.")
    return df_students


def migrate_rel_students_subject(
    sh: gspread.Spreadsheet, df_raw: pd.DataFrame, df_students: pd.DataFrame
) -> pd.DataFrame:
    df_subjects = df_raw[["kumon_id", "subject", "enroll_date_sub"]]
    df_subjects = df_subjects.merge(
        df_students[["student_id", "kumon_id"]],
        how="left",
        on="kumon_id",
        validate="many_to_one",
    ).drop_duplicates()
    df_subjects["subject_id"] = [str(uuid.uuid4()) for _ in range(len(df_subjects))]
    ingested_at = pd.Timestamp.now()
    df_subjects["ingested_at"] = [ingested_at for _ in range(len(df_subjects))]
    cols = ["subject_id", "student_id", "subject", "enroll_date_sub", "ingested_at"]
    df_subjects = df_subjects[cols]

    try:
        worksheet = sh.worksheet("rel_students_subject")
        print("rel_students_subject worksheet opened.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(
            title="rel_students_subject", rows="100", cols="20"
        )

    worksheet.clear()

    print(f"Loading {len(df_subjects)} records in 'rel_students_subject'...")

    set_with_dataframe(
        worksheet=worksheet,
        dataframe=df_subjects,
        row=1,
        col=1,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    print("Data loaded sucessfully.")
    return df_subjects


def migrate_fct_status_report(
    sh: gspread.Spreadsheet,
    df_raw: pd.DataFrame,
    df_students: pd.DataFrame,
    df_subjects: pd.DataFrame,
):
    df_fact = df_raw[
        [
            "kumon_id",
            "subject",
            "report_date",
            "type",
            "grade_id",
            "grade",
            "stage_id",
            "stage",
            "current_lesson",
            "total_sheets",
            "advanced",
            "status",
        ]
    ]
    df_fact = df_fact.merge(
        df_students[["student_id", "kumon_id"]],
        how="left",
        on="kumon_id",
        validate="many_to_one",
    ).merge(
        df_subjects[["subject_id", "student_id", "subject"]],
        how="left",
        on=["student_id", "subject"],
        validate="many_to_one",
    )
    df_fact["fact_id"] = [str(uuid.uuid4()) for _ in range(len(df_fact))]
    ingested_at = pd.Timestamp.now()
    df_fact["ingested_at"] = [ingested_at for _ in range(len(df_fact))]
    cols = [
        "fact_id",
        "subject_id",
        "student_id",
        "report_date",
        "type",
        "grade_id",
        "grade",
        "stage_id",
        "stage",
        "current_lesson",
        "total_sheets",
        "advanced",
        "status",
        "ingested_at",
    ]
    df_fact = df_fact[cols]

    try:
        worksheet = sh.worksheet("fct_status_report")
        print("fct_status_report worksheet opened.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="fct_status_report", rows="100", cols="20")

    worksheet.clear()

    print(f"Loading {len(df_fact)} records in 'fct_status_report'...")

    set_with_dataframe(
        worksheet=worksheet,
        dataframe=df_fact,
        row=1,
        col=1,
        include_index=False,
        include_column_header=True,
        resize=True,
    )

    print("Data loaded sucessfully.")


if __name__ == "__main__":
    gc = get_gspread_client(GOOGLE_APPLICATION_CREDENTIALS)
    sh = gc.open_by_key(SOURCE_ID)

    try:
        worksheet = sh.worksheet("data_cleaned")
        print(f"data-cleaned worksheet opened.")
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet not found.")

    df_raw = get_as_dataframe(worksheet, evaluate_formulas=True)

    # Makes sure business keys have no hidden white spaces before merges
    df_raw["kumon_id"] = (
        df_raw["kumon_id"].astype(str).apply(lambda s: s.strip().upper())
    )
    df_raw["subject"] = df_raw["subject"].astype(str).apply(lambda s: s.strip().upper())

    df_students = migrate_dim_students(sh, df_raw)
    df_subjects = migrate_rel_students_subject(sh, df_raw, df_students)
    migrate_fct_status_report(sh, df_raw, df_students, df_subjects)
