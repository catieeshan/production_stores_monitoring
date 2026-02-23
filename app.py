# =========================================
# MACHINE SHOP PRODUCTION MONITORING SYSTEM
# =========================================

from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
import io
from flask import send_file

# =========================================
# APP CONFIG
# =========================================

app = Flask(__name__)

DATA_FOLDER = "data"
UPLOAD_FOLDER = "uploads"

PART_MASTER_FILE = os.path.join(DATA_FOLDER, "part_master.csv")

# Ensure folders exist
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Ensure Part Master file exists
if not os.path.exists(PART_MASTER_FILE):
    df = pd.DataFrame(columns=[
        "Part Number",
        "Operation No",
        "Cycle Time (min)",
        "Machine Type",
        "Target Per Hour"
    ])
    df.to_csv(PART_MASTER_FILE, index=False)

OPERATOR_MASTER_FILE = os.path.join(DATA_FOLDER, "operator_master.csv")

# Ensure Operator Master file exists
if not os.path.exists(OPERATOR_MASTER_FILE):
    df = pd.DataFrame(columns=[
        "Operator ID",
        "Operator Name",
        "Skill Level",
        "Is Active"
    ])
    df.to_csv(OPERATOR_MASTER_FILE, index=False)

MACHINE_MASTER_FILE = os.path.join(DATA_FOLDER, "machine_master.csv")

# Ensure Machine Master file exists
if not os.path.exists(MACHINE_MASTER_FILE):
    df = pd.DataFrame(columns=[
        "Machine No",
        "Machine Type",
        "Normal Working Hours",
        "OT Working Hours"
    ])
    df.to_csv(MACHINE_MASTER_FILE, index=False)

# Ensure Operator Absenteeism file exists
ABSENTEEISM_FILE = "data/operator_absenteeism.csv"

if not os.path.exists(ABSENTEEISM_FILE):
    pd.DataFrame(columns=["Date", "Operator", "Status"]).to_csv(
        ABSENTEEISM_FILE, index=False
    )

# =========================================
# 🔐 GOOGLE DRIVE AUTO BACKUP ENGINE
# =========================================
import zipfile
from datetime import datetime

def backup_to_drive():

    print("🔵 Starting Google Drive backup...")

    try:
        import os
        import zipfile
        from datetime import datetime
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        KEY_FILE = "/etc/secrets/gdrive_key.json"
        FOLDER_NAME = "CATI_APP_BACKUP"

        print("🔵 Checking key file path:", KEY_FILE)

        if not os.path.exists(KEY_FILE):
            print("❌ KEY FILE NOT FOUND in Render secrets")
            return

        print("🟢 Key file found")

        # -------- AUTH --------
        SCOPES = ['https://www.googleapis.com/auth/drive']

        creds = service_account.Credentials.from_service_account_file(
            KEY_FILE, scopes=SCOPES
        )

        print("🟢 Google auth created")

        service = build('drive', 'v3', credentials=creds)
        print("🟢 Drive service built")

        # -------- FIND FOLDER --------
        print("🔵 Searching backup folder:", FOLDER_NAME)

        results = service.files().list(
            q=f"name='{FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        items = results.get('files', [])
        print("Folder search result:", items)

        if not items:
            print("❌ BACKUP FOLDER NOT FOUND IN DRIVE")
            return

        folder_id = items[0]['id']
        print("🟢 Folder found:", folder_id)

        # -------- CREATE ZIP --------
        zip_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

        print("🔵 Creating zip:", zip_name)

        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk("data"):
                for file in files:
                    if file.endswith(".csv"):
                        full_path = os.path.join(root, file)
                        zipf.write(full_path, os.path.basename(full_path))

        print("🟢 Zip created")

        # -------- UPLOAD --------
        file_metadata = {
            'name': zip_name,
            'parents': [folder_id]
        }

        media = MediaFileUpload(zip_name, resumable=True)

        print("🔵 Uploading to drive...")

        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()

        print("🟢 BACKUP SUCCESSFULLY UPLOADED")

        # =========================================================
        # AUTO RETENTION POLICY (KEEP LAST 30 BACKUPS)
        # =========================================================
        try:
            print("🔵 Checking old backups for cleanup...")

            # get all zip files inside folder
            results = service.files().list(
                q=f"'{folder_id}' in parents and name contains 'backup_'",
                fields="files(id, name, createdTime)",
                orderBy="createdTime desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            files = results.get("files", [])

            KEEP_LIMIT = 30

            if len(files) > KEEP_LIMIT:
                old_files = files[KEEP_LIMIT:]

                for f in old_files:
                    print("🗑 Deleting old backup:", f["name"])
                    service.files().delete(
                        fileId=f["id"],
                        supportsAllDrives=True
                    ).execute()

                print("🟢 Old backups cleaned")

            else:
                print("🟢 Retention OK — no deletion needed")

        except Exception as e:
            print("Retention cleanup skipped:", str(e))

        os.remove(zip_name)

    except Exception as e:
        print("🔴 GOOGLE DRIVE BACKUP FAILED:", str(e))

# =========================================
# MANAGEMENT DASHBOARD – KPI HELPERS
# =========================================

from datetime import datetime, timedelta
import pandas as pd
import os


def load_csv(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def get_dashboard_kpis():
    import pandas as pd

    # ---------------- LOAD DATA ----------------
    main_df = load_csv("data/production_main.csv")
    other_df = load_csv("data/production_other_machine.csv")
    loss_df = load_csv("data/production_loss.csv")

    prod_df = pd.concat([main_df, other_df], ignore_index=True)

    if prod_df.empty:
        return {}

    # ---------------- NORMALIZE ----------------
    prod_df["Date"] = pd.to_datetime(prod_df["Date"], errors="coerce").dt.date
    prod_df["Time_Min"] = pd.to_numeric(prod_df["Time_Min"], errors="coerce").fillna(0)
    prod_df["Good_Qty"] = pd.to_numeric(prod_df["Good_Qty"], errors="coerce").fillna(0)
    prod_df["Mach_Rej"] = pd.to_numeric(prod_df["Mach_Rej"], errors="coerce").fillna(0)
    prod_df["Op_No"] = (
        prod_df["Operation"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(float)
    )

    if not loss_df.empty:
        loss_df["Date"] = pd.to_datetime(loss_df["Date"], errors="coerce").dt.date
        loss_df["Time_Min"] = pd.to_numeric(loss_df["Time_Min"], errors="coerce").fillna(0)

    # ---------------- LAST WORKING DAYS ----------------
    available_days = sorted(
        d for d in prod_df["Date"].dropna().unique()
        if d.weekday() != 3  # skip Thursday
    )

    if len(available_days) == 0:
        return {}

    # 🔵 For KPI + Top Parts + Loss → last 3 days
    last_3_days = available_days[-3:]

    # 🟣 For Shop Performance → last 10 working days
    last_10_days = available_days[-10:]

    prev_day = last_3_days[-1]

    # ==================================================
    # 🔷 TOP 5 PARTS – LAST OP OUTPUT (LAST 3 WORKING DAYS)
    # ==================================================

    df_ops = prod_df.copy()

    # Extract numeric operation number (OP10 → 10)
    df_ops["Op_No"] = (
        df_ops["Operation"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
    )

    df_ops = df_ops[df_ops["Op_No"].notna()]
    df_ops["Op_No"] = df_ops["Op_No"].astype(int)

    top_parts_daily = {}

    for day in last_3_days:
        day_df = df_ops[df_ops["Date"] == day]

        if day_df.empty:
            top_parts_daily[day.strftime("%d-%m-%Y")] = {}
            continue

        # 🔥 STEP 1: find highest operation per part that day
        final_ops = (
            day_df.groupby("Part")["Op_No"]
            .max()
            .reset_index()
            .rename(columns={"Op_No": "FinalOp"})
        )

        day_df = day_df.merge(final_ops, on="Part", how="left")

        # 🔥 STEP 2: keep ONLY final operation rows
        final_df = day_df[day_df["Op_No"] == day_df["FinalOp"]]

        # 🔥 STEP 3: sum Good Qty of final operation
        part_qty = (
            final_df
            .groupby("Part")["Good_Qty"]
            .sum()
            .to_dict()
        )

        top_parts_daily[day.strftime("%d-%m-%Y")] = part_qty

    # Rank parts across last 3 days
    all_parts = set()
    for d in top_parts_daily.values():
        all_parts.update(d.keys())

    ranking = []
    for part in all_parts:
        if not part or str(part).strip().lower() in ["nan", "none"]:
            continue   # 🔥 skip invalid parts

        total = sum(
            top_parts_daily[day].get(part, 0)
            for day in top_parts_daily
        )
        ranking.append((part, total))

    top_5_parts = sorted(ranking, key=lambda x: x[1], reverse=True)[:5]

    top_parts_table = []
    for part, _ in top_5_parts:
        row = {"Part": part}
        for day in top_parts_daily:
            row[day] = int(top_parts_daily[day].get(part, 0))
        top_parts_table.append(row)

    # ==================================================
    # 🔴 KPI CARDS – PREVIOUS WORKING DAY
    # ==================================================
    prev_df = prod_df[prod_df["Date"] == prev_day]

    total_good_qty = int(prev_df["Good_Qty"].sum())
    total_defects = int(prev_df["Mach_Rej"].sum())

    # ==================================================
    # 🔴 LOSS PIES – LAST 3 DAYS
    # ==================================================
    loss_pies = []

    if not loss_df.empty:
        for d in last_3_days:
            d_loss = loss_df[loss_df["Date"] == d]

            pie_data = (
                d_loss.groupby("Loss_Reason", as_index=False)
                .agg(Time_Min=("Time_Min", "sum"))
            ) if not d_loss.empty else pd.DataFrame()

            loss_pies.append({
                "date": d.strftime("%d-%m-%Y"),
                "data": [
                    {"label": r["Loss_Reason"], "value": int(r["Time_Min"])}
                    for _, r in pie_data.iterrows()
                    if r["Time_Min"] > 0
                ]
            })

    # ==================================================
    # 🟣 SHOP PERFORMANCE – LAST 3 DAYS
    # ==================================================
    performance = []

    for d in last_10_days:
        ddf = prod_df[prod_df["Date"] == d]

        machines = ddf["Machine"].nunique()
        available_time = machines * 960

        loss_time = (
            loss_df[loss_df["Date"] == d]["Time_Min"].sum()
            if not loss_df.empty else 0
        )

        good = ddf["Good_Qty"].sum()
        reject = ddf["Mach_Rej"].sum()
        used_time = ddf["Time_Min"].sum()

        availability = ((available_time - loss_time) / available_time * 100) if available_time else 0
        performance_pct = (used_time / available_time * 100) if available_time else 0
        quality = (good / (good + reject) * 100) if (good + reject) else 0
        oee = (availability * performance_pct * quality) / 10000

        performance.append({
            "date": d.strftime("%d-%m-%Y"),
            "availability": round(max(min(availability, 100), 0), 2),
            "performance": round(max(min(performance_pct, 100), 0), 2),
            "quality": round(max(min(quality, 100), 0), 2),
            "oee": round(max(min(oee, 100), 0), 2)
        })

    # ==================================================
    # ✅ FINAL PAYLOAD
    # ==================================================
    return {
        "last_days": [d.strftime("%d-%m-%Y") for d in last_3_days],
        "production_cards": {
            "date": prev_day.strftime("%d-%m-%Y"),
            "total_good_qty": total_good_qty,
            "total_defects": total_defects
        },
        "top_parts": top_parts_table,
        "loss_pies": loss_pies,
        "performance": performance
    }

# =====================================================
# MASTER PROFESSIONAL EXCEL FORMAT ENGINE (GLOBAL)
# =====================================================

def create_professional_excel(
    writer,
    sheet_name,
    report_title,
    filters_text,
    df,
    start_row=5
):
    """
    Universal corporate Excel formatter for all reports
    """

    workbook  = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    # ================= FORMATS =================
    company_fmt = workbook.add_format({
        "bold": True,
        "font_size": 16,
        "align": "center",
        "font_name": "Calibri"
    })

    system_fmt = workbook.add_format({
        "bold": True,
        "font_size": 12,
        "align": "center",
        "font_name": "Calibri"
    })

    report_fmt = workbook.add_format({
        "bold": True,
        "font_size": 12,
        "align": "center",
        "font_name": "Calibri"
    })

    filter_fmt = workbook.add_format({
        "italic": True,
        "font_size": 10,
        "align": "center",
        "font_name": "Calibri"
    })

    header_fmt = workbook.add_format({
        "bold": True,
        "border": 1,
        "align": "center",
        "valign": "vcenter",
        "bg_color": "#D9E1F2",   # visible corporate header color
        "font_name": "Calibri"
    })

    cell_fmt = workbook.add_format({
        "border": 1,
        "align": "center",
        "font_name": "Calibri"
    })

    # ================= CORPORATE HEADER =================
    last_col = len(df.columns) - 1

    worksheet.merge_range(0, 0, 0, last_col,
        "CATI Manufacturing Pvt. Ltd.", company_fmt)

    worksheet.merge_range(1, 0, 1, last_col,
        "Production Monitoring System", system_fmt)

    worksheet.merge_range(2, 0, 2, last_col,
        report_title, report_fmt)

    worksheet.merge_range(3, 0, 3, last_col,
        f"Applied Filters: {filters_text}", filter_fmt)

    # ================= TABLE HEADER =================
    for col_num, col_name in enumerate(df.columns):
        worksheet.write(start_row, col_num, col_name, header_fmt)

    # ================= TABLE DATA =================
    data_start = start_row + 1

    for r in range(len(df)):
        for c in range(len(df.columns)):
            worksheet.write(data_start + r, c, df.iloc[r, c], cell_fmt)

    # ================= AUTO COLUMN WIDTH =================
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).map(len).max(),
            len(col)
        ) + 4
        worksheet.set_column(i, i, max_len)

    # ================= FREEZE =================
    worksheet.freeze_panes(data_start, 0)

    return worksheet

# =========================================
# HOME
# =========================================

@app.route("/")
def home():
    return render_template("home.html")

# =========================================
# PRODUCTION MODULE HOME
# =========================================
@app.route("/production")
def production_home():
    return render_template("production_home.html")

# =========================================
# PART MASTER (MASTER–DETAIL VIEW)
# =========================================

@app.route("/part_master", methods=["GET", "POST"])
def part_master():

    df = pd.read_csv(PART_MASTER_FILE)

    # ==============================
    # HANDLE POST
    # ==============================
    if request.method == "POST":

        # -------- MANUAL ADD --------
        if "part_number" in request.form:
            part_no = request.form["part_number"].strip()
            op_no = request.form["operation_no"].strip()
            cycle_time = float(request.form["cycle_time"])
            machine_type = request.form["machine_type"].strip()

            target_per_hour = int(60 // cycle_time)

            duplicate = df[
                (df["Part Number"] == part_no) &
                (df["Operation No"] == op_no)
            ]

            if len(duplicate) == 0:
                new_row = {
                    "Part Number": part_no,
                    "Operation No": op_no,
                    "Cycle Time (min)": cycle_time,
                    "Machine Type": machine_type,
                    "Target Per Hour": target_per_hour
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                df.to_csv(PART_MASTER_FILE, index=False)

        # -------- EXCEL UPLOAD --------
        if "excel_file" in request.files:
            file = request.files["excel_file"]
            if file and file.filename != "":

                upload_df = pd.read_excel(file)

                for _, row in upload_df.iterrows():
                    part_no = str(row["Part Number"]).strip()
                    op_no = str(row["Operation No"]).strip()
                    cycle_time = float(row["Cycle Time (min)"])
                    machine_type = str(row["Machine Type"]).strip()

                    target_per_hour = int(60 // cycle_time)

                    duplicate = df[
                        (df["Part Number"] == part_no) &
                        (df["Operation No"] == op_no)
                    ]

                    if len(duplicate) == 0:
                        new_row = {
                            "Part Number": part_no,
                            "Operation No": op_no,
                            "Cycle Time (min)": cycle_time,
                            "Machine Type": machine_type,
                            "Target Per Hour": target_per_hour
                        }
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    
                    else:
                        # UPDATE
                        df.loc[
                            (df["Part Number"] == part_no) &
                            (df["Operation No"] == op_no),
                            ["Cycle Time (min)", "Machine Type", "Target Per Hour"]
                        ] = [cycle_time, machine_type, target_per_hour]

                df.to_csv(PART_MASTER_FILE, index=False)

        backup_to_drive()
        return redirect(url_for("part_master"))

    # ==============================
    # GET DATA FOR UI
    # ==============================

    parts = sorted(df["Part Number"].unique().tolist())

    selected_part = request.args.get("part")
    operations = []

    if selected_part:
        operations = df[df["Part Number"] == selected_part].to_dict(orient="records")
    
    return render_template(
        "part_master.html",
        parts=parts,
        selected_part=selected_part,
        operations=operations
    )

# =========================================
# DELETE OPERATION (SAFE: BY KEY)
# =========================================

@app.route("/delete_part_op")
def delete_part_op():
    part = request.args.get("part")
    op = request.args.get("op")

    df = pd.read_csv(PART_MASTER_FILE)

    df = df[~((df["Part Number"] == part) & (df["Operation No"] == op))]

    df.to_csv(PART_MASTER_FILE, index=False)

    backup_to_drive()
    return redirect(url_for("part_master", part=part))

# =========================================
# EDIT OPERATION (SAVE)
# =========================================

@app.route("/edit_part_op", methods=["POST"])
def edit_part_op():
    part = request.form["part_number"]
    old_op = request.form["old_operation_no"]

    new_op = request.form["operation_no"]
    cycle_time = float(request.form["cycle_time"])
    machine_type = request.form["machine_type"]

    # Recalculate target (ROUNDED DOWN)
    target_per_hour = int(60 // cycle_time)

    df = pd.read_csv(PART_MASTER_FILE)

    # Find and update the row
    mask = (df["Part Number"] == part) & (df["Operation No"] == old_op)

    df.loc[mask, "Operation No"] = new_op
    df.loc[mask, "Cycle Time (min)"] = cycle_time
    df.loc[mask, "Machine Type"] = machine_type
    df.loc[mask, "Target Per Hour"] = target_per_hour

    df.to_csv(PART_MASTER_FILE, index=False)
    
    backup_to_drive()
    return redirect(url_for("part_master", part=part))

# =========================================
# OPERATOR MASTER
# =========================================

@app.route("/operator_master", methods=["GET", "POST"])
def operator_master():

    df = pd.read_csv(OPERATOR_MASTER_FILE)

    # ==============================
    # HANDLE POST
    # ==============================
    if request.method == "POST":

        # -------- MANUAL ADD --------
        if "operator_id" in request.form:
            op_id = request.form["operator_id"].strip()
            name = request.form["operator_name"].strip()
            skill = request.form["skill_level"].strip()
            active = request.form["is_active"].strip()

            duplicate = df[df["Operator ID"] == op_id]

            if len(duplicate) == 0:
                new_row = {
                    "Operator ID": op_id,
                    "Operator Name": name,
                    "Skill Level": skill,
                    "Is Active": active
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                df.to_csv(OPERATOR_MASTER_FILE, index=False)

        # -------- EXCEL UPLOAD --------
        if "excel_file" in request.files:
            file = request.files["excel_file"]
            if file and file.filename != "":

                upload_df = pd.read_excel(file)

                for _, row in upload_df.iterrows():
                    op_id = str(row["Operator ID"]).strip()
                    name = str(row["Operator Name"]).strip()
                    skill = str(row["Skill Level"]).strip()
                    active = str(row["Is Active"]).strip()

                    duplicate = df[df["Operator ID"] == op_id]

                    if len(duplicate) == 0:
                        # INSERT
                        new_row = {
                            "Operator ID": op_id,
                            "Operator Name": name,
                            "Skill Level": skill,
                            "Is Active": active
                        }
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    else:
                        # UPDATE
                        df.loc[
                            df["Operator ID"] == op_id,
                            ["Operator Name", "Skill Level", "Is Active"]
                        ] = [name, skill, active]

                df.to_csv(OPERATOR_MASTER_FILE, index=False)

        backup_to_drive()
        return redirect(url_for("operator_master"))

    # ==============================
    # GET DATA FOR UI
    # ==============================

    records = df.to_dict(orient="records")

    search = request.args.get("search", "").lower()
    if search:
        records = [r for r in records if search in str(r["Operator ID"]).lower() or search in str(r["Operator Name"]).lower()]

    edit_id = request.args.get("edit_id")

    return render_template(
        "operator_master.html",
        records=records,
        edit_id=edit_id,
        search=search
    )

# =========================================
# DELETE OPERATOR
# =========================================

@app.route("/delete_operator")
def delete_operator():
    op_id = request.args.get("id")

    df = pd.read_csv(OPERATOR_MASTER_FILE)
    df = df[df["Operator ID"] != op_id]
    df.to_csv(OPERATOR_MASTER_FILE, index=False)

    backup_to_drive()
    return redirect(url_for("operator_master"))

# =========================================
# EDIT OPERATOR (SAVE)
# =========================================

@app.route("/edit_operator", methods=["POST"])
def edit_operator():
    old_id = request.form["old_operator_id"]

    new_id = request.form["operator_id"]
    name = request.form["operator_name"]
    skill = request.form["skill_level"]
    active = request.form["is_active"]

    df = pd.read_csv(OPERATOR_MASTER_FILE)

    mask = df["Operator ID"] == old_id

    df.loc[mask, "Operator ID"] = new_id
    df.loc[mask, "Operator Name"] = name
    df.loc[mask, "Skill Level"] = skill
    df.loc[mask, "Is Active"] = active

    df.to_csv(OPERATOR_MASTER_FILE, index=False)

    backup_to_drive()
    return redirect(url_for("operator_master"))

# =========================================
# MACHINE MASTER
# =========================================

@app.route("/machine_master", methods=["GET", "POST"])
def machine_master():

    df = pd.read_csv(MACHINE_MASTER_FILE)

    # ==============================
    # HANDLE POST
    # ==============================
    if request.method == "POST":

        # -------- MANUAL ADD --------
        if "machine_no" in request.form:
            machine_no = request.form["machine_no"].strip()
            machine_type = request.form["machine_type"].strip()
            normal_hours = float(request.form["normal_hours"])
            ot_hours = float(request.form["ot_hours"])

            duplicate = df[df["Machine No"] == machine_no]

            if len(duplicate) == 0:
                new_row = {
                    "Machine No": machine_no,
                    "Machine Type": machine_type,
                    "Normal Working Hours": normal_hours,
                    "OT Working Hours": ot_hours
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                df.to_csv(MACHINE_MASTER_FILE, index=False)

        # -------- EXCEL UPLOAD --------
        if "excel_file" in request.files:
            file = request.files["excel_file"]
            if file and file.filename != "":
                
                upload_df = pd.read_excel(file)

                for _, row in upload_df.iterrows():
                    machine_no = str(row["Machine No"]).strip()
                    machine_type = str(row["Machine Type"]).strip()
                    normal_hours = float(row["Normal Working Hours"])
                    ot_hours = float(row["OT Working Hours"])

                    duplicate = df[df["Machine No"] == machine_no]

                    if len(duplicate) == 0:
                        # INSERT
                        new_row = {
                            "Machine No": machine_no,
                            "Machine Type": machine_type,
                            "Normal Working Hours": normal_hours,
                            "OT Working Hours": ot_hours
                        }
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    else:
                        # UPDATE
                        df.loc[
                            df["Machine No"] == machine_no,
                            ["Machine Type", "Normal Working Hours", "OT Working Hours"]
                        ] = [machine_type, normal_hours, ot_hours]

                df.to_csv(MACHINE_MASTER_FILE, index=False)

        backup_to_drive()
        return redirect(url_for("machine_master"))

    # ==============================
    # GET DATA FOR UI
    # ==============================

    records = df.to_dict(orient="records")

    search = request.args.get("search", "").lower()
    if search:
        records = [
            r for r in records
            if search in str(r["Machine No"]).lower()
            or search in str(r["Machine Type"]).lower()
        ]

    edit_id = request.args.get("edit_id")
    
    return render_template(
        "machine_master.html",
        records=records,
        edit_id=edit_id,
        search=search
    )

# =========================================
# DELETE MACHINE
# =========================================

@app.route("/delete_machine")
def delete_machine():
    machine_no = request.args.get("no")

    df = pd.read_csv(MACHINE_MASTER_FILE)
    df = df[df["Machine No"] != machine_no]
    df.to_csv(MACHINE_MASTER_FILE, index=False)

    backup_to_drive()
    return redirect(url_for("machine_master"))

# =========================================
# EDIT MACHINE (SAVE)
# =========================================

@app.route("/edit_machine", methods=["POST"])
def edit_machine():
    old_no = request.form["old_machine_no"]

    new_no = request.form["machine_no"]
    machine_type = request.form["machine_type"]
    normal_hours = float(request.form["normal_hours"])
    ot_hours = float(request.form["ot_hours"])

    df = pd.read_csv(MACHINE_MASTER_FILE)

    mask = df["Machine No"] == old_no

    df.loc[mask, "Machine No"] = new_no
    df.loc[mask, "Machine Type"] = machine_type
    df.loc[mask, "Normal Working Hours"] = normal_hours
    df.loc[mask, "OT Working Hours"] = ot_hours

    df.to_csv(MACHINE_MASTER_FILE, index=False)

    backup_to_drive()
    return redirect(url_for("machine_master"))

# =========================================
# PRODUCTION ENTRY (PHASE 1)
# =========================================

# DAILY PRODUCTION REPORT ENTRY

@app.route("/production_entry")
def production_entry():

    # Load masters
    parts_df = pd.read_csv(PART_MASTER_FILE)
    operators_df = pd.read_csv(OPERATOR_MASTER_FILE)
    machines_df = pd.read_csv(MACHINE_MASTER_FILE)

    # Build Part -> Operations -> Machine Type mapping
    part_ops = {}
    for _, row in parts_df.iterrows():
        part = row["Part Number"]
        op = row["Operation No"]
        mtype = row["Machine Type"]

        if part not in part_ops:
            part_ops[part] = []

        part_ops[part].append({
            "op": op,
            "machine_type": mtype
        })

    # Build Machine No -> Machine Type mapping
    machine_types = {}
    for _, row in machines_df.iterrows():
        machine_types[row["Machine No"]] = row["Machine Type"]

    return render_template(
        "production_entry.html",
        parts=sorted(part_ops.keys()),
        part_ops=part_ops,
        machine_types=machine_types,
        operators=operators_df.to_dict(orient="records"),
        machines=machines_df.to_dict(orient="records"),
    )

# PRODUCTION REPORT SAVE

@app.route("/save_production_entry", methods=["POST"])
def save_production_entry():
    import json
    import pandas as pd
    import os
    from datetime import datetime

    # -----------------------------
    # BASIC HEADER DATA
    # -----------------------------
    date = request.form.get("date") or datetime.today().strftime("%Y-%m-%d")
    operator = request.form.get("operator")
    shift = request.form.get("shift")
    ot = request.form.get("ot")
    main_machine = request.form.get("main_machine")

    # -----------------------------
    # JSON TABLE DATA
    # -----------------------------
    main_data = json.loads(request.form.get("main_data", "[]"))
    other_data = json.loads(request.form.get("other_data", "[]"))
    loss_data = json.loads(request.form.get("loss_data", "[]"))

    os.makedirs("data", exist_ok=True)

    # =====================================================
    # MAIN MACHINE PRODUCTION
    # =====================================================
    if main_data:
        df_main = pd.DataFrame([
            {
                "Date": date,
                "Operator": operator,
                "Shift": shift,
                "OT": ot,
                "Machine": main_machine,
                "Part": r.get("part"),
                "Operation": r.get("operation"),
                "Time_Min": r.get("time"),
                "Qty": r.get("qty"),
                "Cast_Rej": r.get("cast"),
                "Mach_Rej": r.get("mach"),
                "Good_Qty": r.get("good")
            }
            for r in main_data
        ])

        path = "data/production_main.csv"
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0
        df_main.to_csv(path, mode="a", index=False, header=write_header)

    # =====================================================
    # OTHER MACHINE PRODUCTION
    # =====================================================
    if other_data:
        df_other = pd.DataFrame([
            {
                "Date": date,
                "Operator": operator,
                "Shift": shift,
                "OT": ot,
                "Machine": r.get("machine"),
                "Part": r.get("part"),
                "Operation": r.get("operation"),
                "Time_Min": r.get("time"),
                "Qty": r.get("qty"),
                "Cast_Rej": r.get("cast"),
                "Mach_Rej": r.get("mach"),
                "Good_Qty": r.get("good")
            }
            for r in other_data
        ])

        path = "data/production_other_machine.csv"
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0
        df_other.to_csv(path, mode="a", index=False, header=write_header)

    # =====================================================
    # LOSS / DOWNTIME
    # =====================================================
    if loss_data:
        df_loss = pd.DataFrame([
            {
                "Date": date,
                "Operator": operator,
                "Shift": shift,
                "OT": ot,
                "Machine": main_machine,
                "Loss_Reason": r.get("reason"),
                "Time_Min": r.get("time")
            }
            for r in loss_data
        ])

        path = "data/production_loss.csv"
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0
        df_loss.to_csv(path, mode="a", index=False, header=write_header)

    # -----------------------------
    # BACK TO ENTRY PAGE
    # -----------------------------
    backup_to_drive()
    return redirect(url_for("production_entry"))

# OPERATOR ABSENTEEISM ENTRY

@app.route("/operator_absenteeism", methods=["GET", "POST"])
def operator_absenteeism():
    import pandas as pd
    import calendar
    from datetime import datetime
    import os

    ABSENT_FILE = "data/operator_absenteeism.csv"
    OPERATOR_FILE = "data/operator_master.csv"

    # ---------- LOAD OPERATORS ----------
    operators_df = pd.read_csv(OPERATOR_FILE)
    operators = sorted(operators_df["Operator Name"].dropna().unique().tolist())

    # ---------- ENSURE ABSENT FILE ----------
    if not os.path.exists(ABSENT_FILE):
        pd.DataFrame(columns=["Date", "Operator"]).to_csv(ABSENT_FILE, index=False)

    absent_df = pd.read_csv(ABSENT_FILE)

    # ---------- ABSENTEEISM SUMMARY (ALL TIME) ----------
    if absent_df.empty:
        absent_summary = []
    else:
        absent_summary = (
            absent_df.groupby("Operator", as_index=False)
            .agg(Absence_Count=("Date", "count"))
            .sort_values("Operator")
            .to_dict(orient="records")
        )

    # ---------- HANDLE POST (MARK ABSENT) ----------
    if request.method == "POST":
        operator = request.form["operator"]
        date = request.form["date"]

        duplicate = absent_df[
            (absent_df["Operator"] == operator) &
            (absent_df["Date"] == date)
        ]

        if duplicate.empty:
            absent_df.loc[len(absent_df)] = {
                "Operator": operator,
                "Date": date
            }
            absent_df.to_csv(ABSENT_FILE, index=False)

        backup_to_drive()

        return redirect(url_for("operator_absenteeism"))

    # ---------- FILTERS ----------
    filter_operator = request.args.get("filter_operator")
    filter_month = request.args.get("filter_month")

    calendar_days = []
    first_weekday = 0

    if filter_operator and filter_month:
        year = datetime.now().year
        month = int(filter_month)

        first_weekday, num_days = calendar.monthrange(year, month)

        absent_dates = set(
            absent_df[
                absent_df["Operator"] == filter_operator
            ]["Date"].tolist()
        )

        for day in range(1, num_days + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            weekday = datetime(year, month, day).weekday()

            if weekday == 3:  # Thursday
                status = "off"
            elif date_str in absent_dates:
                status = "absent"
            else:
                status = "present"

            calendar_days.append({
                "day": day,
                "date": date_str,   # ✅ FIX
                "type": status
            })

    # ---------- MONTHS ----------
    months = [
        {"value": "01", "label": "January"},
        {"value": "02", "label": "February"},
        {"value": "03", "label": "March"},
        {"value": "04", "label": "April"},
        {"value": "05", "label": "May"},
        {"value": "06", "label": "June"},
        {"value": "07", "label": "July"},
        {"value": "08", "label": "August"},
        {"value": "09", "label": "September"},
        {"value": "10", "label": "October"},
        {"value": "11", "label": "November"},
        {"value": "12", "label": "December"},
    ]

    return render_template(
        "operator_absenteeism.html",
        operators=operators,
        months=months,
        filter_operator=filter_operator,
        filter_month=filter_month,
        calendar_days=calendar_days,
        first_weekday=first_weekday,
        absent_summary=absent_summary   # ✅ NEW
    )

DELETE_ABSENCE_CODE = "cati123"

@app.route("/transactions/absenteeism/delete", methods=["POST"])
def delete_absence():
    import pandas as pd
    import os

    code = request.form.get("code", "")
    if code != DELETE_ABSENCE_CODE:
        return "INVALID_CODE", 403

    date = request.form.get("date")
    operator = request.form.get("operator")

    path = "data/operator_absenteeism.csv"

    if not os.path.exists(path):
        return "FILE_NOT_FOUND", 404

    df = pd.read_csv(path)

    before = len(df)

    df = df[~(
        (df["Date"] == date) &
        (df["Operator"] == operator)
    )]

    after = len(df)

    if before == after:
        return "NOT_FOUND", 404

    df.to_csv(path, index=False)
    backup_to_drive()
    return "OK", 200

# =========================================
# REPORTS PAGE
# =========================================

# DAILY PRODUCTION REPORT

@app.route("/reports/daily", methods=["GET"])
def reports_daily():
    import pandas as pd
    import os

    selected_month = request.args.get("month", "").strip()
    selected_date = request.args.get("date", "").strip()
    operator_filter = request.args.get("operator", "").strip()
    part_filter = request.args.get("part", "").strip()
    operation_filter = request.args.get("operation", "").strip()

    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    # ---------------- LOAD DATA ----------------
    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")

    main_df["_source"] = "main"
    other_df["_source"] = "other"

    df = pd.concat([main_df, other_df], ignore_index=True)

    if df.empty:
        return render_template(
            "reports_daily.html",
            active_report="daily",
            records=[],
            operators=[],
            parts=[],
            operations=[],
            months=[],
            selected_month=selected_month,
            selected_date=selected_date,
            operator_filter=operator_filter,
            part_filter=part_filter,
            operation_filter=operation_filter
        )

    # ---------------- NORMALIZE TYPES ----------------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Cast_Rej"] = pd.to_numeric(df["Cast_Rej"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Good_Qty"] = pd.to_numeric(df["Good_Qty"], errors="coerce").fillna(0)

    # ---------------- AVAILABLE TIME (OPERATOR-DAY) ----------------
    df["Operator_Available_Time"] = df["OT"].apply(
        lambda x: 570 if str(x).strip().lower() == "yes" else 480
    )

    # ---------------- TOTAL OPERATOR TIME (PER DAY) ----------------
    df["Operator_Total_Time"] = (
        df.groupby(["Date", "Operator"])["Time_Min"]
        .transform("sum")
    )

    # ---------------- PROPORTIONAL ALLOCATION ----------------
    df["Available_Time"] = (
        df["Time_Min"] / df["Operator_Total_Time"]
    ) * df["Operator_Available_Time"]

    df["Available_Time"] = df["Available_Time"].round(2)

    # Explicit, visible column for table
    df["Time_Spent"] = df["Time_Min"].round(2)

    # ---------------- APPLY FILTERS ----------------
    filtered_df = df.copy()

    if selected_month:
        filtered_df = filtered_df[
            filtered_df["Date"].dt.month == int(selected_month)
        ]

    if selected_date:
        filtered_df = filtered_df[
            filtered_df["Date"] == pd.to_datetime(selected_date)
        ]

    if operator_filter:
        filtered_df = filtered_df[filtered_df["Operator"] == operator_filter]

    if part_filter:
        filtered_df = filtered_df[filtered_df["Part"] == part_filter]

    if operation_filter:
        filtered_df = filtered_df[filtered_df["Operation"] == operation_filter]

    # ---------------- SORTING ----------------
    filtered_df = filtered_df.sort_values(
        by=["Date", "Operator"],
        ascending=[False, True]
    )

    # ---------------- FORMAT DATE ----------------
    # Logic date (used for delete, filters)
    filtered_df["Date"] = filtered_df["Date"].dt.strftime("%Y-%m-%d")

    # Display date (used only in UI)
    filtered_df["Date_Display"] = pd.to_datetime(
        filtered_df["Date"],
        errors="coerce"
    ).dt.strftime("%d-%m-%Y")

    # ---------------- FILTER DROPDOWNS ----------------
    operators = sorted(df["Operator"].dropna().unique().tolist())
    parts = sorted(df["Part"].dropna().unique().tolist())
    operations = sorted(df["Operation"].dropna().unique().tolist())

    months = [
        {"value": "01", "label": "January"},
        {"value": "02", "label": "February"},
        {"value": "03", "label": "March"},
        {"value": "04", "label": "April"},
        {"value": "05", "label": "May"},
        {"value": "06", "label": "June"},
        {"value": "07", "label": "July"},
        {"value": "08", "label": "August"},
        {"value": "09", "label": "September"},
        {"value": "10", "label": "October"},
        {"value": "11", "label": "November"},
        {"value": "12", "label": "December"},
    ]

    return render_template(
        "reports_daily.html",
        active_report="daily",
        records=filtered_df.to_dict(orient="records"),
        operators=operators,
        parts=parts,
        operations=operations,
        months=months,
        selected_month=selected_month,
        selected_date=selected_date,
        operator_filter=operator_filter,
        part_filter=part_filter,
        operation_filter=operation_filter
    )

DELETE_VERIFICATION_CODE = "cati123"

@app.route("/reports/daily/delete", methods=["POST"])
def delete_daily_entry():
    import pandas as pd
    import os
    
    # ---------- VERIFY CODE ----------
    code = request.form.get("code", "")
    if code != DELETE_VERIFICATION_CODE:
        return "INVALID_CODE", 403

    # ---------- READ REQUEST DATA ----------
    req_date = request.form.get("Date")
    req_operator = request.form.get("Operator")
    req_shift = request.form.get("Shift")
    req_machine = request.form.get("Machine")
    req_part = request.form.get("Part")
    req_operation = request.form.get("Operation")
    req_time = float(request.form.get("Time_Min", 0))
    source = request.form.get("source")  # main / other

    # ---------- DETERMINE SOURCE FILE ----------
    prod_path = (
        "data/production_main.csv"
        if source == "main"
        else "data/production_other_machine.csv"
    )

    if not os.path.exists(prod_path):
        return "FILE_NOT_FOUND", 404

    prod_df = pd.read_csv(prod_path)

    # ---------- NORMALIZE ----------
    prod_df["Date"] = pd.to_datetime(
        prod_df["Date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    prod_df["Time_Min"] = pd.to_numeric(
        prod_df["Time_Min"], errors="coerce"
    )

    # ---------- MATCH PRODUCTION ENTRY ----------
    mask = (
        (prod_df["Date"] == req_date) &
        (prod_df["Operator"] == req_operator) &
        (prod_df["Shift"] == req_shift) &
        (prod_df["Machine"] == req_machine) &
        (prod_df["Part"] == req_part) &
        (prod_df["Operation"] == req_operation) &
        (prod_df["Time_Min"].sub(req_time).abs() < 0.0001)
    )

    before = len(prod_df)
    prod_df = prod_df[~mask]
    after = len(prod_df)

    if before == after:
        return "NOT_FOUND", 404

    # ---------- SAVE PRODUCTION FILE ----------
    prod_df.to_csv(prod_path, index=False)

    # =====================================================
    # 🔥 ALSO DELETE RELATED LOSS ENTRIES (CRITICAL FIX)
    # =====================================================
    LOSS_FILE = "data/production_loss.csv"

    if os.path.exists(LOSS_FILE) and os.path.getsize(LOSS_FILE) > 0:
        loss_df = pd.read_csv(LOSS_FILE)

        loss_df["Date"] = pd.to_datetime(
            loss_df["Date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        before_loss = len(loss_df)

        loss_df = loss_df[~(
            (loss_df["Date"] == req_date) &
            (loss_df["Operator"] == req_operator) &
            (loss_df["Shift"] == req_shift) &
            (loss_df["Machine"] == req_machine)
        )]

        after_loss = len(loss_df)

        if before_loss != after_loss:
            loss_df.to_csv(LOSS_FILE, index=False)

    return "OK", 200

# =====================================================
# EXPORT DAILY PRODUCTION REPORT TO EXCEL (PROFESSIONAL)
# =====================================================

@app.route("/reports/daily/export", methods=["GET"])
def export_daily_excel():

    import pandas as pd
    import os
    import io
    from flask import send_file, request

    # ================= FILTERS =================
    selected_month = request.args.get("month", "").strip()
    selected_date = request.args.get("date", "").strip()
    operator_filter = request.args.get("operator", "").strip()
    part_filter = request.args.get("part", "").strip()
    operation_filter = request.args.get("operation", "").strip()

    # ================= LOAD FILES =================
    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")

    main_df["_source"] = "main"
    other_df["_source"] = "other"

    df = pd.concat([main_df, other_df], ignore_index=True)

    if df.empty:
        return "No data available"

    # ================= NORMALIZE =================
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Cast_Rej"] = pd.to_numeric(df["Cast_Rej"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Good_Qty"] = pd.to_numeric(df["Good_Qty"], errors="coerce").fillna(0)

    # ================= AVAILABLE TIME =================
    df["Operator_Available_Time"] = df["OT"].apply(
        lambda x: 570 if str(x).strip().lower() == "yes" else 480
    )

    df["Operator_Total_Time"] = (
        df.groupby(["Date", "Operator"])["Time_Min"].transform("sum")
    )

    df["Available_Time"] = (
        df["Time_Min"] / df["Operator_Total_Time"]
    ) * df["Operator_Available_Time"]

    df["Available_Time"] = df["Available_Time"].round(2)
    df["Time_Spent"] = df["Time_Min"].round(2)

    # ================= APPLY FILTERS =================
    filtered_df = df.copy()

    if selected_month:
        filtered_df = filtered_df[
            filtered_df["Date"].dt.month == int(selected_month)
        ]

    if selected_date:
        filtered_df = filtered_df[
            filtered_df["Date"] == pd.to_datetime(selected_date)
        ]

    if operator_filter:
        filtered_df = filtered_df[filtered_df["Operator"] == operator_filter]

    if part_filter:
        filtered_df = filtered_df[filtered_df["Part"] == part_filter]

    if operation_filter:
        filtered_df = filtered_df[filtered_df["Operation"] == operation_filter]

    if filtered_df.empty:
        return "No data after filters"

    # ================= FORMAT DATE =================
    filtered_df["Date"] = filtered_df["Date"].dt.strftime("%d-%m-%Y")

    export_df = filtered_df[[
        "Date","Operator","Shift","Machine",
        "Part","Operation","Time_Spent",
        "Available_Time","Qty","Cast_Rej",
        "Mach_Rej","Good_Qty"
    ]].copy()

    export_df.columns = [
        "Date","Operator","Shift","Machine",
        "Part","Operation","Time Spent (min)",
        "Available Time (min)","Produced Qty",
        "Cast Rej","Mach Rej","Good Qty"
    ]

    # ================= FILTER TEXT =================
    filters = []

    if operator_filter: filters.append(f"Operator={operator_filter}")
    if selected_month: filters.append(f"Month={selected_month}")
    if selected_date: filters.append(f"Date={selected_date}")
    if part_filter: filters.append(f"Part={part_filter}")
    if operation_filter: filters.append(f"Op={operation_filter}")

    filter_text = " | ".join(filters) if filters else "None"

    # ================= FILE NAME =================
    fname = "Daily_Production"

    if operator_filter:
        fname += f"_{operator_filter}"

    if selected_month:
        fname += f"_M{selected_month}"

    if selected_date:
        fname += f"_{selected_date}"

    fname += ".xlsx"

    # ================= CREATE EXCEL IN MEMORY =================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # MAIN REPORT SHEET
        create_professional_excel(
            writer=writer,
            sheet_name="Daily Report",
            report_title="Daily Production Report",
            filters_text=filter_text,
            df=export_df
        )

        # RAW DATA SHEET
        raw_df = filtered_df.copy()
        raw_df.to_excel(writer, sheet_name="Raw Data", index=False)

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# OPERATOR PERFORMANCE REPORT

@app.route("/reports/operator", methods=["GET"])
def reports_operator():
    import pandas as pd
    import os

    # ---------- LOAD FILES ----------
    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")
    part_df = load("data/part_master.csv")
    absent_df = load("data/operator_absenteeism.csv")

    # ---------- MASTER DATE RANGE (IMPORTANT) ----------
    all_prod_df = pd.concat([main_df, other_df], ignore_index=True)

    if not all_prod_df.empty:
        all_prod_df["Date"] = pd.to_datetime(all_prod_df["Date"], errors="coerce")
        global_start = all_prod_df["Date"].min()
        global_end = all_prod_df["Date"].max()
    else:
        global_start = None
        global_end = None

    operator_filter = request.args.get("operator", "").strip()
    month_filter = request.args.get("month", "").strip()

    prod_df = pd.concat([main_df, other_df], ignore_index=True)

    # ---------- NORMALIZE DATE ----------
    prod_df["Date"] = pd.to_datetime(prod_df["Date"], errors="coerce")

    if not absent_df.empty:
        absent_df["Date"] = pd.to_datetime(absent_df["Date"], errors="coerce")

    # ---------- MONTH FILTER ----------
    if month_filter:
        prod_df = prod_df[prod_df["Date"].dt.month == int(month_filter)]
        if not absent_df.empty:
            absent_df = absent_df[absent_df["Date"].dt.month == int(month_filter)]

    # ---------- OPERATOR FILTER ----------
    if operator_filter:
        prod_df = prod_df[prod_df["Operator"] == operator_filter]

    # 🔴 HARD EXIT: NO DATA FOR MONTH
    if prod_df.empty:
        return render_template(
            "reports_operator.html",
            active_report="operator",
            records=[],
            operators=sorted(
                pd.concat([main_df, other_df])["Operator"]
                .dropna().unique().tolist()
            ),
            operator_filter=operator_filter,
            selected_month=month_filter,
            months=[
                {"value": f"{i:02d}", "label": m}
                for i, m in enumerate(
                    ["January","February","March","April","May","June",
                     "July","August","September","October","November","December"], 1
                )
            ],
            no_data_msg="No operator performance data available for the selected month."
        )

    # ---------- MERGE CYCLE TIME ----------
    df = prod_df.merge(
        part_df,
        left_on=["Part", "Operation"],
        right_on=["Part Number", "Operation No"],
        how="left"
    )

    # ---------- SAFE NUMERIC ----------
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Cycle Time (min)"] = pd.to_numeric(
        df["Cycle Time (min)"], errors="coerce"
    ).fillna(0)

    df = df[(df["Time_Min"] > 0) & (df["Cycle Time (min)"] > 0)]

    # 🔴 HARD EXIT: NOTHING VALID
    if df.empty:
        return render_template(
            "reports_operator.html",
            active_report="operator",
            records=[],
            operators=sorted(prod_df["Operator"].dropna().unique().tolist()),
            operator_filter=operator_filter,
            selected_month=month_filter,
            months=[
                {"value": f"{i:02d}", "label": m}
                for i, m in enumerate(
                    ["January","February","March","April","May","June",
                     "July","August","September","October","November","December"], 1
                )
            ],
            no_data_msg="No operator performance data available for the selected month."
        )

    # ---------- EXPECTED QTY ----------
    df["Expected_Qty"] = df["Time_Min"] / df["Cycle Time (min)"]

    # ---------- DAILY PRODUCTION ----------
    daily_prod = (
        df.groupby(["Operator", "Date"], as_index=False)
        .agg(
            Actual_Qty=("Qty", "sum"),
            Expected_Qty=("Expected_Qty", "sum"),
            Mach_Rej=("Mach_Rej", "sum"),
            Time_Min=("Time_Min", "sum")
        )
    )

    daily_prod["Daily_Productivity"] = (
        (daily_prod["Actual_Qty"] / daily_prod["Expected_Qty"]) * 100
    ).replace([float("inf"), -float("inf")], 0)

    # ---------- DATE GRID (FINAL CORRECT LOGIC) ----------
    operators = daily_prod["Operator"].unique()

    if month_filter:
        # full selected month
        year = global_start.year if global_start is not None else datetime.today().year
        month = int(month_filter)

        start_date = pd.Timestamp(year=year, month=month, day=1)
        end_date = start_date + pd.offsets.MonthEnd(1)

    else:
        # full available production range (ALL operators)
        start_date = global_start
        end_date = global_end

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")

    grid = pd.MultiIndex.from_product(
        [operators, all_dates],
        names=["Operator", "Date"]
    ).to_frame(index=False)

    grid = grid.merge(
        daily_prod[["Operator", "Date", "Daily_Productivity"]],
        on=["Operator", "Date"],
        how="left"
    )

    # ---------- APPLY ABSENT LOGIC (CORRECT) ----------
    grid["Absent"] = 0

    if not absent_df.empty:
        absent_df["Absent"] = 1
        grid = grid.merge(
            absent_df[["Operator", "Date", "Absent"]],
            on=["Operator", "Date"],
            how="left",
            suffixes=("", "_y")
        )
        grid["Absent"] = grid["Absent_y"].fillna(grid["Absent"])
        grid.drop(columns=["Absent_y"], inplace=True)

    def final_productivity(row):
        if row["Absent"] == 1:
            return 0          # ✅ Absent day
        if pd.isna(row["Daily_Productivity"]):
            return None       # ✅ Present but no production → ignore
        return row["Daily_Productivity"]

    grid["Final_Productivity"] = grid.apply(final_productivity, axis=1)

    productivity = (
        grid.dropna(subset=["Final_Productivity"])
        .groupby("Operator", as_index=False)
        .agg({
            "Final_Productivity": "mean"
        })
    )

    productivity.rename(
        columns={"Final_Productivity": "Productivity_%"},
        inplace=True
    )

    # ---------- QUALITY ----------
    quality = (
        df.groupby("Operator", as_index=False)
        .agg(
            Actual_Qty=("Qty", "sum"),
            Mach_Rej=("Mach_Rej", "sum"),
            Total_Time_Min=("Time_Min", "sum"),
            Expected_Qty=("Expected_Qty", "sum")
        )
    )

    quality["Quality_%"] = (
        (1 - (quality["Mach_Rej"] / quality["Actual_Qty"])) * 100
    ).replace([float("inf"), -float("inf")], 0).fillna(0)

    # ---------- FINAL MERGE ----------
    summary = quality.merge(productivity, on="Operator", how="left")

    # ---------- FINAL FORMATTING ----------
    summary["Expected_Qty"] = summary["Expected_Qty"].round(0).astype(int)
    summary["Total_Time_Min"] = summary["Total_Time_Min"].round(2)
    summary["Productivity_%"] = summary["Productivity_%"].round(2)
    summary["Quality_%"] = summary["Quality_%"].round(2)

    summary["Actual_Qty"] = summary["Actual_Qty"].astype(int)
    summary["Mach_Rej"] = summary["Mach_Rej"].astype(int)

    return render_template(
        "reports_operator.html",
        active_report="operator",
        records=summary.to_dict(orient="records"),
        operators=sorted(prod_df["Operator"].dropna().unique().tolist()),
        operator_filter=operator_filter,
        selected_month=month_filter,
        months=[
            {"value": f"{i:02d}", "label": m}
            for i, m in enumerate(
                ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"], 1
            )
        ]
    )

# =====================================================
# EXPORT OPERATOR PERFORMANCE REPORT (PROFESSIONAL MASTER)
# =====================================================

@app.route("/export/operator", methods=["GET"])
def export_operator_report():

    import pandas as pd
    import io
    import os
    from flask import send_file, request

    # ================= FILTERS =================
    operator_filter = request.args.get("operator", "").strip()
    month_filter = request.args.get("month", "").strip()

    # ================= LOAD FILES =================
    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")
    part_df = load("data/part_master.csv")
    absent_df = load("data/operator_absenteeism.csv")

    prod_df = pd.concat([main_df, other_df], ignore_index=True)

    if prod_df.empty:
        return "No data to export"

    prod_df["Date"] = pd.to_datetime(prod_df["Date"], errors="coerce")

    if not absent_df.empty:
        absent_df["Date"] = pd.to_datetime(absent_df["Date"], errors="coerce")

    # ================= FILTER APPLY =================
    if month_filter:
        prod_df = prod_df[prod_df["Date"].dt.month == int(month_filter)]
        if not absent_df.empty:
            absent_df = absent_df[absent_df["Date"].dt.month == int(month_filter)]

    if operator_filter:
        prod_df = prod_df[prod_df["Operator"] == operator_filter]

    if prod_df.empty:
        return "No data after filters"

    # ================= MERGE CYCLE TIME =================
    df = prod_df.merge(
        part_df,
        left_on=["Part", "Operation"],
        right_on=["Part Number", "Operation No"],
        how="left"
    )

    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Cycle Time (min)"] = pd.to_numeric(df["Cycle Time (min)"], errors="coerce").fillna(0)

    df = df[(df["Time_Min"] > 0) & (df["Cycle Time (min)"] > 0)]
    df["Expected_Qty"] = df["Time_Min"] / df["Cycle Time (min)"]

    # ================= DAILY PROD =================
    daily_prod = (
        df.groupby(["Operator", "Date"], as_index=False)
        .agg(
            Actual_Qty=("Qty", "sum"),
            Expected_Qty=("Expected_Qty", "sum"),
            Mach_Rej=("Mach_Rej", "sum"),
            Time_Min=("Time_Min", "sum")
        )
    )

    daily_prod["Daily_Productivity"] = (
        (daily_prod["Actual_Qty"] / daily_prod["Expected_Qty"]) * 100
    ).replace([float("inf"), -float("inf")], 0)

    operators = daily_prod["Operator"].unique()
    start_date = daily_prod["Date"].min()
    end_date = daily_prod["Date"].max()
    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")

    grid = pd.MultiIndex.from_product(
        [operators, all_dates],
        names=["Operator", "Date"]
    ).to_frame(index=False)

    grid = grid.merge(
        daily_prod[["Operator", "Date", "Daily_Productivity"]],
        on=["Operator", "Date"],
        how="left"
    )

    grid["Absent"] = 0

    if not absent_df.empty:
        absent_df["Absent"] = 1
        grid = grid.merge(
            absent_df[["Operator", "Date", "Absent"]],
            on=["Operator", "Date"],
            how="left",
            suffixes=("", "_y")
        )
        grid["Absent"] = grid["Absent_y"].fillna(grid["Absent"])
        grid.drop(columns=["Absent_y"], inplace=True)

    def final_productivity(row):
        if row["Absent"] == 1:
            return 0
        if pd.isna(row["Daily_Productivity"]):
            return None
        return row["Daily_Productivity"]

    grid["Final_Productivity"] = grid.apply(final_productivity, axis=1)

    productivity = (
        grid.dropna(subset=["Final_Productivity"])
        .groupby("Operator", as_index=False)
        .agg({"Final_Productivity": "mean"})
    )

    productivity.rename(columns={"Final_Productivity": "Productivity (%)"}, inplace=True)

    # ================= QUALITY =================
    quality = (
        df.groupby("Operator", as_index=False)
        .agg(
            Actual_Qty=("Qty", "sum"),
            Mach_Rej=("Mach_Rej", "sum"),
            Total_Time=("Time_Min", "sum"),
            Expected_Qty=("Expected_Qty", "sum")
        )
    )

    quality["Quality (%)"] = (
        (1 - (quality["Mach_Rej"] / quality["Actual_Qty"])) * 100
    ).replace([float("inf"), -float("inf")], 0).fillna(0)

    summary = quality.merge(productivity, on="Operator", how="left")

    summary.rename(columns={
        "Operator":"Operator Name",
        "Total_Time":"Total Time (mins.)",
        "Actual_Qty":"Actual Produced Qty.",
        "Expected_Qty":"Expected Qty."
    }, inplace=True)

    summary["Productivity (%)"] = summary["Productivity (%)"].round(2)
    summary["Quality (%)"] = summary["Quality (%)"].round(2)

    # ================= FILTER TEXT =================
    filters = []
    if operator_filter: filters.append(f"Operator={operator_filter}")
    if month_filter: filters.append(f"Month={month_filter}")
    filter_text = " | ".join(filters) if filters else "None"

    # ================= FILE NAME =================
    fname = "Operator_Performance"
    if operator_filter: fname += f"_{operator_filter}"
    if month_filter: fname += f"_M{month_filter}"
    fname += ".xlsx"

    # ================= BUILD EXCEL =================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        create_professional_excel(
            writer=writer,
            sheet_name="Operator Report",
            report_title="Operator Performance Report",
            filters_text=filter_text,
            df=summary
        )

        # RAW DATA
        df.to_excel(writer, sheet_name="Raw Data", index=False)

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# MACHINE WISE OEE REPORT

@app.route("/reports/oee", methods=["GET"])
def reports_oee():
    import pandas as pd
    import os

    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    # ---------- LOAD DATA ----------
    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")
    part_df = load("data/part_master.csv")

    df = pd.concat([main_df, other_df], ignore_index=True)

    # ---------- FILTER VALUES ----------
    machine_filter = request.args.get("machine", "").strip()
    month_filter = request.args.get("month", "").strip()

    if df.empty or part_df.empty:
        return render_template(
            "reports_oee.html",
            active_report="oee",
            records=[],
            machines=[],
            machine_filter=machine_filter,
            selected_month=month_filter
        )

    # ---------- NORMALIZE ----------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Machine"] = df["Machine"].astype(str).str.strip()

    # ---------- MONTH FILTER ----------
    if month_filter:
        df = df[df["Date"].dt.month == int(month_filter)]

    # ---------- MACHINE FILTER ----------
    if machine_filter:
        df = df[df["Machine"] == machine_filter]

    if df.empty:
        return render_template(
            "reports_oee.html",
            active_report="oee",
            records=[],
            machines=sorted(
                pd.concat([main_df, other_df])["Machine"]
                .dropna().astype(str).str.strip().unique().tolist()
            ),
            machine_filter=machine_filter,
            selected_month=month_filter
        )

    # ---------- MERGE CYCLE TIME ----------
    df = df.merge(
        part_df,
        left_on=["Part", "Operation"],
        right_on=["Part Number", "Operation No"],
        how="left"
    )

    df["Cycle Time (min)"] = pd.to_numeric(
        df["Cycle Time (min)"], errors="coerce"
    ).fillna(0)

    # Ignore invalid rows
    df = df[(df["Time_Min"] > 0) & (df["Cycle Time (min)"] > 0)]

    if df.empty:
        return render_template(
            "reports_oee.html",
            active_report="oee",
            records=[],
            machines=sorted(
                pd.concat([main_df, other_df])["Machine"]
                .dropna().astype(str).str.strip().unique().tolist()
            ),
            machine_filter=machine_filter,
            selected_month=month_filter
        )

    # ---------- EXPECTED QTY ----------
    df["Expected_Qty"] = df["Time_Min"] / df["Cycle Time (min)"]

    # ---------- AVAILABLE TIME ----------
    machine_day_time = (
        df.groupby(["Date", "Shift", "Machine"], as_index=False)
        .agg(
            Available_Time=(
                "OT",
                lambda x: 570 if (x.astype(str).str.lower() == "yes").any() else 480
            )
        )
    )

    available_time = (
        machine_day_time
        .groupby("Machine", as_index=False)["Available_Time"]
        .sum()
    )

    # ---------- AGGREGATE ----------
    summary = (
        df.groupby("Machine", as_index=False)
        .agg(
            Time_Spent=("Time_Min", "sum"),
            Produced_Qty=("Qty", "sum"),
            Expected_Qty=("Expected_Qty", "sum"),
            Mach_Rejection=("Mach_Rej", "sum")
        )
    )

    summary = summary.merge(available_time, on="Machine", how="left")

    # ---------- OEE ----------
    summary["Availability_%"] = (
        summary["Time_Spent"] / summary["Available_Time"] * 100
    )

    summary["Performance_%"] = (
        summary["Produced_Qty"] / summary["Expected_Qty"] * 100
    )

    summary["Quality_%"] = (
        1 - (summary["Mach_Rejection"] / summary["Produced_Qty"])
    ) * 100

    summary["OEE_%"] = (
        summary["Availability_%"]
        * summary["Performance_%"]
        * summary["Quality_%"]
        / 10000
    )

    for col in ["Availability_%", "Performance_%", "Quality_%", "OEE_%"]:
        summary[col] = summary[col].round(2)

    summary = summary.sort_values("Machine")

    return render_template(
        "reports_oee.html",
        active_report="oee",
        records=summary.to_dict(orient="records"),
        machines=sorted(
            pd.concat([main_df, other_df])["Machine"]
            .dropna().astype(str).str.strip().unique().tolist()
        ),
        machine_filter=machine_filter,
        selected_month=month_filter,
        months=[
            {"value": f"{i:02d}", "label": m}
            for i, m in enumerate(
                ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"], 1
            )
        ]
    )

# =====================================================
# EXPORT OEE REPORT (PROFESSIONAL + CHART + RAW)
# =====================================================

@app.route("/export/oee", methods=["GET"])
def export_oee_report():

    import pandas as pd
    import io
    import os
    from flask import send_file, request

    # ================= FILTERS =================
    machine_filter = request.args.get("machine", "").strip()
    month_filter = request.args.get("month", "").strip()

    # ================= LOAD =================
    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")
    part_df = load("data/part_master.csv")

    df = pd.concat([main_df, other_df], ignore_index=True)

    if df.empty or part_df.empty:
        return "No data"

    # ================= NORMALIZE =================
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    df["Mach_Rej"] = pd.to_numeric(df["Mach_Rej"], errors="coerce").fillna(0)
    df["Machine"] = df["Machine"].astype(str).str.strip()

    # ================= FILTER =================
    if month_filter:
        df = df[df["Date"].dt.month == int(month_filter)]

    if machine_filter:
        df = df[df["Machine"] == machine_filter]

    if df.empty:
        return "No data after filters"

    # ================= MERGE CYCLE =================
    df = df.merge(
        part_df,
        left_on=["Part","Operation"],
        right_on=["Part Number","Operation No"],
        how="left"
    )

    df["Cycle Time (min)"] = pd.to_numeric(df["Cycle Time (min)"], errors="coerce").fillna(0)
    df = df[(df["Time_Min"] > 0) & (df["Cycle Time (min)"] > 0)]

    if df.empty:
        return "No valid rows"

    df["Expected_Qty"] = df["Time_Min"] / df["Cycle Time (min)"]

    # ================= AVAILABLE =================
    machine_day_time = (
        df.groupby(["Date","Shift","Machine"], as_index=False)
        .agg(
            Available_Time=(
                "OT",
                lambda x: 570 if (x.astype(str).str.lower()=="yes").any() else 480
            )
        )
    )

    available_time = (
        machine_day_time.groupby("Machine",as_index=False)["Available_Time"].sum()
    )

    # ================= SUMMARY =================
    summary = (
        df.groupby("Machine",as_index=False)
        .agg(
            Time_Spent=("Time_Min","sum"),
            Produced_Qty=("Qty","sum"),
            Expected_Qty=("Expected_Qty","sum"),
            Mach_Rejection=("Mach_Rej","sum")
        )
    )

    summary = summary.merge(available_time,on="Machine",how="left")

    summary["Availability (%)"] = (summary["Time_Spent"]/summary["Available_Time"])*100
    summary["Performance (%)"] = (summary["Produced_Qty"]/summary["Expected_Qty"])*100
    summary["Quality (%)"] = (1-(summary["Mach_Rejection"]/summary["Produced_Qty"]))*100
    summary["OEE (%)"] = (
        summary["Availability (%)"]
        * summary["Performance (%)"]
        * summary["Quality (%)"] / 10000
    )

    for c in ["Availability (%)","Performance (%)","Quality (%)","OEE (%)"]:
        summary[c] = summary[c].round(2)

    summary = summary.sort_values("Machine")

    # ================= FILTER TEXT =================
    filters=[]
    if machine_filter: filters.append(f"Machine={machine_filter}")
    if month_filter: filters.append(f"Month={month_filter}")
    filter_text=" | ".join(filters) if filters else "None"

    # ================= FILE NAME =================
    fname="OEE_Report"
    if machine_filter: fname+=f"_{machine_filter}"
    if month_filter: fname+=f"_M{month_filter}"
    fname+=".xlsx"

    # ================= EXCEL =================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # MAIN REPORT
        create_professional_excel(
            writer=writer,
            sheet_name="OEE Report",
            report_title="Machine OEE Report",
            filters_text=filter_text,
            df=summary
        )

        workbook = writer.book
        chart_sheet = workbook.add_worksheet("OEE Chart")

        # -------- chart data --------
        chart_sheet.write_row("A1", ["Machine","OEE"])
        for i,r in summary.iterrows():
            chart_sheet.write_row(i+1,0,[r["Machine"],r["OEE (%)"]])

        chart = workbook.add_chart({"type":"column"})
        chart.add_series({
            "name":"OEE %",
            "categories":["OEE Chart",1,0,len(summary),0],
            "values":["OEE Chart",1,1,len(summary),1],
            "data_labels":{"value":True}
        })
        chart.set_y_axis({"max":100})
        chart.set_title({"name":"OEE by Machine"})
        chart_sheet.insert_chart("D2", chart, {"x_scale":1.6,"y_scale":1.6})

        # RAW DATA
        df.to_excel(writer, sheet_name="Raw Data", index=False)

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# MACHINE UTILIZATION REPORT (WITH MACHINE + MONTH FILTER)

@app.route("/reports/machine", methods=["GET"])
def reports_machine():
    import pandas as pd
    import os

    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    # ---------- LOAD DATA ----------
    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")

    df = pd.concat([main_df, other_df], ignore_index=True)

    machine_filter = request.args.get("machine", "").strip()
    month_filter = request.args.get("month", "").strip()

    if df.empty:
        return render_template(
            "reports_machine.html",
            active_report="machine",
            records=[],
            machines=[],
            machine_filter=machine_filter,
            selected_month=month_filter,
            months=[]
        )

    # ---------- NORMALIZE ----------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Machine"] = df["Machine"].astype(str).str.strip()

    # ---------- MONTH FILTER ----------
    if month_filter:
        df = df[df["Date"].dt.month == int(month_filter)]

    # ---------- MACHINE FILTER ----------
    if machine_filter:
        df = df[df["Machine"] == machine_filter]

    # ---------- HARD EXIT ----------
    if df.empty:
        return render_template(
            "reports_machine.html",
            active_report="machine",
            records=[],
            machines=sorted(pd.concat([main_df, other_df])["Machine"].dropna().unique().tolist()),
            machine_filter=machine_filter,
            selected_month=month_filter,
            months=[
                {"value": f"{i:02d}", "label": m}
                for i, m in enumerate(
                    ["January","February","March","April","May","June",
                     "July","August","September","October","November","December"], 1
                )
            ]
        )

    # ---------- TIME SPENT ----------
    summary = (
        df.groupby(["Date", "Machine"], as_index=False)
        .agg(Time_Spent=("Time_Min", "sum"))
    )

    # ---------- AVAILABLE TIME ----------
    summary["Available_Time"] = 960

    # ---------- UTILIZATION ----------
    summary["Utilization_%"] = (
        (summary["Time_Spent"] / summary["Available_Time"]) * 100
    ).replace([float("inf"), -float("inf")], 0).fillna(0)

    # ---------- FORMAT ----------
    summary["Date"] = summary["Date"].dt.strftime("%Y-%m-%d")

    summary["Date_Display"] = pd.to_datetime(
        summary["Date"],
        errors="coerce"
    ).dt.strftime("%d-%m-%Y")

    summary["Time_Spent"] = summary["Time_Spent"].round(2)
    summary["Available_Time"] = summary["Available_Time"].round(2)
    summary["Utilization_%"] = summary["Utilization_%"].round(2)

    # ---------- SORT ----------
    summary = summary.sort_values(
        by=["Date", "Machine"],
        ascending=[False, True]
    )

    # ---------- MACHINE LIST ----------
    all_machines = sorted(
        pd.concat([main_df, other_df])["Machine"]
        .dropna().astype(str).str.strip().unique().tolist()
    )

    return render_template(
        "reports_machine.html",
        active_report="machine",
        records=summary.to_dict(orient="records"),
        machines=all_machines,
        machine_filter=machine_filter,
        selected_month=month_filter,
        months=[
            {"value": f"{i:02d}", "label": m}
            for i, m in enumerate(
                ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"], 1
            )
        ]
    )

# =====================================================
# EXPORT MACHINE UTILIZATION REPORT (PROFESSIONAL + CHART + RAW)
# =====================================================

@app.route("/export/machine", methods=["GET"])
def export_machine_report():

    import pandas as pd
    import io
    import os
    from flask import send_file, request

    # ================= FILTERS =================
    machine_filter = request.args.get("machine", "").strip()
    month_filter = request.args.get("month", "").strip()

    # ================= LOAD =================
    def load(path):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)

    main_df = load("data/production_main.csv")
    other_df = load("data/production_other_machine.csv")

    df = pd.concat([main_df, other_df], ignore_index=True)

    if df.empty:
        return "No data"

    # ================= NORMALIZE =================
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Machine"] = df["Machine"].astype(str).str.strip()

    # ================= FILTER =================
    if month_filter:
        df = df[df["Date"].dt.month == int(month_filter)]

    if machine_filter:
        df = df[df["Machine"] == machine_filter]

    if df.empty:
        return "No data after filters"

    # ================= SUMMARY =================
    summary = (
        df.groupby(["Date","Machine"], as_index=False)
        .agg(Time_Spent=("Time_Min","sum"))
    )

    summary["Available_Time"] = 960
    summary["Utilization (%)"] = (
        (summary["Time_Spent"]/summary["Available_Time"])*100
    ).replace([float("inf"),-float("inf")],0).fillna(0)

    summary["Date"] = summary["Date"].dt.strftime("%d-%m-%Y")

    summary["Time_Spent"] = summary["Time_Spent"].round(2)
    summary["Available_Time"] = summary["Available_Time"].round(2)
    summary["Utilization (%)"] = summary["Utilization (%)"].round(2)

    summary = summary.sort_values(by=["Date","Machine"],ascending=[False,True])

    # ================= FILTER TEXT =================
    filters=[]
    if machine_filter: filters.append(f"Machine={machine_filter}")
    if month_filter: filters.append(f"Month={month_filter}")
    filter_text=" | ".join(filters) if filters else "None"

    # ================= FILE NAME =================
    fname="Machine_Utilization"
    if machine_filter: fname+=f"_{machine_filter}"
    if month_filter: fname+=f"_M{month_filter}"
    fname+=".xlsx"

    # ================= EXCEL =================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # MAIN REPORT
        create_professional_excel(
            writer=writer,
            sheet_name="Machine Utilization",
            report_title="Machine Utilization Report",
            filters_text=filter_text,
            df=summary
        )

        workbook = writer.book
        chart_sheet = workbook.add_worksheet("Utilization Chart")

        # ---------- chart data ----------
        chart_sheet.write_row("A1", ["Date-Machine","Utilization"])

        for i,r in summary.iterrows():
            chart_sheet.write_row(
                i+1,
                0,
                [f"{r['Date']} | {r['Machine']}", r["Utilization (%)"]]
            )

        chart = workbook.add_chart({"type":"column"})

        chart.add_series({
            "name":"Utilization %",
            "categories":["Utilization Chart",1,0,len(summary),0],
            "values":["Utilization Chart",1,1,len(summary),1],
            "data_labels":{"value":True}
        })

        chart.set_y_axis({"max":100})
        chart.set_title({"name":"Machine Utilization (%)"})

        chart_sheet.insert_chart("D2", chart, {"x_scale":1.6,"y_scale":1.6})

        # RAW DATA
        df.to_excel(writer, sheet_name="Raw Data", index=False)

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# LOSS ANALYSIS REPORT (ONE PIE – LOSS DISTRIBUTION)

@app.route("/reports/loss", methods=["GET"])
def reports_loss():
    import pandas as pd
    import os

    LOSS_FILE = "data/production_loss.csv"

    selected_month = request.args.get("month", "").strip()

    if not os.path.exists(LOSS_FILE) or os.path.getsize(LOSS_FILE) == 0:
        return render_template(
            "reports_loss.html",
            active_report="loss",
            loss_data=[],
            total_minutes=0,
            total_hours=0,
            selected_month=selected_month,
            months=[]
        )

    df = pd.read_csv(LOSS_FILE)

    if df.empty:
        return render_template(
            "reports_loss.html",
            active_report="loss",
            loss_data=[],
            total_minutes=0,
            total_hours=0,
            selected_month=selected_month,
            months=[]
        )

    # ---------- NORMALIZE ----------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Loss_Reason"] = df["Loss_Reason"].astype(str)

    # ---------- MONTH FILTER ----------
    if selected_month:
        df = df[df["Date"].dt.month == int(selected_month)]

    if df.empty:
        return render_template(
            "reports_loss.html",
            active_report="loss",
            loss_data=[],
            total_minutes=0,
            total_hours=0,
            selected_month=selected_month,
            months=[]
        )

    # ---------- AGGREGATE ----------
    summary = (
        df.groupby("Loss_Reason", as_index=False)
        .agg(Total_Time=("Time_Min", "sum"))
    )

    # ---------- CHART DATA ----------
    loss_data = [
        {
            "label": row["Loss_Reason"],
            "value": float(row["Total_Time"])
        }
        for _, row in summary.iterrows()
        if row["Total_Time"] > 0
    ]

    total_minutes = int(summary["Total_Time"].sum())
    total_hours = round(total_minutes / 60, 2)

    months = [
        {"value": "01", "label": "January"},
        {"value": "02", "label": "February"},
        {"value": "03", "label": "March"},
        {"value": "04", "label": "April"},
        {"value": "05", "label": "May"},
        {"value": "06", "label": "June"},
        {"value": "07", "label": "July"},
        {"value": "08", "label": "August"},
        {"value": "09", "label": "September"},
        {"value": "10", "label": "October"},
        {"value": "11", "label": "November"},
        {"value": "12", "label": "December"},
    ]

    return render_template(
        "reports_loss.html",
        active_report="loss",
        loss_data=loss_data,
        total_minutes=total_minutes,
        total_hours=total_hours,
        selected_month=selected_month,
        months=months
    )

# =====================================================
# EXPORT LOSS ANALYSIS REPORT (PROFESSIONAL + PIE + RAW)
# =====================================================

@app.route("/export/loss", methods=["GET"])
def export_loss_report():

    import pandas as pd
    import io
    import os
    from flask import send_file, request

    LOSS_FILE = "data/production_loss.csv"

    month_filter = request.args.get("month","").strip()

    if not os.path.exists(LOSS_FILE) or os.path.getsize(LOSS_FILE)==0:
        return "No data"

    df = pd.read_csv(LOSS_FILE)

    if df.empty:
        return "No data"

    # ================= NORMALIZE =================
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Time_Min"] = pd.to_numeric(df["Time_Min"], errors="coerce").fillna(0)
    df["Loss_Reason"] = df["Loss_Reason"].astype(str)

    # ================= FILTER =================
    if month_filter:
        df = df[df["Date"].dt.month == int(month_filter)]

    if df.empty:
        return "No data after filters"

    # ================= SUMMARY =================
    summary = (
        df.groupby("Loss_Reason",as_index=False)
        .agg(Total_Time=("Time_Min","sum"))
    )

    summary = summary[summary["Total_Time"]>0]
    summary["Hours"] = (summary["Total_Time"]/60).round(2)
    summary = summary.sort_values("Total_Time",ascending=False)

    # ================= FILTER TEXT =================
    filters=[]
    if month_filter: filters.append(f"Month={month_filter}")
    filter_text=" | ".join(filters) if filters else "None"

    # ================= FILE NAME =================
    fname="Loss_Analysis"
    if month_filter: fname+=f"_M{month_filter}"
    fname+=".xlsx"

    # ================= EXCEL =================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        # MAIN REPORT
        create_professional_excel(
            writer=writer,
            sheet_name="Loss Report",
            report_title="Loss Analysis Report",
            filters_text=filter_text,
            df=summary
        )

        workbook = writer.book
        chart_sheet = workbook.add_worksheet("Loss Chart")

        # ---------- chart data ----------
        chart_sheet.write_row("A1", ["Reason","Minutes"])

        for i,r in summary.iterrows():
            chart_sheet.write_row(i+1,0,[r["Loss_Reason"], r["Total_Time"]])

        chart = workbook.add_chart({"type":"pie"})

        chart.add_series({
            "name":"Loss Distribution",
            "categories":["Loss Chart",1,0,len(summary),0],
            "values":["Loss Chart",1,1,len(summary),1],
            "data_labels":{"percentage":True}
        })

        chart.set_title({"name":"Loss Distribution"})

        chart_sheet.insert_chart("D2", chart, {"x_scale":1.6,"y_scale":1.6})

        # RAW DATA
        df.to_excel(writer, sheet_name="Raw Data", index=False)

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ============================================================
# MANAGEMENT DASHBOARD
# ============================================================

@app.route("/management_dashboard", methods=["GET"])
def management_dashboard():
    dashboard = get_dashboard_kpis()

    if not dashboard:
        return render_template(
            "management_dashboard.html",
            dashboard={}
        )

    return render_template(
        "management_dashboard.html",
        dashboard=dashboard
    )

# =========================================
# STORES ITEM MASTER FILE (ERP V2)
# =========================================

STORE_ITEM_FILE = os.path.join(DATA_FOLDER, "store_items.csv")

# Create new structured file if not exists
if not os.path.exists(STORE_ITEM_FILE):
    df = pd.DataFrame(columns=[
        "Item Code",
        "Category",
        "Unit",
        "RM Item Name",
        "FG Item Name",
        "Min Stock",
        "RM Rate",
        "FG Rate"
    ])
    df.to_csv(STORE_ITEM_FILE, index=False)

# =========================================
# STORES MODULE HOME
# =========================================
@app.route("/stores")
def stores_home():
    return render_template("stores_home.html")

# =====================================================
# STORES CONTROL TOWER (SYNC WITH LIVE INVENTORY)
# =====================================================
@app.route("/stores/dashboard")
def stores_dashboard():

    from datetime import datetime, timedelta
    import pandas as pd

    items_df = pd.read_csv(STORE_ITEM_FILE)
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    if items_df.empty:
        return render_template("stores_dashboard.html", data={})

    if ledger_df.empty:
        ledger_df = pd.DataFrame(columns=["Item","Qty","Inward_Type","Date","Value"])

    ledger_df["Qty"] = pd.to_numeric(ledger_df["Qty"], errors="coerce").fillna(0)
    ledger_df["Value"] = pd.to_numeric(ledger_df["Value"], errors="coerce").fillna(0)
    ledger_df["Date"] = pd.to_datetime(ledger_df["Date"], errors="coerce")
    ledger_df["Item"] = ledger_df["Item"].astype(str).str.strip()

    today = datetime.today()
    month_start = today.replace(day=1)

    summary = []

    # =====================================================
    # SAME LOGIC AS LIVE INVENTORY
    # =====================================================
    for _, row in items_df.iterrows():

        code = str(row["Item Code"]).strip()
        category = str(row.get("Category",""))

        rm_rate = float(row.get("RM Rate",0) or 0)
        fg_rate = float(row.get("FG Rate",rm_rate) or rm_rate)
        min_stock = float(row.get("Min Stock",0) or 0)

        item_ledger = ledger_df[ledger_df["Item"] == code]

        inward_rm = item_ledger[item_ledger["Inward_Type"]=="INWARD"]["Qty"].sum()
        issued = item_ledger[item_ledger["Inward_Type"]=="ISSUE"]["Qty"].sum()

        return_rm = item_ledger[item_ledger["Inward_Type"]=="RETURN_RM"]["Qty"].sum()
        return_fg = item_ledger[item_ledger["Inward_Type"]=="RETURN_FG"]["Qty"].sum()
        return_reject = item_ledger[item_ledger["Inward_Type"]=="RETURN_REJECT"]["Qty"].sum()

        outward_fg = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_FG"]["Qty"].sum()
        outward_rm = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_RM"]["Qty"].sum()
        outward_reject = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_REJECT"]["Qty"].sum()
        outward_wip = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_WIP"]["Qty"].sum()

        adj_rm = item_ledger[item_ledger["Inward_Type"]=="ADJ_RM"]["Qty"].sum()
        adj_fg = item_ledger[item_ledger["Inward_Type"]=="ADJ_FG"]["Qty"].sum()
        adj_wip = item_ledger[item_ledger["Inward_Type"]=="ADJ_WIP"]["Qty"].sum()
        adj_rej = item_ledger[item_ledger["Inward_Type"]=="ADJ_REJECT"]["Qty"].sum()
        opening = item_ledger[item_ledger["Inward_Type"]=="OPENING"]["Qty"].sum()

        rm_stock = inward_rm - issued + return_rm - outward_rm + adj_rm + opening
        wip_stock = issued - return_fg - return_reject - outward_wip + adj_wip
        fg_stock = return_fg - outward_fg + adj_fg
        reject_stock = return_reject - outward_reject + adj_rej

        rm_stock = max(rm_stock,0)
        wip_stock = max(wip_stock,0)
        fg_stock = max(fg_stock,0)
        reject_stock = max(reject_stock,0)

        rm_value = rm_stock * rm_rate
        wip_value = wip_stock * (fg_rate * 0.75)
        fg_value = fg_stock * fg_rate
        reject_value = reject_stock * rm_rate

        total_value = rm_value + wip_value + fg_value + reject_value

        summary.append({
            "Item Code": code,
            "Category": category,

            # qty
            "RM Stock": rm_stock,
            "WIP Stock": wip_stock,
            "FG Stock": fg_stock,
            "Reject Stock": reject_stock,
            "Total Stock": rm_stock + wip_stock + fg_stock + reject_stock,

            # values
            "RM Value": rm_value,
            "WIP Value": wip_value,
            "FG Value": fg_value,
            "Reject Value": reject_value,
            "Total Value": total_value,

            "Min": min_stock,
            "Last": item_ledger["Date"].max()
        })

    df = pd.DataFrame(summary)

    # =====================================================
    # HIGH VALUE INVENTORY (TOP 15)
    # =====================================================
    high_value_df = (
        df.sort_values("Total Value", ascending=False)
        .head(15)
    )

    # =====================================================
    # SLOW MOVING WIP (>15 days no FG return)
    # =====================================================
    cutoff_wip = today - timedelta(days=15)

    last_fg = ledger_df[ledger_df["Inward_Type"]=="RETURN_FG"] \
                .groupby("Item")["Date"].max().reset_index()

    last_fg.columns = ["Item Code","Last FG"]

    slow_wip_df = df.merge(last_fg, on="Item Code", how="left")

    slow_wip_df = slow_wip_df[
        (slow_wip_df["WIP Stock"] > 0) &
        (
            slow_wip_df["Last FG"].isna() |
            (slow_wip_df["Last FG"] < cutoff_wip)
        )
    ]

    # =====================================================
    # HIGH REJECTION THIS MONTH
    # =====================================================
    rej_month = ledger_df[
        (ledger_df["Inward_Type"]=="RETURN_REJECT") &
        (ledger_df["Date"]>=month_start)
    ]

    high_rej_df = (
        rej_month.groupby("Item",as_index=False)["Qty"]
        .sum()
        .rename(columns={"Qty":"Qty"})
    )

    # attach value
    high_rej_df = high_rej_df.merge(
        items_df[["Item Code","RM Rate"]],
        left_on="Item",
        right_on="Item Code",
        how="left"
    )

    high_rej_df["Value"] = high_rej_df["Qty"] * high_rej_df["RM Rate"]
    high_rej_df = high_rej_df.sort_values("Value", ascending=False).head(15)

    # ================= KPI =================
    total_inventory_value = df["Total Value"].sum()
    rm_total = df["RM Value"].sum()
    wip_total = df["WIP Value"].sum()
    fg_total = df["FG Value"].sum()
    reject_total = df["Reject Value"].sum()

    # ================= MONTH CONSUMPTION =================
    month_issue = ledger_df[
        (ledger_df["Inward_Type"]=="ISSUE") &
        (ledger_df["Date"]>=month_start)
    ]

    month_consumption = month_issue["Value"].sum()

    # ================= LOW STOCK =================
    low_stock_df = df[df["RM Stock"] <= df["Min"]]
    low_stock_count = len(low_stock_df)

    # ================= DEAD STOCK 15 DAYS =================
    cutoff = today - timedelta(days=15)
    dead_df = df[df["Last"] < cutoff]
    dead_value = dead_df["Total Value"].sum()

    # ================= CATEGORY VALUE =================
    cat_value = (
        df.groupby("Category",as_index=False)["Total Value"]
        .sum()
        .rename(columns={"Total Value":"Value"})
        .sort_values("Value",ascending=False)
    )

    # ================= TOP CONSUMPTION =================
    top_consumed = (
        month_issue.groupby("Item",as_index=False)["Value"]
        .sum()
        .sort_values("Value",ascending=False)
        .head(10)
    )

    # ================= DAILY TREND =================
    month_issue["DateOnly"] = month_issue["Date"].dt.date

    daily_trend = (
        month_issue.groupby("DateOnly", as_index=False)
        .agg(Value=("Value","sum"))
        .sort_values("DateOnly")
    )

    daily_trend["Date"] = daily_trend["DateOnly"].astype(str)
    daily_trend = daily_trend[["Date","Value"]]

    data = {
        "kpi":{
            "inventory_value":round(total_inventory_value,2),
            "rm_value":round(rm_total,2),
            "wip_value":round(wip_total,2),
            "fg_value":round(fg_total,2),
            "reject_value":round(reject_total,2),
            "month_consumption":round(month_consumption,2),
            "low_stock_count":int(low_stock_count),
            "dead_stock_value":round(dead_value,2)
        },

        "category_value":cat_value.to_dict(orient="records"),
        "top_consumed":top_consumed.to_dict(orient="records"),
        "daily_trend":daily_trend.to_dict(orient="records"),

        "low_stock":low_stock_df.to_dict(orient="records"),
        "dead_stock":dead_df.to_dict(orient="records"),

        # NEW TABLES
        "high_value":high_value_df.to_dict(orient="records"),
        "slow_wip":slow_wip_df.to_dict(orient="records"),
        "high_rej":high_rej_df.to_dict(orient="records")
    }

    return render_template("stores_dashboard.html", data=data)

# =========================================
# STORES ITEM MASTER (ERP V2)
# =========================================
@app.route("/stores/item_master", methods=["GET", "POST"])
def stores_item_master():

    df = pd.read_csv(STORE_ITEM_FILE)

    # ==============================
    # HANDLE POST
    # ==============================
    if request.method == "POST":

        # -------- EXCEL UPLOAD --------
        if "excel_file" in request.files:
            file = request.files["excel_file"]

            if file.filename != "":
                
                upload_df = pd.read_excel(file)

                # Ensure correct columns
                required_cols = [
                    "Item Code","Category","Unit",
                    "RM Item Name","FG Item Name",
                    "Min Stock","RM Rate","FG Rate"
                ]

                for col in required_cols:
                    if col not in upload_df.columns:
                        return f"Missing column: {col}"

                # Clean upload
                upload_df = upload_df[required_cols].copy()

                upload_df["Item Code"] = upload_df["Item Code"].astype(str).str.strip()

                # Remove blank item codes
                upload_df = upload_df[upload_df["Item Code"] != ""]

                # Convert numeric safely
                for c in ["Min Stock","RM Rate","FG Rate"]:
                    upload_df[c] = pd.to_numeric(upload_df[c], errors="coerce").fillna(0)

                upload_df.to_csv(STORE_ITEM_FILE, index=False)

        # -------- EDIT SAVE --------
        if "edit_item_code" in request.form:

            code = request.form.get("edit_item_code")

            category = request.form.get("category")
            unit = request.form.get("unit")
            rm_name = request.form.get("rm_name")
            fg_name = request.form.get("fg_name")
            min_stock = request.form.get("min_stock", 0)
            rm_rate = request.form.get("rm_rate", 0)
            fg_rate = request.form.get("fg_rate", 0)

            mask = df["Item Code"] == code

            df.loc[mask, "Category"] = category
            df.loc[mask, "Unit"] = unit
            df.loc[mask, "RM Item Name"] = rm_name
            df.loc[mask, "FG Item Name"] = fg_name
            df.loc[mask, "Min Stock"] = float(min_stock) if min_stock else 0
            df.loc[mask, "RM Rate"] = float(rm_rate) if rm_rate else 0
            df.loc[mask, "FG Rate"] = float(fg_rate) if fg_rate else 0

            df.to_csv(STORE_ITEM_FILE, index=False)

        return redirect("/stores/item_master")

    # ==============================
    # GET VIEW
    # ==============================
    df = pd.read_csv(STORE_ITEM_FILE)

    if not df.empty:
        df = df.sort_values("Item Code")

    records = df.to_dict(orient="records")

    return render_template(
        "stores_item_master.html",
        records=records
    )

# =========================================
# DELETE ITEM CODE
# =========================================
@app.route("/stores/delete_item")
def delete_store_item():

    code = request.args.get("code")

    df = pd.read_csv(STORE_ITEM_FILE)
    df = df[df["Item Code"] != code]
    df.to_csv(STORE_ITEM_FILE, index=False)

    return redirect("/stores/item_master")

# =========================================
# EDIT STORE ITEM (NEW MASTER STRUCTURE)
# =========================================
@app.route("/stores/edit_item", methods=["POST"])
def edit_store_item():

    old_code = request.form.get("old_item_code")   # hidden field in html

    category = request.form.get("category")
    unit = request.form.get("unit")

    rm_name = request.form.get("rm_name", "")
    fg_name = request.form.get("fg_name", "")

    min_stock = request.form.get("min_stock", 0)
    rm_rate = request.form.get("rm_rate", 0)
    fg_rate = request.form.get("fg_rate", 0)

    df = pd.read_csv(STORE_ITEM_FILE)

    mask = df["Item Code"] == old_code

    df.loc[mask, "Category"] = category
    df.loc[mask, "Unit"] = unit
    df.loc[mask, "RM Item Name"] = rm_name
    df.loc[mask, "FG Item Name"] = fg_name
    df.loc[mask, "Min Stock"] = float(min_stock) if min_stock else 0
    df.loc[mask, "RM Rate"] = float(rm_rate) if rm_rate else 0
    df.loc[mask, "FG Rate"] = float(fg_rate) if fg_rate else 0

    df.to_csv(STORE_ITEM_FILE, index=False)

    return redirect("/stores/item_master")

# =====================================================
# STORES LEDGER SYSTEM (MAIN ENGINE)
# =====================================================

STORE_LEDGER_FILE = os.path.join(DATA_FOLDER, "store_ledger.csv")

# Ensure ledger file exists
if not os.path.exists(STORE_LEDGER_FILE):
    pd.DataFrame(columns=[
        "Date",
        "Item",
        "Inward_Type",
        "Qty",
        "Rate",
        "Value",
        "Supplier",
        "Ref_No",
        "Remarks",
        "User",
        "Timestamp"
    ]).to_csv(STORE_LEDGER_FILE, index=False)

# =====================================================
# STORES INWARD PAGE (ERP v2 - ITEM CODE BASED)
# =====================================================
@app.route("/stores/inward", methods=["GET"])
def stores_inward():

    items_df = pd.read_csv(STORE_ITEM_FILE)

    # =========================================
    # ALLOWED CATEGORIES FOR INWARD
    # =========================================
    allowed = [
        "Production Item",
        "Bought Out Items",
        "Packing Material",
        "Consumables",
        "Cutting Tools"
    ]

    items_df = items_df[
        items_df["Category"].isin(allowed)
    ].copy()

    # =========================================
    # DISPLAY NAME LOGIC
    # =========================================
    dropdown = []

    for _, r in items_df.iterrows():

        code = str(r["Item Code"]).strip()
        cat = str(r["Category"]).strip()

        if cat == "Production Item":
            name = str(r["RM Item Name"]).strip()
        else:
            name = str(r["RM Item Name"]).strip()

        label = f"{code} — {name}"
        dropdown.append({
            "code": code,
            "label": label
        })

    dropdown = sorted(dropdown, key=lambda x: x["code"])

    # =========================================
    # SHOW ONLY INWARD ENTRIES
    # =========================================
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)
    ledger_df = ledger_df[ledger_df["Inward_Type"] == "INWARD"]

    if not ledger_df.empty:
        ledger_df = ledger_df.sort_values("Timestamp", ascending=False)

    return render_template(
        "stores_inward.html",
        items=dropdown,
        records=ledger_df.to_dict(orient="records")
    )

# =====================================================
# SAVE STORES INWARD (ERP v2 - ITEM CODE BASED)
# =====================================================
@app.route("/stores/save_inward", methods=["POST"])
def save_stores_inward():

    from datetime import datetime

    date = request.form.get("date")
    invoice = request.form.get("invoice")
    item_code = request.form.get("item")   # now item = Item Code
    qty = request.form.get("qty")
    vendor = request.form.get("vendor")
    received_by = request.form.get("received_by")
    remarks = request.form.get("remarks")

    if not all([date, invoice, item_code, qty, vendor, received_by]):
        return redirect("/stores/inward")

    qty = float(qty)

    # =========================================
    # GET ITEM MASTER DATA
    # =========================================
    item_df = pd.read_csv(STORE_ITEM_FILE)

    row = item_df[item_df["Item Code"] == item_code]

    if row.empty:
        return redirect("/stores/inward")

    row = row.iloc[0]

    category = str(row["Category"]).strip()
    rm_rate = float(row.get("RM Rate", 0))

    # =========================================
    # BLOCK FG ONLY ITEMS FROM INWARD
    # =========================================
    if category == "Production Item":
        rate = rm_rate
    else:
        rate = rm_rate

    value = qty * rate

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    new_row = {
        "Date": date,
        "Item": item_code,   # 🔴 STORE ITEM CODE
        "Inward_Type": "INWARD",
        "Qty": qty,
        "Rate": rate,
        "Value": value,
        "Supplier": vendor,
        "Ref_No": invoice,
        "Remarks": f"Received By: {received_by} | {remarks}",
        "User": "system",
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    ledger_df = pd.concat(
        [ledger_df, pd.DataFrame([new_row])],
        ignore_index=True
    )

    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/inward")

# =========================================
# STORES DELETE/EDIT SECURITY CODE
# =========================================
STORES_DELETE_CODE = "cati123"

# =====================================================
# DELETE STORES INWARD ENTRY
# =====================================================
@app.route("/stores/delete_inward", methods=["POST"])
def delete_inward():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    timestamp = request.form.get("timestamp")

    df = pd.read_csv(STORE_LEDGER_FILE)

    before = len(df)

    df = df[df["Timestamp"] != timestamp]

    after = len(df)

    if before == after:
        return "NOT_FOUND", 404

    df.to_csv(STORE_LEDGER_FILE, index=False)
    backup_to_drive()
    return "OK", 200

# =====================================================
# EDIT STORES INWARD SAVE (ERP v2 - ITEM CODE BASED)
# =====================================================
@app.route("/stores/edit_inward", methods=["POST"])
def edit_inward():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    old_timestamp = request.form.get("old_timestamp")

    date = request.form.get("date")
    invoice = request.form.get("invoice")
    item_code = request.form.get("item")
    qty = float(request.form.get("qty"))
    vendor = request.form.get("vendor")
    received_by = request.form.get("received_by")
    remarks = request.form.get("remarks")

    # =========================================
    # GET RM RATE FROM ITEM MASTER
    # =========================================
    item_df = pd.read_csv(STORE_ITEM_FILE)
    row = item_df[item_df["Item Code"] == item_code]

    if row.empty:
        return redirect("/stores/inward")

    row = row.iloc[0]
    rm_rate = float(row.get("RM Rate", 0))

    rate = rm_rate
    value = qty * rate

    df = pd.read_csv(STORE_LEDGER_FILE)

    mask = df["Timestamp"] == old_timestamp

    df.loc[mask, "Date"] = date
    df.loc[mask, "Item"] = item_code   # 🔴 STORE CODE
    df.loc[mask, "Qty"] = qty
    df.loc[mask, "Rate"] = rate
    df.loc[mask, "Value"] = value
    df.loc[mask, "Supplier"] = vendor
    df.loc[mask, "Ref_No"] = invoice
    df.loc[mask, "Remarks"] = f"Received By: {received_by} | {remarks}"

    df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/inward")

# =====================================================
# STORES ISSUE PAGE (ITEM CODE BASED)
# =====================================================
@app.route("/stores/issue", methods=["GET"])
def stores_issue():

    items_df = pd.read_csv(STORE_ITEM_FILE)
    error = request.args.get("error", "")

    # =========================
    # BUILD ISSUE ITEM LIST
    # =========================
    issue_items = []

    for _, r in items_df.iterrows():

        code = str(r.get("Item Code","")).strip()
        cat = str(r.get("Category","")).strip()
        rm_name = str(r.get("RM Item Name","")).strip()

        if code == "":
            continue

        # ---------- Production items ----------
        if cat.lower() == "production item":

            label = f"{code} — {rm_name}" if rm_name else f"{code} — RM NOT DEFINED"

            issue_items.append({
                "code": code,
                "label": label
            })

        # ---------- Other inward categories ----------
        elif cat.lower() in [
            "bought out items",
            "packing material",
            "consumables",
            "cutting tools"
        ]:

            issue_items.append({
                "code": code,
                "label": f"{code} — {cat}"
            })

    # sort by item code
    issue_items = sorted(issue_items, key=lambda x: x["code"])

    # =========================
    # PURPOSES
    # =========================
    purposes = [
        "Issued to Production - CNC",
        "Issued to Production - VMC",
        "Issued to Production - Deburring",
        "Issued for Rework",
        "Tool Room Consumption",
        "Machine Maintenance",
        "Preventive Maintenance",
        "Fixture Development",
        "Trial / Setting",
        "Material to Job Work",
        "R&D / Testing",
        "Scrap / Wastage",
        "Packing Material"
    ]

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    if not ledger_df.empty:
        ledger_df = ledger_df.sort_values("Timestamp", ascending=False)

    # show only issue entries
    issue_records = ledger_df[ledger_df["Inward_Type"] == "ISSUE"]

    return render_template(
        "stores_issue.html",
        items=issue_items,
        purposes=purposes,
        records=issue_records.to_dict(orient="records"),
        error=error
    )

# =====================================================
# SAVE ISSUE ENTRY (ITEM CODE BASED)
# =====================================================
@app.route("/stores/save_issue", methods=["POST"])
def save_issue():

    from datetime import datetime

    date = request.form.get("date")
    item_code = request.form.get("item")   # now item code
    qty = request.form.get("qty")
    purpose = request.form.get("purpose")
    issued_by = request.form.get("issued_by")
    remarks = request.form.get("remarks")

    if not all([date, item_code, qty, purpose, issued_by]):
        return redirect("/stores/issue")

    qty = float(qty)

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    # =========================
    # CURRENT STOCK CALC
    # =========================
    item_df = ledger_df[ledger_df["Item"] == item_code]

    inward = item_df[item_df["Inward_Type"] == "INWARD"]["Qty"].sum()
    issued = item_df[item_df["Inward_Type"] == "ISSUE"]["Qty"].sum()
    returned = item_df[item_df["Inward_Type"] == "RETURN"]["Qty"].sum()

    current_stock = inward - issued + returned

    if qty > current_stock:
        msg = f"Insufficient stock. Available = {round(current_stock,2)}"
        return redirect(url_for("stores_issue", error=msg))

    # =========================
    # GET RM RATE FROM MASTER
    # =========================
    items_df = pd.read_csv(STORE_ITEM_FILE)
    row = items_df[items_df["Item Code"] == item_code]

    if not row.empty:
        rm_rate = float(row.iloc[0].get("RM Rate", 0) or 0)
    else:
        rm_rate = 0

    value = qty * rm_rate

    # =========================
    # LEDGER ENTRY
    # =========================
    new_row = {
        "Date": date,
        "Item": item_code,
        "Inward_Type": "ISSUE",
        "Qty": qty,
        "Rate": rm_rate,
        "Value": value,
        "Supplier": "",
        "Ref_No": "",
        "Remarks": f"{purpose} | Issued By: {issued_by} | {remarks}",
        "User": "system",
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    ledger_df = pd.concat([ledger_df, pd.DataFrame([new_row])], ignore_index=True)
    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/issue")

# =====================================================
# DELETE ISSUE ENTRY
# =====================================================
@app.route("/stores/delete_issue", methods=["POST"])
def delete_issue():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    timestamp = request.form.get("timestamp")

    df = pd.read_csv(STORE_LEDGER_FILE)

    before = len(df)
    df = df[df["Timestamp"] != timestamp]
    after = len(df)

    if before == after:
        return "NOT_FOUND", 404

    df.to_csv(STORE_LEDGER_FILE, index=False)
    backup_to_drive()
    return "OK", 200

# =====================================================
# EDIT ISSUE ENTRY (ITEM CODE BASED)
# =====================================================
@app.route("/stores/edit_issue", methods=["POST"])
def edit_issue():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    old_timestamp = request.form.get("old_timestamp")

    date = request.form.get("date")
    item_code = request.form.get("item")
    qty = float(request.form.get("qty"))
    purpose = request.form.get("purpose")
    issued_by = request.form.get("issued_by")
    remarks = request.form.get("remarks")

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    # ================= STOCK CHECK AGAIN =================
    item_df = ledger_df[ledger_df["Item"] == item_code]

    inward = item_df[item_df["Inward_Type"] == "INWARD"]["Qty"].sum()
    issued = item_df[item_df["Inward_Type"] == "ISSUE"]["Qty"].sum()
    returned = item_df[item_df["Inward_Type"] == "RETURN"]["Qty"].sum()

    current_stock = inward - issued + returned

    if qty > current_stock:
        return redirect("/stores/issue?error=Edit exceeds available stock")

    # ================= RATE =================
    items_df = pd.read_csv(STORE_ITEM_FILE)
    row = items_df[items_df["Item Code"] == item_code]

    rm_rate = float(row.iloc[0].get("RM Rate", 0)) if not row.empty else 0
    value = qty * rm_rate

    mask = ledger_df["Timestamp"] == old_timestamp

    ledger_df.loc[mask, "Date"] = date
    ledger_df.loc[mask, "Item"] = item_code
    ledger_df.loc[mask, "Qty"] = qty
    ledger_df.loc[mask, "Rate"] = rm_rate
    ledger_df.loc[mask, "Value"] = value
    ledger_df.loc[mask, "Remarks"] = f"{purpose} | Issued By: {issued_by} | {remarks}"

    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/issue")

# =====================================================
# RETURN TO STORES PAGE
# =====================================================
@app.route("/stores/return", methods=["GET"])
def stores_return():

    # ---------- LOAD ITEM MASTER ----------
    items_df = pd.read_csv(STORE_ITEM_FILE)

    # ---------- ALL ITEM CODES ----------
    items_df["Item Code"] = items_df["Item Code"].astype(str).str.strip()

    items = sorted(
        items_df["Item Code"]
        .dropna()
        .loc[items_df["Item Code"] != ""]
        .unique()
        .tolist()
    )

    # ---------- PRODUCTION ITEM CODES ONLY ----------
    production_items = sorted(
        items_df[
            items_df["Category"].astype(str).str.strip().str.lower() == "production item"
        ]["Item Code"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda x: x != ""]
        .unique()
        .tolist()
    )

    # ---------- RETURN TYPES ----------
    return_types = [
        "Finished Goods Return",
        "Rework Return",
        "Casting Rejection",
        "Machining Rejection",
        "Other Return"
    ]

    # ---------- LOAD LEDGER ----------
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    if not ledger_df.empty:
        ledger_df = ledger_df.sort_values("Timestamp", ascending=False)

    # ---------- ONLY RETURN ENTRIES ----------
    return_records = ledger_df[
        ledger_df["Inward_Type"].isin([
            "RETURN_RM",
            "RETURN_FG",
            "RETURN_REJECT"
        ])
    ]

    return render_template(
        "stores_return.html",
        items=items,
        return_types=return_types,
        records=return_records.to_dict(orient="records"),

        # 🔴 REQUIRED FOR JS DROPDOWNS
        all_items=items,
        production_items=production_items
    )

# =====================================================
# SAVE RETURN ENTRY
# =====================================================
@app.route("/stores/save_return", methods=["POST"])
def save_return():

    from datetime import datetime

    date = request.form.get("date")
    item_code = request.form.get("item")
    qty = request.form.get("qty")
    rtype = request.form.get("rtype")
    received_by = request.form.get("received_by")
    remarks = request.form.get("remarks", "")

    if not all([date, item_code, qty, rtype, received_by]):
        return redirect("/stores/return")

    qty = float(qty)

    items_df = pd.read_csv(STORE_ITEM_FILE)
    row = items_df[items_df["Item Code"] == item_code]

    if row.empty:
        return redirect("/stores/return")

    rm_rate = float(row["RM Rate"].values[0] or 0)
    fg_rate = float(row["FG Rate"].values[0] or rm_rate)

    # ---------------- BUCKET DECISION ----------------
    if rtype == "Finished Goods Return":
        inward_type = "RETURN_FG"
        rate = fg_rate

    elif rtype in ["Casting Rejection", "Machining Rejection"]:
        inward_type = "RETURN_REJECT"
        rate = rm_rate

    else:
        inward_type = "RETURN_RM"
        rate = rm_rate

    value = qty * rate

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    new_row = {
        "Date": date,
        "Item": item_code,
        "Inward_Type": inward_type,
        "Qty": qty,
        "Rate": rate,
        "Value": value,
        "Supplier": "",
        "Ref_No": "",
        "Remarks": f"{rtype} | Received By: {received_by} | {remarks}",
        "User": "system",
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    ledger_df = pd.concat(
        [ledger_df, pd.DataFrame([new_row])],
        ignore_index=True
    )

    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/return")

# =====================================================
# DELETE RETURN ENTRY
# =====================================================
@app.route("/stores/delete_return", methods=["POST"])
def delete_return():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    timestamp = request.form.get("timestamp")

    df = pd.read_csv(STORE_LEDGER_FILE)

    before = len(df)
    df = df[df["Timestamp"] != timestamp]
    after = len(df)

    if before == after:
        return "NOT_FOUND", 404

    df.to_csv(STORE_LEDGER_FILE, index=False)
    backup_to_drive()
    return "OK", 200


# =====================================================
# EDIT RETURN ENTRY (FIXED FOR ITEM CODE SYSTEM)
# =====================================================
@app.route("/stores/edit_return", methods=["POST"])
def edit_return():

    code = request.form.get("code", "")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE", 403

    old_timestamp = request.form.get("old_timestamp")

    date = request.form.get("date")
    item_code = request.form.get("item")
    qty = float(request.form.get("qty"))
    rtype = request.form.get("rtype")
    received_by = request.form.get("received_by")
    remarks = request.form.get("remarks", "")

    # ---------- LOAD ITEM MASTER ----------
    items_df = pd.read_csv(STORE_ITEM_FILE)
    row = items_df[items_df["Item Code"] == item_code]

    if row.empty:
        return redirect("/stores/return")

    rm_rate = float(row["RM Rate"].values[0] or 0)
    fg_rate = float(row["FG Rate"].values[0] or rm_rate)

    # ---------- BUCKET ----------
    if rtype == "Finished Goods Return":
        inward_type = "RETURN_FG"
        rate = fg_rate

    elif rtype in ["Casting Rejection", "Machining Rejection"]:
        inward_type = "RETURN_REJECT"
        rate = rm_rate

    else:
        inward_type = "RETURN_RM"
        rate = rm_rate

    value = qty * rate

    df = pd.read_csv(STORE_LEDGER_FILE)

    mask = df["Timestamp"] == old_timestamp

    df.loc[mask, "Date"] = date
    df.loc[mask, "Item"] = item_code
    df.loc[mask, "Inward_Type"] = inward_type
    df.loc[mask, "Qty"] = qty
    df.loc[mask, "Rate"] = rate
    df.loc[mask, "Value"] = value
    df.loc[mask, "Remarks"] = f"{rtype} | Received By: {received_by} | {remarks}"

    df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/return")

# =====================================================
# MATERIAL OUTWARD PAGE
# =====================================================
@app.route("/stores/outward", methods=["GET"])
def stores_outward():

    items_df = pd.read_csv(STORE_ITEM_FILE)

    # all item codes
    items_df["Item Code"] = items_df["Item Code"].astype(str).str.strip()

    items = sorted(
        items_df["Item Code"]
        .dropna()
        .loc[items_df["Item Code"] != ""]
        .unique()
        .tolist()
    )

    outward_types = [
        "Customer Dispatch",
        "Sample Dispatch",
        "Return to Vendor",
        "Job Work Out",
        "Scrap Sale"
    ]

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    if not ledger_df.empty:
        ledger_df = ledger_df.sort_values("Timestamp", ascending=False)

    outward_records = ledger_df[
        ledger_df["Inward_Type"].isin([
            "OUTWARD_FG",
            "OUTWARD_RM",
            "OUTWARD_REJECT"
        ])
    ]

    return render_template(
        "stores_outward.html",
        items=items,
        outward_types=outward_types,
        records=outward_records.to_dict(orient="records")
    )

# =====================================================
# SAVE OUTWARD ENTRY
# =====================================================
@app.route("/stores/save_outward", methods=["POST"])
def save_outward():

    from datetime import datetime

    date = request.form.get("date")
    item_code = request.form.get("item")
    qty = request.form.get("qty")
    otype = request.form.get("otype")
    party = request.form.get("party")
    sent_by = request.form.get("sent_by")
    remarks = request.form.get("remarks","")

    if not all([date, item_code, qty, otype, party, sent_by]):
        return redirect("/stores/outward")

    qty = float(qty)

    # ---------- LOAD DATA ----------
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)
    items_df = pd.read_csv(STORE_ITEM_FILE)

    row = items_df[items_df["Item Code"] == item_code]
    if row.empty:
        return redirect("/stores/outward")

    rm_rate = float(row["RM Rate"].values[0] or 0)
    fg_rate = float(row["FG Rate"].values[0] or rm_rate)

    # ---------- CURRENT STOCK CALCULATION ----------
    item_ledger = ledger_df[ledger_df["Item"] == item_code]

    rm_in = item_ledger[
        item_ledger["Inward_Type"].isin(["INWARD","RETURN_RM"])
    ]["Qty"].sum()

    rm_out = item_ledger[
        item_ledger["Inward_Type"].isin(["ISSUE","OUTWARD_RM"])
    ]["Qty"].sum()

    rm_stock = rm_in - rm_out

    fg_in = item_ledger[
        item_ledger["Inward_Type"] == "RETURN_FG"
    ]["Qty"].sum()

    fg_out = item_ledger[
        item_ledger["Inward_Type"] == "OUTWARD_FG"
    ]["Qty"].sum()

    fg_stock = fg_in - fg_out

    rej_in = item_ledger[
        item_ledger["Inward_Type"] == "RETURN_REJECT"
    ]["Qty"].sum()

    rej_out = item_ledger[
        item_ledger["Inward_Type"] == "OUTWARD_REJECT"
    ]["Qty"].sum()

    rej_stock = rej_in - rej_out

    # ---------- TYPE LOGIC ----------
    if otype in ["Customer Dispatch","Sample Dispatch"]:
        inward_type = "OUTWARD_FG"
        rate = fg_rate

        if qty > fg_stock:
            return redirect(url_for("stores_outward",
                   error=f"FG stock only {round(fg_stock,2)}"))

    elif otype == "Job Work Out":
        inward_type = "OUTWARD_RM"
        rate = rm_rate

        if qty > rm_stock:
            return redirect(url_for("stores_outward",
                   error=f"RM stock only {round(rm_stock,2)}"))

    else:  # reject outward
        inward_type = "OUTWARD_REJECT"
        rate = rm_rate

        if qty > rej_stock:
            return redirect(url_for("stores_outward",
                   error=f"Reject stock only {round(rej_stock,2)}"))

    value = qty * rate

    # ---------- OUTWARD NUMBER ----------
    if "Ref_No" in ledger_df.columns:
        ledger_df["Ref_No"] = ledger_df["Ref_No"].astype(str)
        last = ledger_df[ledger_df["Ref_No"].str.startswith("OUT", na=False)]
        if last.empty:
            next_no = 1
        else:
            last_no = (
                last["Ref_No"]
                .str.replace("OUT-","")
                .astype(int)
                .max()
            )
            next_no = last_no + 1
    else:
        next_no = 1

    outward_no = f"OUT-{str(next_no).zfill(5)}"

    # ---------- SAVE ----------
    new_row = {
        "Date": date,
        "Item": item_code,
        "Inward_Type": inward_type,
        "Qty": qty,
        "Rate": rate,
        "Value": value,
        "Supplier": party,
        "Ref_No": outward_no,
        "Remarks": f"{otype} | Sent By: {sent_by} | {remarks}",
        "User": "system",
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    ledger_df = pd.concat(
        [ledger_df, pd.DataFrame([new_row])],
        ignore_index=True
    )

    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/outward")

# =====================================================
# DELETE OUTWARD ENTRY
# =====================================================
@app.route("/stores/delete_outward", methods=["POST"])
def delete_outward():

    code = request.form.get("code","")
    if code != STORES_DELETE_CODE:
        return "INVALID_CODE",403

    ts = request.form.get("timestamp")

    df = pd.read_csv(STORE_LEDGER_FILE)
    df = df[df["Timestamp"] != ts]
    df.to_csv(STORE_LEDGER_FILE,index=False)

    backup_to_drive()

    return "OK",200

# =====================================================
# STOCK RECONCILIATION PAGE
# =====================================================
@app.route("/stores/reconcile", methods=["GET"])
def stores_reconcile():

    items_df = pd.read_csv(STORE_ITEM_FILE)

    item_codes = sorted(
        items_df["Item Code"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    recon = ledger_df[
        ledger_df["Ref_No"].astype(str).str.contains("RECON", na=False)
    ]

    if not recon.empty:
        recon = recon.sort_values("Timestamp", ascending=False).head(100)

    return render_template(
        "stores_reconcile.html",
        items=item_codes,
        records=recon.to_dict(orient="records")
    )

# =====================================================
# HELPER — GET CURRENT SYSTEM STOCK
# =====================================================
def get_current_stock(item_code):

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)
    ledger_df["Qty"] = pd.to_numeric(ledger_df["Qty"], errors="coerce").fillna(0)

    item_ledger = ledger_df[
        ledger_df["Item"].astype(str).str.strip() == str(item_code).strip()
    ]

    inward_rm = item_ledger[item_ledger["Inward_Type"]=="INWARD"]["Qty"].sum()
    issued = item_ledger[item_ledger["Inward_Type"]=="ISSUE"]["Qty"].sum()

    return_rm = item_ledger[item_ledger["Inward_Type"]=="RETURN_RM"]["Qty"].sum()
    return_fg = item_ledger[item_ledger["Inward_Type"]=="RETURN_FG"]["Qty"].sum()
    return_reject = item_ledger[item_ledger["Inward_Type"]=="RETURN_REJECT"]["Qty"].sum()

    outward_rm = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_RM"]["Qty"].sum()
    outward_fg = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_FG"]["Qty"].sum()
    outward_reject = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_REJECT"]["Qty"].sum()
    outward_wip = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_WIP"]["Qty"].sum()

    rm = inward_rm - issued + return_rm - outward_rm
    wip = issued - return_fg - return_reject - outward_wip
    fg = return_fg - outward_fg
    reject = return_reject - outward_reject

    return {
        "RM": max(rm,0),
        "WIP": max(wip,0),
        "FG": max(fg,0),
        "REJECT": max(reject,0)
    }

# =====================================================
# SAVE MANUAL RECONCILIATION
# =====================================================
@app.route("/stores/save_reconcile", methods=["POST"])
def save_reconcile():

    from datetime import datetime

    date = request.form.get("date")
    item = request.form.get("item")
    physical_qty = float(request.form.get("qty",0))
    stock_type = request.form.get("stock_type")  # RM/WIP/FG/REJECT/OPENING
    remarks = request.form.get("remarks","")

    if not item or not stock_type:
        return redirect("/stores/reconcile")

    items_df = pd.read_csv(STORE_ITEM_FILE)
    row = items_df[items_df["Item Code"].astype(str).str.strip()==item]

    rm_rate = float(row.iloc[0].get("RM Rate",0) if not row.empty else 0)
    fg_rate = float(row.iloc[0].get("FG Rate",rm_rate) if not row.empty else rm_rate)

    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    # ---------- OPENING STOCK ----------
    if stock_type == "OPENING":

        new_row = {
            "Date": date,
            "Item": item,
            "Inward_Type": "INWARD",
            "Qty": physical_qty,
            "Rate": rm_rate,
            "Value": physical_qty*rm_rate,
            "Supplier": "",
            "Ref_No": "RECON_OPENING",
            "Remarks": f"Opening Stock | {remarks}",
            "User": "system",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        ledger_df = pd.concat([ledger_df, pd.DataFrame([new_row])], ignore_index=True)
        ledger_df.to_csv(STORE_LEDGER_FILE, index=False)
        backup_to_drive()
        return redirect("/stores/reconcile")

    # ---------- NORMAL RECON ----------
    current = get_current_stock(item)
    system_qty = current.get(stock_type,0)

    diff = physical_qty - system_qty
    if diff == 0:
        return redirect("/stores/reconcile")

    # ---------- TYPE MAP ----------
    if stock_type == "RM":
        inward_type = "RETURN_RM" if diff > 0 else "OUTWARD_RM"
        rate = rm_rate

    elif stock_type == "WIP":
        inward_type = "ISSUE" if diff > 0 else "OUTWARD_WIP"
        rate = rm_rate

    elif stock_type == "FG":
        inward_type = "RETURN_FG" if diff > 0 else "OUTWARD_FG"
        rate = fg_rate

    elif stock_type == "REJECT":
        inward_type = "RETURN_REJECT" if diff > 0 else "OUTWARD_REJECT"
        rate = rm_rate

    # block negative
    if system_qty + diff < 0:
        return "❌ Cannot reconcile. Stock will go negative."

    new_row = {
        "Date": date,
        "Item": item,
        "Inward_Type": inward_type,
        "Qty": abs(diff),
        "Rate": rate,
        "Value": abs(diff)*rate,
        "Supplier": "",
        "Ref_No": "RECON",
        "Remarks": f"Stock Recon | {stock_type} | Physical:{physical_qty} | System:{system_qty} | {remarks}",
        "User": "system",
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    ledger_df = pd.concat([ledger_df, pd.DataFrame([new_row])], ignore_index=True)
    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)
    backup_to_drive()
    return redirect("/stores/reconcile")

# =====================================================
# EXCEL STOCK UPLOAD (DIRECT ADD — FINAL STABLE)
# =====================================================
@app.route("/stores/upload_reconcile_excel", methods=["POST"])
def upload_reconcile_excel():

    from datetime import datetime
    import pandas as pd

    file = request.files.get("file")
    if not file:
        return redirect("/stores/reconcile")

    df = pd.read_excel(file)
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    today = datetime.today().strftime("%Y-%m-%d")

    for idx, r in df.iterrows():

        item = str(r.get("Item Code","")).strip()
        if item == "" or item.lower() == "nan":
            continue

        rm = float(r.get("RM Stock",0) or 0)
        wip = float(r.get("WIP Stock",0) or 0)
        fg = float(r.get("FG Stock",0) or 0)
        reject = float(r.get("Reject Stock",0) or 0)
        opening = float(r.get("Opening Stock",0) or 0)
        remarks = str(r.get("Remarks",""))

        # unique timestamp per row
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # ---------- RM ----------
        if rm != 0:
            ledger_df.loc[len(ledger_df)] = [
                today,item,"ADJ_RM",rm,0,0,"","EXCEL_RM",
                f"Excel Upload RM | {remarks}",
                "system",ts
            ]

        # ---------- WIP ----------
        if wip != 0:
            ledger_df.loc[len(ledger_df)] = [
                today,item,"ADJ_WIP",wip,0,0,"","EXCEL_WIP",
                f"Excel Upload WIP | {remarks}",
                "system",ts
            ]

        # ---------- FG ----------
        if fg != 0:
            ledger_df.loc[len(ledger_df)] = [
                today,item,"ADJ_FG",fg,0,0,"","EXCEL_FG",
                f"Excel Upload FG | {remarks}",
                "system",ts
            ]

        # ---------- REJECT ----------
        if reject != 0:
            ledger_df.loc[len(ledger_df)] = [
                today,item,"ADJ_REJECT",reject,0,0,"","EXCEL_REJ",
                f"Excel Upload Reject | {remarks}",
                "system",ts
            ]

        # ---------- OPENING ----------
        if opening != 0:
            ledger_df.loc[len(ledger_df)] = [
                today,item,"OPENING",opening,0,0,"","EXCEL_OPEN",
                f"Excel Opening | {remarks}",
                "system",ts
            ]

    ledger_df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return redirect("/stores/reconcile")

# =====================================================
# DELETE RECON ENTRY
# =====================================================
@app.route("/stores/delete_reconcile", methods=["POST"])
def delete_reconcile():

    code = request.form.get("code","")
    if code != STORES_DELETE_CODE:
        return "INVALID",403

    ts = request.form.get("timestamp")

    df = pd.read_csv(STORE_LEDGER_FILE)
    df = df[df["Timestamp"] != ts]
    df.to_csv(STORE_LEDGER_FILE, index=False)

    backup_to_drive()

    return "OK",200

# =====================================================
# LIVE INVENTORY ENGINE (FINAL — WITH RECON SUPPORT)
# =====================================================
@app.route("/stores/inventory", methods=["GET"])
def stores_inventory():

    items_df = pd.read_csv(STORE_ITEM_FILE)
    ledger_df = pd.read_csv(STORE_LEDGER_FILE)

    if ledger_df.empty or items_df.empty:
        return render_template(
            "stores_inventory.html",
            records=[],
            total_value=0,
            total_items=0,
            low_stock=0
        )

    ledger_df["Qty"] = pd.to_numeric(ledger_df["Qty"], errors="coerce").fillna(0)
    ledger_df["Item"] = ledger_df["Item"].astype(str).str.strip()

    summary = []

    for _, row in items_df.iterrows():

        code = str(row["Item Code"]).strip()
        category = str(row.get("Category",""))

        rm_rate = float(row.get("RM Rate",0) or 0)
        fg_rate = float(row.get("FG Rate",rm_rate) or rm_rate)
        min_stock = float(row.get("Min Stock",0) or 0)

        item_ledger = ledger_df[ledger_df["Item"] == code]

        # ================= BASE =================
        inward_rm = item_ledger[item_ledger["Inward_Type"]=="INWARD"]["Qty"].sum()
        issued = item_ledger[item_ledger["Inward_Type"]=="ISSUE"]["Qty"].sum()

        return_rm = item_ledger[item_ledger["Inward_Type"]=="RETURN_RM"]["Qty"].sum()
        return_fg = item_ledger[item_ledger["Inward_Type"]=="RETURN_FG"]["Qty"].sum()
        return_reject = item_ledger[item_ledger["Inward_Type"]=="RETURN_REJECT"]["Qty"].sum()

        outward_rm = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_RM"]["Qty"].sum()
        outward_fg = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_FG"]["Qty"].sum()
        outward_reject = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_REJECT"]["Qty"].sum()
        outward_wip = item_ledger[item_ledger["Inward_Type"]=="OUTWARD_WIP"]["Qty"].sum()

        # ================= RECON ADDITIONS =================
        adj_rm = item_ledger[item_ledger["Inward_Type"]=="ADJ_RM"]["Qty"].sum()
        adj_wip = item_ledger[item_ledger["Inward_Type"]=="ADJ_WIP"]["Qty"].sum()
        adj_fg = item_ledger[item_ledger["Inward_Type"]=="ADJ_FG"]["Qty"].sum()
        adj_reject = item_ledger[item_ledger["Inward_Type"]=="ADJ_REJECT"]["Qty"].sum()
        opening = item_ledger[item_ledger["Inward_Type"]=="OPENING"]["Qty"].sum()

        # ================= STOCK =================
        rm_stock = inward_rm - issued + return_rm - outward_rm + adj_rm + opening
        wip_stock = issued - return_fg - return_reject - outward_wip + adj_wip
        fg_stock = return_fg - outward_fg + adj_fg
        reject_stock = return_reject - outward_reject + adj_reject

        rm_stock = max(rm_stock,0)
        wip_stock = max(wip_stock,0)
        fg_stock = max(fg_stock,0)
        reject_stock = max(reject_stock,0)

        total_stock = rm_stock + wip_stock + fg_stock + reject_stock

        # ================= VALUE =================
        rm_value = rm_stock * rm_rate
        wip_value = wip_stock * (fg_rate * 0.75)
        fg_value = fg_stock * fg_rate
        reject_value = reject_stock * rm_rate

        total_value_item = rm_value + wip_value + fg_value + reject_value

        summary.append({
            "Item Code": code,
            "Category": category,
            "RM Stock": round(rm_stock,2),
            "WIP Stock": round(wip_stock,2),
            "FG Stock": round(fg_stock,2),
            "Reject Stock": round(reject_stock,2),
            "Total Stock": round(total_stock,2),
            "Total Value": round(total_value_item,2),
            "Min": min_stock,
            "Low": rm_stock <= min_stock if min_stock>0 else False
        })

    df = pd.DataFrame(summary)

    total_value = df["Total Value"].sum()
    total_items = len(df)
    low_stock = len(df[df["Low"]==True])

    df = df.sort_values("Item Code")

    return render_template(
        "stores_inventory.html",
        records=df.to_dict(orient="records"),
        total_value=round(total_value,2),
        total_items=total_items,
        low_stock=low_stock
    )

# =========================================================
# SHOPFLOOR TV DASHBOARD (FINAL ROTATING SYSTEM)
# =========================================================
@app.route("/shopfloor_tv")
def shopfloor_tv():

    # ---------- LOAD DATA ----------
    main_df = load_csv("data/production_main.csv")
    other_df = load_csv("data/production_other_machine.csv")
    part_df = load_csv("data/part_master.csv")

    prod_df = pd.concat([main_df, other_df], ignore_index=True)

    if prod_df.empty:
        return "<h2>No production data available</h2>"

    # ---------- NORMALIZE ----------
    prod_df["Date"] = pd.to_datetime(prod_df["Date"], errors="coerce")
    prod_df["Time_Min"] = pd.to_numeric(prod_df["Time_Min"], errors="coerce").fillna(0)
    prod_df["Qty"] = pd.to_numeric(prod_df["Qty"], errors="coerce").fillna(0)
    prod_df["Mach_Rej"] = pd.to_numeric(prod_df.get("Mach_Rej", 0), errors="coerce").fillna(0)
    prod_df["Cast_Rej"] = pd.to_numeric(prod_df.get("Cast_Rej", 0), errors="coerce").fillna(0)
    prod_df["Good_Qty"] = pd.to_numeric(prod_df["Good_Qty"], errors="coerce").fillna(0)

    # ---------- LAST PRODUCTION DAY ----------
    last_date = prod_df["Date"].max()
    day_df = prod_df[prod_df["Date"] == last_date].copy()
    last_date_str = last_date.strftime("%d-%m-%Y")

    # ---------- MERGE CYCLE TIME ----------
    part_df = part_df.rename(columns={
        "Part Number": "Part",
        "Operation No": "Operation"
    })

    part_df["Cycle Time (min)"] = pd.to_numeric(
        part_df["Cycle Time (min)"], errors="coerce"
    ).fillna(0)

    day_df = day_df.merge(
        part_df[["Part", "Operation", "Cycle Time (min)"]],
        on=["Part", "Operation"],
        how="left"
    )

    # =========================================================
    # MACHINE OEE CALCULATION
    # =========================================================
    machines = []

    for machine in sorted(day_df["Machine"].dropna().unique()):

        mdf = day_df[day_df["Machine"] == machine]

        time_spent = mdf["Time_Min"].sum()
        produced = mdf["Qty"].sum()
        good = mdf["Good_Qty"].sum()

        expected = (
            mdf["Time_Min"] / mdf["Cycle Time (min)"]
        ).replace([float("inf"), -float("inf")], 0).sum()

        available_time = (
            570 if (mdf["OT"].astype(str).str.lower() == "yes").any() else 480
        ) * mdf["Shift"].nunique()

        availability = (time_spent / available_time * 100) if available_time else 0
        performance = (produced / expected * 100) if expected else 0
        quality = (good / produced * 100) if produced else 0
        oee = availability * performance * quality / 10000

        machines.append({
            "machine": machine,
            "availability": round(availability, 2),
            "performance": round(performance, 2),
            "quality": round(quality, 2),
            "oee": round(oee, 2)
        })

    # =========================================================
    # OPERATOR PRODUCTIVITY
    # =========================================================
    operator_data = []

    for op in day_df["Operator"].dropna().unique():

        odf = day_df[day_df["Operator"] == op]

        good = odf["Good_Qty"].sum()
        expected = (
            odf["Time_Min"] / odf["Cycle Time (min)"]
        ).replace([float("inf"), -float("inf")], 0).sum()

        prod_pct = (good / expected * 100) if expected else 0

        operator_data.append({
            "name": op,
            "value": round(prod_pct, 2)
        })

    operator_data = sorted(operator_data, key=lambda x: x["value"], reverse=True)

    best_operator = operator_data[0] if operator_data else None
    lowest_operator = operator_data[-1] if operator_data else None

    plant_productivity = round(
        sum([o["value"] for o in operator_data]) / len(operator_data), 2
    ) if operator_data else 0

    # =========================================================
    # REJECTION LOGIC
    # =========================================================
    total_mach_rej = int(day_df["Mach_Rej"].sum())
    total_cast_rej = int(day_df["Cast_Rej"].sum())
    total_rejection = total_mach_rej + total_cast_rej

    worst_operator = None
    if total_mach_rej > 0:
        op_rej = (
            day_df.groupby("Operator")["Mach_Rej"]
            .sum()
            .reset_index()
            .sort_values("Mach_Rej", ascending=False)
        )
        if not op_rej.empty:
            r = op_rej.iloc[0]
            worst_operator = {
                "name": r["Operator"],
                "value": int(r["Mach_Rej"])
            }

    # =========================================================
    # OPERATOR MACHINING REJECTION (FOR QUALITY SCREEN)
    # =========================================================
    operator_rejection = (
        day_df.groupby("Operator", as_index=False)["Mach_Rej"]
        .sum()
    )

    operator_rejection = operator_rejection[
        operator_rejection["Mach_Rej"] > 0
    ].sort_values("Mach_Rej", ascending=False)

    # =========================================================
    # QUALITY SCREEN DATA
    # =========================================================
    part_rej = (
        day_df.groupby("Part", as_index=False)
        .agg({
            "Cast_Rej": "sum",
            "Mach_Rej": "sum"
        })
    )

    part_rej["Total"] = part_rej["Cast_Rej"] + part_rej["Mach_Rej"]
    part_rej = part_rej[part_rej["Total"] > 0]
    part_rej = part_rej.sort_values("Total", ascending=False).head(5)

    # =========================================================
    # MACHINE RANKING
    # =========================================================
    avg_oee = round(sum([m["oee"] for m in machines]) / len(machines), 2) if machines else 0
    best_machine = max(machines, key=lambda x: x["oee"]) if machines else None
    worst_machine = min(machines, key=lambda x: x["oee"]) if machines else None

    total_production = int(day_df["Good_Qty"].sum())

    return render_template(
        "shopfloor_tv_v2.html",
        last_date=last_date_str,
        machines=machines,
        total_production=total_production,
        total_rejection=total_rejection,
        avg_oee=avg_oee,
        plant_productivity=plant_productivity,
        best_machine=best_machine,
        worst_machine=worst_machine,
        best_operator=best_operator,
        lowest_operator=lowest_operator,
        worst_operator=worst_operator,
        part_rej=part_rej.to_dict(orient="records"),
        operators=operator_data,
        operator_rejection=operator_rejection.to_dict(orient="records")
    )

# =========================================
# AUTORESTORE DATA IF ABSENT
# =========================================

import os
import zipfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

def auto_restore_from_drive():
    print("🔵 Checking if data restore needed...")

    DATA_FOLDER = "data"
    KEY_FILE = "/etc/secrets/gdrive_key.json"
    FOLDER_NAME = "CATI_APP_BACKUP"

    # If data folder missing or empty → restore
    if not os.path.exists(DATA_FOLDER) or len(os.listdir(DATA_FOLDER)) == 0:

        print("⚠ Data folder empty. Starting auto-restore...")

        try:
            SCOPES = ['https://www.googleapis.com/auth/drive']
            creds = service_account.Credentials.from_service_account_file(
                KEY_FILE, scopes=SCOPES
            )
            service = build('drive', 'v3', credentials=creds)

            # Find backup folder
            results = service.files().list(
                q=f"name='{FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder'",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            folder_id = results.get('files', [])[0]['id']

            # Get latest backup file
            results = service.files().list(
                q=f"'{folder_id}' in parents and name contains 'backup_'",
                fields="files(id, name, createdTime)",
                orderBy="createdTime desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            files = results.get('files', [])
            if not files:
                print("❌ No backup files found in Drive")
                return

            latest_file = files[0]
            print("🟢 Restoring from:", latest_file["name"])

            request = service.files().get_media(fileId=latest_file['id'],
                                                supportsAllDrives=True)

            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)

            # Save zip temporarily
            temp_zip = "restore_temp.zip"
            with open(temp_zip, "wb") as f:
                f.write(fh.read())

            # Extract into data folder
            os.makedirs(DATA_FOLDER, exist_ok=True)

            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                zip_ref.extractall(DATA_FOLDER)

            os.remove(temp_zip)

            print("🟢 AUTO RESTORE COMPLETE")

        except Exception as e:
            print("🔴 AUTO RESTORE FAILED:", str(e))

    else:
        print("🟢 Data folder OK — no restore needed")

# =========================================
# MAIN
# =========================================

auto_restore_from_drive()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )