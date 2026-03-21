"""
Lutron Designer File Viewer / Editor
Supports .pl files (ZIP archive) and extracted folders.
"""

import os
import uuid
import sqlite3
import zipfile
import shutil
import tempfile
import threading
import webbrowser
import subprocess
import tkinter as tk
from tkinter import filedialog
from flask import Flask, jsonify, request, send_from_directory

try:
    import pyodbc
    PYODBC_OK = True
except ImportError:
    PYODBC_OK = False

app = Flask(__name__, static_folder="static", template_folder="static")

state = {
    "pl_path":       None,   # original .pl file path (None if folder was selected)
    "work_dir":      None,   # temp dir extracted from .pl, or the folder itself
    "db_name":       None,   # SQL Server temp DB name
    "lut_path":      None,   # path of extracted .lut
    "dirty":         False,  # unsaved changes exist
    "template_id":   None,   # active template ID (filename in Templates/)
    "tmpl_zip_name": None,   # same as template_id (kept for clarity in save)
}

SQL_INSTANCE = r".\LUTRON2022"


# ─────────────────────────────────────────────
# Helpers – SQL Server
# ─────────────────────────────────────────────

def sql_conn():
    for drv in ["ODBC Driver 17 for SQL Server", "SQL Server Native Client 11.0", "SQL Server"]:
        try:
            return pyodbc.connect(
                f"DRIVER={{{drv}}};SERVER={SQL_INSTANCE};DATABASE=master;Trusted_Connection=yes;",
                timeout=10
            )
        except pyodbc.Error:
            continue
    raise RuntimeError("SQL Server に接続できません")


def q(sql, params=()):
    """SELECT against current temp DB, return list of dicts."""
    conn = sql_conn()
    cur = conn.cursor()
    cur.execute(f"USE [{state['db_name']}]")
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows


def execute_sql(sql, params=()):
    """DML (INSERT/UPDATE/DELETE) against current temp DB."""
    conn = sql_conn()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(f"USE [{state['db_name']}]")
    cur.execute(sql, params)
    conn.commit()
    conn.close()
    state["dirty"] = True


def drop_db(db_name):
    try:
        drop_sql = (
            f"IF DB_ID(N'{db_name}') IS NOT NULL BEGIN "
            f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{db_name}]; END"
        )
        subprocess.run(["sqlcmd", "-S", SQL_INSTANCE, "-Q", drop_sql],
                       capture_output=True, timeout=30)
    except Exception:
        pass
    lut_dest = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.lut")
    try:
        if os.path.exists(lut_dest):
            os.remove(lut_dest)
    except OSError:
        pass


def restore_lut(lut_path, db_name):
    mdf = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.mdf")
    ldf = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}_log.ldf")
    lut_dest = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.lut")

    drop_sql = (
        f"IF DB_ID(N'{db_name}') IS NOT NULL BEGIN "
        f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
        f"DROP DATABASE [{db_name}]; END"
    )
    subprocess.run(["sqlcmd", "-S", SQL_INSTANCE, "-Q", drop_sql],
                   capture_output=True, timeout=30)

    if os.path.exists(lut_dest):
        try:
            os.remove(lut_dest)
        except OSError:
            lut_dest = os.path.join(r"C:\ProgramData\Lutron",
                                    f"{db_name}_{uuid.uuid4().hex[:8]}.lut")

    shutil.copy2(lut_path, lut_dest)

    sql = (
        f"RESTORE DATABASE [{db_name}] FROM DISK = N'{lut_dest}' WITH "
        f"MOVE N'Project' TO N'{mdf}', "
        f"MOVE N'Project_log' TO N'{ldf}', RECOVERY"
    )
    result = subprocess.run(["sqlcmd", "-S", SQL_INSTANCE, "-Q", sql],
                            capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(result.stdout or result.stderr)


# ─────────────────────────────────────────────
# Helpers – .pl / folder loading
# ─────────────────────────────────────────────

def sqlite_rows(path, query, params=()):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    cur = con.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def extract_pl_to_temp(pl_path):
    """Extract .pl (ZIP) to a fresh temp dir and return its path."""
    out = tempfile.mkdtemp(prefix="ld_pl_")
    with zipfile.ZipFile(pl_path) as z:
        z.extractall(out)
    return out


def list_template_zips(folder):
    """Return sorted list of template ZIP filenames found in Templates/."""
    tmpl_dir = os.path.join(folder, "Templates")
    if not os.path.isdir(tmpl_dir):
        return []
    result = []
    for name in sorted(os.listdir(tmpl_dir)):
        fp = os.path.join(tmpl_dir, name)
        if not os.path.isfile(fp):
            continue
        try:
            with zipfile.ZipFile(fp) as z:
                if any(zi.filename.endswith(".lut") for zi in z.infolist()):
                    result.append(name)
        except Exception:
            pass
    return result


def find_lut_for_template(folder, template_id=None):
    """
    Extract .lut for the given template_id (or first available).
    Returns (lut_path, tmpl_zip_name) or (None, None).
    """
    tmpl_dir = os.path.join(folder, "Templates")
    if not os.path.isdir(tmpl_dir):
        return None, None

    candidates = list_template_zips(folder)
    if not candidates:
        return None, None

    if template_id and template_id in candidates:
        candidates = [template_id] + [c for c in candidates if c != template_id]

    for name in candidates:
        fp = os.path.join(tmpl_dir, name)
        try:
            with zipfile.ZipFile(fp) as z:
                for zi in z.infolist():
                    if zi.filename.endswith(".lut"):
                        out_dir = tempfile.mkdtemp(prefix="ld_lut_")
                        z.extract(zi, out_dir)
                        return os.path.join(out_dir, zi.filename), name
        except Exception:
            pass
    return None, None


def load_project(work_dir, template_id=None):
    """Restore the SQL Server DB from work_dir and update state."""
    if state["db_name"]:
        drop_db(state["db_name"])
    state["db_name"] = None
    state["lut_path"] = None
    state["template_id"] = None
    state["tmpl_zip_name"] = None
    state["dirty"] = False

    lut, tmpl_zip_name = find_lut_for_template(work_dir, template_id)
    if lut and PYODBC_OK:
        db_name = "LDViewer_Temp"
        restore_lut(lut, db_name)
        state["db_name"] = db_name
        state["lut_path"] = lut
        state["template_id"] = tmpl_zip_name
        state["tmpl_zip_name"] = tmpl_zip_name


def backup_lut_from_db(db_name, dest_lut_path):
    """Backup the current SQL Server DB back to a .lut file."""
    sql = f"BACKUP DATABASE [{db_name}] TO DISK = N'{dest_lut_path}' WITH FORMAT, INIT"
    result = subprocess.run(["sqlcmd", "-S", SQL_INSTANCE, "-Q", sql],
                            capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(result.stdout or result.stderr)


def save_back_to_pl():
    """
    Backup current DB → .lut, repack into the Template ZIP,
    then repack everything back into the .pl file.
    """
    if not state["pl_path"] or not state["db_name"]:
        raise RuntimeError("保存対象がありません")
    if not state["tmpl_zip_name"]:
        raise RuntimeError("テンプレート情報がありません")

    work_dir = state["work_dir"]
    tmpl_dir = os.path.join(work_dir, "Templates")
    tmpl_zip_name = state["tmpl_zip_name"]
    tmpl_zip_path = os.path.join(tmpl_dir, tmpl_zip_name)

    # Find the inner .lut name from the template ZIP
    with zipfile.ZipFile(tmpl_zip_path) as z:
        lut_entry = next(zi.filename for zi in z.infolist() if zi.filename.endswith(".lut"))

    # Backup DB → C:\ProgramData\Lutron\ (SQL Server has write access here)
    lut_basename = os.path.basename(lut_entry)
    tmp_lut = os.path.join(r"C:\ProgramData\Lutron", lut_basename)
    if os.path.exists(tmp_lut):
        os.remove(tmp_lut)

    app.logger.info(f"[save] Backing up DB to {tmp_lut}")
    backup_lut_from_db(state["db_name"], tmp_lut)

    if not os.path.exists(tmp_lut):
        raise RuntimeError(f"バックアップ失敗: {tmp_lut} が作成されませんでした")
    app.logger.info(f"[save] Backup size: {os.path.getsize(tmp_lut):,} bytes")

    # Repack: overwrite the template ZIP with new .lut
    tmp_zip = tmpl_zip_path + ".new"
    app.logger.info(f"[save] Repacking template ZIP")
    with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zout:
        zout.write(tmp_lut, lut_entry)
    os.replace(tmp_zip, tmpl_zip_path)
    app.logger.info(f"[save] Template ZIP updated: {os.path.getsize(tmpl_zip_path):,} bytes")

    # Write new .pl to a LOCAL temp directory first, then copy to network drive
    pl_path = state["pl_path"]
    tmp_pl_dir = tempfile.mkdtemp(prefix="ld_save_")
    tmp_pl = os.path.join(tmp_pl_dir, os.path.basename(pl_path))
    try:
        app.logger.info(f"[save] Building new .pl at {tmp_pl}")
        with zipfile.ZipFile(tmp_pl, "w", zipfile.ZIP_DEFLATED) as zout:
            for root, dirs, files in os.walk(work_dir):
                for fname in files:
                    abs_path = os.path.join(root, fname)
                    arc_name = os.path.relpath(abs_path, work_dir).replace("\\", "/")
                    zout.write(abs_path, arc_name)
            # Preserve empty Systems/ directory entry
            sys_dir = os.path.join(work_dir, "Systems")
            if os.path.isdir(sys_dir) and not os.listdir(sys_dir):
                if hasattr(zout, "mkdir"):
                    zout.mkdir("Systems/")

        app.logger.info(f"[save] New .pl size: {os.path.getsize(tmp_pl):,} bytes")
        app.logger.info(f"[save] Copying to {pl_path}")
        shutil.copy2(tmp_pl, pl_path)
        app.logger.info(f"[save] Done. Final .pl size: {os.path.getsize(pl_path):,} bytes")
    finally:
        shutil.rmtree(tmp_pl_dir, ignore_errors=True)

    state["dirty"] = False


# ─────────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/open", methods=["POST"])
def open_file():
    """Open a .pl file or a folder via file dialog."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    mode = request.json.get("mode", "pl")  # "pl" or "folder"

    if mode == "pl":
        path = filedialog.askopenfilename(
            title="LDプロジェクトファイル (.pl) を選択",
            filetypes=[("Lutron Project", "*.pl"), ("All files", "*.*")]
        )
        root.destroy()
        if not path:
            return jsonify({"error": "キャンセルされました"}), 400

        if state["work_dir"] and state["pl_path"]:
            shutil.rmtree(state["work_dir"], ignore_errors=True)

        work_dir = extract_pl_to_temp(path)
        state["pl_path"] = path
        state["work_dir"] = work_dir

    else:  # folder
        path = filedialog.askdirectory(title="LDプロジェクトフォルダを選択")
        root.destroy()
        if not path:
            return jsonify({"error": "キャンセルされました"}), 400
        state["pl_path"] = None
        state["work_dir"] = path

    sqlite_path = os.path.join(state["work_dir"], "PlaceCache.sqlite")
    if not os.path.exists(sqlite_path):
        return jsonify({"error": "PlaceCache.sqlite が見つかりません"}), 400

    try:
        load_project(state["work_dir"])
    except Exception as e:
        return jsonify({"error": f"SQL Server復元失敗: {e}"}), 500

    templates = list_template_zips(state["work_dir"])

    return jsonify({
        "path": path,
        "name": os.path.basename(path),
        "is_pl": mode == "pl",
        "has_template": state["db_name"] is not None,
        "templates": templates,
        "active_template": state["template_id"],
    })


@app.route("/api/switch-template", methods=["POST"])
def switch_template():
    """Switch to a different template and reload the DB."""
    if not state["work_dir"]:
        return jsonify({"error": "未選択"}), 400
    template_id = request.json.get("template_id")
    if not template_id:
        return jsonify({"error": "template_id 必須"}), 400
    try:
        load_project(state["work_dir"], template_id)
    except Exception as e:
        return jsonify({"error": f"テンプレート切替失敗: {e}"}), 500
    return jsonify({"ok": True, "active_template": state["template_id"]})


@app.route("/api/overview")
def overview():
    if not state["work_dir"]:
        return jsonify({"error": "未選択"}), 400
    sqlite_path = os.path.join(state["work_dir"], "PlaceCache.sqlite")
    place = sqlite_rows(sqlite_path, "SELECT * FROM tblPlace LIMIT 1")
    templates = sqlite_rows(sqlite_path, "SELECT id, name, modifiedDate, systemCount, projectDatabaseArtifactId FROM tblTemplate")
    systems = sqlite_rows(sqlite_path, "SELECT * FROM tblTemplatedSystem")
    addr = sqlite_rows(sqlite_path, "SELECT * FROM tblAddressDetails LIMIT 1")
    fs_templates = list_template_zips(state["work_dir"])
    return jsonify({
        "place": place[0] if place else {},
        "templates": templates,
        "fs_templates": fs_templates,
        "active_template": state["template_id"],
        "systems": systems,
        "address": addr[0] if addr else {},
        "dirty": state["dirty"],
        "is_pl": state["pl_path"] is not None,
    })


@app.route("/api/areas")
def areas():
    if not state["db_name"]:
        return jsonify([])
    rows = q("""
        SELECT AreaID, Name, ParentID, HierarchyLevel, IsLeaf, AreaType
        FROM tblArea
        WHERE HierarchyLevel >= 2
        ORDER BY HierarchyLevel, ParentID, SortOrder, Name
    """)
    return jsonify(rows)


@app.route("/api/area/<int:area_id>")
def area_detail(area_id):
    if not state["db_name"]:
        return jsonify({})

    zones = q("""
        SELECT z.ZoneID, z.Name, z.ZoneNumber as LoadNumber,
               z.ControlType, z.ZoneLayer, z.ZoneDescription
        FROM tblZone z
        WHERE z.ParentID = ?
        ORDER BY z.ZoneNumber
    """, (area_id,))

    stations = q("""
        SELECT cs.ControlStationID, cs.Name,
               csd.ControlStationDeviceID, csd.Name as DeviceName, csd.ModelInfoID
        FROM tblControlStation cs
        LEFT JOIN tblControlStationDevice csd ON csd.ParentControlStationID = cs.ControlStationID
        WHERE cs.ParentId = ? AND cs.ParentType = 2
        ORDER BY cs.SortOrder, cs.Name
    """, (area_id,))

    station_ids = list({s["ControlStationDeviceID"] for s in stations if s["ControlStationDeviceID"]})
    buttons = []
    if station_ids:
        ph = ",".join("?" * len(station_ids))
        buttons = q(f"""
            SELECT kb.ButtonID, kb.ButtonNumber, kb.Name as ButtonName,
                   kb.ParentDeviceID,
                   pm.Name as ProgModel, pm.ControlType as ProgControlType,
                   pm.PresetID
            FROM tblKeypadButton kb
            LEFT JOIN tblProgrammingModel pm ON kb.ProgrammingModelID = pm.ProgrammingModelID
            WHERE kb.ParentDeviceID IN ({ph})
            ORDER BY kb.ParentDeviceID, kb.ButtonNumber
        """, station_ids)

    scenes = q("""
        SELECT s.SceneID, s.Name, s.Number, sc.SceneControllerID
        FROM tblSceneController sc
        JOIN tblScene s ON s.ParentSceneControllerID = sc.SceneControllerID
        WHERE sc.ParentID = ? AND sc.ParentType = 2
        ORDER BY s.Number
    """, (area_id,))

    return jsonify({
        "zones": zones,
        "stations": stations,
        "buttons": buttons,
        "scenes": scenes,
    })


# ── Zone editing ──────────────────────────────

@app.route("/api/zone/<int:zone_id>", methods=["PATCH"])
def update_zone(zone_id):
    """Update zone name and/or description."""
    data = request.json
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400

    fields, params = [], []
    if "Name" in data:
        fields.append("Name = ?")
        params.append(data["Name"])
    if "ZoneDescription" in data:
        fields.append("ZoneDescription = ?")
        params.append(data["ZoneDescription"])
    if "ZoneLayer" in data:
        fields.append("ZoneLayer = ?")
        params.append(int(data["ZoneLayer"]))
    if not fields:
        return jsonify({"error": "変更なし"}), 400

    params.append(zone_id)
    execute_sql(f"UPDATE tblZone SET {', '.join(fields)} WHERE ZoneID = ?", params)
    return jsonify({"ok": True})


# ── Save back to .pl ──────────────────────────

@app.route("/api/save", methods=["POST"])
def save():
    if not state["pl_path"]:
        return jsonify({"error": "フォルダモードでは保存できません（.plファイルを開いてください）"}), 400
    if not state["dirty"]:
        return jsonify({"ok": True, "message": "変更なし"})
    try:
        save_back_to_pl()
        return jsonify({"ok": True})
    except Exception as e:
        app.logger.error(f"[save] Error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/shutdown", methods=["POST"])
def shutdown():
    if state["db_name"]:
        drop_db(state["db_name"])
    os.kill(os.getpid(), 9)
    return jsonify({"ok": True})


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def open_browser():
    import time
    time.sleep(1)
    webbrowser.open("http://127.0.0.1:5000")


if __name__ == "__main__":
    t = threading.Thread(target=open_browser, daemon=True)
    t.start()
    app.run(debug=False, port=5000)
