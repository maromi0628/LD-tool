"""
Lutron Designer File Viewer
Visualizes the contents of an LD project folder (PlaceCache.sqlite + .lut template)
"""

import os
import sys
import json
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

# ── Python 3.14 has pyodbc; use it for SQL Server
try:
    import pyodbc
    PYODBC_OK = True
except ImportError:
    PYODBC_OK = False

app = Flask(__name__, static_folder="static", template_folder="static")

# ── State shared between requests
state = {
    "folder": None,
    "db_name": None,   # temp SQL Server DB name
    "lut_path": None,
}

SQL_INSTANCE = r".\LUTRON2022"


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def sql_conn(db="master"):
    for driver in ["ODBC Driver 17 for SQL Server", "SQL Server Native Client 11.0", "SQL Server"]:
        try:
            return pyodbc.connect(
                f"DRIVER={{{driver}}};"
                f"SERVER={SQL_INSTANCE};DATABASE={db};Trusted_Connection=yes;",
                timeout=10
            )
        except pyodbc.Error:
            continue
    raise RuntimeError("SQL Server に接続できません")


def sqlite_rows(path, query, params=()):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    cur = con.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def find_lut(folder):
    """Return path to the first .lut extracted from Templates/"""
    tmpl_dir = os.path.join(folder, "Templates")
    if not os.path.isdir(tmpl_dir):
        return None
    for name in os.listdir(tmpl_dir):
        fp = os.path.join(tmpl_dir, name)
        if os.path.isfile(fp):
            try:
                with zipfile.ZipFile(fp) as z:
                    for zi in z.infolist():
                        if zi.filename.endswith(".lut"):
                            out_dir = tempfile.mkdtemp(prefix="ld_lut_")
                            z.extract(zi, out_dir)
                            return os.path.join(out_dir, zi.filename)
            except Exception:
                pass
    return None


def restore_lut(lut_path, db_name):
    """Restore SQL Server backup to a temporary DB using sqlcmd."""
    mdf = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.mdf")
    ldf = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}_log.ldf")
    lut_dest = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.lut")

    # Force-close all connections then drop
    drop_sql = (
        f"IF DB_ID(N'{db_name}') IS NOT NULL BEGIN "
        f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
        f"DROP DATABASE [{db_name}]; END"
    )
    subprocess.run(
        ["sqlcmd", "-S", SQL_INSTANCE, "-Q", drop_sql],
        capture_output=True, timeout=30
    )

    # Remove old file if locked by previous process
    if os.path.exists(lut_dest):
        try:
            os.remove(lut_dest)
        except OSError:
            # File still locked — use a unique name instead
            import uuid
            lut_dest = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}_{uuid.uuid4().hex[:8]}.lut")
            mdf = mdf  # keep same mdf/ldf paths (REPLACE handles it)

    shutil.copy2(lut_path, lut_dest)

    sql = (
        f"RESTORE DATABASE [{db_name}] FROM DISK = N'{lut_dest}' WITH "
        f"MOVE N'Project' TO N'{mdf}', "
        f"MOVE N'Project_log' TO N'{ldf}', RECOVERY"
    )
    result = subprocess.run(
        ["sqlcmd", "-S", SQL_INSTANCE, "-Q", sql],
        capture_output=True, text=True, timeout=120
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout or result.stderr)


def drop_db(db_name):
    try:
        drop_sql = (
            f"IF DB_ID(N'{db_name}') IS NOT NULL BEGIN "
            f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{db_name}]; END"
        )
        subprocess.run(
            ["sqlcmd", "-S", SQL_INSTANCE, "-Q", drop_sql],
            capture_output=True, timeout=30
        )
    except Exception:
        pass
    # Also remove the copied .lut file
    lut_dest = os.path.join(r"C:\ProgramData\Lutron", f"{db_name}.lut")
    try:
        if os.path.exists(lut_dest):
            os.remove(lut_dest)
    except OSError:
        pass


def q(db, sql, params=()):
    """Run a SELECT on SQL Server via master → USE [db] to avoid permission issues."""
    conn = sql_conn("master")
    cursor = conn.cursor()
    cursor.execute(f"USE [{db}]")
    cursor.execute(sql, params)
    cols = [d[0] for d in cursor.description]
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/select-folder", methods=["POST"])
def select_folder():
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    folder = filedialog.askdirectory(title="LDプロジェクトフォルダを選択")
    root.destroy()
    if not folder:
        return jsonify({"error": "キャンセルされました"}), 400

    sqlite_path = os.path.join(folder, "PlaceCache.sqlite")
    if not os.path.exists(sqlite_path):
        return jsonify({"error": "PlaceCache.sqlite が見つかりません"}), 400

    # Clean up old temp DB
    if state["db_name"]:
        drop_db(state["db_name"])

    state["folder"] = folder
    state["db_name"] = None
    state["lut_path"] = None

    # Try to locate and restore .lut
    if PYODBC_OK:
        lut = find_lut(folder)
        if lut:
            db_name = "LDViewer_Temp"
            try:
                restore_lut(lut, db_name)
                state["db_name"] = db_name
                state["lut_path"] = lut
            except Exception as e:
                return jsonify({"error": f"SQL Server復元失敗: {e}"}), 500

    return jsonify({"folder": folder, "has_template": state["db_name"] is not None})


@app.route("/api/overview")
def overview():
    if not state["folder"]:
        return jsonify({"error": "フォルダ未選択"}), 400

    sqlite_path = os.path.join(state["folder"], "PlaceCache.sqlite")

    place = sqlite_rows(sqlite_path, "SELECT * FROM tblPlace LIMIT 1")
    templates = sqlite_rows(sqlite_path, "SELECT id, name, modifiedDate, systemCount FROM tblTemplate")
    systems = sqlite_rows(sqlite_path, "SELECT * FROM tblTemplatedSystem")
    addr = sqlite_rows(sqlite_path, "SELECT * FROM tblAddressDetails LIMIT 1")

    return jsonify({
        "place": place[0] if place else {},
        "templates": templates,
        "systems": systems,
        "address": addr[0] if addr else {},
    })


@app.route("/api/areas")
def areas():
    if not state["db_name"]:
        return jsonify([])
    rows = q(state["db_name"], """
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
    db = state["db_name"]

    # Zones / Loads
    zones = q(db, """
        SELECT z.ZoneID, z.Name, z.ZoneNumber as LoadNumber,
               z.ControlType, z.ZoneLayer, z.ZoneDescription
        FROM tblZone z
        WHERE z.ParentID = ?
        ORDER BY z.ZoneNumber
    """, (area_id,))

    # Control Stations (keypads)
    stations = q(db, """
        SELECT cs.ControlStationID, cs.Name,
               csd.ControlStationDeviceID, csd.Name as DeviceName, csd.ModelInfoID
        FROM tblControlStation cs
        LEFT JOIN tblControlStationDevice csd ON csd.ParentControlStationID = cs.ControlStationID
        WHERE cs.ParentId = ? AND cs.ParentType = 2
        ORDER BY cs.SortOrder, cs.Name
    """, (area_id,))

    # Buttons per station device
    station_ids = list({s["ControlStationDeviceID"] for s in stations if s["ControlStationDeviceID"]})
    buttons = []
    if station_ids:
        placeholders = ",".join("?" * len(station_ids))
        buttons = q(db, f"""
            SELECT kb.ButtonID, kb.ButtonNumber, kb.Name as ButtonName,
                   kb.ParentDeviceID,
                   pm.Name as ProgModel, pm.ControlType as ProgControlType,
                   pm.PresetID
            FROM tblKeypadButton kb
            LEFT JOIN tblProgrammingModel pm ON kb.ProgrammingModelID = pm.ProgrammingModelID
            WHERE kb.ParentDeviceID IN ({placeholders})
            ORDER BY kb.ParentDeviceID, kb.ButtonNumber
        """, station_ids)

    # Scenes (via SceneController)
    scenes = q(db, """
        SELECT s.SceneID, s.Name, s.Number,
               sc.SceneControllerID
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
