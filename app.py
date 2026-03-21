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


def execute_sqls(statements):
    """Execute multiple DML statements in a single transaction."""
    conn = sql_conn()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(f"USE [{state['db_name']}]")
    for sql, params in statements:
        cur.execute(sql, params)
    conn.commit()
    conn.close()
    state["dirty"] = True


# LoadType → ControlType mapping
_CCO_MAINTAINED = {25, 37}
_CCO_PULSED     = {26}
_HVAC           = {48, 135}
_FAN            = {7, 46, 127, 128}
_SHADE          = {36, 53, 129, 130, 149}
_VENETIAN       = {49}
_SWITCHED       = {4, 17, 18, 20, 21, 22, 23, 24, 32, 33, 35, 40, 54, 111, 118, 133, 134}

def load_type_to_control_type(lt):
    if lt in _CCO_MAINTAINED: return 7
    if lt in _CCO_PULSED:     return 8
    if lt in _HVAC:           return 9
    if lt in _FAN:            return 6
    if lt in _SHADE:          return 5
    if lt in _VENETIAN:       return 10
    if lt in _SWITCHED:       return 2
    return 1  # Dimmed


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

    # Read all entries from the original template ZIP (preserve non-.lut files)
    with zipfile.ZipFile(tmpl_zip_path) as z:
        lut_entry = next(zi.filename for zi in z.infolist() if zi.filename.endswith(".lut"))
        other_entries = [
            (zi, z.read(zi.filename))
            for zi in z.infolist()
            if not zi.filename.endswith(".lut")
        ]
    app.logger.info(f"[save] Template ZIP: lut={lut_entry}, other files={len(other_entries)}")

    # Backup DB → C:\ProgramData\Lutron\ using a unique name to avoid file lock issues
    tmp_lut = os.path.join(r"C:\ProgramData\Lutron", f"LDViewer_bak_{uuid.uuid4().hex[:8]}.lut")

    app.logger.info(f"[save] Backing up DB to {tmp_lut}")
    backup_lut_from_db(state["db_name"], tmp_lut)

    if not os.path.exists(tmp_lut):
        raise RuntimeError(f"バックアップ失敗: {tmp_lut} が作成されませんでした")
    app.logger.info(f"[save] Backup size: {os.path.getsize(tmp_lut):,} bytes")

    # Repack: preserve all original files, replace only the .lut
    tmp_zip = tmpl_zip_path + ".new"
    app.logger.info(f"[save] Repacking template ZIP")
    with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED) as zout:
        for zi, data in other_entries:
            zout.writestr(zi, data)
        zout.write(tmp_lut, lut_entry)
    os.replace(tmp_zip, tmpl_zip_path)
    app.logger.info(f"[save] Template ZIP updated: {os.path.getsize(tmpl_zip_path):,} bytes")
    try:
        os.remove(tmp_lut)
    except OSError:
        pass

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
               z.ControlType, z.ZoneLayer, z.ZoneDescription,
               sl.LoadType, lt.Description as LoadTypeDesc
        FROM tblZone z
        LEFT JOIN tblSwitchLeg sl ON sl.SwitchLegID = z.ZoneID + 1
        LEFT JOIN lstLoadType lt ON lt.LoadTypeID = sl.LoadType
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


# ── Load type master ──────────────────────────

@app.route("/api/load-types")
def load_types():
    if not state["db_name"]:
        return jsonify([])
    rows = q("SELECT LoadTypeID, Description FROM lstLoadType ORDER BY LoadTypeID")
    for r in rows:
        r["ControlType"] = load_type_to_control_type(r["LoadTypeID"])
    return jsonify(rows)


@app.route("/api/zone", methods=["POST"])
def add_zone():
    """Add a new zone (tblZone + tblSwitchLeg) to an area."""
    data = request.json
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400

    area_id   = int(data["area_id"])
    name      = data.get("name", "New Zone").strip() or "New Zone"
    load_type = int(data.get("load_type", 98))
    zone_layer = int(data.get("zone_layer", 0))
    control_type = load_type_to_control_type(load_type)

    # Next IDs from tblNextObjectID (authoritative counter shared across all tables)
    next_obj = q("SELECT NextObjectID FROM tblNextObjectID")[0]["NextObjectID"]
    new_zone_id = next_obj
    new_sl_id   = next_obj + 1

    next_num = q(
        "SELECT ISNULL(MAX(ZoneNumber), 0) + 1 as n FROM tblZone WHERE ParentID = ?",
        (area_id,)
    )[0]["n"]
    sort_order = next_num - 1

    # Use ZoneColorInfo from an existing zone in the same area (LD default = 18)
    zci_row = q(
        "SELECT TOP 1 ZoneColorInfo FROM tblZone WHERE ParentID = ? ORDER BY ZoneNumber",
        (area_id,)
    )
    zone_color_info = zci_row[0]["ZoneColorInfo"] if zci_row else 18

    # Get ControllerID/ControllerType from tblZonable for an existing zone in this area
    zonable_row = q("""
        SELECT TOP 1 zn.ControllerID, zn.ControllerType
        FROM tblZonable zn
        JOIN tblZone tz ON zn.AssociatedZoneID = tz.ZoneID
        WHERE tz.ParentID = ?
        ORDER BY tz.ZoneNumber
    """, (area_id,))
    controller_id   = zonable_row[0]["ControllerID"]   if zonable_row else None
    controller_type = zonable_row[0]["ControllerType"] if zonable_row else 3

    # Get LoadTypePropertyType and Voltage from existing fixture with same load type
    fixture_props = q(
        "SELECT TOP 1 LoadTypePropertyType, Voltage FROM tblFixture WHERE LoadType = ?",
        (load_type,)
    )
    ltp_type = fixture_props[0]["LoadTypePropertyType"] if fixture_props else 4
    voltage  = fixture_props[0]["Voltage"]              if fixture_props else 2

    # Get tblFixtureLighting template from existing fixture with same load type
    fl_props = q("""
        SELECT TOP 1 fl.*
        FROM tblFixtureLighting fl
        JOIN tblFixture f ON fl.FixtureID = f.FixtureID
        WHERE f.LoadType = ?
        ORDER BY f.FixtureID
    """, (load_type,))
    fl = fl_props[0] if fl_props else None

    new_fa_id      = next_obj + 2
    new_fixture_id = next_obj + 3

    # Xid: 22-char URL-safe base64
    import base64
    xid_z       = base64.urlsafe_b64encode(uuid.uuid4().bytes)[:22].decode()
    xid_sl      = base64.urlsafe_b64encode(uuid.uuid4().bytes)[:22].decode()
    xid_fa      = base64.urlsafe_b64encode(uuid.uuid4().bytes)[:22].decode()
    xid_fixture = base64.urlsafe_b64encode(uuid.uuid4().bytes)[:22].decode()

    execute_sqls([
        ("""
            INSERT INTO tblZone (
                ZoneID, ParentID, Name, DesignRevision, DatabaseRevision,
                ZoneNumber, SortOrder, ZoneDescription,
                RaiseLowerConfiguration, ControlType, ObjectType, WhereUsedId,
                ZoneColorInfo, ObjectActivationState, ZoneConfiguration,
                ZoneLayer, Guid, Xid
            ) VALUES (
                ?, ?, ?, 1, 0,
                ?, ?, '',
                0, ?, 15, 2147483647,
                ?, 0, 1,
                ?, NEWID(), ?
            )
        """, (new_zone_id, area_id, name, next_num, sort_order, control_type, zone_color_info, zone_layer, xid_z)),
        ("""
            INSERT INTO tblSwitchLeg (
                SwitchLegID, ParentID, Name, DesignRevision, DatabaseRevision,
                SortOrder, OutputNumberOnLink, AbsoluteMinimumLevel, BurnInTime,
                ElectronicBypassLevel, HighEnd, InrushDelay, LampRunHoursThreshold,
                LowEnd, ManualOverrideLevel, AbsoluteMaximumLevel, IsNightLight,
                EmergencyModeType, ProgrammedOffLevel, LoadType, ObjectType, AFCI,
                LampLifeExpectancy, LampPreWarningTime, WhereUsedId, ObjectActivationState, Xid
            ) VALUES (
                ?, ?, ?, 1, 0,
                ?, 65535, 0, 100,
                0, 100, 0, 10000,
                1, 100, 100, 0,
                1, 0, ?, 10, 0,
                20000, 100, 2147483647, 0, ?
            )
        """, (new_sl_id, area_id, str(next_num), sort_order, load_type, xid_sl)),
        # tblZonable: links zone to its controller (ZonableID = SwitchLegID, same as LD format)
        ("""
            INSERT INTO tblZonable (ZonableID, AssociatedZoneID, ControllerID, ControllerType, ZonableObjectType)
            VALUES (?, ?, ?, ?, 10)
        """, (new_sl_id, new_zone_id, controller_id, controller_type)),
        # tblFixture must be inserted BEFORE tblFixtureAssignment (FK constraint)
        # tblFixture: ParentID = FixtureAssignmentID, ParentType = 7, ObjectType = 6
        ("""
            INSERT INTO tblFixture (
                FixtureID, Name, DesignRevision, DatabaseRevision,
                ManufacturerModel, ManufacturerName, Notes,
                PriceCurrency, PriceValue, LoadTypePropertyType,
                LoadType, Voltage, FixtureWattage, SortOrder,
                ParentID, ParentType, ObjectType, FixtureDescription,
                FixtureInfoID, PhaseControl,
                AssociatedFixtureGroupId, FixtureControllerModelInfo,
                WhereUsedId, Xid
            ) VALUES (
                ?, 'Override Fixture', 1, 0,
                '', '', '',
                '', 0, ?,
                ?, ?, 0, -1,
                ?, 7, 6, '',
                0, 0,
                NULL, NULL,
                2147483647, ?
            )
        """, (new_fixture_id, ltp_type, load_type, voltage, new_fa_id, xid_fixture)),
        # tblFixtureAssignment: ParentID = SwitchLegID, ParentType = 10
        ("""
            INSERT INTO tblFixtureAssignment (
                FixtureAssignmentID, ParentID, ParentType, Name, DesignRevision, DatabaseRevision,
                NumberofFixtures, SortOrder, FixtureID, WhereUsedId, Xid
            ) VALUES (?, ?, 10, 'FixtureAssignment 001', 1, 0, 1, 0, ?, 2147483647, ?)
        """, (new_fa_id, new_sl_id, new_fixture_id, xid_fa)),
        # tblFixtureLighting: copy ballast/lighting params from existing fixture with same LoadType
        ("""
            INSERT INTO tblFixtureLighting (
                FixtureID, BallastInfoModelInfoID, BallastInterfaceModelInfoID,
                BlipTimeOffset, BlipWidth, ElectronicBypassTime,
                LampQuantity, LampWattage, LoadInterfaceModelInfoID, LoadInterfaceQuantity,
                Softstart, VoltageCompensationDisabled, VoltageCompensationAlgorithm,
                BlankingPulse, FrequencyFiltering, SoftwarePll, Slushing,
                LampType, DimmingRange, LowEnd, HighEnd, PhysicalLowEnd, PhysicalHighEnd,
                AbsoluteMinimumLevel, BallastFactor, SizeID, DefaultControlsID,
                MountingTypeID, OptionsID, LampLifeExpectancy
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?
            )
        """, (
            new_fixture_id,
            fl["BallastInfoModelInfoID"]        if fl else 1856,
            fl["BallastInterfaceModelInfoID"]   if fl else None,
            fl["BlipTimeOffset"]                if fl else 0,
            fl["BlipWidth"]                     if fl else 6,
            fl["ElectronicBypassTime"]          if fl else 0,
            fl["LampQuantity"]                  if fl else 1,
            fl["LampWattage"]                   if fl else 0,
            fl["LoadInterfaceModelInfoID"]      if fl else None,
            fl["LoadInterfaceQuantity"]         if fl else 0,
            fl["Softstart"]                     if fl else True,
            fl["VoltageCompensationDisabled"]   if fl else False,
            fl["VoltageCompensationAlgorithm"]  if fl else 0,
            fl["BlankingPulse"]                 if fl else 1,
            fl["FrequencyFiltering"]            if fl else 1,
            fl["SoftwarePll"]                   if fl else 0,
            fl["Slushing"]                      if fl else 3,
            fl["LampType"]                      if fl else 0,
            fl["DimmingRange"]                  if fl else 0,
            fl["LowEnd"]                        if fl else 1,
            fl["HighEnd"]                       if fl else 100,
            fl["PhysicalLowEnd"]                if fl else 2700,
            fl["PhysicalHighEnd"]               if fl else 6000,
            fl["AbsoluteMinimumLevel"]          if fl else 0,
            fl["BallastFactor"]                 if fl else 1,
            fl["SizeID"]                        if fl else 0,
            fl["DefaultControlsID"]             if fl else 0,
            fl["MountingTypeID"]                if fl else 0,
            fl["OptionsID"]                     if fl else 0,
            fl["LampLifeExpectancy"]            if fl else 20000,
        )),
        # tblDaylightable: DaylightableID = SwitchLegID, ObjectType=10 (switchleg)
        ("""
            INSERT INTO tblDaylightable (DaylightableID, DaylightableObjectType, GainGroupID, DaylightingDesignType)
            VALUES (?, 10, NULL, 1)
        """, (new_sl_id,)),
        # Advance the global ID counter by 4 (zone, switchleg, fixture_assignment, fixture)
        ("UPDATE tblNextObjectID SET NextObjectID = ?", (next_obj + 4,)),
    ])

    return jsonify({"ok": True, "zone_id": new_zone_id, "zone_number": next_num})


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


# ── Zone delete ───────────────────────────────

@app.route("/api/zone/<int:zone_id>", methods=["DELETE"])
def delete_zone(zone_id):
    """Delete a zone and its associated SwitchLeg."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400

    zones = q("SELECT ZoneID FROM tblZone WHERE ZoneID = ?", (zone_id,))
    if not zones:
        return jsonify({"error": "Zone not found"}), 404

    # SwitchLeg relationship: SwitchLegID = ZoneID + 1 (LD format)
    # Also clean up any wrongly-created SwitchLeg with ParentID=zone_id
    execute_sqls([
        ("DELETE FROM tblFixtureLighting WHERE FixtureID = ?", (zone_id + 3,)),
        ("DELETE FROM tblFixture WHERE ParentID = ? AND ParentType = 7", (zone_id + 2,)),
        ("DELETE FROM tblFixtureAssignment WHERE ParentID = ? AND ParentType = 10", (zone_id + 1,)),
        ("DELETE FROM tblDaylightable WHERE DaylightableID = ?", (zone_id + 1,)),
        ("DELETE FROM tblZonable WHERE AssociatedZoneID = ? OR ZonableID = ?",
         (zone_id, zone_id + 1)),
        ("DELETE FROM tblSwitchLeg WHERE SwitchLegID = ? OR ParentID = ?",
         (zone_id + 1, zone_id)),
        ("DELETE FROM tblZone WHERE ZoneID = ?", (zone_id,)),
    ])
    return jsonify({"ok": True})


# ── Zone debug/compare ────────────────────────

def _try_q(sql, params=()):
    try:
        return q(sql, params)
    except Exception as e:
        return [{"__error__": str(e)}]


@app.route("/api/zone/<int:zone_id>/fix-switchleg", methods=["POST"])
def fix_zone_switchleg(zone_id):
    """Fix a zone whose SwitchLeg has ParentID=zone_id instead of area_id."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    zone = q("SELECT ZoneID, ParentID, ZoneNumber FROM tblZone WHERE ZoneID = ?", (zone_id,))
    if not zone:
        return jsonify({"error": "Zone not found"}), 404
    area_id    = zone[0]["ParentID"]
    zone_number = zone[0]["ZoneNumber"]
    sl_id = zone_id + 1
    execute_sqls([
        ("UPDATE tblZone SET ObjectType = 15 WHERE ZoneID = ? AND ObjectType != 15",
         (zone_id,)),
        ("UPDATE tblSwitchLeg SET ParentID = ?, Name = ? WHERE SwitchLegID = ?",
         (area_id, str(zone_number), sl_id)),
    ])
    return jsonify({"ok": True, "fixed_sl_id": sl_id, "new_parent_id": area_id})


@app.route("/api/zone/<int:zone_id>/debug")
def debug_zone(zone_id):
    """Return full data for a zone across multiple tables (for diagnosis)."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    zone = _try_q("SELECT * FROM tblZone WHERE ZoneID = ?", (zone_id,))
    sl_by_parent = _try_q("SELECT * FROM tblSwitchLeg WHERE ParentID = ?", (zone_id,))
    sl_by_id = _try_q("SELECT * FROM tblSwitchLeg WHERE SwitchLegID = ?", (zone_id + 1,))
    # Preset entries referencing this zone
    presets = _try_q("SELECT * FROM tblPreset WHERE ZoneID = ?", (zone_id,))
    # SwitchLeg by parent (zone's area)
    area_id = zone[0]["ParentID"] if zone and "ParentID" in zone[0] else None
    orphan_sls = []
    if area_id:
        orphan_sls = _try_q(
            "SELECT * FROM tblSwitchLeg WHERE ParentID = ?", (area_id,)
        )
    return jsonify({
        "zone": zone[0] if zone else None,
        "switchleg_by_parentid": sl_by_parent,
        "switchleg_by_id_plus1": sl_by_id,
        "presets_for_zone": presets,
        "switchlegs_with_area_as_parent": orphan_sls,
    })


@app.route("/api/schema-inspect")
def schema_inspect():
    """List tables that have columns likely referencing zones (ZoneID, SwitchLegID, etc.)"""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400

    # All user tables
    tables = _try_q("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_NAME
    """)

    # Columns named ZoneID or SwitchLegID anywhere
    zone_cols = _try_q("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE COLUMN_NAME IN ('ZoneID','SwitchLegID','ZoneId','SwitchlegId','OutputID')
           OR COLUMN_NAME LIKE '%ZoneID%'
           OR COLUMN_NAME LIKE '%SwitchLeg%'
        ORDER BY TABLE_NAME, COLUMN_NAME
    """)

    return jsonify({
        "all_tables": [r["TABLE_NAME"] for r in tables if "TABLE_NAME" in r],
        "zone_related_columns": zone_cols,
    })


@app.route("/api/db-integrity")
def db_integrity():
    """Check tblGuidManager, tblNextObjectID, FK violations, and triggers."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400

    guid_manager_cols = _try_q("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'tblGuidManager' ORDER BY ORDINAL_POSITION
    """)
    guid_sample = _try_q("SELECT TOP 3 * FROM tblGuidManager")
    next_id = _try_q("SELECT * FROM tblNextObjectID")
    triggers = _try_q("""
        SELECT t.name, OBJECT_NAME(t.parent_id) as table_name
        FROM sys.triggers t
        WHERE OBJECT_NAME(t.parent_id) IN ('tblZone','tblSwitchLeg')
    """)
    fk_violations = _try_q("""
        SELECT fk.name as constraint_name,
               tp.name as parent_table, cp.name as parent_col,
               tr.name as ref_table, cr.name as ref_col
        FROM sys.foreign_keys fk
        JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
        JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id
                            AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id
                            AND fkc.referenced_column_id = cr.column_id
        WHERE tp.name IN ('tblZone','tblSwitchLeg')
    """)
    return jsonify({
        "guid_manager_columns": [r.get("COLUMN_NAME") for r in guid_manager_cols],
        "guid_manager_sample": guid_sample,
        "next_object_id": next_id,
        "triggers_on_zone_switchleg": triggers,
        "fk_constraints": fk_violations,
    })


@app.route("/api/table-search", methods=["POST"])
def table_search():
    """Search a table for rows where a given column matches any of the provided IDs."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    data = request.json
    table  = data.get("table", "")
    column = data.get("column", "")
    ids    = data.get("ids", [])
    if not table or not column or not ids:
        return jsonify({"rows": []})
    # Validate table/column names (allow only alphanumeric + underscore)
    import re
    if not re.match(r'^[A-Za-z0-9_]+$', table) or not re.match(r'^[A-Za-z0-9_]+$', column):
        return jsonify({"error": "invalid name"}), 400
    ph = ",".join("?" * len(ids))
    rows = _try_q(f"SELECT TOP 20 * FROM [{table}] WHERE [{column}] IN ({ph})", ids)
    return jsonify({"rows": rows})


@app.route("/api/table-schema/<table_name>")
def table_schema(table_name):
    """Show all column names for a given table."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    cols = _try_q("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (table_name,))
    sample = _try_q(f"SELECT TOP 3 * FROM [{table_name}]")
    return jsonify({"columns": cols, "sample": sample})


@app.route("/api/zone-cross-table/<int:zone_id>")
def zone_cross_table(zone_id):
    """Check all zone-related tables for a given zone (to find missing entries)."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    sl_id = zone_id + 1
    return jsonify({
        "zone":                _try_q("SELECT * FROM tblZone WHERE ZoneID = ?", (zone_id,)),
        "switchleg":           _try_q("SELECT * FROM tblSwitchLeg WHERE SwitchLegID = ?", (sl_id,)),
        "zonable":             _try_q("SELECT * FROM tblZonable WHERE AssociatedZoneID = ?", (zone_id,)),
        "zone_control_ui":     _try_q("SELECT * FROM tblZoneControlUI WHERE AssignedZoneID = ?", (zone_id,)),
        "zone_hvac":           _try_q("SELECT * FROM tblZoneHVAC WHERE ZoneID = ?", (zone_id,)),
        "zone_phantom_hvac":   _try_q("SELECT * FROM tblZonePhantomHVAC WHERE ZoneID = ?", (zone_id,)),
        "dl_testpoint_sl":     _try_q("SELECT * FROM tblDLTestpointSwitchLeg WHERE AssignedSwitchLegID = ?", (sl_id,)),
        "shade_switchleg":     _try_q("SELECT * FROM tblShadeSwitchLeg WHERE SwitchLegID = ?", (sl_id,)),
        "sl_group_assoc":      _try_q("SELECT * FROM tblSwitchlegGroupAssociation WHERE AssociatedSwitchlegId = ?", (sl_id,)),
        # tblPreset: ParentType=15 → zone, ParentType=10 → switchleg
        "presets_zone":        _try_q("SELECT * FROM tblPreset WHERE ParentID = ? AND ParentType = 15", (zone_id,)),
        "presets_sl":          _try_q("SELECT * FROM tblPreset WHERE ParentID = ? AND ParentType = 10", (sl_id,)),
        "domain_obj":          _try_q("SELECT * FROM tblDomain WHERE DomainID = ?", (zone_id,)),
        "graphical_obj":       _try_q("SELECT * FROM tblGraphicalObject WHERE ParentID = ?", (zone_id,)),
        # tblFixtureAssignment: ParentType=10 → SwitchLeg; FixtureID = ZoneID+3
        "fixture_assignment":   _try_q("SELECT * FROM tblFixtureAssignment WHERE ParentID = ? AND ParentType = 10", (sl_id,)),
        "fixture":              _try_q("SELECT * FROM tblFixture WHERE FixtureID = ?", (zone_id + 3,)),
        # Fixture sub-tables (keyed by FixtureID = zone_id+3)
        "fixture_lighting":     _try_q("SELECT * FROM tblFixtureLighting WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_led":          _try_q("SELECT * FROM tblFixtureLed WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_led_classic":  _try_q("SELECT * FROM tblFixtureLedClassic WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_cco":          _try_q("SELECT * FROM tblFixtureCCO WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_fan":          _try_q("SELECT * FROM tblFixtureFan WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_hvac":         _try_q("SELECT * FROM tblFixtureHvac WHERE FixtureID = ?", (zone_id + 3,)),
        "lutron_fixture":       _try_q("SELECT * FROM tblLutronFixture WHERE FixtureID = ?", (zone_id + 3,)),
        "fixture_group":        _try_q("SELECT * FROM tblFixtureGroup WHERE ParentID = ? AND ParentType = 6", (zone_id + 3,)),
        "power_on_state":       _try_q("SELECT * FROM tblPowerOnState WHERE ParentID = ?", (sl_id,)),
        "daylightable":         _try_q("SELECT * FROM tblDaylightable WHERE DaylightableID = ?", (sl_id,)),
        "object_processor_map": _try_q("SELECT * FROM tblObjectToProcessorMap WHERE DomainObjectID = ?", (zone_id,)),
    })


@app.route("/api/zone-full-compare/<int:existing_id>/<int:new_id>")
def zone_full_compare(existing_id, new_id):
    """Full column-by-column comparison between two zones and their SwitchLegs."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    z1  = _try_q("SELECT * FROM tblZone WHERE ZoneID = ?", (existing_id,))
    z2  = _try_q("SELECT * FROM tblZone WHERE ZoneID = ?", (new_id,))
    sl1 = _try_q("SELECT * FROM tblSwitchLeg WHERE SwitchLegID = ?", (existing_id + 1,))
    sl2 = _try_q("SELECT * FROM tblSwitchLeg WHERE SwitchLegID = ?", (new_id + 1,))
    return jsonify({
        "existing_zone": z1[0] if z1 else None,
        "new_zone":      z2[0] if z2 else None,
        "existing_switchleg": sl1[0] if sl1 else None,
        "new_switchleg":      sl2[0] if sl2 else None,
    })


@app.route("/api/area/<int:area_id>/debug")
def debug_area(area_id):
    """Full dump of all zones+switchlegs in an area for comparison."""
    if not state["db_name"]:
        return jsonify({"error": "DB未接続"}), 400
    zones = _try_q("SELECT * FROM tblZone WHERE ParentID = ? ORDER BY ZoneNumber", (area_id,))
    zone_ids = [z["ZoneID"] for z in zones if "ZoneID" in z]
    result = []
    for z in zones:
        zid = z.get("ZoneID")
        sl_parent = _try_q("SELECT * FROM tblSwitchLeg WHERE ParentID = ?", (zid,))
        sl_id1    = _try_q("SELECT * FROM tblSwitchLeg WHERE SwitchLegID = ?", (zid + 1,))
        presets   = _try_q("SELECT * FROM tblPreset WHERE ZoneID = ?", (zid,))
        result.append({
            "zone": z,
            "switchleg_by_parentid": sl_parent,
            "switchleg_by_id_plus1": sl_id1,
            "presets": presets,
        })
    # Orphaned SwitchLegs (ParentID = area, not a zone)
    orphan_sls = _try_q(
        "SELECT * FROM tblSwitchLeg WHERE ParentID = ?", (area_id,)
    )
    return jsonify({"zones": result, "orphaned_switchlegs": orphan_sls})


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
