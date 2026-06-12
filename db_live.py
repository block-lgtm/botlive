import sqlite3
import os
from threading import Lock
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE  = os.path.join(BASE_DIR, "trades_live.db")
_DB_LOCK = Lock()


def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _DB_LOCK:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id          TEXT PRIMARY KEY,
                bot_name    TEXT,
                symbol      TEXT,
                side        TEXT,
                entry_price REAL,
                qty         REAL,
                open_time   TEXT,
                close_time  TEXT,
                natr        REAL,
                vol_text    TEXT,
                vol_24h     REAL,
                corr_btc    TEXT,
                signals     TEXT,
                delta_pct   REAL
            );

            CREATE TABLE IF NOT EXISTS trade_strategies (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id   TEXT REFERENCES trades(id),
                strategy   TEXT,
                tp         REAL,
                sl         REAL,
                status     TEXT DEFAULT 'OPEN',
                close_time TEXT,
                pnl_pct    REAL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_symbol
                ON trades(symbol);
            CREATE INDEX IF NOT EXISTS idx_strategies_trade
                ON trade_strategies(trade_id);
            CREATE INDEX IF NOT EXISTS idx_strategies_status
                ON trade_strategies(status);
        """)
        conn.commit()
        conn.close()
    print(f"✅ Live БД инициализирована: {DB_FILE}")


def insert_trade(trade_id, bot_name, trade_info, vol_text, vol_24h, corr_text):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO trades
                    (id, bot_name, symbol, side, entry_price, qty, open_time,
                     natr, vol_text, vol_24h, corr_btc, signals, delta_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_id,
                bot_name,
                trade_info["symbol"],
                trade_info.get("side", ""),
                trade_info["entry_price"],
                trade_info.get("qty", 0),
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                trade_info.get("natr"),
                vol_text,
                vol_24h,
                str(corr_text),
                ", ".join(trade_info.get("signals", [])),
                trade_info.get("delta_pct"),
            ))

            for strat_name, strat_data in trade_info["strategies"].items():
                conn.execute("""
                    INSERT INTO trade_strategies
                        (trade_id, strategy, tp, sl, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    trade_id,
                    strat_name,
                    strat_data["tp"],
                    strat_data["sl"],
                    strat_data.get("status", "OPEN"),
                ))

            conn.commit()
        except Exception as e:
            import traceback
            print(f"Ошибка insert_trade {trade_id}: {e}")
            traceback.print_exc()
        finally:
            conn.close()


def update_strategy_status(trade_id, strategy_name, status, pnl_pct=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                UPDATE trade_strategies
                SET status = ?, close_time = ?, pnl_pct = ?
                WHERE trade_id = ? AND strategy = ?
            """, (status, now, pnl_pct, trade_id, strategy_name))

            row = conn.execute("""
                SELECT COUNT(*) as cnt FROM trade_strategies
                WHERE trade_id = ? AND status = 'OPEN'
            """, (trade_id,)).fetchone()

            if row["cnt"] == 0:
                conn.execute(
                    "UPDATE trades SET close_time = ? WHERE id = ?",
                    (now, trade_id)
                )
            conn.commit()
        except Exception as e:
            import traceback
            print(f"Ошибка update_strategy_status {trade_id}: {e}")
            traceback.print_exc()
        finally:
            conn.close()


def get_open_trades():
    with _DB_LOCK:
        conn = get_conn()
        try:
            rows = conn.execute("""
                SELECT t.*,
                       s.strategy, s.tp, s.sl,
                       s.status as strat_status,
                       s.close_time as strat_close
                FROM trades t
                JOIN trade_strategies s ON s.trade_id = t.id
                WHERE t.id IN (
                    SELECT DISTINCT trade_id FROM trade_strategies
                    WHERE status = 'OPEN' AND tp IS NOT NULL
                )
                ORDER BY t.open_time DESC
            """).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_closed_trades(limit=2000, bot_name=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            if bot_name:
                rows = conn.execute("""
                    SELECT t.*, s.strategy, s.tp, s.sl,
                           s.status as strat_status,
                           s.close_time as strat_close
                    FROM trades t
                    JOIN trade_strategies s ON s.trade_id = t.id
                    WHERE t.id IN (
                        SELECT id FROM trades
                        WHERE close_time IS NOT NULL AND bot_name = ?
                        ORDER BY close_time DESC LIMIT ?
                    )
                    ORDER BY t.close_time DESC
                """, (bot_name, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT t.*, s.strategy, s.tp, s.sl,
                           s.status as strat_status,
                           s.close_time as strat_close
                    FROM trades t
                    JOIN trade_strategies s ON s.trade_id = t.id
                    WHERE t.id IN (
                        SELECT id FROM trades
                        WHERE close_time IS NOT NULL
                        ORDER BY close_time DESC LIMIT ?
                    )
                    ORDER BY t.close_time DESC
                """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_stats(bot_name=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            if bot_name:
                rows = conn.execute("""
                    SELECT s.strategy, s.status, COUNT(*) as cnt
                    FROM trade_strategies s
                    JOIN trades t ON t.id = s.trade_id
                    WHERE t.bot_name = ?
                    GROUP BY s.strategy, s.status
                """, (bot_name,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT strategy, status, COUNT(*) as cnt
                    FROM trade_strategies
                    GROUP BY strategy, status
                """).fetchall()

            stats = {}
            for row in rows:
                strat = row["strategy"]
                if strat not in stats:
                    stats[strat] = {"tp": 0, "sl": 0, "open": 0, "winrate": 0.0}
                if row["status"] == "TP":
                    stats[strat]["tp"] = row["cnt"]
                elif row["status"] == "SL":
                    stats[strat]["sl"] = row["cnt"]
                elif row["status"] == "OPEN":
                    stats[strat]["open"] = row["cnt"]

            for strat, s in stats.items():
                total = s["tp"] + s["sl"]
                s["winrate"] = round(s["tp"] / total * 100, 1) if total > 0 else 0.0
                s["total"]   = total
            return stats
        finally:
            conn.close()


def get_daily_stats(bot_name=None, date_from=None, date_to=None, strategies=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []
            if bot_name and bot_name != "all":
                conditions.append("t.bot_name = ?"); params.append(bot_name)
            if date_from:
                conditions.append("DATE(s.close_time) >= ?"); params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?"); params.append(date_to)
            if strategies:
                conditions.append(f"s.strategy IN ({','.join('?'*len(strategies))})"); params.extend(strategies)

            where = " AND ".join(conditions)
            rows  = conn.execute(f"""
                SELECT DATE(s.close_time) as date,
                       s.strategy, s.status, COUNT(*) as cnt
                FROM trade_strategies s
                JOIN trades t ON t.id = s.trade_id
                WHERE {where}
                GROUP BY DATE(s.close_time), s.strategy, s.status
                ORDER BY date ASC
            """, params).fetchall()

            STRAT_PCT = {"12:4": {"tp": 12, "sl": 4}}
            from collections import defaultdict
            days = defaultdict(lambda: {"tp": 0, "sl": 0, "pnl": 0.0})
            for row in rows:
                d   = row["date"]
                pct = STRAT_PCT.get(row["strategy"], {"tp": 0, "sl": 0})
                if row["status"] == "TP":
                    days[d]["tp"] += row["cnt"]
                    days[d]["pnl"] += row["cnt"] * pct["tp"]
                else:
                    days[d]["sl"] += row["cnt"]
                    days[d]["pnl"] -= row["cnt"] * pct["sl"]

            result = []
            for date in sorted(days.keys()):
                d     = days[date]
                total = d["tp"] + d["sl"]
                result.append({
                    "date":    date,
                    "tp":      d["tp"],
                    "sl":      d["sl"],
                    "pnl":     round(d["pnl"], 2),
                    "winrate": round(d["tp"] / total * 100, 1) if total > 0 else 0,
                })
            return result
        finally:
            conn.close()


def manual_close_strategy(trade_id, strategy_name, close_price):
    with _DB_LOCK:
        conn = get_conn()
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            row = conn.execute("""
                SELECT s.tp, s.sl, t.entry_price, t.side
                FROM trade_strategies s
                JOIN trades t ON t.id = s.trade_id
                WHERE s.trade_id = ? AND s.strategy = ?
            """, (trade_id, strategy_name)).fetchone()
            if not row:
                return {"error": "not found"}

            entry = row["entry_price"]
            side  = row["side"]
            pnl_pct = ((close_price - entry) / entry * 100) if side == "BUY" \
                      else ((entry - close_price) / entry * 100)
            status = "TP" if pnl_pct >= 0 else "SL"

            conn.execute("""
                UPDATE trade_strategies SET status = ?, close_time = ?, pnl_pct = ?
                WHERE trade_id = ? AND strategy = ?
            """, (status, now, round(pnl_pct, 2), trade_id, strategy_name))
            open_count = conn.execute("""
                SELECT COUNT(*) FROM trade_strategies
                WHERE trade_id = ? AND status = 'OPEN'
            """, (trade_id,)).fetchone()[0]
            if open_count == 0:
                conn.execute("UPDATE trades SET close_time = ? WHERE id = ?", (now, trade_id))

            conn.commit()
            return {"status": status, "pnl_pct": round(pnl_pct, 2)}
        except Exception as e:
            import traceback; traceback.print_exc()
            return {"error": str(e)}
        finally:
            conn.close()

def get_symbol_stats(date_from=None, date_to=None, strategies=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []
            if date_from:
                conditions.append("DATE(s.close_time) >= ?"); params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?"); params.append(date_to)
            if strategies:
                conditions.append(f"s.strategy IN ({','.join('?'*len(strategies))})"); params.extend(strategies)
            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT t.symbol, s.strategy, s.status, COUNT(*) as cnt
                FROM trade_strategies s JOIN trades t ON t.id=s.trade_id
                WHERE {where}
                GROUP BY t.symbol, s.strategy, s.status
            """, params).fetchall()
            STRAT_PCT = {"12:4": {"tp": 12, "sl": 4}}
            from collections import defaultdict
            syms = defaultdict(lambda: {"tp":0,"sl":0,"pnl":0.0})
            for row in rows:
                pct = STRAT_PCT.get(row["strategy"], {"tp":0,"sl":0})
                if row["status"] == "TP":
                    syms[row["symbol"]]["tp"] += row["cnt"]
                    syms[row["symbol"]]["pnl"] += row["cnt"] * pct["tp"]
                else:
                    syms[row["symbol"]]["sl"] += row["cnt"]
                    syms[row["symbol"]]["pnl"] -= row["cnt"] * pct["sl"]
            result = []
            for sym, d in syms.items():
                total = d["tp"] + d["sl"]
                result.append({"symbol":sym,"tp":d["tp"],"sl":d["sl"],
                    "pnl":round(d["pnl"],2),
                    "winrate":round(d["tp"]/total*100,1) if total>0 else 0})
            return sorted(result, key=lambda x: x["pnl"], reverse=True)
        finally:
            conn.close()


def get_weekday_stats(date_from=None, date_to=None, strategies=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []
            if date_from:
                conditions.append("DATE(s.close_time) >= ?"); params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?"); params.append(date_to)
            if strategies:
                conditions.append(f"s.strategy IN ({','.join('?'*len(strategies))})"); params.extend(strategies)
            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT strftime('%w', s.close_time) as dow,
                       s.strategy, s.status, COUNT(*) as cnt
                FROM trade_strategies s JOIN trades t ON t.id=s.trade_id
                WHERE {where}
                GROUP BY dow, s.strategy, s.status
            """, params).fetchall()
            STRAT_PCT = {"12:4": {"tp": 12, "sl": 4}}
            DAY_MAP = {"0":"Sunday","1":"Monday","2":"Tuesday","3":"Wednesday","4":"Thursday","5":"Friday","6":"Saturday"}
            ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            from collections import defaultdict
            days = defaultdict(lambda: {"tp":0,"sl":0,"pnl":0.0})
            for row in rows:
                day = DAY_MAP.get(row["dow"], row["dow"])
                pct = STRAT_PCT.get(row["strategy"], {"tp":0,"sl":0})
                if row["status"] == "TP":
                    days[day]["tp"] += row["cnt"]; days[day]["pnl"] += row["cnt"]*pct["tp"]
                else:
                    days[day]["sl"] += row["cnt"]; days[day]["pnl"] -= row["cnt"]*pct["sl"]
            result = []
            for day in ORDER:
                if day in days:
                    d = days[day]; total = d["tp"]+d["sl"]
                    result.append({"day":day,"tp":d["tp"],"sl":d["sl"],
                        "pnl":round(d["pnl"],2),
                        "winrate":round(d["tp"]/total*100,1) if total>0 else 0})
            return result
        finally:
            conn.close()