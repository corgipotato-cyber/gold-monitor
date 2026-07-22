# -*- coding: utf-8 -*-
"""
monitor_web.py — 黄金定价体制监测数据管道（公开网页版 / GitHub Actions）
计算逻辑与 Kimi 内部看板的 monitor_build.py 完全一致（锚缺口 / 流量乘数 / CTA 模拟仓位 /
金银相关象限 / 回撤状态 / 体制判定 / 13 周情景推演），差异仅在数据接入：

  金银日线: 仓库内 iFinD 历史缓存（data/au_*.csv, data/ag_*.csv，2016 年起）
            + akshare spot_hist_sge 增量（近 30 天，合并去重写回缓存段文件）
            —— akshare 历史自 2016-12 起、缺 2016 全年，不能直接替代缓存
  FRED:     DFII10 / DEXCHUS 直连 fredgraph.csv（requests，失败回退 curl，再失败用缓存）
  CFTC COT: fut_disagg 年度 zip（requests/curl，失败用缓存 f_year_*.txt）

  所有网络失败降级为使用缓存并打印 WARN，不中断；除非连缓存都缺失。
  输出: 仓库根 data.json（结构与 monitor_data.json 一致）
"""
import json
import math
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------- 路径与常量
PIPELINE_DIR = Path(__file__).resolve().parent
WS = PIPELINE_DIR.parent                    # 仓库根（web/）
DATA = WS / "data"
COT_DIR = DATA / "cot"
OUT_PATH = WS / "data.json"

TODAY = date.today().isoformat()
CUR_YEAR = date.today().year
ANCHOR_END = "2021-12-31"     # M2 锚拟合窗口 2016-01-01 ~ 2021-12-31
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
AK_LOOKBACK_DAYS = 30         # akshare 增量回看窗口

# 缓存段文件（iFinD 历史，随仓库提交；最后一段接收 akshare 增量写回）
SEG_FILES = ["{p}_2016_2018.csv", "{p}_2019_2021.csv", "{p}_2022_2024.csv", "{p}_2025_now.csv"]
LIVE_SEG = "{p}_2025_now.csv"  # 增量写回段
AK_SYMBOL = {"au": "Au99.99", "ag": "Ag99.99"}
THSCODE = {"au": "AU9999.SHG", "ag": "AG9999.SHG"}

# G4 象限统计（报告表 5，硬编码并注明来源）
QUADRANT_STATS = {
    "low_corr":   {"ret12w": 2.9,  "maxdd": -2.7, "n": 435, "note": "正常定价"},
    "high_up":    {"ret12w": 9.9,  "maxdd": -1.4, "n": 32,  "note": "篮子上行趋势，顺势"},
    "high_down":  {"ret12w": 19.3, "maxdd": -6.3, "n": 11,  "note": "无差别抛售=投降式出清指纹"},
}

# 报告表 6：八次急跌事件（硬编码）
HISTORY_EVENTS = [
    {"date": "2016-05-03", "dd_pct": -6.7,  "d_oi_pct": -1,  "d_mm_oi_pct": -4.7, "flip_timing": "未触发",        "t20_pct": 9.6,  "t40_pct": 9.3},
    {"date": "2020-03-09", "dd_pct": -12.1, "d_oi_pct": -21, "d_mm_oi_pct": -8.4, "flip_timing": "当期(谷底当日)", "t20_pct": 12.0, "t40_pct": 18.5},
    {"date": "2020-08-07", "dd_pct": -8.3,  "d_oi_pct": -3,  "d_mm_oi_pct": -3.2, "flip_timing": "未触发",        "t20_pct": 3.5,  "t40_pct": 2.0},
    {"date": "2022-03-09", "dd_pct": -6.2,  "d_oi_pct": -5,  "d_mm_oi_pct": -4.4, "flip_timing": "滞后54交易日",  "t20_pct": 3.0,  "t40_pct": -5.1},
    {"date": "2024-10-30", "dd_pct": -8.5,  "d_oi_pct": -13, "d_mm_oi_pct": -6.9, "flip_timing": "未触发",        "t20_pct": 5.8,  "t40_pct": 5.7},
    {"date": "2025-04-22", "dd_pct": -9.6,  "d_oi_pct": -2,  "d_mm_oi_pct": -3.8, "flip_timing": "未触发",        "t20_pct": 8.0,  "t40_pct": 5.2},
    {"date": "2025-10-17", "dd_pct": -9.7,  "d_oi_pct": -7,  "d_mm_oi_pct": -1.6, "flip_timing": "未触发",        "t20_pct": 5.2,  "t40_pct": 13.4},
    {"date": "2026-01-29", "dd_pct": -17.1, "d_oi_pct": -16, "d_mm_oi_pct": -5.3, "flip_timing": "滞后49交易日",  "t20_pct": 12.3, "t40_pct": 4.7},
]


def log(msg):
    print(f"[monitor-web] {msg}", flush=True)


# ---------------------------------------------------------------- 下载（requests -> curl 双通道）
def download(url, dest: Path, timeout=300):
    """requests 优先；失败回退 curl（本地 Windows 环境 requests/urllib 偶发被断连，curl 已验证可用）。"""
    dest = Path(dest)
    err_req = None
    try:
        import requests
        with requests.get(url, headers=UA, timeout=(20, min(timeout, 240)), stream=True) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1 << 16):
                    f.write(chunk)
        if dest.exists() and dest.stat().st_size >= 100:
            return
        err_req = f"下载内容过小 ({dest.stat().st_size if dest.exists() else 0}B)"
    except Exception as e:  # noqa: BLE001 —— 任何 requests 失败都回退 curl
        err_req = f"{type(e).__name__}: {e}"
    if shutil.which("curl"):
        log(f"requests 失败({err_req})，回退 curl: {url}")
        r = subprocess.run(["curl", "-sL", "--fail", "-A", UA["User-Agent"],
                            "--connect-timeout", "20", "--max-time", str(min(timeout, 240)),
                            "--retry", "2", "-o", str(dest), url],
                           capture_output=True, text=True, timeout=timeout)
        if r.returncode == 0 and dest.exists() and dest.stat().st_size >= 100:
            return
        raise RuntimeError(f"curl 也失败: rc={r.returncode} {r.stderr[-300:]}")
    raise RuntimeError(f"下载失败且无 curl 回退: {err_req}")


# ---------------------------------------------------------------- 金银日线：iFinD 缓存 + akshare 增量
def fetch_sge(prefix):
    """读 data/ 缓存段；akshare 拉近 30 天增量，合并去重写回最新段。失败则用纯缓存。"""
    frames = []
    for tpl in SEG_FILES:
        fp = DATA / tpl.format(p=prefix)
        if not fp.exists():
            raise RuntimeError(f"缓存缺失: {fp}（历史基础数据应随仓库提交）")
        df = pd.read_csv(fp)
        df["time"] = pd.to_datetime(df["time"])
        frames.append(df)
    cached = pd.concat(frames).drop_duplicates("time").sort_values("time")

    try:
        import akshare as ak
        log(f"fetch akshare spot_hist_sge {AK_SYMBOL[prefix]}（近 {AK_LOOKBACK_DAYS} 天增量）")
        new = ak.spot_hist_sge(symbol=AK_SYMBOL[prefix])
        new["time"] = pd.to_datetime(new["date"])
        cutoff = pd.Timestamp(TODAY) - pd.Timedelta(days=AK_LOOKBACK_DAYS)
        new = new[new["time"] >= cutoff]
        if len(new):
            # 对齐 iFinD 缓存列结构后写回最新段文件
            live_fp = DATA / LIVE_SEG.format(p=prefix)
            live = pd.read_csv(live_fp)
            cols = list(live.columns)
            live["time"] = pd.to_datetime(live["time"])
            add = pd.DataFrame({
                "open": new["open"], "high": new["high"], "low": new["low"],
                "close": new["close"], "volume": np.nan, "thscode": THSCODE[prefix],
                "time": new["time"], "thsname_cn": "NA", "thsname_en": "NA", "currency": "NA",
            })[cols]
            merged = (pd.concat([live, add]).drop_duplicates("time", keep="last")
                        .sort_values("time"))
            merged["time"] = merged["time"].dt.strftime("%Y-%m-%d")
            merged.to_csv(live_fp, index=False)
            log(f"akshare 增量 {len(new)} 行已合并写回 {live_fp.name}（至 {merged['time'].iloc[-1]}）")
            merged["time"] = pd.to_datetime(merged["time"])
            cached = (pd.concat([cached, merged[cols]])
                        .drop_duplicates("time", keep="last").sort_values("time"))
        else:
            log("akshare 无近 30 天新数据，使用缓存")
    except Exception as e:  # noqa: BLE001 —— 网络/接口失败均降级
        log(f"WARN: akshare 拉取失败({type(e).__name__}: {e})，使用缓存 {prefix}_*.csv")

    out = cached.set_index("time")["close"].astype(float)
    return out


def fetch_fred(series_id, fname):
    fp = DATA / fname
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd=2016-01-01"
    log(f"fetch FRED {series_id}")
    try:
        download(url, fp, timeout=120)
    except Exception as e:  # noqa: BLE001
        if fp.exists():
            log(f"WARN: {type(e).__name__}; 使用缓存 {fname}")
        else:
            raise
    df = pd.read_csv(fp)
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    s = pd.to_numeric(df[series_id], errors="coerce")
    return pd.Series(s.values, index=df["observation_date"]).dropna()


def fetch_cot():
    frames = []
    for y in range(2016, CUR_YEAR + 1):
        zf = COT_DIR / f"fut_disagg_{y}.zip"
        tf = COT_DIR / f"f_year_{y}.txt"
        if y < CUR_YEAR and tf.exists():
            pass  # 静态缓存
        else:
            log(f"fetch COT fut_disagg {y}")
            url = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{y}.zip"
            try:
                download(url, zf, timeout=300)
            except Exception as e:  # noqa: BLE001
                if tf.exists():
                    log(f"WARN: {type(e).__name__}; 使用缓存 {tf.name}")
                else:
                    raise
            else:
                with zipfile.ZipFile(zf) as z:
                    member = [m for m in z.namelist() if m.lower().endswith(".txt")][0]
                    z.extract(member, COT_DIR)
                    (COT_DIR / member).replace(tf)
        df = pd.read_csv(tf, low_memory=False)
        g = df[df["CFTC_Contract_Market_Code"].astype(str).str.strip() == "088691"].copy()
        # 精确匹配市场名，防微型合约 MGC 混入
        g = g[g["Market_and_Exchange_Names"].str.strip() == "GOLD - COMMODITY EXCHANGE INC."]
        g["date"] = pd.to_datetime(g["Report_Date_as_YYYY-MM-DD"])
        frames.append(g[["date", "Open_Interest_All",
                         "M_Money_Positions_Long_All", "M_Money_Positions_Short_All"]])
    cot = pd.concat(frames)
    for c in ["Open_Interest_All", "M_Money_Positions_Long_All", "M_Money_Positions_Short_All"]:
        cot[c] = pd.to_numeric(cot[c], errors="coerce")
    # 同一周二两条记录时保留 OI 大的（双保险）
    cot = (cot.sort_values("Open_Interest_All")
              .drop_duplicates("date", keep="last")
              .sort_values("date").set_index("date"))
    cot["mm_net"] = cot["M_Money_Positions_Long_All"] - cot["M_Money_Positions_Short_All"]
    cot["oi"] = cot["Open_Interest_All"]
    return cot[["mm_net", "oi"]]


# ---------------------------------------------------------------- 主流程
def main():
    au = fetch_sge("au")
    ag = fetch_sge("ag")
    tips_d = fetch_fred("DFII10", "dfii10.csv")
    fx_d = fetch_fred("DEXCHUS", "dexchus.csv")
    cot = fetch_cot()
    log(f"AU {au.index.min().date()}~{au.index.max().date()} n={len(au)}; "
        f"AG n={len(ag)}; COT n={len(cot)} 至 {cot.index.max().date()}")

    # ---- 合成美元金价（报告 3.2）
    fx = fx_d.reindex(au.index).ffill()
    tips = tips_d.reindex(au.index).ffill()
    gold_usd = au / fx * 31.1035
    gold_cny = au.copy()

    # ---- M2 锚缺口：2016-2021 日频 OLS ln(G)=α+β·TIPS
    mask = gold_usd.index <= ANCHOR_END
    x = tips[mask].values
    y = np.log(gold_usd[mask].values)
    ok = ~np.isnan(x) & ~np.isnan(y)
    beta, alpha = np.polyfit(x[ok], y[ok], 1)
    yhat = alpha + beta * x[ok]
    r2 = 1 - ((y[ok] - yhat) ** 2).sum() / ((y[ok] - y[ok].mean()) ** 2).sum()
    anchor_implied_d = np.exp(alpha + beta * tips)
    gap_pct_d = (gold_usd / anchor_implied_d - 1) * 100
    cur_gap = float(gap_pct_d.iloc[-1])
    peak_date = gap_pct_d.idxmax()
    peak_gap = float(gap_pct_d.max())
    log(f"M2 锚缺口: 当前 {cur_gap:.1f}% (报告+359%), 峰值 {peak_gap:.1f}% @ {peak_date.date()} (报告+480% @2026-01-29); α={alpha:.3f} β={beta:.3f} R²={r2:.3f}")

    # ---- 周频主序列（W-FRI）
    gold_w = gold_usd.resample("W-FRI").last().dropna()
    ag_usd = ag / fx_d.reindex(ag.index).ffill() * 31.1035 / 1000
    silver_w = ag_usd.resample("W-FRI").last().dropna()
    wret = gold_w.pct_change()  # 周收益
    anchor_w = anchor_implied_d.resample("W-FRI").last().reindex(gold_w.index)
    gap_w = (gold_w / anchor_w - 1) * 100

    # ---- M3 流量乘数：flow_oi = ΔMM_net/OI（%）
    # 口径实证：周二对周二窗口（金价取 COT 报告日前值填充）可复现报告表 2
    gtue = gold_usd.reindex(cot.index, method="ffill")
    rtue = gtue.pct_change()
    flow_oi = cot["mm_net"].diff() / cot["oi"] * 100  # 单位：% OI
    j = pd.DataFrame({"r_bp": rtue * 1e4, "flow": flow_oi}).dropna()
    m_full, a_full = np.polyfit(j["flow"], j["r_bp"], 1)
    # 滚动 52 周斜率
    win = 52
    m_roll = (j["r_bp"].rolling(win).cov(j["flow"]) / j["flow"].rolling(win).var())
    m_latest = float(m_roll.iloc[-1])
    m_mean = float(m_roll.mean())
    # 映射到 W-FRI 周标签供图表序列使用
    m_roll_w = m_roll.copy()
    m_roll_w.index = m_roll_w.index + pd.to_timedelta(4 - m_roll_w.index.weekday, unit="D")
    log(f"M3 流量乘数: 全样本 {m_full:.1f}bp (报告23.6bp), 滚动52周最新 {m_latest:.1f}bp (报告9.1bp), 滚动均值 {m_mean:.1f}bp; 周样本 n={len(j)}")

    # ---- M4 CTA 模拟仓位（日频）
    mas = {n: gold_usd.rolling(n).mean() for n in (20, 60, 120, 252)}
    signs = {n: np.sign(gold_usd - mas[n]) for n in mas}
    signal = sum(signs.values()) / 4
    dret = gold_usd.pct_change()
    vol20_ann = dret.rolling(20).std() * math.sqrt(252)
    pos = signal * (0.12 / vol20_ann).clip(upper=2)
    pos = pos.dropna()
    cur_pos = float(pos.iloc[-1])
    tail = pos.iloc[-756:]
    pos_pctile = float((tail < cur_pos).mean() * 100)
    flip_count = int(sum(1 for n in mas if float(signs[n].iloc[-1]) < 0))
    sig_dict = {f"ma{n}": int(float(signs[n].iloc[-1])) for n in (20, 60, 120, 252)}
    cur_vol = float(vol20_ann.iloc[-1])
    log(f"M4 CTA: pos={cur_pos:.2f}, 分位={pos_pctile:.1f}% (报告~3%), flip={flip_count}/4, σ20={cur_vol:.2f}")

    # ---- G4 金银相关（周频 12 周滚动，156 周分位）
    sret = silver_w.pct_change()
    pair = pd.DataFrame({"g": wret, "s": sret}).dropna()
    corr12 = pair["g"].rolling(12, min_periods=10).corr(pair["s"])
    corr12 = corr12.reindex(gold_w.index).ffill()
    cur_corr = float(corr12.iloc[-1])
    corr_pctile_s = corr12.rolling(156, min_periods=104).apply(
        lambda v: (v[:-1] < v[-1]).mean() * 100, raw=True)
    cur_cpct = float(corr_pctile_s.iloc[-1])
    # 冲顶时分位（2026-01 附近最高）
    peak_window = corr_pctile_s.loc["2025-11":"2026-03"].max()
    dir12 = "up" if float(gold_w.iloc[-1] / gold_w.iloc[-13] - 1) > 0 else "down"
    quadrant = ("high_down" if cur_cpct >= 90 and dir12 == "down"
                else "high_up" if cur_cpct >= 90 else "low_corr")
    log(f"G4 相关: 12周 corr={cur_corr:.2f}, 分位={cur_cpct:.0f} (冲顶时 {peak_window:.0f}, 报告~97), 方向={dir12}, 象限={quadrant}")

    # ---- 回撤状态（M5：60 日高点起 20 个交易日内回撤>6%，反弹 2% 结束）
    high60 = gold_usd.rolling(60, min_periods=20).max()
    dd = (gold_usd / high60 - 1) * 100
    cur_dd = float(dd.iloc[-1])
    cur_high60 = float(high60.iloc[-1])
    in_event, event_start = False, None
    px = gold_usd.values
    idx = gold_usd.index
    last_high_i = int(np.argmax(px[max(0, len(px) - 60):])) + max(0, len(px) - 60)
    trough = px[last_high_i]
    for k in range(last_high_i + 1, len(px)):
        trough = min(trough, px[k])
        cur_ev_dd = px[k] / px[last_high_i] - 1
        if cur_ev_dd < -0.06 and (k - last_high_i) <= 20:
            if event_start is None:
                event_start = str(idx[k].date())
            if px[k] / trough - 1 >= 0.02:
                event_start = None  # 反弹 2%，事件结束
        elif cur_ev_dd < -0.06 and event_start is None:
            event_start = str(idx[k].date())  # 超 20 日仍深跌，视为事件延续
            if px[k] / trough - 1 >= 0.02:
                event_start = None
    in_event = event_start is not None
    ma60 = float(mas[60].iloc[-1])
    above_ma60 = bool(gold_usd.iloc[-1] > ma60)
    log(f"回撤: 当前 {cur_dd:.1f}% (60日高点 {cur_high60:.0f}), 事件中={in_event}, MA60={ma60:.0f}, above={above_ma60}")

    # ---- 10.3 体制判定与确认信号
    flip_ok = flip_count <= 2
    corr_ok = cur_cpct < 90
    ma60_ok = above_ma60
    conf_count = int(flip_ok) + int(corr_ok) + int(ma60_ok)
    if pos_pctile <= 10 and quadrant == "high_down":
        stage = "L3 出清观察窗口（CTA 出清完毕，等待新买家）"
        desc = ("CTA 仓位降至 3 年低分位、期货乘数低位、族内无差别抛售尾段（高相关+下跌象限）、"
                "锚缺口自峰值回落——卖压按机械顺序释放完毕，定价权等待新买家接棒。")
    elif conf_count >= 2:
        stage = "L4 重建窗口（确认信号过半）"
        desc = "确认信号满足 ≥2 条，可按报告口径重建仓位。"
    else:
        stage = "L2 出清进行中"
        desc = "趋势资金卖压仍是进行时，确认信号不足。"
    action_hint = ("满足 ≥2 条确认信号（翻空≤2/4、相关分位<90、收复MA60）可重建仓位；"
                   f"当前满足 {conf_count}/3 条。监测优于预测——周频看板回答『市场在哪个世界』。")
    log(f"体制: {stage}, 确认 {conf_count}/3")

    # ---- 滚动 3 个月（13 周）情景推演（方向性，非点预测）
    p0 = float(gold_usd.iloc[-1])
    q = QUADRANT_STATS[quadrant]
    base_ret = q["ret12w"] / 100
    bull_ret = base_ret + 0.08
    bear_ret = -0.08 if flip_count >= 3 else -0.05
    if flip_count >= 4:
        probs = {"bear": 0.40, "base": 0.42, "bull": 0.18}
    elif flip_count == 3:
        probs = {"bear": 0.30, "base": 0.48, "bull": 0.22}
    else:
        probs = {"bear": 0.18, "base": 0.52, "bull": 0.30}

    sig_w = float(dret.rolling(20).std().iloc[-1] * math.sqrt(7))  # 周化已实现波动
    rng = np.random.default_rng(42)
    H = 13

    def shaped_mean(kind, end_ret):
        t = np.arange(1, H + 1)
        if kind == "base" and quadrant == "high_down":
            dd_phase = np.minimum(t / 4, 1.0) * (q["maxdd"] / 100 * 0.8)
            rec = np.maximum(t - 4, 0) / (H - 4)
            return dd_phase + rec * (end_ret - (q["maxdd"] / 100 * 0.8))
        if kind == "bear":
            return -np.sqrt(t / H) * abs(end_ret)  # 阴跌形态
        return (t / H) * end_ret  # bull / 普通 base：渐进

    def sim_paths(kind, end_ret, n=4000):
        mean = shaped_mean(kind, end_ret)
        paths = np.zeros((n, H))
        for k in range(H):
            bridge_sd = sig_w * math.sqrt((k + 1) * (1 - (k + 1) / (H + 1)))
            paths[:, k] = p0 * np.exp(mean[k] + rng.normal(0, max(bridge_sd, 1e-6), n))
        return paths

    paths = {k: sim_paths(k, r) for k, r in (("bear", bear_ret), ("base", base_ret), ("bull", bull_ret))}
    mix = np.concatenate([paths["bear"], paths["base"], paths["bull"]])
    weights = np.concatenate([[probs["bear"]] * len(paths["bear"]),
                              [probs["base"]] * len(paths["base"]),
                              [probs["bull"]] * len(paths["bull"])])
    weights /= weights.sum()

    def wq(vals, w, q_):
        o = np.argsort(vals)
        cw = np.cumsum(w[o])
        return float(vals[o][np.searchsorted(cw, q_)])
    p10 = [wq(mix[:, k], weights, 0.10) for k in range(H)]
    p50 = [wq(mix[:, k], weights, 0.50) for k in range(H)]
    p90 = [wq(mix[:, k], weights, 0.90) for k in range(H)]
    scen_med = {k: [float(np.median(v[:, i])) for i in range(H)] for k, v in paths.items()}

    last_fri = gold_w.index[-1]
    fweeks = []
    for k in range(H):
        d = (last_fri + pd.Timedelta(days=7 * (k + 1))).date().isoformat()
        fweeks.append({"date": d,
                       "p10": round(p10[k], 1), "p50": round(p50[k], 1), "p90": round(p90[k], 1),
                       "bear": round(scen_med["bear"][k], 1),
                       "base": round(scen_med["base"][k], 1),
                       "bull": round(scen_med["bull"][k], 1)})

    scen_desc = {
        "bear": "滞后翻空阴跌：CTA 4/4 翻空的卖压是进行时（2026-03-23 确认、滞后 49 交易日），反弹被机械卖出压制；参考 2022-03 事件 T+40 -5.1%。",
        "base": f"历史同象限均值路径（{q['note']}，n={q['n']}）：先 {q['maxdd']}% 回撤、后 {q['ret12w']}% 反弹——无差别抛售是投降式出清指纹，央行购金构成缺口中枢的地板。",
        "bull": "V 型修复：未翻空事件 5/5 V 型 + 急跌 T+20 全正规律；确认信号若快速集齐，新买家接棒，价格压力反转（P1/P7 文献）。",
    }
    scenarios = []
    for key, name, ret in (("bear", "悲观·滞后翻空阴跌", bear_ret),
                           ("base", "基准·同象限历史均值", base_ret),
                           ("bull", "乐观·V型修复", bull_ret)):
        scenarios.append({"key": key, "name": name, "prob": probs[key],
                          "end_price": round(p0 * math.exp(ret), 1),
                          "end_ret_pct": round((math.exp(ret) - 1) * 100, 1),
                          "desc": scen_desc[key]})

    # ---- series（最近 156 周）
    s_tail = 156
    gw = gold_w.iloc[-s_tail:]
    idx_s = gw.index
    cta_w = pos.resample("W-FRI").last().reindex(idx_s)
    m_w = m_roll_w.reindex(idx_s, method="ffill")
    cp_w = corr_pctile_s.reindex(idx_s)
    series = {
        "dates": [d.date().isoformat() for d in idx_s],
        "gold_usd": [round(float(v), 1) for v in gw],
        "anchor_implied": [round(float(v), 1) if pd.notna(v) else None for v in anchor_w.reindex(idx_s)],
        "gap_pct": [round(float(v), 1) if pd.notna(v) else None for v in gap_w.reindex(idx_s)],
        "m_bp": [round(float(v), 1) if pd.notna(v) else None for v in m_w],
        "cta_pos": [round(float(v), 2) if pd.notna(v) else None for v in cta_w],
        "corr_pctile": [round(float(v), 0) if pd.notna(v) else None for v in cp_w],
        "weekly_ret_pct": [round(float(v) * 100, 2) if pd.notna(v) else None for v in wret.reindex(idx_s)],
    }

    out = {
        "as_of": str(gold_usd.index[-1].date()),
        "generated_at": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
        "price": {
            "gold_usd": round(p0, 1),
            "gold_cny": round(float(gold_cny.iloc[-1]), 2),
            "silver_usd": round(float(ag_usd.iloc[-1]), 2),
            "usdcny": round(float(fx.iloc[-1]), 4),
            "tips": round(float(tips.iloc[-1]), 2),
            "week_chg_pct": round(float(wret.iloc[-1]) * 100, 2),
            "drawdown_from_high_pct": round(cur_dd, 2),
            "high_60d": round(cur_high60, 1),
            "ma60": round(ma60, 1),
            "above_ma60": above_ma60,
        },
        "indicators": {
            "anchor_gap": {"pct": round(cur_gap, 1), "peak_pct": round(peak_gap, 1),
                           "peak_date": str(peak_date.date()),
                           "alpha": round(alpha, 4), "beta": round(beta, 4), "r2_fit": round(r2, 3)},
            "flow_multiplier": {"latest_bp": round(m_latest, 1), "mean_bp": round(m_mean, 1),
                                "full_sample_bp": round(m_full, 1),
                                "min_note": "滚动 52 周窗口；最新值低位=期货通道边际定价影响减弱，卖压来自 CTA 棒"},
            "cta": {"pos": round(cur_pos, 2), "pos_percentile": round(pos_pctile, 1),
                    "flip_count": flip_count, "signals": sig_dict,
                    "vol20_ann": round(cur_vol, 3)},
            "correlation": {"corr_12w": round(cur_corr, 2), "percentile": round(cur_cpct, 0),
                            "direction": dir12, "quadrant": quadrant},
            "drawdown": {"current_pct": round(cur_dd, 2), "in_event": in_event,
                         "event_start": event_start},
            "confirmation": {"flip_ok": flip_ok, "corr_ok": corr_ok, "ma60_ok": ma60_ok,
                             "count": conf_count},
        },
        "regime": {"stage": stage, "desc": desc, "action_hint": action_hint},
        "forecast": {
            "horizon_weeks": H,
            "method_note": ("方向性情景推演，非点预测（报告纪律：监测优于预测）。情景锚=G4 象限统计（表5，硬编码）；"
                            "路径=对数正态桥接，端点收益按情景、噪声形状匹配当前 20 日已实现波动率（√t 扩散）；"
                            "分位带由三情景按主观概率加权混合生成。"),
            "quadrant_anchor": {"key": quadrant, **{k: q[k] for k in ("ret12w", "maxdd", "n", "note")}},
            "scenarios": scenarios,
            "weeks": fweeks,
        },
        "series": series,
        "history_events": HISTORY_EVENTS,
        "caveats": [
            "锚缺口幅度依赖外推模型设定，应解读为『锚失效程度的序数度量』，不构成『应该跌回多少』的估值结论（报告 M2 解读纪律）。",
            "情景推演为方向性证据：高相关+下跌象限 n=11，急跌事件 n=8，小样本结论有效期以周计。",
            "银数据稀疏（约 100 obs/年），仅用于周频相关性，不进入日频回归。",
            "ETF 流量为公开数据缺口，归因中该棒次为推断；COT 与 CTA 模拟两通道构成归因骨架。",
            "合成美元金价与伦敦现货跟踪误差 ±0.5% 以内；USDCNY 周末缺失按前值填充。",
            "所有回测为样本内结果；前向跟踪满 20 个新观测后应复核（报告 7.4）。",
            "M3 流量乘数回归采用周二对周二窗口（实证复现报告表 2 的唯一口径）；W-FRI 周五收益 join 会使 M 系统性偏低约 9bp。",
            "CTA 翻空计数对最新一日敏感：报告基准日 2026-07-20 为 4/4 翻空、仓位 2.6% 分位；最新交易日价格收复 MA20 后翻空数回落，属数据演进而非口径差异。",
        ],
    }

    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    size_kb = OUT_PATH.stat().st_size / 1024
    log(f"written {OUT_PATH} ({size_kb:.1f} KB)")

    # ---- 验证对照
    print("\n===== 验证对照（报告参考值）=====")
    checks = [
        ("锚缺口当前", f"{cur_gap:.0f}%", "+359%", abs(cur_gap - 359) <= 10),
        ("锚缺口峰值", f"{peak_gap:.0f}% @{peak_date.date()}", "+480% @2026-01-29", abs(peak_gap - 480) <= 15),
        ("滚动M最新", f"{m_latest:.1f}bp", "9.1bp", abs(m_latest - 9.1) <= 5),
        ("全样本M", f"{m_full:.1f}bp", "23.6bp", abs(m_full - 23.6) <= 4),
        ("CTA pos分位", f"{pos_pctile:.1f}%", "~3%", pos_pctile <= 10),
        ("相关分位冲顶", f"{peak_window:.0f}", "~97", peak_window >= 90),
        ("金价量级", f"{p0:.0f} USD", "~4000", 3500 <= p0 <= 4500),
    ]
    for name, got, ref, okk in checks:
        print(f"  [{'OK ' if okk else 'DIFF'}] {name}: {got}  vs  报告 {ref}")


if __name__ == "__main__":
    main()
