# 黄金价格趋势监测（公开网页版）

流量定价体制框架下的黄金监测看板：锚缺口、流量乘数、CTA 模拟仓位、金银相关象限、回撤状态与 13 周情景推演。静态站点（GitHub Pages）+ GitHub Actions 定时刷新数据。

## 框架来源

分析框架与指标口径来自《黄金定价失真归因报告 v3》（G1 锚断裂 / G2 流量乘数 / G6 定价权切换 / G4 相关性象限 / G5 接棒出清）。计算逻辑与内部看板 `monitor_build.py` 完全一致，网页版管道为 `pipeline/monitor_web.py`。

## 数据源

| 数据 | 来源 | 说明 |
|---|---|---|
| 上海金 Au99.99 / 上海银 Ag99.99 日线 | `data/au_*.csv`、`data/ag_*.csv`（iFinD 历史缓存，2016 年起，随仓库提交）+ akshare `spot_hist_sge` 增量 | akshare 历史自 2016-12 起、缺 2016 全年，仅用于近 30 天增量合并去重写回缓存；拉取失败自动降级用缓存 |
| 10 年 TIPS 实际收益率（DFII10）、人民币汇率（DEXCHUS） | FRED fredgraph.csv 直连 | 失败降级用缓存 |
| CFTC COT 黄金管理基金持仓（088691） | cftc.gov fut_disagg 年度 zip | 历史年为静态缓存，当年 zip 每次刷新，失败降级用缓存 |

所有网络失败均降级为使用仓库内缓存并继续产出 `data.json`，除非缓存本身缺失。

## 更新机制

- `.github/workflows/update.yml`：
  - 每天 UTC 10:40（北京时间 18:40，上金所收盘后）
  - 每周六 UTC 01:20（北京时间 09:20，CFTC COT 发布后）
  - 支持 `workflow_dispatch` 手动触发
- 管道产出仓库根 `data.json` 并增量更新 `data/` 缓存；有变更则由 github-actions bot 自动 commit + push
- 页面 `index.html` 加载时先 `fetch('data.json')`，失败则使用内嵌的兜底数据（`INITIAL_DATA`）

## 本地运行

```bash
pip install -r pipeline/requirements.txt
python pipeline/monitor_web.py   # 产出 data.json，并打印与报告参考值的验证对照
```

## 免责声明

本页面仅为研究监测工具：锚缺口是"锚失效程度的序数度量"而非估值结论；情景推演为方向性证据（小样本，有效期以周计）；合成美元金价与伦敦现货跟踪误差 ±0.5% 以内。所有内容不构成投资建议。
