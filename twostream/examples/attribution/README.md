# attribution mart — 命令示例

前置：已用 `interval_join.jobcli` 跑过连接并把结果投递到一个 SQLite 仓库
（`warehouse.db`），记下 `jobcli status --db job.db` 里的 `job_id`。

```bash
WH=warehouse.db
JOB=$(python -m interval_join.jobcli status --db job.db | python -c 'import json,sys;print(json.load(sys.stdin)["job_id"])')

# 1) 创建 mart：绑定仓库身份、作业集合与窗口参数 W/O
python -m attribution_mart.cli create --mart mart.db --warehouse "$WH" \
    --jobs "$JOB" --width 10 --origin 0

# 2) 录入/替换初始归属台账（整体替换）
cat > history.json <<'JSON'
{
  "dev-a": [{"start": 0, "end": 20, "owner": "A"},
            {"start": 20, "end": null, "owner": "B"}],
  "dev-b": [{"start": null, "end": null, "owner": "unknown"}]
}
JSON
python -m attribution_mart.cli correct --mart mart.db \
    --expected-revision 0 --history history.json

# 3) 同步仓库里的新明细（只处理一个快照区间，原子发布新版本）
python -m attribution_mart.cli sync --mart mart.db

# 4) 查询统计（记下 view_token 供展开使用）
python -m attribution_mart.cli query --mart mart.db
python -m attribution_mart.cli query --mart mart.db --after 0:A --limit 20

# 5) 展开一个组的明细（分页；过期版本会被拒绝）
python -m attribution_mart.cli expand --mart mart.db --view "$VIEW" \
    --window 0 --owner A --kind match --limit 100

# 6) 只改一个设备的一小段时间，其他时段/设备保持不变
python - <<'PY'
import json
json.dump({"dev-a": [{"start": 0, "end": 8, "owner": "A"},
                     {"start": 8, "end": 20, "owner": "C"},
                     {"start": 20, "end": None, "owner": "B"}]},
          open("history2.json", "w"))
PY
python -m attribution_mart.cli correct --mart mart.db \
    --expected-revision "$REV" --history history2.json
python -m attribution_mart.cli query --mart mart.db

# 7) 仓库换了路径（同一个仓库身份）
python -m attribution_mart.cli rebind --mart mart.db --warehouse /new/path/warehouse.db

# 8) 状态
python -m attribution_mart.cli status --mart mart.db
```

`sync` / `correct` 需要独占修改锁；另一个进程同时修改会以退出码 3 失败，可重试。
