# twostream — 双流 interval join 运行库

两条采集链（左 = 设备请求，右 = 设备响应）按**设备键 + 时间区间**在持续到达的事件流上
做连接，状态落 SQLite，结果经 outbox 逐条流出。纯 Python 标准库，无第三方依赖。

## 语义

配置：整数 `lo <= hi`，模式 `inner | left_outer | full_outer`。
左事件 `l` 与右事件 `r` 匹配，当且仅当两侧键均非 null 且相等，并且

```
l.time + lo <= r.time <= l.time + hi        （两端包含）
```

- 每个满足条件的**身份对** `(side, partition, seq)` 恰好输出一次；同值不同 seq 是不同
  事件，不做最近邻配对；匹配在第二个事件到达时即可发出。
- 分区在两侧**预先声明**。消息：`data(partition, seq, key, time, payload)`、
  `watermark(partition, w)`、`end(partition)`。
- `seq` 在分区内严格递增；重复或倒退 → `InputError`（不设重送账本）。
- `time/w` 为任意精度整数（内部用保序文本编码落盘，>64 位时间精确参与比较）。
- `w` 承诺该分区之后正常数据 `time >= w`；`time < w` 的数据进 **late 旁路**（不参与
  连接、不撤销任何已发结果）；`time == w` 仍属正常。水位单调；`end` 表示该分区水位
  为 +∞。无自动空闲判断——沉默分区会把本侧最小水位钉在 -∞。
- 未配左行只有当**右侧所有分区最小水位严格大于 `l.time + hi`** 才补一次 null
  （left_outer / full_outer）；右行对称：需左侧最小水位严格大于 `r.time - lo`
  （即左窗口 `[r.time-hi, r.time-lo]` 封闭），仅 full_outer 补 `(null, r)`。
  本侧水位前进不能替代对侧证明。null 键记录按对应外连接规则等待最终结论。

## 状态与回收

- 事件状态、匹配台账、outbox、latebox 都在 SQLite 中；匹配台账是**追加式**的
  （不修改旧事件行），与状态用同一时间证明回收。
- 回收不看"是否已匹配"：已匹配事件仍可能与晚到的合法事件配对。状态删除的唯一依据
  是对侧最小水位的边界证明（`l.time + hi < right_min` / `r.time - lo < left_min`）。
- 到达即被证明不可再配的事件（如对侧已 end）直接给出最终结论，不落状态。
- 结果按迭代器流出；热点键的匹配逐批写入 outbox，不在内存组装全量结果。

## API

```python
from interval_join import JoinEngine

eng = JoinEngine("state.db", lo=-2, hi=5, mode="full_outer",
                 left_partitions=["lp0"], right_partitions=["rp0", "rp1"])
eng.feed_data("L", "lp0", 1, "dev-1", 10, {"kind": "request"})
eng.feed_watermark("R", "rp0", 15)
eng.feed_end("R", "rp1")
for r in eng.pull_results(): ...   # Match / LeftPad / RightPad，取出即删
for l in eng.pull_late(): ...      # late 旁路记录
eng.watermarks()                   # 两侧各分区水位与最小水位
eng.pending()                      # 两侧待定行数 / outbox / late 积压
eng.status                         # 'running' | 'finished' | 'failed'
```

`finished` 只在两侧所有分区都 `end` 后成立；暂时没有待定行**不等于**作业完成。
任何 `InputError` / `ResourceError` 都会使引擎进入 `failed` 并拒绝后续输入。

## 事件文件演练

JSON Lines，首条可为 `config`；`barrier` 记录触发演练检查点，不进入引擎。
整数字段接受任意大小 JSON 整数或十进制字符串（>64 位安全）。

```bash
cd twostream
python -m interval_join.runner eventfiles/main.jsonl --db /tmp/state.db
python -m drills.drill_main         # 主例：L@10 / R@15 / 水位 15,30 -> 16
python -m drills.drill_two_matches  # R2@12 先配，R@15 再配：已匹配≠可回收
python -m drills.drill_reverse      # R@20 等待左上界 22（需严格大于）
python -m drills.drill_barrier      # 沉默分区挡住另一分区的大水位
python -m drills.drill_interleave   # 小样本离线枚举 vs 多种合法交错
python -m drills.drill_hotkey       # 磁盘热点键大输入：计数/采样/RSS/回收
python -m drills.run_all            # 全部演练 + 单元测试
python -m unittest discover -s tests -v
```

CLI 退出码：0 完成；1 输入/资源错误（失败）；2 用法错误；3 流未完结（有分区未 end）。

## 限制

- 首版不做崩溃恢复；seq/水位台账在内存，事件状态与结果落盘。
- 单进程单线程；输出顺序随调度变化，但身份对多重集合、补 null 集合、late 集合确定。
