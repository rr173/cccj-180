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

## 持久作业与确认投递（可恢复连接作业）

`JobStore` 在单个 SQLite 文件中保存恢复所需的全部状态：事件状态、匹配台账、
分区最后 seq、水位、end、待投递结果（outbox/latebox）、late、输入确认位置
（批次台账）与目标绑定。任意精度时间一律以十进制文本持久化，不会降成浮点数。

- **job_id**：创建时生成，永久不变；创建时冻结连接参数与两侧分区声明，重开
  （`JobStore.open`）直接恢复。同一路径不会默默新建另一个作业、清空旧表或接受
  不同配置（`create` 遇非空文件抛 `JobExistsError`）。空文件、旧版仅存事件表的
  库、未知版本、损坏或无法解释的元数据都明确报 `StoreFormatError` 且不写入。
- **批次推进**：采集端提交信封 `{"batch_no": n, "messages": [...]}`，`batch_no`
  从 1 开始连续；消息沿用现有 data/watermark/end 格式。每批要么全部生效要么
  完全不生效，回执在持久提交后才返回。重送已确认批次且内容相同（按解码后的
  JSON 值比较：对象键序与空白无关，数组顺序、字符串/数值类型有意义；拒绝
  NaN/Infinity 与不可序列化对象）返回原回执，不重新配对、不重做回收、不再报
  重复 seq；号码相同内容不同、跳号、批内非法消息都拒绝，状态停在上一个确认点，
  之后仍可提交合法批次。全部 end 之后新批次拒绝，历史批次仍能重送取回执。
- **确认投递**：业务结果（`results`）与 late（`late`）各有独立、严格递增且
  不复用的 `delivery_offset`，一次交付由 `(job_id, 通道, offset)` 标识。
  `read_page` 按条数上限读页，不删除、重读一致；每页带回凭据（绑定作业、
  通道、页起点与末尾、内容哈希）。`ack_page` 只接受从当前确认位置开始的整页
  累计确认：跳行、跨通道/跨作业凭据、伪造末尾都被拒；同一凭据重复确认是
  成功空操作。确认后可回收已交付内容，offset 不会重新从 1 开始。处理完成
  （所有分区 end）与交付完成（无未确认结果）分开呈现：`status()` 里
  `processing.status` 与 `delivery.fully_delivered` / `warehouse_complete`。
- **修改权**：输入推进、确认、投递、绑定目标都需要修改锁
  （`job.acquire()` / `with job.modify():`），同一作业只允许一个进程持有；
  另一个修改者得到 `BusyError`（没有"最后写入者获胜"）。持有者被
  `os._exit`/`SIGKILL` 终止后锁由内核释放，后继进程直接接管，无需人工删锁。
  只读 `status()` 不需要锁，且只显示最后提交的完整状态。

```python
from interval_join import JobStore, deliver_to_warehouse

job = JobStore.create("job.db", lo=-2, hi=5, mode="full_outer",
                      left_partitions=["lp0"], right_partitions=["rp0"])
with job.modify():
    receipt = job.submit_batch({"batch_no": 1, "messages": [
        {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
         "key": "dev-1", "time": 10},
        {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
         "key": "dev-1", "time": 12}]})
page = job.read_page("results", 500)     # 查看待投递页（只读，可不持锁）
with job.modify():
    job.ack_page(page.credential)        # 累计确认整页
    summary = deliver_to_warehouse(job, "warehouse.db", page_size=500)
job.close()
job = JobStore.open("job.db")            # 重开：配置与全部进度恢复
print(job.status())
```

### 独立 SQLite 仓库投递

`deliver_to_warehouse` 把两条通道逐页投递到**独立**的 SQLite 仓库（两个文件、
两条连接，不借助 ATTACH 或同库事务）：一页在目标库内以一个事务写入记录和
接收凭证，目标提交后才确认源页。目标按 `(job_id, 通道, offset)` 去重：已存在
且内容一致视为重放成功；相同身份内容不同报 `DeliveryConflictError`，不覆盖、
不确认。首次投递绑定目标仓库自身的持久身份；之后同一仓库换路径可以继续，
另一个仓库必须拒绝（不会因为源端已确认就悄悄漏掉先前结果）。目标已提交、
源端尚未确认时杀进程，重启后凭接收凭证完成确认，目标记录不重复。投递器
逐页处理，内存取决于页大小而非历史结果总量；没有新输入时也能单独恢复投递。

### 作业命令行

```bash
python -m interval_join.jobcli create --db job.db --lo -2 --hi 5 \
    --mode full_outer --left-partitions lp0 --right-partitions rp0
python -m interval_join.jobcli input --db job.db --batch b1.json   # 或 --batch -
python -m interval_join.jobcli pages --db job.db --channel results --limit 100
python -m interval_join.jobcli ack --db job.db --credential cred.json
python -m interval_join.jobcli status --db job.db
python -m interval_join.jobcli deliver --db job.db --warehouse wh.db --page-size 500
```

退出码：`0` 成功；`1` 拒绝（非法输入、批次冲突/跳号、作业已完结、确认或
投递冲突、仓库身份不符、资源错误）；`2` 用法错误；`3` 忙（另一进程持有
修改锁）；`4` 存储格式错误（缺失/空库/旧格式/不兼容/损坏）。测试钩子导致的
模拟崩溃退出码为 `117`（`TWOSTREAM_CRASH_AT`，正常路径不依赖它）。

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
python -m drills.drill_recover      # 可恢复作业：真实子进程崩溃后的业务验收
python -m drills.drill_deliver      # 热点输入确认投递：中途杀进程再恢复
python -m drills.run_all            # 全部演练 + 单元测试
python -m unittest discover -s tests -v
```

CLI 退出码：0 完成；1 输入/资源错误（失败）；2 用法错误；3 流未完结（有分区未 end）。

## 限制

- `JoinEngine` 单次运行接口不做崩溃恢复（seq/水位台账在内存）；需要恢复
  时使用 `JobStore` 持久作业，其全部状态落盘，且不靠重放事件文件重建。
- 单进程单线程；输出顺序随调度变化，但身份对多重集合、补 null 集合、late
  集合确定。持久作业的交付身份 `(job_id, 通道, offset)` 在重送与重开后稳定。
- 演练生成物（`drills/out/` 下的事件文件与 SQLite 库）不进入版本控制。
