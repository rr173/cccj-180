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

## 设备历史归属统计仓（attribution mart）

运维按“请求发生时设备属于哪个团队”核算连接质量。归属台账通常隔天才补齐，
甚至会改写上周的一段归属。`attribution_mart` 在**独立的 SQLite 文件**中维护
这份统计，输入只读地取自现有 `SQLiteWarehouse` 的 `wh_meta` / `wh_results`：

- **不重跑双流连接**：原 `JobStore`、批次推进、确认投递、`wh_*` 表完全不变。
- 只读接入：源仓库以 `mode=ro` 打开，mart 无法改写仓库行、回执或源作业的确认
  位置。
- 统计粒度 **（窗口起点, 归属）**。每条 match 用左事件时间归窗，lpad 用左时间、
  rpad 用右时间：`window = O + floor((t-O)/W)*W`（左闭右开，负时间向下取整）。
  用同一个锚点时间查设备当时的归属。设备键为 null 或没有覆盖段时进入专门的
  **未知归属组**；它在存储中用空 owner 键表示，而真实 owner 不允许为空文本，
  因此与真实团队名（包括字面值 `"unknown"`）永不混同。
- 每组输出 match 对数、匹配请求数、未匹配请求数（lpad）、未匹配响应数（rpad），
  以及 match 延迟 `rtime-ltime` 的 sum/min/max/p50/p95。匹配请求按
  `(job_id, 左分区, 左 seq)` 去重：一请求匹配多个响应只算一个请求，不同作业
  中同名分区/seq 互不合并。分位数为 nearest-rank `ceil(p*n)`（p50=`(n+1)//2`，
  p95=`(19n+19)//20`），不插值；空 match 组 sum 为 0，min/max/分位数为 null；
  组内计数全为 0 时不保留该组。
- time、归属边界、O、W、延迟与求和全程任意精度整数：SQLite 中存十进制文本并用
  `enc_int` 保序键，绝不经过浮点或 64 位 SQLite 整数运算。

### 创建、同步与版本

```python
from attribution_mart import AttributionMart

mart = AttributionMart.create(
    "mart.db", warehouse="warehouse.db", job_ids=["job-1", "job-2"],
    width=10, origin=0)          # 绑定仓库自身 warehouse_id、作业集合、W、O
with mart.modify():
    mart.replace_attribution(0, {                # 初始台账（可选）
        "dev-a": [{"start": 0, "end": 20, "owner": "A"},
                  {"start": 20, "end": None, "owner": "B"}],
    })
    summary = mart.sync()
```

- 创建时绑定仓库的**持久身份** `warehouse_id`、要处理的 `job_id` 集合，以及固定
  正整数 W 与整数原点 O。仓库换路径仍可用（`AttributionMart.open(path,
  warehouse=new_path)` 或 `mart.rebind_warehouse()`，按身份而非路径匹配）；误指
  另一个仓库抛 `MartIdentityError` 且不写入。
- **一次 sync 只处理同一个已提交读快照**：先在一个只读事务里捕获各配置作业的
  结果上界，并在同一事务内读取恰好 `(已处理, 上界]`；期间采集端新提交的数据对
  本次不可见，留给下一次。返回各作业的捕获上界、已处理位置和新增事实/请求数。
  没有新结果并不表示源作业已结束或已全部交付——mart 只描述仓库现有内容。
- 事实身份是 `(job_id, offset)`；重复同步不会累计第二次。新范围出现 offset 缺口
  或无法解释的行（未知 kind、缺失身份字段）时**明确失败**，上一版本原样保留，
  不会跳过缺口。
- 多个作业的位置向量、事实、请求、延迟频次、当前归属与统计摘要作为**同一个
  mart 版本**在一个事务里发布（WAL）。读者要么看到上一个完整版本，要么看到新
  完整版本，不会看到一半作业或一半统计。

### 归属更正

```python
rev = mart.status()["mart_revision"]
with mart.modify():
    mart.replace_attribution(rev, {
        # 只改 [8,20) 的归属；未覆盖的设备/时段保持原样
        "dev-a": [{"start": 0, "end": 8, "owner": "A"},
                  {"start": 8, "end": 20, "owner": "C"},
                  {"start": 20, "end": None, "owner": "B"}],
        "dev-b": [],                    # 空历史 = 清除该设备全部归属
    })
```

- 传入 `expected_revision`（当前 mart 版本）与若干设备的**完整替换历史**。整体
  校验、整体发布：过期 revision（`StaleRevisionError`）、重复设备键、任一非法
  区间（空段、倒置、重叠、空 owner）都使整次更正被拒绝，其他设备与统计不会部分
  生效。允许空档、允许首尾相接、禁止重叠；输入段次序可任意。
- 一次成功更正只增加**一个维表 revision**，即使替换后的有效归属完全没有变化。
- **不重读源仓库、不重跑 JoinEngine**：先比较新旧时间映射，得到归属实际改变的
  整数区间，再经 `(device, anchor)` 索引按键集分页只访问这些区间内的已存事实。
  只重算受影响汇总组（旧组与新组），其他组不重写。未覆盖到的设备与时段保持原归属。
  更正同样作用于后续新事实：之后同步的新事实按新台账归组。
- 定位证据见 `python -m drills.drill_attribution_scale`（25 万事实中改一个窄区间，
  实际访问事实数、`EXPLAIN QUERY PLAN`、重算组数、峰值 RSS、前后业务数字）。

### 查询与展开

```python
q = mart.query()                       # 当前已提交快照
q["view_token"], q["mart_revision"], q["dim_revision"], q["positions"]
for g in q["groups"]: ...              # (window_start, owner) 及各指标
mart.query(view_token=q["view_token"], after=(0, "A"), limit=50)  # 分页

page = mart.expand(q["view_token"], 0, "A", kind="match", limit=100)
page = mart.expand(q["view_token"], 0, "A", limit=100,
                   after=page["next_after"])              # 下一页
```

- `query` 与 `expand` 在同一只读快照中读取，返回该快照的 mart 版本、维表 revision
  与各作业处理位置。`expand` 必须传入所查版本；版本已过期抛 `StaleVersionError`，
  绝不用新版明细解释旧版数字（不要求无限保留历史快照）。
- 展开按 `(window, owner)` 分页，返回仓库原 `(job_id, offset)`、连接双方身份与
  时间、锚点与窗口起点、延迟，以及命中的归属区间（或未知原因：`null device key`
  / `no covering attribution segment`），并回读仓库原行供独立核对。展开的身份集合
  与组内计数一致，因此可以从明细独立算回统计。

### 并发与恢复

- 每个 mart 同时只允许一个修改者：`with mart.modify():` 使用附带文件上的 flock；
  第二个修改者得到 `MartBusyError`（CLI 退出码 3）。sync 与归属更正互斥。
  读者不需要锁，长操作期间仍读上一个完整版本。
- 提交前被强制结束（SIGKILL / `os._exit`）: 事务回滚，回到旧版；提交后被杀：
  新版完整保留。锁由内核在进程退出时释放，重开后直接继续，无需手工清理。
- 若遇到未知格式版本、缺表、损坏或无法解释的元数据，`MartFormatError`（退出码 4）
  明确拒绝，不会静默解释。

### 命令行

```bash
python -m attribution_mart.cli create --mart mart.db --warehouse warehouse.db \
    --jobs JOB1 JOB2 --width 10 --origin 0
python -m attribution_mart.cli sync --mart mart.db
python -m attribution_mart.cli correct --mart mart.db --expected-revision 3 \
    --history history.json            # 或 --history -
python -m attribution_mart.cli query --mart mart.db [--view TOKEN] \
    [--after WINDOW:OWNER] [--limit N]
python -m attribution_mart.cli expand --mart mart.db --view TOKEN \
    --window 0 --owner A [--kind match] [--limit 100] [--after FACT_ID]
python -m attribution_mart.cli status --mart mart.db
python -m attribution_mart.cli rebind --mart mart.db --warehouse moved.db
```

`history.json` 形如 `{"dev-a": [{"start": 0, "end": 20, "owner": "A"}]}`，
也接受 `{"devices": {...}}`。未知归属组用 `--unknown`（JSON 中为 `null`）。
退出码：`0` 成功；`1` 拒绝（非法历史、过期 revision、仓库不符、新范围缺口、
未知 kind、过期 view）；`2` 用法错误；`3` 忙；`4` mart 格式错误。

### 内部表示与更新策略

- `facts`：每个仓库结果一行，含 `(job_id, offset)`、双方身份与时间、锚点、
  `enc_int` 顺序键的窗口、`owner_k` 与未知原因；`(ws_enc, owner_k)` 与
  `(k, anchor_enc)` 两个索引分别支撑分组统计与按设备区间定位。
- `reqs`：每个匹配请求一行，主键 `(job_id, lp, ls)` 保证去重请求；不同作业互不
  合并。match 延迟另存 `lat_freq`（`(ws_enc, owner_k, lat_enc) -> cnt`），因此
  “一个请求只算一次”与“每个 match 都进入分位数”同时成立。同一请求的所有 match
  共享左事件时间，因此锚点、窗口与归属必然相同——更正时它们整体一起迁移，不会
  出现跨组的“半个请求”。
- 统计摘要 `group_stats` 在每次发布事务内由这些表重算受影响组：min/max/sum 与
  精确 nearest-rank 分位数由一次按 `lat_enc` 升序的流式扫描 + 累计计数得到，
  不在 Python 内存中保存延迟列表。历史更正只把旧组/新组标脏重算。
- 归属台账 `attr_segments`（按设备排序的段）+ `changed_intervals` 二分比较新旧
  归属，只在归属真正改变的区间内定位事实，从而支持“扣旧组、补新组”的精确扣减。
  sync 与更正都通过有界 LRU（`HistoryIndex`）按需查询单个设备的历史，不把整个
  归属维表载入内存。
- 展开时报告的归属区间取自**当前版本**的台账（对当页设备按需查询），因此段被
  拆分/合并但 owner 未变时，无需重写任何事实行即可保持展开结果与数字一致。

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
python -m drills.drill_attribution_hand    # 设备归属：手算“为什么要扣回”
python -m drills.drill_attribution_accept  # 归属统计：独立参考 + 交错 + 崩溃 + 并发
python -m drills.drill_attribution_scale   # 归属统计：25 万事实下窄区间更正的访问证据
python -m drills.run_all            # 全部演练 + 单元测试
python -m unittest discover -s tests -v
```

CLI 退出码：0 完成；1 输入/资源错误（失败）；2 用法错误；3 流未完结（有分区未 end）。

## 限制

- `JoinEngine` 单次运行接口不做崩溃恢复（seq/水位台账在内存）；需要恢复
  时使用 `JobStore` 持久作业，其全部状态落盘，且不靠重放事件文件重建。
- 单进程单线程；输出顺序随调度变化，但身份对多重集合、补 null 集合、late
  集合确定。持久作业的交付身份 `(job_id, 通道, offset)` 在重送与重开后稳定。
- mart 只统计已投递到仓库的结果：不读取 `wh_late`，不推断源作业是否结束或
  是否全部交付。
- 更正只重算被影响的汇总组；未被影响组的摘要行不重写，受影响的组从事实表精确
  重算，因此最小值/分位数位置被移走时不会沿用旧统计。
- 演练生成物（`drills/out/` 下的事件文件与 SQLite 库）不进入版本控制；大数据
  （20 万以上无关事实、热点设备）在运行时产生。
