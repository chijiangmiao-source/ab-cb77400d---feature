# 低温探测阵列校准端点子网审计服务

给定无向图（光纤节点与带正整数成本的干线/支路）和 2–10 个必须连通的校准
端点，返回**总成本最低且连通全部端点的边集**（非端点可作为中继，即 Steiner
树问题），并在同成本方案中以**升序边标识列表的字典序**给出唯一规范见证，
同时返回由该边集导出的连通邻接表。

- API：Python 3 标准库实现，零第三方依赖。
- 求解内核：C11 实现的终端子集动态规划（Dreyfus–Wagner）、节点汇聚合并
  （并查集）与多源最短路闭包（多源 Dijkstra）；不枚举边集、不调用通用
  优化器。
- 异步作业：现场工程师可先提交规模较大的复核取得可追踪的作业标识，再轮询
  最终结论；提交幂等（相同操作标识 + 完全相同载荷永远指向同一作业），作业
  持久化于 SQLite 卷，进程崩溃后仅从可恢复状态重算，绝不发布部分结果。
- 交付：`Dockerfile` 多阶段构建；`compose.yml` 暴露可配置宿主机端口、
  以 `/healthz` 做健康检查，并运行一次性 `verify` 服务。

## 目录结构

```
app/            HTTP 服务、请求校验、求解器 Python 前端、异步作业存储与执行器
core/steiner.c  原生求解内核（终端子集 DP + 汇聚合并 + 多源最短路闭包）
tests/          单元测试、构建产物检查、HTTP 冒烟（同步 + 异步）、verify 总入口
scripts/        离线暴力交叉验证脚本、Compose 端到端验收脚本（不属于服务）
Dockerfile      gcc 编译内核 + slim 运行镜像
compose.yml     api 服务（含作业持久卷）+ 一次性 verify 服务
```

## 快速开始（Docker Compose）

```bash
# 宿主机端口可配置，默认 8080
AUDIT_HOST_PORT=9090 docker compose build
AUDIT_HOST_PORT=9090 docker compose up \
    --abort-on-container-exit --exit-code-from verify
```

- `api` 在容器内监听 8080，宿主机通过 `${AUDIT_HOST_PORT:-8080}` 访问；
  Compose 健康检查周期性请求 `GET /healthz`。
- `verify` 等 `api` 健康后运行一次：求解器与作业存储单元测试 → 构建产物
  检查 → HTTP 冒烟（含并列裁决、无解边界与异步作业全流程），随后以退出码
  报告结果（0 成功）。

仅启动 API：

```bash
AUDIT_HOST_PORT=9090 docker compose up api
curl -s http://127.0.0.1:9090/healthz
```

## 本地运行（无 Docker）

```bash
gcc -O2 -std=c11 -Wall -Wextra -o core/steiner core/steiner.c
AUDIT_PORT=8080 python3 -m app.server
# 另一终端：
python3 tests/verify.py            # 需先启动服务，默认 http://127.0.0.1:8080
```

## API

### `GET /healthz`

```json
{"status": "ok"}
```

### `POST /api/audit`

请求：

```json
{
  "nodes": ["A", "B", "C", "R"],
  "edges": [
    {"id": "e1", "source": "A", "target": "R", "cost": 5},
    {"id": "e2", "source": "B", "target": "R", "cost": 5},
    {"id": "e3", "source": "C", "target": "R", "cost": 5},
    {"id": "p1", "source": "A", "target": "B", "cost": 9}
  ],
  "endpoints": ["A", "B", "C"]
}
```

约束：节点 2–60 个且为唯一 ASCII 标识；边 1–220 条，边标识唯一、成本为
正整数；允许平行边，禁止自环；端点 2–10 个且必须在节点表中声明。

成功响应（200）：

```json
{
  "cost": 15,
  "edge_set": ["e1", "e2", "e3"],
  "edges": [
    {"id": "e1", "source": "A", "target": "R", "cost": 5},
    {"id": "e2", "source": "B", "target": "R", "cost": 5},
    {"id": "e3", "source": "C", "target": "R", "cost": 5}
  ],
  "adjacency": {
    "A": [{"to": "R", "edge": "e1", "cost": 5}],
    "B": [{"to": "R", "edge": "e2", "cost": 5}],
    "C": [{"to": "R", "edge": "e3", "cost": 5}],
    "R": [
      {"to": "A", "edge": "e1", "cost": 5},
      {"to": "B", "edge": "e2", "cost": 5},
      {"to": "C", "edge": "e3", "cost": 5}
    ]
  }
}
```

- `cost`：最低总成本；`edge_set`：按 ASCII 升序排列的规范边标识；
- `edges`：对应边的完整描述（顺序无关，按 id 排序）；
- `adjacency`：**仅由所选边集导出**的连通邻接表（未使用节点不出现）。

### 错误（稳定且可定位，绝不返回部分子网或沿用上次结果）

错误体形如 `{"code": ..., "message": ..., "pointer": ...}`，`pointer` 为
JSON Pointer 风格定位（如 `/edges/3/cost`、`/endpoints/1`）。

| HTTP | code | 触发条件 |
| --- | --- | --- |
| 400 | `MALFORMED_JSON` / `INVALID_BODY` | 非 JSON 或根不是对象 |
| 400 | `NODE_COUNT_OUT_OF_RANGE` / `DUPLICATE_NODE` / `NON_ASCII_ID` / `EMPTY_ID` | 节点问题 |
| 400 | `EDGE_COUNT_OUT_OF_RANGE` / `DUPLICATE_EDGE_ID` / `SELF_LOOP` / `UNKNOWN_NODE` | 边结构问题 |
| 400 | `INVALID_COST` / `NON_POSITIVE_COST` / `COST_OUT_OF_RANGE` | 成本问题 |
| 400 | `ENDPOINT_COUNT_OUT_OF_RANGE` / `DUPLICATE_ENDPOINT` / `UNKNOWN_ENDPOINT` | 端点问题 |
| 422 | `DANGLING_ENDPOINT` | 端点无任何关联边（悬空） |
| 422 | `ENDPOINTS_UNCONNECTED` | 端点位于不同连通分量（无法连通），附 `components` |
| 405/404/411/413/500 | 对应语义码 | 方法错误、路径不存在、缺少长度、载荷过大、内部错误 |

每次请求都重新构建问题并启动一次独立内核进程，失败不会残留任何状态。

## 异步作业 API（大规模复核）

规模较大的校准子网复核可以先提交、再轮询结论：连接中断后凭操作标识重试
即可，绝不会重复发起同一次昂贵计算。

### `POST /api/jobs`

```json
{
  "operation_id": "review-2026-09-24-0007",
  "payload": { "nodes": ["A", "B"], "edges": [{"id": "e1", "source": "A", "target": "B", "cost": 3}], "endpoints": ["A", "B"] }
}
```

- `operation_id`：客户端提供的稳定操作标识（ASCII 字母数字开头，可含
  `. _ ~ -`，最长 128 字符）；`payload`：与 `POST /api/audit` 完全相同的
  审计载荷。
- 服务**先完整校验载荷**（错误体与同步入口一致，pointer 相对于审计载荷），
  校验通过才持久化作业并异步调用既有求解内核。
- 首次创建返回 `202` 与作业文档（`status` 为 `queued`/`running`）。
- **幂等**：相同 `operation_id` + 完全相同载荷（键序无关、数组顺序敏感）
  在并发提交、响应丢失后的重试、服务重启后都指向同一作业，返回 `200` 与
  该作业当前状态；相同标识但载荷不同返回 `409 OPERATION_CONFLICT`，且原
  记录不被覆盖。

### `GET /api/jobs/{operation_id}`

返回四种状态之一：

```json
{"operation_id": "review-2026-09-24-0007", "status": "queued|running"}
{"operation_id": "review-2026-09-24-0007", "status": "succeeded", "result": { "cost": 3, "edge_set": ["e1"], "edges": [...], "adjacency": {...} }}
{"operation_id": "review-2026-09-24-0007", "status": "failed", "error": {"code": "ENDPOINTS_UNCONNECTED", "message": "...", "pointer": "/endpoints/1", "components": [["A"], ["C"]]}}
```

- `result` 与 `POST /api/audit` 的成功响应**完全一致**；`error` 与同步入口
  的错误体（400/422）完全一致。
- 中间态（`queued`/`running`）绝不携带 `result`/`error` 字段；未知标识返回
  `404 JOB_NOT_FOUND`。

### 持久化与崩溃恢复

- 作业记录持久化于 SQLite（`AUDIT_JOB_DB`，容器内默认
  `/app/data/jobs.sqlite3`，Compose 挂载命名卷 `audit-jobs`），服务重启后
  作业、结论与幂等冲突判定全部保留。
- 结果通过**单条原子 UPDATE** 与状态一并发布；进程在写入结果前终止时，该
  作业仍是 `queued`/`running`，重启后仅从持久化的载荷重新计算——绝不发布
  半份边集或部分邻接表；`succeeded`/`failed` 为终态，绝不重算，旧失败绝不
  会被误作新成功。
- `AUDIT_JOB_DELAY_SECONDS`（默认 0）：测试/演示钩子，在作业处于 `running`
  时人为延迟，为崩溃恢复验收提供稳定的终止窗口。

## Compose 端到端验收

`verify` 服务覆盖单元测试与 HTTP 冒烟（含异步提交/轮询/幂等/冲突）。在此
之上，`scripts/acceptance_jobs.py` 驱动真实 Compose 部署完成完整验收：
实际提交作业、并发重复提交、幂等冲突复核，然后**在计算中途 SIGKILL 容器、
重启服务并轮询该作业直至成功**（结果与同步入口逐项一致），最后复核已完成
作业与冲突判定在重启后仍然有效、同步入口语义未变：

```bash
AUDIT_HOST_PORT=9090 docker compose up -d --build api
AUDIT_HOST_PORT=9090 python3 scripts/acceptance_jobs.py
```

## 算法与并列裁决

1. **终端子集动态规划（Dreyfus–Wagner）**：`dp[mask][v]` 表示连通
   `mask` 中端点且触及节点 `v` 的最小代价树。
2. **节点汇聚合并**：可行性阶段用并查集判定悬空端点与跨分量端点；DP
   合并阶段在同一顶点连接两个终端子树（枚举所有二元划分，锚定位去重）。
3. **多源最短路闭包**：每个子集以全部顶点的合并标签为种子，跑一次
   多源 Dijkstra 完成闭包松弛。
4. **规范见证**：仅在 DP 状态中保留单一（代价，见证）对不能保证全局
   字典序最优。因此先求出最优成本 `B`，再按边标识 ASCII 升序做贪心前缀
   扫描——对每条边询问“是否存在包含已选前缀与该边、成本恰为 `B` 的连通
   树”，该 oracle 用并查集收缩必选边后在收缩多重图上再做一次 Steiner DP。
   这是多项式次数的 DP 调用（≤ 边数 + 1），不枚举边集。

`scripts/cross_validate*.py` 对 5000+ 随机小图与暴力枚举逐项核对
（成本、无解、字典序见证、平行边、并列）。
