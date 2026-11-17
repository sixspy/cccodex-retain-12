# cc-retain-lab

CC_Codex 二期（26.11）**数据工程**产品线：**留存裁撤台**（FastAPI + 原生 JS SPA + SQLite）。

## 一句话骨架（自有，非外仓抄题）

**分区留存策略版本 → 到期扫描裁决 → 归档或销毁执行 → 策略与实物分区对账**

与现有六线正交（不是采集目录、ETL 管道、同步源、解析入库、编排流、种子摄取）。  
也不碰外仓常见指纹（Feed 归并 / 作业编排 / CDC 游标 / 质检签发 / 贴源-明细-汇总）。

## 领域

- **所属领域**：数据工程  
- **子方向**：数仓与数据湖（分区留存 / 裁撤闭环）

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | 原生 JS 单页（全中文），青灰底 + 铜强调，顶栏导航（非绿侧栏克隆） |
| 测试 | pytest + httpx（本地秒级闭环，优先于 compose） |

## 基线已通主链

1. 现行留存策略（主题域 / 保留天 / 到期动作）  
2. 实物分区列表（在线 / 归档 / 销毁）  
3. 按基准日到期扫描 → 生成待执行裁决  
4. 单条裁决执行（归档写地址 / 销毁清零）  
5. 策略与实物对账报告  

预留切口（未做，供后续功能题）：策略换版生效窗、重裁作废、批量失败闭互斥、跨域依赖冻结、归档回灌等。

## 影子试算（隔离方式：影子只读）

试算不碰正式分区。三选一已定 **影子只读**（不采用双写对照，也不做晋升原子切换）：

- **分轨可核**：影子扫描台账 / 影子裁决 / 影子执行结果 / 影子分区投影 / 影子报告全部落本机库 `shadow_*` 五表，与正式轨六表分列；每次试算记录正式分区队列指纹，报告与详情接口可复核「正式轨未动」。
- **串轨即拦**：影子试算连接挂 SQLite authorizer 守卫，对正式轨表（partitions / judgments / execute_runs / scan_jobs / reconcile_reports / retain_policies）的写一律拒绝。
- **互斥锁写**：正式扫描、正式执行、影子试算的写临界区共用 `STORAGE_WRITE_LOCK`，且提交在锁内——两侧争用串行，杜绝「影子双成功并把正式轨盖掉」。
- **页面标轨**：扫描 / 分区 / 裁决 / 对账为「正式」轨视图，「影子试算」页全为「影子」轨，逐处标明。

接口：`POST /api/shadow-runs {as_of_date}`、`GET /api/shadow-runs`、`GET /api/shadow-runs/{id}`（含落库复核 verify）。

## 本地（推荐反馈环）

```powershell
cd claude-workspace\cc-retain-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6500/login · **admin / 123456**

## Docker

```powershell
docker compose up -d --build
```

端口 **6500**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
