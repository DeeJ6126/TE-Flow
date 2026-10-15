# TEflow 2.2

源码仓库：[DeeJ6126/TE-Flow](https://github.com/DeeJ6126/TE-Flow)（Private）。作者：DeeJ6126。

部署在 **213 服务器的 /root/teflow**。CLI 和 TUI 共用一个持久化队列、自动资源调度器和 Snakemake 分析流程。

当前版本为 **2.2.1**。已确认、尚未实施的体验改进见 [ROADMAP.md](ROADMAP.md)。仓库保存代码、环境定义和文档；原始数据、参考 index、运行状态、日志、验证产物和访问凭据保留在服务器本地。

本轮完成需求 1、2、3、5。需求 4（单细胞）保留待补充。当前分析模板适用于人类双端 bulk RNA-seq。

## 在服务器上运行

使用已经登录 213 的终端，保持现有代理及反向 SSH 隧道在线。启动脚本会自动加载 TE 环境和代理设置。

**打开 TUI：**

```bash
tf
```

先选择数据类型“Bulk RNA-seq”，再选择“编号列表”，输入一个或多个逗号分隔的编号，回车开始；也可选择“TXT 文件路径”，输入服务器上 TXT 的绝对路径。TXT 为 UTF-8，每行一个编号，空行忽略，重复编号自动去重。一次批量输入共用所选类型；不同类型分批提交。

“单细胞 · 待配置”已预留，选择后会提示分析流程待配置，暂不提交、下载或创建分析任务。新任务必须明确选择类型；查看历史结果不需要选择。

界面顶部常驻四阶段主线：**解析项目 → 下载数据 → 分析数据 → 汇总结果**。

- 左侧展开 GSE，可选择 SRR 或项目级文件；尚未生成结果的 SRR 也会根据样本清单列出。
- 中间始终显示当前 GSE/SRR 的结果目录树；多文件目录默认折叠，点击或回车展开。只有一个文件的目录直接显示为路径，例如 abundance/SRR22521672.cntTable。选择文件后，在右下方预览。刷新保留展开状态、选中位置和滚动位置。
- 区域之间的横线、竖线可按住鼠标拖动，调整相邻区域大小；顶部输入区保持固定。布局自动保存在 state/ui-layout.json，重新打开沿用，并根据终端尺寸限制最小宽高。
- 右上方显示当前范围的精简实时输出：当前步骤、已知运行耗时和最近关键事件。查看某个 SRR 时，上方主线仍显示整个 GSE 的进度。
- 右下方顶部是“文件预览 / 运行日志”页签。F2 切换，点击文件自动回到预览；切换到日志时文件列表保留。
- 表格和文本按页读取，每页最多 100 行。上一页/下一页切换页面；刷新与页签切换保留当前页和滚动位置。超宽表最多预览 60 列，过长内容会明确提示截断，完整数据保存在原文件中。
- 文件预览支持鼠标拖选文字和 Ctrl+C；“复制本页”复制当前页，表格输出带表头的 TSV。预览只读。复制通过终端 OSC 52 发送至本机剪贴板，需要 SSH 终端允许剪贴板写入；复制后不弹出提示窗口。退出界面仍使用 Ctrl+Q。
- BAM、压缩 FASTQ、HTML 等特殊文件显示摘要和路径。默认隐藏内部文件，F3 显示/隐藏，F5 手动刷新。
- 日志默认跟随最新输出；滚轮向上或 Up/PageUp/Home 暂停跟随，End 恢复。换选 GSE/SRR 后日志跟随新范围。
- Ctrl+Q 退出界面；后台任务继续运行。已经打开的旧界面需退出后重新执行 tf 才会加载新布局。

文件浏览只读取当前项目结果目录，读取量有界；文件还在写入、被删除或发生截断时会显示提示。新增任务会保存关键事件，已有历史任务从日志与结果恢复显示。

**CLI：**

简写入口已安装到服务器命令路径，直接使用：

```bash
tf -a GSE219739 -t bulk
tf -a GSE219739,GSE113777 -t bulk
tf -f /root/geo_ids.txt -t bulk
```

`-a / --accession` 传入编号，`-f / --file` 传入服务器 TXT 的绝对路径；两者任选一种。`-t / --type` 是提交时的必填参数，当前使用 bulk；scrna 会在提交前提示单细胞流程待配置。`tf` 单独运行打开 TUI，`tf --help` 查看参数。简写入口自动加载 TE 环境和代理，与下面的完整命令共用同一套调度逻辑。

```bash
# 单项目
/root/teflow/launch.sh run GSE219739 -t bulk

# 多项目，逗号分隔
/root/teflow/launch.sh run GSE219739,GSE113777 -t bulk

# 从服务器绝对路径读取批量清单
/root/teflow/launch.sh run /root/geo_ids.txt -t bulk
```

TXT 示例：

```text
GSE219739
GSE113777
```

默认自动并行，无需 parallel 参数。CLI 在前台查看本批次，等待全部项目结束；Ctrl+C 退出查看时后台任务继续。

终端内显示动态概览；重定向到文件时只输出阶段、进度和状态变化。需要工具细节时使用 --verbose：

```bash
/root/teflow/launch.sh run /root/geo_ids.txt -t bulk --verbose
/root/teflow/launch.sh status
/root/teflow/launch.sh status GSE219739
/root/teflow/launch.sh logs GSE219739
```

若已激活 TE 环境，上述 launch.sh 后的参数也可直接用于 teflow，例如 teflow run GSE219739 -t bulk。维护入口同样明确类型：teflow submit GSE219739 -t bulk；status/logs 查询不需要类型。

CLI 退出码：全部成功 0；有项目失败/中断 1；输入或配置错误 2；Ctrl+C 退出查看 130。重复输入正在运行的编号会查看现有任务，已完成且文件完整时直接复用结果；失败项目可再次输入恢复。

## GEO 的解析和目录

GEO 项目使用真实 GSE 编号作为目录名。解析严格遵循 **GSE 自己列出的 GSM → 显式关联的 SRA Experiment → 全部 Run**，并保留每层关系。一个 GSM 对应多个 Run 时全部纳入；同一 Run 重复关联时只分析一次。

原始 FASTQ：

```text
/root/nfsdata/TEatlas/bulk_rna/GSE219739/
├── GSE219739.metadata.tsv
├── SRR22521672/
│   ├── SRR22521672_1.fastq.gz
│   └── SRR22521672_2.fastq.gz
└── SRR22521673/
    ├── SRR22521673_1.fastq.gz
    └── SRR22521673_2.fastq.gz
```

分析结果：

```text
/root/TEatlas/bulk_rna/GSE219739/
├── samples.tsv                # GSE/GSM/Experiment/Run 对照
├── samples.json               # 完整 Run 与 FASTQ 路径清单
├── config.yaml
├── job.json
├── run.log
├── logs/                      # 详细工具日志
├── SRR22521672/
│   ├── qc/
│   ├── align/
│   └── abundance/SRR22521672.cntTable
├── SRR22521673/
│   ├── qc/
│   ├── align/
│   └── abundance/SRR22521673.cntTable
└── merged.count               # 每个 Run 各占一列
```

单 Run 文件夹保留 TEcount 的 .cntTable，GSE 层生成合并表，合并沿用原模板的 inner join。物理目录按 Run 组织，GSM 与 Run 的关系见 samples.tsv。

仍支持直接输入 SRR/SRP/PRJNA 等编号。新的单 SRR 任务以 abundance/<SRR>.cntTable 为最终结果。旧版本已有结果与历史任务保留。

## 自动并行与资源

所有默认 CLI/TUI 入口共用 /root/teflow/state/jobs.json。输入清单先完整校验，再提交；调度器独立于界面运行，按共享 CPU、内存预算启动不同项目，资源不足时排队。一个项目失败会释放预约资源，其他项目继续。

settings.yaml 的默认总预算自动检测 CPU affinity、Linux cgroup 限制及可用内存，并使用其 80%。为保留原模板 STAR 参数，每个项目采用保守的整工作流资源预约，默认 10 CPU、256000 MB；项目内部由 Snakemake 安排各步骤。预约量用于调度核算，不表示进程始终实际占用这么多内存。

可以修改 settings.yaml 的 total_cores、total_mem_mb、resource_fraction；新一轮空闲批次重新检测预算。--cores N 可调整每项目 CPU 数，仍受共享总预算约束。

## 环境和维护

- 应用：/root/teflow
- 环境：/root/miniconda3/envs/TE，Python 3.12
- 原始模板：/root/TEatlas/bulk_rna/SRA00000
- 参考：沿用模板指定的 STAR index、基因 GTF、TE GTF
- 代理配置：/home/codex/server-213-setup/proxy.env
- 调度记录：/root/teflow/state/jobs.json、scheduler.log
- 旧版备份：/root/teflow-backups/20261014-v1.tar.gz

单端或配对信息不明确的数据会在准备阶段明确报错。单细胞分析尚未接入，属于需求 4，待补充。

setup/create-te.sh 用于重建环境，已经配置好的服务器直接使用启动命令即可。setup/conda-explicit.txt 和 setup/pip-lock.txt 保存软件版本记录。

## 实施与验证记录

详见 IMPLEMENTATION_PLAN.md 和 verification/v2/。测试包括真实 Linux 调度器/worker 多进程、资源排队与失败隔离、GEO 范围解析、Snakemake DAG、CLI 及 Textual Pilot。

2.0 版本验收的完整 Linux 测试 **119 项通过，另有 3 项子测试通过**。真实项目 **GSE219739** 首次运行成功：准确识别两个 Run，下载 4 个 FASTQ（共 48,129,144 字节），完成 fastp、STAR、TEcount 并输出含 **37,921 个基因/TE 特征、两个 Run 计数列**的 merged.count。逐项核对与两个输入计数表完全一致，Run 子目录没有重复合并表。

并行验证包括真实两个 GEO worker 同时启动、单端负向项目 GSE219738 在下载前失败而正常项目继续完成，以及隔离多进程测试中的“两项并行、第三项等待、失败后自动补位”。负向项目的失败记录保留用于审计。

verification/v2/end-to-end.json 保存真实结果审计，tests.log 保存最终测试输出，tui-completed.svg/png 为真实完成界面，GSE219739.merged.tsv 为合并表副本。CLI 的 TXT 批量输入、逗号输入、重复任务复用、退出码和 root/codex 访问均已验证。

简写入口验证见 verification/v2/tf-shortcut.json：单编号、逗号列表、TXT 和 root 登录 Shell 调用均返回 0，复用任务的运行次数不变；互斥参数错误返回 2。重建环境时 setup/install-shortcut.sh 会安装全局入口；发现已有同名命令时保留原命令。

2.1 浏览器改造：完整回归 **174 项测试和 3 项子测试通过**，最终交互专项 **13 项通过**。真实 GSE219739 验证了 SRR 选择、计数表首/次页与源文件一致、GSE 合并表 3 列预览、HTML 摘要、日志页签与暂停/恢复、内部文件开关。170×48 与 80×24 均进行了渲染检查；所有已有任务的状态、运行次数和进程记录保持不变，未重新运行分析。证据位于 verification/browser/，其中 real-ui.json、tests.log 和界面截图可供查阅。

2.2 交互改造：四条可拖动分隔线、保存布局、结果目录折叠、文字选择和当前页复制，以及 CLI/TUI 统一的数据类型选择。Linux 完整回归 **183 项通过**，最终布局专项 **9 项通过**。真实数据分页与复制内容均与源文件一致；单编号、逗号批量和 TXT 的 `-t bulk` 命令已验证复用结果，已有任务的运行次数和进程未变。证据位于 verification/workspace/。剪贴板已验证 OSC52 输出，终端接收仍取决于客户端权限。

2.2.1 微调：取消复制弹窗；单文件目录显示为路径，多文件目录继续折叠；横竖分隔线统一为可拖动的细线。Linux 相关交互测试 **35 项通过**，真实结果界面已检查。
