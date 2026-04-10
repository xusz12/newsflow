# opencli-sequential-news-zh README

这份 README 不是给模型执行用的，主要是给人看，方便后续维护这个 skill 时快速回答两个问题：

1. 这个 skill 目前到底改过哪些 OpenCLI 自带命令
2. 过去用 OpenCLI 时踩过哪些坑

## 近期安全修复

为避免旧的临时结果被误当成新一轮输入、以及正式 `freshNews` 文件被误覆盖，这个 skill 现在增加了几条硬约束：

- 每次运行的中间文件必须放在 `.news_state/runs/<run-dir>/` 下，不能再复用 `.news_state/tmp_current.json` 这类平铺临时文件。
- `run_news_pipeline.py` 现在会写出 `run_id / started_at / finished_at`，后续 `prepare / finalize` 会校验这些身份字段。
- `run_incremental_news.py finalize` 已移除 `--allow-overwrite-existing-run`，正式 `YYYY-MM-DD-HH-mm_freshNews.md` 一旦存在就拒绝覆盖。
- `prepare` 会拒绝消费早于当天最新 finalized run 的旧 `current.json`。
- `finalize` 会校验 `prepare` 时看到的 state 快照；如果 state 期间发生变化，必须重新执行 `prepare`。

## 目前这个 skill 改过的 OpenCLI 内置命令

以下只记录相对于原始 skill 配置有明确变更的内置命令。

### 1. `bbc_news`

- 旧命令：`opencli BbcPublic news --limit 10 --format json`
- 现命令：`opencli bbc news --limit 10 --format json`
- 原因：
  - `BbcPublic` 不是当前可用的内置命令名
  - 当前环境里可用的是 `bbc news`

### 2. `bloomberg_main`

- 旧命令：`opencli bloomberg main --limit 15 --format json`
- 现命令：`opencli BloombergUser main --limit 15 --format json`
- section：`bloomberg_main`
- 原因：
  - 之前误以为有 `bloomberg-latest` 之类的命令
  - 实际内置可用的是 `opencli bloomberg main`
  - 但官方 `bloomberg main` 返回 `title / summary / link / mediaLinks`，缺少 `time`
  - 为避免 `freshNews` 中 Bloomberg 发布时间显示为 `页面未显示`，已复制官方 RSS 思路并新增用户自定义命令 `BloombergUser main`
  - 新命令输出符合当前 skill 的新闻契约：`title / time / url`
  - 新命令保留 `summary`，但不输出无用的 `mediaLinks`

## 当前 commands.json 的结构调整

目前 `references/commands.json` 的顺序被整理成：

1. `opencli` 新闻源在上
2. `twitter` 账户源在下

当前新闻源 section 顺序：

- `middle-east`
- `china`
- `world`
- `business`
- `technology`
- `bloomberg_main`
- `bbc_news`
- `techcrunch`
- `arstechnica`

当前 twitter section 顺序：

- `Ilya Sutskever`
- `郭明錤`
- `seekinganythingbutalpha`
- `外汇交易员`
- `Time Horizon`
- `数字游民Jarod`
- `卡比卡比`
- `Aelia Capitolina`

## 为了兼容这些命令，管道脚本还改了什么

除了 `commands.json`，`scripts/run_news_pipeline.py` 也做了兼容增强，否则仅改命令列表不够。

### `normalize_row()` 当前兼容规则

- `url` 读取顺序：
  - `url`
  - `link`

- `time` 读取顺序：
  - `time`
  - `publishedAt`
  - `pubDate`
  - `date`
  - `published_at`
  - `createdAt`
  - `created_at`

这次改动的核心原因是：

- `opencli bloomberg main` 返回的是 `link`，不是 `url`
- 某些新闻源返回的时间字段名不统一
- `BloombergUser main` 已在 adapter 层直接输出 `url` 和 `time`，因此后续优先使用它，而不是官方 `bloomberg main`
- `summary` 会从 pipeline 透传到增量输出，最终在 `bloomberg_main` / `Bloomberg` 栏目下显示为 `摘要`
- 因 Bloomberg RSS 摘要是英文，`run_incremental_news.py finalize` 会要求 `translated.json` 为这些摘要提供中文 `summary` 或 `summary_zh`；如果缺失或仍非中文，会直接失败，避免英文摘要进入最终 Markdown

## 已经踩过的坑

### 坑 1：CLI 升级了，不代表 Browser Bridge 扩展也升级了

实际遇到过的报错：

- `Unknown action: network-capture-start`

根因：

- `opencli` CLI 已升级到 `1.6.8+`
- 但 Chrome 的 Browser Bridge 扩展仍是旧版本
- 旧扩展不认识新的 `network-capture-start` action

解决方式：

- 从 GitHub 下载并更新最新版 Browser Bridge 扩展

结论：

- 遇到 `network-capture-start` 报错时，第一反应先查扩展版本，不要先怀疑 skill 或 adapter

### 坑 2：不要凭名字猜内置命令是否存在

实际踩过的例子：

- `BbcPublic` 不是当前应使用的命令名
- `bloomberg-latest` 实际并不存在

结论：

- 每次新增命令前，先跑 `opencli list`
- 或直接跑目标命令的 `--help`

### 坑 3：不同内置命令的输出字段并不统一

实际遇到过：

- Bloomberg：`title / summary / link / mediaLinks`
- BBC（早期观察）：`title / description / url`
- BBC（修复后）：补出了 `time`
- Reuters/Twitter：字段形状又不同

结论：

- 不能假设所有新闻命令都天然返回 `title / time / url`
- 新增命令前必须先直接跑一遍 JSON 输出看字段

### 坑 4：`commands.json` 只能列命令，不能做字段映射

这很重要。

`commands.json` 只负责列出：

- section 名
- 命令 argv

它本身不能做下面这些事：

- 把 `link` 改成 `url`
- 把 `publishedAt` 改成 `time`
- 把 `description` 写进最终 markdown

结论：

- 如果是字段不匹配问题，要改 adapter 或改管道脚本
- 不能指望只改 `commands.json` 就解决 schema 问题

### 坑 5：先验证单命令，再验证最小管道

推荐顺序：

1. 先直接跑命令看 JSON：

```bash
opencli <site> <command> --limit 2 --format json
```

2. 再跑最小化管道，只带这个 source：

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_news_pipeline.py --config <mini-config> --out-json /tmp/test.json
```

如果单命令能跑，不代表一定能进管道。

真正要看的是：

- 有没有 `title`
- 有没有 `url` 或至少能被管道映射成 `url`
- 有没有 `time` 或至少能被兼容规则兜底

## 目前建议的维护方式

后续再往这个 skill 里加 OpenCLI 新闻源时，建议按这个流程走：

1. 先确认命令真实存在
2. 直接跑 `--format json` 看字段
3. 判断是否天然符合 `title / url / time`
4. 如果不符合，决定是：
   - 改 adapter
   - 还是扩展管道兼容层
5. 最后再写进 `commands.json`
6. 再跑一次最小管道验证

## 一句话总结

这个 skill 现在的维护重点已经不只是“往 `commands.json` 里加命令”，而是：

- 先确认 OpenCLI 命令真实存在
- 再确认返回字段能进入当前管道
- 如果字段不匹配，就在 adapter 或归一化层补兼容

否则命令虽然能单独跑通，进了 skill 也可能被静默过滤掉。
