# opencli-sequential-news-zh README

这份 README 不是给模型执行用的，主要是给人看，方便后续维护这个 skill 时快速回答两个问题：

1. 这个 skill 目前到底改过哪些 OpenCLI 自带命令
2. 过去用 OpenCLI 时踩过哪些坑

## 目前这个 skill 改过的 OpenCLI 内置命令

以下只记录相对于原始 skill 配置有明确变更的内置命令。

### 1. `bbc_news`

- 旧命令：`opencli BbcPublic news --limit 10 --format json`
- 现命令：`opencli bbc news --limit 10 --format json`
- 原因：
  - `BbcPublic` 不是当前可用的内置命令名
  - 当前环境里可用的是 `bbc news`

### 2. `bloomberg_main`

- 新增命令：`opencli bloomberg main --limit 15 --format json`
- section：`bloomberg_main`
- 原因：
  - 之前误以为有 `bloomberg-latest` 之类的命令
  - 实际内置可用的是 `opencli bloomberg main`

### 3. `sina_china_news`

- 新增命令：`opencli sina china-news --limit 10 --format json`
- section：`sina_china_news`
- 原因：
  - 用于补充中文新闻源
  - 直接命令验证时已确认返回字段包含 `title / url / time`

#### `sina` 命令是怎么新增出来的

这次不是直接靠 `opencli generate` 一步生成成功，而是先失败，再手工补出来的。

实际过程：

1. 先尝试：

```bash
opencli generate https://news.sina.com.cn/china/ --goal "获取前10条新闻"
```

结果：

- 首轮失败
- 加 `-v` 复查后仍失败
- 探索阶段一直是：
  - `Endpoints: 0 total, 0 API`
  - `Capabilities: 0`

2. 确认这不是“页面打不开”，而是“生成器没探测到接口”

- 直接 `curl https://news.sina.com.cn/china/` 可以拿到页面 HTML
- 说明问题不在访问失败，而在自动探测阶段没有识别出真正的数据源

3. 改用手工排查页面数据来源

从页面 HTML 里发现了两个关键线索：

- 页面正文列表容器是 `feed_cont`
- 页面加载了 `feed-news.js`

同时页面内还有一段配置：

- `pageid: 121`
- `firstTab.lid: 1356`

4. 顺着前端脚本继续定位真实接口

检查 `feed-news.js` 后，确认它使用的是新浪 feed API。

最后验证成功的真实接口是：

```text
https://feed.mix.sina.com.cn/api/roll/get?pageid=121&lid=1356&num=10&page=1
```

这个接口返回的 JSON 里已经直接包含：

- `title`
- `url`
- `media_name`
- `ctime` / `mtime` / `intime`

也就是说，这个页面本身是可以稳定做成公开新闻命令的，只是 `opencli generate` 没自动识别出来。

5. 手工新增 adapter

最终新增了文件：

- [china-news.js](/Users/x/.opencli/clis/sina/china-news.js)

设计选择：

- site：`sina`
- name：`china-news`
- strategy：`public`
- browser：`false`
- 参数：
  - `--limit`
  - `--page`

输出字段：

- `rank`
- `title`
- `source`
- `time`
- `url`

6. 时间字段是怎么补出来的

新浪这个接口没直接给格式化后的时间字符串，但给了 Unix 时间戳：

- 优先用 `ctime`
- 其次兜底 `mtime`
- 再兜底 `intime`

最后在 adapter 里把时间统一格式化成 `Asia/Shanghai` 下的：

```text
YYYY/MM/DD HH:MM:SS
```

7. 单命令验证

实际验证命令：

```bash
opencli sina china-news --limit 10 --format json
```

确认返回结果已经稳定包含：

- `title`
- `source`
- `time`
- `url`

#### 这次新增 `sina` 命令带来的经验

这次很典型，适合以后碰到“新闻首页 generate 失败”时直接复用：

- `opencli generate` 失败，不等于站点不能做
- 很多新闻站首页并不是 SSR 直接把列表写进 HTML，而是前端脚本二次请求 feed API
- 这类站点要重点找：
  - 页面里的容器 ID
  - 前端 feed 脚本
  - `pageid` / `lid` / tab 配置
- 一旦能定位到公开 feed API，通常比硬抓 DOM 更稳定

可以把这次方法总结成一个小模板：

1. 先跑 `opencli generate`
2. 如果失败，再跑 `-v`
3. 如果还是 0 endpoint，就直接看 HTML
4. 重点搜：
   - `feed`
   - `pageid`
   - `lid`
   - `api`
5. 找到真实接口后，优先手工写 `public` adapter
6. 最后再把命令接入 `commands.json`

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
- `sina_china_news`
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
