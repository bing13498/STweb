# ST / *ST 股票本地看板

## 功能

- 通过 `AkShare` 拉取全部 `ST` 和 `*ST` 股票代码、名称
- 补充总市值、流通市值、最新股东人数、行业等字段
- 写入本地 `SQLite` 数据库: `data/stocks.db`
- 通过本地 Web 页面展示数据库内容
- 支持页面内手动刷新本地数据库

## 运行

```bash
python init_db.py
python app.py
```

启动后访问:

- `http://127.0.0.1:8000`

## 说明

- 股票数据来自 `ak.stock_zh_a_st_em()`
- 当东方财富 ST 板块不可用时，会回退到新浪 A 股实时行情并在本地筛选 `ST` / `*ST`
- `data/stocks.db` 不再纳入 Git 版本控制，仅保留本地使用
- `python init_db.py` 可单独初始化数据库结构和 `data/notices` 目录
- `python app.py` 首次启动时会自动初始化数据库结构
- 首次运行后，如果页面里还没有数据，点击“刷新本地数据库”即可触发抓取并写库
