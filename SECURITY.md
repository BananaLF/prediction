# 只读安全模型

## 安全目标

程序只能观察公开市场数据、在本地计算并写入本地 SQLite。即使配置错误或上游响应恶意，也不能获得下单、撤单、余额查询、签名或钱包能力。

## 允许的网络面

| Origin | 允许的方法与路径 |
|---|---|
| `https://gamma-api.polymarket.com` | `GET /markets/keyset` |
| `https://clob.polymarket.com` | `POST /books`（公开批量查询） |
| `https://clob.polymarket.com` | `GET /fee-rate` |
| `https://clob.polymarket.com` | `GET /clob-markets/{condition_id}` |
| `wss://ws-subscriptions-clob.polymarket.com` | `/ws/market` 公开频道 |

基础 URL 只允许无用户名、密码、路径、查询和 fragment 的 HTTP(S) origin。REST 客户端使用 `trust_env=False`，拒绝 cookie、HTTP auth、authorization/proxy authorization、API key、签名和 passphrase 请求头。WebSocket 只允许固定 market 频道。

## 明确禁止

- 下单、批量下单、撤单、撤全部订单或查询订单私有状态；
- 余额、额度、持仓、钱包或账户端点；
- 私钥、助记词、钱包连接、交易签名、EIP-712 或链上交易；
- API key、secret、passphrase、签名、`Authorization` 或 `POLY_*` 凭据；
- 用户私有 WebSocket；
- 任意 shell 命令、`shell=True` 或把市场文本拼入命令；
- 将桌面通知失败解释为证据丢失，或在证据落库前通知。

CI 的 `tests/integration/test_read_only_surface.py` 只扫描生产 Python 包，避免文档和安全测试中的禁止词造成假阳性；它检查禁止端点字面量、认证注入、钱包/签名库、私有 WS 和 shell 调用，并钉住允许的公开入口。适配器单元测试使用 `httpx.MockTransport` 验证公开请求契约。

## 威胁与控制

| 威胁 | 控制 |
|---|---|
| 环境中意外存在凭据/代理 | HTTP 不信任环境；构造和每次请求前拒绝凭据头、cookie 和 auth |
| 恶意 condition/token 注入 URL | condition 严格校验并 URL 编码；token 仅作为 JSON/查询参数 |
| 上游超大或畸形响应 | 响应大小上限、严格 JSON/类型/唯一性/映射验证、精确 Decimal |
| WS 丢包或乱序 | 有界队列；溢出/时序回退使 epoch 失效；周期公开 REST 批次按 condition/token/tick/最小量原子校准，失败继续失效；正式结论仍重新获取两次独立 REST |
| Gamma/CLOB 标识符混淆 | Gamma 数字 `market_id` 只作目录身份；CLOB `/books.market` 明确解析为 `condition_id`，引擎只按 condition 绑定盘口 |
| 供应链加入交易能力 | 依赖版本范围、锁定/审计安装物、只读静态测试；禁止钱包和签名依赖 |
| 通知命令注入 | 使用参数数组调用固定 `/usr/bin/osascript`，不使用 shell；文本清理和长度限制 |
| 把桌面通知误当可靠事件流 | SQLite/report 轮询是事实来源；持久单次尝试不保证送达；仅对过期不确定租约做崩溃回收，可能产生可审计重复 |
| 规则文件路径/覆盖攻击 | 拒绝目录 traversal；安全 relation ID；原子独占创建；冲突不覆盖 |
| SQLite 篡改或泄漏 | 专用 OS 用户、目录 0700、数据库/备份 0600、最小化备份访问、校验哈希 |
| WAL 不完整备份 | 停写后使用 SQLite `.backup`，恢复前 integrity check |
| 依赖漏洞或来源污染 | 固定可信索引、保存 lock/hash、定期扫描 CVE；更新必须跑完整测试和只读边界测试 |

## 凭据泄漏处置

本项目不需要任何凭据。若日志、配置、环境或异常中发现疑似 key、secret、passphrase、私钥或助记词：

1. 立即停止进程并隔离日志/数据库副本，限制权限，不在工单中粘贴原文。
2. 在对应平台撤销/轮换凭据；把该主机视为可能泄漏源。
3. 清理 shell history、进程监管配置和日志转发目标；保留脱敏时间线。
4. 查明凭据为何进入只读环境，增加阻断测试后再恢复。

不要把“程序声称没有使用凭据”等同于“凭据没有泄漏”。

## 文件系统和备份

运行目录、规则目录、数据库和备份应归专用用户所有。建议目录权限 `0700`，配置、数据库、WAL、备份和日志 `0600`。备份加密、限制保留期并定期恢复演练。不要把真实数据库、日志或配置提交到 Git；不要删除 WAL 来“修复”数据库。
