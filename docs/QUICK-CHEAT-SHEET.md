# 最简命令速查表

只保留最常用的 `./bin/*` 命令。需要更多背景时，回到 README 或项目说明。

## 准备环境

```console
python -m venv .venv
.venv/bin/pip install -e '.[test]'
./bin/doctor
```

## 查看帮助

```console
./bin/help
./bin/help --verbose
./bin/predmarket --help
```

## 同步市场

```console
./bin/predmarket sync-markets
./bin/predmarket sync-markets --limit 100 --max-pages 2
```

## 扫描机会

```console
./bin/predmarket scan-once
./bin/predmarket scan-once --limit 100
./bin/predmarket scan-once --json
```

## 持续监听

```console
./bin/predmarket watch
./bin/predmarket watch --max-connections 3 --max-events 500
```

## 看结果

```console
./bin/predmarket --json report --limit 100
./bin/predmarket replay OPPORTUNITY_ID
./bin/predmarket validate-opportunity OPPORTUNITY_ID
```

## 一句话记忆

- `sync-markets`：同步市场目录并入库；
- `scan-once`：离线扫一轮；
- `watch`：在线监听再复核；
- `report`：汇总统计；
- `replay`：回放证据；
- `validate-opportunity`：精确验证单个机会。

