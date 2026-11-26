.PHONY: help build build-sync build-web build-desktop start stop restart status logs open-web dev-desktop

WEB_PORT ?= 5173
SYNC_WS_PORT ?= 9322

help:
	@echo "qwerty-learner 多端同步常驻管理:"
	@echo "  make build          编译 sync-server 单二进制 + 前端生产包"
	@echo "  make build-desktop  编译全部并打包 Tauri 桌面客户端 (.app)"
	@echo "  make start          常驻启动: 同步服务 + 网页端(生产) + 桌面客户端"
	@echo "  make stop           停止常驻的同步服务与网页端"
	@echo "  make restart        重启常驻服务"
	@echo "  make status         查看三端运行状态"
	@echo "  make logs           实时查看 sync-server 与 web 日志"
	@echo "  make open-web       在浏览器打开常驻网页端"
	@echo "  make dev-desktop    启动常驻同步服务并以开发模式运行桌面端"

build: build-sync build-web

build-sync:
	cd sync-server && go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o sync-server ./cmd/sync-server

build-web:
	yarn build

build-desktop: build-sync build-web
	cd src-tauri && cargo tauri build

start:
	@bash scripts/service-ctl.sh start-all
	@echo "网页端地址: http://127.0.0.1:$(WEB_PORT)"

stop:
	@bash scripts/service-ctl.sh stop-all

restart: stop start

status:
	@bash scripts/service-ctl.sh status

logs:
	@bash scripts/service-ctl.sh logs

open-web:
	@open "http://127.0.0.1:$(WEB_PORT)"

dev-desktop:
	@bash scripts/service-ctl.sh start-sync
	cd src-tauri && cargo tauri dev
