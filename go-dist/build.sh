#!/usr/bin/env bash
# 构建 fenpei-server / fenpei-client 静态二进制（适配 Ubuntu 20.04/22.04/24.04, amd64）
set -e
cd "$(dirname "$0")"

if [ -d "$HOME/tools/go/bin" ]; then
  export PATH="$HOME/tools/go/bin:$PATH"
fi
export CGO_ENABLED=0 GOOS=linux GOARCH=amd64
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

release_repo="${FENPEI_UPDATE_REPO:-${GITHUB_REPOSITORY:-}}"
if [[ -n "$release_repo" && ! "$release_repo" =~ ^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$ ]]; then
  echo "更新仓库必须是 owner/repo 格式" >&2
  exit 1
fi
build_flags="-s -w"
if [[ -n "$release_repo" ]]; then
  build_flags="$build_flags -X fenpei/internal/update.Repository=$release_repo"
fi
mkdir -p bin
go build -trimpath -ldflags="$build_flags" -o bin/fenpei-server ./cmd/fenpei-server
go build -trimpath -ldflags="$build_flags" -o bin/fenpei-client ./cmd/fenpei-client
echo "构建完成:"
ls -la bin/
file bin/fenpei-server bin/fenpei-client | sed 's/^/  /'
