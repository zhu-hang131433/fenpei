#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
bash build-deb.sh
release_version=$(sed -n 's/.*Version *= *"\([^"]*\)".*/\1/p' internal/version/version.go)
release_stage=$(mktemp -d)
trap 'rm -rf "$release_stage"' EXIT
mkdir -p "$release_stage/fenpei"
install -m 755 bin/fenpei-client "$release_stage/fenpei/fenpei"
install -m 755 bin/fenpei-server "$release_stage/fenpei/fenpei-server"
install -m 644 packaging/platform.example.json "$release_stage/fenpei/platform.example.json"
cp ../README.md "$release_stage/fenpei/README.md"
tar -czf "dist/fenpei_${release_version}_linux_amd64.tar.gz" -C "$release_stage" fenpei
(cd dist && sha256sum "fenpei_${release_version}-1_amd64.deb" "fenpei_${release_version}_linux_amd64.tar.gz" > "fenpei_${release_version}_SHA256SUMS.txt")
printf 'Release 构建完成: %s/dist\n' "$PWD"
