#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
bash build.sh
package_version=$(sed -n 's/.*Version *= *"\([^"]*\)".*/\1/p' internal/version/version.go)
package_stage=$(mktemp -d)
trap 'rm -rf "$package_stage"' EXIT
mkdir -p "$package_stage/DEBIAN" "$package_stage/usr/lib/fenpei" "$package_stage/usr/bin" "$package_stage/usr/share/applications" "$package_stage/usr/share/icons/hicolor/scalable/apps" "$package_stage/usr/share/doc/fenpei" dist
install -m 755 bin/fenpei-client "$package_stage/usr/lib/fenpei/fenpei"
install -m 755 packaging/fenpei-client-launcher "$package_stage/usr/bin/fenpei"
install -m 644 packaging/fenpei-client.desktop "$package_stage/usr/share/applications/fenpei.desktop"
install -m 644 packaging/fenpei-client.svg "$package_stage/usr/share/icons/hicolor/scalable/apps/fenpei.svg"
cat > "$package_stage/DEBIAN/control" <<CONTROL
Package: fenpei
Conflicts: fenpei-client
Replaces: fenpei-client
Version: ${package_version}-1
Section: net
Priority: optional
Architecture: amd64
Maintainer: FenPei Maintainers <fenpei@localhost>
Depends: xdg-utils, util-linux
Installed-Size: $(du -sk "$package_stage/usr" | cut -f1)
Description: FenPei unified LAN task application
 One application with role-based server and client workspaces.
 Admin starts the LAN server; operator discovers and processes tasks.
CONTROL
cat > "$package_stage/usr/share/doc/fenpei/README" <<'DOC'
安装后从应用菜单打开“FenPei 文件分发”，或运行 fenpei。
统一登录页面 http://127.0.0.1:18080/login。
服务端账号 admin，客户端账号 pc01，测试密码均为 root。
admin 登录后启动本机服务端（TCP/UDP 8000）；pc01 登录后自动发现服务端。
点击开始工作才领取任务，暂停立即停止当前下载、压缩和上传。
数据、任务池与日志位于 ${XDG_DATA_HOME:-$HOME/.local/share}/fenpei/。
安装不会自动启动，卸载保留用户数据。请退出旧版程序再打开新版。
平台默认为 10.5.66.51，FENPEI_BASE_URL、MINIO_ADDRESS 可覆盖平台和存储地址。
DOC
find "$package_stage" -type d -exec chmod 755 {} +
chmod 644 "$package_stage/DEBIAN/control" "$package_stage/usr/share/doc/fenpei/README"
package_file="dist/fenpei_${package_version}-1_amd64.deb"
dpkg-deb --root-owner-group --build "$package_stage" "$package_file"
sha256sum "$package_file" > "$package_file.sha256"
printf 'Deb 安装包: %s\n' "$PWD/$package_file"
