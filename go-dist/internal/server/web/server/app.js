import { mountUpdate } from '/assets/workspace/update.js';
mountUpdate('/api/update');
import { api } from '../js/api.js';
import { $, escape, size, renderSnapshot, renderPool } from './view.js';
let snapshot, active = new Set(), page = 1, refreshing = false, busy = false, preview = null, connected = false;
const pageSize = 50;
function render() {
  const query = $('search').value.trim().toLocaleLowerCase();
  const rows = Object.entries(snapshot?.folders_summary || {}).filter(([name,f]) => `${f.name || name} ${f.source_label} ${f.source_path}`.toLocaleLowerCase().includes(query)).sort((a,b) => (a[1].name || a[0]).localeCompare(b[1].name || b[0],'zh-CN'));
  page = Math.min(page,Math.max(1,Math.ceil(rows.length/pageSize)));
  renderPool(rows,active,page,pageSize);
  if (busy || !connected) $('poolRows').querySelectorAll('[data-remove]').forEach(button => { button.disabled = true; });
  $('scan').disabled = busy || !connected;
  $('pause').disabled = busy || !connected;
  $('browse').disabled = busy || !connected;
  $('sourcePath').disabled = busy;
  $('add').disabled = busy || !connected;
}
async function refresh() {
  if (refreshing) return;
  refreshing = true;
  $('refresh').disabled = true;
  try {
    const [status, events] = await Promise.all([api.status(), api.events()]);
    snapshot = {...status, events: events.events};
    connected = true;
    active = renderSnapshot(snapshot);
    $('connection').textContent = `已连接 · ${new Date().toLocaleTimeString('zh-CN')}`;
    $('error').textContent = status.persistence_error ? `${status.persistence_error}。最近操作尚未可靠保存，服务端正在重试。` : '';
    $('error').hidden = !status.persistence_error;
  } catch (error) {
    connected = false;
    $('connection').textContent = '服务连接中断';
    $('error').textContent = `无法更新状态：${error.message}${snapshot ? '。当前显示上次获取的数据。' : ''}`;
    $('error').hidden = false;
  } finally { refreshing = false; $('refresh').disabled = false; render(); }
}
$('refresh').onclick = refresh;
$('search').oninput = () => { page = 1; render(); };
$('prev').onclick = () => { page--; render(); };
$('next').onclick = () => { page++; render(); };
$('sourcePath').oninput = () => { preview = null; $('add').hidden = true; };
$('sourceForm').onsubmit = async event => {
  event.preventDefault();
  if (busy || !connected) return;
  busy = true; preview = null; $('add').hidden = true; render();
  $('sourceMessage').textContent = '正在扫描目录…';
  try {
    preview = await api.scanSource($('sourcePath').value.trim());
    $('sourcePath').value = preview.path;
    const fresh = preview.folders.filter(f => !f.existing);
    $('sourceMessage').textContent = `发现 ${preview.folders.length} 个文件夹 · ${fresh.length} 个新任务 · ${size(fresh.reduce((n,f) => n+f.total_size,0))}${preview.folders.length ? '' : '。请选择包含任务子文件夹的目录。'}`;
    $('add').hidden = fresh.length === 0;
  } catch (error) { $('sourceMessage').textContent = error.message; }
  finally {busy = false; render();}
};
$('add').onclick = async () => {
  if (busy || !connected || !preview) return;
  busy = true; render(); $('sourceMessage').textContent = '正在加入数据池…';
  try {
    const result = await api.addSource(preview.path);
    preview = null; $('add').hidden = true;
    $('sourceMessage').textContent = `已新增 ${result.added} 个任务，保留 ${result.existing} 个已有任务。`;
  } catch (error) { $('sourceMessage').textContent = error.message; }
  finally {busy = false; await refresh(); render();}
};
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
refresh();
setInterval(() => { if (!document.hidden) refresh(); },2000);

let directory = null, browsing = false;
async function browseDirectory(path) {
  if (browsing) return;
  browsing = true;
  $('chooseDirectory').disabled = true;
  $('directoryUp').disabled = true;
  $('directoryError').textContent = '正在读取文件夹…';
  try {
    const result = await api.directories(path);
    directory = result;
    $('directoryPath').textContent = result.path;
    $('directoryShortcuts').innerHTML = result.shortcuts.map(d => `<button type="button" data-directory="${escape(d.path)}">${escape(d.name)}</button>`).join('');
    $('directoryList').innerHTML = result.directories.map(d => `<button type="button" data-directory="${escape(d.path)}"><span aria-hidden="true">▸</span> ${escape(d.name)}</button>`).join('') || '<p class="empty">该目录下没有子文件夹</p>';
    $('directoryError').textContent = '';
  } catch (error) {
    $('directoryError').textContent = error.message;
  } finally {
    browsing = false;
    $('chooseDirectory').disabled = !directory;
    $('directoryUp').disabled = !directory || directory.parent === directory.path;
  }
}
$('browse').onclick = () => {
  $('directoryDialog').showModal();
  browseDirectory($('sourcePath').value.trim() || directory?.path || '');
};
$('closeDirectory').onclick = () => $('directoryDialog').close();
for (const id of ['directoryShortcuts','directoryList']) $(id).onclick = event => {
  const button = event.target.closest('[data-directory]');
  if (button) browseDirectory(button.dataset.directory);
};
$('directoryUp').onclick = () => { if (directory) browseDirectory(directory.parent); };
$('chooseDirectory').onclick = () => {
  if (!directory || browsing) return;
  $('sourcePath').value = directory.path;
  preview = null; $('add').hidden = true;
  $('sourceMessage').textContent = '已选择目录，点击“扫描目录”查看可加入的数据。';
  $('directoryDialog').close();
  $('scan').focus();
};

$('poolRows').addEventListener('click', async event => {
  const button = event.target.closest('[data-remove]');
  if (!button || button.disabled || busy || !connected) return;
  const folder = button.dataset.remove, item = snapshot?.folders_summary?.[folder];
  if (!item || !confirm(`将“${item.name || folder}”从数据池删除？\n只移除池中记录，保留硬盘上的原始文件。`)) return;
  busy = true; render();
  try {
    await api.removeFolder(folder);
    preview = null; $('add').hidden = true;
    $('sourceMessage').textContent = '已从池子移除，硬盘原文件保留。';
  } catch (error) { alert(error.message); }
  finally { busy = false; await refresh(); render(); }
});

const views = {overview:'运行总览', pool:'数据池管理', workers:'客户端监控', events:'运行记录'};
function selectView(view) {
  if (!views[view]) view = 'overview';
  $('pageTitle').textContent = views[view];
  document.querySelectorAll('[data-view]').forEach(button => {
    const selected = button.dataset.view === view;
    button.classList.toggle('selected', selected);
    if (selected) button.setAttribute('aria-current', 'page'); else button.removeAttribute('aria-current');
  });
  document.querySelectorAll('[data-section]').forEach(section => { section.hidden = !section.dataset.section.split(' ').includes(view); });
}
document.querySelector('nav').onclick = event => {
  const button = event.target.closest('[data-view]');
  if (button) location.hash = button.dataset.view;
};
window.addEventListener('hashchange', () => selectView(location.hash.slice(1)));
selectView(location.hash.slice(1));
$('pause').onclick = async () => {
  if (busy || !connected) return;
  busy = true; render();
  try { await api.pause(!snapshot.paused); await refresh(); }
  catch (error) { $('error').textContent = error.message; $('error').hidden = false; }
  finally { busy = false; render(); }
};
api.me().then(user => { $('account').textContent = `${user.username} · 管理员`; }).catch(() => { location.replace('/login'); });
$('logout').onclick = async () => {
  try { await api.logout(); location.replace('/login'); }
  catch (error) { $('error').textContent = error.message; $('error').hidden = false; }
};
