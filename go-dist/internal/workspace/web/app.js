import { mountUpdate } from './update.js';
const $ = id => document.getElementById(id);
const base = document.body.dataset.agentBase;
mountUpdate(`${base}/api/local/update`);
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const phases = {downloading:'正在下载',compressing:'正在压缩',uploading:'正在上传',uploading_zip:'正在上传',starting:'准备上传',notifying:'等待平台确认',retrying:'等待重试',retry_exhausted:'重试已达上限',disk_wait:'等待磁盘空间',completed:'任务完成'};
let status, refreshing = false, busy = false, lastLogs = '', reachable = false;
function size(value) { let n = Number(value) || 0, i = 0; const units = ['B','KB','MB','GB','TB']; while (n >= 1024 && i < 4) {n/=1024;i++;} return `${n.toLocaleString('zh-CN',{maximumFractionDigits:1})} ${units[i]}`; }
async function request(path, body) {
 const response = await fetch(`${base}/api/local/${path}`, {method: body === undefined ? 'GET' : 'POST',headers:body === undefined ? {} : {'Content-Type':'application/json'},body:body === undefined ? undefined : JSON.stringify(body),signal:AbortSignal.timeout(6000),cache:'no-store'});
 const data = await response.json(); if (!response.ok) throw new Error(data.error || `请求失败（${response.status}）`); return data;
}
function controls() {
 $('start').disabled = busy || !reachable || !status?.connected || (status.running && !status.paused);
 $('start').textContent = status?.paused ? '继续工作' : '开始工作';
 $('pause').disabled = busy || !reachable || !status?.running || status.paused;
}
function render(s) {
 const work = s.work || {}, p = work.progress || {}, active = Boolean(p.folder);
 $('connection').textContent = s.connected ? '服务端已连接' : s.server ? '正在重新连接服务端' : '正在自动发现服务端';
 $('state').textContent = s.paused ? '工作已暂停' : s.running ? (active ? '正在工作' : '等待任务') : s.connected ? '准备就绪' : '正在寻找服务端';
 $('hint').textContent = s.paused ? '当前下载、压缩和上传已暂停，点击继续工作恢复。' : s.running ? '自动领取并处理任务，暂无可领取任务时保持等待。' : '自动连接同一局域网内的服务端，点击开始工作后执行任务。';
 $('server').textContent = s.server || '尚未发现服务端'; $('label').textContent = work.label || '';
 $('task').textContent = active ? p.folder : s.running ? '等待服务端分配任务' : '还没有领取任务';
 $('phase').textContent = s.paused ? `已暂停${active ? ` · ${phases[p.phase] || '处理中'}` : ''}` : active ? phases[p.phase] || '准备处理' : s.running ? '排队等待' : '准备就绪';
 $('counts').textContent = `已完成 ${work.completed || 0} · 异常 ${work.failed || 0}`;
 for (const [name,key] of [['download','download_pct'],['compress','compress_pct'],['upload','upload_pct']]) {
  const pct = Math.max(0,Math.min(100,Number(p[key]) || 0));
  $(name).value = active ? pct : 0; $(`${name}Text`).textContent = active ? `${pct}%` : '—';
 }
 $('mode').textContent = s.upload ? '下载后自动上传' : '仅下载'; $('dest').textContent = s.dest;
 $('bytes').textContent = active ? `${size(Math.min(work.downloaded_bytes || 0,work.total_bytes || 0))} / ${size(work.total_bytes)}` : '等待任务';
 const logs = s.logs || [], signature = JSON.stringify(logs);
 if (signature !== lastLogs) {
  const nearBottom = $('logs').scrollHeight - $('logs').scrollTop - $('logs').clientHeight < 60;
  $('logs').innerHTML = logs.map(log => `<div class="log-row"><time>${escape(log.time)}</time><span>${escape(log.message)}</span></div>`).join('') || '<p>暂无日志。点击开始工作后会记录任务执行情况。</p>';
  if (nearBottom || !lastLogs) $('logs').scrollTop = $('logs').scrollHeight;
  lastLogs = signature;
 }
 $('error').textContent = s.error || ''; $('error').hidden = !s.error;
 $('updated').textContent = `更新于 ${new Date().toLocaleTimeString('zh-CN')}`;
 controls();
}
async function refresh() {
 if (refreshing) return; refreshing = true;
 try { status = await request('status'); reachable = true; render(status); }
 catch (error) {
  reachable = false; $('connection').textContent = '本机客户端未连接';
  $('error').textContent = base ? '请先在这台电脑上运行下载的 fenpei-client；客户端会自动寻找服务端并打开工作页面。' : `本机客户端连接中断：${error.message}。请保持客户端程序运行。`;
  $('error').hidden = false; controls();
 } finally {refreshing = false;}
}
for (const action of ['start','pause']) $(action).onclick = async () => {
 if (busy) return; busy = true; controls();
 try { await request(action,{}); await refresh(); }
 catch(error) { $('error').textContent = error.message; $('error').hidden = false; }
 finally { busy = false; controls(); }
};
document.addEventListener('visibilitychange',() => {if (!document.hidden) refresh();});
refresh();setInterval(() => {if (!document.hidden) refresh();},1000);

fetch('/api/auth/me', {cache:'no-store'}).then(r => {
 if (r.ok) $('logout').hidden = false;
}).catch(() => {});
$('logout').onclick = async () => {
 const response = await fetch('/api/auth/logout', {method:'POST'});
 if (response.ok) location.replace('/login');
};
