export const $ = id => document.getElementById(id);
export const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function size(bytes) {
  let n = Number(bytes) || 0, i = 0;
  const units = ['B','KB','MB','GB','TB'];
  while (n >= 1024 && i < 4) { n /= 1024; i++; }
  return `${n.toLocaleString('zh-CN', {maximumFractionDigits:1})} ${units[i]}`;
}
const phases = {downloading:'下载到客户端',compressing:'压缩文件',uploading:'上传到平台',uploading_zip:'上传到平台',starting:'准备上传',notifying:'等待平台确认',retrying:'退避等待重试',retry_exhausted:'重试已达上限',completed:'处理完成',success:'上传完成',skipped:'已跳过'};
function stage(f) {
  const phase = f.phase || f.upload_status;
  const key = {downloading:'download_pct',compressing:'compress_pct',uploading:'upload_pct'}[phase];
  return {label: phases[phase] || '准备处理', pct: key ? Math.max(0,Math.min(100,Number(f[key]) || 0)) : null};
}
function badge(label, kind = '') { return `<span class="badge ${kind}">${escape(label)}</span>`; }
export function renderSnapshot(s) {
  $('dispatchState').textContent = s.paused ? '分发已暂停' : '正在分发';
  $('dispatchState').classList.toggle('paused', s.paused);
  $('dispatchHint').textContent = s.paused ? '停止分配新任务，已领取的任务继续处理。恢复后按等待队列分配。' : '可领取的任务按客户端等待顺序分配，已领取的任务继续处理。';
  $('pause').textContent = s.paused ? '恢复分发' : '暂停分发';
  $('eventList').innerHTML = (s.events || []).map(event => `<article class="event"><time>${escape(event.time)}</time><span class="badge">${escape(event.type)}</span><p>${escape(event.message)}</p></article>`).join('') || '<p class="empty">暂无运行记录，加入数据或连接客户端后会记录活动。</p>';
  const folders = s.folders_summary || {};
  const workers = Object.entries(s.workers || {});
  const online = workers.filter(([,w]) => w.online).sort((a,b) => a[1].label.localeCompare(b[1].label,'zh-CN'));
  const working = online.filter(([,w]) => !w.waiting && (w.active_folder || w.current_folder));
  $('total').textContent = s.total_folders;
  $('volume').textContent = `文件夹 · ${size(Object.values(folders).reduce((n,f) => n + f.total_size,0))}`;
  $('waiting').textContent = workers.filter(([,w]) => w.waiting).length;
  $('working').textContent = working.filter(([,w]) => !w.paused).length;
  $('connected').textContent = online.length;
  $('workCount').textContent = `${working.length} 个任务${working.some(([,w]) => w.paused) ? ` · ${working.filter(([,w]) => w.paused).length} 个已暂停` : ''}`;
  $('clientCount').textContent = `${online.length} 台在线`;
  $('workList').innerHTML = working.map(([,w]) => {
    const name = w.active_folder || w.current_folder, f = folders[name] || {}, p = stage(f);
    if (w.paused) p.label = `已暂停 · ${p.label}`;
    return `<article class="task"><div class="task-head"><strong>${escape(f.name || name)}</strong>${badge(w.label)}</div><small>${escape(f.source_label || '数据池')} · ${size(f.total_size)}</small><div class="stage"><span>${escape(p.label)}${f.retry_count ? ` · 第 ${Number(f.retry_count)} 次重试` : ''}</span><span>${p.pct === null ? '处理中' : `${p.pct}%`}</span></div><div class="track ${p.pct === null ? 'indeterminate' : ''}" ${p.pct === null ? '' : `role="progressbar" aria-label="${escape(p.label)}" aria-valuenow="${p.pct}" aria-valuemin="0" aria-valuemax="100"`}><div class="fill" ${p.pct === null ? '' : `style="width:${p.pct}%"`}></div></div></article>`;
  }).join('') || '<p class="empty">暂无工作中的任务</p>';
  $('clientList').innerHTML = online.map(([,w]) => `<div class="client"><div><strong>${escape(w.label)}</strong><small>${escape(w.ip)}</small></div>${badge(w.paused ? '已暂停' : w.waiting ? '排队等待' : (w.active_folder || w.current_folder) ? '工作中' : '准备就绪', w.waiting ? 'dim' : 'ok')}</div>`).join('') || '<p class="empty">暂无在线客户端</p>';
  $('poolInfo').textContent = `${(s.sources || []).length} 个来源 · ${Object.values(folders).filter(f => f.status === 'pending' && !f.skip).length} 个待领取 · ${s.uploaded} 个已上传${s.paused ? ' · 分发已暂停' : ''}`;
  $('version').textContent = `FenPei v${s.version}`;
  return new Set(working.map(([,w]) => w.active_folder || w.current_folder));
}
export function renderPool(rows, active, page, pageSize) {
  $('poolRows').innerHTML = rows.slice((page-1)*pageSize,page*pageSize).map(([name,f]) => {
    let label = '待领取', kind = 'dim';
    if (f.upload_status === 'success') { label = f.upload_message === '手动标记已完成' ? '人工标记完成' : '已上传'; kind = 'ok'; }
    else if (active.has(name)) { label = stage(f).label; kind = ''; }
    else if (/failed|error|no_jz|no_task|retry_exhausted/.test(f.upload_status || '')) {label = '上传异常'; kind = 'warn';}
    else if (f.skip) label = '已跳过';
    else if (f.status === 'completed') label = '已下载';
    else if (f.status === 'in_progress') {label = '已领取'; kind = '';}
    else if (!f.available) {label = '来源不可读取'; kind = 'warn';}
    return `<tr><td>${escape(f.name || name)}</td><td>${escape(f.source_label || '—')}<small title="${escape(f.source_path)}">${escape(f.source_path)}</small></td><td>${Number(f.file_count) || 0}</td><td>${size(f.total_size)}</td><td title="${escape(f.upload_message)}">${badge(label,kind)}</td><td><button type="button" class="remove-folder" data-remove="${escape(name)}" ${f.can_delete ? '' : 'disabled'} title="${f.can_delete ? '仅从池子移除，保留原文件' : '客户端正在处理，暂不能删除'}">删除</button></td></tr>`;
  }).join('');
  $('poolEmpty').hidden = rows.length > 0;
  $('poolEmpty').textContent = $('search').value.trim() ? '没有匹配的数据' : '池子里暂无数据，可通过上方入口加入硬盘目录。';
  const pages = Math.max(1,Math.ceil(rows.length/pageSize));
  $('pages').hidden = pages <= 1;
  $('pageInfo').textContent = `${page} / ${pages} 页 · ${rows.length} 个文件夹`;
  $('prev').disabled = page <= 1;
  $('next').disabled = page >= pages;
}
