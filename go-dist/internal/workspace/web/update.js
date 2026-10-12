// 两个工作台共用更新提示，外部检查由后端缓存，网络失败不影响任务。
export function mountUpdate(endpoint) {
 const panel=document.getElementById('appUpdate');
 if (!panel) return;
 panel.innerHTML='<div><strong>应用更新</strong><p data-update-message role="status">正在检查版本…</p></div><div class="update-actions"><a data-update-download target="_blank" rel="noopener noreferrer" hidden>下载安装包</a><a data-update-release target="_blank" rel="noopener noreferrer" hidden>更新说明</a><button type="button" data-update-check>检查更新</button></div>';
 const message=panel.querySelector('[data-update-message]');
 const button=panel.querySelector('[data-update-check]');
 let busy=false, poll;
 const safe=link=>{try {const u=new URL(link);return u.protocol==='https:' && u.hostname==='github.com' && !u.username && !u.password && u.pathname.includes('/releases/');} catch{return false;}};
 async function check(force=false) {
  if (busy) return;
  busy=true;button.disabled=true;
  clearTimeout(poll);
  try {
   const response=await fetch(endpoint+(force?'?force=1':''),{cache:'no-store',signal:AbortSignal.timeout(6000)});
   if (!response.ok) throw new Error('暂时无法连接更新服务');
   const s=await response.json();
   message.textContent=s.checking?`当前 v${s.current_version} · 正在检查…`:s.error?`当前 v${s.current_version} · ${s.error}`:s.available?`发现 v${s.latest_version}（当前 v${s.current_version}）。安装前请等待任务结束并退出应用。`:`当前 v${s.current_version} · 已是最新正式版`;
   panel.classList.toggle('update-available',Boolean(s.available));
   for (const [key,selector] of [['download_url','[data-update-download]'],['release_url','[data-update-release]']]) {
    const link=panel.querySelector(selector);
    link.hidden=!s.available || !safe(s[key]);
    if (!link.hidden) link.href=s[key]; else link.removeAttribute('href');
   }
   if (s.checking) poll=setTimeout(()=>check(),2000);
  } catch(error) {message.textContent=`更新检查暂不可用：${error.message}。不影响当前任务。`;}
  finally {busy=false;button.disabled=false;}
 }
 button.onclick=()=>check(true);
 document.addEventListener('visibilitychange',()=>{if (!document.hidden) check();});
 check();setInterval(()=>{if (!document.hidden) check();},60000);
}
