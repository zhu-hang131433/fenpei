// 所有服务通信集中在这里；页面不依赖服务端内部实现。
export async function request(path, body, timeout = 10000) {
  const response = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(timeout),
    cache: 'no-store',
  });
  if (response.status === 401) { location.replace('/login'); throw new Error('登录已过期'); }
  const data = await response.json();
  if (!response.ok || data.ok === false || data.error) throw new Error(data.error || `请求失败（${response.status}）`);
  return data;
}
export const api = {
  events: () => request('/api/events/list'),
  me: () => request('/api/auth/me'),
  logout: () => request('/api/auth/logout', {}),
  pause: paused => request('/api/pause', {paused}),
  scanSource: (path, label) => request('/api/sources/scan', { path, label }, 600000),
  addSource: (path, label) => request('/api/sources', { path, label }, 600000),
  directories: path => request(`/api/directories?path=${encodeURIComponent(path || '')}`),
  removeFolder: folder => request('/api/folders/remove', { folder }),
  status: () => request('/api/status'),
};
