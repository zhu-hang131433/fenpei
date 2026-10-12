const form = document.getElementById('login'), error = document.getElementById('error'), submit = document.getElementById('submit');
form.onsubmit = async event => {
 event.preventDefault(); submit.disabled = true; error.hidden = true;
 try {
  const response = await fetch('/api/auth/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:form.username.value.trim(),password:form.password.value}),signal:AbortSignal.timeout(10000)});
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || '登录失败');
  location.replace(result.redirect);
 } catch (err) { error.textContent = err.message; error.hidden = false; }
 finally { submit.disabled = false; }
};
