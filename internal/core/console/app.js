const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const icon = name => `<svg class="icon" aria-hidden="true"><use href="#i-${name}"/></svg>`;
const count = n => Number.isFinite(Number(n)) ? Number(n).toLocaleString('zh-CN') : '—';
const bytes = n => n == null ? '—' : n >= 1073741824 ? `${(n / 1073741824).toFixed(2)} GiB` : `${(n / 1048576).toFixed(1)} MiB`;
const date = value => !value || !Number.isFinite(new Date(value).getTime()) || new Date(value).getFullYear() < 2000 ? '—' : new Intl.DateTimeFormat('zh-CN', {timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(value));
const duration = ms => ms == null ? '—' : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`;
const titles = {overview:'运行概览',accounts:'账号池',logs:'调用日志',images:'图片管理',settings:'系统设置'};
const groups = {concurrency:['图片生成并发','从任务执行、图片下载到解码和返回，分别控制。'],accounts:['账号校验','控制账号校验速度，以及可用性信息的复用时间。'],timeouts:['超时与重试','明确等待边界，兼顾成功率与请求耗时。'],output:['结果与存储','控制结果返回、图片大小和数据库连接占用。'],access:['访问与密钥','保持管理员登录，管理用于图片接口的调用密钥。']};
const state = {page:'overview',group:'concurrency',demo:false,connected:false,sessionVersion:0,metrics:null,settings:[],limits:[],draft:{},saved:'',samples:[],fetching:false,timer:null,toastTimer:null,lists:{accounts:{cursors:[''],page:0,next:'',items:[],version:0},logs:{cursors:[''],page:0,next:'',items:[],version:0}}};


const storageState={version:0,busy:false,policies:{},jobs:{},timer:null};
const imageState={cursors:[''],page:0,next:'',version:0};
let resultsVersion=0,resultsKey='',resultsAt=0,resultsBusy=false;

async function adminFetch(path,options={}) {
 const version=state.sessionVersion;
 const response=await fetch(path,{...options,headers:{'Content-Type':'application/json',...options.headers},credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(options.timeout||15000)});
 if(version!==state.sessionVersion)throw new Error('登录状态已变化，请重试。');
 if(response.status===401&&state.connected&&!(path==='/admin/session'&&options.method==='POST'))expireSession('登录已失效，可能已在其他浏览器登录，请重新登录。');
 return response;
}
async function api(path,options={}) {
 const response=await adminFetch(path,options);
 let data;try{data=await response.json();}catch{throw new Error(`服务返回了无法解析的响应（${response.status}）`);}
 if(!response.ok){const error=new Error(data.error?.message||`请求失败（${response.status}）`);error.code=response.status;throw error;}
 return data;
}

function toast(message) {
  $('#toast').textContent = message; $('#toast').classList.remove('hidden');
  clearTimeout(state.toastTimer); state.toastTimer = setTimeout(() => $('#toast').classList.add('hidden'), 3500);
}
function setText(id,text) { $(`#${id}`).textContent = text; }
function showConnection(){
 if(state.connected){showDetail('<h2>管理员已登录</h2><p>刷新页面会保持登录。在其他浏览器登录后，此处的会话会失效。</p><button class="button" id="manage-access">访问与密钥</button><button class="button" id="session-logout">退出登录</button>','ADMIN SESSION');$('#manage-access').onclick=()=>{$('#detail-dialog').close();navigate('settings');selectGroup('access');};$('#session-logout').onclick=logoutAdmin;return;}
 $('#connect-error').textContent='';if(!$('#connect-dialog').open)$('#connect-dialog').showModal();
}
function updateConnectionUI(error = '') {
  const active = Boolean(state.connected || state.demo);
  document.body.classList.toggle('connected',active && !error);
  $('#demo-notice').classList.toggle('hidden',!state.demo);
  $('#mode-badge').textContent = error ? '连接异常' : state.demo ? '界面演示' : state.connected ? '实时连接' : '尚未连接';
  $('#mode-badge').className = `badge ${error ? 'failed' : state.demo ? 'safety' : state.connected ? 'success' : 'neutral'}`;
  setText('service-state',error ? '连接中断 · 正在显示上次数据' : state.demo ? '界面演示 · 示例数据' : state.connected ? '服务已连接' : '等待连接服务');
  setText('footer-state',error ? '采样失败' : state.demo ? '示例数据' : state.connected ? '服务已连接' : '未连接服务');
  $('#footer-dot').style.background = error ? 'var(--coral)' : active ? 'var(--mint)' : '#acafa5';
  $('#connection-error').classList.toggle('hidden',!error);
  $('#connection-error').textContent=error;
}
function navigate(page, updateURL = true) {
  if (!titles[page]) page = 'overview';
  state.page = page;
  $$('.page').forEach(el => el.classList.toggle('hidden',el.id !== `page-${page}`));
  $$('.nav-item').forEach(el => {const current=el.dataset.page === page;el.classList.toggle('active',current);if(current)el.setAttribute('aria-current','page');else el.removeAttribute('aria-current');});
  setText('breadcrumb-page',titles[page]); document.title = `${titles[page]} · Image Core`;
  if(updateURL && location.hash !== `#${page}`) history.replaceState(null,'',`#${page}`);
  if(page === 'accounts' || page === 'logs') loadList(page);
 if(page==='logs')loadKeyFilters();
  if(page === 'settings') renderSettings();
 if(page==='images')loadImages();
 clearTimeout(storageState.timer);if(page==='images'||page==='logs')loadStorage(true);
 if(page==='accounts')refreshJobs();
}
function metricSnapshot() {
  const tick=Math.floor(Date.now()/5000);
  return {succeeded:2496,failed:4,safety_rejected:18,active_requests:328+Math.round(Math.sin(tick)*22),queued_requests:16,worker_limit:1000,queue_limit:1000,persistence_errors:0,account_pool:{total_accounts:1269,enabled_accounts:1246,disabled_accounts:23,used_slots:328,available_slots:918,total_slots:1246},runtime:{heap_alloc_bytes:457179136,goroutines:1832},log_active:1,log_queue_waiting:6,log_queue_limit:2048,log_queue_full:0,log_writers:2,thumbnails:{active:1,waiting:3,limit:1},upstream:{pow_active:2,pow_waiting:4,pow_limit:4,download_active:73,download_waiting:24,download_limit:100,validation_active:6,validation_waiting:2,validation_limit:10,verification_active:8,verification_limit:50,download_bytes:6786048328}};
}
function renderMetrics(m) {
  const p=m.account_pool||{},u=m.upstream||{},r=m.runtime||{},t=m.thumbnails||{};

  const total=Number(m.succeeded||0)+Number(m.failed||0),rate=total ? (100*m.succeeded/total).toFixed(2) : '—';

  $('#stat-active').innerHTML=`${count(m.active_requests)}<small>/ ${count(m.worker_limit)}</small>`;
  setText('stat-queued',`等待 ${count(m.queued_requests)}`);
  $('#hero-active').innerHTML=`${count(m.active_requests)}<small> 个任务</small>`;$('#hero-slots').innerHTML=`${count(p.available_slots)}<small> 个</small>`;
  [['pool-enabled',p.enabled_accounts],['pool-disabled',p.disabled_accounts],['pool-used',p.used_slots],['pool-free',p.available_slots]].forEach(([id,value])=>setText(id,count(value)));
  setText('runtime-heap',bytes(r.heap_alloc_bytes));setText('runtime-go',count(r.goroutines));setText('runtime-download',bytes(u.download_bytes));setText('runtime-errors',count((m.persistence_errors||0)+(u.account_persistence_errors||0)));
  setText('footer-workers',`任务 ${count(m.active_requests)} / ${count(m.worker_limit)}`);setText('footer-download',`下载 ${count(u.download_active)} / ${count(u.download_limit)}`);setText('footer-decode',`解码 ${count(u.validation_active)} / ${count(u.validation_limit)}`);
  const waits=[m.queued_requests,u.download_waiting,u.validation_waiting,u.verification_waiting,u.manual_verification_waiting,u.replenishment_waiting,u.hot_maintenance_waiting,u.cold_maintenance_waiting,m.response_waiting,u.pow_waiting,t.waiting,m.log_queue_waiting];
  const caps=[['任务执行',m.active_requests,m.worker_limit,'var(--coral)'],['图片下载',u.download_active,u.download_limit,'var(--purple)'],['图片解码',u.validation_active,u.validation_limit,'var(--mint)'],['前台校验',u.verification_active,u.verification_limit,'var(--amber)'],['手动校验',u.manual_verification_active,u.manual_verification_limit,'var(--mint)'],['热池补充',u.replenishment_active,u.replenishment_limit,'var(--coral)'],['热池维护',u.hot_maintenance_active,u.hot_maintenance_limit,'var(--amber)'],['冷池维护',u.cold_maintenance_active,u.cold_maintenance_limit,'var(--purple)'],['结果返回',m.response_active,m.response_limit,'var(--purple)'],['PoW 计算',u.pow_active,u.pow_limit,'var(--amber)'],['缩略图生成',t.active,t.limit,'var(--mint)'],['日志写入',m.log_active,m.log_writers,'var(--coral)']];
  $('#capacity-list').innerHTML=caps.map(([label,active,limit],i)=>`<div class="capacity-item"><div class="capacity-caption"><span>${label} <small>等待 ${count(waits[i])}</small></span><strong>${count(active)} <small>/ ${count(limit)}</small></strong></div><div class="capacity-track"><div class="capacity-fill" data-bar="${i}"></div></div></div>`).join('');
  caps.forEach(([,active,limit,color],i)=>{const bar=$(`[data-bar="${i}"]`);bar.style.width=`${Math.min(100,Math.max(0,100*(active||0)/(limit||1)))}%`;bar.style.background=color;});
  $('#account-summary').innerHTML=`<span class="summary-chip">账号总数 <strong>${count(p.total_accounts)}</strong></span><span class="summary-chip"><i class="status-dot"></i>已启用 <strong>${count(p.enabled_accounts)}</strong></span><span class="summary-chip">已停用 <strong>${count(p.disabled_accounts)}</strong></span><span class="summary-chip">热池 <strong>${count(p.hot_accounts)}</strong></span><span class="summary-chip">冷池 <strong>${count(p.cold_accounts)}</strong></span><span class="summary-chip">刷新成功 / 失败 <strong>${count(u.account_refresh_succeeded)} / ${count(u.account_refresh_failed)}</strong></span><span class="summary-chip">空闲槽位 <strong>${count(p.available_slots)}</strong></span>`;
  drawChart();
}
function drawChart() {
  const samples=state.samples;
  const w=720,h=190,left=34,right=8,top=9,bottom=26,plotH=h-top-bottom,plotW=w-left-right;
  const max=Math.max(10,...samples.map(p=>Math.max(p.active,p.waiting)))*1.18;
  const x=i=>left+(samples.length>1 ? i/(samples.length-1) : 0)*plotW;
  const y=value=>top+plotH-value/max*plotH;
  const grid=Array.from({length:4},(_,i)=>{const value=max*(3-i)/3,yy=y(value);return `<line x1="${left}" y1="${yy}" x2="${w-right}" y2="${yy}" stroke="#eeeee7" stroke-dasharray="3 5"/><text x="0" y="${yy+3}">${Math.round(value)}</text>`;}).join('');
  const line=key=>samples.map((p,i)=>`${i?'L':'M'} ${x(i).toFixed(2)} ${y(p[key]).toFixed(2)}`).join(' ');
  const area=samples.length ? `${line('active')} L ${x(samples.length-1)} ${h-bottom} L ${left} ${h-bottom} Z` : '';
  const labels=samples.length ? [0,Math.floor((samples.length-1)/2),samples.length-1].map((i,j)=>`<text x="${x(i)}" y="${h-3}" text-anchor="${j===0?'start':j===2?'end':'middle'}">${new Date(samples[i].time).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false})}</text>`).join('') : '';
  $('#concurrency-chart').innerHTML=`<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img" aria-label="实时并发趋势"><defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#e96b56" stop-opacity=".13"/><stop offset="100%" stop-color="#e96b56" stop-opacity="0"/></linearGradient></defs>${grid}<path d="${area}" fill="url(#chart-fill)"/><path d="${line('active')}" fill="none" stroke="#e96b56" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/><path d="${line('waiting')}" fill="none" stroke="#a28acf" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>${labels}</svg>${samples.length<2?'<div class="chart-empty">采样后在这里显示实时趋势</div>':''}`;
  if(samples.length) setText('last-sample',`最近采样 ${new Date(samples.at(-1).time).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false})}`);
}
async function refreshMetrics() {
  if(state.fetching||document.hidden||(!state.demo&&!state.connected))return;
  state.fetching=true;$('#refresh-button').classList.add('loading');
  try {
    const m=state.demo?metricSnapshot():await api('/admin/metrics');
    state.metrics=m;state.samples.push({time:Date.now(),active:m.active_requests||0,waiting:m.upstream?.download_waiting||0});if(state.samples.length>60)state.samples.shift();
    renderMetrics(m);renderExtraMetrics(m);updateConnectionUI();refreshResults();
  } catch(error){updateConnectionUI(error.message);}finally{state.fetching=false;$('#refresh-button').classList.remove('loading');}
}
function resetTimer(){clearInterval(state.timer);const seconds=Number($('#refresh-interval').value);if(seconds)state.timer=setInterval(refreshMetrics,seconds*1000);}
const demoDefinitions = [
 ['CORE_POW_CONCURRENCY','PoW 计算并发','concurrency',0,'0 表示自动使用可用 CPU 数的一半（至少 1）；正数指定同时计算上限。与图片解码及缩略图共享 CPU，实际容量见运行概览。',['CORE_IMAGE_WORKERS','CORE_VALIDATION_CONCURRENCY','CORE_THUMBNAIL_CONCURRENCY','CORE_TOTAL_TIMEOUT']],
 ['CORE_THUMBNAIL_CONCURRENCY','缩略图生成并发','output',1,'首次读取未缓存图片时，同时生成缩略图的数量；已有缓存不占名额。提高会增加 CPU 与解码内存占用，独立于生图下载和解码槽位。',['CORE_VALIDATION_CONCURRENCY','CORE_POW_CONCURRENCY','CORE_MAX_IMAGE_BYTES']],
 ['CORE_LOG_WRITERS','日志写入线程','output',2,'后台批量写入调用日志的并行数。提高可减少日志积压，但会增加 PostgreSQL 写入竞争；不占图片执行槽位。',['CORE_LOG_QUEUE_SIZE','CORE_DB_CONNECTIONS']],
 ['CORE_LOG_QUEUE_SIZE','日志等待队列','output',2048,'图片请求完成后等待写入日志的主队列和溢出队列容量各为此值。主队列满时先进入溢出队列，只有两级队列都满才同步写入，减少日志落库对图片响应的阻塞；不会静默丢日志。',['CORE_LOG_WRITERS','CORE_DB_CONNECTIONS']],
 ['CORE_IMAGE_WORKERS','同时执行的图片任务','concurrency',1000,'最多同时处理的生图请求数。一个请求的多张图片在请求内依次生成。',['CORE_ACCOUNT_SLOTS','CORE_IMAGE_QUEUE_SIZE']],
 ['CORE_IMAGE_QUEUE_SIZE','图片任务等待队列','concurrency',1000,'执行名额占满后的等待容量；执行与排队名额都占满时返回 429。',['CORE_IMAGE_WORKERS','CORE_TOTAL_TIMEOUT']],
 ['CORE_ACCOUNT_SLOTS','每个账号的生成槽位','concurrency',1,'单个账号同时承接的生成数。可用账号数量乘以该值决定账号侧总槽位。',['CORE_IMAGE_WORKERS']],
 ['CORE_DOWNLOAD_CONCURRENCY','图片下载并发','concurrency',100,'同时执行的图片下载数。影响连接、带宽竞争和下载排队；与解码并发不相乘。',['CORE_IMAGE_WORKERS','CORE_VALIDATION_CONCURRENCY','CORE_DOWNLOAD_TIMEOUT']],
 ['CORE_VALIDATION_CONCURRENCY','图片解码并发','concurrency',10,'同时完整解码校验的图片数。影响 CPU 和内存；解码等待也消耗下载超时。',['CORE_DOWNLOAD_CONCURRENCY','CORE_MAX_IMAGE_BYTES','CORE_DOWNLOAD_TIMEOUT']],
 ['CORE_RESPONSE_CONCURRENCY','图片结果返回并发','concurrency',32,'限制结果读取与返回操作，尤其影响 Base64 响应的内存和带宽占用。',['CORE_RESPONSE_FORMAT','CORE_IMAGE_WORKERS']],
 ['CORE_VERIFY_CONCURRENCY','账号校验并发','accounts',50,'同时校验或续期的账号数量。同一账号的重复校验会合并等待。',['CORE_AVAILABILITY_TTL','CORE_METADATA_TIMEOUT']],
 ['CORE_HOT_POOL_SIZE','基础热池数量','accounts',500,'热池基础规模，使用冷池账号时自动扩张，不限制生成并发。',['CORE_HOT_IDLE']],
 ['CORE_HOT_IDLE','热池闲置退出时间','accounts',86400,'闲置超过该时间后缩回基础热池；不移除正在使用的账号。',['CORE_HOT_POOL_SIZE'],'秒'],
 ['CORE_HOT_MAINTENANCE_INTERVAL','热池维护间隔','accounts',1800,'按每个账号最近一次成功校验或生图活性判断。',['CORE_VERIFY_CONCURRENCY'],'秒'],
 ['CORE_COLD_MAINTENANCE_INTERVAL','冷池维护间隔','accounts',86400,'冷池低频保活，独立使用一组刷新名额。',['CORE_VERIFY_CONCURRENCY'],'秒'],
 ['CORE_AVAILABILITY_TTL','账号可用性有效期','accounts',900,'近期校验或生图成功任一时间在有效期内即可复用；AT 过期检查仍会执行。',['CORE_VERIFY_CONCURRENCY'],'秒'],
 ['CORE_TOTAL_TIMEOUT','图片请求总预算','timeouts',120,'普通请求执行和切号共用预算；生成流硬超时后的恢复豁免总预算，仍受阶段超时与切号次数限制。',['CORE_SSE_TIMEOUT','CORE_POLL_TIMEOUT','CORE_SWITCH_RETRIES'],'秒'],
 ['CORE_SSE_TIMEOUT','上游生成流硬超时','timeouts',80,'单次生成流的最长等待时间，到期进入结果恢复与补查流程。',['CORE_POLL_TIMEOUT','CORE_SWITCH_RETRIES'],'秒'],
 ['CORE_POLL_TIMEOUT','图片结果补查超时','timeouts',10,'生成流未取得完整图片标识时，限制补查等待。',['CORE_SSE_TIMEOUT','CORE_SWITCH_RETRIES'],'秒'],
 ['CORE_DOWNLOAD_TIMEOUT','单次图片下载超时','timeouts',15,'获得下载名额后开始计时，包含下载传输与解码等待；下载名额排队受外围预算控制。',['CORE_DOWNLOAD_CONCURRENCY','CORE_VALIDATION_CONCURRENCY','CORE_DOWNLOAD_RETRIES'],'秒'],
 ['CORE_METADATA_TIMEOUT','账号与元数据请求超时','timeouts',30,'限制账号信息与元数据子请求，一次完整校验还受外围预算控制。',['CORE_VERIFY_CONCURRENCY'],'秒'],
 ['CORE_SWITCH_RETRIES','最大切号重试次数','timeouts',2,'首个账号失败后的最大切换次数。2 表示最多尝试 3 个账号。',['CORE_TOTAL_TIMEOUT','CORE_DOWNLOAD_RETRIES'],'次'],
 ['CORE_DOWNLOAD_RETRIES','图片下载重试次数','timeouts',1,'同一张图片首次下载失败后的追加尝试次数，仍失败时再进入切号逻辑。',['CORE_DOWNLOAD_TIMEOUT','CORE_SWITCH_RETRIES'],'次'],
 ['CORE_DB_CONNECTIONS','PostgreSQL 业务连接池','output',32,'业务读写连接数。另有 1 个实例租约连接，总连接数为该值加 1。',['CORE_IMAGE_WORKERS']],
 ['CORE_MAX_IMAGE_BYTES','单张图片大小上限','output',67108864,'拒绝超过上限的图片下载。默认 64 MiB，需结合解码并发评估内存。',['CORE_VALIDATION_CONCURRENCY'],'字节'],
 ['CORE_PUBLIC_URL','图片公开访问地址','output','','留空按当前请求的 IP、域名和端口生成图片 URL。填写域名默认 HTTPS；HTTPS 反代请配置完整公开地址。',['CORE_RESPONSE_FORMAT'],'','text'],
 ['CORE_RESPONSE_FORMAT','默认结果格式','output','url','URL 返回图片地址；Base64 将图片嵌入响应，会增加数据量与内存占用。',['CORE_ALLOW_USER_RESPONSE_FORMAT','CORE_RESPONSE_CONCURRENCY'],'','select'],
 ['CORE_ALLOW_USER_RESPONSE_FORMAT','允许调用方指定返回格式','output',true,'开启后接受调用方格式，并兼容 base64、b64_json 等别名。',['CORE_RESPONSE_FORMAT'],'','boolean']
];
function demoSettings(){return {settings:demoDefinitions.map(([key,label,group,value,description,related,unit,kind])=>({key,label,group,value,default:value,description,related,unit,kind:kind||'number',min:['CORE_POW_CONCURRENCY','CORE_IMAGE_QUEUE_SIZE','CORE_SWITCH_RETRIES','CORE_DOWNLOAD_RETRIES'].includes(key)?0:1})),limits:[{label:'单次请求图片数量',value:'1–4 张',description:'当前接口固定范围；一个请求内依次生成。'},{label:'请求体大小',value:'1 MiB',description:'当前 HTTP 接口固定上限。'}]};}
function installSettings(data) {
  state.settings=data.settings;state.limits=data.limits;state.revision=data.revision??0;
  state.draft=Object.fromEntries(state.settings.map(s=>[s.key,s.saved_value??s.value]));
  state.saved=JSON.stringify(state.draft);renderSettings();
}
function validSetting(setting,value){if(setting.kind==='number')return typeof value==='number'&&Number.isSafeInteger(value)&&value>=(setting.min??1);if(setting.kind==='boolean')return typeof value==='boolean';if(setting.kind==='text')return typeof value==='string';return value==='url'||value==='b64_json';}
function settingsValid(){for(const setting of state.settings){if(!validSetting(setting,state.draft[setting.key])){toast(`请检查「${setting.label}」的值`);navigate('settings');selectGroup(setting.group);$(`[data-setting="${setting.key}"]`)?.focus();return false;}}return true;}
function updateDraftStatus(message){const dirty=JSON.stringify(state.draft)!==state.saved;$('.settings-savebar').classList.toggle('dirty',dirty);setText('draft-state',message||(dirty?'有尚未保存的修改':state.demo?'演示设置未修改':'设置已与服务同步'));}
function selectGroup(group){if(!groups[group])return;state.group=group;renderSettings();}
function renderSettings(){
  const [title,description]=groups[state.group];setText('settings-group-title',title);setText('settings-group-description',description);
  $$('[data-group]').forEach(el=>{el.classList.toggle('active',el.dataset.group===state.group);const total=state.settings.filter(s=>s.group===el.dataset.group).length;if(total)$('span',el).textContent=String(total).padStart(2,'0');});
  const access=state.group==='access';$('.settings-panel').classList.toggle('hidden',access);$('.fixed-limits').classList.toggle('hidden',access);$('.settings-savebar').classList.toggle('hidden',access);$('#access-panel').classList.toggle('hidden',!access);if(access){renderAccess();return;}
  if(!state.settings.length){$('#settings-fields').innerHTML='<div class="empty-cell">连接服务或打开界面演示后查看配置。</div>';return;}
  const labels=Object.fromEntries(state.settings.map(s=>[s.key,s.label]));
  $('#settings-fields').innerHTML=state.settings.filter(s=>s.group===state.group).map(s=>{
    const value=state.draft[s.key];
    const control=s.kind==='boolean'?`<label class="toggle"><input type="checkbox" data-setting="${s.key}" aria-label="${escapeHTML(s.label)}" ${value?'checked':''}><span class="toggle-track"></span></label>`:s.kind==='select'?`<select data-setting="${s.key}" aria-label="${escapeHTML(s.label)}"><option value="url" ${value==='url'?'selected':''}>URL 地址</option><option value="b64_json" ${value==='b64_json'?'selected':''}>Base64</option></select>`:s.kind==='text'?`<input type="text" value="${escapeHTML(value)}" data-setting="${s.key}" aria-label="${escapeHTML(s.label)}" placeholder="留空自动识别访问地址" spellcheck="false">`:`<input type="number" min="${s.min??1}" step="1" value="${escapeHTML(value)}" data-setting="${s.key}" aria-label="${escapeHTML(s.label)}">${s.unit?`<span class="unit">${escapeHTML(s.unit)}</span>`:''}`;
    return `<div class="setting-row${s.kind==='text'?' setting-text-row':''}" id="row-${s.key}"><div><div class="setting-label">${escapeHTML(s.label)}<span class="default-note">默认 ${escapeHTML(s.kind==='text'&&s.default===''?'自动识别':s.default)}</span></div><p class="setting-description">${escapeHTML(s.description)}</p><div class="setting-key">${s.key}</div><div class="apply-note">当前生效：${escapeHTML(s.kind==='text'&&s.value===''?'自动识别':s.value)}${s.unit||''} · ${s.apply==='restart'?'重启后生效':'在线生效'}${s.pending?' · 有待生效更改':''}</div><div class="setting-related"><span>关联</span>${s.related.map(key=>`<button class="related-chip" data-related="${key}">${escapeHTML(labels[key]||key)}</button>`).join('')}</div></div><div class="setting-control">${control}</div></div>`;
  }).join('');
  $('#fixed-limits-content').innerHTML=state.limits.map(s=>`<div class="fixed-limit"><div><span>${escapeHTML(s.label)}</span><strong>${escapeHTML(s.value)}</strong></div><p>${escapeHTML(s.description)}</p></div>`).join('');
  updateDraftStatus();
}
function envText(){return ['# Image Core 配置导出','# 将这些项合并到现有 .env；保留原有数据库地址和密钥。','# 服务端保存的同名设置优先于环境变量。','',...state.settings.flatMap(s=>[`# ${s.label}：${s.description}`,`${s.key}=${state.draft[s.key]}${s.unit==='秒'?'s':''}`,''])].join('\n');}
function activateDemo(){
 clearTimeout(state.jobsTimer);
  state.connected=false;state.demo=true;state.samples=Array.from({length:40},(_,i)=>({time:Date.now()-(39-i)*5000,active:210+Math.round(50*Math.sin(i*.19)+27*Math.sin(i*.63))+i*2,waiting:Math.max(0,16+Math.round(10*Math.sin(i*.41)))}));
  $('#admin-key').value='';$('#connect-dialog').close();installSettings(demoSettings());resetLists();updateConnectionUI();refreshMetrics();navigate(state.page);resetTimer();
}
function resetLists(){storageState.version++;storageState.busy=false;storageState.policies={};storageState.jobs={};imageState.version++;imageState.cursors=[''];imageState.page=0;imageState.next='';for(const kind of ['images','logs'])delete $('#'+kind+'-retention-form').dataset.dirty;for(const list of Object.values(state.lists)){list.cursors=[''];list.page=0;list.next='';list.items=[];list.version++;}}
function demoAccount(i){return {id:`acc_${String(i+1).padStart(5,'0')}`,email:`studio.${['willow','atlas','olive','amber','nova','luna','cedar','moss'][i%8]}${i+1}@example.com`,score:Math.max(25,100-(i*7)%76),enabled:i%9!==8,token_tail:['Hk9mQ2','8vR3nP','d6BxW1','4zL7aE'][i%4],has_refresh_token:i%4!==0,verified_at:new Date(Date.now()-(i+1)*76000).toISOString(),success_at:new Date(Date.now()-(i+1)*121000).toISOString()};}
function demoRequest(i){return {id:`task_${['a4f90c','b3e78d','c6a21f','d9b42e'][i%4]}${String(1042+i)}7b82`,created_at:new Date(Date.now()-(i+1)*117000).toISOString(),model:'gpt-image-2',is_async:i%3===1,response_format:i%5===3?'b64_json':'url',status:i%17===9?'failed':i%13===6?'safety_rejected':'succeeded',duration_ms:Math.round((25+17*Math.abs(Math.sin(i*1.9)))*1000),switch_count:i%7===4?1:0};}
function listParams(kind){return {search:$(`#${kind}-search`).value.trim(),status:$(`#${kind}-status`).value,...(kind==='logs'?{api_key_id:$('#logs-key').value,async:$('#logs-async').value,response_format:$('#logs-format').value,from:$('#logs-from').value,to:$('#logs-to').value}:{})};}
function demoList(kind){const l=state.lists[kind],{search,status,...filters}=listParams(kind);let all=Array.from({length:kind==='accounts'?48:65},(_,i)=>kind==='accounts'?demoAccount(i):demoRequest(i));all=all.filter(row=>(!search||(row.email||row.id).toLowerCase().includes(search.toLowerCase())||row.id.includes(search))&&(!status||(kind==='accounts'?(status==='enabled')===row.enabled:row.status===status)));if(kind==='logs')all=all.filter(row=>(!filters.async||String(row.is_async)===filters.async)&&(!filters.response_format||row.response_format===filters.response_format)&&(!filters.from||new Date(row.created_at)>=new Date(filters.from+'T00:00:00+08:00'))&&(!filters.to||new Date(row.created_at)<new Date(new Date(filters.to+'T00:00:00+08:00').getTime()+86400000)));return {items:all.slice(l.page*20,l.page*20+20),next_cursor:all.length>(l.page+1)*20?`demo-${l.page+1}`:''};}
async function loadList(kind,reset=false){
  const list=state.lists[kind];if(reset){list.cursors=[''];list.page=0;list.next='';}
  const version=++list.version;
  if(!state.demo&&!state.connected){renderEmpty(kind,'连接服务后查看真实记录，或打开界面演示。');return;}
  $(`#${kind}-body`).innerHTML=`<tr><td colspan="8" class="empty-cell">正在读取…</td></tr>`;
  try {
    const {search,...filters}=listParams(kind),params=new URLSearchParams({limit:'20',q:search,...filters,cursor:list.cursors[list.page]||''});
    const data=state.demo?demoList(kind):await api(`/admin/${kind==='accounts'?'accounts':'requests'}?${params}`);
    if(version!==list.version)return;list.items=data.items;list.next=data.next_cursor||'';renderList(kind);
  } catch(error){if(version!==list.version)return;list.items=[];list.next='';renderEmpty(kind,error.message);}finally{if(version===list.version)updatePagination(kind);}
}
function renderEmpty(kind,message){$(`#${kind}-body`).innerHTML=`<tr><td colspan="8" class="empty-cell">${icon(kind==='accounts'?'users':'log')}${escapeHTML(message)}</td></tr>`;updatePagination(kind);}
function statusBadge(status){const cls=status==='succeeded'?'success':status==='safety_rejected'?'safety':'failed';const label=status==='succeeded'?'生成成功':status==='safety_rejected'?'安全审核拒绝':'生成失败';return `<span class="badge ${cls}">${status==='succeeded'?icon('check'):''}${label}</span>`;}
function renderList(kind){
  const list=state.lists[kind];if(!list.items.length){renderEmpty(kind,'没有符合筛选条件的记录。');return;}
  const colors=['mint','purple','amber','coral'];
  $(`#${kind}-body`).innerHTML=list.items.map((row,i)=>kind==='accounts'?`<tr><td><div class="account-cell"><span class="account-avatar ${colors[i%4]}">${escapeHTML((row.email||row.id).slice(0,2).toUpperCase())}</span><span><span class="table-primary">${escapeHTML(row.email||'未填写邮箱')}</span><span class="table-subtitle mono">${escapeHTML(row.id)}</span></span></div></td><td><span class="badge ${row.enabled?'success':'neutral'}">${row.enabled?'已启用':'已停用'}</span><span class="table-subtitle">${row.hot?'热池':'冷池'} · 额度 ${row.quota==null?'未知':count(row.quota)}</span></td><td><span class="score-cell"><strong>${count(row.score)}</strong><span class="capacity-track"><span class="capacity-fill mint-score" data-score="${i}"></span></span></span></td><td class="mono">··· ${escapeHTML(row.token_tail)}</td><td><span class="badge ${row.has_refresh_token?'success':'neutral'}">${row.has_refresh_token?'已配置':'未配置'}</span></td><td>${escapeHTML(date(row.verified_at))}</td><td><button class="text-button" data-account="${i}">详情 ${icon('chevron')}</button></td></tr>`:`<tr><td><span class="table-primary">${escapeHTML(row.model)}</span><span class="table-subtitle mono">${escapeHTML(row.id)}</span></td><td class="time-cell"><span>${escapeHTML(date(row.started_at))}</span><br><span class="muted">${escapeHTML(date(row.finished_at||row.created_at))}</span></td><td><span class="table-primary">${escapeHTML(row.api_key_name||'历史未记录')}</span><span class="badge ${row.is_async?'coral-badge':'neutral'}">${row.is_async?'异步':'同步'}</span></td><td class="mono">${escapeHTML(row.response_format==='b64_json'?'Base64':row.response_format.toUpperCase())}</td><td>${statusBadge(row.status)}</td><td class="mono">${duration(row.duration_ms)}<span class="table-subtitle">排队 ${duration(row.queue_ms)}</span></td><td>${count(row.switch_count)} 次</td><td><button class="text-button" data-request="${escapeHTML(row.id)}">详情 ${icon('chevron')}</button></td></tr>`).join('');
  if(kind==='accounts') $$('[data-score]').forEach(el=>{el.style.width=`${Math.min(100,Math.max(0,list.items[Number(el.dataset.score)].score))}%`;el.style.background='var(--mint)';el.style.display='block';});
  updatePagination(kind);
}
function updatePagination(kind){const list=state.lists[kind];$(`#${kind}-prev`).disabled=list.page===0;$(`#${kind}-next`).disabled=!list.next;setText(`${kind}-page`,`第 ${list.page+1} 页`);}
function showDetail(html,eyebrow){delete $('#detail-content').dataset.jobId;setText('detail-eyebrow',eyebrow);$('#detail-content').innerHTML=html;if(!$('#detail-dialog').open)$('#detail-dialog').showModal();}
function accountDetail(i){const a=state.lists.accounts.items[i];if(!a)return;showDetail(`<h2>账号详情</h2><p>${escapeHTML(a.email||a.id)}</p><div class="detail-grid">${[['状态',a.enabled?'已启用':'已停用'],['热池状态',a.hot?'热池':'冷池'],['图片额度',a.quota==null?'未知':count(a.quota)],['额度恢复时间',date(a.quota_reset_at)],['账号评分',a.score],['Token 尾号',`··· ${a.token_tail}`],['Refresh Token',a.has_refresh_token?'已配置':'未配置'],['最近校验成功',date(a.verified_at)],['最近远程刷新',date(a.last_refresh_at)],['最近刷新错误',a.last_refresh_error||'无'],['刷新退避至',date(a.next_refresh_at)],['最近生图成功',date(a.success_at)],['冷却至',date(a.available_at)],['账号 ID',a.id]].map(([label,value])=>`<div class="detail-item"><span>${label}</span><strong>${escapeHTML(value)}</strong></div>`).join('')}</div><p>使用「完整导出」下载全部凭据、扩展元数据和归并记录。</p><div class="account-actions"><button class="button" data-manage="verify" data-id="${escapeHTML(a.id)}">立即校验</button><button class="button" data-manage="toggle" data-id="${escapeHTML(a.id)}" data-enabled="${a.enabled?'false':'true'}">${a.enabled?'停用账号':'启用账号'}</button><button class="button danger-button" data-manage="delete" data-id="${escapeHTML(a.id)}">删除账号</button></div>`,'ACCOUNT DETAILS');}
async function requestDetail(id){
  showDetail('<h2>日志详情</h2><p>正在读取切号过程…</p>','REQUEST DETAILS');
  try {
    let row;
    if(state.demo){row=state.lists.logs.items.find(r=>r.id===id);row={...row,attempts:Array.from({length:1+row.switch_count},(_,i)=>({email:`studio.${i?'olive':'willow'}@example.com`,duration_ms:row.duration_ms/(1+row.switch_count),started_at:row.created_at,code:i<row.switch_count?'upstream_timeout':row.status==='failed'?'image_result_uncertain':row.status==='safety_rejected'?'content_policy_violation':'',stages_ms:{'账号校验':124,'上游生成流':28300,'图片下载':1238,'图片校验排队':34,'图片完整解码':279}})),error:row.status==='failed'?{code:'image_result_uncertain',message:'上游生成流超时，补查仍未取得完整图片。'}:null};}
    else row=await api(`/admin/requests/${encodeURIComponent(id)}`);
    if(!$('#detail-dialog').open)return;
    const knownStages={pow_solve:'PoW 计算',sse:'上游生成流',download:'图片下载',account_verify:'账号校验',download_wait:'下载排队',validation_wait:'解码排队'};
    showDetail(`<h2>日志详情</h2><p class="mono">${escapeHTML(row.id)}</p><p class="small muted">调用 Key：${escapeHTML(row.api_key_name||'历史未记录')} <span class="mono">${escapeHTML(row.api_key_id||'')}</span></p><div class="detail-meta">${statusBadge(row.status)}<span class="badge neutral">${row.is_async?'异步':'同步'}</span><span class="badge neutral">${escapeHTML(row.response_format)}</span><span class="badge neutral">${duration(row.duration_ms)}</span></div><p class="small muted">开始 ${escapeHTML(date(row.started_at))} · 生成完成 ${escapeHTML(date(row.finished_at||row.created_at))} · 排队 ${duration(row.queue_ms)}</p>${row.error?`<div class="detail-error">${escapeHTML(row.error.message)}<div class="mono">${escapeHTML(row.error.code)}</div></div>`:''}<h3>账号尝试过程</h3><p class="small muted">${(row.attempts||[]).length} 次尝试 · 切号 ${(row.attempts||[]).filter((a,i,all)=>a.code&&i<all.length-1).length} 次</p><div class="attempt-timeline">${(row.attempts||[]).map((a,i)=>`<article class="attempt"><h3>${i+1}. ${escapeHTML(a.email||a.account_id)}</h3><p>${escapeHTML(date(a.started_at))} · 使用 ${duration(a.duration_ms)}</p>${a.code?`<span class="badge ${a.code==='content_policy_violation'?'safety':'failed'}">${escapeHTML(a.code)}</span>`:'<span class="badge success">生成成功</span>'}<div class="stages-grid">${Object.entries(a.stages_ms||{}).map(([key,value])=>`<div><span>${escapeHTML(knownStages[key]||key)}</span><span>${duration(value)}</span></div>`).join('')}</div></article>`).join('')||'<p>当前日志未记录账号尝试。</p>'}</div>${bodySections(row)}`,'REQUEST DETAILS');
    installBodySections(id);
  }catch(error){if($('#detail-dialog').open)showDetail(`<h2>读取失败</h2><p>${escapeHTML(error.message)}</p>`,'REQUEST DETAILS');}
}

$$('.nav-item').forEach(button=>button.addEventListener('click',()=>navigate(button.dataset.page)));
window.addEventListener('hashchange',()=>navigate(location.hash.slice(1),false));
document.addEventListener('click',event=>{
  const go=event.target.closest('[data-go]');if(go)navigate(go.dataset.go);
  const close=event.target.closest('[data-close]');if(close)$(`#${close.dataset.close}`).close();
  const account=event.target.closest('[data-account]');if(account)accountDetail(Number(account.dataset.account));
  const request=event.target.closest('[data-request]');if(request)requestDetail(request.dataset.request);
  const related=event.target.closest('[data-related]');if(related){const setting=state.settings.find(s=>s.key===related.dataset.related);if(setting){selectGroup(setting.group);const row=$(`#row-${setting.key}`);row.scrollIntoView({behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth',block:'center'});$(`[data-setting="${setting.key}"]`).focus({preventScroll:true});}}
});
$$('[data-group]').forEach(button=>button.addEventListener('click',()=>selectGroup(button.dataset.group)));
$('#settings-fields').addEventListener('input',event=>{const key=event.target.dataset.setting;if(!key)return;const setting=state.settings.find(s=>s.key===key);const value=setting.kind==='boolean'?event.target.checked:setting.kind==='number'?(event.target.value===''?null:Number(event.target.value)):event.target.value;state.draft[key]=value;event.target.classList.toggle('invalid',!validSetting(setting,value));updateDraftStatus();});
$('#reset-settings').addEventListener('click',()=>{state.draft=Object.fromEntries(state.settings.map(s=>[s.key,s.default]));renderSettings();toast('已填入默认值，点击保存并生效后应用。');});
$('#save-draft').addEventListener('click',async()=>{
 if(!state.settings.length||!settingsValid())return;
 if(state.demo){toast('演示模式无法修改服务，请先连接管理员。');return;}
 const button=$('#save-draft');button.disabled=true;
 try{
  const data=await api('/admin/config',{method:'PUT',body:JSON.stringify({revision:state.revision,values:state.draft})});
  installSettings(data);toast(data.settings.some(s=>s.pending)?'设置已保存，数据库连接池更改将在重启后生效。':'设置已保存并生效。');
 }catch(error){toast(error.message);if(error.code===409){showDetail('<h2>设置已在其他页面修改</h2><p>当前修改已保留。重新加载服务端配置后，再确认需要调整的项。</p><button class="button primary" id="reload-config">重新加载服务配置</button>','SETTINGS CONFLICT');$('#reload-config').onclick=async()=>{try{installSettings(await api('/admin/config'));$('#detail-dialog').close();}catch(e){toast(e.message);}};}}
 finally{button.disabled=false;}
});
$('#copy-settings').addEventListener('click',async()=>{if(!state.settings.length||!settingsValid())return;try{await navigator.clipboard.writeText(envText());toast('配置已复制，请合并到现有 .env。');}catch{showDetail('<h2>配置内容</h2><p>浏览器未允许访问剪贴板，可手动复制以下内容。</p><div class="detail-json"><pre></pre></div>','CONFIGURATION');$('#detail-content pre').textContent=envText();}});
$('#export-settings').addEventListener('click',()=>{if(!state.settings.length||!settingsValid())return;const url=URL.createObjectURL(new Blob([envText()],{type:'text/plain;charset=utf-8'}));const link=document.createElement('a');link.href=url;link.download='image-core-settings.env';link.click();setTimeout(()=>URL.revokeObjectURL(url),5000);toast('配置已导出。服务端已保存的设置优先于环境变量。');});
$('#connection-button').addEventListener('click',showConnection);$('#profile-button').addEventListener('click',showConnection);$('#connect-from-demo').addEventListener('click',showConnection);$('#demo-button').addEventListener('click',activateDemo);
$('#connect-form').addEventListener('submit',async event=>{
 event.preventDefault();const button=$('button[type="submit"]',event.target);button.disabled=true;$('#connect-error').textContent='';
 try{await api('/admin/session',{method:'POST',body:JSON.stringify({key:$('#admin-key').value})});$('#admin-key').value='';await connectSession();$('#connect-dialog').close();toast('登录成功，刷新页面会保持登录。');}
 catch(error){$('#connect-error').textContent=error.message;}finally{button.disabled=false;}
});
$('#refresh-button').addEventListener('click',()=>{if(!state.demo&&!state.connected)showConnection();else refreshMetrics();});$('#refresh-interval').addEventListener('change',resetTimer);
for(const kind of ['accounts','logs']){
  let debounce;
  $(`#${kind}-search`).addEventListener('input',()=>{clearTimeout(debounce);debounce=setTimeout(()=>loadList(kind,true),350);});
  $(`#${kind}-search`).addEventListener('keydown',event=>{if(event.key==='Enter'){clearTimeout(debounce);loadList(kind,true);}});
  $(`#${kind}-status`).addEventListener('change',()=>loadList(kind,true));
  $(`#${kind}-refresh`).addEventListener('click',()=>loadList(kind,true));
  $(`#${kind}-prev`).addEventListener('click',()=>{if(state.lists[kind].page>0){state.lists[kind].page--;loadList(kind);}});
  $(`#${kind}-next`).addEventListener('click',()=>{const l=state.lists[kind];if(l.next){l.page++;l.cursors[l.page]=l.next;loadList(kind);}});
}
$('#help-button').addEventListener('click',()=>{showDetail(`<h2>接口使用说明</h2><p>生成与图片编辑都支持同步和异步调用。调用方重试时可发送 Idempotency-Key，避免重复生成。</p><div class="detail-grid"><div class="detail-item"><span>创建图片</span><strong class="mono">POST /v1/images/generations</strong></div><div class="detail-item"><span>图片编辑（multipart）</span><strong class="mono">POST /v1/images/edits</strong></div><div class="detail-item"><span>异步结果查询</span><strong class="mono">GET /v1/images/tasks/{task_id}</strong></div></div><div class="detail-json"><pre>Authorization: Bearer &lt;图片调用密钥&gt;\n\n{\n  "model": "gpt-image-2",\n  "prompt": "一只在窗边晒太阳的橘猫",\n  "n": 1,\n  "async": true,\n  "response_format": "url"\n}</pre></div><p>编辑上传 image/image[]，可选 mask/mask[]（PNG）；遮罩透明区域需要编辑，不透明区域保留。支持 prompt、n、async、response_format 等表单字段。上传大小与参考图数量可在设置调整。Key 并发 0 表示不限，排队和异步执行均计入在途名额。相同 Key、路径、Idempotency-Key 的相同请求复用任务，修改参数或图片返回 409；幂等保证受日志和图片保留策略约束。</p><p>管理员首次登录使用 CORE_ADMIN_KEY；后续可在「设置 → 访问与密钥」修改。图片调用 Key 也在该处管理，首次启动会导入 CORE_API_KEY。管理界面的可在线调整项可直接保存；数据库连接池更改在重启后生效。</p>`,'API REFERENCE');});
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refreshMetrics();});
window.addEventListener('beforeunload',event=>{if(JSON.stringify(state.draft)!==state.saved&&state.settings.length){event.preventDefault();event.returnValue='';}});
setText('current-date',new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'long',day:'numeric',weekday:'long'}).format(new Date()));
drawChart();navigate(location.hash.slice(1)||'overview',false);updateConnectionUI();

async function refreshJobs(){
 clearTimeout(state.jobsTimer);
 if(state.demo||!state.connected||state.page!=='accounts'||document.hidden)return;
 try{
  const data=await api('/admin/jobs?scope=accounts');
  if(state.demo||!state.connected)return;
  const wasRunning=new Set((state.jobs||[]).filter(j=>j.status==='running').map(j=>j.id));
  state.jobs=(data.items||[]).filter(j=>j.kind==='import'||j.kind==='push'||j.kind==='verify');
  if(state.jobs.some(j=>j.status!=='running'&&wasRunning.has(j.id))){loadList('accounts',true);refreshMetrics();}
  $('#admin-jobs').innerHTML=state.jobs.slice(0,4).map(j=>{
   const label=j.kind==='push'?'账号推送':j.kind==='import'?'账号导入':'手动账号校验';
   const status=j.status==='running'?'进行中':j.status==='completed'?'已完成':'已中断';
   const pct=j.total?Math.min(100,Math.round(j.processed/j.total*100)):j.status==='completed'?100:0;
   return `<div class="admin-job"><div class="job-heading"><strong>${label} · ${status}</strong><button class="text-button" data-job="${escapeHTML(j.id)}">查看详情 ${icon('chevron')}</button></div><div class="job-meta">${count(j.processed)} / ${count(j.total)} 条 · ${j.kind!=='verify'?`新增 ${count(j.imported)} · 重复 ${count(j.duplicates)}`:`成功 ${count(j.succeeded)}`} · 失败 ${count(j.failed)} · ${escapeHTML(date(j.updated_at))}</div><progress class="job-progress" max="100" value="${pct}" aria-label="${label}进度"></progress></div>`;
  }).join('');
  const openJob=$('#detail-dialog').open&&state.jobs.find(j=>j.id===$('#detail-content').dataset.jobId);
  if(openJob)showJob(openJob);
  if(state.jobs.some(j=>j.status==='running'))state.jobsTimer=setTimeout(refreshJobs,2000);
 }catch(error){$('#admin-jobs').textContent=error.message;if(!state.demo&&state.connected)state.jobsTimer=setTimeout(refreshJobs,5000);}
}
function showJob(j){
 const fields=[['状态',j.status==='running'?'进行中':j.status==='completed'?'已完成':'已中断'],['已处理',`${j.processed} / ${j.total}`],['新增',j.imported],['重复',j.duplicates],['校验成功',j.succeeded],['失败',j.failed]];
 showDetail(`<h2>${j.kind==='push'?'账号推送':j.kind==='import'?'账号导入':'手动校验'}进度</h2><p>关闭弹窗后任务继续运行，可从账号池页面查看进度。</p><div class="detail-grid">${fields.map(([label,value])=>`<div class="detail-item"><span>${label}</span><strong>${count(value)=== '—'?escapeHTML(value):count(value)}</strong></div>`).join('')}</div>${j.message?`<p class="detail-error">${escapeHTML(j.message)}</p>`:''}<h3>失败明细</h3><p class="small muted">最多保留前 100 条失败原因。</p><div class="job-errors">${(j.errors||[]).map(e=>`<div class="job-error-item"><strong>${escapeHTML(e.email||e.account_id||'账号格式错误')}</strong><br>${escapeHTML(e.message)}</div>`).join('')||'<p>暂未记录失败。</p>'}</div><button class="button" data-job="${escapeHTML(j.id)}">刷新详情</button>${j.status==='running'?'':'<button class="button" id="job-refresh-list">刷新账号列表</button>'}`,'BACKGROUND TASK');
 $('#detail-content').dataset.jobId=j.id;
 $('#job-refresh-list')?.addEventListener('click',()=>{$('#detail-dialog').close();loadList('accounts',true);refreshMetrics();});
}
function showImport(){
 if(state.demo||!state.connected){toast('请先连接真实服务再导入账号。');return;}
 const limit=Number(state.settings.find(item=>item.key==='CORE_IMPORT_MAX_MIB')?.value)||128;
 showDetail(`<h2>导入账号</h2><p>支持 CPA 单账号、Sub2API 多账号、普通账号数组及 data 包装。保存完整凭据和原始账号元数据；每批提交 100 条，关闭弹窗后继续导入。</p><label class="form-label" for="import-file">选择 JSON 文件（最大 ${limit} MiB，可在设置修改）</label><input class="file-input" id="import-file" type="file" accept=".json,application/json"><textarea class="import-text" id="import-data" placeholder="粘贴 CPA / Sub2API / 账号 JSON" aria-label="账号 JSON"></textarea><p class="small muted">文件直接上传，不在浏览器中整体读取与解析。仅含 RT 而无 AT 的账号会明确提示失败。</p><div id="import-error" class="form-error" role="alert"></div><button class="button primary" id="import-submit">开始导入</button>`,'IMPORT ACCOUNTS');
 let selectedFile=null;
 $('#import-file').onchange=event=>{selectedFile=null;const file=event.target.files[0];if(!file)return;if(file.size>limit*1024*1024){$('#import-error').textContent=`文件超过 ${limit} MiB`;event.target.value='';return;}selectedFile=file;$('#import-error').textContent='';$('#import-data').value='';$('#import-data').placeholder=`已选择 ${file.name}，点击开始导入。`;};
 $('#import-submit').onclick=async()=>{const button=$('#import-submit');button.disabled=true;$('#import-error').textContent='';try{const body=$('#import-data').value.trim()||selectedFile;if(!body)throw new Error('请选择文件或粘贴账号 JSON。');if(typeof body==='string'&&new Blob([body]).size>limit*1024*1024)throw new Error(`内容超过 ${limit} MiB`);const job=await api('/admin/accounts/import',{method:'POST',body,timeout:300000});selectedFile=null;showJob(job);await refreshJobs();await loadList('accounts',true);}catch(error){if($('#import-error'))$('#import-error').textContent=error.message;else toast(error.message);}finally{if(button.isConnected)button.disabled=false;}};
}
async function showAccountPush(){
 if(state.demo||!state.connected){toast('请先连接真实服务。');return;}
 try{
  const data=await api('/admin/account-push');const config=data.config;const last=data.last_result;
  showDetail(`<h2>账号推送</h2><p>外部程序选择 Sub2API 模式，服务地址填写当前服务根地址。使用独立推送密钥，仅用于接收账号，无法导出账号或操作管理接口。</p><div class="detail-item"><span>接收路径 · GET 测试连接 / POST 推送</span><strong class="mono">${escapeHTML(location.origin+data.path)}</strong></div><p class="small muted">鉴权：X-API-Key 或 Authorization: Bearer；每次最多 ${data.max_mib} MiB。关闭入口或更新密钥立即生效。</p><label class="form-label"><input id="push-enabled" type="checkbox" ${config.enabled?'checked':''}> 启用账号推送</label><label class="form-label" for="push-key">推送密钥 ${config.key?'（当前密钥）':config.has_key?'（旧密钥无法读回，请重新设置一次）':'（尚未设置）'}</label><input class="key-input" id="push-key" value="${escapeHTML(config.key||'')}" type="text" autocomplete="off" spellcheck="false" placeholder="至少 16 个字符"><button class="button" id="push-generate">生成随机密钥</button><p class="small muted">密钥明文保存，重新打开可查看当前保存的密钥；留空保存保留原密钥。重复推送保留主账号正在使用的凭据，新增凭据与元数据保存在关联记录中，不重置评分或管理停用状态。</p><div id="push-error" class="form-error" role="alert"></div><button class="button primary" id="push-save">保存推送配置</button>${last?`<h3>最近一次推送</h3><p>${escapeHTML(date(last.updated_at))} · ${last.status==='running'?'进行中':last.status==='completed'?'已完成':'已中断'} · 新增 ${count(last.imported)} · 复用/归并 ${count(last.duplicates)} · 失败 ${count(last.failed)}</p><button class="button" id="push-last">查看进度与失败原因</button>`:''}`,'ACCOUNT PUSH');
  $('#push-generate').onclick=()=>{const bytes=crypto.getRandomValues(new Uint8Array(24));$('#push-key').value=Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');};
  $('#push-save').onclick=async event=>{const button=event.target;button.disabled=true;try{await api('/admin/account-push',{method:'PUT',body:JSON.stringify({enabled:$('#push-enabled').checked,key:$('#push-key').value,revision:config.revision})});$('#push-key').value='';toast('推送配置已保存');await showAccountPush();}catch(error){if($('#push-error'))$('#push-error').textContent=error.message;}finally{if(button.isConnected)button.disabled=false;}};
  if(last)$('#push-last').onclick=()=>showJob(last);
 }catch(error){toast(error.message);}
}
$('#accounts-push').addEventListener('click',showAccountPush);
async function startVerification(ids,all=false){
 if(state.demo||!state.connected){toast('请先连接真实服务再校验账号。');return;}
 const job=await api('/admin/accounts/verify',{method:'POST',body:JSON.stringify(all?{all:true}:{ids})});showJob(job);refreshJobs();
}
$('#accounts-export').addEventListener('click',async()=>{
 if(state.demo){toast('演示模式不导出账号。');return;}if(!state.connected){showConnection();return;}
 const button=$('#accounts-export');button.disabled=true;button.textContent='正在导出…';
 try{
  const response=await adminFetch('/admin/accounts/export',{timeout:300000});
  if(!response.ok){throw new Error(`账号导出失败（${response.status}）`);}
  const blob=await response.blob();
  // fetch 会将响应传输中断视为失败；直接保存 Blob，避免完整 JSON 解码放大内存。
  const url=URL.createObjectURL(blob);const link=document.createElement('a');link.href=url;link.download=`accounts-full-${new Date().toISOString().slice(0,10)}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);toast('完整账号数据已导出。');
 }catch(error){toast(error.message||'账号导出失败。');}finally{button.disabled=false;button.textContent='完整导出';}
});
$('#accounts-import').addEventListener('click',showImport);
$('#accounts-verify-all').addEventListener('click',()=>{
 if(state.demo||!state.connected){toast('请先连接真实服务再校验账号。');return;}
 showDetail('<h2>校验全部账号</h2><p>将强制远程校验账号池中的全部账号；当前校验成功的账号也会重新检查。任务在后台执行，可关闭弹窗查看进度。</p><p>手动校验有独立并发名额，数量沿用「账号校验并发」设置。同一账号的重复校验仍会合并。</p><button class="button primary" id="confirm-verify-all">开始校验全部账号</button>','ACCOUNT VERIFICATION');
 $('#confirm-verify-all').onclick=async event=>{event.target.disabled=true;try{await startVerification([],true);}catch(error){toast(error.message);if(event.target.isConnected)event.target.disabled=false;}};
});
document.addEventListener('click',async event=>{
 const jobButton=event.target.closest('[data-job]');
 if(jobButton){jobButton.disabled=true;try{showJob(await api('/admin/jobs/'+encodeURIComponent(jobButton.dataset.job)));}catch(error){toast(error.message);}finally{jobButton.disabled=false;}return;}
 const button=event.target.closest('[data-manage]');if(!button)return;
 if(state.demo||!state.connected){toast('演示模式无法修改账号，请先连接真实服务。');return;}
 const id=button.dataset.id, action=button.dataset.manage;
 if(action==='delete'){
  showDetail(`<h2>删除账号</h2><p>账号将从数据库和选号池中移除。已接纳的请求可以继续退场，之后的新请求不会再选中它。此操作无法撤销。</p><p class="mono">${escapeHTML(id)}</p><button class="button danger-button" data-manage="confirm-delete" data-id="${escapeHTML(id)}">确认删除这个账号</button>`,'DELETE ACCOUNT');return;
 }
 button.disabled=true;
 try{
  if(action==='verify'){await startVerification([id]);return;}
  if(action==='toggle')await api('/admin/accounts/'+encodeURIComponent(id),{method:'PATCH',body:JSON.stringify({enabled:button.dataset.enabled==='true'})});
  if(action==='confirm-delete')await api('/admin/accounts/'+encodeURIComponent(id),{method:'DELETE'});
  $('#detail-dialog').close();await loadList('accounts',true);refreshMetrics();toast(action==='confirm-delete'?'账号已删除。':'账号状态已更新。');
 }catch(error){toast(error.message);}finally{button.disabled=false;}
});
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&state.page==='accounts')refreshJobs();});


function resultsParams(){const period=$('#results-period').value;return period==='custom'?new URLSearchParams({from:$('#results-from').value,to:$('#results-to').value}):new URLSearchParams({period});}
async function refreshResults(force=false){
 if(!state.demo&&!state.connected)return;const params=resultsParams(),key=params.toString();if($('#results-period').value==='custom'&&(!$('#results-from').value||!$('#results-to').value))return;
 if(resultsBusy&&!force)return;if(!force&&key===resultsKey&&Date.now()-resultsAt<30000)return;
 const version=++resultsVersion;resultsBusy=true;resultsKey=key;
 try{
  const data=state.demo?{succeeded:1248,failed:8,safety_rejected:19,success_rate:99.36,successful_images:1342,p50_ms:28500,p95_ms:67300,queue_p95_ms:1200,sample_count:1248,computed_at:new Date().toISOString(),historical_image_count_unknown:0}:await api('/admin/results?'+params);
  if(version!==resultsVersion)return;resultsAt=Date.now();$('#results-error').classList.add('hidden');
  setText('stat-success',count(data.succeeded));setText('stat-failed',count(data.failed));setText('stat-safety','审核拒绝 '+count(data.safety_rejected));$('#stat-rate').innerHTML=(data.sample_count+data.failed?Number(data.success_rate).toFixed(2):'—')+'<small>%</small>';
  setText('result-images',count(data.successful_images));setText('result-p50',data.sample_count?duration(data.p50_ms):'—');setText('result-p95',data.sample_count?duration(data.p95_ms):'—');setText('result-queue',duration(data.queue_p95_ms));
  setText('results-note','成功请求耗时包含排队，不含 HTTP 响应传输；按完成日期统计。审核拒绝不计入失败和成功率。统计最多缓存 30 秒 · 更新 '+date(data.computed_at)+(data.historical_image_count_unknown?' · '+count(data.historical_image_count_unknown)+' 条旧日志未记录图片数量':''));
 }catch(e){if(version!==resultsVersion)return;setText('results-error',e.message+'；当前显示上次统计结果。');$('#results-error').classList.remove('hidden');}finally{if(version===resultsVersion)resultsBusy=false;}
}
function renderExtraMetrics(m){
 const s=m.system||{},u=m.upstream||{};
 setText('runtime-cpu',s.cpu_percent==null?'—':Number(s.cpu_percent).toFixed(1)+'%');setText('runtime-memory',bytes(s.memory_used_bytes)+(s.memory_limit_bytes?' / '+bytes(s.memory_limit_bytes):''));setText('runtime-disk',bytes(s.disk_available_bytes));setText('sse-active','活跃生成流 '+count(u.sse_active));
 $('#stage-body').innerHTML=Object.entries(u.stages||{}).sort((a,b)=>Number(b[1].mean_ms)-Number(a[1].mean_ms)).map(([key,v])=>'<tr><td>'+escapeHTML(stageName(key))+'</td><td>'+count(v.count)+'</td><td>'+duration(v.mean_ms)+'</td><td>'+duration(v.max_ms)+'</td></tr>').join('')||'<tr><td colspan="4" class="empty-cell">有生成任务后显示阶段采样</td></tr>';
}
function stageName(key){return ({pow_solve:'PoW 计算',sse:'上游生成流',download:'图片下载',account_verify:'账号校验',download_wait:'下载排队',validation_wait:'解码排队'})[key]||key;}
function bodySections(row){return '<h3>实际调用报文</h3><p class="small muted">保留调用方原始字段；长字符串点击后加载。异步创建响应与任务查询响应分别展示。升级前的日志可能没有原始报文。</p>'+[['request','请求体'],['response',row.is_async?'创建响应体':'响应体'],...(row.is_async?[['task_response','任务查询响应体（最近一次终态）']]:[])].map(([kind,label])=>'<details class="body-section" data-body-kind="'+kind+'"><summary>'+label+'</summary><div class="body-status">展开后读取</div><div class="json-tree hidden"></div><button type="button" class="raw-body-button">查看完整原文（按需加载）</button><pre class="raw-body hidden"></pre></details>').join('');}
function renderJSONNode(n,path,host){
 if(n.kind==='object'||n.kind==='array'){
  const container=document.createElement('details');container.open=true;const summary=document.createElement('summary');summary.textContent=(path?JSON.stringify(path)+': ':'')+(n.kind==='object'?'{':'[')+' '+(n.children||[]).length+' 项';container.append(summary);for(const child of n.children||[])renderJSONNode(child.node,child.key,container);host.append(container);return;
 }
 const line=document.createElement('div');line.className='json-leaf';const key=document.createElement('span');key.className='json-key';key.textContent=path!==''?JSON.stringify(path)+': ':'';line.append(key);const value=document.createElement('span');value.className='json-value';line.append(value);
 if(n.deferred){value.textContent=n.value;const button=document.createElement('button');button.type='button';button.className='deferred-value';button.textContent='加载完整值';button._deferred=n;line.append(button);}else value.textContent=JSON.stringify(n.value);host.append(line);
}
function installBodySections(id){
 for(const section of $$('.body-section')){
  const kind=section.dataset.bodyKind,endpoint='/admin/requests/'+encodeURIComponent(id)+'/body/'+kind;let loaded=false,busy=false;
  section.addEventListener('toggle',async()=>{
   if(!section.open||loaded||busy)return;busy=true;const status=$('.body-status',section),tree=$('.json-tree',section);status.textContent='正在读取报文结构…';
   try{if(state.demo){status.textContent='界面演示不保存真实报文。';loaded=true;return;}const data=await api(endpoint,{timeout:30000});if(!section.isConnected)return;tree.replaceChildren();if(data.multipart){section.dataset.multipart='1';$('.raw-body-button',section).textContent='下载完整 multipart 原文';tree.textContent='multipart 请求包含二进制参考图片，完整原文按需下载。';}else{renderJSONNode(data.tree,'',tree);}tree.classList.remove('hidden');status.textContent=count(data.bytes)+' 字节 · HTTP '+data.http_status+' · 返回时间 '+date(data.captured_at);loaded=true;}catch(e){status.textContent=e.message+'；收起后可再次展开重试。';}finally{busy=false;}
  });
  section.addEventListener('click',async event=>{const rawButton=event.target.closest('.raw-body-button');if(rawButton){if(state.demo){toast('界面演示不保存真实报文。');return;}rawButton.disabled=true;rawButton.textContent='正在加载原文…';try{const response=await adminFetch(endpoint+'?raw=1',{timeout:60000});if(!response.ok)throw new Error('原文读取失败（'+response.status+'）');if(section.dataset.multipart==='1'){const blob=await response.blob();const url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download='request.multipart';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);rawButton.disabled=false;rawButton.textContent='下载完整 multipart 原文';return;}const text=await response.text();if(!rawButton.isConnected)return;const pre=$('.raw-body',section);pre.textContent=text;pre.classList.remove('hidden');rawButton.remove();}catch(e){rawButton.disabled=false;rawButton.textContent='重试读取原文';toast(e.message);}return;}const button=event.target.closest('.deferred-value');if(!button)return;button.disabled=true;button.textContent='正在加载…';const n=button._deferred;
   try{const value=await api(endpoint+'?offset='+n.offset+'&length='+n.length,{timeout:60000});if(!button.isConnected)return;$('.json-value',button.parentElement).textContent=JSON.stringify(value);button.remove();}catch(e){button.disabled=false;button.textContent='重试加载';toast(e.message);}
  });
 }
}
$('#results-period').addEventListener('change',()=>{$('#results-dates').classList.toggle('hidden',$('#results-period').value!=='custom');refreshResults(true);});
for(const id of ['results-from','results-to'])$('#'+id).addEventListener('change',()=>refreshResults(true));
for(const id of ['logs-key','logs-async','logs-format','logs-from','logs-to'])$('#'+id).addEventListener('change',()=>{if(Boolean($('#logs-from').value)!==Boolean($('#logs-to').value))return;loadList('logs',true);});
$('#refresh-button').addEventListener('click',()=>refreshResults(true));

function storageCard(label,value,note=''){return '<div class="storage-stat"><span>'+escapeHTML(label)+'</span><strong>'+escapeHTML(value)+'</strong><small>'+escapeHTML(note)+'</small></div>';}
async function loadStorage(force=false){
 clearTimeout(storageState.timer);if(!['images','logs'].includes(state.page)||document.hidden)return;
 if(state.demo){for(const kind of ['images','logs']){$('#'+kind+'-storage-summary').innerHTML=storageCard('记录数量','0','界面演示，无真实存储')+storageCard('占用空间','0 MiB')+storageCard('待删文件','0');$('#'+kind+'-retention-status').textContent='界面演示不执行清理。';}return;}
 if(!state.connected||storageState.busy)return;storageState.busy=true;const version=storageState.version;
 try{const data=await api('/admin/storage');if(version!==storageState.version)return;const s=data.summary;
 $('#images-storage-summary').innerHTML=storageCard('图片数量',count(s.images.count),data.indexing?'正在建立历史图片索引':data.index_error||'数据库索引统计')+storageCard('原图占用',bytes(s.images.bytes),'不含缩略图缓存')+storageCard('待删文件',count(s.pending_files),bytes(s.pending_bytes)+' · 包含日志文件');
 $('#logs-storage-summary').innerHTML=storageCard('调用日志',count(s.logs.count),'统计每 30 秒更新')+storageCard('响应文件占用',bytes(s.logs.response_bytes),'原始请求体 '+bytes(s.logs.request_bytes))+storageCard('数据库已分配',bytes(s.logs.database_bytes),'含日志、报文表与异步任务；清理后空间供数据库复用');
 for(const policy of data.policies){const kind=policy.resource,form=$('#'+kind+'-retention-form');storageState.policies[kind]=policy;if(force||!form.dataset.dirty){$('#'+kind+'-retain-hours').value=policy.retain_hours;$('#'+kind+'-auto-clean').checked=policy.enabled;$('#'+kind+'-clean-interval').value=policy.interval_minutes;}
 $('#'+kind+'-retention-status').textContent=(policy.enabled?'自动清理已开启 · 下次执行 '+date(policy.next_run_at):'自动清理已关闭')+' · 配置保存在数据库，重启沿用计时。'+(kind==='images'&&data.indexing?' 正在建立历史图片索引。':'');
 $('#'+kind+'-clean-now').disabled=Boolean(data.busy[kind]||(kind==='images'&&data.indexing));
 const job=data.jobs.find(j=>j.kind==='cleanup_'+kind);const previous=storageState.jobs[kind];storageState.jobs[kind]=job;if(job&&job.status!=='running'&&previous?.status==='running'&&job.id===previous.id){if(kind==='images'&&state.page==='images')loadImages(true);if(kind==='logs'&&state.page==='logs')loadList('logs',true);}$('#'+kind+'-cleanup-job').textContent=job?('最近清理 · '+({running:'执行中',succeeded:'已完成',failed:'未完整完成'}[job.status]||job.status)+' · 已扫描 '+count(job.processed)+' · 已清理 '+count(job.succeeded)+' · 跳过 '+count(job.duplicates)+' · 失败 '+count(job.failed)+'。'+(job.message||'')+(job.errors?.length?'；'+job.errors.map(e=>e.message).join('；'):'')):'';
 }
 }catch(e){if(version===storageState.version)$('#'+state.page+'-retention-status').textContent=e.message;}
 finally{storageState.busy=false;if(version===storageState.version&&['images','logs'].includes(state.page))storageState.timer=setTimeout(()=>loadStorage(),5000);}
}
async function loadImages(reset=false){
 if(reset){imageState.cursors=[''];imageState.page=0;imageState.next='';}const version=++imageState.version;const gallery=$('#images-gallery');
 if(!state.connected||state.demo){gallery.innerHTML='<div class="gallery-empty">'+(state.demo?'界面演示不保存真实图片。':'连接服务后查看生成的图片。')+'</div>';updateImagePagination();return;}
 gallery.innerHTML='<div class="gallery-empty">正在读取图片索引…</div>';
 try{const from=$('#images-from').value,to=$('#images-to').value;if(Boolean(from)!==Boolean(to))throw new Error('请选择完整的开始与结束日期。');const q=new URLSearchParams({limit:'24',q:$('#images-search').value.trim(),cursor:imageState.cursors[imageState.page]||'',from,to});const data=await api('/admin/images?'+q);if(version!==imageState.version)return;imageState.next=data.next_cursor||'';
 gallery.innerHTML=data.items.length?data.items.map(im=>{const source='/images/'+encodeURIComponent(im.name),thumb='/images/thumbnails/'+encodeURIComponent(im.name);return '<article class="image-card"><a href="'+source+'" target="_blank" rel="noopener" aria-label="查看原图 '+escapeHTML(im.name)+'"><img loading="lazy" decoding="async" src="'+thumb+'" alt="生成图片预览"></a><div class="image-caption"><strong title="'+escapeHTML(im.name)+'">'+escapeHTML(im.name)+'</strong><small>'+date(im.created_at)+' · '+bytes(im.bytes)+'</small>'+(im.request_id?'<button class="text-button" data-image-request="'+escapeHTML(im.request_id)+'">查看调用日志</button>':'<small>历史图片</small>')+'</div></article>';}).join(''):'<div class="gallery-empty">没有符合筛选条件的图片。</div>';
 }catch(e){if(version===imageState.version)gallery.innerHTML='<div class="gallery-empty">'+escapeHTML(e.message)+'</div>';}
 finally{if(version===imageState.version)updateImagePagination();}
}
function updateImagePagination(){$('#images-page').textContent='第 '+(imageState.page+1)+' 页';$('#images-prev').disabled=imageState.page===0;$('#images-next').disabled=!imageState.next;}
for(const kind of ['images','logs']){
 const form=$('#'+kind+'-retention-form');form.addEventListener('input',()=>{form.dataset.dirty='true';});
 form.addEventListener('submit',async event=>{event.preventDefault();if(state.demo){toast('演示模式不会保存清理策略。');return;}if(!state.connected){showConnection();return;}const policy=storageState.policies[kind];if(!policy){toast('请等待清理策略加载完成。');return;}const button=$('button[type=submit]',form);button.disabled=true;try{await api('/admin/storage/'+kind,{method:'PUT',body:JSON.stringify({enabled:$('#'+kind+'-auto-clean').checked,retain_hours:Number($('#'+kind+'-retain-hours').value),interval_minutes:Number($('#'+kind+'-clean-interval').value),revision:policy.revision})});delete form.dataset.dirty;toast('清理策略已保存。');await loadStorage(true);}catch(e){toast(e.message);}finally{button.disabled=false;}});
 $('#'+kind+'-clean-now').addEventListener('click',async()=>{if(state.demo){toast('演示模式不会删除数据。');return;}if(!state.connected){showConnection();return;}const hours=Number($('#'+kind+'-retain-hours').value);if(!Number.isInteger(hours)||hours<1||hours>87600){toast('保留时间为 1～87600 小时。');return;}const cutoff=new Date(Date.now()-hours*3600000);if(!confirm('确认清理 '+date(cutoff)+' 以前的'+(kind==='images'?'图片？原有图片链接将失效。':'日志及已完成异步任务记录？')+' 此操作无法撤销。'))return;const button=$('#'+kind+'-clean-now');button.disabled=true;try{await api('/admin/storage/'+kind+'/cleanup',{method:'POST',body:JSON.stringify({retain_hours:hours})});toast('清理任务已在后台启动。');await loadStorage();if(kind==='images')loadImages(true);else loadList('logs',true);}catch(e){toast(e.message);button.disabled=false;}});
}
$('#images-refresh').addEventListener('click',()=>{loadImages(true);loadStorage();});
$('#images-prev').addEventListener('click',()=>{if(imageState.page>0){imageState.page--;loadImages();}});
$('#images-next').addEventListener('click',()=>{if(imageState.next){imageState.page++;imageState.cursors[imageState.page]=imageState.next;loadImages();}});
let imageSearchTimer;$('#images-search').addEventListener('input',()=>{clearTimeout(imageSearchTimer);imageSearchTimer=setTimeout(()=>loadImages(true),350);});
for(const id of ['images-from','images-to'])$('#'+id).addEventListener('change',()=>{if(Boolean($('#images-from').value)===Boolean($('#images-to').value))loadImages(true);});
$('#images-gallery').addEventListener('click',event=>{const button=event.target.closest('[data-image-request]');if(button)requestDetail(button.dataset.imageRequest);});


function expireSession(message='已退出登录。'){
 accessVersion++;state.sessionVersion++;state.connected=false;state.demo=false;state.metrics=null;state.samples=[];state.settings=[];state.limits=[];state.draft={};state.saved='';
 clearInterval(state.timer);clearTimeout(state.jobsTimer);clearTimeout(storageState.timer);resetLists();resultsVersion++;resultsAt=0;resultsKey='';
 $('#detail-dialog').close();$('#detail-content').replaceChildren();$('#access-panel').replaceChildren();$('#images-gallery').replaceChildren();$('#admin-jobs').replaceChildren();$('#settings-fields').replaceChildren();
 for(const kind of ['accounts','logs'])renderEmpty(kind,'登录后查看记录。');updateConnectionUI();showConnection();$('#connect-error').textContent=message;
}
async function connectSession(){
 const [config,metrics]=await Promise.all([api('/admin/config'),api('/admin/metrics')]);
 state.sessionVersion++;state.connected=true;state.demo=false;state.metrics=metrics;state.samples=[];resultsAt=0;resultsKey='';installSettings(config);resetLists();updateConnectionUI();renderMetrics(metrics);await refreshMetrics();navigate(state.page);resetTimer();
}
async function restoreSession(){
 try{await api('/admin/session');await connectSession();}
 catch(error){showConnection();if(error.code!==401)$('#connect-error').textContent='暂时无法恢复登录：'+error.message;}
}
async function logoutAdmin(){
 try{await api('/admin/session',{method:'DELETE'});expireSession('已主动退出登录。');}
 catch(error){if(state.connected)toast('退出未完成：'+error.message);}
}
let accessVersion=0;
async function renderAccess(){
 const panel=$('#access-panel'),version=++accessVersion;
 if(state.demo||!state.connected){panel.innerHTML='<div class="empty-cell">登录真实服务后管理密钥。</div>';return;}
 panel.innerHTML='<div class="empty-cell">正在读取访问配置…</div>';
 try{
 const data=await api('/admin/api-keys');if(version!==accessVersion||!state.connected||state.group!=='access')return;
 panel.innerHTML=`<div class="panel access-section"><div class="panel-heading"><div><h2>管理员登录</h2><p>当前会话会保持登录。另一个浏览器登录、主动退出或更换密钥时，会更新登录状态。</p></div><button class="button" id="access-logout">退出登录</button></div><form id="admin-key-form" class="access-form"><label class="form-label" for="current-admin-key">当前管理员密钥</label><input id="current-admin-key" class="key-input" type="password" autocomplete="current-password" required><label class="form-label" for="new-admin-key">新管理员密钥</label><input id="new-admin-key" class="key-input" type="password" autocomplete="new-password" minlength="16" maxlength="256" required><label class="form-label" for="confirm-admin-key">确认新密钥</label><input id="confirm-admin-key" class="key-input" type="password" autocomplete="new-password" minlength="16" maxlength="256" required><p class="small muted">16～256 字节。保存后立即生效并保持当前浏览器登录；其他会话及旧管理员密钥失效。配置保存在数据库，重启不会被 .env 覆盖。</p><div class="form-error" id="admin-key-error" role="alert"></div><button class="button primary" type="submit">修改管理员密钥</button></form></div><div class="panel access-section"><div class="panel-heading"><div><h2>图片调用密钥</h2><p>用于生成图片、查询任务和获取模型列表。停用或删除立即影响后续请求，已接纳的任务继续执行。</p></div><button class="button primary" id="api-key-add">新增密钥</button></div><div id="api-key-list">${data.items.map((item,index)=>`<div class="access-key-row"><div class="access-key-info"><strong>${escapeHTML(item.name)}</strong><span class="badge ${item.enabled?'success':'neutral'}">${item.enabled?'已启用':'已停用'}</span><div class="mono access-key-preview">…${escapeHTML(item.key.slice(-8))}</div><p class="small muted">并发 ${count(item.active)} / ${item.concurrency?count(item.concurrency):'不限'} · 创建于 ${escapeHTML(date(item.created_at))}</p></div><div class="access-key-actions"><button class="button" data-key-stats="${index}">调用统计</button><button class="button" data-edit-key="${index}">查看 / 编辑</button><button class="button" data-toggle-key="${index}">${item.enabled?'停用':'启用'}</button><button class="text-button" data-delete-key="${index}">删除</button></div></div>`).join('')||'<div class="empty-cell">还没有调用密钥，请新增后使用图片接口。</div>'}</div><p class="small muted access-key-note">调用密钥独立于管理员和账号推送密钥。各调用密钥共享号池；新任务只能用创建它的 Key 查询。并发上限包含排队与执行中的任务，0 表示不额外限制，全局容量仍然生效。</p></div>`;
 $('#access-logout').onclick=logoutAdmin;
 $('#admin-key-form').onsubmit=async event=>{
 event.preventDefault();const button=$('button[type="submit"]',event.target),error=$('#admin-key-error');error.textContent='';
 if($('#new-admin-key').value!==$('#confirm-admin-key').value){error.textContent='两次输入的新密钥不一致。';return;}button.disabled=true;
 try{await api('/admin/access/admin-key',{method:'PUT',body:JSON.stringify({current_key:$('#current-admin-key').value,new_key:$('#new-admin-key').value})});event.target.reset();toast('管理员密钥已更新，当前浏览器保持登录。');}
 catch(e){if(error.isConnected)error.textContent=e.message;}finally{button.disabled=false;}
 };
 $('#api-key-add').onclick=()=>editAPIKey();
 $$('[data-key-stats]',panel).forEach(button=>button.onclick=()=>showKeyStats(data.items[Number(button.dataset.keyStats)]));
 $$('[data-edit-key]',panel).forEach(button=>button.onclick=()=>editAPIKey(data.items[Number(button.dataset.editKey)]));
 $$('[data-toggle-key]',panel).forEach(button=>button.onclick=async()=>{const item=data.items[Number(button.dataset.toggleKey)];button.disabled=true;try{await api('/admin/api-keys/'+encodeURIComponent(item.id),{method:'PUT',body:JSON.stringify({name:item.name,key:'',enabled:!item.enabled,revision:item.revision})});await renderAccess();}catch(e){toast(e.message);button.disabled=false;}});
 $$('[data-delete-key]',panel).forEach(button=>button.onclick=async()=>{const item=data.items[Number(button.dataset.deleteKey)];if(!confirm('删除调用密钥「'+item.name+'」？使用该 Key 的后续调用将无法通过鉴权。'))return;button.disabled=true;try{await api('/admin/api-keys/'+encodeURIComponent(item.id),{method:'DELETE',body:JSON.stringify({revision:item.revision})});await renderAccess();}catch(e){toast(e.message);button.disabled=false;}});
 }catch(error){if(version===accessVersion&&state.connected)panel.innerHTML='<div class="empty-cell">'+escapeHTML(error.message)+'</div>';}
}
function editAPIKey(item){
 showDetail(`<h2>${item?'查看 / 编辑调用密钥':'新增调用密钥'}</h2><form id="api-key-form"><label class="form-label" for="api-key-name">名称</label><input id="api-key-name" class="key-input" value="${escapeHTML(item?.name||'')}" maxlength="80" required><label class="form-label" for="api-key-value">调用密钥</label><input id="api-key-value" class="key-input" type="text" autocomplete="off" spellcheck="false" value="${escapeHTML(item?.key||'')}" placeholder="留空自动生成随机密钥"><p class="small muted">新建时留空自动生成；编辑时留空保留原值。自定义密钥需要 16～256 字节。</p><label class="form-label" for="api-key-concurrency">并发上限</label><input id="api-key-concurrency" class="key-input" type="number" min="0" step="1" value="${item?.concurrency||0}" required><p class="small muted">0 表示不额外限制。包含已接纳的排队及执行任务，异步任务也占名额直到结束；超过上限返回 429。与全局执行容量共同生效，调低不会中断在途任务。</p><label class="form-label"><input id="api-key-enabled" type="checkbox" ${!item||item.enabled?'checked':''}> 启用此密钥</label><div id="api-key-error" class="form-error" role="alert"></div><div class="access-key-actions"><button class="button" type="button" id="api-key-copy">复制密钥</button><button class="button primary" type="submit">保存密钥</button></div></form>`,'API KEY');
 $('#api-key-copy').onclick=async()=>{const value=$('#api-key-value').value;if(!value){toast('请先保存生成密钥。');return;}try{await navigator.clipboard.writeText(value);toast('调用密钥已复制。');}catch{ $('#api-key-value').select();toast('已选中密钥，可手动复制。');}};
 $('#api-key-form').onsubmit=async event=>{event.preventDefault();const button=$('button[type="submit"]',event.target),error=$('#api-key-error');button.disabled=true;try{const data=await api('/admin/api-keys'+(item?'/'+encodeURIComponent(item.id):''),{method:item?'PUT':'POST',body:JSON.stringify({name:$('#api-key-name').value,key:$('#api-key-value').value,enabled:$('#api-key-enabled').checked,concurrency:Number($('#api-key-concurrency').value),revision:item?.revision||0})});await renderAccess();editAPIKey(data.item);toast('调用密钥已保存。');}catch(e){if(error.isConnected)error.textContent=e.message;}finally{button.disabled=false;}};
}

async function loadKeyFilters(){
 if(state.demo||!state.connected)return;
 try{const data=await api('/admin/api-keys');if(!state.connected)return;const select=$('#logs-key'),value=select.value;
 select.replaceChildren(new Option('全部调用 Key',''));for(const k of data.items)select.add(new Option(k.name,k.id));
 if(value&&!data.items.some(k=>k.id===value))select.add(new Option('历史 Key · '+value,value));select.value=value;
 }catch(e){toast(e.message);}
}
async function showKeyStats(item){
 showDetail('<h2>'+escapeHTML(item.name)+' · 调用统计</h2><label class="form-label">统计日期<select id="key-stats-period" class="key-input"><option value="today">今天</option><option value="yesterday">昨天</option><option value="7d">近 7 天</option><option value="30d">近 30 天</option></select></label><div id="key-stats-data" aria-live="polite"></div>','KEY STATISTICS');
 const update=async()=>{const target=$('#key-stats-data');if(!target)return;target.textContent='正在统计…';
 try{const data=await api('/admin/api-key-stats?'+new URLSearchParams({api_key_id:item.id,period:$('#key-stats-period').value}));if(!target.isConnected)return;
 target.innerHTML='<div class="detail-meta"><span class="badge neutral">在途 '+count(data.active)+'</span><span class="badge neutral">请求 '+count(data.requests)+'</span><span class="badge success">成功 '+count(data.succeeded)+'</span><span class="badge failed">失败 '+count(data.failed)+'</span><span class="badge safety">审核拒绝 '+count(data.safety_rejected)+'</span></div><p>成功率 '+Number(data.success_rate).toFixed(2)+'% · 成功图片 '+count(data.successful_images)+'</p><p class="mono">P50 '+duration(data.p50_ms)+' · P95 '+duration(data.p95_ms)+'</p><p class="small muted">'+escapeHTML(data.duration_scope)+'。按北京时间完成日期筛选，历史未关联 Key 的记录不计入此 Key。</p>';
 }catch(e){if(target.isConnected)target.textContent=e.message;}};
 $('#key-stats-period').onchange=update;await update();
}
if(new URLSearchParams(location.search).get('demo')==='1')activateDemo();else restoreSession();
