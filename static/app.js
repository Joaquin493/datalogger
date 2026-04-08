let allSignals   = []
let allEvents    = []
let filteredEvs  = []
let sigFilter    = 'on'
let evPage       = 1
let evPageSize   = 50
let sortCol      = 'id'
let sortDir      = -1
let allCounts    = []
let cSortCol     = 'total'
let cSortDir     = -1
let searchTimeout = null

// ── CLOCK ──
function updateClock() {
  const now = new Date()
  document.getElementById('clock').textContent =
    now.toLocaleDateString('es-AR') + '  ' + now.toTimeString().slice(0,8)
}
setInterval(updateClock, 1000)
updateClock()

// ── TABS ──
function switchTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'))
  el.classList.add('active')
  document.getElementById('panel-' + name).classList.add('active')
  if (name === 'sysevents') loadSysEvents()
}

// ── SISTEMA ──
const SYSEV_ICONS = { INICIO: '▶', CONEXION: '●', RECONEXION: '↺', DESCONEXION: '✕' }
const SYSEV_CLASS = { INICIO: 'ev-inicio', CONEXION: 'ev-conexion', RECONEXION: 'ev-reconexion', DESCONEXION: 'ev-desconexion' }

async function loadSysEvents() {
  try {
    const res  = await fetch('/system-events')
    const data = await res.json()
    const tbody = document.getElementById('sysev-body')
    const empty = document.getElementById('sysev-empty')
    if (!data.length) { tbody.innerHTML = ''; empty.style.display = ''; return }
    empty.style.display = 'none'
    tbody.innerHTML = data.map(e => `
      <tr class="${SYSEV_CLASS[e.event_type] || ''}">
        <td><span class="sysev-badge ${e.event_type}">${SYSEV_ICONS[e.event_type] || '•'} ${e.event_type}</span></td>
        <td>${e.description}</td>
        <td>${e.timestamp}</td>
      </tr>`).join('')
  } catch(err) { console.error('sysevents:', err) }
}

// ── STATUS ──
async function loadStatus() {
  try {
    const t0   = performance.now()
    const res  = await fetch('/status')
    const ms   = Math.round(performance.now() - t0)
    const data = await res.json()
    const el   = document.getElementById('plc-status')
    const lat  = document.getElementById('latency')
    if (data.connected) {
      el.className   = 'ok'
      el.textContent = '● PLC CONECTADO'
      el.title       = 'Última conexión: ' + data.last_connected
      if (lat) { lat.textContent = ms + ' ms'; lat.style.color = 'var(--text2)' }
    } else if (data.retries > 0) {
      el.className   = 'error'
      el.textContent = '● DESCONECTADO (reintento ' + data.retries + ')'
      el.title       = data.last_error || ''
      if (lat) { lat.textContent = '— ms'; lat.style.color = 'var(--danger)' }
    } else {
      el.className   = 'connecting'
      el.textContent = '◌ CONECTANDO...'
      if (lat) { lat.textContent = '— ms'; lat.style.color = 'var(--warn)' }
    }
  } catch(e) {
    const lat = document.getElementById('latency')
    if (lat) { lat.textContent = '— ms'; lat.style.color = 'var(--danger)' }
  }
}

// ── SIGNALS ──
function setSignalFilter(f) {
  sigFilter = f
  document.getElementById('btn-all').className = 'filter-btn' + (f==='all' ? ' active-off' : '')
  document.getElementById('btn-on').className  = 'filter-btn' + (f==='on'  ? ' active-on'  : '')
  document.getElementById('btn-off').className = 'filter-btn' + (f==='off' ? ' active-off' : '')
  renderSignals()
}

function isActive(s) {
  if (s.state === 'ON') return true
  const n = Number(s.state)
  return !isNaN(n) && s.state !== '' && n !== 0
}

function renderSignals() {
  const sigSearch = document.getElementById('sig-search')
  const q         = sigSearch ? sigSearch.value.toLowerCase() : ''
  const grid      = document.getElementById('signal-grid')
  if (!grid) return
  const onCount = allSignals.filter(s => isActive(s)).length
  document.getElementById('stat-total').textContent   = allSignals.length
  document.getElementById('stat-on').textContent      = onCount
  document.getElementById('stat-off').textContent     = allSignals.length - onCount
  let filtered = allSignals.filter(s => {
    const active = isActive(s)
    const matchFilter =
      sigFilter === 'all' ||
      (sigFilter === 'on'  && active) ||
      (sigFilter === 'off' && !active)
    const matchSearch = !q ||
      s.tag.toLowerCase().includes(q) ||
      s.description.toLowerCase().includes(q) ||
      s.address.toLowerCase().includes(q)
    return matchFilter && matchSearch
  })
  document.getElementById('stat-visible').textContent = filtered.length
  if (filtered.length === 0) {
    grid.innerHTML = '<div class="empty-state">Sin señales</div>'
    return
  }
  grid.innerHTML = filtered.map(s => `
    <div class="signal-card ${isActive(s) ? 'ON' : 'OFF'}" title="${s.description}">
      <div class="sig-addr">${s.address}</div>
      <div class="sig-tag">${s.tag}</div>
      <div class="sig-state">${s.state}</div>
    </div>`).join('')
}

async function loadSignals() {
  try {
    const res  = await fetch('/signals')
    allSignals = await res.json()
    renderSignals()
  } catch(e) { console.error('signals:', e) }
}

// ── EVENTS ──
function populateTagFilter() {
  const tags = [...new Set(allEvents.map(e => e.tag))].sort()
  const sel  = document.getElementById('ev-tag')
  if (!sel) return
  const cur  = sel.value
  sel.innerHTML = '<option value="">Tag: Todos</option>' +
    tags.map(t => `<option value="${t}" ${t===cur?'selected':''}>${t}</option>`).join('')
}

function onSearchInput() {
  evPage = 1
  clearTimeout(searchTimeout)
  searchTimeout = setTimeout(() => loadEvents(), 400)
}

function applyEvFilters() {
  const state = document.getElementById('ev-state')?.value || ''
  filteredEvs = allEvents.filter(e => !state || e.state === state)
  renderEventsTable()
}

function sortBy(col) {
  if (sortCol === col) sortDir = -sortDir
  else { sortCol = col; sortDir = -1 }
  document.querySelectorAll('th .sort-arrow').forEach(a => a.textContent = '↕')
  const arr = document.getElementById('sort-' + col)
  if (arr) arr.textContent = sortDir === -1 ? '↓' : '↑'
  allEvents.sort((a, b) => {
    let av = a[col], bv = b[col]
    if (col === 'id') { av = +av; bv = +bv }
    return av < bv ? sortDir : av > bv ? -sortDir : 0
  })
  evPage = 1
  applyEvFilters()
}

function renderEventsTable() {
  const total = filteredEvs.length
  const pages = Math.max(1, Math.ceil(total / evPageSize))
  evPage      = Math.min(evPage, pages)
  const from  = (evPage - 1) * evPageSize
  const to    = Math.min(from + evPageSize, total)
  const slice = filteredEvs.slice(from, to)
  document.getElementById('pg-from').textContent  = total ? from + 1 : 0
  document.getElementById('pg-to').textContent    = to
  document.getElementById('pg-total').textContent = total
  const tbody = document.getElementById('events-body')
  const empty = document.getElementById('ev-empty')
  if (!tbody) return
  if (slice.length === 0) {
    tbody.innerHTML = ''
    empty.style.display = 'block'
  } else {
    empty.style.display = 'none'
    tbody.innerHTML = slice.map(e => `
      <tr>
        <td>${e.id}</td>
        <td class="addr-cell">${e.address||'—'}</td>
        <td class="tag-cell">${e.tag}</td>
        <td><span class="badge badge-${e.state}">${e.state}</span></td>
        <td class="desc-cell" title="${e.description||''}">${e.description||'—'}</td>
        <td>${e.timestamp||'—'}</td>
      </tr>`).join('')
  }
  renderPagination(pages)
}

function renderPagination(pages) {
  const ctrl = document.getElementById('page-controls')
  if (!ctrl) return
  const btns = []
  btns.push(`<button class="page-btn" onclick="goPage(${evPage-1})" ${evPage<=1?'disabled':''}>‹</button>`)
  const range = []
  for (let i = 1; i <= pages; i++) {
    if (i===1 || i===pages || (i>=evPage-2 && i<=evPage+2)) range.push(i)
    else if (range[range.length-1] !== '…') range.push('…')
  }
  range.forEach(p => {
    if (p==='…') btns.push(`<span style="color:var(--text2);padding:0 4px;font-family:var(--mono);font-size:12px">…</span>`)
    else btns.push(`<button class="page-btn ${p===evPage?'active':''}" onclick="goPage(${p})">${p}</button>`)
  })
  btns.push(`<button class="page-btn" onclick="goPage(${evPage+1})" ${evPage>=pages?'disabled':''}>›</button>`)
  ctrl.innerHTML = btns.join('')
}

function goPage(p) {
  const pages = Math.max(1, Math.ceil(filteredEvs.length / evPageSize))
  if (p < 1 || p > pages) return
  evPage = p
  renderEventsTable()
}

function changePageSize() {
  evPageSize = +document.getElementById('page-size').value
  evPage = 1
  renderEventsTable()
}

function clearFilters() {
  document.getElementById('ev-search').value = ''
  document.getElementById('ev-state').value  = ''
  document.getElementById('ev-tag').value    = ''
  evPage = 1
  loadEvents()
}

function clearDateFilter() {
  document.getElementById('date-from').value = ''
  document.getElementById('date-to').value   = ''
  evPage = 1
  loadEvents()
}

async function loadEvents() {
  try {
    const savedPage = evPage
    const pageSize  = parseInt(document.getElementById('page-size').value)
    const from      = document.getElementById('date-from').value
    const to        = document.getElementById('date-to').value
    const tag       = document.getElementById('ev-tag').value
    const search    = document.getElementById('ev-search').value
    let url = '/events?limit=' + pageSize * 10
    if (tag)    url += '&tag='       + encodeURIComponent(tag)
    if (search) url += '&search='    + encodeURIComponent(search)
    if (from)   url += '&date_from=' + encodeURIComponent(from)
    if (to)     url += '&date_to='   + encodeURIComponent(to)
    const res = await fetch(url)
    allEvents = await res.json()
    populateTagFilter()
    evPage = savedPage
    applyEvFilters()
  } catch(e) { console.error('events:', e) }
}

// ── CONTADORES ──
async function loadCounts() {
  try {
    const res = await fetch('/events/count')
    allCounts = await res.json()
    renderCounts()
  } catch(e) { console.error('counts:', e) }
}

function sortCounts(col) {
  if (cSortCol === col) cSortDir = -cSortDir
  else { cSortCol = col; cSortDir = -1 }
  document.querySelectorAll('[id^="csort-"]').forEach(a => a.textContent = '↕')
  const arr = document.getElementById('csort-' + col)
  if (arr) arr.textContent = cSortDir === -1 ? '↓' : '↑'
  renderCounts()
}

function renderCounts() {
  const q      = document.getElementById('cnt-search')?.value.toLowerCase() || ''
  const tbody  = document.getElementById('counts-body')
  const empty  = document.getElementById('cnt-empty')
  if (!tbody) return
  let filtered = allCounts.filter(c =>
    !q || c.tag.toLowerCase().includes(q) || (c.description||'').toLowerCase().includes(q)
  )
  filtered.sort((a, b) => {
    let av = a[cSortCol], bv = b[cSortCol]
    if (typeof av === 'number') return (av - bv) * cSortDir
    return String(av).localeCompare(String(bv)) * cSortDir
  })
  if (filtered.length === 0) {
    tbody.innerHTML = ''
    empty.style.display = 'block'
    return
  }
  empty.style.display = 'none'
  tbody.innerHTML = filtered.map(c => `
    <tr>
      <td class="tag-cell">${c.tag}</td>
      <td><strong>${c.total}</strong></td>
      <td style="color:var(--on)">${c.total_on}</td>
      <td style="color:var(--text2)">${c.total_off}</td>
      <td style="color:var(--text2);font-size:11px">${c.last_event||'—'}</td>
      <td class="desc-cell" title="${c.description||''}">${c.description||'—'}</td>
    </tr>`).join('')
}

function exportXLS() {
  const tag    = document.getElementById('ev-tag')?.value    || ''
  const search = document.getElementById('ev-search')?.value || ''
  const from   = document.getElementById('date-from')?.value || ''
  const to     = document.getElementById('date-to')?.value   || ''
  const hasFilter = tag || search || from || to
  if (hasFilter) {
    const filtros = []
    if (tag)    filtros.push(`Tag: ${tag}`)
    if (search) filtros.push(`Búsqueda: "${search}"`)
    if (from)   filtros.push(`Desde: ${from}`)
    if (to)     filtros.push(`Hasta: ${to}`)
    const msg = `Se exportarán solo los eventos filtrados:\n${filtros.join('\n')}\n\n¿Continuar?`
    if (!confirm(msg)) return
  }
  let url = '/export?x=1'
  if (tag)    url += '&tag='       + encodeURIComponent(tag)
  if (search) url += '&search='    + encodeURIComponent(search)
  if (from)   url += '&date_from=' + encodeURIComponent(from)
  if (to)     url += '&date_to='   + encodeURIComponent(to)
  window.open(url)
}

// ── INIT ──
loadSignals()
loadEvents()
loadStatus()
loadCounts()
setInterval(loadSignals, 2000)
setInterval(loadEvents,  5000)
setInterval(loadStatus,  2000)
setInterval(loadCounts,  30000)