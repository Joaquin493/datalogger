let allSignals  = []
let allEvents   = []
let filteredEvs = []
let sigFilter   = 'on'
let evPage      = 1
let evPageSize  = 50
let sortCol     = 'id'
let sortDir     = -1
let allCounts   = []
let cSortCol    = 'total'
let cSortDir    = -1

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
}

// ── STATUS ──
async function loadStatus() {
  try {
    const res  = await fetch('/status')
    const data = await res.json()
    const el   = document.getElementById('plc-status')
    if (data.connected) {
      el.className   = 'ok'
      el.textContent = '● PLC CONECTADO'
      el.title       = 'Última conexión: ' + data.last_connected
    } else if (data.retries > 0) {
      el.className   = 'error'
      el.textContent = '● DESCONECTADO (reintento ' + data.retries + ')'
      el.title       = data.last_error || ''
    } else {
      el.className   = 'connecting'
      el.textContent = '◌ CONECTANDO...'
    }
  } catch(e) {}
}

// ── SIGNALS ──
function setSignalFilter(f) {
  sigFilter = f
  document.getElementById('btn-all').className = 'filter-btn' + (f==='all' ? ' active-off' : '')
  document.getElementById('btn-on').className  = 'filter-btn' + (f==='on'  ? ' active-on'  : '')
  document.getElementById('btn-off').className = 'filter-btn' + (f==='off' ? ' active-off' : '')
  renderSignals()
}

function renderSignals() {
  const q       = document.getElementById('sig-search').value.toLowerCase()
  const grid    = document.getElementById('signal-grid')
  const onCount = allSignals.filter(s => s.state === 'ON').length
  document.getElementById('stat-total').textContent   = allSignals.length
  document.getElementById('stat-on').textContent      = onCount
  document.getElementById('stat-off').textContent     = allSignals.length - onCount
  let filtered = allSignals.filter(s => {
    const matchFilter =
      sigFilter === 'all' ||
      (sigFilter === 'on'  && s.state === 'ON') ||
      (sigFilter === 'off' && s.state !== 'ON')
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
    <div class="signal-card ${s.state}" title="${s.description}">
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
  const cur  = sel.value
  sel.innerHTML = '<option value="">Tag: Todos</option>' +
    tags.map(t => `<option value="${t}" ${t===cur?'selected':''}>${t}</option>`).join('')
}

function applyEvFilters() {
  const q     = document.getElementById('ev-search').value.toLowerCase()
  const state = document.getElementById('ev-state').value
  const tag   = document.getElementById('ev-tag').value
  filteredEvs = allEvents.filter(e => {
    const matchState = !state || e.state === state
    const matchTag   = !tag   || e.tag === tag
    const matchQ     = !q ||
      e.tag.toLowerCase().includes(q) ||
      (e.address||'').toLowerCase().includes(q) ||
      (e.description||'').toLowerCase().includes(q) ||
      (e.timestamp||'').toLowerCase().includes(q)
    return matchState && matchTag && matchQ
  })
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
  applyEvFilters()
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
    let url = '/events?limit=5000'
    if (tag)  url += '&tag='       + encodeURIComponent(tag)
    if (from) url += '&date_from=' + encodeURIComponent(from)
    if (to)   url += '&date_to='   + encodeURIComponent(to)
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
  const q      = document.getElementById('cnt-search').value.toLowerCase()
  const tbody  = document.getElementById('counts-body')
  const empty  = document.getElementById('cnt-empty')
  const maxVal = Math.max(...allCounts.map(c => c.total), 1)
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
  tbody.innerHTML = filtered.map(c => {
    const pct = Math.round((c.total / maxVal) * 100)
    return `
      <tr>
        <td class="tag-cell">${c.tag}</td>
        <td><strong>${c.total}</strong></td>
        <td style="color:var(--on)">${c.total_on}</td>
        <td style="color:var(--text2)">${c.total_off}</td>
        <td>
          <div class="count-bar-wrap">
            <div class="count-bar-bg">
              <div class="count-bar-fill" style="width:${pct}%"></div>
            </div>
            <span style="font-family:var(--mono);font-size:10px;color:var(--text2);min-width:32px">${pct}%</span>
          </div>
        </td>
        <td style="color:var(--text2);font-size:11px">${c.last_event||'—'}</td>
        <td class="desc-cell" title="${c.description||''}">${c.description||'—'}</td>
      </tr>`
  }).join('')
}

function exportXLS() { window.open('/export') }

// ── INIT ──
loadSignals()
loadEvents()
loadStatus()
loadCounts()
setInterval(loadSignals, 2000)
setInterval(loadEvents,  5000)
setInterval(loadStatus,  2000)
setInterval(loadCounts,  30000)