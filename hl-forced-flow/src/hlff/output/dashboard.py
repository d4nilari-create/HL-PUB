"""Статический dashboard (без внешних CDN — работает офлайн).

Один HTML: опрашивает /api/* раз в 2 секунды.
"""

from __future__ import annotations

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>hl-forced-flow — live</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --bg:#0b0e14; --panel:#121722; --line:#1f2633; --txt:#cfd8e3; --mut:#6b7a90;
          --buy:#16c784; --sell:#ea3943; --acc:#4f8cff; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--txt); font:13px/1.45 -apple-system,"Segoe UI",Roboto,sans-serif; padding:16px; }
  h1 { font-size:17px; margin-bottom:4px; }
  h2 { font-size:13px; color:var(--mut); text-transform:uppercase; letter-spacing:.06em; margin:14px 0 6px; }
  .sub { color:var(--mut); font-size:11px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:12px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:12px; }
  .coin { font-weight:700; font-size:14px; }
  .mid { color:var(--acc); }
  .regime { float:right; padding:1px 8px; border-radius:10px; font-size:11px; border:1px solid var(--line); }
  .r-cascade { color:var(--sell); border-color:var(--sell); }
  .r-quiet { color:var(--mut); }
  .r-informed_trend { color:var(--buy); border-color:var(--buy); }
  table { width:100%; border-collapse:collapse; margin-top:8px; }
  th, td { text-align:right; padding:3px 6px; font-size:12px; }
  th { color:var(--mut); font-weight:500; border-bottom:1px solid var(--line); }
  td:first-child, th:first-child { text-align:left; }
  tr.sig td { border-bottom:1px dashed var(--line); }
  .long { color:var(--buy); } .short { color:var(--sell); }
  .conds { color:var(--mut); font-size:11px; padding:2px 6px 8px 6px; }
  .health { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:4px; }
  .ok { background:var(--buy); } .bad { background:var(--sell); }
  .muted { color:var(--mut); font-size:11px; }
  code { background:#1a2230; padding:0 4px; border-radius:4px; }
</style>
</head>
<body>
<h1>hl-forced-flow <span class="muted" id="uptime"></span></h1>
<div class="sub" id="sources"></div>

<h2>Сигналы</h2>
<div id="signals" class="grid"></div>

<h2>Pressure surface (бины от mid)</h2>
<div id="pressure" class="grid"></div>

<h2>Топ доноры ликвидаций</h2>
<div id="donors" class="grid"></div>

<script>
const $ = id => document.getElementById(id);
const fmt = (v, d=0) => v==null ? "—" : Number(v).toLocaleString("en-US", {maximumFractionDigits:d});
const fmtUsd = v => v==null ? "—" : (v>=1e6 ? (v/1e6).toFixed(2)+"M" : v>=1e3 ? (v/1e3).toFixed(1)+"k" : v.toFixed(0));

async function j(url){ const r = await fetch(url); if(!r.ok) throw new Error(url); return r.json(); }

async function tick(){
  try {
    const st = await j("/api/status");
    $("uptime").textContent = "uptime " + st.uptime_sec + "s · events " + fmt(st.n_events);
    $("sources").innerHTML = Object.entries(st.sources).map(([n,ok])=>
      `<span class="health ${ok?'ok':'bad'}"></span>${n}`).join("&nbsp;&nbsp;");

    const sigs = await j("/api/signals");
    $("signals").innerHTML = sigs.active.length ? sigs.active.map(s=>`
      <div class="card">
        <span class="coin ${s.direction}">${s.coin} ${s.direction.toUpperCase()}</span>
        <span class="muted">${s.kind} · ${s.state}</span>
        <table>${s.conditions.map(c=>`<tr class="sig"><td colspan="2" class="conds">· ${c}</td></tr>`).join("")}</table>
      </div>`).join("") : '<div class="card muted">нет активных сигналов</div>';

    const pres = await j("/api/pressure");
    $("pressure").innerHTML = pres.coins.map(p=>{
      if(!p.mid) return `<div class="card"><span class="coin">${p.coin}</span><div class="muted">mid недоступен</div></div>`;
      const rows = p.bins.map(b=>`
        <tr><td>${b.bin} bps</td>
        <td class="short">${fmtUsd(b.forced_sell_usd)}</td>
        <td class="long">${fmtUsd(b.forced_buy_usd)}</td>
        <td>${fmtUsd(b.book_bid_usd)}</td><td>${fmtUsd(b.book_ask_usd)}</td>
        <td>${fmt(b.sell_pressure_ratio,2)}/${fmt(b.buy_pressure_ratio,2)}</td></tr>`).join("");
      return `<div class="card">
        <span class="coin">${p.coin}</span> <span class="mid">${fmt(p.mid, p.mid<10?4:1)}</span>
        <span class="regime r-${p.regime}">${p.regime}</span>
        ${p.stops_stale ? '<span class="muted">stops stale</span>' : ''}
        <table><tr><th>bin</th><th>forced sell</th><th>forced buy</th><th>book bid</th><th>book ask</th><th>ratio s/b</th></tr>${rows}</table>
        <div class="muted">liq за окно: ${fmtUsd(p.liq_flow_window_usd)} USD</div>
      </div>`;}).join("");

    const don = await j("/api/donors");
    $("donors").innerHTML = don.coins.map(c=>`
      <div class="card"><span class="coin">${c.coin}</span>
      <table><tr><th>address</th><th>liq USD</th><th>pnl</th><th>n</th></tr>
      ${c.donors.map(d=>`<tr><td class="muted" style="font-size:10px">${d.user.slice(0,10)}…${d.user.slice(-6)}</td>
        <td>${fmtUsd(d.notional_usd)}</td><td>${fmt(d.realized_pnl,0)}</td><td>${d.liquidations}</td></tr>`).join("") || '<tr><td colspan="4" class="muted">—</td></tr>'}
      </table></div>`).join("");
  } catch(e) { /* следующий тик */ }
}
tick(); setInterval(tick, 2000);
</script>
</body>
</html>
"""
