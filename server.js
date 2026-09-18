import 'dotenv/config';
import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import WebSocket, { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8788);

// === Deriv bağlantısı ===
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';
const DERIV_API_TOKEN = process.env.DERIV_API_TOKEN || '';
// Əvvəl yeni public endpoint, alınmasa köhnə endpoint (avtomatik növbələnir)
const DERIV_WS_URLS = process.env.DERIV_WS_URL
  ? [process.env.DERIV_WS_URL]
  : [
      'wss://api.derivws.com/trading/v1/options/ws/public',
      `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`,
    ];
let urlIdx = 0;

// === Siqnal parametrləri ===
const MIN_CONFIDENCE = Number(process.env.MIN_CONFIDENCE || 72);
const SIGNAL_COOLDOWN_MIN = Number(process.env.SIGNAL_COOLDOWN_MIN || 10);
const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const ADMIN_API_KEY = process.env.ADMIN_API_KEY || '';
const STATE_FILE = process.env.STATE_FILE || '/tmp/deriv13-state.json';

const DEFAULT_SYMBOLS = ['R_10','R_25','R_50','R_75','R_100'];
const rawSyms = (process.env.DERIV_SYMBOLS || '').trim();
const symbols = rawSyms ? rawSyms.split(',').map(s => s.trim()).filter(Boolean) : DEFAULT_SYMBOLS.slice();

const TIMEFRAMES = ['1m', '5m'];
const TREND_TF = '15m';
const GRANULARITY = { '1m': 60, '5m': 300, '15m': 900 };
const ALL_TFS = [...TIMEFRAMES, TREND_TF];

function key(symbol, tf) { return `${symbol}|${tf}`; }

const state = {
  startedAt: Date.now(),
  derivConnected: false,
  scannerEnabled: true,
  candles: new Map(),
  lastAnalysis: new Map(),
  signals: [],
  lastSignalAt: new Map(),
  clients: new Set(),
  activeSymbols: symbols.slice(),
  availableSymbols: [],
};

function saveState() {
  const dump = {
    signals: state.signals.slice(-200),
    lastSignalAt: [...state.lastSignalAt.entries()],
  };
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(dump)); } catch (e) {}
}
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const d = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    if (Array.isArray(d.signals)) state.signals = d.signals;
    if (Array.isArray(d.lastSignalAt)) state.lastSignalAt = new Map(d.lastSignalAt);
  } catch (e) {}
}

function ema(a,p){ if(a.length<p) return null; const k=2/(p+1); let e=a.slice(0,p).reduce((x,y)=>x+y,0)/p; for(let i=p;i<a.length;i++) e=a[i]*k+e*(1-k); return e; }
function sma(a,p){ if(a.length<p) return null; return a.slice(-p).reduce((x,y)=>x+y,0)/p; }
function rsi(a,p=14){ if(a.length<p+1)return null; let g=0,l=0; for(let i=1;i<=p;i++){const d=a[i]-a[i-1];if(d>0)g+=d;else l-=d;}let ag=g/p,al=l/p;for(let i=p+1;i<a.length;i++){const d=a[i]-a[i-1];ag=(ag*(p-1)+(d>0?d:0))/p;al=(al*(p-1)+(d<0?-d:0))/p;}return al===0?100:100-100/(1+ag/al); }
function atr(c,p=14){ if(c.length<p+1)return null; const tr=[];for(let i=1;i<c.length;i++)tr.push(Math.max(c[i].h-c[i].l,Math.abs(c[i].h-c[i-1].c),Math.abs(c[i].l-c[i-1].c)));return ema(tr,p); }
function bollinger(a,p=20,m=2){if(a.length<p)return null;const mid=sma(a,p),s=a.slice(-p).reduce((z,x)=>z+(x-mid)**2,0)/p,sd=Math.sqrt(s);return{mid,upper:mid+m*sd,lower:mid-m*sd};}
function macd(a){if(a.length<35)return null;const fast=ema(a.slice(-80),12),slow=ema(a.slice(-80),26);return fast==null||slow==null?null:fast-slow;}
function stochastic(c,p=14){if(c.length<p)return null;const x=c.slice(-p),hh=Math.max(...x.map(z=>z.h)),ll=Math.min(...x.map(z=>z.l));return hh===ll?50:((x.at(-1).c-ll)/(hh-ll))*100;}
function cci(c,p=20){if(c.length<p)return null;const t=c.slice(-p).map(x=>(x.h+x.l+x.c)/3),m=t.reduce((a,b)=>a+b,0)/p,d=t.reduce((a,b)=>a+Math.abs(b-m),0)/p;return d===0?0:(t.at(-1)-m)/(0.015*d);}
function mfi(c,p=14){if(c.length<p+1)return null;let pos=0,neg=0;for(let i=c.length-p;i<c.length;i++){const a=(c[i-1].h+c[i-1].l+c[i-1].c)/3,b=(c[i].h+c[i].l+c[i].c)/3,m=b*c[i].v;if(b>a)pos+=m;else if(b<a)neg+=m;}if(neg===0)return 100;const r=pos/neg;return 100-100/(1+r);}
function obvTrend(c,n=10){if(c.length<n+1)return 0;let s=0;for(let i=c.length-n;i<c.length;i++){if(c[i].c>c[i-1].c)s+=c[i].v;else if(c[i].c<c[i-1].c)s-=c[i].v;}return s;}
function structure(c){if(c.length<20)return 0;const n=10,a=c.slice(-n),b=c.slice(-2*n,-n),ah=Math.max(...a.map(x=>x.h)),al=Math.min(...a.map(x=>x.l)),bh=Math.max(...b.map(x=>x.h)),bl=Math.min(...b.map(x=>x.l));return ah>bh&&al>bl?1:ah<bh&&al<bl?-1:0;}
function adx(c,p=14){
  if(c.length<p*2+1)return null;
  const plusDM=[],minusDM=[],tr=[];
  for(let i=1;i<c.length;i++){
    const up=c[i].h-c[i-1].h, down=c[i-1].l-c[i].l;
    plusDM.push(up>down&&up>0?up:0);
    minusDM.push(down>up&&down>0?down:0);
    tr.push(Math.max(c[i].h-c[i].l,Math.abs(c[i].h-c[i-1].c),Math.abs(c[i].l-c[i-1].c)));
  }
  const atrS=ema(tr,p);if(!atrS)return null;
  const plusDI=100*(ema(plusDM,p)||0)/atrS, minusDI=100*(ema(minusDM,p)||0)/atrS;
  const sum=plusDI+minusDI;
  return sum===0?0:100*Math.abs(plusDI-minusDI)/sum;
}
function vwap(c,n=50){
  if(c.length<n)return null;
  const s=c.slice(-n);let pv=0,vv=0;
  for(const x of s){pv+=((x.h+x.l+x.c)/3)*x.v;vv+=x.v;}
  return vv===0?null:pv/vv;
}
function ichimoku(c){
  if(c.length<52)return null;
  const mid=n=>{const s=c.slice(-n);return(Math.max(...s.map(x=>x.h))+Math.min(...s.map(x=>x.l)))/2;};
  const tenkan=mid(9),kijun=mid(26),spanA=(tenkan+kijun)/2,spanB=mid(52),price=c.at(-1).c;
  const top=Math.max(spanA,spanB),bot=Math.min(spanA,spanB);
  const score=price>top?1:(price<bot?-1:0);
  return{tenkan,kijun,spanA,spanB,score};
}
function parabolicSar(c){
  if(c.length<10)return null;
  let af=0.02,maxAf=0.2,sar=c[0].l,ep=c[0].h,uptrend=true;
  for(let i=1;i<c.length;i++){
    sar=sar+af*(ep-sar);
    if(uptrend){
      if(c[i].l<sar){uptrend=false;sar=ep;af=0.02;ep=c[i].l;}
      else if(c[i].h>ep){ep=c[i].h;af=Math.min(af+0.02,maxAf);}
    }else{
      if(c[i].h>sar){uptrend=true;sar=ep;af=0.02;ep=c[i].h;}
      else if(c[i].l<ep){ep=c[i].l;af=Math.min(af+0.02,maxAf);}
    }
  }
  return{sar,uptrend};
}
function pivotPoints(c){
  if(c.length<2)return null;
  const p=c[c.length-2],pp=(p.h+p.l+p.c)/3;
  return{pp,r1:2*pp-p.l,s1:2*pp-p.h,r2:pp+(p.h-p.l),s2:pp-(p.h-p.l)};
}
function fibLevels(c){
  const n=Math.min(c.length,50),s=c.slice(-n),hi=Math.max(...s.map(x=>x.h)),lo=Math.min(...s.map(x=>x.l)),diff=hi-lo;
  return{hi,lo,r382:hi-diff*0.382,r500:hi-diff*0.5,r618:hi-diff*0.618};
}

function analyze(c){
  if(c.length<60)return null;
  const close=c.map(x=>x.c),price=close.at(-1),e9=ema(close,9),e21=ema(close,21),e50=ema(close,50),e200=ema(close,200),rv=rsi(close),mv=macd(close),bb=bollinger(close),st=stochastic(c),av=atr(c),cv=cci(c),mf=mfi(c),ob=obvTrend(c),str=structure(c),ax=adx(c),vw=vwap(c),ich=ichimoku(c),psar=parabolicSar(c),piv=pivotPoints(c),fib=fibLevels(c);
  let score=0,reasons=[];
  if(e9>e21&&e21>e50){score+=1;reasons.push('EMA trend +');}else if(e9<e21&&e21<e50){score-=1;reasons.push('EMA trend -');}
  if(e200!=null){if(price>e200){score+=1;reasons.push('EMA200 üzərində');}else{score-=1;reasons.push('EMA200 altında');}}
  if(rv<35){score+=1;reasons.push('RSI oversold');}else if(rv>65){score-=1;reasons.push('RSI overbought');}
  if(mv>0){score+=1;reasons.push('MACD +');}else if(mv<0){score-=1;reasons.push('MACD -');}
  if(bb){if(price<=bb.lower){score+=1;reasons.push('BB alt zolaq');}else if(price>=bb.upper){score-=1;reasons.push('BB üst zolaq');}}
  if(st<20){score+=1;reasons.push('Stoch oversold');}else if(st>80){score-=1;reasons.push('Stoch overbought');}
  if(cv<-100){score+=1;reasons.push('CCI oversold');}else if(cv>100){score-=1;reasons.push('CCI overbought');}
  if(mf<25){score+=1;reasons.push('MFI low');}else if(mf>75){score-=1;reasons.push('MFI high');}
  if(ob>0){score+=1;reasons.push('OBV yüksəlir');}else if(ob<0){score-=1;reasons.push('OBV düşür');}
  if(str>0){score+=1;reasons.push('higher highs/lows');}else if(str<0){score-=1;reasons.push('lower highs/lows');}
  if(vw!=null){if(price>vw){score+=1;reasons.push('VWAP üzərində');}else{score-=1;reasons.push('VWAP altında');}}
  if(ich&&ich.score!==0){if(ich.score>0){score+=1;reasons.push('Ichimoku bulud üzərində');}else{score-=1;reasons.push('Ichimoku bulud altında');}}
  if(psar){if(psar.uptrend){score+=1;reasons.push('Parabolic SAR yüksəliş');}else{score-=1;reasons.push('Parabolic SAR düşüş');}}
  if(piv){if(price>piv.pp){score+=1;reasons.push('Pivot üzərində');}else if(price<piv.pp){score-=1;reasons.push('Pivot altında');}}
  if(fib){if(price>fib.r500){score+=1;reasons.push('Fib 50% üzərində');}else{score-=1;reasons.push('Fib 50% altında');}}
  let confidence=Math.round(Math.min(100,Math.abs(score)/15*100));
  if(ax!=null){
    if(ax>=25){confidence=Math.min(100,confidence+8);reasons.push(`ADX güclü trend (${ax.toFixed(0)})`);}
    else if(ax<15){confidence=Math.max(0,confidence-12);reasons.push(`ADX zəif/yan bazar (${ax.toFixed(0)})`);}
  }
  const signal=score>=4?'LONG':score<=-4?'SHORT':'WAIT';
  return{signal,confidence,score,price,atr:av,rsi:rv,adx:ax,reasons};
}

function fullAnalysis(symbol){
  const a = {};
  for (const tf of ALL_TFS) a[tf] = analyze(getCandles(symbol, tf));
  const m1 = a['1m'], m5 = a['5m'], m15 = a[TREND_TF];
  if (!m1) return null;

  let final = 'WAIT', expiry = null, strength = 'zəif';
  if (m5 && m1.signal !== 'WAIT' && m1.signal === m5.signal) {
    final = m1.signal; expiry = '5m'; strength = 'güclü (1m+5m uyğun)';
  } else if (m15 && m1.signal !== 'WAIT' && m1.signal === m15.signal) {
    final = m1.signal; expiry = '1m'; strength = 'erkən (1m+15m trend uyğun)';
  }

  let confidence = m1.confidence;
  if (final !== 'WAIT') {
    const parts = [m1.confidence, m5?.confidence, m15?.confidence].filter(x => x != null);
    confidence = Math.round(parts.reduce((s, x) => s + x, 0) / parts.length);
    if (m5 && m5.signal === final) confidence = Math.min(100, confidence + 6);
    if (m15 && m15.signal === final) confidence = Math.min(100, confidence + 4);
  }

  const dir = final === 'LONG' ? 'CALL' : final === 'SHORT' ? 'PUT' : 'WAIT';
  const reasons = [...new Set([...(m1?.reasons||[]), ...(m5?.reasons||[])])].slice(0, 6);
  return { symbol, dir, confidence, expiry, strength, price: m1.price, atr: m1.atr, rsi: m1.rsi, timeframes: a, reasons };
}

function getCandles(symbol, tf) { return state.candles.get(key(symbol, tf)) || []; }

let derivWs = null;
let reqSeq = 1;
let pingTimer = null;
let reconnectTimer = null;
// req_id -> { symbol, tf }  (yeni Deriv API cavabda echo_req qaytarmaya bilər)
const reqMeta = new Map();
// subscription.id -> { symbol, tf }
const subMeta = new Map();

// Köhnə API: s.symbol | Yeni API: s.underlying_symbol
const symOf = (s) => (s && (s.symbol || s.underlying_symbol)) || null;

function connectDeriv() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  reqMeta.clear();
  subMeta.clear();
  const wsUrl = DERIV_WS_URLS[urlIdx % DERIV_WS_URLS.length];
  console.log(`[deriv] qoşulur: ${wsUrl.split('?')[0]}`);
  derivWs = new WebSocket(wsUrl);
  const ws = derivWs;
  let gotCandles = false;
  let invalidCount = 0;
  let opened = false;
  let rotated = false;
  const rotate = (why) => {
    if (rotated || DERIV_WS_URLS.length < 2) return;
    rotated = true;
    urlIdx++;
    console.warn(`[deriv] ${why} — başqa endpoint-ə keçilir`);
    try { ws.close(); } catch {}
  };

  ws.on('open', () => {
    opened = true;
    state.derivConnected = true;
    console.log('[deriv] bağlantı quruldu, aktiv simvollar soruşulur...');
    ws.send(JSON.stringify({ active_symbols: 'brief', req_id: reqSeq++ }));
    // Deriv boş qalan bağlantını bağlayır — hər 30 san ping
    clearInterval(pingTimer);
    pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 }));
    }, 30000);
  });

  function subscribeCandles(symbolList) {
    for (const symbol of symbolList) {
      for (const tf of ALL_TFS) {
        const id = reqSeq++;
        reqMeta.set(id, { symbol, tf });
        ws.send(JSON.stringify({
          ticks_history: symbol,
          style: 'candles',
          granularity: GRANULARITY[tf],
          count: 300,
          end: 'latest',
          subscribe: 1,
          req_id: id,
        }));
      }
    }
  }

  // Cavabdan (symbol, tf) tapır: req_id → subscription.id → echo_req → sahələr
  function resolveMeta(msg, granularity, symbolField) {
    let meta = (msg.req_id != null && reqMeta.get(msg.req_id)) || null;
    if (!meta && msg.subscription && msg.subscription.id) meta = subMeta.get(msg.subscription.id) || null;
    if (!meta && msg.echo_req) {
      const symbol = msg.echo_req.ticks_history;
      const tf = tfFromGranularity(msg.echo_req.granularity);
      if (symbol && tf) meta = { symbol, tf };
    }
    if (!meta && symbolField && granularity) {
      const tf = tfFromGranularity(granularity);
      if (tf) meta = { symbol: symbolField, tf };
    }
    if (meta && msg.subscription && msg.subscription.id) subMeta.set(msg.subscription.id, meta);
    return meta;
  }

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.msg_type === 'ping') return;
    if (msg.error) {
      const m = msg.req_id != null ? reqMeta.get(msg.req_id) : null;
      console.error('[deriv] xəta:', msg.error.message, m ? `(${m.symbol} ${m.tf})` : '');
      if (/invalid/i.test(msg.error.message || '') && m) {
        invalidCount++;
        if (!gotCandles && invalidCount >= state.activeSymbols.length * ALL_TFS.length) {
          rotate('bütün simvollar etibarsız sayıldı');
        }
      }
      return;
    }

    if (msg.msg_type === 'active_symbols' && Array.isArray(msg.active_symbols)) {
      const list = msg.active_symbols;
      const all = list.map(symOf).filter(Boolean);
      const nameOf = (s) => s.underlying_symbol_name || s.display_name || '';
      const synthetic = list.filter(s => s.market === 'synthetic_index');
      state.availableSymbols = all;
      console.log(`[deriv] cəmi ${all.length} simvol, ${synthetic.length} sintetik`);
      if (!list.length) {
        console.warn('[deriv] xam cavab:', String(raw).slice(0, 300));
      } else {
        console.log('[deriv] nümunə sahələr:', Object.keys(list[0]).join(','));
        console.log('[deriv] sintetik simvollar:', synthetic.slice(0, 40).map(s => `${symOf(s)}="${nameOf(s)}"`).join(' | '));
      }

      // Hər tələb olunan simvol üçün: dəqiq ad → "Volatility N Index" adı ilə axtarış
      const resolved = [];
      for (const want of symbols) {
        if (all.includes(want)) { resolved.push(want); continue; }
        const m = /^R_(\d+)$/.exec(want);
        if (m) {
          const re = new RegExp(`^volatility\\s*${m[1]}\\s*index$`, 'i');
          const hit = list.find(s => re.test(nameOf(s).trim()));
          if (hit) { console.log(`[deriv] ${want} → ${symOf(hit)} (ad ilə tapıldı)`); resolved.push(symOf(hit)); continue; }
        }
        console.warn(`[deriv] simvol tapılmadı: ${want}`);
      }
      const finalSymbols = [...new Set(resolved)];

      if (!finalSymbols.length) {
        rotate('heç bir simvol tapılmadı');
        return;
      }
      state.activeSymbols = finalSymbols;
      console.log(`[deriv] İstifadə olunan simvollar: ${finalSymbols.join(', ')}`);
      subscribeCandles(finalSymbols);
      return;
    }

    if (msg.msg_type === 'candles' && Array.isArray(msg.candles)) {
      const meta = resolveMeta(msg, msg.echo_req && msg.echo_req.granularity, msg.echo_req && msg.echo_req.ticks_history);
      if (!meta) return;
      gotCandles = true;
      const arr = msg.candles.map(k => ({ t: k.epoch * 1000, o: +k.open, h: +k.high, l: +k.low, c: +k.close, v: 1 }));
      state.candles.set(key(meta.symbol, meta.tf), arr);
      return;
    }

    if (msg.msg_type === 'ohlc' && msg.ohlc) {
      const o = msg.ohlc;
      const meta = resolveMeta(msg, o.granularity, o.symbol || o.underlying_symbol);
      if (!meta) return;
      const { symbol, tf } = meta;
      const arr = state.candles.get(key(symbol, tf)) || [];
      const epochMs = Number(o.open_time) * 1000;
      const candle = { t: epochMs, o: +o.open, h: +o.high, l: +o.low, c: +o.close, v: 1 };
      const last = arr[arr.length - 1];
      if (last && last.t === epochMs) arr[arr.length - 1] = candle;
      else arr.push(candle);
      state.candles.set(key(symbol, tf), arr.slice(-500));
      onCandleUpdate(symbol);
    }
  });

  ws.on('close', () => {
    state.derivConnected = false;
    clearInterval(pingTimer);
    if (!opened && DERIV_WS_URLS.length > 1) urlIdx++;
    console.log('[deriv] bağlantı kəsildi, 3 saniyə sonra yenidən qoşulacaq');
    if (!reconnectTimer) reconnectTimer = setTimeout(connectDeriv, 3000);
  });
  ws.on('error', (e) => console.error('[deriv] WS xətası:', e.message));
}
function tfFromGranularity(g) {
  for (const tf of Object.keys(GRANULARITY)) if (GRANULARITY[tf] === Number(g)) return tf;
  return null;
}

let analysisTimers = new Map();
function onCandleUpdate(symbol) {
  if (analysisTimers.has(symbol)) return;
  analysisTimers.set(symbol, setTimeout(() => {
    analysisTimers.delete(symbol);
    runAnalysis(symbol);
  }, 500));
}

function runAnalysis(symbol) {
  const r = fullAnalysis(symbol);
  if (!r) return;
  state.lastAnalysis.set(symbol, r);
  broadcast({ type: 'analysis', data: r });

  if (!state.scannerEnabled) return;
  if (r.dir === 'WAIT' || r.confidence < MIN_CONFIDENCE) return;

  const prev = state.lastSignalAt.get(symbol);
  const cooldownMs = SIGNAL_COOLDOWN_MIN * 60 * 1000;
  const now = Date.now();
  if (prev && prev.dir === r.dir && now - prev.ts < cooldownMs) return;

  state.lastSignalAt.set(symbol, { dir: r.dir, ts: now });
  const entry = { ts: now, symbol, dir: r.dir, confidence: r.confidence, expiry: r.expiry, price: r.price, reasons: r.reasons };
  state.signals.unshift(entry);
  state.signals = state.signals.slice(0, 200);
  saveState();
  broadcast({ type: 'signal', data: entry });
  sendTelegramSignal(r);
}

async function telegram(method, body) {
  if (!TELEGRAM_TOKEN) { console.error('[telegram] TELEGRAM_BOT_TOKEN boşdur'); return null; }
  try {
    const r = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/${method}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    });
    const j = await r.json();
    if (j && j.ok === false) console.error(`[telegram] rədd edildi: ${j.description}`);
    return j;
  } catch (e) { console.error('[telegram] şəbəkə xətası:', e.message); return null; }
}
function confBar(pct) { const filled = Math.round((pct || 0) / 10); return '█'.repeat(filled) + '░'.repeat(10 - filled); }
function fmt(x) { return x == null ? '--' : Number(x).toLocaleString('en-US', { maximumFractionDigits: 5 }); }
async function sendTelegramSignal(r) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) { console.error('[telegram] TOKEN/CHAT_ID boşdur, siqnal göndərilmədi'); return; }
  const emoji = r.dir === 'CALL' ? '📈' : '📉';
  const dirText = r.dir === 'CALL' ? 'CALL (yuxarı)' : 'PUT (aşağı)';
  const expText = r.expiry === '5m' ? '5 dəqiqə' : '1 dəqiqə';
  const lines = [
    `${emoji} <b>${r.symbol}</b> — <b>${dirText}</b>`,
    `Tövsiyə olunan expiry: <b>${expText}</b> (${r.strength})`,
    `Etibar: <b>${r.confidence}%</b> ${confBar(r.confidence)}`,
    `Qiymət: <code>${fmt(r.price)}</code>`,
    `Səbəblər: ${r.reasons.join(', ')}`,
    `<i>Bu avtomatik texniki siqnaldır, maliyyə məsləhəti deyil. Auto-trade hələ aktiv deyil.</i>`,
  ];
  await telegram('sendMessage', { chat_id: TELEGRAM_CHAT_ID, text: lines.join('\n'), parse_mode: 'HTML' });
}

function broadcast(msg) {
  const s = JSON.stringify(msg);
  for (const c of state.clients) { if (c.readyState === 1) c.send(s); }
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

function requireAdmin(req, res, next) {
  if (!ADMIN_API_KEY) return res.status(403).json({ error: 'ADMIN_API_KEY konfiqurasiya olunmayıb' });
  if (req.headers['x-admin-key'] !== ADMIN_API_KEY) return res.status(401).json({ error: 'yanlış admin key' });
  next();
}

app.get('/api/state', (req, res) => {
  res.json({
    online: true,
    derivConnected: state.derivConnected,
    scannerEnabled: state.scannerEnabled,
    symbols: state.activeSymbols,
    signals: state.signals.slice(0, 50),
    analysis: Object.fromEntries(state.lastAnalysis),
    startedAt: state.startedAt,
  });
});
app.get('/api/analyze/:sym', (req, res) => {
  const r = state.lastAnalysis.get(req.params.sym) || fullAnalysis(req.params.sym);
  if (!r) return res.status(404).json({ error: 'kifayət qədər data yoxdur' });
  res.json(r);
});
app.get('/api/candles/:sym/:tf', (req, res) => {
  res.json(getCandles(req.params.sym, req.params.tf));
});
app.post('/api/scanner', requireAdmin, (req, res) => {
  state.scannerEnabled = !!req.body.enabled;
  broadcast({ type: 'state', data: { scannerEnabled: state.scannerEnabled } });
  res.json({ ok: true, scannerEnabled: state.scannerEnabled });
});

const server = app.listen(PORT, () => console.log(`[http] http://localhost:${PORT} dinlənilir`));
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
  state.clients.add(ws);
  ws.send(JSON.stringify({ type: 'state', data: { scannerEnabled: state.scannerEnabled, derivConnected: state.derivConnected } }));
  ws.on('close', () => state.clients.delete(ws));
});

loadState();
connectDeriv();
