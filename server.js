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
const DERIV_API_TOKEN = (process.env.DERIV_API_TOKEN || '').trim();
// Deriv minimum stake = 0.50; default 10$ (Railway Variables-da DERIV_STAKE_AMOUNT varsa o əvəz edir)
const DERIV_STAKE_AMOUNT = Math.max(0.5, Number(process.env.DERIV_STAKE_AMOUNT || 10));
// Əvvəl yeni public endpoint, alınmasa köhnə endpoint (avtomatik növbələnir)
const DERIV_WS_URLS = process.env.DERIV_WS_URL
  ? [process.env.DERIV_WS_URL]
  : [
      'wss://api.derivws.com/trading/v1/options/ws/public',
      `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`,
    ];
let urlIdx = 0;

// === Siqnal parametrləri ===
// QEYD: əvvəlki dəyər (15) demək olar heç nəyi filtrləmirdi — score>=4 həddi ilə siqnal
// yarandığı andaca confidence artıq ~27%-dən başlayır, ona görə 15% praktikada "filtrsiz" idi
// və çoxlu zəif/yalan siqnal göndərirdi. backtest.js-in öz bucket analizi göstərir ki, real
// statistik üstünlük yalnız ~65%+ etibar diapazonunda görünür — canlı botu da elə kalibrləyirik.
const MIN_CONFIDENCE = Number(process.env.MIN_CONFIDENCE || 40);
const SIGNAL_COOLDOWN_MIN = Number(process.env.SIGNAL_COOLDOWN_MIN || 3);
// Kiçik balans (~$20) üçün qoruyucu parametrlər: stake ≈ balansın ~2%-i, gündəlik əməliyyat limiti,
// və siqnal yalnız 5m+15m+1h hər üçü eyni istiqamətdə olanda verilir
const MAX_DAILY_TRADES = Number(process.env.MAX_DAILY_TRADES || 40);
// Avtomatik alış və hesab səviyyəli risk limitləri (başlanğıc balans üzərindən faizlə)
let AUTO_TRADE = (process.env.AUTO_TRADE || 'true') !== 'false'; // /api/autotrade ilə dəyişdirilə bilir
const START_BALANCE = Number(process.env.START_BALANCE || 500);
const STOP_LOSS_PCT = Number(process.env.STOP_LOSS_PCT || 30);
const TP_MIN_PCT = Number(process.env.TAKE_PROFIT_MIN_PCT || 30);
const TP_MAX_PCT = Number(process.env.TAKE_PROFIT_MAX_PCT || 50);
const MAX_OPEN_TRADES = Number(process.env.MAX_OPEN_TRADES || 10);
const CONSEC_LOSS_PAUSE = Number(process.env.CONSEC_LOSS_PAUSE || 3);
const PAUSE_MIN = Number(process.env.PAUSE_MIN || 15);
const STATUS_EVERY_MIN = Number(process.env.STATUS_EVERY_MIN || 30);
const REQUIRE_FULL_CONFLUENCE = (process.env.REQUIRE_FULL_CONFLUENCE || 'true') !== 'false';
const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const ADMIN_API_KEY = process.env.ADMIN_API_KEY || '';
// Railway-də /tmp redeploy zamanı silinir. Volume /data-ya qoşulubsa avtomatik oradan istifadə olunur.
const STATE_FILE = process.env.STATE_FILE || (fs.existsSync('/data') ? '/data/deriv13-state.json' : '/tmp/deriv13-state.json');
// Siqnal yalnız BAĞLANMIŞ 5m şamlar üzərində hesablanır (formalaşan şamda siqnal "repaint" etməsin)
const CLOSED_CANDLE_ONLY = (process.env.CLOSED_CANDLE_ONLY || 'true') !== 'false';
// Real hesab balansını Deriv-dən oxuyub risk limitlərini ona görə hesabla (START_BALANCE = yuxarı tavan)
const USE_REAL_BALANCE = (process.env.USE_REAL_BALANCE || 'true') !== 'false';
// Eyni simvol üçün eyni xəta mesajı Telegram-a nə qədər tez-tez göndərilsin
const ERR_NOTIFY_COOLDOWN_MIN = Number(process.env.ERR_NOTIFY_COOLDOWN_MIN || 10);
// Düymələri yalnız bu Telegram istifadəçiləri (vergüllə ID) basa bilər; boşdursa yalnız TELEGRAM_CHAT_ID çatının özü
const TELEGRAM_ALLOWED_USER_IDS = (process.env.TELEGRAM_ALLOWED_USER_IDS || '').split(',').map(x => x.trim()).filter(Boolean);

// Real bazarlar: forex cütlükləri, qızıl, gümüş, neft (Deriv simvol adları).
// Qeyd: Railway-də DERIV_SYMBOLS dəyişəni qurulubsa, o bu siyahını əvəz edir — silin və ya yeniləyin.
const DEFAULT_SYMBOLS = ['frxEURUSD','frxGBPUSD','frxUSDJPY','frxAUDUSD','frxXAUUSD','frxXAGUSD','frxBROUSD'];
// Dəqiq simvol adı tapılmasa, ekran adına görə axtarış (məs. neft üçün Deriv-də ad fərqli ola bilər)
const NAME_HINTS = {
  frxXAUUSD: /gold/i, frxXAGUSD: /silver/i, frxBROUSD: /(brent|oil)/i,
  frxEURUSD: /eur\s*\/\s*usd/i, frxGBPUSD: /gbp\s*\/\s*usd/i, frxUSDJPY: /usd\s*\/\s*jpy/i, frxAUDUSD: /aud\s*\/\s*usd/i,
};
// Forex/əmtəə bazarı həftəsonu bağlıdır (təqribən Cümə 22:00 UTC → Bazar 22:00 UTC)
function isWeekendClosed() {
  const d = new Date(), day = d.getUTCDay(), h = d.getUTCHours();
  return day === 6 || (day === 5 && h >= 22) || (day === 0 && h < 22);
}
const rawSyms = (process.env.DERIV_SYMBOLS || '').trim();
const symbols = rawSyms ? rawSyms.split(',').map(s => s.trim()).filter(Boolean) : DEFAULT_SYMBOLS.slice();

const TIMEFRAMES = ['15m'];
const TREND_TF = '1h';
const CONFIRM_TF = '5m';
const GRANULARITY = { '5m': 300, '15m': 900, '1h': 3600 };
const ALL_TFS = [CONFIRM_TF, ...TIMEFRAMES, TREND_TF];

// === Çox simvol + API limitinə uyğunluq ===
// DERIV_SYMBOLS boşdursa: bütün forex / əmtəə / kripto simvolları avtomatik seçilir (bazar başına limitlə).
const AUTO_MARKETS = !rawSyms;
const MARKET_CAPS = {
  forex: Number(process.env.MAX_FOREX || 25),
  commodities: Number(process.env.MAX_COMMODITIES || 10),
  cryptocurrency: Number(process.env.MAX_CRYPTO || 8),
};
const SUB_RANK = { major_pairs: 0, minor_pairs: 1 };
// Hər simvol üçün YALNIZ BİR axın (5m) açılır; 15m və 1h şamları ondan yığılır → sorğu sayı 3 dəfə azalır
const HISTORY_COUNT = Number(process.env.HISTORY_COUNT || 4000);
// Abunə sorğuları arasında fasilə (ms) — Deriv-in dəqiqəlik sorğu limitini aşmamaq üçün
const SUB_INTERVAL_MS = Number(process.env.SUB_INTERVAL_MS || 800);
const ANALYSIS_DEBOUNCE_MS = Number(process.env.ANALYSIS_DEBOUNCE_MS || 2000);

function key(symbol, tf) { return `${symbol}|${tf}`; }

// Siqnalların real nəticə statistikası (expiry bitəndə qiymət müqayisəsi ilə)
const stats = { total: 0, wins: 0, buckets: {} };
let resumeQueue = [];
let tgUpdateOffset = 0;

const state = {
  startedAt: Date.now(),
  derivConnected: false,
  scannerEnabled: true,
  candles: new Map(),
  lastAnalysis: new Map(),
  signals: [],
  lastSignalAt: new Map(),
  pendingDir: new Map(),
  lastSignalBar: new Map(),
  clients: new Set(),
  activeSymbols: symbols.slice(),
  availableSymbols: [],
};

function buildDump() {
  return {
    signals: state.signals.slice(0, 200),
    lastSignalAt: [...state.lastSignalAt.entries()],
    stats,
    tgUpdateOffset,
    session: { day: tradeDay, tradesToday, pnl: session.pnl, wins: session.wins, losses: session.losses, streak: session.streak, pausedUntil: session.pausedUntil, halted: session.halted, reachedTP: session.reachedTP, startBalance: session.startBalance },
    // Açıq əməliyyatlar: restartdan sonra izləməni bərpa etmək üçün
    open: [...session.open.values()].filter((o) => !o.pending && o.contractId),
  };
}
let saving = false, saveAgain = false;
function saveState() {
  if (saving) { saveAgain = true; return; }
  saving = true;
  fs.writeFile(STATE_FILE, JSON.stringify(buildDump()), (e) => {
    if (e) console.error('[state] yazma xətası:', e.message);
    saving = false;
    if (saveAgain) { saveAgain = false; saveState(); }
  });
}
function saveStateSync() { fs.writeFileSync(STATE_FILE, JSON.stringify(buildDump())); }
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const d = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    if (Array.isArray(d.signals)) state.signals = d.signals;
    if (Array.isArray(d.lastSignalAt)) state.lastSignalAt = new Map(d.lastSignalAt);
    if (d.stats && typeof d.stats.total === 'number') Object.assign(stats, d.stats);
    if (Number.isFinite(d.tgUpdateOffset)) tgUpdateOffset = d.tgUpdateOffset;
    if (Array.isArray(d.open)) resumeQueue = d.open;
    // Server yenidən başlayanda gündəlik P&L / stop-loss sayğacı sıfırlanmasın
    if (d.session && d.session.day === new Date().toISOString().slice(0, 10)) {
      tradeDay = d.session.day; tradesToday = d.session.tradesToday || 0;
      const { pnl, wins, losses, streak, pausedUntil, halted, reachedTP, startBalance } = d.session;
      Object.assign(session, { pnl: pnl || 0, wins: wins || 0, losses: losses || 0, streak: streak || 0, pausedUntil: pausedUntil || 0, halted: !!halted, reachedTP: !!reachedTP, startBalance: startBalance || null });
    }
  } catch (e) { console.error('[state] oxuma xətası:', e.message); }
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
function williamsR(c,p=14){
  if(c.length<p)return null;
  const x=c.slice(-p),hh=Math.max(...x.map(z=>z.h)),ll=Math.min(...x.map(z=>z.l));
  return hh===ll?-50:((hh-x.at(-1).c)/(hh-ll))*-100;
}
function keltnerChannel(c,close,p=20,mult=2){
  if(c.length<p||close.length<p)return null;
  const mid=ema(close.slice(-p),p);
  const av=atr(c,p);
  if(mid==null||av==null)return null;
  return{mid,upper:mid+mult*av,lower:mid-mult*av};
}
function heikinAshiTrend(c,n=5){
  if(c.length<n+1)return 0;
  let haO=(c[0].o+c[0].c)/2, haC=(c[0].o+c[0].h+c[0].l+c[0].c)/4;
  let up=0,down=0;
  for(let i=1;i<c.length;i++){
    const newHaC=(c[i].o+c[i].h+c[i].l+c[i].c)/4;
    const newHaO=(haO+haC)/2;
    haO=newHaO; haC=newHaC;
    if(i>=c.length-n){ if(haC>haO) up++; else if(haC<haO) down++; }
  }
  return up>down?1:down>up?-1:0;
}
function superTrend(c,p=10,mult=3){
  if(c.length<p+1)return null;
  let trend=true, finalUpper=null, finalLower=null;
  const start=Math.max(1,c.length-60);
  for(let i=start;i<c.length;i++){
    const av=atr(c.slice(0,i+1),p);
    if(av==null)continue;
    const hl2=(c[i].h+c[i].l)/2;
    let upperBand=hl2+mult*av, lowerBand=hl2-mult*av;
    if(finalUpper==null){finalUpper=upperBand;finalLower=lowerBand;}
    else{
      upperBand=(upperBand<finalUpper||c[i-1].c>finalUpper)?upperBand:finalUpper;
      lowerBand=(lowerBand>finalLower||c[i-1].c<finalLower)?lowerBand:finalLower;
      finalUpper=upperBand; finalLower=lowerBand;
    }
    if(trend&&c[i].c<finalLower)trend=false;
    else if(!trend&&c[i].c>finalUpper)trend=true;
  }
  return{uptrend:trend};
}

function analyze(c){
  if(c.length<60)return null;
  const close=c.map(x=>x.c),price=close.at(-1),e9=ema(close,9),e21=ema(close,21),e50=ema(close,50),e200=ema(close,200),rv=rsi(close),mv=macd(close),bb=bollinger(close),st=stochastic(c),av=atr(c),cv=cci(c),str=structure(c),ax=adx(c),ich=ichimoku(c),psar=parabolicSar(c),piv=pivotPoints(c),fib=fibLevels(c),wr=williamsR(c),kelt=keltnerChannel(c,close),ha=heikinAshiTrend(c),stnd=superTrend(c);
  // QEYD: OBV/MFI/VWAP HESABLANMIR — Deriv sintetik indekslərində real "volume" yoxdur
  // (aşağıda hər şam üçün v:1 sabit qoyulur), ona görə bu indikatorlar burda mənasız/aldadıcı
  // olardı və score-a əlavə "sanki-güvən" verərdi. OKX kripto versiyasında (real hədcm datası ilə)
  // bunlar saxlanılıb, çünki orda faktiki məna daşıyır.
  let score=0,reasons=[];
  if(e9>e21&&e21>e50){score+=1;reasons.push('EMA trend +');}else if(e9<e21&&e21<e50){score-=1;reasons.push('EMA trend -');}
  if(e200!=null){if(price>e200){score+=1;reasons.push('EMA200 üzərində');}else{score-=1;reasons.push('EMA200 altında');}}
  if(rv<35){score+=1;reasons.push('RSI oversold');}else if(rv>65){score-=1;reasons.push('RSI overbought');}
  if(mv>0){score+=1;reasons.push('MACD +');}else if(mv<0){score-=1;reasons.push('MACD -');}
  if(bb){if(price<=bb.lower){score+=1;reasons.push('BB alt zolaq');}else if(price>=bb.upper){score-=1;reasons.push('BB üst zolaq');}}
  if(st<20){score+=1;reasons.push('Stoch oversold');}else if(st>80){score-=1;reasons.push('Stoch overbought');}
  if(cv<-100){score+=1;reasons.push('CCI oversold');}else if(cv>100){score-=1;reasons.push('CCI overbought');}
  if(str>0){score+=1;reasons.push('higher highs/lows');}else if(str<0){score-=1;reasons.push('lower highs/lows');}
  if(ich&&ich.score!==0){if(ich.score>0){score+=1;reasons.push('Ichimoku bulud üzərində');}else{score-=1;reasons.push('Ichimoku bulud altında');}}
  if(psar){if(psar.uptrend){score+=1;reasons.push('Parabolic SAR yüksəliş');}else{score-=1;reasons.push('Parabolic SAR düşüş');}}
  if(piv){if(price>piv.pp){score+=1;reasons.push('Pivot üzərində');}else if(price<piv.pp){score-=1;reasons.push('Pivot altında');}}
  if(fib){if(price>fib.r500){score+=1;reasons.push('Fib 50% üzərində');}else{score-=1;reasons.push('Fib 50% altında');}}
  if(wr!=null){if(wr<-80){score+=1;reasons.push('Williams %R oversold');}else if(wr>-20){score-=1;reasons.push('Williams %R overbought');}}
  if(kelt){if(price<=kelt.lower){score+=1;reasons.push('Keltner alt zolaq');}else if(price>=kelt.upper){score-=1;reasons.push('Keltner üst zolaq');}}
  if(ha!==0){if(ha>0){score+=1;reasons.push('Heikin-Ashi yüksəliş');}else{score-=1;reasons.push('Heikin-Ashi düşüş');}}
  if(stnd){if(stnd.uptrend){score+=1;reasons.push('SuperTrend yüksəliş');}else{score-=1;reasons.push('SuperTrend düşüş');}}
  let confidence=Math.round(Math.min(100,Math.abs(score)/16*100));
  if(ax!=null){
    if(ax>=25){confidence=Math.min(100,confidence+8);reasons.push(`ADX güclü trend (${ax.toFixed(0)})`);}
    else if(ax<15){confidence=Math.max(0,confidence-12);reasons.push(`ADX zəif/yan bazar (${ax.toFixed(0)})`);}
  }
  // ADX<15 = yan/trendsiz bazar → bu şəraitdə əksər trend indikatorları aldadıcı siqnal verir,
  // ona görə MIN_CONFIDENCE-dan asılı olmayaraq siqnalı tam bloklayırıq (yalan siqnalların əsas mənbəyi budur)
  let signal=(ax!=null&&ax<15)?'WAIT':(score>=5?'LONG':score<=-5?'SHORT':'WAIT');
  // Spike filtri: sintetik indekslərdə (xüsusən Boom/Crash/Jump) tək şamda ATR-dən 3+ dəfə
  // böyük hərəkət baş verə bilər — bu zaman indikatorlar etibarsızdır, siqnal bloklanır
  const lastBar=c.at(-1), barRange=lastBar.h-lastBar.l;
  const isSpike = av!=null && barRange > av*3;
  if (isSpike) { signal='WAIT'; reasons.push('Qeyri-adi sıçrayış (spike) aşkarlandı — bloklandı'); }
  return{signal,confidence,score,price,atr:av,rsi:rv,adx:ax,reasons};
}

// CLOSED_CANDLE_ONLY açıqdırsa, formalaşan (sonuncu) 5m şam atılır, 15m/1h ondan yenidən yığılır
function analysisFrames(symbol) {
  const raw5 = state.candles.get(key(symbol, CONFIRM_TF)) || [];
  if (!CLOSED_CANDLE_ONLY) return { c5: raw5, c15: getCandles(symbol, '15m'), c1h: getCandles(symbol, TREND_TF), barT: null };
  const closed = raw5.slice(0, -1);
  return {
    c5: closed,
    c15: aggregate(closed, GRANULARITY['15m']),
    c1h: aggregate(closed, GRANULARITY[TREND_TF]),
    barT: closed.length ? closed[closed.length - 1].t : null,
  };
}

function fullAnalysis(symbol){
  const fr = analysisFrames(symbol);
  const a = { [CONFIRM_TF]: analyze(fr.c5), '15m': analyze(fr.c15), [TREND_TF]: analyze(fr.c1h) };
  const m5 = a[CONFIRM_TF], m15 = a['15m'], h1 = a[TREND_TF];
  if (!m15) return null;

  let final = 'WAIT', expiry = null, strength = 'zəif';
  let confluence = 0;
  if (m15.signal !== 'WAIT') confluence++;
  if (h1 && h1.signal === m15.signal) confluence++;
  if (m5 && m5.signal === m15.signal) confluence++;

  if (h1 && m15.signal !== 'WAIT' && m15.signal === h1.signal && (!REQUIRE_FULL_CONFLUENCE || (m5 && m5.signal === m15.signal))) {
    final = m15.signal; expiry = '15m';
    strength = (m5 && m5.signal === final) ? 'çox güclü (5m+15m+1h uyğun)' : 'güclü (15m+1h trend uyğun)';
  }

  let confidence = m15.confidence;
  if (final !== 'WAIT') {
    const parts = [m15.confidence, h1?.confidence, (m5 && m5.signal === final) ? m5.confidence : null].filter(x => x != null);
    confidence = Math.round(parts.reduce((s, x) => s + x, 0) / parts.length);
    if (h1 && h1.signal === final) confidence = Math.min(100, confidence + 6);
    if (m5 && m5.signal === final) confidence = Math.min(100, confidence + 4);
  }

  const dir = final === 'LONG' ? 'CALL' : final === 'SHORT' ? 'PUT' : 'WAIT';
  const reasons = [...new Set([...(m5?.reasons||[]), ...(m15?.reasons||[]), ...(h1?.reasons||[])])].slice(0, 6);
  return { symbol, dir, confidence, expiry, strength, confluence, price: m15.price, atr: m15.atr, rsi: m15.rsi, timeframes: a, reasons, barT: fr.barT };
}

function getCandles(symbol, tf) { return state.candles.get(key(symbol, tf)) || []; }

let derivWs = null;
let reqSeq = 1;
let pingTimer = null;
let reconnectTimer = null;
let reconnectDelay = 3000;
const MAX_RECONNECT_DELAY = 30000;
let lastAnyCandleAt = Date.now();
// req_id -> { symbol, tf }  (yeni Deriv API cavabda echo_req qaytarmaya bilər)
const reqMeta = new Map();
// subscription.id -> { symbol, tf }
const subMeta = new Map();

// Köhnə API: s.symbol | Yeni API: s.underlying_symbol
const symOf = (s) => (s && (s.symbol || s.underlying_symbol)) || null;

function marketOf(s) {
  const m = s.market || '';
  if (m in MARKET_CAPS) return m;
  return /^cry/i.test(symOf(s) || '') ? 'cryptocurrency' : null;
}
function pickAutoSymbols(list) {
  const out = [];
  for (const mk of Object.keys(MARKET_CAPS)) {
    const items = list.filter((s) => marketOf(s) === mk)
      .sort((a, b) => (SUB_RANK[a.submarket] ?? 2) - (SUB_RANK[b.submarket] ?? 2));
    out.push(...items.slice(0, MARKET_CAPS[mk]).map(symOf).filter(Boolean));
  }
  return out;
}
// 5m şamlardan 15m / 1h şamlarını yığır
function aggregate(raw, gran) {
  const out = []; let cur = null;
  for (const k of raw) {
    const b = Math.floor(k.t / 1000 / gran) * gran * 1000;
    if (!cur || cur.t !== b) { if (cur) out.push(cur); cur = { t: b, o: k.o, h: k.h, l: k.l, c: k.c, v: 1 }; }
    else { if (k.h > cur.h) cur.h = k.h; if (k.l < cur.l) cur.l = k.l; cur.c = k.c; }
  }
  if (cur) out.push(cur);
  return out.slice(-500);
}
function rebuildAgg(symbol) {
  const raw = state.candles.get(key(symbol, CONFIRM_TF)) || [];
  for (const tf of [...TIMEFRAMES, TREND_TF]) state.candles.set(key(symbol, tf), aggregate(raw, GRANULARITY[tf]));
}

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
    reconnectDelay = 3000;
    console.log('[deriv] bağlantı quruldu, aktiv simvollar soruşulur...');
    ws.send(JSON.stringify({ active_symbols: 'brief', req_id: reqSeq++ }));
    // Deriv boş qalan bağlantını bağlayır — hər 30 san ping
    clearInterval(pingTimer);
    pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 }));
    }, 30000);
  });

  // Sorğular növbə ilə, SUB_INTERVAL_MS fasilə ilə göndərilir (limiti aşmamaq üçün)
  let subQueue = [];
  let subTimer = null;
  function sendHistory(symbol) {
    const id = reqSeq++;
    reqMeta.set(id, { symbol, tf: CONFIRM_TF });
    ws.send(JSON.stringify({
      ticks_history: symbol,
      style: 'candles',
      granularity: GRANULARITY[CONFIRM_TF],
      count: HISTORY_COUNT,
      end: 'latest',
      subscribe: 1,
      req_id: id,
    }));
  }
  function subscribeCandles(symbolList) {
    subQueue.push(...symbolList);
    if (subTimer) return;
    subTimer = setInterval(() => {
      const sym = subQueue.shift();
      if (!sym || ws.readyState !== WebSocket.OPEN) { clearInterval(subTimer); subTimer = null; return; }
      sendHistory(sym);
    }, SUB_INTERVAL_MS);
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
      if ((msg.error.code === 'RateLimit' || /rate\s*limit/i.test(msg.error.message || '')) && m) {
        // Limitə dəydik — həmin simvolu 15 san sonra yenidən növbəyə qoy
        setTimeout(() => { if (ws.readyState === WebSocket.OPEN) subscribeCandles([m.symbol]); }, 15000);
        return;
      }
      if (/invalid/i.test(msg.error.message || '') && m) {
        invalidCount++;
        if (!gotCandles && invalidCount >= state.activeSymbols.length) {
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
        const real = list.filter(s => s.market !== 'synthetic_index');
        console.log('[deriv] forex/əmtəə simvolları:', real.slice(0, 80).map(s => `${symOf(s)}="${nameOf(s)}"`).join(' | '));
      }

      // Hər tələb olunan simvol üçün: dəqiq ad → "Volatility N Index" adı ilə axtarış
      const resolved = [];
      for (const want of (AUTO_MARKETS ? [] : symbols)) {
        if (all.includes(want)) { resolved.push(want); continue; }
        const m = /^R_(\d+)$/.exec(want);
        if (m) {
          const re = new RegExp(`^volatility\\s*${m[1]}\\s*index$`, 'i');
          const hit = list.find(s => re.test(nameOf(s).trim()));
          if (hit) { console.log(`[deriv] ${want} → ${symOf(hit)} (ad ilə tapıldı)`); resolved.push(symOf(hit)); continue; }
        }
        if (NAME_HINTS[want]) {
          const hit = list.find(s => s.market !== 'synthetic_index' && NAME_HINTS[want].test(nameOf(s).trim()));
          if (hit) { console.log(`[deriv] ${want} → ${symOf(hit)} (ad ilə tapıldı)`); resolved.push(symOf(hit)); continue; }
        }
        console.warn(`[deriv] simvol tapılmadı: ${want}`);
      }
      let finalSymbols = [...new Set(resolved)];
      if (AUTO_MARKETS) finalSymbols = pickAutoSymbols(list);

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
      rebuildAgg(meta.symbol);
      return;
    }

    if (msg.msg_type === 'ohlc' && msg.ohlc) {
      const o = msg.ohlc;
      const meta = resolveMeta(msg, o.granularity, o.symbol || o.underlying_symbol);
      if (!meta) return;
      lastAnyCandleAt = Date.now();
      const { symbol, tf } = meta;
      const arr = state.candles.get(key(symbol, tf)) || [];
      const epochMs = Number(o.open_time) * 1000;
      const candle = { t: epochMs, o: +o.open, h: +o.high, l: +o.low, c: +o.close, v: 1 };
      const last = arr[arr.length - 1];
      if (last && last.t === epochMs) arr[arr.length - 1] = candle;
      else arr.push(candle);
      state.candles.set(key(symbol, tf), arr.slice(-HISTORY_COUNT));
      onCandleUpdate(symbol);
    }
  });

  ws.on('close', () => {
    state.derivConnected = false;
    clearInterval(pingTimer);
    clearInterval(subTimer); subTimer = null; subQueue = [];
    if (!opened && DERIV_WS_URLS.length > 1) urlIdx++;
    const delay = reconnectDelay;
    console.log(`[deriv] bağlantı kəsildi, ${Math.round(delay / 1000)} saniyə sonra yenidən qoşulacaq`);
    if (!reconnectTimer) reconnectTimer = setTimeout(connectDeriv, delay);
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
  });
  ws.on('error', (e) => console.error('[deriv] WS xətası:', e.message));
}
function tfFromGranularity(g) {
  for (const tf of Object.keys(GRANULARITY)) if (GRANULARITY[tf] === Number(g)) return tf;
  return null;
}

// === Trading bağlantısı (Telegram "AL/Bağla" düymələri üçün) ===
// QEYD: Deriv 2026-da API arxitekturasını dəyişib — köhnə "birbaşa WS-də authorize"
// üsulu artıq işləmir. Yeni axın: REST ilə hesabı tap → OTP al → OTP-li WS URL-ə qoşul.
const DERIV_REST_BASE = 'https://api.derivws.com';
let tradingWs = null;
let tradingAuthorized = false;
let tradingAuthError = null;
let tradingCurrency = null;
let tradingIsVirtual = null;
let tradingAccountId = null;
let tradingBalance = null;
let tradingReconnectDelay = 3000;
let tradingReqSeq = 1;
const tradingPending = new Map(); // req_id -> { resolve, reject, timer }

async function derivRest(method, path, body) {
  const res = await fetch(`${DERIV_REST_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${DERIV_API_TOKEN}`,
      'Deriv-App-ID': String(DERIV_APP_ID),
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const rawText = await res.text();
  let json; try { json = rawText ? JSON.parse(rawText) : null; } catch { json = null; }
  if (!res.ok) {
    const err = json?.errors?.[0];
    const detail = err ? `${err.code}: ${err.message || ''}`.trim() : (rawText ? rawText.slice(0, 200) : res.statusText || '');
    throw new Error(`HTTP ${res.status} (${path}) — ${detail}`);
  }
  return json;
}

// Options trading hesabını tapır (demo hesabı üstün tutur); yoxdursa avtomatik yaradır
async function ensureTradingAccount() {
  const list = await derivRest('GET', '/trading/v1/options/accounts');
  const accounts = list?.data || [];
  let acc = accounts.find((a) => a.account_type === 'demo' || a.is_virtual === true);
  if (!acc) {
    const created = await derivRest('POST', '/trading/v1/options/accounts', { currency: 'USD', group: 'row', account_type: 'demo' });
    acc = Array.isArray(created?.data) ? created.data[0] : created?.data;
  }
  if (!acc) throw new Error('Options trading hesabı tapılmadı/yaradılmadı');
  tradingAccountId = acc.account_id || acc.id || acc.loginid;
  tradingCurrency = acc.currency || 'USD';
  tradingIsVirtual = acc.account_type ? acc.account_type === 'demo' : !!acc.is_virtual;
}

async function connectTradingWs() {
  if (!DERIV_API_TOKEN) { console.log('[trading] DERIV_API_TOKEN boşdur — Telegram AL/Bağla düymələri deaktiv olacaq'); return; }
  try {
    if (!tradingAccountId) await ensureTradingAccount();
    const otpRes = await derivRest('POST', `/trading/v1/options/accounts/${tradingAccountId}/otp`);
    const otpUrl = otpRes?.data?.url;
    if (!otpUrl) throw new Error('OTP url alınmadı');

    const ws = new WebSocket(otpUrl);
    tradingWs = ws;
    let tradingPingTimer = null;
    ws.on('open', () => {
      tradingReconnectDelay = 3000;
      tradingAuthError = null;
      tradingAuthorized = true;
      refreshBalance().catch(() => {});
      resumeOpenTrades();
      console.log(`[trading] qoşuldu — hesab: ${tradingAccountId} (${tradingIsVirtual ? 'DEMO' : 'REAL'}), valyuta: ${tradingCurrency}`);
      clearInterval(tradingPingTimer);
      tradingPingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 }));
      }, 30000);
    });
    ws.on('message', (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      if (msg.req_id && tradingPending.has(msg.req_id)) {
        const p = tradingPending.get(msg.req_id);
        tradingPending.delete(msg.req_id);
        clearTimeout(p.timer);
        if (msg.error) p.reject(new Error(msg.error.message));
        else p.resolve(msg);
      }
    });
    ws.on('close', (code) => {
      tradingAuthorized = false;
      clearInterval(tradingPingTimer);
      // Cavabsız qalan sorğular timeout-a qədər asılı qalmasın
      for (const [, pend] of tradingPending) { clearTimeout(pend.timer); pend.reject(new Error('Trading bağlantısı kəsildi')); }
      tradingPending.clear();
      console.log(`[trading] bağlantı kəsildi (kod ${code}), ${Math.round(tradingReconnectDelay / 1000)}s sonra yenidən qoşulacaq`);
      setTimeout(connectTradingWs, tradingReconnectDelay);
      tradingReconnectDelay = Math.min(tradingReconnectDelay * 2, 30000);
    });
    ws.on('error', (e) => console.error('[trading] ws xətası:', e.message));
  } catch (e) {
    tradingAuthorized = false;
    tradingAuthError = e.message;
    console.error('[trading] qoşulma xətası:', e.message);
    setTimeout(connectTradingWs, tradingReconnectDelay);
    tradingReconnectDelay = Math.min(tradingReconnectDelay * 2, 30000);
  }
}

function tradingRequest(payload, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    if (!DERIV_API_TOKEN) return reject(new Error('DERIV_API_TOKEN qurulmayıb'));
    if (!tradingWs || tradingWs.readyState !== WebSocket.OPEN || !tradingAuthorized) {
      return reject(new Error(tradingAuthError ? `Deriv xətası: ${tradingAuthError}` : 'Trading bağlantısı hələ qurulur, bir neçə saniyə sonra yenidən cəhd edin'));
    }
    const req_id = tradingReqSeq++;
    const timer = setTimeout(() => { tradingPending.delete(req_id); reject(new Error('Deriv-dən cavab gəlmədi (timeout)')); }, timeoutMs);
    tradingPending.set(req_id, { resolve, reject, timer });
    tradingWs.send(JSON.stringify({ ...payload, req_id }));
  });
}

// Yeni API: birbaşa "buy" yoxdur, əvvəlcə "proposal" (qiymət təklifi), sonra onun id-si ilə "buy"
let tradeDay = null, tradesToday = 0;
async function buyContract(symbol, dir, durationMin) {
  rolloverDay();
  if (tradesToday >= MAX_DAILY_TRADES) throw new Error(`Gündəlik limit dolub (${MAX_DAILY_TRADES} əməliyyat)`);
  let proposalMsg;
  try {
    proposalMsg = await tradingRequest({
      proposal: 1,
      amount: DERIV_STAKE_AMOUNT,
      basis: 'stake',
      contract_type: dir,
      currency: tradingCurrency || 'USD',
      underlying_symbol: symbol,
      duration: durationMin,
      duration_unit: 'm',
    });
  } catch (e) {
    if (/not offered for this duration/i.test(e.message)) {
      throw new Error(`${symbol} üçün ${durationMin} dəq müddət hazırda təklif olunmur`);
    }
    throw e;
  }
  const p = proposalMsg.proposal;
  if (!p?.id) throw new Error('Proposal alınmadı');
  const buyMsg = await tradingRequest({ buy: p.id, price: p.ask_price });
  tradesToday++;
  return buyMsg.buy;
}

async function sellContract(contractId) {
  const msg = await tradingRequest({ sell: contractId, price: 0 });
  return msg.sell;
}

// === Avtomatik alış + stop-loss / take-profit (gündəlik sessiya üzrə) ===
const session = { pnl: 0, wins: 0, losses: 0, streak: 0, pausedUntil: 0, halted: false, reachedTP: false, startBalance: null, open: new Map() };
function rolloverDay() {
  const today = new Date().toISOString().slice(0, 10);
  if (tradeDay !== today) {
    tradeDay = today; tradesToday = 0;
    Object.assign(session, { pnl: 0, wins: 0, losses: 0, streak: 0, pausedUntil: 0, halted: false, reachedTP: false, startBalance: null });
    if (tradingAuthorized) refreshBalance().catch(() => {});
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const tgMsg = (text) => (TELEGRAM_TOKEN && TELEGRAM_CHAT_ID)
  ? telegram('sendMessage', { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }) : null;
const esc = (x) => String(x).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const money = (x) => `${x >= 0 ? '+' : ''}${x.toFixed(2)} ${tradingCurrency || ''}`.trim();

// Risk bazası: real gün-başı balans (varsa), amma START_BALANCE-dan böyük olmayaraq
function startBal() {
  if (USE_REAL_BALANCE && session.startBalance > 0) return Math.min(session.startBalance, START_BALANCE);
  return START_BALANCE;
}
async function refreshBalance() {
  const m = await tradingRequest({ balance: 1 });
  const b = Number(m && m.balance && m.balance.balance);
  if (!Number.isFinite(b)) return;
  tradingBalance = b;
  if (USE_REAL_BALANCE && !session.startBalance && b - session.pnl > 0) { session.startBalance = b - session.pnl; saveState(); }
}

// Müqavilə bitəndən sonra nəticəni (mənfəət/zərər) sorğulayır. Bağlantı qopubsa cəhd sayılmır, gözləyir.
async function trackContract(contractId, waitMs) {
  await sleep(waitMs);
  let tries = 0, netWaits = 0;
  while (tries < 12 && netWaits < 120) {
    try {
      const m = await tradingRequest({ proposal_open_contract: 1, contract_id: contractId });
      const c = m.proposal_open_contract;
      if (c && (c.is_sold || c.status === 'won' || c.status === 'lost')) {
        if (c.profit != null) return Number(c.profit);
        return c.status === 'won' ? Number(c.payout) - Number(c.buy_price) : -Number(c.buy_price);
      }
      tries++;
    } catch (e) {
      if (!tradingAuthorized) netWaits++; else tries++;
    }
    await sleep(5000);
  }
  return null;
}

// Risk qapısı: həm avtomatik, həm də düymə ilə alış buradan keçir
function riskGate(symbol) {
  rolloverDay();
  if (session.halted) return 'Gündəlik limit (stop-loss/take-profit) işləyib — bu gün əməliyyat dayandırılıb';
  if (Date.now() < session.pausedUntil) return 'Ardıcıl zərərə görə pauza aktivdir';
  if (session.open.size >= MAX_OPEN_TRADES) return `Açıq əməliyyat limiti dolub (${MAX_OPEN_TRADES})`;
  // Ən pis ssenari: bütün açıq əməliyyatlar + bu yeni əməliyyat itirsə, stop-loss aşılmasın
  if (session.pnl - (session.open.size + 1) * DERIV_STAKE_AMOUNT <= -startBal() * STOP_LOSS_PCT / 100) return 'Stop-loss ehtiyatı: yeni əməliyyat limiti aşa bilər';
  for (const o of session.open.values()) if (o.symbol === symbol) return `${symbol} üzrə artıq açıq əməliyyat var`;
  if (!/^cry/i.test(symbol) && isWeekendClosed()) return 'Bazar bağlıdır (həftəsonu)';
  return null;
}

// Yer SİNXRON rezerv olunur (await-dən əvvəl) — iki siqnal eyni anda gəlsə də ikiqat alış olmur
async function placeTrade(symbol, dir, dur, meta = {}) {
  const why = riskGate(symbol);
  if (why) { const e = new Error(why); e.gate = true; throw e; }
  const slot = `pending:${symbol}:${Date.now()}`;
  session.open.set(slot, { symbol, pending: true });
  let buy;
  try { buy = await buyContract(symbol, dir, dur); }
  finally { session.open.delete(slot); }
  const contractId = Number(buy.contract_id);
  session.open.set(contractId, { contractId, symbol, dir, dur, openedAt: Date.now(), source: meta.source || 'manual' });
  saveState();
  settleTrade(contractId, symbol, dur * 60000 + 5000).catch((e) => console.error('[settle] xəta:', e.message));
  return buy;
}

// Nəticəni gözləyir, P&L / seriya / limitləri yeniləyir (avtomatik və əl alışları üçün eyni)
async function settleTrade(contractId, symbol, waitMs) {
  let profit = await trackContract(contractId, waitMs);
  session.open.delete(contractId);
  let unknown = false;
  if (profit == null) { profit = -DERIV_STAKE_AMOUNT; unknown = true; tgMsg(`❔ #${contractId} nəticəsi alına bilmədi — ehtiyat üçün zərər sayıldı, Deriv-də yoxlayın.`); }
  session.pnl += profit;
  if (profit > 0) { session.wins++; session.streak = 0; } else { session.losses++; session.streak++; }
  if (session.streak >= CONSEC_LOSS_PAUSE) {
    session.pausedUntil = Date.now() + PAUSE_MIN * 60000; session.streak = 0;
    tgMsg(`⏸ ${CONSEC_LOSS_PAUSE} ardıcıl zərər — avto-alış ${PAUSE_MIN} dəq pauzadadır.`);
  }
  saveState();
  tgMsg(`${unknown ? '❔' : profit > 0 ? '✅ QAZANC' : '❌ ZƏRƏR'} ${esc(symbol)} ${money(profit)} · gün: <b>${money(session.pnl)}</b> (${session.wins}W/${session.losses}L)`);
  checkLimits(profit);
  refreshBalance().catch(() => {});
}

// Restartdan sonra açıq qalmış əməliyyatların izlənməsini bərpa edir
function resumeOpenTrades() {
  const q = resumeQueue; resumeQueue = [];
  let n = 0;
  for (const o of q) {
    if (!o || !o.contractId || session.open.has(o.contractId)) continue;
    session.open.set(o.contractId, o);
    const wait = Math.max(0, o.openedAt + o.dur * 60000 + 5000 - Date.now());
    settleTrade(o.contractId, o.symbol, wait).catch((e) => console.error('[settle] xəta:', e.message));
    n++;
  }
  if (n) tgMsg(`♻️ Restartdan sonra ${n} açıq əməliyyatın izlənməsi bərpa olundu.`);
}

const errNotified = new Map();
function notifyTradeError(symbol, msg) {
  const last = errNotified.get(symbol) || 0;
  if (Date.now() - last < ERR_NOTIFY_COOLDOWN_MIN * 60000) return;
  errNotified.set(symbol, Date.now());
  tgMsg(`⚠️ Avto-alış alınmadı (${esc(symbol)}): ${esc(msg)}`);
}

function checkLimits(lastProfit) {
  const base = startBal();
  const lossLimit = -base * STOP_LOSS_PCT / 100;
  const tpMin = base * TP_MIN_PCT / 100;
  const tpMax = base * TP_MAX_PCT / 100;
  let why = null;
  if (session.pnl <= lossLimit) why = `🛑 <b>STOP-LOSS</b> işlədi (${STOP_LOSS_PCT}% = ${money(lossLimit)}). Avto-alış bu gün dayandırıldı.`;
  else if (session.pnl >= tpMax) why = `🎯 <b>TAKE-PROFIT</b> (${TP_MAX_PCT}%) çatıldı. Mənfəət qorunur, avto-alış bu gün dayandırıldı.`;
  else {
    if (session.pnl >= tpMin) session.reachedTP = true;
    if (session.reachedTP && lastProfit <= 0) why = `🎯 <b>TAKE-PROFIT</b>: ${TP_MIN_PCT}%-dən yuxarı mənfəətdən sonra ilk zərər gəldi, avto-alış bu gün dayandırıldı.`;
  }
  if (why) { session.halted = true; saveState(); tgMsg(`${why}
Gündəlik nəticə: <b>${money(session.pnl)}</b> (${session.wins}W/${session.losses}L)`); }
}

async function autoTrade(r) {
  if (!AUTO_TRADE || !DERIV_API_TOKEN) return;
  const dur = expiryMinutes(r.expiry);
  let buy;
  try {
    buy = await placeTrade(r.symbol, r.dir, dur, { source: 'auto' });
  } catch (e) {
    // Müddət təklif olunmursa, cooldown-u işə sal ki, eyni simvol üçün təkrar-təkrar cəhd olmasın
    if (/müddət hazırda təklif olunmur/.test(e.message)) state.lastSignalAt.set(r.symbol, { dir: r.dir, ts: Date.now(), confidence: r.confidence });
    if (!e.gate) notifyTradeError(r.symbol, e.message); // limit/pauza/bağlı bazar kimi gözlənilən hallar səssiz keçilir
    return;
  }
  const acc = tradingIsVirtual ? 'DEMO' : 'REAL';
  tgMsg(`🤖 <b>AVTO-ALIŞ (${acc})</b> ${esc(r.symbol)} ${r.dir} · ${dur} dəq · stake ${DERIV_STAKE_AMOUNT} · etibar ${r.confidence}% · #${buy.contract_id}`);
}

setInterval(() => {
  rolloverDay();
  tgMsg(`📊 Status: gün <b>${money(session.pnl)}</b> (${session.wins}W/${session.losses}L) · balans: ${tradingBalance == null ? '--' : tradingBalance.toFixed(2)} · açıq: ${session.open.size} · ${session.halted ? 'DAYANDIRILIB' : 'aktiv'} · avto-alış: ${AUTO_TRADE && DERIV_API_TOKEN ? 'ON' : 'OFF'} · siqnal win-rate: ${statsLine()}`);
}, STATUS_EVERY_MIN * 60000);

// Siqnalın real nəticəsi: expiry bitəndə qiymət giriş qiymətindən düzgün tərəfdədirmi
function bucketOf(conf) { const b = Math.min(90, Math.floor((conf || 0) / 10) * 10); return `${b}-${b + 9}`; }
function statsLine() { return stats.total ? `${Math.round(stats.wins / stats.total * 100)}% (${stats.wins}/${stats.total})` : '--'; }
function scheduleEvaluation(entry) {
  const dur = expiryMinutes(entry.expiry);
  setTimeout(() => {
    const last = (state.candles.get(key(entry.symbol, CONFIRM_TF)) || []).at(-1);
    if (!last || last.c === entry.price) return;
    const win = entry.dir === 'CALL' ? last.c > entry.price : last.c < entry.price;
    entry.outcome = win ? 'win' : 'loss';
    stats.total++; if (win) stats.wins++;
    const k = bucketOf(entry.confidence);
    const bk = stats.buckets[k] || (stats.buckets[k] = { n: 0, w: 0 });
    bk.n++; if (win) bk.w++;
    saveState();
  }, dur * 60000 + 10000);
}

function expiryMinutes(expiry) {
  const m = /^(\d+)/.exec(expiry || '');
  return m ? Number(m[1]) : 15;
}

// Watchdog: WS "açıq" görünsə də Deriv bəzən data axınını səssizcə kəsir.
// 90 saniyə heç bir yeni şam gəlməzsə, bağlantını məcburi bağlayıb yenidən qururuq.
setInterval(() => {
  if (state.derivConnected && !isWeekendClosed() && Date.now() - lastAnyCandleAt > 300000) {
    console.warn('[deriv] 5 dəqiqədir yeni şam gəlmir, bağlantı yenidən qurulur');
    try { derivWs && derivWs.close(); } catch {}
  }
}, 15000);

let analysisTimers = new Map();
function onCandleUpdate(symbol) {
  if (analysisTimers.has(symbol)) return;
  analysisTimers.set(symbol, setTimeout(() => {
    analysisTimers.delete(symbol);
    runAnalysis(symbol);
  }, ANALYSIS_DEBOUNCE_MS));
}

function runAnalysis(symbol) {
  let r;
  try { rebuildAgg(symbol); r = fullAnalysis(symbol); }
  catch (e) { console.error(`[analiz] ${symbol} xətası:`, e.message); return; }
  if (!r) return;
  state.lastAnalysis.set(symbol, r);
  broadcast({ type: 'analysis', data: r });

  if (!state.scannerEnabled) return;
  if (r.dir === 'WAIT' || r.confidence < MIN_CONFIDENCE) { state.pendingDir.delete(symbol); return; }
  // Bağlanmış şam rejimində hər şam üçün ən çox 1 siqnal
  if (CLOSED_CANDLE_ONLY && r.barT != null && state.lastSignalBar.get(symbol) === r.barT) return;

  // Ən azı 2 ardıcıl analiz eyni istiqaməti təsdiqləməlidir — tək tiklik "yanlış sıçrayış"
  // (qiymətin bir anlıq irəli-geri hərəkəti) siqnal doğurmasın deyə
  // (Bağlanmış şam rejimində lazım deyil: nəticə artıq təsdiqlənmiş şama əsaslanır)
  if (!CLOSED_CANDLE_ONLY) {
    const pend = state.pendingDir.get(symbol);
    if (!pend || pend.dir !== r.dir) {
      state.pendingDir.set(symbol, { dir: r.dir, count: 1 });
      return;
    }
    pend.count++;
    if (pend.count < 2) return;
  }

  const prev = state.lastSignalAt.get(symbol);
  const cooldownMs = SIGNAL_COOLDOWN_MIN * 60 * 1000;
  const now = Date.now();
  const dirChanged = !prev || prev.dir !== r.dir;
  const bigConfidenceShift = prev && Math.abs((prev.confidence ?? 0) - r.confidence) >= 15;
  const cooldownPassed = prev && now - prev.ts >= cooldownMs;
  if (!dirChanged && !bigConfidenceShift && !cooldownPassed) return;

  state.lastSignalAt.set(symbol, { dir: r.dir, ts: now, confidence: r.confidence });
  if (r.barT != null) state.lastSignalBar.set(symbol, r.barT);
  const entry = { ts: now, symbol, dir: r.dir, confidence: r.confidence, expiry: r.expiry, strength: r.strength, confluence: r.confluence, price: r.price, reasons: r.reasons };
  state.signals.unshift(entry);
  state.signals = state.signals.slice(0, 200);
  saveState();
  broadcast({ type: 'signal', data: entry });
  scheduleEvaluation(entry);
  sendTelegramSignal(r);
  autoTrade(r).catch((e) => console.error('[auto] xəta:', e.message));
}

async function telegram(method, body, attempt = 1) {
  if (!TELEGRAM_TOKEN) { console.error('[telegram] TELEGRAM_BOT_TOKEN boşdur'); return null; }
  try {
    const r = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/${method}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    });
    const j = await r.json();
    if (j && j.ok === false) {
      console.error(`[telegram] rədd edildi: ${j.description}`);
      // Rate limit (429) — Telegram-ın göstərdiyi müddət qədər gözləyib bir dəfə yenidən cəhd et
      if (j.error_code === 429 && attempt < 3) {
        const wait = ((j.parameters && j.parameters.retry_after) || 2) * 1000;
        await new Promise(res => setTimeout(res, wait));
        return telegram(method, body, attempt + 1);
      }
    }
    return j;
  } catch (e) {
    console.error('[telegram] şəbəkə xətası:', e.message);
    if (attempt < 3) {
      await new Promise(res => setTimeout(res, 1500 * attempt));
      return telegram(method, body, attempt + 1);
    }
    return null;
  }
}
function confBar(pct) { const filled = Math.round((pct || 0) / 10); return '█'.repeat(filled) + '░'.repeat(10 - filled); }
function fmt(x) { return x == null ? '--' : Number(x).toLocaleString('en-US', { maximumFractionDigits: 5 }); }
async function sendTelegramSignal(r) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) { console.error('[telegram] TOKEN/CHAT_ID boşdur, siqnal göndərilmədi'); return; }
  const emoji = r.dir === 'CALL' ? '📈' : '📉';
  const dirText = r.dir === 'CALL' ? 'CALL (yuxarı)' : 'PUT (aşağı)';
  const expText = r.expiry === '15m' ? '15 dəqiqə' : '5 dəqiqə';
  const confluenceText = r.confluence != null ? ` — 🟢${r.confluence}/3` : '';
  const autoTradeNote = DERIV_API_TOKEN
    ? '<i>Aşağıdakı düymə ilə bir kliklə Deriv-də əməliyyat açıla bilər.</i>'
    : '<i>Bu avtomatik texniki siqnaldır, maliyyə məsləhəti deyil. Auto-trade hələ aktiv deyil.</i>';
  const lines = [
    `${emoji} <b>${r.symbol}</b> — <b>${dirText}</b>${confluenceText}`,
    `Tövsiyə olunan expiry: <b>${expText}</b> (${r.strength})`,
    `Etibar: <b>${r.confidence}%</b> ${confBar(r.confidence)}`,
    `Qiymət: <code>${fmt(r.price)}</code>`,
    `Səbəblər: ${r.reasons.join(', ')}`,
    autoTradeNote,
  ];
  const btnText = r.dir === 'CALL' ? '🟢 AL (CALL)' : '🔴 SAT (PUT)';
  const reply_markup = DERIV_API_TOKEN
    ? { inline_keyboard: [[{ text: btnText, callback_data: `B|${r.symbol}|${r.dir}|${expiryMinutes(r.expiry)}` }]] }
    : undefined;
  await telegram('sendMessage', { chat_id: TELEGRAM_CHAT_ID, text: lines.join('\n'), parse_mode: 'HTML', reply_markup });
}

function broadcast(msg) {
  const s = JSON.stringify(msg);
  for (const c of state.clients) { if (c.readyState === 1) c.send(s); }
}

// === Telegram düymə (callback_query) emalı ===
async function handleCallbackQuery(cq) {
  const data = cq.data || '';
  if (!cq.message) { await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: 'Mesaj tapılmadı' }); return; }
  const chatId = cq.message.chat.id;
  const messageId = cq.message.message_id;
  const baseText = esc(cq.message.text || '');
  // İcazə yoxlanışı: yalnız bizim çat (və istəyə görə icazəli istifadəçilər)
  const userOk = !TELEGRAM_ALLOWED_USER_IDS.length || TELEGRAM_ALLOWED_USER_IDS.includes(String(cq.from && cq.from.id));
  if (!TELEGRAM_CHAT_ID || String(chatId) !== String(TELEGRAM_CHAT_ID) || !userOk) {
    console.warn(`[telegram] icazəsiz düymə cəhdi: chat=${chatId} user=${cq.from && cq.from.id}`);
    await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: '⛔ İcazə yoxdur', show_alert: true });
    return;
  }
  try {
    if (data.startsWith('B|')) {
      const [, symbol, dir, durStr] = data.split('|');
      const durMin = Number(durStr) || 15;
      if (!['CALL', 'PUT'].includes(dir) || !state.activeSymbols.includes(symbol)) throw new Error('Yanlış parametr');
      const buy = await placeTrade(symbol, dir, durMin, { source: 'manual' }); // risk qapısından keçir, P&L-ə yazılır
      const acc = tradingIsVirtual ? 'DEMO' : 'REAL';
      const closeBtn = { inline_keyboard: [[{ text: '🔴 Bağla (indi sat)', callback_data: `S|${buy.contract_id}` }]] };
      await telegram('editMessageText', {
        chat_id: chatId, message_id: messageId, parse_mode: 'HTML',
        text: `${baseText}\n\n✅ <b>ALINDI (${acc})</b> — stake: ${DERIV_STAKE_AMOUNT} ${tradingCurrency || ''} · #${buy.contract_id}`,
        reply_markup: closeBtn,
      });
      await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: '✅ Alındı' });
    } else if (data.startsWith('S|')) {
      const contractId = data.split('|')[1];
      const sell = await sellContract(contractId);
      await telegram('editMessageText', {
        chat_id: chatId, message_id: messageId, parse_mode: 'HTML',
        text: `${baseText}\n\n🔒 <b>BAĞLANDI</b> — satış: ${sell.sold_for} ${tradingCurrency || ''}`,
      });
      await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: '🔒 Bağlandı' });
    }
  } catch (e) {
    console.error('[telegram] callback xətası:', e.message);
    await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: `❌ Xəta: ${e.message}`, show_alert: true });
  }
}

async function pollTelegramUpdates() {
  if (!TELEGRAM_TOKEN) return;
  let delay = 500;
  try {
    const res = await telegram('getUpdates', { offset: tgUpdateOffset, timeout: 25, allowed_updates: ['callback_query'] });
    if (res && res.ok === false && res.error_code === 409) delay = 8000; // başqa instance eyni botu dinləyir
    if (res && res.ok && Array.isArray(res.result) && res.result.length) {
      for (const upd of res.result) {
        tgUpdateOffset = upd.update_id + 1;
        if (upd.callback_query) await handleCallbackQuery(upd.callback_query);
      }
      saveState();
    }
  } catch (e) {
    console.error('[telegram] polling xətası:', e.message);
  } finally {
    setTimeout(pollTelegramUpdates, delay);
  }
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
  const sym = req.params.sym;
  if (!state.activeSymbols.includes(sym)) return res.status(400).json({ error: 'yanlış simvol' });
  const r = state.lastAnalysis.get(sym) || fullAnalysis(sym);
  if (!r) return res.status(404).json({ error: 'kifayət qədər data yoxdur' });
  res.json(r);
});
app.get('/api/candles/:sym/:tf', (req, res) => {
  const { sym, tf } = req.params;
  if (!state.activeSymbols.includes(sym)) return res.status(400).json({ error: 'yanlış simvol' });
  if (!GRANULARITY[tf]) return res.status(400).json({ error: 'yanlış timeframe' });
  res.json(getCandles(sym, tf));
});
app.get('/health', (req, res) => res.json({ ok: true, deriv: state.derivConnected, trading: tradingAuthorized, uptime: Math.round((Date.now() - state.startedAt) / 1000) }));
app.get('/api/stats', (req, res) => {
  const buckets = Object.fromEntries(Object.entries(stats.buckets).map(([k, v]) => [k, { n: v.n, wins: v.w, winRate: v.n ? Math.round(v.w / v.n * 100) : null }]));
  res.json({ total: stats.total, wins: stats.wins, winRate: stats.total ? Math.round(stats.wins / stats.total * 100) : null, buckets });
});
app.get('/api/session', requireAdmin, (req, res) => {
  rolloverDay();
  const { open, ...rest } = session;
  res.json({ day: tradeDay, tradesToday, ...rest, open: [...open.values()], balance: tradingBalance, riskBase: startBal(), autoTrade: AUTO_TRADE,
    account: { id: tradingAccountId, currency: tradingCurrency, virtual: tradingIsVirtual, connected: tradingAuthorized } });
});
// Dayandırılmış / pauzadakı avto-alışı davam etdirir. {"resetPnl":true} gündəlik P&L-i də sıfırlayır (diqqətlə!)
app.post('/api/resume', requireAdmin, (req, res) => {
  rolloverDay();
  Object.assign(session, { halted: false, reachedTP: false, pausedUntil: 0, streak: 0 });
  if (req.body && req.body.resetPnl) Object.assign(session, { pnl: 0, wins: 0, losses: 0 });
  saveState();
  tgMsg(`▶️ Admin avto-alışı davam etdirdi${req.body && req.body.resetPnl ? ' (gündəlik P&L sıfırlandı)' : ''}.`);
  res.json({ ok: true, pnl: session.pnl, halted: session.halted });
});
app.post('/api/autotrade', requireAdmin, (req, res) => {
  AUTO_TRADE = !!(req.body && req.body.enabled);
  tgMsg(`🤖 Avto-alış admin tərəfindən ${AUTO_TRADE ? 'AÇILDI' : 'SÖNDÜRÜLDÜ'}.`);
  res.json({ ok: true, autoTrade: AUTO_TRADE });
});
app.post('/api/scanner', requireAdmin, (req, res) => {
  state.scannerEnabled = !!req.body.enabled;
  broadcast({ type: 'state', data: { scannerEnabled: state.scannerEnabled } });
  res.json({ ok: true, scannerEnabled: state.scannerEnabled });
});

app.use((err, req, res, next) => {
  console.error('[http] marşrut xətası:', err && err.stack || err);
  if (res.headersSent) return next(err);
  res.status(500).json({ error: 'daxili xəta' });
});

const server = app.listen(PORT, () => console.log(`[http] http://localhost:${PORT} dinlənilir`));
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
  state.clients.add(ws);
  ws.send(JSON.stringify({ type: 'state', data: { scannerEnabled: state.scannerEnabled, derivConnected: state.derivConnected } }));
  ws.on('close', () => state.clients.delete(ws));
});

function shutdown(sig) {
  console.log(`[sys] ${sig} alındı, vəziyyət saxlanılır`);
  try { saveStateSync(); } catch (e) { console.error('[state] son yazma xətası:', e.message); }
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Gözlənilməz xətalar serveri çökdürməsin — sadəcə logla və davam et
process.on('uncaughtException', (e) => console.error('[fatal] tutulmamış xəta:', e && e.stack || e));
process.on('unhandledRejection', (e) => console.error('[fatal] tutulmamış promise xətası:', e && e.stack || e));

async function verifyTelegramOnBoot() {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error('[telegram] TELEGRAM_BOT_TOKEN və ya TELEGRAM_CHAT_ID boşdur — Railway Variables-da doldur');
    return;
  }
  const me = await telegram('getMe', {});
  if (!me || me.ok === false) {
    console.error('[telegram] Bot token yanlışdır, getMe uğursuz oldu');
    return;
  }
  console.log(`[telegram] Bot təsdiqləndi: @${me.result.username}`);
  await telegram('sendMessage', {
    chat_id: TELEGRAM_CHAT_ID,
    text: '✅ Deriv siqnal botu işə düşdü və bağlıdır.',
  });
}

async function reportTradingStatusOnBoot() {
  if (!DERIV_API_TOKEN) {
    await telegram('sendMessage', { chat_id: TELEGRAM_CHAT_ID, text: 'ℹ️ DERIV_API_TOKEN qurulmayıb — AL/Bağla düymələri deaktiv olacaq.' });
    return;
  }
  console.log(`[trading] DERIV_API_TOKEN uzunluğu: ${DERIV_API_TOKEN.length} simvol`);
  // authorize cavabı gəlməsi üçün, nəticə bəlli olana qədər (max 20san) yoxlayırıq
  for (let i = 0; i < 10; i++) {
    if (tradingAuthorized || tradingAuthError) break;
    await new Promise((r) => setTimeout(r, 2000));
  }
  if (tradingAuthorized) {
    const acc = tradingIsVirtual ? 'DEMO' : 'REAL ⚠️';
    await telegram('sendMessage', {
      chat_id: TELEGRAM_CHAT_ID,
      text: `✅ Trading bağlantısı hazırdır — hesab: ${acc} (${tradingAccountId}), valyuta: ${tradingCurrency}, stake: ${DERIV_STAKE_AMOUNT}. AL/Bağla düymələri aktivdir.`,
    });
  } else {
    await telegram('sendMessage', {
      chat_id: TELEGRAM_CHAT_ID,
      text: `❌ Trading qoşulması uğursuz oldu: ${tradingAuthError || '20 saniyədə heç bir cavab gəlmədi'}\n\nDERIV_APP_ID: ${DERIV_APP_ID} (token uzunluğu: ${DERIV_API_TOKEN.length}).`,
    });
  }
}

loadState();
connectDeriv();
connectTradingWs();
pollTelegramUpdates();
verifyTelegramOnBoot();
reportTradingStatusOnBoot();
