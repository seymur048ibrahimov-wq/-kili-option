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
const DERIV_STAKE_AMOUNT = Number(process.env.DERIV_STAKE_AMOUNT || 1);
// Əvvəl yeni public endpoint, alınmasa köhnə endpoint (avtomatik növbələnir)
const DERIV_WS_URLS = process.env.DERIV_WS_URL
  ? [process.env.DERIV_WS_URL]
  : [
      'wss://api.derivws.com/trading/v1/options/ws/public',
      `wss://ws.derivws.com/websockets/v3?app_id=${DERIV_APP_ID}`,
    ];
let urlIdx = 0;

// === Siqnal parametrləri ===
// QEYD: confidence formulu indi 16 fərdi indikator əvəzinə 4 indikator "ailəsinin" (trend/
// momentum/volatilite/S-R) çoxluq səsinə əsaslanır (aşağıda analyze() funksiyasına bax), ona görə
// miqyas dəyişib: LONG/SHORT yaranması üçün artıq minimum 3/4 ailə eyni istiqamətdə olmalıdır
// (baza confidence >=75%), sonra ADX-ə görə davamlı şəkildə aşağı çəkilir. MIN_CONFIDENCE bunun
// üzərinə əlavə süzgəcdir.
const MIN_CONFIDENCE = Number(process.env.MIN_CONFIDENCE || 50);
const SIGNAL_COOLDOWN_MIN = Number(process.env.SIGNAL_COOLDOWN_MIN || 10);
const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const ADMIN_API_KEY = process.env.ADMIN_API_KEY || '';
const STATE_FILE = process.env.STATE_FILE || '/tmp/deriv13-state.json';

// === Kalibrasiya / EV süzgəci ===
// Deriv-in Rise/Fall opsionlarında adətən uduşda stake-in ~85-95%-i qazanılır, uduzanda isə
// 100% itirilir. Yəni "doğru istiqamət tapmaq" kifayət deyil — faktiki qazanc üçün win-rate
// bu asimmetrik ödənişi üstələməlidir (breakeven ehtimalı). Botu isə öz keçmiş siqnallarının
// faktiki nəticəsinə (aşağı bax: outcome-tracking) görə özü-özünü kalibrləyir və bu həddən aşağı
// bucket-lərdə siqnal göndərməyi avtomatik dayandırır.
const PAYOUT_PCT = Number(process.env.PAYOUT_PCT || 90); // orta Deriv Rise/Fall ödənişi (stake üstünə mənfəət faizi)
const BREAKEVEN_PROB = 100 / (1 + PAYOUT_PCT / 100); // faiz (0-100) — bu win-rate-dən aşağı olsa, uzunmüddətli EV mənfidir
const CONF_BUCKET_SIZE = 10; // confidence 10-luq bucket-lərə bölünür: 50-59, 60-69, ...
const MIN_SAMPLES_FOR_CALIBRATION = Number(process.env.MIN_SAMPLES_FOR_CALIBRATION || 20); // bucket-də bu qədər nəticə toplanana qədər kalibrasiya tətbiq olunmur, yalnız MIN_CONFIDENCE işləyir
function confBucket(c) { return Math.floor(c / CONF_BUCKET_SIZE) * CONF_BUCKET_SIZE; }

const DEFAULT_SYMBOLS = ['R_10','R_25','R_50','R_75','R_100'];
const rawSyms = (process.env.DERIV_SYMBOLS || '').trim();
const symbols = rawSyms ? rawSyms.split(',').map(s => s.trim()).filter(Boolean) : DEFAULT_SYMBOLS.slice();

const TIMEFRAMES = ['15m'];
const TREND_TF = '1h';
const CONFIRM_TF = '5m';
const MACRO_TFS = ['4h', '1d']; // yalnız kontekst (makro trend + aralıq) üçün — LONG/SHORT qərarına təsir etmir
const GRANULARITY = { '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };
const ALL_TFS = [CONFIRM_TF, ...TIMEFRAMES, TREND_TF, ...MACRO_TFS];

function key(symbol, tf) { return `${symbol}|${tf}`; }

const state = {
  startedAt: Date.now(),
  derivConnected: false,
  scannerEnabled: true,
  candles: new Map(),
  lastAnalysis: new Map(),
  signals: [],
  lastSignalAt: new Map(),
  pendingDir: new Map(),
  clients: new Set(),
  activeSymbols: symbols.slice(),
  availableSymbols: [],
  // === Outcome-tracking (özünü-kalibrləmə) ===
  outcomeStats: new Map(), // confBucket -> { wins, losses }
  pendingOutcomes: [],     // göndərilmiş siqnalların hələ nəticəsi bilinməyənləri: { symbol, dir, tf, entryPrice, bucket, checkAt }
  // === Risk / kill-switch ===
  dayStartEquity: null, dayStartAt: 0, tradingHalted: false, haltReason: null,
};

function saveState() {
  const dump = {
    signals: state.signals.slice(-200),
    lastSignalAt: [...state.lastSignalAt.entries()],
    outcomeStats: [...state.outcomeStats.entries()],
    pendingOutcomes: state.pendingOutcomes,
    dayStartEquity: state.dayStartEquity,
    dayStartAt: state.dayStartAt,
    tradingHalted: state.tradingHalted,
    haltReason: state.haltReason,
  };
  if (pgPool) {
    pgPool.query(
      `INSERT INTO deriv13_state(id,data,updated_at) VALUES(1,$1,now()) ON CONFLICT(id) DO UPDATE SET data=$1, updated_at=now()`,
      [dump]
    ).catch((e) => console.error('[state] Postgres yazma xətası:', e.message));
    return;
  }
  fs.writeFile(STATE_FILE, JSON.stringify(dump), (e) => { if (e) console.error('[state] yazma xətası:', e.message); });
}
async function loadState() {
  let d = null;
  if (pgPool) {
    try { const res = await pgPool.query('SELECT data FROM deriv13_state WHERE id=1'); d = res.rows?.[0]?.data || null; }
    catch (e) { console.error('[state] Postgres oxuma xətası:', e.message); }
  }
  if (!d) {
    try { if (fs.existsSync(STATE_FILE)) d = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (e) {}
  }
  if (!d) return;
  if (Array.isArray(d.signals)) state.signals = d.signals;
  if (Array.isArray(d.lastSignalAt)) state.lastSignalAt = new Map(d.lastSignalAt);
  if (Array.isArray(d.outcomeStats)) state.outcomeStats = new Map(d.outcomeStats);
  if (Array.isArray(d.pendingOutcomes)) state.pendingOutcomes = d.pendingOutcomes;
  if (d.dayStartEquity != null) state.dayStartEquity = d.dayStartEquity;
  if (d.dayStartAt) state.dayStartAt = d.dayStartAt;
  if (d.tradingHalted) { state.tradingHalted = true; state.haltReason = d.haltReason; }
}
// === İxtiyari Postgres (DATABASE_URL) — Railway-də disk müvəqqətidir (/tmp hər redeploy-da silinir),
// ona görə kalibrasiya statistikası və kill-switch vəziyyəti Postgres olmadan hər dəyişiklikdən sonra itir.
// DATABASE_URL təyin olunmayıbsa, avtomatik olaraq fayl-based saxlamaya (STATE_FILE) keçilir (işləyər,
// amma redeploy-da sıfırlanar). Railway-də: New → Database → Postgres, sonra dəyişəni servisə bağla.
let pgPool = null;
async function initDb() {
  if (!process.env.DATABASE_URL) { console.log('[state] DATABASE_URL yoxdur — fayl-based saxlama (/tmp) istifadə olunur, redeploy-da sıfırlanacaq'); return; }
  try {
    const { Pool } = await import('pg');
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pgPool.query(`CREATE TABLE IF NOT EXISTS deriv13_state (id INT PRIMARY KEY DEFAULT 1, data JSONB, updated_at TIMESTAMPTZ DEFAULT now())`);
    console.log('[state] Postgres bağlantısı quruldu — vəziyyət daimi saxlanılacaq');
  } catch (e) {
    console.warn('[state] Postgres qoşula bilmədi ("pg" paketi quraşdırılıbmı?), fayl-based saxlamaya keçilir:', e.message);
    pgPool = null;
  }
}

// Bir bucket-ə WIN/LOSS nəticəsi əlavə edir
function recordOutcome(bucket, win) {
  const s = state.outcomeStats.get(bucket) || { wins: 0, losses: 0 };
  if (win) s.wins++; else s.losses++;
  state.outcomeStats.set(bucket, s);
}
// Bu confidence bucket-i üçün kifayət qədər tarixi nəticə varsa, faktiki win-rate qaytarır (yoxdursa null)
function calibratedWinProb(confidence) {
  const s = state.outcomeStats.get(confBucket(confidence));
  if (!s) return null;
  const total = s.wins + s.losses;
  if (total < MIN_SAMPLES_FOR_CALIBRATION) return null;
  return (s.wins / total) * 100;
}
// Siqnal göndəriləndə "virtual" nəticə izləməyə qoşulur: expiry bitəndə qiymət hansı tərəfə
// getdiyinə görə WIN/LOSS qeydə alınır — real pul qoymasa belə statistika toplanır
function scheduleOutcomeCheck(entry) {
  const minutes = expiryMinutes(entry.expiry);
  state.pendingOutcomes.push({
    symbol: entry.symbol, dir: entry.dir, tf: entry.expiry,
    entryPrice: entry.price, bucket: confBucket(entry.confidence),
    checkAt: Date.now() + minutes * 60 * 1000,
  });
  saveState();
}
function checkPendingOutcomes() {
  const now = Date.now();
  const still = [];
  for (const p of state.pendingOutcomes) {
    if (now < p.checkAt) { still.push(p); continue; }
    const candles = getCandles(p.symbol, p.tf);
    const last = candles.at(-1);
    // Bu simvol üçün hələ yeni şam gəlməyibsə, 5 dəqiqə əlavə gözlə (data gecikməsi)
    if (!last || last.t < p.checkAt) {
      if (now - p.checkAt < 5 * 60 * 1000) { still.push(p); continue; }
      continue; // 5 dəqiqədən çoxdursa, data heç gəlməyib — bu nümunəni atırıq
    }
    const win = p.dir === 'CALL' ? last.c > p.entryPrice : last.c < p.entryPrice;
    recordOutcome(p.bucket, win);
  }
  state.pendingOutcomes = still;
  saveState();
}
setInterval(checkPendingOutcomes, 30000);

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
// === Əlavə (köməkçi) indikatorlar — yalnız OHLC əsasında, real "volume" tələb etmir ===
function aroon(c,p=14){ if(c.length<p+1)return null; const s=c.slice(-(p+1)); let hiIdx=0,loIdx=0; for(let i=1;i<s.length;i++){ if(s[i].h>=s[hiIdx].h) hiIdx=i; if(s[i].l<=s[loIdx].l) loIdx=i; } return { up: 100*hiIdx/p, down: 100*loIdx/p }; }
function roc(a,p=12){ if(a.length<p+1)return null; const prev=a[a.length-1-p]; return prev===0?null:((a.at(-1)-prev)/prev)*100; }
function vortex(c,p=14){ if(c.length<p+1)return null; let vp=0,vm=0,tr=0; for(let i=c.length-p;i<c.length;i++){ vp+=Math.abs(c[i].h-c[i-1].l); vm+=Math.abs(c[i].l-c[i-1].h); tr+=Math.max(c[i].h-c[i].l,Math.abs(c[i].h-c[i-1].c),Math.abs(c[i].l-c[i-1].c)); } return tr===0?null:{ viPlus: vp/tr, viMinus: vm/tr }; }
function donchian(c,p=20){ if(c.length<p)return null; const s=c.slice(-p); return { upper: Math.max(...s.map(x=>x.h)), lower: Math.min(...s.map(x=>x.l)) }; }
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
  const close=c.map(x=>x.c),price=close.at(-1),e9=ema(close,9),e21=ema(close,21),e50=ema(close,50),e200=ema(close,200),rv=rsi(close),mv=macd(close),bb=bollinger(close),st=stochastic(c),av=atr(c),cv=cci(c),str=structure(c),ax=adx(c),ich=ichimoku(c),psar=parabolicSar(c),piv=pivotPoints(c),fib=fibLevels(c),wr=williamsR(c),kelt=keltnerChannel(c,close),ha=heikinAshiTrend(c),stnd=superTrend(c),aro=aroon(c),rc=roc(close),vtx=vortex(c),donch=donchian(c);
  // QEYD: OBV/MFI/VWAP HESABLANMIR — Deriv sintetik indekslərində real "volume" yoxdur
  // (aşağıda hər şam üçün v:1 sabit qoyulur), ona görə bu indikatorlar burda mənasız/aldadıcı
  // olardı və score-a əlavə "sanki-güvən" verərdi. OKX kripto versiyasında (real hədcm datası ilə)
  // bunlar saxlanılıb, çünki orda faktiki məna daşıyır.

  // === Qruplaşdırılmış səsvermə ===
  // Əvvəlki versiyada 16 indikator birbaşa 1x1 cəmlənirdi. Problemi budur ki, bunların çoxu
  // eyni məlumatı (məs. "overbought/oversold") fərqli formullarla təkrarlayır — RSI, Stochastic,
  // CCI və Williams %R demək olar həmişə birlikdə hərəkət edir. Onları müstəqil "səs" kimi saymaq
  // confidence-i süni şəkildə şişirdirdi. İndi indikatorlar 4 "ailəyə" bölünür, hər ailə daxilində
  // çoxluq qaydası ilə YALNIZ ±1 səs verir, yekun score bu 4 ailənin cəmidir (-4..+4). Bu həm
  // real müstəqil dəlil sayını düzgün əks etdirir, həm də LONG/SHORT üçün 4 ailədən ən azı 3-nün
  // (75%) razılaşmasını tələb edərək zəif/qarışıq siqnalları əvvəlcədən süzür.

  let trendVotes=0,reasons=[];
  if(e9>e21&&e21>e50){trendVotes++;reasons.push('EMA trend +');}else if(e9<e21&&e21<e50){trendVotes--;reasons.push('EMA trend -');}
  if(e200!=null){if(price>e200){trendVotes++;reasons.push('EMA200 üzərində');}else{trendVotes--;reasons.push('EMA200 altında');}}
  if(mv>0){trendVotes++;reasons.push('MACD +');}else if(mv<0){trendVotes--;reasons.push('MACD -');}
  if(str>0){trendVotes++;reasons.push('higher highs/lows');}else if(str<0){trendVotes--;reasons.push('lower highs/lows');}
  if(ich&&ich.score!==0){if(ich.score>0){trendVotes++;reasons.push('Ichimoku bulud üzərində');}else{trendVotes--;reasons.push('Ichimoku bulud altında');}}
  if(psar){if(psar.uptrend){trendVotes++;reasons.push('Parabolic SAR yüksəliş');}else{trendVotes--;reasons.push('Parabolic SAR düşüş');}}
  if(ha!==0){if(ha>0){trendVotes++;reasons.push('Heikin-Ashi yüksəliş');}else{trendVotes--;reasons.push('Heikin-Ashi düşüş');}}
  if(stnd){if(stnd.uptrend){trendVotes++;reasons.push('SuperTrend yüksəliş');}else{trendVotes--;reasons.push('SuperTrend düşüş');}}
  if(aro){if(aro.up>70&&aro.down<30){trendVotes++;reasons.push('Aroon yüksəliş');}else if(aro.down>70&&aro.up<30){trendVotes--;reasons.push('Aroon düşüş');}}
  if(vtx){if(vtx.viPlus>vtx.viMinus){trendVotes++;reasons.push('Vortex +');}else if(vtx.viMinus>vtx.viPlus){trendVotes--;reasons.push('Vortex -');}}
  const trendCat=trendVotes>0?1:trendVotes<0?-1:0;

  let momVotes=0;
  if(rv<35){momVotes++;reasons.push('RSI oversold');}else if(rv>65){momVotes--;reasons.push('RSI overbought');}
  if(st<20){momVotes++;reasons.push('Stoch oversold');}else if(st>80){momVotes--;reasons.push('Stoch overbought');}
  if(cv<-100){momVotes++;reasons.push('CCI oversold');}else if(cv>100){momVotes--;reasons.push('CCI overbought');}
  if(wr!=null){if(wr<-80){momVotes++;reasons.push('Williams %R oversold');}else if(wr>-20){momVotes--;reasons.push('Williams %R overbought');}}
  if(rc!=null){if(rc>0){momVotes++;reasons.push('ROC +');}else if(rc<0){momVotes--;reasons.push('ROC -');}}
  const momCat=momVotes>0?1:momVotes<0?-1:0;

  let volVotes=0;
  if(bb){if(price<=bb.lower){volVotes++;reasons.push('BB alt zolaq');}else if(price>=bb.upper){volVotes--;reasons.push('BB üst zolaq');}}
  if(kelt){if(price<=kelt.lower){volVotes++;reasons.push('Keltner alt zolaq');}else if(price>=kelt.upper){volVotes--;reasons.push('Keltner üst zolaq');}}
  if(donch){if(price<=donch.lower){volVotes++;reasons.push('Donchian alt sərhəd');}else if(price>=donch.upper){volVotes--;reasons.push('Donchian üst sərhəd');}}
  const volCat=volVotes>0?1:volVotes<0?-1:0;

  let srVotes=0;
  if(piv){if(price>piv.pp){srVotes++;reasons.push('Pivot üzərində');}else if(price<piv.pp){srVotes--;reasons.push('Pivot altında');}}
  if(fib){if(price>fib.r500){srVotes++;reasons.push('Fib 50% üzərində');}else{srVotes--;reasons.push('Fib 50% altında');}}
  const srCat=srVotes>0?1:srVotes<0?-1:0;

  const score=trendCat+momCat+volCat+srCat; // -4..+4, hər ailədən max ±1

  let confidence=Math.round(Math.abs(score)/4*100);
  // ADX artıq sərt kəsici (əvvəlki: <15 bağla, >=25 +8 bonus) deyil, davamlı multiplier-dir:
  // ADX<=10-da confidence-in 55%-i, ADX>=40-da 100%-i qalır — beləcə ADX=14/16 kimi sərhəd
  // hallarda qəfil keyfiyyət sıçrayışı olmur.
  if(ax!=null){
    const adxFactor=Math.max(0,Math.min(1,(ax-10)/30));
    confidence=Math.round(confidence*(0.55+0.45*adxFactor));
    reasons.push(`ADX ${ax.toFixed(0)}`);
  }
  let signal=score>=3?'LONG':score<=-3?'SHORT':'WAIT';
  // Çox zəif/trendsiz bazar (ADX<10): bu şəraitdə demək olar bütün trend indikatorları aldadıcıdır,
  // ona görə MIN_CONFIDENCE-dan asılı olmayaraq tam bloklanır
  if (ax!=null && ax<10) { signal='WAIT'; reasons.push('ADX<10 — trendsiz bazar, bloklandı'); }
  // Spike filtri: sintetik indekslərdə (xüsusən Boom/Crash/Jump) tək şamda ATR-dən 3+ dəfə
  // böyük hərəkət baş verə bilər — bu zaman indikatorlar etibarsızdır, siqnal bloklanır
  const lastBar=c.at(-1), barRange=lastBar.h-lastBar.l;
  const isSpike = av!=null && barRange > av*3;
  if (isSpike) { signal='WAIT'; reasons.push('Qeyri-adi sıçrayış (spike) aşkarlandı — bloklandı'); }
  return{signal,confidence,score,price,atr:av,rsi:rv,adx:ax,reasons};
}

function fullAnalysis(symbol){
  const a = {};
  for (const tf of ALL_TFS) a[tf] = analyze(getCandles(symbol, tf));
  const m5 = a[CONFIRM_TF], m15 = a['15m'], h1 = a[TREND_TF];
  if (!m15) return null;

  let final = 'WAIT', expiry = null, strength = 'zəif';
  let confluence = 0;
  if (m15.signal !== 'WAIT') confluence++;
  if (h1 && h1.signal === m15.signal) confluence++;
  if (m5 && m5.signal === m15.signal) confluence++;

  if (h1 && m15.signal !== 'WAIT' && m15.signal === h1.signal) {
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

  // === Makro (1D) trend + 4H/24H qiymət aralığı — yalnız kontekst, LONG/SHORT qərarına təsir etmir ===
  const d1 = a['1d'];
  const macroTrend = d1 ? (d1.score > 0 ? 'up' : d1.score < 0 ? 'down' : 'flat') : null;
  const last4h = getCandles(symbol, '4h').at(-1);
  const range4h = last4h ? { lo: last4h.l, hi: last4h.h } : null;
  const last24h1 = getCandles(symbol, '1h').slice(-24);
  const range24h = last24h1.length ? { lo: Math.min(...last24h1.map(x => x.l)), hi: Math.max(...last24h1.map(x => x.h)) } : null;

  return { symbol, dir, confidence, expiry, strength, confluence, price: m15.price, atr: m15.atr, rsi: m15.rsi, timeframes: a, reasons, macroTrend, range4h, range24h };
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
      lastAnyCandleAt = Date.now();
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
async function buyContract(symbol, dir, durationMin) {
  const proposalMsg = await tradingRequest({
    proposal: 1,
    amount: DERIV_STAKE_AMOUNT,
    basis: 'stake',
    contract_type: dir,
    currency: tradingCurrency || 'USD',
    underlying_symbol: symbol,
    duration: durationMin,
    duration_unit: 'm',
  });
  const p = proposalMsg.proposal;
  if (!p?.id) throw new Error('Proposal alınmadı');
  const buyMsg = await tradingRequest({ buy: p.id, price: p.ask_price });
  return buyMsg.buy;
}

async function sellContract(contractId) {
  const msg = await tradingRequest({ sell: contractId, price: 0 });
  return msg.sell;
}

// === Gündəlik itki limiti (kill-switch) ===
// Krip-to botundakı eyni prinsip: gündə balansın müəyyən faizindən çox itirilsə, AL düymələri
// avtomatik bloklanır — sabahkı balans sıfırlanmasına qədər. Default 0 = deaktiv (istəyə bağlı,
// çünki bəzi istifadəçilər üçün DEMO hesabda mənası olmaya bilər); real hesabda MAX_DAILY_LOSS_PCT
// env dəyişənini (məs. 5) təyin etməklə aktivləşdirilir.
const MAX_DAILY_LOSS_PCT = Number(process.env.MAX_DAILY_LOSS_PCT || 0);
async function getBalance() {
  const msg = await tradingRequest({ balance: 1 });
  return msg?.balance || null;
}
async function checkDailyLossLimit() {
  if (MAX_DAILY_LOSS_PCT <= 0) return;
  if (!tradingAuthorized) return;
  try {
    const bal = await getBalance();
    const eq = Number(bal?.balance);
    if (!eq || isNaN(eq)) return;
    const now = Date.now(), dayMs = 24 * 60 * 60 * 1000;
    if (!state.dayStartEquity || now - state.dayStartAt > dayMs) {
      state.dayStartEquity = eq; state.dayStartAt = now; state.tradingHalted = false; state.haltReason = null;
      saveState();
      return;
    }
    const dropPct = ((state.dayStartEquity - eq) / state.dayStartEquity) * 100;
    if (dropPct >= MAX_DAILY_LOSS_PCT && !state.tradingHalted) {
      state.tradingHalted = true;
      state.haltReason = `Gündəlik zərər limiti aşıldı: -${dropPct.toFixed(2)}% (limit ${MAX_DAILY_LOSS_PCT}%)`;
      saveState();
      await telegram('sendMessage', {
        chat_id: TELEGRAM_CHAT_ID, parse_mode: 'HTML',
        text: `🛑 <b>KILL-SWITCH AKTİVLƏŞDİ</b>\n${state.haltReason}\nAL düymələri müvəqqəti bloklandı. Balans növbəti gün sıfırlananda avtomatik bərpa olunacaq.`,
      });
    }
  } catch (e) { console.error('[risk] gündəlik itki yoxlaması xətası:', e.message); }
}
setInterval(checkDailyLossLimit, 5 * 60 * 1000);

function expiryMinutes(expiry) {
  const m = /^(\d+)/.exec(expiry || '');
  return m ? Number(m[1]) : 15;
}

// Watchdog: WS "açıq" görünsə də Deriv bəzən data axınını səssizcə kəsir.
// 90 saniyə heç bir yeni şam gəlməzsə, bağlantını məcburi bağlayıb yenidən qururuq.
setInterval(() => {
  if (state.derivConnected && Date.now() - lastAnyCandleAt > 90000) {
    console.warn('[deriv] 90 saniyədir yeni şam gəlmir, bağlantı yenidən qurulur');
    try { derivWs && derivWs.close(); } catch {}
  }
}, 15000);

let analysisTimers = new Map();
function onCandleUpdate(symbol) {
  if (analysisTimers.has(symbol)) return;
  analysisTimers.set(symbol, setTimeout(() => {
    analysisTimers.delete(symbol);
    runAnalysis(symbol);
  }, 500));
}

function runAnalysis(symbol) {
  let r;
  try { r = fullAnalysis(symbol); }
  catch (e) { console.error(`[analiz] ${symbol} xətası:`, e.message); return; }
  if (!r) return;
  state.lastAnalysis.set(symbol, r);
  broadcast({ type: 'analysis', data: r });

  if (!state.scannerEnabled) return;
  if (r.dir === 'WAIT' || r.confidence < MIN_CONFIDENCE) { state.pendingDir.delete(symbol); return; }

  // Ən azı 2 ardıcıl analiz eyni istiqaməti təsdiqləməlidir — tək tiklik "yanlış sıçrayış"
  // (qiymətin bir anlıq irəli-geri hərəkəti) siqnal doğurmasın deyə
  const pend = state.pendingDir.get(symbol);
  if (!pend || pend.dir !== r.dir) {
    state.pendingDir.set(symbol, { dir: r.dir, count: 1 });
    return;
  }
  pend.count++;
  if (pend.count < 2) return;

  const prev = state.lastSignalAt.get(symbol);
  const cooldownMs = SIGNAL_COOLDOWN_MIN * 60 * 1000;
  const now = Date.now();
  const dirChanged = !prev || prev.dir !== r.dir;
  const bigConfidenceShift = prev && Math.abs((prev.confidence ?? 0) - r.confidence) >= 15;
  const cooldownPassed = prev && now - prev.ts >= cooldownMs;
  if (!dirChanged && !bigConfidenceShift && !cooldownPassed) return;

  // === Kalibrasiya/EV süzgəci ===
  // Bu confidence bucket-i üçün kifayət qədər tarixi (virtual və ya real) nəticə toplanıbsa və
  // faktiki win-rate breakeven-dən aşağıdırsa, bu bucket statistik olaraq zərərli deməkdir —
  // MIN_CONFIDENCE-i keçsə belə siqnal göndərilmir. Cooldown/lastSignalAt yenə də güncəllənir ki,
  // hər analiz dövründə eyni bloklanmış siqnal spam kimi təkrar-təkrar qiymətləndirilməsin.
  state.lastSignalAt.set(symbol, { dir: r.dir, ts: now, confidence: r.confidence });
  const calWin = calibratedWinProb(r.confidence);
  if (calWin != null && calWin < BREAKEVEN_PROB) {
    console.log(`[siqnal] ${symbol} ${r.dir} bloklandı — bucket win-rate ${calWin.toFixed(1)}% < breakeven ${BREAKEVEN_PROB.toFixed(1)}%`);
    return;
  }

  const entry = { ts: now, symbol, dir: r.dir, confidence: r.confidence, expiry: r.expiry, strength: r.strength, confluence: r.confluence, price: r.price, reasons: r.reasons, calibratedWinProb: calWin, macroTrend: r.macroTrend, range4h: r.range4h, range24h: r.range24h };
  state.signals.unshift(entry);
  state.signals = state.signals.slice(0, 200);
  saveState();
  broadcast({ type: 'signal', data: entry });
  sendTelegramSignal(r);
  scheduleOutcomeCheck(entry); // real pul qoyulmasa belə, bu siqnalın "virtual" nəticəsini izləyib özünü-kalibrləmə üçün topla
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
function macroEmoji(t) { return t === 'up' ? '🟢 yuxarı' : t === 'down' ? '🔴 aşağı' : t ? '⚪ neytral' : '—'; }
function rangeText(r) { return r ? `${fmt(r.lo)} – ${fmt(r.hi)}` : '—'; }
async function sendTelegramSignal(r) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) { console.error('[telegram] TOKEN/CHAT_ID boşdur, siqnal göndərilmədi'); return; }
  const emoji = r.dir === 'CALL' ? '📈' : '📉';
  const dirText = r.dir === 'CALL' ? '🟢 LONG (AL)' : '🔴 SHORT (SAT)';
  const expText = r.expiry === '15m' ? '15 dəqiqə' : '5 dəqiqə';
  const calWin = calibratedWinProb(r.confidence);
  const calText = calWin != null
    ? `${calWin.toFixed(0)}% (breakeven: ${BREAKEVEN_PROB.toFixed(0)}%)`
    : `hələ kifayət qədər tarixi nəticə yoxdur (min. ${MIN_SAMPLES_FOR_CALIBRATION})`;
  const autoTradeNote = DERIV_API_TOKEN
    ? '<i>Aşağıdakı düymə ilə bir kliklə Deriv-də əməliyyat açıla bilər.</i>'
    : '<i>Bu avtomatik texniki siqnaldır, maliyyə məsləhəti deyil. Auto-trade hələ aktiv deyil.</i>';
  const lines = [
    `💎 <b>${r.symbol}</b>`,
    `${dirText}   Etibar: <b>${r.confidence}%</b> ${confBar(r.confidence)}`,
    `📍 Giriş: <code>${fmt(r.price)}</code>`,
    `⏱ Expiry: <b>${expText}</b> (${r.strength}${r.confluence != null ? `, 🟢${r.confluence}/3` : ''})`,
    `${emoji} Kalibrlənmiş nəticə: <b>${calText}</b>`,
    `📅 Makro (1D): ${macroEmoji(r.macroTrend)}`,
    `📊 4H aralıq: ${rangeText(r.range4h)}`,
    `📊 24H aralıq: ${rangeText(r.range24h)}`,
    `🧩 Səbəblər: ${r.reasons.join(', ')}`,
    `<i>ℹ️ Bu Rise/Fall (sabit ödənişli) opsiondur — açıq mövqe olmadığı üçün Stop Loss/Take Profit/Leverage tətbiq olunmur; müqavilə ${expText} sonra avtomatik bağlanır.</i>`,
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
  const chatId = cq.message.chat.id;
  const messageId = cq.message.message_id;
  const baseText = cq.message.text || '';
  try {
    if (data.startsWith('B|')) {
      if (state.tradingHalted) {
        await telegram('answerCallbackQuery', { callback_query_id: cq.id, text: `🛑 Bloklanıb: ${state.haltReason || 'kill-switch aktiv'}`, show_alert: true });
        return;
      }
      const [, symbol, dir, durStr] = data.split('|');
      const durMin = Number(durStr) || 15;
      const buy = await buyContract(symbol, dir, durMin);
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

let tgUpdateOffset = 0;
let tgPollDelay = 500; // uğursuz cəhdlərdə tədricən artır (max 30s) — Conflict zamanı iki instansiyanın bir-birini daim "boğmasının" qarşısını alır
async function pollTelegramUpdates() {
  if (!TELEGRAM_TOKEN) return;
  try {
    const res = await telegram('getUpdates', { offset: tgUpdateOffset, timeout: 25, allowed_updates: ['callback_query'] });
    if (res && res.ok && Array.isArray(res.result)) {
      tgPollDelay = 500; // uğurlu cavab — gecikməni sıfırla
      for (const upd of res.result) {
        tgUpdateOffset = upd.update_id + 1;
        if (upd.callback_query) await handleCallbackQuery(upd.callback_query);
      }
    } else if (res && res.ok === false) {
      tgPollDelay = Math.min(tgPollDelay * 2, 30000); // Conflict və s. — geriyə-çəkilmə
    }
  } catch (e) {
    console.error('[telegram] polling xətası:', e.message);
    tgPollDelay = Math.min(tgPollDelay * 2, 30000);
  } finally {
    setTimeout(pollTelegramUpdates, tgPollDelay);
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

app.get('/api/stats', (req, res) => {
  const buckets = [...state.outcomeStats.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([bucket, s]) => {
      const total = s.wins + s.losses;
      return {
        bucket: `${bucket}-${bucket + CONF_BUCKET_SIZE - 1}%`,
        wins: s.wins,
        losses: s.losses,
        total,
        winRate: total ? +((100 * s.wins) / total).toFixed(1) : null,
        calibrated: total >= MIN_SAMPLES_FOR_CALIBRATION,
      };
    });
  res.json({
    payoutPct: PAYOUT_PCT,
    breakevenProb: +BREAKEVEN_PROB.toFixed(1),
    minSamplesForCalibration: MIN_SAMPLES_FOR_CALIBRATION,
    pendingOutcomes: state.pendingOutcomes.length,
    buckets,
  });
});
app.get('/api/state', (req, res) => {
  res.json({
    online: true,
    derivConnected: state.derivConnected,
    scannerEnabled: state.scannerEnabled,
    symbols: state.activeSymbols,
    signals: state.signals.slice(0, 50),
    analysis: Object.fromEntries(state.lastAnalysis),
    startedAt: state.startedAt,
    tradingHalted: state.tradingHalted,
    haltReason: state.haltReason,
    persistence: pgPool ? 'postgres' : 'file (müvəqqəti — DATABASE_URL yoxdur)',
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
  // Unudulmuş/köhnə webhook aktivdirsə, getUpdates (uzun-polling) onunla İŞLƏMİR və Conflict xətası verir —
  // ona görə hər startup-da avtomatik yoxlanılıb təmizlənir (bu, polling başlamazdan ƏVVƏL edilməlidir).
  const wh = await telegram('getWebhookInfo', {});
  if (wh?.ok && wh.result?.url) {
    console.warn(`[telegram] Aktiv webhook tapıldı (${wh.result.url}) — getUpdates ilə uyğun gəlmir, silinir...`);
    await telegram('deleteWebhook', {});
  }
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

async function main() {
  await initDb();
  await loadState();
  connectDeriv();
  connectTradingWs();
  await verifyTelegramOnBoot(); // webhook təmizlənməsi polling BAŞLAMAZDAN ƏVVƏL bitməlidir (Conflict-in qarşısını alır)
  pollTelegramUpdates();
  reportTradingStatusOnBoot();
}
main();
