# app.py — Instagram Profile API v25
# TopSearch (3 retries) + Deep DOM (wait for header) | NO web_profile_info
# 4s API timeout | 40s page timeout | Fast + Reliable

import asyncio
import gc
import hashlib
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import unquote

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

try:
    from playwright.async_api import (
        async_playwright, Browser, BrowserContext, Page, Route,
        TimeoutError as PlaywrightTimeout,
    )
except ImportError:
    sys.exit("playwright required:  pip install playwright && playwright install chromium")

try:
    from faker import Faker
except ImportError:
    sys.exit("faker required:  pip install faker")

import numpy as np


# ==================== CONFIG ====================
HEADLESS         = True
SLOW_MO          = 0
PAGE_TIMEOUT     = 40_000
API_TIMEOUT      = 4.0
HOMEPAGE_TIMEOUT = 10_000
HEADER_WAIT      = 10_000
COOKIE_FILE      = Path(os.getenv("COOKIE_FILE", "dxm.txt"))
PROXY_FILE       = Path(os.getenv("PROXY_FILE", "proxy.txt"))
CACHE_DIR        = Path(os.getenv("CACHE_DIR", "/tmp/ig_cache"))
CACHE_TTL_MIN    = int(os.getenv("CACHE_TTL_MIN", "120"))
PROXY_ENABLED    = os.getenv("PROXY_ENABLED", "true").lower() == "true"

MAX_CONCURRENT_BROWSERS = int(os.getenv("MAX_CONCURRENT_BROWSERS", "1"))
BROWSER_RECYCLE_AFTER   = int(os.getenv("BROWSER_RECYCLE_AFTER", "20"))
GC_AGGRESSIVE           = os.getenv("GC_AGGRESSIVE", "true").lower() == "true"

VALID_KEYS = {"ANSHPAPA": "full_access", "FF": "full_access"}
HEALTH_KEY = os.getenv("HEALTH_KEY", "ANSHPAPA")


fake = Faker()
rng  = np.random.default_rng()

CHROMIUM_ARGS = [
    "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
    "--disable-gpu", "--disable-software-rasterizer",
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process,TranslateUI,BlinkGenPropertyTrees,CalculateNativeWinOcclusion,AutomationControlled",
    "--disable-background-networking", "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows", "--disable-renderer-backgrounding",
    "--disable-ipc-flooding-protection", "--disable-default-apps", "--disable-sync",
    "--disable-translate", "--disable-extensions", "--disable-plugins-discovery",
    "--disable-component-update", "--disable-domain-reliability",
    "--disable-client-side-phishing-detection", "--disable-hang-monitor",
    "--disable-popup-blocking", "--disable-prompt-on-repost", "--metrics-recording-only",
    "--no-first-run", "--no-default-browser-check", "--mute-audio", "--hide-scrollbars",
    "--window-size=1366,768", "--memory-pressure-off", "--disable-dev-tools",
    "--js-flags=--max-old-space-size=128", "--renderer-process-limit=1",
    "--disk-cache-size=1", "--media-cache-size=1", "--disable-logging",
    "--silent-debugger-extension-api", "--force-color-profile=srgb",
    "--disable-accelerated-2d-canvas",
    "--enable-features=NetworkService,NetworkServiceInProcess",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

IG_APP_ID = "936619743392459"

SERVER_START_TIME = time.time()
REQUEST_COUNTER = {"total": 0, "success": 0, "failed": 0, "cached": 0}
LAST_SCRAPE = {"time": None, "username": None, "duration": None, "source": None}


# ============================================================================
# BROWSER POOL
# ============================================================================
class BrowserPool:
    def __init__(self):
        self._pw = None
        self._browser: Optional[Browser] = None
        self._lock = asyncio.Lock()
        self._context_count = 0
        self._total_recycles = 0
        self._started = False
        self._start_time = None

    async def start(self):
        if self._started: return
        async with self._lock:
            if self._started: return
            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(
                headless=HEADLESS, slow_mo=SLOW_MO, args=CHROMIUM_ARGS,
            )
            self._started = True
            self._start_time = time.time()
            print("[pool] browser started")

    async def acquire_browser(self) -> Browser:
        if not self._started:
            await self.start()
        async with self._lock:
            if self._context_count >= BROWSER_RECYCLE_AFTER:
                print(f"[pool] recycling after {self._context_count} contexts")
                try: await self._browser.close()
                except Exception: pass
                self._browser = await self._pw.chromium.launch(
                    headless=HEADLESS, slow_mo=SLOW_MO, args=CHROMIUM_ARGS,
                )
                self._context_count = 0
                self._total_recycles += 1
                self._start_time = time.time()
                if GC_AGGRESSIVE: gc.collect()
            self._context_count += 1
            return self._browser

    async def shutdown(self):
        if self._browser:
            try: await self._browser.close()
            except Exception: pass
        if self._pw:
            try: await self._pw.stop()
            except Exception: pass
        self._started = False
        print("[pool] shutdown complete")

    def stats(self):
        return {
            "started": self._started,
            "context_count": self._context_count,
            "total_recycles": self._total_recycles,
            "uptime_seconds": round(time.time() - self._start_time, 2) if self._start_time else 0,
        }


BROWSER_POOL = BrowserPool()
BROWSER_SEM = asyncio.Semaphore(MAX_CONCURRENT_BROWSERS)


# ============================================================================
# PROXY POOL
# ============================================================================
class ProxyPool:
    def __init__(self):
        self.entries: List[Dict] = []
        self._recent: List[int] = []
        self._cooldown: Dict[int, float] = {}
        self._use_count: Dict[int, int] = {}
        self.load()

    def load(self):
        self.entries = []
        if not PROXY_ENABLED: return
        if not PROXY_FILE.exists():
            print(f"[proxy] {PROXY_FILE} not found — direct")
            return
        for raw in PROXY_FILE.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"): continue
            p = self._parse(line)
            if p: self.entries.append(p)
        for i, e in enumerate(self.entries):
            self._use_count[i] = 0
            print(f"[proxy] {e['server']}")

    @staticmethod
    def _parse(line: str) -> Optional[Dict]:
        m = re.match(r"^(https?://)?([^:@/\s]+):([^@/\s]+)@([^:/\s]+):(\d+)$", line)
        if m:
            s, u, pw, h, p = m.groups()
            return {"server": f"{s or 'http://'}{h}:{p}", "username": u, "password": pw}
        m = re.match(r"^([^:\s]+):(\d+):([^:\s]+):(.+)$", line)
        if m:
            h, p, u, pw = m.groups()
            return {"server": f"http://{h}:{p}", "username": u, "password": pw.strip()}
        m = re.match(r"^(https?://)?([^:/\s]+):(\d+)$", line)
        if m:
            s, h, p = m.groups()
            return {"server": f"{s or 'http://'}{h}:{p}"}
        return None

    def next(self) -> Optional[Dict]:
        if not self.entries: return None
        n = len(self.entries)
        now = time.time()
        self._cooldown = {i: t for i, t in self._cooldown.items() if now - t < 45}
        available = [i for i in range(n) if i not in self._recent and i not in self._cooldown]
        if not available:
            available = [i for i in range(n) if i not in self._cooldown]
        if not available:
            available = list(range(n))
        idx = random.choice(available)
        self._recent.append(idx)
        if len(self._recent) > min(3, max(1, n - 1)):
            self._recent.pop(0)
        self._cooldown[idx] = now
        self._use_count[idx] = self._use_count.get(idx, 0) + 1
        return self.entries[idx]

    def size(self): return len(self.entries)

    def stats(self):
        return {
            "total": len(self.entries),
            "enabled": PROXY_ENABLED,
            "in_cooldown": len(self._cooldown),
            "usage_per_proxy": {self.entries[i]["server"]: c for i, c in self._use_count.items() if i < len(self.entries)},
        }


PROXY_POOL = ProxyPool()


# ============================================================================
# COOKIES
# ============================================================================
def load_cookies(path: Path) -> Dict[str, str]:
    if not path.exists(): return {}
    out = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"): continue
        parts = line.split("\t")
        if len(parts) < 7:
            parts = re.split(r"\s{2,}", line)
        if len(parts) < 7:
            toks = line.split()
            if len(toks) >= 2:
                n, v = toks[-2], toks[-1]
                if re.fullmatch(r"[A-Za-z0-9_]+", n): out[n] = v
            continue
        n, v = parts[5].strip(), parts[6].strip()
        if n and v:
            try: v = unquote(v)
            except Exception: pass
            out[n] = v
    return out


def cookie_expiry_info(path: Path) -> Dict[str, Any]:
    info = {"cookies": {}, "file_exists": False, "file_path": str(path)}
    if not path.exists(): return info
    info["file_exists"] = True
    info["file_mtime"] = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
    info["file_size"] = path.stat().st_size
    now = time.time()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"): continue
        parts = line.split("\t")
        if len(parts) < 7: continue
        try:
            expiry_ts = int(parts[4])
            name = parts[5].strip()
            value = parts[6].strip()
            if expiry_ts == 0:
                status = "session_only"; days_left = None; expires_at = None
            elif expiry_ts < now:
                status = "EXPIRED"
                days_left = round((expiry_ts - now) / 86400, 2)
                expires_at = datetime.fromtimestamp(expiry_ts).isoformat()
            else:
                days_left = round((expiry_ts - now) / 86400, 2)
                expires_at = datetime.fromtimestamp(expiry_ts).isoformat()
                if days_left < 1: status = "expires_soon"
                elif days_left < 7: status = "expires_this_week"
                else: status = "valid"
            masked = value[:6] + "..." + value[-4:] if len(value) > 12 else value[:3] + "..."
            info["cookies"][name] = {
                "status": status, "expires_at": expires_at,
                "days_left": days_left, "value_preview": masked,
                "value_length": len(value),
            }
        except Exception:
            continue
    return info


_AUTH_COOKIES = {"sessionid", "ds_user_id", "csrftoken", "mid", "ig_did", "rur"}

def cookies_pw(c: Dict[str, str]) -> List[Dict]:
    out = []
    for k, v in c.items():
        is_auth = k in _AUTH_COOKIES
        out.append({
            "name": k, "value": v, "domain": ".instagram.com", "path": "/",
            "secure": True,
            "httpOnly": k in ("sessionid", "ds_user_id", "mid", "csrftoken"),
            "sameSite": "None" if is_auth else "Lax",
        })
    return out


def cookies_header(c: Dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in c.items())


def cookie_init_script(c: Dict[str, str]) -> str:
    parts = []
    for k, v in c.items():
        sv = v.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'try{{document.cookie="{k}={sv}; path=/; domain=.instagram.com";}}catch(e){{}}')
    return "(() => {" + "".join(parts) + "})();"


# ============================================================================
# CACHE
# ============================================================================
class Cache:
    MAX_MEM_ENTRIES = 200
    def __init__(self, d: Path, ttl_min: int):
        self.dir = d
        self.dir.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(minutes=ttl_min)
        self.mem: Dict[str, tuple] = {}
        self._hits = 0
        self._miss = 0

    def _k(self, u): return hashlib.sha256(u.lower().encode()).hexdigest()[:16]

    def get(self, u):
        k = self._k(u)
        if k in self.mem:
            ts, d = self.mem[k]
            if datetime.now() - datetime.fromtimestamp(ts) < self.ttl:
                self._hits += 1; return d
            self.mem.pop(k, None)
        f = self.dir / f"{k}.json"
        if f.exists():
            try:
                o = json.loads(f.read_text(encoding="utf-8"))
                at = datetime.fromisoformat(o["_at"])
                if datetime.now() - at < self.ttl:
                    if len(self.mem) >= self.MAX_MEM_ENTRIES:
                        self.mem.pop(next(iter(self.mem)), None)
                    self.mem[k] = (at.timestamp(), o["data"])
                    self._hits += 1
                    return o["data"]
                f.unlink(missing_ok=True)
            except Exception: pass
        self._miss += 1
        return None

    def set(self, u, d):
        k = self._k(u)
        if len(self.mem) >= self.MAX_MEM_ENTRIES:
            self.mem.pop(next(iter(self.mem)), None)
        self.mem[k] = (time.time(), d)
        try:
            (self.dir / f"{k}.json").write_text(
                json.dumps({"_at": datetime.now().isoformat(), "data": d}, ensure_ascii=False),
                encoding="utf-8")
        except Exception: pass

    def clear(self):
        self.mem.clear(); self._hits = 0; self._miss = 0
        for f in self.dir.glob("*.json"):
            try: f.unlink()
            except: pass

    def stats(self):
        return {"mem": len(self.mem), "disk": len(list(self.dir.glob("*.json"))),
                "hits": self._hits, "miss": self._miss,
                "ttl_min": int(self.ttl.total_seconds() / 60)}


CACHE = Cache(CACHE_DIR, CACHE_TTL_MIN)


# ============================================================================
# FINGERPRINT
# ============================================================================
class Fingerprint:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.platform = "Win32" if "Windows" in self.ua else "MacIntel"
        self.locale = "en-US"
        self.timezone = random.choice(["Asia/Kolkata", "Asia/Dubai", "Europe/London", "America/New_York"])
        self.cores = random.choice([4, 8, 12])
        self.mem = random.choice([8, 16])
        self.screen_w = random.choice([1366, 1440, 1536, 1920])
        self.screen_h = random.choice([768, 900, 864, 1080])
        self.scale = random.choice([1, 1, 1.25])
        self.seed = random.randint(1, 2**31 - 1)
        try:
            self.chrome_ver = re.search(r"Chrome/(\d+)", self.ua).group(1)
        except Exception:
            self.chrome_ver = "131"

    def viewport(self):
        return {"width": self.screen_w - random.randint(20, 60),
                "height": self.screen_h - random.randint(80, 140)}

    def screen_dict(self):
        return {"width": self.screen_w, "height": self.screen_h}

    def stealth_script(self) -> str:
        return """
(() => {
    const _plat = '%s';
    const _cores = %d;
    const _mem = %d;
    const _seed = %d;
    const _tz = '%s';
    const _chrome_ver = '%s';
    const _sw = %d, _sh = %d;

    let _sd = _seed >>> 0;
    const rnd = () => { _sd = (_sd * 1664525 + 1013904223) >>> 0; return _sd / 4294967296; };

    const _marked = new WeakSet();

    try {
        const navProto = Object.getPrototypeOf(navigator);
        if ('webdriver' in navProto) delete navProto.webdriver;
        if ('webdriver' in navigator) delete navigator.webdriver;
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
    } catch(e) {}

    const navProps = {
        platform: _plat, hardwareConcurrency: _cores, deviceMemory: _mem,
        languages: Object.freeze(['en-US','en']), language: 'en-US',
        maxTouchPoints: 0, vendor: 'Google Inc.', vendorSub: '',
        productSub: '20030107', pdfViewerEnabled: true, onLine: true,
    };
    for (const [k, v] of Object.entries(navProps)) {
        try { Object.defineProperty(navigator, k, { get: () => v, configurable: true }); } catch(e) {}
    }

    try {
        const uaData = {
            brands: [
                { brand: 'Not_A Brand', version: '24' },
                { brand: 'Chromium', version: _chrome_ver },
                { brand: 'Google Chrome', version: _chrome_ver },
            ],
            mobile: false,
            platform: _plat === 'Win32' ? 'Windows' : 'macOS',
        };
        Object.defineProperty(navigator, 'userAgentData', {
            get: () => ({
                ...uaData,
                getHighEntropyValues: () => Promise.resolve({
                    ...uaData, architecture: 'x86', bitness: '64',
                    fullVersionList: [
                        { brand: 'Not_A Brand', version: '24.0.0.0' },
                        { brand: 'Chromium', version: _chrome_ver + '.0.0.0' },
                        { brand: 'Google Chrome', version: _chrome_ver + '.0.0.0' },
                    ],
                    model: '', platformVersion: '15.0.0',
                    uaFullVersion: _chrome_ver + '.0.0.0', wow64: false,
                }),
                toJSON: () => uaData,
            }),
            configurable: true,
        });
    } catch(e) {}

    try {
        if (!window.chrome) window.chrome = {};
        window.chrome.runtime = window.chrome.runtime || {
            OnInstalledReason: { INSTALL: 'install', UPDATE: 'update' },
            PlatformArch: { X86_64: 'x86-64' },
            PlatformOs: { WIN: 'win' },
        };
        window.chrome.app = window.chrome.app || { getDetails: () => null };
        window.chrome.csi = () => ({ onloadT: Date.now() - 500, pageT: 500, startE: Date.now() - 500, tran: 15 });
    } catch(e) {}

    try {
        const mkMime = (type, suffixes, desc) => ({ type, suffixes, description: desc, enabledPlugin: null });
        const pdfMime1 = mkMime('application/pdf', 'pdf', 'Portable Document Format');
        const pdfMime2 = mkMime('text/pdf', 'pdf', 'Portable Document Format');
        const mkPlugin = (n, f, d, mimes) => {
            const p = { name: n, filename: f, description: d, length: mimes.length };
            mimes.forEach((m, i) => { p[i] = m; m.enabledPlugin = p; });
            p.item = (i) => mimes[i] || null;
            p.namedItem = (x) => mimes.find(y => y.type === x) || null;
            return p;
        };
        const plugins = [
            mkPlugin('PDF Viewer', 'internal-pdf-viewer', 'Portable Document Format', [pdfMime1, pdfMime2]),
            mkPlugin('Chrome PDF Viewer', 'internal-pdf-viewer', 'Portable Document Format', [pdfMime1, pdfMime2]),
            mkPlugin('Chromium PDF Viewer', 'internal-pdf-viewer', 'Portable Document Format', [pdfMime1, pdfMime2]),
        ];
        const pluginsArr = Object.assign([], plugins);
        pluginsArr.item = (i) => plugins[i] || null;
        pluginsArr.namedItem = (n) => plugins.find(p => p.name === n) || null;
        Object.defineProperty(navigator, 'plugins', { get: () => pluginsArr, configurable: true });
        Object.defineProperty(navigator, 'mimeTypes', { get: () => [pdfMime1, pdfMime2], configurable: true });
    } catch(e) {}

    try {
        if (navigator.permissions && navigator.permissions.query) {
            const oQ = navigator.permissions.query;
            navigator.permissions.query = (p) => Promise.resolve({ state: 'prompt', name: p?.name || '', onchange: null });
        }
    } catch(e) {}

    try {
        const oTD = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(...a) {
            try {
                const ctx = this.getContext('2d');
                if (ctx && this.width > 0) {
                    const w = Math.min(this.width, 40), h = Math.min(this.height, 40);
                    const img = ctx.getImageData(0, 0, w, h);
                    for (let i = 0; i < img.data.length; i += 4) {
                        img.data[i] = (img.data[i] + ((rnd() * 3) | 0)) & 0xff;
                        img.data[i+1] = (img.data[i+1] + ((rnd() * 3) | 0)) & 0xff;
                    }
                    ctx.putImageData(img, 0, 0);
                }
            } catch(e) {}
            return oTD.apply(this, a);
        };
    } catch(e) {}

    try {
        const patchGL = (Ctor) => {
            if (!Ctor) return;
            const oGP = Ctor.prototype.getParameter;
            Ctor.prototype.getParameter = function(p) {
                if (p === 37445 || p === 7936) return 'Google Inc. (Intel)';
                if (p === 37446 || p === 7937) return 'ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)';
                return oGP.call(this, p);
            };
        };
        patchGL(window.WebGLRenderingContext);
        patchGL(window.WebGL2RenderingContext);
    } catch(e) {}

    try {
        const stub = function() {
            this.close = () => {};
            this.createOffer = () => Promise.resolve({ type: 'offer', sdp: '' });
            this.createAnswer = () => Promise.resolve({ type: 'answer', sdp: '' });
            this.setLocalDescription = () => Promise.resolve();
            this.setRemoteDescription = () => Promise.resolve();
            this.addIceCandidate = () => Promise.resolve();
            this.getStats = () => Promise.resolve(new Map());
            this.addEventListener = () => {}; this.removeEventListener = () => {};
        };
        window.RTCPeerConnection = stub;
        window.webkitRTCPeerConnection = stub;
    } catch(e) {}

    try {
        if (!navigator.getBattery) {
            navigator.getBattery = () => Promise.resolve({ charging: true, level: 1, chargingTime: 0, dischargingTime: Infinity });
        }
    } catch(e) {}

    try {
        Object.defineProperty(navigator, 'connection', {
            get: () => ({ downlink: 10, effectiveType: '4g', rtt: 50, saveData: false, onchange: null }),
            configurable: true,
        });
    } catch(e) {}

    try {
        if (navigator.mediaDevices) {
            navigator.mediaDevices.enumerateDevices = () => Promise.resolve([
                { deviceId: 'default', kind: 'audioinput', label: '', groupId: 'default' },
                { deviceId: 'default', kind: 'videoinput', label: '', groupId: 'default' },
            ]);
        }
    } catch(e) {}

    try {
        const oBR = Element.prototype.getBoundingClientRect;
        Element.prototype.getBoundingClientRect = function() {
            const r = oBR.call(this);
            if (this.tagName === 'SPAN' && (this.textContent || '').length <= 3) {
                return new DOMRect(r.x, r.y, r.width + 0.3, r.height);
            }
            return r;
        };
    } catch(e) {}

    try { Object.defineProperty(Notification, 'permission', { get: () => 'denied', configurable: true }); } catch(e) {}

    try {
        if (screen.orientation) {
            Object.defineProperty(screen.orientation, 'angle', { get: () => 0, configurable: true });
            Object.defineProperty(screen.orientation, 'type', { get: () => 'landscape-primary', configurable: true });
        }
        Object.defineProperty(screen, 'colorDepth', { get: () => 24, configurable: true });
        Object.defineProperty(screen, 'pixelDepth', { get: () => 24, configurable: true });
    } catch(e) {}

    try {
        Object.defineProperty(window, 'outerWidth', { get: () => _sw, configurable: true });
        Object.defineProperty(window, 'outerHeight', { get: () => _sh, configurable: true });
    } catch(e) {}

    try {
        if (window.speechSynthesis) {
            speechSynthesis.getVoices = () => [
                { default: true, lang: 'en-US', localService: true, name: 'Microsoft David - English (United States)', voiceURI: 'Microsoft David' },
                { default: false, lang: 'en-US', localService: true, name: 'Microsoft Zira - English (United States)', voiceURI: 'Microsoft Zira' },
            ];
        }
    } catch(e) {}

    try { if (!navigator.getGamepads) navigator.getGamepads = () => [null, null, null, null]; } catch(e) {}
    try { if (!navigator.vibrate) navigator.vibrate = () => false; } catch(e) {}

    try {
        const oNow = performance.now.bind(performance);
        const off = rnd() * 0.5;
        performance.now = () => oNow() + off;
    } catch(e) {}

    try {
        const offsets = {
            'Asia/Kolkata': -330, 'Asia/Dubai': -240, 'Europe/London': 0,
            'America/New_York': 300, 'America/Los_Angeles': 480,
        };
        const off = offsets[_tz] !== undefined ? offsets[_tz] : 0;
        Date.prototype.getTimezoneOffset = () => off;
    } catch(e) {}

    try {
        const native = 'function () { [native code] }';
        const oTS = Function.prototype.toString;
        Function.prototype.toString = function() {
            if (_marked.has(this)) return native;
            const s = oTS.call(this);
            if (s.includes('playwright') || s.includes('puppeteer')) return native;
            return s;
        };
    } catch(e) {}

    try { document.hasFocus = () => true; } catch(e) {}
})();
""" % (self.platform, self.cores, self.mem, self.seed, self.timezone, self.chrome_ver,
        self.screen_w, self.screen_h)


# ============================================================================
# ROUTE FILTER
# ============================================================================
BLOCK_PATTERNS = (
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "facebook.com/tr", "connect.facebook.net", "hotjar.com", "fullstory.com",
    "sentry.io", "sentry-cdn.com", "amplitude.com", "mixpanel.com",
    "segment.io", "segment.com", "newrelic.com", "datadoghq.com",
    "optimizely.com", "criteo.com", "taboola.com", "outbrain.com",
    "adnxs.com", "pubmatic.com", "clarity.ms", "tiktok.com/i18n/pixel",
)
BLOCK_TYPES = {"image", "media", "font"}


async def route_filter(route: Route):
    try:
        req = route.request
        if any(b in req.url for b in BLOCK_PATTERNS):
            await route.abort(); return
        if req.resource_type in BLOCK_TYPES:
            await route.abort(); return
        await route.continue_()
    except Exception:
        try: await route.continue_()
        except: pass


# ============================================================================
# POPUP DISMISS
# ============================================================================
POPUP_SELECTORS = [
    'button:has-text("Not Now")',
    'button:has-text("Not now")',
    'button:has-text("Cancel")',
    'div[role="dialog"] [aria-label="Close"]',
    '[aria-label="Close"]',
]


async def dismiss_popups(page: Page) -> int:
    clicked = 0
    for sel in POPUP_SELECTORS:
        try:
            els = await page.query_selector_all(sel)
            for el in els:
                try:
                    if await el.is_visible():
                        await el.click(timeout=400, force=True)
                        clicked += 1
                        await page.wait_for_timeout(60)
                except Exception:
                    continue
            if clicked: break
        except Exception:
            continue
    return clicked


async def has_session_cookies(ctx: BrowserContext) -> bool:
    try:
        cookies = await ctx.cookies("https://www.instagram.com/")
        for c in cookies:
            if c.get("name") == "ds_user_id" and c.get("value"):
                return True
            if c.get("name") == "sessionid" and c.get("value"):
                return True
        return False
    except Exception:
        return False


# ============================================================================
# HELPERS
# ============================================================================
def year_from_id(uid):
    ranges = [
        (0, 100_000, 2008), (100_000, 1_000_000, 2010), (1_000_000, 5_000_000, 2011),
        (5_000_000, 20_000_000, 2012), (20_000_000, 80_000_000, 2013),
        (80_000_000, 200_000_000, 2014), (200_000_000, 400_000_000, 2015),
        (400_000_000, 700_000_000, 2016), (700_000_000, 1_000_000_000, 2017),
        (1_000_000_000, 2_500_000_000, 2018), (2_500_000_000, 4_500_000_000, 2019),
        (4_500_000_000, 7_000_000_000, 2020), (7_000_000_000, 10_000_000_000, 2021),
        (10_000_000_000, 20_000_000_000, 2022), (20_000_000_000, 40_000_000_000, 2023),
        (40_000_000_000, 60_000_000_000, 2024), (60_000_000_000, 80_000_000_000, 2025),
        (80_000_000_000, 999_999_999_999, 2026),
    ]
    for lo, hi, y in ranges:
        if lo <= uid < hi: return y
    return None


def parse_count(s):
    if not s: return None
    s = str(s).strip().replace(",", "").replace(" ", "")
    mult = 1
    if s and s[-1].upper() == "K": mult = 1_000; s = s[:-1]
    elif s and s[-1].upper() == "M": mult = 1_000_000; s = s[:-1]
    elif s and s[-1].upper() == "B": mult = 1_000_000_000; s = s[:-1]
    try: return int(float(s) * mult)
    except ValueError: return None


def upgrade_hd(url):
    if not url: return url
    url = re.sub(r"/s\d+x\d+/", "/", url)
    url = re.sub(r"stp=dst-jpg_s\d+x\d+[^&]*", "stp=dst-jpg", url)
    url = re.sub(r"stp=dst-jpg_s\d+x\d+_tt\d+&?", "", url)
    return re.sub(r"&&+", "&", url).replace("?&", "?").rstrip("&").rstrip("?")


def profile_url(u): return f"https://www.instagram.com/{u}/?hl=en"


def extract_bio_links(bio, ext, api_links=None):
    links, seen = [], set()
    def add(url, typ, text=None):
        if not url or url in seen: return
        seen.add(url)
        links.append({"url": url, "display_text": text or (url[:60] + "..." if len(url) > 60 else url),
            "type": typ, "source": "bio_parser"})
    for l in (api_links or []):
        if isinstance(l, dict) and l.get("url"): add(l["url"], "bio_link", l.get("title") or l["url"])
        elif isinstance(l, str) and l: add(l, "bio_link")
    if bio:
        for m in re.findall(r'(?:https?://|www\.)\S+', bio, re.I): add(m.strip('.,!?;'), "url")
        for m in re.findall(r'\b([a-zA-Z0-9.-]+\.(?:com|net|org|io|co|me|app|ai|dev|info|biz|xyz|link|to|tv)(?:/\S*)?)\b', bio, re.I):
            if not m.startswith("http") and "instagram" not in m.lower(): add(f"https://{m}", "url", m)
        for m in re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', bio): add(f"mailto:{m}", "email", m)
        for m in re.findall(r'@([A-Za-z0-9_.]+)', bio): add(f"https://www.instagram.com/{m}", "mention", f"@{m}")
        for m in re.findall(r'#([A-Za-z0-9_]+)', bio): add(f"https://www.instagram.com/explore/tags/{m}", "hashtag", f"#{m}")
    if ext: add(ext, "external_url")
    return links


# ============================================================================
# TOPSEARCH — 3 RETRIES
# ============================================================================
async def fetch_topsearch(page: Page, username: str, max_retries: int = 3) -> Optional[Dict]:
    """TopSearch with retries — proxy jitter se bachne ke liye."""
    js_code = r"""(username) => {
        return fetch(`https://www.instagram.com/web/search/topsearch/?context=user&count=0&query=${encodeURIComponent(username)}`, {
            headers: {
                'Accept': 'application/json, text/plain, */*',
                'X-Requested-With': 'XMLHttpRequest',
                'X-IG-App-ID': '936619743392459',
            },
            credentials: 'include'
        })
        .then(r => {
            if (!r.ok) return {error: 'http_' + r.status};
            return r.json();
        })
        .then(j => {
            if (j.error) return {error: j.error};
            if (!j || !j.users || !Array.isArray(j.users)) return {error: 'no_users'};
            // Exact match
            for (const item of j.users) {
                const u = item.user || item;
                if (u && u.username && u.username.toLowerCase() === username.toLowerCase()) {
                    return {user: u};
                }
            }
            // Partial — first result
            if (j.users.length > 0) {
                const u = j.users[0].user || j.users[0];
                if (u && u.username) return {user: u};
            }
            return {error: 'no_match'};
        })
        .catch(e => ({error: 'fetch_fail'}));
    }"""

    for attempt in range(max_retries):
        try:
            result = await asyncio.wait_for(
                page.evaluate(js_code, username),
                timeout=API_TIMEOUT
            )
            if result and isinstance(result, dict) and result.get("user"):
                user = result["user"]
                # Verify pk exists
                if user.get("pk") or user.get("id"):
                    return user
        except (asyncio.TimeoutError, Exception):
            pass

        if attempt < max_retries - 1:
            await page.wait_for_timeout(400)

    return None


# ============================================================================
# DEEP DOM EXTRACTION — v25
# ============================================================================
async def extract_dom_deep(page: Page) -> Dict:
    """Deep DOM extraction — multiple strategies, prioritizes reliability."""
    try:
        return await asyncio.wait_for(
            page.evaluate(r"""() => {
                const result = {
                    username: null, full_name: null, biography: null,
                    followers: null, following: null, posts: null,
                    is_verified: false, is_private: false,
                    profile_pic: null, external_url: null,
                    category_name: null, is_business: false,
                    bio_links: [],
                    meta_id: null, meta_desc: null, og_image: null,
                    title_text: null, has_header: false,
                };

                // ============ 1. TITLE TAG (most reliable) ============
                const titleTag = document.querySelector('title');
                if (titleTag) {
                    const t = titleTag.textContent || '';
                    result.title_text = t;
                    // "Instagram (@instagram) • Instagram photos and videos"
                    const mUser = /\(@([^)]+)\)/.exec(t);
                    if (mUser) result.username = mUser[1].trim();
                    const mName = /^(.+?)\s*\(@/.exec(t);
                    if (mName && mName[1].trim()) result.full_name = mName[1].trim();
                    // "No bio available" check
                }

                // ============ 2. META TAGS ============
                const getMeta = (prop) => {
                    const el = document.querySelector(`meta[property="${prop}"], meta[name="${prop}"]`);
                    return el ? (el.getAttribute('content') || '').trim() : null;
                };
                result.og_image = getMeta('og:image');
                result.meta_desc = getMeta('og:description');

                // og:title fallback
                const og_title = getMeta('og:title');
                if (og_title && !result.username) {
                    const m = /\(@([^)]+)\)/.exec(og_title);
                    if (m) result.username = m[1].trim();
                    const m2 = /^(.+?)\s*\(@/.exec(og_title);
                    if (m2 && m2[1].trim() && !result.full_name) result.full_name = m2[1].trim();
                }

                // og:url fallback for username
                const og_url = getMeta('og:url');
                if (og_url && !result.username) {
                    const m = /instagram\.com\/([^\/\?]+)/.exec(og_url);
                    if (m) result.username = m[1];
                }

                // ============ 3. COUNTS from og:description ============
                const desc = result.meta_desc || '';
                const fM = /([\d.,KMB]+)\s+Followers?/i.exec(desc);
                const gM = /([\d.,KMB]+)\s+Following/i.exec(desc);
                const pM = /([\d.,KMB]+)\s+Posts?/i.exec(desc);
                if (fM) result.followers = fM[1];
                if (gM) result.following = gM[1];
                if (pM) result.posts = pM[1];

                // Full name from og:description: "from Instagram (@instagram)"
                if (!result.full_name) {
                    const mName = /from\s+(.+?)\s*\(@/i.exec(desc);
                    if (mName && mName[1].trim()) result.full_name = mName[1].trim();
                }

                // ============ 4. META ID ============
                const ID_KEYS = ['instapp:owner_user_id', 'instapp:user_id',
                                 'profile:user_id', 'owner_user_id', 'user_id',
                                 'og:user:id', 'al:android:url', 'al:ios:url'];
                for (const m of document.querySelectorAll('meta')) {
                    const p = (m.getAttribute('property') || m.getAttribute('name') || '').toLowerCase();
                    const c = (m.getAttribute('content') || '').trim();
                    if (!c) continue;
                    for (const k of ID_KEYS) {
                        if (p === k || p.includes(k)) {
                            if (/^\d{4,}$/.test(c)) { result.meta_id = c; break; }
                            const m2 = /[?&]user[_-]?id=(\d{4,})/i.exec(c) || /[?&]id=(\d{4,})/i.exec(c);
                            if (m2) { result.meta_id = m2[1]; break; }
                        }
                    }
                    if (result.meta_id) break;
                }

                // ============ 5. HEADER (main container) ============
                let header = document.querySelector('main header');
                if (!header) header = document.querySelector('header[role="banner"]');
                if (!header) {
                    for (const h of document.querySelectorAll('header, main > div')) {
                        if (/follower|post/i.test(h.innerText || '')) { header = h; break; }
                    }
                }

                if (header) {
                    result.has_header = true;
                    const txt = (el) => el ? (el.innerText || '').trim() : null;
                    const BAD = /^(close friends|following|followers|suggested|notifications|messages|search|home|reels|explore|profile|instagram|verified|official|follow|message|edit profile|subscribe|see translation|more|contact|follow back|following)$/i;
                    const CNT = /^[\d,.\s]+(followers|following|posts|k|m|b)?$/i;
                    const NUM = /^[\d,.\s]+$/;
                    const LET = /[A-Za-z\u0900-\u097F]/;

                    // Username from h2/h1
                    const uel = header.querySelector('h2, h1');
                    const hUser = txt(uel);
                    if (hUser && !result.username) {
                        result.username = hUser.replace(/^@/, '').trim();
                    }

                    // Full name — biggest font weight
                    const names = [];
                    for (const s of header.querySelectorAll('span[dir="auto"], span')) {
                        const t = txt(s);
                        if (!t || t.length > 100 || t.length < 1) continue;
                        if (t === result.username || BAD.test(t) || t.startsWith('@')) continue;
                        if (NUM.test(t) || CNT.test(t) || !LET.test(t)) continue;
                        const st = getComputedStyle(s);
                        const w = parseInt(st.fontWeight) || 400;
                        const sz = parseFloat(st.fontSize) || 0;
                        if (w >= 600 && sz >= 14) names.push({ text: t, score: w * sz, len: t.length });
                    }
                    if (names.length) {
                        names.sort((a, b) => b.score - a.score || b.len - a.len);
                        result.full_name = names[0].text;
                    }

                    // Verified badge
                    for (const svg of header.querySelectorAll('svg')) {
                        const label = svg.getAttribute('aria-label') || (svg.querySelector('title')?.textContent || '');
                        if (/verified/i.test(label)) { result.is_verified = true; break; }
                    }

                    // Counts from <ul><li>
                    const ul = header.querySelector('ul');
                    if (ul) {
                        for (const li of ul.querySelectorAll('li')) {
                            const t = (li.innerText || '').trim();
                            const tl = t.toLowerCase();
                            const m = /([\d.,KMB]+)/.exec(t);
                            if (!m) continue;
                            if (tl.includes('follower') && !result.followers) result.followers = m[1];
                            else if (tl.includes('following') && !result.following) result.following = m[1];
                            else if (tl.includes('post') && !result.posts) result.posts = m[1];
                        }
                    }

                    // Private
                    if (/this account is private|is private/i.test(header.innerText || '')) {
                        result.is_private = true;
                    }

                    // Bio — MULTI-LINE extraction, priority to container with newlines
                    const bios = [];
                    for (const s of header.querySelectorAll('section span[dir="auto"], section div[dir="auto"], section > div, h1 + div, h2 + div')) {
                        const t = txt(s);
                        if (!t || t.length < 5 || t.length > 800) continue;
                        if (t === result.full_name || t === result.username) continue;
                        if (BAD.test(t)) continue;
                        if (CNT.test(t) || NUM.test(t)) continue;
                        if (!LET.test(t)) continue;
                        if (/^[\d.,KMB]+\s+(followers|following|posts)/i.test(t)) continue;
                        if (/^(https?:\/\/|www\.)/i.test(t) && t.length < 40) continue;
                        bios.push(t);
                    }
                    if (bios.length) {
                        // Prefer bio with newlines (multi-line bios)
                        bios.sort((a, b) => {
                            const aNl = a.split('\n').length;
                            const bNl = b.split('\n').length;
                            if (aNl !== bNl) return bNl - aNl;
                            return b.length - a.length;
                        });
                        result.biography = bios[0];
                    }

                    // If bio still not found, get header text after username line
                    if (!result.biography) {
                        const ht = header.innerText || '';
                        const lines = ht.split('\n').map(l => l.trim()).filter(l => l.length > 0);
                        for (let i = 0; i < lines.length; i++) {
                            const l = lines[i];
                            if (l.length < 5 || l.length > 500) continue;
                            if (BAD.test(l) || CNT.test(l) || NUM.test(l)) continue;
                            if (l === result.full_name || l === result.username) continue;
                            if (!LET.test(l)) continue;
                            if (/^[\d.,KMB]+\s+(followers|following|posts)/i.test(l)) continue;
                            result.biography = l;
                            break;
                        }
                    }

                    // Profile pic
                    const img = header.querySelector('img');
                    if (img && img.src) result.profile_pic = img.src;

                    // External link
                    for (const a of header.querySelectorAll('a[href]')) {
                        const href = a.href || '';
                        if (href.includes('l.instagram.com') || href.includes('/link/')) {
                            // Instagram link redirect
                            try {
                                const u = new URL(href);
                                const realUrl = u.searchParams.get('u');
                                if (realUrl) { result.external_url = decodeURIComponent(realUrl); break; }
                            } catch(e) {}
                        }
                        if (href.startsWith('http') && !href.includes('instagram.com') && !href.includes('facebook.com') && !href.includes('meta.com')) {
                            result.external_url = href;
                            break;
                        }
                    }

                    // Business category
                    const catPattern = /^[A-Z][a-z]+(\s+[A-Z][a-z]+)*$/;
                    for (const s of header.querySelectorAll('span, div')) {
                        const t = txt(s);
                        if (!t || t.length > 60 || t.length < 3) continue;
                        if (s.children.length > 0) continue;
                        if (catPattern.test(t) && !BAD.test(t) && t !== result.full_name && t !== result.username) {
                            // Could be a category — only if within reasonable bounds
                            // Skip if it's likely a name
                            if (t.length < 30) {
                                result.category_name = t;
                                break;
                            }
                        }
                    }
                }

                // ============ 6. JSON-LD ============
                for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
                    try {
                        const j = JSON.parse(s.textContent || '');
                        if (j && (j.name || j.alternateName)) {
                            if (!result.full_name && j.name) result.full_name = j.name;
                            if (!result.username && j.alternateName) result.username = String(j.alternateName).replace(/^@/, '');
                            if (j.description && !result.biography) result.biography = j.description;
                            if (j.image && !result.profile_pic) {
                                result.profile_pic = typeof j.image === 'string' ? j.image : (j.image.url || null);
                            }
                            if (j.interactionStatistic) {
                                for (const stat of j.interactionStatistic) {
                                    const name = ((stat.interactionType && stat.interactionType['@type']) || stat.interactionType || '').toString().toLowerCase();
                                    if (name.includes('follow') && stat.userInteractionCount && !result.followers) {
                                        result.followers = String(stat.userInteractionCount);
                                    }
                                }
                            }
                        }
                    } catch(e) {}
                }

                return result;
            }"""),
            timeout=6.0
        )
    except (asyncio.TimeoutError, Exception):
        return {}


# ============================================================================
# PAGE HTML ID FALLBACK
# ============================================================================
async def extract_id_from_html(page: Page, username: str) -> Optional[str]:
    """Regex scan page HTML for user_id — backup if topsearch fails."""
    try:
        html = await asyncio.wait_for(page.content(), timeout=5.0)
        patterns = [
            rf'"user_id":"(\d{{5,}})"',
            rf'"user_id":\s*(\d{{5,}})',
            rf'"pk":"(\d{{5,}})"',
            rf'"id":"(\d{{5,}})"[^}}]{{0,80}}"username":"{re.escape(username.lower())}"',
            rf'"username":"{re.escape(username.lower())}"[^}}]{{0,80}}"id":"(\d{{5,}})"',
            rf'"profile_id":"(\d{{5,}})"',
        ]
        for p in patterns:
            m = re.search(p, html, re.I)
            if m:
                uid = m.group(1)
                if uid.isdigit() and len(uid) >= 5 and int(uid) > 1000:
                    return uid
    except Exception:
        pass
    return None


# ============================================================================
# PLAYWRIGHT SCRAPE — v25
# ============================================================================
async def scrape_with_playwright(username, cookies):
    url = profile_url(username)
    out = {
        "user": None, "dom_data": None, "final_url": url,
        "error": None, "source": None,
        "logged_in": None, "cookie_inject_ok": False, "login_wall": False,
        "popups_dismissed": 0, "duration_ms": 0, "proxy_used": "direct",
        "topsearch_ok": False, "dom_ok": False, "html_id": None,
        "header_waited": False, "topsearch_attempts": 0,
    }

    t_start = time.time()

    proxy = PROXY_POOL.next()
    out["proxy_used"] = proxy["server"] if proxy else "direct"

    fp = Fingerprint()

    async with BROWSER_SEM:
        browser = await BROWSER_POOL.acquire_browser()

        ctx_kwargs = dict(
            user_agent=fp.ua,
            viewport=fp.viewport(),
            screen=fp.screen_dict(),
            locale=fp.locale,
            timezone_id=fp.timezone,
            device_scale_factor=fp.scale,
            extra_http_headers={"Cookie": cookies_header(cookies)},
            color_scheme=random.choice(["light", "dark"]),
            has_touch=False,
            is_mobile=False,
            java_script_enabled=True,
            bypass_csp=True,
            ignore_https_errors=True,
        )
        if proxy:
            ctx_kwargs["proxy"] = proxy

        ctx: BrowserContext = await browser.new_context(**ctx_kwargs)

        try:
            await ctx.add_init_script(fp.stealth_script())
            await ctx.add_init_script(cookie_init_script(cookies))
            await ctx.route("**/*", route_filter)

            page: Page = await ctx.new_page()

            try:
                # STEP 1: Navigate DIRECTLY to profile (skip homepage — save time)
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                except PlaywrightTimeout:
                    out["error"] = "profile_goto_timeout"

                # STEP 2: Inject cookies (now domain is set)
                if cookies:
                    try:
                        await ctx.add_cookies(cookies_pw(cookies))
                        out["cookie_inject_ok"] = True
                    except Exception:
                        out["cookie_inject_ok"] = False

                # STEP 3: Session check via context
                await page.wait_for_timeout(200)
                ds_present = await has_session_cookies(ctx)
                out["logged_in"] = ds_present

                if not ds_present:
                    out["login_wall"] = True
                    out["error"] = "LOGIN_WALL"
                    out["final_url"] = page.url
                    out["duration_ms"] = round((time.time() - t_start) * 1000, 2)
                    return out

                # STEP 4: Wait for header render (critical for bio/verified)
                try:
                    await page.wait_for_selector("main header, header[role='banner'], main > div > header", timeout=HEADER_WAIT)
                    out["header_waited"] = True
                except PlaywrightTimeout:
                    out["header_waited"] = False
                await page.wait_for_timeout(800)

                out["popups_dismissed"] = await dismiss_popups(page)

                # STEP 5: TopSearch with retries
                ts_user = await fetch_topsearch(page, username, max_retries=3)
                out["topsearch_attempts"] = 3 if not ts_user else (1 if ts_user else 3)
                if ts_user:
                    out["user"] = ts_user
                    out["topsearch_ok"] = True
                    out["source"] = "topsearch"

                # STEP 6: Deep DOM extraction
                dom = await extract_dom_deep(page)
                if dom and isinstance(dom, dict):
                    out["dom_data"] = dom
                    out["dom_ok"] = bool(dom.get("has_header") or dom.get("biography") or dom.get("followers"))

                # STEP 7: If ID still missing, scan HTML
                ts_id = None
                if ts_user:
                    ts_id = ts_user.get("pk") or ts_user.get("id")
                dom_id = dom.get("meta_id") if dom else None

                if not ts_id and not dom_id:
                    html_id = await extract_id_from_html(page, username)
                    if html_id:
                        out["html_id"] = html_id

                out["final_url"] = page.url

            except PlaywrightTimeout:
                out["error"] = out["error"] or "timeout"
            except Exception as e:
                out["error"] = out["error"] or str(e)[:150]
            finally:
                try: await page.close()
                except: pass
        finally:
            try: await ctx.close()
            except: pass
            if GC_AGGRESSIVE:
                gc.collect()

    out["duration_ms"] = round((time.time() - t_start) * 1000, 2)
    return out


# ============================================================================
# BUILD FINAL
# ============================================================================
def build_final(username, pw_out):
    profile = {
        "id": None, "username": username, "full_name": "N/A",
        "biography": "No bio available",
        "is_private": False, "is_verified": False,
        "is_business_account": False, "is_professional_account": False,
        "category_name": None, "business_category_name": None,
        "profile_pic_url": None, "profile_pic_url_hd": None,
        "external_url": profile_url(username),
        "followers": 0, "following": 0, "posts": 0,
        "account_creation_year": None, "has_highlights": False,
        "is_joined_recently": False, "bio_links": [],
    }
    sources = []

    # ---- 1. TOPSEARCH (highest priority for id/username/full_name/verified/private/pic) ----
    ts = pw_out.get("user") or {}
    if ts:
        sources.append("topsearch")
        uid = ts.get("pk") or ts.get("id")
        if uid:
            try: profile["id"] = str(int(str(uid).strip()))
            except Exception: profile["id"] = str(uid).strip()
        if ts.get("username"): profile["username"] = str(ts["username"]).strip()
        if ts.get("full_name"):
            fn = str(ts["full_name"]).strip()
            if fn and not re.fullmatch(r"[\d,\.\s]+", fn):
                profile["full_name"] = fn
        if ts.get("profile_pic_url"):
            pic = upgrade_hd(str(ts["profile_pic_url"]))
            profile["profile_pic_url"] = pic
            profile["profile_pic_url_hd"] = pic
        if ts.get("is_verified") is not None:
            profile["is_verified"] = bool(ts["is_verified"])
        if ts.get("is_private") is not None:
            profile["is_private"] = bool(ts["is_private"])

    # ---- 2. HTML ID fallback ----
    if not profile["id"] and pw_out.get("html_id"):
        profile["id"] = str(pw_out["html_id"])

    # ---- 3. DOM data ----
    dom = pw_out.get("dom_data") or {}
    if dom:
        sources.append("dom")

        # ID fallback
        if not profile["id"] and dom.get("meta_id"):
            profile["id"] = str(dom["meta_id"])

        # Username fallback
        if not profile["username"] or profile["username"] == username or profile["username"] == "N/A":
            if dom.get("username"):
                profile["username"] = str(dom["username"]).lstrip("@").strip()

        # Full name fallback
        if profile["full_name"] in (None, "", "N/A"):
            if dom.get("full_name"):
                fn = str(dom["full_name"]).strip()
                if fn and not re.fullmatch(r"[\d,\.\s]+", fn):
                    profile["full_name"] = fn

        # Bio — high priority
        if dom.get("biography"):
            bio = str(dom["biography"]).strip()
            if bio and len(bio) > 2 and bio != "No bio available":
                profile["biography"] = bio[:300]

        # Counts
        for key in ("followers", "following", "posts"):
            if profile.get(key, 0) == 0:
                val = dom.get(key)
                if val:
                    parsed = parse_count(val)
                    if parsed is not None and parsed > 0:
                        profile[key] = parsed

        # Verified
        if not profile["is_verified"] and dom.get("is_verified"):
            profile["is_verified"] = True

        # Private
        if dom.get("is_private"):
            profile["is_private"] = True

        # Pic fallback
        if not profile["profile_pic_url"]:
            pic = dom.get("profile_pic") or dom.get("og_image")
            if pic:
                hd = upgrade_hd(pic)
                profile["profile_pic_url"] = hd
                profile["profile_pic_url_hd"] = hd

        # External URL
        if dom.get("external_url"):
            ext = str(dom["external_url"]).strip()
            if ext and not ext.startswith("https://www.instagram.com/" + username):
                profile["external_url"] = ext

        # Category
        if dom.get("category_name"):
            profile["category_name"] = str(dom["category_name"]).strip()[:80]

        if dom.get("is_business"):
            profile["is_business_account"] = True

        # Bio links from DOM
        if isinstance(dom.get("bio_links"), list):
            for link in dom["bio_links"]:
                if isinstance(link, str) and link:
                    profile["bio_links"].append({
                        "url": link, "display_text": link[:60],
                        "type": "bio_link", "source": "dom",
                    })

    # ---- 4. Post-processing ----
    if profile.get("id") and not profile.get("account_creation_year"):
        try:
            n = int(profile["id"])
            profile["account_creation_year"] = year_from_id(n)
            profile["is_joined_recently"] = (year_from_id(n) or 0) >= 2024
        except Exception: pass

    # Ensure external_url is correct
    final_uname = profile.get("username") or username
    profile["external_url"] = profile["external_url"] if profile["external_url"] != profile_url(username) else profile_url(final_uname)

    # Pic upgrade
    pic = profile.get("profile_pic_url_hd") or profile.get("profile_pic_url")
    if pic:
        pic = upgrade_hd(pic)
        profile["profile_pic_url"] = pic
        profile["profile_pic_url_hd"] = pic

    # Full name sanity
    if profile.get("full_name") and re.fullmatch(r"[\d,\.\s]+", str(profile["full_name"])):
        profile["full_name"] = "N/A"

    # Bio links — dedupe
    if not isinstance(profile.get("bio_links"), list):
        profile["bio_links"] = []
    seen_urls = set()
    unique_links = []
    for l in profile["bio_links"]:
        if isinstance(l, dict):
            u = l.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                unique_links.append(l)
    profile["bio_links"] = unique_links

    # Add external_url to bio_links if not present
    if profile.get("external_url"):
        ext = profile["external_url"]
        if ext not in seen_urls and not ext.startswith("https://www.instagram.com/" + final_uname):
            profile["bio_links"].insert(0, {
                "url": ext, "display_text": ext[:60],
                "type": "external_url", "source": "dom",
            })

    # Ensure all keys
    keys = ["id","username","full_name","biography","is_private","is_verified",
            "is_business_account","is_professional_account","category_name",
            "business_category_name","profile_pic_url","profile_pic_url_hd",
            "external_url","followers","following","posts",
            "account_creation_year","has_highlights","is_joined_recently","bio_links"]
    for k in keys:
        profile.setdefault(k, None)
    for k in ("followers","following","posts"):
        if profile.get(k) is None: profile[k] = 0

    source_str = "+".join(sources) if sources else "failed"

    return {
        "status": "success" if (profile.get("id") and profile.get("username")) else "error",
        "source": source_str,
        "final_url": profile_url(final_uname),
        "profile": profile,
    }


# ============================================================================
# FASTAPI
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[boot] proxies={PROXY_POOL.size()} cache_ttl={CACHE_TTL_MIN}m "
          f"browsers={MAX_CONCURRENT_BROWSERS} page={PAGE_TIMEOUT}ms api={int(API_TIMEOUT*1000)}ms")
    try:
        await BROWSER_POOL.start()
    except Exception as e:
        print(f"[boot] browser warmup failed: {e}")
    yield
    await BROWSER_POOL.shutdown()
    gc.collect()


app = FastAPI(title="IG Profile API", version="25.0", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)


def _public_profile(profile: Dict) -> Dict:
    return {
        "id": profile.get("id"),
        "username": profile.get("username"),
        "full_name": profile.get("full_name"),
        "biography": profile.get("biography"),
        "is_private": profile.get("is_private", False),
        "is_verified": profile.get("is_verified", False),
        "is_business_account": profile.get("is_business_account", False),
        "is_professional_account": profile.get("is_professional_account", False),
        "category_name": profile.get("category_name"),
        "business_category_name": profile.get("business_category_name"),
        "profile_pic_url": profile.get("profile_pic_url"),
        "profile_pic_url_hd": profile.get("profile_pic_url_hd"),
        "external_url": profile.get("external_url"),
        "followers": profile.get("followers", 0),
        "following": profile.get("following", 0),
        "posts": profile.get("posts", 0),
        "account_creation_year": profile.get("account_creation_year"),
        "has_highlights": profile.get("has_highlights", False),
        "is_joined_recently": profile.get("is_joined_recently", False),
        "bio_links": profile.get("bio_links", []),
    }


@app.get("/")
async def home():
    return {
        "service": "Instagram Info API",
        "version": "25.0.0",
        "endpoints": {
            "profile": "/api/profile/key={api_key}/username={username}",
            "health": "/health/key={health_key}",
            "cache_stats": "/cache/stats/key={api_key}",
            "cache_clear": "/cache/clear/key={api_key}",
        },
        "credit": {"username": "@KINGFFAIAK47x", "made_by": "ANSH AFT"}
    }


@app.get("/health/key={api_key}")
async def health_check(api_key: str):
    if api_key != HEALTH_KEY:
        raise HTTPException(status_code=401, detail="Invalid health key")

    t0 = time.time()
    cookie_info = cookie_expiry_info(COOKIE_FILE)
    cookies = load_cookies(COOKIE_FILE)

    critical = ["sessionid", "ds_user_id", "csrftoken", "mid", "ig_did"]
    critical_status = {}
    for c in critical:
        if c in cookies:
            info = cookie_info["cookies"].get(c, {})
            critical_status[c] = {"present": True, "status": info.get("status", "unknown"),
                                  "days_left": info.get("days_left"), "expires_at": info.get("expires_at")}
        else:
            critical_status[c] = {"present": False, "status": "MISSING"}

    cache_stats = CACHE.stats()
    browser_stats = BROWSER_POOL.stats()
    proxy_stats = PROXY_POOL.stats()
    uptime = time.time() - SERVER_START_TIME

    memory_info = {}
    try:
        import psutil
        proc = psutil.Process()
        mem = proc.memory_info()
        memory_info = {"rss_mb": round(mem.rss / (1024 * 1024), 2),
                       "vms_mb": round(mem.vms / (1024 * 1024), 2),
                       "percent": round(proc.memory_percent(), 2)}
    except ImportError:
        memory_info = {"note": "install psutil"}

    live_test = {"tested": False}
    try:
        test_out = await scrape_with_playwright("instagram", cookies)
        ts = test_out.get("user") or {}
        dom = test_out.get("dom_data") or {}

        live_test = {
            "tested": True,
            "login_detected": test_out.get("logged_in"),
            "cookie_inject_ok": test_out.get("cookie_inject_ok"),
            "login_wall": test_out.get("login_wall"),
            "header_waited": test_out.get("header_waited"),
            "topsearch_ok": test_out.get("topsearch_ok", False),
            "dom_ok": test_out.get("dom_ok", False),
            "html_id_fallback": test_out.get("html_id"),
            "final_source": test_out.get("source"),

            "topsearch_fields": {
                "id": ts.get("pk") or ts.get("id"),
                "username": ts.get("username"),
                "full_name": ts.get("full_name"),
                "is_verified": ts.get("is_verified"),
                "is_private": ts.get("is_private"),
                "has_pic": bool(ts.get("profile_pic_url")),
            } if ts else None,

            "dom_fields": {
                "has_header": dom.get("has_header"),
                "username": dom.get("username"),
                "full_name": dom.get("full_name"),
                "biography": (dom.get("biography") or "")[:150] if dom.get("biography") else None,
                "followers": dom.get("followers"),
                "following": dom.get("following"),
                "posts": dom.get("posts"),
                "is_verified": dom.get("is_verified"),
                "is_private": dom.get("is_private"),
                "has_pic": bool(dom.get("profile_pic") or dom.get("og_image")),
                "meta_id": dom.get("meta_id"),
                "external_url": dom.get("external_url"),
                "category": dom.get("category_name"),
                "title_text": (dom.get("title_text") or "")[:100] if dom.get("title_text") else None,
                "og_desc": (dom.get("meta_desc") or "")[:150] if dom.get("meta_desc") else None,
            } if dom else None,

            "popups_dismissed": test_out.get("popups_dismissed"),
            "duration_ms": test_out.get("duration_ms"),
            "proxy_used": test_out.get("proxy_used"),
            "error": test_out.get("error"),
        }
    except Exception as e:
        live_test = {"tested": True, "error": str(e)[:200]}

    elapsed = round(time.time() - t0, 3)

    return {
        "status": "healthy",
        "response_time": f"{elapsed}s",
        "server": {
            "version": "25.0.0",
            "uptime_seconds": round(uptime, 2),
            "uptime_human": str(timedelta(seconds=int(uptime))),
            "python_version": sys.version.split()[0],
            "started_at": datetime.fromtimestamp(SERVER_START_TIME).isoformat(),
            "page_timeout_ms": PAGE_TIMEOUT,
            "api_timeout_ms": int(API_TIMEOUT * 1000),
            "header_wait_ms": HEADER_WAIT,
        },
        "metrics": REQUEST_COUNTER,
        "last_scrape": LAST_SCRAPE,
        "memory": memory_info,
        "cookies": {
            "file_path": cookie_info["file_path"],
            "file_exists": cookie_info["file_exists"],
            "file_mtime": cookie_info.get("file_mtime"),
            "file_size": cookie_info.get("file_size"),
            "total_cookies": len(cookie_info["cookies"]),
            "critical_cookies": critical_status,
            "all_cookies": cookie_info["cookies"],
        },
        "cache": cache_stats,
        "browser_pool": browser_stats,
        "proxy_pool": proxy_stats,
        "live_test": live_test,
        "credit": {"username": "@KINGFFAIAK47x", "made_by": "ANSH AFT"}
    }


@app.get("/cache/stats/key={api_key}")
async def cache_stats_endpoint(api_key: str):
    if api_key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return {"status": "success", "cache": CACHE.stats()}


@app.get("/cache/clear/key={api_key}")
async def cache_clear_endpoint(api_key: str):
    if api_key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    CACHE.clear()
    gc.collect()
    return {"status": "cleared"}


@app.get("/api/profile/key={api_key}/username={username}")
async def get_profile(api_key: str, username: str):
    t0 = time.time()
    REQUEST_COUNTER["total"] += 1

    if api_key not in VALID_KEYS:
        REQUEST_COUNTER["failed"] += 1
        raise HTTPException(status_code=401, detail="Invalid API key")

    username = username.strip().lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9._]+", username):
        REQUEST_COUNTER["failed"] += 1
        raise HTTPException(status_code=400, detail="invalid username")

    cached = CACHE.get(username)
    if cached:
        profile = cached.get("profile") or {}
        elapsed = round(time.time() - t0, 3)
        REQUEST_COUNTER["success"] += 1
        REQUEST_COUNTER["cached"] += 1
        return {
            "status": "success",
            "response_time": f"{elapsed}s",
            "username": profile.get("username", username),
            "final_url": cached.get("final_url", f"https://www.instagram.com/{username}/?hl=en"),
            "profile": _public_profile(profile),
            "credit": {"username": "@KINGFFAIAK47x", "made_by": "ANSH AFT"}
        }

    cookies = load_cookies(COOKIE_FILE)
    try:
        pw_out = await scrape_with_playwright(username, cookies)
    except Exception as e:
        REQUEST_COUNTER["failed"] += 1
        raise HTTPException(status_code=500, detail=f"scrape failed: {str(e)[:200]}")

    final = build_final(username, pw_out)
    profile = final.get("profile") or {}

    if profile.get("id") and profile.get("username"):
        CACHE.set(username, final)

    LAST_SCRAPE["time"] = datetime.now().isoformat()
    LAST_SCRAPE["username"] = username
    LAST_SCRAPE["duration"] = pw_out.get("duration_ms")
    LAST_SCRAPE["source"] = final.get("source")

    elapsed = round(time.time() - t0, 3)

    if profile.get("id"):
        REQUEST_COUNTER["success"] += 1
    else:
        REQUEST_COUNTER["failed"] += 1

    return {
        "status": "success" if profile.get("id") else "error",
        "response_time": f"{elapsed}s",
        "username": profile.get("username", username),
        "final_url": final.get("final_url"),
        "profile": _public_profile(profile),
        "credit": {"username": "@KINGFFAIAK47x", "made_by": "ANSH AFT"}
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="warning")
