'use strict';
// SPDX-License-Identifier: GPL-3.0-or-later
// jap-ridetube-d daemon (RideTube): plays a video on the MGU. Audio goes through BMW's
// MediaPlayer over SOME/IP, video through
// ytdec, paced by the MediaPlayer clock. The app talks to it on /run/jap-ridetube-d/ctl.sock (JSON
// lines; README.md has the protocol). Node 8: no ?. / ?? / catch without binding.
// 0.1.6: feed{which, items, shelves:[{title, items}]}, items carry meta (the tile's second line).
// The car fetches (ytfetch.py), searches (InnerTube) and makes thumbs (thumb.js) itself, all as the fetch uid; playback starts while the video is still being fetched.
// 0.5.0: downloads (library / download / remove, cache/saved/<id>): kept until removed, played with no network.
// It never exits on its own: a stray exception is logged, audio is released, it goes idle.
const fs = require('fs');
const path = require('path');
const net = require('net');
const https = require('https');
const crypto = require('crypto');
const cp = require('child_process');
const readline = require('readline');
const proxy = require('./proxy.js');

// this service's version (component.json's: test_ytd.js checks they agree), told to the app with the diagnostics
const VERSION = '8.0.0';   // (the iDrive 8 line: upstream's service has the same id and its own 0.x numbers)
const ID_RE = /^[A-Za-z0-9_-]{11}$/;
const isVid = (id) => typeof id === 'string' && ID_RE.test(id);   // (String(['x...']) would pass ID_RE, then break path.join)
// the audio route on iDrive 7 (measured on the car); state/audio.json overrides them (see Audio.init)
const SOURCE_ID = 1046, SINK_ID = 1024, MEDIA_INSTANCE = 'OapGenericMedia';
// where MainApp keeps the SOME/IP interface descriptions: iDrive 7 (oap-mainapp), iDrive 8 (oap-mainapp21)
const SRC_GENS = ['/usr/lib/oap-mainapp/src-gen', '/usr/lib/oap-mainapp/src-gen/someip',
    '/usr/lib/oap-mainapp21/src-gen/someip', '/usr/lib/oap-mainapp21/src-gen'];
const MEDIA_FILES = ['audio.m4a', 'index.bin', 'video.h264'];
// a video's folder: BMW's media engine (its own uid, no capabilities) opens audio.m4a in it, so it must be able to
// pass through; no read bit, so nobody lists it (and the cache above is 0711: the video ids aren't listed either)
const VIDEO_DIR_MODE = 0o711;
// thumb.rgba length -> [w, h]: 160x90 in caches from older proxies, 320x180 since 0.1.6
const THUMB_SIZES = { 57600: [160, 90], 230400: [320, 180] };
// the day this build was released (UTC), bumped with VERSION at each release (tools/release.py refuses one in the
// future). A head unit clock before it is provably wrong: only then does TLS skip the certificate dates (certTime)
const BUILT = '2026-11-26';
// what `hello` says this build answers for real. The frozen names: health, related, hist (feed hist|liked), ytdlp,
// remote, proxy, stream, download (library, download, remove, auto_download). A package adds its name here (through the integrator) once its command works
const CAPS = ['related', 'hist', 'health', 'remote', 'ytdlp', 'download', 'proxy', 'loop'];
const LANG_RE = /^[a-z]{2}$/, REGION_RE = /^[A-Z]{2}$/;   // hello's lang ("ru") and region ("RU")
// the log relay (send_logs posts <REPORT_URL>/v1/report): the only place reports go. MGU_YT_REPORT_URL or opts.reportUrl override it
const REPORT_URL = 'https://ridetube-logs.ridetube-relay.workers.dev';
const RING_MAX = 1500, REPORT_MAX = 200 * 1024, OS_MAX = 2048; // log lines kept for a report; its log and os caps (bytes)

function defaults() {
    const env = process.env;
    const here = __dirname;
    return {
        someip: env.MGU_YT_SOMEIP || '/usr/lib/someip.node',
        srcGen: env.MGU_YT_SRCGEN || null,     // one folder (tests); null: the first of srcGens that has the file
        srcGens: SRC_GENS,
        audioConf: env.MGU_YT_AUDIO || '/var/opt/jap-ridetube-d/state/audio.json',
        ytdec: env.MGU_YT_YTDEC || path.join(here, 'ytdec'),
        cacheDir: env.MGU_YT_CACHE || '/var/opt/jap-ridetube-d/cache',
        logDir: env.MGU_YT_LOGDIR || '/var/opt/jap-ridetube-d/log',
        sock: env.MGU_YT_SOCK || '/run/jap-ridetube-d/ctl.sock',
        sockGid: 12202, // tm_oap_app
        accountFile: env.MGU_YT_ACCOUNT || '/var/opt/jap-ridetube-d/state/account.json',
        reportUrl: env.MGU_YT_REPORT_URL || REPORT_URL,
        reports: false,      // send_logs sends nothing off the car (BeeMTube2)
        reportTimeoutMs: 30000,
        ytRequest: null,     // null: each request in a child as the fetch uid (fetchRequest)
        // the fetch uid's pick (pickFetchUid) and the helper it runs
        stateDir: env.MGU_YT_STATE || '/var/opt/jap-ridetube-d/state',
        etcDir: '/etc',
        oapRoot: '/var/opt/public/oap',
        mgrUid: 12201,       // MainApp's uid: its registry must be owned by it (or root)
        toolRoot: '/var/opt',
        procDir: '/proc',
        mainapps: MAINAPPS,
        fetchCmd: [process.execPath, '--max-http-header-size=65536', __filename, '--fetch'],
        fetcher: ['/var/opt/mgu-python/current/bin/python3', '-I', path.join(here, 'ytfetch.py')],
        thumbCmd: ['/var/opt/jap-node22/current/bin/node', path.join(here, 'thumb.js')],
        startVideoMs: 6000,  // video fetched ahead before playback starts (later the buffering pause covers a slow link)
        fetchRetryMs: [3000, 8000], // a fetch failing before playback (the network): tried again after these waits
        h264Only: env.MGU_YT_H264 === '1', // internal escape hatch: never VP9 (ytfetch.py --avc1)
        spare: true,         // keep a ytfetch.py serve (yt-dlp loaded) waiting while the app is in use
        warmMs: 600000,      // ... for this long after the app's last command
        infoMaxMs: 1500000,
        leaseFile: '/run/mgu-uplink/lease.json',
        storeConf: proxy.STORE_CONF,   // the MGU Store's settings: its `proxy` line carries RideTube's traffic too (proxy.js)
        ytdlpFirstMs: 120000,      // yt-dlp updates (ytfetch.py update): looked for this long after start,
        ytdlpEveryMs: 86400000,    // ... then once a day,
        ytdlpRetryMs: 3600000,     // ... an hour after a failed look, and after a failed play once this long has passed
        ytdlpMaxMs: 900000,        // ... and a look still running after this long is killed
        resume: true,        // a play starts where the video was left (positions.json)
        sponsorBlock: 'https://sponsor.ajay.app/api/skipSegments/',
        skip: ['sponsor', 'selfpromo', 'interaction', 'intro'], // SponsorBlock categories skipped (the play's own `skip` wins)
        sbCategories: ['sponsor', 'selfpromo', 'interaction', 'intro', 'outro', 'music_offtopic'], // looked up, sent in extras  // a prefetched stream lookup is used this long (ytfetch: 30 min, URLs: ~6 h)
        headMarginMs: 1500,  // on the head file: wait for audio.m4a this close to the head's end
        underrunWaitMs: 500, // the player ran dry (underrun): started again at the clock after this
        unplayableWaitMs: 2000, // ITEM_UNPLAYABLE before the first tick: the file given to the player once more after this
        thumbMax: 150,
        ytTimeoutMs: 10000,
        keep: 0,             // played videos kept in the cache besides the downloads (0: deleted once played)
        minFreeMb: 4096,
        dlStopFreeMb: 1024,  // a download under way stops below this much free (it starts again from scratch with minFreeMb)     // a download starts only with this much free where the cache is (/var/opt, shared with the Store)
        freeMb: null,        // (tests) dir -> MB free; null: df
        heldStopMs: 3000,    // paused but the player plays on: STOP again at most this often, then release the audio
        micHoldMs: 150000,   // Majordome's mic hold not released this long (it crashed): released by itself
        dlNetWaitMs: 60000,  // a download failing for the network (not counted): the queue waits this long
        dlTries: 3,          // a download failing this often (network, not the video) is given up until it is asked again
        autoEveryMs: 1800000, // auto-downloads: Subscriptions looked at this often, on Wi-Fi only
        autoMaxS: 3600,      // ... and longer videos left out
        // WP8: only a window of the playing video is kept: in RAM (ramDir, the unit's capped tmpfs), or in the video's
        // cache folder on flash when RAM is short or there is no such mount; the audio stays on flash. ytfetch.py fetches
        // the video at most streamAheadMs past playback and punches it streamBehindMs behind (both cut to ~48 MiB at the
        // format's bitrate); a seek outside the window, or playback streamJumpMs past what is fetched (a dropped link),
        // starts the video's fetch again at that point.
        ramDir: env.MGU_YT_RAM || '/run/jap-ridetube-media',
        ramCheck: true,      // ramDir must be a tmpfs mount point itself (/proc/mounts), never a folder on /run or /dev/shm; tests turn it off
        streamAheadMs: 120000,
        streamBehindMs: 45000,
        streamJumpMs: 30000,
        restreamMs: 10000,   // a catch-up restart at most this often
        ramBudget: 256 << 20, // bytes of RAM a stream may plan for (the unit's tmpfs is 256 MB)
        watchdogMs: 5000,
        tickMs: 5000,
        availWaitMs: 5000,
        stageMs: 8000,       // a proxy given this long to start, per try
        audioRetryMs: 5000,  // the audio start tried again this often until it succeeds
        audioAgainMs: 1000,  // a play's audio start (connect + playFile) failed: tried once more after this (iDrive 7)
        // the unit's interface catalog and the app manager's bundles (its generator): Audio.generate
        catalogs: ['/etc/oap/someip/someip-interfaces.json'],
        searchRoots: ['/usr/lib', '/usr/share', '/etc', '/opt', '/var/opt/public'], // the Diagnostics page's search, when a description is missing
        // Off since 0.4.4: after 0.4.3 a car's Store apps stopped showing. With probe off the service makes the two
        // proxies iDrive 7 needs and nothing else: no other player or DynamicMediaPlayer proxy, no second
        // CommandControl proxy, and the app manager's generator is not loaded for the lists.
        probe: false,
        // Since 0.4.6 (a car's Store apps went missing again on 0.4.4, at some starts): on iDrive 8 the service keeps
        // away from the car while it starts - no SOME/IP binding loaded, no proxy made - until the head unit has
        // been up settleS seconds (iDrive 7: at once, as ever). And the apps' code is no longer read on the unit
        // unless codeScan is set (tools/id8_collect.js reads it from a computer).
        settleS: fs.existsSync('/usr/lib/oap-mainapp21') ? 150 : 0,
        uptimeFile: '/proc/uptime',
        // 0.4.7: the code is read again, but only what says how the car plays for apps: the app manager's own files and
        // the three BMW apps known to play media (codeHomes) - some tens of files, not the 330 of every app - and only
        // when the Diagnostics page is opened. 'all' reads every app; false reads nothing.
        // 0.5.0: off - sdkread.js reads the player's own code instead (codeHomes: the app homes it looks at first)
        codeScan: false,
        codeHomes: ['oap_12209'],
        // 0.4.8: where the car does not offer iDrive 7's player, a play goes through its dynamic media player, asked as
        // BMW's SDK asks it (since 0.5.0: sound.js's DynRoute). false: never.
        dynamic: true,
        dynClientId: 'jap_ridetube8',
        dynHeartbeatTimer: 10000,  // getAudioFocus's timeOutHeartBeatTimer (its unit is not known: far above our beat either way)
        dynHeartbeatMs: 1500,      // heartBeat(token) this often while we hold the focus
        // 0.5.0: on such a head unit a play works through every way to sound this service knows until one plays, and
        // keeps it (sound.js): the dynamic player with BMW's own way of asking it read from the apps on the car
        // (sdkread.js) and every other likely one, then - direct - the service's own player (ytaud + ytalsa.py) on
        // the car's audio router. id8: the head unit is one (its app manager is MainApp-21).
        id8: fs.existsSync('/usr/lib/oap-mainapp21'),
        version: VERSION,
        direct: true,
        ytaud: env.MGU_YT_YTAUD || path.join(here, 'ytaud'),
        alsaCmd: ['/var/opt/mgu-python/current/bin/python3', '-I', path.join(here, 'ytalsa.py')],
        dynBudgetMs: 150000,       // the first dynamic instance's whole search for the way it plays, at most
        dynNextMs: 60000,          // ... and each further instance's
        runDir: '/run/jap-ridetube-d/media',
        diagFull: false,      // the page's sections a car has already told (the description, the policies, the processes, the files)
        playerWaitMs: 3000,   // at the start, the media player given this long to answer before another is looked for
        probeMs: 3000,        // ... and the players asked for this long
        playerAfterMs: 4000,  // at a play, a player that does not answer given this long once its source is connected
        policyDirs: ['/etc/vsomeip'], // the unit's SOME/IP security policies (the Diagnostics page: who may ask for the player)
        codeScanDelayMs: 45000, // how long after its start the service reads the installed apps' code, when the player is not offered
        generators: MAINAPPS.reduce(function (l, m) {
            return l.concat([path.join(m.root, 'src', 'AppManagement', 'AppManagement.js'), path.join(m.root, 'src', 'AppManagement', 'AppManagement.bundle.js')]);
        }, []),
        log: function (m) { console.log(new Date().toISOString() + ' ' + m); }
    };
}

function monoMs() {
    const t = process.hrtime(); // CLOCK_MONOTONIC on Linux
    return Math.floor(t[0] * 1000 + t[1] / 1e6);
}
// the app's Video quality: 360 | 480 | 720, else auto: 720 on Wi-Fi (the uplink's wlan*) or no uplink known, 480 on phone data
function qualityHeight(q, lease) {
    if ([360, 480, 720].indexOf(Number(q)) >= 0) return Number(q);
    return lease && lease.interface && !/^wlan/.test(String(lease.interface)) ? 480 : 720;
}
// members-only (or members-first) videos: a badge somewhere in the tile, its shape unknown (no capture) - matched on the text
// shortcut: a JSON scan of the tile, swap for the badge's field once a real members tile is captured
const isMembers = (t) => /BADGE_STYLE_TYPE_MEMBERS|"Members (only|first)"/i.test(JSON.stringify(t));
// live now or upcoming (a premiere): the overlay's style (LIVE / UPCOMING) or its English text, a lockup's LIVE badge, a
// LIVE_NOW line badge; never a length
// shortcut: the values are YouTube's live enums from its other clients, no TVHTML5 live capture in test/; check on the first one
const LIVE_VALUES = ['LIVE', 'UPCOMING', 'THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE', 'BADGE_STYLE_TYPE_LIVE_NOW'];
const isLive = (marks, duration) => duration === null && marks.some((x) => LIVE_VALUES.indexOf(x) >= 0);
// \bage\b: "confirm your age", "age-restricted"; never yt-dlp's "Unable to download API page / webpage" (the network)
const PERMANENT_RE = /unavailable|private|removed|terminated|copyright|members|\bage\b|not available in your country|can't be played|can't play|head unit clock|blocking this connection/i;   // the last: ytfetch.py BOT_MSG, no retry nor yt-dlp update
// a fetch that failed for the network (the car offline), not for the video
const NET_RE = /internet uid|name resolution|resolve|getaddrinfo|EAI_AGAIN|ENOTFOUND|ENETUNREACH|EHOSTUNREACH|ECONNREFUSED|ECONNRESET|ETIMEDOUT|timed out|unreachable|No route to host|Connection refused|urlopen error|offline/i;
function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
function withTimeout(p, ms, what) {
    return new Promise(function (resolve, reject) {
        const t = setTimeout(function () { reject(new Error(what + ' timed out')); }, ms);
        p.then(function (v) { clearTimeout(t); resolve(v); }, function (e) { clearTimeout(t); reject(e); });
    });
}
// root-owned trees only: a fetch-uid folder goes through Daemon.rmTree (a symlink swapped in between lstat and
// readdir would point root elsewhere)
function rmrf(p) {
    let st;
    try { st = fs.lstatSync(p); } catch (e) { return; }
    if (st.isDirectory()) {
        fs.readdirSync(p).forEach(function (n) { rmrf(path.join(p, n)); });
        fs.rmdirSync(p);
    } else {
        fs.unlinkSync(p);
    }
}
function mkdirp(p) {
    if (fs.existsSync(p)) return;
    mkdirp(path.dirname(p));
    fs.mkdirSync(p, 0o755);
}

// ---------------------------------------------------------------- playback cache (a played video goes, a download stays)
// the downloads asked for: cache/saved/<id> (root's, 0700), a JSON marker {title, channel, duration_s, height, at, auto?,
// failed?}; the files are the video's folder as a play leaves it. Kept until removed: prune and the thumbs' cap skip them
const SAVED_DIR = 'saved';
// null when saved/ is there but cannot be read: the cleanups then delete nothing (none would be known as a download)
function savedIds(cacheDir) {
    try { return fs.readdirSync(path.join(cacheDir, SAVED_DIR)).filter((n) => ID_RE.test(n)); } catch (e) { return e.code === 'ENOENT' ? [] : null; }
}
// MB free in dir's file system (Node 8 has no statfs: df -Pk), null when it cannot be read
function freeMb(dir) {
    try {
        const l = cp.execFileSync('df', ['-Pk', dir], { encoding: 'utf8', timeout: 3000 }).trim().split('\n').pop().trim().split(/\s+/);
        const kb = Number(l[l.length - 3]);
        return kb >= 0 ? Math.floor(kb / 1024) : null;
    } catch (e) { return null; }
}
// a file in a folder the fetch uid owns, read by root: no symlink (last component), only a regular file (a FIFO would
// block root, a device feed it forever), ok(size) true; read from that fd. null on anything else (treated as missing)
function readFetchFile(f, ok) {
    let fd = null;
    try {
        fd = fs.openSync(f, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
        const st = fs.fstatSync(fd);
        if (!st.isFile() || !ok(st.size)) return null;
        const b = Buffer.alloc(st.size);
        let n = 0, r = 1;
        while (n < b.length && r > 0) { r = fs.readSync(fd, b, n, b.length - n, n); n += r; }
        return n === b.length ? b : null;
    } catch (e) {
        return null;
    } finally {
        if (fd !== null) { try { fs.closeSync(fd); } catch (e) { /* closed */ } }
    }
}
const META_MAX = 64 << 10;
function readMeta(dir, id) {
    try {
        const b = readFetchFile(path.join(dir, 'meta.json'), (n) => n <= META_MAX); // ytfetch writes it
        if (!b) throw new Error('no meta');
        const m = JSON.parse(b.toString('utf8'));
        return { id: id, title: m.title || id, channel: m.channel || '', duration_s: m.duration_s || null };
    } catch (e) { return { id: id, title: id, channel: '', duration_s: null }; }
}
// enough to start without a fetch. No video.h264.part: play() stops any fetcher first, so a .part is a
// dead fetch's leftover that would never complete (fetched again instead)
function isPlayable(dir) {
    const has = (n) => fs.existsSync(path.join(dir, n));
    return has('audio.m4a') && !has('audio.m4a.part') && has('index.bin') && has('video.h264');
}
function isReady(dir) {
    let names;
    try { names = fs.readdirSync(dir); } catch (e) { return false; }
    if (names.some(function (n) { return /\.part$/.test(n); })) return false;
    return MEDIA_FILES.every(function (n) { return names.indexOf(n) >= 0; });
}
function listCache(cacheDir) {
    let names;
    try { names = fs.readdirSync(cacheDir); } catch (e) { return []; }
    const out = [];
    names.forEach(function (n) {
        if (!ID_RE.test(n)) return;
        const dir = path.join(cacheDir, n);
        let st;
        try { st = fs.statSync(dir); } catch (e) { return; }
        if (!st.isDirectory()) return;
        const item = readMeta(dir, n);
        item.ready = isReady(dir);
        item.mtime = st.mtime.getTime();
        out.push(item);
    });
    out.sort(function (a, b) { return b.mtime - a.mtime; });
    return out;
}
// SponsorBlock answers, cache/.sb/<id>.json: root's (0700), never in a video's folder, which the fetch uid owns
const SB_DIR = '.sb';
// Keep the `keep` most recent ids (dir mtime); never the protected ones (playing/preparing) nor the saved ones (downloads,
// not counted in keep). rm: how a folder goes (the daemon's rmTree; rmrf by default). .sb/<id>.json (SponsorBlock, root's)
// goes with its video's folder.
function pruneCache(cacheDir, keep, protect, log, rm, saved) {
    const items = listCache(cacheDir);
    const removed = [];
    let kept = 0;
    items.forEach(function (it) {
        if (saved && saved.indexOf(it.id) >= 0) return;
        if (protect.indexOf(it.id) >= 0) { kept++; return; }
        if (kept < keep) { kept++; return; }
        try { (rm || rmrf)(path.join(cacheDir, it.id)); removed.push(it.id); } catch (e) { log('lru: ' + it.id + ': ' + e.message); }
    });
    if (removed.length) log('lru removed ' + removed.join(','));
    const sb = path.join(cacheDir, SB_DIR);
    list(sb).forEach(function (n) {
        const id = n.replace(/\.json$/, '');
        if (protect.indexOf(id) < 0 && !fs.existsSync(path.join(cacheDir, id))) { try { fs.unlinkSync(path.join(sb, n)); } catch (e) { /* gone */ } }
    });
    return removed;
}

// ---------------------------------------------------------------- the fetch uid
// mgu-uplink lets out only the uids it lists, never root: every youtube.com request runs in a child
// as our own "fetch" uid, which the uplink learns from uids.d/jap-ridetube-d (written here, and by the deploy
// tool from state/internet-uids). Picked as mgu-updater's js/uids.js picks its own: the lowest free
// number above MainApp's app uid ranges, kept in state/uids.json while nothing else holds it.
const SELF = 'jap-ridetube-d';
const MAX_UID = 60000; // login.defs UID_MAX
const MAINAPPS = [{ root: '/usr/lib/oap-mainapp', hmi: 'HmiMain3D' }, { root: '/usr/lib/oap-mainapp21', hmi: 'Hmi8' }];
const lines = (f) => { try { return fs.readFileSync(f, 'utf8').split('\n'); } catch (e) { return []; } };
const readJson = (f) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch (e) { return null; } };
const list = (d) => { try { return fs.readdirSync(d); } catch (e) { return []; } };
const isId = (n) => n > 0 && n === Math.floor(n);
const inRanges = (cfg, u) => !!cfg && cfg.ranges.some((r) => u >= r.min && u <= r.max);

// MainApp's app uid ranges [{min,max}] and own uids, from the config of the MainApp the unit runs; null unknown
function mainappRanges(o) {
    const have = o.mainapps.filter((m) => fs.existsSync(path.join(m.root, 'config', 'base.json')));
    let m = have.length === 1 ? have[0] : null;
    if (have.length > 1) {
        const running = list(o.procDir).filter((n) => /^\d+$/.test(n)).map((n) => (lines(path.join(o.procDir, n, 'comm'))[0] || '').trim());
        const which = have.filter((x) => running.indexOf(x.hmi) >= 0);
        m = which.length === 1 ? which[0] : null;
    }
    const c = m && readJson(path.join(m.root, 'config', 'base.json'));
    if (!c) return null;
    const rse = c.usableUidsAndGidsRse || {};
    const ranges = [c.usableUidsAndGids].concat(Object.keys(rse).map((k) => rse[k]))
        .filter((r) => r && isId(Number(r.min)) && Number(r.max) >= Number(r.min)).map((r) => ({ min: Number(r.min), max: Number(r.max) }));
    return ranges.length ? { ranges: ranges, own: (Array.isArray(c.mainAppUids) ? c.mainAppUids : []).map(Number).filter(isId) } : null;
}
// every uid and gid in use: /etc/passwd and group, OAP apps (oap_<uid> folders, MainApp's registry and
// uids.txt), other components' uplink drop-ins, mgu-updater's uids.json, MainApp's own; procs: running ones
function takenIds(o, cfg, procs) {
    const ids = {};
    const add = (v) => { const n = Number(v); if (String(v).trim() !== '' && isId(n)) ids[n] = true; };
    lines(path.join(o.etcDir, 'passwd')).forEach((l) => { const f = l.split(':'); add(f[2] || ''); add(f[3] || ''); });
    lines(path.join(o.etcDir, 'group')).forEach((l) => add(l.split(':')[2] || ''));
    list(o.oapRoot).forEach((n) => { const m = /^oap_(\d+)$/.exec(n); if (m) add(m[1]); });
    const mgr = path.join(o.oapRoot, 'oap_12201');
    list(path.join(mgr, 'data', 'apps')).forEach((n) => { const e = readJson(path.join(mgr, 'data', 'apps', n)); add(e && e.userId !== undefined ? e.userId : ''); });
    lines(path.join(mgr, 'security', 'uids.txt')).forEach((l) => add(l.split('=')[0]));
    const dropins = path.join(o.toolRoot, 'mgu-uplink', 'uids.d');
    list(dropins).filter((n) => n !== SELF && !/^\./.test(n))
        .forEach((n) => lines(path.join(dropins, n)).forEach((l) => l.split('#')[0].split(/[\s,]+/).forEach(add)));
    const upd = readJson(path.join(o.toolRoot, 'mgu-updater', 'state', 'uids.json')) || {};
    Object.keys(upd).forEach((k) => add(upd[k]));
    (cfg ? cfg.own : []).forEach(add);
    if (procs) {
        list(o.procDir).filter((n) => /^\d+$/.test(n)).forEach((pid) => lines(path.join(o.procDir, pid, 'status')).forEach((l) => {
            const m = /^[UG]id:\s+(.*)$/.exec(l);
            if (m) m[1].split(/\s+/).forEach(add);
        }));
    }
    return ids;
}
function writeIfChanged(file, text, mode) {
    try { if (fs.readFileSync(file, 'utf8') === text) return; } catch (e) { /* not there yet */ }
    fs.writeFileSync(file + '.tmp', text, { mode: mode });
    fs.renameSync(file + '.tmp', file);
}
// the kept uid of a role ('fetch', or 'web': the phone remote's server, remote.js) while nothing else holds it, else a
// new pick; kept in state/uids.json ({fetch, web}). Only the fetch uid is written for the uplink (internet-uids, uids.d):
// the web uid has no internet. Each role's uid counts as taken for the other. The fetch uid: the lowest free above
// MainApp's app uid ranges, as mgu-updater picks its own; the web uid: the highest free below MAX_UID, so a later pick
// by another component going up (which cannot see a uid in no uplink drop-in) does not land on it. null: none (logged)
function pickUid(o, key) {
    const map = readJson(path.join(o.stateDir, 'uids.json')) || {};
    const kept = Number(map[key]), name = key === 'fetch' ? 'internet uid' : key + ' uid';
    const others = Object.keys(map).filter((k) => k !== key).map((k) => Number(map[k])).filter(isId);
    const cfg = mainappRanges(o);
    const inRange = (u) => inRanges(cfg, u);
    const taken = (procs) => { const t = takenIds(o, cfg, procs); others.forEach((u) => { t[u] = true; }); return t; };
    let uid = null;
    if (isId(kept) && (key === 'fetch' || kept <= MAX_UID) && !taken(false)[kept] && !inRange(kept)) uid = kept;   // (a fetch uid kept from before 0.4.1 may sit above)
    else if (cfg) {
        const all = taken(true), up = key === 'fetch', step = up ? 1 : -1, low = Math.max.apply(null, cfg.ranges.map((r) => r.max)) + 1;
        let u = up ? low : MAX_UID;
        while (u <= MAX_UID && u >= low && (all[u] || inRange(u))) u += step;
        if (u <= MAX_UID && u >= low) { uid = u; o.log(name + ': ' + u + (isId(kept) ? ' (' + kept + ' is taken)' : '')); }
    }
    if (uid === null) {
        o.log(name + ': none: ' + (cfg ? 'no free uid below ' + MAX_UID : "MainApp's config/base.json (its app uid ranges) cannot be read"));
        return null;
    }
    if (!fs.existsSync(o.stateDir)) { mkdirp(path.dirname(o.stateDir)); fs.mkdirSync(o.stateDir, 0o700); }
    const next = {};
    Object.keys(map).forEach((k) => { if (/^[a-z]{1,16}$/.test(k) && isId(Number(map[k]))) next[k] = Number(map[k]); });
    next[key] = uid;
    writeIfChanged(path.join(o.stateDir, 'uids.json'), JSON.stringify(next) + '\n', 0o644);
    if (key !== 'fetch') return uid;
    writeIfChanged(path.join(o.stateDir, 'internet-uids'), uid + '\n', 0o644);
    const uplink = path.join(o.toolRoot, 'mgu-uplink');
    if (fs.existsSync(path.join(uplink, 'current')) || fs.existsSync(path.join(uplink, 'uids.d'))) {
        try { mkdirp(path.join(uplink, 'uids.d')); writeIfChanged(path.join(uplink, 'uids.d', SELF), uid + '\n', 0o644); } catch (e) { o.log('internet uid: uplink drop-in: ' + e.message); }
    }
    return uid;
}
function pickFetchUid(o) { return pickUid(o, 'fetch'); }
// one request in a child as uid (fetchCmd: ytd.js --fetch). The request, secrets and all, goes on its stdin
// (argv shows in ps; o.childEnv, the proxy, is the env); {status, body} or {error} comes back on its stdout. Killed past the timeout.
function fetchRequest(o, uid, req) {
    return new Promise(function (resolve, reject) {
        if (!o.childEnv) return reject(new Error('fetchRequest: no childEnv (the proxy setting)'));
        let child;
        try {
            // (o.noSetuid: tests not run as root, which cannot switch uid)
            const ids = o.noSetuid ? {} : { uid: uid, gid: uid };
            child = cp.spawn(o.fetchCmd[0], o.fetchCmd.slice(1), Object.assign(ids, { cwd: '/', env: o.childEnv || {}, stdio: ['pipe', 'pipe', 'ignore'] }));
        } catch (e) { return reject(e); }
        const chunks = [];
        let size = 0, done = false;
        const finish = function (e, v) {
            if (done) return;
            done = true;
            clearTimeout(timer);
            if (e) { try { child.kill('SIGKILL'); } catch (x) { /* gone */ } reject(e); } else resolve(v);
        };
        const timer = setTimeout(function () { finish(new Error('timeout')); }, (req.timeoutMs || 10000) + 1000);
        child.on('error', finish);
        child.stdin.on('error', function () { /* it died: close says */ });
        child.stdout.on('data', function (c) {
            size += c.length;
            if (size > 24 << 20) return finish(new Error('answer too big')); // 8 MiB of body, JSON-escaped
            chunks.push(c);
            return undefined;
        });
        child.on('close', function () {
            let r = null;
            try { r = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch (e) { /* none */ }
            if (!r) return finish(new Error('fetch helper gave no answer'));
            if (r.error) return finish(new Error(r.error));
            return finish(null, { status: r.status, body: String(r.body) });
        });
        child.stdin.end(JSON.stringify(req));
    });
}

// ---------------------------------------------------------------- YouTube account: the TV device sign-in (as SmartTube does)
// The car itself talks to www.youtube.com: the TV page's client id + secret (scraped at run time, never
// shipped), a device code the user enters at google.com/device, then TVHTML5 browse for the feeds.
// Tokens live in accountFile (0600, written atomically) and never cross the ctl socket or the log.
const YT = 'https://www.youtube.com';
const TV_UA = 'Mozilla/5.0 (Linux; Tizen 2.4.0) AppleWebKit/538.1 (KHTML, like Gecko) Version/2.4.0 TV Safari/538.1';
const TV_CLIENT = { clientName: 'TVHTML5', clientVersion: '7.20251101.10.00', hl: 'en' };
// TV_CLIENT in hello's language and region (feeds, search, related); English and no region when the app sent none
function tvClient(hl, gl) { return Object.assign({}, TV_CLIENT, hl ? { hl: hl } : {}, gl ? { gl: gl } : {}); }
const CRED_RE = /clientId:"([-\w]+\.apps\.googleusercontent\.com)",\n?[$\w]+:"([-\w]+)"/;
const SCOPE = 'http://gdata.youtube.com https://www.googleapis.com/auth/youtube-paid-content';
const TOKEN_URL = YT + '/o/oauth2/token';
const FEEDS = { home: 'FEwhat_to_watch', subs: 'FEsubscriptions', wl: 'VLWL', hist: 'FEhistory', liked: 'VLLL' };
const FEED_MAX = 100;   // a feed's, channel's, playlist's, search's or related list's videos (a full feed line stays far under the app's 1 MiB)
const PLAYLIST_MAX = 1000; // a playlist's (and Watch later's, Liked's) videos, ~150 B each on the line, read 15 a page
const PAGES_MAX = 70;      // next pages read for one playlist (70 x 15 > PLAYLIST_MAX); shortcut: a longer playlist stops at PLAYLIST_MAX

// method, url, headers, body (JSON or null) -> {status, body (utf8)}; no redirects (none seen on these URLs).
// via (proxy.js fromEnv's): the TLS goes through its tunnel, the name looked up there; { error } fails every request
function ytRequest(method, url, headers, body, timeoutMs, lookup, via) {
    return new Promise(function (resolve, reject) {
        const u = require('url').parse(url);
        const data = body ? Buffer.from(JSON.stringify(body)) : null;
        const h = Object.assign({ 'User-Agent': TV_UA }, headers || {});
        if (data) { h['Content-Type'] = 'application/json'; h['Content-Length'] = data.length; }
        let done = false, size = 0;
        const fail = function (e) { if (!done) { done = true; clearTimeout(timer); reject(e); } };
        // plain http only to this machine (a test's fake relay); any other http: URL still goes out as TLS
        const loop = u.protocol === 'http:' && /^(127\.0\.0\.1|localhost)$/.test(u.hostname);
        let req = null;
        const start = function (cc) {
            if (done) return;
            // a custom connection has no agent to give the default port: Host would say :80 (istt1899's fork found it)
            const o = { hostname: u.hostname, port: u.port, path: u.path, method: method, headers: h, lookup: lookup };
            if (cc) Object.assign(o, { port: Number(u.port) || 443, defaultPort: 443, createConnection: cc });
            req = (loop ? require('http') : https).request(o, function (res) {
                const chunks = [];
                res.on('data', function (c) {
                    size += c.length;
                    if (size > 8 << 20) { fail(new Error('answer too big')); req.abort(); return; }
                    chunks.push(c);
                });
                res.on('end', function () {
                    if (done) return;
                    done = true;
                    clearTimeout(timer);
                    resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') });
                });
                res.on('error', fail);
            });
            req.on('error', fail);
            if (data) req.write(data);
            req.end();
        };
        const timer = setTimeout(function () { fail(new Error('timeout')); if (req) req.abort(); }, timeoutMs || 10000);
        if (!via || loop) return start(null);
        return proxy.connect(via, u.hostname, Number(u.port) || 443, { timeoutMs: timeoutMs || 10000, lookup: lookup }, function (e, sock) {
            if (e) return fail(e);
            if (done) return sock.destroy();
            return start(function () { return require('tls').connect({ socket: sock, servername: u.hostname }); });
        });
    });
}

const runsText = (x) => (!x ? '' : typeof x.simpleText === 'string' ? x.simpleText
    : Array.isArray(x.runs) ? x.runs.map((r) => (r && r.text) || '').join('') : '');
function hms(s) { return /^\d+(:\d{1,2}){1,2}$/.test(s) ? s.split(':').reduce((a, p) => a * 60 + Number(p), 0) : null; }
// One TVHTML5 tileRenderer -> {id,title,channel,duration_s,meta}, or null when it is not a video (channels,
// playlists). A short (reelWatchEndpoint) is a video with short:true: the app decides whether it shows them. A live
// stream or an upcoming premiere (isLive) has live:true and no length.
// meta: the texts of the second line, without its "•" and badges ("4K").
function tileItem(t) {
    const sel = t.onSelectCommand || {}, short = t.contentType === 'TILE_CONTENT_TYPE_SHORTS';
    const ep = short ? sel.reelWatchEndpoint : sel.watchEndpoint;
    const id = t.contentId || (ep && ep.videoId);
    if ((t.contentType !== 'TILE_CONTENT_TYPE_VIDEO' && !short) || !ID_RE.test(String(id))) return null;
    const m = (t.metadata && t.metadata.tileMetadataRenderer) || {};
    const lines = (Array.isArray(m.lines) ? m.lines : []).map((l) => ((l && l.lineRenderer && l.lineRenderer.items) || [])
        .map((i) => runsText(i && i.lineItemRenderer && i.lineItemRenderer.text)));
    const ovs = (t.header && t.header.tileHeaderRenderer && t.header.tileHeaderRenderer.thumbnailOverlays) || [];
    const ov = ovs.map((x) => x && x.thumbnailOverlayTimeStatusRenderer).filter(Boolean)[0];
    const rp = ovs.map((x) => x && x.thumbnailOverlayResumePlaybackRenderer).filter(Boolean)[0];   // the account's own progress
    const it = { id: id, title: runsText(m.title) || id, channel: (lines[0] || []).join(''), duration_s: hms(runsText(ov && ov.text)),
        meta: (lines[1] || []).filter((x) => x && x.trim() !== '\u2022') };
    if (short) it.short = true;
    if (rp && rp.percentDurationWatched > 0) it.watched_pct = Math.min(100, Number(rp.percentDurationWatched) || 0);
    if (ep && ep.startTimeSeconds > 0) it.start_s = Number(ep.startTimeSeconds);
    if (isMembers(t)) it.members = true;
    const badges = [].concat.apply([], (Array.isArray(m.lines) ? m.lines : []).map((l) => ((l && l.lineRenderer && l.lineRenderer.items) || [])
        .map((i) => { const b = i && i.lineItemRenderer && i.lineItemRenderer.badge; return b && b.metadataBadgeRenderer && b.metadataBadgeRenderer.style; })));
    if (isLive([ov && ov.style, runsText(ov && ov.text).toUpperCase()].concat(badges), it.duration_s)) it.live = true;
    return it;
}
// One TVHTML5 lockupViewModel (search answers, 2026-11) -> the same, or null when not a video: playlists
// (contentId a list) and channels (no watchEndpoint). A short (LOCKUP_CONTENT_TYPE_SHORT, a reelWatchEndpoint) has
// short:true, a live one live:true. meta: the second metadata row, without badges.
function lockupItem(l) {
    const cc = l.rendererContext && l.rendererContext.commandContext;
    const cmd = (cc && cc.onTap && cc.onTap.innertubeCommand) || {};
    const ep = cmd.watchEndpoint || cmd.reelWatchEndpoint, id = ep && ep.videoId;
    if (!ID_RE.test(String(id)) || id !== l.contentId) return null;
    const m = (l.metadata && l.metadata.lockupMetadataViewModel) || {};
    const cm = m.metadata && m.metadata.contentMetadataViewModel;
    const rows = ((cm && cm.metadataRows) || []).map((r) => ((r && r.metadataParts) || []).map((x) => (x && x.text && x.text.content) || '').filter(Boolean));
    const tv = (l.contentImage && l.contentImage.thumbnailViewModel) || {};
    const ov = (tv.overlays || []).map((x) => x && x.thumbnailBottomOverlayViewModel).filter(Boolean)[0];
    const bvs = ((ov && ov.badges) || []).map((x) => x && x.thumbnailBadgeViewModel).filter(Boolean);
    const len = bvs.map((b) => b.text).filter(Boolean)[0];
    const it = { id: id, title: (m.title && m.title.content) || id, channel: (rows[0] || []).join(''), duration_s: hms(String(len)), meta: rows[1] || [] };
    if (!cmd.watchEndpoint) it.short = true;
    if (isMembers(l)) it.members = true;
    if (isLive([].concat.apply([], bvs.map((b) => [b.badgeStyle, String(b.text || '').toUpperCase()])), it.duration_s)) it.live = true;
    return it;
}
// every video tile (or lockup) under o, in page order, each id once
function videoTiles(o) {
    const out = [], seen = {};
    (function walk(o) {
        if (!o || typeof o !== 'object') return;
        const t = o.tileRenderer || o.lockupViewModel;
        if (t && typeof t === 'object') {
            const it = o.tileRenderer ? tileItem(t) : lockupItem(t);
            if (it && !seen[it.id]) { seen[it.id] = true; out.push(it); }
            return;
        }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(o);
    return out;
}
// A TVHTML5 browse answer -> its video tiles wherever they sit (shelves, tabs, the playlist), each id once.
// root may be an array of pages (a playlist and its continuations): walked the same way, page order.
function feedItems(root) { return videoTiles(root); }
// A playlist page's (or a continuation's) next-page token, or null. TVHTML5 sends 15 videos a page (2026-11, #17).
function nextPageToken(root) {
    let tok = null;
    (function walk(o) {
        if (tok || !o || typeof o !== 'object') return;
        const l = o.playlistVideoListRenderer || o.playlistVideoListContinuation;
        const c = l && l.continuations && l.continuations[0], n = c && (c.nextContinuationData || c.reloadContinuationData);
        if (n && typeof n.continuation === 'string') { tok = n.continuation; return; }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return tok;
}
const SHELF_MAX = 8, SHELF_TILES = 12;
// A TVHTML5 browse answer -> [{title, items}], one per shelfRenderer with video tiles, page order. An untitled
// shelf (Subscriptions' per-channel groups) is named after its channel when all its tiles share one.
// No shelfRenderer (Watch later is a playlist): [].
function feedShelves(root) {
    const out = [];
    (function walk(o) {
        if (!o || typeof o !== 'object') return;
        const s = o.shelfRenderer;
        if (s && typeof s === 'object') {
            const items = videoTiles(s.content);
            if (!items.length) return;
            const h = (s.headerRenderer && s.headerRenderer.shelfHeaderRenderer) || {};
            const al = h.avatarLockup && h.avatarLockup.avatarLockupRenderer;
            let title = runsText(al && al.title) || runsText(h.title);
            if (!title && items.every((i) => i.channel === items[0].channel)) title = items[0].channel;
            out.push({ title: title, items: items.slice(0, SHELF_TILES) });
            return;
        }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return out.slice(0, SHELF_MAX);
}

// FEchannels (the TV app's "Channels" page: every subscription, A-Z) -> [{id, title, handle}], each channel once
const CHAN_RE = /^UC[\w-]{22}$/;
function channelTiles(root) {
    const out = [], seen = {};
    (function walk(o) {
        if (!o || typeof o !== 'object') return;
        const t = o.tileRenderer;
        if (t && typeof t === 'object') {
            const ep = t.onSelectCommand && t.onSelectCommand.browseEndpoint, id = t.contentId || (ep && ep.browseId);
            if (t.contentType !== 'TILE_CONTENT_TYPE_CHANNEL' || !CHAN_RE.test(String(id)) || seen[id]) return;
            const m = (t.metadata && t.metadata.tileMetadataRenderer) || {};
            const lines = (Array.isArray(m.lines) ? m.lines : []).map((l) => ((l && l.lineRenderer && l.lineRenderer.items) || [])
                .map((i) => runsText(i && i.lineItemRenderer && i.lineItemRenderer.text)).join(''));
            seen[id] = true;
            out.push({ id: id, title: runsText(m.title) || id, handle: lines.filter((x) => x.charAt(0) === '@')[0] || '' });
            return;
        }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return out;
}

// The TV app's Playlists page (FEplaylist_aggregation) -> the account's playlists [{id, title, count, meta, thumb}], each once, page order.
// A tileRenderer (TILE_CONTENT_TYPE_PLAYLIST) or a lockupViewModel (LOCKUP_CONTENT_TYPE_PLAYLIST). id: the playlist's
// (its browse id without "VL"); count: the "N videos" text, null when none; meta: the other texts ("Private", "Updated
// today"); thumb: the video whose picture the tile shows (thumb{id} makes it), null when none. Watch later is skipped:
// the app has it as a page of its own.
const PL_RE = /^[\w-]{2,64}$/;
const isPl = (x) => typeof x === 'string' && PL_RE.test(x);
function playlistTiles(root) {
    const out = [], seen = { WL: true };
    const plId = (c) => {
        const be = c && c.browseEndpoint, wp = c && (c.watchPlaylistEndpoint || c.watchEndpoint);
        if (be && /^VL[\w-]{2,64}$/.test(String(be.browseId))) return be.browseId.slice(2);
        return (wp && wp.playlistId) || null;
    };
    const vidOf = (url) => { const m = /\/vi(?:_webp)?\/([\w-]{11})\//.exec(String(url || '')); return m ? m[1] : null; };
    const firstVid = (o) => {                      // the first thumbnail URL under o that names a video
        let v = null;
        (function walk(x) {
            if (v || !x || typeof x !== 'object') return;
            if (typeof x.url === 'string') v = vidOf(x.url);
            Object.keys(x).forEach((k) => walk(x[k]));
        })(o);
        return v;
    };
    const add = (id, title, texts, thumb) => {
        if (!isPl(id) || seen[id]) return;
        seen[id] = true;
        const COUNT = /^(?:([\d.,\s]+)|no)\s+videos?$/i, n = texts.map((s) => COUNT.exec(s.trim())).filter(Boolean)[0];
        out.push({ id: id, title: title || id, count: n ? Number((n[1] || '0').replace(/[^\d]/g, '')) : null,
            meta: texts.filter((s) => s && s.trim() !== '•' && !COUNT.test(s.trim())), thumb: thumb || null });
    };
    (function walk(o) {
        if (!o || typeof o !== 'object') return;
        const t = o.tileRenderer, l = o.lockupViewModel;
        if (t && typeof t === 'object' && /PLAYLIST/.test(String(t.contentType))) {
            const m = (t.metadata && t.metadata.tileMetadataRenderer) || {};
            const texts = [].concat(...(Array.isArray(m.lines) ? m.lines : []).map((x) => ((x && x.lineRenderer && x.lineRenderer.items) || [])
                .map((i) => runsText(i && i.lineItemRenderer && i.lineItemRenderer.text))));
            const ov = ((t.header && t.header.tileHeaderRenderer && t.header.tileHeaderRenderer.thumbnailOverlays) || [])
                .map((x) => runsText(x && x.thumbnailOverlayTimeStatusRenderer && x.thumbnailOverlayTimeStatusRenderer.text)).filter(Boolean);
            const ep = t.onSelectCommand || {};
            add(plId(ep) || t.contentId, runsText(m.title), ov.concat(texts).filter(Boolean),
                (ep.watchEndpoint && ID_RE.test(String(ep.watchEndpoint.videoId)) && ep.watchEndpoint.videoId) || firstVid(t.header));
            return;
        }
        if (l && typeof l === 'object' && /PLAYLIST/.test(String(l.contentType))) {
            const m = (l.metadata && l.metadata.lockupMetadataViewModel) || {};
            const cm = m.metadata && m.metadata.contentMetadataViewModel;
            const texts = [].concat(...((cm && cm.metadataRows) || []).map((r) => ((r && r.metadataParts) || []).map((x) => (x && x.text && x.text.content) || '')));
            const badges = [];
            (function walk(x) {
                if (!x || typeof x !== 'object') return;
                if (x.thumbnailBadgeViewModel && typeof x.thumbnailBadgeViewModel.text === 'string') badges.push(x.thumbnailBadgeViewModel.text);
                Object.keys(x).forEach((k) => walk(x[k]));
            })(l.contentImage);
            const cc = l.rendererContext && l.rendererContext.commandContext, cmd = (cc && cc.onTap && cc.onTap.innertubeCommand) || {};
            add(plId(cmd) || l.contentId, m.title && m.title.content, badges.concat(texts).filter(Boolean), firstVid(l.contentImage));
            return;
        }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return out;
}
// A playlist page's name (its header: TVHTML5's entityMetadataRenderer, 2026-11), or ''
function playlistTitle(root) {
    let t = '';
    (function walk(o) {
        if (t || !o || typeof o !== 'object') return;
        const h = o.entityMetadataRenderer || o.playlistHeaderRenderer || o.pageHeaderViewModel;
        if (h) { t = runsText(h.title) || (h.title && h.title.dynamicTextViewModel && h.title.dynamicTextViewModel.text && h.title.dynamicTextViewModel.text.content) || ''; if (t) return; }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return t;
}
// playlist/get_add_to_playlist (the Save to playlist list: the playlists the account owns) -> as playlistTiles, with
// no count; meta: its privacy ("Private"); thumb: the video its picture shows, when it names one. Watch later skipped.
function addToPlaylistItems(root) {
    const out = [], seen = { WL: true };
    (function walk(o) {
        if (!o || typeof o !== 'object') return;
        const p = o.playlistAddToOptionRenderer;
        if (p && typeof p === 'object') {
            const id = p.playlistId;
            if (!isPl(id) || seen[id]) return;
            seen[id] = true;
            const pv = String(p.privacy || ''), m = /\/vi(?:_webp)?\/([\w-]{11})\//.exec(JSON.stringify(p.thumbnailRenderer || {}));
            out.push({ id: id, title: runsText(p.title) || id, count: null, meta: pv ? [pv.charAt(0) + pv.slice(1).toLowerCase()] : [], thumb: m ? m[1] : null });
            return;
        }
        Object.keys(o).forEach((k) => walk(o[k]));
    })(root);
    return out;
}

// emit(o): to the app. Every method answers with an event and never rejects (a rejection would be a fault).
// client(): the InnerTube client of this connection's language (tvClient); TV_CLIENT when not given
function Account(opts, emit, client) {
    this.opts = opts;
    this.log = opts.log;
    this.emit = emit;
    this.client = client || (() => TV_CLIENT);
    this.pending = null;     // the device code shown: {cid, sec, device, code, url, interval, expires, timer}
    this.starting = false;
    this.refreshing = null;
    this.feeding = Object.create(null); // which -> the browse in flight, shared by later asks
    this.cred = null;        // the TV page's client id + secret, scraped once a process (again when refused)
    this.gen = 0;            // bumped by sign_out: work started before it saves, shows and re-arms nothing
}
Account.prototype.load = function () {
    try {
        const a = JSON.parse(fs.readFileSync(this.opts.accountFile, 'utf8'));
        return a && (a.refresh || a.access) ? a : null;
    } catch (e) { return null; }
};
Account.prototype.save = function (a) {
    const f = this.opts.accountFile, tmp = f + '.tmp', dir = path.dirname(f);
    if (!fs.existsSync(dir)) { mkdirp(path.dirname(dir)); fs.mkdirSync(dir, 0o700); }
    try { fs.unlinkSync(tmp); } catch (e) { /* none */ }
    const fd = fs.openSync(tmp, 'wx', 0o600);
    try { fs.writeSync(fd, JSON.stringify(a)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(tmp, f);
};
Account.prototype.json = function (method, url, body, headers) {
    return this.opts.ytRequest(method, url, headers || null, body || null, this.opts.ytTimeoutMs).then((r) => {
        let j = null;
        try { j = JSON.parse(r.body); } catch (e) { /* not JSON */ }
        return { status: r.status, j: j };
    });
};
Account.prototype.status = function () {
    if (this.load()) return { ev: 'auth', state: 'signed_in' };
    const p = this.pending;
    if (p && Date.now() < p.expires) return { ev: 'auth', state: 'code', code: p.code, url: p.url, expires: Math.round((p.expires - Date.now()) / 1000) };
    return { ev: 'auth', state: 'signed_out' };
};
// the TV page's client id + secret: inline in the page (2026-11), else in its base script
Account.prototype.creds = async function () {
    const get = (u) => this.opts.ytRequest('GET', u, null, null, this.opts.ytTimeoutMs).then((r) => {
        if (r.status !== 200) throw new Error('tv page ' + r.status);
        return r.body;
    });
    const page = await get(YT + '/tv');
    let m = CRED_RE.exec(page);
    const re = /src="(\/s\/[^"]+(?:\/base\.js|m=base))"/g;
    let s;
    while (!m && (s = re.exec(page))) m = CRED_RE.exec(await get(YT + s[1]));
    if (!m) throw new Error('no client id in the TV page');
    return { cid: m[1], sec: m[2] };
};
// `auth`: signed in or a code still valid -> said again; else a new device code, then polled until used or expired
Account.prototype.start = async function () {
    if (this.status().state !== 'signed_out') return this.emit(this.status());
    if (this.starting) return undefined; // its answer comes
    this.starting = true;
    const g = this.gen;
    const ask = (c) => this.json('POST', YT + '/o/oauth2/device/code',
        { client_id: c.cid, scope: SCOPE, device_id: crypto.randomBytes(16).toString('hex'), device_model: 'ytlr::' });
    try {
        const cached = !!this.cred;
        let c = this.cred || (this.cred = await this.creds());
        if (g !== this.gen) return undefined;
        let r = await ask(c);
        if (g !== this.gen) return undefined;
        if (cached && !(r.j && r.j.device_code)) { // the cached client refused: scrape the page again, once
            this.cred = null;
            c = this.cred = await this.creds();
            if (g !== this.gen) return undefined;
            r = await ask(c);
            if (g !== this.gen) return undefined;
        }
        const j = r.j || {};
        if (!j.user_code || !j.device_code) throw new Error('device code refused: ' + (j.error || r.status));
        this.pending = { cid: c.cid, sec: c.sec, device: j.device_code, code: String(j.user_code), url: j.verification_url || 'https://www.google.com/device',
            interval: Number(j.interval) > 0 ? Number(j.interval) * 1000 : 5000, expires: Date.now() + (Number(j.expires_in) || 1800) * 1000 };
        this.log('sign-in: code issued');
        this.emit(this.status());
        this.poll(this.pending);
    } catch (e) {
        this.log('sign-in: ' + e.message);
        if (g === this.gen) this.emit({ ev: 'auth', state: 'error', msg: e.message });
    } finally {
        this.starting = false;
    }
    return undefined;
};
Account.prototype.poll = function (p) {
    p.timer = setTimeout(() => this.pollOnce(p).catch((e) => {
        this.log('sign-in: ' + e.message);
        if (this.pending === p) { this.pending = null; this.emit({ ev: 'auth', state: 'error', msg: e.message }); }
    }), p.interval);
};
Account.prototype.pollOnce = async function (p) {
    if (this.pending !== p) return undefined;
    if (Date.now() >= p.expires) {
        this.pending = null;
        this.log('sign-in: code expired');
        return this.emit({ ev: 'auth', state: 'signed_out' });
    }
    let r;
    try {
        r = await this.json('POST', TOKEN_URL, { client_id: p.cid, client_secret: p.sec, code: p.device, grant_type: 'http://oauth.net/grant_type/device/1.0' });
    } catch (e) {
        this.log('sign-in poll: ' + e.message); // offline a moment: ask again until the code expires
        return this.pending === p ? this.poll(p) : undefined;
    }
    if (this.pending !== p) return undefined;
    const j = r.j || {};
    if (j.access_token) {
        this.pending = null;
        this.save({ cid: p.cid, sec: p.sec, access: j.access_token, refresh: j.refresh_token || null, exp: Date.now() + (Number(j.expires_in) || 3600) * 1000 });
        this.log('sign-in: signed in');
        return this.emit({ ev: 'auth', state: 'signed_in' });
    }
    if (j.error === 'slow_down') p.interval += 5000;
    if (j.error === 'authorization_pending' || j.error === 'slow_down') return this.poll(p);
    this.pending = null;
    this.log('sign-in: ' + (j.error || 'token ' + r.status));
    return this.emit(j.error === 'access_denied' || j.error === 'expired_token' ? { ev: 'auth', state: 'signed_out' } : { ev: 'auth', state: 'error', msg: j.error || 'token ' + r.status });
};
Account.prototype.signOut = function () {
    this.gen++;
    this.refreshing = null;
    if (this.pending) clearTimeout(this.pending.timer);
    this.pending = null;
    this.drop('asked');
};
Account.prototype.drop = function (why) {
    try { fs.unlinkSync(this.opts.accountFile); } catch (e) { /* none */ }
    this.log('account: signed out (' + why + ')');
    this.emit({ ev: 'auth', state: 'signed_out' });
};
// an access token with 5 min left at least: refreshed when not (force: anyway), one refresh at a time
Account.prototype.token = function (force) {
    const a = this.load();
    const out = new Error('signed out');
    out.signedOut = true;
    if (!a) return Promise.reject(out);
    if (!force && a.access && a.exp - Date.now() > 5 * 60000) return Promise.resolve(a.access);
    if (!this.refreshing) {
        const p = this.refresh(a, out), done = () => { if (this.refreshing === p) this.refreshing = null; };
        this.refreshing = p;
        p.then(done, done);
    }
    return this.refreshing;
};
Account.prototype.refresh = async function (a, out) {
    if (!a.refresh) { this.drop('token expired'); throw out; }
    const g = this.gen;
    const r = await this.json('POST', TOKEN_URL, { client_id: a.cid, client_secret: a.sec, refresh_token: a.refresh, grant_type: 'refresh_token' });
    if (g !== this.gen) throw out; // signed out meanwhile: the new token is not kept
    const j = r.j || {};
    if (j.access_token) {
        a.access = j.access_token;
        a.exp = Date.now() + (Number(j.expires_in) || 3600) * 1000;
        if (j.refresh_token) a.refresh = j.refresh_token;
        this.save(a);
        this.log('account: token refreshed');
        return a.access;
    }
    if (j.error === 'invalid_grant') { this.drop('invalid_grant'); throw out; } // revoked or expired: sign in again
    throw new Error('refresh: ' + (j.error || r.status));
};
Account.prototype.browse = function (browseId, token, client) {
    return this.json('POST', YT + '/youtubei/v1/browse', { context: { client: client || this.client() }, browseId: browseId }, { Authorization: 'Bearer ' + token });
};
// A playlist's first page plus its next pages until `max` videos -> [pages] (just [first] when it has no next one).
// A next page that fails ends the list there: the videos already read still show. early([first]): called once, before
// the next pages are read, so the first 15 show at once (a 1000-video playlist is ~70 requests)
Account.prototype.morePages = async function (first, token, max, early) {
    const pages = [first];
    if (early && nextPageToken(first)) early(pages.slice());
    for (let tok = nextPageToken(first), n = 0; tok && n < PAGES_MAX && feedItems(pages).length < max; n++) {
        let r;
        try { r = await this.json('POST', YT + '/youtubei/v1/browse', { context: { client: this.client() }, continuation: tok }, { Authorization: 'Bearer ' + token }); } catch (e) { r = { status: e.message }; }
        if (r.status !== 200 || !r.j) { this.log('next page: browse ' + r.status); break; }
        pages.push(r.j);
        tok = nextPageToken(r.j);
    }
    return pages;
};
// `feed` {which: home|subs|wl|hist|liked} -> feed{which, items, shelves} or feed{which, error}; hist is the account's
// watch history (FEhistory), liked its Liked videos (VLLL). Signed out: error 'signed out', as every feed
Account.prototype.feed = function (which) {
    which = String(which);
    if (typeof FEEDS[which] !== 'string') { this.emit({ ev: 'feed', which: which, error: 'unknown feed' }); return Promise.resolve(); }
    if (!this.feeding[which]) {
        const done = () => { delete this.feeding[which]; };
        this.feeding[which] = this.loadFeed(which);
        this.feeding[which].then(done, done);
    }
    return this.feeding[which];
};
// where the account left video `id` (ms, from its History's first page), or 0: what another device watched to
Account.prototype.resumeAt = async function (id) {
    const r = await this.browse(FEEDS.hist, await this.token(false));
    const it = r.status === 200 && r.j ? feedItems(r.j).filter((x) => x.id === id)[0] : null;
    return it && it.start_s > 0 ? it.start_s * 1000 : 0;
};
Account.prototype.loadFeed = async function (which) {
    const g = this.gen;
    try {
        let tok = await this.token(false), r = await this.browse(FEEDS[which], tok);
        if (r.status === 401) r = await this.browse(FEEDS[which], tok = await this.token(true)); // the token died early
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        if (r.status !== 200 || !r.j) throw new Error('browse ' + r.status);
        const pl = which === 'wl' || which === 'liked', max = pl ? PLAYLIST_MAX : FEED_MAX;   // playlists: read page by page
        const pages = !pl ? [r.j] : await this.morePages(r.j, tok, max, (p) => this.emit({ ev: 'feed', which: which, items: feedItems(p).slice(0, max), shelves: [], partial: true }));
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        this.emit({ ev: 'feed', which: which, items: feedItems(pages).slice(0, max), shelves: feedShelves(r.j) });
    } catch (e) {
        if (!e.signedOut) this.log('feed ' + which + ': ' + e.message);
        this.emit({ ev: 'feed', which: which, error: e.signedOut ? 'signed out' : e.noUid ? e.message : 'feed offline' });
    }
    return undefined;
};
// `channels` -> channels{items:[{id,title,handle}]} or channels{error}; the list is kept 10 min
// `channel` {id} -> channel{id, items, shelves} (its page: Videos, Popular...) or channel{id, error}
Account.prototype.channels = function () { return this.browseEv({ ev: 'channels' }, 'FEchannels', (j) => ({ items: channelTiles(j) }), 600000); };
Account.prototype.channel = function (id) {
    if (!CHAN_RE.test(String(id))) { this.emit({ ev: 'channel', id: String(id), error: 'bad channel' }); return Promise.resolve(); }
    return this.browseEv({ ev: 'channel', id: id }, id, (j) => ({ items: feedItems(j).slice(0, FEED_MAX), shelves: feedShelves(j) }), 0);
};
// `playlists` -> playlists{items:[{id,title,count,meta,thumb}]} or playlists{error}, never kept. The TV Playlists page;
// when it refuses (seen 2026-11: 404 for an account a while, then 200) or lists none (a page we no longer read:
// every account has Liked videos at least), the Save to playlist list (no counts). That one failing after the
// page's 200: the page's (empty) answer.
// `playlist` {id} -> playlist{id, title, items} (its videos, at most PLAYLIST_MAX, read page by page) or playlist{id, error}
const ANY_VIDEO = 'jNQXAC9IVRw';   // get_add_to_playlist lists the playlists for a video; any public one does
Account.prototype.playlists = async function () {
    const g = this.gen, call = async (fn) => {
        let r = await fn(await this.token(false));
        if (r.status === 401) r = await fn(await this.token(true));
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        return r;
    };
    try {
        const r = await call((tok) => this.browse('FEplaylist_aggregation', tok, TV_CLIENT));   // English: playlistTiles reads "N videos"
        let items = r.status === 200 && r.j ? playlistTiles(r.j) : null;
        if (!items || !items.length) {
            const q = await call((tok) => this.json('POST', YT + '/youtubei/v1/playlist/get_add_to_playlist',
                { context: { client: TV_CLIENT }, videoIds: [ANY_VIDEO] }, { Authorization: 'Bearer ' + tok }));
            if (q.status === 200 && q.j) items = addToPlaylistItems(q.j);
            else if (!items) throw new Error('playlists ' + r.status + ', save list ' + q.status);
            else this.log('playlists: none on the page, save list ' + q.status);
        }
        this.emit({ ev: 'playlists', items: items });
    } catch (e) {
        if (!e.signedOut) this.log('playlists: ' + e.message);
        this.emit({ ev: 'playlists', error: e.signedOut ? 'signed out' : e.noUid ? e.message : 'feed offline' });
    }
    return undefined;
};
Account.prototype.playlist = function (id) {
    if (!isPl(id)) { this.emit({ ev: 'playlist', id: String(id), error: 'bad playlist' }); return Promise.resolve(); }
    if (/^RD/.test(id)) return this.mix(id);
    return this.browseEv({ ev: 'playlist', id: id }, 'VL' + id, (j) => ({ title: playlistTitle(j), items: feedItems(j).slice(0, PLAYLIST_MAX) }), 0, PLAYLIST_MAX);
};
// A Mix (RD + the video it grew from: a saved "Mix - ..." in Playlists) has no browse page (VLRD... -> 400, seen on the
// owner's car 2026-11-20): its videos are the watch page's playlist shelf (/next with playlistId), its title the panel's
Account.prototype.mix = async function (id) {
    const head = { ev: 'playlist', id: id }, g = this.gen;
    const body = { playlistId: id };
    if (/^RD[\w-]{11}$/.test(id)) body.videoId = id.slice(2);
    const next = (tok) => this.json('POST', YT + '/youtubei/v1/next', Object.assign({ context: { client: this.client() } }, body), { Authorization: 'Bearer ' + tok });
    try {
        let r = await next(await this.token(false));
        if (r.status === 401) r = await next(await this.token(true));
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        if (r.status !== 200 || !r.j) throw new Error('next ' + r.status);
        const w = (r.j.contents && r.j.contents.singleColumnWatchNextResults) || {};
        const shelves = (w.pivot && w.pivot.sectionListRenderer && w.pivot.sectionListRenderer.contents) || [];
        const shelf = shelves.find((s) => JSON.stringify(s).indexOf('playlistPanelVideoWrapperRenderer') >= 0);
        const pl = (w.playlist && w.playlist.playlist) || {};
        this.emit(Object.assign({}, head, { title: typeof pl.title === 'string' ? pl.title : '', items: shelf ? feedItems(shelf).slice(0, PLAYLIST_MAX) : [] }));
    } catch (e) {
        if (!e.signedOut) this.log('playlist: ' + e.message);
        this.emit(Object.assign({}, head, { error: e.signedOut ? 'signed out' : e.noUid ? e.message : 'feed offline' }));
    }
    return undefined;
};
// pagesMax: follow a playlist's next pages up to that many videos; parse then gets the array of pages, and the
// first page goes out at once with partial: true (the whole list follows, without it)
Account.prototype.browseEv = async function (head, browseId, parse, keepMs, pagesMax) {
    const g = this.gen, c = this.kept || (this.kept = {}), k = c[browseId];
    try {
        if (k && keepMs && Date.now() - k.t < keepMs && k.g === g) { this.emit(Object.assign({}, head, k.v)); return undefined; }
        let tok = await this.token(false), r = await this.browse(browseId, tok);
        if (r.status === 401) r = await this.browse(browseId, tok = await this.token(true));
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        if (r.status !== 200 || !r.j) throw new Error('browse ' + r.status);
        const j = pagesMax ? await this.morePages(r.j, tok, pagesMax, (p) => this.emit(Object.assign({}, head, parse(p), { partial: true }))) : r.j;
        if (g !== this.gen) throw Object.assign(new Error('signed out'), { signedOut: true });
        const v = parse(j);
        if (keepMs) c[browseId] = { t: Date.now(), g: g, v: v };
        this.emit(Object.assign({}, head, v));
    } catch (e) {
        if (!e.signedOut) this.log(head.ev + ': ' + e.message);
        this.emit(Object.assign({}, head, { error: e.signedOut ? 'signed out' : e.noUid ? e.message : 'feed offline' }));
    }
    return undefined;
};
Account.prototype.close = function () {
    if (this.pending) clearTimeout(this.pending.timer);
    this.pending = null;
};

// ---------------------------------------------------------------- audio: BMW MediaPlayer
// The two interfaces the sound needs, and the files MainApp describes them in.
const CC_ID = 'org.genivi.am.commandinterface.CommandControl';
const MP_ID = 'de.bmw.infotainment.entertainment.mediaplayer.MediaPlayer';
const CC_FILE = CC_ID.replace(/\./g, '_') + '.json';
const MP_FILE = MP_ID.replace(/\./g, '_') + '.json';
// the same interfaces under a numbered name (iDrive 8 numbers its HMI's: remote2.RemoteHmiApplication2)
const CC_LIKE = /(^|_)commandinterface\d*_CommandControl\d*\.json$/i;
const MP_LIKE = /(^|_)mediaplayer\d*_MediaPlayer\d*\.json$/i;
// what the calls below use of each (the Diagnostics page names what a description lacks)
const CC_NEEDS = ['connect', 'disconnect', 'listOfMainConnections', 'listOfMainSources', 'listOfMainSinks'];
const MP_NEEDS = ['playFile', 'sendButtonEvent', 'setTime', 'playerState', 'currentItemTime', 'playbackErrorEvent'];
const MAX_IFACE_JSON = 4 << 20, MAX_CATALOG = 64 << 20;
// names worth a line on the Diagnostics page: anything of the audio manager or the players
const AUDIO_LIKE = /(^|[_.])(am|audio\w*|audiomanager\w*|commandinterface\w*|mediaplayer\w*|entertainment)([_.]|$)/i;

/* The parsed JSON of a regular file of at most max bytes; undefined otherwise. */
function readJsonCapped(file, max) {
    try {
        const st = fs.statSync(file);
        if (!st.isFile() || st.size > max) return undefined;
        return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (e) { return undefined; }
}
/* The instance names a description has, or null when it lists none. */
function instancesOf(cfg) {
    const v = cfg && cfg.instances;
    if (Array.isArray(v)) return v.map((x) => (x && typeof x === 'object' ? String(x.name || x.instance || x.id || '') : String(x))).filter(Boolean);
    return v && typeof v === 'object' ? Object.keys(v) : null;
}
/* The names a description offers (its methods, fields, attributes, broadcasts and events), or null when it has no such lists. */
function offered(cfg) {
    let out = null;
    ['methods', 'fields', 'attributes', 'broadcasts', 'events'].forEach((k) => {
        const v = cfg && cfg[k];
        if (!v || typeof v !== 'object') return;
        out = (out || []).concat(Array.isArray(v) ? v.map((x) => String(x && typeof x === 'object' ? x.name : x)) : Object.keys(v));
    });
    return out;
}
/* The SOME/IP ids a description gives its service and an instance ({ service, instance }: numbers, or null where it
 * does not say them as MainApp's descriptions do: serviceId, instances: { name: id }). */
function idsOf(cfg, inst) {
    const num = (v) => (typeof v === 'number' && isFinite(v) ? v : typeof v === 'string' && /^(0x[0-9a-f]+|\d+)$/i.test(v) ? Number(v) : null);
    const i = cfg && cfg.instances && !Array.isArray(cfg.instances) ? cfg.instances[inst] : null;
    return { service: num(cfg && cfg.serviceId), instance: num(i && typeof i === 'object' ? (i.instanceId !== undefined ? i.instanceId : i.id) : i) };
}
const hex = (n) => (n === null || n === undefined ? '?' : '0x' + n.toString(16));
function errText(e) {
    if (e && typeof e === 'object' && !e.message) { try { return JSON.stringify(e); } catch (x) { /* as a string below */ } }
    return String(e && e.message || e);
}

function Audio(opts, handlers) {
    this.opts = opts;
    this.h = handlers; // {time(v), state(v), error(v)}
    this.cc = null;
    this.mp = null;
    this.connId = null;
    this.src = SOURCE_ID;
    this.sink = SINK_ID;
    this.srcListed = null;  // the audio manager lists our source / sink (null: it gave no list)
    this.sinkListed = null;
    this.building = {};     // 'name|description|instance' -> its proxy, built and started (a try that timed out is joined by the next)
    this.used = null;       // the player in use: { file, cfg, inst, px }
    this.heard = [];        // the players whose events are subscribed (listen)
    this.listed = null;     // the audio manager's lists as last read (lists)
    this.px = { cc: null, mp: null }; // the proxies as they come up (the Diagnostics page asks them)
    this.made = {};         // interface id -> the generator's answer (generate), asked once
    // where the start is and what the car answered: the app's Diagnostics page (Daemon.diagLines), and the reason a
    // play gets while the audio is not up (why)
    this.diag = { stage: 'not started', error: null, short: null, tries: 0, ready: false, binding: null,
        cc: null, mp: null, sources: null, sinks: null, conns: null, notes: [], play: null, events: {}, players: null };
}
/* Said once in the log and kept for the Diagnostics page (the last 8). */
Audio.prototype.note = function (m) {
    const n = this.diag.notes;
    if (n.indexOf(m) >= 0) return;
    n.push(m);
    if (n.length > 8) n.shift();
    this.opts.log(m);
};
// An interface description: the first folder (srcGen, else srcGens in order) that has the file. iDrive 7 keeps them in
// oap-mainapp/src-gen, iDrive 8 in oap-mainapp21/src-gen/someip.
Audio.prototype.iface = function (file) {
    const dirs = this.opts.srcGen ? [this.opts.srcGen] : this.opts.srcGens || SRC_GENS;
    for (const d of dirs) {
        const p = path.join(d, file);
        let cfg = null;
        try { cfg = JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { continue; }
        if (!this.ifaceLogged) this.opts.log('interfaces from ' + d);
        this.ifaceLogged = true;
        this.ifaceFile = p;
        return cfg;
    }
    throw new Error('no ' + file + ' in ' + dirs.join(', '));
};
/* The someipInterfaces/ of the installed apps: the app manager writes a description there for every interface an
 * app's manifest names. */
Audio.prototype.appDirs = function () {
    const root = this.opts.oapRoot;
    if (!root) return [];
    let names = [];
    try { names = fs.readdirSync(root).filter((n) => /^oap_\d+$/.test(n)).sort(); } catch (e) { return []; }
    return names.map((n) => path.join(root, n, 'someipInterfaces'));
};
/* Every description of CommandControl ('cc') or the MediaPlayer ('mp') this head unit has, the likeliest first: its
 * file in each of MainApp's folders, files named like it there (a numbered interface), then the same two in the
 * installed apps' someipInterfaces/: [{ file, cfg, insts, exact, app }]. */
Audio.prototype.candidates = function (kind) {
    const file = kind === 'cc' ? CC_FILE : MP_FILE, like = kind === 'cc' ? CC_LIKE : MP_LIKE;
    const out = [];
    const add = (p, exact, app) => {
        const cfg = readJsonCapped(p, MAX_IFACE_JSON);
        if (cfg && typeof cfg === 'object') out.push({ file: p, cfg: cfg, insts: instancesOf(cfg) || [], exact: exact, app: app });
    };
    const scan = (dirs, app) => {
        dirs.forEach((d) => add(path.join(d, file), true, app));
        dirs.forEach((d) => {
            let names = [];
            try { names = fs.readdirSync(d); } catch (e) { return; }
            names.filter((n) => n !== file && like.test(n)).sort().forEach((n) => add(path.join(d, n), false, app));
        });
    };
    scan(this.opts.srcGen ? [this.opts.srcGen] : this.opts.srcGens || SRC_GENS, false);
    scan(this.appDirs(), true);
    return out;
};
/* The description of CommandControl ('cc') or the MediaPlayer ('mp') as { cfg, file }, with the instance wanted:
 *   1. its file in MainApp's folders (iface: where iDrive 7 has it);
 *   2. the same file in an installed app's someipInterfaces/;
 *   3. a file named like it (a numbered interface) in either;
 *   4. made from the unit's interface catalog by the app manager's own generator (generate).
 * Of several, the one that has the instance wanted, else the one with the most instances. None: the error of 1,
 * which names MainApp's folders. */
Audio.prototype.describe = function (kind, want) {
    const file = kind === 'cc' ? CC_FILE : MP_FILE;
    const name = kind === 'cc' ? 'CommandControl' : 'MediaPlayer';
    let first;
    let got = null;
    try { got = { cfg: this.iface(file), file: this.ifaceFile }; } catch (e) { first = e; }
    if (got) {
        // iDrive 8's MainApp keeps a CommandControl description without the lists of sources and sinks: an installed
        // app's own (the app manager made it of the same interface) that lacks nothing is used instead - still one proxy
        const has = offered(got.cfg), lacks = has && kind === 'cc' ? CC_NEEDS.filter((n) => has.indexOf(n) < 0) : [];
        if (!lacks.length) return got;
        const full = this.candidates(kind).filter((c) => {
            const h = offered(c.cfg);
            return c.app && c.exact && h && CC_NEEDS.every((n) => h.indexOf(n) >= 0) && (!c.insts.length || c.insts.indexOf(want) >= 0);
        })[0];
        if (!full) return got;
        this.note('MainApp\'s CommandControl description lacks ' + lacks.join(', ') + ': using ' + full.file);
        return { cfg: full.cfg, file: full.file };
    }
    const all = this.candidates(kind).filter((c) => c.app || !c.exact);
    const cands = all.some((c) => c.exact) ? all.filter((c) => c.exact) : all;
    if (cands.length) {
        const has = (c) => (c.insts.indexOf(want) >= 0 ? 1 : 0);
        cands.sort((x, y) => has(y) - has(x) || y.insts.length - x.insts.length);
        this.note('the ' + name + ' description is not in MainApp\'s folders: using ' + cands[0].file);
        return { cfg: cands[0].cfg, file: cands[0].file };
    }
    const made = this.generate(kind === 'cc' ? CC_ID : MP_ID);
    if (made.cfg) return { cfg: made.cfg, file: made.from };
    first.short = 'this head unit has no description of the ' + name + ' interface';
    first.generator = made.error;
    throw first;
};
/* The description of interface id, made as the app manager makes an installed app's: its generator (a module of
 * MainApp's AppManagement bundle - a browserify bundle, taken without its entry so that nothing but the generator
 * runs) given the unit's interface catalog and a manifest that names the interface. Asked once per interface:
 * { cfg, from } or { error }. */
Audio.prototype.generate = function (id) {
    if (this.made[id]) return this.made[id];
    let r;
    try { r = this.generateNow(id); } catch (e) { r = { error: 'the generator failed: ' + errText(e) }; }
    this.made[id] = r;
    if (r.cfg) this.note('the description of ' + id + ' was made from ' + r.from);
    return r;
};
Audio.prototype.generateNow = function (id) {
    const cats = this.opts.catalogs || [], gens = this.opts.generators || [];
    const cat = cats.filter((p) => fs.existsSync(p))[0];
    if (!cat) return { error: 'no interface catalog' + (cats.length ? ' (' + cats.join(', ') + ')' : '') };
    const db = readJsonCapped(cat, MAX_CATALOG);
    const ifs = db && db.interfaces && typeof db.interfaces === 'object' ? db.interfaces : null;
    if (!ifs) return { error: cat + ' is not an interface catalog' };
    if (!Object.prototype.hasOwnProperty.call(ifs, id)) return { error: cat + ' has no ' + id };
    const tail = /,\{\},\[[\d,]*\]\)\s*;?\s*$/;
    const said = [];
    for (const g of gens) {
        let text = null;
        try { if (fs.statSync(g).size <= MAX_CATALOG) text = fs.readFileSync(g, 'utf8'); } catch (e) { continue; }
        const m = text && /"someip-interfaces-generator\/IFConfigGenerator(?:\.js)?":(\d+)/.exec(text);
        if (!m || !tail.test(text)) { said.push(g + ' has no generator this service can load'); continue; }
        // eslint-disable-next-line no-new-func
        const req = new Function('require', 'return ' + text.replace(tail, ',{},[])'))(require);
        const out = req(m[1]).generate(db, { proxies: { [id]: '*' } });
        const cfg = out && out[id];
        if (cfg && typeof cfg === 'object') return { cfg: cfg, from: cat + ' (generator of ' + g + ')' };
        said.push(g + ': its generator gave no ' + id);
    }
    return { error: said.length ? said.join('; ') : 'no generator' + (gens.length ? ' (' + gens.join(', ') + ')' : '') };
};
/* The audio manager's interfaces for the players of its sources (through them a player says that its source plays),
 * described from the unit's catalog as generate() does, as lines for the page. Asked for only once a way took samples
 * and was not heard: a source that is connected and still silent may wait for such a word. */
Audio.prototype.amPlayerIfaces = function () {
    if (this.amIf) return this.amIf;
    const out = [];
    const cat = (this.opts.catalogs || []).filter((p) => fs.existsSync(p))[0];
    const db = cat ? readJsonCapped(cat, MAX_CATALOG) : null;
    const ids = db && db.interfaces && typeof db.interfaces === 'object' ? Object.keys(db.interfaces).filter((k) => /(^|\.)(AmPlayerState|AmSourceControl)$/.test(k)).slice(0, 3) : [];
    if (!ids.length) out.push('None in the interface catalog' + (cat ? ' ' + cat : ' (no catalog)') + '.');
    ids.forEach((id) => {
        const r = this.generate(id);
        if (!r.cfg) { out.push(id + ': not described (' + r.error + ')'); return; }
        let lines;
        try { lines = describeIface(r.cfg); } catch (e) { lines = ['could not be printed: ' + errText(e)]; }
        out.push(id + ' | service ' + hex(idsOf(r.cfg, '').service) + ' | instances: ' + ((instancesOf(r.cfg) || []).slice(0, 16).join(', ') || 'none listed'));
        lines.forEach((l) => out.push('  ' + l.slice(0, 1800)));
    });
    this.amIf = out;
    return out;
};
/* The interfaces of the unit's catalog that have a method of this name (what BMW's own player code was read to
 * ask beside the player), described as generate() does: lines for the page. Nothing is asked of the car. */
Audio.prototype.ifacesWith = function (method) {
    this.withM = this.withM || {};
    if (this.withM[method]) return this.withM[method];
    const out = [];
    const cat = (this.opts.catalogs || []).filter((p) => fs.existsSync(p))[0];
    const db = cat ? readJsonCapped(cat, MAX_CATALOG) : null;
    const ifs = db && db.interfaces && typeof db.interfaces === 'object' ? db.interfaces : null;
    const needle = '"' + method + '"';
    const ids = ifs ? Object.keys(ifs).filter((k) => { try { return JSON.stringify(ifs[k]).indexOf(needle) >= 0; } catch (e) { return false; } }) : [];
    if (!ids.length) out.push(method + ': in no interface of the catalog' + (cat ? '' : ' (no catalog)'));
    ids.slice(0, 3).forEach((id) => {
        const r = this.generate(id);
        if (!r.cfg) { out.push(method + ': ' + id + ', not described (' + r.error + ')'); return; }
        let lines;
        try { lines = describeIface(r.cfg).filter((l) => l.indexOf(method) >= 0); } catch (e) { lines = ['could not be printed: ' + errText(e)]; }
        out.push(method + ': ' + id + ' | service ' + hex(idsOf(r.cfg, '').service) + ' | instances: ' + ((instancesOf(r.cfg) || []).slice(0, 24).join(', ') || 'none listed') +
            (lines.length ? ' | ' + lines.join(' | ').slice(0, 900) : ''));
    });
    if (ids.length > 3) out.push(method + ': and ' + (ids.length - 3) + ' more (' + ids.slice(3, 9).join(', ') + ')');
    this.withM[method] = out;
    return out;
};
/* The instance to use of a description: the one measured on iDrive 7 ('1'; the player's from conf) where the
 * description has it or lists none. Else, for CommandControl, the only one there is; for the player (unless
 * audio.json names one) the only one named like the generic OAP media player. Else an error naming those it has. */
Audio.prototype.instance = function (kind, cfg, conf) {
    const insts = instancesOf(cfg), want = kind === 'cc' ? '1' : conf.inst;
    if (!insts || !insts.length || insts.indexOf(want) >= 0) return want;
    const name = kind === 'cc' ? 'CommandControl' : 'MediaPlayer';
    let pick = null;
    if (kind === 'cc') pick = insts.length === 1 ? insts[0] : null;
    else if (!conf.set.instance) {
        const one = (re) => { const l = insts.filter((n) => re.test(n)); return l.length === 1 ? l[0] : null; };
        pick = one(/oap.?generic.?media/i) || one(/generic.?media|oap/i);
    }
    if (pick === null) {
        const e = new Error(name + ' has no instance ' + want + ' (it has ' + insts.join(', ') + (kind === 'mp' ? '; set "instance" in ' + this.opts.audioConf : '') + ')');
        e.short = 'the car\'s ' + name + ' has no instance ' + want;
        throw e;
    }
    this.note(name + ' has no instance ' + want + ' on this head unit: using ' + pick + ' (it has ' + insts.join(', ') + ')');
    return pick;
};
// state/audio.json (optional, written by hand): {"sourceID": n, "sinkID": n, "instance": "..."} for a head unit whose
// audio route differs from iDrive 7's. Read at each init (a fix needs only a restart of the service).
Audio.prototype.conf = function () {
    let c = {};
    try { c = JSON.parse(fs.readFileSync(this.opts.audioConf, 'utf8')) || {}; } catch (e) { /* none: the defaults */ }
    const id = (v, d) => (Number.isInteger(v) && v > 0 && v < 65536 ? v : d);
    return { src: id(c.sourceID, SOURCE_ID), sink: id(c.sinkID, SINK_ID), inst: typeof c.instance === 'string' && c.instance ? c.instance : MEDIA_INSTANCE, set: c };
};
Audio.prototype.binding = function () {
    if (!this.someip) {
        const s = this.opts.someip;
        this.someip = typeof s === 'string' ? require(s) : s;
        this.diag.binding = typeof s === 'string' ? s : 'given';
    }
    return this.someip;
};
/* The proxy of an instance of the interface a description (file) describes, built and started: from a fresh tick (as
 * BMW's SDK calls the binding), once - a try that ran out of time (ms, else stageMs) is joined by the next one, a
 * failed one is made again. */
Audio.prototype.proxy = function (name, cfg, inst, file, ms) {
    const k = name + '|' + (file || '') + '|' + inst;
    if (!this.building[k]) {
        const someip = this.binding();
        const p = new Promise((resolve, reject) => setImmediate(() => {
            Promise.resolve().then(() => someip.buildProxy(cfg, inst)).then((px) => Promise.resolve(px.init()).then(() => px)).then(resolve, reject);
        }));
        this.building[k] = p;
        p.catch(() => { if (this.building[k] === p) delete this.building[k]; });
    }
    return withTimeout(this.building[k], ms || this.opts.stageMs || 8000, 'starting the ' + name + ' proxy').catch((e) => {
        if (/timed out$/.test(errText(e))) e.short = 'the car\'s ' + name + ' does not start';
        throw e;
    });
};
Audio.prototype.init = async function () {
    if (this.cc && this.mp) return;
    const d = this.diag;
    d.tries++;
    try {
        await this.bringUp();
    } catch (e) {
        d.error = errText(e);
        d.short = e && e.short || null;
        throw e;
    }
};
Audio.prototype.bringUp = async function () {
    const d = this.diag;
    d.stage = 'loading the SOME/IP binding';
    this.binding();
    const conf = this.conf();
    this.src = conf.src;
    this.sink = conf.sink;
    // both descriptions before either proxy: the Diagnostics page then has both, whichever is missing
    const found = {};
    let failed = null;
    [['cc', 'CommandControl', CC_NEEDS], ['mp', 'MediaPlayer', MP_NEEDS]].forEach((k) => {
        try {
            const f = this.describe(k[0], k[0] === 'cc' ? '1' : conf.inst);
            // a head unit whose description has no such instance (another firmware): say which it has instead of hanging
            const has = offered(f.cfg);
            d[k[0]] = { file: f.file, instances: instancesOf(f.cfg), instance: null, available: null, error: null,
                lacks: has ? k[2].filter((n) => has.indexOf(n) < 0) : null };
            d[k[0]].instance = f.inst = this.instance(k[0], f.cfg, conf);
            found[k[0]] = f;
        } catch (e) {
            d[k[0]] = Object.assign(d[k[0]] && d[k[0]].file ? d[k[0]] : { file: null, instances: null, instance: null, available: null, lacks: null },
                { error: errText(e), generator: e.generator || null });
            if (!failed) { failed = e; d.stage = 'finding the ' + k[1] + ' description'; }
        }
    });
    if (failed) throw failed;
    d.stage = 'starting the CommandControl proxy';
    const cc = this.px.cc = await this.proxy('CommandControl', found.cc.cfg, found.cc.inst, found.cc.file);
    d.stage = 'starting the MediaPlayer proxy';
    const mp = this.px.mp = await this.proxy('MediaPlayer', found.mp.cfg, found.mp.inst, found.mp.file);
    d.cc.ids = idsOf(found.cc.cfg, found.cc.inst);
    d.mp.ids = idsOf(found.mp.cfg, found.mp.inst);
    // the route, where the audio manager answers already (at boot it may not yet: start reads it once it does)
    d.cc.available = await this.answers(cc);
    d.mp.available = await this.answers(mp);
    if (d.cc.available) {
        d.stage = 'reading the audio manager\'s sources and sinks';
        await this.route(cc, conf);
    }
    this.cc = cc;
    this.mp = mp;
    this.used = { file: found.mp.file, cfg: found.mp.cfg, inst: found.mp.inst, px: mp };
    this.listen(mp);
    d.stage = 'the proxies are up';
};
/* The player's events to the daemon's handlers: heard of the player in use only (findPlayer may change it), each
 * player subscribed once. The last of each is kept for the Diagnostics page. */
Audio.prototype.listen = function (mp) {
    if (this.heard.indexOf(mp) >= 0) return;
    this.heard.push(mp);
    const d = this.diag, h = this.h;
    const safe = (k, fn) => (v) => {
        if (mp !== this.mp) return;
        d.events[k] = v;
        try { fn(v); } catch (e) { this.opts.log('someip handler: ' + (e && e.stack || e)); }
    };
    mp.subscribe({ name: 'playerState', onReceive: safe('state', h.state) });
    mp.subscribe({ name: 'currentItemTime', onReceive: safe('time', h.time) });
    mp.subscribe({ name: 'playbackErrorEvent', onReceive: safe('error', h.error) });
};
/* The players this head unit offers now: every instance of every description of the MediaPlayer it has (the one in
 * use first; the same service and instance under two descriptions asked once; at most 32) given a proxy and asked,
 * for up to waitMs. Kept for the Diagnostics page; returned with the proxies. */
Audio.prototype.probePlayers = async function (waitMs) {
    const seen = {}, list = [], u = this.used, want = this.conf().inst;
    const push = (c, inst) => {
        const ids = idsOf(c.cfg, inst);
        const k = ids.service !== null && ids.instance !== null ? ids.service + '|' + ids.instance : (c.app ? path.basename(c.file) : c.file) + '|' + inst;
        if (seen[k] || list.length >= 120) return;
        seen[k] = true;
        list.push({ file: c.file, cfg: c.cfg, inst: inst, ids: ids, px: null, available: null, error: null });
    };
    if (u) push(u, u.inst);
    this.candidates('mp').forEach((c) => (c.insts.length ? c.insts : [want]).forEach((i) => push(c, i)));
    for (const x of list) {
        try { x.px = await this.proxy('MediaPlayer', x.cfg, x.inst, x.file, 3000); } catch (e) { x.error = errText(e); }
    }
    const end = Date.now() + waitMs;
    for (;;) {
        for (const x of list) if (x.px && !x.available) x.available = await this.answers(x.px);
        if (Date.now() >= end || list.every((x) => !x.px || x.available)) break;
        await sleep(200);
    }
    this.diag.players = list.map((x) => ({ file: x.file, inst: x.inst, ids: x.ids, available: x.available, error: x.error }));
    return list;
};
/* The player to play with, looked for when the one in use is not offered (or is not the measured one): of those the
 * car offers now, the instance wanted (audio.json's, else iDrive 7's) under whichever description; else the one in
 * use; else, unless audio.json names the instance, the only instance named like the generic OAP media player. A
 * change is logged, and the audio source named like the new instance goes with it where the audio manager lists
 * one. True when the player in use answers afterwards. */
Audio.prototype.findPlayer = async function (waitMs) {
    const conf = this.conf(), u = this.used;
    const up = (await this.probePlayers(waitMs)).filter((x) => x.available);
    let pick = up.filter((x) => x.inst === conf.inst)[0] || up.filter((x) => x.px === u.px)[0];
    if (!pick && !conf.set.instance) {
        const one = (re) => {
            const names = up.map((x) => x.inst).filter((n, i, l) => re.test(n) && l.indexOf(n) === i);
            return names.length === 1 ? up.filter((x) => x.inst === names[0])[0] : null;
        };
        pick = one(/oap.?generic.?media/i) || one(/generic.?media|oap/i);
    }
    if (!pick) return false;
    if (pick.px === u.px) return true;
    const d = this.diag, has = offered(pick.cfg);
    this.used = { file: pick.file, cfg: pick.cfg, inst: pick.inst, px: pick.px };
    this.mp = this.px.mp = pick.px;
    this.listen(pick.px);
    d.mp = { file: pick.file, instances: instancesOf(pick.cfg), instance: pick.inst, available: true, error: null, ids: pick.ids,
        lacks: has ? MP_NEEDS.filter((n) => has.indexOf(n) < 0) : null };
    this.note('the MediaPlayer "' + u.inst + '" is not offered on this head unit: using "' + pick.inst + '"' + (pick.file !== u.file ? ' of ' + pick.file : ''));
    // its source: the configured one for the measured instance (route), else the one named like the instance
    this.src = conf.src;
    await this.route(this.cc, conf);
    const l = this.listed;
    if (!conf.set.sourceID && pick.inst !== conf.inst && l && l.sources) {
        const norm = (v) => String(v).toLowerCase().replace(/[^a-z0-9]/g, '');
        const hit = l.sources.filter((x) => norm(l.name(x)) === norm(pick.inst) && l.idOf(x, 'sourceID') !== null);
        if (hit.length === 1 && l.idOf(hit[0], 'sourceID') !== this.src) {
            this.src = l.idOf(hit[0], 'sourceID');
            this.srcListed = true;
            this.note('audio source ' + this.src + ' (' + l.name(hit[0]) + ') goes with the MediaPlayer "' + pick.inst + '"');
        }
    }
    return true;
};
/* iDrive 8's app manager has, of the players, only the description of a DynamicMediaPlayer (and none of the
 * MediaPlayer): what it is, for the Diagnostics page - the description found (MainApp's folders, then the apps'),
 * each of its instances (at most 8) given a proxy and asked, and its fields read from the first that answers
 * (reads only, 6 s in all). Nothing is called on it. */
Audio.prototype.dynamic = async function (ask) {
    if (!this.dyn) {
        this.dyn = { file: null };
        const dirs = (this.opts.srcGen ? [this.opts.srcGen] : this.opts.srcGens || SRC_GENS).concat(this.appDirs());
        for (const dir of dirs) {
            let names = [];
            try { names = fs.readdirSync(dir); } catch (e) { continue; }
            const n = names.filter((x) => /dynamicmediaplayer\w*\.json$/i.test(x)).sort()[0];
            const cfg = n ? readJsonCapped(path.join(dir, n), MAX_IFACE_JSON) : null;
            if (cfg && typeof cfg === 'object') { this.dyn = { file: path.join(dir, n), cfg: cfg }; break; }
        }
    }
    const y = this.dyn, out = { file: y.file, insts: [], values: {}, service: null };
    if (y.file) {
        const names = [];
        ['fields', 'attributes'].forEach((k) => {
            const v = y.cfg[k];
            if (v && typeof v === 'object') (Array.isArray(v) ? v.map((x) => String(x && typeof x === 'object' ? x.name : x)) : Object.keys(v)).forEach((x) => names.push(x));
        });
        const end = Date.now() + 6000;
        let read = false;
        out.service = idsOf(y.cfg, '').service;
        for (const inst of (instancesOf(y.cfg) || []).slice(0, 8)) {
            const x = { name: inst, id: idsOf(y.cfg, inst).instance, available: null, error: null };
            out.insts.push(x);
            // (the description is read; the car is asked only for the page, and then of its first instance: one proxy, reads)
            if (!this.opts.probe && !(ask && out.insts.length === 1)) continue;
            let px = null;
            try { px = await this.proxy('DynamicMediaPlayer', y.cfg, inst, y.file, 3000); } catch (e) { x.error = errText(e); }
            if (px) x.available = await this.answers(px);
            if (!px || !x.available || read) continue;
            read = true;
            for (const f of names.slice(0, 16)) {
                if (Date.now() > end) { out.values[f] = '(not asked: out of time)'; continue; }
                try {
                    const v = await withTimeout(Promise.resolve().then(() => px.getValue(f, 1500)), 2000, f);
                    out.values[f] = String(JSON.stringify(v)).slice(0, 300);
                } catch (e) { out.values[f] = '(' + errText(e).slice(0, 70) + ')'; }
            }
        }
    }
    this.diag.dyn = out;
};
/* At the start: the player in use given playerWaitMs to answer; when it does not, one that does is looked for
 * (findPlayer). Never an error: a play says what is missing. */
Audio.prototype.settlePlayer = async function () {
    const ms = (v, dflt) => (v === undefined ? dflt : v);
    try {
        const end = Date.now() + ms(this.opts.playerWaitMs, 3000);
        let up = await this.answers(this.mp);
        while (!up && Date.now() < end) { await sleep(200); up = await this.answers(this.mp); }
        this.diag.mp.available = up;
        if (!up && this.opts.probe) this.diag.mp.available = await this.findPlayer(ms(this.opts.probeMs, 3000));
        // (the dynamic player's proxy is made now: a proxy answers only a moment after it is made)
        if (!this.diag.mp.available) await this.dynamic(this.opts.dynamic !== false);
    } catch (e) { this.note('looking for the media player failed: ' + errText(e)); }
};
/* The whole start, which the daemon repeats until it succeeds (Daemon.start): the proxies, the audio manager
 * answering, its lists read (the route), the connections a killed daemon left. */
Audio.prototype.start = async function () {
    const d = this.diag;
    if (this.cc && this.mp) d.tries++;       // (init counts the tries that still have the proxies to start)
    try {
        await this.init();
        d.stage = 'waiting for the audio manager (CommandControl) to answer';
        await this.waitAvailable();
        if (!d.sources && !d.sinks) {
            d.stage = 'reading the audio manager\'s sources and sinks';
            await this.route(this.cc, this.conf());
        }
        d.stage = 'looking for the media player';
        await this.settlePlayer();
        d.stage = 'looking for connections an earlier run left';
        const n = await this.dropLeftovers();
        d.ready = true; d.error = null; d.short = null; d.stage = 'ready';
        return n;
    } catch (e) {
        d.error = errText(e);
        d.short = e && e.short || null;
        throw e;
    }
};
/* One line for the app's player when a play finds the audio not started (the Diagnostics page has the rest). */
Audio.prototype.why = function () {
    const d = this.diag;
    return 'the car\'s audio is not ready - ' + (d.short || d.error || d.stage) + ' (Settings > Diagnostics)';
};
/* The proxy the audio manager's sources and sinks are read from: cc, or - where its description lacks those lists
 * (iDrive 8's MainApp keeps a trimmed one) - one more proxy of the same instance, from the description the app
 * manager's generator makes of the unit's catalog. Made once; cc when it cannot be. */
Audio.prototype.listsFrom = async function (cc) {
    const d = this.diag;
    if (!this.opts.probe || !d.cc || !d.cc.lacks || d.cc.lacks.indexOf('listOfMainSources') < 0) return cc;
    if (this.ccLists === undefined) {
        this.ccLists = null;
        try {
            const made = this.generate(CC_ID), has = made.cfg ? offered(made.cfg) : null;
            if (!made.cfg) this.note('the audio manager\'s sources and sinks cannot be read: its description lacks them and none could be made (' + made.error + ')');
            else if (has && has.indexOf('listOfMainSources') < 0) this.note('the audio manager\'s sources and sinks cannot be read: the description made of the catalog lacks them too');
            else {
                const insts = instancesOf(made.cfg);
                this.ccLists = await this.proxy('CommandControl', made.cfg, insts && insts.length && insts.indexOf(d.cc.instance) < 0 ? insts[0] : d.cc.instance, made.from);
            }
        } catch (e) { this.note('the audio manager\'s sources and sinks cannot be read: ' + errText(e)); }
    }
    return this.ccLists || cc;
};
/* The audio manager's lists (each read given 4 s; null: it gave none), kept as text for the log and the Diagnostics page. */
Audio.prototype.lists = async function (cc) {
    const d = this.diag;
    const from = await this.listsFrom(cc);
    const list = async (attr) => {
        const px = attr === 'listOfMainConnections' ? cc : from;
        try {
            const v = await withTimeout(Promise.resolve().then(() => px.getValue(attr, 4000)), 4500, attr);
            if (!Array.isArray(v)) why[attr] = 'it answered ' + (v === null ? 'null' : typeof v);
            return Array.isArray(v) ? v : null;
        } catch (e) { why[attr] = errText(e).slice(0, 90); return null; }
    };
    const why = d.listWhy = {};
    const name = (x) => String(x && (x.name || x.sourceName || x.sinkName) || '');
    const idOf = (x, k) => (x && typeof x[k] === 'number' ? x[k] : null);
    const fmt = (l, k) => (l ? l.map((x) => idOf(x, k) + (name(x) ? ':' + name(x) : '')).join(' ') : null);
    const sources = await list('listOfMainSources'), sinks = await list('listOfMainSinks'), conns = await list('listOfMainConnections');
    d.sources = fmt(sources, 'sourceID');
    d.sinks = fmt(sinks, 'sinkID');
    d.conns = conns ? conns.map((c) => idOf(c, 'mainConnectionID') + ':' + idOf(c, 'sourceID') + '>' + idOf(c, 'sinkID')).join(' ') : null;
    this.srcListed = sources && sources.length ? sources.some((x) => idOf(x, 'sourceID') === this.src) : null;
    this.sinkListed = sinks && sinks.length ? sinks.some((x) => idOf(x, 'sinkID') === this.sink) : null;
    this.listed = { sources: sources, sinks: sinks, conns: conns, name: name, idOf: idOf };
    return this.listed;
};
// The audio manager's sources and sinks, logged (what a head unit other than the measured iDrive 7 has), and the
// route checked against them. The source: when the one configured is not listed but exactly one source named like the
// media player's generic OAP source is, that one is used. The sink: when the one configured is not listed, the only
// sink there is, or the only one the head unit's own connections use (where its sound plays now), is used - a play
// would fail on an unknown sink anyway; among several nothing is picked (state/audio.json sets it). Lists the head
// unit cannot give change nothing.
Audio.prototype.route = async function (cc, conf) {
    const l = await this.lists(cc), d = this.diag;
    const said = 'audio sources ' + (d.sources === null ? 'not readable' : d.sources) + '; sinks ' + (d.sinks === null ? 'not readable' : d.sinks) +
        '; using source ' + this.src + ', sink ' + this.sink + (Object.keys(conf.set).length ? ' (audio.json)' : '');
    if (this.routeLogged !== said) {
        this.routeLogged = said;
        this.opts.log(said);
    }
    if (!conf.set.sourceID && this.srcListed === false) {
        const one = (re) => { const f = l.sources.filter((x) => re.test(l.name(x)) && l.idOf(x, 'sourceID') !== null); return f.length === 1 ? f[0] : null; };
        const named = one(/oap.?generic.?media/i);
        if (named) {
            this.src = l.idOf(named, 'sourceID');
            this.srcListed = true;
            this.note('source ' + SOURCE_ID + ' is not on this head unit: using ' + this.src + ' (' + l.name(named) + ')');
        } else this.note('source ' + this.src + ' is not listed on this head unit; set "sourceID" in ' + this.opts.audioConf);
    }
    if (!conf.set.sinkID && this.sinkListed === false) {
        const ids = l.sinks.map((x) => l.idOf(x, 'sinkID')).filter((v) => v !== null);
        const used = (l.conns || []).map((c) => l.idOf(c, 'sinkID')).filter((v, i, a) => v !== null && ids.indexOf(v) >= 0 && a.indexOf(v) === i);
        const pick = ids.length === 1 ? ids[0] : used.length === 1 ? used[0] : null;
        if (pick !== null) {
            const nm = l.name(l.sinks.filter((x) => l.idOf(x, 'sinkID') === pick)[0]);
            this.sink = pick;
            this.sinkListed = true;
            this.note('sink ' + SINK_ID + ' is not on this head unit: using ' + pick + (nm ? ' (' + nm + ')' : '') + ', ' +
                (ids.length === 1 ? 'the only sink' : 'the sink its own connections use'));
        } else this.note('sink ' + this.sink + ' is not listed on this head unit; set "sinkID" in ' + this.opts.audioConf);
    }
};
/* A proxy's service is offered now: true, false, or null when the binding does not say within 2 s. */
Audio.prototype.answers = async function (px) {
    try { return !!(await withTimeout(Promise.resolve().then(() => px.isAvailable()), 2000, 'isAvailable')); } catch (e) { return null; }
};
/* For the Diagnostics page: what the car says now (the proxies that are up asked). */
Audio.prototype.refresh = async function () {
    const d = this.diag;
    if (this.px.cc && d.cc) d.cc.available = await this.answers(this.px.cc);
    if (this.px.mp && d.mp) d.mp.available = await this.answers(this.px.mp);
    if (this.px.cc && d.cc && d.cc.available) await this.lists(this.px.cc);
    if (this.dyn) { try { await this.dynamic(d.ready); } catch (e) { this.note('the DynamicMediaPlayer could not be asked: ' + errText(e)); } }
};
Audio.prototype.waitAvailable = async function () {
    const end = Date.now() + this.opts.availWaitMs;
    while (!(await this.cc.isAvailable())) {
        if (Date.now() > end) {
            const e = new Error('CommandControl not available');
            e.short = 'the car\'s audio manager (CommandControl) does not answer';
            throw e;
        }
        await sleep(100);
    }
};
// At start: a connection from our source left by a killed daemon would hold the sink.
Audio.prototype.dropLeftovers = async function () {
    await this.waitAvailable();
    const list = await this.cc.getValue('listOfMainConnections');
    const ours = (Array.isArray(list) ? list : []).filter((c) => c && c.sourceID === this.src);
    for (const c of ours) {
        const r = await this.cc.callMethod('disconnect', { mainConnectionID: c.mainConnectionID }, 5000).catch((e) => String(e));
        this.opts.log('leftover connection ' + c.mainConnectionID + ' disconnect ' + JSON.stringify(r));
    }
    return ours.length;
};
// release() waits for a play in flight, so a stop during connect/playFile still finds connId
Audio.prototype.play = function (file) {
    const p = this.playNow(file);
    this.pending = p.catch(() => {});
    return p;
};
Audio.prototype.playNow = async function (file) {
    await this.init();
    await this.waitAvailable();
    // (what the car answered is kept for the Diagnostics page)
    const play = this.diag.play = { file: path.basename(file), route: null, player: null, waited: 0, connect: null, playFile: null, error: null };
    // a rejection that is no Error (the binding's own) becomes one that says which call it was
    const call = (px, what, m, a) => Promise.resolve().then(() => px.callMethod(m, a, 5000))
        .catch((e) => { throw e instanceof Error && e.message ? e : new Error(what + ' failed: ' + errText(e)); });
    const ms = (v, dflt) => (v === undefined ? dflt : v);
    try {
        // the player: the measured one where the car offers it, else one it does offer (a short look: the proxies are up)
        if (this.opts.probe && (!(await this.answers(this.mp)) || (this.used && this.used.inst !== this.conf().inst))) {
            try { await this.findPlayer(ms(this.opts.probeAgainMs, 500)); } catch (e) { this.note('looking for the media player failed: ' + errText(e)); }
        }
        play.player = await this.answers(this.mp);
        // iDrive 8: no such player - every other way to sound is tried until one plays (sound.js)
        if (!play.player && this.opts.sound !== false) {
            if (!this.dyn) { try { await this.dynamic(false); } catch (e) { this.note('the DynamicMediaPlayer could not be looked for: ' + errText(e)); } }
            if (((this.dyn && this.dyn.file) || this.opts.id8) && this.snd().any()) {
                play.route = 'found by trying (see "Sound on this car")';
                play.snd = true;
                await this.snd().play(file, this.playInfo || {});
                this.connId = 'snd';
                return;
            }
        }
        // a route the audio manager's lists did not settle at the start: looked at again
        if (this.srcListed === false || this.sinkListed === false) await this.route(this.cc, this.conf());
        play.route = this.src + '>' + this.sink;
        const r = await call(this.cc, 'audio connect', 'connect', { sourceID: this.src, sinkID: this.sink });
        play.connect = JSON.stringify(r);
        this.opts.log('connect ' + JSON.stringify(r));
        if (!r || (r.amError !== 'E_OK' && r.amError !== 'E_ALREADY_EXISTS')) throw new Error('audio connect: ' + (r && r.amError));
        this.connId = r.mainConnectionID;
        if (this.connId === undefined || this.connId === null) this.connId = -1; // id unknown: release sweeps by source
        // a player the head unit starts with its source: given a moment once the source is connected
        if (play.player === false) {
            const t0 = Date.now(), end = t0 + ms(this.opts.playerAfterMs, 4000);
            while (Date.now() < end && !(play.player = await this.answers(this.mp))) await sleep(200);
            play.waited = Date.now() - t0;
        }
        const p = await call(this.mp, 'playFile', 'playFile', { path: file, languageCode: 7695972 });
        play.playFile = JSON.stringify(p);
        this.opts.log('playFile ' + JSON.stringify(p));
        if (!p || p.EntError !== 'OK') throw new Error('playFile: ' + (p && p.EntError));
    } catch (e) {
        play.error = errText(e);
        // the player not offered on this head unit: that is what the driver is told, whatever the call answered
        if (play.snd) { /* its own words */ } else if (play.player === false && play.connect && !play.playFile) {
            e.message = 'the car does not offer the media player "' + (this.used && this.used.inst) + '" on this head unit (Settings > Diagnostics)';
            play.error += ' | the player does not answer';
        }
        throw e;
    }
};
/* A head unit that does not offer iDrive 7's OapGenericMedia player (iDrive 8): the play goes to sound.js, which
 * works through every way to sound it knows - the car's DynamicMediaPlayer asked as BMW's own code asks it, then the
 * service's own player on the car's audio router - until one plays, and keeps that one. */
/* Whether what plays now keeps its place while stopped and goes on there at PLAY (the service's own player does;
 * BMW's players, stopped, must be started anew). */
Audio.prototype.holds = function () { return this.connId === 'snd' && !!this.sound && this.sound.holds(); };
Audio.prototype.snd = function () {
    if (!this.sound) this.sound = new (require('./sound.js').Sound)(this);
    return this.sound;
};
// playFile again on the live connection: after a STOP the player needs the file, not PLAY
Audio.prototype.replay = function (file, ms) {
    if (this.connId === 'snd') {
        const q = this.sound.replay(file, ms);
        this.pending = q.catch(() => {});
        return q;
    }
    const p = this.mp.callMethod('playFile', { path: file, languageCode: 7695972 }, 5000).then((r) => {
        this.opts.log('replay ' + JSON.stringify(r));
        if (!r || r.EntError !== 'OK') throw new Error('playFile: ' + (r && r.EntError));
    });
    this.pending = p.catch(() => {}); // release() waits for it, as for play()
    return p;
};
Audio.prototype.button = function (b) {
    if (this.connId === 'snd') return this.sound.button(b);
    if (!this.mp) return Promise.resolve();
    return this.mp.callMethod('sendButtonEvent', { button: b, action: 'PRESS' }, 3000)
        .catch((e) => this.opts.log(b + ' failed: ' + e));
};
// setTime(0) is ignored by the car's player (seen on the car: back to the start, the sound plays on where it was and
// the picture follows it back a second later), so the start is asked as 1 ms
Audio.prototype.seek = function (ms) {
    if (this.connId === 'snd') return this.sound.seek(ms).catch((e) => this.opts.log('seek failed: ' + errText(e)));
    if (!this.mp) return Promise.resolve();
    return this.mp.callMethod('setTime', { newTimeElapsed: Math.max(1, Math.round(ms)) }, 3000).catch((e) => this.opts.log('setTime failed: ' + e));
};
// Every exit path ends here. Safe to call twice.
Audio.prototype.release = async function () {
    if (this.sound) this.sound.cancel();       // (a search for the way to sound in flight ends at its next step)
    if (this.pending) await this.pending;
    if (this.releasing) await this.releasing;   // one under way (a paused player that plays on): its disconnect first
    if (this.connId === null) return;
    const id = this.connId;
    if (id === 'snd') {
        this.connId = null;
        await this.sound.release();
        return;
    }
    this.connId = null;
    const p = this.releasing = this.disconnect(id);
    try { await p; } finally { if (this.releasing === p) this.releasing = null; }
};
Audio.prototype.disconnect = async function (id) {
    await this.button('STOP');
    try {
        if (id === -1) throw new Error('no connection id');
        this.opts.log('disconnect ' + JSON.stringify(await this.cc.callMethod('disconnect', { mainConnectionID: id }, 5000)));
    } catch (e) {
        this.opts.log('disconnect failed: ' + e + '; sweeping source ' + this.src);
        await this.dropLeftovers().catch((e2) => this.opts.log('sweep failed: ' + e2));
    }
};

// ---------------------------------------------------------------- log reports: what leaves the car (send_logs)
// text -> the same text with nothing that names the account, the car, the user or what was watched. k: what the daemon
// knows {secrets, titles, ids: {id: 'v'|'p'|'c'}}; salt: one report's, so a video keeps one tag (v#1a2b) in it, and
// reports can't be matched to each other. Node 8: no lookbehind, no named groups, no replaceAll.
function redactLog(text, k, salt, cacheDir) {
    const tag = (kind, id) => kind + '#' + crypto.createHash('sha256').update(salt + id).digest('hex').slice(0, 4);
    const esc = (x) => x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const longest = (a, min) => a.filter((x, i) => typeof x === 'string' && x.length >= min && a.indexOf(x) === i).sort((x, y) => y.length - x.length);
    let s = String(text);
    // what the daemon knows, word for word (min lengths: a short value would shred ordinary words)
    longest(k.secrets || [], 6).forEach((x) => { s = s.split(x).join('[secret]'); });
    longest(k.titles || [], 6).forEach((x) => { s = s.split(x).join('[title]'); });
    const ids = k.ids || {};
    longest(Object.keys(ids), 10).forEach((x) => { s = s.split(x).join(tag(ids[x], x)); });
    // the Store's proxy by context, at any length and whichever it was: "through socks5 <host:port>", "proxy <host:port>:"
    s = s.replace(/(\bnetwork: through \w+ |\bproxy )(\[[0-9a-f:.]+\]|[^\s:\[\]]+):\d{1,5}\b/gi, '$1[proxy]');
    // tokens, cookies, e-mails
    s = s.replace(/\b(Bearer|OAuth)\s+[\w.~+\/-]+=*/gi, '$1 [secret]')
        .replace(/\bya29\.[\w.-]+/g, '[secret]').replace(/\b1\/\/[\w.-]{20,}/g, '[secret]')
        .replace(/\b((?:access_token|refresh_token|device_code|user_code|client_secret|id_token)["']?\s*[:=]\s*["']?)[^"'&\s,;}]+/gi, '$1[secret]')
        .replace(/\b(Set-Cookie|Cookie)\b\s*[:=].*/gi, '$1: [secret]')
        .replace(/\b(SAPISID|APISID|HSID|SSID|SID|LOGIN_INFO|VISITOR_INFO1_LIVE|YSC|__Secure-[\w-]+)=[^;\s"']+/g, '$1=[secret]')
        .replace(/[\w.+-]+@[\w-]+(?:\.[\w-]+)+/g, '[email]');
    // a URL: its scheme and host only (googlevideo URLs carry ip= and signatures; paths carry ids)
    s = s.replace(/\b([a-z][a-z0-9+.-]*:\/\/)(?:[^\/\s"'<>@]*@)?([^\/\s"'<>?#]+)[^\s"'<>]*/gi, '$1$2')
        // a path with a query and no scheme (urllib3's "with url: /videoplayback?...&ip=..."), a lone ip= value
        .replace(/\/[^\s"'()<>?]*\?[^\s"'()<>]+/g, '/[path]').replace(/\bip=[^&\s"'()]+/gi, 'ip=[ip]');
    // ids by their context: a cache path, the log lines that name one, yt-dlp's "[youtube] <id>:", JSON keys
    if (cacheDir) {
        s = s.replace(new RegExp(esc(cacheDir) + '([^\\s"\']*)', 'g'),
            (x, rest) => cacheDir + rest.replace(/\/([\w-]{11})(?=[\/.]|$)/g, (y, id) => '/' + tag('v', id)));
    }
    s = s.replace(/((?:\bv=|\bwatch\/|\/vi\/|youtu\.be\/|\/shorts\/|\/embed\/|\[youtube\]\s+|\b(?:fetch|thumb|prefetch|play|lru:)\s+|"(?:id|videoId|video_id)"\s*:\s*"))([\w-]{11})(?![\w-])/g,
        (x, pre, id) => pre + tag('v', id))
        .replace(/(lru removed )([\w#,-]+)/g, (x, pre, l) => pre + l.split(',').map((id) => (ID_RE.test(id) ? tag('v', id) : id)).join(','))
        .replace(/((?:\blist=|"playlistId"\s*:\s*"))([\w-]{2,64})/g, (x, pre, id) => pre + tag('p', id))
        .replace(/\bUC[\w-]{22}(?![\w-])/g, (id) => tag('c', id));
    // the car's VIN (17 characters, a letter among them), public IP addresses (v4: not 127/8, 10/8, 172.16/12, 192.168/16)
    s = s.replace(/\b[A-HJ-NPR-Z0-9]{17}\b/g, (x) => (/[A-Z]/.test(x) ? '[vin]' : x))
        .replace(/\b(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\b/g, (x, a, b) => {
            a = Number(a); b = Number(b);
            const own = a === 127 || a === 10 || a === 0 || (a === 192 && b === 168) || (a === 172 && b >= 16 && b <= 31) || (a === 169 && b === 254);
            return own || x.split('.').some((n) => Number(n) > 255) ? x : '[ip]';
        })
        .replace(/\b[0-9a-f]{1,4}(?::[0-9a-f]{0,4}){3,7}\b/gi, (x) => (/^(fe80|fc|fd)/i.test(x) ? x : '[ip]'));
    return s;
}
// the newest lines that fit in max bytes (a line on its own past max: none)
function lastBytes(ls, max) {
    let size = 0, i = ls.length;
    while (i > 0 && size + Buffer.byteLength(ls[i - 1]) + 1 <= max) size += Buffer.byteLength(ls[--i]) + 1;
    return ls.slice(i);
}
// a file's last n lines (from its last 256 KiB); [] when it can't be read
function tailLines(f, n) {
    try {
        const fd = fs.openSync(f, 'r');
        try {
            const size = fs.fstatSync(fd).size, len = Math.min(size, 256 << 10), b = Buffer.alloc(len);
            fs.readSync(fd, b, 0, len, size - len);
            const ls = b.toString('utf8').split('\n');
            if (len < size) ls.shift();   // cut mid-line
            return ls.filter(Boolean).slice(-n);
        } finally { fs.closeSync(fd); }
    } catch (e) { return []; }
}

// ---------------------------------------------------------------- daemon
function Daemon(o) {
    this.opts = Object.assign(defaults(), o || {});
    // every log line (the daemon's, Account's, Audio's: they all log through opts.log) also goes to a ring for
    // send_logs, and its last lines are kept for the app's Diagnostics page (diagLines): nobody reads a journal in a car
    const out = this.opts.log;
    this.ring = [];
    this.recent = [];
    this.opts.log = (m) => {
        this.ring.push(new Date().toISOString() + ' ' + m);
        if (this.ring.length > RING_MAX) this.ring.shift();
        this.recent.push(new Date().toISOString().slice(11, 19) + ' ' + String(m).replace(/\s+/g, ' ').slice(0, 240));
        if (this.recent.length > 40) this.recent.shift();
        out(m);
    };
    this.log = this.opts.log;
    this.seen = {};        // video/playlist/channel ids the app sent: a log report tags them (see report)
    this.reporting = false;
    this.state = 'idle';
    this.cur = null;      // {id, title, dir, rect, fill, gen}
    this.gen = 0;         // bumps on every play/stop: stale async work checks it
    this.client = null;
    this.child = null;
    this.deaths = 0;
    // VP9 failed on this car (ytdec exit 4, or a VP9 stream ytfetch could not read): kept in state/codec.json (setAvc1),
    // so later starts go straight to H.264 (deleting the file tries VP9 again)
    const codec = readJson(path.join(this.opts.stateDir, 'codec.json'));
    this.avc1Only = !!this.opts.h264Only || !!(codec && codec.avc1 === true);
    this.codecRetry = null; // the video last fetched again as H.264 after ytdec's exit 4 (once a video)
    this.videoOn = true;
    this.paused = false;
    this.elapsed = 0;
    this.total = 0;
    this.stoppedAt = null;
    this.loop = false;     // `loop` (the app's Repeat): at the audio's end the video plays again from 0 (loopAgain), no `ended`
    this.lastTimeEv = 0;
    this.sawPlay = false;
    this.ticking = false;
    this.preparing = null; // id the fetcher is writing
    this.fetcher = null;   // ytfetch.py, as the fetch uid
    this.ram = null;       // WP8: the RAM folder (start: pickRam), else null: the cache on flash
    this.fetchedMs = Infinity; // video fetched up to there (pts ms)
    this.ytdecKnew = Infinity; // fetchedMs when the running ytdec started (its index then)
    this.videoBehind = false; // its last seek went past the video fetched: a new ytdec at the clock once it is there
    this.buffering = false;
    this.head = null;      // {ms}: playing audio.head.m4a (the first ms of the audio) until audio.m4a is ready
    this.fullReady = false; // audio.m4a complete
    this.headWait = false; // past the head, waiting for audio.m4a: the player is paused, the state buffering
    this.switching = false; // head -> full: the player's events until the seek are the restart's, not ours
    this.replaying = null; // {gen} of the replayAt under way (onTime drops the restarting file's times)
    this.settle = null;    // {ms, until}: after the switch, ignore the player's stale times below ms
    this.lastTickAt = 0;
    this.thumbQ = [Promise.resolve(), Promise.resolve(), Promise.resolve()];
    this.thumbN = 0;
    this.timers = [];
    this.ready = new Promise((r) => { this.readyResolve = r; }); // leftover check done
    this.audio = new Audio(this.opts, {
        time: (v) => this.onTime(v),
        state: (v) => this.onPlayerState(v),
        error: (v) => this.onPlaybackError(v),
        // (a search for the way to sound takes a while: the app's player says what goes on)
        note: (t) => this.emit({ ev: 'note', msg: String(t) })
    });
    this.childFetch = !this.opts.ytRequest;
    if (this.childFetch) this.opts.ytRequest = (m, u, h, b, t) => this.fetch({ method: m, url: u, headers: h, body: b, timeoutMs: t });
    this.fetchUid = null;
    this.webUid = null;    // the phone remote's server (remote.js), picked when it is first turned on
    this.rc = new (require('./remote.js').Remote)(this.opts, {
        log: this.log, emit: (o) => this.emit(o), hasApp: () => !!this.client, shared: () => this.sockShared(), idRe: ID_RE,
        uid: () => this.remoteUid(),
        status: () => ({ state: this.state, id: this.cur ? this.cur.id : null, title: this.cur ? this.cur.title : null, elapsed: this.elapsed, total: this.total })
    });
    this.account = new Account(this.opts, (o) => this.emit(o), () => tvClient(this.hl, this.gl));
}
// the fetch uid (picked on first need; none: an error naming why, never a crash)
Daemon.prototype.uid = function () {
    if (this.fetchUid === null) {
        try { this.fetchUid = pickFetchUid(this.opts); } catch (e) { this.log('internet uid: ' + e.message); }
    }
    if (this.fetchUid === null) throw Object.assign(new Error('no internet uid on the car (see the log)'), { noUid: true });
    return this.fetchUid;
};
// youtube.com, as the fetch uid. The clock wrong: ytfetch.py makes the request (`request`, the same answer), because
// Node 8 has no way to skip only the certificate dates, and rejectUnauthorized:false would skip the whole check
Daemon.prototype.fetch = function (req) {
    try {
        // sign-in (refresh token, client secret) never crosses date-unchecked TLS: it fails with the clock error
        // instead, which is not invalid_grant, so the account stays signed in for when the clock is set
        const env = { childEnv: this.proxyEnv() };
        if (/^https:\/\/[^/]+\/o\/oauth2\//.test(String(req.url || '')) || !this.certTime()) return fetchRequest(Object.assign({}, this.opts, env), this.uid(), req);
        const o = Object.assign({}, this.opts, env, { fetchCmd: this.opts.fetcher.concat(['request', '--no-cert-time', '--cache-dir', this.ytc(), '--lease', this.opts.leaseFile]) });
        return fetchRequest(o, this.uid(), Object.assign({}, req, { headers: Object.assign({ 'User-Agent': TV_UA }, req.headers || {}) }));
    } catch (e) { return Promise.reject(e); }
};
// the head unit's date while its clock is wrong (TLS then skips the certificate dates), else null; asked at each
// fetcher spawn, since the clock may be set while driving. A change after hello is told to the app (`clock`)
Daemon.prototype.certTime = function () {
    const c = clockBehind((this.opts.now || Date.now)());
    if (this.clockSaid === undefined) return null;   // no hello on this connection: no app could warn, so dates checked
    if (this.clockSaid !== undefined && c !== this.clockSaid) {
        this.clockSaid = c;
        this.log(c ? 'clock wrong (' + c + '): certificate dates not checked' : 'clock right again: certificate dates checked');
        this.emit({ ev: 'clock', clock: c });
    }
    return c;
};
// a helper (ytfetch.py, thumb.js) as the fetch uid, as fetchRequest spawns its child
// WP8: the streaming fetcher's window follows playback, "pos <ms>" on its stdin at most once a second (at once for a seek).
// fedMax: the furthest point said since this fetch began; ytfetch.py punches only below fedMax - streamBehindMs.
Daemon.prototype.feedPos = function (ms, now) {
    const f = this.fetcher, c = this.cur;
    if (!f || !f.stdin || !c || !c.stream) return;
    const t = Date.now();
    if (!now && t - (this.posFedAt || 0) < 1000) return;
    this.posFedAt = t;
    ms = Math.max(0, Math.floor(ms));
    c.fedMax = Math.max(c.fedMax || 0, ms);
    try { f.stdin.write('pos ' + ms + '\n'); } catch (e) { /* it ended */ }
};
Daemon.prototype.spawnFetchUid = function (cmd, args, stdin) {
    const ids = this.opts.noSetuid ? {} : { uid: this.uid(), gid: this.uid() };
    return cp.spawn(cmd[0], cmd.slice(1).concat(args), Object.assign(ids, { cwd: '/', env: this.proxyEnv(), stdio: [stdin ? 'pipe' : 'ignore', 'pipe', 'pipe'] }));
};
// The Store's proxy as every fetch-uid child's env (proxy.js toEnv), read again at each spawn: a change in the Store
// applies to the next request. A change drops the spare (its yt-dlp was set up with the old one) and is logged by
// host:port alone; a setting that is wrong fails every request (never direct, which would bypass what the user set),
// unless the app's switch is off: then direct, whatever the Store holds.
Daemon.prototype.proxySetting = function () {
    const raw = proxy.readSetting(this.opts.storeConf), s = this.proxyMode() === 'off' ? null : raw;
    const key = JSON.stringify(proxy.toEnv(s));   // a new password counts as a change too
    if (key !== this.proxyKey) {
        if (this.proxyKey !== undefined) this.dropSpare();
        this.proxyKey = key;
        this.log('network: ' + (!s ? 'direct' : s.error ? s.error : 'through ' + s.proxy.kind + ' ' + s.proxy.label) + (s ? ' (the Store\'s proxy setting)'
            : this.proxyMode() === 'off' ? ' (the Store\'s proxy is off in RideTube)' : ''));
    }
    // the app's switch follows the Store until the user flips it: told when a proxy is set or cleared there
    const set = raw !== null;
    if (set !== this.proxyStore) { const told = this.proxyStore !== undefined; this.proxyStore = set; if (told) this.emit({ ev: 'proxy', mode: this.proxyMode(), store: set }); }
    return s;
};
Daemon.prototype.proxyEnv = function () { return proxy.toEnv(this.proxySetting()); };
// yt-dlp's cache dir (5 chars: listCache's 11-char id filter skips it), the fetch uid's. Root makes and chowns only
// this folder (in root's cache/, so it cannot be swapped); everything below it (info/, yt-dlp's files) is the fetcher's
Daemon.prototype.ytc = function () {
    const ytc = path.join(this.opts.cacheDir, 'ytdlp');
    if (!fs.existsSync(ytc)) fs.mkdirSync(ytc, 0o700);
    this.chownFetch(ytc);
    return ytc;
};
// ---- the account's history and resume point, as SmartTube keeps them: /player (signed in) gives the playbackTracking
// URLs; a playback ping at the start, watchtime pings (st..et) every minute, on pause and at the stop (final when done)
Daemon.prototype.trackStart = async function (c) {
    if (this.account.status().state !== 'signed_in') return;
    try {
        const r = await this.account.json('POST', YT + '/youtubei/v1/player', { context: { client: TV_CLIENT }, videoId: c.id },
            { Authorization: 'Bearer ' + await this.account.token(false) });
        const pt = (r.j && r.j.playbackTracking) || {};
        const pb = pt.videostatsPlaybackUrl && pt.videostatsPlaybackUrl.baseUrl, wt = pt.videostatsWatchtimeUrl && pt.videostatsWatchtimeUrl.baseUrl;
        if (!/^https:\/\/s\.youtube\.com\//.test(pb) || !/^https:\/\/s\.youtube\.com\//.test(wt) || this.cur !== c) return;
        const A = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
        c.track = { pb: pb, wt: wt, st: 0, at: Date.now(), cpn: Array.from(require('crypto').randomBytes(16), (b) => A[b & 63]).join('') };
        await this.ping(c, false);
    } catch (e) { if (!e.signedOut) this.log('history: ' + e.message); }
};
// watch: false = the playback ping (the start), true = a watchtime ping up to now
Daemon.prototype.ping = async function (c, watch, final) {
    const t = c.track;
    if (!t) return;
    const len = Math.round((c.durationMs || this.total || 0) / 1000), pos = Math.max(0, this.elapsed / 1000).toFixed(1);
    const q = { ns: 'yt', ver: 2, cpn: t.cpn, len: len, cmt: pos };
    if (watch) { q.st = t.st; q.et = pos; t.st = pos; }
    if (final) { q.final = 1; if (watch) q.st = q.et = q.cmt = len; }
    t.at = Date.now();
    const url = (watch ? t.wt : t.pb) + '&' + Object.keys(q).map((k) => k + '=' + encodeURIComponent(q[k])).join('&');
    try {
        const r = await this.opts.ytRequest('GET', url, { Authorization: 'Bearer ' + await this.account.token(false), Referer: 'https://www.youtube.com/tv' }, null, 8000);
        if (r.status >= 300) this.log('history ping ' + r.status);
    } catch (e) { this.log('history ping: ' + e.message); }
};

// ---- where each video was left (positions.json, kept after its files go): resume, Continue watching, progress bars
Daemon.prototype.positions = function () {
    if (!this.pos) this.pos = readJson(path.join(this.opts.cacheDir, 'positions.json')) || {};
    return this.pos;
};
Daemon.prototype.savePos = function (force) {
    const c = this.cur;
    if (!c || !this.total || (this.head || this.headFile) && !c.durationMs) return;   // (the head's own ~60 s total is no length)
    if (!force && Date.now() - (this.posAt || 0) < 15000) return;
    this.posAt = Date.now();
    const p = this.positions(), m = readMeta(c.dir, c.id);
    p[c.id] = { ms: this.elapsed, total: this.total, done: this.total - this.elapsed < Math.max(5000, this.total * 0.05),
        at: Date.now(), title: c.title || m.title, channel: m.channel, duration_s: Math.round(this.total / 1000) };
    const ids = Object.keys(p).sort((a, b) => p[b].at - p[a].at);
    ids.slice(300).forEach((id) => { delete p[id]; });
    try {
        const f = path.join(this.opts.cacheDir, 'positions.json');
        fs.writeFileSync(f + '.tmp', JSON.stringify(p), { mode: 0o600 }); // the watch history: root only
        fs.renameSync(f + '.tmp', f);
    } catch (e) { this.log('positions: ' + e.message); }
};
Daemon.prototype.sendPositions = function () {
    const p = this.positions();
    this.emit({ ev: 'positions', items: Object.keys(p).sort((a, b) => p[b].at - p[a].at).map((id) => Object.assign({ id: id }, p[id])) });
};
// ---- chapters (ytfetch's meta) and SponsorBlock segments of the playing video, sent as one 'extras' event
Daemon.prototype.sendExtras = function () {
    const c = this.cur;
    if (c) this.emit({ ev: 'extras', id: c.id, chapters: c.chapters || [], segments: (c.segs || []).map((x) => [x.s, x.e, x.cat]) });
};
Daemon.prototype.loadSegments = async function (c) {
    // root's own folder: a sb.json in the video's folder (0.3.x) is the fetch uid's to plant a symlink in, never read
    const sb = path.join(this.opts.cacheDir, SB_DIR), f = path.join(sb, c.id + '.json');
    let segs = readJson(f);
    if (!Array.isArray(segs)) {
        const pre = require('crypto').createHash('sha256').update(c.id).digest('hex').slice(0, 4);
        const url = this.opts.sponsorBlock + pre + '?categories=' + encodeURIComponent(JSON.stringify(this.opts.sbCategories)) + '&actionTypes=' + encodeURIComponent('["skip"]');
        let r;
        try { r = await this.opts.ytRequest('GET', url, { 'User-Agent': 'jap-ridetube-d (BMW head unit)' }, null, 8000); } catch (e) { this.log('sponsorblock: ' + e.message); return; }
        segs = [];
        if (r.status === 200) {
            let all = [];
            try { all = JSON.parse(r.body); } catch (e) { /* none */ }
            const v = Array.isArray(all) && all.find((x) => x && x.videoID === c.id);
            ((v && v.segments) || []).forEach((g) => {
                if (g && g.actionType === 'skip' && this.opts.sbCategories.indexOf(g.category) >= 0 && Array.isArray(g.segment))
                    segs.push({ s: Math.round(g.segment[0] * 1000), e: Math.round(g.segment[1] * 1000), cat: g.category });
            });
        } else if (r.status !== 404) return; // 404: none for this video (cached as such); else asked again next play
        try {
            if (!fs.existsSync(sb)) fs.mkdirSync(sb, 0o700);
            fs.writeFileSync(f, JSON.stringify(segs), { mode: 0o600 });
        } catch (e) { this.log('sponsorblock cache: ' + e.message); }
    }
    if (this.cur !== c) return;
    c.segs = segs.filter((x) => x && x.e - x.s >= 1000).sort((a, b) => a.s - b.s);
    if (c.segs.length) this.sendExtras();
};
// a segment the clock entered: seek past it, once per play (a seek back into it plays it)
Daemon.prototype.skipSegments = function () {
    const skip = (this.cur && this.cur.skip) || this.opts.skip;   // the others: the app's (a Skip button, or nothing)
    const seg = ((this.cur && this.cur.segs) || []).find((x) => !x.done && skip.indexOf(x.cat) >= 0 && this.elapsed >= x.s && this.elapsed < x.e - 1000);
    if (!seg) return false;
    seg.done = true;
    this.log('skip ' + seg.cat + ' ' + seg.s + '-' + seg.e);
    this.emit({ ev: 'skipped', category: seg.cat, ms: seg.e - seg.s });
    this.seek(seg.e).catch((e) => this.log('skip: ' + e));
    return true;
};

// ---- speed: a warm ytfetch.py (yt-dlp loaded: ~1.2 s on the car) takes the next job, and the stream lookup
// (~2.5 s) runs while the controller rests on a tile (prefetch), so a play skips both
Daemon.prototype.spawnJob = function (job) {
    if (this.certTime()) job = [job[0], '--no-cert-time'].concat(job.slice(1));
    this.proxySetting();   // a Store proxy change since the spare started drops it
    const s = this.spare;
    this.spare = null;
    if (s && s.child.exitCode === null && s.child.signalCode === null) { // still loading: serve reads the job once warm
        if (job.indexOf('--stream') >= 0) s.child.stdin.write(JSON.stringify(job) + '\n');   // (WP8: it reads "pos" lines after it)
        else s.child.stdin.end(JSON.stringify(job) + '\n');
        return s.child;
    }
    if (s) { try { s.child.kill('SIGKILL'); } catch (e) { /* gone */ } }
    return this.spawnFetchUid(this.opts.fetcher, job, job.indexOf('--stream') >= 0);
};
// from tick: a spare while the app is in use and no fetch is starting; none past warmMs
Daemon.prototype.keepWarm = function () {
    const want = this.opts.spare && this.client && Date.now() < (this.usedAt || 0) + this.opts.warmMs;
    if (!want) {
        if (this.spare) { try { this.spare.child.kill('SIGKILL'); } catch (e) { /* gone */ } this.spare = null; }
        return;
    }
    if (this.spare || (this.preparing && this.state === 'preparing')) return;
    const px = this.proxySetting();
    if (px && px.error) return;   // serve stops at once on a wrong proxy setting: no spare, or one would start every tick
    let child;
    try { child = this.spawnFetchUid(this.opts.fetcher, ['serve', '--cache-dir', this.ytc()].concat(this.pyzArgs()), true); } catch (e) { this.log('spare: ' + e.message); return; }
    const s = this.spare = { child: child, warm: false };
    child.stdin.on('error', () => { /* it died: close says */ });
    readline.createInterface({ input: child.stdout }).on('line', (l) => { if (/"ev": ?"warm"/.test(l)) s.warm = true; });
    child.on('error', (e) => this.log('spare: ' + e.message));
    child.on('close', () => { if (this.spare === s) this.spare = null; });
};

// ---- yt-dlp updates: ytfetch.py update (as the fetch uid) puts the newest yt-dlp release, checked and loaded once, in
// <ytc>/pyz and names it in current.json; fetch, extract and serve load it (--pyz) instead of the service's ytdlp.pyz
// while updates are on (the app's setting, kept in state/ytdlp.json; on by default). Looked for ytdlpFirstMs after the
// start, then daily, an hour after a failed look, and after a failed play (the usual sign of a YouTube change) once
// ytdlpRetryMs has passed; only with the uplink's lease (online) and not while a play is preparing.
Daemon.prototype.ytdlpDir = function () { return path.join(this.opts.cacheDir, 'ytdlp', 'pyz'); };
Daemon.prototype.ytdlpAuto = function () {
    if (this.ytdlpOn === undefined) { const s = readJson(path.join(this.opts.stateDir, 'ytdlp.json')); this.ytdlpOn = !(s && s.auto === false); }
    return this.ytdlpOn;
};
// ['--pyz', the update] while on and there (only a file inside ytdlpDir), else [] (ytdlp.pyz)
// current.json is the fetch uid's: read as readFetchFile reads (no symlink, a small regular file); the pyz is only
// ever passed to a fetch-uid child (as --pyz), never read or run by root
Daemon.prototype.ytdlpCurrent = function () {
    const b = readFetchFile(path.join(this.ytdlpDir(), 'current.json'), (n) => n <= 4096);
    try { const c = b && JSON.parse(b.toString('utf8')); return c && typeof c === 'object' ? c : null; } catch (e) { return null; }
};
Daemon.prototype.pyzArgs = function () {
    if (!this.ytdlpAuto()) return [];
    const dir = this.ytdlpDir(), cur = this.ytdlpCurrent();
    const p = cur && typeof cur.pyz === 'string' ? path.resolve(cur.pyz) : '';
    return p.indexOf(dir + path.sep) === 0 && fs.existsSync(p) ? ['--pyz', p] : [];
};
Daemon.prototype.sendYtdlp = function () {
    const args = this.pyzArgs(), cur = args.length ? this.ytdlpCurrent() : null;
    const v = cur && typeof cur.version === 'string' && /^\d{4}\.\d\d\.\d\d(\.\d+)?$/.test(cur.version) ? cur.version : null;
    return this.emit({ ev: 'ytdlp', auto: this.ytdlpAuto(), version: v || this.ytdlpBundled || null,
        bundled: this.ytdlpBundled || null, checking: !!this.ytdlpRun });
};
Daemon.prototype.dropSpare = function () {
    if (this.spare) { try { this.spare.child.kill('SIGKILL'); } catch (e) { /* gone */ } this.spare = null; }
};
Daemon.prototype.setYtdlpAuto = function (on) {
    if (on !== this.ytdlpAuto()) {
        this.ytdlpOn = on;
        try { if (!fs.existsSync(this.opts.stateDir)) { mkdirp(path.dirname(this.opts.stateDir)); fs.mkdirSync(this.opts.stateDir, 0o700); } writeIfChanged(path.join(this.opts.stateDir, 'ytdlp.json'), JSON.stringify({ auto: on }) + '\n', 0o644); } catch (e) { this.log('yt-dlp updates: ' + e.message); }
        this.log('yt-dlp updates ' + (on ? 'on' : 'off'));
        this.dropSpare();   // it loaded the other yt-dlp
        if (on) this.ytdlpNext = Math.min(this.ytdlpNext || 0, Date.now() + this.opts.ytdlpFirstMs);
    }
    return this.sendYtdlp();
};
// a play failed after its retries (not a refused video): look now, unless a look ran within ytdlpRetryMs
Daemon.prototype.ytdlpSoon = function () {
    if (Date.now() - (this.ytdlpAt || 0) >= this.opts.ytdlpRetryMs) this.ytdlpNext = 0;
};
Daemon.prototype.ytdlpTick = function () {
    if (this.ytdlpRun || Date.now() < this.ytdlpNext || !this.ytdlpAuto()) return;
    if (this.state === 'preparing' || !fs.existsSync(this.opts.leaseFile)) return;   // a play is fetching, or offline
    this.updateYtdlp();
};
Daemon.prototype.updateYtdlp = function () {
    this.ytdlpAt = Date.now();
    this.ytdlpNext = this.ytdlpAt + this.opts.ytdlpRetryMs;   // a failed look: again in an hour (one that answered: a day)
    let child;
    try { child = this.spawnFetchUid(this.opts.fetcher, ['update', '--cache-dir', this.ytc()]); } catch (e) { return this.log('yt-dlp update: ' + e.message); }
    const run = this.ytdlpRun = { child: child };
    const kill = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) { /* gone */ } }, this.opts.ytdlpMaxMs);
    let last = null;
    readline.createInterface({ input: child.stdout }).on('line', (l) => { try { last = JSON.parse(l); } catch (e) { /* not an event */ } });
    readline.createInterface({ input: child.stderr }).on('line', (l) => this.log('yt-dlp update: ' + l));
    child.on('error', (e) => this.log('yt-dlp update: ' + e.message));
    child.on('close', (code) => {
        clearTimeout(kill);
        if (this.ytdlpRun !== run) return undefined;
        this.ytdlpRun = null;
        // a release that fails its checks (sha256, does not load or compile) fails the same way in an hour: a day
        if (last && last.ev === 'error' && /^yt-dlp \d/.test(last.msg || '')) this.ytdlpNext = Date.now() + this.opts.ytdlpEveryMs;
        if (!last || last.ev !== 'ytdlp') return this.log('yt-dlp update: ' + ((last && last.msg) || 'exit ' + code));
        this.ytdlpNext = Date.now() + this.opts.ytdlpEveryMs;
        const ver = (v) => (typeof v === 'string' && /^\d{4}\.\d\d\.\d\d(\.\d+)?$/.test(v) ? v : null);   // the fetch uid's words
        this.ytdlpBundled = ver(last.bundled);
        this.log('yt-dlp update: ' + (last.state === 'updated' ? 'updated' : 'current') + ' ' + ver(last.version) + ' (the service has ' + this.ytdlpBundled + ')');
        if (last.state === 'updated') this.dropSpare();   // the next one loads the new yt-dlp
        return this.sendYtdlp();
    });
    return this.sendYtdlp();
};
Daemon.prototype.infoFile = function (id) { return path.join(this.opts.cacheDir, 'ytdlp', 'info', id + '.json'); };
Daemon.prototype.freshInfo = function (id) {
    try { return Date.now() - fs.statSync(this.infoFile(id)).mtime.getTime() < this.opts.infoMaxMs; } catch (e) { return false; }
};
Daemon.prototype.prefetch = function (id) {
    if (!ID_RE.test(String(id)) || !this.opts.spare) return undefined;
    if (this.extracting) { if (this.extracting.id !== id) this.nextPrefetch = id; return undefined; }
    if (isPlayable(path.join(this.opts.cacheDir, id)) || this.preparing === id || this.freshInfo(id)) return undefined;
    // info/ and its stale files: the fetcher makes and prunes them (extract), root never writes below ytdlp/
    const child = this.spawnJob(['extract', '--cache-dir', this.ytc()].concat(this.pyzArgs(), ['--', id, this.infoFile(id)]));
    this.keepWarm(); // the play after this lookup finds a spare, not a cold start (the 5 s poll was too late)
    const x = this.extracting = { id: id, child: child, done: null };
    x.done = new Promise((r) => child.on('close', r)).then(() => {
        if (this.extracting === x) this.extracting = null;
        const n = this.nextPrefetch;
        this.nextPrefetch = null;
        if (n) this.prefetch(n);
    });
    child.on('error', (e) => this.log('prefetch: ' + e.message));
    child.stdout.resume();
    readline.createInterface({ input: child.stderr }).on('line', (l) => this.log('ytfetch (prefetch): ' + l));
    return undefined;
};
// hands a folder root just made to the fetch uid. chownSync follows symlinks, so only a direct child of root's
// cache/ (nobody else can swap it); never a path below a fetch-owned folder (a symlink there: any file to the fetch uid)
// (WP8: or a direct child of the RAM folder, which pickRam checked is root's own real folder, as checkCacheDir does cache/)
Daemon.prototype.chownFetch = function (p) {
    const up = path.dirname(path.resolve(p));
    if (up !== path.resolve(this.opts.cacheDir) && !(this.ram && up === path.resolve(this.ram))) throw new Error('chownFetch outside the cache: ' + p);
    if (!this.opts.noSetuid) fs.chownSync(p, this.uid(), this.uid());
};
// a folder under cache/: a fetch-uid tree is emptied by its owner (rm -rf runs as that uid, so a symlink in it
// reaches nothing that uid could not already touch), then root removes the empty folder (rmdir: never follows one).
// A root-owned tree (or a file) as before. Throws if it stays (logged by the callers), never falls back to root's walk.
// ---- WP8: the video window in RAM. ramMount: dir is itself a tmpfs mount point (/proc/mounts' text), so the unit's
// size cap holds (a folder on /run or /dev/shm would be an uncapped tmpfs); pickRam: ramDir so, made 0711 as the cache
// (the media engine passes through to a video's folder), else null (the window goes to flash)
function ramMount(dir, mounts) {
    return String(mounts || '').split('\n').some((l) => {
        const f = l.split(' ');
        return f.length >= 3 && f[1].replace(/\\040/g, ' ') === dir && f[2] === 'tmpfs';
    });
}
Daemon.prototype.pickRam = function () {
    const o = this.opts, d = o.ramDir;
    let mounts = '';
    try { mounts = fs.readFileSync('/proc/mounts', 'utf8'); } catch (e) { /* not Linux: none unless ramCheck is off */ }
    if (!d) return null;
    if (o.ramCheck && !ramMount(d, mounts)) { this.log('ram dir ' + d + ': not a tmpfs mount point: not used'); return null; }
    try {
        mkdirp(d);
        // root's own real folder, as cache/ (checkCacheDir): a folder or a symlink put there first would point
        // chownFetch's and every root step's video folders elsewhere
        const st = fs.lstatSync(d);
        if (!st.isDirectory()) throw new Error('not a folder (a symlink?)');
        if (st.uid !== process.getuid()) throw new Error('owned by uid ' + st.uid);
        fs.chmodSync(d, 0o711);
        return d;
    } catch (e) { this.log('ram dir ' + d + ': ' + e.message + ': not used'); }
    return null;
};
Daemon.prototype.rmTree = function (p) {
    let st;
    try { st = fs.lstatSync(p); } catch (e) { return; }
    if (!st.isDirectory() || st.uid === 0) { rmrf(p); return; }   // root's own: rm never runs as root
    if (inRanges(this.appRanges(), st.uid)) throw new Error('rm ' + p + ': owned by app uid ' + st.uid + ', left alone');
    // the owner (the fetch uid, or one it had before); its gid as every fetch spawn's: the same number
    const ids = this.opts.noSetuid ? {} : { uid: st.uid, gid: st.uid };
    const r = cp.spawnSync('/bin/rm', ['-rf', '--', p], Object.assign(ids, { cwd: '/', env: {}, stdio: 'ignore', timeout: 30000 }));
    if (r.error) this.log('rm ' + p + ': ' + r.error.message);   // (exit 1 is normal: it cannot remove p itself, root's cache/ holds it)
    try { fs.rmdirSync(p); } catch (e) { if (e.code !== 'ENOENT') throw e; }
};

// cache/ must be root's own real folder, else a swapped one points every root step in it elsewhere: refuse to start.
// 0711: the fetch uid reaches its own folders in it, nobody else can list what was watched (a wider mode is fixed)
Daemon.prototype.checkCacheDir = function () {
    const dir = this.opts.cacheDir, me = process.getuid();   // root on the car (a test: its own uid)
    try { mkdirp(dir); } catch (e) { this.log('cache dir: ' + e.message); }
    const refuse = (why) => Object.assign(new Error('cache dir ' + dir + ' ' + why + ': not starting'), { refuse: true });
    let st;
    try { st = fs.lstatSync(dir); } catch (e) { throw refuse(e.message); }
    if (!st.isDirectory()) throw refuse('is not a folder (a symlink?)');
    if (st.uid !== me) throw refuse('is owned by uid ' + st.uid + ', not ' + me);
    if (st.mode & 0o022) this.log('SECURITY: cache dir ' + dir + ' was ' + (st.mode & 0o777).toString(8) + ' (group/other writable): set to 711');
    fs.chmodSync(dir, 0o711);
};
// MainApp's app uid ranges (mainappRanges), kept once read: they do not change while it runs. null: unknown
Daemon.prototype.appRanges = function () {
    if (!this.ranges) this.ranges = mainappRanges(this.opts);
    return this.ranges;
};
Daemon.prototype.start = async function () {
    this.checkCacheDir();
    try { mkdirp(this.opts.logDir); } catch (e) { this.log('log dir: ' + e.message); }
    this.ram = this.pickRam();
    this.log(this.ram ? 'video windows in RAM: ' + this.ram : 'video windows on flash: no tmpfs mounted at ' + this.opts.ramDir);
    this.listen();
    // ytdec decodes on the GPU with the unit's Intel Media SDK (measured on iDrive 7). A head unit without it still
    // plays the sound (ytdec fails to start twice: "video failed, audio only"); the log says why up front.
    if (!this.opts.noHwCheck) {
        const mfx = ['/usr/lib/libmfxhw64.so', '/usr/lib/libmfxhw64.so.1', '/usr/lib64/libmfxhw64.so'].filter((p) => fs.existsSync(p));
        this.decoder = mfx.length ? 'Intel Media SDK ' + mfx[0] : 'no Intel Media SDK (libmfxhw64) on this head unit - the picture may not show, the sound plays';
        this.log('video decoder: ' + this.decoder);
    }
    this.rc.cleanup();   // a phone remote rule an earlier run left (a crash): removed; nothing listens until it is turned on
    // the uplink learns our fetch uid at start (its 5 s rescan), before anyone signs in
    if (this.childFetch && this.fetchUid === null) { try { this.fetchUid = pickFetchUid(this.opts); } catch (e) { this.log('internet uid: ' + e.message); } }
    this.timers.push(setInterval(() => this.guard(() => this.watchdog()), 1000));
    this.timers.push(setInterval(() => this.guard(() => this.keepWarm()), this.opts.tickMs));
    this.timers.push(setInterval(() => this.guard(() => this.lockSocket()), this.opts.tickMs));   // a reinstall can change the app's uid
    this.ytdlpNext = Date.now() + this.opts.ytdlpFirstMs;
    this.timers.push(setInterval(() => this.guard(() => this.ytdlpTick()), this.opts.tickMs));
    this.timers.push(setInterval(() => this.guard(() => this.downloadTick()), this.opts.tickMs));
    this.keepWarm();
    this.prune();
    // The SOME/IP services may come up after us at boot: retry until the leftover check ran.
    // Tried every audioRetryMs for the first minute, then six times as far apart (a head unit that lacks something
    // will not grow it); a play or the Diagnostics page asks for a try at once (audioKick). The reason is logged when
    // it changes and every 12th try; the page shows the last one.
    let said = null;
    await this.waitForBoot();
    for (let i = 0; !this.closed; i++) {
        try {
            await this.audio.start();
            this.audioKick = null;
            this.readyResolve();
            if (i) this.log('audio ready after ' + (i + 1) + ' tries');
            // a head unit that does not offer the player: how its own apps play media is read, once, a while after
            // the start and in the background, for the Diagnostics page
            if (this.opts.probe && this.audio.diag.mp && this.audio.diag.mp.available === false) {
                const t = setTimeout(() => { this.scanCode().catch(() => {}); }, this.opts.codeScanDelayMs);
                if (t.unref) t.unref();
                this.timers.push(t);
            }
            break;
        } catch (e) {
            const msg = errText(e);
            if (msg !== said || i % 12 === 0) this.log('someip not ready: ' + msg);
            said = msg;
            await new Promise((r) => {
                const t = setTimeout(r, this.opts.audioRetryMs * (i < 12 ? 1 : 6));
                this.audioKick = () => { clearTimeout(t); this.audioKick = null; r(); };
            });
        }
    }
};
/* On iDrive 8: nothing is asked of the car until the head unit has been up settleS seconds (its own apps and the
 * Store's start first). The ctl socket is up meanwhile: search and the pages work, a play says what it waits for. */
Daemon.prototype.waitForBoot = async function () {
    const want = this.opts.settleS || 0;
    const up = () => { try { return parseFloat(fs.readFileSync(this.opts.uptimeFile, 'utf8')); } catch (e) { return Infinity; } };
    let left = want - up();
    if (!(left > 0)) return;
    this.log('waiting ' + Math.ceil(left) + ' s before the car\'s audio is asked for: the head unit is starting');
    let said = 0;
    while (!this.closed && (left = want - up()) > 0) {
        this.audio.diag.stage = 'waiting for the head unit to finish starting (' + Math.ceil(left) + ' s more)';
        // (a play asked meanwhile waits for it - audioReady: the app's player says for how long)
        if (this.state === 'preparing' && Date.now() - said > 4000) { said = Date.now(); this.emit({ ev: 'note', msg: 'The car is still starting: the sound can be asked for in ' + Math.ceil(left) + ' s...' }); }
        await sleep(Math.min(1000, left * 1000 + 20));
    }
};
// The audio up (the leftover sweep done) within ms; else the reason the car's audio is not there, for the app's
// player. A start that is waiting for its next try makes it now.
Daemon.prototype.audioReady = function (ms) {
    if (this.audioKick) this.audioKick();
    // a play while the head unit is still starting (iDrive 8: the service keeps away for settleS) waits for the start
    // to be over, not 10 s: the driver sees the player loading, not an error to press again
    if (!this.audio.diag.ready) {
        let up = Infinity;
        try { up = parseFloat(fs.readFileSync(this.opts.uptimeFile, 'utf8')); } catch (e) { /* unknown: no wait to add */ }
        const left = (this.opts.settleS || 0) - up;
        if (left > 0) ms += left * 1000 + (this.opts.audioStartMs === undefined ? 15000 : this.opts.audioStartMs);
    }
    return withTimeout(this.ready, ms, 'audio service').catch(() => { throw new Error(this.audio.why()); });
};
// ---- diagnostics: the app's Settings > Diagnostics page (nobody reads a journal in a car; a photo of the page tells
// what a head unit other than the measured iDrive 7 has)
/* Files named like re under roots: a walk bounded in entries, depth and time, for the page only (at most 10 found).
 * Folders are told from files by lstat, spared for names that end like a file's. */
function findNamed(roots, re, lim) {
    const out = [], end = Date.now() + lim.ms;
    let left = lim.entries;
    const walk = (dir, depth) => {
        let names;
        try { names = fs.readdirSync(dir); } catch (e) { return; }
        for (const n of names) {
            if (--left <= 0 || out.length >= 10 || Date.now() > end) return;
            const p = path.join(dir, n);
            if (re.test(n)) { out.push(p); continue; }
            if (depth <= 0 || n === 'node_modules' || n[0] === '.' || /\.(so(\.\d+)*|js|json|node|py|pyc|png|jpg|svg|ttf|otf|a|o|h|la|pc|conf|txt|xml|html|css|map|bin|dat|gz|tgz|zip|qm|qml|mo)$/i.test(n)) continue;
            let st;
            try { st = fs.lstatSync(p); } catch (e) { continue; }
            if (st.isDirectory()) walk(p, depth - 1);
        }
    };
    roots.forEach((r) => walk(r, lim.depth));
    return out;
}
/* Where this head unit keeps, or does not keep, what the sound needs: lines for the page. Read when asked; search:
 * also look for the descriptions and a catalog anywhere under o.searchRoots (asked when one is missing). */
function audioInventory(o, search) {
    const out = [];
    const short = (n) => n.replace(/\.json$/, '').replace(/^de[_.]bmw[_.]infotainment[_.]/, '');
    const cap = (l, n) => l.slice(0, n).join(', ') + (l.length > n ? ' (+' + (l.length - n) + ' more)' : '');
    const ours = (n) => n === CC_FILE || n === MP_FILE || CC_LIKE.test(n) || MP_LIKE.test(n);
    // the audio manager's and the players' first, then anything else of the audio
    const audio = (names) => names.filter((n) => AUDIO_LIKE.test(n)).sort((a, b) => (ours(b) ? 1 : 0) - (ours(a) ? 1 : 0) || (a < b ? -1 : a > b ? 1 : 0));
    const ls = (d) => { try { return fs.readdirSync(d).sort(); } catch (e) { return null; } };
    (o.mainapps || []).forEach((m) => {
        const top = ls(m.root);
        if (!top) return;
        const gen = ls(path.join(m.root, 'src-gen'));
        out.push('App manager ' + m.root + ' (' + m.hmi + ')' + (gen ? '; src-gen has: ' + cap(gen.filter((n) => !/\.json$/.test(n)), 10) : '; no src-gen in it (it has: ' + cap(top, 10) + ')'));
    });
    (o.srcGen ? [o.srcGen] : o.srcGens || SRC_GENS).forEach((d) => {
        const all = ls(d);
        if (!all) { out.push(d + ': not there'); return; }
        const names = all.filter((n) => /\.json$/.test(n));
        const like = audio(names);
        out.push(d + ': ' + names.length + ' descriptions, ' + (names.indexOf(CC_FILE) >= 0 ? 'CommandControl yes' : 'CommandControl NO') + ', ' +
            (names.indexOf(MP_FILE) >= 0 ? 'MediaPlayer yes' : 'MediaPlayer NO') + (like.length ? '; of the audio: ' + cap(like.map(short), 14) : ''));
    });
    const homes = (o.oapRoot && ls(o.oapRoot) || []).filter((n) => /^oap_\d+$/.test(n));
    const withOurs = [];
    homes.forEach((h) => {
        const hit = (ls(path.join(o.oapRoot, h, 'someipInterfaces')) || []).filter(ours);
        if (!hit.length) return;
        withOurs.push(h + ' ' + hit.map((n) => {
            const i = instancesOf(readJsonCapped(path.join(o.oapRoot, h, 'someipInterfaces', n), MAX_IFACE_JSON));
            return short(n) + (i ? ' [' + cap(i, 5) + ']' : '');
        }).join(' + '));
    });
    out.push('Installed apps: ' + homes.length + '; with a description of the audio manager or the player: ' + (withOurs.length ? cap(withOurs, 5) : 'none'));
    (o.catalogs || []).forEach((c) => {
        if (!fs.existsSync(c)) {
            // the nearest folder above that is there, and what it holds
            let up = path.dirname(c), has = ls(up);
            for (let i = 0; !has && i < 2; i++) { up = path.dirname(up); has = ls(up); }
            out.push('Interface catalog ' + c + ': not there' + (has ? ' (' + up + ' has: ' + (cap(has, 10) || 'nothing') + ')' : ''));
            return;
        }
        const db = readJsonCapped(c, MAX_CATALOG);
        const ids = db && db.interfaces && typeof db.interfaces === 'object' ? Object.keys(db.interfaces) : null;
        if (!ids) { out.push('Interface catalog ' + c + ': there, but not readable as one'); return; }
        const like = ids.filter((n) => AUDIO_LIKE.test(n) && n !== CC_ID && n !== MP_ID).sort();
        out.push('Interface catalog ' + c + ': ' + ids.length + ' interfaces, CommandControl ' + (ids.indexOf(CC_ID) >= 0 ? 'yes' : 'NO') + ', MediaPlayer ' +
            (ids.indexOf(MP_ID) >= 0 ? 'yes' : 'NO') + (like.length ? '; others of the audio: ' + cap(like.map(short), 60) : ''));
    });
    const gens = (o.generators || []).filter((g) => fs.existsSync(g));
    gens.forEach((g) => {
        let text = '';
        try { if (fs.statSync(g).size <= MAX_CATALOG) text = fs.readFileSync(g, 'utf8'); } catch (e) { /* said below */ }
        out.push('Generator ' + g + ': ' + (/someip-interfaces-generator\/IFConfigGenerator/.test(text) ? 'has the interface generator' : 'no interface generator in it'));
    });
    if (!gens.length && (o.generators || []).length) out.push('Generator: none of ' + o.generators.join(', '));
    if (search && (o.searchRoots || []).length) {
        const known = (o.srcGen ? [o.srcGen] : o.srcGens || SRC_GENS).concat(o.catalogs || []);
        const found = findNamed(o.searchRoots, /(CommandControl|MediaPlayer)\d*\.json$|^someip-interfaces.*\.json$/i, { ms: 1500, entries: 60000, depth: 7 })
            .filter((f) => known.indexOf(f) < 0 && known.indexOf(path.dirname(f)) < 0);
        out.push('Elsewhere under ' + o.searchRoots.join(', ') + ': ' + (found.length ? found.join(', ') : 'no file named like them'));
    }
    return out;
}
/* Who the unit's SOME/IP security names for a service: the policy files under dirs (vsomeip's; iDrive 8 keeps a
 * folder <uid>_<gid> per client) read for the services [{ name, id }]. A client is the <uid>_<gid> folder a file
 * is in, else its policy's credentials, else the file. Lines for the page. */
function someipPolicies(dirs, services) {
    const out = [], files = [];
    const num = (v) => (typeof v === 'number' ? v : typeof v === 'string' && /^(0x[0-9a-f]+|\d+)$/i.test(v.trim()) ? Number(v.trim()) : null);
    const cap = (l, n) => l.slice(0, n).join(', ') + (l.length > n ? ' (+' + (l.length - n) + ' more)' : '');
    const walk = (dir, top, depth) => {
        let names;
        try { names = fs.readdirSync(dir).sort(); } catch (e) { return; }
        names.forEach((n) => {
            if (files.length >= 800) return;
            const p = path.join(dir, n);
            let st;
            try { st = fs.statSync(p); } catch (e) { return; }
            if (st.isDirectory()) { if (depth > 0) walk(p, top, depth - 1); } else if (st.isFile() && st.size <= (2 << 20)) files.push({ p: p, rel: path.relative(top, p) });
        });
    };
    const there = dirs.filter((d) => fs.existsSync(d));
    if (!there.length) return ['SOME/IP security: no policies (' + dirs.join(', ') + ' not there)'];
    there.forEach((d) => walk(d, d, 3));
    const clients = {}, flags = {};
    const hits = services.map(() => ({ requests: {}, offers: {}, other: {} }));
    files.forEach((f) => {
        const j = readJsonCapped(f.p, 2 << 20);
        if (j === undefined) return;
        const folder = /^(\d+_\d+)(\/|$)/.exec(f.rel.split(path.sep).join('/'));
        const visit = (node, keys, client) => {
            if (!node || typeof node !== 'object') return;
            if (!Array.isArray(node)) {
                const c = node.credentials;
                if (c && typeof c === 'object' && (c.uid !== undefined || c.gid !== undefined) && !folder) client = String(c.uid) + '_' + String(c.gid);
                ['check_credentials', 'allow_remote_clients'].forEach((k) => { if (node[k] !== undefined && typeof node[k] !== 'object') flags[k] = String(node[k]); });
                if (node.service !== undefined) {
                    const sv = node.service && typeof node.service === 'object' ? [num(node.service.first), num(node.service.last)] : [num(node.service), num(node.service)];
                    services.forEach((want, i) => {
                        if (want.id === null || sv[0] === null || sv[1] === null || want.id < sv[0] || want.id > sv[1]) return;
                        const kind = keys.indexOf('deny') >= 0 ? 'other' : keys.indexOf('requests') >= 0 ? 'requests' : keys.indexOf('offers') >= 0 ? 'offers' : 'other';
                        const inst = node.instance !== undefined ? node.instance : node.instances;
                        hits[i][kind][client] = inst === undefined ? '' : JSON.stringify(inst).replace(/"/g, '').slice(0, 48);
                    });
                }
            }
            Object.keys(node).forEach((k) => visit(node[k], Array.isArray(node) ? keys : keys.concat([k]), client));
        };
        const client = folder ? folder[1] : f.rel;
        clients[client] = true;
        visit(j, [], client);
    });
    const names = Object.keys(clients).sort();
    const me = (typeof process.getuid === 'function' ? process.getuid() : '?') + '_' + (typeof process.getgid === 'function' ? process.getgid() : '?');
    out.push('SOME/IP security: ' + there.join(', ') + ' has ' + files.length + ' files for ' + names.length + ' clients (' + cap(names, 8) + ')' +
        Object.keys(flags).map((k) => ' | ' + k + ' ' + flags[k]).join('') + ' | this service runs as ' + me + ': ' + (clients[me] ? 'has a policy' : 'NO policy of its own'));
    services.forEach((want, i) => {
        if (want.id === null) { out.push(want.name + ': its description gives no service id, the policies cannot be read for it'); return; }
        const list = (o) => { const k = Object.keys(o).sort(); return k.length ? cap(k.map((c) => c + (o[c] ? ' [' + o[c] + ']' : '')), 8) : 'nobody'; };
        out.push(want.name + ' (service ' + hex(want.id) + '): may be asked for by ' + list(hits[i].requests) + ' | offered by ' + list(hits[i].offers) +
            (Object.keys(hits[i].other).length ? ' | also named (denied or other) for ' + list(hits[i].other) : '') +
            ' | this service (' + me + ') may ask: ' + (hits[i].requests[me] !== undefined ? 'yes' : 'NOT NAMED'));
    });
    return out;
}
/* An interface description in short, for the page: its methods with their arguments and answers, its other
 * sections (fields, events) with their types, and every enumeration once (E1, E2, ...). Read as MainApp's
 * descriptions are written: { methods: { name: { id, schema: { t: 'Struct', e: [{ n, t, e }] }, retSchema } } }, a type
 * { t, e } with e the members (Struct, Union), the element (arrays) or the names (Enum). Lines. */
function describeIface(cfg) {
    const enums = [], seen = {};
    const en = (e) => { const k = JSON.stringify(e); if (seen[k] === undefined) { seen[k] = enums.length; enums.push(e); } return 'E' + (seen[k] + 1); };
    const ty = (n, depth) => {
        if (!n || typeof n !== 'object') return String(n);
        if (depth > 8) return '...';
        if (n.t === 'Enum' && n.e && typeof n.e === 'object' && !Array.isArray(n.e)) return en(n.e);
        if (Array.isArray(n.e)) {
            const parts = n.e.map((m) => (m && m.n !== undefined ? m.n + ':' : '') + ty(m, depth + 1));
            return n.t === 'Union' ? '(' + parts.join(' | ') + ')' : '{' + parts.join(', ') + '}';
        }
        if (n.e && typeof n.e === 'object') return ty(n.e, depth + 1) + '[]';
        return n.t !== undefined ? String(n.t) : JSON.stringify(n).slice(0, 120);
    };
    const members = (sc) => (sc && Array.isArray(sc.e) ? sc.e.map((m) => (m && m.n !== undefined ? m.n + ':' : '') + ty(m, 1)).join(', ') : sc ? ty(sc, 1) : '');
    const out = [];
    Object.keys(cfg || {}).forEach((k) => {
        const v = cfg[k];
        if (k === 'name' || k === 'serviceId' || k === 'instances') return;
        if (!v || typeof v !== 'object' || Array.isArray(v)) { out.push(k + ': ' + String(JSON.stringify(v)).slice(0, 300)); return; }
        const names = Object.keys(v);
        if (names.every((n) => !v[n] || typeof v[n] !== 'object')) { out.push(k + ': ' + JSON.stringify(v).replace(/"/g, '').slice(0, 300)); return; }
        out.push(k + ' (' + names.length + '): ' + names.map((n) => {
            const x = v[n] || {};
            if (!x.schema && !x.retSchema && x.t === undefined) return n + ' ' + JSON.stringify(x).replace(/"/g, '').slice(0, 160);
            return n + (k === 'methods' || x.retSchema ? '(' + members(x.schema) + ')' + (x.retSchema ? ' -> ' + members(x.retSchema) : '') : ': ' + (x.schema ? ty(x.schema, 1) : ty(x, 1)));
        }).join(' | '));
    });
    enums.forEach((e, i) => out.push('E' + (i + 1) + ' = ' + JSON.stringify(e).replace(/"/g, '').replace(/,/g, ', ')));
    return out.map((l) => (l.length > 9000 ? l.slice(0, 9000) + ' ... (+' + (l.length - 9000) + ' more)' : l));
}
/* What runs the media and the audio on this head unit: the processes named like them (their uid and command, from
 * /proc), the systemd units named like them (with their state where systemctl says it within 3 s), and the sound
 * cards. Lines for the page. */
async function carMedia(o) {
    const out = [], proc = o.procRoot || '/proc';
    const cap = (l, n) => l.slice(0, n).join(' | ') + (l.length > n ? ' | +' + (l.length - n) + ' more' : '');
    let pids = null;
    try { pids = fs.readdirSync(proc).filter((n) => /^\d+$/.test(n)); } catch (e) { /* said below */ }
    const hits = [];
    (pids || []).forEach((pid) => {
        let cmd;
        try { cmd = fs.readFileSync(path.join(proc, pid, 'cmdline'), 'utf8').replace(/\0+/g, ' ').trim(); } catch (e) { return; }
        if (!cmd || !/media|player|entertain|audio|gst|oap-mainapp/i.test(cmd) || /jap-ridetube|ytd\.js|ytdec|ytfetch/.test(cmd)) return;
        let uid = '?';
        try { uid = fs.statSync(path.join(proc, pid)).uid; } catch (e) { /* unknown */ }
        const line = uid + ': ' + cmd.slice(0, 150);
        if (hits.indexOf(line) < 0) hits.push(line);
    });
    out.push('Processes of the media and the audio (uid: command): ' + (pids === null ? 'not readable' : hits.length ? cap(hits, 24) : 'none found'));
    const units = [];
    (o.unitDirs || ['/usr/lib/systemd/system', '/lib/systemd/system', '/etc/systemd/system']).forEach((d) => {
        let names = [];
        try { names = fs.readdirSync(d); } catch (e) { return; }
        names.filter((n) => /media|player|entertain|audio/i.test(n) && /\.(service|socket)$/.test(n) && !/jap-ridetube/.test(n)).sort().forEach((n) => { if (units.indexOf(n) < 0) units.push(n); });
    });
    const shown = units.slice(0, 40);
    let states = [];
    if (shown.length) {
        states = await new Promise((res) => {
            try {
                cp.execFile(o.systemctl || 'systemctl', ['is-active'].concat(shown), { timeout: 3000 }, (e, so) => res(String(so || '').split('\n').map((x) => x.trim()).filter(Boolean)));
            } catch (e) { res([]); }
        });
    }
    out.push('Units of the media and the audio: ' + (shown.length ? cap(shown.map((n, i) => n.replace(/\.service$/, '') + (states.length === shown.length ? ' (' + states[i] + ')' : '')), 40) +
        (units.length > 40 ? ' | +' + (units.length - 40) + ' more' : '') : 'none found'));
    const snd = o.asound || '/proc/asound';
    const txt = (f) => { try { return fs.readFileSync(path.join(snd, f), 'utf8').replace(/\s+/g, ' ').trim().slice(0, 1600); } catch (e) { return null; } };
    const cards = txt('cards'), pcm = txt('pcm');
    out.push('Sound cards: ' + (cards === null ? 'not readable' : cards || 'none') + (pcm ? ' | devices: ' + pcm : ''));
    // the names sound is played to (ALSA's configuration), the audio manager's names of the media, and what could play
    const uniq = (l) => l.filter((x, i) => l.indexOf(x) === i);
    const small = (f, max) => { try { const st = fs.statSync(f); return st.isFile() && st.size <= max ? fs.readFileSync(f) : null; } catch (e) { return null; } };
    const ls = (d) => { try { return fs.readdirSync(d).sort(); } catch (e) { return []; } };
    let pcms = [];
    (o.alsaConf || ['/etc/asound.conf', '/usr/share/alsa/alsa.conf.d', '/etc/alsa/conf.d', '/usr/share/alsa/alsa.conf']).forEach((f) => {
        [f].concat(ls(f).map((n) => path.join(f, n))).forEach((file) => {
            const b = small(file, 2 << 20), re = /(?:^|\n)\s*pcm\.!?([\w.-]+)/g;
            let x;
            while (b && (x = re.exec(b.toString('latin1'))) && pcms.length < 400) pcms.push(x[1]);
        });
    });
    pcms = uniq(pcms);
    out.push('Sound device names (ALSA configuration): ' + (pcms.length ? pcms.slice(0, 90).join(', ') + (pcms.length > 90 ? ' (+' + (pcms.length - 90) + ' more)' : '') : 'none found'));
    let names = [];
    const amDir = o.amDir || '/usr/share/audiomanager';
    ls(amDir).forEach((n) => {
        const b = small(path.join(amDir, n), 6 << 20), re = /[A-Za-z][A-Za-z0-9_]{3,48}/g;
        let x;
        while (b && (x = re.exec(b.toString('latin1'))) && names.length < 4000) if (/oap|generic|online|entertain|dynamic|browser/i.test(x[0])) names.push(x[0]);
    });
    names = uniq(names);
    out.push('The audio manager\'s names of the OAP, online and dynamic media (' + amDir + ': ' + (ls(amDir).slice(0, 8).join(', ') || 'not there') + '): ' +
        (names.length ? names.slice(0, 70).join(', ') + (names.length > 70 ? ' (+' + (names.length - 70) + ' more)' : '') : 'none found'));
    const bins = ['gst-launch-1.0', 'gst-play-1.0', 'gst-inspect-1.0', 'aplay', 'ffmpeg', 'mpv'].filter((n) => (o.binDirs || ['/usr/bin', '/bin']).some((d) => fs.existsSync(path.join(d, n))));
    let plug = [];
    (o.gstDirs || ['/usr/lib/gstreamer-1.0', '/usr/lib64/gstreamer-1.0']).forEach((d) => { plug = plug.concat(ls(d).filter((n) => /aac|faad|fdk|opus|alsa|libav|isomp4|audioparsers|audioconvert|audioresample|playback|smartx|ias/i.test(n))); });
    out.push('What could play a file: ' + (bins.join(', ') || 'none of gst-launch-1.0, aplay, ffmpeg') + ' | GStreamer plugins: ' + (uniq(plug).map((n) => n.replace(/^libgst|\.so$/g, '')).slice(0, 40).join(', ') || 'none found'));
    return out;
}
/* How the apps installed on this head unit play media: their code (the .js of every app home and of the app manager,
 * bounded in files, bytes and time, read one at a time with the loop free in between) searched for the player's
 * instance names, the interfaces of the players and the audio manager, calls that start a playback, and the SDK's
 * media modules. Kept on the daemon for the Diagnostics page: what a head unit unlike the measured one does instead
 * of iDrive 7's "OapGenericMedia". */
const CODE_FINDS = [
    ['instances', /["'`]((?:[A-Za-z]*Generic[A-Za-z]*Media|Oap[A-Za-z]*Media|Online[A-Za-z]*Media|[A-Za-z]*MediaPlayerInst)[A-Za-z0-9_]*)["'`]/g],
    ['interfaces', /\b((?:[a-z]\w*\.)+mediaplayer\d*\.[A-Z]\w+|org\.genivi\.am\.[\w.]+|(?:[a-z]\w*\.)+audiomanager\w*\.[A-Z]\w+)\b/g],
    ['calls', /\b(playFile|playUrl|playUri|playStream|playItem|playTrack|setDataSource|requestAudioFocus|requestAudioSource|activateSource|startPlayback)\b\s*[(:=]/g],
    ['SDK modules', /(oap-sdk\/src\/[\w/.-]*(?:media|audio|entertain|player|sound)[\w/.-]*)/gi],
];
Daemon.prototype.scanCode = async function () {
    if (this.code) return this.code;
    const o = this.opts, c = this.code = { state: 'running', files: 0, bytes: 0, found: {}, error: null, snips: [], keys: {}, kinds: {}, calls: {} };
    try {
        const list = [];
        let left = 40000;
        const walk = (dir, label, depth) => {
            let names;
            try { names = fs.readdirSync(dir); } catch (e) { return; }
            for (const n of names) {
                if (--left <= 0 || list.length >= 600) return;
                const p = path.join(dir, n);
                if (/\.js$/.test(n)) { list.push({ p: p, label: label }); continue; }
                if (depth <= 0 || n[0] === '.' || /\.(json|node|so|png|jpg|svg|ttf|otf|tgz|gz|zip|map|css|html|txt|md|bin|dat|jsc)$/i.test(n)) continue;
                let st;
                try { st = fs.lstatSync(p); } catch (e) { continue; }
                if (st.isDirectory()) walk(p, label, depth - 1);
            }
        };
        let homes = [];
        try { homes = fs.readdirSync(o.oapRoot).filter((n) => /^oap_\d+$/.test(n)).sort(); } catch (e) { /* none */ }
        // the app manager's own code first (it is what plays for the apps on iDrive 8), then the apps
        // (targeted: the app manager's files said nothing of the player on a car - the SDK in the app does)
        const mgr = o.codeScan === 'targeted' ? [] : o.mainapps || [];
        mgr.forEach((m) => walk(path.join(m.root, 'src'), 'the app manager', 4));
        mgr.forEach((m) => {
            let names = [];
            try { names = fs.readdirSync(m.root); } catch (e) { return; }
            names.filter((n) => /\.js$/.test(n)).forEach((n) => list.push({ p: path.join(m.root, n), label: 'the app manager' }));
        });
        if (o.codeScan === 'targeted') homes = homes.filter((h) => (o.codeHomes || []).indexOf(h) >= 0);
        homes.forEach((h) => walk(path.join(o.oapRoot, h), h, 6));
        if (o.codeScan === 'targeted' && list.length > 120) list.length = 120;
        const end = Date.now() + 180000;
        for (const f of list) {
            if (this.closed || Date.now() > end || c.bytes > (300 << 20)) break;
            if (/jap_ridetube/.test(f.p)) continue;      // (our own app names what is looked for)
            let text = null;
            try {
                const st = fs.statSync(f.p);
                if (!st.isFile() || st.size < 200 || st.size > (12 << 20)) continue;
                text = await new Promise((res) => fs.readFile(f.p, 'utf8', (e, t) => res(e ? null : t)));
            } catch (e) { continue; }
            if (text === null) continue;
            c.files++;
            c.bytes += text.length;
            // the app: the package the file is in, where its path says one (an app's home is oap_<uid>)
            const m = /node_modules\/([^/]+)\/(?!node_modules)/.exec(f.p.split(path.sep).join('/')) || /\/([^/]+)\/javascript\//.exec(f.p.split(path.sep).join('/'));
            const app = f.label + (m && m[1] !== f.label ? ':' + m[1] : '');
            CODE_FINDS.forEach((k) => {
                const seen = c.found[k[0]] = c.found[k[0]] || {};
                k[1].lastIndex = 0;
                let x, n = 0;
                while ((x = k[1].exec(text)) && n++ < 5000) {
                    const v = x[1].slice(0, 90);
                    if (!seen[v] && Object.keys(seen).length >= 60) continue;
                    (seen[v] = seen[v] || {})[app] = true;
                }
            });
            // what the code does around the players: pieces of it as written (told apart by their middle, so the SDK
            // every app carries is shown once), and the interface calls near the DynamicMediaPlayer
            const shown = [];        // the stretches of this file already taken: a word inside one is not taken again
            const grab = (kind, re, max, before, after) => {
                re.lastIndex = 0;
                let x, last = -1e9;
                while ((x = re.exec(text)) && (c.kinds[kind] || 0) < max && c.snips.length < 26) {
                    if (x.index - last < before + after) continue;
                    if (shown.some((r) => x.index >= r[0] + 40 && x.index <= r[1] - 40)) continue;
                    const key = kind + '|' + text.slice(Math.max(0, x.index - 40), x.index + 80).replace(/\s+/g, ' ');
                    if (c.keys[key]) continue;
                    c.keys[key] = true;
                    last = x.index;
                    c.kinds[kind] = (c.kinds[kind] || 0) + 1;
                    shown.push([x.index - before, x.index + after]);
                    c.snips.push({ kind: kind, where: app, text: text.slice(Math.max(0, x.index - before), x.index + after).replace(/\s+/g, ' ') });
                }
            };
            // (what 0.4.7's pages left open of how BMW's SDK asks the dynamic player: how it turns an item into a
            // playlist entry, how it asks for the focus and keeps it, how a play starts)
            if (o.codeScan === 'targeted') {
                grab('playlistConverter', /playlistConverter\s*[(=]/g, 3, 150, 750);
                grab('timeOutHeartBeatTimer', /timeOutHeartBeatTimer/g, 2, 450, 450);
                grab('generateItemId', /generateItemId\s*\(\s*\)\s*\{/g, 1, 60, 400);
                grab('heartBeat', /\.heartBeat\s*\(/g, 1, 350, 350);
                grab('stringValue', /stringValue/g, 2, 350, 350);
                grab('setCurrentItemPositionAndTime', /\.setCurrentItemPositionAndTime\s*\(/g, 1, 450, 350);
                grab('releaseAudioFocus', /\.releaseAudioFocus\s*\(/g, 1, 350, 300);
                grab('ePlay', /ePlay\s*:/g, 2, 200, 500);
                grab('playFile', /playFile\(e,t,i,a\)\{/g, 1, 20, 900);
            } else {
                grab('createPlaylist', /createPlaylist/g, 2, 350, 550);
                grab('DynamicMediaPlayer', /dynamicmediaplayer/gi, 4, 250, 450);
                grab('playFile', /playFile\s*[(:=]/g, 2, 350, 400);
                grab('OapGenericMedia', /OapGenericMedia["'`]/g, 2, 300, 350);
                grab('GenericMediaPlayer', /GenericMediaPlayer["'`]/g, 1, 300, 350);
            }
            const dynRe = /dynamicmediaplayer/gi, callRe = /\b(callMethod|getValue|setValue|subscribe)\(\s*(?:\{\s*name\s*:\s*)?["'`](\w+)["'`]([^;]{0,90})/g;
            let y, spans = 0;
            while ((y = dynRe.exec(text)) && spans++ < 40) {
                const part = text.slice(Math.max(0, y.index - 2500), y.index + 2500);
                callRe.lastIndex = 0;
                let z;
                while ((z = callRe.exec(part)) && Object.keys(c.calls).length < 30) c.calls[z[1] + ' ' + z[2]] = z[3].replace(/\s+/g, ' ').slice(0, 90);
            }
            await sleep(this.opts.probe ? 15 : 120);
        }
        c.state = 'done';
    } catch (e) {
        c.state = 'failed';
        c.error = errText(e);
    }
    this.log('apps\' code read for how they play media: ' + c.files + ' files, ' + CODE_FINDS.map((k) => Object.keys(c.found[k[0]] || {}).length + ' ' + k[0]).join(', '));
    return c;
};
/* The page's lines of scanCode: each kind's finds, those most apps have first, with up to two of the apps. */
Daemon.prototype.codeLines = function () {
    const c = this.code;
    if (!c) return ['Not read (only read when the car does not offer the media player; Refresh starts it).'];
    const out = [c.state === 'running' ? 'Still reading (' + c.files + ' files so far): Refresh in a moment.' :
        c.files + ' files of the installed apps and the app manager read' + (c.error ? ' | failed: ' + c.error : '')];
    CODE_FINDS.forEach((k) => {
        const seen = c.found[k[0]] || {};
        const vals = Object.keys(seen).sort((x, y) => Object.keys(seen[y]).length - Object.keys(seen[x]).length || (x < y ? -1 : 1));
        out.push(k[0] + ': ' + (vals.length ? vals.slice(0, 10).map((v) => {
            const apps = Object.keys(seen[v]);
            return v + ' (' + apps.slice(0, 2).join(', ') + (apps.length > 2 ? ' +' + (apps.length - 2) : '') + ')';
        }).join(' | ') + (vals.length > 10 ? ' | +' + (vals.length - 10) + ' more' : '') : 'none found'));
    });
    const calls = Object.keys(c.calls);
    if (calls.length) out.push('Calls near the DynamicMediaPlayer in the code: ' + calls.map((k) => k + c.calls[k]).join(' || '));
    c.snips.forEach((x) => out.push('[' + x.kind + ', in ' + x.where + '] ' + x.text));
    return out;
};
/* The page's lines: '# ' starts a section. The sound first (what stops a play), then the files, the picture, the log. */
Daemon.prototype.diagLines = async function () {
    const a = this.audio, d = a.diag, o = this.opts, out = [];
    const yn = (v) => (v === null || v === undefined ? 'unknown' : v ? 'yes' : 'NO');
    const js = (v) => { let t; try { t = typeof v === 'string' ? v : JSON.stringify(v); } catch (e) { t = String(v); } return String(t).slice(0, 160); };
    await a.refresh().catch(() => {});
    out.push('RideTube service ' + VERSION + ' | Node ' + process.version + ' | running ' + Math.round(process.uptime() / 60) + ' min | player: ' + this.state);
    // a head unit without iDrive 7's player: what plays, or the whole story of the last search for a way to sound
    const snd = a.sound || (o.id8 && d.ready && d.mp && d.mp.available === false ? a.snd() : null);
    if (snd) {
        out.push('# Sound on this car (found by trying)');
        let sl = [];
        try { sl = snd.lines(); } catch (e) { sl = ['could not be printed: ' + errText(e)]; }
        sl.forEach((l) => out.push(l));
        // where the car's player may read a file, then how BMW's own code plays one: what our playlist has to become
        try { sl = snd.whereLines(); } catch (e) { sl = ['could not be printed: ' + errText(e)]; }
        if (sl.length) { out.push('# Where the car\'s player may read a file'); sl.forEach((l) => out.push(l)); }
        try { sl = snd.playLines(); } catch (e) { sl = ['could not be printed: ' + errText(e)]; }
        if (sl.length) { out.push('# How BMW\'s own code plays a file'); sl.forEach((l) => out.push(l)); }
        // how the car's programs are wired to its sound, and its sound devices (the whole page only: read on the first car)
        if (o.diagFull) {
            try { sl = snd.plumbingLines().concat(snd.deviceLines()); } catch (e) { sl = ['could not be printed: ' + errText(e)]; }
            if (sl.length) { out.push('# The car\'s sound devices'); sl.forEach((l) => out.push(l)); }
        }
        // a way that took samples and was not heard: how a player tells the audio manager that its source plays
        if (Object.keys((snd.state && snd.state.silent) || {}).some((k) => /^(direct|focus):/.test(k))) {
            try { sl = a.amPlayerIfaces(); } catch (e) { sl = ['could not be read: ' + errText(e)]; }
            out.push('# Played unheard: the audio manager\'s interfaces for players');
            sl.forEach((l) => out.push(l));
        }
    }
    out.push('# Sound');
    out.push(d.ready ? (d.mp && d.mp.available === false ? 'The car\'s audio manager answers; its media player "' + d.mp.instance + '" does NOT (a play fails there).'
        : 'Ready: the car\'s audio manager and player answer.')
        : 'NOT READY - ' + (d.error || 'starting') + ' | stopped at: ' + d.stage + ' | try ' + d.tries);
    // (where the sound is being searched for, the story above tells it: what was read on the first car is left out)
    const brief = !!snd && !!(snd.runs && snd.runs.length) && d.ready && !o.diagFull;
    if (!brief) out.push('SOME/IP binding: ' + (d.binding || 'not loaded' + (typeof o.someip === 'string' ? ' (' + o.someip + (fs.existsSync(o.someip) ? ' is there' : ' is NOT there') + ')' : '')));
    (brief ? [] : [['cc', 'CommandControl'], ['mp', 'MediaPlayer']]).forEach((k) => {
        const x = d[k[0]];
        if (!x) { out.push(k[1] + ': not looked for yet'); return; }
        out.push(k[1] + ': ' + (x.file ? 'description ' + x.file : 'NO description') +
            (x.instances ? ' | instance ' + (x.instance || '-') + ' of ' + (x.instances.length > 12 ? x.instances.length + ' instances' : '[' + x.instances.join(', ') + ']') :
                x.file ? ' | instance ' + (x.instance || '-') + ' (the description lists none)' : '') +
            (x.ids && (x.ids.service !== null || x.ids.instance !== null) ? ' | service ' + hex(x.ids.service) + ', instance ' + hex(x.ids.instance) : '') +
            (x.file ? ' | answers: ' + yn(x.available) : '') + (x.lacks && x.lacks.length ? ' | the description lacks: ' + x.lacks.join(', ') : '') +
            (x.error && x.error !== d.error ? ' | ' + x.error : '') + (x.generator ? ' | generator: ' + x.generator : ''));
    });
    if (brief) { /* said by the story */ } else if (d.players) {
        // those that answer, and each of the OAP, dynamic, online and generic ones
        const tag = (x) => x.inst + (x.ids.instance !== null ? '=' + hex(x.ids.instance) : '');
        const special = d.players.filter((x) => /oap|dynamic|online|generic|browser|temp/i.test(x.inst));
        out.push('Players the car offers: ' + d.players.length + ' asked | answering: ' + (d.players.filter((x) => x.available).map(tag).join(', ') || 'none') +
            (special.length ? ' | of the OAP, dynamic and online ones: ' + special.map((x) => tag(x) + ' ' + (x.error ? 'FAILED' : x.available ? 'yes' : 'NO')).join(', ') : ''));
    } else out.push('Players the car offers: not asked (the one in use answered at the start)');
    const conf = a.conf();
    out.push('Route: source ' + a.src + ' (listed: ' + yn(a.srcListed) + ') to sink ' + a.sink + ' (listed: ' + yn(a.sinkListed) + ')' +
        (Object.keys(conf.set).length ? ' | audio.json: ' + js(conf.set) : ''));
    const why = (k) => (d.listWhy && d.listWhy[k] ? ' (' + d.listWhy[k] + ')' : '');
    out.push('Sources: ' + (d.sources === null ? 'not read' + why('listOfMainSources') : d.sources || 'none'));
    out.push('Sinks: ' + (d.sinks === null ? 'not read' + why('listOfMainSinks') : d.sinks || 'none'));
    out.push('Connections (id:source>sink): ' + (d.conns === null ? 'not read' : d.conns || 'none'));
    d.notes.forEach((n) => out.push('Note: ' + n));
    if (brief) { /* said by the story */ } else if (d.play) {
        out.push('Last play: ' + d.play.file + ' | route ' + (d.play.route || '-') + ' | player answers: ' + yn(d.play.player) + ' | connect ' + (d.play.connect || '-') +
            (d.play.waited ? ' (waited ' + d.play.waited + ' ms for it after connecting)' : '') +
            ' | playFile ' + (d.play.playFile || '-') + (d.play.error ? ' | ERROR ' + d.play.error : ''));
    } else out.push('Last play: none since the service started');
    const ev = Object.keys(d.events);
    if (!brief) out.push('Player events: ' + (ev.length ? ev.map((k) => k + ' ' + js(d.events[k])).join(' | ') : 'none yet'));
    const dyn = d.dyn;
    if (dyn && !brief) {
        out.push('# The dynamic media player');
        if (!dyn.file) out.push('No description of a DynamicMediaPlayer on this head unit.');
        else {
            out.push('Description ' + dyn.file + ' | service ' + hex(dyn.service) + ' | instances: ' + (dyn.insts.length ? dyn.insts.map((x) => x.name + (x.id !== null ? '=' + hex(x.id) : '') + ' ' +
                (x.error ? 'FAILED (' + x.error.slice(0, 60) + ')' : yn(x.available))).join(', ') : 'none listed'));
            const vals = Object.keys(dyn.values);
            out.push('Its fields now: ' + (vals.length ? vals.map((k) => k + ' = ' + dyn.values[k]).join(' | ') : 'none read'));
            let short;
            try { short = describeIface(a.dyn.cfg); } catch (e) { short = ['the description could not be printed: ' + errText(e)]; }
            if (o.diagFull) short.forEach((l) => out.push(l));
            else out.push('Its description: ' + short.filter((l) => /^(methods|fields|events) \(/.test(l)).map((l) => l.split(':')[0]).join(', ') + ' (printed whole by 0.4.6)');
        }
    }
    // the rest of BMW's own player code as read on this car (the whole page only)
    if (snd && o.diagFull) {
        let more = [];
        try { more = snd.codeLines(); } catch (e) { more = ['could not be printed: ' + errText(e)]; }
        if (more.length) { out.push('# BMW\'s own player code, read on this car'); more.forEach((l) => out.push(l)); }
    }
    if (o.diagFull) {
        out.push('# Who may use them');
        let pol;
        try {
            pol = someipPolicies(o.policyDirs || [], [{ name: 'CommandControl', id: d.cc && d.cc.ids ? d.cc.ids.service : null }, { name: 'MediaPlayer', id: d.mp && d.mp.ids ? d.mp.ids.service : null }]
                .concat(dyn && dyn.file ? [{ name: 'DynamicMediaPlayer', id: dyn.service }] : []));
        } catch (e) { pol = ['SOME/IP security: could not be read: ' + errText(e)]; }
        pol.forEach((l) => out.push(l));
        out.push('# What runs the media');
        let run;
        try { run = await carMedia(o); } catch (e) { run = ['could not be read: ' + errText(e)]; }
        run.forEach((l) => out.push(l));
    }
    if (o.codeScan || this.code) out.push('# How the installed apps play media');
    // (read once, when the car does not offer the player: asked for here too, shown when done)
    // never while the head unit is starting its apps: the service has to have run scanAfterS
    const early = process.uptime() < (o.scanAfterS === undefined ? 180 : o.scanAfterS);
    // (when it is done the page is sent again: no Refresh needed)
    if (!o.codeScan && !this.code) { /* nothing: BMW's own player code is read by sdkread.js (above) */ } else if (!this.code && d.ready && d.mp && d.mp.available === false && !early) this.scanCode().then(() => { if (!this.closed) return this.sendDiag(); }).catch(() => {});
    if (o.codeScan && !this.code && early) out.push('Not read yet: the service waits ' + (o.scanAfterS === undefined ? 180 : o.scanAfterS) + ' s after its start, so the car\'s own apps start first. Refresh later.');
    if (o.codeScan || this.code) this.codeLines().forEach((l) => out.push(l));
    if (o.diagFull) {
        out.push('# Files');
        let inv;
        try { inv = audioInventory(o, !(d.cc && d.cc.file && d.mp && d.mp.file)); } catch (e) { inv = ['could not be read: ' + errText(e)]; }
        inv.forEach((l) => out.push(l));
    }
    out.push('# Picture and network');
    out.push('Video decoder: ' + (this.decoder || 'not checked') + ' | ytdec ' + (fs.existsSync(o.ytdec) ? 'there' : 'MISSING'));
    out.push('Internet uid: ' + (this.fetchUid === null ? 'none yet' : this.fetchUid) + ' | uplink lease: ' + (fs.existsSync(o.leaseFile) ? 'there' : 'not there'));
    out.push('# Log (the last lines)');
    this.recent.slice(-8).forEach((l) => out.push(l));
    return out;
};
Daemon.prototype.sendDiag = async function () {
    if (this.audioKick) this.audioKick();      // (a start between tries makes the next now: Refresh shows it)
    let lines;
    try { lines = await this.diagLines(); } catch (e) { lines = ['The diagnostics failed: ' + errText(e)]; }
    this.emit({ ev: 'diag', version: VERSION, ready: this.audio.diag.ready, lines: lines });
};
Daemon.prototype.guard = function (fn) {
    try {
        const r = fn();
        if (r && r.catch) r.catch((e) => this.fault(e));
    } catch (e) {
        this.fault(e);
    }
};
// uncaughtException / unhandledRejection / handler errors: log, release audio, idle.
Daemon.prototype.fault = function (e) {
    this.log('fault: ' + (e && e.stack || e));
    this.emit({ ev: 'error', msg: 'internal error: ' + (e && e.message || e) });
    this.stop('idle').catch((e2) => this.log('stop after fault: ' + e2));
};

// ---- control socket
Daemon.prototype.listen = function () {
    const sock = this.opts.sock;
    try { mkdirp(path.dirname(sock)); } catch (e) { this.log('socket dir: ' + e.message); }
    try { fs.unlinkSync(sock); } catch (e) { /* none */ }
    this.server = net.createServer((c) => this.guard(() => this.onClient(c)));
    this.server.on('error', (e) => this.log('ctl server: ' + e.message));
    this.server.listen(sock, () => {
        this.sockUid = undefined;   // nothing applied yet: lockSocket sets the mode and logs it
        this.lockSocket();
        this.log('listening on ' + sock);
    });
};
// the uid MainApp's registry gives RideTube's app (apps/jap_ridetube.json userId), null if it cannot be read or is
// not an app's: an integer (or all-digit string) inside MainApp's app uid ranges (unchecked while those are unknown),
// never the fetch uid
Daemon.prototype.appUid = function () {
    const f = path.join(this.opts.oapRoot, 'oap_12201', 'data', 'apps', 'jap_ridetube.json');
    const e = this.regOk(f) ? readJson(f) : null;
    const v = e && e.userId;
    const u = typeof v === 'number' || typeof v === 'string' && /^\d+$/.test(v) ? Number(v) : NaN;
    return this.okAppUid(u) ? u : null;
};
Daemon.prototype.okAppUid = function (u) {
    const cfg = this.appRanges();
    return isId(u) && u <= MAX_UID && u !== this.fetchUid && u !== this.webUid && (!cfg || inRanges(cfg, u));
};
// the last good app uid, kept in state/app-uid.json (root's): a failed registry read keeps it, a restart too
Daemon.prototype.knownAppUid = function () {
    // iDrive 8: the socket stays as the service confirmed there has it (0660 root:tm_oap_app). The app has another id
    // there (jap_ridetube8) and MainApp-21's registry has not been read on a car: a wrong uid would lock the app out.
    if (this.opts.id8) return null;
    const f = path.join(this.opts.stateDir, 'app-uid.json');
    if (this.lastAppUid === undefined) {
        const k = Number((readJson(f) || {}).uid);
        this.lastAppUid = this.okAppUid(k) ? k : null;
    }
    const u = this.appUid();
    if (u === null && this.lastAppUid !== null && this.appRemoved()) {
        // uninstalled (or mid-reinstall): its old uid may go to another app, so root only until an entry comes back
        if (!this.regGone) this.log('ctl.sock: RideTube is no longer in MainApp\'s registry: root only');
        this.regGone = true;
        return 0;
    }
    this.regGone = false;
    if (u === null) {
        if (this.lastAppUid !== null && !this.regBad) this.log('ctl.sock: no valid app uid in MainApp\'s registry: kept ' + this.lastAppUid);
        this.regBad = true;
        return this.lastAppUid;
    }
    this.regBad = false;
    if (u !== this.lastAppUid) {
        this.lastAppUid = u;
        try {
            if (!fs.existsSync(this.opts.stateDir)) { mkdirp(path.dirname(this.opts.stateDir)); fs.mkdirSync(this.opts.stateDir, 0o700); }
            writeIfChanged(f, JSON.stringify({ uid: u }) + '\n', 0o600);
        } catch (e) { this.log('app uid: ' + e.message); }
    }
    return u;
};
// true when MainApp's apps folder lists fine and has no jap_ridetube.json (a read error is not a removal)
Daemon.prototype.appRemoved = function () {
    const d = path.join(this.opts.oapRoot, 'oap_12201', 'data', 'apps');
    try { return this.regOk(d) && fs.readdirSync(d).indexOf('jap_ridetube.json') < 0; }
    catch (e) { return false; }
};
// every app can write /var/opt/public/oap (group mgu_persist), so one could rename oap_12201 and plant its own
// registry: trust a path only when each part from oap_12201 down is no symlink, owned by MainApp (or root) and not
// world-writable
Daemon.prototype.regOk = function (p) {
    const top = path.join(this.opts.oapRoot, 'oap_12201'), parts = path.relative(top, p).split(path.sep);
    let cur = top;
    for (let i = 0; i <= parts.length; i++) {
        if (i > 0) cur = path.join(cur, parts[i - 1]);
        let st;
        try { st = fs.lstatSync(cur); } catch (e) { return false; }
        if (st.isSymbolicLink() || (st.uid !== this.opts.mgrUid && st.uid !== 0) || (st.mode & 0o002)) {
            if (!this.regUntrusted) this.log('ctl.sock: MainApp\'s registry is not MainApp\'s (' + cur + '): ignored');
            this.regUntrusted = true;
            return false;
        }
    }
    this.regUntrusted = false;
    return true;
};
// ctl.sock belongs to the app's uid alone (<uid>:root 0600): no other app can play, read feeds or sign out. Shared
// (0660 root:tm_oap_app, every app can connect) only while no app uid was ever known (a fresh install whose registry
// cannot be read). Applied only when it changes, logged then; a connection made before a change is dropped (the app
// reconnects every 2 s under the new owner). A failed chown keeps the previous owner (root only if none), never shared.
// sockUid (read it through sockShared()): the uid it is locked to, 0 root only, null while shared, undefined before listen
Daemon.prototype.lockSocket = function () {
    if (!this.server || !this.server.listening) return;
    const u = this.knownAppUid(), sock = this.opts.sock, was = this.sockUid;
    if (u === was) return;
    try {
        if (u === null) { fs.chmodSync(sock, 0o660); fs.chownSync(sock, 0, this.opts.sockGid); }
        else { fs.chmodSync(sock, 0o600); fs.chownSync(sock, u, 0); }   // closed first, then handed over
        this.log(u === null ? 'ctl.sock: shared with every app (0660 root:tm_oap_app): no app uid known' : u === 0 ? 'ctl.sock: root only (0600)' : 'ctl.sock: app uid ' + u + ' only (0600)');
        this.sockUid = u;
        this.sockErr = null;
    } catch (e) {
        if (u === null) {
            this.sockUid = null;   // shared anyway (0660); the group is best effort (not root: tests)
        } else {
            let back = 0;
            try { if (typeof was === 'number' && was > 0) { fs.chownSync(sock, was, 0); back = was; } } catch (e2) { /* root only */ }
            this.sockUid = back;
        }
        if (e.message !== this.sockErr) this.log('ctl.sock: ' + e.message + (u === null ? ': shared with every app (0660 root:tm_oap_app)' : this.sockUid ? ': kept app uid ' + this.sockUid : ': root only (0600)'));
        this.sockErr = e.message;   // once, not every tick
    }
    if (this.sockUid !== was && was !== undefined && this.client) {
        this.log('ctl.sock: owner changed: the connected client is dropped');
        this.client.destroy();
    }
};
// true while any app on the unit can use the socket (health: "socket: shared with other apps")
Daemon.prototype.sockShared = function () { return typeof this.sockUid !== 'number'; };
Daemon.prototype.onClient = function (c) {
    if (this.client) {
        this.log('new client replaces the old one');
        const old = this.client;
        this.client = null; // its close must not stop playback for the new one
        old.destroy();
    }
    this.client = c;
    this.hl = this.gl = null;   // hello's lang and region hold for this connection only
    // so does `loop`: the app says it again after hello. A client replacing another mid-video carries it over until
    // then ('carried'), so the video does not end in between; a carried loop ends with that video (stop)
    this.loop = this.loop && this.cur ? 'carried' : false;
    this.clockSaid = undefined; // what hello (then `clock`) told this connection about the clock; undefined: no hello yet
    c.setEncoding('utf8');
    let buf = '';
    c.on('data', (d) => {
        buf += d;
        if (buf.length > 65536) { this.log('client line too long'); c.destroy(); return; }
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
            const line = buf.slice(0, i).trim();
            buf = buf.slice(i + 1);
            if (line) this.guard(() => this.onLine(line));
        }
    });
    c.on('error', (e) => this.log('client: ' + e.message));
    c.on('close', () => {
        if (this.client !== c) return;
        this.client = null;
        this.log('client gone: stop');
        this.rc.off('the app went away');
        this.stop('idle').catch((e) => this.log('stop: ' + e));
    });
};
Daemon.prototype.emit = function (o) {
    if (!this.client) return;
    try { this.client.write(JSON.stringify(o) + '\n'); } catch (e) { this.log('emit: ' + e.message); }
};
Daemon.prototype.onLine = function (line) {
    let m;
    try { m = JSON.parse(line); } catch (e) { return this.emit({ ev: 'error', msg: 'bad json' }); }
    if (!m || typeof m.cmd !== 'string') return this.emit({ ev: 'error', msg: 'no cmd' });
    this.usedAt = Date.now();
    if (typeof m.id === 'string') this.see(m.cmd === 'playlist' ? 'p' : m.cmd === 'channel' ? 'c' : 'v', m.id);
    switch (m.cmd) {
    case 'prefetch': return this.prefetch(m.id);
    case 'positions': return this.sendPositions();
    case 'status': this.sendState(); if (this.total) this.emit({ ev: 'time', elapsed: this.elapsed, total: this.total }); return undefined;
    case 'ytdlp': return typeof m.auto === 'boolean' ? this.setYtdlpAuto(m.auto) : this.sendYtdlp();
    case 'search': return this.search(m.q);
    case 'thumb': return this.thumb(m.id);
    case 'play': return this.play(m.id, m.rect, !!m.fill, m.at, m.quality, m.skip, undefined, m.noUpscale === true);
    case 'pause': return this.pause();
    case 'resume': return this.resume();
    case 'seek': return this.seek(m.ms);
    case 'stop': return this.stop('idle');
    case 'video': return this.setVideo(!!m.on);
    case 'fill': return this.setFill(!!m.on);
    case 'loop': this.loop = !!m.on; this.log('loop ' + (this.loop ? 'on' : 'off')); return undefined;
    case 'quality': return this.requality(m.quality);
    case 'auth': return this.account.start();
    case 'auth_status': return this.emit(this.account.status());
    case 'sign_out': return this.account.signOut();
    case 'feed': return this.account.feed(m.which);
    case 'channels': return this.account.channels();
    case 'channel': return this.account.channel(m.id);
    case 'diag': return this.sendDiag();
    case 'nosound': return this.noSound();
    case 'soundreset': if (this.audio.sound || this.opts.id8) { this.audio.snd().reset(); this.log('sound: started over (nothing kept, nothing left out)'); } return this.sendState();
    case 'playlists': return this.account.playlists();
    case 'playlist': return this.account.playlist(m.id);
    case 'send_logs': return this.sendLogs(m.app);
    case 'hello': return this.hello(m);
    case 'health': return this.health();
    case 'related': return this.related(m.id);
    case 'remote': return this.remote(m.on, m.lang);   // 0.4.1
    case 'proxy': return this.proxy(m.mode);
    case 'library': return this.sendLibrary();   // 0.5.0
    case 'download': return this.download(m.id, m.meta, m.quality);
    case 'remove': return this.remove(m.id);
    case 'auto_download': return this.setAutoMax(m.max);
    default: return this.emit({ ev: 'error', msg: 'unknown cmd ' + m.cmd });
    }
};
// `hello` {app, lang?, region?} -> hello{service, caps}. A lang or region that does not match is dropped (logged), never
// refused: the app still learns the caps. Kept for this connection (onClient clears them) as the hl / gl of its requests
Daemon.prototype.hello = function (m) {
    this.hl = typeof m.lang === 'string' && LANG_RE.test(m.lang) ? m.lang : null;
    this.gl = typeof m.region === 'string' && REGION_RE.test(m.region) ? m.region : null;
    const bad = (m.lang !== undefined && !this.hl ? ' (bad lang dropped)' : '') + (m.region !== undefined && !this.gl ? ' (bad region dropped)' : '');
    this.log('hello: app ' + (/^\d+(\.\d+){0,3}$/.test(String(m.app)) ? m.app : 'unknown') + ', lang ' + (this.hl || '-') + ', region ' + (this.gl || '-') + bad);
    const clk = clockBehind((this.opts.now || Date.now)());
    this.clockSaid = clk;
    if (clk) this.log('clock wrong (' + clk + '): certificate dates not checked');
    // WP8: `stream` only where the videos go to RAM (a car without a tmpfs keeps them on flash, as before)
    const caps = CAPS.slice().concat(this.ram ? ['stream'] : []);
    return this.emit(Object.assign({ ev: 'hello', service: VERSION, caps: caps }, clk ? { clock: clk } : {}));
};
// commands of the frozen protocol whose package has not landed: each answers this until its package replaces the body
Daemon.prototype.notImplemented = function (cmd) { return this.emit({ ev: 'error', cmd: cmd, msg: 'not implemented ' + cmd }); };
Daemon.prototype.health = function () { return require('./health.js').run(this, VERSION, (o) => this.emit(o)); };   // WP4: health.js, never rejects
// `related` {id} -> related{id, items} (what YouTube plays after it: the TVHTML5 watch-next answer's video tiles, the
// video itself left out) or related{id, error}. Signed in when we can, anonymous otherwise (as search); kept 10 min
// per video and language, dropped at sign-out. After marilykinson-glitch's fork (GPL-3.0)
const RELATED_KEEP = 600000;
Daemon.prototype.related = async function (id) {
    if (!ID_RE.test(String(id))) return this.emit({ ev: 'related', id: String(id), error: 'bad id' });
    const key = id + '/' + (this.hl || '') + '/' + (this.gl || ''), g = this.account.gen;
    const c = this.relatedKept || (this.relatedKept = {}), k = c[key];
    if (k && k.g === g && Date.now() - k.t < RELATED_KEEP) return this.emit({ ev: 'related', id: id, items: k.items });
    const body = { context: { client: tvClient(this.hl, this.gl) }, videoId: id };
    const ask = (tok) => this.account.json('POST', YT + '/youtubei/v1/next', body, tok ? { Authorization: 'Bearer ' + tok } : null);
    let r;
    try {
        const tok = await this.account.token(false).catch(() => null);
        r = await ask(tok);
        if (r.status === 401 && tok) r = await ask(null);
        if (r.status !== 200 || !r.j) throw new Error('next ' + r.status);
    } catch (e) {
        this.log('related: ' + e.message);
        return this.emit({ ev: 'related', id: id, error: e.noUid ? e.message : 'related offline' });
    }
    const items = feedItems(r.j).filter((x) => x.id !== id).slice(0, FEED_MAX);
    Object.keys(c).forEach((x) => { if (Date.now() - c[x].t >= RELATED_KEEP) delete c[x]; });   // old ones go: no unbounded map
    c[key] = { t: Date.now(), g: g, items: items };
    return this.emit({ ev: 'related', id: id, items: items });
};
// `remote` {on} -> remote{on, url, expires} (remote.js; on not a bool: only the state). WP9, 0.4.1
Daemon.prototype.remote = function (on, lang) { return this.rc.set(on, lang); };
// the web uid (remote.js's server): its own, never the fetch uid nor an app's; throws when there is none
Daemon.prototype.remoteUid = function () {
    if (this.opts.noSetuid) return process.getuid() || 1;   // (tests, not root: the child runs as ourselves)
    if (this.webUid === null) this.webUid = pickUid(this.opts, 'web');
    if (this.webUid === null || this.webUid === this.fetchUid) throw new Error('no web uid on the car (see the log)');
    return this.webUid;
};
// The app's "Use the MGU Store's proxy" setting: store (the default) or off (direct, the Store's proxy left to the
// Store). Only on or off: the address is the Store's alone. Kept in state/proxy.json, told back as {ev:'proxy', mode}
Daemon.prototype.proxy = function (mode) {
    if (mode !== 'store' && mode !== 'off') return this.emit({ ev: 'error', cmd: 'proxy', msg: 'bad proxy mode' });
    if (mode !== this.proxyMode()) {
        this.proxyModeOn = mode;
        try { if (!fs.existsSync(this.opts.stateDir)) { mkdirp(path.dirname(this.opts.stateDir)); fs.mkdirSync(this.opts.stateDir, 0o700); } writeIfChanged(path.join(this.opts.stateDir, 'proxy.json'), JSON.stringify({ mode: mode }) + '\n', 0o644); } catch (e) {
            this.log('proxy: ' + e.message);
            if (mode === 'store') try { fs.unlinkSync(path.join(this.opts.stateDir, 'proxy.json')); } catch (e2) { /* none: store */ }   // never an old off after a restart
        }
    }
    this.proxySetting();   // logs a change, drops the spare, knows whether the Store has a proxy
    return this.emit({ ev: 'proxy', mode: this.proxyMode(), store: this.proxyStore });
};
Daemon.prototype.proxyMode = function () {
    if (this.proxyModeOn === undefined) { const s = readJson(path.join(this.opts.stateDir, 'proxy.json')); this.proxyModeOn = s && s.mode === 'off' ? 'off' : 'store'; }
    return this.proxyModeOn;
};
Daemon.prototype.setState = function (s) {
    if (s !== 'paused' && this.micHeld) { clearTimeout(this.micHeld.timer); this.micHeld = null; }   // played, stopped, failed: no longer the hold's pause
    this.state = s;
    this.sendState();
};
Daemon.prototype.sendState = function (pct) {
    this.buffering = this.isBuffering();
    const o = { ev: 'state', state: this.state, pct: pct === undefined ? (this.state === 'preparing' ? this.pct || 0 : null) : pct,
        id: this.cur ? this.cur.id : null, title: this.cur ? this.cur.title : null, buffering: this.buffering };
    // the way to sound found by trying (iDrive 8): the app offers "No sound?" while it plays
    if (this.audio.sound) o.snd = this.audio.sound.status();
    if (this.ram && this.cur) o.stored = this.cur.stored || 'flash';   // WP8 (cap `stream`): where the playing video is held
    this.emit(o);
};
// playing ahead of the video fetched so far, while the fetcher still writes it
Daemon.prototype.isBuffering = function () {
    const short = !!this.fetcher || !!(this.cur && this.cur.stream);   // (WP8: fetched again soon: not playing on)
    return this.state === 'playing' && (this.headWait || (short && this.elapsed + 1000 > this.fetchedMs));
};
Daemon.prototype.checkBuffering = function () {
    if (this.isBuffering() !== this.buffering) this.sendState();
    if (this.videoBehind && this.child && this.state === 'playing' && !this.paused && !this.headWait && !this.replayingNow()) { // ytdecSeek
        const at = Math.round(this.elapsed + (Date.now() - this.lastTickAt));
        if (at + 1000 < this.fetchedMs) {
            this.log('video: ' + this.fetchedMs + ' fetched, past the seek: ytdec again at ' + at);
            this.killYtdec();
            this.spawnYtdec(at);
        }
    }
};

Daemon.prototype.prune = function () {
    const protect = [];
    if (this.cur) protect.push(this.cur.id);
    if (this.preparing) protect.push(this.preparing);
    if (this.dl) protect.push(this.dl.id);
    if (this.dlStopping) protect.push(this.dlStopping.id);   // stopped, its fetcher not gone yet: still writing there
    const saved = savedIds(this.opts.cacheDir);
    if (!saved) { this.log('lru: the downloads cannot be read, nothing removed'); return []; }
    this.pruneRam(protect);
    return pruneCache(this.opts.cacheDir, this.opts.keep, protect, this.log, (p) => this.rmTree(p), saved);
};
// ---- downloads (0.5.0): `download` {id, meta: {title, channel, duration_s}, quality} queues id (a saved/<id> marker),
// `remove` {id} forgets it and deletes its files, `library` -> library{items: [{id, title, channel, duration_s, ready, pct?,
// error?: 'download failed' | 'not enough space', auto?}]}, newest first, also sent on every change. One at a time, as the
// fetch uid into the video's folder as a play fetches it, only with minFreeMb free, never while a play starts, plays or is
// paused: playing always wins (play() stops it; it starts again from scratch later). A cut one (a reboot, a failure) is
// never counted: ready needs every file and no .part (ytfetch.py renames each file once complete). A downloaded video
// plays from its files (play() fetches nothing for a playable folder), so with no network too.
// nothing kept in RAM past its play
Daemon.prototype.pruneRam = function (protect) { if (this.ram) pruneCache(this.ram, 0, protect, this.log, (p) => this.rmTree(p)); };
Daemon.prototype.savedFile = function (id) { return path.join(this.opts.cacheDir, SAVED_DIR, id); };
Daemon.prototype.savedMeta = function (id) { const m = readJson(this.savedFile(id)); return m && typeof m === 'object' ? m : null; };
// a download: its marker is there (one that cannot be read too: its files are kept, its failures counted)
Daemon.prototype.isSaved = function (id) { return fs.existsSync(this.savedFile(id)); };
Daemon.prototype.writeSaved = function (id, m) {
    const sd = path.join(this.opts.cacheDir, SAVED_DIR), f = this.savedFile(id);
    if (!fs.existsSync(sd)) fs.mkdirSync(sd, 0o700);   // root's, in root's cache/: the fetch uid never reaches a marker
    fs.writeFileSync(f + '.tmp', JSON.stringify(m), { mode: 0o600 });
    fs.renameSync(f + '.tmp', f);
};
// the markers as {id, ...}, newest asked first
Daemon.prototype.saved = function () {
    return (savedIds(this.opts.cacheDir) || []).map((id) => Object.assign({}, this.savedMeta(id), { id: id })).sort((a, b) => (b.at || 0) - (a.at || 0));
};
Daemon.prototype.libraryItems = function () {
    return this.saved().map((m) => {
        const ready = isReady(path.join(this.opts.cacheDir, m.id));
        const o = { id: m.id, title: String(m.title || m.id), channel: String(m.channel || ''), duration_s: m.duration_s > 0 ? m.duration_s : null, ready: ready };
        if (m.auto) o.auto = true;
        if (!ready && m.failed) o.error = 'download failed';
        else if (!ready && this.dl && this.dl.id === m.id) o.pct = this.dl.pct;
        else if (!ready && this.dlFull) o.error = 'not enough space';
        return o;
    });
};
Daemon.prototype.sendLibrary = function () {
    const items = this.libraryItems();
    this.libSig = JSON.stringify(items);
    return this.emit({ ev: 'library', items: items });
};
// the tick: the auto-downloads, the queue, and the library again when it changed (a play's own fetch completing one)
Daemon.prototype.downloadTick = function () {
    this.autoExpire();
    if (this.dl) {   // the space while one downloads: under dlStopFreeMb it stops (an unreadable df lets it go on)
        const free = this.opts.freeMb ? this.opts.freeMb(this.opts.cacheDir) : freeMb(this.opts.cacheDir);
        if (free !== null && free < this.opts.dlStopFreeMb) {
            this.log('download ' + this.dl.id + ': ' + free + ' MB free, under ' + this.opts.dlStopFreeMb + ' - stopped');
            this.dlFull = true;
            this.dlFullAt = Date.now();
            this.stopDownload();   // (its close handler removes the cut files and says not enough space)
        }
    }
    this.autoTick().catch((e) => { this.autoBusy = false; this.log('auto-download: ' + e.message); });
    this.pumpDownload();
    if (this.client && JSON.stringify(this.libraryItems()) !== this.libSig) this.sendLibrary();
};
Daemon.prototype.download = function (id, meta, quality) {
    if (!isVid(id)) return this.emit({ ev: 'error', msg: 'bad id' });
    const m = meta && typeof meta === 'object' ? meta : {}, was = this.savedMeta(id) || {}, f = readMeta(path.join(this.opts.cacheDir, id), id);
    const str = (v, n) => (typeof v === 'string' ? v.slice(0, n) : '');
    try {   // asked by hand: no longer an auto one, nor a failed one
        this.writeSaved(id, { title: (f.title !== id && f.title) || str(m.title, 300) || was.title || id, channel: f.channel || str(m.channel, 200) || was.channel || '',
            duration_s: f.duration_s || (Number(m.duration_s) > 0 ? Math.round(Number(m.duration_s)) : null) || was.duration_s || null,
            height: qualityHeight(quality, readJson(this.opts.leaseFile)), at: Date.now() });
    } catch (e) { this.log('download ' + id + ': ' + e.message); return this.sendLibrary(); }
    (this.dlFails || (this.dlFails = {}))[id] = 0;
    this.log('download asked: ' + id);
    this.thumbData(id).catch((e) => this.log('thumb ' + id + ': ' + e.message));   // kept for the library, offline
    this.sendLibrary();
    this.pumpDownload();
    return undefined;
};
Daemon.prototype.remove = async function (id) {
    if (!isVid(id)) return this.emit({ ev: 'error', msg: 'bad id' });
    const was = this.savedMeta(id);
    if (was && was.auto) (this.autoBad || (this.autoBad = {}))[id] = true;   // not brought back by the next Subscriptions look
    try { fs.unlinkSync(this.savedFile(id)); } catch (e) { /* not saved */ }
    try { fs.unlinkSync(path.join(this.opts.cacheDir, 'thumbs', id + '.rgba')); } catch (e) { /* none */ }
    this.log('download removed: ' + id);
    if (this.dl && this.dl.id === id) await this.stopDownload();
    if (!(this.cur && this.cur.id === id) && this.preparing !== id && !this.isSaved(id)) {   // its files go (the one playing: once it stops)
        try { this.rmTree(path.join(this.opts.cacheDir, id)); } catch (e) { this.log('download ' + id + ': ' + e.message); }
    }
    this.prune();
    this.sendLibrary();
    this.pumpDownload();
    return undefined;
};
// the oldest asked download not on the car yet, when nothing plays or starts and there is room
Daemon.prototype.pumpDownload = function () {
    if (this.closed || this.dl || this.dlStopping || this.fetcher || this.preparing || this.restart || ['preparing', 'playing', 'paused'].indexOf(this.state) >= 0) return undefined;
    // (dlStopping: its close handler pumps once the fetcher is gone, never an rm -rf under it)
    // offline (no lease) none starts: every one would fail for the network; after a network failure, a minute's wait
    const lease = readJson(this.opts.leaseFile);
    if (!lease || Date.now() < (this.dlNetAt || 0)) return undefined;
    const wifi = /^wlan/.test(String(lease.interface));   // the auto ones on Wi-Fi only, as autoTick picks them
    const fails = this.dlFails || (this.dlFails = {}), cache = this.opts.cacheDir;
    const next = this.saved().reverse().find((m) => !m.failed && (wifi || !m.auto) && (fails[m.id] || 0) < this.opts.dlTries && !isReady(path.join(cache, m.id)));
    if (!next) { this.dlFull = false; return undefined; }
    const id = next.id, dir = path.join(cache, id);
    const failed = (e) => {
        fails[id] = (fails[id] || 0) + 1;
        this.log('download ' + id + ': ' + e.message + ' (' + fails[id] + '/' + this.opts.dlTries + ')');
        return this.sendLibrary();
    };
    // a cut download's leftovers go first (also while waiting for space: they take it): from scratch, ytfetch.py cannot
    // go on from a .part
    try { this.rmTree(dir); } catch (e) { return failed(e); }
    if (this.dlFull && Date.now() - this.dlFullAt < 60000) return undefined;   // (df once a minute while full)
    const free = this.opts.freeMb ? this.opts.freeMb(cache) : freeMb(cache);
    if (!(free >= this.opts.minFreeMb)) {   // unreadable: no start either (the eMMC is shared with the Store and the car's tools)
        this.dlFullAt = Date.now();
        if (!this.dlFull) { this.dlFull = true; this.log('download ' + next.id + ': ' + free + ' MB free, under ' + this.opts.minFreeMb + ' - waiting'); this.sendLibrary(); }
        return undefined;
    }
    this.dlFull = false;
    let child;
    try {
        mkdirp(dir);
        fs.chmodSync(dir, VIDEO_DIR_MODE);
        this.chownFetch(dir);
        child = this.spawnJob(['fetch', '--cache-dir', this.ytc(), '--height', String(next.height || 720)].concat(this.avc1Only ? ['--avc1'] : [], this.pyzArgs(), ['--', id, dir]));
    } catch (e) {
        return failed(e);
    }
    const d = this.dl = { id: id, child: child, pct: 0, stopped: false };
    this.log('download ' + id + ': started');
    let err = null, sentPct = 0;
    readline.createInterface({ input: child.stdout }).on('line', (l) => this.guard(() => {
        let e;
        try { e = JSON.parse(l); } catch (x) { return; }
        if (e.ev === 'error') {
            err = String(e.msg);
            if (/^vp9:/.test(err) && !this.avc1Only) { this.setAvc1(); this.log('download: ' + err + ' - H.264 only from now on'); }
        } else if (e.ev === 'video' && Number(e.pct) >= 0 && this.dl === d) {
            d.pct = Math.min(99, Math.round(Number(e.pct)));
            if (d.pct - sentPct >= 5) { sentPct = d.pct; this.sendLibrary(); }
        }
    }));
    readline.createInterface({ input: child.stderr }).on('line', (l) => this.log('ytfetch (download): ' + l));
    child.on('error', (e) => { err = err || e.message; });
    child.on('close', (code, sig) => this.guard(() => {
        if (this.dl === d) this.dl = null;
        const saved = this.isSaved(id), m = this.savedMeta(id) || {};
        if (isReady(dir)) {
            this.log('download ' + id + ': done');
            const f = readMeta(dir, id);
            if (saved) { try { this.writeSaved(id, Object.assign(m, { title: f.title !== id ? f.title : m.title, channel: f.channel || m.channel, duration_s: f.duration_s || m.duration_s })); } catch (e) { this.log('download ' + id + ': ' + e.message); } }
            this.thumbData(id).catch((e) => this.log('thumb ' + id + ': ' + e.message));
        } else if (!d.stopped && saved && NET_RE.test(err || '')) {   // the network, not the video: not counted
            this.dlNetAt = Date.now() + this.opts.dlNetWaitMs;
            this.log('download ' + id + ': ' + err + ' - the network, again in ' + Math.round(this.opts.dlNetWaitMs / 1000) + ' s');
        } else if (!d.stopped && saved) {
            fails[id] = (fails[id] || 0) + 1;
            const why = err || (sig ? 'killed ' + sig : 'exit ' + code), last = PERMANENT_RE.test(why) || fails[id] >= this.opts.dlTries;
            this.log('download ' + id + ': ' + why + ' (' + fails[id] + '/' + this.opts.dlTries + ')' + (last ? ', given up' : ''));
            if (last) {   // kept as failed (asked again by hand: tried again), so a reboot does not retry it; an auto one goes
                try {
                    if (m.auto) { (this.autoBad || (this.autoBad = {}))[id] = true; fs.unlinkSync(this.savedFile(id)); } else this.writeSaved(id, Object.assign(m, { failed: true }));
                } catch (e) { this.log('download ' + id + ': ' + e.message); }
                if (!(this.cur && this.cur.id === id) && this.preparing !== id) { try { this.rmTree(dir); } catch (e) { this.log('download ' + id + ': ' + e.message); } }
            }
        }
        if (!saved) this.prune();   // no longer asked (an auto one gone meanwhile): its files go, now that the fetcher has
        this.sendLibrary();
        this.pumpDownload();
    }));
    this.sendLibrary();
    return undefined;
};
// a play starts, or the download was removed: the fetcher goes (SIGTERM, SIGKILL after 3 s); resolves once it is gone
Daemon.prototype.stopDownload = function () {
    const d = this.dl;
    if (!d) return Promise.resolve();
    this.dl = null;
    d.stopped = true;
    this.dlStopping = d;   // prune keeps its folder until the fetcher is gone
    this.log('download ' + d.id + ': stopped');
    return new Promise((resolve) => {
        const gone = () => { clearTimeout(t); if (this.dlStopping === d) this.dlStopping = null; resolve(); };
        const t = setTimeout(() => { try { d.child.kill('SIGKILL'); } catch (e) { /* gone */ } }, 3000);
        d.child.on('exit', gone);
        if (d.child.exitCode !== null || d.child.signalCode !== null) gone();
        try { d.child.kill('SIGTERM'); } catch (e) { /* gone */ }
    });
};
// ---- auto-downloads (the app's setting `auto_download` {max}, kept in state/autodl.json; absent: off): the max newest
// Subscriptions videos not watched to the end (no Shorts, live or members-only, up to autoMaxS long), looked for every
// autoEveryMs while signed in and on Wi-Fi (the uplink's wlan*: never on phone data, never with no lease). Their markers
// carry auto: true; one goes once watched, or when newer videos push it out, or when the setting goes down
Daemon.prototype.autoMax = function () {
    if (this.autoN === undefined) { const s = readJson(path.join(this.opts.stateDir, 'autodl.json')); this.autoN = s && s.max > 0 && s.max <= 10 ? Math.floor(s.max) : 0; }
    return this.autoN;
};
Daemon.prototype.setAutoMax = function (n) {
    n = Number(n) > 0 && Number(n) <= 10 ? Math.floor(Number(n)) : 0;
    if (n !== this.autoMax()) {
        this.autoN = n;
        this.autoAt = 0;   // looked for at the next tick
        try {
            if (!fs.existsSync(this.opts.stateDir)) { mkdirp(path.dirname(this.opts.stateDir)); fs.mkdirSync(this.opts.stateDir, 0o700); }
            writeIfChanged(path.join(this.opts.stateDir, 'autodl.json'), JSON.stringify({ max: n }) + '\n', 0o644);
        } catch (e) { this.log('auto-downloads: ' + e.message); }
        this.log('auto-downloads: ' + (n ? 'the ' + n + ' newest' : 'off'));
        if (this.autoExpire()) this.sendLibrary();
    }
    return this.emit({ ev: 'auto_download', max: n });
};
// the auto ones past the count (the newest kept) or watched to the end go, never the one playing; true when one went
Daemon.prototype.autoExpire = function () {
    const n = this.autoMax(), pos = this.positions();
    let gone = 0;
    this.saved().filter((m) => m.auto).forEach((m, i) => {
        if ((i < n && !(pos[m.id] && pos[m.id].done)) || (this.cur && this.cur.id === m.id)) return;
        try { fs.unlinkSync(this.savedFile(m.id)); gone++; } catch (e) { return; }
        this.log('auto-download ' + m.id + ': ' + (i < n ? 'watched' : 'over the count'));
        if (this.dl && this.dl.id === m.id) this.stopDownload();
    });
    if (gone) this.prune();
    return gone > 0;
};
Daemon.prototype.autoTick = async function () {
    const n = this.autoMax();
    if (!n || this.autoBusy || Date.now() - (this.autoAt || 0) < this.opts.autoEveryMs) return;
    const lease = readJson(this.opts.leaseFile);
    if (!lease || !/^wlan/.test(String(lease.interface)) || this.account.status().state !== 'signed_in') return;
    this.autoAt = Date.now();
    this.autoBusy = true;
    try {
        const tok = await this.account.token(false);
        let r = await this.account.browse(FEEDS.subs, tok);
        if (r.status === 401) r = await this.account.browse(FEEDS.subs, await this.account.token(true));
        if (r.status !== 200 || !r.j) { this.log('auto-download: Subscriptions ' + r.status); return; }
        const items = feedItems(r.j);
        if (!items.length) { this.log('auto-download: Subscriptions empty, nothing changed'); return; }   // (a bad answer must not drop them all)
        const pos = this.positions(), bad = this.autoBad || {}, now = Date.now();
        const want = items.filter((x) => !x.short && !x.live && !x.members && x.duration_s > 0 && x.duration_s <= this.opts.autoMaxS
            && !(pos[x.id] && pos[x.id].done) && !bad[x.id]).slice(0, n);
        const had = this.saved();
        let changed = 0;
        want.forEach((x, k) => {
            const m = had.find((h) => h.id === x.id);
            if (m && !m.auto) return;   // asked by hand: stays that
            if (!m) changed++;
            this.writeSaved(x.id, Object.assign(m || { title: x.title, channel: x.channel, duration_s: x.duration_s, height: qualityHeight(null, lease) },
                { at: now - k, auto: true }));   // in the feed's order, newest first
        });
        const stops = [];
        had.filter((m) => m.auto && !want.some((x) => x.id === m.id) && !(this.cur && this.cur.id === m.id)).forEach((m) => {
            try { fs.unlinkSync(this.savedFile(m.id)); changed++; this.log('auto-download ' + m.id + ': no longer among the newest'); } catch (e) { /* gone */ }
            if (this.dl && this.dl.id === m.id) stops.push(this.stopDownload());
        });
        await Promise.all(stops);   // its fetcher gone before the prune
        if (!changed) return;
        this.log('auto-download: ' + want.map((x) => x.id).join(' '));
        this.prune();
        this.sendLibrary();
        this.pumpDownload();
    } finally { this.autoBusy = false; }
};
// ---- send_logs {app}: the ring and ytdec.log's tail, redacted (redactLog), to the log relay as the fetch uid
// -> logs_sent{id} (the code the user quotes) or logs_error{msg: closed|busy|offline|failed}. One at a time.
Daemon.prototype.see = function (kind, id) {
    if (!/^[\w-]{2,64}$/.test(id) || this.seen[id]) return;
    this.seen[id] = kind;
    const all = Object.keys(this.seen);
    if (all.length > 2000) delete this.seen[all[0]];
};
// what a report must not carry: the account (the file, and a sign-in under way), titles and ids the daemon knows
Daemon.prototype.known = function () {
    const secrets = [], titles = [], ids = Object.assign({}, this.seen);
    const walk = (o) => {
        if (typeof o === 'string' || typeof o === 'number') secrets.push(String(o));
        else if (o && typeof o === 'object') Object.keys(o).forEach((key) => { if (key !== 'timer') walk(o[key]); });
    };
    walk(readJson(this.opts.accountFile));
    walk(this.account.cred);
    walk(this.account.pending);
    // the Store's proxy: its host, user name and password (raw and as written in a URL) stay out of reports
    const px = proxy.readSetting(this.opts.storeConf);
    const quoted = (x) => encodeURIComponent(x).replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());   // Python's quote(safe='')
    if (px && px.proxy) [px.proxy.user, px.proxy.pass].forEach((x) => { if (x) secrets.push(x, encodeURIComponent(x), quoted(x)); });
    const vid = (id) => { if (ID_RE.test(String(id))) ids[id] = 'v'; };
    const meta = (m) => { [m.title, m.channel].forEach((x) => { if (typeof x === 'string' && x !== m.id) titles.push(x); }); };
    if (this.cur) { vid(this.cur.id); meta(this.cur); }
    [this.preparing, this.nextPrefetch, this.extracting && this.extracting.id].forEach(vid);
    listCache(this.opts.cacheDir).forEach((it) => { vid(it.id); meta(it); });
    (savedIds(this.opts.cacheDir) || []).forEach((id) => { vid(id); meta(Object.assign({}, this.savedMeta(id), { id: id })); });   // auto-downloads never passed see()
    const pos = this.positions();
    Object.keys(pos).forEach((id) => { vid(id); meta(Object.assign({ id: id }, pos[id])); });
    list(path.join(this.opts.cacheDir, 'thumbs')).concat(list(path.join(this.opts.cacheDir, 'ytdlp', 'info'))).forEach((n) => vid(n.split('.')[0]));
    return { secrets: secrets, titles: titles, ids: ids };
};
// health: health.js's lines, last in the log (lastBytes keeps the newest), through the same redaction as the rest
Daemon.prototype.report = function (app, health) {
    const k = this.known(), salt = crypto.randomBytes(16).toString('hex');
    const red = (ls) => redactLog(ls.join('\n'), k, salt, this.opts.cacheDir).split('\n');
    const ytdec = tailLines(path.join(this.opts.logDir, 'ytdec.log'), 300);
    const hl = (health || []).map((h) => 'health ' + h.key + ' ' + (h.ok ? 'ok' : 'FAIL') + ' ' + h.text);
    const log = ['--- ytdec.log, last ' + ytdec.length + ' lines'].concat(ytdec, ['--- jap-ridetube-d ' + VERSION + ', last ' + this.ring.length + ' lines'], this.ring,
        hl.length ? ['--- health'].concat(hl) : []);
    return { app: /^[\d.]{1,12}$/.test(String(app)) ? String(app) : 'unknown', service: VERSION,
        os: lastBytes(red(lines(path.join(this.opts.etcDir, 'os-release')).slice(0, 5)), OS_MAX).join('\n'),
        log: lastBytes(red(log), REPORT_MAX).join('\n') };
};
Daemon.prototype.sendLogs = async function (app) {
    // BeeMTube2 sends no log off the car (upstream's report goes to its author's relay): Settings > Diagnostics shows
    // it on the screen instead (opts.reports: off; the tests of the report switch it on).
    if (!this.opts.reports) { this.log('logs: not sent (this version sends none)'); return this.emit({ ev: 'logs_error', msg: 'closed' }); }
    if (this.reporting) return undefined;
    this.reporting = true;
    try {
        // the health page's lines ride in the report (no clipboard in the car): up to healthMs (8 s) before the POST
        const body = this.report(app, await require('./health.js').run(this, VERSION));
        this.log('logs: sending ' + body.log.length + ' characters');
        const r = await this.opts.ytRequest('POST', String(this.opts.reportUrl).replace(/\/+$/, '') + '/v1/report',
            { 'User-Agent': 'jap-ridetube-d/' + VERSION }, body, this.opts.reportTimeoutMs);
        let id = null;
        try { id = JSON.parse(r.body).id; } catch (e) { /* not JSON */ }
        if (r.status === 200 && /^[A-Za-z0-9]{4,16}$/.test(String(id))) {
            this.log('logs: sent');
            return this.emit({ ev: 'logs_sent', id: String(id) });
        }
        this.log('logs: relay ' + r.status);
        return this.emit({ ev: 'logs_error', msg: r.status === 410 ? 'closed' : r.status === 429 ? 'busy' : 'failed' });
    } catch (e) {
        this.log('logs: ' + e.message);
        // no answer at all (no network, no internet uid, the relay down): offline; else something on our side
        return this.emit({ ev: 'logs_error', msg: e.noUid || /timeout|\bE[A-Z]{3,}\b|socket hang up/.test(e.message) ? 'offline' : 'failed' });
    } finally {
        this.reporting = false;
    }
};
// ---- search / thumbs
// TVHTML5 search: signed in when we can (as feed), anonymous otherwise (it answers lockupViewModels)
Daemon.prototype.search = async function (q) {
    q = typeof q === 'string' ? q.trim() : '';
    if (!q) return this.emit({ ev: 'error', msg: 'empty search' });
    const ask = (tok) => this.account.json('POST', YT + '/youtubei/v1/search', { context: { client: tvClient(this.hl, this.gl) }, query: q },
        tok ? { Authorization: 'Bearer ' + tok } : null);
    let r;
    try {
        const tok = await this.account.token(false).catch(() => null);
        r = await ask(tok);
        if (r.status === 401 && tok) r = await ask(null);
        if (r.status !== 200 || !r.j) throw new Error('search ' + r.status);
    } catch (e) {
        this.log('search: ' + e.message);
        return this.emit({ ev: 'error', msg: e.noUid ? e.message : 'search offline' });
    }
    return this.emit({ ev: 'results', q: q, items: feedItems(r.j).slice(0, FEED_MAX) });
};
Daemon.prototype.thumb = async function (id) {
    if (!isVid(id)) return this.emit({ ev: 'error', msg: 'bad id' });
    let data;
    try { data = await this.thumbData(id); } catch (e) {
        this.log('thumb ' + id + ': ' + e.message);
        return undefined; // no thumb: the app keeps its placeholder
    }
    const wh = THUMB_SIZES[data.length];
    if (!wh) { this.log('thumb ' + id + ': ' + data.length + ' bytes, not 160x90 or 320x180 RGBA'); return undefined; }
    return this.emit({ ev: 'thumb', id: id, w: wh[0], h: wh[1], rgba: data.toString('base64') });
};
// id's thumb: the cached one, else fetched (thumb.js) and kept in thumbs/ (a download's stays there, offline too)
Daemon.prototype.thumbData = async function (id) {
    const tdir = path.join(this.opts.cacheDir, 'thumbs'), tf = path.join(tdir, id + '.rgba');
    const read = (f) => readFetchFile(f, (n) => !!THUMB_SIZES[n]);   // the video folder's is the fetch uid's
    let data = read(path.join(this.opts.cacheDir, id, 'thumb.rgba')) || read(tf);
    if (data) return data;
    // at most 3 helpers at once: each is a Node 22 process, and a page asks for 20 thumbs
    const i = this.thumbN++ % this.thumbQ.length;
    const p = this.thumbQ[i].then(() => this.thumbFetch(id));
    this.thumbQ[i] = p.catch(() => {});
    data = await p;
    try { mkdirp(tdir); fs.chmodSync(tdir, 0o700); fs.writeFileSync(tf, data, { mode: 0o600 }); this.pruneThumbs(tdir); } catch (e) { this.log('thumb cache: ' + e.message); }
    return data;
};
// thumb.js <id> as the fetch uid: exactly 320x180 RGBA on stdout, exit 0; killed after 10 s
Daemon.prototype.thumbFetch = function (id) {
    return new Promise((resolve, reject) => {
        const child = this.spawnFetchUid(this.opts.thumbCmd, [id]);
        const out = [], err = [];
        let size = 0;
        const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) { /* gone */ } }, 10000);
        child.stdout.on('data', (c) => { size += c.length; if (size <= 230400) out.push(c); });
        child.stderr.on('data', (c) => err.push(c));
        child.on('error', (e) => { clearTimeout(timer); reject(e); });
        child.on('close', (code, sig) => {
            clearTimeout(timer);
            if (code === 0 && size === 230400) return resolve(Buffer.concat(out));
            return reject(new Error('thumb helper ' + (sig || 'exit ' + code) + ', ' + size + ' bytes: ' + Buffer.concat(err).toString('utf8', 0, 200).trim()));
        });
    });
};
// thumbs/ keeps the newest thumbMax (mtime), the downloads' besides
Daemon.prototype.pruneThumbs = function (tdir) {
    const saved = savedIds(this.opts.cacheDir), names = saved ? list(tdir).filter((n) => saved.indexOf(n.replace(/\.rgba$/, '')) < 0) : [];
    if (names.length <= this.opts.thumbMax) return;
    names.map((n) => { try { return { n: n, t: fs.statSync(path.join(tdir, n)).mtime.getTime() }; } catch (e) { return { n: n, t: 0 }; } })
        .sort((x, y) => y.t - x.t).slice(this.opts.thumbMax)
        .forEach((x) => { try { fs.unlinkSync(path.join(tdir, x.n)); } catch (e) { /* gone */ } });
};

// ---- playback
// restore: {ms, paused, fill, height}, a quality restart of the same video: back to ms (even under 10 s), paused if it was.
// noUpscale: the app (0.6.6+) draws the picture into the rect itself, so ytdec may send it at the video's own size
// (--no-upscale); an older app gets the rect-sized frame with its bars as before.
// Every play keeps its start in this.restart until its first tick puts it there; a pause, resume, seek (or fill) sent
// meanwhile changes it (the last word counts), a stop or another play replaces it.
Daemon.prototype.play = async function (id, rect, fill, at, quality, skip, restore, noUpscale) {
    if (!isVid(id)) return this.emit({ ev: 'error', msg: 'bad id' });
    if (!/^\d{2,4}x\d{2,4}$/.test(String(rect))) rect = '1280x720';
    const own = restore || { ms: 0, paused: false, height: qualityHeight(quality, readJson(this.opts.leaseFile)) };
    this.restart = own;
    // no YouTube position on the tile (Continue watching): ask the account's History, 2.5 s at most, meanwhile the stop
    const ask = !restore && this.opts.resume && !(Number(at) > 10000) && this.account.load()
        ? withTimeout(this.account.resumeAt(id), 2500, 'history').catch((e) => { this.log('resume: ' + e.message); return 0; }) : null;
    await Promise.all([this.stop(null), this.stopDownload()]);   // playing always wins: a download starts again later
    if (this.restart !== own) return undefined;   // stopped, or replaced, while the one before stopped
    if (ask) at = await ask;
    if (this.restart !== own) return undefined;
    if (own.fill !== undefined) fill = own.fill;
    const gen = ++this.gen;
    const dir = path.join(this.opts.cacheDir, id);
    try { fs.chmodSync(dir, VIDEO_DIR_MODE); } catch (e) { /* not fetched yet: fetchVideo makes it */ }   // (0700 before 0.3.9)
    const meta0 = readMeta(dir, id);
    // the chapters too: meta.json is the fetch uid's (a symlink or a FIFO there is no meta, never followed or waited on)
    const mb = readFetchFile(path.join(dir, 'meta.json'), (n) => n <= META_MAX);
    let chapters;
    try { chapters = mb ? JSON.parse(mb.toString('utf8')).chapters : undefined; } catch (e) { /* none */ }
    this.cur = { id: id, title: meta0.title, durationMs: meta0.duration_s > 0 ? meta0.duration_s * 1000 : undefined, dir: dir, rect: rect, fill: fill, noUpscale: !!noUpscale, gen: gen, height: qualityHeight(quality, readJson(this.opts.leaseFile)),
        skip: Array.isArray(skip) ? skip.filter((x) => typeof x === 'string') : null, chapters: chapters };
    const was = this.positions()[id];
    // the furthest of where it was left here and YouTube's own position (at: another device's): seek there on the first tick
    if (!restore && !own.sought) own.ms = !this.opts.resume ? 0 : Math.max(was && !was.done && was.ms > 10000 ? was.ms : 0, Number(at) > 10000 ? Math.floor(Number(at)) : 0);
    this.posAt = Date.now();
    this.pct = 0;
    this.fetchedMs = Infinity;
    this.head = null;
    this.underruns = [];   // the recoveries of the last minute (onPlaybackError)
    this.timeSent = false; // a `time` of this play went out (onTime)
    this.recovering = false;
    this.headWait = false;
    this.switching = false;
    this.settle = null;
    this.fullReady = true;
    let headMs = null;
    if (!isPlayable(dir)) {
        this.fullReady = false;
        this.setState('preparing');
        this.pruneRam([]);   // the last video's window first: its RAM is this one's
        const waits = this.opts.fetchRetryMs || [];
        let vp9Again = false;
        for (let k = 0; ; k++) {
            try {
                const x = this.extracting;   // its lookup is nearly there: wait for it rather than redo it
                if (x && x.id === id) await withTimeout(x.done, 8000, 'prefetch').catch(() => undefined);
                if (gen !== this.gen) return undefined;
                headMs = await this.fetchVideo(id, dir, k ? 'retry' : undefined);
                break;
            } catch (e) {
                if (gen !== this.gen) return undefined;
                if (/^vp9:/.test(e.message) && !vp9Again) { vp9Again = true; k--; continue; } // again at once, as H.264
                // the video itself refused (gone, private, members, age): no retry; else the network, maybe back soon
                if (k >= waits.length || PERMANENT_RE.test(e.message)) {
                    if (!PERMANENT_RE.test(e.message)) this.ytdlpSoon();   // after the retries: maybe a YouTube change
                    return this.failPlayback('fetch: ' + e.message);
                }
                // the retry extracts again (its stream URLs may be what failed: another uplink), without --info. Root never
                // unlinks info/<id>.json: info/ is the fetch uid's, a symlink there would point the unlink anywhere
                this.log('fetch ' + id + ': ' + e.message + ' - again in ' + waits[k] + ' ms');
                this.emit({ ev: 'retry', id: id, n: k + 1, msg: e.message });
                await sleep(waits[k]);
                if (gen !== this.gen) return undefined;
            }
        }
        if (gen !== this.gen) return undefined;
    }
    try { const now = new Date(); fs.utimesSync(dir, now, now); } catch (e) { /* LRU recency only */ }
    this.sendExtras();
    this.loadSegments(this.cur).catch((e) => this.log('sponsorblock: ' + e));
    this.trackStart(this.cur).catch((e) => this.log('history: ' + e));
    this.prune();
    this.deaths = 0;
    this.paused = false;
    this.elapsed = 0;
    this.total = 0;
    this.stoppedAt = null;
    this.heldAt = null;
    this.headFile = false;
    this.sawPlay = false;
    this.ticking = false;
    this.lastTickAt = Date.now();
    // (iDrive 8: finding the way to sound can take a while, also for a video already fetched: the app says so)
    if (this.opts.id8 && this.state !== 'preparing') { this.pct = 99; this.setState('preparing'); }
    try {
        await this.audioReady(this.opts.audioWaitMs || 10000); // never connect before the leftover sweep
        if (gen !== this.gen) return undefined;
        if (headMs !== null && !this.fullReady) this.head = { ms: headMs };
        if (own.paused) { // paused while it started (a paused quality restart): no sound until resume starts it at ms
            if (this.restart === own) this.restart = null;
            this.paused = true;
            this.stoppedAt = this.elapsed = own.ms;
            this.ticking = true;
            this.spawnYtdec(own.ms);
            this.setState('paused');
            return undefined;
        }
        this.audio.playInfo = { durationMs: this.cur.durationMs, id: id };
        await this.startAudio(path.join(this.cur.dir, this.head ? 'audio.head.m4a' : 'audio.m4a'), gen);
    } catch (e) {
        if (gen !== this.gen) return this.audio.release();
        return this.failPlayback(e.message);
    }
    if (gen !== this.gen) return this.audio.release(); // stopped while connecting
    this.ticking = true;
    this.spawnYtdec(null);
    this.setState('playing');
    if (this.head && this.fullReady) await this.switchToFull(); // audio.m4a came during the connect
    return undefined;
};
// connect + playFile, once more after a wait (BMW can refuse a route just after a disconnect: E_NOT_POSSIBLE, seen on
// marilykinson-glitch's car); gen: the play or resume it belongs to, gone stale -> the first error, no second try
Daemon.prototype.startAudio = async function (file, gen) {
    this.headFile = path.basename(file) === 'audio.head.m4a'; // the player's file: paused on the head, onFullAudio drops `head` first
    let size = '?';
    try { size = fs.statSync(file).size; } catch (e) { /* missing: playFile says so */ }
    this.log('audio: ' + path.basename(file) + ' (' + size + ' B) to the player');
    // (iDrive 8: asked once, as before - the search for the way to sound there is its own second try)
    if (this.opts.id8 || this.audio.sound) return this.audio.play(file);
    try {
        return await this.audio.play(file);
    } catch (e) {
        if (gen !== this.gen) throw e;
        this.log('audio start: ' + e.message + ' - again in ' + this.opts.audioAgainMs + ' ms');
        await this.audio.release();   // (connect may have worked before playFile failed)
        await sleep(this.opts.audioAgainMs);
        if (gen !== this.gen) throw e;
        return this.audio.play(file);
    }
};
// audio.m4a is complete. On the head: switch now; paused: resume() switches (as after a STOP mid-file)
Daemon.prototype.onFullAudio = function () {
    this.fullReady = true;
    if (!this.head || !this.ticking || this.switching) return undefined; // not playing yet: play() picks it
    if (this.paused) {
        this.stoppedAt = this.elapsed;
        this.head = null;
        this.headWait = false;
        return undefined;
    }
    return this.switchToFull();
};
// playFile(audio.m4a) on the live connection, then setTime at the extrapolated clock (ytdec keeps its own meanwhile).
// Waiting (waitFull stopped the player): audio.m4a on a new connection instead, as resume() does - the stopped player
// ignores playFile on its own (car, 2026-11-20: replay OK, then no tick, the clock frozen at a quality restart's place).
Daemon.prototype.switchToFull = async function () {
    // (a player that holds its place - iDrive 8's - is given the whole file where it waits, below, as before)
    if (this.headWait && !this.audio.holds()) {
        this.log('audio: head -> full at ' + this.elapsed + ' (waiting)');
        this.stoppedAt = this.elapsed;
        this.head = null;
        this.headWait = false;
        this.checkBuffering();
        await this.replayAt('audio switch');
        try { fs.unlinkSync(path.join(this.cur.dir, 'audio.head.m4a')); } catch (e) { /* gone, or stopped meanwhile */ }
        return undefined;
    }
    const gen = this.gen, waiting = this.headWait;
    this.switching = true;
    this.head = null;
    this.sawPlay = false;
    this.log('audio: head -> full at ' + this.elapsed + (waiting ? ' (waiting)' : ''));
    let target;
    const where = () => { const t = waiting ? this.elapsed : this.elapsed + (Date.now() - this.lastTickAt); return this.total ? Math.min(t, Math.max(0, this.total - 1000)) : t; };
    try {
        if (this.audio.holds()) {
            // the service's own player starts the whole file where the first seconds are: nothing of its start is heard
            target = where();
            await this.audio.replay(path.join(this.cur.dir, 'audio.m4a'), target);
            this.headFile = false;
        } else {
            await this.audio.replay(path.join(this.cur.dir, 'audio.m4a'));
            this.headFile = false;
            if (gen !== this.gen) return undefined;
            target = where();
            await this.audio.seek(target);
        }
    } catch (e) {
        if (gen !== this.gen) return undefined;
        this.switching = false;
        return this.failPlayback('audio switch: ' + e.message);
    }
    if (gen !== this.gen) return undefined;
    this.switching = false;
    this.headWait = false;
    this.elapsed = target;
    this.lastTickAt = Date.now();
    this.settle = { ms: target, until: Date.now() + 3000 };
    if (waiting) {
        this.ytdecSeek(target);
        if (!this.paused) this.toYtdec('resume');
    }
    if (this.paused) await this.hold(target); // paused during the switch: the playFile started it
    this.checkBuffering();
    try { fs.unlinkSync(path.join(this.cur.dir, 'audio.head.m4a')); } catch (e) { /* the player keeps reading it until then */ }
    return undefined;
};
// past the head before audio.m4a: pause the player and ytdec at ms, buffering until onFullAudio switches
Daemon.prototype.waitFull = function (ms) {
    this.log('audio: past the head at ' + ms + ', waiting for audio.m4a');
    this.headWait = true;
    this.elapsed = ms;
    this.firstTime(ms);
    this.toYtdec('pause');
    this.checkBuffering();
    this.sawPlay = false;             // (STOP: PAUSE leaves the player playing; switchToFull plays the full file anyway)
    return this.audio.button('STOP');
};
// ytfetch.py into a fresh dir; resolves once playable (audio complete or its head, and startVideoMs of video,
// or all of it), rejects if it ends before. Resolves with the head's ms when it starts on the head only, else null.
// It keeps writing the video (and the rest of the audio) after that, while we play.
// how: 'retry' (play's next try: a new extraction, no --info) or 'again' (resumeFetch: playing, into the folder as it is)
// WP8: --stream from where the play starts (its resume point): only the video's window, in ram/<id> or on flash in
// <dir>/win (its 'stream' event says which), the audio in dir as before; a video that cannot stream so goes whole into
// dir (its 'spill' event), and so does a download's video (saved/<id>: the play's fetch makes the download). at:
// {ms, videoOnly} for a restream (the video again from ms). 'again' keeps the play's way (streaming or whole).
Daemon.prototype.fetchVideo = function (id, dir, how, at) {
    return new Promise((resolve, reject) => {
        const again = how === 'again', c = this.cur;
        const stream = at ? true : again ? !!c.stream : !this.isSaved(id);
        const vram = stream && this.ram ? path.join(this.ram, id) : null, win = path.join(dir, 'win');
        const startMs = at ? at.ms : (!again && this.restart ? Math.floor(this.restart.ms) || 0 : (again ? this.elapsed : 0));
        if (!again) {
            this.rmTree(dir); // a killed fetch's leftovers
            mkdirp(dir);
            fs.chmodSync(dir, VIDEO_DIR_MODE);
            this.chownFetch(dir);
            c.vdir = dir;
        }
        if (vram && (!again || !fs.existsSync(vram))) {   // (a restream after a window on flash: RAM may fit now)
            this.rmTree(vram);
            mkdirp(vram);
            fs.chmodSync(vram, VIDEO_DIR_MODE);
            this.chownFetch(vram);
        }
        let info = !how && this.freshInfo(id) ? ['--info', this.infoFile(id)] : [];
        if (at) info = ['--info', path.join(dir, 'stream-info.json')];   // the first fetch's lookup (it looks up again when stale)
        const sargs = !stream ? [] : ['--stream', this.opts.streamAheadMs + ',' + this.opts.streamBehindMs]
            .concat(vram ? ['--video-dir', vram, '--budget', String(this.opts.ramBudget)] : [], startMs > 0 ? ['--start-ms', String(startMs)] : [],
                at && at.videoOnly ? ['--video-only'] : []);
        const child = this.spawnJob(['fetch', '--cache-dir', this.ytc(), '--height', String(this.cur.height || 720)].concat(this.avc1Only ? ['--avc1'] : [], info, sargs, this.pyzArgs(), ['--', id, dir]));
        if (child.stdin) child.stdin.on('error', () => { /* it ended: close says */ });
        this.fetcher = child;
        this.preparing = id;
        this.fetchedMs = stream ? startMs : 0;
        if (stream) { c.stream = true; c.lowMs = startMs; c.fedMax = startMs; }
        let audio = again && this.fullReady, head = null, done = false, started = again, err = null, ytdecAgain = again;
        const pct = (p) => {
            p = Math.min(99, Math.max(this.pct, Math.round(p)));
            if (p !== this.pct && this.state === 'preparing') { this.pct = p; this.sendState(); }
        };
        const check = () => {
            if (!started && (audio || head !== null) && (done || this.fetchedMs >= (c.lowMs || 0) + this.opts.startVideoMs)) { started = true; resolve(audio ? null : head); }
        };
        const end = (why) => {
            if (child === this.fetcher) {
                this.fetcher = null;
                this.preparing = null;
                if (done && why === 'exit 0') this.fetchedMs = Infinity;
                else if (started && c.stream && this.restreamLater(err || why)) { /* the video again from where it plays */ }
                else if (started && this.resumeFetch(id, dir, err || why)) { /* fetched again after a wait */ }
                else if (started && !this.fullReady) { // playing the head: the rest of the audio never comes
                    this.failPlayback('fetch: ' + (err || why)).catch((e) => this.log(e));
                    return;
                } else if (started) {
                    this.log('fetch ' + id + ': ' + (err || why));
                    this.emit({ ev: 'error', msg: 'video incomplete: ' + (err || why) });
                }
                this.checkBuffering();
            }
            if (!started) { started = true; reject(new Error(err || why)); }
        };
        readline.createInterface({ input: child.stdout }).on('line', (l) => this.guard(() => {
            if (child !== this.fetcher) return;
            let m;
            try { m = JSON.parse(l); } catch (e) { return; }
            if (m.ev === 'stream') {
                // only the two folders it may say (ytdec reads it as root)
                c.vdir = vram && m.dir === vram ? vram : win;
                c.stored = c.vdir === vram ? 'ram' : 'flash';
                if (vram && c.vdir !== vram) this.rmTree(vram);
                c.lowMs = Number(m.from_ms) || 0;
                c.behindMs = Number(m.behind_ms) || this.opts.streamBehindMs;
                this.log('fetch ' + id + ': ' + m.codec + ' window in ' + c.stored + ' (' + m.ahead_ms + ' ms ahead, ' + c.behindMs + ' behind' +
                    (m.why ? '; ' + m.why : '') + ') from ' + c.lowMs + ' ms (fragment ' + m.fragment + ' of ' + m.fragments + ')');
            } else if (m.ev === 'spill') {   // the whole video into dir, from 0 whatever the resume point
                c.stream = false;
                c.stored = 'flash';
                c.vdir = dir;
                c.lowMs = 0;
                this.fetchedMs = 0;
                if (vram) this.rmTree(vram);
                this.log('fetch ' + id + ': the whole video on flash: ' + m.why);
            } else if (m.ev === 'trim') {
                c.trimMs = Number(m.ms) || 0;
            } else if (m.ev === 'net') {
                this.log('fetch ' + id + ': ' + (m.ok ? 'the link is back after ' + m.tries + ' tries' : 'no link (' + m.msg + '), trying again'));
            } else if (m.ev === 'meta') {
                if (m.title) this.cur.title = String(m.title);
                if (m.duration_s > 0) this.cur.durationMs = m.duration_s * 1000;
                if (Array.isArray(m.chapters)) this.cur.chapters = m.chapters;
            } else if (m.ev === 'audio') pct(0.7 * (Number(m.pct) || 0));
            else if (m.ev === 'audio_head' && Number(m.ms) > 0) { if (head === null) { head = Number(m.ms); pct(70); } }
            else if (m.ev === 'audio_ready') {
                audio = true;
                pct(70);
                if (started) this.onFullAudio();
                else this.fullReady = true;
            }
            else if (m.ev === 'video') {
                this.fetchedMs = Number(m.ms) || 0;
                if (ytdecAgain && this.child && this.ticking) { // the index begun again: a ytdec reading it from the start, at the clock
                    ytdecAgain = false;
                    this.killYtdec();
                    this.spawnYtdec(this.paused || this.headWait ? this.elapsed : this.elapsed + (Date.now() - this.lastTickAt));
                }
                if (audio || head !== null) pct(70 + 29 * Math.min(1, this.fetchedMs / this.opts.startVideoMs));
            } else if (m.ev === 'done') done = true;
            else if (m.ev === 'error') {
                err = String(m.msg);
                // "vp9: ..." = a VP9 stream it could not read: H.264 from now on (play fetches again at once)
                if (/^vp9:/.test(err) && !this.avc1Only) { this.setAvc1(); this.log('fetch: ' + err + ' - H.264 only from now on'); }
            }
            check();
            this.checkBuffering();
        }));
        readline.createInterface({ input: child.stderr }).on('line', (l) => this.log('ytfetch: ' + l));
        child.on('error', (e) => end(e.message));
        child.on('close', (code, sig) => end(sig ? 'killed ' + sig : 'exit ' + code)); // close: after its last line
    });
};
// The fetcher ended early while playing (the network, mostly): fetched again into the same folder after a wait, twice a
// video at most, then as before. The sound plays on meanwhile (the player holds audio.m4a; the new one replaces it whole,
// on the head its audio_ready switches as usual), ytdec again once the new index starts. False: not this time.
// shortcut: it fetches from 0 again (ytfetch.py has no start position), audio included; WP8's streaming fetcher brings `pos`
Daemon.prototype.resumeFetch = function (id, dir, why) {
    const c = this.cur, n = (c && c.fetchAgain) || 0;
    if (!c || n >= 2 || PERMANENT_RE.test(why) || /storage almost full/.test(why)) return false;
    c.fetchAgain = n + 1;
    const gen = this.gen, wait = (this.opts.fetchRetryMs || [])[n] || 0;
    this.log('fetch ' + id + ': ' + why + ' - fetched again in ' + wait + ' ms (' + c.fetchAgain + ' of 2)');
    sleep(wait).then(() => (gen === this.gen ? this.fetchVideo(id, dir, 'again') : undefined)).catch((e) => { // (could not spawn): as before
        if (gen !== this.gen) return undefined;
        if (!this.fullReady) return this.failPlayback('fetch: ' + e.message);
        return this.emit({ ev: 'error', msg: 'video incomplete: ' + e.message });
    }).catch((e) => this.log(e));
    return true;
};
// ---- WP8: the video again from a point (a seek, a dropped link, a fetcher that ended early), into the same folder. The sound
// plays on from audio.m4a (whole, in the folder); the new fetch writes a fresh video.h264 and index.bin from the fragment
// holding ms (its 'video' events restart ytdec at the clock, as resumeFetch's do). Only once the audio is whole: before
// that the fetch that writes it must go on.
// outsideRam: ms is before what the window surely still holds, or past what is fetched by more than streamJumpMs.
// ytfetch.py punches only below fedMax - behind (what it was told, its window's behind), so that bound (plus 2 s)
// holds whether or not its 'trim' came yet; the stream's own start needs no margin.
Daemon.prototype.outsideRam = function (ms) {
    const c = this.cur;
    if (!c || !c.stream || !this.fullReady || this.head) return false;
    const low = Math.max(c.lowMs || 0, (c.fedMax || 0) - (c.behindMs || this.opts.streamBehindMs) + 2000);
    return ms < low || (this.fetchedMs !== Infinity && ms > this.fetchedMs + this.opts.streamJumpMs);
};
Daemon.prototype.restream = function (ms, why) {
    const c = this.cur;
    if (!c || !c.stream || !this.fullReady) return false;
    c.restreamAt = Date.now();
    const n = c.restreams = (c.restreams || 0) + 1;   // a newer restream before the old fetcher is gone: only it goes on
    this.log('restream ' + c.id + ' at ' + ms + ' (' + why + '; window from ' + (c.lowMs || 0) + ', fetched to ' + this.fetchedMs + ')');
    const gen = this.gen;
    this.stopFetch().then(() => {
        if (gen !== this.gen || this.cur !== c || c.restreams !== n) return undefined;
        ['video.h264', 'video.h264.part', 'index.bin'].forEach((f) => { try { fs.unlinkSync(path.join(c.vdir, f)); } catch (e) { /* none */ } });
        this.fetchedMs = ms;
        this.checkBuffering();
        return this.fetchVideo(c.id, c.dir, 'again', { ms: ms, videoOnly: true });
    }).catch((e) => {
        if (gen !== this.gen) return;
        this.log('restream ' + c.id + ': ' + e.message);
        this.emit({ ev: 'error', msg: 'video incomplete: ' + e.message });
    });
    return true;
};
// the link was gone a while (a tunnel): playback is past what is fetched by more than streamJumpMs: the video from there,
// not the gap; at most every restreamMs
// Also back: a seek before where the stream starts (a resume, then a seek to 0) made while the head played, or while
// waiting for audio.m4a, cannot restream then (the fetcher still writes the audio): the first tick on the full file does.
Daemon.prototype.catchUp = function (elapsed) {
    const c = this.cur;
    if (!c || !c.stream || !this.fullReady || this.head || Date.now() - (c.restreamAt || 0) < this.opts.restreamMs) return;
    if (elapsed + 2000 < (c.lowMs || 0)) return void this.restream(elapsed, 'before the stream');
    if (this.fetchedMs === Infinity || elapsed <= this.fetchedMs + this.opts.streamJumpMs) return;
    this.restream(elapsed, 'behind the clock');
};
// the streaming fetcher ended early while playing: the video again from where it plays, after a wait, 5 times a video at
// most (each fetch already tries the link again for as long as it runs: this is for what it could not get past)
Daemon.prototype.restreamLater = function (why) {
    const c = this.cur;
    if (!c || !this.fullReady || PERMANENT_RE.test(why) || (c.restreamDied || 0) >= 5) return false;
    c.restreamDied = (c.restreamDied || 0) + 1;
    const gen = this.gen, wait = (this.opts.fetchRetryMs || [])[Math.min(c.restreamDied - 1, 1)] || 0;
    this.log('fetch ' + c.id + ': ' + why + ' - the video again in ' + wait + ' ms (' + c.restreamDied + ' of 5)');
    sleep(wait).then(() => {
        if (gen !== this.gen || this.cur !== c || this.fetcher) return;
        this.restream(this.paused || this.headWait ? this.elapsed : this.elapsed + (Date.now() - this.lastTickAt), 'the fetch ended: ' + why);
    }).catch((e) => this.log(e));
    return true;
};
// SIGTERM (it leaves .part files), SIGKILL after 3 s; resolves when it is gone. Called again meanwhile: the same wait
// (a fetcher still exiting may still write the folder)
Daemon.prototype.stopFetch = function () {
    const c = this.fetcher;
    if (!c) return this.fetchGone || Promise.resolve();
    this.fetcher = null;
    this.preparing = null;
    const p = this.fetchGone = new Promise((resolve) => {
        const t = setTimeout(() => { try { c.kill('SIGKILL'); } catch (e) { /* gone */ } }, 3000);
        c.on('exit', () => { clearTimeout(t); resolve(); });
        if (c.exitCode !== null || c.signalCode !== null) { clearTimeout(t); resolve(); }
        try { c.kill('SIGTERM'); } catch (e) { /* gone */ }
    });
    p.then(() => { if (this.fetchGone === p) this.fetchGone = null; });
    return p;
};
Daemon.prototype.failPlayback = async function (msg) {
    this.log('error: ' + msg);
    const g = ++this.gen;   // what a resume, recovery or start under way checks: none may open the audio again
    this.restart = null;
    this.ticking = false;
    this.killYtdec();
    await Promise.all([this.audio.release(), this.stopFetch()]);
    if (g !== this.gen) return;   // a new video (or a stop) came in meanwhile: the error was the old one's
    this.setState('error');
    // offline: no uplink lease, or the network's own words (the app then points to the downloads)
    const offline = !fs.existsSync(this.opts.leaseFile) || NET_RE.test(msg);
    this.emit(Object.assign({ ev: 'error', msg: msg }, offline ? { offline: true } : {}));
    this.pumpDownload();
};
// reason: 'idle' | 'ended' | null (internal, before a new play: no state event)
Daemon.prototype.stop = async function (reason) {
    if (reason) {   // a stop cancels a start under way (a quality restart's too), and none starts while it tears down
        this.restart = null;
        if (this.cur) this.cur.stopped = true;
    }
    if (this.ticking) {
        this.savePos(true);
        if (reason === 'ended' && this.pos && this.cur && this.pos[this.cur.id]) this.pos[this.cur.id].done = true;
        const p = this.cur && this.pos && this.pos[this.cur.id];
        if (this.cur && this.cur.track) this.ping(this.cur, true, !!(p && p.done));
    }
    const g = ++this.gen;
    if (reason && this.loop === 'carried') this.loop = false;   // a loop carried over a reconnect: that video's only
    // a replay still waiting for its file's first tick (a resume, a loop) ends here: left waiting, the next play's first
    // tick (the one that takes it to its resume point) would only wake it
    if (this.tickWaiter) { const w = this.tickWaiter; this.tickWaiter = null; w(); }
    this.ticking = false;
    this.killYtdec();
    await Promise.all([this.audio.release(), this.stopFetch()]);
    if (g !== this.gen) return;   // another stop or a play came in while this one waited: the late tail would wipe it
    if (reason) {
        this.setState(reason);
        if (this.pos) this.sendPositions();
        this.cur = null;
        this.prune();          // a played video goes (not a download)
        this.pumpDownload();
    }
};
/* The driver hears nothing: the way that plays is left out from now on and the video goes on, from where it is, with
 * the next way (sound.js). */
Daemon.prototype.noSound = async function () {
    const s = this.audio.sound, c = this.cur;
    if (!s || !c || (this.state !== 'playing' && this.state !== 'paused')) return this.emit({ ev: 'error', msg: 'nothing plays through a way that can be changed' });
    const name = s.nosound();
    if (!name) return this.emit({ ev: 'error', msg: 'nothing plays through a way that can be changed' });
    this.log('no sound through ' + name + ': the next way');
    await this.stop(null);
    return this.play(c.id, c.rect, c.fill, null, String(c.height || ''), c.skip);
};
// a start under way that pause, resume and seek leave to this.restart (its first tick applies it): a play not at the
// player yet, or a quality restart until that tick (a setTime before it is dropped)
Daemon.prototype.waiting = function () { return !!(this.restart && (this.restart.restore || !this.ticking)); };
Daemon.prototype.pause = async function () {
    if (this.restart) this.restart.paused = true;
    if (this.waiting()) return;
    if (this.state !== 'playing') return;
    this.paused = true;
    this.toYtdec('pause');
    this.setState('paused'); // at once: the button's reply can take seconds, and a second tap must not send it twice
    // the MediaPlayer answers OK to PAUSE (and PLAY) and plays on (car, 2026-11-03); STOP does stop it, and resume()
    // starts the file again where it stopped
    this.savePos(true);
    if (this.cur && this.cur.track) this.ping(this.cur, true);
    if (this.headWait) { this.firstTime(this.elapsed); return; } // already stopped, waiting for audio.m4a
    let at = this.elapsed; // (the last tick: at most a fraction of a second back)
    if (this.restart && !this.restart.restore) { // before the play's first tick: resume() starts at its place
        if (this.restart.ms > 0) at = Math.floor(this.restart.ms);
        this.restart = null;
    }
    this.firstTime(at);
    await this.hold(at);
};
// the app's bar and clock need one `time` per play, tick or not (car, 0.6.0: a pause, or a wait for audio.m4a, at the
// start left none). The head's own ~60 s total is no length
Daemon.prototype.firstTime = function (at) {
    const total = this.cur && (this.cur.durationMs || (this.head || this.headFile ? 0 : this.total));
    if (this.timeSent || !(total > 0)) return;
    this.timeSent = true;
    if (!this.total) this.total = total;
    this.emit({ ev: 'time', elapsed: at, total: total });
};
// the player stopped at ms (STOP: our own, not the end of the file), for resume() to start it there again
Daemon.prototype.hold = function (ms) {
    this.stoppedAt = ms;
    this.elapsed = ms;
    this.sawPlay = false;
    this.heldAt = ms;   // the player's place at our STOP: a tick well past it, it plays on (heldOn)
    this.heldStops = 1;
    this.heldStopAt = Date.now();
    return this.audio.button('STOP');
};
// a tick at ms while paused past heldAt: the player ignored our STOP (car, 0.4.9: the head file played to its end, the
// clock moving on under "paused"). STOP again (at most every heldStopMs); still playing after that one: the connection
// goes (no sound), resume() connects again at stoppedAt as after any STOP mid-file
Daemon.prototype.heldOn = function (ms) {
    if (this.heldStops > 2 || Date.now() - this.heldStopAt < this.opts.heldStopMs) return undefined;
    this.heldStopAt = Date.now();
    if (this.heldStops++ < 2) {
        this.log('audio: the player plays on at ' + ms + ' while paused at ' + this.stoppedAt + ': STOP again');
        return this.audio.button('STOP');
    }
    this.log('audio: the player still plays at ' + ms + ' after a second STOP: audio released, resume connects again at ' + this.stoppedAt);
    return this.audio.release().catch((e) => this.log('release: ' + e));
};
Daemon.prototype.resume = async function () {
    if (this.restart) this.restart.paused = false;
    if (this.waiting()) return;
    if (this.state !== 'paused') return;
    this.paused = false;
    this.heldAt = null;
    this.lastProgress = Date.now(); this.played = 0;
    this.setState('playing');
    if (this.headWait) return; // still past the head: ytdec and the player stay paused until the switch
    if (this.stoppedAt !== null && this.stoppedAt !== undefined) { // the player stopped mid-file: start it again there
        // the service's own player kept its place while stopped: it goes on there (or where a seek meanwhile put it)
        if (this.audio.holds()) {
            const gen = this.gen, at = this.stoppedAt;
            this.sawPlay = false; // the new file's own STOP/PLAY is not a stop mid-file
            this.ytdecSeek(at);
            this.toYtdec('resume');
            let on = false;
            try { await this.audio.seek(at); on = (await this.audio.button('PLAY')) !== false; } catch (e) { this.log('resume in place: ' + e.message); }
            if (gen !== this.gen || this.state !== 'playing') return; // stopped, or paused again meanwhile
            if (on) { this.stoppedAt = null; this.settle = { ms: at, until: Date.now() + 3000 }; return; }
            // (the car gave its sound to another source meanwhile: our way is asked for anew, as below)
            this.log('the sound did not go on in place: its way is started anew');
        }
        return this.replayAt('resume');
    }
    this.toYtdec('resume');
    await this.audio.button('PLAY');
};
// Majordome's mic hold (0.5.2; its mic has no echo cancelling: it heard the video, not the driver). Root only and out of
// band (SIGWINCH, SIGUSR2 to the pid in ytd.pid), never through the app's single ctl client. hold: the app's own pause,
// if it plays; again while held: the timer re-armed. release: the app's own resume, only while that pause is still the
// hold's (any other state meanwhile drops it: setState). Not released within micHoldMs: released by itself
Daemon.prototype.micArm = function () {
    clearTimeout(this.micHeld.timer);
    this.micHeld.timer = setTimeout(() => this.guard(() => this.micRelease('none in ' + this.opts.micHoldMs + ' ms')), this.opts.micHoldMs);
};
Daemon.prototype.micHold = function () {
    if (this.micHeld) return this.micArm();
    if (this.state !== 'playing' || this.waiting()) return undefined;   // (waiting: pause() would only defer it, no 'paused')
    const p = this.pause();   // 'paused' set before its first await
    if (this.state !== 'paused') return p;
    this.micHeld = {};
    this.micArm();
    this.log('mic hold: paused');
    return p;
};
Daemon.prototype.micRelease = function (why) {
    if (!this.micHeld) return undefined;
    clearTimeout(this.micHeld.timer);
    this.micHeld = null;
    this.log('mic hold: released (' + why + '): resume');
    return this.resume();
};
// The player stopped (our STOP, or it ran dry): start the file again at stoppedAt. Stopped, it ignores playFile and PLAY
// (car, 2026-11-03): a new connection, as a new video starts. why: the error's prefix if that fails.
// Until our setTime the new file's times (from 0) are not the place (QJZ9TX). A newer replayAt (a second resume, an
// underrun) takes over: the older one stops at its next await, without failing the play or taking stoppedAt.
Daemon.prototype.replayAt = async function (why) {
    const me = { gen: this.gen };
    this.replaying = me;
    try { await this.replayFile(why, me); } finally { if (this.replaying === me) this.replaying = null; }
};
Daemon.prototype.replayingNow = function () { return !!this.replaying && this.replaying.gen === this.gen; };
Daemon.prototype.replayFile = async function (why, me) {
    const gen = me.gen, gone = () => gen !== this.gen || this.replaying !== me;
    this.sawPlay = false; // the new file's own STOP/PLAY is not a stop mid-file
    this.ytdecSeek(this.stoppedAt);
    this.toYtdec('resume');
    try {
        await this.audio.release();
        if (gone()) return;
        await this.startAudio(path.join(this.cur.dir, this.head ? 'audio.head.m4a' : 'audio.m4a'), gen);
    } catch (e) {
        if (!gone()) await this.failPlayback(why + ': ' + e.message);
        return;
    }
    // (connected after a teardown had released: let it go too)
    if (gen !== this.gen && (this.state === 'error' || this.state === 'idle')) await this.audio.release().catch(() => undefined);
    if (gone() || this.state !== 'playing') return; // stopped, paused again or taken over meanwhile
    // a setTime before the new file's first tick is dropped (it started at 0 on the car): wait for one (3 s at most)
    let w;
    await new Promise((r) => { w = r; this.tickWaiter = r; setTimeout(r, 3000); });
    if (this.tickWaiter === w) this.tickWaiter = null;
    if (gone() || this.state !== 'playing') return;
    const at = this.stoppedAt;  // (a seek meanwhile moved it: seek() keeps it while the file restarts)
    this.stoppedAt = null;
    this.settle = { ms: at, until: Date.now() + 3000 };
    this.lastTickAt = Date.now(); // the clock extrapolates from here, not from before the stop
    await this.audio.seek(at);
};
// `loop` on, the audio at its end: the same video again from 0 with nothing torn down. The player stopped at the file's
// end, so this is a stop mid-file held at 0 (seek keeps stoppedAt) and started again there (replayAt: a new connection,
// as resume does); no `paused` or `ended` said. The lookup and audio.m4a stay; a stream's window is fetched again from 0
// (seek's restream: the video only), a whole video (a download) is just sought. ytdec stays up after its eof (a seek
// plays again). SponsorBlock skips again. False (not looped): not playing the full audio yet, or a start under way.
Daemon.prototype.loopAgain = function () {
    const c = this.cur;
    if (!c || this.state !== 'playing' || this.head || !this.fullReady || this.restart) return false;
    this.log('loop ' + c.id + ': again from 0');
    // the pass watched to its end: kept (done) and told to the History before the clock goes back; the next watchtime
    // range starts at 0 (left at the end, it would run backwards: st past et)
    this.savePos(true);
    if (c.track) { this.ping(c, true); c.track.st = 0; }
    (c.segs || []).forEach((x) => { x.done = false; });
    this.stoppedAt = this.elapsed;   // (held: seek moves it to 0 and leaves the stopped player alone)
    this.heldAt = null;
    this.eof = false;
    this.seek(0).catch((e) => this.log('loop seek: ' + e));
    this.emit({ ev: 'time', elapsed: 0, total: this.total });
    this.replayAt('loop').catch((e) => this.log('loop: ' + e));
    return true;
};
Daemon.prototype.seek = async function (ms) {
    ms = Math.max(0, Math.floor(Number(ms) || 0));
    if (this.restart) { this.restart.ms = ms; this.restart.sought = true; } // the start under way ends there, not where it was left
    if (this.waiting()) return;
    if (this.state !== 'playing' && this.state !== 'paused') return;
    if (this.total) ms = Math.min(ms, Math.max(0, this.total - 1000));
    this.elapsed = ms;
    this.lastProgress = Date.now(); this.played = 0;
    this.lastTickAt = Date.now();
    this.settle = null; // a seek back right after the switch: its times are ours
    this.ytdecSeek(ms);
    if (this.cur && this.cur.stream) {
        if (this.outsideRam(ms)) this.restream(ms, 'seek');
        // a far seek before audio.m4a: not fed, the fetcher would fetch toward ms (punching it all) beside the audio it
        // still writes; its window stays full where it was, and the first tick on the full file restreams (catchUp)
        else if (this.fullReady || ms <= this.fetchedMs + this.opts.streamJumpMs) this.feedPos(ms, true);
    }
    // shortcut: once waiting, a seek back into the head still waits for audio.m4a (a few s), simpler than replaying the head
    if (this.headWait) return this.checkBuffering();
    if (this.head && !this.switching && ms > this.head.ms - this.opts.headMarginMs) return this.waitFull(ms);
    this.checkBuffering();
    if (this.stoppedAt !== null && this.stoppedAt !== undefined) { this.stoppedAt = ms; return undefined; } // resume() replays there
    await this.audio.seek(ms);
};
Daemon.prototype.setVideo = function (on) {
    this.videoOn = on;
    this.lastProgress = Date.now(); this.played = 0;
    this.toYtdec('video ' + (on ? 1 : 0));
};

// zoom while playing: ytdec again at the clock with the other mode (the sound plays on)
Daemon.prototype.setFill = function (on) {
    const c = this.cur;
    if (!c || c.fill === on) return;
    c.fill = on;
    if (!this.child) return; // the next ytdec takes it
    this.killYtdec();
    this.spawnYtdec(this.paused || this.headWait ? this.elapsed : this.elapsed + (Date.now() - this.lastTickAt));
};
// Video quality while playing: the same video fetched again at that height, from where it is (under 10 s too), paused
// if it was. A restart already under way carries its place and mode over; the last quality asked wins.
Daemon.prototype.requality = async function (quality) {
    const c = this.cur, h = qualityHeight(quality, readJson(this.opts.leaseFile)), was = this.restart;
    if (!c || c.stopped || h === (was ? was.height : c.height)) return undefined;
    if (this.isSaved(c.id) && isReady(c.dir)) return undefined;   // a download keeps its file (and plays offline)
    return this.refetch(quality, h);
};
// the same video fetched again at quality (height h) from where it is, paused if it was (a restart under way: its place)
Daemon.prototype.refetch = async function (quality, h) {
    const c = this.cur, was = this.restart;
    const restore = { ms: was ? was.ms : this.elapsed, paused: was ? was.paused : this.state === 'paused', fill: was ? was.fill : undefined, height: h, restore: true };
    this.restart = restore;
    await this.stop(null);
    if (this.restart !== restore) return undefined;   // stopped, replaced or asked again meanwhile: this one is cancelled
    this.rmTree(c.dir);
    return this.play(c.id, c.rect, c.fill, null, quality, c.skip, restore, c.noUpscale);
};

// ---- MediaPlayer events
Daemon.prototype.onTime = function (v) {
    if (!this.ticking || !v || typeof v.elapsed !== 'number') return;
    if (!(v.total > 1)) return; // {elapsed:0,total:1} right after playFile
    if (this.tickWaiter) { this.tickWaiter(); return; } // resume(): the restarted file plays, its setTime can follow
    if (this.replayingNow()) return; // replayAt: the file restarts at 0, our setTime is still to come
    if (this.switching) return; // the restart's times
    if (this.paused && this.stoppedAt !== null && this.stoppedAt !== undefined) { // paused: the place is stoppedAt, not the player's
        if (this.heldAt !== null && v.elapsed > this.heldAt + 1500) this.heldOn(v.elapsed);   // (also waiting past the head)
        if (!this.total && !this.head && !this.headFile) this.total = v.total; // no length known at the pause: this tick's
        this.firstTime(this.stoppedAt);
        return;
    }
    if (this.headWait) return; // a paused player
    if (this.settle && Date.now() < this.settle.until && v.elapsed < this.settle.ms - 2000) return; // before our setTime landed
    const step = v.elapsed - this.elapsed;
    if (step > 0 && step < 2000) this.played = (this.played || 0) + step;   // the watchdog's audio time (seeks excluded)
    this.elapsed = v.elapsed;
    this.lastTickAt = Date.now();
    this.total = this.head || this.headFile ? (this.cur.durationMs || v.total) : v.total; // the head's own total is ~60 s
    if (this.head && !this.fullReady && v.elapsed >= this.head.ms - this.opts.headMarginMs) {
        this.waitFull(v.elapsed).catch((e) => this.log(e));
        return;
    }
    if (this.restart) { // the play's start: where it was left (or the quality restart's place), paused if asked meanwhile
        const r = this.restart, at = Math.floor(r.ms) || 0;
        this.restart = null;
        (at > 0 ? this.seek(at) : Promise.resolve()).then(() => (r.paused && this.state === 'playing' ? this.pause() : undefined))
            .catch((e) => this.log('resume at: ' + e));
        if (at > 0) {
            // the player's next times can still be the old place: none of them goes out as `time` (the app's first one
            // after a play says where it resumed: "Resumed at 12:34")
            this.settle = { ms: at, until: Date.now() + 3000 };
            return;
        }
    }
    if (this.skipSegments()) {
        // a play from the start landing in a segment: its first `time` is still this tick's, not the segment's end
        // (past 10 s, the app would read the play as resumed there)
        if (!this.timeSent) { this.timeSent = true; this.emit({ ev: 'time', elapsed: v.elapsed, total: this.total }); }
        return;
    }
    this.savePos(false);
    if (this.cur.track && Date.now() - this.cur.track.at > 60000) this.ping(this.cur, true);
    this.toYtdec('clock ' + v.elapsed + ' ' + monoMs());
    this.feedPos(v.elapsed, false);
    this.catchUp(v.elapsed);
    this.checkBuffering();
    const now = Date.now();
    if (now - this.lastTimeEv >= 240) { // <= 4/s
        this.lastTimeEv = now;
        this.timeSent = true;
        this.emit({ ev: 'time', elapsed: v.elapsed, total: this.total });
    }
};
Daemon.prototype.onPlayerState = function (v) {
    if (!this.ticking) return;
    if (v === 'PLAYER_STATE_PLAY') this.sawPlay = true; // also mid-switch: the full file's own PLAY arms its end
    if (this.switching) return;
    // paused with the place kept (a STOP heeded late, or the file ran out while it played on): resume() starts it there
    if (v === 'PLAYER_STATE_STOP' && this.paused && this.stoppedAt !== null && this.stoppedAt !== undefined) { this.sawPlay = false; return; }
    // the head file ran out (before onTime saw its end): wait for audio.m4a, never `ended`
    if (v === 'PLAYER_STATE_STOP' && this.head && this.sawPlay) {
        this.sawPlay = false;
        if (!this.headWait) this.waitFull(this.elapsed).catch((e) => this.log(e));
        return;
    }
    // STOP we did not ask for, after it played: the end of the file when near it, else the player
    // stopped mid-file (seen after a PLAY sent while playing): hold as paused, resume restarts it there.
    if (v === 'PLAYER_STATE_STOP' && this.sawPlay) {
        this.sawPlay = false;
        if (!this.total || this.total - this.elapsed < 4000) {
            if (this.loop && this.loopAgain()) return;
            this.log('audio ended');
            this.stop('ended').catch((e) => this.log('stop: ' + e));
            return;
        }
        this.log('audio stopped at ' + this.elapsed + ' of ' + this.total + ': held as paused');
        this.stoppedAt = this.elapsed;
        this.heldAt = null;   // stopped by itself: no STOP of ours to repeat
        this.paused = true;
        this.toYtdec('pause');
        this.setState('paused');
    }
};
// ITEM_BUFFER_UNDERRUN (marilykinson-glitch's car, 2026-11-18, {event: ...} on a long video): the player ran dry, not the
// end. On the head before audio.m4a: wait for it, as at the head's end. During the head -> full switch (or the wait for it),
// a recovery under way, or paused: the same stall, ignored. Else the player again at the clock after a short wait, 3 times
// in a rolling minute, then an error. Any other error event: an error, as before.
// shortcut: matched as UNDERRUN anywhere in the event (one capture), check the exact shape on the car
const UNDERRUN_MAX = 3;
Daemon.prototype.onPlaybackError = function (v) {
    if (!this.ticking) return undefined;
    const msg = 'playback error ' + JSON.stringify(v);
    const fail = (m) => this.failPlayback(m).catch((e) => this.log(e));
    if (/ITEM_UNPLAYABLE/.test(msg) && this.cur && !this.cur.unplayableRetried && !this.total && !this.paused &&
        !this.headWait && !this.switching && !this.recovering && !this.replayingNow()) {
        return this.retryUnplayable(msg).catch((e) => this.log(e));
    }
    if (!/UNDERRUN/.test(msg)) return fail(msg);
    if (this.switching || this.headWait || this.recovering || this.paused || this.replayingNow()) {
        this.log('audio: underrun at ' + this.elapsed + (this.recovering ? ' while recovering' : this.paused ? ' while paused' : this.switching || this.headWait ? ' during the head -> full switch' : ' while the file restarts') + ', ignored');
        return undefined;
    }
    if (this.head && !this.fullReady) {
        this.log('audio: underrun on the head at ' + this.elapsed + ', waiting for audio.m4a');
        return this.waitFull(this.elapsed).catch((e) => this.log(e));
    }
    const now = Date.now();
    this.underruns = this.underruns.filter((t) => now - t < 60000);
    if (this.underruns.length >= UNDERRUN_MAX) return fail('audio underrun: ' + UNDERRUN_MAX + ' recoveries in a minute, stopped');
    this.underruns.push(now);
    return this.recover().catch((e) => this.log('underrun recovery: ' + e));
};
// ITEM_UNPLAYABLE right after playFile, no tick yet (AX4NVT, NW2JEL 2026-11-24/25, our car 2026-11-09): the player had
// read the file's metadata, so the file was opened. Once per video: a new connection and the same file (or audio.m4a,
// complete meanwhile); the deep-resume seek still follows the first tick. A second one: the error, as before.
// shortcut: root cause unproven (player/route state or codec), one blind retry; drop it once the cause is known
Daemon.prototype.retryUnplayable = async function (msg) {
    const gen = this.gen;
    this.cur.unplayableRetried = true;
    this.switching = true;   // the dead file's STOP and times are not ours
    this.sawPlay = false;
    this.toYtdec('pause');   // no sound meanwhile: the picture waits too (as recover() does)
    this.log('audio: ' + msg + ' before the first tick, the player again in ' + this.opts.unplayableWaitMs + ' ms');
    try {
        await this.audio.release();
        await sleep(this.opts.unplayableWaitMs);
        // stopped, paused, a resume's replay or reconnected meanwhile
        if (gen !== this.gen || this.state !== 'playing' || this.paused || this.replayingNow() || this.audio.connId !== null) return;
        if (this.fullReady && this.head) { // audio.m4a complete meanwhile: that one, the head goes
            this.head = null;
            try { fs.unlinkSync(path.join(this.cur.dir, 'audio.head.m4a')); } catch (e) { /* gone */ }
        }
        await this.startAudio(path.join(this.cur.dir, this.head ? 'audio.head.m4a' : 'audio.m4a'), gen);
    } catch (e) {
        if (gen === this.gen) { this.switching = false; await this.failPlayback(msg); }
        return;
    } finally {
        if (gen === this.gen) this.switching = false;
        if (gen === this.gen && this.state === 'playing' && !this.paused) this.toYtdec('resume');
    }
    // (connected after a teardown had released: let it go too)
    if (gen !== this.gen && (this.state === 'error' || this.state === 'idle')) await this.audio.release().catch(() => undefined);
    if (gen === this.gen && this.head && this.fullReady && !this.paused) await this.switchToFull(); // audio.m4a came during the connect (onFullAudio skipped it)
};
Daemon.prototype.recover = async function () {
    const gen = this.gen;
    this.recovering = true;
    this.sawPlay = false;   // the dry player's own STOP is neither the end nor a stop mid-file
    this.toYtdec('pause');
    this.log('audio: underrun at ' + this.elapsed + ', the player again there in ' + this.opts.underrunWaitMs + ' ms');
    try {
        await sleep(this.opts.underrunWaitMs);
        if (gen !== this.gen || this.state !== 'playing' || this.paused) return; // stopped, failed or paused meanwhile
        this.stoppedAt = this.elapsed;
        await this.replayAt('underrun recovery');
    } finally {
        if (gen === this.gen) this.recovering = false;
    }
};

// ---- ytdec
Daemon.prototype.spawnYtdec = function (seekMs) {
    const c = this.cur;
    const args = ['--dir', c.vdir || c.dir, '--rect', c.rect];
    if (c.fill) args.push('--fill');
    if (c.noUpscale) args.push('--no-upscale');
    args.push('--log', path.join(this.opts.logDir, 'ytdec.log'));
    let child;
    try {
        child = cp.spawn(this.opts.ytdec, args, { stdio: ['pipe', 'pipe', 'ignore'] });
    } catch (e) {
        return this.ytdecDied(null, 'spawn: ' + e.message);
    }
    this.child = child;
    this.lastProgress = Date.now(); this.played = 0;
    this.lastDecoded = -1;
    this.eof = false;
    child.stdin.on('error', () => { /* EPIPE when it dies: handled by exit */ });
    child.on('error', (e) => this.ytdecDied(child, e.message));
    child.on('exit', (code, sig) => (code === 4 ? this.ytdecCodec(child) : this.ytdecDied(child, 'exit ' + (sig || code))));
    readline.createInterface({ input: child.stdout }).on('line', (l) => this.guard(() => this.onYtdecLine(child, l)));
    this.ytdecKnew = this.fetchedMs;   // its index at the start: the video fetched so far
    this.videoBehind = seekMs !== null && seekMs + 1000 >= this.fetchedMs;
    if (seekMs !== null) this.toYtdec('seek ' + seekMs);
    if (this.paused || this.headWait) this.toYtdec('pause');
    if (!this.videoOn) this.toYtdec('video 0');
    this.log('ytdec ' + args.join(' ') + (seekMs !== null ? ' (restart at ' + seekMs + ')' : ''));
};
// ytdec reads its index again only when it runs out of frames, and a seek past the frames it has starts at its last
// keyframe and decodes up to the target unseen (car, 2026-11-20: ~100 s of a frozen picture, the sound playing, after a
// quality restart at 23 min; the whole index was there after 12 s). So past what this ytdec knew at its start: a new one
// there, which reads all of it; past the video fetched: the seek now, a new ytdec at the clock once it is (checkBuffering)
Daemon.prototype.ytdecSeek = function (ms) {
    if (!this.child) return;
    if (ms + 1000 < this.ytdecKnew) { this.toYtdec('seek ' + ms); return; }
    if (ms + 1000 < this.fetchedMs) { this.killYtdec(); this.spawnYtdec(ms); return; }
    this.videoBehind = true;
    this.toYtdec('seek ' + ms);
};
Daemon.prototype.toYtdec = function (line) {
    if (!this.child) return;
    try { this.child.stdin.write(line + '\n'); } catch (e) { /* dying */ }
};
Daemon.prototype.onYtdecLine = function (child, l) {
    if (child !== this.child) return;
    let m;
    try { m = JSON.parse(l); } catch (e) { return; }
    if (m.ev === 'stats') {
        if (typeof m.decoded === 'number' && m.decoded !== this.lastDecoded) {
            this.lastDecoded = m.decoded;
            this.lastProgress = Date.now(); this.played = 0;
        }
    } else if (m.ev === 'ready') {
        this.lastProgress = Date.now(); this.played = 0;
    } else if (m.ev === 'eof') {
        this.eof = true;
    } else if (m.ev === 'error') {
        this.log('ytdec error: ' + m.msg);
    }
};
Daemon.prototype.killYtdec = function () {
    const c = this.child;
    if (!c) return;
    this.child = null;
    try { c.stdin.write('quit\n'); } catch (e) { /* gone */ }
    try { c.stdin.end(); } catch (e) { /* gone */ }
    const t = setTimeout(() => { try { c.kill('SIGKILL'); } catch (e) { /* gone */ } }, 1000);
    c.on('exit', () => clearTimeout(t));
};
// Child died on its own (or the watchdog killed it): restart once at the clock, then audio only.
Daemon.prototype.ytdecDied = function (child, why) {
    if (child && child !== this.child) return; // a child we already let go
    this.child = null;
    if (!this.ticking) return;
    this.deaths++;
    this.log('ytdec died (' + why + '), death ' + this.deaths);
    if (this.deaths === 1) return this.spawnYtdec(this.elapsed);
    this.emit({ ev: 'error', msg: 'video failed, audio only' });
    return undefined;
};
// ytdec exit 4: it cannot decode this video's VP9 here (the plugin, or no frame out of it). H.264 from now on, and this
// video fetched again as H.264 from where it is, once (a quality restart at the same height); again: audio only
Daemon.prototype.setAvc1 = function () {
    this.avc1Only = true;
    try {
        if (!fs.existsSync(this.opts.stateDir)) { mkdirp(path.dirname(this.opts.stateDir)); fs.mkdirSync(this.opts.stateDir, 0o700); }
        writeIfChanged(path.join(this.opts.stateDir, 'codec.json'), JSON.stringify({ avc1: true }) + '\n', 0o644);
    } catch (e) { this.log('codec: ' + e.message); }
};
Daemon.prototype.ytdecCodec = function (child) {
    if (child !== this.child) return undefined;
    this.child = null;
    const c = this.cur;
    if (!this.ticking || !c) return undefined;
    this.setAvc1();
    this.log('ytdec: cannot decode ' + c.id + ' here (VP9), H.264 from now on');
    // a download offline is not fetched again: its files are all there is, so audio only, as a retry. Online it is, as
    // H.264 (its marker stays: an H.264 download once that fetch completes)
    if (this.codecRetry === c.id || this.isSaved(c.id) && isReady(c.dir) && !fs.existsSync(this.opts.leaseFile)) { this.emit({ ev: 'error', msg: 'video failed, audio only' }); return undefined; }
    this.codecRetry = c.id;
    return this.refetch(c.height, c.height).catch((e) => this.log('h264 again: ' + e));
};
Daemon.prototype.watchdog = function () {
    if (!this.child || this.state !== 'playing' || !this.videoOn || this.eof) return;
    // waiting for video not fetched yet (buffering, the fetcher ended early, or the clock before where the stream
    // starts: ytdec holds its first frame there): not a stall
    const c = this.cur, before = !!(c && c.stream && this.elapsed + 1000 < (c.lowMs || 0));
    if (this.headWait || this.elapsed + 1000 > this.fetchedMs || before) { this.lastProgress = Date.now(); this.played = 0; return; }
    // ytdec holds while the audio clock holds (a slow player, an announcement): a stall is watchdogMs of AUDIO played
    // with no frame decoded, not of wall time (20/11: 3 plays went audio only on audio that ran ~3x slower than the clock)
    if (Date.now() - this.lastProgress < this.opts.watchdogMs || (this.played || 0) < this.opts.watchdogMs) return;
    this.log('watchdog: no decoded frames for ' + this.opts.watchdogMs + ' ms of audio');
    try { this.child.kill('SIGKILL'); } catch (e) { /* gone */ }
    this.lastProgress = Date.now(); this.played = 0;
};

Daemon.prototype.close = async function () {
    this.closed = true;   // no download starts from here (a stop, a late client-gone)
    if (this.micHeld) { clearTimeout(this.micHeld.timer); this.micHeld = null; }
    if (this.audioKick) this.audioKick();
    this.timers.forEach(clearInterval);
    [this.spare, this.extracting, this.ytdlpRun].forEach((x) => { if (x) { try { x.child.kill('SIGKILL'); } catch (e) { /* gone */ } } });
    this.spare = this.extracting = this.ytdlpRun = null;
    this.account.close();
    const rc = this.rc.close();   // the phone remote off and its rule removed, alongside the audio release (4 s exit)
    await Promise.all([this.stop(null), this.stopDownload()]);   // (a cut download starts again from scratch at the next start)
    await rc;
    if (this.client) this.client.destroy();
    if (this.server) this.server.close();
};

module.exports = { describeIface: describeIface, carMedia: carMedia, audioInventory: audioInventory, someipPolicies: someipPolicies, BUILT: BUILT, ramMount: ramMount, nextPageToken: nextPageToken, clockBehind: clockBehind, qualityHeight: qualityHeight, channelTiles: channelTiles, playlistTiles: playlistTiles, playlistTitle: playlistTitle, addToPlaylistItems: addToPlaylistItems, Daemon: Daemon, Audio: Audio, Account: Account, feedItems: feedItems, isPlayable: isPlayable, redactLog: redactLog, VERSION: VERSION, CAPS: CAPS, REPORT_URL: REPORT_URL, feedShelves: feedShelves, ytRequest: ytRequest, uplinkLookup: uplinkLookup, pickFetchUid: pickFetchUid, pickUid: pickUid, fetchRequest: fetchRequest, listCache: listCache, pruneCache: pruneCache, isReady: isReady };

// the head unit's date (YYYY-MM-DD) when its clock is provably wrong (before BUILT), else null. A clock ahead of real
// time cannot be proven wrong: it gets nothing
function clockBehind(now) { return now < Date.parse(BUILT) ? new Date(now).toISOString().slice(0, 10) : null; }

// the uplink's DNS servers (its lease): the system resolver (127.0.0.1) answers no internet names. No
// lease (uplink down or not installed): the system resolver, as before
function uplinkLookup(leaseFile) {
    const servers = ((readJson(leaseFile || '/run/mgu-uplink/lease.json') || {}).dns || []).filter((x) => typeof x === 'string');
    if (!servers.length) return undefined;
    const r = new (require('dns').Resolver)();
    r.setServers(servers);
    return function (host, opts, cb) {
        if (typeof opts === 'function') cb = opts;
        r.resolve4(host, (e, a) => (e ? cb(e) : cb(null, a[0], 4)));
    };
}

if (require.main === module && process.argv[2] === '--fetch') {
    // the fetch helper (as the fetch uid): one request from stdin, its answer on stdout
    let input = '';
    const out = (o) => process.stdout.write(JSON.stringify(o), () => process.exit(0));
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (d) => { input += d; });
    process.stdin.on('end', () => {
        let q;
        try { q = JSON.parse(input); } catch (e) { return out({ error: 'bad request' }); }
        return ytRequest(q.method, q.url, q.headers, q.body, q.timeoutMs, uplinkLookup(), proxy.fromEnv()).then(out, (e) => out({ error: e.message }));
    });
} else if (require.main === module) {
    const d = new Daemon();
    process.on('uncaughtException', (e) => d.fault(e));
    process.on('unhandledRejection', (e) => d.fault(e));
    let exiting = false;
    ['SIGTERM', 'SIGINT', 'SIGHUP'].forEach((s) => process.on(s, () => {
        if (exiting) return;
        exiting = true;
        d.log(s + ': releasing audio');
        setTimeout(() => process.exit(0), 4000); // TimeoutStopSec=5
        d.close().then(() => process.exit(0), () => process.exit(0));
    }));
    // Majordome's mic hold (micHold): not SIGUSR1, which opens Node's inspector (root) in a process without a listener.
    // ytd.pid is written once both listeners are in: Majordome signals only a daemon that has them (0.5.1 writes none)
    process.on('SIGWINCH', () => d.guard(() => d.micHold()));
    process.on('SIGUSR2', () => d.guard(() => d.micRelease('Majordome')));
    try { fs.writeFileSync(path.join(path.dirname(d.opts.sock), 'ytd.pid'), process.pid + '\n'); } catch (e) { d.log('ytd.pid: ' + e.message); }
    d.start().catch((e) => {
        if (!(e && e.refuse)) return d.fault(e);
        d.log(e.message);
        process.exit(1);
    });
}
