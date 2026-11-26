/* ParagrAlf - decentni hudba na pozadi + maly ovladaci panel.
   Skladby pridavejte do seznamu TRACKS (soubory ve slozce /music/).
   Prohlizece blokuji zvuk do prvniho kliknuti na strance, proto se hudba
   spusti pri prvni interakci uzivatele (kliknuti/klavesa). */
(function () {
  var TRACKS = [
    { src: 'music/glass_sky_rising.mp3', title: 'Glass Sky Rising' },
    { src: 'music/dneska_nikam_neutikam.mp3', title: 'Dneska nikam neutikam' },
    { src: 'music/rano_klepe_na_sklo.mp3', title: 'Ráno klepe na sklo' },
    { src: 'music/samota_v_celofanu.mp3', title: 'Samota v celofánu' }
  ];
  var DEFAULT_VOLUME = 0.10; // decentni vychozi hlasitost (0-1)
  var LS = 'paragralf_music_v2'; // zmena klice = vsem se znovu pouzije vychozi hlasitost

  function load() { try { return JSON.parse(localStorage.getItem(LS)) || {}; } catch (e) { return {}; } }
  function save() {
    try { localStorage.setItem(LS, JSON.stringify({ volume: audio.volume, off: userOff, min: minimized, idx: idx })); } catch (e) {}
  }

  var st = load();
  var idx = Math.min(Math.max(parseInt(st.idx, 10) || 0, 0), TRACKS.length - 1);
  var userOff = !!st.off;      // uzivatel hudbu zastavil/pozastavil -> sama se nespousti
  var minimized = !!st.min;
  var started = false;

  var audio = new Audio();
  audio.preload = 'none';
  audio.volume = DEFAULT_VOLUME; // hlasitost se NEpamatuje - pri kazde navsteve zacina na vychozi hodnote
  audio.addEventListener('ended', function () { go(1, true); });
  audio.addEventListener('play', refresh);
  audio.addEventListener('pause', refresh);

  function setTrack() { audio.src = TRACKS[idx].src; }
  setTrack();

  function play() {
    userOff = false;
    var p = audio.play();
    if (p && p.catch) p.catch(function () {});
    save();
  }
  function pause() { userOff = true; audio.pause(); save(); }
  function stop() { userOff = true; audio.pause(); try { audio.currentTime = 0; } catch (e) {} save(); refresh(); }
  function go(d, auto) {
    idx = (idx + d + TRACKS.length) % TRACKS.length;
    setTrack();
    save();
    if (auto || !audio.paused || !userOff) play(); else refresh();
  }

  // ---- panel ----
  var css = document.createElement('style');
  css.textContent =
    '#pm-wrap{position:fixed;left:14px;bottom:14px;z-index:9999;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}' +
    '#pm-box{display:flex;align-items:center;gap:6px;background:var(--panel,#161f2c);border:1px solid var(--border,#2a3644);border-radius:999px;padding:5px 10px;box-shadow:0 4px 18px rgba(0,0,0,.45);color:var(--text,#e8edf3);}' +
    '#pm-box button{background:transparent;color:var(--text,#e8edf3);border:none;padding:0;min-height:0;width:28px;height:28px;border-radius:50%;font-size:14px;line-height:1;cursor:pointer;box-shadow:none;transform:none;}' +
    '#pm-box button:hover{background:var(--panel-2,#1d2836);transform:none;box-shadow:none;}' +
    '#pm-box button.pm-main{background:var(--accent,#4f8cff);color:#fff;}' +
    '#pm-title{font-size:12px;color:var(--muted,#93a2b3);max-width:120px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}' +
    '#pm-vol{width:70px;accent-color:var(--accent,#4f8cff);margin:0;}' +
    '#pm-wrap.pm-min #pm-box > :not(.pm-keep){display:none;}' +
    '@media(max-width:520px){#pm-title{display:none;}#pm-vol{width:54px;}}';
  document.head.appendChild(css);

  var wrap = document.createElement('div');
  wrap.id = 'pm-wrap';
  wrap.setAttribute('aria-label', 'Hudba na pozadí');
  var box = document.createElement('div');
  box.id = 'pm-box';
  wrap.appendChild(box);

  function btn(txt, title, fn, cls) {
    var b = document.createElement('button');
    b.type = 'button';
    b.textContent = txt;
    b.title = title;
    b.setAttribute('aria-label', title);
    if (cls) b.className = cls;
    b.addEventListener('click', function (e) { e.stopPropagation(); fn(); });
    box.appendChild(b);
    return b;
  }

  var bPrev = btn('⏮', 'Předchozí skladba', function () { go(-1); });
  var bPlay = btn('▶', 'Přehrát / pozastavit', function () { if (audio.paused) play(); else pause(); }, 'pm-main');
  var bStop = btn('⏹', 'Zastavit', stop);
  var bNext = btn('⏭', 'Další skladba', function () { go(1); });
  var title = document.createElement('span');
  title.id = 'pm-title';
  box.appendChild(title);
  var vol = document.createElement('input');
  vol.type = 'range'; vol.id = 'pm-vol'; vol.min = 0; vol.max = 1; vol.step = 0.01;
  vol.value = audio.volume; vol.title = 'Hlasitost'; vol.setAttribute('aria-label', 'Hlasitost');
  vol.addEventListener('input', function () { audio.volume = parseFloat(vol.value); save(); });
  box.appendChild(vol);
  var bMin = btn('♫', 'Skrýt / zobrazit panel', function () { minimized = !minimized; save(); refresh(); });
  bMin.className = 'pm-keep';
  box.appendChild(bMin); // presun na konec

  function refresh() {
    bPlay.textContent = audio.paused ? '▶' : '⏸';
    title.textContent = TRACKS[idx].title;
    wrap.classList.toggle('pm-min', minimized);
    if (TRACKS.length < 2) { bPrev.style.display = 'none'; bNext.style.display = 'none'; }
  }

  function mount() { document.body.appendChild(wrap); refresh(); }
  if (document.body) mount(); else document.addEventListener('DOMContentLoaded', mount);

  // prvni interakce uzivatele -> spustit (pokud ji uzivatel drive nevypnul)
  function firstGesture(e) {
    if (started) return;
    if (e && e.target && e.target.closest && e.target.closest('#pm-wrap')) return;
    started = true;
    document.removeEventListener('pointerdown', firstGesture, true);
    document.removeEventListener('keydown', firstGesture, true);
    if (!userOff) play();
  }
  document.addEventListener('pointerdown', firstGesture, true);
  document.addEventListener('keydown', firstGesture, true);
})();
