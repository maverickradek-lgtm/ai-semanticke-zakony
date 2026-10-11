/* Rozhodnicek - zalozka pro vyhledavani v usnesenich vlady a Poslanecke snemovny.
   Samostatny modul: s hlavni databazi Paragralfu nesdili data, vola jen funkci rozhodnicek-search. */
(function () {
  var state = { inited: false, callFn: null, offset: 0, lastQuery: null, total: 0, years: [] };

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function el(id) { return document.getElementById(id); }

  function fmtDate(d) {
    if (!d) return '';
    var m = String(d).match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? parseInt(m[3], 10) + '. ' + parseInt(m[2], 10) + '. ' + m[1] : String(d);
  }

  function highlight(text, query) {
    var safe = esc(text);
    var words = String(query || '').split(/\s+/).filter(function (w) { return w.length >= 3; });
    words.forEach(function (w) {
      var stem = w.slice(0, Math.max(3, w.length - 2)).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      try { safe = safe.replace(new RegExp('(' + stem + '[\\p{L}\\p{N}]*)', 'giu'), '<mark>$1</mark>'); } catch (e) {}
    });
    return safe;
  }

  function title(r) {
    var o = r.organ === 'vlada' ? 'Usnesení vlády' : 'Usnesení Poslanecké sněmovny';
    return o + ' č. ' + r.cislo + '/' + r.rok;
  }

  var MATCH_LABEL = {
    fulltext: 'přesná shoda', fulltext_volne: 'volnější shoda', semantic: 'podle významu (AI)', cislo: 'číslo usnesení'
  };

  function buildUi(root) {
    root.innerHTML =
      '<div class="card">' +
      '<h2>Rozhodníček <span class="muted" style="font-size:13px;font-weight:400;">usnesení vlády a Poslanecké sněmovny</span></h2>' +
      '<p class="muted" style="margin-top:0;">Samostatné vyhledávání jen v usneseních vlády ČR a Poslanecké sněmovny. Nejdřív hledá přesně podle slov, a když toho najde málo, doplní výsledky podle významu. Nehledá v zákonech ani judikatuře.</p>' +
      '<div class="row" style="margin-bottom:10px;gap:8px;flex-wrap:wrap;">' +
      '<div class="input-wrap" style="flex:1;min-width:240px;"><input type="text" id="rz-q" placeholder="Např. kompenzace zemědělcům, 623/2026, rozpočet SFŽP…"></div>' +
      '<button id="rz-go" class="small">Hledat</button></div>' +
      '<div class="row" style="gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:6px;">' +
      '<label class="muted">Zdroj <select id="rz-organ"><option value="">Vláda i Sněmovna</option><option value="vlada">Jen vláda</option><option value="ps">Jen Sněmovna</option></select></label>' +
      '<label class="muted">Od roku <select id="rz-od"></select></label>' +
      '<label class="muted">Do roku <select id="rz-do"></select></label>' +
      '<label class="muted">Měsíc <select id="rz-mes"><option value="">celý rok / všechny</option>' +
      ['leden', 'únor', 'březen', 'duben', 'květen', 'červen', 'červenec', 'srpen', 'září', 'říjen', 'listopad', 'prosinec']
        .map(function (m, i) { return '<option value="' + (i + 1) + '">' + m + '</option>'; }).join('') +
      '</select></label>' +
      '<label class="muted">Hledání <select id="rz-mode"><option value="auto">Automaticky (přesně, pak významově)</option><option value="fulltext">Jen přesně podle slov</option><option value="semantic">Jen podle významu (AI)</option></select></label>' +
      '</div>' +
      '<div id="rz-stats" class="muted" style="font-size:12px;"></div>' +
      '</div>' +
      '<div id="rz-status" class="muted" style="margin:6px 4px;"></div>' +
      '<div id="rz-results"></div>' +
      '<div id="rz-more-wrap" class="hidden" style="text-align:center;margin:10px;"><button id="rz-more" class="secondary small">Další výsledky</button></div>';

    el('rz-go').onclick = function () { runSearch(true); };
    el('rz-more').onclick = function () { runSearch(false); };
    el('rz-q').addEventListener('keydown', function (e) { if (e.key === 'Enter') runSearch(true); });
    el('rz-mes').addEventListener('change', function () { if (el('rz-q').value.trim()) runSearch(true); });
  }

  function fillYears(years) {
    var opts = function (first) {
      return '<option value="">' + first + '</option>' + years.map(function (y) { return '<option value="' + y + '">' + y + '</option>'; }).join('');
    };
    el('rz-od').innerHTML = opts('nejstarší');
    el('rz-do').innerHTML = opts('nejnovější');
  }

  function loadStats() {
    state.callFn('rozhodnicek-search', { action: 'stats' }).then(function (j) {
      var ys = {};
      (j.by_organ_year || []).forEach(function (r) { ys[r.rok] = true; });
      state.years = Object.keys(ys).map(Number).sort(function (a, b) { return b - a; });
      fillYears(state.years);
      var t = j.totals || {};
      el('rz-stats').textContent = 'V Rozhodníčku je ' + (t.docs || 0) + ' usnesení' +
        (state.years.length ? ' z let ' + state.years[state.years.length - 1] + '–' + state.years[0] : '') +
        '. Hledání podle významu pokrývá zatím ' + (t.chunks ? Math.round(100 * (t.embedded || 0) / t.chunks) : 0) + ' % textů.';
    }).catch(function (e) { el('rz-stats').textContent = 'Nepodařilo se načíst přehled: ' + (e && e.message || e); });
  }

  function range() {
    var od = el('rz-od').value, dd = el('rz-do').value, mes = el('rz-mes').value;
    var from = od, to = dd;
    if (mes) {
      var y = od || dd || (state.years[0] ? String(state.years[0]) : '');
      if (od && dd && od !== dd) { from = od; to = dd; mes = ''; }
      else if (y) { var mm = ('0' + mes).slice(-2); from = y + '-' + mm; to = y + '-' + mm; }
    }
    return { od: from || null, 'do': to || null };
  }

  function runSearch(reset) {
    var q = el('rz-q').value.trim();
    if (!q) return;
    if (reset) { state.offset = 0; el('rz-results').innerHTML = ''; }
    var r = range();
    el('rz-status').textContent = 'Hledám…';
    var body = {
      action: 'search', query: q, mode: el('rz-mode').value, limit: 20, offset: state.offset,
      organ: el('rz-organ').value || null, od: r.od, 'do': r['do']
    };
    state.callFn('rozhodnicek-search', body).then(function (j) {
      state.lastQuery = q;
      render(j, q, reset);
    }).catch(function (e) {
      el('rz-status').textContent = 'Chyba hledání: ' + (e && e.message || e);
    });
  }

  function notice(j) {
    if (j.semantic_note === 'no_gemini_key') return 'Pro hledání podle významu si v Nastavení uložte svůj Gemini API klíč.';
    if (j.semantic_note === 'embedding_unavailable') return 'Hledání podle významu se teď nepodařilo (Gemini neodpovídá nebo je vyčerpaná kvóta).';
    if (j.semantic_note === 'no_embeddings_yet') return 'Významové hledání zatím nemá k dispozici zpracované texty.';
    return '';
  }

  function render(j, q, reset) {
    var res = j.results || [];
    var box = el('rz-results');
    if (!res.length && reset) {
      el('rz-status').textContent = 'Nic nenalezeno. Zkuste jiná slova, širší období nebo hledání podle významu.' + (notice(j) ? ' ' + notice(j) : '');
      el('rz-more-wrap').classList.add('hidden');
      return;
    }
    var info = 'Nalezeno ' + (j.total || res.length) + ' usnesení';
    if (j.semantic_used) info += ' (výsledky označené „podle významu“ doplnila AI)';
    var n = notice(j);
    el('rz-status').textContent = info + (n ? ' – ' + n : '');
    res.forEach(function (r) {
      var d = document.createElement('div');
      d.className = 'card';
      d.style.marginBottom = '10px';
      d.innerHTML =
        '<div class="row" style="justify-content:space-between;gap:8px;flex-wrap:wrap;">' +
        '<strong>' + esc(title(r)) + '</strong>' +
        '<span class="muted" style="font-size:12px;">' + esc(fmtDate(r.datum)) + ' · ' + esc(MATCH_LABEL[r.match] || '') + '</span></div>' +
        (r.nazev ? '<div style="margin:4px 0;">' + highlight(r.nazev, q) + '</div>' : '') +
        '<div class="muted rz-snippet" style="font-size:13px;white-space:pre-wrap;">' + highlight(String(r.snippet || '').slice(0, 400), q) + '…</div>' +
        '<div class="row" style="margin-top:8px;gap:8px;">' +
        '<button class="secondary small rz-text">Zobrazit celý text</button>' +
        (r.zdroj_url ? '<a class="link-btn" target="_blank" rel="noopener" href="' + esc(r.zdroj_url) + '">Zdroj na webu úřadu</a>' : '') +
        (r.soubor_url ? '<a class="link-btn" target="_blank" rel="noopener" href="' + esc(r.soubor_url) + '">Originální soubor</a>' : '') +
        '</div><div class="rz-full hidden" style="margin-top:8px;white-space:pre-wrap;font-size:13px;"></div>';
      d.querySelector('.rz-text').onclick = function () {
        var full = d.querySelector('.rz-full');
        if (!full.classList.contains('hidden')) { full.classList.add('hidden'); return; }
        full.classList.remove('hidden');
        if (full.getAttribute('data-loaded')) return;
        full.textContent = 'Načítám…';
        state.callFn('rozhodnicek-search', { action: 'doc', id: r.id }).then(function (x) {
          full.innerHTML = highlight((x.doc && x.doc.text_plny) || '', q);
          full.setAttribute('data-loaded', '1');
        }).catch(function () { full.textContent = 'Text se nepodařilo načíst.'; });
      };
      box.appendChild(d);
    });
    state.offset += res.length;
    var more = res.length >= 20 && (j.mode_used || []).indexOf('semantic') < 0;
    el('rz-more-wrap').classList.toggle('hidden', !more);
  }

  window.Rozhodnicek = {
    open: function (callFn) {
      state.callFn = callFn;
      var root = el('rozhodnicek-view');
      if (!root) return;
      if (!state.inited) { buildUi(root); state.inited = true; loadStats(); }
    }
  };
})();
