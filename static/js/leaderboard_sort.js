(function(){
  function cmp(a, b) { return a < b ? -1 : a > b ? 1 : 0; }

  function getVal(tr, key) {
    switch (key) {
      case 'pct':  return parseFloat(tr.dataset.pct || '0');
      case 'opps': return parseInt(tr.dataset.opps || '0', 10);
      case 'plus': return parseInt(tr.dataset.plus || '0', 10);
      case 'name': return (tr.dataset.name || '').toString();
      default:     return 0;
    }
  }

  function sortBody(tbody, key, dir) {
    const rows = Array.from(tbody.querySelectorAll('tr'))
      .filter(tr => !tr.dataset.total); // safety

    rows.sort((ra, rb) => {
      const a = getVal(ra, key);
      const b = getVal(rb, key);

      // primary key
      let s = cmp(a, b);
      if (dir === 'desc') s = -s;
      if (s !== 0) return s;

      // tie-breakers: opps desc, plus desc, name asc
      let s2 = cmp(getVal(ra, 'opps'), getVal(rb, 'opps'));
      if (dir === 'asc') s2 = -s2; // keep opps as same dir as primary
      if (s2 !== 0) return s2;

      let s3 = cmp(getVal(ra, 'plus'), getVal(rb, 'plus'));
      if (dir === 'asc') s3 = -s3;
      if (s3 !== 0) return s3;

      return cmp(getVal(ra, 'name'), getVal(rb, 'name')); // always A→Z
    });

    // re-append in sorted order
    rows.forEach(tr => tbody.appendChild(tr));
  }

  function initTable(container) {
    const table = container.querySelector('table.js-leaderboard');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    const select = container.querySelector('select.js-sort');

    function applyDefault() {
      // default: pct desc
      sortBody(tbody, 'pct', 'desc');
    }

    if (select) {
      select.addEventListener('change', () => {
        const [key, dir] = select.value.split(':');
        sortBody(tbody, key, dir);
      });
    }

    // Initial sort
    applyDefault();
  }

  document.addEventListener('DOMContentLoaded', function(){
    document.querySelectorAll('.rounded-2xl.border.shadow-sm').forEach(initTable);
  });
})();
