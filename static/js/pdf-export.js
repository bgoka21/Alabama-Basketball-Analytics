// pdf-export.js
// exports: window.exportStatsPDF({ filename, landscape })

(function () {
  function detectOrientation(rootEl) {
    // Heuristic: if content is much wider than tall, prefer landscape
    const w = rootEl.scrollWidth;
    const h = rootEl.scrollHeight;
    return (w / h) > 1.2 ? 'landscape' : 'portrait';
  }

  window.exportStatsPDF = async function ({ filename = 'stats.pdf', landscape = null } = {}) {
    const root = document.querySelector('#print-area') || document.body;
    if (!root) return;

    // Temporarily widen visibility for capture (does nothing to live layout)
    document.body.classList.add('print-capture');
    await new Promise(r => requestAnimationFrame(r));

    const orientation = landscape || detectOrientation(root);

    const opt = {
      margin:       0.5,                   // inches
      filename,
      image:        { type: 'jpeg', quality: 0.98 },
      html2canvas:  { scale: 2, useCORS: true, backgroundColor: '#ffffff' },
      jsPDF:        { unit: 'in', format: 'letter', orientation }
    };

    try {
      await html2pdf().set(opt).from(root).save();
    } finally {
      document.body.classList.remove('print-capture');
    }
  };
})();
