(function () {
  function setActiveTab(name) {
    const triggers = document.querySelectorAll('[data-adv-tab-trigger]');
    const panels = document.querySelectorAll('[data-adv-tab-panel]');

    triggers.forEach((btn) => {
      const target = btn.getAttribute('data-adv-tab-trigger');
      const isActive = target === name;
      btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
      btn.classList.toggle('border-[#9E1B32]', isActive);
      btn.classList.toggle('text-[#9E1B32]', isActive);
      btn.classList.toggle('border-transparent', !isActive);
      btn.classList.toggle('text-gray-600', !isActive);
      btn.classList.toggle('hover:text-[#9E1B32]', !isActive);
    });

    panels.forEach((panel) => {
      const target = panel.getAttribute('data-adv-tab-panel');
      if (target === name) {
        panel.classList.remove('hidden');
      } else {
        panel.classList.add('hidden');
      }
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    const triggers = document.querySelectorAll('[data-adv-tab-trigger]');
    if (!triggers.length) {
      return;
    }
    const current = document.querySelector('[data-adv-tab-panel]:not(.hidden)');
    const activeName = current ? current.getAttribute('data-adv-tab-panel') : triggers[0].getAttribute('data-adv-tab-trigger');
    setActiveTab(activeName);

    triggers.forEach((btn) => {
      btn.addEventListener('click', () => {
        const target = btn.getAttribute('data-adv-tab-trigger');
        setActiveTab(target);
      });
    });
  });
})();
