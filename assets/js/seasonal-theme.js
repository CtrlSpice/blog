(() => {
  const root = document.documentElement;
  const system = window.matchMedia('(prefers-color-scheme: dark)');
  const validTheme = (value) => /^madoromi-(haru|natsu|aki|fuyu)-(hiru|yoru)$/.test(value);
  let saved = null;
  try {
    saved = localStorage.getItem('theme');
  } catch {
    // Theme selection still works when browser storage is blocked.
  }
  let chosen = validTheme(saved) ? saved : null;

  const apply = (theme) => {
    const [, season, mode] = theme.split('-');
    root.dataset.season = season;
    root.dataset.theme = mode === 'yoru' ? 'dark' : 'light';
  };
  const systemTheme = () => system.matches ? 'madoromi-aki-yoru' : 'madoromi-haru-hiru';
  apply(chosen || systemTheme());

  document.addEventListener('DOMContentLoaded', () => {
    const controls = document.querySelector('.seasonal-controls');
    const trigger = document.getElementById('season-toggle');
    const picker = document.getElementById('season-picker');
    const toggle = document.getElementById('day-night-toggle');
    const options = [...picker.querySelectorAll('[role="radio"]')];

    const syncControls = () => {
      const selected = options.find((option) => option.dataset.season === root.dataset.season);
      const name = selected.querySelector('.season-label').textContent;
      document.getElementById('season-name').textContent = name;
      trigger.setAttribute('aria-label', `Season: ${name}`);
      toggle.setAttribute('aria-pressed', String(root.dataset.theme === 'dark'));
      toggle.title = `Switch to ${root.dataset.theme === 'dark' ? 'day' : 'night'} (Alt + T)`;
      options.forEach((option) => {
        const checked = option === selected;
        option.setAttribute('aria-checked', String(checked));
        option.tabIndex = checked ? 0 : -1;
      });
    };
    const choose = (season, scheme) => {
      chosen = `madoromi-${season}-${scheme === 'dark' ? 'yoru' : 'hiru'}`;
      apply(chosen);
      syncControls();
      try {
        localStorage.setItem('theme', chosen);
      } catch {
        // Retain the choice for this page when storage is unavailable.
      }
    };
    const close = (restoreFocus = false) => {
      picker.hidden = true;
      trigger.setAttribute('aria-expanded', 'false');
      if (restoreFocus) trigger.focus();
    };
    const positionPicker = () => {
      const anchor = trigger.getBoundingClientRect();
      picker.style.left = `${Math.max(8, Math.min(anchor.right - picker.offsetWidth, document.documentElement.clientWidth - picker.offsetWidth - 8))}px`;
      picker.style.top = `${anchor.bottom + 6}px`;
    };

    trigger.addEventListener('click', () => {
      if (!picker.hidden) return close(true);
      picker.hidden = false;
      trigger.setAttribute('aria-expanded', 'true');
      positionPicker();
      picker.querySelector('[aria-checked="true"]').focus({ preventScroll: true });
    });
    toggle.addEventListener('click', () => {
      choose(root.dataset.season, root.dataset.theme === 'dark' ? 'light' : 'dark');
    });
    options.forEach((option, index) => {
      option.addEventListener('click', () => choose(option.dataset.season, root.dataset.theme));
      option.addEventListener('keydown', (event) => {
        let next;
        if (event.key === 'ArrowRight' || event.key === 'ArrowDown') next = (index + 1) % options.length;
        else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') next = (index + options.length - 1) % options.length;
        else if (event.key === 'Home') next = 0;
        else if (event.key === 'End') next = options.length - 1;
        else return;
        event.preventDefault();
        choose(options[next].dataset.season, root.dataset.theme);
        options[next].focus();
      });
    });
    controls.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && !picker.hidden) {
        event.preventDefault();
        close(true);
      }
    });
    controls.addEventListener('focusout', (event) => {
      if (!controls.contains(event.relatedTarget)) close();
    });
    document.addEventListener('pointerdown', (event) => {
      if (!controls.contains(event.target)) close();
    });
    window.addEventListener('resize', () => { if (!picker.hidden) positionPicker(); });
    window.addEventListener('scroll', () => { if (!picker.hidden) positionPicker(); }, { passive: true });
    system.addEventListener('change', () => {
      if (chosen) return;
      apply(systemTheme());
      syncControls();
    });
    syncControls();
    controls.hidden = false;
  });
})();
