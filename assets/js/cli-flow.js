for (const flow of document.querySelectorAll('.cli-flow')) {
  const nav = flow.querySelector('.cli-flow__nav');
  const list = nav.querySelector('[role="tablist"]');
  const template = flow.querySelector('template');
  const panels = [...flow.querySelectorAll('.cli-flow__step')];
  const outputs = flow.querySelectorAll('details.agent-session__raw');
  for (const output of outputs) {
    output.addEventListener('toggle', () => {
      for (const other of outputs) {
        if (other.open !== output.open) other.open = output.open;
      }
    });
  }

  const tabs = panels.map((panel, i) => {
    const tab = template.content.firstElementChild.cloneNode(true);
    tab.querySelector('.cli-flow__number').textContent = i + 1;
    tab.querySelector('.cli-flow__label').textContent = panel.querySelector('h4').textContent;
    tab.id = `${flow.id}-tab-${i + 1}`;
    panel.id = `${flow.id}-step-${i + 1}`;
    tab.setAttribute('aria-controls', panel.id);
    panel.setAttribute('role', 'tabpanel');
    panel.setAttribute('aria-labelledby', tab.id);
    panel.tabIndex = 0;
    list.append(tab);
    return tab;
  });

  function select(index) {
    tabs.forEach((tab, i) => {
      const active = i === index;
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
      panels[i].hidden = !active;
    });
  }

  tabs.forEach((tab, i) => {
    tab.addEventListener('click', () => select(i));
    tab.addEventListener('keydown', (event) => {
      const next = {
        ArrowRight: (i + 1) % tabs.length,
        ArrowLeft: (i + tabs.length - 1) % tabs.length,
        Home: 0,
        End: tabs.length - 1,
      }[event.key];
      if (next === undefined) return;
      event.preventDefault();
      select(next);
      tabs[next].focus();
    });
  });
  select(0);
  nav.hidden = false;
}
