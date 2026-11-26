for (const gallery of document.querySelectorAll('.screenshot-gallery')) {
  const slides = [...gallery.querySelectorAll(':scope > figure')];
  if (slides.length < 2) continue;

  const controls = gallery.querySelector('.screenshot-gallery__controls');
  const captions = slides.map(slide => slide.querySelector('figcaption').textContent);
  gallery.setAttribute('aria-roledescription', 'carousel');
  slides.forEach((slide, i) => {
    slide.setAttribute('role', 'group');
    slide.setAttribute('aria-roledescription', 'slide');
    slide.setAttribute('aria-label', captions[i]);
    slide.querySelector('figcaption').hidden = true;
  });

  let current = 0;
  function show(index) {
    current = (index + slides.length) % slides.length;
    slides.forEach((slide, i) => { slide.hidden = i !== current; });
    controls.querySelector('[data-caption]').textContent = captions[current];
    controls.querySelector('small').textContent = `[${current + 1} of ${slides.length}]`;
  }
  for (const button of controls.querySelectorAll('button')) {
    button.addEventListener('click', () => show(current + Number(button.dataset.step)));
  }
  show(0);
  controls.hidden = false;
}
