(function(){
  const select = document.getElementById('coach-search');
  if(!select) return;
  const form = select.closest('form');
  const badgeContainer = document.getElementById('coach-badges');
  const filterInput = document.getElementById('coach-filter');
  const max = 5;

  function updateHiddenInputs(){
    form.querySelectorAll('input[type="hidden"][name="coaches"]').forEach(el=>el.remove());
    Array.from(select.selectedOptions).forEach(opt=>{
      const h=document.createElement('input');
      h.type='hidden';
      h.name='coaches';
      h.value=opt.value;
      form.appendChild(h);
    });
  }

  function updateDisabled(){
    const selected = Array.from(select.selectedOptions);
    const disable = selected.length >= max;
    Array.from(select.options).forEach(opt=>{
      if(!opt.selected){
        opt.disabled = disable;
      }
    });
  }

  function renderBadges(){
    if(!badgeContainer) return;
    badgeContainer.innerHTML='';
    Array.from(select.selectedOptions).forEach(opt=>{
      const span=document.createElement('span');
      span.className='flex items-center bg-blue-100 text-blue-700 px-2 py-1 rounded-full text-xs';
      span.textContent=opt.value;
      const btn=document.createElement('button');
      btn.type='button';
      btn.className='ml-1 text-blue-500 hover:text-blue-700';
      btn.setAttribute('data-value', opt.value);
      btn.innerHTML='&times;';
      span.appendChild(btn);
      badgeContainer.appendChild(span);
    });
  }

  select.addEventListener('change', ()=>{
    updateHiddenInputs();
    updateDisabled();
    renderBadges();
  });

  if(badgeContainer){
    badgeContainer.addEventListener('click', e=>{
      if(e.target.tagName==='BUTTON'){
        const val=e.target.getAttribute('data-value');
        Array.from(select.options).forEach(opt=>{
          if(opt.value===val){
            opt.selected=false;
          }
        });
        updateHiddenInputs();
        updateDisabled();
        renderBadges();
      }
    });
  }

  if(filterInput){
    filterInput.addEventListener('input', ()=>{
      const q=filterInput.value.toLowerCase();
      Array.from(select.options).forEach(opt=>{
        opt.hidden = !opt.value.toLowerCase().includes(q);
      });
    });
  }

  const initial = select.getAttribute('data-selected');
  if(initial){
    try{
      const arr=JSON.parse(initial);
      Array.from(select.options).forEach(opt=>{
        if(arr.includes(opt.value)){
          opt.selected=true;
        }
      });
    }catch(e){}
  }

  updateHiddenInputs();
  updateDisabled();
  renderBadges();
})();

