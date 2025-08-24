(function(){
  const select = document.getElementById('coach-search');
  if(!select) return;
  const form = select.closest('form');
  const selectedList = document.getElementById('coach-selected');
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

  function updateSelectedList(){
    const names = Array.from(select.selectedOptions).map(o=>o.value);
    if(selectedList){
      selectedList.textContent = names.length ? 'Selected: ' + names.join(', ') : '';
    }
  }

  select.addEventListener('change', ()=>{
    const selected = Array.from(select.selectedOptions);
    if(selected.length > max){
      selected.slice(max).forEach(opt=>opt.selected=false);
    }
    updateHiddenInputs();
    updateSelectedList();
  });

  const initial = select.getAttribute('data-selected');
  if(initial){
    try{
      const arr = JSON.parse(initial);
      Array.from(select.options).forEach(opt=>{
        if(arr.includes(opt.value)){
          opt.selected = true;
        }
      });
    }catch(e){}
  }

  updateHiddenInputs();
  updateSelectedList();
})();

