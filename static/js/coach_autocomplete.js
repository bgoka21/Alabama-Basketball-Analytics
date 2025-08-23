(function(){
  const input = document.getElementById('coach-search');
  if(!input) return;
  const tagContainer = document.getElementById('coach-tags');
  const form = input.closest('form');
  let coachList = [];
  try {
    const data = input.getAttribute('data-coaches');
    if (data) coachList = JSON.parse(data);
  } catch(e) {
    coachList = [];
  }
  if (!coachList.length) {
    fetch('/recruits/coach_list').then(r=>r.json()).then(list=>{
      coachList = list;
      updateDatalist('');
    });
  }
  const selectedInitial = input.getAttribute('data-selected');
  let selected = new Set();
  if (selectedInitial) {
    try {
      JSON.parse(selectedInitial).forEach(c=>addTag(c));
    } catch(e) {}
  }
  const dl = document.createElement('datalist');
  dl.id = 'coach-suggestions';
  document.body.appendChild(dl);
  input.setAttribute('list', dl.id);

  function updateDatalist(filter) {
    dl.innerHTML='';
    const term = (filter || '').toLowerCase();
    coachList.filter(c => c.toLowerCase().includes(term) && !selected.has(c))
             .slice(0,10).forEach(c => {
                const opt=document.createElement('option');
                opt.value=c;
                dl.appendChild(opt);
             });
  }

  function updateHiddenInputs(){
    form.querySelectorAll('input[type="hidden"][name="coaches"]').forEach(el=>el.remove());
    selected.forEach(coach=>{
      const h=document.createElement('input');
      h.type='hidden';
      h.name='coaches';
      h.value=coach;
      form.appendChild(h);
    });
  }

  function addTag(coach){
    if(selected.has(coach) || selected.size>=5) return;
    selected.add(coach);
    const tag=document.createElement('span');
    tag.className='inline-flex items-center bg-gray-200 text-sm px-2 py-1 rounded';
    tag.textContent=coach;
    const close=document.createElement('button');
    close.type='button';
    close.textContent='×';
    close.className='ml-1';
    close.addEventListener('click',()=>{
      selected.delete(coach);
      tag.remove();
      updateHiddenInputs();
      updateDatalist(input.value);
    });
    tag.appendChild(close);
    tagContainer.appendChild(tag);
    updateHiddenInputs();
    updateDatalist(input.value);
  }

  input.addEventListener('input', ()=>{
    updateDatalist(input.value);
  });

  input.addEventListener('change', ()=>{
    const val=input.value.trim();
    if(coachList.includes(val)){
      addTag(val);
    }
    input.value='';
    updateDatalist('');
  });

  form.addEventListener('submit', () => {
    const val = input.value.trim();
    if (coachList.includes(val)) {
      addTag(val);
    }
    input.value = '';
    updateDatalist('');
  });

  // initialize datalist
  updateDatalist('');
})();
