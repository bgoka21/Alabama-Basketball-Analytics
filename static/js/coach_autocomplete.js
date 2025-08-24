(function(){
  const input = document.getElementById('coach-search');
  if(!input) return;
  const tagContainer = document.getElementById('coach-tags');
  const selectedList = document.getElementById('coach-selected');
  const form = input.closest('form');
  let coachList = [];
  let selected = new Set();

  const dl = document.createElement('datalist');
  dl.id = 'coach-suggestions';
  document.body.appendChild(dl);
  input.setAttribute('list', dl.id);

  function updateDatalist(filter) {
    dl.innerHTML='';
    const term = (filter || '').toLowerCase();
    coachList.filter(c => c.toLowerCase().includes(term) && !selected.has(c))
             .forEach(c => {
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
      updateSelectedList();
    });
    tag.appendChild(close);
    tagContainer.appendChild(tag);
    updateHiddenInputs();
    updateDatalist(input.value);
    updateSelectedList();
  }

  function updateSelectedList(){
    if(!selectedList) return;
    selectedList.textContent = selected.size ? 'Selected: ' + Array.from(selected).join(', ') : '';
  }

  async function loadCoaches(){
    try {
      const resp = await fetch('/recruits/coach_list');
      if (!resp.ok) throw new Error('Network response was not ok');
      coachList = await resp.json();
      updateDatalist(input.value);
    } catch(e) {
      coachList = [];
      if(selectedList){
        selectedList.textContent = 'Unable to load coach list; enter names manually.';
      }
    }
  }

  const selectedInitial = input.getAttribute('data-selected');
  if (selectedInitial) {
    try {
      JSON.parse(selectedInitial).forEach(c=>addTag(c));
    } catch(e) {}
  }

  input.addEventListener('input', ()=>{
    updateDatalist(input.value);
  });

  input.addEventListener('change', ()=>{
    const val=input.value.trim();
    if(!coachList.length || coachList.includes(val)){
      addTag(val);
    }
    input.value='';
    updateDatalist('');
  });

  form.addEventListener('submit', () => {
    const val = input.value.trim();
    if (!coachList.length || coachList.includes(val)) {
      addTag(val);
    }
    input.value = '';
    updateDatalist('');
  });

  loadCoaches();
  updateSelectedList();
})();
