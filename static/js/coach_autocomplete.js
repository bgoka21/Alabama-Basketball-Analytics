(function(){
  const input = document.getElementById('coach-search');
  if(!input) return;
  const tagContainer = document.getElementById('coach-tags');
  const selectedList = document.getElementById('coach-selected');
  const form = input.closest('form');
  const coachListUrl = input.getAttribute('data-coach-list-url') || '/recruits/coach_list';
  let coachList = [];
  let coachListLower = [];
  let coachMap = {};
  let selected = new Map();

  const dlExisting = document.getElementById('coach-suggestions');
  const dl = dlExisting || document.createElement('datalist');
  if(!dlExisting){
    dl.id = 'coach-suggestions';
    document.body.appendChild(dl);
  }
  input.setAttribute('list', dl.id);

  function updateDatalist(filter) {
    dl.innerHTML='';
    const term = (filter || '').toLowerCase();
    coachList.forEach((c, idx) => {
      const cLower = coachListLower[idx];
      if (cLower.includes(term) && !selected.has(cLower)) {
        const opt=document.createElement('option');
        opt.value=c;
        dl.appendChild(opt);
      }
    });
  }

  function updateHiddenInputs(){
    form.querySelectorAll('input[type="hidden"][name="coaches"]').forEach(el=>el.remove());
    selected.forEach((orig)=>{
      const h=document.createElement('input');
      h.type='hidden';
      h.name='coaches';
      h.value=orig;
      form.appendChild(h);
    });
  }

  function addTag(coach){
    const orig = coach.trim();
    const norm = orig.toLowerCase();
    if(!norm || selected.has(norm) || selected.size>=5) return;
    const original = coachMap[norm] || orig;
    selected.set(norm, original);
    const tag=document.createElement('span');
    tag.className='inline-flex items-center bg-gray-200 text-sm px-2 py-1 rounded';
    tag.textContent=original;
    const close=document.createElement('button');
    close.type='button';
    close.textContent='×';
    close.className='ml-1';
    close.addEventListener('click',()=>{
      selected.delete(norm);
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
    selectedList.textContent = selected.size ? 'Selected: ' + Array.from(selected.values()).join(', ') : '';
  }

  async function loadCoaches(){
    try {
      const resp = await fetch(coachListUrl);
      if (!resp.ok) throw new Error('Network response was not ok');
      coachList = await resp.json();
      coachMap = {};
      coachListLower = coachList.map(c => {
        const lower = c.toLowerCase();
        coachMap[lower] = c;
        return lower;
      });
      updateDatalist(input.value);
    } catch(e) {
      coachList = [];
      coachListLower = [];
      coachMap = {};
      if(selectedList){
        selectedList.textContent = 'Unable to load coach list; enter names manually.';
      }
    }
  }

  const coachListData = input.getAttribute('data-coach-list');
  if (coachListData) {
    try {
      const parsed = JSON.parse(coachListData);
      coachList = parsed;
      coachMap = {};
      coachListLower = coachList.map(c => {
        const lower = c.toLowerCase();
        coachMap[lower] = c;
        return lower;
      });
      updateDatalist('');
    } catch(e) {}
  }
  if (!coachList.length) {
    loadCoaches();
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
    const valLower = val.toLowerCase();
    if(!coachListLower.length || coachListLower.includes(valLower)){
      addTag(val);
    }
    input.value='';
    updateDatalist('');
  });

  form.addEventListener('submit', () => {
    const val = input.value.trim();
    const valLower = val.toLowerCase();
    if (!coachListLower.length || coachListLower.includes(valLower)) {
      addTag(val);
    }
    input.value = '';
    updateDatalist('');
  });

  updateSelectedList();
})();
