(function(){
  const editBtn = document.getElementById('editDevPlanBtn');
  const viewEl = document.getElementById('devPlanView');
  const formEl = document.getElementById('devPlanForm');
  const cancelBtn = document.getElementById('cancelDevPlan');
  if(!editBtn || !viewEl || !formEl) return;
  editBtn.addEventListener('click', () => {
    viewEl.classList.add('hidden');
    formEl.classList.remove('hidden');
  });
  if(cancelBtn){
    cancelBtn.addEventListener('click', () => {
      formEl.classList.add('hidden');
      viewEl.classList.remove('hidden');
    });
  }
})();
