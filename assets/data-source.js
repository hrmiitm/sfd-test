'use strict';
(()=>{
for(const panel of document.querySelectorAll('.data-source')){
 let pinned=false;
 panel.addEventListener('mouseenter',()=>{panel.open=true;});
 panel.addEventListener('mouseleave',()=>{if(!pinned&&!panel.contains(document.activeElement))panel.open=false;});
 panel.querySelector('summary').addEventListener('click',event=>{event.preventDefault();pinned=!pinned;panel.open=pinned;});
 panel.addEventListener('focusout',()=>{setTimeout(()=>{if(!pinned&&!panel.contains(document.activeElement)&&!panel.matches(':hover'))panel.open=false;},0);});
 panel.addEventListener('keydown',event=>{if(event.key==='Escape'){pinned=false;panel.open=false;panel.querySelector('summary').focus();}});
 panel.querySelector('[data-copy-command]').addEventListener('click',async()=>{
  const command=panel.querySelector('textarea'),status=panel.querySelector('[role=status]');
  try{
   if(navigator.clipboard&&window.isSecureContext)await navigator.clipboard.writeText(command.value);
   else {command.focus();command.select();if(!document.execCommand('copy'))throw Error('Select the command to copy');}
   status.textContent='Copied';
  }catch{command.focus();command.select();status.textContent='Select and copy the command';}
 });
}
})();
