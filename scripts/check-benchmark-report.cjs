// Local Chrome verification without third-party packages. Use a resource-limited scope.
const {spawn}=require('node:child_process');
const fs=require('node:fs');const os=require('node:os');const path=require('node:path');
const {pathToFileURL}=require('node:url');
(async()=>{
 const report=path.resolve(process.argv[2]);const output=path.dirname(report);
 const profile=fs.mkdtempSync(path.join(os.tmpdir(),'sfd-report-check-'));
 const chrome=spawn('google-chrome',['--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--no-first-run',
   '--remote-debugging-port=0','--user-data-dir='+profile,'--window-size=1440,1200',pathToFileURL(report).href],{stdio:'ignore'});
 const delay=ms=>new Promise(r=>setTimeout(r,ms));let socket;
 try{
  for(let i=0;i<100&&!fs.existsSync(path.join(profile,'DevToolsActivePort'));i++)await delay(100);
  const port=fs.readFileSync(path.join(profile,'DevToolsActivePort'),'utf8').split('\n')[0];
  let pages=[];for(let i=0;i<50;i++){pages=await(await fetch(`http://127.0.0.1:${port}/json/list`)).json();if(pages.some(p=>p.type==='page'))break;await delay(100)}
  socket=new WebSocket(pages.find(p=>p.type==='page').webSocketDebuggerUrl);
  await new Promise((resolve,reject)=>{socket.onopen=resolve;socket.onerror=reject});
  let id=0;const pending=new Map(),errors=[];
  socket.onmessage=e=>{const v=JSON.parse(e.data);if(v.method==='Runtime.exceptionThrown')errors.push(v.params.exceptionDetails);if(v.id){const p=pending.get(v.id);pending.delete(v.id);v.error?p.reject(v.error):p.resolve(v.result)}};
  const send=(method,params={})=>new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});socket.send(JSON.stringify({id:n,method,params}))});
  const evaluate=async expression=>{const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value};
  await send('Runtime.enable');await send('Page.enable');
  for(let i=0;i<100;i++){if(await evaluate('document.readyState==="complete" && document.querySelectorAll("#table tr").length===16'))break;await delay(100)}
  await delay(300);
  const desktop=await evaluate(`({rows:document.querySelectorAll('#table tr').length,matrixRows:document.querySelectorAll('#matrix tbody tr').length,points:document.querySelectorAll('#chart .point').length,questions:document.querySelectorAll('#question option').length,series:document.querySelectorAll('#series option').length,overflow:document.documentElement.scrollWidth>innerWidth,axis:document.querySelectorAll('#chart .axis-title')[0]?.textContent})`);
  if(desktop.rows!==16||desktop.matrixRows!==2||desktop.points!==16||desktop.questions!==20||desktop.series!==16||desktop.overflow)throw new Error('Desktop DOM validation failed: '+JSON.stringify(desktop));
  await evaluate(`document.querySelector('#chart .point').dispatchEvent(new MouseEvent('click',{bubbles:true}));document.getElementById('question').value='Q089';document.getElementById('question').dispatchEvent(new Event('change'));`);
  const interaction=await evaluate(`({selected:document.getElementById('selected').textContent,question:document.getElementById('questionText').textContent,answer:document.getElementById('answer').textContent.length})`);
  if(!interaction.selected.includes('correctness')||!interaction.question||interaction.answer<1)throw new Error('Inspector interaction failed');
  await evaluate(`document.getElementById('costBasis').value='answer';document.getElementById('costBasis').dispatchEvent(new Event('change'));`);
  const answerAxis=await evaluate(`document.querySelectorAll('#chart .axis-title')[0].textContent`);
  if(!answerAxis.includes('answer + planning cost'))throw new Error('Cost toggle failed');
  await evaluate(`document.getElementById('costBasis').value='all';document.getElementById('costBasis').dispatchEvent(new Event('change'));`);
  let shot=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});fs.writeFileSync(path.join(output,'report-desktop.png'),Buffer.from(shot.data,'base64'));
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:1100,deviceScaleFactor:1,mobile:false});await delay(350);
  const mobile=await evaluate(`({width:innerWidth,bodyWidth:document.documentElement.scrollWidth,points:document.querySelectorAll('#chart .point').length})`);
  if(mobile.bodyWidth>mobile.width)throw new Error('Mobile body overflow: '+JSON.stringify(mobile));
  shot=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});fs.writeFileSync(path.join(output,'report-mobile.png'),Buffer.from(shot.data,'base64'));
  if(errors.length)throw new Error('Browser JavaScript exceptions: '+JSON.stringify(errors));
  const checks={desktop,mobile,interaction,cost_toggle_passed:true,javascript_errors:errors,checked_utc:new Date().toISOString()};
  fs.writeFileSync(path.join(output,'html-validation.json'),JSON.stringify(checks,null,2));console.log(JSON.stringify(checks));
 }finally{socket?.close();chrome.kill();await new Promise(r=>chrome.once('exit',r));fs.rmSync(profile,{recursive:true,force:true,maxRetries:5,retryDelay:100})}
})().catch(e=>{console.error(e);process.exitCode=1});
