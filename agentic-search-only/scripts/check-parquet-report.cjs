// Verify offline case filters, expandable evidence/context and responsive layout.
const {spawn}=require('node:child_process');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {pathToFileURL}=require('node:url');
(async()=>{
 const report=path.resolve(process.argv[2]),output=path.dirname(report);
 const profile=fs.mkdtempSync(path.join(os.tmpdir(),'sfd-parquet-report-'));
 const chrome=spawn('google-chrome',['--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--no-first-run',
  '--remote-debugging-port=0','--user-data-dir='+profile,'--window-size=1440,1100',pathToFileURL(report).href],{stdio:'ignore'});
 const delay=ms=>new Promise(r=>setTimeout(r,ms));let socket;
 try{
  for(let i=0;i<100&&!fs.existsSync(path.join(profile,'DevToolsActivePort'));i++)await delay(100);
  const port=fs.readFileSync(path.join(profile,'DevToolsActivePort'),'utf8').split('\n')[0];
  const pages=await(await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  socket=new WebSocket(pages.find(p=>p.type==='page').webSocketDebuggerUrl);
  await new Promise((ok,no)=>{socket.onopen=ok;socket.onerror=no});
  let id=0;const pending=new Map(),errors=[];
  socket.onmessage=e=>{const v=JSON.parse(e.data);if(v.method==='Runtime.exceptionThrown')errors.push(v.params.exceptionDetails);
   if(v.id){const p=pending.get(v.id);pending.delete(v.id);v.error?p.reject(v.error):p.resolve(v.result)}};
  const send=(method,params={})=>new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});socket.send(JSON.stringify({id:n,method,params}))});
  const evaluate=async expression=>{const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value};
  await send('Runtime.enable');await send('Page.enable');
  for(let i=0;i<100;i++){if(await evaluate('document.readyState==="complete" && !!document.getElementById("filter-model")'))break;await delay(100)}
  const total=await evaluate('document.querySelectorAll("details.case").length');
  if(!total)throw Error('No case inspectors');
  const ids=await evaluate('Array.from(document.querySelectorAll("#filter-question_id option")).map(o=>o.value).filter(Boolean)');
  const question=ids[0];
  await evaluate(`document.getElementById('filter-question_id').value=${JSON.stringify(question)};document.getElementById('filter-question_id').dispatchEvent(new Event('change'));`);
  const byQuestion=await evaluate('document.querySelectorAll("details.case:not([hidden])").length');
  if(byQuestion!==4)throw Error('Question filter must show four cases, got '+byQuestion);
  await evaluate(`document.getElementById('filter-model').value='openai/gpt-6-luna';document.getElementById('filter-model').dispatchEvent(new Event('change'));`);
  const byModel=await evaluate('document.querySelectorAll("details.case:not([hidden])").length');
  if(byModel!==2)throw Error('Model filter must show two cases');
  await evaluate(`document.getElementById('filter-method').value='sql';document.getElementById('filter-method').dispatchEvent(new Event('change'));document.querySelector('details.case:not([hidden])').open=true;document.querySelectorAll('details.case:not([hidden]) details').forEach(el=>el.open=true);`);
  const inspected=await evaluate(`(()=>{const el=document.querySelector('details.case:not([hidden])');return {visible:document.querySelectorAll('details.case:not([hidden])').length,question:el.dataset.question,model:el.dataset.model,method:el.dataset.method,hasTools:el.textContent.includes('requested_by_model'),hasContext:el.textContent.includes('Exact context sent for the final answer'),hasJudge:el.textContent.includes('Exact context sent to Haiku'),hasRawLinks:el.querySelectorAll('a[href^="raw/"]').length}})()`);
  if(inspected.visible!==1||!inspected.hasTools||!inspected.hasContext||!inspected.hasJudge||!inspected.hasRawLinks)throw Error('Inspector incomplete '+JSON.stringify(inspected));
  await evaluate(`filterIds.forEach(k=>document.getElementById('filter-'+k).value='');filterCases();document.querySelectorAll('details').forEach(el=>el.open=false);`);
  let shot=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});fs.writeFileSync(path.join(output,'case-report-desktop.png'),Buffer.from(shot.data,'base64'));
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:1100,deviceScaleFactor:1,mobile:false});await delay(200);
  const mobile=await evaluate('({width:innerWidth,content:document.documentElement.scrollWidth})');
  if(mobile.content>mobile.width)throw Error('Mobile overflow '+JSON.stringify(mobile));
  shot=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});fs.writeFileSync(path.join(output,'case-report-mobile.png'),Buffer.from(shot.data,'base64'));
  if(errors.length)throw Error('JavaScript errors '+JSON.stringify(errors));
  const checks={total,byQuestion,byModel,inspected,mobile,javascript_errors:errors,checked_utc:new Date().toISOString()};
  fs.writeFileSync(path.join(output,'case-html-validation.json'),JSON.stringify(checks,null,2));console.log(JSON.stringify(checks));
 }finally{socket?.close();chrome.kill();await new Promise(r=>chrome.once('exit',r));fs.rmSync(profile,{recursive:true,force:true,maxRetries:5,retryDelay:100})}
})().catch(e=>{console.error(e);process.exitCode=1});
