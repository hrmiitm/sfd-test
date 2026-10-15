'use strict';
(()=>{
const D=window.BENCHMARK_DATA;
const $=id=>document.getElementById(id);
const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined&&text!==null)n.textContent=String(text);if(cls)n.className=cls;return n;};
if(!D){$('status-banner').textContent='Dashboard data is missing. Run scripts/analyze-results.py to build analysis/all100/dashboard-data.js.';return;}
const num=(x,d=0)=>x===null||x===undefined?'—':Number(x).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const usd=x=>x===null||x===undefined?'unknown':'$'+Number(x).toFixed(6);
const friendly=m=>m.includes('gemini')?'Gemini 3.8 Flash':m.includes('luna')?'GPT-6 Luna':m.includes('haiku')?'Claude Haiku 5.5':m;
const strategy=m=>m==='sql'?'Agentic Search (tool: SQL)':m==='sql_streaming'?'Agentic Search (tools: SQL + streaming)':'Agentic Search (all tools)';
const toolDescription=m=>m==='sql'?'The agent discovers Parquet schemas and runs read-only DuckDB SQL to find and retrieve filing evidence.':m==='sql_streaming'?'The agent uses schema discovery and DuckDB SQL, plus streaming company lookup and paginated filing reads to retrieve passages with source offsets.':'Agentic search over Parquet files using schema discovery, read-only DuckDB SQL and optional streaming lookup and filing reads.';
const strategyNode=m=>{const n=el('span',strategy(m));n.title=toolDescription(m);n.tabIndex=0;return n;};
const pretty=x=>JSON.stringify(x,null,2);
const byId=new Map(D.cases.map(c=>[c.task_id,c]));
const difficulties=['easy','medium','hard','very_hard','extreme','unanswerable'];
const totalCost=D.audit.billing.known_usd;
const scoreNode=(value,callback)=>{const n=el(callback?'button':'span',value===null||value===undefined?'—':num(value,1),'score '+(value==null?'unknown':value<50?'low':value<80?'mid':''));if(callback){n.type='button';n.onclick=callback;n.title='Inspect the answer, tool evidence and Haiku score';}return n;};
const pill=(text,cls)=>el('span',text,'pill '+(cls||''));
const inspectButton=id=>{const b=el('button','Inspect');b.type='button';b.onclick=()=>openCase(id);return b;};
const longText=text=>{const outer=el('div',null,'cell-long');const span=el('span',text??'—');outer.append(span);outer.title=text??'';return outer;};
const moneyCell=(value,row)=>{const n=el('span',(row.unknown_cost_attempts?'≥ ':'')+usd(value),'numeric');if(row.unknown_cost_attempts)n.title=`Known subtotal; ${row.unknown_cost_attempts} unpriced API attempt(s)`;return n;};
const column=(id,label,options={})=>({id,label,get:r=>r[id],...options});
const selectOptions=(id,values,label=v=>v)=>values.forEach(v=>{const o=el('option',label(v));o.value=v;$(id).append(o);});
let inspectorVersion=0;

class DataTable{
 constructor(id,columns,rows,{pageSize=25,searchPlaceholder='Search…',name='table',compact=false,actionsTarget=null}={}){
  this.root=$(id);this.compact=compact;if(compact)this.root.classList.add('compact-table');this.columns=columns;this.rows=rows;this.filter=()=>true;this.page=0;this.pageSize=pageSize;this.sortId=null;this.direction=1;this.name=name;
  this.visible=new Set(columns.filter(c=>c.visible!==false).map(c=>c.id));
  const toolbar=el('div',null,'table-toolbar');this.search=el('input');this.search.type='search';this.search.placeholder=searchPlaceholder;this.search.setAttribute('aria-label',name+' search');
  this.search.addEventListener('input',()=>{clearTimeout(this.debounce);this.debounce=setTimeout(()=>{this.page=0;this.render();},140);});
  const menu=el('details',null,'column-menu');menu.append(el('summary','Columns'));const options=el('div',null,'column-options');
  for(const c of columns){const label=el('label');const check=el('input');check.type='checkbox';check.checked=this.visible.has(c.id);check.dataset.column=c.id;check.addEventListener('change',()=>{if(check.checked)this.visible.add(c.id);else this.visible.delete(c.id);this.render();});label.append(check,document.createTextNode(c.label));options.append(label);}
  menu.append(options);const exportButton=el('button',compact?'CSV':'Export filtered CSV');exportButton.type='button';exportButton.onclick=()=>this.export();exportButton.title='Export visible columns as CSV';if(!compact)toolbar.append(this.search);toolbar.append(menu,exportButton);if(compact)toolbar.classList.add('compact-toolbar');(actionsTarget?$(actionsTarget):this.root).append(toolbar);
  this.scroll=el('div',null,'table-scroll');this.scroll.tabIndex=0;this.scroll.setAttribute('aria-label',name+' scrollable results');this.root.append(this.scroll);
  this.footer=el('div',null,'pagination');if(!compact)this.root.append(this.footer);this.render();
 }
 setRows(rows){this.rows=rows;this.page=0;this.render();}
 setFilter(fn){this.filter=fn;this.page=0;this.render();}
 filtered(){const term=this.search.value.toLowerCase().trim();let rows=this.rows.filter(this.filter);if(term)rows=rows.filter(r=>{const text=r._search??Object.values(r).map(v=>typeof v==='string'?v:'').join(' ').toLowerCase();return text.includes(term);});
  const c=this.columns.find(c=>c.id===this.sortId);if(c)rows=rows.slice().sort((a,b)=>{const x=(c.sortGet||c.get)(a),y=(c.sortGet||c.get)(b);if(x==null&&y==null)return 0;if(x==null)return 1;if(y==null)return -1;const v=typeof x==='number'&&typeof y==='number'?x-y:String(x).localeCompare(String(y),undefined,{numeric:true,sensitivity:'base'});return v*this.direction;});return rows;}
 render(){const rows=this.filtered(),size=this.pageSize||Math.max(rows.length,1),pages=Math.max(1,Math.ceil(rows.length/size));this.page=Math.min(this.page,pages-1);this.filteredCount=rows.length;
  const columns=this.columns.filter(c=>this.visible.has(c.id));const table=el('table');table.dataset.table=this.name;const thead=el('thead'),head=el('tr');
  for(const c of columns){const th=el('th');th.scope='col';const button=el('button',c.label+(this.sortId===c.id?(this.direction===1?' ↑':' ↓'):''));button.type='button';button.dataset.sort=c.id;if(c.description)button.title=c.description;button.onclick=()=>{if(this.sortId===c.id)this.direction*=-1;else{this.sortId=c.id;this.direction=c.sortDefault||1;}this.page=0;this.render();};th.setAttribute('aria-sort',this.sortId===c.id?(this.direction===1?'ascending':'descending'):'none');th.append(button);head.append(th);}thead.append(head);table.append(thead);
  const tbody=el('tbody');for(const r of rows.slice(this.page*size,(this.page+1)*size)){const tr=el('tr');if(r.task_id)tr.dataset.case=r.task_id;if(r.question_id||r.id)tr.dataset.question=r.question_id||r.id;
   for(const c of columns){const td=el('td',null,c.numeric?'numeric':'');const value=c.get(r);const content=c.render?c.render(value,r):value===null||value===undefined?'—':c.digits!==undefined?num(value,c.digits):String(value);if(content instanceof Node)td.append(content);else td.textContent=content;tr.append(td);}tbody.append(tr);}table.append(tbody);this.scroll.replaceChildren(table);
  if(!rows.length)this.scroll.append(el('div','No matching rows. Adjust search or filters.','empty'));
  if(!columns.length)this.scroll.append(el('div','Choose at least one column from Columns.','empty'));
  if(this.compact)return;this.footer.replaceChildren();const count=el('span',`${num(rows.length)} matching rows · ${rows.length?this.page*size+1:0}–${Math.min((this.page+1)*size,rows.length)} shown`);count.dataset.role='row-count';
  const controls=el('div',null,'pagination-controls');const label=el('label','Rows ');const select=el('select');select.setAttribute('aria-label',this.name+' page size');for(const v of [10,25,50,100,0]){const option=el('option',v||'All');option.value=v;option.selected=this.pageSize===v;select.append(option);}select.onchange=()=>{this.pageSize=Number(select.value);this.page=0;this.render();};label.append(select);
  const previous=el('button','Previous');previous.type='button';previous.disabled=this.page===0;previous.onclick=()=>{this.page--;this.render();};
  const next=el('button','Next');next.type='button';next.disabled=this.page>=pages-1;next.onclick=()=>{this.page++;this.render();};controls.append(label,previous,el('span',`${this.page+1} / ${pages}`),next);this.footer.append(count,controls);
 }
 export(){const columns=this.columns.filter(c=>this.visible.has(c.id));const escape=value=>{let s=String(value??'');if(/^[=+\-@]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};const text=[columns.map(c=>escape(c.label)).join(','),...this.filtered().map(r=>columns.map(c=>escape((c.exportGet||c.get)(r))).join(','))].join('\n');const url=URL.createObjectURL(new Blob([text],{type:'text/csv;charset=utf-8'}));const link=el('a');link.href=url;link.download=this.name+'-filtered.csv';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
}

const failed=D.audit.failed_cases;
$('status-banner').replaceChildren(document.createTextNode(`${D.audit.scored_cases}/${D.audit.expected_cases} cases scored. ${failed.length} case failed; ${D.audit.failed_http_attempts.length} failed HTTP attempts recovered on retry. ${D.audit.billing.unknown_attempts} attempts have unknown billing. Original records are unchanged.`));
if(failed.length){const b=el('button','Inspect failed case');b.onclick=()=>openCase(failed[0].task_id);$('status-banner').append(b);}
const ds=D.metadata.dataset;
const facts=[`${num(ds.rows)} stored records in ${ds.files} Parquet files under data/parsed; ${(ds.compressed_bytes/2**30).toFixed(2)} GiB compressed.`,`${num(ds.unique_filing_ids)} unique document IDs (file_stem) across these six files; ${ds.months}.`,`${D.metadata.question_count} questions × 2 answer models × 2 strategies = ${D.audit.expected_cases} planned cases.`,`Direct Parquet tools; no embeddings or vector index. ${D.metadata.run_parameters.max_tools}-tool budget per answer.`,`Four concurrent cases; one local data tool at a time. Shared tool cache enabled.`];
facts.forEach(text=>$('dataset-facts').append(el('li',text)));
const corpusCheck=window.BENCHMARK_CORPUS_VERIFICATION;
if(corpusCheck){
 for(const file of corpusCheck.files){const tr=el('tr');[file.filename,num(file.rows),num(file.unique_document_ids),num(file.unique_company_ids)].forEach(value=>tr.append(el('td',value)));$('corpus-file-counts').append(tr);}
 $('corpus-count-notes').textContent=`Across all six files: ${num(corpusCheck.unique_company_ids)} unique company IDs; ${num(corpusCheck.extra_rows_beyond_unique_document_ids)} additional records share an existing document ID. ID counts are distinct from content counts (${num(corpusCheck.unique_stored_hashes)} stored MD5 values). SQL and PyArrow counts agree.`;
}

D.metadata.settings.forEach(s=>{const tr=el('tr');[s.role,friendly(s.model),s.provider,s.reasoning_effort,s.temperature_sent?String(s.temperature):'omitted'].forEach(v=>tr.append(el('td',v)));$('model-settings').append(tr);});
const metrics=[['Scored cases',`${D.audit.scored_cases} / ${D.audit.expected_cases}`,'One finalization failure remains unscored'],['Known API spend','$'+totalCost.toFixed(4),`${D.audit.billing.unknown_attempts} unpriced errors; includes health checks`],['Inference requests',num(D.audit.api_attempts),`${num(D.audit.api_successful_attempts)} successful · ${D.audit.failed_http_attempts.length} failed; ${D.audit.metadata_http_attempts||0} metadata GETs`],['Tool error responses',num(D.audit.tool_errors),`${D.audit.cases_with_tool_errors} cases affected · ${D.audit.cases_reaching_tool_budget} hit the budget`]];
for(const [label,value,note] of metrics){const card=el('div',null,'metric');card.append(el('div',label,'metric-label'),el('div',value,'metric-value'),el('div',note,'metric-note'));$('metrics').append(card);}
$('generated').textContent='Derived export: '+new Date(D.metadata.generated_utc).toLocaleString()+'. Original runtime: '+num(D.audit.resource.wall_seconds/60,1)+' min. Peak process-tree memory: '+num(D.audit.resource.peak_memory_mib)+' MiB; OOM kills: '+(D.audit.resource.memory_events.oom_kill||0)+'.';

const modelColumns=[
 column('model','Model (Agentic Search)',{render:(v,r)=>{const n=el('div',friendly(v)+' · '+(r.method==='sql'?'SQL':r.method==='sql_streaming'?'SQL + streaming':'Both strategies'),'cell-model');n.title=toolDescription(r.method)+' Model: '+v+(r.provider?' · '+r.provider:'');n.tabIndex=0;return n;}}),
 column('mean_correctness','Avg score overall /100',{numeric:true,render:v=>scoreNode(v),sortDefault:-1}),
 column('known_cost_usd','Cost USD: total / avg per request',{numeric:true,
  description:'Known answer + judge cost. Average per benchmark request (one question for one model and strategy), including failed cases; not per HTTP call.',
  exportGet:r=>`${Number(r.known_cost_usd).toFixed(2)} / ${Number(r.mean_known_cost_usd).toFixed(6)}`,
  render:(v,r)=>{const n=el('span',v==null?'—':`$${Number(v).toFixed(2)} / ${usd(r.mean_known_cost_usd)}`);n.title=`Known cost; average over ${r.expected} benchmark requests (question cases). ${r.unknown_cost_attempts||0} attempts have unknown billing.`;return n;}}),
 column('mean_wall_seconds','Avg time / question (s)',{numeric:true,digits:1}),
 column('method','Strategy',{render:v=>strategyNode(v),visible:false}),column('scored','Scored / planned',{get:r=>r.scored,render:(v,r)=>`${v} / ${r.expected}`,numeric:true}),
 column('tool_requested','Tool calls: total / avg per request',{numeric:true,
  description:'All requested tool calls, including errors, cached calls and budget rejections. Average per benchmark request (question case).',
  exportGet:r=>`${r.tool_requested} / ${Number(r.mean_tools).toFixed(2)}`,
  render:(v,r)=>{const n=el('span',`${num(v)} / ${num(r.mean_tools,2)}`);n.title=`${num(v)} tool requests across ${r.expected} question cases; ${num(r.mean_tools,2)} per benchmark request.`;return n;}}),
 column('mean_citation_support','Citation support /100',{numeric:true,digits:2,visible:false}),
 column('coverage_adjusted_correctness','Missing = 0',{numeric:true,digits:2,visible:false}),
 column('mean_known_cost_usd','Known USD / case',{numeric:true,render:moneyCell,visible:false}),
 column('mean_tools','Mean tools',{numeric:true,digits:2,visible:false}),column('tool_errors','Tool errors',{numeric:true,visible:false}),
 column('tool_dispatched','Dispatched tools',{numeric:true,visible:false}),column('tool_cache_hits','Cache hits',{numeric:true,visible:false}),
 column('cases_with_tool_errors','Cases with tool errors',{numeric:true,visible:false}),column('budget_exhausted_cases','Cases at budget',{numeric:true,visible:false}),
 column('api_attempts','API attempts',{numeric:true,visible:false}),column('api_failed_attempts','HTTP failures',{numeric:true,visible:false}),
 column('unknown_cost_attempts','Unknown costs',{numeric:true,visible:false}),column('answer_known_cost_usd','Known answer USD',{numeric:true,render:moneyCell,visible:false}),
 column('judge_known_cost_usd','Known judge USD',{numeric:true,render:moneyCell,visible:false}),column('prompt_tokens','Input tokens',{numeric:true,visible:false,render:v=>num(v)}),
 column('completion_tokens','Output tokens',{numeric:true,visible:false,render:v=>num(v)}),column('p95_wall_seconds','P95 wall sec',{numeric:true,digits:1,visible:false})];
const modelTable=new DataTable('summary-table',modelColumns,D.strategy_summary,{pageSize:0,name:'model-summary',compact:true,actionsTarget:'summary-actions'});
function difficultyRows(split){
 const groups=split?D.strategy_summary:D.model_summary;
 return groups.map(group=>{
  const result={model:group.model,method:group.method};
  for(const level of difficulties){
   const members=D.cases.filter(c=>c.model===group.model&&(!split||c.method===group.method)&&c.difficulty===level);
   const scored=members.filter(c=>c.status==='ok'&&Number.isFinite(c.correctness));
   result[level]=scored.length?scored.reduce((sum,c)=>sum+c.correctness,0)/scored.length:null;
   result[level+'_scored']=scored.length;result[level+'_expected']=members.length;
  }
  return result;
 });
}
const difficultyColumns=[{...modelColumns[0],label:'Model'},column('method','Strategy',{render:v=>strategyNode(v),visible:false})];
for(const level of difficulties) difficultyColumns.push(column(level,level.replaceAll('_',' '),{numeric:true,render:(value,row)=>{
 const cell=el('div');cell.append(scoreNode(value));cell.title=`${row[level+'_scored']} / ${row[level+'_expected']} scored`;cell.setAttribute('aria-label',`${num(value,1)} out of 100; ${cell.title}`);return cell;
}}));
const difficultyTable=new DataTable('difficulty-table',difficultyColumns,difficultyRows(true),{pageSize:0,name:'difficulty-comparison',compact:true,actionsTarget:'difficulty-actions'});
$('split-strategy').onchange=()=>{const split=$('split-strategy').checked;modelTable.setRows(split?D.strategy_summary:D.model_summary);difficultyTable.setRows(difficultyRows(split));};

const questionColumns=[column('id','Question ID',{render:(v,r)=>{const b=el('button',v);b.onclick=()=>openCase(r.case_ids[0]);return b;}}),column('difficulty','Difficulty',{render:v=>pill(v.replaceAll('_',' '))}),column('question','Question',{render:longText}),column('reference','Reference answer',{render:longText,visible:false})];
for(const model of D.metadata.answer_models)for(const method of D.metadata.methods){const id=model+'|'+method;questionColumns.push(column(id,`${model.includes('gemini')?'Flash':'Luna'} · ${strategy(method)}`,{description:toolDescription(method),get:r=>r.scores[id],numeric:true,render:(v,r)=>{const c=D.cases.find(c=>c.question_id===r.id&&c.model===model&&c.method===method);return scoreNode(v,()=>openCase(c.task_id));}}));}
questionColumns.push(column('mean_correctness','Mean /100',{numeric:true,render:v=>scoreNode(v)}),column('scored','Scored cases',{numeric:true,render:(v,r)=>`${v}/${r.expected}`}),column('total_correctness','Score sum /400',{numeric:true,digits:1,visible:false}),column('tool_requested','Tool requests',{numeric:true,visible:false}),column('known_cost_usd','Known USD',{numeric:true,render:moneyCell,visible:false}),column('flags','Review flags',{get:r=>r.flags.join(', '),visible:false}),column('inspect','Details',{get:()=>'',render:(_,r)=>inspectButton(r.case_ids[0])}));
const questionTable=new DataTable('question-table',questionColumns,D.questions,{name:'question-comparison',searchPlaceholder:'Search question ID, text or reference…'});
selectOptions('question-difficulty',difficulties,v=>v.replaceAll('_',' '));$('question-difficulty').onchange=()=>questionTable.setFilter(q=>!$('question-difficulty').value||q.difficulty===$('question-difficulty').value);

const caseColumns=[column('question_id','Question ID',{render:(v,r)=>{const n=el('button',v);n.onclick=()=>openCase(r.task_id);return n;}}),column('difficulty','Difficulty',{render:v=>pill(v.replaceAll('_',' '))}),
 column('model','Model',{render:v=>el('span',friendly(v),'cell-model')}),column('method','Strategy',{render:v=>strategyNode(v)}),column('status','Status',{render:v=>pill(v==='ok'?'scored':'failed',v)}),
 column('question','Question',{render:longText}),column('answer','Final answer',{render:longText}),column('reference','Reference',{render:longText,visible:false}),
 column('correctness','Correctness /100',{numeric:true,render:(v,r)=>scoreNode(v,()=>openCase(r.task_id))}),column('citation_support','Citation support /100',{numeric:true,render:v=>scoreNode(v),visible:false}),
 column('tool_requested','Tool requests',{numeric:true}),column('tool_errors','Tool errors',{numeric:true}),column('known_cost_usd','Known USD',{numeric:true,render:moneyCell}),column('wall_seconds','Wall sec',{numeric:true,digits:1}),
 column('judge_model','Judge model',{render:v=>v?friendly(v):'not judged',visible:false}),column('judge_rationale','Judge rationale',{render:longText,visible:false}),
 column('api_attempts','API attempts',{numeric:true,visible:false}),column('api_failed_attempts','HTTP failures',{numeric:true,visible:false}),
 column('tool_dispatched','Dispatched tools',{numeric:true,visible:false}),column('tool_cache_hits','Tool cache hits',{numeric:true,visible:false}),column('tool_budget_rejected','Budget rejections',{numeric:true,visible:false}),
 column('prompt_tokens','Input tokens',{numeric:true,render:v=>num(v),visible:false}),column('completion_tokens','Output tokens',{numeric:true,render:v=>num(v),visible:false}),
 column('reasoning_tokens','Reasoning tokens',{numeric:true,render:v=>num(v),visible:false}),column('cache_read_tokens','Cached input tokens',{numeric:true,render:v=>num(v),visible:false}),
 column('excerpt_characters','Excerpt characters',{numeric:true,render:v=>num(v),visible:false}),column('expected_filing_recall','Expected ID recall',{numeric:true,digits:2,visible:false}),
 column('unknown_cost_attempts','Unknown-cost attempts',{numeric:true,visible:false}),column('error','Case error',{render:longText,visible:false}),column('flags','Review flags',{get:r=>r.flags.join(', '),render:longText,visible:false}),
 column('inspect','Details',{get:()=>'',render:(_,r)=>inspectButton(r.task_id)})];
D.cases.forEach(c=>{c._search=[c.question_id,c.question,c.answer,c.reference,c.model,c.provider,c.method,c.judge_rationale,c.error,c.flags.join(' ')].join(' ').toLowerCase();});
const caseTable=new DataTable('case-table',caseColumns,D.cases,{name:'individual-cases',searchPlaceholder:'Search question, answer, reference, model or rationale…'});
selectOptions('case-model',D.metadata.answer_models,friendly);selectOptions('case-method',D.metadata.methods,strategy);selectOptions('case-difficulty',difficulties,v=>v.replaceAll('_',' '));
function filterCases(){caseTable.setFilter(c=>(!$('case-model').value||c.model===$('case-model').value)&&(!$('case-method').value||c.method===$('case-method').value)&&(!$('case-difficulty').value||c.difficulty===$('case-difficulty').value)&&(!$('case-status').value||c.status===$('case-status').value)&&(!$('only-tool-errors').checked||c.tool_errors>0));}
['case-model','case-method','case-difficulty','case-status','only-tool-errors'].forEach(id=>$(id).addEventListener('change',filterCases));
$('reset-case-filters').onclick=()=>{['case-model','case-method','case-difficulty','case-status'].forEach(id=>$(id).value='');$('only-tool-errors').checked=false;caseTable.search.value='';filterCases();};

function drawChart(){const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 1050 500');svg.setAttribute('role','img');const scale=$('chart-scale').value,level=$('chart-difficulty').value;svg.setAttribute('aria-label',`${scale} known cost per case against mean correctness; ${level||'all difficulties'}`);svg.dataset.scale=scale;svg.dataset.difficulty=level;
 const add=(tag,attrs,text,parent=svg)=>{const n=document.createElementNS(ns,tag);Object.entries(attrs).forEach(([k,v])=>n.setAttribute(k,v));if(text!==undefined)n.textContent=text;parent.append(n);return n;};
 const series=D.strategy_summary.map(s=>{
 if(!level)return s;
 const cases=D.cases.filter(c=>c.model===s.model&&c.method===s.method&&c.difficulty===level),scored=cases.filter(c=>c.status==='ok'&&Number.isFinite(c.correctness));
 return {...s,mean_correctness:scored.length?scored.reduce((n,c)=>n+c.correctness,0)/scored.length:null,mean_known_cost_usd:cases.length?cases.reduce((n,c)=>n+(c.known_cost_usd||0),0)/cases.length:null,unknown_cost_attempts:cases.reduce((n,c)=>n+(c.unknown_cost_attempts||0),0)};
 }).filter(s=>s.mean_known_cost_usd>0&&s.mean_correctness!=null),costs=series.map(s=>s.mean_known_cost_usd),lo=Math.floor(Math.log10(Math.min(...costs))),hi=Math.max(lo+1,Math.ceil(Math.log10(Math.max(...costs))));
 const zoomX=$('chart-fit-x').checked,zoomY=$('chart-fit-y').checked;
 const logMin=Math.log10(Math.min(...costs)),logMax=Math.log10(Math.max(...costs)),logPad=Math.max((logMax-logMin)*0.22,0.12);
 const xMin=scale==='log'?(zoomX?logMin-logPad:lo):(zoomX?Math.max(0,Math.min(...costs)-(Math.max(...costs)-Math.min(...costs))*0.22):0);
 const xMax=scale==='log'?(zoomX?logMax+logPad:hi):Math.max(...costs,0.001)*1.22;
 const scores=series.map(s=>s.mean_correctness),yMin=zoomY?Math.max(0,Math.floor((Math.min(...scores)-8)/5)*5):0,yMax=zoomY?Math.min(100,Math.ceil((Math.max(...scores)+8)/5)*5):100;
 const costX=cost=>80+((scale==='log'?Math.log10(cost):cost)-xMin)/(xMax-xMin)*910;
 const scoreY=score=>415-(score-yMin)/(yMax-yMin)*360;
 svg.dataset.yMin=yMin;svg.dataset.yMax=yMax;
 const yStep=zoomY?5:10;
 for(let score=yMin;score<=yMax;score+=yStep){const y=scoreY(score);add('line',{x1:80,x2:990,y1:y,y2:y,class:'chart-grid axis-grid'});add('text',{x:62,y:y+4,'text-anchor':'end',class:'chart-text'},String(score));}
 const ticks=[];
 if(scale==='log'){
  for(let p=Math.ceil(xMin);p<=Math.floor(xMax);p++)ticks.push(10**p);
 }else for(let i=0;i<=8;i++)ticks.push(xMin+(xMax-xMin)*i/8);
 let lastLabel=-Infinity;
 for(const cost of ticks){const x=costX(cost);add('line',{x1:x,x2:x,y1:55,y2:415,class:'chart-grid axis-grid'});if(x-lastLabel>=76){add('text',{x,y:443,'text-anchor':'middle',class:'chart-text'},'$'+Number(cost.toPrecision(3)));lastLabel=x;}}
 add('path',{d:'M80 55V415H990',class:'chart-axis',fill:'none'});
 for(const s of series){const group=add('g',{class:'chart-series','data-strategy':s.method});const x=costX(s.mean_known_cost_usd),y=scoreY(s.mean_correctness),color=s.model.includes('gemini')?'#245ce2':'#087f80';const attrs={fill:s.unknown_cost_attempts?'#fff':color,stroke:color,'stroke-width':2};const point=s.method==='sql'?add('circle',{...attrs,cx:x,cy:y,r:7},undefined,group):add('rect',{...attrs,x:x-6,y:y-6,width:12,height:12},undefined,group);point.append(document.createElementNS(ns,'title'));point.lastChild.textContent=`${toolDescription(s.method)} ${friendly(s.model)} / ${strategy(s.method)}: ${s.mean_correctness.toFixed(2)}/100; ${usd(s.mean_known_cost_usd)} known/case; ${s.unknown_cost_attempts} unpriced attempts`;const above=s.method==='sql'?(s.model.includes('gemini')?false:true):(s.model.includes('gemini')?true:false);const labelY=y+(above?-14:19);add('text',{x,y:labelY,'text-anchor':'middle',class:'chart-label'},(s.model.includes('gemini')?'Flash':'Luna')+' · '+(s.method==='sql'?'SQL':'SQL + streaming')+' · '+s.mean_correctness.toFixed(1)+'/100',group);}
 add('text',{x:535,y:482,'text-anchor':'middle',class:'chart-text'},'Known API USD per planned case · '+(scale==='log'?'logarithmic':'linear')+' scale');add('text',{transform:'translate(18,235) rotate(-90)','text-anchor':'middle',class:'chart-text'},'Mean correctness /100');$('scatter').replaceChildren(svg);
}
selectOptions('chart-difficulty',difficulties,v=>v.replaceAll('_',' '));drawChart();
for(const id of ['chart-scale','chart-difficulty','chart-fit-x','chart-fit-y'])$(id).onchange=()=>{drawChart();updateChartStrategy();};
let selectedStrategy='all',hoveredStrategy=null;
function updateChartStrategy(){
 document.querySelectorAll('#scatter .chart-series').forEach(group=>{
  const visible=hoveredStrategy!==null||selectedStrategy==='all'||group.dataset.strategy===selectedStrategy;
  group.style.display=visible?'':'none';group.setAttribute('aria-hidden',String(!visible));
  group.classList.toggle('chart-muted',visible&&hoveredStrategy!==null&&group.dataset.strategy!==hoveredStrategy);
  group.classList.toggle('chart-emphasized',visible&&hoveredStrategy!==null&&group.dataset.strategy===hoveredStrategy);
 });
 document.querySelectorAll('#strategy-legend button').forEach(button=>{
  button.setAttribute('aria-pressed',String(button.dataset.strategy===selectedStrategy));
 });
 $('chart-selection').textContent=(selectedStrategy==='all'?'All strategies':strategy(selectedStrategy)+' only')+'. Hover to highlight; click to isolate. Click All strategies to reset.';
}
for(const button of document.querySelectorAll('#strategy-legend button')){
 button.title=button.dataset.strategy==='all'?'Show both agentic search approaches':toolDescription(button.dataset.strategy);
 const preview=()=>{hoveredStrategy=button.dataset.strategy==='all'?null:button.dataset.strategy;updateChartStrategy();};
 const clear=()=>{hoveredStrategy=null;updateChartStrategy();};
 button.addEventListener('mouseenter',preview);button.addEventListener('mouseleave',clear);
 button.addEventListener('focus',preview);button.addEventListener('blur',clear);
 button.addEventListener('click',()=>{selectedStrategy=button.dataset.strategy==='all'||selectedStrategy===button.dataset.strategy?'all':button.dataset.strategy;hoveredStrategy=null;updateChartStrategy();});
}
updateChartStrategy();

for(const f of D.audit.findings){const box=el('article',null,'finding '+f.severity);box.append(el('h3',f.severity.toUpperCase()+' · '+f.title),el('p',f.detail));$('audit-findings').append(box);}
Object.entries(D.audit.tool_error_categories).sort((a,b)=>b[1]-a[1]).forEach(([key,count])=>{const row=el('div',null,'error-row');row.append(el('strong',count),document.createTextNode(key.replaceAll('_',' ')));$('tool-errors').append(row);});
for(const h of D.audit.failed_http_attempts){const c=byId.get(h.tag.split(':')[0]),row=el('div',null,'error-row');row.append(el('strong','HTTP '+h.status_code),document.createTextNode((c?`${c.question_id} · ${friendly(c.model)} · ${strategy(c.method)}`:h.tag)+' · '+(h.retry_recovered?'retry recovered':'recovery not confirmed')));if(c){const b=el('button','Inspect');b.onclick=()=>openCase(c.task_id);row.append(b);}$('http-errors').append(row);}

const detailCache=new Map(),pending=new Map();
window.registerBenchmarkCase=(id,value)=>{detailCache.delete(id);detailCache.set(id,value);while(detailCache.size>4)detailCache.delete(detailCache.keys().next().value);const p=pending.get(id);if(p){pending.delete(id);p.resolve(value);}};
function loadDetail(id){if(detailCache.has(id))return Promise.resolve(detailCache.get(id));if(pending.has(id))return pending.get(id).promise;const c=byId.get(id);const script=el('script');let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});pending.set(id,{promise,resolve,reject});script.src='analysis/all100/'+c.detail_js;script.onerror=()=>{pending.delete(id);script.remove();reject(Error('Could not load the case payload. Open its JSON file directly.'));};script.onload=()=>script.remove();document.body.append(script);return promise;}
function lazyJSON(title,value){const details=el('details');details.append(el('summary',title));let rendered=false;details.addEventListener('toggle',()=>{if(details.open&&!rendered){rendered=true;details.append(el('pre',pretty(value)));}});return details;}
function rawLink(path,label){if(window.BENCHMARK_PUBLISHED&&path.includes('reports-whole-data/')){const n=el('span',label+' (local archive)','small');n.title='Original run files stay local. Complete evidence and API context are included in the derived case export.';return n;}const a=el('a',label);a.href=path;a.target='_blank';a.rel='noopener';return a;}
async function openCase(id){const c=byId.get(id);if(!c)return;const version=++inspectorVersion;const dialog=$('case-dialog');$('dialog-title').textContent=c.question_id+' · '+friendly(c.model);$('dialog-eyebrow').textContent=strategy(c.method)+' / '+(c.status==='ok'?'SCORED':'FAILED');
 const picker=$('dialog-case-picker');picker.replaceChildren();D.cases.filter(v=>v.question_id===c.question_id).forEach(v=>{const o=el('option',friendly(v.model)+' · '+strategy(v.method)+' · '+(v.correctness==null?'unscored':num(v.correctness,1)+'/100'));o.value=v.task_id;o.selected=v.task_id===id;picker.append(o);});
 picker.onchange=()=>openCase(picker.value);const content=$('dialog-content');content.replaceChildren();content.append(el('h3','Question'),el('p',c.question));
 const cards=el('div',null,'inspector-metrics');for(const [label,value] of [['Correctness /100',num(c.correctness,1)],['Citation support /100',num(c.citation_support,1)],['Tool requests / dispatched',c.tool_requested+' / '+c.tool_dispatched],['Known answer + judge USD',usd(c.known_cost_usd)]]){const card=el('div',null,'mini-metric');card.append(el('span',label),el('strong',value));cards.append(card);}content.append(cards);
 if(c.status!=='ok')content.append(el('div',c.error||'No completed answer or judgment.','notice'));
 content.append(el('h3','Final answer'),el('pre',c.answer??'No final answer recorded.','inspector-text'),el('h3','Reference answer'),el('pre',c.reference,'inspector-text'),el('h3','Haiku rationale'),el('p',c.judge_rationale??'No judge response was recorded.'));
 const tags=el('div',null,'tag-list');c.flags.forEach(f=>tags.append(pill(f.replaceAll('_',' '),f==='case_failed'?'error':'')));content.append(tags);
 const links=el('div',null,'raw-links');links.append(rawLink('analysis/all100/'+c.detail_json,'Complete derived case JSON'),rawLink(c.result_file,'Original result'));if(c.final_request_file)links.append(rawLink(c.final_request_file,'Original final/latest request'));if(c.judge_request_file)links.append(rawLink(c.judge_request_file,'Original Haiku request'));content.append(links);
 const loading=el('p','Loading complete retrieval and API context…','small');content.append(loading);if(!dialog.open)dialog.showModal();
 try{const full=await loadDetail(id);if(version!==inspectorVersion)return;loading.remove();const block=el('div',null,'inspector-block');
  block.append(lazyJSON('Tool counts, timing, tokens and billing',{counts:full.tool_counts,timing:full.timing,tokens:full.token_usage,billing:full.billing}));
  block.append(el('h3','Retrieval timeline — exact SQL, arguments and returned data'));
  full.tool_trace.forEach((step,index)=>{const d=el('details');d.append(el('summary',`Step ${index+1} · ${step.tool_call.function.name} · ${step.output.error?'ERROR':step.cache_hit?'CACHE HIT':'completed'}`));let ready=false;d.addEventListener('toggle',()=>{if(d.open&&!ready){ready=true;d.append(el('h3','Exact arguments / SQL'),el('pre',step.tool_call.function.arguments),el('h3','Exact returned output'),el('pre',pretty(step.output)));}});block.append(d);});
  block.append(lazyJSON('Exact context sent for final answer (or latest attempt)',full.final_answer_request),lazyJSON('Exact input sent to Haiku and its raw judgment',full.judge),lazyJSON('All API attempts: models, temperature, reasoning, usage, cost and timing',full.api_attempts),lazyJSON('Evidence accounting and partial-case notes',{evidence:full.evidence_accounting,trace_source:full.trace_source,missing_raw:full.missing_raw_requests,finalization_tool_request:full.raw_finalization_requested_tools_without_answer}));
  const apiLinks=el('details');apiLinks.append(el('summary','Open every original request / response'));for(const a of full.api_attempts){const row=el('div',null,'raw-links');row.append(el('span',a.tag+' · '+a.status));if(a.request_file)row.append(rawLink(full.original_run_relative_to_project+'/'+a.request_file,'Request'));if(a.response_file)row.append(rawLink(full.original_run_relative_to_project+'/'+a.response_file,'Response'));apiLinks.append(row);}block.append(apiLinks);content.append(block);
 }catch(error){if(version===inspectorVersion)loading.textContent=error.message;}
}
$('close-dialog').onclick=()=>$('case-dialog').close();$('case-dialog').addEventListener('click',event=>{if(event.target===$('case-dialog')){const r=event.target.getBoundingClientRect();if(event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom)event.target.close();}});
// Expose read-only inspection hooks for the local browser validation script.
window.benchmarkDashboard={tables:{models:modelTable,difficulty:difficultyTable,questions:questionTable,cases:caseTable},openCase,data:D};
})();
