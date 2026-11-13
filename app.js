const $=id=>document.getElementById(id);
const escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={queued:'Queued',running:'Running',succeeded:'Succeeded',dead:'Dead letter',cancelled:'Cancelled'};
let token='',busy=false,before=null,nextBefore=null,selected=null,currentJob=null,listVersion=0,detailVersion=0;
const date=value=>new Date(value*1000).toLocaleString();
function notice(message,error=false){$('notice').textContent=message;$('notice').className=error?'error':'';}
async function api(path,body){const response=await fetch(path,{method:body===undefined?'GET':'POST',headers:body===undefined?{}:{'Content-Type':'application/json','X-Dispatch-Token':token},body:body===undefined?undefined:JSON.stringify(body)});const data=await response.json();if(!response.ok)throw Error(data.error??`Request failed (${response.status}).`);return data;}
function setBusy(value){busy=value;for(const id of ['run','new','drill','recover','submit'])$(id).disabled=value||!token;$('cancel').disabled=value||currentJob?.status!=='queued';}
function renderList(data){
 $('counts').innerHTML=Object.entries(labels).map(([state,label])=>`<div class="count"><strong>${data.counts[state]}</strong><span>${label.toUpperCase()}</span></div>`).join('');
 $('jobs').innerHTML=data.jobs.map(job=>`<tr class="${job.id===selected?'selected':''}"><td><button data-id="${job.id}" aria-label="Inspect job ${job.id}"><strong>#${job.id}</strong><span>${escape(job.key)}</span></button></td><td>${escape(job.kind)}<br><span class="hint">priority ${job.priority}</span></td><td><span class="badge ${job.status}">${labels[job.status]}</span></td><td>${job.attempts} / ${job.max_attempts}</td></tr>`).join('')||'<tr><td colspan="4"><p class="hint">No jobs in this view. Create a job or choose another state.</p></td></tr>';
 nextBefore=data.next_before;$('older').disabled=!nextBefore;$('latest').disabled=before===null;$('page-note').textContent=before?`Jobs before #${before}`:'Latest jobs · newest first';
}
async function refresh(){const version=++listVersion;const params=new URLSearchParams();if($('filter').value)params.set('status',$('filter').value);if(before)params.set('before',before);const data=await api('/api/jobs?'+params);if(version!==listVersion)return;renderList(data);if(selected)await inspect(selected);}
async function inspect(id){const version=++detailVersion;selected=id;const job=await api('/api/jobs/'+id);if(version!==detailVersion)return;currentJob=job;$('empty-detail').hidden=true;$('detail').hidden=false;$('selected-id').textContent='#'+id;$('detail-key').textContent=job.key;$('detail-state').textContent=labels[job.status];$('detail-state').className='badge '+job.status;$('detail-meta').textContent=`${job.kind} · attempt ${job.attempts} / ${job.max_attempts} · priority ${job.priority}`;$('payload').textContent=job.text||'(empty text)';$('result').textContent=job.result===null?'No result committed yet.':JSON.stringify(job.result,null,2);$('job-error').hidden=!job.error;$('job-error').textContent=job.error??'';$('schedule').textContent=job.status==='queued'?`Eligible at ${date(job.available)}. Run a worker to process ready jobs.`:job.status==='running'?`Lease expires at ${date(job.lease_until)}. Refresh or recover after expiry.`:`Updated ${date(job.updated)}.`;
 $('events').innerHTML=job.events.map(e=>`<li><strong>${escape(e.event.replaceAll('_',' '))}</strong><time>${escape(date(e.at))}</time><p>${escape(e.detail)}</p></li>`).join('');
 for(const row of $('jobs').querySelectorAll('tr'))row.classList.toggle('selected',Number(row.querySelector('button')?.dataset.id)===id);
 setBusy(busy);
}
async function mutate(path,body={},message='Action completed.'){
 if(busy)return;setBusy(true);
 try{const data=await api(path,body);before=null;if(data.job)selected=data.job.id;await refresh();notice(typeof message==='function'?message(data):message);return data;}catch(error){notice(error.message,true);}finally{setBusy(false);}
}
$('refresh').addEventListener('click',()=>refresh().then(()=>notice('Queue refreshed.')).catch(error=>notice(error.message,true)));
$('filter').addEventListener('change',()=>{before=null;refresh().catch(error=>notice(error.message,true));});
$('older').addEventListener('click',()=>{before=nextBefore;refresh().catch(error=>notice(error.message,true));});
$('latest').addEventListener('click',()=>{before=null;refresh().catch(error=>notice(error.message,true));});
$('jobs').addEventListener('click',event=>{const button=event.target.closest('button[data-id]');if(button)inspect(Number(button.dataset.id)).catch(error=>notice(error.message,true));});
$('run').addEventListener('click',()=>mutate('/api/run-one',{},data=>data.job?`Job #${data.job.id}: ${labels[data.job.status].toLowerCase()} after attempt ${data.job.attempts}.`:'No job is ready. Delayed jobs and retries wait until their eligible time.'));
$('recover').addEventListener('click',()=>mutate('/api/recover',{},data=>`Recovered ${data.recovered} expired lease${data.recovered===1?'':'s'}.`));
$('drill').addEventListener('click',()=>mutate('/api/drill-lease',{},data=>data.job?`Job #${data.job.id} claimed for five seconds and left unfinished. Recover it after expiry.`:'No ready job to claim.'));
$('cancel').addEventListener('click',()=>{if(currentJob)mutate(`/api/jobs/${currentJob.id}/cancel`,{},'Queued job cancelled.');});
$('new-key').addEventListener('click',()=>{$('key').value=crypto.randomUUID();});
$('new').addEventListener('click',()=>{$('key').value=crypto.randomUUID();$('form-notice').textContent='';$('composer').showModal();});
$('close').addEventListener('click',()=>{$('composer').close();});
$('kind').addEventListener('change',()=>{const demo=$('kind').value==='retry-demo';$('fail-first').disabled=!demo;$('fail-first').value=demo?'1':'0';});
$('job-form').addEventListener('submit',async event=>{
 event.preventDefault();if(busy)return;setBusy(true);$('form-notice').textContent='';
 try{const data=await api('/api/jobs',{key:$('key').value,kind:$('kind').value,text:$('text').value,max_attempts:Number($('max-attempts').value),priority:Number($('priority').value),delay_seconds:Number($('delay').value),fail_first:Number($('fail-first').value)});$('composer').close();before=null;selected=data.job.id;await refresh();notice(data.created?`Job #${data.job.id} persisted. Run the next job when you’re ready.`:`Existing job #${data.job.id} returned. No duplicate was created.`);}catch(error){$('form-notice').textContent=error.message;notice(error.message,true);}finally{setBusy(false);}
});
$('export').addEventListener('click',()=>{if(!currentJob)return;const url=URL.createObjectURL(new Blob([JSON.stringify(currentJob,null,2)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download=`dispatch-job-${currentJob.id}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
setBusy(true);
api('/api/session').then(data=>{token=data.token;return refresh();}).then(()=>notice('Connected. Jobs persist across server restarts.')).catch(error=>notice(error.message,true)).finally(()=>setBusy(false));
