<script setup>
import { ref, useId, onMounted } from 'vue';
import { track } from './analytics.mjs';
const id = useId();
const name = ref(''), email = ref(''), organization = ref(''), message = ref(''), consent = ref(false), website = ref('');
const busy = ref(false), sent = ref(false), error = ref('');
let requestId;
onMounted(() => track('docs_inquiry_opened'));
async function submit() {
  if (busy.value || sent.value) return;
  busy.value = true; error.value = '';
  requestId ||= Array.from(crypto.getRandomValues(new Uint8Array(16)), b => b.toString(16).padStart(2, '0')).join('');
  try {
    const response = await fetch('https://nornicdb.duckdns.org:4269/inquiries', {
      method: 'POST', credentials: 'omit', headers: { 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(12000),
      body: JSON.stringify({ requestId, name: name.value, email: email.value, organization: organization.value, message: message.value, consent: consent.value, website: website.value }),
    });
    if (!response.ok) throw Error(response.status === 429 ? 'The inquiry service is busy or the request limit was reached. Please wait and try again.' : response.status === 413 ? 'Your message is too large. Please shorten it and try again.' : 'Your inquiry could not be saved. Please try again.');
    sent.value = true;
    name.value = email.value = organization.value = message.value = '';
    track('docs_inquiry_submitted');
  } catch (cause) {
    error.value = cause.name === 'TypeError' || cause.name === 'TimeoutError' ? 'The inquiry service is unavailable. Your form is still here; please try again.' : cause.message;
  } finally { busy.value = false; }
}
</script>
<template>
    <section class="inquiry-form" aria-label="Managed services inquiry">
      <template v-if="!sent">
        <form @submit.prevent="submit">
          <label :for="id + '-name'">Name</label><input :id="id + '-name'" v-model="name" autocomplete="name" maxlength="100" required>
          <label :for="id + '-email'">Work email</label><input :id="id + '-email'" v-model="email" type="email" autocomplete="email" maxlength="254" required>
          <label :for="id + '-organization'">Organization <small>(optional)</small></label><input :id="id + '-organization'" v-model="organization" autocomplete="organization" maxlength="160">
          <label :for="id + '-message'">What would you like to build?</label><textarea :id="id + '-message'" v-model="message" maxlength="2000" rows="4" required :aria-describedby="id + '-help'"></textarea>
          <small :id="id + '-help'">Workload, hosting requirements, or timeline. No credentials or confidential data.</small>
          <label class="trap" aria-hidden="true">Website<input v-model="website" tabindex="-1" autocomplete="off"></label>
          <label class="consent"><input v-model="consent" type="checkbox" required><span>You may contact me about this inquiry.</span></label>
          <p class="privacy">Stored in our NornicDB instance for the inquiry administrator to respond. Ask for deletion when we contact you. Form fields are never sent to analytics.</p>
          <button class="submit" :disabled="busy">{{ busy ? 'Sending…' : 'Send inquiry' }}</button>
          <p v-if="error" role="alert" class="error">{{ error }}</p>
        </form>
      </template>
      <p v-else role="status">Thank you. Your inquiry has been received. We’ll reply to the email you provided.</p>
    </section>
</template>
<style scoped>
.inquiry-form{margin:24px 0;padding:24px;border:1px solid var(--vp-c-divider);border-radius:12px;color:var(--vp-c-text-1)}
p{margin:12px 0;color:var(--vp-c-text-2)}label{display:block;font-weight:500;margin:16px 0 6px}input:not([type=checkbox]),textarea{display:block;width:100%;box-sizing:border-box;border:1px solid var(--vp-c-divider);background:var(--vp-c-bg-alt);border-radius:6px;color:inherit;padding:10px 12px;font:inherit}textarea{resize:vertical}input:focus-visible,textarea:focus-visible,button:focus-visible{outline:2px solid var(--vp-c-brand-1);outline-offset:0}.consent{display:flex;gap:10px;align-items:flex-start;font-weight:400}.consent input{margin-top:6px}.privacy,small{font-size:13px;color:var(--vp-c-text-2)}.submit{background:var(--vp-button-brand-bg);color:var(--vp-button-brand-text);padding:10px 18px;border-radius:6px;font-weight:600}.submit:disabled{opacity:.6;cursor:wait}.error{color:var(--vp-c-danger-1)}.trap{position:absolute;left:-10000px}
@media(max-width:600px){.inquiry-form{padding:16px}input:not([type=checkbox]),textarea{font-size:16px}}
</style>
