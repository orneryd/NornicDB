// Public, write-only project token. Never put a personal API key here.
export const projectToken = 'phc_mQqQ7UAKQMihqwUEKgoRY8mcWNbg2UKjfsLATAVJTdB7';
const events = new Set(['$pageview', 'docs_search_results', 'docs_search_result_opened', 'docs_code_explored', 'docs_outbound_clicked', 'docs_inquiry_opened', 'docs_inquiry_submitted']);
const fields = ['distinct_id', '$session_id', '$window_id', '$lib', '$lib_version', 'token', 'path', 'version', 'scope', 'mode', 'results', 'destination'];
export function sanitize(event) {
  if (!event || !events.has(event.event)) return null;
  event.properties = Object.fromEntries(fields.filter(k => event.properties?.[k] !== undefined).map(k => [k, event.properties[k]]));
  event.properties.$process_person_profile = false;
  event.properties.$geoip_disable = true;
  if (event.properties.path) {
    event.properties.$pathname = event.properties.path;
    event.properties.$current_url = 'https://orneryd.github.io' + event.properties.path;
  }
  return event;
}
let client, pending;
export const consentKey = 'nornic-docs-analytics-consent';
export function analyticsAvailable() {
  return typeof location !== 'undefined' && location.hostname === 'orneryd.github.io' && navigator.doNotTrack !== '1' && !navigator.globalPrivacyControl;
}
export function readConsent() {
  try {
    const choice = localStorage.getItem(consentKey);
    if (choice === 'accepted' || choice === 'declined') return choice;
    return localStorage.getItem('nornic-docs-analytics-disabled') === 'true' ? 'declined' : null;
  } catch {
    return 'declined';
  }
}
function allowed() {
  return analyticsAvailable() && readConsent() === 'accepted';
}
async function initialize() {
  if (client || !allowed()) return;
  if (!pending) pending = import('posthog-js').then(({default: posthog}) => {
    if (!allowed()) return;
    posthog.init(projectToken, {
      api_host: 'https://us.i.posthog.com',
      autocapture: false, capture_pageview: false, capture_pageleave: false,
      capture_exceptions: false, capture_performance: false,
      disable_session_recording: true, disable_surveys: true,
      advanced_disable_feature_flags: true, advanced_disable_decide: true,
      disable_external_dependency_loading: true, disable_compression: true,
      person_profiles: 'never', persistence: 'memory', ip: false,
      opt_out_capturing_by_default: true,
      before_send: sanitize,
    });
    client = posthog;
    syncConsent();
  }).catch(() => {}).finally(() => { pending = undefined; });
  return pending;
}
export async function track(event, properties = {}) {
  if (!allowed()) return;
  await initialize();
  if (allowed()) client?.capture(event, {version: import.meta.env?.VITE_DOCS_VERSION || 'current', ...properties});
}
export function writeConsent(choice) {
  try {
    localStorage.setItem(consentKey, choice);
    return true;
  } catch {
    return false;
  }
}
export function syncConsent() {
  if (!client) return;
  if (allowed()) client.opt_in_capturing({captureEventName: false});
  else client.opt_out_capturing();
}
