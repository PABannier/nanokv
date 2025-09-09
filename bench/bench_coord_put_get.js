import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: __ENV.VUS ? +__ENV.VUS : 16,
  duration: __ENV.DUR || '45s',
  thresholds: {
    'http_req_duration{op:put}': ['p(95)<3000'],
    'http_req_duration{op:get}': ['p(95)<1200'],
    'checks': ['rate>0.99'],
  },
};

const BASE = __ENV.BASE || 'http://127.0.0.1:3000';
const SIZE = __ENV.SIZE ? +__ENV.SIZE : (1<<20); // default 1 MiB
const payload = new Uint8Array(SIZE).fill(7);

export default function () {
  const key = encodeURIComponent(`bench/${__ITER}-${Math.random().toString(16).slice(2)}`);
  const put = http.put(`${BASE}/${key}`, payload, { tags: { op: 'put' }});
  check(put, { 'PUT ok': r => r.status === 200 || r.status === 201 });
  const get = http.get(`${BASE}/${key}`, { tags: { op: 'get' }});
  check(get, { 'GET ok': r => r.status === 200 });
  sleep(0);
}
