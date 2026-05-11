/**
 * 秒杀接口压测（k6）
 *
 * 依赖：https://k6.io/docs/getting-started/installation/
 *
 * 1) 单账号冒烟（看延迟、重复下单返回）
 *    浏览器登录后复制 token，或见下方「准备 TOKEN」。
 *
 *    set TOKEN=你的uuid无横线
 *    set VID=秒杀券对应的 voucher id（整数）
 *    k6 run -e BASE=http://127.0.0.1:8081 -e TOKEN=%TOKEN% -e VID=%VID% deploy/stress/k6-seckill.js
 *
 * 2) 多虚拟用户：每个 VU 使用不同 Idempotency-Key（脚本已按 vu+iter 生成）。
 *    业务上同一用户仍只能成功一单；要测「多人抢库存」需多 TOKEN（多账号登录）。
 *
 * 3) 准备 TOKEN（验证码在 Redis）
 *    docker exec life-go-redis redis-cli -a 123456 SET login:code:13800138000 123456
 *    curl -s -X POST http://127.0.0.1:8081/user/login -H "Content-Type: application/json" -d "{\"phone\":\"13800138000\",\"code\":\"123456\"}"
 *    响应里 data 字段即为 TOKEN。
 */
import http from "k6/http";
import { check, fail } from "k6";

export const options = {
  scenarios: {
    seckill: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 20),
      duration: __ENV.DURATION || "30s",
    },
  },
  // 业务大量返回 success:false（重复下单/库存不足）仍可能是 HTTP 200，故不设 http_req_failed
  thresholds: {
    http_req_duration: ["p(95)<5000"],
  },
};

const BASE = __ENV.BASE || "http://127.0.0.1:8081";
const VID = __ENV.VID || "1";

export default function () {
  const token = __ENV.TOKEN;
  if (!token) {
    fail("请设置环境变量 TOKEN（Authorization 值，无 Bearer 前缀）");
  }
  const idem = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const url = `${BASE}/voucher-order/seckill/${VID}`;
  const res = http.post(url, "", {
    headers: {
      Authorization: token,
      "Idempotency-Key": idem,
    },
  });
  const ok = check(res, {
    "HTTP 200": (r) => r.status === 200,
    "JSON success": (r) => {
      try {
        const j = r.json();
        return j && j.success === true;
      } catch {
        return false;
      }
    },
  });
  if (!ok && res.status === 200) {
    // 失败业务：库存不足、重复下单等，便于观察
    try {
      const j = res.json();
      if (j && j.success === false) {
        console.log(`VU${__VU} iter${__ITER}: ${j.errorMsg}`);
      }
    } catch (_) {}
  }
}
