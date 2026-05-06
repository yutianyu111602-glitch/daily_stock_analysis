const crypto = require("crypto");
const https = require("https");

const sha256Hex = (text) => crypto.createHash("sha256").update(text, "utf8").digest("hex");

const hmacSha256 = (key, text, encoding) => (
  crypto.createHmac("sha256", key).update(text, "utf8").digest(encoding)
);

const utcDate = (timestamp) => new Date(timestamp * 1000).toISOString().slice(0, 10);

const callTencentApi = ({ secretId, secretKey, service, host, action, version, region, payload }) => {
  const body = JSON.stringify(payload);
  const timestamp = Math.floor(Date.now() / 1000);
  const date = utcDate(timestamp);
  const algorithm = "TC3-HMAC-SHA256";
  const signedHeaders = "content-type;host";
  const canonicalHeaders = `content-type:application/json; charset=utf-8\nhost:${host}\n`;
  const canonicalRequest = [
    "POST",
    "/",
    "",
    canonicalHeaders,
    signedHeaders,
    sha256Hex(body),
  ].join("\n");
  const credentialScope = `${date}/${service}/tc3_request`;
  const stringToSign = [
    algorithm,
    String(timestamp),
    credentialScope,
    sha256Hex(canonicalRequest),
  ].join("\n");
  const secretDate = hmacSha256(`TC3${secretKey}`, date);
  const secretService = hmacSha256(secretDate, service);
  const secretSigning = hmacSha256(secretService, "tc3_request");
  const signature = hmacSha256(secretSigning, stringToSign, "hex");
  const authorization = `${algorithm} Credential=${secretId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return new Promise((resolve, reject) => {
    const req = https.request({
      method: "POST",
      hostname: host,
      path: "/",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/json; charset=utf-8",
        Host: host,
        "X-TC-Action": action,
        "X-TC-Timestamp": String(timestamp),
        "X-TC-Version": version,
        "X-TC-Region": region,
        "Content-Length": Buffer.byteLength(body),
      },
      timeout: 15000,
    }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        let parsed;
        try {
          parsed = JSON.parse(raw);
        } catch (e) {
          reject(new Error(`Tencent API returned non-JSON response: ${raw.slice(0, 120)}`));
          return;
        }
        const response = parsed.Response || {};
        if (response.Error) {
          reject(new Error(`${response.Error.Code}: ${response.Error.Message}`));
          return;
        }
        resolve(response);
      });
    });
    req.on("timeout", () => req.destroy(new Error("Tencent API request timeout")));
    req.on("error", reject);
    req.write(body);
    req.end();
  });
};

const CHINESE_NUMBERS = {
  一: 1,
  二: 2,
  两: 2,
  三: 3,
  四: 4,
  五: 5,
  六: 6,
  七: 7,
  八: 8,
  九: 9,
  十: 10,
};

const clampLimit = (value) => {
  const limit = Number(value || 10);
  return Math.min(Math.max(Number.isFinite(limit) ? limit : 10, 1), 15);
};

const readNumber = (text, patterns) => {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const raw = match[1];
    if (CHINESE_NUMBERS[raw] !== undefined) return CHINESE_NUMBERS[raw];
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
};

const detectSession = (text) => {
  if (/下午|两点半|2点半|14[:：]?30|1430/.test(text)) return "afternoon";
  if (/上午|十点半|10点半|10[:：]?30|1030|中午|午盘/.test(text)) return "morning";
  return "auto";
};

const parseRuleText = (ruleText) => {
  const text = String(ruleText || "").trim();
  if (!text) return { ok: false, message: "没有收到规则文字。" };

  const limit = clampLimit(readNumber(text, [
    /(?:精选|选|来|给我|不要超过|最多)([一二两三四五六七八九十]|\d{1,2})(?:只|个)?/,
  ]));
  const session = detectSession(text);
  const minTurnoverRate = readNumber(text, [
    /换手(?:率)?(?:要)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?/,
    /换手(?:率)?\D{0,8}([一二两三四五六七八九十]|\d+(?:\.\d+)?)\s*(?:以上|个点)/,
  ]);
  const minVolumeRatio = readNumber(text, [
    /量比(?:要)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)/,
  ]);
  const minSectorChangePct = readNumber(text, [
    /(?:板块|行业)\D{0,8}涨幅(?:要)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?/,
    /(?:板块|行业)\D{0,8}涨\s*(\d+(?:\.\d+)?)\s*(?:个点|%)/,
  ]);
  const sectorRankTopN = readNumber(text, [
    /(?:行业|板块).{0,8}(?:前|排名前)([一二两三四五六七八九十]|\d{1,2})/,
  ]);
  const explicitBias = readNumber(text, [
    /(?:5日线|五日线|MA5).{0,12}乖离(?:率)?(?:小于|低于|不高于|<=|<)\s*(\d+(?:\.\d+)?)\s*%?/i,
  ]);
  const maxBiasMa5Pct = explicitBias || (/别离五日线太远|离五日线别太远/.test(text) ? 9 : null);
  const requireCapitalFlowAllPositive = /超大单/.test(text) && /大单/.test(text) && /中单/.test(text) && /流入|净流入|为正/.test(text);

  const profile = {
    limit,
    minVolumeRatio: minVolumeRatio || 1,
    minTurnoverRate: minTurnoverRate || (session === "afternoon" ? 5 : 3),
    minSectorChangePct: minSectorChangePct || 1,
    sectorRankTopN: sectorRankTopN || 5,
    maxBiasMa5Pct: maxBiasMa5Pct || 9,
    requireAbc: /ABC|A-B-C|三浪|调整/.test(text),
    requireCloseAboveMa20: /站上|突破|高于/.test(text) && /20日|二十日|MA20/i.test(text),
    requireMa10Ma20Up: /10日|十日|MA10/i.test(text) && /20日|二十日|MA20/i.test(text) && /朝上|向上/.test(text),
    requireCLowGtALow: /C浪|低点抬高/.test(text) && /A浪|低点抬高/.test(text),
    requireBHighAboveMa20: /B浪/.test(text) && /20日|二十日|MA20/i.test(text),
    requireCapitalFlowAllPositive,
  };

  profile.turnoverText = `> ${profile.minTurnoverRate}%`;
  profile.sectorRankText = `前 ${profile.sectorRankTopN}`;

  return {
    ok: true,
    ruleText: text,
    session,
    sessionText: session === "afternoon" ? "下午/两点半" : session === "morning" ? "上午/十点半" : "自动判断",
    profile,
    summary: `准备按 ${profile.limit} 只以内做A股短线规则筛选，换手率 ${profile.turnoverText}，行业/板块排名 ${profile.sectorRankText}。`,
    lines: [
      `量比 > ${profile.minVolumeRatio}`,
      `换手率 > ${profile.minTurnoverRate}%`,
      `行业/板块涨幅 > ${profile.minSectorChangePct}%`,
      `行业/板块涨幅榜排名前 ${profile.sectorRankTopN}`,
      `5日线乖离率 < ${profile.maxBiasMa5Pct}%`,
      `ABC结构：${profile.requireAbc ? "要求" : "按默认算法识别"}`,
      `大/中/超大单流入：${profile.requireCapitalFlowAllPositive ? "要求" : "不强制"}`,
    ],
  };
};

const recognizeVoice = async (event) => {
  const secretId = process.env.ASR_SECRET_ID || process.env.DSA_MINIAPP_TENCENTCLOUD_SECRET_ID || "";
  const secretKey = process.env.ASR_SECRET_KEY || process.env.DSA_MINIAPP_TENCENTCLOUD_SECRET_KEY || "";
  if (!secretId || !secretKey) {
    return {
      ok: false,
      message: "云函数还没配置腾讯云 ASR 密钥。可先用文字输入规则。",
    };
  }
  let audio = null;
  if (event.audioBase64) {
    audio = Buffer.from(String(event.audioBase64), "base64");
  } else if (event.fileID) {
    return { ok: false, message: "当前版本不再使用云存储 fileID，请更新小程序后重试。" };
  } else {
    return { ok: false, message: "没有收到语音文件。" };
  }
  if (!audio || audio.length === 0) return { ok: false, message: "语音文件为空。" };
  if (audio.length > 2 * 1024 * 1024) return { ok: false, message: "语音太长，请控制在 30 秒内。" };

  try {
    const result = await callTencentApi({
      secretId,
      secretKey,
      service: "asr",
      host: "asr.tencentcloudapi.com",
      action: "SentenceRecognition",
      version: "2019-06-14",
      region: process.env.ASR_REGION || "ap-shanghai",
      payload: {
        SubServiceType: 2,
        ProjectId: 0,
        ConvertNumMode: 1,
        FilterPunc: 0,
        FilterModal: 0,
        FilterDirty: 0,
        EngSerViceType: process.env.ASR_ENGINE_TYPE || "16k_zh",
        SourceType: 1,
        VoiceFormat: event.voiceFormat || process.env.ASR_VOICE_FORMAT || "mp3",
        Data: audio.toString("base64"),
      },
    });
    const text = String(result.Result || "").trim();
    if (!text) return { ok: false, message: "没有识别到文字。" };
    return { ok: true, text };
  } catch (e) {
    return {
      ok: false,
      message: `ASR 调用失败：${e.message || e.code || "unknown"}`,
    };
  }
};

const dispatchGithubWorkflow = ({ ruleText, session }) => {
  const token = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
  const repo = process.env.GITHUB_REPO || "yutianyu111602-glitch/daily_stock_analysis";
  const workflow = process.env.GITHUB_WORKFLOW || "rule_screener.yml";
  const ref = process.env.GITHUB_REF || "main";
  if (!token) {
    return Promise.resolve({
      ok: false,
      message: "云函数还没配置 GITHUB_TOKEN，规则已解析但未提交 GitHub Actions。",
    });
  }

  const payload = JSON.stringify({
    ref,
    inputs: {
      rule_text: ruleText,
      session,
      ai_review: "false",
    },
  });

  return new Promise((resolve) => {
    const req = https.request({
      method: "POST",
      hostname: "api.github.com",
      path: `/repos/${repo}/actions/workflows/${workflow}/dispatches`,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "daily-stock-miniapp",
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
      timeout: 15000,
    }, (res) => {
      res.resume();
      res.on("end", () => {
        if (res.statusCode === 204) {
          resolve({ ok: true, message: "规则选股任务已提交，结果会推送到 TG 和 Server酱。" });
        } else {
          resolve({ ok: false, message: `GitHub Actions 提交失败，状态码 ${res.statusCode}。` });
        }
      });
    });
    req.on("error", () => resolve({ ok: false, message: "GitHub Actions 提交失败，请检查云函数网络和 Token。" }));
    req.write(payload);
    req.end();
  });
};

const submitRule = async (event) => {
  const parsed = parseRuleText(event.ruleText);
  if (!parsed.ok) return parsed;
  return await dispatchGithubWorkflow({
    ruleText: parsed.ruleText,
    session: event.session || parsed.session,
  });
};

exports.main = async (event) => {
  switch (event.type) {
    case "recognizeVoice":
      return await recognizeVoice(event);
    case "parseRule":
      return parseRuleText(event.ruleText);
    case "submitRule":
      return await submitRule(event);
    default:
      return { ok: false, message: "未知操作。" };
  }
};
