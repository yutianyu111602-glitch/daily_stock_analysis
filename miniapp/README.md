# 规则观察助手小程序

这是给家庭自用的 A 股规则筛选入口。父亲在小程序里按住语音条说规则，小程序把语音转文字并提交 GitHub Actions，筛选结果继续走已配置的 TG 和 Server酱推送链路。

## 本地运行

在微信开发者工具中打开本目录：

```text
C:\code\githubstar\daily_stock_analysis\miniapp
```

云环境 ID 写在 `miniprogram/app.js`，当前使用 `cloudbase-d8gcpi9mjaf9911ab`。

## 必需环境变量

本地 CI 预览/上传需要：

```text
MINIAPP_PRIVATE_KEY_PATH
WECHAT_CLOUD_ENV_ID
WECHAT_MINIAPP_APPID
```

云函数运行需要在 CloudBase 控制台配置：

```text
ASR_SECRET_ID
ASR_SECRET_KEY
ASR_REGION
GITHUB_TOKEN
GITHUB_REPO
GITHUB_WORKFLOW
GITHUB_REF
```

不要把私钥、Token、Secret 写进源码。

## 常用命令

```powershell
npm install
npm run check
npm run preview
npm run status:function
npm run upload:function
npm run upload
```

`npm run upload:function` 默认不让云端临时安装依赖。当前云函数没有 npm 依赖，部署包更小，也避免远端依赖安装卡住；如果以后给云函数新增依赖，先在云函数目录执行 `npm install`，或临时设置 `MINIAPP_REMOTE_NPM_INSTALL=true`。

如果 `npm run upload:function` 报 `invalid ip`，说明小程序上传密钥的 IP 白名单没有当前公网 IP。关闭白名单，或把当前公网 IP 加进去后重试。

如果 `npm run upload:function` 长时间无返回，先执行 `npm run status:function`。只要状态是 `Active`，说明云函数运行态正常；如果要强制确认代码版本，用微信开发者工具右键 `cloudfunctions/quickstartFunctions`，选择上传并部署。

`npm run preview` 会生成预览二维码：

```text
miniapp\tmp\miniapp-ci\preview.jpg
```

## 语音链路

当前实现不再经过云存储中转：

```text
按住说话 -> 本地录音 -> base64 调云函数 -> 腾讯云一句话识别 -> 规则解析 -> GitHub Actions
```

这样比“先上传音频文件再下载识别”少一次云存储读写，失败点更少。
云函数直接用原生 HTTPS 调腾讯云 ASR API，不依赖腾讯云 Node SDK，也不依赖云存储。
