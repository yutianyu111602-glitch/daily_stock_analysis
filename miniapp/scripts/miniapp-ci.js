const fs = require("fs");
const path = require("path");
const ci = require("miniprogram-ci");

const root = path.resolve(__dirname, "..");
const configPath = path.join(root, "project.config.json");
const projectConfig = JSON.parse(fs.readFileSync(configPath, "utf8"));
const appid = process.env.WECHAT_MINIAPP_APPID || projectConfig.appid;
const privateKeyPath = process.env.MINIAPP_PRIVATE_KEY_PATH;
const cloudEnv = process.env.WECHAT_CLOUD_ENV_ID || "cloudbase-d8gcpi9mjaf9911ab";
const functionName = process.env.WECHAT_CLOUD_FUNCTION || "quickstartFunctions";
const remoteNpmInstall = process.env.MINIAPP_REMOTE_NPM_INSTALL === "true";

const fail = (message) => {
  console.error(message);
  process.exit(1);
};

if (!appid) fail("Missing WECHAT_MINIAPP_APPID.");
if (!privateKeyPath) fail("Missing MINIAPP_PRIVATE_KEY_PATH.");
if (!fs.existsSync(privateKeyPath)) fail(`Private key not found: ${privateKeyPath}`);

const project = new ci.Project({
  appid,
  type: "miniProgram",
  projectPath: root,
  privateKeyPath,
  ignores: ["node_modules/**/*", "tmp/**/*", "project.private.config.json"],
});

const setting = {
  es6: true,
  minified: true,
  minifyWXML: true,
  minifyWXSS: true,
};

const version = () => {
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 12);
  return process.env.MINIAPP_VERSION || `0.1.${stamp}`;
};

const ensureTmp = () => {
  const dir = path.join(root, "tmp", "miniapp-ci");
  fs.mkdirSync(dir, { recursive: true });
  return dir;
};

const run = async () => {
  const command = process.argv[2] || "preview";
  if (command === "preview") {
    const out = path.join(ensureTmp(), "preview.jpg");
    const result = await ci.preview({
      project,
      desc: "daily-stock-analysis preview",
      setting,
      qrcodeFormat: "image",
      qrcodeOutputDest: out,
      pagePath: "pages/index/index",
      onProgressUpdate: console.log,
    });
    console.log(JSON.stringify({ ok: true, qrcode: out, result }, null, 2));
    return;
  }
  if (command === "upload") {
    const result = await ci.upload({
      project,
      version: version(),
      desc: process.env.MINIAPP_DESC || "voice rule assistant update",
      setting,
      onProgressUpdate: console.log,
    });
    console.log(JSON.stringify({ ok: true, result }, null, 2));
    return;
  }
  if (command === "upload-function") {
    const functionPath = path.join(root, "cloudfunctions", functionName);
    const functionPackage = JSON.parse(fs.readFileSync(path.join(functionPath, "package.json"), "utf8"));
    const dependencies = Object.keys(functionPackage.dependencies || {});
    if (!remoteNpmInstall && dependencies.length > 0 && !fs.existsSync(path.join(functionPath, "node_modules"))) {
      fail(`Missing local cloud function dependencies. Run: cd ${functionPath} && npm install`);
    }
    const result = await ci.cloud.uploadFunction({
      project,
      env: cloudEnv,
      name: functionName,
      path: functionPath,
      remoteNpmInstall,
    });
    console.log(JSON.stringify({ ok: true, result }, null, 2));
    return;
  }
  fail(`Unknown command: ${command}`);
};

run().catch((error) => {
  fail(error && (error.stack || error.message) ? error.stack || error.message : String(error));
});
