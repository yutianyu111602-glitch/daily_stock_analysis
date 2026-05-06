const path = require("path");

const ci = require("miniprogram-ci");
const cloudApi = require("miniprogram-ci/dist/common/cloud-api");
const cloudCi = require("miniprogram-ci/dist/ci/cloud/cloudapi");

const root = path.resolve(__dirname, "..");
const config = require(path.join(root, "project.config.json"));
const appid = process.env.WECHAT_MINIAPP_APPID || config.appid;
const privateKeyPath = process.env.MINIAPP_PRIVATE_KEY_PATH || "";
const cloudEnv = process.env.WECHAT_CLOUD_ENV_ID || "cloudbase-d8gcpi9mjaf9911ab";
const functionName = process.env.WECHAT_CLOUD_FUNCTION || "quickstartFunctions";

const fail = (message) => {
  console.error(message);
  process.exit(1);
};

const run = async () => {
  if (!appid) fail("Missing WECHAT_MINIAPP_APPID.");
  if (!privateKeyPath) fail("Missing MINIAPP_PRIVATE_KEY_PATH.");

  const project = new ci.Project({
    appid,
    type: "miniProgram",
    projectPath: root,
    privateKeyPath,
    ignores: ["node_modules/**/*", "tmp/**/*", "project.private.config.json"],
  });

  const extAppid = await project.getExtAppid();
  cloudCi.initCloudAPI(extAppid || project.appid);
  const options = {
    request: cloudCi.boundTransactRequest(project),
    transactType: cloudApi.TransactType.IDE,
  };

  const envRes = await cloudApi.tcbGetEnvironments({}, options);
  const env = envRes.envList.find((item) => item.envId === cloudEnv);
  if (!env) fail(`Cloud env not found: ${cloudEnv}`);

  const codeSecret = await cloudCi.get3rdCloudCodeSecret(project);
  const region = env.functions && env.functions[0] && env.functions[0].region;
  const info = await cloudApi.scfGetFunctionInfo({
    namespace: env.envId,
    region,
    functionName,
    codeSecret,
  }, options);

  console.log(JSON.stringify({
    ok: true,
    envId: env.envId,
    region,
    functionName,
    status: info.status,
    statusDesc: info.statusDesc,
    runtime: info.runtime,
    handler: info.handler,
    installDependency: info.installDependency,
    timeout: info.timeout,
    memorySize: info.memorySize,
  }, null, 2));
};

run().catch((error) => fail(error && (error.stack || error.message) ? error.stack || error.message : String(error)));
