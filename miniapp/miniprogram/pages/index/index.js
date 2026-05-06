const recorderManager = wx.getRecorderManager();
const fileSystem = wx.getFileSystemManager();

Page({
  data: {
    recording: false,
    recognizing: false,
    parsing: false,
    submitting: false,
    ruleText: "",
    parsed: null,
    submitResult: null,
    voiceStatus: "按住下方语音条，说完松手。",
    voiceError: "",
    examples: [
      "按我那套规则筛，量比大于1，换手大于3，行业前五，精选10只。",
      "下午两点半严格一点，换手要大于5，大单中单超大单都流入。",
    ],
  },

  onLoad() {
    recorderManager.onStart(() => {
      this.recordStartAt = Date.now();
      this.setData({
        recording: true,
        submitResult: null,
        voiceError: "",
        voiceStatus: "正在听，说完松手发送。",
      });
    });
    recorderManager.onError((res) => {
      this.setData({
        recording: false,
        voiceStatus: "录音没有成功，可以再按住说一次。",
        voiceError: res.errMsg || "录音失败",
      });
      wx.showToast({ title: res.errMsg || "录音失败", icon: "none" });
    });
    recorderManager.onStop((res) => {
      const duration = Date.now() - (this.recordStartAt || Date.now());
      this.setData({ recording: false });
      if (this.cancelingRecord) {
        this.cancelingRecord = false;
        this.setData({ voiceStatus: "已取消这次语音。" });
        return;
      }
      if (duration < 700) {
        this.setData({ voiceStatus: "说话时间太短，请按住说完整一句。" });
        wx.showToast({ title: "说话时间太短", icon: "none" });
        return;
      }
      this.recognizeVoice(res.tempFilePath);
    });
  },

  startRecord() {
    if (this.data.recognizing || this.data.parsing || this.data.submitting) return;
    wx.authorize({
      scope: "scope.record",
      success: () => this.beginRecord(),
      fail: () => {
        wx.showModal({
          title: "需要麦克风权限",
          content: "请允许小程序使用麦克风，才能按住语音条说规则。",
          confirmText: "去设置",
          success: (res) => {
            if (res.confirm) wx.openSetting();
          },
        });
      },
    });
  },

  beginRecord() {
    recorderManager.start({
      duration: 30000,
      sampleRate: 16000,
      numberOfChannels: 1,
      encodeBitRate: 48000,
      format: "mp3",
    });
  },

  stopRecord() {
    if (!this.data.recording) return;
    recorderManager.stop();
  },

  recognizeVoice(tempFilePath) {
    if (!tempFilePath) return;
    if (this.data.recognizing) return;
    this.setData({
      recognizing: true,
      voiceError: "",
      voiceStatus: "正在识别语音，不需要再点。",
    });
    wx.showLoading({ title: "识别语音中" });
    fileSystem.readFile({
      filePath: tempFilePath,
      encoding: "base64",
      success: (fileRes) => {
        wx.cloud.callFunction({
          name: "quickstartFunctions",
          data: {
            type: "recognizeVoice",
            audioBase64: fileRes.data,
            voiceFormat: "mp3",
          },
        })
          .then((resp) => {
            const result = resp.result || {};
            if (!result.ok || !result.text) {
              const message = result.message || "语音识别失败";
              this.setData({ voiceStatus: "没有听清，可以再说一次。", voiceError: message });
              wx.showToast({ title: message, icon: "none" });
              return;
            }
            this.setData({
              ruleText: result.text,
              parsed: null,
              submitResult: null,
              voiceStatus: "已转成文字，请确认系统理解是否正确。",
            });
            this.pendingAutoParse = true;
          })
          .catch((err) => {
            const message = err.errMsg || err.message || "语音识别调用失败";
            this.setData({ voiceStatus: "语音没有发送成功。", voiceError: message });
            wx.showModal({ title: "语音识别失败", content: message, showCancel: false });
          })
          .finally(() => {
            const shouldAutoParse = this.pendingAutoParse;
            this.pendingAutoParse = false;
            this.setData({ recognizing: false });
            wx.hideLoading();
            if (shouldAutoParse) this.parseRule();
          });
      },
      fail: (err) => {
        const message = err.errMsg || "读取录音文件失败";
        this.setData({
          recognizing: false,
          voiceStatus: "录音文件读取失败，请再试一次。",
          voiceError: message,
        });
        wx.hideLoading();
        wx.showModal({ title: "录音读取失败", content: message, showCancel: false });
      },
    });
  },

  cancelRecord() {
    if (!this.data.recording) return;
    this.cancelingRecord = true;
    recorderManager.stop();
    this.setData({
      recording: false,
      voiceStatus: "已取消这次语音。",
    });
  },

  clearRule() {
    this.setData({
      ruleText: "",
      parsed: null,
      submitResult: null,
      voiceError: "",
      voiceStatus: "按住下方语音条，说完松手。",
    });
  },

  copyRuleText() {
    if (!this.data.ruleText) return;
    wx.setClipboardData({
      data: this.data.ruleText,
      success: () => wx.showToast({ title: "已复制", icon: "none" }),
    });
  },

  onTextInput(e) {
    this.setData({
      ruleText: e.detail.value,
      parsed: null,
      submitResult: null,
    });
  },

  useExample(e) {
    const text = this.data.examples[e.currentTarget.dataset.index];
    this.setData({ ruleText: text, parsed: null, submitResult: null });
    this.parseRule();
  },

  parseRule() {
    const ruleText = this.data.ruleText.trim();
    if (this.data.parsing) return;
    if (!ruleText) {
      wx.showToast({ title: "先说一句规则", icon: "none" });
      return;
    }
    this.setData({ parsing: true });
    wx.showLoading({
      title: "理解规则中",
    });
    wx.cloud
      .callFunction({
        name: "quickstartFunctions",
        data: {
          type: "parseRule",
          ruleText,
        },
      })
      .then((resp) => {
        this.setData({ parsed: resp.result });
      })
      .catch((err) => {
        console.error("parseRule failed", err);
        wx.showModal({
          title: "云函数调用失败",
          content: err.errMsg || err.message || "请检查 quickstartFunctions 是否已重新上传部署。",
          showCancel: false,
        });
      })
      .finally(() => {
        this.setData({ parsing: false });
        wx.hideLoading();
      });
  },

  submitRule() {
    if (this.data.submitting) return;
    if (!this.data.parsed || !this.data.parsed.ok) {
      wx.showToast({ title: "先解析规则", icon: "none" });
      return;
    }
    this.setData({ submitting: true });
    wx.cloud
      .callFunction({
        name: "quickstartFunctions",
        data: {
          type: "submitRule",
          ruleText: this.data.ruleText,
          session: this.data.parsed.session,
        },
      })
      .then((resp) => {
        this.setData({ submitResult: resp.result });
      })
      .catch((err) => {
        console.error("submitRule failed", err);
        wx.showModal({
          title: "提交失败",
          content: err.errMsg || err.message || "请检查云函数日志。",
          showCancel: false,
        });
      })
      .finally(() => {
        this.setData({ submitting: false });
      });
  },
});
