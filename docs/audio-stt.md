# 语音转文字（STT）

接口：`POST /v1/audio/transcriptions`，使用现有调用 Key：`Authorization: Bearer YOUR_KEY`。

支持 JSON 音频 URL 和 multipart 文件上传。服务端将两类输入统一为音频文件，使用 Web 账号调用 `/backend-api/transcribe`；真实上游可用性仍需要部署后验证。

## JSON：服务端下载音频 URL

```http
POST /v1/audio/transcriptions
Authorization: Bearer YOUR_KEY
Content-Type: application/json
```

```json
{
  "model": "gpt-transcribe",
  "file_url": "https://example.com/recording.mp3",
  "language": "zh",
  "prompt": "这是一段关于图片生成的讨论。",
  "response_format": "json"
}
```

`file_url` 可以换成 `audio_url`、`url` 或 `file`。这些字段也接受 `{"url":"https://example.com/recording.mp3"}` 对象。只支持一个音频地址；同时提供不同地址会报错。地址必须为部署服务器可访问的 HTTP(S) 地址，不接收用户名、密码、本地文件路径或 Base64 Data URL。

成功响应：

```json
{
  "text": "识别得到的文字。"
}
```

`response_format` 为 `text` 时，返回 `Content-Type: text/plain; charset=utf-8` 和纯文本。省略时默认 `json`。SRT、VTT、带时间戳的 verbose_json 尚不支持，显式请求这些格式会返回 400。

`model` 可省略，默认 `gpt-transcribe`；也接收调用方使用的标识如 `whisper-1`，用于兼容和日志展示。当前 Web 转写端点由上游选择实际转写模型，传入这个字段不会切换成官方 Whisper API。`language`、`prompt` 等普通字符串或数字字段会以表单字段传给上游，是否生效由上游决定。

## multipart：上传本地音频

以下示例使用 Windows 的 `curl.exe`：

```powershell
curl.exe "http://127.0.0.1:9080/v1/audio/transcriptions" `
  -H "Authorization: Bearer YOUR_KEY" `
  -F "file=@C:/audio/recording.mp3" `
  -F "model=gpt-transcribe" `
  -F "language=zh" `
  -F "response_format=json"
```

必须上传且只能上传一个名为 `file` 的非空文件。音频输入按流落盘，不把整个音频拼进上游请求的内存缓冲；基础内容检查会拒绝明显的 HTML、文字等内容，音频能否解码仍由上游判断。

## 配置、线路与错误

- `CORE_MAX_FILE_BYTES`：下载后音频大小上限，同时限制整个 multipart 请求（含表单与边界）的大小，默认 256 MiB。JSON 请求体上限为该值与 1 MiB 的较小值，音频文件本身另行检查。
- `CORE_DOWNLOAD_CONCURRENCY`：URL 下载与图片等下载共用并发名额。
- `CORE_DOWNLOAD_TIMEOUT`：URL 下载超时，包含等待下载名额和所有重定向；最多跟随 5 次重定向，逐次检查地址。下载遵循系统直连/代理配置，不转发调用方 Key、Cookie 或账号 AT。
- `CORE_TEXT_CONCURRENCY`：转写执行与聊天、搜索共享并发限制；满时返回 `429 text_concurrency_full`。
- `CORE_TEXT_TIMEOUT`：开始执行后的账号校验、音频上传、上游转写及切号总预算；URL 准备下载另受下载超时限制。
- `CORE_SWITCH_RETRIES`：沿用现有切号上限；上游限流、令牌失效等按现有错误分类决定是否切号。
- 当前只使用 Web 线路，遵循调用 Key 的账号分组限制。仅允许 Codex 线路的 Key 返回 `403 audio_route_not_allowed`。
- URL 下载失败不选择账号，不切号，也不扣账号分。
- 临时音频在请求结束时清理，进程异常遗留的音频目录在服务启动时回收。原始调用报文随调用日志保留，遵循现有日志清理策略。
- 调用日志新增“语音”类型，支持按语音类型、JSON/Text 返回格式筛选；上游错误保留原始响应和阶段耗时。

常见错误示例：

```json
{
  "error": {
    "code": "audio_input_download_failed",
    "message": "音频下载 HTTP 404"
  }
}
```

其他错误包括：`invalid_audio_url`（400）、`invalid_audio_file`（400）、`audio_too_large`（413）、`audio_input_download_timeout`（504）、`audio_timeout`（504）。以上为错误码和状态说明，错误对象的其他字段沿用项目统一格式。

## 当前能力边界

本次接通的是 STT 语音转文字。TTS `/v1/audio/speech` 仍为占位接口，返回 `501 audio_upstream_unavailable`；`/v1/audio/translations` 暂不支持，返回 `501 audio_translation_unavailable`，不会将原语言转写冒充翻译结果。
