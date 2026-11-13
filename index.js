/**
 * Xiaozhi Zing MP3 MCP Server - Cloud 24/7 Edition
 * Native Zing MP3 HMAC-SHA512 direct resolver + Model Context Protocol
 * Tương thích 100% với Chatbox Xiaozhi ESP32, Render.com, UptimeRobot.
 */

const express = require("express");
const fetch   = require("node-fetch");
const cors    = require("cors");
const path    = require("path");
const fs      = require("fs");
const crypto  = require("crypto");
const WebSocket = require("ws");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const ZING_VERSION = "1.6.34";
const ZING_SECRET_KEY = "2aa2d1c561e809b267f3638c4a307aab";
const ZING_API_KEY = "88265e23d4284f25963e6eedac8fbfa3";
const RESOURCE_URI = "resource://read_from_http";
const STREAM_DONE_FLAG = "[DONE]";

// ─── Lưu thiết bị đang kết nối ────────────────────────────────────────────────
const connectedDevices = new Map();

// ─── Persistent device storage ─────────────────────────────────────────────────
const DEVICES_FILE = path.join(__dirname, "devices.json");

function loadSavedDevices() {
  try {
    if (fs.existsSync(DEVICES_FILE)) {
      const data = fs.readFileSync(DEVICES_FILE, "utf-8");
      const parsed = JSON.parse(data);
      if (Array.isArray(parsed)) return parsed;
    }
  } catch (e) {
    console.error(`[storage] Lỗi đọc devices.json: ${e.message}`);
  }
  return [];
}

function saveDevicesToFile(devices) {
  try {
    fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), "utf-8");
    console.log(`[storage] 💾 Đã lưu ${devices.length} thiết bị vào devices.json`);
  } catch (e) {
    console.error(`[storage] Lỗi ghi devices.json: ${e.message}`);
  }
}

function addSavedDevice(deviceName, wssUrl) {
  const devices = loadSavedDevices();
  const idx = devices.findIndex(d => d.deviceName === deviceName);
  const entry = {
    deviceName,
    wssUrl,
    savedAt: new Date().toISOString(),
    autoConnect: true
  };
  if (idx >= 0) {
    devices[idx] = { ...devices[idx], ...entry };
  } else {
    devices.push(entry);
  }
  saveDevicesToFile(devices);
}

function removeSavedDevice(deviceName) {
  const devices = loadSavedDevices();
  const filtered = devices.filter(d => d.deviceName !== deviceName);
  saveDevicesToFile(filtered);
  return filtered.length < devices.length;
}

// ─── Zing MP3 Native Resolver ─────────────────────────────────────────────────
let zingCookie = "";
let zingCookieTime = 0;

async function ensureZingCookie() {
  if (!zingCookie || (Date.now() - zingCookieTime > 1800000)) {
    try {
      const resp = await fetch("https://zingmp3.vn", {
        headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
        timeout: 6000
      });
      const rawCookies = resp.headers.raw() ? resp.headers.raw()["set-cookie"] : null;
      if (rawCookies && rawCookies.length > 0) {
        zingCookie = rawCookies.map(c => c.split(";")[0]).join("; ");
        zingCookieTime = Date.now();
      }
    } catch (e) {
      console.warn("[Zing] Cookie warning:", e.message);
    }
  }
}

async function searchZingSong(query) {
  try {
    const url = `https://ac.zingmp3.vn/v1/web/ac-suggestions?query=${encodeURIComponent(query)}`;
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
      timeout: 6000
    });
    const data = await resp.json();
    const items = data?.data?.items || [];
    for (const item of items) {
      const songs = item?.suggestions || [];
      if (songs.length > 0) {
        return {
          id: songs[0].id,
          title: songs[0].title,
          artist: songs[0].artistsNames || ""
        };
      }
    }
  } catch (e) {
    console.error("[Zing] Search error:", e.message);
  }
  return null;
}

async function getZingStreamUrl(songId) {
  await ensureZingCookie();
  const ctime = Math.floor(Date.now() / 1000).toString();
  const path = "/api/v2/song/get/streaming";
  const msg = `ctime=${ctime}id=${songId}version=${ZING_VERSION}`;
  const hash256 = crypto.createHash("sha256").update(msg, "utf8").digest("hex");
  const sig = crypto.createHmac("sha512", ZING_SECRET_KEY).update(path + hash256, "utf8").digest("hex");
  const url = `https://zingmp3.vn${path}?id=${songId}&ctime=${ctime}&version=${ZING_VERSION}&apiKey=${ZING_API_KEY}&sig=${sig}`;

  try {
    const resp = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Cookie": zingCookie
      },
      timeout: 8000
    });
    const data = await resp.json();
    if (data?.err === 0 && data?.data) {
      const audioUrl = data.data["128"] || data.data["320"];
      if (audioUrl) {
        try {
          const redResp = await fetch(audioUrl, {
            headers: { "User-Agent": "Mozilla/5.0", "Range": "bytes=0-10" },
            redirect: "follow",
            timeout: 5000
          });
          return redResp.url || audioUrl;
        } catch (_) {
          return audioUrl;
        }
      }
    }
  } catch (e) {
    console.error("[Zing] Stream URL error:", e.message);
  }
  return "";
}

async function readHttpChunk(url, start = 0, end = 0) {
  if (end <= start) end = start + 100 * 1024;
  try {
    const resp = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Range": `bytes=${start}-${end - 1}`,
        "Accept": "*/*"
      },
      timeout: 15000
    });
    if (resp.status === 416) {
      return Buffer.from(STREAM_DONE_FLAG).toString("base64");
    }
    const buf = await resp.buffer();
    if (!buf || buf.length === 0) {
      return Buffer.from(STREAM_DONE_FLAG).toString("base64");
    }
    return buf.toString("base64");
  } catch (e) {
    return Buffer.from(STREAM_DONE_FLAG).toString("base64");
  }
}

// ─── MCP Definitions ──────────────────────────────────────────────────────────
const TOOLS_LIST = [
  {
    name: "play_song",
    description: "Phát bài hát Việt Nam hoặc quốc tế được yêu cầu ra loa thiết bị. BẮT BUỘC dùng công cụ này khi người dùng muốn nghe nhạc hoặc mở bài hát.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Tên bài hát hoặc tên ca sĩ cần phát (ví dụ: 'Một con vịt', 'Cắt Đôi Nỗi Sầu', 'Lạc Trôi Sơn Tùng')"
        }
      },
      required: ["query"]
    }
  },
  {
    name: "search_song",
    description: "Tìm kiếm bài hát hoặc danh sách ca khúc Việt Nam",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Từ khóa bài hát hoặc ca sĩ cần tìm"
        }
      },
      required: ["query"]
    }
  },
  {
    name: "play_music",
    description: "Phát bài hát Việt Nam hoặc quốc tế ra loa thiết bị",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Tên bài hát hoặc tên ca sĩ cần phát" }
      },
      required: ["query"]
    }
  },
  {
    name: "search_music",
    description: "Tìm kiếm bài hát hoặc ca khúc Việt Nam",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Từ khóa bài hát cần tìm" }
      },
      required: ["query"]
    }
  }
];

const RESOURCES_LIST = [
  {
    uri: RESOURCE_URI,
    name: "HTTP Audio Stream",
    description: "Hỗ trợ stream âm thanh trực tiếp từ URL qua HTTP Range chunking cho Xiaozhi",
    mimeType: "application/octet-stream"
  }
];

async function handleToolCall(toolName, args) {
  const query = args?.query || args?.song_name || args?.song || args?.title || "";
  console.log(`[Tool] Gọi '${toolName}' với từ khóa: '${query}'`);

  if (toolName === "play_song" || toolName === "play_music" || toolName === "self.music.play_song") {
    if (!query) {
      return { content: [{ type: "text", text: "Vui lòng cho tôi biết tên bài hát bạn muốn nghe." }] };
    }
    const song = await searchZingSong(query);
    if (!song) {
      return { content: [{ type: "text", text: `Không tìm thấy bài hát '${query}' trên Zing MP3.` }] };
    }
    const displayName = song.artist ? `${song.title} - ${song.artist}` : song.title;
    console.log(`[Zing] Tìm thấy: ${displayName} (ID: ${song.id}). Đang lấy link stream...`);
    
    const streamUrl = await getZingStreamUrl(song.id);
    if (!streamUrl) {
      return { content: [{ type: "text", text: `Bài hát '${displayName}' hiện không thể phát do bản quyền.` }] };
    }
    console.log(`[Zing] Link stream: ${streamUrl.substring(0, 60)}...`);

    return {
      content: [
        {
          type: "text",
          text: `Đã tìm thấy bài hát '${displayName}'. Link stream: ${streamUrl}\nBẮT BUỘC hãy gọi ngay công cụ thiết bị self.music.play_song với tham số url='${streamUrl}' và song_name='${displayName}' để phát ra loa.`
        },
        {
          type: "resource_link",
          uri: RESOURCE_URI,
          name: displayName,
          description: streamUrl,
          mimeType: "audio/mpeg"
        }
      ]
    };
  } else if (toolName === "search_song" || toolName === "search_music") {
    const song = await searchZingSong(query);
    if (song) {
      return { content: [{ type: "text", text: `Tìm thấy bài hát: ${song.title} - ${song.artist}` }] };
    }
    return { content: [{ type: "text", text: `Không tìm thấy bài hát '${query}'.` }] };
  }

  return { content: [{ type: "text", text: `Công cụ ${toolName} không tồn tại.` }], isError: true };
}

// ─── WebSocket Client tới Xiaozhi ─────────────────────────────────────────────
async function handleXiaozhiMessage(ws, raw, deviceName) {
  let msg;
  try {
    msg = JSON.parse(raw.toString());
  } catch {
    return;
  }

  const { id, method, params } = msg;
  if (!method || method.startsWith("notifications/")) return;

  console.log(`[${deviceName}] ← ${method}`);

  let result = null;
  let error  = null;

  try {
    if (method === "initialize") {
      result = {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {}, resources: {} },
        serverInfo: { name: "xiaozhi-vn-music-streamer", version: "3.5.0" }
      };
    } else if (method === "tools/list") {
      result = { tools: TOOLS_LIST };
    } else if (method === "resources/list") {
      result = { resources: RESOURCES_LIST };
    } else if (method === "resources/read") {
      const args = params?.arguments || {};
      const url = args.url || "";
      const start = parseInt(args.start || 0, 10);
      const end = parseInt(args.end || 0, 10);
      const blob = await readHttpChunk(url, start, end);
      result = {
        contents: [
          {
            uri: RESOURCE_URI,
            mimeType: "application/octet-stream",
            blob: blob
          }
        ]
      };
    } else if (method === "tools/call") {
      const toolName = params?.name;
      const args = params?.arguments || {};
      result = await handleToolCall(toolName, args);
    } else if (method === "ping") {
      result = {};
    } else {
      error = { code: -32601, message: `Method not found: ${method}` };
    }
  } catch (e) {
    console.error(`[${deviceName}] Lỗi xử lý:`, e.message);
    error = { code: -32603, message: e.message };
  }

  if (id !== undefined && id !== null) {
    const response = { jsonrpc: "2.0", id };
    if (error) response.error = error;
    else response.result = result;
    ws.send(JSON.stringify(response));
    console.log(`[${deviceName}] → ${method} OK`);
  }
}

function connectToXiaozhi(deviceName, wssUrl) {
  const existing = connectedDevices.get(deviceName);
  if (existing?.ws && existing.ws.readyState === WebSocket.OPEN) {
    try { existing.ws.close(); } catch (_) {}
  }
  if (existing?.reconnectTimer) {
    clearTimeout(existing.reconnectTimer);
  }

  console.log(`[WSS] Đang kết nối tới Xiaozhi (${deviceName}): ${wssUrl.substring(0, 35)}...`);
  const ws = new WebSocket(wssUrl);
  connectedDevices.set(deviceName, {
    ws,
    url: wssUrl,
    status: "connecting",
    connectedAt: null,
    reconnectTimer: null
  });

  ws.on("open", () => {
    const dev = connectedDevices.get(deviceName);
    if (dev) {
      dev.status = "connected";
      dev.connectedAt = new Date().toISOString();
    }
    console.log(`[WSS] >>> KẾT NỐI THÀNH CÔNG (${deviceName})! Trạng thái: TRỰC TUYẾN.`);
  });

  ws.on("message", (data) => handleXiaozhiMessage(ws, data, deviceName));

  ws.on("close", (code, reason) => {
    const dev = connectedDevices.get(deviceName);
    if (dev) dev.status = "disconnected";
    console.warn(`[WSS] Ngắt kết nối (${deviceName}) code=${code}. Thử lại sau 5s...`);
    const timer = setTimeout(() => {
      if (connectedDevices.has(deviceName)) {
        connectToXiaozhi(deviceName, wssUrl);
      }
    }, 5000);
    if (dev) dev.reconnectTimer = timer;
  });

  ws.on("error", (err) => {
    console.error(`[WSS] Lỗi kết nối (${deviceName}):`, err.message);
  });

  return ws;
}

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get("/api/devices", (req, res) => {
  const list = [];
  for (const [name, d] of connectedDevices.entries()) {
    list.push({
      name,
      status: d.status,
      connectedAt: d.connectedAt,
      url: d.url ? d.url.replace(/token=.*$/, "token=***") : ""
    });
  }
  res.json({ devices: list });
});

app.get("/api/saved-devices", (req, res) => {
  const saved = loadSavedDevices();
  const result = saved.map(d => {
    const conn = connectedDevices.get(d.deviceName);
    return {
      ...d,
      wssUrl: d.wssUrl ? d.wssUrl.replace(/token=.*$/, "token=***") : "",
      isConnected: conn?.status === "connected"
    };
  });
  res.json({ savedDevices: result, total: result.length });
});

app.post("/api/connect", (req, res) => {
  const { deviceName, wssUrl } = req.body;
  if (!deviceName || !wssUrl) {
    return res.status(400).json({ error: "Thiếu deviceName hoặc wssUrl" });
  }
  addSavedDevice(deviceName, wssUrl);
  connectToXiaozhi(deviceName, wssUrl);
  res.json({ success: true, message: `Đã bắt đầu kết nối "${deviceName}"`, saved: true });
});

app.post("/api/disconnect", (req, res) => {
  const { deviceName, removeFromSaved } = req.body;
  const dev = connectedDevices.get(deviceName);
  if (dev?.reconnectTimer) clearTimeout(dev.reconnectTimer);
  if (dev?.ws) try { dev.ws.close(); } catch (_) {}
  connectedDevices.delete(deviceName);
  if (removeFromSaved) removeSavedDevice(deviceName);
  res.json({ success: true, message: `Đã ngắt kết nối "${deviceName}"` });
});

app.post("/api/remove-saved-device", (req, res) => {
  const { deviceName } = req.body;
  if (!deviceName) return res.status(400).json({ error: "Thiếu deviceName" });
  removeSavedDevice(deviceName);
  const dev = connectedDevices.get(deviceName);
  if (dev?.reconnectTimer) clearTimeout(dev.reconnectTimer);
  if (dev?.ws) try { dev.ws.close(); } catch (_) {}
  connectedDevices.delete(deviceName);
  res.json({ success: true, message: `Đã xóa "${deviceName}" khỏi danh sách lưu` });
});

// REST API Search endpoint
app.get("/search", async (req, res) => {
  const q = req.query.q || "";
  if (!q) return res.json({ error: "Thiếu ?q=" });
  const song = await searchZingSong(q);
  res.json({ success: !!song, song });
});

// ─── Khởi động Server ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log("========================================");
  console.log(" 🎵 Xiaozhi Zing MP3 MCP Server v3.5 (Cloud 24/7)");
  console.log(` 🌐 Server chạy trên cổng: ${PORT}`);
  console.log("========================================");

  const saved = loadSavedDevices();
  if (saved.length > 0) {
    for (const dev of saved) {
      if (dev.autoConnect !== false && dev.wssUrl) {
        connectToXiaozhi(dev.deviceName, dev.wssUrl);
      }
    }
  }
});
