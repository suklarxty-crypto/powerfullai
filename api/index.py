from flask import Flask, request, jsonify
import json
import os
import uuid
import time
from datetime import datetime
import requests

app = Flask(__name__)

# ==================== CONFIG ====================
API_KEY = "FFG"

# Cookies file - API folder ke bahar root me
COOKIES_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cookies.json")

# ==================== PAYLOAD DEFAULT ====================
PAYLOAD_CONFIG = {
    "model": "auto",
    "history_and_training_disabled": False,
    "enable_message_followups": True,
    "force_use_sse": True,
    "force_use_search": None,
    "force_paragen": False,
    "supports_buffering": False,
    "timezone": "Africa/Cairo",
    "timezone_offset_min": -180,
    "system_hints": [],
    "is_onboarding_conversation": False,
    "no_auth_ad_preferences": {"personalization_enabled": False, "history_enabled": True},
    "client_prepare_dispatch": "debounced",
    "client_prepare_source": "composer_editor_state",
    "client_prepare_state": "success"
}


# ==================== CHATGPT CLASS ====================
class ChatGPT:
    def __init__(self):
        self.session = requests.Session()
        self.payload_config = PAYLOAD_CONFIG.copy()
        self.device_id = str(uuid.uuid4())
        self.conduit_token = ""
        self.chat_req_token = ""
        self.play_integrity_token = ""
        self.convo_session_id = None
        self.turn_trace_id = None
        self.sentry_trace = ""
        self.baggage = ""

        self.base_url = "https://chatgpt.com"
        self.prepare_path = "/backend-api/f/conversation/prepare"
        self.sentinel_path = "/backend-api/sentinel/chat-requirements"
        self.conversation_path = "/backend-api/f/conversation"
        self.user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/131.0.0.0 Safari/537.36")
        self.accept_language = "en-US,en;q=0.9"
        self.timezone = "Africa/Cairo"
        self.timezone_offset = -180

        self._load_cookies()
        self._init_session()

    def _load_cookies(self):
        if os.path.exists(COOKIES_FILE):
            try:
                with open(COOKIES_FILE, "r", encoding="utf-8") as f:
                    cookies = json.load(f)
                for k, v in cookies.items():
                    self.session.cookies.set(k, v, domain=".chatgpt.com")
                    self.session.cookies.set(k, v, domain="chatgpt.com")
            except Exception as e:
                print(f"[cookies] {e}")

    def _generate_sentry(self):
        tid = uuid.uuid4().hex
        self.sentry_trace = f"{tid[:16]}-{tid[16:32]}"
        self.baggage = (
            f"sentry-environment=production,sentry-org_id=33249,"
            f"sentry-public_key=6884768431e4ba548d58cbf3ad96e4ce,"
            f"sentry-release=com.openai.chatgpt%401.2026.195%2B2619512,"
            f"sentry-sample_rand=0.{int(time.time()*1000)%1000000},"
            f"sentry-trace_id={tid[:16]}"
        )

    def _common_headers(self):
        self._generate_sentry()
        return {
            "user-agent": self.user_agent,
            "accept-language": self.accept_language,
            "accept": "application/json",
            "sentry-trace": self.sentry_trace,
            "baggage": self.baggage,
            "origin": "https://chatgpt.com",
            "referer": "https://chatgpt.com/",
            "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "accept-encoding": "gzip, deflate, br"
        }

    def _init_session(self):
        self.convo_session_id = str(uuid.uuid4())
        self.turn_trace_id = str(uuid.uuid4())

        # Prepare
        url = f"{self.base_url}{self.prepare_path}"
        headers = {
            **self._common_headers(),
            "x-oai-convo-session-id": self.convo_session_id,
            "x-oai-turn-trace-id": self.turn_trace_id,
            "x-conduit-token": self.conduit_token or "",
            "x-openai-target-path": self.prepare_path,
            "content-type": "application/json"
        }
        prepare_body = {
            "action": "next", "messages": [],
            "model": self.payload_config["model"],
            "history_and_training_disabled": self.payload_config["history_and_training_disabled"],
            "fork_from_shared_post": False,
            "enable_message_followups": False,
            "force_use_sse": False,
            "force_use_search": None,
            "force_paragen": False,
            "supports_buffering": False,
            "timezone": self.timezone,
            "timezone_offset_min": self.timezone_offset,
            "system_hints": self.payload_config["system_hints"],
            "is_onboarding_conversation": self.payload_config["is_onboarding_conversation"],
            "no_auth_ad_preferences": self.payload_config["no_auth_ad_preferences"],
            "client_prepare_dispatch": self.payload_config["client_prepare_dispatch"],
            "client_prepare_source": self.payload_config["client_prepare_source"]
        }
        try:
            r = self.session.post(url, headers=headers, json=prepare_body, timeout=25)
            if r.ok:
                try:
                    j = r.json()
                    if "conduit_token" in j:
                        self.conduit_token = j["conduit_token"]
                except Exception:
                    pass
        except Exception as e:
            print(f"[prepare] {e}")

        # Sentinel
        url2 = f"{self.base_url}{self.sentinel_path}"
        headers2 = {
            **self._common_headers(),
            "x-openai-target-path": self.sentinel_path,
            "content-type": "application/json"
        }
        try:
            r = self.session.post(url2, headers=headers2, json={}, timeout=25)
            if r.ok:
                try:
                    j = r.json()
                    if "token" in j:
                        self.chat_req_token = j["token"]
                except Exception:
                    pass
        except Exception as e:
            print(f"[sentinel] {e}")

    def send_message(self, text, conversation_id=None, parent_id=None, retry=True):
        url = f"{self.base_url}{self.conversation_path}"
        sentinel = {
            "bot_token": {
                "play_integrity_token": self.play_integrity_token or "",
                "chat_requirement_token": self.chat_req_token or ""
            }
        }
        headers = {
            **self._common_headers(),
            "accept": "text/event-stream,application/json",
            "cache-control": "no-cache",
            "x-sentinel-payload": json.dumps(sentinel),
            "x-conduit-token": self.conduit_token or "",
            "x-oai-convo-session-id": self.convo_session_id,
            "x-oai-turn-trace-id": str(uuid.uuid4()),
            "oai-echo-logs": "1,552,0,822,1,3296,1,5355,0,5533,1,8297,0,8739,1,9818,0,11081,1,12543",
            "x-openai-target-path": self.conversation_path,
            "content-type": "application/json"
        }

        msg_id = str(uuid.uuid4())
        body = {
            "action": "next",
            "messages": [{
                "id": msg_id,
                "author": {"role": "user"},
                "content": {"parts": [text], "content_type": "text"},
                "status": "finished_successfully",
                "recipient": "all",
                "metadata": {
                    "model_slug": self.payload_config["model"],
                    "default_model_slug": "auto"
                }
            }],
            "model": self.payload_config["model"],
            "history_and_training_disabled": self.payload_config["history_and_training_disabled"],
            "enable_message_followups": self.payload_config["enable_message_followups"],
            "force_use_sse": self.payload_config["force_use_sse"],
            "force_use_search": self.payload_config["force_use_search"],
            "force_paragen": self.payload_config["force_paragen"],
            "supports_buffering": self.payload_config["supports_buffering"],
            "timezone": self.timezone,
            "timezone_offset_min": self.timezone_offset,
            "system_hints": self.payload_config["system_hints"],
            "is_onboarding_conversation": self.payload_config["is_onboarding_conversation"],
            "no_auth_ad_preferences": self.payload_config["no_auth_ad_preferences"],
            "client_prepare_state": self.payload_config["client_prepare_state"],
            "stream": True
        }
        if conversation_id:
            body["conversation_id"] = conversation_id
        if parent_id:
            body["parent_message_id"] = parent_id

        try:
            r = self.session.post(url, headers=headers, json=body,
                                  stream=True, timeout=90)
            if r.status_code in (401, 403, 422, 500) and retry:
                self._init_session()
                return self.send_message(text, conversation_id, parent_id, False)
            if not r.ok:
                return None, None, None, None, f"HTTP {r.status_code}: {r.text[:400]}"
        except Exception as e:
            return None, None, None, None, f"Exception: {e}"

        if "x-conduit-token" in r.headers:
            self.conduit_token = r.headers["x-conduit-token"]

        full_text = ""
        new_conv = conversation_id
        new_parent = parent_id
        model_used = self.payload_config["model"]

        try:
            for line in r.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                data = line[6:]
                if data == "[DONE]":
                    break
                try:
                    ev = json.loads(data)
                except Exception:
                    continue
                if ev.get("type") == "resume_conversation_token":
                    new_conv = ev.get("conversation_id", new_conv)
                if "message" in ev:
                    m = ev["message"]
                    if m.get("author", {}).get("role") == "assistant" and \
                            m.get("channel") == "final":
                        new_parent = m.get("id", new_parent)
                        if "metadata" in m and "model_slug" in m["metadata"]:
                            model_used = m["metadata"]["model_slug"]
                        parts = m.get("content", {}).get("parts", [])
                        if parts:
                            cur = "".join([p for p in parts if isinstance(p, str)])
                            if cur != full_text:
                                full_text = cur
        except Exception as e:
            return full_text or None, new_conv, new_parent, model_used, f"Stream: {e}"

        return full_text, new_conv, new_parent, model_used, None


# ==================== ROUTES ====================

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "message": "ChatGPT API running",
        "endpoint": "/api/ai?key=FFG&prompt=hi"
    })


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/api/ai", methods=["GET"])
def ai_endpoint():
    api_key = request.args.get("key", "").strip()
    prompt = request.args.get("prompt", "").strip()
    conversation_id = request.args.get("conversation_id", None)
    parent_id = request.args.get("parent_id", None)

    if not api_key:
        return jsonify({"status": "error", "message": "Missing key", "reply": None}), 401
    if api_key != API_KEY:
        return jsonify({"status": "error", "message": "Invalid key", "reply": None}), 403
    if not prompt:
        return jsonify({"status": "error", "message": "Missing prompt", "reply": None}), 400

    try:
        gpt = ChatGPT()
        reply, new_cid, new_pid, model, error = gpt.send_message(
            prompt, conversation_id, parent_id
        )

        if error:
            return jsonify({"status": "error", "message": error, "reply": None}), 500
        if not reply:
            return jsonify({"status": "error", "message": "No response", "reply": None}), 500

        return jsonify({
            "status": "success",
            "reply": reply,
            "conversation_id": new_cid,
            "parent_id": new_pid,
            "model": model
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "reply": None}), 500
