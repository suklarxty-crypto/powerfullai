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

# ==================== HARDCODED COOKIES ====================
HARDCODED_COOKIES = {
    "oai-did": "57496d26-4a2a-4bd6-904f-888cdeaf7a77",
    "oai-mweb-route-desktop": "1",
    "oai-hlib": "true",
    "oai-client-session-epoch": "0272ac00-bef2-4e38-b906-5fe31b992bd9",
    "_account": "0a4f2a28-ef02-40c6-ad0f-980ba33d6e8f",
    "oai-sc": "0gAAAAABqvlW5e5gYnmyaouWgIF2CfKQ3VzvjS7HubhTrUUF4Uqdhn-1UcRSTHOD7vYIzMdRwNEoR8CCs-rwawVMWgKKdu7_6yD360z57qePMshp8DFZ_jNJ2pdJNi4ZDHm2CU9xxp1wW5_SVeW6teKGgJjCFwU6g9jKa8VlaWU1S5HmyTyBG47EK9lukL9C9yVq1BPBkX9cQpl1EHsmDtvHabzZMWMs5TunnN0q3K5CHPSWfbs2_L38",
    "__Secure-next-auth.session-token.0": "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..G8GO3Bnh2gW4pTN8.525JGm4oYuLu3cx0YGkbsf4z1bJ2lzkRt90keBAsLOGLpH0FJ5IY-aoafr_bqJRngd0di4_sZyZpGEC-XrH2i7TnssJU7rnEPzgr633njCvq9F92PIMSAlwNh7MK5RgBwpOmwWZ_wqporYcXa5ywe_VGWCWFdM-fyoSxP3juCx03wl44QjzTNyBRxip19nqB0Ir9eKIob1N7K7vgtZPubRPzK-248ZqtSh40mHP1JCyOMVOpWKG6HKCUQijHJ8rsoRajDDiLp7354A7irDcQcbkvK9ca73TuwKGa9azf_D0eTC8NlFoQoK4ERsvpIYI00j3P6CSVMisuOA1DzHqvjL3Uy6X4QPORq9hVs3PVH8thHO5RXGoUVI2cU1OAxbQVmaTIzapOUGp2xZyMig1mEJN165kubynKsEXnK4wQMnhKHa6g8fxBlqomRBQhMbntd7qqfTY53ZGVat4gmNufK35KjBlir_0F41fACTqVUAkyS2PjQ63Gwx3pHKanNMKsXrUI_pOYkGFjBfrenHI3m3YuVPvCubOYtus_tkpn5rAcx2DN52U0HkZ7kr4CR7ehWR0Doe6kE8g5Dl1mVspijwVs2cDrc7hWhOurZaIRWD75j0jPgDSDVTJ-6DWD47pTyqeP2NbJk4lq-0f-7P2F_4x4eksHMxnNXm-6AOXv8vLcdU8EkJPG8Hfed8ykozmd0QFDShKmb01BcMfsp7ngTuknJ7ka6xdKGGuB_DS421gK5KdY7QlNHQzSVDVT0TPEpcgUUTGHCAh81sMT0zA0WsaTHrJJdQsPfqG85Mj5OP_lDWIvg2saCM_4_nvfpGCsLU7W3SqU0jiQrX3UAAx3_-okxAbBcCqx59c2K0kadYgrEXsY6fJmnICdLTDjU4fRGBBFX_sMTzKjeb92Y3Hq16CjQFNcalYPQ2L2CYF8a6JaOoVsLxR2FY32p-FWVdMB6annx0ygIkAQVEMmcrwYC-WCE_tnuSx0Cr_s7BP37DhTJP9UaJTfcNqozxLAU6vq4yAZWo-7f3d3nDpDmdWjNp1gA1u-OeqUKiK6Ov4EEAvzRXn1yIjqswmWJZ_m1jmVyi0QrfiA96Nf9rSqanF2s7BtaIVooeMma0iriN-ofmt-D3kYZeQJoRFGdyr1vUAnX2mWTq91vSG5UPstV76aL6zsyFetFRpsfXo6WmKgwGCK9Re1wg2DquIeGTJMakRzTKYlJ8KcHhJOlIP3yKmyaUWBwIOMm1B6JJYJYiAXEsVm3fsmUTs7nhf9y5OnytH3TMAWoYM3_pDizS8nra5z1Eq_7XOz-ArBo1uk0Eow1P1lhlva3mhQ3djele63qBQxn4KlD6juJ2toHFAW_KozZbFbsCHSM8iHVvMEgPkRKAzN4sc9dPLn3HLkbPug8GAzriWiyGNyfHBXr4CglbxqpuROQ-nhpi_XX18n5sAeoimyIUBP9OeMXQc2c8wwVM3BMH7zcEveXCnZpg-xD3L3xwQUplS0T-b2t1wFtHJZkmbyk6ozI6nYa_zlQzuy7K7v4lJoMeOjS4lCNpfj6U8dmJ88X2M64xLgCVf5Z34o3Kw-ymLiIm5Moie4UMfvDLuK5WOpaIzqHE9_v02ar6xR-XK8q8t0r8lEQSZm9szM8CFJ9kXLQGHrkx7jazrIdUjQ1IQEgdqi2SxX0tgzNkvPk0UZzbvSAPUxcH_zrjvXS2dnsrxeBbUIeBf40y_6LaO08FUuvGLyRissp7hgRurh9AW_b1IVm0w8XBtNtNz_Q0Og2BpdALyAmtNEZwYReyt3mPQORNEH01qlzLpHCZrQdGoAmzYLgLzeT6amcw63Fo5eMflShl5mfBMO70LdGEsbnG3VxfKUwhNBC7S1BZ7oKH9riXe5Fi3_6JAxhcIYiK0FaJTgG8UDqJSRFA7Ik1fRVCuRj80J6WuaKzDUaexT8oaqz2ztL4piBwEjssAmWjNPn5bICCafU_xHIbb6X3mYuaq-InciTBzbaoZqOGEtVqx5ZbGRw8ZCAMLYlOlO_JEw4_yRF1u3Mv-vuS4E7Su6Es_ja8TLXCogPZpzJ0FTcEpvHL1BZbTZoJs4uGrB6GeHdHU9R8GFCNfG-uBEFuqEskVQ3j4Fd4k9t6qbW44ROnRhxcDCGoYPAJa-Sh2w2VrrqI7_n4xqcAmnGSz0-2lRqN-yo2L_XeKKF89GZ0feQ2pPahPVoPf0PqJHavDZxulSvRLx95kiTUC8zCYt3b4NYRFV-yMahNZuA7owI7SaVBtxzP9XFFqsjt3dVKYJ2Nt1vxed0LZR6AQF-_hAzr5zRnH3VHJZRWuj-shP8ldDB0ujY7C1lg2XQz2PVz5PokfJYuBffKKcYG7HjCHr-20pGUeFQQiVmFkgMKvgPQB7pzGdTzrUoF-PAu7KCiqLxyiodbi_w-DlRPklbsRU3UUPKKOomzJXV79sRyj2DqnmuIMrLrI3EeYuBe1yq3YVgO9LML-cSOkmVb3WLhTIuXGMzdPhASrDq7gL_YsxkQY0WsDOxF2liuBy1v6aDktak_y0SUJ7p2wvuwYeQbPGZRsQjtwy0JzdahBsQbLr_WalN2ZbtGUemVVZiKgNz5HySs07A3wA1oOUJPRqt5NbYXbP14EXq-ZcnPoFwl5zv-MssYvck4a63Xdjn5Z-Zizuk7BbYDiCNI6Ip9kwV3Uq6bLAQTvsbwOrJaiwIV4ZMjRsYUPsIJKkoPAIrjqjwJq0bbul0EUJBHR2S17KbocXTAlFqW0Ap_YpAuz99lVPlSycDpenDf69f2fa7ehIZjB_5zcPOMJft1SQigvi_KLuVmgBTEhfDjilv0gkRBVZieFpe0kkRBw_UyvYyzpGPz_txslH2ZtR9sugpwDf80xvKObxS2P_Io6bslNLDTkNvX0YIgUJF3iR7nARPasfZNafeMcU3HQ0G7D5CJKkLSutZBmIeBFFMGJo7zHNTiY_p81MYM9rY6sF-Hn8RsScWyqVW5nu3OmV57kQ0IlZDw5TLP6rqM-2riIGahKzw5Wrlm7TCEW08ulwqMU2WGkPb4XbNAjdNct7E7g572629fY2UONMPz83LLWy53zecLdCx1vZSaXIPrRoDB3Q1QxviaAp_yvnD1Ewwmyd6t11-MeRsCHCZ7_yj6lcUMgyeixXod61wUSua8PExIP5cfzyWn_hKQR1U4Mow4f2GwZ2OFh4-jJCrC-vwUQ3X6rPqon1px3ng6o_B3XdAAIOcEwAOkQB6QF-dNP_BgsfGwQd17LMI5L_oOh8q4UfqcSVMtOArOg-LJhB0AmW4_BpU7A5j6od5I_6VT0jaj3H1obtLrwT6rY-IYOxkf4EOQi4RJvcVzjqa8-HbhYnVxU9tIw1BQB4D3kLMX3D86isQvWrNnjxi5zC_TMC9S1iC_J7ZpVnNum1L0ypz7GNr1jwXoV-0f7NapVXtnbEainENuBKWd2Ny4HI96vyKQdFwoDTwgYMSs4BuZ0j4gm-fl9UR32cu9sPpCePqyfbVRwZXAsnQg-USjgViMGZXLi6hcR5ma2gQPgXeiGLscP-9QTxghC6pdF6AWh6DbOPNtg5l028n9JI32mFz01Azwz8G_n-xXGgDxgr_ayf3F6psQbPnyi95jo0IBGofzHMgOQfPqwf4_UTS_dFgyhTNPsvsn4dsqsrZlHwOH83oZ4DhjzgvVpksAoX7vfRB8yNOR5WLmR_ApHycx_MMVI2zyXZ0DW45OS1L_vzIj5Q9EDCd8lT6KEAz1lP8mNvBcSD_SMrvVbQSYmnOHDyx2enmwTsCIPRvNkfI8OJXolm7ZMLPBvoId-_KUhT0jO_aNpi8mGlicrbhWPWHzwV_RfsdSq3Qf61cn7qF9JSJ_mv9_u4bvtpOhq",
    "__Secure-next-auth.session-token.1": "2Gee5LwnYA-QP9paJ-qCEBbmA3qevut0JYOIxV6brDPPAhlLTWIROaQM.eOgY3qVBvUuORQesJc6mdg",
    "cf_clearance": "bcg3aaz8Sgbx2tQrlbpDYagpLATp_RA01_lXe_Z.I7s-1790857271-1.2.1.1-uRLBGZXmOBBGN5q3.o5xI4s51vFN1UjFC3eAYzZPjEw8shfAtmXdj4LGUTlNljXhmNVkDWLWJeM4N8tP8pWqDhn6X4IAfcAt5UENom.yfiqRt4hzX36Ew3foW4n0nJDayWjibgaRumnGxPITa8xK4dghJvSO..kLBYTqImwQ4lDTftrlccd23GN6aNgS.Wyr6VF.675g3V.Qho1X1X2tXi7DvJNDRLbp7x_RRL_1q4qvi_zRCEzBUFIYk8a8CoE.JCxRmFhhppNqHNkI1epaE12_UiCfdprx0rcv9hyTXTM7T68upuYvqf_VsZcEveVgiSbSg_Iqp0hW91tJdX9MB0MtteHaH8f0F9i4kVotCIg",
    "_dd_s_v2": "aid=f57c82a6-10db-4c27-8321-e391df5d5c37&id=26aea063-46ee-46fb-b195-12cd0d01f75b&created=1790857265683&expire=1790859602184&c=0",
    "__cf_bm": "cZKQLSJlfaKwvnrU_nQQ5T2ocLx8AILUK_Hls_IQLS0-1790859135.5820725-1.0.1.1-plNq3oTKBls8d_.MqHZJedisPylY3XIO72JLL3p38hP8Iox60RJSJezP2BlsugLtOvvLOjG0XzUEo3lckErkaw35IefPYk9etNa602NbO91w_BVpHnBuQ5a7uPWtjIss",
    "__Secure-next-auth.callback-url": "https%3A%2F%2Fchatgpt.com%2F",
    "__Host-next-auth.csrf-token": "64e8ff4cd0920cb5324e91bb95c7ae39836d541051f45dee2ffe360fb1dfeb97%7Ce3be8299677dedfa555f24451eaeefbdd92d707be7aec1d559fc8aa312314e86",
    "__cflb": "0H28vzvP5FJafnkHxjEtGkoEyvgGZvmfxuYw3j8GmZd",
    "_cfuvid": "y71bJy3AeZCNed0yaFs4EJFdBI7U1Uij.jhKFAwJBLI-1790857256.8821173-1.0.1.1-_D4nbu1ayYuPOxZDwOcUkdl4G500H1T6W8CYYqvjHcU",
    "oai_pl_permission_state": "disabled",
    "_rdt_uuid": "1790513080591.e3b3e04a-f1ec-4e28-99a5-84bb0d24fe8c",
    "codex_sidebar_width": "340"
}

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
        self.device_id = None
        self.conduit_token = ""
        self.chat_req_token = ""
        self.play_integrity_token = ""
        self.convo_session_id = None
        self.turn_trace_id = None
        self.sentry_trace = ""
        self.baggage = ""
        self.cookie_status = {}

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
        self.device_id = str(uuid.uuid4())
        self._init_session()

    def _load_cookies(self):
        """Load hardcoded cookies"""
        self.cookie_status = {
            "total": 0,
            "has_session_token": False,
            "has_cf_clearance": False,
            "has_oai_sc": False,
            "has_oai_did": False,
            "missing_critical": [],
            "source": "hardcoded"
        }
        
        cookies = HARDCODED_COOKIES
        
        self.cookie_status["total"] = len(cookies)
        
        # Critical check
        critical = {
            "__Secure-next-auth.session-token.0": "has_session_token",
            "cf_clearance": "has_cf_clearance",
            "oai-sc": "has_oai_sc",
            "oai-did": "has_oai_did"
        }
        
        for cname, cflag in critical.items():
            if cname in cookies:
                self.cookie_status[cflag] = True
            else:
                self.cookie_status["missing_critical"].append(cname)
        
        # Load into session
        for k, v in cookies.items():
            try:
                self.session.cookies.set(k, v, domain=".chatgpt.com")
                self.session.cookies.set(k, v, domain="chatgpt.com")
            except Exception:
                pass
        
        print(f"[cookies] Loaded {len(cookies)} hardcoded cookies")

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
        "endpoints": {
            "ai": "/api/ai?key=FFG&prompt=hi",
            "health": "/api/health",
            "session": "/api/session"
        }
    })


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/api/session", methods=["GET"])
def session_info():
    """Show cookie status"""
    gpt = ChatGPT()
    return jsonify({
        "status": "ok",
        "cookie_status": gpt.cookie_status,
        "conduit_token": "yes" if gpt.conduit_token else "no",
        "chat_req_token": "yes" if gpt.chat_req_token else "no",
        "endpoint": "/api/ai?key=FFG&prompt=hi"
    })


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
            if "401" in str(error) or "Unauthorized" in str(error):
                return jsonify({
                    "status": "error",
                    "message": "Cookies expired. Please refresh cookies in code.",
                    "cookie_status": gpt.cookie_status,
                    "reply": None
                }), 401
            
            return jsonify({
                "status": "error",
                "message": error,
                "cookie_status": gpt.cookie_status,
                "reply": None
            }), 500
        
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
