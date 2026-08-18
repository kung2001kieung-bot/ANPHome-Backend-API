"""
MQTT Gateway (single file) — Tasmota / Zigbee2MQTT / myformat

รับ MQTT หลาย format -> normalize เป็น schema เดียว -> เก็บค่าล่าสุดใน Redis Hash
-> เปิด FastAPI ให้อ่านสถานะ / สั่งงานกลับ / รับ event สดผ่าน WebSocket
ไม่มีการเก็บประวัติ (no history)

รัน:
    pip install "fastapi>=0.110" "uvicorn[standard]>=0.27" "aiomqtt>=2.0" \
                "redis>=5.0" "pydantic>=2.5" "pydantic-settings>=2.1"
    python gateway.py            # หรือ  uvicorn gateway:app --port 8000

ตั้งค่าได้ผ่าน env (prefix GW_) หรือไฟล์ .env เช่น GW_MQTT_HOST, GW_REDIS_URL
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import aiomqtt
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gateway")


# --------------------------------------------------------------------------
# 1) Settings — อ่านจาก env (prefix GW_) หรือ .env
# --------------------------------------------------------------------------
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="GW_", extra="ignore")

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_topics: list[str] = ["tele/#", "stat/#", "zigbee2mqtt/#", "myformat/#"]

    redis_url: str = "redis://localhost:6379/0"
    redis_channel: str = "gateway:events"
    state_ttl: int | None = None  # วินาที; None = เก็บตลอด


settings = Settings()


# --------------------------------------------------------------------------
# 2) Schema กลาง — ทุก adapter แปลงมาเป็นรูปแบบนี้
# --------------------------------------------------------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)


class NormalizedEvent(BaseModel):
    source: str                                # tasmota | zigbee2mqtt | myformat
    device_id: str
    ts: datetime = Field(default_factory=_now)
    readings: dict[str, Any] = Field(default_factory=dict)
    available: bool | None = None
    raw_topic: str = ""

    @property
    def key(self) -> str:
        return f"{self.source}:{self.device_id}"


# --------------------------------------------------------------------------
# 3) Adapters — แต่ละตัวมี matches() + parse()
# --------------------------------------------------------------------------
class TasmotaAdapter:
    SOURCE = "tasmota"
    _SKIP = {"time", "tempunit", "pressureunit", "id"}

    @classmethod
    def _flatten(cls, payload: dict) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in payload.items():
            if k.lower() in cls._SKIP:
                continue
            if isinstance(v, dict):
                for sk, sv in v.items():
                    if sk.lower() in cls._SKIP or isinstance(sv, (dict, list)):
                        continue
                    out[sk.lower()] = sv
            elif not isinstance(v, list):
                out[k.lower()] = v
        return out

    @staticmethod
    def matches(topic: str) -> bool:
        parts = topic.split("/")
        return len(parts) >= 3 and parts[0] in {"tele", "stat"}

    @classmethod
    def parse(cls, topic: str, payload: bytes) -> NormalizedEvent | None:
        parts = topic.split("/")
        if len(parts) < 3:
            return None
        device_id, kind = parts[1], parts[2]
        text = payload.decode("utf-8", "ignore").strip()

        if kind == "LWT":
            return NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                                   available=text.lower() == "online", raw_topic=topic)
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                                   readings={kind.lower(): text}, raw_topic=topic)
        if not isinstance(data, dict):
            return None
        return NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                               readings=cls._flatten(data), raw_topic=topic)


class Zigbee2MQTTAdapter:
    SOURCE = "zigbee2mqtt"
    BASE = "zigbee2mqtt"

    @classmethod
    def matches(cls, topic: str) -> bool:
        return topic.startswith(cls.BASE + "/") and not topic.startswith(cls.BASE + "/bridge")

    @classmethod
    def parse(cls, topic: str, payload: bytes) -> NormalizedEvent | None:
        rest = topic[len(cls.BASE) + 1:]
        text = payload.decode("utf-8", "ignore").strip()

        if rest.endswith("/availability"):
            device_id = rest.rsplit("/", 1)[0]
            state = text
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    state = parsed.get("state", text)
            except json.JSONDecodeError:
                pass
            return NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                                   available=str(state).lower() == "online", raw_topic=topic)

        if "/" in rest:  # echo ของคำสั่ง .../set, .../get -> ข้าม
            return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        readings = {k.lower(): v for k, v in data.items() if not isinstance(v, (dict, list))}
        return NormalizedEvent(source=cls.SOURCE, device_id=rest,
                               readings=readings, raw_topic=topic)


class MyFormatAdapter:
    """
    myformat v1
      myformat/<device_id>/telemetry     {"ts":<epoch_ms?>,"data":{...}}
      myformat/<device_id>/status        {"ts":<epoch_ms?>,"data":{...}}
      myformat/<device_id>/availability  "online" | "offline"  (LWT + retained)
      myformat/<device_id>/cmd           {"command":"set","data":{...}}  (ขาออก)
    ts เป็น unix epoch มิลลิวินาที และ optional; ไม่ส่ง = ใช้เวลาที่รับ
    """
    SOURCE = "myformat"
    BASE = "myformat"

    @classmethod
    def matches(cls, topic: str) -> bool:
        return topic.startswith(cls.BASE + "/")

    @staticmethod
    def _ts(raw: Any) -> datetime | None:
        if not isinstance(raw, (int, float)):
            return None
        seconds = raw / 1000 if raw > 1e12 else raw
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    @classmethod
    def parse(cls, topic: str, payload: bytes) -> NormalizedEvent | None:
        parts = topic.split("/")
        if len(parts) < 3:
            return None
        device_id, msg_type = parts[1], parts[2]
        text = payload.decode("utf-8", "ignore").strip()

        if msg_type == "availability":
            return NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                                   available=text.lower() == "online", raw_topic=topic)
        if msg_type not in {"telemetry", "status"}:
            return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict) or not isinstance(data.get("data"), dict):
            return None
        readings = {k.lower(): v for k, v in data["data"].items()
                    if not isinstance(v, (dict, list))}
        ev = NormalizedEvent(source=cls.SOURCE, device_id=device_id,
                             readings=readings, raw_topic=topic)
        ts = cls._ts(data.get("ts"))
        if ts is not None:
            ev.ts = ts
        return ev


# ลำดับสำคัญ: ตัวแรกที่ matches() = True เป็นคนแปลง
_ADAPTERS = [TasmotaAdapter, Zigbee2MQTTAdapter, MyFormatAdapter]


def normalize(topic: str, payload: bytes) -> NormalizedEvent | None:
    for adapter in _ADAPTERS:
        if adapter.matches(topic):
            try:
                return adapter.parse(topic, payload)
            except Exception as e:
                log.warning("adapter %s failed on %s: %s", adapter.SOURCE, topic, e)
                return None
    return None


# --------------------------------------------------------------------------
# 4) Store — Redis Hash (ค่าล่าสุด) + pub/sub
# --------------------------------------------------------------------------
class Store:
    def __init__(self) -> None:
        self.r = redis.from_url(settings.redis_url, decode_responses=True)

    async def close(self) -> None:
        await self.r.aclose()

    @staticmethod
    def _key(ev_key: str) -> str:
        return f"state:{ev_key}"

    async def update(self, ev: NormalizedEvent) -> None:
        key = self._key(ev.key)
        mapping: dict[str, str] = {m: json.dumps(v) for m, v in ev.readings.items()}
        mapping["_source"] = ev.source
        mapping["_device_id"] = ev.device_id
        mapping["_ts"] = ev.ts.isoformat()
        if ev.available is not None:
            mapping["_available"] = json.dumps(ev.available)

        await self.r.hset(key, mapping=mapping)
        if settings.state_ttl:
            await self.r.expire(key, settings.state_ttl)
        await self.r.sadd("devices", ev.key)
        await self.r.publish(settings.redis_channel, ev.model_dump_json())

    async def list_devices(self) -> list[str]:
        return sorted(await self.r.smembers("devices"))

    async def get_state(self, ev_key: str) -> dict[str, Any] | None:
        raw = await self.r.hgetall(self._key(ev_key))
        if not raw:
            return None
        readings: dict[str, Any] = {}
        meta: dict[str, Any] = {}
        for k, v in raw.items():
            target, name = (meta, k[1:]) if k.startswith("_") else (readings, k)
            try:
                target[name] = json.loads(v)
            except json.JSONDecodeError:
                target[name] = v
        return {**meta, "readings": readings}


# --------------------------------------------------------------------------
# 5) MQTT — subscribe loop (reconnect เอง) + publisher
# --------------------------------------------------------------------------
class MQTTGateway:
    def __init__(self, store: Store) -> None:
        self.store = store
        self._client: aiomqtt.Client | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def publish(self, topic: str, payload: str) -> None:
        if self._client is None:
            raise RuntimeError("MQTT ยังไม่เชื่อมต่อ")
        await self._client.publish(topic, payload.encode())

    async def _run(self) -> None:
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=settings.mqtt_host, port=settings.mqtt_port,
                    username=settings.mqtt_username, password=settings.mqtt_password,
                ) as client:
                    self._client = client
                    for t in settings.mqtt_topics:
                        await client.subscribe(t)
                    log.info("subscribed: %s", settings.mqtt_topics)
                    async for message in client.messages:
                        ev = normalize(message.topic.value, message.payload)
                        if ev is not None:
                            await self.store.update(ev)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("mqtt loop error: %s — reconnect ใน 5 วินาที", e)
                self._client = None
                await asyncio.sleep(5)


# --------------------------------------------------------------------------
# 6) FastAPI — REST + WebSocket + publish
# --------------------------------------------------------------------------
class Command(BaseModel):
    topic: str          # เช่น cmnd/livingroom/POWER  หรือ  myformat/tank01/cmd
    payload: str = ""   # เช่น "ON" หรือ '{"command":"set","data":{"power":"on"}}'


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = Store()
    gw = MQTTGateway(store)
    await gw.start()
    app.state.store = store
    app.state.gw = gw
    yield
    await gw.stop()
    await store.close()


app = FastAPI(title="MQTT Gateway", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/devices")
async def devices():
    return await app.state.store.list_devices()


@app.get("/devices/{source}/{device_id}")
async def device_state(source: str, device_id: str):
    state = await app.state.store.get_state(f"{source}:{device_id}")
    if state is None:
        raise HTTPException(404, "ไม่พบอุปกรณ์")
    return state


@app.post("/publish")
async def publish(cmd: Command):
    try:
        await app.state.gw.publish(cmd.topic, cmd.payload)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    return {"published": True, "topic": cmd.topic}


@app.websocket("/ws/events")
async def ws_events(ws: WebSocket):
    await ws.accept()
    r = redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(settings.redis_channel)
    try:
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                await ws.send_text(msg["data"])
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(settings.redis_channel)
        await pubsub.aclose()
        await r.aclose()


# --------------------------------------------------------------------------
# 7) Entrypoint
# --------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("gateway:app", host="0.0.0.0", port=8000, reload=False)
