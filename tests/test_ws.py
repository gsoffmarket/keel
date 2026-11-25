import asyncio
import json

import pytest

from fakes import FakeWS
from keel.hl.ws import OP_TEXT, ConnectionClosed, HLStream, WebSocket, accept_key, encode_frame, read_frame


def test_accept_key_rfc_example():
    # RFC 6455 section 1.3 example
    assert accept_key("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="


@pytest.mark.parametrize("n", [0, 5, 125, 126, 65535, 65536, 200_000])
def test_frame_roundtrip_all_length_encodings(n):
    payload = bytes(range(256)) * (n // 256) + bytes(range(n % 256))

    async def go():
        r = asyncio.StreamReader()
        r.feed_data(encode_frame(OP_TEXT, payload, mask=True))
        r.feed_eof()
        return await read_frame(r, 1 << 24)

    fin, op, got = asyncio.run(go())
    assert fin and op == OP_TEXT and got == payload


def test_fragmented_message_and_ping_reply():
    async def go():
        from keel.hl.ws import OP_CONT, OP_PING

        r = asyncio.StreamReader()
        r.feed_data(encode_frame(OP_TEXT, b'{"a":', mask=False, fin=False))
        r.feed_data(encode_frame(OP_PING, b"hb", mask=False))
        r.feed_data(encode_frame(OP_CONT, b"1}", mask=False, fin=True))
        sent = []

        class W:
            def write(self, b):
                sent.append(b)

            async def drain(self):
                pass

        ws = WebSocket(r, W())  # type: ignore[arg-type]
        return await ws.recv(), sent

    msg, sent = asyncio.run(go())
    assert msg == '{"a":1}'
    assert sent and sent[0][0] == 0x8A  # pong answered while reassembling


def test_oversize_frame_closes():
    async def go():
        r = asyncio.StreamReader()
        r.feed_data(encode_frame(OP_TEXT, b"x" * 2000, mask=False))
        return await read_frame(r, 1000)

    with pytest.raises(ConnectionClosed):
        asyncio.run(go())


def test_connect_ping_and_subscribe_against_fake():
    async def go():
        srv = await FakeWS().start()
        ws = await WebSocket.connect(srv.url)
        await ws.send_text('{"method":"ping"}')
        pong = json.loads(await ws.recv())
        await ws.send_text(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": "#436390"}}))
        ack = json.loads(await ws.recv())
        book = json.loads(await ws.recv())
        await ws.close()
        await srv.stop()
        return pong, ack, book

    pong, ack, book = asyncio.run(go())
    assert pong == {"channel": "pong"}
    assert ack["channel"] == "subscriptionResponse"
    assert book["channel"] == "l2Book" and book["data"]["coin"] == "#436390"


def test_stream_reconnects_and_resubscribes():
    async def go():
        srv = await FakeWS(tick=0.02, drop_after=0.4).start()
        msgs, events = [], []
        st = HLStream(lambda t, ns: msgs.append(t), lambda k, d: events.append(k), url=srv.url, ping_every=0.2, idle_timeout=5)
        await st.subscribe({"type": "l2Book", "coin": "#436390"})
        await st.subscribe({"type": "trades", "coin": "#436390"})
        task = asyncio.ensure_future(st.run())
        await asyncio.sleep(2.6)
        st.stop()
        await asyncio.wait_for(task, 5)
        await srv.stop()
        return srv, msgs, events, st

    srv, msgs, events, st = asyncio.run(go())
    assert srv.connections >= 2 and st.reconnects >= 1
    assert events.count("connected") >= 2 and "disconnected" in events
    # every connection got both subscriptions again
    assert len(srv.subscribe_msgs) >= 4
    assert any('"pong"' in m for m in msgs)


def test_subscription_cap_refuses_and_reports():
    async def go():
        events = []
        st = HLStream(lambda t, ns: None, lambda k, d: events.append((k, d)), url="ws://127.0.0.1:9/ws", max_subscriptions=2)
        ok = [await st.subscribe({"type": "l2Book", "coin": f"#{i}0"}) for i in range(3)]
        return ok, events, st

    ok, events, st = asyncio.run(go())
    assert ok == [True, True, False]
    assert events[0][0] == "subscription_refused" and len(st.refused) == 1


def test_idle_watchdog_triggers_reconnect():
    async def go():
        srv = await FakeWS(tick=10).start()  # server sends nothing on its own
        events = []
        st = HLStream(lambda t, ns: None, lambda k, d: events.append((k, d)), url=srv.url, ping_every=100, idle_timeout=0.6)
        task = asyncio.ensure_future(st.run())
        await asyncio.sleep(2.5)
        st.stop()
        await asyncio.wait_for(task, 5)
        await srv.stop()
        return events

    events = asyncio.run(go())
    reasons = [d.get("reason") for k, d in events if k == "disconnected"]
    assert "idle_timeout" in reasons
