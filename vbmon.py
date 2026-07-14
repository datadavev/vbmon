import asyncio
import datetime
import json
import time

import httpx
import websockets

SHINY_URL = "https://vegbank.org/"


async def measure_websocket_latency(url: str):
    message = ""
    http_latency = 0
    # Initialize HTTPX client to mimic browser session and handshake metadata
    async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
        try:
            start_http = time.perf_counter()
            response = await client.get(url)
            http_latency = time.perf_counter() - start_http

            if response.status_code != 200:
                message = f"HTTP Initialization Failed: Status {response.status_code}"
        except Exception as e:
            message = f"HTTP Connection Error: {e}"
            
    data = {
        "tstamp": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "http_latency": http_latency,
        "ws_latency": 0,
        "frame": "",
        "msg": message,
    }
    if data['msg'] != "":
        print(json.dumps(data))
        return

    # Construct the Shiny WebSocket URL
    # Shiny apps typically use a SockJS or raw websocket fallback endpoint.
    # We target the standard Shiny WebSocket endpoint framework.
    ws_url = url.replace("https://", "wss://").replace("http://", "ws://")
    if not ws_url.endswith("/"):
        ws_url += "/"

    # Standard Shiny/SockJS websocket route format: /__sockjs__/312/xyzabcde/websocket
    ws_target = f"{ws_url}__sockjs__/000/monitor_session/websocket"

    try:
        start_ws = time.perf_counter()
        async with websockets.connect(ws_target, open_timeout=10.0) as websocket:
            data["ws_latency"] = time.perf_counter() - start_ws

            # Read the initial 'o' (open) frame that Shiny/SockJS servers send immediately
            data["frame"] = await asyncio.wait_for(websocket.recv(), timeout=2.0)

    except websockets.exceptions.InvalidStatusCode as e:
        data["msg"] = f"WebSocket Handshake Rejected by Server: Status {e.status_code}"
    except asyncio.TimeoutError:
        data["msg"] = "WebSocket Connection Timed Out."
    except Exception as e:
        data["msg"] = f"WebSocket Connection Failed: {str(e)}"
    print(json.dumps(data))


if __name__ == "__main__":
    # Run the monitoring check once
    asyncio.run(measure_websocket_latency(SHINY_URL))
