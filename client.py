# client.py
import asyncio
import json
import os
import signal
from typing import Any, Dict, Optional, Tuple

import aiohttp
from fastmcp import Client as MCPClient

# ---- Config (override via env vars) ----
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-2k")  # e.g., "qwen2.5:3b" or "phi3:3.8b"
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp")
SUMMARY_AFTER_TOOL = os.getenv("SUMMARY_AFTER_TOOL", "1") == "1"  # set to 0 to disable
MAX_AGENT_STEPS = int(os.getenv("MAX_AGENT_STEPS", "4"))

SYSTEM_PROMPT = """You are an assistant that can use tools exposed by an MCP server.

Available tools:
1) search(search_text: str, limit: int=10) -> list[{"id": str, "title": str}]
   - Use to find FRED series IDs matching a query term.
   - Example: [{"id": "GDP", "title": "Gross Domestic Product"}]

2) get_series(series_id: str, observation_start: str|None, observation_end: str|None)
   -> list[{"date": "YYYY-MM-DD", "value": float}]
   - Use when the user asks for time series values. If you don't know the exact series_id, call search FIRST.

3) get_series_info(series_id: str) -> dict
   - Use to fetch metadata about a known series_id.

Rules:
- When you need tool output, respond with a single JSON object:
  {"action":"call_tool","tool":"search","args":{"search_text":"GDP","limit":5}}
  or
  {"action":"call_tool","tool":"get_series","args":{"series_id":"GDP","observation_start":null,"observation_end":null}}
  or
  {"action":"final","answer":"..."}  (when no tool is needed)

- Strict JSON only. No markdown, no commentary, no trailing text.
- If you do not know the exact series_id, you MUST call search() first, then pick the exact id from the results and call get_series().
- If the user mentions a time window, include observation_start/observation_end. Otherwise you may omit them (null).
"""

# --------------------- Ollama helpers ---------------------

async def ollama_generate(prompt: str, model: str = OLLAMA_MODEL, stream: bool = False) -> str:
    payload = {"model": model, "prompt": prompt, "stream": stream}
    async with aiohttp.ClientSession() as session:
        async with session.post(OLLAMA_URL, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("response", "")

def _coerce_json_object(text: str) -> Dict[str, Any]:
    first, last = text.find("{"), text.rfind("}")
    if first != -1 and last != -1 and last > first:
        chunk = text[first:last+1]
        try:
            obj = json.loads(chunk)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    return {}

async def model_plan(user_message: str, transcript: str = "") -> Dict[str, Any]:
    """Ask model for a JSON action, including recent transcript of tool results for context."""
    prompt = f"""{SYSTEM_PROMPT}

Conversation context for you (may include tool results to guide your next step):
{transcript}

User: {user_message}

Respond with strict JSON only."""
    text = await ollama_generate(prompt)
    obj = _coerce_json_object(text)
    if obj.get("action") in {"call_tool", "final"}:
        return obj
    return {"action": "final", "answer": text.strip()}

# --------------------- MCP content normalization ---------------------

def maybe_json_loads(s: str):
    s = s.strip()
    if not s:
        return s
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return s
    return s

def normalize_mcp_content(content: Any) -> Any:
    if content is None:
        return None
    if isinstance(content, (int, float, bool)):
        return content
    if isinstance(content, str):
        return maybe_json_loads(content)
    if isinstance(content, dict):
        return {k: normalize_mcp_content(v) for k, v in content.items()}
    if isinstance(content, (list, tuple)):
        return [normalize_mcp_content(x) for x in content]
    for attr in ("value", "text", "data", "content", "error"):
        if hasattr(content, attr):
            return normalize_mcp_content(getattr(content, attr))
    return str(content)

async def call_tool_normalized(client: MCPClient, tool: str, args: Dict[str, Any]) -> Any:
    resp = await client.call_tool(tool, args)
    return normalize_mcp_content(resp.content)

# --------------------- Summarization (optional) ---------------------

async def summarize_for_user(raw_content: Any, series_name: Optional[str] = None) -> str:
    label = f' "{series_name}"' if series_name else ""
    summary_prompt = (
        f"Summarize the following time series{label} for a non-technical user. "
        "Mention trend and the most recent value, succinctly.\n\n"
        f"{json.dumps(raw_content)[:6000]}"
    )
    return (await ollama_generate(summary_prompt)).strip()

# --------------------- Agent step loop (unbiased) ---------------------

async def run_agent(user_input: str) -> None:
    """
    Tool-use loop driven by the model:
    - Ask for an action
    - If call_tool, execute it and append the (truncated) result to transcript
    - Feed transcript back to model for the next step
    - No auto-picking series_id or dates; the model must choose using search results
    """
    transcript = ""
    series_name_for_summary: Optional[str] = None

    async with MCPClient(MCP_SERVER_URL) as client:
        for step in range(1, MAX_AGENT_STEPS + 1):
            plan = await model_plan(user_input, transcript=transcript)

            if plan.get("action") == "final":
                print(plan.get("answer", ""))
                return

            if plan.get("action") == "call_tool":
                tool = plan.get("tool")
                args = plan.get("args") or {}

                try:
                    result = await call_tool_normalized(client, tool, args)
                except Exception as e:
                    # Surface tool error and let the model react next step
                    err = f"[Tool error] {type(e).__name__}: {e}"
                    print(err)
                    transcript += f"\nTool {tool} error: {err}"
                    continue

                # Pretty print the raw result for you
                print(f"┌─ Tool result (step {step}) {tool} ─")
                print(json.dumps(result, indent=2))
                print("└─────────────────────────────────")

                # Add a concise snapshot back to the model as context
                # (keep small for local models)
                snap = json.dumps(result[:5], ensure_ascii=False) if isinstance(result, list) else json.dumps(result)
                transcript += f"\nTool {tool} returned (truncated): {snap}"

                # Remember series name if obvious
                if tool == "get_series":
                    sid = args.get("series_id")
                    if isinstance(sid, str):
                        series_name_for_summary = sid

                # Optionally produce a human summary after the *final* tool step.
                # We don't call summarize here—leave it to the model to decide finalization.
                continue

            print(f"[Agent] Unknown action: {plan}")
            return

    # If we ran out of steps without a 'final'
    if SUMMARY_AFTER_TOOL and series_name_for_summary:
        # Try to summarize the latest tool output we included
        print("\n(Note) Reached step limit; consider asking again or raising MAX_AGENT_STEPS.")
    else:
        print("\n(Note) Reached step limit; consider asking again or raising MAX_AGENT_STEPS.")

# --------------------- REPL ---------------------

async def repl() -> None:
    print("MCP REPL ready. Type your question. Commands: :quit, :q, :help")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: None)
        except NotImplementedError:
            pass

    while True:
        try:
            user_input = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            return

        if not user_input:
            continue
        if user_input in {":quit", ":q"}:
            print("Bye.")
            return
        if user_input == ":help":
            print("Examples:\n"
                  "  Search for mortgage rates and fetch the series.\n"
                  "  Get UNRATE since 2020.\n"
                  "  What units are used by series GDP?\n")
            continue

        await run_agent(user_input)

async def main() -> None:
    await repl()

if __name__ == "__main__":
    asyncio.run(main())
