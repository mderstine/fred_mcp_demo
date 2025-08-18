import asyncio
import json
import os
import signal
from typing import Any, Dict
import re
from datetime import date, timedelta
import aiohttp
from fastmcp import Client as MCPClient

# ---- Config (override via env vars) ----
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-2k")  # e.g. qwen2.5:3b or phi3:3.8b
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp")
SUMMARY_AFTER_TOOL = os.getenv("SUMMARY_AFTER_TOOL", "1") == "1"  # set to 0 to disable

SYSTEM_PROMPT = """You are an assistant that can use tools exposed by an MCP server.

Available tools:
1) search(search_text: str, limit: int=10) -> list[{"id": str, "title": str}]
   - Use to find FRED series IDs matching a query term.
   - Example return: [{"id": "GDP", "title": "Gross Domestic Product"}]

2) get_series(series_id: str, observation_start: str|None, observation_end: str|None)
   -> list[{"date": "YYYY-MM-DD", "value": float}]
   - Use when the user asks for time series values.

Rules:
- When you need tool output, respond with a single JSON object:
  {"action":"call_tool","tool":"search","args":{"search_text":"GDP","limit":5}}
  or:
  {"action":"call_tool","tool":"get_series","args":{"series_id":"GDP","observation_start":"2015-01-01","observation_end":null}}

- If the user just wants a conceptual answer and no tool is needed, respond with:
  {"action":"final","answer":"..."}
- Strict JSON only. No markdown, no commentary, no trailing text.
- Prefer search → get_series if the user gives a vague series name.
- Keep date ranges modest if unspecified or omit them.
"""

async def ollama_generate(prompt: str, model: str = OLLAMA_MODEL, stream: bool = False) -> str:
    """Call Ollama /api/generate and return the text response (non-streaming by default)."""
    payload = {"model": model, "prompt": prompt, "stream": stream}
    async with aiohttp.ClientSession() as session:
        async with session.post(OLLAMA_URL, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("response", "")

ID_RE = re.compile(r"^[A-Za-z0-9]{1,25}$")

def extract_id_from_title(title: str) -> str | None:
    """
    FRED titles often end with '[ID]'. Pull that if present.
    """
    m = re.search(r"\[([A-Za-z0-9]{1,25})\]\s*$", title or "")
    return m.group(1) if m else None

def clean_series_id(raw: str | None) -> str | None:
    """
    Trim and validate to FRED's '25 or less alphanumeric' rule.
    If invalid, return None.
    """
    if not raw:
        return None
    s = str(raw).strip()
    return s if ID_RE.fullmatch(s) else None

def pick_best_series_id(results: list[dict], prefer_keywords: list[str] | None = None) -> str | None:
    """
    Given search() results (list of {'id','title'}), choose the most likely ID.
    - Prefer IDs whose title matches all prefer_keywords (case-insensitive).
    - Else, return the first valid-looking ID.
    - Also try to extract [ID] from title if needed.
    """
    candidates: list[tuple[str, str]] = []
    for row in results or []:
        sid = clean_series_id(row.get("id"))
        title = (row.get("title") or "").strip()
        if not sid:
            from_title = extract_id_from_title(title)
            sid = clean_series_id(from_title)
        if sid:
            candidates.append((sid, title))

    if not candidates:
        return None

    if prefer_keywords:
        wants = [k.lower() for k in prefer_keywords]
        for sid, title in candidates:
            t = title.lower()
            if all(w in t for w in wants):
                return sid

    # mortgage-specific heuristic: prefer titles with 'mortgage' and '15' and 'fixed'
    for sid, title in candidates:
        tl = title.lower()
        if "mortgage" in tl and "15" in tl and "fixed" in tl:
            return sid

    # fallback: first valid candidate
    return candidates[0][0]

def _coerce_json_object(text: str) -> Dict[str, Any]:
    """Try to extract a single JSON object from model output; return {} on failure."""
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

async def model_plan(user_message: str) -> Dict[str, Any]:
    """Ask the model for an action JSON; fallback to final answer if parsing fails."""
    prompt = f"""{SYSTEM_PROMPT}

User: {user_message}

Respond with strict JSON only."""
    text = await ollama_generate(prompt)
    obj = _coerce_json_object(text)
    if obj.get("action") in {"call_tool", "final"}:
        return obj
    # fallback: treat raw text as final
    return {"action": "final", "answer": text.strip()}

async def summarize_for_user(raw_content: Any) -> str:
    """Ask the model to summarize tool output for readability."""
    summary_prompt = (
        "Summarize the following tool result for a non-technical user. "
        "Be concise and mention the series name if present.\n\n"
        f"{json.dumps(raw_content)[:6000]}"  # keep small for local model contexts
    )
    return (await ollama_generate(summary_prompt)).strip()

def normalize_mcp_content(content: Any) -> Any:
    """
    FastMCP client returns a list of content objects (e.g., TextContent).
    Convert to plain Python (list/dict/str) so we can JSON-dump safely.
    """
    # Common shapes: list[Content], dicts, scalars
    if content is None:
        return None

    # If it's already a simple type:
    if isinstance(content, (str, int, float, bool)):
        return content
    if isinstance(content, dict):
        return {k: normalize_mcp_content(v) for k, v in content.items()}
    if isinstance(content, (list, tuple)):
        return [normalize_mcp_content(x) for x in content]

    # Try generic attributes often present on content objects
    # e.g., TextContent(text=...), JsonContent(value=...), ErrorContent(error=...)
    for attr in ("value", "text", "data", "content", "error"):
        if hasattr(content, attr):
            return normalize_mcp_content(getattr(content, attr))

    # Fallback: string representation
    return str(content)

async def call_tool_normalized(client: MCPClient, tool: str, args: Dict[str, Any]) -> Any:
    resp = await client.call_tool(tool, args)
    return normalize_mcp_content(resp.content)

async def handle_user_turn(user_input: str) -> None:
    plan = await model_plan(user_input)

    if plan.get("action") == "final":
        print(plan.get("answer", ""))
        return

    if plan.get("action") == "call_tool":
        tool = plan.get("tool")
        args = plan.get("args") or {}

        try:
            async with MCPClient(MCP_SERVER_URL) as client:
                result = await call_tool_normalized(client, tool, args)

                print("┌─ Tool result (raw) ─")
                print(json.dumps(result, indent=2))
                print("└─────────────────────")

                # --- Auto-chain: search -> get_series (last 5 years) ---
                if tool == "search":
                    # Expect a list of {"id": "...", "title": "..."}
                    five_years_ago = (date.today() - timedelta(days=5*365)).isoformat()

                    prefer = ["15-year", "fixed", "united states"]  # steer toward MORTGAGE15US
                    series_id = pick_best_series_id(result, prefer_keywords=prefer)

                    if not series_id:
                        # Try widening: call search again with higher limit, then re-pick
                        try:
                            widened = await call_tool_normalized(client, "search", {"search_text": args.get("search_text", ""), "limit": 25})
                            series_id = pick_best_series_id(widened, prefer_keywords=prefer)
                        except Exception:
                            pass

                    if not series_id:
                        print("[Agent] Could not determine a valid series id from search results.")
                        print("Here are the first few raw rows so you can copy an ID manually:")
                        print(json.dumps(result[:5], indent=2))
                        return

                    print(f"\nChaining to get_series(series_id='{series_id}', observation_start='{five_years_ago}') …")
                    follow_args = {
                        "series_id": series_id,
                        "observation_start": five_years_ago,
                        "observation_end": None,
                    }

                    series_result = await call_tool_normalized(client, "get_series", follow_args)

                    print("┌─ get_series result (raw) ─")
                    print(json.dumps(series_result, indent=2))
                    print("└────────────────────────────")

                    if SUMMARY_AFTER_TOOL:
                        try:
                            summary = await summarize_for_user(series_result)
                            print("\nSummary:", summary)
                        except Exception as e:
                            print(f"[Summary error] {e}")
                else:
                    # Non-search tool: print args and validate series_id if present
                    sid = args.get("series_id")
                    if sid and not clean_series_id(sid):
                        print(f"[Agent] The model proposed an invalid series_id: {sid!r}. "
                            f"Series IDs must be <=25 alphanumeric chars.")
                        return

                    if SUMMARY_AFTER_TOOL:
                        try:
                            summary = await summarize_for_user(result)
                            print("\nSummary:", summary)
                        except Exception as e:
                            print(f"[Summary error] {e}")

        except Exception as e:
            print(f"[Tool error] {e}")
        return

    print(f"[Agent] Unknown action: {plan}")

async def repl() -> None:
    """Simple interactive loop."""
    print("MCP REPL ready. Type your question. Commands: :quit, :q, :help")
    loop = asyncio.get_running_loop()
    # graceful Ctrl+C in REPL
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
            print("Ask anything. Example:\n"
                  "  Find the FRED series for GDP and show values since 2015.\n"
                  "  Get GDP since 2015.\n"
                  "Commands: :quit, :q, :help")
            continue

        await handle_user_turn(user_input)

async def main() -> None:
    await repl()

if __name__ == "__main__":
    asyncio.run(main())
