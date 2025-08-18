# client.py
import asyncio
import json
from typing import Any, Dict, Optional
import aiohttp
from fastmcp import Client as MCPClient

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:3b-2k"  # change to your pulled model (e.g., "qwen2.5:3b" or "phi3:3.8b")

MCP_SERVER_URL = "http://127.0.0.1:8000/mcp"

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
  {"action":"call_tool","tool":"get_series","args":{"series_id":"GDP","observation_start":"2010-01-01","observation_end":null}}

- If the user just wants a conceptual answer and no tool is needed, respond with:
  {"action":"final","answer":"..."}
- Strict JSON only. No markdown, no commentary, no trailing text.
- Prefer search → get_series if the user gives a vague series name.
- Keep date ranges modest if unspecified (e.g., last 5 years) or omit them.
"""

async def ollama_generate(prompt: str, model: str = OLLAMA_MODEL, stream: bool = False) -> str:
    """
    Call Ollama's /api/generate with a single-shot prompt (non-streaming by default)
    and return the model's text output.
    """
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": stream,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(OLLAMA_URL, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            # Non-streaming returns {"response": "...", ...}
            return data.get("response", "")

async def model_plan(user_message: str) -> Dict[str, Any]:
    """
    Ask the model to return a strict JSON action.
    Falls back to {"action":"final","answer": "..."} if parsing fails.
    """
    prompt = f"""{SYSTEM_PROMPT}

User: {user_message}

Respond with strict JSON only."""
    text = await ollama_generate(prompt)
    # Try to isolate a JSON object if the model adds noise (it shouldn't, but defensively parse)
    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last != -1 and last > first:
        text = text[first:last+1]
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "action" in obj:
            return obj
    except json.JSONDecodeError:
        pass
    # Fallback
    return {"action": "final", "answer": text.strip()}

async def run_agent_once(user_message: str) -> None:
    """
    Runs a single turn:
    - Ask model for an action (final or call_tool)
    - If call_tool, execute via MCP and print result
    - If final, print the answer
    """
    plan = await model_plan(user_message)

    if plan.get("action") == "final":
        print(plan.get("answer", ""))
        return

    if plan.get("action") == "call_tool":
        tool = plan.get("tool")
        args = plan.get("args", {}) or {}

        async with MCPClient(MCP_SERVER_URL) as client:
            # Optional: list tools (debug)
            # tools = await client.list_tools()
            # print("TOOLS:", tools)

            try:
                resp = await client.call_tool(tool, args)
            except Exception as e:
                print(f"[Tool Error] {e}")
                return

            # Print the tool’s raw content; you can pretty-print if you like
            print(json.dumps(resp.content, indent=2))

            # You could also chain: feed tool result back to the model for a nice summary.
            # Uncomment to get a final, readable answer:
            """
            summary_prompt = f"Summarize the following tool result for the user:\n{json.dumps(resp.content)})"
            final_answer = await ollama_generate(summary_prompt)
            print(final_answer.strip())
            """
        return

    # Unknown action
    print(f"[Agent] Unknown action: {plan}")

async def main():
    # examples:
    # 1) Vague query → model should choose search(...)
    await run_agent_once("Find the FRED series for US GDP and show me the latest values.")
    # 2) Direct series ID → model should call get_series(...)
    # await run_agent_once("Get the time series for GDP since 2015.")
    # 3) Conceptual question → model may choose final(...)
    # await run_agent_once("What is the difference between GDP and GNP?")

if __name__ == "__main__":
    asyncio.run(main())
