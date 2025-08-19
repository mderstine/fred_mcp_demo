# agent/main.py
import os
from datetime import datetime
from langchain_ollama import ChatOllama  # pip install -U langchain-ollama
from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate
from mcp_tools import get_curve, price_bond, put_curve


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ---- LLM ----
OLLAMA_BASE = os.getenv("OLLAMA_BASE", "http://127.0.0.1:11434")
MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-2k")
llm = ChatOllama(model=MODEL, base_url=OLLAMA_BASE)

# ---- Tools ----
TOOLS = [get_curve, price_bond, put_curve]

# ---- Prompt (ReAct, compact, escaped braces in examples) ----
template = """You are a bond analyst.

TOOLS:
{tools}

You can call these tools: {tool_names}

RULES:
- If asked to price in a named market, first try get_curve with {{ "market": "..." }}.
- If a curve is found, call price_bond with the same market and the user's bond terms.
- If no curve exists, either:
  - ask the user for a market or explicit curve, or
  - call price_bond with an explicit curve if the user supplied one.
- Action Input MUST be valid JSON (double-quoted keys, no trailing commas).
- Do not change the user's instrument or dates; if unclear, ask briefly in Final Answer.
- Keep Thought concise. Do not write Python—use the exact Action / Action Input format.

FORMAT:
Question: {input}
Thought: (short reasoning)
Action: <one of {tool_names}>
Action Input: <JSON object>

...(you will see an Observation)...

Thought: (next step or answer)
Final Answer: <concise answer>

EXAMPLES:

Example 1 (get curve then price):
Question: Price a 3y 5% semiannual $100 bond in usd_govt (issue 2024-01-15, maturity 2027-01-15) as of 2025-08-19. Persist result.
Thought: I should pull the usd_govt curve, then price and persist.
Action: get_curve
Action Input: {{ "market": "usd_govt" }}

Observation: [ {{ "t": 0.5, "rate": 0.0450 }}, {{ "t": 1.0, "rate": 0.0460 }}, ... ]

Thought: I will price using the market with the provided terms.
Action: price_bond
Action Input: {{
  "market": "usd_govt",
  "face": 100.0, "coupon": 0.05, "frequency": "Semiannual",
  "issue_date": "2024-01-15", "maturity_date": "2027-01-15",
  "valuation_date": "2025-08-19",
  "persist": true
}}

Observation: {{ "clean_price": 99.8, "dirty_price": 100.1, "accrued": 0.3, "ytm": 0.0505, "source": "computed" }}

Thought: I have the price. I can present the clean price and key details.
Final Answer: Clean ~99.8, dirty ~100.1, accrued ~0.3, YTM ~5.05% (as of 2025-08-19). Stored in DB.

Example 2 (insert/refresh curve):
Question: Update usd_govt curve with 4 points and then show it.
Thought: I should replace the curve and confirm.
Action: put_curve
Action Input: {{
  "market": "usd_govt",
  "curve": [ {{ "t": 0.5, "rate": 0.0430 }}, {{ "t": 1.0, "rate": 0.0440 }}, {{ "t": 2.0, "rate": 0.0450 }}, {{ "t": 3.0, "rate": 0.0460 }} ],
  "mode": "replace"
}}

Observation: {{ "market": "usd_govt", "points": 4, "mode": "replace" }}

Thought: I will retrieve the curve to confirm changes.
Action: get_curve
Action Input: {{ "market": "usd_govt" }}

Observation: [ {{ "t": 0.5, "rate": 0.0430 }}, {{ "t": 1.0, "rate": 0.0440 }}, {{ "t": 2.0, "rate": 0.0450 }}, {{ "t": 3.0, "rate": 0.0460 }} ]

Thought: The curve is updated.
Final Answer: Updated usd_govt curve with 4 points and confirmed.

{agent_scratchpad}"""

prompt = PromptTemplate.from_template(template)

agent = create_react_agent(llm, TOOLS, prompt)
executor = AgentExecutor(
    agent=agent,
    tools=TOOLS,
    verbose=True,
    handle_parsing_errors=True,
    max_iterations=4,
    return_intermediate_steps=False,
)


def main() -> None:
    print("Agent Ready. Type ':q' to Quit.")
    while True:
        try:
            q = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if q in (":q", ":quit"):
            break
        print(f"[{ts()}] Running...", flush=True)
        out = executor.invoke({"input": q})
        print(f"[{ts()}] Answer:\n{out['output']}\n", flush=True)


if __name__ == "__main__":
    main()
