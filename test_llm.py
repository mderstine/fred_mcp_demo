from openai import OpenAI


def main() -> None:
    client = OpenAI()
    response = client.responses.create(
        model="gpt-4o-mini",
        input="Use the FRED search tool to find the series ID for GDP and then retrieve the latest value.",
        tools=[
            {
                "type": "mcp",
                "server_url": "http://localhost:8000",
                "server_label": "fred",
            }
        ],
    )
    print(response.output_text)


if __name__ == "__main__":
    main()
