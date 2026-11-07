"""`client.nlq` in embedded mode: question to Cypher with no server (#2092).

The provider is either `mock`, which answers a fixed query in-process, or a
stand-in for OpenAI on 127.0.0.1 that records what it is sent, so a test can
check the model sees the question, this graph's schema, the model name and
the key. No test here reaches a real provider.

Needs the compiled extension (`maturin develop`), like `test_embedded.py`.
"""

import json
import os
import shutil
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import samyama

MOCK_ANSWER = "MATCH (n) RETURN n LIMIT 10"
NLQ_VARS = ("NLQ_PROVIDER", "NLQ_MODEL", "NLQ_API_BASE_URL", "OPENAI_API_KEY", "GEMINI_API_KEY")


@pytest.fixture(autouse=True)
def clean_nlq_environment(monkeypatch):
    """Each test starts with no NLQ variable set, whatever the shell has."""
    for var in NLQ_VARS:
        monkeypatch.delenv(var, raising=False)


def people():
    c = samyama.SamyamaClient.embedded()
    c.query('CREATE (:Person {name: "Alice", age: 30})')
    c.query('CREATE (:Person {name: "Bob", age: 25})')
    c.query('CREATE (:City {name: "Pune"})')
    c.query('MATCH (a:Person {name: "Alice"}), (c:City) CREATE (a)-[:LIVES_IN]->(c)')
    return c


class FakeOpenAI:
    """An OpenAI chat-completions endpoint that answers `answer`."""

    def __init__(self, answer):
        self.requests = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append((self.path, self.headers.get("Authorization"), body))
                out = json.dumps({"choices": [{"message": {"content": answer}}]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}/v1"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def fake_openai():
    servers = []

    def start(answer):
        servers.append(FakeOpenAI(answer))
        return servers[-1]

    yield start
    for s in servers:
        s.close()


def inet_sockets():
    if shutil.which("lsof") is None:
        return None
    out = subprocess.run(["lsof", "-a", "-i", "-n", "-P", "-p", str(os.getpid())],
                         capture_output=True, text=True)
    return [line for line in out.stdout.splitlines()[1:] if line.strip()]


# --- it works ------------------------------------------------------------------


def test_embedded_nlq_with_the_mock_provider_returns_a_query_that_runs():
    client = people()
    cypher = client.nlq("Show me some nodes", provider="mock")
    assert cypher == MOCK_ANSWER
    assert len(client.query_readonly(cypher).records) == 3


def test_the_provider_can_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("NLQ_PROVIDER", "mock")
    assert people().nlq("Show me some nodes") == MOCK_ANSWER


def test_the_model_sees_the_question_this_graphs_schema_the_model_and_the_key(fake_openai):
    fake = fake_openai("```cypher\nMATCH (p:Person) RETURN p.name AS name ORDER BY name\n```")
    client = people()
    cypher = client.nlq("Who are the people?", provider="openai", model="gpt-test",
                        api_key="sk-test-2092", api_base_url=fake.base_url)
    assert cypher == "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
    assert client.query_readonly(cypher).records == [["Alice"], ["Bob"]]

    assert len(fake.requests) == 1
    path, auth, body = fake.requests[0]
    assert path == "/v1/chat/completions"
    assert auth == "Bearer sk-test-2092"
    assert body["model"] == "gpt-test"
    prompt = body["messages"][1]["content"]
    assert "Who are the people?" in prompt
    for word in ("Person", "City", "LIVES_IN"):
        assert word in prompt, f"the schema summary lacks {word}"


def test_arguments_take_the_place_of_the_environment(monkeypatch, fake_openai):
    fake = fake_openai("MATCH (n) RETURN count(n)")
    monkeypatch.setenv("NLQ_PROVIDER", "ollama")
    monkeypatch.setenv("NLQ_MODEL", "from-env")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-from-env")
    people().nlq("How many?", provider="openai", model="from-arg",
                 api_key="sk-from-arg", api_base_url=fake.base_url)
    _, auth, body = fake.requests[0]
    assert (auth, body["model"]) == ("Bearer sk-from-arg", "from-arg")


def test_the_key_and_base_url_can_come_from_the_environment(monkeypatch, fake_openai):
    fake = fake_openai("MATCH (n) RETURN count(n)")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-from-env")
    monkeypatch.setenv("NLQ_API_BASE_URL", fake.base_url)
    people().nlq("How many?", provider="openai")
    _, auth, body = fake.requests[0]
    assert auth == "Bearer sk-from-env"
    assert body["model"] == "gpt-4o", "the server's default model"


def test_the_mock_round_trip_opens_no_network_socket():
    before = inet_sockets()
    if before is None:
        pytest.skip("lsof is not available to list this process's sockets")
    client = people()
    assert client.nlq("Show me some nodes", provider="mock") == MOCK_ANSWER
    assert inet_sockets() == before == [], "mock nlq must not touch the network"


def test_the_async_client_passes_the_settings_through(fake_openai):
    import asyncio
    from samyama_aio import AsyncSamyamaClient

    fake = fake_openai("MATCH (p:Person) RETURN p.name")

    async def main():
        client = AsyncSamyamaClient(people())
        mock = await client.nlq("q", provider="mock")
        real = await client.nlq("q", provider="openai", model="gpt-async",
                                api_key="sk-async", api_base_url=fake.base_url)
        return mock, real

    assert asyncio.run(main()) == (MOCK_ANSWER, "MATCH (p:Person) RETURN p.name")
    assert fake.requests[0][2]["model"] == "gpt-async"


# --- refusals ------------------------------------------------------------------


def test_a_query_that_writes_is_refused_and_the_graph_is_untouched(fake_openai):
    fake = fake_openai("MATCH (n) DETACH DELETE n")
    client = people()
    with pytest.raises(RuntimeError, match="write operations"):
        client.nlq("Delete everything", provider="openai", api_key="sk-test",
                   api_base_url=fake.base_url)
    assert client.status().nodes == 3


def test_no_provider_anywhere_names_the_variable():
    with pytest.raises(RuntimeError, match="NLQ_PROVIDER is not set"):
        people().nlq("q")


def test_a_missing_key_names_the_providers_own_variable(monkeypatch):
    # An OpenAI key in the environment is not used for Gemini (#438).
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        people().nlq("q", provider="gemini")
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        people().nlq("q", provider="openai", api_key="")


def test_an_unknown_provider_is_refused_not_defaulted():
    with pytest.raises(RuntimeError, match='unknown NLQ_PROVIDER "gpt"'):
        people().nlq("q", provider="gpt")


def test_a_key_for_a_provider_that_takes_none_is_refused():
    with pytest.raises(RuntimeError, match="takes no API key"):
        people().nlq("q", provider="mock", api_key="sk-x")


def test_a_remote_client_refuses_the_settings_before_any_request():
    # Nothing listens at this address: a request would fail differently.
    remote = samyama.SamyamaClient.connect("http://127.0.0.1:9")
    with pytest.raises(ValueError, match=r"only apply in embedded mode \(got: provider, api_key\)"):
        remote.nlq("q", provider="openai", api_key="sk-x")
    with pytest.raises(ValueError, match=r"\(got: model\)"):
        remote.nlq("q", model="gpt-4o")
