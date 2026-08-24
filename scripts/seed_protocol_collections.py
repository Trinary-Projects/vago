#!/usr/bin/env python3
"""Inspect / fix / seed / probe the ProtocolAnchor + ProtocolInstruction
collections on the US Weaviate instance.

Targets the PRODUCTION instance by default (2026-07-31): both staging and prod
vago now read the same prod Weaviate, so there is one corpus rather than two
that drift. `weaviate-us.curelinktech.in` resolves to prod (us-east4, namespace
hosted-models); staging's own instance is `weaviate-us-staging.curelinktech.in`
and is no longer what vago reads.

SCHEMA OWNERSHIP: disha-backend `weaviate/migrations/00NN_*.json` is the source
of truth for these classes, applied by its migration manager. The class files
next to this script are a convenience copy for --recreate-* and can drift —
prefer recreating from the backend's migrations, and note that those currently
hardcode the STAGING TEI endpointURL, which does not resolve inside the prod
cluster.

Stdlib only, same style as disha-hosted-models validation/weaviate_smoke.py.

Modes (combinable; --inspect always runs first):

  --inspect                 print class config, object counts, and a
                            contract-compliance verdict (read-only, default)
  --recreate-anchor-class   DELETE + re-POST ProtocolAnchor with the
                            text2vec-huggingface contract. DESTRUCTIVE.
                            Refuses when the class holds objects unless --force.
  --seed                    insert the fixture protocols (instructions first,
                            then anchors cross-referencing them)
  --probe                   run nearText queries and print the similarity
                            distribution -- this is the calibration data for
                            the retrieval threshold

The collection contract comes from disha-hosted-models AGENTS.md /
validation/weaviate_smoke.py and is NOT optional:

  vectorizer: text2vec-huggingface
  moduleConfig["text2vec-huggingface"].endpointURL   = internal TEI URL
  moduleConfig["text2vec-huggingface"].vectorizeClassName = False
  per-property moduleConfig[...].vectorizePropertyName   = False
  vectorIndexConfig.distance = cosine

vectorizeClassName defaults to True; leaving it on makes Weaviate prepend the
class name before calling TEI, so stored vectors diverge from a raw-text
/embed call (measured cosine 0.947 on the smoke case).

TEI applies the "Document: " prefix server-side via --default-prompt, so every
client -- this script and vago alike -- sends RAW UNPREFIXED text. Prefixing
here would double-prefix and silently degrade ranking (cosine 0.9937 vs
0.99995) with no error.

Only ProtocolAnchor needs a vectorizer: retrieval searches anchors by text and
follows answeredBy to the instruction. ProtocolInstruction stays
vectorizer=none, matching SituationInstruction on weaviate-v2.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import urllib.error
import urllib.request

ANCHOR_CLASS = "ProtocolAnchor"
INSTRUCTION_CLASS = "ProtocolInstruction"

# Single source of truth for the class definitions: the same files you can
# POST straight to /v1/schema with curl. Do not inline a second copy here.
SCHEMA_DIR = pathlib.Path(__file__).resolve().parent / "weaviate"
CLASS_FILES = {
    INSTRUCTION_CLASS: SCHEMA_DIR / "protocol_instruction.class.json",
    ANCHOR_CLASS: SCHEMA_DIR / "protocol_anchor.class.json",
}


def load_class_def(name: str, tei_url: str) -> dict:
    path = CLASS_FILES[name]
    try:
        definition = json.loads(path.read_text())
    except FileNotFoundError:
        die(f"missing class definition {path}")
    except json.JSONDecodeError as exc:
        die(f"invalid JSON in {path}: {exc}")
    # The file carries the staging TEI URL literally so `curl -d @file` works
    # with no preprocessing; --tei-url overrides it for other namespaces.
    module = (definition.get("moduleConfig") or {}).get("text2vec-huggingface")
    if module is not None:
        module["endpointURL"] = tei_url
    return definition

DEFAULT_WEAVIATE_URL = "https://weaviate-us.curelinktech.in"
DEFAULT_TEI_URL = "http://jina-embeddings-v5-text-small.{namespace}.svc.cluster.local"
DEFAULT_KUBE_CONTEXT = "gke_curelinkai_us-east4_disha-voice-worker-prod"
DEFAULT_NAMESPACE = "hosted-models"
DEFAULT_SECRET = "weaviate-api-key"

REQUEST_TIMEOUT = 60

# Fixture protocols. Deliberately shaped to exercise every branch of the
# retrieval design:
#   - several anchors per instruction  -> the dedupe-by-instruction-id path
#   - turnsThresholdCount 2 / 4 / 5    -> the per-protocol TTL path
#   - one instruction with it OMITTED  -> the default-3 fallback
#   - Hinglish anchors                 -> the real query distribution
FIXTURES: list[dict] = [
    {
        "title": "Acidity or reflux reported during a check-in",
        "turnsThresholdCount": 4,
        "documentVersionPath": "Voice_Agent/protocol_book/acidity/v/1",
        "instructionText": (
            "If the user reports acidity, heartburn, gas or reflux: do not diagnose and do not "
            "suggest any medicine. Ask whether it happens more at night or after specific meals. "
            "Advise finishing dinner at least two hours before lying down, avoiding fried and very "
            "spicy food that day, and sipping warm water. If the user says it has been happening "
            "for more than a week, or mentions chest pain or vomiting, tell them a health coach "
            "will call and flag it for escalation."
        ),
        "anchors": [
            "User says they have acidity at night",
            "मुझे रात में बहुत एसिडिटी हो रही है",
            "User reports heartburn or reflux after meals",
            "User complains of gas and bloating after dinner",
        ],
    },
    {
        "title": "User has not followed the diet plan",
        "turnsThresholdCount": 2,
        "documentVersionPath": "Voice_Agent/protocol_book/diet_non_adherence/v/1",
        "instructionText": (
            "If the user admits they did not follow the diet plan: never scold or express "
            "disappointment. Acknowledge it lightly, ask which specific meal was hardest to "
            "follow, and offer one concrete swap for that single meal rather than restating the "
            "whole plan. Keep it to one suggestion per turn."
        ),
        "anchors": [
            "User admits they did not follow the diet plan",
            "मैंने डाइट फॉलो नहीं की इस हफ्ते",
            "User says they ate outside food or ordered in",
            "User says the diet is too difficult to follow",
        ],
    },
    {
        "title": "Weight has not changed or has increased",
        "turnsThresholdCount": 5,
        "documentVersionPath": "Voice_Agent/protocol_book/weight_plateau/v/1",
        "instructionText": (
            "If the user reports no weight change or a weight increase: normalise it first, "
            "explaining that weight moves in steps and water retention masks fat loss. Ask about "
            "sleep and water intake before anything else, since those are the most common causes. "
            "Do not promise a specific number or timeline. Do not change the plan on this call."
        ),
        "anchors": [
            "User says weight has not reduced at all",
            "मेरा वजन कम नहीं हो रहा है",
            "User reports weight has gone up since last month",
            "User is frustrated about a weight plateau",
        ],
    },
    {
        "title": "User asks to stop or reduce prescribed medication",
        "turnsThresholdCount": 2,
        "documentVersionPath": "Voice_Agent/protocol_book/medication_change/v/1",
        "instructionText": (
            "If the user asks about stopping, reducing or changing any prescribed medication: "
            "never give an opinion on the dose. State clearly that medication changes are only "
            "decided by their doctor, ask what prompted the question, and flag the conversation "
            "for a health-coach callback. This overrides any general lifestyle advice."
        ),
        "anchors": [
            "User asks whether they can stop their BP medicine",
            "क्या मैं अपनी दवा बंद कर सकता हूँ",
            "User wants to reduce their medication dose",
            "User says they stopped taking their tablets on their own",
        ],
    },
    {
        # turnsThresholdCount deliberately omitted -> exercises the default of 3.
        "title": "User is in a hurry or wants to end the call",
        "documentVersionPath": "Voice_Agent/protocol_book/call_wrap_up/v/1",
        "instructionText": (
            "If the user says they are busy, driving, in a meeting or asks to talk later: do not "
            "push the agenda. Confirm in one sentence, ask for a better time slot, and close "
            "warmly. Do not attempt any further data collection on this call."
        ),
        "anchors": [
            "User says they are busy right now and cannot talk",
            "मैं अभी बिजी हूँ बाद में बात करते हैं",
            "User asks to call back later",
            "User says they are driving",
        ],
    },
]

# Probe queries in the exact shape vago will send (see the design note's
# query-text builder): the latest user turn only, raw and unprefixed. Production
# skips turns with fewer than four words, so every probe meets that boundary.
PROBE_QUERIES: list[str] = [
    "nahi didi, mujhe bahut acidity ho rahi hai raat me",
    "nahi didi, bahar ka kha liya do teen baar",
    "kuch nahi hua, balki badh gaya hai",
    "didi main soch raha tha ki dawai band kar du",
    "didi main abhi drive kar raha hoon, baad me baat karein",
    "theek tha didi bas",
]


# ---------------------------------------------------------------- http helpers


def http_request(url: str, headers: dict, method: str = "GET", body: bytes | None = None):
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        return 0, f"transport error: {exc.reason}"


def graphql(url: str, headers: dict, query: str) -> dict:
    status, raw = http_request(
        f"{url}/v1/graphql", headers, method="POST", body=json.dumps({"query": query}).encode()
    )
    if status != 200:
        die(f"GraphQL HTTP {status}: {raw[:500]}")
    payload = json.loads(raw)
    if payload.get("errors"):
        die(f"GraphQL errors: {json.dumps(payload['errors'])[:600]}")
    return payload["data"]


def die(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def load_api_key(args) -> str:
    """Read the key from the k8s secret. Never printed, never logged."""
    if args.api_key_env_value:
        return args.api_key_env_value.strip()
    try:
        encoded = subprocess.run(
            [
                "kubectl", f"--context={args.kube_context}", "-n", args.namespace,
                "get", "secret", args.secret, "-o", "jsonpath={.data.api-key}",
            ],
            capture_output=True, text=True, check=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        die(f"could not read secret {args.secret}: {exc.stderr.strip()[:300]}")
    import base64

    key = base64.b64decode(encoded).decode().strip()
    if not key:
        die("secret holds an empty api-key")
    return key


# -------------------------------------------------------------------- inspect


def fetch_class(url: str, headers: dict, name: str) -> dict | None:
    status, raw = http_request(f"{url}/v1/schema/{name}", headers)
    if status == 404:
        return None
    if status != 200:
        die(f"GET /v1/schema/{name} -> HTTP {status}: {raw[:300]}")
    return json.loads(raw)


def count_objects(url: str, headers: dict, name: str) -> int:
    data = graphql(url, headers, f"{{ Aggregate {{ {name} {{ meta {{ count }} }} }} }}")
    return data["Aggregate"][name][0]["meta"]["count"]


def anchor_contract_problems(cls: dict, tei_url: str) -> list[str]:
    problems = []
    if cls.get("vectorizer") != "text2vec-huggingface":
        problems.append(
            f'vectorizer is {cls.get("vectorizer")!r}, must be "text2vec-huggingface" '
            "(nearText is unavailable without it)"
        )
    module = (cls.get("moduleConfig") or {}).get("text2vec-huggingface") or {}
    if not module.get("endpointURL"):
        problems.append(f"moduleConfig.text2vec-huggingface.endpointURL missing (expected {tei_url})")
    elif module["endpointURL"].rstrip("/") != tei_url.rstrip("/"):
        problems.append(f'endpointURL is {module["endpointURL"]!r}, expected {tei_url!r}')
    if module.get("vectorizeClassName") is not False:
        problems.append("moduleConfig.text2vec-huggingface.vectorizeClassName must be false")
    if (cls.get("vectorIndexConfig") or {}).get("distance") != "cosine":
        problems.append(
            f'vectorIndexConfig.distance is {(cls.get("vectorIndexConfig") or {}).get("distance")!r}, '
            "must be \"cosine\" (the similarity threshold is computed as 1 - distance)"
        )
    props = {p["name"]: p for p in cls.get("properties") or []}
    if "anchorText" not in props:
        problems.append("property anchorText missing")
    else:
        pm = (props["anchorText"].get("moduleConfig") or {}).get("text2vec-huggingface") or {}
        if pm.get("vectorizePropertyName") is not False:
            problems.append("anchorText.moduleConfig.text2vec-huggingface.vectorizePropertyName must be false")
        if pm.get("skip") is True:
            problems.append("anchorText is marked skip=true, so it would not be vectorized")
    if "answeredBy" not in props:
        problems.append("cross-reference property answeredBy missing")
    elif props["answeredBy"].get("dataType") != [INSTRUCTION_CLASS]:
        problems.append(f'answeredBy dataType is {props["answeredBy"].get("dataType")!r}')
    return problems


def do_inspect(url: str, headers: dict, tei_url: str) -> tuple[dict | None, dict | None]:
    print("=== inspect ===")
    anchor = fetch_class(url, headers, ANCHOR_CLASS)
    instruction = fetch_class(url, headers, INSTRUCTION_CLASS)
    for name, cls in ((INSTRUCTION_CLASS, instruction), (ANCHOR_CLASS, anchor)):
        if cls is None:
            print(f"  {name}: ABSENT")
            continue
        vic = cls.get("vectorIndexConfig") or {}
        print(
            f"  {name}: vectorizer={cls.get('vectorizer')!r} distance={vic.get('distance')!r} "
            f"objects={count_objects(url, headers, name)} "
            f"moduleConfig={json.dumps(cls.get('moduleConfig') or {})}"
        )
        print(f"    properties: {[p['name'] for p in cls.get('properties') or []]}")
    if anchor is not None:
        problems = anchor_contract_problems(anchor, tei_url)
        if problems:
            print(f"  VERDICT: {ANCHOR_CLASS} violates the collection contract:")
            for problem in problems:
                print(f"    - {problem}")
            print(f"  Fix with --recreate-anchor-class (a class vectorizer cannot be changed in place).")
        else:
            print(f"  VERDICT: {ANCHOR_CLASS} satisfies the collection contract.")
    if instruction is not None and instruction.get("vectorizer") not in (None, "none"):
        print(
            f"  NOTE: {INSTRUCTION_CLASS}.vectorizer={instruction.get('vectorizer')!r}; "
            "vectorizer=none is expected (instructions are never searched directly)."
        )
    return anchor, instruction


# ------------------------------------------------------------ recreate + seed


def delete_class(url: str, headers: dict, name: str, force: bool) -> None:
    if fetch_class(url, headers, name) is None:
        return
    count = count_objects(url, headers, name)
    if count and not force:
        die(f"{name} holds {count} objects; refusing to delete without --force")
    status, raw = http_request(f"{url}/v1/schema/{name}", headers, method="DELETE")
    if status not in (200, 204):
        die(f"DELETE /v1/schema/{name} -> HTTP {status}: {raw[:300]}")
    print(f"  deleted {name} (held {count} objects)")


def create_class(url: str, headers: dict, name: str, tei_url: str) -> None:
    definition = load_class_def(name, tei_url)
    status, raw = http_request(
        f"{url}/v1/schema", headers, method="POST", body=json.dumps(definition).encode()
    )
    if status not in (200, 201):
        die(f"POST /v1/schema ({name}) -> HTTP {status}: {raw[:400]}")
    module = (definition.get("moduleConfig") or {}).get("text2vec-huggingface") or {}
    suffix = f" endpointURL={module['endpointURL']}" if module.get("endpointURL") else ""
    print(f"  created {name} (vectorizer={definition.get('vectorizer')!r}{suffix})")


def do_recreate_classes(url: str, headers: dict, tei_url: str, force: bool, both: bool) -> None:
    print("=== recreate classes ===")
    # Anchor first: it holds the cross-reference INTO the instruction class, so
    # dropping the target while the referrer still exists is asking for trouble.
    delete_class(url, headers, ANCHOR_CLASS, force)
    if both:
        delete_class(url, headers, INSTRUCTION_CLASS, force)
        create_class(url, headers, INSTRUCTION_CLASS, tei_url)
    elif fetch_class(url, headers, INSTRUCTION_CLASS) is None:
        die(
            f"{INSTRUCTION_CLASS} does not exist and is the target of "
            f"{ANCHOR_CLASS}.answeredBy; pass --recreate-both"
        )
    create_class(url, headers, ANCHOR_CLASS, tei_url)


def insert_object(url: str, headers: dict, class_name: str, properties: dict) -> str:
    body = json.dumps({"class": class_name, "properties": properties}).encode()
    status, raw = http_request(f"{url}/v1/objects", headers, method="POST", body=body)
    if status not in (200, 201):
        die(f"POST /v1/objects ({class_name}) -> HTTP {status}: {raw[:400]}")
    return json.loads(raw)["id"]


def do_seed(url: str, headers: dict) -> None:
    print("=== seed ===")
    anchor_total = 0
    for fixture in FIXTURES:
        properties = {
            "instructionText": fixture["instructionText"],
            "title": fixture["title"],
            "documentVersionPath": fixture["documentVersionPath"],
            # BOTH flags on purpose. There is now a single Weaviate instance
            # (prod), and vago picks its filter field from ENVIRONMENT: a
            # staging worker filters isStaging, a prod worker filters
            # isProduction. Seeding only one flag would make the other
            # environment retrieve nothing, with no error to notice.
            "isStaging": True,
            "isProduction": True,
        }
        # Omitted on purpose for one fixture, to exercise the default-3 fallback.
        if "turnsThresholdCount" in fixture:
            properties["turnsThresholdCount"] = fixture["turnsThresholdCount"]
        instruction_id = insert_object(url, headers, INSTRUCTION_CLASS, properties)
        for anchor_text in fixture["anchors"]:
            # Raw text: TEI adds "Document: " server-side. Do NOT prefix here.
            insert_object(
                url,
                headers,
                ANCHOR_CLASS,
                {
                    "anchorText": anchor_text,
                    "answeredBy": [
                        {"beacon": f"weaviate://localhost/{INSTRUCTION_CLASS}/{instruction_id}"}
                    ],
                },
            )
            anchor_total += 1
        print(
            f"  {fixture['title']!r}: instruction={instruction_id} "
            f"anchors={len(fixture['anchors'])} "
            f"turnsThresholdCount={fixture.get('turnsThresholdCount', '(omitted -> default 3)')}"
        )
    print(f"  seeded {len(FIXTURES)} instructions / {anchor_total} anchors")


# ----------------------------------------------------------------------- probe


def do_probe(url: str, headers: dict, limit: int, threshold: float) -> None:
    print("=== probe (threshold calibration data) ===")
    print(f"  gate: cosine similarity >= {threshold}  [similarity = 1 - distance]")
    tops: list[float] = []
    for query in PROBE_QUERIES:
        gql = (
            f"{{ Get {{ {ANCHOR_CLASS}("
            f"nearText: {{ concepts: {json.dumps([query])} }} "
            f'where: {{ path: ["answeredBy","{INSTRUCTION_CLASS}","isStaging"], '
            "operator: Equal, valueBoolean: true } "
            f"limit: {limit}"
            ") { anchorText answeredBy { ... on "
            f"{INSTRUCTION_CLASS} {{ title turnsThresholdCount _additional {{ id }} }} }} "
            "_additional { id distance } } } }"
        )
        data = graphql(url, headers, gql)
        hits = data["Get"][ANCHOR_CLASS] or []
        user_line = query.split("\n")[-1]
        print(f"\n  query: {user_line[:88]}")
        if not hits:
            print("    (no hits)")
            continue
        # Dedupe by instruction id, best score wins -- the same rule vago applies.
        best: dict[str, dict] = {}
        for hit in hits:
            answered = (hit.get("answeredBy") or [{}])[0]
            instruction_id = (answered.get("_additional") or {}).get("id")
            if not instruction_id:
                continue
            similarity = 1.0 - hit["_additional"]["distance"]
            if instruction_id not in best or similarity > best[instruction_id]["similarity"]:
                best[instruction_id] = {
                    "similarity": similarity,
                    "title": answered.get("title", ""),
                    "ttl": answered.get("turnsThresholdCount"),
                    "anchor": hit["anchorText"],
                }
        ranked = sorted(best.values(), key=lambda item: -item["similarity"])
        tops.append(ranked[0]["similarity"])
        for item in ranked:
            mark = "PASS" if item["similarity"] >= threshold else "    "
            print(
                f"    {mark} sim={item['similarity']:.4f}  ttl={item['ttl']}  "
                f"{item['title'][:44]!r}  <- {item['anchor'][:44]!r}"
            )
        print(f"    deduped {len(hits)} anchors -> {len(ranked)} distinct protocols")
    if tops:
        tops.sort()
        print(
            f"\n  top-hit similarity across {len(tops)} queries: "
            f"min={tops[0]:.4f} median={tops[len(tops)//2]:.4f} max={tops[-1]:.4f}"
        )
        passing = sum(1 for value in tops if value >= threshold)
        print(f"  would qualify at {threshold}: {passing}/{len(tops)} queries")


# ------------------------------------------------------------------------ main


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--weaviate-url", default=DEFAULT_WEAVIATE_URL)
    parser.add_argument("--tei-url", default=None, help=f"default: {DEFAULT_TEI_URL}")
    parser.add_argument("--kube-context", default=DEFAULT_KUBE_CONTEXT)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--secret", default=DEFAULT_SECRET)
    parser.add_argument("--api-key-env-value", default=None, help="use this key instead of reading the secret")
    parser.add_argument(
        "--recreate-anchor-class", action="store_true",
        help="DESTRUCTIVE: drop + recreate ProtocolAnchor from its class file",
    )
    parser.add_argument(
        "--recreate-both", action="store_true",
        help="DESTRUCTIVE: drop + recreate both classes from their class files",
    )
    parser.add_argument("--force", action="store_true", help="allow deleting a non-empty class")
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--threshold", type=float, default=0.70)
    args = parser.parse_args()

    tei_url = args.tei_url or DEFAULT_TEI_URL.format(namespace=args.namespace)
    url = args.weaviate_url.rstrip("/")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {load_api_key(args)}"}

    do_inspect(url, headers, tei_url)
    if args.recreate_anchor_class or args.recreate_both:
        do_recreate_classes(url, headers, tei_url, args.force, both=args.recreate_both)
    if args.seed:
        do_seed(url, headers)
    if args.probe:
        do_probe(url, headers, args.limit, args.threshold)
    if not (args.recreate_anchor_class or args.recreate_both or args.seed or args.probe):
        print("\n(read-only run; pass --recreate-both / --recreate-anchor-class / --seed / --probe to act)")


if __name__ == "__main__":
    main()
