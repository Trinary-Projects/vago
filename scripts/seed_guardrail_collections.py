#!/usr/bin/env python3
"""Inspect / fix / seed / probe the GuardrailAnchor + GuardrailInstruction
collections on the US Weaviate instance.

Sibling to `seed_protocol_collections.py` — same instance, same TEI contract,
same script shape. Read that script's header first if this is your first time
here; only the guardrail-specific deltas are called out below.

Targets the PRODUCTION instance by default, same as the protocol script:
`weaviate-us.curelinktech.in` resolves to prod (us-east4, namespace
hosted-models); staging's own instance (`weaviate-us-staging.curelinktech.in`)
is not what vago reads.

SCHEMA OWNERSHIP: disha-backend `weaviate/migrations/0014_GuardrailInstruction.json`
and `0015_GuardrailAnchor.json` are the source of truth for these classes,
applied by its migration manager. The class files next to this script are a
convenience copy for --recreate-* and can drift — prefer recreating from the
backend's migrations if the two have diverged.

Stdlib only, same style as disha-hosted-models validation/weaviate_smoke.py.

Modes (combinable; --inspect always runs first):

  --inspect                    print class config, object counts, and a
                               contract-compliance verdict (read-only, default)
  --recreate-anchor-class      DELETE + re-POST GuardrailAnchor with the
                               text2vec-huggingface contract. DESTRUCTIVE.
                               Refuses when the class holds objects unless
                               --force.
  --recreate-instruction-class DELETE + re-POST GuardrailInstruction.
                               DESTRUCTIVE. Same --force rule. Pass both flags
                               together to rebuild the pair from scratch (the
                               anchor's answeredBy cross-reference target must
                               exist before the anchor class can be created, so
                               order is handled for you when both are given).
  --seed                       insert the fixture guardrails (instructions
                               first, then anchors cross-referencing them)
  --probe                      run nearText queries and print the similarity
                               distribution -- the calibration data behind the
                               current 0.70 / 0.85 bands (design note §9).
                               Re-run it whenever the corpus changes

The collection contract comes from disha-hosted-models AGENTS.md /
validation/weaviate_smoke.py and is NOT optional, and it is IDENTICAL for both
classes here (unlike protocols, where only ProtocolAnchor is vectorized):

  vectorizer: text2vec-huggingface
  moduleConfig["text2vec-huggingface"].endpointURL   = internal TEI URL
  moduleConfig["text2vec-huggingface"].vectorizeClassName = False
  per-property moduleConfig[...].vectorizePropertyName   = False
  vectorIndexConfig.distance = cosine

vectorizeClassName defaults to True; leaving it on makes Weaviate prepend the
class name before calling TEI, so stored vectors diverge from a raw-text
/embed call (measured cosine 0.947 on the protocol smoke case).

TEI applies the "Document: " prefix server-side via --default-prompt, so every
client -- this script and vago alike -- sends RAW UNPREFIXED text. Prefixing
here would double-prefix and silently degrade ranking (cosine 0.9937 vs
0.99995 on the protocol case) with no error.

GuardrailInstruction is ALSO vectorized (instructionText, skip=false) even
though retrieval never queries it directly -- title and documentVersionPath
are present with skip=true (stored, not embedded) purely so a vectorizer
change is never needed if a future read path wants to search instructions
directly. This mirrors the checked-in backend migration exactly; do not "fix"
it to vectorizer=none the way ProtocolInstruction is -- that would diverge
from `0014_GuardrailInstruction.json`.

GuardrailInstruction has NO turnsThresholdCount. Guardrails have no resident
set, no TTL, no capacity, and no eviction -- every check keeps only its top-1
hit, independent of whatever hit the previous fragment's check. Do not carry
the protocol TTL property across; its absence is a deliberate schema
statement, not an oversight.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import subprocess
import sys
import urllib.error
import urllib.request

ANCHOR_CLASS = "GuardrailAnchor"
INSTRUCTION_CLASS = "GuardrailInstruction"

# Bands from the design note (§9, `disha/guardrail_check.go` constants).
# Deliberately NOT the same as --threshold: --threshold only controls the
# "would qualify" summary line, while these two are the real product bands
# probe results are judged against.
JUDGE_THRESHOLD = 0.55  # TEST-ONLY, see disha/guardrail_record.go
INTERRUPT_THRESHOLD = 0.85

# Single source of truth for the class definitions: the same files you can
# POST straight to /v1/schema with curl. Do not inline a second copy here.
SCHEMA_DIR = pathlib.Path(__file__).resolve().parent / "weaviate"
CLASS_FILES = {
    INSTRUCTION_CLASS: SCHEMA_DIR / "guardrail_instruction.class.json",
    ANCHOR_CLASS: SCHEMA_DIR / "guardrail_anchor.class.json",
}


def load_class_def(name: str, tei_url: str) -> dict:
    path = CLASS_FILES[name]
    try:
        definition = json.loads(path.read_text())
    except FileNotFoundError:
        die(f"missing class definition {path}")
    except json.JSONDecodeError as exc:
        die(f"invalid JSON in {path}: {exc}")
    # The file carries the hosted-models TEI URL literally so `curl -d @file`
    # works with no preprocessing; --tei-url overrides it for other namespaces.
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

# Fixture guardrails. Anchors are phrased as things DISHA (the assistant)
# might say, not things the user says -- guardrail checks query on a fragment
# of the assistant's own generated response (design note §5.2), unlike
# protocol retrieval, which queries on the user's situation. Deliberately
# shaped to exercise every branch:
#   - several anchors per instruction -> the dedupe-by-instruction-id path
#   - four distinct instructions      -> a real top-1-per-fragment spread
#   - Hinglish anchors                -> the real query distribution
FIXTURES: list[dict] = [
    {
        "title": "Advising a user to stop or change prescribed medication",
        "documentVersionPath": "Voice_Agent/guardrail_book/medication_stop_advice/v/1",
        "instructionText": (
            "Never advise, suggest, or imply that the user should stop, reduce, pause, or "
            "change the dosage of any prescribed medication. Medication decisions belong "
            "solely to the prescribing doctor. If medication comes up, redirect the user to "
            "consult their doctor and flag the conversation for a health-coach callback."
        ),
        "anchors": [
            "You can stop taking your blood pressure medicine for now",
            "I think you should reduce your metformin dose to half",
            "It's fine to skip today's tablet since you're feeling better",
            "आप अपनी दवा बंद कर सकते हैं, कोई बात नहीं",
        ],
    },
    {
        "title": "Giving a specific clinical dosage",
        "documentVersionPath": "Voice_Agent/guardrail_book/clinical_dosage/v/1",
        "instructionText": (
            "Never state a specific medication dosage, quantity, or frequency (milligrams, "
            "millilitres, tablets per day) to the user. Dosage is determined only by their "
            "treating doctor based on individual factors the assistant does not have."
        ),
        "anchors": [
            "You should take 500mg of metformin twice a day",
            "Take two tablets of paracetamol every six hours",
            "The right dose for you is 10 milligrams before bedtime",
            "रोज सुबह और शाम 500mg की गोली लीजिए",
        ],
    },
    {
        "title": "Diagnosing a medical condition",
        "documentVersionPath": "Voice_Agent/guardrail_book/diagnosis/v/1",
        "instructionText": (
            "Never diagnose a medical condition or tell the user what disease or disorder "
            "they have. Only a doctor can diagnose. Describe symptoms neutrally and recommend "
            "a medical consultation instead of naming a condition."
        ),
        "anchors": [
            "Based on what you're describing, you have type 2 diabetes",
            "It sounds like you're suffering from PCOS",
            "This is clearly a thyroid problem",
            "आपको शायद डायबिटीज़ है, ये लक्षण उसी के हैं",
        ],
    },
    {
        "title": "Promising a guaranteed outcome",
        "documentVersionPath": "Voice_Agent/guardrail_book/guaranteed_outcome/v/1",
        "instructionText": (
            "Never promise a guaranteed outcome, specific timeline, or exact result from "
            "following the plan, such as an exact weight-loss number or a guaranteed cure. "
            "Speak only in terms of likely trends, never guarantees."
        ),
        "anchors": [
            "I guarantee you will lose 5 kilos in two weeks if you follow this",
            "This plan will definitely cure your acidity completely",
            "You are 100% guaranteed to see results by next month",
            "अगर आप ये प्लान फॉलो करेंगे तो पक्का दो हफ्ते में वज़न कम हो जाएगा",
        ],
    },
]

# Probe fragments in the shape a real check actually queries: one completed
# clause/sentence of the ASSISTANT's own generated text (design note §5.2 --
# "the query text is the fragment, never the accumulation", and it is never a
# Disha+User pair the way protocol retrieval's query is). A mix of near-exact
# restatements of the fixture anchors (expect interrupt-band hits), adjacent
# but safe phrasings (the false-positive risk the design note flags), and
# unrelated small talk (the similarity floor).
PROBE_QUERIES: list[str] = [
    "You can stop your BP tablets for a few days and see how you feel",
    "It might be worth asking your doctor before making any medication changes",
    "Take one tablet of paracetamol if the pain continues",
    "It sounds like this could be related to your thyroid levels",
    "I completely understand, let's talk about your diet plan instead",
    "You will definitely lose weight if you stick to this for a month",
    "That's a great question, let's discuss it further",
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


def vectorizer_contract_problems(cls: dict, tei_url: str, class_name: str, vectorized_props: list[str]) -> list[str]:
    """Shared contract check -- both GuardrailAnchor and GuardrailInstruction
    carry the full text2vec-huggingface contract, unlike protocols where only
    ProtocolAnchor is vectorized."""
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
    for prop_name in vectorized_props:
        if prop_name not in props:
            problems.append(f"property {prop_name} missing")
            continue
        pm = (props[prop_name].get("moduleConfig") or {}).get("text2vec-huggingface") or {}
        if pm.get("vectorizePropertyName") is not False:
            problems.append(f"{prop_name}.moduleConfig.text2vec-huggingface.vectorizePropertyName must be false")
        if pm.get("skip") is True:
            problems.append(f"{prop_name} is marked skip=true, so it would not be vectorized")
    return problems


def anchor_contract_problems(cls: dict, tei_url: str) -> list[str]:
    problems = vectorizer_contract_problems(cls, tei_url, ANCHOR_CLASS, ["anchorText"])
    props = {p["name"]: p for p in cls.get("properties") or []}
    if "answeredBy" not in props:
        problems.append("cross-reference property answeredBy missing")
    elif props["answeredBy"].get("dataType") != [INSTRUCTION_CLASS]:
        problems.append(f'answeredBy dataType is {props["answeredBy"].get("dataType")!r}')
    return problems


def instruction_contract_problems(cls: dict, tei_url: str) -> list[str]:
    problems = vectorizer_contract_problems(cls, tei_url, INSTRUCTION_CLASS, ["instructionText"])
    props = {p["name"]: p for p in cls.get("properties") or []}
    if "turnsThresholdCount" in props:
        problems.append(
            "turnsThresholdCount is present on GuardrailInstruction -- that property belongs only "
            "to ProtocolInstruction; guardrails have no resident set/TTL"
        )
    for prop_name in ("isProduction", "isStaging"):
        if prop_name not in props:
            problems.append(f"property {prop_name} missing")
            continue
        if props[prop_name].get("indexFilterable") is not True:
            problems.append(f"{prop_name}.indexFilterable must be true (it is filtered on at query time)")
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
    if instruction is not None:
        problems = instruction_contract_problems(instruction, tei_url)
        if problems:
            print(f"  VERDICT: {INSTRUCTION_CLASS} violates the collection contract:")
            for problem in problems:
                print(f"    - {problem}")
            print(f"  Fix with --recreate-instruction-class (a class vectorizer cannot be changed in place).")
        else:
            print(f"  VERDICT: {INSTRUCTION_CLASS} satisfies the collection contract.")
    if anchor is not None:
        problems = anchor_contract_problems(anchor, tei_url)
        if problems:
            print(f"  VERDICT: {ANCHOR_CLASS} violates the collection contract:")
            for problem in problems:
                print(f"    - {problem}")
            print(f"  Fix with --recreate-anchor-class (a class vectorizer cannot be changed in place).")
        else:
            print(f"  VERDICT: {ANCHOR_CLASS} satisfies the collection contract.")
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


def do_recreate_classes(
    url: str, headers: dict, tei_url: str, force: bool, recreate_anchor: bool, recreate_instruction: bool
) -> None:
    print("=== recreate classes ===")
    # Anchor first: it holds the cross-reference INTO the instruction class, so
    # dropping the target while the referrer still exists is asking for trouble.
    if recreate_anchor:
        delete_class(url, headers, ANCHOR_CLASS, force)
    if recreate_instruction:
        delete_class(url, headers, INSTRUCTION_CLASS, force)
        create_class(url, headers, INSTRUCTION_CLASS, tei_url)
    elif recreate_anchor and fetch_class(url, headers, INSTRUCTION_CLASS) is None:
        die(
            f"{INSTRUCTION_CLASS} does not exist and is the target of "
            f"{ANCHOR_CLASS}.answeredBy; pass --recreate-instruction-class too"
        )
    if recreate_anchor:
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
            # BOTH flags on purpose, same reasoning as the protocol script:
            # there is a single Weaviate instance, and vago picks its filter
            # field from ENVIRONMENT -- a staging worker filters isStaging, a
            # prod worker filters isProduction. Seeding only one flag would
            # make the other environment retrieve nothing, with no error to
            # notice.
            "isStaging": True,
            "isProduction": True,
        }
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
            f"anchors={len(fixture['anchors'])}"
        )
    print(f"  seeded {len(FIXTURES)} instructions / {anchor_total} anchors")


# ----------------------------------------------------------------------- probe


def do_probe(url: str, headers: dict, limit: int, threshold: float) -> None:
    print("=== probe (threshold calibration data) ===")
    print(
        f"  bands: below {JUDGE_THRESHOLD} = nothing | "
        f"{JUDGE_THRESHOLD}-{INTERRUPT_THRESHOLD} = judge | "
        f"above {INTERRUPT_THRESHOLD} = interrupt   [similarity = 1 - distance]"
    )
    print(f"  (--threshold {threshold} only affects the 'would qualify' summary line below)")
    all_scores: list[float] = []
    tops: list[float] = []
    for query in PROBE_QUERIES:
        gql = (
            f"{{ Get {{ {ANCHOR_CLASS}("
            f"nearText: {{ concepts: {json.dumps([query])} }} "
            f'where: {{ path: ["answeredBy","{INSTRUCTION_CLASS}","isStaging"], '
            "operator: Equal, valueBoolean: true } "
            f"limit: {limit}"
            ") { anchorText answeredBy { ... on "
            f"{INSTRUCTION_CLASS} {{ title documentVersionPath _additional {{ id }} }} }} "
            "_additional { id distance } } } }"
        )
        data = graphql(url, headers, gql)
        hits = data["Get"][ANCHOR_CLASS] or []
        print(f"\n  query: {query[:88]!r}")
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
                    "anchor": hit["anchorText"],
                }
        ranked = sorted(best.values(), key=lambda item: -item["similarity"])
        tops.append(ranked[0]["similarity"])
        for item in ranked:
            all_scores.append(item["similarity"])
            if item["similarity"] > INTERRUPT_THRESHOLD:
                band = "INTERRUPT"
            elif item["similarity"] >= JUDGE_THRESHOLD:
                band = "judge    "
            else:
                band = "below    "
            print(
                f"    [{band}] sim={item['similarity']:.4f}  "
                f"{item['title'][:44]!r}  <- {item['anchor'][:44]!r}"
            )
        print(f"    deduped {len(hits)} anchors -> {len(ranked)} distinct guardrails")
    if tops:
        tops.sort()
        print(
            f"\n  top-hit similarity across {len(tops)} queries: "
            f"min={tops[0]:.4f} median={statistics.median(tops):.4f} max={tops[-1]:.4f}"
        )
        passing = sum(1 for value in tops if value >= threshold)
        print(f"  would qualify at {threshold}: {passing}/{len(tops)} queries")
    if all_scores:
        all_scores.sort()
        print(
            f"  ALL candidate similarities across {len(all_scores)} hits (pre-top-1, post-dedupe): "
            f"min={all_scores[0]:.4f} median={statistics.median(all_scores):.4f} max={all_scores[-1]:.4f}"
        )
        print(
            f"  Bands in force: judge >= {JUDGE_THRESHOLD}, interrupt > {INTERRUPT_THRESHOLD}. "
            "These were set from a fixture run measuring true positives at "
            "0.8892/0.8277/0.7768/0.7491 against true negatives at 0.6525/0.5692/0.4654 "
            "-- an empty gap between 0.6525 and 0.7491, which is where 0.70 sits. "
            "Two things to watch as the real corpus grows: whether any true positive "
            "drops toward the 0.54-0.67 noise floor this model shows even on unrelated "
            "text, and whether anything at all clears the interrupt band (nothing in "
            "the fixture run exceeded 0.8892, so that band was dead at 0.90)."
        )


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
        help="DESTRUCTIVE: drop + recreate GuardrailAnchor from its class file",
    )
    parser.add_argument(
        "--recreate-instruction-class", action="store_true",
        help="DESTRUCTIVE: drop + recreate GuardrailInstruction from its class file",
    )
    parser.add_argument("--force", action="store_true", help="allow deleting a non-empty class")
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--threshold", type=float, default=JUDGE_THRESHOLD)
    args = parser.parse_args()

    tei_url = args.tei_url or DEFAULT_TEI_URL.format(namespace=args.namespace)
    url = args.weaviate_url.rstrip("/")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {load_api_key(args)}"}

    do_inspect(url, headers, tei_url)
    if args.recreate_anchor_class or args.recreate_instruction_class:
        do_recreate_classes(
            url, headers, tei_url, args.force,
            recreate_anchor=args.recreate_anchor_class,
            recreate_instruction=args.recreate_instruction_class,
        )
    if args.seed:
        do_seed(url, headers)
    if args.probe:
        do_probe(url, headers, args.limit, args.threshold)
    if not (args.recreate_anchor_class or args.recreate_instruction_class or args.seed or args.probe):
        print("\n(read-only run; pass --recreate-anchor-class / --recreate-instruction-class / --seed / --probe to act)")


if __name__ == "__main__":
    main()
