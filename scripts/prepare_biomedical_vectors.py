#!/usr/bin/env python3
"""Embed a biomedical source for `cargo bench --bench biomedical_vectors` (#1281).

Two levels, each embedded with one open sentence-transformer model:

  concept   one vector per concept (name + synonyms), typed as one of
            Drug, Disease, Gene, Protein, Target, Pathway, MeSHTerm,
            ClinicalCondition -- or any type your TSV names
  article   one vector per article (title + abstract), typed Article

Sources (any combination; each level needs at least one):

  --mesh-xml desc2025.xml[.gz]   MeSH descriptors (https://nlm.nih.gov/databases/download/mesh.html).
                                 Typed by tree number, see MESH_TYPES below.
  --concepts-tsv FILE            `type<TAB>text` per line, e.g. exported from the
                                 AACT- or Hetionet-derived graph.
  --pubmed-xml FILE.xml[.gz]     PubMed baseline files (repeatable). Articles
                                 without an abstract are skipped.
  --articles-tsv FILE            `type<TAB>text` or `text` per line.

Writes, per level:

    <out>/<level>/base.fvecs        little-endian int32 dim, then dim float32, per vector
    <out>/<level>/base_types.txt    one type per base vector, same order
    <out>/<level>/query.fvecs       held-out vectors used as queries
    <out>/<level>/query_types.txt   their types (informational)
    <out>/metric.txt                cosine (embeddings are L2-normalised)
    <out>/model.txt                 the model name

Usage:
    pip install numpy sentence-transformers
    python3 scripts/prepare_biomedical_vectors.py --mesh-xml desc2025.xml \\
        --pubmed-xml pubmed25n0001.xml.gz --out data/biomedical-vectors
    cargo bench --bench biomedical_vectors -- --data data/biomedical-vectors

`--texts-only` writes `<out>/<level>/texts.tsv` (type, text) instead of
embedding, to check what would be embedded without installing a model.
"""

import argparse
import gzip
import os
import random
import re
import sys
import xml.etree.ElementTree as ET

DEFAULT_MODEL = "NeuML/pubmedbert-base-embeddings"  # 768-d, Apache-2.0

# MeSH tree-number prefixes to concept types, most specific first; the first
# rule matching any of a descriptor's tree numbers wins, and a descriptor
# matching none is a MeSHTerm. A heuristic, not an ontology mapping: MeSH
# has no "Target" or "Pathway" branch, so receptors stand in for targets and
# metabolic networks / signal transduction for pathways.
MESH_TYPES = [
    ("D12.776.543", "Target"),  # Receptors
    ("D12.776", "Protein"),  # Proteins
    ("D08.811", "Protein"),  # Enzymes
    ("G05.360.340", "Gene"),  # Genes
    ("G03.493", "Pathway"),  # Metabolic Networks and Pathways
    ("G04.835", "Pathway"),  # Signal Transduction
    ("C23", "ClinicalCondition"),  # Pathological Conditions, Signs and Symptoms
    ("F03", "Disease"),  # Mental Disorders
    ("C", "Disease"),  # Diseases
    ("D", "Drug"),  # Chemicals and Drugs
]

MAX_SYNONYMS = 10
TYPE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def opener(path):
    return gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")


def mesh_type(tree_numbers):
    for prefix, kind in MESH_TYPES:
        for tn in tree_numbers:
            if tn == prefix or tn.startswith(prefix + ".") or (
                len(prefix) == 1 and tn.startswith(prefix)
            ):
                return kind
    return "MeSHTerm"


def read_mesh(path, limit):
    """(type, "name; synonym; ...") per MeSH descriptor."""
    out = []
    with opener(path) as f:
        for _, el in ET.iterparse(f, events=("end",)):
            if el.tag != "DescriptorRecord":
                continue
            name = el.findtext("DescriptorName/String") or ""
            trees = [t.text or "" for t in el.findall("TreeNumberList/TreeNumber")]
            synonyms = []
            for term in el.iterfind("ConceptList/Concept/TermList/Term/String"):
                s = (term.text or "").strip()
                if s and s != name and s not in synonyms:
                    synonyms.append(s)
            el.clear()
            if not name:
                continue
            text = "; ".join([name] + synonyms[:MAX_SYNONYMS])
            out.append((mesh_type(trees), text))
            if limit and len(out) >= limit:
                break
    return out


def read_pubmed(paths, limit):
    """("Article", "title abstract") per PubMed article with an abstract."""
    out = []
    for path in paths:
        with opener(path) as f:
            for _, el in ET.iterparse(f, events=("end",)):
                if el.tag != "PubmedArticle":
                    continue
                art = el.find("MedlineCitation/Article")
                title = "".join(art.find("ArticleTitle").itertext()).strip() if art is not None and art.find("ArticleTitle") is not None else ""
                parts = []
                if art is not None:
                    for a in art.iterfind("Abstract/AbstractText"):
                        parts.append("".join(a.itertext()).strip())
                el.clear()
                abstract = " ".join(p for p in parts if p)
                if not abstract:
                    continue
                out.append(("Article", f"{title} {abstract}".strip()))
                if limit and len(out) >= limit:
                    return out
    return out


def read_tsv(path, default_type, limit):
    out = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if "\t" in line:
                kind, text = line.split("\t", 1)
            elif default_type:
                kind, text = default_type, line
            else:
                sys.exit(f"{path}:{n}: expected `type<TAB>text`")
            kind = kind.strip()
            if not TYPE_NAME.match(kind):
                sys.exit(f"{path}:{n}: `{kind}` is not a type name (letters, digits, _)")
            out.append((kind, text.strip()))
            if limit and len(out) >= limit:
                break
    return out


def write_fvecs(path, rows):
    import numpy as np

    rows = np.ascontiguousarray(rows, dtype="<f4")
    n, d = rows.shape
    out = np.empty((n, d + 1), dtype="<i4")
    out[:, 0] = d
    out[:, 1:] = rows.view("<i4")
    out.tofile(path)


def split(items, queries, seed):
    """Hold out `queries` items, chosen by seed, as the query set."""
    order = list(range(len(items)))
    random.Random(seed).shuffle(order)
    held = set(order[:queries])
    base = [x for i, x in enumerate(items) if i not in held]
    query = [items[i] for i in sorted(held)]
    return base, query


def embed(model, texts, batch_size):
    vecs = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
        convert_to_numpy=True,
    )
    return vecs.astype("float32")


def write_level(out, level, items, args, model):
    if len(items) <= args.queries:
        sys.exit(f"{level}: {len(items)} items is not more than --queries {args.queries}")
    d = os.path.join(out, level)
    os.makedirs(d, exist_ok=True)
    base, query = split(items, args.queries, args.seed)
    counts = {}
    for kind, _ in base:
        counts[kind] = counts.get(kind, 0) + 1
    print(f"{level}: {len(base)} base, {len(query)} queries; "
          + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    if model is None:
        with open(os.path.join(d, "texts.tsv"), "w", encoding="utf-8") as f:
            for kind, text in base:
                f.write(f"{kind}\t{text}\n")
        return
    for name, rows in (("base", base), ("query", query)):
        write_fvecs(os.path.join(d, f"{name}.fvecs"), embed(model, [t for _, t in rows], args.batch_size))
        with open(os.path.join(d, f"{name}_types.txt"), "w") as f:
            f.writelines(f"{kind}\n" for kind, _ in rows)
    print(f"  wrote {d}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="data/biomedical-vectors")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--mesh-xml")
    p.add_argument("--concepts-tsv")
    p.add_argument("--pubmed-xml", action="append", default=[])
    p.add_argument("--articles-tsv")
    p.add_argument("--max-concepts", type=int, default=0, help="0 = all")
    p.add_argument("--max-articles", type=int, default=0, help="0 = all")
    p.add_argument("--queries", type=int, default=1000, help="held out per level")
    p.add_argument("--seed", type=int, default=1281)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--texts-only", action="store_true")
    args = p.parse_args()

    concepts = []
    if args.mesh_xml:
        concepts += read_mesh(args.mesh_xml, args.max_concepts)
    if args.concepts_tsv:
        concepts += read_tsv(args.concepts_tsv, None, args.max_concepts)
    if args.max_concepts:
        concepts = concepts[: args.max_concepts]
    articles = []
    if args.pubmed_xml:
        articles += read_pubmed(args.pubmed_xml, args.max_articles)
    if args.articles_tsv:
        articles += read_tsv(args.articles_tsv, "Article", args.max_articles)
    if args.max_articles:
        articles = articles[: args.max_articles]
    if not concepts and not articles:
        sys.exit("nothing to embed: give --mesh-xml/--concepts-tsv and/or --pubmed-xml/--articles-tsv")

    model = None
    if not args.texts_only:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            sys.exit("sentence-transformers is required: pip install numpy sentence-transformers")
        model = SentenceTransformer(args.model)
    os.makedirs(args.out, exist_ok=True)
    if concepts:
        write_level(args.out, "concept", concepts, args, model)
    if articles:
        write_level(args.out, "article", articles, args, model)
    if model is not None:
        with open(os.path.join(args.out, "metric.txt"), "w") as f:
            f.write("cosine\n")
        with open(os.path.join(args.out, "model.txt"), "w") as f:
            f.write(args.model + "\n")


if __name__ == "__main__":
    main()
