# Full-text search

```cypher
CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body);

CALL db.index.fulltext.queryNodes('docs', 'graph databases')
YIELD node, score
RETURN node.title AS title, score
ORDER BY score DESC;
```

## Why not `CONTAINS`

`CONTAINS` was the only text search here, and it is a substring match. Four
separate problems, of which only the last is about speed:

| | `CONTAINS 'graph'` | this index |
|---|---|---|
| `Graphs` | misses it | finds it — case-folded and stemmed |
| `polygraph` | **matches it** | does not; `graph` is a different word |
| ranking | none | BM25 |
| cost | scans every node | term lookup |

## Scoring

BM25 with the standard defaults, `k1 = 1.2` and `b = 0.75`:

```
score(D, Q) = Σ  IDF(q) · f(q,D)·(k1+1) / (f(q,D) + k1·(1 - b + b·|D|/avgdl))
```

Two corrections `CONTAINS` has no notion of. A term appearing in every document
distinguishes nothing, so its weight falls to zero (the IDF is **floored** at
zero — unfloored, a very common term scores negative and a document is pushed
*down* the ranking for containing a word you searched for). And a long document
should not outrank a short one merely by holding more words, which is what the
`|D|/avgdl` term corrects.

## Query syntax, all of it

| Form | Means |
|---|---|
| `graph database` | either term; the scores add |
| `"shortest path"` | those terms **adjacent, in that order** |
| `graph*` | any term starting with the stemmed prefix |

No boolean operators and no field selectors. An index covering one property has
nothing to select between, and accepting `AND` while silently treating it as a
term to search for would be worse than not accepting it.

A phrase requires adjacency, not co-occurrence — token positions are stored for
exactly this. A phrase query that only checked both words were present would
match a document mentioning them a page apart, and would look right in every
small test.

An unclosed quote searches the words individually rather than returning
nothing, because it is a typo and a silent empty result is the least useful
possible response to one.

## The stemmer is small, and this is what it does

**It is not a Porter stemmer.** It strips a listed set of English suffixes —
`-ies`, `-ing`, `-ed`, `-es`, `-s` — with a minimum stem length, and nothing
else:

| Conflated | Not conflated |
|---|---|
| `graph` / `graphs` / `graphing` | `analyse` / `analysis` |
| `match` / `matched` | irregular verbs (`run` / `ran`) |
| `study` / `studies` | anything non-English |

The minimum stem length is what stops `is` becoming `i` and `bed` becoming
`b` — an over-eager stemmer collapses unrelated words into one token and
returns documents with nothing in common.

The rule set is fifteen lines in `src/index/fulltext.rs::stem`. What actually
has to hold is that **a query is stemmed by the same function that stemmed the
document**; a stemmer applied to one side only quietly stops matching the very
words it was added to match, and reports nothing while doing it.

**There is no stopword list**, deliberately. BM25 already gives a word that
appears everywhere a weight of zero, and removing such words outright would
break every phrase containing one — `"shortest path to the node"` has two.

## Maintenance

The index is maintained on the same choke point as the property and vector
indexes (`GraphStore::apply_property_set`), so:

- `CREATE FULLTEXT INDEX` **backfills** from the nodes already present. The
  normal order for a load is data first, DDL second, and an index that
  registered without populating would return nothing for every search with no
  error to say why.
- A `SET` that changes the property **replaces** the document's terms. An
  append-only index would keep matching words the document no longer contains,
  and nothing about the result would say so.
- A property that stops being a string is removed from the index rather than
  left behind.

## What this does not do

- **Relationship properties are not indexed.** Nodes only.
- **`ON EACH [n.a, n.b]` creates one index per property**, named `docs` and
  `docs.b`. Neo4j's is a single index over two fields, which differs in how
  scores combine across them. Accepting the syntax with the difference written
  down here seemed better than refusing it; if you need Neo4j's semantics, the
  scores are not comparable.
- **Nothing is persisted.** The index is rebuilt from the graph, so it survives
  a restart only because the DDL is replayed — re-run `CREATE FULLTEXT INDEX`
  after loading a snapshot.
- **No language configuration.** The tokeniser splits on non-alphanumerics and
  the stemmer is English. CJK text tokenises into one term per run of
  characters, which is not useful.
- **`SHOW INDEXES` does not list full-text indexes yet.** Until it does, the
  error from searching a name that does not exist lists the names that do,
  which is the only discovery path.
