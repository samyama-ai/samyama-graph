// The README's vector-search example: three papers with 3-dimensional embeddings.
// Real embeddings are wider (a sentence model gives 384); three keep the example
// on one line, and the index in the README is declared to match.
CREATE (:Paper {title: 'Graph databases', embedding: [0.1, 0.2, 0.3]}),
       (:Paper {title: 'Vector search', embedding: [0.3, 0.2, 0.1]}),
       (:Paper {title: 'Query planning', embedding: [0.9, 0.1, 0.0]})
