// docs/ALGORITHM-CONVENTIONS.md's contamination example: two benchmark
// questions, one quoted verbatim in a corpus document and one nowhere.
CREATE (:Question {text: 'Which river flows through the capital of France before it reaches the sea?'}),
       (:Question {text: 'How many moons orbit the planet Neptune according to the latest survey?'}),
       (:Doc {text: 'Quiz night. Which river flows through the capital of France before it reaches the sea? The Seine.'}),
       (:Doc {text: 'A recipe: whisk two eggs with flour and bake for twenty minutes.'})
