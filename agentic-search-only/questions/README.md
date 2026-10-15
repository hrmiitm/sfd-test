# Benchmark question inputs

`questions.jsonl` contains the 100 questions used by this standalone experiment;
`questions.json` is the same question set in formatted JSON. These inputs were
copied byte-for-byte from the original repository question set so this benchmark
can maintain independent inputs. The original copies remain for the vector-DB
benchmark.

References and gold evidence are sent only to the judge. The agent receives the
question text and approved source-file IDs. The shared corpus stays at
`../../data/parsed/` relative to this directory.
