# AI Tool Usage

## Tools I Used

- **Claude Code (Anthropic, Sonnet)** — Started with this model to review the assignment, prepare the environment, and create the file templates. Later we went over each needed pipeline according to the instructions and coded it together. The cycle was like this: model to create the code, me to ask questions related to the content and implementation, questions to make sure requirements were fulfilled.
- **Claude Desktop (Anthropic, Opus 4.7)** — After creating the pipelines and testing a few of them, I gave the code and requirements to this model to review the code. I got a few inputs and suggestions and we had a discussion about what should be fixed and what should be mentioned in the decisions doc in case I didn't want to handle it. We also had a discussion about trade-offs. For some of the fixes I also consulted the Sonnet model to have a second opinion.
- **Cursor (GPT-5.3-codex)** — used for an extra review pass once the project was mostly stable, mainly to catch things the Claude models might have missed. A few of the items it raised ended up being real fixes.

Sonnet did most of the writing; Opus 4.7 did most of the deeper review and trade-off discussions; Sonnet again as a second opinion when I got stuck between answers; Cursor as a final independent set of eyes.

---

## What Helped Most

- **Building the structure and the initial code.** Basically the model was the one to initiate all the files and the code. Once it was done I went over it, asked questions and raised issues — but 90% of the boilerplate code was done by the model.
- **Reviewing DECISIONS.md for staleness.** After several rounds of code changes, I asked Claude to read DECISIONS end-to-end and flag claims that no longer matched the code.
- **Trade-off framing and consulting.** Discussing with the model why it chose a specific solution, what could be done in another way and what to change. The final decision was mine. Examples: using hashing at the beginning, how to handle duplication, discussing whether to save the log data in a DB or files, asking the model to suggest a stack I'm not familiar with.

---

## What I Had to Fix

- **Streaming.** The code did not handle streaming properly at the beginning — for the JSON file, it was reading the entire file. Also, when checking if the file is not corrupted, we decided to take a small portion of the file for an initial check.
- **Duplication.** We had a long journey with the duplication issue. I first asked the model whether it had implemented deduplication; since it had not, we decided to implement it on both file name and file content. It was not working properly: deleted files were still saved in the DB and were not processed. Then I understood that since the same file can have different processing events, it will result in different outputs. As I wrote in the decisions doc, I decided to skip deduplication for now, while I can suggest a different solution if needed.
- **CSV validation crashing on header-only files.** A CSV with just a header (no data rows) is valid, but the corruption check treated it as broken and rejected it. Wrong error for a perfectly fine input.
- **JSON code did not apply filtering** — came up in tests. The transform step's CSV path handled `filter_rows`, but the JSON path quietly ignored it. Every row passed through. Caught only after I started writing the test with a known expected row count.
- **Using a unique id for the job and another one for the output file** — it made more sense to use the same id, so the file on disk, the Job row, and the API response all share one UUID.
- **Commit before enqueue.** The first version enqueued the job to Redis *before* the DB commit. Under load, a fast worker could pick up the message and look up the Job row before the upload's transaction committed — see "Job not found", silently exit, file orphaned. The independent review caught it. Switched to commit-then-enqueue, with a sweeper that re-enqueues stuck PENDING jobs if the enqueue itself fails. Classic "looks fine in tests, breaks under real concurrency" bug.
- **Worker's exception handler crashing on its own error path.** If the very first DB lookup at the start of the worker failed, the exception handler referenced a variable that was never set — so the handler itself crashed and the real error was lost. One-line fix (initialize the variable before the try block), but the kind of subtle thing AI happily writes and tests don't catch unless you exercise the early-failure path.

---

## What AI Struggled With


- **Over-suggesting class-based abstractions.** As a data engineer — although the task is for a backend engineer — I still need to understand the code structure and discuss it in the next interview. The first code was written with classes, so I asked the model to make it simpler, in a way that the code would be like something I can write myself.
- **Different models, different suggestions.** Like a discussion in a room full of people: given different models the requirements and code, they are never satisfied and always have suggestions for changes and improvements. I went back and forth between the models' suggestions until I picked the solution I decided on (duplication, hashing, compression, UTC time zone). I usually decided to go with the simpler solution at the moment but mention the trade-off in the decisions doc.
- **Big picture.** The model doesn't always see the big picture, which made me ask it whether it changed the decisions file according to a new implementation, or whether it updated code in other relevant places.

---

## Critical Evaluation Process

For every AI suggestion I asked myself:
1. Would I implement it myself? Do I know what the AI is actually suggesting? Is it necessary in terms of efficiency?
2. Is the AI suggestion really needed? It might be nice to have, but AI tends to raise issues, suggestions and fixes just in order to keep working — I actually never got an "it is all OK now" from it.

