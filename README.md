# CLP Blog Collaboration Workspace

This repository contains the working materials for two related articles about CLP, schema-less data, and Presto Native.

## Directories

- [`presto-blog/`](presto-blog/)
  - Presto community article focused on the external native connector, schema-less SQL access, and reusable connector-development lessons.
- [`uber-blog/`](uber-blog/)
  - Uber Engineering CLP Part III article focused on Uber's logging architecture, production deployment, and the Uber–YScope collaboration.

Each directory follows the same core convention:

1. **Knowledge** (`*-knowledge.md`) — detailed technical context, facts, attribution, references, and unresolved questions.
2. **Outline** (`*-outline.md`) — structural record of the article: what each section is, deliberate omissions, figure and footnote specs, and open pre-publication items.
3. **Draft** (`*-draft.md`) — the publication-style article itself.

`presto-blog/` adds [`presto-clp-contract.md`](presto-blog/presto-clp-contract.md), the editorial contract behind its outline: narrative budget, per-section contracts, evidence labels, and safeguards.

`uber-blog/` adds two more: [`clp-part-iii-pitch.md`](uber-blog/clp-part-iii-pitch.md), holding the editorial pitch, tone, restrictions, and a reader-reception table, and [`social-media.md`](uber-blog/social-media.md) for launch copy.

## Recommended Workflow

1. Record new facts and corrections in the appropriate knowledge document.
2. Update the outline when the story or scope changes.
3. Revise the draft after the outline reflects the intended narrative.
4. Resolve verification notes, placeholders, and public-versus-internal boundaries before publication.

