# Lively 🌻

Job liveness and freshness service. Answers "is this job still real and open right now?" for any job, and keeps that answer fresh at scale.

## Status

- [x] Part 1: foundation and data
- [ ] Part 2: liveness detection engine
- [ ] Part 4: adaptive scheduler and reliable infrastructure
- [ ] Part 3: ghost and repost detection
- [ ] Part 5: API, frontend and demo

## Architecture

TODO

## Quick start

    docker compose up -d --build
    docker compose exec collector python -m lively.cli seed seeds/boards.csv
    docker compose exec collector python -m lively.cli discover seeds/candidates.txt
    docker compose exec collector python -m lively.cli sync
    docker compose exec collector python -m lively.cli stats

## Data sources

| ATS        | Endpoint                                                  |
|------------|-----------------------------------------------------------|
| Greenhouse | boards-api.greenhouse.io/v1/boards/{slug}/jobs            |
| Lever      | api.lever.co/v0/postings/{slug}                           |
| Ashby      | api.ashbyhq.com/posting-api/job-board/{slug}              |
| Workable   | apply.workable.com/api/v1/widget/accounts/{slug}          |

## Job lifecycle

new, live, suspect, closed, reposted

## Benchmarks

TODO

## Known limits

TODO

## What's next

TODO
