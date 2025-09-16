# whatwouldyoudoifyouwonthelottery.com

**One-command static data refinery:** fetch → parse → publish → verify → ship (GitHub Pages).

This repo powers **https://whatwouldyoudoifyouwonthelottery.com**.  
Runs locally with a single script and in the cloud via **GitHub Actions** on a daily schedule.

---

## TL;DR

```bash
# first time
chmod +x scripts/run.sh scripts/verify.sh

# end-to-end build
./scripts/run.sh

# sanity checks
./scripts/verify.sh
Pushing to main redeploys via GitHub Pages. A daily workflow also runs on a cron and commits new data/posts.

Planet model (how this repo scales)
Each site/data domain is a planet sharing the same refinery pipeline. Customize inputs + branding; keep the pipeline the same.

Targets: layer1/targets.txt

Adapters: layer2/adapters/*.yaml

Transforms / vision: layer3/*

Branding: assets/*

Public outputs: data/, blog/, feeds/, index.html, planets.html

Copy this layout for new planets; change targets/config/logo and you’re live.

Daily automation (GitHub Actions)
Workflow: .github/workflows/daily.yaml

It performs:

Snap targets

Fetch JSON from sources

Build datasets, blog, and feeds

Verify data/latest.json is present and non-empty

Commit & push only if something changed

Triggers

Daily at 10:07 UTC

Manual Run workflow

On push to key paths (scripts, data, blog, feeds, layer1–3, config, the workflow itself)

Recommended push secret
Some orgs set GITHUB_TOKEN to read-only. Add a classic PAT with repo scope so the workflow can push:

Repo → Settings → Secrets and variables → Actions → New repository secret

Name: ACTIONS_TOKEN

Value: your classic PAT (scope: repo)

The workflow detects ACTIONS_TOKEN and uses it automatically.

Optional failure email
Add these secrets to receive an email when a run fails:

SMTP_SERVER, SMTP_PORT (e.g., 587), SMTP_USERNAME, SMTP_PASSWORD

FROM_EMAIL, TO_EMAIL

Local development
bash
Copy code
# install node deps (if used)
npm install

# end-to-end build (snap → fetch → build → write outputs)
./scripts/run.sh

# only fetch API JSON
node scripts/fetch_json.js

# rebuild blog & feeds from current data
python3 scripts/build_lottery_blog.py

# quick verification (checks latest.json, basic invariants)
./scripts/verify.sh
Outputs appear in:

Datasets: data/latest.json + date-stamped files under data/

Blog posts: blog/

Feeds: feeds/ (RSS/JSON)

Customize this planet
Targets → edit layer1/targets.txt

Per-source adapters → edit/create layer2/adapters/*.yaml

Branding → update assets/logo.svg, assets/styles.css

Site config → config/planet.yaml

Use relative asset paths in HTML/CSS/JS (e.g., ./assets/...), not /assets/....

Repository map
bash
Copy code
assets/               # logos, css, images
blog/                 # generated blog posts (date-stamped)
data/                 # generated datasets (latest.json + history)
feeds/                # generated RSS/JSON feeds
config/planet.yaml    # planet/site config
layer1/               # targets list and run logs
layer2/adapters/      # source-specific adapter configs
layer3/               # higher-level transforms/vision
scripts/              # snap.js, fetch_json.js, build_lottery_blog.py, verify.sh, run.sh
.github/workflows/    # daily.yaml (automation)
index.html, planets.html, license.html, robots.txt, sitemap.xml
Deployment
Branch: main

Hosting: GitHub Pages (Repo → Settings → Pages → Source: Deploy from a branch, main / root)

Custom domain: whatwouldyoudoifyouwonthelottery.com

A records (root): 185.199.108.153, 185.199.109.153, 185.199.110.153, 185.199.111.153

CNAME (www): www → hypnodatalabs.github.io

Enable Enforce HTTPS when available

Troubleshooting
Action didn’t push → ensure ACTIONS_TOKEN secret exists (classic PAT with repo scope).

Verify step failed → check data/latest.json exists and has a non-empty records array.

Pages didn’t update → confirm a new commit exists on main and Pages targets main (root).

Assets missing → ensure paths use ./assets/... and files live at repo root.

License
MIT. Credit appreciated.

Copy code
