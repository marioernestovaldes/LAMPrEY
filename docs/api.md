# API

The API is authenticated and project-scoped. Unauthenticated requests are rejected.

There are now two ways to use it:

- preferred: create an API token and send `Authorization: Token <token>`
- fallback: reuse an authenticated web session with CSRF handling

For command-line and scripted use, token authentication is the simpler option.

This authentication setup is preliminary. The current token flow still uses an authenticated web session to mint the first token. Moving forward, the API is expected to migrate toward token-based authentication as the primary and eventually sole API authentication mechanism.

## Authentication

### Preferred: API token

If you have a token, use that token directly for all normal API requests.

```bash
curl \
  -H "Authorization: Token xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
  -X POST \
  http://127.0.0.1:8000/api/projects
```
Here, replace the token value (`xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`) with your actual token and `http://127.0.0.1:8000/api/projects` with the appropriate API endpoint. Otherwise, see the next section for how to bootstrap a token from the web login.

### Bootstrap a token from the current web login

At the moment, the first token is created from an authenticated web session. Use the following sequence of commands in a terminal after you log in to the web app.

Set your local values first:

```bash
BASE="http://127.0.0.1:8000"  # change if the instance is at a different URL
EMAIL="user@email.com"
PASSWORD="123"
```

Fetch the anonymous home page and save the login form:

```bash
rm -f cookies.txt login.html home.html
curl -sS -c cookies.txt "$BASE/" -o login.html
```

Extract the CSRF token from the login form:

```bash
CSRFTOKEN=$(grep -o 'name="csrfmiddlewaretoken" value="[^"]*"' login.html | head -n1 | sed 's/.*value="//;s/"$//')
```

Submit the login form:

```bash
curl -sS \
  -b cookies.txt \
  -c cookies.txt \
  -e "$BASE/" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "username=$EMAIL" \
  --data-urlencode "password=$PASSWORD" \
  --data-urlencode "csrfmiddlewaretoken=$CSRFTOKEN" \
  --data-urlencode "next=/" \
  "$BASE/" > /dev/null
```

Load the authenticated home page and extract a fresh CSRF token:

```bash
curl -sS -b cookies.txt "$BASE/" -o home.html
APITOKEN=$(grep -o 'name="csrfmiddlewaretoken" value="[^"]*"' home.html | head -n1 | sed 's/.*value="//;s/"$//')
echo $APITOKEN
```

At this point you should have a valid CSRF token in the `APITOKEN` variable. You can use that token to create an API token for future requests.

Create or fetch the API token:

```bash
curl -sS \
  -b cookies.txt \
  -H "X-CSRFToken: $APITOKEN" \
  -H "Referer: $BASE/" \
  -X POST \
  "$BASE/api/token"
```

Example response:

```json
{
  "token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "created": true
}
```

To revoke the current user's token:

```bash
curl \
  -b cookies.txt \
  -H "X-CSRFToken: <csrf-token>" \
  -H "Referer: http://127.0.0.1:8000/" \
  -X DELETE \
  http://127.0.0.1:8000/api/token
```

### Fallback: authenticated web session

The session-based flow is still supported. The `curl` examples that use `-b cookies.txt` assume you first authenticate in the web app, then send that authenticated session with each request.

In those examples, `cookies.txt` means “a saved cookie jar file used by `curl`.” It is only a placeholder filename, not a project file.

Notes:

- non-admin users can only access projects they own or belong to
- non-admin users can only mutate their own runs
- uploads require the pipeline UUID (`pid`)
- most read endpoints use project and pipeline slugs

## Quick start with the seeded demo

The seeded demo is the easiest way to try the read-only endpoints end to end.

If the demo is not present yet, create it first:

```bash
make bootstrap-demo
```

That creates a demo project named `Demo Project` with a seeded pipeline named `TMT QC Demo` and three seeded runs. See [Demo data](demo.md) for the full background.

A practical read flow is:

1. call `/api/projects`
2. call `/api/pipelines` with the project slug from step 1
3. call `/api/qc-data`, `/api/protein-names`, or `/api/protein-groups` with the project and pipeline slugs from step 2

The seeded demo is suitable for read endpoints. It is not the right example for uploads.

### `/api/projects`

Returns the projects visible to the authenticated user.

```bash
curl -H "Authorization: Token <token>" -X POST https://example.com/api/projects
```

Example response:

```json
[
  {
    "pk": 1,
    "name": "Demo Project",
    "description": "Seeded demo project for first-run onboarding.",
    "slug": "demo-project"
  }
]
```

### `/api/pipelines`

Returns pipelines in a project visible to the authenticated user.

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project"}' \
  https://example.com/api/pipelines
```

Example response:

```json
[
  {
    "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "slug": "tmt-qc-demo",
    "name": "TMT QC Demo",
    "path_as_str": "/path/to/pipeline"
  }
]
```

The returned `uuid` is the value you pass as `pid` to `/api/upload/raw`.

### `/api/pipeline-uploaders`

Returns the uploaders visible for a pipeline.

This is mainly useful for pipeline-level filtering in the UI and API clients.

Request fields:

- `project`: project slug
- `pipeline`: pipeline slug

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project","pipeline":"tmt-qc-demo"}' \
  https://example.com/api/pipeline-uploaders
```

Example response:

```json
[
  {
    "label": "user@email.com",
    "value": "user@email.com"
  }
]
```

### `/api/upload/raw`

Uploads a new RAW file to an existing pipeline and creates a corresponding run.

Required form fields:

- `pid`: pipeline UUID
- `orig_file`: uploaded `.raw` file

```bash
curl \
  -H 'Authorization: Token <token>' \
  -F 'pid=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' \
  -F 'orig_file=@/path/to/sample.raw' \
  https://example.com/api/upload/raw
```

Behavior notes:

- the pipeline must belong to a project the current user can access
- the demo pipeline is treated as read-only in the web UI and should not be used as an upload target
- repeated uploads with the same displayed filename still create independent runs

### `/api/qc-data`

Returns QC data for a pipeline as a JSON object of column-to-list mappings.

Common request fields:

- `project`: project slug
- `pipeline`: pipeline slug
- `data_range`: number of most recent runs to include
- `columns`: optional list of columns to return

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project","pipeline":"tmt-qc-demo","data_range":3}' \
  https://example.com/api/qc-data
```

The response can include RawTools-derived columns, MaxQuant summary fields, and computed TMT metrics such as:

- `TMT<n>_missing_values`
- `TMT<n>_peptide_count`
- `TMT<n>_protein_group_count`

### `/api/protein-names`

Returns protein-group identifiers, FASTA headers, mean scores, and mean intensities across the selected run set.

Request fields:

- `project`
- `pipeline`
- `data_range`
- `raw_files`: optional list of displayed raw-file names
- `remove_contaminants`: boolean
- `remove_reversed_sequences`: boolean

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project","pipeline":"tmt-qc-demo","data_range":3,"raw_files":[],"remove_contaminants":true,"remove_reversed_sequences":true}' \
  https://example.com/api/protein-names
```

### `/api/protein-groups`

Returns protein-group level data for selected proteins and runs.

Request fields:

- `project`
- `pipeline`
- `data_range`
- `raw_files`: optional list of displayed raw-file names
- `protein_names`: required list of protein-group identifiers
- `columns`: requested columns; include `"Reporter intensity corrected"` to expand to all detected reporter-intensity columns

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project","pipeline":"tmt-qc-demo","data_range":3,"protein_names":["QC1|Peptide1"],"columns":["Reporter intensity corrected"]}' \
  https://example.com/api/protein-groups
```

### `/api/rawfile`

Updates run state for selected files.

Supported actions:

- `flag`
- `unflag`
- `accept`
- `reject`

Selection fields:

- `project`
- `pipeline`
- one of `run_keys`, `raw_file_ids`, or legacy `raw_files`

`run_keys` is the safest selector because it matches the display key used in the UI and disambiguates duplicate filenames.

```bash
curl \
  -H 'Authorization: Token <token>' \
  -H 'Content-Type: application/json' \
  -d '{"project":"demo-project","pipeline":"tmt-qc-demo","action":"accept","run_keys":["rf1"]}' \
  https://example.com/api/rawfile
```

### `/api/flag/create` and `/api/flag/delete`

These endpoints provide explicit flag toggles and use the same selection rules as `/api/rawfile`.

```bash
curl \
  -H 'Authorization: Token <token>' \
  -X POST \
  -d 'project=demo-project' \
  -d 'pipeline=tmt-qc-demo' \
  -d 'run_keys=rf1' \
  https://example.com/api/flag/create
```
