# polkadot-etl

This repository contains the CLI to extract data from the Polkadot Sidecar.

It is made in the spirit of the `bitcoinetl` CLI [linked
here](https://github.com/blockchain-etl/bitcoin-etl). This CLI was built to
extract block information from the sidecar, and then write these to a file. These exported files can then be loaded into a database of your choice (Postgres, Redshift, etc)

## Pre-Requisites
Before you can start, you will need the URL for a polkadot sidecar API (which in turn points to a polkadot archive node)
Get in touch with node infrastructure providers such as blockdaemon or on-finality who can help provision the archive node and sidecar for you.

This utility is configured to work best with python versions >3.8+ and < 3.10

## Installation

This project can be installed using poetry. Steps to install poetry can be found <a href="https://python-poetry.org/docs/#installing-with-the-official-installer">here</a>.

```
poetry install
poetry shell
```

After installing poetry and activating the virtual environment, you will be able to access cli commands to perform extraction from a Sidecar API  

## Commands

```
$ polkadotetl --help
 Usage: polkadotetl [OPTIONS] COMMAND [ARGS]...                                                                                                                                             
                                                                                                                                                                                            
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --log-level                 TEXT                             Set the loglevel for this application. Accepted values are python loglevels. See here:                                      │
│                                                              https://docs.python.org/3/library/logging.html#levels                                                                       │
│                                                              [default: INFO]                                                                                                             │
│ --install-completion        [bash|zsh|fish|powershell|pwsh]  Install completion for the specified shell. [default: None]                                                                 │
│ --show-completion           [bash|zsh|fish|powershell|pwsh]  Show completion for the specified shell, to copy it or customize the installation. [default: None]                          │
│ --help                                                       Show this message and exit.                                                                                                 │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ enrich         Enriches all Polkadot block response files from a folder and writes the results into a single, new-line-separated file of jsons. This can be directly uploaded to         │
│                BigQuery.                                                                                                                                                                 │
│ export-blocks  Exports blocks from the polkadot sidecar API into a newline-separated jsons file                                                                                          │

│ convert-raw-blocks-to-bigquery-schema  Acts of a folder containing exports from `export-blocks` command and transforms the files into a schema which can be uploaded onto Bigquery                                                                                          │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### 1. Export Blocks
`export-blocks` pulls data for a range of blocks from the sidecar and then writes each raw block response to a file.

##### Sample 
```
polkadotetl export-blocks /Users/polkadot-etl/tmp https://merkle-polkadot-01.merkle.net --start-block 9875710  --end-block 9875715
```

#### 2. Enrich Blocks
`enrich` runs a python function over files extracted by `export-blocks`, flattening them so that they can be written to a datastore for calculating account balances.

This step generates a much simplified schema which is easier to understand and query with an OLAP database
##### Sample
```
polkadotetl enrich /Users/polkadot-etl/tmp/ /Users/polkadot-etl/enriched.json
```

### Load to Bigquery
Sample scripts to load extracted data into Bigquery
Ensure that you have the cloud sdk <a href='https://cloud.google.com/sdk/docs/install'>installed</a> and authenticate with the google cloud

```
polkadotetl export-blocks /Users/polkadot-etl/tmp https://merkle-polkadot-01.merkle.net --start-block 9875710  --end-block 9875715
```

```
polkadotetl convert-raw-blocks-to-bigquery-schema tmp/ tmp2/
```

```
bq load --format=json \
        --project_id=projectid \
        --dataset_id=datasetid \
        transactions \
        /Users/polkadot-etl/tmp2/* \
        /Users/polkadot-etl/schema.json
```