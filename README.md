# polkadot-etl

This repository contains the CLI to extract data from the Polkadot Sidecar.

It is made in the spirit of the `bitcoinetl` CLI [linked
here.](https://github.com/blockchain-etl/bitcoin-etl). This CLI was built to
extract block information from the sidecar, and then write these to a file.

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
│ export-blocks  Exports blocks from the polkadot sidecar API into a newline-separated jsons file                                                                                          │                                                                                                              │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

`export-blocks` pulls data for a range of blocks from the sidecar and then writes each raw block response to a file.

`enrich-blocks` runs a python function over these blocks, flattening them so that they can be written to a datastore for calculating account balances.

## Installation

This project can be installed using poetry.
