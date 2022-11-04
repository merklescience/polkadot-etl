"""Enrichment functionality tests"""


def test_enrich_blocks():
    """Tests whether block responses from the polkadot sidecar can be enriched."""
    import glob
    import json
    import os
    import csv
    from polkadotetl.enrich import enrich_block

    sample_blocks = glob.glob(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "sample_blocks/*.json"
        )
    )
    txns = []
    assert len(sample_blocks) > 0, "Need sample block jsons before testing."
    for sample_block in sample_blocks:
        print(sample_block)
        with open(sample_block, "r") as f:
            sample_block_json = json.load(f)
        enriched_block = enrich_block(sample_block_json)
        assert enriched_block is not None
        assert isinstance(enriched_block, list)
        assert isinstance(enriched_block[0], dict)
        txns.extend(enriched_block)

    with open(
        "result.csv".format(os.path.basename(sample_block)), "w", newline=""
    ) as f:
        fieldnames = enriched_block[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in enriched_block:
            writer.writerow(row)
