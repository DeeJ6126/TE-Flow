"""Single/paired selection and preparation use metadata, never invented mates."""
import gzip
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import prepare_samples as pipeline


def metadata_row(run="SRR1", layout="SINGLE", name=None):
    name = name or f"{run}.fastq.gz"
    return {"run_accession": run, "experiment_accession": "SRX1", "library_layout": layout,
            "library_strategy": "RNA-Seq", "fastq_ftp": "ftp.sra.ebi.ac.uk/" + name,
            "fastq_bytes": "60"}


@pytest.mark.parametrize("name", ["SRR1.fastq.gz", "SRR1_1.fastq.gz", "alias_R1_001.fq.gz"])
def test_single_ena_filename_is_exactly_metadata_name(name):
    files = pipeline.fastq_files(metadata_row(name=name), "SRR1")
    assert files == [{"name": name, "bytes": 60, "mate": 1, "source": "ena-fastq"}]


@pytest.mark.parametrize("layout", ["", "UNKNOWN", "INTERLEAVED"])
def test_unknown_layout_is_rejected_without_guessing(layout):
    with pytest.raises(ValueError, match="无法识别单双端类型"):
        pipeline.fastq_files(metadata_row(layout=layout), "SRR1")


def test_single_multiple_files_are_not_silently_concatenated():
    row = metadata_row()
    row.update(fastq_ftp="ftp.sra.ebi.ac.uk/lane1.fastq.gz;ftp.sra.ebi.ac.uk/lane2.fastq.gz", fastq_bytes="60;60")
    with pytest.raises(ValueError, match="多份 FASTQ"):
        pipeline.fastq_files(row, "SRR1")


def test_ncbi_single_sra_conversion_keeps_actual_layout_and_single_filename():
    with patch.object(pipeline, "_metadata_request", side_effect=OSError("ENA unavailable")), \
         patch.object(pipeline, "fetch_ncbi_runinfo", return_value=([], [{
             "Run": "SRR1", "Experiment": "SRX1", "LibraryLayout": "SINGLE", "LibraryStrategy": "RNA-Seq"}])), \
         patch.object(pipeline, "verify_run_set", return_value={"source": "fixture"}):
        result = pipeline.experiment_metadata("SRX1")
    assert result["SRR1"]["library_layout"] == "SINGLE"
    assert [file["name"] for file in result["SRR1"]["files"]] == ["SRR1.fastq.gz"]


def test_mixed_geo_preparation_and_resume_keep_single_r2_absent(tmp_path):
    payload = gzip.compress(b"@read\nACGT\n+\nIIII\n", mtime=0)
    records = {}
    for run, layout, names in [("SRR1", "SINGLE", ["SRR1.fastq.gz"]),
                               ("SRR2", "PAIRED", ["SRR2_1.fastq.gz", "SRR2_2.fastq.gz"])]:
        row = metadata_row(run, layout)
        row.update(fastq_ftp=";".join("ftp.sra.ebi.ac.uk/" + name for name in names),
                   fastq_bytes=";".join(str(len(payload)) for name in names))
        records[run] = {"row": row, "files": pipeline.fastq_files(row, run), "library_layout": layout,
                        "geo_samples": ["GSM1"], "experiments": ["SRX1"]}
    resolved = {"accession": "GSE1", "gsm_accessions": ["GSM1"],
                "mapping": [{"gse": "GSE1", "gsm": "GSM1", "experiment": "SRX1", "run": run} for run in records],
                "fields": pipeline.ENA_COLUMNS, "records": records}
    raw, output = tmp_path / "raw", tmp_path / "output"

    def download(command, directory, handle, timeout=None):
        run = command[command.index("-i") + 1]
        for file in records[run]["files"]:
            (directory / file["name"]).write_bytes(payload)
        (directory / "success.log").write_text(f"fixture\t{run}\n")

    with patch.object(pipeline, "download_lock", side_effect=lambda _: nullcontext()), \
         patch.object(pipeline.shutil, "which", return_value="/bin/iseq"), \
         patch.object(pipeline, "check_space", return_value=3 * len(payload)), \
         patch.object(pipeline, "resolve_geo_series", return_value=resolved) as resolver, \
         patch.object(pipeline, "run_iseq", side_effect=download) as iseq:
        manifest = pipeline.prepare("GSE1", raw, output, output / "samples.json", output / "prepare.log")
        # Legacy cache records may omit derived fields. A valid TSV provides
        # layout and filenames without another metadata request.
        cache_path = raw / ".teflow-GSE1.geo.complete.json"
        cached = json.loads(cache_path.read_text())
        for record in cached["records"].values():
            record.pop("library_layout", None)
            record.pop("files", None)
        cache_path.write_text(json.dumps(cached))
        resumed = pipeline.prepare("GSE1", raw, output, output / "samples.json", output / "prepare.log")
    assert iseq.call_count == 2
    assert resolver.call_count == 1
    single, paired = manifest["samples"]["SRR1"], manifest["samples"]["SRR2"]
    assert single["library_layout"] == "SINGLE" and single["r2"] is None
    assert len(single["files"]) == 1 and Path(single["r1"]).name == "SRR1.fastq.gz"
    assert paired["library_layout"] == "PAIRED" and Path(paired["r2"]).is_file()
    assert all(value["download_status"] == "reused" for value in resumed["samples"].values())
    assert not (raw / "SRR1" / "SRR1_2.fastq.gz").exists()
