import pandas as pd


sample_name_constraint = "[A-Za-z][A-Za-z0-9]+"


wildcard_constraints:
    sample=sample_name_constraint,

try:
    SampleTable = pd.read_csv("samples.csv", index_col=0)
except FileNotFoundError:
    try:
        SampleTable = pd.read_csv("samples.tsv", sep="\t", index_col=0)
    except FileNotFoundError:
        raise FileNotFoundError(
            "Sample table not found. Please provide a 'samples.csv' or 'samples.tsv' file."
        )

assert SampleTable.index.is_unique, "Sample table index is not unique"
assert SampleTable.index.str.match(
    sample_name_constraint
).all(), "Not all sample names correspond to sample name criteria"


if not SampleTable.columns.str.startswith("Reads_QC").any():

    raise ValueError(
        "Sample table does not contain QC-Fastq columns starting with 'Reads_QC'. Please check the sample table."
    )
    import warnings

    warnings.warn(
        "QC-Fastq paths not in sample table, use default 'QC/reads/{sample}_{fraction}.fastq.gz'"
    )


SAMPLES = SampleTable.index.tolist()
PAIRED = SampleTable.columns.str.contains("R2").any()

if PAIRED:
    FRACTIONS = ["R1", "R2"]
else:
    FRACTIONS = ["se"]


def get_qc_reads(wildcards):
    headers = ["Reads_QC_" + f for f in FRACTIONS]

    try:
        return SampleTable.loc[wildcards.sample, headers]
    except KeyError:
        return expand("QC/reads/{{sample}}_{fraction}.fastq.gz", fraction=FRACTIONS)





