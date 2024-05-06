

pepfile: "sample_table_config.yaml"


# pepschema: f"{snakemake_dir.parent}/config/sample_table_schema.yaml"


SAMPLES = pep.sample_table["sample_name"]
PAIRED = pep.sample_table.columns.str.contains("R2").any()

if PAIRED:
    FRACTIONS = ["R1", "R2"]
else:
    FRACTIONS = ["se"]





def get_qc_reads(wildcards):
    headers = ["Reads_QC_" + f for f in FRACTIONS]
    return pep.sample_table.loc[wildcards.sample, headers]


def sylph_input(wildcards):
    samples_of_block = Sample_blocks[wildcards.block]

    samples = dict(
        R1=pep.sample_table.loc[samples_of_block, "Reads_QC_R1"].tolist(),
        R2=pep.sample_table.loc[samples_of_block, "Reads_QC_R2"].tolist(),
    )
    return samples