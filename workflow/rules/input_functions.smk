

include: "sample_table.smk"


# split samples into blocks
BLOCK_LENGTH = 50
Sample_blocks = {
    f"Block_{i+1}": SAMPLES[i : i + BLOCK_LENGTH]
    for i in range(0, len(SAMPLES), BLOCK_LENGTH)
}





def sylph_input(wildcards):
    samples_of_block = Sample_blocks[wildcards.block]

    output= {}
    for f in FRACTIONS:
        output[f] = SampleTable.loc[samples_of_block,"Reads_QC_" + f ]

    return output


def sylph_profile_input(wildcards):
    subsample_value = Sylph_dbs[wildcards.dbname]["c"]

    return dict(
        sketch_dir=f"Intermediate/sylph/read_sketch_c{subsample_value}/{wildcards.block}",
        db=SYLPH_DB_PATH / f"{wildcards.dbname}.syldb",
    )

