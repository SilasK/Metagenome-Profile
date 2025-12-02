

include: "sample_table.smk"


# split samples into blocks
BLOCK_LENGTH = 50
Sample_blocks = {
    f"Block_{i+1}": SAMPLES[i : i + BLOCK_LENGTH]
    for i in range(0, len(SAMPLES), BLOCK_LENGTH)
}


def sylph_input(wildcards):
    samples_of_block = Sample_blocks[wildcards.block]

    output = {}
    for f in FRACTIONS:
        output[f] = SampleTable.loc[samples_of_block, "Reads_QC_" + f].tolist()

    return output


def sylph_profile_input(wildcards):
    subsample_value = Sylph_dbs[wildcards.dbname]["c"]

    return dict(
        sketch_dir=f"Intermediate/sylph/read_sketch_c{subsample_value}/{wildcards.block}",
        db=SYLPH_DB_PATH / f"{wildcards.dbname}.syldb",
    )


def get_input_genomes(wildcards):
    """
    Get the input genomes for sylph sketching.
    """

    if "genome_dir" in config:

        genome_dir = Path(config["genome_dir"])
    else:
        raise FileNotFoundError(
            "genome_dir not found in config, cannot create a custom sylph database"
        )

    # identify genomes in the genome_dir

    assert genome_dir.exists(), f"genome_dir {genome_dir} does not exist"
    assert genome_dir.is_dir(), f"genome_dir {genome_dir} is not a directory"

    fasta_files = list(genome_dir.glob("*.fa*")) + list(genome_dir.glob("*.fna*"))

    if len(fasta_files) == 0:
        raise ValueError(f"genome_dir {genome_dir} does not contain any fasta files")

    logger.info(f"Found {len(fasta_files)} fasta files in {genome_dir}")
    return fasta_files
