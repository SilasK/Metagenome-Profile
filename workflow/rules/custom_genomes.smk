genome_dir = Path(config["genome_dir"])
assert genome_dir.exists(), f"genome_dir {genome_dir} does not exist"
assert genome_dir.is_dir(), f"genome_dir {genome_dir} is not a directory"
fasta_files = list(genome_dir.glob("*.fa*")) + list(genome_dir.glob("*.fna*"))
if len(fasta_files) == 0:
    raise ValueError(f"genome_dir {genome_dir} does not contain any fasta files")

n_genomes = len(fasta_files)
logger.info(f"Found {n_genomes} fasta files in {genome_dir}")

if n_genomes < 5:
    raise ValueError(f"genome_dir {genome_dir} does not contain at least 5 fasta files")

if "custom_dbname" in config:
    custom_dbname = config["custom_dbname"]

else:
    custom_dbname = genome_dir.name + f"_N{n_genomes}"

logger.info(f"Use custom db name: {custom_dbname}")

Sylph_dbs = {}
Sylph_dbs[custom_dbname] = {
    "c": 200,
}

available_sylph_db_names = [custom_dbname]
config["dbs_for_profiling"] = [custom_dbname]


rule sketch_custom_db:
    input:
        get_input_genomes,
    output:
        SYLPH_DB_PATH / "{custom_dbname}.syldb",
    log:
        "logs/sylph/sketch_db_{custom_dbname}.log",
    threads: 12
    resources:
        runtime=2 * 60,
        mem_mb=10 * 1000,
    params:
        compression=lambda wildcards: Sylph_dbs[wildcards.custom_dbname]["c"],
        output_prefix=lambda wildcards, output: str(output).replace(".syldb", ""),
    conda:
        "../envs/sylph.yaml"
    shell:
        "sylph sketch "
        " -g {input}"
        " -o {params.output_prefix} "
        " -c {params.compression}"
        " -t {threads}"
        " &> {log}"
