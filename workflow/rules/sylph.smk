DB_PATH = Path(config["database_dir"])
SYLPH_DB_PATH = DB_PATH / "Sylph"
SYLPH_TAX_FOLDER = SYLPH_DB_PATH / "taxonomy"

if "genome_dir" in config:

    include: "custom_genomes.smk"

else:

    include: "download_refs.smk"


if "dbs_for_profiling" not in config:
    config["dbs_for_profiling"] = available_sylph_db_names
    logger.error("No 'dbs_for_profiling' are set.")
    exit(1)

else:
    missing_dbs = set(config["dbs_for_profiling"]).difference(available_sylph_db_names)

    if len(missing_dbs) > 0:
        raise Exception(
            f"You want to profile sylph with dbs for which I do not have the information: {missing_dbs}"
        )

    else:
        logger.info(f"Profile for databases: {','.join(config['dbs_for_profiling'])}")


rule sylph_sketch_block:
    "The S parameter gives sample names, if fastq files are not unique. it creates samplename.paired.sylsp"
    input:
        unpack(sylph_input),
    output:
        directory("Intermediate/sylph/read_sketch_c{subsample}/{block}"),
    params:
        sample_names=lambda wildcards: " ".join(Sample_blocks[wildcards.block]),
    log:
        "logs/sylph/sketch_c{subsample}/{block}.log",
    threads: 12
    conda:
        "../envs/sylph.yaml"
    resources:
        runtime=5 * 60,
        mem_mb=10 * 1000,
    shell:
        "sylph sketch "
        " -c {wildcards.subsample} "
        " -1 {input.R1} -2 {input.R2} "
        " -S {params.sample_names} "
        " -d {output} "
        " &> {log}"


rule sylph_profile:
    input:
        unpack(sylph_profile_input),
    output:
        "Intermediate/sylph/profile/{dbname}/{block}.tsv",
    conda:
        "../envs/sylph.yaml"
    threads: 12
    resources:
        runtime=5 * 60,
        mem_mb=250 * 1000,
    log:
        "logs/sylph/profile/{dbname}/{block}.log",
    shell:
        "sylph profile {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} &> {log}"


rule sylph_taxprof:
    input:
        "Intermediate/sylph/profile/{dbname}/{block}.tsv",
        db=SYLPH_TAX_FOLDER
    output:
        directory("Intermediate/sylph/taxprofile/{dbname}/{block}"),
    conda:
        "../envs/sylph-tax.yaml"
    threads: 1
    resources:
        runtime=60,
        mem_mb=1000*10,
    log:
        "logs/sylph/taxprof/{dbname}/{block}.log",
    params:
        tax_name = lambda wildcards: Sylph_dbs[wildcards.dbname]["taxprof_name"],
        virus_host = lambda wc: " --annotate-virus-hosts " if wc.dbname in config["virus_db_names"] else "",
    shell:
        " mkdir -p {output} && "
        "  sylph-tax taxprof {input[0]} -t {params.tax_name} {params.virus_host} -o {output}/ &> {log}"

wildcard_constraints:
    column="ANI|relative_abundance|sequence_abundance|Coverage",

rule tax_merge:
    input:
        expand(
            "Intermediate/sylph/taxprofile/{{dbname}}/{block}/",
            block=Sample_blocks.keys(),
        ),
    output:
        "Sylph/{dbname}/{column}.tsv",

    params:
        parent_dir = lambda wc, input: Path(input[0]).parent,
    log:
        "logs/sylph/concatenate_taxprof/{dbname}_{column}.log",
    conda:
        "../envs/sylph-tax.yaml"
    shell:
        "sylph-tax merge {params.parent_dir}/*/*.sylphmpa "
        " --column {wildcards.column} " 
        " -o {output}  &> {log}"


rule sylph_query:
    input:
        unpack(sylph_profile_input),
    output:
        "Intermediate/sylph/query/{dbname}/{block}.tsv",
    conda:
        "../envs/sylph.yaml"
    threads: 12
    resources:
        runtime=5 * 60,
        mem_mb=250 * 1000,
    log:
        "logs/sylph/query/{dbname}/{block}.log",
    shell:
        "sylph query {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} &> {log}"


localrules:
    concatenate_sylph,


rule concatenate_sylph:
    input:
        expand(
            "Intermediate/sylph/{{method}}/{{dbname}}/{block}.tsv",
            block=Sample_blocks.keys(),
        ),
    output:
        "Sylph/{dbname}_{method}.tsv",
    log:
        "logs/sylph/concatenate_{method}_{dbname}.log",
    script:
        "../scripts/concat_sylph.py"


"""
rule convert_sylph:
    input:
        profile="Sylph/{dbname}_profile.tsv",
        metadata=SYLPH_DB_PATH / "metafile_{dbname}.tsv.gz",
    output:
        abundance="Sylph/{dbname}_abundance.csv",
        taxa="Sylph/{dbname}_taxonomy.csv",
    log:
        "logs/sylph/convert_{dbname}.log",
    threads: 1
    script:
        "../scripts/sylph_to_table.py"
"""
