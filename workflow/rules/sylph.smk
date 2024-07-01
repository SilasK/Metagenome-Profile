
DB_PATH = Path(config["database_dir"])
SYLPH_DB_PATH = DB_PATH / "Sylph"


Sylph_dbs = config["sylph_dbs"]





available_sylph_db_names = list(Sylph_dbs.keys())
if len(available_sylph_db_names) == 0:
    raise Exception("No 'sylph_dbs' defined in config file.")
else:
    logger.info(
        f"Found information for sylph_dbs: {','.join(available_sylph_db_names)}"
    )


if "dbs_for_profiling" not in config:
    config["dbs_for_profiling"] = available_sylph_db_names
    logger.warning("No 'dbs_for_profiling' are set profile all databases.")
else:
    missing_dbs= set(config["dbs_for_profiling"]).difference(available_sylph_db_names)

    if len(missing_dbs) > 0:
        raise Exception(
            f"You want to profile sylph with dbs for which I do not have the information: {missing_dbs}"
        )

    else:
        logger.info(f"Profile for databases: {','.join(config['dbs_for_profiling'])}")





localrules:
    get_sylph_db,
    download_metafile,


rule get_sylph_db:
    output:
        SYLPH_DB_PATH / "{dbname}.syldb",
    params:
        url=lambda wildcards: Sylph_dbs[wildcards.dbname]["url_db"],
    shell:
        "wget {params.url} -O {output} "


rule download_metafile:
    output:
        SYLPH_DB_PATH / "metafile_{dbname}.tsv.gz",
    params:
        url=lambda wildcards: Sylph_dbs[wildcards.dbname]["url_metafile"],
    shell:
        "wget {params.url} -O {output} "





rule sylph_sketch_block:
    input:
        unpack(sylph_input),
    output:
        directory("Intermediate/sylph/read_sketch_c{subsample}/{block}"),
    log:
        "logs/sylph/sketch_c{subsample}/{block}.log",
    threads: 12
    conda:
        "../envs/sylph.yaml"
    resources:
        runtime=5 * 60,
    shell:
        "sylph sketch -c {wildcards.subsample} -1 {input.R1} -2 {input.R2} -d {output} 2> {log}"



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
        "sylph profile {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} 2> {log}"


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
        "sylph query {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} 2> {log}"




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
