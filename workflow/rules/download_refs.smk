# ref deb aproach
localrules:
    download_sylph_db,download_sylph_tax


rule download_sylph_db:
    output:
        SYLPH_DB_PATH / "{dbname}.syldb",
    params:
        url=lambda wildcards: Sylph_dbs[wildcards.dbname]["url_db"],
    log:
        "logs/sylph/download_db_{dbname}.log",
    shell:
        "wget {params.url} -O {output} &> {log}"


Sylph_dbs = config["sylph_dbs"]

available_sylph_db_names = list(Sylph_dbs.keys())
if len(available_sylph_db_names) == 0:
    raise Exception("No 'sylph_dbs' defined in config file.")
else:
    logger.info(
        f"Found information for sylph_dbs: {','.join(available_sylph_db_names)}"
    )



rule download_sylph_tax: 
    output:
        directory(SYLPH_TAX_FOLDER),
    conda:
        "../envs/sylph-tax.yaml",
    log:
        "logs/sylph/download_taxonomy.log",
    shell:
        "mkdir -p {output} && "
        "sylph-tax download --download-to {output} &> {log}"