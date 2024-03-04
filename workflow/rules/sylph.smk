

Sylph_dbs = config["sylph_dbs"]

available_sylph_db_names= list(Sylph_dbs.keys())
if len(available_sylph_db_names)==0:
    raise Exception("No 'sylph_dbs' defined in config file.")
else:
    logger.info(f"Found information for sylph_dbs: {','.join(available_sylph_db_names)}")


if "dbs_for_profiling" not in config: 
    config["dbs_for_profiling"] = available_sylph_db_names
    logger.warning("No 'dbs_for_profiling' are set profile all databases.")
else:
    missing_dbs: set(config["dbs_for_profiling"]).difference(available_sylph_db_names)

    if len(missing_dbs)>0:
        raise Exception(f"You want to profile sylph with dbs for which I do not have the information: {missing_dbs}")
        
    else:
        logger.info(f"Profile for databases: {','.join(config['dbs_for_profiling'])}")
    



DB_PATH= Path(config["db_folder"])
SYLPH_DB_PATH= DB_PATH/"Sylph"


localrules: get_sylph_db, download_metafile

rule get_sylph_db:
    output:
        SYLPH_DB_PATH/"{dbname}.syldb"
    params:
        url= lambda wildcards: Sylph_dbs[wildcards.dbname]["url_db"]
    shell:
        "wget {params.url} -O {output} "



rule download_metafile:
    output:
        SYLPH_DB_PATH/"metafile_{dbname}.tsv"
    params:
        url= lambda wildcards: Sylph_dbs[wildcards.dbname]["url_metafile"]
    shell:
        "wget {params.url} -O {output} "






# split samples into blocks
BLOCK_LENGTH=50
Sample_blocks = {f"Block_{i+1}":SAMPLES[i:i + BLOCK_LENGTH] for i in range(0, len(SAMPLES), BLOCK_LENGTH)}

def sylph_input(wildcards):

    samples_of_block = Sample_blocks[wildcards.block]

    samples = dict(R1 = expand("QC/reads/{sample}_{fraction}.fastq.gz",sample=samples_of_block,fraction="R1"),
    R2 = expand("QC/reads/{sample}_{fraction}.fastq.gz",sample=samples_of_block,fraction="R2"))
    return  samples



wildcards_constraints:
    subsample="/d+"


rule sylph_sketch_block:
    input:
        unpack(sylph_input)
    output:
        directory("Intermediate/sylph/read_sketch_c{subsample}/{block}")
    log:
        "logs/sylph/sketch_c{subsample}/{block}.log"
    threads:
        12
    conda:
        "../envs/sylph.yaml"
    resources:
        runtime = 5*60
    shell:
        "sylph sketch -c {wildcards.subsample} -1 {input.R1} -2 {input.R2} -d {output} 2> {log}"



def sylph_profile_input(wildcards):

    subsample_value= Sylph_db[wildcards.dbname]["c"]

    return dict(sketch_dir = f"Intermediate/sylph/read_sketch_c{subsample_value}/{wildcards.block}",
    db = SYLPH_DB_PATH/f"{wildcards.dbname}.syldb"
    )


rule sylph_profile:
    input:
        unpack(sylph_profile_input)
        
    output:
        "Intermediate/sylph/profile/{dbname}/{block}.tsv"
    conda:
        "../envs/sylph.yaml"
    threads:
        12
    resources:
        runtime=5*60,
        mem_mb=250*1000
    log:
        "logs/sylph/profile/{dbname}/{block}.log"
    shell:
        "sylph profile {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} 2> {log}"


localrules: concatenate_sylph_profiles
rule concatenate_sylph_profiles:
    input:
        expand("Intermediate/sylph/profile/{{dbname}}/{block}.tsv", block = Sample_blocks.keys())
    output:
        "Intermediate/sylph/combined_profile_{dbname}.tsv"
    log:
        "logs/sylph/concatenate_profiles_{dbname}.log"
    script:
        "../scripts/concat_sylph_profiles.py"
        



        


rule convert_sylph:
    input:
        profile= "Intermediate/sylph/combined_profile_{dbname}.tsv",
        metadata= SYLPH_DB_PATH/"metafile_{dbname}.tsv",
    output:
        abundance="Profile/Sylph_abundance_{dbname}.csv",
        taxa= "Profile/Sylph_taxa_{dbname}.csv",
    log:
        "logs/sylph/convert_{dbname}.log"
    threads: 1
    script:
        "../scripts/sylph_to_table.py"
       
