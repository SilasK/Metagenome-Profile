




Sylph_db="/home/users/k/kiesers/scratch/Databases/Sylph/v0.3-c200-gtdb-r214.syldb"
Sylph_metafile= "/home/users/k/kiesers/scratch/Databases/Sylph/gtdb_r214_metadata.tsv.gz"


localrules: get_sylph_db, download_metafile

rule get_sylph_db:
    output:
        Sylph_db
    shell:
        "wget https://storage.googleapis.com/sylph-stuff/v0.3-c200-gtdb-r214.syldb -O {output} "



rule download_metafile:
    output:
        Sylph_metafile
    shell:
        "wget https://github.com/bluenote-1577/sylph-utils/raw/main/gtdb_r214_metadata.tsv.gz -O {output}"



# rule sylph_sketch_one:
#     input:
#         R1 = sample_table.
#     output:
#         "Intermediate/sylph/read_sketch/{sample}.sylsp"
#     threads:
#         1
#     params:
#         sketch_db = lambda wc, output: Path(output[0]).parent
#     conda:
#         "../envs/sylph.yaml"
#     resources:
#         time_min=15,
#         mem_mb=1000
#     log:
#         "logs/sylph/sketch/{sample}.log"
#     shell:
#         "sylph sketch -c 200 -1 {input[0]} -2 {input[1]} -d {params.sketch_db} 2> {log}"


rule sylph_sketch_all:
    input:
        R1 = expand("QC/reads/{sample}_{fraction}.fastq.gz",sample=SAMPLES,fraction="R1"),
        R2 = expand("QC/reads/{sample}_{fraction}.fastq.gz",sample=SAMPLES,fraction="R2")
    output:
        directory("Intermediate/sylph/read_sketch")
    threads:
        12
    conda:
        "../envs/sylph.yaml"
    log:
        "logs/sylph/sketch.log"
    resources:
        time_min=5*60
    shell:
        "sylph sketch -c 200 -1 {input.R1} -2 {input.R2} -d {output} 2> {log}"




rule syldb_profile:
    input:
        db= Sylph_db,
        sketch_dir = rules.sylph_sketch_all.output #expand(rules.sylph_sketch.output,sample=SAMPLES)
    output:
        "Intermediate/sylph/Sylph_profile.tsv"
    conda:
        "../envs/sylph.yaml"
    threads:
        12
    resources:
        time_min=5*60,
        mem_mb=250*1000
    log:
        "logs/sylph/profile.log"
    shell:
        "sylph profile {input.db} {input.sketch_dir}/*.sylsp -u -t {threads} -o  {output} 2> {log}"


rule convert_sylph:
    input:
        profile= rules.syldb_profile.output[0],
        metadata= Sylph_metafile,
    output:
        abundance="Profile/Sylph_abundance.csv",
        taxa= "Profile/Sylph_taxa.csv",
    log:
        "logs/sylph/convert.log"
    threads: 1
    script:
        "../scripts/sylph_to_table.py"
       
