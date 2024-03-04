
import os, sys
import logging, traceback

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.captureWarnings(True)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.error(
        "".join(
            [
                "Uncaught exception: ",
                *traceback.format_exception(exc_type, exc_value, exc_traceback),
            ]
        )
    )


# Install exception handler
sys.excepthook = handle_exception

import pandas as pd
from common.taxonomy import load_green_gene_tax


#input files
profile= snakemake.input.profile
metadata_file= snakemake.input.metadata

def extract_genome_name_from_path(s):
    start = s.rfind('/') + 1
    end = s.find('_genomic', start)
    return s[start:end]


def extract_sample_name(s):
    start = s.rfind('/') + 1
    end = s.find('_R', start)
    if end == -1:  # if '_' is not found, find the first '.'
        end = s.find('.', start)
    return s[start:end]


taxa= load_green_gene_tax(metadata_file,remove_prefix=True)




D= pd.read_table(profile, index_col=[0,1], usecols=[0,1,2]).squeeze()

assert D.index.is_unique

D= D.unstack()

D.columns= D.columns.map(extract_genome_name_from_path)
D.index= D.index.map(extract_sample_name)

D.columns.name= D.columns.name.replace("_file","")
D.index.name= D.index.name.replace("_file","")

D= D.fillna(0)


# save files
D.to_csv(snakemake.output.abundance)

taxa.to_csv(snakemake.output.taxa)