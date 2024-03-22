# PLINK Pipelines

<p align="center">
    <a href="LICENSE" alt="License">
        <img src="https://img.shields.io/badge/License-APGL-5B2D5B.svg" /></a>
  
<a href="https://www.python.org/downloads/" alt="Python">
    <img src="https://img.shields.io/badge/python->=3.10,<4.0-blue.svg" /></a>
  
   <a href="https://pypi.org/project/plink-pipelines/" alt="Python">
        <img src="https://img.shields.io/pypi/v/plink_pipelines.svg" /></a>
  
  <a href="https://codecov.io/gh/arnor-sigurdsson/plink_pipelines" alt="Coverage">
        <img src="https://codecov.io/gh/arnor-sigurdsson/plink_pipelines/branch/master/graph/badge.svg" /></a>       
</p>


A small program to run pipelines to convert from PLINK [`.bed`, `.bim`, `.fam`] genotype filesets to one-hot encoded NumPy arrays on disk, e.g. for usage with [EIR](https://github.com/arnor-sigurdsson/EIR) or your own workflows.

## Installation

```
pip install plink-pipelines
```

Optionally, install [Plink](https://www.cog-genomics.org/plink/) and make sure it is accessible from your `PATH`. This is needed for some options (e.g., `--do_qc`).

## Quickstart

Download [raw sample data](https://drive.google.com/file/d/1LPEPvCerwFNWzcwWL-vXJaQd6HpugDPE/view?usp=sharing) (or use your own `.bed`, `.bim`, `.fam` files).

```bash
plink_pipelines --raw_data_path plink_pipelines_sample_data --output_folder plink_pipelines_sample_data
```

To see the full set of options, do `plink_pipelines --help`.

## Citation

If you use `plink-pipelines` in a scientific publication, we would appreciate if you could use one of the following citations:

```
@article{10.1093/nar/gkad373,
    author    = {Sigurdsson, Arn{\'o}r I and Louloudis, Ioannis and Banasik, Karina and Westergaard, David and Winther, Ole and Lund, Ole and Ostrowski, Sisse Rye and Erikstrup, Christian and Pedersen, Ole Birger Vesterager and Nyegaard, Mette and DBDS Genomic Consortium and Brunak, S{\o}ren and Vilhj{\'a}lmsson, Bjarni J and Rasmussen, Simon},
    title     = {{Deep integrative models for large-scale human genomics}},
    journal   = {Nucleic Acids Research},
    month     = {05},
    year      = {2023}
}

@article{sigurdsson2022improved,
    author    = {Sigurdsson, Arnor Ingi and Ravn, Kirstine and Winther, Ole and Lund, Ole and Brunak, S{\o}ren and Vilhjalmsson, Bjarni J and Rasmussen, Simon},
    title     = {Improved prediction of blood biomarkers using deep learning},
    journal   = {medRxiv},
    pages     = {2022--10},
    year      = {2022},
    publisher = {Cold Spring Harbor Laboratory Press}
}
```
