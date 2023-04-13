# PLINK Pipelines

<p align="center">
    <a href="LICENSE" alt="License">
        <img src="https://img.shields.io/badge/License-APGL-5B2D5B.svg" /></a>
  
  <a href="https://www.python.org/downloads/" alt="Python">
        <img src="https://img.shields.io/badge/python-3.10-blue.svg" /></a>
  
   <a href="https://pypi.org/project/plink-pipelines/" alt="Python">
        <img src="https://img.shields.io/pypi/v/plink_pipelines.svg" /></a>
  
  <a href="https://codecov.io/gh/arnor-sigurdsson/plink_pipelines" alt="Coverage">
        <img src="https://codecov.io/gh/arnor-sigurdsson/plink_pipelines/branch/master/graph/badge.svg" /></a>       
</p>


A small program to run pipelines to convert from PLINK [`.bed`, `.bim`, `.fam`] genotype filesets to arrays on disk, e.g. for usage with [EIR](https://github.com/arnor-sigurdsson/EIR) or your own workflows.

## Installation

```
pip install plink-pipelines
```

Optionally, install [Plink](https://www.cog-genomics.org/plink/) and make sure it is accessible from your `PATH`. This is needed for some options (e.g., `--do_qc`).

## Quickstart

Download [raw sample data](https://drive.google.com/file/d/1LPEPvCerwFNWzcwWL-vXJaQd6HpugDPE/view?usp=sharing) (or use your own `.bed`, `.bim`, `.fam` files.

```bash
plink_pipelines --raw_data_path plink_pipelines_sample_data --output_folder plink_pipelines_sample_data
```

To see the full set of options, do `plink_pipelines --help`.

## Citation

If you use `plink_pipelines` in a scientific publication, we would appreciate if you could use the following citation:

```
@article{sigurdsson2021deep,
  title={Deep integrative models for large-scale human genomics},
  author={Sigurdsson, Arnor Ingi and Westergaard, David and Winther, Ole and Lund, Ole and Brunak, S{\o}ren and Vilhjalmsson, Bjarni J and Rasmussen, Simon},
  journal={bioRxiv},
  year={2021},
  publisher={Cold Spring Harbor Laboratory}
}
