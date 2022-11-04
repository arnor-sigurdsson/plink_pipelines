# plink_pipelines

## Installation

Install [Plink](https://www.cog-genomics.org/plink/) and make sure it is accessible from your `PATH`.

```
pip install plink-pipelines
```

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
