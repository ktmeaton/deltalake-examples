# deltalake-examples
Examples for working with Delta Lake tables.

## Python

```bash
micromamba env create -f envs/python.yml
micromamba activate deltalake-py
python examples/python.py
```

## R

```bash
micromamba env create -f envs/R.yml
micromamba activate deltalake-R
Rscript -e 'install.packages("pysparklyr", repos="")'
micromamba run -n deltalake-py examples/python.py
```