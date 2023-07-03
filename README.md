# FinanceRoutines

| **Documentation**                                                               | **Build Status**                                                                                |
|:-------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|
| [![][docs-stable-img]][docs-stable-url] [![][docs-latest-img]][docs-latest-url] | [![CI Testing](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml?query=branch%3Amain) |




## Examples

### Import data from WRDS

First import the monthly stock file and the compustat funda file
```julia
using FinanceRoutines
# Set up a wrds connection
wrds_conn = FinanceRoutines.open_wrds_pg()

# CRSP
df_msf = import_MSF(wrds_conn); # Import the monthly stock file
df_msf = build_MSF(df_msf); # Run common processing
# Compustat
df_funda = import_Funda(wrds_conn);
df_funda = build_Funda(df_funda);
# Merge both files
df_linktable = FinanceRoutines.import_ccm_link(wrds_conn)
df_msf = link_MSF(df_linktable, df_msf) # merge gvkey on monthly stock file
df_msf = innerjoin(df_msf, df_funda, on = [:gvkey, :date_y], matchmissing=:notequal)
```

### Import the Fama-French three factors

This downloads directly data from Ken French's website and formats the data

```julia
df_FF3 = import_FF3()
# there is an option to download the daily factors
df_FF3_daily = import_FF3(:daily)
```


## To Do

  - `olsgmm` from cochrane GMM code
  - rolling regressions


## Othere references to work with financial data

The package the closest to this one is 
  
- [WrdsMerger.jl](https://github.com/junder873/WRDSMerger.jl); WrdsMerger is probably in a more stable state than this package.


Other packages or sources of code I have used to process the WRDS data

- [WRDS demo on momentum (python)](https://wrds-www.wharton.upenn.edu/documents/1442/wrds_momentum_demo.html)
- Tidy Finance (R) [Book](https://www.tidy-finance.org) and [repo](https://github.com/tidy-finance/website)
- French data [R package](https://nareal.github.io/frenchdata/articles/basic_usage.html)
- Ian Gow [Quarto Book (R)](https://iangow.github.io/far_book/ident.html)
- Replication [Open Source AP (stata)](https://github.com/OpenSourceAP/CrossSection/tree/master)




[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://eloualiche.github.io/FinanceRoutines.jl/
[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://eloualiche.github.io/FinanceRoutines.jl/
