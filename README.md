# FinanceRoutines

| **Documentation**                                                               | **Build Status**                                                                                |
|:-------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|
| [![][docs-stable-img]][docs-stable-url] [![][docs-dev-img]][docs-dev-url] | ![Build Status](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml?query=branch%3Amain) |


[!https://eloualiche.github.io/FinanceRoutines.jl/

[![Build Status](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml?query=branch%3Amain)

## Functions

1. Import Financial data
   - `import_FF3`
   - `build_crsp`

## To Do

  - Time lags for panel data (if lag and data is not offset by one month, then returns missing). 
  - `olsgmm` from cochrane GMM code
  - rolling regressions


## References

- [WRDS demo on momentum](https://wrds-www.wharton.upenn.edu/documents/1442/wrds_momentum_demo.html)
- Tidy Finance [Book](https://www.tidy-finance.org) and [repo](https://github.com/tidy-finance/website)
- French data [R package](https://nareal.github.io/frenchdata/articles/basic_usage.html)
- Ian Gow [Quarto Book](https://iangow.github.io/far_book/ident.html)
- Replication [Open Source AP](https://github.com/OpenSourceAP/CrossSection/tree/master)

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://eloualiche.github.io/FinanceRoutines.jl/

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

