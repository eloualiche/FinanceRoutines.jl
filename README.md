# FinanceRoutines

| **Documentation**                                                               | **Build Status**                                                                                | **Code Coverage**                                                                                |
|:-------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------:|
| [![][docs-stable-img]][docs-stable-url] [![][docs-latest-img]][docs-latest-url] | [![CI Testing](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/eloualiche/FinanceRoutines.jl/actions/workflows/CI.yml?query=branch%3Amain) | [![codecov](https://codecov.io/gh/eloualiche/FinanceRoutines.jl/graph/badge.svg?token=A6IYNO17NK)](https://codecov.io/gh/eloualiche/FinanceRoutines.jl)


`FinanceRoutines.jl` is a package that contains useful functions to download and process academic financial data.

So far the package provides function to import:

  - CRSP and Compustat from the WRDS Postgres server
  - Fama-French three factors series from Ken French's website
  - GSW Yield curves from the [NY Fed](https://www.federalreserve.gov/pubs/feds/2006/200628/200628abs.html)
  - Estimation of betas for stocks

## Installation

`FinanceRoutines.jl` is a not yet a registered package.
You can install it from github  via

```julia
import Pkg
Pkg.add(url="https://github.com/eloualiche/FinanceRoutines.jl")
```

## Examples

### Import data from WRDS

First import the monthly stock file and the compustat funda file
```julia
using FinanceRoutines
using DataFrames

# Set up a wrds connection (requires your WRDS credentials)
wrds_conn = FinanceRoutines.open_wrds_pg()
```

Then we can import the monthly stock file. 
The new version of `FinanceRoutines.jl` supports pulling from the new `CIZ` file format.
```julia
df_msf_v2 = import_MSF_v2(wrds_conn) # CHECK YOUR TWO STEP AUTHENTICATOR
# 3826457×11 DataFrame
#      Row │ permno  mthcaldt    mthret     mthretx    shrout   mthprc       mthcap         mthprevcap     siccd  naics    datem
#          │ Int64   Date        Decimal?   Decimal?   Int64?   Decimal?     Decimal?       Decimal?       Int64  String?  MonthlyD…
# ─────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#        1 │  10000  1986-01-31   0.707317   0.707317     3680     4.375          16100           9430      3990  missing  1986-01
#        2 │  10000  1986-02-28  -0.257143  -0.257143     3680     3.25           11960          16100      3990  missing  1986-02
#        3 │  10000  1986-03-31   0.365385   0.365385     3680     4.4375         16330          11960      3990  missing  1986-03
#        4 │  10000  1986-04-30  -0.098592  -0.098592     3793     4              15172          16330      3990  missing  1986-04
```

On the other side, the package also allows pulling from the compustat funda file:
```julia
df_funda = import_Funda(wrds_conn);
build_Funda!(df_funda)
```

Last there is a function to get the link table and merge crsp with compustat:
```julia
# Merge both files
df_linktable = FinanceRoutines.import_ccm_link(wrds_conn)
 # merge gvkey on monthly stock file
df_msf = link_MSF(df_linktable,  
    select(df_msf_v2, :permno, :mthcaldt=>:date, :datem, :mthret=>:ret, :mthcap))
df_msf = innerjoin(df_msf, df_funda, on = [:gvkey, :datey], matchmissing=:notequal)
```

### Import the Fama-French three factors

This downloads directly data from Ken French's website and formats the data

```julia
df_FF3 = import_FF3()
# there is an option to download the daily factors
df_FF3_daily = import_FF3(:daily)
```

### Estimate treasury bond returns

The function downloads yield curves from the [NY Fed GSW](https://www.federalreserve.gov/pubs/feds/2006/200628/200628abs.html) and estimate returns based on the curves

```julia
df_GSW = import_GSW();
estimate_yield_GSW!(df_GSW; maturity=1); # maturity is in years
select(df_GSW, :date, :yield_1y)
```

### Common operations in asset pricing

Look in the documentation for a guide on how to estimate betas: over the whole sample and using rolling regressions.
The package exports the function `calculate_rolling_betas`.


## To Do

  - `olsgmm` from cochrane GMM code


## Other references to work with financial data

The package the closest to this one is

- [WrdsMerger.jl](https://github.com/junder873/WRDSMerger.jl); WrdsMerger is probably in a more stable state than this package.
- [WRDS.jl](https://github.com/elenev/WRDS.jl); WRDS specific wrappers to interact with the Postgres database.

Other packages or sources of code I have used to process the WRDS data

- [WRDS demo on momentum](https://wrds-www.wharton.upenn.edu/documents/1442/wrds_momentum_demo.html) (python)
- Tidy Finance [Book](https://www.tidy-finance.org) and [repo](https://github.com/tidy-finance/website) (R)
- French data [package](https://nareal.github.io/frenchdata/articles/basic_usage.html) (R)
- Ian Gow's Empirical Research in Accounting [Book](https://iangow.github.io/far_book/) (R)
- Replication [Open Source AP](https://github.com/OpenSourceAP/CrossSection/tree/master) (stata)




[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://eloualiche.github.io/FinanceRoutines.jl/
[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://eloualiche.github.io/FinanceRoutines.jl/
