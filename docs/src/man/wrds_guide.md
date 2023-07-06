# WRDS User Guide


## Opening a WRDS connection

Since we are about to download data from CRSP we set up a connection with our WRDS credentials
```julia
using FinanceRoutines
using DataFrames, DataFramesMeta, Dates
using FixedEffectModels # for regressions
wrds_conn = FinanceRoutines.open_wrds_pg()
```


## Download the monthly stock file from CRSP

```julia
# Import the monthly stock file
df_msf = import_MSF(wrds_conn; date_range = (Date("1980-01-01"), Dates.today())); 
df_msf = build_MSF!(df_msf);    # Run common processing
# keep only what we need from the MSF
select!(df_msf, :permno, :date, :datem, :ret, :mktcap)
```


## Download the annual compustat funda file from WRDS

```julia
df_funda = import_Funda(wrds_conn; date_range = (Date("1980-01-01"), Dates.today())); 
df_funda = build_Funda!(df_funda);
```

## Merge both files CRSP MSF and Compustat Funda

```julia
df_linktable = FinanceRoutines.import_ccm_link(wrds_conn)
df_msf = link_MSF(df_linktable, df_msf) # merge gvkey on monthly stock file

# merge for a crsp/compustat merged file
df_ccm = innerjoin(df_msf, df_funda, on = [:gvkey, :date_y], matchmissing=:notequal)
```

