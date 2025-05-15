# Transitioning to the new CRSP file format

CRSP is changing the way they disseminate price data.
This is mostly relevant for the daily and monthly stock price data.

General information and code examples are available on [WRDS Website](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/).
I try to provide a short guide about how I went about converting some of the most basic crsp filters to the new format.
Note that the legacy files are named `SIZ` (CRSP 1.0) and the new file format is `CIZ` (CRSP 2.0). 

WRDS has excellent guides, and what follows is mainly for my reference and how we would do this simply in julia.
[crsp-ciz-faq](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/crsp-ciz-faq/)

## Getting mapping tables from old to new formats
First, I am going to reference the main mapping and metadata tables that allow to quickly convert old code into modern one.
These tables are available from the WRDS postgres server

```julia
using FinanceRoutines
using DataPipes, DataFrames, DataFramesMeta, DataPipes
wrds_conn = FinanceRoutines.open_wrds_pg(); # open a wrds connection with credentials
see(df) = show(df, allcols=true, allrows=true, truncate=0)
```

```julia
const get_postgres_table = FinanceRoutines.get_postgres_table
# main table for converting columns
siz_to_ciz = get_postgres_table("crsp", "metasiztociz"; wrds_conn=wrds_conn) |> DataFrame 
# flag information
flag_info = get_postgres_table("crsp", "metaFlagInfo"; wrds_conn=wrds_conn) |> DataFrame 
flag_coverage = get_postgres_table("crsp", "metaFlagCoverage"; wrds_conn=wrds_conn) |> DataFrame 
item_info = get_postgres_table("crsp", "metaItemInfo"; wrds_conn=wrds_conn) |> DataFrame 

stock_names = get_postgres_table("crsp", "stocknames"; wrds_conn=wrds_conn) |> DataFrame 
stock_info_hist = get_postgres_table("crsp", "StkSecurityInfoHist"; wrds_conn=wrds_conn) |> DataFrame 
```

## Datasets

I do not fully understand the difference between `stkmthsecuritydata` and `msf_v2` dataset (first is underlying data, second is somehow merged)

In one of the transition slides, there is a dataset mapping from `SIZ` to `CIZ` and the final datasets `DSF_V2` and `MSF_V2`. 

| SIZ                         | CIZ                                | WRDS              |
|-----------------------------|------------------------------------|-------------------|
| **DSF**                     | **stkDlySecurityData**             | **DSF_V2**        |
| **MSF**                     | **stkMthSecurityData**             | **MSF_V2**        |
|                             |                                    |                   |
| **StockNames**              | **stkSecurityInfoHist**            | **StockNames_V2** |
| **DSE\***                   | **stkDelists**                     |                   |
| **MSE\***                   | **stkDistributions**               |                   |
|                             |                                    |                   |
| **DSI**                     | **indDlySeriesData(_ind)**         |                   |
| **MSI**                     | **indMthSeriesData(_ind)**         |                   |


We are going to use the postgres server directly so we load the relevant packages here
```julia
import LibPQ: LibPQ.execute, LibPQ.Connection
import Tables: columntable
```


### Main Stock Files

```julia
postgre_query = """
SELECT *
    FROM crsp.msf_v2
    WHERE mthcaldt >= '2000-01-01' AND mthcaldt <= '2002-01-01'
"""
msf_v2 = execute(wrds_conn, postgre_query) |> columntable |> DataFrame

postgre_query = """
SELECT *
    FROM crsp.stkmthsecuritydata
    WHERE mthcaldt >= '2000-01-01' AND mthcaldt <= '2002-01-01'
"""
stkmthsecuritydata = execute(wrds_conn, postgre_query) |> columntable |> DataFrame
setdiff(names(msf_v2), names(stkmthsecuritydata))
```

### Information on Stocks

```julia
postgre_query = "SELECT * FROM crsp.stkSecurityInfoHist"
stksecurityinfohist = execute(wrds_conn, postgre_query) |> columntable |> DataFrame
postgre_query = "SELECT * FROM crsp.stocknames_v2"
stocknames_v2 = execute(wrds_conn, postgre_query) |> columntable |> DataFrame

names(stksecurityinfohist)
names(stocknames_v2)
```

### Index Files

```julia
indmthseriesdata = execute(wrds_conn, "SELECT * FROM crsp.indmthseriesdata") |> DataFrame

# more information on indices
indseriesinfohdr = execute(wrds_conn, "SELECT * FROM crsp.IndSeriesInfoHdr") |> DataFrame |> see
indfamilyinfohdr = execute(wrds_conn, "SELECT * FROM crsp.IndFamilyInfoHdr") |> DataFrame |> see
stkindmembership = execute(wrds_conn, "SELECT * FROM crsp.stkindmembership_ind") |> DataFrame |> see
```





## Standard Filters

### CRSP Share Codes Filters
It is standard to impose in the legacy file that the share codes is either `10` or `11`. 
For transparency, CRSP replaced the variable `SHRCD` with multiple flags that convey the information more clearly. 

First we are going to want to see the mapping in the metadata mapping table:
```julia
@rsubset(siz_to_ciz, :sizitemname == "SHRCD")
# see how the split is down precisely
unique(@rsubset(siz_to_ciz, :sizitemname == "SHRCD"), :cizitemname)
```
We have five different flags that correspond to the legacy share codes. 
How to map specifically the share code: recall that the first digit `1` translates to "ordinary common shares" and the second digit `0` or `1` translates to securities which "have not been further defined" or "need not be further defined" respectively.

The new flags are `ShareType`, `SecurityType`, `SecuritySubType`, `USIncFlg`, and `IssuerType`.
We can look at `ShareType` in the `metaFlagInfo` table:
```julia
@rsubset(flag_info, contains(:flagtypedesc, r"share.*type"i))
```
We can view how they map 
```julia
@p outerjoin(
    unique(select(stock_names, :permno, :shrcd)),
    unique(select(stock_info_hist, :permno, :sharetype, :securitytype, :securitysubtype, :usincflg, :issuertype)),
    on = :permno) |>
    @rsubset(__, :shrcd ∈ [10, 11]) |>
    groupby(__, [:shrcd, :sharetype, :securitytype, :securitysubtype, :usincflg, :issuertype]) |>
    combine(__, nrow)
```
The mapping at this point is less than obvious, so we gather some more information on the meaning of the relevant flags:
```julia
import Unicode
function get_info_flag(flag_name::String) 
    innerjoin(flag_info,
        select(unique(
            @rsubset(flag_coverage, Unicode.normalize(:itemname, casefold=true)==Unicode.normalize(flag_name, casefold=true)), 
            :flagvalue), :flagkey),
        on = :flagkey)
end

get_info_flag("ShareType") |> see
get_info_flag("SecurityType") |> see
get_info_flag("SecuritySubType") |> see
get_info_flag("USIncFlg") |> see
get_info_flag("IssuerType") |> see
```

To which it appears more clear that the proper mapping will be
```julia
stock_info_hist_subset = @rsubset(stock_info_hist, 
    :sharetype ∈ ["NS", "N/A"], :securitytype=="EQTY", :securitysubtype=="COM", :issuertype ∈ ["ACOR", "CORP"], :usincflg=="Y")
@p outerjoin(
    unique(select(stock_names, :permno, :shrcd)),
    unique(select(stock_info_hist_subset, :permno, :sharetype, :securitytype, :securitysubtype, :usincflg, :issuertype)),
    on = :permno) |>
    groupby(__, [:shrcd, :sharetype, :securitytype, :securitysubtype, :usincflg, :issuertype]) |>
    combine(__, nrow)
```
!!There still seems to be some discrepancy!!
If we do not want to worry, we simply use the [CRSP cross reference guide](https://www.crsp.org/wp-content/uploads/guides/CRSP_Cross_Reference_Guide_1.0_to_2.0.pdf) which leads us to this [mapping table](https://www.crsp.org/wp-content/uploads/ShareCode.html
)


### Exchange Filters
The legacy filters set the exchange code variable to `1`, `2`, or `3` (respectively for NYSE, ASE, or Nasdaq).
There is almost a direct mapping for exchange filters, though it also relies on two flag variables `conditionaltype` and `TradingStatusFlg` to account for halted or suspended trading (which were previously `-1` and `-2`).
Thus new version of the filter would read:
```julia
stock_info_hist_subset = @rsubset(stock_info_hist, :primaryexch ∈ ["N", "A", "Q"])
unique(innerjoin(stock_names, select(stock_info_hist_subset, :permno, :primaryexch), on = :permno), [:exchcd, :primaryexch])
```

To remove halted trading we can filter the additional flags:
```julia
stock_info_hist_subset = @rsubset(stock_info_hist, 
    :primaryexch ∈ ["N", "A", "Q"], :conditionaltype == "RW", :tradingstatusflg == "A")
get_info_flag("conditionaltype") |> see
get_info_flag("TradingStatusFlg") |> see
```





