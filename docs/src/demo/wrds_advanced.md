# Advanced WRDS


The package provides ready-built query to the postgres WRDS database. 
The queries are the most common and used ones and allow to get a crsp/compustat dataset fairly quickly.

Some people will want to run their own queries. 
This is a little outside the scope of the package but I keep this page as a list of examples that can be useful.
Post an issue if you want to add to the list. 

We are always going to start by opening a connection to WRDS.
So for the rest of the examples I will assume that we include the preamble
```julia
using FinanceRoutines
using DataFrames, DataPipes, Dates
import LibPQ

using Prototypes # see https://github.com/eloualiche/Prototypes.jl
wrds_conn = FinanceRoutines.open_wrds_pg();
```

## CRSP: Filtering on names

To get information on what columns are available, query the `information_schema`:
```julia
postgre_query_columns= """
    SELECT *
        FROM information_schema.columns
    WHERE table_schema = 'crsp'
      AND table_name   = 'StkSecurityInfoHist';
"""
msenames_columns = @p LibPQ.execute(wrds_conn, postgre_query_msenames_columns) |> 
    DataFrame |> __.column_name |> sort 
```

Sometimes we want to know what are the tables available, for example the `meta` tables
```julia
postgre_query =  """
SELECT table_name, table_schema, table_type
    FROM information_schema.tables
    WHERE table_name LIKE 'meta%' AND table_schema = 'crsp'
"""
LibPQ.execute(wrds_conn, postgre_query) |> DataFrame 
```


Now imagine that we want to pull columns from the monthly stock file with the following conditions: a given time frame (e.g. the 2000s) and company names that contain "AP".
We would get the `permno` that match from `StkSecurityInfoHist` and match it on the fly to `msf`
```julia
postgre_query = """
SELECT msf.cusip, msf.permno, msf.mthcaldt, msf.mthprc, msf.mthret, msf.mthvol, msf.shrout, msf.siccd,
       stkinfo.issuernm
FROM crsp.msf_v2 AS msf
INNER JOIN crsp.StkSecurityInfoHist AS stkinfo 
  ON msf.permno = stkinfo.permno
WHERE stkinfo.issuernm ~ '(^APPLE|TESLA)'
  AND msf.mthcaldt >= '2010-01-01'
  AND msf.mthcaldt <= '2019-12-31';
"""
df_msf = LibPQ.execute(wrds_conn, postgre_query) |> DataFrame
tabulate(df_msf, [:permno, :issuernm])

 permno  issuernm                   │ Freq.  Percent  Cum           Hist.
────────────────────────────────────┼───────────────────────────────────────────────
 14593   APPLE COMPUTER INC         │  600    29.2    29   ███████████████████████▉
 14593   APPLE INC                  │  600    29.2    58   ███████████████████████▉
 15338   APPLE HOSPITALITY REIT INC │  280    13.6    72   ███████████▏
 93436   TESLA MOTORS INC           │  115     5.6    78   ████▋
 93436   TESLA INC                  │  460    22.4    100  ██████████████████▎
 ```






