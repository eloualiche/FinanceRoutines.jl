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
using DataFrames, DataFramesMeta, Dates
using Prototypes # see https://github.com/eloualiche/Prototypes.jl
wrds_conn = FinanceRoutines.open_wrds_pg();
```

## CRSP: Filtering on names

To get information on what columns are available, query the `information_schema`:
```julia
postgre_query_msenames_columns= """
    SELECT *
        FROM information_schema.columns
    WHERE table_schema = 'crsp'
      AND table_name   = 'msenames';
"""
res_q = execute(wrds_conn, postgre_query_msenames_columns)
msenames_columns = DataFrame(columntable(res_q)).column_name |> sort
```



Now imagine that we want to pull columns from the monthly stock file with the following conditions: a given time frame (e.g. the 2000s) and company names that contain "AP".
We would get the `permno` that match from `msenames` and match it on the fly to `msf`
```julia
postgre_query_msf_with_filter = """
    SELECT msf.cusip, msf.permno, msf.date, msf.prc, msf.ret, msf.vol, msf.shrout, msf.hsiccd,
        msenames.comnam
        FROM crsp.msf
    INNER JOIN crsp.msenames msenames ON msf.permno = msenames.permno
        WHERE msenames.comnam ~ '(^APPLE|TESLA)'
        AND msf.date >= '2010-01-01'
        AND msf.date <= '2019-12-31';
"""
res_q = execute(wrds_conn, postgre_query_msf_with_filter) # this runs in under 1 second (including download)
df_msf = DataFrame(columntable(res_q))
tabulate(df_msf, [:permno, :comnam])

 permno  comnam                     │ Freq.  Percent  Cum
────────────────────────────────────┼──────────────────────
 14593   APPLE COMPUTER INC         │  360    0.339   0.34
 14593   APPLE INC                  │  240    0.226   0.56
 15338   APPLE HOSPITALITY REIT INC │  114    0.107   0.67
 93436   TESLA MOTORS INC           │  116    0.109   0.78
 93436   TESLA INC                  │  232    0.218   1.00
 ```






