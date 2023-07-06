# WRDS User Guide

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

