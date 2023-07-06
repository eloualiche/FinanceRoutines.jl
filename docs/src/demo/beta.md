# Estimating Beta

## Download the monthly stock file from CRSP

Since we are about to download data from CRSP we set up a connection with our WRDS credentials
```julia
using FinanceRoutines
using DataFrames, DataFramesMeta, Dates
using FixedEffectModels # for regressions
wrds_conn = FinanceRoutines.open_wrds_pg()
```

```julia
# Import the monthly stock file
df_msf = import_MSF(wrds_conn; date_range = (Date("1980-01-01"), Dates.today())); 
df_msf = build_MSF!(df_msf);    # Run common processing
# keep only what we need from the MSF
select!(df_msf, :permno, :date, :datem, :ret, :mktcap)
```

## Download the Fama-French three pricing factors from Ken French's website


This downloads directly data from Ken French's website and formats the data

```julia
df_FF3 = import_FF3()
# make sure the returns are expressed in the same unit as in the MSF
transform!(df_FF3, [:mktrf, :smb, :hml, :rf] .=> ByRow((x->x/100)), renamecols=false )
```

## Merge the data and estimate beta

```julia
# Merge the data
df_msf = leftjoin(df_msf, df_FF3, on = [:datem] )
# Create excess return
@rtransform!(df_msf, :ret_rf = :ret - :rf)

# Estimate CAPM beta over the whole sample
sort!(df_msf, [:permno, :date])
for subdf in groupby(df_msf, :permno)
    if size(dropmissing(subdf, [:ret_rf, :mktrf]))[1] > 2
        β_CAPM = coef(reg(subdf, @formula(ret_rf ~ mktrf)))[2]
        subdf[:, :β_CAPM ] .= β_CAPM
    else
        subdf[:, :β_CAPM ] .= missing
    end
end
select(unique(df_msf, [:permno, :β_CAPM]), :permno, :β_CAPM)

# Estimate 3 Factor betas
for subdf in groupby(df_msf, :permno)
    if size(dropmissing(subdf, [:ret_rf, :mktrf, :smb, :hml]))[1] > 2
        β_MKT, β_SMB, β_HML = coef(reg(subdf, @formula(ret_rf ~ mktrf + smb + hml)))[2:4]
        subdf[:, :β_MKT ] .= β_MKT
        subdf[:, :β_SMB ] .= β_SMB
        subdf[:, :β_HML ] .= β_HML
    else
        subdf[:, :β_MKT ] .= missing; 
        subdf[:, :β_SMB ] .= missing; 
        subdf[:, :β_HML ] .= missing
    end
end
unique(df_msf, r"β")
select(unique(df_msf, r"β"), :permno, :β_MKT, :β_SMB, :β_HML)
```

## Rolling betas

...





