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

I export a very simple function for rolling betas (see the test for examples). 

First we prepare the basic dataset from the monthly stock file and the Fama-French risk factors for example
```julia
# Get individual stock returns
df_msf = build_MSF(date_range = (Date("1980-01-01"), Dates.today()), clean_cols=true); 
select!(df_msf, :permno, :date, :datem, :ret, :mktcap)
# Get the monthly factor returns
df_FF3 = import_FF3()
transform!(df_FF3, [:mktrf, :smb, :hml, :rf] .=> ByRow((x->x/100)), renamecols=false)
# merge and create excess returns
df_msf = leftjoin(df_msf, df_FF3, on = [:datem] )
@rtransform!(df_msf, :ret_rf = :ret - :rf)
sort!(df_msf, [:permno, :date])
```

Now we are ready to run the regression using the function `calculate_rolling_betas` that the package exports
```julia
@rtransform!(df_msf, :a=missing, :bMKT=missing, :bSMB=missing, :bHML=missing)

@time for subdf in groupby(df_msf, :permno)
    β = calculate_rolling_betas(
        [ones(nrow(subdf)) subdf.mktrf subdf.smb subdf.hml],
        subdf.ret_rf; 
        window=60,         # 60 months
        min_data=nothing,   # what is the minimum number of nonmissing data to return a proper number
        method=:linalg
    )
    subdf[!, [:a, :bMKT, :bSMB, :bHML]] = β
end

import Statistics: median, mean
combine(groupby(df_msf, :datem), :bMKT .=> 
    [(x-> emptymissing(mean)(skipmissing(x))) (x-> emptymissing(median)(skipmissing(x)))] .=>
    [:bMKT_mean :bMKT_median])
```
Go make some coffee ... this takes a little while (~ 15mn on M2max macbook pro). 
I don't think my method is super efficient 



