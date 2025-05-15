# Estimating Stock Betas

This describes the step by step procedure to estimate betas stock by stock first unconditionally and then using rolling windows.
We first download the monthly stock file.


Since we are about to download data from CRSP we set up a connection with our WRDS credentials
```julia
using FinanceRoutines
using DataFrames, DataPipes, Dates
using FixedEffectModels # for regressions

const wrds_conn = FinanceRoutines.open_wrds_pg()
const date_init = Date("1990-01-01")
```

We are ready to import the monthly stock file:
```julia
df_msf_raw = import_MSF_v2(wrds_conn; date_range = (date_init, Dates.today())); 
```

And the Fama-French three pricing factors from Ken French's website.
This downloads directly data from Ken French's website and formats the data
```julia
df_FF3 = import_FF3()
# make sure the returns are expressed in the same unit as in the MSF
transform!(df_FF3, [:mktrf, :smb, :hml, :rf] .=> ByRow((x->x/100)), renamecols=false )
```


## Unconditional Stock Betas


### Format the monthly stock file from CRSP

```julia
# keep only what we need from the MSF
df_msf = select(df_msf_raw, :permno, :mthcaldt => :date, :datem, 
    [:mthret, :mthcap] .=> ByRow(passmissing(Float64)) .=> [:ret, :mthcap]) # convert from decimals
```


### Merge the data and estimate beta

```julia
# Merge the data
df_msf = leftjoin(df_msf, df_FF3, on = [:datem] )
# Create excess return
transform!(df_msf, [:ret, :rf] => ( (r1, r0) -> r1 .- r0 ) => :ret_rf)

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



## Rolling Betas for Stocks

I export a very simple function for rolling betas (see the test for examples). 

First we prepare the basic dataset from the monthly stock file and the Fama-French risk factors for example
```julia
# Get individual stock returns
df_msf = select(df_msf_raw, :permno, :mthcaldt => :date, :datem, 
    [:mthret, :mthcap] .=> ByRow(passmissing(Float64)) .=> [:ret, :mthcap]) # convert from decimals
# merge and create excess returns
df_msf = leftjoin(df_msf, df_FF3, on = [:datem] )
transform!(df_msf, [:ret, :rf] => ( (r1, r0) -> r1 .- r0 ) => :ret_rf)
sort!(df_msf, [:permno, :date])
```

Now we are ready to run the regression using the function `calculate_rolling_betas` that the package exports
```julia
insertcols!(df_msf, :a=>missing, :bMKT=>missing, :bSMB=>missing, :bHML=>missing)
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
@p df_msf |> groupby(__, :datem) |> 
    combine(__, :bMKT .=> 
        [(x-> emptymissing(mean)(skipmissing(x))) (x-> emptymissing(median)(skipmissing(x)))] .=>
        [:bMKT_mean :bMKT_median])
```
Go make some coffee ... this takes a little while (~ 15mn on M2max macbook pro). 
I don't think my method is super efficient 



