# ------------------------------------------------------------------------------------------
# ImportYields.jl

# Collection of functions that import Treasury Yields data
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
"""
    import_GSW(; date_range)

    GSW Curves

# arguments
    - `date_range::Tuple{Date, Date}`: range for selection of data

"""
function import_GSW(;
  date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()) )

# Download the curves from the Fed
    @info "Downloading GSW Yield Curve Tables"
    url_GSW = "https://www.federalreserve.gov/data/yield-curve-tables/feds200628.csv";
    http_response = Downloads.download(url_GSW);
    df_gsw = CSV.read(http_response, DataFrame, skipto=11, header=10)

  # clean up the table
    rename!(df_gsw, "Date" => "date");
    @p df_gsw |> filter!( (_.date >= date_range[1]) &&  (_.date <= date_range[2]) )
    select!(df_gsw, :date, :BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2);
    transform!(df_gsw, [:BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2] .=>
        ByRow(c -> tryparse(Float64, c) |> (x-> isnothing(x) ? missing : x) ), renamecols=false)


    return df_gsw

end

"""
    estimate_yield_GSW!

# arguments
    - `maturity::Real`: in years

"""
function estimate_yield_GSW!(df::DataFrame;
    maturity::Real=1)

    # @rtransform!(df,
    #     :y=NSSparamtoYield(maturity, :BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2) )
    transform!(df, 
        AsTable([:BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2]) => 
        ByRow(n -> NSSparamtoYield(maturity, n.BETA0, n.BETA1, n.BETA2, n.BETA3, n.TAU1, n.TAU2) ) =>
        :y)

    rename!(df, "y" => "yield_$(maturity)y")

end


"""
    estimate_price_GSW!

# arguments
    - `maturity::Real`: in years

"""
function estimate_price_GSW!(df::DataFrame;
    maturity::Real=1)

    transform!(df, 
        AsTable([:BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2]) => 
        ByRow(n -> NSSparamtoPrice(maturity, n.BETA0, n.BETA1, n.BETA2, n.BETA3, n.TAU1, n.TAU2) ) =>
        :y)


    rename!(df, "y" => "price_$(maturity)y")

end


"""
    estimate_return_GSW!

# arguments
    - `maturity::Real`: in years
    - `frequency::Symbol`: :daily, :monthly, :annual type
    - `type::Symbol`: :log or standard one-period arithmetic return

"""
function estimate_return_GSW!(df::DataFrame;
    maturity::Real=1, frequency::Symbol=:daily, type::Symbol=:log)

    if frequency==:daily
        Δmaturity = 1/360; Δdays = 1;
    elseif frequency==:monthly
        Δmaturity = 1 / 12; Δdays = 30;
    elseif frequency==:annual
        Δmaturity = 1; Δdays = 360;
    end

    sort!(df, :date)
    # @rtransform!(df,
    #     :p2=NSSparamtoPrice(maturity, :BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2),
    #     :p1=NSSparamtoPrice(maturity+Δmaturity, :BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2) );
    transform!(df, 
        AsTable([:BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2]) => 
        ByRow(n -> NSSparamtoPrice(maturity, 
                                   n.BETA0, n.BETA1, n.BETA2, n.BETA3, n.TAU1, n.TAU2) ) => :p2,
        AsTable([:BETA0, :BETA1, :BETA2, :BETA3, :TAU1, :TAU2]) => 
        ByRow(n -> NSSparamtoPrice(maturity + Δmaturity, 
                                   n.BETA0, n.BETA1, n.BETA2, n.BETA3, n.TAU1, n.TAU2) ) => :p1
        )

    transform!(df, [:date, :p1] => ((d,p) -> tlag(d, p, Day(Δdays))) => :lag_p1)
    if type==:log
        transform!(df, [:p2, :lag_p1] => ByRow( (p,lp) -> log(p/lp) ) => "ret_$(maturity)y_$(frequency)" );
    else
        transform!(df, [:p2, :lag_p1] => ByRow( (p,lp) -> (p-lp) / lp ) => "ret_$(maturity)y_$(frequency)" );
    end
    select!(df, Not([:lag_p1, :p1, :p2]) )
    select!(df, [:date, Symbol("ret_$(maturity)y_$(frequency)")],
                Not([:date, Symbol("ret_$(maturity)y_$(frequency)")]) )

    return df

end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
function NSSparamtoPrice(t, B0, B1, B2, B3, T1, T2)
  r = B0 .+ B1.*((1.0 .- exp.(-t/T1))/(t/T1))+ B2*(((1-exp(-t/T1))/(t/T1))-exp(-t/T1)) +
      B3*(((1-exp(-t/T2))/(t/T2))-exp(-t/T2))
  r = log(1 + r/100)
  p = exp(-r*t)
  return(p)
end

function NSSparamtoYield(t, B0, B1, B2, B3, T1, T2)
  r = B0 .+ B1.*((1.0 .- exp.(-t/T1))/(t/T1))+ B2*(((1-exp(-t/T1))/(t/T1))-exp(-t/T1)) +
      B3*(((1-exp(-t/T2))/(t/T2))-exp(-t/T2))
  return(r)
end
# ------------------------------------------------------------------------------------------


# --------------------------------------------------------------
# CLEAN UP BOND DATA
# @time df_gsw = CSV.File("./input/GSW_yield.csv", skipto=11, header=10, missingstring="NA") |> DataFrame;
# --------------------------------------------------------------
