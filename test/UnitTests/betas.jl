@testset verbose=true "betas tests ... " begin

    import Dates: Date, Month
    import Statistics: mean


# -- generate a test dataframe
    function gen_dataset(; 
        return_noise = 0.0,
        missing_ratio = 0.0
        )

        dates = [ 
            Date(2010,1,1):Month(1):Date(2022,12,31),
            Date(2017,1,1):Month(1):Date(2024,12,31),
            Date(2000,1,1):Month(1):Date(2015,12,31)
            ]
        n = length(dates)
        firms = [1, 2, 3]
        α = [0, 0.1, 0.05]
        β = [0.8 1 1.2; 0.2 -0.3 0; -0.8 0.1 0]

        # generate aggregate factors
        date_factors = minimum(minimum.(dates)):Month(1):maximum(maximum.(dates))
        df_factors = DataFrame(
            datem = date_factors,
            mkt = accumulate((x,ϵ) -> 0.9*x + 0.1 * ϵ, randn(length(date_factors))),
            F1  = accumulate((x,ϵ) -> 0.5*x + 0.5 * ϵ, randn(length(date_factors))), 
            F2  = accumulate((x,ϵ) -> 0.05*x + 0.9 * ϵ, randn(length(date_factors)))
            )
        
        df_firms = [
            leftjoin(
                DataFrame(
                    datem = dates[i], firm_id = firms[i], 
                    α = α[i], βmkt = β[1, i], βF1 = β[2, i], βF2 = β[3, i]),
                df_factors, on = :datem)
            for i in 1:3]
        df_firms = reduce(vcat, df_firms)
        
        # -- estimate the return with some noise
        # @transform!(df_firms, 
        #     :ret = :α + :βmkt .* :mkt + :βF1 .* :F1 + :βF2 .* :F2 + 0.0.*randn(nrow(df_firms)) )
        transform!(df_firms,
            AsTable([:α, :βmkt, :mkt, :βF1, :F1, :βF2, :F2]) => 
            (n -> n.α + n.βmkt .* n.mkt + n.βF1 .* n.F1 + n.βF2 .* n.F2 + 0.0.*randn(nrow(df_firms))) => 
            :ret)
        allowmissing!(df_firms, :ret)

        # put some random missing ...     
        df_firms[rand(1:nrow(df_factors), round(Int, missing_ratio*nrow(df_factors))), :ret] .= missing;
        sort!(df_firms, [:firm_id, :datem])

        return df_firms
    end


# -- test the function when we have fixed betas ... but we are running rolling regressions
    df_firms = gen_dataset()
    insertcols!(df_firms, :a => missing, :bmkt => missing, :bF1 => missing, :bF2 => missing)

    for subdf in groupby(df_firms, :firm_id)
        β = calculate_rolling_betas(
            [ones(nrow(subdf)) subdf.mkt subdf.F1 subdf.F2],
            subdf.ret; 
            window=60, min_data=nothing
        )
        # Create and assign columns for β coefficients
        subdf[!, [:a, :bmkt, :bF1, :bF2]] = β
    end
    
    df_test1 = @p df_firms |>
        subset(__, :a => ByRow(x -> !ismissing(x))) |>
        select(__, :datem, :firm_id, 
            [:a, :α]       => ByRow((x,y) -> x - y) => :Δ_a, 
            [:bmkt, :βmkt] => ByRow((x,y) -> x - y) => :Δ_bmkt, 
            [:bF1, :βF1]   => ByRow((x,y) -> x - y) => :Δ_bF1, 
            [:bF2, :βF2]   => ByRow((x,y) -> x - y) => :Δ_bF2) |> 
        groupby(__, :firm_id) |>
        combine(__, [:Δ_a, :Δ_bmkt, :Δ_bF1, :Δ_bF2] .=> mean, renamecols=false)
    
    @test isapprox.(0.0,
        maximum(abs.(Array(combine(df_test1, [:Δ_a, :Δ_bmkt, :Δ_bF1, :Δ_bF2] .=> mean)[1, :]))),
        atol = 1E-10)



end









