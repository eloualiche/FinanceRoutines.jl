@testset "Treasury Yields (GSW)" begin

    import Dates: Date
    import Statistics: mean, std

    df_GSW = import_GSW(date_range = (Date("1970-01-01"), Date("1989-12-31")) )
    @test names(df_GSW) == ["date", "BETA0", "BETA1", "BETA2", "BETA3", "TAU1", "TAU2"]

    estimate_yield_GSW!(df_GSW; maturity=1)
    estimate_price_GSW!(df_GSW; maturity=1)
    estimate_return_GSW!(df_GSW; maturity=2, frequency=:daily, type=:log)

    transform!(df_GSW, :date => (x -> year.(x) .÷ 10 * 10) => :date_decade)

    df_stats = combine(
        groupby(df_GSW, :date_decade),
        :yield_1y => ( x -> mean(skipmissing(x)) ) => :mean_yield,
        :yield_1y => ( x -> sqrt(std(skipmissing(x))) ) => :vol_yield,
        :price_1y => ( x -> mean(skipmissing(x)) ) => :mean_price,
        :price_1y => ( x -> sqrt(std(skipmissing(x))) ) => :vol_price,
        :ret_2y_daily => ( x -> mean(skipmissing(x)) ) => :mean_ret_2y_daily,
        :ret_2y_daily => ( x -> sqrt(std(skipmissing(x))) ) => :vol_ret_2y_daily
    )

    @test df_stats[1, :mean_yield] < df_stats[2, :mean_yield]
    @test df_stats[1, :vol_yield] < df_stats[2, :vol_yield]
    @test df_stats[1, :mean_price] > df_stats[2, :mean_price]
    @test df_stats[1, :vol_price] < df_stats[2, :vol_price]
    @test df_stats[1, :mean_ret_2y_daily] < df_stats[2, :mean_ret_2y_daily]
    @test df_stats[1, :vol_ret_2y_daily] < df_stats[2, :vol_ret_2y_daily]

end
