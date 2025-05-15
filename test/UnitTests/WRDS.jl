@testset verbose=true "WRDS tests ... downloads and build" begin

    import Dates: Date, year, day
    import LibPQ: Connection, execute
    import Tables: columntable

    wrds_conn = FinanceRoutines.open_wrds_pg(
        get(ENV, "WRDS_USERNAME", ""), 
        get(ENV, "WRDS_PWD", ""))
    @test typeof(wrds_conn) == Connection

    date_range_test       = (Date("2000-01-01"), Date("2002-01-01"))
    date_range_test_daily = (Date("2002-02-01"), Date("2002-02-05"))

    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP MSF" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP MSF")
        df_msf = import_MSF(wrds_conn; date_range = date_range_test);
        build_MSF!(df_msf; clean_cols=true);    

        @test minimum(skipmissing(df_msf.date)) >= Date("2000-01-01")
        @test maximum(skipmissing(df_msf.date)) <= Date("2002-01-01")
        @test nrow(df_msf) > 100_000
    end


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP MSF V2" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP MSF V2")

        # new version CIZ of crsp msf
        df_msf_v2 = import_MSF_v2(wrds_conn; date_range = date_range_test, logging_level=:info)
      
        # @test subset(df_msf_v2, [:mthcaldt, :mthprcdt] => (x,y) -> isequal.(x, y) ) |> nrow > 0
        @test subset(df_msf_v2, :mthprc => ByRow(x -> !isequal(x, abs(x))) ) |> nrow == 0
        @test subset(df_msf_v2, :mthcap => (x -> isequal.(x, 0) ) ) |> nrow == 0

        @test minimum(skipmissing(df_msf_v2.mthcaldt)) >= Date("2000-01-01")
        @test maximum(skipmissing(df_msf_v2.mthcaldt)) <= Date("2002-01-01")
        @test nrow(df_msf_v2) > 100_000

        # discrepancy in nrow with df_msf_v2 ... 

    end 


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP DSF" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP DSF")
        df_dsf = import_DSF(wrds_conn; date_range = date_range_test_daily)

        @test nrow(df_dsf) > 20_000
        @test size(unique(day.(df_dsf.date)), 1) > 1
        @test all(map(s -> s in names(df_dsf),
            lowercase.(["PERMNO", "DATE", "RET", "PRC", "SHROUT", "VOL"])))
    end


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP DSF V2" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP DSF V2")
        df_dsf_v2 = import_DSF_v2(wrds_conn; date_range = date_range_test_daily)

        @test nrow(df_dsf_v2) > 20_000
        @test size(unique(day.(df_dsf_v2.dlycaldt)), 1) > 1
        @test all(map(s -> s in names(df_dsf_v2),
            lowercase.(["PERMNO", "DLYCALDT", "DLYRET", "DLYPRC", "DLYVOL", "DLYCAP"])))
    end



    # ----------------------------------------------------------------------------------------- #
    @testset "Compustat FUNDA" begin
        println("\033[1m\033[32m    → running\033[0m: Compustat FUNDA")
        df_funda = import_Funda(wrds_conn;
            date_range = date_range_test,
            variables=["PPENT", "NAICSH", "NAICS"])
        build_Funda!(df_funda; clean_cols=true)

        # check basic properties of the DataFrame (mainly that it has downloaded)
        @test minimum(skipmissing(df_funda.datey)) >= year(Date("2000-01-01"))
        @test maximum(skipmissing(df_funda.datey)) <= year(Date("2002-01-01"))
        @test nrow(df_funda) > 20_000

        # check that the variables are downloaded and in the dataframe
        @test all(map(s -> s in names(df_funda), lowercase.(["PPENT", "NAICSH"])))

    end

end


