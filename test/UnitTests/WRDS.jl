@testset verbose=true "WRDS tests ... downloads and build" begin

    import Dates: Date, year, day
    import LibPQ: Connection, execute
    import Tables: columntable


    WRDS_USERNAME = get(ENV, "WRDS_USERNAME", "");
    WRDS_PWD = get(ENV, "WRDS_PWD", "");

    wrds_conn = FinanceRoutines.open_wrds_pg(WRDS_USERNAME, WRDS_PWD)
    @test typeof(wrds_conn) == Connection


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP MSF" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP MSF")
        df_msf = import_MSF(wrds_conn; date_range = (Date("2000-01-01"), Date("2002-01-01")));
        build_MSF!(df_msf; clean_cols=true);

        @test minimum(skipmissing(df_msf.date)) >= Date("2000-01-01")
        @test maximum(skipmissing(df_msf.date)) <= Date("2002-01-01")
        @test nrow(df_msf) > 100_000
    end


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP MSF V2" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP MSF V2")

        # I have not developped against the new version of crsp
        # this will need to happen as this is the only version that will get released going forward
        # https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/stocks-and-indices/crsp-stock-and-indexes-version-2/crsp-ciz-faq/

        postgre_query_msf = """
        SELECT *
            FROM crsp.msf_v2
            WHERE mthcaldt >= '2000-01-01' AND mthcaldt <= '2002-01-01'
        """
        res_q_msf = execute(wrds_conn, postgre_query_msf)
        df_msf = DataFrame(columntable(res_q_msf))
        # probably stale prices since we do not abs(prc) != prc
        @test subset(df_msf, [:mthcaldt, :mthprcdt] => (x,y) -> isequal.(x, y) ) |> nrow > 0
        @test subset(df_msf, :mthprc => ByRow(x -> !isequal(x, abs(x))) ) |> nrow == 0

    end 


    # ----------------------------------------------------------------------------------------- #
    @testset "CRSP DSF" begin
        println("\033[1m\033[32m    → running\033[0m: CRSP DSF")
        df_dsf = import_DSF(wrds_conn; date_range = (Date("2002-02-01"), Date("2002-02-05")) )

        @test nrow(df_dsf) > 20_000
        @test size(unique(day.(df_dsf.date)), 1)  > 1
        @test all(map(s -> s in names(df_dsf),
            lowercase.(["PERMNO", "DATE", "RET", "PRC", "SHROUT", "VOL"])))
    end


    # ----------------------------------------------------------------------------------------- #
    @testset "Compustat FUNDA" begin
        println("\033[1m\033[32m    → running\033[0m: Compustat FUNDA")
        df_funda = import_Funda(wrds_conn;
            date_range = (Date("2000-01-01"), Date("2002-01-01")),
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


