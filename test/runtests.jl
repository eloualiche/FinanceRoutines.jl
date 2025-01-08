# ---------------------------------------------------------
using FinanceRoutines
using Test

import DataFrames: DataFrame, nrow, rename!
import Dates: Date
# ---------------------------------------------------------


# ---------------------------------------------------------
@testset "FinanceRoutines.jl" begin
    # Write your tests here.

    @testset "For debugging ..." begin
    # FOR DEBUGGING
        @test FinanceRoutines.greet_FinanceRoutines() == "Hello FinanceRoutines!"
        @test FinanceRoutines.greet_FinanceRoutines() != "Hello world!"
    end

    @testset "Importing Fama-French factors from Ken French library" begin
        # import_FF3
        df_FF3 = FinanceRoutines.import_FF3();
        @test names(df_FF3) == ["datem", "mktrf", "smb", "hml",  "rf"];
        @test (nrow(df_FF3) >= 1000 & nrow(df_FF3) <= 1250);
        df_FF3_daily = FinanceRoutines.import_FF3(:daily);
        @test names(df_FF3_daily) == ["date", "mktrf", "smb", "hml",  "rf"]
        @test (nrow(df_FF3_daily) >= 25_000 & nrow(df_FF3_daily) <= 26_000)
    end

    @testset "WRDS tests ... deal with credentials and all" begin
        # build_crsp
        # wrds_conn = FinanceRoutines.open_wrds_pg()
        # df_msf = import_MSF(wrds_conn; date_range = (Date("2000-01-01"), Date("2002-01-01")));
        # build_MSF!(df_msf; clean_cols=true);
        WRDS_USERNAME = get(ENV, "WRDS_USERNAME", "")
        if isempty(WRDS_USERNAME)
            @warn "WRDS_USERNAME not found in environment variables"
        @test WRDS_USERNAME == "test"
    end

end
# ---------------------------------------------------------
