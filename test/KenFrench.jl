@testset "Importing Fama-French factors from Ken French library" begin
        # import_FF3
        df_FF3 = FinanceRoutines.import_FF3();
        @test names(df_FF3) == ["datem", "mktrf", "smb", "hml",  "rf"];
        @test (nrow(df_FF3) >= 1000 & nrow(df_FF3) <= 1250);
        df_FF3_daily = FinanceRoutines.import_FF3(:daily);
        @test names(df_FF3_daily) == ["date", "mktrf", "smb", "hml",  "rf"]
        @test (nrow(df_FF3_daily) >= 25_000 & nrow(df_FF3_daily) <= 26_000)
end
