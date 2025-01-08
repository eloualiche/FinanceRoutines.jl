# --------------------------------------------------------------------------------------------------
using FinanceRoutines
using Test

import DataFrames: DataFrame, nrow, rename!
import Dates: Date, year
import LibPQ: Connection
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
function printtestset(desc)
    # Get the current test set stack TODO
    ts = Test.get_testset_depth()
    # Calculate indent based on depth(fixed for now)
    indent = "  " ^ (length(ts))
    println(indent, "→ Running: ", desc)
end
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
# To run file locally (where environment variables are not defined by CI)
env_file = "../.env.gpg"
if isfile(env_file)
    io = IOBuffer(); run(pipeline(`which gpg`; stdout=io)); gpg_cmd = strip(String(take!(io)))
    io = IOBuffer();
    cmd = run(pipeline(`$gpg_cmd --decrypt $env_file`; stdout=io, stderr=devnull));
    env_WRDS = String(take!(io))
    # populate the environment variables
    for line in split(env_WRDS, "\n")
        !startswith(line, "#") || continue
        isempty(strip(line)) && continue
        if contains(line, "=")
            key, value = split(line, "=", limit=2)
            ENV[strip(key)] = strip(value)
        end
    end
end
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
@testset verbose=true "FinanceRoutines.jl" begin
    # Write your tests here.

    @testset "Debugging tests ..." begin
    # FOR DEBUGGING
        @test FinanceRoutines.greet_FinanceRoutines() == "Hello FinanceRoutines!"
        @test FinanceRoutines.greet_FinanceRoutines() != "Hello world!"
    end

    @testset "Importing Fama-French factors from Ken French library" begin
        printtestset("Ken French")
        # import_FF3
        df_FF3 = FinanceRoutines.import_FF3();
        @test names(df_FF3) == ["datem", "mktrf", "smb", "hml",  "rf"];
        @test (nrow(df_FF3) >= 1000 & nrow(df_FF3) <= 1250);
        df_FF3_daily = FinanceRoutines.import_FF3(:daily);
        @test names(df_FF3_daily) == ["date", "mktrf", "smb", "hml",  "rf"]
        @test (nrow(df_FF3_daily) >= 25_000 & nrow(df_FF3_daily) <= 26_000)
    end

    @testset "WRDS tests ... deal with credentials and all" begin
        WRDS_USERNAME = get(ENV, "WRDS_USERNAME", "")
        WRDS_PWD = get(ENV, "WRDS_PWD", "")
        @test !isempty(WRDS_USERNAME)
        @test !isempty(WRDS_PWD)
    end

    @testset verbose=true "WRDS tests ... downloads and build" begin
        printtestset("WRDS")
        WRDS_USERNAME = get(ENV, "WRDS_USERNAME", "");
        WRDS_PWD = get(ENV, "WRDS_PWD", "");

        wrds_conn = FinanceRoutines.open_wrds_pg(WRDS_USERNAME, WRDS_PWD)
        @test typeof(wrds_conn) == Connection

        @testset "CRSP MSF" begin
            printtestset("CRSP")
            df_msf = import_MSF(wrds_conn; date_range = (Date("2000-01-01"), Date("2002-01-01")));
            build_MSF!(df_msf; clean_cols=true);

            @test minimum(skipmissing(df_msf.date)) >= Date("2000-01-01")
            @test maximum(skipmissing(df_msf.date)) <= Date("2002-01-01")
            @test nrow(df_msf) > 100_000
        end

        @testset "Compustat FUNDA" begin
            printtestset("Compustat")
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

end
# --------------------------------------------------------------------------------------------------
