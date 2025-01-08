# --------------------------------------------------------------------------------------------------
using FinanceRoutines
using Test

import DataFrames: DataFrame, nrow, rename!
import Dates: Date, year
import LibPQ: Connection

const testsuite = [
    "KenFrench", "WRDS"
]
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

    # just for checking things on the fly
    @testset "Debugging tests ..." begin
        @test FinanceRoutines.greet_FinanceRoutines() == "Hello FinanceRoutines!"
        @test FinanceRoutines.greet_FinanceRoutines() != "Hello world!"

        WRDS_USERNAME = get(ENV, "WRDS_USERNAME", "")
        WRDS_PWD = get(ENV, "WRDS_PWD", "")
        @test !isempty(WRDS_USERNAME)
        @test !isempty(WRDS_PWD)
    end

    # Actual tests
    for test in testsuite
        println("\033[1m\033[32m  → RUNNING\033[0m: $(test)")
        include("$test.jl")
        println("\033[1m\033[32m  PASSED\033[0m")
    end

end
# --------------------------------------------------------------------------------------------------
