using FinanceRoutines
using Test

@testset "FinanceRoutines.jl" begin
    # Write your tests here.

    # FOR DEBUGGING
    @test FinanceRoutines.greet_FinanceRoutines() == "Hello FinanceRoutines!"
    @test FinanceRoutines.greet_FinanceRoutines() != "Hello world!"

end
