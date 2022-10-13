# Inside make.jl
push!(LOAD_PATH,"../src/")

using FinanceRoutines
using Documenter
makedocs(
         sitename = "FinanceRoutines.jl",
         modules  = [FinanceRoutines],
         pages=[
                "Home" => "index.md"
               ])
deploydocs(;
    repo="github.com/eloualiche/FinanceRoutines.jl",
)
