# FinanceRoutines.jl

*Some useful tools to work with academic financial data in Julia*

## Introduction

This package provides a collection of source code *lexers* for various languages and markup
formats and a selection of *themes* that can be used to customise the style of the formatted
source code. Additional *lexer* definitions are straightforward to add and are based on the
regular expression lexing mechanism used by [Pygments](http://pygments.org/).

## Installation

`FinanceRoutines.jl` is a not yet a registered package.
You can install it from github  via

```julia
import Pkg
Pkg.add("https://github.com/eloualiche/FinanceRoutines.jl")
```

## Usage

  - Using WRDS (CRSP, Compustat, etc)
      + See the [WRDS User Guide](@ref) for an introduction to using the package to download data from WRDS

  - See the demo to how this integrates into standard estimations in the [Estimating Beta](@ref) demo.



## Other Resources

```@contents
Pages = ["index.md"]
Depth = 3
```

## Functions

```@docs
import_Funda
import_MSF
import_FF3
```

```@docs
build_Funda!
build_MSF!
```

```@docs
FinanceRoutines.open_wrds_pg
```

## Index

```@index
```