# FinanceRoutines.jl

*Some useful tools to work with academic financial data in Julia*

## Introduction

This package provides a collection of routines for academic finance work. 
This is useful to get started with a clean copy of asset prices from CRSP and a ad-hoc merge with the accounting data from the Compustat Funda file. 

I have also added utilities to download treasury yield curves (GSW) and Fama-French research factors.

This is still very much work in progress: file [issues](https://github.com/eloualiche/FinanceRoutines.jl/issues) for comments.


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

  - Demos to how this integrates into standard estimations
    + See how to estimate asset pricing betas in the [Estimating Beta](@ref) demo.


## Other Resources

There are multiple online resources on using the WRDS Postgres database and build the standard finance and accounting datasets:
  
  - Ian D. Gow and Tony Ding: *"Empirical Research in Accounting: Tools and Methods"*; available [here](https://iangow.github.io/far_book/)
  - Chen, Andrew Y. and Tom Zimmermann: *"Open Source Cross-Sectional Asset Pricing"*; 2022, 27:2; available [here](https://www.openassetpricing.com/code/)
  - Christoph Scheuch, Stefan Voigt, Patrick Weiss: *"Tidy Finance with R"*; 2023; Chapman & Hall; available [here](https://www.tidy-finance.org/r/)



## Index

```@index
```