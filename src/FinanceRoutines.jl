module FinanceRoutines


# ---------------------------------------------------------
import Downloads
import ZipFile
import CSV
import DataFrames: DataFrame, rename!
import DataFramesMeta: DataFramesMeta, @subset!, @transform!
import Dates: Date
# ---------------------------------------------------------


# ---------------------------------------------------------
# Import functions
include("ImportFinanceData.jl")
# ---------------------------------------------------------


# ---------------------------------------------------------
# List of exported functions
export greet_FinanceRoutines  # for debugging
export import_FF3             # read monthly FF3   
# ---------------------------------------------------------


end
