module FinanceRoutines


# ------------------------------------------------------------------------------------------
import Downloads
import ZipFile
import CSV
import DataFrames: AbstractDataFrame, AsTable, DataFrame, ByRow, groupby, nrow, passmissing, Not,
  rename!, select, select!, subset!, transform!, leftjoin, disallowmissing!
import DataPipes: @p
import Dates: Dates, Date, Day, Month, year
import Decimals: Decimal
import Downloads: Downloads.download
import FlexiJoins
using FlexiJoins: by_key, by_pred
import GLM: coef, lm
import IntervalSets:(..)
import LibPQ: LibPQ.execute, LibPQ.Connection
import LinearAlgebra: qr
import Logging: Logging, with_logger, ConsoleLogger, @logmsg, Logging.Debug, Logging.Info, Logging.Warn, Logging.Error
import Missings: Missings, missing, disallowmissing
import PeriodicalDates: MonthlyDate
import PanelShift: panellag!, tlag
import ShiftedArrays: lag
import Tables: columntable
import WeakRefStrings: String3, String7, String15
import ZipFile: ZipFile.Reader
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# Import functions
include("Utilities.jl")
include("betas.jl")
include("ImportFamaFrench.jl")
include("ImportYields.jl")
include("ImportCRSP.jl")
include("ImportComp.jl")
include("Merge_CRSP_Comp.jl")
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# List of exported functions
export greet_FinanceRoutines  # for debugging

# Yields on Treasuries
export import_GSW
export estimate_yield_GSW!, estimate_price_GSW!, estimate_return_GSW!

# Fama-French data
export import_FF3

# WRDS
# -- CRSP
export import_MSF, import_MSF_v2    # import Monthly Stock File
export import_DSF, import_DSF_v2    # import Daily Stock File
export build_MSF, build_MSF!        # clean Monthly Stock File
# -- Funda
export import_Funda
export build_Funda!, build_Funda
# -- Link
export link_Funda
export link_MSF

# More practical functions
export calculate_rolling_betas


# ------------------------------------------------------------------------------------------


end
