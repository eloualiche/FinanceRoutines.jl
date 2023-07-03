module FinanceRoutines


# ------------------------------------------------------------------------------------------
import Downloads
import ZipFile
import CSV
import DataFrames: DataFrame, ByRow, groupby, nrow, passmissing, Not,
  rename!, select!, groupby, transform!, leftjoin, disallowmissing!
import DataFramesMeta: DataFramesMeta, 
  @passmissing, @subset!, @rsubset!, @transform!, @rtransform!
import Dates: Dates, Date, Month, year
import Downloads: Downloads.download
import FlexiJoins: innerjoin, by_key, by_pred
import IntervalSets:(..)
import LibPQ: LibPQ.execute, LibPQ.Connection
import Missings: Missings, missing
import MonthlyDates: MonthlyDate
import PanelShift: panellag!
import ShiftedArrays: lag
import Tables: columntable
import WeakRefStrings: String3, String7, String15
import ZipFile: ZipFile.Reader
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# Import functions
include("Utilities.jl")
include("ImportFinanceData.jl")
include("ImportCRSP.jl")
include("ImportComp.jl")
include("Merge_CRSP_Comp.jl")
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# List of exported functions
export greet_FinanceRoutines  # for debugging

# WRDS
# -- CRSP
export import_MSF             # import Monthly Stock File
export import_DSF             # import Daily Stock File
export build_MSF              # clean Monthly Stock File
# -- Funda
export import_Funda
export build_Funda
# -- Link
export link_Funda
export link_MSF

# FF
export import_FF3
# ------------------------------------------------------------------------------------------


end
