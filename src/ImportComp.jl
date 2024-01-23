# ------------------------------------------------------------------------------------------
# ImportComp.jl

# Collection of functions that import 
#  compustat data into julia

# List of exported functions
# export import_Funda 
# export build_Funda
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
"""
    import_Funda(wrds_conn; date_range, variables)
    import_Funda(; 
        date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
        variables::String = "", user="", password="")

Import the funda file from CapitalIQ Compustat on WRDS Postgres server

# Arguments
- `wrds_conn::Connection`: An existing Postgres connection to WRDS; creates one if empty

# Keywords
- `date_range::Tuple{Date, Date}`: A tuple of dates to select data (limits the download size)
- `variables::Vector{String}`: A vector of String of additional variable to include in the download
- `user::String`: username to log into the WRDS cli; default to ask user for authentication
- `password::String`: password to log into the WRDS cli

# Returns
- `df_funda::DataFrame`: DataFrame with compustat funda file
"""
function import_Funda(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::Vector{String} = nothing,
    filter_variables = Dict(:CURCD=>"USD")  # if you want something fanciers ... export variable and do it later
    )

    var_funda = ["GVKEY", "DATADATE", "SICH", "FYR", "FYEAR", 
                 "AT", "LT", "SALE", "EBITDA", "CAPX", "NI", "DV", "CEQ", "CEQL", "SEQ",
                 "TXDITC", "TXP", "TXDB", "ITCB", "DVT", "PSTK","PSTKL", "PSTKRV"]
    !isnothing(variables) && append!(var_funda, uppercase.(variables))
    !isnothing(filter_variables) && append!(var_funda, uppercase.(string.(keys(filter_variables))))

# set up the query for msf
    postgre_query_funda_full = """
        SELECT *
            FROM comp.funda
            WHERE INDFMT = 'INDL' AND DATAFMT = 'STD' AND CONSOL = 'C' AND POPSRC = 'D'
                AND DATADATE >= '$(string(date_range[1]))' 
                AND DATADATE <= '$(string(date_range[2]))'
    """
    postgre_query_funda_var = """
        SELECT $(join(unique(var_funda), ","))
            FROM comp.funda
            WHERE INDFMT = 'INDL' AND DATAFMT = 'STD' AND CONSOL = 'C' AND POPSRC = 'D'
                AND DATADATE >= '$(string(date_range[1]))' 
                AND DATADATE <= '$(string(date_range[2]))'
    """
    res_q_funda = execute(wrds_conn, postgre_query_funda_var)
    df_funda = DataFrame(columntable(res_q_funda));

    # run the filter
    !isnothing(filter_variables) && for (key, value) in Dict(lowercase(string(k)) => v for (k, v) in filter_variables)
        filter!(row -> (ismissing(row[key])) | (row[key] == value), df_funda) # we keep missing ... 
    end

    # clean up the dataframe
    transform!(df_funda, 
        names(df_funda, check_integer.(eachcol(df_funda))) .=> (x->convert.(Union{Missing, Int}, x)); 
        renamecols = false)
    df_funda[!, :gvkey] .= parse.(Int, df_funda[!, :gvkey]);

    return df_funda

end

function import_Funda(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::String = nothing,
    filter_variables::Dict{Symbol, Any} = Dict(:CURCD=>"USD"),
    user::String = "", password::String = "")

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end

    import_Funda(wrds_conn, date_range=date_range, variables=variables)
end
# ------------------------------------------------------------------------------------------




# ------------------------------------------------------------------------------------------
"""
    build_Funda!(df_funda::DataFrame; save)

Clean up the compustat funda file download from crsp (see `import_Funda`)

# Arguments
- `df_funda::DataFrame`: A standard dataframe with compustat data (minimum variables are in `import_Funda`)

# Keywords
- `save::String`: Save a gzip version of the data on path `\$save/funda.csv.gz`; Default does not save the data.

# Returns
- `df_funda::DataFrame`: DataFrame with compustat funda file "cleaned"
"""
function build_Funda!(df::DataFrame;
    save::String = "",
    verbose::Bool = false
    )
    
    verbose && (@info "--- Creating clean funda panel")

    # define book equity value
    verbose && (@info ". Creating book equity")
    @transform!(df, :be = 
        coalesce(:seq, :ceq + :pstk, :at - :lt) + coalesce(:txditc, :txdb + :itcb, 0) -
        coalesce(:pstkrv, :pstkl, :pstk, 0) )
    df[ isless.(df.be, 0), :be] .= missing;
    @rtransform!(df, :date_y = year(:datadate));
    sort!(df, [:gvkey, :date_y, :datadate]) 
    unique!(df, [:gvkey, :date_y], keep=:last) # last obs

    verbose && (@info ". Cleaning superfluous columns INDFMT, etc.")
    select!(df_funda, Not(intersect(names(df_funda), ["indfmt","datafmt","consol","popsrc", "curcd"])) )

    if !(save == "")
        verbose && (@info ". Saving to $save/funda.csv.gz")
        CSV.write(save * "/funda.csv.gz", df, compress=true)
    end

    return df
end
# ------------------------------------------------------------------------------------------

