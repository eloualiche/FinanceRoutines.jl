# ------------------------------------------------------------------------------------------
# ImportCRSP.jl

# Collection of functions that import
#  financial data into julia
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# List of exported functions
# export import_MSF
# export build_MSF

# list
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# function list_crsp(;
#     wrds_conn, user, password)

#     list_libraries = """
#         WITH RECURSIVE "names"("name") AS (
#             SELECT n.nspname AS "name"
#                 FROM pg_catalog.pg_namespace n
#                 WHERE n.nspname !~ '^pg_'
#                     AND n.nspname <> 'information_schema')
#             SELECT "name"
#                 FROM "names"
#                 WHERE pg_catalog.has_schema_privilege(
#                     current_user, "name", 'USAGE') = TRUE;
#         """
#     res_list_libraries = execute(wrds_conn, list_libraries);
#     df_libraries = DataFrame(columntable(res_list_libraries))
#     @rsubset(df_libraries, occursin(r"crsp", :name) )

#     library = "crsp"
#     list_tables = """
#         SELECT table_name FROM INFORMATION_SCHEMA.views
#             WHERE table_schema IN ('$library');
#         """
#     res_list_tables = execute(wrds_conn, list_tables);
#     df_tables = DataFrame(columntable(res_list_tables))
#     @rsubset(df_tables, occursin(r"mse", :table_name) )

#     return run_sql_query(conn, query)


# end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
"""
    import_MSF(wrds_conn; date_range, variables)
    import_MSF(;
        date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
        variables::String = "", user="", password="")

Import the CRSP Monthly Stock File (MSF) from CRSP on WRDS PostGre server

# Arguments
- `wrds_conn::Connection`: An existing Postgres connection to WRDS; creates one if empty

# Keywords
- `date_range::Tuple{Date, Date}`: A tuple of dates to select data (limits the download size)
- `variables::Vector{String}`: A vector of String of additional variable to include in the download

# Returns
- `df_msf_final::DataFrame`: DataFrame with msf crsp file
"""
function import_MSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::Vector{String} = [""]
    )


    # download potential columns
    postgre_query_msenames_columns= """
    SELECT *
      FROM information_schema.columns
     WHERE table_schema = 'crsp'
       AND table_name   = 'msenames'
         ;
    """
    res_q = execute(wrds_conn, postgre_query_msenames_columns)
    msenames_columns = DataFrame(columntable(res_q)).column_name ;
    msenames_columns = intersect(
        uppercase.(msenames_columns),
        vcat(["PERMNO", "NAMEDT", "NAMEENDT", "SHRCD", "EXCHCD", "HEXCD", "NAICS", "HSICCD", "CUSIP"],
             uppercase.(variables)))
    msenames_columns = join(uppercase.(msenames_columns), ", ")

    postgre_query_msf_columns= """
    SELECT *
      FROM information_schema.columns
     WHERE table_schema = 'crsp'
       AND table_name   = 'msf'
         ;
    """
    res_q = execute(wrds_conn, postgre_query_msf_columns)
    msf_columns = DataFrame(columntable(res_q)).column_name ;
    msf_columns = intersect(
        uppercase.(msf_columns),
        vcat("PERMNO","PERMCO","DATE","PRC","ALTPRC","RET","RETX","SHROUT","CFACPR","CFACSHR",
             uppercase.(variables)))
    msf_columns = join(uppercase.(msf_columns), ", ")


# set up the query for msf
    postgre_query_msf = """
        SELECT $msf_columns
            FROM crsp.msf
            WHERE DATE >= '$(string(date_range[1]))' AND DATE <= '$(string(date_range[2]))'
    """
    res_q_msf = execute(wrds_conn, postgre_query_msf)
    df_msf = DataFrame(columntable(res_q_msf))
    transform!(df_msf,     # clean up the dataframe
        names(df_msf, check_integer.(eachcol(df_msf))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false);

    postgre_query_msenames = """
        SELECT $msenames_columns
            FROM crsp.msenames
    """
    res_q_msenames = execute(wrds_conn, postgre_query_msenames)
    df_msenames = DataFrame(columntable(res_q_msenames)) ;
    transform!(df_msenames,
        names(df_msenames, check_integer.(eachcol(df_msenames))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false) ;
    df_msenames[!, :cusip] .= String15.(df_msenames[!, :cusip]);
    df_msenames[ .!ismissing.(df_msenames.naics) , :naics] .= String7.(skipmissing(df_msenames[!, :naics]));
    @rsubset!(df_msenames, :exchcd <= 3 ) ;# we keep negative values
    @rsubset!(df_msenames, :shrcd ∈ (10, 11) ) ;

# set up the query for msedelist
    postgre_query_msedelist = """
        SELECT PERMNO, DLSTDT, DLRET, DLSTCD
            FROM crsp.msedelist
    """
    res_q_msedelist = execute(wrds_conn, postgre_query_msedelist)
    df_msedelist = DataFrame(columntable(res_q_msedelist)) ;
    transform!(df_msedelist,
        names(df_msedelist, check_integer.(eachcol(df_msedelist))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false) ;
    @rtransform!(df_msedelist, :datem = MonthlyDate(:dlstdt));

# --- merge all of the datasets together
    df_msf_final = innerjoin(
        (df_msf, df_msenames),
        by_key(:permno) & by_pred(:date, ∈, x->x.namedt..x.nameendt)
    )
    @rtransform!(df_msf_final, :datem = MonthlyDate(:date) );
    df_msf_final = leftjoin(df_msf_final, df_msedelist, on = [:permno, :datem])

    var_select = unique(vcat(
        :permno, # Security identifier
        :date, # Date of the observation
        :datem,
        :ret, # Return
        :retx, # Return excluding dividends
        :shrout, # Shares outstanding (in thousands)
        :prc,
        :altprc, # Last traded price in a month
        :exchcd, # Exchange code
        :hsiccd, # Industry code
        :naics, # Industry code
        :dlret, # Delisting return
        :dlstcd, # Delisting code
        Symbol.(intersect(variables, names(df_msf_final)))
    ))

    select!(df_msf_final, var_select)

    sort!(df_msf_final, [:permno, :date]);
    # unique(df_msf_final, [:permno, :date])

    return df_msf_final

end

# when there are no connections establisheds
function import_MSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::Vector{String} = [""],
    user::AbstractString = "", password::AbstractString = "")

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end
    import_MSF(wrds_conn, date_range=date_range, variables=variables)

end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
"""
    build_MSF!(df_msf::DataFrame; save, trim_cols, clean_cols, verbose)

Clean up the CRSP Monthly Stock File (see `import_MSF`)

# Arguments
- `df::DataFrame`: A standard dataframe with compustat data (minimum variables are in `import_Funda`)

# Keywords
- `save::String`: Save a gzip version of the data on path `\$save/funda.csv.gz`; Default does not save the data.
- `trim_cols::Bool`: Only keep a subset of relevant columns in the final dataset
- `clean_cols::Bool`: Clean up the columns of the dataframe to be of type Float64; Default is `false` and leaves the Decimal type intact

# Returns
- `df::DataFrame`: DataFrame with crsp MSF file "cleaned"
"""
function build_MSF!(
    df::AbstractDataFrame;
    save::AbstractString = "",
    trim_cols::Bool = false,
    clean_cols::Bool=false,
    verbose::Bool=false
    )

 # Check that all necessary variables are in
    ["mktcap", "shrout", "prc", "permno", "datem", "dlstcd", "ret", "dlret",
     "cfacpr", "cfacshr"]


# Create marketcap:
     @rtransform!(df, :mktcap = :shrout * abs(:prc));
    # @rtransform!(df, :mktcap = :shrout * :cfacshr * abs(:altprc) / :cfacpr) # in 1000s
    # in some instances (spin-offs and other distributions we have cfacpr not equal to cfacshr)
    df[ isequal.(df.mktcap, 0), :mktcap] .= missing;

# Lagged marketcap
    sort!(df, [:permno, :datem])
    # method 1: lag and then merge back
    # df_msf_mktcap_lag = @select(df_msf,
    #         :datem = :datem + Month(1), :permno, :l1m_mktcap2 = :mktcap)
    # df_msf = leftjoin(df_msf, df_msf_mktcap_lag, on = [:permno, :datem])
    panellag!(df, :permno, :datem,
        :mktcap, :l1m_mktcap, Month(1))

# Adjusted returns (see tidy finance following Bali, Engle, and Murray)
    @rtransform! df :ret_adj =
        ismissing(:dlstcd) ? :ret :
            !ismissing(:dlret) ? :dlret :
                (:dlstcd ∈ (500, 520, 580, 584)) || ((:dlstcd >= 551) & (:dlstcd <= 574)) ? -0.3 :
                    :dlstcd == 100 ? :ret : -1.0

# select variables and save
    if trim_cols
        select!(df, :permno, :date, :ret, :mktcap, :l1m_mktcap, :retx, :naics, :hsiccd)
    end

    if clean_cols
        verbose && (@info ". Converting decimal type columns to Float64.")
        for col in names(df)
            if eltype(df[!, col]) == Union{Missing,Decimal} || eltype(df[!, col]) <: Union{Missing,AbstractFloat}
                df[!, col] = convert.(Union{Missing,Float64}, df[!, col])
            elseif eltype(df[!, col]) == Decimal || eltype(df[!, col]) <: AbstractFloat
                df[!, col] = Float64.(df[!, col])
            end
        end
    end

    if !isempty(save)
        !isdir(save) && throw(ArgumentError("save argument referes to a non-existing directory: $save"))
        CSV.write(save * "/msf.csv.gz", df, compress=true)
    end

    return df
end

# --
function build_MSF(
    df::AbstractDataFrame;
    save::AbstractString = "",
    trim_cols::Bool = false,
    clean_cols::Bool=false,
    verbose::Bool=false
    )

    df_res = copy(df)
    build_MSF!(df_res, save = save, trim_cols = trim_cols, clean_cols = clean_cols, verbose = verbose)
    return df_res
end

# --
function build_MSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    save::AbstractString = "",
    trim_cols::Bool = false,
    clean_cols::Bool=false
    )

    df = import_MSF(wrds_conn; date_range=date_range);
    build_MSF!(df, save = save, trim_cols = trim_cols, clean_cols = clean_cols)

    return df
end

# --
function build_MSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    save::AbstractString = "",
    trim_cols::Bool = false,
    clean_cols::Bool=false
    )

    df = import_MSF(; date_range = date_range);
    build_MSF!(df, save = save, trim_cols = trim_cols, clean_cols = clean_cols)

    return df
end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
function import_DSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    )

# set up the query for msf
    postgre_query_dsf = """
        SELECT PERMNO, DATE, RET, PRC, SHROUT, VOL
            FROM crsp.dsf
            WHERE DATE >= '$(string(date_range[1]))' AND DATE <= '$(string(date_range[2]))'
    """
    res_q_dsf = execute(wrds_conn, postgre_query_dsf)
    df_dsf = DataFrame(columntable(res_q_dsf))
    # clean up the dataframe
    transform!(df_dsf,
        names(df_dsf, check_integer.(eachcol(df_dsf))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false)

    return df_dsf
end

# when there are no connections establisheds
function import_DSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::AbstractString = "",
    user::AbstractString = "", password::AbstractString = "")

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end

    return import_DSF(wrds_conn, date_range=date_range, variables=variables)
end
# ------------------------------------------------------------------------------------------
