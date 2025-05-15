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


    # -- GETTING COLUMN NAMES
    # download potential columns
    msenames_columns = get_postgres_columns("crsp", "msenames"; wrds_conn=wrds_conn,
        prior_columns = vcat(["PERMNO", "NAMEDT", "NAMEENDT", "SHRCD", "EXCHCD", "HEXCD", 
                              "NAICS", "HSICCD", "CUSIP"],
                            uppercase.(variables))
    )
    msenames_columns = join(uppercase.(msenames_columns), ", ")

    msf_columns = get_postgres_columns("crsp", "msf"; wrds_conn=wrds_conn,
        prior_columns = vcat(["PERMNO","PERMCO","DATE","PRC","ALTPRC","RET","RETX","SHROUT","CFACPR","CFACSHR"],
                             uppercase.(variables))
    )
    msf_columns = join(uppercase.(msf_columns), ", ")

    # -- GETTING MSF
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

    # -- GETTING MSENAMES
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
    @p df_msenames |> filter!(_.exchcd <= 3 && _.shrcd ∈ (10,11))
    
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
    transform!(df_msedelist, :dlstdt => ByRow(MonthlyDate) => :datem)

# --- merge all of the datasets together
    df_msf_final = FlexiJoins.innerjoin(
        (df_msf, df_msenames),
        by_key(:permno) & by_pred(:date, ∈, x->x.namedt..x.nameendt)
    )
    transform!(df_msf_final, :date => ByRow(MonthlyDate) => :datem)

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
    transform!(df, [:shrout, :prc] => ByRow( (s,p) -> s * abs(p) ) => :mktcap)
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
    transform!(df, 
        AsTable([:ret, :dlstcd, :dlret]) => 
        ByRow(r -> ismissing(r.dlstcd) ? r.ret :
                     !ismissing(r.dlret) ? r.dlret :
                       (r.dlstcd in (500, 520, 580, 584) || (551 <= r.dlstcd <= 574)) ? -0.3 : 
                         r.dlstcd == 100 ? r.ret : -1.0
            ) => :ret_adj)
    # @rtransform! df :ret_adj =
    #     ismissing(:dlstcd) ? :ret :
    #         !ismissing(:dlret) ? :dlret :
    #             (:dlstcd ∈ (500, 520, 580, 584)) || ((:dlstcd >= 551) & (:dlstcd <= 574)) ? -0.3 :
    #                 :dlstcd == 100 ? :ret : -1.0

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
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
"""
    import_MSF_v2(wrds_conn; date_range, variables, logging_level)
    import_MSF_v2(;
        date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
        variables::String = "", user="", password="")

Import the CRSP Monthly Stock File (MSF) from CRSP on WRDS PostGres server from the version 2.0 CIZ files

# Arguments
- `wrds_conn::Connection`: An existing Postgres connection to WRDS; creates one if empty

# Keywords
- `date_range::Tuple{Date, Date}`: A tuple of dates to select data (limits the download size)
- `variables::Vector{String}`: A vector of String of additional variable to include in the download
- `logging_level::Symbol`: How to log results

# Returns
- `df_msf_final::DataFrame`: DataFrame with msf crsp file
"""
function import_MSF_v2(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::Vector{String} = [""],
    logging_level::Symbol = :debug, # either none, debug, info etc... tbd 
    )


    
    # ----------------------------------------------------------------------------------------------
    # the easy way
    @log_msg "# -- GETTING MONTHLY STOCK FILE (CIZ) ... msf_v2"
    # msf_columns = get_postgres_columns("crsp", "msf_v2"; wrds_conn=wrds_conn) |> sort
    postgre_query_msf = """
        SELECT *
            FROM crsp.msf_v2
            WHERE MTHCALDT >= '$(string(date_range[1]))' AND MTHCALDT <= '$(string(date_range[2]))'
              AND SHARETYPE = 'NS' AND SECURITYTYPE = 'EQTY' AND SECURITYSUBTYPE = 'COM' 
              AND USINCFLG = 'Y' AND ISSUERTYPE IN ('ACOR', 'CORP')
              AND PRIMARYEXCH IN ('N', 'A', 'Q') AND CONDITIONALTYPE = 'RW' AND TRADINGSTATUSFLG = 'A'
    """
    df_msf_v2 = execute(wrds_conn, postgre_query_msf) |> DataFrame;
    transform!(df_msf_v2,     # clean up the dataframe
        names(df_msf_v2, check_integer.(eachcol(df_msf_v2))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false);
    # ----------------------------------------------------------------------------------------------


    #=
    # ----------------------------------------------------------------------------------------------
    # the hard way
    # ------
    log_msg("# -- GETTING MONTHLY STOCK FILE (CIZ) ... stkmthsecuritydata")
    msf_columns = get_postgres_columns("crsp", "stkmthsecuritydata"; wrds_conn=wrds_conn) # download potential columns
    # msf_columns = get_postgres_columns("crsp", "msf_v2"; wrds_conn=wrds_conn) # this one is pre-merged!
    msf_columns = join(uppercase.(msf_columns), ", ")

    # legacy SIZ to CIZ conversion of shrcd flag (see doc)    
    #                   conversion of exchcd flag (see doc)
    postgre_query_msf = """
        SELECT $msf_columns
            FROM crsp.stkmthsecuritydata
            WHERE MTHCALDT >= '$(string(date_range[1]))' AND MTHCALDT <= '$(string(date_range[2]))'
              AND SHARETYPE = 'NS' AND SECURITYTYPE = 'EQTY' AND SECURITYSUBTYPE = 'COM' 
              AND USINCFLG = 'Y' AND ISSUERTYPE IN ('ACOR', 'CORP')
              AND PRIMARYEXCH IN ('N', 'A', 'Q') AND CONDITIONALTYPE = 'RW' AND TRADINGSTATUSFLG = 'A'
    """
    df_msf_v2 = execute(wrds_conn, postgre_query_msf) |> DataFrame;
    transform!(df_msf_v2,     # clean up the dataframe
        names(df_msf_v2, check_integer.(eachcol(df_msf_v2))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false);

    # subset!(df_msf_v2, [:sharetype, :securitytype, :securitysubtype, :usincflg, :issuertype] => 
    #                 ByRow( (sh, sec, secsub, usinc, issue) -> 
    #                 sh == "NS" && sec == "EQTY" && secsub == "COM" && usinc == "Y" && issue ∈ ["ACOR", "CORP"]) )
    # # legacy SIZ to CIZ conversion of exchcd flag (see doc)
    # subset!(df_msf_v2, 
    #     :primaryexch => ByRow(p -> p ∈ ["N", "A", "Q"]), 
    #     :conditionaltype => ByRow(c -> c == "RW"), :tradingstatusflg => ByRow(t -> t == "A") )

    
    # -- need to get shrout
    # stkshares = get_postgres_columns("crsp", "stkshares"; wrds_conn=wrds_conn)
    postgre_query_stkshares = """
    SELECT * FROM crsp.stkshares
        WHERE SHRSTARTDT >= '$(string(date_range[1]))' AND SHRENDDT <= '$(string(date_range[2]))'
    """
    # df_stkshares = execute(wrds_conn, postgre_query_stkshares) |> DataFrame;
    df_stkshares = execute(wrds_conn, "SELECT permno, shrstartdt, shrenddt, shrout FROM crsp.stkshares") |> DataFrame;

    # -- no need for delisting returns (already integrated)
    @time df_msf_v2 = FlexiJoins.innerjoin(
        (disallowmissing(df_msf_v2, :mthcaldt),
         disallowmissing(select(df_stkshares, :permno, :shrstartdt, :shrenddt, :shrout), 
                         [:permno, :shrstartdt, :shrenddt]) ),
        by_key(:permno) & by_pred(:mthcaldt, ∈, x->x.shrstartdt..x.shrenddt) )
    # ----------------------------------------------------------------------------------------------
    =#


    # ----------------------------------------------------------------------------------------------
    # ------
    @log_msg "# -- GETTING StkSecurityInfoHist (CIZ)"
    # stksecurityinfo = get_postgres_columns("crsp", "stksecurityinfohist"; wrds_conn=wrds_conn)
    stksecurityinfo_cols = vcat(
        ["PERMNO", "SecInfoStartDt", "SecInfoEndDt", "IssuerNm", "ShareClass", 
         "PrimaryExch", "TradingStatusFlg", "NAICS", "SICCD", "HDRCUSIP"],
        uppercase.(variables)) |> filter(!isempty) |> unique 
    stksecurityinfo = get_postgres_columns("crsp", "stksecurityinfohist"; wrds_conn=wrds_conn,
        prior_columns = stksecurityinfo_cols) |> sort
    stksecurityinfo_cols = join(uppercase.(stksecurityinfo_cols), ", ")

    postgre_query_stksecurityinfo = "SELECT $stksecurityinfo_cols FROM crsp.stksecurityinfohist"
    df_stksecurityinfo = execute(wrds_conn, postgre_query_stksecurityinfo) |> DataFrame;
    transform!(df_stksecurityinfo,
        names(df_stksecurityinfo, check_integer.(eachcol(df_stksecurityinfo))) .=> 
            (x->convert.(Union{Missing, Int}, x));
        renamecols = false) ;
    disallowmissing!(df_stksecurityinfo, [:permno, :secinfostartdt, :secinfoenddt, :issuernm, :hdrcusip])

    # ------
    @log_msg "# -- MERGING STOCK PRICES, INFO FILE"
    # we do left-join here because we dont want to lose obs. 
    df_msf_v2 = FlexiJoins.leftjoin( 
        (df_msf_v2, df_stksecurityinfo),
        by_key(:permno) & by_pred(:mthcaldt, ∈, x->x.secinfostartdt..x.secinfoenddt) )
    # ----------------------------------------------------------------------------------------------


    # ----------------------------------------------------------------------------------------------
    var_select = vcat(
        :permno,   # Security identifier
        :mthcaldt, # Date of the observation
        :mthret, # Return
        :mthretx, # Return excluding dividends
        :shrout, # Shares outstanding (in thousands)
        :mthprc,
        :mthcap,
        :mthprevcap,
        # :mthvol, :mthprcvol # volume and price volume        
        :siccd, # Industry code
        :naics, # Industry code
        Symbol.(intersect(variables, names(df_msf_v2)))
    )

    @p df_msf_v2 |> select!(__, var_select) |> sort!(__, [:permno, :mthcaldt]) |> 
        disallowmissing!(__, [:mthcaldt]) 
    transform!(df_msf_v2, 
        :naics => (x -> replace(x, "0" => missing)) => :naics,
        :mthcaldt => ByRow(MonthlyDate) => :datem)
    # ----------------------------------------------------------------------------------------------


    return df_msf_v2

end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
"""
    build_MSF_v2(df_msf::DataFrame; save, trim_cols, clean_cols, verbose)

Clean up the CRSP Monthly Stock File (see `import_MSF`)

# Arguments
- `df::DataFrame`: A standard dataframe with compustat data (minimum variables are in `import_Funda`)

# Keywords
- `save::String`: Save a gzip version of the data on path `\$save/funda.csv.gz`; Default does not save the data.
- `trim_cols::Bool`: Only keep a subset of relevant columns in the final dataset
- `clean_cols::Bool`: Clean up the columns of the dataframe to be of type Float64; Default is `false` and leaves the Decimal type intact
- `logging_level::Symbol`: How to log results


# Returns
- `df::DataFrame`: DataFrame with crsp MSF file "cleaned"
"""
#= 
REDUNDANT WITH NEW FILES
=#
# --------------------------------------------------------------------------------------------------



# --------------------------------------------------------------------------------------------------
function import_DSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    logging_level::Symbol = :debug, # either none, debug, info etc... tbd 
    )


    # set up the query for msf
    postgre_query_dsf = """
        SELECT PERMNO, DATE, RET, PRC, SHROUT, VOL
            FROM crsp.dsf
            WHERE DATE >= '$(string(date_range[1]))' AND DATE <= '$(string(date_range[2]))'
    """
    df_dsf = execute(wrds_conn, postgre_query_dsf) |> DataFrame
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



# --------------------------------------------------------------------------------------------------
function import_DSF_v2(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    logging_level::Symbol = :debug, # either none, debug, info etc... tbd 
    )


    # could pick either way ... 
    # dsf_columns = get_postgres_columns("crsp", "dsf_v2"; wrds_conn=wrds_conn) |> sort
    # stkmthsecuritydata_columns = get_postgres_columns("crsp", "stkdlysecuritydata"; wrds_conn=wrds_conn) |> sort

# set up the query for msf
    postgre_query_dsf = """
    SELECT PERMNO, DLYCALDT, DLYRET, DLYPRC, DLYVOL, DLYCAP
        FROM crsp.stkdlysecuritydata
        WHERE DLYCALDT >= '$(string(date_range[1]))' AND DLYCALDT <= '$(string(date_range[2]))'
    """
    df_dsf_v2 = execute(wrds_conn, postgre_query_dsf) |> DataFrame

    # clean up the dataframe
    transform!(df_dsf_v2,
        names(df_dsf_v2, check_integer.(eachcol(df_dsf_v2))) .=> (x->convert.(Union{Missing, Int}, x));
        renamecols = false)

    disallowmissing!(df_dsf_v2, :dlycaldt)

    return df_dsf_v2
end

# when there are no connections establisheds
function import_DSF_v2(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Dates.today()),
    variables::AbstractString = "",
    user::AbstractString = "", password::AbstractString = "",
    logging_level::Symbol = :debug, # either none, debug, info etc... tbd 
    )

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end

    return import_DSF_v2(wrds_conn, date_range=date_range, variables=variables, logging_level=logging_level)
end
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# PRIVATE FUNCTIONS 
# TODO REWRITE THESE FUNCTIONS WITHOUT INTERPOLATION FOR SAFETY
# postgres_query = """
#     SELECT table_name
#       FROM information_schema.tables
#      WHERE table_schema = \$1
# """
# postgres_res = execute(wrds_conn, postgres_query, (table_schema,))
function get_postgres_columns(table_schema, table_name; wrds_conn, prior_columns::Vector{String} = [""])
    
    # download potential columns
    postgres_query= """
        SELECT *
          FROM information_schema.columns
         WHERE table_schema = '$table_schema'
           AND table_name   = '$table_name'
             ;
        """

    postgres_res = execute(wrds_conn, postgres_query)
    postgres_columns = DataFrame(columntable(postgres_res)).column_name ;
    if isempty(prior_columns) || prior_columns == [""]
        return uppercase.(postgres_columns)
    else 
        return intersect(uppercase.(postgres_columns), uppercase.(prior_columns))
    end
end 


function get_postgres_table(table_schema, table_name; wrds_conn, prior_columns::Vector{String} = [""])
    
    if isempty(prior_columns) || prior_columns == [""]
        columns = "*"
    else 
        columns = join(uppercase.(prior_columns), ", ")
    end

    postgres_query = """
        SELECT $columns
        FROM $table_schema.$table_name
    """

    postgres_res = execute(wrds_conn, postgres_query)
    return columntable(postgres_res)
end 


