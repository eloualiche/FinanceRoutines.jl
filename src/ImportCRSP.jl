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
        date_range=(Date("1900-01-01"), Date("2030-01-01"), 
        variables::String = "", user="", password="")

Import the CRSP Monthly Stock File (MSF) from CRSP on WRDS PostGre server

# Arguments
- `wrds_conn::Connection`: An existing PostGreSQL connection to WRDS; creates one if empty

# Keywords
- `date_range::Tuple{Date, Date}`: A tuple of dates to select data (limits the download size)
- `variables::Vector{String}`: A vector of String of additional variable to include in the download
- `user::String`: username to log into the WRDS cli; default to ask user for authentication
- `password::String`: password to log into the WRDS cli

# Returns
- `df_msf_final::DataFrame`: DataFrame with msf crsp file
"""
function import_MSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String = ""
    )

# set up the query for msf
    postgre_query_msf = """
        SELECT PERMNO,PERMCO,DATE,PRC,ALTPRC,RET,RETX,SHROUT
            FROM crsp.msf
            WHERE DATE >= '$(string(date_range[1]))' AND DATE <= '$(string(date_range[2]))'
    """
    @time res_q_msf = execute(wrds_conn, postgre_query_msf)
    df_msf = DataFrame(columntable(res_q_msf))
    transform!(df_msf,     # clean up the dataframe
        names(df_msf, check_integer.(eachcol(df_msf))) .=> (x->convert.(Union{Missing, Int}, x)); 
        renamecols = false);

# set up the query for mse
    # postgre_query = """
    #     SELECT DATE, PERMNO, SHRCD, EXCHCD, HEXCD
    #         FROM crsp.mse
    #         WHERE EXTRACT('Year' FROM DATE) = 2013
    # """
    # res = LibPQ.execute(wrds_conn, postgre_query)
    # df_mse = DataFrame(columntable(res))
    # # convert to Int these flag variables
    # transform!(df_mse, 
    #     names(df_mse, Union{Missing, Float64}) .=> (x->convert.(Union{Missing, Int}, x)); 
    #     renamecols = false)
    # # @rsubset(df_mse, !ismissing(:shrcd) )
    # df_mse
    # @rsubset(df_mse, :hexcd ∈ (1, 2, 3) )
    # @rsubset(df_mse, :shrcd ∈ (10, 11) )
    # df_mse.permno |> unique

# set up the query for msenames
    postgre_query_msenames = """
        SELECT PERMNO, NAMEDT, NAMEENDT, SHRCD, EXCHCD, HEXCD, NAICS, HSICCD, CUSIP
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
    select!(df_msf_final, 
        :permno, # Security identifier
        :date, # Date of the observation
        :datem,
        :ret, # Return
        :retx, # Return excluding dividends
        :shrout, # Shares outstanding (in thousands)
        :altprc, # Last traded price in a month
        :exchcd, # Exchange code
        :hsiccd, # Industry code
        :naics, # Industry code
        :dlret, # Delisting return
        :dlstcd # Delisting code
    )
    sort!(df_msf_final, [:permno, :date]);
    # unique(df_msf_final, [:permno, :date])

    return df_msf_final

end

# when there are no connections establisheds
function import_MSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String = "",
    user::String = "", password::String = "")

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
    build_MSF(df_msf::DataFrame; save)

Clean up the compustat funda file download from crsp (see `import_Funda`)

# Arguments
- `df_funda::DataFrame`: A standard dataframe with compustat data (minimum variables are in `import_Funda`)

# Keywords
- `save::String`: Save a gzip version of the data on path `\$save/funda.csv.gz`; Default does not save the data.

# Returns
- `df_msf::DataFrame`: DataFrame with compustat funda file "cleaned"
"""
function build_MSF(df_msf::DataFrame;
    save::String = ""
    )

# Create marketcap:
    @rtransform!(df_msf, :mktcap = abs(:shrout * :altprc)) # in 1000s
    df_msf[ isequal.(df_msf.mktcap, 0), :mktcap] .= missing;

# Lagged marketcap
    sort!(df_msf, [:permno, :datem])
    # method 1: lag and then merge back
    # df_msf_mktcap_lag = @select(df_msf,
    #         :datem = :datem + Month(1), :permno, :l1m_mktcap2 = :mktcap)
    # df_msf = leftjoin(df_msf, df_msf_mktcap_lag, on = [:permno, :datem])
    panellag!(df_msf, :permno, :datem, 
        :mktcap, :l1m_mktcap, Month(1))

# Adjusted returns (see tidy finance following Bali, Engle, and Murray)
    @rtransform! df_msf :ret_adj = 
        ismissing(:dlstcd) ? :ret : 
            !ismissing(:dlret) ? :dlret :
                (:dlstcd ∈ (500, 520, 580, 584)) || ((:dlstcd >= 551) & (:dlstcd <= 574)) ? -0.3 :
                    :dlstcd == 100 ? :ret : -1.0

# select variables and save
    select!(df_msf, :permno, :date, :ret, :mktcap, :l1m_mktcap, :retx, 
        :naics, :hsiccd)
    if !(save == "")
        CSV.write(save * "/msf.csv.gz", df_msf, compress=true)
    end

    return df_msf
end


function build_MSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    save::Bool = false,
    )

    df_msf = import_MSF(wrds_conn; date_range=date_range);
    df_msf = build_msf(df_msf, save = save)
    return df_msf
end


function build_MSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    save::Bool = false,
    )

    df_msf = import_MSF(;date_range);
    df_msf = build_msf(df_msf, save = save)

    return df_msf
end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
function import_DSF(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String
    )

# set up the query for msf
    postgre_query_dsf = """
        SELECT PERMNO, DATE, RET, PRC, SHROUT, VOL
            FROM crsp.dsf
            WHERE DATE >= '$(string(date_range[1]))' AND DATE <= '$(string(date_range[2]))'
    """
    @time res_q_dsf = execute(wrds_conn, postgre_query_dsf)
    @time df_dsf = DataFrame(columntable(res_q_dsf))
    # clean up the dataframe
    transform!(df_dsf, 
        names(df_dsf, check_integer.(eachcol(df_dsf))) .=> (x->convert.(Union{Missing, Int}, x)); 
        renamecols = false)

    return df_dsf
end

# when there are no connections establisheds
function import_DSF(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String = "",
    user::String = "", password::String = "")

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end

    return import_DSF(wrds_conn, date_range=date_range, variables=variables)
end
# ------------------------------------------------------------------------------------------