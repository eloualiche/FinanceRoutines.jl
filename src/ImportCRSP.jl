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
    This comes after import_MSF
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













# # ------------------------------------------------------------------------------------------
# """
#     Multiple dispatch to load from clean CSV
# """

#     # What are the classes?
#     msf_col_classes = [Int, String, Int, Int, Int,  # "PERMNO" "date" "NAMEENDT" "SHRCD"  "EXCHCD"
#         String, String, String, String, String,     # "SICCD" "NCUSIP" "TICKER" "COMNAM" "SHRCLS"
#         String, Int, String, String, String,        # "TSYMBOL" "NAICS"  "PRIMEXCH" "TRDSTAT" "SECSTAT"
#         Int, Int, Int, String, String,              # "PERMCO" "ISSUNO" "HEXCD" "HSICCD" "CUSIP"
#         Int, Float64, Int, Int, Int,                # "DCLRDT" "DLAMT" "DLPDT" "DLSTCD" "NEXTDT
#         Int, Int, Int, Int, Int,                    # "PAYDT"  "RCRDDT" "SHRFLG" "HSICMG" "HSICIG"
#         Int, Float64, Float64, Float64, Int,        # "DISTCD" "DIVAMT" "FACPR"  "FACSHR" "ACPERM"
#         Int, Int, Int, String, Float64,             # "ACCOMP" "SHRENDDT" "NWPERM" "DLRETX" "DLPRC"
#         String, Int, Int, Int, Int,                 # "DLRET"    "TRTSCD"   "NMSIND"   "MMCNT"    "NSDINX"
#         Float64, Float64, Float64, Int, String,     # "BIDLO"    "ASKHI"    "PRC"      "VOL"      "RET"
#         Float64, Float64, Int, Float64, Float64,       # "BID"  "ASK" "SHROUT" CFACPR" "CFACSHR"
#         Float64, Float64, Int, String,                 # "ALTPRC"   "SPREAD"   "ALTPRCDT" "RETX"
#         Float64, Float64, Float64, Float64, Float64];  # "vwretd" "vwretx" "ewretd" "ewretx" "sprtrn"

#     # KEEP SOME COLUMNS
#     col_keep = [:PERMNO, :NAICS, :SICCD, :HSICCD, :date, :PRC, :RET, :SHROUT, 
#                 :HEXCD, :SHRCD] ;
#     col_keep = vcat(col_keep, variables);
#     # col_keep = intersect(Symbol.(names(df_crsp)), col_keep);

#     # READ THE FILE
#     df_msf = CSV.File(
#         expanduser(path_to_file);
#         header=1, types = msf_col_classes, 
#         silencewarnings=true, missingstring="NA", delim=',',
#         select = col_keep
#         ) |> DataFrame;

#     # Lower Case Names
#     rename!(df_msf, lowercase.(names(df_msf)));

#     # Filter Stock CLASSES
#     @rsubset!(df_msf, :hexcd ∈ (1, 2, 3) )
#     @rsubset!(df_msf, :shrcd ∈ (10, 11) )

#     # FILTER THE DATE RANGE
#     @rtransform!(df_msf, :datey = tryparse(Int, :date[1:4]));
#     @subset!(df_msf, :datey .>= date_range[1], :datey .<= date_range[2] )

#     # CLEAN UP THE MAIN VARIABLES (return | market cap | date)
#     @rtransform!(df_msf, :ret = passmissing(tryparse)(Float64, :ret) );  # returns
#     @rtransform!(df_msf, :ret = (x -> isnothing(x) ? missing : x)(:ret) );
#     @rtransform!(df_msf, :me = abs(:prc) * :shrout);
#     @rtransform!(df_msf, :date = Date(:datey, tryparse(Int, :date[5:6]), tryparse(Int, :date[7:8])));
#     @rtransform!(df_msf, :datem = MonthlyDate(:date) );

#     # BEFORE LAGS, REMOVE DUPLICATES BY MONTH|PERMNO
#     # Look for unique keys within returns and market equity
#     unique!(df_msf, [:permno, :datem, :ret, :me]);
#     sort!(df_msf, [:permno, :date]);

#     # LAG ME for one month
#     @transform!(groupby(df_msf, :permno), 
#         :l1m_me = lag(:me, 1), :l1m_datem = lag(:datem, 1) );
#     # missing if lag is not one month prior
#     @rtransform!(df_msf, 
#         @passmissing :l1m_me = (:l1m_datem + Dates.Month(1) .== :datem) ? :l1m_me : Base.missing);

#     # CLEAN UP THE COLUMNS 
#     select!(df_msf, Not(intersect([:shrcd, :hexcd, :l1m_datem], Symbol.(names(df_msf)))))
#     select!(df_msf, vcat([:permno, :date, :ret, :l1m_me], Symbol.(names(df_msf))) |> unique)

#     return df_msf

# end
# # ------------------------------------------------------------------------------------------


# # ------------------------------------------------------------------------------------------
# # Utilities (non exported)
# function check_integer(x::AbstractVector)
#     for i in x
#         !(typeof(i) <: Union{Missing, Number}) && return false
#         ismissing(i) && continue
#         isinteger(i) && continue
#         return false
#     end
#     return true
# end
