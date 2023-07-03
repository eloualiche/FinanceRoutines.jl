# ------------------------------------------------------------------------------------------
# ImportComp.jl

# Collection of functions that import 
#  compustat data into julia
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# List of exported functions
# export import_MSF 
# export build_MSF

# list
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
function import_Funda(wrds_conn::Connection;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String = ""
    )

    var_funda = ["GVKEY", "DATADATE", "SICH", "FYR", "FYEAR", 
               "AT", "LT", "SALE", "EBITDA", "CAPX", "NI", "DV", "CEQ", "CEQL", "SEQ",
               "TXDITC", "TXP", "TXDB", "ITCB", "DVT", "PSTK","PSTKL", "PSTKRV"]

# set up the query for msf
    postgre_query_funda_full = """
        SELECT *
            FROM comp.funda
            WHERE INDFMT = 'INDL' AND DATAFMT = 'STD' AND CONSOL = 'C' AND POPSRC = 'D'
                AND DATADATE >= '$(string(date_range[1]))' 
                AND DATADATE <= '$(string(date_range[2]))'
    """
    postgre_query_funda_var = """
        SELECT $(join(var_funda, ","))
            FROM comp.funda
            WHERE INDFMT = 'INDL' AND DATAFMT = 'STD' AND CONSOL = 'C' AND POPSRC = 'D'
                AND DATADATE >= '$(string(date_range[1]))' 
                AND DATADATE <= '$(string(date_range[2]))'
    """
    @time res_q_funda = execute(wrds_conn, postgre_query_funda_var)
    df_funda = DataFrame(columntable(res_q_funda));

    # clean up the dataframe
    transform!(df_funda, 
        names(df_funda, check_integer.(eachcol(df_funda))) .=> (x->convert.(Union{Missing, Int}, x)); 
        renamecols = false)
    df_funda[!, :gvkey] .= parse.(Int, df_funda[!, :gvkey]);

    return df_funda

end

function import_Funda(;
    date_range::Tuple{Date, Date} = (Date("1900-01-01"), Date("2030-01-01")),
    variables::String = "",
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
function build_Funda(df_funda::DataFrame;
    save::String = ""
    )

    # define book equity value
    @transform!(df_funda, :be = 
        coalesce(:seq, :ceq + :pstk, :at - :lt) + coalesce(:txditc, :txdb + :itcb, 0) -
        coalesce(:pstkrv, :pstkl, :pstk, 0) )
    df_funda[ isless.(df_funda.be, 0), :be] .= missing;
    @rtransform!(df_funda, :date_y = year(:datadate));
    sort!(df_funda, [:gvkey, :date_y, :datadate]) 
    unique!(df_funda, [:gvkey, :date_y], keep=:last) # last obs

    if !(save == "")
        CSV.write(save * "/funda.csv.gz", df_funda, compress=true)
    end

    return df_funda
end
# ------------------------------------------------------------------------------------------

