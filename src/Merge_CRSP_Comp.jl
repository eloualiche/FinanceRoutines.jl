#! /usr/bin/env julia
# ------------------------------------------------------------------------------------------
# Merge_CRSP_Comp.jl

# Collection of functions that get the link files from crsp/compustat
# ------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
# List of exported functions
# export link_Funda
# export link_MSF
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
function import_ccm_link(wrds_conn::Connection)

# df_funda = CSV.read("./tmp/funda.csv.gz", DataFrame);
# df_msf = CSV.read("./tmp/msf.csv.gz", DataFrame);

# Download link table
    postgre_query_linktable = """
        SELECT *
            FROM crsp.ccmxpf_lnkhist
    """
    res_q_linktable = execute(wrds_conn, postgre_query_linktable)

    df_linktable = DataFrame(columntable(res_q_linktable))
    transform!(df_linktable, names(df_linktable, check_integer.(eachcol(df_linktable))) .=>
        (x->convert.(Union{Missing, Int}, x));
        renamecols = false);
    transform!(df_linktable, :gvkey => ByRow(x->parse(Int, x)) => :gvkey);
    transform!(df_linktable, [:linkprim, :liid, :linktype] .=> ByRow(String3), renamecols=false)

# Prepare the table
    @p df_linktable |> filter!(_.linktype ∈ ("LU", "LC", "LS") && _.linkprim ∈ ("P", "C") )
    # @rsubset(df_linktable, !ismissing(:lpermno))
    df_linktable[ ismissing.(df_linktable.linkenddt), :linkenddt ] .= Dates.today();
    disallowmissing!(df_linktable, [:linkdt, :linkenddt, :lpermno]);
    rename!(df_linktable, :lpermno => :permno);

    return df_linktable
end


# when there are no connections establisheds
function import_ccm_link(;
    user::String = "", password::String = "")

    if user == ""
        wrds_conn = open_wrds_pg()
    else
        wrds_conn = open_wrds_pg(user, password)
    end

   return  import_ccm_link(wrds_conn)
end
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
function link_Funda(df_linktable::DataFrame, df_funda::DataFrame)

    funda_link_permno = innerjoin(
        (select(df_funda, :gvkey, :datadate), df_linktable),
        by_key(:gvkey) & by_pred(:datadate, ∈, x->x.linkdt..x.linkenddt) )
    select!(funda_link_permno,
        Not([:gvkey_1, :linkprim, :liid, :linktype, :linkdt, :linkenddt]) )

    return funda_link_permno

end


function link_MSF(df_linktable::DataFrame, df_msf::DataFrame)
# Merge with CRSP
    df_msf_linked = innerjoin(
        (df_msf, df_linktable),
        by_key(:permno) & by_pred(:date, ∈, x->x.linkdt..x.linkenddt)
    )
    @p df_msf_linked |> filter!(.!ismissing.(_.gvkey))
    select!(df_msf_linked, :date, :permno, :gvkey)
# merge this back
    df_msf_merged = leftjoin(df_msf, df_msf_linked, on = [:date, :permno], source="_merge")
    transform!(df_msf_merged, :date => ByRow(year) => :datey)
    select!(df_msf_merged, Not(:_merge))

    return df_msf_merged
end




# function link_ccm(df_linktable, df_msf, df_funda)

# # ccm
#     df_ccm = leftjoin(
#         df_msf_merged, df_funda,
#         on = [:gvkey, :datey], matchmissing = :notequal)

#     if save
#         CSV.write("./tmp/ccm.csv.gz", df_ccm, compress=true)
#     end

# end
# ------------------------------------------------------------------------------------------
