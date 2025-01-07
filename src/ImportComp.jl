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
    variables::Union{Nothing, Vector{String}} = nothing,
    filter_variables = Dict(:CURCD=>"USD")  # if you want something fanciers ... export variable and do it later
    )

    var_funda = ["GVKEY", "DATADATE", "SICH", "FYR", "FYEAR",
                 "AT", "LT", "SALE", "EBITDA", "CAPX", "NI", "DV", "CEQ", "CEQL", "SEQ",
                 "TXDITC", "TXP", "TXDB", "ITCB", "DVT", "PSTK","PSTKL", "PSTKRV"]
    !isnothing(variables) && append!(var_funda, uppercase.(variables))
    !isnothing(filter_variables) && append!(var_funda, uppercase.(string.(keys(filter_variables))))

# TODO WE SHOULD PROBABLY KEEP SOMEWHERE AS DATA THE LIST OF VALID COLUMNS
# THEN THROW A WARNING IF IT DOESNT FIT
    var_check = setdiff(var_funda, compd_funda)
    size(var_check, 1)>0 && (@warn "Queried variables not in dataset ... : $(join(var_check, ","))")
    filter!(in(compd_funda), var_funda)

# set up the query for funda
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
    variables::Union{Nothing, Vector{String}} = nothing,
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
    @rtransform!(df, :datey = year(:datadate));
    sort!(df, [:gvkey, :datey, :datadate])
    unique!(df, [:gvkey, :datey], keep=:last) # last obs

    verbose && (@info ". Cleaning superfluous columns INDFMT, etc.")
    select!(df, Not(intersect(names(df), ["indfmt","datafmt","consol","popsrc", "curcd"])) )

    if !(save == "")
        verbose && (@info ". Saving to $save/funda.csv.gz")
        CSV.write(save * "/funda.csv.gz", df, compress=true)
    end

    return df
end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
const compd_funda = [
    "GVKEY", "DATADATE", "FYEAR", "INDFMT", "CONSOL", "POPSRC", "DATAFMT", "TIC", "CUSIP", "CONM", "ACCTCHG", "ACCTSTD", "ACQMETH", "ADRR", "AJEX",
    "AJP", "BSPR", "COMPST", "CURCD", "CURNCD", "CURRTR", "CURUSCN", "FINAL", "FYR", "ISMOD", "LTCM", "OGM", "PDDUR", "SCF", "SRC", "STALT", "UDPL", "UPD",
    "APDEDATE", "FDATE", "PDATE", "ACCHG", "ACCO", "ACCRT", "ACDO", "ACO", "ACODO", "ACOMINC", "ACOX", "ACOXAR", "ACQAO", "ACQCSHI", "ACQGDWL", "ACQIC",
    "ACQINTAN", "ACQINVT", "ACQLNTAL", "ACQNIINTC", "ACQPPE", "ACQSC", "ACT", "ADPAC", "AEDI", "AFUDCC", "AFUDCI", "ALDO", "AM", "AMC", "AMDC", "AMGW", "ANO",
    "AO", "AOCIDERGL", "AOCIOTHER", "AOCIPEN", "AOCISECGL", "AODO", "AOL2", "AOLOCH", "AOX", "AP", "APALCH", "APB", "APC", "APOFS", "AQA", "AQC", "AQD", "AQEPS",
    "AQI", "AQP", "AQPL1", "AQS", "ARB", "ARC", "ARCE", "ARCED", "ARCEEPS", "ARTFS", "AT", "AUL3", "AUTXR", "BALR", "BANLR", "BAST", "BASTR", "BATR", "BCEF", "BCLR",
    "BCLTBL", "BCNLR", "BCRBL", "BCT", "BCTBL", "BCTR", "BILLEXCE", "BKVLPS", "BLTBL", "CA", "CAPR1", "CAPR2", "CAPR3", "CAPS", "CAPSFT", "CAPX", "CAPXV", "CB", "CBI",
    "CDPAC", "CDVC", "CEIEXBILL", "CEQ", "CEQL", "CEQT", "CFBD", "CFERE", "CFO", "CFPDO", "CGA", "CGRI", "CGTI", "CGUI", "CH", "CHE", "CHECH", "CHS", "CI", "CIBEGNI",
    "CICURR", "CIDERGL", "CIMII", "CIOTHER", "CIPEN", "CISECGL", "CITOTAL", "CLD2", "CLD3", "CLD4", "CLD5", "CLFC", "CLFX", "CLG", "CLIS", "CLL", "CLLC", "CLO", "CLRLL",
    "CLT", "CMP", "CNLTBL", "COGS", "CPCBL", "CPDOI", "CPNLI", "CPPBL", "CPREI", "CRV", "CRVNLI", "CSHFD", "CSHI", "CSHO", "CSHPRI", "CSHR", "CSHRC", "CSHRP", "CSHRSO",
    "CSHRT", "CSHRW", "CSTK", "CSTKCV", "CSTKE", "DBI", "DC", "DCLO", "DCOM", "DCPSTK", "DCS", "DCVSR", "DCVSUB", "DCVT", "DD", "DD1", "DD2", "DD3", "DD4", "DD5", "DEPC",
    "DERAC", "DERALT", "DERHEDGL", "DERLC", "DERLLT", "DFPAC", "DFS", "DFXA", "DILADJ", "DILAVX", "DLC", "DLCCH", "DLTIS", "DLTO", "DLTP", "DLTR", "DLTSUB", "DLTT", "DM",
    "DN", "DO", "DONR", "DP", "DPACB", "DPACC", "DPACLI", "DPACLS", "DPACME", "DPACNR", "DPACO", "DPACRE", "DPACT", "DPC", "DPDC", "DPLTB", "DPRET", "DPSC", "DPSTB", "DPTB",
    "DPTC", "DPTIC", "DPVIEB", "DPVIO", "DPVIR", "DRC", "DRCI", "DRLT", "DS", "DT", "DTEA", "DTED", "DTEEPS", "DTEP", "DUDD", "DV", "DVC", "DVDNP", "DVINTF", "DVP", "DVPA",
    "DVPD", "DVPDP", "DVPIBB", "DVRPIV", "DVRRE", "DVSCO", "DVT", "DXD2", "DXD3", "DXD4", "DXD5", "EA", "EBIT", "EBITDA", "EIEA", "EMOL", "EMP", "EPSFI", "EPSFX", "EPSPI",
    "EPSPX", "ESOPCT", "ESOPDLT", "ESOPNR", "ESOPR", "ESOPT", "ESUB", "ESUBC", "EXCADJ", "EXRE", "FATB", "FATC", "FATD", "FATE", "FATL", "FATN", "FATO", "FATP", "FCA", "FDFR",
    "FEA", "FEL", "FFO", "FFS", "FIAO", "FINACO", "FINAO", "FINCF", "FINCH", "FINDLC", "FINDLT", "FINIVST", "FINLCO", "FINLTO", "FINNP", "FINRECC", "FINRECLT", "FINREV", "FINXINT",
    "FINXOPR", "FOPO", "FOPOX", "FOPT", "FSRCO", "FSRCT", "FUSEO", "FUSET", "GBBL", "GDWL", "GDWLAM", "GDWLIA", "GDWLID", "GDWLIEPS", "GDWLIP", "GEQRV", "GLA", "GLCEA", "GLCED",
    "GLCEEPS", "GLCEP", "GLD", "GLEPS", "GLIV", "GLP", "GOVGR", "GOVTOWN", "GP", "GPHBL", "GPLBL", "GPOBL", "GPRBL", "GPTBL", "GWO", "HEDGEGL", "IAEQ", "IAEQCI", "IAEQMI", "IAFICI",
    "IAFXI", "IAFXMI", "IALI", "IALOI", "IALTI", "IAMLI", "IAOI", "IAPLI", "IAREI", "IASCI", "IASMI", "IASSI", "IASTI", "IATCI", "IATI", "IATMI", "IAUI", "IB", "IBADJ", "IBBL",
    "IBC", "IBCOM", "IBKI", "IBMII", "ICAPT", "IDIIS", "IDILB", "IDILC", "IDIS", "IDIST", "IDIT", "IDITS", "IIRE", "INITB", "INTAN", "INTANO", "INTC", "INTPN", "INVCH", "INVFG",
    "INVO", "INVOFS", "INVREH", "INVREI", "INVRES", "INVRM", "INVT", "INVWIP", "IOBD", "IOI", "IORE", "IP", "IPABL", "IPC", "IPHBL", "IPLBL", "IPOBL", "IPTBL", "IPTI", "IPV",
    "IREI", "IRENT", "IRII", "IRLI", "IRNLI", "IRSI", "ISEQ", "ISEQC", "ISEQM", "ISFI", "ISFXC", "ISFXM", "ISGR", "ISGT", "ISGU", "ISLG", "ISLGC", "ISLGM", "ISLT", "ISNG",
    "ISNGC", "ISNGM", "ISOTC", "ISOTH", "ISOTM", "ISSC", "ISSM", "ISSU", "IST", "ISTC", "ISTM", "ISUT", "ITCB", "ITCC", "ITCI", "IVACO", "IVAEQ", "IVAO", "IVCH", "IVGOD",
    "IVI", "IVNCF", "IVPT", "IVST", "IVSTCH", "LCABG", "LCACL", "LCACR", "LCAG", "LCAL", "LCALT", "LCAM", "LCAO", "LCAST", "LCAT", "LCO", "LCOX", "LCOXAR", "LCOXDR", "LCT",
    "LCUACU", "LI", "LIF", "LIFR", "LIFRP", "LLOML", "LLOO", "LLOT", "LLRCI", "LLRCR", "LLWOCI", "LLWOCR", "LNO", "LO", "LOL2", "LOXDR", "LQPL1", "LRV", "LS", "LSE", "LST",
    "LT", "LUL3", "MIB", "MIBN", "MIBT", "MII", "MRC1", "MRC2", "MRC3", "MRC4", "MRC5", "MRCT", "MRCTA", "MSA", "MSVRV", "MTL", "NAICS", "NAT", "NCO", "NFSR", "NI", "NIADJ", "NIECI",
    "NIINT", "NIINTPFC", "NIINTPFP", "NIIT", "NIM", "NIO", "NIPFC", "NIPFP", "NIT", "NITS", "NOPI", "NOPIO", "NP", "NPANL", "NPAORE", "NPARL", "NPAT", "NRTXT", "NRTXTD",
    "NRTXTEPS", "OANCF", "OB", "OIADP", "OIBDP", "OPEPS", "OPILI", "OPINCAR", "OPINI", "OPIOI", "OPIRI", "OPITI", "OPREPSX", "OPTCA", "OPTDR", "OPTEX", "OPTEXD", "OPTFVGR",
    "OPTGR", "OPTLIFE", "OPTOSBY", "OPTOSEY", "OPTPRCBY", "OPTPRCCA", "OPTPRCEX", "OPTPRCEY", "OPTPRCGR", "OPTPRCWA", "OPTRFR", "OPTVOL", "PALR", "PANLR", "PATR", "PCL",
    "PCLR", "PCNLR", "PCTR", "PDVC", "PI", "PIDOM", "PIFO", "PLL", "PLTBL", "PNCA", "PNCAD", "PNCAEPS", "PNCIA", "PNCID", "PNCIEPS", "PNCIP", "PNCWIA", "PNCWID", "PNCWIEPS",
    "PNCWIP", "PNLBL", "PNLI", "PNRSHO", "POBL", "PPCBL", "PPEGT", "PPENB", "PPENC", "PPENLI", "PPENLS", "PPENME", "PPENNR", "PPENO", "PPENT", "PPEVBB", "PPEVEB", "PPEVO",
    "PPEVR", "PPPABL", "PPPHBL", "PPPOBL", "PPPTBL", "PRC", "PRCA", "PRCAD", "PRCAEPS", "PREBL", "PRI", "PRODV", "PRSHO", "PRSTKC", "PRSTKCC", "PRSTKPC", "PRVT", "PSTK",
    "PSTKC", "PSTKL", "PSTKN", "PSTKR", "PSTKRV", "PTBL", "PTRAN", "PVCL", "PVO", "PVON", "PVPL", "PVT", "PWOI", "RADP", "RAGR", "RARI", "RATI", "RCA", "RCD", "RCEPS",
    "RCL", "RCP", "RDIP", "RDIPA", "RDIPD", "RDIPEPS", "RDP", "RE", "REA", "REAJO", "RECCH", "RECCO", "RECD", "RECT", "RECTA", "RECTR", "RECUB", "RET", "REUNA", "REUNR",
    "REVT", "RIS", "RLL", "RLO", "RLP", "RLRI", "RLT", "RMUM", "RPAG", "RRA", "RRD", "RREPS", "RRP", "RSTCHE", "RSTCHELT", "RVBCI", "RVBPI", "RVBTI", "RVDO", "RVDT",
    "RVEQT", "RVLRV", "RVNO", "RVNT", "RVRI", "RVSI", "RVTI", "RVTXR", "RVUPI", "RVUTX", "SAA", "SAL", "SALE", "SALEPFC", "SALEPFP", "SBDC", "SC", "SCO", "SCSTKC",
    "SECU", "SEQ", "SEQO", "SETA", "SETD", "SETEPS", "SETP", "SIV", "SPCE", "SPCED", "SPCEEPS", "SPI", "SPID", "SPIEPS", "SPIOA", "SPIOP", "SPPE", "SPPIV", "SPSTKC",
    "SRET", "SRT", "SSNP", "SSTK", "STBO", "STIO", "STKCO", "STKCPA", "TDC", "TDSCD", "TDSCE", "TDSG", "TDSLG", "TDSMM", "TDSNG", "TDSO", "TDSS", "TDST", "TEQ", "TF",
    "TFVA", "TFVCE", "TFVL", "TIE", "TII", "TLCF", "TRANSA", "TSA", "TSAFC", "TSO", "TSTK", "TSTKC", "TSTKME", "TSTKN", "TSTKP", "TXACH", "TXBCO", "TXBCOF", "TXC",
    "TXDB", "TXDBA", "TXDBCA", "TXDBCL", "TXDC", "TXDFED", "TXDFO", "TXDI", "TXDITC", "TXDS", "TXEQA", "TXEQII", "TXFED", "TXFO", "TXNDB", "TXNDBA", "TXNDBL", "TXNDBR",
    "TXO", "TXP", "TXPD", "TXR", "TXS", "TXT", "TXTUBADJUST", "TXTUBBEGIN", "TXTUBEND", "TXTUBMAX", "TXTUBMIN", "TXTUBPOSDEC", "TXTUBPOSINC", "TXTUBPOSPDEC", "TXTUBPOSPINC",
    "TXTUBSETTLE", "TXTUBSOFLIMIT", "TXTUBTXTR", "TXTUBXINTBS", "TXTUBXINTIS", "TXVA", "TXW", "UAOLOCH", "UAOX", "UAPT", "UCAPS", "UCCONS", "UCEQ", "UCUSTAD", "UDCOPRES",
    "UDD", "UDFCC", "UDMB", "UDOLT", "UDPCO", "UDPFA", "UDVP", "UFRETSD", "UGI", "UI", "UINVT", "ULCM", "ULCO", "UNIAMI", "UNL", "UNNP", "UNNPL", "UNOPINC", "UNWCC",
    "UOIS", "UOPI", "UOPRES", "UPDVP", "UPMCSTK", "UPMPF", "UPMPFS", "UPMSUBP", "UPSTK", "UPSTKC", "UPSTKSF", "URECT", "URECTR", "UREVUB", "USPI", "USTDNC", "USUBDVP",
    "USUBPSTK", "UTFDOC", "UTFOSC", "UTME", "UTXFED", "UWKCAPC", "UXINST", "UXINTD", "VPAC", "VPO", "WCAP", "WCAPC", "WCAPCH", "WDA", "WDD", "WDEPS", "WDP", "XACC",
    "XAD", "XAGO", "XAGT", "XCOM", "XCOMI", "XDEPL", "XDP", "XDVRE", "XEQO", "XI", "XIDO", "XIDOC", "XINDB", "XINDC", "XINS", "XINST", "XINT", "XINTD", "XINTOPT",
    "XIVI", "XIVRE", "XLR", "XNBI", "XNF", "XNINS", "XNITB", "XOBD", "XOI", "XOPR", "XOPRAR", "XOPTD", "XOPTEPS", "XORE", "XPP", "XPR", "XRD", "XRDP", "XRENT", "XS",
    "XSGA", "XSTF", "XSTFO", "XSTFWS", "XT", "XUW", "XUWLI", "XUWNLI", "XUWOI", "XUWREI", "XUWTI", "IID", "EXCHG", "CIK", "COSTAT", "FIC", "NAICSH", "SICH", "CSHTR_C",
    "DVPSP_C", "DVPSX_C", "PRCC_C", "PRCH_C", "PRCL_C", "ADJEX_C", "CSHTR_F", "DVPSP_F", "DVPSX_F", "MKVALT", "PRCC_F", "PRCH_F", "PRCL_F", "ADJEX_F", "RANK", "AU",
    "AUOP", "AUOPIC", "CEOSO", "CFOSO"]
