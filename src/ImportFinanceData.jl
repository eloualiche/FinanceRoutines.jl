# ---------------------------------------------------------
# ImportFinanceData.jl

# Collection of functions that import 
#  financial data into julia
# ---------------------------------------------------------


# ---------------------------------------------------------
# List of exported functions
# export greet_FinanceRoutines  # for debugging
# export import_FF3             # read monthly FF3   
# ---------------------------------------------------------


# ---------------------------------------------------------
function greet_FinanceRoutines()
    println("Hello FinanceRoutines!")
    return "Hello FinanceRoutines!"
end
# ---------------------------------------------------------


# ---------------------------------------------------------
function import_FF3()

    url_FF = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip"

    http_response = Downloads.download(url_FF);
    z = ZipFile.Reader(http_response) ;
    a_file_in_zip = filter(x -> match(r".*csv", lowercase(x.name)) != nothing, z.files)[1]
    df_FF3 = copy(CSV.File(a_file_in_zip, header=3, footerskip=1) |> DataFrame);
    close(z)

    rename!(df_FF3, [:dateym, :mktrf, :smb, :hml, :rf]);
    @subset!(df_FF3, .!(ismissing.(:dateym)));
    @subset!(df_FF3, .!(ismissing.(:mktrf)));
    @transform!(df_FF3, :dateym = parse.(Int, :dateym) )
    @subset!(df_FF3, :dateym .>= 190000 )    
    @transform!(df_FF3, 
        :date  = Date.(div.(:dateym, 100), rem.(:dateym,100) ),
        :mktrf = parse.(Float64, :mktrf),
        :smb   = parse.(Float64, :smb),
        :hml   = parse.(Float64, :hml),
        :rf    = parse.(Float64, :rf) )

    return(df_FF3)
end
# ---------------------------------------------------------


# ---------------------------------------------------------
"""
    import_FF3(frequency::Symbol)

Download and import the Fama-French 3 Factors from Ken French website. 

If `frequency` is unspecified, import the monthly research returns.
If `frequency` is :daily, import the daily research returns. 

"""
function import_FF3(frequency::Symbol)

    if frequency==:monthly
        return import_FF3()

    elseif frequency==:daily
      url_FF = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
    
        http_response = Downloads.download(url_FF);

        z = ZipFile.Reader(http_response) ;
        a_file_in_zip = filter(x -> match(r".*csv", lowercase(x.name)) != nothing, z.files)[1]
        df_FF3 = copy(CSV.File(a_file_in_zip, header=4, footerskip=1) |> DataFrame);
        close(z)

        rename!(df_FF3, [:dateymd, :mktrf, :smb, :hml, :rf]);
        @subset!(df_FF3, .!(ismissing.(:dateymd)));
        @subset!(df_FF3, .!(ismissing.(:mktrf)));
        @transform!(df_FF3, 
            :date  = Date.(div.(:dateymd, 10000), 
                           rem.(div.(:dateymd, 100), 100), rem.(:dateymd,100) ) 
            )

        return(df_FF3)
    
    else
        @warn "Frequency $frequency not known. Try :daily or leave blank for :monthly"
    end

end
# ---------------------------------------------------------

