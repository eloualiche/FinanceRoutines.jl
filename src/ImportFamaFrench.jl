# ------------------------------------------------------------------------------------------

# ImportFamaFrench.jl

# Collection of functions that import 
#  financial data into julia
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# List of exported functions
# export greet_FinanceRoutines  # for debugging
# export import_FF3             # read monthly FF3   
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
function greet_FinanceRoutines()
    return "Hello FinanceRoutines!"
end
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
function import_FF3()

    url_FF = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip"
    ff_col_classes = [String7, Float64, Float64, Float64, Float64];
    row_lim = div(MonthlyDate(Dates.today()) - MonthlyDate(1926, 7), Dates.Month(1)) - 1

    http_response = Downloads.download(url_FF);
    z = ZipFile.Reader(http_response) ;
    a_file_in_zip = filter(x -> match(r".*csv", lowercase(x.name)) != nothing, z.files)[1]
    df_FF3 = copy(
        CSV.File(a_file_in_zip, 
                 skipto=5, header=4, limit=row_lim, delim=",", 
                 types=ff_col_classes) |> 
        DataFrame);
    close(z)

    rename!(df_FF3, [:dateym, :mktrf, :smb, :hml, :rf]);
    @subset!(df_FF3, .!(ismissing.(:dateym)));
    @subset!(df_FF3, .!(ismissing.(:mktrf)));
    @rtransform!(df_FF3, :dateym = MonthlyDate(:dateym, "yyyymm"))
    @subset!(df_FF3, :dateym .>= MonthlyDate("1900-01", "yyyy-mm") )    

    return df_FF3
end



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

        rename!(df_FF3, [:date, :mktrf, :smb, :hml, :rf]);
        @subset!(df_FF3, .!(ismissing.(:date)));
        @subset!(df_FF3, .!(ismissing.(:mktrf)));
        @rtransform!(df_FF3, :date = Date(string(:date), "yyyymmdd") )

        return df_FF3
    
    else
        @warn "Frequency $frequency not known. Try :daily or leave blank for :monthly"
    end

end
# ------------------------------------------------------------------------------------------
