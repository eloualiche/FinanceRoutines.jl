
# ------------------------------------------------------------------------------------------
function check_integer(x::AbstractVector)
    for i in x
        !(typeof(i) <: Union{Missing, Number}) && return false
        ismissing(i) && continue
        isinteger(i) && continue
        return false
    end
    return true
end
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
"""
    Open a postgresql connection on WRDS server
"""
function open_wrds_pg(user::String, password::String)
    wrds_conn = Connection(
        """
            host = wrds-pgdata.wharton.upenn.edu 
            port = 9737
            user='$user' 
            password='$password'
            sslmode = 'require' dbname = wrds
        """
    )
    return wrds_conn
end    

function open_wrds_pg()
    # prompt to input
    print("Enter WRDS username: ...  ") 
    # Calling rdeadline() function
    user = readline()
    print("Enter WRDS password: ...  ")   
    password = readline()
    return open_wrds_pg(user, password);
end    
# ------------------------------------------------------------------------------------------
