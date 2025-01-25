
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
    Open a Postgres connection on WRDS server
"""
function open_wrds_pg(user::AbstractString, password::AbstractString)
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
    print("Enter WRDS username: ")
    user = readline()
    password_buffer = Base.getpass("Enter WRDS password")
    # con = open_wrds_pg(user, String(password_buffer.data[1:password_buffer.size]));
    con = open_wrds_pg(user, read(password_buffer, String))
    Base.shred!(password_buffer)
    return con
end
# ------------------------------------------------------------------------------------------
