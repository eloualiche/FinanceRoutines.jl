
# --------------------------------------------------------------------------------------------------
function check_integer(x::AbstractVector)
    for i in x
        !(typeof(i) <: Union{Missing, Number}) && return false
        ismissing(i) && continue
        isinteger(i) && continue
        return false
    end
    return true
end
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
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
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
function log_with_level(message::String, level::Symbol=:debug; 
                        _module=@__MODULE__, _file=@__FILE__, _line=@__LINE__)
    # Convert symbol level to proper logging level
    log_level = if level == :debug
        Logging.Debug
    elseif level == :info
        Logging.Info
    elseif level == :warn
        Logging.Warn
    elseif level == :error
        Logging.Error
    else
        @warn "logging level not recognized ($level); default to debug"
        # Default to Debug if unknown level is provided
        Logging.Debug
    end
    
    # Log the message at the specified level, preserving caller information
    with_logger(ConsoleLogger(stderr, log_level)) do
        @logmsg log_level message _module=_module _file=_file _line=_line
    end
end

# I am not sure this is working!
macro log_msg(message)
    quote
        # Use the logging_level variable from the current scope
        if @isdefined(logging_level) && logging_level ∈ [:debug, :info, :warn, :error]
            log_with_level($(esc(message)), logging_level; 
                          _module=@__MODULE__, _file=@__FILE__, _line=@__LINE__)
        end
    end
end# --------------------------------------------------------------------------------------------------
