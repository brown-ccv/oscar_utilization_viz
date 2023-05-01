# src/daily_active_users.jl
using Dates
using CSV
using DataFrames
using DataFramesMeta

include("utils.jl")

raw_jobs_df = CSV.read("data/oscar_jobs_slurm_2014_2017_2020.csv", DataFrame)








jobs_df = @rsubset raw_jobs_df begin 
    :mem_req < 2_000_000
    :tres_alloc != ""
    :sec_runtime < 8640000 # 100 days
end  


jobs_df[!, :memory_gb]    .= jobs_df[:, :mem_req] ./ 1000
jobs_df[!, :start_time]   .= DateTime.(jobs_df[:, :start_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :submit_time]  .= DateTime.(jobs_df[:, :submit_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :end_time]     .= DateTime.(jobs_df[:, :end_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :hour_runtime] .= jobs_df[:, :min_runtime] ./ 60
jobs_df[!, :gpu_per_node] .= get_gpu_per_node(jobs_df[:, :gres_req])
jobs_df[!, :gpus]         .= jobs_df[!, :gpu_per_node] .* jobs_df[!, :nodes_alloc]




first_day = Date(minimum(jobs_df[:, :start_time]))
last_day = Date(maximum(jobs_df[:, :start_time]))

date_range_vec = collect(first_day:Day(1):last_day)







# This function takes two arguments, `start_col` and `end_col`, which are 
# both dataframe columns with timestamps denoting the start time and the end 
# time of a job. The function returns a vector with the same length as the input 
# columns; and each element of the returned vector is a StepRange type, denoting
# the range of the jobs' runtimes.
function compute_daterange(start_col, end_col) 
    n = length(start_col)
    res = Array{StepRange{DateTime, Day}}(undef, n)
    for i in 1:n 
        if start_col[i] > end_col[i] 
            # this path only gets hit if the end time was missing or broken
            continue
        else 
            res[i] = start_col[i]:Day(1):end_col[i]
        end 
    end 
    res 
end 



function get_active_users(job_daterange, job_user)
    n = length(job_daterange)
    days_dict = Dict{Date, Set{String}}()
    for i in 1:n 
        if job_daterange[i].step == Day(0)
            # this path only gets hit if the end time was missing or broken
            continue 
        else 
            job_days = Date.(collect(job_daterange[i]))
            for xday in job_days 
                if xday in keys(days_dict)
                    push!(days_dict[xday], job_user[i])
                else 
                    days_dict[xday] = Set([job_user[i]])
                end
            end 
        end 
    end 
    n_days = length(days_dict)

    days_df = DataFrame(day = Array{Date,1}(undef, n_days),
                        n_users = zeros(Int, n_days))
    i = 1  
    for (k, v) in days_dict 
        days_df[i, :day] = k 
        days_df[i, :n_users] = length(v) 
        i += 1
    end 
    active_users = sort(days_df, :day)
    active_users
end 
        





jobs_df[:, :time_range] = compute_daterange(jobs_df[:, :start_time], jobs_df[:, :end_time])



active_users_dict = get_active_users(jobs_df[:, :time_range], jobs_df[:, :user])



CSV.write("data/daily_active_users.csv", active_users_dict)

