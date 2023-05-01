
using Statistics
using Dates
using CSV 
using DataFrames 
using PooledArrays


raw_df = CSV.read("data/oscar_jobs_slurm_2014_2017_2020.csv", DataFrame)

jobs_df = sort(filter(:tres_alloc => x -> !ismissing(x) , raw_df), :start_time)

user_departments_df = CSV.read("data/oscar_users_and_departments.csv", DataFrame)

date_time_format = Dates.DateFormat("y-m-d H:M:S")

function is_priority_job(qos_name::Vector{Union{Missing, String31}}) 
    result = falses(length(qos_name))
    for i in eachindex(qos_name) 
        if !ismissing(qos_name[i]) 
            if occursin("pri-", qos_name[i]) || occursin("priority", qos_name[i]) 
                result[i] = true 
            end 
        end 
    end 
    return result
end 

function is_condo_job(qos_name::Vector{Union{Missing, String31}}) 
    result = falses(length(qos_name))
    for i in eachindex(qos_name) 
        if !ismissing(qos_name[i]) 
            if occursin("condo", qos_name[i]) 
                result[i] = true 
            end 
        end 
    end 
    return result
end 

function get_job_type(is_condo::BitVector, is_priority::BitVector) 
    n = length(is_condo)
    job_type = Vector{String}(undef, n)

    for i in eachindex(job_type) 

        if is_condo[i] 
            job_type[i] = "condo"
        elseif is_priority[i] 
            job_type[i] = "priority"
        else 
            job_type[i] = "exploratory"
        end 
    end

    return job_type
end 

function get_gpu_requested(gres_req::PooledArrays.PooledVector{Union{Missing, String15}, UInt32, Vector{UInt32}}) 
    result = zeros(Int, length(gres_req))
    
    for i in eachindex(gres_req)
        if !ismissing(gres_req[i]) && occursin("gpu:", gres_req[i])
            if occursin("PER_NODE:gpu:", gres_req[i])
                result[i] = parse(Int, replace(gres_req[i], "PER_NODE:gpu:" => ""))
            elseif occursin("gpu:", gres_req[i]) 
                result[i] = parse(Int, replace(gres_req[i], "gpu:" => ""))
            end  
        end  
    end
    return result
end  


jobs_df[:, :submit_time_dt]       = parse.(DateTime, jobs_df[:, :submit_time], date_time_format)
jobs_df[:, :gpus_req]             = get_gpu_requested(jobs_df[:, :gres_req])
jobs_df[:, :is_gpu_job]           = jobs_df[:, :gpus_req] .> 0
jobs_df[:, :min_waiting_in_queue] = jobs_df[:, :sec_waiting_in_queue] ./ 60
jobs_df[:, :is_priority_job]      = is_priority_job(jobs_df[:, :qos_name])
jobs_df[:, :is_condo_job]         = is_condo_job(jobs_df[:, :qos_name])
jobs_df[:, :cpu_core_hours]       = jobs_df[:, :cpus_req] .* jobs_df[:, :min_runtime] .* 60
jobs_df[:, :submit_hour]          = map(x -> x.value, Dates.Hour.(jobs_df[:, :submit_time_dt]))
jobs_df[:, :year]                 = Dates.year.(jobs_df[:, :submit_time_dt])
jobs_df[:, :job_type]             = get_job_type(jobs_df[:, :is_condo_job], jobs_df[:, :is_priority_job])

jobs_df = leftjoin(jobs_df, user_departments_df, on = :user)

user_gdf = groupby(jobs_df, [:user, :year])

user_df = combine(user_gdf, 
                  nrow => :num_jobs, 
                  :is_priority_job => sum => :num_priority_jobs,
                  :is_condo_job => sum => :num_condo_jobs,
                  :is_priority_job => mean => :prop_priority_jobs,
                  :is_condo_job => mean => :prop_condo_jobs,
                  :is_gpu_job => sum => :num_gpu_jobs,
                  :is_gpu_job => mean => :prop_gpu_jobs,
                  :nodes_alloc => (x -> mean(x .> 1)) => :prop_multi_node_jobs,
                  :cpu_core_hours => sum => :total_cpu_core_hours,
                  :min_waiting_in_queue => mean => :mean_min_wait_in_queue)


user_df = leftjoin(user_df, user_departments_df, on = :user)

CSV.write("/Users/pstey/Desktop/user.csv", user_df)





department_gdf = groupby(jobs_df, :department)


department_df = combine(department_gdf, 
                  nrow => :num_jobs, 
                  :is_priority_job => sum => :num_priority_jobs,
                  :is_condo_job => sum => :num_condo_jobs,
                  :is_priority_job => mean => :prop_priority_jobs,
                  :is_condo_job => mean => :prop_condo_jobs,
                  :is_gpu_job => sum => :num_gpu_jobs,
                  :is_gpu_job => mean => :prop_gpu_jobs,
                  :nodes_alloc => (x -> mean(x .> 1)) => :prop_multi_node_jobs,
                  :cpu_core_hours => sum => :total_cpu_core_hours,
                  :min_waiting_in_queue => mean => :mean_min_wait_in_queue)


CSV.write("/Users/pstey/Desktop/department.csv", department_df)



job_type_gdf = groupby(jobs_df, [:job_type, :year])


job_type_df = combine(job_type_gdf, 
                  nrow => :num_jobs, 
                  :is_gpu_job => sum => :num_gpu_jobs,
                  :is_gpu_job => mean => :prop_gpu_jobs,
                  :nodes_alloc => (x -> mean(x .> 1)) => :prop_multi_node_jobs,
                  :cpu_core_hours => sum => :total_cpu_core_hours,
                  :min_waiting_in_queue => mean => :mean_min_wait_in_queue)


CSV.write("/Users/pstey/Desktop/job_type.csv", job_type_df)