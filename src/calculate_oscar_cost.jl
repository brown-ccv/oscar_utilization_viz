# src/calculate_gcp_cost.jl
using Dates
using CSV
using DataFrames
using DataFramesMeta

include("utils.jl")

raw_jobs_df = CSV.read("data/oscar_jobs_2022-10-31.csv", DataFrame)

sort!(gcp_cost_df, [:spot_price, :cores, :memory])



jobs_df = @rsubset raw_jobs_df begin 
    :mem_req < 2_000_000
    :tres_alloc != ""
    :sec_runtime < 8640000 # 100 days
end  



jobs_df[!, :memory_gb] .= jobs_df[:, :mem_req] ./ 1000

jobs_df[!, :start_time] .= DateTime.(jobs_df[:, :start_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :submit_time] .= DateTime.(jobs_df[:, :submit_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :end_time] .= DateTime.(jobs_df[:, :end_time], dateformat"Y-m-d H:M:S")
jobs_df[!, :hour_runtime] .= jobs_df[:, :min_runtime] ./ 60
jobs_df[!, :gpu_per_node] .= get_gpu_per_node(jobs_df[:, :gres_req])
jobs_df[!, :gpus] .= jobs_df[!, :gpu_per_node] .* jobs_df[!, :nodes_alloc]



year_df = @rsubset jobs_df begin 
    :start_time > Date("2021-11-01")
end 


function get_cpu_cost(cpus_req::Vector{Int64}, hours_runtime::Vector{Float64})
    n = length(cpus_req)
    cpu_cost = zeros(n)
    oscar_cpu_hour_rate = 0.0103


    for i in 1:n 
        cpu_cost[i] = oscar_cpu_hour_rate * cpus_req[i] * hours_runtime[i]
    end 

    return cpu_cost
end 


function get_gpu_cost(num_gpus::Vector{Union{Missing, Int64}}, hours_runtime::Array{Float64, 1})
    n = length(num_gpus) 
    gpu_cost = Array{Float64, 1}(undef, n)
    oscar_gpu_hour_rate = 0.3301

    for i in 1:n 
        if ismissing(num_gpus[i])
            gpu_cost[i] = 0.0 
        else 
            gpu_cost[i] = num_gpus[i] * oscar_gpu_hour_rate * hours_runtime[i]
        end 
    end 
    return gpu_cost
end 


year_df[!, :cpu_cost] = get_cpu_cost(year_df[:, :cpus_req], year_df[:, :hour_runtime])


year_df[!, :gpu_cost] = get_gpu_cost(year_df[:, :gpus], year_df[:, :hour_runtime])

println("Cost of GPUs: ", sum(year_df[:, :gpu_cost]))
println("Cost of CPUs: ", sum(year_df[:, :cpu_cost]))



CSV.write("/Users/pstey/Desktop/year_df.csv", year_df)


