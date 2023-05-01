# src/calculate_gcp_cost.jl
using Dates
using CSV
using DataFrames
using DataFramesMeta

include("utils.jl")

raw_jobs_df = CSV.read("data/oscar_jobs_2022-10-31.csv", DataFrame)
gcp_cost_df = CSV.read("data/gcp_compute_iowa_pricing.csv", DataFrame) 


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


price_hmap = Dict(gcp_cost_df[:, :machine_type] .=> gcp_cost_df[:, :spot_price])

function find_gcp_instance(n_cores::Int, memory::Float64, gcp_instance_df::DataFrame)
    n_instances = nrow(gcp_instance_df)
    machine_type = ""

    for i in 1:n_instances 
        if gcp_instance_df[i, :cores] >= n_cores && gcp_instance_df[i, :memory] >= memory
            machine_type = gcp_instance_df[i, :machine_type]
            break 
        end 
    end
    
    return machine_type
end 


function assign_gcp_instances(jobs_df::DataFrame, gcp_instance_df::DataFrame)
    n_jobs = nrow(jobs_df)
    machine_type = Vector{String}(undef, n_jobs)
    for i in 1:n_jobs
        machine_type[i] = find_gcp_instance(jobs_df[i, :cpus_req], jobs_df[i, :memory_gb], gcp_instance_df)
    end 
    return machine_type
end 

function get_gpu_cost(num_gpus::Vector{Union{Missing, Int64}}, hours_runtime::Array{Float64, 1})
    n = length(num_gpus) 
    gpu_cost = Array{Float64, 1}(undef, n)
    gcp_rate = 2.48

    for i in 1:n 
        if ismissing(num_gpus[i])
            gpu_cost[i] = 0.0 
        else 
            gpu_cost[i] = num_gpus[i] * gcp_rate * hours_runtime[i]
        end 
    end 
    return gpu_cost
end 


function get_cpu_cost(gcp_instance::Vector{String}, hours_runtime::Vector{Float64}, gcp_price_hmap::Dict{String31, Float64})
    n = length(gcp_instance)
    cpu_cost = zeros(n)

    for i in 1:n 
        if gcp_instance[i] == ""
            continue 
        end 
        cpu_cost[i] = gcp_price_hmap[gcp_instance[i]] * hours_runtime[i]
    end 

    return cpu_cost
end 



year_df[!, :gcp_instance] = assign_gcp_instances(year_df, gcp_cost_df)

year_df[!, :cpu_cost] = get_cpu_cost(year_df[:, :gcp_instance], year_df[:, :hour_runtime], price_hmap)


year_df[!, :gpu_cost] = get_gpu_cost(year_df[:, :gpus], year_df[:, :hour_runtime])

println("Cost of GPUs: ", sum(year_df[:, :gpu_cost]))
println("Cost of CPUs: ", sum(year_df[:, :cpu_cost]))



CSV.write("/Users/pstey/Desktop/year_df.csv", year_df)


