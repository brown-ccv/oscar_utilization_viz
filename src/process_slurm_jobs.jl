#process_slurm_jobs.jl
using Dates
using CSV
using DataFrames

# raw_df = CSV.read("carney_condo_20200812.csv")
# raw_df = CSV.read("biomed/biomed_jobs_20200812.csv", types = Dict(:mem_req => UInt128))
raw_df = CSV.read("../data/oscar_all_jobs_2022-10-21.csv", DataFrame)

const DT_FORMAT = "yyyy-mm-dd HH:MM:SS"
const OUTFILE = "../data/oscar_utilization_timeseries.csv"


keep_row = (raw_df[:, :nodes_alloc] .> 0) .& (raw_df[:, :start_time] .!= "1969-12-31 19:00:00")
df = raw_df[keep_row, :]

# df[1, :start_time]


df[!, :unix_start_time] = Int.(datetime2unix.(DateTime.(df[:, :start_time], DT_FORMAT)))
df[!, :unix_submit_time] = Int.(datetime2unix.(DateTime.(df[:, :submit_time], DT_FORMAT)))
df[!, :unix_end_time] = Int.(datetime2unix.(DateTime.(df[:, :end_time], DT_FORMAT)))
  

const started_jobs = findall(df[:, :unix_start_time] .> 0)

const UNIX_DAY1 = minimum(df[started_jobs, :unix_start_time])







function get_gpu_requested(v) 
    n = length(v)
    res = zeros(Int, n)
    for i = 1:n 
        if !ismissing(v[i]) && occursin("PER_NODE:gpu:", v[i])
            res[i] = parse(Int, replace(v[i], "PER_NODE:gpu:" => ""))
        end
    end
    res
end 


df[!, :gpus_req] = get_gpu_requested(df[:, :gres_req])



function gen_timeseries(t1::Array{Int64,1}, t2::Array{Int64,1}, day1::Int64) 
    n = length(t1)
    total_sec = maximum(t2) - day1

    res = zeros(Int, total_sec)

    for i = 1:n
        # Begin by getting the end time of the job. In some cases, this 
        # will be negative. This seems to happen for in-progress jobs.
        end_time = t2[i] < t1[i] ? total_sec : t2[i]
        start_idx = t1[i] - day1

        n_sec = end_time - t1[i]
        end_idx = start_idx + n_sec
        res[start_idx:end_idx] .+= 1
    end 
    res 
end 


function gen_timeseries(t1::Array{Int64,1}, t2::Array{Int64,1}, day1::Int64, res_alloc::Array{Int64,1}) 
    n = length(t1)
    total_sec = maximum(t2) - day1

    res = zeros(Int, total_sec)

    for i = 1:n
        # Begin by getting the end time of the job. In some cases, this 
        # will be negative. This seems to happen for in-progress jobs.
        end_time = t2[i] < t1[i] ? total_sec : t2[i]
        start_idx = t1[i] - day1

        n_sec = end_time - t1[i]
        end_idx = start_idx + n_sec
        res[start_idx:end_idx] .+= res_alloc[i]
    end 
    res 
end 


function test()
    a = [6, 7, 8]
    b = [10, 9, 11]

    c = [1, 5, 9, 1]
    d = [2, 8, 17, 18]

    @assert gen_timeseries(a, b, 3) == [0, 0, 1, 2, 3, 3, 2, 1]
    @assert gen_timeseries(c, d, 0) == [2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]
    println("All tests passed!!")
end 

test()

function main()
    out_df = DataFrame(epoch_time = collect(UNIX_DAY1:maximum(df[:, :unix_end_time])))

    out_df[!, :running_jobs] = gen_timeseries(df[:, :unix_start_time], df[:, :unix_end_time], UNIX_DAY1 - 1)
    out_df[!, :cpus_in_use] = gen_timeseries(df[:, :unix_start_time], 
                                             df[:, :unix_end_time], 
                                             UNIX_DAY1 - 1,
                                             df[:, :cpus_req])
    out_df[!, :gpus_in_use] = gen_timeseries(df[:, :unix_start_time], 
                                             df[:, :unix_end_time], 
                                             UNIX_DAY1 - 1,
                                             df[:, :gpus_req])
    out_df[!, :nodes_in_use] = gen_timeseries(df[:, :unix_start_time], 
                                             df[:, :unix_end_time], 
                                             UNIX_DAY1 - 1,
                                             df[:, :nodes_alloc])


    CSV.write(OUTFILE, out_df)
end 

main()


