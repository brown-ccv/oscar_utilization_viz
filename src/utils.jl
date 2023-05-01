function get_gpu_per_node(gres_req_vec)
    n = length(gres_req_vec)
    gpu_per_node = Vector{Union{Int, Missing}}(undef, n)
    
    for i in 1:n 
        if ismissing(gres_req_vec[i])
            gpu_per_node[i] = missing 
        elseif contains(gres_req_vec[i], "gpu:")

            # The `PER_NODE:` preface is from the newer SLURM database
            if contains(gres_req_vec[i], "PER_NODE:gpu:")
                maybe_int = tryparse(Int, replace(gres_req_vec[i], "PER_NODE:gpu:" => ""))
            else 
                maybe_int = tryparse(Int, replace(gres_req_vec[i], "gpu:" => ""))
            end 
            if typeof(maybe_int) == Int 
                gpu_per_node[i] = maybe_int 
            else 
                gpu_per_node[i] = missing 
            end
        end 
    end 

    return gpu_per_node
end 

"""
The following are the parameter definitions, taken from Walker (2009) see link below.
    C_t: cost for a single year 
    k: cost of captial 
    tcpu: total CPU cores 
    H: total hours 
    mu: average proportion of cores in use

Soure: Walker (2009) http://staff.um.edu.mt/carl.debono/DT_CCE3013_1.pdf
"""
function cpu_hour_cost(C_t, k, tcpu, H, mu) 
    Y = 5
    cost = 0
    for T in 0:(Y - 1) 
        cost += C_t / (1 + k)^T 
    end 

    TC = tcpu * H * mu
    cpu_hour = ((1 - (1/sqrt(2))) * cost) / ((1 - (1/sqrt(2))^Y) * TC)

    cpu_hour 
end 

cpu_core_hour = cpu_hour_cost(2_886_000/5, 0.05, 14_000, 358*24, 0.75)
gpu_card_hour = cpu_hour_cost(2_320_000/5, 0.05, 378, 358*24, 0.7)
