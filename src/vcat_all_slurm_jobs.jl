using CSV 
using DataFrames 


new_df = CSV.read("data/oscar_jobs_slurm_2020.csv", DataFrame)
old_df = CSV.read("data/oscar_jobs_slurm_2017.csv", DataFrame)
older_df = CSV.read("data/oscar_jobs_slurm_2014.csv", DataFrame)

tmp_df = vcat(old_df, new_df)
all_df = vcat(tmp_df, older_df)


CSV.write("data/oscar_jobs_slurm_2014_2017_2020.csv", all_df)


CSV.write("data/unique_users.txt", DataFrame(user = unique(jobs_df[:, :user])))
 