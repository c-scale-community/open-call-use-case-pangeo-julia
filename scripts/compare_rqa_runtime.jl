using RecurrenceAnalysis
using BenchmarkTools
using RQADeforestation
using DelimitedFiles
xs = [2:2:20..., 21:10:100...,100:200:1001..., 1002:400:6000...]



time_matrix = Float64[]
time_vec = Float64[]

mem_matrix = Float64[]
mem_vec = Float64[]
allocs_matrix = Float64[]
allocs_vec = Float64[]
out= [0.]

for l in xs 
    println(l)
    ts = rand(l)
    tmat = @benchmark RQADeforestation.rqatrend_matrix(out, $ts) (samples=1000)
    mtmat = minimum(tmat)
    push!(time_matrix, mtmat.time)
    push!(mem_matrix, mtmat.memory)
    push!(allocs_matrix, mtmat.allocs)
    tvec = @benchmark RQADeforestation.rqatrend(out, $ts)
    mtvec = minimum(tvec)
    push!(time_vec, mtvec.time)
    push!(mem_vec, mtvec.memory)
    push!(allocs_vec, mtvec.allocs)
end

open("rqa_benchmarks_time.txt", "w") do io
    writedlm(io, [time_matrix time_vec])
    end
 open("rqa_benchmarks_mem.txt", "w") do io
    writedlm(io, [mem_matrix mem_vec])
    end
open("rqa_benchmarks_allocs.txt", "w") do io
    writedlm(io, [allocs_matrix allocs_vec])
    end
