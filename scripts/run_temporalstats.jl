using ArchGDAL
using RQADeforestation
using YAXArrays
using Glob
using Dates
Threads.nthreads()
YAXArrays.YAXDefaults.workdir[] = "/eodc/private/pangeojulia/"

using Distributed
nw = 8
addprocs(nw)
@everywhere begin
    using Pkg
    Pkg.activate("/home/ubuntu/RQADeforestation/")
end

@everywhere using ArchGDAL
@everywhere begin
    using YAXArrays
    using RQADeforestation
end
@everywhere using Logging
@everywhere using LoggingExtras
#@everywhere flog = MinLevelLogger(FileLogger("logfile_rqtrend_d066.txt"), Logging.Warn)
#@everywhere Base.global_logger(flog)

indir, pol = "/eodc/products/eodc.eu_sentinel1_backscatter/S1_CSAR_IWGRDH/SIG0/V1M1R1/EQUI7_EU020M/E048N018T3","VH"
thresh=4
orbit = "A117"
filenames = glob("*$(pol)_$(orbit)*.tif", indir)

println("loading the data:")
@time cubevh = RQADeforestation.gdalcube(filenames)
#s =100
#subcube = cubevh[X=(cubevh.X[1],cubevh.X[s]), Y=(cubevh.Y[1], cubevh.Y[s])]
nt =Threads.nthreads()
#@time cubevh[:,:,:];
#@time cubevh[:,:,:];
println("Doing the RQA computation:")
YAXArrays.YAXDefaults.max_cache[]=5e8
t = now()
#stdiopath = "_d066_numw_$(nw)_$(nt)_$t.txt"
#println("stdout" * stdiopath)
#redirect_stdio(stdout="stdout"*stdiopath, stderr="stderr" * stdiopath) do
    println("Start of the processing: ",now())
    @show nw, nt
    @time RQADeforestation.timestats(cubevh; path="/eodc/private/pangeojulia/E048N018T3_timestats_$(pol)_$(orbit)_thresh_$(thresh)_$(nw)_$(nt)_$t.zarr")
    #@time rqatrendvh = RQADeforestation.rqatrend(cubevh,thresh=4, outpath="/eodc/private/pangeojulia/E048N018T3_rqatrend_$(pol)_D066_$(nw)_$(nt)_$t.zarr")
    println("End of the processing: ",now())
#end