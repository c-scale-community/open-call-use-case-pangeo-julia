using ArchGDAL
using RQADeforestation
using YAXArrays
using Glob
Threads.nthreads()

using Distributed
addprocs(7)
@everywhere begin
    using Pkg
    Pkg.activate(".")
end
@everywhere using ArchGDAL
@everywhere begin
    using YAXArrays
    using RQADeforestation
end

indir, pol = "/eodc/products/eodc.eu_sentinel1_backscatter/S1_CSAR_IWGRDH/SIG0/V1M1R1/EQUI7_EU020M/E048N018T3","VH"
filenames = glob("*$(pol)*.tif", indir)[1:100]


@time cubevh = gdalcube(filenames)
@time cubevhag = RQADeforestation.agcube(filenames)

s = 10
@time subcube = cubevh[X=(cubevh.X[1],cubevh.X[s]), Y=(cubevh.Y[1], cubevh.Y[s])];
@time subcubeag = cubevhag[X=(cubevhag.X[1],cubevhag.X[s]), Y=(cubevhag.Y[1], cubevhag.Y[s])];

for s in [2]#,10,50,100,150,200,400,500,600,1000,2000]
    println("Size: $s")
    #println("subsetting:")
    subcube = cubevh[X=(cubevh.X[1],cubevh.X[s]), Y=(cubevh.Y[1], cubevh.Y[s])]
    subcubeag = cubevhag[X=(cubevhag.X[1],cubevhag.X[s]), Y=(cubevhag.Y[1], cubevhag.Y[s])]
    #@show size(subcube)
    #@show size(subcubeag)
    println("countvalid computations")
    println("GDALBand Approach:")
    @time valsub = mapCube(RQADeforestation.countvalid, subcube;indims=InDims("Time"), outdims=OutDims())
    println("AG readraster approach")
    @time valsubag = mapCube(RQADeforestation.countvalidag, subcubeag;indims=InDims("Time"), outdims=OutDims())
end

#valcube = RQADeforestation.countvalid(cubevh)


@time rqatrendvh = RQADeforestation.rqatrend(cubevh)
@time rqatrendvhag = RQADeforestation.rqatrend(cubevhag,thresh=2)

