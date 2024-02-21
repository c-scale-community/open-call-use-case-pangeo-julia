#using Revise
using YAXArrays
println("YAX loaded")
using MultipleTesting
println("MulitpleTesting loaded")

using RQADeforestation
println("RQADeforestatioin loaded")

using Glob
println("Glob loaded")

using DimensionalData: DimensionalData as DD
println("DD loaded")
using NetCDF
println("NetCDF loaded")


indir = "/eodc/private/pangeojulia/rqatrend_EU/"
clusterpaths = glob("*.zarr", indir)

forestdir = joinpath(indir, "forestmasked_thresh_cluster")
mkpath(forestdir)

c = clusterpaths[210]
println("Start for loop")

Threads.@threads for c in clusterpaths
    tile = match(r"E\d\d\dN\d\d\dT3", c).match
    forestpath = "/eodc/private/pangeojulia/forestaggregated/compressed_forestaggregated_20M_EU_$(tile).nc"
    outpath = joinpath(forestdir, "masked_thresh_cluster" * splitext(basename(c))[1])
    if isfile(outpath*".nc")
        println("Skip already computed file")
        continue
    end
    if !isfile(forestpath)
        println("Skip non existing forest tile:")
        println(c)
        continue
    end
    println("Start computation for $c")
    yax = Cube(c)
    @time "Load rqa results" yaxmem = readcubedata(yax)
    forest = Cube(forestpath)
    @time "Load forestmask" forestmem = readcubedata(forest)
    @time "Inner processing" clustermaskeddata = RQADeforestation.inner_postprocessing(yaxmem.data, forestmem.data)
    clustermaskedyax = YAXArray(DD.dims(yax), clustermaskeddata)
    #@time savecube(clustermaskedyax, outpath * ".zarr", overwrite=true)
    chunked = setchunks(clustermaskedyax, (1000,1000))
    @time "Saving"  savecube(chunked, outpath * ".nc", overwrite=true, compress=5)

#    @time savecube(clustermaskedyax, outpath, overwrite=true)
end