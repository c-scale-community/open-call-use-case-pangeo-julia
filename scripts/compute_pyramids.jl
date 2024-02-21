using YAXArrays
using PyramidScheme: PyramidScheme as PS
using NCDatasets
using Glob
using NetCDF
using Statistics
using Rasters

println("Packages loaded")
resampledir = "/eodc/private/pangeojulia/rqatrend_EU/webmercator"


tilepaths = glob("*.nc", resampledir)


for p in tilepaths
    pex = splitext(p)[1]
    pyrdir = pex * "_pyramids"
    if isdir(pyrdir)
        println("Skip $pyrdir")
        continue
    end
    @time "Load data" c = Raster(p, lazy=true)
    @time "Compute pyramids" raspyr, rasaxs = PyramidScheme.getpyramids(mean ∘ skipmissing, yaxeurope; recursive=false)

    mkpath(pyrdir)
    #@show outpaths
    rasters = Raster.(raspyr, rasaxs)
    for (i, r) in enumerate(rasters)
        outpath = joinpath(pyrdir, basename(pex)*"_zoom_" * string(i)* ".nc")
        @show outpath
        @time "write" write(outpath, r ; deflatelevel=3, force=true)
    end
    GC.gc()
end

